/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.service.repo.impl;

import ai.floedb.floecat.catalog.rpc.CurrentSnapshotPointer;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.model.SnapshotKey;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.telemetry.StoreOperationSummary;
import ai.floedb.floecat.types.Hashing;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;
import org.jboss.logging.Logger;

@ApplicationScoped
public class SnapshotRepository {
  private static final Logger LOG = Logger.getLogger(SnapshotRepository.class);

  public enum CurrentSnapshotPointerUpdateResult {
    UPDATED,
    UNCHANGED,
    TABLE_MISSING,
    CONFLICT
  }

  private final GenericResourceRepository<Snapshot, SnapshotKey> repo;
  private final PointerStore pointerStore;
  private final SnapshotCreateCounterStore createCounters;
  private final TableRepository tableRepo;
  private final CurrentSnapshotPointerRepository currentPointerRepo;
  private final Clock clock;

  @Inject
  public SnapshotRepository(
      PointerStore pointerStore,
      BlobStore blobStore,
      TableRepository tableRepo,
      CurrentSnapshotPointerRepository currentPointerRepo) {
    this.repo =
        new GenericResourceRepository<>(
            pointerStore,
            blobStore,
            Schemas.SNAPSHOT,
            Snapshot::parseFrom,
            Snapshot::toByteArray,
            "application/x-protobuf");
    this.pointerStore = pointerStore;
    this.createCounters = new SnapshotCreateCounterStore(pointerStore);
    this.tableRepo = tableRepo;
    this.currentPointerRepo = currentPointerRepo;
    this.clock = Clock.systemUTC();
  }

  public SnapshotRepository(
      PointerStore pointerStore, BlobStore blobStore, TableRepository tableRepo) {
    this(
        pointerStore,
        blobStore,
        tableRepo,
        new CurrentSnapshotPointerRepository(pointerStore, blobStore));
  }

  public SnapshotRepository(
      PointerStore pointerStore,
      BlobStore blobStore,
      TableRepository tableRepo,
      CurrentSnapshotPointerRepository currentPointerRepo,
      Clock clock) {
    this.repo =
        new GenericResourceRepository<>(
            pointerStore,
            blobStore,
            Schemas.SNAPSHOT,
            Snapshot::parseFrom,
            Snapshot::toByteArray,
            "application/x-protobuf");
    this.pointerStore = pointerStore;
    this.createCounters = new SnapshotCreateCounterStore(pointerStore);
    this.tableRepo = tableRepo;
    this.currentPointerRepo = currentPointerRepo;
    this.clock = clock;
  }

  public void create(Snapshot snapshot) {
    String blobUri = snapshotBlobUri(snapshot);
    repo.putBlob(blobUri, snapshot);

    List<String> snapshotPointerKeys = snapshotPointerKeys(snapshot);
    for (int attempt = 0; attempt < BaseResourceRepository.CAS_MAX; attempt++) {
      List<PointerStore.CasOp> ops = new ArrayList<>(snapshotPointerKeys.size() + 1);
      for (String pointerKey : snapshotPointerKeys) {
        ops.add(new PointerStore.CasUpsert(pointerKey, 0L, snapshotPointer(pointerKey, blobUri)));
      }
      ops.addAll(
          createCounters.planIncrementOps(
              List.of(
                  new SnapshotCreateCounterStore.CreateIncrement(
                      snapshot.getTableId().getAccountId()))));

      if (pointerStore.compareAndSetBatch(ops)) {
        return;
      }
      try {
        classifySnapshotCreateConflict(blobUri, snapshotPointerKeys);
      } catch (BaseResourceRepository.AbortRetryableException e) {
        if (attempt + 1 >= BaseResourceRepository.CAS_MAX) {
          throw e;
        }
        backoffCurrentPointerAdvance(attempt);
        continue;
      }
      return;
    }
    throw new BaseResourceRepository.AbortRetryableException(
        "snapshot create counter conflict for: "
            + Keys.snapshotPointerById(
                snapshot.getTableId().getAccountId(),
                snapshot.getTableId().getId(),
                snapshot.getSnapshotId()));
  }

  public boolean update(Snapshot snapshot, long expectedPointerVersion) {
    boolean updated = repo.update(snapshot, expectedPointerVersion);
    if (updated) {
      maybeAdvanceCurrentSnapshotPointer(snapshot.getTableId(), snapshot);
    }
    return updated;
  }

  public boolean delete(ResourceId tableId, long snapshotId) {
    boolean deleted =
        repo.delete(new SnapshotKey(tableId.getAccountId(), tableId.getId(), snapshotId));
    if (deleted) {
      deleteCurrentPointerIfCurrent(tableId, snapshotId);
    }
    return deleted;
  }

  public boolean deleteWithPrecondition(
      ResourceId tableId, long snapshotId, long expectedPointerVersion) {
    boolean deleted =
        repo.deleteWithPrecondition(
            new SnapshotKey(tableId.getAccountId(), tableId.getId(), snapshotId),
            expectedPointerVersion);
    if (deleted) {
      deleteCurrentPointerIfCurrent(tableId, snapshotId);
    }
    return deleted;
  }

  public Optional<Snapshot> getById(ResourceId tableId, long snapshotId) {
    return repo.getByKey(new SnapshotKey(tableId.getAccountId(), tableId.getId(), snapshotId));
  }

  public CurrentSnapshotPointerUpdateResult maybeAdvanceCurrentSnapshotPointer(
      ResourceId tableId, Snapshot candidate) {
    var table = tableRepo.getById(tableId);
    if (table.isEmpty()) {
      return CurrentSnapshotPointerUpdateResult.TABLE_MISSING;
    }

    for (int attempt = 0; attempt < 4; attempt++) {
      Optional<CurrentSnapshotPointer> currentPointer = currentPointerRepo.get(tableId);
      if (!shouldAdvanceCurrentSnapshot(tableId, currentPointer.orElse(null), candidate)) {
        return CurrentSnapshotPointerUpdateResult.UNCHANGED;
      }

      CurrentSnapshotPointer next = buildCurrentPointer(tableId, candidate);
      if (currentPointer.isEmpty()) {
        if (currentPointerRepo.createIfAbsent(next)) {
          return CurrentSnapshotPointerUpdateResult.UPDATED;
        }
      } else {
        Optional<CurrentSnapshotPointerObservation> observed =
            currentPointerForUpdate(tableId, currentPointer.get());
        if (observed.isEmpty()) {
          backoffCurrentPointerAdvance(attempt);
          continue;
        }
        if (!shouldAdvanceCurrentSnapshot(tableId, observed.get().pointer(), candidate)) {
          return CurrentSnapshotPointerUpdateResult.UNCHANGED;
        }
        if (currentPointerRepo.update(next, observed.get().pointerVersion())) {
          return CurrentSnapshotPointerUpdateResult.UPDATED;
        }
      }
      backoffCurrentPointerAdvance(attempt);
    }
    return CurrentSnapshotPointerUpdateResult.CONFLICT;
  }

  private Optional<CurrentSnapshotPointerObservation> currentPointerForUpdate(
      ResourceId tableId, CurrentSnapshotPointer expectedPointer) {
    long expectedVersion;
    try {
      expectedVersion = currentPointerRepo.metaFor(tableId).getPointerVersion();
    } catch (BaseResourceRepository.NotFoundException e) {
      return Optional.empty();
    }
    Optional<CurrentSnapshotPointer> afterMeta = currentPointerRepo.get(tableId);
    if (afterMeta.isEmpty() || !afterMeta.get().equals(expectedPointer)) {
      return Optional.empty();
    }
    return Optional.of(new CurrentSnapshotPointerObservation(afterMeta.get(), expectedVersion));
  }

  private record CurrentSnapshotPointerObservation(
      CurrentSnapshotPointer pointer, long pointerVersion) {}

  private static void backoffCurrentPointerAdvance(int attempt) {
    long baseNanos = (1L << Math.min(attempt, 5)) * 1_000_000L;
    long jitterNanos = ThreadLocalRandom.current().nextLong(250_000L, 1_000_001L);
    LockSupport.parkNanos(baseNanos + jitterNanos);
  }

  private boolean shouldAdvanceCurrentSnapshot(
      ResourceId tableId, CurrentSnapshotPointer currentPointer, Snapshot candidateSnapshot) {
    if (currentPointer == null) {
      return true;
    }

    long currentMs =
        currentPointer.hasUpstreamCreatedAt()
            ? Timestamps.toMillis(currentPointer.getUpstreamCreatedAt())
            : Long.MIN_VALUE;
    long candidateMs =
        candidateSnapshot.hasUpstreamCreatedAt()
            ? Timestamps.toMillis(candidateSnapshot.getUpstreamCreatedAt())
            : Long.MIN_VALUE;

    if (currentPointer.getSnapshotId() == candidateSnapshot.getSnapshotId()) {
      if (currentMs != candidateMs) {
        LOG.warnf(
            "same snapshotId has different upstreamCreatedAt; leaving current pointer unchanged:"
                + " tableId=%s snapshotId=%d currentMs=%d candidateMs=%d",
            tableId, candidateSnapshot.getSnapshotId(), currentMs, candidateMs);
      }
      return false;
    }

    if (candidateMs != currentMs) {
      return candidateMs > currentMs;
    }
    return candidateSnapshot.getSnapshotId() > currentPointer.getSnapshotId();
  }

  public static String metadataLocation(Snapshot snapshot) {
    if (snapshot == null || !snapshot.hasMetadataLocation()) {
      return null;
    }
    String value = snapshot.getMetadataLocation().trim();
    return value.isBlank() ? null : value;
  }

  public Optional<Snapshot> getCurrentSnapshot(ResourceId tableId) {
    if (tableId == null) {
      return Optional.empty();
    }
    Optional<CurrentSnapshotPointer> pointer = currentPointerRepo.get(tableId);
    String fallbackReason = "current_snapshot";
    if (pointer.isPresent()) {
      long snapshotId = pointer.get().getSnapshotId();
      if (snapshotId < 0) {
        fallbackReason = "current_snapshot_invalid_pointer";
      } else {
        Optional<Snapshot> current = getById(tableId, snapshotId);
        StoreOperationSummary.put(
            "current_snapshot_source", current.isPresent() ? "pointer" : "fallback");
        if (current.isPresent()) {
          return current;
        }
        fallbackReason = "current_snapshot_missing_pointer_target";
      }
    }

    if (tableRepo.getById(tableId).isEmpty()) {
      return Optional.empty();
    }
    StoreOperationSummary.put("current_snapshot_source", "fallback");
    StoreOperationSummary.fallback(fallbackReason);
    return latestSnapshotByTime(tableId);
  }

  public Optional<CurrentSnapshotPointer> getCurrentSnapshotPointer(ResourceId tableId) {
    if (tableId == null) {
      return Optional.empty();
    }
    return currentPointerRepo.get(tableId);
  }

  private CurrentSnapshotPointer buildCurrentPointer(ResourceId tableId, Snapshot snapshot) {
    CurrentSnapshotPointer.Builder builder =
        CurrentSnapshotPointer.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshot.getSnapshotId())
            .setUpdatedAt(Timestamps.fromMillis(clock.millis()));
    if (snapshot.hasUpstreamCreatedAt()) {
      builder.setUpstreamCreatedAt(snapshot.getUpstreamCreatedAt());
    }
    return builder.build();
  }

  private void deleteCurrentPointerIfCurrent(ResourceId tableId, long snapshotId) {
    Optional<CurrentSnapshotPointer> before = currentPointerRepo.get(tableId);
    if (before.isEmpty() || before.get().getSnapshotId() != snapshotId) {
      return;
    }
    long expectedVersion;
    try {
      expectedVersion = currentPointerRepo.metaFor(tableId).getPointerVersion();
    } catch (BaseResourceRepository.NotFoundException e) {
      return;
    }
    Optional<CurrentSnapshotPointer> afterMeta = currentPointerRepo.get(tableId);
    if (afterMeta.isEmpty()
        || afterMeta.get().getSnapshotId() != snapshotId
        || !afterMeta.get().equals(before.get())) {
      return;
    }
    currentPointerRepo.deleteWithPrecondition(tableId, expectedVersion);
  }

  public Optional<Snapshot> getAsOf(ResourceId tableId, Timestamp asOf) {
    String prefix = Keys.snapshotPointerByTimePrefix(tableId.getAccountId(), tableId.getId());
    long asOfMs = Timestamps.toMillis(asOf);

    String token = "";
    StringBuilder next = new StringBuilder();
    do {
      List<Snapshot> batch = repo.listByPrefix(prefix, 200, token, next);
      for (Snapshot snapshot : batch) {
        long createdMs = Timestamps.toMillis(snapshot.getUpstreamCreatedAt());
        if (createdMs <= asOfMs) {
          return Optional.of(snapshot);
        }
      }
      token = next.toString();
      next.setLength(0);
    } while (!token.isEmpty());

    return Optional.empty();
  }

  public List<Snapshot> list(
      ResourceId tableId, int limit, String pageToken, StringBuilder nextOut) {
    String prefix = Keys.snapshotPointerByIdPrefix(tableId.getAccountId(), tableId.getId());
    return repo.listByPrefix(prefix, limit, pageToken, nextOut);
  }

  public List<Snapshot> listByTime(
      ResourceId tableId, int limit, String pageToken, StringBuilder nextOut) {
    String prefix = Keys.snapshotPointerByTimePrefix(tableId.getAccountId(), tableId.getId());
    return repo.listByPrefix(prefix, limit, pageToken, nextOut);
  }

  public long currentSnapshotCreateCounter(String accountId) {
    return createCounters.currentCounter(accountId);
  }

  public int count(ResourceId tableId) {
    String prefix = Keys.snapshotPointerByIdPrefix(tableId.getAccountId(), tableId.getId());
    return repo.countByPrefix(prefix);
  }

  public MutationMeta metaFor(ResourceId tableId, long snapshotId) {
    return repo.metaFor(new SnapshotKey(tableId.getAccountId(), tableId.getId(), snapshotId));
  }

  public MutationMeta metaFor(ResourceId tableId, long snapshotId, Timestamp nowTs) {
    return repo.metaFor(
        new SnapshotKey(tableId.getAccountId(), tableId.getId(), snapshotId), nowTs);
  }

  public MutationMeta metaForSafe(ResourceId tableId, long snapshotId) {
    return repo.metaForSafe(new SnapshotKey(tableId.getAccountId(), tableId.getId(), snapshotId));
  }

  private Optional<Snapshot> latestSnapshotByTime(ResourceId tableId) {
    String prefix = Keys.snapshotPointerByTimePrefix(tableId.getAccountId(), tableId.getId());
    String token = "";
    StringBuilder next = new StringBuilder();
    Snapshot best = null;
    long bestCreatedMs = Long.MIN_VALUE;

    do {
      List<Snapshot> batch = repo.listByPrefix(prefix, 200, token, next);
      for (Snapshot snapshot : batch) {
        long createdMs = Timestamps.toMillis(snapshot.getUpstreamCreatedAt());
        if (best == null) {
          best = snapshot;
          bestCreatedMs = createdMs;
          continue;
        }
        if (createdMs != bestCreatedMs) {
          return Optional.of(best);
        }
        if (snapshot.getSnapshotId() > best.getSnapshotId()) {
          best = snapshot;
        }
      }
      token = next.toString();
      next.setLength(0);
    } while (!token.isEmpty());

    return Optional.ofNullable(best);
  }

  private static String snapshotBlobUri(Snapshot snapshot) {
    byte[] bytes = snapshot.toByteArray();
    return Keys.snapshotBlobUri(
        snapshot.getTableId().getAccountId(),
        snapshot.getTableId().getId(),
        snapshot.getSnapshotId(),
        Hashing.sha256Hex(bytes));
  }

  private static Pointer snapshotPointer(String pointerKey, String blobUri) {
    return PointerReferences.blobPointer(pointerKey, blobUri, 1L);
  }

  private static List<String> snapshotPointerKeys(Snapshot snapshot) {
    long upstreamCreatedAtMs =
        snapshot.hasUpstreamCreatedAt() ? Timestamps.toMillis(snapshot.getUpstreamCreatedAt()) : 0L;
    LinkedHashSet<String> keys = new LinkedHashSet<>();
    keys.add(
        Keys.snapshotPointerById(
            snapshot.getTableId().getAccountId(),
            snapshot.getTableId().getId(),
            snapshot.getSnapshotId()));
    keys.add(
        Keys.snapshotPointerByTime(
            snapshot.getTableId().getAccountId(),
            snapshot.getTableId().getId(),
            snapshot.getSnapshotId(),
            upstreamCreatedAtMs));
    return List.copyOf(keys);
  }

  private void classifySnapshotCreateConflict(String blobUri, List<String> pointerKeys) {
    int present = 0;
    int absent = 0;
    for (String pointerKey : pointerKeys) {
      Pointer pointer = pointerStore.get(pointerKey).orElse(null);
      if (pointer == null) {
        absent++;
        continue;
      }
      if (!blobUri.equals(pointer.getBlobUri())) {
        throw new BaseResourceRepository.NameConflictException(
            "pointer bound to different blob: " + pointerKey);
      }
      present++;
    }
    if (absent == 0) {
      return;
    }
    if (present == 0) {
      throw new BaseResourceRepository.AbortRetryableException(
          "snapshot create conflict, no snapshot pointer present: " + pointerKeys.get(0));
    }
    throw new BaseResourceRepository.CorruptionException(
        "partial snapshot create state ("
            + present
            + " present, "
            + absent
            + " absent) for: "
            + pointerKeys);
  }
}
