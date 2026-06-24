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

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.model.SnapshotKey;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.telemetry.StoreOperationSummary;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;

@ApplicationScoped
public class SnapshotRepository {
  public enum CurrentSnapshotPointerUpdateResult {
    UPDATED,
    UNCHANGED,
    TABLE_MISSING,
    CONFLICT
  }

  private final GenericResourceRepository<Snapshot, SnapshotKey> repo;
  private final TableRepository tableRepo;

  @Inject
  public SnapshotRepository(
      PointerStore pointerStore, BlobStore blobStore, TableRepository tableRepo) {
    this.repo =
        new GenericResourceRepository<>(
            pointerStore,
            blobStore,
            Schemas.SNAPSHOT,
            Snapshot::parseFrom,
            Snapshot::toByteArray,
            "application/x-protobuf");
    this.tableRepo = tableRepo;
  }

  public void create(Snapshot snapshot) {
    repo.create(snapshot);
  }

  public boolean update(Snapshot snapshot, long expectedPointerVersion) {
    return repo.update(snapshot, expectedPointerVersion);
  }

  public boolean delete(ResourceId tableId, long snapshotId) {
    return repo.delete(new SnapshotKey(tableId.getAccountId(), tableId.getId(), snapshotId));
  }

  public boolean deleteWithPrecondition(
      ResourceId tableId, long snapshotId, long expectedPointerVersion) {
    return repo.deleteWithPrecondition(
        new SnapshotKey(tableId.getAccountId(), tableId.getId(), snapshotId),
        expectedPointerVersion);
  }

  public Optional<Snapshot> getById(ResourceId tableId, long snapshotId) {
    return repo.getByKey(new SnapshotKey(tableId.getAccountId(), tableId.getId(), snapshotId));
  }

  public CurrentSnapshotPointerUpdateResult maybeAdvanceCurrentSnapshotPointer(
      ResourceId tableId, Snapshot candidate) {
    for (int attempt = 0; attempt < 4; attempt++) {
      Optional<Table> table = tableRepo.getById(tableId);
      if (table.isEmpty()) {
        return CurrentSnapshotPointerUpdateResult.TABLE_MISSING;
      }
      if (!shouldAdvanceCurrentSnapshot(tableId, table.get(), candidate)) {
        return CurrentSnapshotPointerUpdateResult.UNCHANGED;
      }

      long expectedVersion = tableRepo.metaFor(tableId).getPointerVersion();
      Table updated =
          table.get().toBuilder()
              .putProperties("current-snapshot-id", Long.toString(candidate.getSnapshotId()))
              .build();
      if (tableRepo.update(updated, expectedVersion)) {
        return CurrentSnapshotPointerUpdateResult.UPDATED;
      }
      backoffCurrentPointerAdvance(attempt);
    }
    return CurrentSnapshotPointerUpdateResult.CONFLICT;
  }

  private static void backoffCurrentPointerAdvance(int attempt) {
    long baseNanos = (1L << Math.min(attempt, 5)) * 1_000_000L;
    long jitterNanos = ThreadLocalRandom.current().nextLong(250_000L, 1_000_001L);
    LockSupport.parkNanos(baseNanos + jitterNanos);
  }

  private boolean shouldAdvanceCurrentSnapshot(
      ResourceId tableId, Table table, Snapshot candidateSnapshot) {
    String currentSnapshotId = table.getPropertiesMap().get("current-snapshot-id");
    if (currentSnapshotId == null || currentSnapshotId.isBlank()) {
      return true;
    }

    long currentId;
    try {
      currentId = Long.parseLong(currentSnapshotId);
    } catch (NumberFormatException e) {
      return true;
    }

    if (currentId == candidateSnapshot.getSnapshotId()) {
      return false;
    }

    Snapshot currentSnapshot = getById(tableId, currentId).orElse(null);
    if (currentSnapshot == null) {
      return true;
    }

    long currentMs =
        currentSnapshot.hasUpstreamCreatedAt()
            ? Timestamps.toMillis(currentSnapshot.getUpstreamCreatedAt())
            : Long.MIN_VALUE;
    long candidateMs =
        candidateSnapshot.hasUpstreamCreatedAt()
            ? Timestamps.toMillis(candidateSnapshot.getUpstreamCreatedAt())
            : Long.MIN_VALUE;

    if (candidateMs != currentMs) {
      return candidateMs > currentMs;
    }
    return candidateSnapshot.getSnapshotId() > currentSnapshot.getSnapshotId();
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
    Optional<Table> table = tableRepo.getById(tableId);
    if (table.isEmpty()) {
      return Optional.empty();
    }
    String currentSnapshotId = table.get().getPropertiesMap().get("current-snapshot-id");
    if (currentSnapshotId == null || currentSnapshotId.isBlank()) {
      StoreOperationSummary.put("current_snapshot_source", "fallback");
      StoreOperationSummary.fallback("current_snapshot");
      return latestSnapshotByTime(tableId);
    }
    long snapshotId;
    try {
      snapshotId = Long.parseLong(currentSnapshotId);
    } catch (NumberFormatException e) {
      StoreOperationSummary.put("current_snapshot_source", "fallback");
      StoreOperationSummary.fallback("current_snapshot_invalid_property");
      return latestSnapshotByTime(tableId);
    }
    // Iceberg uses -1 as the no-current-snapshot sentinel; valid snapshot ids are non-negative.
    if (snapshotId < 0) {
      StoreOperationSummary.put("current_snapshot_source", "fallback");
      StoreOperationSummary.fallback("current_snapshot_invalid_property");
      return latestSnapshotByTime(tableId);
    }
    Optional<Snapshot> current = getById(tableId, snapshotId);
    StoreOperationSummary.put(
        "current_snapshot_source", current.isPresent() ? "property" : "fallback");
    if (current.isEmpty()) {
      StoreOperationSummary.fallback("current_snapshot_missing_property_target");
    }
    return current.isPresent() ? current : latestSnapshotByTime(tableId);
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
}
