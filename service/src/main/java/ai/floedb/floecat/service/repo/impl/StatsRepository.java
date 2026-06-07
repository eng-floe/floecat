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

import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.identity.TargetStatsRecords;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.stats.spi.StatsTargetType;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.types.Hashing;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@ApplicationScoped
public class StatsRepository implements StatsStore {

  private final PointerStore pointerStore;
  private final BlobStore blobStore;
  private final TargetStatsStorage targetStatsStorage;

  @Inject
  public StatsRepository(PointerStore pointerStore, BlobStore blobStore) {
    this.pointerStore = pointerStore;
    this.blobStore = blobStore;
    this.targetStatsStorage = new TargetStatsStorage(pointerStore, blobStore);
  }

  @Override
  public void putTargetStats(TargetStatsRecord value) {
    TargetStatsRecord canonicalRecord = canonicalRecord(value);
    ActiveSnapshotStats active =
        ensureActiveGeneration(canonicalRecord.getTableId(), canonicalRecord.getSnapshotId());
    targetStatsStorage.create(
        pointerKey(canonicalRecord, active.generationId()),
        blobUri(canonicalRecord, active.generationId()),
        canonicalRecord);
  }

  @Override
  public boolean putTargetStatsIfAbsent(TargetStatsRecord value) {
    TargetStatsRecord canonicalRecord = canonicalRecord(value);
    ActiveSnapshotStats active =
        ensureActiveGeneration(canonicalRecord.getTableId(), canonicalRecord.getSnapshotId());
    return targetStatsStorage.createIfAbsent(
        pointerKey(canonicalRecord, active.generationId()),
        blobUri(canonicalRecord, active.generationId()),
        canonicalRecord);
  }

  @Override
  public Optional<TargetStatsRecord> getTargetStats(
      ResourceId tableId, long snapshotId, StatsTarget target) {
    return activeGeneration(tableId, snapshotId)
        .flatMap(
            active ->
                targetStatsStorage.getByPointer(
                    targetPointerKey(tableId, snapshotId, active.generationId(), target)));
  }

  @Override
  public boolean deleteTargetStats(ResourceId tableId, long snapshotId, StatsTarget target) {
    return activeGeneration(tableId, snapshotId)
        .map(
            active ->
                pointerStore.delete(
                    targetPointerKey(tableId, snapshotId, active.generationId(), target)))
        .orElse(false);
  }

  @Override
  public StatsStorePage listTargetStats(
      ResourceId tableId,
      long snapshotId,
      Optional<StatsTargetType> targetType,
      int limit,
      String pageToken) {
    Optional<ActiveSnapshotStats> active = activeGeneration(tableId, snapshotId);
    if (active.isEmpty()) {
      return new StatsStorePage(List.of(), "");
    }
    StringBuilder next = new StringBuilder();
    List<TargetStatsRecord> rows =
        targetStatsStorage.listByPrefix(
            listPrefix(tableId, snapshotId, active.get().generationId(), targetType),
            Math.max(1, limit),
            pageToken,
            next);
    return new StatsStorePage(rows, next.toString());
  }

  @Override
  public int countTargetStats(
      ResourceId tableId, long snapshotId, Optional<StatsTargetType> targetType) {
    return activeGeneration(tableId, snapshotId)
        .map(
            active ->
                targetStatsStorage.countByPrefix(
                    listPrefix(tableId, snapshotId, active.generationId(), targetType)))
        .orElse(0);
  }

  @Override
  public boolean deleteAllStatsForSnapshot(ResourceId tableId, long snapshotId) {
    activeGeneration(tableId, snapshotId)
        .ifPresent(
            active -> {
              deleteGeneration(
                  active.accountId(), active.tableId(), snapshotId, active.generationId());
              deleteQuietly(() -> blobStore.delete(active.manifestBlobUri()));
              deleteQuietly(
                  () ->
                      pointerStore.compareAndDelete(
                          active.manifestPointerKey(), active.manifestVersion()));
            });
    String generationRoot =
        Keys.snapshotTargetStatsGenerationRootPointer(
            tableId.getAccountId(), tableId.getId(), snapshotId);
    deleteQuietly(() -> pointerStore.deleteByPrefix(generationRoot));
    deleteQuietly(
        () ->
            blobStore.deletePrefix(
                Keys.snapshotTargetStatsBlobPrefix(
                    tableId.getAccountId(), tableId.getId(), snapshotId)));
    deleteQuietly(
        () ->
            pointerStore.delete(
                Keys.snapshotTargetStatsManifestPointer(
                    tableId.getAccountId(), tableId.getId(), snapshotId)));
    return true;
  }

  @Override
  public void replaceAllStatsForSnapshot(
      ResourceId tableId, long snapshotId, List<TargetStatsRecord> records) {
    List<TargetStatsRecord> canonicalRecords =
        (records == null ? List.<TargetStatsRecord>of() : records)
            .stream()
                .map(this::canonicalRecord)
                .peek(record -> requireRecordForSnapshot(tableId, snapshotId, record))
                .toList();
    Optional<ActiveSnapshotStats> current = activeGeneration(tableId, snapshotId);
    String generationId = newGenerationId();

    try {
      for (TargetStatsRecord record : canonicalRecords) {
        targetStatsStorage.create(
            pointerKey(record, generationId), blobUri(record, generationId), record);
      }
      publishActiveGeneration(tableId, snapshotId, generationId, current);
    } catch (RuntimeException e) {
      deleteGeneration(tableId.getAccountId(), tableId.getId(), snapshotId, generationId);
      deleteQuietly(
          () ->
              blobStore.delete(
                  Keys.snapshotTargetStatsManifestBlobUri(
                      tableId.getAccountId(), tableId.getId(), snapshotId, generationId)));
      throw e;
    }

    current.ifPresent(
        active -> {
          deleteGeneration(active.accountId(), active.tableId(), snapshotId, active.generationId());
          deleteQuietly(() -> blobStore.delete(active.manifestBlobUri()));
        });
  }

  @Override
  public MutationMeta metaForTargetStats(
      ResourceId tableId, long snapshotId, StatsTarget target, Timestamp nowTs) {
    ActiveSnapshotStats active =
        activeGeneration(tableId, snapshotId)
            .orElseThrow(
                () ->
                    new BaseResourceRepository.NotFoundException(
                        "No active target stats generation for snapshot " + snapshotId));
    String pointerKey = targetPointerKey(tableId, snapshotId, active.generationId(), target);
    Pointer pointer =
        pointerStore
            .get(pointerKey)
            .orElseThrow(
                () ->
                    new BaseResourceRepository.NotFoundException(
                        "Pointer missing for target-stats: " + pointerKey));
    return targetStatsStorage.metaForPointer(pointerKey, pointer.getBlobUri(), nowTs);
  }

  private TargetStatsRecord canonicalRecord(TargetStatsRecord value) {
    TargetStatsRecord canonical = TargetStatsRecords.canonicalize(value);
    Schemas.TARGET_STATS.keyFromValue.apply(canonical);
    return canonical;
  }

  private void publishActiveGeneration(
      ResourceId tableId,
      long snapshotId,
      String generationId,
      Optional<ActiveSnapshotStats> current) {
    String manifestPointer =
        Keys.snapshotTargetStatsManifestPointer(
            tableId.getAccountId(), tableId.getId(), snapshotId);
    String manifestBlobUri =
        Keys.snapshotTargetStatsManifestBlobUri(
            tableId.getAccountId(), tableId.getId(), snapshotId, generationId);
    StringValue manifest = StringValue.of(generationId);
    targetStatsStorage.putManifestBlob(manifestBlobUri, manifest);

    long expectedVersion = current.map(ActiveSnapshotStats::manifestVersion).orElse(0L);
    Pointer next =
        PointerReferences.blobPointer(manifestPointer, manifestBlobUri, expectedVersion + 1L);
    if (!pointerStore.compareAndSet(manifestPointer, expectedVersion, next)) {
      throw new BaseResourceRepository.AbortRetryableException(
          "active target stats generation update conflicted for snapshot " + snapshotId);
    }
  }

  private Optional<ActiveSnapshotStats> activeGeneration(ResourceId tableId, long snapshotId) {
    String manifestPointer =
        Keys.snapshotTargetStatsManifestPointer(
            tableId.getAccountId(), tableId.getId(), snapshotId);
    return pointerStore
        .get(manifestPointer)
        .map(pointer -> readActiveGeneration(tableId, snapshotId, manifestPointer, pointer));
  }

  private ActiveSnapshotStats ensureActiveGeneration(ResourceId tableId, long snapshotId) {
    Optional<ActiveSnapshotStats> existing = activeGeneration(tableId, snapshotId);
    if (existing.isPresent()) {
      return existing.get();
    }

    String generationId = newGenerationId();
    String manifestPointer =
        Keys.snapshotTargetStatsManifestPointer(
            tableId.getAccountId(), tableId.getId(), snapshotId);
    String manifestBlobUri =
        Keys.snapshotTargetStatsManifestBlobUri(
            tableId.getAccountId(), tableId.getId(), snapshotId, generationId);
    targetStatsStorage.putManifestBlob(manifestBlobUri, StringValue.of(generationId));
    Pointer created = PointerReferences.blobPointer(manifestPointer, manifestBlobUri, 1L);
    if (pointerStore.compareAndSet(manifestPointer, 0L, created)) {
      return new ActiveSnapshotStats(
          tableId.getAccountId(),
          tableId.getId(),
          generationId,
          manifestPointer,
          1L,
          manifestBlobUri);
    }
    deleteQuietly(() -> blobStore.delete(manifestBlobUri));
    return activeGeneration(tableId, snapshotId)
        .orElseThrow(
            () ->
                new BaseResourceRepository.AbortRetryableException(
                    "active target stats generation vanished during create"));
  }

  private ActiveSnapshotStats readActiveGeneration(
      ResourceId tableId, long snapshotId, String manifestPointerKey, Pointer manifestPointer) {
    byte[] bytes = blobStore.get(manifestPointer.getBlobUri());
    try {
      String generationId = StringValue.parseFrom(bytes).getValue();
      if (generationId == null || generationId.isBlank()) {
        throw new BaseResourceRepository.CorruptionException(
            "empty target stats generation manifest for snapshot " + snapshotId, null);
      }
      return new ActiveSnapshotStats(
          tableId.getAccountId(),
          tableId.getId(),
          generationId,
          manifestPointerKey,
          manifestPointer.getVersion(),
          manifestPointer.getBlobUri());
    } catch (Exception e) {
      throw new BaseResourceRepository.CorruptionException(
          "parse failed: " + manifestPointer.getBlobUri(), e);
    }
  }

  private static void requireRecordForSnapshot(
      ResourceId tableId, long snapshotId, TargetStatsRecord record) {
    if (!record.hasTableId()
        || !tableId.getAccountId().equals(record.getTableId().getAccountId())
        || !tableId.getId().equals(record.getTableId().getId())
        || snapshotId != record.getSnapshotId()) {
      throw new IllegalArgumentException(
          "target stats replacement record belongs to a different table snapshot");
    }
  }

  private String listPrefix(
      ResourceId tableId,
      long snapshotId,
      String generationId,
      Optional<StatsTargetType> targetType) {
    return targetType
        .map(
            type ->
                Keys.snapshotTargetColumnStatsGenerationPrefix(
                    tableId.getAccountId(),
                    tableId.getId(),
                    snapshotId,
                    generationId,
                    storagePrefixFor(type)))
        .orElseGet(
            () ->
                Keys.snapshotTargetStatsGenerationPrefix(
                    tableId.getAccountId(), tableId.getId(), snapshotId, generationId));
  }

  private String pointerKey(TargetStatsRecord record, String generationId) {
    return targetPointerKey(
        record.getTableId(), record.getSnapshotId(), generationId, record.getTarget());
  }

  private static String targetPointerKey(
      ResourceId tableId, long snapshotId, String generationId, StatsTarget target) {
    return Keys.snapshotTargetStatsGenerationPointer(
        tableId.getAccountId(),
        tableId.getId(),
        snapshotId,
        generationId,
        StatsTargetIdentity.storageId(target));
  }

  private String blobUri(TargetStatsRecord record, String generationId) {
    return Keys.snapshotTargetStatsBlobUri(
        record.getTableId().getAccountId(),
        record.getTableId().getId(),
        record.getSnapshotId(),
        generationId,
        StatsTargetIdentity.storageId(record.getTarget()),
        Hashing.sha256Hex(record.toByteArray()));
  }

  private void deleteGeneration(
      String accountId, String tableId, long snapshotId, String generationId) {
    deleteQuietly(
        () ->
            pointerStore.deleteByPrefix(
                Keys.snapshotTargetStatsGenerationPrefix(
                    accountId, tableId, snapshotId, generationId)));
    deleteQuietly(
        () ->
            blobStore.deletePrefix(
                Keys.snapshotTargetStatsBlobPrefix(accountId, tableId, snapshotId)
                    + "generations/"
                    + Keys.encodeSegment(generationId)
                    + "/"));
  }

  private static String newGenerationId() {
    return UUID.randomUUID().toString();
  }

  private static String storagePrefixFor(StatsTargetType type) {
    return switch (type) {
      case TABLE -> StatsTargetIdentity.tableStorageIdPrefix();
      case COLUMN -> StatsTargetIdentity.columnStorageIdPrefix();
      case EXPRESSION -> StatsTargetIdentity.expressionStorageIdPrefix();
      case FILE -> StatsTargetIdentity.fileStorageIdPrefix();
    };
  }

  private static void deleteQuietly(Runnable runnable) {
    try {
      runnable.run();
    } catch (Throwable ignore) {
      // ignore
    }
  }

  private record ActiveSnapshotStats(
      String accountId,
      String tableId,
      String generationId,
      String manifestPointerKey,
      long manifestVersion,
      String manifestBlobUri) {}

  private static final class TargetStatsStorage extends BaseResourceRepository<TargetStatsRecord> {

    private TargetStatsStorage(PointerStore pointerStore, BlobStore blobStore) {
      super(
          pointerStore,
          blobStore,
          TargetStatsRecord::parseFrom,
          TargetStatsRecord::toByteArray,
          "application/x-protobuf");
    }

    private Optional<TargetStatsRecord> getByPointer(String pointerKey) {
      return get(pointerKey);
    }

    private void create(String pointerKey, String blobUri, TargetStatsRecord value) {
      putBlob(blobUri, value);
      reserveAllOrRollback(pointerKey, blobUri);
    }

    private boolean createIfAbsent(String pointerKey, String blobUri, TargetStatsRecord value) {
      boolean blobExistedBefore = blobStore.head(blobUri).isPresent();
      putBlob(blobUri, value);
      Pointer reserve = PointerReferences.blobPointer(pointerKey, blobUri, 1L);
      if (!pointerStore.compareAndSet(pointerKey, 0L, reserve)) {
        cleanupCreateIfAbsentBlobOnCasMiss(pointerKey, blobUri, blobExistedBefore);
        return false;
      }
      return true;
    }

    private void putManifestBlob(String blobUri, StringValue manifest) {
      putBlobStrictBytes(blobUri, manifest.toByteArray());
    }

    private MutationMeta metaForPointer(String pointerKey, String blobUri, Timestamp nowTs) {
      return safeMetaOrDefault(pointerKey, blobUri, nowTs);
    }

    private void cleanupCreateIfAbsentBlobOnCasMiss(
        String pointerKey, String blobUri, boolean blobExistedBefore) {
      if (blobExistedBefore || blobUri.isBlank()) {
        return;
      }
      Pointer pointer = pointerStore.get(pointerKey).orElse(null);
      if (pointer != null && blobUri.equals(pointer.getBlobUri())) {
        return;
      }
      try {
        blobStore.delete(blobUri);
      } catch (Throwable ignore) {
        // ignore
      }
    }
  }
}
