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
import ai.floedb.floecat.service.repo.cache.ImmutableBlobCache;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.identity.TargetStatsRecords;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.stats.spi.StatsTargetType;
import ai.floedb.floecat.storage.errors.StorageNotFoundException;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.types.Hashing;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

@ApplicationScoped
public class StatsRepository implements StatsStore {
  private static final int MAX_POINTER_BATCH_SIZE = 100;
  private static final String GENERATION_WRITING = "WRITING";
  private static final String GENERATION_PUBLISHING = "PUBLISHING";
  private static final String GENERATION_PUBLISHED = "PUBLISHED";
  private static final String GENERATION_DELETING = "DELETING";

  private enum GenerationDeleteClaim {
    CLAIMED,
    PUBLISHED,
    IN_PROGRESS
  }

  /**
   * Maximum number of concurrent DynamoDB+S3 reads in a single batch fetch.
   *
   * <p>The AWS SDK HTTP client (Apache) defaults to 50 connections per endpoint. Capping here
   * prevents connection-pool saturation when a single query touches hundreds of columns, which
   * would cause most virtual threads to queue behind the pool and inflate p95 latency. A
   * sliding-window semaphore (rather than chunked batches) lets the next read start the moment any
   * in-flight read completes, so total time ≈ ceil(N / MAX_PARALLEL) × avg_read_ms.
   */
  private static final int MAX_PARALLEL_READS = 50;

  private final PointerStore pointerStore;
  private final BlobStore blobStore;
  private final TargetStatsStorage targetStatsStorage;

  // Nullable (tests): decoded-content cache, used here for the immutable generation-manifest
  // blobs only. Target-stats RECORD blobs are deliberately not cached: they are written to
  // deterministic (not content-addressed) URIs and a re-capture may overwrite one in place, so
  // URI-keyed caching would be unsound for them.
  private final ImmutableBlobCache blobCache;

  public StatsRepository(PointerStore pointerStore, BlobStore blobStore) {
    this(pointerStore, blobStore, null);
  }

  @Inject
  public StatsRepository(
      PointerStore pointerStore, BlobStore blobStore, ImmutableBlobCache blobCache) {
    this.pointerStore = pointerStore;
    this.blobStore = blobStore;
    this.blobCache = blobCache;
    this.targetStatsStorage = new TargetStatsStorage(pointerStore, blobStore);
  }

  /**
   * The generation id inside a generation-manifest blob, decoded once and cached. Sound because a
   * manifest is written once per generation and never rewritten at its URI. ONLY for reads whose
   * freshness is governed elsewhere (the active-generation read follows a LIVE pointer); the
   * frozen-scan path must use {@link #loadGenerationId} directly — its per-page read doubles as the
   * retention guard and a cached value would blind it.
   */
  private Optional<String> readGenerationId(String uri) {
    if (blobCache != null && blobCache.enabled()) {
      return blobCache.get(uri, this::loadGenerationId);
    }
    return loadGenerationId(uri);
  }

  private Optional<String> loadGenerationId(String uri) {
    byte[] bytes;
    try {
      bytes = blobStore.get(uri);
    } catch (StorageNotFoundException e) {
      return Optional.empty();
    } catch (ai.floedb.floecat.storage.errors.StorageAbortRetryableException e) {
      // Map to the repository retryable family like every sibling loader — a throttled read must
      // stay retryable to callers, not surface as an unmapped storage exception.
      throw new BaseResourceRepository.AbortRetryableException(
          "stats generation manifest read retryable: " + uri);
    }
    if (bytes == null) {
      return Optional.empty();
    }
    try {
      return Optional.of(StringValue.parseFrom(bytes).getValue());
    } catch (InvalidProtocolBufferException e) {
      throw new BaseResourceRepository.CorruptionException(
          "unreadable stats generation manifest: " + uri, e);
    }
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
    updateTargetLatestSnapshotIfNewer(
        canonicalRecord.getTableId(), canonicalRecord.getSnapshotId(), canonicalRecord.getTarget());
  }

  @Override
  public void putTargetStatsBatch(
      ResourceId tableId, long snapshotId, List<TargetStatsRecord> records) {
    List<TargetStatsRecord> canonicalRecords =
        (records == null ? List.<TargetStatsRecord>of() : records)
            .stream()
                .filter(java.util.Objects::nonNull)
                .map(this::canonicalRecord)
                .peek(record -> requireRecordForSnapshot(tableId, snapshotId, record))
                .toList();
    if (canonicalRecords.isEmpty()) {
      return;
    }
    ActiveSnapshotStats active = ensureActiveGeneration(tableId, snapshotId);
    List<TargetStatsWrite> writes = new ArrayList<>(canonicalRecords.size());
    for (TargetStatsRecord record : canonicalRecords) {
      writes.add(
          new TargetStatsWrite(
              pointerKey(record, active.generationId()),
              blobUri(record, active.generationId()),
              record));
    }
    targetStatsStorage.createBatch(writes);
    for (TargetStatsRecord record : canonicalRecords) {
      updateTargetLatestSnapshotIfNewer(
          record.getTableId(), record.getSnapshotId(), record.getTarget());
    }
  }

  @Override
  public void replaceTargetStatsInGeneration(
      ResourceId tableId,
      long snapshotId,
      String generationId,
      List<StatsTarget> targetsToReplace,
      List<TargetStatsRecord> records) {
    String effectiveGenerationId = requireGenerationId(generationId);
    List<TargetStatsRecord> canonicalRecords =
        (records == null ? List.<TargetStatsRecord>of() : records)
            .stream()
                .filter(java.util.Objects::nonNull)
                .map(this::canonicalRecord)
                .peek(record -> requireRecordForSnapshot(tableId, snapshotId, record))
                .toList();
    ensureWritableGeneration(tableId, snapshotId, effectiveGenerationId);
    for (StatsTarget target :
        targetsToReplace == null ? List.<StatsTarget>of() : targetsToReplace) {
      if (target != null) {
        pointerStore.delete(targetPointerKey(tableId, snapshotId, effectiveGenerationId, target));
      }
    }
    List<TargetStatsWrite> writes = new ArrayList<>(canonicalRecords.size());
    for (TargetStatsRecord record : canonicalRecords) {
      writes.add(
          new TargetStatsWrite(
              pointerKey(record, effectiveGenerationId),
              blobUri(record, effectiveGenerationId),
              record));
    }
    targetStatsStorage.overwriteBatch(writes);
  }

  @Override
  public void publishStatsGeneration(
      ResourceId tableId,
      long snapshotId,
      String generationId,
      List<TargetStatsRecord> finalRecords) {
    String effectiveGenerationId = requireGenerationId(generationId);
    List<TargetStatsRecord> canonicalRecords =
        (finalRecords == null ? List.<TargetStatsRecord>of() : finalRecords)
            .stream()
                .filter(java.util.Objects::nonNull)
                .map(this::canonicalRecord)
                .peek(record -> requireRecordForSnapshot(tableId, snapshotId, record))
                .toList();
    markGenerationPublishing(tableId, snapshotId, effectiveGenerationId);
    List<TargetStatsWrite> writes = new ArrayList<>(canonicalRecords.size());
    for (TargetStatsRecord record : canonicalRecords) {
      writes.add(
          new TargetStatsWrite(
              pointerKey(record, effectiveGenerationId),
              blobUri(record, effectiveGenerationId),
              record));
    }
    targetStatsStorage.overwriteBatch(writes);
    Optional<ActiveSnapshotStats> current = activeGenerationLive(tableId, snapshotId);
    publishActiveGeneration(tableId, snapshotId, effectiveGenerationId, current);
    for (TargetStatsRecord record :
        listAllInGeneration(tableId, snapshotId, effectiveGenerationId)) {
      updateTargetLatestSnapshotIfNewer(tableId, snapshotId, record.getTarget());
    }
    updateLatestStatsSnapshotIfNewer(tableId, snapshotId);
  }

  @Override
  public UnpublishedGenerationDeleteResult deleteUnpublishedStatsGeneration(
      ResourceId tableId, long snapshotId, String generationId) {
    String effectiveGenerationId = requireGenerationId(generationId);
    String manifestUri =
        Keys.snapshotTargetStatsManifestBlobUri(
            tableId.getAccountId(), tableId.getId(), snapshotId, effectiveGenerationId);
    if (manifestUri.equals(activeStatsGeneration(tableId, snapshotId).orElse(""))) {
      return UnpublishedGenerationDeleteResult.NOT_DELETABLE_PUBLISHED;
    }
    if (blobStore.head(manifestUri).isPresent()) {
      return UnpublishedGenerationDeleteResult.NOT_DELETABLE_PUBLISHED;
    }
    GenerationDeleteClaim deleteClaim =
        markGenerationDeleting(tableId, snapshotId, effectiveGenerationId);
    if (deleteClaim == GenerationDeleteClaim.PUBLISHED) {
      return UnpublishedGenerationDeleteResult.NOT_DELETABLE_PUBLISHED;
    }
    if (deleteClaim == GenerationDeleteClaim.IN_PROGRESS) {
      return UnpublishedGenerationDeleteResult.RETRYABLE_IN_PROGRESS;
    }
    if (manifestUri.equals(activeStatsGeneration(tableId, snapshotId).orElse(""))) {
      throw new BaseResourceRepository.AbortRetryableException(
          "target stats generation became active while cleanup was claiming delete");
    }
    if (blobStore.head(manifestUri).isPresent()) {
      return UnpublishedGenerationDeleteResult.NOT_DELETABLE_PUBLISHED;
    }
    deleteGeneration(tableId.getAccountId(), tableId.getId(), snapshotId, effectiveGenerationId);
    if (manifestUri.equals(activeStatsGeneration(tableId, snapshotId).orElse(""))
        || blobStore.head(manifestUri).isPresent()) {
      throw new BaseResourceRepository.AbortRetryableException(
          "target stats generation publication raced abandoned-generation cleanup");
    }
    return UnpublishedGenerationDeleteResult.DELETED;
  }

  @Override
  public boolean putTargetStatsIfAbsent(TargetStatsRecord value) {
    TargetStatsRecord canonicalRecord = canonicalRecord(value);
    ActiveSnapshotStats active =
        ensureActiveGeneration(canonicalRecord.getTableId(), canonicalRecord.getSnapshotId());
    boolean created =
        targetStatsStorage.createIfAbsent(
            pointerKey(canonicalRecord, active.generationId()),
            blobUri(canonicalRecord, active.generationId()),
            canonicalRecord);
    if (created) {
      updateTargetLatestSnapshotIfNewer(
          canonicalRecord.getTableId(),
          canonicalRecord.getSnapshotId(),
          canonicalRecord.getTarget());
    }
    return created;
  }

  @Override
  public List<TargetStatsRecord> putTargetStatsBatchIfAbsent(
      ResourceId tableId, long snapshotId, List<TargetStatsRecord> records) {
    List<TargetStatsRecord> canonicalRecords =
        (records == null ? List.<TargetStatsRecord>of() : records)
            .stream()
                .filter(java.util.Objects::nonNull)
                .map(this::canonicalRecord)
                .peek(record -> requireRecordForSnapshot(tableId, snapshotId, record))
                .toList();
    if (canonicalRecords.isEmpty()) {
      return List.of();
    }
    ActiveSnapshotStats active = ensureActiveGeneration(tableId, snapshotId);
    List<TargetStatsWrite> writes = new ArrayList<>(canonicalRecords.size());
    for (TargetStatsRecord record : canonicalRecords) {
      writes.add(
          new TargetStatsWrite(
              pointerKey(record, active.generationId()),
              blobUri(record, active.generationId()),
              record));
    }
    List<TargetStatsRecord> created = targetStatsStorage.createBatchIfAbsent(writes);
    for (TargetStatsRecord record : created) {
      updateTargetLatestSnapshotIfNewer(
          record.getTableId(), record.getSnapshotId(), record.getTarget());
    }
    return created;
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

  /**
   * Batch read: resolves the active generation once, then fetches all target stats in parallel
   * using virtual threads.
   *
   * <p>The default {@link StatsStore} implementation calls {@link #getTargetStats} N times
   * sequentially, which re-reads the snapshot manifest on every call. This override:
   *
   * <ol>
   *   <li>Calls {@link #activeGeneration} once per (tableId, snapshotId) — eliminates N−1 redundant
   *       manifest reads (1 DynamoDB GetItem + 1 S3 GetObject per call).
   *   <li>Fetches all N target pointers + stats blobs concurrently via virtual threads — each read
   *       is blocking I/O so virtual threads yield without holding platform threads.
   * </ol>
   *
   * <p>Expected latency improvement: O(N × 0.8ms) → O(1 manifest + max(parallel reads)) ≈ 2ms + 3ms
   * = ~5ms, regardless of N (measured: 425-col TPC-DS: 349ms → ~5ms).
   */
  @Override
  public Map<String, Optional<TargetStatsRecord>> getTargetStatsBatch(
      ResourceId tableId, long snapshotId, List<StatsTarget> targets) {
    if (targets == null || targets.isEmpty()) {
      return Map.of();
    }

    // Resolve active generation ONCE — shared manifest for all targets in this snapshot.
    Optional<ActiveSnapshotStats> activeOpt = activeGeneration(tableId, snapshotId);
    if (activeOpt.isEmpty()) {
      // No stats captured for this snapshot yet — all misses.
      Map<String, Optional<TargetStatsRecord>> out = new LinkedHashMap<>(targets.size());
      for (StatsTarget t : targets) {
        out.put(StatsTargetIdentity.storageId(t), Optional.empty());
      }
      return Collections.unmodifiableMap(out);
    }
    ActiveSnapshotStats active = activeOpt.get();

    // Parallel fetch: one virtual thread per target, bounded by MAX_PARALLEL_READS.
    // The semaphore is a sliding window: as any read completes its slot is immediately
    // available to the next queued thread, minimising total wall-clock time.
    ConcurrentHashMap<String, Optional<TargetStatsRecord>> parallel =
        new ConcurrentHashMap<>(targets.size());
    var semaphore = new Semaphore(Math.min(targets.size(), MAX_PARALLEL_READS));
    try (var exec = Executors.newVirtualThreadPerTaskExecutor()) {
      var futures =
          targets.stream()
              .map(
                  target -> {
                    String key = StatsTargetIdentity.storageId(target);
                    String pKey =
                        targetPointerKey(tableId, snapshotId, active.generationId(), target);
                    return CompletableFuture.runAsync(
                        () -> {
                          semaphore.acquireUninterruptibly();
                          try {
                            parallel.put(key, targetStatsStorage.getByPointer(pKey));
                          } finally {
                            semaphore.release();
                          }
                        },
                        exec);
                  })
              .toList();
      awaitAll(futures);
    }

    // Re-order results to match request order for deterministic output.
    Map<String, Optional<TargetStatsRecord>> out = new LinkedHashMap<>(targets.size());
    for (StatsTarget t : targets) {
      String k = StatsTargetIdentity.storageId(t);
      out.put(k, parallel.getOrDefault(k, Optional.empty()));
    }
    return Collections.unmodifiableMap(out);
  }

  @Override
  public Optional<TargetStatsRecord> getStaleTargetStats(
      ResourceId tableId, long snapshotId, StatsTarget target) {
    // Per-target pointer: O(1), most precise — set on every write for this column.
    OptionalLong targetLatest = findLatestTargetSnapshotId(tableId, snapshotId, target);
    if (targetLatest.isPresent()) {
      Optional<TargetStatsRecord> record =
          getTargetStats(tableId, targetLatest.getAsLong(), target);
      if (record.isPresent()) {
        return record;
      }
      // Pointer present but record deleted — fall through.
    }
    // Table-level pointer: O(1), covers columns where per-target pointer not yet written (legacy).
    OptionalLong staleId = findLatestStatsSnapshotId(tableId, snapshotId);
    if (staleId.isPresent()) {
      Optional<TargetStatsRecord> record = getTargetStats(tableId, staleId.getAsLong(), target);
      if (record.isPresent()) {
        return record;
      }
    }
    // Legacy fallback: prefix scan for data written before either pointer index existed.
    return staleTargetStatsViaScan(tableId, snapshotId, target);
  }

  /**
   * Batch stale lookup. Resolves each target the same way {@link #getStaleTargetStats} does — the
   * per-target pointer first (most precise/newest), then the table-level pointer, then a legacy
   * prefix scan — so the batch and single paths can never disagree on which snapshot serves a
   * target. (They previously could: {@code replaceAllStatsForSnapshot} advances per-target pointers
   * but not the table-level pointer, so a table-level-first batch could return staler stats than a
   * per-target-first single lookup.) The per-target pointer reads run in parallel (bounded by
   * {@link #MAX_PARALLEL_READS}, mirroring {@link #getTargetStatsBatch}) and targets are grouped by
   * resolved snapshot so records are still fetched in batches.
   */
  @Override
  public Map<String, Optional<TargetStatsRecord>> getStaleTargetStatsBatch(
      ResourceId tableId, long snapshotId, List<StatsTarget> targets) {
    if (targets == null || targets.isEmpty()) {
      return Map.of();
    }

    OptionalLong tableLevel = findLatestStatsSnapshotId(tableId, snapshotId);

    // Resolve each target's per-target pointer in parallel — each is a pointer + blob read, and
    // this cold-cache path would otherwise do 2N sequential round-trips.
    ConcurrentHashMap<String, OptionalLong> perTargetSnapshot =
        new ConcurrentHashMap<>(targets.size());
    var semaphore = new Semaphore(Math.min(targets.size(), MAX_PARALLEL_READS));
    try (var exec = Executors.newVirtualThreadPerTaskExecutor()) {
      var futures =
          targets.stream()
              .map(
                  target ->
                      CompletableFuture.runAsync(
                          () -> {
                            semaphore.acquireUninterruptibly();
                            try {
                              perTargetSnapshot.put(
                                  StatsTargetIdentity.storageId(target),
                                  findLatestTargetSnapshotId(tableId, snapshotId, target));
                            } finally {
                              semaphore.release();
                            }
                          },
                          exec))
              .toList();
      awaitAll(futures);
    }

    // Group by resolved snapshot (per-target pointer first, else table-level), preserving order.
    Map<Long, List<StatsTarget>> bySnapshot = new LinkedHashMap<>();
    List<StatsTarget> needScan = new ArrayList<>();
    for (StatsTarget target : targets) {
      OptionalLong resolved =
          perTargetSnapshot.getOrDefault(
              StatsTargetIdentity.storageId(target), OptionalLong.empty());
      if (resolved.isEmpty()) {
        resolved = tableLevel; // per-target pointer not written (legacy) — fall back.
      }
      if (resolved.isPresent()) {
        bySnapshot.computeIfAbsent(resolved.getAsLong(), k -> new ArrayList<>()).add(target);
      } else {
        needScan.add(target);
      }
    }

    Map<String, Optional<TargetStatsRecord>> found = new LinkedHashMap<>(targets.size());
    List<StatsTarget> retryTableLevel = new ArrayList<>();
    for (var group : bySnapshot.entrySet()) {
      long groupSnapshot = group.getKey();
      Map<String, Optional<TargetStatsRecord>> fetched =
          getTargetStatsBatch(tableId, groupSnapshot, group.getValue());
      for (StatsTarget target : group.getValue()) {
        String key = StatsTargetIdentity.storageId(target);
        Optional<TargetStatsRecord> record = fetched.getOrDefault(key, Optional.empty());
        if (record.isPresent()) {
          found.put(key, record);
        } else if (tableLevel.isPresent() && tableLevel.getAsLong() != groupSnapshot) {
          // Per-target pointer resolved but the record was deleted — retry the table-level snapshot
          // (matching single getStaleTargetStats) before falling to a scan.
          retryTableLevel.add(target);
        } else {
          needScan.add(target);
        }
      }
    }
    if (!retryTableLevel.isEmpty()) {
      Map<String, Optional<TargetStatsRecord>> fetched =
          getTargetStatsBatch(tableId, tableLevel.getAsLong(), retryTableLevel);
      for (StatsTarget target : retryTableLevel) {
        String key = StatsTargetIdentity.storageId(target);
        Optional<TargetStatsRecord> record = fetched.getOrDefault(key, Optional.empty());
        if (record.isPresent()) {
          found.put(key, record);
        } else {
          needScan.add(target);
        }
      }
    }
    for (StatsTarget target : needScan) {
      found.put(
          StatsTargetIdentity.storageId(target),
          staleTargetStatsViaScan(tableId, snapshotId, target));
    }

    // Re-order results to match request order for deterministic output.
    Map<String, Optional<TargetStatsRecord>> out = new LinkedHashMap<>(targets.size());
    for (StatsTarget target : targets) {
      String key = StatsTargetIdentity.storageId(target);
      out.put(key, found.getOrDefault(key, Optional.empty()));
    }
    return Collections.unmodifiableMap(out);
  }

  /**
   * O(1) lookup: reads the table-level latest-snapshot pointer and returns its snapshotId when it
   * is ≤ the requested snapshotId. Returns empty when the index has not been written yet
   * (pre-existing data) or when the indexed snapshot is newer than the request.
   */
  private OptionalLong findLatestStatsSnapshotId(ResourceId tableId, long maxSnapshotId) {
    return readLatestSnapshotPointer(
        Keys.tableStatsLatestSnapshotPointer(tableId.getAccountId(), tableId.getId()),
        maxSnapshotId);
  }

  /**
   * O(1) read of a latest-snapshot pointer: returns its snapshotId when ≤ {@code maxSnapshotId}, or
   * empty when the pointer is absent (pre-index data), unreadable, or newer than the request.
   */
  private OptionalLong readLatestSnapshotPointer(String pointerKey, long maxSnapshotId) {
    Optional<Pointer> ptr = pointerStore.get(pointerKey);
    if (ptr.isEmpty()) {
      return OptionalLong.empty();
    }
    try {
      byte[] bytes = blobStore.get(ptr.get().getBlobUri());
      long latestId = Long.parseLong(StringValue.parseFrom(bytes).getValue());
      return latestId <= maxSnapshotId ? OptionalLong.of(latestId) : OptionalLong.empty();
    } catch (Exception e) {
      return OptionalLong.empty();
    }
  }

  /**
   * Advances the table-level latest-snapshot pointer to {@code snapshotId} when it is newer than
   * the current value. Retries the read-CAS loop until either the pointer is already at a {@code >=
   * snapshotId} (another writer advanced it) or our CAS succeeds. Without the retry a
   * higher-snapshotId writer can lose CAS to a lower-snapshotId writer and leave the pointer stale.
   */
  private void updateLatestStatsSnapshotIfNewer(ResourceId tableId, long snapshotId) {
    advanceLatestSnapshotPointer(
        Keys.tableStatsLatestSnapshotPointer(tableId.getAccountId(), tableId.getId()),
        Keys.tableStatsLatestSnapshotBlobUri(tableId.getAccountId(), tableId.getId(), snapshotId),
        snapshotId);
  }

  /**
   * Advances a latest-snapshot pointer to {@code snapshotId} when it is newer than the current
   * value. Retries the read-CAS loop until the pointer is already at {@code >= snapshotId} (another
   * writer advanced it) or our CAS succeeds. Without the retry a higher-snapshotId writer can lose
   * CAS to a lower-snapshotId writer and leave the pointer stale.
   */
  private void advanceLatestSnapshotPointer(String pointerKey, String blobUri, long snapshotId) {
    boolean blobWritten = false;
    for (int attempt = 0; attempt < BaseResourceRepository.CAS_MAX; attempt++) {
      Optional<Pointer> existing = pointerStore.get(pointerKey);
      long currentId = 0L;
      if (existing.isPresent()) {
        try {
          byte[] bytes = blobStore.get(existing.get().getBlobUri());
          currentId = Long.parseLong(StringValue.parseFrom(bytes).getValue());
        } catch (Exception ignored) {
          // Treat corrupt/missing blob as currentId=0 so we overwrite it.
        }
      }
      if (snapshotId <= currentId) {
        return; // Pointer already at a >= snapshotId — nothing to do.
      }
      if (!blobWritten) {
        blobStore.put(
            blobUri,
            StringValue.of(Long.toString(snapshotId)).toByteArray(),
            "application/x-protobuf");
        blobWritten = true;
      }
      long expectedVersion = existing.map(Pointer::getVersion).orElse(0L);
      Pointer next = PointerReferences.blobPointer(pointerKey, blobUri, expectedVersion + 1L);
      if (pointerStore.compareAndSet(pointerKey, expectedVersion, next)) {
        return; // CAS succeeded — pointer advanced.
      }
      // CAS lost to a concurrent writer. Re-read: if they advanced to >= snapshotId we're done;
      // if they advanced to < snapshotId we retry to ensure our higher value eventually wins.
    }
    // Bounded like the other CAS loops (BaseResourceRepository.CAS_MAX): surface sustained
    // contention as a retryable abort rather than spinning forever.
    throw new BaseResourceRepository.AbortRetryableException(
        "exhausted CAS attempts advancing latest-snapshot pointer " + pointerKey);
  }

  /**
   * Advances the per-target latest-snapshot pointer to {@code snapshotId} when it is newer than the
   * current value. Same CAS-retry loop as {@link #updateLatestStatsSnapshotIfNewer} but keyed per
   * column, enabling O(1) stale lookups for targets absent from the table-level snapshot.
   */
  private void updateTargetLatestSnapshotIfNewer(
      ResourceId tableId, long snapshotId, StatsTarget target) {
    String storageId = StatsTargetIdentity.storageId(target);
    advanceLatestSnapshotPointer(
        Keys.targetStatsLatestSnapshotPointer(tableId.getAccountId(), tableId.getId(), storageId),
        Keys.targetStatsLatestSnapshotBlobUri(
            tableId.getAccountId(), tableId.getId(), storageId, snapshotId),
        snapshotId);
  }

  /**
   * O(1) lookup: reads the per-target latest-snapshot pointer and returns its snapshotId when it is
   * ≤ the requested snapshotId. Returns empty when no pointer exists (pre-existing data) or when
   * the indexed snapshot is newer than the request.
   */
  private OptionalLong findLatestTargetSnapshotId(
      ResourceId tableId, long maxSnapshotId, StatsTarget target) {
    String storageId = StatsTargetIdentity.storageId(target);
    return readLatestSnapshotPointer(
        Keys.targetStatsLatestSnapshotPointer(tableId.getAccountId(), tableId.getId(), storageId),
        maxSnapshotId);
  }

  /** Full prefix scan fallback for tables written before the latest-snapshot index existed. */
  private Optional<TargetStatsRecord> staleTargetStatsViaScan(
      ResourceId tableId, long snapshotId, StatsTarget target) {
    String prefix = Keys.snapshotRootPrefix(tableId.getAccountId(), tableId.getId());
    List<Long> candidateSnapshotIds = new ArrayList<>();
    String pageToken = "";
    do {
      StringBuilder nextToken = new StringBuilder();
      List<Pointer> pointers =
          pointerStore.listPointersByPrefix(prefix, 1_000, pageToken, nextToken);
      for (Pointer pointer : pointers) {
        OptionalLong candidate = parseSnapshotIdFromStatsManifestPointer(prefix, pointer.getKey());
        if (candidate.isPresent() && candidate.getAsLong() <= snapshotId) {
          candidateSnapshotIds.add(candidate.getAsLong());
        }
      }
      pageToken = nextToken.toString();
    } while (!pageToken.isBlank());

    candidateSnapshotIds.sort(Comparator.reverseOrder());
    for (long candidateSnapshotId : candidateSnapshotIds) {
      Optional<TargetStatsRecord> record = getTargetStats(tableId, candidateSnapshotId, target);
      if (record.isPresent()) {
        return record;
      }
    }
    return Optional.empty();
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
    return listInGeneration(
        tableId, snapshotId, active.get().generationId(), targetType, limit, pageToken);
  }

  /**
   * Generation-scoped list: the token is the generation manifest blob URI captured from {@link
   * #activeStatsGeneration}; its immutable blob names the generation whose keyspace is read. A
   * missing manifest is a broken retention invariant (frozen generations are retained while
   * referenced) and fails loudly rather than falling back to the live generation.
   */
  @Override
  public StatsStorePage listTargetStatsInGeneration(
      ResourceId tableId,
      long snapshotId,
      String generationToken,
      Optional<StatsTargetType> targetType,
      int limit,
      String pageToken) {
    // A missing frozen manifest is the broken-retention invariant this wants to surface loudly —
    // this per-page read IS the scan's retention guard, so it deliberately BYPASSES the decoded
    // cache: a cached generation id would keep a scan paging "successfully" over a reclaimed
    // generation (empty pages = silently truncated results) for the cache's lifetime, exactly when
    // the guard must fire. The write-through/cached path serves the active-generation read below,
    // whose freshness is governed by its live pointer instead.
    String generationId =
        loadGenerationId(generationToken)
            .orElseThrow(
                () ->
                    new BaseResourceRepository.NotFoundException(
                        "frozen stats generation manifest missing for snapshot "
                            + snapshotId
                            + ": "
                            + generationToken));
    return listInGeneration(tableId, snapshotId, generationId, targetType, limit, pageToken);
  }

  private StatsStorePage listInGeneration(
      ResourceId tableId,
      long snapshotId,
      String generationId,
      Optional<StatsTargetType> targetType,
      int limit,
      String pageToken) {
    StringBuilder next = new StringBuilder();
    List<TargetStatsRecord> rows =
        targetStatsStorage.listByPrefix(
            listPrefix(tableId, snapshotId, generationId, targetType),
            Math.max(1, limit),
            pageToken,
            next);
    return new StatsStorePage(rows, next.toString());
  }

  private List<TargetStatsRecord> listAllInGeneration(
      ResourceId tableId, long snapshotId, String generationId) {
    List<TargetStatsRecord> out = new ArrayList<>();
    String pageToken = "";
    do {
      StatsStorePage page =
          listInGeneration(tableId, snapshotId, generationId, Optional.empty(), 500, pageToken);
      out.addAll(page.records());
      pageToken = page.nextPageToken();
    } while (pageToken != null && !pageToken.isBlank());
    return List.copyOf(out);
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
    Optional<ActiveSnapshotStats> active = activeGenerationLive(tableId, snapshotId);
    active.ifPresent(
        gen -> {
          deleteGeneration(gen.accountId(), gen.tableId(), snapshotId, gen.generationId());
          deleteQuietly(() -> blobStore.delete(gen.manifestBlobUri()));
          deleteQuietly(
              () -> pointerStore.compareAndDelete(gen.manifestPointerKey(), gen.manifestVersion()));
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
    // Per the StatsStore contract, report whether there was a live generation to delete.
    return active.isPresent();
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
    Optional<ActiveSnapshotStats> current = activeGenerationLive(tableId, snapshotId);
    String generationId = newGenerationId();

    try {
      for (TargetStatsRecord record : canonicalRecords) {
        targetStatsStorage.create(
            pointerKey(record, generationId), blobUri(record, generationId), record);
        updateTargetLatestSnapshotIfNewer(tableId, snapshotId, record.getTarget());
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

    // The superseded generation is deliberately NOT deleted here: queries freeze their generation
    // and keep reading its immutable keyspace to completion, so stats stay deterministic at a
    // given pointer with no per-page guard. GC collects a generation once no retained table root
    // and no live query references it.
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
    markGenerationPublishing(tableId, snapshotId, generationId);
    StringValue manifest = StringValue.of(generationId);
    targetStatsStorage.putManifestBlob(manifestBlobUri, manifest);
    if (blobCache != null) {
      // Write-through the DECODED form readGenerationId caches: the first scan/planner read after
      // this publish pays neither a cold fetch nor a parse (URI is per-generation, immutable).
      blobCache.put(manifestBlobUri, generationId);
    }
    long expectedVersion = current.map(ActiveSnapshotStats::manifestVersion).orElse(0L);
    Pointer next =
        PointerReferences.blobPointer(manifestPointer, manifestBlobUri, expectedVersion + 1L);
    if (!pointerStore.compareAndSet(manifestPointer, expectedVersion, next)) {
      throw new BaseResourceRepository.AbortRetryableException(
          "active target stats generation update conflicted for snapshot " + snapshotId);
    }
    markGenerationPublished(tableId, snapshotId, generationId);
  }

  private void ensureWritableGeneration(ResourceId tableId, long snapshotId, String generationId) {
    String lifecyclePointer = generationLifecyclePointer(tableId, snapshotId, generationId);
    for (int attempt = 0; attempt < 8; attempt++) {
      Pointer current = pointerStore.get(lifecyclePointer).orElse(null);
      if (current != null) {
        String state = blankToEmpty(current.getBlobUri());
        if (GENERATION_WRITING.equals(state)) {
          return;
        }
        throw new BaseResourceRepository.AbortRetryableException(
            "target stats generation is not writable: " + generationId + " state=" + state);
      }
      Pointer next =
          PointerReferences.opaqueMarkerPointer(lifecyclePointer, GENERATION_WRITING, 1L);
      if (pointerStore.compareAndSet(lifecyclePointer, 0L, next)) {
        return;
      }
    }
    throw new BaseResourceRepository.AbortRetryableException(
        "target stats generation lifecycle update conflicted: " + generationId);
  }

  private void markGenerationPublishing(ResourceId tableId, long snapshotId, String generationId) {
    String lifecyclePointer = generationLifecyclePointer(tableId, snapshotId, generationId);
    for (int attempt = 0; attempt < 8; attempt++) {
      Pointer current = pointerStore.get(lifecyclePointer).orElse(null);
      long expectedVersion = current == null ? 0L : current.getVersion();
      String state = current == null ? "" : blankToEmpty(current.getBlobUri());
      if (GENERATION_PUBLISHING.equals(state)) {
        return;
      }
      if (GENERATION_PUBLISHED.equals(state)) {
        return;
      }
      if (!state.isBlank() && !GENERATION_WRITING.equals(state)) {
        throw new BaseResourceRepository.AbortRetryableException(
            "target stats generation cannot start publishing: " + generationId + " state=" + state);
      }
      Pointer next =
          PointerReferences.opaqueMarkerPointer(
              lifecyclePointer, GENERATION_PUBLISHING, expectedVersion + 1L);
      if (pointerStore.compareAndSet(lifecyclePointer, expectedVersion, next)) {
        return;
      }
    }
    throw new BaseResourceRepository.AbortRetryableException(
        "target stats generation publishing lifecycle update conflicted: " + generationId);
  }

  private void markGenerationPublished(ResourceId tableId, long snapshotId, String generationId) {
    String lifecyclePointer = generationLifecyclePointer(tableId, snapshotId, generationId);
    for (int attempt = 0; attempt < 8; attempt++) {
      Pointer current = pointerStore.get(lifecyclePointer).orElse(null);
      long expectedVersion = current == null ? 0L : current.getVersion();
      String state = current == null ? "" : blankToEmpty(current.getBlobUri());
      if (GENERATION_PUBLISHED.equals(state)) {
        return;
      }
      if (!state.isBlank() && !GENERATION_PUBLISHING.equals(state)) {
        throw new BaseResourceRepository.AbortRetryableException(
            "target stats generation cannot finish publishing: "
                + generationId
                + " state="
                + state);
      }
      Pointer next =
          PointerReferences.opaqueMarkerPointer(
              lifecyclePointer, GENERATION_PUBLISHED, expectedVersion + 1L);
      if (pointerStore.compareAndSet(lifecyclePointer, expectedVersion, next)) {
        return;
      }
    }
    throw new BaseResourceRepository.AbortRetryableException(
        "target stats generation published lifecycle update conflicted: " + generationId);
  }

  private GenerationDeleteClaim markGenerationDeleting(
      ResourceId tableId, long snapshotId, String generationId) {
    String lifecyclePointer = generationLifecyclePointer(tableId, snapshotId, generationId);
    for (int attempt = 0; attempt < 8; attempt++) {
      Pointer current = pointerStore.get(lifecyclePointer).orElse(null);
      long expectedVersion = current == null ? 0L : current.getVersion();
      String state = current == null ? "" : blankToEmpty(current.getBlobUri());
      if (GENERATION_PUBLISHED.equals(state)) {
        return GenerationDeleteClaim.PUBLISHED;
      }
      if (GENERATION_PUBLISHING.equals(state)) {
        return GenerationDeleteClaim.IN_PROGRESS;
      }
      if (GENERATION_DELETING.equals(state)) {
        return GenerationDeleteClaim.CLAIMED;
      }
      if (!state.isBlank() && !GENERATION_WRITING.equals(state)) {
        return GenerationDeleteClaim.IN_PROGRESS;
      }
      Pointer next =
          PointerReferences.opaqueMarkerPointer(
              lifecyclePointer, GENERATION_DELETING, expectedVersion + 1L);
      if (pointerStore.compareAndSet(lifecyclePointer, expectedVersion, next)) {
        return GenerationDeleteClaim.CLAIMED;
      }
    }
    throw new BaseResourceRepository.AbortRetryableException(
        "target stats generation delete lifecycle update conflicted: " + generationId);
  }

  private static String generationLifecyclePointer(
      ResourceId tableId, long snapshotId, String generationId) {
    return Keys.snapshotTargetStatsGenerationLifecyclePointer(
        tableId.getAccountId(), tableId.getId(), snapshotId, generationId);
  }

  @Override
  public boolean tracksStatsGenerations() {
    return true;
  }

  /**
   * The active generation's manifest blob URI serves as the opaque token: it embeds the generation
   * id, and both replaceAllStatsForSnapshot (pointer swap to a new manifest) and
   * deleteAllStatsForSnapshot (pointer removal) change it. One pointer read, no blob fetch. Empty
   * means this snapshot has no generation yet (a comparable state, per the SPI contract), never
   * "cannot say" — hence {@link #tracksStatsGenerations()} is true.
   */
  @Override
  public Optional<String> activeStatsGeneration(ResourceId tableId, long snapshotId) {
    String manifestPointer =
        Keys.snapshotTargetStatsManifestPointer(
            tableId.getAccountId(), tableId.getId(), snapshotId);
    return pointerStore.get(manifestPointer).map(Pointer::getBlobUri);
  }

  private Optional<ActiveSnapshotStats> activeGeneration(ResourceId tableId, long snapshotId) {
    return activeGeneration(tableId, snapshotId, false);
  }

  /**
   * WRITE-funnel variant: the manifest decode is read LIVE, so the funnel's view of the active
   * generation cannot be a resident decode of a deleted manifest — mutations must observe (and fail
   * on) the store's true state, per the commit-funnel-reads-live rule.
   */
  private Optional<ActiveSnapshotStats> activeGenerationLive(ResourceId tableId, long snapshotId) {
    return activeGeneration(tableId, snapshotId, true);
  }

  private Optional<ActiveSnapshotStats> activeGeneration(
      ResourceId tableId, long snapshotId, boolean live) {
    String manifestPointer =
        Keys.snapshotTargetStatsManifestPointer(
            tableId.getAccountId(), tableId.getId(), snapshotId);
    return pointerStore
        .get(manifestPointer)
        .map(pointer -> readActiveGeneration(tableId, snapshotId, manifestPointer, pointer, live));
  }

  private ActiveSnapshotStats ensureActiveGeneration(ResourceId tableId, long snapshotId) {
    Optional<ActiveSnapshotStats> existing = activeGenerationLive(tableId, snapshotId);
    if (existing.isPresent()) {
      // Generation already exists — advance the latest-snapshot index in case this snapshot
      // is newer than what the index currently records (covers first write after a CAS race).
      updateLatestStatsSnapshotIfNewer(tableId, snapshotId);
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
      updateLatestStatsSnapshotIfNewer(tableId, snapshotId);
      return new ActiveSnapshotStats(
          tableId.getAccountId(),
          tableId.getId(),
          generationId,
          manifestPointer,
          1L,
          manifestBlobUri);
    }
    deleteQuietly(() -> blobStore.delete(manifestBlobUri));
    ActiveSnapshotStats resolved =
        activeGeneration(tableId, snapshotId)
            .orElseThrow(
                () ->
                    new BaseResourceRepository.AbortRetryableException(
                        "active target stats generation vanished during create"));
    updateLatestStatsSnapshotIfNewer(tableId, snapshotId);
    return resolved;
  }

  private ActiveSnapshotStats readActiveGeneration(
      ResourceId tableId,
      long snapshotId,
      String manifestPointerKey,
      Pointer manifestPointer,
      boolean live) {
    String generationId =
        (live
                ? loadGenerationId(manifestPointer.getBlobUri())
                : readGenerationId(manifestPointer.getBlobUri()))
            .orElse(null);
    if (generationId == null || generationId.isBlank()) {
      // A manifest missing or empty UNDER A LIVE POINTER is a broken invariant, not client state.
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

  private static OptionalLong parseSnapshotIdFromStatsManifestPointer(
      String snapshotRootPrefix, String key) {
    if (key == null || !key.startsWith(snapshotRootPrefix)) {
      return OptionalLong.empty();
    }
    int snapshotIdStart = snapshotRootPrefix.length();
    int snapshotIdEnd = snapshotIdStart + 19;
    if (key.length() <= snapshotIdEnd) {
      return OptionalLong.empty();
    }
    if (!key.substring(snapshotIdEnd).equals("/stats/targets-active")) {
      return OptionalLong.empty();
    }
    try {
      return OptionalLong.of(Long.parseLong(key.substring(snapshotIdStart, snapshotIdEnd)));
    } catch (NumberFormatException e) {
      return OptionalLong.empty();
    }
  }

  private String blobUri(TargetStatsRecord record, String generationId) {
    return Keys.snapshotTargetStatsBlobUri(
        record.getTableId().getAccountId(),
        record.getTableId().getId(),
        record.getSnapshotId(),
        generationId,
        StatsTargetIdentity.storageId(record.getTarget()),
        Hashing.sha256Hex(TargetStatsRecords.contentHashImage(record).toByteArray()));
  }

  /**
   * GC hook: reclaim this table's superseded stats generations. A generation survives while any of
   * these hold — its manifest blob URI is protected (referenced by a retained or pinned table root,
   * or frozen by a live scan stream), it is the snapshot's LIVE active generation (the
   * creation-window safeguard: a just-activated generation whose root commit has not landed), its
   * manifest blob does not exist yet (an in-flight replace writes records before publishing), or
   * its manifest blob is younger than {@code minAgeMs} — the publish→flip window: the manifest is
   * written BEFORE the active pointer flips and before the root commit references it, so during
   * that instant a brand-new generation is neither live nor rooted and only its age protects it
   * (the same guard the blob sweep applies). Everything else — record pointers, record blobs, and
   * the manifest blob — is deleted. Returns the number of generations reclaimed.
   */
  public int deleteUnreferencedGenerations(
      ResourceId tableId,
      java.util.function.Predicate<String> isProtectedManifestUri,
      long nowMs,
      long minAgeMs) {
    String accountId = tableId.getAccountId();
    String prefix = Keys.snapshotRootPrefix(accountId, tableId.getId());
    var candidates = new java.util.LinkedHashSet<Keys.GenerationKey>();
    String token = "";
    while (true) {
      StringBuilder next = new StringBuilder();
      List<Pointer> page = pointerStore.listPointersByPrefix(prefix, 500, token, next);
      for (Pointer pointer : page) {
        Keys.GenerationKey generation = Keys.generationFromTargetPointerKey(pointer.getKey());
        if (generation != null) {
          candidates.add(generation);
        }
      }
      token = next.toString();
      if (token.isBlank()) {
        break;
      }
    }

    int reclaimed = 0;
    for (Keys.GenerationKey candidate : candidates) {
      long snapshotId = candidate.snapshotId();
      String generationId = candidate.generationId();
      String manifestUri =
          Keys.snapshotTargetStatsManifestBlobUri(
              accountId, tableId.getId(), snapshotId, generationId);
      // One HEAD answers both existence and age: absent means unpublished (an in-flight replace
      // writes records before publishing) or already reclaimed — not ours to touch.
      var header = blobStore.head(manifestUri).orElse(null);
      if (header == null) {
        continue;
      }
      if (nowMs - com.google.protobuf.util.Timestamps.toMillis(header.getLastModifiedAt())
          < minAgeMs) {
        // publish->flip window: too young to be provably unreferenced. Runs UNCONDITIONALLY, not
        // only when min-age > 0 — matching the CAS blob sweep. nowMs is frozen at pass start, so a
        // generation whose manifest was published mid-sweep has lastModified STRICTLY later than
        // nowMs (negative age, below any min-age including 0) and is fenced; without this,
        // min-age=0
        // would let GC delete a generation out from under an in-flight replace/first publish. (The
        // exact-tie lastModified == nowMs is eligible at min-age=0, but is unreachable — nowMs is
        // stamped before any manifest the sweep could race — so the fence is exact for min-age >
        // 0.)
        continue;
      }
      if (isProtectedManifestUri.test(manifestUri)) {
        continue;
      }
      String liveActive = activeStatsGeneration(tableId, snapshotId).orElse("");
      if (manifestUri.equals(liveActive)) {
        continue; // creation-window safeguard: active pointer target survives regardless of roots
      }
      deleteGeneration(accountId, tableId.getId(), snapshotId, generationId);
      deleteQuietly(() -> blobStore.delete(manifestUri));
      reclaimed++;
    }
    return reclaimed;
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

  private static String requireGenerationId(String generationId) {
    String effective = generationId == null ? "" : generationId.trim();
    if (effective.isBlank()) {
      throw new IllegalArgumentException("stats generation id is required");
    }
    return effective;
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value.trim();
  }

  private static String storagePrefixFor(StatsTargetType type) {
    return switch (type) {
      case TABLE -> StatsTargetIdentity.tableStorageIdPrefix();
      case COLUMN -> StatsTargetIdentity.columnStorageIdPrefix();
      case EXPRESSION -> StatsTargetIdentity.expressionStorageIdPrefix();
      case FILE -> StatsTargetIdentity.fileStorageIdPrefix();
      case COMPOSITE -> StatsTargetIdentity.compositeStorageIdPrefix();
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

  private record TargetStatsWrite(String pointerKey, String blobUri, TargetStatsRecord value) {}

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

    private void createBatch(List<TargetStatsWrite> writes) {
      if (writes == null || writes.isEmpty()) {
        return;
      }
      Map<String, TargetStatsWrite> uniqueWrites = new LinkedHashMap<>();
      for (TargetStatsWrite write : writes) {
        TargetStatsWrite existing = uniqueWrites.putIfAbsent(write.pointerKey(), write);
        if (existing != null && !existing.blobUri().equals(write.blobUri())) {
          throw new NameConflictException("pointer bound to different blob: " + write.pointerKey());
        }
      }
      List<TargetStatsWrite> pending = new ArrayList<>(uniqueWrites.values());
      for (TargetStatsWrite write : pending) {
        putBlob(write.blobUri(), write.value());
      }
      for (int from = 0; from < pending.size(); from += MAX_POINTER_BATCH_SIZE) {
        reserveBatchOrClassify(
            pending.subList(from, Math.min(from + MAX_POINTER_BATCH_SIZE, pending.size())));
      }
    }

    private void overwriteBatch(List<TargetStatsWrite> writes) {
      if (writes == null || writes.isEmpty()) {
        return;
      }
      Map<String, TargetStatsWrite> uniqueWrites = new LinkedHashMap<>();
      for (TargetStatsWrite write : writes) {
        uniqueWrites.put(write.pointerKey(), write);
      }
      for (TargetStatsWrite write : uniqueWrites.values()) {
        overwrite(write.pointerKey(), write.blobUri(), write.value());
      }
    }

    private void overwrite(String pointerKey, String blobUri, TargetStatsRecord value) {
      putBlob(blobUri, value);
      for (int attempt = 0; attempt < CAS_MAX; attempt++) {
        Pointer existing = pointerStore.get(pointerKey).orElse(null);
        long expectedVersion = existing == null ? 0L : existing.getVersion();
        if (existing != null && blobUri.equals(existing.getBlobUri())) {
          return;
        }
        Pointer next =
            PointerReferences.blobPointer(pointerKey, blobUri, Math.max(1L, expectedVersion + 1L));
        if (pointerStore.compareAndSet(pointerKey, expectedVersion, next)) {
          return;
        }
      }
      throw new AbortRetryableException("overwrite conflict: " + pointerKey);
    }

    private boolean createIfAbsent(String pointerKey, String blobUri, TargetStatsRecord value) {
      // The target pointer is bound, so ifAbsent must leave the existing record untouched. Now
      // that content-hash images can map distinct records to one blobUri (timestamp-only resubmits
      // share a blob), writing the blob before this check would overwrite the live record's bytes
      // and still return false on the CAS miss. Check the pointer first and write nothing.
      if (pointerStore.get(pointerKey).isPresent()) {
        return false;
      }
      boolean blobExistedBefore = blobStore.head(blobUri).isPresent();
      putBlob(blobUri, value);
      Pointer reserve = PointerReferences.blobPointer(pointerKey, blobUri, 1L);
      if (!pointerStore.compareAndSet(pointerKey, 0L, reserve)) {
        cleanupCreateIfAbsentBlobOnCasMiss(pointerKey, blobUri, blobExistedBefore);
        return false;
      }
      return true;
    }

    private List<TargetStatsRecord> createBatchIfAbsent(List<TargetStatsWrite> writes) {
      if (writes == null || writes.isEmpty()) {
        return List.of();
      }
      Map<String, TargetStatsWrite> uniqueWrites = new LinkedHashMap<>();
      for (TargetStatsWrite write : writes) {
        TargetStatsWrite existing = uniqueWrites.putIfAbsent(write.pointerKey(), write);
        if (existing != null && !existing.blobUri().equals(write.blobUri())) {
          throw new NameConflictException("pointer bound to different blob: " + write.pointerKey());
        }
      }
      List<TargetStatsWrite> remaining = new ArrayList<>(uniqueWrites.values());
      List<TargetStatsRecord> created = new ArrayList<>(remaining.size());
      while (!remaining.isEmpty()) {
        List<TargetStatsWrite> absent = new ArrayList<>(remaining.size());
        for (TargetStatsWrite write : remaining) {
          if (pointerStore.get(write.pointerKey()).isEmpty()) {
            absent.add(write);
          }
        }
        if (absent.isEmpty()) {
          break;
        }
        boolean[] blobExistedBefore = new boolean[absent.size()];
        for (int i = 0; i < absent.size(); i++) {
          TargetStatsWrite write = absent.get(i);
          blobExistedBefore[i] = blobStore.head(write.blobUri()).isPresent();
          putBlob(write.blobUri(), write.value());
        }
        List<TargetStatsWrite> nextRemaining = new ArrayList<>();
        for (int from = 0; from < absent.size(); from += MAX_POINTER_BATCH_SIZE) {
          List<TargetStatsWrite> batch =
              absent.subList(from, Math.min(from + MAX_POINTER_BATCH_SIZE, absent.size()));
          if (reserveIfAbsentBatch(batch)) {
            batch.forEach(write -> created.add(write.value()));
            continue;
          }
          for (int offset = 0; offset < batch.size(); offset++) {
            TargetStatsWrite write = batch.get(offset);
            Pointer pointer = pointerStore.get(write.pointerKey()).orElse(null);
            if (pointer == null) {
              nextRemaining.add(write);
              continue;
            }
            int originalIndex = from + offset;
            if (!blobExistedBefore[originalIndex]
                && !write.blobUri().equals(pointer.getBlobUri())) {
              cleanupCreateIfAbsentBlobOnCasMiss(
                  write.pointerKey(), write.blobUri(), blobExistedBefore[originalIndex]);
            }
          }
        }
        if (nextRemaining.size() == absent.size()) {
          throw new AbortRetryableException(
              "create conflict, no pointer present: " + absent.get(0).pointerKey());
        }
        remaining = nextRemaining;
      }
      return List.copyOf(created);
    }

    private void putManifestBlob(String blobUri, StringValue manifest) {
      putBlobStrictBytes(blobUri, manifest.toByteArray());
    }

    private MutationMeta metaForPointer(String pointerKey, String blobUri, Timestamp nowTs) {
      return safeMetaOrDefault(pointerKey, blobUri, nowTs);
    }

    private void reserveBatchOrClassify(List<TargetStatsWrite> writes) {
      List<TargetStatsWrite> remaining = new ArrayList<>(writes);
      while (!remaining.isEmpty()) {
        List<PointerStore.CasOp> ops = new ArrayList<>(remaining.size());
        for (TargetStatsWrite write : remaining) {
          ops.add(
              new PointerStore.CasUpsert(
                  write.pointerKey(),
                  0L,
                  PointerReferences.blobPointer(write.pointerKey(), write.blobUri(), 1L)));
        }
        if (pointerStore.compareAndSetBatch(ops)) {
          return;
        }
        List<TargetStatsWrite> nextRemaining = new ArrayList<>();
        for (TargetStatsWrite write : remaining) {
          Pointer pointer = pointerStore.get(write.pointerKey()).orElse(null);
          if (pointer == null) {
            nextRemaining.add(write);
            continue;
          }
          if (!write.blobUri().equals(pointer.getBlobUri())) {
            throw new NameConflictException(
                "pointer bound to different blob: " + write.pointerKey());
          }
        }
        if (nextRemaining.size() == remaining.size()) {
          throw new AbortRetryableException(
              "create conflict, no pointer present: " + remaining.get(0).pointerKey());
        }
        remaining = nextRemaining;
      }
    }

    private boolean reserveIfAbsentBatch(List<TargetStatsWrite> writes) {
      List<PointerStore.CasOp> ops = new ArrayList<>(writes.size());
      for (TargetStatsWrite write : writes) {
        ops.add(
            new PointerStore.CasUpsert(
                write.pointerKey(),
                0L,
                PointerReferences.blobPointer(write.pointerKey(), write.blobUri(), 1L)));
      }
      return pointerStore.compareAndSetBatch(ops);
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

  /**
   * Waits for every parallel read, rethrowing the first failure with its ORIGINAL type: {@code
   * allOf(...).join()} wraps causes in {@link CompletionException}, which would defeat the
   * instanceof-keyed gRPC error mapping the sequential paths feed — retryable faults, not-found and
   * corruption would all collapse into a generic INTERNAL on the batch paths only.
   */
  private static void awaitAll(List<? extends CompletableFuture<?>> futures) {
    try {
      CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
    } catch (CompletionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException runtime) {
        throw runtime;
      }
      if (cause instanceof Error error) {
        throw error;
      }
      throw e;
    }
  }
}
