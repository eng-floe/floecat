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
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.identity.TargetStatsRecords;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.stats.spi.StatsTargetType;
import ai.floedb.floecat.storage.errors.StorageAbortRetryableException;
import ai.floedb.floecat.storage.errors.StorageNotFoundException;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.types.Hashing;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HexFormat;
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
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.eclipse.microprofile.config.ConfigProvider;

@ApplicationScoped
public class StatsRepository implements StatsStore {
  private static final String REUSE_SOURCE_FINGERPRINT_PROPERTY =
      "floedb.reconcile.source-fingerprint-v1";
  private static final String REUSE_STATS_SIGNATURE_PROPERTY =
      "floedb.reconcile.stats-signature-v1";
  private static final int MAX_POINTER_BATCH_SIZE = 100;
  private static final String GENERATION_WRITING = "WRITING";
  private static final String GENERATION_PUBLISHING = "PUBLISHING";
  private static final String GENERATION_PUBLISHED = "PUBLISHED";
  private static final String GENERATION_DELETING = "DELETING";
  private static final String GENERATION_DELETED = "DELETED";
  private static final long DEFAULT_DELETED_GENERATION_FENCE_RETENTION_MS =
      7L * 24L * 60L * 60L * 1000L;

  private enum GenerationDeleteClaim {
    CLAIMED,
    PUBLISHED,
    IN_PROGRESS
  }

  public record GenerationGcResult(
      int generationsReclaimed, int blobDeleteAttempts, int blobsDeleted, boolean pending) {}

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
    updateArtifactIdentityPointer(canonicalRecord);
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
      updateArtifactIdentityPointer(record);
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
  public void registerPrewrittenStatsReferencesInGeneration(
      ResourceId tableId,
      long snapshotId,
      String generationId,
      List<StatsStore.PrewrittenTargetStatsReference> references) {
    String effectiveGenerationId = requireGenerationId(generationId);
    List<PrewrittenStatsWrite> writes =
        prewrittenStatsWrites(tableId, snapshotId, effectiveGenerationId, references);
    ensureWritableGeneration(tableId, snapshotId, effectiveGenerationId);
    targetStatsStorage.overwriteReferencesBatch(writes);
  }

  @Override
  public void markPreparedFileGroup(
      ResourceId tableId,
      long snapshotId,
      String generationId,
      String fileGroupJobId,
      String leaseEpoch,
      String artifactReferencesSha256) {
    String effectiveGenerationId = requireGenerationId(generationId);
    ensureWritableGeneration(tableId, snapshotId, effectiveGenerationId);
    String pointerKey =
        Keys.snapshotTargetStatsGenerationPreparedFileGroupPointer(
            tableId.getAccountId(),
            tableId.getId(),
            snapshotId,
            effectiveGenerationId,
            requireNonBlank(fileGroupJobId, "fileGroupJobId"),
            requireNonBlank(leaseEpoch, "leaseEpoch"));
    String marker =
        requireNonBlank(leaseEpoch, "leaseEpoch") + ":" + requireSha256(artifactReferencesSha256);
    for (int attempt = 0; attempt < BaseResourceRepository.CAS_MAX; attempt++) {
      Pointer current = pointerStore.get(pointerKey).orElse(null);
      if (current != null && marker.equals(current.getBlobUri())) {
        return;
      }
      if (current != null) {
        throw new IllegalStateException(
            "prepared file-group marker conflicts with the accepted result: " + fileGroupJobId);
      }
      if (pointerStore.compareAndSet(
          pointerKey, 0L, PointerReferences.opaqueMarkerPointer(pointerKey, marker, 1L))) {
        return;
      }
    }
    throw new BaseResourceRepository.AbortRetryableException(
        "prepared file-group marker conflicted repeatedly: " + fileGroupJobId);
  }

  @Override
  public boolean isPreparedFileGroup(
      ResourceId tableId,
      long snapshotId,
      String generationId,
      String fileGroupJobId,
      String leaseEpoch,
      String artifactReferencesSha256) {
    String marker =
        requireNonBlank(leaseEpoch, "leaseEpoch") + ":" + requireSha256(artifactReferencesSha256);
    return pointerStore
        .get(
            Keys.snapshotTargetStatsGenerationPreparedFileGroupPointer(
                tableId.getAccountId(),
                tableId.getId(),
                snapshotId,
                requireGenerationId(generationId),
                requireNonBlank(fileGroupJobId, "fileGroupJobId"),
                requireNonBlank(leaseEpoch, "leaseEpoch")))
        .filter(PointerReferences::isOpaqueMarkerPointer)
        .map(Pointer::getBlobUri)
        .filter(marker::equals)
        .isPresent();
  }

  private List<PrewrittenStatsWrite> prewrittenStatsWrites(
      ResourceId tableId,
      long snapshotId,
      String generationId,
      List<StatsStore.PrewrittenTargetStatsReference> references) {
    String requiredPrefix =
        Keys.snapshotTargetStatsGenerationBlobPrefix(
            tableId.getAccountId(), tableId.getId(), snapshotId, generationId);
    List<PrewrittenStatsWrite> writes = new ArrayList<>();
    Map<String, PrewrittenStatsWrite> uniqueWrites = new LinkedHashMap<>();
    for (StatsStore.PrewrittenTargetStatsReference value :
        references == null ? List.<StatsStore.PrewrittenTargetStatsReference>of() : references) {
      if (value == null) {
        continue;
      }
      if (value.targetStorageId() == null
          || value.targetStorageId().isBlank()
          || value.blobUri() == null
          || !value.blobUri().startsWith(requiredPrefix)
          || value.blobBytes() <= 0L
          || value.blobSha256() == null
          || value.blobSha256().length != 32
          || !value
              .blobUri()
              .endsWith(
                  "/"
                      + Hashing.sha256Hex(value.targetStorageId())
                      + "/"
                      + HexFormat.of().formatHex(value.blobSha256())
                      + ".pb")) {
        throw new IllegalArgumentException("invalid prewritten target stats reference");
      }
      PrewrittenStatsWrite write =
          new PrewrittenStatsWrite(
              targetPointerKey(tableId, snapshotId, generationId, value.targetStorageId()),
              value.blobUri(),
              value.blobBytes());
      PrewrittenStatsWrite duplicate = uniqueWrites.putIfAbsent(write.pointerKey(), write);
      if (duplicate != null && !duplicate.sameReference(write)) {
        throw new IllegalArgumentException(
            "duplicate prewritten target stats reference has different content");
      }
    }
    writes.addAll(uniqueWrites.values());
    return writes;
  }

  @Override
  public void publishPrewrittenStatsGeneration(
      ResourceId tableId,
      long snapshotId,
      String generationId,
      List<StatsStore.PrewrittenTargetStatsReference> references) {
    String effectiveGenerationId = requireGenerationId(generationId);
    List<PrewrittenStatsWrite> writes =
        prewrittenStatsWrites(tableId, snapshotId, effectiveGenerationId, references);
    String lifecycleState = generationLifecycleState(tableId, snapshotId, effectiveGenerationId);
    if (lifecycleState.isBlank() || GENERATION_WRITING.equals(lifecycleState)) {
      ensureWritableGeneration(tableId, snapshotId, effectiveGenerationId);
      ensurePublicationIntent(tableId, snapshotId, effectiveGenerationId, writes, true);
      targetStatsStorage.createExactReferencesBatch(writes);
    } else if (GENERATION_PUBLISHING.equals(lifecycleState)
        || GENERATION_PUBLISHED.equals(lifecycleState)) {
      ensurePublicationIntent(tableId, snapshotId, effectiveGenerationId, writes, false);
      targetStatsStorage.verifyExactReferences(writes);
    } else {
      throw new BaseResourceRepository.AbortRetryableException(
          "target stats generation cannot publish: "
              + effectiveGenerationId
              + " state="
              + lifecycleState);
    }
    Optional<ActiveSnapshotStats> current = activeGenerationLive(tableId, snapshotId);
    if (GENERATION_PUBLISHED.equals(lifecycleState)
        && current
            .map(ActiveSnapshotStats::generationId)
            .filter(effectiveGenerationId::equals)
            .isEmpty()) {
      throw new BaseResourceRepository.AbortRetryableException(
          "published target stats generation is no longer active: " + effectiveGenerationId);
    }
    publishActiveGenerationPointer(tableId, snapshotId, effectiveGenerationId, current);
    markGenerationPublished(tableId, snapshotId, effectiveGenerationId);
  }

  @Override
  public boolean publishPreparedStatsGeneration(
      ResourceId tableId,
      long snapshotId,
      String generationId,
      List<StatsStore.PrewrittenTargetStatsReference> finalReferences,
      StatsStore.StatsGenerationPredecessor predecessor,
      StatsStore.PublicationFence publicationFence) {
    String effectiveGenerationId = requireGenerationId(generationId);
    StatsStore.StatsGenerationPredecessor requiredPredecessor =
        java.util.Objects.requireNonNull(predecessor, "predecessor");
    String lifecycleState = generationLifecycleState(tableId, snapshotId, effectiveGenerationId);
    Optional<ActiveSnapshotStats> current = activeGenerationLive(tableId, snapshotId);
    boolean alreadyActive =
        current
            .map(ActiveSnapshotStats::generationId)
            .filter(effectiveGenerationId::equals)
            .isPresent();
    if (GENERATION_PUBLISHED.equals(lifecycleState)) {
      if (!alreadyActive) {
        throw new BaseResourceRepository.AbortRetryableException(
            "published target stats generation is no longer active: " + effectiveGenerationId);
      }
      return true;
    }
    List<PrewrittenStatsWrite> finalWrites =
        prewrittenStatsWrites(tableId, snapshotId, effectiveGenerationId, finalReferences);
    if (lifecycleState.isBlank() || GENERATION_WRITING.equals(lifecycleState)) {
      ensureWritableGeneration(tableId, snapshotId, effectiveGenerationId);
      ensurePublicationIntent(tableId, snapshotId, effectiveGenerationId, finalWrites, true);
      targetStatsStorage.overwriteReferencesBatch(finalWrites);
    } else if (GENERATION_PUBLISHING.equals(lifecycleState)) {
      ensurePublicationIntent(tableId, snapshotId, effectiveGenerationId, finalWrites, false);
      targetStatsStorage.verifyExactReferences(finalWrites);
    } else {
      throw new BaseResourceRepository.AbortRetryableException(
          "prepared target stats generation cannot publish: "
              + effectiveGenerationId
              + " state="
              + lifecycleState);
    }
    if (!alreadyActive
        && (!matchesPredecessor(current, requiredPredecessor)
            || !publishActiveGenerationPointer(
                tableId,
                snapshotId,
                effectiveGenerationId,
                requiredPredecessor,
                publicationFence))) {
      return false;
    }
    markGenerationPublished(tableId, snapshotId, effectiveGenerationId);
    return true;
  }

  @Override
  public StatsStore.StatsGenerationPredecessor prepareStatsGenerationForPublication(
      ResourceId tableId, long snapshotId, String generationId, boolean inheritMissingTargets) {
    String effectiveGenerationId = requireGenerationId(generationId);
    Optional<ActiveSnapshotStats> active = activeGenerationLive(tableId, snapshotId);
    StatsStore.StatsGenerationPredecessor predecessor = predecessorOf(active);
    if (!inheritMissingTargets
        || active.isEmpty()
        || effectiveGenerationId.equals(active.orElseThrow().generationId())) {
      return predecessor;
    }
    ensureGenerationCanRebase(tableId, snapshotId, effectiveGenerationId);
    String sourcePrefix =
        Keys.snapshotTargetStatsGenerationPrefix(
            tableId.getAccountId(),
            tableId.getId(),
            snapshotId,
            active.orElseThrow().generationId());
    String destinationPrefix =
        Keys.snapshotTargetStatsGenerationPrefix(
            tableId.getAccountId(), tableId.getId(), snapshotId, effectiveGenerationId);
    String capturedBlobPrefix =
        Keys.snapshotTargetStatsGenerationBlobPrefix(
            tableId.getAccountId(), tableId.getId(), snapshotId, effectiveGenerationId);
    String pageToken = "";
    do {
      StringBuilder next = new StringBuilder();
      List<Pointer> sourcePointers =
          pointerStore.listPointersByPrefix(sourcePrefix, 500, pageToken, next);
      for (Pointer source : sourcePointers) {
        String destinationKey =
            destinationPrefix + source.getKey().substring(sourcePrefix.length());
        rebaseInheritedStatsPointer(destinationKey, source, capturedBlobPrefix);
      }
      pageToken = next.toString();
    } while (!pageToken.isBlank());
    return predecessor;
  }

  private void rebaseInheritedStatsPointer(
      String destinationKey, Pointer source, String capturedBlobPrefix) {
    for (int attempt = 0; attempt < 4; attempt++) {
      Pointer destination = pointerStore.get(destinationKey).orElse(null);
      if (destination != null && destination.getBlobUri().startsWith(capturedBlobPrefix)) {
        return;
      }
      if (destination != null
          && destination.getBlobUri().equals(source.getBlobUri())
          && destination.hasReferencedObjectSizeBytes() == source.hasReferencedObjectSizeBytes()
          && (!source.hasReferencedObjectSizeBytes()
              || destination.getReferencedObjectSizeBytes()
                  == source.getReferencedObjectSizeBytes())) {
        return;
      }
      long expectedVersion = destination == null ? 0L : destination.getVersion();
      long referencedBytes =
          source.hasReferencedObjectSizeBytes() ? source.getReferencedObjectSizeBytes() : 0L;
      Pointer inherited =
          PointerReferences.blobPointer(
              destinationKey, source.getBlobUri(), expectedVersion + 1L, referencedBytes);
      if (pointerStore.compareAndSet(destinationKey, expectedVersion, inherited)) {
        return;
      }
    }
    throw new BaseResourceRepository.AbortRetryableException(
        "target stats inheritance conflicted repeatedly for " + destinationKey);
  }

  @Override
  public void protectPrewrittenStatsObjectsInGeneration(
      ResourceId tableId,
      long snapshotId,
      String generationId,
      String protectionId,
      List<StatsStore.PrewrittenStatsObject> objects) {
    String effectiveGenerationId = requireGenerationId(generationId);
    ensureWritableGeneration(tableId, snapshotId, effectiveGenerationId);
    String requiredBlobPrefix =
        Keys.snapshotTargetStatsGenerationBlobPrefix(
            tableId.getAccountId(), tableId.getId(), snapshotId, effectiveGenerationId);
    String pointerPrefix =
        Keys.snapshotTargetStatsGenerationProtectionPointerPrefix(
            tableId.getAccountId(),
            tableId.getId(),
            snapshotId,
            effectiveGenerationId,
            protectionId);
    List<PrewrittenStatsWrite> writes = new ArrayList<>();
    for (StatsStore.PrewrittenStatsObject object :
        objects == null ? List.<StatsStore.PrewrittenStatsObject>of() : objects) {
      if (object == null
          || object.blobUri() == null
          || !object.blobUri().startsWith(requiredBlobPrefix)
          || object.blobBytes() <= 0L
          || object.blobSha256() == null
          || object.blobSha256().length != 32) {
        throw new IllegalArgumentException("invalid prewritten stats object protection");
      }
      writes.add(
          new PrewrittenStatsWrite(
              pointerPrefix + Hashing.sha256Hex(object.blobUri()),
              object.blobUri(),
              object.blobBytes()));
    }
    targetStatsStorage.overwriteReferencesBatch(writes);
  }

  @Override
  public void clearPrewrittenStatsObjectProtections(
      ResourceId tableId, long snapshotId, String generationId) {
    pointerStore.deleteByPrefix(
        Keys.snapshotTargetStatsGenerationProtectionsPointerPrefix(
            tableId.getAccountId(),
            tableId.getId(),
            snapshotId,
            requireGenerationId(generationId)));
  }

  @Override
  public void publishStatsGeneration(
      ResourceId tableId,
      long snapshotId,
      String generationId,
      List<TargetStatsRecord> finalRecords) {
    publishStatsGeneration(tableId, snapshotId, generationId, finalRecords, true);
  }

  @Override
  public void publishStatsGeneration(
      ResourceId tableId,
      long snapshotId,
      String generationId,
      List<TargetStatsRecord> finalRecords,
      boolean carryForwardSupersededSketches) {
    String effectiveGenerationId = requireGenerationId(generationId);
    List<TargetStatsRecord> canonicalRecords =
        (finalRecords == null ? List.<TargetStatsRecord>of() : finalRecords)
            .stream()
                .filter(java.util.Objects::nonNull)
                .map(this::canonicalRecord)
                .peek(record -> requireRecordForSnapshot(tableId, snapshotId, record))
                .toList();
    Optional<ActiveSnapshotStats> current = activeGenerationLive(tableId, snapshotId);
    if (carryForwardSupersededSketches) {
      canonicalRecords =
          carrySketchesFromSuperseded(tableId, snapshotId, canonicalRecords, current);
    }
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
    publishActiveGeneration(tableId, snapshotId, effectiveGenerationId, current);
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
    deleteGenerationStrict(
        tableId.getAccountId(), tableId.getId(), snapshotId, effectiveGenerationId);
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
      updateArtifactIdentityPointer(canonicalRecord);
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
      updateArtifactIdentityPointer(record);
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
    return getTargetStatsBatchInResolvedGeneration(
        tableId,
        snapshotId,
        targets,
        activeGeneration(tableId, snapshotId).map(ActiveSnapshotStats::generationId));
  }

  @Override
  public Optional<TargetStatsRecord> getTargetStatsInGeneration(
      ResourceId tableId, long snapshotId, String generationToken, StatsTarget target) {
    return readGenerationIdForFrozenToken(snapshotId, generationToken)
        .flatMap(
            generationId ->
                targetStatsStorage.getByPointer(
                    targetPointerKey(tableId, snapshotId, generationId, target)));
  }

  @Override
  public Map<String, Optional<TargetStatsRecord>> getTargetStatsBatchInGeneration(
      ResourceId tableId, long snapshotId, String generationToken, List<StatsTarget> targets) {
    return getTargetStatsBatchInResolvedGeneration(
        tableId, snapshotId, targets, readGenerationIdForFrozenToken(snapshotId, generationToken));
  }

  private Map<String, Optional<TargetStatsRecord>> getTargetStatsBatchInResolvedGeneration(
      ResourceId tableId,
      long snapshotId,
      List<StatsTarget> targets,
      Optional<String> generationIdOpt) {
    if (targets == null || targets.isEmpty()) {
      return Map.of();
    }

    // Resolve generation ONCE — shared manifest for all targets in this snapshot.
    if (generationIdOpt.isEmpty()) {
      // No stats captured for this snapshot yet — all misses.
      Map<String, Optional<TargetStatsRecord>> out = new LinkedHashMap<>(targets.size());
      for (StatsTarget t : targets) {
        out.put(StatsTargetIdentity.storageId(t), Optional.empty());
      }
      return Collections.unmodifiableMap(out);
    }
    String generationId = generationIdOpt.get();

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
                    String pKey = targetPointerKey(tableId, snapshotId, generationId, target);
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

  private Optional<String> readGenerationIdForFrozenToken(long snapshotId, String generationToken) {
    if (generationToken == null || generationToken.isBlank()) {
      return Optional.empty();
    }
    String unavailableMessage =
        "frozen stats generation manifest unavailable for snapshot "
            + snapshotId
            + ": "
            + generationToken;
    try {
      return Optional.of(
          loadGenerationId(generationToken)
              .orElseThrow(
                  () -> new StatsStore.GenerationUnavailableException(unavailableMessage)));
    } catch (BaseResourceRepository.CorruptionException e) {
      throw new StatsStore.GenerationUnavailableException(unavailableMessage, e);
    }
  }

  @Override
  public Optional<TargetStatsRecord> getReusableTargetStats(
      ResourceId tableId,
      StatsTarget target,
      String sourceFingerprint,
      String statsCaptureSignature) {
    if (tableId == null
        || target == null
        || sourceFingerprint == null
        || sourceFingerprint.isBlank()
        || statsCaptureSignature == null
        || statsCaptureSignature.isBlank()) {
      return Optional.empty();
    }
    String key =
        artifactIdentityPointerKey(tableId, target, sourceFingerprint, statsCaptureSignature);
    Pointer pointer = pointerStore.get(key).orElse(null);
    if (pointer != null && PointerReferences.isOpaqueMarkerPointer(pointer)) {
      try {
        long snapshotId = Long.parseLong(pointer.getBlobUri());
        Optional<TargetStatsRecord> indexed = getTargetStats(tableId, snapshotId, target);
        if (indexed
            .filter(
                record ->
                    sourceFingerprint.equals(
                            record.getPropertiesMap().get(REUSE_SOURCE_FINGERPRINT_PROPERTY))
                        && statsCaptureSignature.equals(
                            record.getPropertiesMap().get(REUSE_STATS_SIGNATURE_PROPERTY)))
            .isPresent()) {
          return indexed;
        }
      } catch (NumberFormatException ignored) {
      }
    }
    Optional<TargetStatsRecord> migrated =
        findHistoricalTargetStats(
            tableId,
            target,
            record ->
                sourceFingerprint.equals(
                        record.getPropertiesMap().get(REUSE_SOURCE_FINGERPRINT_PROPERTY))
                    && statsCaptureSignature.equals(
                        record.getPropertiesMap().get(REUSE_STATS_SIGNATURE_PROPERTY)));
    migrated.ifPresent(this::updateArtifactIdentityPointer);
    return migrated;
  }

  @Override
  public Optional<TargetStatsRecord> findHistoricalTargetStats(
      ResourceId tableId, StatsTarget target, Predicate<TargetStatsRecord> compatibility) {
    if (tableId == null || target == null || compatibility == null) {
      return Optional.empty();
    }
    String prefix = Keys.snapshotRootPrefix(tableId.getAccountId(), tableId.getId());
    java.util.LinkedHashSet<Long> snapshotIds = new java.util.LinkedHashSet<>();
    String pageToken = "";
    do {
      StringBuilder nextToken = new StringBuilder();
      List<Pointer> pointers =
          pointerStore.listPointersByPrefix(prefix, 1_000, pageToken, nextToken);
      for (Pointer pointer : pointers) {
        OptionalLong candidate = parseSnapshotIdFromStatsManifestPointer(prefix, pointer.getKey());
        candidate.ifPresent(snapshotIds::add);
      }
      pageToken = nextToken.toString();
    } while (!pageToken.isBlank());
    for (long snapshotId : snapshotIds) {
      Optional<TargetStatsRecord> candidate = getTargetStats(tableId, snapshotId, target);
      if (candidate.filter(compatibility).isPresent()) {
        return candidate;
      }
    }
    return Optional.empty();
  }

  private String artifactIdentityPointerKey(
      ResourceId tableId,
      StatsTarget target,
      String sourceFingerprint,
      String statsCaptureSignature) {
    return Keys.targetStatsArtifactIdentityPointer(
        tableId.getAccountId(),
        tableId.getId(),
        StatsTargetIdentity.storageId(target),
        sourceFingerprint,
        statsCaptureSignature);
  }

  private void updateArtifactIdentityPointer(TargetStatsRecord record) {
    if (record == null || !record.hasTarget() || !record.getTarget().hasFile()) {
      return;
    }
    String sourceFingerprint =
        record.getPropertiesMap().getOrDefault(REUSE_SOURCE_FINGERPRINT_PROPERTY, "");
    String statsCaptureSignature =
        record.getPropertiesMap().getOrDefault(REUSE_STATS_SIGNATURE_PROPERTY, "");
    if (sourceFingerprint.isBlank() || statsCaptureSignature.isBlank()) {
      return;
    }
    String key =
        artifactIdentityPointerKey(
            record.getTableId(), record.getTarget(), sourceFingerprint, statsCaptureSignature);
    for (int attempt = 0; attempt < BaseResourceRepository.CAS_MAX; attempt++) {
      Pointer current = pointerStore.get(key).orElse(null);
      if (current != null
          && PointerReferences.isOpaqueMarkerPointer(current)
          && Long.toString(record.getSnapshotId()).equals(current.getBlobUri())) {
        return;
      }
      long expectedVersion = current == null ? 0L : current.getVersion();
      if (pointerStore.compareAndSet(
          key,
          expectedVersion,
          PointerReferences.opaqueMarkerPointer(
              key, Long.toString(record.getSnapshotId()), expectedVersion + 1L))) {
        return;
      }
    }
    throw new BaseResourceRepository.AbortRetryableException(
        "target stats artifact identity pointer conflicted repeatedly");
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
    List<BaseResourceRepository.KeyedValue<TargetStatsRecord>> rows =
        targetStatsStorage.listKeyed(
            listPrefix(tableId, snapshotId, generationId, targetType),
            Math.max(1, limit),
            pageToken,
            next);
    List<TargetStatsRecord> records =
        rows.stream().map(BaseResourceRepository.KeyedValue::value).toList();
    List<String> continuationTokens =
        rows.stream().map(row -> pointerStore.pageTokenAfterKey(row.key())).toList();
    return new StatsStorePage(records, next.toString(), continuationTokens);
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
    replaceAllStatsForSnapshot(tableId, snapshotId, records, true);
  }

  @Override
  public void replaceAllStatsForSnapshot(
      ResourceId tableId,
      long snapshotId,
      List<TargetStatsRecord> records,
      boolean carryForwardSupersededSketches) {
    List<TargetStatsRecord> canonicalRecords =
        (records == null ? List.<TargetStatsRecord>of() : records)
            .stream()
                .map(this::canonicalRecord)
                .peek(record -> requireRecordForSnapshot(tableId, snapshotId, record))
                .toList();
    Optional<ActiveSnapshotStats> current = activeGenerationLive(tableId, snapshotId);
    if (carryForwardSupersededSketches) {
      // Incremental generations only enrich: fold the superseded generation's sketch payloads into
      // same-target records before writing, so a scalar-only republish never loses sketches an
      // earlier capture already published for this unchanged snapshot.
      canonicalRecords =
          carrySketchesFromSuperseded(tableId, snapshotId, canonicalRecords, current);
    }
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
    validateTargetStatsRecord(canonical);
    return canonical;
  }

  private static void validateTargetStatsRecord(TargetStatsRecord record) {
    if (record == null) {
      throw new IllegalArgumentException("TargetStatsRecord is required");
    }
    StatsTarget target = record.getTarget();
    switch (target.getTargetCase()) {
      case TABLE -> {
        if (record.getValueCase() != TargetStatsRecord.ValueCase.TABLE) {
          throw new IllegalArgumentException(
              "incompatible target/value: table target requires table value");
        }
      }
      case COLUMN, EXPRESSION -> {
        if (record.getValueCase() != TargetStatsRecord.ValueCase.SCALAR) {
          throw new IllegalArgumentException(
              "incompatible target/value: column/expression target requires scalar value");
        }
      }
      case FILE -> {
        if (record.getValueCase() != TargetStatsRecord.ValueCase.FILE) {
          throw new IllegalArgumentException(
              "incompatible target/value: file target requires file value");
        }
      }
      case COMPOSITE ->
          throw new IllegalArgumentException(
              "incompatible target/value: composite target values are not implemented");
      case TARGET_NOT_SET ->
          throw new IllegalArgumentException("target must be set on TargetStatsRecord");
    }
    StatsTargetIdentity.storageId(target);
  }

  /**
   * Folds the superseded generation's sketch payloads into the records a new generation is about to
   * publish (see {@link StatsGenerationEnrichment} for the contract). One batched read of the
   * superseded generation, only for the scalar-bearing (column-style) targets being republished —
   * this runs on the finalize/republish path, never on the query hot path.
   */
  private List<TargetStatsRecord> carrySketchesFromSuperseded(
      ResourceId tableId,
      long snapshotId,
      List<TargetStatsRecord> incoming,
      Optional<ActiveSnapshotStats> superseded) {
    if (superseded.isEmpty() || incoming.isEmpty()) {
      return incoming;
    }
    List<StatsTarget> scalarTargets =
        incoming.stream()
            .filter(TargetStatsRecord::hasScalar)
            .map(TargetStatsRecord::getTarget)
            .toList();
    if (scalarTargets.isEmpty()) {
      return incoming;
    }
    Map<String, Optional<TargetStatsRecord>> previous =
        getTargetStatsBatchInResolvedGeneration(
            tableId, snapshotId, scalarTargets, superseded.map(ActiveSnapshotStats::generationId));
    return incoming.stream()
        .map(
            record ->
                previous
                    .getOrDefault(
                        StatsTargetIdentity.storageId(record.getTarget()), Optional.empty())
                    .map(prior -> StatsGenerationEnrichment.carrySketchesForward(record, prior))
                    .orElse(record))
        .toList();
  }

  private void publishActiveGeneration(
      ResourceId tableId,
      long snapshotId,
      String generationId,
      Optional<ActiveSnapshotStats> current) {
    publishActiveGenerationPointer(tableId, snapshotId, generationId, current);
    markGenerationPublished(tableId, snapshotId, generationId);
  }

  private void publishActiveGenerationPointer(
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
    Pointer active = pointerStore.get(manifestPointer).orElse(null);
    if (active != null && manifestBlobUri.equals(active.getBlobUri())) {
      return;
    }
    long expectedVersion = current.map(ActiveSnapshotStats::manifestVersion).orElse(0L);
    Pointer next =
        PointerReferences.blobPointer(manifestPointer, manifestBlobUri, expectedVersion + 1L);
    if (!pointerStore.compareAndSet(manifestPointer, expectedVersion, next)) {
      throw new BaseResourceRepository.AbortRetryableException(
          "active target stats generation update conflicted for snapshot " + snapshotId);
    }
  }

  private boolean publishActiveGenerationPointer(
      ResourceId tableId,
      long snapshotId,
      String generationId,
      StatsStore.StatsGenerationPredecessor predecessor,
      StatsStore.PublicationFence publicationFence) {
    String manifestPointer =
        Keys.snapshotTargetStatsManifestPointer(
            tableId.getAccountId(), tableId.getId(), snapshotId);
    String manifestBlobUri =
        Keys.snapshotTargetStatsManifestBlobUri(
            tableId.getAccountId(), tableId.getId(), snapshotId, generationId);
    markGenerationPublishing(tableId, snapshotId, generationId);
    targetStatsStorage.putManifestBlob(manifestBlobUri, StringValue.of(generationId));
    if (blobCache != null) {
      blobCache.put(manifestBlobUri, generationId);
    }
    Pointer active = pointerStore.get(manifestPointer).orElse(null);
    if (active != null && manifestBlobUri.equals(active.getBlobUri())) {
      return true;
    }
    if (!matchesPredecessor(active, predecessor)) {
      return false;
    }
    long expectedVersion = predecessor.manifestVersion();
    Pointer next =
        PointerReferences.blobPointer(manifestPointer, manifestBlobUri, expectedVersion + 1L);
    if (publicationFence == null) {
      return pointerStore.compareAndSet(manifestPointer, expectedVersion, next);
    }
    List<PointerStore.CasOp> publication =
        new ArrayList<>(publicationFence.pointerUpdates().size() + 1);
    publication.add(new PointerStore.CasUpsert(manifestPointer, expectedVersion, next));
    publicationFence.pointerUpdates().stream()
        .map(
            update ->
                (PointerStore.CasOp)
                    new PointerStore.CasUpsert(
                        update.pointerKey(), update.expectedVersion(), update.next()))
        .forEach(publication::add);
    return pointerStore.compareAndSetBatch(publication);
  }

  private static StatsStore.StatsGenerationPredecessor predecessorOf(
      Optional<ActiveSnapshotStats> active) {
    return active
        .map(
            value ->
                new StatsStore.StatsGenerationPredecessor(
                    value.generationId(), value.manifestVersion()))
        .orElseGet(() -> new StatsStore.StatsGenerationPredecessor("", 0L));
  }

  private static boolean matchesPredecessor(
      Optional<ActiveSnapshotStats> active, StatsStore.StatsGenerationPredecessor predecessor) {
    return active
        .map(
            value ->
                value.manifestVersion() == predecessor.manifestVersion()
                    && value.generationId().equals(predecessor.generationId()))
        .orElseGet(
            () -> predecessor.manifestVersion() == 0L && predecessor.generationId().isBlank());
  }

  private static boolean matchesPredecessor(
      Pointer active, StatsStore.StatsGenerationPredecessor predecessor) {
    if (active == null) {
      return predecessor.manifestVersion() == 0L && predecessor.generationId().isBlank();
    }
    return active.getVersion() == predecessor.manifestVersion();
  }

  private void ensureGenerationCanRebase(ResourceId tableId, long snapshotId, String generationId) {
    String lifecycleState = generationLifecycleState(tableId, snapshotId, generationId);
    if (lifecycleState.isBlank() || GENERATION_WRITING.equals(lifecycleState)) {
      ensureWritableGeneration(tableId, snapshotId, generationId);
      return;
    }
    if (GENERATION_PUBLISHING.equals(lifecycleState)) {
      return;
    }
    throw new BaseResourceRepository.AbortRetryableException(
        "target stats generation cannot rebase: " + generationId + " state=" + lifecycleState);
  }

  private String generationLifecycleState(
      ResourceId tableId, long snapshotId, String generationId) {
    Pointer lifecycle =
        pointerStore
            .get(generationLifecyclePointer(tableId, snapshotId, generationId))
            .orElse(null);
    if (lifecycle != null) {
      return blankToEmpty(lifecycle.getBlobUri());
    }
    return pointerStore
            .get(deletedGenerationFencePointer(tableId, snapshotId, generationId))
            .isPresent()
        ? GENERATION_DELETED
        : "";
  }

  private void ensurePublicationIntent(
      ResourceId tableId,
      long snapshotId,
      String generationId,
      List<PrewrittenStatsWrite> writes,
      boolean createIfAbsent) {
    String pointerKey =
        Keys.snapshotTargetStatsGenerationPublicationIntentPointer(
            tableId.getAccountId(), tableId.getId(), snapshotId, generationId);
    String intent = prewrittenStatsPublicationIntent(writes);
    for (int attempt = 0; attempt < 8; attempt++) {
      Pointer current = pointerStore.get(pointerKey).orElse(null);
      if (current != null) {
        if (PointerReferences.isOpaqueMarkerPointer(current)
            && intent.equals(current.getBlobUri())) {
          return;
        }
        throw new IllegalArgumentException(
            "prewritten target stats publication intent changed for generation " + generationId);
      }
      if (!createIfAbsent) {
        throw new IllegalArgumentException(
            "prewritten target stats publication intent is missing for generation " + generationId);
      }
      Pointer next = PointerReferences.opaqueMarkerPointer(pointerKey, intent, 1L);
      if (pointerStore.compareAndSet(pointerKey, 0L, next)) {
        return;
      }
    }
    throw new BaseResourceRepository.AbortRetryableException(
        "prewritten target stats publication intent update conflicted: " + generationId);
  }

  private static String prewrittenStatsPublicationIntent(List<PrewrittenStatsWrite> writes) {
    StringBuilder canonical = new StringBuilder();
    writes.stream()
        .sorted(Comparator.comparing(PrewrittenStatsWrite::pointerKey))
        .forEach(
            write ->
                canonical
                    .append(write.pointerKey().length())
                    .append(':')
                    .append(write.pointerKey())
                    .append(write.blobUri().length())
                    .append(':')
                    .append(write.blobUri())
                    .append(write.blobBytes())
                    .append(';'));
    return "sha256:" + Hashing.sha256Hex(canonical.toString());
  }

  private void ensureWritableGeneration(ResourceId tableId, long snapshotId, String generationId) {
    String lifecyclePointer = generationLifecyclePointer(tableId, snapshotId, generationId);
    String deletedFencePointer = deletedGenerationFencePointer(tableId, snapshotId, generationId);
    for (int attempt = 0; attempt < 8; attempt++) {
      if (pointerStore.get(deletedFencePointer).isPresent()) {
        throw new BaseResourceRepository.AbortRetryableException(
            "target stats generation is not writable: " + generationId + " state=DELETED");
      }
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
        if (pointerStore.get(deletedFencePointer).isPresent()) {
          pointerStore.compareAndDelete(lifecyclePointer, next.getVersion());
          throw new BaseResourceRepository.AbortRetryableException(
              "target stats generation is not writable: " + generationId + " state=DELETED");
        }
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

  private static String deletedGenerationFencePointer(
      ResourceId tableId, long snapshotId, String generationId) {
    return Keys.snapshotTargetStatsDeletedGenerationFencePointer(
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
    ActiveSnapshotStats resolved =
        activeGeneration(tableId, snapshotId)
            .orElseThrow(
                () ->
                    new BaseResourceRepository.AbortRetryableException(
                        "active target stats generation vanished during create"));
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
    return targetPointerKey(
        tableId, snapshotId, generationId, StatsTargetIdentity.storageId(target));
  }

  private static String targetPointerKey(
      ResourceId tableId, long snapshotId, String generationId, String targetStorageId) {
    return Keys.snapshotTargetStatsGenerationPointer(
        tableId.getAccountId(), tableId.getId(), snapshotId, generationId, targetStorageId);
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
    return deleteUnreferencedGenerations(
            tableId, isProtectedManifestUri, nowMs, minAgeMs, Integer.MAX_VALUE, Long.MAX_VALUE)
        .generationsReclaimed();
  }

  public GenerationGcResult deleteUnreferencedGenerations(
      ResourceId tableId,
      java.util.function.Predicate<String> isProtectedManifestUri,
      long nowMs,
      long minAgeMs,
      int maxBlobDeleteAttempts,
      long deadlineMs) {
    if (maxBlobDeleteAttempts <= 0) {
      return new GenerationGcResult(0, 0, 0, true);
    }
    String accountId = tableId.getAccountId();
    String prefix = Keys.snapshotRootPrefix(accountId, tableId.getId());
    var candidates = new java.util.LinkedHashSet<Keys.GenerationKey>();
    String token = "";
    boolean pending = false;
    while (true) {
      if (System.currentTimeMillis() >= deadlineMs) {
        pending = true;
        break;
      }
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
    int deleteAttempts = 0;
    int blobsDeleted = 0;
    for (Keys.GenerationKey candidate : candidates) {
      if (deleteAttempts >= Math.max(0, maxBlobDeleteAttempts)
          || System.currentTimeMillis() >= deadlineMs) {
        pending = true;
        break;
      }
      long snapshotId = candidate.snapshotId();
      String generationId = candidate.generationId();
      String manifestUri =
          Keys.snapshotTargetStatsManifestBlobUri(
              accountId, tableId.getId(), snapshotId, generationId);
      Pointer lifecycle =
          pointerStore
              .get(generationLifecyclePointer(tableId, snapshotId, generationId))
              .orElse(null);
      String lifecycleState = lifecycle == null ? "" : blankToEmpty(lifecycle.getBlobUri());
      if (GENERATION_DELETING.equals(lifecycleState) || GENERATION_DELETED.equals(lifecycleState)) {
        GenerationBlobDeleteResult result =
            deleteGenerationBlobSlice(
                accountId,
                tableId.getId(),
                snapshotId,
                generationId,
                manifestUri,
                maxBlobDeleteAttempts - deleteAttempts,
                deadlineMs);
        deleteAttempts += result.attempts();
        blobsDeleted += result.deleted();
        if (result.pending()) {
          pending = true;
          break;
        }
        deleteGenerationPointers(accountId, tableId.getId(), snapshotId, generationId, true);
        reclaimed++;
        continue;
      }
      // One HEAD answers both existence and age: absent means unpublished (an in-flight replace
      // writes records before publishing) or already reclaimed.
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
      if (isActiveIndexGeneration(tableId, snapshotId, generationId)) {
        continue;
      }
      String liveActive = activeStatsGeneration(tableId, snapshotId).orElse("");
      if (manifestUri.equals(liveActive)) {
        continue; // creation-window safeguard: active pointer target survives regardless of roots
      }
      if (!claimPublishedGenerationForGc(tableId, snapshotId, generationId)) {
        continue;
      }
      if (manifestUri.equals(activeStatsGeneration(tableId, snapshotId).orElse(""))
          || isProtectedManifestUri.test(manifestUri)
          || isActiveIndexGeneration(tableId, snapshotId, generationId)) {
        restoreGenerationPublishedAfterFailedGcClaim(tableId, snapshotId, generationId);
        continue;
      }
      GenerationBlobDeleteResult result =
          deleteGenerationBlobSlice(
              accountId,
              tableId.getId(),
              snapshotId,
              generationId,
              manifestUri,
              maxBlobDeleteAttempts - deleteAttempts,
              deadlineMs);
      deleteAttempts += result.attempts();
      blobsDeleted += result.deleted();
      if (result.pending()) {
        pending = true;
        break;
      }
      deleteGenerationPointers(accountId, tableId.getId(), snapshotId, generationId, true);
      reclaimed++;
    }
    return new GenerationGcResult(reclaimed, deleteAttempts, blobsDeleted, pending);
  }

  private boolean isActiveIndexGeneration(
      ResourceId tableId, long snapshotId, String generationId) {
    return pointerStore
        .get(
            Keys.snapshotIndexArtifactActiveGenerationPointer(
                tableId.getAccountId(), tableId.getId(), snapshotId))
        .map(Pointer::getBlobUri)
        .filter(generationId::equals)
        .isPresent();
  }

  private record GenerationBlobDeleteResult(int attempts, int deleted, boolean pending) {}

  private GenerationBlobDeleteResult deleteGenerationBlobSlice(
      String accountId,
      String tableId,
      long snapshotId,
      String generationId,
      String manifestUri,
      int maxAttempts,
      long deadlineMs) {
    if (maxAttempts <= 0 || System.currentTimeMillis() >= deadlineMs) {
      return new GenerationBlobDeleteResult(0, 0, true);
    }
    String blobPrefix =
        Keys.snapshotTargetStatsGenerationBlobPrefix(accountId, tableId, snapshotId, generationId);
    int attempts = 0;
    int deleted = 0;
    BlobStore.Page page = blobStore.list(blobPrefix, Math.min(maxAttempts, 1000), "");
    for (String key : page.keys()) {
      if (attempts >= maxAttempts || System.currentTimeMillis() >= deadlineMs) {
        return new GenerationBlobDeleteResult(attempts, deleted, true);
      }
      attempts++;
      if (blobStore.delete(key)) {
        deleted++;
      }
    }
    if (!blobStore.list(blobPrefix, 1, "").keys().isEmpty()) {
      return new GenerationBlobDeleteResult(attempts, deleted, true);
    }
    if (blobStore.head(manifestUri).isPresent()) {
      if (attempts >= maxAttempts || System.currentTimeMillis() >= deadlineMs) {
        return new GenerationBlobDeleteResult(attempts, deleted, true);
      }
      attempts++;
      if (blobStore.delete(manifestUri)) {
        deleted++;
      }
    }
    return new GenerationBlobDeleteResult(attempts, deleted, false);
  }

  private void deleteGenerationPointers(
      String accountId,
      String tableId,
      long snapshotId,
      String generationId,
      boolean retainDeletedTombstone) {
    pointerStore.deleteByPrefix(
        Keys.snapshotTargetStatsGenerationPrefix(accountId, tableId, snapshotId, generationId));
    pointerStore.deleteByPrefix(
        Keys.snapshotTargetStatsGenerationProtectionsPointerPrefix(
            accountId, tableId, snapshotId, generationId));
    pointerStore.deleteByPrefix(
        Keys.snapshotIndexArtifactGenerationPrefix(accountId, tableId, snapshotId, generationId));
    pointerStore.deleteByPrefix(
        Keys.snapshotTargetStatsGenerationPointerPrefix(
                accountId, tableId, snapshotId, generationId)
            + "prepared-file-groups/");
    pointerStore.delete(
        Keys.snapshotTargetStatsGenerationPublicationIntentPointer(
            accountId, tableId, snapshotId, generationId));
    if (retainDeletedTombstone) {
      markGenerationDeleted(resourceId(accountId, tableId), snapshotId, generationId);
    } else {
      pointerStore.delete(
          Keys.snapshotTargetStatsGenerationLifecyclePointer(
              accountId, tableId, snapshotId, generationId));
      pointerStore.delete(
          Keys.snapshotTargetStatsDeletedGenerationFencePointer(
              accountId, tableId, snapshotId, generationId));
    }
  }

  private void deleteGeneration(
      String accountId, String tableId, long snapshotId, String generationId) {
    deleteQuietly(
        () -> deleteGenerationPointers(accountId, tableId, snapshotId, generationId, false));
    deleteQuietly(
        () ->
            blobStore.deletePrefix(
                Keys.snapshotTargetStatsGenerationBlobPrefix(
                    accountId, tableId, snapshotId, generationId)));
  }

  private boolean claimPublishedGenerationForGc(
      ResourceId tableId, long snapshotId, String generationId) {
    String lifecyclePointer = generationLifecyclePointer(tableId, snapshotId, generationId);
    for (int attempt = 0; attempt < 8; attempt++) {
      Pointer current = pointerStore.get(lifecyclePointer).orElse(null);
      if (current == null) {
        return false;
      }
      String state = blankToEmpty(current.getBlobUri());
      if (GENERATION_DELETING.equals(state)) {
        return true;
      }
      if (!GENERATION_PUBLISHED.equals(state)) {
        return false;
      }
      Pointer next =
          PointerReferences.opaqueMarkerPointer(
              lifecyclePointer, GENERATION_DELETING, current.getVersion() + 1L);
      if (pointerStore.compareAndSet(lifecyclePointer, current.getVersion(), next)) {
        return true;
      }
    }
    throw new StorageAbortRetryableException(
        "stats generation GC claim conflicted repeatedly: " + generationId);
  }

  private void restoreGenerationPublishedAfterFailedGcClaim(
      ResourceId tableId, long snapshotId, String generationId) {
    String lifecyclePointer = generationLifecyclePointer(tableId, snapshotId, generationId);
    for (int attempt = 0; attempt < 8; attempt++) {
      Pointer current = pointerStore.get(lifecyclePointer).orElse(null);
      if (current == null || GENERATION_PUBLISHED.equals(blankToEmpty(current.getBlobUri()))) {
        return;
      }
      if (!GENERATION_DELETING.equals(blankToEmpty(current.getBlobUri()))) {
        throw new StorageAbortRetryableException(
            "stats generation changed state while restoring failed GC claim: " + generationId);
      }
      Pointer restored =
          PointerReferences.opaqueMarkerPointer(
              lifecyclePointer, GENERATION_PUBLISHED, current.getVersion() + 1L);
      if (pointerStore.compareAndSet(lifecyclePointer, current.getVersion(), restored)) {
        return;
      }
    }
    throw new StorageAbortRetryableException(
        "stats generation GC claim restore conflicted repeatedly: " + generationId);
  }

  private void markGenerationDeleted(ResourceId tableId, long snapshotId, String generationId) {
    String lifecyclePointer = generationLifecyclePointer(tableId, snapshotId, generationId);
    String deletedFencePointer = deletedGenerationFencePointer(tableId, snapshotId, generationId);
    for (int attempt = 0; attempt < 8; attempt++) {
      Pointer current = pointerStore.get(lifecyclePointer).orElse(null);
      if (current == null) {
        if (pointerStore.get(deletedFencePointer).isPresent()) {
          return;
        }
        throw new StorageAbortRetryableException(
            "stats generation lifecycle disappeared while marking deleted: " + generationId);
      }
      String state = blankToEmpty(current.getBlobUri());
      if (!GENERATION_DELETING.equals(state) && !GENERATION_DELETED.equals(state)) {
        throw new StorageAbortRetryableException(
            "stats generation cannot be marked deleted: " + generationId + " state=" + state);
      }
      Pointer deletedFence =
          PointerReferences.opaqueMarkerPointer(deletedFencePointer, GENERATION_DELETED, 1L)
              .toBuilder()
              .setExpiresAt(
                  Timestamps.fromMillis(
                      System.currentTimeMillis() + deletedGenerationFenceRetentionMs()))
              .build();
      Pointer existingFence = pointerStore.get(deletedFencePointer).orElse(null);
      if (existingFence == null
          && !pointerStore.compareAndSet(deletedFencePointer, 0L, deletedFence)) {
        continue;
      }
      if (pointerStore.compareAndDelete(lifecyclePointer, current.getVersion())
          || pointerStore.get(lifecyclePointer).isEmpty()) {
        return;
      }
    }
    throw new StorageAbortRetryableException(
        "stats generation deleted lifecycle update conflicted repeatedly: " + generationId);
  }

  private static long deletedGenerationFenceRetentionMs() {
    long configured =
        ConfigProvider.getConfig()
            .getOptionalValue("floecat.gc.stats-generation-deleted-fence-retention-ms", Long.class)
            .orElse(DEFAULT_DELETED_GENERATION_FENCE_RETENTION_MS);
    long jobRetention =
        ConfigProvider.getConfig()
            .getOptionalValue("floecat.gc.reconcile-jobs.retention-ms", Long.class)
            .orElse(24L * 60L * 60L * 1000L);
    return Math.max(configured, jobRetention + 24L * 60L * 60L * 1000L);
  }

  private void deleteGenerationStrict(
      String accountId, String tableId, long snapshotId, String generationId) {
    String targetPointerPrefix =
        Keys.snapshotTargetStatsGenerationPrefix(accountId, tableId, snapshotId, generationId);
    String protectionPointerPrefix =
        Keys.snapshotTargetStatsGenerationProtectionsPointerPrefix(
            accountId, tableId, snapshotId, generationId);
    String indexPointerPrefix =
        Keys.snapshotIndexArtifactGenerationPrefix(accountId, tableId, snapshotId, generationId);
    String preparedPointerPrefix =
        Keys.snapshotTargetStatsGenerationPointerPrefix(
                accountId, tableId, snapshotId, generationId)
            + "prepared-file-groups/";
    String publicationIntentPointer =
        Keys.snapshotTargetStatsGenerationPublicationIntentPointer(
            accountId, tableId, snapshotId, generationId);
    String blobPrefix =
        Keys.snapshotTargetStatsBlobPrefix(accountId, tableId, snapshotId)
            + "generations/"
            + Keys.encodeSegment(generationId)
            + "/";
    RuntimeException failure = null;
    try {
      pointerStore.deleteByPrefix(targetPointerPrefix);
    } catch (RuntimeException e) {
      failure = e;
    }
    try {
      pointerStore.deleteByPrefix(protectionPointerPrefix);
    } catch (RuntimeException e) {
      failure = accumulateFailure(failure, e);
    }
    try {
      pointerStore.deleteByPrefix(indexPointerPrefix);
    } catch (RuntimeException e) {
      failure = accumulateFailure(failure, e);
    }
    try {
      pointerStore.deleteByPrefix(preparedPointerPrefix);
    } catch (RuntimeException e) {
      failure = accumulateFailure(failure, e);
    }
    try {
      pointerStore.delete(publicationIntentPointer);
    } catch (RuntimeException e) {
      failure = accumulateFailure(failure, e);
    }
    try {
      blobStore.deletePrefix(blobPrefix);
    } catch (RuntimeException e) {
      if (failure == null) {
        failure = e;
      } else {
        failure.addSuppressed(e);
      }
    }
    if (failure != null) {
      throw new StorageAbortRetryableException(
          "target stats generation cleanup failed: " + generationId, failure);
    }
    if (pointerStore.countByPrefix(targetPointerPrefix) != 0
        || pointerStore.countByPrefix(protectionPointerPrefix) != 0
        || pointerStore.countByPrefix(indexPointerPrefix) != 0
        || pointerStore.countByPrefix(preparedPointerPrefix) != 0
        || pointerStore.get(publicationIntentPointer).isPresent()
        || !blobStore.list(blobPrefix, 1, "").keys().isEmpty()) {
      throw new StorageAbortRetryableException(
          "target stats generation cleanup was incomplete: " + generationId);
    }
  }

  private static ResourceId resourceId(String accountId, String tableId) {
    return ResourceId.newBuilder()
        .setAccountId(accountId)
        .setKind(ai.floedb.floecat.common.rpc.ResourceKind.RK_TABLE)
        .setId(tableId)
        .build();
  }

  private static RuntimeException accumulateFailure(
      RuntimeException current, RuntimeException next) {
    if (current == null) {
      return next;
    }
    current.addSuppressed(next);
    return current;
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

  private static String requireNonBlank(String value, String field) {
    String effective = value == null ? "" : value.trim();
    if (effective.isBlank()) {
      throw new IllegalArgumentException(field + " is required");
    }
    return effective;
  }

  private static String requireSha256(String value) {
    String effective = requireNonBlank(value, "artifactReferencesSha256");
    if (effective.length() != 64
        || !effective.chars().allMatch(character -> Character.digit(character, 16) >= 0)) {
      throw new IllegalArgumentException("artifactReferencesSha256 must be 64 hexadecimal digits");
    }
    return effective.toLowerCase(java.util.Locale.ROOT);
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

  private record PrewrittenStatsWrite(String pointerKey, String blobUri, long blobBytes) {
    private boolean sameReference(PrewrittenStatsWrite other) {
      return other != null && blobBytes == other.blobBytes && blobUri.equals(other.blobUri);
    }
  }

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

    private List<KeyedValue<TargetStatsRecord>> listKeyed(
        String prefix, int limit, String token, StringBuilder nextOut) {
      return super.listByPrefixWithKeys(prefix, limit, token, nextOut);
    }

    private void create(String pointerKey, String blobUri, TargetStatsRecord value) {
      putBlob(blobUri, value);
      reserveAllOrRollback(value.getSerializedSize(), pointerKey, blobUri);
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
      forEachPointerBatch(pending, this::reserveBatchOrClassify);
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

    private void overwriteReferencesBatch(List<PrewrittenStatsWrite> writes) {
      if (writes == null || writes.isEmpty()) {
        return;
      }
      Map<String, PrewrittenStatsWrite> uniqueWrites = new LinkedHashMap<>();
      for (PrewrittenStatsWrite write : writes) {
        uniqueWrites.put(write.pointerKey(), write);
      }
      List<PrewrittenStatsWrite> pending = new ArrayList<>(uniqueWrites.values());
      forEachPointerBatch(pending, this::overwriteReferencesChunk);
    }

    private void createExactReferencesBatch(List<PrewrittenStatsWrite> writes) {
      if (writes == null || writes.isEmpty()) {
        return;
      }
      forEachPointerBatch(writes, this::createExactReferencesChunk);
    }

    private void createExactReferencesChunk(List<PrewrittenStatsWrite> writes) {
      List<PrewrittenStatsWrite> remaining = new ArrayList<>(writes);
      for (int attempt = 0; attempt < CAS_MAX; attempt++) {
        List<PointerStore.CasOp> creates = new ArrayList<>();
        List<PrewrittenStatsWrite> missing = new ArrayList<>();
        for (PrewrittenStatsWrite write : remaining) {
          Pointer existing = pointerStore.get(write.pointerKey()).orElse(null);
          if (existing != null) {
            requireExactReference(write, existing);
            continue;
          }
          missing.add(write);
          creates.add(prewrittenStatsUpsert(write, 0L));
        }
        if (creates.isEmpty() || pointerStore.compareAndSetBatch(creates)) {
          return;
        }
        remaining = missing;
      }
      throw new AbortRetryableException(
          "prewritten target stats reference create conflicted: "
              + remaining.getFirst().pointerKey());
    }

    private void verifyExactReferences(List<PrewrittenStatsWrite> writes) {
      for (PrewrittenStatsWrite write : writes) {
        Pointer existing = pointerStore.get(write.pointerKey()).orElse(null);
        if (existing == null) {
          throw new AbortRetryableException(
              "prewritten target stats reference is missing: " + write.pointerKey());
        }
        requireExactReference(write, existing);
      }
    }

    private void requireExactReference(PrewrittenStatsWrite write, Pointer existing) {
      if (!PointerReferences.isBlobPointer(existing)
          || !write.blobUri().equals(existing.getBlobUri())
          || !existing.hasReferencedObjectSizeBytes()
          || write.blobBytes() != existing.getReferencedObjectSizeBytes()) {
        throw new IllegalArgumentException(
            "prewritten target stats reference changed for pointer " + write.pointerKey());
      }
    }

    private void overwriteReferencesChunk(List<PrewrittenStatsWrite> writes) {
      List<PrewrittenStatsWrite> remaining = new ArrayList<>(writes);
      List<PointerStore.CasOp> initial = new ArrayList<>(remaining.size());
      for (PrewrittenStatsWrite write : remaining) {
        initial.add(prewrittenStatsUpsert(write, 0L));
      }
      if (pointerStore.compareAndSetBatch(initial)) {
        return;
      }
      for (int attempt = 1; attempt < CAS_MAX; attempt++) {
        List<PrewrittenStatsWrite> nextRemaining = new ArrayList<>();
        List<PointerStore.CasOp> ops = new ArrayList<>();
        for (PrewrittenStatsWrite write : remaining) {
          Pointer existing = pointerStore.get(write.pointerKey()).orElse(null);
          if (existing != null && write.blobUri().equals(existing.getBlobUri())) {
            continue;
          }
          long expectedVersion = existing == null ? 0L : existing.getVersion();
          nextRemaining.add(write);
          ops.add(prewrittenStatsUpsert(write, expectedVersion));
        }
        if (ops.isEmpty() || pointerStore.compareAndSetBatch(ops)) {
          return;
        }
        remaining = nextRemaining;
      }
      throw new AbortRetryableException("overwrite conflict: " + remaining.getFirst().pointerKey());
    }

    private PointerStore.CasUpsert prewrittenStatsUpsert(
        PrewrittenStatsWrite write, long expectedVersion) {
      return new PointerStore.CasUpsert(
          write.pointerKey(),
          expectedVersion,
          PointerReferences.blobPointer(
              write.pointerKey(),
              write.blobUri(),
              Math.max(1L, expectedVersion + 1L),
              write.blobBytes()));
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
            PointerReferences.blobPointer(
                pointerKey, blobUri, Math.max(1L, expectedVersion + 1L), value.getSerializedSize());
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
      Pointer reserve =
          PointerReferences.blobPointer(pointerKey, blobUri, 1L, value.getSerializedSize());
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

    private static <T> void forEachPointerBatch(List<T> values, Consumer<List<T>> action) {
      for (int from = 0; from < values.size(); from += MAX_POINTER_BATCH_SIZE) {
        action.accept(values.subList(from, Math.min(from + MAX_POINTER_BATCH_SIZE, values.size())));
      }
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
                  PointerReferences.blobPointer(
                      write.pointerKey(), write.blobUri(), 1L, write.value().getSerializedSize())));
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
                PointerReferences.blobPointer(
                    write.pointerKey(), write.blobUri(), 1L, write.value().getSerializedSize())));
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
