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

import ai.floedb.floecat.catalog.rpc.IndexArtifactRecord;
import ai.floedb.floecat.catalog.rpc.IndexTarget;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.reconciler.impl.ReusableArtifactIndexStore;
import ai.floedb.floecat.reconciler.jobs.ReusableArtifactBundleSelection;
import ai.floedb.floecat.reconciler.jobs.ReusableArtifactBundleUris;
import ai.floedb.floecat.reconciler.jobs.ReusableArtifactBundles;
import ai.floedb.floecat.reconciler.rpc.CaptureOutput;
import ai.floedb.floecat.reconciler.rpc.DefaultColumnScope;
import ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundlePayload;
import ai.floedb.floecat.reconciler.rpc.SnapshotCaptureManifest;
import ai.floedb.floecat.service.repo.cache.ImmutableBlobCache;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.service.repo.util.AccountDeletionFence;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.TableBlobReachabilityGuard;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.storage.errors.StorageNotFoundException;
import ai.floedb.floecat.storage.errors.StorageTransactionConflictException;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.types.Hashing;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@ApplicationScoped
public class IndexArtifactRepository {
  // Leave one DynamoDB transaction slot for the account-deletion fence check.
  private static final int MAX_POINTER_BATCH_SIZE = 99;
  private static final int MAX_PARALLEL_SIDECAR_CHECKS = 50;
  private static final String DIRECT_GENERATION = Keys.INDEX_ARTIFACT_DIRECT_GENERATION;
  private static final String LIST_TOKEN_PREFIX = "v1.";

  public record PrewrittenIndexArtifactReference(
      String targetStorageId, String blobUri, long blobBytes, byte[] blobSha256) {}

  public record GenerationPredecessor(
      String generationId,
      long activePointerVersion,
      String captureManifestUri,
      long captureManifestPointerVersion) {
    public GenerationPredecessor {
      generationId = generationId == null ? "" : generationId;
      captureManifestUri = captureManifestUri == null ? "" : captureManifestUri;
    }
  }

  public record GenerationInput(
      GenerationPredecessor predecessor, List<IndexArtifactRecord> artifacts) {}

  public record ActivationFence(String pointerKey, String value, long version) {}

  public record PreparedActivation(
      ActivationFence activationFence,
      StatsStore.PublicationFence publicationFence,
      boolean deleteDirectPredecessor) {}

  private record PrewrittenIndexWrite(
      String pointerKey, String targetStorageId, String blobUri, long blobBytes) {}

  private final PointerStore pointerStore;
  private final BlobStore blobStore;
  private final ImmutableBlobCache blobCache;
  private final TableBlobReachabilityGuard reachabilityGuard;
  private final GenerationArtifactMap generationArtifactMap;

  @Inject
  public IndexArtifactRepository(
      PointerStore pointerStore,
      BlobStore blobStore,
      ImmutableBlobCache blobCache,
      TableBlobReachabilityGuard reachabilityGuard) {
    this.pointerStore = pointerStore;
    this.blobStore = blobStore;
    this.blobCache = blobCache;
    this.reachabilityGuard = reachabilityGuard;
    this.generationArtifactMap = new GenerationArtifactMap(pointerStore, blobStore, blobCache);
  }

  public void putIndexArtifact(IndexArtifactRecord value) {
    requireValidRecord(value);
    ResourceId tableId = value.getTableId();
    reachabilityGuard.publishing(
        tableId,
        () -> {
          putIndexArtifactGuarded(value);
          return null;
        });
  }

  public MutationMeta putIndexArtifactWithCompletion(
      IndexArtifactRecord value,
      Timestamp now,
      Function<MutationMeta, List<PointerStore.CasOp>> completionFactory,
      Consumer<List<PointerStore.CasOp>> completionDiscarder) {
    requireValidRecord(value);
    return reachabilityGuard.publishing(
        value.getTableId(),
        () ->
            putIndexArtifactWithCompletionGuarded(
                value, now, completionFactory, completionDiscarder));
  }

  private MutationMeta putIndexArtifactWithCompletionGuarded(
      IndexArtifactRecord value,
      Timestamp now,
      Function<MutationMeta, List<PointerStore.CasOp>> completionFactory,
      Consumer<List<PointerStore.CasOp>> completionDiscarder) {
    ResourceId tableId = value.getTableId();
    validateManagedSidecars(
        tableId,
        () -> new Keys.GenerationKey(value.getSnapshotId(), DIRECT_GENERATION),
        null,
        Set.of(),
        List.of(value));
    Optional<String> active = activeGeneration(tableId, value.getSnapshotId());
    if (active.isPresent() && !DIRECT_GENERATION.equals(active.get())) {
      throw new IllegalStateException(
          "direct index artifact writes cannot mutate finalized generation " + active.get());
    }
    if (active.isEmpty() && !activateDirectGenerationIfAbsent(tableId, value.getSnapshotId())) {
      active = activeGeneration(tableId, value.getSnapshotId());
      if (active.filter(DIRECT_GENERATION::equals).isEmpty()) {
        throw new BaseResourceRepository.AbortRetryableException(
            "direct index artifact generation activation conflicted");
      }
    }
    String activePointerKey =
        Keys.snapshotIndexArtifactActiveGenerationPointer(
            tableId.getAccountId(), tableId.getId(), value.getSnapshotId());
    Pointer activePointer = pointerStore.get(activePointerKey).orElse(null);
    if (activePointer == null || !DIRECT_GENERATION.equals(activePointer.getBlobUri())) {
      throw new BaseResourceRepository.AbortRetryableException(
          "direct index artifact generation changed before commit");
    }

    String targetStorageId = indexArtifactTargetStorageId(value.getTarget());
    byte[] bytes = value.toByteArray();
    String blobUri =
        Keys.snapshotIndexArtifactGenerationBlobUri(
            tableId.getAccountId(),
            tableId.getId(),
            value.getSnapshotId(),
            DIRECT_GENERATION,
            targetStorageId,
            Hashing.sha256Hex(bytes));
    boolean blobExistedBefore = blobStore.head(blobUri).isPresent();
    blobStore.put(blobUri, bytes, "application/x-protobuf");
    String pointerKey =
        generationPointer(tableId, value.getSnapshotId(), DIRECT_GENERATION, targetStorageId);
    Pointer current = pointerStore.get(pointerKey).orElse(null);
    long expectedVersion = current == null ? 0L : current.getVersion();
    long pointerVersion = expectedVersion + 1L;
    MutationMeta meta =
        MutationMeta.newBuilder()
            .setPointerKey(pointerKey)
            .setBlobUri(blobUri)
            .setPointerVersion(pointerVersion)
            .setUpdatedAt(now)
            .build();
    List<PointerStore.CasOp> ops = new ArrayList<>();
    ops.add(new PointerStore.CasCheck(activePointerKey, activePointer.getVersion()));
    ops.add(
        new PointerStore.CasUpsert(
            pointerKey,
            expectedVersion,
            PointerReferences.blobPointer(pointerKey, blobUri, pointerVersion, bytes.length)));
    List<PointerStore.CasOp> completionOps = completionFactory.apply(meta);
    ops.addAll(completionOps);
    final boolean committed;
    try {
      committed =
          AccountDeletionFence.compareAndSetBatch(pointerStore, tableId.getAccountId(), ops);
    } catch (StorageTransactionConflictException confirmedAbort) {
      completionDiscarder.accept(completionOps);
      if (!blobExistedBefore) {
        deleteBlobQuietly(blobUri);
      }
      throw confirmedAbort;
    }
    if (!committed) {
      completionDiscarder.accept(completionOps);
      if (!blobExistedBefore) {
        deleteBlobQuietly(blobUri);
      }
      throw new BaseResourceRepository.AbortRetryableException(
          "index artifact changed while committing idempotency receipt");
    }
    return meta;
  }

  private void putIndexArtifactGuarded(IndexArtifactRecord value) {
    ResourceId tableId = value.getTableId();
    validateManagedSidecars(
        tableId,
        () -> new Keys.GenerationKey(value.getSnapshotId(), DIRECT_GENERATION),
        null,
        Set.of(),
        List.of(value));
    String targetStorageId = indexArtifactTargetStorageId(value.getTarget());
    byte[] bytes = value.toByteArray();
    String blobSha256 = Hashing.sha256Hex(bytes);
    for (int attempt = 0; attempt < 4; attempt++) {
      Optional<String> before = activeGeneration(tableId, value.getSnapshotId());
      if (before.isPresent() && !DIRECT_GENERATION.equals(before.get())) {
        throw new IllegalStateException(
            "direct index artifact writes cannot mutate finalized generation " + before.get());
      }
      String generationId = before.orElse(DIRECT_GENERATION);
      String blobUri =
          Keys.snapshotIndexArtifactGenerationBlobUri(
              tableId.getAccountId(),
              tableId.getId(),
              value.getSnapshotId(),
              generationId,
              targetStorageId,
              blobSha256);
      blobStore.put(blobUri, bytes, "application/x-protobuf");
      try {
        registerWrites(
            tableId,
            List.of(
                new PrewrittenIndexWrite(
                    generationPointer(
                        tableId, value.getSnapshotId(), generationId, targetStorageId),
                    targetStorageId,
                    blobUri,
                    bytes.length)));
      } catch (BaseResourceRepository.AccountDeletionInProgressException deleting) {
        deleteBlobQuietly(blobUri);
        throw deleting;
      }
      Optional<String> after = activeGeneration(tableId, value.getSnapshotId());
      if (before.equals(after) && after.isPresent()) {
        return;
      }
      if (before.isEmpty() && after.isEmpty()) {
        if (activateDirectGenerationIfAbsent(tableId, value.getSnapshotId())) {
          return;
        }
        after = activeGeneration(tableId, value.getSnapshotId());
      }
      if (after.filter(generationId::equals).isPresent()) {
        return;
      }
      if (DIRECT_GENERATION.equals(generationId)
          && after.filter(active -> !DIRECT_GENERATION.equals(active)).isPresent()) {
        deleteDirectGenerationPointers(tableId, value.getSnapshotId());
        throw new IllegalStateException(
            "direct index artifact write raced with finalized generation activation");
      }
    }
    throw new BaseResourceRepository.AbortRetryableException(
        "active index artifact generation changed repeatedly for snapshot "
            + value.getSnapshotId());
  }

  public void putIndexArtifactsBatch(List<IndexArtifactRecord> values) {
    if (values == null || values.isEmpty()) {
      return;
    }
    for (IndexArtifactRecord value : values) {
      if (value != null) {
        putIndexArtifact(value);
      }
    }
  }

  public Set<Keys.GenerationKey> inheritedManagedSidecarGenerations(
      ResourceId tableId, Iterable<ReusableArtifactBundleSelection> selections) {
    String tablePrefix = Keys.tableTargetStatsBlobPrefix(tableId.getAccountId(), tableId.getId());
    Set<Keys.GenerationKey> generations = new LinkedHashSet<>();
    for (ReusableArtifactBundleSelection selection : selections) {
      if (selection == null || selection.indexFilePaths().isEmpty()) {
        continue;
      }
      String bundleUri = selection.payloadUri();
      if (!bundleUri.startsWith("/accounts/")) {
        continue;
      }
      Keys.GenerationKey carrierGeneration = Keys.generationFromTargetStatsBlobUri(bundleUri);
      if (!bundleUri.startsWith(tablePrefix)
          || carrierGeneration == null
          || !ReusableArtifactBundleUris.isBundleUri(bundleUri)
          || !ReusableArtifactBundleUris.matchesDigest(bundleUri, selection.payloadSha256())) {
        throw new BaseResourceRepository.CorruptionException(
            "managed reusable index bundle is outside the owning table generation");
      }
      byte[] bytes;
      try {
        bytes = blobStore.get(bundleUri);
      } catch (StorageNotFoundException error) {
        throw new BaseResourceRepository.CorruptionException(
            "managed reusable index bundle is missing: " + bundleUri, error);
      }
      if (bytes == null
          || bytes.length != selection.payloadBytes()
          || !Hashing.sha256Hex(bytes)
              .equals(HexFormat.of().formatHex(selection.payloadSha256()))) {
        throw new BaseResourceRepository.CorruptionException(
            "managed reusable index bundle metadata does not match: " + bundleUri);
      }
      ReusableArtifactBundlePayload bundle;
      try {
        bundle = ReusableArtifactBundles.parse(bytes);
      } catch (InvalidProtocolBufferException error) {
        throw new BaseResourceRepository.CorruptionException(
            "managed reusable index bundle is invalid: " + bundleUri, error);
      } catch (IllegalArgumentException error) {
        throw new BaseResourceRepository.CorruptionException(
            "managed reusable index bundle has an unsupported format: " + bundleUri, error);
      }
      Set<String> requiredPaths = new LinkedHashSet<>(selection.indexFilePaths());
      for (IndexArtifactRecord record : bundle.getIndexArtifactsList()) {
        requireValidRecord(record);
        if (!record.getTableId().getAccountId().equals(tableId.getAccountId())
            || !record.getTableId().getId().equals(tableId.getId())) {
          throw new BaseResourceRepository.CorruptionException(
              "managed reusable index record belongs to another table: " + bundleUri);
        }
        String sidecarUri = record.getArtifactUri();
        if (!sidecarUri.startsWith("/accounts/") || !sidecarUri.contains(Keys.SEG_INDEX_SIDECARS)) {
          continue;
        }
        Keys.GenerationKey sidecarGeneration = Keys.generationFromTargetStatsBlobUri(sidecarUri);
        if (!sidecarUri.startsWith(tablePrefix) || sidecarGeneration == null) {
          throw new BaseResourceRepository.CorruptionException(
              "managed reusable index sidecar is outside the owning table generation");
        }
        if (record.hasTarget()
            && record.getTarget().hasFile()
            && requiredPaths.remove(record.getTarget().getFile().getFilePath())) {
          generations.add(sidecarGeneration);
        }
      }
      if (!requiredPaths.isEmpty()) {
        throw new BaseResourceRepository.CorruptionException(
            "managed reusable index bundle is missing selected records: " + bundleUri);
      }
    }
    return Set.copyOf(generations);
  }

  /**
   * Stages references to Floecat-owned protobuf wrappers. Before publishing the pointers, validates
   * each wrapper or bundle and refreshes any table-owned shared sidecars while holding the table
   * publication guard.
   */
  void registerPrewrittenIndexArtifactReferencesInGeneration(
      ResourceId tableId,
      long snapshotId,
      String generationId,
      String requiredBlobPrefix,
      List<PrewrittenIndexArtifactReference> references) {
    registerPrewrittenIndexArtifactReferencesInGeneration(
        tableId, snapshotId, generationId, requiredBlobPrefix, null, Set.of(), references);
  }

  public void registerPrewrittenIndexArtifactReferencesInGeneration(
      ResourceId tableId,
      long snapshotId,
      String generationId,
      String requiredBlobPrefix,
      String requiredManagedSidecarPrefix,
      Set<Keys.GenerationKey> inheritedSidecarGenerations,
      List<PrewrittenIndexArtifactReference> references) {
    reachabilityGuard.publishing(
        tableId,
        () -> {
          registerPrewrittenIndexArtifactReferencesInGenerationGuarded(
              tableId,
              snapshotId,
              generationId,
              requiredBlobPrefix,
              requiredManagedSidecarPrefix,
              inheritedSidecarGenerations,
              references);
          return null;
        });
  }

  private void registerPrewrittenIndexArtifactReferencesInGenerationGuarded(
      ResourceId tableId,
      long snapshotId,
      String generationId,
      String requiredBlobPrefix,
      String requiredManagedSidecarPrefix,
      Set<Keys.GenerationKey> inheritedSidecarGenerations,
      List<PrewrittenIndexArtifactReference> references) {
    if (requiredBlobPrefix == null || requiredBlobPrefix.isBlank()) {
      throw new IllegalArgumentException("requiredBlobPrefix is required");
    }
    if (requiredManagedSidecarPrefix != null && requiredManagedSidecarPrefix.isBlank()) {
      throw new IllegalArgumentException("requiredManagedSidecarPrefix must not be blank");
    }
    Set<Keys.GenerationKey> allowedInheritedGenerations =
        inheritedSidecarGenerations == null ? Set.of() : Set.copyOf(inheritedSidecarGenerations);
    LinkedHashMap<String, PrewrittenIndexWrite> unique = new LinkedHashMap<>();
    for (PrewrittenIndexArtifactReference reference :
        references == null ? List.<PrewrittenIndexArtifactReference>of() : references) {
      boolean bundled =
          reference != null
              && reference.blobUri() != null
              && ReusableArtifactBundleUris.isBundleUri(reference.blobUri());
      String bundledPrefix =
          requiredBlobPrefix.endsWith("index-artifacts/")
              ? requiredBlobPrefix.substring(
                  0, requiredBlobPrefix.length() - "index-artifacts/".length())
              : requiredBlobPrefix;
      if (reference == null
          || reference.targetStorageId() == null
          || reference.targetStorageId().isBlank()
          || reference.blobUri() == null
          || !reference.blobUri().startsWith(bundled ? bundledPrefix : requiredBlobPrefix)
          || reference.blobBytes() <= 0L
          || reference.blobSha256() == null
          || reference.blobSha256().length != 32
          || (bundled
              && !ReusableArtifactBundleUris.matchesDigest(
                  reference.blobUri(), reference.blobSha256()))
          || (!bundled
              && !reference
                  .blobUri()
                  .endsWith(
                      "/"
                          + Hashing.sha256Hex(reference.targetStorageId())
                          + "/"
                          + HexFormat.of().formatHex(reference.blobSha256())
                          + ".pb"))) {
        throw new IllegalArgumentException("invalid prewritten index artifact reference");
      }
      String pointerKey =
          generationPointer(tableId, snapshotId, generationId, reference.targetStorageId());
      PrewrittenIndexWrite write =
          new PrewrittenIndexWrite(
              pointerKey, reference.targetStorageId(), reference.blobUri(), reference.blobBytes());
      PrewrittenIndexWrite duplicate = unique.putIfAbsent(pointerKey, write);
      if (duplicate != null && !duplicate.equals(write)) {
        throw new IllegalArgumentException(
            "duplicate prewritten index artifact reference has different content");
      }
    }
    validatePrewrittenManagedSidecars(
        tableId,
        snapshotId,
        requiredManagedSidecarPrefix,
        allowedInheritedGenerations,
        unique.values());
    registerWrites(tableId, new ArrayList<>(unique.values()));
  }

  public ActivationFence activateGeneration(
      ResourceId tableId, long snapshotId, String generationId, byte[] captureManifestBytes) {
    return activateGeneration(
        tableId,
        snapshotId,
        generationId,
        captureManifestBytes,
        captureGenerationInput(tableId, snapshotId, List.of()).predecessor(),
        false);
  }

  public ActivationFence activateGeneration(
      ResourceId tableId,
      long snapshotId,
      String generationId,
      byte[] captureManifestBytes,
      GenerationPredecessor predecessor,
      boolean inheritCoverage) {
    PreparedActivation prepared =
        prepareGenerationActivation(
            tableId, snapshotId, generationId, captureManifestBytes, predecessor, inheritCoverage);
    if (prepared.publicationFence() != null) {
      List<PointerStore.CasOp> updates =
          prepared.publicationFence().pointerUpdates().stream()
              .map(
                  update ->
                      (PointerStore.CasOp)
                          new PointerStore.CasUpsert(
                              update.pointerKey(), update.expectedVersion(), update.next()))
              .toList();
      if (!AccountDeletionFence.compareAndSetBatch(pointerStore, tableId.getAccountId(), updates)) {
        throw new BaseResourceRepository.AbortRetryableException(
            "index artifact generation activation conflicted for snapshot " + snapshotId);
      }
    }
    completePreparedGenerationActivation(tableId, snapshotId, prepared);
    return prepared.activationFence();
  }

  public PreparedActivation prepareGenerationActivation(
      ResourceId tableId,
      long snapshotId,
      String generationId,
      byte[] captureManifestBytes,
      GenerationPredecessor predecessor,
      boolean inheritCoverage) {
    if (generationId == null || generationId.isBlank()) {
      throw new IllegalArgumentException("generationId is required");
    }
    if (captureManifestBytes == null || captureManifestBytes.length == 0) {
      throw new IllegalArgumentException("capture manifest bytes are required");
    }
    if (inheritCoverage) {
      throw new IllegalArgumentException(
          "generation coverage must be materialized by the snapshot finalizer");
    }
    byte[] effectiveManifestBytes = captureManifestBytes;
    String captureManifestUri =
        Keys.snapshotIndexArtifactCaptureManifestBlobUri(
            tableId.getAccountId(),
            tableId.getId(),
            snapshotId,
            Hashing.sha256Hex(effectiveManifestBytes));
    if (blobStore.head(captureManifestUri).isEmpty()) {
      throw new BaseResourceRepository.AbortRetryableException(
          "prewritten index capture manifest is unavailable: " + captureManifestUri);
    }
    String activePointerKey =
        Keys.snapshotIndexArtifactActiveGenerationPointer(
            tableId.getAccountId(), tableId.getId(), snapshotId);
    String manifestPointerKey =
        Keys.snapshotIndexArtifactCaptureManifestPointer(
            tableId.getAccountId(), tableId.getId(), snapshotId);
    Pointer active = pointerStore.get(activePointerKey).orElse(null);
    Pointer manifest = pointerStore.get(manifestPointerKey).orElse(null);
    if (active != null
        && generationId.equals(active.getBlobUri())
        && manifest != null
        && captureManifestUri.equals(manifest.getBlobUri())) {
      return new PreparedActivation(
          new ActivationFence(activePointerKey, generationId, active.getVersion()),
          null,
          DIRECT_GENERATION.equals(predecessor.generationId()));
    }
    if (!matches(active, predecessor.generationId(), predecessor.activePointerVersion())
        || !matches(
            manifest,
            predecessor.captureManifestUri(),
            predecessor.captureManifestPointerVersion())) {
      throw new BaseResourceRepository.AbortRetryableException(
          "index artifact generation predecessor changed for snapshot " + snapshotId);
    }
    Pointer nextActive =
        PointerReferences.opaqueMarkerPointer(
            activePointerKey, generationId, predecessor.activePointerVersion() + 1L);
    Pointer nextManifest =
        PointerReferences.blobPointer(
            manifestPointerKey,
            captureManifestUri,
            predecessor.captureManifestPointerVersion() + 1L,
            effectiveManifestBytes.length);
    return new PreparedActivation(
        new ActivationFence(activePointerKey, generationId, nextActive.getVersion()),
        new StatsStore.PublicationFence(
            List.of(
                new StatsStore.PublicationPointerUpdate(
                    activePointerKey, predecessor.activePointerVersion(), nextActive),
                new StatsStore.PublicationPointerUpdate(
                    manifestPointerKey,
                    predecessor.captureManifestPointerVersion(),
                    nextManifest))),
        DIRECT_GENERATION.equals(predecessor.generationId()));
  }

  public void completePreparedGenerationActivation(
      ResourceId tableId, long snapshotId, PreparedActivation prepared) {
    if (prepared != null && prepared.deleteDirectPredecessor()) {
      deleteDirectGenerationPointers(tableId, snapshotId);
    }
  }

  public boolean indexCaptureComplete(
      ResourceId tableId, long snapshotId, Set<String> requestedSelectors) {
    Optional<String> generationId = activeGeneration(tableId, snapshotId);
    if (generationId.isEmpty()) {
      return false;
    }
    String manifestPointerKey =
        Keys.snapshotIndexArtifactCaptureManifestPointer(
            tableId.getAccountId(), tableId.getId(), snapshotId);
    Pointer manifestPointer = pointerStore.get(manifestPointerKey).orElse(null);
    if (manifestPointer == null || manifestPointer.getBlobUri().isBlank()) {
      return false;
    }
    SnapshotCaptureManifest manifest =
        loadCaptureManifest(manifestPointer.getBlobUri()).orElse(null);
    if (manifest == null) {
      return false;
    }
    if (manifest.getFormatVersion() != 1
        || !tableId.getAccountId().equals(manifest.getAccountId())
        || !tableId.getId().equals(manifest.getTableId())
        || snapshotId != manifest.getSnapshotId()
        || !generationId.get().equals("full-rescan-" + manifest.getParentJobId())
        || !manifest
            .getCapturePolicy()
            .getOutputsList()
            .contains(CaptureOutput.CO_PARQUET_PAGE_INDEX)
        || manifest.getIndexArtifactCount() != manifest.getSourceFileCount()) {
      return false;
    }
    Set<String> capturedSelectors = new LinkedHashSet<>();
    manifest
        .getCapturePolicy()
        .getColumnsList()
        .forEach(
            column -> {
              if (column.getCaptureIndex() && !column.getSelector().isBlank()) {
                capturedSelectors.add(column.getSelector().trim());
              }
            });
    boolean capturesAllColumns =
        manifest.getCapturePolicy().getDefaultColumnScope() == DefaultColumnScope.DCS_ALL;
    return capturesAllColumns
        || requestedSelectors == null
        || requestedSelectors.isEmpty()
        || capturedSelectors.containsAll(requestedSelectors);
  }

  private Optional<SnapshotCaptureManifest> loadCaptureManifest(String uri) {
    return blobCache == null
        ? loadCaptureManifestUncached(uri)
        : blobCache.get(uri, this::loadCaptureManifestUncached);
  }

  private Optional<SnapshotCaptureManifest> loadCaptureManifestUncached(String uri) {
    try {
      byte[] bytes = blobStore.get(uri);
      return bytes == null
          ? Optional.empty()
          : Optional.of(SnapshotCaptureManifest.parseFrom(bytes));
    } catch (StorageNotFoundException e) {
      return Optional.empty();
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException("invalid snapshot capture manifest at " + uri, e);
    }
  }

  public GenerationInput captureGenerationInput(
      ResourceId tableId, long snapshotId, List<String> filePaths) {
    String activePointerKey =
        Keys.snapshotIndexArtifactActiveGenerationPointer(
            tableId.getAccountId(), tableId.getId(), snapshotId);
    String manifestPointerKey =
        Keys.snapshotIndexArtifactCaptureManifestPointer(
            tableId.getAccountId(), tableId.getId(), snapshotId);
    for (int attempt = 0; attempt < 4; attempt++) {
      Pointer active = pointerStore.get(activePointerKey).orElse(null);
      Pointer manifest = pointerStore.get(manifestPointerKey).orElse(null);
      String generationId = active == null ? "" : active.getBlobUri();
      List<IndexArtifactRecord> artifacts = new ArrayList<>();
      if (!generationId.isBlank()) {
        for (String filePath : filePaths == null ? List.<String>of() : filePaths) {
          String targetStorageId = "file:" + filePath;
          findGenerationPointer(tableId, snapshotId, generationId, targetStorageId)
              .map(pointer -> readRecord(pointer, tableId, snapshotId))
              .ifPresent(artifacts::add);
        }
      }
      Pointer activeAfter = pointerStore.get(activePointerKey).orElse(null);
      Pointer manifestAfter = pointerStore.get(manifestPointerKey).orElse(null);
      if (samePointer(active, activeAfter) && samePointer(manifest, manifestAfter)) {
        return new GenerationInput(
            new GenerationPredecessor(
                generationId,
                active == null ? 0L : active.getVersion(),
                manifest == null ? "" : manifest.getBlobUri(),
                manifest == null ? 0L : manifest.getVersion()),
            List.copyOf(artifacts));
      }
    }
    throw new BaseResourceRepository.AbortRetryableException(
        "active index artifact generation changed repeatedly for snapshot " + snapshotId);
  }

  public GenerationInput loadGenerationInput(
      ResourceId tableId,
      long snapshotId,
      GenerationPredecessor predecessor,
      List<String> filePaths) {
    if (predecessor == null) {
      throw new IllegalArgumentException("index generation predecessor is required");
    }
    List<IndexArtifactRecord> artifacts = new ArrayList<>();
    if (!predecessor.generationId().isBlank()) {
      for (String filePath : filePaths == null ? List.<String>of() : filePaths) {
        String targetStorageId = "file:" + filePath;
        findGenerationPointer(tableId, snapshotId, predecessor.generationId(), targetStorageId)
            .map(pointer -> readRecord(pointer, tableId, snapshotId))
            .ifPresent(artifacts::add);
      }
    }
    return new GenerationInput(predecessor, List.copyOf(artifacts));
  }

  private static boolean samePointer(Pointer left, Pointer right) {
    if (left == null || right == null) {
      return left == right;
    }
    return left.getVersion() == right.getVersion()
        && java.util.Objects.equals(left.getBlobUri(), right.getBlobUri());
  }

  private static boolean matches(Pointer pointer, String value, long version) {
    if (pointer == null) {
      return version == 0L && (value == null || value.isBlank());
    }
    return pointer.getVersion() == version
        && java.util.Objects.equals(pointer.getBlobUri(), value == null ? "" : value);
  }

  private boolean activateDirectGenerationIfAbsent(ResourceId tableId, long snapshotId) {
    String pointerKey =
        Keys.snapshotIndexArtifactActiveGenerationPointer(
            tableId.getAccountId(), tableId.getId(), snapshotId);
    return AccountDeletionFence.compareAndSet(
        pointerStore,
        tableId.getAccountId(),
        pointerKey,
        0L,
        PointerReferences.opaqueMarkerPointer(pointerKey, DIRECT_GENERATION, 1L));
  }

  private void deleteDirectGenerationPointers(ResourceId tableId, long snapshotId) {
    pointerStore.deleteByPrefix(
        Keys.snapshotIndexArtifactGenerationPrefix(
            tableId.getAccountId(), tableId.getId(), snapshotId, DIRECT_GENERATION));
  }

  public Optional<IndexArtifactRecord> getIndexArtifact(
      ResourceId tableId, long snapshotId, IndexTarget target) {
    return activeGeneration(tableId, snapshotId)
        .flatMap(
            generationId ->
                findGenerationPointer(
                        tableId, snapshotId, generationId, indexArtifactTargetStorageId(target))
                    .map(pointer -> readRecord(pointer, tableId, snapshotId)));
  }

  public List<IndexArtifactRecord> listIndexArtifacts(
      ResourceId tableId, long snapshotId, int limit, String pageToken, StringBuilder nextOut) {
    Optional<String> generationId = activeGeneration(tableId, snapshotId);
    if (generationId.isEmpty()) {
      if (nextOut != null) {
        nextOut.setLength(0);
      }
      return List.of();
    }
    IndexListToken token = decodeIndexListToken(pageToken);
    if (token != null && !generationId.get().equals(token.generationId())) {
      throw new BaseResourceRepository.AbortRetryableException(
          "index artifact generation changed while listing snapshot " + snapshotId);
    }
    String backendToken = token == null ? "" : token.backendToken();
    if (generationArtifactMap.manifest(tableId, snapshotId, generationId.get()).isPresent()) {
      ReusableArtifactIndexStore.EntryPage page =
          generationArtifactMap.page(
              tableId,
              snapshotId,
              generationId.get(),
              ReusableArtifactIndexStore.EntryKind.INDEX_ARTIFACT,
              Math.max(1, limit),
              backendToken);
      if (nextOut != null) {
        nextOut.setLength(0);
        if (!page.nextPageToken().isBlank()) {
          nextOut.append(encodeIndexListToken(generationId.get(), page.nextPageToken()));
        }
      }
      return page.entries().stream()
          .map(
              entry ->
                  readRecord(
                      PointerReferences.blobPointer(
                          generationPointer(
                              tableId,
                              snapshotId,
                              generationId.get(),
                              "file:" + entry.getIndexArtifact().getFilePath()),
                          entry.getArtifact().getPayloadUri(),
                          1L,
                          entry.getArtifact().getPayloadBytes()),
                      tableId,
                      snapshotId))
          .toList();
    }
    StringBuilder backendNext = new StringBuilder();
    List<Pointer> pointers =
        pointerStore.listPointersByPrefix(
            Keys.snapshotIndexArtifactGenerationPrefix(
                tableId.getAccountId(), tableId.getId(), snapshotId, generationId.get()),
            Math.max(1, limit),
            backendToken,
            backendNext);
    if (nextOut != null) {
      nextOut.setLength(0);
      if (!backendNext.isEmpty()) {
        nextOut.append(encodeIndexListToken(generationId.get(), backendNext.toString()));
      }
    }
    return pointers.stream().map(pointer -> readRecord(pointer, tableId, snapshotId)).toList();
  }

  private static String encodeIndexListToken(String generationId, String backendToken) {
    String payload = generationId + "\n" + backendToken;
    return LIST_TOKEN_PREFIX
        + Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(payload.getBytes(StandardCharsets.UTF_8));
  }

  private static IndexListToken decodeIndexListToken(String token) {
    if (token == null || token.isBlank()) {
      return null;
    }
    if (!token.startsWith(LIST_TOKEN_PREFIX)) {
      throw new IllegalArgumentException("invalid index artifact page token");
    }
    try {
      String payload =
          new String(
              Base64.getUrlDecoder().decode(token.substring(LIST_TOKEN_PREFIX.length())),
              StandardCharsets.UTF_8);
      int split = payload.indexOf('\n');
      if (split <= 0 || split == payload.length() - 1) {
        throw new IllegalArgumentException("invalid index artifact page token");
      }
      return new IndexListToken(payload.substring(0, split), payload.substring(split + 1));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("invalid index artifact page token", e);
    }
  }

  private record IndexListToken(String generationId, String backendToken) {}

  public int countIndexArtifacts(ResourceId tableId, long snapshotId) {
    Optional<String> active = activeGeneration(tableId, snapshotId);
    if (active.isEmpty()) {
      return 0;
    }
    if (generationArtifactMap.manifest(tableId, snapshotId, active.orElseThrow()).isPresent()) {
      return generationArtifactMap.countIndexes(tableId, snapshotId, active.orElseThrow());
    }
    return pointerStore.countByPrefix(
        Keys.snapshotIndexArtifactGenerationPrefix(
            tableId.getAccountId(), tableId.getId(), snapshotId, active.orElseThrow()));
  }

  public MutationMeta metaForIndexArtifact(
      ResourceId tableId, long snapshotId, IndexTarget target, Timestamp nowTs) {
    String generationId =
        activeGeneration(tableId, snapshotId)
            .orElseThrow(
                () ->
                    new BaseResourceRepository.NotFoundException(
                        "active index artifact generation is missing"));
    Pointer pointer =
        findGenerationPointer(
                tableId, snapshotId, generationId, indexArtifactTargetStorageId(target))
            .orElseThrow(
                () ->
                    new BaseResourceRepository.NotFoundException(
                        "index artifact pointer is missing"));
    String pointerKey = pointer.getKey();
    String etag = blobStore.head(pointer.getBlobUri()).map(header -> header.getEtag()).orElse("");
    return MutationMeta.newBuilder()
        .setPointerKey(pointerKey)
        .setBlobUri(pointer.getBlobUri())
        .setPointerVersion(pointer.getVersion())
        .setEtag(etag)
        .setUpdatedAt(nowTs)
        .build();
  }

  private void registerWrites(ResourceId tableId, List<PrewrittenIndexWrite> writes) {
    for (int from = 0; from < writes.size(); from += MAX_POINTER_BATCH_SIZE) {
      registerChunk(
          tableId, writes.subList(from, Math.min(from + MAX_POINTER_BATCH_SIZE, writes.size())));
    }
  }

  private void validatePrewrittenManagedSidecars(
      ResourceId tableId,
      long snapshotId,
      String requiredManagedSidecarPrefix,
      Set<Keys.GenerationKey> inheritedSidecarGenerations,
      Iterable<PrewrittenIndexWrite> writes) {
    LinkedHashMap<String, List<PrewrittenIndexWrite>> writesByBlob = new LinkedHashMap<>();
    for (PrewrittenIndexWrite write : writes) {
      writesByBlob.computeIfAbsent(write.blobUri(), ignored -> new ArrayList<>()).add(write);
    }
    for (var entry : writesByBlob.entrySet()) {
      String blobUri = entry.getKey();
      List<PrewrittenIndexWrite> blobWrites = entry.getValue();
      if (ReusableArtifactBundleUris.isBundleUri(blobUri)) {
        ReusableArtifactBundlePayload bundle =
            loadCachedReusableArtifactBundle(blobUri)
                .orElseThrow(
                    () ->
                        new BaseResourceRepository.CorruptionException(
                            "prewritten index artifact bundle is missing: " + blobUri));
        for (IndexArtifactRecord record : bundle.getIndexArtifactsList()) {
          requirePrewrittenRecordMatches(tableId, snapshotId, record, blobUri, false);
        }
        validateManagedSidecars(
            tableId,
            () -> requireCarrierGeneration(tableId, blobUri),
            requiredManagedSidecarPrefix,
            inheritedSidecarGenerations,
            bundle.getIndexArtifactsList());
        Set<String> inMemoryBundledTargets = new java.util.HashSet<>();
        for (IndexArtifactRecord record : bundle.getIndexArtifactsList()) {
          if (!inMemoryBundledTargets.add(indexArtifactTargetStorageId(record.getTarget()))) {
            throw new BaseResourceRepository.CorruptionException(
                "prewritten index artifact bundle contains a duplicate target: " + blobUri);
          }
        }
        for (PrewrittenIndexWrite write : blobWrites) {
          if (!inMemoryBundledTargets.contains(write.targetStorageId())) {
            throw new BaseResourceRepository.CorruptionException(
                "prewritten index artifact bundle has no record for target "
                    + write.targetStorageId()
                    + ": "
                    + blobUri);
          }
        }
        continue;
      }
      try {
        byte[] bytes = blobStore.get(blobUri);
        if (bytes == null) {
          throw new StorageNotFoundException(blobUri);
        }
        if (!blobUri.endsWith("/" + Hashing.sha256Hex(bytes) + ".pb")) {
          throw new BaseResourceRepository.CorruptionException(
              "prewritten index artifact content address does not match payload: " + blobUri);
        }
        IndexArtifactRecord record = IndexArtifactRecord.parseFrom(bytes);
        requirePrewrittenRecordMatches(tableId, snapshotId, record, blobUri, false);
        if (!blobWrites
            .getFirst()
            .targetStorageId()
            .equals(indexArtifactTargetStorageId(record.getTarget()))) {
          throw new BaseResourceRepository.CorruptionException(
              "prewritten index artifact target does not match reference: " + blobUri);
        }
        validateManagedSidecars(
            tableId,
            () -> requireCarrierGeneration(tableId, blobUri),
            requiredManagedSidecarPrefix,
            inheritedSidecarGenerations,
            List.of(record));
      } catch (StorageNotFoundException e) {
        throw new BaseResourceRepository.CorruptionException(
            "prewritten index artifact is missing: " + blobUri, e);
      } catch (InvalidProtocolBufferException e) {
        throw new BaseResourceRepository.CorruptionException(
            "prewritten index artifact is invalid: " + blobUri, e);
      }
    }
  }

  private Optional<ReusableArtifactBundlePayload> loadCachedReusableArtifactBundle(
      String bundleUri) {
    Optional<ReusableArtifactBundlePayload> bundle = loadReusableArtifactBundle(bundleUri);
    if (blobCache != null && blobCache.enabled()) {
      bundle.ifPresent(value -> blobCache.put(bundleUri, value));
    }
    return bundle;
  }

  private static void requirePrewrittenRecordMatches(
      ResourceId tableId,
      long snapshotId,
      IndexArtifactRecord record,
      String blobUri,
      boolean allowSnapshotRebind) {
    requireValidRecord(record);
    if (!record.getTableId().getAccountId().equals(tableId.getAccountId())
        || !record.getTableId().getId().equals(tableId.getId())
        || (!allowSnapshotRebind && record.getSnapshotId() != snapshotId)) {
      throw new BaseResourceRepository.CorruptionException(
          "prewritten index artifact belongs to a different table or snapshot: " + blobUri);
    }
  }

  private static Keys.GenerationKey requireCarrierGeneration(
      ResourceId tableId, String carrierBlobUri) {
    String requiredPrefix =
        Keys.tableTargetStatsBlobPrefix(tableId.getAccountId(), tableId.getId());
    Keys.GenerationKey generation = Keys.generationFromTargetStatsBlobUri(carrierBlobUri);
    if (!carrierBlobUri.startsWith(requiredPrefix) || generation == null) {
      throw new BaseResourceRepository.CorruptionException(
          "index artifact carrier is outside an owning generation: " + carrierBlobUri);
    }
    return generation;
  }

  private void validateManagedSidecars(
      ResourceId tableId,
      Supplier<Keys.GenerationKey> carrierGenerationSupplier,
      String requiredManagedSidecarPrefix,
      Set<Keys.GenerationKey> inheritedSidecarGenerations,
      Iterable<IndexArtifactRecord> artifacts) {
    String requiredPrefix =
        Keys.tableTargetStatsBlobPrefix(tableId.getAccountId(), tableId.getId());
    Keys.GenerationKey carrierGeneration = null;
    Set<Keys.GenerationKey> allowedInheritedGenerations =
        inheritedSidecarGenerations == null ? Set.of() : inheritedSidecarGenerations;
    Set<String> inspected = new LinkedHashSet<>();
    for (IndexArtifactRecord artifact : artifacts) {
      String uri = artifact == null ? "" : artifact.getArtifactUri();
      if (!uri.startsWith("/accounts/") || !uri.contains(Keys.SEG_INDEX_SIDECARS)) {
        continue;
      }
      if (carrierGeneration == null) {
        carrierGeneration = carrierGenerationSupplier.get();
      }
      Keys.GenerationKey sidecarGeneration = Keys.generationFromTargetStatsBlobUri(uri);
      if (!uri.startsWith(requiredPrefix)
          || sidecarGeneration == null
          || (!sidecarGeneration.equals(carrierGeneration)
              && !allowedInheritedGenerations.contains(sidecarGeneration))) {
        throw new BaseResourceRepository.CorruptionException(
            "index artifact sidecar is outside its carrier or inherited generation: " + uri);
      }
      if (requiredManagedSidecarPrefix != null
          && sidecarGeneration.equals(carrierGeneration)
          && !uri.startsWith(requiredManagedSidecarPrefix)) {
        throw new BaseResourceRepository.CorruptionException(
            "index artifact sidecar is outside its producer prefix: " + uri);
      }
      if (!inspected.add(uri)) {
        continue;
      }
    }
    if (inspected.isEmpty()) {
      return;
    }
    var semaphore = new Semaphore(Math.min(inspected.size(), MAX_PARALLEL_SIDECAR_CHECKS));
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      List<CompletableFuture<Void>> checks =
          inspected.stream()
              .map(
                  uri ->
                      CompletableFuture.runAsync(
                          () -> {
                            semaphore.acquireUninterruptibly();
                            try {
                              requireSharedSidecar(uri);
                            } finally {
                              semaphore.release();
                            }
                          },
                          executor))
              .toList();
      try {
        CompletableFuture.allOf(checks.toArray(CompletableFuture[]::new)).join();
      } catch (CompletionException e) {
        if (e.getCause() instanceof RuntimeException runtimeException) {
          throw runtimeException;
        }
        throw e;
      }
    }
  }

  private void requireSharedSidecar(String uri) {
    // HEAD is sufficient while the table publication guard is held: if GC deleted first, this
    // fails closed; if publication wins, the pointer becomes visible before unlock and the epoch
    // advance forces any overlapping GC proof to restart. Never GET or rewrite a potentially
    // large Parquet sidecar merely to publish its small wrapper.
    if (blobStore.head(uri).isEmpty()) {
      throw new BaseResourceRepository.CorruptionException(
          "shared index sidecar is missing: " + uri);
    }
  }

  private void registerChunk(ResourceId tableId, List<PrewrittenIndexWrite> writes) {
    List<PrewrittenIndexWrite> remaining = new ArrayList<>(writes);
    List<PointerStore.CasOp> initial = new ArrayList<>(remaining.size());
    for (PrewrittenIndexWrite write : remaining) {
      initial.add(
          new PointerStore.CasUpsert(
              write.pointerKey(),
              0L,
              PointerReferences.blobPointer(
                  write.pointerKey(), write.blobUri(), 1L, write.blobBytes())));
    }
    if (AccountDeletionFence.compareAndSetBatch(pointerStore, tableId.getAccountId(), initial)) {
      return;
    }
    for (int attempt = 1; attempt < 4; attempt++) {
      List<PrewrittenIndexWrite> nextRemaining = new ArrayList<>();
      List<PointerStore.CasOp> ops = new ArrayList<>();
      for (PrewrittenIndexWrite write : remaining) {
        Pointer existing = pointerStore.get(write.pointerKey()).orElse(null);
        if (existing != null && write.blobUri().equals(existing.getBlobUri())) {
          continue;
        }
        long expectedVersion = existing == null ? 0L : existing.getVersion();
        nextRemaining.add(write);
        ops.add(
            new PointerStore.CasUpsert(
                write.pointerKey(),
                expectedVersion,
                PointerReferences.blobPointer(
                    write.pointerKey(), write.blobUri(), expectedVersion + 1L, write.blobBytes())));
      }
      if (ops.isEmpty()
          || AccountDeletionFence.compareAndSetBatch(pointerStore, tableId.getAccountId(), ops)) {
        return;
      }
      remaining = nextRemaining;
    }
    throw new BaseResourceRepository.AbortRetryableException(
        "index artifact reference update conflicted repeatedly for "
            + remaining.getFirst().pointerKey());
  }

  private void deleteBlobQuietly(String blobUri) {
    try {
      blobStore.delete(blobUri);
    } catch (RuntimeException ignored) {
      // Best effort: the durable account fence still prevents publishing the blob.
    }
  }

  private Optional<String> activeGeneration(ResourceId tableId, long snapshotId) {
    return pointerStore
        .get(
            Keys.snapshotIndexArtifactActiveGenerationPointer(
                tableId.getAccountId(), tableId.getId(), snapshotId))
        .map(Pointer::getBlobUri)
        .filter(value -> value != null && !value.isBlank());
  }

  private Optional<Pointer> findGenerationPointer(
      ResourceId tableId, long snapshotId, String generationId, String targetStorageId) {
    String pointerKey = generationPointer(tableId, snapshotId, generationId, targetStorageId);
    Optional<Pointer> current = pointerStore.get(pointerKey);
    if (current.isPresent() || !targetStorageId.startsWith("file:")) {
      return current;
    }
    return generationArtifactMap
        .lookupIndex(tableId, snapshotId, generationId, targetStorageId.substring("file:".length()))
        .map(
            entry ->
                PointerReferences.blobPointer(
                    pointerKey,
                    entry.getArtifact().getPayloadUri(),
                    1L,
                    entry.getArtifact().getPayloadBytes()));
  }

  private IndexArtifactRecord readRecord(Pointer pointer, ResourceId tableId, long snapshotId) {
    try {
      if (!ReusableArtifactBundleUris.isBundleUri(pointer.getBlobUri())) {
        return rebindRecord(
            IndexArtifactRecord.parseFrom(blobStore.get(pointer.getBlobUri())),
            tableId,
            snapshotId);
      }
      ReusableArtifactBundlePayload bundle =
          (blobCache == null
                  ? loadReusableArtifactBundle(pointer.getBlobUri())
                  : blobCache.get(pointer.getBlobUri(), this::loadReusableArtifactBundle))
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "missing index artifact bundle at " + pointer.getBlobUri()));
      for (IndexArtifactRecord record : bundle.getIndexArtifactsList()) {
        String targetStorageId = indexArtifactTargetStorageId(record.getTarget());
        if (pointer.getKey().endsWith("/" + Keys.encodeSegment(targetStorageId))) {
          return rebindRecord(record, tableId, snapshotId);
        }
      }
      throw new InvalidProtocolBufferException(
          "index artifact bundle has no record for pointer " + pointer.getKey());
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException(
          "invalid index artifact wrapper at " + pointer.getBlobUri(), e);
    }
  }

  private static IndexArtifactRecord rebindRecord(
      IndexArtifactRecord record, ResourceId tableId, long snapshotId) {
    if (record == null
        || (record.getSnapshotId() == snapshotId && tableId.equals(record.getTableId()))) {
      return record;
    }
    return record.toBuilder().setTableId(tableId).setSnapshotId(snapshotId).build();
  }

  private Optional<ReusableArtifactBundlePayload> loadReusableArtifactBundle(String uri) {
    try {
      byte[] bytes = blobStore.get(uri);
      if (bytes == null) {
        return Optional.empty();
      }
      if (!ReusableArtifactBundleUris.matchesPayload(uri, bytes)) {
        throw new IllegalStateException("reusable artifact bundle digest mismatch: " + uri);
      }
      return Optional.of(ReusableArtifactBundles.parse(bytes));
    } catch (StorageNotFoundException e) {
      return Optional.empty();
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException("invalid reusable artifact bundle at " + uri, e);
    }
  }

  private static String generationPointer(
      ResourceId tableId, long snapshotId, String generationId, String targetStorageId) {
    return Keys.snapshotIndexArtifactGenerationPointer(
        tableId.getAccountId(), tableId.getId(), snapshotId, generationId, targetStorageId);
  }

  private static void requireValidRecord(IndexArtifactRecord value) {
    if (value == null
        || !value.hasTableId()
        || value.getTableId().getId().isBlank()
        || !value.hasTarget()
        || value.getTarget().getTargetCase() == IndexTarget.TargetCase.TARGET_NOT_SET) {
      throw new IllegalArgumentException("table_id and target must be set on IndexArtifactRecord");
    }
  }

  private static String indexArtifactTargetStorageId(IndexTarget target) {
    return switch (target.getTargetCase()) {
      case FILE -> "file:" + target.getFile().getFilePath();
      case TARGET_NOT_SET ->
          throw new IllegalArgumentException("target must be set on IndexArtifactRecord");
    };
  }
}
