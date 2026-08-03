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
import ai.floedb.floecat.reconciler.jobs.ReusableArtifactBundleUris;
import ai.floedb.floecat.reconciler.rpc.CaptureColumnPolicy;
import ai.floedb.floecat.reconciler.rpc.CaptureOutput;
import ai.floedb.floecat.reconciler.rpc.CapturePolicy;
import ai.floedb.floecat.reconciler.rpc.DefaultColumnScope;
import ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundlePayload;
import ai.floedb.floecat.reconciler.rpc.SnapshotCaptureManifest;
import ai.floedb.floecat.service.repo.cache.ImmutableBlobCache;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.storage.errors.StorageNotFoundException;
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

@ApplicationScoped
public class IndexArtifactRepository {
  private static final int MAX_POINTER_BATCH_SIZE = 100;
  private static final String DIRECT_GENERATION = "direct";
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

  private record PrewrittenIndexWrite(String pointerKey, String blobUri, long blobBytes) {}

  private final PointerStore pointerStore;
  private final BlobStore blobStore;
  private final ImmutableBlobCache blobCache;

  @Inject
  public IndexArtifactRepository(
      PointerStore pointerStore, BlobStore blobStore, ImmutableBlobCache blobCache) {
    this.pointerStore = pointerStore;
    this.blobStore = blobStore;
    this.blobCache = blobCache;
  }

  public IndexArtifactRepository(PointerStore pointerStore, BlobStore blobStore) {
    this(pointerStore, blobStore, null);
  }

  public void putIndexArtifact(IndexArtifactRecord value) {
    requireValidRecord(value);
    ResourceId tableId = value.getTableId();
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
      registerWrites(
          List.of(
              new PrewrittenIndexWrite(
                  generationPointer(tableId, value.getSnapshotId(), generationId, targetStorageId),
                  blobUri,
                  bytes.length)));
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

  /**
   * Stages references to Floecat-owned protobuf wrappers. The referenced index sidecar URI inside
   * each wrapper is deliberately not inspected or copied.
   */
  public void registerPrewrittenIndexArtifactReferencesInGeneration(
      ResourceId tableId,
      long snapshotId,
      String generationId,
      String requiredBlobPrefix,
      List<PrewrittenIndexArtifactReference> references) {
    if (requiredBlobPrefix == null || requiredBlobPrefix.isBlank()) {
      throw new IllegalArgumentException("requiredBlobPrefix is required");
    }
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
          new PrewrittenIndexWrite(pointerKey, reference.blobUri(), reference.blobBytes());
      PrewrittenIndexWrite duplicate = unique.putIfAbsent(pointerKey, write);
      if (duplicate != null && !duplicate.equals(write)) {
        throw new IllegalArgumentException(
            "duplicate prewritten index artifact reference has different content");
      }
    }
    registerWrites(new ArrayList<>(unique.values()));
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
      if (!pointerStore.compareAndSetBatch(updates)) {
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
    byte[] effectiveManifestBytes =
        inheritCoverage
            ? mergeCaptureManifestCoverage(predecessor.captureManifestUri(), captureManifestBytes)
            : captureManifestBytes;
    String captureManifestUri =
        Keys.snapshotIndexArtifactCaptureManifestBlobUri(
            tableId.getAccountId(),
            tableId.getId(),
            snapshotId,
            Hashing.sha256Hex(effectiveManifestBytes));
    blobStore.put(captureManifestUri, effectiveManifestBytes, "application/x-protobuf");
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

  private byte[] mergeCaptureManifestCoverage(
      String predecessorManifestUri, byte[] submittedBytes) {
    SnapshotCaptureManifest submitted = parseCaptureManifest(submittedBytes, "submitted");
    if (predecessorManifestUri == null || predecessorManifestUri.isBlank()) {
      return submittedBytes;
    }
    SnapshotCaptureManifest inherited;
    try {
      inherited = SnapshotCaptureManifest.parseFrom(blobStore.get(predecessorManifestUri));
    } catch (StorageNotFoundException e) {
      throw new BaseResourceRepository.AbortRetryableException(
          "predecessor index capture manifest is unavailable: " + predecessorManifestUri);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException(
          "invalid predecessor index capture manifest at " + predecessorManifestUri, e);
    }
    LinkedHashMap<String, CaptureColumnPolicy> columns = new LinkedHashMap<>();
    for (CaptureColumnPolicy column : submitted.getCapturePolicy().getColumnsList()) {
      columns.put(column.getSelector().trim(), column);
    }
    for (CaptureColumnPolicy column : inherited.getCapturePolicy().getColumnsList()) {
      if (!column.getCaptureIndex() || column.getSelector().isBlank()) {
        continue;
      }
      columns.merge(
          column.getSelector().trim(),
          CaptureColumnPolicy.newBuilder()
              .setSelector(column.getSelector().trim())
              .setCaptureIndex(true)
              .build(),
          (current, ignored) -> current.toBuilder().setCaptureIndex(true).build());
    }
    CapturePolicy current = submitted.getCapturePolicy();
    DefaultColumnScope inheritedScope = inherited.getCapturePolicy().getDefaultColumnScope();
    DefaultColumnScope scope =
        mergeDefaultColumnScope(current.getDefaultColumnScope(), inheritedScope);
    CapturePolicy mergedPolicy =
        current.toBuilder()
            .clearColumns()
            .addAllColumns(columns.values())
            .setDefaultColumnScope(scope)
            .setMaxDefaultColumns(
                Math.max(
                    current.getMaxDefaultColumns(),
                    inherited.getCapturePolicy().getMaxDefaultColumns()))
            .build();
    return submitted.toBuilder().setCapturePolicy(mergedPolicy).build().toByteArray();
  }

  private static DefaultColumnScope mergeDefaultColumnScope(
      DefaultColumnScope current, DefaultColumnScope inherited) {
    if (current == DefaultColumnScope.DCS_ALL || inherited == DefaultColumnScope.DCS_ALL) {
      return DefaultColumnScope.DCS_ALL;
    }
    if (current == DefaultColumnScope.DCS_FIRST_N
        || inherited == DefaultColumnScope.DCS_FIRST_N
        || current == DefaultColumnScope.DCS_UNSPECIFIED
        || inherited == DefaultColumnScope.DCS_UNSPECIFIED) {
      return DefaultColumnScope.DCS_FIRST_N;
    }
    return DefaultColumnScope.DCS_EXPLICIT_ONLY;
  }

  private static SnapshotCaptureManifest parseCaptureManifest(byte[] bytes, String description) {
    try {
      return SnapshotCaptureManifest.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException("invalid " + description + " index capture manifest", e);
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
          pointerStore
              .get(generationPointer(tableId, snapshotId, generationId, targetStorageId))
              .map(this::readRecord)
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
        pointerStore
            .get(
                generationPointer(tableId, snapshotId, predecessor.generationId(), targetStorageId))
            .map(this::readRecord)
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
    return pointerStore.compareAndSet(
        pointerKey, 0L, PointerReferences.opaqueMarkerPointer(pointerKey, DIRECT_GENERATION, 1L));
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
                pointerStore
                    .get(
                        generationPointer(
                            tableId,
                            snapshotId,
                            generationId,
                            indexArtifactTargetStorageId(target)))
                    .map(this::readRecord));
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
    return pointers.stream().map(this::readRecord).toList();
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
    return activeGeneration(tableId, snapshotId)
        .map(
            generationId ->
                pointerStore.countByPrefix(
                    Keys.snapshotIndexArtifactGenerationPrefix(
                        tableId.getAccountId(), tableId.getId(), snapshotId, generationId)))
        .orElse(0);
  }

  public MutationMeta metaForIndexArtifact(
      ResourceId tableId, long snapshotId, IndexTarget target, Timestamp nowTs) {
    String generationId =
        activeGeneration(tableId, snapshotId)
            .orElseThrow(
                () ->
                    new BaseResourceRepository.NotFoundException(
                        "active index artifact generation is missing"));
    String pointerKey =
        generationPointer(tableId, snapshotId, generationId, indexArtifactTargetStorageId(target));
    Pointer pointer =
        pointerStore
            .get(pointerKey)
            .orElseThrow(
                () ->
                    new BaseResourceRepository.NotFoundException(
                        "index artifact pointer is missing: " + pointerKey));
    String etag = blobStore.head(pointer.getBlobUri()).map(header -> header.getEtag()).orElse("");
    return MutationMeta.newBuilder()
        .setPointerKey(pointerKey)
        .setBlobUri(pointer.getBlobUri())
        .setPointerVersion(pointer.getVersion())
        .setEtag(etag)
        .setUpdatedAt(nowTs)
        .build();
  }

  private void registerWrites(List<PrewrittenIndexWrite> writes) {
    for (int from = 0; from < writes.size(); from += MAX_POINTER_BATCH_SIZE) {
      registerChunk(writes.subList(from, Math.min(from + MAX_POINTER_BATCH_SIZE, writes.size())));
    }
  }

  private void registerChunk(List<PrewrittenIndexWrite> writes) {
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
    if (pointerStore.compareAndSetBatch(initial)) {
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
      if (ops.isEmpty() || pointerStore.compareAndSetBatch(ops)) {
        return;
      }
      remaining = nextRemaining;
    }
    throw new BaseResourceRepository.AbortRetryableException(
        "index artifact reference update conflicted repeatedly for "
            + remaining.getFirst().pointerKey());
  }

  private Optional<String> activeGeneration(ResourceId tableId, long snapshotId) {
    return pointerStore
        .get(
            Keys.snapshotIndexArtifactActiveGenerationPointer(
                tableId.getAccountId(), tableId.getId(), snapshotId))
        .map(Pointer::getBlobUri)
        .filter(value -> value != null && !value.isBlank());
  }

  private IndexArtifactRecord readRecord(Pointer pointer) {
    try {
      if (!ReusableArtifactBundleUris.isBundleUri(pointer.getBlobUri())) {
        return IndexArtifactRecord.parseFrom(blobStore.get(pointer.getBlobUri()));
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
          return record;
        }
      }
      throw new InvalidProtocolBufferException(
          "index artifact bundle has no record for pointer " + pointer.getKey());
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException(
          "invalid index artifact wrapper at " + pointer.getBlobUri(), e);
    }
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
      return Optional.of(ReusableArtifactBundlePayload.parseFrom(bytes));
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
