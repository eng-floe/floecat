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
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.types.Hashing;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class IndexArtifactRepository {
  private static final int MAX_POINTER_BATCH_SIZE = 100;
  private static final String DIRECT_GENERATION = "direct";

  public record PrewrittenIndexArtifactReference(
      String targetStorageId, String blobUri, long blobBytes, byte[] blobSha256) {}

  private record PrewrittenIndexWrite(String pointerKey, String blobUri, long blobBytes) {}

  private final PointerStore pointerStore;
  private final BlobStore blobStore;

  @Inject
  public IndexArtifactRepository(PointerStore pointerStore, BlobStore blobStore) {
    this.pointerStore = pointerStore;
    this.blobStore = blobStore;
  }

  public void putIndexArtifact(IndexArtifactRecord value) {
    requireValidRecord(value);
    ResourceId tableId = value.getTableId();
    String targetStorageId = indexArtifactTargetStorageId(value.getTarget());
    byte[] bytes = value.toByteArray();
    String blobUri =
        Keys.snapshotIndexArtifactBlobUri(
            tableId.getAccountId(), tableId.getId(), targetStorageId, Hashing.sha256Hex(bytes));
    blobStore.put(blobUri, bytes, "application/x-protobuf");
    for (int attempt = 0; attempt < 4; attempt++) {
      Optional<String> before = activeGeneration(tableId, value.getSnapshotId());
      String generationId = before.orElse(DIRECT_GENERATION);
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
      List<PrewrittenIndexArtifactReference> references) {
    String requiredPrefix =
        Keys.snapshotTargetStatsGenerationBlobPrefix(
            tableId.getAccountId(), tableId.getId(), snapshotId, generationId);
    LinkedHashMap<String, PrewrittenIndexWrite> unique = new LinkedHashMap<>();
    for (PrewrittenIndexArtifactReference reference :
        references == null ? List.<PrewrittenIndexArtifactReference>of() : references) {
      if (reference == null
          || reference.targetStorageId() == null
          || reference.targetStorageId().isBlank()
          || reference.blobUri() == null
          || !reference.blobUri().startsWith(requiredPrefix)
          || reference.blobBytes() <= 0L
          || reference.blobSha256() == null
          || reference.blobSha256().length != 32
          || !reference
              .blobUri()
              .endsWith(
                  "/"
                      + Hashing.sha256Hex(reference.targetStorageId())
                      + "/"
                      + HexFormat.of().formatHex(reference.blobSha256())
                      + ".pb")) {
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

  public void activateGeneration(ResourceId tableId, long snapshotId, String generationId) {
    if (generationId == null || generationId.isBlank()) {
      throw new IllegalArgumentException("generationId is required");
    }
    String pointerKey =
        Keys.snapshotIndexArtifactActiveGenerationPointer(
            tableId.getAccountId(), tableId.getId(), snapshotId);
    for (int attempt = 0; attempt < 8; attempt++) {
      Pointer current = pointerStore.get(pointerKey).orElse(null);
      if (current != null && generationId.equals(current.getBlobUri())) {
        return;
      }
      long expectedVersion = current == null ? 0L : current.getVersion();
      Pointer next =
          PointerReferences.opaqueMarkerPointer(pointerKey, generationId, expectedVersion + 1L);
      if (pointerStore.compareAndSet(pointerKey, expectedVersion, next)) {
        return;
      }
    }
    throw new BaseResourceRepository.AbortRetryableException(
        "index artifact generation activation conflicted repeatedly for snapshot " + snapshotId);
  }

  private boolean activateDirectGenerationIfAbsent(ResourceId tableId, long snapshotId) {
    String pointerKey =
        Keys.snapshotIndexArtifactActiveGenerationPointer(
            tableId.getAccountId(), tableId.getId(), snapshotId);
    return pointerStore.compareAndSet(
        pointerKey, 0L, PointerReferences.opaqueMarkerPointer(pointerKey, DIRECT_GENERATION, 1L));
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
    List<Pointer> pointers =
        pointerStore.listPointersByPrefix(
            Keys.snapshotIndexArtifactGenerationPrefix(
                tableId.getAccountId(), tableId.getId(), snapshotId, generationId.get()),
            Math.max(1, limit),
            pageToken == null ? "" : pageToken,
            nextOut);
    return pointers.stream().map(this::readRecord).toList();
  }

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
      return IndexArtifactRecord.parseFrom(blobStore.get(pointer.getBlobUri()));
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException(
          "invalid index artifact wrapper at " + pointer.getBlobUri(), e);
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
