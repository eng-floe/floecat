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
import ai.floedb.floecat.service.repo.model.IndexArtifactKey;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.types.Hashing;
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

  public record PrewrittenIndexArtifactReference(
      String targetStorageId, String blobUri, long blobBytes, byte[] blobSha256) {}

  private record PrewrittenIndexWrite(String pointerKey, String blobUri, long blobBytes) {}

  private final GenericResourceRepository<IndexArtifactRecord, IndexArtifactKey> repo;
  private final PointerStore pointerStore;

  @Inject
  public IndexArtifactRepository(PointerStore pointerStore, BlobStore blobStore) {
    this.pointerStore = pointerStore;
    this.repo =
        new GenericResourceRepository<>(
            pointerStore,
            blobStore,
            Schemas.INDEX_ARTIFACT,
            IndexArtifactRecord::parseFrom,
            IndexArtifactRecord::toByteArray,
            "application/x-protobuf");
  }

  public void putIndexArtifact(IndexArtifactRecord value) {
    IndexArtifactKey key =
        indexArtifactLookupKey(value.getTableId(), value.getSnapshotId(), value.getTarget());
    for (int attempt = 0; attempt < 4; attempt++) {
      if (repo.createIfAbsent(value)) {
        return;
      }
      MutationMeta meta = repo.metaFor(key);
      if (repo.update(value, meta.getPointerVersion())) {
        return;
      }
    }
    throw new GenericResourceRepository.AbortRetryableException(
        "index artifact update conflicted repeatedly for target "
            + indexArtifactTargetStorageId(value.getTarget()));
  }

  public void putIndexArtifactsBatch(List<IndexArtifactRecord> values) {
    if (values == null || values.isEmpty()) {
      return;
    }
    for (IndexArtifactRecord value : values) {
      if (value == null) {
        continue;
      }
      putIndexArtifact(value);
    }
  }

  public void registerPrewrittenIndexArtifactReferences(
      ResourceId tableId, long snapshotId, List<PrewrittenIndexArtifactReference> references) {
    LinkedHashMap<String, PrewrittenIndexWrite> unique = new LinkedHashMap<>();
    for (PrewrittenIndexArtifactReference reference :
        references == null ? List.<PrewrittenIndexArtifactReference>of() : references) {
      if (reference == null
          || reference.targetStorageId() == null
          || reference.targetStorageId().isBlank()
          || reference.blobUri() == null
          || reference.blobUri().isBlank()
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
          Keys.snapshotIndexArtifactPointer(
              tableId.getAccountId(), tableId.getId(), snapshotId, reference.targetStorageId());
      unique.put(
          pointerKey,
          new PrewrittenIndexWrite(pointerKey, reference.blobUri(), reference.blobBytes()));
    }
    List<PrewrittenIndexWrite> writes = new ArrayList<>(unique.values());
    for (int from = 0; from < writes.size(); from += MAX_POINTER_BATCH_SIZE) {
      registerPrewrittenIndexArtifactChunk(
          writes.subList(from, Math.min(from + MAX_POINTER_BATCH_SIZE, writes.size())));
    }
  }

  private void registerPrewrittenIndexArtifactChunk(List<PrewrittenIndexWrite> writes) {
    List<PrewrittenIndexWrite> remaining = new ArrayList<>(writes);
    List<PointerStore.CasOp> initial = new ArrayList<>(remaining.size());
    for (PrewrittenIndexWrite write : remaining) {
      initial.add(prewrittenIndexUpsert(write, 0L));
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
        ops.add(prewrittenIndexUpsert(write, expectedVersion));
      }
      if (ops.isEmpty() || pointerStore.compareAndSetBatch(ops)) {
        return;
      }
      remaining = nextRemaining;
    }
    throw new GenericResourceRepository.AbortRetryableException(
        "index artifact reference update conflicted repeatedly for "
            + remaining.getFirst().pointerKey());
  }

  private PointerStore.CasUpsert prewrittenIndexUpsert(
      PrewrittenIndexWrite write, long expectedVersion) {
    return new PointerStore.CasUpsert(
        write.pointerKey(),
        expectedVersion,
        PointerReferences.blobPointer(
            write.pointerKey(), write.blobUri(), expectedVersion + 1L, write.blobBytes()));
  }

  public Optional<IndexArtifactRecord> getIndexArtifact(
      ResourceId tableId, long snapshotId, IndexTarget target) {
    return repo.getByKey(indexArtifactLookupKey(tableId, snapshotId, target));
  }

  public List<IndexArtifactRecord> listIndexArtifacts(
      ResourceId tableId, long snapshotId, int limit, String pageToken, StringBuilder nextOut) {
    return repo.listByPrefix(
        indexArtifactsPrefix(tableId, snapshotId), Math.max(1, limit), pageToken, nextOut);
  }

  public int countIndexArtifacts(ResourceId tableId, long snapshotId) {
    return repo.countByPrefix(indexArtifactsPrefix(tableId, snapshotId));
  }

  public MutationMeta metaForIndexArtifact(
      ResourceId tableId, long snapshotId, IndexTarget target, Timestamp nowTs) {
    return repo.metaFor(indexArtifactLookupKey(tableId, snapshotId, target), nowTs);
  }

  private static IndexArtifactKey indexArtifactLookupKey(
      ResourceId tableId, long snapshotId, IndexTarget target) {
    return new IndexArtifactKey(
        tableId.getAccountId(),
        tableId.getId(),
        snapshotId,
        indexArtifactTargetStorageId(target),
        "");
  }

  private static String indexArtifactsPrefix(ResourceId tableId, long snapshotId) {
    return Keys.snapshotIndexArtifactsPrefix(tableId.getAccountId(), tableId.getId(), snapshotId);
  }

  private static String indexArtifactTargetStorageId(IndexTarget target) {
    return switch (target.getTargetCase()) {
      case FILE -> "file:" + target.getFile().getFilePath();
      case TARGET_NOT_SET ->
          throw new IllegalArgumentException("target must be set on IndexArtifactRecord");
    };
  }
}
