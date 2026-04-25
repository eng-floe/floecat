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
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.model.IndexArtifactKey;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class IndexArtifactRepository {
  private final GenericResourceRepository<IndexArtifactRecord, IndexArtifactKey> repo;

  @Inject
  public IndexArtifactRepository(PointerStore pointerStore, BlobStore blobStore) {
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
