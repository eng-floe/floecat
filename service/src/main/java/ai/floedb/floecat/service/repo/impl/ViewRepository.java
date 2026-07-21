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

import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.scanner.spi.TopologyGraph.RelationRef;
import ai.floedb.floecat.service.repo.cache.ImmutableBlobCache;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.model.ViewKey;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@ApplicationScoped
public class ViewRepository {

  private final GenericResourceRepository<View, ViewKey> repo;

  public ViewRepository(PointerStore pointerStore, BlobStore blobStore) {
    this(pointerStore, blobStore, null);
  }

  @Inject
  public ViewRepository(
      PointerStore pointerStore, BlobStore blobStore, ImmutableBlobCache blobCache) {
    this.repo =
        new GenericResourceRepository<>(
            pointerStore,
            blobStore,
            Schemas.VIEW,
            View::parseFrom,
            View::toByteArray,
            "application/x-protobuf",
            blobCache);
  }

  public void create(View view) {
    repo.create(view);
  }

  public boolean update(View view, long expectedPointerVersion) {
    return repo.update(view, expectedPointerVersion);
  }

  public boolean delete(ResourceId viewResourceId) {
    return repo.delete(new ViewKey(viewResourceId.getAccountId(), viewResourceId.getId()));
  }

  public boolean deleteWithPrecondition(ResourceId viewResourceId, long expectedPointerVersion) {
    return repo.deleteWithPrecondition(
        new ViewKey(viewResourceId.getAccountId(), viewResourceId.getId()), expectedPointerVersion);
  }

  public Optional<View> getById(ResourceId viewResourceId) {
    return repo.getByKey(new ViewKey(viewResourceId.getAccountId(), viewResourceId.getId()));
  }

  public Optional<View> getByName(
      String accountId, String catalogId, String namespaceId, String viewName) {
    return repo.get(Keys.viewPointerByName(accountId, catalogId, namespaceId, viewName));
  }

  public List<View> list(
      String accountId,
      String catalogId,
      String namespaceId,
      int limit,
      String pageToken,
      StringBuilder nextOut) {
    String prefix = Keys.viewPointerByNamePrefix(accountId, catalogId, namespaceId);
    return repo.listByPrefix(prefix, limit, pageToken, nextOut);
  }

  public int count(String accountId, String catalogId, String namespaceId) {
    return repo.countByPrefix(Keys.viewPointerByNamePrefix(accountId, catalogId, namespaceId));
  }

  /**
   * Scans the by-name pointer prefix for a namespace and returns lightweight refs without loading
   * blobs from S3. Falls back to key/blobUri parsing for legacy pointers.
   */
  public List<RelationRef> listRefs(String accountId, String catalogId, String namespaceId) {
    String prefix = Keys.viewPointerByNamePrefix(accountId, catalogId, namespaceId);
    var pointers = repo.listRefsByPrefix(prefix);
    var refs = new ArrayList<RelationRef>(pointers.size());
    for (var p : pointers) {
      TableRepository.toRelationRef(accountId, ResourceKind.RK_VIEW, p).ifPresent(refs::add);
    }
    return refs;
  }

  /** Reads exact by-name view pointers and returns refs without fetching blobs from S3. */
  public List<RelationRef> listRefsByName(
      String accountId, String catalogId, String namespaceId, Set<String> names) {
    if (names == null || names.isEmpty()) {
      return List.of();
    }
    List<RelationRef> refs = new ArrayList<>(names.size());
    for (String name : names) {
      if (name == null || name.isBlank()) {
        continue;
      }
      repo.refByPointer(Keys.viewPointerByName(accountId, catalogId, namespaceId, name))
          .flatMap(p -> TableRepository.toRelationRef(accountId, ResourceKind.RK_VIEW, p))
          .ifPresent(refs::add);
    }
    return refs;
  }

  public MutationMeta metaFor(ResourceId viewResourceId) {
    return repo.metaFor(new ViewKey(viewResourceId.getAccountId(), viewResourceId.getId()));
  }

  public MutationMeta metaFor(ResourceId viewResourceId, Timestamp nowTs) {
    return repo.metaFor(new ViewKey(viewResourceId.getAccountId(), viewResourceId.getId()), nowTs);
  }

  public MutationMeta metaForSafe(ResourceId viewResourceId) {
    return repo.metaForSafe(new ViewKey(viewResourceId.getAccountId(), viewResourceId.getId()));
  }

  /** Pointer-only meta (no blob HEAD, blank etag) for metadata-graph consumers. */
  public MutationMeta pointerMetaForSafe(ResourceId viewResourceId) {
    return repo.pointerMetaForSafe(
        new ViewKey(viewResourceId.getAccountId(), viewResourceId.getId()));
  }

  /** Blob-direct read for graph hydration from resolved metadata; empty if the blob moved. */
  public Optional<View> getByBlobUri(String blobUri) {
    return repo.getByBlobUri(blobUri);
  }

  /** Cache-bypassing read for liveness-bearing callers (see GenericResourceRepository). */
  public Optional<View> getByBlobUriLive(String blobUri) {
    return repo.getByBlobUriLive(blobUri);
  }
}
