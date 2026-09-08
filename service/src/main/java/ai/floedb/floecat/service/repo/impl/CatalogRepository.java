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

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.model.CatalogKey;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository;
import ai.floedb.floecat.service.repo.util.MarkerStore;
import ai.floedb.floecat.service.repo.util.MetadataRepositoryFactory;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

@ApplicationScoped
public class CatalogRepository {

  /** The catalog identity carried by a name pointer; resolving it needs no catalog blob. */
  public record CatalogRef(ResourceId id, String name) {}

  private final GenericResourceRepository<Catalog, CatalogKey> repo;

  public CatalogRepository(PointerStore pointerStore, BlobStore blobStore) {
    this(
        new GenericResourceRepository<>(
            pointerStore,
            blobStore,
            Schemas.CATALOG,
            Catalog::parseFrom,
            Catalog::toByteArray,
            "application/x-protobuf"));
  }

  @Inject
  public CatalogRepository(MetadataRepositoryFactory repositories) {
    this(
        repositories.create(
            Schemas.CATALOG, Catalog::parseFrom, Catalog::toByteArray, "application/x-protobuf"));
  }

  private CatalogRepository(GenericResourceRepository<Catalog, CatalogKey> repo) {
    this.repo = repo;
  }

  public void create(Catalog catalog) {
    repo.create(catalog);
  }

  public GenericResourceRepository.ResourceWithMeta<Catalog> createWithCompletion(
      Catalog catalog,
      Function<GenericResourceRepository.ResourceWithMeta<Catalog>, List<PointerStore.CasOp>>
          completionFactory) {
    return repo.createWithMeta(catalog, completionFactory);
  }

  public Optional<MutationMeta> completeWithMetaIfUnchanged(
      Catalog catalog,
      long expectedPointerVersion,
      Function<GenericResourceRepository.ResourceWithMeta<Catalog>, List<PointerStore.CasOp>>
          completionFactory) {
    return repo.completeWithMetaIfUnchanged(catalog, expectedPointerVersion, completionFactory);
  }

  public boolean update(Catalog catalog, long expectedPointerVersion) {
    return repo.update(catalog, expectedPointerVersion);
  }

  public boolean delete(ResourceId catalogResourceId) {
    return repo.delete(new CatalogKey(catalogResourceId.getAccountId(), catalogResourceId.getId()));
  }

  public boolean deleteWithPrecondition(ResourceId catalogResourceId, long expectedPointerVersion) {
    return repo.deleteWithPrecondition(
        new CatalogKey(catalogResourceId.getAccountId(), catalogResourceId.getId()),
        expectedPointerVersion);
  }

  /**
   * Deletes the catalog under child-set markers the caller sampled before its emptiness checks.
   *
   * <p>Takes them already split, so nothing here decodes a convention. They arrive as a value
   * because the caller samples them before the checks they guard.
   */
  public boolean deleteWhileChildSetsUnchanged(
      ResourceId catalogId, long expectedPointerVersion, MarkerStore.MarkerRemoval markers) {
    return repo.deleteWithPreconditionWhilePointersMatchAndDeletePointers(
        new CatalogKey(catalogId.getAccountId(), catalogId.getId()),
        expectedPointerVersion,
        markers.conditions(),
        markers.toDelete());
  }

  public Optional<Catalog> getById(ResourceId catalogResourceId) {
    return repo.getByKey(
        new CatalogKey(catalogResourceId.getAccountId(), catalogResourceId.getId()));
  }

  public Optional<Catalog> getByName(String accountId, String displayName) {
    return repo.get(Keys.catalogPointerByName(accountId, displayName));
  }

  /** Resolves a catalog name from pointer metadata without fetching its blob. */
  public Optional<CatalogRef> getRefByName(String accountId, String displayName) {
    return repo.refByPointer(Keys.catalogPointerByName(accountId, displayName))
        .flatMap(pointer -> toCatalogRef(accountId, pointer));
  }

  public List<Catalog> list(String accountId, int limit, String pageToken, StringBuilder nextOut) {
    return repo.listByPrefix(Keys.catalogPointerByNamePrefix(accountId), limit, pageToken, nextOut);
  }

  public List<Catalog> listConsistent(
      String accountId, int limit, String pageToken, StringBuilder nextOut) {
    return repo.listByPrefixForMutation(
        Keys.catalogPointerByNamePrefix(accountId), limit, pageToken, nextOut);
  }

  public int count(String accountId) {
    return repo.countByPrefix(Keys.catalogPointerByNamePrefix(accountId));
  }

  /** Body and metadata resolved from the same canonical pointer version. */
  public Optional<GenericResourceRepository.ResourceWithMeta<Catalog>> getByIdWithMeta(
      ResourceId catalogResourceId) {
    return repo.getByKeyWithMeta(
        new CatalogKey(catalogResourceId.getAccountId(), catalogResourceId.getId()));
  }

  public MutationMeta metaFor(ResourceId catalogResourceId) {
    return repo.metaFor(
        new CatalogKey(catalogResourceId.getAccountId(), catalogResourceId.getId()));
  }

  public MutationMeta metaFor(ResourceId catalogResourceId, Timestamp nowTs) {
    return repo.metaFor(
        new CatalogKey(catalogResourceId.getAccountId(), catalogResourceId.getId()), nowTs);
  }

  public MutationMeta metaForSafe(ResourceId catalogResourceId) {
    return repo.metaForSafe(
        new CatalogKey(catalogResourceId.getAccountId(), catalogResourceId.getId()));
  }

  /** Pointer-only meta (no blob HEAD, blank etag) for metadata-graph consumers. */
  public MutationMeta pointerMetaForSafe(ResourceId catalogResourceId) {
    return repo.pointerMetaForSafe(
        new CatalogKey(catalogResourceId.getAccountId(), catalogResourceId.getId()));
  }

  /** The same read, past the cache, for a caller whose verdict a stale pointer would invert. */
  public MutationMeta pointerMetaForSafeConsistent(ResourceId catalogResourceId) {
    return repo.pointerMetaForSafeConsistent(
        new CatalogKey(catalogResourceId.getAccountId(), catalogResourceId.getId()));
  }

  /** Blob-direct read for graph hydration from resolved metadata; empty if the blob moved. */
  public Optional<Catalog> getByBlobUri(String blobUri) {
    return repo.getByBlobUri(blobUri);
  }

  /** Cache-bypassing read for liveness-bearing callers (see GenericResourceRepository). */
  public Optional<Catalog> getByBlobUriLive(String blobUri) {
    return repo.getByBlobUriLive(blobUri);
  }

  public List<ResourceId> listIds(String accountId) {
    String prefix = Keys.catalogPointerByNamePrefix(accountId);
    return repo.listRefsByPrefix(prefix).stream()
        .map(pointer -> toCatalogRef(accountId, pointer))
        .flatMap(Optional::stream)
        .map(CatalogRef::id)
        .toList();
  }

  private static Optional<CatalogRef> toCatalogRef(
      String accountId, ai.floedb.floecat.common.rpc.Pointer pointer) {
    String name =
        pointer.getDisplayName().isEmpty()
            ? Keys.extractLastSegment(pointer.getKey())
            : pointer.getDisplayName();
    ResourceId id = pointer.getResourceId();
    if (id.getId().isEmpty()) {
      String rawId = Keys.extractResourceIdFromBlobUri(pointer.getBlobUri());
      if (rawId.isEmpty()) {
        return Optional.empty();
      }
      id =
          ResourceId.newBuilder()
              .setAccountId(accountId)
              .setId(rawId)
              .setKind(ResourceKind.RK_CATALOG)
              .build();
    }
    return Optional.of(new CatalogRef(id, name));
  }
}
