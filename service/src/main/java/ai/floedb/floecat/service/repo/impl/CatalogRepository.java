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
import ai.floedb.floecat.service.repo.cache.ImmutableBlobCache;
import ai.floedb.floecat.service.repo.model.CatalogKey;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.util.BatchGuard;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class CatalogRepository {

  private final GenericResourceRepository<Catalog, CatalogKey> repo;

  public CatalogRepository(PointerStore pointerStore, BlobStore blobStore) {
    this(pointerStore, blobStore, null);
  }

  @Inject
  public CatalogRepository(
      PointerStore pointerStore, BlobStore blobStore, ImmutableBlobCache blobCache) {
    this.repo =
        new GenericResourceRepository<>(
            pointerStore,
            blobStore,
            Schemas.CATALOG,
            Catalog::parseFrom,
            Catalog::toByteArray,
            "application/x-protobuf",
            blobCache);
  }

  public void create(Catalog catalog) {
    repo.create(catalog);
  }

  /**
   * Guarded create: the catalog becomes visible only while {@code guard} still holds. Used to
   * publish into an account atomically with respect to that account's deletion — see {@link
   * ai.floedb.floecat.service.repo.util.MarkerStore#accountLiveGuard}.
   */
  public void create(Catalog catalog, BatchGuard guard) {
    repo.create(catalog, guard);
  }

  public boolean update(Catalog catalog, long expectedPointerVersion) {
    return repo.update(catalog, expectedPointerVersion);
  }

  public boolean delete(ResourceId catalogResourceId) {
    return repo.delete(new CatalogKey(catalogResourceId.getAccountId(), catalogResourceId.getId()));
  }

  /** Guarded delete by id; see {@link TableRepository#delete(ResourceId, BatchGuard)}. */
  public boolean delete(ResourceId catalogResourceId, BatchGuard guard) {
    return repo.delete(
        new CatalogKey(catalogResourceId.getAccountId(), catalogResourceId.getId()), guard);
  }

  public boolean deleteWithPrecondition(ResourceId catalogResourceId, long expectedPointerVersion) {
    return repo.deleteWithPrecondition(
        new CatalogKey(catalogResourceId.getAccountId(), catalogResourceId.getId()),
        expectedPointerVersion);
  }

  public boolean deleteWithPrecondition(
      ResourceId catalogResourceId, long expectedPointerVersion, BatchGuard guard) {
    return repo.deleteWithPrecondition(
        new CatalogKey(catalogResourceId.getAccountId(), catalogResourceId.getId()),
        expectedPointerVersion,
        guard);
  }

  public Optional<Catalog> getById(ResourceId catalogResourceId) {
    return repo.getByKey(
        new CatalogKey(catalogResourceId.getAccountId(), catalogResourceId.getId()));
  }

  public Optional<Catalog> getByName(String accountId, String displayName) {
    return repo.get(Keys.catalogPointerByName(accountId, displayName));
  }

  public List<Catalog> list(String accountId, int limit, String pageToken, StringBuilder nextOut) {
    return repo.listByPrefix(Keys.catalogPointerByNamePrefix(accountId), limit, pageToken, nextOut);
  }

  public int count(String accountId) {
    return repo.countByPrefix(Keys.catalogPointerByNamePrefix(accountId));
  }

  /**
   * Deletes the account's leftover catalog index rows — its by-id and by-name families — and
   * reports how many.
   *
   * <p>Teardown only. Deleting a catalog whose blob cannot be read removes the canonical pointer
   * but not the by-name row, because the name lives in the blob — so an account sweep has to clear
   * the residue itself or leave an index row naming a catalog that is gone.
   *
   * <p>Scoped to those two families, NOT to {@link Keys#catalogRootPrefix}: a catalog's key space
   * also contains everything nested beneath it — namespace by-path rows, and the by-name rows of
   * every table, view and relation in them. Those are the only handles the recursive drop and a
   * retried DeleteAccount have for reaching those resources, so sweeping the whole root would, on
   * any teardown that had not finished, erase the index for resources that still exist and orphan
   * them permanently — the exact outcome this sweep is here to prevent.
   */
  public int deleteResidualRows(String accountId) {
    return repo.deleteByPrefix(Keys.catalogPointerByIdPrefix(accountId))
        + repo.deleteByPrefix(Keys.catalogPointerByNamePrefix(accountId));
  }

  /** Guarded account-teardown counterpart of {@link #deleteResidualRows(String)}. */
  public int deleteResidualRows(String accountId, BatchGuard accountGone) {
    return repo.deleteByPrefix(Keys.catalogPointerByIdPrefix(accountId), accountGone)
        + repo.deleteByPrefix(Keys.catalogPointerByNamePrefix(accountId), accountGone);
  }

  /**
   * The account's catalog ids, streamed a page at a time from canonical pointer rows.
   *
   * <p>Identity only, and deliberately so: {@link #list} parses every catalog blob, so one
   * unreadable blob fails the whole enumeration. Teardown cannot survive that — it runs after the
   * account pointer is gone, so the exception is not retryable and every attempt fails identically,
   * stranding every catalog behind the unreadable one. The by-id key carries the id, which is all a
   * recursive drop needs.
   */
  public void forEachId(String accountId, java.util.function.Consumer<ResourceId> action) {
    repo.forEachRefByPrefix(
        Keys.catalogPointerByIdPrefix(accountId),
        pointer ->
            action.accept(
                ResourceId.newBuilder()
                    .setAccountId(accountId)
                    .setId(Keys.extractLastSegment(pointer.getKey()))
                    .setKind(ResourceKind.RK_CATALOG)
                    .build()));
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
    List<Catalog> catalogs = repo.listByPrefix(prefix, Integer.MAX_VALUE, "", new StringBuilder());
    List<ResourceId> ids = new java.util.ArrayList<>(catalogs.size());
    for (Catalog c : catalogs) {
      ids.add(c.getResourceId());
    }
    return ids;
  }
}
