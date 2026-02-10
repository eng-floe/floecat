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
import ai.floedb.floecat.service.repo.model.CatalogKey;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository;
import ai.floedb.floecat.service.repo.util.PointerOverlay;
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

  @Inject
  public CatalogRepository(PointerStore pointerStore, BlobStore blobStore, PointerOverlay overlay) {
    this.repo =
        new GenericResourceRepository<>(
            pointerStore,
            blobStore,
            overlay,
            Schemas.CATALOG,
            Catalog::parseFrom,
            Catalog::toByteArray,
            "application/x-protobuf");
  }

  public CatalogRepository(PointerStore pointerStore, BlobStore blobStore) {
    this(pointerStore, blobStore, PointerOverlay.NOOP);
  }

  public void create(Catalog catalog) {
    repo.create(catalog);
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
