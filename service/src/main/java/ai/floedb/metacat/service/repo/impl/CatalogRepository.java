package ai.floedb.metacat.service.repo.impl;

import ai.floedb.metacat.catalog.rpc.Catalog;
import ai.floedb.metacat.common.rpc.MutationMeta;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.service.repo.model.CatalogKey;
import ai.floedb.metacat.service.repo.model.Keys;
import ai.floedb.metacat.service.repo.model.Schemas;
import ai.floedb.metacat.service.repo.util.GenericRepository;
import ai.floedb.metacat.service.storage.BlobStore;
import ai.floedb.metacat.service.storage.PointerStore;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class CatalogRepository {

  private final GenericRepository<Catalog, CatalogKey> repo;

  @Inject
  public CatalogRepository(PointerStore pointerStore, BlobStore blobStore) {
    this.repo =
        new GenericRepository<>(
            pointerStore,
            blobStore,
            Schemas.CATALOG,
            Catalog::parseFrom,
            Catalog::toByteArray,
            "application/x-protobuf");
  }

  public void create(Catalog catalog) {
    repo.create(catalog);
  }

  public boolean update(Catalog catalog, long expectedPointerVersion) {
    return repo.update(catalog, expectedPointerVersion);
  }

  public boolean delete(ResourceId catalogResourceId) {
    return repo.delete(new CatalogKey(catalogResourceId.getTenantId(), catalogResourceId.getId()));
  }

  public boolean deleteWithPrecondition(ResourceId catalogResourceId, long expectedPointerVersion) {
    return repo.deleteWithPrecondition(
        new CatalogKey(catalogResourceId.getTenantId(), catalogResourceId.getId()),
        expectedPointerVersion);
  }

  public Optional<Catalog> getById(ResourceId catalogResourceId) {
    return repo.getByKey(
        new CatalogKey(catalogResourceId.getTenantId(), catalogResourceId.getId()));
  }

  public Optional<Catalog> getByName(String tenantId, String displayName) {
    return repo.get(Keys.catalogPointerByName(tenantId, displayName));
  }

  public List<Catalog> list(String tenantId, int limit, String pageToken, StringBuilder nextOut) {
    return repo.listByPrefix(Keys.catalogPointerByNamePrefix(tenantId), limit, pageToken, nextOut);
  }

  public int count(String tenantId) {
    return repo.countByPrefix(Keys.catalogPointerByNamePrefix(tenantId));
  }

  public MutationMeta metaFor(ResourceId catalogResourceId) {
    return repo.metaFor(new CatalogKey(catalogResourceId.getTenantId(), catalogResourceId.getId()));
  }

  public MutationMeta metaFor(ResourceId catalogResourceId, Timestamp nowTs) {
    return repo.metaFor(
        new CatalogKey(catalogResourceId.getTenantId(), catalogResourceId.getId()), nowTs);
  }

  public MutationMeta metaForSafe(ResourceId catalogResourceId) {
    return repo.metaForSafe(
        new CatalogKey(catalogResourceId.getTenantId(), catalogResourceId.getId()));
  }
}
