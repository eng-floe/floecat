package ai.floedb.metacat.service.repo.impl;

import java.util.List;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import ai.floedb.metacat.catalog.rpc.Catalog;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.service.repo.util.BaseRepository;
import ai.floedb.metacat.service.repo.util.Keys;
import ai.floedb.metacat.service.storage.BlobStore;
import ai.floedb.metacat.service.storage.PointerStore;

@ApplicationScoped
public class CatalogRepository extends BaseRepository<Catalog> {
  private NameIndexRepository nameIndex;

  protected CatalogRepository() { super(); }

  @Inject
  public CatalogRepository(NameIndexRepository nameIndex, PointerStore ptr, BlobStore blobs) {
    super(ptr, blobs, Catalog::parseFrom, Catalog::toByteArray, "application/x-protobuf");
    this.nameIndex = nameIndex;
  }

  public Optional<Catalog> get(ResourceId rid) {
    return get(Keys.catPtr(rid.getTenantId(), rid.getId()));
  }

  public List<Catalog> list(String tenantId, int limit, String token, StringBuilder next) {
    return listByPrefix(Keys.catPtr(tenantId, ""), limit, token, next);
  }

  public int count(String tenantId) {
    return countByPrefix(Keys.catPtr(tenantId, ""));
  }

  public void put(Catalog catalog) {
    var catalogId = catalog.getResourceId();

    delete(catalogId);
    put(Keys.catPtr(catalogId.getTenantId(), catalogId.getId()),
        Keys.catBlob(catalogId.getTenantId(), catalogId.getId()),
        catalog);

    nameIndex.upsertCatalog(catalogId.getTenantId(), catalogId, catalog.getDisplayName());
  }

  public boolean delete(ResourceId catalogId) {
    var tenantId = catalogId.getTenantId();
    String ptrKey = Keys.catPtr(tenantId, catalogId.getId());
    String blobUri = Keys.catBlob(tenantId, catalogId.getId());

    nameIndex.removeCatalog(tenantId, catalogId);

    boolean okPtr = ptr.delete(ptrKey);
    boolean okBlob = blobs.delete(blobUri);
    return okPtr && okBlob;
  }
}
