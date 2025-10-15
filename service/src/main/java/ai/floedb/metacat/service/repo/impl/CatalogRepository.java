package ai.floedb.metacat.service.repo.impl;

import java.time.Clock;
import java.util.List;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import ai.floedb.metacat.catalog.rpc.Catalog;
import ai.floedb.metacat.catalog.rpc.MutationMeta;
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
    var tid = catalogId.getTenantId();
    var key = Keys.catPtr(tid, catalogId.getId());
    var uri = Keys.catBlob(tid, catalogId.getId());

    put(key, uri, catalog);

    nameIndex.upsertCatalog(tid, catalogId, catalog.getDisplayName());
  }

  public boolean update(Catalog catalog, long expectedPointerVersion) {
    var catalogId = catalog.getResourceId();
    var tid = catalogId.getTenantId();
    var key = Keys.catPtr(tid, catalogId.getId());
    var uri = Keys.catBlob(tid, catalogId.getId());

    boolean ok = update(key, uri, catalog, expectedPointerVersion);
    if (ok) {
      nameIndex.upsertCatalog(tid, catalogId, catalog.getDisplayName());
    }
    return ok;
  }

  public boolean delete(ResourceId catalogId) {
    var tenant = catalogId.getTenantId();
    var key = Keys.catPtr(tenant, catalogId.getId());
    var uri = Keys.catBlob(tenant, catalogId.getId());

    try { nameIndex.removeCatalog(tenant, catalogId); } catch (Throwable ignore) {}
    try { ptr.delete(key); } catch (Throwable ignore) {}
    try { blobs.delete(uri); } catch (Throwable ignore) {}

    boolean gonePtr  = ptr.get(key).isEmpty();
    boolean goneBlob = blobs.head(uri).isEmpty();
    return gonePtr && goneBlob;
  }

  public boolean deleteWithPrecondition(ResourceId catalogId, long expectedVersion) {
    var tid = catalogId.getTenantId();
    var key = Keys.catPtr(tid, catalogId.getId());
    var uri = Keys.catBlob(tid, catalogId.getId());

    boolean removed = ptr.compareAndDelete(key, expectedVersion);
    if (!removed) {
      return false;
    }

    blobs.delete(uri);
    nameIndex.removeCatalog(tid, catalogId);

    return true;
  }

  public MutationMeta metaFor(ResourceId catalogId) {
    String tenant = catalogId.getTenantId();
    String key = Keys.catPtr(tenant, catalogId.getId());
    String blob = Keys.catBlob(tenant, catalogId.getId());
    ptr.get(key).orElseThrow(() -> new IllegalStateException(
        "Pointer missing for catalog: " + catalogId.getId()));
    return safeMetaOrDefault(key, blob, clock);
  }

  public MutationMeta metaForSafe(ResourceId catalogId) {
    String tenant = catalogId.getTenantId();
    String key = Keys.catPtr(tenant, catalogId.getId());
    String blob = Keys.catBlob(tenant, catalogId.getId());
    return safeMetaOrDefault(key, blob, clock);
  }
}
