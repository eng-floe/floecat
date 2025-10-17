package ai.floedb.metacat.service.repo.impl;

import java.util.List;
import java.util.Optional;

import com.google.protobuf.Timestamp;
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

  protected CatalogRepository() { super(); }

  @Inject
  public CatalogRepository(PointerStore ptr, BlobStore blobs) {
    super(ptr, blobs, Catalog::parseFrom, Catalog::toByteArray, "application/x-protobuf");
  }

  public Optional<Catalog> getById(ResourceId rid) {
    return get(Keys.catPtr(rid.getTenantId(), rid.getId()));
  }

  public Optional<Catalog> getByName(String tenantId, String displayName) {
    return get(Keys.catByNamePtr(tenantId, displayName));
  }

  public List<Catalog> listByName(String tenantId, int limit, String token, StringBuilder next) {
    return listByPrefix(Keys.catByNamePrefix(tenantId), limit, token, next);
  }

  public int countUnderCatalog(ResourceId catalogId) {
    var pfx = Keys.nsByPathPrefix(catalogId.getTenantId(), catalogId.getId(), List.of());
    return countByPrefix(pfx);
  }

  public int countAll(String tenantId) {
    return countByPrefix(Keys.catByNamePrefix(tenantId));
  }

  public void create(Catalog cat) {
    var rid = cat.getResourceId();
    var tid = rid.getTenantId();

    var byName = Keys.catByNamePtr(tid, cat.getDisplayName());
    var byId = Keys.catPtr(tid, rid.getId());
    var blob = Keys.catBlob(tid, rid.getId());

    putCas(byName, blob);
    try {
      putBlob(blob, cat);
      putCas(byId, blob);
    } catch (RuntimeException e) {
      deleteQuietly(() -> ptr.delete(byName));
      throw e;
    }
  }

  public boolean update(Catalog updated, long expectedPointerVersion) {
    var rid = updated.getResourceId();
    var tid = rid.getTenantId();
    var byId = Keys.catPtr(tid, rid.getId());
    var blob = Keys.catBlob(tid, rid.getId());
    return updateCanonical(byId, blob, updated, expectedPointerVersion);
  }

  public boolean rename(String tenant, ResourceId catId, String newDisplayName, long expectedVersion) {
    var byId = Keys.catPtr(tenant, catId.getId());
    var cur = get(byId).orElseThrow(() -> new IllegalStateException("catalog not found"));
    if (newDisplayName.equals(cur.getDisplayName())) {
      return true;
    }

    var blob = Keys.catBlob(tenant, catId.getId());
    var newByName = Keys.catByNamePtr(tenant, newDisplayName);
    var oldByName = Keys.catByNamePtr(tenant, cur.getDisplayName());

    reserveIndexOrIdempotent(newByName, blob);
    var updated = cur.toBuilder().setDisplayName(newDisplayName).build();
    if (!updateCanonical(byId, blob, updated, expectedVersion)) {
      return false;
    }
    deleteQuietly(() -> ptr.delete(oldByName));
    return true;
  }

  public boolean delete(ResourceId catalogId) {
    var catOpt = getById(catalogId);
    var tid = catalogId.getTenantId();
    var byId = Keys.catPtr(tid, catalogId.getId());
    var blob = Keys.catBlob(tid, catalogId.getId());
    final String byName = catOpt.map(cat -> {
      return Keys.catByNamePtr(tid, cat.getDisplayName());
    }).orElse(null);

    deleteQuietly(() -> ptr.delete(byId));
    deleteQuietly(() -> blobs.delete(blob));
    if (byName != null) deleteQuietly(() -> ptr.delete(byName));
    return ptr.get(byId).isEmpty() && blobs.head(blob).isEmpty();
  }

  public boolean deleteWithPrecondition(ResourceId catalogId, long expectedVersion) {
    var catOpt = getById(catalogId);
    var tid = catalogId.getTenantId();
    var byId = Keys.catPtr(tid, catalogId.getId());
    var blob = Keys.catBlob(tid, catalogId.getId());
    final String byName = catOpt.map(cat -> {
      return Keys.catByNamePtr(tid, cat.getDisplayName());
    }).orElse(null);

    if (!compareAndDeleteOrFalse(ptr, byId, expectedVersion)) return false;
    deleteQuietly(() -> blobs.delete(blob));
    if (byName != null) deleteQuietly(() -> ptr.delete(byName));
    return true;
  }

  public MutationMeta metaFor(ResourceId catalogId, Timestamp nowTs) {
    var t = catalogId.getTenantId();
    var key = Keys.catPtr(t, catalogId.getId());
    var p = ptr.get(key).orElseThrow(() -> new IllegalStateException(
        "Pointer missing for catalog: " + catalogId.getId()));
    return safeMetaOrDefault(key, p.getBlobUri(), nowTs);
  }

  public MutationMeta metaForSafe(ResourceId catalogId, Timestamp nowTs) {
    var t = catalogId.getTenantId();
    var key = Keys.catPtr(t, catalogId.getId());
    var blob = Keys.catBlob(t, catalogId.getId());
    return safeMetaOrDefault(key, blob, nowTs);
  }
}
