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

    reserveIndexOrIdempotent(byId, blob);
    reserveIndexOrIdempotent(byName, blob);
    putBlob(blob, cat);
  }

  public boolean update(Catalog updated, long expectedPointerVersion) {
    var rid = updated.getResourceId();
    var tid = rid.getTenantId();
    var byId = Keys.catPtr(tid, rid.getId());
    var blob = Keys.catBlob(tid, rid.getId());

    putBlob(blob, updated);
    advancePointer(byId, blob, expectedPointerVersion);
    return true;
  }

  public boolean rename(String tenant, ResourceId catId, String newName, long expectedVersion) {
    var byId = Keys.catPtr(tenant, catId.getId());
    var cur = get(byId).orElseThrow(
        () -> new NotFoundException("catalog not found: " + catId.getId()));

    if (newName.equals(cur.getDisplayName())) return true;

    var blob = Keys.catBlob(tenant, catId.getId());
    var newByName = Keys.catByNamePtr(tenant, newName);
    var oldByName = Keys.catByNamePtr(tenant, cur.getDisplayName());

    var updated = cur.toBuilder().setDisplayName(newName).build();
    putBlob(blob, updated);
    reserveIndexOrIdempotent(newByName, blob);
    advancePointer(byId, blob, expectedVersion);
    ptr.get(oldByName).ifPresent(p -> compareAndDeleteOrFalse(oldByName, p.getVersion()));
   return true;
  }

  public boolean delete(ResourceId catalogId) {
    var tid = catalogId.getTenantId();
    var byId = Keys.catPtr(tid, catalogId.getId());
    var blob = Keys.catBlob(tid, catalogId.getId());

    var pIdOpt = ptr.get(byId);
    if (pIdOpt.isEmpty()) {
      deleteQuietly(() -> blobs.delete(blob));
      return true;
    }
    var pId = pIdOpt.get();

    var catOpt = getById(catalogId);
    var byName = catOpt.map(
        c -> Keys.catByNamePtr(tid, c.getDisplayName())).orElse(null);

    if (byName != null) {
      ptr.get(byName).ifPresent(
          p -> compareAndDeleteOrFalse(byName, p.getVersion()));
    }

    if (!compareAndDeleteOrFalse(byId, pId.getVersion())) return false;

    deleteQuietly(() -> blobs.delete(blob));
    return true;
  }

  public boolean deleteWithPrecondition(ResourceId catalogId, long expectedVersion) {
    var tid = catalogId.getTenantId();
    var byId = Keys.catPtr(tid, catalogId.getId());
    var blob = Keys.catBlob(tid, catalogId.getId());

    var catOpt = getById(catalogId);
    var byName = catOpt.map(
        c -> Keys.catByNamePtr(tid, c.getDisplayName())).orElse(null);

    if (!compareAndDeleteOrFalse(byId, expectedVersion)) return false;
    if (byName != null) ptr.get(byName).ifPresent(
        p -> compareAndDeleteOrFalse(byName, p.getVersion()));
    deleteQuietly(() -> blobs.delete(blob));
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
