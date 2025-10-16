package ai.floedb.metacat.service.repo.impl;

import java.util.List;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import ai.floedb.metacat.catalog.rpc.Catalog;
import ai.floedb.metacat.catalog.rpc.MutationMeta;
import ai.floedb.metacat.common.rpc.Pointer;
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

    var namePtr = Pointer.newBuilder().setKey(byName).setBlobUri(blob).setVersion(1L).build();
    if (!ptr.compareAndSet(byName, 0L, namePtr)) {
      var existing = ptr.get(byName).orElse(null);
      if (existing == null || !blob.equals(existing.getBlobUri()))
        throw new IllegalStateException("catalog name already exists");
    }

    var bytes = toBytes.apply(cat);
    var hdr = blobs.head(blob);
    var etag = sha256B64(bytes);
    if (hdr.isEmpty() || !etag.equals(hdr.get().getEtag())) {
      blobs.put(blob, bytes, contentType);
    }

    var canPtr = Pointer.newBuilder().setKey(byId).setBlobUri(blob).setVersion(1L).build();
    if (!ptr.compareAndSet(byId, 0L, canPtr)) {
      var ex = ptr.get(byId).orElse(null);
      if (ex == null || !blob.equals(ex.getBlobUri())) {
        try { ptr.delete(byName); } catch (Throwable ignore) {}
        throw new IllegalStateException("catalog id collision on create");
      }
    }
  }

  public boolean update(Catalog updated, long expectedPointerVersion) {
    var rid = updated.getResourceId();
    var tid = rid.getTenantId();
    var byId = Keys.catPtr(tid, rid.getId());
    var blob = Keys.catBlob(tid, rid.getId());
    return update(byId, blob, updated, expectedPointerVersion);
  }

  public boolean rename(String tenant, ResourceId catId, String newDisplayName, long expectedVersion) {
    var byId = Keys.catPtr(tenant, catId.getId());
    var cur = get(byId).orElseThrow(() -> new IllegalStateException("catalog not found"));
    if (newDisplayName.equals(cur.getDisplayName())) return true;

    var blob = Keys.catBlob(tenant, catId.getId());
    var newByName = Keys.catByNamePtr(tenant, newDisplayName);
    var oldByName = Keys.catByNamePtr(tenant, cur.getDisplayName());

    var reserve = Pointer.newBuilder().setKey(newByName).setBlobUri(blob).setVersion(1L).build();
    if (!ptr.compareAndSet(newByName, 0L, reserve)) {
      var ex = ptr.get(newByName).orElse(null);
      if (ex == null || !blob.equals(ex.getBlobUri())) return false;
    }

    var updated = cur.toBuilder().setDisplayName(newDisplayName).build();
    if (!update(byId, blob, updated, expectedVersion)) return false;

    try { ptr.delete(oldByName); } catch (Throwable ignore) {}
    return true;
  }

  public boolean delete(ResourceId catalogId) {
    var catOpt = getById(catalogId);
    var tid = catalogId.getTenantId();
    var byId = Keys.catPtr(tid, catalogId.getId());
    var blob = Keys.catBlob(tid, catalogId.getId());
    String byName = null;

    if (catOpt.isPresent()) {
      var cat = catOpt.get();
      byName = Keys.catByNamePtr(tid, cat.getDisplayName());
    }

    try { ptr.delete(byId); } catch (Throwable ignore) {}
    try { blobs.delete(blob); } catch (Throwable ignore) {}

    if (byName != null) {
      try { ptr.delete(byName); } catch (Throwable ignore) {}
    }
    return ptr.get(byId).isEmpty() && blobs.head(blob).isEmpty();
  }

  public boolean deleteWithPrecondition(ResourceId catalogId, long expectedVersion) {
    var catOpt = getById(catalogId);
    var tid = catalogId.getTenantId();
    var byId = Keys.catPtr(tid, catalogId.getId());
    var blob = Keys.catBlob(tid, catalogId.getId());
    String byName = null;

    if (catOpt.isPresent()) {
      var cat = catOpt.get();
      byName = Keys.catByNamePtr(tid, cat.getDisplayName());
    }

    if (!ptr.compareAndDelete(byId, expectedVersion)) return false;
    try { blobs.delete(blob); } catch (Throwable ignore) {}

    if (byName != null) {
      try { ptr.delete(byName); } catch (Throwable ignore) {}
    }
    return true;
  }

  public MutationMeta metaFor(ResourceId catalogId) {
    var t = catalogId.getTenantId();
    var key = Keys.catPtr(t, catalogId.getId());
    var p = ptr.get(key).orElseThrow(
        () -> new IllegalStateException("Pointer missing for catalog: " + catalogId.getId()));
    return safeMetaOrDefault(key, p.getBlobUri(), clock);
  }

  public MutationMeta metaForSafe(ResourceId catalogId) {
    var t = catalogId.getTenantId();
    var key = Keys.catPtr(t, catalogId.getId());
    var blob = Keys.catBlob(t, catalogId.getId());
    return safeMetaOrDefault(key, blob, clock);
  }
}
