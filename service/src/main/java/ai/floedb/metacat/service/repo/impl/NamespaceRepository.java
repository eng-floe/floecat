package ai.floedb.metacat.service.repo.impl;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import ai.floedb.metacat.catalog.rpc.MutationMeta;
import ai.floedb.metacat.catalog.rpc.Namespace;
import ai.floedb.metacat.common.rpc.Pointer;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.repo.util.BaseRepository;
import ai.floedb.metacat.service.repo.util.Keys;
import ai.floedb.metacat.service.storage.BlobStore;
import ai.floedb.metacat.service.storage.PointerStore;

@ApplicationScoped
public class NamespaceRepository extends BaseRepository<Namespace> {

  protected NamespaceRepository() { super(); }

  @Inject
  public NamespaceRepository(PointerStore ptr, BlobStore blobs) {
    super(ptr, blobs, Namespace::parseFrom, Namespace::toByteArray, "application/x-protobuf");
  }

  public Optional<Namespace> get(ResourceId catalogId, ResourceId nsId) {
    return get(Keys.nsPtr(nsId.getTenantId(), catalogId.getId(), nsId.getId()));
  }

  public List<Namespace> list(ResourceId catalogId, List<String> pathPrefix, int limit, String token, StringBuilder next) {
    var pfx = Keys.nsByPathPrefix(catalogId.getTenantId(), catalogId.getId(), pathPrefix == null ? List.of() : pathPrefix);
    return listByPrefix(pfx, limit, token, next);
  }

  public int countUnderCatalog(ResourceId catalogId) {
    var pfx = Keys.nsByPathPrefix(catalogId.getTenantId(), catalogId.getId(), List.of());
    return countByPrefix(pfx);
  }

  public Optional<ResourceId> findOwnerCatalog(String tenantId, String nsId) {
    final String root = "/tenants/" + tenantId.toLowerCase() + "/catalogs/";
    final String suffix = "/namespaces/by-id/" + URLEncoder.encode(nsId, StandardCharsets.UTF_8);
    String token = ""; var next = new StringBuilder();
    do {
      var rows = ptr.listPointersByPrefix(root, 200, token, next);
      for (var r : rows) {
        if (r.key().endsWith(suffix)) {
          var parts = r.key().split("/");
          String catId = parts.length > 5 ? URLDecoder.decode(parts[4], StandardCharsets.UTF_8) : "";
          if (!catId.isBlank()) {
            return Optional.of(ResourceId.newBuilder()
                .setTenantId(tenantId).setId(catId).setKind(ResourceKind.RK_CATALOG).build());
          }
        }
      }
      token = next.toString(); next.setLength(0);
    } while (!token.isEmpty());
    return Optional.empty();
  }

  public Optional<ResourceId> getByPath(String tenantId, ResourceId catalogId, List<String> path) {
    var key = Keys.nsByPathPtr(tenantId, catalogId.getId(), path);
    var p = ptr.get(key);
    if (p.isEmpty()) return Optional.empty();
    try {
      var ns = Namespace.parseFrom(blobs.get(p.get().getBlobUri()));
      return Optional.of(ns.getResourceId());
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  public void create(Namespace ns, ResourceId catalogId) {
    requireCatalogId(catalogId);
    var nsRid = ns.getResourceId();
    var tid = nsRid.getTenantId();

    var byId = Keys.nsPtr(tid, catalogId.getId(), nsRid.getId());
    var blob = Keys.nsBlob(tid, catalogId.getId(), nsRid.getId());

    var full = new ArrayList<>(ns.getParentsList());
    if (!ns.getDisplayName().isBlank()) full.add(ns.getDisplayName());
    var byPath = Keys.nsByPathPtr(tid, catalogId.getId(), full);

    var pathPtr = Pointer.newBuilder().setKey(byPath).setBlobUri(blob).setVersion(1L).build();
    if (!ptr.compareAndSet(byPath, 0L, pathPtr)) {
      var ex = ptr.get(byPath).orElse(null);
      if (ex == null || !blob.equals(ex.getBlobUri())) {
        throw new IllegalStateException("namespace already exists at path: " + String.join("/", full));
      }
    }

    put(byId, blob, ns);
  }

  public boolean update(Namespace updated, ResourceId catalogId, long expectedVersion) {
    requireCatalogId(catalogId);
    var tid = updated.getResourceId().getTenantId();
    var byId = Keys.nsPtr(tid, catalogId.getId(), updated.getResourceId().getId());
    var blob = Keys.nsBlob(tid, catalogId.getId(), updated.getResourceId().getId());
    return update(byId, blob, updated, expectedVersion);
  }

  public boolean renameOrMove(
      Namespace updated,
      ResourceId oldCatalogId,
      List<String> oldParents,
      String oldLeaf,
      ResourceId newCatalogId,
      List<String> newParents,
      long expectedVersion) {

    requireCatalogId(oldCatalogId);
    requireCatalogId(newCatalogId);

    var tid = updated.getResourceId().getTenantId();
    var nsId = updated.getResourceId().getId();

    var oldById = Keys.nsPtr(tid, oldCatalogId.getId(), nsId);
    var newById = Keys.nsPtr(tid, newCatalogId.getId(), nsId);

    var oldPath = new ArrayList<>(oldParents);
    oldPath.add(oldLeaf);
    var newPath = new ArrayList<>(newParents);
    newPath.add(updated.getDisplayName());

    var oldByPath = Keys.nsByPathPtr(tid, oldCatalogId.getId(), oldPath);
    var newByPath = Keys.nsByPathPtr(tid, newCatalogId.getId(), newPath);

    var oldBlob = Keys.nsBlob(tid, oldCatalogId.getId(), nsId);
    var newBlob = Keys.nsBlob(tid, newCatalogId.getId(), nsId);

    var reserve = Pointer.newBuilder().setKey(newByPath).setBlobUri(newBlob).setVersion(1L).build();
    if (!ptr.compareAndSet(newByPath, 0L, reserve)) {
      var ex = ptr.get(newByPath).orElse(null);
      if (ex == null || !newBlob.equals(ex.getBlobUri())) return false;
    }

    var bytes = toBytes.apply(updated);
    var etag = sha256B64(bytes);
    var hdr = blobs.head(newBlob);
    if (hdr.isEmpty() || !etag.equals(hdr.get().getEtag())) blobs.put(newBlob, bytes, contentType);

    if (oldCatalogId.getId().equals(newCatalogId.getId())) {
      if (!update(oldById, newBlob, updated, expectedVersion)) return false;
    } else {
      var newPtr = Pointer.newBuilder().setKey(newById).setBlobUri(newBlob).setVersion(1L).build();
      if (!ptr.compareAndSet(newById, 0L, newPtr)) {
        var ex = ptr.get(newById).orElse(null);
        if (ex == null || !newBlob.equals(ex.getBlobUri())) return false;
      }
      if (!ptr.compareAndDelete(oldById, expectedVersion)) return false;
      try { blobs.delete(oldBlob); } catch (Throwable ignore) {}
    }

    try { ptr.delete(oldByPath); } catch (Throwable ignore) {}
    return true;
  }

  public boolean deleteWithPrecondition(ResourceId catalogId, ResourceId namespaceId, long expectedVersion) {
    var nsOpt = get(catalogId, namespaceId);
    var tid = namespaceId.getTenantId();
    var byId = Keys.nsPtr(tid, catalogId.getId(), namespaceId.getId());
    var blob = Keys.nsBlob(tid, catalogId.getId(), namespaceId.getId());
    String byPath = null;

    if (nsOpt.isPresent()) {
      var ns = nsOpt.get();
      var full = new ArrayList<>(ns.getParentsList());
      full.add(ns.getDisplayName());
      byPath = Keys.nsByPathPtr(tid, catalogId.getId(), full);
    }
    
    if (!ptr.compareAndDelete(byId, expectedVersion)) return false;
    try { blobs.delete(blob); } catch (Throwable ignore) {}

    if (byPath != null) {
      try { ptr.delete(byPath); } catch (Throwable ignore) {}
    }
    return true;
  }

  public MutationMeta metaFor(ResourceId catalogId, ResourceId namespaceId) {
    var t = namespaceId.getTenantId();
    var key = Keys.nsPtr(t, catalogId.getId(), namespaceId.getId());
    var p = ptr.get(key).orElseThrow(() -> new IllegalStateException("Pointer missing for namespace: " + namespaceId.getId()));
    return safeMetaOrDefault(key, p.getBlobUri(), clock);
  }

  public MutationMeta metaForSafe(ResourceId catalogId, ResourceId namespaceId) {
    var t = namespaceId.getTenantId();
    var key = Keys.nsPtr(t, catalogId.getId(), namespaceId.getId());
    var blob = Keys.nsBlob(t, catalogId.getId(), namespaceId.getId());
    return safeMetaOrDefault(key, blob, clock);
  }

  private static void requireCatalogId(ResourceId catalogId) {
    if (catalogId == null || catalogId.getId() == null || catalogId.getId().isBlank())
      throw new IllegalArgumentException("namespace requires non-empty catalog_id");
  }
}
