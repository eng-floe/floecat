package ai.floedb.metacat.service.repo.impl;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import ai.floedb.metacat.catalog.rpc.MutationMeta;
import ai.floedb.metacat.catalog.rpc.Namespace;
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
    var pfx = Keys.nsByPathPrefix(
        catalogId.getTenantId(), catalogId.getId(), pathPrefix == null ? List.of() : pathPrefix);
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
    return get(key).map(Namespace::getResourceId);
  }

  public void create(Namespace ns, ResourceId catalogId) {
    requireCatalogId(catalogId);
    var nsRid = ns.getResourceId();
    var tid = nsRid.getTenantId();

    var byId = Keys.nsPtr(tid, catalogId.getId(), nsRid.getId());
    var blob = Keys.nsBlob(tid, nsRid.getId());
    var full = new ArrayList<>(ns.getParentsList());
    if (!ns.getDisplayName().isBlank()) {
      full.add(ns.getDisplayName());
    }
    var byPath = Keys.nsByPathPtr(tid, catalogId.getId(), full);

    reserveIndexOrIdempotent(byId, blob);
    reserveIndexOrIdempotent(byPath, blob);
    putBlob(blob, ns);
  }

  public boolean update(Namespace updated, ResourceId catalogId, long expectedVersion) {
    requireCatalogId(catalogId);
    var tid = updated.getResourceId().getTenantId();
    var byId = Keys.nsPtr(tid, catalogId.getId(), updated.getResourceId().getId());
    var blob = Keys.nsBlob(tid, updated.getResourceId().getId());

    putBlob(blob, updated);
    advancePointer(byId, blob, expectedVersion);
    return true;
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

    var sameCatalog = oldCatalogId.getId().equals(newCatalogId.getId());

    var oldById = Keys.nsPtr(tid, oldCatalogId.getId(), nsId);
    var newById = Keys.nsPtr(tid, newCatalogId.getId(), nsId);

    var oldPath = new ArrayList<>(oldParents); oldPath.add(oldLeaf);
    var newPath = new ArrayList<>(newParents); newPath.add(updated.getDisplayName());

    var oldByPath = Keys.nsByPathPtr(tid, oldCatalogId.getId(), oldPath);
    var newByPath = Keys.nsByPathPtr(tid, newCatalogId.getId(), newPath);

    var blob = Keys.nsBlob(tid, nsId);

    putBlob(blob, updated);
    reserveIndexOrIdempotent(newByPath, blob);
    if (sameCatalog) {
      advancePointer(oldById, blob, expectedVersion);
    } else {
      reserveIndexOrIdempotent(newById, blob);
      if (!compareAndDeleteOrFalse(oldById, expectedVersion)) {
        ptr.get(newById).ifPresent(p -> compareAndDeleteOrFalse(newById, p.getVersion()));
        ptr.get(newByPath).ifPresent(p -> compareAndDeleteOrFalse(newByPath, p.getVersion()));
        return false;
      }
    }

    ptr.get(oldByPath).ifPresent(p -> compareAndDeleteOrFalse(oldByPath, p.getVersion()));
    return true;
  }

  public boolean delete(ResourceId catalogId, ResourceId namespaceId) {
    var tid = namespaceId.getTenantId();
    var byId = Keys.nsPtr(tid, catalogId.getId(), namespaceId.getId());
    var blob = Keys.nsBlob(tid, namespaceId.getId());

    var nsOpt = get(catalogId, namespaceId);
    var byPath = nsOpt.map(ns -> {
      var full = new ArrayList<>(ns.getParentsList());
      full.add(ns.getDisplayName());
      return Keys.nsByPathPtr(tid, catalogId.getId(), full);
    }).orElse(null);

    if (byPath != null) {
      ptr.get(byPath).ifPresent(p -> compareAndDeleteOrFalse(byPath, p.getVersion()));
    }
    ptr.get(byId).ifPresent(p -> compareAndDeleteOrFalse(byId, p.getVersion()));
    deleteQuietly(() -> blobs.delete(blob));
    return true;
  }

  public boolean deleteWithPrecondition(ResourceId catalogId, ResourceId namespaceId, long expectedVersion) {
    var tid = namespaceId.getTenantId();
    var byId = Keys.nsPtr(tid, catalogId.getId(), namespaceId.getId());
    var blob = Keys.nsBlob(tid, namespaceId.getId());

    var nsOpt = get(catalogId, namespaceId);
    var byPath = nsOpt.map(ns -> {
      var full = new ArrayList<>(ns.getParentsList());
      full.add(ns.getDisplayName());
      return Keys.nsByPathPtr(tid, catalogId.getId(), full);
    }).orElse(null);

    if (!compareAndDeleteOrFalse(byId, expectedVersion)) return false;
    if (byPath != null) ptr.get(byPath).ifPresent(
        p -> compareAndDeleteOrFalse(byPath, p.getVersion()));
    deleteQuietly(() -> blobs.delete(blob));
    return true;
  }

  public MutationMeta metaFor(ResourceId catalogId, ResourceId namespaceId, Timestamp nowTs) {
    var t = namespaceId.getTenantId();
    var key = Keys.nsPtr(t, catalogId.getId(), namespaceId.getId());
    var p = ptr.get(key).orElseThrow(() -> new IllegalStateException(
        "Pointer missing for namespace: " + namespaceId.getId()));
    return safeMetaOrDefault(key, p.getBlobUri(), nowTs);
  }

  public MutationMeta metaForSafe(ResourceId catalogId, ResourceId namespaceId, Timestamp nowTs) {
    var t = namespaceId.getTenantId();
    var key = Keys.nsPtr(t, catalogId.getId(), namespaceId.getId());
    var blob = Keys.nsBlob(t, namespaceId.getId());
    return safeMetaOrDefault(key, blob, nowTs);
  }

  private static void requireCatalogId(ResourceId catalogId) {
    if (catalogId == null || catalogId.getId() == null || catalogId.getId().isBlank())
      throw new IllegalArgumentException("namespace requires non-empty catalog_id");
  }
}
