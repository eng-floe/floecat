package ai.floedb.metacat.service.repo.impl;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import ai.floedb.metacat.catalog.rpc.MutationMeta;
import ai.floedb.metacat.catalog.rpc.Namespace;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.Pointer;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.service.repo.util.BaseRepository;
import ai.floedb.metacat.service.repo.util.Keys;
import ai.floedb.metacat.service.storage.BlobStore;
import ai.floedb.metacat.service.storage.PointerStore;

@ApplicationScoped
public class NamespaceRepository extends BaseRepository<Namespace> {
  private NameIndexRepository nameIndex;

  protected NamespaceRepository() { super(); }

  @Inject
  public NamespaceRepository(NameIndexRepository nameIndex, PointerStore ptr, BlobStore blobs) {
    super(ptr, blobs, Namespace::parseFrom, Namespace::toByteArray, "application/x-protobuf");
    this.nameIndex = nameIndex;
  }

  public Optional<Namespace> get(ResourceId nsId) {
    return nameIndex.getNamespaceOwner(nsId.getTenantId(), nsId.getId())
        .flatMap(
            catRid -> get(Keys.nsPtr(nsId.getTenantId(), catRid.getId(), nsId.getId())));
  }

  public List<NameRef> list(ResourceId catalogId, List<String> pathPrefix,
                                int limit, String token, StringBuilder next) {
    return nameIndex.listNamespaceRefsByCatalog(
        catalogId.getTenantId(), catalogId, pathPrefix, limit, token, next);
  }

  public int count(ResourceId catalogId) {
    return countByPrefix(Keys.nsPtr(catalogId.getTenantId(), catalogId.getId(), ""));
  }

  public void put(Namespace ns, ResourceId catalogId) {
    requireCatalogId(catalogId);

    var nsRid = ns.getResourceId();
    var tid = nsRid.getTenantId();

    delete(catalogId, nsRid);
    put(Keys.nsPtr(tid, catalogId.getId(), nsRid.getId()),
        Keys.nsBlob(tid, catalogId.getId(), nsRid.getId()),
        ns);

    List<String> parents = (ns.getParentsList() == null)
        ? Collections.emptyList()
        : ns.getParentsList();

    var catName = nameIndex.getCatalogById(tid, catalogId.getId())
        .map(NameRef::getCatalog)
        .orElseThrow(() -> new IllegalStateException(
            "catalog by-id index missing for " + catalogId.getId()));

    nameIndex.upsertNamespace(tid, catalogId, catName, nsRid, parents, ns.getDisplayName());
  }

  public boolean update(
      Namespace namespace, 
      ResourceId catalogId,
      long expectedPointerVersion) {
    requireCatalogId(catalogId);

    var nsId = namespace.getResourceId();
    var tId = catalogId.getTenantId();
    var key = Keys.nsPtr(tId, catalogId.getId(), nsId.getId());
    var uri = Keys.nsBlob(tId, catalogId.getId(), nsId.getId());

    var catName = nameIndex.getCatalogById(tId, catalogId.getId())
        .map(NameRef::getCatalog).orElse("");

    boolean ok = update(key, uri, namespace, expectedPointerVersion);
    if (ok) {
      nameIndex.upsertNamespace(
          tId,
          catalogId,
          catName,
          nsId,
          namespace.getParentsList(),
          namespace.getDisplayName());
    }
    return ok;
  }

  public boolean renameWithPrecondition(
      Namespace updated,
      ResourceId oldCatalogId,
      List<String> oldFullPath,
      ResourceId newCatalogId,
      List<String> newParents,
      long expectedVersion) {

    requireCatalogId(oldCatalogId);
    requireCatalogId(newCatalogId);

    final var nsId = updated.getResourceId();
    final var tenant = nsId.getTenantId();

    final String oldKey = Keys.nsPtr(tenant, oldCatalogId.getId(), nsId.getId());
    final String oldUri = Keys.nsBlob(tenant, oldCatalogId.getId(), nsId.getId());
    final String newKey = Keys.nsPtr(tenant, newCatalogId.getId(), nsId.getId());
    final String newUri = Keys.nsBlob(tenant, newCatalogId.getId(), nsId.getId());

    final boolean sameCatalog = oldCatalogId.getId().equals(newCatalogId.getId());
    final String newCatalogName =
        nameIndex.getCatalogById(tenant, newCatalogId.getId())
            .map(NameRef::getCatalog)
            .orElseThrow(() -> new IllegalStateException(
                "catalog by-id index missing for " + newCatalogId.getId()));

    if (sameCatalog) {
      boolean ok = update(newKey, newUri, updated, expectedVersion);
      if (!ok) {
        return false;
      }

      nameIndex.removeNamespace(tenant, nsId);
      nameIndex.upsertNamespace(
          tenant, newCatalogId, newCatalogName, nsId, newParents, updated.getDisplayName());
      return true;
    }

    var curPtr = ptr.get(oldKey);
    if (curPtr.isEmpty() || curPtr.get().getVersion() != expectedVersion) {
      return false;
    }

    byte[] bytes = toBytes.apply(updated);
    String etag = sha256B64(bytes);
    var hdr = blobs.head(newUri);
    if (hdr.isEmpty() || !etag.equals(hdr.get().getEtag())) {
      blobs.put(newUri, bytes, contentType);
    }

    var newPtr = Pointer.newBuilder()
        .setKey(newKey).setBlobUri(newUri).setVersion(1L).build();

    var existingNew = ptr.get(newKey);
    if (existingNew.isPresent()) {
      if (!newUri.equals(existingNew.get().getBlobUri())) {
        return false;
      }
    } else {
      if (!ptr.compareAndSet(newKey, 0L, newPtr)) {
        var nowNew = ptr.get(newKey);
        if (nowNew.isEmpty() || !newUri.equals(nowNew.get().getBlobUri())) {
          return false;
        }
      }
    }

    if (!ptr.compareAndDelete(oldKey, expectedVersion)) {
      if (existingNew.isEmpty()) {
        try { ptr.delete(newKey); } catch (Throwable ignore) {}
        try { blobs.delete(newUri); } catch (Throwable ignore) {}
      }
      return false;
    }

    try { blobs.delete(oldUri); } catch (Throwable ignore) {}

    nameIndex.removeNamespace(tenant, nsId);
    nameIndex.upsertNamespace(
        tenant, newCatalogId, newCatalogName, nsId, newParents, updated.getDisplayName());

    return true;
  }

  public boolean delete(ResourceId catalogId, ResourceId namespaceId) {
    var t = namespaceId.getTenantId();
    var c = catalogId.getId();
    var n = namespaceId.getId();
    var key = Keys.nsPtr(t, c, n);
    var uri = Keys.nsBlob(t, c, n);

    try { nameIndex.removeNamespace(t, namespaceId); } catch (Throwable ignore) {}
    try { ptr.delete(key); } catch (Throwable ignore) {}
    try { blobs.delete(uri); } catch (Throwable ignore) {}

    boolean gonePtr = ptr.get(key).isEmpty();
    boolean goneBlob = blobs.head(uri).isEmpty();
    return gonePtr && goneBlob;
  }

  public boolean deleteWithPrecondition(
      ResourceId catalogId,
      ResourceId namespaceId,
      long expectedVersion) {
    var tId = namespaceId.getTenantId();
    var cId = catalogId.getId();
    var nId = namespaceId.getId();
    
    String ptrkey = Keys.nsPtr(tId, cId, nId);
    var blobUri = Keys.nsBlob(tId, cId, nId);

    var p = ptr.get(ptrkey);
    if (p.isEmpty() || p.get().getVersion() != expectedVersion) {
      return false;
    }

    boolean deleted = ptr.compareAndDelete(ptrkey, expectedVersion);
    if (!deleted) 
    {
        return false;
    }

    nameIndex.removeNamespace(tId, namespaceId);

    blobs.delete(blobUri);

    return true;
  }

  public MutationMeta metaFor(ResourceId catalogId, ResourceId namespaceId) {
    String tenant = namespaceId.getTenantId();
    String key = Keys.nsPtr(tenant, catalogId.getId(), namespaceId.getId());
    var p = ptr.get(key).orElseThrow(() -> new IllegalStateException(
        "Pointer missing for namespace: " + namespaceId.getId()));
    return safeMetaOrDefault(key, p.getBlobUri(), clock);
  }

  public MutationMeta metaForSafe(ResourceId catalogId, ResourceId namespaceId) {
    var t = namespaceId.getTenantId();
    var key = Keys.nsPtr(t, catalogId.getId(), namespaceId.getId());
    var uri = Keys.nsBlob(t, catalogId.getId(), namespaceId.getId());
    return safeMetaOrDefault(key, uri, clock);
  }

  private static void requireCatalogId(ResourceId catalogId) {
    if (catalogId == null || catalogId.getId() == null || catalogId.getId().isBlank())
      throw new IllegalArgumentException("namespace requires non-empty catalog_id");
  }
}
