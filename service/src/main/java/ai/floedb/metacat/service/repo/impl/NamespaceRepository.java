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

  public List<Namespace> list(ResourceId catalogId, int limit, String token, StringBuilder next) {
    return listByPrefix(Keys.nsPtr(catalogId.getTenantId(), catalogId.getId(), ""), limit, token, next);
  }

  public int count(ResourceId catalogId) {
    return countByPrefix(Keys.nsPtr(catalogId.getTenantId(), catalogId.getId(), ""));
  }

  public void put(Namespace ns, ResourceId catalogId, List<String> parentPathSegments) {
    var nsRid = ns.getResourceId();
    var tid = nsRid.getTenantId();

    delete(catalogId, nsRid);
    put(Keys.nsPtr(tid, catalogId.getId(), nsRid.getId()),
        Keys.nsBlob(tid, catalogId.getId(), nsRid.getId()),
        ns);

    List<String> parents = (parentPathSegments == null)
        ? Collections.emptyList()
        : parentPathSegments;

    var catName = nameIndex.getCatalogById(tid, catalogId.getId())
        .map(NameRef::getCatalog).orElse("");

    nameIndex.upsertNamespace(tid, catalogId, catName, nsRid, parents, ns.getDisplayName());
  }

  public boolean putWithPrecondition(
      Namespace namespace, 
      ResourceId catalogId,
      List<String> parentPathSegments,
      long expectedPointerVersion) {
    var nsId = namespace.getResourceId();
    var tId = catalogId.getTenantId();
    var key = Keys.nsPtr(tId, catalogId.getId(), nsId.getId());
    var uri = Keys.nsBlob(tId, catalogId.getId(), nsId.getId());

    var catName = nameIndex.getCatalogById(tId, catalogId.getId())
        .map(NameRef::getCatalog).orElse("");

    boolean ok = putWithPrecondition(key, uri, namespace, expectedPointerVersion);
    if (ok) {
      nameIndex.upsertNamespace(
          tId,
          catalogId,
          catName,
          nsId,
          parentPathSegments,
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
            .orElse("");

    if (sameCatalog) {
      boolean ok = putWithPrecondition(newKey, newUri, updated, expectedVersion);
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

    boolean deleted = ptr.delete(oldKey);
    if (!deleted) {
      return false;
    }

    byte[] bytes = toBytes.apply(updated);
    var hdr = blobs.head(newUri);
    String etag = sha256B64(bytes);
    if (hdr.isEmpty() || !etag.equals(hdr.get().getEtag())) {
      blobs.put(newUri, bytes, contentType);
    }

    var next = Pointer.newBuilder()
        .setKey(newKey)
        .setBlobUri(newUri)
        .setVersion(1L).build();
    boolean created = ptr.compareAndSet(newKey, 0L, next);
    if (!created) {
      return false;
    }

    nameIndex.removeNamespace(tenant, nsId);
    nameIndex.upsertNamespace(
        tenant, newCatalogId, newCatalogName, nsId, newParents, updated.getDisplayName());

    return true;
  }

  public boolean delete(ResourceId catalogId, ResourceId nsId) {
    var tId = nsId.getTenantId();
    var cId = catalogId.getId();
    var nId = nsId.getId();

    nameIndex.removeNamespace(tId, nsId);

    String ptrKey = Keys.nsPtr(tId, cId, nId);
    String blobUri = Keys.nsBlob(tId, cId, nId);

    boolean okPtr = ptr.delete(ptrKey);
    boolean okBlob = blobs.delete(blobUri);
    return okPtr && okBlob;
  }

  public MutationMeta metaFor(ResourceId catalogId, ResourceId namespaceId) {
    String tenant = namespaceId.getTenantId();
    String key = Keys.nsPtr(
      tenant, catalogId.getId(), namespaceId.getId());

    var p = ptr.get(key)
        .orElseThrow(
            () -> new IllegalStateException(
                "Pointer missing for namespace: " + namespaceId.getId()));

    var hdr = blobs.head(p.getBlobUri());
    return buildMeta(key, p, hdr, clock);
  }
}
