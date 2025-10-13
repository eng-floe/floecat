package ai.floedb.metacat.service.repo.impl;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import ai.floedb.metacat.catalog.rpc.Namespace;
import ai.floedb.metacat.common.rpc.NameRef;
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
      .flatMap(catRid -> get(Keys.nsPtr(nsId.getTenantId(), catRid.getId(), nsId.getId())));
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

  public boolean delete(ResourceId catalogId, ResourceId nsId) {
    var tid = nsId.getTenantId();
    var cid = catalogId.getId();
    var nid = nsId.getId();

    nameIndex.removeNamespace(tid, nsId);

    String ptrKey = Keys.nsPtr(tid, cid, nid);
    String blobUri = Keys.nsBlob(tid, cid, nid);

    boolean okPtr = ptr.delete(ptrKey);
    boolean okBlob = blobs.delete(blobUri);
    return okPtr && okBlob;
  }
}
