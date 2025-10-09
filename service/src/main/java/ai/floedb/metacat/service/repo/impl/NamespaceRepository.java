package ai.floedb.metacat.service.repo.impl;

import java.util.List;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;

import ai.floedb.metacat.catalog.rpc.Namespace;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.service.repo.util.BaseRepository;
import ai.floedb.metacat.service.repo.util.Keys;
import ai.floedb.metacat.service.storage.BlobStore;
import ai.floedb.metacat.service.storage.PointerStore;

@ApplicationScoped
public class NamespaceRepository extends BaseRepository<Namespace> {
  public NamespaceRepository() {
    super(Namespace::parseFrom, Namespace::toByteArray, "application/x-protobuf");
  }

  public NamespaceRepository(PointerStore ptr, BlobStore blobs) {
    this();
    this.ptr = ptr;
    this.blobs = blobs;
  }

  public Optional<Namespace> get(ResourceId nsId, ResourceId catalogId) {
    return get(Keys.nsPtr(nsId.getTenantId(), catalogId.getId(), nsId.getId()));
  }

  public void put(Namespace ns, ResourceId catalogId) {
    var rid = ns.getResourceId();
    put(Keys.nsPtr(rid.getTenantId(), catalogId.getId(), rid.getId()),
      Keys.nsBlob(rid.getTenantId(), catalogId.getId(), rid.getId()),
      ns);
  }

  public List<Namespace> list(ResourceId catalogId, int limit, String token, StringBuilder next) {
    return listByPrefix(Keys.nsPtr(catalogId.getTenantId(), catalogId.getId(), ""), limit, token, next);
  }

  public int count(ResourceId catalogId) {
    return countByPrefix(Keys.nsPtr(catalogId.getTenantId(), catalogId.getId(), ""));
  }

  public boolean delete(ResourceId catalogId, ResourceId nsId) {
    var tid = nsId.getTenantId();
    var cid = catalogId.getId();
    var nid = nsId.getId();

    String ptrKey = Keys.nsPtr(tid, cid, nid);
    String blobUri = Keys.nsBlob(tid, cid, nid);

    boolean okPtr = ptr.delete(ptrKey);
    boolean okBlob = blobs.delete(blobUri);
    return okPtr && okBlob;
  }
}