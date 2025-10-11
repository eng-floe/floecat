package ai.floedb.metacat.service.repo.impl;

import java.util.List;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;

import ai.floedb.metacat.catalog.rpc.Catalog;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.service.repo.util.BaseRepository;
import ai.floedb.metacat.service.repo.util.Keys;
import ai.floedb.metacat.service.storage.BlobStore;
import ai.floedb.metacat.service.storage.PointerStore;

@ApplicationScoped
public class CatalogRepository extends BaseRepository<Catalog> {

  public CatalogRepository() {
    super(Catalog::parseFrom, Catalog::toByteArray, "application/x-protobuf");
  }

  public CatalogRepository(PointerStore ptr, BlobStore blobs) {
    this();
    this.ptr = ptr;
    this.blobs = blobs;
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

  public void put(Catalog c) {
    var rid = c.getResourceId();
    put(Keys.catPtr(rid.getTenantId(), rid.getId()),
        Keys.catBlob(rid.getTenantId(), rid.getId()),
        c);
  }

  public boolean delete(ResourceId catalogRid) {
    var tid = catalogRid.getTenantId();
    var cid = catalogRid.getId();
    String ptrKey = Keys.catPtr(tid, cid);
    String blobUri = Keys.catBlob(tid, cid);

    boolean okPtr = ptr.delete(ptrKey);
    boolean okBlob = blobs.delete(blobUri);
    return okPtr && okBlob;
  }
}