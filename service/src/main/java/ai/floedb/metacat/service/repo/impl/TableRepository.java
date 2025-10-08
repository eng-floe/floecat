package ai.floedb.metacat.service.repo.impl;

import java.util.List;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;

import ai.floedb.metacat.catalog.rpc.TableDescriptor;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.service.repo.util.BaseRepository;
import ai.floedb.metacat.service.repo.util.Keys;
import ai.floedb.metacat.service.storage.BlobStore;
import ai.floedb.metacat.service.storage.PointerStore;

@ApplicationScoped
public class TableRepository extends BaseRepository<TableDescriptor> {
  public TableRepository() {
    super(TableDescriptor::parseFrom, TableDescriptor::toByteArray, "application/x-protobuf");
  }

  public TableRepository(PointerStore ptr, BlobStore blobs) {
    this();
    this.ptr = ptr;
    this.blobs = blobs;
  }

  public Optional<TableDescriptor> getById(ResourceId tableId) {
    return get(Keys.tblCanonicalPtr(tableId.getTenantId(), tableId.getId()));
  }

  public Optional<TableDescriptor> get(ResourceId tenant, ResourceId catalogId, ResourceId nsId, ResourceId tableId) {
    return get(Keys.tblIndexPtr(tenant.getTenantId(), catalogId.getId(), nsId.getId(), tableId.getId()));
  }

  public void put(TableDescriptor t) {
    var rid = t.getResourceId();
    var cat = t.getCatalogId();
    var ns  = t.getNamespaceId();
    var tid = rid.getTenantId();
    var tbl = rid.getId();

    var uri = Keys.tblBlob(tid, tbl);

    put(Keys.tblCanonicalPtr(tid, tbl), uri, t);
    put(Keys.tblIndexPtr(tid, cat.getId(), ns.getId(), tbl), uri, t);
  }

  public List<TableDescriptor> list(String tenantId, String catalogId, String nsId,
                                    int limit, String token, StringBuilder next) {
    return listByPrefix(Keys.nsIndexPrefix(tenantId, catalogId, nsId), limit, token, next);
  }

  public int count(String tenantId, String catalogId, String nsId) {
    return countByPrefix(Keys.nsIndexPrefix(tenantId, catalogId, nsId));
  }
}