package ai.floedb.metacat.service.repo.impl;

import java.util.List;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import ai.floedb.metacat.catalog.rpc.TableDescriptor;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.service.repo.util.BaseRepository;
import ai.floedb.metacat.service.repo.util.Keys;
import ai.floedb.metacat.service.storage.BlobStore;
import ai.floedb.metacat.service.storage.PointerStore;

@ApplicationScoped
public class TableRepository extends BaseRepository<TableDescriptor> {
  private NameIndexRepository nameIndex;

  protected TableRepository() { super(); }

  @Inject
  public TableRepository(NameIndexRepository nameIndex, PointerStore ptr, BlobStore blobs) {
    super(ptr, blobs, TableDescriptor::parseFrom, TableDescriptor::toByteArray, "application/x-protobuf");
    this.nameIndex = nameIndex;
  }

  public Optional<TableDescriptor> get(ResourceId tableId) {
    return get(Keys.tblCanonicalPtr(tableId.getTenantId(), tableId.getId()));
  }

  public List<TableDescriptor> list(ResourceId nsId, int limit, String token, StringBuilder next) {
    String tenant = nsId.getTenantId();
    String catalogId = requireCatalogIdByNamespaceId(tenant, nsId.getId()).getId();
    return listByPrefix(Keys.nsIndexPrefix(tenant, catalogId, nsId.getId()), limit, token, next);
  }

  public int count(ResourceId nsId) {
    String tenant = nsId.getTenantId();
    String catalogId = requireCatalogIdByNamespaceId(tenant, nsId.getId()).getId();
    return countByPrefix(Keys.nsIndexPrefix(tenant, catalogId, nsId.getId()));
  }

  public void put(TableDescriptor t) {
    var rid = t.getResourceId();
    var cat = t.getCatalogId();
    var ns = t.getNamespaceId();
    var tid = rid.getTenantId();
    var tbl = rid.getId();

    String uri = Keys.tblBlob(tid, tbl);

    List<String> keys = List.of(
      Keys.tblCanonicalPtr(tid, tbl),
      Keys.tblIndexPtr(tid, cat.getId(), ns.getId(), tbl)
    );

    putMulti(keys, uri, t);
  }

  public boolean delete(ResourceId tableId) {
    String tenant = tableId.getTenantId();
    String tblId = tableId.getId();

    ParentIds parents = requireParentsForTable(tenant, tblId);
    String cid = parents.catalogId().getId();
    String nid = parents.namespaceId().getId();

    String canonPtr = Keys.tblCanonicalPtr(tenant, tblId);
    String nsPtr = Keys.tblIndexPtr(tenant, cid, nid, tblId);
    String blobUri = Keys.tblBlob(tenant, tblId);

    boolean okCanon = ptr.delete(canonPtr);
    boolean okNs = ptr.delete(nsPtr);
    boolean okBlob = blobs.delete(blobUri);
    return okCanon && okNs && okBlob;
  }

  private record ParentIds(ResourceId catalogId, ResourceId namespaceId) {}

  private ParentIds requireParentsForTable(String tenantId, String tableId) {
    NameRef tblRef = nameIndex.getTableById(tenantId, tableId)
      .orElseThrow(() -> new IllegalArgumentException("table index missing (by-id): " + tableId));

    ResourceId catalogId = nameIndex.getCatalogByName(tenantId, tblRef.getCatalog())
      .map(NameRef::getResourceId)
      .orElseThrow(() -> new IllegalArgumentException("catalog not found: " + tblRef.getCatalog()));

    NameRef nsRef = nameIndex.getNamespaceByPath(tenantId, catalogId.getId(), tblRef.getPathList())
      .orElseThrow(() -> new IllegalArgumentException(
          "namespace not found for path " + tblRef.getPathList() + " under catalog " + catalogId.getId()));

    return new ParentIds(catalogId, nsRef.getResourceId());
  }

  private ResourceId requireCatalogIdByNamespaceId(String tenantId, String nsId) {
    NameRef nsRef = nameIndex.getNamespaceById(tenantId, nsId)
      .orElseThrow(() -> new IllegalArgumentException("namespace index missing (by-id): " + nsId));

    return nameIndex.getCatalogByName(tenantId, nsRef.getCatalog())
      .map(NameRef::getResourceId)
      .orElseThrow(() -> new IllegalArgumentException("catalog not found: " + nsRef.getCatalog()));
  }
}
