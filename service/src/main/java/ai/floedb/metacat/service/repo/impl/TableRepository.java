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

  public List<NameRef> list(ResourceId nsId, int limit, String token, StringBuilder next) {
    String tenant = nsId.getTenantId();
    String idxPrefix = Keys.idxTblByNamespace(tenant, nsId.getId(), "");
    return nameIndex.listTablesByPrefix(tenant, idxPrefix, limit, token, next);
  }

  public int count(ResourceId nsId) {
    String tenant = nsId.getTenantId();
    String idxPrefix = Keys.idxTblByNamespace(tenant, nsId.getId(), "");
    return countByPrefix(idxPrefix);
  }

  public void put(TableDescriptor td) {
    var tableId = td.getResourceId();
    var tenantId = tableId.getTenantId();
    var catalogId = td.getCatalogId().getId();
    var namespaceId = td.getNamespaceId().getId();

    putAll(List.of(
        Keys.tblCanonicalPtr(tenantId, tableId.getId()),
        Keys.tblPtr(tenantId, catalogId, namespaceId, tableId.getId())
      ), Keys.tblBlob(tenantId, tableId.getId()), td);

    nameIndex.upsertTable(tenantId, td);
  }

  public boolean delete(ResourceId tableId) {
    final String tenant = tableId.getTenantId();
    final String canonPtr = Keys.tblCanonicalPtr(tenant, tableId.getId());
    final String blobUri  = Keys.tblBlob(tenant, tableId.getId());

    var tdOpt = get(tableId);

    if (tdOpt.isEmpty()) {
      try { ptr.delete(canonPtr); } catch (Throwable ignore) {}
      try { blobs.delete(blobUri); } catch (Throwable ignore) {}

      boolean goneCanon = ptr.get(canonPtr).isEmpty();
      boolean goneBlob  = blobs.head(blobUri).isEmpty();
      return goneCanon && goneBlob;
    }

    var td = tdOpt.get();
    String nsPtr = Keys.tblPtr(
        tenant, td.getCatalogId().getId(), td.getNamespaceId().getId(), tableId.getId());

    try { nameIndex.removeTable(tenant, td); } catch (Throwable ignore) {}
    try { ptr.delete(canonPtr); } catch (Throwable ignore) {}
    try { ptr.delete(nsPtr); }   catch (Throwable ignore) {}
    try { blobs.delete(blobUri);} catch (Throwable ignore) {}

    boolean goneCanon = ptr.get(canonPtr).isEmpty();
    boolean goneNs    = ptr.get(nsPtr).isEmpty();
    boolean goneBlob  = blobs.head(blobUri).isEmpty();
    return goneCanon && goneNs && goneBlob;
  }
}
