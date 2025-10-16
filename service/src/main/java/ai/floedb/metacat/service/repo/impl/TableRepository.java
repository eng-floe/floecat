package ai.floedb.metacat.service.repo.impl;

import java.util.List;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import ai.floedb.metacat.catalog.rpc.MutationMeta;
import ai.floedb.metacat.catalog.rpc.TableDescriptor;
import ai.floedb.metacat.common.rpc.Pointer;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.service.repo.util.BaseRepository;
import ai.floedb.metacat.service.repo.util.Keys;
import ai.floedb.metacat.service.storage.BlobStore;
import ai.floedb.metacat.service.storage.PointerStore;

@ApplicationScoped
public class TableRepository extends BaseRepository<TableDescriptor> {

  protected TableRepository() { super(); }

  @Inject
  public TableRepository(PointerStore ptr, BlobStore blobs) {
    super(ptr, blobs, TableDescriptor::parseFrom, TableDescriptor::toByteArray, "application/x-protobuf");
  }

  public Optional<TableDescriptor> get(ResourceId tableId) {
    return get(Keys.tblCanonicalPtr(tableId.getTenantId(), tableId.getId()));
  }

  public List<TableDescriptor> listByNamespace(ResourceId catalogId, ResourceId nsId, int limit, String token, StringBuilder next) {
    var pfx = Keys.tblByNamePrefix(nsId.getTenantId(), catalogId.getId(), nsId.getId());
    return listByPrefix(pfx, limit, token, next);
  }

  public int countUnderNamespace(ResourceId catalogId, ResourceId nsId) {
    var pfx = Keys.tblByNamePrefix(nsId.getTenantId(), catalogId.getId(), nsId.getId());
    return countByPrefix(pfx);
  }

  public void create(TableDescriptor td) {
    requireOwnerIds(td);
    var tid = td.getResourceId().getTenantId();
    var tblId = td.getResourceId().getId();
    var catId = td.getCatalogId().getId();
    var nsId  = td.getNamespaceId().getId();

    var canon = Keys.tblCanonicalPtr(tid, tblId);
    var byName = Keys.tblByNamePtr(tid, catId, nsId, td.getDisplayName());
    var blob = Keys.tblBlob(tid, tblId);

    var p = Pointer.newBuilder().setKey(byName).setBlobUri(blob).setVersion(1L).build();
    if (!ptr.compareAndSet(byName, 0L, p)) {
      var ex = ptr.get(byName).orElse(null);
      if (ex == null || !blob.equals(ex.getBlobUri()))
        throw new IllegalStateException("table name already exists in namespace");
    }

    put(canon, blob, td);
  }

  public boolean update(TableDescriptor updated, long expectedVersion) {
    requireOwnerIds(updated);
    var tid = updated.getResourceId().getTenantId();
    var canon = Keys.tblCanonicalPtr(tid, updated.getResourceId().getId());
    var blob  = Keys.tblBlob(tid, updated.getResourceId().getId());
    return update(canon, blob, updated, expectedVersion);
  }

  public boolean rename(ResourceId tableId, String newDisplayName, long expectedVersion) {
    var tid = tableId.getTenantId();
    var cur = get(tableId).orElseThrow(() -> new IllegalStateException("table not found"));
    if (newDisplayName.equals(cur.getDisplayName())) return true;

    var blob = Keys.tblBlob(tid, tableId.getId());
    var canon = Keys.tblCanonicalPtr(tid, tableId.getId());

    var newByName = Keys.tblByNamePtr(tid, cur.getCatalogId().getId(), cur.getNamespaceId().getId(), newDisplayName);
    var oldByName = Keys.tblByNamePtr(tid, cur.getCatalogId().getId(), cur.getNamespaceId().getId(), cur.getDisplayName());

    var reserve = Pointer.newBuilder().setKey(newByName).setBlobUri(blob).setVersion(1L).build();
    if (!ptr.compareAndSet(newByName, 0L, reserve)) {
      var ex = ptr.get(newByName).orElse(null);
      if (ex == null || !blob.equals(ex.getBlobUri())) return false;
    }

    var updated = cur.toBuilder().setDisplayName(newDisplayName).build();
    if (!update(canon, blob, updated, expectedVersion)) return false;

    try { ptr.delete(oldByName); } catch (Throwable ignore) {}
    return true;
  }

  public boolean move(TableDescriptor updated,
      ResourceId oldCatalogId, ResourceId oldNamespaceId,
      ResourceId newCatalogId, ResourceId newNamespaceId,
      long expectedVersion) {

    requireOwnerIds(updated);

    var tid = updated.getResourceId().getTenantId();
    var tblId = updated.getResourceId().getId();

    var canon = Keys.tblCanonicalPtr(tid, tblId);
    var blob  = Keys.tblBlob(tid, tblId);

    var oldByName = Keys.tblByNamePtr(tid, oldCatalogId.getId(), oldNamespaceId.getId(), updated.getDisplayName());
    var newByName = Keys.tblByNamePtr(tid, newCatalogId.getId(), newNamespaceId.getId(), updated.getDisplayName());

    var reserve = Pointer.newBuilder().setKey(newByName).setBlobUri(blob).setVersion(1L).build();
    if (!ptr.compareAndSet(newByName, 0L, reserve)) {
      var ex = ptr.get(newByName).orElse(null);
      if (ex == null || !blob.equals(ex.getBlobUri())) return false;
    }

    if (!update(canon, blob, updated, expectedVersion)) return false;

    try { ptr.delete(oldByName); } catch (Throwable ignore) {}
    return true;
  }

  public boolean deleteWithPrecondition(ResourceId tableId, long expectedVersion) {
    var tdOpt = get(tableId);
    var tid = tableId.getTenantId();
    var canon = Keys.tblCanonicalPtr(tid, tableId.getId());
    var blob = Keys.tblBlob(tid, tableId.getId());
    String byName = null;

    if (tdOpt.isPresent()) {
      var td = tdOpt.get();
      byName = Keys.tblByNamePtr(
          tid,
          td.getCatalogId().getId(),
          td.getNamespaceId().getId(),
          td.getDisplayName());
    }

    if (!ptr.compareAndDelete(canon, expectedVersion)) {
      return false;
    }

    try { blobs.delete(blob); } catch (Throwable ignore) {}

    if (byName != null) {
      try { ptr.delete(byName); } catch (Throwable ignore) {}
    }
    return true;
  }

  public boolean delete(ResourceId tableId) {
    var tid = tableId.getTenantId();
    var canon = Keys.tblCanonicalPtr(tid, tableId.getId());
    var blob  = Keys.tblBlob(tid, tableId.getId());

    try { ptr.delete(canon); } catch (Throwable ignore) {}
    try { blobs.delete(blob); } catch (Throwable ignore) {}
    return ptr.get(canon).isEmpty() && blobs.head(blob).isEmpty();
  }

  public MutationMeta metaFor(ResourceId tableId) {
    var t = tableId.getTenantId();
    var key = Keys.tblCanonicalPtr(t, tableId.getId());
    var p = ptr.get(key).orElseThrow(() -> new IllegalStateException("Pointer missing for table: " + tableId.getId()));
    return safeMetaOrDefault(key, p.getBlobUri(), clock);
  }

  public MutationMeta metaForSafe(ResourceId tableId) {
    var t = tableId.getTenantId();
    var key = Keys.tblCanonicalPtr(t, tableId.getId());
    var blob = Keys.tblBlob(t, tableId.getId());
    return safeMetaOrDefault(key, blob, clock);
  }

  private static void requireOwnerIds(TableDescriptor td) {
    if (!td.hasCatalogId() || td.getCatalogId().getId().isBlank()
        || !td.hasNamespaceId() || td.getNamespaceId().getId().isBlank()) {
      throw new IllegalArgumentException("table requires catalog_id and namespace_id");
    }
  }
}
