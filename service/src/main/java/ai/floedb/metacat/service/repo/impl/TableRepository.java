package ai.floedb.metacat.service.repo.impl;

import java.util.List;
import java.util.Optional;

import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import ai.floedb.metacat.catalog.rpc.MutationMeta;
import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.service.repo.util.BaseRepository;
import ai.floedb.metacat.service.repo.util.Keys;
import ai.floedb.metacat.service.storage.BlobStore;
import ai.floedb.metacat.service.storage.PointerStore;

@ApplicationScoped
public class TableRepository extends BaseRepository<Table> {

  protected TableRepository() { super(); }

  @Inject
  public TableRepository(PointerStore ptr, BlobStore blobs) {
    super(ptr, blobs, Table::parseFrom, Table::toByteArray, "application/x-protobuf");
  }

  public Optional<Table> get(ResourceId tableId) {
    return get(Keys.tblCanonicalPtr(tableId.getTenantId(), tableId.getId()));
  }

  public List<Table> listByNamespace(ResourceId catalogId, ResourceId nsId, int limit, String token, StringBuilder next) {
    var pfx = Keys.tblByNamePrefix(nsId.getTenantId(), catalogId.getId(), nsId.getId());
    return listByPrefix(pfx, limit, token, next);
  }

  public int countUnderNamespace(ResourceId catalogId, ResourceId nsId) {
    var pfx = Keys.tblByNamePrefix(nsId.getTenantId(), catalogId.getId(), nsId.getId());
    return countByPrefix(pfx);
  }

  public void create(Table table) {
    requireOwnerIds(table);
    var tid = table.getResourceId().getTenantId();
    var tblId = table.getResourceId().getId();
    var catId = table.getCatalogId().getId();
    var nsId = table.getNamespaceId().getId();

    var canon = Keys.tblCanonicalPtr(tid, tblId);
    var byName = Keys.tblByNamePtr(tid, catId, nsId, table.getDisplayName());
    var blob = Keys.tblBlob(tid, tblId);

    reserveIndexOrIdempotent(canon,  blob);
    reserveIndexOrIdempotent(byName, blob);
    putBlob(blob, table);
  }

  public boolean update(Table updated, long expectedVersion) {
    requireOwnerIds(updated);
    var tid = updated.getResourceId().getTenantId();
    var canon = Keys.tblCanonicalPtr(tid, updated.getResourceId().getId());
    var blob = Keys.tblBlob(tid, updated.getResourceId().getId());

    putBlob(blob, updated);
    advancePointer(canon, blob, expectedVersion);
    return true;
  }

  public boolean rename(ResourceId tableId, String newDisplayName, long expectedVersion) {
    var tid = tableId.getTenantId();
    var cur = get(tableId).orElseThrow(() -> new IllegalStateException("table not found"));
    if (newDisplayName.equals(cur.getDisplayName())) {
      return true;
    }

    var blob = Keys.tblBlob(tid, tableId.getId());
    var canon = Keys.tblCanonicalPtr(tid, tableId.getId());

    var newByName = Keys.tblByNamePtr(tid, cur.getCatalogId().getId(), cur.getNamespaceId().getId(), newDisplayName);
    var oldByName = Keys.tblByNamePtr(tid, cur.getCatalogId().getId(), cur.getNamespaceId().getId(), cur.getDisplayName());

    var updated = cur.toBuilder().setDisplayName(newDisplayName).build();

    putBlob(blob, updated);

    reserveIndexOrIdempotent(newByName, blob);
    try {
      advancePointer(canon, blob, expectedVersion);
    } catch (RuntimeException e) {
      ptr.get(newByName).ifPresent(p -> compareAndDeleteOrFalse(ptr, newByName, p.getVersion()));
      throw e;
    }

    ptr.get(oldByName).ifPresent(p -> compareAndDeleteOrFalse(ptr, oldByName, p.getVersion()));
    return true;
  }

  public boolean move(Table updated,
      ResourceId oldCatalogId, ResourceId oldNamespaceId,
      ResourceId newCatalogId, ResourceId newNamespaceId,
      long expectedVersion) {

    requireOwnerIds(updated);

    var tid = updated.getResourceId().getTenantId();
    var tblId = updated.getResourceId().getId();

    var canon = Keys.tblCanonicalPtr(tid, tblId);
    var blob = Keys.tblBlob(tid, tblId);

    var oldByName = Keys.tblByNamePtr(tid, oldCatalogId.getId(), oldNamespaceId.getId(), updated.getDisplayName());
    var newByName = Keys.tblByNamePtr(tid, newCatalogId.getId(), newNamespaceId.getId(), updated.getDisplayName());

    putBlob(blob, updated);
    reserveIndexOrIdempotent(newByName, blob);
    try {
      advancePointer(canon, blob, expectedVersion);
    } catch (RuntimeException e) {
      ptr.get(newByName).ifPresent(p -> compareAndDeleteOrFalse(ptr, newByName, p.getVersion()));
      throw e;
    }

    ptr.get(oldByName).ifPresent(p -> compareAndDeleteOrFalse(ptr, oldByName, p.getVersion()));
    return true;
  }

  public boolean delete(ResourceId tableId) {
    var tid = tableId.getTenantId();
    var canon = Keys.tblCanonicalPtr(tid, tableId.getId());
    var blob = Keys.tblBlob(tid, tableId.getId());

    var tdOpt = get(tableId);
    var byName = tdOpt.map(td -> Keys.tblByNamePtr(
        tid, td.getCatalogId().getId(), td.getNamespaceId().getId(), td.getDisplayName()))
        .orElse(null);

    if (byName != null) {
      ptr.get(byName).ifPresent(p -> compareAndDeleteOrFalse(ptr, byName, p.getVersion()));
    }
    ptr.get(canon).ifPresent(p -> compareAndDeleteOrFalse(ptr, canon, p.getVersion()));
    deleteQuietly(() -> blobs.delete(blob));
    return true;
  }

  public boolean deleteWithPrecondition(ResourceId tableId, long expectedVersion) {
    var tid = tableId.getTenantId();
    var canon = Keys.tblCanonicalPtr(tid, tableId.getId());
    var blob = Keys.tblBlob(tid, tableId.getId());

    var tdOpt = get(tableId);
    var byName = tdOpt.map(td -> Keys.tblByNamePtr(
        tid, td.getCatalogId().getId(), td.getNamespaceId().getId(), td.getDisplayName()))
        .orElse(null);

    if (!compareAndDeleteOrFalse(ptr, canon, expectedVersion)) {
      return false;
    }
    if (byName != null) ptr.get(byName).ifPresent(
        p -> compareAndDeleteOrFalse(ptr, byName, p.getVersion()));
    deleteQuietly(() -> blobs.delete(blob));
    return true;
  }

  public MutationMeta metaFor(ResourceId tableId, Timestamp nowTs) {
    var t = tableId.getTenantId();
    var key = Keys.tblCanonicalPtr(t, tableId.getId());
    var p = ptr.get(key).orElseThrow(() -> new IllegalStateException(
        "Pointer missing for table: " + tableId.getId()));
    return safeMetaOrDefault(key, p.getBlobUri(), nowTs);
  }

  public MutationMeta metaForSafe(ResourceId tableId, Timestamp nowTs) {
    var t = tableId.getTenantId();
    var key = Keys.tblCanonicalPtr(t, tableId.getId());
    var blob = Keys.tblBlob(t, tableId.getId());
    return safeMetaOrDefault(key, blob, nowTs);
  }

  private static void requireOwnerIds(Table td) {
    if (!td.hasCatalogId() || td.getCatalogId().getId().isBlank()
        || !td.hasNamespaceId() || td.getNamespaceId().getId().isBlank()) {
      throw new IllegalArgumentException("table requires catalog_id and namespace_id");
    }
  }
}
