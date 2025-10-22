package ai.floedb.metacat.service.repo.impl;

import ai.floedb.metacat.catalog.rpc.MutationMeta;
import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.service.repo.util.BaseRepository;
import ai.floedb.metacat.service.repo.util.Keys;
import ai.floedb.metacat.service.storage.BlobStore;
import ai.floedb.metacat.service.storage.PointerStore;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class TableRepository extends BaseRepository<Table> {
  public TableRepository() {
    super();
  }

  @Inject
  public TableRepository(PointerStore pointerStore, BlobStore blobs) {
    super(pointerStore, blobs, Table::parseFrom, Table::toByteArray, "application/x-protobuf");
  }

  public Optional<Table> get(ResourceId tableId) {
    return get(Keys.tblCanonicalPtr(tableId.getTenantId(), tableId.getId()));
  }

  public Optional<Table> getByName(ResourceId catalogId, ResourceId nsId, String name) {
    return get(Keys.tblByNamePtr(catalogId.getTenantId(), catalogId.getId(), nsId.getId(), name));
  }

  public List<Table> listByNamespace(
      ResourceId catalogId, ResourceId nsId, int limit, String token, StringBuilder next) {
    var prefix = Keys.tblByNamePrefix(nsId.getTenantId(), catalogId.getId(), nsId.getId());
    return listByPrefix(prefix, limit, token, next);
  }

  public int countUnderNamespace(ResourceId catalogId, ResourceId nsId) {
    var prefix = Keys.tblByNamePrefix(nsId.getTenantId(), catalogId.getId(), nsId.getId());
    return countByPrefix(prefix);
  }

  public void create(Table table) {
    requireOwnerIds(table);
    var tenantId = table.getResourceId().getTenantId();
    var tableId = table.getResourceId();
    var catalogId = table.getCatalogId();
    var namespaceId = table.getNamespaceId();

    var canonicalPointer = Keys.tblCanonicalPtr(tenantId, tableId.getId());
    var byName =
        Keys.tblByNamePtr(tenantId, catalogId.getId(), namespaceId.getId(), table.getDisplayName());
    var blobUri = Keys.tblBlob(tenantId, tableId.getId());

    putBlob(blobUri, table);
    reserveAllOrRollback(canonicalPointer, blobUri, byName, blobUri);
  }

  public boolean update(Table updated, long expectedVersion) {
    requireOwnerIds(updated);
    var tenantId = updated.getResourceId().getTenantId();
    var canonicalPointer = Keys.tblCanonicalPtr(tenantId, updated.getResourceId().getId());
    var blobUri = Keys.tblBlob(tenantId, updated.getResourceId().getId());

    putBlob(blobUri, updated);
    advancePointer(canonicalPointer, blobUri, expectedVersion);

    return true;
  }

  public boolean rename(ResourceId tableId, String newDisplayName, long expectedVersion) {
    var tenantId = tableId.getTenantId();
    var table =
        get(tableId).orElseThrow(() -> new BaseRepository.NotFoundException("table not found"));
    if (newDisplayName.equals(table.getDisplayName())) {
      return true;
    }

    var blobUri = Keys.tblBlob(tenantId, tableId.getId());
    var canonicalPointer = Keys.tblCanonicalPtr(tenantId, tableId.getId());
    var updated = table.toBuilder().setDisplayName(newDisplayName).build();

    putBlob(blobUri, updated);

    var newByName =
        Keys.tblByNamePtr(
            tenantId, table.getCatalogId().getId(), table.getNamespaceId().getId(), newDisplayName);

    reserveAllOrRollback(newByName, blobUri);
    try {
      advancePointer(canonicalPointer, blobUri, expectedVersion);
    } catch (PreconditionFailedException e) {
      pointerStore
          .get(newByName)
          .ifPresent(pointer -> compareAndDeleteOrFalse(newByName, pointer.getVersion()));
      return false;
    }

    var oldByName =
        Keys.tblByNamePtr(
            tenantId,
            table.getCatalogId().getId(),
            table.getNamespaceId().getId(),
            table.getDisplayName());

    pointerStore
        .get(oldByName)
        .ifPresent(pointer -> compareAndDeleteOrFalse(oldByName, pointer.getVersion()));
    return true;
  }

  public boolean move(
      Table updated,
      String oldDisplayName,
      ResourceId oldCatalogId,
      ResourceId oldNamespaceId,
      ResourceId newCatalogId,
      ResourceId newNamespaceId,
      long expectedVersion) {

    requireOwnerIds(updated);

    var tenantId = updated.getResourceId().getTenantId();
    var tableId = updated.getResourceId();

    var canonicalPointer = Keys.tblCanonicalPtr(tenantId, tableId.getId());
    var blobUri = Keys.tblBlob(tenantId, tableId.getId());

    putBlob(blobUri, updated);

    var newByName =
        Keys.tblByNamePtr(
            tenantId, newCatalogId.getId(), newNamespaceId.getId(), updated.getDisplayName());

    reserveAllOrRollback(newByName, blobUri);
    try {
      advancePointer(canonicalPointer, blobUri, expectedVersion);
    } catch (PreconditionFailedException e) {
      pointerStore
          .get(newByName)
          .ifPresent(pointer -> compareAndDeleteOrFalse(newByName, pointer.getVersion()));
      return false;
    }

    var oldByName =
        Keys.tblByNamePtr(tenantId, oldCatalogId.getId(), oldNamespaceId.getId(), oldDisplayName);

    pointerStore
        .get(oldByName)
        .ifPresent(pointer -> compareAndDeleteOrFalse(oldByName, pointer.getVersion()));

    return true;
  }

  public boolean delete(ResourceId tableId) {
    var tenantId = tableId.getTenantId();
    var canonicalPointer = Keys.tblCanonicalPtr(tenantId, tableId.getId());
    var blobUri = Keys.tblBlob(tenantId, tableId.getId());

    var tdOpt = get(tableId);
    var byName =
        tdOpt
            .map(
                td ->
                    Keys.tblByNamePtr(
                        tenantId,
                        td.getCatalogId().getId(),
                        td.getNamespaceId().getId(),
                        td.getDisplayName()))
            .orElse(null);

    if (byName != null) {
      pointerStore
          .get(byName)
          .ifPresent(pointer -> compareAndDeleteOrFalse(byName, pointer.getVersion()));
    }
    pointerStore
        .get(canonicalPointer)
        .ifPresent(pointer -> compareAndDeleteOrFalse(canonicalPointer, pointer.getVersion()));

    deleteQuietly(() -> blobStore.delete(blobUri));

    return true;
  }

  public boolean deleteWithPrecondition(ResourceId tableId, long expectedVersion) {
    var tenantId = tableId.getTenantId();
    var canonicalPointer = Keys.tblCanonicalPtr(tenantId, tableId.getId());
    var blobUri = Keys.tblBlob(tenantId, tableId.getId());

    var tableObt = get(tableId);
    var byName =
        tableObt
            .map(
                table ->
                    Keys.tblByNamePtr(
                        tenantId,
                        table.getCatalogId().getId(),
                        table.getNamespaceId().getId(),
                        table.getDisplayName()))
            .orElse(null);

    if (!compareAndDeleteOrFalse(canonicalPointer, expectedVersion)) {
      return false;
    }

    if (byName != null) {
      pointerStore
          .get(byName)
          .ifPresent(pointer -> compareAndDeleteOrFalse(byName, pointer.getVersion()));
    }

    deleteQuietly(() -> blobStore.delete(blobUri));
    return true;
  }

  public MutationMeta metaFor(ResourceId id) {
    return metaFor(id, Timestamps.fromMillis(clock.millis()));
  }

  public MutationMeta metaFor(ResourceId tableId, Timestamp nowTs) {
    var tenantId = tableId.getTenantId();
    var key = Keys.tblCanonicalPtr(tenantId, tableId.getId());
    var pointer =
        pointerStore
            .get(key)
            .orElseThrow(
                () ->
                    new BaseRepository.NotFoundException(
                        "Pointer missing for table: " + tableId.getId()));
    return safeMetaOrDefault(key, pointer.getBlobUri(), nowTs);
  }

  public MutationMeta metaForSafe(ResourceId id) {
    return metaForSafe(id, Timestamps.fromMillis(clock.millis()));
  }

  public MutationMeta metaForSafe(ResourceId tableId, Timestamp nowTs) {
    var tenantId = tableId.getTenantId();
    var key = Keys.tblCanonicalPtr(tenantId, tableId.getId());
    var blobUri = Keys.tblBlob(tenantId, tableId.getId());
    return safeMetaOrDefault(key, blobUri, nowTs);
  }

  private static void requireOwnerIds(Table table) {
    if (!table.hasCatalogId()
        || table.getCatalogId().getId().isBlank()
        || !table.hasNamespaceId()
        || table.getNamespaceId().getId().isBlank()) {
      throw new IllegalArgumentException("table requires catalog_id and namespace_id");
    }
  }
}
