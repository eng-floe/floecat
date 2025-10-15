package ai.floedb.metacat.service.repo.impl;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import ai.floedb.metacat.catalog.rpc.MutationMeta;
import ai.floedb.metacat.catalog.rpc.TableDescriptor;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.Pointer;
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
    super(
        ptr, 
        blobs,
        TableDescriptor::parseFrom,
        TableDescriptor::toByteArray,
        "application/x-protobuf");
    this.nameIndex = nameIndex;
  }

  public Optional<TableDescriptor> get(ResourceId tableId) {
    return get(Keys.tblCanonicalPtr(tableId.getTenantId(), tableId.getId()));
  }

  public List<NameRef> list(ResourceId nsId, int limit, String token, StringBuilder next) {
    String tenant = nsId.getTenantId();
    String pfx = Keys.idxTblByNamespaceLeafPrefix(tenant, nsId.getId());
    return nameIndex.listTablesByPrefix(tenant, pfx, limit, token, next);
  }

  public int count(ResourceId nsId) {
    String tenant = nsId.getTenantId();
    String pfx = Keys.idxTblByNamespaceLeafPrefix(tenant, nsId.getId());
    return countByPrefix(pfx);
  }

  public void put(TableDescriptor td) {
    requireOwnerIds(td);

    var tableId = td.getResourceId();
    var tenantId = tableId.getTenantId();
    var catalogId = td.getCatalogId().getId();
    var namespaceId = td.getNamespaceId().getId();

    putAll(
        List.of(
            Keys.tblCanonicalPtr(tenantId, tableId.getId()),
            Keys.tblPtr(tenantId, catalogId, namespaceId, tableId.getId())
        ),
        Keys.tblBlob(tenantId, tableId.getId()),
        td);

    nameIndex.upsertTable(tenantId, td);
  }

  public boolean update(TableDescriptor updated, long expectedVersion) {
    requireOwnerIds(updated);

    var tableId = updated.getResourceId();
    var tenantId = tableId.getTenantId();
    var catalogId = updated.getCatalogId().getId();
    var namespaceId = updated.getNamespaceId().getId();

    var canTblKey = Keys.tblCanonicalPtr(tenantId, tableId.getId());
    var tblKey = Keys.tblPtr(tenantId, catalogId, namespaceId, tableId.getId());
    var uri = Keys.tblBlob(tenantId, tableId.getId());

    boolean okCanon = update(canTblKey, uri, updated, expectedVersion);
    if (!okCanon) return false;

    boolean okNs = update(tblKey, uri, updated, expectedVersion);
    if (!okNs) {
      return false;
    }

    nameIndex.removeTable(tenantId, updated);
    nameIndex.upsertTable(tenantId, updated);
    return true;
  }

  public boolean moveWithPrecondition(
      TableDescriptor updated,
      ResourceId oldCatalogId,
      ResourceId oldNamespaceId,
      ResourceId newCatalogId,
      ResourceId newNamespaceId,
      long expectedVersion) {
    requireOwnerIds(updated);

    var tableId = updated.getResourceId();
    var tenantId = tableId.getTenantId();
    var catalogId = updated.getCatalogId().getId();
    var namespaceId = updated.getNamespaceId().getId();

    if (!updated.getCatalogId().getId().equals(newCatalogId.getId())
        || !updated.getNamespaceId().getId().equals(newNamespaceId.getId())) {
      throw new IllegalArgumentException("updated descriptor owner ids must match newCatalogId/newNamespaceId");
    }

    final String tlbKey = Keys.tblCanonicalPtr(tenantId, tableId.getId());
    final String tlbUri = Keys.tblBlob(tenantId, tableId.getId());
    final String oldNsKey = Keys.tblPtr(tenantId, oldCatalogId.getId(), oldNamespaceId.getId(), tableId.getId());
    final String newNsKey = Keys.tblPtr(tenantId, newCatalogId.getId(), newNamespaceId.getId(), tableId.getId());

    final byte[] bytes = toBytes.apply(updated);
    final String etag  = sha256B64(bytes);
    var hdr = blobs.head(tlbUri);
    if (hdr.isEmpty() || !etag.equals(hdr.get().getEtag())) {
      blobs.put(tlbUri, bytes, contentType);
    }

    var curPtr = ptr.get(tlbKey);
    if (curPtr.isEmpty() || curPtr.get().getVersion() != expectedVersion) {
      return false;
    }
    var nextPtr = Pointer.newBuilder()
        .setKey(tlbKey)
        .setBlobUri(tlbUri)
        .setVersion(expectedVersion + 1)
        .build();

    if (!ptr.compareAndSet(tlbKey, expectedVersion, nextPtr)) {
      return false;
    }
      
    try {
      putAll(List.of(newNsKey), tlbUri, updated);
    } catch (Throwable ignore) { }

    try { ptr.delete(oldNsKey); } catch (Throwable ignore) {}

    try { nameIndex.removeTable(tenantId, updated); } catch (Throwable ignore) {}
    try { nameIndex.upsertTable(tenantId, updated); } catch (Throwable ignore) {}

    return true;
  }

  public boolean delete(ResourceId tableId) {
    final String tenant = tableId.getTenantId();
    final String canonPtr = Keys.tblCanonicalPtr(tenant, tableId.getId());
    final String blobUri  = Keys.tblBlob(tenant, tableId.getId());

    var tdOpt = get(tableId);

    if (tdOpt.isEmpty()) {
      try { 
        ptr.delete(canonPtr); 
      } catch (Throwable ignore) {}
      try { 
        blobs.delete(blobUri); 
      } catch (Throwable ignore) {}

      boolean goneCanon = ptr.get(canonPtr).isEmpty();
      boolean goneBlob = blobs.head(blobUri).isEmpty();
      return goneCanon && goneBlob;
    }

    var td = tdOpt.get();
    String nsPtr = Keys.tblPtr(
        tenant, td.getCatalogId().getId(), td.getNamespaceId().getId(), tableId.getId());

    try { nameIndex.removeTable(tenant, td); } catch (Throwable ignore) {}
    try { ptr.delete(canonPtr); } catch (Throwable ignore) {}
    try { ptr.delete(nsPtr); } catch (Throwable ignore) {}
    try { blobs.delete(blobUri);} catch (Throwable ignore) {}

    boolean goneCanon = ptr.get(canonPtr).isEmpty();
    boolean goneNs = ptr.get(nsPtr).isEmpty();
    boolean goneBlob = blobs.head(blobUri).isEmpty();
    return goneCanon && goneNs && goneBlob;
  }

  public boolean deleteWithPrecondition(ResourceId tableId, long expectedVersion) {
    final String tenant = tableId.getTenantId();
    final String canonKey = Keys.tblCanonicalPtr(tenant, tableId.getId());

    var pCanonOpt = ptr.get(canonKey);
    if (pCanonOpt.isEmpty()) {
      bestEffortPurge(tenant, tableId.getId(), null);
      return true;
    }

    TableDescriptor td = null;
    try { 
      td = TableDescriptor.parseFrom(blobs.get(pCanonOpt.get().getBlobUri())); 
    } catch (Throwable ignore) {}

    if (pCanonOpt.get().getVersion() != expectedVersion) {
      return false;
    }

    if (!ptr.compareAndDelete(canonKey, expectedVersion)) {
      return false;
    }

    if (td != null) {
      var nsKey = Keys.tblPtr(
          tenant, td.getCatalogId().getId(), td.getNamespaceId().getId(), tableId.getId());
      try { ptr.delete(nsKey); } catch (Throwable ignore) {}
    } else {
      sweepNamespaceTablePointers(tenant, tableId.getId());
    }

    bestEffortPurge(tenant, tableId.getId(), td);
    return true;
  }

  private void bestEffortPurge(String tenant, String tableId, TableDescriptor td) {
    try { blobs.delete(Keys.tblBlob(tenant, tableId)); } catch (Throwable ignore) {}
    if (td != null) {
      try { nameIndex.removeTable(tenant, td); } catch (Throwable ignore) {}
    } else {
      sweepNameRefsByTableId(tenant, tableId);
    }

    var kById = Keys.idxTblById(tenant, tableId);
    try { ptr.delete(kById); }  catch (Throwable ignore) {}
    try { blobs.delete(Keys.memUriFor(kById, "entry.pb")); } catch (Throwable ignore) {}

    sweepNamespaceTablePointers(tenant, tableId);
  }

  private void sweepNameRefsByTableId(String tenant, String tableId) {
    sweepNameRefPrefix("/tenants/" + tenant.toLowerCase() + "/_index/tables/by-name/", tableId);
    sweepNameRefPrefix("/tenants/" + tenant.toLowerCase() + "/_index/tables/by-namespace/", tableId);
  }

  private void sweepNameRefPrefix(String prefix, String tableId) {
    String token = ""; 
    var next = new StringBuilder();
    do {
      var rows = ptr.listPointersByPrefix(prefix, 200, token, next);
      var uris = new ArrayList<String>(rows.size());
      for (var r : rows) uris.add(r.blobUri());
      var blobMap = blobs.getBatch(uris);
      for (var r : rows) {
        var bytes = blobMap.get(r.blobUri());
        if (bytes == null) continue;
        try {
          var ref = NameRef.parseFrom(bytes);
          if (ref.hasResourceId() && tableId.equals(ref.getResourceId().getId())) {
            try { ptr.delete(r.key()); } catch (Throwable ignore) {}
            try { blobs.delete(r.blobUri()); } catch (Throwable ignore) {}
          }
        } catch (Throwable ignore) {}
      }
      token = next.toString(); next.setLength(0);
    } while (!token.isEmpty());
  }

  private void sweepNamespaceTablePointers(String tenant, String tableId) {
    String root = "/tenants/" + tenant.toLowerCase() + "/catalogs/";
    String encTbl = URLEncoder.encode(tableId, StandardCharsets.UTF_8);
    String token = ""; var next = new StringBuilder();
    do {
      var rows = ptr.listPointersByPrefix(root, 200, token, next);
      for (var r : rows) {
        if (r.key().endsWith("/tables/by-id/" + encTbl)) {
          try { ptr.delete(r.key()); } catch (Throwable ignore) {}
          try { blobs.delete(r.blobUri()); } catch (Throwable ignore) {}
        }
      }
      token = next.toString(); next.setLength(0);
    } while (!token.isEmpty());
  }

  public MutationMeta metaFor(ResourceId tableId) {
    String tenant = tableId.getTenantId();
    String key = Keys.tblCanonicalPtr(tenant, tableId.getId());
    String blob = Keys.tblBlob(tenant, tableId.getId()); 
    ptr.get(key).orElseThrow(() -> new IllegalStateException(
        "Pointer missing for table: " + tableId.getId()));
    return safeMetaOrDefault(key, blob, clock);
  }

  public MutationMeta metaForSafe(ResourceId tableId) {
    String tenant = tableId.getTenantId();
    String key = Keys.tblCanonicalPtr(tenant, tableId.getId());
    String blob = Keys.tblBlob(tenant, tableId.getId());
    return safeMetaOrDefault(key, blob, clock);
  }

  private static void requireOwnerIds(TableDescriptor td) {
    if (!td.hasCatalogId() || td.getCatalogId().getId().isBlank()
    || !td.hasNamespaceId() || td.getNamespaceId().getId().isBlank()) {
      throw new IllegalArgumentException("table requires catalog_id and namespace_id");
    }
  }
}
