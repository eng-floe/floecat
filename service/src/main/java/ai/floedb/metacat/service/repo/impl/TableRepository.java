package ai.floedb.metacat.service.repo.impl;

import java.util.Optional;
import java.util.List;
import java.util.ArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import ai.floedb.metacat.catalog.rpc.Snapshot;
import ai.floedb.metacat.catalog.rpc.TableDescriptor;
import ai.floedb.metacat.common.rpc.Pointer;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.service.storage.BlobStore;
import ai.floedb.metacat.service.storage.PointerStore;

@ApplicationScoped
public class TableRepository {
  @Inject PointerStore ptr;
  @Inject BlobStore blobs;

  private static String tableCanonicalPtr(String tenantId, String tableId) {
    return "/tenants/" + tenantId + "/tables/" + tableId;
  }
  private static String tableBlobUri(String tenantId, String tableId) {
    return "mem://tenants/" + tenantId + "/tables/" + tableId + "/table.pb";
  }

  private static String tableIndexPtr(String tenantId, String catalogId, String nsId, String tableId) {
    return "/tenants/" + tenantId + "/catalogs/" + catalogId + "/namespaces/" + nsId + "/tables/" + tableId;
  }
  private static String nsIndexPrefix(String tenantId, String catalogId, String nsId) {
    return tableIndexPtr(tenantId, catalogId, nsId, "");
  }

  private static String snapshotPrefix(String tenantId, String tableId) {
    return "/tenants/" + tenantId + "/tables/" + tableId + "/snapshots/";
  }

  public void put(TableDescriptor t) {
    var rid = t.getResourceId();
    var cat = t.getCatalogId();
    var ns  = t.getNamespaceId();

    String tenantId = rid.getTenantId();
    String tableId  = rid.getId();

    String uri = tableBlobUri(tenantId, tableId);
    blobs.put(uri, t.toByteArray(), "application/x-protobuf");

    String canonKey = tableCanonicalPtr(tenantId, tableId);
    casUpsert(canonKey, uri);

    String idxKey = tableIndexPtr(tenantId, cat.getId(), ns.getId(), tableId);
    casUpsert(idxKey, uri);
  }

  public Optional<TableDescriptor> getById(ResourceId tableId) {
    String k = tableCanonicalPtr(tableId.getTenantId(), tableId.getId());
    return ptr.get(k).map(p -> parseTable(blobs.get(p.getBlobUri())));
  }

  public Optional<TableDescriptor> get(ResourceId tenant, ResourceId catalogId, ResourceId nsId, ResourceId tableId) {
    String k = tableIndexPtr(tenant.getTenantId(), catalogId.getId(), nsId.getId(), tableId.getId());
    return ptr.get(k).map(p -> parseTable(blobs.get(p.getBlobUri())));
  }

  public List<TableDescriptor> list(String tenantId, String catalogId, String nsId,
                                    int limit, String pageToken, StringBuilder nextToken) {
    String prefix = nsIndexPrefix(tenantId, catalogId, nsId);
    List<String> keys = ptr.listByPrefix(prefix, Math.max(1, limit), pageToken, nextToken);
    List<TableDescriptor> out = new ArrayList<>(keys.size());
    for (String k : keys) {
      ptr.get(k).ifPresent(p -> out.add(parseTable(blobs.get(p.getBlobUri()))));
    }
    return out;
  }

  public int count(String tenantId, String catalogId, String nsId) {
    String prefix = nsIndexPrefix(tenantId, catalogId, nsId);
    StringBuilder ignore = new StringBuilder();
    return ptr.listByPrefix(prefix, Integer.MAX_VALUE, "", ignore).size();
  }

  public List<Snapshot> listSnapshots(ResourceId tableId, int limit, String pageToken, StringBuilder nextOut) {
    String prefix = snapshotPrefix(tableId.getTenantId(), tableId.getId());
    List<String> keys = ptr.listByPrefix(prefix, Math.max(1, limit), pageToken, nextOut);
    List<Snapshot> out = new ArrayList<>(keys.size());
    for (String k : keys) {
      ptr.get(k).ifPresent(p -> out.add(parseSnapshot(blobs.get(p.getBlobUri()))));
    }
    return out;
  }

  public int countSnapshots(ResourceId tableId) {
    String prefix = snapshotPrefix(tableId.getTenantId(), tableId.getId());
    StringBuilder ignore = new StringBuilder();
    return ptr.listByPrefix(prefix, Integer.MAX_VALUE, "", ignore).size();
  }

  public Optional<Snapshot> getCurrentSnapshot(ResourceId tableId) {
    return getById(tableId)
      .filter(t -> t.getCurrentSnapshotId() != 0)
      .map(t -> Snapshot.newBuilder()
        .setSnapshotId(t.getCurrentSnapshotId())
        .setCreatedAtMs(t.getCreatedAtMs())
        .build());
  }

  private void casUpsert(String key, String blobUri) {
    for (int i = 0; i < 10; i++) {
      long expected = ptr.get(key).map(Pointer::getVersion).orElse(0L);
      Pointer next = Pointer.newBuilder().setKey(key).setBlobUri(blobUri).setVersion(expected + 1).build();
      if (ptr.compareAndSet(key, expected, next)) return;
    }
    throw new IllegalStateException("CAS failed: " + key);
  }

  private static TableDescriptor parseTable(byte[] bytes) {
    try { 
      return TableDescriptor.parseFrom(bytes); 
    }
    catch (Exception e) { 
      throw new RuntimeException(e); 
    }
  }

  private static Snapshot parseSnapshot(byte[] bytes) {
    try { 
      return Snapshot.parseFrom(bytes); 
    }
    catch (Exception e) { 
      throw new RuntimeException(e); 
    }
  }
}