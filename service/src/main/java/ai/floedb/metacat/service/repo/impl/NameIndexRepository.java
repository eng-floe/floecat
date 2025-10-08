package ai.floedb.metacat.service.repo.impl;

import java.util.Optional;
import java.util.List;
import java.util.ArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import ai.floedb.metacat.catalog.rpc.CatalogIndexEntry;
import ai.floedb.metacat.catalog.rpc.NamespaceIndexEntry;
import ai.floedb.metacat.catalog.rpc.NamespaceRef;
import ai.floedb.metacat.catalog.rpc.TableIndexEntry;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.Pointer;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.service.storage.BlobStore;
import ai.floedb.metacat.service.storage.PointerStore;

@ApplicationScoped
public class NameIndexRepository {
  @Inject PointerStore ptr;
  @Inject BlobStore blobs;

  public void putCatalogIndex(String tenantId, String displayName, ResourceId catalogId) {
    var entry = CatalogIndexEntry.newBuilder()
      .setDisplayName(displayName).setResourceId(catalogId).build();

    var k1  = "/tenants/" + tenantId + "/_index/catalogs/by-name/" + displayName;
    var uri1 = "mem://" + k1.substring(1) + "/entry.pb";
    putCas(k1, uri1, entry.toByteArray());

    var k2  = "/tenants/" + tenantId + "/_index/catalogs/by-id/" + catalogId.getId();
    var uri2 = "mem://" + k2.substring(1) + "/entry.pb";
    putCas(k2, uri2, entry.toByteArray());
  }

  public Optional<CatalogIndexEntry> getCatalogByName(String tenantId, String displayName) {
    var k = "/tenants/" + tenantId + "/_index/catalogs/by-name/" + displayName;
    return readEntry(k, CatalogIndexEntry::parseFrom);
  }

  public Optional<CatalogIndexEntry> getCatalogById(String tenantId, String id) {
    var k = "/tenants/" + tenantId + "/_index/catalogs/by-id/" + id;
    return readEntry(k, CatalogIndexEntry::parseFrom);
  }

  public void putNamespaceIndex(String tenantId, NamespaceRef ref, ResourceId nsId) {
    var entry = NamespaceIndexEntry.newBuilder().setRef(ref).setResourceId(nsId).build();
    var path = String.join("/", ref.getNamespacePathList());

    var k1 = "/tenants/" + tenantId + "/_index/namespaces/by-path/"
           + ref.getCatalogId().getId() + "/" + path;
    var uri1 = "mem://" + k1.substring(1) + "/entry.pb";
    putCas(k1, uri1, entry.toByteArray());

    var k2 = "/tenants/" + tenantId + "/_index/namespaces/by-id/" + nsId.getId();
    var uri2 = "mem://" + k2.substring(1) + "/entry.pb";
    putCas(k2, uri2, entry.toByteArray());
  }

  public Optional<NamespaceIndexEntry> getNamespaceByPath(String tenantId, String catalogId, List<String> path) {
    var k = "/tenants/" + tenantId + "/_index/namespaces/by-path/" + catalogId + "/" + String.join("/", path);
    return readEntry(k, NamespaceIndexEntry::parseFrom);
  }

  public Optional<NamespaceIndexEntry> getNamespaceById(String tenantId, String id) {
    var k = "/tenants/" + tenantId + "/_index/namespaces/by-id/" + id;
    return readEntry(k, NamespaceIndexEntry::parseFrom);
  }

  public void putTableIndex(String tenantId, NameRef name, ResourceId tableId) {
    var entry = TableIndexEntry.newBuilder().setFqName(name).setResourceId(tableId).build();

    var kName = "/tenants/" + tenantId + "/_index/tables/by-name/" + fqKey(name);
    var uri1  = "mem://" + kName.substring(1) + "/entry.pb";
    putCas(kName, uri1, entry.toByteArray());

    var kId   = "/tenants/" + tenantId + "/_index/tables/by-id/" + tableId.getId();
    var uri2  = "mem://" + kId.substring(1) + "/entry.pb";
    putCas(kId, uri2, entry.toByteArray());
  }

  public Optional<TableIndexEntry> getTableByName(String tenantId, NameRef name) {
    var k = "/tenants/" + tenantId + "/_index/tables/by-name/" + fqKey(name);
    return readEntry(k, TableIndexEntry::parseFrom);
  }

  public Optional<TableIndexEntry> getTableById(String tenantId, String id) {
    var k = "/tenants/" + tenantId + "/_index/tables/by-id/" + id;
    return readEntry(k, TableIndexEntry::parseFrom);
  }

  public List<TableIndexEntry> listTablesByPrefix(String tenantId, NameRef prefix, int limit, String token, StringBuilder nextOut) {
    var pfx = "/tenants/" + tenantId + "/_index/tables/by-name/" + fqPrefix(prefix);
    var keys = ptr.listByPrefix(pfx, Math.max(1, limit), token, nextOut);
    var out = new ArrayList<TableIndexEntry>(keys.size());
    for (var k : keys) {
      readEntry(k, TableIndexEntry::parseFrom).ifPresent(out::add);
    }
    return out;
  }

  private interface Parser<T> { 
    T parse(byte[] bytes) throws Exception; 
  }

  private <T> Optional<T> readEntry(String key, Parser<T> parser) {
    return ptr.get(key).map(p -> {
      var bytes = blobs.get(p.getBlobUri());
      try { 
        return parser.parse(bytes); 
      } catch (Exception e) { 
        throw new RuntimeException(e); 
      }
    });
  }

  private void putCas(String key, String uri, byte[] bytes) {
    blobs.put(uri, bytes, "application/x-protobuf");
    for (int i = 0; i < 10; i++) {
      long expected = ptr.get(key).map(Pointer::getVersion).orElse(0L);
      var next = Pointer.newBuilder().setKey(key).setBlobUri(uri).setVersion(expected + 1).build();
      if (ptr.compareAndSet(key, expected, next)) return;
    }
    throw new IllegalStateException("CAS failed for index key: " + key);
  }

  private static String fqKey(NameRef n) {
    var sb = new StringBuilder(n.getCatalog());
    for (var part : n.getNamespacePathList()) sb.append('/').append(part);
    if (!n.getName().isEmpty()) sb.append('/').append(n.getName());
    return sb.toString();
  }
  
  private static String fqPrefix(NameRef n) {
    var k = fqKey(n);
    return k.endsWith("/") ? k : k + "/";
  }
}