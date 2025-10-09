package ai.floedb.metacat.service.repo.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;

import ai.floedb.metacat.catalog.rpc.CatalogIndexEntry;
import ai.floedb.metacat.catalog.rpc.NamespaceIndexEntry;
import ai.floedb.metacat.catalog.rpc.NamespaceRef;
import ai.floedb.metacat.catalog.rpc.TableIndexEntry;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.service.repo.util.BaseRepository;
import ai.floedb.metacat.service.repo.util.Keys;
import ai.floedb.metacat.service.storage.BlobStore;
import ai.floedb.metacat.service.storage.PointerStore;

@ApplicationScoped
public class NameIndexRepository extends BaseRepository<byte[]> {
  public NameIndexRepository() {
    super(b -> b, b -> b, "application/x-protobuf");
  }

  public NameIndexRepository(PointerStore ptr, BlobStore blobs) {
    this();
    this.ptr = ptr;
    this.blobs = blobs;
  }

  public void putCatalogIndex(String tenantId, String displayName, ResourceId catalogId) {
    var entry = CatalogIndexEntry.newBuilder().setDisplayName(displayName).setResourceId(catalogId).build();
    var bytes = entry.toByteArray();

    var k1 = Keys.idxCatByName(tenantId, displayName);
    put(k1, BaseRepository.memUriFor(k1, "entry.pb"), bytes);

    var k2 = Keys.idxCatById(tenantId, catalogId.getId());
    put(k2, BaseRepository.memUriFor(k2, "entry.pb"), bytes);
  }

  public Optional<CatalogIndexEntry> getCatalogByName(String tenantId, String displayName) {
    return get(Keys.idxCatByName(tenantId, displayName)).map(bytes -> {
      try { 
        return CatalogIndexEntry.parseFrom(bytes); 
      } catch (Exception e) { 
        throw new RuntimeException(e); 
      }
    });
  }

  public Optional<CatalogIndexEntry> getCatalogById(String tenantId, String id) {
    return get(Keys.idxCatById(tenantId, id)).map(bytes -> {
      try { 
        return CatalogIndexEntry.parseFrom(bytes); 
      } catch (Exception e) { 
        throw new RuntimeException(e); 
      }
    });
  }

  public void putNamespaceIndex(String tenantId, NamespaceRef ref, ResourceId nsId) {
    var entry = NamespaceIndexEntry.newBuilder().setRef(ref).setResourceId(nsId).build();
    var bytes = entry.toByteArray();
    var path  = String.join("/", ref.getNamespacePathList());

    var k1 = Keys.idxNsByPath(tenantId, ref.getCatalogId().getId(), path);
    put(k1, BaseRepository.memUriFor(k1, "entry.pb"), bytes);

    var k2 = Keys.idxNsById(tenantId, nsId.getId());
    put(k2, BaseRepository.memUriFor(k2, "entry.pb"), bytes);
  }

  public Optional<NamespaceIndexEntry> getNamespaceByPath(String tenantId, String catalogId, List<String> path) {
    var k = Keys.idxNsByPath(tenantId, catalogId, String.join("/", path));
    return get(k).map(bytes -> {
      try { 
        return NamespaceIndexEntry.parseFrom(bytes); 
      } catch (Exception e) { 
        throw new RuntimeException(e); 
      }
    });
  }

  public Optional<NamespaceIndexEntry> getNamespaceById(String tenantId, String id) {
    var k = Keys.idxNsById(tenantId, id);
    return get(k).map(bytes -> {
      try { 
        return NamespaceIndexEntry.parseFrom(bytes); 
      } catch (Exception e) { 
        throw new RuntimeException(e); 
      }
    });
  }

  public void putTableIndex(String tenantId, NameRef name, ResourceId tableId) {
    var entry = TableIndexEntry.newBuilder().setFqName(name).setResourceId(tableId).build();
    var bytes = entry.toByteArray();
    var fq = fqKey(name);

    var k1 = Keys.idxTblByName(tenantId, fq);
    put(k1, BaseRepository.memUriFor(k1, "entry.pb"), bytes);

    var k2 = Keys.idxTblById(tenantId, tableId.getId());
    put(k2, BaseRepository.memUriFor(k2, "entry.pb"), bytes);
  }

  public Optional<TableIndexEntry> getTableByName(String tenantId, NameRef name) {
    var k = Keys.idxTblByName(tenantId, fqKey(name));
    return get(k).map(bytes -> {
      try { 
        return TableIndexEntry.parseFrom(bytes); 
      } catch (Exception e) { 
        throw new RuntimeException(e); 
      }
    });
  }
  public Optional<TableIndexEntry> getTableById(String tenantId, String id) {
    var k = Keys.idxTblById(tenantId, id);
    return get(k).map(bytes -> {
      try { 
        return TableIndexEntry.parseFrom(bytes); 
      } catch (Exception e) { 
        throw new RuntimeException(e); 
      }
    });
  }

  public List<TableIndexEntry> listTablesByPrefix(String tenantId, NameRef prefix, int limit, String token, StringBuilder nextOut) {
    var pfx = Keys.idxTblByName(tenantId, fqPrefix(prefix));
    var keys = ptr.listByPrefix(pfx, Math.max(1, limit), token, nextOut);
    var out = new ArrayList<TableIndexEntry>(keys.size());
    for (var k : keys) {
      get(k).ifPresent(bytes -> {
        try { 
          out.add(TableIndexEntry.parseFrom(bytes)); 
        } catch (Exception e) { 
          throw new RuntimeException(e); 
        }
      });
    }
    return out;
  }

  public static String fqKey(NameRef n) {
    var sb = new StringBuilder(n.getCatalog());
    for (var part : n.getNamespacePathList()) sb.append('/').append(part);
    if (!n.getName().isEmpty()) {
      sb.append('/').append(n.getName());
    }
    return sb.toString();
  }

  private static String fqPrefix(NameRef n) {
    var k = fqKey(n);
    return k.endsWith("/") ? k : k + "/";
  }

  // Catalog index
  public boolean deleteCatalogByName(String tenantId, String displayName) {
    String k = Keys.idxCatByName(tenantId, displayName);
    var p = ptr.get(k);
    boolean okPtr = ptr.delete(k);
    boolean okBlob = p.isPresent() && blobs.delete(p.get().getBlobUri());
    return okPtr && okBlob;
  }
  public boolean deleteCatalogById(String tenantId, String catalogId) {
    String k = Keys.idxCatById(tenantId, catalogId);
    var p = ptr.get(k);
    boolean okPtr = ptr.delete(k);
    boolean okBlob = p.isPresent() && blobs.delete(p.get().getBlobUri());
    return okPtr && okBlob;
  }

  // Namespace index
  public boolean deleteNamespaceById(String tenantId, String nsId) {
    String k = Keys.idxNsById(tenantId, nsId);
    var p = ptr.get(k);
    boolean okPtr = ptr.delete(k);
    boolean okBlob = p.isPresent() && blobs.delete(p.get().getBlobUri());
    return okPtr && okBlob;
  }
  public boolean deleteNamespaceByPath(String tenantId, String catalogId, java.util.List<String> path) {
    String k = Keys.idxNsByPath(tenantId, catalogId, String.join("/", path));
    var p = ptr.get(k);
    boolean okPtr = ptr.delete(k);
    boolean okBlob = p.isPresent() && blobs.delete(p.get().getBlobUri());
    return okPtr && okBlob;
  }

  // Table index
  public boolean deleteTableById(String tenantId, String tableId) {
    String k = Keys.idxTblById(tenantId, tableId);
    var p = ptr.get(k);
    boolean okPtr = ptr.delete(k);
    boolean okBlob = p.isPresent() && blobs.delete(p.get().getBlobUri());
    return okPtr && okBlob;
  }
  public boolean deleteTableByName(String tenantId, NameRef name) {
    String k = Keys.idxTblByName(tenantId, fqKey(name));
    var p = ptr.get(k);
    boolean okPtr = ptr.delete(k);
    boolean okBlob = p.isPresent() && blobs.delete(p.get().getBlobUri());
    return okPtr && okBlob;
  }
}