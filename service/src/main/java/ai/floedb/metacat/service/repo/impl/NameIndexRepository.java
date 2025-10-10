package ai.floedb.metacat.service.repo.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;

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

  public void putCatalogIndex(String tenantId, NameRef catalogRef, ResourceId catId) {
    byte[] bytes = catalogRef.toByteArray();

    String kByName = Keys.idxCatByName(tenantId, catalogRef.getCatalog());
    String kById = Keys.idxCatById(tenantId, catId.getId());

    put(kByName, BaseRepository.memUriFor(kByName, "entry.pb"), bytes);
    put(kById, BaseRepository.memUriFor(kById, "entry.pb"), bytes);
  }

  public Optional<NameRef> getCatalogByName(String tenantId, String displayName) {
    return get(Keys.idxCatByName(tenantId, displayName)).map(bytes -> {
      try {
        NameRef ref = NameRef.parseFrom(bytes);
        if (!ref.hasResourceId()) {
          throw new IllegalStateException("Stored NameRef missing resource_id for catalog=" + displayName);
        }
        return ref;
      } catch (Exception e) {
        throw new RuntimeException("Failed to parse NameRef for catalog=" + displayName, e);
      }
    });
  }

  public Optional<NameRef> getCatalogById(String tenantId, String id) {
    return get(Keys.idxCatById(tenantId, id)).map(bytes -> {
      try {
        NameRef ref = NameRef.parseFrom(bytes);
        if (!ref.hasResourceId()) {
          throw new IllegalStateException("Stored NameRef missing resource_id for catalog id=" + id);
        }
        return ref;
      } catch (Exception e) {
        throw new RuntimeException("Failed to parse NameRef for catalog id=" + id, e);
      }
    });
  }

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

  public void putNamespaceIndex(String tenantId, NameRef ref) {
    Objects.requireNonNull(tenantId, "tenantId");
    Objects.requireNonNull(ref, "ref");
    Objects.requireNonNull(ref.getResourceId(), "nsId");

    NameRef stored = NameRef.newBuilder(ref).setResourceId(ref.getResourceId()).build();
    byte[] bytes = stored.toByteArray();

    ResourceId catId = requireCatalogIdByName(tenantId, ref.getCatalog());
    String path = joinPath(ref.getNamespacePathList());

    String kByPath = Keys.idxNsByPath(tenantId, catId.getId(), path);
    String kById = Keys.idxNsById(tenantId, ref.getResourceId().getId());

    put(kByPath, BaseRepository.memUriFor(kByPath, "entry.pb"), bytes);
    put(kById, BaseRepository.memUriFor(kById, "entry.pb"), bytes);
  }

  public Optional<NameRef> getNamespaceByPath(String tenantId, String catalogId, List<String> path) {
    String k = Keys.idxNsByPath(tenantId, catalogId, joinPath(path));
    return get(k).map(bytes -> {
      try { 
        return NameRef.parseFrom(bytes); 
      }
      catch (Exception e) { 
        throw new RuntimeException(e); 
      }
    });
  }

  public Optional<NameRef> getNamespaceById(String tenantId, String id) {
    String k = Keys.idxNsById(tenantId, id);
    return get(k).map(bytes -> {
      try { 
        return NameRef.parseFrom(bytes); 
      }
      catch (Exception e) { 
        throw new RuntimeException(e); 
      }
    });
  }

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

  public void putTableIndex(String tenantId, NameRef name, ResourceId tableId) {
    Objects.requireNonNull(tenantId, "tenantId");
    Objects.requireNonNull(name, "name");
    Objects.requireNonNull(tableId, "tableId");

    NameRef stored = NameRef.newBuilder(name).setResourceId(tableId).build();
    byte[] bytes = stored.toByteArray();

    String fq = fqKey(name);

    String kByName = Keys.idxTblByName(tenantId, fq);
    put(kByName, BaseRepository.memUriFor(kByName, "entry.pb"), bytes);

    String kById = Keys.idxTblById(tenantId, tableId.getId());
    put(kById, BaseRepository.memUriFor(kById, "entry.pb"), bytes);
  }

  public Optional<NameRef> getTableByName(String tenantId, NameRef name) {
    String k = Keys.idxTblByName(tenantId, fqKey(name));
    return get(k).map(bytes -> {
      try { 
        return NameRef.parseFrom(bytes); 
      }
      catch (Exception e) { 
        throw new RuntimeException(e); 
      }
    });
  }

  public Optional<NameRef> getTableById(String tenantId, String id) {
    String k = Keys.idxTblById(tenantId, id);
    return get(k).map(bytes -> {
      try { 
        return NameRef.parseFrom(bytes); 
      }
      catch (Exception e) { 
        throw new RuntimeException(e); 
      }
    });
  }

  public List<NameRef> listTablesByPrefix(String tenantId, NameRef prefix, int limit, String token, StringBuilder nextOut) {
    String pfx = Keys.idxTblByName(tenantId, fqPrefix(prefix));
    List<String> keys = ptr.listByPrefix(pfx, Math.max(1, limit), token, nextOut);

    List<NameRef> out = new ArrayList<>(keys.size());
    for (String k : keys) {
      get(k).ifPresent(bytes -> {
        try { 
          out.add(NameRef.parseFrom(bytes)); 
        }
        catch (Exception e) { 
          throw new RuntimeException(e); 
        }
      });
    }
    return out;
  }

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

  private static String fqKey(NameRef n) {
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

  private ResourceId requireCatalogIdByName(String tenantId, String catalogName) {
    return get(Keys.idxCatByName(tenantId, catalogName))
      .map(bytes -> {
        try {
          NameRef ref = NameRef.parseFrom(bytes);
          if (!ref.hasResourceId()) {
            throw new IllegalStateException("Catalog NameRef missing resource_id: " + catalogName);
          }
          return ref.getResourceId();
        } catch (Exception e) {
          throw new RuntimeException("Failed to parse catalog NameRef for: " + catalogName, e);
        }
      })
      .orElseThrow(() -> new IllegalArgumentException("Unknown catalog: " + catalogName));
  }

  private static String joinPath(List<String> parts) {
    return String.join("/", parts);
  }
}