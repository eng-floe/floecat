package ai.floedb.metacat.service.repo.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
public class NameIndexRepository extends BaseRepository<byte[]> {

  protected NameIndexRepository() { super(); }

  @Inject
  public NameIndexRepository(PointerStore ptr, BlobStore blobs) {
    super(ptr, blobs, b -> b, b -> b, "application/x-protobuf");
  }

    public void upsertCatalog(String tenantId, ResourceId catalogId, String displayName) {
    NameRef catalogRef = NameRef.newBuilder()
      .setResourceId(catalogId)
      .setCatalog(displayName)
      .build();
    byte[] bytes = catalogRef.toByteArray();

    String kByName = Keys.idxCatByName(tenantId, displayName);
    String kById = Keys.idxCatById(tenantId, catalogId.getId());
    put(kByName, Keys.memUriFor(kByName, "entry.pb"), bytes);
    put(kById, Keys.memUriFor(kById,   "entry.pb"), bytes);
  }

  public boolean removeCatalog(String tenantId, ResourceId catalogId) {
    var catalogRefOpt = getCatalogById(tenantId, catalogId.getId());
    if (catalogRefOpt.isEmpty()) return false;

    boolean ok1 = deletePtrAndBlob(Keys.idxCatByName(tenantId, catalogRefOpt.get().getCatalog()));
    boolean ok2 = deletePtrAndBlob(Keys.idxCatById(tenantId, catalogId.getId()));
    return ok1 && ok2;
  }

  public void upsertNamespace(String tenantId,
                              ResourceId catalogId,
                              String catalogDisplayName,
                              ResourceId nsId,
                              List<String> parentPathSegments,
                              String nsDisplayName) {
    List<String> parents = parentPathSegments;
    var full = new ArrayList<>(parents);
    if (nsDisplayName != null && !nsDisplayName.isBlank()) {
      full.addAll(List.of(nsDisplayName));
    }

    NameRef nsRef = NameRef.newBuilder()
      .setResourceId(nsId)
      .setCatalog(catalogDisplayName)
      .addAllPath(full)
      .build();
    byte[] bytes = nsRef.toByteArray();

    String kByPath = Keys.idxNsByPath(tenantId, catalogId.getId(), String.join("/", full));
    put(kByPath, Keys.memUriFor(kByPath, "entry.pb"), bytes);

    String kById = Keys.idxNsById(tenantId, nsId.getId());
    put(kById, Keys.memUriFor(kById, "entry.pb"), bytes);

    putNamespaceOwner(tenantId, nsId, catalogId);
  }

  public boolean removeNamespace(String tenantId, ResourceId nsId) {
    var nsRefOpt = getNamespaceById(tenantId, nsId.getId());
    if (nsRefOpt.isEmpty()) return false;

    var catIdOpt = getNamespaceOwner(tenantId, nsId.getId());
    if (catIdOpt.isEmpty()) return false;

    String fullPathKey = Keys.idxNsByPath(
      tenantId, catIdOpt.get().getId(), String.join("/", nsRefOpt.get().getPathList()));

    boolean okId   = deletePtrAndBlob(Keys.idxNsById(tenantId, nsId.getId()));
    boolean okFull = deletePtrAndBlob(fullPathKey);
    boolean okOwn  = deleteNamespaceOwner(tenantId, nsId.getId());

    return okId && okFull && okOwn;
  }

  public void upsertTable(String tenantId,
                          ResourceId tableId,
                          ResourceId namespaceId,
                          String catalogDisplayName,
                          List<String> namespacePathSegments,
                          String tableDisplayName) {
    NameRef tableNameRef = 
      buildTableNameRef(tableId, catalogDisplayName, namespacePathSegments, tableDisplayName);

    byte[] bytes = tableNameRef.toByteArray();

    String kByName = Keys.idxTblByName(tenantId, fqKey(tableNameRef));
    put(kByName, Keys.memUriFor(kByName, "entry.pb"), bytes);

    String kByNamespace = Keys.idxTblByNamespace(tenantId, namespaceId.getId(), tableId.getId());
    put(kByNamespace, Keys.memUriFor(kByNamespace, "entry.pb"), bytes);

    String kById = Keys.idxTblById(tenantId, tableId.getId());
    put(kById, Keys.memUriFor(kById, "entry.pb"), bytes);
  }

  public void upsertTable(String tenantId, TableDescriptor td) {
    String kByNs = Keys.idxTblByNamespace(tenantId, td.getNamespaceId().getId(), td.getResourceId().getId());
    String kById = Keys.idxTblById(tenantId, td.getResourceId().getId());

    var catRef = getCatalogById(tenantId, td.getCatalogId().getId()).orElse(null);
    var nsRef  = getNamespaceById(tenantId, td.getNamespaceId().getId()).orElse(null);

    var prevFullOpt = getTableById(tenantId, td.getResourceId().getId());

    if (catRef != null && nsRef != null) {
      var nsFullPath = nsRef.getPathList();
      NameRef newFull = 
        buildTableNameRef(td.getResourceId(), catRef.getCatalog(), nsFullPath, td.getDisplayName());
      byte[] fullBytes = newFull.toByteArray();

      String kByName = Keys.idxTblByName(tenantId, fqKey(newFull));
      put(kByName, Keys.memUriFor(kByName, "entry.pb"), fullBytes);
      put(kByNs, Keys.memUriFor(kByNs, "entry.pb"), fullBytes);
      put(kById, Keys.memUriFor(kById, "entry.pb"), fullBytes);

      if (prevFullOpt.isPresent() && !prevFullOpt.get().getName().isBlank()) {
        String prevFq = fqKey(prevFullOpt.get());
        String newFq  = fqKey(newFull);
        if (!prevFq.equals(newFq)) {
          deletePtrAndBlob(Keys.idxTblByName(tenantId, prevFq));
        }
      }
    } else {
      byte[] minimal = NameRef.newBuilder().setResourceId(td.getResourceId()).build().toByteArray();
      put(kByNs, Keys.memUriFor(kByNs, "entry.pb"), minimal);
      put(kById, Keys.memUriFor(kById, "entry.pb"), minimal);
    }
  }

  private static NameRef buildTableNameRef(ResourceId tableId,
                                          String catalogDisplayName,
                                          List<String> namespaceFullPath,
                                          String tableDisplayName) {
    return NameRef.newBuilder()
      .setResourceId(tableId)
      .setCatalog(catalogDisplayName)
      .addAllPath(namespaceFullPath == null ? java.util.List.of() : namespaceFullPath)
      .setName(tableDisplayName)
      .build();
  }

  public boolean removeTable(String tenantId, TableDescriptor td) {
    var prev = getTableById(tenantId, td.getResourceId().getId()).orElse(null);
    if (prev != null && !prev.getName().isBlank()) {
      deletePtrAndBlob(Keys.idxTblByName(tenantId, fqKey(prev)));
    }
    boolean okId = deletePtrAndBlob(Keys.idxTblById(tenantId, td.getResourceId().getId()));
    boolean okNs = deletePtrAndBlob(Keys.idxTblByNamespace(
      tenantId, td.getNamespaceId().getId(), td.getResourceId().getId()));
    return okId && okNs;
  }

  public void putNamespaceOwner(String tenantId, ResourceId namespaceId, ResourceId catalogId) {
    String k = Keys.idxNsOwnerById(tenantId, namespaceId.getId());
    put(k, Keys.memUriFor(k, "entry.pb"), catalogId.toByteArray());
  }

  public Optional<ResourceId> getNamespaceOwner(String tenantId, String nsId) {
    String k = Keys.idxNsOwnerById(tenantId, nsId);
    return get(k).map(bytes -> {
      try { return ResourceId.parseFrom(bytes); }
      catch (Exception e) { throw new RuntimeException(e); }
    });
  }

  public boolean deleteNamespaceOwner(String tenantId, String nsId) {
    return deletePtrAndBlob(Keys.idxNsOwnerById(tenantId, nsId));
  }

  private boolean deletePtrAndBlob(String key) {
    var p = ptr.get(key);
    boolean okPtr = ptr.delete(key);
    boolean okBlob = p.isPresent() && blobs.delete(p.get().getBlobUri());
    return okPtr && okBlob;
  }

  private Optional<NameRef> safeParseNameRef(byte[] bytes, String what, String idOrName) {
    if (bytes == null || bytes.length == 0) return Optional.empty();
    try {
      var ref = NameRef.parseFrom(bytes);
      if (!ref.hasResourceId()) {
        return Optional.empty();
      }
      return Optional.of(ref);
    } catch (Exception e) {
      return Optional.empty();
    }
  }
  
  public Optional<NameRef> getCatalogByName(String tenantId, String displayName) {
    return get(Keys.idxCatByName(tenantId, displayName))
      .flatMap(bytes -> safeParseNameRef(bytes, "catalog", displayName));
  }

  public Optional<NameRef> getCatalogById(String tenantId, String id) {
    return get(Keys.idxCatById(tenantId, id))
      .flatMap(bytes -> safeParseNameRef(bytes, "catalog id", id));
  }

  public Optional<NameRef> getNamespaceByPath(String tenantId, String catalogId, List<String> path) {
    List<String> normalized = path;//(path);
    String k = Keys.idxNsByPath(tenantId, catalogId, String.join("/", normalized));
    return get(k).map(bytes -> {
      try { return NameRef.parseFrom(bytes); }
      catch (Exception e) { throw new RuntimeException(e); }
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
    List<PointerStore.Row> rows = ptr.listPointersByPrefix(pfx, Math.max(1, limit), token, nextOut);

    List<String> uris = new ArrayList<>(rows.size());
    for (PointerStore.Row r : rows) uris.add(r.blobUri());
    Map<String, byte[]> blobMap = blobs.getBatch(uris);

    List<NameRef> out = new ArrayList<>(rows.size());
    for (PointerStore.Row r : rows) {
      byte[] bytes = blobMap.get(r.blobUri());
      if (bytes == null) continue;
      try {
        out.add(NameRef.parseFrom(bytes));
      } catch (Exception e) {
        throw new RuntimeException("Failed to parse NameRef for uri=" + r.blobUri() + ", key=" + r.key(), e);
      }
    }
    return out;
  }

  public List<NameRef> listTablesByPrefix(String tenantId, String prefix, int limit, String token, StringBuilder nextOut) {
    List<PointerStore.Row> rows = ptr.listPointersByPrefix(prefix, Math.max(1, limit), token, nextOut);

    List<String> uris = new ArrayList<>(rows.size());
    for (PointerStore.Row r : rows) uris.add(r.blobUri());
    Map<String, byte[]> blobMap = blobs.getBatch(uris);

    List<NameRef> out = new ArrayList<>(rows.size());
    for (PointerStore.Row r : rows) {
      byte[] bytes = blobMap.get(r.blobUri());
      if (bytes == null) continue;
      try {
        out.add(NameRef.parseFrom(bytes));
      } catch (Exception e) {
        throw new RuntimeException("Failed to parse NameRef for uri=" + r.blobUri() + ", key=" + r.key(), e);
      }
    }
    return out;
  }

  public boolean deleteTableByNamespace(String tenantId, TableDescriptor descriptor) {
    String k = Keys.idxTblByNamespace(
      tenantId,
      descriptor.getNamespaceId().getId(),
      descriptor.getResourceId().getId());
    var p = ptr.get(k);
    boolean okPtr = ptr.delete(k);
    boolean okBlob = p.isPresent() && blobs.delete(p.get().getBlobUri());
    return okPtr && okBlob;
  }

  private static String fqKey(NameRef n) {
    var sb = new StringBuilder(n.getCatalog());
    for (var part : n.getPathList()) sb.append('/').append(part);
    if (!n.getName().isEmpty()) {
      sb.append('/').append(n.getName());
    }
    return sb.toString();
  }

  private static String fqPrefix(NameRef n) {
    var k = fqKey(n);
    return k.endsWith("/") ? k : k + "/";
  }

  public static String joinPath(List<String> parts) {
    return String.join("/", parts);
  }
}
