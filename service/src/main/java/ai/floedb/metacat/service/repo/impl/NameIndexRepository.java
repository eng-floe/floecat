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
    put(kById, Keys.memUriFor(kById, "entry.pb"), bytes);
  }

  public boolean removeCatalog(String tenantId, ResourceId catalogId) {
    var catalogRefOpt = getCatalogById(tenantId, catalogId.getId());
    if (catalogRefOpt.isEmpty()) {
      return false;
    }

    boolean ok1 = deletePtrAndBlob(Keys.idxCatByName(tenantId, catalogRefOpt.get().getCatalog()));
    boolean ok2 = deletePtrAndBlob(Keys.idxCatById(tenantId, catalogId.getId()));
    return ok1 && ok2;
  }

  public void upsertNamespace(
      String tenantId,
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

    boolean okId = deletePtrAndBlob(Keys.idxNsById(tenantId, nsId.getId()));
    boolean okFull = deletePtrAndBlob(fullPathKey);
    boolean okOwn = deletePtrAndBlob(Keys.idxNsOwnerById(tenantId, nsId.getId()));

    return okId && okFull && okOwn;
  }

  public void upsertTable(String tenantId, TableDescriptor td) {
    var catRef = getCatalogById(tenantId, td.getCatalogId().getId()).orElse(null);
    var nsRef = getNamespaceById(tenantId, td.getNamespaceId().getId()).orElse(null);

    var prevFullOpt = getTableById(tenantId, td.getResourceId().getId());

    String kById = Keys.idxTblById(tenantId, td.getResourceId().getId());
    String kByLeaf = Keys.idxTblByNamespaceLeaf(tenantId, td.getNamespaceId().getId(), td.getDisplayName());

    if (catRef != null && nsRef != null) {
      var nsPath = nsRef.getPathList();
      var full = NameRef.newBuilder()
          .setResourceId(td.getResourceId())
          .setCatalog(catRef.getCatalog())
          .addAllPath(nsPath)
          .setName(td.getDisplayName())
          .build();
      byte[] bytesFull = full.toByteArray();

      put(kById, Keys.memUriFor(kById, "entry.pb"), bytesFull);
      put(kByLeaf, Keys.memUriFor(kByLeaf, "entry.pb"), bytesFull);
      String kByName = Keys.idxTblByName(tenantId, td.getCatalogId().getId(), nsPath, td.getDisplayName());
      put(kByName, Keys.memUriFor(kByName, "entry.pb"), bytesFull);

      if (prevFullOpt.isPresent()) {
        var prev = prevFullOpt.get();
        if (!prev.getName().isBlank()) {
          String prevKByName = Keys.idxTblByName(tenantId, td.getCatalogId().getId(), prev.getPathList(), prev.getName());
          if (!prevKByName.equals(kByName)) deletePtrAndBlob(prevKByName);

          String prevKByLeaf = Keys.idxTblByNamespaceLeaf(tenantId, td.getNamespaceId().getId(), prev.getName());
          if (!prevKByLeaf.equals(kByLeaf)) deletePtrAndBlob(prevKByLeaf);
        }
      }
    }
  }

  public boolean removeTable(String tenantId, TableDescriptor td) {
    var prev = getTableById(tenantId, td.getResourceId().getId()).orElse(null);
    if (prev != null && !prev.getName().isBlank()) {
      String kByName = Keys.idxTblByName(tenantId, td.getCatalogId().getId(),
          prev.getPathList(), prev.getName());
      deletePtrAndBlob(kByName);
      String kByLeafPrev = Keys.idxTblByNamespaceLeaf(
          tenantId, td.getNamespaceId().getId(), prev.getName());
      deletePtrAndBlob(kByLeafPrev);
    } else {
      String kByLeafCur = Keys.idxTblByNamespaceLeaf(
          tenantId, td.getNamespaceId().getId(), td.getDisplayName());
      deletePtrAndBlob(kByLeafCur);
    }
    boolean okId = deletePtrAndBlob(Keys.idxTblById(tenantId, td.getResourceId().getId()));
    return okId;
  }

  public Optional<NameRef> getTableByName(String tenantId, NameRef name) {
    var cat = getCatalogByName(tenantId, name.getCatalog()).orElse(null);
    if (cat == null) return Optional.empty();
    String k = Keys.idxTblByName(tenantId, cat.getResourceId().getId(), name.getPathList(), name.getName());
    return get(k).map(bytes -> {
      try { 
        return NameRef.parseFrom(bytes); 
      }
      catch (Exception e) { 
        throw new RuntimeException(e); 
      }
    });
  }

  public void putNamespaceOwner(String tenantId, ResourceId namespaceId, ResourceId catalogId) {
    String k = Keys.idxNsOwnerById(tenantId, namespaceId.getId());
    put(k, Keys.memUriFor(k, "entry.pb"), catalogId.toByteArray());
  }

  public Optional<ResourceId> getNamespaceOwner(String tenantId, String nsId) {
    String k = Keys.idxNsOwnerById(tenantId, nsId);
    return get(k).map(bytes -> {
      try { 
        return ResourceId.parseFrom(bytes); 
      }
      catch (Exception e) { 
        throw new RuntimeException(e); 
      }
    });
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
    List<String> normalized = path;
    String k = Keys.idxNsByPath(tenantId, catalogId, String.join("/", normalized));
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
    var cat = getCatalogByName(tenantId, prefix.getCatalog()).orElse(null);
    if (cat == null) return List.of();
    String pfx = Keys.idxTblByNamePrefix(tenantId, cat.getResourceId().getId(), prefix.getPathList());
    return listTablesByPrefix(tenantId, pfx, limit, token, nextOut);
  }

  public List<NameRef> listTablesByPrefix(
      String tenantId,
      String prefix,
      int limit,
      String token,
      StringBuilder nextOut) {
    List<PointerStore.Row> rows = ptr.listPointersByPrefix(
        prefix, Math.max(1, limit), token, nextOut);

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
        throw new RuntimeException(
            "Failed to parse NameRef for uri=" + r.blobUri() + ", key=" + r.key(), e);
      }
    }
    return out;
  }

  public static String joinPath(List<String> parts) {
    return String.join("/", parts);
  }
}
