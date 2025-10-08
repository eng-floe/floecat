package ai.floedb.metacat.service.repo.util;

public final class Keys {
  private Keys() {}
  public static String catPtr(String tid, String catId) {
    return "/tenants/" + tid + "/catalogs/by-id/" + catId;
  }
  public static String catBlob(String tid, String catId) {
    return "mem://tenants/" + tid + "/catalogs/" + catId + "/catalog.pb";
  }

  public static String nsPtr(String tid, String catId, String nsId) {
    return "/tenants/" + tid + "/catalogs/" + catId + "/namespaces/by-id/" + nsId;
  }
  public static String nsBlob(String tid, String catId, String nsId) {
    return "mem://tenants/" + tid + "/catalogs/" + catId + "/namespaces/" + nsId + "/namespace.pb";
  }

  public static String tblCanonicalPtr(String tid, String tblId) {
    return "/tenants/" + tid + "/tables/" + tblId;
  }
  public static String tblBlob(String tid, String tblId) {
    return "mem://tenants/" + tid + "/tables/" + tblId + "/table.pb";
  }
  public static String tblIndexPtr(String tid, String catId, String nsId, String tblId) {
    return "/tenants/" + tid + "/catalogs/" + catId + "/namespaces/" + nsId + "/tables/by-id/" + tblId;
  }
  public static String nsIndexPrefix(String tid, String catId, String nsId) {
    return tblIndexPtr(tid, catId, nsId, "");
  }

  public static String snapPtr(String tenantId, String tableId, long snapshotId) {
    return "/tenants/" + tenantId + "/tables/" + tableId + "/snapshots/" + snapshotId;
  }
  public static String snapPrefix(String tenantId, String tableId) {
    return "/tenants/" + tenantId + "/tables/" + tableId + "/snapshots/";
  }
  public static String snapBlob(String tenantId, String tableId, long snapshotId) {
    return "mem://tenants/" + tenantId + "/tables/" + tableId + "/snapshots/" + snapshotId + "/snapshot.pb";
  }

  public static String idxCatByName(String tid, String name) {
    return "/tenants/" + tid + "/_index/catalogs/by-name/" + name;
  }
  public static String idxCatById(String tid, String id) {
    return "/tenants/" + tid + "/_index/catalogs/by-id/" + id;
  }
  public static String idxNsByPath(String tid, String catalogId, String path) {
    return "/tenants/" + tid + "/_index/namespaces/by-path/" + catalogId + "/" + path;
  }
  public static String idxNsById(String tid, String id) {
    return "/tenants/" + tid + "/_index/namespaces/by-id/" + id;
  }
  public static String idxTblByName(String tid, String fq) {
    return "/tenants/" + tid + "/_index/tables/by-name/" + fq;
  }
  public static String idxTblById(String tid, String id) {
    return "/tenants/" + tid + "/_index/tables/by-id/" + id;
  }
}