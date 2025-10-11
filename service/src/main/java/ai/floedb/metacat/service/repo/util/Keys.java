package ai.floedb.metacat.service.repo.util;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public final class Keys {
  private Keys() {}

  private static String enc(String s) {
    return URLEncoder.encode(s, StandardCharsets.UTF_8);
  }
  private static String normTenant(String tid) { return tid.toLowerCase(); }

  public static String catPtr(String tid, String catId) {
    return "/tenants/" + normTenant(tid) + "/catalogs/by-id/" + enc(catId);
  }
  public static String catBlob(String tid, String catId) {
    return "mem://tenants/" + normTenant(tid) + "/catalogs/" + enc(catId) + "/catalog.pb";
  }

  public static String nsPtr(String tid, String catId, String nsId) {
    return "/tenants/" + normTenant(tid) + "/catalogs/" + enc(catId) + "/namespaces/by-id/" + enc(nsId);
  }
  public static String nsBlob(String tid, String catId, String nsId) {
    return "mem://tenants/" + normTenant(tid) + "/catalogs/" + enc(catId) + "/namespaces/" + enc(nsId) + "/namespace.pb";
  }

  public static String tblCanonicalPtr(String tid, String tblId) {
    return "/tenants/" + normTenant(tid) + "/tables/" + enc(tblId);
  }
  public static String tblBlob(String tid, String tblId) {
    return "mem://tenants/" + normTenant(tid) + "/tables/" + enc(tblId) + "/table.pb";
  }
  public static String tblIndexPtr(String tid, String catId, String nsId, String tblId) {
    return "/tenants/" + normTenant(tid) + "/catalogs/" + enc(catId) + "/namespaces/" + enc(nsId) + "/tables/by-id/" + enc(tblId);
  }
  public static String nsIndexPrefix(String tid, String catId, String nsId) {
    return tblIndexPtr(tid, catId, nsId, "");
  }

  public static String snapPtr(String tid, String tblId, long snapshotId) {
    return "/tenants/" + normTenant(tid) + "/tables/" + enc(tblId) + "/snapshots/" + snapshotId;
  }
  public static String snapPrefix(String tid, String tblId) {
    return "/tenants/" + normTenant(tid) + "/tables/" + enc(tblId) + "/snapshots/";
  }
  public static String snapBlob(String tid, String tblId, long snapshotId) {
    return "mem://tenants/" + normTenant(tid) + "/tables/" + enc(tblId) + "/snapshots/" + snapshotId + "/snapshot.pb";
  }

  public static String idxCatByName(String tid, String name) {
    return "/tenants/" + normTenant(tid) + "/_index/catalogs/by-name/" + enc(name);
  }
  public static String idxCatById(String tid, String id) {
    return "/tenants/" + normTenant(tid) + "/_index/catalogs/by-id/" + enc(id);
  }
  public static String idxNsByPath(String tid, String catalogId, String path) {
    String[] parts = path.split("/");
    String joined = String.join("/", Arrays.stream(parts)
      .map(Keys::enc)
      .toArray(String[]::new));
    return "/tenants/" + normTenant(tid) + "/_index/namespaces/by-path/" + enc(catalogId) + "/" + joined;
  }
  public static String idxNsById(String tid, String id) {
    return "/tenants/" + normTenant(tid) + "/_index/namespaces/by-id/" + enc(id);
  }
  public static String idxTblByName(String tid, String fq) {
    String[] parts = fq.split("/");
    String joined = String.join("/", Arrays.stream(parts)
      .map(Keys::enc)
      .toArray(String[]::new));
    return "/tenants/" + normTenant(tid) + "/_index/tables/by-name/" + joined;
  }
  public static String idxTblById(String tid, String id) {
    return "/tenants/" + normTenant(tid) + "/_index/tables/by-id/" + enc(id);
  }

  public static String memUriFor(String pointerKey, String leaf) {
    String base = pointerKey.startsWith("/") ? pointerKey.substring(1) : pointerKey;
    return "mem://" + base + (leaf.startsWith("/") ? leaf : "/" + leaf);
  }
}
