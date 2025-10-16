package ai.floedb.metacat.service.repo.util;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

public final class Keys {
  private Keys() {}

  private static String enc(String s) { return URLEncoder.encode(s, StandardCharsets.UTF_8); }
  private static String normTenant(String tid) { return tid.toLowerCase(); }

  // ========= Catalog (canonical + by-name) =========
  public static String catPtr(String tid, String catId) {
    return "/tenants/" + normTenant(tid) + "/catalogs/by-id/" + enc(catId);
  }
  public static String catByNamePtr(String tid, String displayName) {
    return "/tenants/" + normTenant(tid) + "/catalogs/by-name/" + enc(displayName);
  }
  public static String catByNamePrefix(String tid) {
    return "/tenants/" + normTenant(tid) + "/catalogs/by-name/";
  }
  public static String catBlob(String tid, String catId) {
    return "mem://tenants/" + normTenant(tid) + "/catalogs/" + enc(catId) + "/catalog.pb";
  }

  // ========= Namespace (canonical under catalog + by-path) =========
  public static String nsPtr(String tid, String catId, String nsId) {
    return "/tenants/" + normTenant(tid) + "/catalogs/" + enc(catId) + "/namespaces/by-id/" + enc(nsId);
  }
  public static String nsByPathPtr(String tid, String catId, List<String> fullPath) {
    String joined = String.join("/", fullPath.stream().map(Keys::enc).toArray(String[]::new));
    return "/tenants/" + normTenant(tid) + "/catalogs/" + enc(catId) + "/namespaces/by-path/" + joined;
  }
  public static String nsByPathPrefix(String tid, String catId, List<String> parentsOrEmpty) {
    String joined = String.join("/", parentsOrEmpty.stream().map(Keys::enc).toArray(String[]::new));
    return "/tenants/" + normTenant(tid) + "/catalogs/" + enc(catId) + "/namespaces/by-path/"
        + (joined.isEmpty() ? "" : joined + "/");
  }
  public static String nsBlob(String tid, String catId, String nsId) {
    return "mem://tenants/" + normTenant(tid) + "/catalogs/" + enc(catId) + "/namespaces/"
        + enc(nsId) + "/namespace.pb";
  }

  // ========= Table (canonical + by-name) =========
  public static String tblCanonicalPtr(String tid, String tblId) {
    return "/tenants/" + normTenant(tid) + "/tables/" + enc(tblId);
  }
  public static String tblByNamePtr(String tid, String catId, String nsId, String leaf) {
    return "/tenants/" + normTenant(tid) + "/catalogs/" + enc(catId)
        + "/namespaces/" + enc(nsId) + "/tables/by-name/" + enc(leaf);
  }
  public static String tblByNamePrefix(String tid, String catId, String nsId) {
    return "/tenants/" + normTenant(tid) + "/catalogs/" + enc(catId)
        + "/namespaces/" + enc(nsId) + "/tables/by-name/";
  }
  public static String tblBlob(String tid, String tblId) {
    return "mem://tenants/" + normTenant(tid) + "/tables/" + enc(tblId) + "/table.pb";
  }

  // ========= Snapshots =========
  public static String snapPtr(String tid, String tblId, long snapshotId) {
    return "/tenants/" + normTenant(tid) + "/tables/" + enc(tblId) + "/snapshots/" + snapshotId;
  }
  public static String snapPtr(String tid, String tblId) {
    return "/tenants/" + normTenant(tid) + "/tables/" + enc(tblId) + "/snapshots/";
  }
  public static String snapBlob(String tid, String tblId, long snapshotId) {
    return "mem://tenants/" + normTenant(tid) + "/tables/" + enc(tblId)
        + "/snapshots/" + snapshotId + "/snapshot.pb";
  }

  // ========= Idempotency key =========

  public static String idemKey(String tid, String op, String key) {
    return "/tenants/" + normTenant(tid) + "/idempotency/" + enc(op) + "/" + enc(key);
  }

  // ========= Helpers =========
  public static String memUriFor(String pointerKey, String leaf) {
    String base = pointerKey.startsWith("/") ? pointerKey.substring(1) : pointerKey;
    return "mem://" + base + (leaf.startsWith("/") ? leaf : "/" + leaf);
  }
}
