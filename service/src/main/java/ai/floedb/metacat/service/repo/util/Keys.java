package ai.floedb.metacat.service.repo.util;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

public final class Keys {

  private static String enc(String s) {
    return URLEncoder.encode(s, StandardCharsets.UTF_8);
  }

  // ========= Tenant =========
  public static String tenPtr(String tid) {
    return "/tenants/" + enc(tid);
  }

  public static String tenByNamePtr(String displayName) {
    return "/tenants/by-name/" + enc(displayName);
  }

  public static String tenByNamePrefix() {
    return "/tenants/by-name/";
  }

  public static String tenBlob(String tid) {
    return "mem://tenants/" + enc(tid) + "/tenent.pb";
  }

  // ========= Catalog =========
  public static String catPtr(String tid, String catId) {
    return "/tenants/" + enc(tid) + "/catalogs/by-id/" + enc(catId);
  }

  public static String catByNamePtr(String tid, String displayName) {
    return "/tenants/" + enc(tid) + "/catalogs/by-name/" + enc(displayName);
  }

  public static String catByNamePrefix(String tid) {
    return "/tenants/" + enc(tid) + "/catalogs/by-name/";
  }

  public static String catBlob(String tid, String catId) {
    return "mem://tenants/" + enc(tid) + "/catalogs/" + enc(catId) + "/catalog.pb";
  }

  // ========= Namespace =========
  public static String nsPtr(String tid, String catId, String nsId) {
    return "/tenants/" + enc(tid) + "/catalogs/" + enc(catId) + "/namespaces/by-id/" + enc(nsId);
  }

  public static String nsByPathPtr(String tid, String catId, List<String> fullPath) {
    String joined = String.join("/", fullPath.stream().map(Keys::enc).toArray(String[]::new));
    return "/tenants/" + enc(tid) + "/catalogs/" + enc(catId) + "/namespaces/by-path/" + joined;
  }

  public static String nsByPathPrefix(String tid, String catId, List<String> parentsOrEmpty) {
    String joined = String.join("/", parentsOrEmpty.stream().map(Keys::enc).toArray(String[]::new));
    return "/tenants/"
        + enc(tid)
        + "/catalogs/"
        + enc(catId)
        + "/namespaces/by-path/"
        + (joined.isEmpty() ? "" : joined + "/");
  }

  public static String nsBlob(String tid, String nsId) {
    return "mem://tenants/" + enc(tid) + "/namespaces/" + enc(nsId) + "/namespace.pb";
  }

  // ========= Table =========
  public static String tblByIdPtr(String tid, String tblId) {
    return "/tenants/" + enc(tid) + "/tables/by-id/" + enc(tblId);
  }

  public static String tblByNamePtr(String tid, String catId, String nsId, String leaf) {
    return "/tenants/"
        + enc(tid)
        + "/catalogs/"
        + enc(catId)
        + "/namespaces/"
        + enc(nsId)
        + "/tables/by-name/"
        + enc(leaf);
  }

  public static String tblByNamePrefix(String tid, String catId, String nsId) {
    return "/tenants/"
        + enc(tid)
        + "/catalogs/"
        + enc(catId)
        + "/namespaces/"
        + enc(nsId)
        + "/tables/by-name/";
  }

  public static String tblBlob(String tid, String tblId) {
    return "mem://tenants/" + enc(tid) + "/tables/" + enc(tblId) + "/table.pb";
  }

  // ========= Snapshots =========
  public static String snapPtrById(String tid, String tblId, long sid) {
    return String.format("/tenants/%s/tables/%s/snapshots/by-id/%019d", enc(tid), enc(tblId), sid);
  }

  public static String snapPtrByIdPrefix(String tid, String tblId) {
    return String.format("/tenants/%s/tables/%s/snapshots/by-id/", enc(tid), enc(tblId));
  }

  public static String snapPtrByTime(String tid, String tblId, long sid, long upstreamCreatedAtMs) {
    long inv = Long.MAX_VALUE - upstreamCreatedAtMs;
    return String.format(
        "/tenants/%s/tables/%s/snapshots/by-time/%019d-%019d", enc(tid), enc(tblId), inv, sid);
  }

  public static String snapPtrByTimePrefix(String tid, String tblId) {
    return String.format("/tenants/%s/tables/%s/snapshots/by-time/", enc(tid), enc(tblId));
  }

  public static String snapBlob(String tid, String tblId, long sid) {
    return String.format(
        "mem://tenants/%s/tables/%s/snapshots/%019d/snapshot.pb", enc(tid), enc(tblId), sid);
  }

  // ========= Stats =========
  public static String snapStatsRoot(String tid, String tblId, long snapId) {
    return String.format("/tenants/%s/tables/%s/snapshots/%d/stats/", enc(tid), enc(tblId), snapId);
  }

  public static String snapTableStatsPtr(String tid, String tblId, long snapId) {
    return snapStatsRoot(tid, tblId, snapId) + "table";
  }

  public static String snapColStatsDir(String tid, String tblId, long snapId) {
    return snapStatsRoot(tid, tblId, snapId) + "columns/";
  }

  public static String snapColStatsPtr(String tid, String tblId, long snapId, String colId) {
    return snapColStatsDir(tid, tblId, snapId) + enc(colId);
  }

  public static String snapTableStatsBlob(String tid, String tblId, long snapId) {
    return String.format(
        "mem://tenants/%s/tables/%s/snapshots/%d/stats/table.pb", enc(tid), enc(tblId), snapId);
  }

  public static String snapColStatsBlob(String tid, String tblId, long snapId, String colId) {
    return String.format(
        "mem://tenants/%s/tables/%s/snapshots/%d/stats/columns/%s/column.pb",
        enc(tid), enc(tblId), snapId, enc(colId));
  }

  public static String snapStatsPrefix(String tid, String tblId, long snapId) {
    return snapStatsRoot(tid, tblId, snapId);
  }

  public static String snapColStatsPrefix(String tid, String tblId, long snapId) {
    return snapColStatsDir(tid, tblId, snapId);
  }

  // ========= Connectors =========
  public static String connByIdPtr(String tenantId, String connectorId) {
    return "/tenants/" + tenantId + "/connectors/by-id/" + enc(connectorId);
  }

  public static String connByNamePtr(String tenantId, String displayName) {
    return "/tenants/" + tenantId + "/connectors/by-name/" + enc(displayName);
  }

  public static String connByNamePrefix(String tenantId) {
    return "/tenants/" + tenantId + "/connectors/by-name/";
  }

  public static String connBlob(String tenantId, String connectorId) {
    return "tenants/" + tenantId + "/connectors/" + enc(connectorId) + "/connector.pb";
  }

  // ========= Idempotency key =========
  public static String idemKey(String tid, String op, String key) {
    return "/tenants/" + enc(tid) + "/idempotency/" + enc(op) + "/" + enc(key);
  }

  // ========= Helpers =========
  public static String memUriFor(String pointerKey, String leaf) {
    String base = pointerKey.startsWith("/") ? pointerKey.substring(1) : pointerKey;
    return "mem://" + base + (leaf.startsWith("/") ? leaf : "/" + leaf);
  }
}
