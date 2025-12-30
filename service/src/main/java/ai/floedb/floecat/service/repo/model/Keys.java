package ai.floedb.floecat.service.repo.model;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

public final class Keys {

  private static String req(String name, String v) {
    if (v == null || v.isBlank()) {
      throw new IllegalArgumentException("key arg '" + name + "' is null/blank");
    }
    return v;
  }

  private static long reqNonNegative(String name, long v) {
    if (v < 0) {
      throw new IllegalArgumentException("key arg '" + name + "' must be >= 0");
    }
    return v;
  }

  private static List<String> reqPath(String name, List<String> segs) {
    if (segs == null || segs.isEmpty()) {
      throw new IllegalArgumentException("key arg '" + name + "' is null/empty");
    }
    for (int i = 0; i < segs.size(); i++) {
      var s = segs.get(i);
      if (s == null || s.isBlank()) {
        throw new IllegalArgumentException(
            "key path '" + name + "' segment[" + i + "] is null/blank");
      }
    }
    return segs;
  }

  private static String encode(String s) {
    return URLEncoder.encode(Objects.requireNonNull(s, "encode value"), StandardCharsets.UTF_8);
  }

  private static String joinPathSegments(List<String> segments) {
    if (segments == null) {
      throw new IllegalArgumentException("key arg 'segments' is null; use List.of()");
    }
    if (segments.isEmpty()) {
      return "";
    }
    String[] enc = new String[segments.size()];
    for (int i = 0; i < segments.size(); i++) {
      enc[i] = encode(req("segments[" + i + "]", segments.get(i)));
    }
    return String.join("/", enc);
  }

  private static String normalizeColumnId(int columnId) {
    return String.format("%019d", columnId);
  }

  // ===== Account =====

  public static String accountRootPointer(String accountId) {
    String tid = req("account_id", accountId);
    return "/accounts/" + encode(tid);
  }

  public static String accountPointerById(String accountId) {
    String tid = req("account_id", accountId);
    return "/accounts/by-id/" + encode(tid);
  }

  public static String accountPointerByName(String displayName) {
    String name = req("display_name", displayName);
    return "/accounts/by-name/" + encode(name);
  }

  public static String accountPointerByNamePrefix() {
    return "/accounts/by-name/";
  }

  public static String accountBlobUri(String accountId, String sha256) {
    String tid = req("account_id", accountId);
    String sha = req("sha256", sha256);
    return String.format("/accounts/%s/account/%s.pb", encode(tid), encode(sha));
  }

  // ===== Catalog =====

  public static String catalogPointerById(String accountId, String catalogId) {
    String tid = req("account_id", accountId);
    String cid = req("catalog_id", catalogId);
    return "/accounts/" + encode(tid) + "/catalogs/by-id/" + encode(cid);
  }

  public static String catalogPointerByName(String accountId, String displayName) {
    String tid = req("account_id", accountId);
    String name = req("display_name", displayName);
    return "/accounts/" + encode(tid) + "/catalogs/by-name/" + encode(name);
  }

  public static String catalogPointerByNamePrefix(String accountId) {
    String tid = req("account_id", accountId);
    return "/accounts/" + encode(tid) + "/catalogs/by-name/";
  }

  public static String catalogBlobUri(String accountId, String catalogId, String sha256) {
    String tid = req("account_id", accountId);
    String cid = req("catalog_id", catalogId);
    String sha = req("sha256", sha256);
    return String.format(
        "/accounts/%s/catalogs/%s/catalog/%s.pb", encode(tid), encode(cid), encode(sha));
  }

  // ===== Namespace =====

  public static String namespacePointerById(String accountId, String namespaceId) {
    String tid = req("account_id", accountId);
    String nid = req("namespace_id", namespaceId);
    return "/accounts/" + encode(tid) + "/namespaces/by-id/" + encode(nid);
  }

  public static String namespacePointerByPath(
      String accountId, String catalogId, List<String> pathSegments) {
    String tid = req("account_id", accountId);
    String cid = req("catalog_id", catalogId);
    String joined = joinPathSegments(reqPath("segments", pathSegments));
    return "/accounts/"
        + encode(tid)
        + "/catalogs/"
        + encode(cid)
        + "/namespaces/by-path/"
        + joined;
  }

  public static String namespacePointerByPathPrefix(
      String accountId, String catalogId, List<String> parentSegmentsOrEmpty) {
    String tid = req("account_id", accountId);
    String cid = req("catalog_id", catalogId);
    if (parentSegmentsOrEmpty == null)
      throw new IllegalArgumentException("key arg 'parent_segments' is null; use List.of()");
    String joined = joinPathSegments(parentSegmentsOrEmpty);
    String suffix = joined.isEmpty() ? "" : joined + "/";
    return "/accounts/"
        + encode(tid)
        + "/catalogs/"
        + encode(cid)
        + "/namespaces/by-path/"
        + suffix;
  }

  public static String namespaceBlobUri(String accountId, String namespaceId, String sha256) {
    String tid = req("account_id", accountId);
    String nid = req("namespace_id", namespaceId);
    String sha = req("sha256", sha256);
    return String.format(
        "/accounts/%s/namespaces/%s/namespace/%s.pb", encode(tid), encode(nid), encode(sha));
  }

  // ===== Table =====

  public static String tablePointerById(String accountId, String tableId) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    return "/accounts/" + encode(tid) + "/tables/by-id/" + encode(tbid);
  }

  public static String tablePointerByName(
      String accountId, String catalogId, String namespaceId, String tableName) {
    String tid = req("account_id", accountId);
    String cid = req("catalog_id", catalogId);
    String nid = req("namespace_id", namespaceId);
    String name = req("table_name", tableName);
    return "/accounts/"
        + encode(tid)
        + "/catalogs/"
        + encode(cid)
        + "/namespaces/"
        + encode(nid)
        + "/tables/by-name/"
        + encode(name);
  }

  public static String tablePointerByNamePrefix(
      String accountId, String catalogId, String namespaceId) {
    String tid = req("account_id", accountId);
    String cid = req("catalog_id", catalogId);
    String nid = req("namespace_id", namespaceId);
    return "/accounts/"
        + encode(tid)
        + "/catalogs/"
        + encode(cid)
        + "/namespaces/"
        + encode(nid)
        + "/tables/by-name/";
  }

  public static String tableBlobUri(String accountId, String tableId, String sha256) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    String sha = req("sha256", sha256);
    return String.format(
        "/accounts/%s/tables/%s/table/%s.pb", encode(tid), encode(tbid), encode(sha));
  }

  // ===== Snapshot =====

  public static String snapshotPointerById(String accountId, String tableId, long snapshotId) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    long sid = reqNonNegative("snapshot_id", snapshotId);
    return String.format(
        "/accounts/%s/tables/%s/snapshots/by-id/%019d", encode(tid), encode(tbid), sid);
  }

  public static String snapshotPointerByIdPrefix(String accountId, String tableId) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    return String.format("/accounts/%s/tables/%s/snapshots/by-id/", encode(tid), encode(tbid));
  }

  public static String snapshotPointerByTime(
      String accountId, String tableId, long snapshotId, long upstreamCreatedAtMs) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    long sid = reqNonNegative("snapshot_id", snapshotId);
    long ts = reqNonNegative("upstream_created_at_ms", upstreamCreatedAtMs);
    long inverted = Long.MAX_VALUE - ts;
    return String.format(
        "/accounts/%s/tables/%s/snapshots/by-time/%019d-%019d",
        encode(tid), encode(tbid), inverted, sid);
  }

  public static String snapshotPointerByTimePrefix(String accountId, String tableId) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    return String.format("/accounts/%s/tables/%s/snapshots/by-time/", encode(tid), encode(tbid));
  }

  public static String snapshotBlobUri(
      String accountId, String tableId, long snapshotId, String sha256) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    long sid = reqNonNegative("snapshot_id", snapshotId);
    String sha = req("sha256", sha256);
    return String.format(
        "/accounts/%s/tables/%s/snapshots/%019d/snapshot/%s.pb",
        encode(tid),
        encode(tbid),
        sid,
        encode(sha));
  }

  // ===== Snapshot Stats =====

  private static String snapshotStatsRootPointer(
      String accountId, String tableId, long snapshotId) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    long sid = reqNonNegative("snapshot_id", snapshotId);
    return String.format(
        "/accounts/%s/tables/%s/snapshots/%019d/stats/", encode(tid), encode(tbid), sid);
  }

  public static String snapshotTableStatsPointer(
      String accountId, String tableId, long snapshotId) {
    return snapshotStatsRootPointer(accountId, tableId, snapshotId) + "table";
  }

  public static String snapshotColumnStatsDirectoryPointer(
      String accountId, String tableId, long snapshotId) {
    return snapshotStatsRootPointer(accountId, tableId, snapshotId) + "columns/";
  }

  public static String snapshotColumnStatsPointer(
      String accountId, String tableId, long snapshotId, int columnId) {
    String cid = normalizeColumnId(columnId);
    return snapshotColumnStatsDirectoryPointer(accountId, tableId, snapshotId) + encode(cid);
  }

  public static String snapshotTableStatsBlobUri(String accountId, String tableId, String sha256) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    return String.format(
        "/accounts/%s/tables/%s/table-stats/%s.pb", encode(tid), encode(tbid), encode(sha256));
  }

  public static String snapshotColumnStatsBlobUri(
      String accountId, String tableId, int columnId, String sha256) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    String cid = normalizeColumnId(columnId);
    return String.format(
        "/accounts/%s/tables/%s/column-stats/%s/%s.pb",
        encode(tid), encode(tbid), encode(cid), encode(sha256));
  }

  public static String snapshotStatsPrefix(String accountId, String tableId, long snapshotId) {
    return snapshotStatsRootPointer(accountId, tableId, snapshotId);
  }

  public static String snapshotColumnStatsPrefix(
      String accountId, String tableId, long snapshotId) {
    return snapshotColumnStatsDirectoryPointer(accountId, tableId, snapshotId);
  }

  public static String snapshotFileStatsDirectoryPointer(
      String accountId, String tableId, long snapshotId) {
    return snapshotStatsRootPointer(accountId, tableId, snapshotId) + "files/";
  }

  public static String snapshotFileStatsPointer(
      String accountId, String tableId, long snapshotId, String filePath) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    long sid = reqNonNegative("snapshot_id", snapshotId);
    String fp = req("file_path", filePath);
    return snapshotFileStatsDirectoryPointer(tid, tbid, sid) + encode(fp);
  }

  public static String snapshotFileStatsBlobUri(
      String accountId, String tableId, String filePath, String sha256) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    String fp = req("file_path", filePath);
    String sha = req("sha256", sha256);
    return String.format(
        "/accounts/%s/tables/%s/file-stats/%s/%s.pb",
        encode(tid), encode(tbid), encode(fp), encode(sha));
  }

  public static String snapshotFileStatsPrefix(String accountId, String tableId, long snapshotId) {
    return snapshotFileStatsDirectoryPointer(accountId, tableId, snapshotId);
  }

  // ===== View =====

  public static String viewPointerById(String accountId, String viewId) {
    String tid = req("account_id", accountId);
    String vid = req("view_id", viewId);
    return "/accounts/" + encode(tid) + "/views/by-id/" + encode(vid);
  }

  public static String viewPointerByName(
      String accountId, String catalogId, String namespaceId, String viewName) {
    String tid = req("account_id", accountId);
    String cid = req("catalog_id", catalogId);
    String nid = req("namespace_id", namespaceId);
    String name = req("view_name", viewName);
    return "/accounts/"
        + encode(tid)
        + "/catalogs/"
        + encode(cid)
        + "/namespaces/"
        + encode(nid)
        + "/views/by-name/"
        + encode(name);
  }

  public static String viewPointerByNamePrefix(
      String accountId, String catalogId, String namespaceId) {
    String tid = req("account_id", accountId);
    String cid = req("catalog_id", catalogId);
    String nid = req("namespace_id", namespaceId);
    return "/accounts/"
        + encode(tid)
        + "/catalogs/"
        + encode(cid)
        + "/namespaces/"
        + encode(nid)
        + "/views/by-name/";
  }

  public static String viewBlobUri(String accountId, String viewId, String sha256) {
    String tid = req("account_id", accountId);
    String vid = req("view_id", viewId);
    String sha = req("sha256", sha256);
    return String.format("/accounts/%s/views/%s/view/%s.pb", encode(tid), encode(vid), encode(sha));
  }

  // ===== Connector =====

  public static String connectorPointerById(String accountId, String connectorId) {
    String tid = req("account_id", accountId);
    String cid = req("connector_id", connectorId);
    return "/accounts/" + encode(tid) + "/connectors/by-id/" + encode(cid);
  }

  public static String connectorPointerByName(String accountId, String displayName) {
    String tid = req("account_id", accountId);
    String name = req("display_name", displayName);
    return "/accounts/" + encode(tid) + "/connectors/by-name/" + encode(name);
  }

  public static String connectorPointerByNamePrefix(String accountId) {
    String tid = req("account_id", accountId);
    return "/accounts/" + encode(tid) + "/connectors/by-name/";
  }

  public static String connectorBlobUri(String accountId, String connectorId, String sha256) {
    String tid = req("account_id", accountId);
    String cid = req("connector_id", connectorId);
    String sha = req("sha256", sha256);
    return String.format(
        "/accounts/%s/connectors/%s/connector/%s.pb", encode(tid), encode(cid), encode(sha));
  }

  // ===== Idempotency =====

  public static String idempotencyKey(String accountId, String operation, String key) {
    String tid = req("account_id", accountId);
    String op = req("operation", operation);
    String k = req("key", key);
    return "/accounts/" + encode(tid) + "/idempotency/" + encode(op) + "/" + encode(k);
  }

  public static String idempotencyBlobUri(String accountId, String key) {
    String tid = req("account_id", accountId);
    String k = req("key", key);
    return "/accounts/" + encode(tid) + "/idempotency/" + encode(k) + "/idempotency.pb";
  }

  public static String idempotencyPrefixAccount(String accountId) {
    String tid = req("account_id", accountId);
    return "/accounts/" + encode(tid) + "/idempotency/";
  }
}
