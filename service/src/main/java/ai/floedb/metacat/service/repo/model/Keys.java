package ai.floedb.metacat.service.repo.model;

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

  private static String normalizeColumnId(String columnId) {
    String id = req("column_id", columnId);
    if (id.matches("^(0|[1-9]\\d*)$")) {
      long v = Long.parseLong(id);
      return String.format("%019d", v);
    }
    return id;
  }

  // ===== Tenant =====

  public static String tenantRootPointer(String tenantId) {
    String tid = req("tenant_id", tenantId);
    return "/tenants/" + encode(tid);
  }

  public static String tenantPointerById(String tenantId) {
    String tid = req("tenant_id", tenantId);
    return "/tenants/by-id/" + encode(tid);
  }

  public static String tenantPointerByName(String displayName) {
    String name = req("display_name", displayName);
    return "/tenants/by-name/" + encode(name);
  }

  public static String tenantPointerByNamePrefix() {
    return "/tenants/by-name/";
  }

  public static String tenantBlobUri(String tenantId) {
    String tid = req("tenant_id", tenantId);
    return "/tenants/" + encode(tid) + "/tenant.pb";
  }

  // ===== Catalog =====

  public static String catalogPointerById(String tenantId, String catalogId) {
    String tid = req("tenant_id", tenantId);
    String cid = req("catalog_id", catalogId);
    return "/tenants/" + encode(tid) + "/catalogs/by-id/" + encode(cid);
  }

  public static String catalogPointerByName(String tenantId, String displayName) {
    String tid = req("tenant_id", tenantId);
    String name = req("display_name", displayName);
    return "/tenants/" + encode(tid) + "/catalogs/by-name/" + encode(name);
  }

  public static String catalogPointerByNamePrefix(String tenantId) {
    String tid = req("tenant_id", tenantId);
    return "/tenants/" + encode(tid) + "/catalogs/by-name/";
  }

  public static String catalogBlobUri(String tenantId, String catalogId) {
    String tid = req("tenant_id", tenantId);
    String cid = req("catalog_id", catalogId);
    return "/tenants/" + encode(tid) + "/catalogs/" + encode(cid) + "/catalog.pb";
  }

  // ===== Namespace =====

  public static String namespacePointerById(String tenantId, String namespaceId) {
    String tid = req("tenant_id", tenantId);
    String nid = req("namespace_id", namespaceId);
    return "/tenants/" + encode(tid) + "/namespaces/by-id/" + encode(nid);
  }

  public static String namespacePointerByPath(
      String tenantId, String catalogId, List<String> pathSegments) {
    String tid = req("tenant_id", tenantId);
    String cid = req("catalog_id", catalogId);
    String joined = joinPathSegments(reqPath("segments", pathSegments));
    return "/tenants/" + encode(tid) + "/catalogs/" + encode(cid) + "/namespaces/by-path/" + joined;
  }

  public static String namespacePointerByPathPrefix(
      String tenantId, String catalogId, List<String> parentSegmentsOrEmpty) {
    String tid = req("tenant_id", tenantId);
    String cid = req("catalog_id", catalogId);
    if (parentSegmentsOrEmpty == null)
      throw new IllegalArgumentException("key arg 'parent_segments' is null; use List.of()");
    String joined = joinPathSegments(parentSegmentsOrEmpty);
    String suffix = joined.isEmpty() ? "" : joined + "/";
    return "/tenants/" + encode(tid) + "/catalogs/" + encode(cid) + "/namespaces/by-path/" + suffix;
  }

  public static String namespaceBlobUri(String tenantId, String namespaceId) {
    String tid = req("tenant_id", tenantId);
    String nid = req("namespace_id", namespaceId);
    return "/tenants/" + encode(tid) + "/namespaces/" + encode(nid) + "/namespace.pb";
  }

  // ===== Table =====

  public static String tablePointerById(String tenantId, String tableId) {
    String tid = req("tenant_id", tenantId);
    String tbid = req("table_id", tableId);
    return "/tenants/" + encode(tid) + "/tables/by-id/" + encode(tbid);
  }

  public static String tablePointerByName(
      String tenantId, String catalogId, String namespaceId, String tableName) {
    String tid = req("tenant_id", tenantId);
    String cid = req("catalog_id", catalogId);
    String nid = req("namespace_id", namespaceId);
    String name = req("table_name", tableName);
    return "/tenants/"
        + encode(tid)
        + "/catalogs/"
        + encode(cid)
        + "/namespaces/"
        + encode(nid)
        + "/tables/by-name/"
        + encode(name);
  }

  public static String tablePointerByNamePrefix(
      String tenantId, String catalogId, String namespaceId) {
    String tid = req("tenant_id", tenantId);
    String cid = req("catalog_id", catalogId);
    String nid = req("namespace_id", namespaceId);
    return "/tenants/"
        + encode(tid)
        + "/catalogs/"
        + encode(cid)
        + "/namespaces/"
        + encode(nid)
        + "/tables/by-name/";
  }

  public static String tableBlobUri(String tenantId, String tableId) {
    String tid = req("tenant_id", tenantId);
    String tbid = req("table_id", tableId);
    return "/tenants/" + encode(tid) + "/tables/" + encode(tbid) + "/table.pb";
  }

  // ===== Snapshot =====

  public static String snapshotPointerById(String tenantId, String tableId, long snapshotId) {
    String tid = req("tenant_id", tenantId);
    String tbid = req("table_id", tableId);
    long sid = reqNonNegative("snapshot_id", snapshotId);
    return String.format(
        "/tenants/%s/tables/%s/snapshots/by-id/%019d", encode(tid), encode(tbid), sid);
  }

  public static String snapshotPointerByIdPrefix(String tenantId, String tableId) {
    String tid = req("tenant_id", tenantId);
    String tbid = req("table_id", tableId);
    return String.format("/tenants/%s/tables/%s/snapshots/by-id/", encode(tid), encode(tbid));
  }

  public static String snapshotPointerByTime(
      String tenantId, String tableId, long snapshotId, long upstreamCreatedAtMs) {
    String tid = req("tenant_id", tenantId);
    String tbid = req("table_id", tableId);
    long sid = reqNonNegative("snapshot_id", snapshotId);
    long ts = reqNonNegative("upstream_created_at_ms", upstreamCreatedAtMs);
    long inverted = Long.MAX_VALUE - ts;
    return String.format(
        "/tenants/%s/tables/%s/snapshots/by-time/%019d-%019d",
        encode(tid), encode(tbid), inverted, sid);
  }

  public static String snapshotPointerByTimePrefix(String tenantId, String tableId) {
    String tid = req("tenant_id", tenantId);
    String tbid = req("table_id", tableId);
    return String.format("/tenants/%s/tables/%s/snapshots/by-time/", encode(tid), encode(tbid));
  }

  public static String snapshotBlobUri(String tenantId, String tableId, long snapshotId) {
    String tid = req("tenant_id", tenantId);
    String tbid = req("table_id", tableId);
    long sid = reqNonNegative("snapshot_id", snapshotId);
    return String.format(
        "/tenants/%s/tables/%s/snapshots/%019d/snapshot.pb", encode(tid), encode(tbid), sid);
  }

  // ===== Snapshot Stats =====

  private static String snapshotStatsRootPointer(String tenantId, String tableId, long snapshotId) {
    String tid = req("tenant_id", tenantId);
    String tbid = req("table_id", tableId);
    long sid = reqNonNegative("snapshot_id", snapshotId);
    return String.format(
        "/tenants/%s/tables/%s/snapshots/%019d/stats/", encode(tid), encode(tbid), sid);
  }

  public static String snapshotTableStatsPointer(String tenantId, String tableId, long snapshotId) {
    return snapshotStatsRootPointer(tenantId, tableId, snapshotId) + "table";
  }

  public static String snapshotColumnStatsDirectoryPointer(
      String tenantId, String tableId, long snapshotId) {
    return snapshotStatsRootPointer(tenantId, tableId, snapshotId) + "columns/";
  }

  public static String snapshotColumnStatsPointer(
      String tenantId, String tableId, long snapshotId, String columnId) {
    String cid = normalizeColumnId(columnId);
    return snapshotColumnStatsDirectoryPointer(tenantId, tableId, snapshotId) + encode(cid);
  }

  public static String snapshotTableStatsBlobUri(String tenantId, String tableId, String sha256) {
    String tid = req("tenant_id", tenantId);
    String tbid = req("table_id", tableId);
    return String.format(
        "/tenants/%s/tables/%s/table-stats/%s.pb", encode(tid), encode(tbid), encode(sha256));
  }

  public static String snapshotColumnStatsBlobUri(
      String tenantId, String tableId, String columnId, String sha256) {
    String tid = req("tenant_id", tenantId);
    String tbid = req("table_id", tableId);
    String cid = normalizeColumnId(columnId);
    return String.format(
        "/tenants/%s/tables/%s/column-stats/%s/%s.pb",
        encode(tid), encode(tbid), encode(cid), encode(sha256));
  }

  public static String snapshotStatsPrefix(String tenantId, String tableId, long snapshotId) {
    return snapshotStatsRootPointer(tenantId, tableId, snapshotId);
  }

  public static String snapshotColumnStatsPrefix(String tenantId, String tableId, long snapshotId) {
    return snapshotColumnStatsDirectoryPointer(tenantId, tableId, snapshotId);
  }

  // ===== View =====

  public static String viewPointerById(String tenantId, String viewId) {
    String tid = req("tenant_id", tenantId);
    String vid = req("view_id", viewId);
    return "/tenants/" + encode(tid) + "/views/by-id/" + encode(vid);
  }

  public static String viewPointerByName(
      String tenantId, String catalogId, String namespaceId, String viewName) {
    String tid = req("tenant_id", tenantId);
    String cid = req("catalog_id", catalogId);
    String nid = req("namespace_id", namespaceId);
    String name = req("view_name", viewName);
    return "/tenants/"
        + encode(tid)
        + "/catalogs/"
        + encode(cid)
        + "/namespaces/"
        + encode(nid)
        + "/views/by-name/"
        + encode(name);
  }

  public static String viewPointerByNamePrefix(
      String tenantId, String catalogId, String namespaceId) {
    String tid = req("tenant_id", tenantId);
    String cid = req("catalog_id", catalogId);
    String nid = req("namespace_id", namespaceId);
    return "/tenants/"
        + encode(tid)
        + "/catalogs/"
        + encode(cid)
        + "/namespaces/"
        + encode(nid)
        + "/views/by-name/";
  }

  public static String viewBlobUri(String tenantId, String viewId) {
    String tid = req("tenant_id", tenantId);
    String vid = req("view_id", viewId);
    return "/tenants/" + encode(tid) + "/views/" + encode(vid) + "/view.pb";
  }

  // ===== Connector =====

  public static String connectorPointerById(String tenantId, String connectorId) {
    String tid = req("tenant_id", tenantId);
    String cid = req("connector_id", connectorId);
    return "/tenants/" + encode(tid) + "/connectors/by-id/" + encode(cid);
  }

  public static String connectorPointerByName(String tenantId, String displayName) {
    String tid = req("tenant_id", tenantId);
    String name = req("display_name", displayName);
    return "/tenants/" + encode(tid) + "/connectors/by-name/" + encode(name);
  }

  public static String connectorPointerByNamePrefix(String tenantId) {
    String tid = req("tenant_id", tenantId);
    return "/tenants/" + encode(tid) + "/connectors/by-name/";
  }

  public static String connectorBlobUri(String tenantId, String connectorId) {
    String tid = req("tenant_id", tenantId);
    String cid = req("connector_id", connectorId);
    return "/tenants/" + encode(tid) + "/connectors/" + encode(cid) + "/connector.pb";
  }

  // ===== Idempotency =====

  public static String idempotencyKey(String tenantId, String operation, String key) {
    String tid = req("tenant_id", tenantId);
    String op = req("operation", operation);
    String k = req("key", key);
    return "/tenants/" + encode(tid) + "/idempotency/" + encode(op) + "/" + encode(k);
  }

  public static String idempotencyBlobUri(String tenantId, String key) {
    String tid = req("tenant_id", tenantId);
    String k = req("key", key);
    return "/tenants/" + encode(tid) + "/idempotency/" + encode(k) + "/idempotency.pb";
  }

  public static String idempotencyPrefixTenant(String tenantId) {
    String tid = req("tenant_id", tenantId);
    return "/tenants/" + encode(tid) + "/idempotency/";
  }
}
