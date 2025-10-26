package ai.floedb.metacat.service.repo.model;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

public final class Keys {

  private static String encode(String s) {
    return URLEncoder.encode(s, StandardCharsets.UTF_8);
  }

  private static String joinEncoded(List<String> segments) {
    return String.join("/", segments.stream().map(Keys::encode).toArray(String[]::new));
  }

  // ===== Tenant =====

  public static String tenantPointerById(String tenantId) {
    return "/tenants/by-id/" + encode(tenantId);
  }

  public static String tenantPointerByName(String displayName) {
    return "/tenants/by-name/" + encode(displayName);
  }

  public static String tenantPointerByNamePrefix() {
    return "/tenants/by-name/";
  }

  public static String tenantBlobUri(String tenantId) {
    return "/tenants/" + encode(tenantId) + "/tenant.pb";
  }

  // ===== Catalog =====

  public static String catalogPointerById(String tenantId, String catalogId) {
    return "/tenants/" + encode(tenantId) + "/catalogs/by-id/" + encode(catalogId);
  }

  public static String catalogPointerByName(String tenantId, String displayName) {
    return "/tenants/" + encode(tenantId) + "/catalogs/by-name/" + encode(displayName);
  }

  public static String catalogPointerByNamePrefix(String tenantId) {
    return "/tenants/" + encode(tenantId) + "/catalogs/by-name/";
  }

  public static String catalogBlobUri(String tenantId, String catalogId) {
    return "/tenants/" + encode(tenantId) + "/catalogs/" + encode(catalogId) + "/catalog.pb";
  }

  // ===== Namespace =====

  public static String namespacePointerById(String tenantId, String namespaceId) {
    return "/tenants/" + encode(tenantId) + "/namespaces/by-id/" + encode(namespaceId);
  }

  public static String namespacePointerByPath(
      String tenantId, String catalogId, List<String> pathSegments) {
    String joined = joinEncoded(pathSegments);
    return "/tenants/"
        + encode(tenantId)
        + "/catalogs/"
        + encode(catalogId)
        + "/namespaces/by-path/"
        + joined;
  }

  public static String namespacePointerByPathPrefix(
      String tenantId, String catalogId, List<String> parentSegmentsOrEmpty) {
    String joined = joinEncoded(parentSegmentsOrEmpty);
    return "/tenants/"
        + encode(tenantId)
        + "/catalogs/"
        + encode(catalogId)
        + "/namespaces/by-path/"
        + (joined.isEmpty() ? "" : joined + "/");
  }

  public static String namespaceBlobUri(String tenantId, String namespaceId) {
    return "/tenants/" + encode(tenantId) + "/namespaces/" + encode(namespaceId) + "/namespace.pb";
  }

  // ===== Table =====

  public static String tablePointerById(String tenantId, String tableId) {
    return "/tenants/" + encode(tenantId) + "/tables/by-id/" + encode(tableId);
  }

  public static String tablePointerByName(
      String tenantId, String catalogId, String namespaceId, String tableName) {
    return "/tenants/"
        + encode(tenantId)
        + "/catalogs/"
        + encode(catalogId)
        + "/namespaces/"
        + encode(namespaceId)
        + "/tables/by-name/"
        + encode(tableName);
  }

  public static String tablePointerByNamePrefix(
      String tenantId, String catalogId, String namespaceId) {
    return "/tenants/"
        + encode(tenantId)
        + "/catalogs/"
        + encode(catalogId)
        + "/namespaces/"
        + encode(namespaceId)
        + "/tables/by-name/";
  }

  public static String tableBlobUri(String tenantId, String tableId) {
    return "/tenants/" + encode(tenantId) + "/tables/" + encode(tableId) + "/table.pb";
  }

  // ===== Snapshot =====

  public static String snapshotPointerById(String tenantId, String tableId, long snapshotId) {
    return String.format(
        "/tenants/%s/tables/%s/snapshots/by-id/%019d",
        encode(tenantId), encode(tableId), snapshotId);
  }

  public static String snapshotPointerByIdPrefix(String tenantId, String tableId) {
    return String.format(
        "/tenants/%s/tables/%s/snapshots/by-id/", encode(tenantId), encode(tableId));
  }

  public static String snapshotPointerByTime(
      String tenantId, String tableId, long snapshotId, long upstreamCreatedAtMs) {
    long inverted = Long.MAX_VALUE - upstreamCreatedAtMs;
    return String.format(
        "/tenants/%s/tables/%s/snapshots/by-time/%019d-%019d",
        encode(tenantId), encode(tableId), inverted, snapshotId);
  }

  public static String snapshotPointerByTimePrefix(String tenantId, String tableId) {
    return String.format(
        "/tenants/%s/tables/%s/snapshots/by-time/", encode(tenantId), encode(tableId));
  }

  public static String snapshotBlobUri(String tenantId, String tableId, long snapshotId) {
    return String.format(
        "/tenants/%s/tables/%s/snapshots/%019d/snapshot.pb",
        encode(tenantId), encode(tableId), snapshotId);
  }

  // ===== Snapshot Stats =====

  private static String snapshotStatsRootPointer(String tenantId, String tableId, long snapshotId) {
    return String.format(
        "/tenants/%s/tables/%s/snapshots/%d/stats/", encode(tenantId), encode(tableId), snapshotId);
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
    return snapshotColumnStatsDirectoryPointer(tenantId, tableId, snapshotId) + encode(columnId);
  }

  public static String snapshotTableStatsBlobUri(String tenantId, String tableId, long snapshotId) {
    return String.format(
        "/tenants/%s/tables/%s/snapshots/%d/stats/table.pb",
        encode(tenantId), encode(tableId), snapshotId);
  }

  public static String snapshotColumnStatsBlobUri(
      String tenantId, String tableId, long snapshotId, String columnId) {
    return String.format(
        "/tenants/%s/tables/%s/snapshots/%d/stats/columns/%s/column.pb",
        encode(tenantId), encode(tableId), snapshotId, encode(columnId));
  }

  public static String snapshotStatsPrefix(String tenantId, String tableId, long snapshotId) {
    return snapshotStatsRootPointer(tenantId, tableId, snapshotId);
  }

  public static String snapshotColumnStatsPrefix(String tenantId, String tableId, long snapshotId) {
    return snapshotColumnStatsDirectoryPointer(tenantId, tableId, snapshotId);
  }

  // ===== Connector =====

  public static String connectorPointerById(String tenantId, String connectorId) {
    return "/tenants/" + encode(tenantId) + "/connectors/by-id/" + encode(connectorId);
  }

  public static String connectorPointerByName(String tenantId, String displayName) {
    return "/tenants/" + encode(tenantId) + "/connectors/by-name/" + encode(displayName);
  }

  public static String connectorPointerByNamePrefix(String tenantId) {
    return "/tenants/" + encode(tenantId) + "/connectors/by-name/";
  }

  public static String connectorBlobUri(String tenantId, String connectorId) {
    return "/tenants/" + encode(tenantId) + "/connectors/" + encode(connectorId) + "/connector.pb";
  }

  // ===== Idempotency =====

  public static String idempotencyKey(String tenantId, String operation, String key) {
    return "/tenants/" + encode(tenantId) + "/idempotency/" + encode(operation) + "/" + encode(key);
  }

  public static String idempotencyBlobUri(String key) {
    return encode(key) + "/idempotency.pb";
  }
}
