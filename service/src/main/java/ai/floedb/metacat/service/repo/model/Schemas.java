package ai.floedb.metacat.service.repo.model;

import static java.util.Map.of;

import ai.floedb.metacat.catalog.rpc.*;
import ai.floedb.metacat.connector.rpc.Connector;
import ai.floedb.metacat.tenancy.rpc.Tenant;
import com.google.protobuf.util.Timestamps;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class Schemas {

  public static final ResourceSchema<Tenant, TenantKey> TENANT =
      ResourceSchema.of(
          "tenant",
          key -> Keys.tenantPointerById(key.tenantId()),
          key -> Keys.tenantBlobUri(key.tenantId()),
          v -> of("byName", Keys.tenantPointerByName(v.getDisplayName())),
          v -> new TenantKey(v.getResourceId().getId()));

  public static final ResourceSchema<Catalog, CatalogKey> CATALOG =
      ResourceSchema.of(
          "catalog",
          key -> Keys.catalogPointerById(key.tenantId(), key.catalogId()),
          key -> Keys.catalogBlobUri(key.tenantId(), key.catalogId()),
          v ->
              of(
                  "byName",
                  Keys.catalogPointerByName(v.getResourceId().getTenantId(), v.getDisplayName())),
          v -> new CatalogKey(v.getResourceId().getTenantId(), v.getResourceId().getId()));

  public static final ResourceSchema<Namespace, NamespaceKey> NAMESPACE =
      ResourceSchema.of(
          "namespace",
          key -> Keys.namespacePointerById(key.tenantId(), key.namespaceId()),
          key -> Keys.namespaceBlobUri(key.tenantId(), key.namespaceId()),
          v -> {
            List<String> fullPath = new ArrayList<>(v.getParentsList());
            fullPath.add(v.getDisplayName());
            return Map.of(
                "byPath",
                Keys.namespacePointerByPath(
                    v.getResourceId().getTenantId(), v.getCatalogId().getId(), fullPath));
          },
          v -> new NamespaceKey(v.getResourceId().getTenantId(), v.getResourceId().getId()));

  public static final ResourceSchema<Table, TableKey> TABLE =
      ResourceSchema.of(
          "table",
          key -> Keys.tablePointerById(key.tenantId(), key.tableId()),
          key -> Keys.tableBlobUri(key.tenantId(), key.tableId()),
          v ->
              of(
                  "byName",
                  Keys.tablePointerByName(
                      v.getResourceId().getTenantId(),
                      v.getCatalogId().getId(),
                      v.getNamespaceId().getId(),
                      v.getDisplayName())),
          v -> new TableKey(v.getResourceId().getTenantId(), v.getResourceId().getId()));

  public static final ResourceSchema<Snapshot, SnapshotKey> SNAPSHOT =
      ResourceSchema.of(
          "snapshot",
          key -> Keys.snapshotPointerById(key.tenantId(), key.tableId(), key.snapshotId()),
          key -> Keys.snapshotBlobUri(key.tenantId(), key.tableId(), key.snapshotId()),
          v ->
              of(
                  "byId",
                      Keys.snapshotPointerById(
                          v.getTableId().getTenantId(), v.getTableId().getId(), v.getSnapshotId()),
                  "byTime",
                      Keys.snapshotPointerByTime(
                          v.getTableId().getTenantId(), v.getTableId().getId(),
                          v.getSnapshotId(), Timestamps.toMillis(v.getUpstreamCreatedAt()))),
          v ->
              new SnapshotKey(
                  v.getTableId().getTenantId(), v.getTableId().getId(), v.getSnapshotId()));

  public static final ResourceSchema<TableStats, TableStatsKey> TABLE_STATS =
      ResourceSchema.of(
          "table-stats",
          key -> Keys.snapshotTableStatsPointer(key.tenantId(), key.tableId(), key.snapshotId()),
          key -> Keys.snapshotTableStatsBlobUri(key.tenantId(), key.tableId(), key.snapshotId()),
          v -> Map.of(),
          v ->
              new TableStatsKey(
                  v.getTableId().getTenantId(), v.getTableId().getId(), v.getSnapshotId()));

  public static final ResourceSchema<ColumnStats, ColumnStatsKey> COLUMN_STATS =
      ResourceSchema.of(
          "column-stats",
          key ->
              Keys.snapshotColumnStatsPointer(
                  key.tenantId(), key.tableId(), key.snapshotId(), key.columnId()),
          key ->
              Keys.snapshotColumnStatsBlobUri(
                  key.tenantId(), key.tableId(), key.snapshotId(), key.columnId()),
          v -> Map.of(),
          v ->
              new ColumnStatsKey(
                  v.getTableId().getTenantId(),
                  v.getTableId().getId(),
                  v.getSnapshotId(),
                  v.getColumnId()));

  public static final ResourceSchema<Connector, ConnectorKey> CONNECTOR =
      ResourceSchema.of(
          "connector",
          key -> Keys.connectorPointerById(key.tenantId(), key.connectorId()),
          key -> Keys.connectorBlobUri(key.tenantId(), key.connectorId()),
          v ->
              of(
                  "byName",
                  Keys.connectorPointerByName(v.getResourceId().getTenantId(), v.getDisplayName())),
          v -> new ConnectorKey(v.getResourceId().getTenantId(), v.getResourceId().getId()));
}
