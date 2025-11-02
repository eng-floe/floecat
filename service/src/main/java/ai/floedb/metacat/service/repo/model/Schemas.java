package ai.floedb.metacat.service.repo.model;

import static java.util.Map.of;

import ai.floedb.metacat.catalog.rpc.Catalog;
import ai.floedb.metacat.catalog.rpc.ColumnStats;
import ai.floedb.metacat.catalog.rpc.Namespace;
import ai.floedb.metacat.catalog.rpc.Snapshot;
import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.catalog.rpc.TableStats;
import ai.floedb.metacat.catalog.rpc.View;
import ai.floedb.metacat.connector.rpc.Connector;
import ai.floedb.metacat.service.repo.util.ColumnStatsNormalizer;
import ai.floedb.metacat.service.repo.util.TableStatsNormalizer;
import ai.floedb.metacat.tenant.rpc.Tenant;
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
      ResourceSchema.<TableStats, TableStatsKey>of(
              "table-stats",
              (TableStatsKey key) ->
                  Keys.snapshotTableStatsPointer(key.tenantId(), key.tableId(), key.snapshotId()),
              (TableStatsKey key) ->
                  Keys.snapshotTableStatsBlobUri(key.tenantId(), key.tableId(), key.sha256()),
              (TableStats v) -> java.util.Map.of(),
              (TableStats v) -> {
                var norm = TableStatsNormalizer.normalize(v);
                var sha = TableStatsNormalizer.sha256Hex(norm.toByteArray());
                return new TableStatsKey(
                    v.getTableId().getTenantId(), v.getTableId().getId(), v.getSnapshotId(), sha);
              })
          .withCasBlobs();

  public static final ResourceSchema<ColumnStats, ColumnStatsKey> COLUMN_STATS =
      ResourceSchema.<ColumnStats, ColumnStatsKey>of(
              "column-stats",
              (ColumnStatsKey key) ->
                  Keys.snapshotColumnStatsPointer(
                      key.tenantId(), key.tableId(), key.snapshotId(), key.columnId()),
              (ColumnStatsKey key) ->
                  Keys.snapshotColumnStatsBlobUri(
                      key.tenantId(), key.tableId(), key.columnId(), key.sha256()),
              (ColumnStats v) -> java.util.Map.of(),
              (ColumnStats v) -> {
                var norm = ColumnStatsNormalizer.normalize(v);
                var sha = ColumnStatsNormalizer.sha256Hex(norm.toByteArray());
                return new ColumnStatsKey(
                    v.getTableId().getTenantId(),
                    v.getTableId().getId(),
                    v.getSnapshotId(),
                    v.getColumnId(),
                    sha);
              })
          .withCasBlobs();

  public static final ResourceSchema<View, ViewKey> VIEW =
      ResourceSchema.of(
          "view",
          key -> Keys.viewPointerById(key.tenantId(), key.viewId()),
          key -> Keys.viewBlobUri(key.tenantId(), key.viewId()),
          v ->
              of(
                  "byName",
                  Keys.viewPointerByName(
                      v.getResourceId().getTenantId(),
                      v.getCatalogId().getId(),
                      v.getNamespaceId().getId(),
                      v.getDisplayName())),
          v -> new ViewKey(v.getResourceId().getTenantId(), v.getResourceId().getId()));

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
