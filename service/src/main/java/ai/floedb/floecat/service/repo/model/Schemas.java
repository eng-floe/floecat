package ai.floedb.floecat.service.repo.model;

import ai.floedb.floecat.account.rpc.Account;
import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.ColumnStats;
import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableStats;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.service.repo.util.ColumnStatsNormalizer;
import ai.floedb.floecat.service.repo.util.ResourceHash;
import ai.floedb.floecat.service.repo.util.TableStatsNormalizer;
import com.google.protobuf.util.Timestamps;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class Schemas {

  public static final ResourceSchema<Account, AccountKey> ACCOUNT =
      ResourceSchema.<Account, AccountKey>of(
              "account",
              key -> Keys.accountPointerById(key.accountId()),
              key -> Keys.accountBlobUri(key.accountId(), key.sha256()),
              v -> Map.of("byName", Keys.accountPointerByName(v.getDisplayName())),
              v -> {
                var sha = ResourceHash.sha256Hex(v.toByteArray());
                return new AccountKey(v.getResourceId().getId(), sha);
              })
          .withCasBlobs();

  public static final ResourceSchema<Catalog, CatalogKey> CATALOG =
      ResourceSchema.<Catalog, CatalogKey>of(
              "catalog",
              key -> Keys.catalogPointerById(key.accountId(), key.catalogId()),
              key -> Keys.catalogBlobUri(key.accountId(), key.catalogId(), key.sha256()),
              v ->
                  Map.of(
                      "byName",
                      Keys.catalogPointerByName(
                          v.getResourceId().getAccountId(), v.getDisplayName())),
              v -> {
                var sha = ResourceHash.sha256Hex(v.toByteArray());
                return new CatalogKey(
                    v.getResourceId().getAccountId(), v.getResourceId().getId(), sha);
              })
          .withCasBlobs();

  public static final ResourceSchema<Namespace, NamespaceKey> NAMESPACE =
      ResourceSchema.<Namespace, NamespaceKey>of(
              "namespace",
              key -> Keys.namespacePointerById(key.accountId(), key.namespaceId()),
              key -> Keys.namespaceBlobUri(key.accountId(), key.namespaceId(), key.sha256()),
              v -> {
                List<String> fullPath = new ArrayList<>(v.getParentsList());
                fullPath.add(v.getDisplayName());
                return Map.of(
                    "byPath",
                    Keys.namespacePointerByPath(
                        v.getResourceId().getAccountId(), v.getCatalogId().getId(), fullPath));
              },
              v -> {
                var sha = ResourceHash.sha256Hex(v.toByteArray());
                return new NamespaceKey(
                    v.getResourceId().getAccountId(), v.getResourceId().getId(), sha);
              })
          .withCasBlobs();

  public static final ResourceSchema<Table, TableKey> TABLE =
      ResourceSchema.<Table, TableKey>of(
              "table",
              key -> Keys.tablePointerById(key.accountId(), key.tableId()),
              key -> Keys.tableBlobUri(key.accountId(), key.tableId(), key.sha256()),
              v ->
                  Map.of(
                      "byName",
                      Keys.tablePointerByName(
                          v.getResourceId().getAccountId(),
                          v.getCatalogId().getId(),
                          v.getNamespaceId().getId(),
                          v.getDisplayName())),
              v -> {
                var sha = ResourceHash.sha256Hex(v.toByteArray());
                return new TableKey(
                    v.getResourceId().getAccountId(), v.getResourceId().getId(), sha);
              })
          .withCasBlobs();

  public static final ResourceSchema<Snapshot, SnapshotKey> SNAPSHOT =
      ResourceSchema.<Snapshot, SnapshotKey>of(
              "snapshot",
              key -> Keys.snapshotPointerById(key.accountId(), key.tableId(), key.snapshotId()),
              key ->
                  Keys.snapshotBlobUri(
                      key.accountId(), key.tableId(), key.snapshotId(), key.sha256()),
              v ->
                  Map.of(
                      "byId",
                          Keys.snapshotPointerById(
                              v.getTableId().getAccountId(),
                              v.getTableId().getId(),
                              v.getSnapshotId()),
                      "byTime",
                          Keys.snapshotPointerByTime(
                              v.getTableId().getAccountId(), v.getTableId().getId(),
                              v.getSnapshotId(), Timestamps.toMillis(v.getUpstreamCreatedAt()))),
              v -> {
                var sha = ResourceHash.sha256Hex(v.toByteArray());
                return new SnapshotKey(
                    v.getTableId().getAccountId(), v.getTableId().getId(), v.getSnapshotId(), sha);
              })
          .withCasBlobs();

  public static final ResourceSchema<TableStats, TableStatsKey> TABLE_STATS =
      ResourceSchema.<TableStats, TableStatsKey>of(
              "table-stats",
              (TableStatsKey key) ->
                  Keys.snapshotTableStatsPointer(key.accountId(), key.tableId(), key.snapshotId()),
              (TableStatsKey key) ->
                  Keys.snapshotTableStatsBlobUri(key.accountId(), key.tableId(), key.sha256()),
              (TableStats v) -> Map.of(),
              (TableStats v) -> {
                var norm = TableStatsNormalizer.normalize(v);
                var sha = TableStatsNormalizer.sha256Hex(norm.toByteArray());
                return new TableStatsKey(
                    v.getTableId().getAccountId(), v.getTableId().getId(), v.getSnapshotId(), sha);
              })
          .withCasBlobs();

  public static final ResourceSchema<ColumnStats, ColumnStatsKey> COLUMN_STATS =
      ResourceSchema.<ColumnStats, ColumnStatsKey>of(
              "column-stats",
              (ColumnStatsKey key) ->
                  Keys.snapshotColumnStatsPointer(
                      key.accountId(), key.tableId(), key.snapshotId(), key.columnId()),
              (ColumnStatsKey key) ->
                  Keys.snapshotColumnStatsBlobUri(
                      key.accountId(), key.tableId(), key.columnId(), key.sha256()),
              (ColumnStats v) -> Map.of(),
              (ColumnStats v) -> {
                var norm = ColumnStatsNormalizer.normalize(v);
                var sha = ColumnStatsNormalizer.sha256Hex(norm.toByteArray());
                return new ColumnStatsKey(
                    v.getTableId().getAccountId(),
                    v.getTableId().getId(),
                    v.getSnapshotId(),
                    v.getColumnId(),
                    sha);
              })
          .withCasBlobs();

  public static final ResourceSchema<FileColumnStats, FileColumnStatsKey> FILE_COLUMN_STATS =
      ResourceSchema.<FileColumnStats, FileColumnStatsKey>of(
              "file-column-stats",
              (FileColumnStatsKey key) ->
                  Keys.snapshotFileStatsPointer(
                      key.accountId(), key.tableId(), key.snapshotId(), key.filePath()),
              (FileColumnStatsKey key) ->
                  Keys.snapshotFileStatsBlobUri(
                      key.accountId(), key.tableId(), key.filePath(), key.sha256()),
              v -> Map.of(),
              v -> {
                var bytes = v.toByteArray();
                var sha = ColumnStatsNormalizer.sha256Hex(bytes);
                return new FileColumnStatsKey(
                    v.getTableId().getAccountId(),
                    v.getTableId().getId(),
                    v.getSnapshotId(),
                    v.getFilePath(),
                    sha);
              })
          .withCasBlobs();

  public static final ResourceSchema<View, ViewKey> VIEW =
      ResourceSchema.<View, ViewKey>of(
              "view",
              key -> Keys.viewPointerById(key.accountId(), key.viewId()),
              key -> Keys.viewBlobUri(key.accountId(), key.viewId(), key.sha256()),
              v ->
                  Map.of(
                      "byName",
                      Keys.viewPointerByName(
                          v.getResourceId().getAccountId(),
                          v.getCatalogId().getId(),
                          v.getNamespaceId().getId(),
                          v.getDisplayName())),
              v -> {
                var sha = ResourceHash.sha256Hex(v.toByteArray());
                return new ViewKey(
                    v.getResourceId().getAccountId(), v.getResourceId().getId(), sha);
              })
          .withCasBlobs();

  public static final ResourceSchema<Connector, ConnectorKey> CONNECTOR =
      ResourceSchema.<Connector, ConnectorKey>of(
              "connector",
              key -> Keys.connectorPointerById(key.accountId(), key.connectorId()),
              key -> Keys.connectorBlobUri(key.accountId(), key.connectorId(), key.sha256()),
              v ->
                  Map.of(
                      "byName",
                      Keys.connectorPointerByName(
                          v.getResourceId().getAccountId(), v.getDisplayName())),
              v -> {
                var sha = ResourceHash.sha256Hex(v.toByteArray());
                return new ConnectorKey(
                    v.getResourceId().getAccountId(), v.getResourceId().getId(), sha);
              })
          .withCasBlobs();
}
