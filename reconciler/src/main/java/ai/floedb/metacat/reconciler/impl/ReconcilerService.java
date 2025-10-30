package ai.floedb.metacat.reconciler.impl;

import static ai.floedb.metacat.reconciler.util.NameParts.split;

import ai.floedb.metacat.catalog.rpc.CatalogSpec;
import ai.floedb.metacat.catalog.rpc.ColumnStats;
import ai.floedb.metacat.catalog.rpc.CreateCatalogRequest;
import ai.floedb.metacat.catalog.rpc.CreateNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.CreateSnapshotRequest;
import ai.floedb.metacat.catalog.rpc.CreateTableRequest;
import ai.floedb.metacat.catalog.rpc.LookupCatalogRequest;
import ai.floedb.metacat.catalog.rpc.NamespaceSpec;
import ai.floedb.metacat.catalog.rpc.PutColumnStatsBatchRequest;
import ai.floedb.metacat.catalog.rpc.PutTableStatsRequest;
import ai.floedb.metacat.catalog.rpc.ResolveCatalogRequest;
import ai.floedb.metacat.catalog.rpc.ResolveNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.ResolveTableRequest;
import ai.floedb.metacat.catalog.rpc.SnapshotSpec;
import ai.floedb.metacat.catalog.rpc.TableFormat;
import ai.floedb.metacat.catalog.rpc.TableSpec;
import ai.floedb.metacat.catalog.rpc.UpdateTableRequest;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.connector.spi.ConnectorConfig;
import ai.floedb.metacat.connector.spi.ConnectorFactory;
import ai.floedb.metacat.connector.spi.ConnectorFormat;
import ai.floedb.metacat.connector.spi.MetacatConnector;
import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.HashSet;
import java.util.List;

@ApplicationScoped
public class ReconcilerService {
  @Inject GrpcClients clients;

  public Result reconcile(ConnectorConfig cfg, boolean fullRescan) throws Exception {
    long scanned = 0;
    long changed = 0;
    long errors = 0;

    try (MetacatConnector connector = ConnectorFactory.create(cfg)) {
      var catalogId = ensureCatalog(cfg.targetCatalogDisplayName(), connector);
      for (String namespace : connector.listNamespaces()) {
        var namespaceId = ensureNamespace(catalogId, namespace);
        for (String tbl : connector.listTables(namespace)) {
          try {
            scanned++;
            var upstreamTable = connector.describe(namespace, tbl);
            var tableId = ensureTable(catalogId, namespaceId, upstreamTable, connector.format());
            ingestAllSnapshotsAndStats(
                tableId, connector.enumerateSnapshotsWithStats(namespace, tbl, tableId));
            changed++;
          } catch (Exception te) {
            te.printStackTrace();
            errors++;
          }
        }
      }
      return errors == 0
          ? new Result(scanned, changed, 0, null)
          : new Result(
              scanned, changed, errors, new RuntimeException("Partial failure: " + errors));
    } catch (Exception e) {
      return new Result(scanned, changed, errors, e);
    }
  }

  public static final class Result {
    public final long scanned;
    public final long changed;
    public final long errors;
    public final Exception error;

    public Result(long scanned, long changed, long errors, Exception error) {
      this.scanned = scanned;
      this.changed = changed;
      this.errors = errors;
      this.error = error;
    }

    public boolean ok() {
      return error == null;
    }

    public String message() {
      return ok() ? "OK" : error.getMessage();
    }
  }

  private ResourceId ensureCatalog(String displayName, MetacatConnector connector) {
    try {
      var response =
          clients
              .directory()
              .resolveCatalog(
                  ResolveCatalogRequest.newBuilder()
                      .setRef(NameRef.newBuilder().setCatalog(displayName).build())
                      .build());
      return response.getResourceId();
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
        throw e;
      }
    }
    var request =
        CreateCatalogRequest.newBuilder()
            .setSpec(
                CatalogSpec.newBuilder()
                    .setDisplayName(displayName)
                    .setConnectorRef(connector.id())
                    .build())
            .build();
    return clients.catalog().createCatalog(request).getCatalog().getResourceId();
  }

  private ResourceId ensureNamespace(ResourceId catalogId, String namespaceFq) {
    var parts = split(namespaceFq);
    try {
      var nameRef =
          NameRef.newBuilder()
              .setCatalog(lookupCatalogName(catalogId))
              .addAllPath(parts.parents)
              .setName(parts.leaf)
              .build();
      return clients
          .directory()
          .resolveNamespace(ResolveNamespaceRequest.newBuilder().setRef(nameRef).build())
          .getResourceId();
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
        throw e;
      }
    }
    var spec =
        NamespaceSpec.newBuilder()
            .setCatalogId(catalogId)
            .setDisplayName(parts.leaf)
            .addAllPath(parts.parents)
            .build();
    var request = CreateNamespaceRequest.newBuilder().setSpec(spec).build();
    return clients.namespace().createNamespace(request).getNamespace().getResourceId();
  }

  private ResourceId ensureTable(
      ResourceId catalogId,
      ResourceId namespaceId,
      MetacatConnector.TableDescriptor upstreamTable,
      ConnectorFormat format) {
    try {
      var nameRef =
          NameRef.newBuilder()
              .setCatalog(lookupCatalogName(catalogId))
              .addAllPath(split(upstreamTable.namespaceFq()).parents)
              .setName(upstreamTable.tableName())
              .build();
      var tableId =
          clients
              .directory()
              .resolveTable(ResolveTableRequest.newBuilder().setRef(nameRef).build())
              .getResourceId();
      maybeUpdateTable(tableId, upstreamTable);
      return tableId;
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
        throw e;
      }
    }

    var spec =
        TableSpec.newBuilder()
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setDisplayName(upstreamTable.tableName())
            .setRootUri(upstreamTable.location())
            .setSchemaJson(upstreamTable.schemaJson())
            .setFormat(toTableFormat(format))
            .putAllProperties(upstreamTable.properties())
            .build();

    var request = CreateTableRequest.newBuilder().setSpec(spec).build();
    return clients.table().createTable(request).getTable().getResourceId();
  }

  private void maybeUpdateTable(
      ResourceId tableId, MetacatConnector.TableDescriptor upstreamTable) {
    TableSpec updated =
        TableSpec.newBuilder()
            .setSchemaJson(upstreamTable.schemaJson())
            .addAllPartitionKeys(upstreamTable.partitionKeys())
            .putAllProperties(upstreamTable.properties())
            .build();
    UpdateTableRequest request =
        UpdateTableRequest.newBuilder().setTableId(tableId).setSpec(updated).build();
    try {
      clients.table().updateTable(request);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() != Status.Code.FAILED_PRECONDITION) {
        throw e;
      }
    }
  }

  private void ensureSnapshot(
      ResourceId tableId, long snapshotId, long parentId, long upstreamTsMs) {
    var spec =
        SnapshotSpec.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setParentSnapshotId(parentId)
            .setUpstreamCreatedAt(Timestamps.fromMillis(upstreamTsMs))
            .setIngestedAt(Timestamps.fromMillis(System.currentTimeMillis()))
            .build();
    var request = CreateSnapshotRequest.newBuilder().setSpec(spec).build();
    try {
      clients.snapshot().createSnapshot(request);
    } catch (StatusRuntimeException e) {
      var statusCode = e.getStatus().getCode();
      if (statusCode == Status.Code.ALREADY_EXISTS) {
        return;
      }

      throw e;
    }
  }

  private String lookupCatalogName(ResourceId catalogId) {
    return clients
        .directory()
        .lookupCatalog(LookupCatalogRequest.newBuilder().setResourceId(catalogId).build())
        .getDisplayName();
  }

  private static TableFormat toTableFormat(ConnectorFormat format) {
    if (format == null) {
      return TableFormat.TF_UNSPECIFIED;
    }

    String name = format.name();
    int i = name.indexOf('_');
    String stem = (i >= 0 && i + 1 < name.length()) ? name.substring(i + 1) : name;

    String target = "TF_" + stem;

    try {
      return TableFormat.valueOf(target);
    } catch (IllegalArgumentException ignored) {
      return TableFormat.TF_UNKNOWN;
    }
  }

  private void ingestAllSnapshotsAndStats(
      ResourceId tableId, List<MetacatConnector.SnapshotBundle> bundles) {

    var seen = new HashSet<Long>();

    for (var snapshotBundle : bundles) {
      if (snapshotBundle == null) {
        continue;
      }

      long snapshotId = snapshotBundle.snapshotId();
      if (snapshotId <= 0 || !seen.add(snapshotId)) {
        continue;
      }

      long parentId = snapshotBundle.parentId();

      long createdAtMs =
          (snapshotBundle.upstreamCreatedAtMs() > 0)
              ? snapshotBundle.upstreamCreatedAtMs()
              : System.currentTimeMillis();

      ensureSnapshot(tableId, snapshotId, parentId, createdAtMs);

      var tsIn = snapshotBundle.tableStats();
      if (tsIn != null) {

        var tStats = tsIn.toBuilder().setTableId(tableId).setSnapshotId(snapshotId).build();

        clients
            .statistics()
            .putTableStats(
                PutTableStatsRequest.newBuilder()
                    .setTableId(tableId)
                    .setSnapshotId(snapshotId)
                    .setStats(tStats)
                    .build());
      }

      var cols = snapshotBundle.columnStats();
      if (cols != null && !cols.isEmpty()) {
        var cStats =
            cols.stream()
                .map(c -> c.toBuilder().setTableId(tableId).setSnapshotId(snapshotId).build())
                .toList();

        int batchSize = 5;
        for (int i = 0; i < cStats.size(); i += batchSize) {
          List<ColumnStats> chunk = cStats.subList(i, Math.min(i + batchSize, cStats.size()));

          PutColumnStatsBatchRequest req =
              PutColumnStatsBatchRequest.newBuilder()
                  .setTableId(tableId)
                  .setSnapshotId(snapshotId)
                  .addAllColumns(chunk)
                  .build();

          clients.statistics().putColumnStatsBatch(req);
        }
      }
    }
  }
}
