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
import ai.floedb.metacat.catalog.rpc.UpstreamRef;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.connector.rpc.Connector;
import ai.floedb.metacat.connector.rpc.ConnectorState;
import ai.floedb.metacat.connector.rpc.GetConnectorRequest;
import ai.floedb.metacat.connector.spi.ConnectorConfigMapper;
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
import java.util.Set;

@ApplicationScoped
public class ReconcilerService {
  @Inject GrpcClients clients;

  public Result reconcile(ResourceId connectorId, boolean fullRescan) {
    long scanned = 0;
    long changed = 0;
    long errors = 0;
    final java.util.ArrayList<String> errSummaries = new java.util.ArrayList<>();

    final Connector stored;
    try {
      stored =
          clients
              .connector()
              .getConnector(GetConnectorRequest.newBuilder().setConnectorId(connectorId).build())
              .getConnector();
    } catch (StatusRuntimeException e) {
      return new Result(
          0, 0, 1, new IllegalArgumentException("Connector not found: " + connectorId.getId(), e));
    }

    if (stored.getState() != ConnectorState.CS_ACTIVE) {
      return new Result(
          0, 0, 1, new IllegalStateException("Connector not ACTIVE: " + connectorId.getId()));
    }

    var cfg = ConnectorConfigMapper.fromProto(stored);

    try (MetacatConnector connector = ConnectorFactory.create(cfg)) {
      var catalogId = ensureCatalog(stored.getDestinationCatalogDisplayName(), connector);

      final List<List<String>> destNsPaths =
          (cfg.destinationNamespacePaths() == null) ? List.of() : cfg.destinationNamespacePaths();
      final String destTable = nullIfBlank(cfg.destinationTableDisplayName());
      final Set<String> destColumns = normalizeSelectors(cfg.destinationTableColumns());

      final List<String> namespaces =
          destNsPaths.isEmpty()
              ? connector.listNamespaces()
              : destNsPaths.stream().map(ReconcilerService::fq).toList();

      for (String namespaceFq : namespaces) {
        final ResourceId namespaceId;
        try {
          namespaceId = ensureNamespace(catalogId, namespaceFq);
        } catch (Exception e) {
          errors++;
          var msg = rootCauseMessage(e);
          errSummaries.add(msg);
          continue;
        }

        var tables = (destTable != null) ? List.of(destTable) : connector.listTables(namespaceFq);
        for (String tbl : tables) {
          scanned++;
          try {
            var upstream = connector.describe(namespaceFq, tbl);
            var effective = overrideDisplay(upstream, namespaceFq, destTable);

            var tableId =
                ensureTable(
                    catalogId,
                    namespaceId,
                    effective,
                    connector.format(),
                    stored.getResourceId(),
                    cfg.uri());

            var bundles =
                connector.enumerateSnapshotsWithStats(
                    namespaceFq, upstream.tableName(), tableId, destColumns);

            ingestAllSnapshotsAndStatsFiltered(tableId, bundles, destColumns);

            changed++;

          } catch (Exception e) {
            errors++;
            var msg = rootCauseMessage(e);
            errSummaries.add(msg);
          }
        }
      }

      if (errors == 0) {
        return new Result(scanned, changed, 0, null);
      } else {
        int limit = Math.min(5, errSummaries.size());
        String summary =
            "Partial failure ("
                + errors
                + "): "
                + String.join(" | ", errSummaries.subList(0, limit));
        return new Result(scanned, changed, errors, new RuntimeException(summary));
      }

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
      ConnectorFormat format,
      ResourceId connectorRid,
      String connectorUri) {

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

      maybeUpdateTable(tableId, upstreamTable, format, connectorRid, connectorUri);
      return tableId;

    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() != Status.Code.NOT_FOUND) throw e;
    }

    var upstream =
        UpstreamRef.newBuilder()
            .setConnectorId(connectorRid)
            .setUri(connectorUri)
            .addAllNamespacePath(List.of(upstreamTable.namespaceFq().split("\\.")))
            .setTableDisplayName(upstreamTable.tableName())
            .setFormat(toTableFormat(format))
            .addAllPartitionKeys(upstreamTable.partitionKeys())
            .build();

    var spec =
        TableSpec.newBuilder()
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setDisplayName(upstreamTable.tableName())
            .setSchemaJson(upstreamTable.schemaJson())
            .setUpstream(upstream)
            .putAllProperties(upstreamTable.properties())
            .build();

    return clients
        .table()
        .createTable(CreateTableRequest.newBuilder().setSpec(spec).build())
        .getTable()
        .getResourceId();
  }

  private void maybeUpdateTable(
      ResourceId tableId,
      MetacatConnector.TableDescriptor upstreamTable,
      ConnectorFormat format,
      ResourceId connectorRid,
      String connectorUri) {

    var upstream =
        UpstreamRef.newBuilder()
            .setConnectorId(connectorRid)
            .setUri(connectorUri)
            .addAllNamespacePath(List.of(upstreamTable.namespaceFq().split("\\.")))
            .setTableDisplayName(upstreamTable.tableName())
            .setFormat(toTableFormat(format))
            .addAllPartitionKeys(upstreamTable.partitionKeys())
            .build();

    var updated =
        TableSpec.newBuilder()
            .setSchemaJson(upstreamTable.schemaJson())
            .setUpstream(upstream)
            .putAllProperties(upstreamTable.properties())
            .build();

    var req = UpdateTableRequest.newBuilder().setTableId(tableId).setSpec(updated).build();

    try {
      clients.table().updateTable(req);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() != Status.Code.FAILED_PRECONDITION) throw e;
    }
  }

  private static Set<String> normalizeSelectors(List<String> in) {
    if (in == null || in.isEmpty()) return Set.of();
    var out = new java.util.LinkedHashSet<String>();
    for (var s : in) {
      if (s == null) continue;
      var t = s.trim();
      if (t.isEmpty()) continue;
      out.add(t.startsWith("#") ? "#" + t.substring(1).trim() : t);
    }
    return out;
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

  private static String nullIfBlank(String s) {
    return (s == null || s.isBlank()) ? null : s;
  }

  private MetacatConnector.TableDescriptor overrideDisplay(
      MetacatConnector.TableDescriptor upstream, String destNamespace, String destTable) {
    if (destNamespace == null && destTable == null) return upstream;
    return new MetacatConnector.TableDescriptor(
        destNamespace != null ? destNamespace : upstream.namespaceFq(),
        destTable != null ? destTable : upstream.tableName(),
        upstream.location(),
        upstream.schemaJson(),
        upstream.partitionKeys(),
        upstream.properties());
  }

  private void ingestAllSnapshotsAndStatsFiltered(
      ResourceId tableId,
      List<MetacatConnector.SnapshotBundle> bundles,
      Set<String> includeSelectors) {

    var seen = new HashSet<Long>();
    for (var snapshotBundle : bundles) {
      if (snapshotBundle == null) continue;
      long snapshotId = snapshotBundle.snapshotId();
      if (snapshotId <= 0 || !seen.add(snapshotId)) continue;

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
        List<ColumnStats> filtered =
            (includeSelectors == null || includeSelectors.isEmpty())
                ? cols
                : cols.stream().filter(c -> matchesSelector(c, includeSelectors)).toList();

        if (!filtered.isEmpty()) {
          int batchSize = 5;
          for (int i = 0; i < filtered.size(); i += batchSize) {
            var chunk =
                filtered.subList(i, Math.min(i + batchSize, filtered.size())).stream()
                    .map(c -> c.toBuilder().setTableId(tableId).setSnapshotId(snapshotId).build())
                    .toList();

            clients
                .statistics()
                .putColumnStatsBatch(
                    PutColumnStatsBatchRequest.newBuilder()
                        .setTableId(tableId)
                        .setSnapshotId(snapshotId)
                        .addAllColumns(chunk)
                        .build());
          }
        }
      }
    }
  }

  private static boolean matchesSelector(ColumnStats c, Set<String> selectors) {
    if (selectors == null || selectors.isEmpty()) return true;
    if (selectors.contains(c.getColumnName())) return true;
    if (!c.getColumnId().isBlank() && selectors.contains("#" + c.getColumnId())) {
      return true;
    }

    return false;
  }

  private static String rootCauseMessage(Throwable t) {
    Throwable r = t;
    while (r.getCause() != null) r = r.getCause();
    String m = r.getMessage();
    return (m == null || m.isBlank()) ? r.getClass().getSimpleName() : m;
  }

  private static String fq(List<String> segments) {
    return String.join(".", segments);
  }
}
