package ai.floedb.metacat.reconciler.impl;

import static ai.floedb.metacat.reconciler.util.NameParts.split;

import ai.floedb.metacat.catalog.rpc.*;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.connector.rpc.Connector;
import ai.floedb.metacat.connector.rpc.ConnectorSpec;
import ai.floedb.metacat.connector.rpc.ConnectorState;
import ai.floedb.metacat.connector.rpc.DestinationTarget;
import ai.floedb.metacat.connector.rpc.GetConnectorRequest;
import ai.floedb.metacat.connector.rpc.SourceSelector;
import ai.floedb.metacat.connector.rpc.UpdateConnectorRequest;
import ai.floedb.metacat.connector.spi.ConnectorConfigMapper;
import ai.floedb.metacat.connector.spi.ConnectorFactory;
import ai.floedb.metacat.connector.spi.ConnectorFormat;
import ai.floedb.metacat.connector.spi.MetacatConnector;
import com.google.protobuf.FieldMask;
import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@ApplicationScoped
public class ReconcilerService {
  @Inject GrpcClients clients;

  private static final int COLUMN_STATS_BATCH_SIZE = 5;

  public Result reconcile(ResourceId connectorId, boolean fullRescan) {
    long scanned = 0;
    long changed = 0;
    long errors = 0;

    final ArrayList<String> errSummaries = new ArrayList<>();

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

    DestinationTarget.Builder destB =
        stored.hasDestination()
            ? stored.getDestination().toBuilder()
            : DestinationTarget.newBuilder();

    FieldMask.Builder dmaskB = FieldMask.newBuilder();

    final SourceSelector source =
        stored.hasSource() ? stored.getSource() : SourceSelector.getDefaultInstance();
    final DestinationTarget dest =
        stored.hasDestination() ? stored.getDestination() : DestinationTarget.getDefaultInstance();

    var cfg = ConnectorConfigMapper.fromProto(stored);

    try (MetacatConnector connector = ConnectorFactory.create(cfg)) {
      final ResourceId destCatalogId = dest.getCatalogId();

      final String sourceNsFq;
      if (source.hasNamespace() && !source.getNamespace().getSegmentsList().isEmpty()) {
        sourceNsFq = fq(source.getNamespace().getSegmentsList());
      } else {
        return new Result(
            0, 0, 1, new IllegalArgumentException("connector.source.namespace is required"));
      }

      final String destNsFq =
          (dest.hasNamespace() && !dest.getNamespace().getSegmentsList().isEmpty())
              ? fq(dest.getNamespace().getSegmentsList())
              : sourceNsFq;

      final ResourceId destNamespaceId = ensureNamespace(destCatalogId, destNsFq);

      if (destB.hasNamespaceId()) {
        if (!destB.getNamespaceId().getId().equals(destNamespaceId.getId())) {
          return new Result(
              0,
              0,
              1,
              new IllegalStateException(
                  "Connector destination.namespace_id disagrees with resolved namespace "
                      + "(expected="
                      + destNsFq
                      + ", existingId="
                      + destB.getNamespaceId().getId()
                      + ")"));
        }
      } else {
        destB.setNamespaceId(destNamespaceId);
        destB.clearNamespace();
        dmaskB.addAllPaths(List.of("destination.namespace_id", "destination.namespace"));
      }

      final List<String> tables =
          (source.getTable() != null && !source.getTable().isBlank())
              ? List.of(source.getTable())
              : connector.listTables(sourceNsFq);

      if (tables.isEmpty()) {
        return new Result(
            scanned,
            changed,
            1,
            new IllegalStateException("No tables found in source namespace: " + sourceNsFq));
      }

      final boolean singleTableMode = tables.size() == 1;
      final Set<String> includeSelectors = normalizeSelectors(source.getColumnsList());

      final String tableDisplayHint =
          (dest.getTableDisplayName() != null && !dest.getTableDisplayName().isBlank())
              ? dest.getTableDisplayName()
              : null;

      for (String srcTable : tables) {
        scanned++;
        try {
          var upstream = connector.describe(sourceNsFq, srcTable);

          final String destTableDisplay =
              (tableDisplayHint != null) ? tableDisplayHint : upstream.tableName();

          var effective = overrideDisplay(upstream, destNsFq, destTableDisplay);

          var destTableId =
              ensureTable(
                  destCatalogId,
                  destNamespaceId,
                  effective,
                  connector.format(),
                  stored.getResourceId(),
                  cfg.uri(),
                  sourceNsFq,
                  srcTable);

          if (singleTableMode && !destB.hasTableId()) {
            destB.setTableId(destTableId);
            destB.clearTableDisplayName();
            dmaskB.addAllPaths(List.of("destination.table_id", "destination.table_display_name"));
          }

          var bundles =
              connector.enumerateSnapshotsWithStats(
                  sourceNsFq, srcTable, destTableId, includeSelectors);

          ingestAllSnapshotsAndStatsFiltered(destTableId, bundles, includeSelectors);
          changed++;
        } catch (Exception e) {
          errors++;
          errSummaries.add(
              "ns="
                  + destNsFq
                  + " table="
                  + sourceNsFq
                  + "."
                  + srcTable
                  + " : "
                  + rootCauseMessage(e));
        }
      }

      DestinationTarget updated = destB.build();
      FieldMask dMask = dmaskB.build();
      if (!updated.equals(stored.getDestination()) && dMask.getPathsCount() > 0) {
        try {
          clients
              .connector()
              .updateConnector(
                  UpdateConnectorRequest.newBuilder()
                      .setConnectorId(stored.getResourceId())
                      .setSpec(ConnectorSpec.newBuilder().setDestination(updated).build())
                      .setUpdateMask(dMask)
                      .build());
        } catch (StatusRuntimeException e) {
          errors++;
          errSummaries.add("updateConnector(destination): " + rootCauseMessage(e));
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
    return clients
        .namespace()
        .createNamespace(CreateNamespaceRequest.newBuilder().setSpec(spec).build())
        .getNamespace()
        .getResourceId();
  }

  private ResourceId ensureTable(
      ResourceId catalogId,
      ResourceId destNamespaceId,
      MetacatConnector.TableDescriptor landingView,
      ConnectorFormat format,
      ResourceId connectorRid,
      String connectorUri,
      String sourceNsFq,
      String sourceTable) {
    try {
      var nameRef =
          NameRef.newBuilder()
              .setCatalog(lookupCatalogName(catalogId))
              .addAllPath(List.of(landingView.namespaceFq().split("\\.")))
              .setName(landingView.tableName())
              .build();

      var tableId =
          clients
              .directory()
              .resolveTable(ResolveTableRequest.newBuilder().setRef(nameRef).build())
              .getResourceId();

      maybeUpdateTable(
          tableId, landingView, format, connectorRid, connectorUri, sourceNsFq, sourceTable);
      return tableId;

    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
        throw e;
      }
    }

    var upstream =
        UpstreamRef.newBuilder()
            .setConnectorId(connectorRid)
            .setUri(connectorUri)
            .addAllNamespacePath(List.of(sourceNsFq.split("\\.")))
            .setTableDisplayName(sourceTable)
            .setFormat(toTableFormat(format))
            .addAllPartitionKeys(landingView.partitionKeys())
            .build();

    var spec =
        TableSpec.newBuilder()
            .setCatalogId(catalogId)
            .setNamespaceId(destNamespaceId)
            .setDisplayName(landingView.tableName())
            .setSchemaJson(landingView.schemaJson())
            .setUpstream(upstream)
            .putAllProperties(landingView.properties())
            .build();

    return clients
        .table()
        .createTable(CreateTableRequest.newBuilder().setSpec(spec).build())
        .getTable()
        .getResourceId();
  }

  private void maybeUpdateTable(
      ResourceId tableId,
      MetacatConnector.TableDescriptor landingView,
      ConnectorFormat format,
      ResourceId connectorRid,
      String connectorUri,
      String sourceNsFq,
      String sourceTable) {

    var upstream =
        UpstreamRef.newBuilder()
            .setConnectorId(connectorRid)
            .setUri(connectorUri)
            .addAllNamespacePath(List.of(sourceNsFq.split("\\.")))
            .setTableDisplayName(sourceTable)
            .setFormat(toTableFormat(format))
            .addAllPartitionKeys(landingView.partitionKeys())
            .build();

    var updated =
        TableSpec.newBuilder()
            .setSchemaJson(landingView.schemaJson())
            .setUpstream(upstream)
            .putAllProperties(landingView.properties())
            .build();

    FieldMask mask =
        FieldMask.newBuilder()
            .addPaths("schema_json")
            .addPaths("upstream")
            .addPaths("properties")
            .build();
    var req =
        UpdateTableRequest.newBuilder()
            .setTableId(tableId)
            .setSpec(updated)
            .setUpdateMask(mask)
            .build();
    try {
      clients.table().updateTable(req);
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
      if (e.getStatus().getCode() == Status.Code.ALREADY_EXISTS) return;
      throw e;
    }
  }

  private void ingestAllSnapshotsAndStatsFiltered(
      ResourceId tableId,
      List<MetacatConnector.SnapshotBundle> bundles,
      Set<String> includeSelectors) {

    var seen = new HashSet<Long>();
    for (var snapshotBundle : bundles) {
      if (snapshotBundle == null) continue;

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
        List<ColumnStats> filtered =
            (includeSelectors == null || includeSelectors.isEmpty())
                ? cols
                : cols.stream().filter(c -> matchesSelector(c, includeSelectors)).toList();

        if (!filtered.isEmpty()) {
          for (int i = 0; i < filtered.size(); i += COLUMN_STATS_BATCH_SIZE) {
            var chunk =
                filtered.subList(i, Math.min(i + COLUMN_STATS_BATCH_SIZE, filtered.size())).stream()
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

  private String lookupCatalogName(ResourceId catalogId) {
    return clients
        .directory()
        .lookupCatalog(LookupCatalogRequest.newBuilder().setResourceId(catalogId).build())
        .getDisplayName();
  }

  private static TableFormat toTableFormat(ConnectorFormat format) {
    if (format == null) return TableFormat.TF_UNSPECIFIED;
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

  private static boolean matchesSelector(ColumnStats c, Set<String> selectors) {
    if (selectors == null || selectors.isEmpty()) return true;
    if (selectors.contains(c.getColumnName())) return true;
    if (!c.getColumnId().isBlank() && selectors.contains("#" + c.getColumnId())) return true;
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

  public static final class Result {
    public final long scanned, changed, errors;
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
}
