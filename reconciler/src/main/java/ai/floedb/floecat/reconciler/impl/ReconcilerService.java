package ai.floedb.floecat.reconciler.impl;

import static ai.floedb.floecat.reconciler.util.NameParts.split;

import ai.floedb.floecat.catalog.rpc.ColumnStats;
import ai.floedb.floecat.catalog.rpc.CreateNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.CreateSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.CreateTableRequest;
import ai.floedb.floecat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.GetTableStatsRequest;
import ai.floedb.floecat.catalog.rpc.LookupCatalogRequest;
import ai.floedb.floecat.catalog.rpc.NamespaceSpec;
import ai.floedb.floecat.catalog.rpc.PutColumnStatsRequest;
import ai.floedb.floecat.catalog.rpc.PutFileColumnStatsRequest;
import ai.floedb.floecat.catalog.rpc.PutTableStatsRequest;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.ResolveTableRequest;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotSpec;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.catalog.rpc.UpdateSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.UpdateTableRequest;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorSpec;
import ai.floedb.floecat.connector.rpc.ConnectorState;
import ai.floedb.floecat.connector.rpc.DestinationTarget;
import ai.floedb.floecat.connector.rpc.GetConnectorRequest;
import ai.floedb.floecat.connector.rpc.SourceSelector;
import ai.floedb.floecat.connector.rpc.UpdateConnectorRequest;
import ai.floedb.floecat.connector.spi.ConnectorConfigMapper;
import ai.floedb.floecat.connector.spi.ConnectorFactory;
import ai.floedb.floecat.connector.spi.ConnectorFormat;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import com.google.protobuf.FieldMask;
import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.smallrye.mutiny.Multi;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

@ApplicationScoped
public class ReconcilerService {

  public enum CaptureMode {
    METADATA_ONLY,
    METADATA_AND_STATS
  }

  @Inject GrpcClients clients;

  public Result reconcile(ResourceId connectorId, boolean fullRescan, ReconcileScope scopeIn) {
    return reconcile(connectorId, fullRescan, scopeIn, CaptureMode.METADATA_AND_STATS);
  }

  public Result reconcile(
      ResourceId connectorId, boolean fullRescan, ReconcileScope scopeIn, CaptureMode captureMode) {
    ReconcileScope scope = scopeIn == null ? ReconcileScope.empty() : scopeIn;
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

    try (FloecatConnector connector = ConnectorFactory.create(cfg)) {
      final ResourceId destCatalogId = dest.getCatalogId();

      final String sourceNsFq;
      if (source.hasNamespace() && !source.getNamespace().getSegmentsList().isEmpty()) {
        sourceNsFq = fq(source.getNamespace().getSegmentsList());
      } else {
        return new Result(
            0, 0, 1, new IllegalArgumentException("connector.source.namespace is required"));
      }

      final String destNsFq;
      final ResourceId destNamespaceId;

      if (dest.hasNamespaceId()) {
        destNamespaceId = dest.getNamespaceId();
        destNsFq = null;
      } else {
        destNsFq =
            (dest.hasNamespace() && !dest.getNamespace().getSegmentsList().isEmpty())
                ? fq(dest.getNamespace().getSegmentsList())
                : sourceNsFq;

        destNamespaceId = ensureNamespace(destCatalogId, destNsFq);
      }

      String scopeNamespaceFq = destNsFq != null ? destNsFq : sourceNsFq;
      if (!scope.matchesNamespace(scopeNamespaceFq)) {
        return new Result(
            0,
            0,
            1,
            new IllegalArgumentException(
                "Connector destination namespace "
                    + scopeNamespaceFq
                    + " does not match requested scope"));
      }

      if (!destB.hasNamespaceId()) {
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

      boolean matchedScope = false;
      for (String srcTable : tables) {
        try {
          var upstream = connector.describe(sourceNsFq, srcTable);

          final String destTableDisplay =
              (tableDisplayHint != null) ? tableDisplayHint : upstream.tableName();

          if (!scope.acceptsTable(scopeNamespaceFq, destTableDisplay)) {
            continue;
          }

          matchedScope = true;
          scanned++;

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

          boolean includeStats = captureMode == CaptureMode.METADATA_AND_STATS;
          var bundles =
              connector.enumerateSnapshotsWithStats(
                  sourceNsFq, srcTable, destTableId, includeSelectors, includeStats);

          ingestAllSnapshotsAndStatsFiltered(
              destTableId, connector, bundles, includeSelectors, includeStats);
          changed++;
        } catch (Exception e) {
          errors++;
          e.printStackTrace();
          errSummaries.add(
              "ns="
                  + scopeNamespaceFq
                  + " table="
                  + sourceNsFq
                  + "."
                  + srcTable
                  + " : "
                  + rootCauseMessage(e));
        }
      }

      if (!matchedScope && scope.hasTableFilter()) {
        return new Result(
            0,
            0,
            1,
            new IllegalArgumentException(
                "No tables matched scope: " + scope.destinationTableDisplayName()));
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
      FloecatConnector.TableDescriptor landingView,
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
            .putAllProperties(landingView.properties());

    return clients
        .table()
        .createTable(CreateTableRequest.newBuilder().setSpec(spec.build()).build())
        .getTable()
        .getResourceId();
  }

  private void maybeUpdateTable(
      ResourceId tableId,
      FloecatConnector.TableDescriptor landingView,
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
            .putAllProperties(landingView.properties());

    FieldMask mask =
        FieldMask.newBuilder()
            .addPaths("schema_json")
            .addPaths("upstream")
            .addPaths("properties")
            .build();
    var req =
        UpdateTableRequest.newBuilder()
            .setTableId(tableId)
            .setSpec(updated.build())
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
      ResourceId tableId,
      FloecatConnector connector,
      FloecatConnector.SnapshotBundle snapshotBundle) {
    if (snapshotBundle == null) {
      return;
    }
    long upstreamTsMs =
        snapshotBundle.upstreamCreatedAtMs() > 0
            ? snapshotBundle.upstreamCreatedAtMs()
            : System.currentTimeMillis();
    SnapshotSpec.Builder spec =
        SnapshotSpec.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotBundle.snapshotId())
            .setParentSnapshotId(snapshotBundle.parentId())
            .setUpstreamCreatedAt(Timestamps.fromMillis(upstreamTsMs))
            .setIngestedAt(Timestamps.fromMillis(System.currentTimeMillis()));
    FieldMask.Builder mask = FieldMask.newBuilder().addPaths("upstream_created_at");
    if (snapshotBundle.parentId() > 0) {
      mask.addPaths("parent_snapshot_id");
    }
    if (snapshotBundle.schemaJson() != null && !snapshotBundle.schemaJson().isBlank()) {
      spec.setSchemaJson(snapshotBundle.schemaJson());
      mask.addPaths("schema_json");
    }
    if (snapshotBundle.partitionSpec() != null) {
      spec.setPartitionSpec(snapshotBundle.partitionSpec());
      mask.addPaths("partition_spec");
    }
    if (snapshotBundle.sequenceNumber() > 0) {
      spec.setSequenceNumber(snapshotBundle.sequenceNumber());
      mask.addPaths("sequence_number");
    }
    if (snapshotBundle.manifestList() != null && !snapshotBundle.manifestList().isBlank()) {
      spec.setManifestList(snapshotBundle.manifestList());
      mask.addPaths("manifest_list");
    }
    Snapshot existingSnapshot = fetchSnapshot(tableId, snapshotBundle.snapshotId());
    if (snapshotBundle.summary() != null && !snapshotBundle.summary().isEmpty()) {
      LinkedHashMap<String, String> mergedSummary = new LinkedHashMap<>(snapshotBundle.summary());
      if (existingSnapshot != null && !existingSnapshot.getSummaryMap().isEmpty()) {
        existingSnapshot
            .getSummaryMap()
            .forEach((key, value) -> mergedSummary.putIfAbsent(key, value));
      }
      spec.putAllSummary(mergedSummary);
      mask.addPaths("summary");
    }
    if (snapshotBundle.schemaId() > 0) {
      spec.setSchemaId(snapshotBundle.schemaId());
      mask.addPaths("schema_id");
    }
    if (snapshotBundle.metadata() != null && !snapshotBundle.metadata().isEmpty()) {
      spec.putAllFormatMetadata(snapshotBundle.metadata());
      mask.addPaths("format_metadata");
    }
    SnapshotSpec snapshotSpec = spec.build();
    boolean exists = existingSnapshot != null;
    if (!exists) {
      var request = CreateSnapshotRequest.newBuilder().setSpec(snapshotSpec).build();
      clients.snapshot().createSnapshot(request);
    } else {
      var updateMask = mask.build();
      if (updateMask.getPathsCount() > 0) {
        var updateReq =
            UpdateSnapshotRequest.newBuilder()
                .setSpec(snapshotSpec)
                .setUpdateMask(updateMask)
                .build();
        clients.snapshot().updateSnapshot(updateReq);
      }
    }
  }

  private void ingestAllSnapshotsAndStatsFiltered(
      ResourceId tableId,
      FloecatConnector connector,
      List<FloecatConnector.SnapshotBundle> bundles,
      Set<String> includeSelectors,
      boolean includeStats) {

    var seen = new HashSet<Long>();
    for (var snapshotBundle : bundles) {
      if (snapshotBundle == null) {
        continue;
      }

      long snapshotId = snapshotBundle.snapshotId();
      if (snapshotId < 0 || !seen.add(snapshotId)) {
        continue;
      }

      ensureSnapshot(tableId, connector, snapshotBundle);

      if (includeStats) {
        if (statsAlreadyCaptured(tableId, snapshotId)) {
          continue;
        }

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
            List<PutColumnStatsRequest> columnRequests = new ArrayList<>();
            for (var c : filtered) {
              columnRequests.add(
                  PutColumnStatsRequest.newBuilder()
                      .setTableId(tableId)
                      .setSnapshotId(snapshotId)
                      .addColumns(
                          c.toBuilder().setTableId(tableId).setSnapshotId(snapshotId).build())
                      .build());
            }
            clients
                .statisticsMutiny()
                .putColumnStats(Multi.createFrom().iterable(columnRequests))
                .await()
                .atMost(Duration.ofMinutes(1));
          }
        }

        var fileCols = snapshotBundle.fileStats();
        if (fileCols != null && !fileCols.isEmpty()) {
          List<PutFileColumnStatsRequest> fileRequests = new ArrayList<>();
          for (var f : fileCols) {
            fileRequests.add(
                PutFileColumnStatsRequest.newBuilder()
                    .setTableId(tableId)
                    .setSnapshotId(snapshotId)
                    .addFiles(f.toBuilder().setTableId(tableId).setSnapshotId(snapshotId).build())
                    .build());
          }
          clients
              .statisticsMutiny()
              .putFileColumnStats(Multi.createFrom().iterable(fileRequests))
              .await()
              .atMost(Duration.ofMinutes(1));
        }
      }
    }
  }

  private boolean statsAlreadyCaptured(ResourceId tableId, long snapshotId) {
    if (snapshotId <= 0) {
      return false;
    }

    try {
      var response =
          clients
              .statistics()
              .getTableStats(
                  GetTableStatsRequest.newBuilder()
                      .setTableId(tableId)
                      .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapshotId).build())
                      .build());
      var stats = response.getStats();
      return stats.hasTableId() && stats.getSnapshotId() == snapshotId;
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        return false;
      }
      throw e;
    }
  }

  private Snapshot fetchSnapshot(ResourceId tableId, long snapshotId) {
    if (snapshotId <= 0) {
      return null;
    }
    try {
      var response =
          clients
              .snapshot()
              .getSnapshot(
                  GetSnapshotRequest.newBuilder()
                      .setTableId(tableId)
                      .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapshotId))
                      .build());
      if (response == null || !response.hasSnapshot()) {
        return null;
      }
      return response.getSnapshot();
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        return null;
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

  private FloecatConnector.TableDescriptor overrideDisplay(
      FloecatConnector.TableDescriptor upstream, String destNamespace, String destTable) {
    if (destNamespace == null && destTable == null) {
      return upstream;
    }

    return new FloecatConnector.TableDescriptor(
        destNamespace != null ? destNamespace : upstream.namespaceFq(),
        destTable != null ? destTable : upstream.tableName(),
        upstream.location(),
        upstream.schemaJson(),
        upstream.partitionKeys(),
        upstream.properties());
  }

  private static boolean matchesSelector(ColumnStats c, Set<String> selectors) {
    if (selectors == null || selectors.isEmpty()) {
      return true;
    }

    if (selectors.contains(c.getColumnName())) {
      return true;
    }

    if (selectors.contains("#" + c.getColumnId())) {
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

  private static Set<String> normalizeSelectors(List<String> in) {
    if (in == null || in.isEmpty()) {
      return Set.of();
    }

    var out = new LinkedHashSet<String>();
    for (var s : in) {
      if (s == null) {
        continue;
      }

      var t = s.trim();
      if (t.isEmpty()) {
        continue;
      }

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
