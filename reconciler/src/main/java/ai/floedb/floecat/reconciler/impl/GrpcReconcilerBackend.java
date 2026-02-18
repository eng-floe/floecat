/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.reconciler.impl;

import ai.floedb.floecat.catalog.rpc.ColumnStats;
import ai.floedb.floecat.catalog.rpc.CreateNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.CreateSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.CreateTableRequest;
import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.GetNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.GetTableStatsRequest;
import ai.floedb.floecat.catalog.rpc.LookupCatalogRequest;
import ai.floedb.floecat.catalog.rpc.MutinyTableStatisticsServiceGrpc;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.NamespaceSpec;
import ai.floedb.floecat.catalog.rpc.PutColumnStatsRequest;
import ai.floedb.floecat.catalog.rpc.PutFileColumnStatsRequest;
import ai.floedb.floecat.catalog.rpc.PutTableStatsRequest;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.ResolveTableRequest;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.floecat.catalog.rpc.SnapshotSpec;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.catalog.rpc.TableStatisticsServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableStats;
import ai.floedb.floecat.catalog.rpc.UpdateSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.UpdateTableRequest;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorSpec;
import ai.floedb.floecat.connector.rpc.ConnectorsGrpc;
import ai.floedb.floecat.connector.rpc.DestinationTarget;
import ai.floedb.floecat.connector.spi.ConnectorFormat;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.reconciler.spi.NameRefNormalizer;
import ai.floedb.floecat.reconciler.spi.ReconcileContext;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend.TableSpecDescriptor;
import ai.floedb.floecat.reconciler.spi.SnapshotHelpers;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Timestamp;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;
import io.quarkus.arc.properties.IfBuildProperty;
import io.quarkus.grpc.GrpcClient;
import io.smallrye.mutiny.Multi;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@IfBuildProperty(name = "floecat.reconciler.backend", stringValue = "remote")
@ApplicationScoped
public class GrpcReconcilerBackend implements ReconcilerBackend {
  private static final Metadata.Key<String> AUTHORIZATION =
      Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> CORRELATION_ID =
      Metadata.Key.of("x-correlation-id", Metadata.ASCII_STRING_MARSHALLER);
  private static final Duration DEFAULT_STATS_TIMEOUT = Duration.ofMinutes(1);

  private final Optional<String> headerName;
  private final Optional<String> staticToken;
  private final Duration statsTimeout;

  public GrpcReconcilerBackend(
      @ConfigProperty(name = "floecat.reconciler.authorization.header") Optional<String> headerName,
      @ConfigProperty(name = "floecat.reconciler.authorization.token") Optional<String> staticToken,
      @ConfigProperty(name = "floecat.reconciler.stats.timeout") Optional<Duration> statsTimeout) {
    this.headerName = headerName.map(String::trim).filter(v -> !v.isBlank());
    this.staticToken = staticToken.map(String::trim).filter(v -> !v.isBlank());
    this.statsTimeout = statsTimeout.orElse(DEFAULT_STATS_TIMEOUT);
  }

  @GrpcClient("floecat")
  DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;

  @GrpcClient("floecat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;

  @GrpcClient("floecat")
  TableServiceGrpc.TableServiceBlockingStub table;

  @GrpcClient("floecat")
  SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshot;

  @GrpcClient("floecat")
  TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub statistics;

  @GrpcClient("floecat")
  MutinyTableStatisticsServiceGrpc.MutinyTableStatisticsServiceStub statisticsMutiny;

  @GrpcClient("floecat")
  ConnectorsGrpc.ConnectorsBlockingStub connector;

  @Override
  public ResourceId ensureNamespace(ReconcileContext ctx, ResourceId catalogId, NameRef namespace) {
    NameRef normalizedNamespace = NameRefNormalizer.normalize(namespace);
    try {
      return directory(ctx)
          .resolveNamespace(
              ResolveNamespaceRequest.newBuilder().setRef(normalizedNamespace).build())
          .getResourceId();
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
        throw e;
      }
    }
    String displayName = normalizedNamespace.getName();
    var spec =
        NamespaceSpec.newBuilder()
            .setCatalogId(catalogId)
            .setDisplayName(displayName)
            .addAllPath(normalizedNamespace.getPathList())
            .build();
    return namespace(ctx)
        .createNamespace(CreateNamespaceRequest.newBuilder().setSpec(spec).build())
        .getNamespace()
        .getResourceId();
  }

  @Override
  public ResourceId ensureTable(
      ReconcileContext ctx, ResourceId namespaceId, NameRef table, TableSpecDescriptor descriptor) {
    NameRef normalizedTable = NameRefNormalizer.normalize(table);
    try {
      ResourceId tableId =
          directory(ctx)
              .resolveTable(ResolveTableRequest.newBuilder().setRef(normalizedTable).build())
              .getResourceId();
      maybeUpdateTable(ctx, tableId, namespaceId, descriptor);
      return tableId;
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
        throw e;
      }
    }
    var namespaceResponse =
        namespace(ctx)
            .getNamespace(GetNamespaceRequest.newBuilder().setNamespaceId(namespaceId).build())
            .getNamespace();
    ResourceId catalogId = namespaceResponse.getCatalogId();

    var spec =
        TableSpec.newBuilder()
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setDisplayName(descriptor.displayName())
            .setSchemaJson(descriptor.schemaJson())
            .setUpstream(buildUpstream(descriptor))
            .putAllProperties(safeProperties(descriptor))
            .build();
    return table(ctx)
        .createTable(CreateTableRequest.newBuilder().setSpec(spec).build())
        .getTable()
        .getResourceId();
  }

  @Override
  public Optional<ResourceId> lookupTable(ReconcileContext ctx, NameRef table) {
    NameRef normalizedTable = NameRefNormalizer.normalize(table);
    try {
      return Optional.of(
          directory(ctx)
              .resolveTable(ResolveTableRequest.newBuilder().setRef(normalizedTable).build())
              .getResourceId());
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        return Optional.empty();
      }
      throw e;
    }
  }

  private void maybeUpdateTable(
      ReconcileContext ctx,
      ResourceId tableId,
      ResourceId namespaceId,
      TableSpecDescriptor descriptor) {
    Namespace ns =
        namespace(ctx)
            .getNamespace(GetNamespaceRequest.newBuilder().setNamespaceId(namespaceId).build())
            .getNamespace();
    ResourceId catalogId = ns.getCatalogId();

    var spec =
        TableSpec.newBuilder()
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setSchemaJson(descriptor.schemaJson())
            .setUpstream(buildUpstream(descriptor))
            .putAllProperties(safeProperties(descriptor))
            .build();

    var mask =
        FieldMask.newBuilder()
            .addPaths("schema_json")
            .addPaths("upstream")
            .addPaths("properties")
            .build();

    var request =
        UpdateTableRequest.newBuilder()
            .setTableId(tableId)
            .setSpec(spec)
            .setUpdateMask(mask)
            .build();
    try {
      table(ctx).updateTable(request);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() != Status.Code.FAILED_PRECONDITION) {
        throw e;
      }
    }
  }

  @Override
  public SnapshotPin snapshotPinFor(
      ReconcileContext ctx, ResourceId tableId, SnapshotRef ref, Optional<Timestamp> asOf) {
    if (ref != null) {
      switch (ref.getWhichCase()) {
        case SNAPSHOT_ID:
          return pin(tableId, ref.getSnapshotId(), null);
        case AS_OF:
          return pin(tableId, 0, ref.getAsOf());
        case SPECIAL:
          if (ref.getSpecial() != SpecialSnapshot.SS_CURRENT) {
            throw new IllegalArgumentException("unsupported special snapshot: " + ref.getSpecial());
          }
          return currentSnapshotPin(ctx, tableId);
        default:
          break;
      }
    }
    if (asOf.isPresent()) {
      return pin(tableId, 0, asOf.get());
    }
    return currentSnapshotPin(ctx, tableId);
  }

  @Override
  public Optional<Snapshot> fetchSnapshot(
      ReconcileContext ctx, ResourceId tableId, long snapshotId) {
    if (snapshotId <= 0) {
      return Optional.empty();
    }
    try {
      return Optional.of(
          snapshot(ctx)
              .getSnapshot(
                  GetSnapshotRequest.newBuilder()
                      .setTableId(tableId)
                      .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapshotId))
                      .build())
              .getSnapshot());
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        return Optional.empty();
      }
      throw e;
    }
  }

  @Override
  public void ingestSnapshot(ReconcileContext ctx, ResourceId tableId, Snapshot snapshot) {
    if (snapshot == null || snapshot.getSnapshotId() <= 0) {
      return;
    }
    var spec = buildSnapshotSpec(snapshot);
    try {
      snapshot(ctx).createSnapshot(CreateSnapshotRequest.newBuilder().setSpec(spec).build());
      return;
    } catch (StatusRuntimeException e) {
      Status.Code code = e.getStatus().getCode();
      if (code != Status.Code.ABORTED && code != Status.Code.ALREADY_EXISTS) {
        throw e;
      }
    }

    var mask =
        FieldMask.newBuilder()
            .addPaths("upstream_created_at")
            .addPaths("ingested_at")
            .addPaths("parent_snapshot_id")
            .addPaths("schema_json")
            .addPaths("partition_spec")
            .addPaths("sequence_number")
            .addPaths("manifest_list")
            .addPaths("summary")
            .addPaths("schema_id")
            .addPaths("format_metadata")
            .build();
    var update = UpdateSnapshotRequest.newBuilder().setSpec(spec).setUpdateMask(mask).build();
    Optional<Snapshot> existing = fetchSnapshot(ctx, tableId, snapshot.getSnapshotId());
    if (existing.isPresent() && SnapshotHelpers.equalsIgnoringIngested(snapshot, existing.get())) {
      return;
    }
    try {
      snapshot(ctx).updateSnapshot(update);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
        throw e;
      }
      snapshot(ctx).createSnapshot(CreateSnapshotRequest.newBuilder().setSpec(spec).build());
    }
  }

  @Override
  public boolean statsAlreadyCaptured(ReconcileContext ctx, ResourceId tableId, long snapshotId) {
    try {
      var response =
          statistics(ctx)
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

  @Override
  public void putTableStats(ReconcileContext ctx, ResourceId tableId, TableStats stats) {
    statistics(ctx)
        .putTableStats(
            PutTableStatsRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshotId(stats.getSnapshotId())
                .setStats(stats)
                .build());
  }

  @Override
  public void putColumnStats(ReconcileContext ctx, List<ColumnStats> stats) {
    if (stats == null || stats.isEmpty()) {
      return;
    }
    statisticsMutiny(ctx)
        .putColumnStats(Multi.createFrom().iterable(groupColumnRequests(stats)))
        .await()
        .atMost(statsTimeout);
  }

  @Override
  public void putFileColumnStats(ReconcileContext ctx, List<FileColumnStats> stats) {
    if (stats == null || stats.isEmpty()) {
      return;
    }
    statisticsMutiny(ctx)
        .putFileColumnStats(Multi.createFrom().iterable(groupFileRequests(stats)))
        .await()
        .atMost(statsTimeout);
  }

  private List<PutColumnStatsRequest> groupColumnRequests(List<ColumnStats> stats) {
    Map<StatsGroupKey, List<ColumnStats>> grouped = new LinkedHashMap<>();
    for (ColumnStats column : stats) {
      StatsGroupKey key = new StatsGroupKey(column.getTableId(), column.getSnapshotId());
      grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(column);
    }
    List<PutColumnStatsRequest> requests = new ArrayList<>(grouped.size());
    for (var entry : grouped.entrySet()) {
      PutColumnStatsRequest.Builder builder =
          PutColumnStatsRequest.newBuilder()
              .setTableId(entry.getKey().tableId)
              .setSnapshotId(entry.getKey().snapshotId);
      entry.getValue().forEach(builder::addColumns);
      requests.add(builder.build());
    }
    return requests;
  }

  private List<PutFileColumnStatsRequest> groupFileRequests(List<FileColumnStats> stats) {
    Map<StatsGroupKey, List<FileColumnStats>> grouped = new LinkedHashMap<>();
    for (FileColumnStats fileStats : stats) {
      StatsGroupKey key = new StatsGroupKey(fileStats.getTableId(), fileStats.getSnapshotId());
      grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(fileStats);
    }
    List<PutFileColumnStatsRequest> requests = new ArrayList<>(grouped.size());
    for (var entry : grouped.entrySet()) {
      PutFileColumnStatsRequest.Builder builder =
          PutFileColumnStatsRequest.newBuilder()
              .setTableId(entry.getKey().tableId)
              .setSnapshotId(entry.getKey().snapshotId);
      entry.getValue().forEach(builder::addFiles);
      requests.add(builder.build());
    }
    return requests;
  }

  @Override
  public String lookupCatalogName(ReconcileContext ctx, ResourceId catalogId) {
    return directory(ctx)
        .lookupCatalog(LookupCatalogRequest.newBuilder().setResourceId(catalogId).build())
        .getDisplayName();
  }

  @Override
  public String resolveNamespaceFq(ReconcileContext ctx, ResourceId namespaceId) {
    Namespace ns =
        namespace(ctx)
            .getNamespace(GetNamespaceRequest.newBuilder().setNamespaceId(namespaceId).build())
            .getNamespace();
    var segments = new ArrayList<String>(ns.getParentsCount() + 1);
    segments.addAll(ns.getParentsList());
    if (!ns.getDisplayName().isBlank()) {
      segments.add(ns.getDisplayName());
    }
    return String.join(".", segments);
  }

  @Override
  public Connector lookupConnector(ReconcileContext ctx, ResourceId connectorId) {
    return connector(ctx)
        .getConnector(
            ai.floedb.floecat.connector.rpc.GetConnectorRequest.newBuilder()
                .setConnectorId(connectorId)
                .build())
        .getConnector();
  }

  @Override
  public void updateConnectorDestination(
      ReconcileContext ctx, ResourceId connectorId, DestinationTarget destination) {
    var spec = ConnectorSpec.newBuilder().setDestination(destination).build();
    var mask = FieldMask.newBuilder().addPaths("destination").build();
    connector(ctx)
        .updateConnector(
            ai.floedb.floecat.connector.rpc.UpdateConnectorRequest.newBuilder()
                .setConnectorId(connectorId)
                .setSpec(spec)
                .setUpdateMask(mask)
                .build());
  }

  private TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub statistics(
      ReconcileContext ctx) {
    return withHeaders(statistics, ctx);
  }

  private MutinyTableStatisticsServiceGrpc.MutinyTableStatisticsServiceStub statisticsMutiny(
      ReconcileContext ctx) {
    return withHeaders(statisticsMutiny, ctx);
  }

  private DirectoryServiceGrpc.DirectoryServiceBlockingStub directory(ReconcileContext ctx) {
    return withHeaders(directory, ctx);
  }

  private NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace(ReconcileContext ctx) {
    return withHeaders(namespace, ctx);
  }

  private TableServiceGrpc.TableServiceBlockingStub table(ReconcileContext ctx) {
    return withHeaders(table, ctx);
  }

  private SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshot(ReconcileContext ctx) {
    return withHeaders(snapshot, ctx);
  }

  private ConnectorsGrpc.ConnectorsBlockingStub connector(ReconcileContext ctx) {
    return withHeaders(connector, ctx);
  }

  private <T extends AbstractStub<T>> T withHeaders(T stub, ReconcileContext ctx) {
    return stub.withInterceptors(
        MetadataUtils.newAttachHeadersInterceptor(metadataForContext(ctx)));
  }

  Metadata metadataForContext(ReconcileContext ctx) {
    Metadata metadata = new Metadata();
    metadata.put(CORRELATION_ID, ctx.correlationId());
    Optional<String> token = ctx.authorizationToken();
    if (token.isEmpty()) {
      token = staticToken;
    }
    token.ifPresent(value -> metadata.put(authHeaderKey(), withBearerPrefix(value)));
    return metadata;
  }

  private Metadata.Key<String> authHeaderKey() {
    if (headerName.isPresent() && !"authorization".equalsIgnoreCase(headerName.get())) {
      return Metadata.Key.of(headerName.get(), Metadata.ASCII_STRING_MARSHALLER);
    }
    return AUTHORIZATION;
  }

  private String withBearerPrefix(String token) {
    if (token.regionMatches(true, 0, "bearer ", 0, 7)) {
      return token;
    }
    return "Bearer " + token;
  }

  private SnapshotPin currentSnapshotPin(ReconcileContext ctx, ResourceId tableId) {
    var response =
        snapshot(ctx)
            .getSnapshot(
                GetSnapshotRequest.newBuilder()
                    .setTableId(tableId)
                    .setSnapshot(
                        SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT).build())
                    .build());
    return pin(tableId, response.getSnapshot().getSnapshotId(), null);
  }

  private SnapshotPin pin(ResourceId tableId, long snapshotId, Timestamp asOf) {
    SnapshotPin.Builder builder = SnapshotPin.newBuilder().setTableId(tableId);
    if (snapshotId > 0) {
      builder.setSnapshotId(snapshotId);
    }
    if (asOf != null) {
      builder.setAsOf(asOf);
    }
    return builder.build();
  }

  private SnapshotSpec buildSnapshotSpec(Snapshot snapshot) {
    SnapshotSpec.Builder builder =
        SnapshotSpec.newBuilder()
            .setTableId(snapshot.getTableId())
            .setSnapshotId(snapshot.getSnapshotId())
            .setUpstreamCreatedAt(snapshot.getUpstreamCreatedAt())
            .setIngestedAt(snapshot.getIngestedAt())
            .setParentSnapshotId(snapshot.getParentSnapshotId());
    if (!snapshot.getSchemaJson().isBlank()) {
      builder.setSchemaJson(snapshot.getSchemaJson());
    }
    if (snapshot.hasPartitionSpec()) {
      builder.setPartitionSpec(snapshot.getPartitionSpec());
    }
    if (snapshot.getSequenceNumber() > 0) {
      builder.setSequenceNumber(snapshot.getSequenceNumber());
    }
    if (!snapshot.getManifestList().isBlank()) {
      builder.setManifestList(snapshot.getManifestList());
    }
    if (!snapshot.getSummaryMap().isEmpty()) {
      builder.putAllSummary(snapshot.getSummaryMap());
    }
    if (snapshot.getSchemaId() > 0) {
      builder.setSchemaId(snapshot.getSchemaId());
    }
    if (!snapshot.getFormatMetadataMap().isEmpty()) {
      builder.putAllFormatMetadata(snapshot.getFormatMetadataMap());
    }
    return builder.build();
  }

  private Map<String, String> safeProperties(TableSpecDescriptor descriptor) {
    return descriptor.properties() != null ? descriptor.properties() : Map.of();
  }

  private UpstreamRef buildUpstream(TableSpecDescriptor descriptor) {
    UpstreamRef.Builder builder =
        UpstreamRef.newBuilder()
            .setConnectorId(descriptor.connectorId())
            .setUri(descriptor.connectorUri())
            .setTableDisplayName(descriptor.sourceTable())
            .setFormat(toTableFormat(descriptor.connectorFormat()))
            .setColumnIdAlgorithm(descriptor.columnIdAlgorithm());
    if (descriptor.sourceNamespace() != null && !descriptor.sourceNamespace().isBlank()) {
      for (String seg : descriptor.sourceNamespace().split("\\.")) {
        if (!seg.isBlank()) {
          builder.addNamespacePath(seg);
        }
      }
    }
    if (descriptor.partitionKeys() != null) {
      builder.addAllPartitionKeys(descriptor.partitionKeys());
    }
    return builder.build();
  }

  private TableFormat toTableFormat(ConnectorFormat format) {
    if (format == null) {
      return TableFormat.TF_UNSPECIFIED;
    }
    String name = format.name();
    int idx = name.indexOf('_');
    String stem = idx >= 0 && idx + 1 < name.length() ? name.substring(idx + 1) : name;
    String target = "TF_" + stem;
    try {
      return TableFormat.valueOf(target);
    } catch (IllegalArgumentException ignored) {
      return TableFormat.TF_UNKNOWN;
    }
  }

  private static final class StatsGroupKey {
    private final ResourceId tableId;
    private final long snapshotId;

    private StatsGroupKey(ResourceId tableId, long snapshotId) {
      this.tableId = tableId;
      this.snapshotId = snapshotId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      StatsGroupKey that = (StatsGroupKey) o;
      return snapshotId == that.snapshotId && tableId.equals(that.tableId);
    }

    @Override
    public int hashCode() {
      int result = tableId.hashCode();
      result = 31 * result + Long.hashCode(snapshotId);
      return result;
    }
  }
}
