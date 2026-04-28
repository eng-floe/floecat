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

import ai.floedb.floecat.catalog.rpc.CreateNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.CreateSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.CreateTableRequest;
import ai.floedb.floecat.catalog.rpc.CreateViewRequest;
import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.GetIndexArtifactRequest;
import ai.floedb.floecat.catalog.rpc.GetNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.GetTableRequest;
import ai.floedb.floecat.catalog.rpc.GetTargetStatsRequest;
import ai.floedb.floecat.catalog.rpc.GetViewRequest;
import ai.floedb.floecat.catalog.rpc.IndexArtifactState;
import ai.floedb.floecat.catalog.rpc.IndexFileTarget;
import ai.floedb.floecat.catalog.rpc.IndexTarget;
import ai.floedb.floecat.catalog.rpc.ListSnapshotsRequest;
import ai.floedb.floecat.catalog.rpc.ListTargetStatsRequest;
import ai.floedb.floecat.catalog.rpc.LookupCatalogRequest;
import ai.floedb.floecat.catalog.rpc.MutinyTableIndexServiceGrpc;
import ai.floedb.floecat.catalog.rpc.MutinyTableStatisticsServiceGrpc;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.NamespaceSpec;
import ai.floedb.floecat.catalog.rpc.PutIndexArtifactItem;
import ai.floedb.floecat.catalog.rpc.PutIndexArtifactsRequest;
import ai.floedb.floecat.catalog.rpc.PutTableConstraintsRequest;
import ai.floedb.floecat.catalog.rpc.PutTargetStatsRequest;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.ResolveTableRequest;
import ai.floedb.floecat.catalog.rpc.ResolveViewRequest;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotConstraints;
import ai.floedb.floecat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.floecat.catalog.rpc.SnapshotSpec;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.StatsTargetKind;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableConstraintsServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.TableIndexServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.catalog.rpc.TableStatisticsServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.catalog.rpc.UpdateSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.UpdateTableRequest;
import ai.floedb.floecat.catalog.rpc.UpdateViewRequest;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.catalog.rpc.ViewServiceGrpc;
import ai.floedb.floecat.catalog.rpc.ViewSpec;
import ai.floedb.floecat.common.rpc.IdempotencyKey;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.connector.common.auth.CredentialResolverSupport;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorSpec;
import ai.floedb.floecat.connector.rpc.ConnectorsGrpc;
import ai.floedb.floecat.connector.rpc.DestinationTarget;
import ai.floedb.floecat.connector.spi.AuthResolutionContext;
import ai.floedb.floecat.connector.spi.ConnectorConfig;
import ai.floedb.floecat.connector.spi.ConnectorConfigMapper;
import ai.floedb.floecat.connector.spi.ConnectorFactory;
import ai.floedb.floecat.connector.spi.ConnectorFormat;
import ai.floedb.floecat.connector.spi.CredentialResolver;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.reconciler.spi.ColumnSelectorCoverage;
import ai.floedb.floecat.reconciler.spi.NameRefNormalizer;
import ai.floedb.floecat.reconciler.spi.ReconcileContext;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend.TableSpecDescriptor;
import ai.floedb.floecat.reconciler.spi.SnapshotHelpers;
import ai.floedb.floecat.reconciler.spi.capture.CaptureEngineRegistry;
import ai.floedb.floecat.reconciler.spi.capture.CaptureEngineRequest;
import ai.floedb.floecat.reconciler.spi.capture.CaptureEngineResult;
import ai.floedb.floecat.reconciler.spi.capture.PlannedFileGroupCaptureRequest;
import ai.floedb.floecat.types.Hashing;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Timestamp;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;
import io.quarkus.grpc.GrpcClient;
import io.smallrye.mutiny.Multi;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@ApplicationScoped
public class GrpcReconcilerBackend implements ReconcilerBackend {
  private static final Logger LOG = Logger.getLogger(GrpcReconcilerBackend.class);

  private static final Metadata.Key<String> AUTHORIZATION =
      Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> CORRELATION_ID =
      Metadata.Key.of("x-correlation-id", Metadata.ASCII_STRING_MARSHALLER);
  private static final Duration DEFAULT_STATS_TIMEOUT = Duration.ofMinutes(1);

  private final Optional<String> headerName;
  private final Optional<String> staticToken;
  private final Duration statsTimeout;
  ConnectorOpener connectorOpener = ConnectorFactory::create;
  @Inject CaptureEngineRegistry captureEngineRegistry;
  @Inject CredentialResolver credentialResolver;

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

  @GrpcClient("floecat")
  ViewServiceGrpc.ViewServiceBlockingStub view;

  @GrpcClient("floecat")
  TableConstraintsServiceGrpc.TableConstraintsServiceBlockingStub constraintsStub;

  @GrpcClient("floecat")
  TableIndexServiceGrpc.TableIndexServiceBlockingStub index;

  @GrpcClient("floecat")
  MutinyTableIndexServiceGrpc.MutinyTableIndexServiceStub indexMutiny;

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
  public Optional<ResourceId> lookupNamespace(ReconcileContext ctx, NameRef namespace) {
    NameRef normalizedNamespace = NameRefNormalizer.normalize(namespace);
    try {
      return Optional.of(
          directory(ctx)
              .resolveNamespace(
                  ResolveNamespaceRequest.newBuilder().setRef(normalizedNamespace).build())
              .getResourceId());
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        return Optional.empty();
      }
      throw e;
    }
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

  @Override
  public Optional<String> lookupTableDisplayName(ReconcileContext ctx, ResourceId tableId) {
    if (tableId == null || tableId.getId().isBlank()) {
      return Optional.empty();
    }
    try {
      return Optional.of(
          table(ctx)
              .getTable(GetTableRequest.newBuilder().setTableId(tableId).build())
              .getTable()
              .getDisplayName());
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        return Optional.empty();
      }
      throw e;
    }
  }

  @Override
  public Optional<DestinationTableMetadata> lookupDestinationTableMetadata(
      ReconcileContext ctx, ResourceId tableId) {
    if (tableId == null || tableId.getId().isBlank()) {
      return Optional.empty();
    }
    try {
      var resolved =
          table(ctx).getTable(GetTableRequest.newBuilder().setTableId(tableId).build()).getTable();
      return Optional.of(
          new DestinationTableMetadata(
              resolved.getCatalogId(),
              resolved.getNamespaceId(),
              resolved.getDisplayName(),
              sourceNamespace(resolved.hasUpstream() ? resolved.getUpstream() : null),
              sourceName(resolved.hasUpstream() ? resolved.getUpstream() : null),
              sourceConnectorId(resolved.hasUpstream() ? resolved.getUpstream() : null)));
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
    // Reconciler must not advance table core OCC version for existing tables.
    // Existing table shape updates are intentionally skipped here; only creation-on-miss
    // is allowed through ensureTable().
  }

  @Override
  public boolean updateTableById(
      ReconcileContext ctx,
      ResourceId tableId,
      ResourceId namespaceId,
      NameRef tableRef,
      TableSpecDescriptor descriptor) {
    var before =
        table(ctx).getTable(GetTableRequest.newBuilder().setTableId(tableId).build()).getTable();
    if (!before.getNamespaceId().equals(namespaceId)) {
      throw new IllegalArgumentException(
          "Destination table namespace mismatch for id: " + tableId.getId());
    }
    NameRef normalizedTable = NameRefNormalizer.normalize(tableRef);
    if (!before.getDisplayName().equals(normalizedTable.getName())) {
      throw new IllegalArgumentException(
          "Destination table display name mismatch for id: " + tableId.getId());
    }

    TableSpec spec =
        TableSpec.newBuilder()
            .setDisplayName(descriptor.displayName())
            .setSchemaJson(descriptor.schemaJson())
            .setUpstream(buildUpstream(descriptor))
            .putAllProperties(safeProperties(descriptor))
            .build();
    var response =
        table(ctx)
            .updateTable(
                UpdateTableRequest.newBuilder()
                    .setTableId(tableId)
                    .setSpec(spec)
                    .setUpdateMask(
                        FieldMask.newBuilder()
                            .addPaths("schema_json")
                            .addPaths("upstream")
                            .addPaths("properties")
                            .build())
                    .build())
            .getTable();
    return !response.equals(before);
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
    if (snapshotId < 0) {
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
  public Set<Long> existingSnapshotIds(ReconcileContext ctx, ResourceId tableId) {
    Set<Long> snapshotIds = new LinkedHashSet<>();
    String token = "";
    do {
      var response =
          snapshot(ctx)
              .listSnapshots(
                  ListSnapshotsRequest.newBuilder()
                      .setTableId(tableId)
                      .setPage(PageRequest.newBuilder().setPageSize(500).setPageToken(token))
                      .build());
      for (Snapshot snapshot : response.getSnapshotsList()) {
        if (snapshot.getSnapshotId() >= 0) {
          snapshotIds.add(snapshot.getSnapshotId());
        }
      }
      token = response.hasPage() ? response.getPage().getNextPageToken() : "";
    } while (!token.isBlank());
    return snapshotIds;
  }

  @Override
  public void ingestSnapshot(ReconcileContext ctx, ResourceId tableId, Snapshot snapshot) {
    if (snapshot == null || snapshot.getSnapshotId() < 0) {
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
  public Optional<FloecatConnector.SnapshotFilePlan> fetchSnapshotFilePlan(
      ReconcileContext ctx, ResourceId tableId, long snapshotId) {
    if (snapshotId < 0) {
      return Optional.empty();
    }
    return withSourceConnector(
        ctx,
        tableId,
        Optional.empty(),
        (source, sourceCtx) -> {
          Optional<FloecatConnector.SnapshotFilePlan> planned =
              source.planSnapshotFiles(
                  sourceCtx.sourceNamespace(), sourceCtx.sourceTable(), tableId, snapshotId);
          LOG.infof(
              "GrpcReconcilerBackend.fetchSnapshotFilePlan tableId=%s snapshotId=%d source=%s.%s present=%s dataFiles=%d deleteFiles=%d",
              tableId.getId(),
              snapshotId,
              sourceCtx.sourceNamespace(),
              sourceCtx.sourceTable(),
              planned.isPresent(),
              planned.map(plan -> plan.dataFiles().size()).orElse(0),
              planned.map(plan -> plan.deleteFiles().size()).orElse(0));
          return planned;
        });
  }

  @Override
  public CaptureEngineResult capturePlannedFileGroup(
      ReconcileContext ctx, PlannedFileGroupCaptureRequest request) {
    if (request == null
        || request.tableId() == null
        || request.plannedFilePaths() == null
        || request.plannedFilePaths().isEmpty()
        || request.snapshotId() < 0) {
      return CaptureEngineResult.empty();
    }
    if (captureEngineRegistry == null) {
      return CaptureEngineResult.empty();
    }
    return withSourceConnector(
        ctx,
        request.tableId(),
        CaptureEngineResult.empty(),
        (source, sourceCtx) -> {
          CaptureEngineResult capture =
              captureEngineRegistry.capture(
                  new CaptureEngineRequest(
                      lookupConnector(ctx, sourceCtx.connectorId()),
                      sourceCtx.sourceNamespace(),
                      sourceCtx.sourceTable(),
                      request.tableId(),
                      request.snapshotId(),
                      request.planId(),
                      request.groupId(),
                      request.plannedFilePaths(),
                      request.statsColumns(),
                      request.indexColumns(),
                      request.requestedStatsTargetKinds(),
                      request.capturePageIndex()));
          if (!request.capturePageIndex() || !capture.stagedIndexArtifacts().isEmpty()) {
            return capture;
          }
          return CaptureEngineResult.of(
              capture.statsRecords(),
              List.of(),
              FileGroupIndexArtifactStager.stage(
                  request.tableId(),
                  request.snapshotId(),
                  request.plannedFilePaths(),
                  capture.statsRecords(),
                  capture.pageIndexEntries()));
        });
  }

  private boolean hasAnyCapturedStats(ReconcileContext ctx, ResourceId tableId, long snapshotId) {
    try {
      var response =
          statistics(ctx).listTargetStats(buildStatsAlreadyCapturedRequest(tableId, snapshotId));
      return response != null && !response.getRecordsList().isEmpty();
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        return false;
      }
      throw e;
    }
  }

  @Override
  public boolean statsAlreadyCapturedForTargetKind(
      ReconcileContext ctx, ResourceId tableId, long snapshotId, StatsTargetKind targetKind) {
    if (targetKind == null || targetKind == StatsTargetKind.STK_UNSPECIFIED) {
      return hasAnyCapturedStats(ctx, tableId, snapshotId);
    }
    try {
      if (targetKind == StatsTargetKind.STK_TABLE) {
        var tableStatsResponse =
            statistics(ctx)
                .listTargetStats(
                    buildStatsAlreadyCapturedRequest(tableId, snapshotId).toBuilder()
                        .addTargetKinds(StatsTargetKind.STK_TABLE)
                        .build());
        if (tableStatsResponse == null || tableStatsResponse.getRecordsList().isEmpty()) {
          return false;
        }
        var tableRecord = tableStatsResponse.getRecords(0);
        if (!tableRecord.hasTable()) {
          return false;
        }
        if (tableRecord.getTable().getDataFileCount() <= 0) {
          return true;
        }
        var fileStatsResponse =
            statistics(ctx)
                .listTargetStats(
                    ListTargetStatsRequest.newBuilder()
                        .setTableId(tableId)
                        .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapshotId).build())
                        .setPage(PageRequest.newBuilder().setPageSize(1).build())
                        .addTargetKinds(StatsTargetKind.STK_FILE)
                        .build());
        return fileStatsResponse != null && !fileStatsResponse.getRecordsList().isEmpty();
      }

      var response =
          statistics(ctx)
              .listTargetStats(
                  buildStatsAlreadyCapturedRequest(tableId, snapshotId).toBuilder()
                      .addTargetKinds(targetKind)
                      .build());
      return response != null && !response.getRecordsList().isEmpty();
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        return false;
      }
      throw e;
    }
  }

  @Override
  public boolean statsCapturedForColumnSelectors(
      ReconcileContext ctx, ResourceId tableId, long snapshotId, Set<String> selectors) {
    ColumnSelectorCoverage.SelectorCoverage required = ColumnSelectorCoverage.parse(selectors);
    if (required.isUnsatisfiable()) {
      return false;
    }
    if (required.isEmpty()) {
      return statsAlreadyCapturedForTargetKind(
          ctx, tableId, snapshotId, StatsTargetKind.STK_COLUMN);
    }
    Set<Long> presentIds = new HashSet<>();
    Set<String> presentNames = new HashSet<>();
    String pageToken = "";
    final int pageSize = 256;
    try {
      do {
        ListTargetStatsRequest request =
            buildStatsAlreadyCapturedRequest(tableId, snapshotId).toBuilder()
                .clearTargetKinds()
                .addTargetKinds(StatsTargetKind.STK_COLUMN)
                .setPage(PageRequest.newBuilder().setPageSize(pageSize).setPageToken(pageToken))
                .build();
        var response = statistics(ctx).listTargetStats(request);
        if (response == null) {
          break;
        }
        for (TargetStatsRecord record : response.getRecordsList()) {
          ColumnSelectorCoverage.recordColumnCoverage(record, presentIds, presentNames);
        }
        if (required.isSatisfiedBy(presentIds, presentNames)) {
          return true;
        }
        pageToken = response.hasPage() ? response.getPage().getNextPageToken() : "";
      } while (pageToken != null && !pageToken.isBlank());
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        return false;
      }
      throw e;
    }
    return required.isSatisfiedBy(presentIds, presentNames);
  }

  @Override
  public boolean statsCapturedForTargets(
      ReconcileContext ctx, ResourceId tableId, long snapshotId, Set<StatsTarget> targets) {
    if (targets == null || targets.isEmpty()) {
      return true;
    }
    try {
      for (StatsTarget target : targets) {
        if (target == null || target.getTargetCase() == StatsTarget.TargetCase.TARGET_NOT_SET) {
          return false;
        }
        var response =
            statistics(ctx)
                .getTargetStats(
                    GetTargetStatsRequest.newBuilder()
                        .setTableId(tableId)
                        .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapshotId).build())
                        .setTarget(target)
                        .build());
        if (response == null || !response.hasStats()) {
          return false;
        }
      }
      return true;
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        return false;
      }
      throw e;
    }
  }

  @Override
  public boolean indexArtifactsCapturedForFilePaths(
      ReconcileContext ctx,
      ResourceId tableId,
      long snapshotId,
      List<String> filePaths,
      Set<String> selectors) {
    if (filePaths == null || filePaths.isEmpty()) {
      return true;
    }
    Set<String> normalizedSelectors = normalizeIndexSelectors(selectors);
    if (normalizedSelectors == null) {
      return false;
    }
    try {
      for (String filePath : filePaths) {
        if (filePath == null || filePath.isBlank()) {
          return false;
        }
        var response =
            index(ctx)
                .getIndexArtifact(
                    GetIndexArtifactRequest.newBuilder()
                        .setTableId(tableId)
                        .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapshotId).build())
                        .setTarget(
                            IndexTarget.newBuilder()
                                .setFile(IndexFileTarget.newBuilder().setFilePath(filePath).build())
                                .build())
                        .build());
        if (response == null
            || !response.hasRecord()
            || response.getRecord().getState() != IndexArtifactState.IAS_READY) {
          return false;
        }
        if (!normalizedSelectors.isEmpty()
            && !persistedIndexSelectors(response.getRecord()).containsAll(normalizedSelectors)) {
          return false;
        }
      }
      return true;
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        return false;
      }
      throw e;
    }
  }

  @Override
  public void putIndexArtifacts(ReconcileContext ctx, List<StagedIndexArtifact> artifacts) {
    if (artifacts == null || artifacts.isEmpty()) {
      return;
    }
    indexMutiny(ctx)
        .putIndexArtifacts(Multi.createFrom().iterable(groupIndexArtifactRequests(artifacts)))
        .await()
        .atMost(statsTimeout);
  }

  @Override
  public void putTargetStats(ReconcileContext ctx, List<TargetStatsRecord> stats) {
    if (stats == null || stats.isEmpty()) {
      return;
    }
    statisticsMutiny(ctx)
        .putTargetStats(Multi.createFrom().iterable(groupTargetRequests(stats)))
        .await()
        .atMost(statsTimeout);
  }

  @Override
  public boolean putSnapshotConstraints(
      ReconcileContext ctx,
      ResourceId tableId,
      long snapshotId,
      SnapshotConstraints snapshotConstraints) {
    if (snapshotConstraints == null || snapshotId < 0) {
      return false;
    }
    return constraintsStub(ctx)
        .putTableConstraints(
            buildPutTableConstraintsRequest(tableId, snapshotId, snapshotConstraints))
        .getChanged();
  }

  static PutTableConstraintsRequest buildPutTableConstraintsRequest(
      ResourceId tableId, long snapshotId, SnapshotConstraints snapshotConstraints) {
    String idempotencyKey =
        snapshotConstraintsIdempotencyKey(tableId, snapshotId, snapshotConstraints);
    return PutTableConstraintsRequest.newBuilder()
        .setTableId(tableId)
        .setSnapshotId(snapshotId)
        .setConstraints(snapshotConstraints)
        .setIdempotency(IdempotencyKey.newBuilder().setKey(idempotencyKey).build())
        .build();
  }

  static ListTargetStatsRequest buildStatsAlreadyCapturedRequest(
      ResourceId tableId, long snapshotId) {
    ResourceId canonicalId =
        tableId.getKind() == ResourceKind.RK_TABLE
            ? tableId
            : tableId.toBuilder().setKind(ResourceKind.RK_TABLE).build();
    return ListTargetStatsRequest.newBuilder()
        .setTableId(canonicalId)
        .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapshotId).build())
        .setPage(PageRequest.newBuilder().setPageSize(1).build())
        .build();
  }

  private static String snapshotConstraintsIdempotencyKey(
      ResourceId tableId, long snapshotId, SnapshotConstraints constraints) {
    String accountId = tableId == null ? "" : tableId.getAccountId();
    String tableValue = tableId == null ? "" : tableId.getId();
    int kindValue = tableId == null ? 0 : tableId.getKindValue();
    // Idempotency is based on protobuf bytes for the emitted payload. This assumes reconciler
    // input ordering is deterministic; semantically equivalent but differently ordered payloads
    // will intentionally hash differently.
    byte[] payload = constraints == null ? new byte[0] : constraints.toByteArray();
    String digest = Hashing.sha256Hex(payload);
    return "reconciler.constraints/"
        + accountId
        + "/"
        + kindValue
        + "/"
        + tableValue
        + "/"
        + snapshotId
        + "/"
        + digest;
  }

  private static Set<String> normalizeIndexSelectors(Set<String> selectors) {
    if (selectors == null || selectors.isEmpty()) {
      return Set.of();
    }
    LinkedHashSet<String> normalized = new LinkedHashSet<>();
    for (String selector : selectors) {
      if (selector == null || selector.isBlank()) {
        continue;
      }
      String trimmed = selector.trim();
      if (trimmed.startsWith("#")) {
        return null;
      }
      normalized.add(trimmed);
    }
    return Set.copyOf(normalized);
  }

  private static Set<String> persistedIndexSelectors(
      ai.floedb.floecat.catalog.rpc.IndexArtifactRecord record) {
    if (record == null) {
      return Set.of();
    }
    String encoded = record.getPropertiesOrDefault("indexed_columns", "");
    if (encoded.isBlank()) {
      return Set.of();
    }
    LinkedHashSet<String> selectors = new LinkedHashSet<>();
    for (String token : encoded.split(",")) {
      if (token != null && !token.isBlank()) {
        selectors.add(token.trim());
      }
    }
    return Set.copyOf(selectors);
  }

  private static List<PutIndexArtifactsRequest> groupIndexArtifactRequests(
      List<StagedIndexArtifact> artifacts) {
    LinkedHashMap<String, PutIndexArtifactsRequest.Builder> grouped = new LinkedHashMap<>();
    for (StagedIndexArtifact artifact : artifacts) {
      if (artifact == null || artifact.record() == null) {
        continue;
      }
      var record = artifact.record();
      if (!record.hasTableId() || record.getTableId().getId().isBlank()) {
        throw new IllegalArgumentException("record missing tableId");
      }
      if (!record.hasTarget()) {
        throw new IllegalArgumentException("record missing target");
      }
      byte[] content = artifact.content();
      if (content == null || content.length == 0) {
        throw new IllegalArgumentException("staged artifact missing content");
      }
      String key = record.getTableId().toString() + "|" + record.getSnapshotId();
      grouped
          .computeIfAbsent(
              key,
              ignored ->
                  PutIndexArtifactsRequest.newBuilder()
                      .setTableId(record.getTableId())
                      .setSnapshotId(record.getSnapshotId()))
          .addItems(
              PutIndexArtifactItem.newBuilder()
                  .setRecord(record)
                  .setContent(com.google.protobuf.ByteString.copyFrom(content))
                  .setContentType(
                      artifact.contentType() == null || artifact.contentType().isBlank()
                          ? "application/x-parquet"
                          : artifact.contentType())
                  .build());
    }
    return grouped.values().stream().map(PutIndexArtifactsRequest.Builder::build).toList();
  }

  private List<PutTargetStatsRequest> groupTargetRequests(List<TargetStatsRecord> stats) {
    Map<StatsGroupKey, List<TargetStatsRecord>> grouped = new LinkedHashMap<>();
    for (TargetStatsRecord record : stats) {
      StatsGroupKey key = new StatsGroupKey(record.getTableId(), record.getSnapshotId());
      grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(record);
    }
    List<PutTargetStatsRequest> requests = new ArrayList<>(grouped.size());
    for (var entry : grouped.entrySet()) {
      PutTargetStatsRequest.Builder builder =
          PutTargetStatsRequest.newBuilder()
              .setTableId(entry.getKey().tableId)
              .setSnapshotId(entry.getKey().snapshotId);
      for (TargetStatsRecord record : entry.getValue()) {
        builder.addRecords(record);
      }
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
  public ViewMutationResult ensureView(ReconcileContext ctx, ViewSpec spec, String idempotencyKey) {
    CreateViewRequest.Builder request = CreateViewRequest.newBuilder().setSpec(spec);
    if (idempotencyKey != null && !idempotencyKey.isBlank()) {
      request.setIdempotency(IdempotencyKey.newBuilder().setKey(idempotencyKey).build());
    }
    try {
      return new ViewMutationResult(
          view(ctx).createView(request.build()).getView().getResourceId(), true);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() != Status.Code.ALREADY_EXISTS) {
        throw e;
      }
    }

    ResourceId existingId =
        resolveViewId(ctx, spec)
            .orElseThrow(
                () -> new IllegalStateException("Existing view could not be resolved by id"));

    View current =
        view(ctx).getView(GetViewRequest.newBuilder().setViewId(existingId).build()).getView();
    if (viewMatchesSpec(current, spec)) {
      return new ViewMutationResult(existingId, false);
    }

    FieldMask mask =
        FieldMask.newBuilder()
            .addPaths("properties")
            .addPaths("sql_definitions")
            .addPaths("base_relations")
            .addPaths("creation_search_path")
            .addPaths("output_columns")
            .build();
    ResourceId updatedId =
        view(ctx)
            .updateView(
                UpdateViewRequest.newBuilder()
                    .setViewId(existingId)
                    .setSpec(spec)
                    .setUpdateMask(mask)
                    .build())
            .getView()
            .getResourceId();
    return new ViewMutationResult(updatedId, true);
  }

  @Override
  public boolean updateViewById(ReconcileContext ctx, ResourceId viewId, ViewSpec spec) {
    if (viewId == null || viewId.getId().isBlank()) {
      throw new IllegalArgumentException("viewId is required");
    }
    View current =
        view(ctx).getView(GetViewRequest.newBuilder().setViewId(viewId).build()).getView();
    if (!current.getCatalogId().equals(spec.getCatalogId())) {
      throw new IllegalArgumentException("Destination view catalog does not match requested spec");
    }
    if (!current.getNamespaceId().equals(spec.getNamespaceId())) {
      throw new IllegalArgumentException(
          "Destination view namespace does not match requested spec");
    }
    if (!current.getDisplayName().equals(spec.getDisplayName())) {
      throw new IllegalArgumentException(
          "Destination view display name does not match requested spec");
    }
    if (viewMatchesSpec(current, spec)) {
      return false;
    }

    FieldMask mask =
        FieldMask.newBuilder()
            .addPaths("properties")
            .addPaths("sql_definitions")
            .addPaths("base_relations")
            .addPaths("creation_search_path")
            .addPaths("output_columns")
            .build();
    view(ctx)
        .updateView(
            UpdateViewRequest.newBuilder()
                .setViewId(viewId)
                .setSpec(spec)
                .setUpdateMask(mask)
                .build());
    return true;
  }

  private Optional<ResourceId> resolveViewId(ReconcileContext ctx, ViewSpec spec) {
    String catalogName = lookupCatalogName(ctx, spec.getCatalogId());
    String namespaceFq = resolveNamespaceFq(ctx, spec.getNamespaceId());
    NameRef.Builder ref =
        NameRef.newBuilder().setCatalog(catalogName).setName(spec.getDisplayName());
    if (namespaceFq != null && !namespaceFq.isBlank()) {
      ref.addAllPath(List.of(namespaceFq.split("\\.")));
    }
    try {
      return Optional.of(
          directory(ctx)
              .resolveView(ResolveViewRequest.newBuilder().setRef(ref.build()).build())
              .getResourceId());
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        return Optional.empty();
      }
      throw e;
    }
  }

  private static boolean viewMatchesSpec(View current, ViewSpec spec) {
    return current.getCatalogId().equals(spec.getCatalogId())
        && current.getNamespaceId().equals(spec.getNamespaceId())
        && current.getDisplayName().equals(spec.getDisplayName())
        && current.getPropertiesMap().equals(spec.getPropertiesMap())
        && current.getSqlDefinitionsList().equals(spec.getSqlDefinitionsList())
        && current.getBaseRelationsList().equals(spec.getBaseRelationsList())
        && current.getCreationSearchPathList().equals(spec.getCreationSearchPathList())
        && current.getOutputColumnsList().equals(spec.getOutputColumnsList());
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

  @Override
  public Optional<ResourceId> lookupView(ReconcileContext ctx, NameRef view) {
    try {
      return Optional.of(
          directory(ctx)
              .resolveView(ResolveViewRequest.newBuilder().setRef(view).build())
              .getResourceId());
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        return Optional.empty();
      }
      throw e;
    }
  }

  @Override
  public Optional<String> lookupViewDisplayName(ReconcileContext ctx, ResourceId viewId) {
    if (viewId == null || viewId.getId().isBlank()) {
      return Optional.empty();
    }
    try {
      return Optional.of(
          view(ctx)
              .getView(GetViewRequest.newBuilder().setViewId(viewId).build())
              .getView()
              .getDisplayName());
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        return Optional.empty();
      }
      throw e;
    }
  }

  @Override
  public Optional<DestinationViewMetadata> lookupDestinationViewMetadata(
      ReconcileContext ctx, ResourceId viewId) {
    if (viewId == null || viewId.getId().isBlank()) {
      return Optional.empty();
    }
    try {
      var resolved =
          view(ctx).getView(GetViewRequest.newBuilder().setViewId(viewId).build()).getView();
      return Optional.of(
          new DestinationViewMetadata(
              resolved.getCatalogId(),
              resolved.getNamespaceId(),
              resolved.getDisplayName(),
              resolved.getPropertiesOrDefault(SOURCE_NAMESPACE_PROPERTY, ""),
              resolved.getPropertiesOrDefault(SOURCE_NAME_PROPERTY, ""),
              sourceConnectorId(
                  resolved.getPropertiesOrDefault(SOURCE_CONNECTOR_ID_PROPERTY, ""))));
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        return Optional.empty();
      }
      throw e;
    }
  }

  private TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub statistics(
      ReconcileContext ctx) {
    return withHeaders(statistics, ctx);
  }

  private MutinyTableStatisticsServiceGrpc.MutinyTableStatisticsServiceStub statisticsMutiny(
      ReconcileContext ctx) {
    return withHeaders(statisticsMutiny, ctx);
  }

  private MutinyTableIndexServiceGrpc.MutinyTableIndexServiceStub indexMutiny(
      ReconcileContext ctx) {
    return withHeaders(indexMutiny, ctx);
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

  private ViewServiceGrpc.ViewServiceBlockingStub view(ReconcileContext ctx) {
    return withHeaders(view, ctx);
  }

  private TableConstraintsServiceGrpc.TableConstraintsServiceBlockingStub constraintsStub(
      ReconcileContext ctx) {
    return withHeaders(constraintsStub, ctx);
  }

  private TableIndexServiceGrpc.TableIndexServiceBlockingStub index(ReconcileContext ctx) {
    return withHeaders(index, ctx);
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

  static String withBearerPrefix(String token) {
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
    if (snapshotId >= 0 && asOf == null) {
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

  private static String sourceNamespace(UpstreamRef upstream) {
    return upstream == null ? "" : String.join(".", upstream.getNamespacePathList());
  }

  private static String sourceName(UpstreamRef upstream) {
    return upstream == null ? "" : upstream.getTableDisplayName();
  }

  private static ResourceId sourceConnectorId(UpstreamRef upstream) {
    return upstream != null && upstream.hasConnectorId() ? upstream.getConnectorId() : null;
  }

  private static ResourceId sourceConnectorId(String connectorId) {
    return connectorId == null || connectorId.isBlank()
        ? null
        : ResourceId.newBuilder().setKind(ResourceKind.RK_CONNECTOR).setId(connectorId).build();
  }

  private Optional<SourceConnectorContext> sourceConnectorContext(
      ReconcileContext ctx, ResourceId tableId) {
    if (tableId == null || tableId.getId().isBlank()) {
      return Optional.empty();
    }
    Table tableRecord;
    try {
      tableRecord =
          table(ctx).getTable(GetTableRequest.newBuilder().setTableId(tableId).build()).getTable();
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        return Optional.empty();
      }
      throw e;
    }
    if (!tableRecord.hasUpstream()) {
      return Optional.empty();
    }
    ResourceId connectorId = sourceConnectorId(tableRecord.getUpstream());
    String sourceNamespace = sourceNamespace(tableRecord.getUpstream());
    String sourceTable = sourceName(tableRecord.getUpstream());
    if (connectorId == null
        || connectorId.getId().isBlank()
        || sourceNamespace.isBlank()
        || sourceTable.isBlank()) {
      return Optional.empty();
    }
    return Optional.of(new SourceConnectorContext(connectorId, sourceNamespace, sourceTable));
  }

  private <T> T withSourceConnector(
      ReconcileContext ctx,
      ResourceId tableId,
      T emptyValue,
      SourceConnectorOperation<T> operation) {
    Optional<SourceConnectorContext> sourceContext = sourceConnectorContext(ctx, tableId);
    if (sourceContext.isEmpty()) {
      return emptyValue;
    }
    Connector connector = lookupConnector(ctx, sourceContext.get().connectorId());
    ResourceId connectorId = sourceContext.get().connectorId();
    ConnectorConfig config =
        resolveCredentials(
            ConnectorConfigMapper.fromProto(connector), connector.getAuth(), connectorId);
    try (FloecatConnector source = connectorOpener.open(config)) {
      return operation.apply(source, sourceContext.get());
    } catch (RuntimeException e) {
      if (isMissingObjectFailure(e)) {
        throw new ReconcileFailureException(
            ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult.FailureKind
                .TABLE_MISSING,
            "source object missing: "
                + sourceContext.get().sourceNamespace()
                + "."
                + sourceContext.get().sourceTable(),
            e);
      }
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to open source connector for table " + tableId.getId(), e);
    }
  }

  private ConnectorConfig resolveCredentials(
      ConnectorConfig base,
      ai.floedb.floecat.connector.rpc.AuthConfig auth,
      ResourceId connectorId) {
    if (auth.hasCredentials()
        && auth.getCredentials().getCredentialCase()
            != ai.floedb.floecat.connector.rpc.AuthCredentials.CredentialCase.CREDENTIAL_NOT_SET) {
      return CredentialResolverSupport.apply(base, auth.getCredentials());
    }
    if (auth == null || auth.getScheme().isBlank() || "none".equalsIgnoreCase(auth.getScheme())) {
      return base;
    }
    return credentialResolver
        .resolve(connectorId.getAccountId(), connectorId.getId())
        .map(c -> CredentialResolverSupport.apply(base, c, AuthResolutionContext.empty()))
        .orElse(base);
  }

  private static boolean isMissingObjectFailure(Throwable t) {
    if (t == null) {
      return false;
    }
    String className = t.getClass().getName();
    if (className.endsWith("NoSuchTableException")
        || className.endsWith("NoSuchViewException")
        || className.endsWith("NoSuchObjectException")
        || className.endsWith("NotFoundException")) {
      return true;
    }
    String message = t.getMessage();
    if (message == null || message.isBlank()) {
      return false;
    }
    String normalized = message.toLowerCase();
    return normalized.contains("http 404")
        || normalized.contains("status 404")
        || normalized.contains("not found")
        || normalized.contains("does not exist");
  }

  @FunctionalInterface
  interface ConnectorOpener {
    FloecatConnector open(ConnectorConfig config) throws Exception;
  }

  private record SourceConnectorContext(
      ResourceId connectorId, String sourceNamespace, String sourceTable) {}

  @FunctionalInterface
  private interface SourceConnectorOperation<T> {
    T apply(FloecatConnector source, SourceConnectorContext sourceContext) throws Exception;
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
