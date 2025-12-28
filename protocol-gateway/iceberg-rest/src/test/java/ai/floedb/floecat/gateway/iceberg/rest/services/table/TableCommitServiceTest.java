package ai.floedb.floecat.gateway.iceberg.rest.services.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.ListSnapshotsRequest;
import ai.floedb.floecat.catalog.rpc.ListSnapshotsResponse;
import ai.floedb.floecat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcClients;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.CommitTableResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.LoadTableResultDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.common.TableMetadataBuilder;
import ai.floedb.floecat.gateway.iceberg.rest.common.TrinoFixtureTestSupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.CommitStageResolver;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.CommitStageResolver.StageResolution;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.SnapshotClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.SnapshotMetadataService;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.TableMetadataImportService;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.StageCommitProcessor.StageCommitResult;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.TableCommitSideEffectService.PostCommitResult;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import com.google.protobuf.FieldMask;
import jakarta.ws.rs.core.Response;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class TableCommitServiceTest {
  private static final TrinoFixtureTestSupport.Fixture FIXTURE =
      TrinoFixtureTestSupport.simpleFixture();

  private final TableCommitService service = new TableCommitService();
  private final GrpcWithHeaders grpc = mock(GrpcWithHeaders.class);
  private final GrpcClients grpcClients = mock(GrpcClients.class);
  private final SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshotStub =
      mock(SnapshotServiceGrpc.SnapshotServiceBlockingStub.class);
  private final CommitResponseBuilder responseBuilder = new CommitResponseBuilder();
  private final TableLifecycleService tableLifecycleService = mock(TableLifecycleService.class);
  private final TableCommitSideEffectService sideEffectService =
      mock(TableCommitSideEffectService.class);
  private final StageMaterializationService stageMaterializationService =
      mock(StageMaterializationService.class);
  private final CommitStageResolver stageResolver = mock(CommitStageResolver.class);
  private final TableUpdatePlanner tableUpdatePlanner = mock(TableUpdatePlanner.class);
  private final TableGatewaySupport tableSupport = mock(TableGatewaySupport.class);
  private final TableMetadataImportService tableMetadataImportService =
      mock(TableMetadataImportService.class);
  private final SnapshotMetadataService snapshotMetadataService =
      mock(SnapshotMetadataService.class);

  @BeforeEach
  void setUp() {
    service.grpc = grpc;
    responseBuilder.setSnapshotClient(new SnapshotClient(grpc));
    service.responseBuilder = responseBuilder;
    service.tableLifecycleService = tableLifecycleService;
    service.sideEffectService = sideEffectService;
    service.stageMaterializationService = stageMaterializationService;
    service.stageResolver = stageResolver;
    service.tableUpdatePlanner = tableUpdatePlanner;
    service.snapshotMetadataService = snapshotMetadataService;
    service.tableMetadataImportService = tableMetadataImportService;
    when(grpc.raw()).thenReturn(grpcClients);
    when(grpcClients.snapshot()).thenReturn(snapshotStub);
    when(grpc.withHeaders(snapshotStub)).thenReturn(snapshotStub);
    when(snapshotStub.listSnapshots(any(ListSnapshotsRequest.class)))
        .thenReturn(ListSnapshotsResponse.newBuilder().build());
    when(stageMaterializationService.resolveStageId(any(), any())).thenReturn(null);
    when(sideEffectService.finalizeCommitResponse(any(), any(), any(), any(), any(), anyBoolean()))
        .thenAnswer(inv -> PostCommitResult.success(inv.getArgument(4)));
    doNothing().when(sideEffectService).runConnectorSync(any(), any(), any(), any());
    when(tableMetadataImportService.importMetadata(any(), any()))
        .thenReturn(
            new TableMetadataImportService.ImportedMetadata(
                null, Map.of(), null, null, null, List.of()));
  }

  @Test
  void commitReturnsStageResolutionError() {
    Response error = Response.status(Response.Status.BAD_REQUEST).entity("boom").build();
    when(stageResolver.resolve(any(TableCommitService.CommitCommand.class)))
        .thenReturn(new StageResolution(null, null, null, error));

    Response response = service.commit(command(emptyCommitRequest()));

    assertSame(error, response);
  }

  @Test
  void commitReturnsUpdatePlanError() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    StageResolution resolution = new StageResolution(tableId, null, null, null);
    when(stageResolver.resolve(any())).thenReturn(resolution);

    Response conflict = Response.status(Response.Status.CONFLICT).entity("conflict").build();
    TableUpdatePlanner.UpdatePlan failurePlan =
        TableUpdatePlanner.UpdatePlan.failure(
            TableSpec.newBuilder(), FieldMask.newBuilder(), conflict);
    when(tableUpdatePlanner.planUpdates(any(), any(), any())).thenReturn(failurePlan);

    Response response = service.commit(command(emptyCommitRequest()));

    assertSame(conflict, response);
  }

  @Test
  void commitPrefersStageMetadata() {
    String stageMetadataLocation = "s3://stage/orders/metadata/00001-abc.metadata.json";
    String materializedLocation = "s3://warehouse/orders/metadata/00001-def.metadata.json";
    Table stagedTable = tableRecord("cat:db:orders", FIXTURE.metadataLocation());
    StageCommitResult stageResult =
        new StageCommitResult(
            stagedTable,
            new LoadTableResultDto(
                stageMetadataLocation, metadataView(stageMetadataLocation), Map.of(), List.of()));
    ResourceId tableId = stagedTable.getResourceId();
    StageResolution resolution = new StageResolution(tableId, stageResult, "stage-1", null);
    when(stageResolver.resolve(any())).thenReturn(resolution);

    TableUpdatePlanner.UpdatePlan successPlan =
        TableUpdatePlanner.UpdatePlan.success(TableSpec.newBuilder(), FieldMask.newBuilder());
    when(tableUpdatePlanner.planUpdates(any(), any(), any())).thenReturn(successPlan);
    IcebergMetadata metadata = FIXTURE.metadata();
    when(tableSupport.loadCurrentMetadata(stagedTable)).thenReturn(metadata);
    when(sideEffectService.synchronizeConnector(
            eq(tableSupport),
            any(),
            eq(List.of("db")),
            any(),
            any(),
            eq("orders"),
            eq(stagedTable),
            any(),
            any(),
            any()))
        .thenReturn(ResourceId.newBuilder().setId("connector-1").build());
    when(sideEffectService.finalizeCommitResponse(
            eq("db"), eq("orders"), eq(tableId), eq(stagedTable), any(), eq(false)))
        .thenReturn(
            PostCommitResult.success(
                new CommitTableResponseDto(
                    materializedLocation, metadataView(materializedLocation))));

    Response response = service.commit(command(stageCommitRequest()));

    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Object entity = response.getEntity();
    assertTrue(entity instanceof CommitTableResponseDto);
    CommitTableResponseDto dto = (CommitTableResponseDto) entity;
    assertEquals(materializedLocation, dto.metadataLocation());
    ArgumentCaptor<String> metadataLocationCaptor = ArgumentCaptor.forClass(String.class);
    verify(sideEffectService)
        .synchronizeConnector(
            eq(tableSupport),
            any(),
            eq(List.of("db")),
            any(),
            any(),
            eq("orders"),
            eq(stagedTable),
            any(),
            metadataLocationCaptor.capture(),
            any());
    String connectorMetadataLocation = metadataLocationCaptor.getValue();
    assertEquals(materializedLocation, connectorMetadataLocation);
    ArgumentCaptor<ResourceId> connectorCaptor = ArgumentCaptor.forClass(ResourceId.class);
    verify(sideEffectService)
        .runConnectorSync(
            eq(tableSupport), connectorCaptor.capture(), eq(List.of("db")), eq("orders"));
    assertEquals("connector-1", connectorCaptor.getValue().getId());
  }

  @Test
  void commitPrefersStageMetadataWhenLocationMissing() {
    Table stagedTable = tableRecord("cat:db:orders", null);
    TableRequests.Create createRequest =
        new TableRequests.Create(
            "orders",
            FIXTURE.table().getSchemaJson(),
            null,
            "s3://warehouse/db/orders",
            Map.of(),
            null,
            null,
            true);
    TableMetadataView stagedMetadata =
        TableMetadataBuilder.fromCreateRequest("orders", stagedTable, createRequest);
    StageCommitResult stageResult =
        new StageCommitResult(
            stagedTable, new LoadTableResultDto(null, stagedMetadata, Map.of(), List.of()));
    ResourceId tableId = stagedTable.getResourceId();
    StageResolution resolution = new StageResolution(tableId, stageResult, "stage-1", null);
    when(stageResolver.resolve(any())).thenReturn(resolution);
    TableUpdatePlanner.UpdatePlan successPlan =
        TableUpdatePlanner.UpdatePlan.success(TableSpec.newBuilder(), FieldMask.newBuilder());
    when(tableUpdatePlanner.planUpdates(any(), any(), any())).thenReturn(successPlan);
    when(tableSupport.loadCurrentMetadata(stagedTable)).thenReturn(null);
    ArgumentCaptor<CommitTableResponseDto> responseCaptor =
        ArgumentCaptor.forClass(CommitTableResponseDto.class);
    when(sideEffectService.finalizeCommitResponse(
            eq("db"),
            eq("orders"),
            eq(tableId),
            eq(stagedTable),
            responseCaptor.capture(),
            eq(false)))
        .thenAnswer(inv -> PostCommitResult.success(inv.getArgument(4)));

    Response response = service.commit(command(stageCommitRequest()));

    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    CommitTableResponseDto captured = responseCaptor.getValue();
    assertTrue(captured.metadata().formatVersion() != null);
    assertTrue(captured.metadata().schemas() != null && !captured.metadata().schemas().isEmpty());
  }

  @Test
  void commitIgnoresStageMetadataWhenSnapshotUpdatesPresent() {
    String catalogMetadataLocation = FIXTURE.metadataLocation();
    String stageMetadataLocation = "s3://stage/orders/metadata/00001-abc.metadata.json";
    Table stagedTable = tableRecord("cat:db:orders", catalogMetadataLocation);
    StageCommitResult stageResult =
        new StageCommitResult(
            stagedTable,
            new LoadTableResultDto(
                stageMetadataLocation, metadataView(stageMetadataLocation), Map.of(), List.of()));
    ResourceId tableId = stagedTable.getResourceId();
    StageResolution resolution = new StageResolution(tableId, stageResult, "stage-1", null);
    when(stageResolver.resolve(any())).thenReturn(resolution);

    TableUpdatePlanner.UpdatePlan successPlan =
        TableUpdatePlanner.UpdatePlan.success(TableSpec.newBuilder(), FieldMask.newBuilder());
    when(tableUpdatePlanner.planUpdates(any(), any(), any())).thenReturn(successPlan);
    IcebergMetadata metadata =
        FIXTURE.metadata().toBuilder().setMetadataLocation(catalogMetadataLocation).build();
    when(tableSupport.loadCurrentMetadata(stagedTable)).thenReturn(metadata);
    when(sideEffectService.synchronizeConnector(
            eq(tableSupport),
            any(),
            eq(List.of("db")),
            any(),
            any(),
            eq("orders"),
            eq(stagedTable),
            any(),
            any(),
            any()))
        .thenReturn(ResourceId.newBuilder().setId("connector-1").build());

    TableRequests.Commit request =
        new TableRequests.Commit(
            "orders",
            List.of("db"),
            null,
            null,
            "stage-commit",
            List.of(),
            List.of(Map.of("action", "remove-snapshots", "snapshot-ids", List.of(1L))));

    Response response = service.commit(command(request));

    CommitTableResponseDto dto = (CommitTableResponseDto) response.getEntity();
    assertEquals(catalogMetadataLocation, dto.metadataLocation());
  }

  @Test
  void requestedMetadataOverridesCatalogResponse() {
    StageResolution resolution =
        new StageResolution(
            ResourceId.newBuilder().setId("cat:db:orders").build(), null, null, null);
    when(stageResolver.resolve(any())).thenReturn(resolution);
    Table table = tableRecord("cat:db:orders", FIXTURE.metadataLocation());
    when(tableLifecycleService.getTable(any())).thenReturn(table);
    TableUpdatePlanner.UpdatePlan successPlan =
        TableUpdatePlanner.UpdatePlan.success(TableSpec.newBuilder(), FieldMask.newBuilder());
    when(tableUpdatePlanner.planUpdates(any(), any(), any())).thenReturn(successPlan);
    IcebergMetadata metadata = FIXTURE.metadata();
    when(tableSupport.loadCurrentMetadata(table)).thenReturn(metadata);
    when(sideEffectService.synchronizeConnector(
            any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(null);

    String requested = "s3://requested/orders/metadata/00001-abc.metadata.json";
    TableRequests.Commit request =
        new TableRequests.Commit(
            "orders",
            List.of("db"),
            null,
            Map.of("metadata-location", requested),
            null,
            List.of(),
            List.of());

    Response response = service.commit(command(request));

    CommitTableResponseDto dto = (CommitTableResponseDto) response.getEntity();
    assertEquals(requested, dto.metadataLocation());
  }

  private TableRequests.Commit emptyCommitRequest() {
    return new TableRequests.Commit(null, List.of("db"), null, null, null, List.of(), List.of());
  }

  private TableRequests.Commit stageCommitRequest() {
    return new TableRequests.Commit(
        "orders", List.of("db"), null, null, "stage-commit", List.of(), List.of());
  }

  private Table tableRecord(String id, String metadataLocation) {
    Table.Builder builder =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId(id))
            .putProperties("location", FIXTURE.table().getPropertiesOrDefault("location", ""));
    if (metadataLocation != null) {
      builder.putProperties("metadata-location", metadataLocation);
    }
    return builder.build();
  }

  private TableMetadataView metadataView(String metadataLocation) {
    TableMetadataView base =
        TableMetadataBuilder.fromCatalog(
            "orders",
            FIXTURE.table(),
            new LinkedHashMap<>(FIXTURE.table().getPropertiesMap()),
            FIXTURE.metadata(),
            FIXTURE.snapshots());
    return base.withMetadataLocation(metadataLocation);
  }

  private TableCommitService.CommitCommand command(TableRequests.Commit request) {
    return new TableCommitService.CommitCommand(
        "foo",
        "db",
        List.of("db"),
        "orders",
        "catalog",
        ResourceId.newBuilder().setId("cat").build(),
        ResourceId.newBuilder().setId("cat:db").build(),
        "idem",
        "txn",
        request,
        tableSupport);
  }
}
