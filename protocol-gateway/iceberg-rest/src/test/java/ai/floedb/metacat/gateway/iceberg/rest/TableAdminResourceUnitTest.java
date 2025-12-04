package ai.floedb.metacat.gateway.iceberg.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.metacat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.metacat.catalog.rpc.ResolveCatalogResponse;
import ai.floedb.metacat.catalog.rpc.ResolveNamespaceResponse;
import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.catalog.rpc.TableServiceGrpc;
import ai.floedb.metacat.catalog.rpc.UpdateTableResponse;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.connector.rpc.Connector;
import ai.floedb.metacat.connector.rpc.ConnectorsGrpc;
import ai.floedb.metacat.connector.rpc.CreateConnectorResponse;
import ai.floedb.metacat.connector.rpc.SyncCaptureResponse;
import ai.floedb.metacat.connector.rpc.TriggerReconcileResponse;
import ai.floedb.metacat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.metacat.gateway.iceberg.grpc.GrpcClients;
import ai.floedb.metacat.gateway.iceberg.grpc.GrpcWithHeaders;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.eclipse.microprofile.config.Config;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TableAdminResourceUnitTest {
  private final TableAdminResource resource = new TableAdminResource();
  private final GrpcWithHeaders grpc = mock(GrpcWithHeaders.class);
  private final IcebergGatewayConfig config = mock(IcebergGatewayConfig.class);
  private final StagedTableService stagedTableService = mock(StagedTableService.class);
  private final TenantContext tenantContext = mock(TenantContext.class);
  private final StageCommitProcessor stageCommitProcessor = mock(StageCommitProcessor.class);
  private final MetadataMirrorService metadataMirrorService = mock(MetadataMirrorService.class);
  private final Config mpConfig = mock(Config.class);
  private final GrpcClients clients = mock(GrpcClients.class);
  private final DirectoryServiceGrpc.DirectoryServiceBlockingStub directoryStub =
      mock(DirectoryServiceGrpc.DirectoryServiceBlockingStub.class);
  private final ConnectorsGrpc.ConnectorsBlockingStub connectorsStub =
      mock(ConnectorsGrpc.ConnectorsBlockingStub.class);
  private final TableServiceGrpc.TableServiceBlockingStub tableStub =
      mock(TableServiceGrpc.TableServiceBlockingStub.class);

  @BeforeEach
  void setUp() {
    resource.grpc = grpc;
    resource.config = config;
    resource.stagedTableService = stagedTableService;
    resource.tenantContext = tenantContext;
    resource.stageCommitProcessor = stageCommitProcessor;
    resource.metadataMirrorService = metadataMirrorService;
    resource.mapper = new ObjectMapper();
    resource.mpConfig = mpConfig;
    resource.initSupport();

    when(mpConfig.getOptionalValue(any(), any())).thenReturn(Optional.empty());
    when(config.catalogMapping()).thenReturn(Map.of());
    when(tenantContext.getTenantId()).thenReturn("tenant1");
    when(grpc.raw()).thenReturn(clients);
    when(clients.directory()).thenReturn(directoryStub);
    when(clients.connectors()).thenReturn(connectorsStub);
    when(clients.table()).thenReturn(tableStub);
    when(grpc.withHeaders(directoryStub)).thenReturn(directoryStub);
    when(grpc.withHeaders(connectorsStub)).thenReturn(connectorsStub);
    when(grpc.withHeaders(tableStub)).thenReturn(tableStub);
    when(directoryStub.resolveCatalog(any()))
        .thenReturn(
            ResolveCatalogResponse.newBuilder()
                .setResourceId(ResourceId.newBuilder().setId("cat").build())
                .build());
    when(directoryStub.resolveNamespace(any()))
        .thenReturn(
            ResolveNamespaceResponse.newBuilder()
                .setResourceId(ResourceId.newBuilder().setId("cat:db").build())
                .build());
    when(connectorsStub.syncCapture(any())).thenReturn(SyncCaptureResponse.newBuilder().build());
    when(connectorsStub.triggerReconcile(any()))
        .thenReturn(TriggerReconcileResponse.newBuilder().setJobId("job").build());
  }

  @Test
  void commitTransactionCreatesConnector() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    Table table =
        Table.newBuilder()
            .setResourceId(tableId)
            .setNamespaceId(ResourceId.newBuilder().setId("cat:db").build())
            .putProperties("metadata-location", "s3://bucket/orders/metadata.json")
            .putProperties("location", "s3://bucket/orders")
            .build();
    LoadTableResultDto loadResult =
        new LoadTableResultDto("s3://bucket/orders/metadata.json", null, Map.of(), List.of());
    StageCommitProcessor.StageCommitResult stageResult =
        new StageCommitProcessor.StageCommitResult(table, loadResult);
    when(stageCommitProcessor.commitStage(any(), any(), any(), any(), any(), any()))
        .thenReturn(stageResult);
    when(tableStub.updateTable(any()))
        .thenReturn(UpdateTableResponse.newBuilder().setTable(table).build());

    ResourceId connectorId = ResourceId.newBuilder().setId("conn").build();
    Connector connector = Connector.newBuilder().setResourceId(connectorId).build();
    when(connectorsStub.createConnector(any()))
        .thenReturn(CreateConnectorResponse.newBuilder().setConnector(connector).build());

    TransactionCommitRequest request =
        new TransactionCommitRequest(
            List.of(
                new TransactionCommitRequest.StagedRefUpdate(
                    new TableIdentifierDto(List.of("db"), "orders"), "stage-commit", null, null)),
            null,
            null);

    var response = resource.commitTransaction("foo", request);
    assertEquals(200, response.getStatus());
    verify(connectorsStub).createConnector(any());
  }
}
