package ai.floedb.metacat.gateway.iceberg.rest.services.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.catalog.rpc.TableFormat;
import ai.floedb.metacat.catalog.rpc.UpdateTableRequest;
import ai.floedb.metacat.catalog.rpc.UpstreamRef;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.gateway.iceberg.rest.api.dto.CommitTableResponseDto;
import ai.floedb.metacat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.metacat.gateway.iceberg.rest.services.metadata.MaterializeMetadataException;
import ai.floedb.metacat.gateway.iceberg.rest.services.metadata.MaterializeMetadataService;
import ai.floedb.metacat.gateway.iceberg.rest.services.metadata.MaterializeMetadataService.MaterializeResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class TableCommitSideEffectServiceTest {
  private final TableCommitSideEffectService service = new TableCommitSideEffectService();
  private final MaterializeMetadataService materializeMetadataService =
      mock(MaterializeMetadataService.class);
  private final TableLifecycleService tableLifecycleService = mock(TableLifecycleService.class);
  private final ObjectMapper mapper = new ObjectMapper();

  @BeforeEach
  void setUp() {
    service.materializeMetadataService = materializeMetadataService;
    service.tableLifecycleService = tableLifecycleService;
  }

  @Test
  void mirrorMetadataUpdatesTableProperties() throws Exception {
    TableMetadataView metadata = metadata("s3://warehouse/tables/orders/metadata.json");
    TableMetadataView mirrored = metadata.withMetadataLocation("s3://mirror/orders/v2.json");
    when(materializeMetadataService.materialize(
            "cat.db", "orders", metadata, "s3://warehouse/tables/orders/metadata.json"))
        .thenReturn(new MaterializeResult("s3://mirror/orders/v2.json", mirrored));
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();

    MaterializeMetadataResult result =
        service.materializeMetadata(
            "cat.db", tableId, "orders", metadata, "s3://warehouse/tables/orders/metadata.json");

    assertNull(result.error());
    assertSame(mirrored, result.metadata());
    assertEquals("s3://mirror/orders/v2.json", result.metadataLocation());

    ArgumentCaptor<UpdateTableRequest> captor = ArgumentCaptor.forClass(UpdateTableRequest.class);
    verify(tableLifecycleService).updateTable(captor.capture());
    UpdateTableRequest request = captor.getValue();
    assertEquals(tableId, request.getTableId());
    assertEquals(
        "s3://mirror/orders/v2.json", request.getSpec().getPropertiesOrThrow("metadata-location"));
  }

  @Test
  void materializeMetadataFallsBackWhenMaterializationFails() throws Exception {
    TableMetadataView metadata = metadata("s3://warehouse/tables/orders/metadata.json");
    when(materializeMetadataService.materialize(any(), any(), any(), any()))
        .thenThrow(new MaterializeMetadataException("mirror failed", new RuntimeException()));

    MaterializeMetadataResult result =
        service.materializeMetadata(
            "cat.db",
            ResourceId.getDefaultInstance(),
            "orders",
            metadata,
            metadata.metadataLocation());

    assertNull(result.error());
    assertSame(metadata, result.metadata());
    assertEquals("s3://warehouse/tables/orders/metadata.json", result.metadataLocation());
    verify(tableLifecycleService, never()).updateTable(any());
  }

  @Test
  void runConnectorSyncIfPossibleTriggersCaptureAndReconcile() {
    TableGatewaySupport tableSupport = mock(TableGatewaySupport.class);
    ResourceId connectorId = ResourceId.newBuilder().setId("connector-1").build();
    List<String> namespacePath = List.of("db1", "nested");

    service.runConnectorSync(tableSupport, connectorId, namespacePath, "orders");

    verify(tableSupport).runSyncMetadataCapture(connectorId, namespacePath, "orders");
    verify(tableSupport).triggerScopedReconcile(connectorId, namespacePath, "orders");
  }

  @Test
  void runConnectorSyncIfPossibleSkipsBlankIds() {
    TableGatewaySupport tableSupport = mock(TableGatewaySupport.class);
    ResourceId emptyId = ResourceId.newBuilder().setId("").build();

    service.runConnectorSync(tableSupport, emptyId, List.of("db"), "orders");

    verify(tableSupport, never()).runSyncMetadataCapture(any(), any(), any());
    verify(tableSupport, never()).triggerScopedReconcile(any(), any(), any());
  }

  @Test
  void applyMaterializationResultUpdatesMetadataAndLocation() throws Exception {
    TableMetadataView original = metadata("s3://orig/location");
    CommitTableResponseDto response = new CommitTableResponseDto("s3://orig/location", original);

    TableMetadataView mirrored = original.withMetadataLocation("s3://new/location");
    MaterializeMetadataResult mirror =
        MaterializeMetadataResult.success(mirrored, "s3://new/location");

    CommitTableResponseDto updated = service.applyMaterializationResult(response, mirror);

    assertEquals("s3://new/location", updated.metadataLocation());
    assertSame(mirrored, updated.metadata());
  }

  @Test
  void applyMaterializationResultReturnsOriginalWhenUnchanged() throws Exception {
    TableMetadataView original = metadata("s3://orig/location");
    CommitTableResponseDto response = new CommitTableResponseDto("s3://orig/location", original);
    MaterializeMetadataResult mirror =
        MaterializeMetadataResult.success(original, "s3://orig/location");

    CommitTableResponseDto result = service.applyMaterializationResult(response, mirror);

    assertSame(response, result);
  }

  @Test
  void finalizeCommitResponseMaterializationAndSynchronizesConnector() throws Exception {
    TableGatewaySupport tableSupport = mock(TableGatewaySupport.class);
    ResourceId connectorId = ResourceId.newBuilder().setId("conn-1").build();
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
            .setUpstream(
                UpstreamRef.newBuilder()
                    .setFormat(TableFormat.TF_ICEBERG)
                    .setConnectorId(connectorId)
                    .setUri("s3://existing/location")
                    .build())
            .build();
    TableMetadataView metadata = metadata("s3://orig/location");
    TableMetadataView mirrored = metadata.withMetadataLocation("s3://mirror/location");
    CommitTableResponseDto response = new CommitTableResponseDto("s3://orig/location", metadata);
    when(materializeMetadataService.materialize("cat.db", "orders", metadata, "s3://orig/location"))
        .thenReturn(new MaterializeResult("s3://mirror/location", mirrored));

    TableCommitSideEffectService.PostCommitResult result =
        service.finalizeCommitResponse(
            "cat.db",
            "orders",
            table.getResourceId(),
            table,
            response,
            false,
            tableSupport,
            "foo",
            List.of("db"),
            ResourceId.newBuilder().setId("ns").build(),
            ResourceId.newBuilder().setId("catalog").build(),
            "idem");

    assertNull(result.error());
    assertEquals("s3://mirror/location", result.response().metadataLocation());
    assertEquals(connectorId, result.connectorId());
    verify(tableSupport).updateConnectorMetadata(connectorId, "s3://mirror/location");
  }

  @Test
  void finalizeCommitResponseSkipsMaterializationWhenRequested() throws Exception {
    TableGatewaySupport tableSupport = mock(TableGatewaySupport.class);
    ResourceId connectorId = ResourceId.newBuilder().setId("conn-1").build();
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
            .setUpstream(
                UpstreamRef.newBuilder()
                    .setFormat(TableFormat.TF_ICEBERG)
                    .setConnectorId(connectorId)
                    .build())
            .build();
    TableMetadataView metadata = metadata("s3://orig/location");
    CommitTableResponseDto response = new CommitTableResponseDto("s3://orig/location", metadata);

    TableCommitSideEffectService.PostCommitResult result =
        service.finalizeCommitResponse(
            "cat.db",
            "orders",
            table.getResourceId(),
            table,
            response,
            true,
            tableSupport,
            "foo",
            List.of("db"),
            ResourceId.newBuilder().setId("ns").build(),
            ResourceId.newBuilder().setId("catalog").build(),
            "idem");

    assertNull(result.error());
    assertSame(response, result.response());
    verify(materializeMetadataService, never()).materialize(any(), any(), any(), any());
    assertEquals(connectorId, result.connectorId());
  }

  private TableMetadataView metadata(String metadataLocation) throws JsonProcessingException {
    return mapper.readValue(
        "{"
            + "\"metadata-location\":\""
            + metadataLocation
            + "\",\"properties\":{\"existing\":\"value\"}}",
        TableMetadataView.class);
  }
}
