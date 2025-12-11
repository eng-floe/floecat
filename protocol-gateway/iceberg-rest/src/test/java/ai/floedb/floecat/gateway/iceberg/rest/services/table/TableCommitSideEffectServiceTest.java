package ai.floedb.floecat.gateway.iceberg.rest.services.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.UpdateTableRequest;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.CommitTableResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.MaterializeMetadataException;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.MaterializeMetadataResult;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.MaterializeMetadataService;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.MaterializeMetadataService.MaterializeResult;
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
  void materializeMetadataUpdatesTableProperties() throws Exception {
    TableMetadataView metadata = metadata("s3://warehouse/tables/orders/metadata.json");
    TableMetadataView materialized =
        metadata.withMetadataLocation(
            "s3://warehouse/tables/orders/metadata/00001-abc.metadata.json");
    when(materializeMetadataService.materialize(
            "cat.db", "orders", metadata, "s3://warehouse/tables/orders/metadata.json"))
        .thenReturn(
            new MaterializeResult(
                "s3://warehouse/tables/orders/metadata/00001-abc.metadata.json", materialized));
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();

    Table table =
        Table.newBuilder()
            .setResourceId(tableId)
            .setUpstream(
                UpstreamRef.newBuilder()
                    .setFormat(TableFormat.TF_ICEBERG)
                    .setConnectorId(ResourceId.newBuilder().setId("conn-1").build())
                    .build())
            .build();
    MaterializeMetadataResult result =
        service.materializeMetadata(
            "cat.db",
            tableId,
            "orders",
            table,
            metadata,
            "s3://warehouse/tables/orders/metadata.json");

    assertNull(result.error());
    assertSame(materialized, result.metadata());
    assertEquals(
        "s3://warehouse/tables/orders/metadata/00001-abc.metadata.json", result.metadataLocation());

    ArgumentCaptor<UpdateTableRequest> captor = ArgumentCaptor.forClass(UpdateTableRequest.class);
    verify(tableLifecycleService).updateTable(captor.capture());
    UpdateTableRequest request = captor.getValue();
    assertEquals(tableId, request.getTableId());
    assertEquals(
        "s3://warehouse/tables/orders/metadata/00001-abc.metadata.json",
        request.getSpec().getPropertiesOrThrow("metadata-location"));
  }

  @Test
  void materializeMetadataReturnsOriginalWhenMetadataMissing() throws Exception {
    MaterializeMetadataResult result =
        service.materializeMetadata(
            "cat.db",
            ResourceId.getDefaultInstance(),
            "orders",
            null,
            null,
            "s3://warehouse/tables/orders/metadata.json");

    assertNull(result.error());
    assertNull(result.metadata());
    assertEquals("s3://warehouse/tables/orders/metadata.json", result.metadataLocation());
    verify(materializeMetadataService, never()).materialize(any(), any(), any(), any());
    verify(tableLifecycleService, never()).updateTable(any());
  }

  @Test
  void materializeMetadataFallsBackWhenMaterializationFails() throws Exception {
    TableMetadataView metadata = metadata("s3://warehouse/tables/orders/metadata.json");
    when(materializeMetadataService.materialize(any(), any(), any(), any()))
        .thenThrow(new MaterializeMetadataException("boom"));

    MaterializeMetadataResult result =
        service.materializeMetadata(
            "cat.db",
            ResourceId.getDefaultInstance(),
            "orders",
            null,
            metadata,
            metadata.metadataLocation());

    assertNull(result.error());
    assertSame(metadata, result.metadata());
    assertEquals(metadata.metadataLocation(), result.metadataLocation());
  }

  @Test
  void runConnectorSyncIfPossibleTriggersCaptureAndReconcile() {
    TableGatewaySupport tableSupport = mock(TableGatewaySupport.class);
    when(tableSupport.connectorIntegrationEnabled()).thenReturn(true);
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
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
            .setUpstream(
                UpstreamRef.newBuilder()
                    .setFormat(TableFormat.TF_ICEBERG)
                    .setUri("s3://existing/location")
                    .build())
            .build();
    TableMetadataView metadata = metadata("s3://orig/location");
    TableMetadataView materialized =
        metadata.withMetadataLocation("s3://orig/metadata/00001-new.metadata.json");
    when(materializeMetadataService.materialize("cat.db", "orders", metadata, "s3://orig/location"))
        .thenReturn(
            new MaterializeResult("s3://orig/metadata/00001-new.metadata.json", materialized));
    CommitTableResponseDto response = new CommitTableResponseDto("s3://orig/location", metadata);

    TableCommitSideEffectService.PostCommitResult result =
        service.finalizeCommitResponse(
            "cat.db", "orders", table.getResourceId(), table, response, false);

    assertNull(result.error());
    assertEquals(
        "s3://orig/metadata/00001-new.metadata.json", result.response().metadataLocation());
  }

  @Test
  void finalizeCommitResponseSkipsMaterializationWhenRequested() throws Exception {
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
            .setUpstream(UpstreamRef.newBuilder().setFormat(TableFormat.TF_ICEBERG).build())
            .build();
    TableMetadataView metadata = metadata("s3://orig/location");
    CommitTableResponseDto response = new CommitTableResponseDto("s3://orig/location", metadata);

    TableCommitSideEffectService.PostCommitResult result =
        service.finalizeCommitResponse(
            "cat.db", "orders", table.getResourceId(), table, response, true);

    assertNull(result.error());
    assertSame(response, result.response());
    verify(materializeMetadataService, never()).materialize(any(), any(), any(), any());
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
