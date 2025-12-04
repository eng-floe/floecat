package ai.floedb.metacat.gateway.iceberg.rest.services.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.metacat.catalog.rpc.UpdateTableRequest;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.metacat.gateway.iceberg.rest.services.metadata.MetadataMirrorException;
import ai.floedb.metacat.gateway.iceberg.rest.services.metadata.MetadataMirrorService;
import ai.floedb.metacat.gateway.iceberg.rest.services.metadata.MetadataMirrorService.MirrorResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class TableCommitSideEffectServiceTest {
  private final TableCommitSideEffectService service = new TableCommitSideEffectService();
  private final MetadataMirrorService metadataMirrorService = mock(MetadataMirrorService.class);
  private final TableLifecycleService tableLifecycleService = mock(TableLifecycleService.class);
  private final ObjectMapper mapper = new ObjectMapper();

  @BeforeEach
  void setUp() {
    service.metadataMirrorService = metadataMirrorService;
    service.tableLifecycleService = tableLifecycleService;
  }

  @Test
  void mirrorMetadataUpdatesTableProperties() throws Exception {
    TableMetadataView metadata = metadata("s3://warehouse/tables/orders/metadata.json");
    TableMetadataView mirrored = metadata.withMetadataLocation("s3://mirror/orders/v2.json");
    when(metadataMirrorService.mirror(
            "cat.db", "orders", metadata, "s3://warehouse/tables/orders/metadata.json"))
        .thenReturn(new MirrorResult("s3://mirror/orders/v2.json", mirrored));
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();

    MirrorMetadataResult result =
        service.mirrorMetadata(
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
  void mirrorMetadataFallsBackWhenMirrorFails() throws Exception {
    TableMetadataView metadata = metadata("s3://warehouse/tables/orders/metadata.json");
    when(metadataMirrorService.mirror(any(), any(), any(), any()))
        .thenThrow(new MetadataMirrorException("mirror failed", new RuntimeException()));

    MirrorMetadataResult result =
        service.mirrorMetadata(
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

  private TableMetadataView metadata(String metadataLocation) throws JsonProcessingException {
    return mapper.readValue(
        "{"
            + "\"metadata-location\":\""
            + metadataLocation
            + "\",\"properties\":{\"existing\":\"value\"}}",
        TableMetadataView.class);
  }
}
