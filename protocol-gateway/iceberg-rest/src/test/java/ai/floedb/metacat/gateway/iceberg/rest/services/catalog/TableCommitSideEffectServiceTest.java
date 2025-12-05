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

  @Test
  void applyMirrorResultUpdatesMetadataAndLocation() throws Exception {
    TableMetadataView original = metadata("s3://orig/location");
    CommitTableResponseDto response = new CommitTableResponseDto("s3://orig/location", original);

    TableMetadataView mirrored = original.withMetadataLocation("s3://new/location");
    MirrorMetadataResult mirror = MirrorMetadataResult.success(mirrored, "s3://new/location");

    CommitTableResponseDto updated = service.applyMirrorResult(response, mirror);

    assertEquals("s3://new/location", updated.metadataLocation());
    assertSame(mirrored, updated.metadata());
  }

  @Test
  void applyMirrorResultReturnsOriginalWhenUnchanged() throws Exception {
    TableMetadataView original = metadata("s3://orig/location");
    CommitTableResponseDto response = new CommitTableResponseDto("s3://orig/location", original);
    MirrorMetadataResult mirror = MirrorMetadataResult.success(original, "s3://orig/location");

    CommitTableResponseDto result = service.applyMirrorResult(response, mirror);

    assertSame(response, result);
  }

  @Test
  void finalizeCommitResponseMirrorsAndSynchronizesConnector() throws Exception {
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
    when(metadataMirrorService.mirror("cat.db", "orders", metadata, "s3://orig/location"))
        .thenReturn(new MirrorResult("s3://mirror/location", mirrored));

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
  void finalizeCommitResponseSkipsMirrorWhenRequested() throws Exception {
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
    verify(metadataMirrorService, never()).mirror(any(), any(), any(), any());
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
