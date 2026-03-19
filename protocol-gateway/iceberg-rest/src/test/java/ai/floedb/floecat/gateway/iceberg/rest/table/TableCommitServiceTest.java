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

package ai.floedb.floecat.gateway.iceberg.rest.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.CommitTableResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TransactionCommitRequest;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.compat.TableFormatSupport;
import ai.floedb.floecat.gateway.iceberg.rest.support.AccountContext;
import ai.floedb.floecat.gateway.iceberg.rest.support.GrpcServiceFacade;
import jakarta.ws.rs.core.Response;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class TableCommitServiceTest {
  private final TransactionCommitService service =
      org.mockito.Mockito.spy(new TransactionCommitService());
  private final IcebergGatewayConfig config = org.mockito.Mockito.mock(IcebergGatewayConfig.class);
  private final IcebergGatewayConfig.DeltaCompatConfig deltaCompatConfig =
      org.mockito.Mockito.mock(IcebergGatewayConfig.DeltaCompatConfig.class);
  private final TableGatewaySupport tableSupport =
      org.mockito.Mockito.mock(TableGatewaySupport.class);
  private final StagedTableService stagedTableService =
      org.mockito.Mockito.mock(StagedTableService.class);
  private final AccountContext accountContext = org.mockito.Mockito.mock(AccountContext.class);
  private final IcebergMetadataService icebergMetadataService =
      org.mockito.Mockito.mock(IcebergMetadataService.class);
  private final GrpcServiceFacade snapshotClient =
      org.mockito.Mockito.mock(GrpcServiceFacade.class);

  @BeforeEach
  void setUp() {
    service.config = config;
    service.tableGatewaySupport = tableSupport;
    service.tableFormatSupport = new TableFormatSupport();
    service.stagedTableService = stagedTableService;
    service.accountContext = accountContext;
    service.icebergMetadataService = icebergMetadataService;
    service.grpcClient = snapshotClient;

    when(config.deltaCompat()).thenReturn(Optional.of(deltaCompatConfig));
    when(deltaCompatConfig.enabled()).thenReturn(false);
    when(deltaCompatConfig.readOnly()).thenReturn(true);
    when(accountContext.getAccountId()).thenReturn("account-1");
    when(stagedTableService.findSingleStage(any(), any(), any(), any()))
        .thenReturn(Optional.empty());
    when(tableSupport.defaultFileIoProperties()).thenReturn(Map.of());
  }

  @Test
  void commitValidatesRequiredBody() {
    Response nullCommand = service.commitTable(null);
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), nullCommand.getStatus());

    Response nullRequest = service.commitTable(command(null));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), nullRequest.getStatus());
  }

  @Test
  void commitReturnsAtomicCommitErrorDirectly() {
    doReturn(Response.status(Response.Status.CONFLICT).build())
        .when(service)
        .commit(any(), any(), any(), any());
    Table table = tableRecord("cat:db:orders");
    when(tableSupport.resolveTableId(eq("catalog"), eq(List.of("db")), eq("orders")))
        .thenReturn(table.getResourceId());
    when(tableSupport.getTable(table.getResourceId())).thenReturn(table);

    Response response = service.commitTable(command(commitWithSingleUpdate()));

    assertEquals(Response.Status.CONFLICT.getStatusCode(), response.getStatus());
  }

  @Test
  void commitReturnsConflictWhenStageIsAborted() {
    when(tableSupport.resolveTableId(eq("catalog"), eq(List.of("db")), eq("orders")))
        .thenThrow(io.grpc.Status.NOT_FOUND.asRuntimeException());
    StagedTableEntry staged =
        new StagedTableEntry(
            new StagedTableKey("account-1", "catalog", List.of("db"), "orders", "stage-1"),
            ResourceId.newBuilder().setId("cat").build(),
            ResourceId.newBuilder().setId("cat:db").build(),
            createRequest(),
            ai.floedb.floecat.catalog.rpc.TableSpec.newBuilder().build(),
            List.of(Map.of("type", "assert-create")),
            StageState.ABORTED,
            Instant.now(),
            Instant.now(),
            "idem");
    when(stagedTableService.getStage(staged.key())).thenReturn(Optional.of(staged));

    Response response = service.commitTable(commandWithStage(emptyCommitRequest(), "stage-1"));

    assertEquals(Response.Status.CONFLICT.getStatusCode(), response.getStatus());
    verify(service, never()).commit(any(), any(), any(), any());
  }

  @Test
  void commitDelegatesToAtomicTransactionAndBuildsResponse() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    Table table = tableRecord("cat:db:orders");
    when(tableSupport.resolveTableId(eq("catalog"), eq(List.of("db")), eq("orders")))
        .thenReturn(tableId);
    when(tableSupport.getTable(tableId)).thenReturn(table);
    doReturn(Response.noContent().build()).when(service).commit(any(), any(), any(), any());

    TableMetadataView metadataView =
        new TableMetadataView(
            2,
            null,
            null,
            "s3://warehouse/db/orders/metadata/00001.metadata.json",
            null,
            Map.of(),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            List.of(),
            List.of(),
            List.of(),
            Map.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());
    CommitTableResponseDto dto =
        new CommitTableResponseDto(metadataView.metadataLocation(), metadataView);
    when(icebergMetadataService.resolveCurrentIcebergMetadata(any(), any())).thenReturn(null);
    when(icebergMetadataService.resolveMetadata(any(), any(), any(), any(), any()))
        .thenReturn(new IcebergMetadataService.ResolvedMetadata(null, metadataView, null));

    Response response = service.commitTable(command(commitWithSingleUpdate()));

    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    ArgumentCaptor<TransactionCommitRequest> txRequestCaptor =
        ArgumentCaptor.forClass(TransactionCommitRequest.class);
    verify(service).commit(eq("foo"), eq("idem"), txRequestCaptor.capture(), eq(tableSupport));
    TransactionCommitRequest txRequest = txRequestCaptor.getValue();
    assertEquals(1, txRequest.tableChanges().size());
    assertEquals("orders", txRequest.tableChanges().get(0).identifier().name());
    assertTrue(txRequest.tableChanges().get(0).identifier().namespace().contains("db"));
  }

  @Test
  void commitDelegatesOriginalUpdatesWithoutLocalMetadataInjection() {
    TableRequests.Commit request = commitWithSingleUpdate();
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    Table table = tableRecord("cat:db:orders");
    when(tableSupport.resolveTableId(eq("catalog"), eq(List.of("db")), eq("orders")))
        .thenReturn(tableId);
    when(tableSupport.getTable(tableId)).thenReturn(table);
    doReturn(Response.noContent().build()).when(service).commit(any(), any(), any(), any());

    TableMetadataView metadataView =
        new TableMetadataView(
            2,
            null,
            null,
            "s3://warehouse/db/orders/metadata/00001.metadata.json",
            null,
            Map.of(),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            List.of(),
            List.of(),
            List.of(),
            Map.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());
    CommitTableResponseDto dto =
        new CommitTableResponseDto(metadataView.metadataLocation(), metadataView);
    when(icebergMetadataService.resolveCurrentIcebergMetadata(any(), any())).thenReturn(null);
    when(icebergMetadataService.resolveMetadata(any(), any(), any(), any(), any()))
        .thenReturn(new IcebergMetadataService.ResolvedMetadata(null, metadataView, null));

    Response response = service.commitTable(command(request));

    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    ArgumentCaptor<TransactionCommitRequest> txRequestCaptor =
        ArgumentCaptor.forClass(TransactionCommitRequest.class);
    verify(service).commit(eq("foo"), eq("idem"), txRequestCaptor.capture(), eq(tableSupport));
    var updates = txRequestCaptor.getValue().tableChanges().get(0).updates();
    assertEquals(
        request.updates(),
        updates,
        "single-table commit should forward caller updates unchanged at this layer");
  }

  @Test
  void commitMergesStagedCreateIntoSingleAtomicTransactionWhenTableMissing() {
    when(tableSupport.resolveTableId(eq("catalog"), eq(List.of("db")), eq("orders")))
        .thenThrow(io.grpc.Status.NOT_FOUND.asRuntimeException())
        .thenReturn(ResourceId.newBuilder().setId("cat:db:orders").build());
    Table created = tableRecord("cat:db:orders");
    when(tableSupport.getTable(ResourceId.newBuilder().setId("cat:db:orders").build()))
        .thenReturn(created);
    doReturn(Response.noContent().build()).when(service).commit(any(), any(), any(), any());

    StagedTableEntry staged =
        new StagedTableEntry(
            new StagedTableKey("account-1", "catalog", List.of("db"), "orders", "stage-1"),
            ResourceId.newBuilder().setId("cat").build(),
            ResourceId.newBuilder().setId("cat:db").build(),
            createRequestWithMetadataLocation(),
            ai.floedb.floecat.catalog.rpc.TableSpec.newBuilder().build(),
            List.of(Map.of("type", "assert-create")),
            StageState.STAGED,
            Instant.now(),
            Instant.now(),
            "idem");
    when(stagedTableService.getStage(staged.key())).thenReturn(Optional.of(staged));

    TransactionCommitRequest stagedCreateTx =
        new TransactionCommitRequest(
            List.of(
                new TransactionCommitRequest.TableChange(
                    new ai.floedb.floecat.gateway.iceberg.rest.api.dto.TableIdentifierDto(
                        List.of("db"), "orders"),
                    List.of(Map.of("type", "assert-create")),
                    List.of(Map.of("action", "add-schema")))));
    doReturn(stagedCreateTx)
        .when(service)
        .buildCreateRequest(any(), any(), any(), any(), any(), any());

    TableMetadataView metadataView =
        new TableMetadataView(
            2,
            null,
            null,
            "s3://warehouse/db/orders/metadata/00001.metadata.json",
            null,
            Map.of(),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            List.of(),
            List.of(),
            List.of(),
            Map.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());
    CommitTableResponseDto dto =
        new CommitTableResponseDto(metadataView.metadataLocation(), metadataView);
    when(icebergMetadataService.resolveCurrentIcebergMetadata(any(), any())).thenReturn(null);
    when(icebergMetadataService.resolveMetadata(any(), any(), any(), any(), any()))
        .thenReturn(new IcebergMetadataService.ResolvedMetadata(null, metadataView, null));

    Response response = service.commitTable(commandWithStage(commitWithSingleUpdate(), "stage-1"));

    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    ArgumentCaptor<TransactionCommitRequest> txRequestCaptor =
        ArgumentCaptor.forClass(TransactionCommitRequest.class);
    verify(service).commit(eq("foo"), eq("idem"), txRequestCaptor.capture(), eq(tableSupport));
    var change = txRequestCaptor.getValue().tableChanges().get(0);
    assertEquals(3, change.updates().size());
    assertEquals("add-schema", change.updates().get(0).get("action"));
    assertEquals("set-properties", change.updates().get(1).get("action"));
    assertEquals("set-properties", change.updates().get(2).get("action"));
    verify(stagedTableService).deleteStage(staged.key());
  }

  @Test
  void commitInjectsStagedMetadataLocationWhenCallerProvidesCreateInitialization() {
    when(tableSupport.resolveTableId(eq("catalog"), eq(List.of("db")), eq("orders")))
        .thenThrow(io.grpc.Status.NOT_FOUND.asRuntimeException())
        .thenReturn(ResourceId.newBuilder().setId("cat:db:orders").build());
    Table created = tableRecord("cat:db:orders");
    when(tableSupport.getTable(ResourceId.newBuilder().setId("cat:db:orders").build()))
        .thenReturn(created);
    doReturn(Response.noContent().build()).when(service).commit(any(), any(), any(), any());

    StagedTableEntry staged =
        new StagedTableEntry(
            new StagedTableKey("account-1", "catalog", List.of("db"), "orders", "stage-1"),
            ResourceId.newBuilder().setId("cat").build(),
            ResourceId.newBuilder().setId("cat:db").build(),
            createRequestWithMetadataLocation(),
            ai.floedb.floecat.catalog.rpc.TableSpec.newBuilder().build(),
            List.of(Map.of("type", "assert-create")),
            StageState.STAGED,
            Instant.now(),
            Instant.now(),
            "idem");
    when(stagedTableService.getStage(staged.key())).thenReturn(Optional.of(staged));

    TableRequests.Commit duckdbStyleCommit =
        new TableRequests.Commit(
            List.of(Map.of("type", "assert-create")),
            List.of(
                Map.of("action", "assign-uuid", "uuid", "uuid-1"),
                Map.of(
                    "action",
                    "add-schema",
                    "schema",
                    Map.of(
                        "schema-id", 1, "last-column-id", 1, "type", "struct", "fields", List.of()),
                    "last-column-id",
                    1)));

    TableMetadataView metadataView =
        new TableMetadataView(
            2,
            null,
            null,
            "s3://warehouse/db/orders/metadata/00001.metadata.json",
            null,
            Map.of(),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            List.of(),
            List.of(),
            List.of(),
            Map.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());
    when(icebergMetadataService.resolveCurrentIcebergMetadata(any(), any())).thenReturn(null);
    when(icebergMetadataService.resolveMetadata(any(), any(), any(), any(), any()))
        .thenReturn(new IcebergMetadataService.ResolvedMetadata(null, metadataView, null));

    Response response = service.commitTable(commandWithStage(duckdbStyleCommit, "stage-1"));

    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    ArgumentCaptor<TransactionCommitRequest> txRequestCaptor =
        ArgumentCaptor.forClass(TransactionCommitRequest.class);
    verify(service).commit(eq("foo"), eq("idem"), txRequestCaptor.capture(), eq(tableSupport));
    var updates = txRequestCaptor.getValue().tableChanges().get(0).updates();
    assertEquals(3, updates.size());
    assertEquals("assign-uuid", updates.get(0).get("action"));
    assertEquals("add-schema", updates.get(1).get("action"));
    assertEquals("set-properties", updates.get(2).get("action"));
    @SuppressWarnings("unchecked")
    Map<String, String> propertyUpdates = (Map<String, String>) updates.get(2).get("updates");
    assertEquals(
        "s3://warehouse/db/orders/metadata/00000-abc.metadata.json",
        propertyUpdates.get("metadata-location"));
    CommitTableResponseDto body =
        assertInstanceOf(CommitTableResponseDto.class, response.getEntity());
    assertEquals(
        "s3://warehouse/db/orders/metadata/00000-abc.metadata.json", body.metadataLocation());
  }

  @Test
  void commitRejectsDeltaWhenCompatReadOnlyEnabled() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    Table deltaTable =
        Table.newBuilder()
            .setResourceId(tableId)
            .setUpstream(UpstreamRef.newBuilder().setFormat(TableFormat.TF_DELTA).build())
            .build();

    when(deltaCompatConfig.enabled()).thenReturn(true);
    when(deltaCompatConfig.readOnly()).thenReturn(true);
    when(tableSupport.resolveTableId(eq("catalog"), eq(List.of("db")), eq("orders")))
        .thenReturn(tableId);
    when(tableSupport.getTable(tableId)).thenReturn(deltaTable);

    Response response = service.commitTable(command(emptyCommitRequest()));

    assertEquals(Response.Status.CONFLICT.getStatusCode(), response.getStatus());
    verify(service, never()).commit(any(), any(), any(), any());
  }

  private TableRequests.Commit emptyCommitRequest() {
    return new TableRequests.Commit(List.of(), List.of());
  }

  private TableRequests.Commit commitWithSingleUpdate() {
    return new TableRequests.Commit(List.of(), List.of(Map.of("action", "set-properties")));
  }

  private TableRequests.Create createRequest() {
    com.fasterxml.jackson.databind.ObjectMapper mapper =
        new com.fasterxml.jackson.databind.ObjectMapper();
    var schema =
        mapper
            .createObjectNode()
            .put("schema-id", 1)
            .put("last-column-id", 1)
            .put("type", "struct")
            .set(
                "fields",
                mapper
                    .createArrayNode()
                    .add(
                        mapper
                            .createObjectNode()
                            .put("id", 1)
                            .put("name", "id")
                            .put("required", true)
                            .put("type", "long")));
    return new TableRequests.Create("orders", schema, null, Map.of(), null, null, true);
  }

  private TableRequests.Create createRequestWithMetadataLocation() {
    TableRequests.Create base = createRequest();
    return new TableRequests.Create(
        base.name(),
        base.schema(),
        base.location(),
        Map.of("metadata-location", "s3://warehouse/db/orders/metadata/00000-abc.metadata.json"),
        base.partitionSpec(),
        base.writeOrder(),
        base.stageCreate());
  }

  private Table tableRecord(String id) {
    return Table.newBuilder()
        .setResourceId(ResourceId.newBuilder().setId(id))
        .putProperties("location", "s3://warehouse/db/orders")
        .putProperties("metadata-location", "s3://warehouse/db/orders/metadata/00001.metadata.json")
        .build();
  }

  private TransactionCommitService.CommitCommand command(TableRequests.Commit request) {
    return commandWithStage(request, null);
  }

  private TransactionCommitService.CommitCommand commandWithStage(
      TableRequests.Commit request, String stageId) {
    return new TransactionCommitService.CommitCommand(
        "foo",
        "db",
        List.of("db"),
        "orders",
        "catalog",
        ResourceId.newBuilder().setId("cat").build(),
        ResourceId.newBuilder().setId("cat:db").build(),
        "idem",
        stageId,
        null,
        request,
        tableSupport);
  }
}
