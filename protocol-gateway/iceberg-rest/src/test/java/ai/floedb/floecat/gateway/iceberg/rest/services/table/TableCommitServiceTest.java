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

package ai.floedb.floecat.gateway.iceberg.rest.services.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
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
import ai.floedb.floecat.gateway.iceberg.rest.services.account.AccountContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.floecat.gateway.iceberg.rest.services.compat.TableFormatSupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.staging.StageState;
import ai.floedb.floecat.gateway.iceberg.rest.services.staging.StagedTableEntry;
import ai.floedb.floecat.gateway.iceberg.rest.services.staging.StagedTableKey;
import ai.floedb.floecat.gateway.iceberg.rest.services.staging.StagedTableService;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.transaction.TransactionCommitService;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import jakarta.ws.rs.core.Response;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class TableCommitServiceTest {
  private final TableCommitService service = new TableCommitService();
  private final IcebergGatewayConfig config = org.mockito.Mockito.mock(IcebergGatewayConfig.class);
  private final IcebergGatewayConfig.DeltaCompatConfig deltaCompatConfig =
      org.mockito.Mockito.mock(IcebergGatewayConfig.DeltaCompatConfig.class);
  private final TableLifecycleService tableLifecycleService =
      org.mockito.Mockito.mock(TableLifecycleService.class);
  private final CommitResponseBuilder responseBuilder =
      org.mockito.Mockito.mock(CommitResponseBuilder.class);
  private final TableGatewaySupport tableSupport =
      org.mockito.Mockito.mock(TableGatewaySupport.class);
  private final TransactionCommitService transactionCommitService =
      org.mockito.Mockito.mock(TransactionCommitService.class);
  private final TableCreateTransactionMapper tableCreateTransactionMapper =
      org.mockito.Mockito.mock(TableCreateTransactionMapper.class);
  private final StagedTableService stagedTableService =
      org.mockito.Mockito.mock(StagedTableService.class);
  private final AccountContext accountContext = org.mockito.Mockito.mock(AccountContext.class);

  @BeforeEach
  void setUp() {
    service.config = config;
    service.tableLifecycleService = tableLifecycleService;
    service.responseBuilder = responseBuilder;
    service.tableFormatSupport = new TableFormatSupport();
    service.transactionCommitService = transactionCommitService;
    service.tableCreateTransactionMapper = tableCreateTransactionMapper;
    service.stagedTableService = stagedTableService;
    service.accountContext = accountContext;

    when(config.deltaCompat()).thenReturn(Optional.of(deltaCompatConfig));
    when(deltaCompatConfig.enabled()).thenReturn(false);
    when(deltaCompatConfig.readOnly()).thenReturn(true);
    when(accountContext.getAccountId()).thenReturn("account-1");
    when(stagedTableService.findSingleStage(any(), any(), any(), any()))
        .thenReturn(Optional.empty());
  }

  @Test
  void commitValidatesRequiredBody() {
    Response nullCommand = service.commit(null);
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), nullCommand.getStatus());

    Response nullRequest = service.commit(command(null));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), nullRequest.getStatus());
  }

  @Test
  void commitReturnsAtomicCommitErrorDirectly() {
    when(transactionCommitService.commit(any(), any(), any(), any()))
        .thenReturn(Response.status(Response.Status.CONFLICT).build());
    Table table = tableRecord("cat:db:orders");
    when(tableLifecycleService.resolveTableId(eq("catalog"), eq(List.of("db")), eq("orders")))
        .thenReturn(table.getResourceId());
    when(tableLifecycleService.getTable(table.getResourceId())).thenReturn(table);

    Response response = service.commit(command(commitWithSingleUpdate()));

    assertEquals(Response.Status.CONFLICT.getStatusCode(), response.getStatus());
  }

  @Test
  void commitReturnsConflictWhenStageIsAborted() {
    when(tableLifecycleService.resolveTableId(eq("catalog"), eq(List.of("db")), eq("orders")))
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

    Response response = service.commit(commandWithStage(emptyCommitRequest(), "stage-1"));

    assertEquals(Response.Status.CONFLICT.getStatusCode(), response.getStatus());
    verify(transactionCommitService, never()).commit(any(), any(), any(), any());
  }

  @Test
  void commitDelegatesToAtomicTransactionAndBuildsResponse() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    Table table = tableRecord("cat:db:orders");
    when(tableLifecycleService.resolveTableId(eq("catalog"), eq(List.of("db")), eq("orders")))
        .thenReturn(tableId);
    when(tableLifecycleService.getTable(tableId)).thenReturn(table);
    when(transactionCommitService.commit(any(), any(), any(), any()))
        .thenReturn(Response.noContent().build());
    when(tableSupport.loadCurrentMetadata(table)).thenReturn(IcebergMetadata.getDefaultInstance());

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
    when(responseBuilder.removedSnapshotIds(any())).thenReturn(Set.of());
    when(responseBuilder.buildInitialResponse(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(dto);
    when(responseBuilder.buildFinalResponse(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(dto);

    Response response = service.commit(command(commitWithSingleUpdate()));

    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    ArgumentCaptor<TransactionCommitRequest> txRequestCaptor =
        ArgumentCaptor.forClass(TransactionCommitRequest.class);
    verify(transactionCommitService)
        .commit(eq("foo"), eq("idem"), txRequestCaptor.capture(), eq(tableSupport));
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
    when(tableLifecycleService.resolveTableId(eq("catalog"), eq(List.of("db")), eq("orders")))
        .thenReturn(tableId);
    when(tableLifecycleService.getTable(tableId)).thenReturn(table);
    when(transactionCommitService.commit(any(), any(), any(), any()))
        .thenReturn(Response.noContent().build());
    when(tableSupport.loadCurrentMetadata(table)).thenReturn(IcebergMetadata.getDefaultInstance());

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
    when(responseBuilder.removedSnapshotIds(any())).thenReturn(Set.of());
    when(responseBuilder.buildInitialResponse(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(dto);
    when(responseBuilder.buildFinalResponse(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(dto);

    Response response = service.commit(command(request));

    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    ArgumentCaptor<TransactionCommitRequest> txRequestCaptor =
        ArgumentCaptor.forClass(TransactionCommitRequest.class);
    verify(transactionCommitService)
        .commit(eq("foo"), eq("idem"), txRequestCaptor.capture(), eq(tableSupport));
    var updates = txRequestCaptor.getValue().tableChanges().get(0).updates();
    assertEquals(
        request.updates(),
        updates,
        "single-table commit should forward caller updates unchanged at this layer");
  }

  @Test
  void commitMergesStagedCreateIntoSingleAtomicTransactionWhenTableMissing() {
    when(tableLifecycleService.resolveTableId(eq("catalog"), eq(List.of("db")), eq("orders")))
        .thenThrow(io.grpc.Status.NOT_FOUND.asRuntimeException())
        .thenReturn(ResourceId.newBuilder().setId("cat:db:orders").build());
    Table created = tableRecord("cat:db:orders");
    when(tableLifecycleService.getTable(ResourceId.newBuilder().setId("cat:db:orders").build()))
        .thenReturn(created);
    when(tableSupport.loadCurrentMetadata(created))
        .thenReturn(IcebergMetadata.getDefaultInstance());
    when(transactionCommitService.commit(any(), any(), any(), any()))
        .thenReturn(Response.noContent().build());

    StagedTableEntry staged =
        new StagedTableEntry(
            new StagedTableKey("account-1", "catalog", List.of("db"), "orders", "stage-1"),
            ResourceId.newBuilder().setId("cat").build(),
            ResourceId.newBuilder().setId("cat:db").build(),
            createRequest(),
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
    when(tableCreateTransactionMapper.buildCreateRequest(any(), any(), any(), any(), any(), any()))
        .thenReturn(stagedCreateTx);

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
    when(responseBuilder.removedSnapshotIds(any())).thenReturn(Set.of());
    when(responseBuilder.buildFinalResponse(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(dto);

    Response response = service.commit(commandWithStage(commitWithSingleUpdate(), "stage-1"));

    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    ArgumentCaptor<TransactionCommitRequest> txRequestCaptor =
        ArgumentCaptor.forClass(TransactionCommitRequest.class);
    verify(transactionCommitService)
        .commit(eq("foo"), eq("idem"), txRequestCaptor.capture(), eq(tableSupport));
    var change = txRequestCaptor.getValue().tableChanges().get(0);
    assertEquals(2, change.updates().size());
    assertEquals("add-schema", change.updates().get(0).get("action"));
    assertEquals("set-properties", change.updates().get(1).get("action"));
    verify(stagedTableService).deleteStage(staged.key());
  }

  @Test
  void commitUsesStageIdAsIdempotencyKeyWhenCallerDidNotProvideOne() {
    when(tableLifecycleService.resolveTableId(eq("catalog"), eq(List.of("db")), eq("orders")))
        .thenThrow(io.grpc.Status.NOT_FOUND.asRuntimeException())
        .thenReturn(ResourceId.newBuilder().setId("cat:db:orders").build());
    Table created = tableRecord("cat:db:orders");
    when(tableLifecycleService.getTable(ResourceId.newBuilder().setId("cat:db:orders").build()))
        .thenReturn(created);
    when(tableSupport.loadCurrentMetadata(created))
        .thenReturn(IcebergMetadata.getDefaultInstance());
    when(transactionCommitService.commit(any(), any(), any(), any()))
        .thenReturn(Response.noContent().build());

    StagedTableEntry staged =
        new StagedTableEntry(
            new StagedTableKey("account-1", "catalog", List.of("db"), "orders", "stage-xyz"),
            ResourceId.newBuilder().setId("cat").build(),
            ResourceId.newBuilder().setId("cat:db").build(),
            createRequest(),
            ai.floedb.floecat.catalog.rpc.TableSpec.newBuilder().build(),
            List.of(Map.of("type", "assert-create")),
            StageState.STAGED,
            Instant.now(),
            Instant.now(),
            null);
    when(stagedTableService.getStage(staged.key())).thenReturn(Optional.of(staged));

    TransactionCommitRequest stagedCreateTx =
        new TransactionCommitRequest(
            List.of(
                new TransactionCommitRequest.TableChange(
                    new ai.floedb.floecat.gateway.iceberg.rest.api.dto.TableIdentifierDto(
                        List.of("db"), "orders"),
                    List.of(Map.of("type", "assert-create")),
                    List.of(Map.of("action", "add-schema")))));
    when(tableCreateTransactionMapper.buildCreateRequest(any(), any(), any(), any(), any(), any()))
        .thenReturn(stagedCreateTx);

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
    when(responseBuilder.removedSnapshotIds(any())).thenReturn(Set.of());
    when(responseBuilder.buildFinalResponse(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(dto);

    Response response =
        service.commit(commandWithStageAndIdempotency(commitWithSingleUpdate(), "stage-xyz", null));

    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    verify(transactionCommitService).commit(eq("foo"), eq("stage-xyz"), any(), eq(tableSupport));
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
    when(tableLifecycleService.resolveTableId(eq("catalog"), eq(List.of("db")), eq("orders")))
        .thenReturn(tableId);
    when(tableLifecycleService.getTable(tableId)).thenReturn(deltaTable);

    Response response = service.commit(command(emptyCommitRequest()));

    assertEquals(Response.Status.CONFLICT.getStatusCode(), response.getStatus());
    verify(transactionCommitService, never()).commit(any(), any(), any(), any());
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

  private Table tableRecord(String id) {
    return Table.newBuilder()
        .setResourceId(ResourceId.newBuilder().setId(id))
        .putProperties("location", "s3://warehouse/db/orders")
        .putProperties("metadata-location", "s3://warehouse/db/orders/metadata/00001.metadata.json")
        .build();
  }

  private TableCommitService.CommitCommand command(TableRequests.Commit request) {
    return commandWithStage(request, null);
  }

  private TableCommitService.CommitCommand commandWithStage(
      TableRequests.Commit request, String stageId) {
    return commandWithStageAndIdempotency(request, stageId, "idem");
  }

  private TableCommitService.CommitCommand commandWithStageAndIdempotency(
      TableRequests.Commit request, String stageId, String idempotencyKey) {
    return new TableCommitService.CommitCommand(
        "foo",
        "db",
        List.of("db"),
        "orders",
        "catalog",
        ResourceId.newBuilder().setId("cat").build(),
        ResourceId.newBuilder().setId("cat:db").build(),
        idempotencyKey,
        stageId,
        null,
        null,
        request,
        tableSupport);
  }
}
