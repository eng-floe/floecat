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

package ai.floedb.floecat.gateway.iceberg.rest.table.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
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
import ai.floedb.floecat.gateway.iceberg.rest.table.StageState;
import ai.floedb.floecat.gateway.iceberg.rest.table.StagedTableEntry;
import ai.floedb.floecat.gateway.iceberg.rest.table.StagedTableKey;
import ai.floedb.floecat.gateway.iceberg.rest.table.StagedTableRepository;
import jakarta.ws.rs.core.Response;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class TableCommitServiceTest {
  private final TransactionCommitService service = Mockito.spy(new TransactionCommitService());
  private final IcebergGatewayConfig config = Mockito.mock(IcebergGatewayConfig.class);
  private final IcebergGatewayConfig.DeltaCompatConfig deltaCompatConfig =
      Mockito.mock(IcebergGatewayConfig.DeltaCompatConfig.class);
  private final TableGatewaySupport tableSupport = Mockito.mock(TableGatewaySupport.class);
  private final StagedTableRepository stagedTableRepository =
      Mockito.mock(StagedTableRepository.class);
  private final AccountContext accountContext = Mockito.mock(AccountContext.class);
  private final CreateCommitNormalizer createCommitNormalizer =
      Mockito.mock(CreateCommitNormalizer.class);
  private final TableCommitResponseService tableCommitResponseService =
      Mockito.mock(TableCommitResponseService.class);
  private final CommitRequestValidationHelper validationHelper =
      new CommitRequestValidationHelper();

  @BeforeEach
  void setUp() {
    service.config = config;
    service.tableFormatSupport = new TableFormatSupport();
    service.stagedTableRepository = stagedTableRepository;
    service.accountContext = accountContext;
    service.validationHelper = validationHelper;
    service.createCommitNormalizer = createCommitNormalizer;
    service.tableCommitResponseService = tableCommitResponseService;

    when(config.deltaCompat()).thenReturn(Optional.of(deltaCompatConfig));
    when(deltaCompatConfig.enabled()).thenReturn(false);
    when(deltaCompatConfig.readOnly()).thenReturn(true);
    when(accountContext.getAccountId()).thenReturn("account-1");
    when(stagedTableRepository.findSingleStage(any(), any(), any(), any()))
        .thenReturn(Optional.empty());
    when(createCommitNormalizer.normalizeFirstWriteCommit(
            any(), any(), any(), any(), any(Boolean.class), any(), any(), any()))
        .thenAnswer(invocation -> invocation.getArgument(6, TableRequests.Commit.class));
    when(tableCommitResponseService.buildCommitResponse(any(), any(), any()))
        .thenReturn(Response.ok(defaultCommitResponse()).build());
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
    StagedTableEntry staged = stagedEntry(StageState.ABORTED, "stage-1");
    when(stagedTableRepository.getStage(staged.key())).thenReturn(Optional.of(staged));

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

    Response response = service.commitTable(command(commitWithSingleUpdate()));

    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    CommitTableResponseDto body =
        assertInstanceOf(CommitTableResponseDto.class, response.getEntity());
    assertEquals("s3://warehouse/db/orders/metadata/00001.metadata.json", body.metadataLocation());

    ArgumentCaptor<TransactionCommitRequest> txRequestCaptor =
        ArgumentCaptor.forClass(TransactionCommitRequest.class);
    verify(service).commit(eq("foo"), eq("idem"), txRequestCaptor.capture(), eq(tableSupport));
    TransactionCommitRequest txRequest = txRequestCaptor.getValue();
    assertEquals(1, txRequest.tableChanges().size());
    assertEquals("orders", txRequest.tableChanges().get(0).identifier().name());
    assertTrue(txRequest.tableChanges().get(0).identifier().namespace().contains("db"));
  }

  @Test
  void commitDeletesStageAfterSuccessfulResponseBuild() {
    when(tableSupport.resolveTableId(eq("catalog"), eq(List.of("db")), eq("orders")))
        .thenThrow(io.grpc.Status.NOT_FOUND.asRuntimeException());
    doReturn(Response.noContent().build()).when(service).commit(any(), any(), any(), any());
    StagedTableEntry staged = stagedEntry(StageState.STAGED, "stage-1");
    when(stagedTableRepository.getStage(staged.key())).thenReturn(Optional.of(staged));

    Response response = service.commitTable(commandWithStage(commitWithSingleUpdate(), "stage-1"));

    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    verify(stagedTableRepository).deleteStage(staged.key());
  }

  @Test
  void commitDeletesStageEvenWhenResponseBuildFails() {
    when(tableSupport.resolveTableId(eq("catalog"), eq(List.of("db")), eq("orders")))
        .thenThrow(io.grpc.Status.NOT_FOUND.asRuntimeException());
    doReturn(Response.noContent().build()).when(service).commit(any(), any(), any(), any());
    StagedTableEntry staged = stagedEntry(StageState.STAGED, "stage-1");
    when(stagedTableRepository.getStage(staged.key())).thenReturn(Optional.of(staged));
    doThrow(new IllegalStateException("response build failed"))
        .when(tableCommitResponseService)
        .buildCommitResponse(any(), any(), eq(staged));

    IllegalStateException error =
        assertThrows(
            IllegalStateException.class,
            () -> service.commitTable(commandWithStage(commitWithSingleUpdate(), "stage-1")));

    assertEquals("response build failed", error.getMessage());
    verify(stagedTableRepository).deleteStage(staged.key());
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

  private CommitTableResponseDto defaultCommitResponse() {
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
    return new CommitTableResponseDto(metadataView.metadataLocation(), metadataView);
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

  private StagedTableEntry stagedEntry(StageState state, String stageId) {
    return new StagedTableEntry(
        new StagedTableKey("account-1", "catalog", List.of("db"), "orders", stageId),
        ResourceId.newBuilder().setId("cat").build(),
        ResourceId.newBuilder().setId("cat:db").build(),
        createRequest(),
        ai.floedb.floecat.catalog.rpc.TableSpec.newBuilder().build(),
        List.of(Map.of("type", "assert-create")),
        state,
        Instant.now(),
        Instant.now(),
        "idem");
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
