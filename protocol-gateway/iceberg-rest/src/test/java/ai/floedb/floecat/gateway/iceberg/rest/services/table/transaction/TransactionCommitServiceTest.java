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

package ai.floedb.floecat.gateway.iceberg.rest.services.table.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.GetSnapshotResponse;
import ai.floedb.floecat.catalog.rpc.GetTableResponse;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.Error;
import ai.floedb.floecat.common.rpc.ErrorCode;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.connector.rpc.ConnectorState;
import ai.floedb.floecat.connector.rpc.GetConnectorRequest;
import ai.floedb.floecat.connector.rpc.GetConnectorResponse;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.CommitTableResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TableIdentifierDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergError;
import ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergErrorResponse;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TransactionCommitRequest;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.CatalogRef;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.ResourceResolver;
import ai.floedb.floecat.gateway.iceberg.rest.common.RefPropertyUtil;
import ai.floedb.floecat.gateway.iceberg.rest.common.SnapshotMetadataUtil;
import ai.floedb.floecat.gateway.iceberg.rest.services.account.AccountContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.GrpcServiceFacade;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.MaterializeMetadataResult;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.CommitResponseBuilder;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.ConnectorProvisioningService;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.TableCommitPlanner;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.TableCreateTransactionMapper;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.TablePropertyService;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.materialization.TableCommitMaterializationService;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.metadata.TableCommitMetadataMutator;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergRef;
import ai.floedb.floecat.storage.kv.Keys;
import ai.floedb.floecat.transaction.rpc.BeginTransactionResponse;
import ai.floedb.floecat.transaction.rpc.CommitTransactionResponse;
import ai.floedb.floecat.transaction.rpc.GetTransactionResponse;
import ai.floedb.floecat.transaction.rpc.PrepareTransactionResponse;
import ai.floedb.floecat.transaction.rpc.ReserveTransactionTableIdResponse;
import ai.floedb.floecat.transaction.rpc.Transaction;
import ai.floedb.floecat.transaction.rpc.TransactionState;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import jakarta.ws.rs.core.Response;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class TransactionCommitServiceTest {
  private final TransactionCommitService service = new TransactionCommitService();
  private final TablePropertyService tablePropertyService = new TablePropertyService();
  private final ConnectorProvisioningService connectorProvisioningService =
      new ConnectorProvisioningService();
  private final AccountContext accountContext = Mockito.mock(AccountContext.class);
  private final ResourceResolver resourceResolver = Mockito.mock(ResourceResolver.class);
  private final TransactionCommitExecutionSupport transactionCommitExecutionSupport =
      new TransactionCommitExecutionSupport();
  private final TransactionCommitTablePlanningSupport tablePlanningSupport =
      new TransactionCommitTablePlanningSupport();
  private final TransactionCommitSnapshotSupport snapshotSupport =
      new TransactionCommitSnapshotSupport();
  private final TableLifecycleService tableLifecycleService =
      Mockito.mock(TableLifecycleService.class);
  private final TableCommitPlanner tableCommitPlanner = Mockito.mock(TableCommitPlanner.class);
  private final TableCreateTransactionMapper tableCreateTransactionMapper =
      Mockito.mock(TableCreateTransactionMapper.class);
  private final TableCommitMaterializationService materializationService =
      Mockito.mock(TableCommitMaterializationService.class);
  private final CommitResponseBuilder responseBuilder = Mockito.mock(CommitResponseBuilder.class);
  private final TableCommitMetadataMutator metadataMutator =
      Mockito.mock(TableCommitMetadataMutator.class);
  private final GrpcServiceFacade grpcClient = Mockito.mock(GrpcServiceFacade.class);
  private final TableGatewaySupport tableSupport = Mockito.mock(TableGatewaySupport.class);

  @BeforeEach
  void setUp() {
    service.accountContext = accountContext;
    service.resourceResolver = resourceResolver;
    service.transactionCommitExecutionSupport = transactionCommitExecutionSupport;
    service.tablePlanningSupport = tablePlanningSupport;
    service.snapshotSupport = snapshotSupport;
    service.tableLifecycleService = tableLifecycleService;
    service.tableCreateTransactionMapper = tableCreateTransactionMapper;
    service.connectorProvisioningService = connectorProvisioningService;
    installField(connectorProvisioningService, "grpcClient", grpcClient);
    transactionCommitExecutionSupport.grpcClient = grpcClient;
    tablePlanningSupport.transactionCommitExecutionSupport = transactionCommitExecutionSupport;
    tablePlanningSupport.tableLifecycleService = tableLifecycleService;
    tablePlanningSupport.grpcClient = grpcClient;
    tablePlanningSupport.tableCommitPlanner = tableCommitPlanner;
    tablePlanningSupport.responseBuilder = responseBuilder;
    tablePlanningSupport.tablePropertyService = tablePropertyService;
    tablePlanningSupport.materializationService = materializationService;
    snapshotSupport.transactionCommitExecutionSupport = transactionCommitExecutionSupport;
    snapshotSupport.tablePropertyService = tablePropertyService;
    snapshotSupport.grpcClient = grpcClient;

    when(accountContext.getAccountId()).thenReturn("acct-1");
    when(resourceResolver.catalog(any()))
        .thenReturn(
            new CatalogRef(
                "pref",
                "cat",
                ResourceId.newBuilder().setAccountId("acct-1").setId("cat-id").build()));
    when(tableLifecycleService.resolveNamespaceId(any(), Mockito.<List<String>>any()))
        .thenReturn(ResourceId.newBuilder().setAccountId("acct-1").setId("ns-id").build());

    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-id").build();
    when(tableLifecycleService.resolveTableId(any(), Mockito.<List<String>>any(), any()))
        .thenReturn(tableId);

    Table table = defaultPlannedTable(tableId);
    when(tableLifecycleService.getTableResponse(any()))
        .thenReturn(
            GetTableResponse.newBuilder()
                .setTable(table)
                .setMeta(MutationMeta.newBuilder().setPointerVersion(7L))
                .build());
    when(tableCommitPlanner.plan(any(), any(), any(), any()))
        .thenReturn(new TableCommitPlanner.PlanResult(table, null));
    when(responseBuilder.buildInitialResponse(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(defaultCommitResponse());
    when(metadataMutator.apply(any(), any())).thenAnswer(invocation -> invocation.getArgument(0));
    when(tableSupport.loadCurrentMetadata(any(Table.class))).thenReturn(null);
    when(tableSupport.connectorIntegrationEnabled()).thenReturn(true);
    when(grpcClient.reserveTransactionTableId(any()))
        .thenReturn(ReserveTransactionTableIdResponse.newBuilder().setTableId(tableId).build());
    when(tableSupport.resolveTableLocation(any(), any()))
        .thenAnswer(
            invocation -> {
              String requestedLocation = invocation.getArgument(0, String.class);
              String metadataLocation = invocation.getArgument(1, String.class);
              if (requestedLocation != null && !requestedLocation.isBlank()) {
                return requestedLocation;
              }
              if (metadataLocation == null || metadataLocation.isBlank()) {
                return null;
              }
              int idx = metadataLocation.indexOf("/metadata/");
              if (idx > 0) {
                return metadataLocation.substring(0, idx);
              }
              int slash = metadataLocation.lastIndexOf('/');
              return slash > 0 ? metadataLocation.substring(0, slash) : metadataLocation;
            });
    when(grpcClient.getConnector(any()))
        .thenAnswer(
            invocation -> {
              GetConnectorRequest request = invocation.getArgument(0, GetConnectorRequest.class);
              ResourceId connectorId = request.getConnectorId();
              return GetConnectorResponse.newBuilder()
                  .setConnector(
                      Connector.newBuilder()
                          .setResourceId(connectorId)
                          .setDisplayName("existing-" + connectorId.getId())
                          .setKind(ConnectorKind.CK_ICEBERG)
                          .setUri("s3://existing")
                          .setState(ConnectorState.CS_PAUSED)
                          .build())
                  .build();
            });
  }

  private static void installField(Object target, String fieldName, Object value) {
    try {
      var field = target.getClass().getDeclaredField(fieldName);
      field.setAccessible(true);
      field.set(target, value);
    } catch (ReflectiveOperationException e) {
      throw new AssertionError("Failed to inject test field " + fieldName, e);
    }
  }

  private CommitTableResponseDto defaultCommitResponse() {
    return new CommitTableResponseDto(
        "s3://meta/default/00001.metadata.json",
        new TableMetadataView(
            2,
            null,
            "s3://floecat/iceberg/orders",
            "s3://meta/default/00001.metadata.json",
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
            List.of()));
  }

  private Table defaultPlannedTable(ResourceId tableId) {
    return Table.newBuilder()
        .setResourceId(tableId)
        .putProperties("location", "s3://floecat/iceberg/orders")
        .build();
  }

  @Test
  void commitCreateBuildsMappedCreateRequestAndUsesTxPath() throws Exception {
    when(tableSupport.connectorIntegrationEnabled()).thenReturn(false);
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.getSnapshot(any()))
        .thenReturn(
            GetSnapshotResponse.newBuilder()
                .setSnapshot(Snapshot.newBuilder().setSnapshotId(44L).setSequenceNumber(2L).build())
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());
    when(tableLifecycleService.resolveTableId(any(), Mockito.<List<String>>any(), any()))
        .thenThrow(new StatusRuntimeException(Status.NOT_FOUND));

    TransactionCommitRequest createTxRequest =
        new TransactionCommitRequest(
            List.of(
                new TransactionCommitRequest.TableChange(
                    new TableIdentifierDto(List.of("db"), "orders"),
                    List.of(Map.of("type", "assert-create")),
                    List.of(
                        Map.of(
                            "action",
                            "set-properties",
                            "updates",
                            Map.of("metadata-location", "s3://meta/00001.metadata.json"))))));
    when(tableCreateTransactionMapper.buildCreateRequest(
            eq(List.of("db")),
            eq("orders"),
            any(ResourceId.class),
            any(ResourceId.class),
            any(TableRequests.Create.class),
            eq(tableSupport)))
        .thenReturn(createTxRequest);

    TableRequests.Create createRequest =
        new TableRequests.Create(
            "orders",
            mapper()
                .readTree(
                    """
                    {
                      "schema-id":1,
                      "last-column-id":1,
                      "type":"struct",
                      "fields":[{"id":1,"name":"id","required":true,"type":"long"}]
                    }
                    """),
            null,
            null,
            null,
            null,
            false);

    Response response =
        service.commitCreate(
            "pref",
            "idem",
            List.of("db"),
            "orders",
            ResourceId.newBuilder().setAccountId("acct-1").setId("cat-id").build(),
            ResourceId.newBuilder().setAccountId("acct-1").setId("ns-id").build(),
            createRequest,
            tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(tableCreateTransactionMapper)
        .buildCreateRequest(
            eq(List.of("db")),
            eq("orders"),
            any(ResourceId.class),
            any(ResourceId.class),
            eq(createRequest),
            eq(tableSupport));
    verify(grpcClient)
        .reserveTransactionTableId(
            argThat(
                request ->
                    request != null
                        && request.getTxId().equals("tx-1")
                        && request.getTableFq().equals("cat.db.orders")));
    verify(grpcClient)
        .prepareTransaction(
            argThat(
                prepare -> {
                  if (prepare == null || prepare.getChangesCount() < 1) {
                    return false;
                  }
                  var tableChange =
                      prepare.getChangesList().stream()
                          .filter(change -> change.hasTableId() && change.hasTable())
                          .findFirst()
                          .orElse(null);
                  if (tableChange == null || !tableChange.hasPrecondition()) {
                    return false;
                  }
                  return tableChange.hasTableId()
                      && tableChange.hasTable()
                      && tableChange.getPrecondition().getExpectedVersion() == 0L;
                }));
    verify(grpcClient).commitTransaction(any());
  }

  @Test
  void commitCreateAllowsMetadataOnlyPreMaterializationBeforeFirstSnapshot() throws Exception {
    when(tableSupport.connectorIntegrationEnabled()).thenReturn(false);
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.getSnapshot(any())).thenThrow(new StatusRuntimeException(Status.NOT_FOUND));
    when(grpcClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());
    when(tableLifecycleService.resolveTableId(any(), Mockito.<List<String>>any(), any()))
        .thenThrow(new StatusRuntimeException(Status.NOT_FOUND));

    TransactionCommitRequest createTxRequest =
        new TransactionCommitRequest(
            List.of(
                new TransactionCommitRequest.TableChange(
                    new TableIdentifierDto(List.of("db"), "orders"),
                    List.of(Map.of("type", "assert-create")),
                    List.of(
                        Map.of(
                            "action",
                            "set-properties",
                            "updates",
                            Map.of("metadata-location", "s3://meta/00001.metadata.json"))))));
    when(tableCreateTransactionMapper.buildCreateRequest(
            eq(List.of("db")),
            eq("orders"),
            any(ResourceId.class),
            any(ResourceId.class),
            any(TableRequests.Create.class),
            eq(tableSupport)))
        .thenReturn(createTxRequest);

    TableRequests.Create createRequest =
        new TableRequests.Create(
            "orders",
            mapper()
                .readTree(
                    """
                    {
                      "schema-id":1,
                      "last-column-id":1,
                      "type":"struct",
                      "fields":[{"id":1,"name":"id","required":true,"type":"long"}]
                    }
                    """),
            null,
            null,
            null,
            null,
            false);

    Response response =
        service.commitCreate(
            "pref",
            "idem",
            List.of("db"),
            "orders",
            ResourceId.newBuilder().setAccountId("acct-1").setId("cat-id").build(),
            ResourceId.newBuilder().setAccountId("acct-1").setId("ns-id").build(),
            createRequest,
            tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(grpcClient).reserveTransactionTableId(any());
    verify(grpcClient).commitTransaction(any());
  }

  @Test
  void commitReturnsNoContentWhenAlreadyAppliedWithoutReplayingSideEffects() {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(grpcClient, never()).prepareTransaction(any());
    verify(grpcClient, never()).commitTransaction(any());
    verify(grpcClient, never()).startCapture(any());
    verify(tableLifecycleService, never())
        .resolveTableId(any(), Mockito.<List<String>>any(), any());
    verify(tableCommitPlanner, never()).plan(any(), any(), any(), any());
  }

  private com.fasterxml.jackson.databind.ObjectMapper mapper() {
    return new com.fasterxml.jackson.databind.ObjectMapper();
  }

  @Test
  void commitReturnsConflictWhenExistingTransactionHashDiffers() {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-1")
                        .setState(TransactionState.TS_OPEN)
                        .putProperties("iceberg.commit.request-hash", "different-hash"))
                .build());

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.CONFLICT.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = assertInstanceOf(IcebergErrorResponse.class, response.getEntity());
    assertEquals("CommitFailedException", error.error().type());
    verify(grpcClient).abortTransaction(any());
    verify(grpcClient, never()).prepareTransaction(any());
    verify(grpcClient, never()).commitTransaction(any());
  }

  @Test
  void commitRejectsDuplicateTableIdentifiers() {
    Response response =
        service.commit("pref", "idem", requestWithDuplicateTableChanges(), tableSupport);

    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = assertInstanceOf(IcebergErrorResponse.class, response.getEntity());
    assertEquals("ValidationException", error.error().type());
    verify(grpcClient, never()).beginTransaction(any());
    verify(grpcClient, never()).prepareTransaction(any());
    verify(grpcClient, never()).commitTransaction(any());
  }

  @Test
  void commitReturnsNoContentWhenCommitReturnsApplied() {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.getSnapshot(any()))
        .thenReturn(
            GetSnapshotResponse.newBuilder()
                .setSnapshot(Snapshot.newBuilder().setSnapshotId(44L).setSequenceNumber(2L).build())
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(grpcClient)
        .prepareTransaction(
            argThat(
                prepare ->
                    prepare != null
                        && prepare.getChangesList().stream()
                            .anyMatch(
                                change ->
                                    change.hasTableId()
                                        && change.hasPrecondition()
                                        && change.getPrecondition().getExpectedVersion() == 7L)
                        && prepare.getChangesList().stream()
                            .noneMatch(
                                change ->
                                    change.hasTargetPointerKey()
                                        && change.getTargetPointerKey().contains("/tx-journal/"))));
  }

  @Test
  void commitReplansAgainstFreshTableVersionWhenPointerAdvancesDuringPlanning() {
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-id").build();
    Table staleTable =
        Table.newBuilder()
            .setResourceId(tableId)
            .putProperties("base", "stale")
            .putProperties("location", "s3://floecat/iceberg/orders")
            .putProperties(
                "metadata-location", "s3://floecat/iceberg/orders/metadata/00001.metadata.json")
            .build();
    Table freshTable =
        Table.newBuilder()
            .setResourceId(tableId)
            .putProperties("base", "fresh")
            .putProperties("location", "s3://floecat/iceberg/orders")
            .putProperties(
                "metadata-location", "s3://floecat/iceberg/orders/metadata/00002.metadata.json")
            .build();
    Table stalePlanned = staleTable.toBuilder().putProperties("planned-version", "stale").build();
    Table freshPlanned = freshTable.toBuilder().putProperties("planned-version", "fresh").build();
    when(tableLifecycleService.getTableResponse(any()))
        .thenReturn(
            GetTableResponse.newBuilder()
                .setTable(staleTable)
                .setMeta(MutationMeta.newBuilder().setPointerVersion(7L))
                .build())
        .thenReturn(
            GetTableResponse.newBuilder()
                .setTable(freshTable)
                .setMeta(MutationMeta.newBuilder().setPointerVersion(8L))
                .build())
        .thenReturn(
            GetTableResponse.newBuilder()
                .setTable(freshTable)
                .setMeta(MutationMeta.newBuilder().setPointerVersion(8L))
                .build());
    when(tableCommitPlanner.plan(any(), any(), any(), any()))
        .thenReturn(new TableCommitPlanner.PlanResult(stalePlanned, null))
        .thenReturn(new TableCommitPlanner.PlanResult(freshPlanned, null));
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(metadataMutator, never()).apply(any(), any());
    verify(grpcClient)
        .prepareTransaction(
            argThat(
                prepare ->
                    prepare.getChangesList().stream()
                        .anyMatch(
                            change ->
                                change.hasTableId()
                                    && change.hasPrecondition()
                                    && change.getPrecondition().getExpectedVersion() == 8L
                                    && "fresh"
                                        .equals(
                                            change
                                                .getTable()
                                                .getPropertiesMap()
                                                .get("planned-version")))));
  }

  @Test
  void commitAddsAtomicConnectorChangesForCreatePath() {
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-id").build();
    Table table =
        Table.newBuilder()
            .setResourceId(tableId)
            .putProperties(
                "metadata-location", "s3://floecat/iceberg/orders/metadata/00001.metadata.json")
            .putProperties("location", "s3://floecat/iceberg/orders")
            .build();
    when(tableLifecycleService.getTableResponse(any()))
        .thenReturn(
            GetTableResponse.newBuilder()
                .setTable(table)
                .setMeta(MutationMeta.newBuilder().setPointerVersion(7L))
                .build());
    when(tableCommitPlanner.plan(any(), any(), any(), any()))
        .thenReturn(new TableCommitPlanner.PlanResult(table, null));
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(grpcClient)
        .prepareTransaction(
            argThat(
                prepare ->
                    prepare.getChangesList().stream()
                                .filter(change -> change.hasConnectorProvisioning())
                                .count()
                            == 1
                        && prepare.getChangesList().stream()
                            .filter(change -> change.hasConnectorProvisioning())
                            .allMatch(
                                change ->
                                    "s3://floecat/iceberg/orders"
                                            .equals(
                                                change.getConnectorProvisioning().getConnectorUri())
                                        && "orders"
                                            .equals(
                                                change
                                                    .getConnectorProvisioning()
                                                    .getSourceTableName())
                                        && List.of("db")
                                            .equals(
                                                change
                                                    .getConnectorProvisioning()
                                                    .getSourceNamespacePathList())
                                        && "filesystem"
                                            .equals(
                                                change
                                                    .getConnectorProvisioning()
                                                    .getPropertiesMap()
                                                    .get("iceberg.source"))
                                        && !change
                                            .getConnectorProvisioning()
                                            .getPropertiesMap()
                                            .containsKey("metadata-location"))
                        && prepare.getChangesList().stream()
                            .anyMatch(
                                change ->
                                    change.hasTable()
                                        && "s3://floecat/iceberg/orders"
                                            .equals(
                                                change
                                                    .getTable()
                                                    .getPropertiesMap()
                                                    .get("location"))
                                        && !change
                                            .getTable()
                                            .getPropertiesMap()
                                            .containsKey("metadata-location"))));
  }

  @Test
  void commitFailsWhenProvisionedConnectorLocationIsMissing() {
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-id").build();
    Table table = Table.newBuilder().setResourceId(tableId).build();
    TableMetadataView defaultMetadata = defaultCommitResponse().metadata();
    when(responseBuilder.buildInitialResponse(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(
            new CommitTableResponseDto(
                null,
                new TableMetadataView(
                    defaultMetadata.formatVersion(),
                    defaultMetadata.tableUuid(),
                    null,
                    defaultMetadata.metadataLocation(),
                    defaultMetadata.lastUpdatedMs(),
                    defaultMetadata.properties(),
                    defaultMetadata.lastColumnId(),
                    defaultMetadata.currentSchemaId(),
                    defaultMetadata.defaultSpecId(),
                    defaultMetadata.lastPartitionId(),
                    defaultMetadata.defaultSortOrderId(),
                    defaultMetadata.currentSnapshotId(),
                    defaultMetadata.lastSequenceNumber(),
                    defaultMetadata.schemas(),
                    defaultMetadata.partitionSpecs(),
                    defaultMetadata.sortOrders(),
                    defaultMetadata.refs(),
                    defaultMetadata.snapshotLog(),
                    defaultMetadata.metadataLog(),
                    defaultMetadata.statistics(),
                    defaultMetadata.partitionStatistics(),
                    defaultMetadata.snapshots())));
    when(tableLifecycleService.getTableResponse(any()))
        .thenReturn(
            GetTableResponse.newBuilder()
                .setTable(table)
                .setMeta(MutationMeta.newBuilder().setPointerVersion(7L))
                .build());
    when(tableCommitPlanner.plan(any(), any(), any(), any()))
        .thenReturn(new TableCommitPlanner.PlanResult(table, null));
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.SERVICE_UNAVAILABLE.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = assertInstanceOf(IcebergErrorResponse.class, response.getEntity());
    assertEquals("CommitStateUnknownException", error.error().type());
    verify(grpcClient).abortTransaction(any());
    verify(grpcClient, never()).prepareTransaction(any());
    verify(grpcClient, never()).commitTransaction(any());
  }

  @Test
  void commitDoesNotMutateExistingConnectorTransactionally() {
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-id").build();
    ResourceId connectorId =
        ResourceId.newBuilder()
            .setAccountId("acct-1")
            .setId("conn-1")
            .setKind(ResourceKind.RK_CONNECTOR)
            .build();
    Table table =
        Table.newBuilder()
            .setResourceId(tableId)
            .setUpstream(UpstreamRef.newBuilder().setConnectorId(connectorId).build())
            .putProperties(
                "metadata-location", "s3://floecat/iceberg/orders/metadata/00002.metadata.json")
            .putProperties("location", "s3://floecat/iceberg/orders")
            .build();
    when(tableLifecycleService.getTableResponse(any()))
        .thenReturn(
            GetTableResponse.newBuilder()
                .setTable(table)
                .setMeta(MutationMeta.newBuilder().setPointerVersion(7L))
                .build());
    when(tableCommitPlanner.plan(any(), any(), any(), any()))
        .thenReturn(new TableCommitPlanner.PlanResult(table, null));
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.getSnapshot(any()))
        .thenReturn(
            GetSnapshotResponse.newBuilder()
                .setSnapshot(Snapshot.newBuilder().setSnapshotId(44L).setSequenceNumber(2L).build())
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(grpcClient)
        .prepareTransaction(
            argThat(
                prepare ->
                    prepare.getChangesList().stream()
                            .noneMatch(
                                change ->
                                    change.hasTargetPointerKey()
                                        && change.getTargetPointerKey().contains("/connectors/"))
                        && prepare.getChangesList().stream()
                            .anyMatch(
                                change ->
                                    change.hasTable()
                                        && change.getTable().hasUpstream()
                                        && connectorId.equals(
                                            change.getTable().getUpstream().getConnectorId()))));
  }

  @Test
  void commitPreMaterializesAndPublishesMetadataLocationAtomically() {
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-id").build();
    Table table = Table.newBuilder().setResourceId(tableId).build();
    when(tableLifecycleService.getTableResponse(any()))
        .thenReturn(
            GetTableResponse.newBuilder()
                .setTable(table)
                .setMeta(MutationMeta.newBuilder().setPointerVersion(7L))
                .build());
    when(tableCommitPlanner.plan(any(), any(), any(), any()))
        .thenReturn(new TableCommitPlanner.PlanResult(table, null));
    when(tableSupport.loadCurrentMetadata(any(Table.class)))
        .thenReturn(IcebergMetadata.getDefaultInstance());
    when(tableSupport.connectorIntegrationEnabled()).thenReturn(false);
    when(materializationService.materializeMetadata(any(), any(), any(), any(), any()))
        .thenReturn(MaterializeMetadataResult.success(null, "s3://meta/new/00002.metadata.json"));
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.getSnapshot(any()))
        .thenReturn(
            GetSnapshotResponse.newBuilder()
                .setSnapshot(Snapshot.newBuilder().setSnapshotId(44L).setSequenceNumber(2L).build())
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(grpcClient)
        .prepareTransaction(
            argThat(
                prepare ->
                    prepare.getChangesList().stream()
                        .map(change -> parseSnapshot(change.getPayload()))
                        .anyMatch(
                            snapshot ->
                                snapshot != null
                                    && "s3://meta/new/00002.metadata.json"
                                        .equals(SnapshotMetadataUtil.metadataLocation(snapshot)))));
  }

  @Test
  void commitWithSnapshotUpdatesPreMaterializesWhenMetadataLocationMissing() {
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-id").build();
    Table table = Table.newBuilder().setResourceId(tableId).build();
    when(tableLifecycleService.getTableResponse(any()))
        .thenReturn(
            GetTableResponse.newBuilder()
                .setTable(table)
                .setMeta(MutationMeta.newBuilder().setPointerVersion(7L))
                .build());
    when(tableCommitPlanner.plan(any(), any(), any(), any()))
        .thenReturn(new TableCommitPlanner.PlanResult(table, null));
    when(tableSupport.loadCurrentMetadata(any(Table.class)))
        .thenReturn(IcebergMetadata.getDefaultInstance());
    when(materializationService.materializeMetadata(any(), any(), any(), any(), any()))
        .thenReturn(MaterializeMetadataResult.success(null, "s3://meta/new/00002.metadata.json"));
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", requestWithAddSnapshot(123L), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(grpcClient)
        .prepareTransaction(
            argThat(
                prepare ->
                    prepare.getChangesList().stream()
                        .map(change -> parseSnapshot(change.getPayload()))
                        .anyMatch(
                            snapshot ->
                                snapshot != null
                                    && snapshot.getSnapshotId() == 123L
                                    && "s3://meta/new/00002.metadata.json"
                                        .equals(SnapshotMetadataUtil.metadataLocation(snapshot)))));
  }

  @Test
  void commitWithSnapshotUpdatesRematerializesWhenMetadataLocationAlreadyExists() {
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-id").build();
    Table table =
        Table.newBuilder()
            .setResourceId(tableId)
            .build();
    when(tableLifecycleService.getTableResponse(any()))
        .thenReturn(
            GetTableResponse.newBuilder()
                .setTable(table)
                .setMeta(MutationMeta.newBuilder().setPointerVersion(7L))
                .build());
    when(tableCommitPlanner.plan(any(), any(), any(), any()))
        .thenReturn(new TableCommitPlanner.PlanResult(table, null));
    when(tableSupport.loadCurrentMetadata(any(Table.class)))
        .thenReturn(IcebergMetadata.newBuilder().build());
    when(materializationService.materializeMetadata(any(), any(), any(), any(), any()))
        .thenReturn(MaterializeMetadataResult.success(null, "s3://meta/new/00002.metadata.json"));
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", requestWithAddSnapshot(123L), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(materializationService).materializeMetadata(any(), any(), any(), any(), any());
    verify(grpcClient)
        .prepareTransaction(
            argThat(
                prepare ->
                    prepare.getChangesList().stream()
                        .map(change -> parseSnapshot(change.getPayload()))
                        .anyMatch(
                            snapshot ->
                                snapshot != null
                                    && snapshot.getSnapshotId() == 123L
                                    && "s3://meta/new/00002.metadata.json"
                                        .equals(SnapshotMetadataUtil.metadataLocation(snapshot)))));
  }

  @Test
  void commitDoesNotEnqueueSnapshotScopedStatsCaptureForAddedSnapshots() {
    ResourceId connectorId = ResourceId.newBuilder().setAccountId("acct-1").setId("conn-1").build();
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-id").build())
            .setUpstream(UpstreamRef.newBuilder().setConnectorId(connectorId).build())
            .build();
    when(tableLifecycleService.getTableResponse(any()))
        .thenReturn(
            GetTableResponse.newBuilder()
                .setTable(table)
                .setMeta(MutationMeta.newBuilder().setPointerVersion(7L))
                .build());
    when(tableCommitPlanner.plan(any(), any(), any(), any()))
        .thenReturn(new TableCommitPlanner.PlanResult(table, null));
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", requestWithAddSnapshot(123L), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(grpcClient, never()).startCapture(any());
  }

  @Test
  void commitWithProvisionedConnectorAddsProvisioningChangeWithoutActivation() {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    ArgumentCaptor<ai.floedb.floecat.transaction.rpc.PrepareTransactionRequest> prepareCaptor =
        ArgumentCaptor.forClass(ai.floedb.floecat.transaction.rpc.PrepareTransactionRequest.class);
    when(grpcClient.prepareTransaction(prepareCaptor.capture()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", requestWithAddSnapshot(123L), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());

    assertEquals(
        1L,
        prepareCaptor.getValue().getChangesList().stream()
            .filter(change -> change.hasConnectorProvisioning())
            .count());
    verify(grpcClient, never()).updateConnector(any());
    verify(grpcClient, never()).startCapture(any());
  }

  @Test
  void commitWithExplicitMetadataLocationSkipsRematerialization() {
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-id").build();
    Table table =
        Table.newBuilder()
            .setResourceId(tableId)
            .build();
    when(tableLifecycleService.getTableResponse(any()))
        .thenReturn(
            GetTableResponse.newBuilder()
                .setTable(table)
                .setMeta(MutationMeta.newBuilder().setPointerVersion(7L))
                .build());
    when(tableCommitPlanner.plan(any(), any(), any(), any()))
        .thenReturn(new TableCommitPlanner.PlanResult(table, null));
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response =
        service.commit(
            "pref",
            "idem",
            requestWithSetMetadataLocationAndAddSnapshot(
                "s3://meta/original/00002.metadata.json", 123L),
            tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(materializationService, never()).materializeMetadata(any(), any(), any(), any(), any());
    verify(grpcClient)
        .prepareTransaction(
            argThat(
                prepare ->
                    prepare.getChangesList().stream()
                        .map(change -> parseSnapshot(change.getPayload()))
                        .anyMatch(
                            snapshot ->
                                snapshot != null
                                    && snapshot.getSnapshotId() == 123L
                                    && "s3://meta/original/00002.metadata.json"
                                        .equals(SnapshotMetadataUtil.metadataLocation(snapshot)))));
  }

  @Test
  void commitWithInvalidExplicitMetadataLocationReturnsValidationError() {
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-id").build();
    Table table =
        Table.newBuilder()
            .setResourceId(tableId)
            .putProperties("metadata-location", "s3://meta/original/00002.metadata.json")
            .build();
    when(tableLifecycleService.getTableResponse(any()))
        .thenReturn(
            GetTableResponse.newBuilder()
                .setTable(table)
                .setMeta(MutationMeta.newBuilder().setPointerVersion(7L))
                .build());
    when(tableCommitPlanner.plan(any(), any(), any(), any()))
        .thenReturn(new TableCommitPlanner.PlanResult(table, null));
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());

    Response response =
        service.commit(
            "pref",
            "idem",
            requestWithSetMetadataLocationAndAddSnapshot("not a uri", 123L),
            tableSupport);

    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    verify(materializationService, never()).materializeMetadata(any(), any(), any(), any(), any());
    verify(grpcClient, never()).prepareTransaction(any());
  }

  @Test
  void commitPreMaterializesWhenCurrentMetadataCannotBeLoaded() {
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-id").build();
    Table table = Table.newBuilder().setResourceId(tableId).build();
    when(tableLifecycleService.getTableResponse(any()))
        .thenReturn(
            GetTableResponse.newBuilder()
                .setTable(table)
                .setMeta(MutationMeta.newBuilder().setPointerVersion(7L))
                .build());
    when(tableCommitPlanner.plan(any(), any(), any(), any()))
        .thenReturn(new TableCommitPlanner.PlanResult(table, null));
    when(tableSupport.loadCurrentMetadata(any(Table.class)))
        .thenThrow(new RuntimeException("missing pointer"));
    when(materializationService.materializeMetadata(any(), any(), any(), any(), any()))
        .thenReturn(MaterializeMetadataResult.success(null, "s3://meta/new/00002.metadata.json"));
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(materializationService).materializeMetadata(any(), any(), any(), any(), any());
    verify(grpcClient).prepareTransaction(any());
  }

  @Test
  void commitWithConnectorRunsPostCommitStatsSyncWithoutConnectorTxChanges() {
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-id").build();
    ResourceId connectorId =
        ResourceId.newBuilder()
            .setAccountId("acct-1")
            .setId("conn-1")
            .setKind(ResourceKind.RK_CONNECTOR)
            .build();
    Table tableWithConnector =
        Table.newBuilder()
            .setResourceId(tableId)
            .setUpstream(UpstreamRef.newBuilder().setConnectorId(connectorId).build())
            .putProperties("metadata-location", "s3://meta/new")
            .build();
    when(tableLifecycleService.getTableResponse(any()))
        .thenReturn(
            GetTableResponse.newBuilder()
                .setTable(tableWithConnector)
                .setMeta(MutationMeta.newBuilder().setPointerVersion(7L))
                .build());
    when(tableCommitPlanner.plan(any(), any(), any(), any()))
        .thenReturn(new TableCommitPlanner.PlanResult(tableWithConnector, null));
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(grpcClient)
        .prepareTransaction(
            argThat(
                prepare ->
                    prepare != null
                        && prepare.getChangesCount() == 1
                        && prepare.getChangesList().stream()
                            .anyMatch(change -> change.hasTableId() && change.hasTable())
                        && prepare.getChangesList().stream()
                            .noneMatch(
                                change ->
                                    change.hasTargetPointerKey()
                                        && change.getTargetPointerKey().contains("/connectors/"))
                        && prepare.getChangesList().stream()
                            .noneMatch(
                                change ->
                                    change.hasTargetPointerKey()
                                        && change.getTargetPointerKey().contains("/snapshots/"))));
  }

  @Test
  void commitWithConnectorAndAddSnapshotSucceeds() {
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-id").build();
    ResourceId connectorId =
        ResourceId.newBuilder()
            .setAccountId("acct-1")
            .setId("conn-1")
            .setKind(ResourceKind.RK_CONNECTOR)
            .build();
    Table tableWithConnector =
        Table.newBuilder()
            .setResourceId(tableId)
            .setUpstream(UpstreamRef.newBuilder().setConnectorId(connectorId).build())
            .putProperties("metadata-location", "s3://meta/new")
            .build();
    when(tableLifecycleService.getTableResponse(any()))
        .thenReturn(
            GetTableResponse.newBuilder()
                .setTable(tableWithConnector)
                .setMeta(MutationMeta.newBuilder().setPointerVersion(7L))
                .build());
    when(tableCommitPlanner.plan(any(), any(), any(), any()))
        .thenReturn(new TableCommitPlanner.PlanResult(tableWithConnector, null));
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", requestWithAddSnapshot(123L), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
  }

  @Test
  void commitCreateFlowAtomicallyCreatesConnectorWhenTableHasNoConnector() {
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-id").build();
    Table tableWithoutConnector =
        Table.newBuilder()
            .setResourceId(tableId)
            .setCatalogId(ResourceId.newBuilder().setAccountId("acct-1").setId("cat-id").build())
            .build();
    when(tableLifecycleService.getTableResponse(any()))
        .thenReturn(
            GetTableResponse.newBuilder()
                .setTable(tableWithoutConnector)
                .setMeta(MutationMeta.newBuilder().setPointerVersion(7L))
                .build());
    when(tableCommitPlanner.plan(any(), any(), any(), any()))
        .thenReturn(new TableCommitPlanner.PlanResult(tableWithoutConnector, null));
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(grpcClient)
        .prepareTransaction(
            argThat(
                prepare ->
                    prepare != null
                        && prepare.getChangesCount() >= 2
                        && prepare.getChangesList().stream()
                                .filter(change -> change.hasConnectorProvisioning())
                                .count()
                            == 1
                        && prepare.getChangesList().stream()
                            .anyMatch(
                                change ->
                                    change.hasTable()
                                        && "s3://floecat/iceberg/orders"
                                            .equals(
                                                change
                                                    .getTable()
                                                    .getPropertiesMap()
                                                    .get("location")))));
  }

  @Test
  void alreadyAppliedWithAddSnapshotReturnsNoContent() {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", requestWithAddSnapshot(123L), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(grpcClient, never()).prepareTransaction(any());
    verify(grpcClient, never()).commitTransaction(any());
    verify(grpcClient, never()).startCapture(any());
    verify(tableLifecycleService, never())
        .resolveTableId(any(), Mockito.<List<String>>any(), any());
    verify(tableCommitPlanner, never()).plan(any(), any(), any(), any());
  }

  @Test
  void alreadyAppliedWithoutReplanningStillReturnsNoContent() {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", requestWithAddSnapshot(123L), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(tableLifecycleService, never())
        .resolveTableId(any(), Mockito.<List<String>>any(), any());
    verify(tableCommitPlanner, never()).plan(any(), any(), any(), any());
  }

  @Test
  void commitReturnsStateUnknownWhenCommitLandsInRetryableState() {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-1")
                        .setState(TransactionState.TS_APPLY_FAILED_RETRYABLE))
                .build());

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.SERVICE_UNAVAILABLE.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = assertInstanceOf(IcebergErrorResponse.class, response.getEntity());
    assertEquals("CommitStateUnknownException", error.error().type());
  }

  @Test
  void commitReturnsConflictWhenCommitLandsInConflictState() {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-1")
                        .setState(TransactionState.TS_APPLY_FAILED_CONFLICT))
                .build());

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.CONFLICT.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = assertInstanceOf(IcebergErrorResponse.class, response.getEntity());
    assertEquals("CommitFailedException", error.error().type());
  }

  @Test
  void commitReturnsNoContentWhenRetryableStateLaterBecomesApplied() {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build())
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-1")
                        .setState(TransactionState.TS_APPLY_FAILED_RETRYABLE))
                .build());

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
  }

  @Test
  void commitMapsDeterministicGrpcFailureToConflict() {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenThrow(new StatusRuntimeException(Status.ABORTED.withDescription("conflict")));

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.CONFLICT.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = assertInstanceOf(IcebergErrorResponse.class, response.getEntity());
    assertEquals("CommitFailedException", error.error().type());
  }

  @Test
  void commitMapsRetryableAbortedFailureToStateUnknownWithoutRollback() {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.commitTransaction(any())).thenThrow(retryableAbortedException());

    Response response = service.commit("pref", "idem", requestWithAddSnapshot(123L), tableSupport);

    assertEquals(Response.Status.SERVICE_UNAVAILABLE.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = assertInstanceOf(IcebergErrorResponse.class, response.getEntity());
    assertEquals("CommitStateUnknownException", error.error().type());
  }

  @Test
  void commitMapsRetryableAbortedFailureToNoContentWhenAppliedAfterPoll() {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build())
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.commitTransaction(any())).thenThrow(retryableAbortedException());

    Response response = service.commit("pref", "idem", requestWithAddSnapshot(123L), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
  }

  @Test
  void commitMapsUnknownGrpcFailureToStateUnknown() {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenThrow(new StatusRuntimeException(Status.UNAVAILABLE.withDescription("transient")));

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.SERVICE_UNAVAILABLE.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = assertInstanceOf(IcebergErrorResponse.class, response.getEntity());
    assertEquals("CommitStateUnknownException", error.error().type());
  }

  @Test
  void commitMapsUnknownStatusToBadGatewayStateUnknown() {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenThrow(new StatusRuntimeException(Status.UNKNOWN.withDescription("bad upstream")));

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.BAD_GATEWAY.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = assertInstanceOf(IcebergErrorResponse.class, response.getEntity());
    assertEquals("CommitStateUnknownException", error.error().type());
  }

  @Test
  void commitMapsRuntimeFailureToInternalServerErrorStateUnknown() {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.commitTransaction(any())).thenThrow(new RuntimeException("boom"));

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = assertInstanceOf(IcebergErrorResponse.class, response.getEntity());
    assertEquals("CommitStateUnknownException", error.error().type());
  }

  @Test
  void commitMapsDeadlineExceededToGatewayTimeoutStateUnknown() {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenThrow(new StatusRuntimeException(Status.DEADLINE_EXCEEDED.withDescription("timeout")));

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.GATEWAY_TIMEOUT.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = assertInstanceOf(IcebergErrorResponse.class, response.getEntity());
    assertEquals("CommitStateUnknownException", error.error().type());
  }

  @Test
  void commitMapsInvalidArgumentGrpcFailureToValidation() {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenThrow(
            new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("bad input")));

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = assertInstanceOf(IcebergErrorResponse.class, response.getEntity());
    assertEquals("ValidationException", error.error().type());
  }

  @Test
  void commitMapsBeginUnavailableToStateUnknown() {
    when(grpcClient.beginTransaction(any()))
        .thenThrow(
            new StatusRuntimeException(
                Status.UNAVAILABLE.withDescription("downstream unavailable")));

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.SERVICE_UNAVAILABLE.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = assertInstanceOf(IcebergErrorResponse.class, response.getEntity());
    assertEquals("CommitStateUnknownException", error.error().type());
    verify(grpcClient, never()).prepareTransaction(any());
    verify(grpcClient, never()).commitTransaction(any());
  }

  @Test
  void commitMapsPrepareUnavailableToStateUnknown() {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenThrow(
            new StatusRuntimeException(
                Status.UNAVAILABLE.withDescription("downstream unavailable")));

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.SERVICE_UNAVAILABLE.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = assertInstanceOf(IcebergErrorResponse.class, response.getEntity());
    assertEquals("CommitStateUnknownException", error.error().type());
    verify(grpcClient, never()).commitTransaction(any());
    verify(grpcClient).abortTransaction(any());
  }

  @Test
  void commitUnknownFailureDoesNotRollbackSnapshots() {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenThrow(new StatusRuntimeException(Status.UNAVAILABLE.withDescription("transient")));

    Response response = service.commit("pref", "idem", requestWithAddSnapshot(123L), tableSupport);

    assertEquals(Response.Status.SERVICE_UNAVAILABLE.getStatusCode(), response.getStatus());
  }

  @Test
  void retryableStateSkipsPrepareAndCommitWithoutIdempotency() {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-1")
                        .setState(TransactionState.TS_APPLY_FAILED_RETRYABLE))
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(grpcClient, never()).prepareTransaction(any());
    verify(grpcClient)
        .commitTransaction(argThat(commit -> commit != null && !commit.hasIdempotency()));
    verify(tableLifecycleService, never())
        .resolveTableId(any(), Mockito.<List<String>>any(), any());
    verify(tableCommitPlanner, never()).plan(any(), any(), any(), any());
  }

  @Test
  void preparedStateCommitsWithoutReplanningWhenRequestHashMatches() throws Exception {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-1")
                        .setState(TransactionState.TS_PREPARED)
                        .putProperties("iceberg.commit.request-hash", requestHash(request()))
                        .build())
                .build());
    when(tableLifecycleService.resolveTableId(any(), Mockito.<List<String>>any(), any()))
        .thenThrow(Status.NOT_FOUND.asRuntimeException());
    when(grpcClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());
    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(grpcClient, never()).prepareTransaction(any());
    verify(grpcClient).commitTransaction(any());
    verify(grpcClient, never()).abortTransaction(any());
    verify(tableLifecycleService, never())
        .resolveTableId(any(), Mockito.<List<String>>any(), any());
    verify(tableCommitPlanner, never()).plan(any(), any(), any(), any());
  }

  @Test
  void alreadyAppliedDoesNotReplayPostCommitUpdates() {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", requestWithPostCommitUpdate(), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(grpcClient, never()).prepareTransaction(any());
    verify(grpcClient, never()).commitTransaction(any());
    verify(grpcClient, never()).startCapture(any());
    verify(tableLifecycleService, never())
        .resolveTableId(any(), Mockito.<List<String>>any(), any());
  }

  @Test
  void alreadyAppliedDoesNotReplayPreCommitSnapshotUpdatesWhenPresent() {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", requestWithAddSnapshot(123L), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(grpcClient, never()).prepareTransaction(any());
    verify(grpcClient, never()).commitTransaction(any());
    verify(grpcClient, never()).startCapture(any());
    verify(tableLifecycleService, never())
        .resolveTableId(any(), Mockito.<List<String>>any(), any());
    verify(tableCommitPlanner, never()).plan(any(), any(), any(), any());
  }

  @Test
  void assertCreateConflictsWhenTableAlreadyExists() {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());

    Response response = service.commit("pref", "idem", requestWithAssertCreate(), tableSupport);

    assertEquals(Response.Status.CONFLICT.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = assertInstanceOf(IcebergErrorResponse.class, response.getEntity());
    assertEquals("CommitFailedException", error.error().type());
    verify(grpcClient, never()).prepareTransaction(any());
    verify(grpcClient, never()).commitTransaction(any());
  }

  @Test
  void assertCreateMissingTablePlansAtomicCreate() {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-create"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-create")
                        .setState(TransactionState.TS_OPEN))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-create")
                        .setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-create")
                        .setState(TransactionState.TS_APPLIED))
                .build());
    when(tableLifecycleService.resolveTableId(any(), Mockito.<List<String>>any(), any()))
        .thenThrow(Status.NOT_FOUND.asRuntimeException());
    when(tableCommitPlanner.plan(any(), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              java.util.function.Supplier<Table> supplier = invocation.getArgument(1);
              return new TableCommitPlanner.PlanResult(supplier.get(), null);
            });

    Response response = service.commit("pref", "idem", requestWithAssertCreate(), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(grpcClient)
        .prepareTransaction(
            argThat(
                prepare ->
                    prepare.getChangesList().stream()
                            .anyMatch(
                                change ->
                                    change.hasTableId()
                                        && change.hasTable()
                                        && change.getTable().hasResourceId()
                                        && change
                                            .getTableId()
                                            .getId()
                                            .equals(change.getTable().getResourceId().getId())
                                        && change.getPrecondition().getExpectedVersion() == 0L)
                        && prepare.getChangesList().stream()
                            .noneMatch(
                                change ->
                                    change.hasTargetPointerKey()
                                        && change.getTargetPointerKey().contains("/tx-journal/"))));
    verify(grpcClient).commitTransaction(any());
  }

  @Test
  void conflictStateSkipsSnapshotAndCommitWork() {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-1")
                        .setState(TransactionState.TS_APPLY_FAILED_CONFLICT))
                .build());

    Response response = service.commit("pref", "idem", requestWithAddSnapshot(123L), tableSupport);

    assertEquals(Response.Status.CONFLICT.getStatusCode(), response.getStatus());
    verify(grpcClient, never()).prepareTransaction(any());
    verify(grpcClient, never()).commitTransaction(any());
  }

  @Test
  void assertRefSnapshotIdNullConflictsWhenRefExists() {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());
    when(tableSupport.loadCurrentMetadata(any(Table.class)))
        .thenReturn(
            IcebergMetadata.newBuilder()
                .setCurrentSnapshotId(123L)
                .putRefs(
                    "main", IcebergRef.newBuilder().setSnapshotId(123L).setType("branch").build())
                .build());

    Response response =
        service.commit("pref", "idem", requestWithAssertRefSnapshotIdNull("main"), tableSupport);

    assertEquals(Response.Status.CONFLICT.getStatusCode(), response.getStatus());
    verify(grpcClient, never()).prepareTransaction(any());
    verify(grpcClient, never()).commitTransaction(any());
  }

  @Test
  void assertRefSnapshotIdNullConflictsWhenRefSnapshotIdIsZero() {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(tableSupport.loadCurrentMetadata(any(Table.class)))
        .thenReturn(
            IcebergMetadata.newBuilder()
                .putRefs(
                    "main", IcebergRef.newBuilder().setSnapshotId(0L).setType("branch").build())
                .build());

    Response response =
        service.commit("pref", "idem", requestWithAssertRefSnapshotIdNull("main"), tableSupport);

    assertEquals(Response.Status.CONFLICT.getStatusCode(), response.getStatus());
    verify(grpcClient, never()).prepareTransaction(any());
    verify(grpcClient, never()).commitTransaction(any());
  }

  @Test
  void assertRefSnapshotIdNullDoesNotConflictWhenMetadataHasDefaultCurrentSnapshotZero() {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());
    when(tableSupport.loadCurrentMetadata(any(Table.class)))
        .thenReturn(IcebergMetadata.getDefaultInstance());

    Response response =
        service.commit("pref", "idem", requestWithAssertRefSnapshotIdNull("main"), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(grpcClient).prepareTransaction(any());
    verify(grpcClient).commitTransaction(any());
  }

  @Test
  void assertRefSnapshotIdNullDoesNotConflictWhenRefPropertySnapshotIdIsMissing() {
    Table tableWithRefProperty =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-id").build())
            .putProperties(RefPropertyUtil.PROPERTY_KEY, "{\"main\":{\"type\":\"branch\"}}")
            .build();
    when(tableLifecycleService.getTableResponse(any()))
        .thenReturn(
            GetTableResponse.newBuilder()
                .setTable(tableWithRefProperty)
                .setMeta(MutationMeta.newBuilder().setPointerVersion(7L))
                .build());
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());
    when(tableSupport.loadCurrentMetadata(any(Table.class)))
        .thenThrow(new RuntimeException("no metadata"));

    Response response =
        service.commit("pref", "idem", requestWithAssertRefSnapshotIdNull("main"), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(grpcClient).prepareTransaction(any());
    verify(grpcClient).commitTransaction(any());
  }

  @Test
  void beginCarriesRequestHashProperty() {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", requestWithAddSnapshot(123L), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(grpcClient)
        .beginTransaction(
            argThat(
                req ->
                    req != null
                        && req.getPropertiesMap().containsKey("iceberg.commit.request-hash")
                        && !req.getPropertiesMap().get("iceberg.commit.request-hash").isBlank()
                        && isValidBase64(
                            req.getPropertiesMap().get("iceberg.commit.request-hash"))));
  }

  @Test
  void beginGeneratesRequestHashIdempotencyFallbackForOrdinaryCommits() {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", null, requestWithAddSnapshot(123L), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(grpcClient)
        .beginTransaction(
            argThat(
                req ->
                    req != null
                        && req.hasIdempotency()
                        && req.getIdempotency().getKey().startsWith("req:cat:")));
  }

  @Test
  void commitCreateRetainsRequestHashIdempotencyFallback() {
    TableRequests.Create createRequest =
        new TableRequests.Create("orders", null, null, Map.of(), null, null, false);
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());
    when(tableCreateTransactionMapper.buildCreateRequest(
            eq(List.of("db")),
            eq("orders"),
            any(ResourceId.class),
            any(ResourceId.class),
            eq(createRequest),
            eq(tableSupport)))
        .thenReturn(requestWithAssertCreate());

    Response response =
        service.commitCreate(
            "pref",
            null,
            List.of("db"),
            "orders",
            ResourceId.newBuilder().setAccountId("acct-1").setId("cat-id").build(),
            ResourceId.newBuilder().setAccountId("acct-1").setId("ns-id").build(),
            createRequest,
            tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(grpcClient)
        .beginTransaction(
            argThat(
                req ->
                    req != null
                        && req.hasIdempotency()
                        && req.getIdempotency().getKey().startsWith("req:cat:")));
  }

  @Test
  void retryWithoutExplicitIdempotencyReusesRequestHashIdempotencyAfterConflict() {
    when(grpcClient.beginTransaction(any()))
        .thenAnswer(
            invocation -> {
              var req =
                  invocation.getArgument(
                      0, ai.floedb.floecat.transaction.rpc.BeginTransactionRequest.class);
              String key = req.hasIdempotency() ? req.getIdempotency().getKey() : "";
              String txId = key == null || key.isBlank() ? "tx-1" : "sticky-tx";
              return BeginTransactionResponse.newBuilder()
                  .setTransaction(Transaction.newBuilder().setTxId(txId))
                  .build();
            });
    when(grpcClient.getTransaction(any()))
        .thenAnswer(
            invocation -> {
              String txId =
                  invocation
                      .getArgument(0, ai.floedb.floecat.transaction.rpc.GetTransactionRequest.class)
                      .getTxId();
              TransactionState state =
                  "sticky-tx".equals(txId)
                      ? TransactionState.TS_APPLY_FAILED_CONFLICT
                      : TransactionState.TS_OPEN;
              return GetTransactionResponse.newBuilder()
                  .setTransaction(Transaction.newBuilder().setTxId(txId).setState(state))
                  .build();
            });
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("sticky-tx")
                        .setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("sticky-tx")
                        .setState(TransactionState.TS_APPLIED))
                .build());

    Response first = service.commit("pref", null, request(), tableSupport);
    Response second = service.commit("pref", null, request(), tableSupport);

    assertEquals(Response.Status.CONFLICT.getStatusCode(), first.getStatus());
    assertEquals(Response.Status.CONFLICT.getStatusCode(), second.getStatus());
    verify(grpcClient, Mockito.times(2))
        .beginTransaction(
            argThat(
                req ->
                    req != null
                        && req.hasIdempotency()
                        && req.getIdempotency().getKey().startsWith("req:cat:")));
  }

  @Test
  void explicitIdempotencyReusesTransactionAndRejectsMismatchedPayloadOnRetry() throws Exception {
    TransactionCommitRequest initialRequest = requestWithAddSnapshot(123L);
    TransactionCommitRequest mismatchedRequest = requestWithAddSnapshot(456L);
    String initialHash = requestHash(initialRequest);

    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("sticky-tx"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("sticky-tx")
                        .setState(TransactionState.TS_OPEN)
                        .putProperties("iceberg.commit.request-hash", initialHash))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("sticky-tx")
                        .setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("sticky-tx")
                        .setState(TransactionState.TS_APPLIED))
                .build());

    Response first = service.commit("pref", "idem", initialRequest, tableSupport);
    Response second = service.commit("pref", "idem", mismatchedRequest, tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), first.getStatus());
    assertEquals(Response.Status.CONFLICT.getStatusCode(), second.getStatus());
    verify(grpcClient, Mockito.times(2))
        .beginTransaction(
            argThat(
                req ->
                    req != null
                        && req.hasIdempotency()
                        && "idem".equals(req.getIdempotency().getKey())));
  }

  @Test
  void requestHashCanonicalizationUsesOriginalMapKeyValues() throws Exception {
    Method canonicalize =
        TransactionCommitService.class.getDeclaredMethod("canonicalize", Object.class);
    canonicalize.setAccessible(true);

    Map<Object, Object> withNumericKey = new LinkedHashMap<>();
    withNumericKey.put(7, "value");
    String numericRendered = (String) canonicalize.invoke(service, withNumericKey);

    assertEquals("{\"7\":\"value\"}", numericRendered);
  }

  @Test
  void sharedKeyEncodingUsesPercentEncodingForSpaces() {
    assertEquals("acct%201", Keys.encodeSegment("acct 1"));
  }

  @Test
  void preCommitSnapshotUsesTxIdFallbackForIdempotency() {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", null, requestWithAddSnapshot(123L), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
  }

  @Test
  void addSnapshotAddsAtomicSnapshotPointerChanges() {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", requestWithAddSnapshot(123L), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(grpcClient)
        .prepareTransaction(
            argThat(
                prepare ->
                    prepare != null
                        && prepare.getChangesList().stream()
                            .anyMatch(
                                change ->
                                    change.hasTargetPointerKey()
                                        && change
                                            .getTargetPointerKey()
                                            .contains("/snapshots/by-id/")
                                        && change.getPayload().size() > 0)
                        && prepare.getChangesList().stream()
                            .anyMatch(
                                change ->
                                    change.hasTargetPointerKey()
                                        && change
                                            .getTargetPointerKey()
                                            .contains("/snapshots/by-time/")
                                        && change.getPayload().size() > 0)));
  }

  @Test
  void addSnapshotPreservesSchemaIdZeroInAtomicPayload() {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response =
        service.commit("pref", "idem", requestWithAddSnapshotAndSchemaId(123L, 0), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(grpcClient)
        .prepareTransaction(
            argThat(
                prepare ->
                    prepare != null
                        && prepare.getChangesList().stream()
                            .filter(
                                change ->
                                    change.hasTargetPointerKey()
                                        && change
                                            .getTargetPointerKey()
                                            .contains("/snapshots/by-id/")
                                        && change.getPayload().size() > 0)
                            .map(change -> parseSnapshot(change.getPayload()))
                            .anyMatch(
                                snapshot -> snapshot != null && snapshot.getSchemaId() == 0)));
  }

  @Test
  void addSnapshotAcceptsSnapshotIdZeroForAtomicPointersAndPostCommitSync() {
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-id").build();
    ResourceId connectorId =
        ResourceId.newBuilder()
            .setAccountId("acct-1")
            .setId("conn-1")
            .setKind(ResourceKind.RK_CONNECTOR)
            .build();
    Table tableWithConnector =
        Table.newBuilder()
            .setResourceId(tableId)
            .setUpstream(UpstreamRef.newBuilder().setConnectorId(connectorId).build())
            .putProperties("metadata-location", "s3://meta/new")
            .build();
    when(tableLifecycleService.getTableResponse(any()))
        .thenReturn(
            GetTableResponse.newBuilder()
                .setTable(tableWithConnector)
                .setMeta(MutationMeta.newBuilder().setPointerVersion(7L))
                .build());
    when(tableCommitPlanner.plan(any(), any(), any(), any()))
        .thenReturn(new TableCommitPlanner.PlanResult(tableWithConnector, null));
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", requestWithAddSnapshot(0L), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(grpcClient)
        .prepareTransaction(
            argThat(
                prepare ->
                    prepare != null
                        && prepare.getChangesList().stream()
                            .anyMatch(
                                change ->
                                    change.hasTargetPointerKey()
                                        && change
                                            .getTargetPointerKey()
                                            .endsWith("/snapshots/by-id/0000000000000000000"))));
  }

  @Test
  void removeSnapshotsAcceptsSnapshotIdZeroForPostCommitPrune() {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.getSnapshot(any()))
        .thenReturn(
            GetSnapshotResponse.newBuilder()
                .setSnapshot(
                    Snapshot.newBuilder()
                        .setSnapshotId(0L)
                        .setUpstreamCreatedAt(Timestamps.fromMillis(123L))
                        .build())
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response =
        service.commit("pref", "idem", requestWithRemoveSnapshots(0L), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(grpcClient)
        .prepareTransaction(
            argThat(
                prepare ->
                    prepare != null
                        && prepare.getChangesList().stream()
                                .filter(
                                    change ->
                                        change.hasTargetPointerKey()
                                            && change.hasIntendedBlobUri()
                                            && change.getIntendedBlobUri().contains("/delete/"))
                                .count()
                            == 2));
  }

  @Test
  void alreadyAppliedSkipsPostCommitSnapshotWorkAndReturnsNoContent() {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", requestWithPostCommitUpdate(), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(grpcClient, never()).prepareTransaction(any());
    verify(grpcClient, never()).commitTransaction(any());
    verify(grpcClient, never()).startCapture(any());
    verify(tableLifecycleService, never())
        .resolveTableId(any(), Mockito.<List<String>>any(), any());
  }

  @Test
  void commitMapsPreCommitUnauthenticatedToUnauthorized() {
    when(grpcClient.beginTransaction(any()))
        .thenThrow(new StatusRuntimeException(Status.UNAUTHENTICATED.withDescription("no token")));

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.UNAUTHORIZED.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = assertInstanceOf(IcebergErrorResponse.class, response.getEntity());
    assertEquals("UnauthorizedException", error.error().type());
  }

  @Test
  void commitMapsCommitPermissionDeniedToForbidden() {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenThrow(
            new StatusRuntimeException(Status.PERMISSION_DENIED.withDescription("forbidden")));

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.FORBIDDEN.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = assertInstanceOf(IcebergErrorResponse.class, response.getEntity());
    assertEquals("ForbiddenException", error.error().type());
  }

  @Test
  void alreadyAppliedReturnsNoContentWithoutRevalidatingUpdates() {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", requestWithUnknownUpdate(), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(grpcClient, never()).prepareTransaction(any());
    verify(grpcClient, never()).commitTransaction(any());
  }

  @Test
  void preCommitSnapshotFailureReturnsCommitStateUnknown() {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.abortTransaction(any())).thenThrow(new RuntimeException("abort failed"));

    Response response = service.commit("pref", "idem", requestWithAddSnapshot(123L), tableSupport);

    assertEquals(Response.Status.SERVICE_UNAVAILABLE.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = response.readEntity(IcebergErrorResponse.class);
    assertEquals("CommitStateUnknownException", error.error().type());
    verify(grpcClient, never()).abortTransaction(any());
    verify(grpcClient).commitTransaction(any());
  }

  @Test
  void preparedStateSkipsRequestShapeValidationWhenRequestHashMatches() throws Exception {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-1")
                        .setState(TransactionState.TS_PREPARED)
                        .putProperties("iceberg.commit.request-hash", requestHash(request()))
                        .build())
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", requestWithNullRequirements(), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(grpcClient, never()).prepareTransaction(any());
    verify(grpcClient).commitTransaction(any());
    verify(tableCommitPlanner, never()).plan(any(), any(), any(), any());
  }

  @Test
  void preparedStateSkipsUpdateValidationWhenRequestHashMatches() throws Exception {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-1")
                        .setState(TransactionState.TS_PREPARED)
                        .putProperties("iceberg.commit.request-hash", requestHash(request()))
                        .build())
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", requestWithNullUpdates(), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(grpcClient, never()).prepareTransaction(any());
    verify(grpcClient).commitTransaction(any());
    verify(tableCommitPlanner, never()).plan(any(), any(), any(), any());
  }

  @Test
  void preparedStateSkipsPlannerRequirementValidationWhenRequestHashMatches() throws Exception {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-1")
                        .setState(TransactionState.TS_PREPARED)
                        .putProperties("iceberg.commit.request-hash", requestHash(request()))
                        .build())
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    when(tableCommitPlanner.plan(any(), any(), any(), any()))
        .thenReturn(
            new TableCommitPlanner.PlanResult(
                null,
                Response.status(Response.Status.BAD_REQUEST)
                    .entity(
                        new IcebergErrorResponse(
                            new IcebergError(
                                "unsupported commit requirement", "ValidationException", 400)))
                    .build()));

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(grpcClient, never()).prepareTransaction(any());
    verify(grpcClient).commitTransaction(any());
    verify(tableCommitPlanner, never()).plan(any(), any(), any(), any());
  }

  @Test
  void preparedStateSkipsPlannerUpdateValidationWhenRequestHashMatches() throws Exception {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-1")
                        .setState(TransactionState.TS_PREPARED)
                        .putProperties("iceberg.commit.request-hash", requestHash(request()))
                        .build())
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    when(tableCommitPlanner.plan(any(), any(), any(), any()))
        .thenReturn(
            new TableCommitPlanner.PlanResult(
                null,
                Response.status(Response.Status.BAD_REQUEST)
                    .entity(
                        new IcebergErrorResponse(
                            new IcebergError(
                                "unsupported commit update action", "ValidationException", 400)))
                    .build()));

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(grpcClient, never()).prepareTransaction(any());
    verify(grpcClient).commitTransaction(any());
    verify(tableCommitPlanner, never()).plan(any(), any(), any(), any());
  }

  @Test
  void openStateWithMultiTableChangesPreparesAllChanges() {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", multiTableRequest(), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(grpcClient)
        .prepareTransaction(
            argThat(
                prepare ->
                    prepare != null
                        && prepare.getChangesList().stream()
                                .filter(change -> change.hasConnectorProvisioning())
                                .count()
                            == 2
                        && prepare.getChangesList().stream()
                                .filter(change -> change.hasTable())
                                .count()
                            == 2
                        && prepare.getChangesList().stream()
                            .noneMatch(
                                change ->
                                    change.hasTargetPointerKey()
                                        && change.getTargetPointerKey().contains("/tx-journal/"))));
  }

  @Test
  void openStateReturnsNotFoundWhenAnyPlannedTableIsMissing() {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(tableLifecycleService.resolveTableId(any(), Mockito.<List<String>>any(), any()))
        .thenReturn(ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-id").build())
        .thenThrow(Status.NOT_FOUND.asRuntimeException());

    Response response = service.commit("pref", "idem", multiTableRequest(), tableSupport);

    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    verify(grpcClient, never()).prepareTransaction(any());
    verify(grpcClient, never()).commitTransaction(any());
  }

  private TransactionCommitRequest request() {
    return new TransactionCommitRequest(
        List.of(
            new TransactionCommitRequest.TableChange(
                new TableIdentifierDto(List.of("db"), "orders"), List.of(), List.of())));
  }

  private TransactionCommitRequest requestWithAddSnapshot(long snapshotId) {
    return requestWithAddSnapshotAndSchemaId(snapshotId, 1);
  }

  private TransactionCommitRequest requestWithAddSnapshotAndParentId(
      long snapshotId, long parentId) {
    Map<String, Object> snapshot = new LinkedHashMap<>();
    snapshot.put("snapshot-id", snapshotId);
    snapshot.put("parent-snapshot-id", parentId);
    snapshot.put("schema-id", 1);
    snapshot.put("schema-json", "{\"type\":\"struct\",\"fields\":[]}");
    Map<String, Object> addUpdate = new LinkedHashMap<>();
    addUpdate.put("action", "add-snapshot");
    addUpdate.put("snapshot", snapshot);
    return new TransactionCommitRequest(
        List.of(
            new TransactionCommitRequest.TableChange(
                new TableIdentifierDto(List.of("db"), "orders"), List.of(), List.of(addUpdate))));
  }

  private TransactionCommitRequest requestWithAddSnapshotAndSchemaId(
      long snapshotId, int schemaId) {
    Map<String, Object> snapshot = new LinkedHashMap<>();
    snapshot.put("snapshot-id", snapshotId);
    snapshot.put("schema-id", schemaId);
    snapshot.put("schema-json", "{\"type\":\"struct\",\"fields\":[]}");
    Map<String, Object> addUpdate = new LinkedHashMap<>();
    addUpdate.put("action", "add-snapshot");
    addUpdate.put("snapshot", snapshot);
    return new TransactionCommitRequest(
        List.of(
            new TransactionCommitRequest.TableChange(
                new TableIdentifierDto(List.of("db"), "orders"), List.of(), List.of(addUpdate))));
  }

  @Test
  void addSnapshotPreservesExplicitZeroParentSnapshotId() throws Exception {
    when(grpcClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(grpcClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(grpcClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response =
        service.commit("pref", "idem", requestWithAddSnapshotAndParentId(123L, 0L), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());

    verify(grpcClient)
        .prepareTransaction(
            argThat(
                prepare -> {
                  var changes = prepare.getChangesList();
                  for (var change : changes) {
                    Snapshot snapshot = parseSnapshot(change.getPayload());
                    if (snapshot != null && snapshot.getSnapshotId() == 123L) {
                      return snapshot.hasParentSnapshotId() && snapshot.getParentSnapshotId() == 0L;
                    }
                  }
                  return false;
                }));
  }

  private TransactionCommitRequest requestWithSetMetadataLocationAndAddSnapshot(
      String metadataLocation, long snapshotId) {
    Map<String, Object> setMetadataLocation = new LinkedHashMap<>();
    setMetadataLocation.put("action", "set-properties");
    setMetadataLocation.put("updates", Map.of("metadata-location", metadataLocation));
    Map<String, Object> snapshot = new LinkedHashMap<>();
    snapshot.put("snapshot-id", snapshotId);
    snapshot.put("schema-id", 1);
    snapshot.put("schema-json", "{\"type\":\"struct\",\"fields\":[]}");
    Map<String, Object> addUpdate = new LinkedHashMap<>();
    addUpdate.put("action", "add-snapshot");
    addUpdate.put("snapshot", snapshot);
    return new TransactionCommitRequest(
        List.of(
            new TransactionCommitRequest.TableChange(
                new TableIdentifierDto(List.of("db"), "orders"),
                List.of(),
                List.of(setMetadataLocation, addUpdate))));
  }

  private Snapshot parseSnapshot(ByteString payload) {
    try {
      return Snapshot.parseFrom(payload);
    } catch (InvalidProtocolBufferException e) {
      return null;
    }
  }

  private String requestHash(TransactionCommitRequest request) throws Exception {
    Method requestHash =
        TransactionCommitService.class.getDeclaredMethod("requestHash", List.class);
    requestHash.setAccessible(true);
    return (String)
        requestHash.invoke(
            service,
            request == null || request.tableChanges() == null ? List.of() : request.tableChanges());
  }

  private TransactionCommitRequest requestWithPostCommitUpdate() {
    Map<String, Object> assignUuid = new LinkedHashMap<>();
    assignUuid.put("action", "assign-uuid");
    assignUuid.put("uuid", "550e8400-e29b-41d4-a716-446655440000");
    return new TransactionCommitRequest(
        List.of(
            new TransactionCommitRequest.TableChange(
                new TableIdentifierDto(List.of("db"), "orders"), List.of(), List.of(assignUuid))));
  }

  private TransactionCommitRequest requestWithRemoveSnapshots(long... snapshotIds) {
    Map<String, Object> remove = new LinkedHashMap<>();
    remove.put("action", "remove-snapshots");
    List<Long> ids = new ArrayList<>();
    for (long snapshotId : snapshotIds) {
      ids.add(snapshotId);
    }
    remove.put("snapshot-ids", ids);
    return new TransactionCommitRequest(
        List.of(
            new TransactionCommitRequest.TableChange(
                new TableIdentifierDto(List.of("db"), "orders"), List.of(), List.of(remove))));
  }

  private TransactionCommitRequest requestWithUnknownUpdate() {
    Map<String, Object> unknown = new LinkedHashMap<>();
    unknown.put("action", "unknown-update-action");
    return new TransactionCommitRequest(
        List.of(
            new TransactionCommitRequest.TableChange(
                new TableIdentifierDto(List.of("db"), "orders"), List.of(), List.of(unknown))));
  }

  private TransactionCommitRequest requestWithNullRequirements() {
    return new TransactionCommitRequest(
        List.of(
            new TransactionCommitRequest.TableChange(
                new TableIdentifierDto(List.of("db"), "orders"), null, List.of())));
  }

  private TransactionCommitRequest requestWithAssertCreate() {
    Map<String, Object> setMetadataLocation = new LinkedHashMap<>();
    setMetadataLocation.put("action", "set-properties");
    setMetadataLocation.put(
        "updates",
        Map.of("metadata-location", "s3://floecat/iceberg/orders/metadata/00001.metadata.json"));
    return new TransactionCommitRequest(
        List.of(
            new TransactionCommitRequest.TableChange(
                new TableIdentifierDto(List.of("db"), "orders"),
                List.of(Map.of("type", "assert-create")),
                List.of(setMetadataLocation))));
  }

  private TransactionCommitRequest requestWithAssertCreateWithoutMetadataLocation() {
    return new TransactionCommitRequest(
        List.of(
            new TransactionCommitRequest.TableChange(
                new TableIdentifierDto(List.of("db"), "orders"),
                List.of(Map.of("type", "assert-create")),
                List.of())));
  }

  private TransactionCommitRequest requestWithAssertRefSnapshotIdNull(String refName) {
    Map<String, Object> requirement = new LinkedHashMap<>();
    requirement.put("type", "assert-ref-snapshot-id");
    requirement.put("ref", refName);
    requirement.put("snapshot-id", null);
    return new TransactionCommitRequest(
        List.of(
            new TransactionCommitRequest.TableChange(
                new TableIdentifierDto(List.of("db"), "orders"), List.of(requirement), List.of())));
  }

  private TransactionCommitRequest requestWithNullUpdates() {
    return new TransactionCommitRequest(
        List.of(
            new TransactionCommitRequest.TableChange(
                new TableIdentifierDto(List.of("db"), "orders"), List.of(), null)));
  }

  private TransactionCommitRequest multiTableRequest() {
    return new TransactionCommitRequest(
        List.of(
            new TransactionCommitRequest.TableChange(
                new TableIdentifierDto(List.of("db"), "orders"), List.of(), List.of()),
            new TransactionCommitRequest.TableChange(
                new TableIdentifierDto(List.of("db"), "orders2"), List.of(), List.of())));
  }

  private TransactionCommitRequest requestWithDuplicateTableChanges() {
    return new TransactionCommitRequest(
        List.of(
            new TransactionCommitRequest.TableChange(
                new TableIdentifierDto(List.of("db"), "orders"), List.of(), List.of()),
            new TransactionCommitRequest.TableChange(
                new TableIdentifierDto(List.of("db"), "orders"), List.of(), List.of())));
  }

  private boolean isValidBase64(String value) {
    try {
      Base64.getUrlDecoder().decode(value);
      return true;
    } catch (IllegalArgumentException e) {
      try {
        Base64.getDecoder().decode(value);
        return true;
      } catch (IllegalArgumentException ignored) {
        return false;
      }
    }
  }

  private StatusRuntimeException retryableAbortedException() {
    Error floecatError = Error.newBuilder().setCode(ErrorCode.MC_ABORT_RETRYABLE).build();
    com.google.rpc.Status statusProto =
        com.google.rpc.Status.newBuilder()
            .setCode(Status.Code.ABORTED.value())
            .setMessage("idempotency record pending")
            .addDetails(Any.pack(floecatError))
            .build();
    return StatusProto.toStatusRuntimeException(statusProto);
  }
}
