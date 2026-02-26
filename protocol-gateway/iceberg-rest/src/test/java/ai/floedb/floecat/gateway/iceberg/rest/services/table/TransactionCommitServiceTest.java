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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.GetTableResponse;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.Error;
import ai.floedb.floecat.common.rpc.ErrorCode;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.CommitTableResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TableIdentifierDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergError;
import ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergErrorResponse;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TransactionCommitRequest;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.CatalogRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.RequestContextFactory;
import ai.floedb.floecat.gateway.iceberg.rest.services.account.AccountContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.SnapshotClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.TransactionClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.MaterializeMetadataResult;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergRef;
import ai.floedb.floecat.transaction.rpc.BeginTransactionResponse;
import ai.floedb.floecat.transaction.rpc.CommitTransactionResponse;
import ai.floedb.floecat.transaction.rpc.GetTransactionResponse;
import ai.floedb.floecat.transaction.rpc.PrepareTransactionResponse;
import ai.floedb.floecat.transaction.rpc.Transaction;
import ai.floedb.floecat.transaction.rpc.TransactionState;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import jakarta.ws.rs.core.Response;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TransactionCommitServiceTest {
  private final TransactionCommitService service = new TransactionCommitService();
  private final AccountContext accountContext = Mockito.mock(AccountContext.class);
  private final RequestContextFactory requestContextFactory =
      Mockito.mock(RequestContextFactory.class);
  private final TableLifecycleService tableLifecycleService =
      Mockito.mock(TableLifecycleService.class);
  private final TableCommitPlanner tableCommitPlanner = Mockito.mock(TableCommitPlanner.class);
  private final TableCommitSideEffectService sideEffectService =
      Mockito.mock(TableCommitSideEffectService.class);
  private final TableCommitMaterializationService materializationService =
      Mockito.mock(TableCommitMaterializationService.class);
  private final CommitResponseBuilder responseBuilder = Mockito.mock(CommitResponseBuilder.class);
  private final SnapshotClient snapshotClient = Mockito.mock(SnapshotClient.class);
  private final TransactionClient transactionClient = Mockito.mock(TransactionClient.class);
  private final TableGatewaySupport tableSupport = Mockito.mock(TableGatewaySupport.class);

  @BeforeEach
  void setUp() {
    service.accountContext = accountContext;
    service.requestContextFactory = requestContextFactory;
    service.tableLifecycleService = tableLifecycleService;
    service.tableCommitPlanner = tableCommitPlanner;
    service.sideEffectService = sideEffectService;
    service.materializationService = materializationService;
    service.responseBuilder = responseBuilder;
    service.snapshotClient = snapshotClient;
    service.transactionClient = transactionClient;

    when(accountContext.getAccountId()).thenReturn("acct-1");
    when(requestContextFactory.catalog(any()))
        .thenReturn(
            new CatalogRequestContext(
                "pref",
                "cat",
                ResourceId.newBuilder().setAccountId("acct-1").setId("cat-id").build()));
    when(tableLifecycleService.resolveNamespaceId(any(), Mockito.<List<String>>any()))
        .thenReturn(ResourceId.newBuilder().setAccountId("acct-1").setId("ns-id").build());

    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-id").build();
    when(tableLifecycleService.resolveTableId(any(), Mockito.<List<String>>any(), any()))
        .thenReturn(tableId);

    Table table = Table.newBuilder().setResourceId(tableId).build();
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
    when(tableSupport.loadCurrentMetadata(any(Table.class))).thenReturn(null);
    when(tableSupport.connectorIntegrationEnabled()).thenReturn(true);
  }

  private CommitTableResponseDto defaultCommitResponse() {
    return new CommitTableResponseDto(
        "s3://meta/default/00001.metadata.json",
        new TableMetadataView(
            2,
            null,
            null,
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

  @Test
  void commitReturnsNoContentWhenAlreadyAppliedAndReplaysSideEffects() {
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(transactionClient, never()).prepareTransaction(any());
    verify(transactionClient, never()).commitTransaction(any());
  }

  @Test
  void commitReturnsConflictWhenExistingTransactionHashDiffers() {
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
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
    verify(transactionClient).abortTransaction(any());
    verify(transactionClient, never()).prepareTransaction(any());
    verify(transactionClient, never()).commitTransaction(any());
  }

  @Test
  void commitReturnsNoContentWhenCommitReturnsApplied() {
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(transactionClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(transactionClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(transactionClient)
        .prepareTransaction(
            argThat(
                prepare ->
                    prepare != null
                        && prepare.getChangesCount() == 1
                        && prepare.getChanges(0).hasPrecondition()
                        && prepare.getChanges(0).getPrecondition().getExpectedVersion() == 7L));
  }

  @Test
  void commitPreMaterializesAndPublishesMetadataLocationAtomically() {
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-id").build();
    Table table =
        Table.newBuilder()
            .setResourceId(tableId)
            .putProperties("metadata-location", "s3://meta/old/00001.metadata.json")
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
        .thenReturn(IcebergMetadata.getDefaultInstance());
    when(tableSupport.connectorIntegrationEnabled()).thenReturn(false);
    when(materializationService.materializeMetadata(any(), any(), any(), any(), any(), any()))
        .thenReturn(MaterializeMetadataResult.success(null, "s3://meta/new/00002.metadata.json"));
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(transactionClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(transactionClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(transactionClient)
        .prepareTransaction(
            argThat(
                prepare ->
                    prepare.getChangesList().stream()
                        .anyMatch(
                            change ->
                                change.hasTable()
                                    && "s3://meta/new/00002.metadata.json"
                                        .equals(
                                            change
                                                .getTable()
                                                .getPropertiesMap()
                                                .get("metadata-location")))));
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
    when(materializationService.materializeMetadata(any(), any(), any(), any(), any(), any()))
        .thenReturn(MaterializeMetadataResult.success(null, "s3://meta/new/00002.metadata.json"));
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(transactionClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(transactionClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", requestWithAddSnapshot(123L), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(transactionClient)
        .prepareTransaction(
            argThat(
                prepare ->
                    prepare.getChangesList().stream()
                        .anyMatch(
                            change ->
                                change.hasTable()
                                    && "s3://meta/new/00002.metadata.json"
                                        .equals(
                                            change
                                                .getTable()
                                                .getPropertiesMap()
                                                .get("metadata-location")))));
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
    when(materializationService.materializeMetadata(any(), any(), any(), any(), any(), any()))
        .thenReturn(MaterializeMetadataResult.success(null, "s3://meta/new/00002.metadata.json"));
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(transactionClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(transactionClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(materializationService).materializeMetadata(any(), any(), any(), any(), any(), any());
    verify(transactionClient)
        .prepareTransaction(
            argThat(
                prepare ->
                    prepare.getChangesList().stream()
                        .anyMatch(
                            change ->
                                change.hasTable()
                                    && "s3://meta/new/00002.metadata.json"
                                        .equals(
                                            change
                                                .getTable()
                                                .getPropertiesMap()
                                                .get("metadata-location")))));
  }

  @Test
  void commitWithConnectorRunsPostCommitStatsSyncWithoutConnectorTxChanges() {
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-id").build();
    ResourceId connectorId = ResourceId.newBuilder().setAccountId("acct-1").setId("conn-1").build();
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
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(transactionClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(transactionClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(transactionClient)
        .prepareTransaction(
            argThat(
                prepare ->
                    prepare != null
                        && prepare.getChangesCount() == 1
                        && prepare.getChangesList().stream()
                            .anyMatch(change -> change.hasTableId() && change.hasTable())
                        && prepare.getChangesList().stream()
                            .noneMatch(change -> change.hasTargetPointerKey())));
    verify(sideEffectService)
        .runPostCommitStatsSyncAttempt(tableSupport, List.of("db"), "orders", tableWithConnector);
  }

  @Test
  void commitCreateFlowSkipsAtomicConnectorCreateWhenTableHasNoConnector() {
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-id").build();
    Table tableWithoutConnector =
        Table.newBuilder()
            .setResourceId(tableId)
            .setCatalogId(ResourceId.newBuilder().setAccountId("acct-1").setId("cat-id").build())
            .putProperties("metadata-location", "s3://meta/new")
            .build();
    when(tableLifecycleService.getTableResponse(any()))
        .thenReturn(
            GetTableResponse.newBuilder()
                .setTable(tableWithoutConnector)
                .setMeta(MutationMeta.newBuilder().setPointerVersion(7L))
                .build());
    when(tableCommitPlanner.plan(any(), any(), any(), any()))
        .thenReturn(new TableCommitPlanner.PlanResult(tableWithoutConnector, null));
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(transactionClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(transactionClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(transactionClient)
        .prepareTransaction(
            argThat(
                prepare ->
                    prepare != null
                        && prepare.getChangesCount() == 1
                        && prepare.getChangesList().stream()
                            .noneMatch(change -> change.hasTargetPointerKey())
                        && prepare.getChangesList().stream()
                            .anyMatch(
                                change ->
                                    change.hasTable()
                                        && !change.getTable().hasUpstream()
                                        && "s3://meta/new"
                                            .equals(
                                                change
                                                    .getTable()
                                                    .getPropertiesMap()
                                                    .get("metadata-location")))));
  }

  @Test
  void commitReturnsStateUnknownWhenCommitLandsInRetryableState() {
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(transactionClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(transactionClient.commitTransaction(any()))
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
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(transactionClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(transactionClient.commitTransaction(any()))
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
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
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
    when(transactionClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(transactionClient.commitTransaction(any()))
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
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(transactionClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(transactionClient.commitTransaction(any()))
        .thenThrow(new StatusRuntimeException(Status.ABORTED.withDescription("conflict")));

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.CONFLICT.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = assertInstanceOf(IcebergErrorResponse.class, response.getEntity());
    assertEquals("CommitFailedException", error.error().type());
  }

  @Test
  void commitMapsRetryableAbortedFailureToStateUnknownWithoutRollback() {
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(transactionClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(transactionClient.commitTransaction(any())).thenThrow(retryableAbortedException());

    Response response = service.commit("pref", "idem", requestWithAddSnapshot(123L), tableSupport);

    assertEquals(Response.Status.SERVICE_UNAVAILABLE.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = assertInstanceOf(IcebergErrorResponse.class, response.getEntity());
    assertEquals("CommitStateUnknownException", error.error().type());
  }

  @Test
  void commitMapsRetryableAbortedFailureToNoContentWhenAppliedAfterPoll() {
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
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
    when(transactionClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(transactionClient.commitTransaction(any())).thenThrow(retryableAbortedException());

    Response response = service.commit("pref", "idem", requestWithAddSnapshot(123L), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
  }

  @Test
  void commitMapsUnknownGrpcFailureToStateUnknown() {
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(transactionClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(transactionClient.commitTransaction(any()))
        .thenThrow(new StatusRuntimeException(Status.UNAVAILABLE.withDescription("transient")));

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.SERVICE_UNAVAILABLE.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = assertInstanceOf(IcebergErrorResponse.class, response.getEntity());
    assertEquals("CommitStateUnknownException", error.error().type());
  }

  @Test
  void commitMapsUnknownStatusToBadGatewayStateUnknown() {
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(transactionClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(transactionClient.commitTransaction(any()))
        .thenThrow(new StatusRuntimeException(Status.UNKNOWN.withDescription("bad upstream")));

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.BAD_GATEWAY.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = assertInstanceOf(IcebergErrorResponse.class, response.getEntity());
    assertEquals("CommitStateUnknownException", error.error().type());
  }

  @Test
  void commitMapsRuntimeFailureToInternalServerErrorStateUnknown() {
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(transactionClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(transactionClient.commitTransaction(any())).thenThrow(new RuntimeException("boom"));

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = assertInstanceOf(IcebergErrorResponse.class, response.getEntity());
    assertEquals("CommitStateUnknownException", error.error().type());
  }

  @Test
  void commitMapsDeadlineExceededToGatewayTimeoutStateUnknown() {
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(transactionClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(transactionClient.commitTransaction(any()))
        .thenThrow(new StatusRuntimeException(Status.DEADLINE_EXCEEDED.withDescription("timeout")));

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.GATEWAY_TIMEOUT.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = assertInstanceOf(IcebergErrorResponse.class, response.getEntity());
    assertEquals("CommitStateUnknownException", error.error().type());
  }

  @Test
  void commitMapsInvalidArgumentGrpcFailureToValidation() {
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(transactionClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(transactionClient.commitTransaction(any()))
        .thenThrow(
            new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("bad input")));

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = assertInstanceOf(IcebergErrorResponse.class, response.getEntity());
    assertEquals("ValidationException", error.error().type());
  }

  @Test
  void commitMapsBeginUnavailableToStateUnknown() {
    when(transactionClient.beginTransaction(any()))
        .thenThrow(
            new StatusRuntimeException(
                Status.UNAVAILABLE.withDescription("downstream unavailable")));

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.SERVICE_UNAVAILABLE.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = assertInstanceOf(IcebergErrorResponse.class, response.getEntity());
    assertEquals("CommitStateUnknownException", error.error().type());
    verify(transactionClient, never()).prepareTransaction(any());
    verify(transactionClient, never()).commitTransaction(any());
  }

  @Test
  void commitMapsPrepareUnavailableToStateUnknown() {
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(transactionClient.prepareTransaction(any()))
        .thenThrow(
            new StatusRuntimeException(
                Status.UNAVAILABLE.withDescription("downstream unavailable")));

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.SERVICE_UNAVAILABLE.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = assertInstanceOf(IcebergErrorResponse.class, response.getEntity());
    assertEquals("CommitStateUnknownException", error.error().type());
    verify(transactionClient, never()).commitTransaction(any());
    verify(transactionClient).abortTransaction(any());
  }

  @Test
  void commitUnknownFailureDoesNotRollbackSnapshots() {
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(transactionClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(transactionClient.commitTransaction(any()))
        .thenThrow(new StatusRuntimeException(Status.UNAVAILABLE.withDescription("transient")));

    Response response = service.commit("pref", "idem", requestWithAddSnapshot(123L), tableSupport);

    assertEquals(Response.Status.SERVICE_UNAVAILABLE.getStatusCode(), response.getStatus());
  }

  @Test
  void retryableStateSkipsPrepareAndCommitWithoutIdempotency() {
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-1")
                        .setState(TransactionState.TS_APPLY_FAILED_RETRYABLE))
                .build());
    when(transactionClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(transactionClient, never()).prepareTransaction(any());
    verify(transactionClient)
        .commitTransaction(argThat(commit -> commit != null && !commit.hasIdempotency()));
  }

  @Test
  void preparedStateFailsWhenPlanningCannotResolveTable() {
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(tableLifecycleService.resolveTableId(any(), Mockito.<List<String>>any(), any()))
        .thenThrow(Status.NOT_FOUND.asRuntimeException());
    when(transactionClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());
    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    verify(transactionClient, never()).prepareTransaction(any());
    verify(transactionClient, never()).commitTransaction(any());
    verify(transactionClient, never()).abortTransaction(any());
  }

  @Test
  void alreadyAppliedReplaysPostCommitUpdates() {
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", requestWithPostCommitUpdate(), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(transactionClient, never()).prepareTransaction(any());
    verify(transactionClient, never()).commitTransaction(any());
  }

  @Test
  void alreadyAppliedReplaysPreCommitSnapshotUpdatesWhenPresent() {
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", requestWithAddSnapshot(123L), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(transactionClient, never()).prepareTransaction(any());
    verify(transactionClient, never()).commitTransaction(any());
    verify(tableCommitPlanner, never()).plan(any(), any(), any(), any());
  }

  @Test
  void assertCreateConflictsWhenTableAlreadyExists() {
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());

    Response response = service.commit("pref", "idem", requestWithAssertCreate(), tableSupport);

    assertEquals(Response.Status.CONFLICT.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = assertInstanceOf(IcebergErrorResponse.class, response.getEntity());
    assertEquals("CommitFailedException", error.error().type());
    verify(transactionClient, never()).prepareTransaction(any());
    verify(transactionClient, never()).commitTransaction(any());
  }

  @Test
  void assertCreateMissingTablePlansAtomicCreate() {
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-create"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-create")
                        .setState(TransactionState.TS_OPEN))
                .build());
    when(transactionClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-create")
                        .setState(TransactionState.TS_PREPARED))
                .build());
    when(transactionClient.commitTransaction(any()))
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
    verify(transactionClient)
        .prepareTransaction(
            argThat(
                prepare ->
                    prepare.getChangesCount() == 1
                        && prepare.getChanges(0).hasTableId()
                        && prepare.getChanges(0).hasTable()
                        && prepare.getChanges(0).getTable().hasResourceId()
                        && prepare
                            .getChanges(0)
                            .getTableId()
                            .getId()
                            .equals(prepare.getChanges(0).getTable().getResourceId().getId())
                        && prepare.getChanges(0).getPrecondition().getExpectedVersion() == 0L));
    verify(transactionClient).commitTransaction(any());
  }

  @Test
  void conflictStateSkipsSnapshotAndCommitWork() {
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-1")
                        .setState(TransactionState.TS_APPLY_FAILED_CONFLICT))
                .build());

    Response response = service.commit("pref", "idem", requestWithAddSnapshot(123L), tableSupport);

    assertEquals(Response.Status.CONFLICT.getStatusCode(), response.getStatus());
    verify(transactionClient, never()).prepareTransaction(any());
    verify(transactionClient, never()).commitTransaction(any());
  }

  @Test
  void assertRefSnapshotIdNullConflictsWhenRefExists() {
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(transactionClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(transactionClient.commitTransaction(any()))
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
    verify(transactionClient, never()).prepareTransaction(any());
    verify(transactionClient, never()).commitTransaction(any());
  }

  @Test
  void beginCarriesRequestHashProperty() {
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", requestWithAddSnapshot(123L), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(transactionClient)
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
  void beginUsesRequestHashAsIdempotencyFallback() {
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", null, requestWithAddSnapshot(123L), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(transactionClient)
        .beginTransaction(
            argThat(
                req ->
                    req != null
                        && req.hasIdempotency()
                        && req.getIdempotency().getKey().startsWith("req:cat:")));
  }

  @Test
  void preCommitSnapshotUsesTxIdFallbackForIdempotency() {
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(transactionClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(transactionClient.commitTransaction(any()))
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
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(transactionClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(transactionClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", requestWithAddSnapshot(123L), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(transactionClient)
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
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(transactionClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(transactionClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response =
        service.commit("pref", "idem", requestWithAddSnapshotAndSchemaId(123L, 0), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(transactionClient)
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
  void postCommitSnapshotFailureStillReturnsNoContentAfterApplied() {
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", requestWithPostCommitUpdate(), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(transactionClient, never()).prepareTransaction(any());
    verify(transactionClient, never()).commitTransaction(any());
  }

  @Test
  void commitMapsPreCommitUnauthenticatedToUnauthorized() {
    when(transactionClient.beginTransaction(any()))
        .thenThrow(new StatusRuntimeException(Status.UNAUTHENTICATED.withDescription("no token")));

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.UNAUTHORIZED.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = assertInstanceOf(IcebergErrorResponse.class, response.getEntity());
    assertEquals("UnauthorizedException", error.error().type());
  }

  @Test
  void commitMapsCommitPermissionDeniedToForbidden() {
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(transactionClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(transactionClient.commitTransaction(any()))
        .thenThrow(
            new StatusRuntimeException(Status.PERMISSION_DENIED.withDescription("forbidden")));

    Response response = service.commit("pref", "idem", request(), tableSupport);

    assertEquals(Response.Status.FORBIDDEN.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = assertInstanceOf(IcebergErrorResponse.class, response.getEntity());
    assertEquals("ForbiddenException", error.error().type());
  }

  @Test
  void alreadyAppliedStillRejectsUnknownUpdateAction() {
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", requestWithUnknownUpdate(), tableSupport);

    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = assertInstanceOf(IcebergErrorResponse.class, response.getEntity());
    assertEquals("ValidationException", error.error().type());
  }

  @Test
  void preCommitSnapshotFailureReturnsCommitStateUnknown() {
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(transactionClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(transactionClient.abortTransaction(any())).thenThrow(new RuntimeException("abort failed"));

    Response response = service.commit("pref", "idem", requestWithAddSnapshot(123L), tableSupport);

    assertEquals(Response.Status.SERVICE_UNAVAILABLE.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = response.readEntity(IcebergErrorResponse.class);
    assertEquals("CommitStateUnknownException", error.error().type());
    verify(transactionClient, never()).abortTransaction(any());
    verify(transactionClient).commitTransaction(any());
  }

  @Test
  void preparedStateRejectsMissingRequirements() {
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());

    Response response = service.commit("pref", "idem", requestWithNullRequirements(), tableSupport);

    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = assertInstanceOf(IcebergErrorResponse.class, response.getEntity());
    assertEquals("ValidationException", error.error().type());
    verify(transactionClient, never()).prepareTransaction(any());
    verify(transactionClient, never()).commitTransaction(any());
  }

  @Test
  void preparedStateRejectsMissingUpdates() {
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());

    Response response = service.commit("pref", "idem", requestWithNullUpdates(), tableSupport);

    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = assertInstanceOf(IcebergErrorResponse.class, response.getEntity());
    assertEquals("ValidationException", error.error().type());
    verify(transactionClient, never()).prepareTransaction(any());
    verify(transactionClient, never()).commitTransaction(any());
  }

  @Test
  void preparedStateReturnsBadRequestWhenPlannerRejectsRequirement() {
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
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

    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = assertInstanceOf(IcebergErrorResponse.class, response.getEntity());
    assertEquals("ValidationException", error.error().type());
    verify(transactionClient, never()).prepareTransaction(any());
    verify(transactionClient, never()).commitTransaction(any());
  }

  @Test
  void preparedStateReturnsBadRequestWhenPlannerRejectsUpdateAction() {
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
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

    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = assertInstanceOf(IcebergErrorResponse.class, response.getEntity());
    assertEquals("ValidationException", error.error().type());
    verify(transactionClient, never()).prepareTransaction(any());
    verify(transactionClient, never()).commitTransaction(any());
  }

  @Test
  void openStateWithMultiTableChangesPreparesAllChanges() {
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(transactionClient.prepareTransaction(any()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(transactionClient.commitTransaction(any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    Response response = service.commit("pref", "idem", multiTableRequest(), tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(transactionClient)
        .prepareTransaction(argThat(prepare -> prepare != null && prepare.getChangesCount() == 2));
  }

  @Test
  void openStateReturnsNotFoundWhenAnyPlannedTableIsMissing() {
    when(transactionClient.beginTransaction(any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(transactionClient.getTransaction(any()))
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
    verify(transactionClient, never()).prepareTransaction(any());
    verify(transactionClient, never()).commitTransaction(any());
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

  private Snapshot parseSnapshot(ByteString payload) {
    try {
      return Snapshot.parseFrom(payload);
    } catch (InvalidProtocolBufferException e) {
      return null;
    }
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

  private TransactionCommitRequest requestWithUnknownUpdate() {
    Map<String, Object> unknown = new LinkedHashMap<>();
    unknown.put("action", "unknown-update-action");
    return new TransactionCommitRequest(
        List.of(
            new TransactionCommitRequest.TableChange(
                new TableIdentifierDto(List.of("db"), "orders"), List.of(), List.of(unknown))));
  }

  private TransactionCommitRequest requestWithSetSnapshotRef(String refName, long snapshotId) {
    Map<String, Object> update = new LinkedHashMap<>();
    update.put("action", "set-snapshot-ref");
    update.put("ref-name", refName);
    update.put("snapshot-id", snapshotId);
    update.put("type", "branch");
    return new TransactionCommitRequest(
        List.of(
            new TransactionCommitRequest.TableChange(
                new TableIdentifierDto(List.of("db"), "orders"), List.of(), List.of(update))));
  }

  private TransactionCommitRequest requestWithNullRequirements() {
    return new TransactionCommitRequest(
        List.of(
            new TransactionCommitRequest.TableChange(
                new TableIdentifierDto(List.of("db"), "orders"), null, List.of())));
  }

  private TransactionCommitRequest requestWithAssertCreate() {
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

  private boolean isValidBase64(String value) {
    try {
      Base64.getDecoder().decode(value);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
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
