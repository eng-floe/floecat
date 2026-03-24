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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TransactionCommitRequest;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.CatalogRef;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.ResourceResolver;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.compat.TableFormatSupport;
import ai.floedb.floecat.gateway.iceberg.rest.support.AccountContext;
import ai.floedb.floecat.gateway.iceberg.rest.support.GrpcServiceFacade;
import ai.floedb.floecat.gateway.iceberg.rest.table.StagedTableRepository;
import ai.floedb.floecat.transaction.rpc.BeginTransactionResponse;
import ai.floedb.floecat.transaction.rpc.GetTransactionResponse;
import ai.floedb.floecat.transaction.rpc.Transaction;
import ai.floedb.floecat.transaction.rpc.TransactionState;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class TransactionCommitServiceTest {
  private final TransactionCommitService service = new TransactionCommitService();
  private final AccountContext accountContext = Mockito.mock(AccountContext.class);
  private final IcebergGatewayConfig config = Mockito.mock(IcebergGatewayConfig.class);
  private final IcebergGatewayConfig.DeltaCompatConfig deltaCompatConfig =
      Mockito.mock(IcebergGatewayConfig.DeltaCompatConfig.class);
  private final ResourceResolver resourceResolver = Mockito.mock(ResourceResolver.class);
  private final GrpcServiceFacade grpcClient = Mockito.mock(GrpcServiceFacade.class);
  private final TransactionExecutor transactionExecutor = Mockito.mock(TransactionExecutor.class);
  private final CommitPlanBuilder commitPlanBuilder = Mockito.mock(CommitPlanBuilder.class);
  private final CommitChangeRequestValidator commitChangeRequestValidator =
      new CommitChangeRequestValidator();
  private final TransactionAborter transactionAborter = Mockito.mock(TransactionAborter.class);
  private final TransactionOutcomePolicy outcomePolicy = new TransactionOutcomePolicy();
  private final CommitRequestValidationHelper validationHelper =
      new CommitRequestValidationHelper();
  private final CreateCommitNormalizer createCommitNormalizer =
      Mockito.mock(CreateCommitNormalizer.class);
  private final TableCommitResponseService tableCommitResponseService =
      Mockito.mock(TableCommitResponseService.class);
  private final StagedTableRepository stagedTableRepository =
      Mockito.mock(StagedTableRepository.class);
  private final TableGatewaySupport tableSupport = Mockito.mock(TableGatewaySupport.class);

  @BeforeEach
  void setUp() {
    service.accountContext = accountContext;
    service.config = config;
    service.resourceResolver = resourceResolver;
    service.tableFormatSupport = new TableFormatSupport();
    service.grpcClient = grpcClient;
    service.transactionExecutor = transactionExecutor;
    service.commitPlanBuilder = commitPlanBuilder;
    service.commitChangeRequestValidator = commitChangeRequestValidator;
    service.transactionAborter = transactionAborter;
    service.outcomePolicy = outcomePolicy;
    service.validationHelper = validationHelper;
    service.createCommitNormalizer = createCommitNormalizer;
    service.tableCommitResponseService = tableCommitResponseService;
    service.stagedTableRepository = stagedTableRepository;
    commitChangeRequestValidator.validationHelper = validationHelper;

    when(accountContext.getAccountId()).thenReturn("acct-1");
    when(config.deltaCompat()).thenReturn(java.util.Optional.of(deltaCompatConfig));
    when(deltaCompatConfig.enabled()).thenReturn(false);
    when(deltaCompatConfig.readOnly()).thenReturn(false);
    when(resourceResolver.catalog(any()))
        .thenReturn(
            new CatalogRef(
                "pref",
                "cat",
                ResourceId.newBuilder().setAccountId("acct-1").setId("cat-id").build()));
    when(createCommitNormalizer.normalizeFirstWriteCommit(
            any(), any(), any(), any(), any(Boolean.class), any(), any(), any()))
        .thenAnswer(invocation -> invocation.getArgument(6, TableRequests.Commit.class));
  }

  @Test
  void commitRequiresAccountContext() {
    when(accountContext.getAccountId()).thenReturn(" ");

    Response response =
        service.commit("pref", "idem", request(singleChange("db", "orders")), tableSupport);

    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
  }

  @Test
  void commitRejectsEmptyChanges() {
    Response response =
        service.commit("pref", "idem", new TransactionCommitRequest(List.of()), tableSupport);

    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
  }

  @Test
  void commitRejectsMissingIdentifier() {
    Response response =
        service.commit(
            "pref",
            "idem",
            new TransactionCommitRequest(
                List.of(new TransactionCommitRequest.TableChange(null, List.of(), List.of()))),
            tableSupport);

    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
  }

  @Test
  void commitRejectsBlankNamespaceSegments() {
    Response response =
        service.commit(
            "pref", "idem", request(singleChange(List.of("db", " "), "orders")), tableSupport);

    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
  }

  @Test
  void commitRejectsDuplicateIdentifiersCaseInsensitively() {
    TransactionCommitRequest request =
        request(singleChange(List.of("db"), "Orders"), singleChange(List.of("DB"), "orders"));

    Response response = service.commit("pref", "idem", request, tableSupport);

    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    verify(resourceResolver, never()).catalog(any());
  }

  @Test
  void commitMapsBeginFailureThroughExecutor() {
    StatusRuntimeException beginFailure = Status.UNAVAILABLE.asRuntimeException();
    Response mapped = Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
    when(grpcClient.beginTransaction(any())).thenThrow(beginFailure);
    when(transactionExecutor.mapPrepareFailure(beginFailure)).thenReturn(mapped);

    Response response =
        service.commit("pref", "idem", request(singleChange("db", "orders")), tableSupport);

    assertSame(mapped, response);
  }

  @Test
  void commitMapsGetTransactionFailureAndAbortsQuietly() {
    when(grpcClient.beginTransaction(any())).thenReturn(beginResponse("tx-1"));
    StatusRuntimeException getFailure = Status.UNAVAILABLE.asRuntimeException();
    Response mapped = Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
    when(grpcClient.getTransaction(any())).thenThrow(getFailure);
    when(transactionExecutor.mapPrepareFailure(getFailure)).thenReturn(mapped);

    Response response =
        service.commit("pref", "idem", request(singleChange("db", "orders")), tableSupport);

    assertSame(mapped, response);
    verify(transactionAborter, never()).abortQuietly("tx-1", "failed to load transaction");
  }

  @Test
  void commitRejectsRequestHashMismatch() {
    when(grpcClient.beginTransaction(any())).thenReturn(beginResponse("tx-1"));
    when(grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-1")
                        .setState(TransactionState.TS_OPEN)
                        .putProperties("iceberg.commit.request-hash", "different"))
                .build());

    Response response =
        service.commit("pref", "idem", request(singleChange("db", "orders")), tableSupport);

    assertEquals(Response.Status.CONFLICT.getStatusCode(), response.getStatus());
    verify(transactionAborter)
        .abortIfOpen(TransactionState.TS_OPEN, "tx-1", "transaction request-hash mismatch");
    verify(commitPlanBuilder, never()).build(any());
  }

  @Test
  void commitReturnsPlannerWebApplicationResponse() {
    when(grpcClient.beginTransaction(any())).thenReturn(beginResponse("tx-1"));
    when(grpcClient.getTransaction(any())).thenReturn(openTransaction("tx-1"));
    Response plannerResponse = Response.status(Response.Status.CONFLICT).build();
    when(commitPlanBuilder.build(any())).thenThrow(new WebApplicationException(plannerResponse));

    Response response =
        service.commit("pref", "idem", request(singleChange("db", "orders")), tableSupport);

    assertSame(plannerResponse, response);
    verify(transactionAborter)
        .abortIfOpen(TransactionState.TS_OPEN, "tx-1", "transaction commit planning failed");
  }

  @Test
  void commitMapsPlannerStatusFailure() {
    when(grpcClient.beginTransaction(any())).thenReturn(beginResponse("tx-1"));
    when(grpcClient.getTransaction(any())).thenReturn(openTransaction("tx-1"));
    StatusRuntimeException plannerFailure = Status.INTERNAL.asRuntimeException();
    Response mapped = Response.status(Response.Status.BAD_GATEWAY).build();
    when(commitPlanBuilder.build(any())).thenThrow(plannerFailure);
    when(transactionExecutor.mapPrepareFailure(plannerFailure)).thenReturn(mapped);

    Response response =
        service.commit("pref", "idem", request(singleChange("db", "orders")), tableSupport);

    assertSame(mapped, response);
    verify(transactionAborter)
        .abortIfOpen(TransactionState.TS_OPEN, "tx-1", "transaction commit planning failed");
  }

  @Test
  void commitMapsPlannerRuntimeFailureToStateUnknown() {
    when(grpcClient.beginTransaction(any())).thenReturn(beginResponse("tx-1"));
    when(grpcClient.getTransaction(any())).thenReturn(openTransaction("tx-1"));
    Response unknown = Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
    when(commitPlanBuilder.build(any())).thenThrow(new IllegalStateException("boom"));
    when(transactionExecutor.stateUnknown()).thenReturn(unknown);

    Response response =
        service.commit("pref", "idem", request(singleChange("db", "orders")), tableSupport);

    assertSame(unknown, response);
    verify(transactionAborter)
        .abortIfOpen(TransactionState.TS_OPEN, "tx-1", "transaction commit planning failed");
  }

  @Test
  void commitDelegatesBuiltPlanToExecutor() {
    when(grpcClient.beginTransaction(any())).thenReturn(beginResponse("tx-1"));
    when(grpcClient.getTransaction(any())).thenReturn(openTransaction("tx-1"));
    CommitPlan plan = new CommitPlan(List.of(), List.of());
    CommitRequestContext plannedContext =
        new CommitRequestContext(
            "acct-1",
            "tx-1",
            "pref",
            "cat",
            ResourceId.newBuilder().setAccountId("acct-1").setId("cat-id").build(),
            "idem",
            "hash",
            123L,
            TransactionState.TS_OPEN,
            tableSupport,
            List.of(),
            true,
            List.of());
    Response executorResponse = Response.noContent().build();
    when(commitPlanBuilder.build(any()))
        .thenReturn(new CommitPlanBuilder.PlanBuildResult(plannedContext, plan));
    when(transactionExecutor.execute(any(), eq(plan))).thenReturn(executorResponse);

    Response response =
        service.commit("pref", "idem", request(singleChange("db", "orders")), tableSupport);

    assertSame(executorResponse, response);
    ArgumentCaptor<CommitRequestContext> contextCaptor =
        ArgumentCaptor.forClass(CommitRequestContext.class);
    verify(commitPlanBuilder).build(contextCaptor.capture());
    assertEquals("tx-1", contextCaptor.getValue().txId());
    assertEquals(TransactionState.TS_OPEN, contextCaptor.getValue().currentState());
    assertSame(tableSupport, contextCaptor.getValue().tableSupport());
    verify(transactionExecutor).execute(same(plannedContext), eq(plan));
  }

  @Test
  void commitTableUsesCommandScopedSupportForPreCommitLoad() {
    TableGatewaySupport commandSupport = Mockito.mock(TableGatewaySupport.class);
    ResourceId tableId = ResourceId.newBuilder().setId("tbl-1").build();
    ai.floedb.floecat.catalog.rpc.Table table =
        ai.floedb.floecat.catalog.rpc.Table.newBuilder().setResourceId(tableId).build();
    when(commandSupport.resolveTableId("cat", List.of("db"), "orders")).thenReturn(tableId);
    when(commandSupport.getTable(tableId)).thenReturn(table);
    when(tableCommitResponseService.buildCommitResponse(any(), any(), any()))
        .thenReturn(Response.ok().build());
    when(transactionExecutor.execute(any(), any())).thenReturn(Response.noContent().build());
    when(grpcClient.beginTransaction(any())).thenReturn(beginResponse("tx-1"));
    when(grpcClient.getTransaction(any())).thenReturn(openTransaction("tx-1"));
    when(commitPlanBuilder.build(any()))
        .thenReturn(
            new CommitPlanBuilder.PlanBuildResult(
                new CommitRequestContext(
                    "acct-1",
                    "tx-1",
                    "pref",
                    "cat",
                    ResourceId.newBuilder().setAccountId("acct-1").setId("cat-id").build(),
                    "idem",
                    "hash",
                    123L,
                    TransactionState.TS_OPEN,
                    commandSupport,
                    List.of(),
                    true,
                    List.of()),
                new CommitPlan(List.of(), List.of())));

    Response response =
        service.commitTable(
            new TransactionCommitService.CommitCommand(
                "pref",
                "db",
                List.of("db"),
                "orders",
                "cat",
                ResourceId.newBuilder().setId("cat-id").build(),
                ResourceId.newBuilder().setId("ns-id").build(),
                "idem",
                null,
                null,
                new TableRequests.Commit(
                    List.of(), List.of(Map.of("action", "set-properties", "updates", Map.of()))),
                commandSupport));

    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    verify(commandSupport).resolveTableId("cat", List.of("db"), "orders");
    verify(commandSupport).getTable(tableId);
  }

  @Test
  void commitRejectsUnsupportedUpdateActionBeforeBegin() {
    Response response =
        service.commit(
            "pref",
            "idem",
            request(
                new TransactionCommitRequest.TableChange(
                    new ai.floedb.floecat.gateway.iceberg.rest.api.dto.TableIdentifierDto(
                        List.of("db"), "orders"),
                    List.of(),
                    List.of(Map.of("action", "drop")))),
            tableSupport);

    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    verify(grpcClient, never()).beginTransaction(any());
  }

  @Test
  void buildCreateRequestWrapsCheckedExceptionsAsIllegalState() throws Exception {
    JsonProcessingException failure = new JsonProcessingException("boom") {};
    when(tableSupport.buildCreateSpec(any(), any(), any(), any())).thenThrow(failure);

    IllegalStateException error =
        assertThrows(
            IllegalStateException.class,
            () ->
                service.buildCreateRequest(
                    List.of("db"),
                    "orders",
                    ResourceId.newBuilder().setId("cat").build(),
                    ResourceId.newBuilder().setId("ns").build(),
                    new TableRequests.Create(null, null, null, Map.of(), null, null, false),
                    tableSupport));

    assertSame(failure, error.getCause());
  }

  private BeginTransactionResponse beginResponse(String txId) {
    return BeginTransactionResponse.newBuilder()
        .setTransaction(Transaction.newBuilder().setTxId(txId))
        .build();
  }

  private GetTransactionResponse openTransaction(String txId) {
    return GetTransactionResponse.newBuilder()
        .setTransaction(Transaction.newBuilder().setTxId(txId).setState(TransactionState.TS_OPEN))
        .build();
  }

  private TransactionCommitRequest request(TransactionCommitRequest.TableChange... changes) {
    return new TransactionCommitRequest(List.of(changes));
  }

  private TransactionCommitRequest.TableChange singleChange(String namespace, String table) {
    return singleChange(List.of(namespace), table);
  }

  private TransactionCommitRequest.TableChange singleChange(List<String> namespace, String table) {
    return new TransactionCommitRequest.TableChange(
        new ai.floedb.floecat.gateway.iceberg.rest.api.dto.TableIdentifierDto(namespace, table),
        List.of(),
        List.of(Map.of("action", "set-properties", "updates", Map.of("owner", "alice"))));
  }
}
