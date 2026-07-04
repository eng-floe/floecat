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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.GetTableResponse;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TableIdentifierDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TransactionCommitRequest;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.CatalogRef;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.ResourceResolver;
import ai.floedb.floecat.gateway.iceberg.rest.services.account.AccountContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.ConnectorProvisioningService;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.TableCreateTransactionMapper;
import ai.floedb.floecat.transaction.rpc.TransactionState;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TransactionCommitServiceTest {

  @Test
  void commitRetriesWithFreshGeneratedTransactionAfterRetryableApplyConflict() {
    TransactionCommitService service = new TransactionCommitService();
    service.accountContext = Mockito.mock(AccountContext.class);
    service.resourceResolver = Mockito.mock(ResourceResolver.class);
    service.transactionCommitExecutionSupport =
        Mockito.mock(TransactionCommitExecutionSupport.class);
    service.tablePlanningSupport = Mockito.mock(TransactionCommitTablePlanningSupport.class);
    service.snapshotSupport = Mockito.mock(TransactionCommitSnapshotSupport.class);
    service.tableLifecycleService = Mockito.mock(TableLifecycleService.class);
    service.tableCreateTransactionMapper = Mockito.mock(TableCreateTransactionMapper.class);
    service.connectorProvisioningService = Mockito.mock(ConnectorProvisioningService.class);
    configureFastRetries(service);

    TableGatewaySupport tableSupport = Mockito.mock(TableGatewaySupport.class);
    ResourceId catalogId = ResourceId.newBuilder().setAccountId("acct").setId("cat-1").build();
    ResourceId namespaceId = ResourceId.newBuilder().setAccountId("acct").setId("ns-1").build();
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct").setId("tbl-1").build();
    Table table = Table.newBuilder().setResourceId(tableId).build();
    TransactionCommitRequest request =
        new TransactionCommitRequest(
            List.of(
                new TransactionCommitRequest.TableChange(
                    new TableIdentifierDto(List.of("db"), "orders"),
                    List.of(),
                    List.of(
                        Map.of("action", "set-properties", "updates", Map.of("owner", "it"))))));

    when(service.accountContext.getAccountId()).thenReturn("acct");
    when(service.resourceResolver.catalog("examples"))
        .thenReturn(new CatalogRef("examples", "examples", catalogId));
    when(service.tableLifecycleService.resolveNamespaceId("examples", List.of("db")))
        .thenReturn(namespaceId);
    when(service.tableLifecycleService.resolveTableId("examples", List.of("db"), "orders"))
        .thenReturn(tableId);
    when(service.tableLifecycleService.getTableResponse(tableId))
        .thenReturn(GetTableResponse.newBuilder().setTable(table).build());
    when(service.tablePlanningSupport.normalizeTableIdentity(table, tableId)).thenReturn(table);
    when(service.tablePlanningSupport.planExistingTableChange(
            any(),
            any(),
            any(),
            eq(tableId),
            eq(catalogId),
            eq(namespaceId),
            eq("db"),
            eq("orders"),
            any(),
            any(),
            any(),
            eq(tableSupport),
            eq(true)))
        .thenReturn(
            new TransactionCommitTablePlanningSupport.PlannedExistingTableChange(
                table, 1L, "s3://bucket/metadata/00002.metadata.json", null));
    when(service.connectorProvisioningService.resolveOrCreateForCommit(
            eq("examples"),
            eq(tableSupport),
            eq(List.of("db")),
            eq("orders"),
            eq(tableId),
            eq(table)))
        .thenReturn(new ConnectorProvisioningService.ProvisionResult(table, List.of(), null));
    when(service.snapshotSupport.planAtomicSnapshotChanges(
            eq("acct"),
            any(),
            eq(tableId),
            eq(table),
            eq(tableSupport),
            eq("s3://bucket/metadata/00002.metadata.json"),
            any(),
            eq(List.of())))
        .thenReturn(new TransactionCommitSnapshotSupport.SnapshotChangePlan(List.of(), null));

    var firstOpen =
        new TransactionCommitExecutionSupport.OpenTransaction(
            "tx-1", TransactionState.TS_OPEN, null, "tx-1");
    var retryOpen =
        new TransactionCommitExecutionSupport.OpenTransaction(
            "tx-2", TransactionState.TS_OPEN, null, "tx-2");
    when(service.transactionCommitExecutionSupport.openTransaction(
            eq(null), eq(true), eq("examples"), any(), eq(null)))
        .thenReturn(new TransactionCommitExecutionSupport.OpenTransactionResult(firstOpen, null));
    when(service.transactionCommitExecutionSupport.openTransaction(
            eq(null), eq(true), eq("examples"), any(), eq("tx-attempt-1")))
        .thenReturn(new TransactionCommitExecutionSupport.OpenTransactionResult(retryOpen, null));

    Response retryableConflict =
        Response.status(Response.Status.CONFLICT)
            .header(TransactionCommitExecutionSupport.RETRYABLE_CONFLICT_PROPERTY, "true")
            .build();
    when(service.transactionCommitExecutionSupport.apply(eq(firstOpen), any()))
        .thenReturn(retryableConflict);
    when(service.transactionCommitExecutionSupport.apply(eq(retryOpen), any()))
        .thenReturn(Response.noContent().build());
    when(service.transactionCommitExecutionSupport.isRetryableConflict(retryableConflict))
        .thenReturn(true);

    Response response = service.commit("examples", null, request, tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(service.transactionCommitExecutionSupport)
        .openTransaction(eq(null), eq(true), eq("examples"), any(), eq(null));
    verify(service.transactionCommitExecutionSupport)
        .openTransaction(eq(null), eq(true), eq("examples"), any(), eq("tx-attempt-1"));
    verify(service.tablePlanningSupport, times(2))
        .planExistingTableChange(
            any(),
            any(),
            any(),
            eq(tableId),
            eq(catalogId),
            eq(namespaceId),
            eq("db"),
            eq("orders"),
            any(),
            any(),
            any(),
            eq(tableSupport),
            eq(true));
  }

  @Test
  void commitRetriesWithClientSuppliedIdempotencyKeyUsingFreshAttemptKey() {
    TransactionCommitService service = baseService();
    TableGatewaySupport tableSupport = Mockito.mock(TableGatewaySupport.class);
    ResourceId catalogId = ResourceId.newBuilder().setAccountId("acct").setId("cat-1").build();
    ResourceId namespaceId = ResourceId.newBuilder().setAccountId("acct").setId("ns-1").build();
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct").setId("tbl-1").build();
    Table table = Table.newBuilder().setResourceId(tableId).build();
    TransactionCommitRequest request = singlePropertyChangeRequest();

    arrangeCommonSingleTablePlanning(service, tableSupport, catalogId, namespaceId, tableId, table);

    var firstOpen =
        new TransactionCommitExecutionSupport.OpenTransaction(
            "tx-1", TransactionState.TS_OPEN, null, "client-key");
    var retryOpen =
        new TransactionCommitExecutionSupport.OpenTransaction(
            "tx-2", TransactionState.TS_OPEN, null, "client-key:tx-attempt-1");
    when(service.transactionCommitExecutionSupport.openTransaction(
            eq("client-key"), eq(true), eq("examples"), any(), eq(null)))
        .thenReturn(new TransactionCommitExecutionSupport.OpenTransactionResult(firstOpen, null));
    when(service.transactionCommitExecutionSupport.openTransaction(
            eq("client-key"), eq(true), eq("examples"), any(), eq("tx-attempt-1")))
        .thenReturn(new TransactionCommitExecutionSupport.OpenTransactionResult(retryOpen, null));

    Response retryableConflict =
        Response.status(Response.Status.CONFLICT)
            .header(TransactionCommitExecutionSupport.RETRYABLE_CONFLICT_PROPERTY, "true")
            .build();
    when(service.transactionCommitExecutionSupport.apply(eq(firstOpen), any()))
        .thenReturn(retryableConflict);
    when(service.transactionCommitExecutionSupport.apply(eq(retryOpen), any()))
        .thenReturn(Response.noContent().build());
    when(service.transactionCommitExecutionSupport.isRetryableConflict(retryableConflict))
        .thenReturn(true);

    Response response = service.commit("examples", "client-key", request, tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(service.transactionCommitExecutionSupport)
        .openTransaction(eq("client-key"), eq(true), eq("examples"), any(), eq("tx-attempt-1"));
    verify(service.tablePlanningSupport, times(2))
        .planExistingTableChange(
            any(),
            any(),
            any(),
            eq(tableId),
            eq(catalogId),
            eq(namespaceId),
            eq("db"),
            eq("orders"),
            any(),
            any(),
            any(),
            eq(tableSupport),
            eq(true));
  }

  @Test
  void commitRetriesPlanningConflictWithFreshTransactionAttempt() {
    TransactionCommitService service = baseService();
    TableGatewaySupport tableSupport = Mockito.mock(TableGatewaySupport.class);
    ResourceId catalogId = ResourceId.newBuilder().setAccountId("acct").setId("cat-1").build();
    ResourceId namespaceId = ResourceId.newBuilder().setAccountId("acct").setId("ns-1").build();
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct").setId("tbl-1").build();
    Table table = Table.newBuilder().setResourceId(tableId).build();
    TransactionCommitRequest request = singlePropertyChangeRequest();

    arrangeCommonSingleTablePlanning(service, tableSupport, catalogId, namespaceId, tableId, table);

    var firstOpen =
        new TransactionCommitExecutionSupport.OpenTransaction(
            "tx-1", TransactionState.TS_OPEN, null, "tx-1");
    var retryOpen =
        new TransactionCommitExecutionSupport.OpenTransaction(
            "tx-2", TransactionState.TS_OPEN, null, "tx-2");
    when(service.transactionCommitExecutionSupport.openTransaction(
            eq(null), eq(true), eq("examples"), any(), eq(null)))
        .thenReturn(new TransactionCommitExecutionSupport.OpenTransactionResult(firstOpen, null));
    when(service.transactionCommitExecutionSupport.openTransaction(
            eq(null), eq(true), eq("examples"), any(), eq("tx-attempt-1")))
        .thenReturn(new TransactionCommitExecutionSupport.OpenTransactionResult(retryOpen, null));

    Response retryablePlanningConflict =
        Response.status(Response.Status.CONFLICT)
            .header(TransactionCommitExecutionSupport.RETRYABLE_CONFLICT_PROPERTY, "true")
            .build();
    when(service.tablePlanningSupport.planExistingTableChange(
            any(),
            any(),
            any(),
            eq(tableId),
            eq(catalogId),
            eq(namespaceId),
            eq("db"),
            eq("orders"),
            any(),
            any(),
            any(),
            eq(tableSupport),
            eq(true)))
        .thenReturn(
            new TransactionCommitTablePlanningSupport.PlannedExistingTableChange(
                null, 0L, null, retryablePlanningConflict))
        .thenReturn(
            new TransactionCommitTablePlanningSupport.PlannedExistingTableChange(
                table, 1L, "s3://bucket/metadata/00002.metadata.json", null));
    when(service.transactionCommitExecutionSupport.isRetryableConflict(retryablePlanningConflict))
        .thenReturn(true);
    when(service.transactionCommitExecutionSupport.apply(eq(retryOpen), any()))
        .thenReturn(Response.noContent().build());

    Response response = service.commit("examples", null, request, tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(service.transactionCommitExecutionSupport, never()).apply(eq(firstOpen), any());
    verify(service.transactionCommitExecutionSupport)
        .openTransaction(eq(null), eq(true), eq("examples"), any(), eq("tx-attempt-1"));
  }

  @Test
  void commitDoesNotRetryAssertRefSnapshotIdConflictFromPlanning() {
    TransactionCommitService service = baseService();
    TableGatewaySupport tableSupport = Mockito.mock(TableGatewaySupport.class);
    ResourceId catalogId = ResourceId.newBuilder().setAccountId("acct").setId("cat-1").build();
    ResourceId namespaceId = ResourceId.newBuilder().setAccountId("acct").setId("ns-1").build();
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct").setId("tbl-1").build();
    Table table = Table.newBuilder().setResourceId(tableId).build();
    TransactionCommitRequest request = singlePropertyChangeRequest();

    arrangeCommonSingleTablePlanning(service, tableSupport, catalogId, namespaceId, tableId, table);

    var firstOpen =
        new TransactionCommitExecutionSupport.OpenTransaction(
            "tx-1", TransactionState.TS_OPEN, null, "tx-1");
    when(service.transactionCommitExecutionSupport.openTransaction(
            eq(null), eq(true), eq("examples"), any(), eq(null)))
        .thenReturn(new TransactionCommitExecutionSupport.OpenTransactionResult(firstOpen, null));

    Response userConflict =
        Response.status(Response.Status.CONFLICT)
            .entity("assert-ref-snapshot-id failed for ref main")
            .build();
    when(service.tablePlanningSupport.planExistingTableChange(
            any(),
            any(),
            any(),
            eq(tableId),
            eq(catalogId),
            eq(namespaceId),
            eq("db"),
            eq("orders"),
            any(),
            any(),
            any(),
            eq(tableSupport),
            eq(true)))
        .thenReturn(
            new TransactionCommitTablePlanningSupport.PlannedExistingTableChange(
                null, 0L, null, userConflict));

    Response response = service.commit("examples", null, request, tableSupport);

    assertEquals(Response.Status.CONFLICT.getStatusCode(), response.getStatus());
    assertTrue(!service.transactionCommitExecutionSupport.isRetryableConflict(response));
    verify(service.transactionCommitExecutionSupport, never())
        .openTransaction(eq(null), eq(true), eq("examples"), any(), eq("tx-attempt-1"));
  }

  @Test
  void commitReopensFreshAttemptInsteadOfReusingApplyFailedConflictTransaction() {
    TransactionCommitService service = baseService();
    TableGatewaySupport tableSupport = Mockito.mock(TableGatewaySupport.class);
    ResourceId catalogId = ResourceId.newBuilder().setAccountId("acct").setId("cat-1").build();
    ResourceId namespaceId = ResourceId.newBuilder().setAccountId("acct").setId("ns-1").build();
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct").setId("tbl-1").build();
    Table table = Table.newBuilder().setResourceId(tableId).build();
    TransactionCommitRequest request = singlePropertyChangeRequest();

    arrangeCommonSingleTablePlanning(service, tableSupport, catalogId, namespaceId, tableId, table);

    var failedOpen =
        new TransactionCommitExecutionSupport.OpenTransaction(
            "tx-1", TransactionState.TS_APPLY_FAILED_CONFLICT, null, "client-key");
    var retryOpen =
        new TransactionCommitExecutionSupport.OpenTransaction(
            "tx-2", TransactionState.TS_OPEN, null, "client-key:tx-attempt-1");
    when(service.transactionCommitExecutionSupport.openTransaction(
            eq("client-key"), eq(true), eq("examples"), any(), eq(null)))
        .thenReturn(new TransactionCommitExecutionSupport.OpenTransactionResult(failedOpen, null));
    when(service.transactionCommitExecutionSupport.openTransaction(
            eq("client-key"), eq(true), eq("examples"), any(), eq("tx-attempt-1")))
        .thenReturn(new TransactionCommitExecutionSupport.OpenTransactionResult(retryOpen, null));
    when(service.transactionCommitExecutionSupport.apply(eq(retryOpen), any()))
        .thenReturn(Response.noContent().build());

    Response response = service.commit("examples", "client-key", request, tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(service.transactionCommitExecutionSupport, never()).apply(eq(failedOpen), any());
    verify(service.transactionCommitExecutionSupport)
        .openTransaction(eq("client-key"), eq(true), eq("examples"), any(), eq("tx-attempt-1"));
  }

  @Test
  void commitReopensFreshAttemptInsteadOfReusingAbortedTransaction() {
    TransactionCommitService service = baseService();
    TableGatewaySupport tableSupport = Mockito.mock(TableGatewaySupport.class);
    ResourceId catalogId = ResourceId.newBuilder().setAccountId("acct").setId("cat-1").build();
    ResourceId namespaceId = ResourceId.newBuilder().setAccountId("acct").setId("ns-1").build();
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct").setId("tbl-1").build();
    Table table = Table.newBuilder().setResourceId(tableId).build();
    TransactionCommitRequest request = singlePropertyChangeRequest();

    arrangeCommonSingleTablePlanning(service, tableSupport, catalogId, namespaceId, tableId, table);

    var abortedOpen =
        new TransactionCommitExecutionSupport.OpenTransaction(
            "tx-1", TransactionState.TS_ABORTED, null, "client-key");
    var retryOpen =
        new TransactionCommitExecutionSupport.OpenTransaction(
            "tx-2", TransactionState.TS_OPEN, null, "client-key:tx-attempt-1");
    when(service.transactionCommitExecutionSupport.openTransaction(
            eq("client-key"), eq(true), eq("examples"), any(), eq(null)))
        .thenReturn(new TransactionCommitExecutionSupport.OpenTransactionResult(abortedOpen, null));
    when(service.transactionCommitExecutionSupport.openTransaction(
            eq("client-key"), eq(true), eq("examples"), any(), eq("tx-attempt-1")))
        .thenReturn(new TransactionCommitExecutionSupport.OpenTransactionResult(retryOpen, null));
    when(service.transactionCommitExecutionSupport.apply(eq(retryOpen), any()))
        .thenReturn(Response.noContent().build());

    Response response = service.commit("examples", "client-key", request, tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(service.transactionCommitExecutionSupport, never()).apply(eq(abortedOpen), any());
    verify(service.transactionCommitExecutionSupport)
        .openTransaction(eq("client-key"), eq(true), eq("examples"), any(), eq("tx-attempt-1"));
  }

  @Test
  void commitStripsInternalRetryHeaderWhenRetryAttemptsAreExhausted() {
    TransactionCommitService service = baseService();
    service.transactionCommitExecutionSupport =
        Mockito.mock(TransactionCommitExecutionSupport.class, Mockito.CALLS_REAL_METHODS);
    configureFastRetries(service);
    TableGatewaySupport tableSupport = Mockito.mock(TableGatewaySupport.class);
    ResourceId catalogId = ResourceId.newBuilder().setAccountId("acct").setId("cat-1").build();
    ResourceId namespaceId = ResourceId.newBuilder().setAccountId("acct").setId("ns-1").build();
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct").setId("tbl-1").build();
    Table table = Table.newBuilder().setResourceId(tableId).build();
    TransactionCommitRequest request = singlePropertyChangeRequest();

    arrangeCommonSingleTablePlanning(service, tableSupport, catalogId, namespaceId, tableId, table);

    var firstOpen =
        new TransactionCommitExecutionSupport.OpenTransaction(
            "tx-1", TransactionState.TS_OPEN, null, "tx-1");
    var secondOpen =
        new TransactionCommitExecutionSupport.OpenTransaction(
            "tx-2", TransactionState.TS_OPEN, null, "tx-2");
    var thirdOpen =
        new TransactionCommitExecutionSupport.OpenTransaction(
            "tx-3", TransactionState.TS_OPEN, null, "tx-3");
    Mockito.doReturn(new TransactionCommitExecutionSupport.OpenTransactionResult(firstOpen, null))
        .when(service.transactionCommitExecutionSupport)
        .openTransaction(eq(null), eq(true), eq("examples"), any(), eq(null));
    Mockito.doReturn(new TransactionCommitExecutionSupport.OpenTransactionResult(secondOpen, null))
        .when(service.transactionCommitExecutionSupport)
        .openTransaction(eq(null), eq(true), eq("examples"), any(), eq("tx-attempt-1"));
    Mockito.doReturn(new TransactionCommitExecutionSupport.OpenTransactionResult(thirdOpen, null))
        .when(service.transactionCommitExecutionSupport)
        .openTransaction(eq(null), eq(true), eq("examples"), any(), eq("tx-attempt-2"));

    Response retryableConflict =
        Response.status(Response.Status.CONFLICT)
            .header(TransactionCommitExecutionSupport.RETRYABLE_CONFLICT_PROPERTY, "true")
            .entity("conflict")
            .build();
    Mockito.doReturn(retryableConflict)
        .when(service.transactionCommitExecutionSupport)
        .apply(any(), any());

    Response response = service.commit("examples", null, request, tableSupport);

    assertEquals(Response.Status.CONFLICT.getStatusCode(), response.getStatus());
    assertEquals("conflict", response.getEntity());
    assertNull(
        response.getHeaderString(TransactionCommitExecutionSupport.RETRYABLE_CONFLICT_PROPERTY));
    verify(service.transactionCommitExecutionSupport)
        .openTransaction(eq(null), eq(true), eq("examples"), any(), eq("tx-attempt-2"));
  }

  @Test
  void commitCanRetryPastThePreviousThreeAttemptBudget() {
    TransactionCommitService service = baseService();
    service.maxInternalReplanAttempts = 5;
    service.internalReplanInitialSleepMs = 0;
    service.internalReplanMaxSleepMs = 0;
    TableGatewaySupport tableSupport = Mockito.mock(TableGatewaySupport.class);
    ResourceId catalogId = ResourceId.newBuilder().setAccountId("acct").setId("cat-1").build();
    ResourceId namespaceId = ResourceId.newBuilder().setAccountId("acct").setId("ns-1").build();
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct").setId("tbl-1").build();
    Table table = Table.newBuilder().setResourceId(tableId).build();
    TransactionCommitRequest request = singlePropertyChangeRequest();

    arrangeCommonSingleTablePlanning(service, tableSupport, catalogId, namespaceId, tableId, table);

    var firstOpen =
        new TransactionCommitExecutionSupport.OpenTransaction(
            "tx-1", TransactionState.TS_OPEN, null, "tx-1");
    var secondOpen =
        new TransactionCommitExecutionSupport.OpenTransaction(
            "tx-2", TransactionState.TS_OPEN, null, "tx-2");
    var thirdOpen =
        new TransactionCommitExecutionSupport.OpenTransaction(
            "tx-3", TransactionState.TS_OPEN, null, "tx-3");
    var fourthOpen =
        new TransactionCommitExecutionSupport.OpenTransaction(
            "tx-4", TransactionState.TS_OPEN, null, "tx-4");
    when(service.transactionCommitExecutionSupport.openTransaction(
            eq(null), eq(true), eq("examples"), any(), eq(null)))
        .thenReturn(new TransactionCommitExecutionSupport.OpenTransactionResult(firstOpen, null));
    when(service.transactionCommitExecutionSupport.openTransaction(
            eq(null), eq(true), eq("examples"), any(), eq("tx-attempt-1")))
        .thenReturn(new TransactionCommitExecutionSupport.OpenTransactionResult(secondOpen, null));
    when(service.transactionCommitExecutionSupport.openTransaction(
            eq(null), eq(true), eq("examples"), any(), eq("tx-attempt-2")))
        .thenReturn(new TransactionCommitExecutionSupport.OpenTransactionResult(thirdOpen, null));
    when(service.transactionCommitExecutionSupport.openTransaction(
            eq(null), eq(true), eq("examples"), any(), eq("tx-attempt-3")))
        .thenReturn(new TransactionCommitExecutionSupport.OpenTransactionResult(fourthOpen, null));

    Response retryableConflict =
        Response.status(Response.Status.CONFLICT)
            .header(TransactionCommitExecutionSupport.RETRYABLE_CONFLICT_PROPERTY, "true")
            .build();
    when(service.transactionCommitExecutionSupport.apply(eq(firstOpen), any()))
        .thenReturn(retryableConflict);
    when(service.transactionCommitExecutionSupport.apply(eq(secondOpen), any()))
        .thenReturn(retryableConflict);
    when(service.transactionCommitExecutionSupport.apply(eq(thirdOpen), any()))
        .thenReturn(retryableConflict);
    when(service.transactionCommitExecutionSupport.apply(eq(fourthOpen), any()))
        .thenReturn(Response.noContent().build());
    when(service.transactionCommitExecutionSupport.isRetryableConflict(retryableConflict))
        .thenReturn(true);

    Response response = service.commit("examples", null, request, tableSupport);

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    verify(service.transactionCommitExecutionSupport)
        .openTransaction(eq(null), eq(true), eq("examples"), any(), eq("tx-attempt-3"));
    verify(service.tablePlanningSupport, times(4))
        .planExistingTableChange(
            any(),
            any(),
            any(),
            eq(tableId),
            eq(catalogId),
            eq(namespaceId),
            eq("db"),
            eq("orders"),
            any(),
            any(),
            any(),
            eq(tableSupport),
            eq(true));
  }

  private TransactionCommitService baseService() {
    TransactionCommitService service = new TransactionCommitService();
    service.accountContext = Mockito.mock(AccountContext.class);
    service.resourceResolver = Mockito.mock(ResourceResolver.class);
    service.transactionCommitExecutionSupport =
        Mockito.mock(TransactionCommitExecutionSupport.class);
    service.tablePlanningSupport = Mockito.mock(TransactionCommitTablePlanningSupport.class);
    service.snapshotSupport = Mockito.mock(TransactionCommitSnapshotSupport.class);
    service.tableLifecycleService = Mockito.mock(TableLifecycleService.class);
    service.tableCreateTransactionMapper = Mockito.mock(TableCreateTransactionMapper.class);
    service.connectorProvisioningService = Mockito.mock(ConnectorProvisioningService.class);
    configureFastRetries(service);
    return service;
  }

  private void configureFastRetries(TransactionCommitService service) {
    service.maxInternalReplanAttempts = 3;
    service.internalReplanInitialSleepMs = 0;
    service.internalReplanMaxSleepMs = 0;
  }

  private TransactionCommitRequest singlePropertyChangeRequest() {
    return new TransactionCommitRequest(
        List.of(
            new TransactionCommitRequest.TableChange(
                new TableIdentifierDto(List.of("db"), "orders"),
                List.of(),
                List.of(Map.of("action", "set-properties", "updates", Map.of("owner", "it"))))));
  }

  private void arrangeCommonSingleTablePlanning(
      TransactionCommitService service,
      TableGatewaySupport tableSupport,
      ResourceId catalogId,
      ResourceId namespaceId,
      ResourceId tableId,
      Table table) {
    when(service.accountContext.getAccountId()).thenReturn("acct");
    when(service.resourceResolver.catalog("examples"))
        .thenReturn(new CatalogRef("examples", "examples", catalogId));
    when(service.tableLifecycleService.resolveNamespaceId("examples", List.of("db")))
        .thenReturn(namespaceId);
    when(service.tableLifecycleService.resolveTableId("examples", List.of("db"), "orders"))
        .thenReturn(tableId);
    when(service.tableLifecycleService.getTableResponse(tableId))
        .thenReturn(GetTableResponse.newBuilder().setTable(table).build());
    when(service.tablePlanningSupport.normalizeTableIdentity(table, tableId)).thenReturn(table);
    when(service.tablePlanningSupport.planExistingTableChange(
            any(),
            any(),
            any(),
            eq(tableId),
            eq(catalogId),
            eq(namespaceId),
            eq("db"),
            eq("orders"),
            any(),
            any(),
            any(),
            eq(tableSupport),
            eq(true)))
        .thenReturn(
            new TransactionCommitTablePlanningSupport.PlannedExistingTableChange(
                table, 1L, "s3://bucket/metadata/00002.metadata.json", null));
    when(service.connectorProvisioningService.resolveOrCreateForCommit(
            eq("examples"),
            eq(tableSupport),
            eq(List.of("db")),
            eq("orders"),
            eq(tableId),
            eq(table)))
        .thenReturn(new ConnectorProvisioningService.ProvisionResult(table, List.of(), null));
    when(service.snapshotSupport.planAtomicSnapshotChanges(
            eq("acct"),
            any(),
            eq(tableId),
            eq(table),
            eq(tableSupport),
            eq("s3://bucket/metadata/00002.metadata.json"),
            any(),
            eq(List.of())))
        .thenReturn(new TransactionCommitSnapshotSupport.SnapshotChangePlan(List.of(), null));
  }
}
