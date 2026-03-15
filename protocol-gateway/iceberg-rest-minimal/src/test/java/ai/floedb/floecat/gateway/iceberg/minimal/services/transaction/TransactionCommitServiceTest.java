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

package ai.floedb.floecat.gateway.iceberg.minimal.services.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.GetTableResponse;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogResponse;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.ResolveTableResponse;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.minimal.api.dto.TableIdentifierDto;
import ai.floedb.floecat.gateway.iceberg.minimal.api.request.TransactionCommitRequest;
import ai.floedb.floecat.transaction.rpc.BeginTransactionResponse;
import ai.floedb.floecat.transaction.rpc.CommitTransactionResponse;
import ai.floedb.floecat.transaction.rpc.GetTransactionResponse;
import ai.floedb.floecat.transaction.rpc.PrepareTransactionResponse;
import ai.floedb.floecat.transaction.rpc.Transaction;
import ai.floedb.floecat.transaction.rpc.TransactionState;
import io.grpc.Status;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TransactionCommitServiceTest {
  private final TransactionBackend backend = Mockito.mock(TransactionBackend.class);
  private final IcebergMetadataCommitService metadataCommitService =
      Mockito.mock(IcebergMetadataCommitService.class);
  private final ConnectorProvisioningService connectorProvisioningService =
      Mockito.mock(ConnectorProvisioningService.class);
  private final TableCommitJournalService commitJournalService =
      Mockito.mock(TableCommitJournalService.class);
  private final TableCommitSideEffectService sideEffectService =
      Mockito.mock(TableCommitSideEffectService.class);
  private final TransactionCommitService service =
      new TransactionCommitService(
          backend,
          metadataCommitService,
          connectorProvisioningService,
          commitJournalService,
          sideEffectService);

  @Test
  void returnsNoContentWhenApplied() {
    stubLookups();
    when(backend.beginTransaction(any(), any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(backend.getTransaction("tx-1"))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_OPEN))
                .build());
    when(backend.prepareTransaction(Mockito.eq("tx-1"), any(), anyList()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_PREPARED))
                .build());
    when(backend.commitTransaction(Mockito.eq("tx-1"), any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());

    assertEquals(204, service.commit("foo", "idem-1", request()).getStatus());
  }

  @Test
  void acceptsStatisticsUpdatesForCompatibility() {
    stubLookups();
    when(metadataCommitService.plan(any(), any(), any(), any()))
        .thenAnswer(
            invocation ->
                new IcebergMetadataCommitService.PlannedCommit(
                    invocation.getArgument(0), List.of()));
    when(backend.beginTransaction(any(), any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-stats"))
                .build());
    when(backend.getTransaction("tx-stats"))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-stats").setState(TransactionState.TS_OPEN))
                .build());
    when(backend.prepareTransaction(Mockito.eq("tx-stats"), any(), anyList()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-stats")
                        .setState(TransactionState.TS_PREPARED))
                .build());
    when(backend.commitTransaction(Mockito.eq("tx-stats"), any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-stats")
                        .setState(TransactionState.TS_APPLIED))
                .build());

    assertEquals(204, service.commit("foo", "idem-1", requestWithStatistics()).getStatus());
  }

  @Test
  void returnsConflictWhenApplyConflicts() {
    stubLookups();
    when(backend.beginTransaction(any(), any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-2"))
                .build());
    when(backend.getTransaction("tx-2"))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-2").setState(TransactionState.TS_OPEN))
                .build());
    when(backend.prepareTransaction(Mockito.eq("tx-2"), any(), anyList()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-2").setState(TransactionState.TS_PREPARED))
                .build());
    when(backend.commitTransaction(Mockito.eq("tx-2"), any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-2")
                        .setState(TransactionState.TS_APPLY_FAILED_CONFLICT))
                .build());

    assertEquals(409, service.commit("foo", "idem-1", request()).getStatus());
  }

  @Test
  void returnsStateUnknownWhenApplyIsRetryable() {
    stubLookups();
    when(backend.beginTransaction(any(), any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-3"))
                .build());
    when(backend.getTransaction("tx-3"))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-3").setState(TransactionState.TS_OPEN))
                .build());
    when(backend.prepareTransaction(Mockito.eq("tx-3"), any(), anyList()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-3").setState(TransactionState.TS_PREPARED))
                .build());
    when(backend.commitTransaction(Mockito.eq("tx-3"), any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-3")
                        .setState(TransactionState.TS_APPLY_FAILED_RETRYABLE))
                .build());

    assertEquals(503, service.commit("foo", "idem-1", request()).getStatus());
  }

  @Test
  void returns404WhenMissingTableWithoutAssertCreate() {
    when(backend.resolveCatalog("foo"))
        .thenReturn(
            ResolveCatalogResponse.newBuilder()
                .setResourceId(ResourceId.newBuilder().setAccountId("acct-1").setId("cat-id"))
                .build());
    when(backend.resolveNamespace(Mockito.eq("foo"), Mockito.eq(List.of("db"))))
        .thenReturn(
            ResolveNamespaceResponse.newBuilder()
                .setResourceId(ResourceId.newBuilder().setAccountId("acct-1").setId("ns-id"))
                .build());
    when(backend.beginTransaction(any(), any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-4"))
                .build());
    when(backend.getTransaction("tx-4"))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-4").setState(TransactionState.TS_OPEN))
                .build());
    when(backend.resolveTable(Mockito.eq("foo"), Mockito.eq(List.of("db")), Mockito.eq("orders")))
        .thenThrow(Status.NOT_FOUND.asRuntimeException());

    assertEquals(404, service.commit("foo", "idem-1", requestWithoutAssertCreate()).getStatus());
  }

  private void stubLookups() {
    when(connectorProvisioningService.resolveOrCreateForCommit(
            any(), any(), anyList(), any(), any(), any(), any(), any()))
        .thenAnswer(
            invocation ->
                new ConnectorProvisioningService.ProvisionResult(
                    invocation.getArgument(7), null, List.of()));
    when(backend.resolveCatalog("foo"))
        .thenReturn(
            ResolveCatalogResponse.newBuilder()
                .setResourceId(ResourceId.newBuilder().setAccountId("acct-1").setId("cat-id"))
                .build());
    when(backend.resolveNamespace(Mockito.eq("foo"), Mockito.eq(List.of("db"))))
        .thenReturn(
            ResolveNamespaceResponse.newBuilder()
                .setResourceId(ResourceId.newBuilder().setAccountId("acct-1").setId("ns-id"))
                .build());
    when(backend.resolveTable(Mockito.eq("foo"), Mockito.eq(List.of("db")), Mockito.eq("orders")))
        .thenReturn(
            ResolveTableResponse.newBuilder()
                .setResourceId(ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-id"))
                .build());
    when(backend.getTable(ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-id").build()))
        .thenReturn(
            GetTableResponse.newBuilder()
                .setTable(
                    Table.newBuilder()
                        .setResourceId(
                            ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-id"))
                        .setCatalogId(
                            ResourceId.newBuilder().setAccountId("acct-1").setId("cat-id"))
                        .setNamespaceId(
                            ResourceId.newBuilder().setAccountId("acct-1").setId("ns-id"))
                        .setDisplayName("orders")
                        .build())
                .setMeta(MutationMeta.newBuilder().setPointerVersion(7L))
                .build());
  }

  private TransactionCommitRequest request() {
    return new TransactionCommitRequest(
        List.of(
            new TransactionCommitRequest.TableChange(
                new TableIdentifierDto(List.of("db"), "orders"),
                List.of(),
                List.of(
                    Map.of(
                        "action", "set-properties", "updates", Map.of("owner", "integration"))))));
  }

  private TransactionCommitRequest requestWithoutAssertCreate() {
    return request();
  }

  private TransactionCommitRequest requestWithStatistics() {
    return new TransactionCommitRequest(
        List.of(
            new TransactionCommitRequest.TableChange(
                new TableIdentifierDto(List.of("db"), "orders"),
                List.of(),
                List.of(
                    Map.of("action", "set-properties", "updates", Map.of("owner", "integration")),
                    Map.of(
                        "action",
                        "set-statistics",
                        "statistics",
                        Map.of(
                            "snapshot-id",
                            101L,
                            "statistics-path",
                            "s3://warehouse/db/orders/stats/puffin-101.bin",
                            "file-size-in-bytes",
                            1234L))))));
  }
}
