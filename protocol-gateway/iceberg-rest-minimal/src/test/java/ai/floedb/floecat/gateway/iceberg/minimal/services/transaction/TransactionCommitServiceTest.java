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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.GetTableResponse;
import ai.floedb.floecat.catalog.rpc.ListSnapshotsResponse;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogResponse;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.ResolveTableResponse;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.minimal.api.dto.TableIdentifierDto;
import ai.floedb.floecat.gateway.iceberg.minimal.api.request.TransactionCommitRequest;
import ai.floedb.floecat.gateway.iceberg.minimal.config.MinimalGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.minimal.services.account.AccountContext;
import ai.floedb.floecat.gateway.iceberg.minimal.services.compat.TableFormatSupport;
import ai.floedb.floecat.gateway.iceberg.minimal.services.metadata.TableMetadataImportService;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergCommitJournalEntry;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergCommitReplayIndex;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import ai.floedb.floecat.transaction.rpc.BeginTransactionResponse;
import ai.floedb.floecat.transaction.rpc.CommitTransactionResponse;
import ai.floedb.floecat.transaction.rpc.GetTransactionResponse;
import ai.floedb.floecat.transaction.rpc.PrepareTransactionResponse;
import ai.floedb.floecat.transaction.rpc.Transaction;
import ai.floedb.floecat.transaction.rpc.TransactionState;
import ai.floedb.floecat.transaction.rpc.TxChange;
import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TransactionCommitServiceTest {
  private static final String REQUEST_HASH = "B0udjENpf9kwkHq-uWxOqJ-1d7Ui21Ou8XYACjNq0i4";

  private final TransactionBackend backend = Mockito.mock(TransactionBackend.class);
  private final MinimalGatewayConfig config = Mockito.mock(MinimalGatewayConfig.class);
  private final AccountContext accountContext = Mockito.mock(AccountContext.class);
  private final TableFormatSupport tableFormatSupport = Mockito.mock(TableFormatSupport.class);
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
          config,
          accountContext,
          tableFormatSupport,
          metadataCommitService,
          connectorProvisioningService,
          commitJournalService,
          sideEffectService);

  {
    when(accountContext.getAccountId()).thenReturn("acct-1");
  }

  @Test
  void rejectsCommitWhenAccountContextMissing() {
    when(accountContext.getAccountId()).thenReturn(" ");

    assertEquals(400, service.commit("foo", "idem-1", request()).getStatus());
  }

  @Test
  void returnsNoContentWhenApplied() {
    stubLookups();
    when(backend.beginTransaction(any(), any(), any()))
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
  void replaysPostCommitTasksWhenTransactionAlreadyApplied() {
    stubLookups();
    when(backend.beginTransaction(any(), any(), any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-applied"))
                .build());
    when(backend.getTransaction("tx-applied"))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-applied")
                        .setState(TransactionState.TS_APPLIED))
                .build());
    when(commitJournalService.getReplayIndex("acct-1", "tx-applied"))
        .thenReturn(
            java.util.Optional.of(
                IcebergCommitReplayIndex.newBuilder()
                    .setTxId("tx-applied")
                    .setRequestHash(REQUEST_HASH)
                    .addEntries(
                        IcebergCommitJournalEntry.newBuilder()
                            .setTxId("tx-applied")
                            .setRequestHash(REQUEST_HASH)
                            .setTableId(
                                ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-id"))
                            .addNamespacePath("db")
                            .setTableName("orders")
                            .setConnectorId(
                                ResourceId.newBuilder().setAccountId("acct-1").setId("conn-1"))
                            .addAddedSnapshotIds(101L)
                            .addRemovedSnapshotIds(55L))
                    .build()));

    assertEquals(204, service.commit("foo", "idem-1", request()).getStatus());
    verify(sideEffectService)
        .pruneRemovedSnapshots(
            eq(ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-id").build()),
            eq(List.of(55L)));
    verify(sideEffectService)
        .schedulePostCommitStatsSync(
            eq(ResourceId.newBuilder().setAccountId("acct-1").setId("conn-1").build()),
            eq(List.of("db")),
            eq("orders"),
            eq(List.of(101L)));
  }

  @Test
  void acceptsStatisticsUpdatesForCompatibility() {
    stubLookups();
    when(metadataCommitService.plan(any(), any(), any(), any()))
        .thenAnswer(
            invocation ->
                new IcebergMetadataCommitService.PlannedCommit(
                    invocation.getArgument(0), List.of()));
    when(backend.beginTransaction(any(), any(), any()))
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
  void acceptsSpecSchemaAndEncryptionUpdatesForCompatibility() {
    stubLookups();
    when(metadataCommitService.plan(any(), any(), any(), any()))
        .thenAnswer(
            invocation ->
                new IcebergMetadataCommitService.PlannedCommit(
                    invocation.getArgument(0), List.of()));
    when(backend.beginTransaction(any(), any(), any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-metadata"))
                .build());
    when(backend.getTransaction("tx-metadata"))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-metadata")
                        .setState(TransactionState.TS_OPEN))
                .build());
    when(backend.prepareTransaction(Mockito.eq("tx-metadata"), any(), anyList()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-metadata")
                        .setState(TransactionState.TS_PREPARED))
                .build());
    when(backend.commitTransaction(Mockito.eq("tx-metadata"), any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-metadata")
                        .setState(TransactionState.TS_APPLIED))
                .build());

    assertEquals(
        204, service.commit("foo", "idem-1", requestWithSpecSchemaAndEncryption()).getStatus());
  }

  @Test
  void returnsConflictWhenApplyConflicts() {
    stubLookups();
    when(backend.beginTransaction(any(), any(), any()))
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
  void blocksCommitsForDeltaTablesWhenCompatIsReadOnly() {
    stubLookups();
    when(config.deltaCompat())
        .thenReturn(
            Optional.of(
                new MinimalGatewayConfig.DeltaCompatConfig() {
                  @Override
                  public boolean enabled() {
                    return true;
                  }

                  @Override
                  public boolean readOnly() {
                    return true;
                  }
                }));
    when(tableFormatSupport.isDelta(any(Table.class))).thenReturn(true);
    when(backend.beginTransaction(any(), any(), any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-delta"))
                .build());
    when(backend.getTransaction("tx-delta"))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-delta").setState(TransactionState.TS_OPEN))
                .build());

    assertEquals(409, service.commit("foo", "idem-delta", request()).getStatus());
  }

  @Test
  void returnsStateUnknownWhenApplyIsRetryable() {
    stubLookups();
    when(backend.beginTransaction(any(), any(), any()))
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
    when(backend.beginTransaction(any(), any(), any()))
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

  @Test
  void doesNotUseHashDerivedIdempotencyKeyWhenClientKeyIsAbsent() {
    stubLookups();
    when(backend.beginTransaction(any(), any(), any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-1"))
                .build());
    when(backend.getTransaction("tx-1"))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-1").setState(TransactionState.TS_APPLIED))
                .build());
    when(commitJournalService.getReplayIndex("acct-1", "tx-1"))
        .thenReturn(
            Optional.of(
                IcebergCommitReplayIndex.newBuilder()
                    .setTxId("tx-1")
                    .setRequestHash(REQUEST_HASH)
                    .addEntries(
                        replayEntry("tx-1")
                            .setMetadataLocation("s3://warehouse/db/orders/metadata/current.json")
                            .setTableUuid("uuid-1"))
                    .build()));

    assertEquals(204, service.commit("foo", null, request()).getStatus());
    verify(backend).beginTransaction(isNull(), eq(REQUEST_HASH), eq(Duration.ofMinutes(30)));
  }

  @Test
  void returnsConflictOnRequestHashMismatchWithoutAbortingTransaction() {
    stubLookups();
    when(backend.beginTransaction(any(), any(), any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-mismatch"))
                .build());
    when(backend.getTransaction("tx-mismatch"))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-mismatch")
                        .setState(TransactionState.TS_OPEN)
                        .putProperties("iceberg.commit.request-hash", "different-hash"))
                .build());

    assertEquals(409, service.commit("foo", "idem-mismatch", request()).getStatus());
    verify(backend, never()).abortTransaction(eq("tx-mismatch"), any());
  }

  @Test
  void returnsStateUnknownWhenTransactionStateIsUnspecified() {
    stubLookups();
    when(backend.beginTransaction(any(), any(), any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-unspecified"))
                .build());
    when(backend.getTransaction("tx-unspecified"))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-unspecified")
                        .setState(TransactionState.TS_UNSPECIFIED))
                .build());

    assertEquals(503, service.commit("foo", "idem-unspecified", request()).getStatus());
    verify(backend, never()).prepareTransaction(eq("tx-unspecified"), any(), anyList());
    verify(backend, never()).commitTransaction(eq("tx-unspecified"), any());
  }

  @Test
  void validatesAssertCreateStateBeforeReplayingAppliedTransaction() {
    stubLookups();
    when(backend.beginTransaction(any(), any(), any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-applied-create"))
                .build());
    when(backend.getTransaction("tx-applied-create"))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-applied-create")
                        .setState(TransactionState.TS_APPLIED))
                .build());
    when(commitJournalService.getReplayIndex("acct-1", "tx-applied-create"))
        .thenReturn(
            Optional.of(
                IcebergCommitReplayIndex.newBuilder()
                    .setTxId("tx-applied-create")
                    .setRequestHash(requestHash(requestWithAssertCreate()))
                    .addEntries(
                        replayEntry("tx-applied-create")
                            .setMetadataLocation("s3://warehouse/db/orders/metadata/current.json")
                            .setTableUuid("uuid-1"))
                    .build()));

    assertEquals(204, service.commit("foo", "idem-create", requestWithAssertCreate()).getStatus());
    verify(backend).resolveTable("foo", List.of("db"), "orders");
  }

  @Test
  void rejectsAppliedReplayWhenJournaledMetadataLocationDoesNotMatch() {
    stubLookups();
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
                        .putProperties(
                            "metadata-location", "s3://warehouse/db/orders/metadata/current.json")
                        .putProperties("table-uuid", "uuid-1")
                        .build())
                .setMeta(MutationMeta.newBuilder().setPointerVersion(7L))
                .build());
    when(backend.beginTransaction(any(), any(), any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-applied-mismatch"))
                .build());
    when(backend.getTransaction("tx-applied-mismatch"))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-applied-mismatch")
                        .setState(TransactionState.TS_APPLIED))
                .build());
    when(commitJournalService.getReplayIndex("acct-1", "tx-applied-mismatch"))
        .thenReturn(
            Optional.of(
                IcebergCommitReplayIndex.newBuilder()
                    .setTxId("tx-applied-mismatch")
                    .setRequestHash(REQUEST_HASH)
                    .addEntries(
                        replayEntry("tx-applied-mismatch")
                            .setMetadataLocation("s3://warehouse/db/orders/metadata/expected.json")
                            .setTableUuid("uuid-1"))
                    .build()));

    assertEquals(409, service.commit("foo", "idem-mismatch", request()).getStatus());
  }

  @Test
  void returnsNotFoundWhenAppliedCreateReplayTargetIsMissing() {
    when(config.deltaCompat()).thenReturn(Optional.empty());
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
    when(backend.beginTransaction(any(), any(), any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-applied-missing"))
                .build());
    when(backend.getTransaction("tx-applied-missing"))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-applied-missing")
                        .setState(TransactionState.TS_APPLIED))
                .build());
    when(commitJournalService.getReplayIndex("acct-1", "tx-applied-missing"))
        .thenReturn(
            Optional.of(
                IcebergCommitReplayIndex.newBuilder()
                    .setTxId("tx-applied-missing")
                    .setRequestHash(requestHash(requestWithAssertCreate()))
                    .addEntries(replayEntry("tx-applied-missing"))
                    .build()));
    when(backend.resolveTable("foo", List.of("db"), "orders"))
        .thenThrow(Status.NOT_FOUND.withDescription("missing").asRuntimeException());
    assertEquals(404, service.commit("foo", "idem-create", requestWithAssertCreate()).getStatus());
    verify(backend).resolveTable("foo", List.of("db"), "orders");
  }

  @Test
  void acceptsAppliedTransactionWhenReplayValidationDataIsMissing() {
    stubLookups();
    when(backend.beginTransaction(any(), any(), any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-applied-missing-replay"))
                .build());
    when(backend.getTransaction("tx-applied-missing-replay"))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-applied-missing-replay")
                        .setState(TransactionState.TS_APPLIED))
                .build());
    when(commitJournalService.getReplayIndex("acct-1", "tx-applied-missing-replay"))
        .thenReturn(Optional.empty());

    assertEquals(204, service.commit("foo", "idem-replay-missing", request()).getStatus());
  }

  @Test
  void replaysAppliedTransactionFromPerTableJournalWhenReplayIndexIsMissing() {
    stubLookups();
    when(backend.beginTransaction(any(), any(), any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-applied-journal-fallback"))
                .build());
    when(backend.getTransaction("tx-applied-journal-fallback"))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-applied-journal-fallback")
                        .setState(TransactionState.TS_APPLIED))
                .build());
    when(commitJournalService.getReplayIndex("acct-1", "tx-applied-journal-fallback"))
        .thenReturn(Optional.empty());
    when(commitJournalService.get("acct-1", "tbl-id", "tx-applied-journal-fallback"))
        .thenReturn(
            Optional.of(
                IcebergCommitJournalEntry.newBuilder()
                    .setTxId("tx-applied-journal-fallback")
                    .setRequestHash(requestHash(request()))
                    .setTableId(ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-id"))
                    .addNamespacePath("db")
                    .setTableName("orders")
                    .setMetadataLocation("s3://warehouse/db/orders/metadata/current.json")
                    .setTableUuid("uuid-1")
                    .build()));

    assertEquals(204, service.commit("foo", "idem-journal-fallback", request()).getStatus());
  }

  @Test
  void acceptsAppliedAssertCreateWhenReplayValidationDataIsMissing() {
    when(config.deltaCompat()).thenReturn(Optional.empty());
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
    when(backend.beginTransaction(any(), any(), any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-applied-missing-create-replay"))
                .build());
    when(backend.getTransaction("tx-applied-missing-create-replay"))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-applied-missing-create-replay")
                        .setState(TransactionState.TS_APPLIED))
                .build());
    when(commitJournalService.getReplayIndex("acct-1", "tx-applied-missing-create-replay"))
        .thenReturn(Optional.empty());
    when(backend.resolveTable("foo", List.of("db"), "orders"))
        .thenThrow(Status.NOT_FOUND.withDescription("missing").asRuntimeException());

    assertEquals(204, service.commit("foo", "idem-create", requestWithAssertCreate()).getStatus());
  }

  @Test
  void usesTransactionCreatedAtForCreateStub() {
    when(config.deltaCompat()).thenReturn(Optional.empty());
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
    when(backend.resolveTable("foo", List.of("db"), "orders"))
        .thenThrow(Status.NOT_FOUND.asRuntimeException());
    when(metadataCommitService.planCreate(any(), any(), any()))
        .thenAnswer(
            invocation ->
                new IcebergMetadataCommitService.PlannedCommit(
                    invocation.getArgument(0), List.of()));
    when(connectorProvisioningService.resolveOrCreateForCommit(
            any(), any(), anyList(), any(), any(), any(), any(), any()))
        .thenAnswer(
            invocation ->
                new ConnectorProvisioningService.ProvisionResult(
                    invocation.getArgument(7), null, List.of()));
    when(backend.beginTransaction(any(), any(), any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-create")
                        .setCreatedAt(Timestamps.fromMillis(1234L)))
                .build());
    when(backend.getTransaction("tx-create"))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-create")
                        .setState(TransactionState.TS_OPEN)
                        .setCreatedAt(Timestamps.fromMillis(1234L)))
                .build());
    when(backend.prepareTransaction(eq("tx-create"), any(), anyList()))
        .thenReturn(
            PrepareTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-create")
                        .setState(TransactionState.TS_PREPARED))
                .build());
    when(backend.commitTransaction(eq("tx-create"), any()))
        .thenReturn(
            CommitTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-create")
                        .setState(TransactionState.TS_APPLIED))
                .build());

    assertEquals(204, service.commit("foo", "idem-create", requestWithAssertCreate()).getStatus());
    verify(metadataCommitService)
        .planCreate(
            Mockito.argThat(
                table ->
                    table != null
                        && table.hasCreatedAt()
                        && Timestamps.toMillis(table.getCreatedAt()) == 1234L),
            any(),
            any());
  }

  @Test
  void validatesImportedReplayStateAgainstJournaledMetadata() {
    TableMetadataImportService.ImportedMetadata imported =
        new TableMetadataImportService.ImportedMetadata(
            "{\"type\":\"struct\",\"schema-id\":0,\"fields\":[]}",
            Map.of("metadata-location", "s3://warehouse/db/orders/metadata/expected.json"),
            "s3://warehouse/db/orders",
            IcebergMetadata.newBuilder()
                .setFormatVersion(2)
                .setTableUuid("uuid-1")
                .setMetadataLocation("s3://warehouse/db/orders/metadata/expected.json")
                .build(),
            null,
            List.of());
    when(config.deltaCompat()).thenReturn(Optional.empty());
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
    when(backend.resolveTable("foo", List.of("db"), "orders"))
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
                        .putProperties(
                            "metadata-location", "s3://warehouse/db/orders/metadata/current.json")
                        .putProperties("table-uuid", "uuid-1")
                        .build())
                .setMeta(MutationMeta.newBuilder().setPointerVersion(7L))
                .build());
    when(backend.beginTransaction(any(), any(), any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-imported-replay"))
                .build());
    when(backend.getTransaction("tx-imported-replay"))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-imported-replay")
                        .setState(TransactionState.TS_APPLIED))
                .build());
    when(commitJournalService.getReplayIndex("acct-1", "tx-imported-replay"))
        .thenReturn(
            Optional.of(
                IcebergCommitReplayIndex.newBuilder()
                    .setTxId("tx-imported-replay")
                    .setRequestHash(
                        registerRequestHash(List.of("db"), "orders", imported, Map.of(), false))
                    .addEntries(
                        replayEntry("tx-imported-replay")
                            .setMetadataLocation("s3://warehouse/db/orders/metadata/expected.json")
                            .setTableUuid("uuid-1"))
                    .build()));

    assertEquals(
        409,
        service
            .registerImported(
                "foo", "idem-import", List.of("db"), "orders", imported, Map.of(), false)
            .getStatus());
  }

  @Test
  void rejectsDuplicateIdentifiersAfterNormalization() {
    TransactionCommitRequest request =
        new TransactionCommitRequest(
            List.of(
                new TransactionCommitRequest.TableChange(
                    new TableIdentifierDto(List.of("db"), "orders"),
                    List.of(),
                    List.of(Map.of("action", "set-properties", "updates", Map.of("k", "v")))),
                new TransactionCommitRequest.TableChange(
                    new TableIdentifierDto(List.of("db"), " orders "),
                    List.of(),
                    List.of(Map.of("action", "set-properties", "updates", Map.of("k2", "v2"))))));

    assertEquals(400, service.commit("foo", "idem-1", request).getStatus());
  }

  @Test
  void rejectsMalformedSimpleUpdates() {
    stubLookups();
    when(backend.beginTransaction(any(), any(), any()))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(Transaction.newBuilder().setTxId("tx-bad"))
                .build());
    when(backend.getTransaction("tx-bad"))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder().setTxId("tx-bad").setState(TransactionState.TS_OPEN))
                .build());
    TransactionCommitRequest request =
        new TransactionCommitRequest(
            List.of(
                new TransactionCommitRequest.TableChange(
                    new TableIdentifierDto(List.of("db"), "orders"),
                    List.of(),
                    List.of(Map.of("action", "set-properties", "updates", "not-a-map")))));

    assertEquals(400, service.commit("foo", "idem-1", request).getStatus());
  }

  @Test
  void concurrentCommitsProduceOneWinnerAndOneConflict() throws Exception {
    var concurrentBackend = new ConcurrentConflictBackend();
    var localConfig = Mockito.mock(MinimalGatewayConfig.class);
    var localAccountContext = Mockito.mock(AccountContext.class);
    var localTableFormatSupport = Mockito.mock(TableFormatSupport.class);
    var localMetadataCommitService = Mockito.mock(IcebergMetadataCommitService.class);
    var localConnectorProvisioningService = Mockito.mock(ConnectorProvisioningService.class);
    var localCommitJournalService = Mockito.mock(TableCommitJournalService.class);
    var localSideEffectService = Mockito.mock(TableCommitSideEffectService.class);
    var localService =
        new TransactionCommitService(
            concurrentBackend,
            localConfig,
            localAccountContext,
            localTableFormatSupport,
            localMetadataCommitService,
            localConnectorProvisioningService,
            localCommitJournalService,
            localSideEffectService);
    when(localConfig.deltaCompat()).thenReturn(Optional.empty());
    when(localAccountContext.getAccountId()).thenReturn("acct-1");

    when(localConnectorProvisioningService.resolveOrCreateForCommit(
            any(), any(), anyList(), any(), any(), any(), any(), any()))
        .thenAnswer(
            invocation ->
                new ConnectorProvisioningService.ProvisionResult(
                    invocation.getArgument(7), null, List.of()));

    var pool = Executors.newFixedThreadPool(2);
    try {
      Future<Integer> first =
          pool.submit(() -> localService.commit("foo", "idem-a", request()).getStatus());
      Future<Integer> second =
          pool.submit(() -> localService.commit("foo", "idem-b", request()).getStatus());

      List<Integer> statuses =
          List.of(first.get(5, TimeUnit.SECONDS), second.get(5, TimeUnit.SECONDS));
      assertEquals(1L, statuses.stream().filter(status -> status == 204).count());
      assertEquals(1L, statuses.stream().filter(status -> status == 409).count());

      verify(localSideEffectService, Mockito.times(1))
          .pruneRemovedSnapshots(
              eq(ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-id").build()),
              eq(List.of()));
      verify(localSideEffectService, Mockito.times(1))
          .schedulePostCommitStatsSync(eq(null), eq(List.of("db")), eq("orders"), eq(List.of()));
    } finally {
      pool.shutdownNow();
    }
  }

  private void stubLookups() {
    when(config.deltaCompat()).thenReturn(Optional.empty());
    when(config.idempotencyKeyLifetime()).thenReturn(Duration.ofMinutes(30));
    when(accountContext.getAccountId()).thenReturn("acct-1");
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
                        .putProperties(
                            "metadata-location", "s3://warehouse/db/orders/metadata/current.json")
                        .putProperties("table-uuid", "uuid-1")
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

  private TransactionCommitRequest requestWithAssertCreate() {
    return new TransactionCommitRequest(
        List.of(
            new TransactionCommitRequest.TableChange(
                new TableIdentifierDto(List.of("db"), "orders"),
                List.of(Map.of("type", "assert-create")),
                List.of(
                    Map.of("action", "set-location", "location", "s3://warehouse/db/orders"),
                    Map.of("action", "upgrade-format-version", "format-version", 2),
                    Map.of(
                        "action",
                        "add-schema",
                        "schema",
                        Map.of(
                            "type",
                            "struct",
                            "schema-id",
                            0,
                            "fields",
                            List.of(
                                Map.of("id", 1, "name", "id", "required", false, "type", "int")))),
                    Map.of("action", "set-current-schema", "schema-id", 0),
                    Map.of("action", "add-spec", "spec", Map.of("spec-id", 0, "fields", List.of())),
                    Map.of("action", "set-default-spec", "spec-id", 0),
                    Map.of(
                        "action",
                        "add-sort-order",
                        "sort-order",
                        Map.of("order-id", 0, "fields", List.of())),
                    Map.of("action", "set-default-sort-order", "sort-order-id", 0),
                    Map.of(
                        "action",
                        "set-properties",
                        "updates",
                        Map.of("write.format.default", "PARQUET", "last-sequence-number", "0"))))));
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

  private TransactionCommitRequest requestWithSpecSchemaAndEncryption() {
    return new TransactionCommitRequest(
        List.of(
            new TransactionCommitRequest.TableChange(
                new TableIdentifierDto(List.of("db"), "orders"),
                List.of(),
                List.of(
                    Map.of("action", "remove-partition-specs", "spec-ids", List.of(1)),
                    Map.of("action", "remove-schemas", "schema-ids", List.of(1)),
                    Map.of(
                        "action",
                        "add-encryption-key",
                        "encryption-key",
                        Map.of(
                            "key-id", "key-1",
                            "encrypted-key-metadata", "AQID")),
                    Map.of("action", "remove-encryption-key", "key-id", "key-0")))));
  }

  private String requestHash(TransactionCommitRequest request) {
    return hashCanonicalValue(canonicalize(request.tableChanges()));
  }

  private IcebergCommitJournalEntry.Builder replayEntry(String txId) {
    return IcebergCommitJournalEntry.newBuilder()
        .setTxId(txId)
        .setRequestHash(REQUEST_HASH)
        .setTableId(ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-id"))
        .addNamespacePath("db")
        .setTableName("orders");
  }

  private String registerRequestHash(
      List<String> namespacePath,
      String tableName,
      TableMetadataImportService.ImportedMetadata imported,
      Map<String, String> mergedProperties,
      boolean overwrite) {
    Map<String, Object> canonical = new LinkedHashMap<>();
    canonical.put("namespace", namespacePath);
    canonical.put("table", tableName);
    canonical.put("overwrite", overwrite);
    canonical.put("properties", mergedProperties == null ? Map.of() : mergedProperties);
    canonical.put(
        "metadata-location",
        imported == null || imported.icebergMetadata() == null
            ? null
            : imported.icebergMetadata().getMetadataLocation());
    canonical.put(
        "current-snapshot-id",
        imported == null || imported.currentSnapshot() == null
            ? null
            : imported.currentSnapshot().snapshotId());
    canonical.put(
        "snapshot-ids",
        imported == null || imported.snapshots() == null
            ? List.of()
            : imported.snapshots().stream()
                .map(TableMetadataImportService.ImportedSnapshot::snapshotId)
                .toList());
    return hashCanonicalValue(canonicalize(canonical));
  }

  private String hashCanonicalValue(String canonicalValue) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      digest.update(canonicalValue.getBytes(StandardCharsets.UTF_8));
      return Base64.getUrlEncoder().withoutPadding().encodeToString(digest.digest());
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private String canonicalize(Object value) {
    if (value == null) {
      return "null";
    }
    if (value instanceof TransactionCommitRequest.TableChange change) {
      return "{identifier:"
          + canonicalize(change.identifier())
          + ",requirements:"
          + canonicalize(change.requirements())
          + ",updates:"
          + canonicalize(change.updates())
          + "}";
    }
    if (value instanceof TableIdentifierDto identifier) {
      return "{namespace:"
          + canonicalize(identifier.namespace())
          + ",name:"
          + canonicalize(identifier.name())
          + "}";
    }
    if (value instanceof Map<?, ?> map) {
      Map<String, Object> sorted = new TreeMap<>();
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        sorted.put(String.valueOf(entry.getKey()), entry.getValue());
      }
      List<String> parts = new ArrayList<>();
      for (Map.Entry<String, Object> entry : sorted.entrySet()) {
        parts.add(entry.getKey() + ":" + canonicalize(entry.getValue()));
      }
      return "{" + String.join(",", parts) + "}";
    }
    if (value instanceof List<?> list) {
      List<String> parts = new ArrayList<>();
      for (Object item : list) {
        parts.add(canonicalize(item));
      }
      return "[" + String.join(",", parts) + "]";
    }
    return String.valueOf(value);
  }

  private static final class ConcurrentConflictBackend implements TransactionBackend {
    private final ResourceId catalogId =
        ResourceId.newBuilder().setAccountId("acct-1").setId("cat-id").build();
    private final ResourceId namespaceId =
        ResourceId.newBuilder().setAccountId("acct-1").setId("ns-id").build();
    private final ResourceId tableId =
        ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-id").build();
    private final Table table =
        Table.newBuilder()
            .setResourceId(tableId)
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setDisplayName("orders")
            .build();
    private final AtomicInteger tableVersion = new AtomicInteger(7);
    private final AtomicInteger txCounter = new AtomicInteger();
    private final ConcurrentHashMap<String, TransactionState> txStates = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, List<TxChange>> preparedChanges =
        new ConcurrentHashMap<>();
    private final CountDownLatch bothPrepared = new CountDownLatch(2);

    @Override
    public ResolveCatalogResponse resolveCatalog(String prefix) {
      return ResolveCatalogResponse.newBuilder().setResourceId(catalogId).build();
    }

    @Override
    public ResolveNamespaceResponse resolveNamespace(String prefix, List<String> namespacePath) {
      return ResolveNamespaceResponse.newBuilder().setResourceId(namespaceId).build();
    }

    @Override
    public ResolveTableResponse resolveTable(
        String prefix, List<String> namespacePath, String tableName) {
      return ResolveTableResponse.newBuilder().setResourceId(tableId).build();
    }

    @Override
    public GetTableResponse getTable(ResourceId ignoredTableId) {
      return GetTableResponse.newBuilder()
          .setTable(table)
          .setMeta(MutationMeta.newBuilder().setPointerVersion(tableVersion.get()))
          .build();
    }

    @Override
    public ListSnapshotsResponse listSnapshots(ResourceId tableId) {
      return ListSnapshotsResponse.getDefaultInstance();
    }

    @Override
    public BeginTransactionResponse beginTransaction(
        String idempotencyKey, String requestHash, Duration ttl) {
      String txId = "tx-" + txCounter.incrementAndGet();
      txStates.put(txId, TransactionState.TS_OPEN);
      return BeginTransactionResponse.newBuilder()
          .setTransaction(Transaction.newBuilder().setTxId(txId))
          .build();
    }

    @Override
    public GetTransactionResponse getTransaction(String txId) {
      return GetTransactionResponse.newBuilder()
          .setTransaction(Transaction.newBuilder().setTxId(txId).setState(txStates.get(txId)))
          .build();
    }

    @Override
    public PrepareTransactionResponse prepareTransaction(
        String txId, String idempotencyKey, List<TxChange> changes) {
      preparedChanges.put(txId, new ArrayList<>(changes));
      txStates.put(txId, TransactionState.TS_PREPARED);
      bothPrepared.countDown();
      try {
        if (!bothPrepared.await(5, TimeUnit.SECONDS)) {
          throw new AssertionError("Timed out waiting for concurrent prepare");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError("Interrupted waiting for concurrent prepare", e);
      }
      return PrepareTransactionResponse.newBuilder()
          .setTransaction(
              Transaction.newBuilder().setTxId(txId).setState(TransactionState.TS_PREPARED))
          .build();
    }

    @Override
    public CommitTransactionResponse commitTransaction(String txId, String idempotencyKey) {
      List<TxChange> changes = preparedChanges.get(txId);
      long expectedVersion =
          changes.stream()
              .filter(change -> change.hasTableId() && change.getTableId().equals(tableId))
              .findFirst()
              .orElseThrow()
              .getPrecondition()
              .getExpectedVersion();
      TransactionState outcome =
          tableVersion.compareAndSet((int) expectedVersion, (int) expectedVersion + 1)
              ? TransactionState.TS_APPLIED
              : TransactionState.TS_APPLY_FAILED_CONFLICT;
      txStates.put(txId, outcome);
      return CommitTransactionResponse.newBuilder()
          .setTransaction(Transaction.newBuilder().setTxId(txId).setState(outcome))
          .build();
    }

    @Override
    public ai.floedb.floecat.transaction.rpc.AbortTransactionResponse abortTransaction(
        String txId, String reason) {
      txStates.put(txId, TransactionState.TS_ABORTED);
      return ai.floedb.floecat.transaction.rpc.AbortTransactionResponse.newBuilder().build();
    }
  }
}
