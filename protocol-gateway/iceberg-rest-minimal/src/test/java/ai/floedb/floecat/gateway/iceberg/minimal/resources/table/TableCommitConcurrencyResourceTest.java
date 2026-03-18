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

package ai.floedb.floecat.gateway.iceberg.minimal.resources.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.GetTableResponse;
import ai.floedb.floecat.catalog.rpc.ListSnapshotsResponse;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogResponse;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.ResolveTableResponse;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.minimal.api.request.TableCommitRequest;
import ai.floedb.floecat.gateway.iceberg.minimal.config.MinimalGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.minimal.services.account.AccountContext;
import ai.floedb.floecat.gateway.iceberg.minimal.services.compat.DeltaIcebergMetadataService;
import ai.floedb.floecat.gateway.iceberg.minimal.services.compat.TableFormatSupport;
import ai.floedb.floecat.gateway.iceberg.minimal.services.metadata.TableMetadataImportService;
import ai.floedb.floecat.gateway.iceberg.minimal.services.table.ConnectorCleanupService;
import ai.floedb.floecat.gateway.iceberg.minimal.services.table.TableBackend;
import ai.floedb.floecat.gateway.iceberg.minimal.services.table.TableStorageCleanupService;
import ai.floedb.floecat.gateway.iceberg.minimal.services.transaction.ConnectorProvisioningService;
import ai.floedb.floecat.gateway.iceberg.minimal.services.transaction.IcebergMetadataCommitService;
import ai.floedb.floecat.gateway.iceberg.minimal.services.transaction.TableCommitJournalService;
import ai.floedb.floecat.gateway.iceberg.minimal.services.transaction.TableCommitSideEffectService;
import ai.floedb.floecat.gateway.iceberg.minimal.services.transaction.TransactionBackend;
import ai.floedb.floecat.gateway.iceberg.minimal.services.transaction.TransactionCommitService;
import ai.floedb.floecat.transaction.rpc.AbortTransactionResponse;
import ai.floedb.floecat.transaction.rpc.BeginTransactionResponse;
import ai.floedb.floecat.transaction.rpc.CommitTransactionResponse;
import ai.floedb.floecat.transaction.rpc.GetTransactionResponse;
import ai.floedb.floecat.transaction.rpc.PrepareTransactionResponse;
import ai.floedb.floecat.transaction.rpc.Transaction;
import ai.floedb.floecat.transaction.rpc.TransactionState;
import ai.floedb.floecat.transaction.rpc.TxChange;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.ws.rs.core.Response;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TableCommitConcurrencyResourceTest {
  @Test
  void concurrentResourceCommitsProduceOneWinnerAndOneConflict() throws Exception {
    var backend = new ConcurrentConflictBackend();
    IcebergMetadataCommitService metadataCommitService =
        Mockito.mock(IcebergMetadataCommitService.class);
    TableFormatSupport tableFormatSupport = Mockito.mock(TableFormatSupport.class);
    ConnectorProvisioningService connectorProvisioningService =
        Mockito.mock(ConnectorProvisioningService.class);
    TableCommitJournalService commitJournalService = Mockito.mock(TableCommitJournalService.class);
    TableCommitSideEffectService sideEffectService =
        Mockito.mock(TableCommitSideEffectService.class);
    when(connectorProvisioningService.resolveOrCreateForCommit(
            any(), any(), any(), any(), any(), any(), any(), any()))
        .thenAnswer(
            invocation ->
                new ConnectorProvisioningService.ProvisionResult(
                    invocation.getArgument(7), null, List.of()));
    when(metadataCommitService.plan(any(), any(), any(), any()))
        .thenAnswer(
            invocation ->
                new IcebergMetadataCommitService.PlannedCommit(
                    ((Table) invocation.getArgument(0)).toBuilder().putProperties("k", "v").build(),
                    List.of()));
    AccountContext accountContext = Mockito.mock(AccountContext.class);
    when(accountContext.getAccountId()).thenReturn("acct-1");

    TransactionCommitService txService =
        new TransactionCommitService(
            backend,
            testConfig(),
            accountContext,
            tableFormatSupport,
            metadataCommitService,
            connectorProvisioningService,
            commitJournalService,
            sideEffectService);

    TableBackend tableBackend = Mockito.mock(TableBackend.class);
    when(tableBackend.get("foo", List.of("db"), "orders")).thenReturn(backend.currentTable);

    TableResource resource =
        new TableResource(
            tableBackend,
            new ObjectMapper(),
            Mockito.mock(TableMetadataImportService.class),
            txService,
            Mockito.mock(ConnectorCleanupService.class),
            Mockito.mock(TableStorageCleanupService.class),
            testConfig(),
            Mockito.mock(DeltaIcebergMetadataService.class));

    var pool = Executors.newFixedThreadPool(2);
    try {
      Future<Response> first =
          pool.submit(() -> resource.commit("foo", "db", "orders", "idem-1", commitRequest()));
      Future<Response> second =
          pool.submit(() -> resource.commit("foo", "db", "orders", "idem-2", commitRequest()));

      int a = first.get(5, TimeUnit.SECONDS).getStatus();
      int b = second.get(5, TimeUnit.SECONDS).getStatus();

      assertEquals(List.of(200, 409), List.of(a, b).stream().sorted().toList());
    } finally {
      pool.shutdownNow();
    }
  }

  private TableCommitRequest commitRequest() {
    return new TableCommitRequest(
        List.of(Map.of("type", "assert-table-uuid", "uuid", "uuid-1")),
        List.of(Map.of("action", "set-properties", "updates", Map.of("k", "v"))));
  }

  private MinimalGatewayConfig testConfig() {
    return new MinimalGatewayConfig() {
      @Override
      public String upstreamTarget() {
        return "localhost:9100";
      }

      @Override
      public boolean upstreamPlaintext() {
        return true;
      }

      @Override
      public String authMode() {
        return "dev";
      }

      @Override
      public String authHeader() {
        return "authorization";
      }

      @Override
      public String accountClaim() {
        return "account_id";
      }

      @Override
      public Optional<String> defaultAccountId() {
        return Optional.of("acct-1");
      }

      @Override
      public Optional<String> defaultAuthorization() {
        return Optional.empty();
      }

      @Override
      public Optional<String> defaultPrefix() {
        return Optional.of("examples");
      }

      @Override
      public boolean devAllowMissingAuth() {
        return false;
      }

      @Override
      public Map<String, String> catalogMapping() {
        return Map.of("foo", "foo");
      }

      @Override
      public Duration idempotencyKeyLifetime() {
        return Duration.ofMinutes(30);
      }

      @Override
      public Duration planTaskTtl() {
        return Duration.ofMinutes(5);
      }

      @Override
      public int planTaskFilesPerTask() {
        return 128;
      }

      @Override
      public Optional<StorageCredentialConfig> storageCredential() {
        return Optional.empty();
      }

      @Override
      public Optional<String> metadataFileIo() {
        return Optional.empty();
      }

      @Override
      public Optional<String> metadataFileIoRoot() {
        return Optional.empty();
      }

      @Override
      public Optional<String> metadataS3Endpoint() {
        return Optional.empty();
      }

      @Override
      public boolean metadataS3PathStyleAccess() {
        return true;
      }

      @Override
      public Optional<String> metadataS3Region() {
        return Optional.empty();
      }

      @Override
      public Optional<String> metadataClientRegion() {
        return Optional.empty();
      }

      @Override
      public Optional<String> metadataS3AccessKeyId() {
        return Optional.empty();
      }

      @Override
      public Optional<String> metadataS3SecretAccessKey() {
        return Optional.empty();
      }

      @Override
      public Optional<String> defaultWarehousePath() {
        return Optional.empty();
      }

      @Override
      public Optional<DeltaCompatConfig> deltaCompat() {
        return Optional.empty();
      }

      @Override
      public boolean logRequestBodies() {
        return false;
      }

      @Override
      public int logRequestBodyMaxChars() {
        return 8192;
      }
    };
  }

  private static final class ConcurrentConflictBackend implements TransactionBackend {
    private final AtomicInteger sequence = new AtomicInteger(1);
    private final ConcurrentHashMap<String, TransactionState> states = new ConcurrentHashMap<>();
    private final CountDownLatch bothPrepared = new CountDownLatch(2);
    private volatile int tableVersion = 1;
    private final Table currentTable =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-1"))
            .setDisplayName("orders")
            .putProperties("table-uuid", "uuid-1")
            .build();

    @Override
    public BeginTransactionResponse beginTransaction(
        String idempotencyKey, String requestHash, java.time.Duration ttl) {
      String txId = "tx-" + sequence.getAndIncrement();
      states.put(txId, TransactionState.TS_OPEN);
      return BeginTransactionResponse.newBuilder()
          .setTransaction(Transaction.newBuilder().setTxId(txId).setState(TransactionState.TS_OPEN))
          .build();
    }

    @Override
    public GetTransactionResponse getTransaction(String txId) {
      return GetTransactionResponse.newBuilder()
          .setTransaction(Transaction.newBuilder().setTxId(txId).setState(states.get(txId)))
          .build();
    }

    @Override
    public PrepareTransactionResponse prepareTransaction(
        String txId, String idempotencyKey, List<TxChange> changes) {
      states.put(txId, TransactionState.TS_PREPARED);
      bothPrepared.countDown();
      return PrepareTransactionResponse.newBuilder()
          .setTransaction(
              Transaction.newBuilder().setTxId(txId).setState(TransactionState.TS_PREPARED))
          .build();
    }

    @Override
    public CommitTransactionResponse commitTransaction(String txId, String idempotencyKey) {
      try {
        if (!bothPrepared.await(5, TimeUnit.SECONDS)) {
          throw new AssertionError("Timed out waiting for concurrent prepare");
        }
      } catch (InterruptedException e) {
        throw new AssertionError(e);
      }
      if (tableVersion == 1) {
        tableVersion = 2;
        states.put(txId, TransactionState.TS_APPLIED);
        return CommitTransactionResponse.newBuilder()
            .setTransaction(
                Transaction.newBuilder().setTxId(txId).setState(TransactionState.TS_APPLIED))
            .build();
      }
      states.put(txId, TransactionState.TS_APPLY_FAILED_CONFLICT);
      return CommitTransactionResponse.newBuilder()
          .setTransaction(
              Transaction.newBuilder()
                  .setTxId(txId)
                  .setState(TransactionState.TS_APPLY_FAILED_CONFLICT))
          .build();
    }

    @Override
    public AbortTransactionResponse abortTransaction(String txId, String reason) {
      states.put(txId, TransactionState.TS_ABORTED);
      return AbortTransactionResponse.newBuilder()
          .setTransaction(
              Transaction.newBuilder().setTxId(txId).setState(TransactionState.TS_ABORTED))
          .build();
    }

    @Override
    public ResolveCatalogResponse resolveCatalog(String prefix) {
      return ResolveCatalogResponse.newBuilder()
          .setResourceId(ResourceId.newBuilder().setAccountId("acct-1").setId("cat-1"))
          .build();
    }

    @Override
    public ResolveNamespaceResponse resolveNamespace(String prefix, List<String> namespacePath) {
      return ResolveNamespaceResponse.newBuilder()
          .setResourceId(ResourceId.newBuilder().setAccountId("acct-1").setId("ns-1"))
          .build();
    }

    @Override
    public ResolveTableResponse resolveTable(
        String prefix, List<String> namespacePath, String tableName) {
      return ResolveTableResponse.newBuilder().setResourceId(currentTable.getResourceId()).build();
    }

    @Override
    public GetTableResponse getTable(ResourceId tableId) {
      return GetTableResponse.newBuilder()
          .setTable(currentTable)
          .setMeta(MutationMeta.newBuilder().setPointerVersion(1))
          .build();
    }

    @Override
    public ListSnapshotsResponse listSnapshots(ResourceId tableId) {
      return ListSnapshotsResponse.getDefaultInstance();
    }
  }
}
