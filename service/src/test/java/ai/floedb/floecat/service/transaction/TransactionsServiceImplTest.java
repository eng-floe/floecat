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

package ai.floedb.floecat.service.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.Precondition;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.service.metagraph.overlay.user.UserGraph;
import ai.floedb.floecat.service.metagraph.resolver.NameResolver;
import ai.floedb.floecat.service.repo.impl.TransactionIntentRepository;
import ai.floedb.floecat.service.repo.impl.TransactionRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.transaction.impl.TransactionIntentApplierSupport;
import ai.floedb.floecat.service.transaction.impl.TransactionsServiceImpl;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.transaction.rpc.CommitTransactionRequest;
import ai.floedb.floecat.transaction.rpc.ConnectorProvisioning;
import ai.floedb.floecat.transaction.rpc.PrepareTransactionRequest;
import ai.floedb.floecat.transaction.rpc.Transaction;
import ai.floedb.floecat.transaction.rpc.TransactionIntent;
import ai.floedb.floecat.transaction.rpc.TransactionState;
import ai.floedb.floecat.transaction.rpc.TxChange;
import com.google.protobuf.util.Timestamps;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

class TransactionsServiceImplTest {

  @Test
  void commitAppliedCleansIntentsAfterStateUpdate() throws Exception {
    var service = new TransactionsServiceImpl();
    var txRepo = Mockito.mock(TransactionRepository.class);
    var intentRepo = Mockito.mock(TransactionIntentRepository.class);
    var applier = Mockito.mock(TransactionIntentApplierSupport.class);

    inject(service, "txRepo", txRepo);
    inject(service, "intentRepo", intentRepo);
    inject(service, "intentApplierSupport", applier);

    Transaction txn =
        Transaction.newBuilder()
            .setAccountId("acct")
            .setTxId("tx-1")
            .setState(TransactionState.TS_PREPARED)
            .setUpdatedAt(Timestamps.fromMillis(1))
            .build();
    Transaction txnApplying = txn.toBuilder().setState(TransactionState.TS_APPLYING).build();
    TransactionIntent intent =
        TransactionIntent.newBuilder()
            .setAccountId("acct")
            .setTxId("tx-1")
            .setTargetPointerKey("/accounts/acct/custom/key-1")
            .setBlobUri("s3://bucket/blob-1")
            .setCreatedAt(Timestamps.fromMillis(1))
            .build();

    when(txRepo.getById("acct", "tx-1"))
        .thenReturn(Optional.of(txn), Optional.of(txn), Optional.of(txnApplying));
    when(intentRepo.listByTx("acct", "tx-1")).thenReturn(List.of(intent));
    when(intentRepo.getByTarget("acct", "/accounts/acct/custom/key-1"))
        .thenReturn(Optional.of(intent));
    when(applier.applyTransactionBestEffort(List.of(intent), intentRepo))
        .thenReturn(TransactionIntentApplierSupport.ApplyOutcome.applied());
    when(txRepo.metaFor("acct", "tx-1"))
        .thenReturn(
            MutationMeta.newBuilder().setPointerVersion(11L).build(),
            MutationMeta.newBuilder().setPointerVersion(12L).build());
    when(txRepo.update(
            argThat(
                updated -> updated != null && updated.getState() == TransactionState.TS_APPLYING),
            anyLong()))
        .thenReturn(true);
    when(txRepo.update(
            argThat(
                updated -> updated != null && updated.getState() == TransactionState.TS_APPLIED),
            anyLong()))
        .thenReturn(true);
    when(intentRepo.deleteBothIndicesBestEffort(intent)).thenReturn(true);

    Transaction committed =
        invokeCommitPrivate(
            service,
            "acct",
            CommitTransactionRequest.newBuilder().setTxId("tx-1").build(),
            Timestamps.fromMillis(10));

    assertEquals(TransactionState.TS_APPLIED, committed.getState());
    InOrder ordered = inOrder(txRepo, intentRepo);
    ordered
        .verify(txRepo)
        .update(
            argThat(
                updated -> updated != null && updated.getState() == TransactionState.TS_APPLIED),
            anyLong());
    ordered.verify(intentRepo).deleteBothIndicesBestEffort(intent);
  }

  @Test
  void commitRetryableDoesNotCleanupIntents() throws Exception {
    var service = new TransactionsServiceImpl();
    var txRepo = Mockito.mock(TransactionRepository.class);
    var intentRepo = Mockito.mock(TransactionIntentRepository.class);
    var applier = Mockito.mock(TransactionIntentApplierSupport.class);

    inject(service, "txRepo", txRepo);
    inject(service, "intentRepo", intentRepo);
    inject(service, "intentApplierSupport", applier);

    Transaction txn =
        Transaction.newBuilder()
            .setAccountId("acct")
            .setTxId("tx-1")
            .setState(TransactionState.TS_PREPARED)
            .setUpdatedAt(Timestamps.fromMillis(1))
            .build();
    Transaction txnApplying = txn.toBuilder().setState(TransactionState.TS_APPLYING).build();
    TransactionIntent intent =
        TransactionIntent.newBuilder()
            .setAccountId("acct")
            .setTxId("tx-1")
            .setTargetPointerKey("/accounts/acct/custom/key-1")
            .setBlobUri("s3://bucket/blob-1")
            .setCreatedAt(Timestamps.fromMillis(1))
            .build();

    when(txRepo.getById("acct", "tx-1"))
        .thenReturn(Optional.of(txn), Optional.of(txn), Optional.of(txnApplying));
    when(intentRepo.listByTx("acct", "tx-1")).thenReturn(List.of(intent));
    when(intentRepo.getByTarget("acct", "/accounts/acct/custom/key-1"))
        .thenReturn(Optional.of(intent));
    when(applier.applyTransactionBestEffort(List.of(intent), intentRepo))
        .thenReturn(TransactionIntentApplierSupport.ApplyOutcome.retryable("X", "retry"));
    when(intentRepo.update(
            argThat(
                candidate ->
                    candidate != null
                        && candidate.getTxId().equals(intent.getTxId())
                        && candidate.getTargetPointerKey().equals(intent.getTargetPointerKey())
                        && candidate.getApplyErrorCode().equals("X")
                        && candidate.getApplyErrorMessage().equals("retry"))))
        .thenReturn(true);
    when(txRepo.metaFor("acct", "tx-1"))
        .thenReturn(
            MutationMeta.newBuilder().setPointerVersion(12L).build(),
            MutationMeta.newBuilder().setPointerVersion(13L).build());
    when(txRepo.update(
            argThat(
                updated -> updated != null && updated.getState() == TransactionState.TS_APPLYING),
            anyLong()))
        .thenReturn(true);
    when(txRepo.update(
            argThat(
                updated ->
                    updated != null
                        && updated.getState() == TransactionState.TS_APPLY_FAILED_RETRYABLE),
            anyLong()))
        .thenReturn(true);

    Transaction committed =
        invokeCommitPrivate(
            service,
            "acct",
            CommitTransactionRequest.newBuilder().setTxId("tx-1").build(),
            Timestamps.fromMillis(10));

    assertEquals(TransactionState.TS_APPLY_FAILED_RETRYABLE, committed.getState());
    verify(intentRepo, never()).deleteBothIndicesBestEffort(intent);
  }

  @Test
  void commitFromRetryableCanTransitionToApplied() throws Exception {
    var service = new TransactionsServiceImpl();
    var txRepo = Mockito.mock(TransactionRepository.class);
    var intentRepo = Mockito.mock(TransactionIntentRepository.class);
    var applier = Mockito.mock(TransactionIntentApplierSupport.class);

    inject(service, "txRepo", txRepo);
    inject(service, "intentRepo", intentRepo);
    inject(service, "intentApplierSupport", applier);

    Transaction txn =
        Transaction.newBuilder()
            .setAccountId("acct")
            .setTxId("tx-1")
            .setState(TransactionState.TS_APPLY_FAILED_RETRYABLE)
            .setUpdatedAt(Timestamps.fromMillis(1))
            .build();
    Transaction txnApplying = txn.toBuilder().setState(TransactionState.TS_APPLYING).build();
    TransactionIntent intent =
        TransactionIntent.newBuilder()
            .setAccountId("acct")
            .setTxId("tx-1")
            .setTargetPointerKey("/accounts/acct/custom/key-1")
            .setBlobUri("s3://bucket/blob-1")
            .setCreatedAt(Timestamps.fromMillis(1))
            .build();

    when(txRepo.getById("acct", "tx-1"))
        .thenReturn(Optional.of(txn), Optional.of(txn), Optional.of(txnApplying));
    when(intentRepo.listByTx("acct", "tx-1")).thenReturn(List.of(intent));
    when(intentRepo.getByTarget("acct", "/accounts/acct/custom/key-1"))
        .thenReturn(Optional.of(intent));
    when(applier.applyTransactionBestEffort(List.of(intent), intentRepo))
        .thenReturn(TransactionIntentApplierSupport.ApplyOutcome.applied());
    when(txRepo.metaFor("acct", "tx-1"))
        .thenReturn(
            MutationMeta.newBuilder().setPointerVersion(13L).build(),
            MutationMeta.newBuilder().setPointerVersion(14L).build());
    when(txRepo.update(
            argThat(
                updated -> updated != null && updated.getState() == TransactionState.TS_APPLYING),
            anyLong()))
        .thenReturn(true);
    when(txRepo.update(
            argThat(
                updated -> updated != null && updated.getState() == TransactionState.TS_APPLIED),
            anyLong()))
        .thenReturn(true);
    when(intentRepo.deleteBothIndicesBestEffort(intent)).thenReturn(true);

    Transaction committed =
        invokeCommitPrivate(
            service,
            "acct",
            CommitTransactionRequest.newBuilder().setTxId("tx-1").build(),
            Timestamps.fromMillis(10));

    assertEquals(TransactionState.TS_APPLIED, committed.getState());
    verify(intentRepo).deleteBothIndicesBestEffort(intent);
  }

  @Test
  void commitConflictAnnotatesIntentAndTransitionsToConflictState() throws Exception {
    var service = new TransactionsServiceImpl();
    var txRepo = Mockito.mock(TransactionRepository.class);
    var intentRepo = Mockito.mock(TransactionIntentRepository.class);
    var applier = Mockito.mock(TransactionIntentApplierSupport.class);
    var pointerStore = Mockito.mock(ai.floedb.floecat.storage.spi.PointerStore.class);

    inject(service, "txRepo", txRepo);
    inject(service, "intentRepo", intentRepo);
    inject(service, "intentApplierSupport", applier);
    inject(service, "pointerStore", pointerStore);

    Transaction txn =
        Transaction.newBuilder()
            .setAccountId("acct")
            .setTxId("tx-1")
            .setState(TransactionState.TS_PREPARED)
            .setUpdatedAt(Timestamps.fromMillis(1))
            .build();
    Transaction txnApplying = txn.toBuilder().setState(TransactionState.TS_APPLYING).build();
    TransactionIntent intent =
        TransactionIntent.newBuilder()
            .setAccountId("acct")
            .setTxId("tx-1")
            .setTargetPointerKey("/accounts/acct/custom/key-1")
            .setBlobUri("s3://bucket/blob-1")
            .setCreatedAt(Timestamps.fromMillis(1))
            .build();

    when(txRepo.getById("acct", "tx-1"))
        .thenReturn(Optional.of(txn), Optional.of(txn), Optional.of(txnApplying));
    when(intentRepo.listByTx("acct", "tx-1")).thenReturn(List.of(intent));
    when(intentRepo.getByTarget("acct", "/accounts/acct/custom/key-1"))
        .thenReturn(Optional.of(intent));
    when(applier.applyTransactionBestEffort(List.of(intent), intentRepo))
        .thenReturn(
            TransactionIntentApplierSupport.ApplyOutcome.conflict(
                "EXPECTED_VERSION_MISMATCH", "pointer version changed", 3L, 4L, null));
    when(pointerStore.get("/accounts/acct/custom/key-1")).thenReturn(Optional.empty());
    when(intentRepo.update(
            argThat(
                candidate ->
                    candidate.getApplyErrorCode().equals("EXPECTED_VERSION_MISMATCH")
                        && candidate.getApplyErrorMessage().equals("pointer version changed")
                        && candidate.getApplyErrorExpectedVersion() == 3L
                        && candidate.getApplyErrorActualVersion() == 4L
                        && candidate.hasApplyErrorAt())))
        .thenReturn(true);
    when(txRepo.metaFor("acct", "tx-1"))
        .thenReturn(
            MutationMeta.newBuilder().setPointerVersion(14L).build(),
            MutationMeta.newBuilder().setPointerVersion(15L).build());
    when(txRepo.update(
            argThat(
                updated -> updated != null && updated.getState() == TransactionState.TS_APPLYING),
            anyLong()))
        .thenReturn(true);
    when(txRepo.update(
            argThat(
                updated ->
                    updated != null
                        && updated.getState() == TransactionState.TS_APPLY_FAILED_CONFLICT),
            anyLong()))
        .thenReturn(true);

    Transaction committed =
        invokeCommitPrivate(
            service,
            "acct",
            CommitTransactionRequest.newBuilder().setTxId("tx-1").build(),
            Timestamps.fromMillis(10));

    assertEquals(TransactionState.TS_APPLY_FAILED_CONFLICT, committed.getState());
    verify(intentRepo, never()).deleteBothIndicesBestEffort(intent);
    verify(intentRepo)
        .update(
            argThat(
                candidate ->
                    candidate.getTxId().equals(intent.getTxId())
                        && candidate.getTargetPointerKey().equals(intent.getTargetPointerKey())
                        && candidate.hasApplyErrorAt()));
    assertTrue(committed.hasUpdatedAt());
  }

  @Test
  void commitApplyingConflictFinalizesAppliedWhenIntentsAlreadyApplied() throws Exception {
    var service = new TransactionsServiceImpl();
    var txRepo = Mockito.mock(TransactionRepository.class);
    var intentRepo = Mockito.mock(TransactionIntentRepository.class);
    var applier = Mockito.mock(TransactionIntentApplierSupport.class);
    var pointerStore = Mockito.mock(ai.floedb.floecat.storage.spi.PointerStore.class);

    inject(service, "txRepo", txRepo);
    inject(service, "intentRepo", intentRepo);
    inject(service, "intentApplierSupport", applier);
    inject(service, "pointerStore", pointerStore);

    Transaction txn =
        Transaction.newBuilder()
            .setAccountId("acct")
            .setTxId("tx-1")
            .setState(TransactionState.TS_APPLYING)
            .setUpdatedAt(Timestamps.fromMillis(1))
            .build();
    TransactionIntent intent =
        TransactionIntent.newBuilder()
            .setAccountId("acct")
            .setTxId("tx-1")
            .setTargetPointerKey("/accounts/acct/custom/key-1")
            .setBlobUri("s3://bucket/blob-1")
            .setCreatedAt(Timestamps.fromMillis(1))
            .build();

    when(txRepo.getById("acct", "tx-1")).thenReturn(Optional.of(txn), Optional.of(txn));
    when(intentRepo.listByTx("acct", "tx-1")).thenReturn(List.of(intent));
    when(intentRepo.getByTarget("acct", "/accounts/acct/custom/key-1"))
        .thenReturn(Optional.of(intent));
    when(applier.applyTransactionBestEffort(List.of(intent), intentRepo))
        .thenReturn(
            TransactionIntentApplierSupport.ApplyOutcome.conflict(
                "EXPECTED_VERSION_MISMATCH", "pointer version changed", 1L, 2L, null));
    when(pointerStore.get("/accounts/acct/custom/key-1"))
        .thenReturn(
            Optional.of(
                Pointer.newBuilder()
                    .setKey("/accounts/acct/custom/key-1")
                    .setBlobUri("s3://bucket/blob-1")
                    .setVersion(2L)
                    .build()));
    when(txRepo.metaFor("acct", "tx-1"))
        .thenReturn(MutationMeta.newBuilder().setPointerVersion(21L).build());
    when(txRepo.update(
            argThat(
                updated -> updated != null && updated.getState() == TransactionState.TS_APPLIED),
            anyLong()))
        .thenReturn(true);
    when(intentRepo.deleteBothIndicesBestEffort(intent)).thenReturn(true);

    Transaction committed =
        invokeCommitPrivate(
            service,
            "acct",
            CommitTransactionRequest.newBuilder().setTxId("tx-1").build(),
            Timestamps.fromMillis(10));

    assertEquals(TransactionState.TS_APPLIED, committed.getState());
    verify(intentRepo).deleteBothIndicesBestEffort(intent);
    verify(intentRepo, never())
        .update(
            argThat(
                candidate ->
                    candidate.getTxId().equals(intent.getTxId())
                        && candidate.getTargetPointerKey().equals(intent.getTargetPointerKey())
                        && candidate.hasApplyErrorAt()));
  }

  @Test
  void commitApplyingIgnoresExpiryAndFinalizesApplied() throws Exception {
    var service = new TransactionsServiceImpl();
    var txRepo = Mockito.mock(TransactionRepository.class);
    var intentRepo = Mockito.mock(TransactionIntentRepository.class);
    var applier = Mockito.mock(TransactionIntentApplierSupport.class);

    inject(service, "txRepo", txRepo);
    inject(service, "intentRepo", intentRepo);
    inject(service, "intentApplierSupport", applier);

    Transaction txn =
        Transaction.newBuilder()
            .setAccountId("acct")
            .setTxId("tx-1")
            .setState(TransactionState.TS_APPLYING)
            .setCreatedAt(Timestamps.fromMillis(1))
            .setUpdatedAt(Timestamps.fromMillis(1))
            .setExpiresAt(Timestamps.fromMillis(2))
            .build();
    TransactionIntent intent =
        TransactionIntent.newBuilder()
            .setAccountId("acct")
            .setTxId("tx-1")
            .setTargetPointerKey("/accounts/acct/custom/key-1")
            .setBlobUri("s3://bucket/blob-1")
            .setCreatedAt(Timestamps.fromMillis(1))
            .build();

    when(txRepo.getById("acct", "tx-1")).thenReturn(Optional.of(txn), Optional.of(txn));
    when(intentRepo.listByTx("acct", "tx-1")).thenReturn(List.of(intent));
    when(intentRepo.getByTarget("acct", "/accounts/acct/custom/key-1"))
        .thenReturn(Optional.of(intent));
    when(applier.applyTransactionBestEffort(List.of(intent), intentRepo))
        .thenReturn(TransactionIntentApplierSupport.ApplyOutcome.applied());
    when(txRepo.metaFor("acct", "tx-1"))
        .thenReturn(MutationMeta.newBuilder().setPointerVersion(31L).build());
    when(txRepo.update(
            argThat(
                updated -> updated != null && updated.getState() == TransactionState.TS_APPLIED),
            anyLong()))
        .thenReturn(true);
    when(intentRepo.deleteBothIndicesBestEffort(intent)).thenReturn(true);

    Transaction committed =
        invokeCommitPrivate(
            service,
            "acct",
            CommitTransactionRequest.newBuilder().setTxId("tx-1").build(),
            Timestamps.fromMillis(10));

    assertEquals(TransactionState.TS_APPLIED, committed.getState());
    verify(intentRepo).deleteBothIndicesBestEffort(intent);
  }

  @Test
  void commitAppliedInvalidatesTouchedTableGraphEntry() throws Exception {
    var service = new TransactionsServiceImpl();
    var txRepo = Mockito.mock(TransactionRepository.class);
    var intentRepo = Mockito.mock(TransactionIntentRepository.class);
    var applier = Mockito.mock(TransactionIntentApplierSupport.class);
    var metadataGraph = Mockito.mock(UserGraph.class);

    inject(service, "txRepo", txRepo);
    inject(service, "intentRepo", intentRepo);
    inject(service, "intentApplierSupport", applier);
    inject(service, "metadataGraph", metadataGraph);

    String accountId = "acct";
    String txId = "tx-1";
    String tableId = "table-1";
    Transaction txn =
        Transaction.newBuilder()
            .setAccountId(accountId)
            .setTxId(txId)
            .setState(TransactionState.TS_PREPARED)
            .setUpdatedAt(Timestamps.fromMillis(1))
            .build();
    Transaction txnApplying = txn.toBuilder().setState(TransactionState.TS_APPLYING).build();
    TransactionIntent intent =
        TransactionIntent.newBuilder()
            .setAccountId(accountId)
            .setTxId(txId)
            .setTargetPointerKey(Keys.tablePointerById(accountId, tableId))
            .setBlobUri("s3://bucket/blob-1")
            .setCreatedAt(Timestamps.fromMillis(1))
            .build();

    when(txRepo.getById(accountId, txId))
        .thenReturn(Optional.of(txn), Optional.of(txn), Optional.of(txnApplying));
    when(intentRepo.listByTx(accountId, txId)).thenReturn(List.of(intent));
    when(intentRepo.getByTarget(accountId, intent.getTargetPointerKey()))
        .thenReturn(Optional.of(intent));
    when(applier.applyTransactionBestEffort(List.of(intent), intentRepo))
        .thenReturn(TransactionIntentApplierSupport.ApplyOutcome.applied());
    when(txRepo.metaFor(accountId, txId))
        .thenReturn(
            MutationMeta.newBuilder().setPointerVersion(11L).build(),
            MutationMeta.newBuilder().setPointerVersion(12L).build());
    when(txRepo.update(
            argThat(
                updated -> updated != null && updated.getState() == TransactionState.TS_APPLYING),
            anyLong()))
        .thenReturn(true);
    when(txRepo.update(
            argThat(
                updated -> updated != null && updated.getState() == TransactionState.TS_APPLIED),
            anyLong()))
        .thenReturn(true);
    when(intentRepo.deleteBothIndicesBestEffort(intent)).thenReturn(true);

    Transaction committed =
        invokeCommitPrivate(
            service,
            accountId,
            CommitTransactionRequest.newBuilder().setTxId(txId).build(),
            Timestamps.fromMillis(10));

    assertEquals(TransactionState.TS_APPLIED, committed.getState());
    verify(metadataGraph)
        .invalidate(
            argThat(
                id ->
                    id != null
                        && accountId.equals(id.getAccountId())
                        && tableId.equals(id.getId())
                        && id.getKind() == ResourceKind.RK_TABLE));
  }

  @Test
  void prepareWithoutExpectedVersionPreconditionCapturesCurrentPointerVersion() throws Exception {
    var service = new TransactionsServiceImpl();
    var txRepo = Mockito.mock(TransactionRepository.class);
    var intentRepo = Mockito.mock(TransactionIntentRepository.class);
    var pointerStore = Mockito.mock(ai.floedb.floecat.storage.spi.PointerStore.class);
    var blobStore = Mockito.mock(ai.floedb.floecat.storage.spi.BlobStore.class);
    var resolver = Mockito.mock(NameResolver.class);

    inject(service, "txRepo", txRepo);
    inject(service, "intentRepo", intentRepo);
    inject(service, "pointerStore", pointerStore);
    inject(service, "blobStore", blobStore);
    inject(service, "nameResolver", resolver);

    String accountId = "acct";
    String txId = "tx-1";
    String tableId = "table-1";
    String pointerKey = Keys.tablePointerById(accountId, tableId);
    Transaction txn =
        Transaction.newBuilder()
            .setAccountId(accountId)
            .setTxId(txId)
            .setState(TransactionState.TS_OPEN)
            .setUpdatedAt(Timestamps.fromMillis(1))
            .build();

    when(txRepo.getById(accountId, txId)).thenReturn(Optional.of(txn));
    when(pointerStore.get(pointerKey))
        .thenReturn(Optional.of(Pointer.newBuilder().setKey(pointerKey).setVersion(7L).build()));
    when(intentRepo.getByTarget(accountId, pointerKey)).thenReturn(Optional.empty());
    when(txRepo.metaFor(accountId, txId))
        .thenReturn(MutationMeta.newBuilder().setPointerVersion(1L).build());
    when(txRepo.update(
            argThat(
                updated -> updated != null && updated.getState() == TransactionState.TS_PREPARED),
            anyLong()))
        .thenReturn(true);

    var request =
        PrepareTransactionRequest.newBuilder()
            .setTxId(txId)
            .addChanges(
                TxChange.newBuilder()
                    .setTableId(
                        ResourceId.newBuilder()
                            .setAccountId(accountId)
                            .setId(tableId)
                            .setKind(ResourceKind.RK_TABLE))
                    .setIntendedBlobUri(Keys.accountRootPrefix(accountId) + "/tables/intended-1"))
            .build();

    Transaction prepared =
        invokePreparePrivate(service, accountId, request, Timestamps.fromMillis(10));

    assertEquals(TransactionState.TS_PREPARED, prepared.getState());
    verify(intentRepo)
        .create(
            argThat(
                intent ->
                    intent.getTxId().equals(txId)
                        && intent.getTargetPointerKey().equals(pointerKey)
                        && intent.hasExpectedVersion()
                        && intent.getExpectedVersion() == 7L));
  }

  @Test
  void prepareAlreadyPreparedMatchesWithoutRecheckingLivePointerVersion() throws Exception {
    var service = new TransactionsServiceImpl();
    var txRepo = Mockito.mock(TransactionRepository.class);
    var intentRepo = Mockito.mock(TransactionIntentRepository.class);
    var pointerStore = Mockito.mock(ai.floedb.floecat.storage.spi.PointerStore.class);

    inject(service, "txRepo", txRepo);
    inject(service, "intentRepo", intentRepo);
    inject(service, "pointerStore", pointerStore);

    String accountId = "acct";
    String txId = "tx-1";
    String target = "/accounts/acct/custom/key-1";
    String blobUri = "/accounts/acct/objects/staged-1";
    Transaction txn =
        Transaction.newBuilder()
            .setAccountId(accountId)
            .setTxId(txId)
            .setState(TransactionState.TS_PREPARED)
            .setCreatedAt(Timestamps.fromMillis(1))
            .setUpdatedAt(Timestamps.fromMillis(1))
            .setExpiresAt(Timestamps.fromMillis(60_000))
            .build();
    TransactionIntent storedIntent =
        TransactionIntent.newBuilder()
            .setAccountId(accountId)
            .setTxId(txId)
            .setTargetPointerKey(target)
            .setBlobUri(blobUri)
            .setExpectedVersion(7L)
            .setCreatedAt(Timestamps.fromMillis(1))
            .build();

    when(txRepo.getById(accountId, txId)).thenReturn(Optional.of(txn));
    when(intentRepo.listByTx(accountId, txId)).thenReturn(List.of(storedIntent));
    // Live pointer version drift should not matter for already-prepared replay matching.
    when(pointerStore.get(target))
        .thenReturn(Optional.of(Pointer.newBuilder().setKey(target).setVersion(999L).build()));

    var request =
        PrepareTransactionRequest.newBuilder()
            .setTxId(txId)
            .addChanges(
                TxChange.newBuilder()
                    .setTargetPointerKey(target)
                    .setIntendedBlobUri(blobUri)
                    .setPrecondition(Precondition.newBuilder().setExpectedVersion(7L)))
            .build();

    Transaction prepared =
        invokePreparePrivate(service, accountId, request, Timestamps.fromMillis(10));

    assertEquals(TransactionState.TS_PREPARED, prepared.getState());
    verify(txRepo, never()).update(any(), anyLong());
  }

  @Test
  void prepareTableDeleteIntentStoresExpectedOwnedNamePointerKey() throws Exception {
    var service = new TransactionsServiceImpl();
    var txRepo = Mockito.mock(TransactionRepository.class);
    var intentRepo = Mockito.mock(TransactionIntentRepository.class);
    var pointerStore = Mockito.mock(ai.floedb.floecat.storage.spi.PointerStore.class);
    var blobStore = Mockito.mock(BlobStore.class);

    inject(service, "txRepo", txRepo);
    inject(service, "intentRepo", intentRepo);
    inject(service, "pointerStore", pointerStore);
    inject(service, "blobStore", blobStore);

    String accountId = "acct";
    String txId = "tx-1";
    String tableId = "table-1";
    String targetKey = Keys.tablePointerById(accountId, tableId);
    String tableBlobUri = Keys.tableBlobUri(accountId, tableId, "sha");
    Transaction txn =
        Transaction.newBuilder()
            .setAccountId(accountId)
            .setTxId(txId)
            .setState(TransactionState.TS_OPEN)
            .setUpdatedAt(Timestamps.fromMillis(1))
            .build();
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setAccountId(accountId).setId(tableId))
            .setCatalogId(ResourceId.newBuilder().setAccountId(accountId).setId("cat-1"))
            .setNamespaceId(ResourceId.newBuilder().setAccountId(accountId).setId("ns-1"))
            .setDisplayName("orders")
            .build();

    when(txRepo.getById(accountId, txId)).thenReturn(Optional.of(txn));
    when(pointerStore.get(targetKey))
        .thenReturn(
            Optional.of(
                Pointer.newBuilder()
                    .setKey(targetKey)
                    .setBlobUri(tableBlobUri)
                    .setVersion(7L)
                    .build()));
    when(blobStore.get(tableBlobUri)).thenReturn(table.toByteArray());
    when(intentRepo.getByTarget(accountId, targetKey)).thenReturn(Optional.empty());
    when(txRepo.metaFor(accountId, txId))
        .thenReturn(MutationMeta.newBuilder().setPointerVersion(1L).build());
    when(txRepo.update(
            argThat(
                updated -> updated != null && updated.getState() == TransactionState.TS_PREPARED),
            anyLong()))
        .thenReturn(true);

    var request =
        PrepareTransactionRequest.newBuilder()
            .setTxId(txId)
            .addChanges(
                TxChange.newBuilder()
                    .setTableId(
                        ResourceId.newBuilder()
                            .setAccountId(accountId)
                            .setId(tableId)
                            .setKind(ResourceKind.RK_TABLE))
                    .setIntendedBlobUri(
                        Keys.transactionDeleteSentinelUri(accountId, txId, targetKey)))
            .build();

    Transaction prepared =
        invokePreparePrivate(service, accountId, request, Timestamps.fromMillis(10));

    assertEquals(TransactionState.TS_PREPARED, prepared.getState());
    verify(intentRepo)
        .create(
            argThat(
                intent ->
                    intent.getTargetPointerKey().equals(targetKey)
                        && intent
                            .getBlobUri()
                            .equals(Keys.transactionDeleteSentinelUri(accountId, txId, targetKey))
                        && intent
                            .getExpectedOwnedNamePointerKey()
                            .equals(
                                Keys.tablePointerByName(accountId, "cat-1", "ns-1", "orders"))));
  }

  @Test
  void intentsAlreadyAppliedTreatsTableDeleteSentinelAsAppliedWhenPointersAreCleared()
      throws Exception {
    var service = new TransactionsServiceImpl();
    var pointerStore = Mockito.mock(ai.floedb.floecat.storage.spi.PointerStore.class);

    inject(service, "pointerStore", pointerStore);

    String accountId = "acct";
    String txId = "tx-1";
    String tableId = "table-1";
    String targetKey = Keys.tablePointerById(accountId, tableId);
    String nameKey = Keys.tablePointerByName(accountId, "cat-1", "ns-1", "orders");
    TransactionIntent intent =
        TransactionIntent.newBuilder()
            .setAccountId(accountId)
            .setTxId(txId)
            .setTargetPointerKey(targetKey)
            .setBlobUri(Keys.transactionDeleteSentinelUri(accountId, txId, targetKey))
            .setExpectedOwnedNamePointerKey(nameKey)
            .setCreatedAt(Timestamps.fromMillis(1))
            .build();

    when(pointerStore.get(targetKey)).thenReturn(Optional.empty());
    when(pointerStore.get(nameKey)).thenReturn(Optional.empty());

    Method m = TransactionsServiceImpl.class.getDeclaredMethod("intentsAlreadyApplied", List.class);
    m.setAccessible(true);

    assertTrue((Boolean) m.invoke(service, List.of(intent)));
  }

  @Test
  void abortExpiredLeavesConflictStateUnchanged() throws Exception {
    var service = new TransactionsServiceImpl();
    var txRepo = Mockito.mock(TransactionRepository.class);
    var intentRepo = Mockito.mock(TransactionIntentRepository.class);

    inject(service, "txRepo", txRepo);
    inject(service, "intentRepo", intentRepo);

    String accountId = "acct";
    String txId = "tx-1";
    Transaction txn =
        Transaction.newBuilder()
            .setAccountId(accountId)
            .setTxId(txId)
            .setState(TransactionState.TS_APPLY_FAILED_CONFLICT)
            .setUpdatedAt(Timestamps.fromMillis(1))
            .build();

    when(txRepo.getById(accountId, txId)).thenReturn(Optional.of(txn));
    var aborted = invokeAbortExpired(service, txn, Timestamps.fromMillis(3));
    assertEquals(TransactionState.TS_APPLY_FAILED_CONFLICT, aborted.getState());
    verify(txRepo, never()).metaFor(accountId, txId);
    verify(txRepo, never())
        .update(
            argThat(
                updated -> updated != null && updated.getState() == TransactionState.TS_ABORTED),
            anyLong());
  }

  @Test
  void retryableStateIsNotCommittedFamily() throws Exception {
    var service = new TransactionsServiceImpl();
    Method m =
        TransactionsServiceImpl.class.getDeclaredMethod(
            "isTerminalNonAbortableState", TransactionState.class);
    m.setAccessible(true);

    assertFalse((Boolean) m.invoke(service, TransactionState.TS_APPLY_FAILED_RETRYABLE));
    assertTrue((Boolean) m.invoke(service, TransactionState.TS_APPLIED));
  }

  @Test
  void buildProvisionedConnectorMaterializesGatewayProvisioningHint() throws Exception {
    var service = new TransactionsServiceImpl();

    Table table =
        Table.newBuilder()
            .setResourceId(resourceId("tbl-1", ResourceKind.RK_TABLE))
            .setCatalogId(resourceId("cat-1", ResourceKind.RK_CATALOG))
            .setNamespaceId(resourceId("ns-1", ResourceKind.RK_NAMESPACE))
            .setDisplayName("orders")
            .setUpstream(
                UpstreamRef.newBuilder()
                    .addNamespacePath("db")
                    .setTableDisplayName("orders")
                    .setUri("s3://warehouse/db/orders")
                    .build())
            .putProperties("s3.region", "us-east-1")
            .build();

    Connector connector =
        invokeBuildProvisionedConnector(
            service,
            "acct",
            "tx-1",
            table,
            ConnectorProvisioning.newBuilder()
                .setConnectorUri("s3://warehouse/db/orders")
                .addSourceNamespacePath("db")
                .setSourceTableName("orders")
                .setDisplayName("register:pref:db.orders")
                .setDescription("Filesystem connector")
                .putProperties("iceberg.source", "filesystem")
                .putProperties("s3.region", "us-east-1")
                .putProperties("floecat.connector.mode", "capture-only")
                .build());

    assertEquals(ConnectorKind.CK_ICEBERG, connector.getKind());
    assertEquals("s3://warehouse/db/orders", connector.getUri());
    assertEquals("register:pref:db.orders", connector.getDisplayName());
    assertEquals("Filesystem connector", connector.getDescription());
    assertEquals(List.of("db"), connector.getSource().getNamespace().getSegmentsList());
    assertEquals("orders", connector.getSource().getTable());
    assertEquals("filesystem", connector.getPropertiesOrThrow("iceberg.source"));
    assertEquals("us-east-1", connector.getPropertiesOrThrow("s3.region"));
    assertNotNull(connector.getResourceId());
  }

  @Test
  void buildProvisionedConnectorRequiresConnectorUri() throws Exception {
    var service = new TransactionsServiceImpl();

    Table table =
        Table.newBuilder()
            .setResourceId(resourceId("tbl-1", ResourceKind.RK_TABLE))
            .setCatalogId(resourceId("cat-1", ResourceKind.RK_CATALOG))
            .setNamespaceId(resourceId("ns-1", ResourceKind.RK_NAMESPACE))
            .setDisplayName("orders")
            .setUpstream(
                UpstreamRef.newBuilder()
                    .addNamespacePath("db")
                    .setTableDisplayName("orders")
                    .setUri("s3://warehouse/db/orders")
                    .build())
            .build();

    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                invokeBuildProvisionedConnector(
                    service,
                    "acct",
                    "tx-1",
                    table,
                    ConnectorProvisioning.newBuilder()
                        .addSourceNamespacePath("db")
                        .setSourceTableName("orders")
                        .setDisplayName("register:pref:db.orders")
                        .putProperties("iceberg.source", "filesystem")
                        .build()));

    assertTrue(error.getMessage().contains("connector_uri"));
  }

  @Test
  void reserveTransactionTableIdPersistsAndReusesReservedId() throws Exception {
    var service = new TransactionsServiceImpl();
    var txRepo = Mockito.mock(TransactionRepository.class);

    inject(service, "txRepo", txRepo);

    String accountId = "acct";
    String txId = "tx-1";
    String tableFq = "cat.db.orders";
    Transaction openTxn =
        Transaction.newBuilder()
            .setAccountId(accountId)
            .setTxId(txId)
            .setState(TransactionState.TS_OPEN)
            .setUpdatedAt(Timestamps.fromMillis(1))
            .build();
    AtomicReference<Transaction> txnRef = new AtomicReference<>(openTxn);

    when(txRepo.getById(accountId, txId)).thenAnswer(invocation -> Optional.of(txnRef.get()));
    when(txRepo.metaFor(accountId, txId))
        .thenReturn(MutationMeta.newBuilder().setPointerVersion(7L).build());
    when(txRepo.update(any(Transaction.class), anyLong()))
        .thenAnswer(
            invocation -> {
              txnRef.set(invocation.getArgument(0, Transaction.class));
              return true;
            });

    ResourceId first =
        invokeReserveTransactionTableId(
            service, accountId, txId, tableFq, Timestamps.fromMillis(10));
    ResourceId second =
        invokeReserveTransactionTableId(
            service, accountId, txId, tableFq, Timestamps.fromMillis(11));

    assertFalse(first.getId().isBlank());
    assertEquals(ResourceKind.RK_TABLE, first.getKind());
    assertEquals(first, second);
    verify(txRepo).update(any(Transaction.class), anyLong());
  }

  @Test
  void reserveTransactionTableIdRejectsNonOpenTransactionWithoutReservation() throws Exception {
    var service = new TransactionsServiceImpl();
    var txRepo = Mockito.mock(TransactionRepository.class);

    inject(service, "txRepo", txRepo);

    String accountId = "acct";
    String txId = "tx-1";
    Transaction preparedTxn =
        Transaction.newBuilder()
            .setAccountId(accountId)
            .setTxId(txId)
            .setState(TransactionState.TS_PREPARED)
            .setUpdatedAt(Timestamps.fromMillis(1))
            .build();

    when(txRepo.getById(accountId, txId)).thenReturn(Optional.of(preparedTxn));

    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                invokeReserveTransactionTableId(
                    service, accountId, txId, "cat.db.orders", Timestamps.fromMillis(10)));

    assertTrue(error.getMessage().contains("open transaction"));
    verify(txRepo, never()).update(any(Transaction.class), anyLong());
  }

  private static Transaction invokeCommitPrivate(
      TransactionsServiceImpl service,
      String accountId,
      CommitTransactionRequest request,
      com.google.protobuf.Timestamp now)
      throws Exception {
    Method m =
        TransactionsServiceImpl.class.getDeclaredMethod(
            "commitTransaction",
            String.class,
            CommitTransactionRequest.class,
            com.google.protobuf.Timestamp.class);
    m.setAccessible(true);
    return (Transaction) m.invoke(service, accountId, request, now);
  }

  private static Transaction invokePreparePrivate(
      TransactionsServiceImpl service,
      String accountId,
      PrepareTransactionRequest request,
      com.google.protobuf.Timestamp now)
      throws Exception {
    Method m =
        TransactionsServiceImpl.class.getDeclaredMethod(
            "prepareTransaction",
            String.class,
            PrepareTransactionRequest.class,
            com.google.protobuf.Timestamp.class);
    m.setAccessible(true);
    return (Transaction) m.invoke(service, accountId, request, now);
  }

  private static Transaction invokeAbortExpired(
      TransactionsServiceImpl service, Transaction txn, com.google.protobuf.Timestamp now)
      throws Exception {
    Method m =
        TransactionsServiceImpl.class.getDeclaredMethod(
            "abortExpired", Transaction.class, com.google.protobuf.Timestamp.class);
    m.setAccessible(true);
    return (Transaction) m.invoke(service, txn, now);
  }

  private static Connector invokeBuildProvisionedConnector(
      TransactionsServiceImpl service,
      String accountId,
      String txId,
      Table table,
      ConnectorProvisioning provisioning)
      throws Exception {
    Method m =
        TransactionsServiceImpl.class.getDeclaredMethod(
            "buildProvisionedConnector",
            String.class,
            String.class,
            Table.class,
            ConnectorProvisioning.class,
            com.google.protobuf.Timestamp.class);
    m.setAccessible(true);
    try {
      return (Connector) m.invoke(service, accountId, txId, table, provisioning, null);
    } catch (InvocationTargetException e) {
      if (e.getCause() instanceof Exception ex) {
        throw ex;
      }
      throw e;
    }
  }

  private static ResourceId invokeReserveTransactionTableId(
      TransactionsServiceImpl service,
      String accountId,
      String txId,
      String tableFq,
      com.google.protobuf.Timestamp now)
      throws Exception {
    Method m =
        TransactionsServiceImpl.class.getDeclaredMethod(
            "reserveTransactionTableId",
            String.class,
            String.class,
            String.class,
            com.google.protobuf.Timestamp.class);
    m.setAccessible(true);
    try {
      return (ResourceId) m.invoke(service, accountId, txId, tableFq, now);
    } catch (InvocationTargetException e) {
      if (e.getCause() instanceof Exception ex) {
        throw ex;
      }
      throw e;
    }
  }

  private static ResourceId resourceId(String id, ResourceKind kind) {
    return ResourceId.newBuilder().setAccountId("acct").setId(id).setKind(kind).build();
  }

  private static void inject(Object target, String field, Object value) throws Exception {
    Field f = target.getClass().getDeclaredField(field);
    f.setAccessible(true);
    f.set(target, value);
  }
}
