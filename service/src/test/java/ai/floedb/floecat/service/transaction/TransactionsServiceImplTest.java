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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.metagraph.resolver.NameResolver;
import ai.floedb.floecat.service.repo.impl.TransactionIntentRepository;
import ai.floedb.floecat.service.repo.impl.TransactionRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.transaction.impl.TransactionIntentApplierSupport;
import ai.floedb.floecat.service.transaction.impl.TransactionsServiceImpl;
import ai.floedb.floecat.transaction.rpc.CommitTransactionRequest;
import ai.floedb.floecat.transaction.rpc.PrepareTransactionRequest;
import ai.floedb.floecat.transaction.rpc.Transaction;
import ai.floedb.floecat.transaction.rpc.TransactionIntent;
import ai.floedb.floecat.transaction.rpc.TransactionState;
import ai.floedb.floecat.transaction.rpc.TxChange;
import com.google.protobuf.util.Timestamps;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
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
    TransactionIntent intent =
        TransactionIntent.newBuilder()
            .setAccountId("acct")
            .setTxId("tx-1")
            .setTargetPointerKey("/accounts/acct/custom/key-1")
            .setBlobUri("s3://bucket/blob-1")
            .setCreatedAt(Timestamps.fromMillis(1))
            .build();

    when(txRepo.getById("acct", "tx-1")).thenReturn(Optional.of(txn));
    when(intentRepo.listByTx("acct", "tx-1")).thenReturn(List.of(intent));
    when(intentRepo.getByTarget("acct", "/accounts/acct/custom/key-1"))
        .thenReturn(Optional.of(intent));
    when(applier.applyTransactionBestEffort(List.of(intent), intentRepo))
        .thenReturn(TransactionIntentApplierSupport.ApplyOutcome.applied());
    when(txRepo.metaFor("acct", "tx-1"))
        .thenReturn(MutationMeta.newBuilder().setPointerVersion(11L).build());
    when(txRepo.update(
            argThat(updated -> updated.getState() == TransactionState.TS_APPLIED), anyLong()))
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
        .update(argThat(updated -> updated.getState() == TransactionState.TS_APPLIED), anyLong());
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
    TransactionIntent intent =
        TransactionIntent.newBuilder()
            .setAccountId("acct")
            .setTxId("tx-1")
            .setTargetPointerKey("/accounts/acct/custom/key-1")
            .setBlobUri("s3://bucket/blob-1")
            .setCreatedAt(Timestamps.fromMillis(1))
            .build();

    when(txRepo.getById("acct", "tx-1")).thenReturn(Optional.of(txn));
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
        .thenReturn(MutationMeta.newBuilder().setPointerVersion(12L).build());
    when(txRepo.update(
            argThat(updated -> updated.getState() == TransactionState.TS_APPLY_FAILED_RETRYABLE),
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
    TransactionIntent intent =
        TransactionIntent.newBuilder()
            .setAccountId("acct")
            .setTxId("tx-1")
            .setTargetPointerKey("/accounts/acct/custom/key-1")
            .setBlobUri("s3://bucket/blob-1")
            .setCreatedAt(Timestamps.fromMillis(1))
            .build();

    when(txRepo.getById("acct", "tx-1")).thenReturn(Optional.of(txn));
    when(intentRepo.listByTx("acct", "tx-1")).thenReturn(List.of(intent));
    when(intentRepo.getByTarget("acct", "/accounts/acct/custom/key-1"))
        .thenReturn(Optional.of(intent));
    when(applier.applyTransactionBestEffort(List.of(intent), intentRepo))
        .thenReturn(TransactionIntentApplierSupport.ApplyOutcome.applied());
    when(txRepo.metaFor("acct", "tx-1"))
        .thenReturn(MutationMeta.newBuilder().setPointerVersion(13L).build());
    when(txRepo.update(
            argThat(updated -> updated.getState() == TransactionState.TS_APPLIED), anyLong()))
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
    TransactionIntent intent =
        TransactionIntent.newBuilder()
            .setAccountId("acct")
            .setTxId("tx-1")
            .setTargetPointerKey("/accounts/acct/custom/key-1")
            .setBlobUri("s3://bucket/blob-1")
            .setCreatedAt(Timestamps.fromMillis(1))
            .build();

    when(txRepo.getById("acct", "tx-1")).thenReturn(Optional.of(txn));
    when(intentRepo.listByTx("acct", "tx-1")).thenReturn(List.of(intent));
    when(intentRepo.getByTarget("acct", "/accounts/acct/custom/key-1"))
        .thenReturn(Optional.of(intent));
    when(applier.applyTransactionBestEffort(List.of(intent), intentRepo))
        .thenReturn(
            TransactionIntentApplierSupport.ApplyOutcome.conflict(
                "EXPECTED_VERSION_MISMATCH", "pointer version changed", 3L, 4L, null));
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
        .thenReturn(MutationMeta.newBuilder().setPointerVersion(14L).build());
    when(txRepo.update(
            argThat(updated -> updated.getState() == TransactionState.TS_APPLY_FAILED_CONFLICT),
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
            argThat(updated -> updated.getState() == TransactionState.TS_PREPARED), anyLong()))
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
        .update(argThat(updated -> updated.getState() == TransactionState.TS_ABORTED), anyLong());
  }

  @Test
  void retryableStateIsNotCommittedFamily() throws Exception {
    var service = new TransactionsServiceImpl();
    Method m =
        TransactionsServiceImpl.class.getDeclaredMethod(
            "isCommittedFamily", TransactionState.class);
    m.setAccessible(true);

    assertFalse((Boolean) m.invoke(service, TransactionState.TS_APPLY_FAILED_RETRYABLE));
    assertTrue((Boolean) m.invoke(service, TransactionState.TS_APPLIED));
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

  private static void inject(Object target, String field, Object value) throws Exception {
    Field f = target.getClass().getDeclaredField(field);
    f.setAccessible(true);
    f.set(target, value);
  }
}
