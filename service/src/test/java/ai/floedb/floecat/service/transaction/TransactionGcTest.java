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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.service.gc.TransactionGc;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.transaction.rpc.Transaction;
import ai.floedb.floecat.transaction.rpc.TransactionIntent;
import ai.floedb.floecat.transaction.rpc.TransactionState;
import ai.floedb.floecat.types.Hashing;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import java.lang.reflect.Field;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class TransactionGcTest {

  @Test
  void abortedTransactionCleansIntents() throws Exception {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();

    String accountId = "acct";
    String txId = "tx1";
    String targetKey = "/accounts/acct/tables/by-id/t1";

    var tx =
        Transaction.newBuilder()
            .setTxId(txId)
            .setAccountId(accountId)
            .setState(TransactionState.TS_ABORTED)
            .setCreatedAt(now())
            .setUpdatedAt(now())
            .build();
    var txSha = Hashing.sha256Hex(tx.toByteArray());
    var txBlob = Keys.transactionBlobUri(accountId, txId, txSha);
    blobs.put(txBlob, tx.toByteArray(), "application/x-protobuf");
    String txPtr = Keys.transactionPointerById(accountId, txId);
    pointers.compareAndSet(txPtr, 0L, PointerReferences.blobPointer(txPtr, txBlob, 1L));

    var intent =
        TransactionIntent.newBuilder()
            .setTxId(txId)
            .setAccountId(accountId)
            .setTargetPointerKey(targetKey)
            .setBlobUri("s3://example/blob")
            .setExpectedVersion(1L)
            .setCreatedAt(now())
            .build();
    var intentSha = Hashing.sha256Hex(intent.toByteArray());
    var intentBlob = Keys.transactionIntentBlobUri(accountId, txId, intentSha);
    blobs.put(intentBlob, intent.toByteArray(), "application/x-protobuf");

    String byTarget = Keys.transactionIntentPointerByTarget(accountId, targetKey);
    pointers.compareAndSet(byTarget, 0L, PointerReferences.blobPointer(byTarget, intentBlob, 1L));
    String byTx = Keys.transactionIntentPointerByTx(accountId, txId, targetKey);
    pointers.compareAndSet(byTx, 0L, PointerReferences.blobPointer(byTx, intentBlob, 1L));

    var gc = new TransactionGc();
    inject(gc, "pointerStore", pointers);
    inject(gc, "blobStore", blobs);

    gc.runForAccount(accountId, System.currentTimeMillis() + 5000);

    assertTrue(pointers.get(byTarget).isEmpty(), "by-target intent pointer should be deleted");
    assertTrue(pointers.get(byTx).isEmpty(), "by-tx intent pointer should be deleted");
  }

  @Test
  void committedFamilyWithoutIntentsIsCollectedAfterExpiry() throws Exception {
    assertCollected(TransactionState.TS_APPLIED);
    assertCollected(TransactionState.TS_APPLY_FAILED_RETRYABLE);
    assertCollected(TransactionState.TS_APPLY_FAILED_CONFLICT);
  }

  @Test
  void appliedWithIntentsAreReclaimedAsStaleOwners() throws Exception {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();

    String accountId = "acct";
    String txId = "tx-with-intent";
    String targetKey = "/accounts/acct/tables/by-id/t1";

    putTransaction(
        pointers,
        blobs,
        accountId,
        txId,
        TransactionState.TS_APPLIED,
        Timestamps.fromMillis(System.currentTimeMillis() - 300_000L));
    putIntent(pointers, blobs, accountId, txId, targetKey);

    var gc = new TransactionGc();
    inject(gc, "pointerStore", pointers);
    inject(gc, "blobStore", blobs);

    gc.runForAccount(accountId, System.currentTimeMillis() + 5000);

    String txPtr = Keys.transactionPointerById(accountId, txId);
    String byTarget = Keys.transactionIntentPointerByTarget(accountId, targetKey);
    String byTx = Keys.transactionIntentPointerByTx(accountId, txId, targetKey);
    assertTrue(
        pointers.get(txPtr).isPresent(),
        "tx pointer remains until a subsequent pass after intents");
    assertTrue(pointers.get(byTarget).isEmpty(), "applied owner target intent should be reclaimed");
    assertTrue(pointers.get(byTx).isEmpty(), "applied owner by-tx intent should be reclaimed");
  }

  @Test
  void danglingTargetIntentForAppliedOwnerIsReclaimed() throws Exception {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    String accountId = "acct";
    String txId = "tx-applied";
    String targetKey = "/accounts/acct/tables/by-id/t1";

    putTransaction(
        pointers,
        blobs,
        accountId,
        txId,
        TransactionState.TS_APPLIED,
        Timestamps.fromMillis(System.currentTimeMillis() - 300_000L));
    putIntent(pointers, blobs, accountId, txId, targetKey);
    String byTx = Keys.transactionIntentPointerByTx(accountId, txId, targetKey);
    pointers.get(byTx).ifPresent(ptr -> pointers.compareAndDelete(ptr.getKey(), ptr.getVersion()));

    var gc = new TransactionGc();
    inject(gc, "pointerStore", pointers);
    inject(gc, "blobStore", blobs);
    gc.runForAccount(accountId, System.currentTimeMillis() + 5000);

    String byTarget = Keys.transactionIntentPointerByTarget(accountId, targetKey);
    assertTrue(pointers.get(byTarget).isEmpty(), "dangling by-target intent should be reclaimed");
  }

  @Test
  void danglingTargetIntentForExpiredRetryableOwnerIsReclaimed() throws Exception {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    String accountId = "acct";
    String txId = "tx-retryable-expired";
    String targetKey = "/accounts/acct/tables/by-id/t2";

    putTransaction(
        pointers,
        blobs,
        accountId,
        txId,
        TransactionState.TS_APPLY_FAILED_RETRYABLE,
        Timestamps.fromMillis(System.currentTimeMillis() - 300_000L));
    putIntent(pointers, blobs, accountId, txId, targetKey);
    String byTx = Keys.transactionIntentPointerByTx(accountId, txId, targetKey);
    pointers.get(byTx).ifPresent(ptr -> pointers.compareAndDelete(ptr.getKey(), ptr.getVersion()));

    var gc = new TransactionGc();
    inject(gc, "pointerStore", pointers);
    inject(gc, "blobStore", blobs);
    gc.runForAccount(accountId, System.currentTimeMillis() + 5000);

    String byTarget = Keys.transactionIntentPointerByTarget(accountId, targetKey);
    assertTrue(
        pointers.get(byTarget).isEmpty(),
        "dangling by-target intent for expired retryable owner should be reclaimed");
  }

  @Test
  void cleanupIntentsForTxDoesNotDeleteDifferentOwnerTargetIntent() throws Exception {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    String accountId = "acct";
    String staleTxId = "tx-stale";
    String activeTxId = "tx-active";
    String targetKey = "/accounts/acct/tables/by-id/t1";

    putTransaction(
        pointers,
        blobs,
        accountId,
        staleTxId,
        TransactionState.TS_ABORTED,
        Timestamps.fromMillis(System.currentTimeMillis() - 300_000L));
    putTransaction(
        pointers,
        blobs,
        accountId,
        activeTxId,
        TransactionState.TS_OPEN,
        Timestamps.fromMillis(System.currentTimeMillis() + 300_000L));

    var activeIntentBlob = putIntentBlob(blobs, accountId, activeTxId, targetKey);
    String byTarget = Keys.transactionIntentPointerByTarget(accountId, targetKey);
    pointers.compareAndSet(
        byTarget, 0L, PointerReferences.blobPointer(byTarget, activeIntentBlob, 1L));

    // Simulate stale by-tx row pointing to someone else's intent blob.
    String staleByTx = Keys.transactionIntentPointerByTx(accountId, staleTxId, targetKey);
    pointers.compareAndSet(
        staleByTx, 0L, PointerReferences.blobPointer(staleByTx, activeIntentBlob, 1L));

    var gc = new TransactionGc();
    inject(gc, "pointerStore", pointers);
    inject(gc, "blobStore", blobs);
    gc.runForAccount(accountId, System.currentTimeMillis() + 5000);

    assertTrue(
        pointers.get(byTarget).isPresent(),
        "by-target intent owned by different tx should not be deleted");
    assertTrue(
        pointers.get(staleByTx).isEmpty(), "stale by-tx row should be cleaned up for aborted tx");
  }

  private void assertCollected(TransactionState state) throws Exception {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();

    String accountId = "acct";
    String txId = "tx-" + state.name().toLowerCase();
    putTransaction(
        pointers,
        blobs,
        accountId,
        txId,
        state,
        Timestamps.fromMillis(System.currentTimeMillis() - 300_000L));

    var gc = new TransactionGc();
    inject(gc, "pointerStore", pointers);
    inject(gc, "blobStore", blobs);

    gc.runForAccount(accountId, System.currentTimeMillis() + 5000);

    String txPtr = Keys.transactionPointerById(accountId, txId);
    assertTrue(pointers.get(txPtr).isEmpty(), "tx pointer should be collected for " + state.name());
  }

  private void putTransaction(
      InMemoryPointerStore pointers,
      InMemoryBlobStore blobs,
      String accountId,
      String txId,
      TransactionState state,
      Timestamp expiresAt) {
    var tx =
        Transaction.newBuilder()
            .setTxId(txId)
            .setAccountId(accountId)
            .setState(state)
            .setCreatedAt(now())
            .setUpdatedAt(now())
            .setExpiresAt(expiresAt)
            .build();
    var txSha = Hashing.sha256Hex(tx.toByteArray());
    var txBlob = Keys.transactionBlobUri(accountId, txId, txSha);
    blobs.put(txBlob, tx.toByteArray(), "application/x-protobuf");
    String txPtr = Keys.transactionPointerById(accountId, txId);
    pointers.compareAndSet(txPtr, 0L, PointerReferences.blobPointer(txPtr, txBlob, 1L));
  }

  private void putIntent(
      InMemoryPointerStore pointers,
      InMemoryBlobStore blobs,
      String accountId,
      String txId,
      String targetKey) {
    String intentBlob = putIntentBlob(blobs, accountId, txId, targetKey);
    String byTarget = Keys.transactionIntentPointerByTarget(accountId, targetKey);
    pointers.compareAndSet(byTarget, 0L, PointerReferences.blobPointer(byTarget, intentBlob, 1L));
    String byTx = Keys.transactionIntentPointerByTx(accountId, txId, targetKey);
    pointers.compareAndSet(byTx, 0L, PointerReferences.blobPointer(byTx, intentBlob, 1L));
  }

  private String putIntentBlob(
      InMemoryBlobStore blobs, String accountId, String txId, String targetKey) {
    var intent =
        TransactionIntent.newBuilder()
            .setTxId(txId)
            .setAccountId(accountId)
            .setTargetPointerKey(targetKey)
            .setBlobUri("s3://example/blob")
            .setExpectedVersion(1L)
            .setCreatedAt(now())
            .build();
    var intentSha = Hashing.sha256Hex(intent.toByteArray());
    var intentBlob = Keys.transactionIntentBlobUri(accountId, txId, intentSha);
    blobs.put(intentBlob, intent.toByteArray(), "application/x-protobuf");
    return intentBlob;
  }

  private static Timestamp now() {
    return Timestamps.fromMillis(System.currentTimeMillis());
  }

  @Test
  void enqueueTouchingAnExistingMarkerDefeatsAStaleVersionedDelete() {
    // GC lists the marker at v1, resyncs, and clears with a versioned delete. A NEW failure
    // recorded in between must bump the marker's version so the stale delete loses and the new
    // failure stays recorded — otherwise a transactions-only table never converges.
    var pointers = new InMemoryPointerStore();
    var queue = new ai.floedb.floecat.service.catalog.impl.RootResyncQueue(pointers);
    var rid =
        ai.floedb.floecat.common.rpc.ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("tbl-race")
            .setKind(ai.floedb.floecat.common.rpc.ResourceKind.RK_TABLE)
            .build();
    String marker = Keys.rootResyncPendingPointer("acct", "tbl-race");

    queue.enqueue(rid);
    long observedByGc = pointers.get(marker).orElseThrow().getVersion();

    queue.enqueue(rid); // the racing failure: must touch, not no-op

    assertTrue(
        pointers.get(marker).orElseThrow().getVersion() > observedByGc,
        "a second enqueue must bump the marker version");
    org.junit.jupiter.api.Assertions.assertFalse(
        pointers.compareAndDelete(marker, observedByGc),
        "the GC pass's stale versioned delete must lose");
    assertTrue(pointers.get(marker).isPresent(), "the new failure stays recorded");
  }

  @Test
  void aMarkerDeletedBetweenTheFailedCreateAndTheTouchIsRecreated() {
    // The one-shot touch had a hole one level deeper than the no-op enqueue: create fails (marker
    // exists) -> GC's versioned delete lands -> the touch finds nothing and silently skips ->
    // the failure is unrecorded. The enqueue must loop back to a fresh create.
    boolean[] intercepted = {false};
    var rid =
        ai.floedb.floecat.common.rpc.ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("tbl-vanish")
            .setKind(ai.floedb.floecat.common.rpc.ResourceKind.RK_TABLE)
            .build();
    String marker = Keys.rootResyncPendingPointer("acct", "tbl-vanish");
    var store =
        new InMemoryPointerStore() {
          @Override
          public java.util.Optional<ai.floedb.floecat.common.rpc.Pointer> get(String key) {
            if (!intercepted[0] && marker.equals(key)) {
              intercepted[0] = true;
              delete(key); // the GC pass's delete racing in
              return java.util.Optional.empty();
            }
            return super.get(key);
          }
        };
    // A marker already exists, so the first create-CAS fails and the enqueue reads it back —
    // which is exactly when the interceptor makes it vanish.
    store.compareAndSet(marker, 0L, PointerReferences.blobPointer(marker, "", 1L));
    var queue = new ai.floedb.floecat.service.catalog.impl.RootResyncQueue(store);

    queue.enqueue(rid);

    assertTrue(
        store.get(marker).isPresent(),
        "the enqueue must re-create a marker deleted out from under its touch");
  }

  @Test
  void aFailingMarkerStoreIsAbsorbedWithoutFailingTheTransactionPath() {
    // The enqueue is the last resort after a failed resync; if the pointer store is down for the
    // marker write too, the failure is absorbed (WARN) — the transaction is already durable and
    // must not fail. Convergence is then only re-attempted by later traffic; this is the accepted
    // limitation of a fully-down store.
    var failingStore =
        new InMemoryPointerStore() {
          @Override
          public boolean compareAndSet(
              String key, long expectedVersion, ai.floedb.floecat.common.rpc.Pointer pointer) {
            throw new IllegalStateException("store down");
          }
        };
    var queue = new ai.floedb.floecat.service.catalog.impl.RootResyncQueue(failingStore);
    var rid =
        ai.floedb.floecat.common.rpc.ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("tbl-down")
            .setKind(ai.floedb.floecat.common.rpc.ResourceKind.RK_TABLE)
            .build();

    org.junit.jupiter.api.Assertions.assertThrows(
        IllegalStateException.class, () -> queue.enqueue(rid));
    // The CALLER (resyncRootOrLeaveMarker) absorbs this; asserting the throw here documents that
    // the queue itself stays honest and the absorption lives at the transaction boundary.
  }

  @Test
  void pendingRootResyncMarkersAreReDrivenAndClearedOnSuccess() throws Exception {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    String accountId = "acct";
    String tableId = "tbl-resync";

    // The durable marker a transaction leaves when its post-apply root resync was absorbed.
    String marker = Keys.rootResyncPendingPointer(accountId, tableId);
    pointers.compareAndSet(marker, 0L, PointerReferences.blobPointer(marker, "", 1L));

    var roots = new ai.floedb.floecat.service.repo.impl.TableRootRepository(pointers, blobs);
    var committer = new ai.floedb.floecat.service.catalog.impl.TableRootCommitter(roots);
    var tables = mock(ai.floedb.floecat.service.repo.impl.TableRepository.class);
    when(tables.metaForSafe(any()))
        .thenReturn(
            ai.floedb.floecat.common.rpc.MutationMeta.newBuilder()
                .setBlobUri("s3://t/table.pb")
                .setEtag("e1")
                .build());
    var snapshots = mock(ai.floedb.floecat.service.repo.impl.SnapshotRepository.class);
    when(snapshots.latestRegisteredSnapshotPointer(any())).thenReturn(Optional.empty());
    var statsStore = mock(ai.floedb.floecat.stats.spi.StatsStore.class);
    when(statsStore.tracksStatsGenerations()).thenReturn(true);
    var writer =
        new ai.floedb.floecat.service.catalog.impl.TableRootWriter(
            roots,
            committer,
            tables,
            snapshots,
            mock(ai.floedb.floecat.service.repo.impl.ConstraintRepository.class),
            statsStore);

    var gc = new TransactionGc();
    inject(gc, "pointerStore", pointers);
    inject(gc, "blobStore", blobs);
    inject(gc, "rootWriter", writer);

    gc.runForAccount(accountId, System.currentTimeMillis() + 5000);

    assertTrue(pointers.get(marker).isEmpty(), "marker clears after a successful re-drive");
    var rid =
        ai.floedb.floecat.common.rpc.ResourceId.newBuilder()
            .setAccountId(accountId)
            .setId(tableId)
            .setKind(ai.floedb.floecat.common.rpc.ResourceKind.RK_TABLE)
            .build();
    assertEquals(
        "s3://t/table.pb",
        roots.get(rid).orElseThrow().getDefinitionRef().getUri(),
        "the re-driven resync converged the root with committed state");
  }

  @Test
  void aStillFailingResyncKeepsItsMarkerForTheNextPass() throws Exception {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    String accountId = "acct";
    String tableId = "tbl-stuck";

    String marker = Keys.rootResyncPendingPointer(accountId, tableId);
    pointers.compareAndSet(marker, 0L, PointerReferences.blobPointer(marker, "", 1L));

    var roots = new ai.floedb.floecat.service.repo.impl.TableRootRepository(pointers, blobs);
    var committer = new ai.floedb.floecat.service.catalog.impl.TableRootCommitter(roots);
    var tables = mock(ai.floedb.floecat.service.repo.impl.TableRepository.class);
    when(tables.metaForSafe(any())).thenThrow(new IllegalStateException("store down"));
    var writer =
        new ai.floedb.floecat.service.catalog.impl.TableRootWriter(
            roots,
            committer,
            tables,
            mock(ai.floedb.floecat.service.repo.impl.SnapshotRepository.class),
            mock(ai.floedb.floecat.service.repo.impl.ConstraintRepository.class),
            mock(ai.floedb.floecat.stats.spi.StatsStore.class));

    var gc = new TransactionGc();
    inject(gc, "pointerStore", pointers);
    inject(gc, "blobStore", blobs);
    inject(gc, "rootWriter", writer);

    gc.runForAccount(accountId, System.currentTimeMillis() + 5000);

    assertTrue(
        pointers.get(marker).isPresent(),
        "the marker survives until a re-drive actually converges");
  }

  private static void inject(Object target, String field, Object value) throws Exception {
    Field f = target.getClass().getDeclaredField(field);
    f.setAccessible(true);
    f.set(target, value);
  }
}
