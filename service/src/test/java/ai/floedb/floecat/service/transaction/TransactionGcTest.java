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

import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.gc.TransactionGc;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.ResourceHash;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.transaction.rpc.Transaction;
import ai.floedb.floecat.transaction.rpc.TransactionIntent;
import ai.floedb.floecat.transaction.rpc.TransactionState;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import java.lang.reflect.Field;
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
    var txSha = ResourceHash.sha256Hex(tx.toByteArray());
    var txBlob = Keys.transactionBlobUri(accountId, txId, txSha);
    blobs.put(txBlob, tx.toByteArray(), "application/x-protobuf");
    String txPtr = Keys.transactionPointerById(accountId, txId);
    pointers.compareAndSet(
        txPtr, 0L, Pointer.newBuilder().setKey(txPtr).setBlobUri(txBlob).setVersion(1L).build());

    var intent =
        TransactionIntent.newBuilder()
            .setTxId(txId)
            .setAccountId(accountId)
            .setTargetPointerKey(targetKey)
            .setBlobUri("s3://example/blob")
            .setExpectedVersion(1L)
            .setCreatedAt(now())
            .build();
    var intentSha = ResourceHash.sha256Hex(intent.toByteArray());
    var intentBlob = Keys.transactionIntentBlobUri(accountId, txId, intentSha);
    blobs.put(intentBlob, intent.toByteArray(), "application/x-protobuf");

    String byTarget = Keys.transactionIntentPointerByTarget(accountId, targetKey);
    pointers.compareAndSet(
        byTarget,
        0L,
        Pointer.newBuilder().setKey(byTarget).setBlobUri(intentBlob).setVersion(1L).build());
    String byTx = Keys.transactionIntentPointerByTx(accountId, txId, targetKey);
    pointers.compareAndSet(
        byTx, 0L, Pointer.newBuilder().setKey(byTx).setBlobUri(intentBlob).setVersion(1L).build());

    var gc = new TransactionGc();
    inject(gc, "pointerStore", pointers);
    inject(gc, "blobStore", blobs);

    gc.runForAccount(accountId, System.currentTimeMillis() + 5000);

    assertTrue(pointers.get(byTarget).isEmpty(), "by-target intent pointer should be deleted");
    assertTrue(pointers.get(byTx).isEmpty(), "by-tx intent pointer should be deleted");
  }

  private static Timestamp now() {
    return Timestamps.fromMillis(System.currentTimeMillis());
  }

  private static void inject(Object target, String field, Object value) throws Exception {
    Field f = target.getClass().getDeclaredField(field);
    f.setAccessible(true);
    f.set(target, value);
  }
}
