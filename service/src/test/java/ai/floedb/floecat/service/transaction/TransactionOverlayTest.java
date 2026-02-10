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

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.ResourceHash;
import ai.floedb.floecat.service.repo.util.TransactionOverlay;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.transaction.rpc.Transaction;
import ai.floedb.floecat.transaction.rpc.TransactionIntent;
import ai.floedb.floecat.transaction.rpc.TransactionState;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import java.lang.reflect.Field;
import org.junit.jupiter.api.Test;

class TransactionOverlayTest {

  @Test
  void committedIntentOverridesPointer() throws Exception {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();

    String accountId = "acct";
    String catalogId = "cat";
    String namespaceId = "ns";
    String tableId = "tbl";

    var oldTable = table(accountId, catalogId, namespaceId, tableId, "old_name");
    var oldSha = ResourceHash.sha256Hex(oldTable.toByteArray());
    var oldBlob = Keys.tableBlobUri(accountId, tableId, oldSha);
    blobs.put(oldBlob, oldTable.toByteArray(), "application/x-protobuf");

    String tablePtrKey = Keys.tablePointerById(accountId, tableId);
    pointers.compareAndSet(
        tablePtrKey,
        0L,
        Pointer.newBuilder().setKey(tablePtrKey).setBlobUri(oldBlob).setVersion(1L).build());

    var newTable = table(accountId, catalogId, namespaceId, tableId, "new_name");
    var newSha = ResourceHash.sha256Hex(newTable.toByteArray());
    var newBlob = Keys.tableBlobUri(accountId, tableId, newSha);
    blobs.put(newBlob, newTable.toByteArray(), "application/x-protobuf");

    String txId = "tx1";
    var tx =
        Transaction.newBuilder()
            .setTxId(txId)
            .setAccountId(accountId)
            .setState(TransactionState.TS_COMMITTED)
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
            .setTargetPointerKey(tablePtrKey)
            .setBlobUri(newBlob)
            .setExpectedVersion(1L)
            .setCreatedAt(now())
            .build();
    var intentSha = ResourceHash.sha256Hex(intent.toByteArray());
    var intentBlob = Keys.transactionIntentBlobUri(accountId, txId, intentSha);
    blobs.put(intentBlob, intent.toByteArray(), "application/x-protobuf");
    String intentPtr = Keys.transactionIntentPointerByTarget(accountId, tablePtrKey);
    pointers.compareAndSet(
        intentPtr,
        0L,
        Pointer.newBuilder().setKey(intentPtr).setBlobUri(intentBlob).setVersion(1L).build());

    var overlay = new TransactionOverlay();
    inject(overlay, "pointerStore", pointers);
    inject(overlay, "blobStore", blobs);

    var effective =
        overlay
            .resolveEffectivePointer(tablePtrKey, pointers.get(tablePtrKey).orElseThrow())
            .orElseThrow();
    assertEquals(newBlob, effective.getBlobUri());
  }

  @Test
  void uncommittedIntentDoesNotOverride() throws Exception {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();

    String accountId = "acct";
    String catalogId = "cat";
    String namespaceId = "ns";
    String tableId = "tbl";

    var oldTable = table(accountId, catalogId, namespaceId, tableId, "old_name");
    var oldSha = ResourceHash.sha256Hex(oldTable.toByteArray());
    var oldBlob = Keys.tableBlobUri(accountId, tableId, oldSha);
    blobs.put(oldBlob, oldTable.toByteArray(), "application/x-protobuf");

    String tablePtrKey = Keys.tablePointerById(accountId, tableId);
    pointers.compareAndSet(
        tablePtrKey,
        0L,
        Pointer.newBuilder().setKey(tablePtrKey).setBlobUri(oldBlob).setVersion(1L).build());

    var newTable = table(accountId, catalogId, namespaceId, tableId, "new_name");
    var newSha = ResourceHash.sha256Hex(newTable.toByteArray());
    var newBlob = Keys.tableBlobUri(accountId, tableId, newSha);
    blobs.put(newBlob, newTable.toByteArray(), "application/x-protobuf");

    String txId = "tx1";
    var tx =
        Transaction.newBuilder()
            .setTxId(txId)
            .setAccountId(accountId)
            .setState(TransactionState.TS_OPEN)
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
            .setTargetPointerKey(tablePtrKey)
            .setBlobUri(newBlob)
            .setExpectedVersion(1L)
            .setCreatedAt(now())
            .build();
    var intentSha = ResourceHash.sha256Hex(intent.toByteArray());
    var intentBlob = Keys.transactionIntentBlobUri(accountId, txId, intentSha);
    blobs.put(intentBlob, intent.toByteArray(), "application/x-protobuf");
    String intentPtr = Keys.transactionIntentPointerByTarget(accountId, tablePtrKey);
    pointers.compareAndSet(
        intentPtr,
        0L,
        Pointer.newBuilder().setKey(intentPtr).setBlobUri(intentBlob).setVersion(1L).build());

    var overlay = new TransactionOverlay();
    inject(overlay, "pointerStore", pointers);
    inject(overlay, "blobStore", blobs);

    var effective =
        overlay
            .resolveEffectivePointer(tablePtrKey, pointers.get(tablePtrKey).orElseThrow())
            .orElseThrow();
    assertEquals(oldBlob, effective.getBlobUri());
  }

  private static Table table(
      String accountId, String catalogId, String namespaceId, String tableId, String name) {
    return Table.newBuilder()
        .setResourceId(
            ResourceId.newBuilder()
                .setAccountId(accountId)
                .setKind(ResourceKind.RK_TABLE)
                .setId(tableId))
        .setCatalogId(
            ResourceId.newBuilder()
                .setAccountId(accountId)
                .setKind(ResourceKind.RK_CATALOG)
                .setId(catalogId))
        .setNamespaceId(
            ResourceId.newBuilder()
                .setAccountId(accountId)
                .setKind(ResourceKind.RK_NAMESPACE)
                .setId(namespaceId))
        .setDisplayName(name)
        .setSchemaJson("{}")
        .setCreatedAt(now())
        .build();
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
