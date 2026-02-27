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

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.impl.TransactionIntentRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.transaction.impl.TransactionIntentApplierSupport;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.transaction.rpc.TransactionIntent;
import com.google.protobuf.util.Timestamps;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class TransactionIntentApplierSupportTest {

  @Test
  void applyTransactionRejectsWhenPointerOpsExceedLimit() throws Exception {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var intentRepo = new TransactionIntentRepository(pointers, blobs);

    var support = new TransactionIntentApplierSupport();
    inject(support, "pointerStore", pointers);
    inject(support, "blobStore", blobs);

    List<TransactionIntent> intents = new ArrayList<>();
    for (int i = 0; i < 101; i++) {
      intents.add(
          TransactionIntent.newBuilder()
              .setAccountId("acct")
              .setTxId("tx-1")
              .setTargetPointerKey("/accounts/acct/custom/key-" + i)
              .setBlobUri("s3://bucket/blob-" + i)
              .setCreatedAt(Timestamps.fromMillis(i + 1))
              .build());
    }

    var outcome = support.applyTransactionBestEffort(intents, intentRepo);
    assertEquals(TransactionIntentApplierSupport.ApplyStatus.CONFLICT, outcome.status());
    assertEquals("POINTER_TXN_TOO_LARGE", outcome.errorCode());
  }

  @Test
  void applyTransactionRejectsDuplicateTargetPointerKeyWithinSingleTx() throws Exception {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var intentRepo = new TransactionIntentRepository(pointers, blobs);

    var support = new TransactionIntentApplierSupport();
    inject(support, "pointerStore", pointers);
    inject(support, "blobStore", blobs);

    String targetKey = "/accounts/acct/custom/key-dup";
    TransactionIntent intentA =
        TransactionIntent.newBuilder()
            .setAccountId("acct")
            .setTxId("tx-1")
            .setTargetPointerKey(targetKey)
            .setBlobUri("s3://bucket/blob-a")
            .setCreatedAt(Timestamps.fromMillis(1))
            .build();
    TransactionIntent intentB =
        TransactionIntent.newBuilder()
            .setAccountId("acct")
            .setTxId("tx-1")
            .setTargetPointerKey(targetKey)
            .setBlobUri("s3://bucket/blob-b")
            .setCreatedAt(Timestamps.fromMillis(2))
            .build();

    var outcome = support.applyTransactionBestEffort(List.of(intentA, intentB), intentRepo);

    assertEquals(TransactionIntentApplierSupport.ApplyStatus.CONFLICT, outcome.status());
    assertEquals("POINTER_TXN_DUPLICATE_KEY", outcome.errorCode());
    assertTrue(
        pointers.get(targetKey).isEmpty(),
        "no pointer write should be applied when a duplicate key is detected");
  }

  @Test
  void applyTransactionDoesNotDeleteIntentsDirectly() throws Exception {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var intentRepo = new TransactionIntentRepository(pointers, blobs);

    var support = new TransactionIntentApplierSupport();
    inject(support, "pointerStore", pointers);
    inject(support, "blobStore", blobs);

    TransactionIntent intent =
        TransactionIntent.newBuilder()
            .setAccountId("acct")
            .setTxId("tx-1")
            .setTargetPointerKey("/accounts/acct/custom/key-1")
            .setBlobUri("s3://bucket/blob-1")
            .setCreatedAt(Timestamps.fromMillis(1))
            .build();
    intentRepo.create(intent);

    var outcome = support.applyTransactionBestEffort(List.of(intent), intentRepo);

    assertEquals(TransactionIntentApplierSupport.ApplyStatus.APPLIED, outcome.status());
    assertTrue(
        intentRepo.getByTarget("acct", "/accounts/acct/custom/key-1").isPresent(),
        "intent entry should remain until transaction state is durably updated");
    assertEquals(1, intentRepo.listByTx("acct", "tx-1").size());
  }

  @Test
  void applyTransactionRejectsTableIntentTargetMismatch() throws Exception {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var intentRepo = new TransactionIntentRepository(pointers, blobs);

    var support = new TransactionIntentApplierSupport();
    inject(support, "pointerStore", pointers);
    inject(support, "blobStore", blobs);

    String blobUri = "s3://bucket/table-a";
    Table tablePayload =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setAccountId("acct").setId("table-a"))
            .setCatalogId(ResourceId.newBuilder().setAccountId("acct").setId("cat-1"))
            .setNamespaceId(ResourceId.newBuilder().setAccountId("acct").setId("ns-1"))
            .setDisplayName("orders")
            .build();
    blobs.put(blobUri, tablePayload.toByteArray(), "application/x-protobuf");

    String mismatchedPointerKey = Keys.tablePointerById("acct", "table-b");
    TransactionIntent intent =
        TransactionIntent.newBuilder()
            .setAccountId("acct")
            .setTxId("tx-1")
            .setTargetPointerKey(mismatchedPointerKey)
            .setBlobUri(blobUri)
            .setCreatedAt(Timestamps.fromMillis(1))
            .build();

    var outcome = support.applyTransactionBestEffort(List.of(intent), intentRepo);

    assertEquals(TransactionIntentApplierSupport.ApplyStatus.CONFLICT, outcome.status());
    assertEquals("TABLE_INTENT_TARGET_MISMATCH", outcome.errorCode());
  }

  @Test
  void applyTransactionReportsNamePointerConflictWithoutPartialApply() throws Exception {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var intentRepo = new TransactionIntentRepository(pointers, blobs);

    var support = new TransactionIntentApplierSupport();
    inject(support, "pointerStore", pointers);
    inject(support, "blobStore", blobs);

    String accountId = "acct";
    String catalogId = "cat-1";
    String namespaceId = "ns-1";
    String tableAId = "table-a";
    String tableBId = "table-b";

    Table tableAOriginal =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setAccountId(accountId).setId(tableAId))
            .setCatalogId(ResourceId.newBuilder().setAccountId(accountId).setId(catalogId))
            .setNamespaceId(ResourceId.newBuilder().setAccountId(accountId).setId(namespaceId))
            .setDisplayName("orders-a")
            .build();
    String tableAOriginalBlob = "s3://bucket/table-a-original";
    blobs.put(tableAOriginalBlob, tableAOriginal.toByteArray(), "application/x-protobuf");

    String tableAByIdKey = Keys.tablePointerById(accountId, tableAId);
    pointers.compareAndSet(
        tableAByIdKey,
        0L,
        Pointer.newBuilder()
            .setKey(tableAByIdKey)
            .setBlobUri(tableAOriginalBlob)
            .setVersion(1L)
            .build());

    String contestedName = "orders-contested";
    Table tableB =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setAccountId(accountId).setId(tableBId))
            .setCatalogId(ResourceId.newBuilder().setAccountId(accountId).setId(catalogId))
            .setNamespaceId(ResourceId.newBuilder().setAccountId(accountId).setId(namespaceId))
            .setDisplayName(contestedName)
            .build();
    String tableBBlob = "s3://bucket/table-b";
    blobs.put(tableBBlob, tableB.toByteArray(), "application/x-protobuf");

    String contestedNameKey =
        Keys.tablePointerByName(accountId, catalogId, namespaceId, contestedName);
    pointers.compareAndSet(
        contestedNameKey,
        0L,
        Pointer.newBuilder()
            .setKey(contestedNameKey)
            .setBlobUri(tableBBlob)
            .setVersion(1L)
            .build());

    Table tableANext = tableAOriginal.toBuilder().setDisplayName(contestedName).build();
    String tableANextBlob = "s3://bucket/table-a-next";
    blobs.put(tableANextBlob, tableANext.toByteArray(), "application/x-protobuf");

    TransactionIntent intent =
        TransactionIntent.newBuilder()
            .setAccountId(accountId)
            .setTxId("tx-1")
            .setTargetPointerKey(tableAByIdKey)
            .setBlobUri(tableANextBlob)
            .setExpectedVersion(1L)
            .setCreatedAt(Timestamps.fromMillis(1))
            .build();

    var outcome = support.applyTransactionBestEffort(List.of(intent), intentRepo);

    assertEquals(TransactionIntentApplierSupport.ApplyStatus.CONFLICT, outcome.status());
    assertEquals("NAME_POINTER_CONFLICT", outcome.errorCode());
    assertEquals(tableBId, outcome.conflictOwner());
    assertEquals(
        tableAOriginalBlob,
        pointers.get(tableAByIdKey).orElseThrow().getBlobUri(),
        "table-by-id pointer must remain unchanged on name pointer conflict");
  }

  private static void inject(Object target, String field, Object value) throws Exception {
    Field f = target.getClass().getDeclaredField(field);
    f.setAccessible(true);
    f.set(target, value);
  }
}
