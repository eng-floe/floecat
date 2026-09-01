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
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.impl.TransactionIntentRepository;
import ai.floedb.floecat.service.repo.impl.TransactionRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.service.repo.util.MarkerStore;
import ai.floedb.floecat.service.transaction.impl.TransactionIntentApplierSupport;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.transaction.rpc.Transaction;
import ai.floedb.floecat.transaction.rpc.TransactionIntent;
import ai.floedb.floecat.transaction.rpc.TransactionState;
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

    var support = newSupport(pointers, blobs);

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

    var support = newSupport(pointers, blobs);

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

    var support = newSupport(pointers, blobs);

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
  void applyTransactionRejectsIntentWhenAccountDeletionIsAlreadyFenced() throws Exception {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var intentRepo = new TransactionIntentRepository(pointers, blobs);
    var support = newSupport(pointers, blobs);
    String targetKey = "/accounts/acct/custom/key-1";
    String markerKey = Keys.accountDeletionMarker("acct");
    List<ai.floedb.floecat.storage.spi.PointerStore.CasOp> creates = new ArrayList<>();
    creates.add(
        new ai.floedb.floecat.storage.spi.PointerStore.CasUpsert(
            markerKey, 0L, PointerReferences.opaqueMarkerPointer(markerKey, "deleting", 1L)));
    for (String shardKey : Keys.accountDeletionFenceShards("acct")) {
      creates.add(
          new ai.floedb.floecat.storage.spi.PointerStore.CasUpsert(
              shardKey, 0L, PointerReferences.opaqueMarkerPointer(shardKey, "deleting", 1L)));
    }
    assertTrue(pointers.compareAndSetBatch(creates));
    TransactionIntent intent =
        TransactionIntent.newBuilder()
            .setAccountId("acct")
            .setTxId("tx-1")
            .setTargetPointerKey(targetKey)
            .setBlobUri("s3://bucket/blob-1")
            .setCreatedAt(Timestamps.fromMillis(1))
            .build();

    var outcome = support.applyTransactionBestEffort(List.of(intent), intentRepo);

    assertEquals(TransactionIntentApplierSupport.ApplyStatus.CONFLICT, outcome.status());
    assertEquals("ACCOUNT_DELETION_IN_PROGRESS", outcome.errorCode());
    assertTrue(pointers.get(targetKey).isEmpty());
  }

  @Test
  void applyTransactionAtomicallyChecksFenceInsidePointerBatch() throws Exception {
    var pointers = new HookedPointerStore();
    var blobs = new InMemoryBlobStore();
    var intentRepo = new TransactionIntentRepository(pointers, blobs);
    var txRepo = new TransactionRepository(pointers, blobs);
    var support = newSupport(pointers, blobs);
    String accountId = "acct";
    String txId = "tx-1";
    String targetKey = "/accounts/acct/custom/key-1";
    Transaction currentTxn =
        Transaction.newBuilder()
            .setAccountId(accountId)
            .setTxId(txId)
            .setState(TransactionState.TS_APPLYING)
            .setUpdatedAt(Timestamps.fromMillis(1))
            .build();
    txRepo.create(currentTxn);
    long txPointerVersion = txRepo.metaFor(accountId, txId).getPointerVersion();
    TransactionIntent intent =
        TransactionIntent.newBuilder()
            .setAccountId(accountId)
            .setTxId(txId)
            .setTargetPointerKey(targetKey)
            .setBlobUri("s3://bucket/blob-1")
            .setCreatedAt(Timestamps.fromMillis(1))
            .build();
    intentRepo.create(intent);
    String markerKey = Keys.accountDeletionMarker(accountId);
    pointers.beforeBatch(
        () -> {
          List<ai.floedb.floecat.storage.spi.PointerStore.CasOp> creates = new ArrayList<>();
          creates.add(
              new ai.floedb.floecat.storage.spi.PointerStore.CasUpsert(
                  markerKey, 0L, PointerReferences.opaqueMarkerPointer(markerKey, "deleting", 1L)));
          for (String shardKey : Keys.accountDeletionFenceShards(accountId)) {
            creates.add(
                new ai.floedb.floecat.storage.spi.PointerStore.CasUpsert(
                    shardKey, 0L, PointerReferences.opaqueMarkerPointer(shardKey, "deleting", 1L)));
          }
          pointers.compareAndSetBatch(creates);
        });
    Transaction appliedTxn =
        currentTxn.toBuilder()
            .setState(TransactionState.TS_APPLIED)
            .setUpdatedAt(Timestamps.fromMillis(2))
            .build();

    var outcome =
        support.applyTransactionAtomically(
            appliedTxn, txPointerVersion, List.of(intent), intentRepo);

    assertEquals(TransactionIntentApplierSupport.ApplyStatus.CONFLICT, outcome.status());
    assertEquals("ACCOUNT_DELETION_IN_PROGRESS", outcome.errorCode());
    assertTrue(pointers.get(targetKey).isEmpty());
    assertTrue(intentRepo.getByTarget(accountId, targetKey).isPresent());
    assertEquals(
        TransactionState.TS_APPLYING, txRepo.getById(accountId, txId).orElseThrow().getState());
  }

  @Test
  void applyTransactionRejectsTableIntentTargetMismatch() throws Exception {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var intentRepo = new TransactionIntentRepository(pointers, blobs);

    var support = newSupport(pointers, blobs);

    String blobUri = "s3://bucket/table-a";
    Table tablePayload =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setAccountId("acct").setId("table-a"))
            .setCatalogId(
                ResourceId.newBuilder()
                    .setAccountId("acct")
                    .setId("cat-1")
                    .setKind(ResourceKind.RK_CATALOG))
            .setNamespaceId(
                ResourceId.newBuilder()
                    .setAccountId("acct")
                    .setId("ns-1")
                    .setKind(ResourceKind.RK_NAMESPACE))
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
  void applyTransactionRejectsSystemTablePayloadBeforePointerOps() throws Exception {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var intentRepo = new TransactionIntentRepository(pointers, blobs);

    var support = newSupport(pointers, blobs);

    ResourceId systemTableId =
        SystemNodeRegistry.resourceId("engine", ResourceKind.RK_TABLE, "information_schema.tables");
    String blobUri = "s3://bucket/system-table";
    Table tablePayload =
        Table.newBuilder()
            .setResourceId(systemTableId)
            .setCatalogId(
                ResourceId.newBuilder()
                    .setAccountId("acct")
                    .setId("cat-1")
                    .setKind(ResourceKind.RK_CATALOG))
            .setNamespaceId(
                ResourceId.newBuilder()
                    .setAccountId("acct")
                    .setId("ns-1")
                    .setKind(ResourceKind.RK_NAMESPACE))
            .setDisplayName("tables")
            .build();
    blobs.put(blobUri, tablePayload.toByteArray(), "application/x-protobuf");

    String pointerKey = Keys.tablePointerById(systemTableId.getAccountId(), systemTableId.getId());
    TransactionIntent intent =
        TransactionIntent.newBuilder()
            .setAccountId(systemTableId.getAccountId())
            .setTxId("tx-1")
            .setTargetPointerKey(pointerKey)
            .setBlobUri(blobUri)
            .setCreatedAt(Timestamps.fromMillis(1))
            .build();

    var outcome = support.applyTransactionBestEffort(List.of(intent), intentRepo);

    assertEquals(TransactionIntentApplierSupport.ApplyStatus.CONFLICT, outcome.status());
    assertEquals("SYSTEM_OBJECT_IMMUTABLE", outcome.errorCode());
    assertTrue(pointers.get(pointerKey).isEmpty(), "system table pointer must not be written");
  }

  @Test
  void applyTransactionDeletesNonTablePointerWhenDeleteSentinelIsUsed() throws Exception {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var intentRepo = new TransactionIntentRepository(pointers, blobs);

    var support = newSupport(pointers, blobs);

    String accountId = "acct";
    String targetKey = Keys.snapshotPointerById(accountId, "table-1", 7L);
    pointers.compareAndSet(
        targetKey, 0L, PointerReferences.blobPointer(targetKey, "/blob/snap-7", 1L));

    TransactionIntent intent =
        TransactionIntent.newBuilder()
            .setAccountId(accountId)
            .setTxId("tx-1")
            .setTargetPointerKey(targetKey)
            .setBlobUri(Keys.transactionDeleteSentinelUri(accountId, "tx-1", targetKey))
            .setExpectedVersion(1L)
            .setCreatedAt(Timestamps.fromMillis(1))
            .build();

    var outcome = support.applyTransactionBestEffort(List.of(intent), intentRepo);

    assertEquals(TransactionIntentApplierSupport.ApplyStatus.APPLIED, outcome.status());
    assertTrue(pointers.get(targetKey).isEmpty(), "delete intent should remove the target pointer");
  }

  @Test
  void applyTransactionDeletesTablePointerAndOwnedNamePointerWhenDeleteSentinelIsUsed()
      throws Exception {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var intentRepo = new TransactionIntentRepository(pointers, blobs);

    var support = newSupport(pointers, blobs);

    String accountId = "acct";
    String catalogId = "cat-1";
    String namespaceId = "ns-1";
    seedNamespace(pointers, accountId, namespaceId);
    String tableId = "table-1";
    String blobUri = "/accounts/acct/tables/table-1/table/blob.pb";
    String byIdKey = Keys.tablePointerById(accountId, tableId);
    String byNameKey = Keys.tablePointerByName(accountId, catalogId, namespaceId, "orders");

    Table table =
        Table.newBuilder()
            .setResourceId(
                ResourceId.newBuilder()
                    .setAccountId(accountId)
                    .setId(tableId)
                    .setKind(ResourceKind.RK_TABLE))
            .setCatalogId(
                ResourceId.newBuilder()
                    .setAccountId(accountId)
                    .setId(catalogId)
                    .setKind(ResourceKind.RK_CATALOG))
            .setNamespaceId(
                ResourceId.newBuilder()
                    .setAccountId(accountId)
                    .setId(namespaceId)
                    .setKind(ResourceKind.RK_NAMESPACE))
            .setDisplayName("orders")
            .build();
    blobs.put(blobUri, table.toByteArray(), "application/x-protobuf");
    pointers.compareAndSet(byIdKey, 0L, PointerReferences.blobPointer(byIdKey, blobUri, 1L));
    pointers.compareAndSet(byNameKey, 0L, PointerReferences.blobPointer(byNameKey, blobUri, 1L));

    TransactionIntent intent =
        TransactionIntent.newBuilder()
            .setAccountId(accountId)
            .setTxId("tx-1")
            .setTargetPointerKey(byIdKey)
            .setBlobUri(Keys.transactionDeleteSentinelUri(accountId, "tx-1", byIdKey))
            .setExpectedVersion(1L)
            .setCreatedAt(Timestamps.fromMillis(1))
            .build();

    var outcome = support.applyTransactionBestEffort(List.of(intent), intentRepo);

    assertEquals(TransactionIntentApplierSupport.ApplyStatus.APPLIED, outcome.status());
    assertTrue(pointers.get(byIdKey).isEmpty(), "table by-id pointer should be removed");
    assertTrue(pointers.get(byNameKey).isEmpty(), "owned table by-name pointer should be removed");
  }

  @Test
  void applyTransactionReportsNamePointerConflictWithoutPartialApply() throws Exception {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var intentRepo = new TransactionIntentRepository(pointers, blobs);

    var support = newSupport(pointers, blobs);

    String accountId = "acct";
    String catalogId = "cat-1";
    String namespaceId = "ns-1";
    seedNamespace(pointers, accountId, namespaceId);
    String tableAId = "table-a";
    String tableBId = "table-b";

    Table tableAOriginal =
        Table.newBuilder()
            .setResourceId(
                ResourceId.newBuilder()
                    .setAccountId(accountId)
                    .setId(tableAId)
                    .setKind(ResourceKind.RK_TABLE))
            .setCatalogId(
                ResourceId.newBuilder()
                    .setAccountId(accountId)
                    .setId(catalogId)
                    .setKind(ResourceKind.RK_CATALOG))
            .setNamespaceId(
                ResourceId.newBuilder()
                    .setAccountId(accountId)
                    .setId(namespaceId)
                    .setKind(ResourceKind.RK_NAMESPACE))
            .setDisplayName("orders-a")
            .build();
    String tableAOriginalBlob = "s3://bucket/table-a-original";
    blobs.put(tableAOriginalBlob, tableAOriginal.toByteArray(), "application/x-protobuf");

    String tableAByIdKey = Keys.tablePointerById(accountId, tableAId);
    pointers.compareAndSet(
        tableAByIdKey, 0L, PointerReferences.blobPointer(tableAByIdKey, tableAOriginalBlob, 1L));

    String contestedName = "orders-contested";
    Table tableB =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setAccountId(accountId).setId(tableBId))
            .setCatalogId(
                ResourceId.newBuilder()
                    .setAccountId(accountId)
                    .setId(catalogId)
                    .setKind(ResourceKind.RK_CATALOG))
            .setNamespaceId(
                ResourceId.newBuilder()
                    .setAccountId(accountId)
                    .setId(namespaceId)
                    .setKind(ResourceKind.RK_NAMESPACE))
            .setDisplayName(contestedName)
            .build();
    String tableBBlob = "s3://bucket/table-b";
    blobs.put(tableBBlob, tableB.toByteArray(), "application/x-protobuf");

    String contestedNameKey =
        Keys.tablePointerByName(accountId, catalogId, namespaceId, contestedName);
    pointers.compareAndSet(
        contestedNameKey, 0L, PointerReferences.blobPointer(contestedNameKey, tableBBlob, 1L));

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

  @Test
  void applyTransactionAtomicallyLeavesTargetAndIntentsUnchangedWhenFinalizeCasFails()
      throws Exception {
    var pointers = new HookedPointerStore();
    var blobs = new InMemoryBlobStore();
    var intentRepo = new TransactionIntentRepository(pointers, blobs);
    var txRepo = new TransactionRepository(pointers, blobs);

    var support = newSupport(pointers, blobs);

    String accountId = "acct";
    String txId = "tx-1";
    String targetKey = "/accounts/acct/custom/key-1";
    String currentBlob = "s3://bucket/blob-current";
    String nextBlob = "s3://bucket/blob-next";
    pointers.compareAndSet(
        targetKey, 0L, PointerReferences.blobPointer(targetKey, currentBlob, 1L));

    Transaction currentTxn =
        Transaction.newBuilder()
            .setAccountId(accountId)
            .setTxId(txId)
            .setState(TransactionState.TS_APPLYING)
            .setUpdatedAt(Timestamps.fromMillis(1))
            .build();
    txRepo.create(currentTxn);
    long txPointerVersion = txRepo.metaFor(accountId, txId).getPointerVersion();
    String txKey = Keys.transactionPointerById(accountId, txId);

    TransactionIntent intent =
        TransactionIntent.newBuilder()
            .setAccountId(accountId)
            .setTxId(txId)
            .setTargetPointerKey(targetKey)
            .setBlobUri(nextBlob)
            .setExpectedVersion(1L)
            .setCreatedAt(Timestamps.fromMillis(1))
            .build();
    intentRepo.create(intent);

    Transaction conflictingTxn =
        currentTxn.toBuilder().setUpdatedAt(Timestamps.fromMillis(2)).build();
    String conflictingBlob = Keys.transactionBlobUri(accountId, txId, "competing-finalize");
    blobs.put(conflictingBlob, conflictingTxn.toByteArray(), "application/x-protobuf");
    pointers.beforeBatch(
        () ->
            pointers.compareAndSet(
                txKey,
                txPointerVersion,
                PointerReferences.blobPointer(txKey, conflictingBlob, 0L)));

    Transaction appliedTxn =
        currentTxn.toBuilder()
            .setState(TransactionState.TS_APPLIED)
            .setUpdatedAt(Timestamps.fromMillis(10))
            .build();
    var outcome =
        support.applyTransactionAtomically(
            appliedTxn, txPointerVersion, List.of(intent), intentRepo);

    assertEquals(TransactionIntentApplierSupport.ApplyStatus.RETRYABLE, outcome.status());
    assertEquals(currentBlob, pointers.get(targetKey).orElseThrow().getBlobUri());
    assertEquals(conflictingBlob, pointers.get(txKey).orElseThrow().getBlobUri());
    assertTrue(intentRepo.getByTarget(accountId, targetKey).isPresent());
    assertEquals(1, intentRepo.listByTx(accountId, txId).size());
  }

  @Test
  void applyTransactionAtomicallyLeavesTargetAndTransactionUnchangedWhenIntentCleanupCasFails()
      throws Exception {
    var pointers = new HookedPointerStore();
    var blobs = new InMemoryBlobStore();
    var intentRepo = new TransactionIntentRepository(pointers, blobs);
    var txRepo = new TransactionRepository(pointers, blobs);

    var support = newSupport(pointers, blobs);

    String accountId = "acct";
    String txId = "tx-1";
    String targetKey = "/accounts/acct/custom/key-1";
    String currentBlob = "s3://bucket/blob-current";
    String nextBlob = "s3://bucket/blob-next";
    pointers.compareAndSet(
        targetKey, 0L, PointerReferences.blobPointer(targetKey, currentBlob, 1L));

    Transaction currentTxn =
        Transaction.newBuilder()
            .setAccountId(accountId)
            .setTxId(txId)
            .setState(TransactionState.TS_APPLYING)
            .setUpdatedAt(Timestamps.fromMillis(1))
            .build();
    txRepo.create(currentTxn);
    long txPointerVersion = txRepo.metaFor(accountId, txId).getPointerVersion();
    String txKey = Keys.transactionPointerById(accountId, txId);

    TransactionIntent intent =
        TransactionIntent.newBuilder()
            .setAccountId(accountId)
            .setTxId(txId)
            .setTargetPointerKey(targetKey)
            .setBlobUri(nextBlob)
            .setExpectedVersion(1L)
            .setCreatedAt(Timestamps.fromMillis(1))
            .build();
    intentRepo.create(intent);

    String byTxKey = Keys.transactionIntentPointerByTx(accountId, txId, targetKey);
    pointers.beforeBatch(
        () -> {
          var byTxPointer = pointers.get(byTxKey).orElseThrow();
          pointers.compareAndSet(byTxKey, byTxPointer.getVersion(), byTxPointer);
        });

    Transaction appliedTxn =
        currentTxn.toBuilder()
            .setState(TransactionState.TS_APPLIED)
            .setUpdatedAt(Timestamps.fromMillis(10))
            .build();
    var outcome =
        support.applyTransactionAtomically(
            appliedTxn, txPointerVersion, List.of(intent), intentRepo);

    assertEquals(TransactionIntentApplierSupport.ApplyStatus.RETRYABLE, outcome.status());
    assertEquals(currentBlob, pointers.get(targetKey).orElseThrow().getBlobUri());
    assertEquals(
        TransactionState.TS_APPLYING,
        readTransaction(blobs, pointers.get(txKey).orElseThrow().getBlobUri()).getState());
    assertTrue(intentRepo.getByTarget(accountId, targetKey).isPresent());
    assertEquals(1, intentRepo.listByTx(accountId, txId).size());
  }

  @Test
  void applyTransactionAtomicallyProtectsNoOpTargetWithCasCheck() throws Exception {
    var pointers = new HookedPointerStore();
    var blobs = new InMemoryBlobStore();
    var intentRepo = new TransactionIntentRepository(pointers, blobs);
    var txRepo = new TransactionRepository(pointers, blobs);

    var support = newSupport(pointers, blobs);

    String accountId = "acct";
    String txId = "tx-1";
    String targetKey = "/accounts/acct/custom/key-1";
    String desiredBlob = "s3://bucket/blob-desired";
    String competingBlob = "s3://bucket/blob-competing";
    pointers.compareAndSet(
        targetKey, 0L, PointerReferences.blobPointer(targetKey, desiredBlob, 1L));

    Transaction currentTxn =
        Transaction.newBuilder()
            .setAccountId(accountId)
            .setTxId(txId)
            .setState(TransactionState.TS_APPLYING)
            .setUpdatedAt(Timestamps.fromMillis(1))
            .build();
    txRepo.create(currentTxn);
    long txPointerVersion = txRepo.metaFor(accountId, txId).getPointerVersion();

    TransactionIntent intent =
        TransactionIntent.newBuilder()
            .setAccountId(accountId)
            .setTxId(txId)
            .setTargetPointerKey(targetKey)
            .setBlobUri(desiredBlob)
            .setExpectedVersion(1L)
            .setCreatedAt(Timestamps.fromMillis(1))
            .build();
    intentRepo.create(intent);

    pointers.beforeBatch(
        () ->
            pointers.compareAndSet(
                targetKey, 1L, PointerReferences.blobPointer(targetKey, competingBlob, 0L)));

    Transaction appliedTxn =
        currentTxn.toBuilder()
            .setState(TransactionState.TS_APPLIED)
            .setUpdatedAt(Timestamps.fromMillis(10))
            .build();
    var outcome =
        support.applyTransactionAtomically(
            appliedTxn, txPointerVersion, List.of(intent), intentRepo);

    assertEquals(TransactionIntentApplierSupport.ApplyStatus.CONFLICT, outcome.status());
    assertEquals("EXPECTED_VERSION_MISMATCH", outcome.errorCode());
    assertEquals(competingBlob, pointers.get(targetKey).orElseThrow().getBlobUri());
    assertTrue(intentRepo.getByTarget(accountId, targetKey).isPresent());
    assertEquals(1, intentRepo.listByTx(accountId, txId).size());
  }

  @Test
  void applyTransactionAtomicallyProtectsAbsentDeleteWithCasCheckAbsent() throws Exception {
    var pointers = new HookedPointerStore();
    var blobs = new InMemoryBlobStore();
    var intentRepo = new TransactionIntentRepository(pointers, blobs);
    var txRepo = new TransactionRepository(pointers, blobs);

    var support = newSupport(pointers, blobs);

    String accountId = "acct";
    String txId = "tx-1";
    String targetKey = "/accounts/acct/custom/key-1";
    String competingBlob = "s3://bucket/blob-competing";

    Transaction currentTxn =
        Transaction.newBuilder()
            .setAccountId(accountId)
            .setTxId(txId)
            .setState(TransactionState.TS_APPLYING)
            .setUpdatedAt(Timestamps.fromMillis(1))
            .build();
    txRepo.create(currentTxn);
    long txPointerVersion = txRepo.metaFor(accountId, txId).getPointerVersion();

    TransactionIntent intent =
        TransactionIntent.newBuilder()
            .setAccountId(accountId)
            .setTxId(txId)
            .setTargetPointerKey(targetKey)
            .setBlobUri(Keys.transactionDeleteSentinelUri(accountId, txId, targetKey))
            .setExpectedVersion(0L)
            .setCreatedAt(Timestamps.fromMillis(1))
            .build();
    intentRepo.create(intent);

    pointers.beforeBatch(
        () ->
            pointers.compareAndSet(
                targetKey, 0L, PointerReferences.blobPointer(targetKey, competingBlob, 0L)));

    Transaction appliedTxn =
        currentTxn.toBuilder()
            .setState(TransactionState.TS_APPLIED)
            .setUpdatedAt(Timestamps.fromMillis(10))
            .build();
    var outcome =
        support.applyTransactionAtomically(
            appliedTxn, txPointerVersion, List.of(intent), intentRepo);

    assertEquals(TransactionIntentApplierSupport.ApplyStatus.CONFLICT, outcome.status());
    assertEquals("EXPECTED_VERSION_MISMATCH", outcome.errorCode());
    assertEquals(competingBlob, pointers.get(targetKey).orElseThrow().getBlobUri());
    assertTrue(intentRepo.getByTarget(accountId, targetKey).isPresent());
    assertEquals(1, intentRepo.listByTx(accountId, txId).size());
  }

  @Test
  void applyTransactionClaimsSharedRelationNamePointerOnCreate() throws Exception {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var intentRepo = new TransactionIntentRepository(pointers, blobs);

    var support = newSupport(pointers, blobs);

    String accountId = "acct";
    String catalogId = "cat-1";
    String namespaceId = "ns-1";
    seedNamespace(pointers, accountId, namespaceId);
    String tableId = "table-1";
    String byIdKey = Keys.tablePointerById(accountId, tableId);
    String relationKey = Keys.relationPointerByName(accountId, catalogId, namespaceId, "orders");

    Table table =
        Table.newBuilder()
            .setResourceId(
                ResourceId.newBuilder()
                    .setAccountId(accountId)
                    .setId(tableId)
                    .setKind(ResourceKind.RK_TABLE))
            .setCatalogId(
                ResourceId.newBuilder()
                    .setAccountId(accountId)
                    .setId(catalogId)
                    .setKind(ResourceKind.RK_CATALOG))
            .setNamespaceId(
                ResourceId.newBuilder()
                    .setAccountId(accountId)
                    .setId(namespaceId)
                    .setKind(ResourceKind.RK_NAMESPACE))
            .setDisplayName("orders")
            .build();
    String blobUri = "/accounts/acct/tables/table-1/table/blob.pb";
    blobs.put(blobUri, table.toByteArray(), "application/x-protobuf");

    TransactionIntent intent =
        TransactionIntent.newBuilder()
            .setAccountId(accountId)
            .setTxId("tx-1")
            .setTargetPointerKey(byIdKey)
            .setBlobUri(blobUri)
            .setCreatedAt(Timestamps.fromMillis(1))
            .build();

    var outcome = support.applyTransactionBestEffort(List.of(intent), intentRepo);

    assertEquals(TransactionIntentApplierSupport.ApplyStatus.APPLIED, outcome.status());
    assertTrue(pointers.get(relationKey).isPresent(), "shared relation-name claim must be created");
    assertEquals(tableId, pointers.get(relationKey).orElseThrow().getResourceId().getId());
  }

  /**
   * A transactional table create must advance its namespace's relation marker inside the same
   * pointer batch.
   *
   * <p>Exclusion between this batch and a DeleteNamespace batch is only key overlap.
   * DeleteNamespace asserts that marker to prove the namespace holds nothing; if this batch never
   * touches the key, the two share none, neither can lose to the other, and the delete commits
   * while the table lands -- leaving a table addressable under a namespace id that no longer
   * exists.
   *
   * <p>Asserted as participation because that is the deterministic half: the marker moving is what
   * a concurrent delete contends with.
   */
  @Test
  void applyTransactionAdvancesTheNamespaceRelationMarkerOnTableCreate() throws Exception {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var intentRepo = new TransactionIntentRepository(pointers, blobs);
    var support = newSupport(pointers, blobs);

    String accountId = "acct";
    String catalogId = "cat-1";
    String namespaceId = "ns-1";
    seedNamespace(pointers, accountId, namespaceId);
    String tableId = "table-1";
    String markerKey = Keys.namespaceRelationsMarker(accountId, namespaceId);
    long before = pointers.get(markerKey).map(Pointer::getVersion).orElse(0L);

    Table table =
        Table.newBuilder()
            .setResourceId(
                ResourceId.newBuilder()
                    .setAccountId(accountId)
                    .setId(tableId)
                    .setKind(ResourceKind.RK_TABLE))
            .setCatalogId(
                ResourceId.newBuilder()
                    .setAccountId(accountId)
                    .setId(catalogId)
                    .setKind(ResourceKind.RK_CATALOG))
            .setNamespaceId(
                ResourceId.newBuilder()
                    .setAccountId(accountId)
                    .setId(namespaceId)
                    .setKind(ResourceKind.RK_NAMESPACE))
            .setDisplayName("orders")
            .build();
    String blobUri = "/accounts/acct/tables/table-1/table/blob.pb";
    blobs.put(blobUri, table.toByteArray(), "application/x-protobuf");

    TransactionIntent intent =
        TransactionIntent.newBuilder()
            .setAccountId(accountId)
            .setTxId("tx-1")
            .setTargetPointerKey(Keys.tablePointerById(accountId, tableId))
            .setBlobUri(blobUri)
            .setCreatedAt(Timestamps.fromMillis(1))
            .build();

    var outcome = support.applyTransactionBestEffort(List.of(intent), intentRepo);

    assertEquals(TransactionIntentApplierSupport.ApplyStatus.APPLIED, outcome.status());
    assertTrue(
        pointers.get(markerKey).map(Pointer::getVersion).orElse(0L) > before,
        "a transactional table create must advance the namespace's relation marker, or a namespace"
            + " delete cannot exclude it");
  }

  /** A namespace delete cannot share a transaction with a table created inside that namespace. */
  @Test
  void applyTransactionRejectsNamespaceDeleteBeforeTableCreateInThatNamespace() throws Exception {
    assertNamespaceDeleteAndTableCreateConflict(true);
  }

  /** The same collision is rejected when the table intent is planned first. */
  @Test
  void applyTransactionRejectsNamespaceDeleteAfterTableCreateInThatNamespace() throws Exception {
    assertNamespaceDeleteAndTableCreateConflict(false);
  }

  private void assertNamespaceDeleteAndTableCreateConflict(boolean deleteFirst) throws Exception {
    var fixture = newApplyFixture();
    var pointers = fixture.pointers();
    var blobs = fixture.blobs();

    String accountId = "acct";
    String txId = "tx-1";
    String namespaceId = "ns-1";
    String namespaceKey = Keys.namespacePointerById(accountId, namespaceId);
    seedNamespace(pointers, accountId, namespaceId);

    String tableId = "table-1";
    String tableKey = Keys.tablePointerById(accountId, tableId);
    Table table = table(accountId, "cat-1", namespaceId, tableId, "orders");
    String tableBlobUri = "/accounts/acct/tables/table-1/table/blob.pb";
    blobs.put(tableBlobUri, table.toByteArray(), "application/x-protobuf");

    TransactionIntent namespaceDelete =
        TransactionIntent.newBuilder()
            .setAccountId(accountId)
            .setTxId(txId)
            .setTargetPointerKey(namespaceKey)
            .setBlobUri(Keys.transactionDeleteSentinelUri(accountId, txId, namespaceKey))
            .setCreatedAt(Timestamps.fromMillis(1))
            .build();
    TransactionIntent tableCreate = tableCreateIntent(accountId, txId, tableId, tableBlobUri, 2L);

    var outcome =
        fixture
            .support()
            .applyTransactionBestEffort(
                deleteFirst
                    ? List.of(namespaceDelete, tableCreate)
                    : List.of(tableCreate, namespaceDelete),
                fixture.intentRepo());

    assertEquals(TransactionIntentApplierSupport.ApplyStatus.CONFLICT, outcome.status());
    assertEquals("POINTER_TXN_DUPLICATE_KEY", outcome.errorCode());
    assertTrue(pointers.get(namespaceKey).isPresent(), "the namespace delete must not commit");
    assertTrue(pointers.get(tableKey).isEmpty(), "the orphaned table must not be created");
  }

  /** Two table creates may share an identical namespace fence in one transaction. */
  @Test
  void applyTransactionDeduplicatesIdenticalNamespaceJoinConditions() throws Exception {
    var fixture = newApplyFixture();
    var pointers = fixture.pointers();
    var blobs = fixture.blobs();

    String accountId = "acct";
    String namespaceId = "ns-1";
    seedNamespace(pointers, accountId, namespaceId);
    Table orders = table(accountId, "cat-1", namespaceId, "table-1", "orders");
    Table invoices = table(accountId, "cat-1", namespaceId, "table-2", "invoices");
    String ordersBlob = "/accounts/acct/tables/table-1/table/blob.pb";
    String invoicesBlob = "/accounts/acct/tables/table-2/table/blob.pb";
    blobs.put(ordersBlob, orders.toByteArray(), "application/x-protobuf");
    blobs.put(invoicesBlob, invoices.toByteArray(), "application/x-protobuf");

    TransactionIntent createOrders =
        tableCreateIntent(accountId, "tx-1", "table-1", ordersBlob, 1L);
    TransactionIntent createInvoices =
        tableCreateIntent(accountId, "tx-1", "table-2", invoicesBlob, 2L);

    var outcome =
        fixture
            .support()
            .applyTransactionBestEffort(
                List.of(createOrders, createInvoices), fixture.intentRepo());

    assertEquals(TransactionIntentApplierSupport.ApplyStatus.APPLIED, outcome.status());
    assertTrue(pointers.get(Keys.tablePointerById(accountId, "table-1")).isPresent());
    assertTrue(pointers.get(Keys.tablePointerById(accountId, "table-2")).isPresent());
    assertEquals(
        1L,
        pointers
            .get(Keys.namespaceRelationsMarker(accountId, namespaceId))
            .orElseThrow()
            .getVersion());
  }

  /**
   * A table intent whose namespace is already gone is refused, not applied.
   *
   * <p>The one branch of the join that is not the happy path, and nothing covered it before or
   * after the applier moved onto the shared fence -- where "already gone" changed from a
   * version-zero read to a caught NotFoundException. Applying anyway would put a table under a
   * namespace that no longer exists, which is the whole point of the read dependency.
   */
  @Test
  void applyTransactionRefusesATableWhoseNamespaceIsAlreadyGone() throws Exception {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var intentRepo = new TransactionIntentRepository(pointers, blobs);
    var support = newSupport(pointers, blobs);

    String accountId = "acct";
    String tableId = "table-1";
    // Deliberately no seedNamespace: the namespace has no canonical pointer.
    Table table =
        Table.newBuilder()
            .setResourceId(
                ResourceId.newBuilder()
                    .setAccountId(accountId)
                    .setId(tableId)
                    .setKind(ResourceKind.RK_TABLE))
            .setCatalogId(
                ResourceId.newBuilder()
                    .setAccountId(accountId)
                    .setId("cat-1")
                    .setKind(ResourceKind.RK_CATALOG))
            .setNamespaceId(
                ResourceId.newBuilder()
                    .setAccountId(accountId)
                    .setId("ns-gone")
                    .setKind(ResourceKind.RK_NAMESPACE))
            .setDisplayName("orders")
            .build();
    String blobUri = "/accounts/acct/tables/table-1/table/blob.pb";
    blobs.put(blobUri, table.toByteArray(), "application/x-protobuf");

    TransactionIntent intent =
        TransactionIntent.newBuilder()
            .setAccountId(accountId)
            .setTxId("tx-1")
            .setTargetPointerKey(Keys.tablePointerById(accountId, tableId))
            .setBlobUri(blobUri)
            .setCreatedAt(Timestamps.fromMillis(1))
            .build();

    var outcome = support.applyTransactionBestEffort(List.of(intent), intentRepo);

    assertEquals(TransactionIntentApplierSupport.ApplyStatus.CONFLICT, outcome.status());
    assertEquals("NAMESPACE_NOT_FOUND", outcome.errorCode());
    assertTrue(
        pointers.get(Keys.tablePointerById(accountId, tableId)).isEmpty(),
        "nothing is written for a table whose namespace is gone");
  }

  @Test
  void applyTransactionRejectsTableCreateWhenViewHoldsRelationName() throws Exception {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var intentRepo = new TransactionIntentRepository(pointers, blobs);

    var support = newSupport(pointers, blobs);

    String accountId = "acct";
    String catalogId = "cat-1";
    String namespaceId = "ns-1";
    seedNamespace(pointers, accountId, namespaceId);
    String relationKey = Keys.relationPointerByName(accountId, catalogId, namespaceId, "orders");

    // A view already owns the shared relation-name claim for "orders".
    ResourceId viewId =
        ResourceId.newBuilder()
            .setAccountId(accountId)
            .setId("view-9")
            .setKind(ResourceKind.RK_VIEW)
            .build();
    String viewBlob = "/accounts/acct/views/view-9/view/blob.pb";
    pointers.compareAndSet(
        relationKey,
        0L,
        PointerReferences.blobPointer(relationKey, viewBlob, 1L, viewId, "orders"));

    String tableId = "table-1";
    String byIdKey = Keys.tablePointerById(accountId, tableId);
    Table table =
        Table.newBuilder()
            .setResourceId(
                ResourceId.newBuilder()
                    .setAccountId(accountId)
                    .setId(tableId)
                    .setKind(ResourceKind.RK_TABLE))
            .setCatalogId(
                ResourceId.newBuilder()
                    .setAccountId(accountId)
                    .setId(catalogId)
                    .setKind(ResourceKind.RK_CATALOG))
            .setNamespaceId(
                ResourceId.newBuilder()
                    .setAccountId(accountId)
                    .setId(namespaceId)
                    .setKind(ResourceKind.RK_NAMESPACE))
            .setDisplayName("orders")
            .build();
    String blobUri = "/accounts/acct/tables/table-1/table/blob.pb";
    blobs.put(blobUri, table.toByteArray(), "application/x-protobuf");

    TransactionIntent intent =
        TransactionIntent.newBuilder()
            .setAccountId(accountId)
            .setTxId("tx-1")
            .setTargetPointerKey(byIdKey)
            .setBlobUri(blobUri)
            .setCreatedAt(Timestamps.fromMillis(1))
            .build();

    var outcome = support.applyTransactionBestEffort(List.of(intent), intentRepo);

    assertEquals(TransactionIntentApplierSupport.ApplyStatus.CONFLICT, outcome.status());
    assertEquals("RELATION_NAME_CONFLICT", outcome.errorCode());
    assertTrue(
        pointers.get(byIdKey).isEmpty(), "table by-id pointer must not be created on conflict");
  }

  /**
   * An update that leaves a table in the container it is already counted in moves no marker.
   *
   * <p>The applier used to assert on every table upsert, so a schema edit, a property change or an
   * idempotent replay contended on the namespace's relation marker with every other table commit
   * into that namespace -- and, once the commit path grew a local retry, burned that budget. The
   * relation set is unchanged, so a concurrent namespace delete counts this table and refuses
   * either way. Creates still assert: see the marker-advance test above.
   */
  @Test
  void applyTransactionDoesNotAdvanceTheMarkerForAnUpdateThatStaysPut() throws Exception {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var intentRepo = new TransactionIntentRepository(pointers, blobs);
    var support = newSupport(pointers, blobs);

    String accountId = "acct";
    String catalogId = "cat-1";
    String namespaceId = "ns-1";
    seedNamespace(pointers, accountId, namespaceId);
    String markerKey = Keys.namespaceRelationsMarker(accountId, namespaceId);

    ResourceId tableRid =
        ResourceId.newBuilder()
            .setAccountId(accountId)
            .setId("table-1")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    Table current =
        Table.newBuilder()
            .setResourceId(tableRid)
            .setCatalogId(
                ResourceId.newBuilder()
                    .setAccountId(accountId)
                    .setId(catalogId)
                    .setKind(ResourceKind.RK_CATALOG))
            .setNamespaceId(
                ResourceId.newBuilder()
                    .setAccountId(accountId)
                    .setId(namespaceId)
                    .setKind(ResourceKind.RK_NAMESPACE))
            .setDisplayName("orders")
            .build();
    String byIdKey = Keys.tablePointerById(accountId, tableRid.getId());
    String nameKey = Keys.tablePointerByName(accountId, catalogId, namespaceId, "orders");
    String claimKey = Keys.relationPointerByName(accountId, catalogId, namespaceId, "orders");
    String currentBlob = "/accounts/acct/tables/table-1/table/current.pb";
    blobs.put(currentBlob, current.toByteArray(), "application/x-protobuf");
    pointers.compareAndSet(byIdKey, 0L, PointerReferences.blobPointer(byIdKey, currentBlob, 1L));
    pointers.compareAndSet(nameKey, 0L, PointerReferences.blobPointer(nameKey, currentBlob, 1L));
    pointers.compareAndSet(
        claimKey,
        0L,
        PointerReferences.blobPointer(claimKey, currentBlob, 1L).toBuilder()
            .setResourceId(tableRid)
            .build());

    // Same catalog, same namespace, same name: only the schema changes.
    Table next = current.toBuilder().setSchemaJson("{\"type\":\"struct\"}").build();
    String nextBlob = "/accounts/acct/tables/table-1/table/next.pb";
    blobs.put(nextBlob, next.toByteArray(), "application/x-protobuf");

    long markerBefore = pointers.get(markerKey).map(Pointer::getVersion).orElse(0L);

    TransactionIntent intent =
        TransactionIntent.newBuilder()
            .setAccountId(accountId)
            .setTxId("tx-1")
            .setTargetPointerKey(byIdKey)
            .setBlobUri(nextBlob)
            .setCreatedAt(Timestamps.fromMillis(1))
            .build();

    var outcome = support.applyTransactionBestEffort(List.of(intent), intentRepo);

    assertEquals(TransactionIntentApplierSupport.ApplyStatus.APPLIED, outcome.status());
    assertEquals(
        markerBefore,
        pointers.get(markerKey).map(Pointer::getVersion).orElse(0L),
        "the relation set did not change, so nothing should contend on its marker");
  }

  @Test
  void applyTransactionMovesRelationClaimOnRename() throws Exception {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var intentRepo = new TransactionIntentRepository(pointers, blobs);

    var support = newSupport(pointers, blobs);

    String accountId = "acct";
    String catalogId = "cat-1";
    String namespaceId = "ns-1";
    seedNamespace(pointers, accountId, namespaceId);
    ResourceId tableRid =
        ResourceId.newBuilder()
            .setAccountId(accountId)
            .setId("table-1")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    String byIdKey = Keys.tablePointerById(accountId, tableRid.getId());
    String oldNameKey = Keys.tablePointerByName(accountId, catalogId, namespaceId, "orders");
    String oldClaimKey = Keys.relationPointerByName(accountId, catalogId, namespaceId, "orders");
    String newNameKey = Keys.tablePointerByName(accountId, catalogId, namespaceId, "invoices");
    String newClaimKey = Keys.relationPointerByName(accountId, catalogId, namespaceId, "invoices");

    Table current =
        Table.newBuilder()
            .setResourceId(tableRid)
            .setCatalogId(
                ResourceId.newBuilder()
                    .setAccountId(accountId)
                    .setId(catalogId)
                    .setKind(ResourceKind.RK_CATALOG))
            .setNamespaceId(
                ResourceId.newBuilder()
                    .setAccountId(accountId)
                    .setId(namespaceId)
                    .setKind(ResourceKind.RK_NAMESPACE))
            .setDisplayName("orders")
            .build();
    String currentBlob = "/accounts/acct/tables/table-1/table/current.pb";
    blobs.put(currentBlob, current.toByteArray(), "application/x-protobuf");
    pointers.compareAndSet(byIdKey, 0L, PointerReferences.blobPointer(byIdKey, currentBlob, 1L));
    pointers.compareAndSet(
        oldNameKey, 0L, PointerReferences.blobPointer(oldNameKey, currentBlob, 1L));
    pointers.compareAndSet(
        oldClaimKey,
        0L,
        PointerReferences.blobPointer(oldClaimKey, currentBlob, 1L, tableRid, "orders"));

    Table renamed = current.toBuilder().setDisplayName("invoices").build();
    String renamedBlob = "/accounts/acct/tables/table-1/table/renamed.pb";
    blobs.put(renamedBlob, renamed.toByteArray(), "application/x-protobuf");

    TransactionIntent intent =
        TransactionIntent.newBuilder()
            .setAccountId(accountId)
            .setTxId("tx-1")
            .setTargetPointerKey(byIdKey)
            .setBlobUri(renamedBlob)
            .setExpectedVersion(1L)
            .setCreatedAt(Timestamps.fromMillis(1))
            .build();

    var outcome = support.applyTransactionBestEffort(List.of(intent), intentRepo);

    assertEquals(TransactionIntentApplierSupport.ApplyStatus.APPLIED, outcome.status());
    assertTrue(pointers.get(oldClaimKey).isEmpty(), "old name's claim must be released");
    assertTrue(pointers.get(oldNameKey).isEmpty(), "old by-name pointer must be released");
    assertTrue(pointers.get(newClaimKey).isPresent(), "new name's claim must be reserved");
    assertEquals(tableRid.getId(), pointers.get(newClaimKey).orElseThrow().getResourceId().getId());
    assertTrue(pointers.get(newNameKey).isPresent(), "new by-name pointer must be reserved");
  }

  private TransactionIntentApplierSupport newSupport(
      InMemoryPointerStore pointerStore, InMemoryBlobStore blobStore) throws Exception {
    var support = new TransactionIntentApplierSupport();
    inject(support, "pointerStore", pointerStore);
    inject(support, "blobStore", blobStore);
    inject(support, "graphView", permissiveGraphView());
    // A real MarkerStore over the same pointer store: the applier asserts the shared protocol now,
    // so the fence it emits has to be the one MarkerStore builds, not a stub of it.
    var markerStore = new MarkerStore();
    inject(markerStore, "pointerStore", pointerStore);
    inject(support, "markerStore", markerStore);
    return support;
  }

  /** Shared in-memory transaction-apply seam for tests that inspect committed pointer state. */
  private record ApplyFixture(
      InMemoryPointerStore pointers,
      InMemoryBlobStore blobs,
      TransactionIntentRepository intentRepo,
      TransactionIntentApplierSupport support) {}

  /** Creates an apply fixture whose repository, support object, and stores share the same state. */
  private ApplyFixture newApplyFixture() throws Exception {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var intentRepo = new TransactionIntentRepository(pointers, blobs);
    return new ApplyFixture(pointers, blobs, intentRepo, newSupport(pointers, blobs));
  }

  private static Transaction readTransaction(InMemoryBlobStore blobs, String blobUri)
      throws Exception {
    return Transaction.parseFrom(blobs.get(blobUri));
  }

  private static final class HookedPointerStore extends InMemoryPointerStore {
    private Runnable beforeBatch;
    private boolean ran;

    void beforeBatch(Runnable hook) {
      this.beforeBatch = hook;
      this.ran = false;
    }

    @Override
    public boolean compareAndSetBatch(List<ai.floedb.floecat.storage.spi.PointerStore.CasOp> ops) {
      if (!ran && beforeBatch != null) {
        ran = true;
        beforeBatch.run();
      }
      return super.compareAndSetBatch(ops);
    }
  }

  /**
   * Permissive graph view for the apply-time write-eligibility guard: resolves the acct/cat-1/ns-1/
   * table-1 objects the table-payload tests use as writable user objects, so eligibility passes and
   * each test exercises its actual pointer/claim assertion. (The guard now fails closed on a null
   * graph view, so tests must supply one.)
   */
  private static ai.floedb.floecat.scanner.spi.CatalogGraphView permissiveGraphView() {
    // Permit-all graph view: resolves any catalog/namespace/table id as a writable user object so
    // the
    // apply-time write-eligibility guard passes and each test exercises its actual pointer/claim
    // assertion. (The guard now fails closed on a null graph view, so tests must supply one.) The
    // synthesized namespace reports catalog "cat-1" to satisfy requireNamespaceInCatalog, matching
    // the catalog id these table payloads use.
    return new ai.floedb.floecat.systemcatalog.util.TestCatalogGraphView() {
      @Override
      public java.util.Optional<ai.floedb.floecat.metagraph.model.GraphNode> resolve(
          ResourceId id) {
        return switch (id.getKind()) {
          case RK_CATALOG ->
              java.util.Optional.of(
                  new ai.floedb.floecat.metagraph.model.CatalogNode(
                      id,
                      "blob://test/v1",
                      id.getId(),
                      java.util.Map.of(),
                      java.util.Optional.empty(),
                      java.util.Optional.empty(),
                      java.util.Optional.empty(),
                      java.util.Map.of()));
          case RK_NAMESPACE ->
              java.util.Optional.of(
                  new ai.floedb.floecat.metagraph.model.NamespaceNode(
                      id,
                      "blob://test/v1",
                      ResourceId.newBuilder()
                          .setAccountId(id.getAccountId())
                          .setId("cat-1")
                          .setKind(ResourceKind.RK_CATALOG)
                          .build(),
                      java.util.List.of(),
                      id.getId(),
                      ai.floedb.floecat.metagraph.model.GraphNodeOrigin.USER,
                      java.util.Map.of(),
                      java.util.Map.of()));
          case RK_TABLE ->
              java.util.Optional.of(
                  ai.floedb.floecat.service.testsupport.TestNodes.tableNode(id, "{}"));
          default -> java.util.Optional.empty();
        };
      }
    };
  }

  /**
   * Seeds the namespace row a table intent joins.
   *
   * <p>A table intent asserts the namespace's canonical pointer in its own batch, so a namespace
   * that was never created reads as deleted and the intent is refused. Only the pointer's presence
   * and version matter to that assertion.
   */
  private static void seedNamespace(
      InMemoryPointerStore pointers, String accountId, String namespaceId) {
    String key = Keys.namespacePointerById(accountId, namespaceId);
    pointers.compareAndSet(key, 0L, PointerReferences.opaqueMarkerPointer(key, namespaceId, 1L));
  }

  /** Builds a user table payload in the requested catalog and namespace for transaction tests. */
  private static Table table(
      String accountId, String catalogId, String namespaceId, String tableId, String displayName) {
    return Table.newBuilder()
        .setResourceId(
            ResourceId.newBuilder()
                .setAccountId(accountId)
                .setId(tableId)
                .setKind(ResourceKind.RK_TABLE))
        .setCatalogId(
            ResourceId.newBuilder()
                .setAccountId(accountId)
                .setId(catalogId)
                .setKind(ResourceKind.RK_CATALOG))
        .setNamespaceId(
            ResourceId.newBuilder()
                .setAccountId(accountId)
                .setId(namespaceId)
                .setKind(ResourceKind.RK_NAMESPACE))
        .setDisplayName(displayName)
        .build();
  }

  /** Builds a table-create intent targeting the table's canonical by-id pointer. */
  private static TransactionIntent tableCreateIntent(
      String accountId, String txId, String tableId, String blobUri, long createdAtMillis) {
    return TransactionIntent.newBuilder()
        .setAccountId(accountId)
        .setTxId(txId)
        .setTargetPointerKey(Keys.tablePointerById(accountId, tableId))
        .setBlobUri(blobUri)
        .setCreatedAt(Timestamps.fromMillis(createdAtMillis))
        .build();
  }

  private static void inject(Object target, String field, Object value) throws Exception {
    Field f = target.getClass().getDeclaredField(field);
    f.setAccessible(true);
    f.set(target, value);
  }
}
