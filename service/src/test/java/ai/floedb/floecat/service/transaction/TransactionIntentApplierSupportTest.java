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

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.impl.SnapshotCreateSequenceStore;
import ai.floedb.floecat.service.repo.impl.TransactionIntentRepository;
import ai.floedb.floecat.service.repo.impl.TransactionRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
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

    var support = new TransactionIntentApplierSupport();
    inject(support, "pointerStore", pointers);
    inject(support, "blobStore", blobs);
    inject(support, "overlay", permissiveOverlay());

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
    inject(support, "overlay", permissiveOverlay());

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
    inject(support, "overlay", permissiveOverlay());

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
  void applyTransactionBestEffortAdvancesSnapshotCreateSequenceForNewSnapshot() throws Exception {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var intentRepo = new TransactionIntentRepository(pointers, blobs);

    var support = new TransactionIntentApplierSupport();
    inject(support, "pointerStore", pointers);
    inject(support, "blobStore", blobs);
    inject(support, "overlay", permissiveOverlay());

    String accountId = "acct";
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(accountId)
            .setId("table-1")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    long upstreamCreatedMs = 1234L;
    Snapshot snapshot =
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(42L)
            .setUpstreamCreatedAt(Timestamps.fromMillis(upstreamCreatedMs))
            .build();
    String blobUri =
        Keys.snapshotBlobUri(
            accountId,
            tableId.getId(),
            snapshot.getSnapshotId(),
            ai.floedb.floecat.types.Hashing.sha256Hex(snapshot.toByteArray()));
    blobs.put(blobUri, snapshot.toByteArray(), "application/x-protobuf");

    TransactionIntent byId =
        TransactionIntent.newBuilder()
            .setAccountId(accountId)
            .setTxId("tx-1")
            .setTargetPointerKey(Keys.snapshotPointerById(accountId, tableId.getId(), 42L))
            .setBlobUri(blobUri)
            .setCreatedAt(Timestamps.fromMillis(1))
            .build();
    TransactionIntent byTime =
        TransactionIntent.newBuilder()
            .setAccountId(accountId)
            .setTxId("tx-1")
            .setTargetPointerKey(
                Keys.snapshotPointerByTime(accountId, tableId.getId(), 42L, upstreamCreatedMs))
            .setBlobUri(blobUri)
            .setCreatedAt(Timestamps.fromMillis(1))
            .build();

    var outcome = support.applyTransactionBestEffort(List.of(byId, byTime), intentRepo);

    assertEquals(TransactionIntentApplierSupport.ApplyStatus.APPLIED, outcome.status());
    var createSequences = new SnapshotCreateSequenceStore(pointers);
    assertEquals(1L, createSequences.currentSequence(accountId));
  }

  @Test
  void applyTransactionRejectsTableIntentTargetMismatch() throws Exception {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var intentRepo = new TransactionIntentRepository(pointers, blobs);

    var support = new TransactionIntentApplierSupport();
    inject(support, "pointerStore", pointers);
    inject(support, "blobStore", blobs);
    inject(support, "overlay", permissiveOverlay());

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

    var support = new TransactionIntentApplierSupport();
    inject(support, "pointerStore", pointers);
    inject(support, "blobStore", blobs);
    inject(support, "overlay", permissiveOverlay());

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

    var support = new TransactionIntentApplierSupport();
    inject(support, "pointerStore", pointers);
    inject(support, "blobStore", blobs);
    inject(support, "overlay", permissiveOverlay());

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

    var support = new TransactionIntentApplierSupport();
    inject(support, "pointerStore", pointers);
    inject(support, "blobStore", blobs);
    inject(support, "overlay", permissiveOverlay());

    String accountId = "acct";
    String catalogId = "cat-1";
    String namespaceId = "ns-1";
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

    var support = new TransactionIntentApplierSupport();
    inject(support, "pointerStore", pointers);
    inject(support, "blobStore", blobs);
    inject(support, "overlay", permissiveOverlay());

    String accountId = "acct";
    String catalogId = "cat-1";
    String namespaceId = "ns-1";
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

    var support = new TransactionIntentApplierSupport();
    inject(support, "pointerStore", pointers);
    inject(support, "blobStore", blobs);
    inject(support, "overlay", permissiveOverlay());

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

    var support = new TransactionIntentApplierSupport();
    inject(support, "pointerStore", pointers);
    inject(support, "blobStore", blobs);
    inject(support, "overlay", permissiveOverlay());

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

    var support = new TransactionIntentApplierSupport();
    inject(support, "pointerStore", pointers);
    inject(support, "blobStore", blobs);
    inject(support, "overlay", permissiveOverlay());

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

    var support = new TransactionIntentApplierSupport();
    inject(support, "pointerStore", pointers);
    inject(support, "blobStore", blobs);
    inject(support, "overlay", permissiveOverlay());

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

    var support = new TransactionIntentApplierSupport();
    inject(support, "pointerStore", pointers);
    inject(support, "blobStore", blobs);
    inject(support, "overlay", permissiveOverlay());

    String accountId = "acct";
    String catalogId = "cat-1";
    String namespaceId = "ns-1";
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

  @Test
  void applyTransactionRejectsTableCreateWhenViewHoldsRelationName() throws Exception {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var intentRepo = new TransactionIntentRepository(pointers, blobs);

    var support = new TransactionIntentApplierSupport();
    inject(support, "pointerStore", pointers);
    inject(support, "blobStore", blobs);
    inject(support, "overlay", permissiveOverlay());

    String accountId = "acct";
    String catalogId = "cat-1";
    String namespaceId = "ns-1";
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
   * Permissive overlay for the apply-time write-eligibility guard: resolves the acct/cat-1/ns-1/
   * table-1 objects the table-payload tests use as writable user objects, so eligibility passes and
   * each test exercises its actual pointer/claim assertion. (The guard now fails closed on a null
   * overlay, so tests must supply one.)
   */
  private static ai.floedb.floecat.scanner.spi.CatalogOverlay permissiveOverlay() {
    // Permit-all overlay: resolves any catalog/namespace/table id as a writable user object so the
    // apply-time write-eligibility guard passes and each test exercises its actual pointer/claim
    // assertion. (The guard now fails closed on a null overlay, so tests must supply one.) The
    // synthesized namespace reports catalog "cat-1" to satisfy requireNamespaceInCatalog, matching
    // the catalog id these table payloads use.
    return new ai.floedb.floecat.systemcatalog.util.TestCatalogOverlay() {
      @Override
      public java.util.Optional<ai.floedb.floecat.metagraph.model.GraphNode> resolve(
          ResourceId id) {
        return switch (id.getKind()) {
          case RK_CATALOG ->
              java.util.Optional.of(
                  new ai.floedb.floecat.metagraph.model.CatalogNode(
                      id,
                      1L,
                      java.time.Instant.EPOCH,
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
                      1L,
                      java.time.Instant.EPOCH,
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

  private static void inject(Object target, String field, Object value) throws Exception {
    Field f = target.getClass().getDeclaredField(field);
    f.setAccessible(true);
    f.set(target, value);
  }
}
