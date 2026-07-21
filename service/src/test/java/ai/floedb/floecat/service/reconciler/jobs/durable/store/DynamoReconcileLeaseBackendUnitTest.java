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

package ai.floedb.floecat.service.reconciler.jobs.durable.store;

import static ai.floedb.floecat.storage.kv.KvAttributes.ATTR_PARTITION_KEY;
import static ai.floedb.floecat.storage.kv.KvAttributes.ATTR_SORT_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.PointerReferenceKind;
import ai.floedb.floecat.service.repo.model.Keys;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsResponse;

class DynamoReconcileLeaseBackendUnitTest {
  private static final String TABLE = "floecat_pointers";
  private static final String ACCOUNT_ID = "acct-1";
  private static final String JOB_ID = "job-1";
  private static final String LOOKUP_KEY = Keys.reconcileJobLookupPointerById(JOB_ID);
  private static final String CANONICAL_KEY = Keys.reconcileJobPointerById(ACCOUNT_ID, JOB_ID);

  @Test
  void jobIndexLookupUpsertWritesCorrectLookupPartition() {
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.transactWriteItems(any(TransactWriteItemsRequest.class)))
        .thenReturn(TransactWriteItemsResponse.builder().build());
    DynamoReconcileLeaseBackend backend = new DynamoReconcileLeaseBackend();
    backend.bind(() -> dynamoDb, TABLE);

    boolean committed =
        backend.compareAndSetBatch(
            new ReconcileJobIndexStore.JobIndexWriteBatch(
                List.of(
                    new ReconcileJobIndexStore.JobIndexUpsert(
                        LOOKUP_KEY, 0L, CANONICAL_KEY, PointerReferenceKind.PRK_POINTER_KEY)),
                ReconcileJobIndexStore.ReadyQueueMutation.empty()),
            ReconcileLeaseBackend.LeaseWriteBatch.empty());

    assertTrue(committed);
    ArgumentCaptor<TransactWriteItemsRequest> captor =
        ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
    verify(dynamoDb).transactWriteItems(captor.capture());
    var items = captor.getValue().transactItems();
    assertEquals(2, items.size());
    var item = items.getFirst().put().item();
    assertEquals("reconcile-job-lookup", item.get(ATTR_PARTITION_KEY).s());
    assertEquals("job/" + JOB_ID, item.get(ATTR_SORT_KEY).s());
    assertEquals(
        "reconcile-job/by-id", items.get(1).conditionCheck().key().get(ATTR_PARTITION_KEY).s());
  }

  @Test
  void jobIndexLookupDeleteUsesCurrentAndLegacyPartitions() {
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.transactWriteItems(any(TransactWriteItemsRequest.class)))
        .thenReturn(TransactWriteItemsResponse.builder().build());
    DynamoReconcileLeaseBackend backend = new DynamoReconcileLeaseBackend();
    backend.bind(() -> dynamoDb, TABLE);

    boolean committed =
        backend.compareAndSetBatch(
            new ReconcileJobIndexStore.JobIndexWriteBatch(
                List.of(
                    new ReconcileJobIndexStore.JobIndexDelete(
                        LOOKUP_KEY, 1L, CANONICAL_KEY, "reconcile-job-lookup")),
                ReconcileJobIndexStore.ReadyQueueMutation.empty()),
            ReconcileLeaseBackend.LeaseWriteBatch.empty());

    assertTrue(committed);
    ArgumentCaptor<TransactWriteItemsRequest> captor =
        ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
    verify(dynamoDb).transactWriteItems(captor.capture());
    var items = captor.getValue().transactItems();
    assertEquals(2, items.size());
    assertEquals(
        "reconcile-job-lookup", items.getFirst().delete().key().get(ATTR_PARTITION_KEY).s());
    assertEquals("1", items.getFirst().delete().expressionAttributeValues().get(":expected").n());
    assertEquals(
        "reconcile-job/by-id", items.get(1).conditionCheck().key().get(ATTR_PARTITION_KEY).s());
  }

  @Test
  void jobIndexOwnedReferenceDeleteChecksVersionAndCanonicalOwner() {
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.transactWriteItems(any(TransactWriteItemsRequest.class)))
        .thenReturn(TransactWriteItemsResponse.builder().build());
    DynamoReconcileLeaseBackend backend = new DynamoReconcileLeaseBackend();
    backend.bind(() -> dynamoDb, TABLE);
    String dedupeKey = Keys.reconcileDedupePointer(ACCOUNT_ID, "hash-1");

    assertTrue(
        backend.compareAndSetBatch(
            new ReconcileJobIndexStore.JobIndexWriteBatch(
                List.of(new ReconcileJobIndexStore.JobIndexDelete(dedupeKey, 1L, CANONICAL_KEY)),
                ReconcileJobIndexStore.ReadyQueueMutation.empty()),
            ReconcileLeaseBackend.LeaseWriteBatch.empty()));

    ArgumentCaptor<TransactWriteItemsRequest> captor =
        ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
    verify(dynamoDb).transactWriteItems(captor.capture());
    var delete = captor.getValue().transactItems().getFirst().delete();
    assertEquals("#v = :expected AND #owner = :owner", delete.conditionExpression());
    assertEquals(
        JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY,
        delete.expressionAttributeNames().get("#owner"));
    assertEquals(CANONICAL_KEY, delete.expressionAttributeValues().get(":owner").s());
  }

  @Test
  void canonicalUpsertPreservesCleanupManifestCompleteness() {
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.transactWriteItems(any(TransactWriteItemsRequest.class)))
        .thenReturn(TransactWriteItemsResponse.builder().build());
    DynamoReconcileLeaseBackend backend = new DynamoReconcileLeaseBackend();
    backend.bind(() -> dynamoDb, TABLE);
    String projectionKey = Keys.reconcileJobProjectionPointer(ACCOUNT_ID, JOB_ID);

    boolean committed =
        backend.compareAndSetBatch(
            new ReconcileJobIndexStore.JobIndexWriteBatch(
                List.of(
                    new ReconcileJobIndexStore.JobIndexUpsert(
                        CANONICAL_KEY,
                        1L,
                        "inline:reconcile-job:e30",
                        PointerReferenceKind.PRK_INLINE_JSON,
                        new ReconcileJobIndexCleanupManifest(
                            List.of(LOOKUP_KEY), List.of(), List.of(projectionKey)))),
                ReconcileJobIndexStore.ReadyQueueMutation.empty()),
            ReconcileLeaseBackend.LeaseWriteBatch.empty());

    assertTrue(committed);
    ArgumentCaptor<TransactWriteItemsRequest> captor =
        ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
    verify(dynamoDb).transactWriteItems(captor.capture());
    var update = captor.getValue().transactItems().getFirst().update();
    assertEquals(LOOKUP_KEY, update.expressionAttributeValues().get(":idx").l().getFirst().s());
    assertEquals(projectionKey, update.expressionAttributeValues().get(":ptr").l().getFirst().s());
    assertTrue(update.expressionAttributeValues().get(":true").bool());
    assertTrue(update.updateExpression().contains("#complete = :true"));
    assertTrue(
        update
            .updateExpression()
            .contains("REMOVE #scan, #cursor, #drained, #legacyIdx, #legacyReady"));
    assertEquals(
        JobIndexBackendSupport.ATTR_CLEANUP_DELETE_IN_PROGRESS,
        update.expressionAttributeNames().get("#lock"));
    assertEquals("#v = :expected AND attribute_not_exists(#lock)", update.conditionExpression());
  }

  @Test
  void cleanupOwnedReferenceDeleteAllowsAlreadyAbsentRow() {
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.transactWriteItems(any(TransactWriteItemsRequest.class)))
        .thenReturn(TransactWriteItemsResponse.builder().build());
    DynamoReconcileLeaseBackend backend = new DynamoReconcileLeaseBackend();
    backend.bind(() -> dynamoDb, TABLE);
    String dedupeKey = Keys.reconcileDedupePointer(ACCOUNT_ID, "hash-cleanup");

    assertTrue(
        backend.compareAndSetBatch(
            new ReconcileJobIndexStore.JobIndexWriteBatch(
                List.of(
                    new ReconcileJobIndexStore.JobIndexDelete(
                        dedupeKey, 1L, CANONICAL_KEY, "", false, true)),
                ReconcileJobIndexStore.ReadyQueueMutation.empty()),
            ReconcileLeaseBackend.LeaseWriteBatch.empty()));

    ArgumentCaptor<TransactWriteItemsRequest> captor =
        ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
    verify(dynamoDb).transactWriteItems(captor.capture());
    assertEquals(
        "attribute_not_exists(#pk) OR (#v = :expected AND #owner = :owner)",
        captor.getValue().transactItems().getFirst().delete().conditionExpression());
  }

  @Test
  void rejectsUnsupportedJobIndexUpsertKey() {
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    DynamoReconcileLeaseBackend backend = new DynamoReconcileLeaseBackend();
    backend.bind(() -> dynamoDb, TABLE);
    String unsupportedKey = "/unsupported/index/upsert";

    IllegalArgumentException failure =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                backend.compareAndSetBatch(
                    new ReconcileJobIndexStore.JobIndexWriteBatch(
                        List.of(
                            new ReconcileJobIndexStore.JobIndexUpsert(
                                unsupportedKey,
                                0L,
                                CANONICAL_KEY,
                                PointerReferenceKind.PRK_POINTER_KEY)),
                        ReconcileJobIndexStore.ReadyQueueMutation.empty()),
                    ReconcileLeaseBackend.LeaseWriteBatch.empty()));

    assertEquals(
        "Unsupported reconcile job index upsert key: " + unsupportedKey, failure.getMessage());
    verify(dynamoDb, never()).transactWriteItems(any(TransactWriteItemsRequest.class));
  }

  @Test
  void rejectsUnsupportedJobIndexDeleteKey() {
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    DynamoReconcileLeaseBackend backend = new DynamoReconcileLeaseBackend();
    backend.bind(() -> dynamoDb, TABLE);
    String unsupportedKey = "/unsupported/index/delete";

    IllegalArgumentException failure =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                backend.compareAndSetBatch(
                    new ReconcileJobIndexStore.JobIndexWriteBatch(
                        List.of(new ReconcileJobIndexStore.JobIndexDelete(unsupportedKey, 1L)),
                        ReconcileJobIndexStore.ReadyQueueMutation.empty()),
                    ReconcileLeaseBackend.LeaseWriteBatch.empty()));

    assertEquals(
        "Unsupported reconcile job index delete key: " + unsupportedKey, failure.getMessage());
    verify(dynamoDb, never()).transactWriteItems(any(TransactWriteItemsRequest.class));
  }

  @Test
  void rejectsUnsupportedJobIndexCheckAbsentKey() {
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    DynamoReconcileLeaseBackend backend = new DynamoReconcileLeaseBackend();
    backend.bind(() -> dynamoDb, TABLE);
    String unsupportedKey = "/unsupported/index/check-absent";

    IllegalArgumentException failure =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                backend.compareAndSetBatch(
                    new ReconcileJobIndexStore.JobIndexWriteBatch(
                        List.of(new ReconcileJobIndexStore.JobIndexCheckAbsent(unsupportedKey)),
                        ReconcileJobIndexStore.ReadyQueueMutation.empty()),
                    ReconcileLeaseBackend.LeaseWriteBatch.empty()));

    assertEquals(
        "Unsupported reconcile job index check-absent key: " + unsupportedKey,
        failure.getMessage());
    verify(dynamoDb, never()).transactWriteItems(any(TransactWriteItemsRequest.class));
  }

  @Test
  void rejectsCombinedTransactionsOverDynamoLimit() {
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    DynamoReconcileLeaseBackend backend = new DynamoReconcileLeaseBackend();
    backend.bind(() -> dynamoDb, TABLE);
    var writes =
        IntStream.range(0, 101)
            .mapToObj(
                index ->
                    new ReconcileJobIndexStore.JobIndexUpsert(
                        Keys.reconcileJobLookupPointerById("job-" + index),
                        0L,
                        CANONICAL_KEY,
                        PointerReferenceKind.PRK_POINTER_KEY))
            .map(ReconcileJobIndexStore.JobIndexWriteOp.class::cast)
            .toList();

    assertThrows(
        IllegalArgumentException.class,
        () ->
            backend.compareAndSetBatch(
                new ReconcileJobIndexStore.JobIndexWriteBatch(
                    writes, ReconcileJobIndexStore.ReadyQueueMutation.empty()),
                ReconcileLeaseBackend.LeaseWriteBatch.empty()));
    verify(dynamoDb, never()).transactWriteItems(any(TransactWriteItemsRequest.class));
  }
}
