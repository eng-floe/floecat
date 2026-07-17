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

import static ai.floedb.floecat.storage.kv.KvAttributes.ATTR_KIND;
import static ai.floedb.floecat.storage.kv.KvAttributes.ATTR_PARTITION_KEY;
import static ai.floedb.floecat.storage.kv.KvAttributes.ATTR_SORT_KEY;
import static ai.floedb.floecat.storage.kv.KvAttributes.ATTR_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.PointerReferenceKind;
import ai.floedb.floecat.service.repo.model.Keys;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsResponse;

class DynamoReconcileJobIndexBackendTest {
  private static final String TABLE = "floecat_pointers";
  private static final String ACCOUNT_ID = "acct-1";
  private static final String JOB_ID = "job-1";
  private static final String LOOKUP_KEY = Keys.reconcileJobLookupPointerById(JOB_ID);
  private static final String CANONICAL_KEY = Keys.reconcileJobPointerById(ACCOUNT_ID, JOB_ID);

  @Test
  void lookupPointerIsNotParsedAsCanonicalJobKey() {
    assertTrue(JobIndexBackendSupport.parseLookupKey(LOOKUP_KEY) != null);
    assertNull(JobIndexBackendSupport.parseCanonicalJobKey(LOOKUP_KEY));
    assertNull(
        JobIndexBackendSupport.parseCanonicalJobKey(
            Keys.reconcileJobPointerById("by-name", JOB_ID)));
  }

  @Test
  void lookupPrefixIsNotParsedAsCanonicalPrefix() {
    assertNull(
        JobIndexBackendSupport.parseCanonicalPrefix(Keys.reconcileJobLookupPointerByIdPrefix()));
    assertNull(
        JobIndexBackendSupport.parseCanonicalPrefix(Keys.reconcileJobPointerByIdPrefix("by-name")));
  }

  @Test
  void lookupUpsertWritesCorrectLookupPartition() {
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.transactWriteItems(any(TransactWriteItemsRequest.class)))
        .thenReturn(TransactWriteItemsResponse.builder().build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    boolean committed =
        backend.compareAndSetBatch(
            new ReconcileJobIndexStore.JobIndexWriteBatch(
                List.of(
                    new ReconcileJobIndexStore.JobIndexUpsert(
                        LOOKUP_KEY, 0L, CANONICAL_KEY, PointerReferenceKind.PRK_POINTER_KEY)),
                ReconcileJobIndexStore.ReadyQueueMutation.empty()));

    assertTrue(committed);
    ArgumentCaptor<TransactWriteItemsRequest> captor =
        ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
    verify(dynamoDb).transactWriteItems(captor.capture());
    var items = captor.getValue().transactItems();
    assertEquals(2, items.size());
    var item = items.getFirst().put().item();
    assertEquals("reconcile-job-lookup", item.get(ATTR_PARTITION_KEY).s());
    assertEquals("job/" + JOB_ID, item.get(ATTR_SORT_KEY).s());
    assertEquals(JobIndexBackendSupport.KIND_LOOKUP, item.get(ATTR_KIND).s());
    assertEquals(LOOKUP_KEY, item.get(JobIndexBackendSupport.ATTR_POINTER_KEY).s());
    assertEquals(CANONICAL_KEY, item.get(JobIndexBackendSupport.ATTR_BLOB_URI).s());
    assertEquals(
        "reconcile-job/by-id", items.get(1).conditionCheck().key().get(ATTR_PARTITION_KEY).s());
    assertEquals("job/" + JOB_ID, items.get(1).conditionCheck().key().get(ATTR_SORT_KEY).s());
  }

  @Test
  void lookupLoadFallsBackToLegacyCanonicalPartition() {
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.getItem(any(GetItemRequest.class)))
        .thenReturn(GetItemResponse.builder().build())
        .thenReturn(
            GetItemResponse.builder()
                .item(
                    Map.of(
                        ATTR_PARTITION_KEY,
                        AttributeValue.fromS("reconcile-job/by-id"),
                        ATTR_SORT_KEY,
                        AttributeValue.fromS("job/" + JOB_ID),
                        ATTR_KIND,
                        AttributeValue.fromS(JobIndexBackendSupport.KIND_CANONICAL_JOB),
                        ATTR_VERSION,
                        AttributeValue.fromN("1"),
                        JobIndexBackendSupport.ATTR_POINTER_KEY,
                        AttributeValue.fromS(LOOKUP_KEY),
                        JobIndexBackendSupport.ATTR_BLOB_URI,
                        AttributeValue.fromS(CANONICAL_KEY)))
                .build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    var loaded = backend.loadIndexEntry(LOOKUP_KEY);

    assertTrue(loaded.isPresent());
    assertEquals(LOOKUP_KEY, loaded.get().pointerKey());
    assertEquals(CANONICAL_KEY, loaded.get().blobUri());
    assertEquals(1L, loaded.get().version());
    ArgumentCaptor<GetItemRequest> captor = ArgumentCaptor.forClass(GetItemRequest.class);
    verify(dynamoDb, org.mockito.Mockito.times(2)).getItem(captor.capture());
    assertEquals(
        "reconcile-job-lookup", captor.getAllValues().get(0).key().get(ATTR_PARTITION_KEY).s());
    assertEquals(
        "reconcile-job/by-id", captor.getAllValues().get(1).key().get(ATTR_PARTITION_KEY).s());
  }

  @Test
  void lookupCheckAbsentChecksCorrectedAndLegacyPartitions() {
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.transactWriteItems(any(TransactWriteItemsRequest.class)))
        .thenReturn(TransactWriteItemsResponse.builder().build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    boolean committed =
        backend.compareAndSetBatch(
            new ReconcileJobIndexStore.JobIndexWriteBatch(
                List.of(new ReconcileJobIndexStore.JobIndexCheckAbsent(LOOKUP_KEY)),
                ReconcileJobIndexStore.ReadyQueueMutation.empty()));

    assertTrue(committed);
    ArgumentCaptor<TransactWriteItemsRequest> captor =
        ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
    verify(dynamoDb).transactWriteItems(captor.capture());
    var items = captor.getValue().transactItems();
    assertEquals(2, items.size());
    assertEquals(
        "reconcile-job-lookup", items.get(0).conditionCheck().key().get(ATTR_PARTITION_KEY).s());
    assertEquals(
        "reconcile-job/by-id", items.get(1).conditionCheck().key().get(ATTR_PARTITION_KEY).s());
    assertFalse(items.get(0).conditionCheck().key().get(ATTR_SORT_KEY).s().isBlank());
  }

  @Test
  void physicalWriteItemCountIncludesLegacyLookupGuardForNewLookupUpsert() {
    var batch =
        new ReconcileJobIndexStore.JobIndexWriteBatch(
            List.of(
                new ReconcileJobIndexStore.JobIndexUpsert(
                    Keys.reconcileDedupePointer(ACCOUNT_ID, "hash-1"),
                    0L,
                    CANONICAL_KEY,
                    PointerReferenceKind.PRK_POINTER_KEY),
                new ReconcileJobIndexStore.JobIndexUpsert(
                    CANONICAL_KEY,
                    0L,
                    "inline:reconcile-job:e30",
                    PointerReferenceKind.PRK_INLINE_JSON),
                new ReconcileJobIndexStore.JobIndexUpsert(
                    LOOKUP_KEY, 0L, CANONICAL_KEY, PointerReferenceKind.PRK_POINTER_KEY)),
            new ReconcileJobIndexStore.ReadyQueueMutation(
                List.of(
                    new ReconcileJobIndexStore.ReadyQueueWrite(
                        Keys.reconcileReadyPointerByDue(1L, ACCOUNT_ID, "lane", JOB_ID),
                        CANONICAL_KEY,
                        PointerReferenceKind.PRK_POINTER_KEY)),
                List.of()));

    assertEquals(5, NativeReconcileJobIndexStore.physicalWriteItemCount(batch));
  }

  @Test
  void residualPurgeOnlyDeletesReferenceRowsWithReferenceCondition() {
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.scan(any(ScanRequest.class)))
        .thenReturn(
            ScanResponse.builder()
                .items(
                    Map.of(
                        ATTR_PARTITION_KEY,
                        AttributeValue.fromS("reconcile-job-lookup"),
                        ATTR_SORT_KEY,
                        AttributeValue.fromS("job/" + JOB_ID),
                        ATTR_KIND,
                        AttributeValue.fromS(JobIndexBackendSupport.KIND_LOOKUP),
                        ATTR_VERSION,
                        AttributeValue.fromN("1"),
                        JobIndexBackendSupport.ATTR_POINTER_KEY,
                        AttributeValue.fromS(LOOKUP_KEY),
                        JobIndexBackendSupport.ATTR_BLOB_URI,
                        AttributeValue.fromS(CANONICAL_KEY)))
                .build());
    when(dynamoDb.deleteItem(any(DeleteItemRequest.class)))
        .thenReturn(DeleteItemResponse.builder().build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    boolean purged = backend.purgeEntriesByCanonicalReference(CANONICAL_KEY);

    assertTrue(purged);
    ArgumentCaptor<ScanRequest> scanCaptor = ArgumentCaptor.forClass(ScanRequest.class);
    verify(dynamoDb).scan(scanCaptor.capture());
    assertEquals(
        "#canonical = :canonical OR #blob = :canonical", scanCaptor.getValue().filterExpression());
    assertFalse(
        scanCaptor
            .getValue()
            .expressionAttributeNames()
            .containsValue(JobIndexBackendSupport.ATTR_POINTER_KEY));
    ArgumentCaptor<DeleteItemRequest> deleteCaptor =
        ArgumentCaptor.forClass(DeleteItemRequest.class);
    verify(dynamoDb).deleteItem(deleteCaptor.capture());
    assertEquals(
        "#canonical = :canonical OR #blob = :canonical",
        deleteCaptor.getValue().conditionExpression());
  }
}
