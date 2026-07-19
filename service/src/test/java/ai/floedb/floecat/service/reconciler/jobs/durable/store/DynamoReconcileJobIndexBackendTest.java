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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.PointerReferenceKind;
import ai.floedb.floecat.service.repo.model.Keys;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsResponse;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

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
  }

  @Test
  void lookupLoadFallsBackToLegacyCanonicalPartitionWithoutMigrating() {
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.getItem(any(GetItemRequest.class)))
        .thenAnswer(
            invocation -> {
              GetItemRequest request = invocation.getArgument(0);
              String partitionKey = request.key().get(ATTR_PARTITION_KEY).s();
              if (!JobIndexBackendSupport.legacyLookupPartitionKey().equals(partitionKey)) {
                return GetItemResponse.builder().build();
              }
              return GetItemResponse.builder()
                  .item(
                      Map.of(
                          ATTR_PARTITION_KEY,
                          AttributeValue.fromS(partitionKey),
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
                  .build();
            });
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    JobIndexEntrySnapshot loaded = backend.loadIndexEntry(LOOKUP_KEY).orElseThrow();

    assertEquals(LOOKUP_KEY, loaded.pointerKey());
    assertEquals(CANONICAL_KEY, loaded.blobUri());
    assertEquals(1L, loaded.version());
    assertEquals(
        JobIndexBackendSupport.legacyLookupPartitionKey(), loaded.lookupStoragePartitionKey());
    ArgumentCaptor<GetItemRequest> captor = ArgumentCaptor.forClass(GetItemRequest.class);
    verify(dynamoDb, times(2)).getItem(captor.capture());
    assertEquals(
        List.of("reconcile-job-lookup", "reconcile-job/by-id"),
        captor.getAllValues().stream()
            .map(request -> request.key().get(ATTR_PARTITION_KEY).s())
            .toList());
    verify(dynamoDb, never()).transactWriteItems(any(TransactWriteItemsRequest.class));
  }

  @Test
  void lookupLoadUsesCurrentPartitionWithoutLegacyProbe() {
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.getItem(any(GetItemRequest.class)))
        .thenReturn(
            GetItemResponse.builder()
                .item(
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
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    assertEquals(CANONICAL_KEY, backend.loadIndexEntry(LOOKUP_KEY).orElseThrow().blobUri());

    verify(dynamoDb).getItem(any(GetItemRequest.class));
    verify(dynamoDb, never()).transactWriteItems(any(TransactWriteItemsRequest.class));
  }

  @Test
  void canonicalLoadAndListExposePersistedCleanupLock() {
    String blob = "inline:reconcile-job:e30";
    Map<String, AttributeValue> lockedCanonical =
        Map.of(
            ATTR_PARTITION_KEY,
            AttributeValue.fromS(JobIndexBackendSupport.canonicalPartitionKey(ACCOUNT_ID)),
            ATTR_SORT_KEY,
            AttributeValue.fromS("job/" + JOB_ID),
            ATTR_KIND,
            AttributeValue.fromS(JobIndexBackendSupport.KIND_CANONICAL_JOB),
            ATTR_VERSION,
            AttributeValue.fromN("2"),
            JobIndexBackendSupport.ATTR_POINTER_KEY,
            AttributeValue.fromS(CANONICAL_KEY),
            JobIndexBackendSupport.ATTR_BLOB_URI,
            AttributeValue.fromS(blob),
            JobIndexBackendSupport.ATTR_CLEANUP_DELETE_IN_PROGRESS,
            AttributeValue.fromBool(true));
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.getItem(any(GetItemRequest.class)))
        .thenReturn(GetItemResponse.builder().item(lockedCanonical).build());
    when(dynamoDb.query(any(QueryRequest.class)))
        .thenReturn(QueryResponse.builder().items(lockedCanonical).build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    JobIndexEntrySnapshot loaded = backend.loadIndexEntry(CANONICAL_KEY).orElseThrow();
    var listed = backend.listCanonicalEntries(ACCOUNT_ID, 10, "");

    assertTrue(loaded.cleanupLocked());
    assertEquals(2L, loaded.version());
    assertEquals(1, listed.entries().size());
    assertTrue(listed.entries().getFirst().cleanupLocked());
  }

  @Test
  void legacyLookupBackfillDeletesConflictingLegacyOwnerAndKeepsCurrentOwnerAuthoritative() {
    String conflictingCanonical = Keys.reconcileJobPointerById("acct-legacy", JOB_ID);
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.query(any(QueryRequest.class)))
        .thenReturn(
            QueryResponse.builder()
                .items(
                    Map.of(
                        ATTR_PARTITION_KEY,
                        AttributeValue.fromS(JobIndexBackendSupport.legacyLookupPartitionKey()),
                        ATTR_SORT_KEY,
                        AttributeValue.fromS("job/" + JOB_ID),
                        ATTR_KIND,
                        AttributeValue.fromS(JobIndexBackendSupport.KIND_CANONICAL_JOB),
                        ATTR_VERSION,
                        AttributeValue.fromN("1"),
                        JobIndexBackendSupport.ATTR_POINTER_KEY,
                        AttributeValue.fromS(LOOKUP_KEY),
                        JobIndexBackendSupport.ATTR_BLOB_URI,
                        AttributeValue.fromS(conflictingCanonical)))
                .build());
    when(dynamoDb.getItem(any(GetItemRequest.class)))
        .thenAnswer(
            invocation -> {
              GetItemRequest request = invocation.getArgument(0);
              String partitionKey = request.key().get(ATTR_PARTITION_KEY).s();
              if (!JobIndexBackendSupport.lookupPartitionKey().equals(partitionKey)) {
                return GetItemResponse.builder().build();
              }
              return GetItemResponse.builder()
                  .item(
                      Map.of(
                          ATTR_PARTITION_KEY,
                          AttributeValue.fromS(partitionKey),
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
                  .build();
            });
    when(dynamoDb.transactWriteItems(any(TransactWriteItemsRequest.class)))
        .thenThrow(TransactionCanceledException.builder().build())
        .thenReturn(TransactWriteItemsResponse.builder().build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    var result = backend.migrateLegacyLookupEntries(25, "");

    assertEquals(1, result.scanned());
    assertEquals(1, result.migrated());
    assertEquals(0, result.conflicted());
    assertEquals(0, result.retryable());
    ArgumentCaptor<TransactWriteItemsRequest> txCaptor =
        ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
    verify(dynamoDb, times(2)).transactWriteItems(txCaptor.capture());
    var conflictDelete = txCaptor.getAllValues().get(1).transactItems().getFirst().delete();
    assertEquals(
        JobIndexBackendSupport.legacyLookupPartitionKey(),
        conflictDelete.key().get(ATTR_PARTITION_KEY).s());
    assertEquals(
        conflictingCanonical, conflictDelete.expressionAttributeValues().get(":reference").s());
  }

  @Test
  void legacyLookupBackfillQueriesOnlyLegacyPartition() {
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.query(any(QueryRequest.class)))
        .thenReturn(
            QueryResponse.builder()
                .items(
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
    when(dynamoDb.transactWriteItems(any(TransactWriteItemsRequest.class)))
        .thenReturn(TransactWriteItemsResponse.builder().build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    var result = backend.migrateLegacyLookupEntries(25, "");

    assertEquals(1, result.scanned());
    assertEquals(1, result.migrated());
    assertEquals(0, result.conflicted());
    assertEquals(0, result.retryable());
    ArgumentCaptor<QueryRequest> queryCaptor = ArgumentCaptor.forClass(QueryRequest.class);
    verify(dynamoDb).query(queryCaptor.capture());
    assertEquals(
        "reconcile-job/by-id", queryCaptor.getValue().expressionAttributeValues().get(":pk").s());
  }

  @Test
  void dedupePaginationTokenResumesFromItsPhysicalSortKey() {
    String dedupeKey = Keys.reconcileDedupePointer(ACCOUNT_ID, "hash-1");
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder().build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    backend.listDedupeEntries(ACCOUNT_ID, 25, dedupeKey);

    ArgumentCaptor<QueryRequest> queryCaptor = ArgumentCaptor.forClass(QueryRequest.class);
    verify(dynamoDb).query(queryCaptor.capture());
    var parsed = JobIndexBackendSupport.parseDedupeKey(dedupeKey);
    assertEquals(
        JobIndexBackendSupport.dedupeSortKey(parsed),
        queryCaptor.getValue().exclusiveStartKey().get(ATTR_SORT_KEY).s());
  }

  @Test
  void malformedLegacyLookupRowDoesNotTruncatePaginatedMigration() {
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    Map<String, AttributeValue> physicalKey =
        Map.of(
            ATTR_PARTITION_KEY,
            AttributeValue.fromS("reconcile-job/by-id"),
            ATTR_SORT_KEY,
            AttributeValue.fromS("job/" + JOB_ID));
    when(dynamoDb.query(any(QueryRequest.class)))
        .thenReturn(
            QueryResponse.builder()
                .items(
                    Map.of(
                        ATTR_PARTITION_KEY,
                        AttributeValue.fromS("reconcile-job/by-id"),
                        ATTR_SORT_KEY,
                        AttributeValue.fromS("job/" + JOB_ID),
                        ATTR_VERSION,
                        AttributeValue.fromN("1"),
                        JobIndexBackendSupport.ATTR_POINTER_KEY,
                        AttributeValue.fromS("garbage"),
                        JobIndexBackendSupport.ATTR_BLOB_URI,
                        AttributeValue.fromS(CANONICAL_KEY)))
                .lastEvaluatedKey(physicalKey)
                .build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    var first = backend.migrateLegacyLookupEntries(25, "");
    backend.migrateLegacyLookupEntries(25, first.nextPageToken());

    assertEquals(1, first.conflicted());
    assertTrue(!first.nextPageToken().isBlank());
    ArgumentCaptor<QueryRequest> queryCaptor = ArgumentCaptor.forClass(QueryRequest.class);
    verify(dynamoDb, times(2)).query(queryCaptor.capture());
    assertEquals(
        "job/" + JOB_ID,
        queryCaptor.getAllValues().get(1).exclusiveStartKey().get(ATTR_SORT_KEY).s());
  }

  @Test
  void legacyLookupBackfillReportsTransientWriteFailureAsRetryable() {
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.query(any(QueryRequest.class)))
        .thenReturn(
            QueryResponse.builder()
                .items(
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
    when(dynamoDb.transactWriteItems(any(TransactWriteItemsRequest.class)))
        .thenThrow(new RuntimeException("transient write failure"));
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    var result = backend.migrateLegacyLookupEntries(25, "");

    assertEquals(1, result.scanned());
    assertEquals(0, result.migrated());
    assertEquals(0, result.conflicted());
    assertEquals(1, result.retryable());
  }

  @Test
  void legacyLookupBackfillSkipsQueryWhenDurableMarkerExists() {
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.getItem(any(GetItemRequest.class)))
        .thenReturn(
            GetItemResponse.builder()
                .item(
                    Map.of(
                        ATTR_PARTITION_KEY,
                        AttributeValue.fromS("reconcile-job-maintenance"),
                        ATTR_SORT_KEY,
                        AttributeValue.fromS("legacy-lookup-v1")))
                .build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    var result = backend.migrateLegacyLookupEntries(25, "");

    assertEquals(0, result.scanned());
    verify(dynamoDb, never()).query(any(QueryRequest.class));
  }

  @Test
  void completingLegacyLookupBackfillWritesDurableMarker() {
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    assertTrue(
        backend.completeLegacyMigration(
            ReconcileJobIndexBackend.LegacyMigration.LOOKUP, "owner-1", 7L, 1_000L));

    ArgumentCaptor<TransactWriteItemsRequest> captor =
        ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
    verify(dynamoDb).transactWriteItems(captor.capture());
    var items = captor.getValue().transactItems();
    assertEquals(2, items.size());
    assertEquals(
        "owner-1", items.getFirst().delete().expressionAttributeValues().get(":owner").s());
    assertEquals("7", items.getFirst().delete().expressionAttributeValues().get(":fence").n());
    assertEquals(
        "reconcile-job-maintenance", items.get(1).put().item().get(ATTR_PARTITION_KEY).s());
    assertEquals("legacy-lookup-v1", items.get(1).put().item().get(ATTR_SORT_KEY).s());
  }

  @Test
  void legacyMigrationLeaseRestoresDurableCheckpointAndFencesUpdates() {
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.getItem(any(GetItemRequest.class)))
        .thenReturn(
            GetItemResponse.builder().build(),
            GetItemResponse.builder()
                .item(
                    Map.ofEntries(
                        Map.entry("migration_owner", AttributeValue.fromS("owner-1")),
                        Map.entry("migration_fence", AttributeValue.fromN("7")),
                        Map.entry("migration_page_token", AttributeValue.fromS("page-2")),
                        Map.entry("migration_changed", AttributeValue.fromN("3")),
                        Map.entry("migration_unresolvable", AttributeValue.fromN("1")),
                        Map.entry("migration_conflicted", AttributeValue.fromN("2")),
                        Map.entry("migration_retryable", AttributeValue.fromN("4")),
                        Map.entry("migration_quiet_pass_complete", AttributeValue.fromBool(false))))
                .build());
    when(dynamoDb.transactWriteItems(any(TransactWriteItemsRequest.class)))
        .thenReturn(TransactWriteItemsResponse.builder().build());
    when(dynamoDb.updateItem(any(UpdateItemRequest.class)))
        .thenReturn(UpdateItemResponse.builder().build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    var lease =
        backend
            .acquireLegacyMigrationLease(
                ReconcileJobIndexBackend.LegacyMigration.CLEANUP, "owner-1", 1_000L, 100L)
            .orElseThrow();
    assertEquals(7L, lease.fence());
    assertEquals("page-2", lease.progress().pageToken());
    assertEquals(3, lease.progress().changed());
    assertEquals(1, lease.progress().unresolvable());
    assertEquals(2, lease.progress().conflicted());
    assertEquals(4, lease.progress().retryable());

    assertTrue(
        backend.checkpointLegacyMigration(
            ReconcileJobIndexBackend.LegacyMigration.CLEANUP,
            "owner-1",
            lease.fence(),
            new ReconcileJobIndexBackend.LegacyMigrationProgress("page-3", 3, 1, 2, 4, false),
            1_050L,
            100L));

    ArgumentCaptor<TransactWriteItemsRequest> acquireCaptor =
        ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
    verify(dynamoDb).transactWriteItems(acquireCaptor.capture());
    var acquireItems = acquireCaptor.getValue().transactItems();
    assertEquals(2, acquireItems.size());
    assertEquals(
        "legacy-cleanup-manifest-v2",
        acquireItems.get(0).conditionCheck().key().get(ATTR_SORT_KEY).s());
    assertEquals(
        "attribute_not_exists(#pk)", acquireItems.get(0).conditionCheck().conditionExpression());
    var acquire = acquireItems.get(1).update();
    assertEquals("legacy-cleanup-manifest-v2-progress", acquire.key().get(ATTR_SORT_KEY).s());
    assertTrue(acquire.conditionExpression().contains("#expires <= :now"));
    ArgumentCaptor<UpdateItemRequest> checkpointCaptor =
        ArgumentCaptor.forClass(UpdateItemRequest.class);
    verify(dynamoDb).updateItem(checkpointCaptor.capture());
    UpdateItemRequest checkpoint = checkpointCaptor.getValue();
    assertEquals(
        "#owner = :owner AND #fence = :fence AND #expires >= :now",
        checkpoint.conditionExpression());
    assertEquals("7", checkpoint.expressionAttributeValues().get(":fence").n());
    assertEquals("page-3", checkpoint.expressionAttributeValues().get(":page").s());
  }

  @Test
  void completedLegacyMigrationCleansRaceEraProgressAndCachesCompletion() {
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.getItem(any(GetItemRequest.class)))
        .thenReturn(
            GetItemResponse.builder()
                .item(
                    Map.of(
                        ATTR_PARTITION_KEY,
                        AttributeValue.fromS("reconcile-job-maintenance"),
                        ATTR_SORT_KEY,
                        AttributeValue.fromS("legacy-lookup-v1")))
                .build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    assertTrue(backend.legacyMigrationComplete(ReconcileJobIndexBackend.LegacyMigration.LOOKUP));
    assertTrue(backend.legacyMigrationComplete(ReconcileJobIndexBackend.LegacyMigration.LOOKUP));

    ArgumentCaptor<DeleteItemRequest> captor = ArgumentCaptor.forClass(DeleteItemRequest.class);
    verify(dynamoDb).deleteItem(captor.capture());
    assertEquals("legacy-lookup-v1-progress", captor.getValue().key().get(ATTR_SORT_KEY).s());
    verify(dynamoDb).getItem(any(GetItemRequest.class));
    verify(dynamoDb, never()).transactWriteItems(any(TransactWriteItemsRequest.class));
  }

  @Test
  void completionWinningLeaseAcquisitionRaceCannotRecreateProgress() {
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.getItem(any(GetItemRequest.class)))
        .thenReturn(
            GetItemResponse.builder().build(),
            GetItemResponse.builder()
                .item(
                    Map.of(
                        ATTR_PARTITION_KEY,
                        AttributeValue.fromS("reconcile-job-maintenance"),
                        ATTR_SORT_KEY,
                        AttributeValue.fromS("legacy-cleanup-manifest-v2")))
                .build());
    when(dynamoDb.transactWriteItems(any(TransactWriteItemsRequest.class)))
        .thenThrow(TransactionCanceledException.builder().build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    assertTrue(
        backend
            .acquireLegacyMigrationLease(
                ReconcileJobIndexBackend.LegacyMigration.CLEANUP, "losing-owner", 1_000L, 100L)
            .isEmpty());

    verify(dynamoDb).transactWriteItems(any(TransactWriteItemsRequest.class));
    ArgumentCaptor<DeleteItemRequest> deleteCaptor =
        ArgumentCaptor.forClass(DeleteItemRequest.class);
    verify(dynamoDb).deleteItem(deleteCaptor.capture());
    assertEquals(
        "legacy-cleanup-manifest-v2-progress",
        deleteCaptor.getValue().key().get(ATTR_SORT_KEY).s());
    verify(dynamoDb, never()).updateItem(any(UpdateItemRequest.class));
  }

  @Test
  void staleLegacyMigrationLeaseCannotCheckpoint() {
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.updateItem(any(UpdateItemRequest.class)))
        .thenThrow(ConditionalCheckFailedException.builder().build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    assertFalse(
        backend.checkpointLegacyMigration(
            ReconcileJobIndexBackend.LegacyMigration.CLEANUP,
            "stale-owner",
            1L,
            ReconcileJobIndexBackend.LegacyMigrationProgress.empty(),
            1_000L,
            100L));
  }

  @Test
  void legacyMigrationCompletionDeletesOwnedQuietCheckpointAndWritesMarkerAtomically() {
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().build());
    when(dynamoDb.transactWriteItems(any(TransactWriteItemsRequest.class)))
        .thenReturn(TransactWriteItemsResponse.builder().build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    assertTrue(
        backend.completeLegacyMigration(
            ReconcileJobIndexBackend.LegacyMigration.CLEANUP, "owner-1", 9L, 1_000L));

    ArgumentCaptor<TransactWriteItemsRequest> captor =
        ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
    verify(dynamoDb).transactWriteItems(captor.capture());
    var items = captor.getValue().transactItems();
    assertEquals(2, items.size());
    var checkpointDelete = items.get(0).delete();
    assertEquals(
        "legacy-cleanup-manifest-v2-progress", checkpointDelete.key().get(ATTR_SORT_KEY).s());
    assertTrue(checkpointDelete.conditionExpression().contains("#quiet = :quiet"));
    assertFalse(checkpointDelete.conditionExpression().contains("#unresolvable"));
    assertFalse(checkpointDelete.conditionExpression().contains("#conflicted"));
    assertEquals("owner-1", checkpointDelete.expressionAttributeValues().get(":owner").s());
    assertEquals("9", checkpointDelete.expressionAttributeValues().get(":fence").n());
    assertEquals("legacy-cleanup-manifest-v2", items.get(1).put().item().get(ATTR_SORT_KEY).s());
  }

  @Test
  void lookupCheckAbsentChecksLookupPartition() {
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
        List.of("reconcile-job-lookup", "reconcile-job/by-id"),
        items.stream()
            .map(item -> item.conditionCheck().key().get(ATTR_PARTITION_KEY).s())
            .toList());
    assertTrue(
        items.stream()
            .noneMatch(item -> item.conditionCheck().key().get(ATTR_SORT_KEY).s().isBlank()));
  }

  @Test
  void lookupDeleteUsesOriginalVersionForObservedCurrentPartition() {
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.transactWriteItems(any(TransactWriteItemsRequest.class)))
        .thenReturn(TransactWriteItemsResponse.builder().build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    assertTrue(
        backend.compareAndSetBatch(
            new ReconcileJobIndexStore.JobIndexWriteBatch(
                List.of(
                    new ReconcileJobIndexStore.JobIndexDelete(
                        LOOKUP_KEY, 1L, CANONICAL_KEY, "reconcile-job-lookup")),
                ReconcileJobIndexStore.ReadyQueueMutation.empty())));

    ArgumentCaptor<TransactWriteItemsRequest> captor =
        ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
    verify(dynamoDb).transactWriteItems(captor.capture());
    var items = captor.getValue().transactItems();
    assertEquals(
        "reconcile-job-lookup", items.getFirst().delete().key().get(ATTR_PARTITION_KEY).s());
    assertEquals("1", items.getFirst().delete().expressionAttributeValues().get(":expected").n());
    assertEquals(
        "reconcile-job/by-id", items.get(1).conditionCheck().key().get(ATTR_PARTITION_KEY).s());
  }

  @Test
  void lookupDeleteLeavesConflictingLegacyOwnerUntouched() {
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.transactWriteItems(any(TransactWriteItemsRequest.class)))
        .thenReturn(TransactWriteItemsResponse.builder().build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    assertTrue(
        backend.compareAndSetBatch(
            new ReconcileJobIndexStore.JobIndexWriteBatch(
                List.of(
                    new ReconcileJobIndexStore.JobIndexDelete(
                        LOOKUP_KEY, 1L, CANONICAL_KEY, "reconcile-job-lookup")),
                ReconcileJobIndexStore.ReadyQueueMutation.empty())));

    ArgumentCaptor<TransactWriteItemsRequest> captor =
        ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
    verify(dynamoDb).transactWriteItems(captor.capture());
    var items = captor.getValue().transactItems();
    assertEquals(
        "reconcile-job-lookup", items.getFirst().delete().key().get(ATTR_PARTITION_KEY).s());
    assertEquals(
        "reconcile-job/by-id", items.get(1).conditionCheck().key().get(ATTR_PARTITION_KEY).s());
  }

  @Test
  void ownedReferenceDeleteChecksVersionAndCanonicalOwner() {
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.transactWriteItems(any(TransactWriteItemsRequest.class)))
        .thenReturn(TransactWriteItemsResponse.builder().build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);
    String dedupeKey = Keys.reconcileDedupePointer(ACCOUNT_ID, "hash-1");

    assertTrue(
        backend.compareAndSetBatch(
            new ReconcileJobIndexStore.JobIndexWriteBatch(
                List.of(new ReconcileJobIndexStore.JobIndexDelete(dedupeKey, 1L, CANONICAL_KEY)),
                ReconcileJobIndexStore.ReadyQueueMutation.empty())));

    ArgumentCaptor<TransactWriteItemsRequest> captor =
        ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
    verify(dynamoDb).transactWriteItems(captor.capture());
    var delete = captor.getValue().transactItems().getFirst().delete();
    assertEquals("#v = :expected AND #ref = :reference", delete.conditionExpression());
    assertEquals(
        JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY,
        delete.expressionAttributeNames().get("#ref"));
    assertEquals(CANONICAL_KEY, delete.expressionAttributeValues().get(":reference").s());
  }

  @Test
  void cleanupOwnedReferenceDeleteAllowsAlreadyAbsentRow() {
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.transactWriteItems(any(TransactWriteItemsRequest.class)))
        .thenReturn(TransactWriteItemsResponse.builder().build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);
    String dedupeKey = Keys.reconcileDedupePointer(ACCOUNT_ID, "hash-cleanup");

    assertTrue(
        backend.compareAndSetBatch(
            new ReconcileJobIndexStore.JobIndexWriteBatch(
                List.of(
                    new ReconcileJobIndexStore.JobIndexDelete(
                        dedupeKey, 1L, CANONICAL_KEY, "", false, true)),
                ReconcileJobIndexStore.ReadyQueueMutation.empty())));

    ArgumentCaptor<TransactWriteItemsRequest> captor =
        ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
    verify(dynamoDb).transactWriteItems(captor.capture());
    assertEquals(
        "attribute_not_exists(#pk) OR (#v = :expected AND #ref = :reference)",
        captor.getValue().transactItems().getFirst().delete().conditionExpression());
  }

  @Test
  void physicalWriteItemCountCountsEachPointerMutationOnce() {
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
  void physicalWriteItemCountIncludesBothLookupCompatibilityLocations() {
    var batch =
        new ReconcileJobIndexStore.JobIndexWriteBatch(
            List.of(
                new ReconcileJobIndexStore.JobIndexDelete(LOOKUP_KEY, 1L),
                new ReconcileJobIndexStore.JobIndexCheckAbsent(LOOKUP_KEY)),
            ReconcileJobIndexStore.ReadyQueueMutation.empty());

    assertEquals(4, NativeReconcileJobIndexStore.physicalWriteItemCount(batch));
  }

  @Test
  void rejectsTransactionsOverDynamoLimit() {
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
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
                    writes, ReconcileJobIndexStore.ReadyQueueMutation.empty())));
    verify(dynamoDb, never()).transactWriteItems(any(TransactWriteItemsRequest.class));
  }

  @Test
  void canonicalUpsertStoresCleanupManifestOnCanonicalRow() {
    String readyKey = Keys.reconcileReadyPointerByDue(1L, ACCOUNT_ID, "lane", JOB_ID);
    String projectionKey = Keys.reconcileJobProjectionPointer(ACCOUNT_ID, JOB_ID);
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.transactWriteItems(any(TransactWriteItemsRequest.class)))
        .thenReturn(TransactWriteItemsResponse.builder().build());
    when(dynamoDb.getItem(any(GetItemRequest.class)))
        .thenReturn(
            GetItemResponse.builder()
                .item(
                    Map.of(
                        ATTR_PARTITION_KEY,
                        AttributeValue.fromS("reconcile-job/" + ACCOUNT_ID),
                        ATTR_SORT_KEY,
                        AttributeValue.fromS("job/" + JOB_ID),
                        ATTR_KIND,
                        AttributeValue.fromS(JobIndexBackendSupport.KIND_CANONICAL_JOB),
                        ATTR_VERSION,
                        AttributeValue.fromN("1"),
                        JobIndexBackendSupport.ATTR_POINTER_KEY,
                        AttributeValue.fromS(CANONICAL_KEY),
                        JobIndexBackendSupport.ATTR_BLOB_URI,
                        AttributeValue.fromS("inline:reconcile-job:e30"),
                        JobIndexBackendSupport.ATTR_CLEANUP_MANIFEST_COMPLETE,
                        AttributeValue.fromBool(true),
                        JobIndexBackendSupport.ATTR_CLEANUP_INDEX_POINTER_KEYS,
                        AttributeValue.fromL(List.of(AttributeValue.fromS(LOOKUP_KEY))),
                        JobIndexBackendSupport.ATTR_CLEANUP_READY_POINTER_KEYS,
                        AttributeValue.fromL(List.of(AttributeValue.fromS(readyKey))),
                        JobIndexBackendSupport.ATTR_CLEANUP_POINTER_KEYS,
                        AttributeValue.fromL(List.of(AttributeValue.fromS(projectionKey)))))
                .build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    boolean committed =
        backend.compareAndSetBatch(
            new ReconcileJobIndexStore.JobIndexWriteBatch(
                List.of(
                    new ReconcileJobIndexStore.JobIndexUpsert(
                        CANONICAL_KEY,
                        0L,
                        "inline:reconcile-job:e30",
                        PointerReferenceKind.PRK_INLINE_JSON,
                        new ReconcileJobIndexCleanupManifest(
                            List.of(LOOKUP_KEY), List.of(readyKey), List.of(projectionKey)))),
                ReconcileJobIndexStore.ReadyQueueMutation.empty()));
    var manifest = backend.loadCleanupManifest(CANONICAL_KEY);

    assertTrue(committed);
    ArgumentCaptor<TransactWriteItemsRequest> txCaptor =
        ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
    verify(dynamoDb).transactWriteItems(txCaptor.capture());
    var item = txCaptor.getValue().transactItems().getFirst().put().item();
    assertEquals(
        LOOKUP_KEY,
        item.get(JobIndexBackendSupport.ATTR_CLEANUP_INDEX_POINTER_KEYS).l().getFirst().s());
    assertEquals(
        readyKey,
        item.get(JobIndexBackendSupport.ATTR_CLEANUP_READY_POINTER_KEYS).l().getFirst().s());
    assertEquals(
        projectionKey,
        item.get(JobIndexBackendSupport.ATTR_CLEANUP_POINTER_KEYS).l().getFirst().s());
    assertTrue(item.get(JobIndexBackendSupport.ATTR_CLEANUP_MANIFEST_COMPLETE).bool());
    assertEquals(List.of(LOOKUP_KEY), manifest.indexPointerKeys());
    assertEquals(List.of(readyKey), manifest.readyPointerKeys());
    assertEquals(List.of(projectionKey), manifest.pointerKeys());
    verify(dynamoDb, never()).scan(any(ScanRequest.class));
  }

  @Test
  void cleanupForcesLiveReadyDiscoveryAtReferencePhaseCapacity() {
    String blob = "inline:reconcile-job:e30";
    List<String> readyKeys =
        IntStream.range(0, 99)
            .mapToObj(
                index -> Keys.reconcileReadyPointerByDue(index + 1L, ACCOUNT_ID, "lane", JOB_ID))
            .toList();
    String liveReadyKey = readyKeys.getLast();
    var liveReadyRow = ReadyQueueBackendSupport.toReadyQueueRow(liveReadyKey);
    Map<String, AttributeValue> marker =
        Map.of(ATTR_KIND, AttributeValue.fromS("ReconcileJobLegacyCleanupMigration"));
    Map<String, AttributeValue> canonical = new HashMap<>();
    canonical.put(
        ATTR_PARTITION_KEY,
        AttributeValue.fromS(JobIndexBackendSupport.canonicalPartitionKey(ACCOUNT_ID)));
    canonical.put(ATTR_SORT_KEY, AttributeValue.fromS("job/" + JOB_ID));
    canonical.put(ATTR_KIND, AttributeValue.fromS(JobIndexBackendSupport.KIND_CANONICAL_JOB));
    canonical.put(ATTR_VERSION, AttributeValue.fromN("1"));
    canonical.put(JobIndexBackendSupport.ATTR_POINTER_KEY, AttributeValue.fromS(CANONICAL_KEY));
    canonical.put(JobIndexBackendSupport.ATTR_BLOB_URI, AttributeValue.fromS(blob));
    canonical.put(
        JobIndexBackendSupport.ATTR_CLEANUP_MANIFEST_COMPLETE, AttributeValue.fromBool(true));
    canonical.put(
        JobIndexBackendSupport.ATTR_CLEANUP_INDEX_POINTER_KEYS,
        AttributeValue.fromL(List.of(AttributeValue.fromS(LOOKUP_KEY))));
    canonical.put(
        JobIndexBackendSupport.ATTR_CLEANUP_READY_POINTER_KEYS,
        AttributeValue.fromL(readyKeys.stream().map(AttributeValue::fromS).toList()));
    Map<String, AttributeValue> locked = new HashMap<>(canonical);
    locked.put(ATTR_VERSION, AttributeValue.fromN("2"));
    locked.put(
        JobIndexBackendSupport.ATTR_CLEANUP_DELETE_IN_PROGRESS, AttributeValue.fromBool(true));
    locked.put(
        JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_SCAN_REQUIRED, AttributeValue.fromBool(true));
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.getItem(any(GetItemRequest.class)))
        .thenReturn(
            GetItemResponse.builder().item(marker).build(),
            GetItemResponse.builder().item(canonical).build());
    when(dynamoDb.updateItem(any(UpdateItemRequest.class)))
        .thenReturn(UpdateItemResponse.builder().attributes(locked).build());
    when(dynamoDb.scan(any(ScanRequest.class)))
        .thenReturn(
            ScanResponse.builder()
                .items(
                    Map.of(
                        ATTR_PARTITION_KEY,
                        AttributeValue.fromS(liveReadyRow.partitionKey()),
                        ATTR_SORT_KEY,
                        AttributeValue.fromS(liveReadyRow.sortKey()),
                        ATTR_KIND,
                        AttributeValue.fromS(DynamoReconcileReadyQueueBackend.KIND_READY_ENTRY),
                        DynamoReconcileReadyQueueBackend.ATTR_READY_POINTER_KEY,
                        AttributeValue.fromS(liveReadyKey),
                        DynamoReconcileReadyQueueBackend.ATTR_CANONICAL_POINTER_KEY,
                        AttributeValue.fromS(CANONICAL_KEY)))
                .build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    var session =
        backend.beginJobCleanup(
            new CanonicalPointerSnapshot(CANONICAL_KEY, blob, 1L),
            ReconcileJobIndexCleanupManifest.EMPTY);

    assertTrue(session.isEmpty());
    verify(dynamoDb).scan(any(ScanRequest.class));
    verify(dynamoDb).transactWriteItems(any(TransactWriteItemsRequest.class));
    ArgumentCaptor<UpdateItemRequest> updateCaptor =
        ArgumentCaptor.forClass(UpdateItemRequest.class);
    verify(dynamoDb, times(2)).updateItem(updateCaptor.capture());
    assertTrue(updateCaptor.getAllValues().getFirst().updateExpression().contains("#scan = :true"));
    assertTrue(
        updateCaptor.getAllValues().getLast().updateExpression().contains("#drained = :true"));
  }

  @Test
  void resumedCleanupReplacesPersistedReadyKeysWithLiveDiscovery() {
    String blob = "inline:reconcile-job:e30";
    String staleReadyKey = Keys.reconcileReadyPointerByDue(1L, ACCOUNT_ID, "lane", JOB_ID);
    String liveReadyKey = Keys.reconcileReadyPointerByDue(2L, ACCOUNT_ID, "lane", JOB_ID);
    var liveReadyRow = ReadyQueueBackendSupport.toReadyQueueRow(liveReadyKey);
    Map<String, AttributeValue> marker =
        Map.of(ATTR_KIND, AttributeValue.fromS("ReconcileJobLegacyCleanupMigration"));
    Map<String, AttributeValue> locked = new HashMap<>();
    locked.put(
        ATTR_PARTITION_KEY,
        AttributeValue.fromS(JobIndexBackendSupport.canonicalPartitionKey(ACCOUNT_ID)));
    locked.put(ATTR_SORT_KEY, AttributeValue.fromS("job/" + JOB_ID));
    locked.put(ATTR_KIND, AttributeValue.fromS(JobIndexBackendSupport.KIND_CANONICAL_JOB));
    locked.put(ATTR_VERSION, AttributeValue.fromN("2"));
    locked.put(JobIndexBackendSupport.ATTR_POINTER_KEY, AttributeValue.fromS(CANONICAL_KEY));
    locked.put(JobIndexBackendSupport.ATTR_BLOB_URI, AttributeValue.fromS(blob));
    locked.put(
        JobIndexBackendSupport.ATTR_CLEANUP_DELETE_IN_PROGRESS, AttributeValue.fromBool(true));
    locked.put(
        JobIndexBackendSupport.ATTR_CLEANUP_MANIFEST_COMPLETE, AttributeValue.fromBool(true));
    locked.put(
        JobIndexBackendSupport.ATTR_CLEANUP_INDEX_POINTER_KEYS,
        AttributeValue.fromL(List.of(AttributeValue.fromS(LOOKUP_KEY))));
    locked.put(
        JobIndexBackendSupport.ATTR_CLEANUP_READY_POINTER_KEYS,
        AttributeValue.fromL(
            List.of(AttributeValue.fromS(staleReadyKey), AttributeValue.fromS(liveReadyKey))));
    Map<String, AttributeValue> relocked = new HashMap<>(locked);
    relocked.put(ATTR_VERSION, AttributeValue.fromN("3"));
    relocked.put(
        JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_SCAN_REQUIRED, AttributeValue.fromBool(true));
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.getItem(any(GetItemRequest.class)))
        .thenReturn(
            GetItemResponse.builder().item(marker).build(),
            GetItemResponse.builder().item(locked).build());
    when(dynamoDb.updateItem(any(UpdateItemRequest.class)))
        .thenReturn(UpdateItemResponse.builder().attributes(relocked).build());
    when(dynamoDb.scan(any(ScanRequest.class)))
        .thenReturn(
            ScanResponse.builder()
                .items(
                    Map.of(
                        ATTR_PARTITION_KEY,
                        AttributeValue.fromS(liveReadyRow.partitionKey()),
                        ATTR_SORT_KEY,
                        AttributeValue.fromS(liveReadyRow.sortKey()),
                        ATTR_KIND,
                        AttributeValue.fromS(DynamoReconcileReadyQueueBackend.KIND_READY_ENTRY),
                        DynamoReconcileReadyQueueBackend.ATTR_READY_POINTER_KEY,
                        AttributeValue.fromS(liveReadyKey),
                        DynamoReconcileReadyQueueBackend.ATTR_CANONICAL_POINTER_KEY,
                        AttributeValue.fromS(CANONICAL_KEY)))
                .build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    var session =
        backend.beginJobCleanup(
            new CanonicalPointerSnapshot(CANONICAL_KEY, blob, 2L),
            ReconcileJobIndexCleanupManifest.EMPTY);

    assertTrue(session.isEmpty());
    verify(dynamoDb).scan(any(ScanRequest.class));
    verify(dynamoDb).transactWriteItems(any(TransactWriteItemsRequest.class));
  }

  @Test
  void migratesLegacyCleanupManifestsWithOnePaginatedScan() {
    String parentKey = Keys.reconcileJobByParentPointer(ACCOUNT_ID, "parent-1", JOB_ID);
    String readyKey = Keys.reconcileReadyPointerByDue(1L, ACCOUNT_ID, "lane", JOB_ID);
    var parsedParent = JobIndexBackendSupport.parseParentKey(parentKey);
    var readyRow = ReadyQueueBackendSupport.toReadyQueueRow(readyKey);
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.getItem(any(GetItemRequest.class)))
        .thenReturn(
            GetItemResponse.builder().build(),
            GetItemResponse.builder()
                .item(
                    Map.of(
                        ATTR_PARTITION_KEY,
                        AttributeValue.fromS("reconcile-job/" + ACCOUNT_ID),
                        ATTR_SORT_KEY,
                        AttributeValue.fromS("job/" + JOB_ID),
                        ATTR_KIND,
                        AttributeValue.fromS(JobIndexBackendSupport.KIND_CANONICAL_JOB),
                        ATTR_VERSION,
                        AttributeValue.fromN("1"),
                        JobIndexBackendSupport.ATTR_POINTER_KEY,
                        AttributeValue.fromS(CANONICAL_KEY),
                        JobIndexBackendSupport.ATTR_BLOB_URI,
                        AttributeValue.fromS("unreadable")))
                .build());
    when(dynamoDb.scan(any(ScanRequest.class)))
        .thenReturn(
            ScanResponse.builder()
                .scannedCount(3)
                .items(
                    Map.of(
                        ATTR_PARTITION_KEY,
                        AttributeValue.fromS(
                            JobIndexBackendSupport.canonicalPartitionKey(ACCOUNT_ID)),
                        ATTR_SORT_KEY,
                        AttributeValue.fromS("job/" + JOB_ID),
                        ATTR_KIND,
                        AttributeValue.fromS(JobIndexBackendSupport.KIND_CANONICAL_JOB),
                        JobIndexBackendSupport.ATTR_POINTER_KEY,
                        AttributeValue.fromS(CANONICAL_KEY),
                        JobIndexBackendSupport.ATTR_BLOB_URI,
                        AttributeValue.fromS("unreadable")),
                    Map.of(
                        ATTR_PARTITION_KEY,
                        AttributeValue.fromS(
                            JobIndexBackendSupport.parentPartitionKey(parsedParent)),
                        ATTR_SORT_KEY,
                        AttributeValue.fromS(JobIndexBackendSupport.parentSortKey(parsedParent)),
                        ATTR_KIND,
                        AttributeValue.fromS(JobIndexBackendSupport.KIND_PARENT),
                        JobIndexBackendSupport.ATTR_POINTER_KEY,
                        AttributeValue.fromS(parentKey),
                        JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY,
                        AttributeValue.fromS(CANONICAL_KEY)),
                    Map.of(
                        ATTR_PARTITION_KEY,
                        AttributeValue.fromS(readyRow.partitionKey()),
                        ATTR_SORT_KEY,
                        AttributeValue.fromS(readyRow.sortKey()),
                        ATTR_KIND,
                        AttributeValue.fromS(DynamoReconcileReadyQueueBackend.KIND_READY_ENTRY),
                        DynamoReconcileReadyQueueBackend.ATTR_READY_POINTER_KEY,
                        AttributeValue.fromS(readyKey),
                        DynamoReconcileReadyQueueBackend.ATTR_CANONICAL_POINTER_KEY,
                        AttributeValue.fromS(CANONICAL_KEY)))
                .build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    var result = backend.migrateLegacyCleanupManifests(500, "");

    assertEquals(3, result.scanned());
    assertEquals(1, result.manifestsUpdated());
    assertEquals(0, result.conflicted());
    assertEquals(0, result.retryable());
    assertEquals(List.of(CANONICAL_KEY), result.canonicalPointerKeys());
    ArgumentCaptor<ScanRequest> scanCaptor = ArgumentCaptor.forClass(ScanRequest.class);
    verify(dynamoDb).scan(scanCaptor.capture());
    assertEquals(500, scanCaptor.getValue().limit());
    ArgumentCaptor<UpdateItemRequest> updateCaptor =
        ArgumentCaptor.forClass(UpdateItemRequest.class);
    verify(dynamoDb).updateItem(updateCaptor.capture());
    assertTrue(updateCaptor.getValue().updateExpression().contains("SET #v = :next ADD"));
    assertTrue(
        updateCaptor
            .getValue()
            .expressionAttributeValues()
            .get(":legacyIdx")
            .ss()
            .containsAll(List.of(LOOKUP_KEY, parentKey)));
    assertEquals(
        List.of(readyKey),
        updateCaptor.getValue().expressionAttributeValues().get(":legacyReady").ss());
    assertEquals("2", updateCaptor.getValue().expressionAttributeValues().get(":next").n());
  }

  @Test
  void oversizedLegacyManifestUsesExplicitBoundedFallback() {
    Map<String, AttributeValue> canonical =
        Map.of(
            ATTR_PARTITION_KEY,
            AttributeValue.fromS(JobIndexBackendSupport.canonicalPartitionKey(ACCOUNT_ID)),
            ATTR_SORT_KEY,
            AttributeValue.fromS("job/" + JOB_ID),
            ATTR_KIND,
            AttributeValue.fromS(JobIndexBackendSupport.KIND_CANONICAL_JOB),
            ATTR_VERSION,
            AttributeValue.fromN("1"),
            JobIndexBackendSupport.ATTR_POINTER_KEY,
            AttributeValue.fromS(CANONICAL_KEY),
            JobIndexBackendSupport.ATTR_BLOB_URI,
            AttributeValue.fromS("inline:reconcile-job:e30"));
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.getItem(any(GetItemRequest.class)))
        .thenReturn(
            GetItemResponse.builder().build(), GetItemResponse.builder().item(canonical).build());
    when(dynamoDb.scan(any(ScanRequest.class)))
        .thenReturn(ScanResponse.builder().scannedCount(1).items(canonical).build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.legacyCleanupManifestMaxBytes = 1;
    backend.bind(() -> dynamoDb, TABLE);

    var result = backend.migrateLegacyCleanupManifests(500, "");

    assertEquals(1, result.manifestsUpdated());
    ArgumentCaptor<UpdateItemRequest> update = ArgumentCaptor.forClass(UpdateItemRequest.class);
    verify(dynamoDb).updateItem(update.capture());
    assertTrue(update.getValue().updateExpression().contains("#scan = :true"));
    assertTrue(update.getValue().updateExpression().contains("REMOVE #legacyIdx, #legacyReady"));
  }

  @Test
  void legacyCleanupMigrationRejectsLogicalPointerAtWrongPhysicalKey() {
    String parentKey = Keys.reconcileJobByParentPointer(ACCOUNT_ID, "parent-1", JOB_ID);
    Map<String, AttributeValue> canonical =
        Map.of(
            ATTR_PARTITION_KEY,
            AttributeValue.fromS(JobIndexBackendSupport.canonicalPartitionKey(ACCOUNT_ID)),
            ATTR_SORT_KEY,
            AttributeValue.fromS("job/" + JOB_ID),
            ATTR_KIND,
            AttributeValue.fromS(JobIndexBackendSupport.KIND_CANONICAL_JOB),
            ATTR_VERSION,
            AttributeValue.fromN("1"),
            JobIndexBackendSupport.ATTR_POINTER_KEY,
            AttributeValue.fromS(CANONICAL_KEY));
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.getItem(any(GetItemRequest.class)))
        .thenReturn(
            GetItemResponse.builder().build(), GetItemResponse.builder().item(canonical).build());
    when(dynamoDb.scan(any(ScanRequest.class)))
        .thenReturn(
            ScanResponse.builder()
                .scannedCount(1)
                .items(
                    Map.of(
                        ATTR_PARTITION_KEY,
                        AttributeValue.fromS("wrong-parent-partition"),
                        ATTR_SORT_KEY,
                        AttributeValue.fromS("wrong-parent-sort"),
                        ATTR_KIND,
                        AttributeValue.fromS(JobIndexBackendSupport.KIND_PARENT),
                        JobIndexBackendSupport.ATTR_POINTER_KEY,
                        AttributeValue.fromS(parentKey),
                        JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY,
                        AttributeValue.fromS(CANONICAL_KEY)))
                .build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    var result = backend.migrateLegacyCleanupManifests(500, "");

    assertEquals(1, result.conflicted());
  }

  @Test
  void legacyCleanupMigrationRejectsReadyPointerAtWrongPhysicalKey() {
    String readyKey = Keys.reconcileReadyPointerByDue(1L, ACCOUNT_ID, "lane", JOB_ID);
    Map<String, AttributeValue> canonical =
        Map.of(
            ATTR_PARTITION_KEY,
            AttributeValue.fromS(JobIndexBackendSupport.canonicalPartitionKey(ACCOUNT_ID)),
            ATTR_SORT_KEY,
            AttributeValue.fromS("job/" + JOB_ID),
            ATTR_KIND,
            AttributeValue.fromS(JobIndexBackendSupport.KIND_CANONICAL_JOB),
            ATTR_VERSION,
            AttributeValue.fromN("1"),
            JobIndexBackendSupport.ATTR_POINTER_KEY,
            AttributeValue.fromS(CANONICAL_KEY));
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.getItem(any(GetItemRequest.class)))
        .thenReturn(
            GetItemResponse.builder().build(), GetItemResponse.builder().item(canonical).build());
    when(dynamoDb.scan(any(ScanRequest.class)))
        .thenReturn(
            ScanResponse.builder()
                .scannedCount(1)
                .items(
                    Map.of(
                        ATTR_PARTITION_KEY,
                        AttributeValue.fromS("wrong-ready-partition"),
                        ATTR_SORT_KEY,
                        AttributeValue.fromS("wrong-ready-sort"),
                        ATTR_KIND,
                        AttributeValue.fromS(DynamoReconcileReadyQueueBackend.KIND_READY_ENTRY),
                        DynamoReconcileReadyQueueBackend.ATTR_READY_POINTER_KEY,
                        AttributeValue.fromS(readyKey),
                        DynamoReconcileReadyQueueBackend.ATTR_CANONICAL_POINTER_KEY,
                        AttributeValue.fromS(CANONICAL_KEY)))
                .build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    var result = backend.migrateLegacyCleanupManifests(500, "");

    assertEquals(1, result.conflicted());
  }

  @Test
  void legacyCleanupLocksBeforeStrongOwnedReferenceScan() {
    String parentKey = Keys.reconcileJobByParentPointer(ACCOUNT_ID, "parent-1", JOB_ID);
    var parsedParent = JobIndexBackendSupport.parseParentKey(parentKey);
    String blob = "inline:reconcile-job:e30";
    Map<String, AttributeValue> marker =
        Map.of(ATTR_KIND, AttributeValue.fromS("ReconcileJobLegacyCleanupMigration"));
    Map<String, AttributeValue> canonical = new HashMap<>();
    canonical.put(
        ATTR_PARTITION_KEY,
        AttributeValue.fromS(JobIndexBackendSupport.canonicalPartitionKey(ACCOUNT_ID)));
    canonical.put(ATTR_SORT_KEY, AttributeValue.fromS("job/" + JOB_ID));
    canonical.put(ATTR_KIND, AttributeValue.fromS(JobIndexBackendSupport.KIND_CANONICAL_JOB));
    canonical.put(ATTR_VERSION, AttributeValue.fromN("1"));
    canonical.put(JobIndexBackendSupport.ATTR_POINTER_KEY, AttributeValue.fromS(CANONICAL_KEY));
    canonical.put(JobIndexBackendSupport.ATTR_BLOB_URI, AttributeValue.fromS(blob));
    canonical.put(
        JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_SCAN_REQUIRED, AttributeValue.fromBool(true));
    canonical.put(
        JobIndexBackendSupport.ATTR_CLEANUP_INDEX_POINTER_KEYS,
        AttributeValue.fromL(List.of(AttributeValue.fromS(LOOKUP_KEY))));
    Map<String, AttributeValue> locked = new HashMap<>(canonical);
    locked.put(ATTR_VERSION, AttributeValue.fromN("2"));
    locked.put(
        JobIndexBackendSupport.ATTR_CLEANUP_DELETE_IN_PROGRESS, AttributeValue.fromBool(true));
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.getItem(any(GetItemRequest.class)))
        .thenReturn(
            GetItemResponse.builder().item(marker).build(),
            GetItemResponse.builder().item(canonical).build());
    when(dynamoDb.updateItem(any(UpdateItemRequest.class)))
        .thenReturn(UpdateItemResponse.builder().attributes(locked).build());
    when(dynamoDb.scan(any(ScanRequest.class)))
        .thenReturn(
            ScanResponse.builder()
                .items(
                    Map.of(
                        ATTR_PARTITION_KEY,
                        AttributeValue.fromS(
                            JobIndexBackendSupport.parentPartitionKey(parsedParent)),
                        ATTR_SORT_KEY,
                        AttributeValue.fromS(JobIndexBackendSupport.parentSortKey(parsedParent)),
                        ATTR_KIND,
                        AttributeValue.fromS(JobIndexBackendSupport.KIND_PARENT),
                        JobIndexBackendSupport.ATTR_POINTER_KEY,
                        AttributeValue.fromS(parentKey),
                        JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY,
                        AttributeValue.fromS(CANONICAL_KEY)))
                .build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    var session =
        backend.beginJobCleanup(
            new CanonicalPointerSnapshot(CANONICAL_KEY, blob, 1L),
            ReconcileJobIndexCleanupManifest.EMPTY);

    assertTrue(session.isEmpty());
    InOrder order = inOrder(dynamoDb);
    order.verify(dynamoDb).updateItem(any(UpdateItemRequest.class));
    order.verify(dynamoDb).scan(any(ScanRequest.class));
    order.verify(dynamoDb).transactWriteItems(any(TransactWriteItemsRequest.class));
    order.verify(dynamoDb).updateItem(any(UpdateItemRequest.class));
    ArgumentCaptor<ScanRequest> scanCaptor = ArgumentCaptor.forClass(ScanRequest.class);
    verify(dynamoDb).scan(scanCaptor.capture());
    assertTrue(scanCaptor.getValue().consistentRead());
    assertEquals(100, scanCaptor.getValue().limit());
  }

  @Test
  void legacyCleanupFallbackPersistsCursorAndScansOneBoundedPagePerVisit() {
    String parentKey = Keys.reconcileJobByParentPointer(ACCOUNT_ID, "parent-1", JOB_ID);
    String blob = "inline:reconcile-job:e30";
    Map<String, AttributeValue> marker =
        Map.of(ATTR_KIND, AttributeValue.fromS("ReconcileJobLegacyCleanupMigration"));
    Map<String, AttributeValue> canonical = new HashMap<>();
    canonical.put(
        ATTR_PARTITION_KEY,
        AttributeValue.fromS(JobIndexBackendSupport.canonicalPartitionKey(ACCOUNT_ID)));
    canonical.put(ATTR_SORT_KEY, AttributeValue.fromS("job/" + JOB_ID));
    canonical.put(ATTR_KIND, AttributeValue.fromS(JobIndexBackendSupport.KIND_CANONICAL_JOB));
    canonical.put(ATTR_VERSION, AttributeValue.fromN("1"));
    canonical.put(JobIndexBackendSupport.ATTR_POINTER_KEY, AttributeValue.fromS(CANONICAL_KEY));
    canonical.put(JobIndexBackendSupport.ATTR_BLOB_URI, AttributeValue.fromS(blob));
    canonical.put(
        JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_SCAN_REQUIRED, AttributeValue.fromBool(true));
    Map<String, AttributeValue> locked = new HashMap<>(canonical);
    locked.put(ATTR_VERSION, AttributeValue.fromN("2"));
    locked.put(
        JobIndexBackendSupport.ATTR_CLEANUP_DELETE_IN_PROGRESS, AttributeValue.fromBool(true));
    String cursorPartition = "cursor-partition";
    String cursorSort = "cursor-sort";
    String cursor = ReadyQueueBackendSupport.encodeCursor(cursorPartition, cursorSort);
    Map<String, AttributeValue> resumed = new HashMap<>(locked);
    resumed.put(ATTR_VERSION, AttributeValue.fromN("3"));
    resumed.put(
        JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_SCAN_CURSOR, AttributeValue.fromS(cursor));
    Map<String, AttributeValue> ownedParent =
        Map.of(
            ATTR_PARTITION_KEY,
            AttributeValue.fromS("legacy-parent-partition"),
            ATTR_SORT_KEY,
            AttributeValue.fromS("legacy-parent-sort"),
            ATTR_KIND,
            AttributeValue.fromS(JobIndexBackendSupport.KIND_PARENT),
            JobIndexBackendSupport.ATTR_POINTER_KEY,
            AttributeValue.fromS(parentKey),
            JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY,
            AttributeValue.fromS(CANONICAL_KEY));
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.getItem(any(GetItemRequest.class)))
        .thenReturn(
            GetItemResponse.builder().item(marker).build(),
            GetItemResponse.builder().item(canonical).build(),
            GetItemResponse.builder().item(resumed).build());
    when(dynamoDb.updateItem(any(UpdateItemRequest.class)))
        .thenReturn(UpdateItemResponse.builder().attributes(locked).build());
    when(dynamoDb.scan(any(ScanRequest.class)))
        .thenReturn(
            ScanResponse.builder()
                .items(ownedParent)
                .lastEvaluatedKey(
                    Map.of(
                        ATTR_PARTITION_KEY,
                        AttributeValue.fromS(cursorPartition),
                        ATTR_SORT_KEY,
                        AttributeValue.fromS(cursorSort)))
                .build(),
            ScanResponse.builder().build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.legacyCleanupFallbackScanPageSize = 7;
    backend.bind(() -> dynamoDb, TABLE);

    assertTrue(
        backend
            .beginJobCleanup(
                new CanonicalPointerSnapshot(CANONICAL_KEY, blob, 1L),
                ReconcileJobIndexCleanupManifest.EMPTY)
            .isEmpty());
    assertTrue(
        backend
            .beginJobCleanup(
                new CanonicalPointerSnapshot(CANONICAL_KEY, blob, 3L),
                ReconcileJobIndexCleanupManifest.EMPTY)
            .isEmpty());

    ArgumentCaptor<ScanRequest> scans = ArgumentCaptor.forClass(ScanRequest.class);
    verify(dynamoDb, times(2)).scan(scans.capture());
    assertEquals(7, scans.getAllValues().getFirst().limit());
    assertTrue(scans.getAllValues().getFirst().exclusiveStartKey().isEmpty());
    assertEquals(
        cursorPartition,
        scans.getAllValues().getLast().exclusiveStartKey().get(ATTR_PARTITION_KEY).s());
    assertEquals(
        cursorSort, scans.getAllValues().getLast().exclusiveStartKey().get(ATTR_SORT_KEY).s());
  }

  @Test
  void legacyCleanupPromotesBoundedGlobalManifestWithoutPerJobScan() {
    String parentKey = Keys.reconcileJobByParentPointer(ACCOUNT_ID, "parent-1", JOB_ID);
    String blob = "inline:reconcile-job:e30";
    Map<String, AttributeValue> marker =
        Map.of(ATTR_KIND, AttributeValue.fromS("ReconcileJobLegacyCleanupMigration"));
    Map<String, AttributeValue> canonical = new HashMap<>();
    canonical.put(
        ATTR_PARTITION_KEY,
        AttributeValue.fromS(JobIndexBackendSupport.canonicalPartitionKey(ACCOUNT_ID)));
    canonical.put(ATTR_SORT_KEY, AttributeValue.fromS("job/" + JOB_ID));
    canonical.put(ATTR_KIND, AttributeValue.fromS(JobIndexBackendSupport.KIND_CANONICAL_JOB));
    canonical.put(ATTR_VERSION, AttributeValue.fromN("1"));
    canonical.put(JobIndexBackendSupport.ATTR_POINTER_KEY, AttributeValue.fromS(CANONICAL_KEY));
    canonical.put(JobIndexBackendSupport.ATTR_BLOB_URI, AttributeValue.fromS(blob));
    canonical.put(
        JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_INDEX_POINTER_KEYS,
        AttributeValue.fromL(
            List.of(AttributeValue.fromS(LOOKUP_KEY), AttributeValue.fromS(parentKey))));
    Map<String, AttributeValue> promoted = new HashMap<>(canonical);
    promoted.put(ATTR_VERSION, AttributeValue.fromN("2"));
    promoted.put(
        JobIndexBackendSupport.ATTR_CLEANUP_DELETE_IN_PROGRESS, AttributeValue.fromBool(true));
    promoted.put(
        JobIndexBackendSupport.ATTR_CLEANUP_MANIFEST_COMPLETE, AttributeValue.fromBool(true));
    promoted.put(
        JobIndexBackendSupport.ATTR_CLEANUP_INDEX_POINTER_KEYS,
        AttributeValue.fromL(
            List.of(AttributeValue.fromS(LOOKUP_KEY), AttributeValue.fromS(parentKey))));
    promoted.remove(JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_INDEX_POINTER_KEYS);
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.getItem(any(GetItemRequest.class)))
        .thenReturn(
            GetItemResponse.builder().item(marker).build(),
            GetItemResponse.builder().item(canonical).build());
    when(dynamoDb.updateItem(any(UpdateItemRequest.class)))
        .thenReturn(UpdateItemResponse.builder().attributes(promoted).build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    var session =
        backend.beginJobCleanup(
            new CanonicalPointerSnapshot(CANONICAL_KEY, blob, 1L),
            ReconcileJobIndexCleanupManifest.EMPTY);

    assertTrue(session.isPresent());
    assertEquals(
        List.of(LOOKUP_KEY, parentKey), session.orElseThrow().manifest().indexPointerKeys());
    verify(dynamoDb, never()).scan(any(ScanRequest.class));
    ArgumentCaptor<UpdateItemRequest> update = ArgumentCaptor.forClass(UpdateItemRequest.class);
    verify(dynamoDb).updateItem(update.capture());
    assertTrue(update.getValue().updateExpression().contains("#complete = :true"));
    assertTrue(update.getValue().updateExpression().contains("REMOVE #legacyIdx"));
  }

  @Test
  void legacyCleanupPromotionFailureSwitchesToResumableScan() {
    String blob = "inline:reconcile-job:e30";
    Map<String, AttributeValue> marker =
        Map.of(ATTR_KIND, AttributeValue.fromS("ReconcileJobLegacyCleanupMigration"));
    Map<String, AttributeValue> canonical = new HashMap<>();
    canonical.put(
        ATTR_PARTITION_KEY,
        AttributeValue.fromS(JobIndexBackendSupport.canonicalPartitionKey(ACCOUNT_ID)));
    canonical.put(ATTR_SORT_KEY, AttributeValue.fromS("job/" + JOB_ID));
    canonical.put(ATTR_KIND, AttributeValue.fromS(JobIndexBackendSupport.KIND_CANONICAL_JOB));
    canonical.put(ATTR_VERSION, AttributeValue.fromN("1"));
    canonical.put(JobIndexBackendSupport.ATTR_POINTER_KEY, AttributeValue.fromS(CANONICAL_KEY));
    canonical.put(JobIndexBackendSupport.ATTR_BLOB_URI, AttributeValue.fromS(blob));
    canonical.put(
        JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_INDEX_POINTER_KEYS,
        AttributeValue.fromSs(List.of(LOOKUP_KEY)));
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.getItem(any(GetItemRequest.class)))
        .thenReturn(
            GetItemResponse.builder().item(marker).build(),
            GetItemResponse.builder().item(canonical).build());
    when(dynamoDb.updateItem(any(UpdateItemRequest.class)))
        .thenThrow(new RuntimeException("item too large"))
        .thenReturn(UpdateItemResponse.builder().build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    var session =
        backend.beginJobCleanup(
            new CanonicalPointerSnapshot(CANONICAL_KEY, blob, 1L),
            ReconcileJobIndexCleanupManifest.EMPTY);

    assertTrue(session.isEmpty());
    ArgumentCaptor<UpdateItemRequest> updates = ArgumentCaptor.forClass(UpdateItemRequest.class);
    verify(dynamoDb, times(2)).updateItem(updates.capture());
    assertTrue(updates.getAllValues().getFirst().updateExpression().contains("#complete = :true"));
    assertTrue(updates.getAllValues().getLast().updateExpression().contains("#scan = :true"));
    assertTrue(updates.getAllValues().getLast().updateExpression().contains("REMOVE #drained"));
    verify(dynamoDb, never()).scan(any(ScanRequest.class));
  }

  @Test
  void drainedLegacyCleanupReturnsCanonicalOnlySession() {
    String blob = "inline:reconcile-job:e30";
    Map<String, AttributeValue> marker =
        Map.of(ATTR_KIND, AttributeValue.fromS("ReconcileJobLegacyCleanupMigration"));
    Map<String, AttributeValue> drained = new HashMap<>();
    drained.put(
        ATTR_PARTITION_KEY,
        AttributeValue.fromS(JobIndexBackendSupport.canonicalPartitionKey(ACCOUNT_ID)));
    drained.put(ATTR_SORT_KEY, AttributeValue.fromS("job/" + JOB_ID));
    drained.put(ATTR_KIND, AttributeValue.fromS(JobIndexBackendSupport.KIND_CANONICAL_JOB));
    drained.put(ATTR_VERSION, AttributeValue.fromN("4"));
    drained.put(JobIndexBackendSupport.ATTR_POINTER_KEY, AttributeValue.fromS(CANONICAL_KEY));
    drained.put(JobIndexBackendSupport.ATTR_BLOB_URI, AttributeValue.fromS(blob));
    drained.put(
        JobIndexBackendSupport.ATTR_CLEANUP_DELETE_IN_PROGRESS, AttributeValue.fromBool(true));
    drained.put(
        JobIndexBackendSupport.ATTR_CLEANUP_MANIFEST_COMPLETE, AttributeValue.fromBool(true));
    drained.put(
        JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_SCAN_DRAINED, AttributeValue.fromBool(true));
    drained.put(
        JobIndexBackendSupport.ATTR_CLEANUP_INDEX_POINTER_KEYS, AttributeValue.fromL(List.of()));
    drained.put(
        JobIndexBackendSupport.ATTR_CLEANUP_READY_POINTER_KEYS, AttributeValue.fromL(List.of()));
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.getItem(any(GetItemRequest.class)))
        .thenReturn(
            GetItemResponse.builder().item(marker).build(),
            GetItemResponse.builder().item(drained).build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    var session =
        backend.beginJobCleanup(
            new CanonicalPointerSnapshot(CANONICAL_KEY, blob, 4L),
            ReconcileJobIndexCleanupManifest.EMPTY);

    assertTrue(session.isPresent());
    assertTrue(session.orElseThrow().manifest().isEmpty());
    assertTrue(session.orElseThrow().footprintDrained());
    verify(dynamoDb, never()).scan(any(ScanRequest.class));
    verify(dynamoDb, never()).updateItem(any(UpdateItemRequest.class));
  }

  @Test
  void legacyCleanupResumesPersistedLockAfterFreshDiscoveryFailure() {
    String blob = "inline:reconcile-job:e30";
    Map<String, AttributeValue> marker =
        Map.of(ATTR_KIND, AttributeValue.fromS("ReconcileJobLegacyCleanupMigration"));
    Map<String, AttributeValue> canonical = new HashMap<>();
    canonical.put(
        ATTR_PARTITION_KEY,
        AttributeValue.fromS(JobIndexBackendSupport.canonicalPartitionKey(ACCOUNT_ID)));
    canonical.put(ATTR_SORT_KEY, AttributeValue.fromS("job/" + JOB_ID));
    canonical.put(ATTR_KIND, AttributeValue.fromS(JobIndexBackendSupport.KIND_CANONICAL_JOB));
    canonical.put(ATTR_VERSION, AttributeValue.fromN("1"));
    canonical.put(JobIndexBackendSupport.ATTR_POINTER_KEY, AttributeValue.fromS(CANONICAL_KEY));
    canonical.put(JobIndexBackendSupport.ATTR_BLOB_URI, AttributeValue.fromS(blob));
    canonical.put(
        JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_SCAN_REQUIRED, AttributeValue.fromBool(true));
    canonical.put(
        JobIndexBackendSupport.ATTR_CLEANUP_INDEX_POINTER_KEYS,
        AttributeValue.fromL(List.of(AttributeValue.fromS(LOOKUP_KEY))));
    Map<String, AttributeValue> locked = new HashMap<>(canonical);
    locked.put(ATTR_VERSION, AttributeValue.fromN("2"));
    locked.put(
        JobIndexBackendSupport.ATTR_CLEANUP_DELETE_IN_PROGRESS, AttributeValue.fromBool(true));
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.getItem(any(GetItemRequest.class)))
        .thenReturn(
            GetItemResponse.builder().item(marker).build(),
            GetItemResponse.builder().item(canonical).build(),
            GetItemResponse.builder().item(locked).build());
    when(dynamoDb.updateItem(any(UpdateItemRequest.class)))
        .thenReturn(UpdateItemResponse.builder().attributes(locked).build());
    when(dynamoDb.scan(any(ScanRequest.class)))
        .thenThrow(new RuntimeException("transient scan failure"))
        .thenReturn(ScanResponse.builder().build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    var first =
        backend.beginJobCleanup(
            new CanonicalPointerSnapshot(CANONICAL_KEY, blob, 1L),
            ReconcileJobIndexCleanupManifest.EMPTY);
    var resumed =
        backend.beginJobCleanup(
            new CanonicalPointerSnapshot(CANONICAL_KEY, blob, 2L),
            ReconcileJobIndexCleanupManifest.EMPTY);

    assertTrue(first.isEmpty());
    assertTrue(resumed.isEmpty());
    verify(dynamoDb, times(2)).updateItem(any(UpdateItemRequest.class));
    verify(dynamoDb, times(2)).scan(any(ScanRequest.class));
  }

  @Test
  void legacyCleanupStrongScanRebuildsMalformedPersistedManifest() {
    String blob = "inline:reconcile-job:e30";
    Map<String, AttributeValue> marker =
        Map.of(ATTR_KIND, AttributeValue.fromS("ReconcileJobLegacyCleanupMigration"));
    Map<String, AttributeValue> canonical = new HashMap<>();
    canonical.put(
        ATTR_PARTITION_KEY,
        AttributeValue.fromS(JobIndexBackendSupport.canonicalPartitionKey(ACCOUNT_ID)));
    canonical.put(ATTR_SORT_KEY, AttributeValue.fromS("job/" + JOB_ID));
    canonical.put(ATTR_KIND, AttributeValue.fromS(JobIndexBackendSupport.KIND_CANONICAL_JOB));
    canonical.put(ATTR_VERSION, AttributeValue.fromN("1"));
    canonical.put(JobIndexBackendSupport.ATTR_POINTER_KEY, AttributeValue.fromS(CANONICAL_KEY));
    canonical.put(JobIndexBackendSupport.ATTR_BLOB_URI, AttributeValue.fromS(blob));
    canonical.put(
        JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_SCAN_REQUIRED, AttributeValue.fromBool(true));
    canonical.put(
        JobIndexBackendSupport.ATTR_CLEANUP_INDEX_POINTER_KEYS,
        AttributeValue.fromL(List.of(AttributeValue.fromS("/malformed/cleanup/key"))));
    Map<String, AttributeValue> locked = new HashMap<>(canonical);
    locked.put(ATTR_VERSION, AttributeValue.fromN("2"));
    locked.put(
        JobIndexBackendSupport.ATTR_CLEANUP_DELETE_IN_PROGRESS, AttributeValue.fromBool(true));
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.getItem(any(GetItemRequest.class)))
        .thenReturn(
            GetItemResponse.builder().item(marker).build(),
            GetItemResponse.builder().item(canonical).build());
    when(dynamoDb.updateItem(any(UpdateItemRequest.class)))
        .thenReturn(UpdateItemResponse.builder().attributes(locked).build());
    when(dynamoDb.scan(any(ScanRequest.class))).thenReturn(ScanResponse.builder().build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    var session =
        backend.beginJobCleanup(
            new CanonicalPointerSnapshot(CANONICAL_KEY, blob, 1L),
            ReconcileJobIndexCleanupManifest.EMPTY);

    assertTrue(session.isEmpty());
    verify(dynamoDb).scan(any(ScanRequest.class));
  }

  @Test
  void legacyCleanupConditionallyDeletesOwnedReferenceAtWrongPhysicalKey() {
    String parentKey = Keys.reconcileJobByParentPointer(ACCOUNT_ID, "parent-1", JOB_ID);
    String blob = "inline:reconcile-job:e30";
    Map<String, AttributeValue> marker =
        Map.of(ATTR_KIND, AttributeValue.fromS("ReconcileJobLegacyCleanupMigration"));
    Map<String, AttributeValue> canonical = new HashMap<>();
    canonical.put(
        ATTR_PARTITION_KEY,
        AttributeValue.fromS(JobIndexBackendSupport.canonicalPartitionKey(ACCOUNT_ID)));
    canonical.put(ATTR_SORT_KEY, AttributeValue.fromS("job/" + JOB_ID));
    canonical.put(ATTR_KIND, AttributeValue.fromS(JobIndexBackendSupport.KIND_CANONICAL_JOB));
    canonical.put(ATTR_VERSION, AttributeValue.fromN("1"));
    canonical.put(JobIndexBackendSupport.ATTR_POINTER_KEY, AttributeValue.fromS(CANONICAL_KEY));
    canonical.put(JobIndexBackendSupport.ATTR_BLOB_URI, AttributeValue.fromS(blob));
    canonical.put(
        JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_SCAN_REQUIRED, AttributeValue.fromBool(true));
    Map<String, AttributeValue> locked = new HashMap<>(canonical);
    locked.put(ATTR_VERSION, AttributeValue.fromN("2"));
    locked.put(
        JobIndexBackendSupport.ATTR_CLEANUP_DELETE_IN_PROGRESS, AttributeValue.fromBool(true));
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.getItem(any(GetItemRequest.class)))
        .thenReturn(
            GetItemResponse.builder().item(marker).build(),
            GetItemResponse.builder().item(canonical).build());
    when(dynamoDb.updateItem(any(UpdateItemRequest.class)))
        .thenReturn(UpdateItemResponse.builder().attributes(locked).build());
    when(dynamoDb.scan(any(ScanRequest.class)))
        .thenReturn(
            ScanResponse.builder()
                .items(
                    Map.of(
                        ATTR_PARTITION_KEY,
                        AttributeValue.fromS("wrong-parent-partition"),
                        ATTR_SORT_KEY,
                        AttributeValue.fromS("wrong-parent-sort"),
                        ATTR_VERSION,
                        AttributeValue.fromN("7"),
                        ATTR_KIND,
                        AttributeValue.fromS(JobIndexBackendSupport.KIND_PARENT),
                        JobIndexBackendSupport.ATTR_POINTER_KEY,
                        AttributeValue.fromS(parentKey),
                        JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY,
                        AttributeValue.fromS(CANONICAL_KEY)))
                .build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    var session =
        backend.beginJobCleanup(
            new CanonicalPointerSnapshot(CANONICAL_KEY, blob, 1L),
            ReconcileJobIndexCleanupManifest.EMPTY);

    assertTrue(session.isEmpty());
    InOrder order = inOrder(dynamoDb);
    order.verify(dynamoDb).updateItem(any(UpdateItemRequest.class));
    order.verify(dynamoDb).scan(any(ScanRequest.class));
    order.verify(dynamoDb).transactWriteItems(any(TransactWriteItemsRequest.class));
    ArgumentCaptor<TransactWriteItemsRequest> deleteCaptor =
        ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
    verify(dynamoDb).transactWriteItems(deleteCaptor.capture());
    var delete = deleteCaptor.getValue().transactItems().get(1).delete();
    assertEquals("wrong-parent-partition", delete.key().get(ATTR_PARTITION_KEY).s());
    assertEquals("wrong-parent-sort", delete.key().get(ATTR_SORT_KEY).s());
    assertEquals(
        JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY,
        delete.expressionAttributeNames().get("#owner"));
    assertEquals("7", delete.expressionAttributeValues().get(":version").n());
    assertEquals(CANONICAL_KEY, delete.expressionAttributeValues().get(":canonical").s());
  }

  @Test
  void legacyCleanupRetriesWhenMalformedPhysicalDeleteLosesRace() {
    String parentKey = Keys.reconcileJobByParentPointer(ACCOUNT_ID, "parent-1", JOB_ID);
    String blob = "inline:reconcile-job:e30";
    Map<String, AttributeValue> marker =
        Map.of(ATTR_KIND, AttributeValue.fromS("ReconcileJobLegacyCleanupMigration"));
    Map<String, AttributeValue> canonical = new HashMap<>();
    canonical.put(
        ATTR_PARTITION_KEY,
        AttributeValue.fromS(JobIndexBackendSupport.canonicalPartitionKey(ACCOUNT_ID)));
    canonical.put(ATTR_SORT_KEY, AttributeValue.fromS("job/" + JOB_ID));
    canonical.put(ATTR_KIND, AttributeValue.fromS(JobIndexBackendSupport.KIND_CANONICAL_JOB));
    canonical.put(ATTR_VERSION, AttributeValue.fromN("1"));
    canonical.put(JobIndexBackendSupport.ATTR_POINTER_KEY, AttributeValue.fromS(CANONICAL_KEY));
    canonical.put(JobIndexBackendSupport.ATTR_BLOB_URI, AttributeValue.fromS(blob));
    canonical.put(
        JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_SCAN_REQUIRED, AttributeValue.fromBool(true));
    Map<String, AttributeValue> locked = new HashMap<>(canonical);
    locked.put(ATTR_VERSION, AttributeValue.fromN("2"));
    locked.put(
        JobIndexBackendSupport.ATTR_CLEANUP_DELETE_IN_PROGRESS, AttributeValue.fromBool(true));
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.getItem(any(GetItemRequest.class)))
        .thenReturn(
            GetItemResponse.builder().item(marker).build(),
            GetItemResponse.builder().item(canonical).build());
    when(dynamoDb.updateItem(any(UpdateItemRequest.class)))
        .thenReturn(UpdateItemResponse.builder().attributes(locked).build());
    when(dynamoDb.scan(any(ScanRequest.class)))
        .thenReturn(
            ScanResponse.builder()
                .items(
                    Map.of(
                        ATTR_PARTITION_KEY,
                        AttributeValue.fromS("wrong-parent-partition"),
                        ATTR_SORT_KEY,
                        AttributeValue.fromS("wrong-parent-sort"),
                        ATTR_VERSION,
                        AttributeValue.fromN("7"),
                        ATTR_KIND,
                        AttributeValue.fromS(JobIndexBackendSupport.KIND_PARENT),
                        JobIndexBackendSupport.ATTR_POINTER_KEY,
                        AttributeValue.fromS(parentKey),
                        JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY,
                        AttributeValue.fromS(CANONICAL_KEY)))
                .build());
    when(dynamoDb.transactWriteItems(any(TransactWriteItemsRequest.class)))
        .thenThrow(TransactionCanceledException.builder().message("lost race").build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    var session =
        backend.beginJobCleanup(
            new CanonicalPointerSnapshot(CANONICAL_KEY, blob, 1L),
            ReconcileJobIndexCleanupManifest.EMPTY);

    assertTrue(session.isEmpty());
    verify(dynamoDb).transactWriteItems(any(TransactWriteItemsRequest.class));
    verify(dynamoDb).updateItem(any(UpdateItemRequest.class));
  }

  @Test
  void legacyCleanupNeverRawDeletesCanonicalPhysicalRow() {
    Map<String, AttributeValue> marker =
        Map.of(ATTR_KIND, AttributeValue.fromS("ReconcileJobLegacyCleanupMigration"));
    Map<String, AttributeValue> canonical = new HashMap<>();
    canonical.put(
        ATTR_PARTITION_KEY,
        AttributeValue.fromS(JobIndexBackendSupport.canonicalPartitionKey(ACCOUNT_ID)));
    canonical.put(ATTR_SORT_KEY, AttributeValue.fromS("job/" + JOB_ID));
    canonical.put(ATTR_KIND, AttributeValue.fromS(JobIndexBackendSupport.KIND_CANONICAL_JOB));
    canonical.put(ATTR_VERSION, AttributeValue.fromN("1"));
    canonical.put(JobIndexBackendSupport.ATTR_POINTER_KEY, AttributeValue.fromS(CANONICAL_KEY));
    canonical.put(JobIndexBackendSupport.ATTR_BLOB_URI, AttributeValue.fromS(CANONICAL_KEY));
    canonical.put(
        JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_SCAN_REQUIRED, AttributeValue.fromBool(true));
    Map<String, AttributeValue> locked = new HashMap<>(canonical);
    locked.put(ATTR_VERSION, AttributeValue.fromN("2"));
    locked.put(
        JobIndexBackendSupport.ATTR_CLEANUP_DELETE_IN_PROGRESS, AttributeValue.fromBool(true));
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.getItem(any(GetItemRequest.class)))
        .thenReturn(
            GetItemResponse.builder().item(marker).build(),
            GetItemResponse.builder().item(canonical).build());
    when(dynamoDb.updateItem(any(UpdateItemRequest.class)))
        .thenReturn(UpdateItemResponse.builder().attributes(locked).build());
    when(dynamoDb.scan(any(ScanRequest.class)))
        .thenReturn(ScanResponse.builder().items(locked).build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    var session =
        backend.beginJobCleanup(
            new CanonicalPointerSnapshot(CANONICAL_KEY, CANONICAL_KEY, 1L),
            ReconcileJobIndexCleanupManifest.EMPTY);

    assertTrue(session.isEmpty());
    verify(dynamoDb, never()).transactWriteItems(any(TransactWriteItemsRequest.class));
  }

  @Test
  void legacyCleanupMigrationReportsUnresolvableRowsWithoutBlockingConflicts() {
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().build());
    when(dynamoDb.scan(any(ScanRequest.class)))
        .thenReturn(
            ScanResponse.builder()
                .scannedCount(1)
                .items(
                    Map.of(
                        ATTR_PARTITION_KEY,
                        AttributeValue.fromS("corrupt-parent-pk"),
                        ATTR_SORT_KEY,
                        AttributeValue.fromS("corrupt-parent-sk"),
                        ATTR_KIND,
                        AttributeValue.fromS(JobIndexBackendSupport.KIND_PARENT),
                        JobIndexBackendSupport.ATTR_POINTER_KEY,
                        AttributeValue.fromS("/corrupt/legacy/parent"),
                        JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY,
                        AttributeValue.fromS("not-a-canonical-pointer")))
                .build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    var result = backend.migrateLegacyCleanupManifests(500, "");

    assertEquals(1, result.scanned());
    assertEquals(0, result.manifestsUpdated());
    assertEquals(1, result.unresolvable());
    assertEquals(0, result.conflicted());
    assertEquals(0, result.retryable());
    verify(dynamoDb, never()).updateItem(any(UpdateItemRequest.class));
    verify(dynamoDb, never()).transactWriteItems(any(TransactWriteItemsRequest.class));
  }

  @Test
  void legacyCleanupMigrationConditionallyDeletesOrphanedIndexRows() {
    String parentKey = Keys.reconcileJobByParentPointer(ACCOUNT_ID, "parent-1", JOB_ID);
    var parsedParent = JobIndexBackendSupport.parseParentKey(parentKey);
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.getItem(any(GetItemRequest.class)))
        .thenAnswer(
            invocation -> {
              GetItemRequest request = invocation.getArgument(0);
              String partitionKey = request.key().get(ATTR_PARTITION_KEY).s();
              if (!JobIndexBackendSupport.parentPartitionKey(parsedParent).equals(partitionKey)) {
                return GetItemResponse.builder().build();
              }
              return GetItemResponse.builder()
                  .item(
                      Map.of(
                          ATTR_PARTITION_KEY,
                          AttributeValue.fromS(partitionKey),
                          ATTR_SORT_KEY,
                          AttributeValue.fromS(JobIndexBackendSupport.parentSortKey(parsedParent)),
                          ATTR_KIND,
                          AttributeValue.fromS(JobIndexBackendSupport.KIND_PARENT),
                          ATTR_VERSION,
                          AttributeValue.fromN("1"),
                          JobIndexBackendSupport.ATTR_POINTER_KEY,
                          AttributeValue.fromS(parentKey),
                          JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY,
                          AttributeValue.fromS(CANONICAL_KEY)))
                  .build();
            });
    when(dynamoDb.scan(any(ScanRequest.class)))
        .thenReturn(
            ScanResponse.builder()
                .scannedCount(1)
                .items(
                    Map.of(
                        ATTR_PARTITION_KEY,
                        AttributeValue.fromS(
                            JobIndexBackendSupport.parentPartitionKey(parsedParent)),
                        ATTR_SORT_KEY,
                        AttributeValue.fromS(JobIndexBackendSupport.parentSortKey(parsedParent)),
                        ATTR_KIND,
                        AttributeValue.fromS(JobIndexBackendSupport.KIND_PARENT),
                        JobIndexBackendSupport.ATTR_POINTER_KEY,
                        AttributeValue.fromS(parentKey),
                        JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY,
                        AttributeValue.fromS(CANONICAL_KEY)))
                .build());
    when(dynamoDb.transactWriteItems(any(TransactWriteItemsRequest.class)))
        .thenReturn(TransactWriteItemsResponse.builder().build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    var result = backend.migrateLegacyCleanupManifests(500, "");

    assertEquals(1, result.scanned());
    assertEquals(1, result.manifestsUpdated());
    assertEquals(0, result.conflicted());
    assertEquals(0, result.retryable());
    ArgumentCaptor<TransactWriteItemsRequest> txCaptor =
        ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
    verify(dynamoDb).transactWriteItems(txCaptor.capture());
    var tx = txCaptor.getValue().transactItems();
    assertEquals(2, tx.size());
    assertEquals(
        JobIndexBackendSupport.canonicalPartitionKey(ACCOUNT_ID),
        tx.getFirst().conditionCheck().key().get(ATTR_PARTITION_KEY).s());
    assertEquals(
        JobIndexBackendSupport.parentPartitionKey(parsedParent),
        tx.get(1).delete().key().get(ATTR_PARTITION_KEY).s());
    assertEquals(
        CANONICAL_KEY, tx.get(1).delete().expressionAttributeValues().get(":reference").s());
  }

  @Test
  void concurrentLegacyCleanupMergeIsRecognizedAsCompleted() {
    Map<String, AttributeValue> legacyCanonical =
        Map.of(
            ATTR_PARTITION_KEY,
            AttributeValue.fromS("reconcile-job/" + ACCOUNT_ID),
            ATTR_SORT_KEY,
            AttributeValue.fromS("job/" + JOB_ID),
            ATTR_KIND,
            AttributeValue.fromS(JobIndexBackendSupport.KIND_CANONICAL_JOB),
            ATTR_VERSION,
            AttributeValue.fromN("1"),
            JobIndexBackendSupport.ATTR_POINTER_KEY,
            AttributeValue.fromS(CANONICAL_KEY),
            JobIndexBackendSupport.ATTR_BLOB_URI,
            AttributeValue.fromS("unreadable"));
    Map<String, AttributeValue> concurrentlyUpdated = new HashMap<>(legacyCanonical);
    concurrentlyUpdated.put(
        JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_SCAN_REQUIRED, AttributeValue.fromBool(true));
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.getItem(any(GetItemRequest.class)))
        .thenReturn(
            GetItemResponse.builder().build(),
            GetItemResponse.builder().item(legacyCanonical).build(),
            GetItemResponse.builder().item(concurrentlyUpdated).build());
    when(dynamoDb.scan(any(ScanRequest.class)))
        .thenReturn(ScanResponse.builder().scannedCount(1).items(legacyCanonical).build());
    when(dynamoDb.updateItem(any(UpdateItemRequest.class)))
        .thenThrow(ConditionalCheckFailedException.builder().build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    var result = backend.migrateLegacyCleanupManifests(500, "");

    assertEquals(1, result.scanned());
    assertEquals(0, result.manifestsUpdated());
    assertEquals(0, result.conflicted());
    assertEquals(0, result.retryable());
  }

  @Test
  void legacyCleanupMigrationRecognizesAlreadyAccumulatedCompleteCanonical() {
    String parentKey = Keys.reconcileJobByParentPointer(ACCOUNT_ID, "legacy-parent", JOB_ID);
    var parsedParent = JobIndexBackendSupport.parseParentKey(parentKey);
    Map<String, AttributeValue> completeCanonical =
        Map.ofEntries(
            Map.entry(ATTR_PARTITION_KEY, AttributeValue.fromS("reconcile-job/" + ACCOUNT_ID)),
            Map.entry(ATTR_SORT_KEY, AttributeValue.fromS("job/" + JOB_ID)),
            Map.entry(ATTR_KIND, AttributeValue.fromS(JobIndexBackendSupport.KIND_CANONICAL_JOB)),
            Map.entry(ATTR_VERSION, AttributeValue.fromN("7")),
            Map.entry(JobIndexBackendSupport.ATTR_POINTER_KEY, AttributeValue.fromS(CANONICAL_KEY)),
            Map.entry(
                JobIndexBackendSupport.ATTR_BLOB_URI,
                AttributeValue.fromS("inline:reconcile-job:e30")),
            Map.entry(
                JobIndexBackendSupport.ATTR_CLEANUP_MANIFEST_COMPLETE,
                AttributeValue.fromBool(true)),
            Map.entry(
                JobIndexBackendSupport.ATTR_CLEANUP_INDEX_POINTER_KEYS,
                AttributeValue.fromL(List.of(AttributeValue.fromS(LOOKUP_KEY)))),
            Map.entry(
                JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_INDEX_POINTER_KEYS,
                AttributeValue.fromSs(List.of(parentKey))));
    Map<String, AttributeValue> parentRow =
        Map.of(
            ATTR_PARTITION_KEY,
            AttributeValue.fromS(JobIndexBackendSupport.parentPartitionKey(parsedParent)),
            ATTR_SORT_KEY,
            AttributeValue.fromS(JobIndexBackendSupport.parentSortKey(parsedParent)),
            ATTR_KIND,
            AttributeValue.fromS(JobIndexBackendSupport.KIND_PARENT),
            JobIndexBackendSupport.ATTR_POINTER_KEY,
            AttributeValue.fromS(parentKey),
            JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY,
            AttributeValue.fromS(CANONICAL_KEY));
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.getItem(any(GetItemRequest.class)))
        .thenReturn(
            GetItemResponse.builder().build(),
            GetItemResponse.builder().item(completeCanonical).build());
    when(dynamoDb.scan(any(ScanRequest.class)))
        .thenReturn(ScanResponse.builder().scannedCount(1).items(parentRow).build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    var result = backend.migrateLegacyCleanupManifests(500, "");

    assertEquals(0, result.manifestsUpdated());
    verify(dynamoDb, never()).updateItem(any(UpdateItemRequest.class));
  }

  @Test
  void legacyCleanupManifestRemainsHiddenUntilMigrationMarkerIsWritten() {
    Map<String, AttributeValue> legacyCanonical =
        Map.of(
            ATTR_PARTITION_KEY,
            AttributeValue.fromS("reconcile-job/" + ACCOUNT_ID),
            ATTR_SORT_KEY,
            AttributeValue.fromS("job/" + JOB_ID),
            ATTR_KIND,
            AttributeValue.fromS(JobIndexBackendSupport.KIND_CANONICAL_JOB),
            ATTR_VERSION,
            AttributeValue.fromN("1"),
            JobIndexBackendSupport.ATTR_POINTER_KEY,
            AttributeValue.fromS(CANONICAL_KEY),
            JobIndexBackendSupport.ATTR_BLOB_URI,
            AttributeValue.fromS("unreadable"),
            JobIndexBackendSupport.ATTR_CLEANUP_INDEX_POINTER_KEYS,
            AttributeValue.fromL(List.of(AttributeValue.fromS(LOOKUP_KEY))));
    DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
    when(dynamoDb.getItem(any(GetItemRequest.class)))
        .thenReturn(
            GetItemResponse.builder().item(legacyCanonical).build(),
            GetItemResponse.builder().build(),
            GetItemResponse.builder().build(),
            GetItemResponse.builder().item(legacyCanonical).build());
    DynamoReconcileJobIndexBackend backend = new DynamoReconcileJobIndexBackend();
    backend.bind(() -> dynamoDb, TABLE);

    assertTrue(backend.loadCleanupManifest(CANONICAL_KEY).isEmpty());
    assertTrue(
        backend.completeLegacyMigration(
            ReconcileJobIndexBackend.LegacyMigration.CLEANUP, "owner-1", 7L, 1_000L));
    assertEquals(
        List.of(LOOKUP_KEY), backend.loadCleanupManifest(CANONICAL_KEY).indexPointerKeys());
  }
}
