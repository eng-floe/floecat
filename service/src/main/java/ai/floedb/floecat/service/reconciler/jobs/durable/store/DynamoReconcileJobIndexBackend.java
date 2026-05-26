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

import ai.floedb.floecat.storage.kv.KvAttributes;
import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.Delete;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

@Singleton
@IfBuildProperty(name = "floecat.kv", stringValue = "dynamodb")
public class DynamoReconcileJobIndexBackend implements ReconcileJobIndexBackend {
  @Inject DynamoDbClient dynamoDb;

  @ConfigProperty(name = "floecat.kv.table", defaultValue = "floecat_pointers")
  String table = "floecat_pointers";

  @Inject
  public DynamoReconcileJobIndexBackend() {}

  public void bind(DynamoDbClient dynamoDb, String table) {
    this.dynamoDb = dynamoDb;
    this.table = table;
  }

  @Override
  public Optional<JobIndexEntrySnapshot> loadIndexEntry(String pointerKey) {
    var canonicalKey = JobIndexBackendSupport.parseCanonicalJobKey(pointerKey);
    if (canonicalKey != null) {
      return loadCanonicalPointer(canonicalKey);
    }
    var lookupKey = JobIndexBackendSupport.parseLookupKey(pointerKey);
    if (lookupKey != null) {
      return loadIndexPointer(
          JobIndexBackendSupport.lookupPartitionKey(),
          JobIndexBackendSupport.lookupSortKey(lookupKey),
          JobIndexBackendSupport.ATTR_BLOB_URI);
    }
    var parentKey = JobIndexBackendSupport.parseParentKey(pointerKey);
    if (parentKey != null) {
      return loadIndexPointer(
          JobIndexBackendSupport.parentPartitionKey(parentKey),
          JobIndexBackendSupport.parentSortKey(parentKey),
          JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY);
    }
    var connectorKey = JobIndexBackendSupport.parseConnectorKey(pointerKey);
    if (connectorKey != null) {
      return loadIndexPointer(
          JobIndexBackendSupport.connectorPartitionKey(connectorKey),
          JobIndexBackendSupport.connectorSortKey(connectorKey),
          JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY);
    }
    var globalStateKey = JobIndexBackendSupport.parseGlobalStateKey(pointerKey);
    if (globalStateKey != null) {
      return loadIndexPointer(
          JobIndexBackendSupport.globalStatePartitionKey(globalStateKey),
          JobIndexBackendSupport.globalStateSortKey(globalStateKey),
          JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY);
    }
    var accountStateKey = JobIndexBackendSupport.parseAccountStateKey(pointerKey);
    if (accountStateKey != null) {
      return loadIndexPointer(
          JobIndexBackendSupport.accountStatePartitionKey(accountStateKey),
          JobIndexBackendSupport.accountStateSortKey(accountStateKey),
          JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY);
    }
    var connectorStateKey = JobIndexBackendSupport.parseConnectorStateKey(pointerKey);
    if (connectorStateKey != null) {
      return loadIndexPointer(
          JobIndexBackendSupport.connectorStatePartitionKey(connectorStateKey),
          JobIndexBackendSupport.connectorStateSortKey(connectorStateKey),
          JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY);
    }
    var dedupeKey = JobIndexBackendSupport.parseDedupeKey(pointerKey);
    if (dedupeKey != null) {
      return loadIndexPointer(
          JobIndexBackendSupport.dedupePartitionKey(dedupeKey),
          JobIndexBackendSupport.dedupeSortKey(dedupeKey),
          JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY);
    }
    return Optional.empty();
  }

  @Override
  public boolean compareAndSetBatch(ReconcileJobIndexStore.JobIndexWriteBatch batch) {
    return compareAndSetDynamo(batch);
  }

  @Override
  public JobIndexQueryPage listCanonicalEntries(String accountId, int limit, String pageToken) {
    if (blank(accountId)) {
      return new JobIndexQueryPage(List.of(), "");
    }
    String partitionKey = JobIndexBackendSupport.canonicalPartitionKey(accountId);
    return blank(partitionKey)
        ? new JobIndexQueryPage(List.of(), "")
        : listIndexPointers(partitionKey, pageToken, limit, JobIndexBackendSupport.ATTR_BLOB_URI);
  }

  @Override
  public JobIndexQueryPage listDedupeEntries(String accountId, int limit, String pageToken) {
    if (blank(accountId)) {
      return new JobIndexQueryPage(List.of(), "");
    }
    String partitionKey = JobIndexBackendSupport.dedupePartitionKey(accountId);
    return blank(partitionKey)
        ? new JobIndexQueryPage(List.of(), "")
        : listIndexPointers(
            partitionKey, pageToken, limit, JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY);
  }

  @Override
  public JobIndexQueryPage listParentEntries(
      String accountId, String parentJobId, int limit, String pageToken) {
    if (blank(accountId) || blank(parentJobId)) {
      return new JobIndexQueryPage(List.of(), "");
    }
    String partitionKey = JobIndexBackendSupport.parentPartitionKey(accountId, parentJobId);
    return blank(partitionKey)
        ? new JobIndexQueryPage(List.of(), "")
        : listIndexPointers(
            partitionKey, pageToken, limit, JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY);
  }

  @Override
  public JobIndexQueryPage listConnectorEntries(
      String accountId, String connectorId, int limit, String pageToken) {
    if (blank(accountId) || blank(connectorId)) {
      return new JobIndexQueryPage(List.of(), "");
    }
    String partitionKey = JobIndexBackendSupport.connectorPartitionKey(accountId, connectorId);
    return blank(partitionKey)
        ? new JobIndexQueryPage(List.of(), "")
        : listIndexPointers(
            partitionKey, pageToken, limit, JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY);
  }

  @Override
  public JobIndexQueryPage listGlobalStateEntries(String state, int limit, String pageToken) {
    if (blank(state)) {
      return new JobIndexQueryPage(List.of(), "");
    }
    String partitionKey = JobIndexBackendSupport.globalStatePartitionKey(state);
    return blank(partitionKey)
        ? new JobIndexQueryPage(List.of(), "")
        : listIndexPointers(
            partitionKey, pageToken, limit, JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY);
  }

  @Override
  public JobIndexQueryPage listAccountStateEntries(
      String accountId, String state, int limit, String pageToken) {
    if (blank(accountId) || blank(state)) {
      return new JobIndexQueryPage(List.of(), "");
    }
    String partitionKey = JobIndexBackendSupport.accountStatePartitionKey(accountId, state);
    return blank(partitionKey)
        ? new JobIndexQueryPage(List.of(), "")
        : listIndexPointers(
            partitionKey, pageToken, limit, JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY);
  }

  @Override
  public JobIndexQueryPage listConnectorStateEntries(
      String accountId, String connectorId, String state, int limit, String pageToken) {
    if (blank(accountId) || blank(connectorId) || blank(state)) {
      return new JobIndexQueryPage(List.of(), "");
    }
    String partitionKey =
        JobIndexBackendSupport.connectorStatePartitionKey(accountId, connectorId, state);
    return blank(partitionKey)
        ? new JobIndexQueryPage(List.of(), "")
        : listIndexPointers(
            partitionKey, pageToken, limit, JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY);
  }

  @Override
  public boolean purgeEntriesByCanonicalReference(String canonicalPointerKey) {
    if (blank(canonicalPointerKey)) {
      return false;
    }
    boolean deleted = false;
    Map<String, AttributeValue> exclusiveStartKey = null;
    do {
      ScanRequest.Builder request =
          ScanRequest.builder()
              .tableName(table)
              .consistentRead(true)
              .expressionAttributeNames(
                  Map.of(
                      "#pointer", JobIndexBackendSupport.ATTR_POINTER_KEY,
                      "#canonical", JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY,
                      "#blob", JobIndexBackendSupport.ATTR_BLOB_URI))
              .filterExpression(
                  "#pointer = :canonical OR #canonical = :canonical OR #blob = :canonical")
              .expressionAttributeValues(
                  Map.of(":canonical", AttributeValue.fromS(canonicalPointerKey)));
      if (exclusiveStartKey != null && !exclusiveStartKey.isEmpty()) {
        request.exclusiveStartKey(exclusiveStartKey);
      }
      var response = dynamoDb.scan(request.build());
      for (var item : response.items()) {
        if (item == null || item.isEmpty()) {
          continue;
        }
        dynamoDb.deleteItem(
            software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest.builder()
                .tableName(table)
                .key(
                    Map.of(
                        ATTR_PARTITION_KEY, item.get(ATTR_PARTITION_KEY),
                        ATTR_SORT_KEY, item.get(ATTR_SORT_KEY)))
                .build());
        deleted = true;
      }
      exclusiveStartKey = response.lastEvaluatedKey();
    } while (exclusiveStartKey != null && !exclusiveStartKey.isEmpty());
    return deleted;
  }

  private boolean compareAndSetDynamo(ReconcileJobIndexStore.JobIndexWriteBatch batch) {
    List<TransactWriteItem> tx = new ArrayList<>();
    for (ReconcileJobIndexStore.JobIndexWriteOp op : batch.writes()) {
      if (op instanceof ReconcileJobIndexStore.JobIndexUpsert upsert) {
        tx.add(buildPointerUpsert(upsert));
      } else if (op instanceof ReconcileJobIndexStore.JobIndexDelete delete) {
        tx.add(buildPointerDelete(delete));
      }
    }
    for (var upsert : batch.readyMutation().upserts()) {
      ReadyQueueBackendSupport.ReadyQueueRow row =
          ReadyQueueBackendSupport.toReadyQueueRow(
              upsert.readyPointerKey(), upsert.canonicalPointerKey());
      if (row != null) {
        tx.add(buildReadyUpsert(row));
      }
    }
    for (String readyPointerKey : batch.readyMutation().deletes()) {
      ReadyQueueBackendSupport.ReadyQueueRow row =
          ReadyQueueBackendSupport.toReadyQueueRow(readyPointerKey, "");
      if (row != null) {
        tx.add(buildReadyDelete(row));
      }
    }
    try {
      dynamoDb.transactWriteItems(TransactWriteItemsRequest.builder().transactItems(tx).build());
      return true;
    } catch (TransactionCanceledException e) {
      return false;
    }
  }

  private TransactWriteItem buildReadyUpsert(ReadyQueueBackendSupport.ReadyQueueRow row) {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(KvAttributes.ATTR_PARTITION_KEY, AttributeValue.fromS(row.partitionKey()));
    item.put(KvAttributes.ATTR_SORT_KEY, AttributeValue.fromS(row.sortKey()));
    item.put(
        KvAttributes.ATTR_KIND,
        AttributeValue.fromS(DynamoReconcileReadyQueueBackend.KIND_READY_ENTRY));
    item.put(
        DynamoReconcileReadyQueueBackend.ATTR_READY_POINTER_KEY,
        AttributeValue.fromS(row.entry().readyPointerKey()));
    item.put(
        DynamoReconcileReadyQueueBackend.ATTR_CANONICAL_POINTER_KEY,
        AttributeValue.fromS(row.entry().canonicalPointerKey()));
    item.put(
        DynamoReconcileReadyQueueBackend.ATTR_ACCOUNT_ID,
        AttributeValue.fromS(row.entry().accountId()));
    item.put(
        DynamoReconcileReadyQueueBackend.ATTR_JOB_ID, AttributeValue.fromS(row.entry().jobId()));
    item.put(
        DynamoReconcileReadyQueueBackend.ATTR_DUE_AT_MS,
        AttributeValue.fromN(Long.toString(row.entry().dueAtMs())));
    item.put(
        DynamoReconcileReadyQueueBackend.ATTR_INDEX_TYPE,
        AttributeValue.fromS(row.entry().indexType().name()));
    item.put(
        DynamoReconcileReadyQueueBackend.ATTR_FILTER_VALUE,
        AttributeValue.fromS(row.entry().filterValue()));
    return TransactWriteItem.builder()
        .put(Put.builder().tableName(table).item(item).build())
        .build();
  }

  private TransactWriteItem buildReadyDelete(ReadyQueueBackendSupport.ReadyQueueRow row) {
    return TransactWriteItem.builder()
        .delete(
            Delete.builder()
                .tableName(table)
                .key(
                    Map.of(
                        KvAttributes.ATTR_PARTITION_KEY, AttributeValue.fromS(row.partitionKey()),
                        KvAttributes.ATTR_SORT_KEY, AttributeValue.fromS(row.sortKey())))
                .build())
        .build();
  }

  private Optional<JobIndexEntrySnapshot> loadCanonicalPointer(
      JobIndexBackendSupport.CanonicalJobKey key) {
    var response =
        dynamoDb.getItem(
            GetItemRequest.builder()
                .tableName(table)
                .consistentRead(true)
                .key(
                    Map.of(
                        ATTR_PARTITION_KEY,
                        AttributeValue.fromS(JobIndexBackendSupport.canonicalPartitionKey(key)),
                        ATTR_SORT_KEY,
                        AttributeValue.fromS(JobIndexBackendSupport.canonicalSortKey(key))))
                .build());
    if (!response.hasItem() || response.item().isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        new JobIndexEntrySnapshot(
            stringAttr(response.item(), JobIndexBackendSupport.ATTR_POINTER_KEY),
            stringAttr(response.item(), JobIndexBackendSupport.ATTR_BLOB_URI),
            longAttr(response.item(), ATTR_VERSION)));
  }

  private Optional<JobIndexEntrySnapshot> loadIndexPointer(
      String partitionKey, String sortKey, String referenceAttributeName) {
    var response =
        dynamoDb.getItem(
            GetItemRequest.builder()
                .tableName(table)
                .consistentRead(true)
                .key(
                    Map.of(
                        ATTR_PARTITION_KEY, AttributeValue.fromS(partitionKey),
                        ATTR_SORT_KEY, AttributeValue.fromS(sortKey)))
                .build());
    if (!response.hasItem() || response.item().isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        new JobIndexEntrySnapshot(
            stringAttr(response.item(), JobIndexBackendSupport.ATTR_POINTER_KEY),
            stringAttr(response.item(), referenceAttributeName),
            longAttr(response.item(), ATTR_VERSION)));
  }

  private JobIndexQueryPage listIndexPointers(
      String partitionKey, String pageToken, int limit, String referenceAttributeName) {
    QueryRequest.Builder query =
        QueryRequest.builder()
            .tableName(table)
            .consistentRead(true)
            .limit(Math.max(1, limit))
            .expressionAttributeNames(Map.of("#pk", ATTR_PARTITION_KEY))
            .keyConditionExpression("#pk = :pk")
            .expressionAttributeValues(Map.of(":pk", AttributeValue.fromS(partitionKey)));
    String resumeSortKey = sortKeyFromPageToken(pageToken);
    if (!resumeSortKey.isBlank()) {
      query.exclusiveStartKey(
          Map.of(
              ATTR_PARTITION_KEY, AttributeValue.fromS(partitionKey),
              ATTR_SORT_KEY, AttributeValue.fromS(resumeSortKey)));
    }
    var response = dynamoDb.query(query.build());
    List<JobIndexEntrySnapshot> pointers = new ArrayList<>(response.items().size());
    for (var item : response.items()) {
      pointers.add(
          new JobIndexEntrySnapshot(
              stringAttr(item, JobIndexBackendSupport.ATTR_POINTER_KEY),
              stringAttr(item, referenceAttributeName),
              longAttr(item, ATTR_VERSION)));
    }
    String nextPageToken = "";
    if (response.lastEvaluatedKey() != null
        && !response.lastEvaluatedKey().isEmpty()
        && !response.items().isEmpty()) {
      nextPageToken =
          stringAttr(
              response.items().get(response.items().size() - 1),
              JobIndexBackendSupport.ATTR_POINTER_KEY);
    }
    return new JobIndexQueryPage(List.copyOf(pointers), nextPageToken);
  }

  private String sortKeyFromPageToken(String pageToken) {
    if (pageToken == null || pageToken.isBlank()) {
      return "";
    }
    var canonicalKey = JobIndexBackendSupport.parseCanonicalJobKey(pageToken);
    if (canonicalKey != null) {
      return JobIndexBackendSupport.canonicalSortKey(canonicalKey);
    }
    var parentKey = JobIndexBackendSupport.parseParentKey(pageToken);
    if (parentKey != null) {
      return JobIndexBackendSupport.parentSortKey(parentKey);
    }
    var connectorKey = JobIndexBackendSupport.parseConnectorKey(pageToken);
    if (connectorKey != null) {
      return JobIndexBackendSupport.connectorSortKey(connectorKey);
    }
    var globalStateKey = JobIndexBackendSupport.parseGlobalStateKey(pageToken);
    if (globalStateKey != null) {
      return JobIndexBackendSupport.globalStateSortKey(globalStateKey);
    }
    var accountStateKey = JobIndexBackendSupport.parseAccountStateKey(pageToken);
    if (accountStateKey != null) {
      return JobIndexBackendSupport.accountStateSortKey(accountStateKey);
    }
    var connectorStateKey = JobIndexBackendSupport.parseConnectorStateKey(pageToken);
    if (connectorStateKey != null) {
      return JobIndexBackendSupport.connectorStateSortKey(connectorStateKey);
    }
    return "";
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }

  private TransactWriteItem buildPointerUpsert(ReconcileJobIndexStore.JobIndexUpsert upsert) {
    var canonicalKey = JobIndexBackendSupport.parseCanonicalJobKey(upsert.pointerKey());
    if (canonicalKey != null) {
      return buildCanonicalUpsert(canonicalKey, upsert);
    }
    var lookupKey = JobIndexBackendSupport.parseLookupKey(upsert.pointerKey());
    if (lookupKey != null) {
      return buildIndexUpsert(
          JobIndexBackendSupport.lookupPartitionKey(),
          JobIndexBackendSupport.lookupSortKey(lookupKey),
          JobIndexBackendSupport.KIND_LOOKUP,
          upsert,
          JobIndexBackendSupport.ATTR_BLOB_URI);
    }
    var parentKey = JobIndexBackendSupport.parseParentKey(upsert.pointerKey());
    if (parentKey != null) {
      return buildIndexUpsert(
          JobIndexBackendSupport.parentPartitionKey(parentKey),
          JobIndexBackendSupport.parentSortKey(parentKey),
          JobIndexBackendSupport.KIND_PARENT,
          upsert,
          JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY);
    }
    var connectorKey = JobIndexBackendSupport.parseConnectorKey(upsert.pointerKey());
    if (connectorKey != null) {
      return buildIndexUpsert(
          JobIndexBackendSupport.connectorPartitionKey(connectorKey),
          JobIndexBackendSupport.connectorSortKey(connectorKey),
          JobIndexBackendSupport.KIND_CONNECTOR,
          upsert,
          JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY);
    }
    var globalStateKey = JobIndexBackendSupport.parseGlobalStateKey(upsert.pointerKey());
    if (globalStateKey != null) {
      return buildIndexUpsert(
          JobIndexBackendSupport.globalStatePartitionKey(globalStateKey),
          JobIndexBackendSupport.globalStateSortKey(globalStateKey),
          JobIndexBackendSupport.KIND_GLOBAL_STATE,
          upsert,
          JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY);
    }
    var accountStateKey = JobIndexBackendSupport.parseAccountStateKey(upsert.pointerKey());
    if (accountStateKey != null) {
      return buildIndexUpsert(
          JobIndexBackendSupport.accountStatePartitionKey(accountStateKey),
          JobIndexBackendSupport.accountStateSortKey(accountStateKey),
          JobIndexBackendSupport.KIND_ACCOUNT_STATE,
          upsert,
          JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY);
    }
    var connectorStateKey = JobIndexBackendSupport.parseConnectorStateKey(upsert.pointerKey());
    if (connectorStateKey != null) {
      return buildIndexUpsert(
          JobIndexBackendSupport.connectorStatePartitionKey(connectorStateKey),
          JobIndexBackendSupport.connectorStateSortKey(connectorStateKey),
          JobIndexBackendSupport.KIND_CONNECTOR_STATE,
          upsert,
          JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY);
    }
    var dedupeKey = JobIndexBackendSupport.parseDedupeKey(upsert.pointerKey());
    if (dedupeKey != null) {
      return buildIndexUpsert(
          JobIndexBackendSupport.dedupePartitionKey(dedupeKey),
          JobIndexBackendSupport.dedupeSortKey(dedupeKey),
          JobIndexBackendSupport.KIND_DEDUPE,
          upsert,
          JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY);
    }
    throw new IllegalArgumentException(
        "Unsupported reconcile job index upsert key: " + upsert.pointerKey());
  }

  private TransactWriteItem buildPointerDelete(ReconcileJobIndexStore.JobIndexDelete delete) {
    var canonicalKey = JobIndexBackendSupport.parseCanonicalJobKey(delete.pointerKey());
    if (canonicalKey != null) {
      return buildDelete(
          JobIndexBackendSupport.canonicalPartitionKey(canonicalKey),
          JobIndexBackendSupport.canonicalSortKey(canonicalKey),
          delete.expectedVersion());
    }
    var lookupKey = JobIndexBackendSupport.parseLookupKey(delete.pointerKey());
    if (lookupKey != null) {
      return buildDelete(
          JobIndexBackendSupport.lookupPartitionKey(),
          JobIndexBackendSupport.lookupSortKey(lookupKey),
          delete.expectedVersion());
    }
    var parentKey = JobIndexBackendSupport.parseParentKey(delete.pointerKey());
    if (parentKey != null) {
      return buildDelete(
          JobIndexBackendSupport.parentPartitionKey(parentKey),
          JobIndexBackendSupport.parentSortKey(parentKey),
          delete.expectedVersion());
    }
    var connectorKey = JobIndexBackendSupport.parseConnectorKey(delete.pointerKey());
    if (connectorKey != null) {
      return buildDelete(
          JobIndexBackendSupport.connectorPartitionKey(connectorKey),
          JobIndexBackendSupport.connectorSortKey(connectorKey),
          delete.expectedVersion());
    }
    var globalStateKey = JobIndexBackendSupport.parseGlobalStateKey(delete.pointerKey());
    if (globalStateKey != null) {
      return buildDelete(
          JobIndexBackendSupport.globalStatePartitionKey(globalStateKey),
          JobIndexBackendSupport.globalStateSortKey(globalStateKey),
          delete.expectedVersion());
    }
    var accountStateKey = JobIndexBackendSupport.parseAccountStateKey(delete.pointerKey());
    if (accountStateKey != null) {
      return buildDelete(
          JobIndexBackendSupport.accountStatePartitionKey(accountStateKey),
          JobIndexBackendSupport.accountStateSortKey(accountStateKey),
          delete.expectedVersion());
    }
    var connectorStateKey = JobIndexBackendSupport.parseConnectorStateKey(delete.pointerKey());
    if (connectorStateKey != null) {
      return buildDelete(
          JobIndexBackendSupport.connectorStatePartitionKey(connectorStateKey),
          JobIndexBackendSupport.connectorStateSortKey(connectorStateKey),
          delete.expectedVersion());
    }
    var dedupeKey = JobIndexBackendSupport.parseDedupeKey(delete.pointerKey());
    if (dedupeKey != null) {
      return buildDelete(
          JobIndexBackendSupport.dedupePartitionKey(dedupeKey),
          JobIndexBackendSupport.dedupeSortKey(dedupeKey),
          delete.expectedVersion());
    }
    throw new IllegalArgumentException(
        "Unsupported reconcile job index delete key: " + delete.pointerKey());
  }

  private TransactWriteItem buildCanonicalUpsert(
      JobIndexBackendSupport.CanonicalJobKey key, ReconcileJobIndexStore.JobIndexUpsert upsert) {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(
        ATTR_PARTITION_KEY,
        AttributeValue.fromS(JobIndexBackendSupport.canonicalPartitionKey(key)));
    item.put(ATTR_SORT_KEY, AttributeValue.fromS(JobIndexBackendSupport.canonicalSortKey(key)));
    item.put(ATTR_KIND, AttributeValue.fromS(JobIndexBackendSupport.KIND_CANONICAL_JOB));
    item.put(ATTR_VERSION, AttributeValue.fromN(Long.toString(upsert.expectedVersion() + 1L)));
    item.put(JobIndexBackendSupport.ATTR_POINTER_KEY, AttributeValue.fromS(key.pointerKey()));
    item.put(JobIndexBackendSupport.ATTR_BLOB_URI, AttributeValue.fromS(upsert.blobUri()));
    item.put(JobIndexBackendSupport.ATTR_ACCOUNT_ID, AttributeValue.fromS(key.accountSegment()));
    item.put(JobIndexBackendSupport.ATTR_JOB_ID, AttributeValue.fromS(key.jobSegment()));
    return buildPut(item, upsert.expectedVersion());
  }

  private TransactWriteItem buildIndexUpsert(
      String partitionKey,
      String sortKey,
      String kind,
      ReconcileJobIndexStore.JobIndexUpsert upsert,
      String referenceAttributeName) {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(ATTR_PARTITION_KEY, AttributeValue.fromS(partitionKey));
    item.put(ATTR_SORT_KEY, AttributeValue.fromS(sortKey));
    item.put(ATTR_KIND, AttributeValue.fromS(kind));
    item.put(ATTR_VERSION, AttributeValue.fromN(Long.toString(upsert.expectedVersion() + 1L)));
    item.put(JobIndexBackendSupport.ATTR_POINTER_KEY, AttributeValue.fromS(upsert.pointerKey()));
    item.put(referenceAttributeName, AttributeValue.fromS(upsert.blobUri()));
    return buildPut(item, upsert.expectedVersion());
  }

  private TransactWriteItem buildPut(Map<String, AttributeValue> item, long expectedVersion) {
    Put.Builder put = Put.builder().tableName(table).item(item);
    if (expectedVersion == 0L) {
      put.conditionExpression("attribute_not_exists(#pk)")
          .expressionAttributeNames(Map.of("#pk", ATTR_PARTITION_KEY));
    } else {
      put.conditionExpression("#v = :expected")
          .expressionAttributeNames(Map.of("#v", ATTR_VERSION))
          .expressionAttributeValues(
              Map.of(":expected", AttributeValue.fromN(Long.toString(expectedVersion))));
    }
    return TransactWriteItem.builder().put(put.build()).build();
  }

  private TransactWriteItem buildDelete(String partitionKey, String sortKey, long expectedVersion) {
    return TransactWriteItem.builder()
        .delete(
            Delete.builder()
                .tableName(table)
                .key(
                    Map.of(
                        ATTR_PARTITION_KEY, AttributeValue.fromS(partitionKey),
                        ATTR_SORT_KEY, AttributeValue.fromS(sortKey)))
                .conditionExpression("#v = :expected")
                .expressionAttributeNames(Map.of("#v", ATTR_VERSION))
                .expressionAttributeValues(
                    Map.of(":expected", AttributeValue.fromN(Long.toString(expectedVersion))))
                .build())
        .build();
  }

  private JobIndexBackendSupport.CanonicalJobKey parseCanonicalPrefix(String prefix) {
    return JobIndexBackendSupport.parseCanonicalPrefix(prefix);
  }

  private JobIndexBackendSupport.ParentKey parseParentPrefix(String prefix) {
    return JobIndexBackendSupport.parseParentPrefix(prefix);
  }

  private JobIndexBackendSupport.ConnectorKey parseConnectorPrefix(String prefix) {
    return JobIndexBackendSupport.parseConnectorPrefix(prefix);
  }

  private JobIndexBackendSupport.GlobalStateKey parseGlobalStatePrefix(String prefix) {
    return JobIndexBackendSupport.parseGlobalStatePrefix(prefix);
  }

  private JobIndexBackendSupport.AccountStateKey parseAccountStatePrefix(String prefix) {
    return JobIndexBackendSupport.parseAccountStatePrefix(prefix);
  }

  private JobIndexBackendSupport.ConnectorStateKey parseConnectorStatePrefix(String prefix) {
    return JobIndexBackendSupport.parseConnectorStatePrefix(prefix);
  }

  private static String stringAttr(Map<String, AttributeValue> item, String name) {
    AttributeValue value = item.get(name);
    return value == null || value.s() == null ? "" : value.s();
  }

  private static long longAttr(Map<String, AttributeValue> item, String name) {
    AttributeValue value = item.get(name);
    if (value == null || value.n() == null) {
      return 0L;
    }
    try {
      return Long.parseLong(value.n());
    } catch (NumberFormatException ignored) {
      return 0L;
    }
  }
}
