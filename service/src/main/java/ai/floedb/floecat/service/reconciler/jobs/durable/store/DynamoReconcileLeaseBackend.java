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

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.storage.spi.PointerStore.CasDelete;
import ai.floedb.floecat.storage.spi.PointerStore.CasOp;
import ai.floedb.floecat.storage.spi.PointerStore.CasUpsert;
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
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

@Singleton
@IfBuildProperty(name = "floecat.kv", stringValue = "dynamodb")
public class DynamoReconcileLeaseBackend implements ReconcileLeaseBackend {
  @Inject DynamoDbClient dynamoDb;

  @ConfigProperty(name = "floecat.kv.table", defaultValue = "floecat_pointers")
  String table = "floecat_pointers";

  @Inject
  public DynamoReconcileLeaseBackend() {}

  public void bind(DynamoDbClient dynamoDb, String table) {
    this.dynamoDb = dynamoDb;
    this.table = table;
  }

  @Override
  public Optional<Pointer> loadPointer(String pointerKey) {
    LeaseBackendSupport.LeasePointerKey leaseKey =
        LeaseBackendSupport.parseLeasePointerKey(pointerKey);
    if (leaseKey != null) {
      return loadLeasePointer(leaseKey);
    }
    LeaseBackendSupport.LeaseExpiryPointerKey expiryKey =
        LeaseBackendSupport.parseLeaseExpiryPointerKey(pointerKey);
    if (expiryKey != null) {
      return loadLeaseExpiryPointer(expiryKey);
    }
    return DynamoPointerBackendSupport.loadPointer(dynamoDb, table, pointerKey);
  }

  @Override
  public boolean compareAndSetBatch(List<CasOp> ops) {
    if (ops == null || ops.isEmpty()) {
      return true;
    }
    List<TransactWriteItem> tx = new ArrayList<>(ops.size());
    for (CasOp op : ops) {
      if (op instanceof CasUpsert upsert) {
        LeaseBackendSupport.LeasePointerKey leaseKey =
            LeaseBackendSupport.parseLeasePointerKey(upsert.key());
        if (leaseKey != null) {
          tx.add(buildLeaseUpsert(leaseKey, upsert));
          continue;
        }
        LeaseBackendSupport.LeaseExpiryPointerKey expiryKey =
            LeaseBackendSupport.parseLeaseExpiryPointerKey(upsert.key());
        if (expiryKey != null) {
          tx.add(buildLeaseExpiryUpsert(expiryKey, upsert));
          continue;
        }
        ReadyQueueBackendSupport.ReadyQueueRow readyRow =
            ReadyQueueBackendSupport.toReadyQueueRow(upsert.key(), upsert.next().getBlobUri());
        if (readyRow != null) {
          tx.add(buildReadyUpsert(readyRow));
          continue;
        }
        if (appendJobIndexUpsert(tx, upsert)) {
          continue;
        }
        tx.add(DynamoPointerBackendSupport.buildPointerUpsert(table, upsert));
      } else if (op instanceof CasDelete delete) {
        LeaseBackendSupport.LeasePointerKey leaseKey =
            LeaseBackendSupport.parseLeasePointerKey(delete.key());
        if (leaseKey != null) {
          tx.add(buildLeaseDelete(leaseKey, delete));
          continue;
        }
        LeaseBackendSupport.LeaseExpiryPointerKey expiryKey =
            LeaseBackendSupport.parseLeaseExpiryPointerKey(delete.key());
        if (expiryKey != null) {
          tx.add(buildLeaseExpiryDelete(expiryKey, delete));
          continue;
        }
        ReadyQueueBackendSupport.ReadyQueueRow readyRow =
            ReadyQueueBackendSupport.toReadyQueueRow(delete.key(), "");
        if (readyRow != null) {
          tx.add(buildReadyDelete(readyRow));
          continue;
        }
        if (appendJobIndexDelete(tx, delete)) {
          continue;
        }
        tx.add(DynamoPointerBackendSupport.buildPointerDelete(table, delete));
      }
    }
    try {
      dynamoDb.transactWriteItems(TransactWriteItemsRequest.builder().transactItems(tx).build());
      return true;
    } catch (TransactionCanceledException e) {
      return false;
    }
  }

  @Override
  public List<Pointer> listPointersByPrefix(
      String prefix, int limit, String pageToken, StringBuilder nextPageToken) {
    if (LeaseBackendSupport.LEASE_EXPIRY_POINTER_PREFIX.equals(prefix)) {
      return listLeaseExpiryPointers(limit, pageToken, nextPageToken);
    }
    return DynamoPointerBackendSupport.listPointersByPrefix(
        dynamoDb, table, prefix, limit, pageToken, nextPageToken);
  }

  private Optional<Pointer> loadLeasePointer(LeaseBackendSupport.LeasePointerKey leaseKey) {
    var response =
        dynamoDb.getItem(
            GetItemRequest.builder()
                .tableName(table)
                .consistentRead(true)
                .key(
                    Map.of(
                        ATTR_PARTITION_KEY,
                        AttributeValue.fromS(LeaseBackendSupport.leasePartitionKey(leaseKey)),
                        ATTR_SORT_KEY,
                        AttributeValue.fromS(LeaseBackendSupport.leaseSortKey(leaseKey))))
                .build());
    if (!response.hasItem() || response.item().isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        Pointer.newBuilder()
            .setKey(stringAttr(response.item(), LeaseBackendSupport.ATTR_POINTER_KEY))
            .setBlobUri(stringAttr(response.item(), DynamoPointerBackendSupport.ATTR_BLOB_URI))
            .setVersion(longAttr(response.item(), ATTR_VERSION))
            .build());
  }

  private Optional<Pointer> loadLeaseExpiryPointer(
      LeaseBackendSupport.LeaseExpiryPointerKey expiryKey) {
    var response =
        dynamoDb.getItem(
            GetItemRequest.builder()
                .tableName(table)
                .consistentRead(true)
                .key(
                    Map.of(
                        ATTR_PARTITION_KEY,
                        AttributeValue.fromS(LeaseBackendSupport.LEASE_EXPIRY_PARTITION_KEY),
                        ATTR_SORT_KEY,
                        AttributeValue.fromS(LeaseBackendSupport.leaseExpirySortKey(expiryKey))))
                .build());
    if (!response.hasItem() || response.item().isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        Pointer.newBuilder()
            .setKey(stringAttr(response.item(), LeaseBackendSupport.ATTR_POINTER_KEY))
            .setBlobUri(stringAttr(response.item(), LeaseBackendSupport.ATTR_CANONICAL_POINTER_KEY))
            .setVersion(longAttr(response.item(), ATTR_VERSION))
            .build());
  }

  private List<Pointer> listLeaseExpiryPointers(
      int limit, String pageToken, StringBuilder nextPageToken) {
    QueryRequest.Builder query =
        QueryRequest.builder()
            .tableName(table)
            .consistentRead(true)
            .limit(Math.max(1, limit))
            .expressionAttributeNames(Map.of("#pk", ATTR_PARTITION_KEY))
            .keyConditionExpression("#pk = :pk")
            .expressionAttributeValues(
                Map.of(
                    ":pk", AttributeValue.fromS(LeaseBackendSupport.LEASE_EXPIRY_PARTITION_KEY)));

    String token = LeaseBackendSupport.decodeLeaseExpiryPageToken(pageToken);
    if (!token.isBlank()) {
      query.exclusiveStartKey(
          Map.of(
              ATTR_PARTITION_KEY,
              AttributeValue.fromS(LeaseBackendSupport.LEASE_EXPIRY_PARTITION_KEY),
              ATTR_SORT_KEY,
              AttributeValue.fromS(token)));
    }

    var response = dynamoDb.query(query.build());
    List<Pointer> pointers = new ArrayList<>(response.items().size());
    for (var item : response.items()) {
      pointers.add(
          Pointer.newBuilder()
              .setKey(stringAttr(item, LeaseBackendSupport.ATTR_POINTER_KEY))
              .setBlobUri(stringAttr(item, LeaseBackendSupport.ATTR_CANONICAL_POINTER_KEY))
              .setVersion(longAttr(item, ATTR_VERSION))
              .build());
    }
    nextPageToken.setLength(0);
    if (response.lastEvaluatedKey() != null && !response.lastEvaluatedKey().isEmpty()) {
      nextPageToken.append(
          LeaseBackendSupport.encodeLeaseExpiryPageToken(
              response.lastEvaluatedKey().get(ATTR_SORT_KEY).s()));
    }
    return pointers;
  }

  private TransactWriteItem buildLeaseUpsert(
      LeaseBackendSupport.LeasePointerKey leaseKey, CasUpsert upsert) {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(
        ATTR_PARTITION_KEY, AttributeValue.fromS(LeaseBackendSupport.leasePartitionKey(leaseKey)));
    item.put(ATTR_SORT_KEY, AttributeValue.fromS(LeaseBackendSupport.leaseSortKey(leaseKey)));
    item.put(ATTR_KIND, AttributeValue.fromS(LeaseBackendSupport.KIND_LEASE_ENTRY));
    item.put(ATTR_VERSION, AttributeValue.fromN(Long.toString(upsert.expectedVersion() + 1L)));
    item.put(LeaseBackendSupport.ATTR_POINTER_KEY, AttributeValue.fromS(leaseKey.pointerKey()));
    item.put(
        DynamoPointerBackendSupport.ATTR_BLOB_URI,
        AttributeValue.fromS(upsert.next().getBlobUri()));
    Put.Builder put = Put.builder().tableName(table).item(item);
    if (upsert.expectedVersion() == 0L) {
      put.conditionExpression("attribute_not_exists(#pk)")
          .expressionAttributeNames(Map.of("#pk", ATTR_PARTITION_KEY));
    } else {
      put.conditionExpression("#v = :expected")
          .expressionAttributeNames(Map.of("#v", ATTR_VERSION))
          .expressionAttributeValues(
              Map.of(":expected", AttributeValue.fromN(Long.toString(upsert.expectedVersion()))));
    }
    return TransactWriteItem.builder().put(put.build()).build();
  }

  private TransactWriteItem buildLeaseDelete(
      LeaseBackendSupport.LeasePointerKey leaseKey, CasDelete delete) {
    return TransactWriteItem.builder()
        .delete(
            Delete.builder()
                .tableName(table)
                .key(
                    Map.of(
                        ATTR_PARTITION_KEY,
                        AttributeValue.fromS(LeaseBackendSupport.leasePartitionKey(leaseKey)),
                        ATTR_SORT_KEY,
                        AttributeValue.fromS(LeaseBackendSupport.leaseSortKey(leaseKey))))
                .conditionExpression("#v = :expected")
                .expressionAttributeNames(Map.of("#v", ATTR_VERSION))
                .expressionAttributeValues(
                    Map.of(
                        ":expected", AttributeValue.fromN(Long.toString(delete.expectedVersion()))))
                .build())
        .build();
  }

  private TransactWriteItem buildLeaseExpiryUpsert(
      LeaseBackendSupport.LeaseExpiryPointerKey expiryKey, CasUpsert upsert) {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(
        ATTR_PARTITION_KEY, AttributeValue.fromS(LeaseBackendSupport.LEASE_EXPIRY_PARTITION_KEY));
    item.put(
        ATTR_SORT_KEY, AttributeValue.fromS(LeaseBackendSupport.leaseExpirySortKey(expiryKey)));
    item.put(ATTR_KIND, AttributeValue.fromS(LeaseBackendSupport.KIND_LEASE_EXPIRY_ENTRY));
    item.put(ATTR_VERSION, AttributeValue.fromN(Long.toString(upsert.expectedVersion() + 1L)));
    item.put(LeaseBackendSupport.ATTR_POINTER_KEY, AttributeValue.fromS(expiryKey.pointerKey()));
    item.put(
        LeaseBackendSupport.ATTR_CANONICAL_POINTER_KEY,
        AttributeValue.fromS(upsert.next().getBlobUri()));
    Put.Builder put = Put.builder().tableName(table).item(item);
    if (upsert.expectedVersion() == 0L) {
      put.conditionExpression("attribute_not_exists(#pk)")
          .expressionAttributeNames(Map.of("#pk", ATTR_PARTITION_KEY));
    } else {
      put.conditionExpression("#v = :expected")
          .expressionAttributeNames(Map.of("#v", ATTR_VERSION))
          .expressionAttributeValues(
              Map.of(":expected", AttributeValue.fromN(Long.toString(upsert.expectedVersion()))));
    }
    return TransactWriteItem.builder().put(put.build()).build();
  }

  private TransactWriteItem buildLeaseExpiryDelete(
      LeaseBackendSupport.LeaseExpiryPointerKey expiryKey, CasDelete delete) {
    return TransactWriteItem.builder()
        .delete(
            Delete.builder()
                .tableName(table)
                .key(
                    Map.of(
                        ATTR_PARTITION_KEY,
                        AttributeValue.fromS(LeaseBackendSupport.LEASE_EXPIRY_PARTITION_KEY),
                        ATTR_SORT_KEY,
                        AttributeValue.fromS(LeaseBackendSupport.leaseExpirySortKey(expiryKey))))
                .conditionExpression("#v = :expected")
                .expressionAttributeNames(Map.of("#v", ATTR_VERSION))
                .expressionAttributeValues(
                    Map.of(
                        ":expected", AttributeValue.fromN(Long.toString(delete.expectedVersion()))))
                .build())
        .build();
  }

  private boolean appendJobIndexUpsert(List<TransactWriteItem> tx, CasUpsert upsert) {
    var canonicalKey = JobIndexBackendSupport.parseCanonicalJobKey(upsert.key());
    if (canonicalKey != null) {
      tx.add(buildCanonicalUpsert(canonicalKey, upsert));
      return true;
    }
    var lookupKey = JobIndexBackendSupport.parseLookupKey(upsert.key());
    if (lookupKey != null) {
      tx.add(
          buildJobIndexReferenceUpsert(
              JobIndexBackendSupport.lookupPartitionKey(),
              JobIndexBackendSupport.lookupSortKey(lookupKey),
              JobIndexBackendSupport.KIND_LOOKUP,
              upsert,
              JobIndexBackendSupport.ATTR_BLOB_URI));
      return true;
    }
    var parentKey = JobIndexBackendSupport.parseParentKey(upsert.key());
    if (parentKey != null) {
      tx.add(
          buildJobIndexReferenceUpsert(
              JobIndexBackendSupport.parentPartitionKey(parentKey),
              JobIndexBackendSupport.parentSortKey(parentKey),
              JobIndexBackendSupport.KIND_PARENT,
              upsert,
              JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY));
      return true;
    }
    var connectorKey = JobIndexBackendSupport.parseConnectorKey(upsert.key());
    if (connectorKey != null) {
      tx.add(
          buildJobIndexReferenceUpsert(
              JobIndexBackendSupport.connectorPartitionKey(connectorKey),
              JobIndexBackendSupport.connectorSortKey(connectorKey),
              JobIndexBackendSupport.KIND_CONNECTOR,
              upsert,
              JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY));
      return true;
    }
    var globalStateKey = JobIndexBackendSupport.parseGlobalStateKey(upsert.key());
    if (globalStateKey != null) {
      tx.add(
          buildJobIndexReferenceUpsert(
              JobIndexBackendSupport.globalStatePartitionKey(globalStateKey),
              JobIndexBackendSupport.globalStateSortKey(globalStateKey),
              JobIndexBackendSupport.KIND_GLOBAL_STATE,
              upsert,
              JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY));
      return true;
    }
    var accountStateKey = JobIndexBackendSupport.parseAccountStateKey(upsert.key());
    if (accountStateKey != null) {
      tx.add(
          buildJobIndexReferenceUpsert(
              JobIndexBackendSupport.accountStatePartitionKey(accountStateKey),
              JobIndexBackendSupport.accountStateSortKey(accountStateKey),
              JobIndexBackendSupport.KIND_ACCOUNT_STATE,
              upsert,
              JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY));
      return true;
    }
    var connectorStateKey = JobIndexBackendSupport.parseConnectorStateKey(upsert.key());
    if (connectorStateKey != null) {
      tx.add(
          buildJobIndexReferenceUpsert(
              JobIndexBackendSupport.connectorStatePartitionKey(connectorStateKey),
              JobIndexBackendSupport.connectorStateSortKey(connectorStateKey),
              JobIndexBackendSupport.KIND_CONNECTOR_STATE,
              upsert,
              JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY));
      return true;
    }
    var dedupeKey = JobIndexBackendSupport.parseDedupeKey(upsert.key());
    if (dedupeKey != null) {
      tx.add(
          buildJobIndexReferenceUpsert(
              JobIndexBackendSupport.dedupePartitionKey(dedupeKey),
              JobIndexBackendSupport.dedupeSortKey(dedupeKey),
              JobIndexBackendSupport.KIND_DEDUPE,
              upsert,
              JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY));
      return true;
    }
    return false;
  }

  private boolean appendJobIndexDelete(List<TransactWriteItem> tx, CasDelete delete) {
    var canonicalKey = JobIndexBackendSupport.parseCanonicalJobKey(delete.key());
    if (canonicalKey != null) {
      tx.add(
          buildDelete(
              JobIndexBackendSupport.canonicalPartitionKey(canonicalKey),
              JobIndexBackendSupport.canonicalSortKey(canonicalKey),
              delete.expectedVersion()));
      return true;
    }
    var lookupKey = JobIndexBackendSupport.parseLookupKey(delete.key());
    if (lookupKey != null) {
      tx.add(
          buildDelete(
              JobIndexBackendSupport.lookupPartitionKey(),
              JobIndexBackendSupport.lookupSortKey(lookupKey),
              delete.expectedVersion()));
      return true;
    }
    var parentKey = JobIndexBackendSupport.parseParentKey(delete.key());
    if (parentKey != null) {
      tx.add(
          buildDelete(
              JobIndexBackendSupport.parentPartitionKey(parentKey),
              JobIndexBackendSupport.parentSortKey(parentKey),
              delete.expectedVersion()));
      return true;
    }
    var connectorKey = JobIndexBackendSupport.parseConnectorKey(delete.key());
    if (connectorKey != null) {
      tx.add(
          buildDelete(
              JobIndexBackendSupport.connectorPartitionKey(connectorKey),
              JobIndexBackendSupport.connectorSortKey(connectorKey),
              delete.expectedVersion()));
      return true;
    }
    var globalStateKey = JobIndexBackendSupport.parseGlobalStateKey(delete.key());
    if (globalStateKey != null) {
      tx.add(
          buildDelete(
              JobIndexBackendSupport.globalStatePartitionKey(globalStateKey),
              JobIndexBackendSupport.globalStateSortKey(globalStateKey),
              delete.expectedVersion()));
      return true;
    }
    var accountStateKey = JobIndexBackendSupport.parseAccountStateKey(delete.key());
    if (accountStateKey != null) {
      tx.add(
          buildDelete(
              JobIndexBackendSupport.accountStatePartitionKey(accountStateKey),
              JobIndexBackendSupport.accountStateSortKey(accountStateKey),
              delete.expectedVersion()));
      return true;
    }
    var connectorStateKey = JobIndexBackendSupport.parseConnectorStateKey(delete.key());
    if (connectorStateKey != null) {
      tx.add(
          buildDelete(
              JobIndexBackendSupport.connectorStatePartitionKey(connectorStateKey),
              JobIndexBackendSupport.connectorStateSortKey(connectorStateKey),
              delete.expectedVersion()));
      return true;
    }
    var dedupeKey = JobIndexBackendSupport.parseDedupeKey(delete.key());
    if (dedupeKey != null) {
      tx.add(
          buildDelete(
              JobIndexBackendSupport.dedupePartitionKey(dedupeKey),
              JobIndexBackendSupport.dedupeSortKey(dedupeKey),
              delete.expectedVersion()));
      return true;
    }
    return false;
  }

  private TransactWriteItem buildReadyUpsert(ReadyQueueBackendSupport.ReadyQueueRow row) {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(ATTR_PARTITION_KEY, AttributeValue.fromS(row.partitionKey()));
    item.put(ATTR_SORT_KEY, AttributeValue.fromS(row.sortKey()));
    item.put(ATTR_KIND, AttributeValue.fromS(DynamoReconcileReadyQueueBackend.KIND_READY_ENTRY));
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
                        ATTR_PARTITION_KEY, AttributeValue.fromS(row.partitionKey()),
                        ATTR_SORT_KEY, AttributeValue.fromS(row.sortKey())))
                .build())
        .build();
  }

  private TransactWriteItem buildCanonicalUpsert(
      JobIndexBackendSupport.CanonicalJobKey key, CasUpsert upsert) {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(
        ATTR_PARTITION_KEY,
        AttributeValue.fromS(JobIndexBackendSupport.canonicalPartitionKey(key)));
    item.put(ATTR_SORT_KEY, AttributeValue.fromS(JobIndexBackendSupport.canonicalSortKey(key)));
    item.put(ATTR_KIND, AttributeValue.fromS(JobIndexBackendSupport.KIND_CANONICAL_JOB));
    item.put(ATTR_VERSION, AttributeValue.fromN(Long.toString(upsert.expectedVersion() + 1L)));
    item.put(JobIndexBackendSupport.ATTR_POINTER_KEY, AttributeValue.fromS(key.pointerKey()));
    item.put(
        JobIndexBackendSupport.ATTR_BLOB_URI, AttributeValue.fromS(upsert.next().getBlobUri()));
    item.put(JobIndexBackendSupport.ATTR_ACCOUNT_ID, AttributeValue.fromS(key.accountSegment()));
    item.put(JobIndexBackendSupport.ATTR_JOB_ID, AttributeValue.fromS(key.jobSegment()));
    return buildPut(item, upsert.expectedVersion());
  }

  private TransactWriteItem buildJobIndexReferenceUpsert(
      String partitionKey,
      String sortKey,
      String kind,
      CasUpsert upsert,
      String referenceAttributeName) {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(ATTR_PARTITION_KEY, AttributeValue.fromS(partitionKey));
    item.put(ATTR_SORT_KEY, AttributeValue.fromS(sortKey));
    item.put(ATTR_KIND, AttributeValue.fromS(kind));
    item.put(ATTR_VERSION, AttributeValue.fromN(Long.toString(upsert.expectedVersion() + 1L)));
    item.put(JobIndexBackendSupport.ATTR_POINTER_KEY, AttributeValue.fromS(upsert.key()));
    item.put(referenceAttributeName, AttributeValue.fromS(upsert.next().getBlobUri()));
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
