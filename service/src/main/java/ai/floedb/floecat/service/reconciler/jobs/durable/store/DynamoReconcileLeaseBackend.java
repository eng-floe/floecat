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

import ai.floedb.floecat.storage.aws.DynamoDbClientManager;
import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionCheck;
import software.amazon.awssdk.services.dynamodb.model.Delete;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.Update;

@Singleton
@IfBuildProperty(name = "floecat.kv", stringValue = "dynamodb")
public class DynamoReconcileLeaseBackend implements ReconcileLeaseBackend {
  private static final String ATTR_BLOB_URI = "blob_uri";

  @Inject Instance<DynamoDbClientManager> dynamoDbClientManager;
  private final RefreshingDynamoCaller dynamoCaller = new RefreshingDynamoCaller();

  @ConfigProperty(name = "floecat.kv.table", defaultValue = "floecat_pointers")
  String table = "floecat_pointers";

  public DynamoReconcileLeaseBackend() {}

  public void bind(Supplier<DynamoDbClient> dynamoDbSupplier, String table) {
    dynamoCaller.bind(dynamoDbSupplier);
    this.table = table;
  }

  public void bind(DynamoDbClientManager manager, String table) {
    dynamoCaller.bind(manager);
    this.table = table;
  }

  @Override
  public Optional<LeaseRecordSnapshot> loadLease(String accountId, String jobId) {
    return loadLeaseSnapshot(
        new LeaseBackendSupport.LeasePointerKey(
            "", accountId == null ? "" : accountId, jobId == null ? "" : jobId));
  }

  @Override
  public Optional<LeaseExpirySnapshot> loadLeaseExpiry(String leaseExpiryKey) {
    LeaseBackendSupport.LeaseExpiryPointerKey expiryKey =
        LeaseBackendSupport.parseLeaseExpiryPointerKey(leaseExpiryKey);
    return expiryKey == null ? Optional.empty() : loadLeaseExpirySnapshot(expiryKey);
  }

  @Override
  public Optional<LeaseOwnerSnapshot> loadOwner(String ownerKey) {
    var response =
        dynamoCaller.call(
            dynamoDbClientManager,
            client ->
                client.getItem(
                    GetItemRequest.builder()
                        .tableName(table)
                        .consistentRead(true)
                        .key(
                            Map.of(
                                ATTR_PARTITION_KEY,
                                AttributeValue.fromS(
                                    LeaseBackendSupport.ownerPartitionKey(ownerKey)),
                                ATTR_SORT_KEY,
                                AttributeValue.fromS(LeaseBackendSupport.LEASE_OWNER_SORT_KEY)))
                        .build()));
    if (!response.hasItem() || response.item().isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        new LeaseOwnerSnapshot(
            stringAttr(response.item(), LeaseBackendSupport.ATTR_POINTER_KEY),
            stringAttr(response.item(), LeaseBackendSupport.ATTR_CANONICAL_POINTER_KEY),
            longAttr(response.item(), ATTR_VERSION)));
  }

  @Override
  public ReconcileLeaseStore.LeaseExpiryScanPage scanExpiredLeaseEntries(
      int limit, String pageToken) {
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

    var response = dynamoCaller.call(dynamoDbClientManager, client -> client.query(query.build()));
    List<ReconcileLeaseStore.LeaseExpiryEntry> entries = new ArrayList<>(response.items().size());
    for (var item : response.items()) {
      entries.add(
          new ReconcileLeaseStore.LeaseExpiryEntry(
              stringAttr(item, LeaseBackendSupport.ATTR_POINTER_KEY),
              stringAttr(item, LeaseBackendSupport.ATTR_CANONICAL_POINTER_KEY)));
    }
    String nextToken = "";
    if (response.lastEvaluatedKey() != null && !response.lastEvaluatedKey().isEmpty()) {
      nextToken =
          LeaseBackendSupport.encodeLeaseExpiryPageToken(
              response.lastEvaluatedKey().get(ATTR_SORT_KEY).s());
    }
    return new ReconcileLeaseStore.LeaseExpiryScanPage(List.copyOf(entries), nextToken);
  }

  @Override
  public boolean compareAndSetBatch(
      ReconcileJobIndexStore.JobIndexWriteBatch jobIndexBatch, LeaseWriteBatch leaseBatch) {
    if ((jobIndexBatch == null
            || (jobIndexBatch.writes().isEmpty() && jobIndexBatch.readyMutation().isEmpty()))
        && (leaseBatch == null || leaseBatch.writes().isEmpty())) {
      return true;
    }
    List<TransactWriteItem> tx = new ArrayList<>();
    if (jobIndexBatch != null) {
      for (ReconcileJobIndexStore.JobIndexWriteOp write : jobIndexBatch.writes()) {
        if (write instanceof ReconcileJobIndexStore.JobIndexUpsert upsert) {
          appendJobIndexUpsert(tx, upsert);
        } else if (write instanceof ReconcileJobIndexStore.JobIndexDelete delete) {
          appendJobIndexDelete(tx, delete);
        } else if (write instanceof ReconcileJobIndexStore.JobIndexCheck check) {
          appendJobIndexCheck(tx, check);
        } else if (write instanceof ReconcileJobIndexStore.JobIndexCheckAbsent check) {
          appendJobIndexCheckAbsent(tx, check.pointerKey());
        }
      }
      for (ReconcileJobIndexStore.ReadyQueueWrite readyUpsert :
          jobIndexBatch.readyMutation().upserts()) {
        tx.add(
            buildReadyUpsert(
                ReadyQueueBackendSupport.toReadyQueueRow(
                    readyUpsert.readyPointerKey(), readyUpsert.canonicalPointerKey())));
      }
      for (String readyDeleteKey : jobIndexBatch.readyMutation().deletes()) {
        // Resolve the delete key from the ready pointer alone (the canonical it referenced is being
        // rewritten in this same lease transaction). A blank canonical made the row resolve to
        // null,
        // dropping the delete item so the old ready row leaked.
        ReadyQueueBackendSupport.ReadyQueueRow row =
            ReadyQueueBackendSupport.toReadyQueueRow(readyDeleteKey);
        if (row != null) {
          tx.add(buildReadyDelete(row));
        }
      }
    }
    if (leaseBatch != null) {
      Set<String> mutatedLeaseRecords = new HashSet<>();
      Map<String, Long> conditionedLeaseRecords = new HashMap<>();
      for (LeaseWriteOp write : leaseBatch.writes()) {
        if (write instanceof LeaseRecordUpsert upsert) {
          mutatedLeaseRecords.add(leaseRecordKey(upsert.accountId(), upsert.jobId()));
        } else if (write instanceof LeaseRecordDelete delete) {
          mutatedLeaseRecords.add(leaseRecordKey(delete.accountId(), delete.jobId()));
        }
      }
      for (LeaseWriteOp write : leaseBatch.writes()) {
        if (write instanceof LeaseRecordCondition condition) {
          String leaseRecordKey = leaseRecordKey(condition.accountId(), condition.jobId());
          if (mutatedLeaseRecords.contains(leaseRecordKey)) {
            continue;
          }
          Long priorExpectedVersion =
              conditionedLeaseRecords.putIfAbsent(leaseRecordKey, condition.expectedVersion());
          if (priorExpectedVersion != null) {
            if (priorExpectedVersion.longValue() != condition.expectedVersion()) {
              return false;
            }
            continue;
          }
          tx.add(buildLeaseCondition(condition));
        } else if (write instanceof LeaseRecordUpsert upsert) {
          tx.add(buildLeaseUpsert(upsert));
        } else if (write instanceof LeaseRecordDelete delete) {
          tx.add(buildLeaseDelete(delete));
        } else if (write instanceof LeaseExpiryUpsert upsert) {
          tx.add(buildLeaseExpiryUpsert(upsert));
        } else if (write instanceof LeaseExpiryDelete delete) {
          tx.add(buildLeaseExpiryDelete(delete));
        } else if (write instanceof LeaseOwnerUpsert upsert) {
          tx.add(buildLeaseOwnerUpsert(upsert));
        } else if (write instanceof LeaseOwnerDelete delete) {
          tx.add(buildLeaseOwnerDelete(delete));
        }
      }
    }
    if (tx.size() > ReconcileJobWriteLimits.MAX_TRANSACTION_ITEMS) {
      throw new IllegalArgumentException(
          "DynamoDB transaction exceeds "
              + ReconcileJobWriteLimits.MAX_TRANSACTION_ITEMS
              + " items: "
              + tx.size());
    }
    try {
      dynamoCaller.callVoid(
          dynamoDbClientManager,
          client ->
              client.transactWriteItems(
                  TransactWriteItemsRequest.builder().transactItems(tx).build()));
      return true;
    } catch (TransactionCanceledException e) {
      return false;
    }
  }

  private String leaseRecordKey(String accountId, String jobId) {
    return LeaseBackendSupport.leasePointerKey(accountId, jobId);
  }

  private Optional<LeaseRecordSnapshot> loadLeaseSnapshot(
      LeaseBackendSupport.LeasePointerKey leaseKey) {
    var response =
        dynamoCaller.call(
            dynamoDbClientManager,
            client ->
                client.getItem(
                    GetItemRequest.builder()
                        .tableName(table)
                        .consistentRead(true)
                        .key(
                            Map.of(
                                ATTR_PARTITION_KEY,
                                AttributeValue.fromS(
                                    LeaseBackendSupport.leasePartitionKey(leaseKey)),
                                ATTR_SORT_KEY,
                                AttributeValue.fromS(LeaseBackendSupport.leaseSortKey(leaseKey))))
                        .build()));
    if (!response.hasItem() || response.item().isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        new LeaseRecordSnapshot(
            stringAttr(response.item(), ATTR_BLOB_URI), longAttr(response.item(), ATTR_VERSION)));
  }

  private Optional<LeaseExpirySnapshot> loadLeaseExpirySnapshot(
      LeaseBackendSupport.LeaseExpiryPointerKey expiryKey) {
    var response =
        dynamoCaller.call(
            dynamoDbClientManager,
            client ->
                client.getItem(
                    GetItemRequest.builder()
                        .tableName(table)
                        .consistentRead(true)
                        .key(
                            Map.of(
                                ATTR_PARTITION_KEY,
                                AttributeValue.fromS(
                                    LeaseBackendSupport.LEASE_EXPIRY_PARTITION_KEY),
                                ATTR_SORT_KEY,
                                AttributeValue.fromS(
                                    LeaseBackendSupport.leaseExpirySortKey(expiryKey))))
                        .build()));
    if (!response.hasItem() || response.item().isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        new LeaseExpirySnapshot(
            stringAttr(response.item(), LeaseBackendSupport.ATTR_POINTER_KEY),
            stringAttr(response.item(), LeaseBackendSupport.ATTR_CANONICAL_POINTER_KEY),
            longAttr(response.item(), ATTR_VERSION)));
  }

  private TransactWriteItem buildLeaseUpsert(LeaseRecordUpsert upsert) {
    LeaseBackendSupport.LeasePointerKey leaseKey =
        new LeaseBackendSupport.LeasePointerKey("", upsert.accountId(), upsert.jobId());
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(
        ATTR_PARTITION_KEY, AttributeValue.fromS(LeaseBackendSupport.leasePartitionKey(leaseKey)));
    item.put(ATTR_SORT_KEY, AttributeValue.fromS(LeaseBackendSupport.leaseSortKey(leaseKey)));
    item.put(ATTR_KIND, AttributeValue.fromS(LeaseBackendSupport.KIND_LEASE_ENTRY));
    item.put(ATTR_VERSION, AttributeValue.fromN(Long.toString(upsert.expectedVersion() + 1L)));
    item.put(LeaseBackendSupport.ATTR_POINTER_KEY, AttributeValue.fromS(leaseKey.pointerKey()));
    item.put(ATTR_BLOB_URI, AttributeValue.fromS(upsert.encodedLease()));
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

  private TransactWriteItem buildLeaseCondition(LeaseRecordCondition condition) {
    LeaseBackendSupport.LeasePointerKey leaseKey =
        new LeaseBackendSupport.LeasePointerKey("", condition.accountId(), condition.jobId());
    ConditionCheck.Builder check =
        ConditionCheck.builder()
            .tableName(table)
            .key(
                Map.of(
                    ATTR_PARTITION_KEY,
                    AttributeValue.fromS(LeaseBackendSupport.leasePartitionKey(leaseKey)),
                    ATTR_SORT_KEY,
                    AttributeValue.fromS(LeaseBackendSupport.leaseSortKey(leaseKey))));
    if (condition.expectedVersion() == 0L) {
      check
          .conditionExpression("attribute_not_exists(#pk)")
          .expressionAttributeNames(Map.of("#pk", ATTR_PARTITION_KEY));
    } else {
      check
          .conditionExpression("#v = :expected")
          .expressionAttributeNames(Map.of("#v", ATTR_VERSION))
          .expressionAttributeValues(
              Map.of(
                  ":expected", AttributeValue.fromN(Long.toString(condition.expectedVersion()))));
    }
    return TransactWriteItem.builder().conditionCheck(check.build()).build();
  }

  private TransactWriteItem buildLeaseDelete(LeaseRecordDelete delete) {
    LeaseBackendSupport.LeasePointerKey leaseKey =
        new LeaseBackendSupport.LeasePointerKey("", delete.accountId(), delete.jobId());
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

  private TransactWriteItem buildLeaseExpiryUpsert(LeaseExpiryUpsert upsert) {
    LeaseBackendSupport.LeaseExpiryPointerKey expiryKey =
        LeaseBackendSupport.parseLeaseExpiryPointerKey(upsert.leaseExpiryKey());
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
        AttributeValue.fromS(upsert.canonicalPointerKey()));
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

  private TransactWriteItem buildLeaseExpiryDelete(LeaseExpiryDelete delete) {
    LeaseBackendSupport.LeaseExpiryPointerKey expiryKey =
        LeaseBackendSupport.parseLeaseExpiryPointerKey(delete.leaseExpiryKey());
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

  private TransactWriteItem buildLeaseOwnerUpsert(LeaseOwnerUpsert upsert) {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(
        ATTR_PARTITION_KEY,
        AttributeValue.fromS(LeaseBackendSupport.ownerPartitionKey(upsert.ownerKey())));
    item.put(ATTR_SORT_KEY, AttributeValue.fromS(LeaseBackendSupport.LEASE_OWNER_SORT_KEY));
    item.put(ATTR_KIND, AttributeValue.fromS(LeaseBackendSupport.KIND_LEASE_OWNER_ENTRY));
    item.put(ATTR_VERSION, AttributeValue.fromN(Long.toString(upsert.expectedVersion() + 1L)));
    item.put(LeaseBackendSupport.ATTR_POINTER_KEY, AttributeValue.fromS(upsert.ownerKey()));
    item.put(
        LeaseBackendSupport.ATTR_CANONICAL_POINTER_KEY,
        AttributeValue.fromS(upsert.canonicalPointerKey()));
    return buildPut(item, upsert.expectedVersion());
  }

  private TransactWriteItem buildLeaseOwnerDelete(LeaseOwnerDelete delete) {
    return buildDelete(
        LeaseBackendSupport.ownerPartitionKey(delete.ownerKey()),
        LeaseBackendSupport.LEASE_OWNER_SORT_KEY,
        delete.expectedVersion());
  }

  private void appendJobIndexUpsert(
      List<TransactWriteItem> tx, ReconcileJobIndexStore.JobIndexUpsert upsert) {
    var lookupKey = JobIndexBackendSupport.parseLookupKey(upsert.pointerKey());
    if (lookupKey != null) {
      var storageKey = JobIndexBackendSupport.currentLookupStorageKey(lookupKey);
      tx.add(
          buildJobIndexReferenceUpsert(
              storageKey.partitionKey(),
              storageKey.sortKey(),
              JobIndexBackendSupport.KIND_LOOKUP,
              upsert,
              JobIndexBackendSupport.ATTR_BLOB_URI));
      tx.add(DynamoReconcileJobLookupCompatibility.legacyCheckAbsent(table, lookupKey));
      return;
    }
    var canonicalKey = JobIndexBackendSupport.parseCanonicalJobKey(upsert.pointerKey());
    if (canonicalKey != null) {
      tx.add(buildCanonicalUpsert(canonicalKey, upsert));
      return;
    }
    var parentKey = JobIndexBackendSupport.parseParentKey(upsert.pointerKey());
    if (parentKey != null) {
      tx.add(
          buildJobIndexReferenceUpsert(
              JobIndexBackendSupport.parentPartitionKey(parentKey),
              JobIndexBackendSupport.parentSortKey(parentKey),
              JobIndexBackendSupport.KIND_PARENT,
              upsert,
              JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY));
      return;
    }
    var connectorKey = JobIndexBackendSupport.parseConnectorKey(upsert.pointerKey());
    if (connectorKey != null) {
      tx.add(
          buildJobIndexReferenceUpsert(
              JobIndexBackendSupport.connectorPartitionKey(connectorKey),
              JobIndexBackendSupport.connectorSortKey(connectorKey),
              JobIndexBackendSupport.KIND_CONNECTOR,
              upsert,
              JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY));
      return;
    }
    var globalStateKey = JobIndexBackendSupport.parseGlobalStateKey(upsert.pointerKey());
    if (globalStateKey != null) {
      tx.add(
          buildJobIndexReferenceUpsert(
              JobIndexBackendSupport.globalStatePartitionKey(globalStateKey),
              JobIndexBackendSupport.globalStateSortKey(globalStateKey),
              JobIndexBackendSupport.KIND_GLOBAL_STATE,
              upsert,
              JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY));
      return;
    }
    var accountStateKey = JobIndexBackendSupport.parseAccountStateKey(upsert.pointerKey());
    if (accountStateKey != null) {
      tx.add(
          buildJobIndexReferenceUpsert(
              JobIndexBackendSupport.accountStatePartitionKey(accountStateKey),
              JobIndexBackendSupport.accountStateSortKey(accountStateKey),
              JobIndexBackendSupport.KIND_ACCOUNT_STATE,
              upsert,
              JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY));
      return;
    }
    var connectorStateKey = JobIndexBackendSupport.parseConnectorStateKey(upsert.pointerKey());
    if (connectorStateKey != null) {
      tx.add(
          buildJobIndexReferenceUpsert(
              JobIndexBackendSupport.connectorStatePartitionKey(connectorStateKey),
              JobIndexBackendSupport.connectorStateSortKey(connectorStateKey),
              JobIndexBackendSupport.KIND_CONNECTOR_STATE,
              upsert,
              JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY));
      return;
    }
    var dedupeKey = JobIndexBackendSupport.parseDedupeKey(upsert.pointerKey());
    if (dedupeKey != null) {
      tx.add(
          buildJobIndexReferenceUpsert(
              JobIndexBackendSupport.dedupePartitionKey(dedupeKey),
              JobIndexBackendSupport.dedupeSortKey(dedupeKey),
              JobIndexBackendSupport.KIND_DEDUPE,
              upsert,
              JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY));
      return;
    }
    throw new IllegalArgumentException(
        "Unsupported reconcile job index upsert key: " + upsert.pointerKey());
  }

  private void appendJobIndexDelete(
      List<TransactWriteItem> tx, ReconcileJobIndexStore.JobIndexDelete delete) {
    var lookupKey = JobIndexBackendSupport.parseLookupKey(delete.pointerKey());
    if (lookupKey != null) {
      tx.addAll(
          DynamoReconcileJobLookupCompatibility.ownedDeletes(
              table,
              lookupKey,
              delete.expectedVersion(),
              delete.expectedCanonicalPointerKey(),
              delete.expectedLookupStoragePartitionKey()));
      return;
    }
    var canonicalKey = JobIndexBackendSupport.parseCanonicalJobKey(delete.pointerKey());
    if (canonicalKey != null) {
      tx.add(
          buildCanonicalDelete(
              JobIndexBackendSupport.canonicalPartitionKey(canonicalKey),
              JobIndexBackendSupport.canonicalSortKey(canonicalKey),
              delete));
      return;
    }
    var parentKey = JobIndexBackendSupport.parseParentKey(delete.pointerKey());
    if (parentKey != null) {
      tx.add(
          buildReferenceDelete(
              JobIndexBackendSupport.parentPartitionKey(parentKey),
              JobIndexBackendSupport.parentSortKey(parentKey),
              delete));
      return;
    }
    var connectorKey = JobIndexBackendSupport.parseConnectorKey(delete.pointerKey());
    if (connectorKey != null) {
      tx.add(
          buildReferenceDelete(
              JobIndexBackendSupport.connectorPartitionKey(connectorKey),
              JobIndexBackendSupport.connectorSortKey(connectorKey),
              delete));
      return;
    }
    var globalStateKey = JobIndexBackendSupport.parseGlobalStateKey(delete.pointerKey());
    if (globalStateKey != null) {
      tx.add(
          buildReferenceDelete(
              JobIndexBackendSupport.globalStatePartitionKey(globalStateKey),
              JobIndexBackendSupport.globalStateSortKey(globalStateKey),
              delete));
      return;
    }
    var accountStateKey = JobIndexBackendSupport.parseAccountStateKey(delete.pointerKey());
    if (accountStateKey != null) {
      tx.add(
          buildReferenceDelete(
              JobIndexBackendSupport.accountStatePartitionKey(accountStateKey),
              JobIndexBackendSupport.accountStateSortKey(accountStateKey),
              delete));
      return;
    }
    var connectorStateKey = JobIndexBackendSupport.parseConnectorStateKey(delete.pointerKey());
    if (connectorStateKey != null) {
      tx.add(
          buildReferenceDelete(
              JobIndexBackendSupport.connectorStatePartitionKey(connectorStateKey),
              JobIndexBackendSupport.connectorStateSortKey(connectorStateKey),
              delete));
      return;
    }
    var dedupeKey = JobIndexBackendSupport.parseDedupeKey(delete.pointerKey());
    if (dedupeKey != null) {
      tx.add(
          buildReferenceDelete(
              JobIndexBackendSupport.dedupePartitionKey(dedupeKey),
              JobIndexBackendSupport.dedupeSortKey(dedupeKey),
              delete));
      return;
    }
    throw new IllegalArgumentException(
        "Unsupported reconcile job index delete key: " + delete.pointerKey());
  }

  private void appendJobIndexCheckAbsent(List<TransactWriteItem> tx, String pointerKey) {
    var lookupKey = JobIndexBackendSupport.parseLookupKey(pointerKey);
    if (lookupKey == null) {
      throw new IllegalArgumentException(
          "Unsupported reconcile job index check-absent key: " + pointerKey);
    }
    tx.addAll(DynamoReconcileJobLookupCompatibility.checkAbsent(table, lookupKey));
  }

  private void appendJobIndexCheck(
      List<TransactWriteItem> tx, ReconcileJobIndexStore.JobIndexCheck check) {
    var canonicalKey = JobIndexBackendSupport.parseCanonicalJobKey(check.pointerKey());
    if (canonicalKey == null) {
      throw new IllegalArgumentException(
          "Unsupported reconcile job index version check key: " + check.pointerKey());
    }
    Map<String, String> names = new HashMap<>();
    names.put("#v", ATTR_VERSION);
    Map<String, AttributeValue> values = new HashMap<>();
    values.put(":expected", AttributeValue.fromN(Long.toString(check.expectedVersion())));
    String condition = "#v = :expected";
    if (check.requireCleanupLock()) {
      names.put("#lock", JobIndexBackendSupport.ATTR_CLEANUP_DELETE_IN_PROGRESS);
      values.put(":true", AttributeValue.fromBool(true));
      condition += " AND #lock = :true";
    }
    tx.add(
        TransactWriteItem.builder()
            .conditionCheck(
                ConditionCheck.builder()
                    .tableName(table)
                    .key(
                        Map.of(
                            ATTR_PARTITION_KEY,
                            AttributeValue.fromS(
                                JobIndexBackendSupport.canonicalPartitionKey(canonicalKey)),
                            ATTR_SORT_KEY,
                            AttributeValue.fromS(
                                JobIndexBackendSupport.canonicalSortKey(canonicalKey))))
                    .conditionExpression(condition)
                    .expressionAttributeNames(names)
                    .expressionAttributeValues(values)
                    .build())
            .build());
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
      JobIndexBackendSupport.CanonicalJobKey key, ReconcileJobIndexStore.JobIndexUpsert upsert) {
    if (upsert.expectedVersion() > 0L) {
      Map<String, String> names =
          Map.ofEntries(
              Map.entry("#kind", ATTR_KIND),
              Map.entry("#v", ATTR_VERSION),
              Map.entry("#pointer", JobIndexBackendSupport.ATTR_POINTER_KEY),
              Map.entry("#blob", JobIndexBackendSupport.ATTR_BLOB_URI),
              Map.entry("#account", JobIndexBackendSupport.ATTR_ACCOUNT_ID),
              Map.entry("#job", JobIndexBackendSupport.ATTR_JOB_ID),
              Map.entry("#idx", JobIndexBackendSupport.ATTR_CLEANUP_INDEX_POINTER_KEYS),
              Map.entry("#ready", JobIndexBackendSupport.ATTR_CLEANUP_READY_POINTER_KEYS),
              Map.entry("#lock", JobIndexBackendSupport.ATTR_CLEANUP_DELETE_IN_PROGRESS));
      Map<String, AttributeValue> values = new HashMap<>();
      values.put(":kind", AttributeValue.fromS(JobIndexBackendSupport.KIND_CANONICAL_JOB));
      values.put(":expected", AttributeValue.fromN(Long.toString(upsert.expectedVersion())));
      values.put(":next", AttributeValue.fromN(Long.toString(upsert.expectedVersion() + 1L)));
      values.put(":pointer", AttributeValue.fromS(key.pointerKey()));
      values.put(":blob", AttributeValue.fromS(upsert.blobUri()));
      values.put(":account", AttributeValue.fromS(key.accountSegment()));
      values.put(":job", AttributeValue.fromS(key.jobSegment()));
      values.put(":idx", stringListValue(upsert.cleanupManifest().indexPointerKeys()));
      values.put(":ready", stringListValue(upsert.cleanupManifest().readyPointerKeys()));
      return TransactWriteItem.builder()
          .update(
              Update.builder()
                  .tableName(table)
                  .key(
                      Map.of(
                          ATTR_PARTITION_KEY,
                          AttributeValue.fromS(JobIndexBackendSupport.canonicalPartitionKey(key)),
                          ATTR_SORT_KEY,
                          AttributeValue.fromS(JobIndexBackendSupport.canonicalSortKey(key))))
                  .updateExpression(
                      "SET #kind = :kind, #v = :next, #pointer = :pointer, #blob = :blob, "
                          + "#account = :account, #job = :job, #idx = :idx, #ready = :ready")
                  .conditionExpression("#v = :expected AND attribute_not_exists(#lock)")
                  .expressionAttributeNames(names)
                  .expressionAttributeValues(values)
                  .build())
          .build();
    }
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
    item.put(JobIndexBackendSupport.ATTR_CLEANUP_MANIFEST_COMPLETE, AttributeValue.fromBool(true));
    putStringList(
        item,
        JobIndexBackendSupport.ATTR_CLEANUP_INDEX_POINTER_KEYS,
        upsert.cleanupManifest().indexPointerKeys());
    putStringList(
        item,
        JobIndexBackendSupport.ATTR_CLEANUP_READY_POINTER_KEYS,
        upsert.cleanupManifest().readyPointerKeys());
    return buildPut(item, upsert.expectedVersion());
  }

  private TransactWriteItem buildJobIndexReferenceUpsert(
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

  private TransactWriteItem buildCanonicalDelete(
      String partitionKey, String sortKey, ReconcileJobIndexStore.JobIndexDelete delete) {
    Map<String, String> names = new HashMap<>();
    names.put("#v", ATTR_VERSION);
    Map<String, AttributeValue> values = new HashMap<>();
    values.put(":expected", AttributeValue.fromN(Long.toString(delete.expectedVersion())));
    String condition = "#v = :expected";
    if (delete.requireCleanupLock()) {
      names.put("#lock", JobIndexBackendSupport.ATTR_CLEANUP_DELETE_IN_PROGRESS);
      values.put(":true", AttributeValue.fromBool(true));
      condition += " AND #lock = :true";
    }
    return TransactWriteItem.builder()
        .delete(
            Delete.builder()
                .tableName(table)
                .key(
                    Map.of(
                        ATTR_PARTITION_KEY, AttributeValue.fromS(partitionKey),
                        ATTR_SORT_KEY, AttributeValue.fromS(sortKey)))
                .conditionExpression(condition)
                .expressionAttributeNames(names)
                .expressionAttributeValues(values)
                .build())
        .build();
  }

  private TransactWriteItem buildReferenceDelete(
      String partitionKey, String sortKey, ReconcileJobIndexStore.JobIndexDelete delete) {
    if (delete.expectedCanonicalPointerKey().isBlank()) {
      return buildDelete(partitionKey, sortKey, delete.expectedVersion());
    }
    return TransactWriteItem.builder()
        .delete(
            Delete.builder()
                .tableName(table)
                .key(
                    Map.of(
                        ATTR_PARTITION_KEY, AttributeValue.fromS(partitionKey),
                        ATTR_SORT_KEY, AttributeValue.fromS(sortKey)))
                .conditionExpression("#v = :expected AND #owner = :owner")
                .expressionAttributeNames(
                    Map.of(
                        "#v",
                        ATTR_VERSION,
                        "#owner",
                        JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY))
                .expressionAttributeValues(
                    Map.of(
                        ":expected", AttributeValue.fromN(Long.toString(delete.expectedVersion())),
                        ":owner", AttributeValue.fromS(delete.expectedCanonicalPointerKey())))
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

  private static void putStringList(
      Map<String, AttributeValue> item, String name, List<String> values) {
    if (values == null || values.isEmpty()) {
      return;
    }
    List<AttributeValue> attrs = new ArrayList<>();
    for (String value : values) {
      if (value != null && !value.isBlank()) {
        attrs.add(AttributeValue.fromS(value));
      }
    }
    if (!attrs.isEmpty()) {
      item.put(name, AttributeValue.fromL(attrs));
    }
  }

  private static AttributeValue stringListValue(List<String> values) {
    return AttributeValue.fromL(
        values == null
            ? List.of()
            : values.stream()
                .filter(value -> value != null && !value.isBlank())
                .map(AttributeValue::fromS)
                .toList());
  }
}
