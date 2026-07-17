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

import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.storage.aws.DynamoDbClientManager;
import ai.floedb.floecat.storage.spi.PointerStore.CasDelete;
import ai.floedb.floecat.storage.spi.PointerStore.CasUpsert;
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
          appendJobIndexUpsert(
              tx,
              new CasUpsert(
                  upsert.pointerKey(),
                  upsert.expectedVersion(),
                  switch (upsert.referenceKind()) {
                    case PRK_BLOB_URI ->
                        PointerReferences.blobPointer(
                            upsert.pointerKey(), upsert.blobUri(), upsert.expectedVersion() + 1L);
                    case PRK_INLINE_JSON ->
                        PointerReferences.inlineJsonPointer(
                            upsert.pointerKey(), upsert.blobUri(), upsert.expectedVersion() + 1L);
                    case PRK_POINTER_KEY ->
                        PointerReferences.pointerKeyPointer(
                            upsert.pointerKey(), upsert.blobUri(), upsert.expectedVersion() + 1L);
                    case PRK_OPAQUE_MARKER ->
                        PointerReferences.opaqueMarkerPointer(
                            upsert.pointerKey(), upsert.blobUri(), upsert.expectedVersion() + 1L);
                    case PRK_UNSPECIFIED, UNRECOGNIZED ->
                        throw new IllegalStateException(
                            "missing pointer reference kind for " + upsert.pointerKey());
                  }));
        } else if (write instanceof ReconcileJobIndexStore.JobIndexDelete delete) {
          appendJobIndexDelete(tx, new CasDelete(delete.pointerKey(), delete.expectedVersion()));
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

  private boolean appendJobIndexUpsert(List<TransactWriteItem> tx, CasUpsert upsert) {
    var lookupKey = JobIndexBackendSupport.parseLookupKey(upsert.key());
    if (lookupKey != null) {
      String sortKey = JobIndexBackendSupport.lookupSortKey(lookupKey);
      tx.add(
          buildJobIndexReferenceUpsert(
              JobIndexBackendSupport.lookupPartitionKey(),
              sortKey,
              JobIndexBackendSupport.KIND_LOOKUP,
              upsert,
              JobIndexBackendSupport.ATTR_BLOB_URI));
      if (upsert.expectedVersion() == 0L) {
        tx.add(buildCheckAbsent(JobIndexBackendSupport.legacyLookupPartitionKey(), sortKey));
      }
      return true;
    }
    var canonicalKey = JobIndexBackendSupport.parseCanonicalJobKey(upsert.key());
    if (canonicalKey != null) {
      tx.add(buildCanonicalUpsert(canonicalKey, upsert));
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
    var lookupKey = JobIndexBackendSupport.parseLookupKey(delete.key());
    if (lookupKey != null) {
      LookupPhysicalKey physicalKey = lookupDeletePhysicalKey(lookupKey, delete.expectedVersion());
      tx.add(
          buildDelete(physicalKey.partitionKey(), physicalKey.sortKey(), delete.expectedVersion()));
      return true;
    }
    var canonicalKey = JobIndexBackendSupport.parseCanonicalJobKey(delete.key());
    if (canonicalKey != null) {
      tx.add(
          buildDelete(
              JobIndexBackendSupport.canonicalPartitionKey(canonicalKey),
              JobIndexBackendSupport.canonicalSortKey(canonicalKey),
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

  private LookupPhysicalKey lookupDeletePhysicalKey(
      JobIndexBackendSupport.LookupKey lookupKey, long expectedVersion) {
    String sortKey = JobIndexBackendSupport.lookupSortKey(lookupKey);
    var current =
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
                                AttributeValue.fromS(JobIndexBackendSupport.lookupPartitionKey()),
                                ATTR_SORT_KEY,
                                AttributeValue.fromS(sortKey)))
                        .build()));
    if (current.hasItem()
        && !current.item().isEmpty()
        && longAttr(current.item(), ATTR_VERSION) == expectedVersion) {
      return new LookupPhysicalKey(JobIndexBackendSupport.lookupPartitionKey(), sortKey);
    }
    return new LookupPhysicalKey(JobIndexBackendSupport.legacyLookupPartitionKey(), sortKey);
  }

  private record LookupPhysicalKey(String partitionKey, String sortKey) {}

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

  private TransactWriteItem buildCheckAbsent(String partitionKey, String sortKey) {
    return TransactWriteItem.builder()
        .conditionCheck(
            ConditionCheck.builder()
                .tableName(table)
                .key(
                    Map.of(
                        ATTR_PARTITION_KEY, AttributeValue.fromS(partitionKey),
                        ATTR_SORT_KEY, AttributeValue.fromS(sortKey)))
                .conditionExpression("attribute_not_exists(#pk)")
                .expressionAttributeNames(Map.of("#pk", ATTR_PARTITION_KEY))
                .build())
        .build();
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
