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
import static ai.floedb.floecat.storage.kv.KvAttributes.ATTR_VERSION;

import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.aws.DynamoDbClientManager;
import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.exception.AbortedException;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionCheck;
import software.amazon.awssdk.services.dynamodb.model.Delete;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

@Singleton
@IfBuildProperty(name = "floecat.kv", stringValue = "dynamodb")
public class DynamoReconcileReadyQueueBackend implements ReconcileReadyQueueBackend {
  static final String ATTR_READY_POINTER_KEY = "ready_pointer_key";
  static final String ATTR_CANONICAL_POINTER_KEY = "canonical_pointer_key";
  static final String ATTR_ACCOUNT_ID = "account_id";
  static final String ATTR_JOB_ID = "job_id";
  static final String ATTR_DUE_AT_MS = "due_at_ms";
  static final String ATTR_INDEX_TYPE = "ready_index_type";
  static final String ATTR_FILTER_VALUE = "ready_filter_value";
  static final String KIND_READY_ENTRY = "ReconcileReadyEntry";
  private static final String GLOBAL_POINTER_PARTITION_KEY = "_ACCOUNT_DIR";
  private static final String ATTR_BLOB_URI = "blob_uri";

  @Inject Instance<DynamoDbClientManager> dynamoDbClientManager;
  private final RefreshingDynamoCaller dynamoCaller = new RefreshingDynamoCaller();

  @ConfigProperty(name = "floecat.kv.table", defaultValue = "floecat_pointers")
  String table = "floecat_pointers";

  public DynamoReconcileReadyQueueBackend() {}

  public void bind(Supplier<DynamoDbClient> dynamoDbSupplier, String table) {
    dynamoCaller.bind(dynamoDbSupplier);
    this.table = table;
  }

  public void bind(DynamoDbClientManager manager, String table) {
    dynamoCaller.bind(manager);
    this.table = table;
  }

  @Override
  public ReconcileReadyQueueStore.ReadyQueueScanPage scanReadySlice(
      ReadyQueueSlice slice,
      int pageSize,
      String pageToken,
      ReconcileReadyQueueStore.LeaseScanStats scanStats) {
    assertLeaseScanActive(scanStats);
    QueryRequest.Builder query =
        QueryRequest.builder()
            .tableName(table)
            .consistentRead(true)
            .limit(Math.max(1, pageSize))
            .expressionAttributeNames(Map.of("#pk", ATTR_PARTITION_KEY))
            .keyConditionExpression("#pk = :pk")
            .expressionAttributeValues(
                Map.of(":pk", AttributeValue.fromS(ReadyQueueBackendSupport.partitionKey(slice))));

    String token = ReadyQueueBackendSupport.stripLeadingSlash(pageToken);
    if (!token.isBlank()) {
      query.exclusiveStartKey(
          Map.of(
              ATTR_PARTITION_KEY,
                  AttributeValue.fromS(ReadyQueueBackendSupport.partitionKey(slice)),
              ATTR_SORT_KEY, AttributeValue.fromS(token)));
    }

    applyLeaseScanTimeout(query, scanStats);
    var response =
        runLeaseScanCall(
            scanStats,
            () -> dynamoCaller.call(dynamoDbClientManager, client -> client.query(query.build())));
    List<ReconcileReadyQueueStore.ReadyQueueEntry> entries =
        new ArrayList<>(response.items().size());
    for (var item : response.items()) {
      entries.add(
          new ReconcileReadyQueueStore.ReadyQueueEntry(
              stringAttr(item, ATTR_READY_POINTER_KEY),
              stringAttr(item, ATTR_CANONICAL_POINTER_KEY),
              stringAttr(item, ATTR_ACCOUNT_ID),
              stringAttr(item, ATTR_JOB_ID),
              longAttr(item, ATTR_DUE_AT_MS),
              slice.indexType(),
              stringAttr(item, ATTR_FILTER_VALUE)));
    }

    String nextPageToken = "";
    if (response.lastEvaluatedKey() != null && !response.lastEvaluatedKey().isEmpty()) {
      nextPageToken = "/" + response.lastEvaluatedKey().get(ATTR_SORT_KEY).s();
    }
    return new ReconcileReadyQueueStore.ReadyQueueScanPage(entries, nextPageToken);
  }

  @Override
  public ReadyQueueScanPage scanAllReadyEntries(int pageSize, String pageToken) {
    QueryRequest.Builder query =
        QueryRequest.builder()
            .tableName(table)
            .consistentRead(true)
            .limit(Math.max(1, pageSize))
            .expressionAttributeNames(Map.of("#pk", ATTR_PARTITION_KEY))
            .keyConditionExpression("#pk = :pk")
            .expressionAttributeValues(
                Map.of(
                    ":pk",
                    AttributeValue.fromS(ReadyQueueBackendSupport.maintenancePartitionKey())));
    String token = ReadyQueueBackendSupport.stripLeadingSlash(pageToken);
    if (!token.isBlank()) {
      query.exclusiveStartKey(
          Map.of(
              ATTR_PARTITION_KEY,
                  AttributeValue.fromS(ReadyQueueBackendSupport.maintenancePartitionKey()),
              ATTR_SORT_KEY, AttributeValue.fromS(token)));
    }
    var response = dynamoCaller.call(dynamoDbClientManager, client -> client.query(query.build()));
    List<ReconcileReadyQueueStore.ReadyQueueEntry> entries = new ArrayList<>();
    for (var item : response.items()) {
      try {
        entries.add(
            new ReconcileReadyQueueStore.ReadyQueueEntry(
                stringAttr(item, ATTR_READY_POINTER_KEY),
                stringAttr(item, ATTR_CANONICAL_POINTER_KEY),
                stringAttr(item, ATTR_ACCOUNT_ID),
                stringAttr(item, ATTR_JOB_ID),
                longAttr(item, ATTR_DUE_AT_MS),
                ReconcileReadyQueueStore.ReadyIndexType.valueOf(stringAttr(item, ATTR_INDEX_TYPE)),
                stringAttr(item, ATTR_FILTER_VALUE)));
      } catch (IllegalArgumentException ignored) {
        // Corrupt maintenance rows are removed through normal storage repair/TTL rather than
        // being presented as a different ready index type.
      }
    }
    String next = "";
    if (response.lastEvaluatedKey() != null && !response.lastEvaluatedKey().isEmpty()) {
      next = "/" + response.lastEvaluatedKey().get(ATTR_SORT_KEY).s();
    }
    return new ReadyQueueScanPage(List.copyOf(entries), next);
  }

  @Override
  public boolean deleteReadyEntry(
      ReconcileReadyQueueStore.ReadyQueueEntry expected,
      CanonicalPointerSnapshot expectedCanonicalSnapshot) {
    if (expected == null) {
      return false;
    }
    if (expectedCanonicalSnapshot != null
        && !java.util.Objects.equals(
            expected.canonicalPointerKey(), expectedCanonicalSnapshot.canonicalPointerKey())) {
      return false;
    }
    ReadyQueueBackendSupport.ReadyQueueRow row =
        ReadyQueueBackendSupport.toReadyQueueRow(
            expected.readyPointerKey(), expected.canonicalPointerKey());
    var canonicalKey = JobIndexBackendSupport.parseCanonicalJobKey(expected.canonicalPointerKey());
    if (row == null || canonicalKey == null) {
      return false;
    }
    Map<String, AttributeValue> canonicalStorageKey =
        Map.of(
            ATTR_PARTITION_KEY,
            AttributeValue.fromS(JobIndexBackendSupport.canonicalPartitionKey(canonicalKey)),
            ATTR_SORT_KEY,
            AttributeValue.fromS(JobIndexBackendSupport.canonicalSortKey(canonicalKey)));
    ConditionCheck.Builder canonicalCheck =
        ConditionCheck.builder().tableName(table).key(canonicalStorageKey);
    if (expectedCanonicalSnapshot == null) {
      canonicalCheck
          .conditionExpression("attribute_not_exists(#pk)")
          .expressionAttributeNames(Map.of("#pk", ATTR_PARTITION_KEY));
    } else {
      canonicalCheck
          .conditionExpression("#v = :v AND #ref = :ref")
          .expressionAttributeNames(
              Map.of("#v", ATTR_VERSION, "#ref", JobIndexBackendSupport.ATTR_BLOB_URI))
          .expressionAttributeValues(
              Map.of(
                  ":v", AttributeValue.fromN(Long.toString(expectedCanonicalSnapshot.version())),
                  ":ref", AttributeValue.fromS(expectedCanonicalSnapshot.blobUri())));
    }
    Delete readyDelete =
        Delete.builder()
            .tableName(table)
            .key(
                Map.of(
                    ATTR_PARTITION_KEY, AttributeValue.fromS(row.partitionKey()),
                    ATTR_SORT_KEY, AttributeValue.fromS(row.sortKey())))
            .conditionExpression("#ready = :ready AND #canonical = :canonical")
            .expressionAttributeNames(
                Map.of(
                    "#ready", ATTR_READY_POINTER_KEY,
                    "#canonical", ATTR_CANONICAL_POINTER_KEY))
            .expressionAttributeValues(
                Map.of(
                    ":ready", AttributeValue.fromS(expected.readyPointerKey()),
                    ":canonical", AttributeValue.fromS(expected.canonicalPointerKey())))
            .build();
    try {
      dynamoCaller.callVoid(
          dynamoDbClientManager,
          client ->
              client.transactWriteItems(
                  TransactWriteItemsRequest.builder()
                      .transactItems(
                          TransactWriteItem.builder()
                              .conditionCheck(canonicalCheck.build())
                              .build(),
                          TransactWriteItem.builder().delete(readyDelete).build(),
                          TransactWriteItem.builder()
                              .delete(
                                  Delete.builder()
                                      .tableName(table)
                                      .key(
                                          Map.of(
                                              ATTR_PARTITION_KEY,
                                              AttributeValue.fromS(
                                                  ReadyQueueBackendSupport
                                                      .maintenancePartitionKey()),
                                              ATTR_SORT_KEY,
                                              AttributeValue.fromS(
                                                  ReadyQueueBackendSupport.maintenanceSortKey(
                                                      row))))
                                      .build())
                              .build())
                      .build()));
      return true;
    } catch (TransactionCanceledException ignored) {
      return false;
    }
  }

  @Override
  public Optional<CanonicalPointerSnapshot> loadCanonicalSnapshot(
      String canonicalPointerKey, ReconcileReadyQueueStore.LeaseScanStats scanStats) {
    assertLeaseScanActive(scanStats);
    var canonicalJobKey = JobIndexBackendSupport.parseCanonicalJobKey(canonicalPointerKey);
    if (canonicalJobKey != null) {
      GetItemRequest.Builder request =
          GetItemRequest.builder()
              .tableName(table)
              .consistentRead(true)
              .key(
                  Map.of(
                      ATTR_PARTITION_KEY,
                      AttributeValue.fromS(
                          JobIndexBackendSupport.canonicalPartitionKey(canonicalJobKey)),
                      ATTR_SORT_KEY,
                      AttributeValue.fromS(
                          JobIndexBackendSupport.canonicalSortKey(canonicalJobKey))));
      applyLeaseScanTimeout(request, scanStats);
      var response =
          runLeaseScanCall(
              scanStats,
              () ->
                  dynamoCaller.call(
                      dynamoDbClientManager, client -> client.getItem(request.build())));
      if (!response.hasItem() || response.item().isEmpty()) {
        return Optional.empty();
      }
      return Optional.of(
          new CanonicalPointerSnapshot(
              canonicalPointerKey,
              stringAttr(response.item(), ATTR_BLOB_URI),
              longAttr(response.item(), ATTR_VERSION)));
    }

    DynamoPointerKey key = dynamoPointerKey(canonicalPointerKey);
    if (key == null) {
      return Optional.empty();
    }
    GetItemRequest.Builder request =
        GetItemRequest.builder()
            .tableName(table)
            .consistentRead(true)
            .key(
                Map.of(
                    ATTR_PARTITION_KEY, AttributeValue.fromS(key.partitionKey()),
                    ATTR_SORT_KEY, AttributeValue.fromS(key.sortKey())));
    applyLeaseScanTimeout(request, scanStats);
    var response =
        runLeaseScanCall(
            scanStats,
            () ->
                dynamoCaller.call(
                    dynamoDbClientManager, client -> client.getItem(request.build())));
    if (!response.hasItem() || response.item().isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        new CanonicalPointerSnapshot(
            canonicalPointerKey,
            stringAttr(response.item(), ATTR_BLOB_URI),
            longAttr(response.item(), ATTR_VERSION)));
  }

  private static DynamoPointerKey dynamoPointerKey(String pointerKey) {
    String normalized = ReadyQueueBackendSupport.stripLeadingSlash(pointerKey);
    if (normalized.isBlank()) {
      return null;
    }
    String accountByIdPrefix =
        ReadyQueueBackendSupport.stripLeadingSlash(Keys.accountPointerByIdPrefix());
    String accountByNamePrefix =
        ReadyQueueBackendSupport.stripLeadingSlash(Keys.accountPointerByNamePrefix());
    String accountRootPrefix = ReadyQueueBackendSupport.stripLeadingSlash(Keys.accountRootPrefix());
    if (normalized.startsWith(accountByIdPrefix) || normalized.startsWith(accountByNamePrefix)) {
      return new DynamoPointerKey(GLOBAL_POINTER_PARTITION_KEY, normalized);
    }
    if (!normalized.startsWith(accountRootPrefix)) {
      return null;
    }
    int firstSlash = normalized.indexOf('/');
    int secondSlash = normalized.indexOf('/', firstSlash + 1);
    if (secondSlash < 0) {
      return null;
    }
    String accountId = normalized.substring(firstSlash + 1, secondSlash);
    String remainder = normalized.substring(secondSlash + 1);
    return new DynamoPointerKey(accountRootPrefix + accountId, remainder);
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

  private record DynamoPointerKey(String partitionKey, String sortKey) {}

  private static void assertLeaseScanActive(ReconcileReadyQueueStore.LeaseScanStats scanStats) {
    if (scanStats != null && scanStats.shouldStop()) {
      throw new LeaseScanAbortedException(scanStats.abortedByCaller);
    }
  }

  private static void applyLeaseScanTimeout(
      QueryRequest.Builder request, ReconcileReadyQueueStore.LeaseScanStats scanStats) {
    leaseScanTimeout(scanStats).ifPresent(request::overrideConfiguration);
  }

  private static void applyLeaseScanTimeout(
      GetItemRequest.Builder request, ReconcileReadyQueueStore.LeaseScanStats scanStats) {
    leaseScanTimeout(scanStats).ifPresent(request::overrideConfiguration);
  }

  private static Optional<AwsRequestOverrideConfiguration> leaseScanTimeout(
      ReconcileReadyQueueStore.LeaseScanStats scanStats) {
    if (scanStats == null || scanStats.deadlineAtMs <= 0L) {
      return Optional.empty();
    }
    long remainingMs = scanStats.deadlineAtMs - System.currentTimeMillis();
    if (remainingMs <= 0L) {
      scanStats.abortedByDeadline = true;
      throw new LeaseScanAbortedException(false);
    }
    return Optional.of(
        AwsRequestOverrideConfiguration.builder()
            .apiCallAttemptTimeout(Duration.ofMillis(Math.max(1L, remainingMs)))
            .apiCallTimeout(Duration.ofMillis(Math.max(1L, remainingMs)))
            .build());
  }

  private static <T> T runLeaseScanCall(
      ReconcileReadyQueueStore.LeaseScanStats scanStats, java.util.function.Supplier<T> call) {
    try {
      return call.get();
    } catch (ApiCallTimeoutException | ApiCallAttemptTimeoutException timeout) {
      throw leaseScanAborted(scanStats, false, timeout);
    } catch (AbortedException aborted) {
      throw leaseScanAborted(scanStats, callerCancelled(scanStats), aborted);
    }
  }

  private static LeaseScanAbortedException leaseScanAborted(
      ReconcileReadyQueueStore.LeaseScanStats scanStats,
      boolean callerCancelled,
      RuntimeException cause) {
    if (scanStats != null) {
      if (callerCancelled) {
        scanStats.abortedByCaller = true;
      } else {
        scanStats.abortedByDeadline = true;
      }
    }
    return new LeaseScanAbortedException(callerCancelled, cause);
  }

  private static boolean callerCancelled(ReconcileReadyQueueStore.LeaseScanStats scanStats) {
    return scanStats != null && scanStats.cancelled != null && scanStats.cancelled.getAsBoolean();
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value;
  }
}
