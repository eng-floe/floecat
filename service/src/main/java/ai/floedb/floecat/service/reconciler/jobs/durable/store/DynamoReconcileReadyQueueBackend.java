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

import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

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

  private DynamoDbClient dynamoDb;
  private String table;

  @Inject
  public DynamoReconcileReadyQueueBackend(
      DynamoDbClient dynamoDb, @ConfigProperty(name = "floecat.kv.table") String table) {
    this.dynamoDb = dynamoDb;
    this.table = table;
  }

  public DynamoReconcileReadyQueueBackend() {}

  public void bind(DynamoDbClient dynamoDb, String table) {
    this.dynamoDb = dynamoDb;
    this.table = table;
  }

  @Override
  public ReconcileReadyQueueStore.ReadyQueueScanPage scanReadySlice(
      ReadyQueueSlice slice, int pageSize, String pageToken) {
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

    var response = dynamoDb.query(query.build());
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
    ScanRequest.Builder scan =
        ScanRequest.builder()
            .tableName(table)
            .consistentRead(true)
            .limit(Math.max(1, pageSize))
            .expressionAttributeNames(Map.of("#pk", ATTR_PARTITION_KEY))
            .filterExpression("begins_with(#pk, :pk)")
            .expressionAttributeValues(Map.of(":pk", AttributeValue.fromS("reconcile-ready#")));

    ReadyQueueBackendSupport.ReadyRowCursor cursor =
        ReadyQueueBackendSupport.decodeCursor(blankToEmpty(pageToken));
    if (cursor != null) {
      scan.exclusiveStartKey(
          Map.of(
              ATTR_PARTITION_KEY,
              AttributeValue.fromS(cursor.partitionKey()),
              ATTR_SORT_KEY,
              AttributeValue.fromS(cursor.sortKey())));
    }

    var response = dynamoDb.scan(scan.build());
    List<ReconcileReadyQueueStore.ReadyQueueEntry> entries =
        new ArrayList<>(response.items().size());
    for (var item : response.items()) {
      ReadyQueueBackendSupport.ReadyQueueRow row =
          ReadyQueueBackendSupport.rowFromNativeReadyItem(
              stringAttr(item, ATTR_READY_POINTER_KEY),
              stringAttr(item, ATTR_CANONICAL_POINTER_KEY),
              stringAttr(item, ATTR_PARTITION_KEY),
              stringAttr(item, ATTR_SORT_KEY),
              stringAttr(item, ATTR_FILTER_VALUE),
              stringAttr(item, ATTR_INDEX_TYPE),
              stringAttr(item, ATTR_ACCOUNT_ID),
              stringAttr(item, ATTR_JOB_ID),
              longAttr(item, ATTR_DUE_AT_MS));
      if (row != null) {
        entries.add(row.entry());
      }
    }

    String nextPageToken = "";
    if (response.lastEvaluatedKey() != null && !response.lastEvaluatedKey().isEmpty()) {
      nextPageToken =
          ReadyQueueBackendSupport.encodeCursor(
              stringAttr(response.lastEvaluatedKey(), ATTR_PARTITION_KEY),
              stringAttr(response.lastEvaluatedKey(), ATTR_SORT_KEY));
    }
    return new ReadyQueueScanPage(entries, nextPageToken);
  }

  @Override
  public boolean deleteReadyEntry(String readyPointerKey) {
    // Resolve the primary key from the ready pointer alone. The canonical pointer key is a stored
    // attribute, not part of the key, and a stale/leaked entry's canonical pointer is often already
    // gone; passing a blank canonical here used to make decode reject the key, so this delete was a
    // silent no-op for every caller (inline prune and ReconcileJobGc alike).
    ReadyQueueBackendSupport.ReadyQueueRow row =
        ReadyQueueBackendSupport.toReadyQueueRow(readyPointerKey);
    if (row == null) {
      return false;
    }
    dynamoDb.deleteItem(
        DeleteItemRequest.builder()
            .tableName(table)
            .key(
                Map.of(
                    ATTR_PARTITION_KEY, AttributeValue.fromS(row.partitionKey()),
                    ATTR_SORT_KEY, AttributeValue.fromS(row.sortKey())))
            .build());
    return true;
  }

  @Override
  public Optional<CanonicalPointerSnapshot> loadCanonicalSnapshot(String canonicalPointerKey) {
    var canonicalJobKey = JobIndexBackendSupport.parseCanonicalJobKey(canonicalPointerKey);
    if (canonicalJobKey != null) {
      var response =
          dynamoDb.getItem(
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
                              JobIndexBackendSupport.canonicalSortKey(canonicalJobKey))))
                  .build());
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
    var response =
        dynamoDb.getItem(
            GetItemRequest.builder()
                .tableName(table)
                .consistentRead(true)
                .key(
                    Map.of(
                        ATTR_PARTITION_KEY, AttributeValue.fromS(key.partitionKey()),
                        ATTR_SORT_KEY, AttributeValue.fromS(key.sortKey())))
                .build());
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
    if (normalized.startsWith("accounts/by-id/") || normalized.startsWith("accounts/by-name/")) {
      return new DynamoPointerKey(GLOBAL_POINTER_PARTITION_KEY, normalized);
    }
    if (!normalized.startsWith("accounts/")) {
      return null;
    }
    int firstSlash = normalized.indexOf('/');
    int secondSlash = normalized.indexOf('/', firstSlash + 1);
    if (secondSlash < 0) {
      return null;
    }
    String accountId = normalized.substring(firstSlash + 1, secondSlash);
    String remainder = normalized.substring(secondSlash + 1);
    return new DynamoPointerKey("accounts/" + accountId, remainder);
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

  private static String blankToEmpty(String value) {
    return value == null ? "" : value;
  }
}
