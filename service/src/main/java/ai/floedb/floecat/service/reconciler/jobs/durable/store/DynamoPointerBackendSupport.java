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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.Delete;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

final class DynamoPointerBackendSupport {
  static final String POINTER_KIND = "Pointer";
  static final String GLOBAL_POINTER_PARTITION_KEY = "_ACCOUNT_DIR";
  static final String ATTR_BLOB_URI = "blob_uri";

  private DynamoPointerBackendSupport() {}

  static Optional<Pointer> loadPointer(DynamoDbClient dynamoDb, String table, String pointerKey) {
    DynamoPointerKey key = dynamoPointerKey(pointerKey);
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
        Pointer.newBuilder()
            .setKey(pointerKey)
            .setBlobUri(stringAttr(response.item(), ATTR_BLOB_URI))
            .setVersion(longAttr(response.item(), ATTR_VERSION))
            .build());
  }

  static List<Pointer> listPointersByPrefix(
      DynamoDbClient dynamoDb,
      String table,
      String prefix,
      int limit,
      String pageToken,
      StringBuilder nextPageToken) {
    DynamoPointerPrefix queryPrefix = dynamoPrefixKey(prefix);
    QueryRequest.Builder query =
        QueryRequest.builder().tableName(table).consistentRead(true).limit(Math.max(1, limit));

    if (queryPrefix.sortKeyPrefix().isBlank()) {
      query
          .expressionAttributeNames(Map.of("#pk", ATTR_PARTITION_KEY))
          .keyConditionExpression("#pk = :pk")
          .expressionAttributeValues(
              Map.of(":pk", AttributeValue.fromS(queryPrefix.partitionKey())));
    } else {
      query
          .expressionAttributeNames(Map.of("#pk", ATTR_PARTITION_KEY, "#sk", ATTR_SORT_KEY))
          .keyConditionExpression("#pk = :pk AND begins_with(#sk, :skp)")
          .expressionAttributeValues(
              Map.of(
                  ":pk", AttributeValue.fromS(queryPrefix.partitionKey()),
                  ":skp", AttributeValue.fromS(queryPrefix.sortKeyPrefix())));
    }

    String token = stripLeadingSlash(pageToken);
    if (!token.isBlank()) {
      query.exclusiveStartKey(
          Map.of(
              ATTR_PARTITION_KEY, AttributeValue.fromS(queryPrefix.partitionKey()),
              ATTR_SORT_KEY, AttributeValue.fromS(token)));
    }

    var response = dynamoDb.query(query.build());
    List<Pointer> pointers = new ArrayList<>(response.items().size());
    for (var item : response.items()) {
      pointers.add(
          Pointer.newBuilder()
              .setKey("/" + stringAttr(item, ATTR_SORT_KEY))
              .setBlobUri(stringAttr(item, ATTR_BLOB_URI))
              .setVersion(longAttr(item, ATTR_VERSION))
              .build());
    }
    nextPageToken.setLength(0);
    if (response.lastEvaluatedKey() != null && !response.lastEvaluatedKey().isEmpty()) {
      nextPageToken.append('/').append(response.lastEvaluatedKey().get(ATTR_SORT_KEY).s());
    }
    return pointers;
  }

  static boolean compareAndSetBatch(DynamoDbClient dynamoDb, String table, List<CasOp> ops) {
    if (ops == null || ops.isEmpty()) {
      return true;
    }
    List<TransactWriteItem> tx = new ArrayList<>(ops.size());
    for (CasOp op : ops) {
      if (op instanceof CasUpsert upsert) {
        tx.add(buildPointerUpsert(table, upsert));
      } else if (op instanceof CasDelete delete) {
        tx.add(buildPointerDelete(table, delete));
      }
    }
    try {
      dynamoDb.transactWriteItems(TransactWriteItemsRequest.builder().transactItems(tx).build());
      return true;
    } catch (TransactionCanceledException e) {
      return false;
    }
  }

  static TransactWriteItem buildPointerUpsert(String table, CasUpsert upsert) {
    DynamoPointerKey key = dynamoPointerKey(upsert.key());
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(ATTR_PARTITION_KEY, AttributeValue.fromS(key.partitionKey()));
    item.put(ATTR_SORT_KEY, AttributeValue.fromS(key.sortKey()));
    item.put(ATTR_KIND, AttributeValue.fromS(POINTER_KIND));
    item.put(ATTR_VERSION, AttributeValue.fromN(Long.toString(upsert.expectedVersion() + 1L)));
    item.put(ATTR_BLOB_URI, AttributeValue.fromS(upsert.next().getBlobUri()));
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

  static TransactWriteItem buildPointerDelete(String table, CasDelete delete) {
    DynamoPointerKey key = dynamoPointerKey(delete.key());
    return TransactWriteItem.builder()
        .delete(
            Delete.builder()
                .tableName(table)
                .key(
                    Map.of(
                        ATTR_PARTITION_KEY, AttributeValue.fromS(key.partitionKey()),
                        ATTR_SORT_KEY, AttributeValue.fromS(key.sortKey())))
                .conditionExpression("#v = :expected")
                .expressionAttributeNames(Map.of("#v", ATTR_VERSION))
                .expressionAttributeValues(
                    Map.of(
                        ":expected", AttributeValue.fromN(Long.toString(delete.expectedVersion()))))
                .build())
        .build();
  }

  static DynamoPointerKey dynamoPointerKey(String pointerKey) {
    String normalized = stripLeadingSlash(pointerKey);
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

  private static DynamoPointerPrefix dynamoPrefixKey(String prefix) {
    String normalized = stripLeadingSlash(prefix);
    if (normalized.equals("accounts") || normalized.equals("accounts/")) {
      return new DynamoPointerPrefix("accounts", "");
    }
    if (normalized.startsWith("accounts/by-id/") || normalized.startsWith("accounts/by-name/")) {
      return new DynamoPointerPrefix(GLOBAL_POINTER_PARTITION_KEY, normalized);
    }
    if (!normalized.startsWith("accounts/")) {
      throw new IllegalArgumentException("unexpected prefix: " + prefix);
    }
    int firstSlash = normalized.indexOf('/');
    int secondSlash = normalized.indexOf('/', firstSlash + 1);
    String accountId =
        secondSlash < 0
            ? normalized.substring(firstSlash + 1)
            : normalized.substring(firstSlash + 1, secondSlash);
    String remainderPrefix = secondSlash < 0 ? "" : normalized.substring(secondSlash + 1);
    return new DynamoPointerPrefix("accounts/" + accountId, remainderPrefix);
  }

  static String stripLeadingSlash(String key) {
    if (key == null || key.isBlank()) {
      return "";
    }
    return key.startsWith("/") ? key.substring(1) : key;
  }

  static String stringAttr(Map<String, AttributeValue> item, String name) {
    AttributeValue value = item.get(name);
    return value == null || value.s() == null ? "" : value.s();
  }

  static long longAttr(Map<String, AttributeValue> item, String name) {
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

  record DynamoPointerKey(String partitionKey, String sortKey) {}

  private record DynamoPointerPrefix(String partitionKey, String sortKeyPrefix) {}
}
