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

import java.util.List;
import java.util.Map;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionCheck;
import software.amazon.awssdk.services.dynamodb.model.Delete;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;

final class DynamoReconcileJobLookupCompatibility {
  private DynamoReconcileJobLookupCompatibility() {}

  static TransactWriteItem legacyCheckAbsent(
      String table, JobIndexBackendSupport.LookupKey lookupKey) {
    return checkAbsent(table, JobIndexBackendSupport.legacyLookupStorageKey(lookupKey));
  }

  static List<TransactWriteItem> checkAbsent(
      String table, JobIndexBackendSupport.LookupKey lookupKey) {
    return JobIndexBackendSupport.lookupReadStorageKeys(lookupKey).stream()
        .map(key -> checkAbsent(table, key))
        .toList();
  }

  static List<TransactWriteItem> deletes(
      String table, JobIndexBackendSupport.LookupKey lookupKey, long expectedVersion) {
    return deletes(table, lookupKey, expectedVersion, "");
  }

  static List<TransactWriteItem> deletes(
      String table,
      JobIndexBackendSupport.LookupKey lookupKey,
      long expectedVersion,
      String expectedCanonicalPointerKey) {
    return JobIndexBackendSupport.lookupReadStorageKeys(lookupKey).stream()
        .map(
            key ->
                deleteIfVersionOrAbsent(table, key, expectedVersion, expectedCanonicalPointerKey))
        .toList();
  }

  private static TransactWriteItem checkAbsent(
      String table, JobIndexBackendSupport.LookupStorageKey key) {
    return TransactWriteItem.builder()
        .conditionCheck(
            ConditionCheck.builder()
                .tableName(table)
                .key(dynamoKey(key))
                .conditionExpression("attribute_not_exists(#pk)")
                .expressionAttributeNames(Map.of("#pk", ATTR_PARTITION_KEY))
                .build())
        .build();
  }

  private static TransactWriteItem deleteIfVersionOrAbsent(
      String table,
      JobIndexBackendSupport.LookupStorageKey key,
      long expectedVersion,
      String expectedCanonicalPointerKey) {
    boolean checkOwner =
        expectedCanonicalPointerKey != null && !expectedCanonicalPointerKey.isBlank();
    Map<String, String> names = new java.util.HashMap<>();
    names.put("#pk", ATTR_PARTITION_KEY);
    names.put("#v", ATTR_VERSION);
    Map<String, AttributeValue> values = new java.util.HashMap<>();
    values.put(":expected", AttributeValue.fromN(Long.toString(expectedVersion)));
    String condition = "attribute_not_exists(#pk) OR #v = :expected";
    if (checkOwner) {
      names.put("#owner", JobIndexBackendSupport.ATTR_BLOB_URI);
      values.put(":owner", AttributeValue.fromS(expectedCanonicalPointerKey));
      condition = "attribute_not_exists(#pk) OR (#v = :expected AND #owner = :owner)";
    }
    return TransactWriteItem.builder()
        .delete(
            Delete.builder()
                .tableName(table)
                .key(dynamoKey(key))
                .conditionExpression(condition)
                .expressionAttributeNames(names)
                .expressionAttributeValues(values)
                .build())
        .build();
  }

  private static Map<String, AttributeValue> dynamoKey(
      JobIndexBackendSupport.LookupStorageKey key) {
    return Map.of(
        ATTR_PARTITION_KEY,
        AttributeValue.fromS(key.partitionKey()),
        ATTR_SORT_KEY,
        AttributeValue.fromS(key.sortKey()));
  }
}
