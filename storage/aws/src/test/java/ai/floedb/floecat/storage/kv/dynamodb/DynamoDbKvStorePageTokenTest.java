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
package ai.floedb.floecat.storage.kv.dynamodb;

import static ai.floedb.floecat.storage.kv.dynamodb.DynamoDbKvStore.decodeToken;
import static ai.floedb.floecat.storage.kv.dynamodb.DynamoDbKvStore.encodeToken;
import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.floedb.floecat.storage.kv.KvAttributes;
import ai.floedb.floecat.storage.kv.KvStore;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Pins the token contract behind {@link DynamoDbKvStore#pageTokenAfterKey}: a synthesized token
 * must be byte-identical to the lastEvaluatedKey token the store would emit for the same (pk, sk)
 * position, so callers can resume a scan exactly after a row they consumed.
 */
class DynamoDbKvStorePageTokenTest implements KvAttributes {

  private final DynamoDbKvStore store =
      new DynamoDbKvStore((software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient) null, "t");

  @Test
  void pageTokenAfterKeyMatchesLastEvaluatedKeyEncoding() {
    var key = new KvStore.Key("accounts/acct-1", "catalogs/cat/namespaces/by-path/sales");
    Map<String, AttributeValue> lek =
        Map.of(
            ATTR_PARTITION_KEY, AttributeValue.fromS(key.partitionKey()),
            ATTR_SORT_KEY, AttributeValue.fromS(key.sortKey()));

    assertEquals(encodeToken(lek).orElseThrow(), store.pageTokenAfterKey(key));
  }

  @Test
  void pageTokenAfterKeyRoundTripsToTheSamePosition() {
    var key = new KvStore.Key("accounts/acct-1", "tables/by-name/order%20items");

    Map<String, AttributeValue> decoded =
        decodeToken(Optional.of(store.pageTokenAfterKey(key))).orElseThrow();

    assertEquals(key.partitionKey(), decoded.get(ATTR_PARTITION_KEY).s());
    assertEquals(key.sortKey(), decoded.get(ATTR_SORT_KEY).s());
  }
}
