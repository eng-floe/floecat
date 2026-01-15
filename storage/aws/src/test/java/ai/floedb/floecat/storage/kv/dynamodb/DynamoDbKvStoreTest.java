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

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.storage.kv.KvAttributes;
import ai.floedb.floecat.storage.kv.KvStore;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.CancellationReason;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsResponse;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

public class DynamoDbKvStoreTest {

  @Test
  void putCas_create_only_uses_attribute_not_exists_and_succeeds() {
    FakeDynamoDbHandler handler = new FakeDynamoDbHandler();
    DynamoDbKvStore store = newStore(handler);

    KvStore.Record record = record("pk", "sk", "K", "v", 1L, Map.of());
    assertTrue(store.putCas(record, 0L).await().indefinitely());
    assertEquals("attribute_not_exists(pk)", handler.lastPutRequest.conditionExpression());
  }

  @Test
  void putCas_rejects_record_version_0() {
    FakeDynamoDbHandler handler = new FakeDynamoDbHandler();
    DynamoDbKvStore store = newStore(handler);

    KvStore.Record record = record("pk", "sk", "K", "v", 0L, Map.of());
    assertThrows(IllegalArgumentException.class, () -> store.putCas(record, 0L));
  }

  @Test
  void putCas_update_only_fails_when_version_mismatch() {
    FakeDynamoDbHandler handler = new FakeDynamoDbHandler();
    DynamoDbKvStore store = newStore(handler);

    KvStore.Record record = record("pk", "sk", "K", "v1", 1L, Map.of());
    assertTrue(store.putCas(record, 0L).await().indefinitely());

    KvStore.Record update = record("pk", "sk", "K", "v2", 2L, Map.of());
    assertFalse(store.putCas(update, 999L).await().indefinitely());
    assertEquals("#v = :ev", handler.lastPutRequest.conditionExpression());
  }

  @Test
  void deleteCas_expectedVersion_0_throws() {
    FakeDynamoDbHandler handler = new FakeDynamoDbHandler();
    DynamoDbKvStore store = newStore(handler);

    assertThrows(IllegalArgumentException.class, () -> store.deleteCas(key("pk", "sk"), 0L));
  }

  @Test
  void deleteCas_fails_when_version_mismatch() {
    FakeDynamoDbHandler handler = new FakeDynamoDbHandler();
    DynamoDbKvStore store = newStore(handler);

    KvStore.Record record = record("pk", "sk", "K", "v1", 1L, Map.of());
    assertTrue(store.putCas(record, 0L).await().indefinitely());
    assertFalse(store.deleteCas(key("pk", "sk"), 2L).await().indefinitely());
  }

  @Test
  void get_returns_record_with_attrs_and_version() {
    FakeDynamoDbHandler handler = new FakeDynamoDbHandler();
    DynamoDbKvStore store = newStore(handler);

    KvStore.Record record = record("pk", "sk", "K", "v1", 3L, Map.of("foo", "bar"));
    assertTrue(store.putCas(record, 0L).await().indefinitely());

    KvStore.Record got = store.get(key("pk", "sk")).await().indefinitely().orElseThrow();
    assertEquals(3L, got.version());
    assertEquals("bar", got.attrs().get("foo"));
  }

  @Test
  void recordToAv_skips_empty_value() {
    FakeDynamoDbHandler handler = new FakeDynamoDbHandler();
    DynamoDbKvStore store = newStore(handler);

    KvStore.Record record = new KvStore.Record(key("pk", "sk"), "K", new byte[0], Map.of(), 1L);
    assertTrue(store.putCas(record, 0L).await().indefinitely());
    assertFalse(handler.lastPutRequest.item().containsKey(KvAttributes.ATTR_VALUE));

    KvStore.Record got = store.get(key("pk", "sk")).await().indefinitely().orElseThrow();
    assertEquals(0, got.value().length);
  }

  @Test
  void attrs_round_trip_excludes_reserved_fields() {
    FakeDynamoDbHandler handler = new FakeDynamoDbHandler();
    DynamoDbKvStore store = newStore(handler);

    Map<String, String> attrs = Map.of("user", "ok");

    KvStore.Record record = record("pk", "sk", "K", "v1", 1L, attrs);
    assertTrue(store.putCas(record, 0L).await().indefinitely());

    KvStore.Record got = store.get(key("pk", "sk")).await().indefinitely().orElseThrow();
    assertEquals("ok", got.attrs().get("user"));
    assertFalse(got.attrs().containsKey(KvAttributes.ATTR_PARTITION_KEY));
    assertFalse(got.attrs().containsKey(KvAttributes.ATTR_SORT_KEY));
    assertFalse(got.attrs().containsKey(KvAttributes.ATTR_KIND));
    assertFalse(got.attrs().containsKey(KvAttributes.ATTR_VALUE));
    assertFalse(got.attrs().containsKey(KvAttributes.ATTR_VERSION));
  }

  @Test
  void encodeToken_empty_returns_empty_optional() throws Exception {
    Optional<String> token = DynamoDbKvStore.encodeToken(Map.of());
    assertTrue(token.isEmpty());
  }

  @Test
  void encodeToken_null_returns_empty_optional() throws Exception {
    Optional<String> token = DynamoDbKvStore.encodeToken(null);
    assertTrue(token.isEmpty());
  }

  @Test
  void token_round_trip_encode_decode_preserves_pk_and_sk() throws Exception {
    Map<String, AttributeValue> lek =
        Map.of(
            KvAttributes.ATTR_PARTITION_KEY, AttributeValue.builder().s("pk").build(),
            KvAttributes.ATTR_SORT_KEY, AttributeValue.builder().s("sk").build());

    Optional<String> token = DynamoDbKvStore.encodeToken(lek);
    Optional<Map<String, AttributeValue>> decoded = DynamoDbKvStore.decodeToken(token);
    assertTrue(decoded.isPresent());
    assertEquals("pk", decoded.get().get(KvAttributes.ATTR_PARTITION_KEY).s());
    assertEquals("sk", decoded.get().get(KvAttributes.ATTR_SORT_KEY).s());
  }

  @Test
  void decodeToken_bad_format_throws() throws Exception {
    String raw = Base64.getUrlEncoder().withoutPadding().encodeToString("onlyone".getBytes());
    assertThrows(
        IllegalArgumentException.class, () -> DynamoDbKvStore.decodeToken(Optional.of(raw)));
  }

  @Test
  void decodeToken_non_base64_throws() throws Exception {
    assertThrows(
        IllegalArgumentException.class,
        () -> DynamoDbKvStore.decodeToken(Optional.of("not-base64")));
  }

  @Test
  void queryByPartitionKeyPrefix_with_prefix_returns_only_matching_sks() {
    FakeDynamoDbHandler handler = new FakeDynamoDbHandler();
    DynamoDbKvStore store = newStore(handler);

    put(store, "pk1", "alpha", 1L);
    put(store, "pk1", "beta", 1L);
    put(store, "pk1", "a/one", 1L);

    var page =
        store.queryByPartitionKeyPrefix("pk1", "a/", 10, Optional.empty()).await().indefinitely();
    Set<String> sks = new TreeSet<>();
    page.items().forEach(r -> sks.add(r.key().sortKey()));
    assertEquals(Set.of("a/one"), sks);
  }

  @Test
  void queryByPartitionKeyPrefix_without_prefix_returns_all_in_pk() {
    FakeDynamoDbHandler handler = new FakeDynamoDbHandler();
    DynamoDbKvStore store = newStore(handler);

    put(store, "pk1", "a", 1L);
    put(store, "pk1", "b", 1L);
    put(store, "pk2", "c", 1L);

    var page =
        store.queryByPartitionKeyPrefix("pk1", null, 10, Optional.empty()).await().indefinitely();
    assertEquals(2, page.items().size());
  }

  @Test
  void queryByPartitionKeyPrefix_empty_sort_key_prefix_matches_all_in_pk() {
    FakeDynamoDbHandler handler = new FakeDynamoDbHandler();
    DynamoDbKvStore store = newStore(handler);

    put(store, "pk1", "a", 1L);
    put(store, "pk1", "b", 1L);

    var page =
        store.queryByPartitionKeyPrefix("pk1", "", 10, Optional.empty()).await().indefinitely();
    assertEquals(2, page.items().size());
  }

  @Test
  void queryByPartitionKeyPrefix_multi_page_token_works() {
    FakeDynamoDbHandler handler = new FakeDynamoDbHandler();
    DynamoDbKvStore store = newStore(handler);

    for (int i = 0; i < 5; i++) {
      put(store, "pk1", "sk" + i, 1L);
    }

    var page1 =
        store.queryByPartitionKeyPrefix("pk1", "sk", 2, Optional.empty()).await().indefinitely();
    assertEquals(2, page1.items().size());
    assertTrue(page1.nextToken().isPresent());

    var page2 =
        store.queryByPartitionKeyPrefix("pk1", "sk", 2, page1.nextToken()).await().indefinitely();
    assertEquals(2, page2.items().size());
  }

  @Test
  void queryByPartitionKeyPrefix_blank_pk_causes_scan() {
    FakeDynamoDbHandler handler = new FakeDynamoDbHandler();
    DynamoDbKvStore store = newStore(handler);

    put(store, "pk1", "sk1", 1L);
    store.queryByPartitionKeyPrefix("", "sk", 10, Optional.empty()).await().indefinitely();
    assertNotNull(handler.lastScanRequest);
    assertNull(handler.lastQueryRequest);
  }

  @Test
  void deleteByPrefix_single_page_deletes_all_items() {
    FakeDynamoDbHandler handler = new FakeDynamoDbHandler();
    DynamoDbKvStore store = newStore(handler);

    put(store, "pk1", "p/1", 1L);
    put(store, "pk1", "p/2", 1L);
    put(store, "pk1", "q/1", 1L);

    int deleted = store.deleteByPrefix("pk1", "p/").await().indefinitely();
    assertEquals(2, deleted);
    assertTrue(store.get(key("pk1", "p/1")).await().indefinitely().isEmpty());
    assertFalse(store.get(key("pk1", "q/1")).await().indefinitely().isEmpty());
  }

  @Test
  void deleteByPrefix_null_partitionKey_throws() {
    FakeDynamoDbHandler handler = new FakeDynamoDbHandler();
    DynamoDbKvStore store = newStore(handler);

    assertThrows(NullPointerException.class, () -> store.deleteByPrefix(null, "p/"));
  }

  @Test
  void deleteByPrefix_null_sortKeyPrefix_deletes_all_in_pk() {
    FakeDynamoDbHandler handler = new FakeDynamoDbHandler();
    DynamoDbKvStore store = newStore(handler);

    put(store, "pk1", "a", 1L);
    put(store, "pk1", "b", 1L);
    put(store, "pk2", "c", 1L);

    int deleted = store.deleteByPrefix("pk1", null).await().indefinitely();
    assertEquals(2, deleted);
    assertTrue(store.get(key("pk1", "a")).await().indefinitely().isEmpty());
    assertTrue(store.get(key("pk2", "c")).await().indefinitely().isPresent());
  }

  @Test
  void deleteByPrefix_multi_page_deletes_all_items() {
    FakeDynamoDbHandler handler = new FakeDynamoDbHandler();
    DynamoDbKvStore store = newStore(handler);
    int limit = DynamoDbKvStore.DELETE_BATCH_LIMIT;

    for (int i = 0; i < limit + 5; i++) {
      put(store, "pk1", "p/" + i, 1L);
    }

    int deleted = store.deleteByPrefix("pk1", "p/").await().indefinitely();
    assertEquals(limit + 5, deleted);
    assertTrue(store.isEmpty().await().indefinitely());
  }

  @Test
  void deleteByPrefix_returns_count_equal_to_deleted_items() {
    FakeDynamoDbHandler handler = new FakeDynamoDbHandler();
    DynamoDbKvStore store = newStore(handler);

    put(store, "pk1", "p/1", 1L);
    put(store, "pk1", "p/2", 1L);

    assertEquals(2, store.deleteByPrefix("pk1", "p/").await().indefinitely());
  }

  @Test
  void deleteByPrefix_no_matches_returns_0() {
    FakeDynamoDbHandler handler = new FakeDynamoDbHandler();
    DynamoDbKvStore store = newStore(handler);

    put(store, "pk1", "p/1", 1L);
    assertEquals(0, store.deleteByPrefix("pk2", "p/").await().indefinitely());
  }

  @Test
  void deleteByPrefix_handles_empty_resp_items_safely() {
    FakeDynamoDbHandler handler = new FakeDynamoDbHandler();
    handler.setQueryResponses(
        List.of(
            QueryResponse.builder()
                .items(List.of())
                .lastEvaluatedKey(
                    Map.of(
                        KvAttributes.ATTR_PARTITION_KEY,
                        AttributeValue.builder().s("pk1").build(),
                        KvAttributes.ATTR_SORT_KEY,
                        AttributeValue.builder().s("sk1").build()))
                .build(),
            QueryResponse.builder().items(List.of()).lastEvaluatedKey(Map.of()).build()));
    DynamoDbKvStore store = newStore(handler);

    int deleted = store.deleteByPrefix("pk1", "p/").await().indefinitely();
    assertEquals(0, deleted);
  }

  @Test
  void deleteByPrefix_retries_unprocessed_items_until_empty() {
    FakeDynamoDbHandler handler = new FakeDynamoDbHandler();
    DynamoDbKvStore store = newStore(handler);

    put(store, "pk1", "p/1", 1L);
    put(store, "pk1", "p/2", 1L);

    handler.setUnprocessedOnce(1);
    int deleted = store.deleteByPrefix("pk1", "p/").await().indefinitely();
    assertEquals(2, deleted);
    assertTrue(handler.batchWriteCalls > 1);
  }

  @Test
  void txnWriteCas_empty_ops_returns_true() {
    FakeDynamoDbHandler handler = new FakeDynamoDbHandler();
    DynamoDbKvStore store = newStore(handler);

    assertTrue(store.txnWriteCas(List.of()).await().indefinitely());
  }

  @Test
  void txnWriteCas_put_create_only_fails_if_exists() {
    FakeDynamoDbHandler handler = new FakeDynamoDbHandler();
    DynamoDbKvStore store = newStore(handler);

    put(store, "pk1", "sk1", 1L);

    var ops =
        List.<KvStore.TxnOp>of(
            new KvStore.TxnPut(record("pk1", "sk1", "K", "v", 1L, Map.of()), 0L));
    assertFalse(store.txnWriteCas(ops).await().indefinitely());
  }

  @Test
  void txnWriteCas_mixed_put_and_delete_all_succeed() {
    FakeDynamoDbHandler handler = new FakeDynamoDbHandler();
    DynamoDbKvStore store = newStore(handler);

    put(store, "pk1", "del", 1L);

    var ops =
        List.<KvStore.TxnOp>of(
            new KvStore.TxnDelete(key("pk1", "del"), 1L),
            new KvStore.TxnPut(record("pk1", "new", "K", "v", 1L, Map.of()), 0L));
    assertTrue(store.txnWriteCas(ops).await().indefinitely());
    assertTrue(store.get(key("pk1", "del")).await().indefinitely().isEmpty());
    assertTrue(store.get(key("pk1", "new")).await().indefinitely().isPresent());
  }

  @Test
  void txnWriteCas_returns_false_on_conditional_failure() {
    FakeDynamoDbHandler handler = new FakeDynamoDbHandler();
    DynamoDbKvStore store = newStore(handler);

    put(store, "pk1", "del", 1L);

    var ops = List.<KvStore.TxnOp>of(new KvStore.TxnDelete(key("pk1", "del"), 2L));
    assertFalse(store.txnWriteCas(ops).await().indefinitely());
  }

  @Test
  void txnWriteCas_returns_false_with_mixed_cancellation_reasons() {
    FakeDynamoDbHandler handler = new FakeDynamoDbHandler();
    handler.setCancellationReasonCodes(List.of("Other", "ConditionalCheckFailed"));
    DynamoDbKvStore store = newStore(handler);

    var ops =
        List.<KvStore.TxnOp>of(
            new KvStore.TxnPut(record("pk1", "sk1", "K", "v", 1L, Map.of()), 0L));
    assertFalse(store.txnWriteCas(ops).await().indefinitely());
  }

  @Test
  void txnWriteCas_rethrows_on_non_conditional_failure() {
    FakeDynamoDbHandler handler = new FakeDynamoDbHandler();
    handler.failTxnWithNonConditional = true;
    DynamoDbKvStore store = newStore(handler);

    var ops =
        List.<KvStore.TxnOp>of(
            new KvStore.TxnPut(record("pk1", "sk1", "K", "v", 1L, Map.of()), 0L));
    assertThrows(RuntimeException.class, () -> store.txnWriteCas(ops).await().indefinitely());
  }

  @Test
  void reset_creates_table_and_store_is_empty_after() {
    FakeDynamoDbHandler handler = new FakeDynamoDbHandler();
    DynamoDbKvStore store = newStore(handler);

    put(store, "pk1", "sk1", 1L);
    store.reset().await().indefinitely();

    assertTrue(store.isEmpty().await().indefinitely());
    assertTrue(handler.createTableCalls > 0);
  }

  @Test
  void dump_does_not_throw_when_table_missing() {
    FakeDynamoDbHandler handler = new FakeDynamoDbHandler();
    handler.failScan = true;
    DynamoDbKvStore store = newStore(handler);

    store.dump("missing").await().indefinitely();
  }

  private static DynamoDbKvStore newStore(FakeDynamoDbHandler handler) {
    DynamoDbAsyncClient client =
        (DynamoDbAsyncClient)
            Proxy.newProxyInstance(
                DynamoDbAsyncClient.class.getClassLoader(),
                new Class<?>[] {DynamoDbAsyncClient.class},
                handler);
    return new DynamoDbKvStore(client, handler.tableName);
  }

  private static void put(DynamoDbKvStore store, String pk, String sk, long version) {
    KvStore.Record record = record(pk, sk, "K", "v", version, Map.of());
    assertTrue(store.putCas(record, 0L).await().indefinitely());
  }

  private static KvStore.Key key(String pk, String sk) {
    return new KvStore.Key(pk, sk);
  }

  private static KvStore.Record record(
      String pk, String sk, String kind, String value, long version, Map<String, String> attrs) {
    return new KvStore.Record(
        new KvStore.Key(pk, sk), kind, value.getBytes(StandardCharsets.UTF_8), attrs, version);
  }

  private static final class FakeDynamoDbHandler implements InvocationHandler {
    private final Map<String, Map<String, AttributeValue>> items = new ConcurrentHashMap<>();
    private final AtomicBoolean unprocessedUsed = new AtomicBoolean(false);
    private final String tableName = "test-table";
    private final Deque<QueryResponse> queryResponses = new ArrayDeque<>();
    private PutItemRequest lastPutRequest;
    private DeleteItemRequest lastDeleteRequest;
    private QueryRequest lastQueryRequest;
    private ScanRequest lastScanRequest;
    private int batchWriteCalls;
    private boolean tableExists = true;
    private boolean tableActive = true;
    private boolean failScan;
    private boolean failTxnWithNonConditional;
    private List<String> cancellationReasonCodes;
    private int createTableCalls;
    private int deleteTableCalls;
    private int unprocessedCount;

    private void setQueryResponses(List<QueryResponse> responses) {
      queryResponses.clear();
      queryResponses.addAll(responses);
    }

    private void setUnprocessedOnce(int count) {
      this.unprocessedCount = count;
      this.unprocessedUsed.set(false);
    }

    private void setCancellationReasonCodes(List<String> codes) {
      this.cancellationReasonCodes = codes;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) {
      String name = method.getName();
      if (name.equals("putItem") && args.length == 1 && args[0] instanceof PutItemRequest req) {
        return handlePutItem(req);
      }
      if (name.equals("deleteItem")
          && args.length == 1
          && args[0] instanceof DeleteItemRequest req) {
        return handleDeleteItem(req);
      }
      if (name.equals("getItem") && args.length == 1 && args[0] instanceof GetItemRequest req) {
        return handleGetItem(req);
      }
      if (name.equals("query") && args.length == 1 && args[0] instanceof QueryRequest req) {
        return handleQuery(req);
      }
      if (name.equals("scan") && args.length == 1 && args[0] instanceof ScanRequest req) {
        return handleScan(req);
      }
      if (name.equals("batchWriteItem")
          && args.length == 1
          && args[0] instanceof BatchWriteItemRequest req) {
        return handleBatchWrite(req);
      }
      if (name.equals("transactWriteItems")
          && args.length == 1
          && args[0] instanceof TransactWriteItemsRequest req) {
        return handleTransactWrite(req);
      }
      if (name.equals("deleteTable") && args.length == 1 && args[0] instanceof DeleteTableRequest) {
        return handleDeleteTable();
      }
      if (name.equals("createTable") && args.length == 1 && args[0] instanceof CreateTableRequest) {
        return handleCreateTable();
      }
      if (name.equals("describeTable")
          && args.length == 1
          && args[0] instanceof DescribeTableRequest) {
        return handleDescribeTable();
      }
      if (name.equals("close")) {
        return null;
      }
      throw new UnsupportedOperationException("Unhandled method: " + name);
    }

    private CompletableFuture<PutItemResponse> handlePutItem(PutItemRequest req) {
      this.lastPutRequest = req;
      String key = keyFromItem(req.item());
      Map<String, AttributeValue> existing = items.get(key);
      String condition = req.conditionExpression();
      if ("attribute_not_exists(pk)".equals(condition)) {
        if (existing != null) {
          return failedConditional();
        }
      } else if ("#v = :ev".equals(condition)) {
        long expected = Long.parseLong(req.expressionAttributeValues().get(":ev").n());
        if (existing == null || versionOf(existing) != expected) {
          return failedConditional();
        }
      }
      items.put(key, Map.copyOf(req.item()));
      return CompletableFuture.completedFuture(PutItemResponse.builder().build());
    }

    private CompletableFuture<DeleteItemResponse> handleDeleteItem(DeleteItemRequest req) {
      this.lastDeleteRequest = req;
      String key = keyFromKey(req.key());
      Map<String, AttributeValue> existing = items.get(key);
      long expected = Long.parseLong(req.expressionAttributeValues().get(":ev").n());
      if (existing == null || versionOf(existing) != expected) {
        return failedConditional();
      }
      items.remove(key);
      return CompletableFuture.completedFuture(DeleteItemResponse.builder().build());
    }

    private CompletableFuture<GetItemResponse> handleGetItem(GetItemRequest req) {
      String key = keyFromKey(req.key());
      Map<String, AttributeValue> item = items.get(key);
      if (item == null) {
        return CompletableFuture.completedFuture(GetItemResponse.builder().build());
      }
      return CompletableFuture.completedFuture(GetItemResponse.builder().item(item).build());
    }

    private CompletableFuture<QueryResponse> handleQuery(QueryRequest req) {
      this.lastQueryRequest = req;
      if (!queryResponses.isEmpty()) {
        return CompletableFuture.completedFuture(queryResponses.removeFirst());
      }

      String pk = req.expressionAttributeValues().get(":pk").s();
      String skPrefix =
          req.expressionAttributeValues().containsKey(":skp")
              ? req.expressionAttributeValues().get(":skp").s()
              : null;

      List<Map<String, AttributeValue>> all = new ArrayList<>();
      for (Map<String, AttributeValue> item : items.values()) {
        if (!Objects.equals(pk, item.get(KvAttributes.ATTR_PARTITION_KEY).s())) {
          continue;
        }
        if (skPrefix != null && !item.get(KvAttributes.ATTR_SORT_KEY).s().startsWith(skPrefix)) {
          continue;
        }
        all.add(item);
      }
      all.sort(Comparator.comparing(a -> a.get(KvAttributes.ATTR_SORT_KEY).s()));

      int start = 0;
      if (req.exclusiveStartKey() != null && !req.exclusiveStartKey().isEmpty()) {
        String startKey = keyFromKey(req.exclusiveStartKey());
        start = indexAfter(all, startKey);
      }

      int limit = req.limit() == null ? all.size() : req.limit();
      int end = Math.min(all.size(), start + limit);
      List<Map<String, AttributeValue>> page = new ArrayList<>(all.subList(start, end));
      Map<String, AttributeValue> lek =
          end < all.size() ? keyMapFromItem(all.get(end - 1)) : Map.of();

      return CompletableFuture.completedFuture(
          QueryResponse.builder().items(page).lastEvaluatedKey(lek).build());
    }

    private CompletableFuture<ScanResponse> handleScan(ScanRequest req) {
      this.lastScanRequest = req;
      if (failScan) {
        CompletableFuture<ScanResponse> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("scan failed"));
        return failed;
      }

      String skPrefix = null;
      if (req.expressionAttributeValues() != null
          && req.expressionAttributeValues().containsKey(":skp")) {
        skPrefix = req.expressionAttributeValues().get(":skp").s();
      }

      List<Map<String, AttributeValue>> all = new ArrayList<>();
      for (Map<String, AttributeValue> item : items.values()) {
        if (skPrefix != null && !item.get(KvAttributes.ATTR_SORT_KEY).s().startsWith(skPrefix)) {
          continue;
        }
        all.add(item);
      }
      all.sort(
          Comparator.comparing(
                  (Map<String, AttributeValue> a) -> a.get(KvAttributes.ATTR_PARTITION_KEY).s())
              .thenComparing(a -> a.get(KvAttributes.ATTR_SORT_KEY).s()));

      int start = 0;
      if (req.exclusiveStartKey() != null && !req.exclusiveStartKey().isEmpty()) {
        String startKey = keyFromKey(req.exclusiveStartKey());
        start = indexAfter(all, startKey);
      }

      int limit = req.limit() == null ? all.size() : req.limit();
      int end = Math.min(all.size(), start + limit);
      List<Map<String, AttributeValue>> page = new ArrayList<>(all.subList(start, end));
      Map<String, AttributeValue> lek =
          end < all.size() ? keyMapFromItem(all.get(end - 1)) : Map.of();

      return CompletableFuture.completedFuture(
          ScanResponse.builder().items(page).lastEvaluatedKey(lek).build());
    }

    private CompletableFuture<BatchWriteItemResponse> handleBatchWrite(BatchWriteItemRequest req) {
      batchWriteCalls++;
      List<WriteRequest> writes =
          req.requestItems().values().stream().findFirst().orElse(List.of());
      List<WriteRequest> unprocessed = List.of();
      if (unprocessedCount > 0 && unprocessedUsed.compareAndSet(false, true)) {
        unprocessed = new ArrayList<>(writes.subList(0, Math.min(unprocessedCount, writes.size())));
      }

      for (WriteRequest wr : writes) {
        if (!unprocessed.contains(wr) && wr.deleteRequest() != null) {
          String key = keyFromKey(wr.deleteRequest().key());
          items.remove(key);
        }
      }

      Map<String, List<WriteRequest>> unprocessedMap =
          unprocessed.isEmpty() ? Map.of() : Map.of(tableName, unprocessed);

      return CompletableFuture.completedFuture(
          BatchWriteItemResponse.builder().unprocessedItems(unprocessedMap).build());
    }

    private CompletableFuture<TransactWriteItemsResponse> handleTransactWrite(
        TransactWriteItemsRequest req) {
      if (cancellationReasonCodes != null) {
        return failedTransaction(cancellationReasonCodes);
      }
      if (failTxnWithNonConditional) {
        return failedTransaction("Other");
      }

      for (TransactWriteItem item : req.transactItems()) {
        if (item.put() != null) {
          String key = keyFromItem(item.put().item());
          Map<String, AttributeValue> existing = items.get(key);
          String condition = item.put().conditionExpression();
          if ("attribute_not_exists(pk)".equals(condition)) {
            if (existing != null) {
              return failedTransaction("ConditionalCheckFailed");
            }
          } else if ("#v = :ev".equals(condition)) {
            long expected = Long.parseLong(item.put().expressionAttributeValues().get(":ev").n());
            if (existing == null || versionOf(existing) != expected) {
              return failedTransaction("ConditionalCheckFailed");
            }
          }
        } else if (item.delete() != null) {
          String key = keyFromKey(item.delete().key());
          Map<String, AttributeValue> existing = items.get(key);
          long expected = Long.parseLong(item.delete().expressionAttributeValues().get(":ev").n());
          if (existing == null || versionOf(existing) != expected) {
            return failedTransaction("ConditionalCheckFailed");
          }
        }
      }

      for (TransactWriteItem item : req.transactItems()) {
        if (item.put() != null) {
          String key = keyFromItem(item.put().item());
          items.put(key, Map.copyOf(item.put().item()));
        } else if (item.delete() != null) {
          String key = keyFromKey(item.delete().key());
          items.remove(key);
        }
      }

      return CompletableFuture.completedFuture(TransactWriteItemsResponse.builder().build());
    }

    private CompletableFuture<DeleteTableResponse> handleDeleteTable() {
      deleteTableCalls++;
      items.clear();
      tableExists = false;
      return CompletableFuture.completedFuture(DeleteTableResponse.builder().build());
    }

    private CompletableFuture<CreateTableResponse> handleCreateTable() {
      createTableCalls++;
      tableExists = true;
      tableActive = true;
      return CompletableFuture.completedFuture(CreateTableResponse.builder().build());
    }

    private CompletableFuture<DescribeTableResponse> handleDescribeTable() {
      if (!tableExists) {
        CompletableFuture<DescribeTableResponse> failed = new CompletableFuture<>();
        failed.completeExceptionally(
            ResourceNotFoundException.builder().message("missing").build());
        return failed;
      }
      TableStatus status = tableActive ? TableStatus.ACTIVE : TableStatus.CREATING;
      return CompletableFuture.completedFuture(
          DescribeTableResponse.builder()
              .table(TableDescription.builder().tableStatus(status).build())
              .build());
    }

    private static String keyFromItem(Map<String, AttributeValue> item) {
      return item.get(KvAttributes.ATTR_PARTITION_KEY).s()
          + "|"
          + item.get(KvAttributes.ATTR_SORT_KEY).s();
    }

    private static String keyFromKey(Map<String, AttributeValue> key) {
      return key.get(KvAttributes.ATTR_PARTITION_KEY).s()
          + "|"
          + key.get(KvAttributes.ATTR_SORT_KEY).s();
    }

    private static Map<String, AttributeValue> keyMapFromItem(Map<String, AttributeValue> item) {
      return Map.of(
          KvAttributes.ATTR_PARTITION_KEY,
          AttributeValue.builder().s(item.get(KvAttributes.ATTR_PARTITION_KEY).s()).build(),
          KvAttributes.ATTR_SORT_KEY,
          AttributeValue.builder().s(item.get(KvAttributes.ATTR_SORT_KEY).s()).build());
    }

    private static long versionOf(Map<String, AttributeValue> item) {
      return Long.parseLong(item.get(KvAttributes.ATTR_VERSION).n());
    }

    private static int indexAfter(List<Map<String, AttributeValue>> items, String key) {
      for (int i = 0; i < items.size(); i++) {
        if (key.equals(keyFromItem(items.get(i)))) {
          return i + 1;
        }
      }
      return 0;
    }

    private static <T> CompletableFuture<T> failedConditional() {
      CompletableFuture<T> failed = new CompletableFuture<>();
      failed.completeExceptionally(
          ConditionalCheckFailedException.builder().message("conditional failed").build());
      return failed;
    }

    private static <T> CompletableFuture<T> failedTransaction(String code) {
      return failedTransaction(List.of(code));
    }

    private static <T> CompletableFuture<T> failedTransaction(List<String> codes) {
      CompletableFuture<T> failed = new CompletableFuture<>();
      List<CancellationReason> reasons = new ArrayList<>(codes.size());
      for (String code : codes) {
        reasons.add(CancellationReason.builder().code(code).build());
      }
      failed.completeExceptionally(
          TransactionCanceledException.builder()
              .message("tx failed")
              .cancellationReasons(reasons)
              .build());
      return failed;
    }
  }
}
