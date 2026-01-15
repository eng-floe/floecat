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

import ai.floedb.floecat.storage.kv.KvAttributes;
import ai.floedb.floecat.storage.kv.KvStore;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;

public final class DynamoDbKvStore implements KvStore, KvAttributes {
  static final int DELETE_BATCH_LIMIT = Integer.getInteger("floedb.floecat.delete.batch.size", 25);
  private final DynamoDbAsyncClient ddb;
  private final String table;

  public DynamoDbKvStore(DynamoDbAsyncClient ddb, String table) {
    this.ddb = ddb;
    this.table = table;
  }

  String getTable() {
    return this.table;
  }

  // ---- Mapping helpers (private to Dynamo)

  private static AttributeValue S(String v) {
    return AttributeValue.fromS(v);
  }

  private static AttributeValue N(long v) {
    return AttributeValue.fromN(Long.toString(v));
  }

  private static AttributeValue B(byte[] v) {
    return AttributeValue.fromB(SdkBytes.fromByteArray(v));
  }

  private static Map<String, AttributeValue> keyMap(Key k) {
    return Map.of(ATTR_PARTITION_KEY, S(k.partitionKey()), ATTR_SORT_KEY, S(k.sortKey()));
  }

  private static Map<String, AttributeValue> attrsToAv(Map<String, String> attrs) {
    if (attrs == null || attrs.isEmpty()) return Map.of();
    var out = new HashMap<String, AttributeValue>(attrs.size());
    for (var e : attrs.entrySet()) {
      out.put(e.getKey(), S(e.getValue()));
    }
    return out;
  }

  private static Map<String, String> avToAttrs(Map<String, AttributeValue> item) {
    var out = new HashMap<String, String>();
    for (var e : item.entrySet()) {
      var k = e.getKey();
      if (k.equals(ATTR_PARTITION_KEY)
          || k.equals(ATTR_SORT_KEY)
          || k.equals(ATTR_KIND)
          || k.equals(ATTR_VALUE)
          || k.equals(ATTR_VERSION)) continue;
      var v = e.getValue();
      if (v.s() != null) out.put(k, v.s());
    }
    return out;
  }

  private static long avToVersion(Map<String, AttributeValue> item) {
    var v = item.get(ATTR_VERSION);
    if (v == null || v.n() == null) return 0L;
    try {
      return Long.parseLong(v.n());
    } catch (NumberFormatException e) {
      return 0L;
    }
  }

  private static Record avToRecord(Map<String, AttributeValue> item) {
    var pk = item.get(ATTR_PARTITION_KEY).s();
    var sk = item.get(ATTR_SORT_KEY).s();
    var kind = item.containsKey(ATTR_KIND) ? item.get(ATTR_KIND).s() : "";
    var value =
        item.containsKey(ATTR_VALUE) && item.get(ATTR_VALUE).b() != null
            ? item.get(ATTR_VALUE).b().asByteArray()
            : new byte[0];
    var ver = avToVersion(item);
    return new Record(new Key(pk, sk), kind, value, avToAttrs(item), ver);
  }

  private Map<String, AttributeValue> recordToAv(Record r) {
    var item = new HashMap<String, AttributeValue>();
    item.put(ATTR_PARTITION_KEY, S(r.key().partitionKey()));
    item.put(ATTR_SORT_KEY, S(r.key().sortKey()));
    item.put(ATTR_KIND, S(r.kind()));
    var value = r.value();
    if (value != null && value.length > 0) {
      item.put(ATTR_VALUE, B(r.value()));
    }
    item.put(ATTR_VERSION, N(r.version()));
    item.putAll(attrsToAv(r.attrs()));
    return item;
  }

  // ---- Paging token (pk+sk only)

  static Optional<String> encodeToken(Map<String, AttributeValue> lek) {
    if (lek == null || lek.isEmpty()) return Optional.empty();
    var raw = lek.get(ATTR_PARTITION_KEY).s() + "\n" + lek.get(ATTR_SORT_KEY).s();
    return Optional.of(
        Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(raw.getBytes(StandardCharsets.UTF_8)));
  }

  static Optional<Map<String, AttributeValue>> decodeToken(Optional<String> token) {
    if (token.isEmpty()) return Optional.empty();
    var raw = new String(Base64.getUrlDecoder().decode(token.get()), StandardCharsets.UTF_8);
    var parts = raw.split("\n", 2);
    if (parts.length != 2) throw new IllegalArgumentException("Bad page token");
    return Optional.of(Map.of(ATTR_PARTITION_KEY, S(parts[0]), ATTR_SORT_KEY, S(parts[1])));
  }

  // ---- KvStore (reads)

  @Override
  public Uni<Optional<Record>> get(Key key) {
    var req =
        GetItemRequest.builder().tableName(table).key(keyMap(key)).consistentRead(true).build();

    return Uni.createFrom()
        .completionStage(ddb.getItem(req))
        .map(resp -> resp.hasItem() ? Optional.of(avToRecord(resp.item())) : Optional.empty());
  }

  // ---- KvStore (CAS writes)

  @Override
  public Uni<Boolean> putCas(Record record, long expectedVersion) {
    if (record.version() <= 0) {
      throw new IllegalArgumentException("record.version must be > 0 for CAS put");
    }
    if (expectedVersion < 0) {
      throw new IllegalArgumentException("expectedVersion must be >= 0");
    }

    PutItemRequest.Builder b = PutItemRequest.builder().tableName(table).item(recordToAv(record));

    if (expectedVersion == 0L) {
      // create-only
      b.conditionExpression("attribute_not_exists(pk)");
    } else {
      // update-only if ver matches
      b.conditionExpression("#v = :ev")
          .expressionAttributeNames(Map.of("#v", ATTR_VERSION))
          .expressionAttributeValues(Map.of(":ev", N(expectedVersion)));
    }

    return Uni.createFrom()
        .completionStage(ddb.putItem(b.build()))
        .replaceWith(true)
        .onFailure(ConditionalCheckFailedException.class)
        .recoverWithItem(false);
  }

  @Override
  public Uni<Boolean> deleteCas(Key key, long expectedVersion) {
    if (expectedVersion <= 0) {
      throw new IllegalArgumentException("expectedVersion must be > 0 for CAS delete");
    }

    var req =
        DeleteItemRequest.builder()
            .tableName(table)
            .key(keyMap(key))
            .conditionExpression("#v = :ev")
            .expressionAttributeNames(Map.of("#v", ATTR_VERSION))
            .expressionAttributeValues(Map.of(":ev", N(expectedVersion)))
            .build();

    return Uni.createFrom()
        .completionStage(ddb.deleteItem(req))
        .replaceWith(true)
        .onFailure(ConditionalCheckFailedException.class)
        .recoverWithItem(false);
  }

  // ---- Query

  @Override
  public Uni<Page> queryByPartitionKeyPrefix(
      String pk, String skPrefix, int limit, Optional<String> pageToken) {

    // If we are needing to do a scan here?
    // NB: a scan is not intended to be part of the api used by normal clients.
    // Scans are currently used by integration tests to assert various conditions.
    if (pk == null || pk.isEmpty()) {
      var sb = ScanRequest.builder().tableName(table).limit(limit);

      if (skPrefix != null && !skPrefix.isEmpty()) {
        sb.expressionAttributeNames(Map.of("#sk", ATTR_SORT_KEY))
            .filterExpression("begins_with(#sk, :skp)")
            .expressionAttributeValues(Map.of(":skp", S(skPrefix)));
      }

      decodeToken(pageToken).ifPresent(sb::exclusiveStartKey);

      return Uni.createFrom()
          .completionStage(ddb.scan(sb.build()))
          .map(
              resp -> {
                var items = new ArrayList<Record>(resp.items().size());
                for (var it : resp.items()) items.add(avToRecord(it));
                return new Page(items, encodeToken(resp.lastEvaluatedKey()));
              });
    } else {
      var qb = QueryRequest.builder().tableName(table).limit(limit);

      if (skPrefix == null || skPrefix.isEmpty()) {
        qb.expressionAttributeNames(Map.of("#pk", ATTR_PARTITION_KEY))
            .keyConditionExpression("#pk = :pk")
            .expressionAttributeValues(Map.of(":pk", S(pk)));
      } else {
        qb.expressionAttributeNames(Map.of("#pk", ATTR_PARTITION_KEY, "#sk", ATTR_SORT_KEY))
            .keyConditionExpression("#pk = :pk AND begins_with(#sk, :skp)")
            .expressionAttributeValues(
                Map.of(
                    ":pk", S(pk),
                    ":skp", S(skPrefix)));
      }

      decodeToken(pageToken).ifPresent(qb::exclusiveStartKey);

      return Uni.createFrom()
          .completionStage(ddb.query(qb.build()))
          .map(
              resp -> {
                var items = new ArrayList<Record>(resp.items().size());
                for (var it : resp.items()) items.add(avToRecord(it));
                return new Page(items, encodeToken(resp.lastEvaluatedKey()));
              });
    }
  }

  @Override
  public Uni<Integer> deleteByPrefix(String partitionKey, String sortKeyPrefix) {
    Objects.requireNonNull(partitionKey, "Partition must be provided for delete by prefix");
    final var totalDeleted = new AtomicInteger(0);
    final var lekRef = new AtomicReference<Map<String, AttributeValue>>(null);

    return Multi.createBy()
        .repeating()
        .uni(
            () -> {
              QueryRequest.Builder qb =
                  QueryRequest.builder()
                      .tableName(table)
                      .projectionExpression("#pk,#sk")
                      .expressionAttributeNames(
                          Map.of("#pk", ATTR_PARTITION_KEY, "#sk", ATTR_SORT_KEY))
                      .consistentRead(true)
                      .limit(DELETE_BATCH_LIMIT);

              var lek = lekRef.get();
              if (lek != null && !lek.isEmpty()) {
                qb.exclusiveStartKey(lek);
              }

              if (sortKeyPrefix == null || sortKeyPrefix.isEmpty()) {
                qb.keyConditionExpression("#pk = :pk")
                    .expressionAttributeValues(Map.of(":pk", S(partitionKey)));
              } else {
                qb.keyConditionExpression("#pk = :pk AND begins_with(#sk, :skp)")
                    .expressionAttributeValues(
                        Map.of(
                            ":pk", S(partitionKey),
                            ":skp", S(sortKeyPrefix)));
              }

              return Uni.createFrom().completionStage(ddb.query(qb.build()));
            })
        .whilst(resp -> resp.lastEvaluatedKey() != null && !resp.lastEvaluatedKey().isEmpty())
        .onItem()
        .call(
            resp -> {
              // advance LEK for the next iteration
              lekRef.set(resp.lastEvaluatedKey());

              // build delete requests for this page
              var deletes = new ArrayList<WriteRequest>(resp.items().size());
              for (var item : resp.items()) {
                var key =
                    Map.of(
                        ATTR_PARTITION_KEY, item.get(ATTR_PARTITION_KEY),
                        ATTR_SORT_KEY, item.get(ATTR_SORT_KEY));
                deletes.add(
                    WriteRequest.builder()
                        .deleteRequest(DeleteRequest.builder().key(key).build())
                        .build());
              }

              return deleteBatchWithRetry(deletes, 0)
                  .invoke(deleted -> totalDeleted.addAndGet(deleted))
                  .replaceWithVoid();
            })
        .collect()
        .asList()
        .replaceWith(totalDeleted::get);
  }

  private Uni<Integer> deleteBatchWithRetry(List<WriteRequest> batch, int attempt) {
    // Batch deletes can't be empty, so handle this case.
    if (batch.isEmpty()) {
      return Uni.createFrom().item(0);
    }

    // Attempt to delete this batch.
    return Uni.createFrom()
        .completionStage(
            ddb.batchWriteItem(
                BatchWriteItemRequest.builder().requestItems(Map.of(table, batch)).build()))
        .onItem()
        .transformToUni(
            resp -> {

              // Determine the unprocessed items, to send as subsequent batch.
              var unprocessed =
                  resp.unprocessedItems() == null
                      ? List.<WriteRequest>of()
                      : resp.unprocessedItems().getOrDefault(table, List.of());

              int processedThisAttempt = batch.size() - unprocessed.size();

              if (unprocessed.isEmpty()) {
                return Uni.createFrom().item(processedThisAttempt);
              }

              // Figure out a backoff delay (jitter), in case the batch write is rate-limiting.
              long baseMs = 25L;
              long maxMs = 1000L;
              long expMs = Math.min(maxMs, baseMs * (1L << Math.min(attempt, 6)));
              long jitterMs = ThreadLocalRandom.current().nextLong(0, expMs + 1);

              return Uni.createFrom()
                  .voidItem()
                  .onItem()
                  .delayIt()
                  .by(Duration.ofMillis(jitterMs))
                  .replaceWith(unprocessed)
                  .onItem()
                  .transformToUni(next -> deleteBatchWithRetry(next, attempt + 1))
                  .onItem()
                  .transform(nextProcessed -> processedThisAttempt + nextProcessed);
            });
  }

  // ---- Transactions (CAS-only)

  @Override
  public Uni<Boolean> txnWriteCas(List<TxnOp> ops) {
    if (ops == null || ops.isEmpty()) return Uni.createFrom().item(true);

    var tx = new ArrayList<TransactWriteItem>(ops.size());

    for (var op : ops) {
      if (op instanceof TxnPut p) {
        var put = Put.builder().tableName(table).item(recordToAv(p.record()));

        if (p.expectedVersion() == 0L) {
          put.conditionExpression("attribute_not_exists(pk)");
        } else {
          put.conditionExpression("#v = :ev")
              .expressionAttributeNames(Map.of("#v", ATTR_VERSION))
              .expressionAttributeValues(Map.of(":ev", N(p.expectedVersion())));
        }

        tx.add(TransactWriteItem.builder().put(put.build()).build());

      } else if (op instanceof TxnDelete d) {
        var del =
            Delete.builder()
                .tableName(table)
                .key(keyMap(d.key()))
                .conditionExpression("#v = :ev")
                .expressionAttributeNames(Map.of("#v", ATTR_VERSION))
                .expressionAttributeValues(Map.of(":ev", N(d.expectedVersion())))
                .build();

        tx.add(TransactWriteItem.builder().delete(del).build());
      }
    }

    var req = TransactWriteItemsRequest.builder().transactItems(tx).build();

    return Uni.createFrom()
        .completionStage(ddb.transactWriteItems(req))
        .replaceWith(true)
        .onFailure(TransactionCanceledException.class)
        .recoverWithItem(
            t -> {
              // Return false only for conditional check failure; otherwise rethrow.
              TransactionCanceledException ex = (TransactionCanceledException) t;
              if (ex.cancellationReasons() != null) {
                for (var r : ex.cancellationReasons()) {
                  if (r != null && "ConditionalCheckFailed".equals(r.code())) {
                    return false;
                  }
                }
              }
              throw ex;
            });
  }

  // ---- Test helpers

  @Override
  public Uni<Void> reset() {
    return Uni.createFrom().completionStage(CompletableFuture.runAsync(this::resetTableIfExists));
  }

  @Override
  public Uni<Boolean> isEmpty() {
    var req =
        ScanRequest.builder()
            .tableName(this.table)
            .limit(1) // only need to find one item
            .build();

    return Uni.createFrom().completionStage(ddb.scan(req).thenApply(r -> r.items().isEmpty()));
  }

  @Override
  public Uni<Void> dump(String header) {
    return Uni.createFrom()
        .completionStage(CompletableFuture.runAsync(() -> listTableIfExists(header)));
  }

  void resetTableIfExists() {
    dropTableIfExists(this.table);
    createKvTable(this.table);
    waitUntilActive(this.table, Duration.ofSeconds(10));
  }

  void listTableIfExists(String header) {
    try {
      var req =
          ScanRequest.builder()
              .tableName(this.table)
              .limit(100) // keep logs sane
              .build();

      var resp = ddb.scan(req).get();

      System.out.println("\n=== DUMP TABLE: " + this.table + " " + header + " ===");

      if (resp.items().isEmpty()) {
        System.out.println("(empty)");
        return;
      }

      int i = 0;
      for (Map<String, AttributeValue> item : resp.items()) {
        System.out.printf("[%03d] %s%n", i++, pretty(item));
      }

      System.out.println("=== END TABLE DUMP ===\n");

    } catch (ExecutionException e) {
      // Table probably does not exist — safe to ignore in tests
      System.out.println("Table not found: " + this.table);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private static String pretty(Map<String, AttributeValue> item) {
    StringBuilder sb = new StringBuilder("{");
    boolean first = true;

    for (var e : item.entrySet()) {
      if (!first) sb.append(", ");
      first = false;

      sb.append(e.getKey()).append("=");

      var v = e.getValue();
      if (v.s() != null) sb.append('"').append(v.s()).append('"');
      else if (v.b() != null) sb.append(String.format("<binary %d>", v.b().asByteArray().length));
      else if (v.hasL()) sb.append(v.l());
      else if (v.hasM()) sb.append(v.m());
      else if (v.hasBs()) sb.append("<binary-set>");
      else if (v.hasNs()) sb.append(v.ns());
      else if (v.n() != null) sb.append(v.n());
      else if (v.bool() != null) sb.append(v.bool());
      else if (v.hasSs()) sb.append(v.ss());
      else sb.append(v);
    }

    sb.append("}");
    return sb.toString();
  }

  private void dropTableIfExists(String tableName) {
    try {
      ddb.deleteTable(DeleteTableRequest.builder().tableName(tableName).build()).join();
      // Wait until it's really gone (Local is fast, AWS would be slower)
      for (int i = 0; i < 50; i++) {
        if (!tableExists(tableName)) return;
        sleep(50);
      }
    } catch (Throwable t) {
      // ignore ResourceNotFound
    }
  }

  private void createKvTable(String tableName) {
    var req =
        CreateTableRequest.builder()
            .tableName(tableName)
            .billingMode(BillingMode.PAY_PER_REQUEST)
            .attributeDefinitions(
                AttributeDefinition.builder()
                    .attributeName(ATTR_PARTITION_KEY)
                    .attributeType(ScalarAttributeType.S)
                    .build(),
                AttributeDefinition.builder()
                    .attributeName(ATTR_SORT_KEY)
                    .attributeType(ScalarAttributeType.S)
                    .build())
            .keySchema(
                KeySchemaElement.builder()
                    .attributeName(ATTR_PARTITION_KEY)
                    .keyType(KeyType.HASH)
                    .build(),
                KeySchemaElement.builder()
                    .attributeName(ATTR_SORT_KEY)
                    .keyType(KeyType.RANGE)
                    .build())
            .build();

    ddb.createTable(req).join();
  }

  private boolean tableExists(String tableName) {
    try {
      ddb.describeTable(DescribeTableRequest.builder().tableName(tableName).build()).join();
      return true;
    } catch (Throwable t) {
      return false;
    }
  }

  private void waitUntilActive(String tableName, Duration timeout) {
    long deadline = System.currentTimeMillis() + timeout.toMillis();
    while (System.currentTimeMillis() < deadline) {
      try {
        var resp =
            ddb.describeTable(DescribeTableRequest.builder().tableName(tableName).build()).join();
        if (resp.table().tableStatus() == TableStatus.ACTIVE) return;
      } catch (Throwable ignored) {
      }
      sleep(50);
    }
    throw new IllegalStateException("Table did not become ACTIVE: " + tableName);
  }

  private void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(ie);
    }
  }
}
