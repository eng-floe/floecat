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

import ai.floedb.floecat.storage.errors.StorageAbortRetryableException;
import ai.floedb.floecat.storage.kv.AttrValue;
import ai.floedb.floecat.storage.kv.AttrWriteRules;
import ai.floedb.floecat.storage.kv.KvAttributes;
import ai.floedb.floecat.storage.kv.KvStore;
import ai.floedb.floecat.storage.kv.MetadataAttrUpdates;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;

public final class DynamoDbKvStore implements KvStore, KvAttributes {
  static final int DELETE_BATCH_LIMIT = Integer.getInteger("floedb.floecat.delete.batch.size", 25);
  private final AsyncDynamoCaller ddb;
  private final BlockingDynamoCaller blockingDdb;
  private final String table;

  public DynamoDbKvStore(DynamoDbAsyncClient ddb, String table) {
    this(
        new AsyncDynamoCaller() {
          @Override
          public <T> CompletionStage<T> call(
              Function<DynamoDbAsyncClient, CompletionStage<T>> operation) {
            return operation.apply(ddb);
          }
        },
        new BlockingDynamoCaller() {
          @Override
          public <T> T call(Function<DynamoDbAsyncClient, T> operation) {
            return operation.apply(ddb);
          }
        },
        table);
  }

  public DynamoDbKvStore(AsyncDynamoCaller ddb, BlockingDynamoCaller blockingDdb, String table) {
    this.ddb = ddb;
    this.blockingDdb = blockingDdb;
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

  /** A typed attr as its native DynamoDB type; {@code N} is what makes atomic ADD possible. */
  private static AttributeValue attrToAv(AttrValue v) {
    return switch (v) {
      case AttrValue.StringValue s -> S(s.value());
      case AttrValue.NumberValue n -> N(n.value());
    };
  }

  private static Map<String, AttributeValue> attrsToAv(Map<String, AttrValue> attrs) {
    if (attrs == null || attrs.isEmpty()) return Map.of();
    var out = new HashMap<String, AttributeValue>(attrs.size());
    for (var e : attrs.entrySet()) {
      out.put(e.getKey(), attrToAv(e.getValue()));
    }
    return out;
  }

  private static Map<String, AttrValue> avToAttrs(Map<String, AttributeValue> item) {
    var out = new HashMap<String, AttrValue>();
    for (var e : item.entrySet()) {
      var k = e.getKey();
      // Record's constructor refuses reserved names, but other writers do put them on rows;
      // dropping them keeps such rows readable.
      if (KvAttributes.RESERVED_ATTRS.contains(k)) continue;
      var v = e.getValue();
      if (v.s() != null) {
        out.put(k, AttrValue.of(v.s()));
      } else if (v.n() != null) {
        try {
          out.put(k, AttrValue.of(Long.parseLong(v.n())));
        } catch (NumberFormatException ex) {
          // N is wider than long and admits fractions; a value that does not fit degrades to its
          // decimal text rather than vanishing from the record.
          out.put(k, AttrValue.of(v.n()));
        }
      }
      // B/BOOL/M/L have no AttrValue representation and are dropped.
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
    // Every whole-record write (putCas and txnWriteCas) serializes here, before the request is
    // built.
    AttrWriteRules.checkExpiryIsString(r.attrs());
    var item = new HashMap<String, AttributeValue>();
    // Attrs first, so the structural fields win if a reserved name ever slips past Record's
    // constructor. ATTR_TTL is never written here, so the constructor is its only guard.
    item.putAll(attrsToAv(r.attrs()));
    item.put(ATTR_PARTITION_KEY, S(r.key().partitionKey()));
    item.put(ATTR_SORT_KEY, S(r.key().sortKey()));
    item.put(ATTR_KIND, S(r.kind()));
    var value = r.value();
    if (value != null && value.length > 0) {
      item.put(ATTR_VALUE, B(r.value()));
    }
    item.put(ATTR_VERSION, N(r.version()));
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

  @Override
  public String pageTokenAfterKey(Key key) {
    // Tokens are the (pk, sk) position encoded exactly as lastEvaluatedKey tokens are; DynamoDB's
    // exclusiveStartKey is a position, not an item reference, so the key need not still exist.
    return encodeToken(keyMap(key)).orElseThrow();
  }

  private static <T> Uni<T> fromStage(CompletionStage<T> stage) {
    return Uni.createFrom()
        .emitter(
            emitter -> {
              stage.whenComplete(
                  (item, failure) -> {
                    if (failure != null) {
                      emitter.fail(unwrapStageFailure(failure));
                    } else {
                      emitter.complete(item);
                    }
                  });
              emitter.onTermination(
                  () -> {
                    if (stage instanceof CompletableFuture<?> future) {
                      future.cancel(true);
                    }
                  });
            });
  }

  private static Throwable unwrapStageFailure(Throwable failure) {
    Throwable current = failure;
    while ((current instanceof CompletionException || current instanceof ExecutionException)
        && current.getCause() != null) {
      current = current.getCause();
    }
    return current;
  }

  private <T> Uni<T> dynamo(Function<DynamoDbAsyncClient, CompletionStage<T>> operation) {
    return fromStage(ddb.call(operation));
  }

  private <T> T dynamoBlocking(Function<DynamoDbAsyncClient, T> operation) {
    return blockingDdb.call(operation);
  }

  // ---- KvStore (reads)

  @Override
  public Uni<Optional<Record>> get(Key key) {
    var req =
        GetItemRequest.builder().tableName(table).key(keyMap(key)).consistentRead(true).build();

    return dynamo(client -> client.getItem(req))
        .map(resp -> resp.hasItem() ? Optional.of(avToRecord(resp.item())) : Optional.empty());
  }

  @Override
  public Uni<Map<Key, Record>> getBatch(List<Key> keys) {
    List<Key> stable = keys == null ? List.of() : new ArrayList<>(new LinkedHashSet<>(keys));
    if (stable.isEmpty()) {
      return Uni.createFrom().item(Map.of());
    }
    List<Uni<Map<Key, Record>>> chunks = new ArrayList<>();
    for (int from = 0; from < stable.size(); from += 100) {
      List<Key> chunk = stable.subList(from, Math.min(from + 100, stable.size()));
      chunks.add(
          dynamo(
              client ->
                  batchGetAll(
                      client,
                      chunk.stream().map(DynamoDbKvStore::keyMap).toList(),
                      new LinkedHashMap<>(),
                      0)));
    }
    return Uni.combine()
        .all()
        .unis(chunks)
        .with(
            values -> {
              Map<Key, Record> out = new LinkedHashMap<>();
              for (Object value : values) {
                @SuppressWarnings("unchecked")
                Map<Key, Record> records = (Map<Key, Record>) value;
                out.putAll(records);
              }
              return Map.copyOf(out);
            });
  }

  private CompletionStage<Map<Key, Record>> batchGetAll(
      DynamoDbAsyncClient client,
      List<Map<String, AttributeValue>> keys,
      Map<Key, Record> accumulated,
      int attempt) {
    if (keys.isEmpty()) {
      return CompletableFuture.completedFuture(Map.copyOf(accumulated));
    }
    if (attempt >= 8) {
      return CompletableFuture.failedFuture(
          new StorageAbortRetryableException(
              "DynamoDB batch get left unprocessed keys after repeated retries"));
    }
    KeysAndAttributes requestKeys =
        KeysAndAttributes.builder().keys(keys).consistentRead(true).build();
    BatchGetItemRequest request =
        BatchGetItemRequest.builder().requestItems(Map.of(table, requestKeys)).build();
    return client
        .batchGetItem(request)
        .thenCompose(
            response -> {
              for (Map<String, AttributeValue> item :
                  response.responses().getOrDefault(table, List.of())) {
                Record record = avToRecord(item);
                accumulated.put(record.key(), record);
              }
              KeysAndAttributes unprocessed = response.unprocessedKeys().get(table);
              List<Map<String, AttributeValue>> remaining =
                  unprocessed == null ? List.of() : unprocessed.keys();
              if (remaining.isEmpty()) {
                return CompletableFuture.completedFuture(Map.copyOf(accumulated));
              }
              long delayMs = batchGetRetryDelayMs(attempt);
              return CompletableFuture.runAsync(
                      () -> {}, CompletableFuture.delayedExecutor(delayMs, TimeUnit.MILLISECONDS))
                  .thenCompose(ignored -> batchGetAll(client, remaining, accumulated, attempt + 1));
            });
  }

  static long batchGetRetryDelayMs(int attempt) {
    long baseMs = 25L;
    long maxMs = 1000L;
    long expMs = Math.min(maxMs, baseMs * (1L << Math.min(Math.max(0, attempt), 6)));
    long jitterFloorMs = Math.max(1L, expMs / 2L);
    return ThreadLocalRandom.current().nextLong(jitterFloorMs, expMs + 1L);
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

    return dynamo(client -> client.putItem(b.build()))
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

    return dynamo(client -> client.deleteItem(req))
        .replaceWith(true)
        .onFailure(ConditionalCheckFailedException.class)
        .recoverWithItem(false);
  }

  @Override
  public Uni<Optional<Long>> updateMetadataAttrsIfExists(
      Key key, Map<String, AttrValue> sets, Map<String, Long> increments) {
    MetadataAttrUpdates.validate(key, sets, increments);

    // Every attribute reaches the expression through a #placeholder: "value", "version" and
    // "timestamp" are DynamoDB reserved words, and caller-supplied attr names may be too.
    var names = new HashMap<String, String>();
    names.put("#pk", ATTR_PARTITION_KEY);
    names.put("#value", ATTR_VALUE);
    names.put("#version", ATTR_VERSION);
    var values = new HashMap<String, AttributeValue>();
    values.put(":one", N(1L));

    var setTerms = new ArrayList<String>(sets.size());
    int i = 0;
    for (var e : sets.entrySet()) {
      names.put("#s" + i, e.getKey());
      values.put(":s" + i, attrToAv(e.getValue()));
      setTerms.add("#s" + i + " = :s" + i);
      i++;
    }

    var addTerms = new ArrayList<String>(increments.size() + 1);
    i = 0;
    for (var e : increments.entrySet()) {
      names.put("#a" + i, e.getKey());
      values.put(":a" + i, N(e.getValue()));
      addTerms.add("#a" + i + " :a" + i);
      i++;
    }
    // ADD creates a missing attribute at the delta, so a record with no stored version becomes 1.
    addTerms.add("#version :one");

    // SET with an empty clause is a ValidationException, so it is omitted when there are no terms.
    var expr = new StringBuilder();
    if (!setTerms.isEmpty()) {
      expr.append("SET ").append(String.join(", ", setTerms)).append(' ');
    }
    expr.append("ADD ").append(String.join(", ", addTerms));

    var req =
        UpdateItemRequest.builder()
            .tableName(table)
            .key(keyMap(key))
            .updateExpression(expr.toString())
            // attribute_not_exists(#value) is the attrs-only guard: a value-carrying record keeps a
            // copy of its version inside the serialized payload, which this update cannot rewrite.
            .conditionExpression("attribute_exists(#pk) AND attribute_not_exists(#value)")
            .expressionAttributeNames(names)
            .expressionAttributeValues(values)
            .returnValues(ReturnValue.UPDATED_NEW)
            .build();

    return dynamo(client -> client.updateItem(req))
        .map(resp -> Optional.of(newVersionOf(resp)))
        // Absent and refused are the same condition failure server-side; both map to empty.
        // Nothing else is recovered — a ValidationException (ADD on a string-typed attribute) must
        // reach the caller.
        .onFailure(ConditionalCheckFailedException.class)
        .recoverWithItem(Optional.<Long>empty());
  }

  /**
   * The post-update version from an {@code UPDATED_NEW} response; a missing or unparsable one is
   * corruption, never absence.
   */
  private static long newVersionOf(UpdateItemResponse resp) {
    var v = resp.attributes() == null ? null : resp.attributes().get(ATTR_VERSION);
    if (v == null || v.n() == null) {
      throw new IllegalStateException(
          "UpdateItem returned no numeric " + ATTR_VERSION + " attribute: " + resp.attributes());
    }
    try {
      return Long.parseLong(v.n());
    } catch (NumberFormatException e) {
      throw new IllegalStateException(
          "UpdateItem returned a non-integral " + ATTR_VERSION + ": " + v.n(), e);
    }
  }

  // ---- Query

  @Override
  public Uni<Page> queryByPartitionKeyPrefix(
      String pk, String skPrefix, int limit, Optional<String> pageToken) {
    return queryByPartitionKeyPrefix(pk, skPrefix, limit, pageToken, false);
  }

  @Override
  public Uni<Page> queryByPartitionKeyPrefix(
      String pk, String skPrefix, int limit, Optional<String> pageToken, boolean consistentRead) {
    if (pk == null || pk.isBlank()) {
      throw new IllegalArgumentException("partition key must be provided for query");
    }

    var qb = QueryRequest.builder().tableName(table).limit(limit).consistentRead(consistentRead);

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

    return dynamo(client -> client.query(qb.build()))
        .map(
            resp -> {
              var items = new ArrayList<Record>(resp.items().size());
              for (var it : resp.items()) items.add(avToRecord(it));
              return new Page(items, encodeToken(resp.lastEvaluatedKey()));
            });
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

              return dynamo(client -> client.query(qb.build()));
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
    return dynamo(
            client ->
                client.batchWriteItem(
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
              long jitterMs = ThreadLocalRandom.current().nextLong(baseMs, expMs + 1);

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

      } else if (op instanceof TxnPutUnconditional p) {
        tx.add(
            TransactWriteItem.builder()
                .put(Put.builder().tableName(table).item(recordToAv(p.record())).build())
                .build());
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
      } else if (op instanceof TxnCheck c) {
        var check =
            ConditionCheck.builder()
                .tableName(table)
                .key(keyMap(c.key()))
                .conditionExpression("#v = :ev")
                .expressionAttributeNames(Map.of("#v", ATTR_VERSION))
                .expressionAttributeValues(Map.of(":ev", N(c.expectedVersion())))
                .build();
        tx.add(TransactWriteItem.builder().conditionCheck(check).build());
      } else if (op instanceof TxnCheckAbsent c) {
        var check =
            ConditionCheck.builder()
                .tableName(table)
                .key(keyMap(c.key()))
                .conditionExpression("attribute_not_exists(pk)")
                .build();
        tx.add(TransactWriteItem.builder().conditionCheck(check).build());
      }
    }

    var req = TransactWriteItemsRequest.builder().transactItems(tx).build();

    return dynamo(client -> client.transactWriteItems(req))
        .replaceWith(true)
        .onFailure(TransactionCanceledException.class)
        .recoverWithItem(
            t -> {
              // Return false for retryable conflicts; throw on unexpected reasons.
              TransactionCanceledException ex = (TransactionCanceledException) t;
              boolean sawRetryable = false;
              if (ex.cancellationReasons() != null) {
                for (var r : ex.cancellationReasons()) {
                  if (r == null || "None".equals(r.code())) {
                    continue;
                  }
                  if ("ConditionalCheckFailed".equals(r.code())
                      || "TransactionConflict".equals(r.code())) {
                    sawRetryable = true;
                    continue;
                  }
                  // Non-retryable reason — abort immediately
                  throw ex;
                }
              }
              if (sawRetryable) {
                return false;
              }
              throw ex;
            });
  }

  // ---- Test helpers

  @Override
  public Uni<Void> reset() {
    return fromStage(CompletableFuture.runAsync(this::resetTableIfExists));
  }

  @Override
  public Uni<Boolean> isEmpty() {
    var req =
        ScanRequest.builder()
            .tableName(this.table)
            .limit(1) // only need to find one item
            .build();

    return dynamo(client -> client.scan(req).thenApply(r -> r.items().isEmpty()));
  }

  @Override
  public Uni<Void> dump(String header) {
    return fromStage(CompletableFuture.runAsync(() -> listTableIfExists(header)));
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

      var resp =
          dynamoBlocking(
              client -> {
                try {
                  return client.scan(req).get();
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });

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

    } catch (RuntimeException e) {
      if (hasCause(e, InterruptedException.class)) {
        Thread.currentThread().interrupt();
      }
      // Table probably does not exist — safe to ignore in tests
      System.out.println("Table not found: " + this.table);
    }
  }

  private static boolean hasCause(Throwable failure, Class<? extends Throwable> causeType) {
    Throwable current = failure;
    while (current != null) {
      if (causeType.isInstance(current)) {
        return true;
      }
      current = current.getCause();
    }
    return false;
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
      dynamoBlocking(
          client ->
              client.deleteTable(DeleteTableRequest.builder().tableName(tableName).build()).join());
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

    dynamoBlocking(client -> client.createTable(req).join());
  }

  private boolean tableExists(String tableName) {
    try {
      dynamoBlocking(
          client ->
              client
                  .describeTable(DescribeTableRequest.builder().tableName(tableName).build())
                  .join());
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
            dynamoBlocking(
                client ->
                    client
                        .describeTable(DescribeTableRequest.builder().tableName(tableName).build())
                        .join());
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

  @FunctionalInterface
  public interface AsyncDynamoCaller {
    <T> CompletionStage<T> call(Function<DynamoDbAsyncClient, CompletionStage<T>> operation);
  }

  @FunctionalInterface
  public interface BlockingDynamoCaller {
    <T> T call(Function<DynamoDbAsyncClient, T> operation);
  }
}
