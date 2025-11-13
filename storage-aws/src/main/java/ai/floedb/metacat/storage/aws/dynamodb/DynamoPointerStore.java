package ai.floedb.metacat.storage.aws.dynamodb;

import ai.floedb.metacat.common.rpc.Pointer;
import ai.floedb.metacat.storage.PointerStore;
import ai.floedb.metacat.storage.errors.StorageAbortRetryableException;
import ai.floedb.metacat.storage.errors.StorageCorruptionException;
import ai.floedb.metacat.storage.errors.StorageException;
import ai.floedb.metacat.storage.errors.StoragePreconditionFailedException;
import com.google.protobuf.util.Timestamps;
import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.inject.Singleton;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.Select;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

@Singleton
@IfBuildProperty(name = "metacat.kv", stringValue = "dynamodb")
public final class DynamoPointerStore implements PointerStore {
  private static final String ATTR_PK = "pk";
  private static final String ATTR_SK = "sk";
  private static final String ATTR_BLOB_URI = "blob_uri";
  private static final String ATTR_VERSION = "version";
  private static final String ATTR_EXPIRES_AT = "expires_at";
  private static final String GLOBAL_PK = "_TENANT_DIR";

  private final DynamoDbClient dynamoDb;
  private final String table;

  public DynamoPointerStore(
      DynamoDbClient ddb, @ConfigProperty(name = "metacat.kv.table") String table) {
    this.dynamoDb = Objects.requireNonNull(ddb);
    this.table = Objects.requireNonNull(table);
  }

  @Override
  public Optional<Pointer> get(String key) {
    var mappedKey = mapKey(key);
    try {
      var out =
          dynamoDb.getItem(
              r -> r.tableName(table).consistentRead(true).key(kv(mappedKey.pk(), mappedKey.sk())));

      if (!out.hasItem() || out.item().isEmpty()) {
        return Optional.empty();
      }

      return Optional.of(fromItemToPointer(key, out.item()));
    } catch (DynamoDbException e) {
      throw mapAndWrap("GetItem", key, e);
    } catch (SdkClientException e) {
      throw new StorageAbortRetryableException(msg("GetItem", key, e.getMessage()));
    }
  }

  public boolean compareAndSet(String key, long expectedVersion, Pointer next) {
    var mappedKey = mapKey(key);

    if (expectedVersion == 0L) {
      var item = new HashMap<String, AttributeValue>();
      item.put(ATTR_PK, AttributeValue.fromS(mappedKey.pk()));
      item.put(ATTR_SK, AttributeValue.fromS(mappedKey.sk()));
      item.put(ATTR_BLOB_URI, AttributeValue.fromS(next.getBlobUri()));
      item.put(ATTR_VERSION, AttributeValue.fromN(Long.toString(next.getVersion())));

      if (next.hasExpiresAt()) {
        long ttl = Timestamps.toMillis(next.getExpiresAt()) / 1000L;
        item.put(ATTR_EXPIRES_AT, AttributeValue.fromN(Long.toString(ttl)));
      }
      try {
        dynamoDb.putItem(
            r ->
                r.tableName(table)
                    .item(item)
                    .conditionExpression("attribute_not_exists(#pk) AND attribute_not_exists(#sk)")
                    .expressionAttributeNames(Map.of("#pk", ATTR_PK, "#sk", ATTR_SK)));
        return true;
      } catch (ConditionalCheckFailedException ccfe) {
        return false;
      } catch (DynamoDbException e) {
        throw mapAndWrap("PutItem", key, e);
      } catch (SdkClientException e) {
        throw new StorageAbortRetryableException(msg("PutItem", key, e.getMessage()));
      }
    } else {
      final var exprNames = new HashMap<String, String>();
      exprNames.put("#b", ATTR_BLOB_URI);
      exprNames.put("#v", ATTR_VERSION);

      final var exprValues = new HashMap<String, AttributeValue>();
      exprValues.put(":b", AttributeValue.fromS(next.getBlobUri()));
      exprValues.put(":expected", AttributeValue.fromN(Long.toString(expectedVersion)));
      exprValues.put(":next", AttributeValue.fromN(Long.toString(next.getVersion())));

      final boolean hasTtl = next.hasExpiresAt();
      if (hasTtl) {
        long ttl = Timestamps.toMillis(next.getExpiresAt()) / 1000L;
        exprValues.put(":e", AttributeValue.fromN(Long.toString(ttl)));
      }

      final String updateExpr =
          hasTtl
              ? "SET #b = :b, #v = :next, " + ATTR_EXPIRES_AT + " = :e"
              : "SET #b = :b, #v = :next";

      try {
        dynamoDb.updateItem(
            responseBuilder ->
                responseBuilder
                    .tableName(table)
                    .key(kv(mappedKey.pk(), mappedKey.sk()))
                    .updateExpression(updateExpr)
                    .conditionExpression("#v = :expected")
                    .expressionAttributeNames(exprNames)
                    .expressionAttributeValues(exprValues));
        return true;
      } catch (ConditionalCheckFailedException ccfe) {
        return false;
      } catch (DynamoDbException e) {
        throw mapAndWrap("UpdateItem", key, e);
      } catch (SdkClientException e) {
        throw new StorageAbortRetryableException(msg("UpdateItem", key, e.getMessage()));
      }
    }
  }

  @Override
  public boolean delete(String key) {
    var mappedKey = mapKey(key);
    try {
      var out =
          dynamoDb.deleteItem(r -> r.tableName(table).key(kv(mappedKey.pk(), mappedKey.sk())));

      return out.sdkHttpResponse().isSuccessful();
    } catch (DynamoDbException e) {
      throw mapAndWrap("DeleteItem", key, e);
    } catch (SdkClientException e) {
      throw new StorageAbortRetryableException(msg("DeleteItem", key, e.getMessage()));
    }
  }

  @Override
  public int deleteByPrefix(String prefix) {
    var mapped = mapPrefix(prefix);
    int deleted = 0;
    Map<String, AttributeValue> lek = null;

    do {
      QueryRequest.Builder qb =
          QueryRequest.builder()
              .tableName(table)
              .projectionExpression("#pk,#sk")
              .expressionAttributeNames(Map.of("#pk", ATTR_PK, "#sk", ATTR_SK))
              .consistentRead(true)
              .limit(1000)
              .exclusiveStartKey(lek);

      if (mapped.skPrefix().isEmpty()) {
        qb.keyConditionExpression("#pk = :pk")
            .expressionAttributeValues(Map.of(":pk", AttributeValue.fromS(mapped.pk())));
      } else {
        qb.keyConditionExpression("#pk = :pk AND begins_with(#sk, :skp)")
            .expressionAttributeValues(
                Map.of(
                    ":pk", AttributeValue.fromS(mapped.pk()),
                    ":skp", AttributeValue.fromS(mapped.skPrefix())));
      }

      var page = dynamoDb.query(qb.build());

      List<WriteRequest> batch = new ArrayList<>(25);
      for (var item : page.items()) {
        var del =
            DeleteRequest.builder().key(kv(attrS(item, ATTR_PK), attrS(item, ATTR_SK))).build();
        batch.add(WriteRequest.builder().deleteRequest(del).build());

        if (batch.size() == 25) {
          deleted += execBatch(batch);
          batch.clear();
        }
      }
      if (!batch.isEmpty()) {
        deleted += execBatch(batch);
      }

      lek = page.lastEvaluatedKey();
    } while (lek != null && !lek.isEmpty());

    return deleted;
  }

  private int execBatch(List<WriteRequest> batch) {
    int total = 0;
    Map<String, List<WriteRequest>> req = Map.of(table, List.copyOf(batch));
    BatchWriteItemResponse resp = dynamoDb.batchWriteItem(b -> b.requestItems(req));

    int processed = batch.size();
    while (resp.hasUnprocessedItems() && !resp.unprocessedItems().isEmpty()) {
      var unp = resp.unprocessedItems().getOrDefault(table, List.of());
      processed -= unp.size();
      if (unp.isEmpty()) {
        break;
      }

      resp = dynamoDb.batchWriteItem(b -> b.requestItems(Map.of(table, unp)));
      processed += (unp.size() - resp.unprocessedItems().getOrDefault(table, List.of()).size());
    }
    total += processed;
    return total;
  }

  @Override
  public boolean compareAndDelete(String key, long expectedVersion) {
    var mappedKey = mapKey(key);
    try {
      dynamoDb.deleteItem(
          responseBuilder ->
              responseBuilder
                  .tableName(table)
                  .key(kv(mappedKey.pk(), mappedKey.sk()))
                  .conditionExpression(ATTR_VERSION + " = :v")
                  .expressionAttributeValues(
                      Map.of(":v", AttributeValue.fromN(Long.toString(expectedVersion)))));

      return true;
    } catch (ConditionalCheckFailedException ccfe) {
      return false;
    } catch (DynamoDbException e) {
      throw mapAndWrap("DeleteItem", key, e);
    } catch (SdkClientException e) {
      throw new StorageAbortRetryableException(msg("DeleteItem", key, e.getMessage()));
    }
  }

  @Override
  public List<Pointer> listPointersByPrefix(
      String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {

    var mappedPrefix = mapPrefix(prefix);

    final Map<String, String> names = new HashMap<>();
    names.put("#pk", ATTR_PK);
    names.put("#sk", ATTR_SK);
    names.put("#b", ATTR_BLOB_URI);
    names.put("#v", ATTR_VERSION);
    names.put("#e", ATTR_EXPIRES_AT);

    QueryRequest.Builder qb =
        QueryRequest.builder()
            .tableName(table)
            .consistentRead(true)
            .limit(Math.max(1, limit))
            .projectionExpression("#pk,#sk,#b,#v,#e")
            .expressionAttributeNames(names);

    final Map<String, AttributeValue> values = new HashMap<>();
    if (mappedPrefix.skPrefix().isEmpty()) {
      qb.keyConditionExpression("#pk = :pk");
      values.put(":pk", AttributeValue.fromS(mappedPrefix.pk()));
    } else {
      qb.keyConditionExpression("#pk = :pk AND begins_with(#sk, :skp)");
      values.put(":pk", AttributeValue.fromS(mappedPrefix.pk()));
      values.put(":skp", AttributeValue.fromS(mappedPrefix.skPrefix()));
    }
    qb.expressionAttributeValues(values);

    if (pageToken != null && !pageToken.isBlank()) {
      qb.exclusiveStartKey(decodeToken(pageToken));
    }

    try {
      var query = dynamoDb.query(qb.build());

      if (nextTokenOut != null) {
        nextTokenOut.setLength(0);
        var lek = query.lastEvaluatedKey();
        if (lek != null && !lek.isEmpty()) {
          nextTokenOut.append(encodeToken(lek));
        }
      }

      var rows = new ArrayList<Pointer>(query.count());
      for (var item : query.items()) {
        String pk = attrS(item, ATTR_PK);
        String sk = attrS(item, ATTR_SK);
        String pointerKey = fullKey(pk, sk);
        rows.add(fromItemToPointer(pointerKey, item));
      }
      return rows;

    } catch (DynamoDbException e) {
      throw mapAndWrap("Query", prefix, e);
    } catch (SdkClientException e) {
      throw new StorageAbortRetryableException(msg("Query", prefix, e.getMessage()));
    }
  }

  @Override
  public int countByPrefix(String prefix) {
    var mappedPrefix = mapPrefix(prefix);
    int total = 0;
    Map<String, AttributeValue> evaluatedKeys = null;

    try {
      do {
        QueryRequest.Builder queryBuilder =
            QueryRequest.builder()
                .tableName(table)
                .select(Select.COUNT)
                .consistentRead(true)
                .exclusiveStartKey(evaluatedKeys);

        if (mappedPrefix.skPrefix().isEmpty()) {
          queryBuilder
              .keyConditionExpression("#pk = :pk")
              .expressionAttributeNames(Map.of("#pk", ATTR_PK))
              .expressionAttributeValues(Map.of(":pk", AttributeValue.fromS(mappedPrefix.pk())));
        } else {
          queryBuilder
              .keyConditionExpression("#pk = :pk AND begins_with(#sk, :skp)")
              .expressionAttributeNames(Map.of("#pk", ATTR_PK, "#sk", ATTR_SK))
              .expressionAttributeValues(
                  Map.of(
                      ":pk", AttributeValue.fromS(mappedPrefix.pk()),
                      ":skp", AttributeValue.fromS(mappedPrefix.skPrefix())));
        }

        var query = dynamoDb.query(queryBuilder.build());
        total += query.count();
        evaluatedKeys = query.lastEvaluatedKey();
      } while (evaluatedKeys != null && !evaluatedKeys.isEmpty());

      return total;
    } catch (DynamoDbException e) {
      throw mapAndWrap("Query(COUNT)", prefix, e);
    } catch (SdkClientException e) {
      throw new StorageAbortRetryableException(msg("Query(COUNT)", prefix, e.getMessage()));
    }
  }

  private static String msg(String op, String key, String detail) {
    return "dynamodb " + op + " failed for key=" + key + (detail == null ? "" : " : " + detail);
  }

  private StorageException mapAndWrap(String op, String key, DynamoDbException de) {
    int statusCode = de.statusCode();
    String detail =
        de.awsErrorDetails() != null ? de.awsErrorDetails().errorMessage() : de.getMessage();

    if (de instanceof ResourceNotFoundException) {
      return new StorageCorruptionException(msg(op, key, "resource not found: " + detail), de);
    }
    if (de instanceof ProvisionedThroughputExceededException
        || "ThrottlingException"
            .equals(de.awsErrorDetails() != null ? de.awsErrorDetails().errorCode() : null)
        || statusCode >= 500) {
      return new StorageAbortRetryableException(msg(op, key, detail));
    }
    if (de instanceof ConditionalCheckFailedException) {
      return new StoragePreconditionFailedException(msg(op, key, "conditional check failed"));
    }

    return new StorageException(msg(op, key, detail), de);
  }

  private static Map<String, AttributeValue> kv(String pk, String sk) {
    return Map.of(ATTR_PK, AttributeValue.fromS(pk), ATTR_SK, AttributeValue.fromS(sk));
  }

  private Pointer fromItemToPointer(String originalKey, Map<String, AttributeValue> item) {
    var pointerBuilder =
        Pointer.newBuilder()
            .setKey(originalKey)
            .setBlobUri(attrS(item, ATTR_BLOB_URI))
            .setVersion(Long.parseLong(attrN(item, ATTR_VERSION)));
    if (item.containsKey(ATTR_EXPIRES_AT)) {
      long epochSec = Long.parseLong(attrN(item, ATTR_EXPIRES_AT));
      if (epochSec > 0) {
        pointerBuilder.setExpiresAt(Timestamps.fromMillis(epochSec * 1000L));
      }
    }
    return pointerBuilder.build();
  }

  private static String attrS(Map<String, AttributeValue> m, String k) {
    var v = m.get(k);
    if (v == null || v.s() == null)
      throw new StorageCorruptionException("missing attr: " + k, null);
    return v.s();
  }

  private static String attrN(Map<String, AttributeValue> m, String k) {
    var v = m.get(k);
    if (v == null || v.n() == null)
      throw new StorageCorruptionException("missing attr: " + k, null);
    return v.n();
  }

  private static String fullKey(String pk, String sk) {
    if (GLOBAL_PK.equals(pk)) {
      return "/" + sk;
    }

    return "/tenants/" + pk + "/" + sk;
  }

  private static String encodeToken(Map<String, AttributeValue> lek) {
    String pk = lek.getOrDefault(ATTR_PK, AttributeValue.fromS("")).s();
    String sk = lek.getOrDefault(ATTR_SK, AttributeValue.fromS("")).s();
    String payload = pk + "\n" + sk;
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(payload.getBytes(StandardCharsets.UTF_8));
  }

  private static Map<String, AttributeValue> decodeToken(String token) {
    var bytes = Base64.getUrlDecoder().decode(token);
    var s = new String(bytes, StandardCharsets.UTF_8);
    int idx = s.indexOf('\n');
    if (idx <= 0) {
      throw new IllegalArgumentException("bad page token");
    }

    String pk = s.substring(0, idx);
    String sk = s.substring(idx + 1);
    return kv(pk, sk);
  }

  private static MappedKey mapKey(String pointerKey) {
    String k = pointerKey.startsWith("/") ? pointerKey.substring(1) : pointerKey;
    if (k.startsWith("tenants/by-id/") || k.startsWith("tenants/by-name/")) {
      return new MappedKey(GLOBAL_PK, k);
    }

    if (!k.startsWith("tenants/")) {
      throw new IllegalArgumentException("unexpected key: " + pointerKey);
    }

    int firstSlash = k.indexOf('/');
    int secondSlash = k.indexOf('/', firstSlash + 1);

    if (secondSlash < 0) {
      throw new IllegalArgumentException("bad key: " + pointerKey);
    }

    String tenantId = k.substring(firstSlash + 1, secondSlash);
    String remainder = k.substring(secondSlash + 1);
    return new MappedKey(tenantId, remainder);
  }

  private static MappedPrefix mapPrefix(String prefix) {
    String p = prefix.startsWith("/") ? prefix.substring(1) : prefix;
    if (!p.endsWith("/")) {
      p = p + "/";
    }

    if (p.equals("tenants/")
        || p.equals("idempotency/")
        || p.startsWith("tenants/by-id/")
        || p.startsWith("tenants/by-name/")) {
      return new MappedPrefix(GLOBAL_PK, p);
    }

    if (!p.startsWith("tenants/")) {
      throw new IllegalArgumentException("unexpected prefix: " + prefix);
    }

    int firstSlash = p.indexOf('/');
    int secondSlash = p.indexOf('/', firstSlash + 1);
    if (secondSlash < 0) {
      throw new IllegalArgumentException("bad prefix: " + prefix);
    }

    String tenantId = p.substring(firstSlash + 1, secondSlash);
    String remainderPrefix = p.substring(secondSlash + 1);
    return new MappedPrefix(tenantId, remainderPrefix);
  }

  private record MappedKey(String pk, String sk) {}

  private record MappedPrefix(String pk, String skPrefix) {}
}
