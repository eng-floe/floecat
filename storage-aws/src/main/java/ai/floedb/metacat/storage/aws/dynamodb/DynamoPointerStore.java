package ai.floedb.metacat.storage.aws.dynamodb;

import ai.floedb.metacat.common.rpc.Pointer;
import ai.floedb.metacat.storage.PointerStore;
import ai.floedb.metacat.storage.errors.StorageCorruptionException;
import com.google.protobuf.util.Timestamps;
import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.inject.Singleton;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

@Singleton
@IfBuildProperty(name = "metacat.kv", stringValue = "dynamodb")
public final class DynamoPointerStore implements PointerStore {
  private static final String ATTR_PK = "pk";
  private static final String ATTR_SK = "sk";
  private static final String ATTR_BLOB_URI = "blob_uri";
  private static final String ATTR_VERSION = "version";
  private static final String ATTR_EXPIRES_AT = "expires_at";
  private static final String GLOBAL_PK = "_TENANT_DIR";

  private final DynamoDbClient ddb;
  private final String table;
  private final Clock clock;

  public DynamoPointerStore(DynamoDbClient ddb, String table, Clock clock) {
    this.ddb = Objects.requireNonNull(ddb);
    this.table = Objects.requireNonNull(table);
    this.clock = Objects.requireNonNullElse(clock, Clock.systemUTC());
  }

  @Override
  public Optional<Pointer> get(String key) {
    var mk = mapKey(key);
    var out = ddb.getItem(r -> r.tableName(table).key(kv(mk.pk(), mk.sk())));
    if (!out.hasItem() || out.item().isEmpty()) return Optional.empty();
    return Optional.of(fromItemToPointer(key, out.item()));
  }

  @Override
  public boolean compareAndSet(String key, long expectedVersion, Pointer next) {
    var mk = mapKey(key);

    if (expectedVersion == 0L) {
      var item = new HashMap<String, AttributeValue>();
      item.put(ATTR_PK, AttributeValue.fromS(mk.pk()));
      item.put(ATTR_SK, AttributeValue.fromS(mk.sk()));
      item.put(ATTR_BLOB_URI, AttributeValue.fromS(next.getBlobUri()));
      item.put(ATTR_VERSION, AttributeValue.fromN(Long.toString(next.getVersion())));
      if (next.hasExpiresAt()) {
        long ttl = Timestamps.toMillis(next.getExpiresAt()) / 1000L;
        item.put(ATTR_EXPIRES_AT, AttributeValue.fromN(Long.toString(ttl)));
      }
      try {
        ddb.putItem(
            r ->
                r.tableName(table)
                    .item(item)
                    .conditionExpression("attribute_not_exists(#pk) AND attribute_not_exists(#sk)")
                    .expressionAttributeNames(Map.of("#pk", ATTR_PK, "#sk", ATTR_SK)));
        return true;
      } catch (ConditionalCheckFailedException ccfe) {
        return false;
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
        ddb.updateItem(
            r ->
                r.tableName(table)
                    .key(kv(mk.pk(), mk.sk()))
                    .updateExpression(updateExpr)
                    .conditionExpression("#v = :expected")
                    .expressionAttributeNames(exprNames)
                    .expressionAttributeValues(exprValues));
        return true;
      } catch (ConditionalCheckFailedException ccfe) {
        return false;
      }
    }
  }

  @Override
  public boolean delete(String key) {
    var mk = mapKey(key);
    var out = ddb.deleteItem(r -> r.tableName(table).key(kv(mk.pk(), mk.sk())));
    return out.sdkHttpResponse().isSuccessful();
  }

  @Override
  public boolean compareAndDelete(String key, long expectedVersion) {
    var mk = mapKey(key);
    try {
      ddb.deleteItem(
          r ->
              r.tableName(table)
                  .key(kv(mk.pk(), mk.sk()))
                  .conditionExpression(ATTR_VERSION + " = :v")
                  .expressionAttributeValues(
                      Map.of(":v", AttributeValue.fromN(Long.toString(expectedVersion)))));
      return true;
    } catch (ConditionalCheckFailedException ccfe) {
      return false;
    }
  }

  @Override
  public List<Row> listPointersByPrefix(
      String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
    var mp = mapPrefix(prefix);
    Map<String, String> names = Map.of("#pk", ATTR_PK, "#sk", ATTR_SK);
    Map<String, AttributeValue> values = new HashMap<>();
    values.put(":pk", AttributeValue.fromS(mp.pk()));
    values.put(":skp", AttributeValue.fromS(mp.skPrefix()));

    var builder =
        QueryRequest.builder()
            .tableName(table)
            .keyConditionExpression("#pk = :pk AND begins_with(#sk, :skp)")
            .expressionAttributeNames(names)
            .expressionAttributeValues(values)
            .limit(Math.max(1, limit));

    if (pageToken != null && !pageToken.isBlank()) {
      var eks = decodeToken(pageToken);
      builder.exclusiveStartKey(eks);
    }

    var q = ddb.query(builder.build());
    if (q.lastEvaluatedKey() != null && !q.lastEvaluatedKey().isEmpty()) {
      nextTokenOut.append(encodeToken(q.lastEvaluatedKey()));
    }

    var rows = new ArrayList<Row>(q.count());
    for (var item : q.items()) {
      String pk = attrS(item, ATTR_PK);
      String sk = attrS(item, ATTR_SK);
      String pointerKey = fullKey(pk, sk);
      String blobUri = attrS(item, ATTR_BLOB_URI);
      long version = Long.parseLong(attrN(item, ATTR_VERSION));
      rows.add(new Row(pointerKey, blobUri, version));
    }
    return rows;
  }

  @Override
  public int countByPrefix(String prefix) {
    var mp = mapPrefix(prefix);
    final Map<String, String> names = Map.of("#pk", ATTR_PK, "#sk", ATTR_SK);
    final Map<String, AttributeValue> values = new HashMap<>();
    values.put(":pk", AttributeValue.fromS(mp.pk()));
    values.put(":skp", AttributeValue.fromS(mp.skPrefix()));

    int total = 0;
    Map<String, AttributeValue> eks = null;

    do {
      final Map<String, AttributeValue> startKey = eks;
      var q =
          ddb.query(
              r ->
                  r.tableName(table)
                      .keyConditionExpression("#pk = :pk AND begins_with(#sk, :skp)")
                      .expressionAttributeNames(names)
                      .expressionAttributeValues(values)
                      .select(Select.COUNT)
                      .exclusiveStartKey(startKey));
      total += q.count();
      eks = q.lastEvaluatedKey();
    } while (eks != null && !eks.isEmpty());

    return total;
  }

  private static Map<String, AttributeValue> kv(String pk, String sk) {
    return Map.of(ATTR_PK, AttributeValue.fromS(pk), ATTR_SK, AttributeValue.fromS(sk));
  }

  private Pointer fromItemToPointer(String originalKey, Map<String, AttributeValue> item) {
    var b =
        Pointer.newBuilder()
            .setKey(originalKey)
            .setBlobUri(attrS(item, ATTR_BLOB_URI))
            .setVersion(Long.parseLong(attrN(item, ATTR_VERSION)));
    if (item.containsKey(ATTR_EXPIRES_AT)) {
      long epochSec = Long.parseLong(attrN(item, ATTR_EXPIRES_AT));
      if (epochSec > 0) {
        b.setExpiresAt(Timestamps.fromMillis(epochSec * 1000L));
      }
    }
    return b.build();
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
    if (GLOBAL_PK.equals(pk)) return "/" + sk;
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
    if (idx <= 0) throw new IllegalArgumentException("bad page token");
    String pk = s.substring(0, idx);
    String sk = s.substring(idx + 1);
    return kv(pk, sk);
  }

  private static MappedKey mapKey(String pointerKey) {
    String k = pointerKey.startsWith("/") ? pointerKey.substring(1) : pointerKey;
    if (k.startsWith("tenants/by-id/") || k.startsWith("tenants/by-name/")) {
      return new MappedKey(GLOBAL_PK, k);
    }
    if (!k.startsWith("tenants/"))
      throw new IllegalArgumentException("unexpected key: " + pointerKey);
    int firstSlash = k.indexOf('/');
    int secondSlash = k.indexOf('/', firstSlash + 1);
    if (secondSlash < 0) throw new IllegalArgumentException("bad key: " + pointerKey);
    String tenantId = k.substring(firstSlash + 1, secondSlash);
    String remainder = k.substring(secondSlash + 1);
    return new MappedKey(tenantId, remainder);
  }

  private static MappedPrefix mapPrefix(String prefix) {
    String p = prefix.startsWith("/") ? prefix.substring(1) : prefix;
    if (p.startsWith("tenants/by-id/") || p.startsWith("tenants/by-name/")) {
      return new MappedPrefix(GLOBAL_PK, p);
    }
    if (!p.startsWith("tenants/"))
      throw new IllegalArgumentException("unexpected prefix: " + prefix);
    int firstSlash = p.indexOf('/');
    int secondSlash = p.indexOf('/', firstSlash + 1);
    if (secondSlash < 0) throw new IllegalArgumentException("bad prefix: " + prefix);
    String tenantId = p.substring(firstSlash + 1, secondSlash);
    String remainderPrefix = p.substring(secondSlash + 1);
    return new MappedPrefix(tenantId, remainderPrefix);
  }

  private record MappedKey(String pk, String sk) {}

  private record MappedPrefix(String pk, String skPrefix) {}
}
