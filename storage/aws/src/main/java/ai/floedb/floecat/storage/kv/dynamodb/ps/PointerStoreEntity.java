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
package ai.floedb.floecat.storage.kv.dynamodb.ps;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.storage.kv.AbstractEntity;
import ai.floedb.floecat.storage.kv.KvStore;
import ai.floedb.floecat.storage.kv.KvStore.Key;
import ai.floedb.floecat.storage.kv.cdi.KvTable;
import com.google.protobuf.util.Timestamps;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.Map;
import java.util.Optional;

/**
 * Mutiny KV-backed implementation of the Floecat {@code PointerStore} storage model.
 *
 * <p>Keyspace layout:
 *
 * <ul>
 *   <li>Partition key: {@code pointers}
 *   <li>Sort key: the pointer's {@code key} string (verbatim)
 * </ul>
 *
 * <p>CAS semantics:
 *
 * <ul>
 *   <li>Create uses {@code expectedVersion==0} (create-if-absent)
 *   <li>Update uses {@code expectedVersion>0} (update-if-version-matches)
 *   <li>Delete without an expected version is best-effort (read current, then CAS delete)
 * </ul>
 */
@Singleton
public final class PointerStoreEntity extends AbstractEntity<Pointer> {

  static final String KIND_POINTER = "Pointer";
  static final String GLOBAL_PK = "_ACCOUNT_DIR";
  static final String ATTR_BLOB_URI = "blob_uri";

  @Inject
  public PointerStoreEntity(@KvTable("floecat") KvStore kv) {
    super(
        kv,
        KIND_POINTER,
        Pointer.getDefaultInstance(),
        (p, v) -> p.toBuilder().setVersion(v).build());
  }

  // ---- Keys

  private static KvStore.Key pointerKey(String pointerKey) {
    String k = pointerKey.startsWith("/") ? pointerKey.substring(1) : pointerKey;
    if (k.startsWith("accounts/by-id/") || k.startsWith("accounts/by-name/")) {
      return new KvStore.Key(GLOBAL_PK, k);
    }

    if (!k.startsWith("accounts/")) {
      throw new IllegalArgumentException("unexpected key: " + pointerKey);
    }

    int firstSlash = k.indexOf('/');
    int secondSlash = k.indexOf('/', firstSlash + 1);

    if (secondSlash < 0) {
      throw new IllegalArgumentException("bad key: " + pointerKey);
    }

    String accountId = k.substring(firstSlash + 1, secondSlash);
    String remainder = k.substring(secondSlash + 1);

    // Keep the key verbatim so callers can do strict prefix matching.
    return new KvStore.Key("accounts/" + accountId, remainder);
  }

  static KvStore.Key prefixKey(String prefix) {
    String p = prefix.startsWith("/") ? prefix.substring(1) : prefix;
    if (!p.endsWith("/")) {
      p = p + "/";
    }

    if (p.equals("accounts/")) {
      return new KvStore.Key("accounts", "/");
    }

    if (p.startsWith("accounts/by-id/") || p.startsWith("accounts/by-name/")) {
      return new KvStore.Key(GLOBAL_PK, p);
    }

    if (!p.startsWith("accounts/")) {
      throw new IllegalArgumentException("unexpected prefix: " + prefix);
    }

    int firstSlash = p.indexOf('/');
    int secondSlash = p.indexOf('/', firstSlash + 1);
    if (secondSlash < 0) {
      throw new IllegalArgumentException("bad prefix: " + prefix);
    }

    String accountId = p.substring(firstSlash + 1, secondSlash);
    if (accountId.isEmpty()) {
      throw new IllegalArgumentException("bad prefix: " + prefix);
    }
    String remainderPrefix = p.substring(secondSlash + 1);
    return new KvStore.Key("accounts/" + accountId, remainderPrefix);
  }

  // ---- We won't use the "value" of the base entity; all data is in the Pointer message and
  // attributes on the KV record.

  protected byte[] encode(Pointer pointer) {
    return null;
  }

  protected Pointer decode(KvStore.Record r) {
    var builder =
        Pointer.newBuilder()
            .setKey(keyOf(r.key()))
            .setBlobUri(r.attrs().getOrDefault(ATTR_BLOB_URI, ""))
            .setVersion(r.version());
    var expiresAtStr = r.attrs().get(ATTR_EXPIRES_AT);
    if (expiresAtStr != null) {
      long ts = Long.parseLong(expiresAtStr);
      builder.setExpiresAt(Timestamps.fromMillis(ts * 1000L));
    }

    return builder.buildPartial();
  }

  // ---- Timestamp support

  @Override
  protected Pointer setExpiresAt(Pointer pointer, long timestamp) {
    return pointer.toBuilder()
        .setExpiresAt(Timestamps.fromMillis(timestamp * 1000L))
        .buildPartial();
  }

  @Override
  protected long getExpiresAt(Pointer pointer) {
    if (pointer.hasExpiresAt()) {
      long ttl = Timestamps.toMillis(pointer.getExpiresAt()) / 1000L;
      return ttl;
    } else {
      return 0L;
    }
  }

  // ---- CRUD

  public Uni<Optional<Pointer>> get(String key) {
    return get(pointerKey(key));
  }

  /**
   * Compare-and-set for pointers.
   *
   * <p>If {@code expectedVersion==0}, this is create-if-absent. Otherwise, it's an update
   * conditioned on the current stored version matching {@code expectedVersion}.
   */
  public Uni<Boolean> compareAndSet(String key, long expectedVersion, Pointer pointer) {
    return putCanonicalCas(
            pointerKey(key),
            KIND_POINTER,
            pointer,
            Map.of(ATTR_BLOB_URI, pointer.getBlobUri()),
            expectedVersion)
        .map(Optional::isPresent);
  }

  /**
   * Best-effort delete.
   *
   * <p>Reads the current record version and then CAS deletes it. Returns false if absent or if a
   * concurrent update wins the race.
   */
  public Uni<Boolean> delete(String key) {
    var k = pointerKey(key);
    return kv.get(k)
        .onItem()
        .transformToUni(
            opt -> {
              if (opt.isEmpty()) return Uni.createFrom().item(false);
              return deleteCas(k, opt.get().version());
            });
  }

  /**
   * Compare-and-delete.
   *
   * <p>{@code expectedVersion} must be > 0.
   */
  public Uni<Boolean> compareAndDelete(String key, long expectedVersion) {
    if (expectedVersion <= 0L) return Uni.createFrom().item(false);
    return deleteCas(pointerKey(key), expectedVersion);
  }

  // ---- List

  public Uni<EntityPage<Pointer>> listByPrefix(
      String prefix, int limit, Optional<String> pageToken) {
    var prefixKey = prefixKey(prefix);
    return kv.queryByPartitionKeyPrefix(
            prefixKey.partitionKey(), prefixKey.sortKey(), limit, pageToken)
        .map(
            page ->
                new EntityPage<>(
                    page.items().stream().map(this::decode).toList(), page.nextToken()));
  }

  /**
   * List pointer keys by prefix.
   *
   * <p>This is used by synchronous adapters for bulk operations (delete/count) without assuming the
   * {@link Pointer} protobuf contains the pointer key string.
   */
  public Uni<EntityPage<String>> listKeysByPrefix(
      String prefix, int limit, Optional<String> pageToken) {
    var prefixKey = prefixKey(prefix);
    return kv.queryByPartitionKeyPrefix(
            prefixKey.partitionKey(), prefixKey.sortKey(), limit, pageToken)
        .map(
            page ->
                new EntityPage<>(
                    page.items().stream().map(r -> keyOf(r.key())).toList(), page.nextToken()));
  }

  /**
   * Delete pointer keys by prefix.
   *
   * @param prefix
   * @return count of items deleted
   */
  public Uni<Integer> deleteByPrefix(String prefix) {
    var prefixKey = prefixKey(prefix);
    return kv.deleteByPrefix(prefixKey.partitionKey(), prefixKey.sortKey());
  }

  // ---- Helpers (testing)

  private String keyOf(Key key) {
    if (key.partitionKey().equals(GLOBAL_PK)) {
      return key.sortKey();
    } else {
      return key.toString();
    }
  }

  static KvStore.Key _testKey(String key) {
    return pointerKey(key);
  }
}
