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
package ai.floedb.floecat.kv;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Base class for KV-backed entities.
 *
 * <p>Writes are CAS-only: callers must supply an expected version and we atomically write/delete
 * based on that expected version.
 */
public abstract class AbstractEntity<M extends MessageLite> implements KvAttributes {
  protected final KvStore kv;
  private final M defaultInstance;
  private final VersionAccessor<M> versionAccessor;

  /**
   * Provides version read/write for the entity message type.
   *
   * <p>Implementation is typically {@code (m,v) ->m.toBuilder().setVersion(v).build()}.
   */
  public interface VersionAccessor<M> {
    M withVersion(M message, long newVersion);
  }

  @Inject
  protected AbstractEntity(KvStore kv, M defaultInstance, VersionAccessor<M> versionAccessor) {
    this.kv = kv;
    this.defaultInstance = defaultInstance;
    this.versionAccessor = versionAccessor;
  }

  protected byte[] encode(M message) {
    return message != null ? message.toByteArray() : null;
  }

  @SuppressWarnings("unchecked")
  protected M decode(KvStore.Record r) {
    try {
      byte[] data = r.value();
      if (data == null || data.length == 0) {
        return null;
      }
      var m = (M) defaultInstance.getParserForType().parseFrom(data);
      var ts = r.attrs().get(ATTR_EXPIRES_AT);
      if (ts != null) {
        m = setExpiresAt(m, Long.parseLong(ts));
      }
      return m;
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  protected M setExpiresAt(M message, long timestamp) {
    // Default no-op
    return message;
  }

  protected long getExpiresAt(M message) {
    return 0L;
  }

  protected static Map<String, String> pointerAttrs(KvStore.Key target) {
    return Map.of(
        TARGET_PARTITION_KEY, target.partitionKey(),
        TARGET_SORT_KEY, target.sortKey());
  }

  protected static String normalize(String s) {
    return s.trim().toLowerCase(Locale.ROOT);
  }

  // ---- Reads

  protected Uni<Optional<M>> get(KvStore.Key key) {
    return kv.get(key).map(opt -> opt.map(r -> decode(r)));
  }

  // ---- CAS helpers

  /**
   * Builds the next monotonically increasing version given an expected version. expectedVersion==0
   * => nextVersion==1.
   */
  protected static long nextVersion(long expectedVersion) {
    if (expectedVersion < 0) throw new IllegalArgumentException("expectedVersion must be >= 0");
    return expectedVersion + 1;
  }

  /**
   * CAS put for a canonical (protobuf) entity.
   *
   * <p>The written record.version and protobuf message version are both set to nextVersion.
   */
  protected Uni<Boolean> putCanonicalCas(
      KvStore.Key key, String kind, M message, Map<String, String> attrs, long expectedVersion) {

    long nv = nextVersion(expectedVersion);
    M withVer = versionAccessor != null ? versionAccessor.withVersion(message, nv) : message;
    long ts = getExpiresAt(withVer);
    if (ts > 0) {
      attrs = new java.util.HashMap<>(attrs);
      attrs.put(ATTR_EXPIRES_AT, Long.toString(ts));
    }
    var rec = new KvStore.Record(key, kind, encode(withVer), attrs, nv);
    return kv.putCas(rec, expectedVersion);
  }

  /** CAS delete for any item (canonical or pointer/index). */
  protected Uni<Boolean> deleteCas(KvStore.Key key, long expectedVersion) {
    return kv.deleteCas(key, expectedVersion);
  }

  /**
   * CAS put for a non-protobuf item (pointer/index/metadata).
   *
   * <p>Caller supplies raw value bytes; we still write a monotonically increasing version.
   */
  protected Uni<Boolean> putRawCas(
      KvStore.Key key, String kind, byte[] value, Map<String, String> attrs, long expectedVersion) {

    long nv = nextVersion(expectedVersion);
    var rec = new KvStore.Record(key, kind, value, attrs, nv);
    return kv.putCas(rec, expectedVersion);
  }

  // ---- Common pointer patterns

  protected Uni<Optional<M>> getViaPointer(KvStore.Key pointerKey) {
    return kv.get(pointerKey)
        .onItem()
        .transformToUni(
            optPtr -> {
              if (optPtr.isEmpty()) return Uni.createFrom().item(Optional.empty());
              var ptr = optPtr.get();
              var tpk = ptr.attrs().get(TARGET_PARTITION_KEY);
              var tsk = ptr.attrs().get(TARGET_SORT_KEY);
              if (tpk == null || tsk == null) return Uni.createFrom().item(Optional.empty());
              return get(new KvStore.Key(tpk, tsk));
            });
  }

  public record EntityPage<T>(List<T> items, Optional<String> nextToken) {}

  /**
   * Generic pointer-based list helper.
   *
   * <p>Pointer items must contain:
   *
   * <ul>
   *   <li>{@link KvAttributes#TARGET_PARTITION_KEY}
   *   <li>{@link KvAttributes#TARGET_SORT_KEY}
   * </ul>
   */
  protected Uni<EntityPage<M>> listByPartitionKeyIndex(
      String partitionKey, String sortKeyPrefix, int limit, Optional<String> pageToken) {
    return kv.queryByPartitionKeyPrefix(partitionKey, sortKeyPrefix, limit, pageToken)
        .chain(
            page -> {
              // Short-circuit empty pages (avoids Mutiny empty-combine bug)
              if (page.items().isEmpty()) {
                return Uni.createFrom().item(new EntityPage<>(List.of(), page.nextToken()));
              }

              List<Uni<Optional<KvStore.Record>>> reads =
                  page.items().stream()
                      .map(
                          r -> {
                            String pk = r.attrs().get(KvAttributes.TARGET_PARTITION_KEY);
                            String sk = r.attrs().get(KvAttributes.TARGET_SORT_KEY);
                            return kv.get(new KvStore.Key(pk, sk));
                          })
                      .toList();

              return Uni.combine()
                  .all()
                  .unis(reads)
                  .with(
                      list -> {
                        var out = new ArrayList<M>();
                        for (Object o : list) {
                          @SuppressWarnings("unchecked")
                          var opt = (Optional<KvStore.Record>) o;
                          opt.ifPresent(a -> out.add(decode(a)));
                        }
                        return new EntityPage<M>(out, page.nextToken());
                      });
            });
  }

  public boolean isEmpty() {
    return kv.isEmpty().await().indefinitely();
  }

  public KvStore getKvStore() {
    return this.kv;
  }
}
