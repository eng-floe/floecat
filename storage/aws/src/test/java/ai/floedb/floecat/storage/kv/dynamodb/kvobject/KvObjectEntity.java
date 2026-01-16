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
package ai.floedb.floecat.storage.kv.dynamodb.kvobject;

import ai.floedb.floecat.storage.kv.AbstractEntity;
import ai.floedb.floecat.storage.kv.Keys;
import ai.floedb.floecat.storage.kv.KvStore;
import ai.floedb.floecat.storage.kv.cdi.KvTable;
import ai.floedb.floecat.test.rpc.KvObject;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * A simple entity used for unit testing the core KV abstractions.
 *
 * <p>Access patterns:
 *
 * <ul>
 *   <li>Get by id (canonical record)
 *   <li>List by value1 (non-unique pointer index)
 *   <li>List by value2 (non-unique pointer index)
 * </ul>
 */
@Singleton
public final class KvObjectEntity extends AbstractEntity<KvObject> {

  static final String KIND_TEST_OBJECT = "KvObject";
  static final String KIND_INDEXREF = "IndexRef";
  static final String KIND_SUB_OBJECT = "SubObject";

  // ---- Key conventions (string-only; portable)

  static final String PK_TESTOBJ = "testobj";
  static final String SK_META = "META";
  static final String SK_SUB = "subobj";

  static final String DIM_VALUE1 = "by-value1";
  static final String DIM_VALUE2 = "by-value2";

  static final String ATTR_SUBVALUE = "sub-key";

  @Inject
  public KvObjectEntity(@KvTable("floecat-test") KvStore kv) {
    super(
        kv,
        KIND_TEST_OBJECT,
        KvObject.getDefaultInstance(),
        (o, v) -> o.toBuilder().setVersion(v).build());
  }

  static KvStore.Key canonicalKey(String id) {
    return Keys.key(Keys.join(PK_TESTOBJ, normalize(id)), SK_META);
  }

  private static KvStore.Key indexByValue1(long value1, String id) {
    return Keys.key(
        Keys.join(PK_TESTOBJ, DIM_VALUE1, String.valueOf(value1)),
        Keys.join(PK_TESTOBJ, normalize(id)));
  }

  private static KvStore.Key indexByValue2(String value2, String id) {
    return Keys.key(
        Keys.join(PK_TESTOBJ, DIM_VALUE2, normalize(value2)), Keys.join(PK_TESTOBJ, normalize(id)));
  }

  /** Create a new object (id is unique) and add non-unique secondary index pointers. */
  public Uni<Boolean> create(KvObject obj) {
    String id = obj.getId();
    var cKey = canonicalKey(id);

    long expectedVersion = 0L;
    long newVersion = nextVersion(expectedVersion);

    var canonical = obj.toBuilder().setVersion(newVersion).build();
    var canonicalRec =
        new KvStore.Record(cKey, KIND_TEST_OBJECT, encode(canonical), Map.of(), newVersion);

    var pointerAttrs = pointerAttrs(cKey);

    var byV1 =
        new KvStore.Record(
            indexByValue1(obj.getValue1(), id),
            KIND_INDEXREF,
            new byte[0],
            pointerAttrs,
            newVersion);
    var byV2 =
        new KvStore.Record(
            indexByValue2(obj.getValue2(), id),
            KIND_INDEXREF,
            new byte[0],
            pointerAttrs,
            newVersion);

    return kv.txnWriteCas(
        List.of(
            new KvStore.TxnPut(canonicalRec, expectedVersion),
            new KvStore.TxnPut(byV1, expectedVersion),
            new KvStore.TxnPut(byV2, expectedVersion)));
  }

  public Uni<Boolean> createSecondary(KvObject obj, String subKey, String subValue) {
    String id = obj.getId();
    var sKey = Keys.key(Keys.join(PK_TESTOBJ, normalize(id)), Keys.join(SK_SUB, subKey));
    var rec =
        new KvStore.Record(sKey, KIND_SUB_OBJECT, new byte[0], Map.of(ATTR_SUBVALUE, subValue), 1);
    return kv.txnWriteCas(List.of(new KvStore.TxnPut(rec, 0)));
  }

  public Uni<Optional<KvObject>> getById(String id) {
    return this.get(canonicalKey(id));
  }

  public Uni<EntityPage<KvObject>> listByValue1(
      long value1, int limit, Optional<String> pageToken) {
    return listByPartitionKeyIndex(
        Keys.join(PK_TESTOBJ, DIM_VALUE1, String.valueOf(value1)),
        Keys.prefix(PK_TESTOBJ),
        limit,
        pageToken);
  }

  public Uni<EntityPage<KvObject>> listByValue2(
      String value2, int limit, Optional<String> pageToken) {
    return listByPartitionKeyIndex(
        Keys.join(PK_TESTOBJ, DIM_VALUE2, normalize(value2)),
        Keys.prefix(PK_TESTOBJ),
        limit,
        pageToken);
  }

  /**
   * Update the object (CAS).
   *
   * <p>If value1 or value2 changes, the old index pointers are deleted and new pointers are
   * inserted.
   */
  public Uni<Boolean> update(KvObject next) {
    String id = next.getId();
    var cKey = canonicalKey(id);

    return kv.get(cKey)
        .onItem()
        .transformToUni(
            optOldRec -> {
              if (optOldRec.isEmpty()) {
                return Uni.createFrom()
                    .failure(new NoSuchElementException("KvObject not found id=" + id));
              }

              var oldRec = optOldRec.get();
              long expectedRev = oldRec.version();

              var oldObj = decode(oldRec);

              var oldV1Key = indexByValue1(oldObj.getValue1(), id);
              var oldV2Key = indexByValue2(oldObj.getValue2(), id);

              var newV1Key = indexByValue1(next.getValue1(), id);
              var newV2Key = indexByValue2(next.getValue2(), id);

              long nextRev = expectedRev + 1;
              var canonical = next.toBuilder().setVersion(nextRev).build();
              var canonicalRec =
                  new KvStore.Record(cKey, KIND_TEST_OBJECT, encode(canonical), Map.of(), nextRev);

              var ops = new ArrayList<KvStore.TxnOp>();

              // Canonical CAS update
              ops.add(new KvStore.TxnPut(canonicalRec, expectedRev));

              // value1 pointer
              if (oldV1Key.equals(newV1Key)) {
                ops.add(
                    new KvStore.TxnPut(
                        new KvStore.Record(
                            newV1Key, KIND_INDEXREF, new byte[0], pointerAttrs(cKey), nextRev),
                        expectedRev));
              } else {
                ops.add(new KvStore.TxnDelete(oldV1Key, expectedRev));
                ops.add(
                    new KvStore.TxnPut(
                        new KvStore.Record(
                            newV1Key, KIND_INDEXREF, new byte[0], pointerAttrs(cKey), nextRev),
                        0L));
              }

              // value2 pointer
              if (oldV2Key.equals(newV2Key)) {
                ops.add(
                    new KvStore.TxnPut(
                        new KvStore.Record(
                            newV2Key, KIND_INDEXREF, new byte[0], pointerAttrs(cKey), nextRev),
                        expectedRev));
              } else {
                ops.add(new KvStore.TxnDelete(oldV2Key, expectedRev));
                ops.add(
                    new KvStore.TxnPut(
                        new KvStore.Record(
                            newV2Key, KIND_INDEXREF, new byte[0], pointerAttrs(cKey), nextRev),
                        0L));
              }

              return kv.txnWriteCas(ops);
            });
  }

  /** Delete the canonical record and any index pointers (CAS). */
  public Uni<Boolean> delete(String id) {
    var cKey = canonicalKey(id);

    return kv.get(cKey)
        .onItem()
        .transformToUni(
            optOldRec -> {
              if (optOldRec.isEmpty()) return Uni.createFrom().item(Boolean.TRUE);

              var oldRec = optOldRec.get();
              long expectedRev = oldRec.version();
              var oldObj = decode(oldRec);

              return kv.txnWriteCas(
                  List.of(
                      new KvStore.TxnDelete(cKey, expectedRev),
                      new KvStore.TxnDelete(indexByValue1(oldObj.getValue1(), id), expectedRev),
                      new KvStore.TxnDelete(indexByValue2(oldObj.getValue2(), id), expectedRev)));
            });
  }
}
