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

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.storage.kv.AbstractEntity;
import ai.floedb.floecat.storage.kv.AbstractEntityTest;
import ai.floedb.floecat.storage.kv.Keys;
import ai.floedb.floecat.test.rpc.KvObject;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.HashSet;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

@QuarkusTest
@EnabledIfSystemProperty(named = "floecat.kv", matches = "dynamodb")
public class KvObjectEntityTest extends AbstractEntityTest<KvObject> {
  @ConfigProperty(name = "floecat.kv.table")
  String kvTable;

  @Inject KvObjectEntity testObjects;

  @Override
  protected AbstractEntity<KvObject> getEntity() {
    return testObjects;
  }

  @Test
  void create_and_lookup_by_id_value1_value2() {
    var created =
        testObjects
            .create(
                KvObject.newBuilder()
                    .setId("obj-1")
                    .setValue1(42)
                    .setValue2("alpha")
                    .setValue3(true)
                    .build())
            .await()
            .indefinitely();
    assertTrue(created);

    var byId = testObjects.getById("obj-1").await().indefinitely();
    assertTrue(byId.isPresent());
    assertEquals("obj-1", byId.get().getId());
    assertEquals(1L, byId.get().getVersion());
    assertEquals(42, byId.get().getValue1());
    assertEquals("alpha", byId.get().getValue2());
    assertTrue(byId.get().getValue3());

    var byV1 = testObjects.listByValue1(42, 10, Optional.empty()).await().indefinitely();
    assertEquals(1, byV1.items().size());
    assertEquals("obj-1", byV1.items().get(0).getId());

    var byV2 = testObjects.listByValue2("alpha", 10, Optional.empty()).await().indefinitely();
    assertEquals(1, byV2.items().size());
    assertEquals("obj-1", byV2.items().get(0).getId());
  }

  @Test
  void secondary_indexes_allow_multiple_items() {
    testObjects
        .create(
            KvObject.newBuilder()
                .setId("obj-1")
                .setValue1(7)
                .setValue2("x")
                .setValue3(false)
                .build())
        .await()
        .indefinitely();
    testObjects
        .create(
            KvObject.newBuilder()
                .setId("obj-2")
                .setValue1(7)
                .setValue2("y")
                .setValue3(true)
                .build())
        .await()
        .indefinitely();

    var byV1 = testObjects.listByValue1(7, 10, Optional.empty()).await().indefinitely();
    assertEquals(2, byV1.items().size());
    assertTrue(byV1.items().stream().anyMatch(o -> o.getId().equals("obj-1")));
    assertTrue(byV1.items().stream().anyMatch(o -> o.getId().equals("obj-2")));
  }

  @Test
  void update_moves_secondary_index_pointers() {
    testObjects
        .create(
            KvObject.newBuilder()
                .setId("obj-1")
                .setValue1(1)
                .setValue2("old")
                .setValue3(false)
                .build())
        .await()
        .indefinitely();

    // Update both indexed fields
    var updated =
        testObjects
            .update(
                KvObject.newBuilder()
                    .setId("obj-1")
                    .setValue1(2)
                    .setValue2("new")
                    .setValue3(true)
                    .build())
            .await()
            .indefinitely();
    assertTrue(updated);

    var byId = testObjects.getById("obj-1").await().indefinitely();
    assertTrue(byId.isPresent());
    assertEquals(2L, byId.get().getVersion());
    assertEquals(2, byId.get().getValue1());
    assertEquals("new", byId.get().getValue2());
    assertTrue(byId.get().getValue3());

    var oldV1 = testObjects.listByValue1(1, 10, Optional.empty()).await().indefinitely();
    assertTrue(oldV1.items().isEmpty());

    var oldV2 = testObjects.listByValue2("old", 10, Optional.empty()).await().indefinitely();
    assertTrue(oldV2.items().isEmpty());

    var newV1 = testObjects.listByValue1(2, 10, Optional.empty()).await().indefinitely();
    assertEquals(1, newV1.items().size());
    assertEquals("obj-1", newV1.items().get(0).getId());

    var newV2 = testObjects.listByValue2("new", 10, Optional.empty()).await().indefinitely();
    assertEquals(1, newV2.items().size());
    assertEquals("obj-1", newV2.items().get(0).getId());
  }

  @Test
  void delete_removes_canonical_and_secondary_indexes() {
    testObjects
        .create(
            KvObject.newBuilder()
                .setId("obj-del")
                .setValue1(123)
                .setValue2("zzz")
                .setValue3(false)
                .build())
        .await()
        .indefinitely();

    var deleted = testObjects.delete("obj-del").await().indefinitely();
    assertTrue(deleted);

    assertTrue(testObjects.getById("obj-del").await().indefinitely().isEmpty());
    assertTrue(
        testObjects
            .listByValue1(123, 10, Optional.empty())
            .await()
            .indefinitely()
            .items()
            .isEmpty());
    assertTrue(
        testObjects
            .listByValue2("zzz", 10, Optional.empty())
            .await()
            .indefinitely()
            .items()
            .isEmpty());
  }

  @Test
  void large_listings() {

    // Create a bunch of objects.
    var expectedIds = new HashSet<String>();
    var count = 200;
    for (int i = 0; i < count; i++) {
      var id = "obj-" + i;
      expectedIds.add(id);
      var created =
          testObjects
              .create(
                  KvObject.newBuilder()
                      .setId(id)
                      .setValue1(i)
                      .setValue2("v" + i)
                      .setValue3((i % 2) == 0)
                      .build())
              .await()
              .indefinitely();
      assertTrue(created);
    }

    // Check that we can get them back and the index listings are accurate.
    for (int i = 0; i < count; i++) {
      var id = "obj-" + i;
      assertTrue(testObjects.getById(id).await().indefinitely().isPresent());

      var byV1 = testObjects.listByValue1(i, 10, Optional.empty()).await().indefinitely();
      assertEquals(1, byV1.items().size());
      assertEquals(id, byV1.items().get(0).getId());

      var byV2 = testObjects.listByValue2("v" + i, 10, Optional.empty()).await().indefinitely();
      assertEquals(1, byV2.items().size());
      assertEquals(id, byV2.items().get(0).getId());
    }

    // Delete one item by prefix.
    var iter = expectedIds.iterator();
    var oneId = iter.next();
    var oneKey = Keys.join(KvObjectEntity.PK_TESTOBJ, oneId);
    var deleted = testObjects.getKvStore().deleteByPrefix(oneKey, null).await().indefinitely();
    assertEquals(1, deleted);
    iter.remove();
    assertTrue(testObjects.getById(oneId).await().indefinitely().isEmpty());
    for (var id : expectedIds) {
      assertTrue(testObjects.getById(id).await().indefinitely().isPresent());
    }

    // Now create secondary objects under one.
    var oneObject =
        testObjects.getById(expectedIds.iterator().next()).await().indefinitely().orElseThrow();
    for (int i = 0; i < count; i++) {
      assertTrue(
          testObjects
              .createSecondary(oneObject, "second-" + i, Integer.toString(i))
              .await()
              .indefinitely());
    }

    // Check we get the listing back with prefix.
    var withSubObjectsKey = Keys.join(KvObjectEntity.PK_TESTOBJ, oneObject.getId());
    assertEquals(count + 1, listRecords(withSubObjectsKey, null).await().indefinitely().size());

    // Check we can bulk delete by prefix.
    deleted =
        testObjects.getKvStore().deleteByPrefix(withSubObjectsKey, null).await().indefinitely();
    assertEquals(count + 1, deleted);
  }
}
