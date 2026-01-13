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
package ai.floedb.floecat.kv.dynamodb.ps;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.kv.AbstractEntity;
import ai.floedb.floecat.kv.test.AbstractEntityTest;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

@QuarkusTest
@EnabledIfSystemProperty(named = "floecat.kv", matches = "dynamodb")
public class PointerStoreEntityTest extends AbstractEntityTest<Pointer> {

  @Inject PointerStoreEntity pointers;

  @Override
  protected AbstractEntity<Pointer> getEntity() {
    return pointers;
  }

  @Test
  void create_get_list_and_delete() {
    var p =
        Pointer.newBuilder().setKey("accounts/by-id/1/catalog/abc").setBlobUri("s3://b/1").build();

    assertTrue(pointers.compareAndSet(p.getKey(), 0L, p).await().indefinitely());

    var got = pointers.get(p.getKey()).await().indefinitely().orElseThrow();
    assertEquals(p.getKey(), got.getKey());
    assertEquals("s3://b/1", got.getBlobUri());
    assertEquals(1L, got.getVersion());

    var page =
        pointers.listByPrefix("accounts/by-id/1/", 50, Optional.empty()).await().indefinitely();
    assertEquals(1, page.items().size());
    assertEquals(p.getKey(), page.items().get(0).getKey());

    assertTrue(pointers.delete(p.getKey()).await().indefinitely());
    assertTrue(pointers.get(p.getKey()).await().indefinitely().isEmpty());
    assertEmpty();
  }

  @Test
  void cas_updates_increment_version_and_wrong_expected_fails() {
    String key = "accounts/by-id/2/ns/x";
    var p1 = Pointer.newBuilder().setKey(key).setBlobUri("s3://b/v1").build();
    assertTrue(pointers.compareAndSet(key, 0L, p1).await().indefinitely());
    assertEquals(1L, pointers.get(key).await().indefinitely().orElseThrow().getVersion());

    // Wrong expected version should fail.
    assertFalse(
        pointers
            .compareAndSet(
                key, 999L, Pointer.newBuilder().setKey(key).setBlobUri("s3://b/bad").build())
            .await()
            .indefinitely());

    // Correct expected version should succeed and bump to 2.
    assertTrue(
        pointers
            .compareAndSet(
                key, 1L, Pointer.newBuilder().setKey(key).setBlobUri("s3://b/v2").build())
            .await()
            .indefinitely());
    var got2 = pointers.get(key).await().indefinitely().orElseThrow();
    assertEquals("s3://b/v2", got2.getBlobUri());
    assertEquals(2L, got2.getVersion());
  }

  @Test
  void multiple_updates_then_delete_removes_record_with_version_gt_1() {
    String key = "accounts/by-id/3/catalog/c1";

    assertTrue(
        pointers
            .compareAndSet(key, 0L, Pointer.newBuilder().setKey(key).setBlobUri("s3://b/1").build())
            .await()
            .indefinitely());

    assertTrue(
        pointers
            .compareAndSet(key, 1L, Pointer.newBuilder().setKey(key).setBlobUri("s3://b/2").build())
            .await()
            .indefinitely());
    assertTrue(
        pointers
            .compareAndSet(key, 2L, Pointer.newBuilder().setKey(key).setBlobUri("s3://b/3").build())
            .await()
            .indefinitely());

    var got = pointers.get(key).await().indefinitely().orElseThrow();
    assertEquals(3L, got.getVersion());

    // delete() is best-effort but should succeed in the single-threaded test.
    assertTrue(pointers.delete(key).await().indefinitely());
    assertEmpty();
  }

  private void assertEmpty() {
    this.pointers.getKvStore().dump("PointerStoreEntityTest.assertEmpty");
    assertTrue(
        pointers
            .listKeysByPrefix("accounts/by-id/", 10, Optional.empty())
            .await()
            .indefinitely()
            .items()
            .isEmpty());
    assertTrue(
        pointers
            .listKeysByPrefix("accounts/by-name/", 10, Optional.empty())
            .await()
            .indefinitely()
            .items()
            .isEmpty());
    assertTrue(
        pointers
            .listKeysByPrefix("accounts/", 10, Optional.empty())
            .await()
            .indefinitely()
            .items()
            .isEmpty());
  }

  @Test
  void list_by_prefix_paginates() {
    for (int i = 0; i < 7; i++) {
      String key = "accounts/by-id/9/p/" + i;
      assertTrue(
          pointers
              .compareAndSet(
                  key, 0L, Pointer.newBuilder().setKey(key).setBlobUri("s3://b/" + i).build())
              .await()
              .indefinitely());
    }

    var p1 =
        pointers.listByPrefix("accounts/by-id/9/p/", 3, Optional.empty()).await().indefinitely();
    assertEquals(3, p1.items().size());
    assertTrue(p1.nextToken().isPresent());

    var p2 = pointers.listByPrefix("accounts/by-id/9/p/", 3, p1.nextToken()).await().indefinitely();
    assertEquals(3, p2.items().size());
    assertTrue(p2.nextToken().isPresent());

    var p3 = pointers.listByPrefix("accounts/by-id/9/p/", 3, p2.nextToken()).await().indefinitely();
    assertEquals(1, p3.items().size());
  }
}
