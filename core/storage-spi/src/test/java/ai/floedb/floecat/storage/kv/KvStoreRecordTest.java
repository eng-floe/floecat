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
package ai.floedb.floecat.storage.kv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Constructor rules of {@link KvStore.Record} that every backend relies on: an attribute may not
 * borrow a structural field's name, and a null attrs map normalizes rather than blowing up.
 */
public class KvStoreRecordTest {

  @Test
  void record_rejects_every_structural_attr_name() {
    for (String name : KvAttributes.STRUCTURAL_ATTRS) {
      Map<String, AttrValue> attrs = Map.of(name, AttrValue.of("x"));
      IllegalArgumentException thrown =
          assertThrows(IllegalArgumentException.class, () -> newRecord(attrs));
      assertTrue(thrown.getMessage().contains(name), thrown.getMessage());
    }
  }

  @Test
  void record_rejects_structural_attr_name_mixed_in_with_ordinary_ones() {
    Map<String, AttrValue> attrs =
        Map.of("ordinary", AttrValue.of("x"), KvAttributes.ATTR_VERSION, AttrValue.of(7L));
    assertThrows(IllegalArgumentException.class, () -> newRecord(attrs));
  }

  @Test
  void record_null_attrs_becomes_empty_map() {
    KvStore.Record rec = newRecord(null);
    assertNotNull(rec.attrs());
    assertEquals(Map.of(), rec.attrs());
  }

  @Test
  void record_copies_attrs_so_later_caller_mutation_is_invisible() {
    Map<String, AttrValue> attrs = new HashMap<>();
    attrs.put("ordinary", AttrValue.of("x"));

    KvStore.Record rec = newRecord(attrs);
    attrs.put("added", AttrValue.of("y"));

    assertEquals(Map.of("ordinary", AttrValue.of("x")), rec.attrs());
  }

  private static KvStore.Record newRecord(Map<String, AttrValue> attrs) {
    return new KvStore.Record(new KvStore.Key("pk", "sk"), "KIND", new byte[0], attrs, 1L);
  }
}
