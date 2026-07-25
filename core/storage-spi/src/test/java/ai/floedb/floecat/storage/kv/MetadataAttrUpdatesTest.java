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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Argument rules of {@link MetadataAttrUpdates#validate}, which both stores delegate to so that a
 * caller mistake is rejected identically — and synchronously, before any request is issued —
 * regardless of backend. The store-level behaviors (absent record, non-empty value, incrementing a
 * string) belong to the two {@code KvStoreContractTest} classes; this pins only the shared
 * front-door validation, including the branches no contract test reaches.
 */
public class MetadataAttrUpdatesTest {

  private static final KvStore.Key KEY = new KvStore.Key("pk1", "sk1");

  @Test
  void accepts_sets_only() {
    assertDoesNotThrow(
        () -> MetadataAttrUpdates.validate(KEY, Map.of("a", AttrValue.of("x")), Map.of()));
  }

  @Test
  void accepts_increments_only() {
    assertDoesNotThrow(() -> MetadataAttrUpdates.validate(KEY, Map.of(), Map.of("n", 1L)));
  }

  @Test
  void rejects_null_key() {
    assertMessageContains(
        "key", () -> MetadataAttrUpdates.validate(null, Map.of("a", AttrValue.of("x")), Map.of()));
  }

  @Test
  void rejects_null_sets_map() {
    assertMessageContains("sets", () -> MetadataAttrUpdates.validate(KEY, null, Map.of("n", 1L)));
  }

  @Test
  void rejects_null_increments_map() {
    assertMessageContains(
        "increments",
        () -> MetadataAttrUpdates.validate(KEY, Map.of("a", AttrValue.of("x")), null));
  }

  @Test
  void rejects_both_maps_empty() {
    // A no-op update would still advance the version, so it is a caller error rather than a no-op.
    assertMessageContains("non-empty", () -> MetadataAttrUpdates.validate(KEY, Map.of(), Map.of()));
  }

  @Test
  void rejects_every_structural_attr_name_in_sets() {
    for (String name : KvAttributes.STRUCTURAL_ATTRS) {
      assertMessageContains(
          name, () -> MetadataAttrUpdates.validate(KEY, Map.of(name, AttrValue.of("x")), Map.of()));
    }
  }

  @Test
  void rejects_every_structural_attr_name_in_increments() {
    for (String name : KvAttributes.STRUCTURAL_ATTRS) {
      assertMessageContains(
          name, () -> MetadataAttrUpdates.validate(KEY, Map.of(), Map.of(name, 1L)));
    }
  }

  @Test
  void rejects_same_attr_in_both_maps() {
    assertMessageContains(
        "ambiguous",
        () ->
            MetadataAttrUpdates.validate(KEY, Map.of("dup", AttrValue.of("x")), Map.of("dup", 1L)));
  }

  @Test
  void rejects_blank_attr_name() {
    for (String name : new String[] {"", " ", "\t"}) {
      assertMessageContains(
          "blank",
          () -> MetadataAttrUpdates.validate(KEY, Map.of(name, AttrValue.of("x")), Map.of()));
    }
  }

  @Test
  void rejects_null_attr_name() {
    // Map.of rejects null keys, so the null-name branch is only reachable via a HashMap.
    Map<String, AttrValue> sets = new HashMap<>();
    sets.put(null, AttrValue.of("x"));
    assertMessageContains("blank", () -> MetadataAttrUpdates.validate(KEY, sets, Map.of()));
  }

  @Test
  void rejects_null_attr_value_in_sets() {
    Map<String, AttrValue> sets = new HashMap<>();
    sets.put("a", null);
    assertMessageContains("null value", () -> MetadataAttrUpdates.validate(KEY, sets, Map.of()));
  }

  @Test
  void rejects_null_increment_amount() {
    Map<String, Long> increments = new HashMap<>();
    increments.put("n", null);
    assertMessageContains(
        "null value", () -> MetadataAttrUpdates.validate(KEY, Map.of(), increments));
  }

  private static void assertMessageContains(
      String needle, org.junit.jupiter.api.function.Executable call) {
    IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, call);
    assertTrue(
        thrown.getMessage() != null && thrown.getMessage().contains(needle),
        "expected message to contain \"" + needle + "\" but was: " + thrown.getMessage());
  }
}
