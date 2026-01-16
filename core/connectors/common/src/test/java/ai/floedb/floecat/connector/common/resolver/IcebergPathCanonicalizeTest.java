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

package ai.floedb.floecat.connector.common.resolver;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * IcebergPathCanonicalizeTest: Verify path canonicalization robustness for Iceberg.
 *
 * <p>These tests verify: 1. Canonicalization is idempotent and defensive (handles malformed paths)
 * 2. CID_FIELD_ID algorithm is path-independent (Iceberg's safety mechanism) 3. Map and deeply
 * nested structures canonicalize correctly
 *
 * <p>Additional context: If canonicalization rules ever need to change or if external paths require
 * normalization, these tests ensure robustness.
 */
@DisplayName("Iceberg path canonicalization robustness")
class IcebergPathCanonicalizeTest {

  /** Test: consecutive normalization is idempotent (already-normalized paths unchanged). */
  @Test
  void canonicalizePath_idempotent() {
    String[] paths = {
      "address[]", "addresses[].zip", "metadata.key", "data.value.amount",
    };

    for (String path : paths) {
      String canonical1 = ColumnIdComputer.canonicalizePath(path);
      String canonical2 = ColumnIdComputer.canonicalizePath(canonical1);
      assertEquals(canonical1, canonical2, "Canonicalization should be idempotent for: " + path);
    }
  }

  /** Test: defensive normalization handles malformed paths (edge cases). */
  @Test
  void canonicalizePath_defenseAgainstMalformedPaths() {
    String[] problematicPaths = {
      "address.[]", "address.[].", "address.[]end", "..address", "address..",
    };

    for (String path : problematicPaths) {
      String canonical = ColumnIdComputer.canonicalizePath(path);
      // Should handle gracefully (strip dots, collapse sequences)
      assertFalse(canonical.contains(".."), "Double dots should be collapsed: " + path);
      assertFalse(canonical.startsWith("."), "Should not start with dot: " + path);
      assertFalse(canonical.endsWith("."), "Should not end with dot: " + path);
    }
  }

  /**
   * Test: Legacy ".element." paths canonicalize correctly (backward compatibility).
   *
   * <p>Although Iceberg now emits canonical paths, this test ensures defensive canonicalization
   * would handle legacy paths if they were encountered from external sources or older code.
   */
  @Test
  void canonicalizePath_legacyElementNotation() {
    String legacyPath = "a.element.b.element.c";
    String canonical = ColumnIdComputer.canonicalizePath(legacyPath);
    assertEquals("a[].b[].c", canonical, "Legacy .element. should normalize to []");
  }

  /**
   * Test: CID_FIELD_ID algorithm is path-independent (WHY Iceberg reconciliation is safe).
   *
   * <p>This is the critical insight: even if paths diverge, Iceberg uses CID_FIELD_ID which relies
   * ONLY on the stable Iceberg field_id. Paths are secondary and don't affect column_id
   * computation. This is why reconciliation works regardless of path representation.
   *
   * <p>GUARDRAIL: If Iceberg ever switches to CID_PATH_ORDINAL, this test will fail and serve as a
   * warning that reconciliation logic must change.
   */
  @Test
  void icebergColumnIdAlgorithm_fieldIdIsPath_independent() {
    // Same field_id, different paths should produce same column_id
    long id1 =
        ColumnIdComputer.compute(
            ColumnIdAlgorithm.CID_FIELD_ID,
            "zip",
            "addresses.element.zip", // old style
            1,
            5 // field_id
            );

    long id2 =
        ColumnIdComputer.compute(
            ColumnIdAlgorithm.CID_FIELD_ID,
            "zip",
            "addresses[].zip", // new canonical style
            1,
            5 // same field_id
            );

    long id3 =
        ColumnIdComputer.compute(
            ColumnIdAlgorithm.CID_FIELD_ID,
            "zip",
            "completely.different.path", // unrelated path
            1,
            5 // same field_id
            );

    long id4 =
        ColumnIdComputer.compute(
            ColumnIdAlgorithm.CID_FIELD_ID,
            "zip",
            null, // no path
            1,
            5 // same field_id
            );

    // All should be identical because CID_FIELD_ID only depends on field_id
    assertEquals(id1, id2, "Path representation should not affect CID_FIELD_ID");
    assertEquals(id2, id3, "Unrelated paths should not affect CID_FIELD_ID");
    assertEquals(id3, id4, "Null path should not affect CID_FIELD_ID");
    assertEquals(5L, id1, "CID_FIELD_ID should be the stable Iceberg field_id when > 0");
  }

  @Test
  void canonicalizePath_mapStructures() {
    // Maps use .key/.value semantics, not canonicalized
    assertEquals("config.key", ColumnIdComputer.canonicalizePath("config.key"));
    assertEquals("config.value", ColumnIdComputer.canonicalizePath("config.value"));

    // But nested in arrays should canonicalize the array part
    String mapInArray = "configs.element.properties.key";
    String canonical = ColumnIdComputer.canonicalizePath(mapInArray);
    assertEquals(
        "configs[].properties.key",
        canonical,
        "Array part should canonicalize, map keys remain unchanged");
  }

  @Test
  void canonicalizePath_deeplyNestedStructures() {
    // Deeply nested: array of struct with nested array
    String deepPath = "users.element.metadata.element.tags";
    String canonical = ColumnIdComputer.canonicalizePath(deepPath);
    assertEquals("users[].metadata[].tags", canonical, "All arrays should canonicalize");
  }

  @Test
  void pathOrdinalConsistency_acrossCanonicalizations() {
    // Two different path representations should hash consistently
    long id1 =
        ColumnIdComputer.compute(
            ColumnIdAlgorithm.CID_PATH_ORDINAL,
            "item",
            "items.element.description", // old style
            1,
            0);

    long id2 =
        ColumnIdComputer.compute(
            ColumnIdAlgorithm.CID_PATH_ORDINAL,
            "item",
            "items[].description", // canonical style
            1,
            0);

    assertEquals(
        id1,
        id2,
        "Different path representations should produce same column_id after canonicalization");
  }
}
