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
import ai.floedb.floecat.query.rpc.SchemaColumn;
import org.junit.jupiter.api.Test;

class ColumnIdComputerTest {

  @Test
  void canonicalizePath_normalizesElementSegments() {
    assertEquals("a[].b", ColumnIdComputer.canonicalizePath("a.element.b"));
    assertEquals("a[]", ColumnIdComputer.canonicalizePath("a.element"));
    assertEquals("a[].b", ColumnIdComputer.canonicalizePath("a..element..b")); // dot collapse
    assertEquals("a", ColumnIdComputer.canonicalizePath(".a.")); // trim edge dots
  }

  @Test
  void compute_fieldIdPolicy_isJustFieldId() {
    assertEquals(10L, ColumnIdComputer.compute(ColumnIdAlgorithm.CID_FIELD_ID, "x", "x", 1, 10));
    assertEquals(0L, ColumnIdComputer.compute(ColumnIdAlgorithm.CID_FIELD_ID, "x", "x", 1, 0));
    assertEquals(0L, ColumnIdComputer.compute(ColumnIdAlgorithm.CID_FIELD_ID, "x", "x", 1, -1));
  }

  @Test
  void compute_pathOrdinalPolicy_requiresOrdinalAndPathOrName() {
    // ordinal must be > 0
    assertEquals(0L, ColumnIdComputer.compute(ColumnIdAlgorithm.CID_PATH_ORDINAL, "a", "a", 0, 0));
    assertEquals(0L, ColumnIdComputer.compute(ColumnIdAlgorithm.CID_PATH_ORDINAL, "a", "", 0, 0));

    // requires path or name not blank
    assertEquals(0L, ColumnIdComputer.compute(ColumnIdAlgorithm.CID_PATH_ORDINAL, "", "", 1, 0));
  }

  @Test
  void compute_pathOrdinalPolicy_isStableAndDependsOnCanonicalPathAndOrdinal() {
    long id1 =
        ColumnIdComputer.compute(ColumnIdAlgorithm.CID_PATH_ORDINAL, "a", "a.element.b", 3, 0);
    long id2 = ColumnIdComputer.compute(ColumnIdAlgorithm.CID_PATH_ORDINAL, "a", "a[].b", 3, 0);
    assertEquals(id1, id2, "canonicalization should make these equivalent");

    long id3 = ColumnIdComputer.compute(ColumnIdAlgorithm.CID_PATH_ORDINAL, "a", "a[].b", 4, 0);
    assertNotEquals(id1, id3, "ordinal must affect the id");

    long id4 = ColumnIdComputer.compute(ColumnIdAlgorithm.CID_PATH_ORDINAL, "a", "a[].c", 3, 0);
    assertNotEquals(id1, id4, "path must affect the id");

    assertNotEquals(0L, id1);
  }

  @Test
  void withComputedId_setsSchemaColumnIdWhenComputable() {
    SchemaColumn input =
        SchemaColumn.newBuilder()
            .setName("lat")
            .setPhysicalPath("location.lat")
            .setOrdinal(2)
            .build();

    SchemaColumn out = ColumnIdComputer.withComputedId(ColumnIdAlgorithm.CID_PATH_ORDINAL, input);
    assertNotEquals(0L, out.getId());
    assertEquals(input.getName(), out.getName());

    // if it cannot compute, it should return unchanged
    SchemaColumn bad =
        SchemaColumn.newBuilder()
            .setName("lat")
            .setPhysicalPath("location.lat")
            .setOrdinal(0)
            .build();
    SchemaColumn out2 = ColumnIdComputer.withComputedId(ColumnIdAlgorithm.CID_PATH_ORDINAL, bad);
    assertEquals(0L, out2.getId());
  }

  @Test
  void canonicalizePath_withUtf8Characters() {
    // Test that UTF-8 multi-byte characters don't break path canonicalization
    String pathWithCyrillic = "данные.element.value"; // Cyrillic: 3-byte UTF-8
    String canonical = ColumnIdComputer.canonicalizePath(pathWithCyrillic);
    assertEquals("данные[].value", canonical, "UTF-8 paths should canonicalize correctly");

    // Test emoji (4-byte UTF-8 surrogate pair in Java): not typical but should be safe
    String pathWithEmoji = "data🔒.element.sensitive"; // emoji: 4-byte UTF-8
    String canonical2 = ColumnIdComputer.canonicalizePath(pathWithEmoji);
    assertEquals("data🔒[].sensitive", canonical2, "Emoji paths should canonicalize safely");
  }

  @Test
  void compute_pathOrdinalPolicy_withUtf8CharactersStable() {
    // UTF-8 encoding should be deterministic for hash computation
    long id1 = ColumnIdComputer.compute(ColumnIdAlgorithm.CID_PATH_ORDINAL, "name", "назв", 5, 0);
    long id2 = ColumnIdComputer.compute(ColumnIdAlgorithm.CID_PATH_ORDINAL, "name", "назв", 5, 0);
    assertEquals(id1, id2, "UTF-8 paths should hash deterministically across calls (idempotent)");

    // Same ordinal + path should always produce same ID
    assertNotEquals(0L, id1, "Should produce non-zero ID for valid inputs");
  }

  @Test
  void compute_pathOrdinalPolicy_sentinelZeroHandling() {
    // ColumnIdComputer prevents ID=0 by converting to 1 if hash collides
    // This is a guardrail: if hash(path, ordinal) happens to be 0, map to 1
    long idFromLowOrdinal =
        ColumnIdComputer.compute(ColumnIdAlgorithm.CID_PATH_ORDINAL, "x", "x", 1, 0);
    assertNotEquals(0L, idFromLowOrdinal, "Even edge cases should not produce ID=0");
  }

  @Test
  void compute_unspecifiedAlgorithm_defaultsToCidPathOrdinal() {
    // UNSPECIFIED or null should default to CID_PATH_ORDINAL
    long id1 = ColumnIdComputer.compute(ColumnIdAlgorithm.CID_UNSPECIFIED, "col", "col", 5, 0);
    long id2 = ColumnIdComputer.compute(ColumnIdAlgorithm.CID_PATH_ORDINAL, "col", "col", 5, 0);
    assertEquals(id1, id2, "UNSPECIFIED should default to PATH_ORDINAL");

    long id3 = ColumnIdComputer.compute(null, "col", "col", 5, 0);
    assertEquals(id1, id3, "null algorithm should default to PATH_ORDINAL");
  }

  @Test
  void canonicalizePath_mapAndStructPaths() {
    // Map paths: key/value semantics
    assertEquals("m.key", ColumnIdComputer.canonicalizePath("m.key"));
    assertEquals("m.value", ColumnIdComputer.canonicalizePath("m.value"));

    // Nested map in array
    assertEquals(
        "items[].properties.key",
        ColumnIdComputer.canonicalizePath("items.element.properties.key"));

    // Array of maps
    assertEquals(
        "configs[].meta.value", ColumnIdComputer.canonicalizePath("configs.element.meta.value"));
  }
}
