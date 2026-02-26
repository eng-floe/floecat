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

package ai.floedb.floecat.types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class LogicalComparatorsTest {

  @Test
  void compareSupportsOrderableTypes() {
    LogicalType intType = LogicalType.of(LogicalKind.INT);

    assertTrue(LogicalComparators.compare(intType, 1L, 2L) < 0);
    assertEquals(0, LogicalComparators.compare(intType, 3L, 3L));
  }

  @Test
  void compareRejectsNonOrderableType() {
    LogicalType jsonType = LogicalType.of(LogicalKind.JSON);

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> LogicalComparators.compare(jsonType, "{\"a\":1}", "{\"b\":2}"));
    assertTrue(ex.getMessage().contains("not orderable"));
  }

  @Test
  void exposesOrderableClassification() {
    assertTrue(LogicalComparators.isOrderable(LogicalType.of(LogicalKind.INT)));
    assertTrue(LogicalComparators.isOrderable(LogicalType.of(LogicalKind.STRING)));
    assertFalse(LogicalComparators.isOrderable(LogicalType.of(LogicalKind.JSON)));
    assertFalse(LogicalComparators.isOrderable(LogicalType.of(LogicalKind.INTERVAL)));
    assertFalse(LogicalComparators.isOrderable(LogicalType.of(LogicalKind.ARRAY)));
    assertFalse(LogicalComparators.isOrderable(LogicalType.of(LogicalKind.MAP)));
    assertFalse(LogicalComparators.isOrderable(LogicalType.of(LogicalKind.STRUCT)));
    assertFalse(LogicalComparators.isOrderable(LogicalType.of(LogicalKind.VARIANT)));
  }
}
