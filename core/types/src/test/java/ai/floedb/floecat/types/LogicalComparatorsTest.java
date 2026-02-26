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

import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDateTime;
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

  @Test
  void timestampAndTimestamptzNormalizeToDifferentTemporalTypes() {
    LogicalType timestampType = LogicalType.of(LogicalKind.TIMESTAMP);
    LogicalType timestamptzType = LogicalType.of(LogicalKind.TIMESTAMPTZ);

    Object ts = LogicalComparators.normalize(timestampType, "2026-02-26T12:34:56.123456");
    Object tstz = LogicalComparators.normalize(timestamptzType, "2026-02-26T12:34:56.123456Z");

    assertTrue(ts instanceof LocalDateTime);
    assertTrue(tstz instanceof Instant);
  }

  @Test
  void timestampComparisonAcceptsLegacyInstantString() {
    LogicalType timestampType = LogicalType.of(LogicalKind.TIMESTAMP);
    assertEquals(
        0,
        LogicalComparators.compare(timestampType, "2026-02-26T12:34:56", "2026-02-26T12:34:56Z"));
  }

  @Test
  void intNormalizeRejectsOutOfRangeValues() {
    LogicalType intType = LogicalType.of(LogicalKind.INT);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            LogicalComparators.normalize(
                intType, BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE)));
  }
}
