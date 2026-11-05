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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;
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
    assertTrue(LogicalComparators.isStatsOrderable(LogicalType.of(LogicalKind.INT)));
    assertTrue(LogicalComparators.isStatsOrderable(LogicalType.of(LogicalKind.STRING)));
    assertFalse(LogicalComparators.isStatsOrderable(LogicalType.of(LogicalKind.JSON)));
    assertFalse(LogicalComparators.isStatsOrderable(LogicalType.of(LogicalKind.INTERVAL)));
    assertFalse(LogicalComparators.isStatsOrderable(LogicalType.of(LogicalKind.ARRAY)));
    assertFalse(LogicalComparators.isStatsOrderable(LogicalType.of(LogicalKind.MAP)));
    assertFalse(LogicalComparators.isStatsOrderable(LogicalType.of(LogicalKind.STRUCT)));
    assertFalse(LogicalComparators.isStatsOrderable(LogicalType.of(LogicalKind.VARIANT)));
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
  void timestampComparisonRejectsZonedString() {
    LogicalType timestampType = LogicalType.of(LogicalKind.TIMESTAMP);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            LogicalComparators.compare(
                timestampType, "2026-02-26T12:34:56", "2026-02-26T12:34:56Z"));
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

  @Test
  void stringsAndUuidsOrderAsTheirUnsignedBytes() {
    LogicalType string = LogicalType.of(LogicalKind.STRING);
    LogicalType uuid = LogicalType.of(LogicalKind.UUID);

    // U+E000 is below U+1F600 in UTF-8, though its UTF-16 unit is above the surrogate's.
    assertTrue(LogicalComparators.compare(string, "\uE000", "\uD83D\uDE00") < 0);
    // 8000... is above 7fff... unsigned, though below it as a signed long.
    assertTrue(
        LogicalComparators.compare(
                uuid,
                UUID.fromString("7fffffff-ffff-ffff-ffff-ffffffffffff"),
                UUID.fromString("80000000-0000-0000-0000-000000000000"))
            < 0);
  }

  @Test
  void anUpperBoundFinerThanItsTypeIsRoundedUp() {
    LogicalType micros = LogicalType.temporal(LogicalKind.TIMESTAMP, 6);
    LocalDateTime nanos = LocalDateTime.of(2026, 1, 1, 0, 0, 0, 1_500);

    assertEquals(
        LocalDateTime.of(2026, 1, 1, 0, 0, 0, 2_000),
        LogicalComparators.normalizeUpper(micros, nanos));
    assertEquals(
        LocalDateTime.of(2026, 1, 1, 0, 0, 0, 1_000), LogicalComparators.normalize(micros, nanos));
    assertEquals(
        LocalDateTime.of(2026, 1, 1, 0, 0, 0, 2_000),
        LogicalComparators.normalizeUpper(micros, "2026-01-01T00:00:00.0000015"));
    // A nanosecond column keeps every digit, whatever the value's spelling.
    assertEquals(
        LocalDateTime.of(2026, 1, 1, 0, 0, 0, 1_500),
        LogicalComparators.normalize(
            LogicalType.temporal(LogicalKind.TIMESTAMP, 9), "2026-01-01T00:00:00.0000015"));
    // Rounding up past midnight leaves TIME's range.
    assertNull(
        LogicalComparators.normalizeUpper(
            LogicalType.temporal(LogicalKind.TIME, 6), LocalTime.of(23, 59, 59, 999_999_500)));
  }

  @Test
  void aStringIsOrderedOnlyAsText() {
    // Bytes would be ordered by their Java identity, "[B@...".
    assertThrows(
        IllegalArgumentException.class,
        () -> LogicalComparators.normalize(LogicalType.of(LogicalKind.STRING), new byte[] {0x61}));
  }
}
