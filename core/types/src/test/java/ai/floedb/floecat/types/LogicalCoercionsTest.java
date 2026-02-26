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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import org.junit.jupiter.api.Test;

class LogicalCoercionsTest {

  @Test
  void coercesTimestampAsTimezoneNaiveLocalDateTime() {
    LogicalType timestampType = LogicalType.of(LogicalKind.TIMESTAMP);
    Object local = LogicalCoercions.coerceStatValue(timestampType, "2026-02-26T12:34:56.123456");
    Object legacyInstant = LogicalCoercions.coerceStatValue(timestampType, "2026-02-26T12:34:56Z");

    assertTrue(local instanceof LocalDateTime);
    assertEquals(LocalDateTime.of(2026, 2, 26, 12, 34, 56, 123_456_000), local);
    assertEquals(LocalDateTime.of(2026, 2, 26, 12, 34, 56), legacyInstant);
  }

  @Test
  void coercesTimestamptzAsUtcInstant() {
    LogicalType timestamptzType = LogicalType.of(LogicalKind.TIMESTAMPTZ);
    Object coerced = LogicalCoercions.coerceStatValue(timestamptzType, "2026-02-26T12:34:56Z");
    assertTrue(coerced instanceof Instant);
    assertEquals(Instant.parse("2026-02-26T12:34:56Z"), coerced);
  }

  @Test
  void coercesTimeNumericValuesWithCanonicalUnitHeuristics() {
    LogicalType timeType = LogicalType.of(LogicalKind.TIME);

    Object fromSecondsString = LogicalCoercions.coerceStatValue(timeType, "3661");
    Object fromMillisString = LogicalCoercions.coerceStatValue(timeType, "3661000");
    Object fromMicrosString = LogicalCoercions.coerceStatValue(timeType, "3661000000");
    Object fromNanosString = LogicalCoercions.coerceStatValue(timeType, "3661000000000");
    Object fromNumber = LogicalCoercions.coerceStatValue(timeType, 3661L);

    LocalTime expected = LocalTime.of(1, 1, 1);
    assertEquals(expected, fromSecondsString);
    assertEquals(expected, fromMillisString);
    assertEquals(expected, fromMicrosString);
    assertEquals(expected, fromNanosString);
    assertEquals(expected, fromNumber);
  }

  @Test
  void intCoercionRejectsOutOfRangeAndFractionalNumbers() {
    LogicalType intType = LogicalType.of(LogicalKind.INT);
    assertEquals(42L, LogicalCoercions.coerceStatValue(intType, BigInteger.valueOf(42L)));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            LogicalCoercions.coerceStatValue(
                intType, BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE)));
    assertThrows(
        IllegalArgumentException.class,
        () -> LogicalCoercions.coerceStatValue(intType, new BigDecimal("1.25")));
  }
}
