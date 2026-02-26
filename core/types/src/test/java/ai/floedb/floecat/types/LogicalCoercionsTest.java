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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class LogicalCoercionsTest {

  @Test
  void coercesTimestampAsTimezoneNaiveLocalDateTime() {
    LogicalType timestampType = LogicalType.of(LogicalKind.TIMESTAMP);
    Object local = LogicalCoercions.coerceStatValue(timestampType, "2026-02-26T12:34:56.123456");

    assertTrue(local instanceof LocalDateTime);
    assertEquals(LocalDateTime.of(2026, 2, 26, 12, 34, 56, 123_456_000), local);
  }

  @Test
  void timestampCoercionRejectsZonedStringsByDefault() {
    LogicalType timestampType = LogicalType.of(LogicalKind.TIMESTAMP);
    assertThrows(
        IllegalArgumentException.class,
        () -> LogicalCoercions.coerceStatValue(timestampType, "2026-02-26T12:34:56Z"));
  }

  @Test
  void coercesTimestamptzAsUtcInstant() {
    LogicalType timestamptzType = LogicalType.of(LogicalKind.TIMESTAMPTZ);
    Object coerced = LogicalCoercions.coerceStatValue(timestamptzType, "2026-02-26T12:34:56Z");
    assertTrue(coerced instanceof Instant);
    assertEquals(Instant.parse("2026-02-26T12:34:56Z"), coerced);
  }

  @Test
  void timestampCoercionCanConvertZonedStringsWhenPolicyEnabled() {
    LogicalType timestampType = LogicalType.of(LogicalKind.TIMESTAMP);
    String policyKey = "floecat.timestamp_no_tz.policy";
    String zoneKey = "floecat.session.timezone";
    String prevPolicy = System.getProperty(policyKey);
    String prevZone = System.getProperty(zoneKey);
    try {
      System.setProperty(policyKey, "CONVERT_TO_SESSION_ZONE");
      System.setProperty(zoneKey, "UTC");
      Object legacy = LogicalCoercions.coerceStatValue(timestampType, "2026-02-26T12:34:56Z");
      assertEquals(LocalDateTime.of(2026, 2, 26, 12, 34, 56), legacy);
    } finally {
      if (prevPolicy == null) {
        System.clearProperty(policyKey);
      } else {
        System.setProperty(policyKey, prevPolicy);
      }
      if (prevZone == null) {
        System.clearProperty(zoneKey);
      } else {
        System.setProperty(zoneKey, prevZone);
      }
    }
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
  void timeCoercionRejectsFractionalNumber() {
    LogicalType timeType = LogicalType.of(LogicalKind.TIME);
    assertThrows(
        IllegalArgumentException.class, () -> LogicalCoercions.coerceStatValue(timeType, 1.5));
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

  // ---------------------------------------------------------------------------
  // BOOLEAN
  // ---------------------------------------------------------------------------

  @Test
  void booleanCoercionFromBooleanInstance() {
    LogicalType t = LogicalType.of(LogicalKind.BOOLEAN);
    assertEquals(Boolean.TRUE, LogicalCoercions.coerceStatValue(t, true));
    assertEquals(Boolean.FALSE, LogicalCoercions.coerceStatValue(t, false));
  }

  @Test
  void booleanCoercionFromString() {
    LogicalType t = LogicalType.of(LogicalKind.BOOLEAN);
    assertEquals(Boolean.TRUE, LogicalCoercions.coerceStatValue(t, "true"));
    assertEquals(Boolean.FALSE, LogicalCoercions.coerceStatValue(t, "false"));
    assertEquals(Boolean.TRUE, LogicalCoercions.coerceStatValue(t, "1"));
    assertEquals(Boolean.FALSE, LogicalCoercions.coerceStatValue(t, "0"));
  }

  @Test
  void booleanCoercionRejectsGarbageString() {
    LogicalType t = LogicalType.of(LogicalKind.BOOLEAN);
    assertThrows(IllegalArgumentException.class, () -> LogicalCoercions.coerceStatValue(t, "yes"));
  }

  // ---------------------------------------------------------------------------
  // FLOAT / DOUBLE
  // ---------------------------------------------------------------------------

  @Test
  void floatCoercionFromNumber() {
    LogicalType t = LogicalType.of(LogicalKind.FLOAT);
    Object result = LogicalCoercions.coerceStatValue(t, 3.14);
    assertTrue(result instanceof Float);
    assertEquals(3.14f, (Float) result, 0.001f);
  }

  @Test
  void floatCoercionFromString() {
    LogicalType t = LogicalType.of(LogicalKind.FLOAT);
    assertEquals(Float.NaN, LogicalCoercions.coerceStatValue(t, "NaN"));
    assertEquals(Float.POSITIVE_INFINITY, LogicalCoercions.coerceStatValue(t, "Infinity"));
    assertEquals(Float.NEGATIVE_INFINITY, LogicalCoercions.coerceStatValue(t, "-Infinity"));
    assertEquals(1.5f, LogicalCoercions.coerceStatValue(t, "1.5"));
  }

  @Test
  void doubleCoercionFromNumber() {
    LogicalType t = LogicalType.of(LogicalKind.DOUBLE);
    Object result = LogicalCoercions.coerceStatValue(t, 2.718f);
    assertTrue(result instanceof Double);
  }

  @Test
  void doubleCoercionFromString() {
    LogicalType t = LogicalType.of(LogicalKind.DOUBLE);
    assertEquals(Double.NaN, LogicalCoercions.coerceStatValue(t, "NaN"));
    assertEquals(Double.POSITIVE_INFINITY, LogicalCoercions.coerceStatValue(t, "Infinity"));
    assertEquals(1.5, LogicalCoercions.coerceStatValue(t, "1.5"));
  }

  // ---------------------------------------------------------------------------
  // STRING
  // ---------------------------------------------------------------------------

  @Test
  void stringCoercionReturnsToString() {
    LogicalType t = LogicalType.of(LogicalKind.STRING);
    assertEquals("hello", LogicalCoercions.coerceStatValue(t, "hello"));
    assertEquals("42", LogicalCoercions.coerceStatValue(t, 42));
  }

  // ---------------------------------------------------------------------------
  // BINARY
  // ---------------------------------------------------------------------------

  @Test
  void binaryCoercionFromByteArray() {
    LogicalType t = LogicalType.of(LogicalKind.BINARY);
    byte[] input = new byte[] {1, 2, 3};
    assertSame(input, LogicalCoercions.coerceStatValue(t, input));
  }

  @Test
  void binaryCoercionFromHexString() {
    LogicalType t = LogicalType.of(LogicalKind.BINARY);
    byte[] result = (byte[]) LogicalCoercions.coerceStatValue(t, "0xDEADBEEF");
    assertArrayEquals(new byte[] {(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF}, result);
  }

  @Test
  void binaryCoercionRejectsInvalidHex() {
    LogicalType t = LogicalType.of(LogicalKind.BINARY);
    assertThrows(
        IllegalArgumentException.class, () -> LogicalCoercions.coerceStatValue(t, "0xABC"));
    assertThrows(
        IllegalArgumentException.class, () -> LogicalCoercions.coerceStatValue(t, "0xDEADBEG0"));
  }

  // ---------------------------------------------------------------------------
  // UUID
  // ---------------------------------------------------------------------------

  @Test
  void uuidCoercionFromUuidInstance() {
    LogicalType t = LogicalType.of(LogicalKind.UUID);
    UUID u = UUID.randomUUID();
    assertEquals(u, LogicalCoercions.coerceStatValue(t, u));
  }

  @Test
  void uuidCoercionFromString() {
    LogicalType t = LogicalType.of(LogicalKind.UUID);
    String s = "550e8400-e29b-41d4-a716-446655440000";
    Object result = LogicalCoercions.coerceStatValue(t, s);
    assertTrue(result instanceof UUID);
    assertEquals(UUID.fromString(s), result);
  }

  // ---------------------------------------------------------------------------
  // DATE
  // ---------------------------------------------------------------------------

  @Test
  void dateCoercionFromEpochDay() {
    LogicalType t = LogicalType.of(LogicalKind.DATE);
    // epoch day 0 = 1970-01-01
    assertEquals(LocalDate.of(1970, 1, 1), LogicalCoercions.coerceStatValue(t, 0L));
  }

  @Test
  void dateCoercionRejectsFractionalNumber() {
    LogicalType t = LogicalType.of(LogicalKind.DATE);
    assertThrows(IllegalArgumentException.class, () -> LogicalCoercions.coerceStatValue(t, 1.25));
  }

  @Test
  void dateCoercionFromString() {
    LogicalType t = LogicalType.of(LogicalKind.DATE);
    assertEquals(LocalDate.of(2026, 2, 26), LogicalCoercions.coerceStatValue(t, "2026-02-26"));
  }

  // ---------------------------------------------------------------------------
  // DECIMAL
  // ---------------------------------------------------------------------------

  @Test
  void decimalCoercionFromBigDecimal() {
    LogicalType t = LogicalType.decimal(10, 2);
    BigDecimal bd = new BigDecimal("123.45");
    assertSame(bd, LogicalCoercions.coerceStatValue(t, bd));
  }

  @Test
  void decimalCoercionFromString() {
    LogicalType t = LogicalType.decimal(10, 2);
    Object result = LogicalCoercions.coerceStatValue(t, "123.45");
    assertTrue(result instanceof BigDecimal);
    assertEquals(new BigDecimal("123.45"), result);
  }

  @Test
  void decimalCoercionFromLong() {
    LogicalType t = LogicalType.decimal(18, 0);
    Object result = LogicalCoercions.coerceStatValue(t, 42L);
    assertTrue(result instanceof BigDecimal);
    assertEquals(new BigDecimal("42"), result);
  }

  // ---------------------------------------------------------------------------
  // Complex / semi-structured types — pass-through
  // ---------------------------------------------------------------------------

  @Test
  void intervalPassesThroughUnchanged() {
    LogicalType t = LogicalType.of(LogicalKind.INTERVAL);
    Object sentinel = new Object();
    assertSame(sentinel, LogicalCoercions.coerceStatValue(t, sentinel));
  }

  @Test
  void jsonPassesThroughUnchanged() {
    LogicalType t = LogicalType.of(LogicalKind.JSON);
    String json = "{\"a\":1}";
    assertSame(json, LogicalCoercions.coerceStatValue(t, json));
  }

  @Test
  void complexTypesPassThroughUnchanged() {
    List<LogicalType> complexTypes =
        List.of(
            LogicalType.of(LogicalKind.ARRAY),
            LogicalType.of(LogicalKind.MAP),
            LogicalType.of(LogicalKind.STRUCT),
            LogicalType.of(LogicalKind.VARIANT));
    Object sentinel = new Object();
    for (LogicalType t : complexTypes) {
      assertSame(
          sentinel,
          LogicalCoercions.coerceStatValue(t, sentinel),
          "Expected pass-through for " + t.kind());
    }
  }

  // ---------------------------------------------------------------------------
  // Null
  // ---------------------------------------------------------------------------

  @Test
  void nullInputReturnsNull() {
    assertNull(LogicalCoercions.coerceStatValue(LogicalType.of(LogicalKind.STRING), null));
    assertNull(LogicalCoercions.coerceStatValue(LogicalType.of(LogicalKind.INT), null));
  }
}
