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
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ValueEncodersTest {

  @Test
  void decimalBoundsNormalizeTrailingZerosAndNegativeZero() {
    LogicalType decimalType = LogicalType.decimal(10, 5);
    assertEquals("123.45", ValueEncoders.encodeToString(decimalType, new BigDecimal("00123.4500")));
    assertEquals("0", ValueEncoders.encodeToString(decimalType, new BigDecimal("-0.00")));
  }

  @Test
  void timestampAndTimestamptzRejectNumericInputs() {
    LogicalType timestampType = LogicalType.of(LogicalKind.TIMESTAMP);
    LogicalType timestamptzType = LogicalType.of(LogicalKind.TIMESTAMPTZ);

    assertThrows(
        IllegalArgumentException.class,
        () -> ValueEncoders.encodeToString(timestampType, 1_000_000_000L));
    assertThrows(
        IllegalArgumentException.class,
        () -> ValueEncoders.encodeToString(timestamptzType, 1_000_000_000L));
  }

  @Test
  void decodeTimestampReturnsLocalDateTimeAndRejectsZonedStrings() {
    LogicalType timestampType = LogicalType.of(LogicalKind.TIMESTAMP);
    Object fromLocal = ValueEncoders.decodeFromString(timestampType, "2026-02-26T12:34:56.123456");
    assertEquals(LocalDateTime.of(2026, 2, 26, 12, 34, 56, 123_456_000), fromLocal);

    assertThrows(
        IllegalArgumentException.class,
        () -> ValueEncoders.decodeFromString(timestampType, "2026-02-26T12:34:56Z"));
  }

  @Test
  void primitivesEncodeWithCanonicalStrings() {
    assertEquals("true", ValueEncoders.encodeToString(LogicalType.of(LogicalKind.BOOLEAN), true));
    assertEquals("false", ValueEncoders.encodeToString(LogicalType.of(LogicalKind.BOOLEAN), false));

    // All integer sizes collapse to canonical INT (64-bit Long).
    assertEquals(
        "-123", ValueEncoders.encodeToString(LogicalType.of(LogicalKind.INT), (short) -123));
    assertEquals("456", ValueEncoders.encodeToString(LogicalType.of(LogicalKind.INT), 456));
    assertEquals("-789", ValueEncoders.encodeToString(LogicalType.of(LogicalKind.INT), -789L));

    assertEquals("0", ValueEncoders.encodeToString(LogicalType.of(LogicalKind.FLOAT), -0f));
    assertEquals("1.5", ValueEncoders.encodeToString(LogicalType.of(LogicalKind.FLOAT), 1.5f));
    assertEquals("0", ValueEncoders.encodeToString(LogicalType.of(LogicalKind.DOUBLE), -0d));

    LocalDate date = LocalDate.of(2024, 1, 2);
    assertEquals(
        "2024-01-02", ValueEncoders.encodeToString(LogicalType.of(LogicalKind.DATE), date));

    LocalTime time = LocalTime.of(5, 4, 3, 120_000_000);
    assertEquals(
        "05:04:03.12", ValueEncoders.encodeToString(LogicalType.of(LogicalKind.TIME), time));
  }

  @Test
  void temporalPrecisionControlsEncoding() {
    LocalDateTime ts = LocalDateTime.of(2026, 2, 26, 12, 34, 56, 123_456_789);
    LocalTime time = LocalTime.of(1, 2, 3, 456_789_000);
    Instant instant = Instant.parse("2026-02-26T12:34:56.123456789Z");

    LogicalType ts3 = LogicalType.temporal(LogicalKind.TIMESTAMP, 3);
    LogicalType time0 = LogicalType.temporal(LogicalKind.TIME, 0);
    LogicalType tstz3 = LogicalType.temporal(LogicalKind.TIMESTAMPTZ, 3);

    assertEquals("2026-02-26T12:34:56.123", ValueEncoders.encodeToString(ts3, ts));
    assertEquals("01:02:03", ValueEncoders.encodeToString(time0, time));
    assertEquals("2026-02-26T12:34:56.123Z", ValueEncoders.encodeToString(tstz3, instant));
  }

  @Test
  void intEncodingRejectsOverflowAndNonIntegralNumericValues() {
    LogicalType intType = LogicalType.of(LogicalKind.INT);
    assertEquals(
        Long.toString(Long.MAX_VALUE),
        ValueEncoders.encodeToString(intType, BigInteger.valueOf(Long.MAX_VALUE)));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            ValueEncoders.encodeToString(
                intType, BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE)));
    assertThrows(
        IllegalArgumentException.class,
        () -> ValueEncoders.encodeToString(intType, new BigDecimal("123.45")));
  }

  @Test
  void stringBinaryAndUuidEncodeFromMultipleInputs() {
    assertEquals(
        "hello", ValueEncoders.encodeToString(LogicalType.of(LogicalKind.STRING), "hello"));
    assertEquals(
        "hello",
        ValueEncoders.encodeToString(
            LogicalType.of(LogicalKind.STRING), new StringBuilder("hello")));

    byte[] bytes = new byte[] {0x00, 0x7f, (byte) 0xff};
    assertEquals(
        Base64.getEncoder().encodeToString(bytes),
        ValueEncoders.encodeToString(LogicalType.of(LogicalKind.BINARY), bytes));

    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    assertEquals(
        Base64.getEncoder().encodeToString(bytes),
        ValueEncoders.encodeToString(LogicalType.of(LogicalKind.BINARY), buffer));

    UUID uuid = UUID.fromString("f47ac10b-58cc-4372-a567-0e02b2c3d479");
    assertEquals(
        uuid.toString(), ValueEncoders.encodeToString(LogicalType.of(LogicalKind.UUID), uuid));
    assertEquals(
        uuid.toString(),
        ValueEncoders.encodeToString(LogicalType.of(LogicalKind.UUID), uuid.toString()));
  }

  @Test
  void floatSpecialTokensStayLiteral() {
    LogicalType floatType = LogicalType.of(LogicalKind.FLOAT);
    LogicalType doubleType = LogicalType.of(LogicalKind.DOUBLE);
    assertEquals("NaN", ValueEncoders.encodeToString(floatType, Float.NaN));
    assertEquals("Infinity", ValueEncoders.encodeToString(floatType, Float.POSITIVE_INFINITY));
    assertEquals("-Infinity", ValueEncoders.encodeToString(floatType, Float.NEGATIVE_INFINITY));
    assertEquals("NaN", ValueEncoders.encodeToString(doubleType, Double.NaN));
    assertEquals("Infinity", ValueEncoders.encodeToString(doubleType, Double.POSITIVE_INFINITY));
    assertEquals("-Infinity", ValueEncoders.encodeToString(doubleType, Double.NEGATIVE_INFINITY));
  }

  @Test
  void timeRejectsNumericInputs() {
    LogicalType timeType = LogicalType.of(LogicalKind.TIME);
    assertThrows(
        IllegalArgumentException.class, () -> ValueEncoders.encodeToString(timeType, 3_661L));
  }

  @Test
  void dateNumericInterpretsEpochDay() {
    LogicalType dateType = LogicalType.of(LogicalKind.DATE);
    assertEquals("1970-01-02", ValueEncoders.encodeToString(dateType, 1L));
  }

  @Test
  void temporalNumericRejectsFractionalValues() {
    LogicalType dateType = LogicalType.of(LogicalKind.DATE);
    assertThrows(
        IllegalArgumentException.class, () -> ValueEncoders.encodeToString(dateType, 1.25));
  }

  @Test
  void intervalBinaryEncodingRoundTrips() {
    LogicalType intervalType = LogicalType.of(LogicalKind.INTERVAL);
    byte[] payload = new byte[] {1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0};

    String encoded = ValueEncoders.encodeToString(intervalType, payload);
    Object decoded = ValueEncoders.decodeFromString(intervalType, encoded);

    assertEquals(Base64.getEncoder().encodeToString(payload), encoded);
    assertTrue(decoded instanceof byte[]);
    assertEquals(payload.length, ((byte[]) decoded).length);
    for (int i = 0; i < payload.length; i++) {
      assertEquals(payload[i], ((byte[]) decoded)[i]);
    }
  }

  @Test
  void intervalEncodingRejectsNonBinaryAndWrongWidth() {
    LogicalType intervalType = LogicalType.of(LogicalKind.INTERVAL);
    assertThrows(
        IllegalArgumentException.class, () -> ValueEncoders.encodeToString(intervalType, "1 day"));
    assertThrows(
        IllegalArgumentException.class,
        () -> ValueEncoders.encodeToString(intervalType, new byte[] {1, 2, 3}));
  }

  @Test
  void complexTypesRejectMinMaxEncoding() {
    assertThrows(
        IllegalArgumentException.class,
        () -> ValueEncoders.encodeToString(LogicalType.of(LogicalKind.JSON), "{}"));
    assertThrows(
        IllegalArgumentException.class,
        () -> ValueEncoders.encodeToString(LogicalType.of(LogicalKind.ARRAY), List.of(1, 2, 3)));
    assertThrows(
        IllegalArgumentException.class,
        () -> ValueEncoders.encodeToString(LogicalType.of(LogicalKind.MAP), Map.of("k", "v")));
    assertThrows(
        IllegalArgumentException.class,
        () -> ValueEncoders.encodeToString(LogicalType.of(LogicalKind.STRUCT), new Object()));
    assertThrows(
        IllegalArgumentException.class,
        () -> ValueEncoders.encodeToString(LogicalType.of(LogicalKind.VARIANT), "{}"));
  }
}
