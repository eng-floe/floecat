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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Objects;
import java.util.UUID;

public final class ValueEncoders {

  private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ISO_LOCAL_DATE;
  private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ISO_LOCAL_TIME;
  private static final DateTimeFormatter LOCAL_TS_FMT = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
  private static final DateTimeFormatter INSTANT_FMT = DateTimeFormatter.ISO_INSTANT;

  /**
   * Encode {@code value} to the canonical string used by ColumnStats min/max. Each {@link
   * LogicalKind} maps to the format described in {@code floecat/catalog/stats.proto}: booleans as
   * {@code "true"/"false"}, integers as base-10, floats with {@code Float/Double.toString},
   * dates/times in ISO-8601, decimals as plain strings, binaries as base64, etc.
   */
  public static String encodeToString(LogicalType t, Object value) {
    if (value == null) {
      return null;
    }

    Objects.requireNonNull(t, "LogicalType required");

    switch (t.kind()) {
      case BOOLEAN:
        // Boolean bounds are lowercase "true" or "false".
        return Boolean.toString((Boolean) value);
      case INT:
        // All integer sizes collapse to canonical 64-bit INT encoded as base-10.
        if (value instanceof Number n) {
          return Long.toString(Int64Coercions.checkedLong(n));
        }
        if (value instanceof CharSequence s) {
          String str = trimOrNull(s);
          if (str == null) {
            return null;
          }
          return Long.toString(Long.parseLong(str));
        }
        throw new ClassCastException(
            "INT value must be Number or String but was " + value.getClass());
      case FLOAT:
        // Floating bounds follow Float.toString (includes "NaN", "Infinity", "-Infinity")
        // but normalize ±0 to a stable "0".
        if (value instanceof Number n) {
          return canonicalFloat(n.floatValue());
        }
        if (value instanceof CharSequence s) {
          String str = trimOrNull(s);
          if (str == null) {
            return null;
          }
          return canonicalFloat(Float.parseFloat(str));
        }
        throw new ClassCastException(
            "FLOAT value must be Number or String but was " + value.getClass());
      case DOUBLE:
        if (value instanceof Number n) {
          return canonicalDouble(n.doubleValue());
        }
        if (value instanceof CharSequence s) {
          String str = trimOrNull(s);
          if (str == null) {
            return null;
          }
          return canonicalDouble(Double.parseDouble(str));
        }
        throw new ClassCastException(
            "DOUBLE value must be Number or String but was " + value.getClass());

      case DATE:
        {
          // ISO-8601 local date (yyyy-MM-dd) is the canonical form.
          if (value instanceof LocalDate d) {
            return DATE_FMT.format(d);
          }

          if (value instanceof Number n) {
            return DATE_FMT.format(LocalDate.ofEpochDay(Int64Coercions.checkedLong(n)));
          }

          if (value instanceof CharSequence s) {
            return LocalDate.parse(s).toString();
          }

          throw new IllegalArgumentException(
              "DATE must be LocalDate, epoch-day Number, or ISO yyyy-MM-dd String but was: "
                  + value.getClass().getName());
        }

      case TIME:
        {
          // ISO-8601 local time (HH:mm:ss[.fraction]) is emitted; numeric values are not accepted.
          Integer precision = t.temporalPrecision();
          if (value instanceof LocalTime t0) {
            return TemporalCoercions.formatLocalTime(t0, precision);
          }

          if (value instanceof CharSequence s) {
            return TemporalCoercions.formatLocalTime(LocalTime.parse(s.toString()), precision);
          }

          throw new IllegalArgumentException(
              "TIME must be LocalTime or ISO HH:mm:ss[.nnn] String but was: "
                  + value.getClass().getName());
        }

      case TIMESTAMP:
        {
          // TIMESTAMP is timezone-naive and encoded as ISO local date-time (no zone suffix).
          Integer precision = t.temporalPrecision();
          if (value instanceof LocalDateTime ts) {
            return TemporalCoercions.formatLocalDateTime(ts, precision);
          }

          if (value instanceof Instant i) {
            return TemporalCoercions.formatLocalDateTime(
                TemporalCoercions.localDateTimeFromInstantNoTz(i), precision);
          }

          if (value instanceof CharSequence s) {
            return TemporalCoercions.formatLocalDateTime(
                TemporalCoercions.parseTimestampNoTz(s.toString()), precision);
          }

          throw new IllegalArgumentException(
              "TIMESTAMP must be LocalDateTime, Instant, or ISO-8601 local date-time String"
                  + " but was: "
                  + value.getClass().getName());
        }

      case TIMESTAMPTZ:
        {
          // TIMESTAMPTZ is always UTC-normalised and encoded as ISO instant with Z suffix.
          Integer precision = t.temporalPrecision();
          if (value instanceof Instant i) {
            return TemporalCoercions.formatInstantUtc(i, precision);
          }

          if (value instanceof CharSequence s) {
            return TemporalCoercions.formatInstantUtc(
                TemporalCoercions.parseZonedInstant(s.toString()), precision);
          }

          throw new IllegalArgumentException(
              "TIMESTAMPTZ must be Instant or ISO-8601 String but was: "
                  + value.getClass().getName());
        }

      case STRING:
        return asUtf8String(value);
      case UUID:
        if (value instanceof UUID u) {
          return u.toString();
        }
        if (value instanceof CharSequence s) {
          return UUID.fromString(s.toString()).toString();
        }
        throw new IllegalArgumentException(
            "UUID value must be UUID or String but was: " + value.getClass().getName());
      case BINARY:
        return Base64.getEncoder().encodeToString(asBytes(value));
      case DECIMAL:
        return canonicalDecimal((BigDecimal) value);
      case JSON:
        // JSON has no meaningful min/max ordering semantics.
        throw new IllegalArgumentException(
            "min/max encoding is unsupported for non-stats-orderable type " + t.kind().name());
      case INTERVAL:
        return encodeInterval(t, value);
      case ARRAY:
      case MAP:
      case STRUCT:
      case VARIANT:
        // Complex/container types have no stable min/max ordering semantics.
        throw new IllegalArgumentException(
            "min/max encoding is unsupported for complex type " + t.kind().name());

      default:
        return value.toString();
    }
  }

  public static Object decodeFromString(LogicalType t, String encoded) {
    if (encoded == null) {
      return null;
    }

    Objects.requireNonNull(t, "LogicalType required");

    switch (t.kind()) {
      case BOOLEAN:
        return parseBooleanStrict(encoded);
      case INT:
        // All integer sizes decode as canonical 64-bit Long.
        return Long.parseLong(encoded);
      case FLOAT:
        return Float.parseFloat(encoded);
      case DOUBLE:
        return Double.parseDouble(encoded);
      case DATE:
        return LocalDate.parse(encoded, DATE_FMT);
      case TIME:
        return TemporalCoercions.truncateToTemporalPrecision(
            LocalTime.parse(encoded, TIME_FMT), t.temporalPrecision());
      case TIMESTAMP:
        return TemporalCoercions.truncateToTemporalPrecision(
            TemporalCoercions.parseTimestampNoTz(encoded), t.temporalPrecision());
      case TIMESTAMPTZ:
        // TIMESTAMPTZ is always UTC-normalised and decoded as Instant.
        return TemporalCoercions.truncateToTemporalPrecision(
            TemporalCoercions.parseZonedInstant(encoded), t.temporalPrecision());
      case STRING:
        return encoded;
      case JSON:
        // JSON is stored as a UTF-8 string; decoding is identical to STRING.
        return encoded;
      case UUID:
        return UUID.fromString(encoded);
      case BINARY:
        return Base64.getDecoder().decode(encoded);
      case INTERVAL:
        return decodeInterval(t, encoded);
      case ARRAY:
      case MAP:
      case STRUCT:
      case VARIANT:
        // Backward compatibility for previously persisted opaque values.
        return Base64.getDecoder().decode(encoded);
      case DECIMAL:
        return new BigDecimal(encoded);
      default:
        return encoded;
    }
  }

  public static byte[] decodeBinary(String encoded) {
    if (encoded == null) {
      return null;
    }

    return Base64.getDecoder().decode(encoded);
  }

  private static String canonicalFloat(float v) {
    if (v == 0f) {
      return "0";
    }
    return Float.toString(v);
  }

  private static String canonicalDouble(double v) {
    if (v == 0d) {
      return "0";
    }
    return Double.toString(v);
  }

  private static String canonicalDecimal(BigDecimal value) {
    BigDecimal normalized = value.stripTrailingZeros();
    if (normalized.compareTo(BigDecimal.ZERO) == 0) {
      normalized = BigDecimal.ZERO;
    }
    return normalized.toPlainString();
  }

  private static boolean parseBooleanStrict(String raw) {
    String normalized = raw.trim().toLowerCase(java.util.Locale.ROOT);
    return switch (normalized) {
      case "true", "t", "1" -> true;
      case "false", "f", "0" -> false;
      default -> throw new IllegalArgumentException("Invalid BOOLEAN value: " + raw);
    };
  }

  private static byte[] asBytes(Object v) {
    if (v instanceof byte[] arr) {
      return arr;
    }

    if (v instanceof ByteBuffer bb) {
      byte[] dst = new byte[bb.remaining()];
      bb.duplicate().get(dst);
      return dst;
    }

    if (v instanceof LogicalComparators.ByteArrayComparable cmp) {
      return cmp.copy();
    }

    throw new IllegalArgumentException(
        "BINARY value must be byte[] or ByteBuffer: " + v.getClass());
  }

  private static String encodeInterval(LogicalType t, Object value) {
    IntervalRange range = t.intervalRange() == null ? IntervalRange.UNSPECIFIED : t.intervalRange();
    Integer fractionalPrecision = t.intervalFractionalPrecision();
    if (value instanceof CharSequence cs) {
      return normalizeIntervalString(cs.toString(), range, fractionalPrecision);
    }
    if (value instanceof java.time.Period p) {
      if (range == IntervalRange.DAY_TO_SECOND) {
        throw new IllegalArgumentException(
            "INTERVAL DAY TO SECOND does not accept year-month values");
      }
      if (range == IntervalRange.YEAR_TO_MONTH && p.getDays() != 0) {
        throw new IllegalArgumentException(
            "INTERVAL YEAR TO MONTH must not include day components");
      }
      return p.toString();
    }
    if (value instanceof java.time.Duration d) {
      if (range == IntervalRange.YEAR_TO_MONTH) {
        throw new IllegalArgumentException(
            "INTERVAL YEAR TO MONTH does not accept day-time values");
      }
      return truncateDuration(d, fractionalPrecision).toString();
    }
    throw new IllegalArgumentException(
        "INTERVAL value must be ISO-8601 String, Period, or Duration but was: "
            + value.getClass().getName());
  }

  private static Object decodeInterval(LogicalType t, String encoded) {
    IntervalRange range = t.intervalRange() == null ? IntervalRange.UNSPECIFIED : t.intervalRange();
    Integer fractionalPrecision = t.intervalFractionalPrecision();
    String raw = encoded.trim();
    if (raw.isEmpty()) {
      throw new IllegalArgumentException("INTERVAL string must not be blank");
    }
    return switch (range) {
      case YEAR_TO_MONTH -> normalizeYearMonthInterval(raw);
      case DAY_TO_SECOND -> normalizeDayTimeInterval(raw, fractionalPrecision);
      case UNSPECIFIED -> normalizeIntervalString(raw, range, fractionalPrecision);
    };
  }

  private static String normalizeIntervalString(
      String raw, IntervalRange range, Integer fractionalPrecision) {
    String trimmed = raw.trim();
    if (trimmed.isEmpty()) {
      throw new IllegalArgumentException("INTERVAL string must not be blank");
    }
    if (range == IntervalRange.UNSPECIFIED) {
      try {
        return normalizeIntervalAuto(trimmed, fractionalPrecision);
      } catch (RuntimeException e) {
        throw new IllegalArgumentException("INTERVAL string must be ISO-8601", e);
      }
    }
    if (range == IntervalRange.YEAR_TO_MONTH) {
      return normalizeYearMonthInterval(trimmed);
    }
    return normalizeDayTimeInterval(trimmed, fractionalPrecision);
  }

  private static String normalizeYearMonthInterval(String raw) {
    java.time.Period p;
    try {
      p = java.time.Period.parse(raw);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("INTERVAL string must be ISO-8601 year-month", e);
    }
    if (p.getDays() != 0) {
      throw new IllegalArgumentException("INTERVAL YEAR TO MONTH must not include day components");
    }
    return p.toString();
  }

  private static String normalizePeriodInterval(String raw) {
    try {
      return java.time.Period.parse(raw).toString();
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("INTERVAL string must be ISO-8601 year-month", e);
    }
  }

  private static String normalizeDayTimeInterval(String raw, Integer fractionalPrecision) {
    try {
      java.time.Duration duration = java.time.Duration.parse(raw);
      return truncateDuration(duration, fractionalPrecision).toString();
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("INTERVAL string must be ISO-8601 day-time", e);
    }
  }

  private static String normalizeIntervalAuto(String raw, Integer fractionalPrecision) {
    try {
      return normalizePeriodInterval(raw);
    } catch (RuntimeException ignore) {
      // fall through to duration parsing
    }
    try {
      return normalizeDayTimeInterval(raw, fractionalPrecision);
    } catch (RuntimeException ignore) {
      // fall through to combined parsing
    }
    return normalizeMixedInterval(raw, fractionalPrecision);
  }

  private static String normalizeMixedInterval(String raw, Integer fractionalPrecision) {
    int tPos = raw.indexOf('T');
    if (tPos <= 0) {
      throw new IllegalArgumentException("INTERVAL string must be ISO-8601");
    }
    String datePart = raw.substring(0, tPos);
    String timePart = raw.substring(tPos + 1);
    if (timePart.isEmpty()) {
      throw new IllegalArgumentException("INTERVAL string must be ISO-8601");
    }
    java.time.Period p =
        "P".equals(datePart) ? java.time.Period.ZERO : java.time.Period.parse(datePart);
    java.time.Duration d =
        truncateDuration(java.time.Duration.parse("PT" + timePart), fractionalPrecision);
    if (p.isZero()) {
      return d.toString();
    }
    if (d.isZero()) {
      return p.toString();
    }
    return p.toString() + d.toString().substring(1);
  }

  private static java.time.Duration truncateDuration(
      java.time.Duration duration, Integer fractionalPrecision) {
    if (fractionalPrecision == null) {
      return duration;
    }
    int precision = fractionalPrecision;
    if (precision >= 9) {
      return duration;
    }
    int nanos = duration.getNano();
    int factor = pow10(9 - precision);
    int truncated = (nanos / factor) * factor;
    if (truncated == nanos) {
      return duration;
    }
    return java.time.Duration.ofSeconds(duration.getSeconds(), truncated);
  }

  private static int pow10(int exponent) {
    int value = 1;
    for (int i = 0; i < exponent; i++) {
      value *= 10;
    }
    return value;
  }

  private static String asUtf8String(Object v) {
    if (v instanceof String s) {
      return s;
    }

    if (v instanceof CharSequence cs) {
      return cs.toString();
    }

    if (v instanceof byte[] arr) {
      return new String(arr, StandardCharsets.UTF_8);
    }

    if (v instanceof ByteBuffer bb) {
      ByteBuffer dup = bb.duplicate();
      byte[] bytes = new byte[dup.remaining()];
      dup.get(bytes);
      return new String(bytes, StandardCharsets.UTF_8);
    }

    throw new IllegalArgumentException(
        "STRING value must be String, CharSequence, byte[] or ByteBuffer, not " + v.getClass());
  }

  private static String trimOrNull(CharSequence cs) {
    if (cs == null) {
      return null;
    }
    String s = cs.toString().trim();
    return s.isEmpty() ? null : s;
  }
}
