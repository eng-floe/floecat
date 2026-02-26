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
import java.time.ZoneOffset;
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
          // ISO-8601 local time (HH:mm:ss[.fraction]) is emitted; numeric values map to
          // seconds/milliseconds/microseconds/nanoseconds heuristically. Canonical TIME is
          // microsecond precision, so truncate any sub-micro nanos.
          if (value instanceof LocalTime t0) {
            return TIME_FMT.format(TemporalCoercions.truncateToMicros(t0));
          }

          if (value instanceof Number n) {
            long dayNanos = TemporalCoercions.timeNanosOfDay(Int64Coercions.checkedLong(n));
            long micros = Math.floorDiv(dayNanos, 1_000L);
            return TIME_FMT.format(LocalTime.ofNanoOfDay(micros * 1_000L));
          }
          if (value instanceof CharSequence s) {
            return TIME_FMT.format(
                TemporalCoercions.truncateToMicros(LocalTime.parse(s.toString())));
          }

          throw new IllegalArgumentException(
              "TIME must be LocalTime, Number (s/ms/µs/ns), or ISO HH:mm:ss[.nnn] String but was: "
                  + value.getClass().getName());
        }

      case TIMESTAMP:
        {
          // TIMESTAMP is timezone-naive and encoded as ISO local date-time (no zone suffix).
          if (value instanceof LocalDateTime ts) {
            return LOCAL_TS_FMT.format(TemporalCoercions.truncateToMicros(ts));
          }

          if (value instanceof Instant i) {
            return LOCAL_TS_FMT.format(
                TemporalCoercions.truncateToMicros(LocalDateTime.ofInstant(i, ZoneOffset.UTC)));
          }

          if (value instanceof Number n) {
            return LOCAL_TS_FMT.format(
                TemporalCoercions.localDateTimeFromNumber(Int64Coercions.checkedLong(n)));
          }

          if (value instanceof CharSequence s) {
            return LOCAL_TS_FMT.format(TemporalCoercions.parseTimestampNoTz(s.toString()));
          }

          throw new IllegalArgumentException(
              "TIMESTAMP must be LocalDateTime, Instant, numeric seconds/millis/micros/nanos,"
                  + " or ISO-8601 local/instant String"
                  + " but was: "
                  + value.getClass().getName());
        }

      case TIMESTAMPTZ:
        {
          // TIMESTAMPTZ is always UTC-normalised and encoded as ISO instant with Z suffix.
          if (value instanceof Instant i) {
            return INSTANT_FMT.format(TemporalCoercions.truncateToMicros(i));
          }

          if (value instanceof Number n) {
            return INSTANT_FMT.format(
                TemporalCoercions.instantFromNumber(Int64Coercions.checkedLong(n)));
          }

          if (value instanceof CharSequence s) {
            return INSTANT_FMT.format(
                TemporalCoercions.truncateToMicros(Instant.parse(s.toString())));
          }

          throw new IllegalArgumentException(
              "TIMESTAMPTZ must be Instant, numeric seconds/millis/micros/nanos, or ISO-8601"
                  + " String but was: "
                  + value.getClass().getName());
        }

      case STRING:
        return asUtf8String(value);
      case UUID:
        return ((UUID) value).toString();
      case BINARY:
        return Base64.getEncoder().encodeToString(asBytes(value));
      case DECIMAL:
        return canonicalDecimal((BigDecimal) value);
      case JSON:
        // JSON has no meaningful min/max ordering semantics.
        throw new IllegalArgumentException(
            "min/max encoding is unsupported for non-orderable type " + t.kind().name());
      case INTERVAL:
        // INTERVAL is encoded as Base64 of its 12-byte wire representation (months/days/millis).
        return Base64.getEncoder().encodeToString(asIntervalBytes(value));
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
        return TemporalCoercions.truncateToMicros(LocalTime.parse(encoded, TIME_FMT));
      case TIMESTAMP:
        return TemporalCoercions.parseTimestampNoTz(encoded);
      case TIMESTAMPTZ:
        // TIMESTAMPTZ is always UTC-normalised and decoded as Instant.
        return TemporalCoercions.truncateToMicros(Instant.parse(encoded));
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

  private static byte[] asIntervalBytes(Object v) {
    byte[] bytes;
    if (v instanceof byte[] arr) {
      bytes = arr;
    } else if (v instanceof ByteBuffer bb) {
      ByteBuffer dup = bb.duplicate();
      bytes = new byte[dup.remaining()];
      dup.get(bytes);
    } else if (v instanceof LogicalComparators.ByteArrayComparable cmp) {
      bytes = cmp.copy();
    } else {
      throw new IllegalArgumentException(
          "INTERVAL value must be byte[] or ByteBuffer, not " + v.getClass());
    }
    if (bytes.length != 12) {
      throw new IllegalArgumentException(
          "INTERVAL value must be exactly 12 bytes (months/days/millis), got " + bytes.length);
    }
    return bytes;
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
