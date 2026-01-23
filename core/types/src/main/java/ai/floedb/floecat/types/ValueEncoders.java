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
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Objects;
import java.util.UUID;

public final class ValueEncoders {

  private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ISO_LOCAL_DATE;
  private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ISO_LOCAL_TIME;
  private static final DateTimeFormatter INSTANT_FMT = DateTimeFormatter.ISO_INSTANT;
  private static final long NANOS_PER_DAY = 86_400_000_000_000L;
  private static final long TIME_SECONDS_THRESHOLD = 86_400L;
  private static final long TIME_MILLIS_THRESHOLD = 86_400_000L;
  private static final long TIME_MICROS_THRESHOLD = 86_400_000_000L;

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
      case INT16:
        if (value instanceof Number n) {
          // Signed integer siblings are emitted as canonical base-10 digits, no leading zeros.
          return Short.toString(n.shortValue());
        }
        if (value instanceof CharSequence s) {
          String str = trimOrNull(s);
          if (str == null) {
            return null;
          }
          return Short.toString(Short.parseShort(str));
        }
        throw new ClassCastException(
            "INT16 value must be Number or String but was " + value.getClass());
      case INT32:
        if (value instanceof Number n) {
          // INT32 bounds use base-10 decimals with optional '-' sign.
          return Integer.toString(n.intValue());
        }
        if (value instanceof CharSequence s) {
          String str = trimOrNull(s);
          if (str == null) {
            return null;
          }
          return Integer.toString(Integer.parseInt(str));
        }
        throw new ClassCastException(
            "INT32 value must be Number or String but was " + value.getClass());
      case INT64:
        if (value instanceof Number n) {
          // INT64 bounds follow the same base-10 encoding as INT32.
          return Long.toString(n.longValue());
        }
        if (value instanceof CharSequence s) {
          String str = trimOrNull(s);
          if (str == null) {
            return null;
          }
          return Long.toString(Long.parseLong(str));
        }
        throw new ClassCastException(
            "INT64 value must be Number or String but was " + value.getClass());
      case FLOAT32:
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
            "FLOAT32 value must be Number or String but was " + value.getClass());
      case FLOAT64:
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
            "FLOAT64 value must be Number or String but was " + value.getClass());

      case DATE:
        {
          // ISO-8601 local date (yyyy-MM-dd) is the canonical form.
          if (value instanceof LocalDate d) {
            return DATE_FMT.format(d);
          }

          if (value instanceof Number n) {
            return DATE_FMT.format(LocalDate.ofEpochDay(n.longValue()));
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
          // seconds/milliseconds/microseconds/nanoseconds heuristically.
          if (value instanceof LocalTime t0) {
            return TIME_FMT.format(t0);
          }

          if (value instanceof Number n) {
            long v = n.longValue();
            long abs = Math.abs(v);
            long nanos;
            if (abs < TIME_SECONDS_THRESHOLD) {
              nanos = v * 1_000_000_000L;
            } else if (abs < TIME_MILLIS_THRESHOLD) {
              nanos = v * 1_000_000L;
            } else if (abs < TIME_MICROS_THRESHOLD) {
              nanos = v * 1_000L;
            } else {
              nanos = v;
            }

            long dayNanos = Math.floorMod(nanos, NANOS_PER_DAY);
            return TIME_FMT.format(LocalTime.ofNanoOfDay(dayNanos));
          }
          if (value instanceof CharSequence s) {
            return LocalTime.parse(s).toString();
          }

          throw new IllegalArgumentException(
              "TIME must be LocalTime, Number (s/ms/µs/ns), or ISO HH:mm:ss[.nnn] String but was: "
                  + value.getClass().getName());
        }

      case TIMESTAMP:
        {
          // UTC ISO-8601 instant with Z suffix; numeric inputs are interpreted heuristically
          // (seconds/millis/micros/nanos) but the emitted string is always ISO.
          if (value instanceof Instant i) {
            return INSTANT_FMT.format(i);
          }

          if (value instanceof Number n) {
            return INSTANT_FMT.format(instantFromNumber(n.longValue()));
          }

          if (value instanceof CharSequence s) {
            return INSTANT_FMT.format(Instant.parse(s.toString()));
          }

          throw new IllegalArgumentException(
              "TIMESTAMP must be Instant, numeric seconds/millis/micros/nanos, or ISO-8601 String"
                  + " but was: "
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
        return Boolean.parseBoolean(encoded);
      case INT16:
        return Short.parseShort(encoded);
      case INT32:
        return Integer.parseInt(encoded);
      case INT64:
        return Long.parseLong(encoded);
      case FLOAT32:
        return Float.parseFloat(encoded);
      case FLOAT64:
        return Double.parseDouble(encoded);
      case DATE:
        return LocalDate.parse(encoded, DATE_FMT);
      case TIME:
        return LocalTime.parse(encoded, TIME_FMT);
      case TIMESTAMP:
        return Instant.parse(encoded);
      case STRING:
        return encoded;
      case UUID:
        return UUID.fromString(encoded);
      case BINARY:
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

  private static final long NANOS_THRESHOLD = 1_000_000_000_000_000_000L;
  private static final long MICROS_THRESHOLD = 1_000_000_000_000_000L;
  private static final long MILLIS_THRESHOLD = 1_000_000_000_000L;

  private static Instant instantFromNumber(long v) {
    long av = Math.abs(v);
    if (av >= NANOS_THRESHOLD) {
      long secs = Math.floorDiv(v, 1_000_000_000L);
      int nanos = (int) Math.floorMod(v, 1_000_000_000L);
      return Instant.ofEpochSecond(secs, nanos);
    } else if (av >= MICROS_THRESHOLD) {
      long secs = Math.floorDiv(v, 1_000_000L);
      long micros = Math.floorMod(v, 1_000_000L);
      return Instant.ofEpochSecond(secs, (int) (micros * 1_000L));
    } else if (av >= MILLIS_THRESHOLD) {
      return Instant.ofEpochMilli(v);
    } else {
      return Instant.ofEpochSecond(v);
    }
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
