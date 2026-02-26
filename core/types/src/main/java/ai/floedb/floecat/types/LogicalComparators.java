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
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Base64;
import java.util.UUID;

/**
 * Provides ordering and normalisation utilities for FloeCat canonical logical types.
 *
 * <p>Used primarily by statistics builders that need to compare encoded min/max values stored as
 * strings or objects. Comparisons are performed on <em>normalised</em> Java values (e.g., all
 * integers are promoted to {@link Long}, all UTC timestamps to {@link java.time.Instant}) so that
 * histogram builders can order them without deserialising raw binary payloads.
 *
 * <p><b>Complex and semi-structured types</b> ({@link LogicalKind#INTERVAL}, {@link
 * LogicalKind#JSON}, {@link LogicalKind#ARRAY}, {@link LogicalKind#MAP}, {@link
 * LogicalKind#STRUCT}, {@link LogicalKind#VARIANT}) have no meaningful min/max ordering. {@link
 * #isOrderable(LogicalType)} returns {@code false} for these kinds, and {@link
 * #normalize(LogicalType, Object)} returns {@code null} rather than throwing.
 */
public final class LogicalComparators {

  /**
   * Returns {@code true} iff values of the given type can be meaningfully ordered.
   *
   * <p>All scalar numeric, string, binary, UUID, and temporal types (except INTERVAL) are
   * orderable. Complex and semi-structured types are not.
   *
   * @param t the logical type to test (null → {@code false})
   * @return {@code true} if the type is orderable
   */
  public static boolean isOrderable(LogicalType t) {
    if (t == null) {
      return false;
    }
    return switch (t.kind()) {
      case BOOLEAN,
          INT,
          FLOAT,
          DOUBLE,
          DECIMAL,
          STRING,
          UUID,
          BINARY,
          DATE,
          TIME,
          TIMESTAMP,
          TIMESTAMPTZ ->
          true;
      case INTERVAL, JSON, ARRAY, MAP, STRUCT, VARIANT -> false;
    };
  }

  /**
   * Compares two values of the given type after normalisation.
   *
   * <p>Both values are normalised to their canonical Java representation (via {@link
   * #normalize(LogicalType, Object)}) before comparison. The returned int follows the standard
   * {@link Comparable#compareTo} contract: negative, zero, or positive.
   *
   * @param t the logical type governing comparison semantics
   * @param a left-hand value (null treated as "less than everything")
   * @param b right-hand value (null treated as "less than everything")
   * @return comparison result
   * @throws IllegalArgumentException if the type is not orderable
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public static int compare(LogicalType t, Object a, Object b) {
    if (a == b) {
      return 0;
    }

    if (a == null) {
      return -1;
    }

    if (b == null) {
      return 1;
    }

    if (!isOrderable(t)) {
      String kind = (t == null) ? "<null>" : t.kind().name();
      throw new IllegalArgumentException("Logical type is not orderable: " + kind);
    }

    Object na = normalize(t, a);
    Object nb = normalize(t, b);

    if (!(na instanceof Comparable<?> c)) {
      throw new IllegalArgumentException(
          "Normalized value is not Comparable for type "
              + t.kind().name()
              + ": "
              + na.getClass().getName());
    }

    try {
      return ((Comparable) c).compareTo(nb);
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          "Incompatible normalized values for type "
              + t.kind().name()
              + ": "
              + na.getClass().getName()
              + " vs "
              + nb.getClass().getName(),
          e);
    }
  }

  /**
   * Normalises a raw stat value to its canonical Java type for ordering.
   *
   * <p>Examples: any {@link Number} for INT → {@link Long}; string for TIMESTAMP → {@link
   * java.time.LocalDateTime}; string for TIMESTAMPTZ → {@link java.time.Instant}. For unorderable
   * kinds (INTERVAL, JSON, ARRAY, MAP, STRUCT, VARIANT), returns {@code null}.
   *
   * @param t the logical type
   * @param v the raw value (may be a Java primitive wrapper, String, or byte array)
   * @return normalised value, or {@code null} if the kind is not orderable
   */
  public static Object normalize(LogicalType t, Object v) {
    switch (t.kind()) {
      case BOOLEAN:
        return (Boolean) v;

      case INT:
        // All integer sizes collapse to canonical 64-bit Long.
        if (v instanceof Number n) {
          return Int64Coercions.checkedLong(n);
        }
        throw typeErr("INT", v);

      case FLOAT:
        return (v instanceof Float) ? v : ((Number) v).floatValue();

      case DOUBLE:
        return (v instanceof Double) ? v : ((Number) v).doubleValue();

      case DECIMAL:
        if (v instanceof BigDecimal bd) {
          return bd;
        }

        if (v instanceof CharSequence s) {
          return new BigDecimal(s.toString());
        }

        if (v instanceof Number n) {
          return new BigDecimal(n.toString());
        }

        throw typeErr("DECIMAL", v);

      case STRING:
        return (v instanceof CharSequence cs) ? cs.toString() : v.toString();

      case UUID:
        return (v instanceof UUID u) ? u : UUID.fromString(v.toString());

      case BINARY:
        byte[] bytes;
        if (v instanceof byte[] arr) {
          bytes = arr;
        } else if (v instanceof ByteBuffer bb) {
          var dup = bb.duplicate();
          bytes = new byte[dup.remaining()];
          dup.get(bytes);
        } else if (v instanceof CharSequence s) {
          bytes = Base64.getDecoder().decode(s.toString());
        } else {
          throw typeErr("BINARY", v);
        }
        return new ByteArrayComparable(bytes);

      case DATE:
        if (v instanceof LocalDate d) {
          return d;
        }

        if (v instanceof Number n) {
          return LocalDate.ofEpochDay(n.longValue());
        }

        if (v instanceof CharSequence s) {
          return LocalDate.parse(s.toString());
        }

        throw typeErr("DATE", v);

      case TIME:
        {
          if (v instanceof LocalTime t0) {
            return t0;
          }

          if (v instanceof CharSequence s) {
            return LocalTime.parse(s.toString());
          }

          if (v instanceof Number n) {
            long nanos = toTimeNanos(n.longValue());
            long day = 86_400_000_000_000L;
            long norm = Math.floorMod(nanos, day);
            return LocalTime.ofNanoOfDay(norm);
          }
          throw typeErr("TIME", v);
        }

      case TIMESTAMP:
        {
          if (v instanceof LocalDateTime ts) {
            return ts;
          }

          if (v instanceof Instant i) {
            return LocalDateTime.ofInstant(i, ZoneOffset.UTC);
          }

          if (v instanceof CharSequence s) {
            return parseTimestampNoTz(s.toString());
          }

          if (v instanceof Number n) {
            return localDateTimeFromNumber(n.longValue());
          }

          throw typeErr("TIMESTAMP", v);
        }

      case TIMESTAMPTZ:
        {
          // TIMESTAMPTZ is always UTC-normalised and compared as Instant.
          if (v instanceof Instant i) {
            return i;
          }

          if (v instanceof CharSequence s) {
            return Instant.parse(s.toString());
          }

          if (v instanceof Number n) {
            return instantFromNumber(n.longValue());
          }

          throw typeErr("TIMESTAMPTZ", v);
        }

        // INTERVAL, JSON, and complex types (ARRAY, MAP, STRUCT, VARIANT) have no meaningful
        // min/max stats ordering; callers should check for null before comparing.
    }
    return null;
  }

  private static IllegalArgumentException typeErr(String kind, Object v) {
    return new IllegalArgumentException(
        kind + " compare expects canonical types, got: " + v.getClass().getName());
  }

  private static long toTimeNanos(long v) {
    long abs = Math.abs(v);
    if (abs < 86_400L) {
      return v * 1_000_000_000L;
    }

    if (abs < 86_400_000L) {
      return v * 1_000_000L;
    }

    if (abs < 86_400_000_000L) {
      return v * 1_000L;
    }

    return v;
  }

  private static Instant instantFromNumber(long v) {
    long av = Math.abs(v);
    if (av >= 1_000_000_000_000_000_000L) {
      long secs = Math.floorDiv(v, 1_000_000_000L);
      long nanos = Math.floorMod(v, 1_000_000_000L);
      return Instant.ofEpochSecond(secs, nanos);
    } else if (av >= 1_000_000_000_000_000L) {
      long secs = Math.floorDiv(v, 1_000_000L);
      long micros = Math.floorMod(v, 1_000_000L);
      return Instant.ofEpochSecond(secs, micros * 1_000L);
    } else if (av >= 1_000_000_000_000L) {
      return Instant.ofEpochMilli(v);
    } else {
      return Instant.ofEpochSecond(v);
    }
  }

  private static LocalDateTime localDateTimeFromNumber(long v) {
    return LocalDateTime.ofInstant(instantFromNumber(v), ZoneOffset.UTC);
  }

  private static LocalDateTime parseTimestampNoTz(String raw) {
    try {
      return LocalDateTime.parse(raw);
    } catch (DateTimeParseException ignore) {
      return LocalDateTime.ofInstant(Instant.parse(raw), ZoneOffset.UTC);
    }
  }

  public static final class ByteArrayComparable implements Comparable<ByteArrayComparable> {
    private final byte[] b;

    ByteArrayComparable(byte[] b) {
      this.b = b;
    }

    @Override
    public int compareTo(ByteArrayComparable o) {
      int len = Math.min(b.length, o.b.length);
      for (int i = 0; i < len; i++) {
        int d = (b[i] & 0xFF) - (o.b[i] & 0xFF);
        if (d != 0) {
          return d;
        }
      }
      return Integer.compare(b.length, o.b.length);
    }

    public byte[] copy() {
      return Arrays.copyOf(b, b.length);
    }
  }
}
