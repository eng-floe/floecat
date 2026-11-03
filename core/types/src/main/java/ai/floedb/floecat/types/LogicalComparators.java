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
import java.util.Arrays;
import java.util.Base64;
import java.util.UUID;

/**
 * Provides ordering and normalisation utilities for Floecat canonical logical types.
 *
 * <p>Used primarily by statistics builders that need to compare encoded min/max values stored as
 * strings or objects. Comparisons are performed on <em>normalised</em> Java values (e.g., all
 * integers are promoted to {@link Long}, TIMESTAMP to {@link java.time.LocalDateTime}, TIMESTAMPTZ
 * to {@link java.time.Instant}) so that histogram builders can order them without deserialising raw
 * binary payloads.
 *
 * <p><b>Complex and semi-structured types</b> ({@link LogicalKind#INTERVAL}, {@link
 * LogicalKind#JSON}, {@link LogicalKind#ARRAY}, {@link LogicalKind#MAP}, {@link
 * LogicalKind#STRUCT}, {@link LogicalKind#VARIANT}) have no meaningful min/max ordering. {@link
 * #isStatsOrderable(LogicalType)} returns {@code false} for these kinds, and {@link
 * #normalize(LogicalType, Object)} returns {@code null} rather than throwing.
 */
public final class LogicalComparators {

  /**
   * Returns {@code true} iff values of the given type can be meaningfully ordered for stats.
   *
   * <p>All scalar numeric, string, binary, UUID, and temporal types (except INTERVAL) are orderable
   * for stats. Complex and semi-structured types are not.
   *
   * @param t the logical type to test (null → {@code false})
   * @return {@code true} if the type is orderable
   */
  public static boolean isStatsOrderable(LogicalType t) {
    if (t == null) {
      return false;
    }
    return t.kind().isStatsOrderable();
  }

  /**
   * Whether {@code v} is a -0.0, which the legacy string spelling of a bound writes as 0: a -0.0
   * minimum spelled so would exclude the -0.0 values it covers.
   */
  public static boolean isNegativeZero(Object v) {
    return v instanceof Double d && Double.doubleToRawLongBits(d) == Long.MIN_VALUE
        || v instanceof Float f && Float.floatToRawIntBits(f) == Integer.MIN_VALUE;
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

    if (!isStatsOrderable(t)) {
      String kind = (t == null) ? "<null>" : t.kind().name();
      throw new IllegalArgumentException("Logical type is not orderable: " + kind);
    }

    Object na = normalize(t, a);
    Object nb = normalize(t, b);

    // Strings and UUIDs order as their bytes do, unsigned, as Iceberg, Parquet and Delta bounds
    // do: String.compareTo orders UTF-16 code units, and UUID.compareTo signed halves.
    if (na instanceof String sa && nb instanceof String sb) {
      return compareCodePoints(sa, sb);
    }
    if (na instanceof UUID ua && nb instanceof UUID ub) {
      int high = Long.compareUnsigned(ua.getMostSignificantBits(), ub.getMostSignificantBits());
      return high != 0
          ? high
          : Long.compareUnsigned(ua.getLeastSignificantBits(), ub.getLeastSignificantBits());
    }

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
   * <p>Examples: any {@link Number} for INT → {@link Long}; string/instant for TIMESTAMP → {@link
   * java.time.LocalDateTime} (session policy applies); string/instant for TIMESTAMPTZ → {@link
   * java.time.Instant}. For unorderable kinds (INTERVAL, JSON, ARRAY, MAP, STRUCT, VARIANT),
   * returns {@code null}.
   *
   * @param t the logical type
   * @param v the raw value (may be a Java primitive wrapper, String, or byte array)
   * @return normalised value, or {@code null} if the kind is not stats-orderable
   */
  public static Object normalize(LogicalType t, Object v) {
    if (!isStatsOrderable(t)) {
      return null;
    }
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
        if (v instanceof ByteArrayComparable normalized) {
          return normalized;
        }
        byte[] bytes;
        if (v instanceof byte[] arr) {
          bytes = arr;
        } else if (v instanceof ByteBuffer bb) {
          var dup = bb.duplicate();
          bytes = new byte[dup.remaining()];
          dup.get(bytes);
        } else if (v instanceof CharSequence s) {
          String raw = s.toString().trim();
          if (raw.startsWith("0x") || raw.startsWith("0X")) {
            bytes = HexBytes.decodeHexBytes(raw.substring(2));
          } else {
            bytes = Base64.getDecoder().decode(raw);
          }
        } else {
          throw typeErr("BINARY", v);
        }
        return new ByteArrayComparable(bytes);

      case DATE:
        if (v instanceof LocalDate d) {
          return d;
        }

        if (v instanceof Number n) {
          return LocalDate.ofEpochDay(Int64Coercions.checkedLong(n));
        }

        if (v instanceof CharSequence s) {
          return LocalDate.parse(s.toString());
        }

        throw typeErr("DATE", v);

      case TIME:
      case TIMESTAMP:
      case TIMESTAMPTZ:
        return truncate(exactTemporal(t.kind(), v), t.temporalPrecision());

        // INTERVAL, JSON, and complex types (ARRAY, MAP, STRUCT, VARIANT) have no meaningful
        // min/max stats ordering; callers should check for null before comparing.
    }
    return null;
  }

  /** Orders by code point, which is the unsigned order of the strings' UTF-8 bytes. */
  private static int compareCodePoints(String a, String b) {
    int i = 0;
    int j = 0;
    while (i < a.length() && j < b.length()) {
      int ca = a.codePointAt(i);
      int cb = b.codePointAt(j);
      if (ca != cb) {
        return Integer.compare(ca, cb);
      }
      i += Character.charCount(ca);
      j += Character.charCount(cb);
    }
    return Integer.compare(a.length() - i, b.length() - j);
  }

  /**
   * As {@link #normalize}, for a lower bound: a value {@code t} cannot hold exactly (a temporal
   * value finer than its precision, a double on a FLOAT column) is rounded down, so the bound still
   * holds for it.
   */
  public static Object normalizeLower(LogicalType t, Object v) {
    if (isStatsOrderable(t) && t.kind() == LogicalKind.FLOAT && v instanceof Number n) {
      float f = n.floatValue();
      return (double) f > n.doubleValue() ? Math.nextDown(f) : f;
    }
    return normalize(t, v);
  }

  /**
   * As {@link #normalize}, for an upper bound: a value {@code t} cannot hold exactly is rounded up,
   * so the bound still holds for it. Null for a type that has no order, or for a TIME rounded up
   * past midnight; throws for a timestamp rounded up past the type's range.
   */
  public static Object normalizeUpper(LogicalType t, Object v) {
    if (!isStatsOrderable(t)) {
      return null;
    }
    if (t.kind() == LogicalKind.FLOAT && v instanceof Number n) {
      float f = n.floatValue();
      return (double) f < n.doubleValue() ? Math.nextUp(f) : f;
    }
    return switch (t.kind()) {
      case TIME, TIMESTAMP, TIMESTAMPTZ -> {
        Object exact = exactTemporal(t.kind(), v);
        Object floor = truncate(exact, t.temporalPrecision());
        if (floor.equals(exact)) {
          yield floor;
        }
        long unit = TemporalCoercions.temporalUnitNanos(t.temporalPrecision());
        yield switch (floor) {
          case LocalTime time -> {
            LocalTime up = time.plusNanos(unit);
            yield up.isAfter(time) ? up : null;
          }
          case LocalDateTime timestamp -> timestamp.plusNanos(unit);
          default -> ((Instant) floor).plusNanos(unit);
        };
      }
      default -> normalize(t, v);
    };
  }

  /** A TIME, TIMESTAMP or TIMESTAMPTZ value with every fractional digit it was given. */
  private static Object exactTemporal(LogicalKind kind, Object v) {
    return switch (kind) {
      case TIME ->
          switch (v) {
            case LocalTime time -> time;
            case CharSequence s -> LocalTime.parse(s.toString());
            default -> throw typeErr("TIME", v);
          };
      case TIMESTAMP ->
          switch (v) {
            case Instant instant ->
                TemporalCoercions.localDateTimeFromInstantNoTzAtPrecision(
                    instant, LogicalType.MAX_TEMPORAL_PRECISION);
            case CharSequence s ->
                TemporalCoercions.parseTimestampNoTzAtPrecision(
                    s.toString(), LogicalType.MAX_TEMPORAL_PRECISION);
            default -> TemporalCoercions.coerceTimestampNoTz(v);
          };
      // TIMESTAMPTZ is always UTC-normalised and compared as Instant.
      default -> TemporalCoercions.coerceInstant(v);
    };
  }

  private static Object truncate(Object temporal, Integer precision) {
    return switch (temporal) {
      case LocalTime time -> TemporalCoercions.truncateToTemporalPrecision(time, precision);
      case LocalDateTime timestamp ->
          TemporalCoercions.truncateToTemporalPrecision(timestamp, precision);
      default -> TemporalCoercions.truncateToTemporalPrecision((Instant) temporal, precision);
    };
  }

  private static IllegalArgumentException typeErr(String kind, Object v) {
    return new IllegalArgumentException(
        kind + " compare expects canonical types, got: " + v.getClass().getName());
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
