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
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Base64;
import java.util.Locale;
import java.util.UUID;

/**
 * Coerces raw stat values from external sources (Parquet stats, Delta checkpoint metadata, etc.)
 * into the canonical Java types expected by FloeCat's statistics engine.
 *
 * <p>Each connector produces min/max/ndv statistics in its own native representation. This class
 * normalises those values to a single canonical form per {@link LogicalKind} so that the statistics
 * engine and query planner can compare them without source-format-specific logic. For temporal
 * types, connectors are expected to supply ISO-8601 strings or {@code java.time} objects; numeric
 * epoch values should be converted by the connector before calling this API.
 *
 * <p><b>Complex and semi-structured types</b> ({@link LogicalKind#INTERVAL}, {@link
 * LogicalKind#JSON}, {@link LogicalKind#ARRAY}, {@link LogicalKind#MAP}, {@link
 * LogicalKind#STRUCT}, {@link LogicalKind#VARIANT}) have no standard stat coercion; the input value
 * is returned unchanged.
 *
 * @see LogicalComparators for ordering normalised values
 * @see ValueEncoders for encoding values into canonical strings/bytes
 */
public final class LogicalCoercions {

  /**
   * Coerces a raw stat value to the canonical Java type for the given logical type.
   *
   * <p>Typical coercions:
   *
   * <ul>
   *   <li>{@code INT}: any {@link Number} or {@link String} → {@link Long}
   *   <li>{@code FLOAT}: any {@link Number} or {@link String} → {@link Float}
   *   <li>{@code DOUBLE}: any {@link Number} or {@link String} → {@link Double}
   *   <li>{@code DECIMAL}: {@link java.math.BigDecimal} / {@link Number} / {@link String} → {@link
   *       java.math.BigDecimal}
   *   <li>{@code TIMESTAMP}: {@link String} (ISO local date-time) / {@link java.time.Instant} →
   *       {@link java.time.LocalDateTime} (timezone-naive, session policy applies)
   *   <li>{@code TIMESTAMPTZ}: {@link String} / {@link java.time.Instant} → {@link
   *       java.time.Instant} (UTC)
   *   <li>{@code DATE}: {@link Number} (epoch-days) / {@link String} → {@link java.time.LocalDate}
   *   <li>{@code TIME}: {@link String} (ISO local time) → {@link java.time.LocalTime}
   *   <li>{@code BINARY}: {@code byte[]}, {@link java.nio.ByteBuffer}, or hex-string → {@code
   *       byte[]}
   *   <li>Complex/semi-structured: returned unchanged
   * </ul>
   *
   * @param t the logical type governing the coercion
   * @param v the raw value (null is returned as null)
   * @return coerced value in canonical Java form
   * @throws IllegalArgumentException if the value cannot be coerced to the requested type
   */
  public static Object coerceStatValue(LogicalType t, Object v) {
    if (v == null) {
      return null;
    }
    switch (t.kind()) {
      case BOOLEAN -> {
        if (v instanceof Boolean b) {
          return b;
        }
        return parseBooleanStrict(v);
      }
      case INT -> {
        // All integer sizes collapse to canonical 64-bit Long.
        if (v instanceof Number n) {
          return Int64Coercions.checkedLong(n);
        }
        return Long.parseLong(v.toString());
      }
      case FLOAT -> {
        if (v instanceof Number n) {
          return n.floatValue();
        }
        String s = v.toString();
        if ("NaN".equalsIgnoreCase(s)) {
          return Float.NaN;
        }
        if ("Infinity".equalsIgnoreCase(s) || "+Infinity".equalsIgnoreCase(s))
          return Float.POSITIVE_INFINITY;
        if ("-Infinity".equalsIgnoreCase(s)) {
          return Float.NEGATIVE_INFINITY;
        }
        return Float.parseFloat(s);
      }
      case DOUBLE -> {
        if (v instanceof Number n) {
          return n.doubleValue();
        }
        String s = v.toString();
        if ("NaN".equalsIgnoreCase(s)) {
          return Double.NaN;
        }
        if ("Infinity".equalsIgnoreCase(s) || "+Infinity".equalsIgnoreCase(s))
          return Double.POSITIVE_INFINITY;
        if ("-Infinity".equalsIgnoreCase(s)) {
          return Double.NEGATIVE_INFINITY;
        }
        return Double.parseDouble(s);
      }
      case DECIMAL -> {
        if (v instanceof BigDecimal bd) {
          return bd;
        }
        if (v instanceof Number n) {
          return new BigDecimal(n.toString());
        }
        return new BigDecimal(v.toString());
      }
      case STRING -> {
        return v.toString();
      }
      case BINARY -> {
        if (v instanceof byte[] b) {
          return b;
        }
        if (v instanceof ByteBuffer bb) {
          ByteBuffer dup = bb.duplicate();
          byte[] out = new byte[dup.remaining()];
          dup.get(out);
          return out;
        }
        String s = v.toString().trim();
        if (s.startsWith("0x") || s.startsWith("0X")) {
          return HexBytes.decodeHexBytes(s.substring(2));
        }
        try {
          return Base64.getDecoder().decode(s);
        } catch (IllegalArgumentException e) {
          throw new IllegalArgumentException("Invalid base64 BINARY value: " + s, e);
        }
      }
      case DATE -> {
        if (v instanceof LocalDate d) {
          return d;
        }
        if (v instanceof Number n) {
          return LocalDate.ofEpochDay(Int64Coercions.checkedLong(n));
        }
        return LocalDate.parse(v.toString());
      }
      case TIME -> {
        if (v instanceof LocalTime t0) {
          return TemporalCoercions.truncateToTemporalPrecision(t0, t.temporalPrecision());
        }
        String s = v.toString();
        try {
          return TemporalCoercions.truncateToTemporalPrecision(
              LocalTime.parse(s), t.temporalPrecision());
        } catch (Exception ignore) {
          // fall through
        }
        throw new IllegalArgumentException(
            "TIME value must be LocalTime or ISO HH:mm:ss[.nnn] String but was: " + s);
      }
      case TIMESTAMP -> {
        try {
          return TemporalCoercions.truncateToTemporalPrecision(
              TemporalCoercions.coerceTimestampNoTz(v), t.temporalPrecision());
        } catch (Exception ignore) {
          // fall through
        }
        throw new IllegalArgumentException(
            "TIMESTAMP value must be LocalDateTime, Instant, or ISO local date-time string but was:"
                + " "
                + v);
      }
      case TIMESTAMPTZ -> {
        // TIMESTAMPTZ is always UTC-normalised and coerced as Instant.
        try {
          return TemporalCoercions.truncateToTemporalPrecision(
              TemporalCoercions.coerceInstant(v), t.temporalPrecision());
        } catch (Exception ignore) {
          // fall through
        }
        throw new IllegalArgumentException(
            "TIMESTAMPTZ value must be Instant or ISO-8601 String but was: " + v);
      }
      case UUID -> {
        if (v instanceof UUID u) {
          return u;
        }
        return UUID.fromString(v.toString());
      }
        // INTERVAL, JSON, and complex types (ARRAY, MAP, STRUCT, VARIANT) have no standard
        // coercion; return the value unchanged.
    }
    return v;
  }

  private static boolean parseBooleanStrict(Object v) {
    String raw = v.toString().trim();
    String normalized = raw.toLowerCase(Locale.ROOT);
    return switch (normalized) {
      case "true", "t", "1" -> true;
      case "false", "f", "0" -> false;
      default -> throw new IllegalArgumentException("Invalid BOOLEAN value: " + raw);
    };
  }
}
