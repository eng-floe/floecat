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
import java.util.Base64;
import java.util.UUID;

public final class LogicalCoercions {
  private static final long NANOS_PER_DAY = 86_400_000_000_000L;

  public static Object coerceStatValue(LogicalType t, Object v) {
    if (v == null) {
      return null;
    }
    switch (t.kind()) {
      case BOOLEAN -> {
        if (v instanceof Boolean b) {
          return b;
        }
        return Boolean.parseBoolean(v.toString());
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
          String hex = s.substring(2);
          int len = hex.length();
          byte[] out = new byte[len / 2];
          for (int i = 0; i < out.length; i++) {
            int hi = Character.digit(hex.charAt(2 * i), 16);
            int lo = Character.digit(hex.charAt(2 * i + 1), 16);
            out[i] = (byte) ((hi << 4) | lo);
          }
          return out;
        }
        return Base64.getDecoder().decode(s);
      }
      case DATE -> {
        if (v instanceof LocalDate d) {
          return d;
        }
        if (v instanceof Number n) {
          return LocalDate.ofEpochDay(n.longValue());
        }
        return LocalDate.parse(v.toString());
      }
      case TIME -> {
        if (v instanceof LocalTime t0) {
          return t0;
        }
        if (v instanceof Number n) {
          long nanos = toTimeNanos(n.longValue());
          long norm = Math.floorMod(nanos, NANOS_PER_DAY);
          return LocalTime.ofNanoOfDay(norm);
        }
        String s = v.toString();
        try {
          return LocalTime.parse(s);
        } catch (Exception ignore) {
        }
        long nv = Long.parseLong(s);
        long norm = Math.floorMod(toTimeNanos(nv), NANOS_PER_DAY);
        return LocalTime.ofNanoOfDay(norm);
      }
      case TIMESTAMP -> {
        if (v instanceof LocalDateTime ts) {
          return ts;
        }
        if (v instanceof Instant i) {
          return LocalDateTime.ofInstant(i, ZoneOffset.UTC);
        }
        String s = v.toString();
        try {
          return LocalDateTime.parse(s);
        } catch (DateTimeParseException ignore) {
        }
        try {
          return LocalDateTime.ofInstant(Instant.parse(s), ZoneOffset.UTC);
        } catch (DateTimeParseException ignore) {
        }
        long x = Long.parseLong(s);
        return LocalDateTime.ofInstant(instantFromNumber(x), ZoneOffset.UTC);
      }
      case TIMESTAMPTZ -> {
        // TIMESTAMPTZ is always UTC-normalised and coerced as Instant.
        if (v instanceof Instant i) {
          return i;
        }
        String s = v.toString();
        try {
          return Instant.parse(s);
        } catch (Exception ignore) {
        }
        long x = Long.parseLong(s);
        return instantFromNumber(x);
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

  private static Instant instantFromNumber(long v) {
    long av = Math.abs(v);
    if (av >= 1_000_000_000_000_000_000L) {
      long secs = Math.floorDiv(v, 1_000_000_000L);
      long nanos = Math.floorMod(v, 1_000_000_000L);
      return Instant.ofEpochSecond(secs, nanos);
    }
    if (av >= 1_000_000_000_000_000L) {
      long secs = Math.floorDiv(v, 1_000_000L);
      long micros = Math.floorMod(v, 1_000_000L);
      return Instant.ofEpochSecond(secs, micros * 1_000L);
    }
    if (av >= 1_000_000_000_000L) {
      return Instant.ofEpochMilli(v);
    }
    return Instant.ofEpochSecond(v);
  }

  private static long toTimeNanos(long v) {
    long abs = Math.abs(v);
    if (abs < 86_400L) {
      return v * 1_000_000_000L; // seconds
    }
    if (abs < 86_400_000L) {
      return v * 1_000_000L; // milliseconds
    }
    if (abs < 86_400_000_000L) {
      return v * 1_000L; // microseconds
    }
    return v; // nanoseconds
  }
}
