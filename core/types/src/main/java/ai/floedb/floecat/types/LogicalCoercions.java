package ai.floedb.floecat.types;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Base64;
import java.util.UUID;

public final class LogicalCoercions {
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
      case INT16 -> {
        if (v instanceof Number n) {
          return n.shortValue();
        }
        return Short.parseShort(v.toString());
      }
      case INT32 -> {
        if (v instanceof Number n) {
          return n.intValue();
        }
        return Integer.parseInt(v.toString());
      }
      case INT64 -> {
        if (v instanceof Number n) {
          return n.longValue();
        }
        return Long.parseLong(v.toString());
      }
      case FLOAT32 -> {
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
      case FLOAT64 -> {
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
        String s = v.toString();
        try {
          return LocalTime.parse(s);
        } catch (Exception ignore) {
        }
        long nv = Long.parseLong(s);
        long day = 86_400_000_000_000L;
        long norm = Math.floorMod(nv, day);
        return LocalTime.ofNanoOfDay(norm);
      }
      case TIMESTAMP -> {
        if (v instanceof Instant i) {
          return i;
        }
        String s = v.toString();
        try {
          return Instant.parse(s);
        } catch (Exception ignore) {
        }
        long x = Long.parseLong(s);
        long ax = Math.abs(x);
        if (ax >= 1_000_000_000_000L && ax < 1_000_000_000_000_000L) {
          return Instant.ofEpochMilli(x);
        }
        if (ax >= 1_000_000_000_000_000L) {
          long secs = Math.floorDiv(x, 1_000_000L);
          long micros = Math.floorMod(x, 1_000_000L);
          return Instant.ofEpochSecond(secs, micros * 1_000L);
        }
        return Instant.ofEpochSecond(x);
      }
      case UUID -> {
        if (v instanceof UUID u) {
          return u;
        }
        return UUID.fromString(v.toString());
      }
    }
    return v;
  }
}
