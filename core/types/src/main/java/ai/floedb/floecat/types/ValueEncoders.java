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

  public static String encodeToString(LogicalType t, Object value) {
    if (value == null) {
      return null;
    }

    Objects.requireNonNull(t, "LogicalType required");

    switch (t.kind()) {
      case BOOLEAN:
        return Boolean.toString((Boolean) value);
      case INT16:
        return Short.toString((Short) value);
      case INT32:
        return Integer.toString((Integer) value);
      case INT64:
        return Long.toString((Long) value);
      case FLOAT32:
        return Float.toString((Float) value);
      case FLOAT64:
        return Double.toString((Double) value);

      case DATE:
        {
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
          if (value instanceof LocalTime t0) {
            return TIME_FMT.format(t0);
          }

          if (value instanceof Number n) {
            long v = n.longValue();
            final long NANOS_PER_DAY = 86_400_000_000_000L;

            long abs = Math.abs(v);
            long nanos;
            if (abs < 86_400L) {
              nanos = v * 1_000_000_000L;
            } else if (abs < 86_400_000L) {
              nanos = v * 1_000_000L;
            } else if (abs < 86_400_000_000L) {
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
          if (value instanceof Instant i) {
            return INSTANT_FMT.format(i);
          }

          if (value instanceof Number n) {
            return INSTANT_FMT.format(instantFromNumber((n.longValue())));
          }

          if (value instanceof CharSequence s) {
            return INSTANT_FMT.format(Instant.parse(s.toString()));
          }

          throw new IllegalArgumentException(
              "TIMESTAMP must be Instant, epoch-millis Number, or ISO-8601 String but was: "
                  + value.getClass().getName());
        }

      case STRING:
        return asUtf8String(value);
      case UUID:
        return ((UUID) value).toString();
      case BINARY:
        return Base64.getEncoder().encodeToString(asBytes(value));
      case DECIMAL:
        return ((BigDecimal) value).toPlainString();

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

  private static byte[] asBytes(Object v) {
    if (v instanceof byte[] arr) {
      return arr;
    }

    if (v instanceof ByteBuffer bb) {
      byte[] dst = new byte[bb.remaining()];
      bb.duplicate().get(dst);
      return dst;
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

  private static Instant instantFromNumber(long v) {
    long av = Math.abs(v);
    if (av >= 1_000_000_000_000L && av < 1_000_000_000_000_000L) {
      return Instant.ofEpochMilli(v);
    } else if (av >= 1_000_000_000_000_000L) {
      long secs = Math.floorDiv(v, 1_000_000L);
      long micros = Math.floorMod(v, 1_000_000L);
      return Instant.ofEpochSecond(secs, (int) (micros * 1_000L));
    } else {
      return Instant.ofEpochSecond(v);
    }
  }
}
