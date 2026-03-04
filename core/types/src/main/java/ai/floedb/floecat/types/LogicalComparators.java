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
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Base64;
import java.util.UUID;

public final class LogicalComparators {

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

    Object na = normalize(t, a);
    Object nb = normalize(t, b);

    return ((Comparable) na).compareTo(nb);
  }

  public static Object normalize(LogicalType t, Object v) {
    switch (t.kind()) {
      case BOOLEAN:
        return (Boolean) v;

      case INT16:
        return (v instanceof Short) ? v : ((Number) v).shortValue();

      case INT32:
        return (v instanceof Integer) ? v : ((Number) v).intValue();

      case INT64:
        return (v instanceof Long) ? v : ((Number) v).longValue();

      case FLOAT32:
        return (v instanceof Float) ? v : ((Number) v).floatValue();

      case FLOAT64:
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
          if (v instanceof Instant i) {
            return i;
          }

          if (v instanceof CharSequence s) {
            return Instant.parse(s.toString());
          }

          if (v instanceof Number n) {
            return instantFromNumber(n.longValue());
          }

          throw typeErr("TIMESTAMP", v);
        }
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
    if (av >= 1_000_000_000_000L && av < 1_000_000_000_000_000L) {
      return Instant.ofEpochMilli(v);
    } else if (av >= 1_000_000_000_000_000L) {
      long secs = Math.floorDiv(v, 1_000_000L);
      long micros = Math.floorMod(v, 1_000_000L);
      return Instant.ofEpochSecond(secs, micros * 1_000L);
    } else {
      return Instant.ofEpochSecond(v);
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
