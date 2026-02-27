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

package ai.floedb.floecat.scanner.utils;

import ai.floedb.floecat.scanner.spi.SystemObjectRow;
import ai.floedb.floecat.types.TemporalCoercions;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.UUID;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Helper for converting Object[] rows into Arrow vectors.
 *
 * <p>Engine plugins may implement Arrow-native scanners without using this helper.
 */
public final class ArrowConversion {

  private ArrowConversion() {}

  public static VectorSchemaRoot fill(VectorSchemaRoot root, Iterable<SystemObjectRow> rows) {
    Objects.requireNonNull(root, "root");
    Objects.requireNonNull(rows, "rows");

    var vectors = root.getFieldVectors();
    int columnCount = vectors.size();
    int rowIndex = 0;

    for (SystemObjectRow row : rows) {
      Object[] values = row.values();
      if (values.length != columnCount) {
        throw new IllegalArgumentException(
            "row column count mismatch: expected=" + columnCount + " actual=" + values.length);
      }
      for (int column = 0; column < columnCount; column++) {
        writeValue(vectors.get(column), rowIndex, values[column]);
      }
      rowIndex++;
    }

    for (FieldVector vector : vectors) {
      vector.setValueCount(rowIndex);
    }
    root.setRowCount(rowIndex);
    return root;
  }

  private static void writeValue(FieldVector vector, int idx, Object value) {
    if (value == null) {
      vector.setNull(idx);
      return;
    }
    if (vector instanceof VarCharVector varChar) {
      byte[] bytes = value.toString().getBytes(StandardCharsets.UTF_8);
      varChar.setSafe(idx, bytes);
      return;
    }
    if (vector instanceof IntVector intVec) {
      intVec.setSafe(idx, ((Number) value).intValue());
      return;
    }
    if (vector instanceof BigIntVector bigInt) {
      bigInt.setSafe(idx, ((Number) value).longValue());
      return;
    }
    if (vector instanceof Float4Vector float4) {
      float4.setSafe(idx, ((Number) value).floatValue());
      return;
    }
    if (vector instanceof Float8Vector float8) {
      float8.setSafe(idx, ((Number) value).doubleValue());
      return;
    }
    if (vector instanceof BitVector bit) {
      bit.setSafe(idx, (((Boolean) value) ? 1 : 0));
      return;
    }
    if (vector instanceof DateDayVector date) {
      date.setSafe(idx, toDateDay(value));
      return;
    }
    if (vector instanceof TimeMicroVector time) {
      time.setSafe(idx, toTimeMicros(value));
      return;
    }
    if (vector instanceof TimeStampMicroVector timestamp) {
      timestamp.setSafe(idx, toTimestampMicrosNoTz(value));
      return;
    }
    if (vector instanceof TimeStampMicroTZVector timestamptz) {
      timestamptz.setSafe(idx, toTimestampMicrosTz(value));
      return;
    }
    if (vector instanceof DecimalVector decimal) {
      decimal.setSafe(idx, toDecimal(value));
      return;
    }
    if (vector instanceof VarBinaryVector varBinary) {
      varBinary.setSafe(idx, toBytes(value));
      return;
    }
    if (vector instanceof FixedSizeBinaryVector fixed) {
      fixed.setSafe(idx, toFixedBytes(value, fixed.getByteWidth()));
      return;
    }
    if (vector instanceof IntervalDayVector) {
      throw new IllegalArgumentException(
          "INTERVAL values are not supported for Arrow output (no stable representation)");
    }
    throw new IllegalArgumentException(
        "unsupported vector type " + vector.getClass().getSimpleName());
  }

  private static int toDateDay(Object value) {
    if (value instanceof LocalDate date) {
      return Math.toIntExact(date.toEpochDay());
    }
    if (value instanceof Number number) {
      return number.intValue();
    }
    if (value instanceof CharSequence text) {
      return Math.toIntExact(LocalDate.parse(text.toString()).toEpochDay());
    }
    throw new IllegalArgumentException("DATE value must be LocalDate, Number, or ISO date string");
  }

  private static long toTimeMicros(Object value) {
    if (value instanceof LocalTime time) {
      LocalTime truncated = TemporalCoercions.truncateToMicros(time);
      return truncated.toNanoOfDay() / 1_000L;
    }
    if (value instanceof Number number) {
      long dayNanos = TemporalCoercions.timeNanosOfDay(number.longValue());
      return Math.floorDiv(dayNanos, 1_000L);
    }
    if (value instanceof CharSequence text) {
      LocalTime parsed = LocalTime.parse(text.toString());
      return TemporalCoercions.truncateToMicros(parsed).toNanoOfDay() / 1_000L;
    }
    throw new IllegalArgumentException("TIME value must be LocalTime, Number, or ISO time string");
  }

  private static long toTimestampMicrosNoTz(Object value) {
    if (value instanceof Instant instant) {
      LocalDateTime local = TemporalCoercions.localDateTimeFromInstantNoTz(instant);
      return local.toInstant(ZoneOffset.UTC).getEpochSecond() * 1_000_000L
          + local.getNano() / 1_000L;
    }
    if (value instanceof LocalDateTime localDateTime) {
      LocalDateTime truncated = TemporalCoercions.truncateToMicros(localDateTime);
      Instant instant = truncated.toInstant(ZoneOffset.UTC);
      return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000L;
    }
    if (value instanceof Number number) {
      LocalDateTime local = TemporalCoercions.localDateTimeFromNumber(number.longValue());
      Instant instant = local.toInstant(ZoneOffset.UTC);
      return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000L;
    }
    if (value instanceof CharSequence text) {
      String raw = text.toString();
      try {
        LocalDateTime local = TemporalCoercions.parseTimestampNoTz(raw);
        Instant instant = local.toInstant(ZoneOffset.UTC);
        return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000L;
      } catch (RuntimeException ignored) {
        LocalDateTime local = TemporalCoercions.localDateTimeFromNumber(Long.parseLong(raw));
        Instant instant = local.toInstant(ZoneOffset.UTC);
        return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000L;
      }
    }
    throw new IllegalArgumentException(
        "TIMESTAMP value must be Instant, LocalDateTime, Number, or ISO local/instant string");
  }

  private static long toTimestampMicrosTz(Object value) {
    Instant instant;
    if (value instanceof Instant i) {
      instant = TemporalCoercions.truncateToMicros(i);
    } else if (value instanceof Number number) {
      instant = TemporalCoercions.instantFromNumber(number.longValue());
    } else if (value instanceof CharSequence text) {
      instant =
          TemporalCoercions.truncateToMicros(TemporalCoercions.parseZonedInstant(text.toString()));
    } else {
      throw new IllegalArgumentException(
          "TIMESTAMPTZ value must be Instant, Number, or ISO instant string");
    }
    return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000L;
  }

  private static BigDecimal toDecimal(Object value) {
    if (value instanceof BigDecimal decimal) {
      return decimal;
    }
    if (value instanceof Number number) {
      return new BigDecimal(number.toString());
    }
    if (value instanceof CharSequence text) {
      return new BigDecimal(text.toString());
    }
    throw new IllegalArgumentException("DECIMAL value must be BigDecimal, Number, or string");
  }

  private static byte[] toBytes(Object value) {
    if (value instanceof byte[] bytes) {
      return bytes;
    }
    if (value instanceof ByteBuffer buffer) {
      ByteBuffer copy = buffer.duplicate();
      byte[] bytes = new byte[copy.remaining()];
      copy.get(bytes);
      return bytes;
    }
    return value.toString().getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] toFixedBytes(Object value, int width) {
    byte[] bytes;
    if (value instanceof UUID uuid) {
      ByteBuffer buffer = ByteBuffer.allocate(16);
      buffer.putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits());
      bytes = buffer.array();
    } else if (value instanceof CharSequence text && width == 16) {
      UUID uuid = UUID.fromString(text.toString());
      ByteBuffer buffer = ByteBuffer.allocate(16);
      buffer.putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits());
      bytes = buffer.array();
    } else {
      bytes = toBytes(value);
    }
    if (bytes.length != width) {
      throw new IllegalArgumentException(
          "fixed-size binary value width mismatch: expected=" + width + " actual=" + bytes.length);
    }
    return bytes;
  }
}
