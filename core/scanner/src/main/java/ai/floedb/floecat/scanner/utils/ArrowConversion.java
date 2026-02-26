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
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
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
      timestamp.setSafe(idx, toTimestampMicros(value));
      return;
    }
    if (vector instanceof TimeStampMicroTZVector timestamptz) {
      timestamptz.setSafe(idx, toTimestampMicros(value));
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
    if (vector instanceof IntervalDayVector interval) {
      setIntervalDay(interval, idx, value);
      return;
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
      return time.toNanoOfDay() / 1_000L;
    }
    if (value instanceof Number number) {
      return number.longValue();
    }
    if (value instanceof CharSequence text) {
      return LocalTime.parse(text.toString()).toNanoOfDay() / 1_000L;
    }
    throw new IllegalArgumentException("TIME value must be LocalTime, Number, or ISO time string");
  }

  private static long toTimestampMicros(Object value) {
    if (value instanceof Instant instant) {
      return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000L;
    }
    if (value instanceof LocalDateTime localDateTime) {
      Instant instant = localDateTime.toInstant(ZoneOffset.UTC);
      return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000L;
    }
    if (value instanceof Number number) {
      return number.longValue();
    }
    if (value instanceof CharSequence text) {
      String raw = text.toString();
      try {
        Instant instant = Instant.parse(raw);
        return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000L;
      } catch (DateTimeParseException ignored) {
        try {
          LocalDateTime localDateTime = LocalDateTime.parse(raw);
          Instant instant = localDateTime.toInstant(ZoneOffset.UTC);
          return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000L;
        } catch (DateTimeParseException ignored2) {
          return Long.parseLong(raw);
        }
      }
    }
    throw new IllegalArgumentException(
        "TIMESTAMP value must be Instant, LocalDateTime, Number, or ISO local/instant string");
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

  private static void setIntervalDay(IntervalDayVector interval, int idx, Object value) {
    if (value instanceof Duration duration) {
      long totalMillis = duration.toMillis();
      int days = Math.toIntExact(Math.floorDiv(totalMillis, 86_400_000L));
      int millis = Math.toIntExact(Math.floorMod(totalMillis, 86_400_000L));
      interval.setSafe(idx, days, millis);
      return;
    }
    if (value instanceof Number number) {
      long totalMillis = number.longValue();
      int days = Math.toIntExact(Math.floorDiv(totalMillis, 86_400_000L));
      int millis = Math.toIntExact(Math.floorMod(totalMillis, 86_400_000L));
      interval.setSafe(idx, days, millis);
      return;
    }
    byte[] bytes = null;
    if (value instanceof byte[] arr) {
      bytes = arr;
    } else if (value instanceof ByteBuffer buffer) {
      ByteBuffer copy = buffer.duplicate();
      bytes = new byte[copy.remaining()];
      copy.get(bytes);
    }
    if (bytes != null && bytes.length >= 12) {
      ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
      bb.getInt(); // months (ignored by DAY_TIME interval representation)
      int days = bb.getInt();
      int millis = bb.getInt();
      interval.setSafe(idx, days, millis);
      return;
    }
    throw new IllegalArgumentException(
        "INTERVAL value must be Duration, Number (millis), or 12-byte parquet interval payload");
  }
}
