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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.scanner.spi.SystemObjectRow;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.UUID;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

class ArrowConversionTest {

  @Test
  void fill_populatesVectors() {
    Assumptions.assumeTrue(isArrowAvailable(), "Arrow memory allocator is unavailable");

    Field col1 = new Field("col1", FieldType.nullable(new ArrowType.Utf8()), List.of());
    Field col2 = new Field("col2", FieldType.nullable(new ArrowType.Int(32, true)), List.of());
    Schema schema = new Schema(List.of(col1, col2));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {

      SystemObjectRow row1 = new SystemObjectRow(new Object[] {"alice", 1});
      SystemObjectRow row2 = new SystemObjectRow(new Object[] {"bob", 2});

      ArrowConversion.fill(root, List.of(row1, row2));

      VarCharVector names = (VarCharVector) root.getVector("col1");
      IntVector ints = (IntVector) root.getVector("col2");

      assertThat(root.getRowCount()).isEqualTo(2);
      assertThat(names.getObject(0).toString()).isEqualTo("alice");
      assertThat(names.getObject(1).toString()).isEqualTo("bob");
      assertThat(ints.getObject(0)).isEqualTo(1);
      assertThat(ints.getObject(1)).isEqualTo(2);
    }
  }

  @Test
  void fill_allowsNulls() {
    Assumptions.assumeTrue(isArrowAvailable(), "Arrow memory allocator is unavailable");

    Field col1 = new Field("col1", FieldType.nullable(new ArrowType.Utf8()), List.of());
    Field col2 = new Field("col2", FieldType.nullable(new ArrowType.Int(32, true)), List.of());
    Schema schema = new Schema(List.of(col1, col2));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {

      SystemObjectRow row = new SystemObjectRow(new Object[] {null, null});
      ArrowConversion.fill(root, List.of(row));

      VarCharVector names = (VarCharVector) root.getVector("col1");
      IntVector ints = (IntVector) root.getVector("col2");

      assertThat(root.getRowCount()).isEqualTo(1);
      assertThat(names.getObject(0)).isNull();
      assertThat(ints.getObject(0)).isNull();
    }
  }

  @Test
  void fill_requiresColumnCount() {
    Assumptions.assumeTrue(isArrowAvailable(), "Arrow memory allocator is unavailable");

    Field col1 = new Field("col1", FieldType.nullable(new ArrowType.Utf8()), List.of());
    Schema schema = new Schema(List.of(col1));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {

      SystemObjectRow bad = new SystemObjectRow(new Object[] {"one", 2});

      assertThatThrownBy(() -> ArrowConversion.fill(root, List.of(bad)))
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Test
  void fill_rejectsUnsupportedVector() {
    Assumptions.assumeTrue(isArrowAvailable(), "Arrow memory allocator is unavailable");

    Field col = new Field("col", FieldType.nullable(new ArrowType.Null()), List.of());
    Schema schema = new Schema(List.of(col));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {

      SystemObjectRow row = new SystemObjectRow(new Object[] {"value"});

      assertThatThrownBy(() -> ArrowConversion.fill(root, List.of(row)))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("unsupported vector type");
    }
  }

  @Test
  void fill_supportsCanonicalTemporalBinaryAndDecimalVectors() {
    Assumptions.assumeTrue(isArrowAvailable(), "Arrow memory allocator is unavailable");

    Schema schema =
        new Schema(
            List.of(
                new Field(
                    "date_col", FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), List.of()),
                new Field(
                    "time_col",
                    FieldType.nullable(new ArrowType.Time(TimeUnit.MICROSECOND, 64)),
                    List.of()),
                new Field(
                    "ts_col",
                    FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)),
                    List.of()),
                new Field(
                    "tstz_col",
                    FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")),
                    List.of()),
                new Field(
                    "dec_col", FieldType.nullable(new ArrowType.Decimal(12, 3, 128)), List.of()),
                new Field("bin_col", FieldType.nullable(new ArrowType.Binary()), List.of()),
                new Field(
                    "uuid_col", FieldType.nullable(new ArrowType.FixedSizeBinary(16)), List.of()),
                new Field(
                    "iv_col",
                    FieldType.nullable(new ArrowType.Interval(IntervalUnit.DAY_TIME)),
                    List.of())));

    LocalDate date = LocalDate.of(2026, 2, 26);
    LocalTime time = LocalTime.of(12, 34, 56, 123_456_000);
    Instant ts = Instant.parse("2026-02-26T11:22:33.123456Z");
    Instant tstz = Instant.parse("2026-02-26T10:00:00Z");
    BigDecimal decimal = new BigDecimal("123.456");
    byte[] binary = new byte[] {1, 2, 3};
    UUID uuid = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
    Duration interval = Duration.ofDays(2).plusHours(3).plusMillis(4);

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      ArrowConversion.fill(
          root,
          List.of(
              new SystemObjectRow(
                  new Object[] {date, time, ts, tstz, decimal, binary, uuid, interval})));

      DateDayVector dateVector = (DateDayVector) root.getVector("date_col");
      TimeMicroVector timeVector = (TimeMicroVector) root.getVector("time_col");
      TimeStampMicroVector tsVector = (TimeStampMicroVector) root.getVector("ts_col");
      TimeStampMicroTZVector tstzVector = (TimeStampMicroTZVector) root.getVector("tstz_col");
      DecimalVector decimalVector = (DecimalVector) root.getVector("dec_col");
      VarBinaryVector binaryVector = (VarBinaryVector) root.getVector("bin_col");
      FixedSizeBinaryVector uuidVector = (FixedSizeBinaryVector) root.getVector("uuid_col");
      IntervalDayVector intervalVector = (IntervalDayVector) root.getVector("iv_col");

      assertThat(dateVector.get(0)).isEqualTo((int) date.toEpochDay());
      assertThat(timeVector.get(0)).isEqualTo(time.toNanoOfDay() / 1_000L);
      assertThat(tsVector.get(0))
          .isEqualTo(ts.getEpochSecond() * 1_000_000L + ts.getNano() / 1_000L);
      assertThat(tstzVector.get(0))
          .isEqualTo(tstz.getEpochSecond() * 1_000_000L + tstz.getNano() / 1_000L);
      assertThat(decimalVector.getObject(0)).isEqualByComparingTo(decimal);
      assertThat(binaryVector.get(0)).containsExactly(binary);
      assertThat(uuidVector.get(0)).hasSize(16);
      assertThat(intervalVector.getObject(0)).isEqualTo(interval);
    }
  }

  @Test
  void fill_parsesCanonicalTimestampStringsForTimestampAndTimestamptzVectors() {
    Assumptions.assumeTrue(isArrowAvailable(), "Arrow memory allocator is unavailable");

    Schema schema =
        new Schema(
            List.of(
                new Field(
                    "ts_col",
                    FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)),
                    List.of()),
                new Field(
                    "tstz_col",
                    FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")),
                    List.of())));

    String ts = "2026-02-26T12:34:56.123456";
    String tstz = "2026-02-26T10:00:00Z";

    LocalDateTime parsedTs = LocalDateTime.parse(ts);
    Instant parsedTstz = Instant.parse(tstz);
    long tsMicros =
        parsedTs.toInstant(java.time.ZoneOffset.UTC).getEpochSecond() * 1_000_000L
            + parsedTs.toInstant(java.time.ZoneOffset.UTC).getNano() / 1_000L;
    long tstzMicros = parsedTstz.getEpochSecond() * 1_000_000L + parsedTstz.getNano() / 1_000L;

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      ArrowConversion.fill(root, List.of(new SystemObjectRow(new Object[] {ts, tstz})));

      TimeStampMicroVector tsVector = (TimeStampMicroVector) root.getVector("ts_col");
      TimeStampMicroTZVector tstzVector = (TimeStampMicroTZVector) root.getVector("tstz_col");

      assertThat(tsVector.get(0)).isEqualTo(tsMicros);
      assertThat(tstzVector.get(0)).isEqualTo(tstzMicros);
    }
  }

  private static boolean isArrowAvailable() {
    try (BufferAllocator ignored = new RootAllocator(Long.MAX_VALUE)) {
      return true;
    } catch (Throwable ignored) {
      return false;
    }
  }
}
