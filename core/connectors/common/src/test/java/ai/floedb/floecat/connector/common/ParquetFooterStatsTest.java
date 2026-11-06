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

package ai.floedb.floecat.connector.common;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.types.LogicalKind;
import ai.floedb.floecat.types.LogicalType;
import ai.floedb.floecat.types.TemporalCoercions;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Set;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

class ParquetFooterStatsTest {

  @Test
  void colAgg_skipsMinMaxForNonOrderableLogicalTypes() {
    ParquetFooterStats.ColAgg agg = new ParquetFooterStats.ColAgg();
    LogicalType interval = LogicalType.of(LogicalKind.INTERVAL);

    agg.addRowGroup(10, intStats(0), "a", "z", interval);

    assertThat(agg.min()).isNull();
    assertThat(agg.max()).isNull();
  }

  @Test
  void colAgg_preservesOrderingForOrderableLogicalTypes() {
    ParquetFooterStats.ColAgg agg = new ParquetFooterStats.ColAgg();
    LogicalType intType = LogicalType.of(LogicalKind.INT);

    agg.addRowGroup(10, intStats(0), 9L, 9L, intType);
    agg.addRowGroup(10, intStats(0), 2L, 12L, intType);

    assertThat(agg.min()).isEqualTo(2L);
    assertThat(agg.max()).isEqualTo(12L);
  }

  @Test
  void aRowGroupWithoutStatisticsLeavesTheFileWithoutThem() {
    ParquetFooterStats.ColAgg agg = new ParquetFooterStats.ColAgg();
    LogicalType intType = LogicalType.of(LogicalKind.INT);

    agg.addRowGroup(10, null, null, null, intType);
    agg.addRowGroup(10, intStats(0), 1L, 5L, intType);

    assertThat(agg.nulls()).isNull();
    assertThat(agg.min()).isNull();
    assertThat(agg.max()).isNull();
  }

  @Test
  void aStatisticWithoutValuesHasNoBounds() {
    // Parquet reads a row group with a null count but no min/max (all NULL, or bounds it drops) as
    // the type's default, 0.
    var stats = intStats(3);
    var chunk =
        ColumnChunkMetaData.get(
            ColumnPath.get("c"),
            PrimitiveTypeName.INT64,
            CompressionCodecName.UNCOMPRESSED,
            null,
            Set.of(),
            stats,
            0,
            0,
            3,
            0,
            0);

    assertThat(stats.genericGetMin()).isEqualTo(0L);
    assertThat(ParquetFooterStats.boundOf(stats, chunk, LogicalType.of(LogicalKind.INT), true))
        .isNull();
  }

  /** INT64 statistics with {@code nulls} NULLs and no bounds set. */
  private static Statistics<?> intStats(long nulls) {
    var stats = Statistics.createStats(Types.required(PrimitiveTypeName.INT64).named("c"));
    stats.setNumNulls(nulls);
    return stats;
  }

  @Test
  void timeStatsDropOutOfRangeValues() throws Exception {
    Object lta = timeLogicalTypeAnnotation(TimeUnit.MICROS);
    Method m =
        ParquetFooterStats.class.getDeclaredMethod("timeStatValue", Object.class, Object.class);
    m.setAccessible(true);

    long nanosPerDay = TemporalCoercions.NANOS_PER_DAY;
    long outOfRangeMicros = nanosPerDay / 1_000L;
    Object outOfRange = m.invoke(null, lta, outOfRangeMicros);
    assertThat(outOfRange).isNull();

    long inRangeNanos = nanosPerDay - 1;
    long inRangeMicros = inRangeNanos / 1_000L;
    Object inRange = m.invoke(null, lta, inRangeMicros);
    assertThat(inRange).isEqualTo(LocalTime.ofNanoOfDay(inRangeMicros * 1_000L));
  }

  @Test
  void timestampStatsWithoutLogicalAnnotationFallbackToMicrosForTimestamp() throws Exception {
    Method m =
        ParquetFooterStats.class.getDeclaredMethod(
            "timestampStatValue", Object.class, Object.class, boolean.class, boolean.class);
    m.setAccessible(true);

    long micros = 1_735_734_896_123_456L;
    Object value = m.invoke(null, null, micros, false, true);

    Instant instant = Instant.parse("2025-01-01T12:34:56.123456Z");
    assertThat(value).isEqualTo(LocalDateTime.ofInstant(instant, ZoneOffset.UTC));
  }

  @Test
  void timestampStatsWithoutLogicalAnnotationFallbackToMicrosForTimestamptz() throws Exception {
    Method m =
        ParquetFooterStats.class.getDeclaredMethod(
            "timestampStatValue", Object.class, Object.class, boolean.class, boolean.class);
    m.setAccessible(true);

    long micros = 1_735_734_896_123_456L;
    Object value = m.invoke(null, null, micros, true, true);

    assertThat(value).isEqualTo(Instant.parse("2025-01-01T12:34:56.123456Z"));
  }

  @Test
  void timestampStatsDecodeInt96BinaryForTimestamp() throws Exception {
    Method m =
        ParquetFooterStats.class.getDeclaredMethod(
            "timestampStatValue", Object.class, Object.class, boolean.class, boolean.class);
    m.setAccessible(true);

    Instant instant = Instant.parse("2025-01-01T12:34:56.123456Z");
    Object value = m.invoke(null, null, int96Binary(instant), false, true);

    assertThat(value).isEqualTo(LocalDateTime.ofInstant(instant, ZoneOffset.UTC));
  }

  @Test
  void timestampStatsDecodeInt96BinaryForTimestamptz() throws Exception {
    Method m =
        ParquetFooterStats.class.getDeclaredMethod(
            "timestampStatValue", Object.class, Object.class, boolean.class, boolean.class);
    m.setAccessible(true);

    Instant instant = Instant.parse("2025-01-01T12:34:56.123456Z");
    Object value = m.invoke(null, null, int96Binary(instant), true, true);

    assertThat(value).isEqualTo(instant);
  }

  private static Object timeLogicalTypeAnnotation(TimeUnit unit) throws Exception {
    for (Method method : LogicalTypeAnnotation.class.getMethods()) {
      if (!method.getName().equals("timeType")) {
        continue;
      }
      Class<?>[] params = method.getParameterTypes();
      if (params.length == 2 && params[0] == boolean.class && params[1] == TimeUnit.class) {
        return method.invoke(null, false, unit);
      }
    }
    throw new IllegalStateException("Unable to locate LogicalTypeAnnotation.timeType");
  }

  @Test
  void aStringBoundCutInsideACharacterIsDropped() {
    PrimitiveType type =
        Types.required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("s");
    LogicalType string = LogicalType.of(LogicalKind.STRING);
    // "a" then the first three bytes of U+1F600's four: decoded leniently, "a" then U+FFFD.
    byte[] cut = {0x61, (byte) 0xF0, (byte) 0x9F, (byte) 0x98};
    assertThat(ParquetFooterStats.boundOf(stringStats(type, cut), chunkOf(type), string, false))
        .isNull();
  }

  @Test
  void aStringStoredAsPlainBinaryIsDecodedAsText() {
    PrimitiveType plain = Types.required(PrimitiveTypeName.BINARY).named("b");
    LogicalType string = LogicalType.of(LogicalKind.STRING);
    byte[] whole = "a\uD83D\uDE00".getBytes(StandardCharsets.UTF_8);
    assertThat(ParquetFooterStats.boundOf(stringStats(plain, whole), chunkOf(plain), string, false))
        .isEqualTo("a\uD83D\uDE00");
  }

  /** Statistics of column {@code type} whose minimum and maximum are {@code bound}. */
  private static Statistics<?> stringStats(PrimitiveType type, byte[] bound) {
    return Statistics.getBuilderForReading(type)
        .withMin(bound)
        .withMax(bound)
        .withNumNulls(0)
        .build();
  }

  /** A column chunk of {@code type}. */
  private static ColumnChunkMetaData chunkOf(PrimitiveType type) {
    return ColumnChunkMetaData.get(
        ColumnPath.get(type.getName()),
        type,
        CompressionCodecName.UNCOMPRESSED,
        null,
        Set.of(),
        Statistics.createStats(type),
        0,
        0,
        1,
        0,
        0);
  }

  private static Binary int96Binary(Instant instant) {
    long epochSeconds = instant.getEpochSecond();
    long epochDay = Math.floorDiv(epochSeconds, 86_400L);
    long secondsOfDay = Math.floorMod(epochSeconds, 86_400L);
    long nanosOfDay = secondsOfDay * 1_000_000_000L + instant.getNano();
    int julianDay = Math.toIntExact(epochDay + 2_440_588L);

    ByteBuffer buffer = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN);
    buffer.putLong(nanosOfDay);
    buffer.putInt(julianDay);
    return Binary.fromConstantByteArray(buffer.array());
  }
}
