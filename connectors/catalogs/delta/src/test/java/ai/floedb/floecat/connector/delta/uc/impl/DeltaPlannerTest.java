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

package ai.floedb.floecat.connector.delta.uc.impl;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.types.LogicalKind;
import ai.floedb.floecat.types.LogicalType;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.statistics.DataFileStatistics;
import io.delta.kernel.types.FieldMetadata;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

class DeltaPlannerTest {

  @Test
  void canonicalizeStatValueConvertsTimestampMicrosToLocalDateTime() {
    Object canonical =
        DeltaPlanner.canonicalizeStatValue(
            LogicalType.of(LogicalKind.TIMESTAMP), 1_735_734_896_123_456L);

    assertThat(canonical).isEqualTo(LocalDateTime.of(2025, 1, 1, 12, 34, 56, 123_456_000));
  }

  @Test
  void canonicalizeStatValueConvertsTimestamptzMicrosToInstant() {
    Object canonical =
        DeltaPlanner.canonicalizeStatValue(
            LogicalType.of(LogicalKind.TIMESTAMPTZ), 1_735_734_896_123_456L);

    assertThat(canonical).isEqualTo(Instant.parse("2025-01-01T12:34:56.123456Z"));
  }

  @Test
  void canonicalizeStatValueLeavesNonTemporalValuesUntouched() {
    Object canonical =
        DeltaPlanner.canonicalizeStatValue(LogicalType.of(LogicalKind.STRING), "already-canonical");

    assertThat(canonical).isEqualTo("already-canonical");
  }

  @Test
  void statsNameMapIncludesPhysicalColumnMappingNames() {
    StructType schema =
        new StructType()
            .add(
                new StructField(
                    "logical_id",
                    IntegerType.INTEGER,
                    true,
                    FieldMetadata.builder()
                        .putString(DeltaPlanner.COLUMN_MAPPING_PHYSICAL_NAME_KEY, "col-123")
                        .build()))
            .add("plain_name", StringType.STRING, true);

    Map<String, String> mapping = DeltaPlanner.statsNameMap(schema);

    assertThat(mapping)
        .containsEntry("logical_id", "logical_id")
        .containsEntry("col-123", "logical_id")
        .containsEntry("plain_name", "plain_name");
  }

  @Test
  void physicalNameReadsDeltaColumnMappingMetadata() {
    FieldMetadata metadata =
        FieldMetadata.builder()
            .putString(DeltaPlanner.COLUMN_MAPPING_PHYSICAL_NAME_KEY, "col-456")
            .build();

    assertThat(DeltaPlanner.physicalName(metadata)).isEqualTo("col-456");
  }

  @Test
  void checkpointStructStatsFromAddRowReadsStatsParsedAndMapsPhysicalNames() {
    StructType addSchema =
        new StructType()
            .add("path", StringType.STRING, true)
            .add(
                DeltaPlanner.STATS_PARSED_FIELD,
                new StructType()
                    .add("numRecords", LongType.LONG, true)
                    .add(
                        "minValues",
                        new StructType()
                            .add("col-123", IntegerType.INTEGER, true)
                            .add("plain_name", StringType.STRING, true),
                        true)
                    .add(
                        "maxValues",
                        new StructType()
                            .add("col-123", IntegerType.INTEGER, true)
                            .add("plain_name", StringType.STRING, true),
                        true)
                    .add(
                        "nullCount",
                        new StructType()
                            .add("col-123", LongType.LONG, true)
                            .add("plain_name", LongType.LONG, true),
                        true),
                true);

    StructType statsSchema = (StructType) addSchema.at(1).getDataType();
    StructType minSchema =
        (StructType) statsSchema.at(statsSchema.indexOf("minValues")).getDataType();
    StructType maxSchema =
        (StructType) statsSchema.at(statsSchema.indexOf("maxValues")).getDataType();
    StructType nullCountSchema =
        (StructType) statsSchema.at(statsSchema.indexOf("nullCount")).getDataType();
    GenericRow statsRow =
        new RowBuilder(statsSchema)
            .put("numRecords", 10L)
            .put(
                "minValues",
                new RowBuilder(minSchema).put("col-123", 7).put("plain_name", "a").row())
            .put(
                "maxValues",
                new RowBuilder(maxSchema).put("col-123", 9).put("plain_name", "z").row())
            .put(
                "nullCount",
                new RowBuilder(nullCountSchema).put("col-123", 2L).put("plain_name", 1L).row())
            .row();

    GenericRow addRow =
        new RowBuilder(addSchema)
            .put("path", "part-000.parquet")
            .put(DeltaPlanner.STATS_PARSED_FIELD, statsRow)
            .row();

    Map<String, String> statsNameToLogical =
        Map.of("col-123", "logical_id", "logical_id", "logical_id", "plain_name", "plain_name");
    Optional<DataFileStatistics> stats =
        DeltaPlanner.checkpointStructStatsFromAddRow(
            addRow, statsNameToLogical, Set.of("logical_id", "plain_name"));

    assertThat(stats).isPresent();
    assertThat(stats.get().getNumRecords()).isEqualTo(10L);
    assertThat(stats.get().getMinValues())
        .containsEntry(new Column("logical_id"), io.delta.kernel.expressions.Literal.ofInt(7))
        .containsEntry(new Column("plain_name"), io.delta.kernel.expressions.Literal.ofString("a"));
    assertThat(stats.get().getMaxValues())
        .containsEntry(new Column("logical_id"), io.delta.kernel.expressions.Literal.ofInt(9))
        .containsEntry(new Column("plain_name"), io.delta.kernel.expressions.Literal.ofString("z"));
    assertThat(stats.get().getNullCount())
        .containsEntry(new Column("logical_id"), 2L)
        .containsEntry(new Column("plain_name"), 1L);
  }

  @Test
  void absoluteDataPathResolvesRelativeAddPathAgainstTableRoot() {
    assertThat(DeltaPlanner.absoluteDataPath("s3://bucket/table", "part-000.parquet"))
        .isEqualTo("s3://bucket/table/part-000.parquet");
    assertThat(DeltaPlanner.absoluteDataPath("s3://bucket/table", "s3://other/part-000.parquet"))
        .isEqualTo("s3://other/part-000.parquet");
  }

  private static final class RowBuilder {
    private final StructType schema;
    private final Map<Integer, Object> values = new LinkedHashMap<>();

    private RowBuilder(io.delta.kernel.types.DataType dataType) {
      this((StructType) dataType);
    }

    private RowBuilder(StructType schema) {
      this.schema = schema;
    }

    private RowBuilder put(String fieldName, Object value) {
      values.put(schema.indexOf(fieldName), value);
      return this;
    }

    private GenericRow row() {
      return new GenericRow(schema, values);
    }
  }
}
