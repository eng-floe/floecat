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

import ai.floedb.floecat.connector.delta.identity.ColumnMappingMode;
import ai.floedb.floecat.connector.delta.identity.DeltaResolvedSchema;
import ai.floedb.floecat.connector.delta.identity.DeltaSchemaResolver;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.skipping.StatsSchemaHelper;
import io.delta.kernel.types.FieldMetadata;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Covers the Kernel-facing half of column mapping; the decisions themselves live in core. */
class DeltaColumnMappingTest {

  @Test
  void effectiveColumnMappingModeRequiresProtocolSupport() {
    assertThat(effectiveMode(ColumnMappingMode.NAME, new Protocol(1, 2)))
        .isEqualTo(ColumnMappingMode.NONE);
    assertThat(effectiveMode(ColumnMappingMode.NAME, new Protocol(2, 5)))
        .isEqualTo(ColumnMappingMode.NAME);
    assertThat(effectiveMode(ColumnMappingMode.NAME, new Protocol(3, 7, Set.of(), Set.of())))
        .isEqualTo(ColumnMappingMode.NONE);
    assertThat(
            effectiveMode(
                ColumnMappingMode.ID,
                new Protocol(3, 7, Set.of("columnMapping"), Set.of("columnMapping"))))
        .isEqualTo(ColumnMappingMode.ID);
  }

  @Test
  void physicalNameReadsDeltaColumnMappingMetadata() {
    FieldMetadata metadata =
        FieldMetadata.builder().putString(DeltaColumnMapping.PHYSICAL_NAME_KEY, "col-456").build();

    assertThat(DeltaColumnMapping.physicalName(metadata)).isEqualTo("col-456");
    assertThat(DeltaColumnMapping.physicalName(null)).isNull();
    assertThat(DeltaColumnMapping.physicalName(FieldMetadata.empty())).isNull();
  }

  @Test
  void resolvesNestedPhysicalStatisticsNamesToLogicalPaths() {
    DeltaResolvedSchema resolved = mappedNestedSchema(ColumnMappingMode.NAME);

    assertThat(
            DeltaColumnMapping.logicalNameForStats(
                new Column(new String[] {"col-address", "col-zip"}),
                resolved,
                Set.of("address.zip")))
        .isEqualTo("address.zip");
  }

  @Test
  void statisticsColumnsOutsideTheRequestedSetResolveToNothing() {
    DeltaResolvedSchema resolved = mappedNestedSchema(ColumnMappingMode.NAME);

    assertThat(
            DeltaColumnMapping.logicalNameForStats(
                new Column(new String[] {"col-address", "col-zip"}), resolved, Set.of()))
        .isNull();
    assertThat(DeltaColumnMapping.logicalNameForStats(null, resolved, Set.of("address.zip")))
        .isNull();
  }

  @Test
  void footerNameMappingResolvesNestedPhysicalPath() {
    DeltaResolvedSchema resolved = mappedNestedSchema(ColumnMappingMode.NAME);

    assertThat(
            DeltaColumnMapping.logicalNameForFooter(
                List.of("col-address", "col-zip"), 999, resolved, Set.of("address.zip")))
        .isEqualTo("address.zip");
  }

  @Test
  void footerIdMappingUsesFieldIdRatherThanPhysicalPath() {
    DeltaResolvedSchema resolved = mappedNestedSchema(ColumnMappingMode.ID);

    assertThat(
            DeltaColumnMapping.logicalNameForFooter(
                List.of("not", "the", "physical", "path"), 2, resolved, Set.of("address.zip")))
        .isEqualTo("address.zip");
    assertThat(
            DeltaColumnMapping.logicalNameForFooter(
                List.of("col-address", "col-zip"), null, resolved, Set.of("address.zip")))
        .isNull();
  }

  @Test
  void emptyStatsSelectionDoesNotProjectEveryCheckpointColumn() {
    StructType schema =
        new StructType()
            .add("first", IntegerType.INTEGER, true)
            .add("second", StringType.STRING, true);

    StructType projected = DeltaColumnMapping.projectedStatsDataSchema(schema, Set.of());
    StructType statsSchema = StatsSchemaHelper.getStatsSchema(projected, Set.of());

    assertThat(projected.length()).isZero();
    assertThat(statsSchema.fieldNames())
        .doesNotContain(StatsSchemaHelper.MIN, StatsSchemaHelper.MAX, StatsSchemaHelper.NULL_COUNT);
  }

  @Test
  void projectedStatsDataSchemaRetainsTheParentOfANestedSelection() {
    StructType schema =
        new StructType()
            .add(
                "address",
                new StructType()
                    .add("city", StringType.STRING, true)
                    .add("zip", IntegerType.INTEGER, true),
                true)
            .add("unselected", LongType.LONG, true);

    StructType projected =
        DeltaColumnMapping.projectedStatsDataSchema(schema, Set.of("address.zip"));

    assertThat(projected.fieldNames()).containsExactly("address");
    assertThat(((StructType) projected.get("address").getDataType()).fieldNames())
        .containsExactly("zip");
  }

  private static ColumnMappingMode effectiveMode(
      ColumnMappingMode configuredMode, Protocol protocol) {
    return configuredMode.effective(DeltaColumnMapping.supportsColumnMapping(protocol));
  }

  private static DeltaResolvedSchema mappedNestedSchema(ColumnMappingMode mode) {
    FieldMetadata addressMetadata =
        FieldMetadata.builder()
            .putLong(DeltaSchemaResolver.COLUMN_ID, 1L)
            .putString(DeltaSchemaResolver.PHYSICAL_NAME, "col-address")
            .build();
    FieldMetadata zipMetadata =
        FieldMetadata.builder()
            .putLong(DeltaSchemaResolver.COLUMN_ID, 2L)
            .putString(DeltaSchemaResolver.PHYSICAL_NAME, "col-zip")
            .build();
    StructType schema =
        new StructType()
            .add(
                new StructField(
                    "address",
                    new StructType()
                        .add(new StructField("zip", IntegerType.INTEGER, true, zipMetadata)),
                    true,
                    addressMetadata));
    return DeltaSchemaResolver.resolve(schema, mode);
  }
}
