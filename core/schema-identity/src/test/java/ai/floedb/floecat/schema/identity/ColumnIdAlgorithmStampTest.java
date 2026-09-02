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

package ai.floedb.floecat.schema.identity;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.schema.identity.delta.ColumnMappingMode;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ColumnIdAlgorithmStampTest {

  @Test
  void icebergStampsFieldId() {
    assertThat(ColumnIdAlgorithmStamp.forTable(TableFormat.TF_ICEBERG, Map.of()))
        .isEqualTo(ColumnIdAlgorithm.CID_FIELD_ID);
    assertThat(ColumnIdAlgorithmStamp.forTable("iceberg", null))
        .isEqualTo(ColumnIdAlgorithm.CID_FIELD_ID);
  }

  @ParameterizedTest
  @ValueSource(strings = {"id", "name", "ID", " Name "})
  void deltaWithColumnMappingStampsFieldId(String mode) {
    var properties = Map.of(ColumnMappingMode.PROPERTY_KEY, mode);

    assertThat(ColumnIdAlgorithmStamp.forTable(TableFormat.TF_DELTA, properties))
        .isEqualTo(ColumnIdAlgorithm.CID_FIELD_ID);
  }

  @Test
  void deltaWithoutColumnMappingStampsPathOrdinal() {
    assertThat(ColumnIdAlgorithmStamp.forTable(TableFormat.TF_DELTA, Map.of()))
        .isEqualTo(ColumnIdAlgorithm.CID_PATH_ORDINAL);
    assertThat(
            ColumnIdAlgorithmStamp.forTable(
                TableFormat.TF_DELTA, Map.of(ColumnMappingMode.PROPERTY_KEY, "none")))
        .isEqualTo(ColumnIdAlgorithm.CID_PATH_ORDINAL);
  }

  @Test
  void unrecognizedModeIsRejectedRatherThanTreatedAsDisabled() {
    // Falling back to NONE here would pick the unmapped identity scheme and read physical names
    // as though they were logical ones.
    assertThatThrownBy(
            () ->
                ColumnIdAlgorithmStamp.forTable(
                    TableFormat.TF_DELTA, Map.of(ColumnMappingMode.PROPERTY_KEY, "sideways")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported delta.columnMapping.mode='sideways'");
  }

  @Test
  void missingModePropertyMeansNone() {
    assertThat(ColumnMappingMode.fromProperties(Map.of())).isEqualTo(ColumnMappingMode.NONE);
    assertThat(ColumnMappingMode.fromProperties(null)).isEqualTo(ColumnMappingMode.NONE);
    assertThat(ColumnMappingMode.fromProperties(Map.of(ColumnMappingMode.PROPERTY_KEY, "  ")))
        .isEqualTo(ColumnMappingMode.NONE);
  }

  @Test
  void parsesColumnMappingMode() {
    assertThat(ColumnMappingMode.parse("id")).isEqualTo(ColumnMappingMode.ID);
    assertThat(ColumnMappingMode.parse("NAME")).isEqualTo(ColumnMappingMode.NAME);
    assertThat(ColumnMappingMode.parse("none")).isEqualTo(ColumnMappingMode.NONE);
    assertThat(ColumnMappingMode.parse(null)).isEqualTo(ColumnMappingMode.NONE);
    assertThat(ColumnMappingMode.parse("")).isEqualTo(ColumnMappingMode.NONE);
    assertThat(ColumnMappingMode.NAME.isEnabled()).isTrue();
    assertThat(ColumnMappingMode.NONE.isEnabled()).isFalse();
    assertThatThrownBy(() -> ColumnMappingMode.parse("sideways"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void unsupportedFormatsFailFast() {
    assertThatThrownBy(() -> ColumnIdAlgorithmStamp.forTable(TableFormat.TF_UNKNOWN, Map.of()))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> ColumnIdAlgorithmStamp.forTable("hudi", Map.of()))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> ColumnIdAlgorithmStamp.forTable("", Map.of()))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
