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

package ai.floedb.floecat.schema.identity.delta;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.Test;

class ColumnMappingModeTest {

  @Test
  void readsTheConfiguredModeFromProperties() {
    assertThat(ColumnMappingMode.fromTableProperties(null)).isEqualTo(ColumnMappingMode.NONE);
    assertThat(ColumnMappingMode.fromTableProperties(Map.of())).isEqualTo(ColumnMappingMode.NONE);
    assertThat(ColumnMappingMode.fromTableProperties(Map.of(ColumnMappingMode.PROPERTY, " Name ")))
        .isEqualTo(ColumnMappingMode.NAME);
    assertThat(ColumnMappingMode.fromTableProperties(Map.of(ColumnMappingMode.PROPERTY, "ID")))
        .isEqualTo(ColumnMappingMode.ID);
  }

  @Test
  void rejectsAnUnrecognizedMode() {
    assertThatThrownBy(
            () ->
                ColumnMappingMode.fromTableProperties(
                    Map.of(ColumnMappingMode.PROPERTY, "physical")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(ColumnMappingMode.PROPERTY);
  }

  @Test
  void configuredMappingWithoutProtocolSupportReadsAsNone() {
    assertThat(ColumnMappingMode.NAME.effective(false)).isEqualTo(ColumnMappingMode.NONE);
    assertThat(ColumnMappingMode.ID.effective(false)).isEqualTo(ColumnMappingMode.NONE);
    assertThat(ColumnMappingMode.NAME.effective(true)).isEqualTo(ColumnMappingMode.NAME);
    assertThat(ColumnMappingMode.ID.effective(true)).isEqualTo(ColumnMappingMode.ID);
    assertThat(ColumnMappingMode.NONE.effective(true)).isEqualTo(ColumnMappingMode.NONE);
  }

  @Test
  void unmappedTablesNeverConsultTheProtocol() {
    assertThat(
            ColumnMappingMode.effectiveFromTableProperties(
                Map.of(),
                () -> {
                  throw new AssertionError("the protocol must not be read");
                }))
        .isEqualTo(ColumnMappingMode.NONE);
  }

  @Test
  void mappedTablesConsultTheProtocol() {
    Map<String, String> properties = Map.of(ColumnMappingMode.PROPERTY, "name");

    assertThat(ColumnMappingMode.effectiveFromTableProperties(properties, () -> true))
        .isEqualTo(ColumnMappingMode.NAME);
    assertThat(ColumnMappingMode.effectiveFromTableProperties(properties, () -> false))
        .isEqualTo(ColumnMappingMode.NONE);
  }
}
