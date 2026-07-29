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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the recursive nested type tree on {@link LogicalType} and its string grammar in
 * {@link LogicalTypeFormat}.
 */
class LogicalTypeTreeTest {

  private static final LogicalType INT = LogicalType.of(LogicalKind.INT);
  private static final LogicalType STRING = LogicalType.of(LogicalKind.STRING);

  // ---------------------------------------------------------------------------
  // Model
  // ---------------------------------------------------------------------------

  @Test
  void arrayCarriesElementTypeAndNullability() {
    LogicalType t = LogicalType.array(INT, false);
    assertThat(t.kind()).isEqualTo(LogicalKind.ARRAY);
    assertThat(t.element()).isEqualTo(INT);
    assertThat(t.elementNullable()).isFalse();
    assertThat(t.hasTypeTree()).isTrue();
  }

  @Test
  void mapCarriesKeyValueTypes() {
    LogicalType t = LogicalType.map(STRING, INT, true);
    assertThat(t.kind()).isEqualTo(LogicalKind.MAP);
    assertThat(t.key()).isEqualTo(STRING);
    assertThat(t.value()).isEqualTo(INT);
    assertThat(t.valueNullable()).isTrue();
  }

  @Test
  void structCarriesFieldsInOrder() {
    LogicalType t =
        LogicalType.struct(
            List.of(new LogicalField("a", true, INT), new LogicalField("b", false, STRING)));
    assertThat(t.kind()).isEqualTo(LogicalKind.STRUCT);
    assertThat(t.fields()).hasSize(2);
    assertThat(t.fields().get(0).name()).isEqualTo("a");
    assertThat(t.fields().get(1).nullable()).isFalse();
  }

  @Test
  void bareContainerTagsHaveNoTree() {
    for (LogicalKind kind :
        List.of(LogicalKind.ARRAY, LogicalKind.MAP, LogicalKind.STRUCT, LogicalKind.VARIANT)) {
      LogicalType t = LogicalType.of(kind);
      assertThat(t.hasTypeTree()).isFalse();
      assertThat(t.element()).isNull();
      assertThat(t.fields()).isNull();
    }
  }

  @Test
  void nestedTreesCompose() {
    // ARRAY<STRUCT<sku: STRING, quantities: ARRAY<INT>>>
    LogicalType t =
        LogicalType.array(
            LogicalType.struct(
                List.of(
                    new LogicalField("sku", true, STRING),
                    new LogicalField("quantities", true, LogicalType.array(INT, true)))),
            true);
    assertThat(t.element().fields().get(1).type().element()).isEqualTo(INT);
  }

  @Test
  void treeParticipatesInEquality() {
    assertThat(LogicalType.array(INT, true)).isEqualTo(LogicalType.array(INT, true));
    assertThat(LogicalType.array(INT, true)).isNotEqualTo(LogicalType.array(STRING, true));
    assertThat(LogicalType.array(INT, true)).isNotEqualTo(LogicalType.array(INT, false));
    assertThat(LogicalType.array(INT, true)).isNotEqualTo(LogicalType.of(LogicalKind.ARRAY));
  }

  @Test
  void emptyStructFieldsRejected() {
    assertThatThrownBy(() -> LogicalType.struct(List.of()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void blankFieldNameRejected() {
    assertThatThrownBy(() -> new LogicalField("  ", true, INT))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // ---------------------------------------------------------------------------
  // format()
  // ---------------------------------------------------------------------------

  @Test
  void formatNestedTypes() {
    assertThat(LogicalTypeFormat.format(LogicalType.array(INT, true))).isEqualTo("ARRAY<INT>");
    assertThat(LogicalTypeFormat.format(LogicalType.map(STRING, LogicalType.decimal(10, 2), true)))
        .isEqualTo("MAP<STRING, DECIMAL(10,2)>");
    assertThat(
            LogicalTypeFormat.format(
                LogicalType.struct(
                    List.of(
                        new LogicalField("sku", true, STRING),
                        new LogicalField("quantities", true, LogicalType.array(INT, true))))))
        .isEqualTo("STRUCT<sku: STRING, quantities: ARRAY<INT>>");
  }

  @Test
  void formatQuotesNonIdentifierFieldNames() {
    LogicalType t =
        LogicalType.struct(List.of(new LogicalField("weird name, with \"stuff\"", true, INT)));
    assertThat(LogicalTypeFormat.format(t))
        .isEqualTo("STRUCT<\"weird name, with \"\"stuff\"\"\": INT>");
  }

  @Test
  void formatBareContainerStaysFlat() {
    assertThat(LogicalTypeFormat.format(LogicalType.of(LogicalKind.ARRAY))).isEqualTo("ARRAY");
    assertThat(LogicalTypeFormat.format(LogicalType.of(LogicalKind.MAP))).isEqualTo("MAP");
    assertThat(LogicalTypeFormat.format(LogicalType.of(LogicalKind.STRUCT))).isEqualTo("STRUCT");
  }

  // ---------------------------------------------------------------------------
  // parse()
  // ---------------------------------------------------------------------------

  @Test
  void parseNestedTypesRoundTrip() {
    List<LogicalType> cases =
        List.of(
            LogicalType.array(INT, true),
            LogicalType.array(LogicalType.array(INT, true), true),
            LogicalType.map(STRING, LogicalType.array(LogicalType.decimal(38, 9), true), true),
            LogicalType.struct(
                List.of(
                    new LogicalField("sku", true, STRING),
                    new LogicalField("quantities", true, LogicalType.array(INT, true)))),
            LogicalType.array(
                LogicalType.struct(
                    List.of(
                        new LogicalField("a", true, LogicalType.temporal(LogicalKind.TIMESTAMP, 3)),
                        new LogicalField("weird name, with \"stuff\"", true, INT))),
                true));
    for (LogicalType t : cases) {
      assertThat(LogicalTypeFormat.parse(LogicalTypeFormat.format(t))).isEqualTo(t);
    }
  }

  @Test
  void parseIsCaseInsensitiveForTypeNamesButPreservesFieldNames() {
    LogicalType t = LogicalTypeFormat.parse("array< struct< Sku : string , n : bigint > >");
    assertThat(t.kind()).isEqualTo(LogicalKind.ARRAY);
    assertThat(t.element().fields().get(0).name()).isEqualTo("Sku");
    assertThat(t.element().fields().get(0).type()).isEqualTo(STRING);
    assertThat(t.element().fields().get(1).type()).isEqualTo(INT);
  }

  @Test
  void parseBareContainerTags() {
    assertThat(LogicalTypeFormat.parse("ARRAY")).isEqualTo(LogicalType.of(LogicalKind.ARRAY));
    assertThat(LogicalTypeFormat.parse("MAP")).isEqualTo(LogicalType.of(LogicalKind.MAP));
    assertThat(LogicalTypeFormat.parse("STRUCT")).isEqualTo(LogicalType.of(LogicalKind.STRUCT));
  }

  @Test
  void parseRejectsMalformedNestedTypes() {
    for (String bad :
        List.of(
            "ARRAY<",
            "ARRAY<>",
            "ARRAY<INT",
            "ARRAY<INT>>",
            "MAP<STRING>",
            "MAP<STRING, INT",
            "STRUCT<>",
            "STRUCT<a>",
            "STRUCT<a: >",
            "STRUCT<: INT>",
            "ARRAY<NOT_A_TYPE>")) {
      assertThatThrownBy(() -> LogicalTypeFormat.parse(bad))
          .as("parse(%s)", bad)
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Test
  void parseStillRejectsParenParameterisedContainers() {
    assertThatThrownBy(() -> LogicalTypeFormat.parse("ARRAY(INT)"))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
