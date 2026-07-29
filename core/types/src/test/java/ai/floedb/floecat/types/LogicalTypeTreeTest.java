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
  void emptyStructIsExplicitlyKnownEmpty() {
    LogicalType t = LogicalType.struct(List.of());
    assertThat(t.hasTypeTree()).isTrue();
    assertThat(t.fields()).isEmpty();
    assertThat(t).isNotEqualTo(LogicalType.of(LogicalKind.STRUCT));
    assertThat(LogicalTypeFormat.format(t)).isEqualTo("STRUCT<>");
    assertThat(LogicalTypeFormat.parse("STRUCT<>")).isEqualTo(t);
  }

  @Test
  void parseRejectsExcessiveNestingDepth() {
    String deep = "ARRAY<".repeat(1000) + "INT" + ">".repeat(1000);
    assertThatThrownBy(() -> LogicalTypeFormat.parse(deep))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("nesting depth");
  }

  @Test
  void parseAcceptsReasonableNestingDepth() {
    String ok = "ARRAY<".repeat(20) + "INT" + ">".repeat(20);
    LogicalType t = LogicalTypeFormat.parse(ok);
    assertThat(t.kind()).isEqualTo(LogicalKind.ARRAY);
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

  // ---------------------------------------------------------------------------
  // Protobuf round-trip (the lossless wire format — unlike the string grammar,
  // it preserves nested nullability)
  // ---------------------------------------------------------------------------

  @Test
  void protoRoundTripPreservesNestedNullability() {
    List<LogicalType> cases =
        List.of(
            // ARRAY<INT NOT NULL>
            LogicalType.array(INT, false),
            // MAP<STRING, DECIMAL(12,2) NOT NULL>
            LogicalType.map(STRING, LogicalType.decimal(12, 2), false),
            // STRUCT<sku: STRING NOT NULL, quantities: ARRAY<INT>>
            LogicalType.struct(
                List.of(
                    new LogicalField("sku", false, STRING),
                    new LogicalField("quantities", true, LogicalType.array(INT, true)))),
            // explicitly known empty struct
            LogicalType.struct(List.of()),
            // legacy non-parameterised tags survive too
            LogicalType.of(LogicalKind.ARRAY),
            LogicalType.of(LogicalKind.MAP),
            LogicalType.of(LogicalKind.STRUCT),
            LogicalType.of(LogicalKind.VARIANT),
            // scalars with parameters
            LogicalType.decimal(38, 9),
            LogicalType.temporal(LogicalKind.TIMESTAMP, 3),
            LogicalType.interval(IntervalRange.DAY_TO_SECOND, 2, 3),
            LogicalType.of(LogicalKind.INTERVAL));
    for (LogicalType t : cases) {
      assertThat(LogicalTypeProtoAdapter.fromProto(LogicalTypeProtoAdapter.toProto(t)))
          .as("proto round trip of %s", t)
          .isEqualTo(t);
    }
  }

  @Test
  void wireEncodesRequiredSoUnsetDefaultsToNullable() {
    // A producer that never sets the required bits must decode as nullable (the safe default).
    ai.floedb.floecat.types.rpc.LogicalType bareWire =
        ai.floedb.floecat.types.rpc.LogicalType.newBuilder()
            .setKind(ai.floedb.floecat.types.rpc.LogicalType.Kind.TK_ARRAY)
            .setArray(
                ai.floedb.floecat.types.rpc.ArrayShape.newBuilder()
                    .setElement(LogicalTypeProtoAdapter.toProto(INT)))
            .build();
    assertThat(LogicalTypeProtoAdapter.fromProto(bareWire).elementNullable()).isTrue();

    // And the inversion is explicit both ways: NOT NULL model <-> required wire.
    ai.floedb.floecat.types.rpc.LogicalType notNullWire =
        LogicalTypeProtoAdapter.toProto(LogicalType.array(INT, false));
    assertThat(notNullWire.getArray().getElementRequired()).isTrue();
    ai.floedb.floecat.types.rpc.LogicalType nullableWire =
        LogicalTypeProtoAdapter.toProto(
            LogicalType.struct(List.of(new LogicalField("a", true, INT))));
    assertThat(nullableWire.getStruct().getFields(0).getRequired()).isFalse();
  }

  @Test
  void stringGrammarLosesNullabilityButProtoDoesNot() {
    LogicalType t = LogicalType.array(INT, false);
    // The grammar has no nullability syntax: re-parsing defaults to nullable.
    assertThat(LogicalTypeFormat.parse(LogicalTypeFormat.format(t)).elementNullable()).isTrue();
    // The proto tree keeps it.
    assertThat(
            LogicalTypeProtoAdapter.fromProto(LogicalTypeProtoAdapter.toProto(t)).elementNullable())
        .isFalse();
  }

  // ---------------------------------------------------------------------------
  // Legacy column recovery (pre-migration logical_type string in unknown fields)
  // ---------------------------------------------------------------------------

  private static ai.floedb.floecat.query.rpc.SchemaColumn legacyColumn(String legacyLogicalType) {
    // Simulate a column persisted before the typed migration: field 2 (the reserved
    // logical_type string) arrives in the parsed message's unknown fields.
    com.google.protobuf.UnknownFieldSet unknown =
        com.google.protobuf.UnknownFieldSet.newBuilder()
            .addField(
                2,
                com.google.protobuf.UnknownFieldSet.Field.newBuilder()
                    .addLengthDelimited(
                        com.google.protobuf.ByteString.copyFromUtf8(legacyLogicalType))
                    .build())
            .build();
    return ai.floedb.floecat.query.rpc.SchemaColumn.newBuilder()
        .setName("c")
        .setUnknownFields(unknown)
        .build();
  }

  @Test
  void upgradeLegacyColumnRecoversScalarType() {
    var upgraded = LogicalTypeProtoAdapter.upgradeLegacyColumn(legacyColumn("DECIMAL(10,2)"));
    assertThat(upgraded.hasType()).isTrue();
    assertThat(LogicalTypeProtoAdapter.columnType(upgraded)).isEqualTo(LogicalType.decimal(10, 2));
  }

  @Test
  void upgradeLegacyColumnRecoversComplexTagAsLegacyForm() {
    var upgraded = LogicalTypeProtoAdapter.upgradeLegacyColumn(legacyColumn("ARRAY"));
    assertThat(upgraded.hasType()).isTrue();
    LogicalType t = LogicalTypeProtoAdapter.columnType(upgraded);
    assertThat(t.kind()).isEqualTo(LogicalKind.ARRAY);
    assertThat(t.hasTypeTree()).isFalse();
  }

  @Test
  void upgradeLegacyColumnRecoversBytesWrittenByPreChangeSchema() throws Exception {
    // Serialize field 2 exactly as the pre-change proto did (a known string field), then parse
    // those bytes as the NEW SchemaColumn — proving real pre-migration bytes arrive as the
    // unknown-field shape the recovery reads, not just that our hand-built fixture does.
    java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
    com.google.protobuf.CodedOutputStream out =
        com.google.protobuf.CodedOutputStream.newInstance(bytes);
    out.writeString(1, "amount"); // name
    out.writeString(2, "DECIMAL(12,2)"); // legacy logical_type, field 2, wire type 2
    out.writeBool(4, true); // nullable
    out.flush();

    ai.floedb.floecat.query.rpc.SchemaColumn parsed =
        ai.floedb.floecat.query.rpc.SchemaColumn.parseFrom(bytes.toByteArray());
    assertThat(parsed.hasType()).isFalse();
    assertThat(parsed.getName()).isEqualTo("amount");

    var upgraded = LogicalTypeProtoAdapter.upgradeLegacyColumn(parsed);
    assertThat(upgraded.hasType()).isTrue();
    assertThat(LogicalTypeProtoAdapter.columnType(upgraded)).isEqualTo(LogicalType.decimal(12, 2));
    assertThat(upgraded.getNullable()).isTrue();
  }

  @Test
  void upgradeLegacyColumnLeavesTypedAndUnrecoverableColumnsAlone() {
    // Already typed: unchanged.
    var typed =
        ai.floedb.floecat.query.rpc.SchemaColumn.newBuilder()
            .setName("c")
            .setType(LogicalTypeProtoAdapter.toProto(INT))
            .build();
    assertThat(LogicalTypeProtoAdapter.upgradeLegacyColumn(typed)).isSameAs(typed);
    // No legacy value: unchanged.
    var bare = ai.floedb.floecat.query.rpc.SchemaColumn.newBuilder().setName("c").build();
    assertThat(LogicalTypeProtoAdapter.upgradeLegacyColumn(bare)).isSameAs(bare);
    // Unparseable legacy value: unchanged (readers degrade as for any untyped column).
    var junk = legacyColumn("NOT_A_TYPE");
    assertThat(LogicalTypeProtoAdapter.upgradeLegacyColumn(junk)).isSameAs(junk);
  }

  @Test
  void fromProtoRejectsUnspecifiedKind() {
    ai.floedb.floecat.types.rpc.LogicalType unspecified =
        ai.floedb.floecat.types.rpc.LogicalType.newBuilder().build();
    assertThatThrownBy(() -> LogicalTypeProtoAdapter.fromProto(unspecified))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unrecognized logical type kind");
  }

  @Test
  void fromProtoRejectsShapeKindMismatch() {
    ai.floedb.floecat.types.rpc.LogicalType arrayShapeOnMapKind =
        ai.floedb.floecat.types.rpc.LogicalType.newBuilder()
            .setKind(ai.floedb.floecat.types.rpc.LogicalType.Kind.TK_MAP)
            .setArray(
                ai.floedb.floecat.types.rpc.ArrayShape.newBuilder()
                    .setElement(LogicalTypeProtoAdapter.toProto(INT))
                    .setElementRequired(false))
            .build();
    assertThatThrownBy(() -> LogicalTypeProtoAdapter.fromProto(arrayShapeOnMapKind))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("array shape on non-ARRAY kind");

    ai.floedb.floecat.types.rpc.LogicalType structShapeOnIntKind =
        ai.floedb.floecat.types.rpc.LogicalType.newBuilder()
            .setKind(ai.floedb.floecat.types.rpc.LogicalType.Kind.TK_INT)
            .setStruct(ai.floedb.floecat.types.rpc.StructShape.newBuilder())
            .build();
    assertThatThrownBy(() -> LogicalTypeProtoAdapter.fromProto(structShapeOnIntKind))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("struct shape on non-STRUCT kind");
  }

  @Test
  void formatTagKeepsContainersFlatAndScalarsFull() {
    assertThat(LogicalTypeFormat.formatTag(LogicalType.array(INT, true))).isEqualTo("ARRAY");
    assertThat(LogicalTypeFormat.formatTag(LogicalType.map(STRING, INT, true))).isEqualTo("MAP");
    assertThat(
            LogicalTypeFormat.formatTag(
                LogicalType.struct(List.of(new LogicalField("a", true, INT)))))
        .isEqualTo("STRUCT");
    assertThat(LogicalTypeFormat.formatTag(LogicalType.of(LogicalKind.VARIANT)))
        .isEqualTo("VARIANT");
    assertThat(LogicalTypeFormat.formatTag(LogicalType.decimal(10, 2))).isEqualTo("DECIMAL(10,2)");
    assertThat(LogicalTypeFormat.formatTag(LogicalType.temporal(LogicalKind.TIMESTAMP, 3)))
        .isEqualTo("TIMESTAMP(3)");
  }

  @Test
  void toProtoRejectsExcessiveNestingDepth() {
    LogicalType deep = INT;
    for (int i = 0; i < 200; i++) {
      deep = LogicalType.array(deep, true);
    }
    LogicalType finalDeep = deep;
    assertThatThrownBy(() -> LogicalTypeProtoAdapter.toProto(finalDeep))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("nesting depth");
  }

  @Test
  void fromProtoRejectsExcessiveNestingDepth() {
    ai.floedb.floecat.types.rpc.LogicalType leaf =
        LogicalTypeProtoAdapter.toProto(LogicalType.of(LogicalKind.INT));
    ai.floedb.floecat.types.rpc.LogicalType deep = leaf;
    for (int i = 0; i < 200; i++) {
      deep =
          ai.floedb.floecat.types.rpc.LogicalType.newBuilder()
              .setKind(ai.floedb.floecat.types.rpc.LogicalType.Kind.TK_ARRAY)
              .setArray(
                  ai.floedb.floecat.types.rpc.ArrayShape.newBuilder()
                      .setElement(deep)
                      .setElementRequired(false))
              .build();
    }
    ai.floedb.floecat.types.rpc.LogicalType finalDeep = deep;
    assertThatThrownBy(() -> LogicalTypeProtoAdapter.fromProto(finalDeep))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("nesting depth");
  }
}
