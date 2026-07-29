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

package ai.floedb.floecat.arrow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.types.LogicalTypeProtoAdapter;
import java.util.List;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

class ArrowSchemaUtilTest {

  @Test
  void mapsCanonicalIntToSigned64BitArrowInt() {
    SchemaColumn column =
        SchemaColumn.newBuilder()
            .setName("id")
            .setType(LogicalTypeProtoAdapter.parseToProto("INT"))
            .build();

    Schema schema = ArrowSchemaUtil.toArrowSchema(List.of(column));
    ArrowType.Int arrowType = (ArrowType.Int) schema.getFields().get(0).getType();

    assertThat(arrowType.getBitWidth()).isEqualTo(64);
    assertThat(arrowType.getIsSigned()).isTrue();
  }

  @Test
  void mapsIntegerAliasToSigned64BitArrowInt() {
    SchemaColumn column =
        SchemaColumn.newBuilder()
            .setName("id")
            .setType(LogicalTypeProtoAdapter.parseToProto("INTEGER"))
            .build();

    Schema schema = ArrowSchemaUtil.toArrowSchema(List.of(column));
    ArrowType.Int arrowType = (ArrowType.Int) schema.getFields().get(0).getType();

    assertThat(arrowType.getBitWidth()).isEqualTo(64);
    assertThat(arrowType.getIsSigned()).isTrue();
  }

  @Test
  void mapsSmallintAliasToSigned64BitArrowInt() {
    SchemaColumn column =
        SchemaColumn.newBuilder()
            .setName("id")
            .setType(LogicalTypeProtoAdapter.parseToProto("SMALLINT"))
            .build();

    Schema schema = ArrowSchemaUtil.toArrowSchema(List.of(column));
    ArrowType.Int arrowType = (ArrowType.Int) schema.getFields().get(0).getType();

    assertThat(arrowType.getBitWidth()).isEqualTo(64);
    assertThat(arrowType.getIsSigned()).isTrue();
  }

  @Test
  void mapsCanonicalTemporalTypesToArrowTemporalTypes() {
    Schema schema =
        ArrowSchemaUtil.toArrowSchema(
            List.of(
                SchemaColumn.newBuilder()
                    .setName("d")
                    .setType(LogicalTypeProtoAdapter.parseToProto("DATE"))
                    .build(),
                SchemaColumn.newBuilder()
                    .setName("t")
                    .setType(LogicalTypeProtoAdapter.parseToProto("TIME"))
                    .build(),
                SchemaColumn.newBuilder()
                    .setName("ts")
                    .setType(LogicalTypeProtoAdapter.parseToProto("TIMESTAMP"))
                    .build(),
                SchemaColumn.newBuilder()
                    .setName("tstz")
                    .setType(LogicalTypeProtoAdapter.parseToProto("TIMESTAMPTZ"))
                    .build()));

    ArrowType.Date dateType = (ArrowType.Date) schema.getFields().get(0).getType();
    ArrowType.Time timeType = (ArrowType.Time) schema.getFields().get(1).getType();
    ArrowType.Timestamp timestampType = (ArrowType.Timestamp) schema.getFields().get(2).getType();
    ArrowType.Timestamp timestamptzType = (ArrowType.Timestamp) schema.getFields().get(3).getType();

    assertThat(dateType.getUnit()).isEqualTo(DateUnit.DAY);
    assertThat(timeType.getUnit()).isEqualTo(TimeUnit.MICROSECOND);
    assertThat(timeType.getBitWidth()).isEqualTo(64);
    assertThat(timestampType.getUnit()).isEqualTo(TimeUnit.MICROSECOND);
    assertThat(timestampType.getTimezone()).isNull();
    assertThat(timestamptzType.getUnit()).isEqualTo(TimeUnit.MICROSECOND);
    assertThat(timestamptzType.getTimezone()).isEqualTo("UTC");
  }

  @Test
  void mapsCanonicalDecimalAndBinaryFamilyToArrowSpecificTypes() {
    Schema schema =
        ArrowSchemaUtil.toArrowSchema(
            List.of(
                SchemaColumn.newBuilder()
                    .setName("dec")
                    .setType(LogicalTypeProtoAdapter.parseToProto("DECIMAL(12,3)"))
                    .build(),
                SchemaColumn.newBuilder()
                    .setName("dec256")
                    .setType(LogicalTypeProtoAdapter.parseToProto("DECIMAL(50,2)"))
                    .build(),
                SchemaColumn.newBuilder()
                    .setName("uuid")
                    .setType(LogicalTypeProtoAdapter.parseToProto("UUID"))
                    .build(),
                SchemaColumn.newBuilder()
                    .setName("bin")
                    .setType(LogicalTypeProtoAdapter.parseToProto("BINARY"))
                    .build(),
                SchemaColumn.newBuilder()
                    .setName("json")
                    .setType(LogicalTypeProtoAdapter.parseToProto("JSON"))
                    .build()));

    ArrowType.Decimal decimalType = (ArrowType.Decimal) schema.getFields().get(0).getType();
    ArrowType.Decimal decimal256Type = (ArrowType.Decimal) schema.getFields().get(1).getType();
    ArrowType.FixedSizeBinary uuidType =
        (ArrowType.FixedSizeBinary) schema.getFields().get(2).getType();
    ArrowType.Binary binaryType = (ArrowType.Binary) schema.getFields().get(3).getType();
    ArrowType.Utf8 jsonType = (ArrowType.Utf8) schema.getFields().get(4).getType();

    assertThat(decimalType.getPrecision()).isEqualTo(12);
    assertThat(decimalType.getScale()).isEqualTo(3);
    assertThat(decimalType.getBitWidth()).isEqualTo(128);
    assertThat(decimal256Type.getPrecision()).isEqualTo(50);
    assertThat(decimal256Type.getScale()).isEqualTo(2);
    assertThat(decimal256Type.getBitWidth()).isEqualTo(256);
    assertThat(uuidType.getByteWidth()).isEqualTo(16);
    assertThat(binaryType).isNotNull();
    assertThat(jsonType).isNotNull();
  }

  @Test
  void decimalPrecisionAboveArrowLimitThrows() {
    SchemaColumn column =
        SchemaColumn.newBuilder()
            .setName("dec")
            .setType(LogicalTypeProtoAdapter.parseToProto("DECIMAL(77,2)"))
            .build();
    assertThatThrownBy(() -> ArrowSchemaUtil.toArrowSchema(List.of(column)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("DECIMAL precision")
        .hasMessageContaining("max 76");
  }

  @Test
  void intervalIsRejectedForArrowSchema() {
    SchemaColumn column =
        SchemaColumn.newBuilder()
            .setName("iv")
            .setType(LogicalTypeProtoAdapter.parseToProto("INTERVAL"))
            .build();
    assertThatThrownBy(() -> ArrowSchemaUtil.toArrowSchema(List.of(column)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("INTERVAL has no stable Arrow representation");
  }

  @Test
  void complexLogicalTypesAreRejectedForArrowSchema() {
    for (String type : List.of("ARRAY", "MAP", "STRUCT", "VARIANT")) {
      SchemaColumn column =
          SchemaColumn.newBuilder()
              .setName("x")
              .setType(LogicalTypeProtoAdapter.parseToProto(type))
              .build();
      assertThatThrownBy(() -> ArrowSchemaUtil.toArrowSchema(List.of(column)))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Complex logical types");
    }
  }

  @Test
  void unknownLogicalTypeKindFailsFast() {
    SchemaColumn column =
        SchemaColumn.newBuilder()
            .setName("x")
            .setType(ai.floedb.floecat.types.rpc.LogicalType.newBuilder())
            .build();
    assertThatThrownBy(() -> ArrowSchemaUtil.toArrowSchema(List.of(column)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unrecognized logical type kind");
  }

  @Test
  void missingTypeFailsFast() {
    SchemaColumn column = SchemaColumn.newBuilder().setName("x").build();
    assertThatThrownBy(() -> ArrowSchemaUtil.toArrowSchema(List.of(column)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("has no type");
  }

  @Test
  void derivesLogicalTypeFromArrowUtf8Field() {
    Field field = Field.nullable("text", new ArrowType.Utf8());
    assertThat(ArrowSchemaUtil.logicalType(field)).isEqualTo("STRING");
  }

  @Test
  void derivesLogicalTypeFromArrowDecimalField() {
    Field field = Field.nullable("dec", new ArrowType.Decimal(12, 3, 128));
    assertThat(ArrowSchemaUtil.logicalType(field)).isEqualTo("DECIMAL(12,3)");
  }

  @Test
  void derivesLogicalTypeFromArrowTimestampWithTz() {
    Field field = Field.nullable("ts", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"));
    assertThat(ArrowSchemaUtil.logicalType(field)).isEqualTo("TIMESTAMPTZ(6)");
  }

  @Test
  void derivesLogicalTypeFromArrowTimeWithoutTz() {
    Field field = Field.nullable("t", new ArrowType.Time(TimeUnit.MICROSECOND, 64));
    assertThat(ArrowSchemaUtil.logicalType(field)).isEqualTo("TIME(6)");
  }
}
