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

package ai.floedb.floecat.connector.common.resolver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.query.rpc.SchemaDescriptor;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link IcebergSchemaMapper}.
 *
 * <p>Validates that {@link IcebergSchemaMapper#map} emits correct canonical {@code logical_type}
 * strings for all Iceberg primitive and complex types, with particular focus on:
 *
 * <ul>
 *   <li>Timestamp UTC semantics: {@code withZone()} → {@code "TIMESTAMPTZ"}, {@code withoutZone()}
 *       → {@code "TIMESTAMP"}
 *   <li>Integer collapsing: INTEGER and LONG both → {@code "INT"}
 *   <li>Complex container nodes (LIST, MAP, STRUCT) → leaf={@code false}
 *   <li>DECIMAL → {@code "DECIMAL(p,s)"}
 * </ul>
 */
class IcebergSchemaMapperTest {

  /** Maps the given schema fields through {@link IcebergSchemaMapper} using FIELD_ID column ids. */
  private static SchemaDescriptor map(Types.NestedField... fields) {
    Schema schema = new Schema(fields);
    String json = SchemaParser.toJson(schema);
    return IcebergSchemaMapper.map(ColumnIdAlgorithm.CID_FIELD_ID, json, Set.of());
  }

  /** Returns the single column from a single-field schema. */
  private static SchemaColumn singleColumn(Types.NestedField field) {
    SchemaDescriptor desc = map(field);
    assertThat(desc.getColumnsCount()).isGreaterThanOrEqualTo(1);
    return desc.getColumns(0);
  }

  // ---------------------------------------------------------------------------
  // Timestamp semantics (critical correctness test)
  // ---------------------------------------------------------------------------

  @Test
  void utcTimestampMapsToTimestamptz() {
    Types.TimestampType sourceType = Types.TimestampType.withZone();
    SchemaColumn col = singleColumn(Types.NestedField.optional(1, "ts_utc", sourceType));
    assertThat(col.getLogicalType()).isEqualTo("TIMESTAMPTZ");
    assertThat(col.getSourceType().getEngineKind()).isEqualTo("iceberg");
    assertThat(col.getSourceType().getDeclaredType()).isEqualTo(sourceType.toString());
    assertThat(col.getLeaf()).isTrue();
  }

  @Test
  void nonUtcTimestampMapsToTimestamp() {
    SchemaColumn col =
        singleColumn(Types.NestedField.optional(1, "ts_ntz", Types.TimestampType.withoutZone()));
    assertThat(col.getLogicalType()).isEqualTo("TIMESTAMP");
    assertThat(col.getLeaf()).isTrue();
  }

  // ---------------------------------------------------------------------------
  // Integer collapsing
  // ---------------------------------------------------------------------------

  @Test
  void integerMapsToInt() {
    SchemaColumn col = singleColumn(Types.NestedField.required(1, "n", Types.IntegerType.get()));
    assertThat(col.getLogicalType()).isEqualTo("INT");
  }

  @Test
  void longMapsToInt() {
    SchemaColumn col = singleColumn(Types.NestedField.required(1, "n", Types.LongType.get()));
    assertThat(col.getLogicalType()).isEqualTo("INT");
  }

  // ---------------------------------------------------------------------------
  // Other scalar types
  // ---------------------------------------------------------------------------

  @Test
  void scalarTypesMappedCorrectly() {
    SchemaDescriptor desc =
        map(
            Types.NestedField.required(1, "b", Types.BooleanType.get()),
            Types.NestedField.optional(2, "f", Types.FloatType.get()),
            Types.NestedField.optional(3, "d", Types.DoubleType.get()),
            Types.NestedField.optional(4, "s", Types.StringType.get()),
            Types.NestedField.optional(5, "bin", Types.BinaryType.get()),
            Types.NestedField.optional(6, "u", Types.UUIDType.get()),
            Types.NestedField.optional(7, "dt", Types.DateType.get()),
            Types.NestedField.optional(8, "t", Types.TimeType.get()));

    assertThat(desc.getColumns(0).getLogicalType()).isEqualTo("BOOLEAN");
    assertThat(desc.getColumns(1).getLogicalType()).isEqualTo("FLOAT");
    assertThat(desc.getColumns(2).getLogicalType()).isEqualTo("DOUBLE");
    assertThat(desc.getColumns(3).getLogicalType()).isEqualTo("STRING");
    assertThat(desc.getColumns(4).getLogicalType()).isEqualTo("BINARY");
    assertThat(desc.getColumns(5).getLogicalType()).isEqualTo("UUID");
    assertThat(desc.getColumns(6).getLogicalType()).isEqualTo("DATE");
    assertThat(desc.getColumns(7).getLogicalType()).isEqualTo("TIME");
  }

  // ---------------------------------------------------------------------------
  // DECIMAL
  // ---------------------------------------------------------------------------

  @Test
  void decimalMapsWithPrecisionAndScale() {
    SchemaColumn col =
        singleColumn(Types.NestedField.optional(1, "amt", Types.DecimalType.of(10, 2)));
    assertThat(col.getLogicalType()).isEqualTo("DECIMAL(10,2)");
    assertThat(col.getLeaf()).isTrue();
  }

  // ---------------------------------------------------------------------------
  // Complex / container types — canonical name + leaf=false
  // ---------------------------------------------------------------------------

  @Test
  void listFieldMapsToArrayAndIsNotLeaf() {
    SchemaColumn col =
        singleColumn(
            Types.NestedField.optional(
                1, "items", Types.ListType.ofOptional(2, Types.StringType.get())));
    assertThat(col.getLogicalType()).isEqualTo("ARRAY");
    assertThat(col.getLeaf()).isFalse();
  }

  @Test
  void mapFieldMapsToMapAndIsNotLeaf() {
    SchemaColumn col =
        singleColumn(
            Types.NestedField.optional(
                1,
                "attrs",
                Types.MapType.ofOptional(2, 3, Types.StringType.get(), Types.StringType.get())));
    assertThat(col.getLogicalType()).isEqualTo("MAP");
    assertThat(col.getLeaf()).isFalse();
  }

  @Test
  void structFieldMapsToStructAndIsNotLeaf() {
    SchemaColumn col =
        singleColumn(
            Types.NestedField.optional(
                1,
                "info",
                Types.StructType.of(
                    Types.NestedField.optional(2, "city", Types.StringType.get()))));
    assertThat(col.getLogicalType()).isEqualTo("STRUCT");
    assertThat(col.getLeaf()).isFalse();
  }

  // ---------------------------------------------------------------------------
  // Nested struct expands children
  // ---------------------------------------------------------------------------

  @Test
  void nestedStructExpandsChildColumns() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(
                2,
                "address",
                Types.StructType.of(
                    Types.NestedField.optional(3, "city", Types.StringType.get()),
                    Types.NestedField.optional(4, "zip", Types.StringType.get()))));
    String json = SchemaParser.toJson(schema);
    SchemaDescriptor desc = IcebergSchemaMapper.map(ColumnIdAlgorithm.CID_FIELD_ID, json, Set.of());

    // Expected: id, address (container), address.city, address.zip
    assertThat(desc.getColumnsCount()).isEqualTo(4);

    SchemaColumn id = desc.getColumns(0);
    assertThat(id.getName()).isEqualTo("id");
    assertThat(id.getLogicalType()).isEqualTo("INT");
    assertThat(id.getLeaf()).isTrue();

    SchemaColumn address = desc.getColumns(1);
    assertThat(address.getName()).isEqualTo("address");
    assertThat(address.getLogicalType()).isEqualTo("STRUCT");
    assertThat(address.getLeaf()).isFalse();
    assertThat(address.getPhysicalPath()).isEqualTo("address");

    SchemaColumn city = desc.getColumns(2);
    assertThat(city.getName()).isEqualTo("city");
    assertThat(city.getPhysicalPath()).isEqualTo("address.city");
    assertThat(city.getLeaf()).isTrue();
    assertThat(city.getLogicalType()).isEqualTo("STRING");

    SchemaColumn zip = desc.getColumns(3);
    assertThat(zip.getName()).isEqualTo("zip");
    assertThat(zip.getPhysicalPath()).isEqualTo("address.zip");
    assertThat(zip.getLeaf()).isTrue();
  }

  // ---------------------------------------------------------------------------
  // Partition key propagation
  // ---------------------------------------------------------------------------

  @Test
  void partitionKeyIsMarkedOnMatchingColumn() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "dt", Types.DateType.get()));
    String json = SchemaParser.toJson(schema);
    SchemaDescriptor desc =
        IcebergSchemaMapper.map(ColumnIdAlgorithm.CID_FIELD_ID, json, Set.of("dt"));

    SchemaColumn id = desc.getColumns(0);
    assertThat(id.getPartitionKey()).isFalse();

    SchemaColumn dt = desc.getColumns(1);
    assertThat(dt.getPartitionKey()).isTrue();
  }

  // ---------------------------------------------------------------------------
  // Ordinals
  // ---------------------------------------------------------------------------

  @Test
  void topLevelOrdinalsAreOneBasedAndStable() {
    SchemaDescriptor desc =
        map(
            Types.NestedField.required(1, "a", Types.LongType.get()),
            Types.NestedField.optional(2, "b", Types.StringType.get()),
            Types.NestedField.optional(3, "c", Types.BooleanType.get()));

    assertThat(desc.getColumns(0).getOrdinal()).isEqualTo(1);
    assertThat(desc.getColumns(1).getOrdinal()).isEqualTo(2);
    assertThat(desc.getColumns(2).getOrdinal()).isEqualTo(3);
  }

  // ---------------------------------------------------------------------------
  // CID_FIELD_ID: column id == field id
  // ---------------------------------------------------------------------------

  @Test
  void fieldIdPolicyAssignsIcebergFieldIdAsColumnId() {
    Schema schema =
        new Schema(
            List.of(
                Types.NestedField.required(42, "x", Types.LongType.get()),
                Types.NestedField.optional(99, "y", Types.StringType.get())));
    String json = SchemaParser.toJson(schema);
    SchemaDescriptor desc = IcebergSchemaMapper.map(ColumnIdAlgorithm.CID_FIELD_ID, json, Set.of());

    assertThat(desc.getColumns(0).getId()).isEqualTo(42L);
    assertThat(desc.getColumns(1).getId()).isEqualTo(99L);
  }

  @Test
  void invalidIcebergSchemaFailsFast() {
    assertThatThrownBy(
            () -> IcebergSchemaMapper.map(ColumnIdAlgorithm.CID_FIELD_ID, "{not-json", Set.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Failed to parse Iceberg schema JSON");
  }
}
