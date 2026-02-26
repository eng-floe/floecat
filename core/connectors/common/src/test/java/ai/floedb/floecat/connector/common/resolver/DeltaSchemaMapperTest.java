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

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.query.rpc.SchemaDescriptor;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Unit tests for the package-private {@link DeltaSchemaMapper}.
 *
 * <p>Validates that Delta Lake JSON schema types are converted to the correct canonical
 * logical-type strings. Key focus areas:
 *
 * <ul>
 *   <li>Timestamp semantics: {@code "timestamp"} (UTC) → {@code "TIMESTAMPTZ"}; {@code
 *       "timestamp_ntz"} (naive) → {@code "TIMESTAMP"}
 *   <li>Integer collapsing: all Delta int flavours → {@code "INT"}
 *   <li>Complex object nodes (struct/array/map) → correct canonical name, {@code leaf=false}
 *   <li>DECIMAL passthrough: {@code "decimal(10,2)"} → {@code "DECIMAL(10,2)"}
 *   <li>Unknown scalars → {@code "BINARY"} (null-bug regression)
 * </ul>
 */
class DeltaSchemaMapperTest {

  private static final ColumnIdAlgorithm CID = ColumnIdAlgorithm.CID_PATH_ORDINAL;

  // ---------------------------------------------------------------------------
  // Helper: build a minimal Delta schema JSON with a single scalar field
  // ---------------------------------------------------------------------------

  private static String singleFieldSchema(String fieldName, String deltaType) {
    return """
        {"fields":[{"name":"%s","type":"%s","nullable":true}]}
        """
        .formatted(fieldName, deltaType);
  }

  private static SchemaColumn firstColumn(String schemaJson) {
    SchemaDescriptor desc = DeltaSchemaMapper.map(CID, schemaJson, Set.of());
    assertThat(desc.getColumnsCount()).isGreaterThanOrEqualTo(1);
    return desc.getColumns(0);
  }

  // ---------------------------------------------------------------------------
  // Timestamp semantics (critical correctness tests)
  // ---------------------------------------------------------------------------

  @Test
  void deltaTimestampMapsToTimestamptz() {
    // Delta "timestamp" is UTC-adjusted → canonical TIMESTAMPTZ
    SchemaColumn col = firstColumn(singleFieldSchema("ts", "timestamp"));
    assertThat(col.getLogicalType()).isEqualTo("TIMESTAMPTZ");
    assertThat(col.getLeaf()).isTrue();
  }

  @Test
  void deltaTimestampNtzMapsToTimestamp() {
    // Delta "timestamp_ntz" is timezone-naive → canonical TIMESTAMP
    SchemaColumn col = firstColumn(singleFieldSchema("ts", "timestamp_ntz"));
    assertThat(col.getLogicalType()).isEqualTo("TIMESTAMP");
    assertThat(col.getLeaf()).isTrue();
  }

  // ---------------------------------------------------------------------------
  // Integer aliases (all collapse to INT)
  // ---------------------------------------------------------------------------

  @ParameterizedTest(name = "''{0}'' -> INT")
  @ValueSource(
      strings = {"byte", "tinyint", "short", "smallint", "integer", "int", "long", "bigint"})
  void integerAliasesMapsToInt(String deltaType) {
    SchemaColumn col = firstColumn(singleFieldSchema("n", deltaType));
    assertThat(col.getLogicalType()).isEqualTo("INT");
  }

  // ---------------------------------------------------------------------------
  // Scalar type mapping
  // ---------------------------------------------------------------------------

  @ParameterizedTest(name = "''{0}'' -> ''{1}''")
  @CsvSource({
    "boolean, BOOLEAN",
    "float,   FLOAT",
    "double,  DOUBLE",
    "string,  STRING",
    "binary,  BINARY",
    "date,    DATE",
    "interval,INTERVAL",
  })
  void scalarTypesMappedCorrectly(String deltaType, String expected) {
    SchemaColumn col = firstColumn(singleFieldSchema("col", deltaType));
    assertThat(col.getLogicalType()).isEqualTo(expected);
  }

  // ---------------------------------------------------------------------------
  // DECIMAL passthrough
  // ---------------------------------------------------------------------------

  @Test
  void decimalTypeIsUppercasedAndPassedThrough() {
    SchemaColumn col = firstColumn(singleFieldSchema("amt", "decimal(10,2)"));
    assertThat(col.getLogicalType()).isEqualTo("DECIMAL(10,2)");
  }

  @Test
  void decimalWithVariousPrecisions() {
    assertThat(firstColumn(singleFieldSchema("x", "decimal(38,0)")).getLogicalType())
        .isEqualTo("DECIMAL(38,0)");
    assertThat(firstColumn(singleFieldSchema("x", "decimal(1,0)")).getLogicalType())
        .isEqualTo("DECIMAL(1,0)");
  }

  // ---------------------------------------------------------------------------
  // Unknown scalar type → BINARY (null-bug regression guard)
  // ---------------------------------------------------------------------------

  @Test
  void unknownScalarMappsToBinaryNotNull() {
    SchemaColumn col = firstColumn(singleFieldSchema("x", "someunknowntype"));
    assertThat(col.getLogicalType()).isNotNull();
    assertThat(col.getLogicalType()).isEqualTo("BINARY");
  }

  // ---------------------------------------------------------------------------
  // Complex / container types — canonical name + leaf=false
  // ---------------------------------------------------------------------------

  @Test
  void structObjectNodeMapsToStructAndIsNotLeaf() {
    String json =
        """
        {"fields":[
          {"name":"addr","type":{"type":"struct","fields":[
            {"name":"city","type":"string","nullable":true}
          ]},"nullable":true}
        ]}
        """;
    SchemaColumn col = firstColumn(json);
    assertThat(col.getLogicalType()).isEqualTo("STRUCT");
    assertThat(col.getLeaf()).isFalse();
  }

  @Test
  void arrayObjectNodeMapsToArrayAndIsNotLeaf() {
    String json =
        """
        {"fields":[
          {"name":"items","type":{"type":"array","elementType":"string","containsNull":true},
           "nullable":true}
        ]}
        """;
    SchemaColumn col = firstColumn(json);
    assertThat(col.getLogicalType()).isEqualTo("ARRAY");
    assertThat(col.getLeaf()).isFalse();
  }

  @Test
  void mapObjectNodeMapsToMapAndIsNotLeaf() {
    String json =
        """
        {"fields":[
          {"name":"props","type":{"type":"map","keyType":"string","valueType":"string",
           "valueContainsNull":true},"nullable":true}
        ]}
        """;
    SchemaColumn col = firstColumn(json);
    assertThat(col.getLogicalType()).isEqualTo("MAP");
    assertThat(col.getLeaf()).isFalse();
  }

  // ---------------------------------------------------------------------------
  // Nested struct expands child columns
  // ---------------------------------------------------------------------------

  @Test
  void nestedStructExpandsChildColumns() {
    String json =
        """
        {"fields":[
          {"name":"id","type":"long","nullable":false},
          {"name":"location","type":{
            "type":"struct",
            "fields":[
              {"name":"lat","type":"double","nullable":true},
              {"name":"lon","type":"double","nullable":true}
            ]
          },"nullable":true}
        ]}
        """;
    SchemaDescriptor desc = DeltaSchemaMapper.map(CID, json, Set.of());

    // id, location (container), location.lat, location.lon
    assertThat(desc.getColumnsCount()).isEqualTo(4);

    assertThat(desc.getColumns(0).getName()).isEqualTo("id");
    assertThat(desc.getColumns(0).getLogicalType()).isEqualTo("INT");
    assertThat(desc.getColumns(0).getLeaf()).isTrue();

    assertThat(desc.getColumns(1).getName()).isEqualTo("location");
    assertThat(desc.getColumns(1).getLogicalType()).isEqualTo("STRUCT");
    assertThat(desc.getColumns(1).getLeaf()).isFalse();

    assertThat(desc.getColumns(2).getPhysicalPath()).isEqualTo("location.lat");
    assertThat(desc.getColumns(2).getLogicalType()).isEqualTo("DOUBLE");
    assertThat(desc.getColumns(2).getLeaf()).isTrue();

    assertThat(desc.getColumns(3).getPhysicalPath()).isEqualTo("location.lon");
    assertThat(desc.getColumns(3).getLogicalType()).isEqualTo("DOUBLE");
    assertThat(desc.getColumns(3).getLeaf()).isTrue();
  }

  // ---------------------------------------------------------------------------
  // Partition key propagation
  // ---------------------------------------------------------------------------

  @Test
  void partitionKeyIsMarkedOnMatchingColumn() {
    String json =
        """
        {"fields":[
          {"name":"id","type":"long","nullable":false},
          {"name":"dt","type":"date","nullable":true}
        ]}
        """;
    SchemaDescriptor desc = DeltaSchemaMapper.map(CID, json, Set.of("dt"));

    assertThat(desc.getColumns(0).getPartitionKey()).isFalse();
    assertThat(desc.getColumns(1).getPartitionKey()).isTrue();
  }

  // ---------------------------------------------------------------------------
  // Ordinals are 1-based within each struct
  // ---------------------------------------------------------------------------

  @Test
  void topLevelOrdinalsAreOneBased() {
    String json =
        """
        {"fields":[
          {"name":"a","type":"long","nullable":false},
          {"name":"b","type":"string","nullable":true},
          {"name":"c","type":"boolean","nullable":true}
        ]}
        """;
    SchemaDescriptor desc = DeltaSchemaMapper.map(CID, json, Set.of());

    assertThat(desc.getColumns(0).getOrdinal()).isEqualTo(1);
    assertThat(desc.getColumns(1).getOrdinal()).isEqualTo(2);
    assertThat(desc.getColumns(2).getOrdinal()).isEqualTo(3);
  }

  // ---------------------------------------------------------------------------
  // Null-safe: empty / malformed JSON produces an empty descriptor, no exception
  // ---------------------------------------------------------------------------

  @Test
  void emptySchemaProducesNoColumns() {
    SchemaDescriptor desc = DeltaSchemaMapper.map(CID, "{}", Set.of());
    assertThat(desc.getColumnsCount()).isEqualTo(0);
  }
}
