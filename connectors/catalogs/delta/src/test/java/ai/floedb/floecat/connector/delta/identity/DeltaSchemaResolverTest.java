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

package ai.floedb.floecat.connector.delta.identity;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.schema.identity.ColumnPath;
import ai.floedb.floecat.schema.identity.ResolvedSchema;
import ai.floedb.floecat.schema.identity.SchemaNode;
import io.delta.kernel.types.FieldMetadata;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import java.util.OptionalInt;
import org.junit.jupiter.api.Test;

class DeltaSchemaResolverTest {

  @Test
  void readsEffectiveMappingModeFromKernelValidatedProperties() {
    assertThat(ColumnMappingMode.fromTableProperties(java.util.Map.of()))
        .isEqualTo(ColumnMappingMode.NONE);
    assertThat(
            ColumnMappingMode.fromTableProperties(
                java.util.Map.of(ColumnMappingMode.PROPERTY, "name")))
        .isEqualTo(ColumnMappingMode.NAME);
  }

  @Test
  void resolvesEveryUnmappedNestedNodeWithoutInventingSourceIdentity() {
    ResolvedSchema schema =
        DeltaSchemaResolver.resolve(
                """
            {
              "type": "struct",
              "fields": [
                {"name": "a.b", "type": "integer", "nullable": true, "metadata": {}},
                {
                  "name": "a",
                  "type": {
                    "type": "struct",
                    "fields": [{
                      "name": "b",
                      "type": {
                        "type": "array",
                        "elementType": {
                          "type": "map",
                          "keyType": "string",
                          "valueType": "long",
                          "valueContainsNull": true
                        },
                        "containsNull": true
                      },
                      "nullable": true,
                      "metadata": {}
                    }]
                  },
                  "nullable": true,
                  "metadata": {}
                }
              ]
            }
            """,
                ColumnMappingMode.NONE)
            .schema();

    assertThat(schema.nodes())
        .extracting(node -> node.path().display())
        .containsExactly("a.b", "a", "a.b", "a.b[]", "a.b[].key", "a.b[]{}");
    assertThat(schema.nodes()).allMatch(node -> node.nativeFieldId().isEmpty());
    assertThat(schema.nodes()).allMatch(node -> node.sourcePhysicalPath().isEmpty());
    assertThat(schema.byPath(ColumnPath.ROOT.field("a.b"))).isPresent();
    assertThat(schema.byPath(ColumnPath.ROOT.field("a").field("b"))).isPresent();
  }

  @Test
  void resolvesMappedFieldsAndCollectionNestedIds() {
    DeltaResolvedSchema resolved =
        DeltaSchemaResolver.resolve(
            """
            {
              "type": "struct",
              "fields": [
                {
                  "name": "items",
                  "type": {
                    "type": "array",
                    "elementType": {
                      "type": "struct",
                      "fields": [{
                        "name": "sku",
                        "type": "string",
                        "nullable": false,
                        "metadata": {
                          "delta.columnMapping.id": 5,
                          "delta.columnMapping.physicalName": "col-sku"
                        }
                      }]
                    },
                    "containsNull": true
                  },
                  "nullable": true,
                  "metadata": {
                    "delta.columnMapping.id": 4,
                    "delta.columnMapping.physicalName": "col-items",
                    "delta.columnMapping.nested.ids": {"col-items.element": 100}
                  }
                },
                {
                  "name": "attributes",
                  "type": {
                    "type": "map",
                    "keyType": "string",
                    "valueType": {"type": "array", "elementType": "long", "containsNull": false},
                    "valueContainsNull": true
                  },
                  "nullable": true,
                  "metadata": {
                    "delta.columnMapping.id": 6,
                    "delta.columnMapping.physicalName": "col-attributes",
                    "delta.columnMapping.nested.ids": {
                      "col-attributes.key": 101,
                      "col-attributes.value": 102,
                      "col-attributes.value.element": 103
                    }
                  }
                },
                {
                  "name": "profile",
                  "type": {
                    "type": "struct",
                    "fields": [{
                      "name": "age",
                      "type": "integer",
                      "nullable": true,
                      "metadata": {
                        "delta.columnMapping.id": 8,
                        "delta.columnMapping.physicalName": "col-age"
                      }
                    }]
                  },
                  "nullable": true,
                  "metadata": {
                    "delta.columnMapping.id": 7,
                    "delta.columnMapping.physicalName": "col-profile"
                  }
                }
              ]
            }
            """,
            ColumnMappingMode.NAME);
    ResolvedSchema schema = resolved.schema();

    assertNode(schema, ColumnPath.ROOT.field("items"), 4, "col-items");
    assertNode(schema, ColumnPath.ROOT.field("items").arrayElement(), 100, "col-items[]");
    assertNode(
        schema,
        ColumnPath.ROOT.field("items").arrayElement().field("sku"),
        5,
        "col-items[].col-sku");
    assertNode(schema, ColumnPath.ROOT.field("attributes").mapKey(), 101, "col-attributes.key");
    assertNode(schema, ColumnPath.ROOT.field("attributes").mapValue(), 102, "col-attributes{}");
    assertNode(
        schema,
        ColumnPath.ROOT.field("attributes").mapValue().arrayElement(),
        103,
        "col-attributes{}[]");
    assertThat(resolved.nodeForStatsNames(java.util.List.of("col-profile", "col-age")))
        .map(node -> node.path().display())
        .contains("profile.age");
  }

  @Test
  void ignoresResidualMappingMetadataWhenMappingIsNotEffective() {
    ResolvedSchema schema =
        DeltaSchemaResolver.resolve(
                """
            {"type":"struct","fields":[{
              "name":"id",
              "type":"long",
              "nullable":false,
              "metadata":{
                "delta.columnMapping.id":-1,
                "delta.columnMapping.physicalName":"old-physical-name",
                "delta.columnMapping.nested.ids":"not-an-object"
              }
            }]}
            """,
                ColumnMappingMode.NONE)
            .schema();

    SchemaNode node = schema.byPath(ColumnPath.ROOT.field("id")).orElseThrow();
    assertThat(node.nativeFieldId()).isEmpty();
    assertThat(node.sourcePhysicalPath()).isEmpty();
  }

  @Test
  void rejectsMappedFieldsWithoutRequiredMetadata() {
    assertThatThrownBy(
            () ->
                DeltaSchemaResolver.resolve(
                    """
                    {"type":"struct","fields":[{
                      "name":"id","type":"long","nullable":false,"metadata":{}
                    }]}
                    """,
                    ColumnMappingMode.NAME))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(DeltaSchemaResolver.COLUMN_ID)
        .hasMessageContaining(DeltaSchemaResolver.PHYSICAL_NAME);
  }

  @Test
  void rejectsUnusedNestedIds() {
    assertThatThrownBy(
            () ->
                DeltaSchemaResolver.resolve(
                    """
                    {"type":"struct","fields":[{
                      "name":"items",
                      "type":{"type":"array","elementType":"long","containsNull":false},
                      "nullable":true,
                      "metadata":{
                        "delta.columnMapping.id":1,
                        "delta.columnMapping.physicalName":"col-items",
                        "delta.columnMapping.nested.ids":{
                          "col-items.element":2,
                          "col-items.value":3
                        }
                      }
                    }]}
                    """,
                    ColumnMappingMode.ID))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("unused nested ID")
        .hasMessageContaining("col-items.value");
  }

  /**
   * Delta Kernel owns the parse, so a structurally malformed type is rejected there — before any
   * mapping metadata is examined. The resolver deliberately does not re-validate schema shape.
   */
  @Test
  void leavesMalformedCollectionTypesToTheKernelParse() {
    assertThatThrownBy(
            () ->
                DeltaSchemaResolver.resolve(
                    """
                    {"type":"struct","fields":[{
                      "name":"items",
                      "type":{"type":"array"},
                      "nullable":true,
                      "metadata":{
                        "delta.columnMapping.id":1,
                        "delta.columnMapping.physicalName":"col-items",
                        "delta.columnMapping.nested.ids":{"col-items.element":2}
                      }
                    }]}
                    """,
                    ColumnMappingMode.ID))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("array data type");
  }

  /** The kernel accepts an empty field name; no path can address one, so the resolver rejects it. */
  @Test
  void rejectsFieldsTheKernelAcceptsButNoPathCanAddress() {
    assertThatThrownBy(
            () ->
                DeltaSchemaResolver.resolve(
                    """
                    {"type":"struct","fields":[{
                      "name":"","type":"long","nullable":true,"metadata":{}
                    }]}
                    """,
                    ColumnMappingMode.NONE))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("field 1 under <root> has no name");
  }

  /** Callers holding a kernel schema resolve it directly, without a round trip through JSON. */
  @Test
  void resolvesTheKernelSchemaWithoutReparsingJson() {
    StructType schema =
        new StructType()
            .add("id", LongType.LONG, false)
            .add(
                "nested",
                new StructType().add("leaf", StringType.STRING, true),
                true,
                FieldMetadata.builder().putLong(DeltaSchemaResolver.COLUMN_ID, 3L).build());

    ResolvedSchema resolved = DeltaSchemaResolver.resolve(schema, ColumnMappingMode.NONE).schema();

    assertThat(resolved.nodes())
        .extracting(node -> node.path().display())
        .containsExactly("id", "nested", "nested.leaf");
    assertThat(resolved.byPath(ColumnPath.ROOT.field("nested")).orElseThrow().leaf()).isFalse();
    assertThat(resolved.nodes()).allMatch(node -> node.nativeFieldId().isEmpty());
  }

  @Test
  void rejectsDuplicateNativeIdsAcrossFieldsAndCollectionInteriors() {
    assertThatThrownBy(
            () ->
                DeltaSchemaResolver.resolve(
                    """
                    {"type":"struct","fields":[{
                      "name":"items",
                      "type":{"type":"array","elementType":"long","containsNull":false},
                      "nullable":true,
                      "metadata":{
                        "delta.columnMapping.id":1,
                        "delta.columnMapping.physicalName":"col-items",
                        "delta.columnMapping.nested.ids":{"col-items.element":1}
                      }
                    }]}
                    """,
                    ColumnMappingMode.NAME))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Duplicate native field ID 1");
  }

  private static void assertNode(
      ResolvedSchema schema, ColumnPath path, int nativeId, String physicalPath) {
    SchemaNode node = schema.byPath(path).orElseThrow();
    assertThat(node.nativeFieldId()).isEqualTo(OptionalInt.of(nativeId));
    assertThat(node.sourcePhysicalPath().orElseThrow().display()).isEqualTo(physicalPath);
  }
}
