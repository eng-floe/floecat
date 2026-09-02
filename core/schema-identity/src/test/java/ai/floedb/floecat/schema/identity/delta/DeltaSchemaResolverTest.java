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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.schema.identity.ColumnPath;
import ai.floedb.floecat.schema.identity.NativeIdentityAssigner;
import ai.floedb.floecat.schema.identity.NodeKind;
import ai.floedb.floecat.schema.identity.SchemaNode;
import java.util.List;
import java.util.OptionalInt;
import org.junit.jupiter.api.Test;

class DeltaSchemaResolverTest {

  /**
   * A mapped schema exercising every node kind. Nested ids are rooted at the enclosing field's
   * physical name and extended once per collection level, as Delta's writer produces them.
   */
  private static final String MAPPED_SCHEMA =
      """
      {"type":"struct","fields":[
        {"name":"id","type":"long","nullable":false,
         "metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"col-aaa"}},
        {"name":"s","nullable":true,
         "metadata":{"delta.columnMapping.id":2,"delta.columnMapping.physicalName":"col-bbb"},
         "type":{"type":"struct","fields":[
           {"name":"a","type":"integer","nullable":true,
            "metadata":{"delta.columnMapping.id":3,"delta.columnMapping.physicalName":"col-ccc"}},
           {"name":"b","type":"string","nullable":true,
            "metadata":{"delta.columnMapping.id":4,"delta.columnMapping.physicalName":"col-ddd"}}
         ]}},
        {"name":"items","nullable":true,
         "metadata":{"delta.columnMapping.id":5,"delta.columnMapping.physicalName":"col-eee",
                     "delta.columnMapping.nested.ids":{"col-eee.element":50}},
         "type":{"type":"array","containsNull":true,
           "elementType":{"type":"struct","fields":[
             {"name":"x","type":"double","nullable":true,
              "metadata":{"delta.columnMapping.id":6,"delta.columnMapping.physicalName":"col-fff"}}
           ]}}},
        {"name":"attrs","nullable":true,
         "metadata":{"delta.columnMapping.id":7,"delta.columnMapping.physicalName":"col-ggg",
                     "delta.columnMapping.nested.ids":{
                       "col-ggg.key":70,"col-ggg.value":71,"col-ggg.value.element":72}},
         "type":{"type":"map","valueContainsNull":true,
           "keyType":"string",
           "valueType":{"type":"array","containsNull":true,"elementType":"long"}}}
      ]}
      """;

  private static final String UNMAPPED_SCHEMA =
      """
      {"type":"struct","fields":[
        {"name":"id","type":"long","nullable":false,"metadata":{}},
        {"name":"s","nullable":true,"metadata":{},
         "type":{"type":"struct","fields":[
           {"name":"a","type":"integer","nullable":true,"metadata":{}},
           {"name":"b","type":"string","nullable":true,"metadata":{}}
         ]}},
        {"name":"items","nullable":true,"metadata":{},
         "type":{"type":"array","containsNull":true,"elementType":"long"}}
      ]}
      """;

  private static ColumnPath path(String first, String... rest) {
    ColumnPath p = ColumnPath.ROOT.field(first);
    for (String segment : rest) {
      p = p.field(segment);
    }
    return p;
  }

  // ---------------------------------------------------------------- structured paths

  @Test
  void distinguishesALiteralDottedFieldFromNesting() {
    String schema =
        """
        {"type":"struct","fields":[
          {"name":"a.b","type":"long","nullable":true,"metadata":{}},
          {"name":"a","nullable":true,"metadata":{},
           "type":{"type":"struct","fields":[
             {"name":"b","type":"string","nullable":true,"metadata":{}}
           ]}}
        ]}
        """;
    var resolver = DeltaSchemaResolver.resolve(schema, ColumnMappingMode.NONE);

    var literal = resolver.schema().byPath(path("a.b"));
    var nested = resolver.schema().byPath(path("a", "b"));
    assertThat(literal).isPresent();
    assertThat(nested).isPresent();
    assertThat(literal.get()).isNotEqualTo(nested.get());
    assertThat(literal.get().leaf()).isTrue();
    // Both render the same; only the structured path tells them apart.
    assertThat(literal.get().path().display()).isEqualTo(nested.get().path().display());
  }

  @Test
  void fieldNamesContainingDisplaySyntaxDoNotCollideWithCollectionNodes() {
    String schema =
        """
        {"type":"struct","fields":[
          {"name":"arr[]","type":"long","nullable":true,"metadata":{}},
          {"name":"arr","nullable":true,"metadata":{},
           "type":{"type":"array","containsNull":true,"elementType":"long"}}
        ]}
        """;
    var resolver = DeltaSchemaResolver.resolve(schema, ColumnMappingMode.NONE);

    assertThat(resolver.schema().byPath(path("arr[]"))).isPresent();
    assertThat(resolver.schema().byPath(ColumnPath.ROOT.field("arr").arrayElement())).isPresent();
    assertThat(resolver.schema().byPath(path("arr[]")).get())
        .isNotEqualTo(resolver.schema().byPath(ColumnPath.ROOT.field("arr").arrayElement()).get());
  }

  // ---------------------------------------------------------------- format ids

  @Test
  void ordinaryMappedFieldsCarryTheirColumnMappingId() {
    var resolver = DeltaSchemaResolver.resolve(MAPPED_SCHEMA, ColumnMappingMode.NAME);

    assertThat(resolver.schema().byPath(path("id")).orElseThrow().nativeFieldId())
        .isEqualTo(OptionalInt.of(1));
    assertThat(resolver.schema().byPath(path("s", "b")).orElseThrow().nativeFieldId())
        .isEqualTo(OptionalInt.of(4));
    // A struct field beneath a collection keeps its own ordinary id.
    ColumnPath itemsX = ColumnPath.ROOT.field("items").arrayElement().field("x");
    assertThat(resolver.schema().byPath(itemsX).orElseThrow().nativeFieldId())
        .isEqualTo(OptionalInt.of(6));
  }

  @Test
  void arrayElementTakesItsNestedId() {
    var resolver = DeltaSchemaResolver.resolve(MAPPED_SCHEMA, ColumnMappingMode.NAME);
    var element =
        resolver.schema().byPath(ColumnPath.ROOT.field("items").arrayElement()).orElseThrow();

    assertThat(element.kind()).isEqualTo(NodeKind.ARRAY_ELEMENT);
    assertThat(element.nativeFieldId()).isEqualTo(OptionalInt.of(50));
    assertThat(resolver.schema().byNativeFieldId(50)).contains(element);
  }

  @Test
  void mapKeyAndValueTakeTheirNestedIds() {
    var resolver = DeltaSchemaResolver.resolve(MAPPED_SCHEMA, ColumnMappingMode.NAME);

    var key = resolver.schema().byPath(ColumnPath.ROOT.field("attrs").mapKey()).orElseThrow();
    var value = resolver.schema().byPath(ColumnPath.ROOT.field("attrs").mapValue()).orElseThrow();

    assertThat(key.kind()).isEqualTo(NodeKind.MAP_KEY);
    assertThat(key.nativeFieldId()).isEqualTo(OptionalInt.of(70));
    assertThat(key.ordinal()).isEqualTo(1);
    assertThat(value.kind()).isEqualTo(NodeKind.MAP_VALUE);
    assertThat(value.nativeFieldId()).isEqualTo(OptionalInt.of(71));
    assertThat(value.ordinal()).isEqualTo(2);
  }

  @Test
  void recursivelyNestedCollectionTakesTheAccumulatedNestedKey() {
    var resolver = DeltaSchemaResolver.resolve(MAPPED_SCHEMA, ColumnMappingMode.NAME);
    // attrs MAP<STRING, ARRAY<LONG>> -> the array element under the map value.
    ColumnPath deep = ColumnPath.ROOT.field("attrs").mapValue().arrayElement();

    assertThat(resolver.schema().byPath(deep).orElseThrow().nativeFieldId())
        .isEqualTo(OptionalInt.of(72));
  }

  @Test
  void nestedIdChainRestartsAtAStructFieldBeneathACollection() {
    // items[] has a nested id on `items`; x beneath it uses its own ordinary id, and x's own
    // array element would use a nested id keyed from x's physical name.
    String schema =
        """
        {"type":"struct","fields":[
          {"name":"items","nullable":true,
           "metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"col-a",
                       "delta.columnMapping.nested.ids":{"col-a.element":10}},
           "type":{"type":"array","containsNull":true,
             "elementType":{"type":"struct","fields":[
               {"name":"x","nullable":true,
                "metadata":{"delta.columnMapping.id":2,"delta.columnMapping.physicalName":"col-b",
                            "delta.columnMapping.nested.ids":{"col-b.element":20}},
                "type":{"type":"array","containsNull":true,"elementType":"int"}}
             ]}}}
        ]}
        """;
    var resolver = DeltaSchemaResolver.resolve(schema, ColumnMappingMode.ID);

    ColumnPath outerElement = ColumnPath.ROOT.field("items").arrayElement();
    ColumnPath innerElement = outerElement.field("x").arrayElement();
    assertThat(resolver.schema().byPath(outerElement).orElseThrow().nativeFieldId())
        .isEqualTo(OptionalInt.of(10));
    assertThat(resolver.schema().byPath(innerElement).orElseThrow().nativeFieldId())
        .isEqualTo(OptionalInt.of(20));
  }

  @Test
  void collectionInteriorsWithoutNestedIdsAreLegalAndReportNoFormatId() {
    String schema =
        """
        {"type":"struct","fields":[
          {"name":"items","nullable":true,
           "metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"col-a"},
           "type":{"type":"array","containsNull":true,"elementType":"long"}},
          {"name":"attrs","nullable":true,
           "metadata":{"delta.columnMapping.id":2,"delta.columnMapping.physicalName":"col-b"},
           "type":{"type":"map","valueContainsNull":true,
             "keyType":"string","valueType":"long"}}
        ]}
        """;
    var resolver = DeltaSchemaResolver.resolve(schema, ColumnMappingMode.NAME);

    assertThat(
            resolver.schema().byPath(ColumnPath.ROOT.field("items").arrayElement()).orElseThrow())
        .returns(false, SchemaNode::hasNativeFieldId)
        .returns(NodeKind.ARRAY_ELEMENT, SchemaNode::kind);
    assertThat(resolver.schema().byPath(ColumnPath.ROOT.field("attrs").mapKey()).orElseThrow())
        .returns(false, SchemaNode::hasNativeFieldId);
    assertThat(resolver.schema().byPath(ColumnPath.ROOT.field("attrs").mapValue()).orElseThrow())
        .returns(false, SchemaNode::hasNativeFieldId);
  }

  // ------------------------------------------------- authoritative vs residual mapping metadata

  /** Mapping metadata left behind on a table whose effective mode is NONE. */
  private static final String RESIDUAL_METADATA_SCHEMA =
      """
      {"type":"struct","fields":[
        {"name":"id","type":"long","nullable":true,
         "metadata":{"delta.columnMapping.id":17,"delta.columnMapping.physicalName":"col-abc"}},
        {"name":"items","nullable":true,
         "metadata":{"delta.columnMapping.id":18,"delta.columnMapping.physicalName":"col-def",
                     "delta.columnMapping.nested.ids":{"col-def.element":19}},
         "type":{"type":"array","containsNull":true,"elementType":"long"}}
      ]}
      """;

  @Test
  void residualMappingMetadataIsInertWhenTheEffectiveModeIsNone() {
    // Delta resolves by display name in none mode, so leftover mapping metadata governs nothing.
    var schema =
        DeltaSchemaResolver.resolve(RESIDUAL_METADATA_SCHEMA, ColumnMappingMode.NONE).schema();

    assertThat(schema.nodes()).isNotEmpty();
    assertThat(schema.nodes()).allMatch(n -> n.nativeFieldId().isEmpty());
    assertThat(schema.nodes()).allMatch(n -> n.sourcePhysicalPath().isEmpty());
    assertThat(schema.byNativeFieldId(17)).isEmpty();
    assertThat(schema.hasTotalNativeIds()).isFalse();
  }

  @Test
  void residualMetadataCannotSeedCanonicalIdentity() {
    var schema =
        DeltaSchemaResolver.resolve(RESIDUAL_METADATA_SCHEMA, ColumnMappingMode.NONE).schema();

    assertThat(NativeIdentityAssigner.canSeed(schema)).isFalse();
    assertThatThrownBy(() -> NativeIdentityAssigner.seed(schema))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void noneModeResolvesParquetPathsByDisplayNameNotResidualPhysicalName() {
    var resolver = DeltaSchemaResolver.resolve(RESIDUAL_METADATA_SCHEMA, ColumnMappingMode.NONE);

    assertThat(resolver.nodeForParquetPath("id")).isPresent();
    assertThat(resolver.nodeForParquetPath("col-abc")).isEmpty();
  }

  @Test
  void unknownModeYieldsNoAuthoritativeIdentityButStillSupportsLookup() {
    // Not knowing the effective mode means not knowing whether the metadata governs.
    var resolver = DeltaSchemaResolver.resolve(MAPPED_SCHEMA);
    var schema = resolver.schema();

    assertThat(resolver.effectiveMappingMode()).isEmpty();
    assertThat(schema.nodes()).allMatch(n -> n.nativeFieldId().isEmpty());
    assertThat(schema.nodes()).allMatch(n -> n.sourcePhysicalPath().isEmpty());
    assertThat(NativeIdentityAssigner.canSeed(schema)).isFalse();

    // Lookup still consults the residual names, which is what this overload is for.
    assertThat(resolver.nodeForParquetPath("col-bbb.col-ccc"))
        .map(n -> n.path())
        .contains(path("s", "a"));
    assertThat(resolver.nodeForStatsNames(List.of("col-bbb", "col-ccc")))
        .map(n -> n.path())
        .contains(path("s", "a"));
  }

  @Test
  void mappedModeYieldsAuthoritativeIdentity() {
    var schema = DeltaSchemaResolver.resolve(MAPPED_SCHEMA, ColumnMappingMode.NAME).schema();

    assertThat(schema.byPath(path("id")).orElseThrow().nativeFieldId())
        .isEqualTo(OptionalInt.of(1));
    assertThat(schema.byPath(path("id")).orElseThrow().sourcePhysicalPath()).isPresent();
  }

  // ---------------------------------------------------------------- validation

  @Test
  void duplicateOrdinaryIdsAreRejected() {
    String schema =
        """
        {"type":"struct","fields":[
          {"name":"a","type":"long","nullable":true,
           "metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"col-a"}},
          {"name":"b","type":"long","nullable":true,
           "metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"col-b"}}
        ]}
        """;
    assertThatThrownBy(() -> DeltaSchemaResolver.resolve(schema, ColumnMappingMode.ID))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("duplicate native field id 1");
  }

  @Test
  void duplicateNestedIdsAreRejected() {
    String schema =
        """
        {"type":"struct","fields":[
          {"name":"a","nullable":true,
           "metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"col-a",
                       "delta.columnMapping.nested.ids":{"col-a.element":90}},
           "type":{"type":"array","containsNull":true,"elementType":"long"}},
          {"name":"b","nullable":true,
           "metadata":{"delta.columnMapping.id":2,"delta.columnMapping.physicalName":"col-b",
                       "delta.columnMapping.nested.ids":{"col-b.element":90}},
           "type":{"type":"array","containsNull":true,"elementType":"long"}}
        ]}
        """;
    assertThatThrownBy(() -> DeltaSchemaResolver.resolve(schema, ColumnMappingMode.ID))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("duplicate native field id 90");
  }

  @Test
  void ordinaryAndNestedIdsShareOneNamespace() {
    // Both feed SchemaNode.nativeFieldId, so ResolvedSchema enforces uniqueness across the union.
    String schema =
        """
        {"type":"struct","fields":[
          {"name":"a","nullable":true,
           "metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"col-a",
                       "delta.columnMapping.nested.ids":{"col-a.element":2}},
           "type":{"type":"array","containsNull":true,"elementType":"long"}},
          {"name":"b","type":"long","nullable":true,
           "metadata":{"delta.columnMapping.id":2,"delta.columnMapping.physicalName":"col-b"}}
        ]}
        """;
    assertThatThrownBy(() -> DeltaSchemaResolver.resolve(schema, ColumnMappingMode.ID))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("duplicate native field id 2");
  }

  @Test
  void malformedNestedIdMetadataIsRejected() {
    String nonObject =
        """
        {"type":"struct","fields":[
          {"name":"a","nullable":true,
           "metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"col-a",
                       "delta.columnMapping.nested.ids":"nope"},
           "type":{"type":"array","containsNull":true,"elementType":"long"}}
        ]}
        """;
    assertThatThrownBy(() -> DeltaSchemaResolver.resolve(nonObject, ColumnMappingMode.ID))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("non-object delta.columnMapping.nested.ids");

    String negative =
        """
        {"type":"struct","fields":[
          {"name":"a","nullable":true,
           "metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"col-a",
                       "delta.columnMapping.nested.ids":{"col-a.element":0}},
           "type":{"type":"array","containsNull":true,"elementType":"long"}}
        ]}
        """;
    assertThatThrownBy(() -> DeltaSchemaResolver.resolve(negative, ColumnMappingMode.ID))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("non-positive nested id");
  }

  @Test
  void nestedIdNamingNoRealNodeIsRejected() {
    String schema =
        """
        {"type":"struct","fields":[
          {"name":"a","nullable":true,
           "metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"col-a",
                       "delta.columnMapping.nested.ids":{"col-a.key":5}},
           "type":{"type":"array","containsNull":true,"elementType":"long"}}
        ]}
        """;
    assertThatThrownBy(() -> DeltaSchemaResolver.resolve(schema, ColumnMappingMode.ID))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("matches no node in its type");
  }

  @Test
  void mappedFieldMissingItsIdIsRejected() {
    String schema =
        """
        {"type":"struct","fields":[
          {"name":"a","type":"long","nullable":true,
           "metadata":{"delta.columnMapping.physicalName":"col-a"}}
        ]}
        """;
    assertThatThrownBy(() -> DeltaSchemaResolver.resolve(schema, ColumnMappingMode.ID))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("has no delta.columnMapping.id");
  }

  @Test
  void mappedFieldMissingItsPhysicalNameIsRejected() {
    String schema =
        """
        {"type":"struct","fields":[
          {"name":"a","type":"long","nullable":true,"metadata":{"delta.columnMapping.id":1}}
        ]}
        """;
    assertThatThrownBy(() -> DeltaSchemaResolver.resolve(schema, ColumnMappingMode.NAME))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("has no delta.columnMapping.physicalName");
  }

  @Test
  void unmappedSchemaNeedsNoMappingMetadata() {
    assertThatCode(() -> DeltaSchemaResolver.resolve(UNMAPPED_SCHEMA, ColumnMappingMode.NONE))
        .doesNotThrowAnyException();
  }

  @Test
  void duplicatePhysicalPathsAreRejected() {
    String schema =
        """
        {"type":"struct","fields":[
          {"name":"a","type":"long","nullable":true,
           "metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"col-x"}},
          {"name":"b","type":"long","nullable":true,
           "metadata":{"delta.columnMapping.id":2,"delta.columnMapping.physicalName":"col-x"}}
        ]}
        """;
    assertThatThrownBy(() -> DeltaSchemaResolver.resolve(schema, ColumnMappingMode.ID))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("duplicate physical path");
  }

  // ---------------------------------------------------------------- parquet resolution

  @Test
  void resolvesParquetPathsThroughCollectionWrappers() {
    var resolver = DeltaSchemaResolver.resolve(MAPPED_SCHEMA, ColumnMappingMode.NAME);

    assertThat(resolver.nodeForParquetPath("col-eee.list.element.col-fff"))
        .map(n -> n.path().display())
        .contains("items[].x");
    assertThat(resolver.nodeForParquetPath("col-ggg.key_value.key"))
        .map(n -> n.path().display())
        .contains("attrs.key");
    assertThat(resolver.nodeForParquetPath("col-ggg.key_value.value.list.element"))
        .map(n -> n.path().display())
        .contains("attrs{}[]");
  }

  @Test
  void structFieldsNamedLikeParquetWrappersAreNotMistakenForCollections() {
    String schema =
        """
        {"type":"struct","fields":[
          {"name":"a","nullable":true,"metadata":{},
           "type":{"type":"struct","fields":[
             {"name":"list","nullable":true,"metadata":{},
              "type":{"type":"struct","fields":[
                {"name":"element","type":"integer","nullable":true,"metadata":{}}
              ]}}
           ]}},
          {"name":"kv","nullable":true,"metadata":{},
           "type":{"type":"struct","fields":[
             {"name":"key_value","nullable":true,"metadata":{},
              "type":{"type":"struct","fields":[
                {"name":"key","type":"string","nullable":true,"metadata":{}}
              ]}}
           ]}}
        ]}
        """;
    var resolver = DeltaSchemaResolver.resolve(schema, ColumnMappingMode.NONE);

    assertThat(resolver.nodeForParquetPath("a.list.element"))
        .map(n -> n.path())
        .contains(path("a", "list", "element"));
    assertThat(resolver.nodeForParquetPath("kv.key_value.key"))
        .map(n -> n.path())
        .contains(path("kv", "key_value", "key"));
  }

  @Test
  void resolvesParquetPathsForUnmappedTables() {
    var resolver = DeltaSchemaResolver.resolve(UNMAPPED_SCHEMA, ColumnMappingMode.NONE);

    assertThat(resolver.nodeForParquetPath("s.a")).map(n -> n.path()).contains(path("s", "a"));
    assertThat(resolver.nodeForParquetPath("items.list.element"))
        .map(n -> n.kind())
        .contains(NodeKind.ARRAY_ELEMENT);
  }

  @Test
  void unmappedTablesDeclareNoSourcePhysicalPaths() {
    // The resolver reports what the source contains. An unmapped table declares no physical names,
    // so it gets none — not a copy of the logical path. Any stable physical identity for such a
    // node is Floecat-maintained state, not source metadata.
    var resolver = DeltaSchemaResolver.resolve(UNMAPPED_SCHEMA, ColumnMappingMode.NONE);

    assertThat(resolver.schema().nodes()).isNotEmpty();
    assertThat(resolver.schema().nodes()).allMatch(n -> n.sourcePhysicalPath().isEmpty());
    assertThat(resolver.schema().bySourcePhysicalPath(path("s", "a"))).isEmpty();
    // Parquet resolution still works: it uses the effective names internally.
    assertThat(resolver.nodeForParquetPath("s.a")).isPresent();
  }

  @Test
  void mappedTablesReportOnlyDeclaredPhysicalPaths() {
    var resolver = DeltaSchemaResolver.resolve(MAPPED_SCHEMA, ColumnMappingMode.NAME);

    assertThat(resolver.schema().nodes()).allMatch(n -> n.sourcePhysicalPath().isPresent());
    assertThat(resolver.schema().byPath(path("s", "a")).orElseThrow().sourcePhysicalPath())
        .contains(path("col-bbb", "col-ccc"));
    // A collection interior inherits its parent's declared physical prefix structurally.
    assertThat(
            resolver
                .schema()
                .byPath(ColumnPath.ROOT.field("items").arrayElement())
                .orElseThrow()
                .sourcePhysicalPath())
        .contains(ColumnPath.ROOT.field("col-eee").arrayElement());
  }

  @Test
  void unknownParquetPathResolvesToNothing() {
    var resolver = DeltaSchemaResolver.resolve(MAPPED_SCHEMA, ColumnMappingMode.NAME);

    assertThat(resolver.nodeForParquetPath("col-eee.list.nope")).isEmpty();
    assertThat(resolver.nodeForParquetPath("")).isEmpty();
    assertThat(resolver.nodeForParquetPath(null)).isEmpty();
  }

  // ---------------------------------------------------------------- statistics names

  @Test
  void resolvesNestedStatsNamesRatherThanCollapsingToTheParent() {
    var resolver = DeltaSchemaResolver.resolve(MAPPED_SCHEMA, ColumnMappingMode.NAME);

    assertThat(resolver.nodeForStatsNames(List.of("col-bbb", "col-ccc")))
        .map(n -> n.path())
        .contains(path("s", "a"));
    assertThat(resolver.nodeForStatsNames(List.of("col-bbb", "col-ddd")))
        .map(n -> n.path())
        .contains(path("s", "b"));
    assertThat(resolver.nodeForStatsNames(List.of("col-bbb")))
        .map(n -> n.path())
        .contains(path("s"));
  }

  @Test
  void mappedStatisticsResolveOnlyPhysicalSpellings() {
    var resolver = DeltaSchemaResolver.resolve(MAPPED_SCHEMA, ColumnMappingMode.NAME);

    assertThat(resolver.nodeForStatsNames(List.of("s", "a"))).isEmpty();
  }

  @Test
  void unmappedStatisticsResolveByLogicalName() {
    var resolver = DeltaSchemaResolver.resolve(UNMAPPED_SCHEMA, ColumnMappingMode.NONE);

    assertThat(resolver.nodeForStatsNames(List.of("s", "a")))
        .map(n -> n.path())
        .contains(path("s", "a"));
  }

  @Test
  void statisticsDoNotDescendIntoCollections() {
    var resolver = DeltaSchemaResolver.resolve(MAPPED_SCHEMA, ColumnMappingMode.NAME);

    assertThat(resolver.nodeForStatsNames(List.of("col-eee", "col-fff"))).isEmpty();
  }

  @Test
  void unknownStatsSegmentYieldsEmptyRatherThanAnAncestor() {
    var resolver = DeltaSchemaResolver.resolve(MAPPED_SCHEMA, ColumnMappingMode.NAME);

    assertThat(resolver.nodeForStatsNames(List.of("col-bbb", "nope"))).isEmpty();
    assertThat(resolver.nodeForStatsNames(List.of())).isEmpty();
  }

  @Test
  void physicalSpellingWinsTheStatsCollisionWhenTheModeIsUnknown() {
    // "col-aaa" is both the physical name of "id" and the logical name of a second column.
    String schema =
        """
        {"type":"struct","fields":[
          {"name":"id","type":"long","nullable":true,
           "metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"col-aaa"}},
          {"name":"col-aaa","type":"string","nullable":true,
           "metadata":{"delta.columnMapping.id":2,"delta.columnMapping.physicalName":"col-zzz"}}
        ]}
        """;
    var unknownMode = DeltaSchemaResolver.resolve(schema);
    assertThat(unknownMode.nodeForStatsNames(List.of("col-aaa")))
        .map(n -> n.path())
        .contains(path("id"));

    var mapped = DeltaSchemaResolver.resolve(schema, ColumnMappingMode.NAME);
    assertThat(mapped.nodeForStatsNames(List.of("col-aaa")))
        .map(n -> n.path())
        .contains(path("id"));
    assertThat(mapped.nodeForStatsNames(List.of("col-zzz")))
        .map(n -> n.path())
        .contains(path("col-aaa"));
  }

  // ---------------------------------------------------------------- structural position

  @Test
  void exposesSiblingOrdinalsForUnmappedIdentityAssignment() {
    var resolver = DeltaSchemaResolver.resolve(UNMAPPED_SCHEMA, ColumnMappingMode.NONE);

    assertThat(resolver.schema().byPath(path("id")).orElseThrow().ordinal()).isEqualTo(1);
    assertThat(resolver.schema().byPath(path("s")).orElseThrow().ordinal()).isEqualTo(2);
    assertThat(resolver.schema().byPath(path("items")).orElseThrow().ordinal()).isEqualTo(3);
    assertThat(resolver.schema().byPath(path("s", "a")).orElseThrow().ordinal()).isEqualTo(1);
    assertThat(resolver.schema().byPath(path("s", "b")).orElseThrow().ordinal()).isEqualTo(2);
    assertThat(
            resolver
                .schema()
                .byPath(ColumnPath.ROOT.field("items").arrayElement())
                .orElseThrow()
                .ordinal())
        .isEqualTo(1);
  }

  @Test
  void exposesEveryNodeInTraversalOrder() {
    var resolver = DeltaSchemaResolver.resolve(UNMAPPED_SCHEMA, ColumnMappingMode.NONE);

    assertThat(resolver.schema().nodes())
        .extracting(n -> n.path().display())
        .containsExactly("id", "s", "s.a", "s.b", "items", "items[]");
  }

  @Test
  void marksContainersAsNonLeaf() {
    var resolver = DeltaSchemaResolver.resolve(MAPPED_SCHEMA, ColumnMappingMode.NAME);

    assertThat(resolver.schema().byPath(path("s")).orElseThrow().leaf()).isFalse();
    assertThat(resolver.schema().byPath(path("items")).orElseThrow().leaf()).isFalse();
    assertThat(resolver.schema().byPath(path("s", "a")).orElseThrow().leaf()).isTrue();
  }

  // ---------------------------------------------------------------- parsing

  @Test
  void blankSchemaYieldsEmptyResolver() {
    assertThat(DeltaSchemaResolver.resolve("").schema().nodes()).isEmpty();
    assertThat(DeltaSchemaResolver.resolve(null).schema().nodes()).isEmpty();
  }

  @Test
  void jsonThatIsNotADeltaStructFailsRatherThanResolvingToNothing() {
    // A syntactically valid document that is not a struct schema must not read as "no columns".
    for (String notASchema : List.of("{\"type\":\"integer\"}", "{\"foo\":1}", "[]", "\"text\"")) {
      assertThatThrownBy(() -> DeltaSchemaResolver.resolve(notASchema, ColumnMappingMode.NONE))
          .as("schema: %s", notASchema)
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Test
  void nestedStructWithoutFieldsIsRejected() {
    String schema =
        """
        {"type":"struct","fields":[
          {"name":"s","nullable":true,"metadata":{},"type":{"type":"struct"}}
        ]}
        """;
    assertThatThrownBy(() -> DeltaSchemaResolver.resolve(schema, ColumnMappingMode.NONE))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("is not a Delta struct schema");
  }

  @Test
  void anExplicitlyEmptyStructIsLegal() {
    var schema =
        DeltaSchemaResolver.resolve("{\"type\":\"struct\",\"fields\":[]}", ColumnMappingMode.NONE)
            .schema();

    assertThat(schema.nodes()).isEmpty();
  }

  @Test
  void malformedSchemaFailsFast() {
    assertThatThrownBy(() -> DeltaSchemaResolver.resolve("{not json"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Failed to parse Delta schema JSON");
  }
}
