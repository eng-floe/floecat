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

import ai.floedb.floecat.schema.identity.delta.ColumnMappingMode;
import ai.floedb.floecat.schema.identity.delta.DeltaSchemaResolver;
import ai.floedb.floecat.schema.identity.iceberg.IcebergSchemaResolver;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The point of the neutral model: the same logical schema must yield the same node universe
 * whichever format it came from. If these drift, the abstraction has become format-shaped.
 */
class FormatNeutralityTest {

  private static List<String> paths(ResolvedSchema schema) {
    return schema.nodes().stream().map(n -> n.path().display()).toList();
  }

  private static List<NodeKind> kinds(ResolvedSchema schema) {
    return schema.nodes().stream().map(SchemaNode::kind).toList();
  }

  private static List<Integer> ordinals(ResolvedSchema schema) {
    return schema.nodes().stream().map(SchemaNode::ordinal).toList();
  }

  @Test
  void icebergProducesTheExpectedNodeUniverse() {
    var schema = IcebergSchemaResolver.resolve(SchemaFixtures.ICEBERG_JSON);

    assertThat(paths(schema)).containsExactlyElementsOf(SchemaFixtures.EXPECTED_PATHS);
    assertThat(kinds(schema)).containsExactlyElementsOf(SchemaFixtures.EXPECTED_KINDS);
  }

  @Test
  void unmappedDeltaProducesTheSameNodeUniverse() {
    var schema =
        DeltaSchemaResolver.resolve(SchemaFixtures.DELTA_UNMAPPED_JSON, ColumnMappingMode.NONE)
            .schema();

    assertThat(paths(schema)).containsExactlyElementsOf(SchemaFixtures.EXPECTED_PATHS);
    assertThat(kinds(schema)).containsExactlyElementsOf(SchemaFixtures.EXPECTED_KINDS);
  }

  @Test
  void mappedDeltaProducesTheSameNodeUniverse() {
    var schema =
        DeltaSchemaResolver.resolve(SchemaFixtures.DELTA_MAPPED_JSON, ColumnMappingMode.NAME)
            .schema();

    assertThat(paths(schema)).containsExactlyElementsOf(SchemaFixtures.EXPECTED_PATHS);
    assertThat(kinds(schema)).containsExactlyElementsOf(SchemaFixtures.EXPECTED_KINDS);
  }

  @Test
  void everyProducerAgreesOnPathsKindsAndOrdinals() {
    var iceberg = IcebergSchemaResolver.resolve(SchemaFixtures.ICEBERG_JSON);
    var deltaUnmapped =
        DeltaSchemaResolver.resolve(SchemaFixtures.DELTA_UNMAPPED_JSON, ColumnMappingMode.NONE)
            .schema();
    var deltaMapped =
        DeltaSchemaResolver.resolve(SchemaFixtures.DELTA_MAPPED_JSON, ColumnMappingMode.ID)
            .schema();

    assertThat(paths(deltaUnmapped)).isEqualTo(paths(iceberg));
    assertThat(paths(deltaMapped)).isEqualTo(paths(iceberg));
    assertThat(kinds(deltaUnmapped)).isEqualTo(kinds(iceberg));
    assertThat(kinds(deltaMapped)).isEqualTo(kinds(iceberg));
    assertThat(ordinals(deltaUnmapped)).isEqualTo(ordinals(iceberg));
    assertThat(ordinals(deltaMapped)).isEqualTo(ordinals(iceberg));
  }

  @Test
  void formatsDifferOnlyInWhereIdsComeFrom() {
    var iceberg = IcebergSchemaResolver.resolve(SchemaFixtures.ICEBERG_JSON);
    var deltaMapped =
        DeltaSchemaResolver.resolve(SchemaFixtures.DELTA_MAPPED_JSON, ColumnMappingMode.ID)
            .schema();
    var deltaUnmapped =
        DeltaSchemaResolver.resolve(SchemaFixtures.DELTA_UNMAPPED_JSON, ColumnMappingMode.NONE)
            .schema();

    assertThat(iceberg.hasTotalNativeIds()).isTrue();
    assertThat(deltaMapped.hasTotalNativeIds()).isTrue();
    assertThat(deltaUnmapped.hasTotalNativeIds()).isFalse();
    assertThat(deltaUnmapped.nodesWithoutNativeIds()).hasSameSizeAs(deltaUnmapped.nodes());

    // Only Delta carries a per-node physical path; Iceberg has no analogue.
    assertThat(iceberg.nodes()).allMatch(n -> n.sourcePhysicalPath().isEmpty());
    assertThat(deltaMapped.nodes()).allMatch(n -> n.sourcePhysicalPath().isPresent());
  }

  @Test
  void bothFormatsSatisfyTheIdentityInvariantWhenNativeIdsAreTotal() {
    for (ResolvedSchema schema :
        List.of(
            IcebergSchemaResolver.resolve(SchemaFixtures.ICEBERG_JSON),
            DeltaSchemaResolver.resolve(SchemaFixtures.DELTA_MAPPED_JSON, ColumnMappingMode.ID)
                .schema())) {
      SchemaIdentityMap identities = NativeIdentityAssigner.seed(schema);

      assertThat(identities.size()).isEqualTo(schema.nodes().size());
      for (SchemaNode node : schema.nodes()) {
        var identity = identities.byCanonicalPath(node.path());
        assertThat(identity).as("path -> id is total").isPresent();
        assertThat(identities.byId(identity.orElseThrow().columnId()))
            .as("id -> path is the exact inverse")
            .contains(identity.orElseThrow());
      }
    }
  }
}
