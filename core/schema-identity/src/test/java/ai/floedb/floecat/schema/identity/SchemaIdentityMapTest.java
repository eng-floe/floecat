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

import ai.floedb.floecat.catalog.rpc.TableFormat;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import org.junit.jupiter.api.Test;

class SchemaIdentityMapTest {

  private static SchemaNode node(String name, int nativeId) {
    return new SchemaNode(
        ColumnPath.ROOT.field(name),
        NodeKind.FIELD,
        1,
        true,
        OptionalInt.of(nativeId),
        Optional.empty());
  }

  /** An identity with no provenance — valid only against a node that has no native id. */
  private static ColumnIdentity identity(String name, long id) {
    return new ColumnIdentity(ColumnPath.ROOT.field(name), id, NodeKind.FIELD, Optional.empty());
  }

  /** An identity that adopts an Iceberg native id, provenance included. */
  private static ColumnIdentity adopted(String name, int nativeId) {
    return new ColumnIdentity(
        ColumnPath.ROOT.field(name),
        nativeId,
        NodeKind.FIELD,
        Optional.of(new FormatIdentity(TableFormat.TF_ICEBERG, nativeId)));
  }

  @Test
  void resolvesInBothDirections() {
    var schema = ResolvedSchema.of(TableFormat.TF_ICEBERG, List.of(node("a", 1), node("b", 2)));
    var map = NativeIdentityAssigner.seed(schema);

    assertThat(map.byCanonicalPath(ColumnPath.ROOT.field("a")).orElseThrow().columnId())
        .isEqualTo(1L);
    assertThat(map.byId(1L).orElseThrow().path()).isEqualTo(ColumnPath.ROOT.field("a"));
    assertThat(map.byId(99L)).isEmpty();
  }

  @Test
  void rejectsANodeWithNoIdentity() {
    var schema = ResolvedSchema.of(TableFormat.TF_ICEBERG, List.of(node("a", 1), node("b", 2)));

    assertThatThrownBy(
            () -> SchemaIdentityMap.of(TableFormat.TF_ICEBERG, schema, List.of(adopted("a", 1))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("node 'b' has no identity");
  }

  @Test
  void rejectsAnIdentityNamingNoNode() {
    var schema = ResolvedSchema.of(TableFormat.TF_ICEBERG, List.of(node("a", 1)));

    assertThatThrownBy(
            () ->
                SchemaIdentityMap.of(
                    TableFormat.TF_ICEBERG, schema, List.of(adopted("a", 1), adopted("ghost", 2))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("names no node in the schema");
  }

  @Test
  void rejectsTwoNodesSharingAColumnId() {
    var schema = ResolvedSchema.of(TableFormat.TF_ICEBERG, List.of(node("a", 1), node("b", 2)));

    assertThatThrownBy(
            () ->
                SchemaIdentityMap.of(
                    TableFormat.TF_ICEBERG, schema, List.of(adopted("a", 7), adopted("b", 7))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("column id 7 is shared");
  }

  @Test
  void rejectsANonPositiveColumnId() {
    assertThatThrownBy(() -> identity("a", 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("column id must be positive");
  }

  @Test
  void nativeAssignerRefusesSchemasWithGaps() {
    var withGap =
        ResolvedSchema.of(
            TableFormat.TF_DELTA,
            List.of(
                node("a", 1),
                new SchemaNode(
                    ColumnPath.ROOT.field("b"),
                    NodeKind.FIELD,
                    2,
                    true,
                    OptionalInt.empty(),
                    Optional.empty())));

    assertThat(NativeIdentityAssigner.canSeed(withGap)).isFalse();
    assertThatThrownBy(() -> NativeIdentityAssigner.seed(withGap))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("1 node(s) have none, starting with 'b'");
  }

  @Test
  void adoptedIdentitiesRecordTheirFormatProvenance() {
    var schema = ResolvedSchema.of(TableFormat.TF_ICEBERG, List.of(node("a", 42)));
    var identity = NativeIdentityAssigner.seed(schema).byId(42L).orElseThrow();

    assertThat(identity.formatIdentity()).isPresent();
    assertThat(identity.formatIdentity().orElseThrow().format()).isEqualTo(TableFormat.TF_ICEBERG);
    assertThat(identity.formatIdentity().orElseThrow().fieldId()).isEqualTo(42);
  }

  @Test
  void rejectsAnIdentityWithTheWrongKind() {
    var schema = ResolvedSchema.of(TableFormat.TF_ICEBERG, List.of(node("a", 1)));
    var wrongKind =
        new ColumnIdentity(
            ColumnPath.ROOT.field("a"),
            1L,
            NodeKind.MAP_KEY,
            Optional.of(new FormatIdentity(TableFormat.TF_ICEBERG, 1)));

    assertThatThrownBy(
            () -> SchemaIdentityMap.of(TableFormat.TF_ICEBERG, schema, List.of(wrongKind)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("claims kind MAP_KEY but the node is FIELD");
  }

  @Test
  void rejectsProvenanceTheProducerNeverFound() {
    var withoutNative =
        ResolvedSchema.of(
            TableFormat.TF_DELTA,
            List.of(
                new SchemaNode(
                    ColumnPath.ROOT.field("a"),
                    NodeKind.FIELD,
                    1,
                    true,
                    OptionalInt.empty(),
                    Optional.empty())));
    var claimsNative =
        new ColumnIdentity(
            ColumnPath.ROOT.field("a"),
            1L,
            NodeKind.FIELD,
            Optional.of(new FormatIdentity(TableFormat.TF_DELTA, 9)));

    assertThatThrownBy(
            () -> SchemaIdentityMap.of(TableFormat.TF_DELTA, withoutNative, List.of(claimsNative)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("records native id 9 but the node has none");
  }

  @Test
  void rejectsProvenanceThatContradictsTheNativeId() {
    var schema = ResolvedSchema.of(TableFormat.TF_ICEBERG, List.of(node("a", 1)));
    var mismatched =
        new ColumnIdentity(
            ColumnPath.ROOT.field("a"),
            1L,
            NodeKind.FIELD,
            Optional.of(new FormatIdentity(TableFormat.TF_ICEBERG, 77)));

    assertThatThrownBy(
            () -> SchemaIdentityMap.of(TableFormat.TF_ICEBERG, schema, List.of(mismatched)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("records native id 77 but the node has 1");
  }

  @Test
  void rejectsAnIdentityThatDropsProvenanceTheNodeHas() {
    var schema = ResolvedSchema.of(TableFormat.TF_ICEBERG, List.of(node("a", 1)));

    assertThatThrownBy(
            () -> SchemaIdentityMap.of(TableFormat.TF_ICEBERG, schema, List.of(identity("a", 1))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("records no format provenance but the node has native id 1");
  }

  @Test
  void rejectsProvenanceFromAnotherFormat() {
    var schema = ResolvedSchema.of(TableFormat.TF_ICEBERG, List.of(node("a", 1)));
    var foreign =
        new ColumnIdentity(
            ColumnPath.ROOT.field("a"),
            1L,
            NodeKind.FIELD,
            Optional.of(new FormatIdentity(TableFormat.TF_DELTA, 1)));

    assertThatThrownBy(() -> SchemaIdentityMap.of(TableFormat.TF_ICEBERG, schema, List.of(foreign)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("records provenance from TF_DELTA, not TF_ICEBERG");
  }

  @Test
  void rejectsAFormatThatDisagreesWithTheSchema() {
    var schema = ResolvedSchema.of(TableFormat.TF_ICEBERG, List.of(node("a", 1)));

    assertThatThrownBy(() -> SchemaIdentityMap.of(TableFormat.TF_DELTA, schema, List.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("format TF_DELTA does not match schema format TF_ICEBERG");
  }

  @Test
  void resolvedSchemaRejectsDuplicateNativeIds() {
    assertThatThrownBy(
            () -> ResolvedSchema.of(TableFormat.TF_ICEBERG, List.of(node("a", 1), node("b", 1))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("duplicate native field id 1");
  }

  @Test
  void resolvedSchemaRejectsDuplicateCanonicalPaths() {
    assertThatThrownBy(
            () -> ResolvedSchema.of(TableFormat.TF_ICEBERG, List.of(node("a", 1), node("a", 2))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("duplicate canonical path 'a'");
  }
}
