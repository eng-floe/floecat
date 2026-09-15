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

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import org.junit.jupiter.api.Test;

class SchemaIdentityReconcilerTest {
  @Test
  void mappedRenamePreservesNativeIdentity() {
    var first =
        reconcile(
            10, IdentityMode.NATIVE_FIELD_ID, Optional.empty(), nativeField("old_name", 1, 7));
    var renamed =
        reconcile(
            11,
            IdentityMode.NATIVE_FIELD_ID,
            Optional.of(first.state()),
            nativeField("new_name", 1, 7));

    assertThat(first.nodes().getFirst().canonicalId()).isEqualTo(7);
    assertThat(renamed.nodes().getFirst().canonicalId()).isEqualTo(7);
  }

  @Test
  void unmappedRenameAllocatesNewIdentity() {
    var first = reconcile(10, IdentityMode.STRUCTURED_PATH, Optional.empty(), field("old_name", 1));
    var renamed =
        reconcile(
            11, IdentityMode.STRUCTURED_PATH, Optional.of(first.state()), field("new_name", 1));

    assertThat(first.nodes().getFirst().canonicalId()).isEqualTo(1);
    assertThat(renamed.nodes().getFirst().canonicalId()).isEqualTo(2);
    assertThat(renamed.state().entries())
        .extracting(SchemaIdentityEntry::path)
        .containsExactly(ColumnPath.ROOT.field("new_name"));
  }

  @Test
  void unmappedDropAndLaterReaddAllocatesNewIdentity() {
    var present = reconcile(10, IdentityMode.STRUCTURED_PATH, Optional.empty(), field("x", 1));
    var dropped = reconcile(11, IdentityMode.STRUCTURED_PATH, Optional.of(present.state()));
    var readded =
        reconcile(12, IdentityMode.STRUCTURED_PATH, Optional.of(dropped.state()), field("x", 1));

    assertThat(present.nodes().getFirst().canonicalId()).isEqualTo(1);
    assertThat(dropped.state().entries()).isEmpty();
    assertThat(dropped.state().highWaterMark()).isEqualTo(1);
    assertThat(readded.nodes().getFirst().canonicalId()).isEqualTo(2);
  }

  @Test
  void reorderAndUnrelatedAddPreserveExistingIdentities() {
    var first =
        reconcile(20, IdentityMode.STRUCTURED_PATH, Optional.empty(), field("a", 1), field("b", 2));
    var evolved =
        reconcile(
            21,
            IdentityMode.STRUCTURED_PATH,
            Optional.of(first.state()),
            field("b", 1),
            field("new", 2),
            field("a", 3));

    assertThat(evolved.state().byPath(ColumnPath.ROOT.field("a")).orElseThrow().canonicalId())
        .isEqualTo(1);
    assertThat(evolved.state().byPath(ColumnPath.ROOT.field("b")).orElseThrow().canonicalId())
        .isEqualTo(2);
    assertThat(evolved.state().byPath(ColumnPath.ROOT.field("new")).orElseThrow().canonicalId())
        .isEqualTo(3);
  }

  @Test
  void assignsEveryCollectionInterior() {
    ColumnPath array = ColumnPath.ROOT.field("items");
    ColumnPath map = array.arrayElement().field("attributes");
    var result =
        reconcile(
            0,
            IdentityMode.STRUCTURED_PATH,
            Optional.empty(),
            node(array, 1),
            node(array.arrayElement(), 1),
            node(map, 1),
            node(map.mapKey(), 1),
            node(map.mapValue(), 2));

    assertThat(result.nodes())
        .extracting(CanonicalSchemaNode::canonicalId)
        .containsExactly(1L, 2L, 3L, 4L, 5L);
  }

  @Test
  void unmappedGapIsRejectedBecauseDropReaddCannotBeDistinguished() {
    var first = reconcile(3, IdentityMode.STRUCTURED_PATH, Optional.empty(), field("x", 1));

    assertThatThrownBy(
            () ->
                reconcile(
                    5, IdentityMode.STRUCTURED_PATH, Optional.of(first.state()), field("x", 1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot skip source versions")
        .hasMessageContaining("expected 4");
  }

  @Test
  void mappedGapIsSafeBecauseNativeIdentityIsAuthoritative() {
    var first =
        reconcile(3, IdentityMode.NATIVE_FIELD_ID, Optional.empty(), nativeField("old", 1, 9));
    var later =
        reconcile(
            8, IdentityMode.NATIVE_FIELD_ID, Optional.of(first.state()), nativeField("new", 1, 9));

    assertThat(later.nodes().getFirst().canonicalId()).isEqualTo(9);
  }

  @Test
  void modeChangeRequiresExplicitReset() {
    var first = reconcile(3, IdentityMode.STRUCTURED_PATH, Optional.empty(), field("x", 1));

    assertThatThrownBy(
            () ->
                reconcile(
                    4,
                    IdentityMode.NATIVE_FIELD_ID,
                    Optional.of(first.state()),
                    nativeField("x", 1, 1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("clean identity reset");
  }

  @Test
  void fingerprintIsDeterministicAndBindsHighWaterMark() {
    var first =
        reconcile(1, IdentityMode.STRUCTURED_PATH, Optional.empty(), field("a", 1), field("b", 2));
    var unchanged =
        reconcile(
            2,
            IdentityMode.STRUCTURED_PATH,
            Optional.of(first.state()),
            field("a", 1),
            field("b", 2));
    var dropped =
        reconcile(2, IdentityMode.STRUCTURED_PATH, Optional.of(first.state()), field("a", 1));

    assertThat(unchanged.state().fingerprint()).isEqualTo(first.state().fingerprint());
    assertThat(dropped.state().fingerprint()).isNotEqualTo(first.state().fingerprint());
  }

  @Test
  void restoreRejectsDuplicateCanonicalIds() {
    List<SchemaIdentityEntry> entries =
        List.of(
            new SchemaIdentityEntry(ColumnPath.ROOT.field("a"), OptionalInt.empty(), 1L),
            new SchemaIdentityEntry(ColumnPath.ROOT.field("b"), OptionalInt.empty(), 1L));

    assertThatThrownBy(
            () ->
                SchemaIdentityState.restore(
                    1L, 1L, IdentityMode.STRUCTURED_PATH, entries, "irrelevant"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Duplicate canonical column ID 1");
  }

  @Test
  void resetAllocatesAbovePreviousHighWaterMark() {
    var reset =
        SchemaIdentityReconciler.reset(
            ResolvedSchema.of(List.of(field("a", 1), field("c", 2))),
            30L,
            IdentityMode.STRUCTURED_PATH,
            3L);

    assertThat(reset.nodes()).extracting(CanonicalSchemaNode::canonicalId).containsExactly(4L, 5L);
    assertThat(reset.state().highWaterMark()).isEqualTo(5L);
  }

  @Test
  void restoreRejectsIncorrectFingerprint() {
    var state =
        reconcile(1L, IdentityMode.STRUCTURED_PATH, Optional.empty(), field("a", 1)).state();

    assertThatThrownBy(
            () ->
                SchemaIdentityState.restore(
                    state.sourceVersion(),
                    state.highWaterMark(),
                    state.mode(),
                    state.entries(),
                    "sha256:incorrect"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("fingerprint does not match");
  }

  private static SchemaIdentityReconciler.Result reconcile(
      long version,
      IdentityMode mode,
      Optional<SchemaIdentityState> previous,
      SchemaNode... nodes) {
    return SchemaIdentityReconciler.reconcile(
        ResolvedSchema.of(List.of(nodes)), version, mode, previous);
  }

  private static SchemaNode field(String name, int ordinal) {
    return node(ColumnPath.ROOT.field(name), ordinal);
  }

  private static SchemaNode nativeField(String name, int ordinal, int nativeId) {
    return new SchemaNode(
        ColumnPath.ROOT.field(name),
        ordinal,
        true,
        OptionalInt.of(nativeId),
        Optional.of(ColumnPath.ROOT.field(name)));
  }

  private static SchemaNode node(ColumnPath path, int ordinal) {
    return new SchemaNode(path, ordinal, true, OptionalInt.empty(), Optional.empty());
  }
}
