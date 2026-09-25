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
  void mappedCollectionInteriorsUseStableDerivedIdsWithoutAHighWaterAllocation() {
    ColumnPath items = ColumnPath.ROOT.field("items");
    ColumnPath element = items.arrayElement();
    var withoutNestedId =
        reconcile(
            3,
            IdentityMode.NATIVE_FIELD_ID,
            Optional.empty(),
            nativeNode(items, 1, 17),
            node(element, 1));
    var withNestedId =
        reconcile(
            9,
            IdentityMode.NATIVE_FIELD_ID,
            Optional.of(withoutNestedId.state()),
            nativeNode(items, 1, 17),
            nativeNode(element, 1, 99));

    long expected = (1L << 62) | (17L << 24) | 1L;
    assertThat(withoutNestedId.nodes())
        .extracting(CanonicalSchemaNode::canonicalId)
        .containsExactly(17L, expected);
    assertThat(withoutNestedId.state().highWaterMark()).isEqualTo(17L);
    assertThat(withNestedId.nodes().get(1).canonicalId()).isEqualTo(expected);
    assertThat(withNestedId.state().entries().get(1).nativeFieldId()).hasValue(99);
    assertThat(withNestedId.state().fingerprint()).isEqualTo(withoutNestedId.state().fingerprint());
  }

  @Test
  void nestedCollectionSuffixesAreDistinctAndUseTheNearestFieldId() {
    ColumnPath items = ColumnPath.ROOT.field("items");
    ColumnPath element = items.arrayElement();
    ColumnPath key = element.mapKey();
    ColumnPath value = element.mapValue();
    var result =
        reconcile(
            0,
            IdentityMode.NATIVE_FIELD_ID,
            Optional.empty(),
            nativeNode(items, 1, 23),
            node(element, 1),
            node(key, 1),
            node(value, 2));

    long prefix = (1L << 62) | (23L << 24);
    assertThat(result.nodes())
        .extracting(CanonicalSchemaNode::canonicalId)
        .containsExactly(23L, prefix | 1L, prefix | 6L, prefix | 7L);
  }

  @Test
  void derivedCollectionIdentityRejectsMoreThanTwelveLevels() {
    ColumnPath root = ColumnPath.ROOT.field("items");
    ColumnPath tooDeep = root;
    for (int i = 0; i < 13; i++) {
      tooDeep = tooDeep.arrayElement();
    }
    SchemaNode deepNode = node(tooDeep, 1);

    assertThatThrownBy(
            () ->
                reconcile(
                    0,
                    IdentityMode.NATIVE_FIELD_ID,
                    Optional.empty(),
                    nativeNode(root, 1, 1),
                    deepNode))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("maximum derived depth of 12");
  }

  @Test
  void completeMetadataHistoryAllowsSparseSourceVersions() {
    var first = reconcile(3, IdentityMode.STRUCTURED_PATH, Optional.empty(), field("x", 1));
    var later =
        reconcile(50, IdentityMode.STRUCTURED_PATH, Optional.of(first.state()), field("x", 1));

    assertThat(later.nodes().getFirst().canonicalId()).isEqualTo(1L);
  }

  @Test
  void historyGapForcesAResetAboveTheRetainedHighWaterMark() {
    var first = reconcile(3, IdentityMode.STRUCTURED_PATH, Optional.empty(), field("x", 1));

    var reset =
        SchemaIdentityReconciler.reconcile(
            ResolvedSchema.of(List.of(field("x", 1))),
            50,
            IdentityMode.STRUCTURED_PATH,
            Optional.of(first.state()),
            HistoryCoverage.GAP);

    assertThat(reset.nodes().getFirst().canonicalId()).isEqualTo(2L);
    assertThat(reset.state().highWaterMark()).isEqualTo(2L);
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
  void historyGapCannotBypassModeChangeGuard() {
    var first = reconcile(3, IdentityMode.STRUCTURED_PATH, Optional.empty(), field("x", 1));

    assertThatThrownBy(
            () ->
                SchemaIdentityReconciler.reconcile(
                    ResolvedSchema.of(List.of(nativeField("x", 1, 1))),
                    4,
                    IdentityMode.NATIVE_FIELD_ID,
                    Optional.of(first.state()),
                    HistoryCoverage.GAP))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("clean identity reset");
  }

  @Test
  void historyGapCannotMoveSourceVersionBackwards() {
    var first = reconcile(50, IdentityMode.STRUCTURED_PATH, Optional.empty(), field("x", 1));

    assertThatThrownBy(
            () ->
                SchemaIdentityReconciler.reconcile(
                    ResolvedSchema.of(List.of(field("x", 1))),
                    7,
                    IdentityMode.STRUCTURED_PATH,
                    Optional.of(first.state()),
                    HistoryCoverage.GAP))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Source version 7 does not follow 50");
  }

  @Test
  void fingerprintIsDeterministicAndBindsTheIdentityMapping() {
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
  void nativeFingerprintIsIndependentOfReconciliationHistory() {
    var prior =
        reconcile(
            1,
            IdentityMode.NATIVE_FIELD_ID,
            Optional.empty(),
            nativeField("a", 1, 1),
            nativeField("b", 2, 2),
            nativeField("retired", 3, 99));
    var warm =
        reconcile(
            2,
            IdentityMode.NATIVE_FIELD_ID,
            Optional.of(prior.state()),
            nativeField("a", 1, 1),
            nativeField("b", 2, 2));
    var cold =
        reconcile(
            2,
            IdentityMode.NATIVE_FIELD_ID,
            Optional.empty(),
            nativeField("a", 1, 1),
            nativeField("b", 2, 2));

    assertThat(warm.nodes()).isEqualTo(cold.nodes());
    assertThat(warm.state().highWaterMark()).isEqualTo(99L);
    assertThat(cold.state().highWaterMark()).isEqualTo(2L);
    assertThat(warm.state().fingerprint()).isEqualTo(cold.state().fingerprint());
    assertThat(warm.state().stateChecksum()).isNotEqualTo(cold.state().stateChecksum());
  }

  @Test
  void fingerprintIgnoresNonIdentityNativeProvenance() {
    var first =
        reconcile(1, IdentityMode.STRUCTURED_PATH, Optional.empty(), nativeField("a", 1, 7));
    var changedProvenance =
        reconcile(
            2, IdentityMode.STRUCTURED_PATH, Optional.of(first.state()), nativeField("a", 1, 11));

    assertThat(changedProvenance.state().entries().getFirst().nativeFieldId()).hasValue(11);
    assertThat(changedProvenance.state().fingerprint()).isEqualTo(first.state().fingerprint());
  }

  @Test
  void stampingADataOnlyVersionPreservesIdentityAndFingerprint() {
    var first = reconcile(4, IdentityMode.STRUCTURED_PATH, Optional.empty(), field("a", 1));

    SchemaIdentityState stamped =
        SchemaIdentityReconciler.stampSourceVersion(
            first.state(), 1_000_000L, HistoryCoverage.COMPLETE_METADATA_HISTORY);

    assertThat(stamped.sourceVersion()).isEqualTo(1_000_000L);
    assertThat(stamped.entries()).isEqualTo(first.state().entries());
    assertThat(stamped.highWaterMark()).isEqualTo(first.state().highWaterMark());
    assertThat(stamped.fingerprint()).isEqualTo(first.state().fingerprint());
    assertThat(stamped.stateChecksum()).isNotEqualTo(first.state().stateChecksum());
  }

  @Test
  void stampingCannotMoveSourceVersionBackwards() {
    var first = reconcile(4, IdentityMode.STRUCTURED_PATH, Optional.empty(), field("a", 1));

    assertThatThrownBy(
            () ->
                SchemaIdentityReconciler.stampSourceVersion(
                    first.state(), 3L, HistoryCoverage.COMPLETE_METADATA_HISTORY))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot stamp source version 3 before 4");
  }

  @Test
  void stampingRejectsAHistoryGap() {
    var first = reconcile(4, IdentityMode.STRUCTURED_PATH, Optional.empty(), field("a", 1));

    assertThatThrownBy(
            () ->
                SchemaIdentityReconciler.stampSourceVersion(first.state(), 5L, HistoryCoverage.GAP))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot stamp across a metadata history gap");
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
                    1L, 1L, IdentityMode.STRUCTURED_PATH, entries, "irrelevant", "irrelevant"))
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
                    "sha256:incorrect",
                    state.stateChecksum()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("fingerprint does not match");
  }

  @Test
  void restoreRejectsIncorrectStateChecksum() {
    var state =
        reconcile(1L, IdentityMode.STRUCTURED_PATH, Optional.empty(), field("a", 1)).state();

    assertThatThrownBy(
            () ->
                SchemaIdentityState.restore(
                    state.sourceVersion(),
                    state.highWaterMark(),
                    state.mode(),
                    state.entries(),
                    state.fingerprint(),
                    "sha256:incorrect"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("state checksum does not match");
  }

  private static SchemaIdentityReconciler.Result reconcile(
      long version,
      IdentityMode mode,
      Optional<SchemaIdentityState> previous,
      SchemaNode... nodes) {
    return SchemaIdentityReconciler.reconcile(
        ResolvedSchema.of(List.of(nodes)),
        version,
        mode,
        previous,
        HistoryCoverage.COMPLETE_METADATA_HISTORY);
  }

  private static SchemaNode field(String name, int ordinal) {
    return node(ColumnPath.ROOT.field(name), ordinal);
  }

  private static SchemaNode nativeField(String name, int ordinal, int nativeId) {
    return nativeNode(ColumnPath.ROOT.field(name), ordinal, nativeId);
  }

  private static SchemaNode nativeNode(ColumnPath path, int ordinal, int nativeId) {
    return new SchemaNode(path, ordinal, true, OptionalInt.of(nativeId), Optional.of(path));
  }

  private static SchemaNode node(ColumnPath path, int ordinal) {
    return new SchemaNode(path, ordinal, true, OptionalInt.empty(), Optional.empty());
  }
}
