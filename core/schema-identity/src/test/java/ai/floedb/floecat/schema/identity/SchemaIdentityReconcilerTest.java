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
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class SchemaIdentityReconcilerTest {
  @Test
  void mappedRenamePreservesNativeIdentity() {
    var first =
        reconcile(
            10, IdentityMode.NATIVE_FIELD_ID, Optional.empty(), nativeField("old_name", 1, 7));
    var renamed =
        reconcile(
            11, IdentityMode.NATIVE_FIELD_ID, Optional.of(first), nativeField("new_name", 1, 7));

    assertThat(first.entries().getFirst().canonicalId()).isEqualTo(1L);
    assertThat(renamed.entries().getFirst().canonicalId()).isEqualTo(1L);
  }

  @Test
  void unmappedRenameAllocatesNewIdentity() {
    var first = reconcile(10, IdentityMode.STRUCTURED_PATH, Optional.empty(), field("old_name", 1));
    var renamed =
        reconcile(11, IdentityMode.STRUCTURED_PATH, Optional.of(first), field("new_name", 1));

    assertThat(first.entries().getFirst().canonicalId()).isEqualTo(1);
    assertThat(renamed.entries().getFirst().canonicalId()).isEqualTo(2);
    assertThat(renamed.entries())
        .extracting(SchemaIdentityEntry::path)
        .containsExactly(ColumnPath.ROOT.field("new_name"));
  }

  @Test
  void unmappedDropAndLaterReaddAllocatesNewIdentity() {
    var present = reconcile(10, IdentityMode.STRUCTURED_PATH, Optional.empty(), field("x", 1));
    var dropped = reconcile(11, IdentityMode.STRUCTURED_PATH, Optional.of(present));
    var readded = reconcile(12, IdentityMode.STRUCTURED_PATH, Optional.of(dropped), field("x", 1));

    assertThat(present.entries().getFirst().canonicalId()).isEqualTo(1);
    assertThat(dropped.entries()).isEmpty();
    assertThat(dropped.highWaterMark()).isEqualTo(1);
    assertThat(readded.entries().getFirst().canonicalId()).isEqualTo(2);
  }

  @Test
  void reorderAndUnrelatedAddPreserveExistingIdentities() {
    var first =
        reconcile(20, IdentityMode.STRUCTURED_PATH, Optional.empty(), field("a", 1), field("b", 2));
    var evolved =
        reconcile(
            21,
            IdentityMode.STRUCTURED_PATH,
            Optional.of(first),
            field("b", 1),
            field("new", 2),
            field("a", 3));

    assertThat(evolved.byPath(ColumnPath.ROOT.field("a")).orElseThrow().canonicalId()).isEqualTo(1);
    assertThat(evolved.byPath(ColumnPath.ROOT.field("b")).orElseThrow().canonicalId()).isEqualTo(2);
    assertThat(evolved.byPath(ColumnPath.ROOT.field("new")).orElseThrow().canonicalId())
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

    assertThat(result.entries().getFirst().canonicalId()).isEqualTo(1L);
    assertThat(result.entries().get(2).canonicalId()).isEqualTo(2L);
    assertThat(result.entries())
        .filteredOn(node -> node.path().elements().getLast().kind() != NodeKind.FIELD)
        .extracting(SchemaIdentityEntry::canonicalId)
        .allMatch(CanonicalColumnId::isDerived)
        .doesNotHaveDuplicates();
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
            Optional.of(withoutNestedId),
            nativeNode(items, 1, 17),
            nativeNode(element, 1, 99));

    assertThat(withoutNestedId.entries())
        .extracting(SchemaIdentityEntry::canonicalId)
        .containsExactly(1L, withoutNestedId.entries().get(1).canonicalId());
    assertThat(CanonicalColumnId.isDerived(withoutNestedId.entries().get(1).canonicalId()))
        .isTrue();
    assertThat(withoutNestedId.entries().get(1).canonicalId()).isEqualTo(9154591074475197868L);
    assertThat(withoutNestedId.highWaterMark()).isEqualTo(1L);
    assertThat(withNestedId.entries().get(1).canonicalId())
        .isEqualTo(withoutNestedId.entries().get(1).canonicalId());
    assertThat(withNestedId.entries().get(1).nativeFieldId()).hasValue(99);
    assertThat(withNestedId.fingerprint()).isEqualTo(withoutNestedId.fingerprint());
  }

  @Test
  void derivedCollectionIdentitySupportsMoreThanTwelveLevelsAndMappedRename() {
    ColumnPath root = ColumnPath.ROOT.field("items");
    ColumnPath deep = root;
    ColumnPath renamedDeep = ColumnPath.ROOT.field("renamed");
    for (int i = 0; i < 100; i++) {
      deep = deep.arrayElement();
      renamedDeep = renamedDeep.arrayElement();
    }
    var first =
        reconcile(
            0,
            IdentityMode.NATIVE_FIELD_ID,
            Optional.empty(),
            nativeNode(root, 1, 1),
            node(deep, 1));
    var renamed =
        reconcile(
            1,
            IdentityMode.NATIVE_FIELD_ID,
            Optional.of(first),
            nativeNode(ColumnPath.ROOT.field("renamed"), 1, 1),
            node(renamedDeep, 1));

    assertThat(CanonicalColumnId.isDerived(first.entries().get(1).canonicalId())).isTrue();
    assertThat(renamed.entries().get(1).canonicalId())
        .isEqualTo(first.entries().get(1).canonicalId());
  }

  @Test
  void completeMetadataHistoryAllowsSparseSourceVersions() {
    var first = reconcile(3, IdentityMode.STRUCTURED_PATH, Optional.empty(), field("x", 1));
    var later = reconcile(50, IdentityMode.STRUCTURED_PATH, Optional.of(first), field("x", 1));

    assertThat(later.entries().getFirst().canonicalId()).isEqualTo(1L);
  }

  @Test
  void modeChangeRequiresExplicitReset() {
    var first = reconcile(3, IdentityMode.STRUCTURED_PATH, Optional.empty(), field("x", 1));

    assertThatThrownBy(
            () ->
                reconcile(
                    4, IdentityMode.NATIVE_FIELD_ID, Optional.of(first), nativeField("x", 1, 1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("clean identity reset");
  }

  @Test
  void fingerprintIsDeterministicAndBindsTheIdentityMapping() {
    var first =
        reconcile(1, IdentityMode.STRUCTURED_PATH, Optional.empty(), field("a", 1), field("b", 2));
    var unchanged =
        reconcile(
            2, IdentityMode.STRUCTURED_PATH, Optional.of(first), field("a", 1), field("b", 2));
    var dropped = reconcile(2, IdentityMode.STRUCTURED_PATH, Optional.of(first), field("a", 1));

    assertThat(unchanged.fingerprint()).isEqualTo(first.fingerprint());
    assertThat(dropped.fingerprint()).isNotEqualTo(first.fingerprint());
  }

  @Test
  void fingerprintAndChecksumHaveCrossLanguageGoldenVectors() {
    var result = reconcile(7, IdentityMode.STRUCTURED_PATH, Optional.empty(), field("a😀", 1));

    assertThat(result.fingerprint())
        .isEqualTo("sha256:aef445f2dd509c4a33e88d9f689798896ea902a8a741667e3fc51640d8f89176");
    assertThat(result.stateChecksum())
        .isEqualTo("sha256:6e670fd6e82ed45100654f195bf2c328b7ee6a6183991e74092e9a2efd64909e");
  }

  @Test
  void nativeFingerprintIgnoresRetiredIdsWithinAGeneration() {
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
            Optional.of(prior),
            nativeField("a", 1, 1),
            nativeField("b", 2, 2));
    var cold =
        reconcile(
            2,
            IdentityMode.NATIVE_FIELD_ID,
            Optional.empty(),
            nativeField("a", 1, 1),
            nativeField("b", 2, 2));

    assertThat(warm.entries()).isEqualTo(cold.entries());
    assertThat(warm.highWaterMark()).isEqualTo(3L);
    assertThat(cold.highWaterMark()).isEqualTo(2L);
    assertThat(warm.fingerprint()).isEqualTo(cold.fingerprint());
    assertThat(warm.stateChecksum()).isNotEqualTo(cold.stateChecksum());
  }

  @Test
  void preferredNativeIdsPreserveBackfillContinuity() {
    var result =
        SchemaIdentityReconciler.reconcile(
            ResolvedSchema.of(List.of(nativeField("renamed", 1, 17))),
            5L,
            IdentityMode.NATIVE_FIELD_ID,
            Optional.empty(),
            40L,
            PreferredCanonicalIds.byNativeId(java.util.Map.of(17, 9L)),
            HistoryTier.VERSIONED_LOG);

    assertThat(result.entries().getFirst().canonicalId()).isEqualTo(9L);
    assertThat(result.highWaterMark()).isEqualTo(40L);
  }

  @Test
  void preferredNativeIdCannotTakeAnotherNativeIdsCanonicalIdAtTheSamePath() {
    var previous =
        reconcile(4L, IdentityMode.NATIVE_FIELD_ID, Optional.empty(), nativeField("value", 1, 10));

    var result =
        SchemaIdentityReconciler.reconcile(
            ResolvedSchema.of(List.of(nativeField("value", 1, 17))),
            5L,
            IdentityMode.NATIVE_FIELD_ID,
            Optional.of(previous),
            40L,
            PreferredCanonicalIds.byNativeId(java.util.Map.of(17, 1L)),
            HistoryTier.VERSIONED_LOG);

    assertThat(result.entries().getFirst().canonicalId()).isEqualTo(41L);
  }

  @Test
  void fingerprintIgnoresNonIdentityNativeProvenance() {
    var first =
        reconcile(1, IdentityMode.STRUCTURED_PATH, Optional.empty(), nativeField("a", 1, 7));
    var changedProvenance =
        reconcile(2, IdentityMode.STRUCTURED_PATH, Optional.of(first), nativeField("a", 1, 11));

    assertThat(changedProvenance.entries().getFirst().nativeFieldId()).hasValue(11);
    assertThat(changedProvenance.fingerprint()).isEqualTo(first.fingerprint());
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
                    1L,
                    1L,
                    IdentityMode.STRUCTURED_PATH,
                    HistoryTier.VERSIONED_LOG,
                    OptionalLong.empty(),
                    true,
                    entries,
                    "irrelevant",
                    "irrelevant"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Duplicate canonical column ID 1");
  }

  @Test
  void restoreRejectsMissingOrDuplicateNativeFieldIds() {
    for (OptionalInt nativeId : List.of(OptionalInt.empty(), OptionalInt.of(7))) {
      var entries =
          List.of(
              new SchemaIdentityEntry(ColumnPath.ROOT.field("a"), OptionalInt.of(7), 1L),
              new SchemaIdentityEntry(ColumnPath.ROOT.field("b"), nativeId, 2L));
      assertThatThrownBy(
              () ->
                  SchemaIdentityState.restore(
                      1L,
                      2L,
                      IdentityMode.NATIVE_FIELD_ID,
                      HistoryTier.VERSIONED_LOG,
                      OptionalLong.empty(),
                      true,
                      entries,
                      "irrelevant",
                      "irrelevant"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining(
              nativeId.isPresent() ? "Duplicate native field ID" : "positive native ID");
    }
  }

  @Test
  void newGenerationAllocatesAboveTheHighWaterMarkFloor() {
    var reset =
        SchemaIdentityReconciler.reconcile(
            ResolvedSchema.of(List.of(field("a", 1), field("c", 2))),
            30L,
            IdentityMode.STRUCTURED_PATH,
            Optional.empty(),
            3L,
            PreferredCanonicalIds.NONE,
            HistoryTier.VERSIONED_LOG);

    assertThat(reset.entries())
        .extracting(SchemaIdentityEntry::canonicalId)
        .containsExactly(4L, 5L);
    assertThat(reset.highWaterMark()).isEqualTo(5L);
  }

  private static SchemaIdentityState reconcile(
      long version,
      IdentityMode mode,
      Optional<SchemaIdentityState> previous,
      SchemaNode... nodes) {
    return SchemaIdentityReconciler.reconcile(
        ResolvedSchema.of(List.of(nodes)),
        version,
        mode,
        previous,
        0L,
        PreferredCanonicalIds.NONE,
        HistoryTier.VERSIONED_LOG);
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
