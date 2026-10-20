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

import ai.floedb.floecat.catalog.rpc.Snapshot;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import org.junit.jupiter.api.Test;

class ColumnIdentityMapsTest {
  @Test
  void roundTripsAndValidatesTheCompleteState() {
    var state = state(7L);
    var map = ColumnIdentityMaps.toProto(state);

    assertThat(ColumnIdentityMaps.restoreForSnapshot(map, 7L).entries()).isEqualTo(state.entries());
    assertThat(ColumnIdentityMaps.restore(map).historyTier()).isEqualTo(HistoryTier.VERSIONED_LOG);
    assertThat(
            ColumnIdentityMaps.fingerprint(
                Snapshot.newBuilder().setSnapshotId(7L).setColumnIdentityMap(map).build()))
        .contains(state.fingerprint());
  }

  @Test
  void rejectsFingerprintSnapshotVersionAndMissingHistoryTier() {
    var map = ColumnIdentityMaps.toProto(state(7L));

    assertThatThrownBy(
            () ->
                ColumnIdentityMaps.restore(
                    map.toBuilder().setFingerprint("sha256:tampered").build()))
        .hasMessageContaining("fingerprint does not match");
    assertThatThrownBy(() -> ColumnIdentityMaps.restoreForSnapshot(map, 8L))
        .hasMessageContaining("does not match snapshot 8");
    assertThatThrownBy(() -> ColumnIdentityMaps.restore(map.toBuilder().clearHistoryTier().build()))
        .hasMessageContaining("no history tier");
  }

  @Test
  void nativeHighWaterMarkIsIntegrityProtected() {
    SchemaNode node =
        new SchemaNode(
            ColumnPath.ROOT.field("value"),
            1,
            true,
            OptionalInt.of(7),
            Optional.of(ColumnPath.ROOT.field("value")));
    SchemaIdentityState state =
        SchemaIdentityReconciler.reconcile(
            ResolvedSchema.of(List.of(node)),
            9L,
            IdentityMode.NATIVE_FIELD_ID,
            Optional.empty(),
            0L,
            PreferredCanonicalIds.NONE,
            HistoryTier.VERSIONED_LOG);
    var map = ColumnIdentityMaps.withNativeHighWaterMark(ColumnIdentityMaps.toProto(state), 11L);

    assertThat(ColumnIdentityMaps.restore(map).nativeHighWaterMark()).hasValue(11L);
    assertThatThrownBy(
            () -> ColumnIdentityMaps.restore(map.toBuilder().setNativeHighWaterMark(12L).build()))
        .hasMessageContaining("state checksum does not match");
    assertThatThrownBy(
            () ->
                ColumnIdentityMaps.restore(
                    map.toBuilder()
                        .setEntries(0, map.getEntries(0).toBuilder().setNativeFieldId(8).build())
                        .build()))
        .hasMessageContaining("state checksum does not match");
  }

  @Test
  void historyTierIsIntegrityProtected() {
    var map = ColumnIdentityMaps.toProto(state(7L));

    assertThatThrownBy(
            () ->
                ColumnIdentityMaps.restore(
                    map.toBuilder()
                        .setHistoryTier(
                            ai.floedb.floecat.catalog.rpc.ColumnIdentityHistoryTier
                                .COLUMN_IDENTITY_HISTORY_TIER_NONE)
                        .build()))
        .hasMessageContaining("state checksum does not match");
  }

  @Test
  void rejectsThePreviousFormatVersionAsUnsupported() {
    var map = ColumnIdentityMaps.toProto(state(7L));

    assertThatThrownBy(
            () -> ColumnIdentityMaps.restore(map.toBuilder().setFormatVersion(1).build()))
        .hasMessage("Unsupported column identity map format 1");
  }

  @Test
  void anUnanchoredSourceVersionIsPersistedAndIntegrityProtected() {
    var anchored = ColumnIdentityMaps.toProto(state(7L));
    var unanchored = ColumnIdentityMaps.withoutAnchoredSourceVersion(anchored);

    assertThat(ColumnIdentityMaps.restore(anchored).sourceVersionAnchored()).isTrue();
    assertThat(ColumnIdentityMaps.restore(unanchored).sourceVersionAnchored()).isFalse();
    assertThatThrownBy(
            () ->
                ColumnIdentityMaps.restore(
                    unanchored.toBuilder().setSourceVersionAnchored(true).build()))
        .hasMessageContaining("state checksum does not match");
  }

  @Test
  void anUnanchoredSourceCanReconcileAtTheSameLocalVersion() {
    SchemaIdentityState previous =
        ColumnIdentityMaps.restore(
            ColumnIdentityMaps.withoutAnchoredSourceVersion(ColumnIdentityMaps.toProto(state(0L))));
    SchemaNode retained =
        new SchemaNode(
            ColumnPath.ROOT.field("value"), 1, true, OptionalInt.empty(), Optional.empty());
    SchemaNode added =
        new SchemaNode(
            ColumnPath.ROOT.field("added"), 2, true, OptionalInt.empty(), Optional.empty());

    SchemaIdentityState reconciled =
        SchemaIdentityReconciler.reconcile(
            ResolvedSchema.of(List.of(retained, added)),
            0L,
            IdentityMode.STRUCTURED_PATH,
            Optional.of(previous),
            previous.highWaterMark(),
            PreferredCanonicalIds.NONE,
            HistoryTier.NONE);

    assertThat(reconciled.byPath(retained.path()).orElseThrow().canonicalId())
        .isEqualTo(previous.byPath(retained.path()).orElseThrow().canonicalId());
    assertThat(reconciled.byPath(added.path()).orElseThrow().canonicalId())
        .isGreaterThan(previous.highWaterMark());
  }

  @Test
  void historyTierChangesRecomputeTheProtectedState() {
    var original = ColumnIdentityMaps.toProto(state(7L));

    var changed = ColumnIdentityMaps.withHistoryTier(original, HistoryTier.NONE);

    assertThat(ColumnIdentityMaps.restore(changed).historyTier()).isEqualTo(HistoryTier.NONE);
    assertThat(changed.getStateChecksum()).isNotEqualTo(original.getStateChecksum());
    assertThat(ColumnIdentityMaps.withHistoryTier(changed, HistoryTier.NONE)).isSameAs(changed);
  }

  private static SchemaIdentityState state(long version) {
    SchemaNode node =
        new SchemaNode(
            ColumnPath.ROOT.field("value"), 1, true, OptionalInt.empty(), Optional.empty());
    return SchemaIdentityReconciler.reconcile(
        ResolvedSchema.of(List.of(node)),
        version,
        IdentityMode.STRUCTURED_PATH,
        Optional.empty(),
        0L,
        PreferredCanonicalIds.NONE,
        HistoryTier.VERSIONED_LOG);
  }
}
