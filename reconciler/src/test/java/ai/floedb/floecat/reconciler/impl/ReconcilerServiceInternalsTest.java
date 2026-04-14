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

package ai.floedb.floecat.reconciler.impl;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.connector.rpc.SourceSelector;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ReconcilerServiceInternalsTest {

  @Test
  void effectiveSelectorsPreferScopeColumnsOverConnectorSourceColumns() {
    ReconcileScope scope = ReconcileScope.of(List.of(List.of("ns")), "tbl", List.of("barf", "#2"));
    SourceSelector source = SourceSelector.newBuilder().addColumns("i").addColumns("#1").build();

    Set<String> selectors = ReconcilerService.effectiveSelectors(scope, source);

    assertThat(selectors).containsExactlyInAnyOrder("barf", "#2");
  }

  @Test
  void effectiveSelectorsFallbackToConnectorSourceColumnsWhenScopeHasNoColumns() {
    ReconcileScope scope = ReconcileScope.of(List.of(List.of("ns")), "tbl", List.of());
    SourceSelector source = SourceSelector.newBuilder().addColumns("i").addColumns("#1").build();

    Set<String> selectors = ReconcilerService.effectiveSelectors(scope, source);

    assertThat(selectors).containsExactlyInAnyOrder("i", "#1");
  }

  @Test
  void filterBundlesForModeSkipsAlreadyIngestedSnapshotsForIncremental() {
    List<FloecatConnector.SnapshotBundle> bundles =
        List.of(bundle(10L, 0L, 1L), bundle(11L, 10L, 2L), bundle(12L, 11L, 3L));

    List<FloecatConnector.SnapshotBundle> filtered =
        ReconcilerService.filterBundlesForMode(
            bundles, false, false, Set.of(10L, 12L), noopProgress());

    assertThat(filtered)
        .extracting(FloecatConnector.SnapshotBundle::snapshotId)
        .containsExactly(11L);
  }

  @Test
  void filterBundlesForModeKeepsAllSnapshotsForFullRescan() {
    List<FloecatConnector.SnapshotBundle> bundles =
        List.of(bundle(10L, 0L, 1L), bundle(11L, 10L, 2L));

    List<FloecatConnector.SnapshotBundle> filtered =
        ReconcilerService.filterBundlesForMode(
            bundles, true, false, Set.of(10L, 12L), noopProgress());

    assertThat(filtered)
        .extracting(FloecatConnector.SnapshotBundle::snapshotId)
        .containsExactly(10L, 11L);
  }

  @Test
  void filterBundlesForModeSkipsKnownSnapshotsWhenStatsAreIncluded() {
    List<FloecatConnector.SnapshotBundle> bundles =
        List.of(bundle(10L, 0L, 1L), bundle(11L, 10L, 2L));

    List<FloecatConnector.SnapshotBundle> filtered =
        ReconcilerService.filterBundlesForMode(bundles, false, true, Set.of(10L), noopProgress());

    assertThat(filtered)
        .extracting(FloecatConnector.SnapshotBundle::snapshotId)
        .containsExactly(10L, 11L);
  }

  @Test
  void filterBundlesForModeAppliesIncrementalPruningUsingExistingSnapshotIds() {
    List<FloecatConnector.SnapshotBundle> bundles =
        List.of(bundle(10L, 0L, 1L), bundle(11L, 10L, 2L), bundle(12L, 11L, 3L));

    List<FloecatConnector.SnapshotBundle> filtered =
        ReconcilerService.filterBundlesForMode(
            bundles, false, false, Set.of(11L, 12L), noopProgress());

    assertThat(filtered)
        .extracting(FloecatConnector.SnapshotBundle::snapshotId)
        .containsExactly(10L);
  }

  @Test
  void knownSnapshotIdsForEnumerationKeepsKnownSnapshotsForMetadataOnlyRuns() {
    Set<Long> metadataOnly =
        ReconcilerService.knownSnapshotIdsForEnumeration(
            false, false, Set.of(10L, 11L), id -> false);
    Set<Long> fullRescan =
        ReconcilerService.knownSnapshotIdsForEnumeration(
            true, false, Set.of(10L, 11L), id -> false);

    assertThat(metadataOnly).containsExactlyInAnyOrder(10L, 11L);
    assertThat(fullRescan).isEmpty();
  }

  @Test
  void knownSnapshotIdsForEnumerationKeepsKnownSnapshotsWithoutStatsEnumerable() {
    Set<Long> statsOnly =
        ReconcilerService.knownSnapshotIdsForEnumeration(
            false, true, Set.of(10L, 11L), id -> false);

    assertThat(statsOnly).isEmpty();
  }

  @Test
  void knownSnapshotIdsForEnumerationPrunesOnlyKnownSnapshotsWithCapturedStats() {
    Set<Long> statsOnly =
        ReconcilerService.knownSnapshotIdsForEnumeration(
            false, true, Set.of(10L, 11L), id -> id == 10L);

    assertThat(statsOnly).containsExactly(10L);
  }

  @Test
  void tableChangedIsFalseWhenNoBundlesRemain() {
    assertThat(ReconcilerService.tableChanged(List.of())).isFalse();
  }

  @Test
  void tableChangedIsTrueWhenBundlesRemain() {
    assertThat(ReconcilerService.tableChanged(List.of(bundle(11L, 10L, 2L)))).isTrue();
  }

  private static FloecatConnector.SnapshotBundle bundle(
      long snapshotId, long parentId, long createdAtMs) {
    return new FloecatConnector.SnapshotBundle(
        snapshotId, parentId, createdAtMs, List.of(), "", null, 0L, null, Map.of(), 0, Map.of());
  }

  private static ReconcilerService.ProgressListener noopProgress() {
    return (s, c, e, sp, stp, m) -> {};
  }
}
