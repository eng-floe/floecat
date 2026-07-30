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

import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotSelection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ReconcilerServiceInternalsTest {

  @Test
  void captureOnlyCurrentPreservesConnectorSelectionAndRestrictsEligibilityToKnownSnapshots() {
    Set<Long> eligible =
        ReconcilerService.captureOnlyEnumerationTargetSnapshotIds(Set.of(17L, 42L, 91L), Set.of());

    FloecatConnector.SnapshotEnumerationOptions options =
        ReconcilerService.snapshotEnumerationOptions(
            ReconcileSnapshotSelection.current(), true, Set.of(), eligible);

    assertThat(options.selectionKind()).isEqualTo(FloecatConnector.SnapshotSelectionKind.CURRENT);
    assertThat(options.targetSnapshotIds()).containsExactlyInAnyOrder(17L, 42L, 91L);
    assertThat(options.selectionSnapshotIds()).isEmpty();
  }

  @Test
  void captureOnlyLatestNPreservesConnectorSelectionAndIntersectsScopedTargetsWithKnownSnapshots() {
    Set<Long> eligible =
        ReconcilerService.captureOnlyEnumerationTargetSnapshotIds(
            Set.of(17L, 42L, 91L), Set.of(42L, 73L, 91L));

    FloecatConnector.SnapshotEnumerationOptions options =
        ReconcilerService.snapshotEnumerationOptions(
            ReconcileSnapshotSelection.latestN(2), true, Set.of(), eligible);

    assertThat(options.selectionKind()).isEqualTo(FloecatConnector.SnapshotSelectionKind.LATEST_N);
    assertThat(options.latestN()).isEqualTo(2);
    assertThat(options.targetSnapshotIds()).containsExactlyInAnyOrder(42L, 91L);
    assertThat(options.selectionSnapshotIds()).isEmpty();
  }

  @Test
  void filterBundlesForModeKeepsOnlyKnownLocalSnapshotsForIncrementalCaptureOnly() {
    List<FloecatConnector.SnapshotBundle> bundles =
        List.of(bundle(10L, 0L, 1L), bundle(11L, 10L, 2L), bundle(12L, 11L, 3L));

    List<FloecatConnector.SnapshotBundle> filtered =
        QueuedReconcileWorkerSupport.filterBundlesForMode(
            bundles, false, Set.of(10L, 12L), noopProgress());

    assertThat(filtered)
        .extracting(FloecatConnector.SnapshotBundle::snapshotId)
        .containsExactly(10L, 12L);
  }

  @Test
  void filterBundlesForModeKeepsOnlyKnownLocalSnapshotsForCaptureOnly() {
    List<FloecatConnector.SnapshotBundle> bundles =
        List.of(bundle(10L, 0L, 1L), bundle(11L, 10L, 2L));

    List<FloecatConnector.SnapshotBundle> filtered =
        QueuedReconcileWorkerSupport.filterBundlesForMode(
            bundles, false, Set.of(10L, 12L), noopProgress());

    assertThat(filtered)
        .extracting(FloecatConnector.SnapshotBundle::snapshotId)
        .containsExactly(10L);
  }

  @Test
  void filterBundlesForModeSkipsSnapshotsMissingFromLocalMetadataDuringIncrementalCaptureOnly() {
    List<FloecatConnector.SnapshotBundle> bundles =
        List.of(bundle(10L, 0L, 1L), bundle(11L, 10L, 2L), bundle(12L, 11L, 3L));

    List<FloecatConnector.SnapshotBundle> filtered =
        QueuedReconcileWorkerSupport.filterBundlesForMode(
            bundles, false, Set.of(11L, 12L), noopProgress());

    assertThat(filtered)
        .extracting(FloecatConnector.SnapshotBundle::snapshotId)
        .containsExactly(11L, 12L);
  }

  private static FloecatConnector.SnapshotBundle bundle(
      long snapshotId, long parentId, long createdAtMs) {
    return new FloecatConnector.SnapshotBundle(
        snapshotId, parentId, createdAtMs, "", null, 0L, null, Map.of(), 0, null);
  }

  private static QueuedReconcileWorkerSupport.ProgressListener noopProgress() {
    return (ts, tc, vs, vc, e, sp, stp, m) -> {};
  }
}
