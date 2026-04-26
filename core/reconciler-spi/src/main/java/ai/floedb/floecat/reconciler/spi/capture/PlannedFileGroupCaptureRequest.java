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

package ai.floedb.floecat.reconciler.spi.capture;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Backend-facing request aligned to one planned reconcile file-group execution. */
public record PlannedFileGroupCaptureRequest(
    String planId,
    String groupId,
    ResourceId tableId,
    long snapshotId,
    List<String> plannedFilePaths,
    Set<String> statsColumns,
    Set<String> indexColumns,
    Set<FloecatConnector.StatsTargetKind> requestedStatsTargetKinds,
    boolean capturePageIndex) {
  public PlannedFileGroupCaptureRequest {
    planId = planId == null ? "" : planId.trim();
    groupId = groupId == null ? "" : groupId.trim();
    plannedFilePaths =
        plannedFilePaths == null
            ? List.of()
            : plannedFilePaths.stream()
                .filter(path -> path != null && !path.isBlank())
                .map(String::trim)
                .toList();
    statsColumns = normalizeSelectors(statsColumns);
    indexColumns = normalizeSelectors(indexColumns);
    requestedStatsTargetKinds =
        requestedStatsTargetKinds == null
            ? Set.of()
            : Set.copyOf(new LinkedHashSet<>(requestedStatsTargetKinds));
  }

  private static Set<String> normalizeSelectors(Set<String> selectors) {
    if (selectors == null) {
      return Set.of();
    }
    return Set.copyOf(
        selectors.stream()
            .filter(selector -> selector != null && !selector.isBlank())
            .map(String::trim)
            .collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new)));
  }

  public static PlannedFileGroupCaptureRequest of(
      String planId,
      String groupId,
      ResourceId tableId,
      long snapshotId,
      List<String> plannedFilePaths,
      Set<String> statsColumns,
      Set<String> indexColumns,
      Set<FloecatConnector.StatsTargetKind> requestedStatsTargetKinds,
      boolean capturePageIndex) {
    return new PlannedFileGroupCaptureRequest(
        planId,
        groupId,
        tableId,
        snapshotId,
        plannedFilePaths,
        statsColumns,
        indexColumns,
        requestedStatsTargetKinds,
        capturePageIndex);
  }

  public boolean requestsStats() {
    return !requestedStatsTargetKinds.isEmpty();
  }
}
