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
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Source-aware capture request used by pluggable capture engines. */
public record CaptureEngineRequest(
    Connector sourceConnector,
    String sourceNamespace,
    String sourceTable,
    ResourceId tableId,
    long snapshotId,
    String planId,
    String groupId,
    List<String> plannedFilePaths,
    Set<String> statsColumns,
    Set<String> indexColumns,
    Set<FloecatConnector.StatsTargetKind> requestedStatsTargetKinds,
    boolean capturePageIndex) {
  public CaptureEngineRequest {
    sourceNamespace = sourceNamespace == null ? "" : sourceNamespace.trim();
    sourceTable = sourceTable == null ? "" : sourceTable.trim();
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
    return selectors == null
        ? Set.of()
        : Set.copyOf(
            selectors.stream()
                .filter(column -> column != null && !column.isBlank())
                .map(String::trim)
                .collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new)));
  }

  public boolean requestsStats() {
    return !requestedStatsTargetKinds.isEmpty();
  }

  public boolean hasOutputs() {
    return requestsStats() || capturePageIndex;
  }

  public boolean hasColumnSelectors() {
    return !statsColumns.isEmpty() || !indexColumns.isEmpty();
  }

  public boolean hasSourceContext() {
    if (sourceConnector == null
        || tableId == null
        || sourceNamespace.isBlank()
        || sourceTable.isBlank()
        || snapshotId < 0) {
      return false;
    }
    return requestsStats() || !plannedFilePaths.isEmpty();
  }

  public boolean isFileGroupScoped() {
    return tableId != null && snapshotId >= 0 && !plannedFilePaths.isEmpty();
  }

  public boolean expectsCompleteFileGroupOutputs() {
    return hasOutputs() && isFileGroupScoped();
  }
}
