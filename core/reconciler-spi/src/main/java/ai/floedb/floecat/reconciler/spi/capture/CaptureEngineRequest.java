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
import ai.floedb.floecat.stats.spi.JobCostHint;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Source-aware capture request used by pluggable capture engines.
 *
 * <p>The {@link #maxCost()} field limits which stat kinds an engine may attempt within this
 * request. Engines that would exceed this budget must return {@link CaptureEngineResult#empty()}
 * rather than blocking; the registry will fall through to the next eligible engine or return an
 * empty result. See {@link CaptureEngineRegistry} and {@link CaptureEngine#estimatedCost}.
 */
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
    FloecatConnector.ColumnSelectorPolicy columnSelectorPolicy,
    Set<FloecatConnector.StatsTargetKind> requestedStatsTargetKinds,
    boolean capturePageIndex,
    Optional<String> authorizationToken,
    /** Upper bound on the cost this request permits an engine to incur. Default: EXPENSIVE. */
    JobCostHint maxCost) {
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
    columnSelectorPolicy =
        columnSelectorPolicy == null
            ? FloecatConnector.ColumnSelectorPolicy.defaults()
            : columnSelectorPolicy;
    requestedStatsTargetKinds =
        requestedStatsTargetKinds == null
            ? Set.of()
            : Set.copyOf(new LinkedHashSet<>(requestedStatsTargetKinds));
    authorizationToken =
        authorizationToken == null
            ? Optional.empty()
            : authorizationToken.map(String::trim).filter(token -> !token.isBlank());
    maxCost = maxCost == null ? JobCostHint.EXPENSIVE : maxCost;
  }

  /**
   * Backward-compatible constructor that sets {@code maxCost = EXPENSIVE} (no restriction).
   *
   * <p>Existing callers that do not need to enforce a cost budget may use this 13-arg form; new
   * callers that need cost enforcement should use the full 14-arg form.
   */
  public CaptureEngineRequest(
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
      boolean capturePageIndex,
      Optional<String> authorizationToken) {
    this(
        sourceConnector,
        sourceNamespace,
        sourceTable,
        tableId,
        snapshotId,
        planId,
        groupId,
        plannedFilePaths,
        statsColumns,
        indexColumns,
        FloecatConnector.ColumnSelectorPolicy.defaults(),
        requestedStatsTargetKinds,
        capturePageIndex,
        authorizationToken,
        JobCostHint.EXPENSIVE);
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
