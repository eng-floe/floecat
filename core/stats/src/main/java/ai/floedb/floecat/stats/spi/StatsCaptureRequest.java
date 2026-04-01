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

package ai.floedb.floecat.stats.spi;

import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.common.rpc.ResourceId;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Canonical request envelope used by stats engine routing.
 *
 * <p>{@code snapshotId} accepts non-negative values. A value of {@code 0} is reserved for
 * unresolved/current-snapshot flows and may be resolved upstream before execution.
 *
 * <p>{@code columnSelectors} allows scoped capture requests to declare the set of relevant columns
 * in one request (for example destination column IDs or source column names).
 *
 * <p>{@code correlationId} is optional but should be populated by caller-facing services to keep
 * engine routing logs traceable.
 *
 * <p>{@code columnSelectors} carries optional connector-native selector hints (for example column
 * names) for scoped capture in reconciliation/control-plane workflows.
 */
public record StatsCaptureRequest(
    ResourceId tableId,
    long snapshotId,
    StatsTarget target,
    Set<String> columnSelectors,
    Set<StatsKind> requestedKinds,
    StatsExecutionMode executionMode,
    String connectorType,
    String correlationId,
    boolean samplingRequested,
    Optional<Duration> latencyBudget) {

  public StatsCaptureRequest {
    tableId = Objects.requireNonNull(tableId, "tableId");
    if (snapshotId < 0L) {
      throw new IllegalArgumentException("snapshotId must be non-negative");
    }
    target = Objects.requireNonNull(target, "target");
    columnSelectors = Set.copyOf(Objects.requireNonNull(columnSelectors, "columnSelectors"));
    requestedKinds = Set.copyOf(Objects.requireNonNull(requestedKinds, "requestedKinds"));
    executionMode = Objects.requireNonNull(executionMode, "executionMode");
    connectorType = connectorType == null ? "" : connectorType.trim().toLowerCase();
    correlationId = correlationId == null ? "" : correlationId.trim();
    latencyBudget = Objects.requireNonNullElse(latencyBudget, Optional.empty());
    if (latencyBudget.isPresent()
        && (latencyBudget.get().isZero() || latencyBudget.get().isNegative())) {
      throw new IllegalArgumentException("latencyBudget must be positive when present");
    }
  }

  /** Convenience constructor that defaults {@code latencyBudget} to empty. */
  public StatsCaptureRequest(
      ResourceId tableId,
      long snapshotId,
      StatsTarget target,
      Set<String> columnSelectors,
      Set<StatsKind> requestedKinds,
      StatsExecutionMode executionMode,
      String connectorType,
      String correlationId,
      boolean samplingRequested) {
    this(
        tableId,
        snapshotId,
        target,
        columnSelectors,
        requestedKinds,
        executionMode,
        connectorType,
        correlationId,
        samplingRequested,
        Optional.empty());
  }
}
