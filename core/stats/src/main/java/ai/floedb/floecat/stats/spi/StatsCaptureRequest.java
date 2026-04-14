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
 * <p>{@code snapshotId} accepts non-negative values. A value of {@code 0} is a valid concrete
 * snapshot identifier for connectors that use zero-based snapshot/version numbering.
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

  public static Builder builder(ResourceId tableId, long snapshotId, StatsTarget target) {
    return new Builder(tableId, snapshotId, target);
  }

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

  /** Builder to avoid fragile positional argument calls. */
  public static final class Builder {
    private final ResourceId tableId;
    private final long snapshotId;
    private final StatsTarget target;
    private Set<String> columnSelectors = Set.of();
    private Set<StatsKind> requestedKinds = Set.of();
    private StatsExecutionMode executionMode = StatsExecutionMode.SYNC;
    private String connectorType = "";
    private String correlationId = "";
    private boolean samplingRequested;
    private Optional<Duration> latencyBudget = Optional.empty();

    private Builder(ResourceId tableId, long snapshotId, StatsTarget target) {
      this.tableId = tableId;
      this.snapshotId = snapshotId;
      this.target = target;
    }

    public Builder columnSelectors(Set<String> columnSelectors) {
      this.columnSelectors = columnSelectors == null ? Set.of() : Set.copyOf(columnSelectors);
      return this;
    }

    public Builder requestedKinds(Set<StatsKind> requestedKinds) {
      this.requestedKinds = requestedKinds == null ? Set.of() : Set.copyOf(requestedKinds);
      return this;
    }

    public Builder executionMode(StatsExecutionMode executionMode) {
      this.executionMode = executionMode;
      return this;
    }

    public Builder connectorType(String connectorType) {
      this.connectorType = connectorType == null ? "" : connectorType;
      return this;
    }

    public Builder correlationId(String correlationId) {
      this.correlationId = correlationId == null ? "" : correlationId;
      return this;
    }

    public Builder samplingRequested(boolean samplingRequested) {
      this.samplingRequested = samplingRequested;
      return this;
    }

    public Builder latencyBudget(Optional<Duration> latencyBudget) {
      this.latencyBudget = latencyBudget == null ? Optional.empty() : latencyBudget;
      return this;
    }

    public StatsCaptureRequest build() {
      return new StatsCaptureRequest(
          tableId,
          snapshotId,
          target,
          columnSelectors,
          requestedKinds,
          executionMode,
          connectorType,
          correlationId,
          samplingRequested,
          latencyBudget);
    }
  }
}
