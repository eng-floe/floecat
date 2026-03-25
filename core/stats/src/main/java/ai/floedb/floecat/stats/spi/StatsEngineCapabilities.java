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

import java.util.Objects;
import java.util.Set;

/** Capability descriptor for a stats capture engine. */
public record StatsEngineCapabilities(
    Set<String> connectors,
    Set<StatsTargetType> targetTypes,
    Set<StatsStatisticKind> statisticKinds,
    Set<StatsExecutionMode> executionModes,
    Set<StatsSamplingSupport> samplingSupport,
    boolean snapshotAware) {

  public StatsEngineCapabilities {
    connectors = Set.copyOf(Objects.requireNonNull(connectors, "connectors"));
    targetTypes = Set.copyOf(Objects.requireNonNull(targetTypes, "targetTypes"));
    statisticKinds = Set.copyOf(Objects.requireNonNull(statisticKinds, "statisticKinds"));
    executionModes = Set.copyOf(Objects.requireNonNull(executionModes, "executionModes"));
    samplingSupport = Set.copyOf(Objects.requireNonNull(samplingSupport, "samplingSupport"));
  }

  public boolean supports(StatsCaptureRequest request) {
    Objects.requireNonNull(request, "request");
    if (request.snapshotId() > 0L && !snapshotAware) {
      return false;
    }
    if (!targetTypes.contains(StatsTargetType.from(request.target()))) {
      return false;
    }
    if (!executionModes.contains(request.executionMode())) {
      return false;
    }
    if (!request.connectorType().isBlank()
        && !connectors.isEmpty()
        && !connectors.contains(request.connectorType())) {
      return false;
    }
    if (!request.requestedKinds().isEmpty()
        && !statisticKinds.containsAll(request.requestedKinds())) {
      return false;
    }
    if (request.samplingRequested() && samplingSupport.equals(Set.of(StatsSamplingSupport.NONE))) {
      return false;
    }
    return true;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private Set<String> connectors = Set.of();
    private Set<StatsTargetType> targetTypes = Set.of();
    private Set<StatsStatisticKind> statisticKinds = Set.of();
    private Set<StatsExecutionMode> executionModes = Set.of(StatsExecutionMode.SYNC);
    private Set<StatsSamplingSupport> samplingSupport = Set.of(StatsSamplingSupport.NONE);
    private boolean snapshotAware = true;

    private Builder() {}

    public Builder connectors(Set<String> connectors) {
      this.connectors = Set.copyOf(connectors);
      return this;
    }

    public Builder targetTypes(Set<StatsTargetType> targetTypes) {
      this.targetTypes = Set.copyOf(targetTypes);
      return this;
    }

    public Builder statisticKinds(Set<StatsStatisticKind> statisticKinds) {
      this.statisticKinds = Set.copyOf(statisticKinds);
      return this;
    }

    public Builder executionModes(Set<StatsExecutionMode> executionModes) {
      this.executionModes = Set.copyOf(executionModes);
      return this;
    }

    public Builder samplingSupport(Set<StatsSamplingSupport> samplingSupport) {
      this.samplingSupport = Set.copyOf(samplingSupport);
      return this;
    }

    public Builder snapshotAware(boolean snapshotAware) {
      this.snapshotAware = snapshotAware;
      return this;
    }

    public StatsEngineCapabilities build() {
      return new StatsEngineCapabilities(
          connectors, targetTypes, statisticKinds, executionModes, samplingSupport, snapshotAware);
    }
  }
}
