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

import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Capability descriptor for a stats capture engine.
 *
 * <p>Important empty-set semantics:
 *
 * <p>- Empty {@code connectors} means no connector constraint (matches all connectors).
 *
 * <p>- Empty {@code targetTypes} means no target-type constraint (matches all target types).
 *
 * <p>- Empty {@code statisticKinds} means no kind constraint (matches all statistic kinds).
 *
 * <p>Engine authors should set {@code targetTypes(...)} explicitly unless match-all behavior is
 * intended.
 */
public record StatsCapabilities(
    Set<String> connectors,
    Set<StatsTargetType> targetTypes,
    Map<StatsTargetType, Set<StatsKind>> statisticKindsByTarget,
    Set<StatsExecutionMode> executionModes,
    Set<StatsSamplingSupport> samplingSupport,
    boolean snapshotAware) {

  public StatsCapabilities {
    connectors = normalizeConnectors(connectors);
    targetTypes = Set.copyOf(Objects.requireNonNull(targetTypes, "targetTypes"));
    statisticKindsByTarget = normalizeStatisticKindsByTarget(statisticKindsByTarget);
    if (!targetTypes.containsAll(statisticKindsByTarget.keySet())) {
      throw new IllegalArgumentException(
          "statisticKindsByTarget contains target types not declared in targetTypes");
    }
    if (!statisticKindsByTarget.keySet().containsAll(targetTypes)) {
      throw new IllegalArgumentException(
          "statisticKindsByTarget must declare supported kinds for each targetType");
    }
    executionModes = Set.copyOf(Objects.requireNonNull(executionModes, "executionModes"));
    samplingSupport = Set.copyOf(Objects.requireNonNull(samplingSupport, "samplingSupport"));
  }

  /**
   * Returns whether these capabilities support the given request.
   *
   * <p>Empty sets in {@code connectors}, {@code targetTypes}, and {@code statisticKinds} are
   * treated as no constraint for that dimension (match-all).
   */
  public boolean supports(StatsCaptureRequest request) {
    Objects.requireNonNull(request, "request");
    StatsTargetType targetType = StatsTargetType.from(request.target());
    if (request.snapshotId() > 0L && !snapshotAware) {
      return false;
    }
    if (!targetTypes.isEmpty() && !targetTypes.contains(targetType)) {
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
        && !supportedKindsFor(targetType).containsAll(request.requestedKinds())) {
      return false;
    }
    if (request.samplingRequested() && samplingSupport.equals(Set.of(StatsSamplingSupport.NONE))) {
      return false;
    }
    return true;
  }

  public Set<StatsKind> supportedKindsFor(StatsTargetType targetType) {
    return statisticKindsByTarget.getOrDefault(Objects.requireNonNull(targetType), Set.of());
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Matches all connectors, target types, and statistic kinds with default sync/no-sampling. */
  public static StatsCapabilities matchAll() {
    return builder().build();
  }

  /**
   * Matches all target types and statistic kinds for a specific connector type.
   *
   * <p>{@code connectorType} is normalized to lower-case to match request normalization.
   */
  public static StatsCapabilities forConnector(String connectorType) {
    String normalized = connectorType == null ? "" : connectorType.trim().toLowerCase();
    if (normalized.isBlank()) {
      throw new IllegalArgumentException("connectorType must not be blank");
    }
    return builder().connectors(Set.of(normalized)).build();
  }

  /**
   * Builder for engine capabilities.
   *
   * <p>Defaults:
   *
   * <ul>
   *   <li>{@code connectors}: empty set (all connectors)
   *   <li>{@code targetTypes}: empty set (all target types)
   *   <li>{@code statisticKinds}: empty set (all statistic kinds)
   *   <li>{@code executionModes}: {@code [SYNC]}
   *   <li>{@code samplingSupport}: {@code [NONE]}
   *   <li>{@code snapshotAware}: {@code true}
   * </ul>
   */
  public static final class Builder {
    private Set<String> connectors = Set.of();
    private Set<StatsTargetType> targetTypes = Set.of();
    private Map<StatsTargetType, Set<StatsKind>> statisticKindsByTarget = Map.of();
    private Set<StatsExecutionMode> executionModes = Set.of(StatsExecutionMode.SYNC);
    // Default to NONE unless explicitly declared otherwise.
    private Set<StatsSamplingSupport> samplingSupport = Set.of(StatsSamplingSupport.NONE);
    private boolean snapshotAware = true;

    private Builder() {}

    /** Limits matching to the provided connector types (empty set means all connectors). */
    public Builder connectors(Set<String> connectors) {
      this.connectors = normalizeConnectors(connectors);
      return this;
    }

    /** Limits matching to the provided target types (empty set means all target types). */
    public Builder targetTypes(Set<StatsTargetType> targetTypes) {
      this.targetTypes = Set.copyOf(targetTypes);
      return this;
    }

    /** Limits matching to the provided statistic kinds (empty set means all kinds). */
    public Builder statisticKindsByTarget(
        Map<StatsTargetType, Set<StatsKind>> statisticKindsByTarget) {
      this.statisticKindsByTarget = normalizeStatisticKindsByTarget(statisticKindsByTarget);
      return this;
    }

    /** Declares execution modes accepted by the engine (default is {@code SYNC} only). */
    public Builder executionModes(Set<StatsExecutionMode> executionModes) {
      this.executionModes = Set.copyOf(executionModes);
      return this;
    }

    /** Declares sampling features accepted by the engine (default is {@code NONE}). */
    public Builder samplingSupport(Set<StatsSamplingSupport> samplingSupport) {
      this.samplingSupport = Set.copyOf(samplingSupport);
      return this;
    }

    /** Declares whether snapshot-scoped requests are supported by this engine. */
    public Builder snapshotAware(boolean snapshotAware) {
      this.snapshotAware = snapshotAware;
      return this;
    }

    public StatsCapabilities build() {
      return new StatsCapabilities(
          connectors,
          targetTypes,
          statisticKindsByTarget,
          executionModes,
          samplingSupport,
          snapshotAware);
    }
  }

  private static Set<String> normalizeConnectors(Set<String> connectors) {
    return Set.copyOf(
        Objects.requireNonNull(connectors, "connectors").stream()
            .map(Objects::requireNonNull)
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .map(s -> s.toLowerCase(Locale.ROOT))
            .collect(Collectors.toSet()));
  }

  private static Map<StatsTargetType, Set<StatsKind>> normalizeStatisticKindsByTarget(
      Map<StatsTargetType, Set<StatsKind>> statisticKindsByTarget) {
    return Map.copyOf(
        Objects.requireNonNull(statisticKindsByTarget, "statisticKindsByTarget").entrySet().stream()
            .collect(
                Collectors.toMap(
                    e -> Objects.requireNonNull(e.getKey(), "targetType"),
                    e -> Set.copyOf(Objects.requireNonNull(e.getValue(), "statisticKinds")))));
  }
}
