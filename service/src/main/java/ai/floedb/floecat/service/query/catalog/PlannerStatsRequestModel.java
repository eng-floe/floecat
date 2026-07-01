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

package ai.floedb.floecat.service.query.catalog;

import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.query.rpc.ConstraintServingOptions;
import ai.floedb.floecat.query.rpc.StatRole;
import ai.floedb.floecat.query.rpc.StatsServingOptions;
import ai.floedb.floecat.stats.spi.StatsResolutionResult;
import ai.floedb.floecat.stats.spi.StatsSyncOutcome;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

record PlannerStatsNormalizedRequest(
    List<PlannerStatsTableRequest> tables, long requestedTargets, long omittedByBudget) {}

/**
 * Normalized table-scoped work produced before serving.
 *
 * @param targets targets to serve normally
 * @param omittedTargets targets dropped by the count cap and emitted as OMITTED_BY_BUDGET
 * @param snapshotOverride explicit request snapshot_id, if present; overrides query pin lookup
 */
record PlannerStatsTableRequest(
    ResourceId tableId,
    List<PlannerStatsTargetNeed> targets,
    List<PlannerStatsTargetNeed> omittedTargets,
    OptionalLong snapshotOverride) {}

record PlannerStatsTargetNeed(
    StatsTarget target,
    List<PlannerStatsStatRequest> requestedStats,
    /** Pre-computed storage identity key; avoids repeated String allocation on hot path. */
    String storageId,
    int priority) {

  PlannerStatsTargetNeed {
    requestedStats = requestedStats == null ? List.of() : List.copyOf(requestedStats);
  }

  boolean requestsAnySketchPayload() {
    return requestedStats.stream().anyMatch(PlannerStatsStatRequest::requestsSketchPayload);
  }
}

record PlannerStatsStatRequest(
    StatRole role,
    String sketchType,
    int priority,
    int maxBytes,
    boolean omittedByPolicy,
    String omitReason) {
  PlannerStatsStatRequest {
    sketchType = sketchType == null ? "" : sketchType;
    omitReason = omitReason == null ? "" : omitReason;
  }

  String identityKey() {
    return role.getNumber() + "|" + sketchType;
  }

  boolean requestsSketchPayload() {
    return PlannerStatsRequestNormalizer.isSketchRole(role);
  }

  PlannerStatsStatRequest omittedByPolicy(String reason) {
    return new PlannerStatsStatRequest(role, sketchType, priority, maxBytes, true, reason);
  }
}

record PlannerStatsServingPolicy(
    Duration latencyBudget,
    long maxResponseBytes,
    long maxConstraintBytes,
    int maxScalarTargets,
    int maxSketchTargets,
    boolean allowSyncCapture,
    boolean staleOk) {
  private static final long DEFAULT_MAX_RESPONSE_BYTES = 8L * 1024L * 1024L;
  private static final long DEFAULT_MAX_CONSTRAINT_BYTES = 1024L * 1024L;
  private static final int DEFAULT_MAX_SCALAR_TARGETS = 10_000;
  private static final int DEFAULT_MAX_SKETCH_TARGETS = 256;
  private static final Duration DEFAULT_LATENCY_BUDGET = Duration.ofMillis(20);

  static PlannerStatsServingPolicy from(StatsServingOptions options, int serverMaxTargets) {
    StatsServingOptions opts = options == null ? StatsServingOptions.getDefaultInstance() : options;
    int scalarTargets =
        opts.getMaxScalarTargets() > 0
            ? Math.min(opts.getMaxScalarTargets(), serverMaxTargets)
            : Math.min(DEFAULT_MAX_SCALAR_TARGETS, serverMaxTargets);
    return new PlannerStatsServingPolicy(
        opts.getLatencyBudgetMs() > 0
            ? Duration.ofMillis(opts.getLatencyBudgetMs())
            : DEFAULT_LATENCY_BUDGET,
        opts.getMaxResponseBytes() > 0 ? opts.getMaxResponseBytes() : DEFAULT_MAX_RESPONSE_BYTES,
        opts.hasMaxConstraintBytes() ? opts.getMaxConstraintBytes() : DEFAULT_MAX_CONSTRAINT_BYTES,
        Math.max(0, scalarTargets),
        opts.getMaxSketchTargets() > 0 ? opts.getMaxSketchTargets() : DEFAULT_MAX_SKETCH_TARGETS,
        /* Optional bool: absent means server default true. */
        !opts.hasAllowSyncCapture() || opts.getAllowSyncCapture(),
        /* Optional bool: absent means server default true. */
        !opts.hasStaleOk() || opts.getStaleOk());
  }
}

record PlannerConstraintServingPolicy(long maxConstraintBytes) {
  private static final long DEFAULT_MAX_CONSTRAINT_BYTES = 1024L * 1024L;

  static PlannerConstraintServingPolicy from(ConstraintServingOptions options) {
    ConstraintServingOptions opts =
        options == null ? ConstraintServingOptions.getDefaultInstance() : options;
    return new PlannerConstraintServingPolicy(
        opts.hasMaxConstraintBytes() ? opts.getMaxConstraintBytes() : DEFAULT_MAX_CONSTRAINT_BYTES);
  }
}

record PlannerTargetStatsLookupResult(
    Optional<TargetStatsRecord> stats,
    StatsSyncOutcome outcome,
    String outcomeDetail,
    boolean stale) {
  PlannerTargetStatsLookupResult {
    stats = stats == null ? Optional.empty() : stats;
    outcome = outcome == null ? StatsSyncOutcome.SKIPPED : outcome;
    outcomeDetail = outcomeDetail == null ? "" : outcomeDetail;
  }

  static PlannerTargetStatsLookupResult hit(TargetStatsRecord stats) {
    return new PlannerTargetStatsLookupResult(Optional.of(stats), StatsSyncOutcome.HIT, "", false);
  }

  static PlannerTargetStatsLookupResult stale(TargetStatsRecord stats, String detail) {
    return new PlannerTargetStatsLookupResult(
        Optional.of(stats), StatsSyncOutcome.HIT, detail == null ? "stale_hit" : detail, true);
  }

  static PlannerTargetStatsLookupResult skipped(String detail) {
    return new PlannerTargetStatsLookupResult(
        Optional.empty(), StatsSyncOutcome.SKIPPED, detail, false);
  }

  static PlannerTargetStatsLookupResult fromResolution(StatsResolutionResult result) {
    if (result == null) {
      return skipped("missing_resolution_result");
    }
    return new PlannerTargetStatsLookupResult(
        result.stats(), result.outcome(), result.outcomeDetail(), result.stale());
  }

  PlannerTargetStatsLookupResult withStaleFallback(
      boolean staleOk, java.util.function.Supplier<Optional<TargetStatsRecord>> staleLookup) {
    if (stats.isPresent() || !staleOk) {
      return this;
    }
    return staleLookup.get().map(s -> stale(s, outcomeDetail)).orElse(this);
  }
}
