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

package ai.floedb.floecat.service.statistics.scheduler;

import ai.floedb.floecat.reconciler.jobs.CoverageLevel;
import ai.floedb.floecat.reconciler.jobs.SchedulerHealthBand;
import ai.floedb.floecat.reconciler.jobs.StatsPriorityClass;
import java.util.Map;
import java.util.OptionalLong;

/**
 * Read-only view of the scheduler state, passed to all policy calls.
 *
 * <p>Implementations must be cheap to call — no blocking I/O is permitted. All returned values
 * represent a snapshot of state at or near the time the policy method is invoked. Callers must not
 * assume the returned values are perfectly consistent with each other.
 *
 * <p>This interface is injected into policy implementations via {@link SchedulerPolicyRegistry}.
 * Tests supply lightweight in-memory implementations.
 */
public interface SchedulerContext {

  /**
   * Current health band of the scheduler, as determined by the most recent {@code queueStats()}.
   */
  SchedulerHealthBand currentBand();

  /**
   * Number of jobs currently waiting in the ready queue, broken down by priority class.
   *
   * <p>Keys are present for all {@link StatsPriorityClass} values; absent keys mean 0.
   */
  Map<StatsPriorityClass, Long> queueDepthByClass();

  /**
   * Epoch-millisecond timestamp of the last successful stats capture for the given table, or empty
   * if no successful capture has been recorded.
   *
   * <p>Used by the age scoring factor: jobs targeting tables that have not been successfully
   * captured recently receive a higher urgency score.
   */
  OptionalLong lastSuccessfulCaptureMs(String tableId);

  /**
   * Current coverage completeness for the given (table, snapshot) pair.
   *
   * <p>Used by the coverage scoring factor: {@link CoverageLevel#NONE} produces the highest urgency
   * contribution; {@link CoverageLevel#FULL} produces zero contribution.
   *
   * <p>When the snapshot ID is unknown or coverage cannot be determined, implementations should
   * return {@link CoverageLevel#NONE} to bias toward capturing rather than skipping.
   */
  CoverageLevel coverageLevel(String tableId, long snapshotId);

  /**
   * Estimated number of changed rows between the previous snapshot and the given snapshot for a
   * table, or empty if the delta cannot be determined.
   *
   * <p>Used by the delta scoring factor: a large row delta (relative to total rows) produces a
   * higher urgency score. When empty, the scorer treats the delta as unknown and applies a
   * conservative mid-range contribution.
   */
  OptionalLong snapshotDeltaRows(String tableId, long snapshotId);

  /**
   * Number of planner stats-resolution hits for the table in the current + previous demand window.
   * A hit is recorded each time the planner calls {@code StatsOrchestrator.resolve()} or {@code
   * resolveBatch()} for this table.
   *
   * <p>Used by the demand multiplier: heavily-requested tables receive a higher multiplier, causing
   * their jobs to sort first within a priority class.
   *
   * <p>The {@code tableId} parameter must be in compound {@code accountId:tableId} form.
   *
   * <p>Default implementation returns 0 (neutral — no demand multiplier applied), suitable for
   * implementations that do not track demand.
   */
  default long recentPlannerRequestCount(String tableId) {
    return 0L;
  }

  /**
   * Number of planner demand hits for a specific column selector in the current + previous demand
   * window. The {@code normalizedColumnSelector} must already be trimmed and lowercased.
   *
   * <p>Used by the demand multiplier when column-scoped stats jobs are being scored: columns that
   * are actively requested by the planner receive a higher multiplier.
   *
   * <p>The {@code tableId} parameter must be in compound {@code accountId:tableId} form.
   *
   * <p>Default implementation returns 0.
   */
  default long recentColumnRequestCount(String tableId, String normalizedColumnSelector) {
    return 0L;
  }
}
