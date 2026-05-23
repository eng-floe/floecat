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

import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.StatsPriorityClass;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;

/**
 * Policy SPI for assigning a priority class, score, and lane to a reconcile job at enqueue time.
 *
 * <p>Implementations must not assign {@link StatsPriorityClass#P0_SYNC}: that class is reserved for
 * the stats orchestrator's sync-capture path and is never delegated to a policy bean. {@link
 * SchedulerPolicyRegistry} validates this invariant at startup.
 *
 * <p>Implementations must be annotated with {@link SchedulerProfile} so the registry can resolve
 * them by name.
 *
 * <p>All methods must be cheap to call — no blocking I/O is permitted.
 */
public interface SchedulerPriorityPolicy {

  /**
   * Assigns a priority class, score, and lane to a stats capture request.
   *
   * <p>Must never return a {@link PriorityAssignment} whose {@link
   * PriorityAssignment#priorityClass} is {@link StatsPriorityClass#P0_SYNC}.
   *
   * @param request the stats capture request being enqueued
   * @param context read-only view of current scheduler state
   * @return the priority assignment to apply at enqueue
   */
  PriorityAssignment assign(StatsCaptureRequest request, SchedulerContext context);

  /**
   * Assigns a priority class and score to a reconcile job that originates from the reconciler
   * itself (e.g., {@code PLAN_SNAPSHOT} or {@code EXEC_FILE_GROUP}), rather than from the stats
   * orchestrator.
   *
   * <p>Default: assigns {@link StatsPriorityClass#P1_FRESHNESS} for new snapshots, {@link
   * StatsPriorityClass#P3_BACKGROUND} otherwise. Score is 0 in both cases. Lane is supplied by the
   * caller as {@code laneKey} (typically {@code accountId:tableId}).
   *
   * <p>Must never return a {@link PriorityAssignment} whose {@link
   * PriorityAssignment#priorityClass} is {@link StatsPriorityClass#P0_SYNC}.
   */
  default PriorityAssignment assignForReconcileJob(
      ReconcileJobKind kind,
      String laneKey,
      long snapshotId,
      boolean isNewSnapshot,
      SchedulerContext context) {
    StatsPriorityClass cls =
        isNewSnapshot ? StatsPriorityClass.P1_FRESHNESS : StatsPriorityClass.P3_BACKGROUND;
    return new PriorityAssignment(cls, 0L, laneKey);
  }

  /**
   * The result of a priority assignment: the class that determines dispatch order, a score that
   * breaks ties within the class (higher score = dispatched first), and the lane key used for WRR
   * fairness.
   *
   * @param priorityClass dispatch priority; must not be {@link StatsPriorityClass#P0_SYNC} when
   *     returned from a policy bean
   * @param score tie-breaker within the class; higher values are dispatched first; must be ≥ 0
   * @param laneKey WRR fairness bucket (e.g. {@code "accountId:tableId"}); blank means no WRR
   *     bucketing
   */
  record PriorityAssignment(StatsPriorityClass priorityClass, long score, String laneKey) {
    public PriorityAssignment {
      if (priorityClass == null) {
        priorityClass = StatsPriorityClass.P3_BACKGROUND;
      }
      score = Math.max(0L, score);
      laneKey = laneKey == null ? "" : laneKey.trim();
    }
  }
}
