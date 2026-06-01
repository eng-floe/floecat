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

package ai.floedb.floecat.reconciler.jobs.impl;

import ai.floedb.floecat.reconciler.jobs.SchedulerHealthBand;
import ai.floedb.floecat.reconciler.jobs.StatsPriorityClass;

/**
 * Pure-static scheduling utilities shared between {@link InMemoryReconcileJobStore} and the durable
 * job store.
 *
 * <h2>Admission vs. profile deferral</h2>
 *
 * <p>{@link #admissionDeferMs} implements a store-level deferral layer on top of the profile's
 * deterministic {@link ai.floedb.floecat.stats.spi.JobCostHint#MEDIUM} decision. In particular:
 *
 * <ul>
 *   <li>Under {@link SchedulerHealthBand#YELLOW}, P3 jobs are deferred with 50% probability
 *       (probabilistic jitter); the profile's admission policy admits them deterministically.
 *   <li>Under {@link SchedulerHealthBand#ORANGE} and {@link SchedulerHealthBand#RED}, the profile
 *       defers P3 deterministically; the store adds the concrete delay here.
 *   <li>P0 and P1 are never deferred at this layer.
 * </ul>
 *
 * <p>These two layers are intentional — the profile layer is deterministic (for external
 * pluggability and testability); the store layer adds jitter and concrete delay values that are
 * implementation details of the queue store. Do not consolidate them without updating both sides.
 */
public final class SchedulerStoreHelpers {

  /** Concrete deferral delay applied when a job's admission is deferred. */
  public static final long DEFER_DELAY_MS = 5_000L;

  /** P3 jobs older than this are eligible for starvation-aging promotion to P2. */
  public static final long P3_AGING_THRESHOLD_MS = 300_000L; // 5 min

  /** P2 jobs older than this are eligible for starvation-aging promotion to P1. */
  public static final long P2_AGING_THRESHOLD_MS = 120_000L; // 2 min

  /** A promoted job is not re-promoted within this window. */
  public static final long AGING_COOLDOWN_MS = 60_000L; // 1 min

  /**
   * Tag written to {@link ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy#attributes}
   * when the orchestrator-layer admission policy returns DEFER.
   */
  public static final String ATTR_POLICY_DEFERRED = "policy_deferred";

  private SchedulerStoreHelpers() {}

  /**
   * Returns the number of milliseconds to delay a job's {@code nextAttemptAtMs} for admission
   * control purposes, or {@code 0} if the job should be admitted immediately.
   *
   * <p>P0 and P1 are always admitted (returns 0). P3 under YELLOW is admitted with 50% probability
   * (probabilistic jitter). P2 under RED and P3 under ORANGE/RED are always deferred.
   */
  public static long admissionDeferMs(StatsPriorityClass cls, SchedulerHealthBand band) {
    return admissionDeferMs(cls, band, false);
  }

  /**
   * Variant that respects a prior orchestrator-layer DEFER vote stored in the job's attributes.
   * When {@code policyDeferred} is {@code true}, P3 is deferred in YELLOW regardless of the
   * probabilistic gate.
   */
  public static long admissionDeferMs(
      StatsPriorityClass cls, SchedulerHealthBand band, boolean policyDeferred) {
    if (cls == null
        || cls == StatsPriorityClass.P0_SYNC
        || cls == StatsPriorityClass.P1_FRESHNESS) {
      return 0L; // P0 and P1 are never deferred at the store layer
    }
    return switch (cls) {
      case P2_REPAIR -> band == SchedulerHealthBand.RED ? DEFER_DELAY_MS : 0L;
      case P3_BACKGROUND ->
          switch (band) {
            case GREEN -> 0L;
            case YELLOW -> {
              // Probabilistic 50% deferral; orchestrator DEFER vote forces deferral.
              if (policyDeferred) {
                yield DEFER_DELAY_MS;
              }
              yield (System.nanoTime() & 1L) == 0L ? DEFER_DELAY_MS : 0L;
            }
            case ORANGE, RED -> DEFER_DELAY_MS;
          };
      default -> 0L;
    };
  }

  /**
   * Returns the starvation-aging threshold in milliseconds for the given priority class. A job that
   * has been waiting longer than this threshold is eligible for promotion to the next more urgent
   * class.
   *
   * <p>P0 and P1 are never promoted (returns {@link Long#MAX_VALUE}).
   */
  public static long agingThresholdMs(StatsPriorityClass cls) {
    if (cls == null) {
      return Long.MAX_VALUE;
    }
    return switch (cls) {
      case P3_BACKGROUND -> P3_AGING_THRESHOLD_MS;
      case P2_REPAIR -> P2_AGING_THRESHOLD_MS;
      default -> Long.MAX_VALUE; // P0 and P1 do not age
    };
  }
}
