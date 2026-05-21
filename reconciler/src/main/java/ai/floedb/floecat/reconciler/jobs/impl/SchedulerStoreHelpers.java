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
import java.util.concurrent.ThreadLocalRandom;

/**
 * Pure-static scheduler helpers shared by all {@link
 * ai.floedb.floecat.reconciler.jobs.ReconcileJobStore} implementations.
 *
 * <p><b>Admission split contract:</b> {@link #admissionDeferMs} handles the full deferral matrix
 * including probabilistic 50% deferral for P3 under {@link SchedulerHealthBand#YELLOW}. The
 * profile-layer {@code SchedulerAdmissionPolicy.decide()} defers P3 only at ORANGE and above — the
 * store adds YELLOW jitter on top. Neither side must be changed without updating the other.
 *
 * <p>All methods are package-private. No instances are created.
 */
public final class SchedulerStoreHelpers {

  // ---------------------------------------------------------------------------
  // Shared constants — referenced by stores and tests
  // ---------------------------------------------------------------------------

  /** How long to delay a deferred job before it becomes eligible again (ms). */
  public static final long DEFER_DELAY_MS = 5_000L;

  /** Aging threshold for P3_BACKGROUND jobs (ms). Jobs older than this are counted as promoted. */
  public static final long P3_AGING_THRESHOLD_MS = 300_000L; // 5 min

  /** Aging threshold for P2_REPAIR jobs (ms). */
  public static final long P2_AGING_THRESHOLD_MS = 120_000L; // 2 min

  /**
   * Aging threshold for P1_FRESHNESS jobs (ms). Effectively disabled — P1 jobs are already high
   * priority and should not be "promoted" further.
   */
  public static final long P1_AGING_THRESHOLD_MS = Long.MAX_VALUE;

  /** Cooldown window (ms) during which a promoted job is not counted again. */
  public static final long AGING_COOLDOWN_MS = 60_000L;

  private SchedulerStoreHelpers() {}

  /**
   * Returns the admission deferral delay in milliseconds for a job of the given priority class
   * under the given health band. A return value of {@code 0} means admit immediately.
   *
   * <p>P0_SYNC and P1_FRESHNESS are always admitted (return {@code 0}) regardless of band. P2 is
   * deferred only under RED. P3 is deferred under ORANGE and RED, and deferred with 50% probability
   * under YELLOW (store-layer jitter on top of the profile's ORANGE|RED-only rule).
   *
   * @param cls the priority class of the job being enqueued
   * @param band the current health band of the scheduler
   * @return deferral delay in ms; {@code 0} means admit without delay
   */
  public static long admissionDeferMs(StatsPriorityClass cls, SchedulerHealthBand band) {
    return switch (cls) {
      case P0_SYNC, P1_FRESHNESS -> 0L; // always admit
      case P2_REPAIR -> band == SchedulerHealthBand.RED ? DEFER_DELAY_MS : 0L;
      case P3_BACKGROUND ->
          switch (band) {
            case GREEN -> 0L;
            case YELLOW -> ThreadLocalRandom.current().nextBoolean() ? 0L : DEFER_DELAY_MS;
            case ORANGE, RED -> DEFER_DELAY_MS;
          };
    };
  }

  /**
   * Returns the starvation-aging threshold in milliseconds for the given priority class. A job that
   * has been waiting longer than this threshold is eligible to be counted as a starvation
   * promotion.
   *
   * <p>P0_SYNC and P1_FRESHNESS return {@link Long#MAX_VALUE} — they never age because they are
   * already the highest priority classes.
   *
   * @param cls the priority class of the job
   * @return aging threshold in ms; {@link Long#MAX_VALUE} means the class never ages
   */
  public static long agingThresholdMs(StatsPriorityClass cls) {
    return switch (cls) {
      case P3_BACKGROUND -> P3_AGING_THRESHOLD_MS;
      case P2_REPAIR -> P2_AGING_THRESHOLD_MS;
      case P1_FRESHNESS -> P1_AGING_THRESHOLD_MS; // effectively disabled
      default -> Long.MAX_VALUE; // P0_SYNC never ages
    };
  }
}
