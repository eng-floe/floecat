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

import ai.floedb.floecat.reconciler.jobs.SchedulerHealthBand;
import ai.floedb.floecat.reconciler.jobs.StatsPriorityClass;

/**
 * Policy SPI for deciding whether a job should be admitted, deferred, or rejected at enqueue time.
 *
 * <p>The following invariant is non-negotiable and is validated by {@link SchedulerPolicyRegistry}
 * at startup: when the incoming job's priority class is {@link StatsPriorityClass#P0_SYNC}, {@link
 * #decide} must always return {@link AdmissionDecision#ADMIT} regardless of band.
 *
 * <p>The stats orchestrator calls {@link #decide} before every async enqueue. {@link
 * AdmissionDecision#REJECT} skips the enqueue entirely and returns a degraded result to the caller.
 * {@link AdmissionDecision#DEFER} allows the enqueue; band-based deferral timing is then applied
 * inside the job store. P0_SYNC jobs are never routed through this path — they bypass the scheduler
 * policy entirely and are always dispatched synchronously.
 *
 * <p>Implementations must be annotated with {@link SchedulerProfile} so the registry can resolve
 * them by name.
 *
 * <p>All methods must be cheap to call — no blocking I/O is permitted.
 */
public interface SchedulerAdmissionPolicy {

  /**
   * Decides whether to admit, defer, or reject a job given its priority assignment and the current
   * scheduler health band.
   *
   * <p>Contract:
   *
   * <ul>
   *   <li>When {@code assignment.priorityClass()} is {@link StatsPriorityClass#P0_SYNC}, must
   *       always return {@link AdmissionDecision#ADMIT}.
   *   <li>When returning {@link AdmissionDecision#DEFER}, the job is enqueued with a delayed {@code
   *       nextAttemptAtMs} and becomes eligible again after that delay.
   *   <li>When returning {@link AdmissionDecision#REJECT}, the job is not enqueued and the caller
   *       returns a degraded result to the originating request. Use sparingly; P0 and P1 must never
   *       be rejected.
   * </ul>
   *
   * @param assignment the priority assignment produced by {@link SchedulerPriorityPolicy#assign}
   * @param currentBand current health band of the scheduler
   * @return the admission decision
   */
  AdmissionDecision decide(
      SchedulerPriorityPolicy.PriorityAssignment assignment, SchedulerHealthBand currentBand);

  /** Outcome of an admission decision for a newly enqueued job. */
  enum AdmissionDecision {

    /** Job is enqueued immediately and eligible for dispatch without delay. */
    ADMIT,

    /**
     * Job is enqueued with a delayed {@code nextAttemptAtMs}. The job will be eligible for dispatch
     * after the delay elapses. Counted in {@code reconcile.admission.deferred} metrics.
     */
    DEFER,

    /**
     * Job is not enqueued. The caller returns a degraded result. Counted in {@code
     * reconcile.admission.rejected} metrics. Must not be returned for P0 or P1 jobs. Should not be
     * returned for P2 jobs either — doing so silently drops async follow-up work for queries that
     * already observed a sync TIMEOUT or FAILED, leaving coverage permanently missing.
     */
    REJECT
  }
}
