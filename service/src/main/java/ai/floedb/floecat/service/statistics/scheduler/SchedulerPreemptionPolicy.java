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

import ai.floedb.floecat.reconciler.jobs.StatsPriorityClass;
import java.util.List;
import java.util.Optional;

/**
 * Policy SPI for selecting a victim job to preempt when a higher-priority job cannot be dispatched
 * because all workers are busy with lower-priority work.
 *
 * <p>This interface is defined here for forward compatibility but is not wired into the scheduler
 * until preemption is enabled (controlled by {@code floecat.stats.scheduler.preemption.enabled},
 * which defaults to {@code false}). Implementations will be invoked only when that flag is set.
 *
 * <p>The following invariant is non-negotiable and is validated by {@link SchedulerPolicyRegistry}
 * at startup: {@link #selectVictim} must never return a job whose priority class is {@link
 * StatsPriorityClass#P0_SYNC}.
 *
 * <p>Implementations must be annotated with {@link SchedulerProfile} so the registry can resolve
 * them by name.
 *
 * <p>All methods must be cheap to call — no blocking I/O is permitted.
 */
public interface SchedulerPreemptionPolicy {

  /**
   * Selects a victim {@code EXEC_FILE_GROUP} job to preempt in order to free a worker for the
   * incoming job.
   *
   * <p>Only jobs in {@code JS_RUNNING} state with priority class below the incoming job's class are
   * eligible candidates. The scheduler guarantees that the provided {@code candidates} list
   * contains only eligible jobs (non-P0, running, not on preemption cooldown).
   *
   * <p>Contract:
   *
   * <ul>
   *   <li>Must never return a job whose {@link RunningJobInfo#priorityClass()} is {@link
   *       StatsPriorityClass#P0_SYNC}.
   *   <li>Must return empty if no suitable victim exists; in that case no preemption occurs.
   *   <li>The returned job ID must be present in {@code candidates}.
   * </ul>
   *
   * @param incomingJobId the job waiting for a free worker
   * @param candidates eligible running jobs that may be preempted; never contains P0 jobs
   * @param context read-only view of current scheduler state
   * @return the job ID of the victim to preempt, or empty if no preemption should occur
   */
  Optional<String> selectVictim(
      String incomingJobId, List<RunningJobInfo> candidates, SchedulerContext context);

  /**
   * Read-only view of a running job that is a candidate for preemption.
   *
   * @param jobId the job's identifier
   * @param priorityClass the job's priority class; never {@link StatsPriorityClass#P0_SYNC} in
   *     candidates passed to {@link #selectVictim}
   * @param startedAtMs epoch-millisecond timestamp when the job began executing
   * @param completedFiles number of files successfully processed so far
   * @param totalFiles total number of files in the job's file group
   */
  record RunningJobInfo(
      String jobId,
      StatsPriorityClass priorityClass,
      long startedAtMs,
      int completedFiles,
      int totalFiles) {}
}
