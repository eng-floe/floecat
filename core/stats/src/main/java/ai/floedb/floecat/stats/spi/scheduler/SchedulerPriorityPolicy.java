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
package ai.floedb.floecat.stats.spi.scheduler;

import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsPriorityClass;

public interface SchedulerPriorityPolicy {
  PriorityAssignment assign(StatsCaptureRequest request, SchedulerContext context);

  /**
   * Called for reconciler-side jobs (PLAN_SNAPSHOT, EXEC_FILE_GROUP) at enqueue time. The {@code
   * jobKind} is the reconcile job kind name (e.g. {@code "PLAN_SNAPSHOT"}). Default: P1_FRESHNESS
   * for new-snapshot work, P3_BACKGROUND otherwise.
   */
  default PriorityAssignment assignForReconcileJob(
      String jobKind,
      String tableId,
      long snapshotId,
      boolean isNewSnapshot,
      SchedulerContext context) {
    StatsPriorityClass cls =
        isNewSnapshot ? StatsPriorityClass.P1_FRESHNESS : StatsPriorityClass.P3_BACKGROUND;
    return new PriorityAssignment(cls, 0L, tableId);
  }

  record PriorityAssignment(StatsPriorityClass priorityClass, long score, String laneKey) {
    public PriorityAssignment {
      if (priorityClass == null) priorityClass = StatsPriorityClass.P3_BACKGROUND;
      score = Math.max(0L, score);
      laneKey = laneKey == null ? "" : laneKey.trim();
    }
  }
}
