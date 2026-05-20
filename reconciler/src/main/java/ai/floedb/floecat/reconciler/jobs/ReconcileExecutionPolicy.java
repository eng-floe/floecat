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

package ai.floedb.floecat.reconciler.jobs;

import java.util.Map;
import java.util.TreeMap;

/**
 * Execution policy attached to every reconcile job.
 *
 * <p>In addition to the legacy {@link ReconcileExecutionClass} (kept for backward compatibility),
 * jobs now carry a {@link StatsPriorityClass} and a {@code priorityScore} that drive the
 * priority-aware queue introduced in the stats scheduling model.
 *
 * <p>Defaults: {@link StatsPriorityClass#P3_BACKGROUND} / score 0, preserving the previous FIFO
 * behaviour for all existing callers that use {@link #defaults()}.
 */
public record ReconcileExecutionPolicy(
    ReconcileExecutionClass executionClass,
    /**
     * Advisory fairness lane key set by the caller (e.g. {@code "accountId:tableId"}).
     *
     * <p><b>Phase 1 note:</b> {@link
     * ai.floedb.floecat.reconciler.jobs.impl.InMemoryReconcileJobStore} currently ignores this
     * field when assigning the actual lane mutex. The store derives its own lane key from the job's
     * scope and job-kind, which achieves equivalent per-table serialisation. This field is carried
     * on the job record for Phase 2, where the weighted-round-robin fairness layer will use it as
     * the WRR unit. Do not rely on this value for correctness decisions in Phase 1.
     *
     * <p>TODO(phase2): wire this into the store's lane assignment when non-blank.
     */
    String lane,
    Map<String, String> attributes,
    StatsPriorityClass priorityClass,
    long priorityScore) {

  private static final ReconcileExecutionPolicy DEFAULT_POLICY =
      new ReconcileExecutionPolicy(
          ReconcileExecutionClass.DEFAULT, "", Map.of(), StatsPriorityClass.P3_BACKGROUND, 0L);

  public ReconcileExecutionPolicy {
    executionClass = executionClass == null ? ReconcileExecutionClass.DEFAULT : executionClass;
    lane = lane == null ? "" : lane.trim();
    priorityClass = priorityClass == null ? StatsPriorityClass.P3_BACKGROUND : priorityClass;
    priorityScore = Math.max(0L, priorityScore);

    Map<String, String> normalized = new TreeMap<>();
    if (attributes != null) {
      for (var entry : attributes.entrySet()) {
        String key = entry.getKey();
        if (key == null || key.isBlank()) {
          continue;
        }
        normalized.put(key.trim(), entry.getValue() == null ? "" : entry.getValue().trim());
      }
    }
    attributes = Map.copyOf(normalized);
  }

  /** Returns the default policy: P3_BACKGROUND, score 0, no lane, no attributes. */
  public static ReconcileExecutionPolicy defaults() {
    return DEFAULT_POLICY;
  }

  /**
   * Legacy factory (execution-class form). Sets {@link StatsPriorityClass#P3_BACKGROUND} and score
   * 0 so that existing callers are unaffected.
   */
  public static ReconcileExecutionPolicy of(
      ReconcileExecutionClass executionClass, String lane, Map<String, String> attributes) {
    if ((executionClass == null || executionClass == ReconcileExecutionClass.DEFAULT)
        && (lane == null || lane.isBlank())
        && (attributes == null || attributes.isEmpty())) {
      return DEFAULT_POLICY;
    }
    return new ReconcileExecutionPolicy(
        executionClass, lane, attributes, StatsPriorityClass.P3_BACKGROUND, 0L);
  }

  /**
   * Full factory that sets all scheduling fields explicitly.
   *
   * <p>Use this form when the caller knows the priority class and score (e.g. the stats
   * orchestrator for P0_SYNC, or the policy registry for scored async jobs).
   */
  public static ReconcileExecutionPolicy of(
      StatsPriorityClass priorityClass,
      String lane,
      Map<String, String> attributes,
      long priorityScore) {
    if (priorityClass == StatsPriorityClass.P3_BACKGROUND
        && priorityScore == 0L
        && (lane == null || lane.isBlank())
        && (attributes == null || attributes.isEmpty())) {
      return DEFAULT_POLICY;
    }
    return new ReconcileExecutionPolicy(
        ReconcileExecutionClass.DEFAULT, lane, attributes, priorityClass, priorityScore);
  }

  /**
   * Convenience factory for cases where only the priority class and lane matter (score = 0).
   *
   * <p>Primarily used in the orchestrator before scoring is live (phase 1).
   */
  public static ReconcileExecutionPolicy of(
      StatsPriorityClass priorityClass, String lane, Map<String, String> attributes) {
    return of(priorityClass, lane, attributes, 0L);
  }

  public boolean isDefaultPolicy() {
    return executionClass == ReconcileExecutionClass.DEFAULT
        && lane.isBlank()
        && attributes.isEmpty()
        && priorityClass == StatsPriorityClass.P3_BACKGROUND
        && priorityScore == 0L;
  }
}
