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
 * Execution scheduling policy for a reconcile job.
 *
 * <p>In addition to the legacy {@code executionClass} / {@code lane} / {@code attributes} fields,
 * this record now carries a {@link StatsPriorityClass} and a {@code priorityScore}. These two
 * fields drive priority-aware leasing: jobs are dispatched in class order (P0 first), and within a
 * class by descending score.
 *
 * <p>Existing callers that use {@link #defaults()} or the 3-arg {@link #of(ReconcileExecutionClass,
 * String, Map)} factory receive {@link StatsPriorityClass#P3_BACKGROUND} and score {@code 0} by
 * default — no behavioral change.
 */
public record ReconcileExecutionPolicy(
    ReconcileExecutionClass executionClass,
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

  public static ReconcileExecutionPolicy defaults() {
    return DEFAULT_POLICY;
  }

  /**
   * Legacy 3-arg factory. Priority defaults to {@link StatsPriorityClass#P3_BACKGROUND} with score
   * {@code 0}. Prefer the 5-arg overload when priority information is available.
   */
  public static ReconcileExecutionPolicy of(
      ReconcileExecutionClass executionClass, String lane, Map<String, String> attributes) {
    return of(executionClass, lane, attributes, StatsPriorityClass.P3_BACKGROUND, 0L);
  }

  /**
   * Full factory. Returns the cached {@link #DEFAULT_POLICY} singleton when all fields are
   * default-valued to avoid needless allocation on the hot path.
   */
  public static ReconcileExecutionPolicy of(
      StatsPriorityClass priorityClass,
      String lane,
      Map<String, String> attributes,
      long priorityScore) {
    return of(ReconcileExecutionClass.DEFAULT, lane, attributes, priorityClass, priorityScore);
  }

  public static ReconcileExecutionPolicy of(
      ReconcileExecutionClass executionClass,
      String lane,
      Map<String, String> attributes,
      StatsPriorityClass priorityClass,
      long priorityScore) {
    if ((executionClass == null || executionClass == ReconcileExecutionClass.DEFAULT)
        && (lane == null || lane.isBlank())
        && (attributes == null || attributes.isEmpty())
        && (priorityClass == null || priorityClass == StatsPriorityClass.P3_BACKGROUND)
        && priorityScore == 0L) {
      return DEFAULT_POLICY;
    }
    return new ReconcileExecutionPolicy(
        executionClass, lane, attributes, priorityClass, priorityScore);
  }

  public boolean isDefaultPolicy() {
    return executionClass == ReconcileExecutionClass.DEFAULT
        && lane.isBlank()
        && attributes.isEmpty()
        && priorityClass == StatsPriorityClass.P3_BACKGROUND
        && priorityScore == 0L;
  }
}
