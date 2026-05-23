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

/**
 * Coarse health indicator for the reconcile scheduler, derived from queue-depth thresholds.
 *
 * <p>The band transitions are computed on every {@code queueStats()} call and are stored in {@code
 * InMemoryReconcileJobStore.currentBand}. Dwell hysteresis (preventing flapping) is not yet
 * implemented.
 *
 * <h2>Band semantics</h2>
 *
 * <ul>
 *   <li>{@link #RED} — A {@link StatsPriorityClass#P0_SYNC} job has been queued longer than the
 *       sync budget (1&nbsp;s). This band clears immediately when no P0 job exceeds the budget.
 *       Admission of {@link StatsPriorityClass#P2_REPAIR} and {@link
 *       StatsPriorityClass#P3_BACKGROUND} work is deferred while RED.
 *   <li>{@link #ORANGE} — The {@link StatsPriorityClass#P2_REPAIR} queue depth exceeds 200 entries.
 *       Background ({@link StatsPriorityClass#P3_BACKGROUND}) admission is deferred while ORANGE.
 *   <li>{@link #YELLOW} — The {@link StatsPriorityClass#P3_BACKGROUND} queue depth exceeds 500
 *       entries. Background admission is probabilistically deferred (~50 %) while YELLOW.
 *   <li>{@link #GREEN} — No thresholds are breached; the scheduler is operating normally.
 * </ul>
 *
 * <h2>Transitions</h2>
 *
 * {@link #escalate()} moves to the next more-severe band; {@link #relax()} moves to the next
 * less-severe band. Both are bounded: escalating from RED stays RED, relaxing from GREEN stays
 * GREEN. These helpers are provided for future hysteresis logic and for unit tests.
 */
public enum SchedulerHealthBand {
  GREEN,
  YELLOW,
  ORANGE,
  RED;

  /**
   * Returns the next more-severe band.
   *
   * <ul>
   *   <li>GREEN → YELLOW
   *   <li>YELLOW → ORANGE
   *   <li>ORANGE → RED
   *   <li>RED → RED (ceiling)
   * </ul>
   */
  public SchedulerHealthBand escalate() {
    return switch (this) {
      case GREEN -> YELLOW;
      case YELLOW -> ORANGE;
      case ORANGE, RED -> RED;
    };
  }

  /**
   * Returns the next less-severe band.
   *
   * <ul>
   *   <li>GREEN → GREEN (floor)
   *   <li>YELLOW → GREEN
   *   <li>ORANGE → YELLOW
   *   <li>RED → ORANGE
   * </ul>
   */
  public SchedulerHealthBand relax() {
    return switch (this) {
      case GREEN -> GREEN;
      case YELLOW -> GREEN;
      case ORANGE -> YELLOW;
      case RED -> ORANGE;
    };
  }
}
