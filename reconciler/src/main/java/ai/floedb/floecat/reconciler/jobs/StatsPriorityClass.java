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
 * Priority class for reconcile jobs in the stats scheduling model.
 *
 * <p>Classes are ordered by urgency: {@link #P0_SYNC} is always dispatched first, {@link
 * #P3_BACKGROUND} last. The {@link #order} field reflects this and can be used for comparisons.
 *
 * <p>P0_SYNC is assigned exclusively by the stats orchestrator for query-time bounded capture. It
 * is never assigned by any pluggable policy SPI. All other classes may be assigned by the active
 * {@code SchedulerPriorityPolicy}.
 */
public enum StatsPriorityClass {

  /**
   * Query-time synchronous capture within a strict latency budget.
   *
   * <p>Assigned only by the stats orchestrator. Never assigned by any policy SPI. Always dispatched
   * ahead of all other classes.
   */
  P0_SYNC(0),

  /**
   * New-snapshot work that must complete quickly to keep data fresh and queryable.
   *
   * <p>Assigned for {@code PLAN_SNAPSHOT} and {@code EXEC_FILE_GROUP} jobs that originate from a
   * newly detected snapshot. Completing this work feeds Floescan lookup supervisor epoch-polling
   * latency directly.
   */
  P1_FRESHNESS(1),

  /**
   * Async follow-up after a weak sync outcome (PARTIAL, TIMEOUT, or FAILED).
   *
   * <p>Enqueued by the stats orchestrator when a sync capture did not fully succeed, so that the
   * missing coverage is eventually filled without blocking the planner again.
   */
  P2_REPAIR(2),

  /**
   * Routine background refresh and maintenance work.
   *
   * <p>Default class for all background stats refreshes and re-index jobs. May be deferred or
   * throttled under backpressure without impacting planner responsiveness.
   */
  P3_BACKGROUND(3);

  /** Numeric ordering: lower value = higher urgency. */
  public final int order;

  StatsPriorityClass(int order) {
    this.order = order;
  }

  /**
   * Returns the next more-urgent class, capped at {@link #P1_FRESHNESS}.
   *
   * <p>P0_SYNC cannot be promoted to — it is reserved for the orchestrator. P1_FRESHNESS is the
   * highest class reachable via promotion.
   *
   * <ul>
   *   <li>P3_BACKGROUND → P2_REPAIR
   *   <li>P2_REPAIR → P1_FRESHNESS
   *   <li>P1_FRESHNESS → P1_FRESHNESS (ceiling)
   *   <li>P0_SYNC → P0_SYNC (unreachable via promotion; returned unchanged)
   * </ul>
   */
  public StatsPriorityClass promote() {
    return switch (this) {
      case P3_BACKGROUND -> P2_REPAIR;
      case P2_REPAIR -> P1_FRESHNESS;
      case P1_FRESHNESS, P0_SYNC -> this;
    };
  }

  /** Parses a class from its name, returning {@link #P3_BACKGROUND} for unrecognised values. */
  public static StatsPriorityClass fromString(String value) {
    if (value == null || value.isBlank()) {
      return P3_BACKGROUND;
    }
    try {
      return StatsPriorityClass.valueOf(value.trim().toUpperCase());
    } catch (IllegalArgumentException ignored) {
      return P3_BACKGROUND;
    }
  }
}
