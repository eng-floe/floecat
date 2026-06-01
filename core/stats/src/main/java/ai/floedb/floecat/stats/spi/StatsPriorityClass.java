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

package ai.floedb.floecat.stats.spi;

/**
 * Priority class for stats reconcile jobs. Lower {@link #order} values are dispatched first.
 *
 * <ul>
 *   <li>{@link #P0_SYNC} — query-time bounded capture; set exclusively by the stats orchestrator
 *       sync path; never returned by the scheduler policy SPI.
 *   <li>{@link #P1_FRESHNESS} — new-snapshot work; assigned by the planner worker for freshly
 *       discovered snapshots so that Floescan lookup-supervisor latency stays low.
 *   <li>{@link #P2_REPAIR} — async follow-up after a {@code PARTIAL}, {@code TIMEOUT}, or {@code
 *       FAILED} sync outcome.
 *   <li>{@link #P3_BACKGROUND} — routine background refresh and maintenance.
 * </ul>
 */
public enum StatsPriorityClass {
  P0_SYNC(0),
  P1_FRESHNESS(1),
  P2_REPAIR(2),
  P3_BACKGROUND(3);

  /** Numeric ordering used as the sort-key prefix in the ready queue. Lower = higher priority. */
  public final int order;

  StatsPriorityClass(int order) {
    this.order = order;
  }

  /**
   * Returns the next more-urgent class. P3→P2, P2→P1. P1 and P0 are returned unchanged (P0 is
   * assigned only by the orchestrator and is never promoted into).
   */
  public StatsPriorityClass promote() {
    return switch (this) {
      case P3_BACKGROUND -> P2_REPAIR;
      case P2_REPAIR -> P1_FRESHNESS;
      default -> this; // P1 and P0 do not promote
    };
  }

  /**
   * Parses from a string value (case-insensitive). Returns {@link #P3_BACKGROUND} for {@code null}
   * or unrecognised values so that legacy job records without a priority field default safely.
   */
  public static StatsPriorityClass fromString(String value) {
    if (value == null || value.isBlank()) {
      return P3_BACKGROUND;
    }
    try {
      return StatsPriorityClass.valueOf(value.toUpperCase(java.util.Locale.ROOT));
    } catch (IllegalArgumentException ignored) {
      return P3_BACKGROUND;
    }
  }
}
