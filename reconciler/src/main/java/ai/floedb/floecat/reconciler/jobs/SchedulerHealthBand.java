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
 * Coarse health indicator for the reconcile job queue, used by admission control and autoscaling.
 *
 * <p>The enum ordinal is the canonical numeric value exposed by the {@code
 * floecat.service.reconcile.health_band} gauge metric (0=GREEN, 1=YELLOW, 2=ORANGE, 3=RED).
 * Dashboard thresholds and autoscaler rules depend on this ordering — do not reorder values.
 *
 * <table>
 *   <caption>Band semantics and admission behavior</caption>
 *   <tr><th>Band</th><th>Trigger</th><th>P2 admission</th><th>P3 admission</th></tr>
 *   <tr><td>GREEN</td><td>Normal operations</td><td>ADMIT</td><td>ADMIT</td></tr>
 *   <tr><td>YELLOW</td><td>P3 depth &gt; 500</td><td>ADMIT</td><td>ADMIT (50% probabilistic defer in store)</td></tr>
 *   <tr><td>ORANGE</td><td>P2 depth &gt; 200</td><td>ADMIT</td><td>DEFER</td></tr>
 *   <tr><td>RED</td><td>P0 job age &gt; 1 s</td><td>DEFER</td><td>DEFER</td></tr>
 * </table>
 *
 * <p>P0_SYNC and P1_FRESHNESS are always admitted regardless of band.
 */
public enum SchedulerHealthBand {
  GREEN,
  YELLOW,
  ORANGE,
  RED;

  /** Returns the next more-severe band, or the same band if already at RED. */
  public SchedulerHealthBand escalate() {
    return switch (this) {
      case GREEN -> YELLOW;
      case YELLOW -> ORANGE;
      case ORANGE, RED -> RED;
    };
  }

  /** Returns the next less-severe band, or the same band if already at GREEN. */
  public SchedulerHealthBand relax() {
    return switch (this) {
      case RED -> ORANGE;
      case ORANGE -> YELLOW;
      case YELLOW, GREEN -> GREEN;
    };
  }
}
