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
 * Cost tier for a stats capture job. Used in two ways:
 *
 * <ol>
 *   <li>As {@code ReconcileCapturePolicy.maxCost}: a budget ceiling. Engines whose {@code
 *       estimatedCost()} exceeds the ceiling are skipped for this job. The sync path sets {@link
 *       #MEDIUM} to exclude expensive full-column scans; the async path defaults to {@link
 *       #EXPENSIVE} (no restriction).
 *   <li>As a scoring divisor in {@code DefaultSchedulerProfile}: expensive jobs receive a lower
 *       normalised score than cheap jobs with identical urgency signals, biasing the scheduler
 *       toward high value-per-cost captures.
 * </ol>
 */
public enum JobCostHint {
  /** Footer reads only (row count, file count, total bytes, min/max). */
  CHEAP,
  /** Partial-column reads (null count, some aggregates). */
  MEDIUM,
  /** Full-column or full-file reads (NDV, histogram, Parquet page index). */
  EXPENSIVE;

  /**
   * Returns {@code true} if this cost tier fits within the given {@code budget}. A job with cost
   * {@code CHEAP} fits any budget; {@code EXPENSIVE} only fits an {@code EXPENSIVE} budget.
   */
  public boolean fitsIn(JobCostHint budget) {
    return this.ordinal() <= budget.ordinal();
  }
}
