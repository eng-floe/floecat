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
 * Coarse cost estimate for a stats capture or index job.
 *
 * <p>Used by the scheduler to prefer high value-per-cost jobs within a priority class, and by the
 * sync capture path to restrict which stat kinds an engine may attempt within a latency budget.
 *
 * <p>Engines report their cost via {@link StatsCaptureEngine#estimatedCost} and {@code
 * CaptureEngine.estimatedCost()}. Implementations must not perform I/O to compute the hint.
 *
 * <p>Rough priors by stat kind and connector type:
 *
 * <table>
 *   <tr><th>Stat kind</th><th>Parquet connector</th><th>JDBC connector</th></tr>
 *   <tr><td>ROW_COUNT, FILE_COUNT, TOTAL_BYTES, MIN_MAX</td><td>CHEAP (footer read)</td><td>EXPENSIVE</td></tr>
 *   <tr><td>NULL_COUNT, partial column scan</td><td>MEDIUM</td><td>EXPENSIVE</td></tr>
 *   <tr><td>NDV, HISTOGRAM, MCV</td><td>EXPENSIVE (full column)</td><td>EXPENSIVE</td></tr>
 *   <tr><td>PARQUET_PAGE_INDEX (Floescan)</td><td>EXPENSIVE (full file read)</td><td>N/A</td></tr>
 * </table>
 *
 * <p>These are defaults. Engine implementations override via {@code estimatedCost()}.
 */
public enum JobCostHint {

  /** Metadata-only or footer-level read. Suitable within a sync latency budget. */
  CHEAP,

  /** Partial column scan or light aggregation. May be attempted in sync with a loose budget. */
  MEDIUM,

  /** Full column scan, histogram computation, or full Parquet file read for index construction. */
  EXPENSIVE;

  /**
   * Returns true if this cost is within the given budget.
   *
   * <p>CHEAP fits in any budget. MEDIUM fits in MEDIUM or EXPENSIVE. EXPENSIVE only fits in
   * EXPENSIVE.
   */
  public boolean fitsIn(JobCostHint budget) {
    return this.ordinal() <= budget.ordinal();
  }
}
