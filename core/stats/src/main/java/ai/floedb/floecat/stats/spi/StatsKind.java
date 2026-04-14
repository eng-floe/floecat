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

/** Supported statistic families for capability routing. */
public enum StatsKind {
  /** Row-count statistics (table/file targets). */
  ROW_COUNT,
  /** File-count statistics (table targets). */
  FILE_COUNT,
  /** Total-bytes statistics (table/file targets). */
  TOTAL_BYTES,
  /** Null-count statistics (scalar targets). */
  NULL_COUNT,
  /** Distinct-count estimates (scalar targets). */
  NDV,
  /** Minimum/maximum value bounds (scalar targets). */
  MIN_MAX,
  /** Distribution summaries such as histograms (scalar targets). */
  HISTOGRAM
}
