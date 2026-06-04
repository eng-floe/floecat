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
 * Completeness of stats coverage for a (table, snapshot) pair.
 *
 * <p>Used by the scheduler coverage scoring factor: {@link #NONE} produces the highest urgency
 * contribution (100); {@link #FULL} produces zero contribution.
 *
 * <p>Implementations must treat {@code FULL} as a closed state — once coverage is recorded as
 * {@link #FULL} for a given (table, snapshot) pair it must not be downgraded to {@link #PARTIAL}.
 */
public enum CoverageLevel {
  /** No stats have been captured for this snapshot. */
  NONE,
  /** Some stat kinds are present but not all requested outputs. */
  PARTIAL,
  /** All requested stat outputs have been successfully captured. */
  FULL
}
