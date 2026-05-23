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
 * Describes how completely stats exist for a given (table, snapshot, target) scope.
 *
 * <p>Used as an input to the async priority scoring model. A higher coverage gap produces a higher
 * urgency score. Kept as an enum rather than a boolean so that partial coverage can be expressed
 * and scored independently of full absence.
 *
 * <p>Coverage is evaluated per (table_id, snapshot_id) at enqueue time and baked into the job's
 * {@code priorityScore}. It is not re-evaluated at dispatch time.
 */
public enum CoverageLevel {

  /**
   * No stats exist for this target in the given snapshot.
   *
   * <p>Produces the highest urgency contribution in scoring (score contribution: 100).
   */
  NONE,

  /**
   * Some stat kinds are present but others are absent or stale.
   *
   * <p>For example: row count is available but NDV or histogram is missing. Score contribution: 50.
   */
  PARTIAL,

  /**
   * All requested stat kinds are present and not stale.
   *
   * <p>Score contribution: 0. A FULL-coverage job still runs if scheduled, but contributes nothing
   * to urgency from this factor.
   */
  FULL
}
