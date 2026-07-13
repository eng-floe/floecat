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

package ai.floedb.floecat.service.catalog.impl;

import ai.floedb.floecat.stats.spi.StatsStore;

/**
 * The ONE place the snapshot-visibility gate decision lives. A deployment gates visibility on
 * finalize when its stats store tracks generations: a manifest entry without its generation ref is
 * not yet query-ready (file list, indexes, and stats have not published). The gate is enforced at
 * READ time — a query read or pin will not resolve to a current whose entry carries no generation
 * ref; the newest finalized snapshot at or before it is served instead (snapshot isolation). The
 * root's {@code current_snapshot_id} itself advances freely at registration and resync so logical
 * (Iceberg) metadata can move current without exposing an unfinalized scan. A store that tracks no
 * generations — or no store at all — cannot express readiness and is exempt everywhere.
 */
public final class StatsVisibilityGate {

  private StatsVisibilityGate() {}

  public static boolean gateOnFinalize(StatsStore store) {
    return store != null && store.tracksStatsGenerations();
  }
}
