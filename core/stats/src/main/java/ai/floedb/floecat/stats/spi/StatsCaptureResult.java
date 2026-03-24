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

import ai.floedb.floecat.catalog.rpc.ColumnStats;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TableStats;
import java.util.Map;
import java.util.Objects;

/**
 * Provider-produced stats record for a single target.
 *
 * <p>This type is the output of one capture engine execution. It is not the final
 * planner/user-facing resolved answer and does not imply multi-provider composition.
 */
public record StatsCaptureResult(
    String engineId, StatsTarget target, StatsCaptureValue value, Map<String, String> attributes) {

  public StatsCaptureResult {
    engineId = Objects.requireNonNull(engineId, "engineId");
    target = Objects.requireNonNull(target, "target");
    value = Objects.requireNonNull(value, "value");
    attributes = Map.copyOf(Objects.requireNonNull(attributes, "attributes"));
  }

  public static StatsCaptureResult forTable(
      String engineId, StatsTarget target, TableStats tableStats, Map<String, String> attrs) {
    return new StatsCaptureResult(
        engineId, target, StatsCaptureValue.forTable(tableStats), attrs == null ? Map.of() : attrs);
  }

  public static StatsCaptureResult forColumn(
      String engineId, StatsTarget target, ColumnStats columnStats, Map<String, String> attrs) {
    return new StatsCaptureResult(
        engineId,
        target,
        StatsCaptureValue.forColumn(StatsColumnValue.fromColumnStats(columnStats)),
        attrs == null ? Map.of() : attrs);
  }
}
