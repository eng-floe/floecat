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
import ai.floedb.floecat.catalog.rpc.UpstreamStamp;
import java.util.Objects;
import java.util.Optional;

/** Column-target statistics value shape. */
public record StatsColumnValue(
    long columnId,
    String columnName,
    String logicalType,
    Optional<UpstreamStamp> upstream,
    StatsValueSummary valueStats) {

  public StatsColumnValue {
    columnName = columnName == null ? "" : columnName;
    logicalType = logicalType == null ? "" : logicalType;
    upstream = Objects.requireNonNullElse(upstream, Optional.empty());
    valueStats = Objects.requireNonNull(valueStats, "valueStats");
  }

  public static StatsColumnValue fromColumnStats(ColumnStats columnStats) {
    Objects.requireNonNull(columnStats, "columnStats");
    return new StatsColumnValue(
        columnStats.getColumnId(),
        columnStats.getColumnName(),
        columnStats.getLogicalType(),
        columnStats.hasUpstream() ? Optional.of(columnStats.getUpstream()) : Optional.empty(),
        StatsValueSummary.fromColumn(columnStats));
  }
}
