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

package ai.floedb.floecat.scanner.spi;

import ai.floedb.floecat.catalog.rpc.Ndv;
import ai.floedb.floecat.common.rpc.ResourceId;
import java.util.Optional;
import java.util.OptionalLong;

/** Shared provider that surfaces table/column stats to metadata consumers. */
public interface StatsProvider {

  default Optional<TableStatsView> tableStats(ResourceId tableId) {
    return Optional.empty();
  }

  default Optional<ColumnStatsView> columnStats(ResourceId tableId, long columnId) {
    return Optional.empty();
  }

  default OptionalLong pinnedSnapshotId(ResourceId tableId) {
    return OptionalLong.empty();
  }

  StatsProvider NONE = new StatsProvider() {};

  interface TableStatsView {
    /** Table identity */
    ResourceId tableId();

    /** Snapshot id tied to these statistics */
    long snapshotId();

    /** Row count when explicitly reported (zero may be a real value). */
    OptionalLong rowCountValue();

    /** Total size when explicitly reported (zero may be a real value). */
    OptionalLong totalSizeBytesValue();
  }

  interface ColumnStatsView {
    /** Table identity */
    ResourceId tableId();

    /** Column id */
    long columnId();

    /**
     * Logical type string (same encoding as {@link
     * ai.floedb.floecat.query.rpc.SchemaColumn#logical_type}).
     */
    String logicalType();

    /** Column name (can be empty when unknown). */
    default String columnName() {
      return "";
    }

    /** Value count when explicitly reported. */
    long valueCount();

    /** Null count when explicitly reported. */
    OptionalLong nullCountValue();

    /** NaN count when explicitly reported. */
    OptionalLong nanCountValue();

    /**
     * Canonical min bound (UTF-8 string following the rules documented in {@code
     * floecat/catalog/stats.proto}). Presence indicates the bound was observed even when the string
     * content is empty.
     */
    Optional<String> minValue();

    /** Canonical max bound (UTF-8 string, same encoding as {@link #minValue()}). */
    Optional<String> maxValue();

    /** NDV summary (exact or approximate) if available. */
    Optional<Ndv> ndv();
  }
}
