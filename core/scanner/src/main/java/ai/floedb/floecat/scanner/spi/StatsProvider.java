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
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import java.util.List;
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

  /**
   * Returns latest known upstream snapshot id for the table.
   *
   * <p>This is intended for system-table scans that need "latest snapshot" semantics without
   * requiring query snapshot pinning.
   */
  default OptionalLong latestSnapshotId(ResourceId tableId) {
    return OptionalLong.empty();
  }

  /**
   * Returns the latest snapshot id for which persisted stats exist for the requested target type.
   *
   * <p>When no persisted stats exist, returns empty.
   */
  default OptionalLong latestPersistedStatsSnapshotId(
      ResourceId tableId, Optional<String> targetType) {
    return OptionalLong.empty();
  }

  /**
   * Lists persisted target stats without triggering capture orchestration.
   *
   * <p>Scanner-side callers use this to read persisted rows only. Implementations should interpret
   * {@code targetType} case-insensitively when provided (for example: TABLE, COLUMN, EXPRESSION).
   */
  default TargetStatsPage listPersistedStats(
      ResourceId tableId,
      long snapshotId,
      Optional<String> targetType,
      int limit,
      String pageToken) {
    return TargetStatsPage.EMPTY;
  }

  StatsProvider NONE = new StatsProvider() {};

  record TargetStatsPage(List<TargetStatsRecord> items, String nextToken) {
    public static final TargetStatsPage EMPTY = new TargetStatsPage(List.of(), "");

    public TargetStatsPage {
      items = items == null ? List.of() : List.copyOf(items);
      nextToken = nextToken == null ? "" : nextToken;
    }
  }

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
