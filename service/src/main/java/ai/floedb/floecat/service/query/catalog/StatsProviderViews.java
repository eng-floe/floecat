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

package ai.floedb.floecat.service.query.catalog;

import ai.floedb.floecat.catalog.rpc.ColumnStats;
import ai.floedb.floecat.catalog.rpc.Ndv;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.impl.StatsRepository;
import ai.floedb.floecat.systemcatalog.spi.scanner.StatsProvider;
import java.util.Optional;
import java.util.OptionalLong;

final class StatsProviderViews {

  private StatsProviderViews() {}

  static StatsProvider.TableStatsView tableStatsView(StatsRepository.TableStatsView stats) {
    OptionalLong rowCount = OptionalLong.of(stats.rowCount());
    OptionalLong totalSize = OptionalLong.of(stats.totalSizeBytes());
    return new TableStatsViewImpl(stats.tableId(), stats.snapshotId(), rowCount, totalSize);
  }

  static StatsProvider.ColumnStatsView columnStatsView(ColumnStats stats) {
    Optional<String> min = stats.hasMin() ? Optional.of(stats.getMin()) : Optional.empty();
    Optional<String> max = stats.hasMax() ? Optional.of(stats.getMax()) : Optional.empty();
    OptionalLong nullCount =
        stats.hasNullCount() ? OptionalLong.of(stats.getNullCount()) : OptionalLong.empty();
    OptionalLong nanCount =
        stats.hasNanCount() ? OptionalLong.of(stats.getNanCount()) : OptionalLong.empty();
    return new ColumnStatsViewImpl(
        stats.getTableId(),
        stats.getColumnId(),
        stats.getColumnName(),
        stats.getValueCount(),
        nullCount,
        nanCount,
        stats.getLogicalType(),
        min,
        max,
        stats.hasNdv() ? stats.getNdv() : null);
  }

  private static final class TableStatsViewImpl implements StatsProvider.TableStatsView {
    private final ResourceId tableId;
    private final long snapshotId;
    private final OptionalLong rowCount;
    private final OptionalLong totalSizeBytes;

    private TableStatsViewImpl(
        ResourceId tableId, long snapshotId, OptionalLong rowCount, OptionalLong totalSizeBytes) {
      this.tableId = tableId;
      this.snapshotId = snapshotId;
      this.rowCount = rowCount;
      this.totalSizeBytes = totalSizeBytes;
    }

    @Override
    public ResourceId tableId() {
      return tableId;
    }

    @Override
    public long snapshotId() {
      return snapshotId;
    }

    @Override
    public OptionalLong rowCountValue() {
      return rowCount;
    }

    @Override
    public OptionalLong totalSizeBytesValue() {
      return totalSizeBytes;
    }
  }

  private static final class ColumnStatsViewImpl implements StatsProvider.ColumnStatsView {
    private final ResourceId tableId;
    private final long columnId;
    private final String columnName;
    private final long valueCount;
    private final OptionalLong nullCount;
    private final OptionalLong nanCount;
    private final String logicalType;
    private final Optional<String> min;
    private final Optional<String> max;
    private final Ndv ndv;

    private ColumnStatsViewImpl(
        ResourceId tableId,
        long columnId,
        String columnName,
        long valueCount,
        OptionalLong nullCount,
        OptionalLong nanCount,
        String logicalType,
        Optional<String> min,
        Optional<String> max,
        Ndv ndv) {
      this.tableId = tableId;
      this.columnId = columnId;
      this.columnName = columnName;
      this.valueCount = valueCount;
      this.nullCount = nullCount;
      this.nanCount = nanCount;
      this.logicalType = logicalType;
      this.min = min;
      this.max = max;
      this.ndv = ndv;
    }

    @Override
    public ResourceId tableId() {
      return tableId;
    }

    @Override
    public long columnId() {
      return columnId;
    }

    @Override
    public String logicalType() {
      return logicalType;
    }

    @Override
    public String columnName() {
      return columnName;
    }

    @Override
    public long valueCount() {
      return valueCount;
    }

    @Override
    public OptionalLong nullCountValue() {
      return nullCount;
    }

    @Override
    public OptionalLong nanCountValue() {
      return nanCount;
    }

    @Override
    public Optional<String> minValue() {
      return min;
    }

    @Override
    public Optional<String> maxValue() {
      return max;
    }

    @Override
    public Optional<Ndv> ndv() {
      return Optional.ofNullable(ndv);
    }
  }
}
