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
 * distributed under the License is distributed on an "AS-IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.service.query.catalog;

import ai.floedb.floecat.catalog.rpc.Ndv;
import ai.floedb.floecat.catalog.rpc.ScalarStats;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TableStatsTarget;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.query.rpc.RelationStats;
import ai.floedb.floecat.scanner.spi.StatsProvider;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.service.statistics.engine.StatsEngineRegistry;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsExecutionMode;
import ai.floedb.floecat.stats.spi.StatsUnsupportedTargetException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.jboss.logging.Logger;

@ApplicationScoped
public final class StatsProviderFactory {

  private static final Logger LOG = Logger.getLogger(StatsProviderFactory.class);

  private final StatsEngineRegistry statsEngineRegistry;
  private final QueryContextStore queryStore;

  @Inject
  public StatsProviderFactory(
      StatsEngineRegistry statsEngineRegistry, QueryContextStore queryStore) {
    this.statsEngineRegistry = statsEngineRegistry;
    this.queryStore = queryStore;
  }

  public StatsProvider forQuery(QueryContext ctx, String correlationId) {
    return new CachedStatsProvider(statsEngineRegistry, queryStore, ctx, correlationId);
  }

  SnapshotPinLookup pinLookupForQuery(QueryContext ctx, String correlationId) {
    return new SnapshotPinResolver(queryStore, ctx, correlationId);
  }

  private static final class CachedStatsProvider implements StatsProvider {

    private final StatsEngineRegistry statsEngineRegistry;
    private final SnapshotPinResolver pinResolver;
    private final ConcurrentMap<SnapshotScopedRelationKey, Optional<StatsProvider.TableStatsView>>
        tableCache = new ConcurrentHashMap<>();

    private CachedStatsProvider(
        StatsEngineRegistry statsEngineRegistry,
        QueryContextStore queryStore,
        QueryContext ctx,
        String correlationId) {
      this.statsEngineRegistry = statsEngineRegistry;
      this.pinResolver = new SnapshotPinResolver(queryStore, ctx, correlationId);
    }

    @Override
    public Optional<StatsProvider.TableStatsView> tableStats(ResourceId tableId) {
      return pinResolver.withPinnedSnapshot(
          tableId,
          snapshotId ->
              tableCache.computeIfAbsent(
                  SnapshotScopedRelationKey.of(tableId, snapshotId),
                  key -> safeTableStats(tableId, snapshotId)));
    }

    @Override
    public Optional<StatsProvider.ColumnStatsView> columnStats(ResourceId tableId, long columnId) {
      return pinResolver.withPinnedSnapshot(
          tableId, snapshotId -> safeColumnStats(tableId, snapshotId, columnId));
    }

    @Override
    public OptionalLong pinnedSnapshotId(ResourceId tableId) {
      return pinResolver.pinnedSnapshotId(tableId);
    }

    private Optional<StatsProvider.TableStatsView> safeTableStats(
        ResourceId tableId, long snapshotId) {
      try {
        var request =
            new StatsCaptureRequest(
                tableId,
                snapshotId,
                StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()).build(),
                // Empty requested kinds means "accept any available stat family".
                Set.of(),
                StatsExecutionMode.SYNC,
                // Empty connector type means no connector-based engine filtering.
                "",
                false);
        return statsEngineRegistry
            .capture(request)
            .map(result -> result.record())
            .filter(TargetStatsRecord::hasTable)
            .map(CachedStatsProvider::toTableStatsView);
      } catch (StatsUnsupportedTargetException e) {
        LOG.debugf(
            "table stats unsupported for %s snapshot %s targetType=%s",
            tableId, snapshotId, e.targetType());
        return Optional.empty();
      } catch (RuntimeException e) {
        LOG.debugf(e, "table stats lookup failed for %s snapshot %s", tableId, snapshotId);
        return Optional.empty();
      }
    }

    private Optional<StatsProvider.ColumnStatsView> safeColumnStats(
        ResourceId tableId, long snapshotId, long columnId) {
      try {
        var request =
            new StatsCaptureRequest(
                tableId,
                snapshotId,
                StatsTargetIdentity.columnTarget(columnId),
                // Empty requested kinds means "accept any available stat family".
                Set.of(),
                StatsExecutionMode.SYNC,
                // Empty connector type means no connector-based engine filtering.
                "",
                false);
        return statsEngineRegistry
            .capture(request)
            .map(result -> result.record())
            .filter(TargetStatsRecord::hasScalar)
            .map(CachedStatsProvider::toColumnStatsView);
      } catch (StatsUnsupportedTargetException e) {
        LOG.debugf(
            "column stats unsupported for %s column %s snapshot %s targetType=%s",
            tableId, columnId, snapshotId, e.targetType());
        return Optional.empty();
      } catch (RuntimeException e) {
        LOG.debugf(
            e,
            "column stats lookup failed for %s column %s snapshot %s",
            tableId,
            columnId,
            snapshotId);
        return Optional.empty();
      }
    }

    private static StatsProvider.TableStatsView toTableStatsView(TargetStatsRecord record) {
      return new TableStatsViewImpl(
          record.getTableId(),
          record.getSnapshotId(),
          OptionalLong.of(record.getTable().getRowCount()),
          OptionalLong.of(record.getTable().getTotalSizeBytes()));
    }

    private static StatsProvider.ColumnStatsView toColumnStatsView(TargetStatsRecord record) {
      ScalarStats scalar = record.getScalar();
      return new ColumnStatsViewImpl(
          record.getTableId(),
          record.getTarget().getColumn().getColumnId(),
          scalar.getDisplayName(),
          scalar.getValueCount(),
          scalar.hasNullCount() ? OptionalLong.of(scalar.getNullCount()) : OptionalLong.empty(),
          scalar.hasNanCount() ? OptionalLong.of(scalar.getNanCount()) : OptionalLong.empty(),
          scalar.getLogicalType(),
          scalar.hasMin() ? Optional.of(scalar.getMin()) : Optional.empty(),
          scalar.hasMax() ? Optional.of(scalar.getMax()) : Optional.empty(),
          scalar.hasNdv() ? scalar.getNdv() : null);
    }

    private record TableStatsViewImpl(
        ResourceId tableId, long snapshotId, OptionalLong rowCount, OptionalLong totalSizeBytes)
        implements StatsProvider.TableStatsView {
      @Override
      public OptionalLong rowCountValue() {
        return rowCount;
      }

      @Override
      public OptionalLong totalSizeBytesValue() {
        return totalSizeBytes;
      }
    }

    private record ColumnStatsViewImpl(
        ResourceId tableId,
        long columnId,
        String columnName,
        long valueCount,
        OptionalLong nullCount,
        OptionalLong nanCount,
        String logicalType,
        Optional<String> min,
        Optional<String> max,
        Ndv ndvValue)
        implements StatsProvider.ColumnStatsView {
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
        return Optional.ofNullable(ndvValue);
      }
    }
  }

  static RelationStats toRelationStats(StatsProvider.TableStatsView stats) {
    RelationStats.Builder builder = RelationStats.newBuilder();
    stats.rowCountValue().ifPresent(builder::setRowCount);
    stats.totalSizeBytesValue().ifPresent(builder::setTotalSizeBytes);
    return builder.build();
  }
}
