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
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.query.rpc.RelationStats;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.service.repo.impl.StatsRepository;
import ai.floedb.floecat.systemcatalog.spi.scanner.StatsProvider;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.jboss.logging.Logger;

@ApplicationScoped
public final class StatsProviderFactory {

  private static final Logger LOG = Logger.getLogger(StatsProviderFactory.class);

  private final StatsRepository repository;
  private final QueryContextStore queryStore;

  private record TableKey(String accountId, String relationId, int kindValue, long snapshotId) {}

  @Inject
  public StatsProviderFactory(StatsRepository repository, QueryContextStore queryStore) {
    this.repository = repository;
    this.queryStore = queryStore;
  }

  public StatsProvider forQuery(QueryContext ctx, String correlationId) {
    return new CachedStatsProvider(repository, queryStore, ctx, correlationId);
  }

  private static final class CachedStatsProvider implements StatsProvider {

    private final StatsRepository repository;
    private final QueryContextStore queryStore;
    private final QueryContext initialCtx;
    private final String queryId;
    private final String correlationId;
    private final ConcurrentMap<TableKey, Optional<StatsProvider.TableStatsView>> tableCache =
        new ConcurrentHashMap<>();

    private CachedStatsProvider(
        StatsRepository repository,
        QueryContextStore queryStore,
        QueryContext ctx,
        String correlationId) {
      this.repository = repository;
      this.queryStore = queryStore;
      this.initialCtx = ctx;
      this.queryId = ctx.getQueryId();
      this.correlationId = correlationId;
    }

    @Override
    public Optional<StatsProvider.TableStatsView> tableStats(ResourceId tableId) {
      return liveContext()
          .findSnapshotPin(tableId, correlationId)
          .flatMap(
              pin -> {
                long snapshotId = pin.getSnapshotId();
                TableKey key =
                    new TableKey(
                        tableId.getAccountId(),
                        tableId.getId(),
                        tableId.getKindValue(),
                        snapshotId);
                return tableCache.computeIfAbsent(key, k -> safeTableStats(tableId, snapshotId));
              });
    }

    @Override
    public Optional<StatsProvider.ColumnStatsView> columnStats(ResourceId tableId, long columnId) {
      return liveContext()
          .findSnapshotPin(tableId, correlationId)
          .flatMap(pin -> safeColumnStats(tableId, pin.getSnapshotId(), columnId));
    }

    private QueryContext liveContext() {
      return queryStore.get(queryId).orElse(initialCtx);
    }

    private Optional<StatsProvider.TableStatsView> safeTableStats(
        ResourceId tableId, long snapshotId) {
      try {
        return repository
            .getTableStatsView(tableId, snapshotId)
            .map(StatsProviderFactory::toTableStatsView);
      } catch (RuntimeException e) {
        LOG.debugf(e, "table stats lookup failed for %s snapshot %s", tableId, snapshotId);
        return Optional.empty();
      }
    }

    private Optional<StatsProvider.ColumnStatsView> safeColumnStats(
        ResourceId tableId, long snapshotId, long columnId) {
      try {
        return repository
            .getColumnStats(tableId, snapshotId, columnId)
            .map(StatsProviderFactory::toColumnStatsView);
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
  }

  private static StatsProvider.TableStatsView toTableStatsView(
      StatsRepository.TableStatsView stats) {
    return new TableStatsViewImpl(
        stats.tableId(), stats.snapshotId(), stats.rowCount(), stats.totalSizeBytes());
  }

  private static StatsProvider.ColumnStatsView toColumnStatsView(ColumnStats stats) {
    return new ColumnStatsViewImpl(
        stats.getTableId(),
        stats.getColumnId(),
        stats.getColumnName(),
        stats.getValueCount(),
        stats.getNullCount());
  }

  private static final class TableStatsViewImpl implements StatsProvider.TableStatsView {
    private final ResourceId tableId;
    private final long snapshotId;
    private final long rowCount;
    private final long totalSizeBytes;

    private TableStatsViewImpl(
        ResourceId tableId, long snapshotId, long rowCount, long totalSizeBytes) {
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
    public long rowCount() {
      return rowCount;
    }

    @Override
    public long totalSizeBytes() {
      return totalSizeBytes;
    }
  }

  private static final class ColumnStatsViewImpl implements StatsProvider.ColumnStatsView {
    private final ResourceId tableId;
    private final long columnId;
    private final String columnName;
    private final long valueCount;
    private final long nullCount;

    private ColumnStatsViewImpl(
        ResourceId tableId, long columnId, String columnName, long valueCount, long nullCount) {
      this.tableId = tableId;
      this.columnId = columnId;
      this.columnName = columnName;
      this.valueCount = valueCount;
      this.nullCount = nullCount;
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
    public String columnName() {
      return columnName;
    }

    @Override
    public long valueCount() {
      return valueCount;
    }

    @Override
    public long nullCount() {
      return nullCount;
    }
  }

  static RelationStats toRelationStats(StatsProvider.TableStatsView stats) {
    return RelationStats.newBuilder()
        .setRowCount(stats.rowCount())
        .setTotalSizeBytes(stats.totalSizeBytes())
        .build();
  }
}
