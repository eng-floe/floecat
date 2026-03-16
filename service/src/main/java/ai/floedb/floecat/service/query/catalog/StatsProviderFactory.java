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

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.query.rpc.RelationStats;
import ai.floedb.floecat.scanner.spi.StatsProvider;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.service.repo.impl.StatsRepository;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.jboss.logging.Logger;

@ApplicationScoped
public final class StatsProviderFactory {

  private static final Logger LOG = Logger.getLogger(StatsProviderFactory.class);

  private final StatsRepository repository;
  private final QueryContextStore queryStore;

  @Inject
  public StatsProviderFactory(StatsRepository repository, QueryContextStore queryStore) {
    this.repository = repository;
    this.queryStore = queryStore;
  }

  public StatsProvider forQuery(QueryContext ctx, String correlationId) {
    return new CachedStatsProvider(repository, queryStore, ctx, correlationId);
  }

  SnapshotPinLookup pinLookupForQuery(QueryContext ctx, String correlationId) {
    return new SnapshotPinResolver(queryStore, ctx, correlationId);
  }

  private static final class CachedStatsProvider implements StatsProvider {

    private final StatsRepository repository;
    private final SnapshotPinResolver pinResolver;
    private final ConcurrentMap<SnapshotScopedRelationKey, Optional<StatsProvider.TableStatsView>>
        tableCache = new ConcurrentHashMap<>();

    private CachedStatsProvider(
        StatsRepository repository,
        QueryContextStore queryStore,
        QueryContext ctx,
        String correlationId) {
      this.repository = repository;
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
        return repository
            .getTableStatsView(tableId, snapshotId)
            .map(StatsProviderViews::tableStatsView);
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
            .map(StatsProviderViews::columnStatsView);
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

  // Implementation provided by StatsProviderViews.

  static RelationStats toRelationStats(StatsProvider.TableStatsView stats) {
    RelationStats.Builder builder = RelationStats.newBuilder();
    stats.rowCountValue().ifPresent(builder::setRowCount);
    stats.totalSizeBytesValue().ifPresent(builder::setTotalSizeBytes);
    return builder.build();
  }
}
