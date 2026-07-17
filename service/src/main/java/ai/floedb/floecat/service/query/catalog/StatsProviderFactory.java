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
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TableStatsTarget;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.query.rpc.RelationStats;
import ai.floedb.floecat.scanner.spi.StatsProvider;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.statistics.StatsOrchestrator;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsExecutionMode;
import ai.floedb.floecat.stats.spi.StatsResolutionResult;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.jboss.logging.Logger;

@ApplicationScoped
public final class StatsProviderFactory {

  private static final Logger LOG = Logger.getLogger(StatsProviderFactory.class);

  private final StatsOrchestrator statsOrchestrator;
  private final TableRepository tableRepository;
  private final QueryContextStore queryStore;
  private final SnapshotRepository snapshotRepository;
  private final Duration syncLatencyBudget;
  private final Duration syncMaxLatencyBudget;
  private final boolean syncEnabled;

  @Inject
  public StatsProviderFactory(
      StatsOrchestrator statsOrchestrator,
      TableRepository tableRepository,
      QueryContextStore queryStore,
      SnapshotRepository snapshotRepository,
      StatsProviderFactoryConfig config) {
    this.statsOrchestrator = statsOrchestrator;
    this.tableRepository = tableRepository;
    this.queryStore = queryStore;
    this.snapshotRepository = snapshotRepository;
    this.syncMaxLatencyBudget = config.syncMaxLatencyBudget();
    this.syncLatencyBudget = clampToMax(config.syncLatencyBudget(), syncMaxLatencyBudget);
    this.syncEnabled = config.syncEnabled();
  }

  public StatsProviderFactory(
      StatsOrchestrator statsOrchestrator,
      TableRepository tableRepository,
      QueryContextStore queryStore) {
    this(statsOrchestrator, tableRepository, queryStore, null, defaultConfig());
  }

  public StatsProvider forQuery(QueryContext ctx, String correlationId) {
    return new CachedStatsProvider(
        statsOrchestrator,
        tableRepository,
        queryStore,
        snapshotRepository,
        ctx,
        correlationId,
        false,
        syncLatencyBudget,
        syncEnabled);
  }

  public StatsProvider forSystemScan(QueryContext ctx, String correlationId) {
    return new CachedStatsProvider(
        statsOrchestrator,
        tableRepository,
        queryStore,
        snapshotRepository,
        ctx,
        correlationId,
        true,
        syncLatencyBudget,
        syncEnabled);
  }

  SnapshotPinLookup pinLookupForQuery(QueryContext ctx, String correlationId) {
    return new SnapshotPinResolver(queryStore, ctx, correlationId);
  }

  private static final class CachedStatsProvider implements StatsProvider {

    private final StatsOrchestrator statsOrchestrator;
    private final TableRepository tableRepository;
    private final SnapshotRepository snapshotRepository;
    private final SnapshotPinResolver pinResolver;
    private final String correlationId;
    private final boolean allowUnpinnedLatestSnapshotFallback;
    private final Duration syncLatencyBudget;
    private final boolean syncEnabled;
    private final ConcurrentMap<SnapshotScopedRelationKey, Optional<StatsProvider.TableStatsView>>
        tableCache = new ConcurrentHashMap<>();
    private final ConcurrentMap<ResourceId, OptionalLong> latestSnapshotCache =
        new ConcurrentHashMap<>();

    private CachedStatsProvider(
        StatsOrchestrator statsOrchestrator,
        TableRepository tableRepository,
        QueryContextStore queryStore,
        SnapshotRepository snapshotRepository,
        QueryContext ctx,
        String correlationId,
        boolean allowUnpinnedLatestSnapshotFallback,
        Duration syncLatencyBudget,
        boolean syncEnabled) {
      this.statsOrchestrator = statsOrchestrator;
      this.tableRepository = tableRepository;
      this.snapshotRepository = snapshotRepository;
      this.correlationId = correlationId == null ? "" : correlationId;
      this.syncLatencyBudget = syncLatencyBudget;
      this.syncEnabled = syncEnabled;
      this.pinResolver = new SnapshotPinResolver(queryStore, ctx, correlationId);
      this.allowUnpinnedLatestSnapshotFallback = allowUnpinnedLatestSnapshotFallback;
    }

    // Generation policy: these lookups scope stats to the pin's SNAPSHOT and read the pin's frozen
    // stats generation FIRST via resolveInGeneration — the same generation the scan uses — so plans
    // are stable and consistent with the scan. The newest (live active) generation only fills a
    // target the pinned generation lacks, so an incomplete pinned generation never yields
    // NOT_FOUND; an unpinned read uses the live/newest generation directly. The SCAN stays
    // authoritative for results (it freezes stats_generation_ref_uri); a planner/scan generation
    // difference affects estimation only, never correctness.
    @Override
    public Optional<StatsProvider.TableStatsView> tableStats(ResourceId tableId) {
      if (allowUnpinnedLatestSnapshotFallback) {
        return latestSnapshotTableStats(tableId);
      }
      Optional<StatsProvider.TableStatsView> pinnedStats =
          pinResolver.withPinnedSnapshot(
              tableId,
              snapshotId ->
                  tableCache.computeIfAbsent(
                      SnapshotScopedRelationKey.of(tableId, snapshotId),
                      key -> safeTableStats(tableId, snapshotId)));
      if (pinnedStats.isPresent()) {
        return pinnedStats;
      }
      return Optional.empty();
    }

    private Optional<StatsProvider.TableStatsView> latestSnapshotTableStats(ResourceId tableId) {
      if (!allowUnpinnedLatestSnapshotFallback || tableId == null || snapshotRepository == null) {
        return Optional.empty();
      }
      OptionalLong latestSnapshotId =
          latestSnapshotCache.computeIfAbsent(tableId, id -> resolveLatestSnapshotId(id));
      if (latestSnapshotId.isEmpty()) {
        return Optional.empty();
      }
      long snapshotId = latestSnapshotId.getAsLong();
      return tableCache.computeIfAbsent(
          SnapshotScopedRelationKey.of(tableId, snapshotId),
          key -> safeTableStats(tableId, snapshotId));
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

    private OptionalLong resolveLatestSnapshotId(ResourceId tableId) {
      try {
        // "Latest" means latest QUERY-VISIBLE: the repository's default current-snapshot read is
        // gate-aware, so system scans agree with what queries can read.
        Optional<Snapshot> snapshot = snapshotRepository.getCurrentSnapshot(tableId);
        if (snapshot.isPresent()) {
          return OptionalLong.of(snapshot.get().getSnapshotId());
        }
      } catch (RuntimeException e) {
        LOG.debugf(e, "latest snapshot lookup failed for %s", tableId);
      }
      return OptionalLong.empty();
    }

    private Optional<StatsProvider.TableStatsView> safeTableStats(
        ResourceId tableId, long snapshotId) {
      try {
        var request =
            StatsCaptureRequest.builder(
                    tableId,
                    snapshotId,
                    StatsTarget.newBuilder()
                        .setTable(TableStatsTarget.getDefaultInstance())
                        .build())
                .columnSelectors(Set.of())
                // Empty requested kinds means "accept any available stat family".
                .requestedKinds(Set.of())
                .executionMode(syncEnabled ? StatsExecutionMode.SYNC : StatsExecutionMode.ASYNC)
                .connectorType(connectorTypeFor(tableId))
                .correlationId(correlationId)
                .latencyBudget(syncEnabled ? Optional.of(syncLatencyBudget) : Optional.empty())
                .build();
        StatsResolutionResult result =
            statsOrchestrator.resolveInGeneration(
                request, pinResolver.pinnedStatsGenerationRef(tableId));
        return result
            .stats()
            .filter(TargetStatsRecord::hasTable)
            .map(CachedStatsProvider::toTableStatsView);
      } catch (RuntimeException e) {
        LOG.debugf(e, "table stats lookup failed for %s snapshot %s", tableId, snapshotId);
        return Optional.empty();
      }
    }

    private Optional<StatsProvider.ColumnStatsView> safeColumnStats(
        ResourceId tableId, long snapshotId, long columnId) {
      try {
        var request =
            StatsCaptureRequest.builder(
                    tableId, snapshotId, StatsTargetIdentity.columnTarget(columnId))
                .columnSelectors(Set.of())
                // Empty requested kinds means "accept any available stat family".
                .requestedKinds(Set.of())
                .executionMode(syncEnabled ? StatsExecutionMode.SYNC : StatsExecutionMode.ASYNC)
                .connectorType(connectorTypeFor(tableId))
                .correlationId(correlationId)
                .latencyBudget(syncEnabled ? Optional.of(syncLatencyBudget) : Optional.empty())
                .build();
        StatsResolutionResult result =
            statsOrchestrator.resolveInGeneration(
                request, pinResolver.pinnedStatsGenerationRef(tableId));
        return result
            .stats()
            .filter(TargetStatsRecord::hasScalar)
            .map(CachedStatsProvider::toColumnStatsView);
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

    private String connectorTypeFor(ResourceId tableId) {
      return ConnectorTypeResolver.connectorTypeFor(tableRepository, tableId);
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
      // Estimate-less envelopes are sketch carriers only (see the shared
      // PlannerStatsResultMaterializer.hasEstimate mode gate) — never a zero-NDV estimate.
      Ndv estimatedNdv =
          scalar.hasNdv() && PlannerStatsResultMaterializer.hasEstimate(scalar.getNdv())
              ? scalar.getNdv()
              : null;
      return new ColumnStatsViewImpl(
          record.getTableId(),
          record.getTarget().getColumn().getColumnId(),
          scalar.getDisplayName(),
          scalar.getRowCount(),
          scalar.hasNullCount() ? OptionalLong.of(scalar.getNullCount()) : OptionalLong.empty(),
          scalar.hasNanCount() ? OptionalLong.of(scalar.getNanCount()) : OptionalLong.empty(),
          scalar.getLogicalType(),
          scalar.hasMin() ? Optional.of(scalar.getMin()) : Optional.empty(),
          scalar.hasMax() ? Optional.of(scalar.getMax()) : Optional.empty(),
          estimatedNdv,
          scalar.hasAvgWidthBytes()
              ? OptionalLong.of(scalar.getAvgWidthBytes())
              : OptionalLong.empty());
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
        long rowCount,
        OptionalLong nullCount,
        OptionalLong nanCount,
        String logicalType,
        Optional<String> min,
        Optional<String> max,
        Ndv ndvValue,
        OptionalLong avgWidth)
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

      @Override
      public OptionalLong avgWidthBytes() {
        return avgWidth;
      }
    }
  }

  static RelationStats toRelationStats(StatsProvider.TableStatsView stats) {
    RelationStats.Builder builder = RelationStats.newBuilder();
    stats.rowCountValue().ifPresent(builder::setRowCount);
    stats.totalSizeBytesValue().ifPresent(builder::setTotalSizeBytes);
    return builder.build();
  }

  @ConfigMapping(prefix = "floecat.stats.sync")
  interface StatsProviderFactoryConfig {
    @WithDefault("1s")
    Duration latencyBudget();

    @WithDefault("true")
    boolean enabled();

    @WithDefault("10s")
    Duration maxLatencyBudget();

    default Duration syncLatencyBudget() {
      return latencyBudget();
    }

    default boolean syncEnabled() {
      return enabled();
    }

    default Duration syncMaxLatencyBudget() {
      return maxLatencyBudget();
    }
  }

  private static StatsProviderFactoryConfig defaultConfig() {
    return new StatsProviderFactoryConfig() {
      @Override
      public Duration latencyBudget() {
        return Duration.ofSeconds(1);
      }

      @Override
      public boolean enabled() {
        return true;
      }

      @Override
      public Duration maxLatencyBudget() {
        return Duration.ofSeconds(10);
      }
    };
  }

  private static Duration clampToMax(Duration requested, Duration max) {
    if (requested == null) {
      return max;
    }
    if (max == null || max.isZero() || max.isNegative()) {
      return requested;
    }
    return requested.compareTo(max) > 0 ? max : requested;
  }
}
