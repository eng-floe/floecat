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
import ai.floedb.floecat.query.rpc.ColumnStatsBatch;
import ai.floedb.floecat.query.rpc.ColumnStatsBundleChunk;
import ai.floedb.floecat.query.rpc.ColumnStatsBundleEnd;
import ai.floedb.floecat.query.rpc.ColumnStatsBundleHeader;
import ai.floedb.floecat.query.rpc.ColumnStatsInfo;
import ai.floedb.floecat.query.rpc.ColumnStatsResult;
import ai.floedb.floecat.query.rpc.FetchColumnStatsRequest;
import ai.floedb.floecat.query.rpc.StatsFailure;
import ai.floedb.floecat.query.rpc.StatsStatus;
import ai.floedb.floecat.query.rpc.StatsWarning;
import ai.floedb.floecat.query.rpc.TableColumnStatsRequest;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.service.repo.impl.StatsRepository;
import ai.floedb.floecat.service.repo.impl.StatsRepository.ColumnStatsBatchResult;
import ai.floedb.floecat.systemcatalog.spi.scanner.StatsProvider;
import io.smallrye.mutiny.Multi;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.OptionalLong;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@ApplicationScoped
public class PlannerStatsBundleService {

  private static final Logger LOG = Logger.getLogger(PlannerStatsBundleService.class);

  private static final String PIN_MISSING_CODE = "planner_stats.pin.missing";
  private static final String COLUMN_MISSING_CODE = "planner_stats.column_stats.missing";
  private static final String COLUMN_ERROR_CODE = "planner_stats.column_stats.error";
  private static final String SCAN_CAPPED_CODE = "planner_stats.column_stats.scan_capped";

  private final StatsProviderFactory statsFactory;
  private final StatsRepository repository;
  private final int maxTables;
  private final int maxColumns;
  private final int maxResultsPerChunk;

  private static final record PlannerStatsLimits(
      int maxTables, int maxColumns, int maxResultsPerChunk) {}

  @Inject
  public PlannerStatsBundleService(
      StatsProviderFactory statsFactory,
      StatsRepository repository,
      @ConfigProperty(name = "floecat.planner.stats.max-tables", defaultValue = "50") int maxTables,
      @ConfigProperty(name = "floecat.planner.stats.max-columns", defaultValue = "1024")
          int maxColumns,
      @ConfigProperty(name = "floecat.planner.stats.max-results-per-chunk", defaultValue = "100")
          int maxResultsPerChunk) {
    this(
        statsFactory,
        repository,
        new PlannerStatsLimits(maxTables, maxColumns, maxResultsPerChunk));
  }

  PlannerStatsBundleService(
      StatsProviderFactory statsFactory, StatsRepository repository, PlannerStatsLimits limits) {
    this.statsFactory = Objects.requireNonNull(statsFactory, "statsFactory");
    this.repository = Objects.requireNonNull(repository, "repository");
    this.maxTables = Math.max(1, limits.maxTables);
    this.maxColumns = Math.max(1, limits.maxColumns);
    this.maxResultsPerChunk = Math.max(1, limits.maxResultsPerChunk);
  }

  public static PlannerStatsBundleService forTesting(
      StatsProviderFactory statsFactory,
      StatsRepository repository,
      int maxTables,
      int maxColumns,
      int maxResultsPerChunk) {
    return new PlannerStatsBundleService(
        statsFactory,
        repository,
        new PlannerStatsLimits(maxTables, maxColumns, maxResultsPerChunk));
  }

  public Multi<ColumnStatsBundleChunk> stream(
      String correlationId, QueryContext ctx, FetchColumnStatsRequest request) {
    FetchColumnStatsRequest safeRequest =
        request == null ? FetchColumnStatsRequest.getDefaultInstance() : request;
    NormalizedRequest normalized = normalizeRequest(correlationId, safeRequest);
    List<TableRequest> tableRequests = normalized.tables();
    return Multi.createFrom()
        .<ColumnStatsBundleChunk>deferred(
            () ->
                Multi.createFrom()
                    .iterable(
                        () ->
                            new PlannerStatsIterator(
                                ctx.getQueryId(),
                                correlationId,
                                tableRequests,
                                statsFactory.forQuery(ctx, correlationId),
                                repository,
                                maxResultsPerChunk,
                                tableRequests.size(),
                                normalized.requestedColumns())));
  }

  private NormalizedRequest normalizeRequest(
      String correlationId, FetchColumnStatsRequest request) {
    if (request.getTablesCount() == 0) {
      throw GrpcErrors.invalidArgument(
          correlationId, "planner_stats.request.tables.missing", Map.of());
    }
    if (request.getTablesCount() > maxTables) {
      throw GrpcErrors.invalidArgument(
          correlationId,
          "planner_stats.request.tables.limit",
          Map.of("max_tables", Integer.toString(maxTables)));
    }

    List<TableRequest> normalized = new ArrayList<>(request.getTablesCount());
    long totalColumns = 0;

    for (int tableIndex = 0; tableIndex < request.getTablesCount(); tableIndex++) {
      TableColumnStatsRequest table = request.getTables(tableIndex);
      if (!table.hasTableId()) {
        throw GrpcErrors.invalidArgument(
            correlationId,
            "planner_stats.request.table_id.missing",
            Map.of("table_index", Integer.toString(tableIndex)));
      }
      if (table.getColumnIdsCount() == 0) {
        throw GrpcErrors.invalidArgument(
            correlationId,
            "planner_stats.request.columns.missing",
            Map.of("table_id", table.getTableId().getId()));
      }

      List<Long> deduped =
          dedupeColumnIds(correlationId, table.getTableId().getId(), table.getColumnIdsList());
      if (deduped.isEmpty()) {
        throw GrpcErrors.invalidArgument(
            correlationId,
            "planner_stats.request.columns.missing",
            Map.of("table_id", table.getTableId().getId()));
      }

      normalized.add(new TableRequest(table.getTableId(), List.copyOf(deduped)));
      totalColumns += deduped.size();
      if (totalColumns > maxColumns) {
        throw GrpcErrors.invalidArgument(
            correlationId,
            "planner_stats.request.columns.limit",
            Map.of("max_columns", Integer.toString(maxColumns)));
      }
    }

    return new NormalizedRequest(List.copyOf(normalized), totalColumns);
  }

  private static List<Long> dedupeColumnIds(
      String correlationId, String tableId, List<Long> columnIds) {
    LinkedHashSet<Long> seen = new LinkedHashSet<>(columnIds.size());
    for (Long id : columnIds) {
      if (id == null || id <= 0) {
        throw GrpcErrors.invalidArgument(
            correlationId,
            "planner_stats.request.column_id.invalid",
            Map.of("table_id", tableId, "column_id", id == null ? "null" : Long.toString(id)));
      }
      seen.add(id);
    }
    return new ArrayList<>(seen);
  }

  private static final class PlannerStatsIterator implements Iterator<ColumnStatsBundleChunk> {

    private final String queryId;
    private final String correlationId;
    private final List<TableWork> tableWorks;
    private final StatsProvider statsProvider;
    private final StatsRepository repository;
    private final int maxResultsPerChunk;
    private final long requestedTables;
    private final long requestedColumns;

    private int nextTableIndex = 0;
    private long seq = 1;
    private long returnedColumns = 0;
    private long notFoundColumns = 0;
    private long errorColumns = 0;
    private boolean headerEmitted = false;
    private boolean endEmitted = false;
    private final List<StatsWarning> pendingWarnings = new ArrayList<>();

    private PlannerStatsIterator(
        String queryId,
        String correlationId,
        List<TableRequest> tables,
        StatsProvider statsProvider,
        StatsRepository repository,
        int maxResultsPerChunk,
        long requestedTables,
        long requestedColumns) {
      this.queryId = queryId;
      this.correlationId = correlationId;
      this.statsProvider = statsProvider;
      this.repository = repository;
      this.maxResultsPerChunk = maxResultsPerChunk;
      this.requestedTables = requestedTables;
      this.requestedColumns = requestedColumns;
      this.tableWorks = new ArrayList<>(tables.size());
      for (TableRequest table : tables) {
        this.tableWorks.add(new TableWork(table.tableId(), table.columnIds()));
      }
    }

    @Override
    public boolean hasNext() {
      return !endEmitted;
    }

    @Override
    public ColumnStatsBundleChunk next() {
      if (!headerEmitted) {
        headerEmitted = true;
        return headerChunk();
      }

      if (hasMoreWork()) {
        return batchChunk();
      }

      if (!endEmitted) {
        endEmitted = true;
        return endChunk();
      }

      throw new NoSuchElementException();
    }

    private boolean hasMoreWork() {
      return nextTableIndex < tableWorks.size();
    }

    private ColumnStatsBundleChunk headerChunk() {
      ColumnStatsBundleHeader header = ColumnStatsBundleHeader.newBuilder().build();
      return ColumnStatsBundleChunk.newBuilder()
          .setQueryId(queryId)
          .setSeq(seq++)
          .setHeader(header)
          .build();
    }

    private ColumnStatsBundleChunk batchChunk() {
      List<ColumnStatsResult> out = new ArrayList<>(Math.min(maxResultsPerChunk, 16));
      while (out.size() < maxResultsPerChunk && hasMoreWork()) {
        TableWork work = tableWorks.get(nextTableIndex);
        ColumnStatsResult result = processNextColumn(work);
        out.add(result);
        if (!work.hasMoreColumns()) {
          nextTableIndex++;
        }
      }
      ColumnStatsBatch.Builder batchBuilder = ColumnStatsBatch.newBuilder().addAllColumns(out);
      if (!pendingWarnings.isEmpty()) {
        batchBuilder.addAllWarnings(pendingWarnings);
        pendingWarnings.clear();
      }
      ColumnStatsBatch batch = batchBuilder.build();
      return ColumnStatsBundleChunk.newBuilder()
          .setQueryId(queryId)
          .setSeq(seq++)
          .setBatch(batch)
          .build();
    }

    private ColumnStatsResult processNextColumn(TableWork work) {
      long columnId = work.columnIdAt(work.columnIndex());
      ResourceId tableId = work.tableId;
      OptionalLong snapshot = resolveSnapshot(work);
      ColumnStatsResult result;
      if (snapshot.isEmpty()) {
        errorColumns++;
        result = pinMissingResult(tableId, columnId);
      } else {
        ColumnStats stats = lookupStats(work, snapshot.getAsLong(), columnId);
        if (stats != null) {
          returnedColumns++;
          result = buildFoundResult(tableId, columnId, stats);
        } else if (work.statsError != null) {
          errorColumns++;
          result = buildErrorResult(tableId, columnId, snapshot.getAsLong(), work.statsError);
        } else {
          notFoundColumns++;
          result =
              notFoundResult(
                  tableId, columnId, COLUMN_MISSING_CODE, "column stats missing", snapshot);
        }
      }
      work.advanceColumn();
      return result;
    }

    private OptionalLong resolveSnapshot(TableWork work) {
      if (work.pinResolved) {
        return work.pinnedSnapshot;
      }
      work.pinnedSnapshot = statsProvider.pinnedSnapshotId(work.tableId);
      work.pinResolved = true;
      return work.pinnedSnapshot;
    }

    private ColumnStats lookupStats(TableWork work, long snapshotId, long columnId) {
      if (work.statsByColumn == null && work.statsError == null) {
        loadStats(work, snapshotId);
      }
      if (work.statsError != null) {
        return null;
      }
      return work.statsByColumn.get(columnId);
    }

    private void loadStats(TableWork work, long snapshotId) {
      try {
        ColumnStatsBatchResult batch =
            repository.getColumnStatsBatchSmart(work.tableId, snapshotId, work.columnIds);
        work.statsByColumn = batch.stats();
        if (batch.capped()) {
          recordScanWarning(work, snapshotId, batch);
        }
      } catch (RuntimeException e) {
        work.statsError = e;
        work.statsByColumn = Map.of();
        LOG.debugf(e, "column stats lookup failed for %s snapshot %s", work.tableId, snapshotId);
      }
    }

    private void recordScanWarning(TableWork work, long snapshotId, ColumnStatsBatchResult batch) {
      if (!batch.capped()) {
        return;
      }
      StatsWarning warning =
          StatsWarning.newBuilder()
              .setCode(SCAN_CAPPED_CODE)
              .setMessage("column stats scan hit the configured page cap")
              .putDetails("table_id", work.tableId.getId())
              .putDetails("snapshot_id", Long.toString(snapshotId))
              .putDetails("pages_scanned", Integer.toString(batch.pagesScanned()))
              .putDetails("scan_limit", Integer.toString(repository.maxScanPages()))
              .putDetails("columns_requested", Integer.toString(work.columnIds.size()))
              .putDetails("columns_found", Integer.toString(batch.stats().size()))
              .build();
      pendingWarnings.add(warning);
    }

    private ColumnStatsResult buildFoundResult(
        ResourceId tableId, long columnId, ColumnStats stats) {
      StatsProvider.ColumnStatsView view = StatsProviderViews.columnStatsView(stats);
      ColumnStatsInfo.Builder info =
          ColumnStatsInfo.newBuilder()
              .setValueCount(stats.getValueCount())
              .setLogicalType(view.logicalType())
              .setTdigest(stats.getTdigest())
              .setHistogram(stats.getHistogram());

      view.nullCountValue().ifPresent(info::setNullCount);
      view.nanCountValue().ifPresent(info::setNanCount);
      view.minValue().ifPresent(info::setMin);
      view.maxValue().ifPresent(info::setMax);
      view.ndv().ifPresent(info::setNdv);

      ColumnStatsResult.Builder builder =
          ColumnStatsResult.newBuilder()
              .setTableId(tableId)
              .setColumnId(columnId)
              .setColumnName(view.columnName())
              .setStatus(StatsStatus.STATS_STATUS_FOUND)
              .setStats(info.build());
      return builder.build();
    }

    private ColumnStatsResult buildErrorResult(
        ResourceId tableId, long columnId, long snapshotId, RuntimeException e) {
      StatsFailure failure =
          StatsFailure.newBuilder()
              .setCode(COLUMN_ERROR_CODE)
              .setMessage("column stats lookup failed")
              .putDetails("table_id", tableId.getId())
              .putDetails("account_id", tableId.getAccountId())
              .putDetails("column_id", Long.toString(columnId))
              .putDetails("snapshot_id", Long.toString(snapshotId))
              .putDetails("exception", e.getClass().getSimpleName())
              .putDetails("message", e.getMessage() == null ? "" : e.getMessage())
              .build();
      return ColumnStatsResult.newBuilder()
          .setTableId(tableId)
          .setColumnId(columnId)
          .setStatus(StatsStatus.STATS_STATUS_ERROR)
          .setFailure(failure)
          .build();
    }

    private ColumnStatsResult notFoundResult(
        ResourceId tableId, long columnId, String code, String message, OptionalLong snapshotId) {
      StatsFailure failure =
          StatsFailure.newBuilder()
              .setCode(code)
              .setMessage(message)
              .putDetails("table_id", tableId.getId())
              .putDetails("account_id", tableId.getAccountId())
              .putDetails("column_id", Long.toString(columnId))
              .build();
      if (snapshotId.isPresent()) {
        failure =
            failure.toBuilder()
                .putDetails("snapshot_id", Long.toString(snapshotId.getAsLong()))
                .build();
      }
      return ColumnStatsResult.newBuilder()
          .setTableId(tableId)
          .setColumnId(columnId)
          .setStatus(StatsStatus.STATS_STATUS_NOT_FOUND)
          .setFailure(failure)
          .build();
    }

    private ColumnStatsResult pinMissingResult(ResourceId tableId, long columnId) {
      StatsFailure failure =
          StatsFailure.newBuilder()
              .setCode(PIN_MISSING_CODE)
              .setMessage("snapshot pin missing")
              .putDetails("table_id", tableId.getId())
              .putDetails("account_id", tableId.getAccountId())
              .putDetails("column_id", Long.toString(columnId))
              .build();
      return ColumnStatsResult.newBuilder()
          .setTableId(tableId)
          .setColumnId(columnId)
          .setStatus(StatsStatus.STATS_STATUS_ERROR)
          .setFailure(failure)
          .build();
    }

    private ColumnStatsBundleChunk endChunk() {
      ColumnStatsBundleEnd end =
          ColumnStatsBundleEnd.newBuilder()
              .setRequestedTables(requestedTables)
              .setRequestedColumns(requestedColumns)
              .setReturnedColumns(returnedColumns)
              .setNotFoundColumns(notFoundColumns)
              .setErrorColumns(errorColumns)
              .build();
      return ColumnStatsBundleChunk.newBuilder()
          .setQueryId(queryId)
          .setSeq(seq++)
          .setEnd(end)
          .build();
    }
  }

  private static final class TableWork {
    private final ResourceId tableId;
    private final List<Long> columnIds;
    private OptionalLong pinnedSnapshot = OptionalLong.empty();
    private boolean pinResolved = false;
    private Map<Long, ColumnStats> statsByColumn;
    private RuntimeException statsError;
    private int columnIndex = 0;

    private TableWork(ResourceId tableId, List<Long> columnIds) {
      this.tableId = tableId;
      this.columnIds = columnIds;
    }

    private boolean hasMoreColumns() {
      return columnIndex < columnIds.size();
    }

    private long columnIdAt(int index) {
      return columnIds.get(index);
    }

    private void advanceColumn() {
      columnIndex++;
    }

    private int columnIndex() {
      return columnIndex;
    }
  }

  private record TableRequest(ResourceId tableId, List<Long> columnIds) {}

  private record NormalizedRequest(List<TableRequest> tables, long requestedColumns) {}
}
