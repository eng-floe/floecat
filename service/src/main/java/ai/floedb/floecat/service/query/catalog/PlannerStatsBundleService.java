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

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.*;

import ai.floedb.floecat.catalog.rpc.ColumnStats;
import ai.floedb.floecat.catalog.rpc.ConstraintDefinition;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.query.rpc.BundleFailure;
import ai.floedb.floecat.query.rpc.BundleResultStatus;
import ai.floedb.floecat.query.rpc.ColumnStatsBatch;
import ai.floedb.floecat.query.rpc.ColumnStatsBundleChunk;
import ai.floedb.floecat.query.rpc.ColumnStatsBundleEnd;
import ai.floedb.floecat.query.rpc.ColumnStatsBundleHeader;
import ai.floedb.floecat.query.rpc.ColumnStatsInfo;
import ai.floedb.floecat.query.rpc.ColumnStatsResult;
import ai.floedb.floecat.query.rpc.FetchColumnStatsRequest;
import ai.floedb.floecat.query.rpc.FetchTableConstraintsRequest;
import ai.floedb.floecat.query.rpc.StatsWarning;
import ai.floedb.floecat.query.rpc.TableColumnStatsRequest;
import ai.floedb.floecat.query.rpc.TableConstraintsBatch;
import ai.floedb.floecat.query.rpc.TableConstraintsBundleChunk;
import ai.floedb.floecat.query.rpc.TableConstraintsBundleEnd;
import ai.floedb.floecat.query.rpc.TableConstraintsBundleHeader;
import ai.floedb.floecat.query.rpc.TableConstraintsResult;
import ai.floedb.floecat.scanner.spi.ConstraintProvider;
import ai.floedb.floecat.scanner.spi.StatsProvider;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.service.repo.impl.StatsRepository;
import ai.floedb.floecat.service.repo.impl.StatsRepository.ColumnStatsBatchResult;
import io.smallrye.mutiny.Multi;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@ApplicationScoped
public class PlannerStatsBundleService {

  private static final Logger LOG = Logger.getLogger(PlannerStatsBundleService.class);

  private static final String PIN_MISSING_CODE = "planner_stats.pin.missing";
  private static final String COLUMN_MISSING_CODE = "planner_stats.column_stats.missing";
  private static final String COLUMN_ERROR_CODE = "planner_stats.column_stats.error";
  private static final String CONSTRAINT_MISSING_CODE = "planner_stats.constraints.missing";
  private static final String CONSTRAINT_ERROR_CODE = "planner_stats.constraints.error";
  private static final String SCAN_CAPPED_CODE = "planner_stats.column_stats.scan_capped";

  private final StatsProviderFactory statsFactory;
  private final Supplier<ConstraintProvider> constraintProviderSupplier;
  private final BiFunction<Set<String>, Map<String, Set<Long>>, ConstraintPruner>
      constraintPrunerFactory;
  private final Function<Set<String>, ConstraintPruner> constraintsOnlyPrunerFactory;
  private final StatsRepository repository;
  private final int maxTables;
  private final int maxColumns;
  private final int maxResultsPerChunk;

  private static final record PlannerStatsLimits(
      int maxTables, int maxColumns, int maxResultsPerChunk) {}

  @Inject
  public PlannerStatsBundleService(
      StatsProviderFactory statsFactory,
      ConstraintProviderFactory constraintFactory,
      ConstraintPrunerFactory constraintPrunerFactory,
      StatsRepository repository,
      @ConfigProperty(name = "floecat.planner.stats.max-tables", defaultValue = "50") int maxTables,
      @ConfigProperty(name = "floecat.planner.stats.max-columns", defaultValue = "1024")
          int maxColumns,
      @ConfigProperty(name = "floecat.planner.stats.max-results-per-chunk", defaultValue = "100")
          int maxResultsPerChunk) {
    this(
        statsFactory,
        constraintFactory::provider,
        constraintPrunerFactory::forRequest,
        constraintPrunerFactory::forConstraintsOnlyRequest,
        repository,
        new PlannerStatsLimits(maxTables, maxColumns, maxResultsPerChunk));
  }

  PlannerStatsBundleService(
      StatsProviderFactory statsFactory,
      Supplier<ConstraintProvider> constraintProviderSupplier,
      BiFunction<Set<String>, Map<String, Set<Long>>, ConstraintPruner> constraintPrunerFactory,
      Function<Set<String>, ConstraintPruner> constraintsOnlyPrunerFactory,
      StatsRepository repository,
      PlannerStatsLimits limits) {
    this.statsFactory = Objects.requireNonNull(statsFactory, "statsFactory");
    this.constraintProviderSupplier =
        Objects.requireNonNull(constraintProviderSupplier, "constraintProviderSupplier");
    this.constraintPrunerFactory =
        Objects.requireNonNull(constraintPrunerFactory, "constraintPrunerFactory");
    this.constraintsOnlyPrunerFactory =
        Objects.requireNonNull(constraintsOnlyPrunerFactory, "constraintsOnlyPrunerFactory");
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
    return forTesting(
        statsFactory,
        ConstraintProvider.NONE,
        repository,
        maxTables,
        maxColumns,
        maxResultsPerChunk);
  }

  public static PlannerStatsBundleService forTesting(
      StatsProviderFactory statsFactory,
      ConstraintProvider constraintProvider,
      StatsRepository repository,
      int maxTables,
      int maxColumns,
      int maxResultsPerChunk) {
    return new PlannerStatsBundleService(
        statsFactory,
        () -> constraintProvider == null ? ConstraintProvider.NONE : constraintProvider,
        RequestScopeConstraintPruner::new,
        RequestScopeConstraintPruner::forRequestedTablesOnly,
        repository,
        new PlannerStatsLimits(maxTables, maxColumns, maxResultsPerChunk));
  }

  public Multi<ColumnStatsBundleChunk> stream(
      String correlationId, QueryContext ctx, FetchColumnStatsRequest request) {
    FetchColumnStatsRequest safeRequest =
        request == null ? FetchColumnStatsRequest.getDefaultInstance() : request;
    NormalizedRequest normalized = normalizeRequest(correlationId, safeRequest);
    List<TableRequest> tableRequests = normalized.tables();
    ConstraintProvider constraintProvider =
        safeRequest.getIncludeConstraints()
            ? constraintProviderSupplier.get()
            : ConstraintProvider.NONE;
    SnapshotPinLookup pinLookup = statsFactory.pinLookupForQuery(ctx, correlationId);
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
                                pinLookup,
                                constraintProvider,
                                safeRequest.getIncludeConstraints(),
                                constraintPrunerFactory,
                                repository,
                                maxResultsPerChunk,
                                tableRequests.size(),
                                normalized.requestedColumns())));
  }

  public Multi<TableConstraintsBundleChunk> streamConstraints(
      String correlationId, QueryContext ctx, FetchTableConstraintsRequest request) {
    FetchTableConstraintsRequest safeRequest =
        request == null ? FetchTableConstraintsRequest.getDefaultInstance() : request;
    List<ResourceId> tableIds = normalizeConstraintsRequest(correlationId, safeRequest);
    Set<String> requestedRelationKeys = new LinkedHashSet<>();
    for (ResourceId tableId : tableIds) {
      requestedRelationKeys.add(RequestScopeConstraintPruner.relationKey(tableId));
    }
    ConstraintPruner constraintPruner =
        constraintsOnlyPrunerFactory.apply(Set.copyOf(requestedRelationKeys));
    ConstraintProvider constraintProvider = constraintProviderSupplier.get();
    SnapshotPinLookup pinLookup = statsFactory.pinLookupForQuery(ctx, correlationId);
    return Multi.createFrom()
        .<TableConstraintsBundleChunk>deferred(
            () ->
                Multi.createFrom()
                    .iterable(
                        () ->
                            new TableConstraintsIterator(
                                ctx.getQueryId(),
                                tableIds,
                                pinLookup,
                                constraintProvider,
                                constraintPruner,
                                maxResultsPerChunk)));
  }

  private NormalizedRequest normalizeRequest(
      String correlationId, FetchColumnStatsRequest request) {
    if (request.getTablesCount() == 0) {
      throw GrpcErrors.invalidArgument(
          correlationId, PLANNER_STATS_REQUEST_TABLES_MISSING, Map.of());
    }
    if (request.getTablesCount() > maxTables) {
      throw GrpcErrors.invalidArgument(
          correlationId,
          PLANNER_STATS_REQUEST_TABLES_LIMIT,
          Map.of("max_tables", Integer.toString(maxTables)));
    }

    List<TableRequest> normalized = new ArrayList<>(request.getTablesCount());
    long totalColumns = 0;

    for (int tableIndex = 0; tableIndex < request.getTablesCount(); tableIndex++) {
      TableColumnStatsRequest table = request.getTables(tableIndex);
      if (!table.hasTableId()) {
        throw GrpcErrors.invalidArgument(
            correlationId,
            PLANNER_STATS_REQUEST_TABLE_ID_MISSING,
            Map.of("table_index", Integer.toString(tableIndex)));
      }
      if (table.getColumnIdsCount() == 0) {
        throw GrpcErrors.invalidArgument(
            correlationId,
            PLANNER_STATS_REQUEST_COLUMNS_MISSING,
            Map.of("table_id", table.getTableId().getId()));
      }

      List<Long> deduped =
          dedupeColumnIds(correlationId, table.getTableId().getId(), table.getColumnIdsList());
      if (deduped.isEmpty()) {
        throw GrpcErrors.invalidArgument(
            correlationId,
            PLANNER_STATS_REQUEST_COLUMNS_MISSING,
            Map.of("table_id", table.getTableId().getId()));
      }

      normalized.add(new TableRequest(table.getTableId(), List.copyOf(deduped)));
      totalColumns += deduped.size();
      if (totalColumns > maxColumns) {
        throw GrpcErrors.invalidArgument(
            correlationId,
            PLANNER_STATS_REQUEST_COLUMNS_LIMIT,
            Map.of("max_columns", Integer.toString(maxColumns)));
      }
    }

    return new NormalizedRequest(List.copyOf(normalized), totalColumns);
  }

  private List<ResourceId> normalizeConstraintsRequest(
      String correlationId, FetchTableConstraintsRequest request) {
    if (request.getTableIdsCount() == 0) {
      throw GrpcErrors.invalidArgument(
          correlationId, PLANNER_STATS_REQUEST_TABLES_MISSING, Map.of());
    }
    if (request.getTableIdsCount() > maxTables) {
      throw GrpcErrors.invalidArgument(
          correlationId,
          PLANNER_STATS_REQUEST_TABLES_LIMIT,
          Map.of("max_tables", Integer.toString(maxTables)));
    }

    Map<String, ResourceId> deduped = new LinkedHashMap<>(request.getTableIdsCount());
    for (int tableIndex = 0; tableIndex < request.getTableIdsCount(); tableIndex++) {
      ResourceId tableId = request.getTableIds(tableIndex);
      if (tableId.getId().isBlank()) {
        throw GrpcErrors.invalidArgument(
            correlationId,
            PLANNER_STATS_REQUEST_TABLE_ID_MISSING,
            Map.of("table_index", Integer.toString(tableIndex)));
      }
      deduped.putIfAbsent(RequestScopeConstraintPruner.relationKey(tableId), tableId);
    }
    return List.copyOf(deduped.values());
  }

  private static List<Long> dedupeColumnIds(
      String correlationId, String tableId, List<Long> columnIds) {
    LinkedHashSet<Long> seen = new LinkedHashSet<>(columnIds.size());
    for (Long id : columnIds) {
      if (id == null || id <= 0) {
        throw GrpcErrors.invalidArgument(
            correlationId,
            PLANNER_STATS_REQUEST_COLUMN_ID_INVALID,
            Map.of("table_id", tableId, "column_id", id == null ? "null" : Long.toString(id)));
      }
      seen.add(id);
    }
    return new ArrayList<>(seen);
  }

  // ---------------------------------------------------------------------------
  // Shared iterator base
  // ---------------------------------------------------------------------------

  private abstract static class BundleIterator<C> implements Iterator<C> {

    protected final String queryId;
    protected long seq = 1;
    private boolean headerEmitted = false;
    private boolean endEmitted = false;

    protected BundleIterator(String queryId) {
      this.queryId = queryId;
    }

    @Override
    public final boolean hasNext() {
      return !endEmitted;
    }

    @Override
    public final C next() {
      if (!headerEmitted) {
        headerEmitted = true;
        return header();
      }
      if (hasMoreWork()) {
        return batch();
      }
      if (!endEmitted) {
        endEmitted = true;
        return end();
      }
      throw new NoSuchElementException();
    }

    protected abstract C header();

    protected abstract C batch();

    protected abstract C end();

    protected abstract boolean hasMoreWork();
  }

  // ---------------------------------------------------------------------------
  // Column stats iterator
  // ---------------------------------------------------------------------------

  private static final class PlannerStatsIterator extends BundleIterator<ColumnStatsBundleChunk> {

    private final String correlationId;
    private final List<TableWork> tableWorks;
    private final SnapshotPinLookup pinLookup;
    private final ConstraintProvider constraintProvider;
    private final boolean includeConstraints;
    private final ConstraintPruner constraintPruner;
    private final StatsRepository repository;
    private final int maxResultsPerChunk;
    private final long requestedTables;
    private final long requestedColumns;

    private int nextTableIndex = 0;
    private long returnedColumns = 0;
    private long notFoundColumns = 0;
    private long errorColumns = 0;
    private final List<StatsWarning> pendingWarnings = new ArrayList<>();

    private PlannerStatsIterator(
        String queryId,
        String correlationId,
        List<TableRequest> tables,
        SnapshotPinLookup pinLookup,
        ConstraintProvider constraintProvider,
        boolean includeConstraints,
        BiFunction<Set<String>, Map<String, Set<Long>>, ConstraintPruner> constraintPrunerFactory,
        StatsRepository repository,
        int maxResultsPerChunk,
        long requestedTables,
        long requestedColumns) {
      super(queryId);
      this.correlationId = correlationId;
      this.pinLookup = pinLookup;
      this.constraintProvider = constraintProvider;
      this.includeConstraints = includeConstraints;
      this.repository = repository;
      this.maxResultsPerChunk = maxResultsPerChunk;
      this.requestedTables = requestedTables;
      this.requestedColumns = requestedColumns;
      Set<String> requestedRelationKeys = new LinkedHashSet<>();
      Map<String, Set<Long>> requestedColumnsByRelationKey = new LinkedHashMap<>();
      this.tableWorks = new ArrayList<>(tables.size());
      for (TableRequest table : tables) {
        String key = RequestScopeConstraintPruner.relationKey(table.tableId());
        requestedRelationKeys.add(key);
        requestedColumnsByRelationKey.put(key, new LinkedHashSet<>(table.columnIds()));
        this.tableWorks.add(new TableWork(table.tableId(), table.columnIds()));
      }
      this.constraintPruner =
          constraintPrunerFactory.apply(
              Set.copyOf(requestedRelationKeys),
              requestedColumnsByRelationKey.entrySet().stream()
                  .collect(
                      java.util.stream.Collectors.toUnmodifiableMap(
                          Map.Entry::getKey, e -> Set.copyOf(e.getValue()))));
    }

    @Override
    protected boolean hasMoreWork() {
      return nextTableIndex < tableWorks.size();
    }

    @Override
    protected ColumnStatsBundleChunk header() {
      return ColumnStatsBundleChunk.newBuilder()
          .setQueryId(queryId)
          .setSeq(seq++)
          .setHeader(ColumnStatsBundleHeader.newBuilder().build())
          .build();
    }

    @Override
    protected ColumnStatsBundleChunk batch() {
      List<ColumnStatsResult> out = new ArrayList<>(Math.min(maxResultsPerChunk, 16));
      List<TableConstraintsResult> constraintsOut = new ArrayList<>();
      while (out.size() < maxResultsPerChunk && hasMoreWork()) {
        TableWork work = tableWorks.get(nextTableIndex);
        ColumnStatsResult result = processNextColumn(work);
        out.add(result);
        if (includeConstraints && !work.constraintsEmitted()) {
          constraintsOut.add(processConstraints(work));
          work.markConstraintsEmitted();
        }
        if (!work.hasMoreColumns()) {
          nextTableIndex++;
        }
      }
      ColumnStatsBatch.Builder batchBuilder =
          ColumnStatsBatch.newBuilder().addAllColumns(out).addAllConstraints(constraintsOut);
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

    @Override
    protected ColumnStatsBundleChunk end() {
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

    private TableConstraintsResult processConstraints(TableWork work) {
      return resolveConstraintResult(
              work.tableId, resolveSnapshot(work), constraintProvider, constraintPruner)
          .result();
    }

    private OptionalLong resolveSnapshot(TableWork work) {
      if (work.pinResolved) {
        return work.pinnedSnapshot;
      }
      work.pinnedSnapshot = pinLookup.pinnedSnapshotId(work.tableId);
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

      return ColumnStatsResult.newBuilder()
          .setTableId(tableId)
          .setColumnId(columnId)
          .setColumnName(view.columnName())
          .setStatus(BundleResultStatus.BUNDLE_RESULT_STATUS_FOUND)
          .setStats(info.build())
          .build();
    }

    private ColumnStatsResult buildErrorResult(
        ResourceId tableId, long columnId, long snapshotId, RuntimeException e) {
      BundleFailure failure =
          failureBase(tableId, COLUMN_ERROR_CODE, "column stats lookup failed")
              .putDetails("column_id", Long.toString(columnId))
              .putDetails("snapshot_id", Long.toString(snapshotId))
              .putDetails("exception", e.getClass().getSimpleName())
              .putDetails("message", e.getMessage() == null ? "" : e.getMessage())
              .build();
      return ColumnStatsResult.newBuilder()
          .setTableId(tableId)
          .setColumnId(columnId)
          .setStatus(BundleResultStatus.BUNDLE_RESULT_STATUS_ERROR)
          .setFailure(failure)
          .build();
    }

    private ColumnStatsResult notFoundResult(
        ResourceId tableId, long columnId, String code, String message, OptionalLong snapshotId) {
      BundleFailure.Builder failure =
          failureBase(tableId, code, message).putDetails("column_id", Long.toString(columnId));
      snapshotId.ifPresent(id -> failure.putDetails("snapshot_id", Long.toString(id)));
      return ColumnStatsResult.newBuilder()
          .setTableId(tableId)
          .setColumnId(columnId)
          .setStatus(BundleResultStatus.BUNDLE_RESULT_STATUS_NOT_FOUND)
          .setFailure(failure.build())
          .build();
    }

    private ColumnStatsResult pinMissingResult(ResourceId tableId, long columnId) {
      BundleFailure failure =
          failureBase(tableId, PIN_MISSING_CODE, "snapshot pin missing")
              .putDetails("column_id", Long.toString(columnId))
              .build();
      return ColumnStatsResult.newBuilder()
          .setTableId(tableId)
          .setColumnId(columnId)
          .setStatus(BundleResultStatus.BUNDLE_RESULT_STATUS_ERROR)
          .setFailure(failure)
          .build();
    }
  }

  // ---------------------------------------------------------------------------
  // Constraints-only iterator
  // ---------------------------------------------------------------------------

  private static final class TableConstraintsIterator
      extends BundleIterator<TableConstraintsBundleChunk> {

    private final List<ResourceId> tableIds;
    private final SnapshotPinLookup pinLookup;
    private final ConstraintProvider constraintProvider;
    private final ConstraintPruner constraintPruner;
    private final int maxResultsPerChunk;

    private int nextTableIndex = 0;
    private long returnedTables = 0;
    private long notFoundTables = 0;
    private long errorTables = 0;

    private TableConstraintsIterator(
        String queryId,
        List<ResourceId> tableIds,
        SnapshotPinLookup pinLookup,
        ConstraintProvider constraintProvider,
        ConstraintPruner constraintPruner,
        int maxResultsPerChunk) {
      super(queryId);
      this.tableIds = tableIds;
      this.pinLookup = pinLookup;
      this.constraintProvider = constraintProvider;
      this.constraintPruner = constraintPruner;
      this.maxResultsPerChunk = maxResultsPerChunk;
    }

    @Override
    protected boolean hasMoreWork() {
      return nextTableIndex < tableIds.size();
    }

    @Override
    protected TableConstraintsBundleChunk header() {
      return TableConstraintsBundleChunk.newBuilder()
          .setQueryId(queryId)
          .setSeq(seq++)
          .setHeader(TableConstraintsBundleHeader.newBuilder().build())
          .build();
    }

    @Override
    protected TableConstraintsBundleChunk batch() {
      List<TableConstraintsResult> out = new ArrayList<>(Math.min(maxResultsPerChunk, 16));
      while (out.size() < maxResultsPerChunk && nextTableIndex < tableIds.size()) {
        out.add(processTable(tableIds.get(nextTableIndex++)));
      }
      TableConstraintsBatch batch =
          TableConstraintsBatch.newBuilder().addAllConstraints(out).build();
      return TableConstraintsBundleChunk.newBuilder()
          .setQueryId(queryId)
          .setSeq(seq++)
          .setBatch(batch)
          .build();
    }

    @Override
    protected TableConstraintsBundleChunk end() {
      TableConstraintsBundleEnd end =
          TableConstraintsBundleEnd.newBuilder()
              .setRequestedTables(tableIds.size())
              .setReturnedTables(returnedTables)
              .setNotFoundTables(notFoundTables)
              .setErrorTables(errorTables)
              .build();
      return TableConstraintsBundleChunk.newBuilder()
          .setQueryId(queryId)
          .setSeq(seq++)
          .setEnd(end)
          .build();
    }

    private TableConstraintsResult processTable(ResourceId tableId) {
      ConstraintResolution result =
          resolveConstraintResult(
              tableId, pinLookup.pinnedSnapshotId(tableId), constraintProvider, constraintPruner);
      switch (result.status()) {
        case FOUND -> returnedTables++;
        case NOT_FOUND -> notFoundTables++;
        case ERROR -> errorTables++;
      }
      return result.result();
    }
  }

  // ---------------------------------------------------------------------------
  // Constraint resolution (shared between both iterators)
  // ---------------------------------------------------------------------------

  private enum ConstraintResolutionStatus {
    FOUND,
    NOT_FOUND,
    ERROR
  }

  private record ConstraintResolution(
      TableConstraintsResult result, ConstraintResolutionStatus status) {}

  private static ConstraintResolution resolveConstraintResult(
      ResourceId tableId,
      OptionalLong snapshotId,
      ConstraintProvider constraintProvider,
      ConstraintPruner constraintPruner) {
    if (snapshotId.isEmpty()) {
      LOG.debugf("constraint pin missing for %s", tableId.getId());
      return new ConstraintResolution(
          constraintPinMissingResult(tableId), ConstraintResolutionStatus.ERROR);
    }

    try {
      OptionalLong pinnedSnapshotId = snapshotId;
      // Deliberate client-contract choice: constraints "absence" states are intentionally
      // collapsed to NOT_FOUND for planner simplicity, while callers can distinguish the cause via
      // failure.details["reason"]. This is not a statement that these storage states are
      // equivalent:
      // - provider_missing: no provider view
      // - provider_empty: provider view with zero constraints
      // - pruned_empty: provider view exists but request-scope pruning hid all constraints
      // TODO: Revisit status mapping if planner clients need to distinguish provider_empty as
      // FOUND with zero constraints while keeping provider_missing as NOT_FOUND.
      var maybeView = constraintProvider.constraints(tableId, pinnedSnapshotId);
      if (maybeView.isEmpty()) {
        LOG.tracef(
            "no constraints stored for %s snapshot %d", tableId.getId(), snapshotId.getAsLong());
        return new ConstraintResolution(
            constraintNotFoundResult(tableId, pinnedSnapshotId, "provider_missing"),
            ConstraintResolutionStatus.NOT_FOUND);
      }

      List<ConstraintDefinition> visible = maybeView.get().constraints();
      if (visible.isEmpty()) {
        LOG.debugf(
            "constraint provider returned empty bundle for %s snapshot %d",
            tableId.getId(), snapshotId.getAsLong());
        return new ConstraintResolution(
            constraintNotFoundResult(tableId, pinnedSnapshotId, "provider_empty"),
            ConstraintResolutionStatus.NOT_FOUND);
      }
      visible = constraintPruner.prune(tableId, visible);

      // Semantics choice: provider FOUND + prune-to-empty is surfaced as NOT_FOUND to callers.
      // This keeps client handling simple: no visible constraints for the request scope.
      if (visible.isEmpty()) {
        LOG.tracef(
            "all constraints pruned for %s snapshot %d", tableId.getId(), snapshotId.getAsLong());
        return new ConstraintResolution(
            constraintNotFoundResult(tableId, pinnedSnapshotId, "pruned_empty"),
            ConstraintResolutionStatus.NOT_FOUND);
      }

      LOG.tracef(
          "constraints resolved for %s snapshot %d: %d constraints",
          tableId.getId(), snapshotId.getAsLong(), visible.size());
      if (LOG.isTraceEnabled()) {
        for (ConstraintDefinition c : visible) {
          LOG.tracef(
              "  constraint: type=%s name=%s columns=%d enforcement=%s",
              c.getType(), c.getName(), c.getColumnsCount(), c.getEnforcement());
        }
      }
      return new ConstraintResolution(
          TableConstraintsResult.newBuilder()
              .setTableId(tableId)
              .setStatus(BundleResultStatus.BUNDLE_RESULT_STATUS_FOUND)
              .addAllConstraints(visible)
              .build(),
          ConstraintResolutionStatus.FOUND);
    } catch (RuntimeException e) {
      LOG.debugf(
          e,
          "constraint lookup failed for %s snapshot %d",
          tableId.getId(),
          snapshotId.getAsLong());
      return new ConstraintResolution(
          constraintErrorResult(tableId, OptionalLong.of(snapshotId.getAsLong()), e),
          ConstraintResolutionStatus.ERROR);
    }
  }

  // ---------------------------------------------------------------------------
  // Failure builders
  // ---------------------------------------------------------------------------

  private static BundleFailure.Builder failureBase(
      ResourceId tableId, String code, String message) {
    return BundleFailure.newBuilder()
        .setCode(code)
        .setMessage(message)
        .putDetails("table_id", tableId.getId())
        .putDetails("account_id", tableId.getAccountId());
  }

  private static TableConstraintsResult constraintPinMissingResult(ResourceId tableId) {
    BundleFailure failure = failureBase(tableId, PIN_MISSING_CODE, "snapshot pin missing").build();
    return TableConstraintsResult.newBuilder()
        .setTableId(tableId)
        .setStatus(BundleResultStatus.BUNDLE_RESULT_STATUS_ERROR)
        .setFailure(failure)
        .build();
  }

  private static TableConstraintsResult constraintNotFoundResult(
      ResourceId tableId, OptionalLong snapshotId, String reason) {
    BundleFailure.Builder failure =
        failureBase(tableId, CONSTRAINT_MISSING_CODE, "constraints missing")
            .putDetails("reason", reason);
    snapshotId.ifPresent(id -> failure.putDetails("snapshot_id", Long.toString(id)));
    return TableConstraintsResult.newBuilder()
        .setTableId(tableId)
        .setStatus(BundleResultStatus.BUNDLE_RESULT_STATUS_NOT_FOUND)
        .setFailure(failure.build())
        .build();
  }

  private static TableConstraintsResult constraintErrorResult(
      ResourceId tableId, OptionalLong snapshotId, RuntimeException exception) {
    BundleFailure.Builder failure =
        failureBase(tableId, CONSTRAINT_ERROR_CODE, "constraints lookup failed");
    snapshotId.ifPresent(id -> failure.putDetails("snapshot_id", Long.toString(id)));
    failure
        .putDetails("exception", exception.getClass().getSimpleName())
        .putDetails("message", exception.getMessage() == null ? "" : exception.getMessage());
    return TableConstraintsResult.newBuilder()
        .setTableId(tableId)
        .setStatus(BundleResultStatus.BUNDLE_RESULT_STATUS_ERROR)
        .setFailure(failure.build())
        .build();
  }

  // ---------------------------------------------------------------------------
  // Supporting types
  // ---------------------------------------------------------------------------

  private static final class TableWork {
    private final ResourceId tableId;
    private final List<Long> columnIds;
    private OptionalLong pinnedSnapshot = OptionalLong.empty();
    private boolean pinResolved = false;
    private Map<Long, ColumnStats> statsByColumn;
    private RuntimeException statsError;
    private int columnIndex = 0;
    private boolean constraintsEmitted = false;

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

    private boolean constraintsEmitted() {
      return constraintsEmitted;
    }

    private void markConstraintsEmitted() {
      this.constraintsEmitted = true;
    }
  }

  private record TableRequest(ResourceId tableId, List<Long> columnIds) {}

  private record NormalizedRequest(List<TableRequest> tables, long requestedColumns) {}
}
