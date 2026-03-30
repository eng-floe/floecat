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

import ai.floedb.floecat.catalog.rpc.ConstraintDefinition;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.query.rpc.BundleFailure;
import ai.floedb.floecat.query.rpc.BundleResultStatus;
import ai.floedb.floecat.query.rpc.FetchTableConstraintsRequest;
import ai.floedb.floecat.query.rpc.FetchTargetStatsRequest;
import ai.floedb.floecat.query.rpc.TableConstraintsBatch;
import ai.floedb.floecat.query.rpc.TableConstraintsBundleChunk;
import ai.floedb.floecat.query.rpc.TableConstraintsBundleEnd;
import ai.floedb.floecat.query.rpc.TableConstraintsBundleHeader;
import ai.floedb.floecat.query.rpc.TableConstraintsResult;
import ai.floedb.floecat.query.rpc.TableTargetStatsRequest;
import ai.floedb.floecat.query.rpc.TargetStatsBatch;
import ai.floedb.floecat.query.rpc.TargetStatsBundleChunk;
import ai.floedb.floecat.query.rpc.TargetStatsBundleEnd;
import ai.floedb.floecat.query.rpc.TargetStatsBundleHeader;
import ai.floedb.floecat.query.rpc.TargetStatsResult;
import ai.floedb.floecat.scanner.spi.ConstraintProvider;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.service.statistics.engine.StatsEngineRegistry;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsExecutionMode;
import ai.floedb.floecat.stats.spi.StatsStore;
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
import java.util.Optional;
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
  private static final String TARGET_MISSING_CODE = "planner_stats.target_stats.missing";
  private static final String TARGET_ERROR_CODE = "planner_stats.target_stats.error";
  private static final String CONSTRAINT_MISSING_CODE = "planner_stats.constraints.missing";
  private static final String CONSTRAINT_ERROR_CODE = "planner_stats.constraints.error";

  private final StatsProviderFactory statsFactory;
  private final Supplier<ConstraintProvider> constraintProviderSupplier;
  private final BiFunction<Set<String>, Map<String, Set<Long>>, ConstraintPruner>
      constraintPrunerFactory;
  private final Function<Set<String>, ConstraintPruner> constraintsOnlyPrunerFactory;
  private final TargetStatsLookup targetStatsLookup;
  private final int maxTables;
  private final int maxTargets;
  private final int maxResultsPerChunk;

  private static final record PlannerStatsLimits(
      int maxTables, int maxTargets, int maxResultsPerChunk) {}

  @Inject
  public PlannerStatsBundleService(
      StatsProviderFactory statsFactory,
      ConstraintProviderFactory constraintFactory,
      ConstraintPrunerFactory constraintPrunerFactory,
      StatsEngineRegistry statsEngineRegistry,
      @ConfigProperty(name = "floecat.planner.stats.max-tables", defaultValue = "50") int maxTables,
      @ConfigProperty(name = "floecat.planner.stats.max-targets", defaultValue = "1024")
          int maxTargets,
      @ConfigProperty(name = "floecat.planner.stats.max-results-per-chunk", defaultValue = "100")
          int maxResultsPerChunk) {
    this(
        statsFactory,
        constraintFactory::provider,
        constraintPrunerFactory::forRequest,
        constraintPrunerFactory::forConstraintsOnlyRequest,
        providerLookup(statsEngineRegistry),
        new PlannerStatsLimits(maxTables, maxTargets, maxResultsPerChunk));
  }

  PlannerStatsBundleService(
      StatsProviderFactory statsFactory,
      Supplier<ConstraintProvider> constraintProviderSupplier,
      BiFunction<Set<String>, Map<String, Set<Long>>, ConstraintPruner> constraintPrunerFactory,
      Function<Set<String>, ConstraintPruner> constraintsOnlyPrunerFactory,
      TargetStatsLookup targetStatsLookup,
      PlannerStatsLimits limits) {
    this.statsFactory = Objects.requireNonNull(statsFactory, "statsFactory");
    this.constraintProviderSupplier =
        Objects.requireNonNull(constraintProviderSupplier, "constraintProviderSupplier");
    this.constraintPrunerFactory =
        Objects.requireNonNull(constraintPrunerFactory, "constraintPrunerFactory");
    this.constraintsOnlyPrunerFactory =
        Objects.requireNonNull(constraintsOnlyPrunerFactory, "constraintsOnlyPrunerFactory");
    this.targetStatsLookup = Objects.requireNonNull(targetStatsLookup, "targetStatsLookup");
    this.maxTables = Math.max(1, limits.maxTables);
    this.maxTargets = Math.max(1, limits.maxTargets);
    this.maxResultsPerChunk = Math.max(1, limits.maxResultsPerChunk);
  }

  public static PlannerStatsBundleService forTesting(
      StatsProviderFactory statsFactory,
      StatsStore statsStore,
      int maxTables,
      int maxTargets,
      int maxResultsPerChunk) {
    return forTesting(
        statsFactory,
        ConstraintProvider.NONE,
        statsStore,
        maxTables,
        maxTargets,
        maxResultsPerChunk);
  }

  public static PlannerStatsBundleService forTesting(
      StatsProviderFactory statsFactory,
      ConstraintProvider constraintProvider,
      StatsStore statsStore,
      int maxTables,
      int maxTargets,
      int maxResultsPerChunk) {
    return new PlannerStatsBundleService(
        statsFactory,
        () -> constraintProvider == null ? ConstraintProvider.NONE : constraintProvider,
        RequestScopeConstraintPruner::new,
        RequestScopeConstraintPruner::forRequestedTablesOnly,
        statsStore::getTargetStats,
        new PlannerStatsLimits(maxTables, maxTargets, maxResultsPerChunk));
  }

  public Multi<TargetStatsBundleChunk> streamTargets(
      String correlationId, QueryContext ctx, FetchTargetStatsRequest request) {
    FetchTargetStatsRequest safeRequest =
        request == null ? FetchTargetStatsRequest.getDefaultInstance() : request;
    NormalizedRequest normalized = normalizeRequest(correlationId, safeRequest);
    List<TableRequest> tableRequests = normalized.tables();
    ConstraintProvider constraintProvider =
        safeRequest.getIncludeConstraints()
            ? constraintProviderSupplier.get()
            : ConstraintProvider.NONE;
    SnapshotPinLookup pinLookup = statsFactory.pinLookupForQuery(ctx, correlationId);
    return Multi.createFrom()
        .<TargetStatsBundleChunk>deferred(
            () ->
                Multi.createFrom()
                    .iterable(
                        () ->
                            new TargetStatsIterator(
                                ctx.getQueryId(),
                                correlationId,
                                tableRequests,
                                pinLookup,
                                constraintProvider,
                                safeRequest.getIncludeConstraints(),
                                constraintPrunerFactory,
                                targetStatsLookup,
                                maxResultsPerChunk,
                                tableRequests.size(),
                                normalized.requestedTargets())));
  }

  @FunctionalInterface
  private interface TargetStatsLookup {
    Optional<TargetStatsRecord> get(ResourceId tableId, long snapshotId, StatsTarget target);
  }

  private static TargetStatsLookup providerLookup(StatsEngineRegistry statsEngineRegistry) {
    Objects.requireNonNull(statsEngineRegistry, "statsEngineRegistry");
    return (tableId, snapshotId, target) ->
        statsEngineRegistry
            .capture(
                new StatsCaptureRequest(
                    tableId, snapshotId, target, Set.of(), StatsExecutionMode.SYNC, "", "", false))
            .map(ai.floedb.floecat.stats.spi.StatsCaptureResult::record);
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
      String correlationId, FetchTargetStatsRequest request) {
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
    long totalTargets = 0;

    for (int tableIndex = 0; tableIndex < request.getTablesCount(); tableIndex++) {
      TableTargetStatsRequest table = request.getTables(tableIndex);
      if (!table.hasTableId()) {
        throw GrpcErrors.invalidArgument(
            correlationId,
            PLANNER_STATS_REQUEST_TABLE_ID_MISSING,
            Map.of("table_index", Integer.toString(tableIndex)));
      }
      if (table.getTargetsCount() == 0) {
        throw GrpcErrors.invalidArgument(
            correlationId,
            PLANNER_STATS_REQUEST_TARGETS_MISSING,
            Map.of("table_id", table.getTableId().getId()));
      }

      List<StatsTarget> deduped =
          dedupeTargets(correlationId, table.getTableId().getId(), table.getTargetsList());
      if (deduped.isEmpty()) {
        throw GrpcErrors.invalidArgument(
            correlationId,
            PLANNER_STATS_REQUEST_TARGETS_MISSING,
            Map.of("table_id", table.getTableId().getId()));
      }

      normalized.add(new TableRequest(table.getTableId(), List.copyOf(deduped)));
      totalTargets += deduped.size();
      if (totalTargets > maxTargets) {
        throw GrpcErrors.invalidArgument(
            correlationId,
            PLANNER_STATS_REQUEST_TARGETS_LIMIT,
            Map.of("max_targets", Integer.toString(maxTargets)));
      }
    }

    return new NormalizedRequest(List.copyOf(normalized), totalTargets);
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

  private static List<StatsTarget> dedupeTargets(
      String correlationId, String tableId, List<StatsTarget> targets) {
    Map<String, StatsTarget> deduped = new LinkedHashMap<>(targets.size());
    for (StatsTarget target : targets) {
      if (target == null || target.getTargetCase() == StatsTarget.TargetCase.TARGET_NOT_SET) {
        throw GrpcErrors.invalidArgument(
            correlationId, PLANNER_STATS_REQUEST_TARGET_INVALID, Map.of());
      }
      if (target.hasColumn() && target.getColumn().getColumnId() <= 0) {
        throw GrpcErrors.invalidArgument(
            correlationId, PLANNER_STATS_REQUEST_TARGET_INVALID, Map.of());
      }
      deduped.putIfAbsent(StatsTargetIdentity.storageId(target), target);
    }
    return new ArrayList<>(deduped.values());
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
  // Target stats iterator
  // ---------------------------------------------------------------------------

  private static final class TargetStatsIterator extends BundleIterator<TargetStatsBundleChunk> {

    private final String correlationId;
    private final List<TableWork> tableWorks;
    private final SnapshotPinLookup pinLookup;
    private final ConstraintProvider constraintProvider;
    private final boolean includeConstraints;
    private final ConstraintPruner constraintPruner;
    private final TargetStatsLookup targetStatsLookup;
    private final int maxResultsPerChunk;
    private final long requestedTables;
    private final long requestedTargets;

    private int nextTableIndex = 0;
    private long returnedTargets = 0;
    private long notFoundTargets = 0;
    private long errorTargets = 0;

    private TargetStatsIterator(
        String queryId,
        String correlationId,
        List<TableRequest> tables,
        SnapshotPinLookup pinLookup,
        ConstraintProvider constraintProvider,
        boolean includeConstraints,
        BiFunction<Set<String>, Map<String, Set<Long>>, ConstraintPruner> constraintPrunerFactory,
        TargetStatsLookup targetStatsLookup,
        int maxResultsPerChunk,
        long requestedTables,
        long requestedTargets) {
      super(queryId);
      this.correlationId = correlationId;
      this.pinLookup = pinLookup;
      this.constraintProvider = constraintProvider;
      this.includeConstraints = includeConstraints;
      this.targetStatsLookup = targetStatsLookup;
      this.maxResultsPerChunk = maxResultsPerChunk;
      this.requestedTables = requestedTables;
      this.requestedTargets = requestedTargets;
      Set<String> requestedRelationKeys = new LinkedHashSet<>();
      Map<String, Set<Long>> requestedColumnsByRelationKey = new LinkedHashMap<>();
      this.tableWorks = new ArrayList<>(tables.size());
      for (TableRequest table : tables) {
        String key = RequestScopeConstraintPruner.relationKey(table.tableId());
        requestedRelationKeys.add(key);
        Set<Long> requestedColumnIds = new LinkedHashSet<>();
        for (StatsTarget target : table.targets()) {
          if (target.hasColumn()) {
            requestedColumnIds.add(target.getColumn().getColumnId());
          }
        }
        requestedColumnsByRelationKey.put(key, requestedColumnIds);
        this.tableWorks.add(new TableWork(table.tableId(), table.targets()));
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
    protected TargetStatsBundleChunk header() {
      return TargetStatsBundleChunk.newBuilder()
          .setQueryId(queryId)
          .setSeq(seq++)
          .setHeader(TargetStatsBundleHeader.newBuilder().build())
          .build();
    }

    @Override
    protected TargetStatsBundleChunk batch() {
      List<TargetStatsResult> out = new ArrayList<>(Math.min(maxResultsPerChunk, 16));
      List<TableConstraintsResult> constraintsOut = new ArrayList<>();
      while (out.size() < maxResultsPerChunk && hasMoreWork()) {
        TableWork work = tableWorks.get(nextTableIndex);
        TargetStatsResult result = processNextTarget(work);
        out.add(result);
        if (includeConstraints && !work.constraintsEmitted()) {
          constraintsOut.add(processConstraints(work));
          work.markConstraintsEmitted();
        }
        if (!work.hasMoreTargets()) {
          nextTableIndex++;
        }
      }
      TargetStatsBatch batch =
          TargetStatsBatch.newBuilder()
              .addAllTargets(out)
              .addAllConstraints(constraintsOut)
              .build();
      return TargetStatsBundleChunk.newBuilder()
          .setQueryId(queryId)
          .setSeq(seq++)
          .setBatch(batch)
          .build();
    }

    @Override
    protected TargetStatsBundleChunk end() {
      TargetStatsBundleEnd end =
          TargetStatsBundleEnd.newBuilder()
              .setRequestedTables(requestedTables)
              .setRequestedTargets(requestedTargets)
              .setReturnedTargets(returnedTargets)
              .setNotFoundTargets(notFoundTargets)
              .setErrorTargets(errorTargets)
              .build();
      return TargetStatsBundleChunk.newBuilder()
          .setQueryId(queryId)
          .setSeq(seq++)
          .setEnd(end)
          .build();
    }

    private TargetStatsResult processNextTarget(TableWork work) {
      StatsTarget target = work.targetAt(work.targetIndex());
      ResourceId tableId = work.tableId;
      OptionalLong snapshot = resolveSnapshot(work);
      TargetStatsResult result;
      if (snapshot.isEmpty()) {
        errorTargets++;
        result = pinMissingResult(tableId, target);
      } else {
        try {
          Optional<TargetStatsRecord> recordOpt =
              targetStatsLookup.get(tableId, snapshot.getAsLong(), target);
          if (recordOpt.isPresent()) {
            returnedTargets++;
            result = buildFoundResult(recordOpt.get());
          } else {
            notFoundTargets++;
            result =
                notFoundResult(
                    tableId, target, TARGET_MISSING_CODE, "target stats missing", snapshot);
          }
        } catch (RuntimeException e) {
          errorTargets++;
          result = buildErrorResult(tableId, target, snapshot.getAsLong(), e);
          LOG.debugf(
              e, "target stats lookup failed for %s snapshot %s", tableId, snapshot.getAsLong());
        }
      }
      work.advanceTarget();
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

    private static TargetStatsResult buildFoundResult(TargetStatsRecord stats) {
      return TargetStatsResult.newBuilder()
          .setTableId(stats.getTableId())
          .setSnapshotId(stats.getSnapshotId())
          .setTarget(stats.getTarget())
          .setStatus(BundleResultStatus.BUNDLE_RESULT_STATUS_FOUND)
          .setStats(stats)
          .build();
    }

    private TargetStatsResult buildErrorResult(
        ResourceId tableId, StatsTarget target, long snapshotId, RuntimeException e) {
      BundleFailure.Builder failure =
          failureBase(tableId, TARGET_ERROR_CODE, "target stats lookup failed")
              .putDetails("snapshot_id", Long.toString(snapshotId))
              .putDetails("target", StatsTargetIdentity.storageId(target))
              .putDetails("exception", e.getClass().getSimpleName())
              .putDetails("message", e.getMessage() == null ? "" : e.getMessage());
      return TargetStatsResult.newBuilder()
          .setTableId(tableId)
          .setSnapshotId(snapshotId)
          .setTarget(target)
          .setStatus(BundleResultStatus.BUNDLE_RESULT_STATUS_ERROR)
          .setFailure(failure.build())
          .build();
    }

    private TargetStatsResult notFoundResult(
        ResourceId tableId,
        StatsTarget target,
        String code,
        String message,
        OptionalLong snapshotId) {
      BundleFailure.Builder failure =
          failureBase(tableId, code, message)
              .putDetails("target", StatsTargetIdentity.storageId(target));
      snapshotId.ifPresent(id -> failure.putDetails("snapshot_id", Long.toString(id)));
      TargetStatsResult.Builder builder =
          TargetStatsResult.newBuilder()
              .setTableId(tableId)
              .setTarget(target)
              .setStatus(BundleResultStatus.BUNDLE_RESULT_STATUS_NOT_FOUND)
              .setFailure(failure.build());
      snapshotId.ifPresent(builder::setSnapshotId);
      return builder.build();
    }

    private TargetStatsResult pinMissingResult(ResourceId tableId, StatsTarget target) {
      BundleFailure failure =
          failureBase(tableId, PIN_MISSING_CODE, "snapshot pin missing")
              .putDetails("target", StatsTargetIdentity.storageId(target))
              .build();
      return TargetStatsResult.newBuilder()
          .setTableId(tableId)
          .setTarget(target)
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
      // Deliberate client-contract choice: constraints "absence" states are intentionally
      // collapsed to NOT_FOUND for planner simplicity, while callers can distinguish the cause via
      // failure.details["reason"]. This is not a statement that these storage states are
      // equivalent:
      // - provider_missing: no provider view
      // - provider_empty: provider view with zero constraints
      // - pruned_empty: provider view exists but request-scope pruning hid all constraints
      // TODO: Revisit status mapping if planner clients need to distinguish provider_empty as
      // FOUND with zero constraints while keeping provider_missing as NOT_FOUND. A typed
      // ConstraintAbsenceReason enum field in ConstraintResolution would let callers branch
      // without parsing the failure.details["reason"] string.
      var maybeView = constraintProvider.constraints(tableId, snapshotId);
      if (maybeView.isEmpty()) {
        LOG.tracef(
            "no constraints stored for %s snapshot %d", tableId.getId(), snapshotId.getAsLong());
        return new ConstraintResolution(
            constraintNotFoundResult(tableId, snapshotId, "provider_missing"),
            ConstraintResolutionStatus.NOT_FOUND);
      }

      List<ConstraintDefinition> visible = maybeView.get().constraints();
      if (visible.isEmpty()) {
        LOG.debugf(
            "constraint provider returned empty bundle for %s snapshot %d",
            tableId.getId(), snapshotId.getAsLong());
        return new ConstraintResolution(
            constraintNotFoundResult(tableId, snapshotId, "provider_empty"),
            ConstraintResolutionStatus.NOT_FOUND);
      }
      visible = constraintPruner.prune(tableId, visible);

      // Semantics choice: provider FOUND + prune-to-empty is surfaced as NOT_FOUND to callers.
      // This keeps client handling simple: no visible constraints for the request scope.
      if (visible.isEmpty()) {
        LOG.tracef(
            "all constraints pruned for %s snapshot %d", tableId.getId(), snapshotId.getAsLong());
        return new ConstraintResolution(
            constraintNotFoundResult(tableId, snapshotId, "pruned_empty"),
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
          constraintErrorResult(tableId, snapshotId, e), ConstraintResolutionStatus.ERROR);
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
    private final List<StatsTarget> targets;
    private OptionalLong pinnedSnapshot = OptionalLong.empty();
    private boolean pinResolved = false;
    private int targetIndex = 0;
    private boolean constraintsEmitted = false;

    private TableWork(ResourceId tableId, List<StatsTarget> targets) {
      this.tableId = tableId;
      this.targets = targets;
    }

    private boolean hasMoreTargets() {
      return targetIndex < targets.size();
    }

    private StatsTarget targetAt(int index) {
      return targets.get(index);
    }

    private void advanceTarget() {
      targetIndex++;
    }

    private int targetIndex() {
      return targetIndex;
    }

    private boolean constraintsEmitted() {
      return constraintsEmitted;
    }

    private void markConstraintsEmitted() {
      this.constraintsEmitted = true;
    }
  }

  private record TableRequest(ResourceId tableId, List<StatsTarget> targets) {}

  private record NormalizedRequest(List<TableRequest> tables, long requestedTargets) {}
}
