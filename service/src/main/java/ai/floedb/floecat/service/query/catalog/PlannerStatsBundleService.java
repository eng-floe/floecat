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
import ai.floedb.floecat.catalog.rpc.SnapshotConstraints;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.query.rpc.BundleFailure;
import ai.floedb.floecat.query.rpc.BundleResultStatus;
import ai.floedb.floecat.query.rpc.FetchTableConstraintsRequest;
import ai.floedb.floecat.query.rpc.FetchTargetStatsRequest;
import ai.floedb.floecat.query.rpc.StatsResultStatus;
import ai.floedb.floecat.query.rpc.TableConstraintsBatch;
import ai.floedb.floecat.query.rpc.TableConstraintsBundleChunk;
import ai.floedb.floecat.query.rpc.TableConstraintsBundleEnd;
import ai.floedb.floecat.query.rpc.TableConstraintsBundleHeader;
import ai.floedb.floecat.query.rpc.TableConstraintsResult;
import ai.floedb.floecat.query.rpc.TargetStatsBatch;
import ai.floedb.floecat.query.rpc.TargetStatsBundleChunk;
import ai.floedb.floecat.query.rpc.TargetStatsBundleEnd;
import ai.floedb.floecat.query.rpc.TargetStatsBundleHeader;
import ai.floedb.floecat.query.rpc.TargetStatsResult;
import ai.floedb.floecat.scanner.spi.ConstraintProvider;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.service.repo.impl.ConstraintRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.statistics.StatsOrchestrator;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsExecutionMode;
import ai.floedb.floecat.stats.spi.StatsResolutionResult;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.stats.spi.StatsSyncOutcome;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.PhaseDiagnostics;
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
  private static final String CONSTRAINT_OMITTED_CODE = "planner_stats.constraints.omitted";

  private final StatsProviderFactory statsFactory;
  private final Supplier<ConstraintProvider> constraintProviderSupplier;
  private final ConstraintRepository constraintRepository;
  private final BiFunction<Set<String>, Map<String, Set<Long>>, ConstraintPruner>
      constraintPrunerFactory;
  private final Function<Set<String>, ConstraintPruner> constraintsOnlyPrunerFactory;
  private final TargetStatsLookup targetStatsLookup;
  private final PlannerStatsRequestNormalizer requestNormalizer;
  private final int maxTables;
  private final int maxTargets;
  private final int maxResultsPerChunk;
  @Inject Observability observability;

  private static final record PlannerStatsLimits(
      int maxTables, int maxTargets, int maxResultsPerChunk) {}

  @Inject
  public PlannerStatsBundleService(
      StatsProviderFactory statsFactory,
      ConstraintProviderFactory constraintFactory,
      ConstraintRepository constraintRepository,
      ConstraintPrunerFactory constraintPrunerFactory,
      StatsOrchestrator statsOrchestrator,
      TableRepository tableRepository,
      @ConfigProperty(name = "floecat.planner.stats.max-tables", defaultValue = "50") int maxTables,
      @ConfigProperty(name = "floecat.planner.stats.max-targets", defaultValue = "10000")
          int maxTargets,
      @ConfigProperty(name = "floecat.planner.stats.max-results-per-chunk", defaultValue = "100")
          int maxResultsPerChunk) {
    this(
        statsFactory,
        constraintFactory::pinnedQueryProvider,
        constraintRepository,
        constraintPrunerFactory::forRequest,
        constraintPrunerFactory::forConstraintsOnlyRequest,
        providerLookup(statsOrchestrator, tableRepository),
        new PlannerStatsLimits(maxTables, maxTargets, maxResultsPerChunk));
  }

  PlannerStatsBundleService(
      StatsProviderFactory statsFactory,
      Supplier<ConstraintProvider> constraintProviderSupplier,
      ConstraintRepository constraintRepository,
      BiFunction<Set<String>, Map<String, Set<Long>>, ConstraintPruner> constraintPrunerFactory,
      Function<Set<String>, ConstraintPruner> constraintsOnlyPrunerFactory,
      TargetStatsLookup targetStatsLookup,
      PlannerStatsLimits limits) {
    this.statsFactory = Objects.requireNonNull(statsFactory, "statsFactory");
    this.constraintProviderSupplier =
        Objects.requireNonNull(constraintProviderSupplier, "constraintProviderSupplier");
    this.constraintRepository = constraintRepository;
    this.constraintPrunerFactory =
        Objects.requireNonNull(constraintPrunerFactory, "constraintPrunerFactory");
    this.constraintsOnlyPrunerFactory =
        Objects.requireNonNull(constraintsOnlyPrunerFactory, "constraintsOnlyPrunerFactory");
    this.targetStatsLookup = Objects.requireNonNull(targetStatsLookup, "targetStatsLookup");
    this.maxTables = Math.max(1, limits.maxTables);
    this.maxTargets = Math.max(1, limits.maxTargets);
    this.maxResultsPerChunk = Math.max(1, limits.maxResultsPerChunk);
    this.requestNormalizer = new PlannerStatsRequestNormalizer(this.maxTables, this.maxTargets);
  }

  /**
   * Test factory that wires through the real {@code providerLookup()} path — including {@link
   * StatsOrchestrator#resolvePlannerBatch} with stale-before-sync ordering. Use this for
   * integration tests that need to verify the full resolution chain.
   */
  static PlannerStatsBundleService forTestingWithRealLookup(
      StatsOrchestrator orchestrator,
      TableRepository tableRepository,
      StatsProviderFactory statsFactory,
      int maxTables,
      int maxTargets,
      int maxResultsPerChunk) {
    return new PlannerStatsBundleService(
        statsFactory,
        () -> ConstraintProvider.NONE,
        null,
        RequestScopeConstraintPruner::new,
        RequestScopeConstraintPruner::forRequestedTablesOnly,
        providerLookup(orchestrator, tableRepository),
        new PlannerStatsLimits(maxTables, maxTargets, maxResultsPerChunk));
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
    return forTesting(
        statsFactory,
        constraintProvider,
        null,
        statsStore,
        maxTables,
        maxTargets,
        maxResultsPerChunk);
  }

  public static PlannerStatsBundleService forTesting(
      StatsProviderFactory statsFactory,
      ConstraintProvider constraintProvider,
      ConstraintRepository constraintRepository,
      StatsStore statsStore,
      int maxTables,
      int maxTargets,
      int maxResultsPerChunk) {
    return new PlannerStatsBundleService(
        statsFactory,
        () -> constraintProvider == null ? ConstraintProvider.NONE : constraintProvider,
        constraintRepository,
        RequestScopeConstraintPruner::new,
        RequestScopeConstraintPruner::forRequestedTablesOnly,
        (tableId, snapshotId, statsGenerationRef, targets, policy, deadlineNanos) -> {
          Map<String, PlannerTargetStatsLookupResult> byTarget = new LinkedHashMap<>();
          for (PlannerStatsTargetNeed target : targets) {
            PlannerTargetStatsLookupResult result =
                statsStore
                    .getTargetStats(tableId, snapshotId, target.target())
                    .map(PlannerTargetStatsLookupResult::hit)
                    .orElseGet(() -> PlannerTargetStatsLookupResult.skipped("test_store_miss"))
                    .withStaleFallback(
                        policy.staleOk(),
                        () -> statsStore.getStaleTargetStats(tableId, snapshotId, target.target()));
            byTarget.put(StatsTargetIdentity.storageId(target.target()), result);
          }
          return Map.copyOf(byTarget);
        },
        new PlannerStatsLimits(maxTables, maxTargets, maxResultsPerChunk));
  }

  public Multi<TargetStatsBundleChunk> streamTargets(
      String correlationId, QueryContext ctx, FetchTargetStatsRequest request) {
    PhaseDiagnostics diagnostics = diagnostics("planner_target_stats");
    long startedNanos = System.nanoTime();
    FetchTargetStatsRequest safeRequest =
        request == null ? FetchTargetStatsRequest.getDefaultInstance() : request;
    diagnostics.put("correlation_id", correlationId);
    diagnostics.put("query_id", ctx == null ? "" : ctx.getQueryId());
    diagnostics.put("include_constraints", safeRequest.getIncludeConstraints());
    diagnostics.put("requested_tables_raw", safeRequest.getTablesCount());
    try {
      PlannerStatsNormalizedRequest normalized =
          diagnostics.time(
              "normalize_request",
              () -> requestNormalizer.normalizeTargets(correlationId, safeRequest));
      PlannerStatsServingPolicy servingPolicy =
          PlannerStatsServingPolicy.from(safeRequest.getOptions(), maxTargets);
      List<PlannerStatsTableRequest> tableRequests = normalized.tables();
      diagnostics.put("requested_tables", tableRequests.size());
      diagnostics.put("requested_targets", normalized.requestedTargets());
      diagnostics.put("max_results_per_chunk", maxResultsPerChunk);
      ConstraintProvider constraintProvider =
          diagnostics.time(
              "constraint_provider",
              () ->
                  safeRequest.getIncludeConstraints()
                      ? constraintProviderSupplier.get()
                      : ConstraintProvider.NONE);
      SnapshotPinLookup pinLookup =
          diagnostics.time(
              "pin_lookup_provider", () -> statsFactory.pinLookupForQuery(ctx, correlationId));
      return Multi.createFrom()
          .<TargetStatsBundleChunk>deferred(
              () -> {
                TargetStatsIterator iterator =
                    new TargetStatsIterator(
                        ctx == null ? "" : ctx.getQueryId(),
                        correlationId,
                        tableRequests,
                        pinLookup,
                        constraintProvider,
                        constraintRepository,
                        safeRequest.getIncludeConstraints(),
                        constraintPrunerFactory,
                        targetStatsLookup,
                        maxResultsPerChunk,
                        tableRequests.size(),
                        normalized.requestedTargets(),
                        normalized.omittedByBudget(),
                        servingPolicy,
                        System.nanoTime() + servingPolicy.latencyBudget().toNanos(),
                        diagnostics,
                        startedNanos);
                return Multi.createFrom()
                    .iterable(() -> iterator)
                    .onCancellation()
                    .invoke(iterator::emitCancelledSummary);
              });
    } catch (RuntimeException | Error e) {
      diagnostics.put("outcome", "failed");
      diagnostics.put("error", e.getClass().getSimpleName());
      diagnostics.nanos("total", System.nanoTime() - startedNanos);
      diagnostics.emit("floecat.planner_target_stats.summary");
      throw e;
    }
  }

  @FunctionalInterface
  private interface TargetStatsLookup {
    Map<String, PlannerTargetStatsLookupResult> get(
        ResourceId tableId,
        long snapshotId,
        Optional<String> statsGenerationRef,
        List<PlannerStatsTargetNeed> targets,
        PlannerStatsServingPolicy policy,
        long deadlineNanos);
  }

  private static TargetStatsLookup providerLookup(
      StatsOrchestrator statsOrchestrator, TableRepository tableRepository) {
    Objects.requireNonNull(statsOrchestrator, "statsOrchestrator");
    Objects.requireNonNull(tableRepository, "tableRepository");
    return (tableId, snapshotId, statsGenerationRef, targets, policy, deadlineNanos) -> {
      if (targets == null || targets.isEmpty()) {
        return Map.of();
      }
      String connectorType = ConnectorTypeResolver.connectorTypeFor(tableRepository, tableId);
      /* Build one StatsCaptureRequest per target, all sharing the same execution parameters.
       * resolvePlannerBatch() will issue a single batch store read, apply stale-before-sync
       * ordering, and only block on sync capture for targets still missing after the stale check.
       * This replaces the old N×resolve() loop that made one store read + optional sync per target. */
      List<StatsCaptureRequest> requests = new ArrayList<>(targets.size());
      for (PlannerStatsTargetNeed target : targets) {
        requests.add(
            StatsCaptureRequest.builder(tableId, snapshotId, target.target())
                .columnSelectors(Set.of())
                .requestedKinds(Set.of())
                .executionMode(
                    policy.allowSyncCapture() ? StatsExecutionMode.SYNC : StatsExecutionMode.ASYNC)
                .connectorType(connectorType)
                .latencyBudget(
                    Optional.empty()) /* managed via deadlineNanos in resolvePlannerBatch */
                .samplingRequested(target.requestsAnySketchPayload())
                .build());
      }

      java.util.Map<String, StatsResolutionResult> resolved =
          statsOrchestrator.resolvePlannerBatchInGeneration(
              requests, statsGenerationRef, policy.staleOk(), deadlineNanos);

      Map<String, PlannerTargetStatsLookupResult> byTarget = new LinkedHashMap<>(targets.size());
      for (PlannerStatsTargetNeed target : targets) {
        StatsResolutionResult r = resolved.get(target.storageId());
        if (r != null && !r.hasStats() && r.outcome() == StatsSyncOutcome.TIMEOUT) {
          LOG.debugf(
              "planner_stats sync_timeout table=%s target=%s detail=%s",
              tableId, target.storageId(), r.outcomeDetail());
        }
        byTarget.put(target.storageId(), PlannerTargetStatsLookupResult.fromResolution(r));
      }
      return Map.copyOf(byTarget);
    };
  }

  public Multi<TableConstraintsBundleChunk> streamConstraints(
      String correlationId, QueryContext ctx, FetchTableConstraintsRequest request) {
    PhaseDiagnostics diagnostics = diagnostics("planner_constraints");
    long startedNanos = System.nanoTime();
    FetchTableConstraintsRequest safeRequest =
        request == null ? FetchTableConstraintsRequest.getDefaultInstance() : request;
    diagnostics.put("correlation_id", correlationId);
    diagnostics.put("query_id", ctx == null ? "" : ctx.getQueryId());
    diagnostics.put("requested_tables_raw", safeRequest.getTableIdsCount());
    try {
      List<ResourceId> tableIds =
          diagnostics.time(
              "normalize_request",
              () -> requestNormalizer.normalizeConstraints(correlationId, safeRequest));
      diagnostics.put("requested_tables", tableIds.size());
      diagnostics.put("max_results_per_chunk", maxResultsPerChunk);
      PlannerConstraintServingPolicy servingPolicy =
          PlannerConstraintServingPolicy.from(safeRequest.getOptions());
      Set<String> requestedRelationKeys = new LinkedHashSet<>();
      for (ResourceId tableId : tableIds) {
        requestedRelationKeys.add(RequestScopeConstraintPruner.relationKey(tableId));
      }
      ConstraintPruner constraintPruner =
          diagnostics.time(
              "constraint_pruner",
              () -> constraintsOnlyPrunerFactory.apply(Set.copyOf(requestedRelationKeys)));
      ConstraintProvider constraintProvider =
          diagnostics.time("constraint_provider", constraintProviderSupplier::get);
      SnapshotPinLookup pinLookup =
          diagnostics.time(
              "pin_lookup_provider", () -> statsFactory.pinLookupForQuery(ctx, correlationId));
      return Multi.createFrom()
          .<TableConstraintsBundleChunk>deferred(
              () -> {
                TableConstraintsIterator iterator =
                    new TableConstraintsIterator(
                        ctx == null ? "" : ctx.getQueryId(),
                        tableIds,
                        pinLookup,
                        constraintProvider,
                        constraintRepository,
                        constraintPruner,
                        maxResultsPerChunk,
                        servingPolicy,
                        diagnostics,
                        startedNanos);
                return Multi.createFrom()
                    .iterable(() -> iterator)
                    .onCancellation()
                    .invoke(iterator::emitCancelledSummary);
              });
    } catch (RuntimeException | Error e) {
      diagnostics.put("outcome", "failed");
      diagnostics.put("error", e.getClass().getSimpleName());
      diagnostics.nanos("total", System.nanoTime() - startedNanos);
      diagnostics.emit("floecat.planner_constraints.summary");
      throw e;
    }
  }

  // ---------------------------------------------------------------------------
  // Shared iterator base
  // ---------------------------------------------------------------------------

  private abstract static class BundleIterator<C> implements Iterator<C> {

    protected final String queryId;
    protected final PhaseDiagnostics diagnostics;
    protected long seq = 1;
    private final String eventName;
    private final long startedNanos;
    private final long streamStartedNanos;
    private boolean headerEmitted = false;
    private boolean endEmitted = false;
    private boolean summaryEmitted = false;
    private long messages = 0;
    private long chunks = 0;

    protected BundleIterator(
        String queryId, PhaseDiagnostics diagnostics, String eventName, long startedNanos) {
      this.queryId = queryId;
      this.diagnostics = diagnostics == null ? PhaseDiagnostics.NOOP : diagnostics;
      this.eventName = eventName;
      this.startedNanos = startedNanos;
      this.streamStartedNanos = System.nanoTime();
    }

    @Override
    public final boolean hasNext() {
      return !endEmitted;
    }

    @Override
    public final C next() {
      try {
        C chunk;
        if (!headerEmitted) {
          headerEmitted = true;
          chunk = header();
        } else if (hasMoreWork()) {
          chunk = batch();
        } else if (!endEmitted) {
          endEmitted = true;
          chunk = end();
          emitSummary("success", null);
        } else {
          throw new NoSuchElementException();
        }
        chunks++;
        messages++;
        return chunk;
      } catch (RuntimeException | Error e) {
        emitSummary("failed", e);
        throw e;
      }
    }

    protected abstract C header();

    protected abstract C batch();

    protected abstract C end();

    protected abstract boolean hasMoreWork();

    protected void putSummaryCounters() {}

    protected void emitCancelledSummary() {
      emitSummary("cancelled", null);
    }

    private void emitSummary(String outcome, Throwable error) {
      if (summaryEmitted) {
        return;
      }
      summaryEmitted = true;
      diagnostics.put("outcome", outcome);
      if (error != null) {
        diagnostics.put("error", error.getClass().getSimpleName());
      }
      putSummaryCounters();
      diagnostics.put("chunks", chunks);
      diagnostics.put("messages", messages);
      diagnostics.nanos("total", System.nanoTime() - startedNanos);
      diagnostics.nanos("stream", System.nanoTime() - streamStartedNanos);
      diagnostics.emit(eventName);
    }
  }

  // ---------------------------------------------------------------------------
  // Target stats iterator
  // ---------------------------------------------------------------------------

  private static final class TargetStatsIterator extends BundleIterator<TargetStatsBundleChunk> {

    private final String correlationId;
    private final List<TableWork> tableWorks;
    private final SnapshotPinLookup pinLookup;
    private final ConstraintProvider constraintProvider;
    private final ConstraintRepository constraintRepository;
    private final boolean includeConstraints;
    private final ConstraintPruner constraintPruner;
    private final TargetStatsLookup targetStatsLookup;
    private final int maxResultsPerChunk;
    private final long requestedTables;
    private final long requestedTargets;

    /** Targets already dropped by the target-count cap in normalizeRequest(). */
    private final long preOmittedByBudget;

    private final PlannerStatsServingPolicy servingPolicy;
    private final long requestDeadlineNanos;

    private int nextTableIndex = 0;
    private long returnedTargets = 0;
    private long notFoundTargets = 0;
    private long errorTargets = 0;
    private long omittedByBudget = 0;
    private long partialTargets = 0;
    private long staleTargets = 0;
    private long responseBytes = 0;
    private long constraintBytes = 0;
    private boolean byteBudgetExhausted = false;

    private TargetStatsIterator(
        String queryId,
        String correlationId,
        List<PlannerStatsTableRequest> tables,
        SnapshotPinLookup pinLookup,
        ConstraintProvider constraintProvider,
        ConstraintRepository constraintRepository,
        boolean includeConstraints,
        BiFunction<Set<String>, Map<String, Set<Long>>, ConstraintPruner> constraintPrunerFactory,
        TargetStatsLookup targetStatsLookup,
        int maxResultsPerChunk,
        long requestedTables,
        long requestedTargets,
        long preOmittedByBudget,
        PlannerStatsServingPolicy servingPolicy,
        long requestDeadlineNanos,
        PhaseDiagnostics diagnostics,
        long startedNanos) {
      super(queryId, diagnostics, "floecat.planner_target_stats.summary", startedNanos);
      this.correlationId = correlationId;
      this.pinLookup = pinLookup;
      this.constraintProvider = constraintProvider;
      this.constraintRepository = constraintRepository;
      this.includeConstraints = includeConstraints;
      this.targetStatsLookup = targetStatsLookup;
      this.maxResultsPerChunk = maxResultsPerChunk;
      this.requestedTables = requestedTables;
      this.requestedTargets = requestedTargets;
      this.preOmittedByBudget = preOmittedByBudget;
      this.servingPolicy = servingPolicy;
      this.requestDeadlineNanos = requestDeadlineNanos;
      /* omittedByBudget starts at 0; pre-omitted targets are drained via hasPreOmitted()
       * and each increments omittedByBudget exactly once in processNextTarget(). */
      this.omittedByBudget = 0;
      Set<String> requestedRelationKeys = new LinkedHashSet<>();
      Map<String, Set<Long>> requestedColumnsByRelationKey = new LinkedHashMap<>();
      this.tableWorks = new ArrayList<>(tables.size());
      for (PlannerStatsTableRequest table : tables) {
        String key = RequestScopeConstraintPruner.relationKey(table.tableId());
        requestedRelationKeys.add(key);
        Set<Long> requestedColumnIds = new LinkedHashSet<>();
        for (PlannerStatsTargetNeed target : table.targets()) {
          if (target.target().hasColumn()) {
            requestedColumnIds.add(target.target().getColumn().getColumnId());
          }
        }
        requestedColumnsByRelationKey.put(key, requestedColumnIds);
        this.tableWorks.add(
            new TableWork(
                table.tableId(),
                table.targets(),
                table.omittedTargets(),
                table.snapshotOverride()));
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
      return nextTableIndex < tableWorks.size() && (!byteBudgetExhausted || includeConstraints);
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
      while (out.size() + constraintsOut.size() < maxResultsPerChunk && hasMoreWork()) {
        TableWork work = tableWorks.get(nextTableIndex);
        TargetStatsResult result = work.hasMoreTargets() ? processNextTarget(work) : null;
        if (result != null) {
          out.add(result);
        }
        /* Emit constraints for this table even when target stats are omitted by budget.
         * Constraint bytes are charged to max_constraint_bytes, not max_response_bytes,
         * because constraints are relation metadata rather than column stats payload. */
        if (includeConstraints && !work.constraintsEmitted()) {
          var constraintResult = chargeConstraintResult(processConstraints(work));
          constraintsOut.add(constraintResult);
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
              .setRequestedTargets(requestedTargets + preOmittedByBudget)
              .setReturnedTargets(returnedTargets)
              .setNotFoundTargets(notFoundTargets)
              .setErrorTargets(errorTargets)
              .setOmittedByBudget(omittedByBudget)
              .setPartialTargets(partialTargets)
              .setStaleTargets(staleTargets)
              .build();
      return TargetStatsBundleChunk.newBuilder()
          .setQueryId(queryId)
          .setSeq(seq++)
          .setEnd(end)
          .build();
    }

    @Override
    protected void putSummaryCounters() {
      diagnostics.put("requested_tables", requestedTables);
      diagnostics.put("requested_targets", requestedTargets);
      diagnostics.put("returned_targets", returnedTargets);
      diagnostics.put("not_found_targets", notFoundTargets);
      diagnostics.put("error_targets", errorTargets);
      diagnostics.put("omitted_by_budget", omittedByBudget);
      diagnostics.put("partial_targets", partialTargets);
      diagnostics.put("stale_targets", staleTargets);
      diagnostics.put("response_bytes", responseBytes);
      diagnostics.put("constraint_bytes", constraintBytes);
    }

    private TargetStatsResult processNextTarget(TableWork work) {
      /* Drain pre-omitted (count-cap) targets first with explicit per-target status. */
      if (work.hasPreOmitted()) {
        TargetStatsResult omitted =
            stampPinnedSnapshot(
                work, omittedByBudgetResult(work.tableId, work.peekPreOmitted().target()));
        work.nextPreOmitted(); /* advance before omitAllRemainingByBudget for correct count */
        omittedByBudget++;
        if (!tryCharge(omitted)) {
          /* Hyper-tight cap: even the ~50-byte accounting result doesn't fit. */
          omitAllRemainingByBudget();
          return null;
        }
        return omitted;
      }

      PlannerStatsTargetNeed need = work.targetAt(work.targetIndex());
      StatsTarget target = need.target();
      ResourceId tableId = work.tableId;

      OptionalLong snapshot = resolveSnapshotTimed(work);
      TargetStatsResult result;
      TargetResultCounter counter = TargetResultCounter.NONE;
      if (snapshot.isEmpty()) {
        result = pinMissingResult(tableId, target);
        counter = TargetResultCounter.ERROR;
      } else {
        try {
          loadTargetBatchTimed(work, snapshot.getAsLong());
          PlannerTargetStatsLookupResult lookupResult = work.lookupTarget(need.storageId());
          if (lookupResult.stats().isPresent()) {
            PlannerStatsResultMaterializer.Materialized materialized =
                PlannerStatsResultMaterializer.materialize(need, lookupResult);
            StatsResultStatus status = materialized.status();
            counter =
                switch (status) {
                  case STATS_RESULT_HIT_STALE -> TargetResultCounter.STALE;
                  case STATS_RESULT_HIT_PARTIAL -> TargetResultCounter.PARTIAL;
                  default -> TargetResultCounter.RETURNED;
                };
            /* A result can be simultaneously stale AND partial; the primary status is STALE
             * but partialTargets must also be incremented so the end chunk is accurate. */
            if (status == StatsResultStatus.STATS_RESULT_HIT_STALE
                && materialized.returnedStats().stream()
                    .anyMatch(s -> s.getStatus() != StatsResultStatus.STATS_RESULT_HIT_COMPLETE)) {
              partialTargets++;
            }
            result = PlannerStatsResultMaterializer.buildFoundResult(materialized);
          } else {
            if (lookupResult.outcome() == StatsSyncOutcome.FAILED) {
              result =
                  buildErrorResult(
                      tableId,
                      target,
                      snapshot.getAsLong(),
                      new RuntimeException(lookupResult.outcomeDetail()));
              counter = TargetResultCounter.ERROR;
            } else if (resultStatusForMiss(lookupResult)
                == StatsResultStatus.STATS_RESULT_SYNC_CAPTURE_TIMEOUT) {
              result = timeoutResult(tableId, target, snapshot, lookupResult.outcomeDetail());
              counter = TargetResultCounter.ERROR;
            } else {
              result =
                  notFoundResult(
                      tableId, target, TARGET_MISSING_CODE, missingMessage(lookupResult), snapshot);
              counter = TargetResultCounter.NOT_FOUND;
            }
          }
        } catch (RuntimeException e) {
          result = buildErrorResult(tableId, target, snapshot.getAsLong(), e);
          counter = TargetResultCounter.ERROR;
          LOG.debugf(
              e, "target stats lookup failed for %s snapshot %s", tableId, snapshot.getAsLong());
        }
      }
      result = stampPinnedSnapshot(work, result);
      if (!tryCharge(result)) {
        TargetStatsResult omitted =
            stampPinnedSnapshot(work, omittedByBudgetResult(tableId, target));
        /* Advance first so omitAllRemainingByBudget's remainingTargetCount() excludes
         * the current target (which we're already accounting for with omittedByBudget++). */
        work.advanceTarget();
        omittedByBudget++;
        if (!tryCharge(omitted)) {
          /* Hyper-tight cap: even the ~50-byte accounting result doesn't fit. */
          omitAllRemainingByBudget();
          return null;
        }
        /* Emit OMITTED_BY_BUDGET and batch-omit all remaining (avoids looping). */
        omitAllRemainingByBudget();
        return omitted;
      }
      incrementCounter(counter);
      work.advanceTarget();
      return result;
    }

    private void incrementCounter(TargetResultCounter counter) {
      switch (counter) {
        case RETURNED -> returnedTargets++;
        case NOT_FOUND -> notFoundTargets++;
        case ERROR -> errorTargets++;
        case PARTIAL -> {
          returnedTargets++;
          partialTargets++;
        }
        case STALE -> {
          returnedTargets++;
          staleTargets++;
        }
        case NONE -> {}
      }
    }

    private boolean tryCharge(TargetStatsResult result) {
      if (servingPolicy.maxResponseBytes() <= 0) {
        return true;
      }
      // Unlike the constraint budget, the response budget charges every emitted result (incl.
      // NOT_FOUND envelopes) for wire-size safety.
      int candidateBytes = result.getSerializedSize() + RESULT_FRAMING_BYTES;
      if (responseBytes + candidateBytes > servingPolicy.maxResponseBytes()) {
        return false;
      }
      responseBytes += candidateBytes;
      return true;
    }

    private void omitAllRemainingByBudget() {
      omittedByBudget += remainingTargetCount();
      byteBudgetExhausted = true;
      for (int i = nextTableIndex; i < tableWorks.size(); i++) {
        tableWorks.get(i).markTargetsOmitted();
      }
    }

    private long remainingTargetCount() {
      long remaining = 0;
      for (int i = nextTableIndex; i < tableWorks.size(); i++) {
        remaining += tableWorks.get(i).remainingTargetCount();
      }
      return remaining;
    }

    private TableConstraintsResult processConstraints(TableWork work) {
      return resolveConstraintsTimed(work).result();
    }

    private TableConstraintsResult chargeConstraintResult(TableConstraintsResult result) {
      int charge =
          constraintChargeBytes(result, constraintBytes, servingPolicy.maxConstraintBytes());
      if (charge < 0) {
        return constraintOmittedByBudgetResult(result.getTableId());
      }
      constraintBytes += charge;
      return result;
    }

    private OptionalLong resolveSnapshotTimed(TableWork work) {
      return diagnostics.time("pin_resolve", () -> resolveSnapshot(work));
    }

    private void loadTargetBatchTimed(TableWork work, long snapshotId) {
      diagnostics.time(
          "target_batch_lookup",
          () ->
              work.ensureTargetBatchLoaded(
                  targetStatsLookup,
                  snapshotId,
                  pinLookup.pinnedStatsGenerationRef(work.tableId),
                  servingPolicy,
                  requestDeadlineNanos));
    }

    private ConstraintResolution resolveConstraintsTimed(TableWork work) {
      return diagnostics.time(
          "constraint_resolve",
          () ->
              resolveConstraintResult(
                  work.tableId,
                  resolveSnapshot(work),
                  pinLookup.pinnedConstraintsRef(work.tableId),
                  constraintRepository,
                  constraintProvider,
                  constraintPruner));
    }

    /** The query's pinned snapshot for this table, resolved once and memoized on the work item. */
    private OptionalLong pinnedSnapshotFor(TableWork work) {
      if (!work.pinResolved) {
        work.pinnedSnapshot = pinLookup.pinnedSnapshotId(work.tableId);
        work.pinResolved = true;
      }
      return work.pinnedSnapshot;
    }

    private OptionalLong resolveSnapshot(TableWork work) {
      /*
       * The query pin is authoritative: stats (and the correctness constraints served from the same
       * resolver) are read only at the pinned snapshot. A request snapshot_id may restate the pinned
       * snapshot but must never redirect reads to a different one — a divergent value is a query
       * consistency error, not a silent bypass.
       */
      OptionalLong pinned = pinnedSnapshotFor(work);
      if (work.snapshotOverride.isPresent()
          && pinned.isPresent()
          && work.snapshotOverride.getAsLong() != pinned.getAsLong()) {
        throw GrpcErrors.preconditionFailed(
            correlationId,
            QUERY_TABLE_PIN_CONFLICT,
            Map.of(
                "table_id", work.tableId.getId(),
                "pinned_snapshot", Long.toString(pinned.getAsLong()),
                "requested_snapshot", Long.toString(work.snapshotOverride.getAsLong())));
      }
      // Invariant (confirmed): the planner never issues a stats request carrying a snapshot_id
      // override for a table it did not pin, so an override with an empty pin is unreachable. If
      // that ever changes, an unpinned override would be silently dropped here (empty stats), which
      // this branch would rather surface than serve — revisit before allowing unpinned overrides.
      return pinned;
    }

    /**
     * Stamp the query's pinned snapshot onto a per-target result so the planner can tell whether
     * the served stats (snapshot_id) are behind the pinned snapshot. No-op when the table is not
     * pinned. Reuses the same memoized lookup as {@link #resolveSnapshot}.
     */
    private TargetStatsResult stampPinnedSnapshot(TableWork work, TargetStatsResult result) {
      OptionalLong pinned = pinnedSnapshotFor(work);
      return pinned.isEmpty()
          ? result
          : result.toBuilder().setPinnedSnapshotId(pinned.getAsLong()).build();
    }

    private static TargetStatsResult omittedByBudgetResult(ResourceId tableId, StatsTarget target) {
      return TargetStatsResult.newBuilder()
          .setTableId(tableId)
          .setTarget(target)
          .setStatus(StatsResultStatus.STATS_RESULT_OMITTED_BY_BUDGET)
          .build();
    }

    private static StatsResultStatus resultStatusForMiss(
        PlannerTargetStatsLookupResult lookupResult) {
      return lookupResult.outcome() == StatsSyncOutcome.TIMEOUT
          ? StatsResultStatus.STATS_RESULT_SYNC_CAPTURE_TIMEOUT
          : StatsResultStatus.STATS_RESULT_NOT_FOUND;
    }

    private static String missingMessage(PlannerTargetStatsLookupResult lookupResult) {
      if (lookupResult.outcomeDetail() == null || lookupResult.outcomeDetail().isBlank()) {
        return "target stats missing";
      }
      return "target stats missing: " + lookupResult.outcomeDetail();
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
          .setStatus(StatsResultStatus.STATS_RESULT_ERROR)
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
              .setStatus(StatsResultStatus.STATS_RESULT_NOT_FOUND)
              .setFailure(failure.build());
      snapshotId.ifPresent(builder::setSnapshotId);
      return builder.build();
    }

    private TargetStatsResult timeoutResult(
        ResourceId tableId, StatsTarget target, OptionalLong snapshotId, String detail) {
      BundleFailure.Builder failure =
          failureBase(tableId, TARGET_MISSING_CODE, "sync stats capture timed out")
              .putDetails("target", StatsTargetIdentity.storageId(target));
      snapshotId.ifPresent(id -> failure.putDetails("snapshot_id", Long.toString(id)));
      if (detail != null && !detail.isBlank()) {
        failure.putDetails("detail", detail);
      }
      TargetStatsResult.Builder builder =
          TargetStatsResult.newBuilder()
              .setTableId(tableId)
              .setTarget(target)
              .setStatus(StatsResultStatus.STATS_RESULT_SYNC_CAPTURE_TIMEOUT)
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
          .setStatus(StatsResultStatus.STATS_RESULT_ERROR)
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
    private final ConstraintRepository constraintRepository;
    private final ConstraintPruner constraintPruner;
    private final int maxResultsPerChunk;
    private final PlannerConstraintServingPolicy servingPolicy;

    private int nextTableIndex = 0;
    private long returnedTables = 0;
    private long notFoundTables = 0;
    private long errorTables = 0;
    private long omittedTables = 0;
    private long constraintBytes = 0;

    private TableConstraintsIterator(
        String queryId,
        List<ResourceId> tableIds,
        SnapshotPinLookup pinLookup,
        ConstraintProvider constraintProvider,
        ConstraintRepository constraintRepository,
        ConstraintPruner constraintPruner,
        int maxResultsPerChunk,
        PlannerConstraintServingPolicy servingPolicy,
        PhaseDiagnostics diagnostics,
        long startedNanos) {
      super(queryId, diagnostics, "floecat.planner_constraints.summary", startedNanos);
      this.tableIds = tableIds;
      this.pinLookup = pinLookup;
      this.constraintProvider = constraintProvider;
      this.constraintRepository = constraintRepository;
      this.constraintPruner = constraintPruner;
      this.maxResultsPerChunk = maxResultsPerChunk;
      this.servingPolicy = servingPolicy;
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
              .setOmittedByBudget(omittedTables)
              .build();
      return TableConstraintsBundleChunk.newBuilder()
          .setQueryId(queryId)
          .setSeq(seq++)
          .setEnd(end)
          .build();
    }

    @Override
    protected void putSummaryCounters() {
      diagnostics.put("requested_tables", tableIds.size());
      diagnostics.put("returned_tables", returnedTables);
      diagnostics.put("not_found_tables", notFoundTables);
      diagnostics.put("error_tables", errorTables);
      diagnostics.put("omitted_by_budget", omittedTables);
      diagnostics.put("constraint_bytes", constraintBytes);
    }

    private TableConstraintsResult processTable(ResourceId tableId) {
      ConstraintResolution result = chargeConstraintResult(resolveConstraintsTimed(tableId));
      switch (result.status()) {
        case FOUND -> returnedTables++;
        case NOT_FOUND -> notFoundTables++;
        case ERROR -> errorTables++;
        case OMITTED -> omittedTables++;
      }
      return result.result();
    }

    private ConstraintResolution chargeConstraintResult(ConstraintResolution resolution) {
      TableConstraintsResult result = resolution.result();
      int charge =
          constraintChargeBytes(result, constraintBytes, servingPolicy.maxConstraintBytes());
      if (charge < 0) {
        return new ConstraintResolution(
            constraintOmittedByBudgetResult(result.getTableId()),
            ConstraintResolutionStatus.OMITTED);
      }
      constraintBytes += charge;
      return resolution;
    }

    private ConstraintResolution resolveConstraintsTimed(ResourceId tableId) {
      return diagnostics.time(
          "constraint_resolve",
          () ->
              resolveConstraintResult(
                  tableId,
                  resolveSnapshotTimed(tableId),
                  pinLookup.pinnedConstraintsRef(tableId),
                  constraintRepository,
                  constraintProvider,
                  constraintPruner));
    }

    private OptionalLong resolveSnapshotTimed(ResourceId tableId) {
      return diagnostics.time("pin_resolve", () -> pinLookup.pinnedSnapshotId(tableId));
    }
  }

  // ---------------------------------------------------------------------------
  // Constraint resolution (shared between both iterators)
  // ---------------------------------------------------------------------------

  private enum ConstraintResolutionStatus {
    FOUND,
    NOT_FOUND,
    ERROR,
    OMITTED
  }

  private enum TargetResultCounter {
    NONE,
    RETURNED,
    NOT_FOUND,
    ERROR,
    PARTIAL,
    STALE
  }

  private record ConstraintResolution(
      TableConstraintsResult result, ConstraintResolutionStatus status) {}

  /**
   * Serve the constraints frozen on the query's pin. A user table's bundle loads by the immutable
   * ref the pin copied from its root entry — never the live pointer — so constraints are
   * deterministic for the query's lifetime: a pin with no ref stays constraint-free even if a
   * bundle appears mid-query, and an in-place constraints write never changes what a running query
   * sees. System relations (no pins) resolve through the routed provider as before. A pinned bundle
   * whose blob is gone is a catalog-integrity error, not a silent walk to live state.
   */
  private static ConstraintResolution resolveConstraintResult(
      ResourceId tableId,
      OptionalLong snapshotId,
      Optional<SnapshotPinLookup.PinnedConstraintsRef> pinnedRef,
      ConstraintRepository constraintRepository,
      ConstraintProvider constraintProvider,
      ConstraintPruner constraintPruner) {
    // No early pin-missing return on an empty snapshot: SYSTEM relations are provider-backed and
    // carry no snapshot pin, so they must reach the routed provider below. A USER table with no pin
    // is the real pin-missing case, distinguished in the unpinned branch. -1 stands in for "no
    // pinned snapshot" in the log lines.
    long sidForLog = snapshotId.orElse(-1L);
    try {
      List<ConstraintDefinition> visible;
      String servedRefVersion;
      if (pinnedRef.isPresent()) {
        if (constraintRepository == null) {
          // A pinned ref with no repository to load it from is a wiring bug (or a test factory
          // exercising a path it did not set up), not a missing-blob condition — fail loudly
          // instead of logging a broken-invariant warning that points at the wrong culprit.
          throw new IllegalStateException(
              "pinned constraints ref present but no ConstraintRepository is wired: "
                  + pinnedRef.get().uri());
        }
        Optional<SnapshotConstraints> bundle =
            // LIVE: emptiness here is the broken-retention detector (pinned blobs are GC-rooted
            // for the query's lifetime); a resident decode must not suppress the warning exactly
            // where traffic — and cache warmth — is highest.
            constraintRepository.getByBlobUriLive(pinnedRef.get().uri());
        if (bundle.isEmpty()) {
          // The pin froze a bundle ref whose blob is no longer retrievable: pinned blobs are
          // GC-rooted for the query's lifetime, so this is a broken invariant, never client state.
          LOG.warnf(
              "pinned constraints blob missing for %s snapshot %d: %s",
              tableId.getId(), sidForLog, pinnedRef.get().uri());
          return new ConstraintResolution(
              constraintErrorResult(
                  tableId,
                  snapshotId,
                  new IllegalStateException(
                      "pinned constraints blob missing: " + pinnedRef.get().uri())),
              ConstraintResolutionStatus.ERROR);
        }
        visible = bundle.get().getConstraintsList();
        servedRefVersion = pinnedRef.get().version();
      } else {
        // No ref on the pin. System relations resolve through the routed provider (which ignores
        // the absent snapshot); a pinned user table deterministically has no constraints for this
        // query (none existed at pin time); an UNPINNED user table is a real pin-missing.
        var systemView = constraintProvider.constraints(tableId, snapshotId);
        if (systemView.isEmpty()) {
          if (snapshotId.isEmpty()) {
            // Unpinned and the routed provider served nothing — a user table that was never pinned
            // (a system relation would have been served above). This is the real pin-missing case.
            LOG.debugf("constraint pin missing for %s", tableId.getId());
            return new ConstraintResolution(
                constraintPinMissingResult(tableId), ConstraintResolutionStatus.ERROR);
          }
          LOG.tracef("no constraints pinned for %s snapshot %d", tableId.getId(), sidForLog);
          return new ConstraintResolution(
              constraintNotFoundResult(tableId, snapshotId, "absent_at_pin"),
              ConstraintResolutionStatus.NOT_FOUND);
        }
        visible = systemView.get().constraints();
        servedRefVersion = "";
      }

      // Deliberate client-contract choice: constraints "absence" states are intentionally
      // collapsed to NOT_FOUND for planner simplicity, while callers can distinguish the cause via
      // failure.details["reason"]:
      // - absent_at_pin: no bundle existed when the query pinned the table
      // - provider_empty: a pinned/system bundle with zero constraints
      // - pruned_empty: bundle exists but request-scope pruning hid all constraints
      if (visible.isEmpty()) {
        LOG.debugf("constraints bundle empty for %s snapshot %d", tableId.getId(), sidForLog);
        return new ConstraintResolution(
            constraintNotFoundResult(tableId, snapshotId, "provider_empty"),
            ConstraintResolutionStatus.NOT_FOUND);
      }
      visible = constraintPruner.prune(tableId, visible);

      // Semantics choice: provider FOUND + prune-to-empty is surfaced as NOT_FOUND to callers.
      // This keeps client handling simple: no visible constraints for the request scope.
      if (visible.isEmpty()) {
        LOG.tracef("all constraints pruned for %s snapshot %d", tableId.getId(), sidForLog);
        return new ConstraintResolution(
            constraintNotFoundResult(tableId, snapshotId, "pruned_empty"),
            ConstraintResolutionStatus.NOT_FOUND);
      }

      LOG.tracef(
          "constraints resolved for %s snapshot %d: %d constraints",
          tableId.getId(), sidForLog, visible.size());
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
              // Echo the pinned ref version so the result matches the pin identity.
              .setConstraintsRefVersion(servedRefVersion)
              .addAllConstraints(visible)
              .build(),
          ConstraintResolutionStatus.FOUND);
    } catch (RuntimeException e) {
      LOG.debugf(e, "constraint lookup failed for %s snapshot %d", tableId.getId(), sidForLog);
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

  /** Framing overhead (bytes) budgeted per emitted result, approximating proto/gRPC wrapping. */
  private static final int RESULT_FRAMING_BYTES = 8;

  /**
   * Bytes to charge against the constraint byte budget for {@code result}, or {@code -1} if a FOUND
   * result would exceed {@code maxBytes} (budget exhausted). Only FOUND results carry a constraint
   * payload; failure envelopes (NOT_FOUND/ERROR/OMITTED) and an uncapped budget ({@code maxBytes <=
   * 0}) return 0 — they fit and cost nothing, so a run of failing tables cannot starve a later
   * table's real constraints.
   */
  private static int constraintChargeBytes(
      TableConstraintsResult result, long usedBytes, long maxBytes) {
    if (maxBytes <= 0 || result.getStatus() != BundleResultStatus.BUNDLE_RESULT_STATUS_FOUND) {
      return 0;
    }
    int candidate = result.getSerializedSize() + RESULT_FRAMING_BYTES;
    return usedBytes + candidate <= maxBytes ? candidate : -1;
  }

  private static TableConstraintsResult constraintOmittedByBudgetResult(ResourceId tableId) {
    BundleFailure failure =
        failureBase(tableId, CONSTRAINT_OMITTED_CODE, "constraints omitted by budget").build();
    return TableConstraintsResult.newBuilder()
        .setTableId(tableId)
        .setStatus(BundleResultStatus.BUNDLE_RESULT_STATUS_OMITTED_BY_BUDGET)
        .setFailure(failure)
        .build();
  }

  // ---------------------------------------------------------------------------
  // Supporting types
  // ---------------------------------------------------------------------------

  private static final class TableWork {
    private final ResourceId tableId;

    /** Normal targets to serve after pre-omitted are drained. */
    private final List<PlannerStatsTargetNeed> targets;

    /** Targets dropped by the count cap; drained first as OMITTED_BY_BUDGET. */
    private final List<PlannerStatsTargetNeed> preOmitted;

    /**
     * Explicit snapshot id restated by the request. The query pin stays authoritative: a value that
     * matches the pin is accepted, a divergent one fails with QUERY_TABLE_PIN_CONFLICT (see
     * resolveSnapshot) — it never redirects reads away from the pinned snapshot.
     */
    private final OptionalLong snapshotOverride;

    /** The query's pinned snapshot for this table, resolved once and reused for stamping. */
    private OptionalLong pinnedSnapshot = OptionalLong.empty();

    private boolean pinResolved = false;

    /** Cursor into preOmitted — independent of targetIndex. */
    private int preOmittedIndex = 0;

    /** Cursor into targets. */
    private int targetIndex = 0;

    private boolean constraintsEmitted = false;
    private OptionalLong loadedSnapshot = OptionalLong.empty();
    private Optional<String> loadedStatsGenerationRef = Optional.empty();
    private Map<String, PlannerTargetStatsLookupResult> loadedTargetsById = Map.of();
    private RuntimeException loadFailure;

    private TableWork(
        ResourceId tableId,
        List<PlannerStatsTargetNeed> targets,
        List<PlannerStatsTargetNeed> preOmitted,
        OptionalLong snapshotOverride) {
      this.tableId = tableId;
      this.targets = targets;
      this.preOmitted = preOmitted;
      this.snapshotOverride = snapshotOverride != null ? snapshotOverride : OptionalLong.empty();
    }

    /** True when pre-omitted targets remain to drain. */
    private boolean hasPreOmitted() {
      return preOmittedIndex < preOmitted.size();
    }

    private PlannerStatsTargetNeed peekPreOmitted() {
      return preOmitted.get(preOmittedIndex);
    }

    /** Returns the next pre-omitted target and advances the pre-omitted cursor. */
    private PlannerStatsTargetNeed nextPreOmitted() {
      return preOmitted.get(preOmittedIndex++);
    }

    private boolean hasMoreTargets() {
      return preOmittedIndex < preOmitted.size() || targetIndex < targets.size();
    }

    private PlannerStatsTargetNeed targetAt(int index) {
      return targets.get(index);
    }

    private void advanceTarget() {
      targetIndex++;
    }

    private int targetIndex() {
      return targetIndex; /* direct — no arithmetic needed */
    }

    private boolean constraintsEmitted() {
      return constraintsEmitted;
    }

    private void markConstraintsEmitted() {
      this.constraintsEmitted = true;
    }

    private void ensureTargetBatchLoaded(
        TargetStatsLookup lookup,
        long snapshotId,
        Optional<String> statsGenerationRef,
        PlannerStatsServingPolicy servingPolicy,
        long deadlineNanos) {
      Optional<String> normalizedStatsGenerationRef =
          statsGenerationRef == null
              ? Optional.empty()
              : statsGenerationRef.filter(token -> !token.isBlank());
      if (loadedSnapshot.isPresent()
          && loadedSnapshot.getAsLong() == snapshotId
          && loadedStatsGenerationRef.equals(normalizedStatsGenerationRef)) {
        if (loadFailure != null) {
          throw loadFailure;
        }
        return;
      }
      loadedSnapshot = OptionalLong.of(snapshotId);
      loadedStatsGenerationRef = normalizedStatsGenerationRef;
      try {
        loadedTargetsById =
            lookup.get(
                tableId,
                snapshotId,
                normalizedStatsGenerationRef,
                targets,
                servingPolicy,
                deadlineNanos);
        loadFailure = null;
      } catch (RuntimeException e) {
        loadedTargetsById = Map.of();
        loadFailure = e;
        throw e;
      }
    }

    /**
     * Look up stats using the pre-computed storageId from the PlannerStatsTargetNeed (avoids
     * recomputing).
     */
    private PlannerTargetStatsLookupResult lookupTarget(String storageId) {
      if (loadedTargetsById == null || loadedTargetsById.isEmpty()) {
        return PlannerTargetStatsLookupResult.skipped("not_loaded");
      }
      return loadedTargetsById.getOrDefault(
          storageId, PlannerTargetStatsLookupResult.skipped("missing"));
    }

    private long remainingTargetCount() {
      return (preOmitted.size() - preOmittedIndex) + (targets.size() - targetIndex);
    }

    private void markTargetsOmitted() {
      preOmittedIndex = preOmitted.size();
      targetIndex = targets.size();
    }
  }

  private PhaseDiagnostics diagnostics(String operation) {
    return observability == null
        ? PhaseDiagnostics.NOOP
        : observability.diagnostics("service", operation);
  }
}
