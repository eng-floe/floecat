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

package ai.floedb.floecat.service.query.resolver;

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.*;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.QueryInput;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.query.rpc.PinKind;
import ai.floedb.floecat.query.rpc.RelationPinSet;
import ai.floedb.floecat.query.rpc.SnapshotSet;
import ai.floedb.floecat.query.rpc.TablePin;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.service.concurrent.Futures;
import ai.floedb.floecat.service.concurrent.MetadataFanout;
import ai.floedb.floecat.service.context.PropagatedContext;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.query.QueryPins;
import ai.floedb.floecat.service.query.ViewContextUtils;
import ai.floedb.floecat.service.repo.util.RepositoryReads;
import ai.floedb.floecat.telemetry.AggregatingPhaseDiagnostics;
import ai.floedb.floecat.telemetry.PhaseDiagnostics;
import com.google.protobuf.Timestamp;
import io.grpc.Context;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;

/**
 * QueryInputResolver
 *
 * <p>Resolves {@link QueryInput} into:
 *
 * <ul>
 *   <li>Resolved {@link ResourceId} (table/view)
 *   <li>A blob-backed {@link TablePin} per table, always resolved to a concrete snapshot (an AS_OF
 *       reference resolves once; its timestamp is kept only as provenance on the pin)
 * </ul>
 *
 * <p>This is invoked by DescribeInputs() and GetUserObjects(), before any QueryContext exists.
 *
 * <p>Behavior:
 *
 * <ol>
 *   <li>Resolve NameRef → Table or View
 *   <li>Apply explicit snapshot overrides
 *   <li>Apply as-of defaults when present
 *   <li>Fallback to SNAPSHOT(CURRENT) for tables
 *   <li>Views never use snapshots
 *   <li>View base-relation NameRefs are enriched before resolution: if {@code catalog} is blank the
 *       query's default catalog is substituted; if {@code path} is empty the view's {@code
 *       creationSearchPath} is used — this ensures base relations re-resolve exactly as they did at
 *       view-creation time, regardless of the current query search-path.
 * </ol>
 *
 * <p>This resolver does not persist query context. While resolving, it registers each constructed
 * pin as a transient GC root and releases discarded or abandoned registrations when the resolution
 * attempt ends.
 */
@ApplicationScoped
public class QueryInputResolver {

  private static final Logger LOG = Logger.getLogger(QueryInputResolver.class);
  private static final int DEFAULT_MAX_PARALLEL_INPUT_RESOLUTIONS = 8;
  private static final int MAX_PARALLEL_INPUT_RESOLUTIONS = 16;
  private static final long SNAPSHOT_WAIT_POLL_MILLIS = 10;

  // Cap on inputs resolved concurrently. Each is an independent, mostly-blocking chain of metadata
  // store reads; a small fan-out overlaps their round-trips without flooding the store. This is a
  // per-request bound only; each table-pin chain also passes through pinResolutionReads so all
  // requests share the process-wide metadata-I/O ceiling. ConfigProvider keeps construction
  // dependent on graph/store collaborators while production reads the deployment setting once.
  private final int maxParallelInputResolutions;
  private final CatalogOverlay metadataGraph;
  // Admits one complete table-pin chain. Its root/snapshot repositories intentionally stay direct;
  // admitting their leaves too would acquire the same process gate twice for one operation.
  private final RepositoryReads.ReadPolicy pinResolutionReads;

  // Registers each resolved pin's blobs as a transient GC root at construction time (see
  // QueryContextStore.registerResolvingPinBlobs). Null in unit tests that construct the resolver
  // without a store — registration is simply skipped then.
  private final QueryContextStore queryStore;

  @Inject
  public QueryInputResolver(
      CatalogOverlay metadataGraph,
      QueryContextStore queryStore,
      RepositoryReads.ReadPolicy pinResolutionReads) {
    this.metadataGraph = metadataGraph;
    this.queryStore = queryStore;
    this.pinResolutionReads = pinResolutionReads;
    this.maxParallelInputResolutions = configuredMaxParallelInputResolutions();
  }

  /** Compatibility constructor for direct callers that own their metadata execution policy. */
  public QueryInputResolver(CatalogOverlay metadataGraph, QueryContextStore queryStore) {
    this(metadataGraph, queryStore, RepositoryReads.directPolicy());
  }

  /** Test-only constructor: no store (no pin-root registration). */
  public QueryInputResolver(CatalogOverlay metadataGraph) {
    this(metadataGraph, null);
  }

  /** Read and clamp the per-request fan-out width so invalid deployment values remain safe. */
  private static int configuredMaxParallelInputResolutions() {
    int configured =
        ConfigProvider.getConfig()
            .getOptionalValue("floecat.query.resolver.max_parallel_inputs", Integer.class)
            .orElse(DEFAULT_MAX_PARALLEL_INPUT_RESOLUTIONS);
    int clamped = Math.max(1, Math.min(MAX_PARALLEL_INPUT_RESOLUTIONS, configured));
    if (configured != clamped) {
      LOG.warnf(
          "floecat.query.resolver.max_parallel_inputs must be between 1 and %d; using %d"
              + " instead of %d",
          MAX_PARALLEL_INPUT_RESOLUTIONS, clamped, configured);
    }
    return clamped;
  }

  // =============================================================================
  // Result container
  // =============================================================================

  /** Immutable container returned to callers. */
  public record ResolutionResult(
      List<ResourceId> resolved, RelationPinSet relationPinSet, byte[] asOfDefaultBytes) {
    /** Projection for read-only consumers that still speak SnapshotPin. */
    public SnapshotSet snapshotSet() {
      return QueryPins.toSnapshotSet(relationPinSet);
    }
  }

  // =============================================================================
  // Per-call accumulation state
  // =============================================================================

  /** Context shared by input-planning workers; every mutable collaborator is thread-safe. */
  private record ResolutionWork(
      String queryId,
      String correlationId,
      Optional<Timestamp> asOfDefault,
      Optional<String> defaultCatalog,
      ConcurrentMap<ResourceId, CompletableFuture<TablePin>> currentSnapshotPinCache,
      CurrentSnapshotCacheOwnership currentSnapshotCacheOwnership,
      ResolvingPinRoots resolvingPinRoots,
      PhaseDiagnostics diagnostics,
      BooleanSupplier cancelled) {

    ResolutionWork {
      diagnostics = diagnostics == null ? PhaseDiagnostics.NOOP : diagnostics;
    }

    /** Keep all shared ownership while directing one worker's metrics to its accumulator. */
    ResolutionWork withDiagnostics(PhaseDiagnostics taskDiagnostics) {
      return new ResolutionWork(
          queryId,
          correlationId,
          asOfDefault,
          defaultCatalog,
          currentSnapshotPinCache,
          currentSnapshotCacheOwnership,
          resolvingPinRoots,
          taskDiagnostics,
          cancelled);
    }
  }

  /** Request-thread-only ordered accumulation for one resolution call. */
  private static final class ResolutionState {
    final ResolutionWork work;
    final List<ResourceId> resolved = new ArrayList<>();
    // Keep insertion order (matching input order) while deduplicating by table ID.
    final Map<ResourceId, TablePin> pinByTableId = new LinkedHashMap<>();

    ResolutionState(ResolutionWork work) {
      this.work = work;
    }
  }

  // =============================================================================
  // Main resolution entrypoint
  // =============================================================================

  /**
   * Convenience overload with no query id (resolving-pin roots are not registered), no shared
   * current-snapshot pin cache, and no diagnostics. Used by unit tests that exercise resolution in
   * isolation.
   */
  public ResolutionResult resolveInputs(
      String correlationId,
      List<QueryInput> inputs,
      Optional<Timestamp> asOfDefault,
      Optional<ResourceId> defaultCatalogId) {
    return resolveInputs(
        "",
        correlationId,
        inputs,
        asOfDefault,
        defaultCatalogId,
        new ConcurrentHashMap<ResourceId, CompletableFuture<TablePin>>(),
        null);
  }

  /**
   * Performs full resolution of inputs:
   *
   * <ul>
   *   <li>NAME ⇒ directory lookup
   *   <li>TABLE_ID / VIEW_ID ⇒ used directly
   *   <li>snapshot override ⇒ enforced
   *   <li>as-of-default ⇒ resolved once to the latest snapshot at or before the timestamp
   *   <li>fallback for tables ⇒ CURRENT snapshot
   * </ul>
   *
   * <p>{@code defaultCatalogId} is used only when expanding view base relations: if a base-relation
   * {@link NameRef} has a blank catalog or empty path it is enriched with the query's default
   * catalog / creation search-path before resolution. Non-view inputs are unaffected. {@code
   * currentSnapshotPinCache} is shared by concurrent input tasks, so it must provide atomic {@link
   * ConcurrentMap#putIfAbsent} and conditional removal.
   */
  public ResolutionResult resolveInputs(
      String queryId,
      String correlationId,
      List<QueryInput> inputs,
      Optional<Timestamp> asOfDefault,
      Optional<ResourceId> defaultCatalogId,
      ConcurrentMap<ResourceId, CompletableFuture<TablePin>> currentSnapshotPinCache,
      PhaseDiagnostics diagnostics) {
    return resolveInputsCore(
        queryId,
        correlationId,
        inputs,
        asOfDefault,
        defaultCatalogId,
        currentSnapshotPinCache,
        diagnostics,
        Context.current()::isCancelled);
  }

  /**
   * Compatibility overload for callers and subclasses using the completed-pin cache contract. Plain
   * maps are copied into the concurrent single-flight representation before fan-out and are updated
   * only after successful resolution on the calling thread.
   */
  public ResolutionResult resolveInputs(
      String queryId,
      String correlationId,
      List<QueryInput> inputs,
      Optional<Timestamp> asOfDefault,
      Optional<ResourceId> defaultCatalogId,
      Map<ResourceId, TablePin> currentSnapshotPinCache,
      PhaseDiagnostics diagnostics) {
    Map<ResourceId, TablePin> initialCache = new LinkedHashMap<>(currentSnapshotPinCache);
    ConcurrentMap<ResourceId, CompletableFuture<TablePin>> singleFlightCache =
        toSingleFlightCache(initialCache);
    ResolutionResult result =
        resolveInputsCore(
            queryId,
            correlationId,
            inputs,
            asOfDefault,
            defaultCatalogId,
            singleFlightCache,
            diagnostics,
            Context.current()::isCancelled);
    try {
      completedPins(singleFlightCache)
          .forEach(
              (tableId, pin) -> {
                if (!initialCache.containsKey(tableId)
                    || !java.util.Objects.equals(initialCache.get(tableId), pin)) {
                  currentSnapshotPinCache.put(tableId, pin);
                }
              });
    } catch (RuntimeException | Error copyFailure) {
      boolean restored = false;
      try {
        if (!currentSnapshotPinCache.equals(initialCache)) {
          currentSnapshotPinCache.clear();
          currentSnapshotPinCache.putAll(initialCache);
        }
        restored = currentSnapshotPinCache.equals(initialCache);
      } catch (RuntimeException | Error rollbackFailure) {
        copyFailure.addSuppressed(rollbackFailure);
      }
      if (restored && queryStore != null && queryId != null && !queryId.isEmpty()) {
        try {
          queryStore.releaseResolvingPinBlobs(
              queryId, QueryPins.gcRootUris(result.relationPinSet()));
        } catch (RuntimeException | Error cleanupFailure) {
          copyFailure.addSuppressed(cleanupFailure);
        }
      }
      throw copyFailure;
    }
    return result;
  }

  /**
   * As {@link #resolveInputs(String, String, List, Optional, Optional, ConcurrentMap,
   * PhaseDiagnostics)}, but stops before additional metadata work and interrupts fan-out tasks when
   * {@code cancelled} becomes true. The supplier may be read concurrently by the caller and worker
   * threads, so it must be non-blocking and thread-safe (typically {@link AtomicBoolean#get}).
   * Observed cancellation throws {@link CancellationException}.
   */
  public ResolutionResult resolveInputs(
      String queryId,
      String correlationId,
      List<QueryInput> inputs,
      Optional<Timestamp> asOfDefault,
      Optional<ResourceId> defaultCatalogId,
      ConcurrentMap<ResourceId, CompletableFuture<TablePin>> currentSnapshotPinCache,
      PhaseDiagnostics diagnostics,
      BooleanSupplier cancelled) {
    return resolveInputsCore(
        queryId,
        correlationId,
        inputs,
        asOfDefault,
        defaultCatalogId,
        currentSnapshotPinCache,
        diagnostics,
        cancelled);
  }

  /** Convert completed pins to the concurrent placeholder representation used during fan-out. */
  private static ConcurrentMap<ResourceId, CompletableFuture<TablePin>> toSingleFlightCache(
      Map<ResourceId, TablePin> completedPinCache) {
    ConcurrentMap<ResourceId, CompletableFuture<TablePin>> singleFlightCache =
        new ConcurrentHashMap<>();
    completedPinCache.forEach(
        (tableId, pin) -> singleFlightCache.put(tableId, CompletableFuture.completedFuture(pin)));
    return singleFlightCache;
  }

  /** Snapshot successfully completed placeholders without waiting for active lookups. */
  private static Map<ResourceId, TablePin> completedPins(
      ConcurrentMap<ResourceId, CompletableFuture<TablePin>> singleFlightCache) {
    Map<ResourceId, TablePin> completed = new LinkedHashMap<>();
    singleFlightCache.forEach(
        (tableId, pinFuture) -> {
          if (pinFuture.isDone()
              && !pinFuture.isCompletedExceptionally()
              && !pinFuture.isCancelled()) {
            completed.put(tableId, Futures.join(pinFuture));
          }
        });
    return completed;
  }

  /** Run the shared resolution implementation for cancellable and non-cancellable callers. */
  private ResolutionResult resolveInputsCore(
      String queryId,
      String correlationId,
      List<QueryInput> inputs,
      Optional<Timestamp> asOfDefault,
      Optional<ResourceId> defaultCatalogId,
      ConcurrentMap<ResourceId, CompletableFuture<TablePin>> currentSnapshotPinCache,
      PhaseDiagnostics diagnostics,
      BooleanSupplier cancelled) {
    throwIfCancelled(cancelled);
    PhaseDiagnostics diag = diagnostics == null ? PhaseDiagnostics.NOOP : diagnostics;

    // Bind the request's cancellation token for the whole resolution so every auto-admitted store
    // read — including reads on fan-out workers — can abort its admission wait when this request
    // cancels. Closing the scope restores the prior binding and never throws.
    try (var cancellationScope = PropagatedContext.bindCancellation(cancelled)) {
      // Resolve catalog display-name once up-front — used to fill in blank catalog fields in
      // view base-relation NameRefs so they re-resolve exactly as they did at view-creation time.
      Optional<String> defaultCatalog = Optional.empty();
      if (metadataGraph != null && defaultCatalogId.isPresent()) {
        diag.count("pin.default_catalog_lookups");
        long defaultCatalogStartNs = System.nanoTime();
        try {
          defaultCatalog =
              metadataGraph.catalog(defaultCatalogId.get()).map(CatalogNode::displayName);
        } finally {
          diag.nanos("pin.default_catalog_resolve", System.nanoTime() - defaultCatalogStartNs);
        }
      }
      throwIfCancelled(cancelled);

      var state =
          new ResolutionState(
              new ResolutionWork(
                  queryId,
                  correlationId,
                  asOfDefault,
                  defaultCatalog,
                  currentSnapshotPinCache,
                  new CurrentSnapshotCacheOwnership(currentSnapshotPinCache),
                  new ResolvingPinRoots(queryStore, queryId),
                  diag,
                  cancelled));

      try {
        // Batch-resolve all NAME inputs up front: names sharing a catalog/namespace resolve their
        // scope once instead of once per input.
        List<NameRef> nameInputs =
            inputs.stream()
                .filter(in -> in.getTargetCase() == QueryInput.TargetCase.NAME)
                .map(QueryInput::getName)
                .toList();
        Map<NameRef, Optional<ResourceId>> resolvedNames =
            nameInputs.isEmpty() ? Map.of() : metadataGraph.resolveNames(correlationId, nameInputs);
        throwIfCancelled(cancelled);

        // Resolve each input to its id and the table pins it contributes (a table yields its own
        // pin; a view yields its base tables' pins). planInput reads the metadata graph and the
        // shared current-snapshot cache; it does not touch `resolved` or `pinByTableId`. Inputs
        // resolve independently, so overlays that explicitly support concurrent resolution fan them
        // out. An overlay that opts out may retain request-thread-confined lifecycle state, so its
        // entire planning loop remains on the caller thread. Results always merge in input order.
        planInputs(state, inputs, resolvedNames, cancelled);
        throwIfCancelled(cancelled);

        RelationPinSet relationPinSet =
            RelationPinSet.newBuilder()
                .addAllPins(state.pinByTableId.values().stream().map(QueryPins::ofTable).toList())
                .build();
        diag.add("pin.resolver_output_pins", relationPinSet.getPinsCount());
        return new ResolutionResult(
            state.resolved, relationPinSet, asOfDefault.map(Timestamp::toByteArray).orElse(null));
      } catch (RuntimeException | Error e) {
        try {
          // Evict attempt-owned cache entries before releasing their transient roots, so another
          // resolve call cannot observe an unrooted completed pin in between those operations.
          state.work.currentSnapshotCacheOwnership().closeAndEvict();
        } catch (RuntimeException | Error cleanupFailure) {
          e.addSuppressed(cleanupFailure);
        }
        try {
          state.work.resolvingPinRoots().releaseAll();
        } catch (RuntimeException | Error cleanupFailure) {
          e.addSuppressed(cleanupFailure);
        }
        throw e;
      }
    }
  }

  // =============================================================================
  // Pin resolution
  // =============================================================================

  /**
   * One input's resolution: the id recorded in {@code resolved}, and the table pins it contributes,
   * ordered. A view's id is recorded but the pins are its base tables'; a table records its id and
   * its own single pin. A view may retain a successfully resolved dependency prefix plus a terminal
   * failure; ordered merge consumes that prefix before rethrowing the deferred failure.
   */
  private record InputPlan(ResourceId resolvedId, List<TablePin> pins, Throwable terminalFailure) {}

  /** Merge one completed plan on the request thread, preserving request-order semantics. */
  private void mergePlan(ResolutionState state, InputPlan plan) {
    state.resolved.add(plan.resolvedId());
    for (TablePin pin : plan.pins()) {
      mergePin(state, pin);
    }
    if (plan.terminalFailure() instanceof RuntimeException runtime) {
      throw runtime;
    }
    if (plan.terminalFailure() instanceof Error error) {
      throw error;
    }
  }

  /**
   * Resolve each input to its id and pins and merge the plans in input order. Inputs are
   * independent, so an overlay that supports concurrent resolution fans them out (a single input
   * stays on the caller thread); an overlay that owns request-thread-confined state runs serially.
   * {@link MetadataFanout} owns that choice and delivers plans in input order (each table-pin chain
   * is admitted as one metadata operation), so the merge stays deterministic (first-touch-wins,
   * conflict detection). Each input gets its own state view so mutable per-plan fields never become
   * shared task state; only immutable resolution context and the explicitly thread-safe
   * single-flight cache, root tracker, and diagnostics accumulator are shared.
   *
   * <p>Tasks report to a shared thread-safe accumulator instead of the request's diagnostics (not
   * guaranteed thread-safe); its per-key totals are flushed to the real diagnostics once resolution
   * has joined, even on a failed/cancelled sibling. The accumulator keeps counters and durations
   * only (it omits one-shot put/emit values that cannot be combined across inputs), and its task
   * timing keys are aggregate work time, so they may exceed the enclosing wall-clock phase.
   */
  private void planInputs(
      ResolutionState state,
      List<QueryInput> inputs,
      Map<NameRef, Optional<ResourceId>> resolvedNames,
      BooleanSupplier cancelled) {
    MetadataFanout fanout =
        metadataGraph.supportsConcurrentResolution()
            ? MetadataFanout.concurrent(maxParallelInputResolutions)
            : MetadataFanout.serial();
    AggregatingPhaseDiagnostics taskDiagnostics = new AggregatingPhaseDiagnostics();
    try {
      fanout.forEachOrdered(
          inputs,
          in -> planInput(state.work.withDiagnostics(taskDiagnostics), in, resolvedNames),
          plan -> mergePlan(state, plan),
          cancelled);
    } finally {
      // A failed or cancelled sibling can still leave completed tasks' pin work in the accumulator.
      // Preserve those counters for the request's failure telemetry too.
      taskDiagnostics.flushInto(state.work.diagnostics);
    }
  }

  private static void throwIfCancelled(BooleanSupplier cancelled) {
    if (cancelled.getAsBoolean()) {
      throw new CancellationException("input resolution cancelled");
    }
  }

  /**
   * Resolve one input to its {@link InputPlan}, reading the metadata graph and updating the shared
   * current-snapshot cache and diagnostics. It does not read or write {@code state.resolved} or
   * {@code state.pinByTableId}; the caller merges the returned pins. Callers invoke this method
   * serially unless those cache and diagnostics collaborators are thread-safe.
   */
  private InputPlan planInput(
      ResolutionWork state, QueryInput in, Map<NameRef, Optional<ResourceId>> resolvedNames) {
    state.diagnostics.count("pin.resolver_inputs");
    SnapshotRef override = in.getSnapshot();
    ResourceId rid =
        switch (in.getTargetCase()) {
          case NAME -> {
            state.diagnostics.count("pin.name_inputs");
            long nameResolveStartNs = System.nanoTime();
            ResourceId resolved =
                resolvedNames
                    .getOrDefault(in.getName(), Optional.empty())
                    .orElseThrow(
                        () ->
                            GrpcErrors.notFound(
                                state.correlationId,
                                QUERY_INPUT_UNRESOLVED,
                                Map.of("name", in.getName().toString())));
            state.diagnostics.nanos(
                "pin.input_name_resolve", System.nanoTime() - nameResolveStartNs);
            yield resolved;
          }
          case TABLE_ID -> {
            state.diagnostics.count("pin.table_id_inputs");
            yield in.getTableId();
          }
          case VIEW_ID -> {
            state.diagnostics.count("pin.view_id_inputs");
            yield in.getViewId();
          }
          default ->
              throw GrpcErrors.invalidArgument(state.correlationId, QUERY_INPUT_INVALID, Map.of());
        };

    List<TablePin> pins = new ArrayList<>();
    try {
      if (rid.getKind() == ResourceKind.RK_VIEW) {
        // Views are not pinned directly. We only pin their base tables.
        // Reject snapshot_id overrides for views; allow AS-OF and apply it to dependency pins.
        validateViewOverride(state.correlationId, rid, override);
        collectBaseTablePins(
            state, rid, effectiveAsOf(override, state.asOfDefault), new HashSet<>(), pins);
      } else {
        TablePin pin = pinForResource(state, rid, override, state.asOfDefault);
        if (pin != null) {
          pins.add(pin);
        }
      }
    } catch (RuntimeException | Error failure) {
      // Preserve a view's successful dependency prefix so its conflicts are reconciled in request
      // order before a later dependency's planning failure is reported.
      return new InputPlan(rid, pins, failure);
    }
    return new InputPlan(rid, pins, null);
  }

  private TablePin pinForResource(
      ResolutionWork state, ResourceId rid, SnapshotRef override, Optional<Timestamp> asOfDefault) {
    return switch (rid.getKind()) {
      case RK_TABLE -> pinForTable(state, rid, override, asOfDefault);
      case RK_VIEW -> {
        // Views are not pinned directly. Dependency pinning is handled by the caller.
        validateViewOverride(state.correlationId, rid, override);
        yield null;
      }
      default ->
          throw GrpcErrors.invalidArgument(
              state.correlationId, QUERY_INPUT_INVALID, Map.of("resource_id", rid.getId()));
    };
  }

  /**
   * Resolve one table pin. CURRENT lookups use a single-flight holder whose owner constructs and
   * roots the pin before publication; failures evict the holder and wake waiters. Explicit and
   * AS-OF requests reuse an exactly matching committed pin or construct and root a fresh one.
   */
  private TablePin pinForTable(
      ResolutionWork state, ResourceId rid, SnapshotRef override, Optional<Timestamp> asOfDefault) {
    Optional<Timestamp> effectiveAsOfDefault =
        isExplicitCurrentSnapshot(override) ? Optional.empty() : asOfDefault;
    if (usesCurrentSnapshotFallback(override, effectiveAsOfDefault)) {
      // Single-flight per table: two references to the same table's CURRENT snapshot must freeze
      // the same snapshot even when they resolve on different threads. Otherwise, an ingest between
      // independent lookups can turn a compatible pair into a conflict. A CompletableFuture
      // placeholder provides single-flight without holding a map-bin lock across the store call:
      // the task that inserts the placeholder performs the lookup, and same-table tasks await it.
      while (true) {
        CompletableFuture<TablePin> holder = new CompletableFuture<>();
        CompletableFuture<TablePin> inflight =
            state.currentSnapshotPinCache.putIfAbsent(rid, holder);
        TablePin pin;
        if (inflight == null) {
          if (!state.currentSnapshotCacheOwnership.claim(rid, holder)) {
            throw new CancellationException("input resolution no longer active");
          }
          try {
            long snapshotPinStartNs = System.nanoTime();
            pin =
                pinResolutionReads.read(
                    () ->
                        metadataGraph.tablePinFor(
                            state.correlationId, rid, override, effectiveAsOfDefault));
            state.resolvingPinRoots.register(pin);
            state.diagnostics.count("pin.snapshot_calls");
            state.diagnostics.nanos("pin.snapshot_lookup", System.nanoTime() - snapshotPinStartNs);
          } catch (RuntimeException | Error e) {
            // Never cache a failure: drop the placeholder so a retry re-resolves, and release any
            // callers already awaiting this id with the same error.
            state.currentSnapshotPinCache.remove(rid, holder);
            state.currentSnapshotCacheOwnership.forget(rid, holder);
            holder.completeExceptionally(e);
            throw e;
          }
          holder.complete(pin);
          state.diagnostics.count("pin.current_snapshot_cache_misses");
          return pin;
        }

        pin = awaitCurrentSnapshot(state, inflight);
        // Failed owners remove the holder under this same lock before releasing their root. A
        // waiter therefore either installs its own root while the holder remains published or
        // retries against the replacement entry without using a retired pin.
        synchronized (inflight) {
          if (state.currentSnapshotPinCache.get(rid) == inflight) {
            state.resolvingPinRoots.register(pin);
            state.diagnostics.count("pin.current_snapshot_cache_hits");
            return pin;
          }
        }
        throwIfCancelled(state.cancelled);
      }
    }
    state.diagnostics.count(
        "pin.explicit_snapshot_pins", override != null && override.hasSnapshotId());
    state.diagnostics.count(
        "pin.asof_snapshot_pins",
        (override != null && override.hasAsOf()) || asOfDefault.isPresent());
    // Reuse a committed pin that froze this exact explicit or AS-OF request. The committed query
    // keeps its blobs rooted even after the live manifest no longer contains that snapshot, and
    // first-touch semantics require subsequent resolution to return the same frozen pin. A
    // different snapshot id or timestamp still resolves against the live root.
    if (queryStore != null) {
      Optional<TablePin> reused =
          queryStore
              .get(state.queryId)
              .flatMap(
                  ctx -> QueryPins.findTablePin(ctx.parseRelationPins(state.correlationId), rid))
              .filter(pin -> reusableFor(pin, override, asOfDefault));
      if (reused.isPresent()) {
        state.diagnostics.count("pin.committed_pin_reuse");
        TablePin pin = reused.get();
        state.resolvingPinRoots.register(pin);
        return pin;
      }
    }
    long snapshotPinStartNs = System.nanoTime();
    TablePin resolved =
        pinResolutionReads.read(
            () -> metadataGraph.tablePinFor(state.correlationId, rid, override, asOfDefault));
    state.resolvingPinRoots.register(resolved);
    state.diagnostics.count("pin.snapshot_calls");
    state.diagnostics.nanos("pin.snapshot_lookup", System.nanoTime() - snapshotPinStartNs);
    return resolved;
  }

  /** Await a single-flight winner without stranding a cancelled waiter on the executor. */
  private TablePin awaitCurrentSnapshot(
      ResolutionWork state, CompletableFuture<TablePin> inflight) {
    while (true) {
      throwIfCancelled(state.cancelled);
      if (Thread.currentThread().isInterrupted()) {
        throw new CancellationException("interrupted while awaiting current snapshot pin");
      }
      try {
        return inflight.get(SNAPSHOT_WAIT_POLL_MILLIS, TimeUnit.MILLISECONDS);
      } catch (TimeoutException ignored) {
        // A bounded wait lets the next loop observe cancellation or an executor interrupt.
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new CancellationException("interrupted while awaiting current snapshot pin");
      } catch (ExecutionException e) {
        throw Futures.propagate(e.getCause(), "unexpected checked exception from async task");
      }
    }
  }

  private boolean usesCurrentSnapshotFallback(
      SnapshotRef override, Optional<Timestamp> asOfDefault) {
    if (isExplicitCurrentSnapshot(override)) {
      return true;
    }
    if (override != null && override.getWhichCase() != SnapshotRef.WhichCase.WHICH_NOT_SET) {
      return false;
    }
    return asOfDefault.isEmpty();
  }

  /** An explicit CURRENT selector takes precedence over a request-wide AS-OF default. */
  private boolean isExplicitCurrentSnapshot(SnapshotRef override) {
    return override != null
        && override.getWhichCase() == SnapshotRef.WhichCase.SPECIAL
        && override.getSpecial() == SpecialSnapshot.SS_CURRENT;
  }

  private void validateViewOverride(String correlationId, ResourceId viewId, SnapshotRef override) {
    if (override != null && override.hasSnapshotId()) {
      throw GrpcErrors.invalidArgument(
          correlationId, QUERY_INPUT_VIEW_CANNOT_USE_SNAPSHOT_ID, Map.of("id", viewId.getId()));
    }
  }

  /**
   * Selects dependency pinning time: explicit CURRENT clears the request default, explicit AS-OF
   * replaces it, and an input without either selector inherits the request default.
   */
  private Optional<Timestamp> effectiveAsOf(SnapshotRef override, Optional<Timestamp> asOfDefault) {
    if (isExplicitCurrentSnapshot(override)) {
      return Optional.empty();
    }
    if (override != null && override.hasAsOf()) {
      return Optional.of(override.getAsOf());
    }
    return asOfDefault;
  }

  /**
   * Whether a query's already-committed pin froze the SAME request this resolution is making, so it
   * can be reused instead of re-resolving against the live root. Explicit snapshot_id: the ids
   * match. AS_OF (or an asOfDefault): the pin is an AS_OF pin frozen for the same original
   * timestamp — later reads use its resolved snapshot_id, so reusing preserves within-query
   * consistency even after that snapshot leaves the live manifest. CURRENT is deliberately never
   * reused here: it is served from the per-request cache above, and a fresh CURRENT request is
   * meant to re-resolve to the live current.
   */
  private boolean reusableFor(TablePin pin, SnapshotRef override, Optional<Timestamp> asOfDefault) {
    if (override != null && override.hasSnapshotId()) {
      return pin.getSnapshotId() == override.getSnapshotId();
    }
    Optional<Timestamp> asOf = effectiveAsOf(override, asOfDefault);
    return asOf.isPresent()
        && pin.getPinKind() == PinKind.PIN_KIND_AS_OF
        && pin.hasOriginalAsOf()
        && pin.getOriginalAsOf().equals(asOf.get());
  }

  /**
   * Append the table pins reachable from {@code relationId} to {@code out} in dependency order: a
   * table contributes its own pin; a view is expanded through its base relations. {@code seen}
   * guards against reference cycles. Appends rather than merging, so the caller decides ordering
   * and deduplication.
   */
  private void collectBaseTablePins(
      ResolutionWork state,
      ResourceId relationId,
      Optional<Timestamp> effectiveAsOf,
      Set<String> seen,
      List<TablePin> out) {
    String key = QueryPins.pinKey(relationId);
    if (!seen.add(key)) {
      return;
    }
    if (relationId.getKind() == ResourceKind.RK_TABLE) {
      TablePin pin = pinForResource(state, relationId, null, effectiveAsOf);
      if (pin != null) {
        out.add(pin);
      }
      return;
    }
    long viewResolveStartNs = System.nanoTime();
    Optional<ViewNode> view =
        metadataGraph
            .resolve(relationId)
            .filter(ViewNode.class::isInstance)
            .map(ViewNode.class::cast);
    state.diagnostics.nanos("pin.view_node_resolve", System.nanoTime() - viewResolveStartNs);
    view.ifPresent(
        resolvedView -> {
          // Batch-resolve the view's base relations: bases typically share the view's
          // catalog/namespace, so the scope resolves once for the whole set.
          List<NameRef> baseRefs =
              resolvedView.baseRelations().stream()
                  .map(
                      base ->
                          ViewContextUtils.enrichForViewContext(
                              base, resolvedView, state.defaultCatalog.orElse("")))
                  .toList();
          if (baseRefs.isEmpty()) {
            return;
          }
          long baseNameStartNs = System.nanoTime();
          Map<NameRef, Optional<ResourceId>> baseIds =
              metadataGraph.resolveNames(state.correlationId, baseRefs);
          state.diagnostics.nanos(
              "pin.view_base_name_resolve", System.nanoTime() - baseNameStartNs);
          for (NameRef baseRef : baseRefs) {
            state.diagnostics.count("pin.view_base_name_resolutions");
            baseIds
                .getOrDefault(baseRef, Optional.empty())
                .ifPresent(rid -> collectBaseTablePins(state, rid, effectiveAsOf, seen, out));
          }
        });
  }

  private void mergePin(ResolutionState state, TablePin pin) {
    if (pin == null) {
      return;
    }
    TablePin existing = state.pinByTableId.get(pin.getTableId());
    if (existing == null) {
      state.pinByTableId.put(pin.getTableId(), pin);
      return;
    }
    // First-touch wins: compatible later pins relinquish only their own provisional roots.
    QueryPins.reconcile(existing, pin, state.work.correlationId);
    if (existing != pin) {
      discardCompatiblePin(state, existing, pin);
    }
  }

  /**
   * Rebind a compatible CURRENT cache entry before releasing the losing pin's provisional roots.
   */
  private void discardCompatiblePin(
      ResolutionState state, TablePin retainedPin, TablePin losingPin) {
    state.work.currentSnapshotCacheOwnership.replaceCompatiblePin(losingPin, retainedPin);
    state.work.resolvingPinRoots.discard(losingPin);
  }
}
