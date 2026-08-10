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
import java.util.function.Consumer;
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
  private static final long GLOBAL_PERMIT_POLL_MILLIS = 10;

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
    this(metadataGraph, queryStore, pinResolutionReads, configuredMaxParallelInputResolutions());
  }

  private QueryInputResolver(
      CatalogOverlay metadataGraph,
      QueryContextStore queryStore,
      RepositoryReads.ReadPolicy pinResolutionReads,
      int maxParallelInputResolutions) {
    this.metadataGraph = metadataGraph;
    this.queryStore = queryStore;
    this.pinResolutionReads = pinResolutionReads;
    this.maxParallelInputResolutions = maxParallelInputResolutions;
  }

  /** Compatibility constructor for direct callers that own their metadata execution policy. */
  public QueryInputResolver(CatalogOverlay metadataGraph, QueryContextStore queryStore) {
    this(
        metadataGraph,
        queryStore,
        RepositoryReads.directPolicy(),
        configuredMaxParallelInputResolutions());
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

  /**
   * Per-request single-flight cache for CURRENT-snapshot table pins. Failed lookups are evicted so
   * a later caller may retry, while successful pins remain frozen for the cache lifetime.
   */
  public static final class CurrentSnapshotPinCache {
    private final ConcurrentMap<ResourceId, CompletableFuture<TablePin>> pins;

    public CurrentSnapshotPinCache() {
      this(new ConcurrentHashMap<>());
    }

    private CurrentSnapshotPinCache(ConcurrentMap<ResourceId, CompletableFuture<TablePin>> pins) {
      this.pins = pins;
    }

    ConcurrentMap<ResourceId, CompletableFuture<TablePin>> entries() {
      return pins;
    }
  }

  /**
   * Explicit per-call ownership for one resolution attempt.
   *
   * <p>Compatibility overloads construct this value before entering the one extension seam. The
   * resolver core then receives its cache and cancellation policy as ordinary parameters, never
   * through ambient thread state.
   */
  public record ResolutionAttempt(
      CurrentSnapshotPinCache currentSnapshotPinCache,
      PhaseDiagnostics diagnostics,
      BooleanSupplier cancelled) {
    public ResolutionAttempt {
      currentSnapshotPinCache = java.util.Objects.requireNonNull(currentSnapshotPinCache);
      cancelled = java.util.Objects.requireNonNull(cancelled);
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
      CurrentSnapshotPinCache currentSnapshotPinCache,
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
        new CurrentSnapshotPinCache(),
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
   * currentSnapshotPinCache} is shared by concurrent input tasks and freezes each table's CURRENT
   * snapshot for the cache lifetime.
   */
  public ResolutionResult resolveInputs(
      String queryId,
      String correlationId,
      List<QueryInput> inputs,
      Optional<Timestamp> asOfDefault,
      Optional<ResourceId> defaultCatalogId,
      CurrentSnapshotPinCache currentSnapshotPinCache,
      PhaseDiagnostics diagnostics) {
    return resolveInputsAttempt(
        queryId,
        correlationId,
        inputs,
        asOfDefault,
        defaultCatalogId,
        new ResolutionAttempt(
            currentSnapshotPinCache, diagnostics, Context.current()::isCancelled));
  }

  /**
   * Compatibility overload for callers compiled against the original exposed single-flight map. The
   * supplied map remains the cache owner; this method only wraps its placeholder convention in the
   * typed cache API.
   */
  public ResolutionResult resolveInputs(
      String queryId,
      String correlationId,
      List<QueryInput> inputs,
      Optional<Timestamp> asOfDefault,
      Optional<ResourceId> defaultCatalogId,
      ConcurrentMap<ResourceId, CompletableFuture<TablePin>> currentSnapshotPinCache,
      PhaseDiagnostics diagnostics) {
    return resolveInputs(
        queryId,
        correlationId,
        inputs,
        asOfDefault,
        defaultCatalogId,
        new CurrentSnapshotPinCache(currentSnapshotPinCache),
        diagnostics);
  }

  /** Compatibility bridge for the cancellable concurrent-map overload. */
  public ResolutionResult resolveInputs(
      String queryId,
      String correlationId,
      List<QueryInput> inputs,
      Optional<Timestamp> asOfDefault,
      Optional<ResourceId> defaultCatalogId,
      ConcurrentMap<ResourceId, CompletableFuture<TablePin>> currentSnapshotPinCache,
      PhaseDiagnostics diagnostics,
      BooleanSupplier cancelled) {
    return resolveInputs(
        queryId,
        correlationId,
        inputs,
        asOfDefault,
        defaultCatalogId,
        new CurrentSnapshotPinCache(currentSnapshotPinCache),
        diagnostics,
        cancelled);
  }

  /**
   * Backward-compatible overload for callers compiled against the original cache contract.
   *
   * <p>The supplied cache is not required to be concurrent. Copy it into a concurrent single-flight
   * cache for this call, then copy successfully resolved pins back on the calling thread. Worker
   * tasks therefore never mutate a caller-owned plain map. If copy-back fails, restore the map's
   * original contents before releasing the attempt's transient roots. When restoration cannot be
   * proven, retain those roots until the store grace period so any copied prefix remains protected.
   */
  public ResolutionResult resolveInputs(
      String queryId,
      String correlationId,
      List<QueryInput> inputs,
      Optional<Timestamp> asOfDefault,
      Optional<ResourceId> defaultCatalogId,
      Map<ResourceId, TablePin> currentSnapshotPinCache,
      PhaseDiagnostics diagnostics) {
    Map<ResourceId, TablePin> initialCache =
        LegacyCurrentSnapshotPinCaches.snapshot(currentSnapshotPinCache);
    CurrentSnapshotPinCache singleFlightCache =
        LegacyCurrentSnapshotPinCaches.singleFlight(initialCache);
    ResolutionResult result =
        resolveInputsAttempt(
            queryId,
            correlationId,
            inputs,
            asOfDefault,
            defaultCatalogId,
            new ResolutionAttempt(singleFlightCache, diagnostics, Context.current()::isCancelled));
    try {
      LegacyCurrentSnapshotPinCaches.copySuccessful(singleFlightCache, currentSnapshotPinCache);
    } catch (RuntimeException | Error copyFailure) {
      if (LegacyCurrentSnapshotPinCaches.restore(
          currentSnapshotPinCache, initialCache, copyFailure)) {
        try {
          if (queryStore != null && queryId != null && !queryId.isEmpty()) {
            queryStore.releaseResolvingPinBlobs(
                queryId, QueryPins.gcRootUris(result.relationPinSet()));
          }
        } catch (RuntimeException | Error cleanupFailure) {
          copyFailure.addSuppressed(cleanupFailure);
        }
      } else {
        // A map with non-transactional or rejecting mutations may retain a copied prefix. Keep the
        // attempt's roots until the store grace period rather than exposing that prefix unrooted.
        LOG.warnf(
            "Retaining resolving pin roots after legacy cache rollback failed query_id=%s",
            queryId);
      }
      throw copyFailure;
    }
    return result;
  }

  /**
   * As {@link #resolveInputs(String, String, List, Optional, Optional, CurrentSnapshotPinCache,
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
      CurrentSnapshotPinCache currentSnapshotPinCache,
      PhaseDiagnostics diagnostics,
      BooleanSupplier cancelled) {
    return resolveInputsAttempt(
        queryId,
        correlationId,
        inputs,
        asOfDefault,
        defaultCatalogId,
        new ResolutionAttempt(currentSnapshotPinCache, diagnostics, cancelled));
  }

  /**
   * Stable test extension seam. Production overloads adapt their legacy inputs into one explicit
   * attempt before reaching this method; the resolution implementation itself is non-virtual.
   */
  protected ResolutionResult resolveInputsAttempt(
      String queryId,
      String correlationId,
      List<QueryInput> inputs,
      Optional<Timestamp> asOfDefault,
      Optional<ResourceId> defaultCatalogId,
      ResolutionAttempt attempt) {
    return resolveInputsCore(
        queryId,
        correlationId,
        inputs,
        asOfDefault,
        defaultCatalogId,
        attempt.currentSnapshotPinCache(),
        attempt.diagnostics(),
        attempt.cancelled());
  }

  /** Run the base resolution implementation after compatibility overload dispatch has completed. */
  private ResolutionResult resolveInputsCore(
      String queryId,
      String correlationId,
      List<QueryInput> inputs,
      Optional<Timestamp> asOfDefault,
      Optional<ResourceId> defaultCatalogId,
      CurrentSnapshotPinCache currentSnapshotPinCache,
      PhaseDiagnostics diagnostics,
      BooleanSupplier cancelled) {
    throwIfCancelled(cancelled);
    PhaseDiagnostics diag = diagnostics == null ? PhaseDiagnostics.NOOP : diagnostics;

    // Bind the request cancellation signal so store reads auto-admitted at the repository layer —
    // including those dispatched on fan-out workers — abort their admission wait when this request
    // cancels.
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
                  new CurrentSnapshotCacheOwnership(currentSnapshotPinCache.pins),
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
        // resolve independently, so overlays that support concurrent resolution fan them out;
        // otherwise the fan-out runs serially on the caller thread. Results merge in input order.
        planInputs(state, inputs, resolvedNames, cancelled);

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

  /** A compatible losing pin and the first-touch pin retained by ordered merge. */
  private record CompatibleDiscard(TablePin losingPin, TablePin retainedPin) {}

  /** Merge one completed plan on the request thread, preserving request-order semantics. */
  private void mergePlan(
      ResolutionState state, InputPlan plan, Consumer<CompatibleDiscard> discardCompatiblePin) {
    state.resolved.add(plan.resolvedId());
    for (TablePin pin : plan.pins()) {
      mergePin(state, pin, discardCompatiblePin);
    }
    if (plan.terminalFailure() instanceof RuntimeException runtime) {
      throw runtime;
    }
    if (plan.terminalFailure() instanceof Error error) {
      throw error;
    }
  }

  /**
   * Resolve the inputs and gather their plans in input order. When the overlay supports concurrent
   * resolution the units fan out; otherwise MetadataFanout runs them serially on the caller thread.
   * Tasks report to a thread-safe accumulator instead of the request's diagnostics (which is not
   * guaranteed thread-safe); its per-key totals — snapshot-lookup calls and time, cache hits/misses
   * — are flushed to the real diagnostics once resolution has joined. The result is the per-RPC
   * aggregate of those counters, not a per-relation breakdown; the coarse phase timings are
   * measured by the caller around this call regardless. Each input receives its own state view so
   * mutable per-plan fields can never become shared task state; only immutable resolution context
   * and the explicitly thread-safe single-flight cache, root tracker, and diagnostics accumulator
   * are shared. Keep off-thread diagnostics to counters and durations only; the accumulator safely
   * omits one-shot put/emit values that cannot be combined across inputs. The task timing keys are
   * aggregate work time in the concurrent path, so they may exceed the enclosing wall-clock
   * resolver phase; dashboards must not treat them as elapsed time. Gathering plans in input order
   * keeps the caller's merge deterministic (first-touch-wins, conflict detection).
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
    List<CompatibleDiscard> deferredDiscards = new ArrayList<>();
    try {
      fanout.forEachOrdered(
          inputs,
          in -> planInput(state.work.withDiagnostics(taskDiagnostics), in, resolvedNames),
          plan -> mergePlan(state, plan, deferredDiscards::add),
          cancelled);
      // Ordered merging can finish a compatible CURRENT holder while a later parallel planner is
      // still registering its own use of that holder. Rebind losing holders only after every
      // planner has joined, so every waiter observes one stable first-touch pin.
      deferredDiscards.forEach(discard -> discardCompatiblePin(state, discard));
    } finally {
      // A failed or cancelled sibling can still leave completed tasks' pin work in this
      // accumulator. Preserve those counters for the request's failure telemetry too.
      taskDiagnostics.flushInto(state.work.diagnostics());
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
            state.currentSnapshotPinCache.pins.putIfAbsent(rid, holder);
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
            state.currentSnapshotPinCache.pins.remove(rid, holder);
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
          if (state.currentSnapshotPinCache.pins.get(rid) == inflight) {
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
        return inflight.get(GLOBAL_PERMIT_POLL_MILLIS, TimeUnit.MILLISECONDS);
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

  private void mergePin(
      ResolutionState state, TablePin pin, Consumer<CompatibleDiscard> discardCompatiblePin) {
    if (pin == null) {
      return;
    }
    TablePin existing = state.pinByTableId.get(pin.getTableId());
    if (existing == null) {
      state.pinByTableId.put(pin.getTableId(), pin);
      return;
    }
    // First-touch wins: compatible later pins relinquish only their own provisional roots.
    QueryPins.reconcile(existing, pin, state.work.correlationId());
    if (existing != pin) {
      discardCompatiblePin.accept(new CompatibleDiscard(pin, existing));
    }
  }

  /** Rebind a compatible losing pin's cache entry before releasing its provisional roots. */
  private void discardCompatiblePin(ResolutionState state, CompatibleDiscard discard) {
    state
        .work
        .currentSnapshotCacheOwnership()
        .replaceCompatiblePin(discard.losingPin(), discard.retainedPin());
    state.work.resolvingPinRoots().discard(discard.losingPin());
  }
}
