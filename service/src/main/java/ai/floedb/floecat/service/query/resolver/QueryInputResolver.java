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
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.query.rpc.PinKind;
import ai.floedb.floecat.query.rpc.RelationPinSet;
import ai.floedb.floecat.query.rpc.SnapshotSet;
import ai.floedb.floecat.query.rpc.TablePin;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.service.concurrent.BoundedFanout;
import ai.floedb.floecat.service.concurrent.Futures;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.query.QueryPins;
import ai.floedb.floecat.service.query.ViewContextUtils;
import ai.floedb.floecat.telemetry.AggregatingPhaseDiagnostics;
import ai.floedb.floecat.telemetry.PhaseDiagnostics;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.eclipse.microprofile.config.ConfigProvider;

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

  // Cap on inputs resolved concurrently. Each is an independent, mostly-blocking chain of metadata
  // store reads; a small fan-out overlaps their round-trips without flooding the store. Shared with
  // the catalog bundle's relation fan-out via the same config property. Per-request bound only: the
  // virtual-thread executor has no shared-pool ceiling. Read once here via ConfigProvider (not a
  // @ConfigProperty ctor param) so the new-constructed test call sites keep compiling while prod
  // still honors config.
  private final int maxParallelInputResolutions;

  private final CatalogOverlay metadataGraph;

  // Registers each resolved pin's blobs as a transient GC root at construction time (see
  // QueryContextStore.registerResolvingPinBlobs). Null in unit tests that construct the resolver
  // without a store — registration is simply skipped then.
  private final QueryContextStore queryStore;

  // Runs per-input resolution off the request thread on virtual threads, not the shared Quarkus
  // worker pool: the driver blocks joining this fan-out and must not contend with it for the same
  // pool. The semaphore in BoundedFanout bounds concurrency; OTel context is re-established per
  // task.
  private final Executor blockingExecutor = Executors.newVirtualThreadPerTaskExecutor();

  @Inject
  public QueryInputResolver(CatalogOverlay metadataGraph, QueryContextStore queryStore) {
    this.metadataGraph = metadataGraph;
    this.queryStore = queryStore;
    // Clamp to >=1: a 0/negative permit count would wedge the fan-out's permit wait forever.
    // UserObjectBundleService warns on this same config key; clamp silently here to avoid a
    // duplicate warning.
    this.maxParallelInputResolutions =
        Math.max(
            1,
            ConfigProvider.getConfig()
                .getOptionalValue("floecat.catalog.bundle.max_parallel_relations", Integer.class)
                .orElse(8));
  }

  /** Test-only constructor: no store (no pin-root registration). */
  public QueryInputResolver(CatalogOverlay metadataGraph) {
    this(metadataGraph, null);
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

  /**
   * Mutable accumulation state for a single {@link #resolveInputs} call.
   *
   * <p>Bundles the values that are constant across the entire resolution pass ({@code
   * correlationId}, {@code asOfDefault}, {@code defaultCatalog}) together with the two collections
   * that are built up incrementally ({@code resolved}, {@code pinByTableId}). Passing a single
   * state object instead of individual parameters keeps the private helper signatures concise.
   */
  private static final class ResolutionState {
    // Stable per-query id under which resolved pins are registered as transient GC roots, so the
    // committing RPC can release them by the same key (its correlation id changes across RPCs).
    final String queryId;
    final String correlationId;
    final Optional<Timestamp> asOfDefault;
    final Optional<String> defaultCatalog;
    final List<ResourceId> resolved = new ArrayList<>();
    // Keep insertion order (matching input order) while deduplicating by table ID.
    final Map<ResourceId, TablePin> pinByTableId = new LinkedHashMap<>();
    // Request-local cache for current-snapshot table pins (no override, no as-of).
    final ConcurrentMap<ResourceId, CompletableFuture<TablePin>> currentSnapshotPinCache;
    // Tracks transient roots owned by this resolution attempt until they are retained or discarded.
    final ResolvingPinRoots resolvingPinRoots;
    final PhaseDiagnostics diagnostics;

    ResolutionState(
        String queryId,
        String correlationId,
        Optional<Timestamp> asOfDefault,
        Optional<String> defaultCatalog,
        ConcurrentMap<ResourceId, CompletableFuture<TablePin>> currentSnapshotPinCache,
        ResolvingPinRoots resolvingPinRoots,
        PhaseDiagnostics diagnostics) {
      this.queryId = queryId;
      this.correlationId = correlationId;
      this.asOfDefault = asOfDefault;
      this.defaultCatalog = defaultCatalog;
      this.currentSnapshotPinCache = currentSnapshotPinCache;
      this.resolvingPinRoots = resolvingPinRoots;
      this.diagnostics = diagnostics == null ? PhaseDiagnostics.NOOP : diagnostics;
    }

    /**
     * A view of this state that reports to {@code taskDiagnostics} instead of the shared
     * diagnostics, for resolving one input off the request thread. Shares the read-only fields and
     * the current-snapshot cache (which must be thread-safe); its own {@code resolved} / {@code
     * pinByTableId} are unused, since off-thread resolution only computes pins and never merges.
     */
    ResolutionState withDiagnostics(PhaseDiagnostics taskDiagnostics) {
      return new ResolutionState(
          queryId,
          correlationId,
          asOfDefault,
          defaultCatalog,
          currentSnapshotPinCache,
          resolvingPinRoots,
          taskDiagnostics);
    }
  }

  /**
   * Owns the transient registrations made while one {@link #resolveInputs} call constructs pins. A
   * retained pin stays registered until its context commit makes it durable; a discarded pin or
   * failed resolution releases only the registration this attempt created.
   */
  private static final class ResolvingPinRoots {
    private final QueryContextStore queryStore;
    private final String queryId;
    private final Map<TablePin, List<String>> rootsByPin = new IdentityHashMap<>();

    ResolvingPinRoots(QueryContextStore queryStore, String queryId) {
      this.queryStore = queryStore;
      this.queryId = queryId;
    }

    /**
     * Register a pin's roots once for this attempt. A store failure leaves the pin untracked so
     * callers propagate the failure without attempting a compensating release.
     */
    synchronized void register(TablePin pin) {
      if (queryStore == null || queryId == null || queryId.isEmpty() || pin == null) {
        return;
      }
      if (rootsByPin.containsKey(pin)) {
        return;
      }
      List<String> roots = QueryPins.gcRootUris(pin);
      if (roots.isEmpty()) {
        return;
      }
      queryStore.registerResolvingPinBlobs(queryId, roots);
      rootsByPin.put(pin, roots);
    }

    /**
     * Release this attempt's registration for a compatible pin that lost ordered first-touch. A
     * store failure retains ownership so a later failure cleanup can retry the release.
     */
    synchronized void discard(TablePin pin) {
      List<String> roots = rootsByPin.get(pin);
      if (roots != null) {
        queryStore.releaseResolvingPinBlobs(queryId, roots);
        rootsByPin.remove(pin);
      }
    }

    /**
     * Release every registration still owned by this failed attempt. A store failure stops release
     * and propagates to the caller, which retains the original resolution failure.
     */
    synchronized void releaseAll() {
      var iterator = rootsByPin.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<TablePin, List<String>> entry = iterator.next();
        queryStore.releaseResolvingPinBlobs(queryId, entry.getValue());
        iterator.remove();
      }
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
        "", correlationId, inputs, asOfDefault, defaultCatalogId, new ConcurrentHashMap<>(), null);
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
    PhaseDiagnostics diag = diagnostics == null ? PhaseDiagnostics.NOOP : diagnostics;

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

    var state =
        new ResolutionState(
            queryId,
            correlationId,
            asOfDefault,
            defaultCatalog,
            currentSnapshotPinCache,
            new ResolvingPinRoots(queryStore, queryId),
            diag);

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

      // Resolve each input to its id and the table pins it contributes (a table yields its own pin;
      // a view yields its base tables' pins). planInput reads the metadata graph and the shared
      // current-snapshot cache; it does not touch `resolved` or `pinByTableId`. Inputs resolve
      // independently, so with more than one they are fanned out; the results are gathered in input
      // order for the merge below.
      List<InputPlan> plans =
          inputs.size() > 1
              ? planInputsConcurrently(state, inputs, resolvedNames)
              : planInputsSerially(state, inputs, resolvedNames);

      // Merge the plans into the pin set in input order. Order decides two things: `resolved`
      // mirrors the request order, and mergePin keeps the first pin seen for a table while raising
      // QUERY_TABLE_PIN_CONFLICT when a later reference pins the same table with an incompatible
      // snapshot/as-of. Every kept pin is already GC-rooted at construction, before the caller
      // reads any schema/stats or persists the context.
      for (InputPlan plan : plans) {
        state.resolved.add(plan.resolvedId());
        for (TablePin pin : plan.pins()) {
          mergePin(state, pin);
        }
      }

      RelationPinSet relationPinSet =
          RelationPinSet.newBuilder()
              .addAllPins(state.pinByTableId.values().stream().map(QueryPins::ofTable).toList())
              .build();
      diag.add("pin.resolver_output_pins", relationPinSet.getPinsCount());
      return new ResolutionResult(
          state.resolved, relationPinSet, asOfDefault.map(Timestamp::toByteArray).orElse(null));
    } catch (RuntimeException | Error e) {
      try {
        state.resolvingPinRoots.releaseAll();
      } catch (RuntimeException | Error cleanupFailure) {
        e.addSuppressed(cleanupFailure);
      }
      throw e;
    }
  }

  // =============================================================================
  // Pin resolution
  // =============================================================================

  /**
   * One input's resolution: the id recorded in {@code resolved}, and the table pins it contributes,
   * ordered. A view's id is recorded but the pins are its base tables'; a table records its id and
   * its own single pin.
   */
  private record InputPlan(ResourceId resolvedId, List<TablePin> pins, Throwable terminalFailure) {}
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

  /** Resolve the inputs one at a time on the calling thread, reporting to the shared diagnostics. */
  private List<InputPlan> planInputsSerially(
      ResolutionState state,
      List<QueryInput> inputs,
      Map<NameRef, Optional<ResourceId>> resolvedNames) {
    List<InputPlan> plans = new ArrayList<>(inputs.size());
    for (QueryInput in : inputs) {
      plans.add(planInput(state, in, resolvedNames));
    }
    return plans;
  }

  /**
   * Resolve the inputs across the blocking executor and gather their plans in input order. Tasks
   * report to a shared thread-safe accumulator instead of the request's diagnostics (which is not
   * guaranteed thread-safe); its per-key totals — snapshot-lookup calls and time, cache hits/misses
   * — are flushed to the real diagnostics once resolution has joined. The result is the per-RPC
   * aggregate of those counters, not a per-relation breakdown; the coarse phase timings are
   * measured by the caller around this call regardless. One task state is shared by all tasks
   * because everything they touch is immutable or thread-safe (the current-snapshot cache and the
   * accumulator). Keep off-thread diagnostics to counters and durations only; one-shot put/emit
   * values must stay on the request thread because the accumulator intentionally drops them.
   * Gathering plans in input order keeps the caller's merge deterministic (first-touch-wins,
   * conflict detection).
   */
  private List<InputPlan> planInputsConcurrently(
      ResolutionState state,
      List<QueryInput> inputs,
      Map<NameRef, Optional<ResourceId>> resolvedNames) {
    AggregatingPhaseDiagnostics taskDiagnostics = new AggregatingPhaseDiagnostics();
    ResolutionState taskState = state.withDiagnostics(taskDiagnostics);
    List<InputPlan> plans =
        BoundedFanout.mapOrdered(
            inputs,
            maxParallelInputResolutions,
            blockingExecutor,
            in -> planInput(taskState, in, resolvedNames));
    taskDiagnostics.flushInto(state.diagnostics);
    return plans;
  }

  /**
   * Resolve one input to its {@link InputPlan}, reading the metadata graph and updating the shared
   * current-snapshot cache and diagnostics. It does not read or write {@code state.resolved} or
   * {@code state.pinByTableId}; the caller merges the returned pins. Callers invoke this method
   * serially unless those cache and diagnostics collaborators are thread-safe.
   */
  private InputPlan planInput(
      ResolutionState state, QueryInput in, Map<NameRef, Optional<ResourceId>> resolvedNames) {
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
      ResolutionState state,
      ResourceId rid,
      SnapshotRef override,
      Optional<Timestamp> asOfDefault) {
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

  private TablePin pinForTable(
      ResolutionState state,
      ResourceId rid,
      SnapshotRef override,
      Optional<Timestamp> asOfDefault) {
    if (usesCurrentSnapshotFallback(override, asOfDefault)) {
      // Single-flight per table: two references to the same table's CURRENT snapshot must freeze
      // the
      // SAME snapshot even when they resolve on different threads, or an ingest landing between two
      // independent lookups would give them different snapshots and turn a compatible pair into a
      // pin conflict. A CompletableFuture placeholder gives that single-flight WITHOUT
      // computeIfAbsent
      // holding the map's bin lock across the store round-trip — which would serialize unrelated
      // table ids that hash to the same bin. The winner (whoever inserts the incomplete future)
      // runs
      // the one lookup; concurrent same-id callers await its result.
      CompletableFuture<TablePin> holder = new CompletableFuture<>();
      CompletableFuture<TablePin> inflight = state.currentSnapshotPinCache.putIfAbsent(rid, holder);
      TablePin pin;
      if (inflight == null) {
        try {
          long snapshotPinStartNs = System.nanoTime();
          pin = metadataGraph.tablePinFor(state.correlationId, rid, override, asOfDefault);
          state.resolvingPinRoots.register(pin);
          state.diagnostics.count("pin.snapshot_calls");
          state.diagnostics.nanos("pin.snapshot_lookup", System.nanoTime() - snapshotPinStartNs);
        } catch (RuntimeException | Error e) {
          // Never cache a failure: drop the placeholder so a retry re-resolves, and release any
          // callers already awaiting this id with the same error.
          state.currentSnapshotPinCache.remove(rid, holder);
          holder.completeExceptionally(e);
          throw e;
        }
        holder.complete(pin);
        state.diagnostics.count("pin.current_snapshot_cache_misses");
      } else {
        pin = Futures.join(inflight);
        state.diagnostics.count("pin.current_snapshot_cache_hits");
      }
      return pin;
    }
    state.diagnostics.count(
        "pin.explicit_snapshot_pins", override != null && override.hasSnapshotId());
    state.diagnostics.count(
        "pin.asof_snapshot_pins",
        (override != null && override.hasAsOf()) || asOfDefault.isPresent());
    // Before re-resolving against the LIVE root (which throws once the pinned snapshot has left the
    // manifest — deleted or expired), reuse the query's existing committed pin if it already froze
    // THIS same request, mirroring the CURRENT path's per-request cache and the first-touch-wins
    // rule. A snapshot pinned at BeginQuery keeps its blobs GC-rooted for the query's lifetime, so
    // a
    // later DescribeInputs restating the same request must get the pin back — not a spurious
    // NOT_FOUND or a QUERY_TABLE_PIN_CONFLICT from resolving a different snapshot at the same time.
    // Covers explicit snapshot_id AND AS_OF (incl. an asOfDefault): both resolve deterministically
    // to one frozen snapshot. Only a genuinely different request (other id / other as-of)
    // re-resolves.
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
    TablePin resolved = metadataGraph.tablePinFor(state.correlationId, rid, override, asOfDefault);
    state.resolvingPinRoots.register(resolved);
    state.diagnostics.count("pin.snapshot_calls");
    state.diagnostics.nanos("pin.snapshot_lookup", System.nanoTime() - snapshotPinStartNs);
    return resolved;
  }

  private boolean usesCurrentSnapshotFallback(
      SnapshotRef override, Optional<Timestamp> asOfDefault) {
    if (override != null && override.getWhichCase() != SnapshotRef.WhichCase.WHICH_NOT_SET) {
      return false;
    }
    return asOfDefault.isEmpty();
  }

  private void validateViewOverride(String correlationId, ResourceId viewId, SnapshotRef override) {
    if (override != null && override.hasSnapshotId()) {
      throw GrpcErrors.invalidArgument(
          correlationId, QUERY_INPUT_VIEW_CANNOT_USE_SNAPSHOT_ID, Map.of("id", viewId.getId()));
    }
  }

  // Helper method to compute effective as-of timestamp for dependency pinning
  private Optional<Timestamp> effectiveAsOf(SnapshotRef override, Optional<Timestamp> asOfDefault) {
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
      ResolutionState state,
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
    QueryPins.reconcile(existing, pin, state.correlationId);
    if (existing != pin) {
      state.resolvingPinRoots.discard(pin);
    }
  }
}
