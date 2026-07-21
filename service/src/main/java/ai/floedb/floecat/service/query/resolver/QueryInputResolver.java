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
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.query.QueryPins;
import ai.floedb.floecat.service.query.ViewContextUtils;
import ai.floedb.floecat.telemetry.PhaseDiagnostics;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
 * <p>No side effects: this class only computes resolution, it does not mutate or persist anything.
 */
@ApplicationScoped
public class QueryInputResolver {

  private final CatalogOverlay metadataGraph;

  // Registers each resolved pin's blobs as a transient GC root at construction time (see
  // QueryContextStore.registerResolvingPinBlobs). Null in unit tests that construct the resolver
  // without a store — registration is simply skipped then.
  private final QueryContextStore queryStore;

  @Inject
  public QueryInputResolver(CatalogOverlay metadataGraph, QueryContextStore queryStore) {
    this.metadataGraph = metadataGraph;
    this.queryStore = queryStore;
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
    final Map<ResourceId, TablePin> currentSnapshotPinCache;
    final PhaseDiagnostics diagnostics;

    ResolutionState(
        String queryId,
        String correlationId,
        Optional<Timestamp> asOfDefault,
        Optional<String> defaultCatalog,
        Map<ResourceId, TablePin> currentSnapshotPinCache,
        PhaseDiagnostics diagnostics) {
      this.queryId = queryId;
      this.correlationId = correlationId;
      this.asOfDefault = asOfDefault;
      this.defaultCatalog = defaultCatalog;
      this.currentSnapshotPinCache = currentSnapshotPinCache;
      this.diagnostics = diagnostics == null ? PhaseDiagnostics.NOOP : diagnostics;
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
        "", correlationId, inputs, asOfDefault, defaultCatalogId, new LinkedHashMap<>(), null);
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
   * catalog / creation search-path before resolution. Non-view inputs are unaffected.
   */
  public ResolutionResult resolveInputs(
      String queryId,
      String correlationId,
      List<QueryInput> inputs,
      Optional<Timestamp> asOfDefault,
      Optional<ResourceId> defaultCatalogId,
      Map<ResourceId, TablePin> currentSnapshotPinCache,
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
            queryId, correlationId, asOfDefault, defaultCatalog, currentSnapshotPinCache, diag);

    // Batch-resolve all NAME inputs up front: names sharing a catalog/namespace resolve their
    // scope once instead of once per input.
    List<NameRef> nameInputs =
        inputs.stream()
            .filter(in -> in.getTargetCase() == QueryInput.TargetCase.NAME)
            .map(QueryInput::getName)
            .toList();
    Map<NameRef, Optional<ResourceId>> resolvedNames =
        nameInputs.isEmpty() ? Map.of() : metadataGraph.resolveNames(correlationId, nameInputs);

    for (QueryInput in : inputs) {
      diag.count("pin.resolver_inputs");
      SnapshotRef override = in.getSnapshot();

      switch (in.getTargetCase()) {
        case NAME -> {
          diag.count("pin.name_inputs");
          long nameResolveStartNs = System.nanoTime();
          ResourceId rid =
              resolvedNames
                  .getOrDefault(in.getName(), Optional.empty())
                  .orElseThrow(
                      () ->
                          GrpcErrors.notFound(
                              correlationId,
                              QUERY_INPUT_UNRESOLVED,
                              Map.of("name", in.getName().toString())));
          diag.nanos("pin.input_name_resolve", System.nanoTime() - nameResolveStartNs);
          addResolvedAndPins(state, rid, override);
        }

        case TABLE_ID -> {
          diag.count("pin.table_id_inputs");
          addResolvedAndPins(state, in.getTableId(), override);
        }

        case VIEW_ID -> {
          diag.count("pin.view_id_inputs");
          addResolvedAndPins(state, in.getViewId(), override);
        }

        default -> throw GrpcErrors.invalidArgument(correlationId, QUERY_INPUT_INVALID, Map.of());
      }
    }

    RelationPinSet relationPinSet =
        RelationPinSet.newBuilder()
            .addAllPins(state.pinByTableId.values().stream().map(QueryPins::ofTable).toList())
            .build();
    diag.add("pin.resolver_output_pins", relationPinSet.getPinsCount());
    return new ResolutionResult(
        state.resolved, relationPinSet, asOfDefault.map(Timestamp::toByteArray).orElse(null));
  }

  // =============================================================================
  // Pin resolution
  // =============================================================================

  private void addResolvedAndPins(ResolutionState state, ResourceId rid, SnapshotRef override) {
    state.resolved.add(rid);

    if (rid.getKind() == ResourceKind.RK_VIEW) {
      // Views are not pinned directly. We only pin their base tables.
      // Reject snapshot_id overrides for views; allow AS-OF and apply it to dependency pins.
      validateViewOverride(state.correlationId, rid, override);
      collectBaseTables(state, rid, effectiveAsOf(override, state.asOfDefault), new HashSet<>());
      return;
    }

    mergePin(
        state.pinByTableId,
        pinForResource(state, rid, override, state.asOfDefault),
        state.queryId,
        state.correlationId);
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
      TablePin cached = state.currentSnapshotPinCache.get(rid);
      if (cached != null) {
        state.diagnostics.count("pin.current_snapshot_cache_hits");
        return cached;
      }
      state.diagnostics.count("pin.current_snapshot_cache_misses");
      long snapshotPinStartNs = System.nanoTime();
      TablePin resolved =
          metadataGraph.tablePinFor(state.correlationId, rid, override, asOfDefault);
      state.diagnostics.count("pin.snapshot_calls");
      state.diagnostics.nanos("pin.snapshot_lookup", System.nanoTime() - snapshotPinStartNs);
      state.currentSnapshotPinCache.put(rid, resolved);
      return resolved;
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
        return reused.get();
      }
    }
    long snapshotPinStartNs = System.nanoTime();
    TablePin resolved = metadataGraph.tablePinFor(state.correlationId, rid, override, asOfDefault);
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

  private void collectBaseTables(
      ResolutionState state,
      ResourceId relationId,
      Optional<Timestamp> effectiveAsOf,
      Set<String> seen) {
    String key = QueryPins.pinKey(relationId);
    if (!seen.add(key)) {
      return;
    }
    if (relationId.getKind() == ResourceKind.RK_TABLE) {
      mergePin(
          state.pinByTableId,
          pinForResource(state, relationId, null, effectiveAsOf),
          state.queryId,
          state.correlationId);
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
                .ifPresent(rid -> collectBaseTables(state, rid, effectiveAsOf, seen));
          }
        });
  }

  private void mergePin(
      Map<ResourceId, TablePin> pinByTableId, TablePin pin, String queryId, String correlationId) {
    if (pin == null) {
      return;
    }
    TablePin existing = pinByTableId.get(pin.getTableId());
    if (existing == null) {
      // First touch: this is the pin the context will store. Root its blobs as a transient GC root
      // the instant it is constructed — before any downstream expansion/obligations/schema/stats or
      // persistence — so a concurrent table change plus blob GC cannot delete a just-resolved blob.
      // Keyed by the stable query id so the committing RPC releases it by the same key regardless
      // of
      // which RPC's correlation id resolved. Registering only the kept pin (not a compatible
      // incoming
      // pin that reconcile then discards) avoids rooting a discarded pin's possibly-different table
      // blob, which the commit — comparing against the kept pin — would never unroot.
      if (queryStore != null) {
        // The SAME uri set the committed context will root (QueryPins.gcRootUris) — critically
        // including the pinned ROOT, whose chain expansion is what protects the manifest pages;
        // omitting it left the whole chain sweepable during the resolving window.
        queryStore.registerResolvingPinBlobs(queryId, QueryPins.gcRootUris(pin));
      }
      pinByTableId.put(pin.getTableId(), pin);
      return;
    }
    // First-touch wins: reuse the existing (already-rooted) pin when compatible; conflicting
    // temporal
    // intents for the same table fail planning rather than silently depending on resolution order.
    QueryPins.reconcile(existing, pin, correlationId);
  }
}
