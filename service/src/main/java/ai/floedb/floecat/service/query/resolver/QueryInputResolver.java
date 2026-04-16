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
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.query.rpc.SnapshotSet;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.query.ViewContextUtils;
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
 *   <li>SnapshotPin containing either:
 *       <ul>
 *         <li>a snapshot ID
 *         <li>or a timestamp-based "as-of" pin
 *       </ul>
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

  @Inject CatalogOverlay metadataGraph;

  public QueryInputResolver() {}

  /** Test-only constructor to avoid reflection-based field injection in unit tests. */
  public QueryInputResolver(CatalogOverlay metadataGraph) {
    this.metadataGraph = metadataGraph;
  }

  // =============================================================================
  // Result container
  // =============================================================================

  /** Immutable container returned to callers. */
  public record ResolutionResult(
      List<ResourceId> resolved, SnapshotSet snapshotSet, byte[] asOfDefaultBytes) {}

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
    final String correlationId;
    final Optional<Timestamp> asOfDefault;
    final Optional<String> defaultCatalog;
    final List<ResourceId> resolved = new ArrayList<>();
    // Keep insertion order (matching input order) while deduplicating by table ID.
    final Map<ResourceId, SnapshotPin> pinByTableId = new LinkedHashMap<>();
    // Request-local cache for current-snapshot table pins (no override, no as-of).
    final Map<ResourceId, SnapshotPin> currentSnapshotPinCache;

    ResolutionState(
        String correlationId,
        Optional<Timestamp> asOfDefault,
        Optional<String> defaultCatalog,
        Map<ResourceId, SnapshotPin> currentSnapshotPinCache) {
      this.correlationId = correlationId;
      this.asOfDefault = asOfDefault;
      this.defaultCatalog = defaultCatalog;
      this.currentSnapshotPinCache = currentSnapshotPinCache;
    }
  }

  // =============================================================================
  // Main resolution entrypoint
  // =============================================================================

  /**
   * Performs full resolution of inputs:
   *
   * <ul>
   *   <li>NAME ⇒ directory lookup
   *   <li>TABLE_ID / VIEW_ID ⇒ used directly
   *   <li>snapshot override ⇒ enforced
   *   <li>as-of-default ⇒ timestamp pin
   *   <li>fallback for tables ⇒ CURRENT snapshot
   * </ul>
   *
   * <p>{@code defaultCatalogId} is used only when expanding view base relations: if a base-relation
   * {@link NameRef} has a blank catalog or empty path it is enriched with the query's default
   * catalog / creation search-path before resolution. Non-view inputs are unaffected.
   */
  public ResolutionResult resolveInputs(
      String correlationId,
      List<QueryInput> inputs,
      Optional<Timestamp> asOfDefault,
      Optional<ResourceId> defaultCatalogId) {
    return resolveInputs(
        correlationId, inputs, asOfDefault, defaultCatalogId, new LinkedHashMap<>());
  }

  public ResolutionResult resolveInputs(
      String correlationId,
      List<QueryInput> inputs,
      Optional<Timestamp> asOfDefault,
      Optional<ResourceId> defaultCatalogId,
      Map<ResourceId, SnapshotPin> currentSnapshotPinCache) {

    // Resolve catalog display-name once up-front — used to fill in blank catalog fields in
    // view base-relation NameRefs so they re-resolve exactly as they did at view-creation time.
    Optional<String> defaultCatalog =
        metadataGraph == null
            ? Optional.empty()
            : defaultCatalogId.flatMap(
                id -> metadataGraph.catalog(id).map(CatalogNode::displayName));

    var state =
        new ResolutionState(correlationId, asOfDefault, defaultCatalog, currentSnapshotPinCache);

    for (QueryInput in : inputs) {
      SnapshotRef override = in.getSnapshot();

      switch (in.getTargetCase()) {
        case NAME -> {
          ResourceId rid =
              metadataGraph
                  .resolveName(correlationId, in.getName())
                  .orElseThrow(
                      () ->
                          GrpcErrors.notFound(
                              correlationId,
                              QUERY_INPUT_UNRESOLVED,
                              Map.of("name", in.getName().toString())));
          addResolvedAndPins(state, rid, override);
        }

        case TABLE_ID -> addResolvedAndPins(state, in.getTableId(), override);

        case VIEW_ID -> addResolvedAndPins(state, in.getViewId(), override);

        default -> throw GrpcErrors.invalidArgument(correlationId, QUERY_INPUT_INVALID, Map.of());
      }
    }

    return new ResolutionResult(
        state.resolved,
        SnapshotSet.newBuilder().addAllPins(state.pinByTableId.values()).build(),
        asOfDefault.map(Timestamp::toByteArray).orElse(null));
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

    mergePin(state.pinByTableId, pinForResource(state, rid, override, state.asOfDefault));
  }

  private SnapshotPin pinForResource(
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

  private SnapshotPin pinForTable(
      ResolutionState state,
      ResourceId rid,
      SnapshotRef override,
      Optional<Timestamp> asOfDefault) {
    if (usesCurrentSnapshotFallback(override, asOfDefault)) {
      SnapshotPin cached = state.currentSnapshotPinCache.get(rid);
      if (cached != null) {
        return cached;
      }
      SnapshotPin resolved =
          metadataGraph.snapshotPinFor(state.correlationId, rid, override, asOfDefault);
      state.currentSnapshotPinCache.put(rid, resolved);
      return resolved;
    }
    return metadataGraph.snapshotPinFor(state.correlationId, rid, override, asOfDefault);
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

  private void collectBaseTables(
      ResolutionState state,
      ResourceId relationId,
      Optional<Timestamp> effectiveAsOf,
      Set<String> seen) {
    String key = pinKey(relationId);
    if (!seen.add(key)) {
      return;
    }
    if (relationId.getKind() == ResourceKind.RK_TABLE) {
      mergePin(state.pinByTableId, pinForResource(state, relationId, null, effectiveAsOf));
      return;
    }
    metadataGraph
        .resolve(relationId)
        .filter(ViewNode.class::isInstance)
        .map(ViewNode.class::cast)
        .ifPresent(
            view -> {
              for (var base : view.baseRelations()) {
                metadataGraph
                    .resolveName(
                        state.correlationId,
                        ViewContextUtils.enrichForViewContext(
                            base, view, state.defaultCatalog.orElse("")))
                    .ifPresent(rid -> collectBaseTables(state, rid, effectiveAsOf, seen));
              }
            });
  }

  private static String pinKey(ResourceId rid) {
    return String.join(":", rid.getAccountId(), rid.getKind().name(), rid.getId());
  }

  private void mergePin(Map<ResourceId, SnapshotPin> pinByTableId, SnapshotPin pin) {
    if (pin == null) {
      return;
    }
    SnapshotPin existing = pinByTableId.get(pin.getTableId());
    // Keep the strongest pin (snapshot_id > as_of > none) when multiple records target the same
    // table.
    if (existing == null || pinStrength(pin) > pinStrength(existing)) {
      pinByTableId.put(pin.getTableId(), pin);
    }
  }

  private static int pinStrength(SnapshotPin pin) {
    if (pin == null) {
      return -1;
    }
    if (pin.hasSnapshotId() && pin.getSnapshotId() >= 0) {
      return 3;
    }
    if (pin.hasAsOf()) {
      return 2;
    }
    if (pin.hasSnapshotId()) {
      return 1;
    }
    return 0;
  }
}
