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

import ai.floedb.floecat.common.rpc.QueryInput;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.query.rpc.SnapshotSet;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
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
 * </ol>
 *
 * <p>No side effects: this class only computes resolution, it does not mutate or persist anything.
 */
@ApplicationScoped
public class QueryInputResolver {

  @Inject CatalogOverlay metadataGraph;

  // =============================================================================
  // Result container
  // =============================================================================

  /** Immutable container returned to callers. */
  public record ResolutionResult(
      List<ResourceId> resolved, SnapshotSet snapshotSet, byte[] asOfDefaultBytes) {}

  // Helper method to compute effective as-of timestamp for dependency pinning
  private Optional<Timestamp> effectiveAsOf(SnapshotRef override, Optional<Timestamp> asOfDefault) {
    if (override != null && override.hasAsOf()) {
      return Optional.of(override.getAsOf());
    }
    return asOfDefault;
  }

  private void validateViewOverride(String correlationId, ResourceId viewId, SnapshotRef override) {
    if (override != null && override.hasSnapshotId()) {
      throw GrpcErrors.invalidArgument(
          correlationId, QUERY_INPUT_VIEW_CANNOT_USE_SNAPSHOT_ID, Map.of("id", viewId.getId()));
    }
  }

  private void addResolvedAndPins(
      String correlationId,
      ResourceId rid,
      SnapshotRef override,
      Optional<Timestamp> asOfDefault,
      List<ResourceId> resolved,
      Map<ResourceId, SnapshotPin> pinByTableId) {

    resolved.add(rid);

    if (rid.getKind() == ResourceKind.RK_VIEW) {
      // Views are not pinned directly. We only pin their base tables.
      // Reject snapshot_id overrides for views; allow AS-OF and apply it to dependency pins.
      validateViewOverride(correlationId, rid, override);
      collectBaseTables(
          correlationId, rid, effectiveAsOf(override, asOfDefault), new HashSet<>(), pinByTableId);
      return;
    }

    SnapshotPin pin = pinForResource(correlationId, rid, override, asOfDefault);
    mergePin(pinByTableId, pin);
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
   */
  public ResolutionResult resolveInputs(
      String correlationId, List<QueryInput> inputs, Optional<Timestamp> asOfDefault) {

    List<ResourceId> resolved = new ArrayList<>();
    // Keep snapshots in insertion order (matching input order) while deduplicating by table ID.
    Map<ResourceId, SnapshotPin> pinByTableId = new LinkedHashMap<>();

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
          addResolvedAndPins(correlationId, rid, override, asOfDefault, resolved, pinByTableId);
        }

        case TABLE_ID -> {
          ResourceId rid = in.getTableId();
          addResolvedAndPins(correlationId, rid, override, asOfDefault, resolved, pinByTableId);
        }

        case VIEW_ID -> {
          ResourceId rid = in.getViewId();
          addResolvedAndPins(correlationId, rid, override, asOfDefault, resolved, pinByTableId);
        }

        default -> throw GrpcErrors.invalidArgument(correlationId, QUERY_INPUT_INVALID, Map.of());
      }
    }

    return new ResolutionResult(
        resolved,
        SnapshotSet.newBuilder().addAllPins(pinByTableId.values()).build(),
        asOfDefault.map(Timestamp::toByteArray).orElse(null));
  }

  // =============================================================================
  // Pin resolution
  // =============================================================================

  private SnapshotPin pinForResource(
      String correlationId, ResourceId rid, SnapshotRef override, Optional<Timestamp> asOfDefault) {
    return switch (rid.getKind()) {
      case RK_TABLE -> metadataGraph.snapshotPinFor(correlationId, rid, override, asOfDefault);
      case RK_VIEW -> {
        // Views are not pinned directly. Dependency pinning is handled by the caller.
        validateViewOverride(correlationId, rid, override);
        yield null;
      }
      default ->
          throw GrpcErrors.invalidArgument(
              correlationId, QUERY_INPUT_INVALID, Map.of("resource_id", rid.getId()));
    };
  }

  private void collectBaseTables(
      String correlationId,
      ResourceId relationId,
      Optional<Timestamp> effectiveAsOf,
      Set<String> seen,
      Map<ResourceId, SnapshotPin> pinByTableId) {
    String key = relationId.getKind().name() + ":" + relationId.getId();
    if (!seen.add(key)) {
      return;
    }
    if (relationId.getKind() == ResourceKind.RK_TABLE) {
      SnapshotPin pin = pinForResource(correlationId, relationId, null, effectiveAsOf);
      mergePin(pinByTableId, pin);
      return;
    }
    metadataGraph
        .resolve(relationId)
        .filter(ViewNode.class::isInstance)
        .map(ViewNode.class::cast)
        .ifPresent(
            view -> {
              for (ResourceId base : view.baseRelations()) {
                collectBaseTables(correlationId, base, effectiveAsOf, seen, pinByTableId);
              }
            });
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
