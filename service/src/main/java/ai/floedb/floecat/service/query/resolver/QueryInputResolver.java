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

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.query.rpc.QueryInput;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.query.rpc.SnapshotSet;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.systemcatalog.spi.scanner.CatalogOverlay;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
 * <p>This is invoked by DescribeInputs() and GetCatalogBundle(), before any QueryContext exists.
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
    List<SnapshotPin> pins = new ArrayList<>();

    for (QueryInput in : inputs) {

      ResourceId rid;

      switch (in.getTargetCase()) {
        case NAME -> {
          rid = metadataGraph.resolveName(correlationId, in.getName());
          resolved.add(rid);

          pins.add(pinForResource(correlationId, rid, in.getSnapshot(), asOfDefault));
        }

        case TABLE_ID -> {
          rid = in.getTableId();
          resolved.add(rid);

          pins.add(pinForResource(correlationId, rid, in.getSnapshot(), asOfDefault));
        }

        case VIEW_ID -> {
          rid = in.getViewId();
          resolved.add(rid);

          pins.add(pinForResource(correlationId, rid, in.getSnapshot(), asOfDefault));
        }

        default -> throw GrpcErrors.invalidArgument(correlationId, "query.input.invalid", Map.of());
      }
    }

    return new ResolutionResult(
        resolved,
        SnapshotSet.newBuilder().addAllPins(pins).build(),
        asOfDefault.map(Timestamp::toByteArray).orElse(null));
  }

  // =============================================================================
  // Pin resolution
  // =============================================================================

  private SnapshotPin pinForResource(
      String correlationId, ResourceId rid, SnapshotRef override, Optional<Timestamp> asOfDefault) {
    return switch (rid.getKind()) {
      case RK_TABLE -> metadataGraph.snapshotPinFor(correlationId, rid, override, asOfDefault);
      case RK_VIEW -> buildViewPin(correlationId, rid, override, asOfDefault);
      default ->
          throw GrpcErrors.invalidArgument(
              correlationId, "query.input.invalid", Map.of("resource_id", rid.getId()));
    };
  }

  private SnapshotPin buildViewPin(
      String correlationId, ResourceId rid, SnapshotRef override, Optional<Timestamp> asOfDefault) {
    if (override != null && override.hasSnapshotId()) {
      throw GrpcErrors.invalidArgument(
          correlationId, "query.input.view.cannot_use_snapshot_id", Map.of("id", rid.getId()));
    }

    SnapshotPin.Builder builder = SnapshotPin.newBuilder().setTableId(rid);

    if (override != null && override.hasAsOf()) {
      builder.setAsOf(override.getAsOf());
      return builder.build();
    }

    asOfDefault.ifPresent(builder::setAsOf);
    return builder.build();
  }
}
