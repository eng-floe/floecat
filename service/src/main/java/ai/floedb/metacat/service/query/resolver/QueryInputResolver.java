package ai.floedb.metacat.service.query.resolver;

import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.SnapshotRef;
import ai.floedb.metacat.query.rpc.QueryInput;
import ai.floedb.metacat.query.rpc.SnapshotPin;
import ai.floedb.metacat.query.rpc.SnapshotSet;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.query.graph.MetadataGraph;
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

  @Inject MetadataGraph metadataGraph;

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
  // Name resolution
  // =============================================================================

  /**
   * Attempts to resolve a NameRef into either a table or view.
   *
   * <p>Resolution tries:
   *
   * <ol>
   *   <li>resolveTable()
   *   <li>resolveView()
   * </ol>
   *
   * <p>Failure modes:
   *
   * <ul>
   *   <li>No match ⇒ INVALID_ARGUMENT(query.input.unresolved)
   *   <li>Multiple matches ⇒ INVALID_ARGUMENT(query.input.ambiguous)
   * </ul>
   */
  private ResourceId resolveName(String cid, NameRef ref) {
    return metadataGraph.resolveName(cid, ref);
  }

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
