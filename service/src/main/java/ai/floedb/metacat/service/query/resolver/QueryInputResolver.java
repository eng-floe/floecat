package ai.floedb.metacat.service.query.resolve;

import ai.floedb.metacat.catalog.rpc.*;
import ai.floedb.metacat.common.rpc.*;
import ai.floedb.metacat.query.rpc.*;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import com.google.protobuf.Timestamp;
import io.quarkus.grpc.GrpcClient;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.*;

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

  /** gRPC Directory resolution (tables/views). */
  @GrpcClient("metacat")
  DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;

  /** gRPC Snapshot resolution API. */
  @GrpcClient("metacat")
  SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshots;

  /**
   * Optional test hooks overriding the concrete gRPC APIs. These allow pure unit tests without
   * starting a server.
   */
  interface DirectoryApi {
    ResolveTableResponse resolveTable(ResolveTableRequest req);

    ResolveViewResponse resolveView(ResolveViewRequest req);
  }

  interface SnapshotApi {
    GetSnapshotResponse getSnapshot(GetSnapshotRequest req);
  }

  DirectoryApi dirApi = null;
  SnapshotApi snapApi = null;

  /** Test-only injection point for Directory API. */
  void setDirectoryApi(DirectoryApi api) {
    this.dirApi = api;
  }

  /** Test-only injection point for Snapshot API. */
  void setSnapshotApi(SnapshotApi api) {
    this.snapApi = api;
  }

  /** Chooses real gRPC API or test override for directory. */
  private DirectoryApi dir() {
    if (dirApi != null) return dirApi;
    return new DirectoryApi() {
      public ResolveTableResponse resolveTable(ResolveTableRequest r) {
        return directory.resolveTable(r);
      }

      public ResolveViewResponse resolveView(ResolveViewRequest r) {
        return directory.resolveView(r);
      }
    };
  }

  /** Chooses real gRPC API or test override for snapshots. */
  private SnapshotApi snaps() {
    return (snapApi != null) ? snapApi : req -> snapshots.getSnapshot(req);
  }

  // =============================================================================
  // Result container
  // =============================================================================

  /** Immutable container returned to callers. */
  public record ResolutionResult(
      List<ResourceId> resolved, SnapshotSet snapshotSet, byte[] asOfDefaultBytes) {}

  // =============================================================================
  // Snapshot selection return type
  // =============================================================================

  /** Represents either: - snapshotId (id > 0), OR - timestamp-based "as-of" (ts != null) */
  private static final class SnapChoice {
    final long id;
    final Timestamp ts;

    SnapChoice(long id, Timestamp ts) {
      this.id = id;
      this.ts = ts;
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
   */
  public ResolutionResult resolveInputs(
      String correlationId, List<QueryInput> inputs, Optional<Timestamp> asOfDefault) {

    List<ResourceId> resolved = new ArrayList<>();
    List<SnapshotPin> pins = new ArrayList<>();

    for (QueryInput in : inputs) {

      ResourceId rid;

      switch (in.getTargetCase()) {
        case NAME -> {
          rid = resolveName(correlationId, in.getName());
          resolved.add(rid);

          SnapChoice sc = selectSnapshot(correlationId, rid, in.getSnapshot(), asOfDefault);

          pins.add(buildPin(rid, sc));
        }

        case TABLE_ID -> {
          rid = in.getTableId();
          resolved.add(rid);

          SnapChoice sc = selectSnapshot(correlationId, rid, in.getSnapshot(), asOfDefault);

          pins.add(buildPin(rid, sc));
        }

        case VIEW_ID -> {
          rid = in.getViewId();
          resolved.add(rid);

          SnapshotRef override = in.getSnapshot();

          // Views do NOT support snapshot_id
          if (override != null && override.hasSnapshotId()) {
            throw GrpcErrors.invalidArgument(
                correlationId,
                "query.input.view.cannot_use_snapshot_id",
                Map.of("id", rid.getId()));
          }

          // 1. Explicit override.as_of → use it
          if (override != null && override.hasAsOf()) {
            pins.add(buildPin(rid, new SnapChoice(0L, override.getAsOf())));
            break;
          }

          // 2. asOfDefault → timestamp pin
          if (asOfDefault.isPresent()) {
            pins.add(buildPin(rid, new SnapChoice(0L, asOfDefault.get())));
            break;
          }

          // 3. No overrides → unpinned view
          pins.add(buildPin(rid, new SnapChoice(0L, null)));
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

    if (ref.hasResourceId()) return ref.getResourceId();

    List<ResourceId> matches = new ArrayList<>(2);

    try {
      matches.add(
          dir().resolveTable(ResolveTableRequest.newBuilder().setRef(ref).build()).getResourceId());
    } catch (Exception ignored) {
    }

    try {
      matches.add(
          dir().resolveView(ResolveViewRequest.newBuilder().setRef(ref).build()).getResourceId());
    } catch (Exception ignored) {
    }

    if (matches.isEmpty()) {
      throw GrpcErrors.invalidArgument(
          cid, "query.input.unresolved", Map.of("name", ref.toString()));
    }

    if (matches.size() > 1) {
      throw GrpcErrors.invalidArgument(
          cid, "query.input.ambiguous", Map.of("name", ref.toString()));
    }

    return matches.get(0);
  }

  // =============================================================================
  // Snapshot logic
  // =============================================================================

  /**
   * Computes which snapshot or timestamp pin should be applied.
   *
   * <p>Priority:
   *
   * <ol>
   *   <li>explicit override snapshot_id
   *   <li>explicit override as_of timestamp
   *   <li>asOfDefault timestamp
   *   <li>snapshot(SPECIAL = CURRENT)
   * </ol>
   */
  private SnapChoice selectSnapshot(
      String cid, ResourceId rid, SnapshotRef override, Optional<Timestamp> asOfDefault) {

    // 1. explicit snapshot override
    if (override != null && override.hasSnapshotId())
      return new SnapChoice(override.getSnapshotId(), null);

    // 2. explicit as-of override
    if (override != null && override.hasAsOf()) return new SnapChoice(0L, override.getAsOf());

    // 3. as-of-default
    if (asOfDefault.isPresent()) return new SnapChoice(0L, asOfDefault.get());

    // 4. fallback to CURRENT
    var resp =
        snaps()
            .getSnapshot(
                GetSnapshotRequest.newBuilder()
                    .setTableId(rid)
                    .setSnapshot(SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT))
                    .build());

    return new SnapChoice(resp.getSnapshot().getSnapshotId(), null);
  }

  /** Builds a SnapshotPin using the computed SnapChoice. */
  private SnapshotPin buildPin(ResourceId rid, SnapChoice sc) {
    SnapshotPin.Builder b = SnapshotPin.newBuilder().setTableId(rid);

    if (sc.id > 0) b.setSnapshotId(sc.id);

    if (sc.ts != null) b.setAsOf(sc.ts);

    return b.build();
  }
}
