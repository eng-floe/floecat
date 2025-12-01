package ai.floedb.metacat.service.query.resolve;

import ai.floedb.metacat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.metacat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.metacat.catalog.rpc.ResolveTableRequest;
import ai.floedb.metacat.catalog.rpc.ResolveViewRequest;
import ai.floedb.metacat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.metacat.catalog.rpc.TableServiceGrpc;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.SnapshotRef;
import ai.floedb.metacat.common.rpc.SpecialSnapshot;
import ai.floedb.metacat.query.rpc.QueryInput;
import ai.floedb.metacat.query.rpc.SnapshotPin;
import ai.floedb.metacat.query.rpc.SnapshotSet;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import com.google.protobuf.Timestamp;
import io.quarkus.grpc.GrpcClient;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.*;

/**
 * QueryInputResolver ------------------
 *
 * <p>Resolves QueryInput → (ResourceId, SnapshotPin) BEFORE any QueryContext exists.
 *
 * <p>Responsibilities: - Resolve NameRef → ResourceId (table OR view OR namespace) - Apply snapshot
 * overrides - Apply as-of default if provided - Fallback to "snapshot of CURRENT" for tables
 *
 * <p>This resolver: - does NOT mutate QueryContext - does NOT store anything - is used by
 * DescribeInputs() and GetCatalogBundle()
 */
@ApplicationScoped
public class QueryInputResolver {

  @GrpcClient("metacat")
  DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;

  @GrpcClient("metacat")
  TableServiceGrpc.TableServiceBlockingStub tables;

  @GrpcClient("metacat")
  SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshots;

  /** Encapsulates the resolved ResourceIds + snapshot pins + as-of-default marshalled form. */
  public static record ResolutionResult(
      List<ResourceId> resolved, SnapshotSet snapshotSet, byte[] asOfDefaultBytes) {}

  /**
   * Resolves all inputs:
   *
   * <p>QueryInput → ResourceId QueryInput + overrides/as-of-default → SnapshotPin
   */
  public ResolutionResult resolveInputs(
      String correlationId, List<QueryInput> inputs, Optional<Timestamp> asOfDefault) {

    List<ResourceId> resolved = new ArrayList<>();
    List<SnapshotPin> pins = new ArrayList<>();

    for (QueryInput in : inputs) {

      switch (in.getTargetCase()) {
        case NAME -> {
          NameRef ref = in.getName();

          // Resolve name → table OR view OR namespace
          ResourceId rid = resolveName(correlationId, ref);
          resolved.add(rid);

          long snap = selectSnapshot(correlationId, rid, in.getSnapshot(), asOfDefault);
          pins.add(buildPin(rid, snap, asOfDefault));
        }

        case TABLE_ID -> {
          ResourceId rid = in.getTableId();
          resolved.add(rid);

          long snap = selectSnapshot(correlationId, rid, in.getSnapshot(), asOfDefault);
          pins.add(buildPin(rid, snap, asOfDefault));
        }

        case VIEW_ID -> {
          ResourceId rid = in.getViewId();
          resolved.add(rid);

          // Views do NOT have storage snapshots
          pins.add(buildPin(rid, 0L, asOfDefault));
        }

        default -> throw GrpcErrors.invalidArgument(correlationId, "query.input.invalid", Map.of());
      }
    }

    return new ResolutionResult(
        resolved,
        SnapshotSet.newBuilder().addAllPins(pins).build(),
        asOfDefault.map(Timestamp::toByteArray).orElse(null));
  }

  // =====================================================================
  // Name Resolution
  // =====================================================================

  /** Attempts resolution */
  private ResourceId resolveName(String cid, NameRef ref) {
    if (ref.hasResourceId()) {
      return ref.getResourceId();
    }

    List<ResourceId> matches = new ArrayList<>(2);

    // Try table
    try {
      var table =
          directory
              .resolveTable(ResolveTableRequest.newBuilder().setRef(ref).build())
              .getResourceId();
      matches.add(table);
    } catch (Exception ignore) {
    }

    // Try view
    try {
      var view =
          directory
              .resolveView(ResolveViewRequest.newBuilder().setRef(ref).build())
              .getResourceId();
      matches.add(view);
    } catch (Exception ignore) {
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

  // =====================================================================
  // Snapshot Selection
  // =====================================================================

  private SnapshotPin buildPin(ResourceId rid, long snap, Optional<Timestamp> asOf) {
    SnapshotPin.Builder b = SnapshotPin.newBuilder().setTableId(rid);

    if (snap > 0) {
      b.setSnapshotId(snap);
    } else if (asOf.isPresent()) {
      // timestamp pin
      b.setAsOf(asOf.get());
    }

    return b.build();
  }

  /**
   * Snapshot resolution for TABLES:
   *
   * <p>1. explicit override snapshot_id → use it 2. explicit override as_of → timestamp pin 3.
   * asOfDefault provided → timestamp pin 4. fallback to latest snapshot via `snapshot(special =
   * CURRENT)`
   *
   * <p>Views always return 0 and use timestamp (if any).
   */
  private long selectSnapshot(
      String cid, ResourceId rid, SnapshotRef override, Optional<Timestamp> asOfDefault) {

    // --- 1. explicit snapshot_id override
    if (override != null && override.hasSnapshotId()) {
      return override.getSnapshotId();
    }

    // --- 2. explicit as-of override
    if (override != null && override.hasAsOf()) {
      return 0L; // timestamp pin
    }

    // --- 3. fallback to as-of-default
    if (asOfDefault.isPresent()) {
      return 0L;
    }

    // --- 4. fallback to CURRENT snapshot
    var latest =
        snapshots.getSnapshot(
            GetSnapshotRequest.newBuilder()
                .setTableId(rid)
                .setSnapshot(SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT))
                .build());

    return latest.getSnapshot().getSnapshotId();
  }
}
