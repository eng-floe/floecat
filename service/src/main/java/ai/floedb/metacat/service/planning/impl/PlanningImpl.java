package ai.floedb.metacat.service.planning.impl;

import ai.floedb.metacat.catalog.rpc.DirectoryGrpc;
import ai.floedb.metacat.catalog.rpc.GetCurrentSnapshotRequest;
import ai.floedb.metacat.catalog.rpc.ResolveNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.ResolveTableRequest;
import ai.floedb.metacat.catalog.rpc.ResourceAccessGrpc;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.common.rpc.SnapshotRef;
import ai.floedb.metacat.planning.rpc.BeginPlanRequest;
import ai.floedb.metacat.planning.rpc.BeginPlanResponse;
import ai.floedb.metacat.planning.rpc.EndPlanRequest;
import ai.floedb.metacat.planning.rpc.EndPlanResponse;
import ai.floedb.metacat.planning.rpc.ExpansionMap;
import ai.floedb.metacat.planning.rpc.GetPlanRequest;
import ai.floedb.metacat.planning.rpc.GetPlanResponse;
import ai.floedb.metacat.planning.rpc.PlanDescriptor;
import ai.floedb.metacat.planning.rpc.Planning;
import ai.floedb.metacat.planning.rpc.RenewPlanRequest;
import ai.floedb.metacat.planning.rpc.RenewPlanResponse;
import ai.floedb.metacat.planning.rpc.SnapshotPin;
import ai.floedb.metacat.planning.rpc.SnapshotSet;
import ai.floedb.metacat.service.common.BaseServiceImpl;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.planning.PlanContextStore;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.*;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@GrpcService
public class PlanningImpl extends BaseServiceImpl implements Planning {
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;

  @GrpcClient("directory")
  DirectoryGrpc.DirectoryBlockingStub directory;

  @GrpcClient("resource-access")
  ResourceAccessGrpc.ResourceAccessBlockingStub access;

  @Inject PlanContextStore plans;

  @Inject
  @ConfigProperty(name = "metacat.plan.default-ttl-ms", defaultValue = "60000")
  long defaultTtlMs;

  @Override
  public Uni<BeginPlanResponse> beginPlan(BeginPlanRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principal.get();
              var correlationId = principalContext.getCorrelationId();

              authz.require(principalContext, "catalog.read");

              if (request.getInputsCount() == 0) {
                throw GrpcErrors.invalidArgument(correlationId, "plan.inputs.required", Map.of());
              }

              final long ttlMs =
                  (request.getTtlSeconds() > 0
                          ? request.getTtlSeconds()
                          : (int) (defaultTtlMs / 1000))
                      * 1000L;

              final Optional<Timestamp> asOfDefault =
                  request.hasAsOfDefault()
                      ? Optional.of(request.getAsOfDefault())
                      : Optional.empty();

              final List<Resolved> resolvedInputs = new ArrayList<>();
              for (var in : request.getInputsList()) {
                switch (in.getTargetCase()) {
                  case NAME -> {
                    var nr = in.getName();
                    checkNameRef(nr);

                    ResourceId rid;
                    boolean isTable;
                    if (nr.getName() != null && !nr.getName().isBlank()) {
                      rid =
                          directory
                              .resolveTable(ResolveTableRequest.newBuilder().setRef(nr).build())
                              .getResourceId();
                      isTable = true;
                    } else {
                      rid =
                          directory
                              .resolveNamespace(
                                  ResolveNamespaceRequest.newBuilder().setRef(nr).build())
                              .getResourceId();
                      isTable = false;
                    }
                    long snapId = computeSnapshotPin(in.getSnapshot(), asOfDefault, isTable, rid);
                    resolvedInputs.add(new Resolved(rid, isTable, snapId));
                  }
                  case TABLE_ID -> {
                    var rid = in.getTableId();
                    ensureKind(rid, ResourceKind.RK_TABLE, "table_id", correlationId);
                    long snapId = computeSnapshotPin(in.getSnapshot(), asOfDefault, true, rid);
                    resolvedInputs.add(new Resolved(rid, true, snapId));
                  }
                  case VIEW_ID -> {
                    var rid = in.getViewId();
                    ensureKind(rid, ResourceKind.RK_OVERLAY, "view_id", correlationId);
                    resolvedInputs.add(new Resolved(rid, false, 0L));
                  }

                  default ->
                      throw GrpcErrors.invalidArgument(
                          correlationId, "plan.target.required", Map.of());
                }
              }

              var expansion = ExpansionMap.newBuilder().build();

              var snapshots = SnapshotSet.newBuilder();
              for (var input : resolvedInputs) {
                if (input.isTable && input.snapshotId > 0) {
                  snapshots.addPins(
                      SnapshotPin.newBuilder()
                          .setTableId(input.rid)
                          .setSnapshotId(input.snapshotId));
                } else if (input.isTable && input.snapshotId == 0 && asOfDefault.isPresent()) {
                  snapshots.addPins(
                      SnapshotPin.newBuilder().setTableId(input.rid).setAsOf(asOfDefault.get()));
                }
              }

              String planId = UUID.randomUUID().toString();
              byte[] expansionBytes = expansion.toByteArray();
              byte[] snapshotBytes = snapshots.build().toByteArray();

              var planContext =
                  PlanContext.newActive(
                      planId,
                      principalContext.getTenantId(),
                      principalContext,
                      expansionBytes,
                      snapshotBytes,
                      ttlMs,
                      1L);
              plans.put(planContext);

              try {
                return BeginPlanResponse.newBuilder()
                    .setPlanId(planId)
                    .setExpiresAt(ts(planContext.getExpiresAtMs()))
                    .setSnapshots(SnapshotSet.parseFrom(snapshotBytes))
                    .setExpansion(ExpansionMap.parseFrom(expansionBytes))
                    .build();
              } catch (InvalidProtocolBufferException e) {
                throw GrpcErrors.internal(
                    correlationId, "plan.expansion.parse_failed", Map.of("plan_id", planId));
              }
            }),
        correlationId());
  }

  @Override
  public Uni<RenewPlanResponse> renewPlan(RenewPlanRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principal.get();
              var correlationId = principalContext.getCorrelationId();

              authz.require(principalContext, "catalog.read");

              String planId = mustNonEmpty(request.getPlanId(), "plan_id", correlationId);
              final long ttlMs =
                  (request.getTtlSeconds() > 0
                          ? request.getTtlSeconds()
                          : (int) (defaultTtlMs / 1000))
                      * 1000L;
              final long requestedExp = clock.millis() + ttlMs;

              var updated = plans.extendLease(planId, requestedExp);
              if (updated.isEmpty()) {
                throw GrpcErrors.notFound(
                    correlationId, "plan.not_found", Map.of("plan_id", planId));
              }
              return RenewPlanResponse.newBuilder()
                  .setPlanId(planId)
                  .setExpiresAt(ts(updated.get().getExpiresAtMs()))
                  .build();
            }),
        correlationId());
  }

  @Override
  public Uni<EndPlanResponse> endPlan(EndPlanRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principal.get();
              var correlationId = principalContext.getCorrelationId();

              authz.require(principalContext, "catalog.read");

              String planId = mustNonEmpty(request.getPlanId(), "plan_id", correlationId);
              var ended = plans.end(planId, request.getCommit());
              if (ended.isEmpty()) {
                throw GrpcErrors.notFound(
                    correlationId, "plan.not_found", Map.of("plan_id", planId));
              }
              return EndPlanResponse.newBuilder().setPlanId(planId).build();
            }),
        correlationId());
  }

  @Override
  public Uni<GetPlanResponse> getPlan(GetPlanRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principal.get();
              var correlationId = principalContext.getCorrelationId();
              authz.require(principalContext, "catalog.read");

              String planId = mustNonEmpty(request.getPlanId(), "plan_id", correlationId);
              var planContextOpt = plans.get(planId);
              if (planContextOpt.isEmpty()) {
                throw GrpcErrors.notFound(
                    correlationId, "plan.not_found", Map.of("plan_id", planId));
              }
              var planContext = planContextOpt.get();

              var planDescriptor =
                  PlanDescriptor.newBuilder()
                      .setPlanId(planContext.getPlanId())
                      .setTenantId(planContext.getTenantId())
                      .setCreatedAt(ts(planContext.getCreatedAtMs()))
                      .setExpiresAt(ts(planContext.getExpiresAtMs()));

              if (planContext.getSnapshotSet() != null) {
                try {
                  planDescriptor.setSnapshots(SnapshotSet.parseFrom(planContext.getSnapshotSet()));
                } catch (InvalidProtocolBufferException e) {
                  throw GrpcErrors.internal(
                      correlationId, "plan.snapshot.parse_failed", Map.of("plan_id", planId));
                }
              }
              if (planContext.getExpansionMap() != null) {
                try {
                  planDescriptor.setExpansion(
                      ExpansionMap.parseFrom(planContext.getExpansionMap()));
                } catch (InvalidProtocolBufferException e) {
                  throw GrpcErrors.internal(
                      correlationId, "plan.expansion.parse_failed", Map.of("plan_id", planId));
                }
              }

              return GetPlanResponse.newBuilder().setPlan(planDescriptor.build()).build();
            }),
        correlationId());
  }

  private static Timestamp ts(long millis) {
    long s = Math.floorDiv(millis, 1000);
    int n = (int) ((millis % 1000) * 1_000_000);
    return Timestamp.newBuilder().setSeconds(s).setNanos(n).build();
  }

  private void checkNameRef(NameRef nameRef) {
    if (nameRef.getCatalog() == null || nameRef.getCatalog().isBlank()) {
      throw GrpcErrors.invalidArgument(correlationId(), "catalog.missing", Map.of());
    }
    for (String pathSegment : nameRef.getPathList()) {
      if (pathSegment == null || pathSegment.isBlank()) {
        throw GrpcErrors.invalidArgument(correlationId(), "path.segment.blank", Map.of());
      }
      if (pathSegment.contains("/")) {
        throw GrpcErrors.invalidArgument(
            correlationId(), "path.segment.contains_slash", Map.of("segment", pathSegment));
      }
    }
  }

  private long computeSnapshotPin(
      SnapshotRef snapshotRef,
      Optional<Timestamp> asOfDefault,
      boolean isTable,
      ResourceId tableId) {

    if (!isTable) {
      return 0L;
    }

    if (snapshotRef == null || snapshotRef.getWhichCase() == SnapshotRef.WhichCase.WHICH_NOT_SET) {
      var cur =
          access.getCurrentSnapshot(
              GetCurrentSnapshotRequest.newBuilder().setTableId(tableId).build());
      return cur.hasSnapshot() ? cur.getSnapshot().getSnapshotId() : 0L;
    }

    return access
        .getCurrentSnapshot(GetCurrentSnapshotRequest.newBuilder().setTableId(tableId).build())
        .getSnapshot()
        .getSnapshotId();
  }

  private record Resolved(ResourceId rid, boolean isTable, long snapshotId) {}
}
