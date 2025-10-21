package ai.floedb.metacat.service.planning.impl;

import java.util.*;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;

import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.common.rpc.SnapshotRef;
import ai.floedb.metacat.catalog.rpc.DirectoryGrpc;
import ai.floedb.metacat.catalog.rpc.ResolveTableRequest;
import ai.floedb.metacat.catalog.rpc.ResolveNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.ResourceAccessGrpc;
import ai.floedb.metacat.catalog.rpc.GetCurrentSnapshotRequest;
import ai.floedb.metacat.planning.rpc.BeginPlanRequest;
import ai.floedb.metacat.planning.rpc.BeginPlanResponse;
import ai.floedb.metacat.planning.rpc.RenewPlanRequest;
import ai.floedb.metacat.planning.rpc.RenewPlanResponse;
import ai.floedb.metacat.planning.rpc.EndPlanRequest;
import ai.floedb.metacat.planning.rpc.EndPlanResponse;
import ai.floedb.metacat.planning.rpc.GetPlanRequest;
import ai.floedb.metacat.planning.rpc.GetPlanResponse;
import ai.floedb.metacat.planning.rpc.Planning;
import ai.floedb.metacat.planning.rpc.ExpansionMap;
import ai.floedb.metacat.planning.rpc.SnapshotSet;
import ai.floedb.metacat.planning.rpc.SnapshotPin;
import ai.floedb.metacat.planning.rpc.PlanDescriptor;
import ai.floedb.metacat.service.common.BaseServiceImpl;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.planning.PlanContextStore;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;

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
  public Uni<BeginPlanResponse> beginPlan(BeginPlanRequest req) {
    return mapFailures(run(() -> {
      var p = principal.get();
      var corr = p.getCorrelationId();
      authz.require(p, "catalog.read");

      if (req.getInputsCount() == 0) {
        throw GrpcErrors.invalidArgument(corr, "plan.inputs.required", Map.of());
      }

      final long ttlMs = (req.getTtlSeconds() > 0 ? req.getTtlSeconds() : (int) (defaultTtlMs / 1000)) * 1000L;
      final Optional<Timestamp> asOfDefault = req.hasAsOfDefault()
          ? Optional.of(req.getAsOfDefault())
          : Optional.empty();

      final List<Resolved> resolved = new ArrayList<>();
      for (var in : req.getInputsList()) {
        switch (in.getTargetCase()) {
          case NAME -> {
            var nr = in.getName();
            checkNameRef(nr);

            ResourceId rid;
            boolean isTable;
            if (nr.getName() != null && !nr.getName().isBlank()) {
              rid = directory.resolveTable(
                  ResolveTableRequest.newBuilder().setRef(nr).build()).getResourceId();
              isTable = true;
            } else {
              rid = directory.resolveNamespace(
                  ResolveNamespaceRequest.newBuilder().setRef(nr).build()).getResourceId();
              isTable = false;
            }
            long snapId = computeSnapshotPin(in.getSnapshot(), asOfDefault, isTable, rid);
            resolved.add(new Resolved(rid, isTable, snapId));
          }
          case TABLE_ID -> {
            var rid = in.getTableId();
            ensureKind(rid, ResourceKind.RK_TABLE, "table_id", corr);
            long snapId = computeSnapshotPin(in.getSnapshot(), asOfDefault, true, rid);
            resolved.add(new Resolved(rid, true, snapId));
          }
          case VIEW_ID -> {
            var rid = in.getViewId();
            ensureKind(rid, ResourceKind.RK_OVERLAY, "view_id", corr);
            resolved.add(new Resolved(rid, false, 0L));
          }
          case TARGET_NOT_SET -> throw GrpcErrors.invalidArgument(
              corr, "plan.target.required", Map.of());
        }
      }

      var expansion = ExpansionMap.newBuilder().build();

      var snapshots = SnapshotSet.newBuilder();
      for (var r : resolved) {
        if (r.isTable && r.snapshotId > 0) {
          snapshots.addPins(SnapshotPin.newBuilder()
              .setTableId(r.rid)
              .setSnapshotId(r.snapshotId));
        } else if (r.isTable && r.snapshotId == 0 && asOfDefault.isPresent()) {
          snapshots.addPins(SnapshotPin.newBuilder()
              .setTableId(r.rid)
              .setAsOf(asOfDefault.get()));
        }
      }

      String planId = UUID.randomUUID().toString();
      byte[] expansionBytes = expansion.toByteArray();
      byte[] snapshotBytes = snapshots.build().toByteArray();

      var ctx = PlanContext.newActive(
          planId,
          p.getTenantId(),
          p,
          expansionBytes,
          snapshotBytes,
          ttlMs,
          1L
      );
      plans.put(ctx);

      try {
        return BeginPlanResponse.newBuilder()
            .setPlanId(planId)
            .setExpiresAt(ts(ctx.getExpiresAtMs()))
            .setSnapshots(SnapshotSet.parseFrom(snapshotBytes))
            .setExpansion(ExpansionMap.parseFrom(expansionBytes))
            .build();
      } catch (InvalidProtocolBufferException e) {
        throw GrpcErrors.internal(corr, "plan.expansion.parse_failed", Map.of("plan_id", planId));
      }
    }), corrId());
  }

  @Override
  public Uni<RenewPlanResponse> renewPlan(RenewPlanRequest req) {
    return mapFailures(run(() -> {
      var p = principal.get();
      var corr = p.getCorrelationId();
      authz.require(p, "catalog.read");

      String planId = mustNonEmpty(req.getPlanId(), "plan_id", corr);
      final long ttlMs = (req.getTtlSeconds() > 0 ? req.getTtlSeconds() : (int) (defaultTtlMs / 1000)) * 1000L;
      final long requestedExp = clock.millis() + ttlMs;

      var updated = plans.extendLease(planId, requestedExp);
      if (updated.isEmpty()) {
        throw GrpcErrors.notFound(corr, "plan.not_found", Map.of("plan_id", planId));
      }
      return RenewPlanResponse.newBuilder()
          .setPlanId(planId)
          .setExpiresAt(ts(updated.get().getExpiresAtMs()))
          .build();
    }), corrId());
  }

  @Override
  public Uni<EndPlanResponse> endPlan(EndPlanRequest req) {
    return mapFailures(run(() -> {
      var p = principal.get();
      var corr = p.getCorrelationId();
      authz.require(p, "catalog.read");

      String planId = mustNonEmpty(req.getPlanId(), "plan_id", corr);
      var ended = plans.end(planId, req.getCommit());
      if (ended.isEmpty()) {
        throw GrpcErrors.notFound(corr, "plan.not_found", Map.of("plan_id", planId));
      }
      return EndPlanResponse.newBuilder().setPlanId(planId).build();
    }), corrId());
  }

  @Override
  public Uni<GetPlanResponse> getPlan(GetPlanRequest req) {
    return mapFailures(run(() -> {
      var p = principal.get();
      var corr = p.getCorrelationId();
      authz.require(p, "catalog.read");

      String planId = mustNonEmpty(req.getPlanId(), "plan_id", corr);
      var ctxOpt = plans.get(planId);
      if (ctxOpt.isEmpty()) {
        throw GrpcErrors.notFound(corr, "plan.not_found", Map.of("plan_id", planId));
      }
      var ctx = ctxOpt.get();

      var pd = PlanDescriptor.newBuilder()
          .setPlanId(ctx.getPlanId())
          .setTenantId(ctx.getTenantId())
          .setCreatedAt(ts(ctx.getCreatedAtMs()))
          .setExpiresAt(ts(ctx.getExpiresAtMs()));

      if (ctx.getSnapshotSet() != null) {
        try { pd.setSnapshots(SnapshotSet.parseFrom(ctx.getSnapshotSet())); }
        catch (InvalidProtocolBufferException e) {
          throw GrpcErrors.internal(corr, "plan.snapshot.parse_failed", Map.of("plan_id", planId));
        }
      }
      if (ctx.getExpansionMap() != null) {
        try { pd.setExpansion(ExpansionMap.parseFrom(ctx.getExpansionMap())); }
        catch (InvalidProtocolBufferException e) {
          throw GrpcErrors.internal(corr, "plan.expansion.parse_failed", Map.of("plan_id", planId));
        }
      }

      return GetPlanResponse.newBuilder().setPlan(pd.build()).build();
    }), corrId());
  }

  private static Timestamp ts(long millis) {
    long s = Math.floorDiv(millis, 1000);
    int  n = (int) ((millis % 1000) * 1_000_000);
    return Timestamp.newBuilder().setSeconds(s).setNanos(n).build();
  }

  private void checkNameRef(NameRef nr) {
    if (nr.getCatalog() == null || nr.getCatalog().isBlank()) {
      throw GrpcErrors.invalidArgument(corrId(), "catalog.missing", Map.of());
    }
    for (String seg : nr.getPathList()) {
      if (seg == null || seg.isBlank()) {
        throw GrpcErrors.invalidArgument(corrId(), "path.segment.blank", Map.of());
      }
      if (seg.contains("/")) {
        throw GrpcErrors.invalidArgument(corrId(), "path.segment.contains_slash", Map.of("segment", seg));
      }
    }
  }

  private long computeSnapshotPin(
      SnapshotRef sr,
      Optional<Timestamp> asOfDefault,
      boolean isTable,
      ResourceId rid) {

    if (!isTable) return 0L;

    if (sr == null || sr.getWhichCase() == SnapshotRef.WhichCase.WHICH_NOT_SET) {
      var cur = access.getCurrentSnapshot(
          GetCurrentSnapshotRequest.newBuilder().setTableId(rid).build());
      return cur.hasSnapshot() ? cur.getSnapshot().getSnapshotId() : 0L;
    }

    return access.getCurrentSnapshot(
        GetCurrentSnapshotRequest.newBuilder().setTableId(rid).build())
        .getSnapshot().getSnapshotId();
  }

  private record Resolved(ResourceId rid, boolean isTable, long snapshotId) {}
}
