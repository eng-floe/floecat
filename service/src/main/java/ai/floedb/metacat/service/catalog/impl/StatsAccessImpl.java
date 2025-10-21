package ai.floedb.metacat.service.catalog.impl;

import java.util.Map;

import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;

import ai.floedb.metacat.catalog.rpc.GetTableStatsRequest;
import ai.floedb.metacat.catalog.rpc.GetTableStatsResponse;
import ai.floedb.metacat.catalog.rpc.ListColumnStatsRequest;
import ai.floedb.metacat.catalog.rpc.ListColumnStatsResponse;
import ai.floedb.metacat.catalog.rpc.Snapshot;
import ai.floedb.metacat.catalog.rpc.StatsAccess;
import ai.floedb.metacat.common.rpc.SnapshotRef;
import ai.floedb.metacat.common.rpc.SpecialSnapshot;
import ai.floedb.metacat.common.rpc.PageResponse;
import ai.floedb.metacat.service.common.BaseServiceImpl;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.repo.impl.SnapshotRepository;
import ai.floedb.metacat.service.repo.impl.StatsRepository;
import ai.floedb.metacat.service.repo.util.Keys;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;

@GrpcService
public class StatsAccessImpl extends BaseServiceImpl implements StatsAccess {
  @Inject SnapshotRepository snapshots;
  @Inject StatsRepository stats;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;

  @Override
  public Uni<GetTableStatsResponse> getTableStats(GetTableStatsRequest req) {
    return mapFailures(run(() -> {
      var p = principal.get();
      authz.require(p, "catalog.read");

      final var tableId = req.getTableId();
      final var ref = req.getSnapshot();
      if (ref == null || ref.getWhichCase() == SnapshotRef.WhichCase.WHICH_NOT_SET) {
        throw GrpcErrors.invalidArgument(corrId(), "snapshot.missing", Map.of());
      }

      final long snapId;
      switch (ref.getWhichCase()) {
        case SNAPSHOT_ID -> snapId = ref.getSnapshotId();
        case SPECIAL -> {
          if (ref.getSpecial() != SpecialSnapshot.SS_CURRENT) {
            throw GrpcErrors.invalidArgument(
                corrId(), "snapshot.special.missing", Map.of());
          }
          snapId = snapshots.getCurrentSnapshot(tableId)
              .map(Snapshot::getSnapshotId)
              .orElseThrow(() -> GrpcErrors.notFound(
                  corrId(), "snapshot", Map.of("id", tableId.getId())));
        }
        case AS_OF -> {
          var asOf = ref.getAsOf();
          snapId = snapshots.getAsOf(tableId, asOf)
              .map(Snapshot::getSnapshotId)
              .orElseThrow(() -> GrpcErrors.notFound(
                  corrId(), "snapshot",
                  Map.of("id", tableId.getId())));
        }
        default -> throw GrpcErrors.invalidArgument(corrId(), "snapshot.missing", Map.of());
      }

      return stats.getTableStats(tableId, snapId)
              .map(s -> GetTableStatsResponse.newBuilder().setStats(s).build())
              .orElseThrow(() -> GrpcErrors.notFound(
                  corrId(), "table_stats",
                  Map.of("table_id", tableId.getId(), "snapshot_id", Long.toString(snapId))));
    }), corrId());
  }

  @Override
  public Uni<ListColumnStatsResponse> listColumnStats(ListColumnStatsRequest req) {
    return mapFailures(run(() -> {
      var p = principal.get();
      authz.require(p, "catalog.read");

      final int limit = (req.hasPage() && req.getPage().getPageSize() > 0)
          ? req.getPage().getPageSize() : 200;
      final String token = req.hasPage() ? req.getPage().getPageToken() : "";
      final StringBuilder next = new StringBuilder();

      final var tableId = req.getTableId();
      final var ref = req.getSnapshot();
      if (ref == null || ref.getWhichCase() == SnapshotRef.WhichCase.WHICH_NOT_SET) {
        throw GrpcErrors.invalidArgument(corrId(), "snapshot.missing", Map.of());
      }

      final long snapId;
      switch (ref.getWhichCase()) {
        case SNAPSHOT_ID -> snapId = ref.getSnapshotId();
        case SPECIAL -> {
          if (ref.getSpecial() != SpecialSnapshot.SS_CURRENT) {
            throw GrpcErrors.invalidArgument(
                corrId(), "snapshot.special.missing", Map.of());
          }
          snapId = snapshots.getCurrentSnapshot(tableId)
              .map(Snapshot::getSnapshotId)
              .orElseThrow(() -> GrpcErrors.notFound(
                  corrId(), "snapshot", Map.of("id", tableId.getId())));
        }
        default -> throw GrpcErrors.invalidArgument(
            corrId(), "snapshot.missing", Map.of());
      }

      var items = stats.listColumnStats(tableId, snapId, Math.max(1, limit), token, next);
      var tenant = tableId.getTenantId();
      int total = stats.countByPrefix(Keys.snapColStatsPrefix(tenant, tableId.getId(), snapId));

      return ListColumnStatsResponse.newBuilder()
          .addAllColumns(items)
          .setPage(PageResponse.newBuilder()
              .setNextPageToken(next.toString())
              .setTotalSize(total)
              .build())
          .build();
    }), corrId());
  }
}
