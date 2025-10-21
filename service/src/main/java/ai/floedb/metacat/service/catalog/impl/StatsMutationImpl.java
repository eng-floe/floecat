package ai.floedb.metacat.service.catalog.impl;

import java.util.Map;

import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import ai.floedb.metacat.catalog.rpc.ColumnStats;
import ai.floedb.metacat.catalog.rpc.PutColumnStatsBatchRequest;
import ai.floedb.metacat.catalog.rpc.PutColumnStatsBatchResponse;
import ai.floedb.metacat.catalog.rpc.PutTableStatsRequest;
import ai.floedb.metacat.catalog.rpc.PutTableStatsResponse;
import ai.floedb.metacat.catalog.rpc.StatsMutation;
import ai.floedb.metacat.catalog.rpc.TableStats;
import ai.floedb.metacat.service.catalog.util.MutationOps;
import ai.floedb.metacat.service.common.BaseServiceImpl;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.repo.impl.SnapshotRepository;
import ai.floedb.metacat.service.repo.impl.StatsRepository;
import ai.floedb.metacat.service.repo.impl.TableRepository;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;
import ai.floedb.metacat.service.storage.IdempotencyStore;
import ai.floedb.metacat.service.storage.util.IdempotencyGuard;

@GrpcService
public class StatsMutationImpl extends BaseServiceImpl implements StatsMutation {

  @Inject TableRepository tables;
  @Inject SnapshotRepository snapshots;
  @Inject StatsRepository stats;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject IdempotencyStore idempotencyStore;

  public StatsMutationImpl() {
  }

  @Override
  public Uni<PutTableStatsResponse> putTableStats(PutTableStatsRequest req) {
    return mapFailures(runWithRetry(() -> {
      var p = principal.get();
      authz.require(p, "table.write");

      var tsNow = nowTs();

      tables.get(req.getTableId()).orElseThrow(() -> GrpcErrors.notFound(
          corrId(),"table", Map.of("id", req.getTableId().getId())));
      snapshots.get(req.getTableId(), req.getSnapshotId())
          .orElseThrow(() -> GrpcErrors.notFound(
              corrId(),"snapshot", Map.of("id", Long.toString(req.getSnapshotId()))));

      final var idemKey = req.hasIdempotency() ? req.getIdempotency().getKey() : "";

      var normalized = req.getStats().toBuilder()
          .setTableId(req.getTableId())
          .setSnapshotId(req.getSnapshotId())
          .build();
      final byte[] fp = normalized.toByteArray();

      var out = MutationOps.createProto(
          p.getTenantId(),
          "PutTableStats",
          idemKey,
          () -> fp,
          () -> {
          stats.putTableStats(req.getTableId(), req.getSnapshotId(), normalized);
          return new IdempotencyGuard.CreateResult<>(
              normalized, req.getTableId());
          },
          (ts) -> stats.metaForTableStats(req.getTableId(), req.getSnapshotId(), tsNow),
          idempotencyStore,
          tsNow,
          IDEMPOTENCY_TTL_SECONDS,
          this::corrId,
          TableStats::parseFrom
      );

      return PutTableStatsResponse.newBuilder()
          .setStats(normalized)
          .setMeta(out.meta)
          .build();
    }), corrId());
  }

  @Override
  public Uni<PutColumnStatsBatchResponse> putColumnStatsBatch(PutColumnStatsBatchRequest req) {
    return mapFailures(runWithRetry(() -> {
      var p = principal.get();
      authz.require(p, "table.write");

      var tsNow = nowTs();
      
      tables.get(req.getTableId()).orElseThrow(() -> GrpcErrors.notFound(
          corrId(),"table", Map.of("id", req.getTableId().getId())));
      snapshots.get(req.getTableId(), req.getSnapshotId())
          .orElseThrow(() -> GrpcErrors.notFound(
              corrId(),"snapshot", Map.of("id", Long.toString(req.getSnapshotId()))));

      final var tenant = p.getTenantId();
      final var baseKey = req.hasIdempotency() ? req.getIdempotency().getKey() : "";

      int upserted = 0;
      for (var raw : req.getColumnsList()) {
        var cs = raw.toBuilder()
            .setTableId(req.getTableId())
            .setSnapshotId(req.getSnapshotId())
            .build();

        final byte[] fp = cs.toByteArray();
        final String itemKey = baseKey.isBlank() ? "" : (baseKey + "/col/" + cs.getColumnId());

        MutationOps.createProto(
            tenant,
            "PutColumnStats",
            itemKey,
            () -> fp,
            () -> {
              stats.putColumnStats(req.getTableId(), req.getSnapshotId(), cs);
              return new IdempotencyGuard.CreateResult<>(cs, req.getTableId());
            },
            (ignored) -> stats.metaForColumnStats(req.getTableId(), req.getSnapshotId(), cs.getColumnId(), tsNow),
            idempotencyStore,
            tsNow,
            IDEMPOTENCY_TTL_SECONDS,
            this::corrId,
            ColumnStats::parseFrom
        );

        upserted++;
      }

      return PutColumnStatsBatchResponse.newBuilder()
          .setUpserted(upserted)
          .build();
    }), corrId());
  }
}
