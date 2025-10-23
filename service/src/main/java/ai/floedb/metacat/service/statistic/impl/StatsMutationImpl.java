package ai.floedb.metacat.service.statistic.impl;

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
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.Map;

@GrpcService
public class StatsMutationImpl extends BaseServiceImpl implements StatsMutation {
  @Inject TableRepository tables;
  @Inject SnapshotRepository snapshots;
  @Inject StatsRepository stats;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject IdempotencyStore idempotencyStore;

  @Override
  public Uni<PutTableStatsResponse> putTableStats(PutTableStatsRequest request) {
    return mapFailures(
        runWithRetry(
            () -> {
              var principalContext = principal.get();

              authz.require(principalContext, "table.write");

              var tsNow = nowTs();

              tables
                  .getById(request.getTableId())
                  .orElseThrow(
                      () ->
                          GrpcErrors.notFound(
                              correlationId(),
                              "table",
                              Map.of("id", request.getTableId().getId())));

              snapshots
                  .getById(request.getTableId(), request.getSnapshotId())
                  .orElseThrow(
                      () ->
                          GrpcErrors.notFound(
                              correlationId(),
                              "snapshot",
                              Map.of("id", Long.toString(request.getSnapshotId()))));

              var idemKey = request.hasIdempotency() ? request.getIdempotency().getKey() : "";

              var normalized =
                  request.getStats().toBuilder()
                      .setTableId(request.getTableId())
                      .setSnapshotId(request.getSnapshotId())
                      .build();
              byte[] fingerprint = normalized.toByteArray();

              var tableStatsProto =
                  MutationOps.createProto(
                      principalContext.getTenantId().getId(),
                      "PutTableStats",
                      idemKey,
                      () -> fingerprint,
                      () -> {
                        stats.putTableStats(
                            request.getTableId(), request.getSnapshotId(), normalized);
                        return new IdempotencyGuard.CreateResult<>(
                            normalized, request.getTableId());
                      },
                      (ignored) ->
                          stats.metaForTableStats(
                              request.getTableId(), request.getSnapshotId(), tsNow),
                      idempotencyStore,
                      tsNow,
                      IDEMPOTENCY_TTL_SECONDS,
                      this::correlationId,
                      TableStats::parseFrom);

              return PutTableStatsResponse.newBuilder()
                  .setStats(normalized)
                  .setMeta(tableStatsProto.meta)
                  .build();
            }),
        correlationId());
  }

  @Override
  public Uni<PutColumnStatsBatchResponse> putColumnStatsBatch(PutColumnStatsBatchRequest request) {
    return mapFailures(
        runWithRetry(
            () -> {
              var principalContext = principal.get();

              authz.require(principalContext, "table.write");

              var tsNow = nowTs();

              tables
                  .getById(request.getTableId())
                  .orElseThrow(
                      () ->
                          GrpcErrors.notFound(
                              correlationId(),
                              "table",
                              Map.of("id", request.getTableId().getId())));

              snapshots
                  .getById(request.getTableId(), request.getSnapshotId())
                  .orElseThrow(
                      () ->
                          GrpcErrors.notFound(
                              correlationId(),
                              "snapshot",
                              Map.of("id", Long.toString(request.getSnapshotId()))));

              var tenant = principalContext.getTenantId().getId();
              var baseKey = request.hasIdempotency() ? request.getIdempotency().getKey() : "";

              int upserted = 0;
              for (var raw : request.getColumnsList()) {
                var columnStats =
                    raw.toBuilder()
                        .setTableId(request.getTableId())
                        .setSnapshotId(request.getSnapshotId())
                        .build();

                byte[] fingerprint = columnStats.toByteArray();
                String idempotencyKey =
                    baseKey.isBlank() ? "" : (baseKey + "/col/" + columnStats.getColumnId());

                MutationOps.createProto(
                    tenant,
                    "PutColumnStats",
                    idempotencyKey,
                    () -> fingerprint,
                    () -> {
                      stats.putColumnStats(
                          request.getTableId(), request.getSnapshotId(), columnStats);
                      return new IdempotencyGuard.CreateResult<>(columnStats, request.getTableId());
                    },
                    (colStats) ->
                        stats.metaForColumnStats(
                            request.getTableId(),
                            request.getSnapshotId(),
                            colStats.getColumnId(),
                            tsNow),
                    idempotencyStore,
                    tsNow,
                    IDEMPOTENCY_TTL_SECONDS,
                    this::correlationId,
                    ColumnStats::parseFrom);

                upserted++;
              }

              return PutColumnStatsBatchResponse.newBuilder().setUpserted(upserted).build();
            }),
        correlationId());
  }
}
