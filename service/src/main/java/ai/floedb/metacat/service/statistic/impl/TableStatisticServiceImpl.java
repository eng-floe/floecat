package ai.floedb.metacat.service.statistic.impl;

import ai.floedb.metacat.catalog.rpc.*;
import ai.floedb.metacat.common.rpc.PageResponse;
import ai.floedb.metacat.common.rpc.SnapshotRef;
import ai.floedb.metacat.common.rpc.SpecialSnapshot;
import ai.floedb.metacat.service.common.BaseServiceImpl;
import ai.floedb.metacat.service.common.MutationOps;
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
public class TableStatisticServiceImpl extends BaseServiceImpl implements TableStatisticService {

  @Inject TableRepository tables;
  @Inject SnapshotRepository snapshots;
  @Inject StatsRepository stats;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject IdempotencyStore idempotencyStore;

  @Override
  public Uni<GetTableStatsResponse> getTableStats(GetTableStatsRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principal.get();

              authz.require(principalContext, "catalog.read");

              final var tableId = request.getTableId();
              final var ref = request.getSnapshot();
              if (ref == null || ref.getWhichCase() == SnapshotRef.WhichCase.WHICH_NOT_SET) {
                throw GrpcErrors.invalidArgument(correlationId(), "snapshot.missing", Map.of());
              }

              final long snapId;
              switch (ref.getWhichCase()) {
                case SNAPSHOT_ID -> snapId = ref.getSnapshotId();
                case SPECIAL -> {
                  if (ref.getSpecial() != SpecialSnapshot.SS_CURRENT) {
                    throw GrpcErrors.invalidArgument(
                        correlationId(), "snapshot.special.missing", Map.of());
                  }
                  snapId =
                      snapshots
                          .getCurrentSnapshot(tableId)
                          .map(Snapshot::getSnapshotId)
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(
                                      correlationId(), "snapshot", Map.of("id", tableId.getId())));
                }
                case AS_OF -> {
                  var asOf = ref.getAsOf();
                  snapId =
                      snapshots
                          .getAsOf(tableId, asOf)
                          .map(Snapshot::getSnapshotId)
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(
                                      correlationId(), "snapshot", Map.of("id", tableId.getId())));
                }
                default ->
                    throw GrpcErrors.invalidArgument(correlationId(), "snapshot.missing", Map.of());
              }

              return stats
                  .getTableStats(tableId, snapId)
                  .map(s -> GetTableStatsResponse.newBuilder().setStats(s).build())
                  .orElseThrow(
                      () ->
                          GrpcErrors.notFound(
                              correlationId(),
                              "table_stats",
                              Map.of(
                                  "table_id",
                                  tableId.getId(),
                                  "snapshot_id",
                                  Long.toString(snapId))));
            }),
        correlationId());
  }

  @Override
  public Uni<ListColumnStatsResponse> listColumnStats(ListColumnStatsRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principal.get();

              authz.require(principalContext, "catalog.read");

              final int limit =
                  (request.hasPage() && request.getPage().getPageSize() > 0)
                      ? request.getPage().getPageSize()
                      : 200;
              final String token = request.hasPage() ? request.getPage().getPageToken() : "";
              final StringBuilder next = new StringBuilder();

              final var tableId = request.getTableId();
              final var ref = request.getSnapshot();
              if (ref == null || ref.getWhichCase() == SnapshotRef.WhichCase.WHICH_NOT_SET) {
                throw GrpcErrors.invalidArgument(correlationId(), "snapshot.missing", Map.of());
              }

              final long snapId;
              switch (ref.getWhichCase()) {
                case SNAPSHOT_ID -> snapId = ref.getSnapshotId();
                case SPECIAL -> {
                  if (ref.getSpecial() != SpecialSnapshot.SS_CURRENT) {
                    throw GrpcErrors.invalidArgument(
                        correlationId(), "snapshot.special.missing", Map.of());
                  }
                  snapId =
                      snapshots
                          .getCurrentSnapshot(tableId)
                          .map(Snapshot::getSnapshotId)
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(
                                      correlationId(), "snapshot", Map.of("id", tableId.getId())));
                }
                default ->
                    throw GrpcErrors.invalidArgument(correlationId(), "snapshot.missing", Map.of());
              }

              var items = stats.list(tableId, snapId, Math.max(1, limit), token, next);
              int total = stats.count(tableId, snapId);

              return ListColumnStatsResponse.newBuilder()
                  .addAllColumns(items)
                  .setPage(
                      PageResponse.newBuilder()
                          .setNextPageToken(next.toString())
                          .setTotalSize(total)
                          .build())
                  .build();
            }),
        correlationId());
  }

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
                      principalContext.getTenantId(),
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

              var tenant = principalContext.getTenantId();
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
