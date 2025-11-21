package ai.floedb.metacat.service.statistics.impl;

import ai.floedb.metacat.catalog.rpc.ColumnStats;
import ai.floedb.metacat.catalog.rpc.FileColumnStats;
import ai.floedb.metacat.catalog.rpc.GetTableStatsRequest;
import ai.floedb.metacat.catalog.rpc.GetTableStatsResponse;
import ai.floedb.metacat.catalog.rpc.ListColumnStatsRequest;
import ai.floedb.metacat.catalog.rpc.ListColumnStatsResponse;
import ai.floedb.metacat.catalog.rpc.ListFileColumnStatsRequest;
import ai.floedb.metacat.catalog.rpc.ListFileColumnStatsResponse;
import ai.floedb.metacat.catalog.rpc.PutColumnStatsBatchRequest;
import ai.floedb.metacat.catalog.rpc.PutColumnStatsBatchResponse;
import ai.floedb.metacat.catalog.rpc.PutFileColumnStatsBatchRequest;
import ai.floedb.metacat.catalog.rpc.PutFileColumnStatsBatchResponse;
import ai.floedb.metacat.catalog.rpc.PutTableStatsRequest;
import ai.floedb.metacat.catalog.rpc.PutTableStatsResponse;
import ai.floedb.metacat.catalog.rpc.Snapshot;
import ai.floedb.metacat.catalog.rpc.TableStatisticsService;
import ai.floedb.metacat.catalog.rpc.TableStats;
import ai.floedb.metacat.common.rpc.PageResponse;
import ai.floedb.metacat.common.rpc.SnapshotRef;
import ai.floedb.metacat.common.rpc.SpecialSnapshot;
import ai.floedb.metacat.service.common.BaseServiceImpl;
import ai.floedb.metacat.service.common.IdempotencyGuard;
import ai.floedb.metacat.service.common.LogHelper;
import ai.floedb.metacat.service.common.MutationOps;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.repo.IdempotencyRepository;
import ai.floedb.metacat.service.repo.impl.SnapshotRepository;
import ai.floedb.metacat.service.repo.impl.StatsRepository;
import ai.floedb.metacat.service.repo.impl.TableRepository;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.Map;
import org.jboss.logging.Logger;

@GrpcService
public class TableStatisticsServiceImpl extends BaseServiceImpl implements TableStatisticsService {

  @Inject TableRepository tables;
  @Inject SnapshotRepository snapshots;
  @Inject StatsRepository stats;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject IdempotencyRepository idempotencyStore;

  private static final Logger LOG = Logger.getLogger(TableStatisticsService.class);

  @Override
  public Uni<GetTableStatsResponse> getTableStats(GetTableStatsRequest request) {
    var L = LogHelper.start(LOG, "GetTableStats");

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
                                          correlationId(),
                                          "snapshot",
                                          Map.of("id", tableId.getId())));
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
                                          correlationId(),
                                          "snapshot",
                                          Map.of("id", tableId.getId())));
                    }
                    default ->
                        throw GrpcErrors.invalidArgument(
                            correlationId(), "snapshot.missing", Map.of());
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
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<ListColumnStatsResponse> listColumnStats(ListColumnStatsRequest request) {
    var L = LogHelper.start(LOG, "ListColumnStats");

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
                                          correlationId(),
                                          "snapshot",
                                          Map.of("id", tableId.getId())));
                    }
                    default ->
                        throw GrpcErrors.invalidArgument(
                            correlationId(), "snapshot.missing", Map.of());
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
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<ListFileColumnStatsResponse> listFileColumnStats(ListFileColumnStatsRequest request) {

    var L = LogHelper.start(LOG, "ListFileColumnStats");

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
                                          correlationId(),
                                          "snapshot",
                                          Map.of("id", tableId.getId())));
                    }
                    default ->
                        throw GrpcErrors.invalidArgument(
                            correlationId(), "snapshot.missing", Map.of());
                  }

                  var items = stats.listFileStats(tableId, snapId, Math.max(1, limit), token, next);
                  int total = stats.countFileStats(tableId, snapId);

                  return ListFileColumnStatsResponse.newBuilder()
                      .addAllFileColumns(items)
                      .setPage(
                          PageResponse.newBuilder()
                              .setNextPageToken(next.toString())
                              .setTotalSize(total)
                              .build())
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<PutTableStatsResponse> putTableStats(PutTableStatsRequest request) {
    var L = LogHelper.start(LOG, "PutTableStats");

    return mapFailures(
            runWithRetry(
                () -> {
                  var pc = principal.get();
                  var tenantId = pc.getTenantId();

                  authz.require(pc, "table.write");

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

                  var fingerprint = request.getStats().toByteArray();
                  var explicitKey =
                      request.hasIdempotency() ? request.getIdempotency().getKey().trim() : "";
                  var idempotencyKey = explicitKey.isEmpty() ? null : explicitKey;

                  if (idempotencyKey == null) {
                    stats.putTableStats(
                        request.getTableId(), request.getSnapshotId(), request.getStats());
                    var meta =
                        stats.metaForTableStats(
                            request.getTableId(), request.getSnapshotId(), tsNow);
                    return PutTableStatsResponse.newBuilder()
                        .setStats(request.getStats())
                        .setMeta(meta)
                        .build();
                  }

                  var result =
                      MutationOps.createProto(
                          tenantId,
                          "PutTableStats",
                          idempotencyKey,
                          () -> fingerprint,
                          () -> {
                            stats.putTableStats(
                                request.getTableId(), request.getSnapshotId(), request.getStats());
                            return new IdempotencyGuard.CreateResult<>(
                                request.getStats(), request.getTableId());
                          },
                          ignored ->
                              stats.metaForTableStats(
                                  request.getTableId(), request.getSnapshotId(), tsNow),
                          idempotencyStore,
                          tsNow,
                          idempotencyTtlSeconds(),
                          this::correlationId,
                          TableStats::parseFrom,
                          rec ->
                              stats
                                  .getTableStats(request.getTableId(), request.getSnapshotId())
                                  .isPresent());

                  return PutTableStatsResponse.newBuilder()
                      .setStats(request.getStats())
                      .setMeta(result.meta)
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<PutColumnStatsBatchResponse> putColumnStatsBatch(PutColumnStatsBatchRequest request) {
    var L = LogHelper.start(LOG, "PutColumnStatsBatch");

    return mapFailures(
            runWithRetry(
                () -> {
                  var pc = principal.get();
                  var tenantId = pc.getTenantId();

                  authz.require(pc, "table.write");

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

                  int upserted = 0;
                  for (var raw : request.getColumnsList()) {
                    var columnStats =
                        raw.toBuilder()
                            .setTableId(request.getTableId())
                            .setSnapshotId(request.getSnapshotId())
                            .build();

                    var fingerprint = raw.toByteArray();
                    var explicitKey =
                        request.hasIdempotency() ? request.getIdempotency().getKey().trim() : "";
                    var idempotencyKey = explicitKey.isEmpty() ? null : explicitKey;

                    if (idempotencyKey == null) {
                      stats.putColumnStats(
                          request.getTableId(), request.getSnapshotId(), columnStats);
                      upserted++;
                      continue;
                    }

                    MutationOps.createProto(
                        tenantId,
                        "PutColumnStats",
                        idempotencyKey,
                        () -> fingerprint,
                        () -> {
                          stats.putColumnStats(
                              request.getTableId(), request.getSnapshotId(), columnStats);
                          return new IdempotencyGuard.CreateResult<>(
                              columnStats, request.getTableId());
                        },
                        cs ->
                            stats.metaForColumnStats(
                                request.getTableId(),
                                request.getSnapshotId(),
                                cs.getColumnId(),
                                tsNow),
                        idempotencyStore,
                        tsNow,
                        idempotencyTtlSeconds(),
                        this::correlationId,
                        ColumnStats::parseFrom,
                        rec ->
                            stats
                                .getColumnStats(
                                    request.getTableId(),
                                    request.getSnapshotId(),
                                    raw.getColumnId())
                                .isPresent());

                    upserted++;
                  }

                  return PutColumnStatsBatchResponse.newBuilder().setUpserted(upserted).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<PutFileColumnStatsBatchResponse> putFileColumnStatsBatch(
      PutFileColumnStatsBatchRequest request) {

    var L = LogHelper.start(LOG, "PutFileColumnStatsBatch");

    return mapFailures(
            runWithRetry(
                () -> {
                  var pc = principal.get();
                  var tenantId = pc.getTenantId();

                  authz.require(pc, "table.write");

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

                  int upserted = 0;

                  for (var raw : request.getFilesList()) {
                    var fileStats =
                        raw.toBuilder()
                            .setTableId(request.getTableId())
                            .setSnapshotId(request.getSnapshotId())
                            .build();

                    var fingerprint = raw.toByteArray();
                    var explicitKey =
                        request.hasIdempotency() ? request.getIdempotency().getKey().trim() : "";
                    var idempotencyKey = explicitKey.isEmpty() ? null : explicitKey;

                    if (idempotencyKey == null) {
                      stats.putFileColumnStats(
                          request.getTableId(), request.getSnapshotId(), fileStats);
                      upserted++;
                      continue;
                    }

                    MutationOps.createProto(
                        tenantId,
                        "PutFileColumnStats",
                        idempotencyKey,
                        () -> fingerprint,
                        () -> {
                          stats.putFileColumnStats(
                              request.getTableId(), request.getSnapshotId(), fileStats);
                          return new IdempotencyGuard.CreateResult<>(
                              fileStats, request.getTableId());
                        },
                        fs ->
                            stats.metaForFileColumnStats(
                                request.getTableId(),
                                request.getSnapshotId(),
                                fs.getFilePath(),
                                tsNow),
                        idempotencyStore,
                        tsNow,
                        idempotencyTtlSeconds(),
                        this::correlationId,
                        FileColumnStats::parseFrom,
                        rec ->
                            stats
                                .getFileColumnStats(
                                    request.getTableId(),
                                    request.getSnapshotId(),
                                    raw.getFilePath())
                                .isPresent());

                    upserted++;
                  }

                  return PutFileColumnStatsBatchResponse.newBuilder().setUpserted(upserted).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }
}
