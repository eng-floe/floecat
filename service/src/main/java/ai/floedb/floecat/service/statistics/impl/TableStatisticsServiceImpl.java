package ai.floedb.floecat.service.statistics.impl;

import ai.floedb.floecat.catalog.rpc.ColumnStats;
import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.GetTableStatsRequest;
import ai.floedb.floecat.catalog.rpc.GetTableStatsResponse;
import ai.floedb.floecat.catalog.rpc.ListColumnStatsRequest;
import ai.floedb.floecat.catalog.rpc.ListColumnStatsResponse;
import ai.floedb.floecat.catalog.rpc.ListFileColumnStatsRequest;
import ai.floedb.floecat.catalog.rpc.ListFileColumnStatsResponse;
import ai.floedb.floecat.catalog.rpc.PutColumnStatsRequest;
import ai.floedb.floecat.catalog.rpc.PutColumnStatsResponse;
import ai.floedb.floecat.catalog.rpc.PutFileColumnStatsRequest;
import ai.floedb.floecat.catalog.rpc.PutFileColumnStatsResponse;
import ai.floedb.floecat.catalog.rpc.PutTableStatsRequest;
import ai.floedb.floecat.catalog.rpc.PutTableStatsResponse;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.TableStatisticsService;
import ai.floedb.floecat.catalog.rpc.TableStats;
import ai.floedb.floecat.common.rpc.PageResponse;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.IdempotencyGuard;
import ai.floedb.floecat.service.common.LogHelper;
import ai.floedb.floecat.service.common.MutationOps;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.StatsRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
                  var accountId = pc.getAccountId();

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

                  if (snapshots.getById(request.getTableId(), request.getSnapshotId()).isEmpty()) {
                    LOG.debugf(
                        "Received metrics for unknown snapshot tableId=%s snapshotId=%d; storing"
                            + " stats anyway",
                        request.getTableId().getId(), request.getSnapshotId());
                  }

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
                      runIdempotentCreate(
                          () ->
                              MutationOps.createProto(
                                  accountId,
                                  "PutTableStats",
                                  idempotencyKey,
                                  () -> fingerprint,
                                  () -> {
                                    stats.putTableStats(
                                        request.getTableId(),
                                        request.getSnapshotId(),
                                        request.getStats());
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
                                          .getTableStats(
                                              request.getTableId(), request.getSnapshotId())
                                          .isPresent()));

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
  public Uni<PutColumnStatsResponse> putColumnStats(Multi<PutColumnStatsRequest> requests) {
    var L = LogHelper.start(LOG, "PutColumnStats");

    var state = new AtomicReference<>(StreamState.initial());
    AtomicInteger upserted = new AtomicInteger();

    return mapFailures(
            requests
                .onItem()
                .transformToUniAndConcatenate(
                    req -> runWithRetry(() -> processColumnStats(state, req, upserted)))
                .collect()
                .last()
                .onItem()
                .ifNull()
                .failWith(
                    () ->
                        GrpcErrors.invalidArgument(correlationId(), "column_stats.empty", Map.of()))
                .replaceWith(
                    () -> PutColumnStatsResponse.newBuilder().setUpserted(upserted.get()).build()),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<PutFileColumnStatsResponse> putFileColumnStats(
      Multi<PutFileColumnStatsRequest> requests) {

    var L = LogHelper.start(LOG, "PutFileColumnStats");

    var state = new AtomicReference<>(StreamState.initial());
    AtomicInteger upserted = new AtomicInteger();

    return mapFailures(
            requests
                .onItem()
                .transformToUniAndConcatenate(
                    req -> runWithRetry(() -> processFileColumnStats(state, req, upserted)))
                .collect()
                .last()
                .onItem()
                .ifNull()
                .failWith(
                    () ->
                        GrpcErrors.invalidArgument(
                            correlationId(), "file_column_stats.empty", Map.of()))
                .replaceWith(
                    () ->
                        PutFileColumnStatsResponse.newBuilder()
                            .setUpserted(upserted.get())
                            .build()),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  private record StreamState(
      ResourceId tableId, long snapshotId, String idempotencyKey, boolean validated) {
    static StreamState initial() {
      return new StreamState(null, -1L, null, false);
    }

    StreamState with(
        ResourceId tableId, long snapshotId, String idempotencyKey, boolean validated) {
      return new StreamState(tableId, snapshotId, idempotencyKey, validated);
    }
  }

  private StreamState ensureState(StreamState state, ResourceId tableId, long snapshotId) {
    if (state.tableId == null) {
      return state.with(tableId, snapshotId, null, false);
    }
    if (!state.tableId.equals(tableId) || state.snapshotId != snapshotId) {
      throw GrpcErrors.invalidArgument(correlationId(), "stats.inconsistent_target", Map.of());
    }
    return state;
  }

  private StreamState ensureIdempotency(StreamState state, String candidate) {
    if (candidate == null || candidate.isBlank()) {
      return state;
    }
    if (state.idempotencyKey == null) {
      return state.with(state.tableId, state.snapshotId, candidate.trim(), state.validated);
    }
    if (!state.idempotencyKey.equals(candidate.trim())) {
      throw GrpcErrors.invalidArgument(correlationId(), "idempotency.inconsistent_key", Map.of());
    }
    return state;
  }

  private StreamState validateOnce(StreamState state) {
    if (state.validated) {
      return state;
    }
    var pc = principal.get();
    authz.require(pc, "table.write");

    tables
        .getById(state.tableId)
        .orElseThrow(
            () ->
                GrpcErrors.notFound(correlationId(), "table", Map.of("id", state.tableId.getId())));

    snapshots
        .getById(state.tableId, state.snapshotId)
        .orElseThrow(
            () ->
                GrpcErrors.notFound(
                    correlationId(), "snapshot", Map.of("id", Long.toString(state.snapshotId))));

    return state.with(state.tableId, state.snapshotId, state.idempotencyKey, true);
  }

  private Boolean processColumnStats(
      AtomicReference<StreamState> stateRef, PutColumnStatsRequest req, AtomicInteger upserted) {
    StreamState computed =
        ensureState(stateRef.get(), req.getTableId(), req.getSnapshotId()); // may throw on mismatch
    computed =
        ensureIdempotency(computed, req.hasIdempotency() ? req.getIdempotency().getKey() : null);
    computed = validateOnce(computed);
    stateRef.set(computed);
    final StreamState next = computed;

    var accountId = principal.get().getAccountId();
    var tsNow = nowTs();

    for (var raw : req.getColumnsList()) {
      var columnStats =
          raw.toBuilder().setTableId(next.tableId()).setSnapshotId(next.snapshotId()).build();
      var fingerprint = raw.toByteArray();

      if (next.idempotencyKey() == null) {
        stats.putColumnStats(next.tableId(), next.snapshotId(), columnStats);
        upserted.incrementAndGet();
        continue;
      }

      runIdempotentCreate(
          () ->
              MutationOps.createProto(
                  accountId,
                  "PutColumnStats",
                  next.idempotencyKey(),
                  () -> fingerprint,
                  () -> {
                    stats.putColumnStats(next.tableId(), next.snapshotId(), columnStats);
                    return new IdempotencyGuard.CreateResult<>(columnStats, next.tableId());
                  },
                  cs ->
                      stats.metaForColumnStats(
                          next.tableId(), next.snapshotId(), cs.getColumnId(), tsNow),
                  idempotencyStore,
                  tsNow,
                  idempotencyTtlSeconds(),
                  this::correlationId,
                  ColumnStats::parseFrom,
                  rec ->
                      stats
                          .getColumnStats(next.tableId(), next.snapshotId(), raw.getColumnId())
                          .isPresent()));

      upserted.incrementAndGet();
    }
    return Boolean.TRUE;
  }

  private Boolean processFileColumnStats(
      AtomicReference<StreamState> stateRef,
      PutFileColumnStatsRequest req,
      AtomicInteger upserted) {
    StreamState computed =
        ensureState(stateRef.get(), req.getTableId(), req.getSnapshotId()); // may throw on mismatch
    computed =
        ensureIdempotency(computed, req.hasIdempotency() ? req.getIdempotency().getKey() : null);
    computed = validateOnce(computed);
    stateRef.set(computed);
    final StreamState next = computed;

    var accountId = principal.get().getAccountId();
    var tsNow = nowTs();

    for (var raw : req.getFilesList()) {
      var fileStats =
          raw.toBuilder().setTableId(next.tableId()).setSnapshotId(next.snapshotId()).build();
      var fingerprint = raw.toByteArray();

      if (next.idempotencyKey() == null) {
        stats.putFileColumnStats(next.tableId(), next.snapshotId(), fileStats);
        upserted.incrementAndGet();
        continue;
      }

      runIdempotentCreate(
          () ->
              MutationOps.createProto(
                  accountId,
                  "PutFileColumnStats",
                  next.idempotencyKey(),
                  () -> fingerprint,
                  () -> {
                    stats.putFileColumnStats(next.tableId(), next.snapshotId(), fileStats);
                    return new IdempotencyGuard.CreateResult<>(fileStats, next.tableId());
                  },
                  fs ->
                      stats.metaForFileColumnStats(
                          next.tableId(), next.snapshotId(), fs.getFilePath(), tsNow),
                  idempotencyStore,
                  tsNow,
                  idempotencyTtlSeconds(),
                  this::correlationId,
                  FileColumnStats::parseFrom,
                  rec ->
                      stats
                          .getFileColumnStats(next.tableId(), next.snapshotId(), raw.getFilePath())
                          .isPresent()));

      upserted.incrementAndGet();
    }
    return Boolean.TRUE;
  }
}
