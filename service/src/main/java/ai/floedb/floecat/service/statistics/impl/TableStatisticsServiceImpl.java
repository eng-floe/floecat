package ai.floedb.floecat.service.statistics.impl;

import ai.floedb.floecat.catalog.rpc.ColumnStats;
import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.GetTableStatsRequest;
import ai.floedb.floecat.catalog.rpc.GetTableStatsResponse;
import ai.floedb.floecat.catalog.rpc.ListColumnStatsRequest;
import ai.floedb.floecat.catalog.rpc.ListColumnStatsResponse;
import ai.floedb.floecat.catalog.rpc.ListFileColumnStatsRequest;
import ai.floedb.floecat.catalog.rpc.ListFileColumnStatsResponse;
import ai.floedb.floecat.catalog.rpc.Ndv;
import ai.floedb.floecat.catalog.rpc.NdvApprox;
import ai.floedb.floecat.catalog.rpc.NdvSketch;
import ai.floedb.floecat.catalog.rpc.PutColumnStatsRequest;
import ai.floedb.floecat.catalog.rpc.PutColumnStatsResponse;
import ai.floedb.floecat.catalog.rpc.PutFileColumnStatsRequest;
import ai.floedb.floecat.catalog.rpc.PutFileColumnStatsResponse;
import ai.floedb.floecat.catalog.rpc.PutTableStatsRequest;
import ai.floedb.floecat.catalog.rpc.PutTableStatsResponse;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.TableStatisticsService;
import ai.floedb.floecat.catalog.rpc.TableStats;
import ai.floedb.floecat.catalog.rpc.UpstreamStamp;
import ai.floedb.floecat.common.rpc.PageResponse;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.Canonicalizer;
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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
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

                  var fingerprint = canonicalFingerprint(request.getStats());
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
                                  TableStats::parseFrom));

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
      var fingerprint = canonicalFingerprint(columnStats);

      if (next.idempotencyKey() == null) {
        stats.putColumnStats(next.tableId(), next.snapshotId(), columnStats);
        upserted.incrementAndGet();
        continue;
      }

      var itemKey = itemIdempotencyKey(next.idempotencyKey(), "col", raw.getColumnId());
      runIdempotentCreate(
          () ->
              MutationOps.createProto(
                  accountId,
                  "PutColumnStats",
                  itemKey,
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
                  ColumnStats::parseFrom));

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
      var fingerprint = canonicalFingerprint(fileStats);

      if (next.idempotencyKey() == null) {
        stats.putFileColumnStats(next.tableId(), next.snapshotId(), fileStats);
        upserted.incrementAndGet();
        continue;
      }

      var itemKey =
          itemIdempotencyKey(next.idempotencyKey(), "file", hashFilePath(raw.getFilePath()));
      runIdempotentCreate(
          () ->
              MutationOps.createProto(
                  accountId,
                  "PutFileColumnStats",
                  itemKey,
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
                  FileColumnStats::parseFrom));

      upserted.incrementAndGet();
    }
    return Boolean.TRUE;
  }

  private static String itemIdempotencyKey(String baseKey, String kind, Object itemId) {
    return baseKey + ":" + kind + ":" + String.valueOf(itemId);
  }

  private static String hashFilePath(String filePath) {
    if (filePath == null || filePath.isBlank()) {
      return "empty";
    }
    return hashFingerprint(filePath.getBytes(StandardCharsets.UTF_8));
  }

  private static byte[] canonicalFingerprint(TableStats stats) {
    var c = new Canonicalizer();
    canonicalResourceId(c, "table_id", stats.getTableId());
    c.scalar("snapshot_id", stats.getSnapshotId());
    canonicalUpstream(c, "upstream", stats.hasUpstream() ? stats.getUpstream() : null);
    c.scalar("row_count", stats.getRowCount());
    c.scalar("data_file_count", stats.getDataFileCount());
    c.scalar("total_size_bytes", stats.getTotalSizeBytes());
    canonicalNdv(c, "ndv", stats.hasNdv() ? stats.getNdv() : null);
    c.map("properties", stats.getPropertiesMap());
    return c.bytes();
  }

  private static byte[] canonicalFingerprint(ColumnStats stats) {
    var c = new Canonicalizer();
    canonicalResourceId(c, "table_id", stats.getTableId());
    c.scalar("snapshot_id", stats.getSnapshotId());
    c.scalar("column_id", stats.getColumnId());
    c.scalar("column_name", stats.getColumnName());
    c.scalar("logical_type", stats.getLogicalType());
    canonicalUpstream(c, "upstream", stats.hasUpstream() ? stats.getUpstream() : null);
    c.scalar("value_count", stats.getValueCount());
    c.scalar("null_count", stats.getNullCount());
    c.scalar("nan_count", stats.getNanCount());
    canonicalNdv(c, "ndv", stats.hasNdv() ? stats.getNdv() : null);
    c.scalar("min", stats.getMin());
    c.scalar("max", stats.getMax());
    c.scalar("histogram_b64", bytesToB64(stats.getHistogram().toByteArray()));
    c.scalar("tdigest_b64", bytesToB64(stats.getTdigest().toByteArray()));
    c.map("properties", stats.getPropertiesMap());
    return c.bytes();
  }

  private static byte[] canonicalFingerprint(FileColumnStats stats) {
    var c = new Canonicalizer();
    canonicalResourceId(c, "table_id", stats.getTableId());
    c.scalar("snapshot_id", stats.getSnapshotId());
    c.scalar("file_path", stats.getFilePath());
    c.scalar("file_format", stats.getFileFormat());
    c.scalar("row_count", stats.getRowCount());
    c.scalar("size_bytes", stats.getSizeBytes());
    c.scalar("file_content", stats.getFileContent().name());
    c.scalar("partition_data_json", stats.getPartitionDataJson());
    c.scalar("partition_spec_id", stats.getPartitionSpecId());
    if (stats.hasSequenceNumber()) {
      c.scalar("sequence_number", stats.getSequenceNumber());
    }

    var eqIds = new ArrayList<Integer>(stats.getEqualityFieldIdsList());
    eqIds.sort(Comparator.naturalOrder());
    c.list("equality_field_ids", eqIds);

    var cols = new ArrayList<>(stats.getColumnsList());
    cols.sort(
        Comparator.comparingInt(ColumnStats::getColumnId)
            .thenComparing(ColumnStats::getColumnName));
    for (var col : cols) {
      c.group(
          "column_" + col.getColumnId(),
          g -> g.scalar("fp_b64", bytesToB64(canonicalFingerprint(col))));
    }

    return c.bytes();
  }

  private static void canonicalResourceId(Canonicalizer c, String key, ResourceId id) {
    c.group(
        key,
        g -> {
          if (id == null) {
            return;
          }
          g.scalar("account_id", id.getAccountId());
          g.scalar("id", id.getId());
          g.scalar("kind", id.getKind().name());
        });
  }

  private static void canonicalUpstream(Canonicalizer c, String key, UpstreamStamp up) {
    if (up == null) {
      return;
    }
    c.group(
        key,
        g -> {
          g.scalar("system", up.getSystem().name());
          g.scalar("table_native_id", up.getTableNativeId());
          g.scalar("commit_ref", up.getCommitRef());
          g.scalar("fetched_at_seconds", up.hasFetchedAt() ? up.getFetchedAt().getSeconds() : 0L);
          g.scalar("fetched_at_nanos", up.hasFetchedAt() ? up.getFetchedAt().getNanos() : 0);
          g.map("properties", up.getPropertiesMap());
        });
  }

  private static void canonicalNdv(Canonicalizer c, String key, Ndv ndv) {
    if (ndv == null) {
      return;
    }
    c.group(
        key,
        g -> {
          g.scalar("mode", ndv.getModeCase().name());
          if (ndv.hasExact()) {
            g.scalar("exact", ndv.getExact());
          }
          if (ndv.hasApprox()) {
            canonicalNdvApprox(g, "approx", ndv.getApprox());
          }
          var sketches = new ArrayList<>(ndv.getSketchesList());
          sketches.sort(
              Comparator.comparing(NdvSketch::getType)
                  .thenComparing(NdvSketch::getEncoding)
                  .thenComparing(NdvSketch::getCompression)
                  .thenComparingInt(NdvSketch::getVersion)
                  .thenComparing(s -> bytesToB64(s.getData().toByteArray())));
          for (var sketch : sketches) {
            canonicalNdvSketch(g, "sketch_" + sketch.getType(), sketch);
          }
        });
  }

  private static void canonicalNdvApprox(Canonicalizer c, String key, NdvApprox approx) {
    c.group(
        key,
        g -> {
          g.scalar("estimate", approx.getEstimate());
          g.scalar("relative_standard_error", approx.getRelativeStandardError());
          g.scalar("confidence_lower", approx.getConfidenceLower());
          g.scalar("confidence_upper", approx.getConfidenceUpper());
          g.scalar("confidence_level", approx.getConfidenceLevel());
          g.scalar("rows_seen", approx.getRowsSeen());
          g.scalar("rows_total", approx.getRowsTotal());
          g.scalar("method", approx.getMethod());
          g.map("properties", approx.getPropertiesMap());
        });
  }

  private static void canonicalNdvSketch(Canonicalizer c, String key, NdvSketch sketch) {
    c.group(
        key,
        g -> {
          g.scalar("type", sketch.getType());
          g.scalar("encoding", sketch.getEncoding());
          g.scalar("compression", sketch.getCompression());
          g.scalar("version", sketch.getVersion());
          g.scalar("data_b64", bytesToB64(sketch.getData().toByteArray()));
          g.map("properties", sketch.getPropertiesMap());
        });
  }

  private static String bytesToB64(byte[] data) {
    if (data == null || data.length == 0) {
      return "";
    }
    return Base64.getEncoder().encodeToString(data);
  }
}
