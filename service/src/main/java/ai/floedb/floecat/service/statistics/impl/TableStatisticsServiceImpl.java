/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.service.statistics.impl;

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.*;

import ai.floedb.floecat.catalog.rpc.GetTargetStatsRequest;
import ai.floedb.floecat.catalog.rpc.GetTargetStatsResponse;
import ai.floedb.floecat.catalog.rpc.ListTargetStatsRequest;
import ai.floedb.floecat.catalog.rpc.ListTargetStatsResponse;
import ai.floedb.floecat.catalog.rpc.PutTargetStatsRequest;
import ai.floedb.floecat.catalog.rpc.PutTargetStatsResponse;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.StatsMetadata;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.StatsTargetKind;
import ai.floedb.floecat.catalog.rpc.TableStatisticsService;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
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
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.stats.spi.StatsTargetType;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.jboss.logging.Logger;

@GrpcService
public class TableStatisticsServiceImpl extends BaseServiceImpl implements TableStatisticsService {

  @Inject TableRepository tables;
  @Inject SnapshotRepository snapshots;
  @Inject StatsStore statsStore;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject IdempotencyRepository idempotencyStore;

  private static final Logger LOG = Logger.getLogger(TableStatisticsService.class);

  @Override
  public Uni<GetTargetStatsResponse> getTargetStats(GetTargetStatsRequest request) {
    var L = LogHelper.start(LOG, "GetTargetStats");

    return mapFailures(
            run(
                () -> {
                  var principalContext = principal.get();

                  authz.require(principalContext, "catalog.read");

                  final var tableId = request.getTableId();
                  final long snapId = resolveSnapshotId(tableId, request.getSnapshot());
                  StatsTarget target = request.getTarget();
                  if (target == null
                      || target.getTargetCase() == StatsTarget.TargetCase.TARGET_NOT_SET) {
                    throw GrpcErrors.invalidArgument(
                        correlationId(), STATS_INCONSISTENT_TARGET, Map.of());
                  }

                  return statsStore
                      .getTargetStats(tableId, snapId, target)
                      .map(
                          statsRecord ->
                              GetTargetStatsResponse.newBuilder().setStats(statsRecord).build())
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  correlationId(),
                                  TABLE_STATS,
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
  public Uni<ListTargetStatsResponse> listTargetStats(ListTargetStatsRequest request) {
    var L = LogHelper.start(LOG, "ListTargetStats");

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
                  final var tableId = request.getTableId();
                  final long snapId = resolveSnapshotId(tableId, request.getSnapshot());
                  List<StatsTargetKind> kinds = request.getTargetKindsList();
                  if (kinds.size() > 1) {
                    throw GrpcErrors.invalidArgument(
                        correlationId(),
                        STATS_INCONSISTENT_TARGET,
                        Map.of("reason", "list_target_stats supports at most one target kind"));
                  }

                  Optional<StatsTargetType> targetType =
                      kinds.isEmpty() || kinds.get(0) == StatsTargetKind.STK_UNSPECIFIED
                          ? Optional.empty()
                          : Optional.of(toTargetType(kinds.get(0)));

                  var listResult =
                      statsStore.listTargetStats(
                          tableId, snapId, targetType, Math.max(1, limit), token);
                  int total = statsStore.countTargetStats(tableId, snapId, targetType);

                  return ListTargetStatsResponse.newBuilder()
                      .addAllRecords(listResult.records())
                      .setPage(
                          PageResponse.newBuilder()
                              .setNextPageToken(listResult.nextPageToken())
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
  public Uni<PutTargetStatsResponse> putTargetStats(Multi<PutTargetStatsRequest> requests) {
    var L = LogHelper.start(LOG, "PutTargetStats");

    var state = new AtomicReference<>(StreamState.initial());
    AtomicInteger upserted = new AtomicInteger();

    return mapFailures(
            requests
                .onItem()
                .transformToUniAndConcatenate(
                    req -> runWithRetry(() -> processTargetStats(state, req, upserted)))
                .collect()
                .last()
                .onItem()
                .ifNull()
                .failWith(
                    () -> GrpcErrors.invalidArgument(correlationId(), TARGET_STATS_EMPTY, Map.of()))
                .replaceWith(
                    () -> PutTargetStatsResponse.newBuilder().setUpserted(upserted.get()).build()),
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
      throw GrpcErrors.invalidArgument(correlationId(), STATS_INCONSISTENT_TARGET, Map.of());
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
      throw GrpcErrors.invalidArgument(correlationId(), IDEMPOTENCY_INCONSISTENT_KEY, Map.of());
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
            () -> GrpcErrors.notFound(correlationId(), TABLE, Map.of("id", state.tableId.getId())));

    snapshots
        .getById(state.tableId, state.snapshotId)
        .orElseThrow(
            () ->
                GrpcErrors.notFound(
                    correlationId(), SNAPSHOT, Map.of("id", Long.toString(state.snapshotId))));

    return state.with(state.tableId, state.snapshotId, state.idempotencyKey, true);
  }

  private Boolean processTargetStats(
      AtomicReference<StreamState> stateRef, PutTargetStatsRequest req, AtomicInteger upserted) {
    StreamState computed =
        ensureState(stateRef.get(), req.getTableId(), req.getSnapshotId()); // may throw on mismatch
    computed =
        ensureIdempotency(computed, req.hasIdempotency() ? req.getIdempotency().getKey() : null);
    computed = validateOnce(computed);
    stateRef.set(computed);
    final StreamState next = computed;

    var accountId = principal.get().getAccountId();
    var tsNow = nowTs();

    for (var raw : req.getRecordsList()) {
      if (!raw.hasTarget()
          || raw.getTarget().getTargetCase() == StatsTarget.TargetCase.TARGET_NOT_SET) {
        throw GrpcErrors.invalidArgument(correlationId(), STATS_INCONSISTENT_TARGET, Map.of());
      }
      var targetRecord =
          raw.toBuilder().setTableId(next.tableId()).setSnapshotId(next.snapshotId()).build();
      validateStatsMetadata(targetRecord.hasMetadata() ? targetRecord.getMetadata() : null);
      var fingerprint = StatsCanonicalizer.canonicalFingerprint(targetRecord);

      if (next.idempotencyKey() == null) {
        statsStore.putTargetStats(targetRecord);
        upserted.incrementAndGet();
        continue;
      }

      String targetKey = StatsTargetIdentity.storageId(targetRecord.getTarget());
      var itemKey = itemIdempotencyKey(next.idempotencyKey(), "target", hashString(targetKey));
      runIdempotentCreate(
          () ->
              MutationOps.createProto(
                  accountId,
                  "PutTargetStats",
                  itemKey,
                  () -> fingerprint,
                  () -> {
                    statsStore.putTargetStats(targetRecord);
                    return new IdempotencyGuard.CreateResult<>(targetRecord, next.tableId());
                  },
                  rec ->
                      statsStore.metaForTargetStats(
                          next.tableId(), next.snapshotId(), rec.getTarget(), tsNow),
                  idempotencyStore,
                  tsNow,
                  idempotencyTtlSeconds(),
                  this::correlationId,
                  TargetStatsRecord::parseFrom));

      upserted.incrementAndGet();
    }
    return Boolean.TRUE;
  }

  private static String itemIdempotencyKey(String baseKey, String kind, Object itemId) {
    return baseKey + ":" + kind + ":" + String.valueOf(itemId);
  }

  private static String hashString(String value) {
    if (value == null || value.isBlank()) {
      return "empty";
    }
    return hashFingerprint(value.getBytes(StandardCharsets.UTF_8));
  }

  private void validateStatsMetadata(StatsMetadata metadata) {
    if (metadata == null) {
      return;
    }
    if (metadata.hasConfidenceLevel()) {
      double value = metadata.getConfidenceLevel();
      if (Double.isNaN(value) || value < 0.0d || value > 1.0d) {
        throw GrpcErrors.invalidArgument(
            correlationId(),
            STATS_METADATA_CONFIDENCE_INVALID,
            Map.of("value", Double.toString(value)));
      }
    }
    if (!metadata.hasCoverage()) {
      return;
    }
    validateCoverageField(
        "rows_scanned",
        metadata.getCoverage().hasRowsScanned(),
        metadata.getCoverage().getRowsScanned());
    validateCoverageField(
        "files_scanned",
        metadata.getCoverage().hasFilesScanned(),
        metadata.getCoverage().getFilesScanned());
    validateCoverageField(
        "row_groups_sampled",
        metadata.getCoverage().hasRowGroupsSampled(),
        metadata.getCoverage().getRowGroupsSampled());
    validateCoverageField(
        "bytes_scanned",
        metadata.getCoverage().hasBytesScanned(),
        metadata.getCoverage().getBytesScanned());
  }

  private void validateCoverageField(String field, boolean present, long value) {
    if (!present || value >= 0L) {
      return;
    }
    throw GrpcErrors.invalidArgument(
        correlationId(),
        STATS_METADATA_COVERAGE_NEGATIVE,
        Map.of("field", field, "value", Long.toString(value)));
  }

  private long resolveSnapshotId(ResourceId tableId, SnapshotRef ref) {
    if (ref == null || ref.getWhichCase() == SnapshotRef.WhichCase.WHICH_NOT_SET) {
      throw GrpcErrors.invalidArgument(correlationId(), SNAPSHOT_MISSING, Map.of());
    }
    return switch (ref.getWhichCase()) {
      case SNAPSHOT_ID -> ref.getSnapshotId();
      case SPECIAL -> {
        if (ref.getSpecial() != SpecialSnapshot.SS_CURRENT) {
          throw GrpcErrors.invalidArgument(correlationId(), SNAPSHOT_SPECIAL_MISSING, Map.of());
        }
        yield snapshots
            .getCurrentSnapshot(tableId)
            .map(Snapshot::getSnapshotId)
            .orElseThrow(
                () ->
                    GrpcErrors.notFound(correlationId(), SNAPSHOT, Map.of("id", tableId.getId())));
      }
      case AS_OF ->
          snapshots
              .getAsOf(tableId, ref.getAsOf())
              .map(Snapshot::getSnapshotId)
              .orElseThrow(
                  () ->
                      GrpcErrors.notFound(
                          correlationId(), SNAPSHOT, Map.of("id", tableId.getId())));
      default -> throw GrpcErrors.invalidArgument(correlationId(), SNAPSHOT_MISSING, Map.of());
    };
  }

  private static StatsTargetType toTargetType(StatsTargetKind kind) {
    return switch (kind) {
      case STK_TABLE -> StatsTargetType.TABLE;
      case STK_COLUMN -> StatsTargetType.COLUMN;
      case STK_EXPRESSION -> StatsTargetType.EXPRESSION;
      case STK_FILE -> StatsTargetType.FILE;
      case STK_UNSPECIFIED ->
          throw new IllegalArgumentException("target kind must not be STK_UNSPECIFIED");
      case UNRECOGNIZED -> throw new IllegalArgumentException("target kind is unrecognized");
    };
  }
}
