package ai.floedb.metacat.service.catalog.impl;

import ai.floedb.metacat.catalog.rpc.CreateSnapshotRequest;
import ai.floedb.metacat.catalog.rpc.CreateSnapshotResponse;
import ai.floedb.metacat.catalog.rpc.DeleteSnapshotRequest;
import ai.floedb.metacat.catalog.rpc.DeleteSnapshotResponse;
import ai.floedb.metacat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.metacat.catalog.rpc.GetSnapshotResponse;
import ai.floedb.metacat.catalog.rpc.ListSnapshotsRequest;
import ai.floedb.metacat.catalog.rpc.ListSnapshotsResponse;
import ai.floedb.metacat.catalog.rpc.Snapshot;
import ai.floedb.metacat.catalog.rpc.SnapshotService;
import ai.floedb.metacat.common.rpc.MutationMeta;
import ai.floedb.metacat.common.rpc.ResourceKind;
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
import ai.floedb.metacat.service.repo.util.BaseResourceRepository;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import org.jboss.logging.Logger;

@GrpcService
public class SnapshotServiceImpl extends BaseServiceImpl implements SnapshotService {

  @Inject TableRepository tableRepo;
  @Inject SnapshotRepository snapshotRepo;
  @Inject StatsRepository statsRepo;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject IdempotencyRepository idempotencyStore;

  private static final Logger LOG = Logger.getLogger(SnapshotService.class);

  @Override
  public Uni<ListSnapshotsResponse> listSnapshots(ListSnapshotsRequest request) {
    var L = LogHelper.start(LOG, "ListSnapshots");

    return mapFailures(
            run(
                () -> {
                  var principalContext = principal.get();
                  authz.require(principalContext, "table.read");

                  tableRepo
                      .getById(request.getTableId())
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  correlationId(),
                                  "table",
                                  Map.of("id", request.getTableId().getId())));

                  var pageIn = MutationOps.pageIn(request.hasPage() ? request.getPage() : null);

                  var next = new StringBuilder();
                  final List<Snapshot> snaps;
                  try {
                    snaps =
                        snapshotRepo.list(
                            request.getTableId(), Math.max(1, pageIn.limit), pageIn.token, next);
                  } catch (IllegalArgumentException badToken) {
                    throw GrpcErrors.invalidArgument(
                        correlationId(), "page_token.invalid", Map.of("page_token", pageIn.token));
                  }

                  int total = snapshotRepo.count(request.getTableId());

                  var page = MutationOps.pageOut(next.toString(), total);

                  return ListSnapshotsResponse.newBuilder()
                      .addAllSnapshots(snaps)
                      .setPage(page)
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<GetSnapshotResponse> getSnapshot(GetSnapshotRequest request) {
    var L = LogHelper.start(LOG, "GetCurrentSnapshot");

    return mapFailures(
            run(
                () -> {
                  var principalContext = principal.get();

                  authz.require(principalContext, "table.read");

                  final var tableId = request.getTableId();
                  tableRepo
                      .getById(tableId)
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  correlationId(), "table", Map.of("id", tableId.getId())));

                  final var ref = request.getSnapshot();
                  if (ref == null || ref.getWhichCase() == SnapshotRef.WhichCase.WHICH_NOT_SET) {
                    throw GrpcErrors.invalidArgument(correlationId(), "snapshot.missing", Map.of());
                  }

                  Snapshot snap;
                  switch (ref.getWhichCase()) {
                    case SNAPSHOT_ID -> {
                      var snapId = ref.getSnapshotId();
                      snap =
                          snapshotRepo
                              .getById(request.getTableId(), snapId)
                              .orElseThrow(
                                  () ->
                                      GrpcErrors.notFound(
                                          correlationId(),
                                          "snapshot",
                                          Map.of(
                                              "reason",
                                              "no_snapshots",
                                              "table_id",
                                              request.getTableId().getId())));
                    }
                    case SPECIAL -> {
                      if (ref.getSpecial() != SpecialSnapshot.SS_CURRENT) {
                        throw GrpcErrors.invalidArgument(
                            correlationId(), "snapshot.special.missing", Map.of());
                      }
                      snap =
                          snapshotRepo
                              .getCurrentSnapshot(tableId)
                              .orElseThrow(
                                  () ->
                                      GrpcErrors.notFound(
                                          correlationId(),
                                          "snapshot",
                                          Map.of("id", tableId.getId())));
                    }
                    case AS_OF -> {
                      var asOf = ref.getAsOf();
                      snap =
                          snapshotRepo
                              .getAsOf(tableId, asOf)
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

                  return GetSnapshotResponse.newBuilder().setSnapshot(snap).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<CreateSnapshotResponse> createSnapshot(CreateSnapshotRequest request) {
    var L = LogHelper.start(LOG, "CreateSnapshot");

    return mapFailures(
            runWithRetry(
                () -> {
                  var pc = principal.get();
                  var tenantId = pc.getTenantId();
                  var corr = pc.getCorrelationId();

                  authz.require(pc, "table.write");

                  var tableId = request.getSpec().getTableId();
                  ensureKind(tableId, ResourceKind.RK_TABLE, "table_id", corr);

                  tableRepo
                      .getById(tableId)
                      .orElseThrow(
                          () -> GrpcErrors.notFound(corr, "table", Map.of("id", tableId.getId())));

                  var tsNow = nowTs();

                  var explicitKey =
                      request.hasIdempotency() ? request.getIdempotency().getKey().trim() : "";
                  var idempotencyKey = explicitKey.isEmpty() ? null : explicitKey;

                  var fingerprint = request.getSpec().toBuilder().build().toByteArray();

                  var snap =
                      Snapshot.newBuilder()
                          .setTableId(tableId)
                          .setSnapshotId(request.getSpec().getSnapshotId())
                          .setIngestedAt(tsNow)
                          .setUpstreamCreatedAt(request.getSpec().getUpstreamCreatedAt())
                          .setParentSnapshotId(request.getSpec().getParentSnapshotId())
                          .build();

                  if (idempotencyKey == null) {
                    var existing = snapshotRepo.getById(tableId, snap.getSnapshotId());
                    if (existing.isPresent()) {
                      var meta = snapshotRepo.metaForSafe(tableId, snap.getSnapshotId());
                      return CreateSnapshotResponse.newBuilder()
                          .setSnapshot(existing.get())
                          .setMeta(meta)
                          .build();
                    }
                    snapshotRepo.create(snap);
                    var meta = snapshotRepo.metaForSafe(tableId, snap.getSnapshotId());
                    return CreateSnapshotResponse.newBuilder()
                        .setSnapshot(snap)
                        .setMeta(meta)
                        .build();
                  }

                  var result =
                      MutationOps.createProto(
                          tenantId,
                          "CreateSnapshot",
                          idempotencyKey,
                          () -> fingerprint,
                          () -> {
                            snapshotRepo.create(snap);
                            return new IdempotencyGuard.CreateResult<>(snap, snap.getTableId());
                          },
                          s -> snapshotRepo.metaForSafe(s.getTableId(), s.getSnapshotId()),
                          idempotencyStore,
                          tsNow,
                          idempotencyTtlSeconds(),
                          this::correlationId,
                          Snapshot::parseFrom,
                          rec ->
                              snapshotRepo
                                  .getById(rec.getResourceId(), snap.getSnapshotId())
                                  .isPresent());

                  return CreateSnapshotResponse.newBuilder()
                      .setSnapshot(result.body)
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
  public Uni<DeleteSnapshotResponse> deleteSnapshot(DeleteSnapshotRequest request) {
    var L = LogHelper.start(LOG, "DeleteSnapshot");

    return mapFailures(
            runWithRetry(
                () -> {
                  var principalContext = principal.get();
                  var correlationId = principalContext.getCorrelationId();

                  authz.require(principalContext, "table.write");

                  var tableId = request.getTableId();
                  long snapshotId = request.getSnapshotId();
                  ensureKind(tableId, ResourceKind.RK_TABLE, "table_id", correlationId);

                  MutationMeta meta;
                  try {
                    meta = snapshotRepo.metaFor(tableId, snapshotId);
                  } catch (BaseResourceRepository.NotFoundException e) {
                    statsRepo.deleteAllStatsForSnapshot(tableId, snapshotId);
                    return DeleteSnapshotResponse.newBuilder().build();
                  }

                  long expectedVersion = meta.getPointerVersion();
                  enforcePreconditions(correlationId, meta, request.getPrecondition());

                  try {
                    boolean ok =
                        snapshotRepo.deleteWithPrecondition(tableId, snapshotId, expectedVersion);
                    if (!ok) {
                      var nowMeta = snapshotRepo.metaForSafe(tableId, snapshotId);
                      throw GrpcErrors.preconditionFailed(
                          correlationId,
                          "version_mismatch",
                          Map.of(
                              "expected", Long.toString(expectedVersion),
                              "actual", Long.toString(nowMeta.getPointerVersion())));
                    }

                    statsRepo.deleteAllStatsForSnapshot(tableId, snapshotId);
                  } catch (BaseResourceRepository.PreconditionFailedException pfe) {
                    var nowMeta = snapshotRepo.metaForSafe(tableId, snapshotId);
                    throw GrpcErrors.preconditionFailed(
                        correlationId,
                        "version_mismatch",
                        Map.of(
                            "expected", Long.toString(expectedVersion),
                            "actual", Long.toString(nowMeta.getPointerVersion())));
                  } catch (BaseResourceRepository.NotFoundException nfe) {
                    throw GrpcErrors.notFound(
                        correlationId, "snapshot", Map.of("id", Long.toString(snapshotId)));
                  }

                  return DeleteSnapshotResponse.newBuilder().setMeta(meta).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }
}
