package ai.floedb.metacat.service.catalog.impl;

import ai.floedb.metacat.catalog.rpc.CreateSnapshotRequest;
import ai.floedb.metacat.catalog.rpc.CreateSnapshotResponse;
import ai.floedb.metacat.catalog.rpc.DeleteSnapshotRequest;
import ai.floedb.metacat.catalog.rpc.DeleteSnapshotResponse;
import ai.floedb.metacat.catalog.rpc.GetCurrentSnapshotRequest;
import ai.floedb.metacat.catalog.rpc.GetCurrentSnapshotResponse;
import ai.floedb.metacat.catalog.rpc.ListSnapshotsRequest;
import ai.floedb.metacat.catalog.rpc.ListSnapshotsResponse;
import ai.floedb.metacat.catalog.rpc.Snapshot;
import ai.floedb.metacat.catalog.rpc.SnapshotService;
import ai.floedb.metacat.common.rpc.MutationMeta;
import ai.floedb.metacat.common.rpc.PageResponse;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.common.BaseServiceImpl;
import ai.floedb.metacat.service.common.IdempotencyGuard;
import ai.floedb.metacat.service.common.LogHelper;
import ai.floedb.metacat.service.common.MutationOps;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.repo.IdempotencyRepository;
import ai.floedb.metacat.service.repo.impl.SnapshotRepository;
import ai.floedb.metacat.service.repo.impl.TableRepository;
import ai.floedb.metacat.service.repo.util.BaseResourceRepository;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.Map;
import org.jboss.logging.Logger;

@GrpcService
public class SnapshotServiceImpl extends BaseServiceImpl implements SnapshotService {

  @Inject TableRepository tableRepo;
  @Inject SnapshotRepository snapshotRepo;
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

                  final int limit =
                      (request.hasPage() && request.getPage().getPageSize() > 0)
                          ? request.getPage().getPageSize()
                          : 50;
                  final String token = request.hasPage() ? request.getPage().getPageToken() : "";
                  final StringBuilder next = new StringBuilder();

                  var snaps =
                      snapshotRepo.list(request.getTableId(), Math.max(1, limit), token, next);
                  int total = snapshotRepo.count(request.getTableId());

                  var page =
                      PageResponse.newBuilder()
                          .setNextPageToken(next.toString())
                          .setTotalSize(total)
                          .build();

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
  public Uni<GetCurrentSnapshotResponse> getCurrentSnapshot(GetCurrentSnapshotRequest request) {
    var L = LogHelper.start(LOG, "GetCurrentSnapshot");

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

                  var snap =
                      snapshotRepo
                          .getCurrentSnapshot(request.getTableId())
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

                  return GetCurrentSnapshotResponse.newBuilder().setSnapshot(snap).build();
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
                  var principalContext = principal.get();
                  var tenantId = principalContext.getTenantId();
                  var correlationId = principalContext.getCorrelationId();

                  authz.require(principalContext, "table.write");

                  var tableId = request.getSpec().getTableId();
                  ensureKind(tableId, ResourceKind.RK_TABLE, "table_id", correlationId);

                  tableRepo
                      .getById(tableId)
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  correlationId, "table", Map.of("id", tableId.getId())));

                  var tsNow = nowTs();

                  var fingerprint = request.getSpec().toBuilder().build().toByteArray();
                  var idempotencyKey =
                      request.hasIdempotency() && !request.getIdempotency().getKey().isBlank()
                          ? request.getIdempotency().getKey()
                          : hashFingerprint(fingerprint);

                  var snapshotProto =
                      MutationOps.createProto(
                          tenantId,
                          "CreateSnapshot",
                          idempotencyKey,
                          () -> fingerprint,
                          () -> {
                            var snap =
                                Snapshot.newBuilder()
                                    .setTableId(tableId)
                                    .setSnapshotId(request.getSpec().getSnapshotId())
                                    .setIngestedAt(tsNow)
                                    .setUpstreamCreatedAt(request.getSpec().getUpstreamCreatedAt())
                                    .setParentSnapshotId(request.getSpec().getParentSnapshotId())
                                    .build();
                            snapshotRepo.create(snap);
                            return new IdempotencyGuard.CreateResult<>(snap, snap.getTableId());
                          },
                          (snapshot) ->
                              snapshotRepo.metaForSafe(
                                  snapshot.getTableId(), snapshot.getSnapshotId()),
                          idempotencyStore,
                          tsNow,
                          idempotencyTtlSeconds(),
                          this::correlationId,
                          Snapshot::parseFrom,
                          rec ->
                              snapshotRepo
                                  .getById(rec.getResourceId(), request.getSpec().getSnapshotId())
                                  .isPresent());

                  return CreateSnapshotResponse.newBuilder()
                      .setSnapshot(snapshotProto.body)
                      .setMeta(snapshotProto.meta)
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
                    snapshotRepo.delete(tableId, snapshotId);
                    return DeleteSnapshotResponse.newBuilder()
                        .setMeta(snapshotRepo.metaForSafe(tableId, snapshotId))
                        .build();
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
