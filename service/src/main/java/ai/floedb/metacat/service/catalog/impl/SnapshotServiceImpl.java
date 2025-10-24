package ai.floedb.metacat.service.catalog.impl;

import ai.floedb.metacat.catalog.rpc.*;
import ai.floedb.metacat.common.rpc.PageResponse;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.catalog.util.MutationOps;
import ai.floedb.metacat.service.common.BaseServiceImpl;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.repo.impl.*;
import ai.floedb.metacat.service.repo.util.BaseRepository;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;
import ai.floedb.metacat.service.storage.IdempotencyStore;
import ai.floedb.metacat.service.storage.PointerStore;
import ai.floedb.metacat.service.storage.util.IdempotencyGuard;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.Map;

@GrpcService
public class SnapshotServiceImpl extends BaseServiceImpl implements SnapshotService {

  @Inject TableRepository tableRepo;
  @Inject SnapshotRepository snapshotRepo;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject PointerStore ptr;
  @Inject IdempotencyStore idempotencyStore;

  @Override
  public Uni<ListSnapshotsResponse> listSnapshots(ListSnapshotsRequest request) {
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

              var snaps = snapshotRepo.list(request.getTableId(), Math.max(1, limit), token, next);
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
        correlationId());
  }

  @Override
  public Uni<GetCurrentSnapshotResponse> getCurrentSnapshot(GetCurrentSnapshotRequest request) {
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
        correlationId());
  }

  @Override
  public Uni<CreateSnapshotResponse> createSnapshot(CreateSnapshotRequest request) {
    return mapFailures(
        runWithRetry(
            () -> {
              var principalContext = principal.get();
              var correlationId = principalContext.getCorrelationId();

              authz.require(principalContext, "table.write");

              var tsNow = nowTs();

              var tenantId = principalContext.getTenantId().getId();
              var idempotencyKey =
                  request.hasIdempotency() ? request.getIdempotency().getKey() : "";

              byte[] fingerprint = request.getSpec().toBuilder().build().toByteArray();

              var snapshotProto =
                  MutationOps.createProto(
                      tenantId,
                      "CreateSnapshot",
                      idempotencyKey,
                      () -> fingerprint,
                      () -> {
                        var snap =
                            Snapshot.newBuilder()
                                .setTableId(request.getSpec().getTableId())
                                .setSnapshotId(request.getSpec().getSnapshotId())
                                .setIngestedAt(tsNow)
                                .setUpstreamCreatedAt(request.getSpec().getUpstreamCreatedAt())
                                .setParentSnapshotId(request.getSpec().getParentSnapshotId())
                                .build();
                        try {
                          snapshotRepo.create(snap);
                        } catch (BaseRepository.NameConflictException e) {
                          if (!idempotencyKey.isBlank()) {
                            var existing =
                                snapshotRepo.getById(
                                    request.getSpec().getTableId(),
                                    request.getSpec().getSnapshotId());
                            if (existing.isPresent()) {
                              return new IdempotencyGuard.CreateResult<>(
                                  existing.get(), existing.get().getTableId());
                            }
                          }
                          throw GrpcErrors.conflict(
                              correlationId,
                              "snapshot.already_exists",
                              Map.of("id", Long.toString(request.getSpec().getSnapshotId())));
                        }

                        return new IdempotencyGuard.CreateResult<>(snap, snap.getTableId());
                      },
                      (snapshot) ->
                          snapshotRepo.metaForSafe(snapshot.getTableId(), snapshot.getSnapshotId()),
                      idempotencyStore,
                      tsNow,
                      IDEMPOTENCY_TTL_SECONDS,
                      this::correlationId,
                      Snapshot::parseFrom);

              return CreateSnapshotResponse.newBuilder().setMeta(snapshotProto.meta).build();
            }),
        correlationId());
  }

  @Override
  public Uni<DeleteSnapshotResponse> deleteSnapshot(DeleteSnapshotRequest request) {
    return mapFailures(
        runWithRetry(
            () -> {
              var principalContext = principal.get();
              var correlationId = principalContext.getCorrelationId();

              authz.require(principalContext, "table.write");

              var tableId = request.getTableId();
              long snapshotId = request.getSnapshotId();
              ensureKind(tableId, ResourceKind.RK_TABLE, "table_id", correlationId);

              var meta = snapshotRepo.metaFor(tableId, snapshotId);
              long expectedVersion = meta.getPointerVersion();
              enforcePreconditions(correlationId, meta, request.getPrecondition());

              try {
                snapshotRepo.deleteWithPrecondition(tableId, snapshotId, expectedVersion);
              } catch (BaseRepository.PreconditionFailedException pfe) {
                var nowMeta = snapshotRepo.metaForSafe(tableId, snapshotId);
                throw GrpcErrors.preconditionFailed(
                    correlationId,
                    "version_mismatch",
                    Map.of(
                        "expected",
                        Long.toString(expectedVersion),
                        "actual",
                        Long.toString(nowMeta.getPointerVersion())));
              } catch (BaseRepository.NotFoundException nfe) {
                throw GrpcErrors.notFound(
                    correlationId, "snapshot", Map.of("id", Long.toString(snapshotId)));
              }

              return DeleteSnapshotResponse.newBuilder().setMeta(meta).build();
            }),
        correlationId());
  }
}
