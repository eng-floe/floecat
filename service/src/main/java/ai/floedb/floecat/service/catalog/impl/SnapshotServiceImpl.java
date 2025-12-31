package ai.floedb.floecat.service.catalog.impl;

import ai.floedb.floecat.catalog.rpc.CreateSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.CreateSnapshotResponse;
import ai.floedb.floecat.catalog.rpc.DeleteSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.DeleteSnapshotResponse;
import ai.floedb.floecat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.GetSnapshotResponse;
import ai.floedb.floecat.catalog.rpc.ListSnapshotsRequest;
import ai.floedb.floecat.catalog.rpc.ListSnapshotsResponse;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotService;
import ai.floedb.floecat.catalog.rpc.SnapshotSpec;
import ai.floedb.floecat.catalog.rpc.UpdateSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.UpdateSnapshotResponse;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceKind;
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
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import com.google.protobuf.FieldMask;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
                        snapshotRepo.listByTime(
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
                  var accountId = pc.getAccountId();
                  var corr = pc.getCorrelationId();

                  authz.require(pc, "table.write");

                  var tableId = request.getSpec().getTableId();
                  ensureKind(tableId, ResourceKind.RK_TABLE, "table_id", corr);

                  var table =
                      tableRepo
                          .getById(tableId)
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(
                                      corr, "table", Map.of("id", tableId.getId())));

                  var tsNow = nowTs();

                  var explicitKey =
                      request.hasIdempotency() ? request.getIdempotency().getKey().trim() : "";
                  var idempotencyKey = explicitKey.isEmpty() ? null : explicitKey;

                  var fingerprint = request.getSpec().toBuilder().build().toByteArray();

                  var spec = request.getSpec();
                  var snapBuilder =
                      Snapshot.newBuilder()
                          .setTableId(tableId)
                          .setSnapshotId(spec.getSnapshotId())
                          .setIngestedAt(tsNow)
                          .setUpstreamCreatedAt(spec.getUpstreamCreatedAt())
                          .setParentSnapshotId(spec.getParentSnapshotId());
                  if (spec.hasSchemaJson() && !spec.getSchemaJson().isBlank()) {
                    snapBuilder.setSchemaJson(spec.getSchemaJson());
                  } else if (!table.getSchemaJson().isBlank()) {
                    snapBuilder.setSchemaJson(table.getSchemaJson());
                  }
                  if (spec.hasPartitionSpec()) {
                    snapBuilder.setPartitionSpec(spec.getPartitionSpec());
                  }
                  if (spec.hasSequenceNumber()) {
                    snapBuilder.setSequenceNumber(spec.getSequenceNumber());
                  }
                  if (spec.hasManifestList()) {
                    snapBuilder.setManifestList(spec.getManifestList());
                  }
                  if (!spec.getSummaryMap().isEmpty()) {
                    snapBuilder.putAllSummary(spec.getSummaryMap());
                  }
                  if (spec.hasSchemaId()) {
                    snapBuilder.setSchemaId(spec.getSchemaId());
                  }
                  if (!spec.getFormatMetadataMap().isEmpty()) {
                    snapBuilder.putAllFormatMetadata(spec.getFormatMetadataMap());
                  }
                  var snap = snapBuilder.build();

                  if (idempotencyKey == null) {
                    var existing = snapshotRepo.getById(tableId, snap.getSnapshotId());
                    if (existing.isPresent()) {
                      var stored = normalizeSnapshotForComparison(existing.get());
                      var incoming = normalizeSnapshotForComparison(snap);
                      if (!stored.equals(incoming)) {
                        throw GrpcErrors.conflict(
                            corr,
                            "snapshot.mismatch",
                            Map.of(
                                "table_id", tableId.getId(),
                                "snapshot_id", Long.toString(snap.getSnapshotId())));
                      }
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
                      runIdempotentCreate(
                          () ->
                              MutationOps.createProto(
                                  accountId,
                                  "CreateSnapshot",
                                  idempotencyKey,
                                  () -> fingerprint,
                                  () -> {
                                    snapshotRepo.create(snap);
                                    return new IdempotencyGuard.CreateResult<>(
                                        snap, snap.getTableId());
                                  },
                                  s -> snapshotRepo.metaForSafe(s.getTableId(), s.getSnapshotId()),
                                  idempotencyStore,
                                  tsNow,
                                  idempotencyTtlSeconds(),
                                  this::correlationId,
                                  Snapshot::parseFrom));

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

  @Override
  public Uni<UpdateSnapshotResponse> updateSnapshot(UpdateSnapshotRequest request) {
    var L = LogHelper.start(LOG, "UpdateSnapshot");

    return mapFailures(
            runWithRetry(
                () -> {
                  var pc = principal.get();
                  var corr = pc.getCorrelationId();

                  authz.require(pc, "table.write");

                  if (!request.hasSpec()) {
                    throw GrpcErrors.invalidArgument(corr, "spec.required", Map.of());
                  }

                  var spec = request.getSpec();
                  if (!spec.hasTableId()
                      || spec.getTableId().getId().isBlank()
                      || spec.getSnapshotId() == 0L) {
                    throw GrpcErrors.invalidArgument(corr, "spec.missing_ids", Map.of());
                  }

                  var tableId = spec.getTableId();
                  long snapshotId = spec.getSnapshotId();
                  ensureKind(tableId, ResourceKind.RK_TABLE, "table_id", corr);

                  if (!request.hasUpdateMask() || request.getUpdateMask().getPathsCount() == 0) {
                    throw GrpcErrors.invalidArgument(corr, "update_mask.required", Map.of());
                  }

                  var meta = snapshotRepo.metaFor(tableId, snapshotId);
                  enforcePreconditions(corr, meta, request.getPrecondition());

                  var existing =
                      snapshotRepo
                          .getById(tableId, snapshotId)
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(
                                      corr,
                                      "snapshot",
                                      Map.of(
                                          "table_id", tableId.getId(),
                                          "snapshot_id", Long.toString(snapshotId))));

                  var desired =
                      applySnapshotSpecPatch(existing, spec, request.getUpdateMask(), corr);

                  if (desired.equals(existing)) {
                    var noopMeta = snapshotRepo.metaForSafe(tableId, snapshotId);
                    enforcePreconditions(corr, noopMeta, request.getPrecondition());
                    return UpdateSnapshotResponse.newBuilder()
                        .setSnapshot(existing)
                        .setMeta(noopMeta)
                        .build();
                  }

                  var conflictInfo =
                      Map.of(
                          "table_id", tableId.getId(),
                          "snapshot_id", Long.toString(snapshotId));

                  try {
                    boolean ok = snapshotRepo.update(desired, meta.getPointerVersion());
                    if (!ok) {
                      var nowMeta = snapshotRepo.metaForSafe(tableId, snapshotId);
                      throw GrpcErrors.preconditionFailed(
                          corr,
                          "version_mismatch",
                          Map.of(
                              "expected", Long.toString(meta.getPointerVersion()),
                              "actual", Long.toString(nowMeta.getPointerVersion())));
                    }
                  } catch (BaseResourceRepository.NameConflictException nce) {
                    throw GrpcErrors.conflict(corr, "snapshot.already_exists", conflictInfo);
                  } catch (BaseResourceRepository.PreconditionFailedException pfe) {
                    var nowMeta = snapshotRepo.metaForSafe(tableId, snapshotId);
                    throw GrpcErrors.preconditionFailed(
                        corr,
                        "version_mismatch",
                        Map.of(
                            "expected", Long.toString(meta.getPointerVersion()),
                            "actual", Long.toString(nowMeta.getPointerVersion())));
                  }

                  var outMeta = snapshotRepo.metaForSafe(tableId, snapshotId);

                  return UpdateSnapshotResponse.newBuilder()
                      .setSnapshot(desired)
                      .setMeta(outMeta)
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  private static final Set<String> SNAPSHOT_MUTABLE_PATHS =
      Set.of(
          "upstream_created_at",
          "parent_snapshot_id",
          "schema_json",
          "partition_spec",
          "sequence_number",
          "manifest_list",
          "summary",
          "schema_id",
          "format_metadata");

  private Snapshot applySnapshotSpecPatch(
      Snapshot current, SnapshotSpec spec, FieldMask mask, String corr) {
    var paths = normalizedMaskPaths(mask);
    if (paths.isEmpty()) {
      throw GrpcErrors.invalidArgument(corr, "update_mask.required", Map.of());
    }

    for (var path : paths) {
      if (!SNAPSHOT_MUTABLE_PATHS.contains(path)) {
        throw GrpcErrors.invalidArgument(corr, "update_mask.path.invalid", Map.of("path", path));
      }
    }

    var builder = current.toBuilder();
    for (String path : paths) {
      switch (path) {
        case "upstream_created_at" -> {
          if (!spec.hasUpstreamCreatedAt()) {
            throw GrpcErrors.invalidArgument(
                corr, "snapshot.upstream_created_at.required", Map.of());
          }
          builder.setUpstreamCreatedAt(spec.getUpstreamCreatedAt());
        }
        case "parent_snapshot_id" -> builder.setParentSnapshotId(spec.getParentSnapshotId());
        case "schema_json" -> {
          if (!spec.hasSchemaJson()) {
            throw GrpcErrors.invalidArgument(corr, "snapshot.schema_json.required", Map.of());
          }
          builder.setSchemaJson(spec.getSchemaJson());
        }
        case "partition_spec" -> {
          if (!spec.hasPartitionSpec()) {
            throw GrpcErrors.invalidArgument(corr, "snapshot.partition_spec.required", Map.of());
          }
          builder.setPartitionSpec(spec.getPartitionSpec());
        }
        case "sequence_number" -> {
          if (!spec.hasSequenceNumber()) {
            throw GrpcErrors.invalidArgument(corr, "snapshot.sequence_number.required", Map.of());
          }
          builder.setSequenceNumber(spec.getSequenceNumber());
        }
        case "manifest_list" -> {
          if (!spec.hasManifestList()) {
            throw GrpcErrors.invalidArgument(corr, "snapshot.manifest_list.required", Map.of());
          }
          builder.setManifestList(spec.getManifestList());
        }
        case "summary" -> {
          builder.clearSummary();
          builder.putAllSummary(spec.getSummaryMap());
        }
        case "schema_id" -> {
          if (!spec.hasSchemaId()) {
            throw GrpcErrors.invalidArgument(corr, "snapshot.schema_id.required", Map.of());
          }
          builder.setSchemaId(spec.getSchemaId());
        }
        case "format_metadata" -> {
          if (spec.getFormatMetadataCount() == 0) {
            throw GrpcErrors.invalidArgument(corr, "snapshot.iceberg.required", Map.of());
          }
          builder.clearFormatMetadata();
          builder.putAllFormatMetadata(spec.getFormatMetadataMap());
        }
        default ->
            throw GrpcErrors.invalidArgument(
                corr, "update_mask.path.invalid", Map.of("path", path));
      }
    }
    return builder.build();
  }

  private static Snapshot normalizeSnapshotForComparison(Snapshot snapshot) {
    return snapshot.toBuilder().clearIngestedAt().build();
  }
}
