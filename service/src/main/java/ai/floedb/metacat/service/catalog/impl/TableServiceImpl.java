package ai.floedb.metacat.service.catalog.impl;

import ai.floedb.metacat.catalog.rpc.*;
import ai.floedb.metacat.common.rpc.MutationMeta;
import ai.floedb.metacat.common.rpc.PageResponse;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.catalog.util.MutationOps;
import ai.floedb.metacat.service.common.BaseServiceImpl;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.repo.impl.CatalogRepository;
import ai.floedb.metacat.service.repo.impl.NamespaceRepository;
import ai.floedb.metacat.service.repo.impl.TableRepository;
import ai.floedb.metacat.service.repo.util.BaseRepository;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;
import ai.floedb.metacat.service.storage.IdempotencyStore;
import ai.floedb.metacat.service.storage.util.IdempotencyGuard;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.UUID;

@GrpcService
public class TableServiceImpl extends BaseServiceImpl implements TableService {

  @Inject CatalogRepository catalogRepo;
  @Inject NamespaceRepository nsRepo;
  @Inject TableRepository tableRepo;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject IdempotencyStore idempotencyStore;

  @Override
  public Uni<ListTablesResponse> listTables(ListTablesRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principal.get();
              authz.require(principalContext, "table.read");

              var namespaceId = request.getNamespaceId();
              var namespace =
                  nsRepo
                      .getById(namespaceId)
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  correlationId(), "namespace", Map.of("id", namespaceId.getId())));

              final int limit =
                  (request.hasPage() && request.getPage().getPageSize() > 0)
                      ? request.getPage().getPageSize()
                      : 50;
              final String token = request.hasPage() ? request.getPage().getPageToken() : "";
              final StringBuilder next = new StringBuilder();

              var catalogId = namespace.getCatalogId();
              var items =
                  tableRepo.list(
                      principalContext.getTenantId(),
                      catalogId.getId(),
                      namespaceId.getId(),
                      Math.max(1, limit),
                      token,
                      next);

              int total =
                  tableRepo.count(
                      principalContext.getTenantId(), catalogId.getId(), namespaceId.getId());

              var page =
                  PageResponse.newBuilder()
                      .setNextPageToken(next.toString())
                      .setTotalSize(total)
                      .build();

              return ListTablesResponse.newBuilder().addAllTables(items).setPage(page).build();
            }),
        correlationId());
  }

  @Override
  public Uni<CreateTableResponse> createTable(CreateTableRequest request) {
    return mapFailures(
        runWithRetry(
            () -> {
              var principalContext = principal.get();
              var correlationId = principalContext.getCorrelationId();
              authz.require(principalContext, "table.write");

              var tsNow = nowTs();

              catalogRepo
                  .getById(request.getSpec().getCatalogId())
                  .orElseThrow(
                      () ->
                          GrpcErrors.notFound(
                              correlationId,
                              "catalog",
                              Map.of("id", request.getSpec().getCatalogId().getId())));

              nsRepo
                  .getById(request.getSpec().getNamespaceId())
                  .orElseThrow(
                      () ->
                          GrpcErrors.notFound(
                              correlationId,
                              "namespace",
                              Map.of("id", request.getSpec().getNamespaceId().getId())));

              var idempotencyKey =
                  request.hasIdempotency() ? request.getIdempotency().getKey() : "";
              byte[] fingerprint =
                  request.getSpec().toBuilder().clearDescription().build().toByteArray();

              var tenantId = principalContext.getTenantId();
              var tableProto =
                  MutationOps.createProto(
                      tenantId,
                      "CreateTable",
                      idempotencyKey,
                      () -> fingerprint,
                      () -> {
                        String tableUuid =
                            !idempotencyKey.isBlank()
                                ? deterministicUuid(tenantId, "table", idempotencyKey)
                                : UUID.randomUUID().toString();

                        var tableResourceId =
                            ResourceId.newBuilder()
                                .setTenantId(tenantId)
                                .setId(tableUuid)
                                .setKind(ResourceKind.RK_TABLE)
                                .build();

                        var table =
                            Table.newBuilder()
                                .setResourceId(tableResourceId)
                                .setDisplayName(
                                    mustNonEmpty(
                                        request.getSpec().getDisplayName(),
                                        "display_name",
                                        correlationId))
                                .setDescription(request.getSpec().getDescription())
                                .setFormat(request.getSpec().getFormat())
                                .setCatalogId(request.getSpec().getCatalogId())
                                .setNamespaceId(request.getSpec().getNamespaceId())
                                .setRootUri(
                                    mustNonEmpty(
                                        request.getSpec().getRootUri(), "root_uri", correlationId))
                                .setSchemaJson(
                                    mustNonEmpty(
                                        request.getSpec().getSchemaJson(),
                                        "schema_json",
                                        correlationId))
                                .setCreatedAt(tsNow)
                                .build();

                        try {
                          tableRepo.create(table);
                        } catch (BaseRepository.NameConflictException e) {
                          if (!idempotencyKey.isBlank()) {
                            return tableRepo
                                .getByName(
                                    tenantId,
                                    request.getSpec().getCatalogId().getId(),
                                    request.getSpec().getNamespaceId().getId(),
                                    table.getDisplayName())
                                .map(
                                    existing ->
                                        new IdempotencyGuard.CreateResult<>(
                                            existing, existing.getResourceId()))
                                .orElseThrow(
                                    () ->
                                        GrpcErrors.conflict(
                                            correlationId,
                                            "table.already_exists",
                                            Map.of("display_name", table.getDisplayName())));
                          }
                          throw GrpcErrors.conflict(
                              correlationId,
                              "table.already_exists",
                              Map.of("display_name", table.getDisplayName()));
                        }

                        return new IdempotencyGuard.CreateResult<>(table, tableResourceId);
                      },
                      (table) -> tableRepo.metaForSafe(table.getResourceId()),
                      idempotencyStore,
                      tsNow,
                      IDEMPOTENCY_TTL_SECONDS,
                      this::correlationId,
                      Table::parseFrom);

              return CreateTableResponse.newBuilder()
                  .setTable(tableProto.body)
                  .setMeta(tableProto.meta)
                  .build();
            }),
        correlationId());
  }

  @Override
  public Uni<GetTableDescriptorResponse> getTableDescriptor(GetTableDescriptorRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principal.get();
              authz.require(principalContext, "table.read");

              var table =
                  tableRepo
                      .getById(request.getTableId())
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  correlationId(),
                                  "table",
                                  Map.of("id", request.getTableId().getId())));
              return GetTableDescriptorResponse.newBuilder().setTable(table).build();
            }),
        correlationId());
  }

  @Override
  public Uni<UpdateTableSchemaResponse> updateTableSchema(UpdateTableSchemaRequest request) {
    return mapFailures(
        runWithRetry(
            () -> {
              var principalContext = principal.get();
              var correlationId = principalContext.getCorrelationId();
              authz.require(principalContext, "table.write");

              var tableId = request.getTableId();
              ensureKind(tableId, ResourceKind.RK_TABLE, "table_id", correlationId);

              var current =
                  tableRepo
                      .getById(tableId)
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  correlationId, "table", Map.of("id", tableId.getId())));

              var updated = current.toBuilder().setSchemaJson(request.getSchemaJson()).build();

              if (updated.equals(current)) {
                var metaNoop = tableRepo.metaForSafe(tableId);
                enforcePreconditions(correlationId, metaNoop, request.getPrecondition());
                return UpdateTableSchemaResponse.newBuilder()
                    .setTable(current)
                    .setMeta(metaNoop)
                    .build();
              }

              var meta = tableRepo.metaFor(tableId);
              long expectedVersion = meta.getPointerVersion();
              enforcePreconditions(correlationId, meta, request.getPrecondition());

              try {
                tableRepo.update(updated, expectedVersion);
              } catch (BaseRepository.PreconditionFailedException pfe) {
                var nowMeta = tableRepo.metaForSafe(tableId);
                throw GrpcErrors.preconditionFailed(
                    correlationId,
                    "version_mismatch",
                    Map.of(
                        "expected", Long.toString(expectedVersion),
                        "actual", Long.toString(nowMeta.getPointerVersion())));
              }

              var outMeta = tableRepo.metaForSafe(tableId);
              return UpdateTableSchemaResponse.newBuilder()
                  .setTable(updated)
                  .setMeta(outMeta)
                  .build();
            }),
        correlationId());
  }

  @Override
  public Uni<RenameTableResponse> renameTable(RenameTableRequest request) {
    return mapFailures(
        runWithRetry(
            () -> {
              var principalContext = principal.get();
              var correlationId = principalContext.getCorrelationId();
              authz.require(principalContext, "table.write");

              var tableId = request.getTableId();
              ensureKind(tableId, ResourceKind.RK_TABLE, "table_id", correlationId);

              var current =
                  tableRepo
                      .getById(tableId)
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  correlationId, "table", Map.of("id", tableId.getId())));

              var newDisplayName =
                  mustNonEmpty(request.getNewDisplayName(), "display_name", correlationId);

              if (newDisplayName.equals(current.getDisplayName())) {
                var metaNoop = tableRepo.metaForSafe(tableId);
                enforcePreconditions(correlationId, metaNoop, request.getPrecondition());
                return RenameTableResponse.newBuilder().setTable(current).setMeta(metaNoop).build();
              }

              var updated = current.toBuilder().setDisplayName(newDisplayName).build();

              var meta = tableRepo.metaFor(tableId);
              long expectedVersion = meta.getPointerVersion();
              enforcePreconditions(correlationId, meta, request.getPrecondition());

              try {
                boolean ok = tableRepo.update(updated, expectedVersion);
                if (!ok) {
                  var nowMeta = tableRepo.metaForSafe(tableId);
                  throw GrpcErrors.preconditionFailed(
                      correlationId,
                      "version_mismatch",
                      Map.of(
                          "expected", Long.toString(expectedVersion),
                          "actual", Long.toString(nowMeta.getPointerVersion())));
                }
              } catch (BaseRepository.NameConflictException nce) {
                throw GrpcErrors.conflict(
                    correlationId, "table.already_exists", Map.of("display_name", newDisplayName));
              }

              var outMeta = tableRepo.metaForSafe(tableId);
              var latest = tableRepo.getById(tableId).orElse(updated);
              return RenameTableResponse.newBuilder().setTable(latest).setMeta(outMeta).build();
            }),
        correlationId());
  }

  @Override
  public Uni<MoveTableResponse> moveTable(MoveTableRequest request) {
    return mapFailures(
        runWithRetry(
            () -> {
              final var pc = principal.get();
              final var corr = pc.getCorrelationId();
              authz.require(pc, "table.write");

              final var tableId = request.getTableId();
              ensureKind(tableId, ResourceKind.RK_TABLE, "table_id", corr);

              final var current =
                  tableRepo
                      .getById(tableId)
                      .orElseThrow(
                          () -> GrpcErrors.notFound(corr, "table", Map.of("id", tableId.getId())));

              if (!request.hasNewNamespaceId() || request.getNewNamespaceId().getId().isBlank()) {
                throw GrpcErrors.invalidArgument(corr, null, Map.of("field", "new_namespace_id"));
              }

              final var newNamespaceId = request.getNewNamespaceId();
              ensureKind(newNamespaceId, ResourceKind.RK_NAMESPACE, "new_namespace_id", corr);

              final var newNamespace =
                  nsRepo
                      .getById(newNamespaceId)
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  corr, "namespace", Map.of("id", newNamespaceId.getId())));
              final var newCatalogId = newNamespace.getCatalogId();

              final var targetDisplayName =
                  (request.getNewDisplayName() != null && !request.getNewDisplayName().isBlank())
                      ? request.getNewDisplayName()
                      : current.getDisplayName();

              final boolean sameNamespace =
                  current.getNamespaceId().getId().equals(newNamespaceId.getId());
              final boolean sameName = current.getDisplayName().equals(targetDisplayName);

              if (sameNamespace && sameName) {
                final var metaNoop = tableRepo.metaForSafe(tableId);
                enforcePreconditions(corr, metaNoop, request.getPrecondition());
                return MoveTableResponse.newBuilder().setTable(current).setMeta(metaNoop).build();
              }

              final var updated =
                  current.toBuilder()
                      .setDisplayName(targetDisplayName)
                      .setCatalogId(newCatalogId)
                      .setNamespaceId(newNamespaceId)
                      .build();

              final var meta = tableRepo.metaFor(tableId);
              final long expectedVersion = meta.getPointerVersion();
              enforcePreconditions(corr, meta, request.getPrecondition());

              try {
                boolean ok = tableRepo.update(updated, expectedVersion);
                if (!ok) {
                  final var nowMeta = tableRepo.metaForSafe(tableId);
                  throw GrpcErrors.preconditionFailed(
                      corr,
                      "version_mismatch",
                      Map.of(
                          "expected", Long.toString(expectedVersion),
                          "actual", Long.toString(nowMeta.getPointerVersion())));
                }
              } catch (BaseRepository.NameConflictException nce) {
                throw GrpcErrors.conflict(
                    corr, "table.already_exists", Map.of("display_name", targetDisplayName));
              }

              final var outMeta = tableRepo.metaForSafe(tableId);
              final var latest = tableRepo.getById(tableId).orElse(updated);
              return MoveTableResponse.newBuilder().setTable(latest).setMeta(outMeta).build();
            }),
        correlationId());
  }

  @Override
  public Uni<DeleteTableResponse> deleteTable(DeleteTableRequest request) {
    return mapFailures(
        runWithRetry(
            () -> {
              var principalContext = principal.get();
              var correlationId = principalContext.getCorrelationId();
              authz.require(principalContext, "table.write");

              var tableId = request.getTableId();
              ensureKind(tableId, ResourceKind.RK_TABLE, "table_id", correlationId);

              MutationMeta meta;
              try {
                meta = tableRepo.metaFor(tableId);
              } catch (BaseRepository.NotFoundException pointerMissing) {
                tableRepo.delete(tableId);
                return DeleteTableResponse.newBuilder()
                    .setMeta(tableRepo.metaForSafe(tableId))
                    .build();
              }

              long expectedVersion = meta.getPointerVersion();
              enforcePreconditions(correlationId, meta, request.getPrecondition());

              try {
                boolean ok = tableRepo.deleteWithPrecondition(tableId, expectedVersion);
                if (!ok) {
                  var nowMeta = tableRepo.metaForSafe(tableId);
                  throw GrpcErrors.preconditionFailed(
                      correlationId,
                      "version_mismatch",
                      Map.of(
                          "expected", Long.toString(expectedVersion),
                          "actual", Long.toString(nowMeta.getPointerVersion())));
                }
              } catch (BaseRepository.NotFoundException blobMissing) {
                throw GrpcErrors.notFound(correlationId, "table", Map.of("id", tableId.getId()));
              }

              return DeleteTableResponse.newBuilder().setMeta(meta).build();
            }),
        correlationId());
  }
}
