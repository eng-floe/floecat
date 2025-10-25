package ai.floedb.metacat.service.catalog.impl;

import ai.floedb.metacat.catalog.rpc.*;
import ai.floedb.metacat.common.rpc.MutationMeta;
import ai.floedb.metacat.common.rpc.PageResponse;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.catalog.util.MutationOps;
import ai.floedb.metacat.service.common.BaseServiceImpl;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.repo.impl.*;
import ai.floedb.metacat.service.repo.util.BaseRepository;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;
import ai.floedb.metacat.service.storage.IdempotencyStore;
import ai.floedb.metacat.service.storage.util.IdempotencyGuard;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

@GrpcService
public class CatalogServiceImpl extends BaseServiceImpl implements CatalogService {

  @Inject CatalogRepository catalogRepo;
  @Inject NamespaceRepository namespaceRepo;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject IdempotencyStore idempotencyStore;

  @Override
  public Uni<ListCatalogsResponse> listCatalogs(ListCatalogsRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principal.get();

              authz.require(principalContext, "catalog.read");

              final int limit =
                  (request.hasPage() && request.getPage().getPageSize() > 0)
                      ? request.getPage().getPageSize()
                      : 50;
              final String token = request.hasPage() ? request.getPage().getPageToken() : "";
              final StringBuilder next = new StringBuilder();

              var catalogs =
                  catalogRepo.list(principalContext.getTenantId(), Math.max(1, limit), token, next);

              int total = catalogRepo.count(principalContext.getTenantId());

              var page =
                  PageResponse.newBuilder()
                      .setNextPageToken(next.toString())
                      .setTotalSize(total)
                      .build();

              return ListCatalogsResponse.newBuilder()
                  .addAllCatalogs(catalogs)
                  .setPage(page)
                  .build();
            }),
        correlationId());
  }

  @Override
  public Uni<GetCatalogResponse> getCatalog(GetCatalogRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principal.get();

              authz.require(principalContext, "catalog.read");

              return catalogRepo
                  .getById(request.getCatalogId())
                  .map(c -> GetCatalogResponse.newBuilder().setCatalog(c).build())
                  .orElseThrow(
                      () ->
                          GrpcErrors.notFound(
                              correlationId(),
                              "catalog",
                              Map.of("id", request.getCatalogId().getId())));
            }),
        correlationId());
  }

  @Override
  public Uni<CreateCatalogResponse> createCatalog(CreateCatalogRequest request) {
    return mapFailures(
        runWithRetry(
            () -> {
              final var principalContext = principal.get();
              final var correlationId = principalContext.getCorrelationId();
              final var tenantId = principalContext.getTenantId();

              authz.require(principalContext, "catalog.write");

              final var idempotencyKey =
                  request.hasIdempotency() ? request.getIdempotency().getKey() : "";

              final byte[] fingerprint =
                  request.getSpec().toBuilder().clearDescription().build().toByteArray();

              var tsNow = nowTs();

              var catalogProto =
                  MutationOps.createProto(
                      tenantId,
                      "CreateCatalog",
                      idempotencyKey,
                      () -> fingerprint,
                      () -> {
                        String catalogUuid =
                            !idempotencyKey.isBlank()
                                ? deterministicUuid(tenantId, "catalog", idempotencyKey)
                                : UUID.randomUUID().toString();

                        var catalogId =
                            ResourceId.newBuilder()
                                .setTenantId(tenantId)
                                .setId(catalogUuid)
                                .setKind(ResourceKind.RK_CATALOG)
                                .build();

                        var built =
                            Catalog.newBuilder()
                                .setResourceId(catalogId)
                                .setDisplayName(
                                    mustNonEmpty(
                                        request.getSpec().getDisplayName(),
                                        "display_name",
                                        correlationId))
                                .setDescription(request.getSpec().getDescription())
                                .setCreatedAt(tsNow)
                                .build();

                        try {
                          catalogRepo.create(built);
                        } catch (BaseRepository.NameConflictException e) {
                          if (!idempotencyKey.isBlank()) {
                            var existing = catalogRepo.getByName(tenantId, built.getDisplayName());
                            if (existing.isPresent()) {
                              return new IdempotencyGuard.CreateResult<>(
                                  existing.get(), existing.get().getResourceId());
                            }
                          }
                          throw GrpcErrors.conflict(
                              correlationId,
                              "catalog.already_exists",
                              Map.of("display_name", built.getDisplayName()));
                        }

                        return new IdempotencyGuard.CreateResult<>(built, catalogId);
                      },
                      (catalog) -> catalogRepo.metaForSafe(catalog.getResourceId()),
                      idempotencyStore,
                      tsNow,
                      IDEMPOTENCY_TTL_SECONDS,
                      this::correlationId,
                      Catalog::parseFrom);

              return CreateCatalogResponse.newBuilder()
                  .setCatalog(catalogProto.body)
                  .setMeta(catalogProto.meta)
                  .build();
            }),
        correlationId());
  }

  @Override
  public Uni<UpdateCatalogResponse> updateCatalog(UpdateCatalogRequest request) {
    return mapFailures(
        runWithRetry(
            () -> {
              var principalContext = principal.get();
              var correlationId = principalContext.getCorrelationId();

              authz.require(principalContext, "catalog.write");

              var catalogId = request.getCatalogId();
              ensureKind(catalogId, ResourceKind.RK_CATALOG, "catalog_id", correlationId);

              var current =
                  catalogRepo
                      .getById(catalogId)
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  correlationId, "catalog", Map.of("id", catalogId.getId())));

              var desiredName =
                  mustNonEmpty(request.getSpec().getDisplayName(), "display_name", correlationId);
              var desiredDescription = request.getSpec().getDescription();

              if (desiredName.equals(current.getDisplayName())
                  && Objects.equals(desiredDescription, current.getDescription())) {
                var metaNoop = catalogRepo.metaForSafe(catalogId);
                enforcePreconditions(correlationId, metaNoop, request.getPrecondition());
                return UpdateCatalogResponse.newBuilder()
                    .setCatalog(current)
                    .setMeta(metaNoop)
                    .build();
              }

              var updated =
                  current.toBuilder()
                      .setDisplayName(desiredName)
                      .setDescription(desiredDescription)
                      .build();

              var meta = catalogRepo.metaFor(catalogId);
              enforcePreconditions(correlationId, meta, request.getPrecondition());
              long expectedVersion = meta.getPointerVersion();

              final boolean ok;
              try {
                ok = catalogRepo.update(updated, expectedVersion);
              } catch (BaseRepository.NameConflictException nce) {
                throw GrpcErrors.conflict(
                    correlationId, "catalog.already_exists", Map.of("display_name", desiredName));
              }

              if (!ok) {
                var nowMeta = catalogRepo.metaForSafe(catalogId);
                throw GrpcErrors.preconditionFailed(
                    correlationId,
                    "version_mismatch",
                    Map.of(
                        "expected", Long.toString(expectedVersion),
                        "actual", Long.toString(nowMeta.getPointerVersion())));
              }

              return UpdateCatalogResponse.newBuilder()
                  .setCatalog(catalogRepo.getById(catalogId).orElse(updated))
                  .setMeta(catalogRepo.metaForSafe(catalogId))
                  .build();
            }),
        correlationId());
  }

  @Override
  public Uni<DeleteCatalogResponse> deleteCatalog(DeleteCatalogRequest request) {
    return mapFailures(
        runWithRetry(
            () -> {
              var principalContext = principal.get();
              var correlationId = principalContext.getCorrelationId();

              authz.require(principalContext, "catalog.write");

              var catalogId = request.getCatalogId();
              ensureKind(catalogId, ResourceKind.RK_CATALOG, "catalog_id", correlationId);

              if (request.getRequireEmpty()
                  && namespaceRepo.count(catalogId.getTenantId(), catalogId.getId(), List.of())
                      > 0) {
                var current = catalogRepo.getById(catalogId).orElse(null);
                var displayName =
                    (current != null && !current.getDisplayName().isBlank())
                        ? current.getDisplayName()
                        : catalogId.getId();
                throw GrpcErrors.conflict(
                    correlationId, "catalog.not_empty", Map.of("display_name", displayName));
              }

              MutationMeta meta;
              try {
                meta = catalogRepo.metaFor(catalogId);
              } catch (BaseRepository.NotFoundException e) {
                catalogRepo.delete(catalogId);
                return DeleteCatalogResponse.newBuilder()
                    .setMeta(catalogRepo.metaForSafe(catalogId))
                    .build();
              }

              enforcePreconditions(correlationId, meta, request.getPrecondition());
              long expectedVersion = meta.getPointerVersion();

              try {
                boolean ok = catalogRepo.deleteWithPrecondition(catalogId, expectedVersion);
                if (!ok) {
                  var nowMeta = catalogRepo.metaForSafe(catalogId);
                  throw GrpcErrors.preconditionFailed(
                      correlationId,
                      "version_mismatch",
                      Map.of(
                          "expected", Long.toString(expectedVersion),
                          "actual", Long.toString(nowMeta.getPointerVersion())));
                }
              } catch (BaseRepository.NotFoundException nfe) {
                throw GrpcErrors.notFound(
                    correlationId, "catalog", Map.of("id", catalogId.getId()));
              }

              return DeleteCatalogResponse.newBuilder().setMeta(meta).build();
            }),
        correlationId());
  }
}
