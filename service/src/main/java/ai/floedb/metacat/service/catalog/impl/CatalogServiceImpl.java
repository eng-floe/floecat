package ai.floedb.metacat.service.catalog.impl;

import ai.floedb.metacat.catalog.rpc.*;
import ai.floedb.metacat.common.rpc.PageResponse;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.catalog.util.MutationOps;
import ai.floedb.metacat.service.common.BaseServiceImpl;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.repo.impl.*;
import ai.floedb.metacat.service.repo.util.BaseRepository;
import ai.floedb.metacat.service.repo.util.Keys;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;
import ai.floedb.metacat.service.storage.IdempotencyStore;
import ai.floedb.metacat.service.storage.PointerStore;
import ai.floedb.metacat.service.storage.util.IdempotencyGuard;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;

@GrpcService
public class CatalogServiceImpl extends BaseServiceImpl implements CatalogService {

    @Inject
    NamespaceRepository nsRepo;
    @Inject
    CatalogRepository catalogRepo;
    @Inject
    PrincipalProvider principal;
    @Inject
    Authorizer authz;
    @Inject
    PointerStore ptr;
    @Inject
    IdempotencyStore idempotencyStore;

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
                                    catalogRepo.listByName(
                                            principalContext.getTenantId().getId(), Math.max(1, limit), token, next);

                            int total = catalogRepo.count(principalContext.getTenantId().getId());

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
                            final var tenantId = principalContext.getTenantId().getId();

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

                            var tsNow = nowTs();

                            var catalogId = request.getCatalogId();
                            ensureKind(catalogId, ResourceKind.RK_CATALOG, "catalog_id", correlationId);

                            var prev =
                                    catalogRepo
                                            .getById(catalogId)
                                            .orElseThrow(
                                                    () ->
                                                            GrpcErrors.notFound(
                                                                    correlationId, "catalog", Map.of("id", catalogId.getId())));

                            var desiredName =
                                    mustNonEmpty(request.getSpec().getDisplayName(), "display_name", correlationId);
                            var desiredDescription = request.getSpec().getDescription();

                            var meta = catalogRepo.metaFor(catalogId);
                            long expectedVersion = meta.getPointerVersion();
                            enforcePreconditions(correlationId, meta, request.getPrecondition());

                            if (desiredName.equals(prev.getDisplayName())
                                    && Objects.equals(desiredDescription, prev.getDescription())) {
                                return UpdateCatalogResponse.newBuilder()
                                        .setCatalog(prev)
                                        .setMeta(catalogRepo.metaForSafe(catalogId))
                                        .build();
                            }

                            if (!desiredName.equals(prev.getDisplayName())) {
                                try {
                                    catalogRepo.rename(
                                            principalContext.getTenantId().getId(),
                                            catalogId,
                                            desiredName,
                                            expectedVersion);
                                } catch (BaseRepository.NameConflictException nce) {
                                    throw GrpcErrors.conflict(
                                            correlationId, "catalog.already_exists", Map.of("display_name", desiredName));
                                } catch (BaseRepository.PreconditionFailedException pfe) {
                                    var nowMeta = catalogRepo.metaForSafe(catalogId);
                                    throw GrpcErrors.preconditionFailed(
                                            correlationId,
                                            "version_mismatch",
                                            Map.of(
                                                    "expected",
                                                    Long.toString(expectedVersion),
                                                    "actual",
                                                    Long.toString(nowMeta.getPointerVersion())));
                                }

                                if (!Objects.equals(desiredDescription, prev.getDescription())) {
                                    meta = catalogRepo.metaFor(catalogId);
                                    expectedVersion = meta.getPointerVersion();
                                    var renamedObj = catalogRepo.getById(catalogId).orElse(prev);
                                    var withDesc = renamedObj.toBuilder().setDescription(desiredDescription).build();

                                    try {
                                        catalogRepo.update(withDesc, meta.getPointerVersion());
                                    } catch (BaseRepository.PreconditionFailedException pfe) {
                                        var nowMeta = catalogRepo.metaForSafe(catalogId);
                                        throw GrpcErrors.preconditionFailed(
                                                correlationId,
                                                "version_mismatch",
                                                Map.of(
                                                        "expected",
                                                        Long.toString(expectedVersion),
                                                        "actual",
                                                        Long.toString(nowMeta.getPointerVersion())));
                                    }
                                }
                            } else {
                                meta = catalogRepo.metaFor(catalogId);
                                expectedVersion = meta.getPointerVersion();
                                var updated = prev.toBuilder().setDescription(desiredDescription).build();

                                try {
                                    catalogRepo.update(updated, meta.getPointerVersion());
                                } catch (BaseRepository.PreconditionFailedException pfe) {
                                    var nowMeta = catalogRepo.metaForSafe(catalogId);
                                    throw GrpcErrors.preconditionFailed(
                                            correlationId,
                                            "version_mismatch",
                                            Map.of(
                                                    "expected",
                                                    Long.toString(expectedVersion),
                                                    "actual",
                                                    Long.toString(nowMeta.getPointerVersion())));
                                }
                            }

                            return UpdateCatalogResponse.newBuilder()
                                    .setCatalog(catalogRepo.getById(catalogId).orElse(prev))
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

                            var key = Keys.catPtr(catalogId.getTenantId(), catalogId.getId());
                            if (ptr.get(key).isEmpty()) {
                                catalogRepo.delete(catalogId);
                                var safe = catalogRepo.metaForSafe(catalogId);
                                return DeleteCatalogResponse.newBuilder().setMeta(safe).build();
                            }

                            if (request.getRequireEmpty() && nsRepo.count(catalogId) > 0) {
                                var currentNamespace = catalogRepo.getById(catalogId).orElse(null);
                                var displayName =
                                        (currentNamespace != null && !currentNamespace.getDisplayName().isBlank())
                                                ? currentNamespace.getDisplayName()
                                                : catalogId.getId();
                                throw GrpcErrors.conflict(
                                        correlationId, "catalog.not_empty", Map.of("display_name", displayName));
                            }

                            var meta = catalogRepo.metaFor(catalogId);
                            long expectedVersion = meta.getPointerVersion();
                            enforcePreconditions(correlationId, meta, request.getPrecondition());

                            try {
                                catalogRepo.deleteWithPrecondition(catalogId, expectedVersion);
                            } catch (BaseRepository.PreconditionFailedException pfe) {
                                var nowMeta = catalogRepo.metaForSafe(catalogId);
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
                                        correlationId, "catalog", Map.of("id", catalogId.getId()));
                            }

                            return DeleteCatalogResponse.newBuilder().setMeta(meta).build();
                        }),
                correlationId());
    }
}
