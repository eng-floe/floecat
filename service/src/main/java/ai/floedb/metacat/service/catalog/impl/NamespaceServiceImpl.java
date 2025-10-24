package ai.floedb.metacat.service.catalog.impl;

import ai.floedb.metacat.catalog.rpc.*;
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
import ai.floedb.metacat.service.storage.PointerStore;
import ai.floedb.metacat.service.storage.util.IdempotencyGuard;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;

import java.util.*;

@GrpcService
public class NamespaceServiceImpl extends BaseServiceImpl implements NamespaceService {

    @Inject
    NamespaceRepository nsRepo;
    @Inject
    CatalogRepository catalogRepo;
    @Inject
    TableRepository tableRepo;
    @Inject
    PrincipalProvider principal;
    @Inject
    Authorizer authz;
    @Inject
    PointerStore ptr;
    @Inject
    IdempotencyStore idempotencyStore;

    @Override
    public Uni<ListNamespacesResponse> listNamespaces(ListNamespacesRequest request) {
        return mapFailures(
                run(
                        () -> {
                            var principalContext = principal.get();

                            authz.require(principalContext, "namespace.read");

                            var catalogId = request.getCatalogId();
                            catalogRepo
                                    .getById(catalogId)
                                    .orElseThrow(
                                            () ->
                                                    GrpcErrors.notFound(
                                                            correlationId(), "catalog", Map.of("id", catalogId.getId())));

                            final int limit =
                                    (request.hasPage() && request.getPage().getPageSize() > 0)
                                            ? request.getPage().getPageSize()
                                            : 50;
                            final String token = request.hasPage() ? request.getPage().getPageToken() : "";
                            final StringBuilder next = new StringBuilder();

                            var namespaces = nsRepo.listByName(catalogId, null, Math.max(1, limit), token, next);
                            int total = nsRepo.count(catalogId);

                            var page =
                                    PageResponse.newBuilder()
                                            .setNextPageToken(next.toString())
                                            .setTotalSize(total)
                                            .build();

                            return ListNamespacesResponse.newBuilder()
                                    .addAllNamespaces(namespaces)
                                    .setPage(page)
                                    .build();
                        }),
                correlationId());
    }

    @Override
    public Uni<GetNamespaceResponse> getNamespace(GetNamespaceRequest request) {
        return mapFailures(
                run(
                        () -> {
                            var principalContext = principal.get();

                            authz.require(principalContext, "namespace.read");

                            var namespaceId = request.getNamespaceId();

                            var namespace =
                                    nsRepo
                                            .getById(namespaceId)
                                            .orElseThrow(
                                                    () ->
                                                            GrpcErrors.notFound(
                                                                    correlationId(), "namespace", Map.of("id", namespaceId.getId())));

                            return GetNamespaceResponse.newBuilder().setNamespace(namespace).build();
                        }),
                correlationId());
    }

    @Override
    public Uni<CreateNamespaceResponse> createNamespace(CreateNamespaceRequest request) {
        return mapFailures(
                runWithRetry(
                        () -> {
                            var principalContext = principal.get();
                            var correlationId = principalContext.getCorrelationId();

                            authz.require(principalContext, "namespace.write");

                            var tsNow = nowTs();

                            var tenantId = principalContext.getTenantId().getId();
                            var idempotencyKey =
                                    request.hasIdempotency() ? request.getIdempotency().getKey() : "";
                            byte[] fingerprint =
                                    request.getSpec().toBuilder().clearDescription().build().toByteArray();

                            var namespaceProto =
                                    MutationOps.createProto(
                                            tenantId,
                                            "CreateNamespace",
                                            idempotencyKey,
                                            () -> fingerprint,
                                            () -> {
                                                String namespaceUuid =
                                                        !idempotencyKey.isBlank()
                                                                ? deterministicUuid(tenantId, "namespace", idempotencyKey)
                                                                : UUID.randomUUID().toString();

                                                var namespaceId =
                                                        ResourceId.newBuilder()
                                                                .setTenantId(tenantId)
                                                                .setId(namespaceUuid)
                                                                .setKind(ResourceKind.RK_NAMESPACE)
                                                                .build();

                                                if (catalogRepo.getById(request.getSpec().getCatalogId()).isEmpty()) {
                                                    throw GrpcErrors.notFound(
                                                            correlationId,
                                                            "catalog",
                                                            Map.of("id", request.getSpec().getCatalogId().getId()));
                                                }

                                                var built =
                                                        Namespace.newBuilder()
                                                                .setResourceId(namespaceId)
                                                                .setDisplayName(
                                                                        mustNonEmpty(
                                                                                request.getSpec().getDisplayName(),
                                                                                "display_name",
                                                                                correlationId))
                                                                .addAllParents(request.getSpec().getPathList())
                                                                .setDescription(request.getSpec().getDescription())
                                                                .setCatalogId(request.getSpec().getCatalogId())
                                                                .setCreatedAt(tsNow)
                                                                .build();

                                                try {
                                                    nsRepo.create(built);
                                                } catch (BaseRepository.NameConflictException e) {
                                                    if (!idempotencyKey.isBlank()) {
                                                        var fullPath = new ArrayList<>(request.getSpec().getPathList());
                                                        fullPath.add(request.getSpec().getDisplayName());
                                                        var existingId =
                                                                nsRepo.getByPath(
                                                                        tenantId, request.getSpec().getCatalogId(), fullPath);
                                                        if (existingId.isPresent()) {
                                                            var existing = nsRepo.getById(existingId.get());
                                                            if (existing.isPresent()) {
                                                                return new IdempotencyGuard.CreateResult<>(
                                                                        existing.get(), existingId.get());
                                                            }
                                                        }
                                                    }
                                                    var pretty =
                                                            prettyNamespacePath(
                                                                    request.getSpec().getPathList(),
                                                                    request.getSpec().getDisplayName());

                                                    throw GrpcErrors.conflict(
                                                            correlationId,
                                                            "namespace.already_exists",
                                                            Map.of(
                                                                    "catalog",
                                                                    request.getSpec().getCatalogId().getId(),
                                                                    "path",
                                                                    pretty));
                                                }

                                                return new IdempotencyGuard.CreateResult<>(built, namespaceId);
                                            },
                                            (namespace) ->
                                                    nsRepo.metaForSafe(
                                                            request.getSpec().getCatalogId(), namespace.getResourceId()),
                                            idempotencyStore,
                                            tsNow,
                                            IDEMPOTENCY_TTL_SECONDS,
                                            this::correlationId,
                                            Namespace::parseFrom);

                            return CreateNamespaceResponse.newBuilder()
                                    .setNamespace(namespaceProto.body)
                                    .setMeta(namespaceProto.meta)
                                    .build();
                        }),
                correlationId());
    }

    @Override
    public Uni<RenameNamespaceResponse> renameNamespace(RenameNamespaceRequest request) {
        return mapFailures(
                runWithRetry(
                        () -> {
                            var principalContext = principal.get();
                            var correlationId = principalContext.getCorrelationId();

                            authz.require(principalContext, "namespace.write");

                            var namespaceId = request.getNamespaceId();
                            ensureKind(namespaceId, ResourceKind.RK_NAMESPACE, "namespace_id", correlationId);

                            var tenantId = principalContext.getTenantId().getId();

                            var currentNamespace =
                                    nsRepo
                                            .getById(namespaceId)
                                            .orElseThrow(
                                                    () ->
                                                            GrpcErrors.notFound(
                                                                    correlationId, "namespace", Map.of("id", namespaceId.getId())));
                            var currentCatalogId = currentNamespace.getCatalogId();
                            var pathProvided = !request.getNewPathList().isEmpty();
                            var newLeaf =
                                    pathProvided
                                            ? request.getNewPath(request.getNewPathCount() - 1)
                                            : (request.getNewDisplayName().isBlank()
                                            ? currentNamespace.getDisplayName()
                                            : request.getNewDisplayName());

                            var currentParents = currentNamespace.getParentsList();
                            var newParents =
                                    pathProvided
                                            ? List.copyOf(
                                            request.getNewPathList().subList(0, request.getNewPathCount() - 1))
                                            : currentParents;

                            var targetCatalogId =
                                    (request.hasNewCatalogId() && !request.getNewCatalogId().getId().isBlank())
                                            ? request.getNewCatalogId()
                                            : currentCatalogId;

                            if (catalogRepo.getById(targetCatalogId).isEmpty()) {
                                throw GrpcErrors.notFound(
                                        correlationId, "catalog", Map.of("id", targetCatalogId.getId()));
                            }

                            var sameCatalog = targetCatalogId.getId().equals(currentCatalogId.getId());
                            var sameLeaf = newLeaf.equals(currentNamespace.getDisplayName());
                            var sameParents = Objects.equals(currentParents, newParents);

                            var meta = nsRepo.metaFor(currentCatalogId, namespaceId);
                            long expectedVersion = meta.getPointerVersion();
                            enforcePreconditions(correlationId, meta, request.getPrecondition());

                            if (sameCatalog && sameLeaf && sameParents) {
                                var metaNoop = nsRepo.metaForSafe(currentCatalogId, namespaceId);
                                return RenameNamespaceResponse.newBuilder()
                                        .setNamespace(currentNamespace)
                                        .setMeta(metaNoop)
                                        .build();
                            }

                            var updated =
                                    currentNamespace.toBuilder()
                                            .setDisplayName(newLeaf)
                                            .clearParents()
                                            .addAllParents(newParents)
                                            .build();

                            try {
                                nsRepo.renameOrMove(
                                        updated,
                                        currentCatalogId,
                                        currentParents,
                                        currentNamespace.getDisplayName(),
                                        targetCatalogId,
                                        newParents,
                                        expectedVersion);
                            } catch (BaseRepository.NameConflictException e) {
                                var parts = new ArrayList<>(newParents);
                                parts.add(newLeaf);
                                var pretty = String.join("/", parts);
                                throw GrpcErrors.conflict(
                                        correlationId,
                                        "namespace.already_exists",
                                        Map.of("catalog", targetCatalogId.getId(), "path", pretty));
                            } catch (BaseRepository.PreconditionFailedException pfe) {
                                var nowMeta = nsRepo.metaForSafe(currentCatalogId, namespaceId);
                                throw GrpcErrors.preconditionFailed(
                                        correlationId,
                                        "version_mismatch",
                                        Map.of(
                                                "expected",
                                                Long.toString(expectedVersion),
                                                "actual",
                                                Long.toString(nowMeta.getPointerVersion())));
                            }

                            var outMeta = nsRepo.metaForSafe(targetCatalogId, namespaceId);
                            return RenameNamespaceResponse.newBuilder()
                                    .setNamespace(updated)
                                    .setMeta(outMeta)
                                    .build();
                        }),
                correlationId());
    }

    @Override
    public Uni<DeleteNamespaceResponse> deleteNamespace(DeleteNamespaceRequest request) {
        return mapFailures(
                runWithRetry(
                        () -> {
                            var principalContext = principal.get();
                            var correlationId = principalContext.getCorrelationId();

                            authz.require(principalContext, "namespace.write");

                            final var tenantId = principalContext.getTenantId().getId();
                            final var namespaceId = request.getNamespaceId();
                            ensureKind(namespaceId, ResourceKind.RK_NAMESPACE, "namespace_id", correlationId);

                            var namespace =
                                    nsRepo
                                            .getById(namespaceId)
                                            .orElseThrow(
                                                    () ->
                                                            GrpcErrors.notFound(
                                                                    correlationId, "namespace", Map.of("id", namespaceId.getId())));

                            var catalogId = namespace.getCatalogId();

                            if (catalogId == null) {
                                var safe =
                                        nsRepo.metaForSafe(
                                                ResourceId.newBuilder()
                                                        .setTenantId(tenantId)
                                                        .setId("_unknown_")
                                                        .setKind(ResourceKind.RK_CATALOG)
                                                        .build(),
                                                namespaceId);
                                return DeleteNamespaceResponse.newBuilder().setMeta(safe).build();
                            }

                            if (request.getRequireEmpty() && tableRepo.count(catalogId, namespaceId) > 0) {
                                var currentNamespace =
                                        nsRepo
                                                .getById(namespaceId)
                                                .orElseThrow(
                                                        () ->
                                                                GrpcErrors.notFound(
                                                                        correlationId, "namespace", Map.of("id", namespaceId.getId())));

                                String displayName =
                                        (currentNamespace != null)
                                                ? prettyNamespacePath(
                                                currentNamespace.getParentsList(), currentNamespace.getDisplayName())
                                                : namespaceId.getId();
                                throw GrpcErrors.conflict(
                                        correlationId, "namespace.not_empty", Map.of("display_name", displayName));
                            }

                            var meta = nsRepo.metaFor(catalogId, namespaceId);
                            long expectedVersion = meta.getPointerVersion();
                            enforcePreconditions(correlationId, meta, request.getPrecondition());

                            try {
                                nsRepo.deleteWithPrecondition(catalogId, namespaceId, expectedVersion);
                            } catch (BaseRepository.PreconditionFailedException pfe) {
                                var nowMeta = nsRepo.metaForSafe(catalogId, namespaceId);
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
                                        correlationId, "namespace", Map.of("id", namespaceId.getId()));
                            }

                            return DeleteNamespaceResponse.newBuilder().setMeta(meta).build();
                        }),
                correlationId());
    }
}
