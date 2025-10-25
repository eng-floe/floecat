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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

@GrpcService
public class NamespaceServiceImpl extends BaseServiceImpl implements NamespaceService {

  @Inject CatalogRepository catalogRepo;
  @Inject NamespaceRepository namespaceRepo;
  @Inject TableRepository tableRepo;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject IdempotencyStore idempotencyStore;

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

              var namespaces =
                  namespaceRepo.list(
                      principalContext.getTenantId(),
                      catalogId.getId(),
                      List.of(),
                      Math.max(1, limit),
                      token,
                      next);
              int total =
                  namespaceRepo.count(principalContext.getTenantId(), catalogId.getId(), List.of());

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
                  namespaceRepo
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

              catalogRepo
                  .getById(request.getSpec().getCatalogId())
                  .orElseThrow(
                      () ->
                          GrpcErrors.notFound(
                              correlationId,
                              "catalog",
                              Map.of("id", request.getSpec().getCatalogId().getId())));

              var tsNow = nowTs();

              var tenantId = principalContext.getTenantId();
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
                          namespaceRepo.create(built);
                        } catch (BaseRepository.NameConflictException e) {
                          if (!idempotencyKey.isBlank()) {
                            var fullPath = new ArrayList<>(request.getSpec().getPathList());
                            fullPath.add(request.getSpec().getDisplayName());
                            var existing =
                                namespaceRepo.getByPath(
                                    tenantId, request.getSpec().getCatalogId().getId(), fullPath);
                            if (existing.isPresent()) {
                              return new IdempotencyGuard.CreateResult<>(
                                  existing.get(), existing.get().getResourceId());
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
                      (namespace) -> namespaceRepo.metaForSafe(namespace.getResourceId()),
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

              var current =
                  namespaceRepo
                      .getById(namespaceId)
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  correlationId, "namespace", Map.of("id", namespaceId.getId())));

              var currentCatalogId = current.getCatalogId();

              boolean pathProvided = !request.getNewPathList().isEmpty();
              String newLeaf =
                  pathProvided
                      ? request.getNewPath(request.getNewPathCount() - 1)
                      : (request.getNewDisplayName().isBlank()
                          ? current.getDisplayName()
                          : request.getNewDisplayName());

              var currentParents = current.getParentsList();
              List<String> newParents =
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

              boolean sameCatalog = targetCatalogId.getId().equals(currentCatalogId.getId());
              boolean sameLeaf = newLeaf.equals(current.getDisplayName());
              boolean sameParents = Objects.equals(currentParents, newParents);
              if (sameCatalog && sameLeaf && sameParents) {
                return RenameNamespaceResponse.newBuilder()
                    .setNamespace(current)
                    .setMeta(namespaceRepo.metaForSafe(namespaceId))
                    .build();
              }

              var updated =
                  current.toBuilder()
                      .setCatalogId(targetCatalogId)
                      .setDisplayName(newLeaf)
                      .clearParents()
                      .addAllParents(newParents)
                      .build();

              var meta = namespaceRepo.metaFor(namespaceId);
              enforcePreconditions(correlationId, meta, request.getPrecondition());
              long expectedVersion = meta.getPointerVersion();

              final boolean ok;
              try {
                ok = namespaceRepo.update(updated, expectedVersion);
              } catch (BaseRepository.NameConflictException nce) {
                var parts = new ArrayList<>(newParents);
                parts.add(newLeaf);
                var pretty = String.join("/", parts);
                throw GrpcErrors.conflict(
                    correlationId,
                    "namespace.already_exists",
                    Map.of("catalog", targetCatalogId.getId(), "path", pretty));
              }

              if (!ok) {
                var nowMeta = namespaceRepo.metaForSafe(namespaceId);
                throw GrpcErrors.preconditionFailed(
                    correlationId,
                    "version_mismatch",
                    Map.of(
                        "expected", Long.toString(expectedVersion),
                        "actual", Long.toString(nowMeta.getPointerVersion())));
              }

              return RenameNamespaceResponse.newBuilder()
                  .setNamespace(namespaceRepo.getById(namespaceId).orElse(updated))
                  .setMeta(namespaceRepo.metaForSafe(namespaceId))
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

              final var namespaceId = request.getNamespaceId();
              ensureKind(namespaceId, ResourceKind.RK_NAMESPACE, "namespace_id", correlationId);

              MutationMeta meta;
              try {
                meta = namespaceRepo.metaFor(namespaceId);
              } catch (BaseRepository.NotFoundException e) {
                namespaceRepo.delete(namespaceId);
                return DeleteNamespaceResponse.newBuilder()
                    .setMeta(namespaceRepo.metaForSafe(namespaceId))
                    .build();
              }

              var namespace = namespaceRepo.getById(namespaceId).orElse(null);
              var catalogId =
                  (namespace != null && namespace.hasCatalogId()) ? namespace.getCatalogId() : null;

              if (catalogId == null) {
                var safe = namespaceRepo.metaForSafe(namespaceId);
                namespaceRepo.delete(namespaceId);
                return DeleteNamespaceResponse.newBuilder().setMeta(safe).build();
              }

              if (request.getRequireEmpty()
                  && tableRepo.count(
                          catalogId.getTenantId(), catalogId.getId(), namespaceId.getId())
                      > 0) {
                var pretty =
                    prettyNamespacePath(namespace.getParentsList(), namespace.getDisplayName());
                throw GrpcErrors.conflict(
                    correlationId, "namespace.not_empty", Map.of("display_name", pretty));
              }

              enforcePreconditions(correlationId, meta, request.getPrecondition());
              long expectedVersion = meta.getPointerVersion();

              try {
                boolean ok = namespaceRepo.deleteWithPrecondition(namespaceId, expectedVersion);
                if (!ok) {
                  var nowMeta = namespaceRepo.metaForSafe(namespaceId);
                  throw GrpcErrors.preconditionFailed(
                      correlationId,
                      "version_mismatch",
                      Map.of(
                          "expected", Long.toString(expectedVersion),
                          "actual", Long.toString(nowMeta.getPointerVersion())));
                }
              } catch (BaseRepository.NotFoundException nfe) {
                throw GrpcErrors.notFound(
                    correlationId, "namespace", Map.of("id", namespaceId.getId()));
              }

              return DeleteNamespaceResponse.newBuilder().setMeta(meta).build();
            }),
        correlationId());
  }
}
