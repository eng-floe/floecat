package ai.floedb.metacat.service.catalog.impl;

import ai.floedb.metacat.catalog.rpc.CreateNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.CreateNamespaceResponse;
import ai.floedb.metacat.catalog.rpc.DeleteNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.DeleteNamespaceResponse;
import ai.floedb.metacat.catalog.rpc.GetNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.GetNamespaceResponse;
import ai.floedb.metacat.catalog.rpc.ListNamespacesRequest;
import ai.floedb.metacat.catalog.rpc.ListNamespacesResponse;
import ai.floedb.metacat.catalog.rpc.Namespace;
import ai.floedb.metacat.catalog.rpc.NamespaceService;
import ai.floedb.metacat.catalog.rpc.NamespaceSpec;
import ai.floedb.metacat.catalog.rpc.UpdateNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.UpdateNamespaceResponse;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.common.BaseServiceImpl;
import ai.floedb.metacat.service.common.Canonicalizer;
import ai.floedb.metacat.service.common.IdempotencyGuard;
import ai.floedb.metacat.service.common.MutationOps;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.repo.IdempotencyRepository;
import ai.floedb.metacat.service.repo.impl.CatalogRepository;
import ai.floedb.metacat.service.repo.impl.NamespaceRepository;
import ai.floedb.metacat.service.repo.impl.TableRepository;
import ai.floedb.metacat.service.repo.util.BaseResourceRepository;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@GrpcService
public class NamespaceServiceImpl extends BaseServiceImpl implements NamespaceService {

  @Inject CatalogRepository catalogRepo;
  @Inject NamespaceRepository namespaceRepo;
  @Inject TableRepository tableRepo;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject IdempotencyRepository idempotencyStore;

  @Override
  public Uni<ListNamespacesResponse> listNamespaces(ListNamespacesRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principal.get();
              authz.require(principalContext, "namespace.read");

              var pageIn = MutationOps.pageIn(request.hasPage() ? request.getPage() : null);
              var next = new StringBuilder();

              var namespaces =
                  namespaceRepo.list(
                      principalContext.getTenantId(),
                      request.getCatalogId().getId(),
                      List.of(),
                      Math.max(1, pageIn.limit),
                      pageIn.token,
                      next);

              var page =
                  MutationOps.pageOut(
                      next.toString(), catalogRepo.count(principalContext.getTenantId()));

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
              var tenantId = principalContext.getTenantId();
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

              var fingerprint = canonicalFingerprint(request.getSpec());
              var idempotencyKey =
                  request.hasIdempotency() && !request.getIdempotency().getKey().isBlank()
                      ? request.getIdempotency().getKey()
                      : hashFingerprint(fingerprint);

              var namespaceProto =
                  MutationOps.createProto(
                      tenantId,
                      "CreateNamespace",
                      idempotencyKey,
                      () -> fingerprint,
                      () -> {
                        String namespaceUuid =
                            deterministicUuid(tenantId, "namespace", idempotencyKey);

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
                        } catch (BaseResourceRepository.NameConflictException e) {
                          var fullPath = new ArrayList<>(request.getSpec().getPathList());
                          fullPath.add(request.getSpec().getDisplayName());
                          var existing =
                              namespaceRepo.getByPath(
                                  tenantId, request.getSpec().getCatalogId().getId(), fullPath);
                          if (existing.isPresent()) {
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

                          throw new BaseResourceRepository.AbortRetryableException(
                              "name conflict visibility window");
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
  public Uni<UpdateNamespaceResponse> updateNamespace(UpdateNamespaceRequest request) {
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

              ResourceId desiredCatalogId =
                  request.hasCatalogId() && !request.getCatalogId().getId().isBlank()
                      ? request.getCatalogId()
                      : current.getCatalogId();
              ensureKind(desiredCatalogId, ResourceKind.RK_CATALOG, "catalog_id", correlationId);

              catalogRepo
                  .getById(desiredCatalogId)
                  .orElseThrow(
                      () ->
                          GrpcErrors.notFound(
                              correlationId, "catalog", Map.of("id", desiredCatalogId.getId())));

              String desiredDisplay;
              List<String> desiredParents;

              var reqDisplay = request.getDisplayName();
              var reqPath = request.getPathList();

              if (!reqDisplay.isBlank()) {
                desiredDisplay = mustNonEmpty(reqDisplay, "display_name", correlationId);
                desiredParents = !reqPath.isEmpty() ? reqPath : current.getParentsList();
              } else if (!reqPath.isEmpty()) {
                if (reqPath.size() == 1) {
                  desiredDisplay = mustNonEmpty(reqPath.get(0), "path[0]", correlationId);
                  desiredParents = List.of();
                } else {
                  desiredDisplay =
                      mustNonEmpty(reqPath.get(reqPath.size() - 1), "path[last]", correlationId);
                  desiredParents = reqPath.subList(0, reqPath.size() - 1);
                }
              } else {
                desiredDisplay = current.getDisplayName();
                desiredParents = current.getParentsList();
              }

              var desired =
                  current.toBuilder()
                      .setDisplayName(desiredDisplay)
                      .clearParents()
                      .addAllParents(desiredParents)
                      .setCatalogId(desiredCatalogId)
                      .build();

              if (desired.equals(current)) {
                var metaNoop = namespaceRepo.metaForSafe(namespaceId);
                MutationOps.BaseServiceChecks.enforcePreconditions(
                    correlationId, metaNoop, request.getPrecondition());
                return UpdateNamespaceResponse.newBuilder()
                    .setNamespace(current)
                    .setMeta(metaNoop)
                    .build();
              }

              MutationOps.updateWithPreconditions(
                  () -> namespaceRepo.metaFor(namespaceId),
                  request.getPrecondition(),
                  expected -> namespaceRepo.update(desired, expected),
                  () -> namespaceRepo.metaForSafe(namespaceId),
                  correlationId,
                  "namespace",
                  Map.of("display_name", desiredDisplay, "catalog_id", desiredCatalogId.getId()));

              var outMeta = namespaceRepo.metaForSafe(namespaceId);
              var latest = namespaceRepo.getById(namespaceId).orElse(desired);
              return UpdateNamespaceResponse.newBuilder()
                  .setNamespace(latest)
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

              var namespaceId = request.getNamespaceId();
              ensureKind(namespaceId, ResourceKind.RK_NAMESPACE, "namespace_id", correlationId);

              var namespace = namespaceRepo.getById(namespaceId).orElse(null);
              var catalogId =
                  (namespace != null && namespace.hasCatalogId()) ? namespace.getCatalogId() : null;
              if (catalogId == null) {
                var safe = namespaceRepo.metaForSafe(namespaceId);
                namespaceRepo.delete(namespaceId);
                return DeleteNamespaceResponse.newBuilder().setMeta(safe).build();
              }

              if (tableRepo.count(catalogId.getTenantId(), catalogId.getId(), namespaceId.getId())
                  > 0) {
                var pretty =
                    prettyNamespacePath(namespace.getParentsList(), namespace.getDisplayName());
                throw GrpcErrors.conflict(
                    correlationId, "namespace.not_empty", Map.of("display_name", pretty));
              }

              var meta =
                  MutationOps.deleteWithPreconditions(
                      () -> namespaceRepo.metaFor(namespaceId),
                      request.getPrecondition(),
                      expected -> namespaceRepo.deleteWithPrecondition(namespaceId, expected),
                      () -> namespaceRepo.metaForSafe(namespaceId),
                      correlationId,
                      "namespace",
                      Map.of("id", namespaceId.getId()));

              return DeleteNamespaceResponse.newBuilder().setMeta(meta).build();
            }),
        correlationId());
  }

  private static byte[] canonicalFingerprint(NamespaceSpec s) {
    return new Canonicalizer()
        .scalar("cat", s.getCatalogId().getId())
        .scalar("name", s.getDisplayName())
        .scalar("description", s.getDescription())
        .scalar("policy", s.getPolicyRef())
        .list("parents", s.getPathList())
        .map("annotations", s.getAnnotationsMap())
        .scalar("policy", s.getPolicyRef())
        .bytes();
  }
}
