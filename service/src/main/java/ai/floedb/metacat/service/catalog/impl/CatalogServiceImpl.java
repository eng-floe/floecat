package ai.floedb.metacat.service.catalog.impl;

import ai.floedb.metacat.catalog.rpc.Catalog;
import ai.floedb.metacat.catalog.rpc.CatalogService;
import ai.floedb.metacat.catalog.rpc.CatalogSpec;
import ai.floedb.metacat.catalog.rpc.CreateCatalogRequest;
import ai.floedb.metacat.catalog.rpc.CreateCatalogResponse;
import ai.floedb.metacat.catalog.rpc.DeleteCatalogRequest;
import ai.floedb.metacat.catalog.rpc.DeleteCatalogResponse;
import ai.floedb.metacat.catalog.rpc.GetCatalogRequest;
import ai.floedb.metacat.catalog.rpc.GetCatalogResponse;
import ai.floedb.metacat.catalog.rpc.ListCatalogsRequest;
import ai.floedb.metacat.catalog.rpc.ListCatalogsResponse;
import ai.floedb.metacat.catalog.rpc.UpdateCatalogRequest;
import ai.floedb.metacat.catalog.rpc.UpdateCatalogResponse;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.common.BaseServiceImpl;
import ai.floedb.metacat.service.common.Canonicalizer;
import ai.floedb.metacat.service.common.IdempotencyGuard;
import ai.floedb.metacat.service.common.LogHelper;
import ai.floedb.metacat.service.common.MutationOps;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.repo.IdempotencyRepository;
import ai.floedb.metacat.service.repo.impl.CatalogRepository;
import ai.floedb.metacat.service.repo.impl.NamespaceRepository;
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
public class CatalogServiceImpl extends BaseServiceImpl implements CatalogService {

  @Inject CatalogRepository catalogRepo;
  @Inject NamespaceRepository namespaceRepo;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject IdempotencyRepository idempotencyStore;

  private static final Logger LOG = Logger.getLogger(CatalogService.class);

  @Override
  public Uni<ListCatalogsResponse> listCatalogs(ListCatalogsRequest request) {
    var L = LogHelper.start(LOG, "ListCatalogs");

    return mapFailures(
            run(
                () -> {
                  var principalContext = principal.get();
                  authz.require(principalContext, "catalog.read");

                  var pageIn = MutationOps.pageIn(request.hasPage() ? request.getPage() : null);
                  var next = new StringBuilder();

                  var catalogs =
                      catalogRepo.list(
                          principalContext.getTenantId(),
                          Math.max(1, pageIn.limit),
                          pageIn.token,
                          next);

                  var page =
                      MutationOps.pageOut(
                          next.toString(), catalogRepo.count(principalContext.getTenantId()));

                  return ListCatalogsResponse.newBuilder()
                      .addAllCatalogs(catalogs)
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
  public Uni<GetCatalogResponse> getCatalog(GetCatalogRequest request) {
    var L = LogHelper.start(LOG, "GetCatalog");

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
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<CreateCatalogResponse> createCatalog(CreateCatalogRequest request) {
    var L = LogHelper.start(LOG, "CreateCatalog");

    return mapFailures(
            runWithRetry(
                () -> {
                  var principalContext = principal.get();
                  var correlationId = principalContext.getCorrelationId();
                  var tenantId = principalContext.getTenantId();

                  authz.require(principalContext, "catalog.write");

                  var fingerprint = canonicalFingerprint(request.getSpec());
                  var idempotencyKey =
                      request.hasIdempotency() && !request.getIdempotency().getKey().isBlank()
                          ? request.getIdempotency().getKey()
                          : hashFingerprint(fingerprint);

                  var tsNow = nowTs();

                  var catalogProto =
                      MutationOps.createProto(
                          tenantId,
                          "CreateCatalog",
                          idempotencyKey,
                          () -> fingerprint,
                          () -> {
                            String catalogUuid =
                                deterministicUuid(tenantId, "catalog", idempotencyKey);

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
                            } catch (BaseResourceRepository.NameConflictException e) {
                              var existing =
                                  catalogRepo.getByName(tenantId, built.getDisplayName());
                              if (existing.isPresent()) {
                                throw GrpcErrors.conflict(
                                    correlationId,
                                    "catalog.already_exists",
                                    Map.of("display_name", built.getDisplayName()));
                              }

                              throw new BaseResourceRepository.AbortRetryableException(
                                  "name conflict visibility window");
                            }

                            return new IdempotencyGuard.CreateResult<>(built, catalogId);
                          },
                          (catalog) -> catalogRepo.metaForSafe(catalog.getResourceId()),
                          idempotencyStore,
                          tsNow,
                          idempotencyTtlSeconds(),
                          this::correlationId,
                          Catalog::parseFrom);

                  return CreateCatalogResponse.newBuilder()
                      .setCatalog(catalogProto.body)
                      .setMeta(catalogProto.meta)
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<UpdateCatalogResponse> updateCatalog(UpdateCatalogRequest request) {
    var L = LogHelper.start(LOG, "UpdateCatalog");

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
                      mustNonEmpty(
                          request.getSpec().getDisplayName(), "display_name", correlationId);
                  var desiredDesc = request.getSpec().getDescription();

                  var updated =
                      current.toBuilder()
                          .setDisplayName(desiredName)
                          .setDescription(desiredDesc)
                          .build();

                  if (updated.equals(current)) {
                    var metaNoop = catalogRepo.metaForSafe(catalogId);
                    MutationOps.BaseServiceChecks.enforcePreconditions(
                        correlationId, metaNoop, request.getPrecondition());
                    return UpdateCatalogResponse.newBuilder()
                        .setCatalog(current)
                        .setMeta(metaNoop)
                        .build();
                  }

                  MutationOps.updateWithPreconditions(
                      () -> catalogRepo.metaFor(catalogId),
                      request.getPrecondition(),
                      expected -> catalogRepo.update(updated, expected),
                      () -> catalogRepo.metaForSafe(catalogId),
                      correlationId,
                      "catalog",
                      Map.of("display_name", desiredName));

                  var outMeta = catalogRepo.metaForSafe(catalogId);
                  var latest = catalogRepo.getById(catalogId).orElse(updated);
                  return UpdateCatalogResponse.newBuilder()
                      .setCatalog(latest)
                      .setMeta(outMeta)
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<DeleteCatalogResponse> deleteCatalog(DeleteCatalogRequest request) {
    var L = LogHelper.start(LOG, "DeleteCatalog");

    return mapFailures(
            runWithRetry(
                () -> {
                  var principalContext = principal.get();
                  var correlationId = principalContext.getCorrelationId();
                  authz.require(principalContext, "catalog.write");
                  var id = request.getCatalogId();
                  ensureKind(id, ResourceKind.RK_CATALOG, "catalog_id", correlationId);

                  if (namespaceRepo.count(id.getTenantId(), id.getId(), List.of()) > 0) {
                    var currentCatalog = catalogRepo.getById(id).orElse(null);
                    var displayName =
                        (currentCatalog != null && !currentCatalog.getDisplayName().isBlank())
                            ? currentCatalog.getDisplayName()
                            : id.getId();
                    throw GrpcErrors.conflict(
                        correlationId, "catalog.not_empty", Map.of("display_name", displayName));
                  }

                  var meta =
                      MutationOps.deleteWithPreconditions(
                          () -> catalogRepo.metaFor(id),
                          request.getPrecondition(),
                          expected -> catalogRepo.deleteWithPrecondition(id, expected),
                          () -> catalogRepo.metaForSafe(id),
                          correlationId,
                          "catalog",
                          Map.of("id", id.getId()));

                  return DeleteCatalogResponse.newBuilder().setMeta(meta).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  private static byte[] canonicalFingerprint(CatalogSpec s) {
    return new Canonicalizer()
        .scalar("name", s.getDisplayName())
        .scalar("description", s.getDescription())
        .scalar("policy", s.getPolicyRef())
        .scalar("connectorRef", s.getConnectorRef())
        .map("opt", s.getOptionsMap())
        .bytes();
  }
}
