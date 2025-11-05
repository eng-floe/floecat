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
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;
import com.google.protobuf.FieldMask;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jboss.logging.Logger;

@GrpcService
public class CatalogServiceImpl extends BaseServiceImpl implements CatalogService {

  @Inject CatalogRepository catalogRepo;
  @Inject NamespaceRepository namespaceRepo;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject IdempotencyRepository idempotencyStore;

  private static final Set<String> CATALOG_MUTABLE_PATHS =
      Set.of("display_name", "description", "connector_ref", "options", "policy_ref");

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
                            catalogRepo.create(built);
                            return new IdempotencyGuard.CreateResult<>(built, catalogId);
                          },
                          (catalog) -> catalogRepo.metaForSafe(catalog.getResourceId()),
                          idempotencyStore,
                          tsNow,
                          idempotencyTtlSeconds(),
                          this::correlationId,
                          Catalog::parseFrom,
                          rec -> catalogRepo.getById(rec.getResourceId()).isPresent());

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
                  var pctx = principal.get();
                  var corr = pctx.getCorrelationId();
                  authz.require(pctx, "catalog.write");

                  var catalogId = request.getCatalogId();
                  ensureKind(catalogId, ResourceKind.RK_CATALOG, "catalog_id", corr);

                  var current =
                      catalogRepo
                          .getById(catalogId)
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(
                                      corr, "catalog", Map.of("id", catalogId.getId())));

                  if (!request.hasUpdateMask() || request.getUpdateMask().getPathsCount() == 0) {
                    throw GrpcErrors.invalidArgument(corr, "update_mask.required", Map.of());
                  }

                  var spec = request.getSpec();
                  var mask = request.getUpdateMask();

                  var desired = applyCatalogSpecPatch(current, spec, mask, corr);

                  if (desired.equals(current)) {
                    var metaNoop = catalogRepo.metaForSafe(catalogId);
                    MutationOps.BaseServiceChecks.enforcePreconditions(
                        corr, metaNoop, request.getPrecondition());
                    return UpdateCatalogResponse.newBuilder()
                        .setCatalog(current)
                        .setMeta(metaNoop)
                        .build();
                  }

                  MutationOps.updateWithPreconditions(
                      () -> catalogRepo.metaFor(catalogId),
                      request.getPrecondition(),
                      expectedVersion -> catalogRepo.update(desired, expectedVersion),
                      () -> catalogRepo.metaForSafe(catalogId),
                      corr,
                      "catalog",
                      Map.of("display_name", desired.getDisplayName()));

                  var outMeta = catalogRepo.metaForSafe(catalogId);
                  var latest = catalogRepo.getById(catalogId).orElse(desired);

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

  private Catalog applyCatalogSpecPatch(
      Catalog current, CatalogSpec spec, FieldMask mask, String corr) {

    var paths = normalizedMaskPaths(mask);
    if (paths.isEmpty()) {
      throw GrpcErrors.invalidArgument(corr, "update_mask.required", Map.of());
    }

    for (var p : paths) {
      if (!CATALOG_MUTABLE_PATHS.contains(p)) {
        throw GrpcErrors.invalidArgument(corr, "update_mask.path.invalid", Map.of("path", p));
      }
    }

    var b = current.toBuilder();

    if (maskTargets(mask, "display_name")) {
      var name = spec.getDisplayName();
      if (name == null || name.isBlank()) {
        throw GrpcErrors.invalidArgument(corr, "display_name.required", Map.of());
      }
      b.setDisplayName(name);
    }

    if (maskTargets(mask, "description")) {
      if (spec.hasDescription()) {
        b.setDescription(spec.getDescription());
      } else {
        b.clearDescription();
      }
    }

    if (maskTargets(mask, "connector_ref")) {
      if (spec.hasConnectorRef()) {
        b.setConnectorRef(spec.getConnectorRef());
      } else {
        b.clearConnectorRef();
      }
    }

    if (maskTargets(mask, "policy_ref")) {
      if (spec.hasPolicyRef()) {
        b.setPolicyRef(spec.getPolicyRef());
      } else {
        b.clearPolicyRef();
      }
    }

    if (maskTargets(mask, "properties")) {
      b.clearProperties().putAllProperties(spec.getPropertiesMap());
    }

    return b.build();
  }

  private static byte[] canonicalFingerprint(CatalogSpec s) {
    return new Canonicalizer().scalar("name", normalizeName(s.getDisplayName())).bytes();
  }
}
