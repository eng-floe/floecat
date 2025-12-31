package ai.floedb.floecat.service.catalog.impl;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.CatalogService;
import ai.floedb.floecat.catalog.rpc.CatalogSpec;
import ai.floedb.floecat.catalog.rpc.CreateCatalogRequest;
import ai.floedb.floecat.catalog.rpc.CreateCatalogResponse;
import ai.floedb.floecat.catalog.rpc.DeleteCatalogRequest;
import ai.floedb.floecat.catalog.rpc.DeleteCatalogResponse;
import ai.floedb.floecat.catalog.rpc.GetCatalogRequest;
import ai.floedb.floecat.catalog.rpc.GetCatalogResponse;
import ai.floedb.floecat.catalog.rpc.ListCatalogsRequest;
import ai.floedb.floecat.catalog.rpc.ListCatalogsResponse;
import ai.floedb.floecat.catalog.rpc.UpdateCatalogRequest;
import ai.floedb.floecat.catalog.rpc.UpdateCatalogResponse;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.Canonicalizer;
import ai.floedb.floecat.service.common.IdempotencyGuard;
import ai.floedb.floecat.service.common.LogHelper;
import ai.floedb.floecat.service.common.MutationOps;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.metagraph.overlay.user.UserGraph;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
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
public class CatalogServiceImpl extends BaseServiceImpl implements CatalogService {

  @Inject CatalogRepository catalogRepo;
  @Inject NamespaceRepository namespaceRepo;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject IdempotencyRepository idempotencyStore;
  @Inject UserGraph metadataGraph;

  private static final Set<String> CATALOG_MUTABLE_PATHS =
      Set.of("display_name", "description", "connector_ref", "properties", "policy_ref");

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

                  List<Catalog> catalogs = null;
                  try {
                    catalogs =
                        catalogRepo.list(
                            principalContext.getAccountId(),
                            Math.max(1, pageIn.limit),
                            pageIn.token,
                            next);
                  } catch (IllegalArgumentException badToken) {
                    throw GrpcErrors.invalidArgument(
                        correlationId(), "page_token.invalid", Map.of("page_token", pageIn.token));
                  }

                  var page =
                      MutationOps.pageOut(
                          next.toString(), catalogRepo.count(principalContext.getAccountId()));

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
                  var pc = principal.get();
                  var corr = pc.getCorrelationId();
                  var accountId = pc.getAccountId();

                  authz.require(pc, "catalog.write");

                  var tsNow = nowTs();

                  var spec = request.getSpec();
                  var rawName = mustNonEmpty(spec.getDisplayName(), "display_name", corr);
                  var normName = normalizeName(rawName);

                  var explicitKey =
                      request.hasIdempotency() ? request.getIdempotency().getKey().trim() : "";
                  var idempotencyKey = explicitKey.isEmpty() ? null : explicitKey;

                  var normalizedSpec = spec.toBuilder().setDisplayName(normName).build();
                  var fingerprint = canonicalFingerprint(normalizedSpec);
                  var catalogId = randomResourceId(accountId, ResourceKind.RK_CATALOG);

                  var built =
                      Catalog.newBuilder()
                          .setResourceId(catalogId)
                          .setDisplayName(normName)
                          .setDescription(spec.getDescription())
                          .setCreatedAt(tsNow)
                          .build();

                  if (idempotencyKey == null) {
                    var existing = catalogRepo.getByName(accountId, normName);
                    if (existing.isPresent()) {
                      throw GrpcErrors.conflict(
                          corr, "catalog.already_exists", Map.of("display_name", normName));
                    }

                    catalogRepo.create(built);
                    metadataGraph.invalidate(catalogId);
                    var meta = catalogRepo.metaForSafe(catalogId);
                    return CreateCatalogResponse.newBuilder()
                        .setCatalog(built)
                        .setMeta(meta)
                        .build();
                  }

                  var result =
                      runIdempotentCreate(
                          () ->
                              MutationOps.createProto(
                                  accountId,
                                  "CreateCatalog",
                                  idempotencyKey,
                                  () -> fingerprint,
                                  () -> {
                                    catalogRepo.create(built);
                                    metadataGraph.invalidate(catalogId);
                                    return new IdempotencyGuard.CreateResult<>(built, catalogId);
                                  },
                                  c -> catalogRepo.metaForSafe(c.getResourceId()),
                                  idempotencyStore,
                                  tsNow,
                                  idempotencyTtlSeconds(),
                                  this::correlationId,
                                  Catalog::parseFrom));

                  return CreateCatalogResponse.newBuilder()
                      .setCatalog(result.body)
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

                  if (!request.hasUpdateMask() || request.getUpdateMask().getPathsCount() == 0) {
                    throw GrpcErrors.invalidArgument(corr, "update_mask.required", Map.of());
                  }

                  var spec = request.getSpec();
                  var mask = request.getUpdateMask();

                  var meta = catalogRepo.metaFor(catalogId);
                  MutationOps.BaseServiceChecks.enforcePreconditions(
                      corr, meta, request.getPrecondition());

                  var current =
                      catalogRepo
                          .getById(catalogId)
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(
                                      corr, "catalog", Map.of("id", catalogId.getId())));

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

                  try {
                    boolean ok = catalogRepo.update(desired, meta.getPointerVersion());
                    if (!ok) {
                      var nowMeta = catalogRepo.metaForSafe(catalogId);
                      throw GrpcErrors.preconditionFailed(
                          corr,
                          "version_mismatch",
                          Map.of(
                              "expected", Long.toString(meta.getPointerVersion()),
                              "actual", Long.toString(nowMeta.getPointerVersion())));
                    }
                  } catch (BaseResourceRepository.NameConflictException nce) {
                    throw GrpcErrors.conflict(
                        corr,
                        "catalog.already_exists",
                        Map.of("display_name", desired.getDisplayName()));
                  } catch (BaseResourceRepository.PreconditionFailedException pfe) {
                    var nowMeta = catalogRepo.metaForSafe(catalogId);
                    throw GrpcErrors.preconditionFailed(
                        corr,
                        "version_mismatch",
                        Map.of(
                            "expected", Long.toString(meta.getPointerVersion()),
                            "actual", Long.toString(nowMeta.getPointerVersion())));
                  }
                  metadataGraph.invalidate(catalogId);

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

                  MutationMeta meta;
                  try {
                    meta = catalogRepo.metaFor(id);
                  } catch (BaseResourceRepository.NotFoundException missing) {
                    var safe = catalogRepo.metaForSafe(id);
                    MutationOps.BaseServiceChecks.enforcePreconditions(
                        correlationId, safe, request.getPrecondition());
                    catalogRepo.delete(id);
                    metadataGraph.invalidate(id);
                    return DeleteCatalogResponse.newBuilder().setMeta(safe).build();
                  }

                  if (namespaceRepo.count(id.getAccountId(), id.getId(), List.of()) > 0) {
                    var currentCatalog = catalogRepo.getById(id).orElse(null);
                    var displayName =
                        (currentCatalog != null && !currentCatalog.getDisplayName().isBlank())
                            ? currentCatalog.getDisplayName()
                            : id.getId();
                    throw GrpcErrors.conflict(
                        correlationId, "catalog.not_empty", Map.of("display_name", displayName));
                  }

                  var out =
                      MutationOps.deleteWithPreconditions(
                          () -> meta,
                          request.getPrecondition(),
                          expected -> catalogRepo.deleteWithPrecondition(id, expected),
                          () -> catalogRepo.metaForSafe(id),
                          correlationId,
                          "catalog",
                          Map.of("id", id.getId()));

                  metadataGraph.invalidate(id);
                  return DeleteCatalogResponse.newBuilder().setMeta(out).build();
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
    return new Canonicalizer()
        .scalar("name", normalizeName(s.getDisplayName()))
        .scalar("description", s.getDescription())
        .scalar("connector_ref", s.getConnectorRef())
        .scalar("policy_ref", s.getPolicyRef())
        .map("properties", s.getPropertiesMap())
        .bytes();
  }
}
