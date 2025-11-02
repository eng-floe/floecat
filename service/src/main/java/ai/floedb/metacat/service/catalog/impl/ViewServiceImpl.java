package ai.floedb.metacat.service.catalog.impl;

import ai.floedb.metacat.catalog.rpc.CreateViewRequest;
import ai.floedb.metacat.catalog.rpc.CreateViewResponse;
import ai.floedb.metacat.catalog.rpc.DeleteViewRequest;
import ai.floedb.metacat.catalog.rpc.DeleteViewResponse;
import ai.floedb.metacat.catalog.rpc.GetViewRequest;
import ai.floedb.metacat.catalog.rpc.GetViewResponse;
import ai.floedb.metacat.catalog.rpc.ListViewsRequest;
import ai.floedb.metacat.catalog.rpc.ListViewsResponse;
import ai.floedb.metacat.catalog.rpc.UpdateViewRequest;
import ai.floedb.metacat.catalog.rpc.UpdateViewResponse;
import ai.floedb.metacat.catalog.rpc.View;
import ai.floedb.metacat.catalog.rpc.ViewService;
import ai.floedb.metacat.catalog.rpc.ViewSpec;
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
import ai.floedb.metacat.service.repo.impl.ViewRepository;
import ai.floedb.metacat.service.repo.util.BaseResourceRepository;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.Map;
import org.jboss.logging.Logger;

@GrpcService
public class ViewServiceImpl extends BaseServiceImpl implements ViewService {

  @Inject CatalogRepository catalogRepo;
  @Inject NamespaceRepository namespaceRepo;
  @Inject ViewRepository viewRepo;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject IdempotencyRepository idempotencyStore;

  private static final Logger LOG = Logger.getLogger(ViewService.class);

  @Override
  public Uni<ListViewsResponse> listViews(ListViewsRequest request) {
    var L = LogHelper.start(LOG, "ListViews");

    return mapFailures(
            run(
                () -> {
                  var principalContext = principal.get();
                  authz.require(principalContext, "view.read");

                  var pageIn = MutationOps.pageIn(request.hasPage() ? request.getPage() : null);
                  var next = new StringBuilder();

                  var namespaceId = request.getNamespaceId();
                  ensureKind(
                      namespaceId, ResourceKind.RK_NAMESPACE, "namespace_id", correlationId());

                  var namespace =
                      namespaceRepo
                          .getById(namespaceId)
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(
                                      correlationId(),
                                      "namespace",
                                      Map.of("id", namespaceId.getId())));

                  var catalogId = namespace.getCatalogId();
                  var views =
                      viewRepo.list(
                          principalContext.getTenantId(),
                          catalogId.getId(),
                          namespaceId.getId(),
                          Math.max(1, pageIn.limit),
                          pageIn.token,
                          next);

                  var page =
                      MutationOps.pageOut(
                          next.toString(),
                          viewRepo.count(
                              principalContext.getTenantId(),
                              catalogId.getId(),
                              namespaceId.getId()));

                  return ListViewsResponse.newBuilder().addAllViews(views).setPage(page).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<GetViewResponse> getView(GetViewRequest request) {
    var L = LogHelper.start(LOG, "GetView");

    return mapFailures(
            run(
                () -> {
                  var principalContext = principal.get();
                  authz.require(principalContext, "view.read");

                  var viewId = request.getViewId();
                  ensureKind(viewId, ResourceKind.RK_VIEW, "view_id", correlationId());

                  var view =
                      viewRepo
                          .getById(viewId)
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(
                                      correlationId(), "view", Map.of("id", viewId.getId())));

                  return GetViewResponse.newBuilder().setView(view).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<CreateViewResponse> createView(CreateViewRequest request) {
    var L = LogHelper.start(LOG, "CreateView");

    return mapFailures(
            runWithRetry(
                () -> {
                  var principalContext = principal.get();
                  var correlationId = principalContext.getCorrelationId();
                  authz.require(principalContext, "view.write");

                  if (!request.hasSpec()) {
                    throw GrpcErrors.invalidArgument(correlationId, "view.missing_spec", Map.of());
                  }
                  var spec = request.getSpec();

                  if (!spec.hasCatalogId()) {
                    throw GrpcErrors.invalidArgument(
                        correlationId, "view.missing_catalog_id", Map.of());
                  }
                  if (!spec.hasNamespaceId()) {
                    throw GrpcErrors.invalidArgument(
                        correlationId, "view.missing_namespace_id", Map.of());
                  }

                  var catalogId = spec.getCatalogId();
                  ensureKind(catalogId, ResourceKind.RK_CATALOG, "spec.catalog_id", correlationId);
                  catalogRepo
                      .getById(catalogId)
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  correlationId, "catalog", Map.of("id", catalogId.getId())));

                  var namespaceId = spec.getNamespaceId();
                  ensureKind(
                      namespaceId, ResourceKind.RK_NAMESPACE, "spec.namespace_id", correlationId);
                  var namespace =
                      namespaceRepo
                          .getById(namespaceId)
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(
                                      correlationId,
                                      "namespace",
                                      Map.of("id", namespaceId.getId())));
                  if (!namespace.getCatalogId().getId().equals(catalogId.getId())) {
                    throw GrpcErrors.invalidArgument(
                        correlationId,
                        "namespace.catalog_mismatch",
                        Map.of(
                            "namespace_id", namespaceId.getId(),
                            "namespace.catalog_id", namespace.getCatalogId().getId(),
                            "catalog_id", catalogId.getId()));
                  }

                  var tsNow = nowTs();
                  var fingerprint = canonicalFingerprint(request.getSpec());
                  var idempotencyKey =
                      request.hasIdempotency() && !request.getIdempotency().getKey().isBlank()
                          ? request.getIdempotency().getKey()
                          : hashFingerprint(fingerprint);

                  var tenantId = principalContext.getTenantId();
                  var viewProto =
                      MutationOps.createProto(
                          tenantId,
                          "CreateView",
                          idempotencyKey,
                          () -> fingerprint,
                          () -> {
                            String viewUuid = deterministicUuid(tenantId, "table", idempotencyKey);

                            var viewResourceId =
                                ResourceId.newBuilder()
                                    .setTenantId(tenantId)
                                    .setId(viewUuid)
                                    .setKind(ResourceKind.RK_VIEW)
                                    .build();

                            var view =
                                View.newBuilder()
                                    .setResourceId(viewResourceId)
                                    .setCatalogId(spec.getCatalogId())
                                    .setNamespaceId(spec.getNamespaceId())
                                    .setDisplayName(
                                        mustNonEmpty(
                                            spec.getDisplayName(),
                                            "spec.display_name",
                                            correlationId))
                                    .setDescription(spec.getDescription())
                                    .setSql(mustNonEmpty(spec.getSql(), "spec.sql", correlationId))
                                    .setCreatedAt(tsNow)
                                    .putAllProperties(spec.getPropertiesMap())
                                    .build();

                            try {
                              viewRepo.create(view);
                            } catch (BaseResourceRepository.NameConflictException e) {
                              var existing =
                                  viewRepo.getByName(
                                      tenantId,
                                      request.getSpec().getCatalogId().getId(),
                                      request.getSpec().getNamespaceId().getId(),
                                      view.getDisplayName());

                              if (existing.isPresent()) {
                                throw GrpcErrors.conflict(
                                    correlationId,
                                    "view.already_exists",
                                    Map.of("display_name", view.getDisplayName()));
                              }

                              throw new BaseResourceRepository.AbortRetryableException(
                                  "name conflict visibility window");
                            }

                            return new IdempotencyGuard.CreateResult<>(view, viewResourceId);
                          },
                          (view) -> viewRepo.metaForSafe(view.getResourceId()),
                          idempotencyStore,
                          tsNow,
                          idempotencyTtlSeconds(),
                          this::correlationId,
                          View::parseFrom);

                  return CreateViewResponse.newBuilder()
                      .setView(viewProto.body)
                      .setMeta(viewProto.meta)
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<UpdateViewResponse> updateView(UpdateViewRequest request) {
    var L = LogHelper.start(LOG, "UpdateView");

    return mapFailures(
            runWithRetry(
                () -> {
                  var principalContext = principal.get();
                  var correlationId = principalContext.getCorrelationId();
                  authz.require(principalContext, "view.write");

                  var viewId = request.getViewId();
                  ensureKind(viewId, ResourceKind.RK_VIEW, "view_id", correlationId);

                  var current =
                      viewRepo
                          .getById(viewId)
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(
                                      correlationId, "view", Map.of("id", viewId.getId())));

                  var spec = request.getSpec();
                  var updatedBuilder = current.toBuilder();

                  if (!spec.getDisplayName().isBlank()) {
                    updatedBuilder.setDisplayName(
                        mustNonEmpty(spec.getDisplayName(), "spec.display_name", correlationId));
                  }
                  if (!spec.getDescription().isBlank()) {
                    updatedBuilder.setDescription(spec.getDescription());
                  }
                  if (!spec.getSql().isBlank()) {
                    updatedBuilder.setSql(mustNonEmpty(spec.getSql(), "spec.sql", correlationId));
                  }
                  if (!spec.getPropertiesMap().isEmpty()) {
                    updatedBuilder.clearProperties().putAllProperties(spec.getPropertiesMap());
                  }

                  boolean catalogChanged = false;
                  boolean namespaceChanged = false;

                  if (spec.hasCatalogId()) {
                    var catalogId = spec.getCatalogId();
                    ensureKind(
                        catalogId, ResourceKind.RK_CATALOG, "spec.catalog_id", correlationId);
                    catalogRepo
                        .getById(catalogId)
                        .orElseThrow(
                            () ->
                                GrpcErrors.notFound(
                                    correlationId, "catalog", Map.of("id", catalogId.getId())));
                    updatedBuilder.setCatalogId(catalogId);
                    catalogChanged = true;
                  }

                  if (spec.hasNamespaceId()) {
                    var namespaceId = spec.getNamespaceId();
                    ensureKind(
                        namespaceId, ResourceKind.RK_NAMESPACE, "spec.namespace_id", correlationId);
                    var namespace =
                        namespaceRepo
                            .getById(namespaceId)
                            .orElseThrow(
                                () ->
                                    GrpcErrors.notFound(
                                        correlationId,
                                        "namespace",
                                        Map.of("id", namespaceId.getId())));

                    var effectiveCatalogId =
                        catalogChanged ? updatedBuilder.getCatalogId() : current.getCatalogId();
                    if (!namespace.getCatalogId().getId().equals(effectiveCatalogId.getId())) {
                      throw GrpcErrors.invalidArgument(
                          correlationId,
                          "namespace.catalog_mismatch",
                          Map.of(
                              "namespace_id", namespaceId.getId(),
                              "namespace.catalog_id", namespace.getCatalogId().getId(),
                              "catalog_id", effectiveCatalogId.getId()));
                    }
                    updatedBuilder.setNamespaceId(namespaceId);
                    namespaceChanged = true;
                  }

                  if (catalogChanged && !namespaceChanged) {
                    var effectiveCatalogId = updatedBuilder.getCatalogId();
                    var namespace =
                        namespaceRepo
                            .getById(updatedBuilder.getNamespaceId())
                            .orElseThrow(
                                () ->
                                    GrpcErrors.notFound(
                                        correlationId,
                                        "namespace",
                                        Map.of("id", updatedBuilder.getNamespaceId().getId())));
                    if (!namespace.getCatalogId().getId().equals(effectiveCatalogId.getId())) {
                      throw GrpcErrors.invalidArgument(
                          correlationId,
                          "namespace.catalog_mismatch",
                          Map.of(
                              "namespace_id", updatedBuilder.getNamespaceId().getId(),
                              "namespace.catalog_id", namespace.getCatalogId().getId(),
                              "catalog_id", effectiveCatalogId.getId()));
                    }
                  }

                  var desired = updatedBuilder.build();
                  if (desired.equals(current)) {
                    var meta = viewRepo.metaForSafe(viewId);
                    MutationOps.BaseServiceChecks.enforcePreconditions(
                        correlationId, meta, request.getPrecondition());
                    return UpdateViewResponse.newBuilder().setView(current).setMeta(meta).build();
                  }

                  var conflictInfo =
                      Map.of(
                          "display_name", desired.getDisplayName(),
                          "catalog_id", desired.getCatalogId().getId(),
                          "namespace_id", desired.getNamespaceId().getId());

                  MutationOps.updateWithPreconditions(
                      () -> viewRepo.metaFor(viewId),
                      request.getPrecondition(),
                      expected -> viewRepo.update(desired, expected),
                      () -> viewRepo.metaForSafe(viewId),
                      correlationId,
                      "view",
                      conflictInfo);

                  var outMeta = viewRepo.metaForSafe(viewId);
                  var latest = viewRepo.getById(viewId).orElse(desired);
                  return UpdateViewResponse.newBuilder().setView(latest).setMeta(outMeta).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<DeleteViewResponse> deleteView(DeleteViewRequest request) {
    var L = LogHelper.start(LOG, "DeleteView");

    return mapFailures(
            runWithRetry(
                () -> {
                  var principalContext = principal.get();
                  var correlationId = principalContext.getCorrelationId();
                  authz.require(principalContext, "view.write");

                  var viewId = request.getViewId();
                  ensureKind(viewId, ResourceKind.RK_VIEW, "view_id", correlationId);

                  try {
                    var meta =
                        MutationOps.deleteWithPreconditions(
                            () -> viewRepo.metaFor(viewId),
                            request.getPrecondition(),
                            expected -> viewRepo.deleteWithPrecondition(viewId, expected),
                            () -> viewRepo.metaForSafe(viewId),
                            correlationId,
                            "view",
                            Map.of("id", viewId.getId()));

                    return DeleteViewResponse.newBuilder().setMeta(meta).build();
                  } catch (BaseResourceRepository.NotFoundException missing) {
                    viewRepo.delete(viewId);
                    return DeleteViewResponse.newBuilder()
                        .setMeta(viewRepo.metaForSafe(viewId))
                        .build();
                  }
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  private static byte[] canonicalFingerprint(ViewSpec s) {
    return new Canonicalizer()
        .scalar("cat", s.getCatalogId().getId())
        .scalar("ns", s.getNamespaceId().getId())
        .scalar("name", s.getDisplayName())
        .scalar("description", s.getDescription())
        .scalar("sql", s.getSql())
        .scalar("schema", s.getSchemaJson())
        .map("prop", s.getPropertiesMap())
        .bytes();
  }
}
