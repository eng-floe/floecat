package ai.floedb.metacat.service.tenant.impl;

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
import ai.floedb.metacat.service.repo.impl.TenantRepository;
import ai.floedb.metacat.service.repo.util.BaseResourceRepository;
import ai.floedb.metacat.service.repo.util.BaseResourceRepository.AbortRetryableException;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;
import ai.floedb.metacat.tenant.rpc.CreateTenantRequest;
import ai.floedb.metacat.tenant.rpc.CreateTenantResponse;
import ai.floedb.metacat.tenant.rpc.DeleteTenantRequest;
import ai.floedb.metacat.tenant.rpc.DeleteTenantResponse;
import ai.floedb.metacat.tenant.rpc.GetTenantRequest;
import ai.floedb.metacat.tenant.rpc.GetTenantResponse;
import ai.floedb.metacat.tenant.rpc.ListTenantsRequest;
import ai.floedb.metacat.tenant.rpc.ListTenantsResponse;
import ai.floedb.metacat.tenant.rpc.Tenant;
import ai.floedb.metacat.tenant.rpc.TenantService;
import ai.floedb.metacat.tenant.rpc.TenantSpec;
import ai.floedb.metacat.tenant.rpc.UpdateTenantRequest;
import ai.floedb.metacat.tenant.rpc.UpdateTenantResponse;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.Map;
import org.jboss.logging.Logger;

@GrpcService
public class TenantServiceImpl extends BaseServiceImpl implements TenantService {
  @Inject TenantRepository tenantRepo;
  @Inject CatalogRepository catalogRepo;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject IdempotencyRepository idempotencyStore;

  private static final Logger LOG = Logger.getLogger(TenantService.class);

  @Override
  public Uni<ListTenantsResponse> listTenants(ListTenantsRequest request) {
    var L = LogHelper.start(LOG, "ListTenants");

    return mapFailures(
            run(
                () -> {
                  var principalContext = principal.get();
                  authz.require(principalContext, "tenant.read");

                  var pageIn = MutationOps.pageIn(request.hasPage() ? request.getPage() : null);
                  var next = new StringBuilder();

                  var tenants = tenantRepo.list(Math.max(1, pageIn.limit), pageIn.token, next);

                  var page = MutationOps.pageOut(next.toString(), tenantRepo.count());

                  return ListTenantsResponse.newBuilder()
                      .addAllTenants(tenants)
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
  public Uni<GetTenantResponse> getTenant(GetTenantRequest request) {
    var L = LogHelper.start(LOG, "GetTenant");

    return mapFailures(
            runWithRetry(
                () -> {
                  final var principalContext = principal.get();
                  final var correlationId = principalContext.getCorrelationId();
                  authz.require(principalContext, "tenant.read");

                  var resourceId = request.getTenantId();
                  ensureKind(resourceId, ResourceKind.RK_TENANT, "tenant_id", correlationId);

                  var tenant =
                      tenantRepo
                          .getById(resourceId)
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(
                                      correlationId, "tenant", Map.of("id", resourceId.getId())));

                  return GetTenantResponse.newBuilder().setTenant(tenant).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<CreateTenantResponse> createTenant(CreateTenantRequest request) {
    var L = LogHelper.start(LOG, "CreateTenant");

    return mapFailures(
            runWithRetry(
                () -> {
                  final var principalContext = principal.get();
                  final var correlationId = principalContext.getCorrelationId();
                  final var tenantId = principalContext.getTenantId();

                  authz.require(principalContext, "tenant.write");

                  final var tsNow = nowTs();

                  var fingerprint = canonicalFingerprint(request.getSpec());
                  var idempotencyKey =
                      request.hasIdempotency() && !request.getIdempotency().getKey().isBlank()
                          ? request.getIdempotency().getKey()
                          : hashFingerprint(fingerprint);

                  var result =
                      MutationOps.createProto(
                          tenantId,
                          "CreateTenant",
                          idempotencyKey,
                          () -> fingerprint,
                          () -> {
                            final String tenantUuid =
                                deterministicUuid(tenantId, "tenant", idempotencyKey);

                            var resourceId =
                                ResourceId.newBuilder()
                                    .setTenantId(tenantId)
                                    .setId(tenantUuid)
                                    .setKind(ResourceKind.RK_TENANT)
                                    .build();

                            var tenant =
                                Tenant.newBuilder()
                                    .setResourceId(resourceId)
                                    .setDisplayName(
                                        mustNonEmpty(
                                            request.getSpec().getDisplayName(),
                                            "display_name",
                                            correlationId))
                                    .setDescription(request.getSpec().getDescription())
                                    .setCreatedAt(tsNow)
                                    .build();

                            try {
                              tenantRepo.create(tenant);
                            } catch (BaseResourceRepository.NameConflictException nce) {
                              var existing = tenantRepo.getByName(tenant.getDisplayName());
                              if (existing.isPresent()) {
                                throw GrpcErrors.conflict(
                                    correlationId,
                                    "tenant.already_exists",
                                    Map.of("display_name", tenant.getDisplayName()));
                              }

                              throw new AbortRetryableException("name conflict visibility window");
                            }

                            return new IdempotencyGuard.CreateResult<>(tenant, resourceId);
                          },
                          (t) -> tenantRepo.metaFor(t.getResourceId()),
                          idempotencyStore,
                          tsNow,
                          idempotencyTtlSeconds(),
                          this::correlationId,
                          Tenant::parseFrom);

                  return CreateTenantResponse.newBuilder()
                      .setTenant(result.body)
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
  public Uni<UpdateTenantResponse> updateTenant(UpdateTenantRequest request) {
    var L = LogHelper.start(LOG, "UpdateTenant");

    return mapFailures(
            runWithRetry(
                () -> {
                  final var pc = principal.get();
                  final var corr = pc.getCorrelationId();
                  authz.require(pc, "tenant.write");

                  var tenantId = request.getTenantId();
                  ensureKind(tenantId, ResourceKind.RK_TENANT, "tenant_id", corr);

                  var current =
                      tenantRepo
                          .getById(tenantId)
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(
                                      corr, "tenant", Map.of("id", tenantId.getId())));

                  var desiredName =
                      mustNonEmpty(request.getSpec().getDisplayName(), "display_name", corr);
                  var desiredDesc = request.getSpec().getDescription();

                  var desired =
                      current.toBuilder()
                          .setDisplayName(desiredName)
                          .setDescription(desiredDesc)
                          .build();

                  if (desired.equals(current)) {
                    var metaNoop = tenantRepo.metaForSafe(tenantId);
                    MutationOps.BaseServiceChecks.enforcePreconditions(
                        corr, metaNoop, request.getPrecondition());
                    return UpdateTenantResponse.newBuilder()
                        .setTenant(current)
                        .setMeta(metaNoop)
                        .build();
                  }

                  MutationOps.updateWithPreconditions(
                      () -> tenantRepo.metaFor(tenantId),
                      request.getPrecondition(),
                      expected -> tenantRepo.update(desired, expected),
                      () -> tenantRepo.metaForSafe(tenantId),
                      corr,
                      "tenant",
                      Map.of("display_name", desiredName));

                  var outMeta = tenantRepo.metaForSafe(tenantId);
                  var latest = tenantRepo.getById(tenantId).orElse(desired);
                  return UpdateTenantResponse.newBuilder()
                      .setTenant(latest)
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
  public Uni<DeleteTenantResponse> deleteTenant(DeleteTenantRequest request) {
    var L = LogHelper.start(LOG, "DeleteTenant");

    return mapFailures(
            runWithRetry(
                () -> {
                  final var pc = principal.get();
                  final var corr = pc.getCorrelationId();
                  authz.require(pc, "tenant.write");

                  var tenantId = request.getTenantId();
                  ensureKind(tenantId, ResourceKind.RK_TENANT, "tenant_id", corr);

                  if (catalogRepo.count(tenantId.getId()) > 0) {
                    var cur = tenantRepo.getById(tenantId).orElse(null);
                    var name =
                        (cur != null && !cur.getDisplayName().isBlank())
                            ? cur.getDisplayName()
                            : tenantId.getId();
                    throw GrpcErrors.conflict(
                        corr, "tenant.not_empty", Map.of("display_name", name));
                  }

                  var meta =
                      MutationOps.deleteWithPreconditions(
                          () -> tenantRepo.metaFor(tenantId),
                          request.getPrecondition(),
                          expected -> tenantRepo.deleteWithPrecondition(tenantId, expected),
                          () -> tenantRepo.metaForSafe(tenantId),
                          corr,
                          "tenant",
                          Map.of("id", tenantId.getId()));

                  return DeleteTenantResponse.newBuilder().setMeta(meta).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  private static byte[] canonicalFingerprint(TenantSpec s) {
    return new Canonicalizer()
        .scalar("name", s.getDisplayName())
        .scalar("description", s.getDescription())
        .bytes();
  }
}
