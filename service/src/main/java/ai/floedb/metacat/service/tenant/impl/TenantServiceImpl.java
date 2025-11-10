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
import com.google.protobuf.FieldMask;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jboss.logging.Logger;

@GrpcService
public class TenantServiceImpl extends BaseServiceImpl implements TenantService {
  @Inject TenantRepository tenantRepo;
  @Inject CatalogRepository catalogRepo;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject IdempotencyRepository idempotencyStore;

  private static final Set<String> TENANT_MUTABLE_PATHS = Set.of("display_name", "description");

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

                  List<Tenant> tenants;
                  try {
                    tenants = tenantRepo.list(Math.max(1, pageIn.limit), pageIn.token, next);
                  } catch (IllegalArgumentException badToken) {
                    throw GrpcErrors.invalidArgument(
                        correlationId(), "page_token.invalid", Map.of("page_token", pageIn.token));
                  }

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
                  final var pc = principal.get();
                  final var corr = pc.getCorrelationId();
                  final var tenantId = pc.getTenantId();
                  authz.require(pc, "tenant.write");

                  final var tsNow = nowTs();

                  final var spec = request.getSpec();
                  final String rawName = mustNonEmpty(spec.getDisplayName(), "display_name", corr);
                  final String normName = normalizeName(rawName);

                  final String explicitKey =
                      request.hasIdempotency() ? request.getIdempotency().getKey().trim() : "";
                  final String idempotencyKey = explicitKey.isEmpty() ? null : explicitKey;

                  final byte[] fingerprint = canonicalFingerprint(spec);

                  final String tenantUuid =
                      deterministicUuid(
                          tenantId,
                          "tenant",
                          Base64.getUrlEncoder().withoutPadding().encodeToString(fingerprint));

                  final var resourceId =
                      ResourceId.newBuilder()
                          .setTenantId(tenantId)
                          .setId(tenantUuid)
                          .setKind(ResourceKind.RK_TENANT)
                          .build();

                  final var desiredTenant =
                      Tenant.newBuilder()
                          .setResourceId(resourceId)
                          .setDisplayName(normName)
                          .setDescription(spec.getDescription())
                          .setCreatedAt(tsNow)
                          .build();

                  if (idempotencyKey == null) {
                    var existingOpt = tenantRepo.getByName(normName);
                    if (existingOpt.isPresent()) {
                      var existing = existingOpt.get();
                      var meta = tenantRepo.metaForSafe(existing.getResourceId());
                      return CreateTenantResponse.newBuilder()
                          .setTenant(existing)
                          .setMeta(meta)
                          .build();
                    }

                    tenantRepo.create(desiredTenant);
                    var meta = tenantRepo.metaForSafe(resourceId);
                    return CreateTenantResponse.newBuilder()
                        .setTenant(desiredTenant)
                        .setMeta(meta)
                        .build();
                  }

                  var result =
                      MutationOps.createProto(
                          tenantId,
                          "CreateTenant",
                          idempotencyKey,
                          () -> fingerprint,
                          () -> {
                            tenantRepo.create(desiredTenant);
                            return new IdempotencyGuard.CreateResult<>(desiredTenant, resourceId);
                          },
                          (t) -> tenantRepo.metaFor(t.getResourceId()),
                          idempotencyStore,
                          tsNow,
                          idempotencyTtlSeconds(),
                          this::correlationId,
                          Tenant::parseFrom,
                          rec -> tenantRepo.getById(rec.getResourceId()).isPresent());

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

                  if (!request.hasUpdateMask() || request.getUpdateMask().getPathsCount() == 0) {
                    throw GrpcErrors.invalidArgument(corr, "update_mask.required", Map.of());
                  }

                  var spec = request.getSpec();
                  var mask = request.getUpdateMask();

                  var desired = applyTenantSpecPatch(current, spec, mask, corr);

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
                      Map.of("display_name", desired.getDisplayName()));

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

  private Tenant applyTenantSpecPatch(
      Tenant current, TenantSpec spec, FieldMask mask, String corr) {

    var paths = normalizedMaskPaths(mask);
    if (paths.isEmpty()) {
      throw GrpcErrors.invalidArgument(corr, "update_mask.required", Map.of());
    }

    for (var p : paths) {
      if (!TENANT_MUTABLE_PATHS.contains(p)) {
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

    return b.build();
  }

  private static byte[] canonicalFingerprint(TenantSpec s) {
    return new Canonicalizer().scalar("name", normalizeName(s.getDisplayName())).bytes();
  }
}
