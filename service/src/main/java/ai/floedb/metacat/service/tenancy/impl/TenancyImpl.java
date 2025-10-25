package ai.floedb.metacat.service.tenancy.impl;

import ai.floedb.metacat.common.rpc.MutationMeta;
import ai.floedb.metacat.common.rpc.PageResponse;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.catalog.util.MutationOps;
import ai.floedb.metacat.service.common.BaseServiceImpl;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.repo.impl.CatalogRepository;
import ai.floedb.metacat.service.repo.impl.TenantRepository;
import ai.floedb.metacat.service.repo.util.BaseRepository;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;
import ai.floedb.metacat.service.storage.IdempotencyStore;
import ai.floedb.metacat.service.storage.util.IdempotencyGuard;
import ai.floedb.metacat.tenancy.rpc.*;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.UUID;

@GrpcService
public class TenancyImpl extends BaseServiceImpl implements Tenancy {
  @Inject TenantRepository tenants;
  @Inject CatalogRepository catalogs;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject IdempotencyStore idempotencyStore;

  @Override
  public Uni<CreateTenantResponse> createTenant(CreateTenantRequest request) {
    return mapFailures(
        runWithRetry(
            () -> {
              final var principalContext = principal.get();
              final var correlationId = principalContext.getCorrelationId();
              final var tenantId = principalContext.getTenantId();

              authz.require(principalContext, "tenant.write");

              final var tsNow = nowTs();
              final var idempotencyKey =
                  request.hasIdempotency() ? request.getIdempotency().getKey() : "";

              final byte[] fingerprint = request.getSpec().toBuilder().build().toByteArray();

              var result =
                  MutationOps.createProto(
                      tenantId,
                      "CreateTenant",
                      idempotencyKey,
                      () -> fingerprint,
                      () -> {
                        final String tenantUuid =
                            !idempotencyKey.isBlank()
                                ? deterministicUuid(tenantId, "tenant", idempotencyKey)
                                : UUID.randomUUID().toString();

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
                          tenants.create(tenant);
                        } catch (BaseRepository.NameConflictException nce) {
                          if (!idempotencyKey.isBlank()) {
                            var existing = tenants.getByName(tenant.getDisplayName());
                            if (existing.isPresent()) {
                              return new IdempotencyGuard.CreateResult<>(
                                  existing.get(), existing.get().getResourceId());
                            }
                          }
                          throw GrpcErrors.conflict(
                              correlationId,
                              "tenant.already_exists",
                              Map.of("display_name", tenant.getDisplayName()));
                        }

                        return new IdempotencyGuard.CreateResult<>(tenant, resourceId);
                      },
                      (t) -> tenants.metaFor(t.getResourceId()),
                      idempotencyStore,
                      tsNow,
                      IDEMPOTENCY_TTL_SECONDS,
                      this::correlationId,
                      Tenant::parseFrom);

              return CreateTenantResponse.newBuilder()
                  .setTenant(result.body)
                  .setMeta(result.meta)
                  .build();
            }),
        correlationId());
  }

  @Override
  public Uni<GetTenantResponse> getTenant(GetTenantRequest request) {
    return mapFailures(
        runWithRetry(
            () -> {
              final var principalContext = principal.get();
              final var correlationId = principalContext.getCorrelationId();
              authz.require(principalContext, "tenant.read");

              var resourceId = request.getTenantId();
              ensureKind(resourceId, ResourceKind.RK_TENANT, "tenant_id", correlationId);

              var tenant =
                  tenants
                      .getById(resourceId)
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  correlationId, "tenant", Map.of("id", resourceId.getId())));

              return GetTenantResponse.newBuilder().setTenant(tenant).build();
            }),
        correlationId());
  }

  @Override
  public Uni<ListTenantsResponse> listTenants(ListTenantsRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principal.get();

              authz.require(principalContext, "tenant.read");

              final int limit =
                  (request.hasPage() && request.getPage().getPageSize() > 0)
                      ? request.getPage().getPageSize()
                      : 50;
              final String token = request.hasPage() ? request.getPage().getPageToken() : "";
              final StringBuilder next = new StringBuilder();

              var items = tenants.list(Math.max(1, limit), token, next);

              int total = tenants.count();

              var page =
                  PageResponse.newBuilder()
                      .setNextPageToken(next.toString())
                      .setTotalSize(total)
                      .build();

              return ListTenantsResponse.newBuilder().addAllTenants(items).setPage(page).build();
            }),
        correlationId());
  }

  @Override
  public Uni<UpdateTenantResponse> updateTenant(UpdateTenantRequest request) {
    return mapFailures(
        runWithRetry(
            () -> {
              final var principalContext = principal.get();
              final var correlationId = principalContext.getCorrelationId();
              authz.require(principalContext, "tenant.write");

              var resourceId = request.getTenantId();
              ensureKind(resourceId, ResourceKind.RK_TENANT, "tenant_id", correlationId);

              var currentTenant =
                  tenants
                      .getById(resourceId)
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  correlationId, "tenant", Map.of("id", resourceId.getId())));

              var desiredDisplayName =
                  mustNonEmpty(request.getSpec().getDisplayName(), "display_name", correlationId);
              var desiredDescription = request.getSpec().getDescription();

              var desiredTenant =
                  currentTenant.toBuilder()
                      .setDisplayName(desiredDisplayName)
                      .setDescription(desiredDescription)
                      .build();

              if (desiredTenant.equals(currentTenant)) {
                var metaNoop = tenants.metaForSafe(resourceId);
                enforcePreconditions(correlationId, metaNoop, request.getPrecondition());
                return UpdateTenantResponse.newBuilder()
                    .setTenant(currentTenant)
                    .setMeta(metaNoop)
                    .build();
              }

              var currentMeta = tenants.metaFor(resourceId);
              enforcePreconditions(correlationId, currentMeta, request.getPrecondition());
              long expectedVersion = currentMeta.getPointerVersion();

              try {
                boolean ok = tenants.update(desiredTenant, expectedVersion);
                if (!ok) {
                  var now = tenants.metaForSafe(resourceId);
                  throw GrpcErrors.preconditionFailed(
                      correlationId,
                      "version_mismatch",
                      Map.of(
                          "expected", Long.toString(expectedVersion),
                          "actual", Long.toString(now.getPointerVersion())));
                }
              } catch (BaseRepository.NameConflictException nce) {
                throw GrpcErrors.conflict(
                    correlationId,
                    "tenant.already_exists",
                    Map.of("display_name", desiredDisplayName));
              }

              return UpdateTenantResponse.newBuilder()
                  .setTenant(tenants.getById(resourceId).orElse(desiredTenant))
                  .setMeta(tenants.metaForSafe(resourceId))
                  .build();
            }),
        correlationId());
  }

  @Override
  public Uni<DeleteTenantResponse> deleteTenant(DeleteTenantRequest request) {
    return mapFailures(
        runWithRetry(
            () -> {
              final var principalContext = principal.get();
              final var correlationId = principalContext.getCorrelationId();
              authz.require(principalContext, "tenant.write");

              var resourceId = request.getTenantId();
              ensureKind(resourceId, ResourceKind.RK_TENANT, "tenant_id", correlationId);

              if (catalogs.count(resourceId.getId()) > 0) {
                var currentTenant = tenants.getById(resourceId).orElse(null);
                var displayName =
                    (currentTenant != null && !currentTenant.getDisplayName().isBlank())
                        ? currentTenant.getDisplayName()
                        : resourceId.getId();
                throw GrpcErrors.conflict(
                    correlationId, "tenant.not_empty", Map.of("display_name", displayName));
              }

              MutationMeta currentMeta;
              try {
                currentMeta = tenants.metaFor(resourceId);
              } catch (BaseRepository.NotFoundException pointerMissing) {
                tenants.delete(resourceId);
                return DeleteTenantResponse.newBuilder()
                    .setMeta(tenants.metaForSafe(resourceId))
                    .build();
              }

              enforcePreconditions(correlationId, currentMeta, request.getPrecondition());
              long expectedVersion = currentMeta.getPointerVersion();

              try {
                boolean ok = tenants.deleteWithPrecondition(resourceId, expectedVersion);
                if (!ok) {
                  var now = tenants.metaForSafe(resourceId);
                  throw GrpcErrors.preconditionFailed(
                      correlationId,
                      "version_mismatch",
                      Map.of(
                          "expected", Long.toString(expectedVersion),
                          "actual", Long.toString(now.getPointerVersion())));
                }
              } catch (BaseRepository.NotFoundException nfe) {
                throw GrpcErrors.notFound(
                    correlationId, "tenant", Map.of("id", resourceId.getId()));
              }

              return DeleteTenantResponse.newBuilder().setMeta(currentMeta).build();
            }),
        correlationId());
  }
}
