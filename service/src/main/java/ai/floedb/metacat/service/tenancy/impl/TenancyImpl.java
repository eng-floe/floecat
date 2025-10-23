package ai.floedb.metacat.service.tenancy.impl;

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
import java.util.Objects;
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
              final var tenantId = principalContext.getTenantId().getId();

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
                        final String tenantUUID =
                            !idempotencyKey.isBlank()
                                ? deterministicUuid(tenantId, "tenant", idempotencyKey)
                                : UUID.randomUUID().toString();

                        var rid =
                            ResourceId.newBuilder()
                                .setTenantId(tenantId)
                                .setId(tenantUUID)
                                .setKind(ResourceKind.RK_TENANT)
                                .build();

                        var built =
                            Tenant.newBuilder()
                                .setResourceId(rid)
                                .setDisplayName(
                                    mustNonEmpty(
                                        request.getSpec().getDisplayName(),
                                        "display_name",
                                        correlationId))
                                .setDescription(request.getSpec().getDescription())
                                .setCreatedAt(tsNow)
                                .build();

                        try {
                          tenants.create(built);
                        } catch (BaseRepository.NameConflictException nce) {
                          if (!idempotencyKey.isBlank()) {
                            var existing = tenants.getByName(built.getDisplayName());
                            if (existing.isPresent()) {
                              return new IdempotencyGuard.CreateResult<>(
                                  existing.get(), existing.get().getResourceId());
                            }
                          }
                          throw GrpcErrors.conflict(
                              correlationId,
                              "tenant.already_exists",
                              Map.of("display_name", built.getDisplayName()));
                        }

                        return new IdempotencyGuard.CreateResult<>(built, rid);
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

              var rid = request.getTenantId();
              ensureKind(rid, ResourceKind.RK_TENANT, "tenant_id", correlationId);

              var t =
                  tenants
                      .getById(rid)
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  correlationId, "tenant", Map.of("id", rid.getId())));

              return GetTenantResponse.newBuilder().setTenant(t).build();
            }),
        correlationId());
  }

  @Override
  public Uni<ListTenantsResponse> listTenants(ListTenantsRequest request) {
    return mapFailures(
        runWithRetry(
            () -> {
              final var principalContext = principal.get();
              authz.require(principalContext, "tenant.read");

              final int pageSize = Math.max(1, request.getPageSize());
              final String token = request.getPageToken();
              final var next = new StringBuilder();

              var out =
                  tenants.listByName(
                      pageSize, Objects.toString(request.getNamePrefix(), ""), token, next);

              return ListTenantsResponse.newBuilder()
                  .addAllTenants(out)
                  .setNextPageToken(next.toString())
                  .build();
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

              var rid = request.getTenantId();
              ensureKind(rid, ResourceKind.RK_TENANT, "tenant_id", correlationId);

              var prev =
                  tenants
                      .getById(rid)
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  correlationId, "tenant", Map.of("id", rid.getId())));

              var desiredName =
                  mustNonEmpty(request.getSpec().getDisplayName(), "display_name", correlationId);
              var desiredDesc = request.getSpec().getDescription();

              var meta = tenants.metaFor(rid);
              long expectedVersion = meta.getPointerVersion();
              enforcePreconditions(correlationId, meta, request.getPrecondition());

              if (desiredName.equals(prev.getDisplayName())
                  && Objects.equals(desiredDesc, prev.getDescription())) {
                return UpdateTenantResponse.newBuilder()
                    .setTenant(prev)
                    .setMeta(tenants.metaForSafe(rid, nowTs()))
                    .build();
              }

              if (!desiredName.equals(prev.getDisplayName())) {
                try {
                  tenants.rename(rid, desiredName, expectedVersion);
                } catch (BaseRepository.NameConflictException nce) {
                  throw GrpcErrors.conflict(
                      correlationId, "tenant.already_exists", Map.of("display_name", desiredName));
                } catch (BaseRepository.PreconditionFailedException pfe) {
                  var now = tenants.metaForSafe(rid);
                  throw GrpcErrors.preconditionFailed(
                      correlationId,
                      "version_mismatch",
                      Map.of(
                          "expected", Long.toString(expectedVersion),
                          "actual", Long.toString(now.getPointerVersion())));
                }

                if (!Objects.equals(desiredDesc, prev.getDescription())) {
                  meta = tenants.metaFor(rid);
                  expectedVersion = meta.getPointerVersion();
                  var renamed = tenants.getById(rid).orElse(prev);
                  var withDesc = renamed.toBuilder().setDescription(desiredDesc).build();
                  try {
                    tenants.update(withDesc, expectedVersion);
                  } catch (BaseRepository.PreconditionFailedException pfe) {
                    var now = tenants.metaForSafe(rid);
                    throw GrpcErrors.preconditionFailed(
                        correlationId,
                        "version_mismatch",
                        Map.of(
                            "expected", Long.toString(expectedVersion),
                            "actual", Long.toString(now.getPointerVersion())));
                  }
                }
              } else {
                var updated = prev.toBuilder().setDescription(desiredDesc).build();
                try {
                  tenants.update(updated, expectedVersion);
                } catch (BaseRepository.PreconditionFailedException pfe) {
                  var now = tenants.metaForSafe(rid);
                  throw GrpcErrors.preconditionFailed(
                      correlationId,
                      "version_mismatch",
                      Map.of(
                          "expected", Long.toString(expectedVersion),
                          "actual", Long.toString(now.getPointerVersion())));
                }
              }

              return UpdateTenantResponse.newBuilder()
                  .setTenant(tenants.getById(rid).orElse(prev))
                  .setMeta(tenants.metaForSafe(rid))
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

              var tenantId = request.getTenantId();
              ensureKind(tenantId, ResourceKind.RK_TENANT, "tenant_id", correlationId);

              if (catalogs.count(tenantId.getId()) > 0) {
                var currentTenant = tenants.getById(tenantId).orElse(null);
                var displayName =
                    (currentTenant != null && !currentTenant.getDisplayName().isBlank())
                        ? currentTenant.getDisplayName()
                        : tenantId.getId();
                throw GrpcErrors.conflict(
                    correlationId, "tenant.not_empty", Map.of("display_name", displayName));
              }

              var meta = tenants.metaFor(tenantId);
              long expectedVersion = meta.getPointerVersion();
              enforcePreconditions(correlationId, meta, request.getPrecondition());

              try {
                tenants.deleteWithPrecondition(tenantId, expectedVersion);
              } catch (BaseRepository.PreconditionFailedException pfe) {
                var now = tenants.metaForSafe(tenantId);
                throw GrpcErrors.preconditionFailed(
                    correlationId,
                    "version_mismatch",
                    Map.of(
                        "expected", Long.toString(expectedVersion),
                        "actual", Long.toString(now.getPointerVersion())));
              } catch (BaseRepository.NotFoundException nfe) {
                throw GrpcErrors.notFound(correlationId, "tenant", Map.of("id", tenantId.getId()));
              }

              return DeleteTenantResponse.newBuilder().setMeta(meta).build();
            }),
        correlationId());
  }
}
