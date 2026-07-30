/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.service.storage.impl;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.AuthCredentials;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.spi.ConnectorConfig;
import ai.floedb.floecat.connector.spi.ConnectorConfigMapper;
import ai.floedb.floecat.reconciler.impl.ReconcileLeaseGrpcStatus;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.MutationOps;
import ai.floedb.floecat.service.error.impl.GeneratedErrorMessages;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.StorageAuthorityRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.security.RolePermissions;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.storage.rpc.CreateStorageAuthorityRequest;
import ai.floedb.floecat.storage.rpc.CreateStorageAuthorityResponse;
import ai.floedb.floecat.storage.rpc.DeleteStorageAuthorityRequest;
import ai.floedb.floecat.storage.rpc.DeleteStorageAuthorityResponse;
import ai.floedb.floecat.storage.rpc.GetStorageAuthorityRequest;
import ai.floedb.floecat.storage.rpc.GetStorageAuthorityResponse;
import ai.floedb.floecat.storage.rpc.ListStorageAuthoritiesRequest;
import ai.floedb.floecat.storage.rpc.ListStorageAuthoritiesResponse;
import ai.floedb.floecat.storage.rpc.ResolveSnapshotCompatStorageRequest;
import ai.floedb.floecat.storage.rpc.ResolveSnapshotCompatStorageResponse;
import ai.floedb.floecat.storage.rpc.ResolveStorageAuthorityResponse;
import ai.floedb.floecat.storage.rpc.StorageAuthorities;
import ai.floedb.floecat.storage.rpc.StorageAuthority;
import ai.floedb.floecat.storage.rpc.StorageAuthoritySpec;
import ai.floedb.floecat.storage.rpc.StorageCredentialUsage;
import ai.floedb.floecat.storage.rpc.UpdateStorageAuthorityRequest;
import ai.floedb.floecat.storage.rpc.UpdateStorageAuthorityResponse;
import ai.floedb.floecat.storage.rpc.VendStorageCredentialsRequest;
import ai.floedb.floecat.storage.secrets.SecretsManager;
import com.google.protobuf.FieldMask;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@GrpcService
public class StorageAuthorityServiceImpl extends BaseServiceImpl implements StorageAuthorities {
  private static final Set<String> MUTABLE_PATHS =
      Set.of(
          "display_name",
          "description",
          "enabled",
          "type",
          "location_prefix",
          "region",
          "endpoint",
          "path_style_access",
          "assume_role_arn",
          "assume_role_external_id",
          "assume_role_session_name",
          "duration_seconds",
          "credentials");

  @Inject StorageAuthorityRepository repo;
  @Inject PrincipalProvider principalProvider;
  @Inject Authorizer authz;
  @Inject IdempotencyRepository idempotencyStore;
  @Inject StorageAuthorityResolver resolver;
  @Inject SecretsManager secretsManager;
  @Inject TableRepository tableRepo;
  @Inject ConnectorRepository connectorRepo;
  @Inject SnapshotRepository snapshotRepo;
  @Inject ReconcileJobStore reconcileJobs;

  @ConfigProperty(name = "floecat.blob")
  String blobStoreType;

  @ConfigProperty(name = "floecat.blob.s3.bucket")
  String blobBucket;

  @ConfigProperty(name = "floecat.storage.aws.region", defaultValue = "us-east-1")
  String storageAwsRegion;

  @ConfigProperty(name = "floecat.storage.aws.s3.endpoint")
  java.util.Optional<String> storageAwsS3Endpoint;

  @ConfigProperty(name = "floecat.storage.aws.s3.path-style-access", defaultValue = "true")
  boolean storageAwsPathStyleAccess;

  @Override
  public Uni<ListStorageAuthoritiesResponse> listStorageAuthorities(
      ListStorageAuthoritiesRequest request) {
    return mapFailures(
        run(
            () -> {
              PrincipalContext principal = principalProvider.get();
              authz.require(principal, "connector.manage");
              var pageIn = MutationOps.pageIn(request.hasPage() ? request.getPage() : null);
              StringBuilder next = new StringBuilder();
              List<StorageAuthority> authorities =
                  repo.list(principal.getAccountId(), pageIn.limit, pageIn.token, next);
              return ListStorageAuthoritiesResponse.newBuilder()
                  .addAllAuthorities(authorities)
                  .setPage(
                      MutationOps.pageOut(next.toString(), repo.count(principal.getAccountId())))
                  .build();
            }),
        correlationId());
  }

  @Override
  public Uni<GetStorageAuthorityResponse> getStorageAuthority(GetStorageAuthorityRequest request) {
    return mapFailures(
        run(
            () -> {
              PrincipalContext principal = principalProvider.get();
              authz.require(principal, "connector.manage");
              ResourceId authorityId =
                  scopedStorageAuthorityId(
                      principal.getAccountId(), request.getAuthorityId(), correlationId());
              StorageAuthority authority =
                  repo.getById(authorityId)
                      .orElseThrow(
                          () ->
                              new BaseResourceRepository.NotFoundException(
                                  "storage authority not found"));
              return GetStorageAuthorityResponse.newBuilder().setAuthority(authority).build();
            }),
        correlationId());
  }

  @Override
  public Uni<CreateStorageAuthorityResponse> createStorageAuthority(
      CreateStorageAuthorityRequest request) {
    return mapFailures(
        run(
            () -> {
              PrincipalContext principal = principalProvider.get();
              authz.require(principal, List.of("connector.manage", "connector.create"));
              StorageAuthoritySpec spec = request.getSpec();
              validateSpec(spec, true, correlationId());
              String accountId = principal.getAccountId();
              String idempotencyKey =
                  request.hasIdempotency() ? request.getIdempotency().getKey() : "";
              var op =
                  MutationOps.createProto(
                      accountId,
                      "create-storage-authority",
                      idempotencyKey,
                      () -> spec.toByteArray(),
                      () -> {
                        ResourceId authorityId =
                            randomResourceId(accountId, ResourceKind.RK_STORAGE_AUTHORITY);
                        StorageAuthority authority =
                            buildAuthority(authorityId, spec, null, nowTs());
                        repo.create(authority);
                        storeCredentials(accountId, authorityId.getId(), spec);
                        return new ai.floedb.floecat.service.common.IdempotencyGuard.CreateResult<>(
                            authority, authorityId);
                      },
                      authority -> repo.metaFor(authority.getResourceId()),
                      idempotencyStore,
                      nowTs(),
                      idempotencyTtlSeconds(),
                      this::correlationId,
                      StorageAuthority::parseFrom);
              return CreateStorageAuthorityResponse.newBuilder()
                  .setAuthority(op.body)
                  .setMeta(op.meta)
                  .build();
            }),
        correlationId());
  }

  @Override
  public Uni<UpdateStorageAuthorityResponse> updateStorageAuthority(
      UpdateStorageAuthorityRequest request) {
    return mapFailures(
        run(
            () -> {
              PrincipalContext principal = principalProvider.get();
              authz.require(principal, "connector.manage");
              ResourceId authorityId =
                  scopedStorageAuthorityId(
                      principal.getAccountId(), request.getAuthorityId(), correlationId());
              StorageAuthority current =
                  repo.getById(authorityId)
                      .orElseThrow(
                          () ->
                              new BaseResourceRepository.NotFoundException(
                                  "storage authority not found"));
              validateUpdateMask(request.getUpdateMask(), correlationId());
              StorageAuthoritySpec desiredSpec =
                  mergeSpec(current, request.getSpec(), request.getUpdateMask());
              boolean credentialsTouched = credentialsTouched(request.getUpdateMask());
              validateSpec(desiredSpec, false, correlationId());
              MutationOps.updateWithPreconditions(
                  () -> repo.metaFor(authorityId),
                  request.getPrecondition(),
                  expectedVersion -> {
                    StorageAuthority updated =
                        buildAuthority(authorityId, desiredSpec, current, nowTs());
                    boolean ok = repo.update(updated, expectedVersion);
                    if (ok && credentialsTouched) {
                      if (hasCredentials(desiredSpec)) {
                        storeCredentials(
                            authorityId.getAccountId(), authorityId.getId(), desiredSpec);
                      } else {
                        secretsManager.delete(
                            authorityId.getAccountId(),
                            StorageAuthorityResolver.STORAGE_AUTHORITY_SECRET_TYPE,
                            authorityId.getId());
                      }
                    }
                    return ok;
                  },
                  () -> repo.metaFor(authorityId),
                  correlationId(),
                  "storage-authority",
                  Map.of("display_name", current.getDisplayName()));
              StorageAuthority updated =
                  repo.getById(authorityId)
                      .orElseThrow(
                          () ->
                              new BaseResourceRepository.NotFoundException(
                                  "storage authority not found"));
              return UpdateStorageAuthorityResponse.newBuilder()
                  .setAuthority(updated)
                  .setMeta(repo.metaFor(authorityId))
                  .build();
            }),
        correlationId());
  }

  @Override
  public Uni<DeleteStorageAuthorityResponse> deleteStorageAuthority(
      DeleteStorageAuthorityRequest request) {
    return mapFailures(
        runWithRetry(
            () -> {
              PrincipalContext principal = principalProvider.get();
              authz.require(principal, "connector.manage");
              ResourceId authorityId =
                  scopedStorageAuthorityId(
                      principal.getAccountId(), request.getAuthorityId(), correlationId());
              MutationMeta meta =
                  MutationOps.deleteWithPreconditions(
                      () -> repo.metaFor(authorityId),
                      request.getPrecondition(),
                      expectedVersion -> repo.deleteWithPrecondition(authorityId, expectedVersion),
                      () -> repo.metaForSafe(authorityId),
                      correlationId(),
                      "storage-authority",
                      Map.of("authority_id", authorityId.getId()));
              secretsManager.delete(
                  authorityId.getAccountId(),
                  StorageAuthorityResolver.STORAGE_AUTHORITY_SECRET_TYPE,
                  authorityId.getId());
              return DeleteStorageAuthorityResponse.newBuilder().setMeta(meta).build();
            }),
        correlationId());
  }

  @Override
  public Uni<ResolveStorageAuthorityResponse> vendStorageCredentials(
      VendStorageCredentialsRequest request) {
    return mapFailures(
        run(
            () -> {
              PrincipalContext principal = principalProvider.get();
              String accountId = trimToNull(request.getAccountId());
              if (accountId == null) {
                throw GrpcErrors.invalidArgument(
                    correlationId(), null, Map.of("field", "account_id"));
              }
              boolean serverSide = usage(request) == StorageCredentialUsage.SCU_SERVER;
              CredentialScope credentialScope =
                  authorizeAndResolveLocation(principal, request, accountId);
              List<StorageAuthority> authorities =
                  repo.list(accountId, Integer.MAX_VALUE, "", new StringBuilder());
              StorageAuthority authority =
                  StorageAuthorityResolver.resolveBest(
                          authorities, credentialScope.authorityLookupLocationPrefix())
                      .orElse(null);
              validateAuthorityCoversSessionScope(
                  authority, credentialScope.sessionScopeLocations());
              return resolver.buildResponse(
                  authority,
                  credentialScope.responseLocationPrefix(),
                  credentialScope.sessionScopeLocations(),
                  accountId,
                  serverSide);
            }),
        correlationId());
  }

  @Override
  public Uni<ResolveSnapshotCompatStorageResponse> resolveSnapshotCompatStorage(
      ResolveSnapshotCompatStorageRequest request) {
    return mapFailures(
        run(
            () -> {
              PrincipalContext principal = principalProvider.get();
              authz.require(principal, List.of("connector.read", "table.read", "catalog.read"));
              ResourceId tableId =
                  scopedTableId(principal.getAccountId(), request.getTableId(), correlationId());
              long snapshotId = request.getSnapshotId();
              if (snapshotId < 0) {
                throw GrpcErrors.invalidArgument(
                    correlationId(), null, Map.of("field", "snapshot_id"));
              }
              loadVisibleTable(tableId);
              snapshotRepo
                  .getById(tableId, snapshotId)
                  .orElseThrow(
                      () ->
                          GrpcErrors.notFound(
                              correlationId(),
                              GeneratedErrorMessages.MessageKey.SNAPSHOT,
                              Map.of("id", Long.toString(snapshotId))));
              String locationPrefix = resolveSnapshotCompatLocationPrefix(tableId, snapshotId);
              ResolveStorageAuthorityResponse storage;
              if ("memory".equalsIgnoreCase(trimToNull(blobStoreType))) {
                storage = ResolveStorageAuthorityResponse.getDefaultInstance();
              } else {
                storage = resolveSnapshotCompatStorageSettings(locationPrefix);
              }
              return ResolveSnapshotCompatStorageResponse.newBuilder()
                  .setLocationPrefix(locationPrefix)
                  .setStorage(storage)
                  .build();
            }),
        correlationId());
  }

  private static void validateUpdateMask(FieldMask mask, String corr) {
    if (mask == null || mask.getPathsCount() == 0) {
      return;
    }
    for (String path : mask.getPathsList()) {
      if (!MUTABLE_PATHS.contains(path)) {
        throw GrpcErrors.invalidArgument(corr, null, Map.of("field", path));
      }
    }
  }

  private static boolean credentialsTouched(FieldMask mask) {
    // A full replace (no update mask) treats omitted credentials as an explicit removal.
    return mask == null || mask.getPathsCount() == 0 || mask.getPathsList().contains("credentials");
  }

  private static void validateSpec(StorageAuthoritySpec spec, boolean creating, String corr) {
    if (spec == null) {
      throw GrpcErrors.invalidArgument(corr, null, Map.of("field", "spec"));
    }
    if (trimToNull(spec.getDisplayName()) == null) {
      throw GrpcErrors.invalidArgument(corr, null, Map.of("field", "spec.display_name"));
    }
    if (trimToNull(spec.getLocationPrefix()) == null) {
      throw GrpcErrors.invalidArgument(corr, null, Map.of("field", "spec.location_prefix"));
    }
    if (creating
        && !hasCredentials(spec)
        && (trimToNull(spec.hasAssumeRoleArn() ? spec.getAssumeRoleArn() : null) == null)) {
      throw GrpcErrors.invalidArgument(
          corr, null, Map.of("field", "spec.credentials|spec.assume_role_arn"));
    }
  }

  private static boolean hasCredentials(StorageAuthoritySpec spec) {
    return spec != null
        && spec.hasCredentials()
        && spec.getCredentials().getCredentialCase()
            != AuthCredentials.CredentialCase.CREDENTIAL_NOT_SET;
  }

  private ResourceId scopedStorageAuthorityId(
      String accountId, ResourceId authorityId, String corr) {
    ensureKind(authorityId, ResourceKind.RK_STORAGE_AUTHORITY, "authority_id", corr);
    return authorityId.toBuilder().setAccountId(accountId).build();
  }

  private ResourceId scopedTableId(String accountId, ResourceId tableId, String corr) {
    ensureKind(tableId, ResourceKind.RK_TABLE, "table_id", corr);
    return tableId.toBuilder().setAccountId(accountId).build();
  }

  private Table loadVisibleTable(ResourceId tableId) {
    return tableRepo
        .getById(tableId)
        .orElseThrow(
            () ->
                GrpcErrors.notFound(
                    correlationId(),
                    GeneratedErrorMessages.MessageKey.TABLE,
                    Map.of("id", tableId.getId())));
  }

  private String resolveTableLocationPrefix(Table table) {
    return TableStorageLocationResolver.resolveTableLocation(table, snapshotRepo);
  }

  private CredentialScope authorizeAndResolveLocation(
      PrincipalContext principal, VendStorageCredentialsRequest request, String accountId) {
    if (request.hasExecutionBinding() && request.getExecutionBinding().hasReconcileLease()) {
      authz.require(principal, RolePermissions.STORAGE_AUTHORITY_RESOLVE_INTERNAL);
      return resolveExecutionBoundLocation(request, validateExecutionLease(request, accountId));
    }
    if (request.hasTableId()) {
      authz.require(principal, List.of("connector.read", "table.read", "catalog.read"));
      ResourceId tableId =
          scopedTableId(principal.getAccountId(), request.getTableId(), correlationId());
      if (!accountId.equals(tableId.getAccountId())) {
        throw GrpcErrors.invalidArgument(correlationId(), null, Map.of("field", "account_id"));
      }
      String locationPrefix = resolveTableScopedLocation(request, tableId);
      return CredentialScope.forSingleLocation(locationPrefix);
    }
    authz.require(principal, RolePermissions.STORAGE_AUTHORITY_RESOLVE_INTERNAL);
    return CredentialScope.forSingleLocation(validateExplicitLocation(request));
  }

  private String resolveTableScopedLocation(
      VendStorageCredentialsRequest request, ResourceId tableId) {
    String requestedLocationPrefix =
        request.hasLocationPrefix() ? trimToNull(request.getLocationPrefix()) : null;
    Table tableRecord = loadVisibleTable(tableId);
    String resolvedLocationPrefix = resolveTableLocationPrefix(tableRecord);
    if (resolvedLocationPrefix == null) {
      throw new IllegalArgumentException(
          "Credential vending was requested but no concrete storage location is available for this table");
    }
    if (requestedLocationPrefix != null
        && resolvedLocationPrefix != null
        && !StorageAuthorityResolver.matchesLocationPrefix(
            requestedLocationPrefix, resolvedLocationPrefix)) {
      throw GrpcErrors.invalidArgument(correlationId(), null, Map.of("field", "location_prefix"));
    }
    return requestedLocationPrefix != null ? requestedLocationPrefix : resolvedLocationPrefix;
  }

  private String validateExplicitLocation(VendStorageCredentialsRequest request) {
    String locationPrefix =
        request.hasLocationPrefix() ? trimToNull(request.getLocationPrefix()) : null;
    if (locationPrefix == null) {
      throw new IllegalArgumentException(
          "Credential vending was requested but no concrete storage location is available for this table");
    }
    return locationPrefix;
  }

  private CredentialScope resolveExecutionBoundLocation(
      VendStorageCredentialsRequest request, ReconcileJobStore.ReconcileJob job) {
    String requestedLocationPrefix = validateExplicitLocation(request);
    CredentialScope scope = resolveLeaseScopedLocation(job);
    if ((scope == null || scope.authorityLookupLocationPrefix() == null) && request.hasTableId()) {
      scope = resolveDiscoveryTableLocation(request.getTableId(), job);
    }
    if (scope == null || scope.authorityLookupLocationPrefix() == null) {
      scope = resolvePlannerBootstrapLocation(requestedLocationPrefix, job);
    }
    if (scope == null || scope.authorityLookupLocationPrefix() == null) {
      throw io.grpc.Status.FAILED_PRECONDITION
          .withDescription("reconcile lease is not bound to a concrete storage location")
          .asRuntimeException();
    }
    if (requestedLocationPrefix == null) {
      return scope;
    }
    if (!isWithinExecutionScope(requestedLocationPrefix, scope)) {
      throw io.grpc.Status.PERMISSION_DENIED
          .withDescription("requested location is outside the leased reconcile storage scope")
          .asRuntimeException();
    }
    return scope.withResponseLocationPrefix(requestedLocationPrefix);
  }

  private static StorageCredentialUsage usage(VendStorageCredentialsRequest request) {
    if (request == null || request.getUsage() == StorageCredentialUsage.SCU_UNSPECIFIED) {
      return StorageCredentialUsage.SCU_CLIENT;
    }
    return request.getUsage();
  }

  private ReconcileJobStore.ReconcileJob validateExecutionLease(
      VendStorageCredentialsRequest request, String accountId) {
    if (request == null) {
      return null;
    }
    if (!request.hasExecutionBinding() || !request.getExecutionBinding().hasReconcileLease()) {
      return null;
    }
    String jobId = trimToNull(request.getExecutionBinding().getReconcileLease().getJobId());
    String leaseEpoch =
        trimToNull(request.getExecutionBinding().getReconcileLease().getLeaseEpoch());
    if (jobId == null || leaseEpoch == null) {
      throw GrpcErrors.invalidArgument(correlationId(), null, Map.of("field", "execution_binding"));
    }
    boolean renewed = reconcileJobs.renewLease(jobId, leaseEpoch);
    if (!renewed) {
      throw ReconcileLeaseGrpcStatus.leasePreconditionFailed("reconcile lease is no longer valid");
    }
    ReconcileJobStore.ReconcileJob job =
        reconcileJobs
            .getLeaseView(jobId)
            .orElseThrow(
                () ->
                    ReconcileLeaseGrpcStatus.leasePreconditionFailed(
                        "reconcile job not found: " + jobId));
    if (!accountId.equals(job.accountId)) {
      throw io.grpc.Status.PERMISSION_DENIED
          .withDescription("reconcile lease account does not match requested account")
          .asRuntimeException();
    }
    if (!isActiveLeasedState(job.state)) {
      throw ReconcileLeaseGrpcStatus.leasePreconditionFailed(
          "reconcile job is no longer active for lease "
              + jobId
              + " state="
              + (job.state == null ? "" : job.state));
    }
    return job;
  }

  private static boolean isActiveLeasedState(String state) {
    return "JS_LEASED".equals(state) || "JS_RUNNING".equals(state) || "JS_CANCELLING".equals(state);
  }

  private CredentialScope resolveLeaseScopedLocation(ReconcileJobStore.ReconcileJob job) {
    if (job == null) {
      return null;
    }
    String tableId = leasedTableId(job);
    if (tableId == null) {
      return null;
    }
    String locationPrefix =
        resolveTableLocationPrefix(
            loadVisibleTable(
                ResourceId.newBuilder()
                    .setAccountId(job.accountId)
                    .setKind(ResourceKind.RK_TABLE)
                    .setId(tableId)
                    .build()));
    return CredentialScope.forSingleLocation(locationPrefix);
  }

  private CredentialScope resolveDiscoveryTableLocation(
      ResourceId requestedTableId, ReconcileJobStore.ReconcileJob job) {
    if (job == null
        || job.jobKind != ReconcileJobKind.PLAN_TABLE
        || job.tableTask == null
        || !job.tableTask.discoveryMode()
        || (job.tableTask.destinationTableId() != null
            && !job.tableTask.destinationTableId().isBlank())) {
      return null;
    }
    ResourceId tableId = scopedTableId(job.accountId, requestedTableId, correlationId());
    Table table = loadVisibleTable(tableId);
    if (!table.hasUpstream()
        || !table.getUpstream().hasConnectorId()
        || !job.connectorId.equals(table.getUpstream().getConnectorId().getId())
        || !job.tableTask
            .sourceNamespace()
            .equals(String.join(".", table.getUpstream().getNamespacePathList()))
        || !job.tableTask.sourceTable().equals(table.getUpstream().getTableDisplayName())) {
      throw io.grpc.Status.PERMISSION_DENIED
          .withDescription("requested table does not match the leased reconcile table")
          .asRuntimeException();
    }
    return CredentialScope.forSingleLocation(resolveTableLocationPrefix(table));
  }

  private CredentialScope resolvePlannerBootstrapLocation(
      String requestedLocationPrefix, ReconcileJobStore.ReconcileJob job) {
    if (job == null || !allowsConnectorBootstrapScope(job)) {
      return null;
    }
    ResourceId connectorId =
        ResourceId.newBuilder()
            .setAccountId(job.accountId)
            .setKind(ResourceKind.RK_CONNECTOR)
            .setId(job.connectorId)
            .build();
    Connector connector =
        connectorRepo
            .getById(connectorId)
            .orElseThrow(
                () ->
                    io.grpc.Status.NOT_FOUND
                        .withDescription("reconcile connector not found: " + job.connectorId)
                        .asRuntimeException());
    String bootstrapLocation = connectorSourceStorageLocation(connector);
    if (bootstrapLocation == null) {
      return null;
    }
    if (!StorageAuthorityResolver.matchesLocationPrefix(
        requestedLocationPrefix, bootstrapLocation)) {
      throw io.grpc.Status.PERMISSION_DENIED
          .withDescription("requested location is outside the leased reconcile connector scope")
          .asRuntimeException();
    }
    return CredentialScope.forSingleLocation(bootstrapLocation);
  }

  private static boolean allowsConnectorBootstrapScope(ReconcileJobStore.ReconcileJob job) {
    if (job.jobKind == ReconcileJobKind.PLAN_VIEW) {
      return true;
    }
    return job.jobKind == ReconcileJobKind.PLAN_TABLE
        && job.tableTask != null
        && job.tableTask.discoveryMode()
        && (job.tableTask.destinationTableId() == null
            || job.tableTask.destinationTableId().isBlank());
  }

  private static String connectorSourceStorageLocation(Connector connector) {
    if (connector == null || !connector.hasResourceId()) {
      return null;
    }
    ConnectorConfig config = ConnectorConfigMapper.fromProto(connector);
    if (config.kind() == ConnectorConfig.Kind.DELTA) {
      String location = trimToNull(config.options().get("storage_location"));
      return location != null ? location : trimToNull(config.options().get("delta.table-root"));
    }
    if (config.kind() == ConnectorConfig.Kind.ICEBERG
        && "filesystem".equalsIgnoreCase(trimToNull(config.options().get("iceberg.source")))) {
      return trimToNull(config.uri());
    }
    return null;
  }

  private static boolean isWithinExecutionScope(
      String requestedLocationPrefix, CredentialScope scope) {
    if (requestedLocationPrefix == null || scope == null) {
      return false;
    }
    List<String> sessionScopes = scope.sessionScopeLocations();
    if (sessionScopes == null || sessionScopes.isEmpty()) {
      return StorageAuthorityResolver.matchesLocationPrefix(
          requestedLocationPrefix, scope.authorityLookupLocationPrefix());
    }
    for (String sessionScope : sessionScopes) {
      if (StorageAuthorityResolver.matchesLocationPrefix(sessionScope, requestedLocationPrefix)
          || StorageAuthorityResolver.matchesLocationPrefix(
              requestedLocationPrefix, sessionScope)) {
        return true;
      }
    }
    return false;
  }

  private static void validateAuthorityCoversSessionScope(
      StorageAuthority authority, List<String> sessionScopeLocations) {
    if (authority == null
        || sessionScopeLocations == null
        || sessionScopeLocations.isEmpty()
        || authority.getLocationPrefix().isBlank()) {
      return;
    }
    for (String sessionScopeLocation : sessionScopeLocations) {
      if (!StorageAuthorityResolver.matchesLocationPrefix(
          sessionScopeLocation, authority.getLocationPrefix())) {
        throw io.grpc.Status.FAILED_PRECONDITION
            .withDescription("leased reconcile storage scope spans multiple storage authorities")
            .asRuntimeException();
      }
    }
  }

  private static String leasedTableId(ReconcileJobStore.ReconcileJob job) {
    if (job == null) {
      return null;
    }
    if (job.fileGroupTask != null && !job.fileGroupTask.tableId().isBlank()) {
      return job.fileGroupTask.tableId();
    }
    if (job.snapshotTask != null && !job.snapshotTask.tableId().isBlank()) {
      return job.snapshotTask.tableId();
    }
    if (job.tableTask != null
        && job.tableTask.destinationTableId() != null
        && !job.tableTask.destinationTableId().isBlank()) {
      return job.tableTask.destinationTableId();
    }
    return null;
  }

  private static boolean isExecutionBound(VendStorageCredentialsRequest request) {
    return request != null
        && request.hasExecutionBinding()
        && request.getExecutionBinding().hasReconcileLease();
  }

  private String resolveSnapshotCompatLocationPrefix(ResourceId tableId, long snapshotId) {
    String normalizedBlobType = trimToNull(blobStoreType);
    if (!"memory".equalsIgnoreCase(normalizedBlobType)
        && !"s3".equalsIgnoreCase(normalizedBlobType)) {
      throw new IllegalStateException("Snapshot compat storage requires floecat.blob=memory or s3");
    }
    String bucket = trimToNull(blobBucket);
    if (bucket == null) {
      throw new IllegalStateException("Snapshot compat storage requires floecat.blob.s3.bucket");
    }
    String keyPrefix =
        Keys.snapshotCompatIcebergRestPrefix(tableId.getAccountId(), tableId.getId(), snapshotId);
    return "s3://" + bucket + keyPrefix;
  }

  private ResolveStorageAuthorityResponse resolveSnapshotCompatStorageSettings(
      String locationPrefix) {
    StorageAuthority authority =
        StorageAuthority.newBuilder()
            .setType("s3")
            .setLocationPrefix(locationPrefix == null ? "" : locationPrefix)
            .setRegion(
                trimToNull(storageAwsRegion) == null ? "us-east-1" : trimToNull(storageAwsRegion))
            .setPathStyleAccess(storageAwsPathStyleAccess)
            .build();
    String endpoint =
        storageAwsS3Endpoint == null ? null : trimToNull(storageAwsS3Endpoint.orElse(null));
    if (endpoint != null) {
      authority = authority.toBuilder().setEndpoint(endpoint).build();
    }
    return ResolveStorageAuthorityResponse.newBuilder()
        .putAllClientSafeConfig(resolver.clientSafeConfig(authority))
        .build();
  }

  private void storeCredentials(String accountId, String authorityId, StorageAuthoritySpec spec) {
    if (!hasCredentials(spec)) {
      return;
    }
    storeCredentials(secretsManager, accountId, authorityId, spec.getCredentials());
  }

  private record CredentialScope(
      String authorityLookupLocationPrefix,
      String responseLocationPrefix,
      List<String> sessionScopeLocations) {
    static CredentialScope forSingleLocation(String locationPrefix) {
      return new CredentialScope(
          locationPrefix,
          locationPrefix,
          locationPrefix == null ? List.of() : List.of(locationPrefix));
    }

    CredentialScope withResponseLocationPrefix(String responseLocationPrefix) {
      return new CredentialScope(
          authorityLookupLocationPrefix, responseLocationPrefix, sessionScopeLocations);
    }
  }

  public static void storeCredentials(
      SecretsManager secretsManager,
      String accountId,
      String authorityId,
      AuthCredentials credentials) {
    if (secretsManager == null
        || accountId == null
        || accountId.isBlank()
        || authorityId == null
        || authorityId.isBlank()
        || credentials == null) {
      return;
    }
    byte[] payload = credentials.toByteArray();
    boolean exists =
        secretsManager
            .get(accountId, StorageAuthorityResolver.STORAGE_AUTHORITY_SECRET_TYPE, authorityId)
            .isPresent();
    if (exists) {
      secretsManager.update(
          accountId, StorageAuthorityResolver.STORAGE_AUTHORITY_SECRET_TYPE, authorityId, payload);
    } else {
      secretsManager.put(
          accountId, StorageAuthorityResolver.STORAGE_AUTHORITY_SECRET_TYPE, authorityId, payload);
    }
  }

  private StorageAuthority buildAuthority(
      ResourceId authorityId,
      StorageAuthoritySpec spec,
      StorageAuthority prior,
      com.google.protobuf.Timestamp now) {
    StorageAuthority.Builder builder =
        StorageAuthority.newBuilder()
            .setResourceId(authorityId)
            .setDisplayName(spec.getDisplayName())
            .setEnabled(spec.getEnabled())
            .setType(trimToNull(spec.getType()) == null ? "s3" : spec.getType())
            .setLocationPrefix(spec.getLocationPrefix())
            .setCreatedAt(prior != null ? prior.getCreatedAt() : now)
            .setUpdatedAt(now);
    if (spec.hasDescription()) {
      builder.setDescription(spec.getDescription());
    }
    if (spec.hasRegion()) {
      builder.setRegion(spec.getRegion());
    }
    if (spec.hasEndpoint()) {
      builder.setEndpoint(spec.getEndpoint());
    }
    if (spec.hasPathStyleAccess()) {
      builder.setPathStyleAccess(spec.getPathStyleAccess());
    }
    if (spec.hasAssumeRoleArn()) {
      builder.setAssumeRoleArn(spec.getAssumeRoleArn());
    }
    if (spec.hasAssumeRoleExternalId()) {
      builder.setAssumeRoleExternalId(spec.getAssumeRoleExternalId());
    }
    if (spec.hasAssumeRoleSessionName()) {
      builder.setAssumeRoleSessionName(spec.getAssumeRoleSessionName());
    }
    if (spec.hasDurationSeconds()) {
      builder.setDurationSeconds(spec.getDurationSeconds());
    }
    return builder.build();
  }

  private StorageAuthoritySpec mergeSpec(
      StorageAuthority current, StorageAuthoritySpec patch, FieldMask mask) {
    StorageAuthoritySpec.Builder builder =
        StorageAuthoritySpec.newBuilder()
            .setDisplayName(current.getDisplayName())
            .setEnabled(current.getEnabled())
            .setType(current.getType())
            .setLocationPrefix(current.getLocationPrefix());
    if (current.hasDescription()) {
      builder.setDescription(current.getDescription());
    }
    if (current.hasRegion()) {
      builder.setRegion(current.getRegion());
    }
    if (current.hasEndpoint()) {
      builder.setEndpoint(current.getEndpoint());
    }
    if (current.hasPathStyleAccess()) {
      builder.setPathStyleAccess(current.getPathStyleAccess());
    }
    if (current.hasAssumeRoleArn()) {
      builder.setAssumeRoleArn(current.getAssumeRoleArn());
    }
    if (current.hasAssumeRoleExternalId()) {
      builder.setAssumeRoleExternalId(current.getAssumeRoleExternalId());
    }
    if (current.hasAssumeRoleSessionName()) {
      builder.setAssumeRoleSessionName(current.getAssumeRoleSessionName());
    }
    if (current.hasDurationSeconds()) {
      builder.setDurationSeconds(current.getDurationSeconds());
    }
    if (mask == null || mask.getPathsCount() == 0) {
      return patch;
    }
    for (String path : mask.getPathsList()) {
      switch (path) {
        case "display_name" -> builder.setDisplayName(patch.getDisplayName());
        case "description" -> {
          builder.clearDescription();
          if (patch.hasDescription()) {
            builder.setDescription(patch.getDescription());
          }
        }
        case "enabled" -> builder.setEnabled(patch.getEnabled());
        case "type" -> {
          builder.clearType();
          if (patch.hasType()) {
            builder.setType(patch.getType());
          }
        }
        case "location_prefix" -> builder.setLocationPrefix(patch.getLocationPrefix());
        case "region" -> {
          builder.clearRegion();
          if (patch.hasRegion()) {
            builder.setRegion(patch.getRegion());
          }
        }
        case "endpoint" -> {
          builder.clearEndpoint();
          if (patch.hasEndpoint()) {
            builder.setEndpoint(patch.getEndpoint());
          }
        }
        case "path_style_access" -> {
          builder.clearPathStyleAccess();
          if (patch.hasPathStyleAccess()) {
            builder.setPathStyleAccess(patch.getPathStyleAccess());
          }
        }
        case "assume_role_arn" -> {
          builder.clearAssumeRoleArn();
          if (patch.hasAssumeRoleArn()) {
            builder.setAssumeRoleArn(patch.getAssumeRoleArn());
          }
        }
        case "assume_role_external_id" -> {
          builder.clearAssumeRoleExternalId();
          if (patch.hasAssumeRoleExternalId()) {
            builder.setAssumeRoleExternalId(patch.getAssumeRoleExternalId());
          }
        }
        case "assume_role_session_name" -> {
          builder.clearAssumeRoleSessionName();
          if (patch.hasAssumeRoleSessionName()) {
            builder.setAssumeRoleSessionName(patch.getAssumeRoleSessionName());
          }
        }
        case "duration_seconds" -> {
          builder.clearDurationSeconds();
          if (patch.hasDurationSeconds()) {
            builder.setDurationSeconds(patch.getDurationSeconds());
          }
        }
        case "credentials" -> {
          builder.clearCredentials();
          if (patch.hasCredentials()) {
            builder.setCredentials(patch.getCredentials());
          }
        }
        default -> throw new IllegalArgumentException("Unsupported update path: " + path);
      }
    }
    return builder.build();
  }
}
