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
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.MutationOps;
import ai.floedb.floecat.service.error.impl.GeneratedErrorMessages;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.StorageAuthorityRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
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
import ai.floedb.floecat.storage.rpc.ResolveStorageAuthorityForAccountLocationRequest;
import ai.floedb.floecat.storage.rpc.ResolveStorageAuthorityForLocationRequest;
import ai.floedb.floecat.storage.rpc.ResolveStorageAuthorityRequest;
import ai.floedb.floecat.storage.rpc.ResolveStorageAuthorityResponse;
import ai.floedb.floecat.storage.rpc.StorageAuthorities;
import ai.floedb.floecat.storage.rpc.StorageAuthority;
import ai.floedb.floecat.storage.rpc.StorageAuthoritySpec;
import ai.floedb.floecat.storage.rpc.UpdateStorageAuthorityRequest;
import ai.floedb.floecat.storage.rpc.UpdateStorageAuthorityResponse;
import ai.floedb.floecat.storage.secrets.SecretsManager;
import com.google.protobuf.FieldMask;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        run(
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
  public Uni<ResolveStorageAuthorityResponse> resolveStorageAuthority(
      ResolveStorageAuthorityRequest request) {
    return mapFailures(
        run(
            () -> {
              PrincipalContext principal = principalProvider.get();
              authz.require(principal, List.of("connector.read", "table.read", "catalog.read"));
              ResourceId tableId =
                  scopedTableId(principal.getAccountId(), request.getTableId(), correlationId());
              String requestedLocationPrefix =
                  request.hasLocationPrefix() ? trimToNull(request.getLocationPrefix()) : null;
              Table tableRecord = loadVisibleTable(tableId);
              String resolvedLocationPrefix = resolveTableLocationPrefix(tableRecord);
              if (resolvedLocationPrefix == null && request.getRequired()) {
                throw new IllegalArgumentException(
                    "Credential vending was requested but no concrete storage location is available for this table");
              }
              if (requestedLocationPrefix != null
                  && resolvedLocationPrefix != null
                  && !StorageAuthorityResolver.matchesLocationPrefix(
                      requestedLocationPrefix, resolvedLocationPrefix)) {
                throw GrpcErrors.invalidArgument(
                    correlationId(), null, Map.of("field", "location_prefix"));
              }
              String locationPrefix = resolvedLocationPrefix;
              List<StorageAuthority> authorities =
                  repo.list(tableId.getAccountId(), Integer.MAX_VALUE, "", new StringBuilder());
              StorageAuthority authority =
                  StorageAuthorityResolver.resolveBest(authorities, locationPrefix).orElse(null);
              return resolver.buildResponse(
                  authority,
                  locationPrefix,
                  tableId.getAccountId(),
                  request.getIncludeCredentials(),
                  request.getRequired(),
                  request.getServerSide());
            }),
        correlationId());
  }

  @Override
  public Uni<ResolveStorageAuthorityResponse> resolveStorageAuthorityForLocation(
      ResolveStorageAuthorityForLocationRequest request) {
    return mapFailures(
        run(
            () -> {
              PrincipalContext principal = principalProvider.get();
              authz.require(principal, RolePermissions.STORAGE_AUTHORITY_RESOLVE_INTERNAL);
              String locationPrefix =
                  request.hasLocationPrefix() ? trimToNull(request.getLocationPrefix()) : null;
              if (locationPrefix == null && request.getRequired()) {
                throw new IllegalArgumentException(
                    "Credential vending was requested but no concrete storage location is available for this table");
              }
              List<StorageAuthority> authorities =
                  repo.list(principal.getAccountId(), Integer.MAX_VALUE, "", new StringBuilder());
              StorageAuthority authority =
                  StorageAuthorityResolver.resolveBest(authorities, locationPrefix).orElse(null);
              return resolver.buildResponse(
                  authority,
                  locationPrefix,
                  principal.getAccountId(),
                  request.getIncludeCredentials(),
                  request.getRequired(),
                  true);
            }),
        correlationId());
  }

  @Override
  public Uni<ResolveStorageAuthorityResponse> resolveStorageAuthorityForAccountLocation(
      ResolveStorageAuthorityForAccountLocationRequest request) {
    return mapFailures(
        run(
            () -> {
              PrincipalContext principal = principalProvider.get();
              authz.require(principal, RolePermissions.STORAGE_AUTHORITY_RESOLVE_INTERNAL);
              String accountId = trimToNull(request.getAccountId());
              if (accountId == null) {
                throw GrpcErrors.invalidArgument(
                    correlationId(), null, Map.of("field", "account_id"));
              }
              String locationPrefix =
                  request.hasLocationPrefix() ? trimToNull(request.getLocationPrefix()) : null;
              if (locationPrefix == null && request.getRequired()) {
                throw new IllegalArgumentException(
                    "Credential vending was requested but no concrete storage location is available for this table");
              }
              List<StorageAuthority> authorities =
                  repo.list(accountId, Integer.MAX_VALUE, "", new StringBuilder());
              StorageAuthority authority =
                  StorageAuthorityResolver.resolveBest(authorities, locationPrefix).orElse(null);
              return resolver.buildResponse(
                  authority,
                  locationPrefix,
                  accountId,
                  request.getIncludeCredentials(),
                  request.getRequired(),
                  true);
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
    if (creating && !hasCredentials(spec)) {
      throw GrpcErrors.invalidArgument(corr, null, Map.of("field", "spec.credentials"));
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

  private static String resolveTableLocationPrefix(Table table) {
    if (table == null) {
      return null;
    }
    String location = resolveStorageUri(table.getPropertiesMap().get("location"));
    if (location != null) {
      return location;
    }
    String storageLocation = resolveStorageUri(table.getPropertiesMap().get("storage_location"));
    if (storageLocation != null) {
      return storageLocation;
    }
    String deltaTableRoot = resolveStorageUri(table.getPropertiesMap().get("delta.table-root"));
    if (deltaTableRoot != null) {
      return deltaTableRoot;
    }
    String externalLocation = resolveStorageUri(table.getPropertiesMap().get("external.location"));
    if (externalLocation != null) {
      return externalLocation;
    }
    String metadataLocation = table.getPropertiesMap().get("metadata-location");
    if (metadataLocation != null && !metadataLocation.isBlank()) {
      int idx = metadataLocation.indexOf("/metadata/");
      if (idx > 0) {
        return metadataLocation.substring(0, idx);
      }
      int slash = metadataLocation.lastIndexOf('/');
      if (slash > 0) {
        return metadataLocation.substring(0, slash);
      }
    }
    String upstreamUri = table.hasUpstream() ? table.getUpstream().getUri() : null;
    return resolveStorageUri(upstreamUri);
  }

  private static String resolveStorageUri(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    String trimmed = value.trim();
    String lower = trimmed.toLowerCase(java.util.Locale.ROOT);
    if (lower.startsWith("s3://")
        || lower.startsWith("s3a://")
        || lower.startsWith("s3n://")
        || lower.startsWith("abfs://")
        || lower.startsWith("abfss://")
        || lower.startsWith("gs://")
        || lower.startsWith("gcs://")
        || lower.startsWith("wasb://")
        || lower.startsWith("wasbs://")
        || lower.startsWith("adl://")
        || lower.startsWith("oss://")
        || lower.startsWith("cos://")
        || lower.startsWith("file://")) {
      return trimmed;
    }
    return null;
  }

  private void storeCredentials(String accountId, String authorityId, StorageAuthoritySpec spec) {
    if (!hasCredentials(spec)) {
      return;
    }
    byte[] payload = spec.getCredentials().toByteArray();
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
