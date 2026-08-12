/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package ai.floedb.floecat.service.integration;

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.CATALOG_INTEGRATION;
import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.CATALOG_INTEGRATION_ALREADY_EXISTS;
import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.CATALOG_INTEGRATION_CHANGED;
import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.CATALOG_INTEGRATION_DELETION_IN_PROGRESS;
import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.FIELD;
import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.SELECTOR_REQUIRED;
import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.UPDATE_MASK_REQUIRED;

import ai.floedb.floecat.common.rpc.CreateMode;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.integration.rpc.AwsAssumeRoleAuthentication;
import ai.floedb.floecat.integration.rpc.CatalogAuthentication;
import ai.floedb.floecat.integration.rpc.CatalogIntegration;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationCredentials;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationEntry;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationSpec;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationType;
import ai.floedb.floecat.integration.rpc.CatalogIntegrations;
import ai.floedb.floecat.integration.rpc.CreateCatalogIntegrationRequest;
import ai.floedb.floecat.integration.rpc.CreateCatalogIntegrationResponse;
import ai.floedb.floecat.integration.rpc.DeleteCatalogIntegrationRequest;
import ai.floedb.floecat.integration.rpc.DeleteCatalogIntegrationResponse;
import ai.floedb.floecat.integration.rpc.GetCatalogIntegrationRequest;
import ai.floedb.floecat.integration.rpc.GetCatalogIntegrationResponse;
import ai.floedb.floecat.integration.rpc.ListCatalogIntegrationsRequest;
import ai.floedb.floecat.integration.rpc.ListCatalogIntegrationsResponse;
import ai.floedb.floecat.integration.rpc.UpdateCatalogIntegrationAuthenticationRequest;
import ai.floedb.floecat.integration.rpc.UpdateCatalogIntegrationAuthenticationResponse;
import ai.floedb.floecat.integration.rpc.UpdateCatalogIntegrationRequest;
import ai.floedb.floecat.integration.rpc.UpdateCatalogIntegrationResponse;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.Canonicalizer;
import ai.floedb.floecat.service.common.IdempotencyGuard;
import ai.floedb.floecat.service.common.LogHelper;
import ai.floedb.floecat.service.common.MutationOps;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.CatalogIntegrationRepository;
import ai.floedb.floecat.service.repo.impl.CatalogOverlayRepository;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.MarkerStore;
import ai.floedb.floecat.service.security.RolePermissions;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import com.google.protobuf.FieldMask;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.jboss.logging.Logger;

@GrpcService
public class CatalogIntegrationsImpl extends BaseServiceImpl implements CatalogIntegrations {
  private static final Logger LOG = Logger.getLogger(CatalogIntegrations.class);
  private static final Set<String> MUTABLE_PATHS =
      Set.of("display_name", "catalog_uri", "properties");

  @Inject CatalogIntegrationRepository integrations;
  @Inject CatalogOverlayRepository overlays;
  @Inject CatalogRepository catalogs;
  @Inject MarkerStore markerStore;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject IdempotencyRepository idempotencyStore;
  @Inject CatalogIntegrationCredentialStore credentialStore;
  @Inject CatalogIntegrationCredentialCleanup credentialCleanup;

  @Override
  public Uni<ListCatalogIntegrationsResponse> listCatalogIntegrations(
      ListCatalogIntegrationsRequest request) {
    var L = LogHelper.start(LOG, "ListCatalogIntegrations");

    return mapFailures(
            run(
                () -> {
                  var pc = principal.get();
                  authz.require(pc, RolePermissions.CATALOG_INTEGRATION_READ);
                  var page = MutationOps.pageIn(request.hasPage() ? request.getPage() : null);
                  var next = new StringBuilder();
                  var rows =
                      integrations.listWithMeta(
                          pc.getAccountId(), Math.max(1, page.limit), page.token, next);
                  var response = ListCatalogIntegrationsResponse.newBuilder();
                  rows.forEach(
                      row ->
                          response.addEntries(
                              CatalogIntegrationEntry.newBuilder()
                                  .setIntegration(row.value())
                                  .setMeta(row.meta())));
                  return response
                      .setPage(
                          MutationOps.pageOut(
                              next.toString(), integrations.count(pc.getAccountId())))
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<GetCatalogIntegrationResponse> getCatalogIntegration(
      GetCatalogIntegrationRequest request) {
    var L = LogHelper.start(LOG, "GetCatalogIntegration");

    return mapFailures(
            run(
                () -> {
                  var pc = principal.get();
                  authz.require(pc, RolePermissions.CATALOG_INTEGRATION_READ);
                  if (!request.hasIntegrationId() && !request.hasDisplayName())
                    throw GrpcErrors.invalidArgument(
                        pc.getCorrelationId(), SELECTOR_REQUIRED, Map.of("field", "selector"));
                  var row =
                      (request.hasIntegrationId()
                              ? integrations.getByIdWithMeta(
                                  scopedId(pc.getAccountId(), request.getIntegrationId()))
                              : request.hasDisplayName()
                                  ? integrations.getByNameWithMeta(
                                      pc.getAccountId(),
                                      mustNonEmpty(
                                          request.getDisplayName(),
                                          "display_name",
                                          pc.getCorrelationId()))
                                  : throwMissingIntegrationSelector())
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(
                                      pc.getCorrelationId(),
                                      CATALOG_INTEGRATION,
                                      Map.of(
                                          "id",
                                          request.hasIntegrationId()
                                              ? request.getIntegrationId().getId()
                                              : request.getDisplayName())));
                  return GetCatalogIntegrationResponse.newBuilder()
                      .setIntegration(row.value())
                      .setMeta(row.meta())
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  private static java.util.Optional<
          ai.floedb.floecat.service.repo.util.GenericResourceRepository.ResourceWithMeta<
              CatalogIntegration>>
      throwMissingIntegrationSelector() {
    throw new IllegalStateException("validated integration selector missing");
  }

  @Override
  public Uni<CreateCatalogIntegrationResponse> createCatalogIntegration(
      CreateCatalogIntegrationRequest request) {
    var L = LogHelper.start(LOG, "CreateCatalogIntegration");

    return mapFailures(
            runWithRetry(
                () -> {
                  var pc = principal.get();
                  authz.require(pc, RolePermissions.CATALOG_INTEGRATION_WRITE);
                  String corr = pc.getCorrelationId();
                  CatalogIntegrationSpec spec = request.getSpec();
                  String name = mustNonEmpty(spec.getDisplayName(), "display_name", corr);
                  var createPolicy =
                      CatalogCreatePolicy.validate(
                          request.getCreateMode(),
                          request.hasIdempotency() ? request.getIdempotency().getKey() : "",
                          corr);
                  String key = createPolicy.idempotencyKey();
                  CreateMode mode = createPolicy.mode();
                  if (mode == CreateMode.CM_RETURN_EXISTING) {
                    var existing = integrations.getByNameWithMeta(pc.getAccountId(), name);
                    if (existing.isPresent()) {
                      return CreateCatalogIntegrationResponse.newBuilder()
                          .setIntegration(existing.get().value())
                          .setMeta(existing.get().meta())
                          .build();
                    }
                  }
                  validateType(spec.getType(), corr);
                  String uri = validateCatalogUri(spec.getCatalogUri(), corr);
                  Map<String, String> properties =
                      validateConnectionProperties(spec.getPropertiesMap(), corr);
                  if (!spec.hasAuthentication()
                      || spec.getAuthentication().getConfigurationCase()
                          == CatalogAuthentication.ConfigurationCase.CONFIGURATION_NOT_SET) {
                    throw GrpcErrors.invalidArgument(
                        corr, FIELD, Map.of("field", "authentication.configuration"));
                  }
                  PreparedAuthentication preparedAuthentication =
                      prepareAuthentication(
                          spec.hasAuthentication()
                              ? spec.getAuthentication()
                              : CatalogAuthentication.getDefaultInstance(),
                          request.hasCredentials()
                              ? request.getCredentials()
                              : CatalogIntegrationCredentials.getDefaultInstance(),
                          1L,
                          corr);
                  validateAuthenticationType(
                      spec.getType(), preparedAuthentication.authentication(), corr);
                  byte[] fingerprint =
                      new Canonicalizer()
                          .scalar("display_name", name)
                          .scalar("type", spec.getType())
                          .scalar("catalog_uri", uri)
                          .map("properties", properties)
                          .scalar("authentication", preparedAuthentication.authentication())
                          .bytes();
                  var now = nowTs();

                  if (key.isEmpty()) {
                    ResourceId integrationId =
                        randomResourceId(pc.getAccountId(), ResourceKind.RK_CATALOG_INTEGRATION);
                    credentialStore.store(
                        integrationId,
                        preparedAuthentication.authentication().getCredentialGeneration(),
                        preparedAuthentication.credentials());
                    try {
                      var created =
                          createOrReplace(
                              integrationId,
                              name,
                              spec.getType(),
                              uri,
                              properties,
                              preparedAuthentication.authentication(),
                              mode,
                              false,
                              null,
                              now,
                              corr);
                      if (!created.newIdentityPublished()) {
                        cleanupPrepared(integrationId, preparedAuthentication);
                      }
                      return CreateCatalogIntegrationResponse.newBuilder()
                          .setIntegration(created.row().value())
                          .setMeta(created.row().meta())
                          .build();
                    } catch (RuntimeException retryableFailure) {
                      cleanupPrepared(integrationId, preparedAuthentication);
                      throw retryableFailure;
                    }
                  }
                  var result =
                      runIdempotentCreate(
                          () ->
                              MutationOps.createProtoRecoverable(
                                  pc.getAccountId(),
                                  "CreateCatalogIntegration",
                                  key,
                                  () -> fingerprint,
                                  () ->
                                      randomResourceId(
                                          pc.getAccountId(), ResourceKind.RK_CATALOG_INTEGRATION),
                                  (reservedId, completion) -> {
                                    var recovered = integrations.getByIdWithMeta(reservedId);
                                    if (recovered.isPresent()) {
                                      throw new BaseResourceRepository.AbortRetryableException(
                                          "integration exists before its idempotency receipt committed");
                                    }
                                    credentialStore.store(
                                        reservedId,
                                        preparedAuthentication
                                            .authentication()
                                            .getCredentialGeneration(),
                                        preparedAuthentication.credentials());
                                    try {
                                      var created =
                                          createOrReplace(
                                              reservedId,
                                              name,
                                              spec.getType(),
                                              uri,
                                              properties,
                                              preparedAuthentication.authentication(),
                                              CreateMode.CM_ERROR_IF_EXISTS,
                                              true,
                                              completion,
                                              now,
                                              corr);
                                      return new IdempotencyGuard.CommittedCreate<>(
                                          created.row().value(),
                                          created.row().value().getResourceId(),
                                          created.row().meta());
                                    } catch (RuntimeException definiteFailure) {
                                      cleanupPrepared(reservedId, preparedAuthentication);
                                      throw definiteFailure;
                                    }
                                  },
                                  idempotencyStore,
                                  now,
                                  idempotencyTtlSeconds(),
                                  this::correlationId,
                                  CatalogIntegration::parseFrom));
                  return CreateCatalogIntegrationResponse.newBuilder()
                      .setIntegration(result.body)
                      .setMeta(result.meta)
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  private record CreateOutcome(
      ai.floedb.floecat.service.repo.util.GenericResourceRepository.ResourceWithMeta<
              CatalogIntegration>
          row,
      boolean newIdentityPublished) {}

  private CreateOutcome createOrReplace(
      ResourceId newId,
      String name,
      CatalogIntegrationType type,
      String uri,
      Map<String, String> properties,
      CatalogAuthentication authentication,
      CreateMode mode,
      boolean reservedIdentity,
      IdempotencyGuard.SuccessCommitter<CatalogIntegration> completion,
      com.google.protobuf.Timestamp now,
      String corr) {
    var existing = integrations.getByNameWithMeta(newId.getAccountId(), name);
    if (existing.isPresent()) {
      var current = existing.get();
      if (reservedIdentity && current.value().getResourceId().equals(newId))
        return new CreateOutcome(current, true);
      if (mode == CreateMode.CM_RETURN_EXISTING) return new CreateOutcome(current, false);
      if (mode != CreateMode.CM_REPLACE)
        throw GrpcErrors.alreadyExists(
            corr, CATALOG_INTEGRATION_ALREADY_EXISTS, Map.of("display_name", name));
      long markerVersion =
          markerStore.catalogIntegrationOverlaysMarkerVersion(current.value().getResourceId());
      if (markerVersion > 0L) {
        throw GrpcErrors.conflict(
            corr,
            CATALOG_INTEGRATION_DELETION_IN_PROGRESS,
            Map.of("reason", "dependent overlays exist"));
      }
      var replacement =
          applyAuthentication(
                  CatalogIntegration.newBuilder()
                      .setResourceId(newId)
                      .setDisplayName(name)
                      .setType(type)
                      .setCatalogUri(uri)
                      .putAllProperties(properties)
                      .setCreatedAt(now)
                      .setUpdatedAt(now),
                  authentication)
              .build();
      credentialCleanup.schedule(current.value());
      try {
        var replaced =
            integrations.replaceIdentityWithMeta(
                current.value(), current.meta().getPointerVersion(), replacement);
        if (replaced.isEmpty()) {
          throw new BaseResourceRepository.AbortRetryableException(
              "integration or overlays changed during replacement");
        }
        credentialCleanup.cleanIfSuperseded(current.value());
        return new CreateOutcome(replaced.get(), true);
      } catch (RuntimeException failure) {
        credentialCleanup.cancelIfResourceUnchanged(
            current.value(), current.meta().getPointerVersion());
        throw failure;
      }
    }
    var desired =
        applyAuthentication(
                CatalogIntegration.newBuilder()
                    .setResourceId(newId)
                    .setDisplayName(name)
                    .setType(type)
                    .setCatalogUri(uri)
                    .putAllProperties(properties)
                    .setCreatedAt(now)
                    .setUpdatedAt(now),
                authentication)
            .build();
    try {
      var created =
          completion == null
              ? integrations.createWithMeta(desired)
              : integrations.createWithMetaAndCompletion(
                  desired,
                  row ->
                      completion.prepare(
                          new IdempotencyGuard.CommittedCreate<>(
                              row.value(), row.value().getResourceId(), row.meta())));
      return new CreateOutcome(created, true);
    } catch (BaseResourceRepository.NameConflictException conflict) {
      if (reservedIdentity) {
        var recovered = integrations.getByIdWithMeta(newId);
        if (recovered.isPresent())
          throw new BaseResourceRepository.AbortRetryableException(
              "integration exists before its idempotency receipt committed");
        var owner = integrations.getByNameWithMeta(newId.getAccountId(), name);
        if (owner.isEmpty()) {
          throw new BaseResourceRepository.AbortRetryableException(
              "integration create conflict not yet visible");
        }
        if (owner.get().value().getResourceId().equals(newId))
          return new CreateOutcome(owner.get(), true);
      }
      if (mode != CreateMode.CM_ERROR_IF_EXISTS)
        throw new BaseResourceRepository.AbortRetryableException(
            "integration name owner changed during create");
      throw GrpcErrors.alreadyExists(
          corr, CATALOG_INTEGRATION_ALREADY_EXISTS, Map.of("display_name", name));
    }
  }

  @Override
  public Uni<UpdateCatalogIntegrationResponse> updateCatalogIntegration(
      UpdateCatalogIntegrationRequest request) {
    var L = LogHelper.start(LOG, "UpdateCatalogIntegration");

    return mapFailures(
            runWithRetry(
                () -> {
                  var pc = principal.get();
                  authz.require(pc, RolePermissions.CATALOG_INTEGRATION_WRITE);
                  String corr = pc.getCorrelationId();
                  ResourceId id = scopedId(pc.getAccountId(), request.getIntegrationId());
                  Set<String> paths =
                      requiredMask(request.hasUpdateMask() ? request.getUpdateMask() : null, corr);
                  var current =
                      integrations
                          .getByIdWithMeta(id)
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(
                                      corr, CATALOG_INTEGRATION, Map.of("id", id.getId())));
                  MutationOps.BaseServiceChecks.enforcePreconditions(
                      corr, current.meta(), request.getPrecondition());
                  var desiredBuilder = current.value().toBuilder();
                  if (paths.contains("display_name")) {
                    desiredBuilder.setDisplayName(
                        mustNonEmpty(request.getSpec().getDisplayName(), "display_name", corr));
                  }
                  if (paths.contains("catalog_uri")) {
                    desiredBuilder.setCatalogUri(
                        validateCatalogUri(request.getSpec().getCatalogUri(), corr));
                  }
                  if (paths.contains("properties")) {
                    desiredBuilder
                        .clearProperties()
                        .putAllProperties(
                            validateConnectionProperties(
                                request.getSpec().getPropertiesMap(), corr));
                  }
                  CatalogIntegration desired = desiredBuilder.setUpdatedAt(nowTs()).build();
                  try {
                    var meta =
                        integrations
                            .updateWithMetaUnlessDeleting(
                                desired, current.meta().getPointerVersion())
                            .orElseThrow(
                                () ->
                                    GrpcErrors.preconditionFailed(
                                        corr, CATALOG_INTEGRATION_CHANGED, Map.of()));
                    return UpdateCatalogIntegrationResponse.newBuilder()
                        .setIntegration(desired)
                        .setMeta(meta)
                        .build();
                  } catch (BaseResourceRepository.NameConflictException e) {
                    throw GrpcErrors.alreadyExists(
                        corr,
                        CATALOG_INTEGRATION_ALREADY_EXISTS,
                        Map.of("display_name", desired.getDisplayName()));
                  }
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<UpdateCatalogIntegrationAuthenticationResponse> updateCatalogIntegrationAuthentication(
      UpdateCatalogIntegrationAuthenticationRequest request) {
    var L = LogHelper.start(LOG, "UpdateCatalogIntegrationAuthentication");

    return mapFailures(
            runWithRetry(
                () -> {
                  var pc = principal.get();
                  authz.require(pc, RolePermissions.CATALOG_INTEGRATION_WRITE);
                  String corr = pc.getCorrelationId();
                  ResourceId id = scopedId(pc.getAccountId(), request.getIntegrationId());
                  if (!request.hasAuthentication()
                      || request.getAuthentication().getConfigurationCase()
                          == CatalogAuthentication.ConfigurationCase.CONFIGURATION_NOT_SET) {
                    throw GrpcErrors.invalidArgument(
                        corr, FIELD, Map.of("field", "authentication.configuration"));
                  }
                  var current =
                      integrations
                          .getByIdWithMeta(id)
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(
                                      corr, CATALOG_INTEGRATION, Map.of("id", id.getId())));
                  MutationOps.BaseServiceChecks.enforcePreconditions(
                      corr, current.meta(), request.getPrecondition());
                  long nextGeneration =
                      current.value().hasAuthentication()
                          ? current.value().getAuthentication().getCredentialGeneration() + 1L
                          : 1L;
                  PreparedAuthentication prepared =
                      prepareAuthentication(
                          request.hasAuthentication()
                              ? request.getAuthentication()
                              : CatalogAuthentication.getDefaultInstance(),
                          request.hasCredentials()
                              ? request.getCredentials()
                              : CatalogIntegrationCredentials.getDefaultInstance(),
                          nextGeneration,
                          corr);
                  validateAuthenticationType(
                      current.value().getType(), prepared.authentication(), corr);
                  long allocatedGeneration =
                      credentialStore.storeRotation(
                          id,
                          prepared.authentication().getCredentialGeneration(),
                          prepared.credentials());
                  if (allocatedGeneration != prepared.authentication().getCredentialGeneration()) {
                    prepared =
                        new PreparedAuthentication(
                            prepared.authentication().toBuilder()
                                .setCredentialGeneration(allocatedGeneration)
                                .build(),
                            prepared.credentials());
                  }
                  CatalogIntegration desired =
                      applyAuthentication(current.value().toBuilder(), prepared.authentication())
                          .setUpdatedAt(nowTs())
                          .build();
                  credentialCleanup.schedule(current.value());
                  try {
                    var meta =
                        integrations
                            .updateWithMetaUnlessDeleting(
                                desired, current.meta().getPointerVersion())
                            .orElseThrow(
                                () ->
                                    GrpcErrors.preconditionFailed(
                                        corr, CATALOG_INTEGRATION_CHANGED, Map.of()));
                    credentialCleanup.cleanIfSuperseded(current.value());
                    return UpdateCatalogIntegrationAuthenticationResponse.newBuilder()
                        .setIntegration(desired)
                        .setMeta(meta)
                        .build();
                  } catch (RuntimeException definiteFailure) {
                    credentialCleanup.cancelIfResourceUnchanged(
                        current.value(), current.meta().getPointerVersion());
                    cleanupPreparedAuthenticationUnlessPublished(id, prepared);
                    throw definiteFailure;
                  }
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<DeleteCatalogIntegrationResponse> deleteCatalogIntegration(
      DeleteCatalogIntegrationRequest request) {
    var L = LogHelper.start(LOG, "DeleteCatalogIntegration");

    return mapFailures(
        runWithRetry(
            () -> {
              var pc = principal.get();
              authz.require(pc, RolePermissions.CATALOG_INTEGRATION_WRITE);
              if (request.getCascade()) authz.require(pc, RolePermissions.CATALOG_OVERLAY_WRITE);
              String corr = pc.getCorrelationId();
              ResourceId id = scopedId(pc.getAccountId(), request.getIntegrationId());
              var current = integrations.getByIdWithMeta(id);
              if (current.isEmpty()) {
                if (hasMeaningfulPrecondition(request.getPrecondition()))
                  throw GrpcErrors.notFound(corr, CATALOG_INTEGRATION, Map.of("id", id.getId()));
                return DeleteCatalogIntegrationResponse.newBuilder()
                    .setMeta(integrations.metaForSafe(id))
                    .build();
              }
              var meta = current.get().meta();
              MutationOps.BaseServiceChecks.enforcePreconditions(
                  corr, meta, request.getPrecondition());
              if (request.getCascade()) {
                credentialCleanup.schedule(current.get().value());
                cascadeDeleteIntegration(id, meta);
                credentialCleanup.cleanIfSuperseded(current.get().value());
                return DeleteCatalogIntegrationResponse.newBuilder().setMeta(meta).build();
              }
              if (integrations.cascadeDeletionFenceVersion(id) > 0L)
                throw GrpcErrors.conflict(
                    corr,
                    CATALOG_INTEGRATION_DELETION_IN_PROGRESS,
                    Map.of("reason", "cascade deletion in progress"));
              long markerVersion = markerStore.catalogIntegrationOverlaysMarkerVersion(id);
              if (markerVersion > 0L)
                throw GrpcErrors.conflict(
                    corr,
                    CATALOG_INTEGRATION_DELETION_IN_PROGRESS,
                    Map.of("reason", "dependent overlays exist"));
              credentialCleanup.schedule(current.get().value());
              try {
                if (!integrations.deleteWithPreconditionAndNoOverlayMarker(
                    id, meta.getPointerVersion())) {
                  throw new BaseResourceRepository.AbortRetryableException(
                      "integration or overlays changed during deletion");
                }
              } catch (RuntimeException failure) {
                credentialCleanup.cancelIfResourceUnchanged(
                    current.get().value(), meta.getPointerVersion());
                throw failure;
              }
              credentialCleanup.cleanIfSuperseded(current.get().value());
              return DeleteCatalogIntegrationResponse.newBuilder().setMeta(meta).build();
            }),
        correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  private void cascadeDeleteIntegration(
      ResourceId integrationId, ai.floedb.floecat.common.rpc.MutationMeta integrationMeta) {
    if (!integrations.beginCascadeDeletion(integrationId, integrationMeta.getPointerVersion())) {
      throw new BaseResourceRepository.AbortRetryableException(
          "integration changed while cascade deletion was fenced");
    }

    while (true) {
      var next = new StringBuilder();
      var dependents =
          overlays.listByIntegrationWithMetaConsistent(
              integrationId.getAccountId(), integrationId.getId(), 100, "", next);
      if (dependents.isEmpty()) break;
      for (var dependent : dependents) {
        if (!dependent.value().hasCatalogId()) {
          throw new BaseResourceRepository.CorruptionException(
              "overlay is missing its owned catalog: " + dependent.value().getResourceId().getId(),
              null);
        }
        var ownedCatalogDeletes = catalogs.prepareDeleteOps(dependent.value().getCatalogId());
        if (!overlays.deleteWithOwnedCatalog(
            dependent.value().getResourceId(),
            dependent.meta().getPointerVersion(),
            ownedCatalogDeletes)) {
          throw new BaseResourceRepository.AbortRetryableException(
              "overlay changed during integration cascade deletion");
        }
      }
    }

    int remaining =
        overlays.countByIntegration(integrationId.getAccountId(), integrationId.getId());
    if (remaining != 0) {
      throw new BaseResourceRepository.AbortRetryableException(
          "overlays appeared during integration cascade deletion");
    }
    long markerVersion = markerStore.catalogIntegrationOverlaysMarkerVersion(integrationId);
    long fenceVersion = integrations.cascadeDeletionFenceVersion(integrationId);
    if (fenceVersion == 0L
        || !integrations.deleteWithPreconditionForCascadeDeletion(
            integrationId, integrationMeta.getPointerVersion(), markerVersion, fenceVersion)) {
      throw new BaseResourceRepository.AbortRetryableException(
          "integration changed while cascade deletion completed");
    }
  }

  private static Set<String> requiredMask(FieldMask mask, String corr) {
    if (mask == null || mask.getPathsCount() == 0) {
      throw GrpcErrors.invalidArgument(corr, UPDATE_MASK_REQUIRED, Map.of("field", "update_mask"));
    }
    Set<String> paths = Set.copyOf(mask.getPathsList());
    if (paths.size() != mask.getPathsCount() || !MUTABLE_PATHS.containsAll(paths)) {
      throw GrpcErrors.invalidArgument(corr, FIELD, Map.of("field", "update_mask"));
    }
    return paths;
  }

  private void cleanupPreparedAuthenticationUnlessPublished(
      ResourceId integrationId, PreparedAuthentication prepared) {
    if (prepared.credentials().getCredentialCase()
        == CatalogIntegrationCredentials.CredentialCase.CREDENTIAL_NOT_SET) {
      return;
    }
    var published = integrations.getByIdForMutation(integrationId);
    if (published.isPresent()
        && published.get().hasAuthentication()
        && published.get().getAuthentication().getCredentialsConfigured()
        && published.get().getAuthentication().getCredentialGeneration()
            == prepared.authentication().getCredentialGeneration()) {
      return;
    }
    cleanupPrepared(integrationId, prepared);
  }

  private void cleanupPrepared(ResourceId integrationId, PreparedAuthentication prepared) {
    credentialCleanup.cleanPrepared(
        integrationId, prepared.authentication().getCredentialGeneration(), prepared.credentials());
  }

  private static void validateType(CatalogIntegrationType type, String corr) {
    if (type != CatalogIntegrationType.CIT_ICEBERG_REST && type != CatalogIntegrationType.CIT_UNITY)
      throw GrpcErrors.invalidArgument(corr, FIELD, Map.of("field", "type"));
  }

  private static void validateAuthenticationType(
      CatalogIntegrationType integrationType, CatalogAuthentication authentication, String corr) {
    var configuration = authentication.getConfigurationCase();
    if (configuration == CatalogAuthentication.ConfigurationCase.CONFIGURATION_NOT_SET) return;
    if (integrationType == CatalogIntegrationType.CIT_UNITY
        && configuration != CatalogAuthentication.ConfigurationCase.OAUTH_CLIENT_CREDENTIALS
        && configuration != CatalogAuthentication.ConfigurationCase.BEARER) {
      throw GrpcErrors.invalidArgument(
          corr, FIELD, Map.of("field", "authentication.configuration"));
    }
  }

  private static String validateCatalogUri(String value, String corr) {
    String candidate = value;
    if (candidate == null || candidate.isBlank() || candidate.indexOf('\0') >= 0)
      throw GrpcErrors.invalidArgument(corr, FIELD, Map.of("field", "catalog_uri"));
    try {
      URI uri = URI.create(candidate);
      if (!("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme()))
          || uri.isOpaque()
          || uri.getHost() == null
          || uri.getHost().isBlank()
          || uri.getRawUserInfo() != null
          || uri.getRawFragment() != null) throw new IllegalArgumentException();
      String rawQuery = uri.getRawQuery();
      if (rawQuery != null) {
        for (String parameter : rawQuery.split("[&;]", -1)) {
          int separator = parameter.indexOf('=');
          String rawKey = separator < 0 ? parameter : parameter.substring(0, separator);
          String key = URLDecoder.decode(rawKey, StandardCharsets.UTF_8);
          String canonical =
              ai.floedb.floecat.service.common.PersistedSecretPropertyValidator.canonicalSecretKey(
                  key);
          if (ai.floedb.floecat.service.common.PersistedSecretPropertyValidator
                  .isForbiddenPersistedSecretKey(key)
              || canonical.equals("sig")
              || canonical.equals("signature")
              || canonical.endsWith("_signature")) throw new IllegalArgumentException();
        }
      }
      return candidate;
    } catch (IllegalArgumentException e) {
      throw GrpcErrors.invalidArgument(corr, FIELD, Map.of("field", "catalog_uri"));
    }
  }

  private static Map<String, String> validateConnectionProperties(
      Map<String, String> properties, String corr) {
    ai.floedb.floecat.service.common.PersistedSecretPropertyValidator.validateNoSecretKeys(
        properties, corr, "properties");
    return Map.copyOf(properties);
  }

  private record PreparedAuthentication(
      CatalogAuthentication authentication, CatalogIntegrationCredentials credentials) {}

  private static PreparedAuthentication prepareAuthentication(
      CatalogAuthentication requested,
      CatalogIntegrationCredentials credentials,
      long generation,
      String corr) {
    if (requested.getCredentialsConfigured() || requested.getCredentialGeneration() != 0L) {
      throw GrpcErrors.invalidArgument(corr, FIELD, Map.of("field", "authentication.state"));
    }
    var configuration = requested.getConfigurationCase();
    var credential = credentials.getCredentialCase();
    if (configuration == CatalogAuthentication.ConfigurationCase.CONFIGURATION_NOT_SET) {
      throw GrpcErrors.invalidArgument(
          corr, FIELD, Map.of("field", "authentication.configuration"));
    }

    switch (configuration) {
      case OAUTH_CLIENT_CREDENTIALS -> {
        var config = requested.getOauthClientCredentials();
        requireNonBlank(config.getClientId(), "authentication.client_id", corr);
        if (config.hasTokenUri()) validateCatalogUri(config.getTokenUri(), corr);
        var scopes = new LinkedHashSet<String>();
        for (String scope : config.getScopesList()) {
          scopes.add(requireNonBlank(scope, "authentication.scopes", corr));
        }
        if (scopes.size() != config.getScopesCount()) {
          throw GrpcErrors.invalidArgument(corr, FIELD, Map.of("field", "authentication.scopes"));
        }
        requireCredentialCase(
            credential, CatalogIntegrationCredentials.CredentialCase.OAUTH_CLIENT_SECRET, corr);
        requireNonBlank(credentials.getOauthClientSecret().getValue(), "credentials", corr);
      }
      case BEARER -> {
        requireCredentialCase(
            credential, CatalogIntegrationCredentials.CredentialCase.BEARER_TOKEN, corr);
        requireNonBlank(credentials.getBearerToken().getValue(), "credentials", corr);
      }
      case AWS_ASSUME_ROLE -> {
        var config = requested.getAwsAssumeRole();
        validateAssumeRole(config, corr);
        requireCredentialCase(
            credential, CatalogIntegrationCredentials.CredentialCase.CREDENTIAL_NOT_SET, corr);
      }
      case AWS_ACCESS_KEY -> {
        requireNonBlank(
            requested.getAwsAccessKey().getAccessKeyId(), "authentication.access_key_id", corr);
        requireCredentialCase(
            credential, CatalogIntegrationCredentials.CredentialCase.AWS_ACCESS_KEY, corr);
        validateAccessKeySecret(credentials, corr);
      }
      case AWS_SIGV4 -> {
        var config = requested.getAwsSigv4();
        requireNonBlank(config.getRegion(), "authentication.region", corr);
        if (config.hasSigningName()) {
          requireNonBlank(config.getSigningName(), "authentication.signing_name", corr);
        }
        switch (config.getCredentialsCase()) {
          case AWS_DEFAULT -> {
            requireCredentialCase(
                credential, CatalogIntegrationCredentials.CredentialCase.CREDENTIAL_NOT_SET, corr);
          }
          case AWS_ASSUME_ROLE -> {
            var assume = config.getAwsAssumeRole();
            validateAssumeRole(assume, corr);
            requireCredentialCase(
                credential, CatalogIntegrationCredentials.CredentialCase.CREDENTIAL_NOT_SET, corr);
          }
          case AWS_ACCESS_KEY -> {
            requireNonBlank(
                config.getAwsAccessKey().getAccessKeyId(), "authentication.access_key_id", corr);
            requireCredentialCase(
                credential, CatalogIntegrationCredentials.CredentialCase.AWS_ACCESS_KEY, corr);
            validateAccessKeySecret(credentials, corr);
          }
          case CREDENTIALS_NOT_SET ->
              throw GrpcErrors.invalidArgument(
                  corr, FIELD, Map.of("field", "authentication.aws_sigv4.credentials"));
        }
      }
      case CONFIGURATION_NOT_SET -> throw new IllegalStateException("handled above");
    }

    boolean storesCredentials =
        credential != CatalogIntegrationCredentials.CredentialCase.CREDENTIAL_NOT_SET;
    var persisted =
        requested.toBuilder()
            .setCredentialsConfigured(storesCredentials)
            .setCredentialGeneration(storesCredentials ? generation : 0L);
    return new PreparedAuthentication(persisted.build(), credentials);
  }

  private static void validateAssumeRole(AwsAssumeRoleAuthentication config, String corr) {
    requireNonBlank(config.getRoleArn(), "authentication.role_arn", corr);
    if (config.hasExternalId()) {
      requireNonBlank(config.getExternalId(), "authentication.external_id", corr);
    }
    if (config.hasRoleSessionName()) {
      requireNonBlank(config.getRoleSessionName(), "authentication.role_session_name", corr);
    }
  }

  private static void validateAccessKeySecret(
      CatalogIntegrationCredentials credentials, String corr) {
    if (credentials.getCredentialCase()
        != CatalogIntegrationCredentials.CredentialCase.AWS_ACCESS_KEY) {
      return;
    }
    var secret = credentials.getAwsAccessKey();
    requireNonBlank(secret.getSecretAccessKey(), "credentials.secret_access_key", corr);
    if (secret.hasSessionToken()) {
      requireNonBlank(secret.getSessionToken(), "credentials.session_token", corr);
    }
  }

  private static void requireCredentialCase(
      CatalogIntegrationCredentials.CredentialCase actual,
      CatalogIntegrationCredentials.CredentialCase expected,
      String corr) {
    if (actual == expected) return;
    throw GrpcErrors.invalidArgument(corr, FIELD, Map.of("field", "credentials"));
  }

  private static String requireNonBlank(String value, String field, String corr) {
    if (value == null || value.isBlank()) {
      throw GrpcErrors.invalidArgument(corr, FIELD, Map.of("field", field));
    }
    return value.trim();
  }

  private static CatalogIntegration.Builder applyAuthentication(
      CatalogIntegration.Builder builder, CatalogAuthentication authentication) {
    if (authentication.getConfigurationCase()
        == CatalogAuthentication.ConfigurationCase.CONFIGURATION_NOT_SET) {
      return builder.clearAuthentication();
    }
    return builder.setAuthentication(authentication);
  }

  private ResourceId scopedId(String accountId, ResourceId id) {
    ensureKind(id, ResourceKind.RK_CATALOG_INTEGRATION, "integration_id", correlationId());
    mustNonEmpty(id.getAccountId(), "integration_id.account_id", correlationId());
    mustNonEmpty(id.getId(), "integration_id.id", correlationId());
    if (!accountId.equals(id.getAccountId()))
      throw GrpcErrors.invalidArgument(
          correlationId(), FIELD, Map.of("field", "integration_id.account_id"));
    return id.toBuilder().setAccountId(accountId).build();
  }
}
