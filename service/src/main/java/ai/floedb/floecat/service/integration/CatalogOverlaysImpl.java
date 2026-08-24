/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package ai.floedb.floecat.service.integration;

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.CATALOG;
import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.CATALOG_INTEGRATION;
import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.CATALOG_OVERLAY;
import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.CATALOG_OVERLAY_ALREADY_EXISTS;
import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.CATALOG_OVERLAY_CHANGED;
import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.FIELD;
import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.SELECTOR_REQUIRED;
import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.UPDATE_MASK_REQUIRED;

import ai.floedb.floecat.common.rpc.CreateMode;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.integration.rpc.CatalogOverlay;
import ai.floedb.floecat.integration.rpc.CatalogOverlayEntry;
import ai.floedb.floecat.integration.rpc.CatalogOverlaySpec;
import ai.floedb.floecat.integration.rpc.CatalogOverlays;
import ai.floedb.floecat.integration.rpc.CreateCatalogOverlayRequest;
import ai.floedb.floecat.integration.rpc.CreateCatalogOverlayResponse;
import ai.floedb.floecat.integration.rpc.DeleteCatalogOverlayRequest;
import ai.floedb.floecat.integration.rpc.DeleteCatalogOverlayResponse;
import ai.floedb.floecat.integration.rpc.GetCatalogOverlayRequest;
import ai.floedb.floecat.integration.rpc.GetCatalogOverlayResponse;
import ai.floedb.floecat.integration.rpc.ListCatalogOverlaysRequest;
import ai.floedb.floecat.integration.rpc.ListCatalogOverlaysResponse;
import ai.floedb.floecat.integration.rpc.NamespacePath;
import ai.floedb.floecat.integration.rpc.ReconcileCatalogOverlayRequest;
import ai.floedb.floecat.integration.rpc.ReconcileCatalogOverlayResponse;
import ai.floedb.floecat.integration.rpc.UpdateCatalogOverlayRequest;
import ai.floedb.floecat.integration.rpc.UpdateCatalogOverlayResponse;
import ai.floedb.floecat.scanner.spi.CatalogGraphView;
import ai.floedb.floecat.service.catalog.impl.surface.CatalogSurfaceWritePolicy;
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
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.MarkerStore;
import ai.floedb.floecat.service.security.RolePermissions;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import com.google.protobuf.FieldMask;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jboss.logging.Logger;

@GrpcService
public class CatalogOverlaysImpl extends BaseServiceImpl implements CatalogOverlays {
  private static final Logger LOG = Logger.getLogger(CatalogOverlays.class);
  private static final Set<String> MUTABLE_PATHS =
      Set.of("display_name", "include_namespaces", "exclude_namespaces");

  @Inject CatalogOverlayRepository overlays;
  @Inject CatalogIntegrationRepository integrations;
  @Inject CatalogRepository catalogs;
  @Inject MarkerStore markerStore;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject IdempotencyRepository idempotencyStore;
  @Inject CatalogOverlayReconciler reconciler;
  @Inject CatalogGraphView graphView;

  @Override
  public Uni<ListCatalogOverlaysResponse> listCatalogOverlays(ListCatalogOverlaysRequest request) {
    var L = LogHelper.start(LOG, "ListCatalogOverlays");

    return mapFailures(
            run(
                () -> {
                  var pc = principal.get();
                  authz.require(pc, RolePermissions.CATALOG_OVERLAY_READ);
                  var page = MutationOps.pageIn(request.hasPage() ? request.getPage() : null);
                  var next = new StringBuilder();
                  List<
                          ai.floedb.floecat.service.repo.util.GenericResourceRepository
                                  .ResourceWithMeta<
                              CatalogOverlay>>
                      rows;
                  int total;
                  if (request.hasIntegrationId()) {
                    ResourceId integrationId =
                        scopedIntegrationId(pc.getAccountId(), request.getIntegrationId());
                    rows =
                        overlays.listByIntegrationWithMeta(
                            pc.getAccountId(),
                            integrationId.getId(),
                            Math.max(1, page.limit),
                            page.token,
                            next);
                    total = overlays.countByIntegration(pc.getAccountId(), integrationId.getId());
                  } else {
                    rows =
                        overlays.listWithMeta(
                            pc.getAccountId(), Math.max(1, page.limit), page.token, next);
                    total = overlays.count(pc.getAccountId());
                  }
                  var response = ListCatalogOverlaysResponse.newBuilder();
                  rows.forEach(
                      row ->
                          response.addEntries(
                              CatalogOverlayEntry.newBuilder()
                                  .setOverlay(row.value())
                                  .setMeta(row.meta())));
                  return response.setPage(MutationOps.pageOut(next.toString(), total)).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<GetCatalogOverlayResponse> getCatalogOverlay(GetCatalogOverlayRequest request) {
    var L = LogHelper.start(LOG, "GetCatalogOverlay");

    return mapFailures(
            run(
                () -> {
                  var pc = principal.get();
                  authz.require(pc, RolePermissions.CATALOG_OVERLAY_READ);
                  if (!request.hasOverlayId() && !request.hasDisplayName())
                    throw GrpcErrors.invalidArgument(
                        pc.getCorrelationId(), SELECTOR_REQUIRED, Map.of("field", "selector"));
                  var row =
                      (request.hasOverlayId()
                              ? overlays.getByIdWithMeta(
                                  scopedOverlayId(pc.getAccountId(), request.getOverlayId()))
                              : request.hasDisplayName()
                                  ? overlays.getByNameWithMeta(
                                      pc.getAccountId(),
                                      normalizeName(
                                          mustNonEmpty(
                                              request.getDisplayName(),
                                              "display_name",
                                              pc.getCorrelationId())))
                                  : throwMissingOverlaySelector())
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(
                                      pc.getCorrelationId(),
                                      CATALOG_OVERLAY,
                                      Map.of(
                                          "id",
                                          request.hasOverlayId()
                                              ? request.getOverlayId().getId()
                                              : request.getDisplayName())));
                  return GetCatalogOverlayResponse.newBuilder()
                      .setOverlay(row.value())
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
              CatalogOverlay>>
      throwMissingOverlaySelector() {
    throw new IllegalStateException("validated overlay selector missing");
  }

  @Override
  public Uni<CreateCatalogOverlayResponse> createCatalogOverlay(
      CreateCatalogOverlayRequest request) {
    var L = LogHelper.start(LOG, "CreateCatalogOverlay");

    return mapFailures(
            runWithRetry(
                () -> {
                  var pc = principal.get();
                  authz.require(pc, RolePermissions.CATALOG_OVERLAY_WRITE);
                  authz.require(pc, RolePermissions.CATALOG_INTEGRATION_USE);
                  // The overlay will materialize managed resources into the selected catalog.
                  authz.require(pc, RolePermissions.CATALOG_WRITE);
                  String corr = pc.getCorrelationId();
                  CatalogOverlaySpec spec = request.getSpec();
                  String name =
                      normalizeName(mustNonEmpty(spec.getDisplayName(), "display_name", corr));
                  var createPolicy =
                      CatalogCreatePolicy.validate(
                          request.getCreateMode(),
                          request.hasIdempotency() ? request.getIdempotency().getKey() : "",
                          corr);
                  String key = createPolicy.idempotencyKey();
                  CreateMode mode = createPolicy.mode();
                  if (mode == CreateMode.CM_RETURN_EXISTING) {
                    var existing = overlays.getByNameWithMeta(pc.getAccountId(), name);
                    if (existing.isPresent()) {
                      return CreateCatalogOverlayResponse.newBuilder()
                          .setOverlay(existing.get().value())
                          .setMeta(existing.get().meta())
                          .build();
                    }
                  }
                  ResourceId integrationId =
                      scopedIntegrationId(pc.getAccountId(), spec.getIntegrationId());
                  ResourceId catalogId = scopedCatalogId(pc.getAccountId(), spec.getCatalogId());
                  catalogSurfaceWritePolicy().requireWritableCatalog(catalogId, "catalog_id", corr);
                  List<NamespacePath> includes =
                      normalizePaths(spec.getIncludeNamespacesList(), "include_namespaces", corr);
                  List<NamespacePath> excludes =
                      normalizePaths(spec.getExcludeNamespacesList(), "exclude_namespaces", corr);
                  byte[] fingerprint =
                      canonicalFingerprint(name, integrationId, catalogId, includes, excludes);
                  var now = nowTs();

                  if (key.isEmpty()) {
                    var created =
                        createOrReplace(
                            randomResourceId(pc.getAccountId(), ResourceKind.RK_CATALOG_OVERLAY),
                            name,
                            integrationId,
                            catalogId,
                            includes,
                            excludes,
                            mode,
                            false,
                            null,
                            now,
                            corr);
                    return CreateCatalogOverlayResponse.newBuilder()
                        .setOverlay(created.value())
                        .setMeta(created.meta())
                        .build();
                  }
                  var result =
                      runIdempotentCreate(
                          () ->
                              MutationOps.createProtoRecoverable(
                                  pc.getAccountId(),
                                  "CreateCatalogOverlay",
                                  key,
                                  () -> fingerprint,
                                  () ->
                                      randomResourceId(
                                          pc.getAccountId(), ResourceKind.RK_CATALOG_OVERLAY),
                                  (reservedId, completion) -> {
                                    var recovered = overlays.getByIdWithMeta(reservedId);
                                    if (recovered.isPresent()) {
                                      throw new BaseResourceRepository.AbortRetryableException(
                                          "overlay exists before its idempotency receipt committed");
                                    }
                                    var created =
                                        createOrReplace(
                                            reservedId,
                                            name,
                                            integrationId,
                                            catalogId,
                                            includes,
                                            excludes,
                                            CreateMode.CM_ERROR_IF_EXISTS,
                                            true,
                                            completion,
                                            now,
                                            corr);
                                    return new IdempotencyGuard.CommittedCreate<>(
                                        created.value(),
                                        created.value().getResourceId(),
                                        created.meta());
                                  },
                                  idempotencyStore,
                                  now,
                                  idempotencyTtlSeconds(),
                                  this::correlationId,
                                  CatalogOverlay::parseFrom));
                  return CreateCatalogOverlayResponse.newBuilder()
                      .setOverlay(result.body)
                      .setMeta(result.meta)
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  private ai.floedb.floecat.service.repo.util.GenericResourceRepository.ResourceWithMeta<
          CatalogOverlay>
      createOrReplace(
          ResourceId overlayId,
          String name,
          ResourceId integrationId,
          ResourceId catalogId,
          List<NamespacePath> includes,
          List<NamespacePath> excludes,
          CreateMode mode,
          boolean reservedIdentity,
          IdempotencyGuard.SuccessCommitter<CatalogOverlay> completion,
          com.google.protobuf.Timestamp now,
          String corr) {
    String accountId = overlayId.getAccountId();
    var integration =
        integrations
            .getByIdWithMeta(integrationId)
            .orElseThrow(
                () ->
                    GrpcErrors.notFound(
                        corr, CATALOG_INTEGRATION, Map.of("id", integrationId.getId())));
    var catalog =
        catalogs
            .getByIdWithMeta(catalogId)
            .orElseThrow(() -> GrpcErrors.notFound(corr, CATALOG, Map.of("id", catalogId.getId())));
    Map<String, Long> requiredVersions = new java.util.HashMap<>();
    requiredVersions.put(
        Keys.catalogIntegrationPointerById(accountId, integrationId.getId()),
        integration.meta().getPointerVersion());
    requiredVersions.put(
        Keys.catalogPointerById(accountId, catalogId.getId()), catalog.meta().getPointerVersion());
    Set<String> requiredAbsent =
        new java.util.HashSet<>(
            Set.of(
                Keys.catalogIntegrationDeletionMarker(accountId, integrationId.getId()),
                Keys.catalogOverlayDeletionMarker(accountId, overlayId.getId())));
    Map<String, Long> markerVersions = new java.util.HashMap<>();
    markerVersions.put(
        Keys.catalogIntegrationOverlaysMarker(accountId, integrationId.getId()),
        markerStore.catalogIntegrationOverlaysMarkerVersion(integrationId));
    markerVersions.put(
        Keys.catalogOverlaysMarker(accountId, catalogId.getId()),
        markerStore.catalogOverlaysMarkerVersion(catalogId));
    var existing = overlays.getByNameWithMeta(accountId, name);
    if (existing.isPresent()) {
      var current = existing.get();
      if (reservedIdentity && current.value().getResourceId().equals(overlayId)) {
        throw new BaseResourceRepository.AbortRetryableException(
            "overlay exists before its idempotency receipt committed");
      }
      if (mode == CreateMode.CM_RETURN_EXISTING) return current;
      if (mode != CreateMode.CM_REPLACE)
        throw GrpcErrors.alreadyExists(
            corr, CATALOG_OVERLAY_ALREADY_EXISTS, Map.of("display_name", name));

      Map<String, Long> parentVersions = new java.util.HashMap<>(requiredVersions);
      ResourceId oldIntegrationId = current.value().getIntegrationId();
      if (!oldIntegrationId.equals(integrationId)) {
        var oldIntegration =
            integrations
                .getByIdWithMeta(oldIntegrationId)
                .orElseThrow(
                    () ->
                        new BaseResourceRepository.CorruptionException(
                            "overlay references missing integration: " + oldIntegrationId.getId(),
                            null));
        parentVersions.put(
            Keys.catalogIntegrationPointerById(accountId, oldIntegrationId.getId()),
            oldIntegration.meta().getPointerVersion());
        requiredAbsent.add(
            Keys.catalogIntegrationDeletionMarker(accountId, oldIntegrationId.getId()));
      }
      if (!oldIntegrationId.equals(integrationId)) {
        markerVersions.put(
            Keys.catalogIntegrationOverlaysMarker(accountId, oldIntegrationId.getId()),
            markerStore.catalogIntegrationOverlaysMarkerVersion(oldIntegrationId));
      }
      ResourceId oldCatalogId = current.value().getCatalogId();
      if (!oldCatalogId.equals(catalogId)) {
        var oldCatalog =
            catalogs
                .getByIdWithMeta(oldCatalogId)
                .orElseThrow(
                    () ->
                        new BaseResourceRepository.CorruptionException(
                            "overlay references missing catalog: " + oldCatalogId.getId(), null));
        parentVersions.put(
            Keys.catalogPointerById(accountId, oldCatalogId.getId()),
            oldCatalog.meta().getPointerVersion());
        markerVersions.put(
            Keys.catalogOverlaysMarker(accountId, oldCatalogId.getId()),
            markerStore.catalogOverlaysMarkerVersion(oldCatalogId));
      }
      var replacement =
          CatalogOverlay.newBuilder()
              .setResourceId(overlayId)
              .setCatalogId(catalogId)
              .setDisplayName(name)
              .setIntegrationId(integrationId)
              .addAllIncludeNamespaces(includes)
              .addAllExcludeNamespaces(excludes)
              .setCreatedAt(now)
              .setUpdatedAt(now)
              .build();
      Map<String, Long> replacementParents = Map.copyOf(parentVersions);
      Set<String> replacementRequiredAbsent = Set.copyOf(requiredAbsent);
      Map<String, Long> replacementMarkers = Map.copyOf(markerVersions);
      if (!overlays.beginDeletion(
          current.value().getResourceId(), current.meta().getPointerVersion())) {
        throw new BaseResourceRepository.AbortRetryableException(
            "overlay changed while replacement fenced its old identity");
      }
      reconciler.retireMaterializedResources(current.value());
      return overlays
          .replaceIdentityAttachedWithMeta(
              current.value(),
              current.meta().getPointerVersion(),
              replacement,
              replacementParents,
              replacementRequiredAbsent,
              replacementMarkers)
          .orElseThrow(
              () ->
                  new BaseResourceRepository.AbortRetryableException(
                      "overlay, integration, or catalog changed during replacement"));
    }
    CatalogOverlay overlay =
        CatalogOverlay.newBuilder()
            .setResourceId(overlayId)
            .setDisplayName(name)
            .setIntegrationId(integrationId)
            .setCatalogId(catalogId)
            .addAllIncludeNamespaces(includes)
            .addAllExcludeNamespaces(excludes)
            .setCreatedAt(now)
            .setUpdatedAt(now)
            .build();
    try {
      return overlays
          .createAttachedWithMetaAndCompanions(
              overlay,
              Map.copyOf(requiredVersions),
              requiredAbsent,
              Map.copyOf(markerVersions),
              completion == null
                  ? null
                  : row ->
                      List.of(
                          completion.prepare(
                              new IdempotencyGuard.CommittedCreate<>(
                                  row.value(), row.value().getResourceId(), row.meta()))))
          .orElseThrow(
              () ->
                  new BaseResourceRepository.AbortRetryableException(
                      "overlay parent changed during creation"));
    } catch (BaseResourceRepository.NameConflictException e) {
      if (reservedIdentity) {
        var recovered = overlays.getByIdWithMeta(overlayId);
        if (recovered.isPresent())
          throw new BaseResourceRepository.AbortRetryableException(
              "overlay exists before its idempotency receipt committed");
        var owner = overlays.getByNameWithMeta(accountId, name);
        if (owner.isPresent() && owner.get().value().getResourceId().equals(overlayId)) {
          throw new BaseResourceRepository.AbortRetryableException(
              "overlay exists before its idempotency receipt committed");
        }
      }
      if (reservedIdentity) {
        throw new BaseResourceRepository.AbortRetryableException(
            "overlay create conflict not yet visible");
      }
      if (mode != CreateMode.CM_ERROR_IF_EXISTS)
        throw new BaseResourceRepository.AbortRetryableException(
            "overlay name owner changed during create");
      throw GrpcErrors.alreadyExists(
          corr, CATALOG_OVERLAY_ALREADY_EXISTS, Map.of("display_name", name));
    }
  }

  @Override
  public Uni<UpdateCatalogOverlayResponse> updateCatalogOverlay(
      UpdateCatalogOverlayRequest request) {
    var L = LogHelper.start(LOG, "UpdateCatalogOverlay");

    return mapFailures(
            runWithRetry(
                () -> {
                  var pc = principal.get();
                  authz.require(pc, RolePermissions.CATALOG_OVERLAY_WRITE);
                  String corr = pc.getCorrelationId();
                  ResourceId id = scopedOverlayId(pc.getAccountId(), request.getOverlayId());
                  requiredMask(request.hasUpdateMask() ? request.getUpdateMask() : null, corr);
                  var current =
                      overlays
                          .getByIdWithMeta(id)
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(
                                      corr, CATALOG_OVERLAY, Map.of("id", id.getId())));
                  MutationOps.BaseServiceChecks.enforcePreconditions(
                      corr, current.meta(), request.getPrecondition());
                  var desiredBuilder = current.value().toBuilder();
                  for (String path : request.getUpdateMask().getPathsList()) {
                    switch (path) {
                      case "display_name" ->
                          desiredBuilder.setDisplayName(
                              normalizeName(
                                  mustNonEmpty(
                                      request.getSpec().getDisplayName(), "display_name", corr)));
                      case "include_namespaces" ->
                          desiredBuilder
                              .clearIncludeNamespaces()
                              .addAllIncludeNamespaces(
                                  normalizePaths(
                                      request.getSpec().getIncludeNamespacesList(), path, corr));
                      case "exclude_namespaces" ->
                          desiredBuilder
                              .clearExcludeNamespaces()
                              .addAllExcludeNamespaces(
                                  normalizePaths(
                                      request.getSpec().getExcludeNamespacesList(), path, corr));
                      default ->
                          throw GrpcErrors.invalidArgument(corr, FIELD, Map.of("field", path));
                    }
                  }
                  CatalogOverlay desired = desiredBuilder.setUpdatedAt(nowTs()).build();
                  try {
                    var meta =
                        overlays
                            .updateWithMetaUnlessIntegrationDeleting(
                                desired, current.meta().getPointerVersion())
                            .orElseThrow(
                                () ->
                                    GrpcErrors.preconditionFailed(
                                        corr, CATALOG_OVERLAY_CHANGED, Map.of()));
                    return UpdateCatalogOverlayResponse.newBuilder()
                        .setOverlay(desired)
                        .setMeta(meta)
                        .build();
                  } catch (BaseResourceRepository.NameConflictException e) {
                    throw GrpcErrors.alreadyExists(
                        corr,
                        CATALOG_OVERLAY_ALREADY_EXISTS,
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
  public Uni<ReconcileCatalogOverlayResponse> reconcileCatalogOverlay(
      ReconcileCatalogOverlayRequest request) {
    return mapFailures(
        runWithRetry(
            () -> {
              var pc = principal.get();
              authz.require(pc, RolePermissions.CATALOG_OVERLAY_RECONCILE);
              authz.require(pc, RolePermissions.CATALOG_INTEGRATION_USE);
              String corr = pc.getCorrelationId();
              ResourceId overlayId = scopedOverlayId(pc.getAccountId(), request.getOverlayId());
              var current =
                  overlays
                      .getByIdWithMeta(overlayId)
                      .orElseThrow(
                          () -> GrpcErrors.notFound(corr, null, Map.of("id", overlayId.getId())));
              MutationOps.BaseServiceChecks.enforcePreconditions(
                  corr, current.meta(), request.getPrecondition());
              catalogSurfaceWritePolicy()
                  .requireWritableCatalog(current.value().getCatalogId(), "catalog_id", corr);
              var integration =
                  integrations
                      .getByIdWithMeta(current.value().getIntegrationId())
                      .orElseThrow(
                          () ->
                              new BaseResourceRepository.CorruptionException(
                                  "overlay references a missing Catalog Integration: "
                                      + current.value().getIntegrationId().getId(),
                                  null));
              var result =
                  reconciler.reconcile(
                      current.value(), current.meta(), integration.value(), integration.meta());
              return ReconcileCatalogOverlayResponse.newBuilder()
                  .setMeta(current.meta())
                  .setNamespacesCreated(result.namespacesCreated())
                  .setNamespacesDeleted(result.namespacesDeleted())
                  .setTablesCreated(result.tablesCreated())
                  .setTablesUpdated(result.tablesUpdated())
                  .setTablesDeleted(result.tablesDeleted())
                  .setViewsCreated(result.viewsCreated())
                  .setViewsUpdated(result.viewsUpdated())
                  .setViewsDeleted(result.viewsDeleted())
                  .build();
            }),
        correlationId());
  }

  @Override
  public Uni<DeleteCatalogOverlayResponse> deleteCatalogOverlay(
      DeleteCatalogOverlayRequest request) {
    var L = LogHelper.start(LOG, "DeleteCatalogOverlay");

    return mapFailures(
            runWithRetry(
                () -> {
                  var pc = principal.get();
                  authz.require(pc, RolePermissions.CATALOG_OVERLAY_DELETE);
                  String corr = pc.getCorrelationId();
                  ResourceId id = scopedOverlayId(pc.getAccountId(), request.getOverlayId());
                  var current = overlays.getByIdWithMeta(id);
                  if (current.isEmpty()) {
                    if (hasMeaningfulPrecondition(request.getPrecondition()))
                      throw GrpcErrors.notFound(corr, CATALOG_OVERLAY, Map.of("id", id.getId()));
                    return DeleteCatalogOverlayResponse.newBuilder()
                        .setMeta(overlays.metaForSafe(id))
                        .build();
                  }
                  var meta = current.get().meta();
                  MutationOps.BaseServiceChecks.enforcePreconditions(
                      corr, meta, request.getPrecondition());
                  if (!overlays.beginDeletion(id, meta.getPointerVersion()))
                    throw new BaseResourceRepository.AbortRetryableException(
                        "overlay changed while deletion was fenced");
                  reconciler.retireMaterializedResources(current.get().value());
                  // Managed descendants retire behind the fence first. The final transaction removes
                  // only the overlay, its dependencies, and the fence; the target catalog remains.
                  long fenceVersion = overlays.deletionFenceVersion(id);
                  if (fenceVersion == 0L
                      || !overlays.deleteWithFence(id, meta.getPointerVersion(), fenceVersion))
                    throw new BaseResourceRepository.AbortRetryableException(
                        "overlay changed during deletion");
                  return DeleteCatalogOverlayResponse.newBuilder().setMeta(meta).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  private CatalogSurfaceWritePolicy catalogSurfaceWritePolicy() {
    return new CatalogSurfaceWritePolicy(graphView);
  }

  private List<NamespacePath> normalizePaths(List<NamespacePath> paths, String field, String corr) {
    LinkedHashMap<String, NamespacePath> unique = new LinkedHashMap<>();
    for (NamespacePath path : paths) {
      if (path.getSegmentsCount() == 0)
        throw GrpcErrors.invalidArgument(corr, FIELD, Map.of("field", field));
      var normalized = NamespacePath.newBuilder();
      for (String segment : path.getSegmentsList())
        normalized.addSegments(normalizeName(mustNonEmpty(segment, field, corr)));
      NamespacePath value = normalized.build();
      unique.putIfAbsent(canonicalNamespacePath(value), value);
    }
    var result = new ArrayList<>(unique.values());
    result.sort(CatalogOverlaysImpl::compareNamespacePaths);
    return List.copyOf(result);
  }

  private static int compareNamespacePaths(NamespacePath left, NamespacePath right) {
    int common = Math.min(left.getSegmentsCount(), right.getSegmentsCount());
    for (int i = 0; i < common; i++) {
      int compared = left.getSegments(i).compareTo(right.getSegments(i));
      if (compared != 0) return compared;
    }
    return Integer.compare(left.getSegmentsCount(), right.getSegmentsCount());
  }

  private static FieldMask requiredMask(FieldMask mask, String corr) {
    if (mask == null
        || mask.getPathsCount() == 0
        || mask.getPathsList().stream().anyMatch(path -> !MUTABLE_PATHS.contains(path)))
      throw GrpcErrors.invalidArgument(corr, UPDATE_MASK_REQUIRED, Map.of("field", "update_mask"));
    return mask;
  }

  private ResourceId scopedOverlayId(String accountId, ResourceId id) {
    ensureKind(id, ResourceKind.RK_CATALOG_OVERLAY, "overlay_id", correlationId());
    validateScopedAccount(accountId, id, "overlay_id");
    return id.toBuilder().setAccountId(accountId).build();
  }

  private ResourceId scopedIntegrationId(String accountId, ResourceId id) {
    ensureKind(id, ResourceKind.RK_CATALOG_INTEGRATION, "integration_id", correlationId());
    validateScopedAccount(accountId, id, "integration_id");
    return id.toBuilder().setAccountId(accountId).build();
  }

  private ResourceId scopedCatalogId(String accountId, ResourceId id) {
    ensureKind(id, ResourceKind.RK_CATALOG, "catalog_id", correlationId());
    validateScopedAccount(accountId, id, "catalog_id");
    return id.toBuilder().setAccountId(accountId).build();
  }

  private void validateScopedAccount(String accountId, ResourceId id, String field) {
    mustNonEmpty(id.getAccountId(), field + ".account_id", correlationId());
    mustNonEmpty(id.getId(), field + ".id", correlationId());
    if (!accountId.equals(id.getAccountId()))
      throw GrpcErrors.invalidArgument(
          correlationId(), FIELD, Map.of("field", field + ".account_id"));
  }

  private static byte[] canonicalFingerprint(
      String name,
      ResourceId integrationId,
      ResourceId catalogId,
      List<NamespacePath> includes,
      List<NamespacePath> excludes) {
    var canonical =
        new Canonicalizer()
            .scalar("display_name", name)
            .scalar("integration_id", integrationId.getId())
            .scalar("catalog_id", catalogId.getId());
    includes.forEach(
        path -> canonical.scalar("include_namespaces[]", canonicalNamespacePath(path)));
    excludes.forEach(
        path -> canonical.scalar("exclude_namespaces[]", canonicalNamespacePath(path)));
    return canonical.bytes();
  }

  static byte[] canonicalFingerprintForTest(
      String name,
      ResourceId integrationId,
      ResourceId catalogId,
      List<NamespacePath> includes,
      List<NamespacePath> excludes) {
    return canonicalFingerprint(name, integrationId, catalogId, includes, excludes);
  }

  private static String canonicalNamespacePath(NamespacePath path) {
    return Base64.getEncoder().encodeToString(path.toByteArray());
  }
}
