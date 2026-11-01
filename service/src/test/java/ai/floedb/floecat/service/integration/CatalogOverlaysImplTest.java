/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package ai.floedb.floecat.service.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.common.rpc.CreateMode;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.integration.rpc.CatalogIntegration;
import ai.floedb.floecat.integration.rpc.CatalogOverlay;
import ai.floedb.floecat.integration.rpc.CatalogOverlaySpec;
import ai.floedb.floecat.integration.rpc.CreateCatalogOverlayRequest;
import ai.floedb.floecat.integration.rpc.CreateCatalogOverlayResponse;
import ai.floedb.floecat.integration.rpc.DeleteCatalogOverlayRequest;
import ai.floedb.floecat.integration.rpc.NamespacePath;
import ai.floedb.floecat.integration.rpc.ReconcileCatalogOverlayRequest;
import ai.floedb.floecat.integration.rpc.UpdateCatalogOverlayRequest;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.scanner.spi.CatalogGraphView;
import ai.floedb.floecat.service.catalog.impl.CascadeDeletion;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.CatalogIntegrationRepository;
import ai.floedb.floecat.service.repo.impl.CatalogOverlayRepository;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository.PointerConditions;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository.ResourceWithMeta;
import ai.floedb.floecat.service.repo.util.MarkerStore;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import com.google.protobuf.FieldMask;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class CatalogOverlaysImplTest {
  private CatalogOverlaysImpl service;
  private CascadeDeletion cascadeDeletion;

  @BeforeEach
  void setUp() {
    service = new CatalogOverlaysImpl();
    service.overlays = mock(CatalogOverlayRepository.class);
    service.integrations = mock(CatalogIntegrationRepository.class);
    service.catalogs = mock(CatalogRepository.class);
    cascadeDeletion = mock(CascadeDeletion.class);
    service.lifecycle = mock(CatalogOverlayLifecycle.class);
    service.markerStore = mock(MarkerStore.class);
    service.principal = mock(PrincipalProvider.class);
    service.authz = mock(Authorizer.class);
    service.idempotencyStore = mock(IdempotencyRepository.class);
    service.reconciler = mock(CatalogOverlayReconciler.class);
    service.graphView = mock(CatalogGraphView.class);
    installBasePrincipal(service, service.principal);
    when(service.principal.get()).thenReturn(principal());
    stubIntegration(integrationId(), 5L);
    stubCatalog(catalogId(), 7L);
    when(service.graphView.resolve(eq(catalogId()), any()))
        .thenReturn(Optional.of(catalogNode(catalogId())));
    when(service.markerStore.catalogIntegrationOverlaysMarkerVersion(integrationId()))
        .thenReturn(2L);
    when(service.markerStore.catalogOverlaysMarkerVersion(catalogId())).thenReturn(3L);
    when(service.lifecycle.deleteFenced(any(), anyLong(), anyLong(), any())).thenReturn(true);
    when(service.overlays.createAttachedWithMetaAndCompanions(any(), any(), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              CatalogOverlay value = invocation.getArgument(0);
              var meta = MutationMeta.newBuilder().setPointerVersion(1L).build();
              Function<ResourceWithMeta<CatalogOverlay>, List<PointerStore.CasOp>> companions =
                  invocation.getArgument(4);
              if (companions != null) companions.apply(new ResourceWithMeta<>(value, meta));
              return Optional.of(new ResourceWithMeta<>(value, meta));
            });
  }

  @Test
  void createReferencesExistingCatalogAndPublishesBothDependencies() {
    var response = create(CatalogOverlaySpec.newBuilder());

    var overlay = ArgumentCaptor.forClass(CatalogOverlay.class);
    @SuppressWarnings("unchecked")
    var parents = ArgumentCaptor.forClass(Map.class);
    @SuppressWarnings("unchecked")
    var requiredAbsent = ArgumentCaptor.forClass(Set.class);
    @SuppressWarnings("unchecked")
    var markers = ArgumentCaptor.forClass(Map.class);
    verify(service.overlays)
        .createAttachedWithMetaAndCompanions(
            overlay.capture(),
            parents.capture(),
            requiredAbsent.capture(),
            markers.capture(),
            any());

    assertEquals(catalogId(), response.getOverlay().getCatalogId());
    assertEquals(integrationId(), response.getOverlay().getIntegrationId());
    assertEquals(
        5L, parents.getValue().get(Keys.catalogIntegrationPointerById("acct", "integration")));
    assertEquals(7L, parents.getValue().get(Keys.catalogPointerById("acct", "catalog")));
    assertTrue(requiredAbsent.getValue().contains(Keys.catalogDeletionMarker("acct", "catalog")));
    assertEquals(
        2L, markers.getValue().get(Keys.catalogIntegrationOverlaysMarker("acct", "integration")));
    assertEquals(3L, markers.getValue().get(Keys.catalogOverlaysMarker("acct", "catalog")));
    assertFalse(response.getOverlay().getOwnsCatalog());
  }

  @Test
  void createWithoutCatalogAtomicallyCreatesOwnedCatalogWithOverlayName() {
    var response =
        service
            .createCatalogOverlay(
                CreateCatalogOverlayRequest.newBuilder()
                    .setSpec(
                        CatalogOverlaySpec.newBuilder()
                            .setDisplayName("sales")
                            .setIntegrationId(integrationId()))
                    .build())
            .await()
            .indefinitely();

    var catalog = ArgumentCaptor.forClass(Catalog.class);
    verify(service.catalogs).prepareCreateOps(catalog.capture());
    assertEquals("sales", catalog.getValue().getDisplayName());
    assertEquals(catalog.getValue().getResourceId(), response.getOverlay().getCatalogId());
    assertEquals(ResourceKind.RK_CATALOG, response.getOverlay().getCatalogId().getKind());
    assertEquals(true, response.getOverlay().getOwnsCatalog());
    verify(service.graphView, never()).resolve(eq(response.getOverlay().getCatalogId()), any());
  }

  @Test
  void existingOverlayWinsBeforeAutomaticCatalogNameResolution() {
    var current =
        overlay(
            id("existing", ResourceKind.RK_CATALOG_OVERLAY), integrationId(), catalogId(), "sales");
    when(service.overlays.getByNameWithMeta("acct", "sales"))
        .thenReturn(Optional.of(row(current, 3L)));

    var error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .createCatalogOverlay(
                        CreateCatalogOverlayRequest.newBuilder()
                            .setSpec(
                                CatalogOverlaySpec.newBuilder()
                                    .setDisplayName("sales")
                                    .setIntegrationId(integrationId()))
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.ALREADY_EXISTS, error.getStatus().getCode());
    verify(service.catalogs, never()).getByName("acct", "sales");
  }

  @Test
  void automaticCatalogCreationRejectsAnUnownedCatalogNameCollision() {
    when(service.catalogs.getByName("acct", "sales"))
        .thenReturn(
            Optional.of(
                Catalog.newBuilder().setResourceId(catalogId()).setDisplayName("sales").build()));

    var error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .createCatalogOverlay(
                        CreateCatalogOverlayRequest.newBuilder()
                            .setSpec(
                                CatalogOverlaySpec.newBuilder()
                                    .setDisplayName("sales")
                                    .setIntegrationId(integrationId()))
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.ALREADY_EXISTS, error.getStatus().getCode());
    verify(service.overlays, never()).acquireDeletionFence(any(), anyLong());
  }

  @Test
  void concurrentAutomaticCreateRetriesToReturnWinningOverlay() {
    var winner =
        overlay(
                id("winner", ResourceKind.RK_CATALOG_OVERLAY),
                integrationId(),
                id("winner-catalog", ResourceKind.RK_CATALOG),
                "sales")
            .toBuilder()
            .setOwnsCatalog(true)
            .build();
    when(service.overlays.getByNameWithMeta("acct", "sales"))
        .thenReturn(Optional.empty(), Optional.of(row(winner, 2L)));
    when(service.overlays.createAttachedWithMetaAndCompanions(any(), any(), any(), any(), any()))
        .thenThrow(new BaseResourceRepository.NameConflictException("concurrent create"));

    var response =
        service
            .createCatalogOverlay(
                CreateCatalogOverlayRequest.newBuilder()
                    .setCreateMode(CreateMode.CM_RETURN_EXISTING)
                    .setSpec(
                        CatalogOverlaySpec.newBuilder()
                            .setDisplayName("sales")
                            .setIntegrationId(integrationId()))
                    .build())
            .await()
            .indefinitely();

    assertEquals(winner, response.getOverlay());
  }

  @Test
  void createAllowsOverlayAndCatalogToHaveTheSameName() {
    var response = create(CatalogOverlaySpec.newBuilder());

    assertEquals("sales", response.getOverlay().getDisplayName());
    verify(service.catalogs, never()).getByName(any(), any());
  }

  @Test
  void createRejectsMissingTargetCatalog() {
    when(service.catalogs.getByIdWithMeta(catalogId())).thenReturn(Optional.empty());

    var error =
        assertThrows(StatusRuntimeException.class, () -> create(CatalogOverlaySpec.newBuilder()));

    assertEquals(Status.Code.NOT_FOUND, error.getStatus().getCode());
    verify(service.overlays, never())
        .createAttachedWithMetaAndCompanions(any(), any(), any(), any(), any());
  }

  @Test
  void renameChangesOnlyTheOverlay() {
    var overlayId = id("overlay", ResourceKind.RK_CATALOG_OVERLAY);
    var current = overlay(overlayId, integrationId(), catalogId(), "sales");
    when(service.overlays.getByIdWithMeta(overlayId)).thenReturn(Optional.of(row(current, 7L)));
    when(service.overlays.updateWithMetaUnlessIntegrationDeleting(any(), eq(7L)))
        .thenReturn(Optional.of(MutationMeta.newBuilder().setPointerVersion(8L).build()));

    var response =
        service
            .updateCatalogOverlay(
                UpdateCatalogOverlayRequest.newBuilder()
                    .setOverlayId(overlayId)
                    .setSpec(CatalogOverlaySpec.newBuilder().setDisplayName("revenue"))
                    .setUpdateMask(FieldMask.newBuilder().addPaths("display_name"))
                    .build())
            .await()
            .indefinitely();

    assertEquals("revenue", response.getOverlay().getDisplayName());
    assertEquals(catalogId(), response.getOverlay().getCatalogId());
    verify(service.catalogs, never()).getByName(any(), any());
    verify(service.catalogs, never()).update(any(), anyLong());
  }

  @Test
  void catalogBindingIsImmutableThroughUpdate() {
    var error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .updateCatalogOverlay(
                        UpdateCatalogOverlayRequest.newBuilder()
                            .setOverlayId(id("overlay", ResourceKind.RK_CATALOG_OVERLAY))
                            .setSpec(
                                CatalogOverlaySpec.newBuilder()
                                    .setCatalogId(id("other", ResourceKind.RK_CATALOG)))
                            .setUpdateMask(FieldMask.newBuilder().addPaths("catalog_id"))
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
  }

  @Test
  void replacingCreateMayRebindIntegrationAndCatalog() {
    var oldOverlayId = id("old-overlay", ResourceKind.RK_CATALOG_OVERLAY);
    var oldIntegrationId = id("old-integration", ResourceKind.RK_CATALOG_INTEGRATION);
    var oldCatalogId = id("old-catalog", ResourceKind.RK_CATALOG);
    var current = overlay(oldOverlayId, oldIntegrationId, oldCatalogId, "sales");
    when(service.overlays.getByNameWithMeta("acct", "sales"))
        .thenReturn(Optional.of(row(current, 9L)));
    stubIntegration(oldIntegrationId, 11L);
    stubCatalog(oldCatalogId, 13L);
    when(service.markerStore.catalogIntegrationOverlaysMarkerVersion(oldIntegrationId))
        .thenReturn(4L);
    when(service.markerStore.catalogOverlaysMarkerVersion(oldCatalogId)).thenReturn(6L);
    when(service.overlays.acquireDeletionFence(oldOverlayId, 9L))
        .thenReturn(
            Optional.of(
                new ai.floedb.floecat.service.repo.util.DurableDeletionFence.Fence(1L, true)));
    when(service.overlays.replaceIdentityAttachedWithMeta(
            any(), eq(9L), any(), any(), any(), any(), any(), any()))
        .thenAnswer(invocation -> Optional.of(row(invocation.getArgument(2), 1L)));

    var response =
        service
            .createCatalogOverlay(
                CreateCatalogOverlayRequest.newBuilder()
                    .setCreateMode(CreateMode.CM_REPLACE)
                    .setSpec(baseSpec())
                    .build())
            .await()
            .indefinitely();

    assertNotEquals(oldOverlayId, response.getOverlay().getResourceId());
    assertEquals(integrationId(), response.getOverlay().getIntegrationId());
    assertEquals(catalogId(), response.getOverlay().getCatalogId());
    @SuppressWarnings("unchecked")
    var markers = ArgumentCaptor.forClass(Map.class);
    verify(service.overlays)
        .replaceIdentityAttachedWithMeta(
            any(), eq(9L), any(), any(), any(), markers.capture(), any(), any());
    verify(service.reconciler).retireMaterializedResources(current);
    assertEquals(4, markers.getValue().size());
  }

  @Test
  void replacingOwnedOverlayWithoutCatalogReusesItsOwnedCatalog() {
    var oldOverlayId = id("old-overlay", ResourceKind.RK_CATALOG_OVERLAY);
    var current =
        overlay(oldOverlayId, integrationId(), catalogId(), "sales").toBuilder()
            .setOwnsCatalog(true)
            .build();
    when(service.overlays.getByNameWithMeta("acct", "sales"))
        .thenReturn(Optional.of(row(current, 9L)));
    when(service.overlays.acquireDeletionFence(oldOverlayId, 9L))
        .thenReturn(Optional.of(fence(1L, true)));
    when(service.overlays.replaceIdentityAttachedWithMeta(
            any(), eq(9L), any(), any(), any(), any(), any(), any()))
        .thenAnswer(invocation -> Optional.of(row(invocation.getArgument(2), 1L)));

    var response =
        service
            .createCatalogOverlay(
                CreateCatalogOverlayRequest.newBuilder()
                    .setCreateMode(CreateMode.CM_REPLACE)
                    .setSpec(
                        CatalogOverlaySpec.newBuilder()
                            .setDisplayName("sales")
                            .setIntegrationId(integrationId()))
                    .build())
            .await()
            .indefinitely();

    assertEquals(catalogId(), response.getOverlay().getCatalogId());
    assertTrue(response.getOverlay().getOwnsCatalog());
    verify(service.catalogs, never()).prepareCreateOps(any());
    verify(service.catalogs, never()).prepareDeleteOps(any());
    verify(service.lifecycle, never()).preflightOwnedCatalogRemoval(any(), any());
  }

  @Test
  void replacingOwnedOverlayWithExplicitCatalogDeletesOldOwnedCatalog() {
    installRealLifecycle();
    var oldOverlayId = id("old-overlay", ResourceKind.RK_CATALOG_OVERLAY);
    var oldCatalogId = id("old-catalog", ResourceKind.RK_CATALOG);
    var current =
        overlay(oldOverlayId, integrationId(), oldCatalogId, "sales").toBuilder()
            .setOwnsCatalog(true)
            .build();
    var oldCatalog =
        Catalog.newBuilder().setResourceId(oldCatalogId).setDisplayName("sales").build();
    when(service.overlays.getByNameWithMeta("acct", "sales"))
        .thenReturn(Optional.of(row(current, 9L)));
    when(service.catalogs.getByIdWithMeta(oldCatalogId))
        .thenReturn(Optional.of(row(oldCatalog, 13L)));
    when(service.overlays.countByCatalog("acct", "old-catalog")).thenReturn(1);
    when(service.markerStore.catalogOverlaysMarkerVersion(oldCatalogId)).thenReturn(6L);
    var childMarkers =
        new MarkerStore.MarkerRemoval(
            new PointerConditions(Map.of(), Set.of(), Map.of()), Map.of());
    when(cascadeDeletion.prepareCatalogDeletion(
            eq(oldCatalogId), any(), any(Runnable.class), eq(oldOverlayId)))
        .thenReturn(new CascadeDeletion.PreparedCatalogDeletion(fence(17L, true), childMarkers));
    when(service.catalogs.prepareDeleteOps(oldCatalogId)).thenReturn(List.of());
    when(service.overlays.acquireDeletionFence(oldOverlayId, 9L))
        .thenReturn(Optional.of(fence(1L, true)));
    when(service.overlays.replaceIdentityAttachedWithMeta(
            any(), eq(9L), any(), any(), any(), any(), any(), any()))
        .thenAnswer(invocation -> Optional.of(row(invocation.getArgument(2), 1L)));

    var response =
        service
            .createCatalogOverlay(
                CreateCatalogOverlayRequest.newBuilder()
                    .setCreateMode(CreateMode.CM_REPLACE)
                    .setSpec(baseSpec())
                    .build())
            .await()
            .indefinitely();

    assertFalse(response.getOverlay().getOwnsCatalog());
    assertEquals(catalogId(), response.getOverlay().getCatalogId());
    verify(service.catalogs).prepareDeleteOps(oldCatalogId);
  }

  @Test
  void replacingExplicitOverlayWithoutCatalogCreatesOwnedCatalog() {
    var oldOverlayId = id("old-overlay", ResourceKind.RK_CATALOG_OVERLAY);
    var oldCatalogId = id("old-catalog", ResourceKind.RK_CATALOG);
    var current = overlay(oldOverlayId, integrationId(), oldCatalogId, "sales");
    when(service.overlays.getByNameWithMeta("acct", "sales"))
        .thenReturn(Optional.of(row(current, 9L)));
    when(service.catalogs.getByIdWithMeta(oldCatalogId))
        .thenReturn(
            Optional.of(
                row(
                    Catalog.newBuilder()
                        .setResourceId(oldCatalogId)
                        .setDisplayName("external")
                        .build(),
                    13L)));
    when(service.markerStore.catalogOverlaysMarkerVersion(oldCatalogId)).thenReturn(6L);
    when(service.overlays.acquireDeletionFence(oldOverlayId, 9L))
        .thenReturn(Optional.of(fence(1L, true)));
    when(service.overlays.replaceIdentityAttachedWithMeta(
            any(), eq(9L), any(), any(), any(), any(), any(), any()))
        .thenAnswer(invocation -> Optional.of(row(invocation.getArgument(2), 1L)));

    var response =
        service
            .createCatalogOverlay(
                CreateCatalogOverlayRequest.newBuilder()
                    .setCreateMode(CreateMode.CM_REPLACE)
                    .setSpec(
                        CatalogOverlaySpec.newBuilder()
                            .setDisplayName("sales")
                            .setIntegrationId(integrationId()))
                    .build())
            .await()
            .indefinitely();

    assertTrue(response.getOverlay().getOwnsCatalog());
    assertNotEquals(oldCatalogId, response.getOverlay().getCatalogId());
    verify(service.catalogs).prepareCreateOps(any());
    verify(service.catalogs, never()).prepareDeleteOps(oldCatalogId);
  }

  @Test
  void automaticCatalogNameRaceDuringReplaceReturnsCatalogConflict() {
    var oldOverlayId = id("old-overlay", ResourceKind.RK_CATALOG_OVERLAY);
    var oldCatalogId = id("old-catalog", ResourceKind.RK_CATALOG);
    var current = overlay(oldOverlayId, integrationId(), oldCatalogId, "sales");
    when(service.overlays.getByNameWithMeta("acct", "sales"))
        .thenReturn(Optional.of(row(current, 9L)));
    when(service.catalogs.getByName("acct", "sales"))
        .thenReturn(
            Optional.empty(),
            Optional.of(
                Catalog.newBuilder().setResourceId(catalogId()).setDisplayName("sales").build()));
    when(service.catalogs.getByIdWithMeta(oldCatalogId))
        .thenReturn(
            Optional.of(
                row(
                    Catalog.newBuilder()
                        .setResourceId(oldCatalogId)
                        .setDisplayName("external")
                        .build(),
                    13L)));
    when(service.overlays.acquireDeletionFence(oldOverlayId, 9L))
        .thenReturn(Optional.of(fence(1L, true)));
    when(service.overlays.replaceIdentityAttachedWithMeta(
            any(), eq(9L), any(), any(), any(), any(), any(), any()))
        .thenThrow(new BaseResourceRepository.NameConflictException("catalog name claimed"));

    var error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .createCatalogOverlay(
                        CreateCatalogOverlayRequest.newBuilder()
                            .setCreateMode(CreateMode.CM_REPLACE)
                            .setSpec(
                                CatalogOverlaySpec.newBuilder()
                                    .setDisplayName("sales")
                                    .setIntegrationId(integrationId()))
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.ALREADY_EXISTS, error.getStatus().getCode());
  }

  @Test
  void replacementValidatesTheOldCatalogBeforeInstallingADeletionFence() {
    var oldOverlayId = id("old-overlay", ResourceKind.RK_CATALOG_OVERLAY);
    var missingOldCatalogId = id("missing-old-catalog", ResourceKind.RK_CATALOG);
    var current = overlay(oldOverlayId, integrationId(), missingOldCatalogId, "sales");
    when(service.overlays.getByNameWithMeta("acct", "sales"))
        .thenReturn(Optional.of(row(current, 9L)));

    var error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .createCatalogOverlay(
                        CreateCatalogOverlayRequest.newBuilder()
                            .setCreateMode(CreateMode.CM_REPLACE)
                            .setSpec(baseSpec())
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.INTERNAL, error.getStatus().getCode());
    verify(service.overlays, never()).acquireDeletionFence(any(), anyLong());
    verify(service.reconciler, never()).retireMaterializedResources(any());
  }

  @Test
  void createRejectsASystemCatalogBeforeMaterializationCanBeScheduled() {
    ResourceId systemCatalogId =
        SystemNodeRegistry.systemCatalogContainerId("floedb").toBuilder()
            .setAccountId("acct")
            .build();

    var error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .createCatalogOverlay(
                        CreateCatalogOverlayRequest.newBuilder()
                            .setSpec(baseSpec().toBuilder().setCatalogId(systemCatalogId))
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.PERMISSION_DENIED, error.getStatus().getCode());
    verify(service.overlays, never())
        .createAttachedWithMetaAndCompanions(any(), any(), any(), any(), any());
  }

  @Test
  void reconcileReportsMaterializedMetadataChanges() {
    service.authz = new Authorizer();
    var overlayId = id("overlay", ResourceKind.RK_CATALOG_OVERLAY);
    var current = overlay(overlayId, integrationId(), catalogId(), "sales");
    var overlayMeta = MutationMeta.newBuilder().setPointerVersion(7L).build();
    var integration = CatalogIntegration.newBuilder().setResourceId(integrationId()).build();
    var integrationMeta = MutationMeta.newBuilder().setPointerVersion(5L).build();
    when(service.principal.get())
        .thenReturn(
            PrincipalContext.newBuilder()
                .setAccountId("acct")
                .setCorrelationId("corr")
                .addAllPermissions(Set.of("catalog-overlay.reconcile", "catalog-integration.use"))
                .build());
    when(service.overlays.getByIdWithMeta(overlayId))
        .thenReturn(Optional.of(new ResourceWithMeta<>(current, overlayMeta)));
    when(service.integrations.getByIdWithMeta(integrationId()))
        .thenReturn(Optional.of(new ResourceWithMeta<>(integration, integrationMeta)));
    when(service.reconciler.reconcile(current, overlayMeta, integration, integrationMeta, true))
        .thenReturn(new CatalogOverlayReconciler.Result(2, 1, 3, 4, 5, 6, 7, 8));

    var response =
        service
            .reconcileCatalogOverlay(
                ReconcileCatalogOverlayRequest.newBuilder()
                    .setOverlayId(overlayId)
                    .setSkipColumnIdentityReconciliation(true)
                    .build())
            .await()
            .indefinitely();

    assertEquals(2, response.getNamespacesCreated());
    assertEquals(1, response.getNamespacesDeleted());
    assertEquals(3, response.getTablesCreated());
    assertEquals(4, response.getTablesUpdated());
    assertEquals(5, response.getTablesDeleted());
    assertEquals(6, response.getViewsCreated());
    assertEquals(7, response.getViewsUpdated());
    assertEquals(8, response.getViewsDeleted());
    verify(service.reconciler).reconcile(current, overlayMeta, integration, integrationMeta, true);
  }

  @Test
  void reconcileRejectsAPreviouslyPersistedSystemCatalogOverlay() {
    service.authz = new Authorizer();
    ResourceId systemCatalogId =
        SystemNodeRegistry.systemCatalogContainerId("floedb").toBuilder()
            .setAccountId("acct")
            .build();
    var overlayId = id("overlay", ResourceKind.RK_CATALOG_OVERLAY);
    var current = overlay(overlayId, integrationId(), systemCatalogId, "sales");
    when(service.principal.get())
        .thenReturn(
            PrincipalContext.newBuilder()
                .setAccountId("acct")
                .setCorrelationId("corr")
                .addAllPermissions(Set.of("catalog-overlay.reconcile", "catalog-integration.use"))
                .build());
    when(service.overlays.getByIdWithMeta(overlayId)).thenReturn(Optional.of(row(current, 7L)));

    var error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .reconcileCatalogOverlay(
                        ReconcileCatalogOverlayRequest.newBuilder().setOverlayId(overlayId).build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.PERMISSION_DENIED, error.getStatus().getCode());
    verify(service.reconciler, never()).reconcile(any(), any(), any(), any(), anyBoolean());
  }

  @Test
  void overlayWriteDoesNotAuthorizeDelete() {
    service.authz = new Authorizer();
    var overlayId = id("overlay", ResourceKind.RK_CATALOG_OVERLAY);
    when(service.principal.get())
        .thenReturn(
            PrincipalContext.newBuilder()
                .setAccountId("acct")
                .setCorrelationId("corr")
                .addPermissions("catalog-overlay.write")
                .build());

    var error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .deleteCatalogOverlay(
                        DeleteCatalogOverlayRequest.newBuilder().setOverlayId(overlayId).build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.PERMISSION_DENIED, error.getStatus().getCode());
    verify(service.overlays, never()).getByIdWithMeta(any());
  }

  @Test
  void deleteInstallsFenceAndLeavesCatalogUntouched() {
    var overlayId = id("overlay", ResourceKind.RK_CATALOG_OVERLAY);
    var current = overlay(overlayId, integrationId(), catalogId(), "sales");
    when(service.overlays.getByIdWithMeta(overlayId)).thenReturn(Optional.of(row(current, 7L)));
    when(service.overlays.acquireDeletionFence(overlayId, 7L))
        .thenReturn(
            Optional.of(
                new ai.floedb.floecat.service.repo.util.DurableDeletionFence.Fence(1L, true)));

    service
        .deleteCatalogOverlay(
            DeleteCatalogOverlayRequest.newBuilder().setOverlayId(overlayId).build())
        .await()
        .indefinitely();

    verify(service.overlays).acquireDeletionFence(overlayId, 7L);
    verify(service.reconciler).retireMaterializedResources(current);
    verify(service.lifecycle).deleteFenced(current, 7L, 1L, "corr");
    verify(service.catalogs, never()).delete(any());
  }

  @Test
  void deleteOwnedOverlayDeletesItsCatalogInTheSameFencedMutation() {
    var overlayId = id("overlay", ResourceKind.RK_CATALOG_OVERLAY);
    var current =
        overlay(overlayId, integrationId(), catalogId(), "sales").toBuilder()
            .setOwnsCatalog(true)
            .build();
    when(service.overlays.getByIdWithMeta(overlayId)).thenReturn(Optional.of(row(current, 7L)));
    when(service.overlays.acquireDeletionFence(overlayId, 7L))
        .thenReturn(Optional.of(fence(1L, true)));
    when(service.overlays.countByCatalog("acct", "catalog")).thenReturn(1);
    when(service.catalogs.getByIdWithMeta(catalogId()))
        .thenReturn(
            Optional.of(
                row(
                    Catalog.newBuilder().setResourceId(catalogId()).setDisplayName("sales").build(),
                    5L)));
    when(service.markerStore.catalogChildSetMarkers(catalogId()))
        .thenReturn(
            new MarkerStore.MarkerRemoval(
                new PointerConditions(Map.of(), Set.of(), Map.of()), Map.of()));
    when(service.catalogs.prepareDeleteOps(catalogId())).thenReturn(List.of());

    service
        .deleteCatalogOverlay(
            DeleteCatalogOverlayRequest.newBuilder().setOverlayId(overlayId).build())
        .await()
        .indefinitely();

    verify(service.lifecycle).preflightOwnedCatalogRemoval(current, "corr");
    verify(service.lifecycle).deleteFenced(current, 7L, 1L, "corr");
    verify(service.reconciler, never()).retireMaterializedResources(current);
  }

  @Test
  void ownedDeleteRejectsSharingBeforeFencingOrRetiringContent() {
    installRealLifecycle();
    var overlayId = id("overlay", ResourceKind.RK_CATALOG_OVERLAY);
    var current =
        overlay(overlayId, integrationId(), catalogId(), "sales").toBuilder()
            .setOwnsCatalog(true)
            .build();
    when(service.overlays.getByIdWithMeta(overlayId)).thenReturn(Optional.of(row(current, 7L)));
    when(service.overlays.countByCatalog("acct", "catalog")).thenReturn(2);
    when(service.catalogs.getByIdWithMeta(catalogId()))
        .thenReturn(
            Optional.of(
                row(
                    Catalog.newBuilder().setResourceId(catalogId()).setDisplayName("sales").build(),
                    5L)));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            service
                .deleteCatalogOverlay(
                    DeleteCatalogOverlayRequest.newBuilder().setOverlayId(overlayId).build())
                .await()
                .indefinitely());

    verify(service.overlays, never()).acquireDeletionFence(any(), anyLong());
    verify(service.reconciler, never()).retireMaterializedResources(any());
  }

  @Test
  void ownedDeleteRequiresEveryCatalogCascadePermissionBeforeFencing() {
    service.authz = new Authorizer();
    when(service.principal.get())
        .thenReturn(
            PrincipalContext.newBuilder()
                .setAccountId("acct")
                .setCorrelationId("corr")
                .addPermissions("catalog-overlay.delete")
                .addPermissions("catalog.write")
                .addPermissions("table.write")
                .addPermissions("view.write")
                .build());
    var overlayId = id("overlay", ResourceKind.RK_CATALOG_OVERLAY);
    var current =
        overlay(overlayId, integrationId(), catalogId(), "sales").toBuilder()
            .setOwnsCatalog(true)
            .build();
    when(service.overlays.getByIdWithMeta(overlayId)).thenReturn(Optional.of(row(current, 7L)));

    var error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .deleteCatalogOverlay(
                        DeleteCatalogOverlayRequest.newBuilder().setOverlayId(overlayId).build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.PERMISSION_DENIED, error.getStatus().getCode());
    verify(service.overlays, never()).acquireDeletionFence(any(), anyLong());
  }

  @Test
  void ownedFinalDeleteRaceKeepsDurableFencesForRetry() {
    installRealLifecycle();
    var overlayId = id("overlay", ResourceKind.RK_CATALOG_OVERLAY);
    var current =
        overlay(overlayId, integrationId(), catalogId(), "sales").toBuilder()
            .setOwnsCatalog(true)
            .build();
    var catalog = Catalog.newBuilder().setResourceId(catalogId()).setDisplayName("sales").build();
    when(service.catalogs.getByIdWithMeta(catalogId())).thenReturn(Optional.of(row(catalog, 5L)));
    when(service.overlays.countByCatalog("acct", "catalog")).thenReturn(1);
    var childMarkers =
        new MarkerStore.MarkerRemoval(
            new PointerConditions(Map.of(), Set.of(), Map.of()), Map.of());
    when(cascadeDeletion.prepareCatalogDeletion(
            eq(catalogId()), any(), any(Runnable.class), eq(overlayId)))
        .thenReturn(new CascadeDeletion.PreparedCatalogDeletion(fence(2L, true), childMarkers));
    when(service.catalogs.prepareDeleteOps(catalogId())).thenReturn(List.of());
    when(service.overlays.deleteWithFenceAndCompanions(
            eq(overlayId), eq(7L), eq(1L), any(), any(), any()))
        .thenReturn(false);

    assertFalse(service.lifecycle.deleteFenced(current, 7L, 1L, "corr"));

    verify(service.overlays, never()).cancelDeletion(any(), anyLong(), anyLong());
  }

  @Test
  void failedCatalogCascadeLeavesOverlayFenceInPlace() {
    installRealLifecycle();
    var overlayId = id("overlay", ResourceKind.RK_CATALOG_OVERLAY);
    var current =
        overlay(overlayId, integrationId(), catalogId(), "sales").toBuilder()
            .setOwnsCatalog(true)
            .build();
    when(service.catalogs.getByIdWithMeta(catalogId()))
        .thenReturn(
            Optional.of(
                row(
                    Catalog.newBuilder().setResourceId(catalogId()).setDisplayName("sales").build(),
                    5L)));
    when(service.overlays.countByCatalog("acct", "catalog")).thenReturn(1);
    when(cascadeDeletion.prepareCatalogDeletion(
            eq(catalogId()), any(), any(Runnable.class), eq(overlayId)))
        .thenThrow(new BaseResourceRepository.AbortRetryableException("namespace changed"));

    assertThrows(
        BaseResourceRepository.AbortRetryableException.class,
        () -> service.lifecycle.deleteFenced(current, 7L, 1L, "corr"));

    verify(service.overlays, never()).cancelDeletion(any(), anyLong(), anyLong());
  }

  private void installRealLifecycle() {
    service.lifecycle = new CatalogOverlayLifecycle();
    service.lifecycle.overlays = service.overlays;
    service.lifecycle.catalogs = service.catalogs;
    service.lifecycle.cascadeDeletion = cascadeDeletion;
  }

  private static ai.floedb.floecat.service.repo.util.DurableDeletionFence.Fence fence(
      long version, boolean newlyInstalled) {
    return new ai.floedb.floecat.service.repo.util.DurableDeletionFence.Fence(
        version, newlyInstalled);
  }

  @Test
  void selectionIsNormalizedDeduplicatedAndSegmentBoundariesArePreserved() {
    create(
        CatalogOverlaySpec.newBuilder()
            .addIncludeNamespaces(path(" b "))
            .addIncludeNamespaces(path("a", "b"))
            .addIncludeNamespaces(path("a", "b"))
            .addIncludeNamespaces(path("a.b")));

    var captured = ArgumentCaptor.forClass(CatalogOverlay.class);
    verify(service.overlays)
        .createAttachedWithMetaAndCompanions(captured.capture(), any(), any(), any(), any());
    assertEquals(
        List.of(path("a", "b"), path("a.b"), path("b")),
        captured.getValue().getIncludeNamespacesList());
  }

  @Test
  void catalogBindingParticipatesInIdempotencyFingerprint() {
    byte[] first =
        CatalogOverlaysImpl.canonicalFingerprintForTest(
            "sales", integrationId(), catalogId(), List.of(path("a")), List.of());
    byte[] second =
        CatalogOverlaysImpl.canonicalFingerprintForTest(
            "sales",
            integrationId(),
            id("other", ResourceKind.RK_CATALOG),
            List.of(path("a")),
            List.of());

    assertFalse(Arrays.equals(first, second));
  }

  private CreateCatalogOverlayResponse create(CatalogOverlaySpec.Builder spec) {
    return service
        .createCatalogOverlay(
            CreateCatalogOverlayRequest.newBuilder().setSpec(spec.mergeFrom(baseSpec())).build())
        .await()
        .indefinitely();
  }

  private CatalogOverlaySpec baseSpec() {
    return CatalogOverlaySpec.newBuilder()
        .setDisplayName("sales")
        .setIntegrationId(integrationId())
        .setCatalogId(catalogId())
        .build();
  }

  private void stubIntegration(ResourceId id, long version) {
    var value = CatalogIntegration.newBuilder().setResourceId(id).build();
    when(service.integrations.getByIdWithMeta(id)).thenReturn(Optional.of(row(value, version)));
  }

  private void stubCatalog(ResourceId id, long version) {
    var value = Catalog.newBuilder().setResourceId(id).setDisplayName("analytics").build();
    when(service.catalogs.getByIdWithMeta(id)).thenReturn(Optional.of(row(value, version)));
  }

  private static <T> ResourceWithMeta<T> row(T value, long version) {
    return new ResourceWithMeta<>(
        value, MutationMeta.newBuilder().setPointerVersion(version).build());
  }

  private static CatalogOverlay overlay(
      ResourceId overlayId, ResourceId integrationId, ResourceId catalogId, String name) {
    return CatalogOverlay.newBuilder()
        .setResourceId(overlayId)
        .setDisplayName(name)
        .setIntegrationId(integrationId)
        .setCatalogId(catalogId)
        .build();
  }

  private static PrincipalContext principal() {
    return PrincipalContext.newBuilder()
        .setAccountId("acct")
        .setCorrelationId("corr")
        .addAllPermissions(
            Set.of(
                "catalog-overlay.read",
                "catalog-overlay.write",
                "catalog-integration.use",
                "catalog.write"))
        .build();
  }

  private static CatalogNode catalogNode(ResourceId id) {
    return new CatalogNode(
        id,
        "blob://catalog",
        "analytics",
        Map.of(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Map.of());
  }

  private static NamespacePath path(String... segments) {
    return NamespacePath.newBuilder().addAllSegments(List.of(segments)).build();
  }

  private static ResourceId integrationId() {
    return id("integration", ResourceKind.RK_CATALOG_INTEGRATION);
  }

  private static ResourceId catalogId() {
    return id("catalog", ResourceKind.RK_CATALOG);
  }

  private static ResourceId id(String value, ResourceKind kind) {
    return ResourceId.newBuilder().setAccountId("acct").setId(value).setKind(kind).build();
  }

  private static void installBasePrincipal(
      CatalogOverlaysImpl service, PrincipalProvider provider) {
    try {
      Field field =
          ai.floedb.floecat.service.common.BaseServiceImpl.class.getDeclaredField("principal");
      field.setAccessible(true);
      field.set(service, provider);
    } catch (ReflectiveOperationException e) {
      throw new AssertionError(e);
    }
  }
}
