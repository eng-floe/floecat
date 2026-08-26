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
import static org.mockito.ArgumentMatchers.any;
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
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.CatalogIntegrationRepository;
import ai.floedb.floecat.service.repo.impl.CatalogOverlayRepository;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.model.Keys;
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

  @BeforeEach
  void setUp() {
    service = new CatalogOverlaysImpl();
    service.overlays = mock(CatalogOverlayRepository.class);
    service.integrations = mock(CatalogIntegrationRepository.class);
    service.catalogs = mock(CatalogRepository.class);
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
    when(service.graphView.resolve(catalogId())).thenReturn(Optional.of(catalogNode(catalogId())));
    when(service.markerStore.catalogIntegrationOverlaysMarkerVersion(integrationId()))
        .thenReturn(2L);
    when(service.markerStore.catalogOverlaysMarkerVersion(catalogId())).thenReturn(3L);
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
    var markers = ArgumentCaptor.forClass(Map.class);
    verify(service.overlays)
        .createAttachedWithMetaAndCompanions(
            overlay.capture(), parents.capture(), any(), markers.capture(), any());

    assertEquals(catalogId(), response.getOverlay().getCatalogId());
    assertEquals(integrationId(), response.getOverlay().getIntegrationId());
    assertEquals(
        5L, parents.getValue().get(Keys.catalogIntegrationPointerById("acct", "integration")));
    assertEquals(7L, parents.getValue().get(Keys.catalogPointerById("acct", "catalog")));
    assertEquals(
        2L, markers.getValue().get(Keys.catalogIntegrationOverlaysMarker("acct", "integration")));
    assertEquals(3L, markers.getValue().get(Keys.catalogOverlaysMarker("acct", "catalog")));
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
    when(service.overlays.beginDeletion(oldOverlayId, 9L)).thenReturn(true);
    when(service.overlays.replaceIdentityAttachedWithMeta(
            any(), eq(9L), any(), any(), any(), any()))
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
        .replaceIdentityAttachedWithMeta(any(), eq(9L), any(), any(), any(), markers.capture());
    verify(service.reconciler).retireMaterializedResources(current);
    assertEquals(4, markers.getValue().size());
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
    verify(service.overlays, never()).beginDeletion(any(), anyLong());
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
    when(service.reconciler.reconcile(current, overlayMeta, integration, integrationMeta))
        .thenReturn(new CatalogOverlayReconciler.Result(2, 1, 3, 4, 5, 6, 7, 8));

    var response =
        service
            .reconcileCatalogOverlay(
                ReconcileCatalogOverlayRequest.newBuilder().setOverlayId(overlayId).build())
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
    verify(service.reconciler, never()).reconcile(any(), any(), any(), any());
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
    when(service.overlays.beginDeletion(overlayId, 7L)).thenReturn(true);
    when(service.overlays.deletionFenceVersion(overlayId)).thenReturn(1L);
    when(service.overlays.deleteWithFence(overlayId, 7L, 1L)).thenReturn(true);

    service
        .deleteCatalogOverlay(
            DeleteCatalogOverlayRequest.newBuilder().setOverlayId(overlayId).build())
        .await()
        .indefinitely();

    verify(service.overlays).beginDeletion(overlayId, 7L);
    verify(service.reconciler).retireMaterializedResources(current);
    verify(service.overlays).deleteWithFence(overlayId, 7L, 1L);
    verify(service.catalogs, never()).delete(any());
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
