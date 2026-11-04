/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package ai.floedb.floecat.service.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.integration.rpc.CatalogOverlay;
import ai.floedb.floecat.service.repo.impl.CatalogIntegrationRepository;
import ai.floedb.floecat.service.repo.impl.CatalogOverlayRepository;
import ai.floedb.floecat.service.repo.util.DurableDeletionFence;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository.ResourceWithMeta;
import ai.floedb.floecat.service.repo.util.MarkerStore;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CatalogIntegrationCascadeDeletionTest {
  private CatalogIntegrationCascadeDeletion deletion;

  @BeforeEach
  void setUp() {
    deletion = new CatalogIntegrationCascadeDeletion();
    deletion.integrations = mock(CatalogIntegrationRepository.class);
    deletion.overlays = mock(CatalogOverlayRepository.class);
    deletion.overlayReconciler = mock(CatalogOverlayReconciler.class);
    deletion.overlayLifecycle = mock(CatalogOverlayLifecycle.class);
    deletion.markerStore = mock(MarkerStore.class);
  }

  @Test
  void fencesRetiresDependentsAndCompletesWithAcquiredFenceVersion() {
    ResourceId integrationId = id("integration", ResourceKind.RK_CATALOG_INTEGRATION);
    MutationMeta integrationMeta = MutationMeta.newBuilder().setPointerVersion(7L).build();
    ResourceId overlayId = id("overlay", ResourceKind.RK_CATALOG_OVERLAY);
    CatalogOverlay overlay =
        CatalogOverlay.newBuilder()
            .setResourceId(overlayId)
            .setCatalogId(id("catalog", ResourceKind.RK_CATALOG))
            .setIntegrationId(integrationId)
            .build();
    MutationMeta overlayMeta = MutationMeta.newBuilder().setPointerVersion(3L).build();
    when(deletion.integrations.acquireCascadeDeletionFence(integrationId, 7L))
        .thenReturn(Optional.of(new DurableDeletionFence.Fence(11L, true)));
    when(deletion.overlays.listByIntegrationWithMetaConsistent(
            eq("acct"), eq("integration"), eq(100), eq(""), any()))
        .thenReturn(
            List.of(new ResourceWithMeta<>(overlay, overlayMeta)),
            List.of(new ResourceWithMeta<>(overlay, overlayMeta)),
            List.of());
    when(deletion.overlays.acquireDeletionFence(overlayId, 3L))
        .thenReturn(Optional.of(new DurableDeletionFence.Fence(5L, true)));
    when(deletion.overlays.deleteWithFence(overlayId, 3L, 5L)).thenReturn(true);
    when(deletion.overlays.countByIntegration("acct", "integration")).thenReturn(0);
    when(deletion.markerStore.catalogIntegrationOverlaysMarkerVersion(integrationId))
        .thenReturn(4L);
    when(deletion.integrations.deleteWithPreconditionForCascadeDeletion(integrationId, 7L, 4L, 11L))
        .thenReturn(true);

    assertEquals(
        integrationMeta, deletion.delete(integrationId, integrationMeta, "correlation", () -> {}));

    verify(deletion.overlayReconciler).retireMaterializedResources(overlay);
    verify(deletion.overlays).deleteWithFence(overlayId, 3L, 5L);
    verify(deletion.integrations)
        .deleteWithPreconditionForCascadeDeletion(integrationId, 7L, 4L, 11L);
  }

  @Test
  void recursivelyDeletesCatalogOwnedByDependentOverlay() {
    ResourceId integrationId = id("integration", ResourceKind.RK_CATALOG_INTEGRATION);
    MutationMeta integrationMeta = MutationMeta.newBuilder().setPointerVersion(7L).build();
    ResourceId overlayId = id("overlay", ResourceKind.RK_CATALOG_OVERLAY);
    CatalogOverlay overlay =
        CatalogOverlay.newBuilder()
            .setResourceId(overlayId)
            .setCatalogId(id("catalog", ResourceKind.RK_CATALOG))
            .setIntegrationId(integrationId)
            .setOwnsCatalog(true)
            .build();
    MutationMeta overlayMeta = MutationMeta.newBuilder().setPointerVersion(3L).build();
    when(deletion.integrations.acquireCascadeDeletionFence(integrationId, 7L))
        .thenReturn(Optional.of(new DurableDeletionFence.Fence(11L, true)));
    when(deletion.overlays.listByIntegrationWithMetaConsistent(
            eq("acct"), eq("integration"), eq(100), eq(""), any()))
        .thenReturn(
            List.of(new ResourceWithMeta<>(overlay, overlayMeta)),
            List.of(new ResourceWithMeta<>(overlay, overlayMeta)),
            List.of());
    when(deletion.overlays.acquireDeletionFence(overlayId, 3L))
        .thenReturn(Optional.of(new DurableDeletionFence.Fence(5L, true)));
    when(deletion.overlayLifecycle.deleteFenced(
            overlay, 3L, new DurableDeletionFence.Fence(5L, true), "correlation", integrationId))
        .thenReturn(true);
    when(deletion.overlays.countByIntegration("acct", "integration")).thenReturn(0);
    when(deletion.markerStore.catalogIntegrationOverlaysMarkerVersion(integrationId))
        .thenReturn(4L);
    when(deletion.integrations.deleteWithPreconditionForCascadeDeletion(integrationId, 7L, 4L, 11L))
        .thenReturn(true);
    Runnable requireOwnedCatalogDelete = mock(Runnable.class);

    assertEquals(
        integrationMeta,
        deletion.delete(integrationId, integrationMeta, "correlation", requireOwnedCatalogDelete));

    verify(requireOwnedCatalogDelete, org.mockito.Mockito.times(2)).run();
    verify(deletion.overlayLifecycle, org.mockito.Mockito.times(2))
        .preflightOwnedCatalogRemoval(overlay, "correlation", integrationId);
    verify(deletion.overlayLifecycle)
        .deleteFenced(
            overlay, 3L, new DurableDeletionFence.Fence(5L, true), "correlation", integrationId);
    verify(deletion.overlayReconciler, never()).retireMaterializedResources(any());
  }

  @Test
  void releasesNewIntegrationFenceWhenSharingAppearsAfterPreflight() {
    ResourceId integrationId = id("integration", ResourceKind.RK_CATALOG_INTEGRATION);
    MutationMeta integrationMeta = MutationMeta.newBuilder().setPointerVersion(7L).build();
    CatalogOverlay overlay =
        CatalogOverlay.newBuilder()
            .setResourceId(id("overlay", ResourceKind.RK_CATALOG_OVERLAY))
            .setCatalogId(id("catalog", ResourceKind.RK_CATALOG))
            .setIntegrationId(integrationId)
            .setOwnsCatalog(true)
            .build();
    MutationMeta overlayMeta = MutationMeta.newBuilder().setPointerVersion(3L).build();
    when(deletion.overlays.listByIntegrationWithMetaConsistent(
            eq("acct"), eq("integration"), eq(100), eq(""), any()))
        .thenReturn(
            List.of(new ResourceWithMeta<>(overlay, overlayMeta)),
            List.of(new ResourceWithMeta<>(overlay, overlayMeta)));
    when(deletion.integrations.acquireCascadeDeletionFence(integrationId, 7L))
        .thenReturn(Optional.of(new DurableDeletionFence.Fence(11L, true)));
    doNothing()
        .doThrow(
            io.grpc.Status.ABORTED.withDescription("catalog became shared").asRuntimeException())
        .when(deletion.overlayLifecycle)
        .preflightOwnedCatalogRemoval(overlay, "correlation", integrationId);

    org.junit.jupiter.api.Assertions.assertThrows(
        io.grpc.StatusRuntimeException.class,
        () -> deletion.delete(integrationId, integrationMeta, "correlation", () -> {}));

    verify(deletion.integrations).cancelCascadeDeletion(integrationId, 7L, 11L);
    verify(deletion.overlays, never()).acquireDeletionFence(any(), anyLong());
  }

  @Test
  void releasesNewIntegrationFenceWhenSharingAppearsDuringOwnedCatalogDelete() {
    ResourceId integrationId = id("integration", ResourceKind.RK_CATALOG_INTEGRATION);
    MutationMeta integrationMeta = MutationMeta.newBuilder().setPointerVersion(7L).build();
    ResourceId overlayId = id("overlay", ResourceKind.RK_CATALOG_OVERLAY);
    CatalogOverlay overlay =
        CatalogOverlay.newBuilder()
            .setResourceId(overlayId)
            .setCatalogId(id("catalog", ResourceKind.RK_CATALOG))
            .setIntegrationId(integrationId)
            .setOwnsCatalog(true)
            .build();
    MutationMeta overlayMeta = MutationMeta.newBuilder().setPointerVersion(3L).build();
    when(deletion.overlays.listByIntegrationWithMetaConsistent(
            eq("acct"), eq("integration"), eq(100), eq(""), any()))
        .thenReturn(
            List.of(new ResourceWithMeta<>(overlay, overlayMeta)),
            List.of(new ResourceWithMeta<>(overlay, overlayMeta)));
    when(deletion.integrations.acquireCascadeDeletionFence(integrationId, 7L))
        .thenReturn(Optional.of(new DurableDeletionFence.Fence(11L, true)));
    when(deletion.overlays.acquireDeletionFence(overlayId, 3L))
        .thenReturn(Optional.of(new DurableDeletionFence.Fence(5L, true)));
    doThrow(io.grpc.Status.ABORTED.withDescription("catalog became shared").asRuntimeException())
        .when(deletion.overlayLifecycle)
        .deleteFenced(
            overlay, 3L, new DurableDeletionFence.Fence(5L, true), "correlation", integrationId);

    org.junit.jupiter.api.Assertions.assertThrows(
        io.grpc.StatusRuntimeException.class,
        () -> deletion.delete(integrationId, integrationMeta, "correlation", () -> {}));

    verify(deletion.integrations).cancelCascadeDeletion(integrationId, 7L, 11L);
  }

  private static ResourceId id(String value, ResourceKind kind) {
    return ResourceId.newBuilder().setAccountId("acct").setId(value).setKind(kind).build();
  }
}
