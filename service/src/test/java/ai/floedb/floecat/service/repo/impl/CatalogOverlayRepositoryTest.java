/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

package ai.floedb.floecat.service.repo.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.integration.rpc.CatalogOverlay;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class CatalogOverlayRepositoryTest {

  @Test
  void createPublishesBothDependenciesAndAdvancesBothMarkers() {
    var pointers = new InMemoryPointerStore();
    var overlays = new CatalogOverlayRepository(pointers, new InMemoryBlobStore());
    var overlay = overlay("overlay", "integration", "catalog");
    var markers =
        Map.of(
            Keys.catalogIntegrationOverlaysMarker("account", "integration"), 0L,
            Keys.catalogOverlaysMarker("account", "catalog"), 0L);

    assertTrue(overlays.createAttachedWithMeta(overlay, Map.of(), Set.of(), markers).isPresent());

    assertEquals(1, overlays.countByIntegration("account", "integration"));
    assertEquals(1, overlays.countByCatalog("account", "catalog"));
    assertEquals(
        1L,
        pointers
            .get(Keys.catalogIntegrationOverlaysMarker("account", "integration"))
            .orElseThrow()
            .getVersion());
    assertEquals(
        1L,
        pointers.get(Keys.catalogOverlaysMarker("account", "catalog")).orElseThrow().getVersion());
  }

  @Test
  void multipleOverlaysMayReferenceTheSameCatalog() {
    var pointers = new InMemoryPointerStore();
    var overlays = new CatalogOverlayRepository(pointers, new InMemoryBlobStore());
    var first = overlay("first", "crm", "analytics");
    var second =
        overlay("second", "finance", "analytics").toBuilder().setDisplayName("finance").build();

    assertTrue(
        overlays
            .createAttachedWithMeta(
                first,
                Map.of(),
                Set.of(),
                Map.of(
                    Keys.catalogIntegrationOverlaysMarker("account", "crm"), 0L,
                    Keys.catalogOverlaysMarker("account", "analytics"), 0L))
            .isPresent());
    assertTrue(
        overlays
            .createAttachedWithMeta(
                second,
                Map.of(),
                Set.of(),
                Map.of(
                    Keys.catalogIntegrationOverlaysMarker("account", "finance"), 0L,
                    Keys.catalogOverlaysMarker("account", "analytics"), 1L))
            .isPresent());

    assertEquals(2, overlays.countByCatalog("account", "analytics"));
    assertEquals(1, overlays.countByIntegration("account", "crm"));
    assertEquals(1, overlays.countByIntegration("account", "finance"));
  }

  @Test
  void deletionFenceBlocksUpdatesAndDeleteLeavesTargetCatalog() {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var overlays = new CatalogOverlayRepository(pointers, blobs);
    var catalogs = new CatalogRepository(pointers, blobs);
    var catalogId = id("catalog", ResourceKind.RK_CATALOG);
    catalogs.create(
        Catalog.newBuilder().setResourceId(catalogId).setDisplayName("analytics").build());
    var overlay = overlay("overlay", "integration", "catalog");
    overlays.create(overlay);

    assertTrue(overlays.beginDeletion(overlay.getResourceId(), 1L));
    assertTrue(
        overlays
            .updateWithMetaUnlessIntegrationDeleting(
                overlay.toBuilder().setDisplayName("renamed").build(), 1L)
            .isEmpty());
    long fenceVersion = overlays.deletionFenceVersion(overlay.getResourceId());
    assertTrue(overlays.deleteWithFence(overlay.getResourceId(), 1L, fenceVersion));

    assertTrue(overlays.getById(overlay.getResourceId()).isEmpty());
    assertEquals(0, overlays.countByIntegration("account", "integration"));
    assertEquals(0, overlays.countByCatalog("account", "catalog"));
    assertTrue(catalogs.getById(catalogId).isPresent());
    assertTrue(pointers.get(Keys.catalogOverlayDeletionMarker("account", "overlay")).isEmpty());
  }

  @Test
  void replacementMayRebindBothParents() {
    var pointers = new InMemoryPointerStore();
    var overlays = new CatalogOverlayRepository(pointers, new InMemoryBlobStore());
    var current = overlay("old-overlay", "old-integration", "old-catalog");
    overlays.createAttachedWithMeta(
        current,
        Map.of(),
        Set.of(),
        Map.of(
            Keys.catalogIntegrationOverlaysMarker("account", "old-integration"), 0L,
            Keys.catalogOverlaysMarker("account", "old-catalog"), 0L));
    var replacement = overlay("new-overlay", "new-integration", "new-catalog");

    assertTrue(
        overlays
            .replaceIdentityAttachedWithMeta(
                current,
                1L,
                replacement,
                Map.of(),
                Set.of(),
                Map.of(
                    Keys.catalogIntegrationOverlaysMarker("account", "old-integration"), 1L,
                    Keys.catalogIntegrationOverlaysMarker("account", "new-integration"), 0L,
                    Keys.catalogOverlaysMarker("account", "old-catalog"), 1L,
                    Keys.catalogOverlaysMarker("account", "new-catalog"), 0L))
            .isPresent());

    assertTrue(overlays.getById(current.getResourceId()).isEmpty());
    assertEquals(replacement, overlays.getById(replacement.getResourceId()).orElseThrow());
    assertEquals(0, overlays.countByIntegration("account", "old-integration"));
    assertEquals(1, overlays.countByIntegration("account", "new-integration"));
    assertEquals(0, overlays.countByCatalog("account", "old-catalog"));
    assertEquals(1, overlays.countByCatalog("account", "new-catalog"));
  }

  @Test
  void createFailsWhenAParentVersionDoesNotMatch() {
    var overlays =
        new CatalogOverlayRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    var value = overlay("overlay", "integration", "catalog");

    assertTrue(
        overlays
            .createAttachedWithMeta(
                value,
                Map.of(Keys.catalogPointerById("account", "catalog"), 1L),
                Set.of(),
                Map.of())
            .isEmpty());
    assertFalse(overlays.getById(value.getResourceId()).isPresent());
  }

  private static CatalogOverlay overlay(String overlayId, String integrationId, String catalogId) {
    return CatalogOverlay.newBuilder()
        .setResourceId(id(overlayId, ResourceKind.RK_CATALOG_OVERLAY))
        .setDisplayName("sales")
        .setIntegrationId(id(integrationId, ResourceKind.RK_CATALOG_INTEGRATION))
        .setCatalogId(id(catalogId, ResourceKind.RK_CATALOG))
        .build();
  }

  private static ResourceId id(String value, ResourceKind kind) {
    return ResourceId.newBuilder().setAccountId("account").setId(value).setKind(kind).build();
  }
}
