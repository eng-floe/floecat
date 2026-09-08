/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

package ai.floedb.floecat.service.repo.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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
    assertTrue(overlays.beginDeletion(current.getResourceId(), 1L));
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
    assertEquals(0L, overlays.deletionFenceVersion(current.getResourceId()));
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

  /**
   * A {@code get} that is behind a committed write for one key, while {@code getConsistent} is not.
   *
   * <p>That is the pointer cache's shape, not the store's: {@code DynamoDbKvStore} reads single
   * keys with {@code consistentRead(true)}, so at the store the two are the same read. Every family
   * reaching {@code getWithMeta} is uncached today, so this models the decorator rather than
   * anything those keys meet in production -- it pins the behaviour for the point at which one of
   * them becomes cached, which is when the bug below would go live.
   */
  private static final class StaleReadFor extends InMemoryPointerStore {
    private final String key;
    private ai.floedb.floecat.common.rpc.Pointer frozen;

    StaleReadFor(String key) {
      this.key = key;
    }

    void freeze() {
      frozen = super.get(key).orElse(null);
    }

    @Override
    public java.util.Optional<ai.floedb.floecat.common.rpc.Pointer> get(String k) {
      return key.equals(k) && frozen != null ? java.util.Optional.of(frozen) : super.get(k);
    }

    // The SPI default delegates getConsistent to get, which would freeze this one too. A
    // consistent read is authoritative by definition, so it must see the committed value.
    @Override
    public java.util.Optional<ai.floedb.floecat.common.rpc.Pointer> getConsistent(String k) {
      return super.get(k);
    }

    // A prefix listing is behind in the same way -- which is why listPointersByPrefixConsistent
    // exists as a separate method -- so a page can name a blob a commit has already superseded.
    @Override
    public java.util.List<ai.floedb.floecat.common.rpc.Pointer> listPointersByPrefix(
        String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
      var page = super.listPointersByPrefix(prefix, limit, pageToken, nextTokenOut);
      if (frozen == null) {
        return page;
      }
      return page.stream().map(p -> key.equals(p.getKey()) ? frozen : p).toList();
    }
  }

  @Test
  void aByNameLookupOverAStaleReadReturnsTheNewBodyWithMetaThatMatchesIt() {
    // The secondary read is behind a commit that moved the blob, and CAS GC swept the old one.
    // Re-resolving is only half the job: the body/meta coherence check compares against the uri
    // the read resolved AT, so leaving the vanished uri in place either drops the row, or -- when
    // the canonical read is behind too -- pairs the new body with meta built from the old pointer,
    // whose etag is HEADed off a blob that no longer exists.
    String byName = Keys.catalogOverlayPointerByName("account", "sales");
    var pointers = new StaleReadFor(byName);
    var blobs = new InMemoryBlobStore();
    var overlays = new CatalogOverlayRepository(pointers, blobs);
    var overlay = overlay("overlay", "integration", "catalog");
    overlays.create(overlay);

    String staleUri = pointers.get(byName).orElseThrow().getBlobUri();
    pointers.freeze(); // this key now reads behind

    assertTrue(
        overlays.update(
            overlay.toBuilder().setCatalogId(id("catalog-2", ResourceKind.RK_CATALOG)).build(),
            1L));
    blobs.delete(staleUri);

    var found = overlays.getByNameWithMeta("account", "sales");
    assertTrue(found.isPresent(), "a superseded blob must not read as a missing overlay");
    assertEquals("catalog-2", found.orElseThrow().value().getCatalogId().getId());
    assertNotEquals(
        staleUri,
        found.orElseThrow().meta().getBlobUri(),
        "meta must describe the blob the body came from, not the one that vanished");
  }

  @Test
  void aListPageOverAStaleReadKeepsTheRowRatherThanShorteningThePage() {
    // The same shape through the list path. Both go through one helper now, but they did not when
    // this bug was written into each of them separately, and only one of the two was ever covered.
    String byName = Keys.catalogOverlayPointerByName("account", "sales");
    var pointers = new StaleReadFor(byName);
    var blobs = new InMemoryBlobStore();
    var overlays = new CatalogOverlayRepository(pointers, blobs);
    var overlay = overlay("overlay", "integration", "catalog");
    overlays.create(overlay);

    String staleUri = pointers.get(byName).orElseThrow().getBlobUri();
    pointers.freeze();

    assertTrue(
        overlays.update(
            overlay.toBuilder().setCatalogId(id("catalog-2", ResourceKind.RK_CATALOG)).build(),
            1L));
    blobs.delete(staleUri);

    var page = overlays.listWithMeta("account", 10, "", new StringBuilder());
    assertEquals(1, page.size(), "a superseded blob must not silently shorten the page");
    assertEquals("catalog-2", page.get(0).value().getCatalogId().getId());
    assertNotEquals(staleUri, page.get(0).meta().getBlobUri());
  }

  @Test
  void aDeleteResolvesItsBodyFreshlySoItDropsTheCurrentNamePointer() {
    // delete() takes its CAS version from a consistent read and its BODY from readForMutation.
    // If the body is one rename behind, the secondary CasDeletes name the old pointer and the
    // current one is orphaned: a name nothing can resolve and nothing will ever clean up.
    var overlay = overlay("overlay", "integration", "catalog");
    String canonicalById =
        Keys.catalogOverlayPointerById("account", overlay.getResourceId().getId());
    var pointers = new StaleReadFor(canonicalById);
    var blobs = new InMemoryBlobStore();
    var overlays = new CatalogOverlayRepository(pointers, blobs);
    overlays.create(overlay);

    pointers.freeze(); // the canonical pointer now reads behind

    // Renamed after the freeze. update() reads consistently, so the rename itself lands.
    assertTrue(overlays.update(overlay.toBuilder().setDisplayName("renamed").build(), 1L));
    assertTrue(
        pointers.get(Keys.catalogOverlayPointerByName("account", "renamed")).isPresent(),
        "precondition: the rename published the new name pointer");

    assertTrue(overlays.delete(overlay.getResourceId()));

    assertFalse(
        pointers.get(Keys.catalogOverlayPointerByName("account", "renamed")).isPresent(),
        "the delete must drop the name the resource actually has, not the one a stale body named");
  }
}
