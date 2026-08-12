/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

package ai.floedb.floecat.service.repo.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.integration.rpc.CatalogIntegration;
import ai.floedb.floecat.integration.rpc.CatalogOverlay;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.rpc.IdempotencyRecord;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.google.protobuf.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class CatalogOverlayRepositoryTest {

  @Test
  void overlayAndItsOwnedCatalogCommitInOneTransaction() {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var catalogs = new CatalogRepository(pointers, blobs);
    var overlays = new CatalogOverlayRepository(pointers, blobs);
    ResourceId catalogId = id("catalog", ResourceKind.RK_CATALOG);
    CatalogOverlay overlay = overlay("overlay").toBuilder().setCatalogId(catalogId).build();
    Catalog owned =
        Catalog.newBuilder()
            .setResourceId(catalogId)
            .setDisplayName(overlay.getDisplayName())
            .setOverlayId(overlay.getResourceId())
            .build();

    assertTrue(
        overlays
            .createAttachedWithMetaAndCompanions(
                overlay, Map.of(), Set.of(), 0L, row -> catalogs.prepareCreateOps(owned))
            .isPresent());

    assertEquals("sales", catalogs.getByName("account", "sales").orElseThrow().getDisplayName());
    assertEquals(overlay.getResourceId(), catalogs.getById(catalogId).orElseThrow().getOverlayId());
  }

  @Test
  void overlayCreateCommitsNothingWhenItsCatalogNameIsTaken() {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var catalogs = new CatalogRepository(pointers, blobs);
    var overlays = new CatalogOverlayRepository(pointers, blobs);
    catalogs.create(
        Catalog.newBuilder()
            .setResourceId(id("existing", ResourceKind.RK_CATALOG))
            .setDisplayName("sales")
            .build());

    ResourceId catalogId = id("catalog", ResourceKind.RK_CATALOG);
    CatalogOverlay overlay = overlay("overlay").toBuilder().setCatalogId(catalogId).build();
    Catalog owned =
        Catalog.newBuilder()
            .setResourceId(catalogId)
            .setDisplayName("sales")
            .setOverlayId(overlay.getResourceId())
            .build();

    // The companion catalog loses its name reservation, so the whole batch commits nothing and the
    // caller is told this is a name conflict rather than a transient one.
    assertThrows(
        BaseResourceRepository.NameConflictException.class,
        () ->
            overlays.createAttachedWithMetaAndCompanions(
                overlay, Map.of(), Set.of(), 0L, row -> catalogs.prepareCreateOps(owned)));
    assertTrue(overlays.getById(overlay.getResourceId()).isEmpty());
    assertTrue(catalogs.getById(catalogId).isEmpty());
  }

  @Test
  void deletingAnOverlayDeletesTheCatalogItOwns() {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var catalogs = new CatalogRepository(pointers, blobs);
    var overlays = new CatalogOverlayRepository(pointers, blobs);
    ResourceId catalogId = id("catalog", ResourceKind.RK_CATALOG);
    CatalogOverlay overlay = overlay("overlay").toBuilder().setCatalogId(catalogId).build();
    Catalog owned =
        Catalog.newBuilder()
            .setResourceId(catalogId)
            .setDisplayName(overlay.getDisplayName())
            .setOverlayId(overlay.getResourceId())
            .build();
    overlays.createAttachedWithMetaAndCompanions(
        overlay, Map.of(), Set.of(), 0L, row -> catalogs.prepareCreateOps(owned));

    long version = overlays.metaFor(overlay.getResourceId()).getPointerVersion();
    assertTrue(
        overlays.deleteWithOwnedCatalog(
            overlay.getResourceId(), version, catalogs.prepareDeleteOps(catalogId)));

    assertTrue(overlays.getById(overlay.getResourceId()).isEmpty());
    assertTrue(catalogs.getById(catalogId).isEmpty());
    assertTrue(catalogs.getByName("account", "sales").isEmpty());
  }

  @Test
  void indexesOverlayAndDeletesAllPointersAtomically() {
    var repo = new CatalogOverlayRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    var overlay = overlay("overlay");
    repo.create(overlay);

    assertEquals(overlay, repo.getById(overlay.getResourceId()).orElseThrow());
    assertEquals(overlay, repo.getByName("account", "sales").orElseThrow());
    assertEquals(1, repo.countByIntegration("account", "integration"));

    assertTrue(repo.deleteWithPrecondition(overlay.getResourceId(), 1L));
    assertFalse(repo.getById(overlay.getResourceId()).isPresent());
    assertFalse(repo.getByName("account", "sales").isPresent());
    assertEquals(0, repo.countByIntegration("account", "integration"));
  }

  @Test
  void integrationMayHaveMultipleOverlays() {
    var repo = new CatalogOverlayRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    repo.create(overlay("overlay"));

    repo.create(overlay("other").toBuilder().setDisplayName("other").build());
    assertEquals(2, repo.countByIntegration("account", "integration"));
  }

  @Test
  void createChecksParentsAndAdvancesIntegrationMarkerAtomically() {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var integrations = new CatalogIntegrationRepository(pointers, blobs);
    var overlays = new CatalogOverlayRepository(pointers, blobs);
    CatalogOverlay overlay = overlay("overlay");
    ResourceId integrationId = id("integration", ResourceKind.RK_CATALOG_INTEGRATION);
    ResourceId catalogId = id("catalog", ResourceKind.RK_CATALOG);
    integrations.create(
        CatalogIntegration.newBuilder()
            .setResourceId(integrationId)
            .setDisplayName("integration")
            .build());
    long integrationVersion = integrations.metaFor(integrationId).getPointerVersion();

    assertTrue(
        overlays.createAttached(
            overlay,
            Map.of(
                Keys.catalogIntegrationPointerById("account", "integration"), integrationVersion),
            Set.of(),
            0L));
    assertEquals(
        1L,
        pointers
            .get(Keys.catalogIntegrationOverlaysMarker("account", "integration"))
            .orElseThrow()
            .getVersion());

    assertFalse(
        integrations.deleteWithPreconditionAndNoOverlayMarker(integrationId, integrationVersion));

    long overlayVersion = overlays.metaFor(overlay.getResourceId()).getPointerVersion();
    assertTrue(overlays.deleteWithPrecondition(overlay.getResourceId(), overlayVersion));
    assertTrue(integrations.beginCascadeDeletion(integrationId, integrationVersion));
    assertTrue(
        integrations.deleteWithPreconditionForCascadeDeletion(
            integrationId, integrationVersion, 1L, 1L));
    assertFalse(integrations.getById(integrationId).isPresent());
    assertFalse(
        pointers.get(Keys.catalogIntegrationOverlaysMarker("account", "integration")).isPresent());
  }

  @Test
  void identityReplacementMovesIntegrationIndexAndAdvancesBothMarkersAtomically() {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var integrations = new CatalogIntegrationRepository(pointers, blobs);
    var overlays = new CatalogOverlayRepository(pointers, blobs);
    ResourceId oldId = id("old", ResourceKind.RK_CATALOG_INTEGRATION);
    ResourceId nextId = id("next", ResourceKind.RK_CATALOG_INTEGRATION);
    integrations.create(
        CatalogIntegration.newBuilder().setResourceId(oldId).setDisplayName("old").build());
    integrations.create(
        CatalogIntegration.newBuilder().setResourceId(nextId).setDisplayName("next").build());
    CatalogOverlay original = overlay("overlay").toBuilder().setIntegrationId(oldId).build();
    long oldVersion = integrations.metaFor(oldId).getPointerVersion();
    long nextVersion = integrations.metaFor(nextId).getPointerVersion();
    assertTrue(
        overlays.createAttached(
            original,
            Map.of(Keys.catalogIntegrationPointerById("account", "old"), oldVersion),
            Set.of(),
            0L));

    CatalogOverlay replacement =
        original.toBuilder()
            .setResourceId(id("replacement", ResourceKind.RK_CATALOG_OVERLAY))
            .setIntegrationId(nextId)
            .build();
    var replaced =
        overlays
            .replaceIdentityAttachedWithMeta(
                original,
                1L,
                replacement,
                Map.of(
                    Keys.catalogIntegrationPointerById("account", "old"), oldVersion,
                    Keys.catalogIntegrationPointerById("account", "next"), nextVersion),
                Set.of(
                    Keys.catalogIntegrationDeletionMarker("account", "old"),
                    Keys.catalogIntegrationDeletionMarker("account", "next")),
                Map.of(
                    Keys.catalogIntegrationOverlaysMarker("account", "old"), 1L,
                    Keys.catalogIntegrationOverlaysMarker("account", "next"), 0L),
                List.of())
            .orElseThrow();

    assertEquals(1L, replaced.meta().getPointerVersion());
    assertFalse(overlays.getById(original.getResourceId()).isPresent());
    assertEquals(replacement, overlays.getById(replacement.getResourceId()).orElseThrow());
    assertEquals(0, overlays.countByIntegration("account", "old"));
    assertEquals(1, overlays.countByIntegration("account", "next"));
    assertEquals(
        2L,
        pointers
            .get(Keys.catalogIntegrationOverlaysMarker("account", "old"))
            .orElseThrow()
            .getVersion());
    assertEquals(
        1L,
        pointers
            .get(Keys.catalogIntegrationOverlaysMarker("account", "next"))
            .orElseThrow()
            .getVersion());
  }

  @Test
  void cascadeDeletionFenceAtomicallyRejectsANewOverlayAttachment() {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var integrations = new CatalogIntegrationRepository(pointers, blobs);
    var overlays = new CatalogOverlayRepository(pointers, blobs);
    ResourceId integrationId = id("integration", ResourceKind.RK_CATALOG_INTEGRATION);
    integrations.create(
        CatalogIntegration.newBuilder()
            .setResourceId(integrationId)
            .setDisplayName("integration")
            .build());
    long integrationVersion = integrations.metaFor(integrationId).getPointerVersion();
    assertTrue(integrations.beginCascadeDeletion(integrationId, integrationVersion));

    assertFalse(
        overlays.createAttached(
            overlay("overlay"),
            Map.of(
                Keys.catalogIntegrationPointerById("account", "integration"), integrationVersion),
            Set.of(Keys.catalogIntegrationDeletionMarker("account", "integration")),
            0L));
    assertFalse(overlays.getById(id("overlay", ResourceKind.RK_CATALOG_OVERLAY)).isPresent());
  }

  @Test
  void idempotencyReceiptAndOverlayPointerSetBothCommitWhenAcknowledgementIsLost()
      throws Exception {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var integrations = new CatalogIntegrationRepository(pointers, blobs);
    ResourceId integrationId = id("integration", ResourceKind.RK_CATALOG_INTEGRATION);
    integrations.create(
        CatalogIntegration.newBuilder()
            .setResourceId(integrationId)
            .setDisplayName("integration")
            .build());
    long integrationVersion = integrations.metaFor(integrationId).getPointerVersion();
    var commitThenFail = new RepoTestPointerStores.CommitThenFailBatchPointerStore(pointers);
    var overlays = new CatalogOverlayRepository(commitThenFail, blobs);
    var idempotency = new IdempotencyRepositoryImpl(commitThenFail, blobs);
    CatalogOverlay overlay = overlay("overlay");
    String operation = "CreateCatalogOverlay";
    String key = Keys.idempotencyKey("account", operation, "request-1");
    Timestamp createdAt = timestamp(1L);
    Timestamp expiresAt = timestamp(2L);
    var committedMeta = new AtomicReference<ai.floedb.floecat.common.rpc.MutationMeta>();
    assertTrue(
        idempotency.createPending(
            "account",
            key,
            operation,
            "request-hash",
            overlay.getResourceId(),
            createdAt,
            expiresAt));

    assertThrows(
        RepoTestPointerStores.CommitThenFailBatchPointerStore.InjectedAcknowledgementFailure.class,
        () ->
            overlays.createAttachedWithMetaAndCompanions(
                overlay,
                Map.of(
                    Keys.catalogIntegrationPointerById("account", "integration"),
                    integrationVersion),
                Set.of(Keys.catalogIntegrationDeletionMarker("account", "integration")),
                0L,
                row -> {
                  committedMeta.set(row.meta());
                  return List.of(
                      (PointerStore.CasOp)
                          idempotency.prepareSuccess(
                              "account",
                              key,
                              operation,
                              "request-hash",
                              row.value().getResourceId(),
                              row.meta(),
                              row.value().toByteArray(),
                              createdAt,
                              expiresAt));
                }));

    var receipt = idempotency.get(key).orElseThrow();
    assertEquals(IdempotencyRecord.Status.SUCCEEDED, receipt.getStatus());
    assertEquals(committedMeta.get(), receipt.getMeta());
    assertEquals(overlay, CatalogOverlay.parseFrom(receipt.getPayload()));
    assertEquals(overlay, overlays.getById(overlay.getResourceId()).orElseThrow());
    assertEquals(overlay, overlays.getByName("account", "sales").orElseThrow());
    assertEquals(1, overlays.countByIntegration("account", "integration"));
    assertEquals(
        1L,
        pointers
            .get(Keys.catalogIntegrationOverlaysMarker("account", "integration"))
            .orElseThrow()
            .getVersion());
  }

  @Test
  void idempotencyReceiptAndOverlayPointerSetBothRemainUncommittedWhenBatchFails() {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var integrations = new CatalogIntegrationRepository(pointers, blobs);
    ResourceId integrationId = id("integration", ResourceKind.RK_CATALOG_INTEGRATION);
    integrations.create(
        CatalogIntegration.newBuilder()
            .setResourceId(integrationId)
            .setDisplayName("integration")
            .build());
    long integrationVersion = integrations.metaFor(integrationId).getPointerVersion();
    var failing = new RepoTestPointerStores.FailingBatchPointerStore(pointers);
    var overlays = new CatalogOverlayRepository(failing, blobs);
    var idempotency = new IdempotencyRepositoryImpl(failing, blobs);
    CatalogOverlay overlay = overlay("overlay");
    String operation = "CreateCatalogOverlay";
    String key = Keys.idempotencyKey("account", operation, "request-1");
    Timestamp createdAt = timestamp(1L);
    Timestamp expiresAt = timestamp(2L);
    assertTrue(
        idempotency.createPending(
            "account",
            key,
            operation,
            "request-hash",
            overlay.getResourceId(),
            createdAt,
            expiresAt));

    assertThrows(
        RepoTestPointerStores.FailingBatchPointerStore.InjectedBatchFailure.class,
        () ->
            overlays.createAttachedWithMetaAndCompanions(
                overlay,
                Map.of(
                    Keys.catalogIntegrationPointerById("account", "integration"),
                    integrationVersion),
                Set.of(Keys.catalogIntegrationDeletionMarker("account", "integration")),
                0L,
                row ->
                    List.of(
                        (PointerStore.CasOp)
                            idempotency.prepareSuccess(
                                "account",
                                key,
                                operation,
                                "request-hash",
                                row.value().getResourceId(),
                                row.meta(),
                                row.value().toByteArray(),
                                createdAt,
                                expiresAt))));

    assertTrue(overlays.getById(overlay.getResourceId()).isEmpty());
    assertTrue(overlays.getByName("account", "sales").isEmpty());
    assertEquals(0, overlays.countByIntegration("account", "integration"));
    assertTrue(
        pointers.get(Keys.catalogIntegrationOverlaysMarker("account", "integration")).isEmpty());
    assertEquals(IdempotencyRecord.Status.PENDING, idempotency.get(key).orElseThrow().getStatus());
  }

  private static Timestamp timestamp(long seconds) {
    return Timestamp.newBuilder().setSeconds(seconds).build();
  }

  private static CatalogOverlay overlay(String overlayId) {
    return CatalogOverlay.newBuilder()
        .setResourceId(id(overlayId, ResourceKind.RK_CATALOG_OVERLAY))
        .setDisplayName("sales")
        .setIntegrationId(id("integration", ResourceKind.RK_CATALOG_INTEGRATION))
        .build();
  }

  private static ResourceId id(String id, ResourceKind kind) {
    return ResourceId.newBuilder().setAccountId("account").setId(id).setKind(kind).build();
  }
}
