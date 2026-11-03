/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package ai.floedb.floecat.service.catalog.impl;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.impl.CatalogOverlayRepository;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.MarkerStore;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import java.util.List;
import org.junit.jupiter.api.Test;

class CascadeDeletionTest {

  @Test
  void catalogDriverOwnsFenceSubtreeCleanupAndFinalTransaction() {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var catalogs = new CatalogRepository(pointers, blobs);
    var namespaces = new NamespaceRepository(pointers, blobs);
    ResourceId catalogId = id("acct", "catalog", ResourceKind.RK_CATALOG);
    catalogs.create(
        Catalog.newBuilder().setResourceId(catalogId).setDisplayName("catalog").build());
    namespaces.create(
        namespace(id("acct", "root", ResourceKind.RK_NAMESPACE), catalogId, "root", List.of()));
    namespaces.create(
        namespace(
            id("acct", "child", ResourceKind.RK_NAMESPACE), catalogId, "child", List.of("root")));

    var cascade = new CascadeDeletion();
    cascade.catalogRepo = catalogs;
    cascade.namespaceRepo = namespaces;
    cascade.overlays = new CatalogOverlayRepository(pointers, blobs);
    cascade.markerStore = markerStoreOver(pointers);

    cascade.deleteCatalog(catalogId, catalogs.metaFor(catalogId), () -> {});

    assertTrue(catalogs.getById(catalogId).isEmpty());
    assertTrue(namespaces.getById(id("acct", "root", ResourceKind.RK_NAMESPACE)).isEmpty());
    assertTrue(namespaces.getById(id("acct", "child", ResourceKind.RK_NAMESPACE)).isEmpty());
    assertTrue(pointers.get(Keys.catalogDeletionMarker("acct", "catalog")).isEmpty());
  }

  @Test
  void overlayPermissionIsCheckedBeforeCatalogFenceIsInstalled() {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var catalogs = new CatalogRepository(pointers, blobs);
    ResourceId catalogId = id("acct", "catalog", ResourceKind.RK_CATALOG);
    catalogs.create(
        Catalog.newBuilder().setResourceId(catalogId).setDisplayName("catalog").build());
    CatalogOverlayRepository overlays = mock(CatalogOverlayRepository.class);
    when(overlays.countByCatalog("acct", "catalog")).thenReturn(1);

    var cascade = new CascadeDeletion();
    cascade.catalogRepo = catalogs;
    cascade.overlays = overlays;

    assertThrows(
        SecurityException.class,
        () ->
            cascade.deleteCatalog(
                catalogId,
                catalogs.metaFor(catalogId),
                () -> {
                  throw new SecurityException("denied");
                }));

    assertTrue(pointers.get(Keys.catalogDeletionMarker("acct", "catalog")).isEmpty());
  }

  @Test
  void validationRejectionReleasesNewCatalogFenceBeforeCleanup() {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var catalogs = new CatalogRepository(pointers, blobs);
    var namespaces = new NamespaceRepository(pointers, blobs);
    ResourceId catalogId = id("acct", "catalog", ResourceKind.RK_CATALOG);
    ResourceId namespaceId = id("acct", "local", ResourceKind.RK_NAMESPACE);
    catalogs.create(
        Catalog.newBuilder().setResourceId(catalogId).setDisplayName("catalog").build());
    namespaces.create(namespace(namespaceId, catalogId, "local", List.of()));

    var cascade = new CascadeDeletion();
    cascade.catalogRepo = catalogs;
    cascade.namespaceRepo = namespaces;
    cascade.overlays = new CatalogOverlayRepository(pointers, blobs);

    assertThrows(
        IllegalStateException.class,
        () ->
            cascade.prepareCatalogDeletion(
                catalogId,
                catalogs.metaFor(catalogId),
                () -> {},
                null,
                () -> {
                  throw new IllegalStateException("explicit cascade required");
                }));

    assertTrue(pointers.get(Keys.catalogDeletionMarker("acct", "catalog")).isEmpty());
    assertTrue(namespaces.getById(namespaceId).isPresent());
  }

  @Test
  void catalogFenceRefusesARelationBeforeNamespaceFencingBegins() {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var catalogs = new CatalogRepository(pointers, blobs);
    var namespaces = new NamespaceRepository(pointers, blobs);
    var tables = new TableRepository(pointers, blobs);
    var markers = markerStoreOver(pointers);
    ResourceId catalogId = id("acct", "catalog", ResourceKind.RK_CATALOG);
    ResourceId namespaceId = id("acct", "managed", ResourceKind.RK_NAMESPACE);
    ResourceId tableId = id("acct", "racing-table", ResourceKind.RK_TABLE);
    catalogs.create(
        Catalog.newBuilder().setResourceId(catalogId).setDisplayName("catalog").build());
    namespaces.create(namespace(namespaceId, catalogId, "managed", List.of()));

    var cascade = new CascadeDeletion();
    cascade.catalogRepo = catalogs;
    cascade.namespaceRepo = namespaces;
    cascade.overlays = new CatalogOverlayRepository(pointers, blobs);
    cascade.markerStore = markers;

    cascade.prepareCatalogDeletion(
        catalogId,
        catalogs.metaFor(catalogId),
        () -> {},
        null,
        () -> {
          var racingTable =
              Table.newBuilder()
                  .setResourceId(tableId)
                  .setCatalogId(catalogId)
                  .setNamespaceId(namespaceId)
                  .setDisplayName("racing")
                  .build();
          assertTrue(
              tables
                  .createWhilePointersMatch(
                      racingTable, markers.relationCreateFence(catalogId, namespaceId))
                  .isEmpty(),
              "the Catalog fence refuses a relation before namespace cleanup starts");
        });

    assertTrue(tables.getById(tableId).isEmpty());
  }

  @Test
  void failedPassLeavesDurableFencesAndTheNextPassResumes() {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    ResourceId catalogId = id("acct", "catalog", ResourceKind.RK_CATALOG);
    new CatalogRepository(pointers, blobs)
        .create(Catalog.newBuilder().setResourceId(catalogId).setDisplayName("catalog").build());

    ResourceId childId = id("acct", "child", ResourceKind.RK_NAMESPACE);
    ResourceId grandchildId = id("acct", "grandchild", ResourceKind.RK_NAMESPACE);
    var namespaces = new FailOnceNamespaceRepository(pointers, blobs, childId);
    namespaces.create(namespace(childId, catalogId, "child", List.of("root")));
    namespaces.create(namespace(grandchildId, catalogId, "grandchild", List.of("root", "child")));

    var cascade = new CascadeDeletion();
    cascade.namespaceRepo = namespaces;
    cascade.markerStore = markerStoreOver(pointers);

    assertThrows(
        BaseResourceRepository.AbortRetryableException.class,
        () -> cascade.deleteDescendantNamespaces(catalogId, List.of("root")));
    assertTrue(namespaces.getById(childId).isPresent());
    assertTrue(pointers.get(Keys.namespaceDeletionMarker("acct", "child")).isPresent());
    assertFalse(namespaces.getById(grandchildId).isPresent());

    cascade.deleteDescendantNamespaces(catalogId, List.of("root"));

    assertFalse(namespaces.getById(childId).isPresent());
    assertTrue(pointers.get(Keys.namespaceDeletionMarker("acct", "child")).isEmpty());
  }

  private static Namespace namespace(
      ResourceId id, ResourceId catalogId, String name, List<String> parents) {
    return Namespace.newBuilder()
        .setResourceId(id)
        .setCatalogId(catalogId)
        .setDisplayName(name)
        .addAllParents(parents)
        .build();
  }

  private static ResourceId id(String account, String id, ResourceKind kind) {
    return ResourceId.newBuilder().setAccountId(account).setId(id).setKind(kind).build();
  }

  private static MarkerStore markerStoreOver(InMemoryPointerStore pointers) {
    var markers = new MarkerStore();
    try {
      var field = MarkerStore.class.getDeclaredField("pointerStore");
      field.setAccessible(true);
      field.set(markers, pointers);
      return markers;
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  private static final class FailOnceNamespaceRepository extends NamespaceRepository {
    private final ResourceId failId;
    private boolean failed;

    private FailOnceNamespaceRepository(
        InMemoryPointerStore pointers, InMemoryBlobStore blobs, ResourceId failId) {
      super(pointers, blobs);
      this.failId = failId;
    }

    @Override
    public boolean deleteForCascade(
        ResourceId namespaceId,
        ResourceId catalogId,
        long expectedPointerVersion,
        long expectedFenceVersion,
        MarkerStore.MarkerRemoval markers) {
      if (!failed && namespaceId.equals(failId)) {
        failed = true;
        return false;
      }
      return super.deleteForCascade(
          namespaceId, catalogId, expectedPointerVersion, expectedFenceVersion, markers);
    }
  }
}
