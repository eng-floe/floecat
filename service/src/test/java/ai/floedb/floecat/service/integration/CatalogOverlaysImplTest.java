/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package ai.floedb.floecat.service.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.common.rpc.CreateMode;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.integration.rpc.*;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.CatalogIntegrationRepository;
import ai.floedb.floecat.service.repo.impl.CatalogOverlayRepository;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository.ResourceWithMeta;
import ai.floedb.floecat.service.repo.util.MarkerStore;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.storage.rpc.IdempotencyRecord;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.google.protobuf.FieldMask;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
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
    installBasePrincipal(service, service.principal);
    when(service.principal.get()).thenReturn(principal());
    var integration = CatalogIntegration.newBuilder().setResourceId(integrationId()).build();
    when(service.integrations.getByIdWithMeta(integrationId()))
        .thenReturn(
            Optional.of(
                new ResourceWithMeta<>(
                    integration, MutationMeta.newBuilder().setPointerVersion(5).build())));
    when(service.catalogs.prepareCreateOps(any())).thenReturn(List.of());
    when(service.overlays.createAttachedWithMetaAndCompanions(
            any(), any(), any(), anyLong(), any()))
        .thenAnswer(
            invocation -> {
              CatalogOverlay value = invocation.getArgument(0);
              var meta = MutationMeta.newBuilder().setPointerVersion(1).build();
              Function<ResourceWithMeta<CatalogOverlay>, List<PointerStore.CasOp>> companions =
                  invocation.getArgument(4);
              if (companions != null) {
                companions.apply(new ResourceWithMeta<>(value, meta));
              }
              return Optional.of(new ResourceWithMeta<>(value, meta));
            });
  }

  @Test
  void overlayOwnsACatalogCarryingItsName() {
    var response = create(CatalogOverlaySpec.newBuilder());

    var overlayCaptor = ArgumentCaptor.forClass(CatalogOverlay.class);
    verify(service.overlays)
        .createAttachedWithMetaAndCompanions(
            overlayCaptor.capture(), any(), any(), anyLong(), any());
    var catalogCaptor = ArgumentCaptor.forClass(Catalog.class);
    verify(service.catalogs).prepareCreateOps(catalogCaptor.capture());

    assertEquals("sales", response.getOverlay().getDisplayName());
    assertEquals(integrationId(), overlayCaptor.getValue().getIntegrationId());
    // The overlay and the catalog it owns reference each other and share a name.
    Catalog owned = catalogCaptor.getValue();
    assertEquals("sales", owned.getDisplayName());
    assertEquals(overlayCaptor.getValue().getResourceId(), owned.getOverlayId());
    assertEquals(owned.getResourceId(), overlayCaptor.getValue().getCatalogId());
  }

  @Test
  void createNormalizesOverlayNameBeforeLookupAndPersistence() {
    var response =
        service
            .createCatalogOverlay(
                CreateCatalogOverlayRequest.newBuilder()
                    .setSpec(
                        CatalogOverlaySpec.newBuilder()
                            .setDisplayName("  sales  ")
                            .setIntegrationId(integrationId()))
                    .build())
            .await()
            .indefinitely();

    var captured = ArgumentCaptor.forClass(CatalogOverlay.class);
    verify(service.overlays).getByNameWithMeta("acct", "sales");
    verify(service.overlays)
        .createAttachedWithMetaAndCompanions(captured.capture(), any(), any(), anyLong(), any());
    assertEquals("sales", captured.getValue().getDisplayName());
    assertEquals("sales", response.getOverlay().getDisplayName());
  }

  @Test
  void createRequiresOverlayWriteIntegrationUseAndCatalogWrite() {
    service.authz = new Authorizer();
    when(service.principal.get())
        .thenReturn(principal("catalog-overlay.write", "catalog-integration.use", "catalog.write"));
    create(CatalogOverlaySpec.newBuilder());
    verify(service.integrations).getByIdWithMeta(integrationId());
  }

  @Test
  void renamingAnOverlayRenamesTheCatalogItOwnsInTheSameTransaction() {
    ResourceId catalogId = id("catalog", ResourceKind.RK_CATALOG);
    var current =
        CatalogOverlay.newBuilder()
            .setResourceId(id("overlay", ResourceKind.RK_CATALOG_OVERLAY))
            .setDisplayName("sales")
            .setIntegrationId(integrationId())
            .setCatalogId(catalogId)
            .build();
    when(service.overlays.getByIdWithMeta(id("overlay", ResourceKind.RK_CATALOG_OVERLAY)))
        .thenReturn(
            Optional.of(
                new ResourceWithMeta<>(
                    current, MutationMeta.newBuilder().setPointerVersion(7).build())));
    when(service.catalogs.getByIdWithMeta(catalogId))
        .thenReturn(
            Optional.of(
                new ResourceWithMeta<>(
                    Catalog.newBuilder()
                        .setResourceId(catalogId)
                        .setDisplayName("sales")
                        .setOverlayId(id("overlay", ResourceKind.RK_CATALOG_OVERLAY))
                        .build(),
                    MutationMeta.newBuilder().setPointerVersion(3).build())));
    when(service.catalogs.prepareUpdateOps(any(), anyLong())).thenReturn(List.of());
    when(service.overlays.updateWithMetaUnlessIntegrationDeleting(any(), eq(7L), any()))
        .thenReturn(Optional.of(MutationMeta.newBuilder().setPointerVersion(8).build()));

    service
        .updateCatalogOverlay(
            UpdateCatalogOverlayRequest.newBuilder()
                .setOverlayId(id("overlay", ResourceKind.RK_CATALOG_OVERLAY))
                .setUpdateMask(FieldMask.newBuilder().addPaths("display_name"))
                .setSpec(CatalogOverlaySpec.newBuilder().setDisplayName("revenue"))
                .build())
        .await()
        .indefinitely();

    // The owned catalog is renamed from its own current pointer version, and those operations are
    // handed to the overlay update so both names move in one transaction.
    var renamed = ArgumentCaptor.forClass(Catalog.class);
    verify(service.catalogs).prepareUpdateOps(renamed.capture(), eq(3L));
    assertEquals("revenue", renamed.getValue().getDisplayName());
    assertEquals(id("overlay", ResourceKind.RK_CATALOG_OVERLAY), renamed.getValue().getOverlayId());
    verify(service.overlays).updateWithMetaUnlessIntegrationDeleting(any(), eq(7L), any());
  }

  @Test
  void selectionOnlyUpdateLeavesTheOwnedCatalogAlone() {
    var current =
        CatalogOverlay.newBuilder()
            .setResourceId(id("overlay", ResourceKind.RK_CATALOG_OVERLAY))
            .setDisplayName("sales")
            .setIntegrationId(integrationId())
            .setCatalogId(id("catalog", ResourceKind.RK_CATALOG))
            .build();
    when(service.overlays.getByIdWithMeta(id("overlay", ResourceKind.RK_CATALOG_OVERLAY)))
        .thenReturn(
            Optional.of(
                new ResourceWithMeta<>(
                    current, MutationMeta.newBuilder().setPointerVersion(7).build())));
    when(service.overlays.updateWithMetaUnlessIntegrationDeleting(any(), eq(7L), any()))
        .thenReturn(Optional.of(MutationMeta.newBuilder().setPointerVersion(8).build()));

    service
        .updateCatalogOverlay(
            UpdateCatalogOverlayRequest.newBuilder()
                .setOverlayId(id("overlay", ResourceKind.RK_CATALOG_OVERLAY))
                .setUpdateMask(FieldMask.newBuilder().addPaths("include_namespaces"))
                .setSpec(
                    CatalogOverlaySpec.newBuilder()
                        .addIncludeNamespaces(NamespacePath.newBuilder().addSegments("foo")))
                .build())
        .await()
        .indefinitely();

    verify(service.catalogs, never()).prepareUpdateOps(any(), anyLong());
  }

  @Test
  void createRejectsNameReservedByCatalog() {
    when(service.catalogs.getByName("acct", "sales"))
        .thenReturn(
            Optional.of(
                Catalog.newBuilder()
                    .setResourceId(id("catalog", ResourceKind.RK_CATALOG))
                    .setDisplayName("sales")
                    .build()));
    // The owned catalog loses its name reservation inside the atomic batch; the repository reports
    // that as a name conflict and the service renders it as ALREADY_EXISTS.
    when(service.overlays.createAttachedWithMetaAndCompanions(
            any(), any(), any(), anyLong(), any()))
        .thenThrow(
            new BaseResourceRepository.NameConflictException("companion pointer already reserved"));

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
    verify(service.overlays)
        .createAttachedWithMetaAndCompanions(any(), any(), any(), anyLong(), any());
  }

  @Test
  void createRejectsUnknownConflictMode() {
    var error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .createCatalogOverlay(
                        CreateCatalogOverlayRequest.newBuilder()
                            .setCreateModeValue(99)
                            .setSpec(
                                CatalogOverlaySpec.newBuilder()
                                    .setDisplayName("sales")
                                    .setIntegrationId(integrationId()))
                            .build())
                    .await()
                    .indefinitely());
    assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
  }

  @Test
  void ifNotExistsReturnsExistingBeforeValidatingUnusedIntegrationBinding() {
    var existing =
        CatalogOverlay.newBuilder()
            .setResourceId(id("existing", ResourceKind.RK_CATALOG_OVERLAY))
            .setDisplayName("sales")
            .setIntegrationId(integrationId())
            .build();
    var meta = MutationMeta.newBuilder().setPointerVersion(9L).build();
    when(service.overlays.getByNameWithMeta("acct", "sales"))
        .thenReturn(Optional.of(new ResourceWithMeta<>(existing, meta)));

    var response =
        service
            .createCatalogOverlay(
                CreateCatalogOverlayRequest.newBuilder()
                    .setCreateMode(CreateMode.CM_RETURN_EXISTING)
                    .setSpec(CatalogOverlaySpec.newBuilder().setDisplayName("sales"))
                    .build())
            .await()
            .indefinitely();

    assertEquals(existing, response.getOverlay());
    assertEquals(meta, response.getMeta());
    verify(service.integrations, never()).getByIdWithMeta(any());
    verify(service.overlays, never())
        .createAttachedWithMetaAndCompanions(any(), any(), any(), anyLong(), any());
  }

  @Test
  void namespaceNamesAreExactAndPathsAreDeduplicatedDeterministically() {
    create(
        CatalogOverlaySpec.newBuilder()
            .addIncludeNamespaces(path("z"))
            .addIncludeNamespaces(path("a", "b"))
            .addIncludeNamespaces(path("z")));
    var captured = ArgumentCaptor.forClass(CatalogOverlay.class);
    verify(service.overlays)
        .createAttachedWithMetaAndCompanions(captured.capture(), any(), any(), anyLong(), any());
    assertEquals(2, captured.getValue().getIncludeNamespacesCount());
    assertEquals(path("a", "b"), captured.getValue().getIncludeNamespaces(0));
  }

  @Test
  void idempotencyFingerprintPreservesPathBoundaries() {
    byte[] one =
        CatalogOverlaysImpl.canonicalFingerprintForTest(
            "o", integrationId(), List.of(path("a", "b")), List.of());
    byte[] two =
        CatalogOverlaysImpl.canonicalFingerprintForTest(
            "o", integrationId(), List.of(path("a"), path("b")), List.of());
    assertFalse(Arrays.equals(one, two));
  }

  @Test
  void updateCanReplaceNamespaceSelection() {
    ResourceId overlayId = id("overlay", ResourceKind.RK_CATALOG_OVERLAY);
    ResourceId catalogId = id("catalog", ResourceKind.RK_CATALOG);
    var row =
        CatalogOverlay.newBuilder()
            .setResourceId(overlayId)
            .setCatalogId(catalogId)
            .setDisplayName("sales")
            .setIntegrationId(integrationId())
            .build();
    var meta = MutationMeta.newBuilder().setPointerVersion(7).build();
    when(service.overlays.getByIdWithMeta(overlayId))
        .thenReturn(Optional.of(new ResourceWithMeta<>(row, meta)));
    when(service.catalogs.getByIdWithMeta(catalogId))
        .thenReturn(
            Optional.of(
                new ResourceWithMeta<>(
                    Catalog.newBuilder()
                        .setResourceId(catalogId)
                        .setDisplayName("sales")
                        .setOverlayId(overlayId)
                        .build(),
                    MutationMeta.newBuilder().setPointerVersion(3L).build())));
    when(service.catalogs.prepareUpdateOps(any(), eq(3L))).thenReturn(List.of());
    when(service.overlays.updateWithMetaUnlessIntegrationDeleting(any(), eq(7L), any()))
        .thenReturn(Optional.of(MutationMeta.newBuilder().setPointerVersion(8).build()));

    var response =
        service
            .updateCatalogOverlay(
                UpdateCatalogOverlayRequest.newBuilder()
                    .setOverlayId(overlayId)
                    .setSpec(CatalogOverlaySpec.newBuilder().addExcludeNamespaces(path("private")))
                    .setUpdateMask(FieldMask.newBuilder().addPaths("exclude_namespaces"))
                    .build())
            .await()
            .indefinitely();
    assertEquals(path("private"), response.getOverlay().getExcludeNamespaces(0));
    assertEquals(8L, response.getMeta().getPointerVersion());
  }

  @Test
  void updateNormalizesSqlCatalogNameBeforeCollisionCheckAndPersistence() {
    ResourceId overlayId = id("overlay", ResourceKind.RK_CATALOG_OVERLAY);
    ResourceId catalogId = id("catalog", ResourceKind.RK_CATALOG);
    var row =
        CatalogOverlay.newBuilder()
            .setResourceId(overlayId)
            .setCatalogId(catalogId)
            .setDisplayName("sales")
            .setIntegrationId(integrationId())
            .build();
    var meta = MutationMeta.newBuilder().setPointerVersion(7).build();
    when(service.overlays.getByIdWithMeta(overlayId))
        .thenReturn(Optional.of(new ResourceWithMeta<>(row, meta)));
    when(service.catalogs.getByIdWithMeta(catalogId))
        .thenReturn(
            Optional.of(
                new ResourceWithMeta<>(
                    Catalog.newBuilder()
                        .setResourceId(catalogId)
                        .setDisplayName("sales")
                        .setOverlayId(overlayId)
                        .build(),
                    MutationMeta.newBuilder().setPointerVersion(3L).build())));
    when(service.catalogs.prepareUpdateOps(any(), eq(3L))).thenReturn(List.of());
    when(service.overlays.updateWithMetaUnlessIntegrationDeleting(any(), eq(7L), any()))
        .thenReturn(Optional.of(MutationMeta.newBuilder().setPointerVersion(8).build()));

    var response =
        service
            .updateCatalogOverlay(
                UpdateCatalogOverlayRequest.newBuilder()
                    .setOverlayId(overlayId)
                    .setSpec(CatalogOverlaySpec.newBuilder().setDisplayName("  revenue  "))
                    .setUpdateMask(FieldMask.newBuilder().addPaths("display_name"))
                    .build())
            .await()
            .indefinitely();

    verify(service.catalogs).getByName("acct", "revenue");
    var desired = ArgumentCaptor.forClass(CatalogOverlay.class);
    verify(service.overlays)
        .updateWithMetaUnlessIntegrationDeleting(desired.capture(), eq(7L), any());
    assertEquals("revenue", desired.getValue().getDisplayName());
    assertEquals("revenue", response.getOverlay().getDisplayName());
  }

  @Test
  void legacyConnectorPermissionDoesNotAuthorizeCreate() {
    service.authz = new Authorizer();
    when(service.principal.get()).thenReturn(principal("connector.manage"));
    var error =
        assertThrows(StatusRuntimeException.class, () -> create(CatalogOverlaySpec.newBuilder()));
    assertEquals(Status.Code.PERMISSION_DENIED, error.getStatus().getCode());
  }

  @Test
  void createOrReplaceSwapsIdentityAndCanChangeIntegrationBinding() {
    ResourceId overlayId = id("overlay", ResourceKind.RK_CATALOG_OVERLAY);
    ResourceId catalogId = id("catalog", ResourceKind.RK_CATALOG);
    ResourceId oldIntegrationId = id("old-integration", ResourceKind.RK_CATALOG_INTEGRATION);
    var current =
        CatalogOverlay.newBuilder()
            .setResourceId(overlayId)
            .setCatalogId(catalogId)
            .setDisplayName("sales")
            .setIntegrationId(oldIntegrationId)
            .build();
    when(service.overlays.getByNameWithMeta("acct", "sales"))
        .thenReturn(
            Optional.of(
                new ResourceWithMeta<>(
                    current, MutationMeta.newBuilder().setPointerVersion(4L).build())));
    when(service.catalogs.getByIdWithMeta(catalogId))
        .thenReturn(
            Optional.of(
                new ResourceWithMeta<>(
                    Catalog.newBuilder()
                        .setResourceId(catalogId)
                        .setDisplayName("sales")
                        .setOverlayId(overlayId)
                        .build(),
                    MutationMeta.newBuilder().setPointerVersion(8L).build())));
    when(service.catalogs.prepareUpdateOps(any(), eq(8L))).thenReturn(List.of());
    when(service.integrations.getByIdWithMeta(oldIntegrationId))
        .thenReturn(
            Optional.of(
                new ResourceWithMeta<>(
                    CatalogIntegration.newBuilder().setResourceId(oldIntegrationId).build(),
                    MutationMeta.newBuilder().setPointerVersion(6L).build())));
    when(service.overlays.replaceIdentityAttachedWithMeta(
            any(), eq(4L), any(), any(), any(), any(), any()))
        .thenAnswer(
            invocation ->
                Optional.of(
                    new ResourceWithMeta<>(
                        invocation.getArgument(2),
                        MutationMeta.newBuilder().setPointerVersion(1L).build())));

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

    assertNotEquals(overlayId, response.getOverlay().getResourceId());
    assertEquals(integrationId(), response.getOverlay().getIntegrationId());
    assertEquals(1L, response.getMeta().getPointerVersion());
    verify(service.overlays)
        .replaceIdentityAttachedWithMeta(any(), eq(4L), any(), any(), any(), any(), any());
    assertEquals(catalogId, response.getOverlay().getCatalogId());
    var owner = ArgumentCaptor.forClass(Catalog.class);
    verify(service.catalogs).prepareUpdateOps(owner.capture(), eq(8L));
    assertEquals(response.getOverlay().getResourceId(), owner.getValue().getOverlayId());
  }

  @Test
  void idempotentCreateRequiresAtomicReceiptWithResourcePublication() {
    var receipt = new AtomicReference<IdempotencyRecord>();
    when(service.idempotencyStore.get(any()))
        .thenAnswer(invocation -> Optional.ofNullable(receipt.get()));
    when(service.idempotencyStore.createPending(
            any(), any(), any(), any(), any(ResourceId.class), any(), any()))
        .thenReturn(true);
    when(service.idempotencyStore.prepareSuccess(
            any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              String opName = invocation.getArgument(2, String.class);
              String requestHash = invocation.getArgument(3, String.class);
              ResourceId resourceId = invocation.getArgument(4, ResourceId.class);
              MutationMeta meta = invocation.getArgument(5, MutationMeta.class);
              byte[] payload = invocation.getArgument(6, byte[].class);
              com.google.protobuf.Timestamp createdAt =
                  invocation.getArgument(7, com.google.protobuf.Timestamp.class);
              com.google.protobuf.Timestamp expiresAt =
                  invocation.getArgument(8, com.google.protobuf.Timestamp.class);
              receipt.set(
                  IdempotencyRecord.newBuilder()
                      .setOpName(opName)
                      .setRequestHash(requestHash)
                      .setStatus(IdempotencyRecord.Status.SUCCEEDED)
                      .setResourceId(resourceId)
                      .setMeta(meta)
                      .setPayload(com.google.protobuf.ByteString.copyFrom(payload))
                      .setCreatedAt(createdAt)
                      .setExpiresAt(expiresAt)
                      .build());
              return new PointerStore.CasUpsert(
                  "receipt", 1L, Pointer.newBuilder().setKey("receipt").build());
            });
    when(service.overlays.createAttachedWithMetaAndCompanions(
            any(), any(), any(), anyLong(), any()))
        .thenAnswer(
            invocation -> {
              CatalogOverlay value = invocation.getArgument(0);
              var row =
                  new ResourceWithMeta<>(
                      value, MutationMeta.newBuilder().setPointerVersion(1L).build());
              Function<ResourceWithMeta<CatalogOverlay>, PointerStore.CasUpsert> completion =
                  invocation.getArgument(4);
              completion.apply(row);
              return Optional.of(row);
            });

    var response =
        service
            .createCatalogOverlay(
                CreateCatalogOverlayRequest.newBuilder()
                    .setIdempotency(
                        ai.floedb.floecat.common.rpc.IdempotencyKey.newBuilder().setKey("key"))
                    .setSpec(
                        CatalogOverlaySpec.newBuilder()
                            .setDisplayName("sales")
                            .setIntegrationId(integrationId()))
                    .build())
            .await()
            .indefinitely();

    assertEquals(receipt.get().getResourceId(), response.getOverlay().getResourceId());
    verify(service.idempotencyStore)
        .prepareSuccess(any(), any(), any(), any(), any(), any(), any(), any(), any());
    verify(service.idempotencyStore, never())
        .finalizeSuccess(any(), any(), any(), any(), any(), any(), any(), any(), any());
    verify(service.idempotencyStore, never()).delete(any());
  }

  @Test
  void idempotentCreateAdoptsReservedIdentityPublishedByConcurrentRetry() {
    var reservedId = new AtomicReference<ResourceId>();
    var requestHash = new AtomicReference<String>();
    var createdAt = new AtomicReference<com.google.protobuf.Timestamp>();
    var expiresAt = new AtomicReference<com.google.protobuf.Timestamp>();
    var meta = MutationMeta.newBuilder().setPointerVersion(9L).build();
    when(service.idempotencyStore.get(any()))
        .thenAnswer(
            invocation -> {
              if (reservedId.get() == null) return Optional.empty();
              var overlay =
                  CatalogOverlay.newBuilder()
                      .setResourceId(reservedId.get())
                      .setDisplayName("sales")
                      .setIntegrationId(integrationId())
                      .build();
              return Optional.of(
                  IdempotencyRecord.newBuilder()
                      .setOpName("CreateCatalogOverlay")
                      .setRequestHash(requestHash.get())
                      .setStatus(IdempotencyRecord.Status.SUCCEEDED)
                      .setResourceId(reservedId.get())
                      .setMeta(meta)
                      .setPayload(overlay.toByteString())
                      .setCreatedAt(createdAt.get())
                      .setExpiresAt(expiresAt.get())
                      .build());
            });
    when(service.idempotencyStore.createPending(
            any(), any(), any(), any(), any(ResourceId.class), any(), any()))
        .thenAnswer(
            invocation -> {
              requestHash.set(invocation.getArgument(3));
              reservedId.set(invocation.getArgument(4));
              createdAt.set(invocation.getArgument(5));
              expiresAt.set(invocation.getArgument(6));
              return true;
            });
    when(service.overlays.getByNameWithMeta("acct", "sales"))
        .thenAnswer(
            invocation ->
                Optional.of(
                    new ResourceWithMeta<>(
                        CatalogOverlay.newBuilder()
                            .setResourceId(reservedId.get())
                            .setDisplayName("sales")
                            .setIntegrationId(integrationId())
                            .build(),
                        meta)));

    var response =
        service
            .createCatalogOverlay(
                CreateCatalogOverlayRequest.newBuilder()
                    .setIdempotency(
                        ai.floedb.floecat.common.rpc.IdempotencyKey.newBuilder().setKey("key"))
                    .setSpec(
                        CatalogOverlaySpec.newBuilder()
                            .setDisplayName("sales")
                            .setIntegrationId(integrationId()))
                    .build())
            .await()
            .indefinitely();

    assertEquals(reservedId.get(), response.getOverlay().getResourceId());
    assertEquals(meta, response.getMeta());
    verify(service.overlays, never())
        .createAttachedWithMetaAndCompanions(any(), any(), any(), anyLong(), any());
    verify(service.idempotencyStore, never())
        .prepareSuccess(any(), any(), any(), any(), any(), any(), any(), any(), any());
  }

  private CreateCatalogOverlayResponse create(CatalogOverlaySpec.Builder spec) {
    return service
        .createCatalogOverlay(
            CreateCatalogOverlayRequest.newBuilder()
                .setSpec(spec.setDisplayName("sales").setIntegrationId(integrationId()))
                .build())
        .await()
        .indefinitely();
  }

  private static PrincipalContext principal() {
    return principal("catalog-overlay.read", "catalog-overlay.write", "catalog-integration.use");
  }

  private static PrincipalContext principal(String... permissions) {
    return PrincipalContext.newBuilder()
        .setAccountId("acct")
        .setCorrelationId("corr")
        .addAllPermissions(List.of(permissions))
        .build();
  }

  private static NamespacePath path(String... segments) {
    return NamespacePath.newBuilder().addAllSegments(List.of(segments)).build();
  }

  private static ResourceId integrationId() {
    return id("integration", ResourceKind.RK_CATALOG_INTEGRATION);
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
