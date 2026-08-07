/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.floedb.floecat.service.catalog.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.CatalogSpec;
import ai.floedb.floecat.catalog.rpc.DeleteCatalogRequest;
import ai.floedb.floecat.catalog.rpc.UpdateCatalogRequest;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.Precondition;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.service.context.EngineContextProvider;
import ai.floedb.floecat.service.metagraph.overlay.user.UserGraph;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.BatchGuard;
import ai.floedb.floecat.service.repo.util.MarkerStore;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import com.google.protobuf.FieldMask;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CatalogServiceImplSystemCatalogTest {

  private CatalogServiceImpl svc;
  private CatalogRepository catalogRepo;
  private NamespaceRepository namespaceRepo;
  private CatalogOverlay overlay;
  private EngineContextProvider engineContext;
  private MarkerStore markerStore;
  private UserGraph metadataGraph;
  private RecursiveResourceDropper recursiveDropper;
  private Authorizer authz;

  @BeforeEach
  void setup() {
    svc = new CatalogServiceImpl();

    catalogRepo = mock(CatalogRepository.class);
    namespaceRepo = mock(NamespaceRepository.class);
    PrincipalProvider principal = mock(PrincipalProvider.class);
    authz = mock(Authorizer.class);
    engineContext = mock(EngineContextProvider.class);
    overlay = mock(CatalogOverlay.class);
    markerStore = mock(MarkerStore.class);
    metadataGraph = mock(UserGraph.class);
    recursiveDropper = mock(RecursiveResourceDropper.class);

    svc.catalogRepo = catalogRepo;
    svc.namespaceRepo = namespaceRepo;
    svc.principal = principal;
    svc.authz = authz;
    svc.engineContext = engineContext;
    svc.overlay = overlay;
    svc.markerStore = markerStore;
    svc.metadataGraph = metadataGraph;
    svc.recursiveDropper = recursiveDropper;

    var pc = mock(PrincipalContext.class);
    when(principal.get()).thenReturn(pc);
    when(pc.getCorrelationId()).thenReturn("corr");
    when(pc.getAccountId()).thenReturn("acct");
    when(engineContext.isPresent()).thenReturn(true);
    when(engineContext.effectiveEngineKind()).thenReturn("floecat_internal");
    when(overlay.catalog(any())).thenReturn(Optional.empty());
    doNothing().when(authz).require(any(), anyString());
    when(authz.allows(any(), eq("namespace.write"))).thenReturn(true);
  }

  @Test
  void updateCatalog_systemCatalog_permissionDenied() {
    ResourceId systemCatalogId = systemCatalogId();

    var req =
        UpdateCatalogRequest.newBuilder()
            .setCatalogId(systemCatalogId)
            .setSpec(CatalogSpec.newBuilder().setDisplayName("x").build())
            .setUpdateMask(FieldMask.newBuilder().addPaths("display_name").build())
            .build();

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class, () -> svc.updateCatalog(req).await().indefinitely());

    assertEquals(Status.Code.PERMISSION_DENIED, ex.getStatus().getCode());
    verifyNoInteractions(catalogRepo);
  }

  @Test
  void deleteCatalog_systemCatalog_permissionDenied() {
    ResourceId systemCatalogId = systemCatalogId();

    var req = DeleteCatalogRequest.newBuilder().setCatalogId(systemCatalogId).build();

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class, () -> svc.deleteCatalog(req).await().indefinitely());

    assertEquals(Status.Code.PERMISSION_DENIED, ex.getStatus().getCode());
    verifyNoInteractions(catalogRepo);
  }

  @Test
  void deleteCatalog_missingUserCatalog_isIdempotent() {
    // Delete of an already-gone user catalog (no precondition) is a best-effort no-op, not
    // NOT_FOUND: the delete guard must not require the catalog to resolve through the overlay
    // before the repository fallback runs.
    ResourceId id =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_CATALOG)
            .setId("already-gone")
            .build();
    when(markerStore.catalogMarkerVersion(id)).thenReturn(0L);
    when(catalogRepo.metaFor(id))
        .thenThrow(new BaseResourceRepository.NotFoundException("catalog missing"));
    when(catalogRepo.metaForSafe(id)).thenReturn(MutationMeta.getDefaultInstance());

    var req = DeleteCatalogRequest.newBuilder().setCatalogId(id).build();

    var response = svc.deleteCatalog(req).await().indefinitely();

    assertEquals(0L, response.getMeta().getPointerVersion());
    verify(metadataGraph).invalidate(id);
  }

  @Test
  void conditionalDeleteOfMissingCatalogStillReclaimsItsChildrenMarker() {
    ResourceId id =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_CATALOG)
            .setId("already-gone")
            .build();
    when(markerStore.catalogMarkerVersion(id)).thenReturn(0L);
    when(catalogRepo.metaFor(id))
        .thenThrow(new BaseResourceRepository.NotFoundException("catalog missing"));
    when(catalogRepo.metaForSafe(id)).thenReturn(MutationMeta.getDefaultInstance());

    var request =
        DeleteCatalogRequest.newBuilder()
            .setCatalogId(id)
            .setPrecondition(Precondition.newBuilder().setExpectedVersion(7L))
            .build();

    var failure =
        assertThrows(
            StatusRuntimeException.class, () -> svc.deleteCatalog(request).await().indefinitely());

    assertEquals(Status.Code.NOT_FOUND, failure.getStatus().getCode());
    verify(markerStore).deleteCatalogMarker(id);
  }

  @Test
  void deleteCatalogChecksTheScannedMarkerInsideThePointerDelete() {
    ResourceId id =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_CATALOG)
            .setId("user-catalog")
            .build();
    var meta =
        MutationMeta.newBuilder()
            .setPointerVersion(7L)
            .setPointerKey("/accounts/acct/catalogs/by-id/user-catalog")
            .build();
    var childrenGuard = mock(BatchGuard.class);
    when(markerStore.catalogMarkerVersion(id)).thenReturn(4L);
    when(markerStore.catalogChildrenUnchangedGuard(id, 4L)).thenReturn(childrenGuard);
    when(catalogRepo.metaFor(id)).thenReturn(meta);
    when(namespaceRepo.hasAnyRefUnder("acct", "user-catalog", java.util.List.of()))
        .thenReturn(false);
    when(catalogRepo.deleteWithPrecondition(id, 7L, childrenGuard)).thenReturn(true);

    svc.deleteCatalog(DeleteCatalogRequest.newBuilder().setCatalogId(id).build())
        .await()
        .indefinitely();

    verify(catalogRepo).deleteWithPrecondition(eq(id), eq(7L), eq(childrenGuard));
    verify(recursiveDropper, never()).reclaimStrandedNamespacePaths(id);
    verify(markerStore).deleteCatalogMarker(id);
  }

  @Test
  void deleteCatalogRejectsAStalePreconditionBeforeRepairingNamespaceRows() {
    ResourceId id =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_CATALOG)
            .setId("user-catalog")
            .build();
    when(catalogRepo.metaFor(id))
        .thenReturn(
            MutationMeta.newBuilder()
                .setPointerVersion(7L)
                .setPointerKey("/accounts/acct/catalogs/by-id/user-catalog")
                .build());
    var request =
        DeleteCatalogRequest.newBuilder()
            .setCatalogId(id)
            .setPrecondition(Precondition.newBuilder().setExpectedVersion(6L))
            .build();

    StatusRuntimeException failure =
        assertThrows(
            StatusRuntimeException.class, () -> svc.deleteCatalog(request).await().indefinitely());

    assertEquals(Status.Code.FAILED_PRECONDITION, failure.getStatus().getCode());
    verify(namespaceRepo, never()).hasAnyRefUnder(anyString(), anyString(), any());
    verify(recursiveDropper, never()).reclaimStrandedNamespacePaths(id);
  }

  @Test
  void deleteCatalogRejectsALiveNamespaceWithoutRunningTheRepairSweep() {
    ResourceId id =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_CATALOG)
            .setId("user-catalog")
            .build();
    when(catalogRepo.metaFor(id))
        .thenReturn(
            MutationMeta.newBuilder()
                .setPointerVersion(7L)
                .setPointerKey("/accounts/acct/catalogs/by-id/user-catalog")
                .build());
    when(namespaceRepo.hasAnyRefUnder("acct", "user-catalog", java.util.List.of()))
        .thenReturn(true);
    when(namespaceRepo.hasLiveRefUnder("acct", "user-catalog", java.util.List.of()))
        .thenReturn(true);

    assertThrows(
        StatusRuntimeException.class,
        () ->
            svc.deleteCatalog(DeleteCatalogRequest.newBuilder().setCatalogId(id).build())
                .await()
                .indefinitely());

    verify(namespaceRepo).hasLiveRefUnder("acct", "user-catalog", java.util.List.of());
    verify(recursiveDropper, never()).reclaimStrandedNamespacePaths(id);
  }

  @Test
  void deleteCatalogDoesNotReclaimNamespaceRowsWithoutNamespaceWrite() {
    ResourceId id =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_CATALOG)
            .setId("user-catalog")
            .build();
    when(catalogRepo.metaFor(id))
        .thenReturn(
            MutationMeta.newBuilder()
                .setPointerVersion(7L)
                .setPointerKey("/accounts/acct/catalogs/by-id/user-catalog")
                .build());
    when(namespaceRepo.hasAnyRefUnder("acct", "user-catalog", java.util.List.of()))
        .thenReturn(true);
    when(authz.allows(any(), eq("namespace.write"))).thenReturn(false);

    assertThrows(
        StatusRuntimeException.class,
        () ->
            svc.deleteCatalog(DeleteCatalogRequest.newBuilder().setCatalogId(id).build())
                .await()
                .indefinitely());

    verify(namespaceRepo, never()).hasLiveRefUnder(anyString(), anyString(), any());
    verify(recursiveDropper, never()).reclaimStrandedNamespacePaths(id);
  }

  private static ResourceId systemCatalogId() {
    return SystemNodeRegistry.systemCatalogContainerId("floecat_internal");
  }
}
