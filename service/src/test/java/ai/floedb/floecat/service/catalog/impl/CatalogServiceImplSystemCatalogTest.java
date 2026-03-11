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
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.CatalogSpec;
import ai.floedb.floecat.catalog.rpc.DeleteCatalogRequest;
import ai.floedb.floecat.catalog.rpc.GetCatalogRequest;
import ai.floedb.floecat.catalog.rpc.ListCatalogsRequest;
import ai.floedb.floecat.catalog.rpc.UpdateCatalogRequest;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.service.context.EngineContextProvider;
import ai.floedb.floecat.service.metagraph.overlay.user.UserGraph;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.util.MarkerStore;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import com.google.protobuf.FieldMask;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CatalogServiceImplSystemCatalogTest {

  private CatalogServiceImpl svc;
  private CatalogRepository catalogRepo;
  private CatalogOverlay overlay;
  private EngineContextProvider engineContext;
  private MarkerStore markerStore;
  private UserGraph metadataGraph;

  @BeforeEach
  void setup() {
    svc = new CatalogServiceImpl();

    catalogRepo = mock(CatalogRepository.class);
    PrincipalProvider principal = mock(PrincipalProvider.class);
    Authorizer authz = mock(Authorizer.class);
    engineContext = mock(EngineContextProvider.class);
    overlay = mock(CatalogOverlay.class);
    markerStore = mock(MarkerStore.class);
    metadataGraph = mock(UserGraph.class);

    svc.catalogRepo = catalogRepo;
    svc.principal = principal;
    svc.authz = authz;
    svc.engineContext = engineContext;
    svc.overlay = overlay;
    svc.markerStore = markerStore;
    svc.metadataGraph = metadataGraph;

    var pc = mock(PrincipalContext.class);
    when(principal.get()).thenReturn(pc);
    when(pc.getCorrelationId()).thenReturn("corr");
    when(pc.getAccountId()).thenReturn("acct");
    when(engineContext.isPresent()).thenReturn(true);
    when(engineContext.effectiveEngineKind()).thenReturn("floecat_internal");
    when(overlay.catalog(any())).thenReturn(Optional.empty());
    doNothing().when(authz).require(any(), anyString());
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
  void getCatalog_systemCatalogIdFromUserAccount_resolvesSyntheticSystemCatalog() {
    ResourceId canonicalSystemId = systemCatalogId();
    ResourceId callerScopedId = callerScopedCatalogId(canonicalSystemId);

    var req = GetCatalogRequest.newBuilder().setCatalogId(callerScopedId).build();
    when(catalogRepo.getById(callerScopedId)).thenReturn(Optional.empty());
    when(overlay.catalog(canonicalSystemId))
        .thenReturn(Optional.of(systemCatalogNode(canonicalSystemId)));
    var res = svc.getCatalog(req).await().indefinitely();

    assertEquals("floecat_internal", res.getCatalog().getDisplayName());
    assertEquals(canonicalSystemId.getId(), res.getCatalog().getResourceId().getId());
    verify(catalogRepo).getById(callerScopedId);
    verify(overlay).catalog(canonicalSystemId);
    verifyNoMoreInteractions(catalogRepo);
  }

  @Test
  void getCatalog_systemCatalog_notVisibleInOverlay_notFound() {
    ResourceId canonicalSystemId = systemCatalogId();
    ResourceId callerScopedId = callerScopedCatalogId(canonicalSystemId);

    var req = GetCatalogRequest.newBuilder().setCatalogId(callerScopedId).build();
    when(catalogRepo.getById(callerScopedId)).thenReturn(Optional.empty());
    when(overlay.catalog(canonicalSystemId)).thenReturn(Optional.empty());

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class, () -> svc.getCatalog(req).await().indefinitely());
    assertEquals(Status.Code.NOT_FOUND, ex.getStatus().getCode());
    verify(catalogRepo).getById(callerScopedId);
    verify(overlay).catalog(canonicalSystemId);
  }

  @Test
  void listCatalogs_allowsRepoTokenWithCatPrefix() {
    String repoToken = "cat:repo_cursor";
    var req =
        ListCatalogsRequest.newBuilder()
            .setPage(PageRequest.newBuilder().setPageSize(2).setPageToken(repoToken))
            .build();

    ResourceId canonicalSystemId = systemCatalogId();
    when(overlay.catalog(canonicalSystemId))
        .thenReturn(Optional.of(systemCatalogNode(canonicalSystemId)));

    when(catalogRepo.count("acct")).thenReturn(1);
    when(catalogRepo.list(eq("acct"), eq(2), eq(repoToken), any(StringBuilder.class)))
        .thenReturn(List.of(Catalog.newBuilder().setDisplayName("examples").build()));

    var res = svc.listCatalogs(req).await().indefinitely();

    assertEquals(2, res.getCatalogsCount());
    verify(catalogRepo).list(eq("acct"), eq(2), eq(repoToken), any(StringBuilder.class));
  }

  @Test
  void listCatalogs_rawCatSystemToken_treatedAsRepoToken() {
    String repoToken = "cat:system";
    when(catalogRepo.count("acct")).thenReturn(1);
    when(catalogRepo.list(eq("acct"), eq(2), eq(repoToken), any(StringBuilder.class)))
        .thenReturn(List.of(Catalog.newBuilder().setDisplayName("examples").build()));

    var req =
        ListCatalogsRequest.newBuilder()
            .setPage(PageRequest.newBuilder().setPageSize(2).setPageToken(repoToken))
            .build();
    var res = svc.listCatalogs(req).await().indefinitely();

    assertEquals(1, res.getCatalogsCount());
    assertEquals("examples", res.getCatalogs(0).getDisplayName());
    verify(catalogRepo).list(eq("acct"), eq(2), eq(repoToken), any(StringBuilder.class));
    verify(overlay).catalog(systemCatalogId());
  }

  @Test
  void listCatalogs_repoEndEmitsServiceOwnedSystemToken() {
    ResourceId canonicalSystemId = systemCatalogId();
    when(overlay.catalog(canonicalSystemId))
        .thenReturn(Optional.of(systemCatalogNode(canonicalSystemId)));
    when(catalogRepo.count("acct")).thenReturn(1);
    when(catalogRepo.list(eq("acct"), eq(1), eq(""), any(StringBuilder.class)))
        .thenReturn(List.of(Catalog.newBuilder().setDisplayName("examples").build()));

    var req =
        ListCatalogsRequest.newBuilder().setPage(PageRequest.newBuilder().setPageSize(1)).build();
    var res = svc.listCatalogs(req).await().indefinitely();

    assertEquals(systemPhasePageToken(), res.getPage().getNextPageToken());
  }

  @Test
  void listCatalogs_systemCatalogNotVisible_notListed() {
    ResourceId canonicalSystemId = systemCatalogId();
    when(overlay.catalog(canonicalSystemId)).thenReturn(Optional.empty());
    when(catalogRepo.count("acct")).thenReturn(1);
    when(catalogRepo.list(eq("acct"), eq(5), eq(""), any(StringBuilder.class)))
        .thenReturn(List.of(Catalog.newBuilder().setDisplayName("examples").build()));

    var req =
        ListCatalogsRequest.newBuilder().setPage(PageRequest.newBuilder().setPageSize(5)).build();
    var res = svc.listCatalogs(req).await().indefinitely();

    assertEquals(1, res.getCatalogsCount());
    assertEquals("examples", res.getCatalogs(0).getDisplayName());
  }

  @Test
  void listCatalogs_engineAbsent_onlyUserCatalogsWhenDefaultSystemNotVisible() {
    when(engineContext.isPresent()).thenReturn(false);
    when(catalogRepo.count("acct")).thenReturn(1);
    when(catalogRepo.list(eq("acct"), eq(5), eq(""), any(StringBuilder.class)))
        .thenReturn(List.of(Catalog.newBuilder().setDisplayName("examples").build()));

    var req =
        ListCatalogsRequest.newBuilder().setPage(PageRequest.newBuilder().setPageSize(5)).build();
    var res = svc.listCatalogs(req).await().indefinitely();

    assertEquals(1, res.getCatalogsCount());
    assertEquals("examples", res.getCatalogs(0).getDisplayName());
    verify(overlay).catalog(systemCatalogId());
  }

  @Test
  void getCatalog_engineAbsent_systemId_notFound() {
    when(engineContext.isPresent()).thenReturn(false);
    ResourceId canonicalSystemId = systemCatalogId();
    ResourceId callerScopedId = callerScopedCatalogId(canonicalSystemId);
    when(catalogRepo.getById(callerScopedId)).thenReturn(Optional.empty());

    var req = GetCatalogRequest.newBuilder().setCatalogId(callerScopedId).build();

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class, () -> svc.getCatalog(req).await().indefinitely());
    assertEquals(Status.Code.NOT_FOUND, ex.getStatus().getCode());
    verify(catalogRepo).getById(callerScopedId);
    verify(overlay).catalog(canonicalSystemId);
  }

  @Test
  void listCatalogs_engineAbsent_encodedSystemToken_invalidArgument() {
    when(engineContext.isPresent()).thenReturn(false);
    when(catalogRepo.count("acct")).thenReturn(0);

    var req =
        ListCatalogsRequest.newBuilder()
            .setPage(PageRequest.newBuilder().setPageSize(10).setPageToken(systemPhasePageToken()))
            .build();

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class, () -> svc.listCatalogs(req).await().indefinitely());
    assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
    verify(overlay).catalog(systemCatalogId());
  }

  @Test
  void updateCatalog_engineAbsent_systemLikeId_stillPermissionDenied() {
    when(engineContext.isPresent()).thenReturn(false);
    ResourceId callerScopedId = callerScopedCatalogId(systemCatalogId());

    var req =
        UpdateCatalogRequest.newBuilder()
            .setCatalogId(callerScopedId)
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
  void deleteCatalog_engineAbsent_systemLikeId_stillPermissionDenied() {
    when(engineContext.isPresent()).thenReturn(false);
    ResourceId callerScopedId = callerScopedCatalogId(systemCatalogId());

    var req = DeleteCatalogRequest.newBuilder().setCatalogId(callerScopedId).build();

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class, () -> svc.deleteCatalog(req).await().indefinitely());
    assertEquals(Status.Code.PERMISSION_DENIED, ex.getStatus().getCode());
    verifyNoInteractions(catalogRepo, markerStore, metadataGraph, overlay);
  }

  private static ResourceId systemCatalogId() {
    return SystemNodeRegistry.systemCatalogContainerId("floecat_internal");
  }

  private static ResourceId callerScopedCatalogId(ResourceId canonicalSystemId) {
    return ResourceId.newBuilder()
        .setAccountId("acct")
        .setKind(ResourceKind.RK_CATALOG)
        .setId(canonicalSystemId.getId())
        .build();
  }

  private static CatalogNode systemCatalogNode(ResourceId canonicalSystemId) {
    return new CatalogNode(
        canonicalSystemId,
        0L,
        Instant.EPOCH,
        "floecat_internal",
        Map.of(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Map.of());
  }

  private static String systemPhasePageToken() {
    return servicePageTokenPayload("s");
  }

  private static String servicePageTokenPayload(String payload) {
    return "svc:catalogs:v1:"
        + Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(payload.getBytes(StandardCharsets.UTF_8));
  }
}
