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
package ai.floedb.floecat.service.catalog.impl.surface;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.GetCatalogRequest;
import ai.floedb.floecat.catalog.rpc.ListCatalogsRequest;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.service.context.EngineContextProvider;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
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

class CatalogSurfaceCatalogsTest {

  private CatalogRepository catalogRepo;
  private CatalogOverlay overlay;
  private CatalogSurfaceCatalogs surface;
  private CatalogSurfaceWritePolicy writePolicy;

  @BeforeEach
  void setup() {
    catalogRepo = mock(CatalogRepository.class);
    overlay = mock(CatalogOverlay.class);
    EngineContextProvider engineContext = mock(EngineContextProvider.class);

    when(engineContext.effectiveEngineKind()).thenReturn("floecat_internal");
    when(overlay.catalog(any())).thenReturn(Optional.empty());

    surface = new CatalogSurfaceCatalogs(catalogRepo, overlay, engineContext);
    writePolicy = new CatalogSurfaceWritePolicy(overlay);
  }

  @Test
  void listCatalogsRepoEndEmitsServiceOwnedSystemToken() {
    ResourceId canonicalSystemId = systemCatalogId();
    when(overlay.catalog(canonicalSystemId))
        .thenReturn(Optional.of(systemCatalogNode(canonicalSystemId)));
    when(catalogRepo.count("acct")).thenReturn(1);
    when(catalogRepo.list(eq("acct"), eq(1), eq(""), any(StringBuilder.class)))
        .thenReturn(List.of(Catalog.newBuilder().setDisplayName("examples").build()));

    var req =
        ListCatalogsRequest.newBuilder().setPage(PageRequest.newBuilder().setPageSize(1)).build();

    var res = surface.listCatalogs(req, "acct", "corr");

    assertEquals(systemPhasePageToken(), res.getPage().getNextPageToken());
  }

  @Test
  void getCatalogReadsVisibleSystemCatalogFromCatalogSurface() {
    ResourceId canonicalSystemId = systemCatalogId();
    ResourceId callerScopedId = callerScopedCatalogId(canonicalSystemId);

    when(catalogRepo.getById(callerScopedId)).thenReturn(Optional.empty());
    when(overlay.catalog(canonicalSystemId))
        .thenReturn(Optional.of(systemCatalogNode(canonicalSystemId)));

    var res =
        surface.getCatalog(
            GetCatalogRequest.newBuilder().setCatalogId(callerScopedId).build(), "corr");

    assertEquals("floecat_internal", res.getCatalog().getDisplayName());
    assertEquals(canonicalSystemId.getId(), res.getCatalog().getResourceId().getId());
    verify(catalogRepo).getById(callerScopedId);
    verify(overlay).catalog(canonicalSystemId);
    verifyNoMoreInteractions(catalogRepo);
  }

  @Test
  void requireWritableCatalogRejectsSystemCatalogBeforeRepoLookup() {
    ResourceId systemCatalogId = systemCatalogId();

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () -> writePolicy.requireWritableCatalog(systemCatalogId, "corr"));

    assertEquals(Status.Code.PERMISSION_DENIED, ex.getStatus().getCode());
    verifyNoInteractions(catalogRepo, overlay);
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
