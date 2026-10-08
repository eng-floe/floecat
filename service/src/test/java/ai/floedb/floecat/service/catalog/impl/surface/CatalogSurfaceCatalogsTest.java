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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.GetCatalogRequest;
import ai.floedb.floecat.catalog.rpc.ListCatalogsRequest;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.scanner.spi.CatalogGraphView;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext.CatalogEntry;
import ai.floedb.floecat.scanner.utils.CatalogContext;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.CatalogRepository.CatalogRef;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CatalogSurfaceCatalogsTest {

  private CatalogRepository catalogRepo;
  private CatalogGraphView graphView;
  private CatalogSurfaceCatalogs surface;

  @BeforeEach
  void setup() {
    catalogRepo = mock(CatalogRepository.class);
    graphView = mock(CatalogGraphView.class);
    when(graphView.catalog(any(), any())).thenReturn(Optional.empty());

    surface = new CatalogSurfaceCatalogs(catalogRepo, graphView, CatalogContext.empty());
  }

  @Test
  void listCatalogsRepoEndEmitsServiceOwnedSystemToken() {
    ResourceId canonicalSystemId = systemCatalogId();
    when(graphView.catalog(eq(canonicalSystemId), any()))
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
    when(graphView.catalog(eq(canonicalSystemId), any()))
        .thenReturn(Optional.of(systemCatalogNode(canonicalSystemId)));

    var res =
        surface.getCatalog(
            GetCatalogRequest.newBuilder().setCatalogId(callerScopedId).build(), "corr");

    assertEquals("floecat_internal", res.getCatalog().getDisplayName());
    assertEquals(canonicalSystemId.getId(), res.getCatalog().getResourceId().getId());
    verify(catalogRepo).getById(callerScopedId);
    verify(graphView).catalog(eq(canonicalSystemId), any());
    verifyNoMoreInteractions(catalogRepo);
  }

  @Test
  void getCatalogHiddenSystemCatalogReturnsNotFound() {
    ResourceId canonicalSystemId = systemCatalogId();
    ResourceId callerScopedId = callerScopedCatalogId(canonicalSystemId);

    when(catalogRepo.getById(callerScopedId)).thenReturn(Optional.empty());
    when(graphView.catalog(eq(canonicalSystemId), any())).thenReturn(Optional.empty());

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                surface.getCatalog(
                    GetCatalogRequest.newBuilder().setCatalogId(callerScopedId).build(), "corr"));

    assertEquals(Status.Code.NOT_FOUND, ex.getStatus().getCode());
    verify(catalogRepo).getById(callerScopedId);
    verify(graphView).catalog(eq(canonicalSystemId), any());
  }

  @Test
  void listCatalogsAllowsRawRepoTokensWithLegacyCatalogPrefix() {
    String repoToken = "cat:repo_cursor";
    ResourceId canonicalSystemId = systemCatalogId();
    when(graphView.catalog(eq(canonicalSystemId), any()))
        .thenReturn(Optional.of(systemCatalogNode(canonicalSystemId)));
    when(catalogRepo.count("acct")).thenReturn(1);
    when(catalogRepo.list(eq("acct"), eq(2), eq(repoToken), any(StringBuilder.class)))
        .thenReturn(List.of(Catalog.newBuilder().setDisplayName("examples").build()));

    var req =
        ListCatalogsRequest.newBuilder()
            .setPage(PageRequest.newBuilder().setPageSize(2).setPageToken(repoToken))
            .build();

    var res = surface.listCatalogs(req, "acct", "corr");

    assertEquals(2, res.getCatalogsCount());
    verify(catalogRepo).list(eq("acct"), eq(2), eq(repoToken), any(StringBuilder.class));
  }

  @Test
  void listCatalogsRawCatSystemTokenIsStillARepoToken() {
    String repoToken = "cat:system";
    when(catalogRepo.count("acct")).thenReturn(1);
    when(catalogRepo.list(eq("acct"), eq(2), eq(repoToken), any(StringBuilder.class)))
        .thenReturn(List.of(Catalog.newBuilder().setDisplayName("examples").build()));

    var req =
        ListCatalogsRequest.newBuilder()
            .setPage(PageRequest.newBuilder().setPageSize(2).setPageToken(repoToken))
            .build();

    var res = surface.listCatalogs(req, "acct", "corr");

    assertEquals(1, res.getCatalogsCount());
    assertEquals("examples", res.getCatalogs(0).getDisplayName());
    verify(catalogRepo).list(eq("acct"), eq(2), eq(repoToken), any(StringBuilder.class));
    verify(graphView).catalog(eq(systemCatalogId()), any());
  }

  @Test
  void listCatalogsHidesSystemCatalogWhenGraphViewCannotSeeIt() {
    ResourceId canonicalSystemId = systemCatalogId();
    when(graphView.catalog(eq(canonicalSystemId), any())).thenReturn(Optional.empty());
    when(catalogRepo.count("acct")).thenReturn(1);
    when(catalogRepo.list(eq("acct"), eq(5), eq(""), any(StringBuilder.class)))
        .thenReturn(List.of(Catalog.newBuilder().setDisplayName("examples").build()));

    var req =
        ListCatalogsRequest.newBuilder().setPage(PageRequest.newBuilder().setPageSize(5)).build();

    var res = surface.listCatalogs(req, "acct", "corr");

    assertEquals(1, res.getCatalogsCount());
    assertEquals("examples", res.getCatalogs(0).getDisplayName());
  }

  @Test
  void listCatalogsRejectsSystemPhaseTokenWhenSystemCatalogIsHidden() {
    when(graphView.catalog(eq(systemCatalogId()), any())).thenReturn(Optional.empty());
    when(catalogRepo.count("acct")).thenReturn(0);

    var req =
        ListCatalogsRequest.newBuilder()
            .setPage(PageRequest.newBuilder().setPageSize(10).setPageToken(systemPhasePageToken()))
            .build();

    StatusRuntimeException ex =
        assertThrows(StatusRuntimeException.class, () -> surface.listCatalogs(req, "acct", "corr"));

    assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
    verify(graphView).catalog(eq(systemCatalogId()), any());
  }

  @Test
  void listCatalogEntriesWithoutDescriptionReadsPointersOnly() {
    ResourceId catalogId = userCatalogId("cat-1");
    when(catalogRepo.listRefs("acct")).thenReturn(List.of(new CatalogRef(catalogId, "examples")));

    List<CatalogEntry> entries = surface.listCatalogEntries("acct", false);

    assertEquals(List.of(new CatalogEntry(catalogId, "examples", null)), entries);
    verify(catalogRepo, never()).list(any(), anyInt(), any(), any());
  }

  @Test
  void listCatalogEntriesWithDescriptionPagesCatalogObjectsAndAppendsSystemCatalog() {
    ResourceId canonicalSystemId = systemCatalogId();
    when(graphView.catalog(eq(canonicalSystemId), any()))
        .thenReturn(Optional.of(systemCatalogNode(canonicalSystemId)));
    ResourceId first = userCatalogId("cat-1");
    ResourceId second = userCatalogId("cat-2");
    when(catalogRepo.list(eq("acct"), anyInt(), eq(""), any(StringBuilder.class)))
        .thenAnswer(
            inv -> {
              inv.getArgument(3, StringBuilder.class).append("next");
              return List.of(
                  Catalog.newBuilder()
                      .setResourceId(first)
                      .setDisplayName("examples")
                      .setDescription("primary")
                      .build());
            });
    when(catalogRepo.list(eq("acct"), anyInt(), eq("next"), any(StringBuilder.class)))
        .thenReturn(
            List.of(Catalog.newBuilder().setResourceId(second).setDescription("  ").build()));

    List<CatalogEntry> entries = surface.listCatalogEntries("acct", true);

    assertEquals(3, entries.size());
    assertEquals(new CatalogEntry(first, "examples", "primary"), entries.get(0));
    assertEquals(new CatalogEntry(second, "cat-2", null), entries.get(1));
    assertEquals("floecat_internal", entries.get(2).name());
    verify(catalogRepo, never()).listRefs(any());
  }

  @Test
  void listCatalogEntriesNamesAnUnnamedCatalogByIdOnBothPaths() {
    ResourceId id = userCatalogId("cat-1");
    when(catalogRepo.listRefs("acct")).thenReturn(List.of(new CatalogRef(id, "")));
    when(catalogRepo.list(eq("acct"), anyInt(), eq(""), any(StringBuilder.class)))
        .thenReturn(List.of(Catalog.newBuilder().setResourceId(id).build()));

    assertEquals("cat-1", surface.listCatalogEntries("acct", false).get(0).name());
    assertEquals("cat-1", surface.listCatalogEntries("acct", true).get(0).name());
  }

  private static ResourceId userCatalogId(String id) {
    return ResourceId.newBuilder()
        .setAccountId("acct")
        .setKind(ResourceKind.RK_CATALOG)
        .setId(id)
        .build();
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
        "blob://test/v0",
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
