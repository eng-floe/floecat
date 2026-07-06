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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.GetViewRequest;
import ai.floedb.floecat.catalog.rpc.ListViewsRequest;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.EngineHint;
import ai.floedb.floecat.metagraph.model.EngineHintKey;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.systemcatalog.util.TestCatalogOverlay;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CatalogSurfaceViewsTest {

  private static final String ACCOUNT_ID = "acct";
  private static final String CORRELATION_ID = "corr";

  private final ResourceId catalogId = id(ResourceKind.RK_CATALOG, "cat");
  private final ResourceId namespaceId = id(ResourceKind.RK_NAMESPACE, "ns");

  private ViewRepository viewRepo;
  private TestCatalogOverlay overlay;
  private CatalogSurfaceViews surface;

  @BeforeEach
  void setup() {
    viewRepo = mock(ViewRepository.class);
    overlay = new TestCatalogOverlay();
    surface = new CatalogSurfaceViews(viewRepo, overlay);

    overlay.addNode(
        new NamespaceNode(
            namespaceId,
            1L,
            Instant.now(),
            catalogId,
            List.of(),
            "public",
            GraphNodeOrigin.USER,
            Map.of(),
            Map.of()));
  }

  @Test
  void listViewsKeepsRepoPhaseBeforeSystemPhase() {
    View userView = view("orders_view");
    when(viewRepo.list(eq(ACCOUNT_ID), eq("cat"), eq("ns"), eq(1), eq(""), any()))
        .thenAnswer(
            invocation -> {
              StringBuilder nextOut = invocation.getArgument(5);
              nextOut.append("repo-next");
              return List.of(userView);
            });
    when(viewRepo.count(ACCOUNT_ID, "cat", "ns")).thenReturn(1);
    overlay.addRelation(namespaceId, viewNode("z_system", GraphNodeOrigin.SYSTEM));
    overlay.addRelation(namespaceId, viewNode("a_system", GraphNodeOrigin.SYSTEM));

    var firstPage =
        surface.listViews(
            ListViewsRequest.newBuilder()
                .setNamespaceId(namespaceId)
                .setPage(PageRequest.newBuilder().setPageSize(1))
                .build(),
            ACCOUNT_ID,
            CORRELATION_ID);

    assertEquals(List.of("orders_view"), names(firstPage.getViewsList()));
    assertEquals("repo-next", firstPage.getPage().getNextPageToken());
    assertEquals(3, firstPage.getPage().getTotalSize());

    when(viewRepo.list(eq(ACCOUNT_ID), eq("cat"), eq("ns"), eq(1), eq("repo-next"), any()))
        .thenAnswer(
            invocation -> {
              StringBuilder nextOut = invocation.getArgument(5);
              nextOut.append("");
              return List.of();
            });

    var bridgePage =
        surface.listViews(
            ListViewsRequest.newBuilder()
                .setNamespaceId(namespaceId)
                .setPage(PageRequest.newBuilder().setPageSize(1).setPageToken("repo-next"))
                .build(),
            ACCOUNT_ID,
            CORRELATION_ID);

    assertEquals(List.of("a_system"), names(bridgePage.getViewsList()));
    assertTrue(bridgePage.getPage().getNextPageToken().startsWith("view:"));
    assertFalse(bridgePage.getPage().getNextPageToken().equals("view:"));
    assertEquals(3, bridgePage.getPage().getTotalSize());

    var systemPage =
        surface.listViews(
            ListViewsRequest.newBuilder()
                .setNamespaceId(namespaceId)
                .setPage(
                    PageRequest.newBuilder()
                        .setPageSize(1)
                        .setPageToken(bridgePage.getPage().getNextPageToken()))
                .build(),
            ACCOUNT_ID,
            CORRELATION_ID);

    assertEquals(List.of("z_system"), names(systemPage.getViewsList()));
    assertTrue(systemPage.getPage().getNextPageToken().startsWith("view:"));
    verify(viewRepo).list(eq(ACCOUNT_ID), eq("cat"), eq("ns"), eq(1), eq("repo-next"), any());
  }

  @Test
  void listViewsRejectsMalformedServicePageToken() {
    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                surface.listViews(
                    ListViewsRequest.newBuilder()
                        .setNamespaceId(namespaceId)
                        .setPage(PageRequest.newBuilder().setPageToken("view:%%%"))
                        .build(),
                    ACCOUNT_ID,
                    CORRELATION_ID));

    assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
  }

  @Test
  void getViewReadsSystemViewFromCatalogSurfaceWithoutRepoLookup() {
    var systemView = viewNode("engine_views", GraphNodeOrigin.SYSTEM);
    overlay.addRelation(namespaceId, systemView);

    var response =
        surface.getView(
            GetViewRequest.newBuilder().setViewId(systemView.id()).build(), CORRELATION_ID);

    assertEquals(systemView.id(), response.getView().getResourceId());
    assertEquals("engine_views", response.getView().getDisplayName());
    verifyNoInteractions(viewRepo);
  }

  private static List<String> names(List<View> views) {
    return views.stream().map(View::getDisplayName).toList();
  }

  private View view(String displayName) {
    return View.newBuilder()
        .setResourceId(id(ResourceKind.RK_VIEW, "view_" + displayName))
        .setCatalogId(catalogId)
        .setNamespaceId(namespaceId)
        .setDisplayName(displayName)
        .build();
  }

  private ViewNode viewNode(String displayName, GraphNodeOrigin origin) {
    return new ViewNode(
        id(ResourceKind.RK_VIEW, "sys_" + displayName),
        1L,
        Instant.now(),
        catalogId,
        namespaceId,
        displayName,
        "select 1",
        "sql",
        List.<SchemaColumn>of(),
        List.of(),
        List.of(),
        origin,
        Map.of(),
        Optional.empty(),
        Map.<Long, Map<EngineHintKey, EngineHint>>of(),
        Map.<EngineHintKey, EngineHint>of());
  }

  private static ResourceId id(ResourceKind kind, String id) {
    return ResourceId.newBuilder().setAccountId(ACCOUNT_ID).setKind(kind).setId(id).build();
  }
}
