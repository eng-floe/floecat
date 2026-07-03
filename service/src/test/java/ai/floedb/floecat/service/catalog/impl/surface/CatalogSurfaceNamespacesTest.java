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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.GetNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.ListNamespacesRequest;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.util.TestCatalogOverlay;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CatalogSurfaceNamespacesTest {

  private static final String ACCOUNT_ID = "acct";
  private static final String CORRELATION_ID = "corr";

  private final ResourceId catalogId = id(ResourceKind.RK_CATALOG, "cat");

  private NamespaceRepository namespaceRepo;
  private TestCatalogOverlay overlay;
  private CatalogSurfaceNamespaces surface;

  @BeforeEach
  void setup() {
    namespaceRepo = mock(NamespaceRepository.class);
    overlay = new TestCatalogOverlay();
    surface = new CatalogSurfaceNamespaces(namespaceRepo, overlay);

    overlay.addNode(catalogNode(catalogId, "examples"));
  }

  @Test
  void listNamespacesKeepsRepoPhaseBeforeSystemPhase() {
    stubUserNamespaces(List.of(namespace("alpha", List.of())));
    overlay.addNode(
        namespaceNode(systemNamespaceId("information_schema"), "information_schema", List.of()));

    var firstPage =
        surface.listNamespaces(
            ListNamespacesRequest.newBuilder()
                .setCatalogId(catalogId)
                .setPage(PageRequest.newBuilder().setPageSize(1))
                .build(),
            ACCOUNT_ID,
            CORRELATION_ID);

    assertEquals(List.of("alpha"), names(firstPage.getNamespacesList()));
    // Page filled in the repo phase: the token is a precise repo cursor, not a system-phase token.
    String repoCursor = firstPage.getPage().getNextPageToken();
    assertTrue(!repoCursor.isBlank() && !repoCursor.startsWith("ns:"));
    assertEquals(2, firstPage.getPage().getTotalSize());

    var systemPage =
        surface.listNamespaces(
            ListNamespacesRequest.newBuilder()
                .setCatalogId(catalogId)
                .setPage(PageRequest.newBuilder().setPageSize(1).setPageToken(repoCursor))
                .build(),
            ACCOUNT_ID,
            CORRELATION_ID);

    assertEquals(List.of("information_schema"), names(systemPage.getNamespacesList()));
    // It is the only system namespace and it exactly fills the page, so there is no next page —
    // the system phase must not advertise a continuation whose next page would be empty.
    assertTrue(systemPage.getPage().getNextPageToken().isEmpty());
  }

  @Test
  void listNamespacesSystemExactlyFillsPageEmitsNoToken() {
    // Regression: a page that exactly consumes the last system namespace must not advertise an
    // ns: continuation, whose next page would come back empty.
    stubUserNamespaces(List.of());
    overlay.addNode(
        namespaceNode(systemNamespaceId("information_schema"), "information_schema", List.of()));

    var page =
        surface.listNamespaces(
            ListNamespacesRequest.newBuilder()
                .setCatalogId(catalogId)
                .setPage(PageRequest.newBuilder().setPageSize(1))
                .build(),
            ACCOUNT_ID,
            CORRELATION_ID);

    assertEquals(List.of("information_schema"), names(page.getNamespacesList()));
    assertTrue(
        page.getPage().getNextPageToken().isEmpty(),
        "exact-fill on the last system namespace must not emit a continuation token");
  }

  @Test
  void listNamespacesDoesNotSkipMatchesWhenPageFillsMidBatch() {
    // Regression: the repo scan over-fetches (batch >= 64) and post-filters, so with more matching
    // children than one batch the continuation must resume after the last *emitted* row, not after
    // the scanned batch. A batch-end cursor silently skips every match scanned past the page fill.
    var many = new java.util.ArrayList<Namespace>();
    var expected = new java.util.ArrayList<String>();
    for (int i = 0; i < 70; i++) {
      String name = String.format("ns%02d", i);
      many.add(namespace(name, List.of()));
      expected.add(name);
    }
    stubUserNamespaces(many);

    assertEquals(expected, pageAllNamespaceNames(surface, 3));
  }

  @Test
  void listNamespacesPagesThroughAllUserNamespacesWhenRepoExhaustsInOneBatch() {
    // Regression: a single repo batch that exhausts the cursor may contain more matches than fit
    // on one page. Later pages must keep returning the remaining user namespaces rather than
    // jumping straight to the (empty) system phase.
    stubUserNamespaces(
        List.of(
            namespace("alpha", List.of()),
            namespace("bravo", List.of()),
            namespace("charlie", List.of())));

    assertEquals(List.of("alpha", "bravo", "charlie"), pageAllNamespaceNames(surface, 1));
  }

  @Test
  void listNamespacesPagesCompletelyOverARealRepositoryAndStore() {
    // End-to-end over the real chain: pager -> NamespaceRepository -> InMemoryPointerStore,
    // including listTokenAfter -> pageTokenAfterKey cursor synthesis. Catches any drift between
    // the pointer keys the repository writes and the keys the pager resumes from — mocked-repo
    // tests re-implement the cursor contract and cannot.
    var pointers = new ai.floedb.floecat.storage.memory.InMemoryPointerStore();
    var blobs = new ai.floedb.floecat.storage.memory.InMemoryBlobStore();
    var realRepo = new NamespaceRepository(pointers, blobs);
    var realSurface = new CatalogSurfaceNamespaces(realRepo, overlay);

    var expected = new java.util.ArrayList<String>();
    for (int i = 0; i < 150; i++) {
      String name = String.format("ns%03d", i);
      expected.add(name);
      realRepo.create(
          Namespace.newBuilder()
              .setResourceId(id(ResourceKind.RK_NAMESPACE, "id_" + name))
              .setCatalogId(catalogId)
              .setDisplayName(name)
              .build());
    }

    assertEquals(expected, pageAllNamespaceNames(realSurface, 7));
  }

  @Test
  void listNamespacesDoesNotDropSystemNamespacesSortingBeforeLastUserNamespace() {
    // Regression: the system phase must resume by a system relative name, not by the last user
    // relative name. "alpha" sorts before the last user namespace "orders" and must still appear.
    stubUserNamespaces(List.of(namespace("orders", List.of())));
    overlay.addNode(namespaceNode(systemNamespaceId("alpha"), "alpha", List.of()));
    overlay.addNode(
        namespaceNode(systemNamespaceId("information_schema"), "information_schema", List.of()));

    assertEquals(
        List.of("orders", "alpha", "information_schema"), pageAllNamespaceNames(surface, 1));
  }

  @Test
  void listNamespacesRejectsMalformedServicePageToken() {
    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                surface.listNamespaces(
                    ListNamespacesRequest.newBuilder()
                        .setCatalogId(catalogId)
                        .setPage(PageRequest.newBuilder().setPageToken("ns:%%%"))
                        .build(),
                    ACCOUNT_ID,
                    CORRELATION_ID));

    assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
  }

  @Test
  void getNamespaceReadsSystemNamespaceFromCatalogSurface() {
    ResourceId namespaceId = systemNamespaceId("information_schema");
    overlay.addNode(namespaceNode(namespaceId, "information_schema", List.of()));
    when(namespaceRepo.getById(namespaceId)).thenReturn(Optional.empty());

    var res =
        surface.getNamespace(
            GetNamespaceRequest.newBuilder().setNamespaceId(namespaceId).build(), CORRELATION_ID);

    assertEquals("information_schema", res.getNamespace().getDisplayName());
    assertEquals(namespaceId, res.getNamespace().getResourceId());
    verify(namespaceRepo).getById(namespaceId);
    verifyNoMoreInteractions(namespaceRepo);
  }

  private static List<String> names(List<Namespace> namespaces) {
    return namespaces.stream().map(Namespace::getDisplayName).toList();
  }

  /**
   * Stubs the repo mock as a faithful pager backend: rows are served in name order, {@code list}
   * returns up to {@code limit} rows strictly after the cursor name (blank = start) and sets the
   * next cursor to the last returned name when more remain, and {@code listTokenAfter} returns the
   * row's name — mirroring the real store's "resume after this key" semantics.
   */
  private void stubUserNamespaces(List<Namespace> userNamespaces) {
    var sorted = new java.util.ArrayList<>(userNamespaces);
    sorted.sort(java.util.Comparator.comparing(Namespace::getDisplayName));
    when(namespaceRepo.list(eq(ACCOUNT_ID), eq("cat"), eq(List.of()), anyInt(), anyString(), any()))
        .thenAnswer(
            invocation -> {
              int limit = invocation.getArgument(3);
              String cursor = invocation.getArgument(4);
              StringBuilder nextOut = invocation.getArgument(5);
              var remaining =
                  sorted.stream()
                      .filter(
                          ns ->
                              cursor == null
                                  || cursor.isBlank()
                                  || ns.getDisplayName().compareTo(cursor) > 0)
                      .toList();
              var page = remaining.subList(0, Math.min(limit, remaining.size()));
              if (!page.isEmpty() && page.size() < remaining.size()) {
                nextOut.append(page.get(page.size() - 1).getDisplayName());
              }
              return page;
            });
    when(namespaceRepo.listTokenAfter(eq(ACCOUNT_ID), eq("cat"), any()))
        .thenAnswer(
            invocation -> {
              List<String> fullPath = invocation.getArgument(2);
              return fullPath.get(fullPath.size() - 1);
            });
  }

  /** Drains every page (page_size {@code pageSize}) and returns the concatenated display names. */
  private List<String> pageAllNamespaceNames(CatalogSurfaceNamespaces target, int pageSize) {
    var collected = new java.util.ArrayList<String>();
    String token = "";
    for (int guard = 0; guard < 100; guard++) {
      var response =
          target.listNamespaces(
              ListNamespacesRequest.newBuilder()
                  .setCatalogId(catalogId)
                  .setPage(PageRequest.newBuilder().setPageSize(pageSize).setPageToken(token))
                  .build(),
              ACCOUNT_ID,
              CORRELATION_ID);
      collected.addAll(names(response.getNamespacesList()));
      token = response.getPage().getNextPageToken();
      if (token.isBlank()) {
        return collected;
      }
    }
    throw new AssertionError("pagination did not terminate; collected so far: " + collected);
  }

  private Namespace namespace(String name, List<String> path) {
    return Namespace.newBuilder()
        .setResourceId(id(ResourceKind.RK_NAMESPACE, name))
        .setCatalogId(catalogId)
        .setDisplayName(name)
        .addAllParents(path)
        .build();
  }

  private NamespaceNode namespaceNode(ResourceId namespaceId, String name, List<String> path) {
    return new NamespaceNode(
        namespaceId,
        1L,
        Instant.EPOCH,
        catalogId,
        path,
        name,
        GraphNodeOrigin.SYSTEM,
        Map.of(),
        Map.of());
  }

  private static CatalogNode catalogNode(ResourceId catalogId, String displayName) {
    return new CatalogNode(
        catalogId,
        0L,
        Instant.EPOCH,
        displayName,
        Map.of(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Map.of());
  }

  private static ResourceId id(ResourceKind kind, String id) {
    return ResourceId.newBuilder().setAccountId(ACCOUNT_ID).setKind(kind).setId(id).build();
  }

  private static ResourceId systemNamespaceId(String name) {
    return SystemNodeRegistry.resourceId("engine", ResourceKind.RK_NAMESPACE, name);
  }
}
