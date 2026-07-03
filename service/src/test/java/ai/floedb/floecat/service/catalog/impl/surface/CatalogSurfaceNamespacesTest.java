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
    Namespace userNamespace = namespace("alpha", List.of());
    var systemNamespace =
        namespaceNode(systemNamespaceId("information_schema"), "information_schema", List.of());
    overlay.addNode(systemNamespace);

    when(namespaceRepo.list(eq(ACCOUNT_ID), eq("cat"), eq(List.of()), anyInt(), anyString(), any()))
        .thenAnswer(
            invocation -> {
              int limit = invocation.getArgument(3);
              String cursor = invocation.getArgument(4);
              StringBuilder nextOut = invocation.getArgument(5);
              if (limit == 64 && cursor.isBlank()) {
                nextOut.append("repo-next");
                return List.of(userNamespace);
              }
              if (limit == 1000 && cursor.isBlank()) {
                return List.of(userNamespace);
              }
              return List.of();
            });

    var firstPage =
        surface.listNamespaces(
            ListNamespacesRequest.newBuilder()
                .setCatalogId(catalogId)
                .setPage(PageRequest.newBuilder().setPageSize(1))
                .build(),
            ACCOUNT_ID,
            CORRELATION_ID);

    assertEquals(List.of("alpha"), names(firstPage.getNamespacesList()));
    assertEquals("repo-next", firstPage.getPage().getNextPageToken());
    assertEquals(2, firstPage.getPage().getTotalSize());

    var systemPage =
        surface.listNamespaces(
            ListNamespacesRequest.newBuilder()
                .setCatalogId(catalogId)
                .setPage(PageRequest.newBuilder().setPageSize(1).setPageToken("repo-next"))
                .build(),
            ACCOUNT_ID,
            CORRELATION_ID);

    assertEquals(List.of("information_schema"), names(systemPage.getNamespacesList()));
    assertTrue(systemPage.getPage().getNextPageToken().startsWith("ns:"));
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

    assertEquals(List.of("alpha", "bravo", "charlie"), pageAllNamespaceNames(1));
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

  /** Stubs the repo so all user namespaces come back in a single exhausting batch. */
  private void stubUserNamespaces(List<Namespace> userNamespaces) {
    when(namespaceRepo.list(eq(ACCOUNT_ID), eq("cat"), eq(List.of()), anyInt(), anyString(), any()))
        .thenAnswer(
            invocation -> {
              String cursor = invocation.getArgument(4);
              // Leave the "next" cursor blank: a blank next-token signals repo exhaustion.
              return cursor == null || cursor.isBlank() ? userNamespaces : List.of();
            });
  }

  /** Drains every page (page_size {@code pageSize}) and returns the concatenated display names. */
  private List<String> pageAllNamespaceNames(int pageSize) {
    var collected = new java.util.ArrayList<String>();
    String token = "";
    for (int guard = 0; guard < 100; guard++) {
      var response =
          surface.listNamespaces(
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
