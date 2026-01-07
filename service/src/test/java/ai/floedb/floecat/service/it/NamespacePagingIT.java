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

package ai.floedb.floecat.service.it;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.catalog.rpc.*;
import ai.floedb.floecat.common.rpc.ErrorCode;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.util.PagingTestUtil;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.*;

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NamespacesPagingIT {

  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalogs;

  @GrpcClient("floecat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespaces;

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  private ResourceId catalogId;

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();
    var cat = TestSupport.createCatalog(catalogs, "cat-ns", "ns tests");
    catalogId = cat.getResourceId();

    TestSupport.createNamespace(namespaces, catalogId, "a", List.of(), "root child a");
    TestSupport.createNamespace(namespaces, catalogId, "b", List.of("a"), "level 2");
    TestSupport.createNamespace(namespaces, catalogId, "c", List.of("a", "b"), "level 3");
    TestSupport.createNamespace(namespaces, catalogId, "x", List.of("a"), "level 2");
    TestSupport.createNamespace(namespaces, catalogId, "z", List.of(), "root child z");
    TestSupport.createNamespace(namespaces, catalogId, "y", List.of("z"), "level 2");
    TestSupport.createNamespace(namespaces, catalogId, "n", List.of("m"), "deep only");
  }

  @Test
  void listNamespacesChildrenOnlyPaging() {
    PagingTestUtil.GrpcPager<Namespace> pager =
        (pageSize, token) -> {
          var req =
              ListNamespacesRequest.newBuilder()
                  .setCatalogId(catalogId)
                  .setChildrenOnly(true)
                  .setRecursive(false)
                  .setPage(
                      PageRequest.newBuilder()
                          .setPageSize(pageSize)
                          .setPageToken(token == null ? "" : token))
                  .build();
          var resp = namespaces.listNamespaces(req);
          return new PagingTestUtil.PageChunk<>(
              resp.getNamespacesList(),
              resp.getPage().getNextPageToken(),
              resp.getPage().getTotalSize());
        };

    PagingTestUtil.assertBasicTwoPageFlow(pager, 2);

    var all = collectAllNamespacesAtRootChildrenOnly(100);
    var names = toNames(all);

    var expected = Set.of("a", "m", "z");
    assertEquals(expected, new HashSet<>(names));

    var totalResp =
        namespaces.listNamespaces(
            ListNamespacesRequest.newBuilder()
                .setCatalogId(catalogId)
                .setChildrenOnly(true)
                .setRecursive(false)
                .setPage(PageRequest.newBuilder().setPageSize(1))
                .build());
    assertEquals(3, totalResp.getPage().getTotalSize());
  }

  @Test
  void listNamespaces_recursive_prefix_a() {
    var resp =
        namespaces.listNamespaces(
            ListNamespacesRequest.newBuilder()
                .setCatalogId(catalogId)
                .setRecursive(true)
                .setChildrenOnly(false)
                .setNamePrefix("a")
                .setPage(PageRequest.newBuilder().setPageSize(100))
                .build());

    var names = toQualifiedNames(resp.getNamespacesList());
    assertTrue(names.contains("a"));
    assertTrue(names.contains("a.b"));
    assertTrue(names.contains("a.b.c"));
    assertTrue(names.contains("a.x"));

    long countA = names.stream().filter(n -> n.startsWith("a")).count();
    assertEquals(4, countA);
  }

  @Test
  void listNamespaces_invalid_token() throws Exception {
    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                namespaces.listNamespaces(
                    ListNamespacesRequest.newBuilder()
                        .setCatalogId(catalogId)
                        .setChildrenOnly(true)
                        .setRecursive(false)
                        .setPage(
                            PageRequest.newBuilder().setPageSize(2).setPageToken("bogus-token"))
                        .build()));

    TestSupport.assertGrpcAndMc(
        ex,
        Status.Code.INVALID_ARGUMENT,
        ErrorCode.MC_INVALID_ARGUMENT,
        "Invalid page token: bogus-token");
  }

  private List<Namespace> collectAllNamespacesAtRootChildrenOnly(int pageSize) {
    var all = new ArrayList<Namespace>();
    String token = "";
    for (int i = 0; i < 50; i++) {
      var resp =
          namespaces.listNamespaces(
              ListNamespacesRequest.newBuilder()
                  .setCatalogId(catalogId)
                  .setChildrenOnly(true)
                  .setRecursive(false)
                  .setPage(PageRequest.newBuilder().setPageSize(pageSize).setPageToken(token))
                  .build());
      all.addAll(resp.getNamespacesList());
      var nt = resp.getPage().getNextPageToken();
      if (nt == null || nt.isBlank()) break;
      token = nt;
    }
    return all;
  }

  private static List<String> toNames(List<Namespace> list) {
    return list.stream().map(Namespace::getDisplayName).collect(Collectors.toList());
  }

  private static List<String> toQualifiedNames(List<Namespace> list) {
    return list.stream()
        .map(
            ns -> {
              var parents = new ArrayList<>(ns.getParentsList());
              if (!ns.getDisplayName().isBlank()) {
                parents.add(ns.getDisplayName());
              }
              return String.join(".", parents);
            })
        .collect(Collectors.toList());
  }
}
