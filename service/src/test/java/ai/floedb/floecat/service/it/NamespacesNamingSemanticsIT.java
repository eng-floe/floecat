package ai.floedb.floecat.service.it;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.catalog.rpc.*;
import ai.floedb.floecat.common.rpc.ErrorCode;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.util.PagingTestUtil;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import ai.floedb.floecat.storage.BlobStore;
import ai.floedb.floecat.storage.PointerStore;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NamespacesNamingSemanticsIT {

  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalogs;

  @GrpcClient("floecat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespaces;

  @GrpcClient("floecat")
  DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;
  @Inject PointerStore ptr;
  @Inject BlobStore blobs;

  private ResourceId catalogId;
  private String account;

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();

    var cat = TestSupport.createCatalog(catalogs, "cat-names", "naming semantics");
    catalogId = cat.getResourceId();
    account = catalogId.getAccountId();

    TestSupport.createNamespace(namespaces, catalogId, "db.v1", List.of("env.prod"), "lvl 1");
    TestSupport.createNamespace(
        namespaces, catalogId, "schema.test", List.of("env.prod", "db.v1"), "lvl 2");
    TestSupport.createNamespace(namespaces, catalogId, "a.b", List.of("env.prod"), "sib with dot");
    TestSupport.createNamespace(namespaces, catalogId, "foo.bar.baz", List.of(), "dot-leaf");
  }

  private static List<String> toNames(List<Namespace> list) {
    return list.stream().map(Namespace::getDisplayName).collect(Collectors.toList());
  }

  private static List<String> toQualifiedNames(List<Namespace> list) {
    return list.stream()
        .map(
            ns -> {
              var parts = new ArrayList<>(ns.getParentsList());
              if (!ns.getDisplayName().isBlank()) parts.add(ns.getDisplayName());
              return String.join(".", parts);
            })
        .collect(Collectors.toList());
  }

  private List<Namespace> listChildrenAtRoot(int pageSize) {
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

  private ResourceId resolve(String catalog, List<String> parents, String leaf) {
    var ref = NameRef.newBuilder().setCatalog(catalog).addAllPath(parents).setName(leaf).build();
    return directory
        .resolveNamespace(ResolveNamespaceRequest.newBuilder().setRef(ref).build())
        .getResourceId();
  }

  static record RoundTripCase(String display, List<String> parents, String note) {}

  static Stream<RoundTripCase> roundTripCases() {
    return Stream.of(
        new RoundTripCase("foo.bar", List.of(), "literal dotted leaf at root"),
        new RoundTripCase("Orders 2025", List.of("Prod West"), "spaces in leaf, plain parent"),
        new RoundTripCase(
            "db.v1", List.of("env.prod"), "both parent & leaf include dots literally"),
        new RoundTripCase("a.b", List.of("env.prod"), "sibling with dot under literal parent"),
        new RoundTripCase("back\\slash", List.of("prod\\west"), "backslashes preserved"),
        new RoundTripCase("weird\\.name", List.of("seg\\.ment"), "backslash + dot preserved"));
  }

  @ParameterizedTest(name = "[{index}] {2}")
  @MethodSource("roundTripCases")
  void names_roundTripResolve(RoundTripCase c) {
    var ns = TestSupport.createNamespace(namespaces, catalogId, c.display, c.parents, c.note);

    assertEquals(c.parents, ns.getParentsList(), "parents must round-trip");
    assertEquals(c.display, ns.getDisplayName(), "display must round-trip");

    var ns2 = TestSupport.createNamespace(namespaces, catalogId, c.display, c.parents, "retry");
    assertEquals(ns.getResourceId().getId(), ns2.getResourceId().getId(), "RID must be stable");

    var rid = resolve("cat-names", c.parents, c.display);
    assertEquals(ns.getResourceId().getId(), rid.getId(), "resolveNamespace must match RID");
  }

  @Test
  void literalHierarchyDoNotCollide() {
    var nsA =
        TestSupport.createNamespace(
            namespaces, catalogId, "schema", List.of("env.prod", "db"), "A");
    var nsB =
        TestSupport.createNamespace(
            namespaces, catalogId, "schema", List.of("env", "prod", "db"), "B");

    assertNotEquals(
        nsA.getResourceId().getId(),
        nsB.getResourceId().getId(),
        "RIDs must differ for literal-dot vs hierarchical path");
  }

  @Test
  void dottedDisplayIsLiteralWhenParentsEmpty() {
    var cat = TestSupport.createCatalog(catalogs, "cat-lit", "");
    var ns =
        TestSupport.createNamespace(namespaces, cat.getResourceId(), "foo.bar", List.of(), "lit");
    assertEquals(List.of(), ns.getParentsList());
    assertEquals("foo.bar", ns.getDisplayName());
  }

  @Test
  void prefixEnvDotMatchesLiteralSegmentTree() {
    var resp =
        namespaces.listNamespaces(
            ListNamespacesRequest.newBuilder()
                .setCatalogId(catalogId)
                .setRecursive(true)
                .setChildrenOnly(false)
                .setNamePrefix("env.")
                .setPage(PageRequest.newBuilder().setPageSize(100))
                .build());

    var q = toQualifiedNames(resp.getNamespacesList());
    assertTrue(q.contains("env.prod"), "root literal-dot parent present");
    assertTrue(q.contains("env.prod.db.v1"), "literal child present");
    assertTrue(q.contains("env.prod.db.v1.schema.test"), "deep leaf present");
    assertTrue(q.contains("env.prod.a.b"), "sibling leaf present");
  }

  @Test
  void prefixSelectivityAndNonMatches() {
    var envp =
        namespaces.listNamespaces(
            ListNamespacesRequest.newBuilder()
                .setCatalogId(catalogId)
                .setRecursive(true)
                .setChildrenOnly(false)
                .setNamePrefix("env.p")
                .setPage(PageRequest.newBuilder().setPageSize(100))
                .build());
    assertTrue(
        toQualifiedNames(envp.getNamespacesList()).stream().allMatch(s -> s.startsWith("env.p")),
        "all results must start with env.p");

    var ef =
        namespaces.listNamespaces(
            ListNamespacesRequest.newBuilder()
                .setCatalogId(catalogId)
                .setRecursive(true)
                .setChildrenOnly(false)
                .setNamePrefix("e.f")
                .setPage(PageRequest.newBuilder().setPageSize(100))
                .build());
    assertTrue(ef.getNamespacesList().isEmpty(), "e.f should not match env.prod.*");
  }

  @Test
  void rootChildrenIncludeLiteralDotSegmentAndPagingStable() {
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

    PagingTestUtil.assertBasicTwoPageFlow(pager, 1);

    var names = new HashSet<>(toNames(listChildrenAtRoot(100)));
    assertTrue(names.contains("env.prod"), "must list literal-dot parent at root");
    assertTrue(names.contains("foo.bar.baz"), "must list literal dotted leaf at root");
  }

  @Test
  void invalidTokenYieldsInvalidArgument() throws Exception {
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
                            PageRequest.newBuilder().setPageSize(1).setPageToken("bogus-token"))
                        .build()));
    TestSupport.assertGrpcAndMc(
        ex,
        Status.Code.INVALID_ARGUMENT,
        ErrorCode.MC_INVALID_ARGUMENT,
        "Invalid page token: bogus-token");
  }

  @Test
  void byPathPointersExistForLiteralDotPaths() {
    var full = List.of("env.prod", "db.v1", "schema.test");
    String byPathKey = Keys.namespacePointerByPath(account, catalogId.getId(), full);
    assertTrue(ptr.get(byPathKey).isPresent(), "namespace by-path pointer missing for dotted path");

    var rid = resolve("cat-names", full.subList(0, full.size() - 1), full.get(full.size() - 1));
    assertNotNull(rid.getId());
  }

  static Stream<List<String>> badParents() {
    return Stream.of(List.of(""), List.of("  "), List.of("ok", ""), List.of(" ok ", "   "));
  }

  @Test
  void blankDisplayNameRejected() {
    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () -> TestSupport.createNamespace(namespaces, catalogId, "  ", List.of("a"), "bad"));
    assertEquals(Status.Code.INVALID_ARGUMENT, Status.fromThrowable(ex).getCode());
  }

  @ParameterizedTest
  @MethodSource("badParents")
  void badParentSegmentsRejected(List<String> parents) {
    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () -> TestSupport.createNamespace(namespaces, catalogId, "ok", parents, "bad"));
    assertEquals(Status.Code.INVALID_ARGUMENT, Status.fromThrowable(ex).getCode());
  }

  @Test
  void namesWithSpacesRoundTripIempotent() {
    var cat = TestSupport.createCatalog(catalogs, "My Catalog", "");
    var ns1 =
        TestSupport.createNamespace(
            namespaces, cat.getResourceId(), "Orders 2025", List.of("Prod West"), "spaced");

    var ns2 =
        TestSupport.createNamespace(
            namespaces, cat.getResourceId(), "Orders 2025", List.of("Prod West"), "retry");

    assertEquals(ns1.getResourceId().getId(), ns2.getResourceId().getId());
    assertEquals(List.of("Prod West"), ns1.getParentsList());
    assertEquals("Orders 2025", ns1.getDisplayName());
  }
}
