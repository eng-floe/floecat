package ai.floedb.metacat.service.it;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.*;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.PageRequest;
import ai.floedb.metacat.service.bootstrap.impl.SeedRunner;
import ai.floedb.metacat.service.util.TestDataResetter;
import ai.floedb.metacat.service.util.TestSupport;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.List;
import org.junit.jupiter.api.*;

@QuarkusTest
class DirectoryIT {
  @GrpcClient("directory")
  DirectoryGrpc.DirectoryBlockingStub directory;

  @GrpcClient("catalog-service")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  @GrpcClient("namespace-service")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;

  @GrpcClient("table-service")
  TableServiceGrpc.TableServiceBlockingStub table;

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();
  }

  @Test
  void resolveAndLookupCatalog() {
    var cat = TestSupport.createCatalog(catalog, "resolveAndLookupCatalog", "");

    var ref = NameRef.newBuilder().setCatalog("resolveAndLookupCatalog").build();
    var r = directory.resolveCatalog(ResolveCatalogRequest.newBuilder().setRef(ref).build());
    assertEquals(cat.getResourceId().getTenantId(), r.getResourceId().getTenantId());

    var l =
        directory.lookupCatalog(
            LookupCatalogRequest.newBuilder().setResourceId(r.getResourceId()).build());
    assertTrue(
        l.getDisplayName().equals("resolveAndLookupCatalog") || l.getDisplayName().isEmpty());
  }

  @Test
  void resolveAndLookupNamespace() {
    var cat = TestSupport.createCatalog(catalog, "resolveAndLookupNamespace", "");

    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "2025", List.of("staging"), "core ns");

    var ref =
        NameRef.newBuilder()
            .setCatalog(cat.getDisplayName())
            .addPath("staging")
            .setName("2025")
            .build();

    directory.resolveNamespace(ResolveNamespaceRequest.newBuilder().setRef(ref).build());

    var lookup =
        directory.lookupNamespace(
            LookupNamespaceRequest.newBuilder().setResourceId(ns.getResourceId()).build());

    assertEquals(cat.getDisplayName(), lookup.getRef().getCatalog());
    assertEquals(List.of("staging"), lookup.getRef().getPathList());
    assertEquals("2025", lookup.getRef().getName());
  }

  @Test
  void resolveAndLookupTable() {
    var cat = TestSupport.createCatalog(catalog, "resolveAndLookupTable", "");

    var ns = TestSupport.createNamespace(namespace, cat.getResourceId(), "core", null, "core ns");
    TestSupport.createTable(
        table, cat.getResourceId(), ns.getResourceId(), "orders", "s3://barf", "{}", "none");

    var nameRef =
        NameRef.newBuilder()
            .setCatalog(cat.getDisplayName())
            .addPath("core")
            .setName("orders")
            .build();

    var resolved = directory.resolveTable(ResolveTableRequest.newBuilder().setRef(nameRef).build());

    var lookup =
        directory.lookupTable(
            LookupTableRequest.newBuilder().setResourceId(resolved.getResourceId()).build());

    assertEquals(cat.getDisplayName(), lookup.getName().getCatalog());
    assertEquals(List.of("core"), lookup.getName().getPathList());
    assertEquals("orders", lookup.getName().getName());
  }

  @Test
  void resolveTableNotFound() {
    var missing =
        NameRef.newBuilder().setCatalog("sales").addPath("core").setName("does_not_exist").build();

    var ex =
        assertThrows(
            io.grpc.StatusRuntimeException.class,
            () -> directory.resolveTable(ResolveTableRequest.newBuilder().setRef(missing).build()));

    assertEquals(io.grpc.Status.NOT_FOUND.getCode(), ex.getStatus().getCode());
  }

  @Test
  void resolveFullyQualifiedTables() {
    var cat =
        TestSupport.createCatalog(
            catalog, "resolveFQTables_prefix_salesCore_returnsOrdersAndLineitem", "");

    var ns = TestSupport.createNamespace(namespace, cat.getResourceId(), "core", null, "core ns");
    TestSupport.createTable(
        table, cat.getResourceId(), ns.getResourceId(), "orders", "s3://barf", "{}", "none");
    TestSupport.createTable(
        table, cat.getResourceId(), ns.getResourceId(), "lineitem", "s3://barf", "{}", "none");

    var prefix = NameRef.newBuilder().setCatalog(cat.getDisplayName()).addPath("core").build();

    var resp =
        directory.resolveFQTables(ResolveFQTablesRequest.newBuilder().setPrefix(prefix).build());

    assertTrue(resp.getTablesCount() == 2);
    var names = resp.getTablesList().stream().map(e -> e.getName().getName()).toList();
    assertTrue(names.contains("orders"));
    assertTrue(names.contains("lineitem"));

    for (var e : resp.getTablesList()) {
      assertEquals(cat.getDisplayName(), e.getName().getCatalog());
      assertEquals(List.of("core"), e.getName().getPathList());
      assertFalse(e.getResourceId().getId().isEmpty());
    }
  }

  @Test
  void resolveFullyQualifiedTablesNestedNamespace() {
    var cat =
        TestSupport.createCatalog(
            catalog, "resolveFQTables_prefix_salesStaging2025_returnsTwo", "");

    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "2025", List.of("staging"), "core ns");
    TestSupport.createTable(
        table, cat.getResourceId(), ns.getResourceId(), "orders", "s3://barf", "{}", "none");
    TestSupport.createTable(
        table, cat.getResourceId(), ns.getResourceId(), "lineitem", "s3://barf", "{}", "none");

    var prefix =
        NameRef.newBuilder()
            .setCatalog(cat.getDisplayName())
            .addPath("staging")
            .addPath("2025")
            .build();

    var resp =
        directory.resolveFQTables(ResolveFQTablesRequest.newBuilder().setPrefix(prefix).build());

    assertTrue(resp.getTablesCount() == 2);
    for (var e : resp.getTablesList()) {
      assertEquals(cat.getDisplayName(), e.getName().getCatalog());
      assertEquals(List.of("staging", "2025"), e.getName().getPathList());
      assertFalse(e.getName().getName().isEmpty());
      assertFalse(e.getResourceId().getId().isEmpty());
    }
  }

  @Test
  void renameTableReflectedInDirectoryService() {
    var cat = TestSupport.createCatalog(catalog, "barf1", "barf cat");

    var ns = TestSupport.createNamespace(namespace, cat.getResourceId(), "core", null, "core ns");
    TestSupport.createTable(
        table, cat.getResourceId(), ns.getResourceId(), "t0", "s3://barf", "{}", "none");
    var path = List.of("core");

    var oldRef =
        NameRef.newBuilder()
            .setCatalog(cat.getDisplayName())
            .addAllPath(path)
            .setName("t0")
            .build();
    var id =
        directory
            .resolveTable(ResolveTableRequest.newBuilder().setRef(oldRef).build())
            .getResourceId();

    TestSupport.renameTable(table, id, "t1");

    assertThrows(
        io.grpc.StatusRuntimeException.class,
        () -> directory.resolveTable(ResolveTableRequest.newBuilder().setRef(oldRef).build()));

    var newRef =
        NameRef.newBuilder()
            .setCatalog(cat.getDisplayName())
            .addAllPath(path)
            .setName("t1")
            .build();
    var resolved = directory.resolveTable(ResolveTableRequest.newBuilder().setRef(newRef).build());
    var looked =
        directory.lookupTable(
            LookupTableRequest.newBuilder().setResourceId(resolved.getResourceId()).build());
    assertEquals("t1", looked.getName().getName());
  }

  @Test
  void renameNamespaceReflectedInDirectoryService() {
    var cat = TestSupport.createCatalog(catalog, "barf2", "barf cat");

    var ns =
        TestSupport.createNamespace(namespace, cat.getResourceId(), "a", List.of("p"), "core ns");
    var id = ns.getResourceId();
    var oldRef =
        NameRef.newBuilder().setCatalog(cat.getDisplayName()).addPath("p").addPath("a").build();

    namespace
        .updateNamespace(
            UpdateNamespaceRequest.newBuilder().setNamespaceId(id).setDisplayName("b").build())
        .getNamespace();

    assertThrows(
        StatusRuntimeException.class,
        () ->
            directory.resolveNamespace(
                ResolveNamespaceRequest.newBuilder().setRef(oldRef).build()));

    var newRef =
        NameRef.newBuilder().setCatalog(cat.getDisplayName()).addPath("p").addPath("b").build();
    var resolved =
        directory.resolveNamespace(ResolveNamespaceRequest.newBuilder().setRef(newRef).build());

    var looked =
        directory.lookupNamespace(
            LookupNamespaceRequest.newBuilder().setResourceId(resolved.getResourceId()).build());
    assertEquals(List.of("p"), looked.getRef().getPathList());
    assertEquals("b", looked.getRef().getName());
  }

  @Test
  void resolveFullyQualifiedTablesPaging() {
    var cat =
        TestSupport.createCatalog(catalog, "resolveFQTables_list_selector_paging_and_errors", "");

    var ns = TestSupport.createNamespace(namespace, cat.getResourceId(), "core", null, "core ns");
    TestSupport.createTable(
        table, cat.getResourceId(), ns.getResourceId(), "orders", "s3://barf", "{}", "none");
    TestSupport.createTable(
        table, cat.getResourceId(), ns.getResourceId(), "lineitem", "s3://barf", "{}", "none");

    var names =
        List.of(
            NameRef.newBuilder()
                .setCatalog(cat.getDisplayName())
                .addPath("core")
                .setName("orders")
                .build(),
            NameRef.newBuilder()
                .setCatalog(cat.getDisplayName())
                .addPath("core")
                .setName("lineitem")
                .build());
    var req =
        ResolveFQTablesRequest.newBuilder()
            .setList(NameList.newBuilder().addAllNames(names))
            .build();

    var page1 =
        directory.resolveFQTables(
            ResolveFQTablesRequest.newBuilder(req)
                .setPage(PageRequest.newBuilder().setPageSize(1))
                .build());
    assertEquals(1, page1.getTablesCount());
    var token = page1.getPage().getNextPageToken();

    var page2 =
        directory.resolveFQTables(
            ResolveFQTablesRequest.newBuilder(req)
                .setPage(PageRequest.newBuilder().setPageToken(token).setPageSize(1))
                .build());
    assertEquals(1, page2.getTablesCount());

    assertThrows(
        StatusRuntimeException.class,
        () ->
            directory.resolveFQTables(
                ResolveFQTablesRequest.newBuilder(req)
                    .setPage(PageRequest.newBuilder().setPageToken("not-an-int"))
                    .build()));
  }

  @Test
  void resolveAndLookupUnicodeAndSpaces() {
    var cat = TestSupport.createCatalog(catalog, "barf3", "barf cat");

    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "2025", List.of("staging"), "2025 ns");
    TestSupport.createTable(
        table,
        cat.getResourceId(),
        ns.getResourceId(),
        "staging events 🧪",
        "s3://barf",
        "{}",
        "none");

    var nameRef =
        NameRef.newBuilder()
            .setCatalog("barf3")
            .addPath("staging")
            .addPath("2025")
            .setName("staging events 🧪")
            .build();

    var resolved = directory.resolveTable(ResolveTableRequest.newBuilder().setRef(nameRef).build());
    var lookup =
        directory.lookupTable(
            LookupTableRequest.newBuilder().setResourceId(resolved.getResourceId()).build());
    assertEquals(List.of("staging", "2025"), lookup.getName().getPathList());
    assertEquals("staging events 🧪", lookup.getName().getName());
  }

  @Test
  void lookupUnknownReturnsEmpty() {
    var bogus =
        ai.floedb.metacat.common.rpc.ResourceId.newBuilder()
            .setTenantId(TestSupport.DEFAULT_SEED_TENANT)
            .setId("nope")
            .build();

    var lcat =
        directory.lookupCatalog(LookupCatalogRequest.newBuilder().setResourceId(bogus).build());
    assertTrue(lcat.getDisplayName().isEmpty());

    var lns =
        directory.lookupNamespace(LookupNamespaceRequest.newBuilder().setResourceId(bogus).build());
    assertFalse(lns.hasRef());

    var ltbl = directory.lookupTable(LookupTableRequest.newBuilder().setResourceId(bogus).build());
    assertFalse(ltbl.hasName());
  }

  @Test
  void fullyQualifiedTableLookupPreservesCase() {
    var bad =
        NameRef.newBuilder().setCatalog("Sales").addPath("core/extra").setName("orders").build();
    assertThrows(
        StatusRuntimeException.class,
        () -> directory.resolveTable(ResolveTableRequest.newBuilder().setRef(bad).build()));
  }
}
