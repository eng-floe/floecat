package ai.floedb.metacat.service.it;

import java.util.List;

import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.DirectoryGrpc;
import ai.floedb.metacat.catalog.rpc.LookupCatalogRequest;
import ai.floedb.metacat.catalog.rpc.LookupNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.LookupTableRequest;
import ai.floedb.metacat.catalog.rpc.NameList;
import ai.floedb.metacat.catalog.rpc.RenameNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.ResolveCatalogRequest;
import ai.floedb.metacat.catalog.rpc.ResolveFQTablesRequest;
import ai.floedb.metacat.catalog.rpc.ResolveNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.ResolveTableRequest;
import ai.floedb.metacat.catalog.rpc.ResourceMutationGrpc;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.PageRequest;

@QuarkusTest
class DirectoryIT {
  @GrpcClient("directory")
  DirectoryGrpc.DirectoryBlockingStub directory;

  @GrpcClient("resource-mutation")
  ResourceMutationGrpc.ResourceMutationBlockingStub mutation;

  @Test
  void resolveAndLookupCatalog() {
    var cat = TestSupport.createCatalog(mutation, "resolveAndLookupCatalog", "");
    TestSupport.seedTenantId(directory,  cat.getDisplayName());

    var ref = NameRef.newBuilder().setCatalog("resolveAndLookupCatalog").build();
    var r = directory.resolveCatalog(ResolveCatalogRequest.newBuilder().setRef(ref).build());
    assertEquals("t-0001", r.getResourceId().getTenantId());

    var l = directory.lookupCatalog(LookupCatalogRequest.newBuilder()
        .setResourceId(r.getResourceId()).build());
    assertTrue(l.getDisplayName().equals(
        "resolveAndLookupCatalog") || l.getDisplayName().isEmpty());
  }

  @Test
  void resolveAndLookupNamespace() {
    var cat = TestSupport.createCatalog(mutation, "resolveAndLookupNamespace", "");
    TestSupport.seedTenantId(directory,  cat.getDisplayName());

    var ns = TestSupport.createNamespace(mutation, cat.getResourceId(),
        "2025", List.of("staging"), "core ns");

    var ref = NameRef.newBuilder()
        .setCatalog(cat.getDisplayName())
        .addPath("staging")
        .setName("2025")
        .build();

    directory.resolveNamespace(
        ResolveNamespaceRequest.newBuilder().setRef(ref).build());

    var lookup = directory.lookupNamespace(
        LookupNamespaceRequest.newBuilder().setResourceId(ns.getResourceId()).build());

    assertEquals(cat.getDisplayName(), lookup.getRef().getCatalog());
    assertEquals(List.of("staging"), lookup.getRef().getPathList());
    assertEquals("2025", lookup.getRef().getName());
  }

  @Test
  void resolveAndLookupTable() {
    var cat = TestSupport.createCatalog(mutation, "resolveAndLookupTable", "");
    TestSupport.seedTenantId(directory,  cat.getDisplayName());

    var ns = TestSupport.createNamespace(mutation, cat.getResourceId(),
        "core", null, "core ns");
    TestSupport.createTable(mutation, cat.getResourceId(),
        ns.getResourceId(), "orders", "s3://barf", "{}", "none");

    var nameRef = NameRef.newBuilder()
        .setCatalog(cat.getDisplayName())
        .addPath("core")
        .setName("orders")
        .build();

    var resolved = directory.resolveTable(
        ResolveTableRequest.newBuilder().setRef(nameRef).build());

    var lookup = directory.lookupTable(
        LookupTableRequest.newBuilder().setResourceId(resolved.getResourceId()).build());

    assertEquals(cat.getDisplayName(), lookup.getName().getCatalog());
    assertEquals(List.of("core"), lookup.getName().getPathList());
    assertEquals("orders", lookup.getName().getName());
  }

  @Test
  void resolveTable_notFound_yieldsNotFound() {
    var missing = NameRef.newBuilder()
        .setCatalog("sales")
        .addPath("core")
        .setName("does_not_exist")
        .build();

    var ex = assertThrows(io.grpc.StatusRuntimeException.class, () ->
        directory.resolveTable(ResolveTableRequest.newBuilder().setRef(missing).build()));

    assertEquals(io.grpc.Status.NOT_FOUND.getCode(), ex.getStatus().getCode());
  }

  @Test
  void resolveFullyQualifiedTables_prefix_salesCore_returnsOrdersAndLineitem() {
    var cat = TestSupport.createCatalog(mutation,
        "resolveFQTables_prefix_salesCore_returnsOrdersAndLineitem", "");
    TestSupport.seedTenantId(directory,  cat.getDisplayName());

    var ns = TestSupport.createNamespace(mutation,
        cat.getResourceId(), "core", null, "core ns");
    TestSupport.createTable(mutation, cat.getResourceId(), ns.getResourceId(),
        "orders", "s3://barf", "{}", "none");
    TestSupport.createTable(mutation, cat.getResourceId(), ns.getResourceId(),
        "lineitem", "s3://barf", "{}", "none");

    var prefix = NameRef.newBuilder()
        .setCatalog(cat.getDisplayName())
        .addPath("core")
        .build();

    var resp = directory.resolveFQTables(
        ResolveFQTablesRequest.newBuilder().setPrefix(prefix).build());

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
  void resolveFullyQualifiedTables_prefix_salesStaging2025_returnsTwo() {
    var cat = TestSupport.createCatalog(mutation,
        "resolveFQTables_prefix_salesStaging2025_returnsTwo", "");
    TestSupport.seedTenantId(directory,  cat.getDisplayName());

    var ns = TestSupport.createNamespace(mutation, cat.getResourceId(),
        "2025", List.of("staging"), "core ns");
    TestSupport.createTable(mutation, cat.getResourceId(), ns.getResourceId(),
        "orders", "s3://barf", "{}", "none");
    TestSupport.createTable(mutation, cat.getResourceId(), ns.getResourceId(),
        "lineitem", "s3://barf", "{}", "none");

    var prefix = NameRef.newBuilder()
        .setCatalog(cat.getDisplayName()).addPath("staging").addPath("2025")
        .build();

    var resp = directory.resolveFQTables(
        ResolveFQTablesRequest.newBuilder().setPrefix(prefix).build());

    assertTrue(resp.getTablesCount() == 2);
    for (var e : resp.getTablesList()) {
      assertEquals(cat.getDisplayName(), e.getName().getCatalog());
      assertEquals(List.of("staging","2025"), e.getName().getPathList());
      assertFalse(e.getName().getName().isEmpty());
      assertFalse(e.getResourceId().getId().isEmpty());
    }
  }

  @Test
  void resolve_and_lookup_financeCore_glEntries_roundtrip() {
    var cat = TestSupport.createCatalog(mutation,
        "resolve_and_lookup_financeCore_glEntries_roundtrip", "");
    TestSupport.seedTenantId(directory, cat.getDisplayName());

    var ns = TestSupport.createNamespace(mutation, cat.getResourceId(), "core", null, "core ns");
    TestSupport.createTable(mutation, cat.getResourceId(), ns.getResourceId(),
        "gl_entries", "s3://barf", "{}", "none");

    var name = NameRef.newBuilder()
        .setCatalog("finance").addPath("core").setName("gl_entries")
        .build();

    var resolved = directory.resolveTable(
        ResolveTableRequest.newBuilder().setRef(name).build());

    assertEquals("t-0001", resolved.getResourceId().getTenantId());
    assertFalse(resolved.getResourceId().getId().isEmpty());

    var lookup = directory.lookupTable(
        LookupTableRequest.newBuilder().setResourceId(resolved.getResourceId()).build());

    assertEquals("finance", lookup.getName().getCatalog());
    assertEquals(List.of("core"), lookup.getName().getPathList());
    assertEquals("gl_entries", lookup.getName().getName());
  }

  @Test
  void renameTable_reflectedInDirectory() {
    var cat = TestSupport.createCatalog(mutation, "barf1", "barf cat");
    TestSupport.seedTenantId(directory, cat.getDisplayName());

    var ns = TestSupport.createNamespace(
        mutation, cat.getResourceId(), "core", null, "core ns");
    TestSupport.createTable(
        mutation, cat.getResourceId(), ns.getResourceId(), "t0", "s3://barf", "{}", "none");
    var path = List.of("core");

    var oldRef = NameRef.newBuilder().setCatalog(
        cat.getDisplayName()).addAllPath(path).setName("t0").build();
    var id = directory.resolveTable(
        ResolveTableRequest.newBuilder().setRef(oldRef).build()).getResourceId();

    TestSupport.renameTable(mutation, id, "t1");

    assertThrows(
        io.grpc.StatusRuntimeException.class,
            () -> directory.resolveTable(
                ResolveTableRequest.newBuilder().setRef(oldRef).build()));

    var newRef = NameRef.newBuilder().setCatalog(
        cat.getDisplayName()).addAllPath(path).setName("t1").build();
    var resolved = directory.resolveTable(ResolveTableRequest.newBuilder().setRef(newRef).build());
    var looked = directory.lookupTable(
        LookupTableRequest.newBuilder().setResourceId(resolved.getResourceId()).build());
    assertEquals("t1", looked.getName().getName());
  }

  @Test
  void renameNamespace_reflectedInDirectory() {
    var cat = TestSupport.createCatalog(mutation, "barf2", "barf cat");
    TestSupport.seedTenantId(directory,  cat.getDisplayName());

    var ns = TestSupport.createNamespace(mutation, cat.getResourceId(),
        "a", List.of("p"), "core ns");
    var id = ns.getResourceId();
    var oldRef = NameRef.newBuilder().setCatalog(
        cat.getDisplayName()).addPath("p").addPath("a").build();

    mutation.renameNamespace(RenameNamespaceRequest.newBuilder()
        .setNamespaceId(id)
        .setNewDisplayName("b")
        .build()).getNamespace();

    assertThrows(StatusRuntimeException.class, () ->
        directory.resolveNamespace(
            ResolveNamespaceRequest.newBuilder().setRef(oldRef).build()));

    var newRef = NameRef.newBuilder().setCatalog(
        cat.getDisplayName()).addPath("p").addPath("b").build();
    var resolved = directory.resolveNamespace(
        ResolveNamespaceRequest.newBuilder().setRef(newRef).build());

    var looked = directory.lookupNamespace(
        LookupNamespaceRequest.newBuilder().setResourceId(resolved.getResourceId()).build());
    assertEquals(List.of("p"), looked.getRef().getPathList());
    assertEquals("b", looked.getRef().getName());
  }

  @Test
  void resolveFullyQualifiedTables_list_selector_paging_and_errors() {
    var cat = TestSupport.createCatalog(mutation,
        "resolveFQTables_list_selector_paging_and_errors", "");
    TestSupport.seedTenantId(directory,  cat.getDisplayName());

    var ns = TestSupport.createNamespace(mutation, cat.getResourceId(),
        "core", null, "core ns");
    TestSupport.createTable(mutation, cat.getResourceId(), ns.getResourceId(),
        "orders", "s3://barf", "{}", "none");
    TestSupport.createTable(mutation, cat.getResourceId(), ns.getResourceId(),
        "lineitem", "s3://barf", "{}", "none");

    var names = List.of(
        NameRef.newBuilder().setCatalog(
            cat.getDisplayName()).addPath("core").setName("orders").build(),
        NameRef.newBuilder().setCatalog(
            cat.getDisplayName()).addPath("core").setName("lineitem").build()
    );
    var req = ResolveFQTablesRequest.newBuilder().setList(
        NameList.newBuilder().addAllNames(names)).build();

    var page1 = directory.resolveFQTables(ResolveFQTablesRequest.newBuilder(req)
        .setPage(PageRequest.newBuilder().setPageSize(1)).build());
    assertEquals(1, page1.getTablesCount());
    var token = page1.getPage().getNextPageToken();

    var page2 = directory.resolveFQTables(ResolveFQTablesRequest.newBuilder(req)
        .setPage(PageRequest.newBuilder().setPageToken(token).setPageSize(1)).build());
    assertEquals(1, page2.getTablesCount());

    assertThrows(StatusRuntimeException.class, () ->
        directory.resolveFQTables(ResolveFQTablesRequest.newBuilder(req)
            .setPage(PageRequest.newBuilder().setPageToken("not-an-int")).build()));
  }

  @Test
  void resolveAndLookup_unicodeAndSpaces() {
    var cat = TestSupport.createCatalog(mutation, "barf3", "barf cat");
    TestSupport.seedTenantId(directory, cat.getDisplayName());

    var ns = TestSupport.createNamespace(mutation, cat.getResourceId(),
        "2025", List.of("staging"), "2025 ns");
    TestSupport.createTable(mutation, cat.getResourceId(), ns.getResourceId(),
        "staging events 🧪", "s3://barf", "{}", "none");

    var nameRef = NameRef.newBuilder()
        .setCatalog("barf3").addPath("staging")
            .addPath("2025").setName("staging events 🧪").build();

    var resolved = directory.resolveTable(
        ResolveTableRequest.newBuilder().setRef(nameRef).build());
    var lookup = directory.lookupTable(
        LookupTableRequest.newBuilder().setResourceId(resolved.getResourceId()).build());
    assertEquals(List.of("staging", "2025"), lookup.getName().getPathList());
    assertEquals("staging events 🧪", lookup.getName().getName());
  }

  @Test
  void lookup_unknowns_return_empty_payloads() {
    var bogus = ai.floedb.metacat.common.rpc.ResourceId.newBuilder()
        .setTenantId("t-0001").setId("nope").build();

    var lcat = directory.lookupCatalog(
        LookupCatalogRequest.newBuilder().setResourceId(bogus).build());
    assertTrue(lcat.getDisplayName().isEmpty());

    var lns = directory.lookupNamespace(
        LookupNamespaceRequest.newBuilder().setResourceId(bogus).build());
    assertFalse(lns.hasRef());

    var ltbl = directory.lookupTable(LookupTableRequest.newBuilder().setResourceId(bogus).build());
    assertFalse(ltbl.hasName());
  }

  @Test
  void pathSegments_are_not_split_and_case_is_preserved() {
    var bad = NameRef.newBuilder().setCatalog("Sales")
        .addPath("core/extra").setName("orders").build();
    assertThrows(StatusRuntimeException.class, () ->
        directory.resolveTable(ResolveTableRequest.newBuilder().setRef(bad).build()));
  }
}
