package ai.floedb.metacat.service.it;

import java.util.List;

import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.DirectoryGrpc;
import ai.floedb.metacat.catalog.rpc.LookupCatalogRequest;
import ai.floedb.metacat.catalog.rpc.LookupNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.LookupTableRequest;
import ai.floedb.metacat.catalog.rpc.ResolveCatalogRequest;
import ai.floedb.metacat.catalog.rpc.ResolveFQTablesRequest;
import ai.floedb.metacat.catalog.rpc.ResolveNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.ResolveTableRequest;
import ai.floedb.metacat.common.rpc.NameRef;

@QuarkusTest
class DirectoryIT {
  @GrpcClient("directory")
  DirectoryGrpc.DirectoryBlockingStub directory;

  @Test
  void resolveAndLookupCatalog() {
    var r = directory.resolveCatalog(ResolveCatalogRequest.newBuilder()
      .setDisplayName("sales").build());
    assertEquals("t-0001", r.getResourceId().getTenantId());

    var l = directory.lookupCatalog(LookupCatalogRequest.newBuilder()
      .setResourceId(r.getResourceId()).build());
    assertTrue(l.getDisplayName().equals("sales") || l.getDisplayName().isEmpty());
  }

  @Test
  void resolveAndLookupNamespace() {
    var ref = NameRef.newBuilder()
      .setCatalog("sales")
      .addNamespacePath("staging")
      .addNamespacePath("2025")
      .build();

    var ns = directory.resolveNamespace(
      ResolveNamespaceRequest.newBuilder().setRef(ref).build());

    var lookup = directory.lookupNamespace(
      LookupNamespaceRequest.newBuilder().setResourceId(ns.getResourceId()).build());

    assertEquals("sales", lookup.getRef().getCatalog());
    assertEquals(List.of("staging","2025"), lookup.getRef().getNamespacePathList());
  }

  @Test
  void resolveAndLookupTable() {
    var nameRef = NameRef.newBuilder()
      .setCatalog("sales")
      .addNamespacePath("core")
      .setName("orders")
      .build();

    var resolved = directory.resolveTable(
      ResolveTableRequest.newBuilder().setName(nameRef).build());

    var lookup = directory.lookupTable(
      LookupTableRequest.newBuilder().setResourceId(resolved.getResourceId()).build());

    assertEquals("sales", lookup.getName().getCatalog());
    assertEquals(List.of("core"), lookup.getName().getNamespacePathList());
    assertEquals("orders", lookup.getName().getName());
  }

  @Test
  void resolveTable_notFound_yieldsNotFound() {
    var missing = NameRef.newBuilder()
      .setCatalog("sales")
      .addNamespacePath("core")
      .setName("does_not_exist")
      .build();

    var ex = assertThrows(io.grpc.StatusRuntimeException.class, () ->
      directory.resolveTable(ResolveTableRequest.newBuilder().setName(missing).build()));

    assertEquals(io.grpc.Status.NOT_FOUND.getCode(), ex.getStatus().getCode());
  }

  @Test
  void resolveFQTables_prefix_salesCore_returnsOrdersAndLineitem() {
    var prefix = NameRef.newBuilder()
      .setCatalog("sales")
      .addNamespacePath("core")
      .build();

    var resp = directory.resolveFQTables(
      ResolveFQTablesRequest.newBuilder().setPrefix(prefix).build());

    assertTrue(resp.getTablesCount() == 2);
    var names = resp.getTablesList().stream().map(e -> e.getName().getName()).toList();
    assertTrue(names.contains("orders"));
    assertTrue(names.contains("lineitem"));

    for (var e : resp.getTablesList()) {
      assertEquals("sales", e.getName().getCatalog());
      assertEquals(List.of("core"), e.getName().getNamespacePathList());
      assertFalse(e.getResourceId().getId().isEmpty());
    }
  }

  @Test
  void resolveFQTables_prefix_salesStaging2025_returnsTwo() {
    var prefix = NameRef.newBuilder()
      .setCatalog("sales").addNamespacePath("staging").addNamespacePath("2025")
      .build();

    var resp = directory.resolveFQTables(
      ResolveFQTablesRequest.newBuilder().setPrefix(prefix).build());

    assertTrue(resp.getTablesCount() == 2);
    for (var e : resp.getTablesList()) {
      assertEquals("sales", e.getName().getCatalog());
      assertEquals(List.of("staging","2025"), e.getName().getNamespacePathList());
      assertFalse(e.getName().getName().isEmpty());
      assertFalse(e.getResourceId().getId().isEmpty());
    }
  }

  @Test
  void resolve_and_lookup_financeCore_glEntries_roundtrip() {
    var name = NameRef.newBuilder()
      .setCatalog("finance").addNamespacePath("core").setName("gl_entries")
      .build();

    var resolved = directory.resolveTable(
      ResolveTableRequest.newBuilder().setName(name).build());

    assertEquals("t-0001", resolved.getResourceId().getTenantId());
    assertFalse(resolved.getResourceId().getId().isEmpty());

    var lookup = directory.lookupTable(
      LookupTableRequest.newBuilder().setResourceId(resolved.getResourceId()).build());

    assertEquals("finance", lookup.getName().getCatalog());
    assertEquals(List.of("core"), lookup.getName().getNamespacePathList());
    assertEquals("gl_entries", lookup.getName().getName());
  }
}