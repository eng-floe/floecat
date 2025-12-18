package ai.floedb.floecat.service.it;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.query.rpc.BeginQueryRequest;
import ai.floedb.floecat.query.rpc.QueryServiceGrpc;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import ai.floedb.floecat.system.rpc.QuerySystemScanServiceGrpc;
import ai.floedb.floecat.system.rpc.ScanSystemTableRequest;
import ai.floedb.floecat.system.rpc.ScanSystemTableResponse;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for QuerySystemScanService.
 *
 * <p>These tests validate: - correct resolution of system tables - catalog scoping - engine scoping
 * - engine-agnostic visibility of information_schema
 */
@QuarkusTest
public class QuerySystemScanServiceIT {

  @GrpcClient("floecat")
  QuerySystemScanServiceGrpc.QuerySystemScanServiceBlockingStub systemScan;

  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  @GrpcClient("floecat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;

  @GrpcClient("floecat")
  TableServiceGrpc.TableServiceBlockingStub table;

  @GrpcClient("floecat")
  QueryServiceGrpc.QueryServiceBlockingStub queryService;

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  private String catalogPrefix = getClass().getSimpleName() + "_";

  private static ResourceId systemTable(String engineKind, String schema, String table) {
    return ResourceId.newBuilder()
        .setAccountId("_system")
        .setKind(ResourceKind.RK_TABLE)
        .setId(engineKind + ":" + schema + "." + table)
        .build();
  }

  private String beginQuery(ResourceId catalogId) {
    return queryService
        .beginQuery(BeginQueryRequest.newBuilder().setDefaultCatalogId(catalogId).build())
        .getQuery()
        .getQueryId();
  }

  @BeforeEach
  void reset() {
    resetter.wipeAll();
    seeder.seedData();
  }

  @Test
  void informationSchemaReturnsRowsForQueryCatalog() {
    var catName = catalogPrefix + "info";
    var cat = TestSupport.createCatalog(catalog, catName, "");
    var ns =
        TestSupport.createNamespace(namespace, cat.getResourceId(), "ns", List.of("analytics"), "");
    TestSupport.createTable(
        table,
        cat.getResourceId(),
        ns.getResourceId(),
        "orders",
        "s3://bucket/orders",
        "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
        "orders table");

    ResourceId systemTableId = systemTable("trino", "information_schema", "tables");

    ScanSystemTableResponse resp =
        systemScan.scanSystemTable(
            ScanSystemTableRequest.newBuilder()
                .setQueryId(beginQuery(cat.getResourceId()))
                .setTableId(systemTableId)
                .build());

    assertTrue(resp.getRowsCount() > 0);
    List<List<String>> rows = rows(resp);
    assertTrue(
        rows.stream().anyMatch(r -> r.contains(cat.getDisplayName())),
        "Rows should contain the catalog name");
    assertTrue(
        rows.stream().anyMatch(r -> r.contains("orders")),
        "Rows should contain the user table name 'orders'");
  }

  @Test
  void systemScanIsScopedToQueryCatalog() {
    var catA = TestSupport.createCatalog(catalog, catalogPrefix + "A", "");
    var catB = TestSupport.createCatalog(catalog, catalogPrefix + "B", "");

    var nsA = TestSupport.createNamespace(namespace, catA.getResourceId(), "nsA", List.of("a"), "");
    var nsB = TestSupport.createNamespace(namespace, catB.getResourceId(), "nsB", List.of("b"), "");

    TestSupport.createTable(
        table,
        catA.getResourceId(),
        nsA.getResourceId(),
        "foo",
        "s3://bucket/foo",
        "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
        "foo");

    TestSupport.createTable(
        table,
        catB.getResourceId(),
        nsB.getResourceId(),
        "bar",
        "s3://bucket/bar",
        "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
        "bar");

    ResourceId systemTableId = systemTable("trino", "information_schema", "tables");

    ScanSystemTableResponse resp =
        systemScan.scanSystemTable(
            ScanSystemTableRequest.newBuilder()
                .setQueryId(beginQuery(catB.getResourceId()))
                .setTableId(systemTableId)
                .build());

    List<List<String>> rows = rows(resp);
    assertTrue(rows.stream().anyMatch(r -> r.contains("foo")), "Rows should contain 'foo'");
    assertFalse(rows.stream().anyMatch(r -> r.contains("bar")), "Rows should NOT contain 'bar'");
  }

  @Test
  void pgCatalogRejectedForNonPostgresEngine() {
    var cat = TestSupport.createCatalog(catalog, "catName", "");
    ResourceId systemTableId = systemTable("trino", "pg_catalog", "pg_class");

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                systemScan.scanSystemTable(
                    ScanSystemTableRequest.newBuilder()
                        .setQueryId(beginQuery(cat.getResourceId()))
                        .setTableId(systemTableId)
                        .build()));

    assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
  }

  @Test
  void informationSchemaVisibleForAnyEngine() {
    var catName = catalogPrefix + "no_engine";
    var cat = TestSupport.createCatalog(catalog, catName, "");

    ResourceId systemTableId = systemTable("trino", "information_schema", "schemata");

    ScanSystemTableResponse resp =
        systemScan.scanSystemTable(
            ScanSystemTableRequest.newBuilder()
                .setQueryId(beginQuery(cat.getResourceId()))
                .setTableId(systemTableId)
                .build());

    assertTrue(resp.getRowsCount() > 0);
    List<List<String>> rows = rows(resp);
    assertTrue(
        rows.stream().anyMatch(r -> r.contains(catName)),
        "Rows should contain the created catalog name");
  }

  private List<List<String>> rows(ScanSystemTableResponse resp) {
    return resp.getRowsList().stream().map(r -> List.copyOf(r.getValuesList())).toList();
  }
}
