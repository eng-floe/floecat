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
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
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
    String kind =
        (engineKind == null || engineKind.isBlank())
            ? SystemNodeRegistry.FLOECAT_DEFAULT_CATALOG
            : engineKind;
    return ResourceId.newBuilder()
        .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
        .setKind(ResourceKind.RK_TABLE)
        .setId(kind + ":" + schema + "." + table)
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

    ResourceId systemTableId = systemTable("pg", "information_schema", "tables");
    var stub = withEngine(systemScan, "pg");
    ScanSystemTableResponse resp =
        stub.scanSystemTable(
            ScanSystemTableRequest.newBuilder()
                .setQueryId(beginQuery(cat.getResourceId()))
                .setTableId(systemTableId)
                .build());

    assertTrue(resp.getRowsCount() > 0);
    List<List<String>> rows = rows(resp);
    List<List<String>> ordersRows =
        rows.stream().filter(r -> r.size() > 2 && "orders".equals(r.get(2))).toList();
    assertEquals(1, ordersRows.size(), "There should be exactly one 'orders' table row");
    List<String> ordersRow = ordersRows.get(0);
    assertEquals(cat.getDisplayName(), ordersRow.get(0), "table_catalog should match catalog name");
    assertEquals(
        "analytics.ns", ordersRow.get(1), "table_schema should be hierarchical 'analytics.ns'");
    assertEquals("orders", ordersRow.get(2), "table_name should be 'orders'");
    assertEquals("BASE TABLE", ordersRow.get(3), "table_type should be 'BASE TABLE'");
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

    var stub = withEngine(systemScan, "trino");
    ScanSystemTableResponse resp =
        stub.scanSystemTable(
            ScanSystemTableRequest.newBuilder()
                .setQueryId(beginQuery(catB.getResourceId()))
                .setTableId(systemTableId)
                .build());

    List<List<String>> rows = rows(resp);
    assertFalse(
        rows.stream().anyMatch(r -> r.size() > 2 && "foo".equals(r.get(2))),
        "Rows should not contain 'foo'");
    List<List<String>> barRows =
        rows.stream().filter(r -> r.size() > 2 && "bar".equals(r.get(2))).toList();
    assertEquals(1, barRows.size(), "There should be exactly one 'bar' table row");
    assertEquals("b.nsB", barRows.get(0).get(1), "Schema value for 'bar' should be 'b.nsB'");
  }

  @Test
  void pgCatalogRejectedForNonPostgresEngine() {
    var cat = TestSupport.createCatalog(catalog, "catName", "");
    ResourceId systemTableId = systemTable("", "pg_catalog", "pg_class");

    var stub = withEngine(systemScan, "");
    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                stub.scanSystemTable(
                    ScanSystemTableRequest.newBuilder()
                        .setQueryId(beginQuery(cat.getResourceId()))
                        .setTableId(systemTableId)
                        .build()));

    assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
  }

  @Test
  void informationSchemaVisibleForAnyEngine() {
    String engineKind = "no_engine";
    var catName = catalogPrefix + engineKind;

    var cat = TestSupport.createCatalog(catalog, catName, "");
    ResourceId systemTableId = systemTable(engineKind, "information_schema", "schemata");

    var stub = withEngine(systemScan, engineKind);
    ScanSystemTableResponse resp =
        stub.scanSystemTable(
            ScanSystemTableRequest.newBuilder()
                .setQueryId(beginQuery(cat.getResourceId()))
                .setTableId(systemTableId)
                .build());

    assertTrue(resp.getRowsCount() > 0);
    List<List<String>> rows = rows(resp);
    boolean found =
        rows.stream()
            .anyMatch(
                r ->
                    r.size() > 1
                        && engineKind.equals(r.get(0))
                        && "information_schema".equals(r.get(1)));
    assertTrue(
        found,
        "There should be a row with table_catalog = no-engine and schema = 'information_schema'");
  }

  // ------------------------------------------------------------------------
  // Helpers
  // ------------------------------------------------------------------------
  private List<List<String>> rows(ScanSystemTableResponse resp) {
    return resp.getRowsList().stream().map(r -> List.copyOf(r.getValuesList())).toList();
  }

  private static final Metadata.Key<String> ENGINE_KIND_HEADER =
      Metadata.Key.of("x-engine-kind", Metadata.ASCII_STRING_MARSHALLER);

  private static final Metadata.Key<String> ENGINE_VERSION_HEADER =
      Metadata.Key.of("x-engine-version", Metadata.ASCII_STRING_MARSHALLER);

  private QuerySystemScanServiceGrpc.QuerySystemScanServiceBlockingStub withEngine(
      QuerySystemScanServiceGrpc.QuerySystemScanServiceBlockingStub stub, String engineKind) {

    Metadata metadata = new Metadata();
    metadata.put(ENGINE_KIND_HEADER, engineKind);
    metadata.put(ENGINE_VERSION_HEADER, "");

    return stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
  }
}
