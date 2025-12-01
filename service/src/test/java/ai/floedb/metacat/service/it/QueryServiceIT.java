package ai.floedb.metacat.service.it;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.*;
import ai.floedb.metacat.common.rpc.ErrorCode;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.SnapshotRef;
import ai.floedb.metacat.connector.rpc.AuthConfig;
import ai.floedb.metacat.connector.rpc.Connector;
import ai.floedb.metacat.connector.rpc.ConnectorKind;
import ai.floedb.metacat.connector.rpc.ConnectorSpec;
import ai.floedb.metacat.connector.rpc.ConnectorsGrpc;
import ai.floedb.metacat.connector.rpc.DestinationTarget;
import ai.floedb.metacat.connector.rpc.NamespacePath;
import ai.floedb.metacat.connector.rpc.SourceSelector;
import ai.floedb.metacat.query.rpc.BeginQueryRequest;
import ai.floedb.metacat.query.rpc.EndQueryRequest;
import ai.floedb.metacat.query.rpc.FetchScanBundleRequest;
import ai.floedb.metacat.query.rpc.QueryInput;
import ai.floedb.metacat.query.rpc.QueryServiceGrpc;
import ai.floedb.metacat.query.rpc.RenewQueryRequest;
import ai.floedb.metacat.service.bootstrap.impl.SeedRunner;
import ai.floedb.metacat.service.util.TestDataResetter;
import ai.floedb.metacat.service.util.TestSupport;
import com.google.protobuf.FieldMask;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
class QueryServiceIT {
  @GrpcClient("metacat")
  QueryServiceGrpc.QueryServiceBlockingStub queries;

  @GrpcClient("metacat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  @GrpcClient("metacat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;

  @GrpcClient("metacat")
  TableServiceGrpc.TableServiceBlockingStub table;

  @GrpcClient("metacat")
  SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshot;

  @GrpcClient("metacat")
  DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;

  @GrpcClient("metacat")
  ConnectorsGrpc.ConnectorsBlockingStub connectors;

  String catalogPrefix = this.getClass().getSimpleName() + "_";

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();
  }

  @Test
  void queryBeginRenewEnd() {
    var catName = catalogPrefix + "cat1";
    var cat = TestSupport.createCatalog(catalog, catName, "");
    var ns = TestSupport.createNamespace(namespace, cat.getResourceId(), "sch", List.of("db"), "");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "orders",
            "s3://bucket/orders",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");
    var snap =
        TestSupport.createSnapshot(
            snapshot, tbl.getResourceId(), 0L, System.currentTimeMillis() - 10_000L);

    var name =
        NameRef.newBuilder()
            .setCatalog(catName)
            .addPath("db")
            .addPath("sch")
            .setName("orders")
            .build();

    var req =
        BeginQueryRequest.newBuilder()
            .addInputs(
                QueryInput.newBuilder()
                    .setName(name)
                    .setTableId(tbl.getResourceId())
                    .setSnapshot(
                        SnapshotRef.newBuilder().setSnapshotId(snap.getSnapshotId()).build())
                    .build())
            .setTtlSeconds(2)
            .build();

    var begin = queries.beginQuery(req);
    assertTrue(begin.hasQuery());
    var beginQuery = begin.getQuery();
    assertFalse(beginQuery.getQueryId().isBlank());
    assertTrue(beginQuery.getSnapshots().getPinsCount() >= 0);

    var renew =
        queries.renewQuery(
            RenewQueryRequest.newBuilder()
                .setQueryId(beginQuery.getQueryId())
                .setTtlSeconds(2)
                .build());
    assertEquals(beginQuery.getQueryId(), renew.getQueryId());

    var end =
        queries.endQuery(
            EndQueryRequest.newBuilder()
                .setQueryId(beginQuery.getQueryId())
                .setCommit(true)
                .build());
    assertEquals(beginQuery.getQueryId(), end.getQueryId());
  }

  // Ensures FetchScanBundle streams a bundle for tables that were pinned during BeginQuery.
  @Test
  void fetchScanBundleReturnsBundleForPinnedTable() {
    var catName = catalogPrefix + "scan_cat";
    var cat = TestSupport.createCatalog(catalog, catName, "");
    var ns =
        TestSupport.createNamespace(namespace, cat.getResourceId(), "scan_ns", List.of("scan"), "");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "scan_orders",
            "s3://bucket/scan_orders",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "scan table");
    var snap =
        TestSupport.createSnapshot(
            snapshot, tbl.getResourceId(), 501L, System.currentTimeMillis() - 1_000L);

    var connector = createDummyConnector(cat.getResourceId(), ns.getResourceId(), "bundle");
    attachConnectorToTable(tbl.getResourceId(), connector);

    var name =
        NameRef.newBuilder().setCatalog(catName).addPath("scan").setName("scan_orders").build();
    var begin =
        queries.beginQuery(
            BeginQueryRequest.newBuilder()
                .addInputs(
                    QueryInput.newBuilder()
                        .setName(name)
                        .setTableId(tbl.getResourceId())
                        .setSnapshot(
                            SnapshotRef.newBuilder().setSnapshotId(snap.getSnapshotId()).build()))
                .build());

    var resp =
        queries.fetchScanBundle(
            FetchScanBundleRequest.newBuilder()
                .setQueryId(begin.getQuery().getQueryId())
                .setTableId(tbl.getResourceId())
                .build());

    assertTrue(resp.hasBundle());
    assertEquals(0, resp.getBundle().getDataFilesCount());
    assertEquals(0, resp.getBundle().getDeleteFilesCount());
  }

  // Ensures FetchScanBundle rejects requests for tables that were not part of the lease.
  @Test
  void fetchScanBundleRejectsUnpinnedTables() throws Exception {
    var catName = catalogPrefix + "scan_nf";
    var cat = TestSupport.createCatalog(catalog, catName, "");
    var ns =
        TestSupport.createNamespace(namespace, cat.getResourceId(), "scan_nf", List.of("nf"), "");
    var tblA =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "nf_orders",
            "s3://bucket/nf_orders",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "nf table");
    var tblB =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "nf_other",
            "s3://bucket/nf_other",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "nf other table");

    var snap =
        TestSupport.createSnapshot(
            snapshot, tblA.getResourceId(), 777L, System.currentTimeMillis() - 5_000L);

    var connector = createDummyConnector(cat.getResourceId(), ns.getResourceId(), "reject");
    attachConnectorToTable(tblA.getResourceId(), connector);

    var begin =
        queries.beginQuery(
            BeginQueryRequest.newBuilder()
                .addInputs(
                    QueryInput.newBuilder()
                        .setTableId(tblA.getResourceId())
                        .setSnapshot(
                            SnapshotRef.newBuilder().setSnapshotId(snap.getSnapshotId()).build()))
                .build());

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                queries.fetchScanBundle(
                    FetchScanBundleRequest.newBuilder()
                        .setQueryId(begin.getQuery().getQueryId())
                        .setTableId(tblB.getResourceId())
                        .build()));
    TestSupport.assertGrpcAndMc(
        ex, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, "not pinned for query");
  }

  private Connector createDummyConnector(
      ResourceId catalogId, ResourceId namespaceId, String suffix) {
    var source =
        SourceSelector.newBuilder()
            .setNamespace(
                NamespacePath.newBuilder().addSegments("analytics").addSegments("sales").build())
            .build();
    var destination =
        DestinationTarget.newBuilder().setCatalogId(catalogId).setNamespaceId(namespaceId).build();
    var spec =
        ConnectorSpec.newBuilder()
            .setDisplayName("qs-" + suffix)
            .setKind(ConnectorKind.CK_UNITY)
            .setUri("dummy://ignored")
            .setSource(source)
            .setDestination(destination)
            .setAuth(AuthConfig.newBuilder().setScheme("none").build())
            .build();
    return TestSupport.createConnector(connectors, spec);
  }

  private void attachConnectorToTable(ResourceId tableId, Connector connector) {
    var upstream =
        UpstreamRef.newBuilder()
            .setConnectorId(connector.getResourceId())
            .setUri("dummy://ignored")
            .setTableDisplayName(connector.getDisplayName() + "_src")
            .setFormat(TableFormat.TF_ICEBERG)
            .addNamespacePath("analytics")
            .addNamespacePath("sales")
            .build();
    var spec = TableSpec.newBuilder().setUpstream(upstream).build();
    var mask = FieldMask.newBuilder().addPaths("upstream").build();
    table.updateTable(
        UpdateTableRequest.newBuilder()
            .setTableId(tableId)
            .setSpec(spec)
            .setUpdateMask(mask)
            .build());
  }
}
