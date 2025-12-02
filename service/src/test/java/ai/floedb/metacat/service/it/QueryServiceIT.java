package ai.floedb.metacat.service.it;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.*;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.SnapshotRef;
import ai.floedb.metacat.connector.rpc.ConnectorsGrpc;
import ai.floedb.metacat.query.rpc.*;
import ai.floedb.metacat.service.bootstrap.impl.SeedRunner;
import ai.floedb.metacat.service.util.TestDataResetter;
import ai.floedb.metacat.service.util.TestSupport;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for the QueryService lifecycle (begin/renew/end/get).
 *
 * <p>After API changes: - BeginQuery no longer accepts inputs - DescribeInputs is now required
 * before scanning or planning
 */
@QuarkusTest
class QueryServiceIT {

  @GrpcClient("metacat")
  QueryServiceGrpc.QueryServiceBlockingStub queries;

  @GrpcClient("metacat")
  QuerySchemaServiceGrpc.QuerySchemaServiceBlockingStub schema;

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

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  String catalogPrefix = this.getClass().getSimpleName() + "_";

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();
  }

  /** Verifies BeginQuery, DescribeInputs, RenewQuery, and EndQuery lifecycle behavior. */
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

    //
    // === NEW API: BeginQuery takes no inputs ===
    //
    var begin = queries.beginQuery(BeginQueryRequest.newBuilder().setTtlSeconds(2).build());

    assertTrue(begin.hasQuery());
    var query = begin.getQuery();
    assertFalse(query.getQueryId().isBlank(), "BeginQuery must return a queryId");

    //
    // === NEW API: DescribeInputs pins tables + snapshots ===
    //
    var desc =
        schema.describeInputs(
            DescribeInputsRequest.newBuilder()
                .setQueryId(query.getQueryId())
                .addInputs(
                    QueryInput.newBuilder()
                        .setName(name)
                        .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snap.getSnapshotId())))
                .build());

    // Should return exactly one schema
    assertEquals(1, desc.getSchemasCount(), "DescribeInputs must return matching schemas");

    //
    // === RenewQuery ===
    //
    var renew =
        queries.renewQuery(
            RenewQueryRequest.newBuilder().setQueryId(query.getQueryId()).setTtlSeconds(2).build());

    assertEquals(query.getQueryId(), renew.getQueryId());

    //
    // === EndQuery ===
    //
    var end =
        queries.endQuery(
            EndQueryRequest.newBuilder().setQueryId(query.getQueryId()).setCommit(true).build());

    assertEquals(query.getQueryId(), end.getQueryId());
  }
}
