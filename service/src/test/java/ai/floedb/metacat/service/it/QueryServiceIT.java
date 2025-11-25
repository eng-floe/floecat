package ai.floedb.metacat.service.it;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.*;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.SnapshotRef;
import ai.floedb.metacat.query.rpc.BeginQueryRequest;
import ai.floedb.metacat.query.rpc.EndQueryRequest;
import ai.floedb.metacat.query.rpc.QueryInput;
import ai.floedb.metacat.query.rpc.QueryServiceGrpc;
import ai.floedb.metacat.query.rpc.RenewQueryRequest;
import ai.floedb.metacat.service.bootstrap.impl.SeedRunner;
import ai.floedb.metacat.service.util.TestDataResetter;
import ai.floedb.metacat.service.util.TestSupport;
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
}
