package ai.floedb.metacat.service.it;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.*;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.SnapshotRef;
import ai.floedb.metacat.planning.rpc.BeginPlanRequest;
import ai.floedb.metacat.planning.rpc.EndPlanRequest;
import ai.floedb.metacat.planning.rpc.PlanInput;
import ai.floedb.metacat.planning.rpc.PlanningGrpc;
import ai.floedb.metacat.planning.rpc.RenewPlanRequest;
import ai.floedb.metacat.service.util.TestSupport;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import java.util.List;
import org.junit.jupiter.api.Test;

@QuarkusTest
class PlanningIT {
  @GrpcClient("planning")
  PlanningGrpc.PlanningBlockingStub planning;

  @GrpcClient("catalog-service")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  @GrpcClient("namespace-service")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;

  @GrpcClient("table-service")
  TableServiceGrpc.TableServiceBlockingStub table;

  @GrpcClient("snapshot-service")
  SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshot;

  @GrpcClient("directory")
  DirectoryGrpc.DirectoryBlockingStub directory;

  String catalogPrefix = this.getClass().getSimpleName() + "_";

  @Test
  void planBeginRenewEnd() {
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
        BeginPlanRequest.newBuilder()
            .addInputs(
                PlanInput.newBuilder()
                    .setName(name)
                    .setTableId(tbl.getResourceId())
                    .setSnapshot(
                        SnapshotRef.newBuilder().setSnapshotId(snap.getSnapshotId()).build())
                    .build())
            .setTtlSeconds(2)
            .build();

    var begin = planning.beginPlan(req);
    assertFalse(begin.getPlanId().isBlank());
    assertTrue(begin.getSnapshots().getPinsCount() >= 0);

    var renew =
        planning.renewPlan(
            RenewPlanRequest.newBuilder().setPlanId(begin.getPlanId()).setTtlSeconds(2).build());
    assertEquals(begin.getPlanId(), renew.getPlanId());

    var end =
        planning.endPlan(
            EndPlanRequest.newBuilder().setPlanId(begin.getPlanId()).setCommit(true).build());
    assertEquals(begin.getPlanId(), end.getPlanId());
  }
}
