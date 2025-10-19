package ai.floedb.metacat.service.it;

import java.time.Duration;
import java.util.List;

import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.connector.rpc.*;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.reconciler.impl.ReconcilerScheduler;
import ai.floedb.metacat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.metacat.service.repo.impl.*;
import ai.floedb.metacat.service.repo.util.Keys;

@QuarkusTest
public class ConnectorIT {

  @GrpcClient("connectors")
  ConnectorsGrpc.ConnectorsBlockingStub connectors;

  @Inject ReconcileJobStore jobs;
  @Inject ReconcilerScheduler scheduler;

  @Inject CatalogRepository catalogs;
  @Inject NamespaceRepository namespaces;
  @Inject TableRepository tables;
  @Inject SnapshotRepository snaps;

  @Test
  void Connector_end_to_end() throws Exception {
    var conn = TestSupport.createConnector(connectors,
        ConnectorSpec.newBuilder()
            .setDisplayName("dummy-conn")
            .setKind(ConnectorKind.CK_UNITY)
            .setTargetCatalogDisplayName("cat-e2e")
            .setTargetTenantId("t-0001")
            .setUri("dummy://ignored")
            .setAuth(AuthConfig.newBuilder()
                .setScheme("none")
                .build())
            .build());

    var rid = conn.getResourceId();
    assertEquals(ResourceKind.RK_CONNECTOR, rid.getKind());

    var trig = connectors.triggerReconcile(TriggerReconcileRequest.newBuilder()
        .setConnectorId(rid)
        .setFullRescan(true)
        .build());

    String jobId = trig.getJobId();
    assertFalse(jobId.isBlank());

    scheduler.signalScheduler();

    var deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
    ReconcileJobStore.ReconcileJob job;
    for (;;) {
      job = jobs.get(jobId).orElse(null);
      if (job != null && ("JS_SUCCEEDED".equals(job.state) || "JS_FAILED".equals(job.state))) {
        break;
      }
      if (System.nanoTime() > deadline) {
        break;
      }
      Thread.sleep(25);
    }
    assertNotNull(job);
    assertEquals("JS_SUCCEEDED", job.state);

    String dump = catalogs.dumpByPrefix(Keys.catByNamePrefix("t-0001"), 100);
    System.out.println(dump);

    var catId = catalogs.getByName(
        "t-0001", "cat-e2e").orElseThrow().getResourceId();

    assertEquals(2, namespaces.countUnderCatalog(catId));

    var dbNsId = namespaces.getByPath("t-0001", catId, List.of("db")).orElseThrow();
    var anaNsId = namespaces.getByPath(
        "t-0001", catId, List.of("analytics","sales")).orElseThrow();

    assertEquals(2, tables.listByNamespace(
        catId, dbNsId, 50, "", new StringBuilder()).size());
    assertEquals(1, tables.listByNamespace(
        catId, anaNsId, 50, "", new StringBuilder()).size());

    var anyTable = tables.listByNamespace(
        catId, dbNsId,  50, "", new StringBuilder()).get(0).getResourceId();
    assertTrue(snaps.get(anyTable, 42L).isPresent());
  }
}
