package ai.floedb.metacat.service.it;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.DirectoryGrpc;
import ai.floedb.metacat.common.rpc.ErrorCode;
import ai.floedb.metacat.common.rpc.IdempotencyKey;
import ai.floedb.metacat.common.rpc.PageRequest;
import ai.floedb.metacat.common.rpc.Precondition;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.connector.rpc.*;
import ai.floedb.metacat.reconciler.impl.ReconcilerScheduler;
import ai.floedb.metacat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.metacat.service.repo.impl.*;
import ai.floedb.metacat.service.util.TestSupport;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.*;

@QuarkusTest
public class ConnectorIT {
  @GrpcClient("connectors")
  ConnectorsGrpc.ConnectorsBlockingStub connectors;

  @GrpcClient("directory")
  DirectoryGrpc.DirectoryBlockingStub directory;

  @Inject ReconcileJobStore jobs;
  @Inject ReconcilerScheduler scheduler;
  @Inject CatalogRepository catalogs;
  @Inject NamespaceRepository namespaces;
  @Inject TableRepository tables;
  @Inject SnapshotRepository snaps;

  @Test
  void connectorEndToEnd() throws Exception {
    var tenantId = TestSupport.createTenantId("t-0001");
    var conn =
        TestSupport.createConnector(
            connectors,
            ConnectorSpec.newBuilder()
                .setDisplayName("dummy-conn")
                .setKind(ConnectorKind.CK_UNITY)
                .setTargetCatalogDisplayName("cat-e2e")
                .setTargetTenantId(tenantId)
                .setUri("dummy://ignored")
                .setAuth(AuthConfig.newBuilder().setScheme("none").build())
                .build());

    var rid = conn.getResourceId();
    assertEquals(ResourceKind.RK_CONNECTOR, rid.getKind());

    var trig =
        connectors.triggerReconcile(
            TriggerReconcileRequest.newBuilder().setConnectorId(rid).setFullRescan(true).build());

    String jobId = trig.getJobId();
    assertFalse(jobId.isBlank());

    scheduler.signalScheduler();

    var deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
    ReconcileJobStore.ReconcileJob job;
    for (; ; ) {
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

    var catId = catalogs.getByName(tenantId.getId(), "cat-e2e").orElseThrow().getResourceId();

    assertEquals(2, namespaces.count(catId));

    var dbNsId = namespaces.getByPath(tenantId.getId(), catId, List.of("db")).orElseThrow();
    var anaNsId =
        namespaces.getByPath(tenantId.getId(), catId, List.of("analytics", "sales")).orElseThrow();

    assertEquals(2, tables.listByNamespace(catId, dbNsId, 50, "", new StringBuilder()).size());
    assertEquals(1, tables.listByNamespace(catId, anaNsId, 50, "", new StringBuilder()).size());

    var anyTable =
        tables.listByNamespace(catId, dbNsId, 50, "", new StringBuilder()).get(0).getResourceId();
    assertTrue(snaps.get(anyTable, 42L).isPresent());
  }

  @Test
  void createConnectorIdempotent() {
    var spec =
        ConnectorSpec.newBuilder()
            .setDisplayName("idem-1")
            .setKind(ConnectorKind.CK_UNITY)
            .setTargetCatalogDisplayName("cat-idem")
            .setUri("dummy://x")
            .build();

    var idem = IdempotencyKey.newBuilder().setKey("fixed-key-1").build();

    var r1 =
        connectors.createConnector(
            CreateConnectorRequest.newBuilder().setSpec(spec).setIdempotency(idem).build());
    var r2 =
        connectors.createConnector(
            CreateConnectorRequest.newBuilder().setSpec(spec).setIdempotency(idem).build());

    assertEquals(
        r1.getConnector().getResourceId().getId(), r2.getConnector().getResourceId().getId());
    assertEquals(r1.getMeta().getPointerVersion(), r2.getMeta().getPointerVersion());
  }

  @Test
  void getConnectorNotFound() {
    var badRid =
        ResourceId.newBuilder()
            .setTenantId("t-0001")
            .setId("nope")
            .setKind(ResourceKind.RK_CONNECTOR)
            .build();
    var ex =
        assertThrows(
            io.grpc.StatusRuntimeException.class,
            () ->
                connectors.getConnector(
                    GetConnectorRequest.newBuilder().setConnectorId(badRid).build()));
    assertEquals(io.grpc.Status.Code.NOT_FOUND, ex.getStatus().getCode());
  }

  @Test
  void listConnectorsPagination() {
    for (int i = 0; i < 5; i++) {
      TestSupport.createConnector(
          connectors,
          ConnectorSpec.newBuilder()
              .setDisplayName("p-" + i)
              .setKind(ConnectorKind.CK_UNITY)
              .setTargetCatalogDisplayName("cat-p")
              .setUri("dummy://x")
              .build());
    }
    String token = "";
    int total = 0;
    for (int page = 0; page < 5; page++) {
      var resp =
          connectors.listConnectors(
              ListConnectorsRequest.newBuilder()
                  .setPage(PageRequest.newBuilder().setPageSize(2).setPageToken(token))
                  .build());
      total += resp.getConnectorsCount();
      token = resp.getPage().getNextPageToken();
      if (token.isEmpty()) {
        break;
      }
    }
    assertTrue(total >= 5);
  }

  @Test
  void updateConnectorRenameConflict() throws Exception {
    var a =
        TestSupport.createConnector(
            connectors,
            ConnectorSpec.newBuilder()
                .setDisplayName("u-a")
                .setKind(ConnectorKind.CK_UNITY)
                .setTargetCatalogDisplayName("cat-u")
                .setUri("dummy://x")
                .build());
    var b =
        TestSupport.createConnector(
            connectors,
            ConnectorSpec.newBuilder()
                .setDisplayName("u-b")
                .setKind(ConnectorKind.CK_UNITY)
                .setTargetCatalogDisplayName("cat-u")
                .setUri("dummy://x")
                .build());

    // rename u-a -> u-a1
    var ok =
        connectors.updateConnector(
            UpdateConnectorRequest.newBuilder()
                .setConnectorId(a.getResourceId())
                .setSpec(ConnectorSpec.newBuilder().setDisplayName("u-a1"))
                .build());
    assertEquals("u-a1", ok.getConnector().getDisplayName());

    // rename u-b -> u-a1
    var ex =
        assertThrows(
            io.grpc.StatusRuntimeException.class,
            () ->
                connectors.updateConnector(
                    UpdateConnectorRequest.newBuilder()
                        .setConnectorId(b.getResourceId())
                        .setSpec(ConnectorSpec.newBuilder().setDisplayName("u-a1"))
                        .build()));

    TestSupport.assertGrpcAndMc(
        ex, Status.Code.ABORTED, ErrorCode.MC_CONFLICT, "Connector \"u-a1\" already exists");
  }

  @Test
  void updateConnectorPreconditionMismatch() throws Exception {
    var c =
        TestSupport.createConnector(
            connectors,
            ConnectorSpec.newBuilder()
                .setDisplayName("pre-a")
                .setKind(ConnectorKind.CK_UNITY)
                .setTargetCatalogDisplayName("cat-pre")
                .setUri("dummy://x")
                .build());

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                connectors.updateConnector(
                    UpdateConnectorRequest.newBuilder()
                        .setConnectorId(c.getResourceId())
                        .setSpec(ConnectorSpec.newBuilder().setUri("dummy://changed"))
                        .setPrecondition(
                            Precondition.newBuilder().setExpectedVersion(9999)) // wrong version
                        .build()));

    TestSupport.assertGrpcAndMc(
        ex, Status.Code.FAILED_PRECONDITION, ErrorCode.MC_PRECONDITION_FAILED, "Version mismatch");
  }

  @Test
  void deleteConnectorIdempotent() {
    var c =
        TestSupport.createConnector(
            connectors,
            ConnectorSpec.newBuilder()
                .setDisplayName("del-1")
                .setKind(ConnectorKind.CK_UNITY)
                .setTargetCatalogDisplayName("cat-del")
                .setUri("dummy://x")
                .build());

    connectors.deleteConnector(
        DeleteConnectorRequest.newBuilder().setConnectorId(c.getResourceId()).build());

    // deleting again must be OK (no throw), returns safe meta
    connectors.deleteConnector(
        DeleteConnectorRequest.newBuilder().setConnectorId(c.getResourceId()).build());
  }

  @Test
  void triggerReconcileNotFound() throws Exception {
    var rid =
        ai.floedb.metacat.common.rpc.ResourceId.newBuilder()
            .setTenantId("t-0001")
            .setId("missing")
            .setKind(ResourceKind.RK_CONNECTOR)
            .build();
    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                connectors.triggerReconcile(
                    TriggerReconcileRequest.newBuilder().setConnectorId(rid).build()));

    TestSupport.assertGrpcAndMc(
        ex, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, "Connector not found");
  }

  @Test
  void getReconcileJobNotFound() throws Exception {
    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                connectors.getReconcileJob(
                    GetReconcileJobRequest.newBuilder().setJobId("zzz").build()));

    TestSupport.assertGrpcAndMc(ex, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, "Job not found");
  }

  @Test
  void validateConnectorOkAndFail() throws Exception {
    var ok =
        connectors.validateConnector(
            ValidateConnectorRequest.newBuilder()
                .setSpec(
                    ConnectorSpec.newBuilder()
                        .setDisplayName("v-ok")
                        .setKind(ConnectorKind.CK_UNITY)
                        .setTargetCatalogDisplayName("cat-v")
                        .setUri("dummy://x"))
                .build());
    assertTrue(ok.getOk());

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                connectors.validateConnector(
                    ValidateConnectorRequest.newBuilder()
                        .setSpec(
                            ConnectorSpec.newBuilder()
                                .setDisplayName("v-bad")
                                .setKind(ConnectorKind.CK_UNSPECIFIED)
                                .setTargetCatalogDisplayName("cat-v")
                                .setUri("dummy://x"))
                        .build()));

    TestSupport.assertGrpcAndMc(
        ex, Status.Code.INVALID_ARGUMENT, ErrorCode.MC_INVALID_ARGUMENT, "Invalid argument");
  }
}
