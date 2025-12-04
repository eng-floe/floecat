package ai.floedb.metacat.service.it;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.metacat.catalog.rpc.ColumnStats;
import ai.floedb.metacat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.metacat.catalog.rpc.ListFileColumnStatsRequest;
import ai.floedb.metacat.catalog.rpc.TableStatisticsServiceGrpc;
import ai.floedb.metacat.common.rpc.ErrorCode;
import ai.floedb.metacat.common.rpc.IdempotencyKey;
import ai.floedb.metacat.common.rpc.PageRequest;
import ai.floedb.metacat.common.rpc.Precondition;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.common.rpc.SnapshotRef;
import ai.floedb.metacat.common.rpc.SpecialSnapshot;
import ai.floedb.metacat.connector.rpc.*;
import ai.floedb.metacat.connector.spi.ConnectorConfigMapper;
import ai.floedb.metacat.connector.spi.ConnectorFactory;
import ai.floedb.metacat.reconciler.impl.ReconcilerScheduler;
import ai.floedb.metacat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.metacat.service.bootstrap.impl.SeedRunner;
import ai.floedb.metacat.service.repo.impl.*;
import ai.floedb.metacat.service.util.TestDataResetter;
import ai.floedb.metacat.service.util.TestSupport;
import com.google.protobuf.FieldMask;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.*;

@QuarkusTest
public class ConnectorIT {
  @GrpcClient("metacat")
  ConnectorsGrpc.ConnectorsBlockingStub connectors;

  @GrpcClient("metacat")
  DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;

  @GrpcClient("metacat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalogService;

  @GrpcClient("metacat")
  TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub statsService;

  @Inject ReconcileJobStore jobs;
  @Inject ReconcilerScheduler scheduler;
  @Inject CatalogRepository catalogs;
  @Inject NamespaceRepository namespaces;
  @Inject TableRepository tables;
  @Inject SnapshotRepository snaps;
  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();
  }

  @Test
  void mapperToFactoryDummyWorks() {
    var proto =
        Connector.newBuilder()
            .setDisplayName("dummy-conn")
            .setKind(ConnectorKind.CK_UNITY)
            .setUri("dummy://ignored")
            .setDestination(dest("cat-e2e"))
            .build();

    var cfg = ConnectorConfigMapper.fromProto(proto);
    try (var conn = ConnectorFactory.create(cfg)) {
      assertEquals("dummy", conn.id());
    }
  }

  @Test
  void connectorEndToEnd() throws Exception {
    var tenantId = TestSupport.createTenantId(TestSupport.DEFAULT_SEED_TENANT);
    TestSupport.createCatalog(catalogService, "cat-e2e", "");
    var conn =
        TestSupport.createConnector(
            connectors,
            ConnectorSpec.newBuilder()
                .setDisplayName("dummy-conn")
                .setKind(ConnectorKind.CK_UNITY)
                .setUri("dummy://ignored")
                .setSource(source(List.of("db")))
                .setDestination(dest("cat-e2e"))
                .setAuth(AuthConfig.newBuilder().setScheme("none").build())
                .build());

    var rid = conn.getResourceId();
    var job = runReconcile(rid);
    assertNotNull(job);
    assertEquals("JS_SUCCEEDED", job.state, () -> "job failed: " + job.message);

    conn =
        TestSupport.createConnector(
            connectors,
            ConnectorSpec.newBuilder()
                .setDisplayName("dummy-conn2")
                .setKind(ConnectorKind.CK_UNITY)
                .setUri("dummy://ignored")
                .setSource(source(List.of("analytics", "sales")))
                .setDestination(dest("cat-e2e"))
                .setAuth(AuthConfig.newBuilder().setScheme("none").build())
                .build());

    rid = conn.getResourceId();
    var job2 = runReconcile(rid);
    assertNotNull(job2);
    assertEquals("JS_SUCCEEDED", job2.state, () -> "job failed: " + job2.message);

    var catId = catalogs.getByName(tenantId.getId(), "cat-e2e").orElseThrow().getResourceId();

    assertEquals(3, namespaces.count(tenantId.getId(), catId.getId(), List.of()));

    var dbNsId = namespaces.getByPath(tenantId.getId(), catId.getId(), List.of("db")).orElseThrow();
    var anaNsId =
        namespaces
            .getByPath(tenantId.getId(), catId.getId(), List.of("analytics", "sales"))
            .orElseThrow();

    assertEquals(
        2,
        tables
            .list(
                tenantId.getId(),
                catId.getId(),
                dbNsId.getResourceId().getId(),
                50,
                "",
                new StringBuilder())
            .size());
    assertEquals(
        1,
        tables
            .list(
                tenantId.getId(),
                catId.getId(),
                anaNsId.getResourceId().getId(),
                50,
                "",
                new StringBuilder())
            .size());

    var anyTable =
        tables
            .list(
                tenantId.getId(),
                catId.getId(),
                dbNsId.getResourceId().getId(),
                50,
                "",
                new StringBuilder())
            .get(0)
            .getResourceId();
    assertTrue(snaps.getById(anyTable, 42L).isPresent());

    // Should be able to run it again without issues
    rid = conn.getResourceId();
    var job3 = runReconcile(rid);
    assertNotNull(job3);
    assertEquals("JS_SUCCEEDED", job3.state, () -> "job failed: " + job3.message);
  }

  @Test
  void dummyConnectorStatsRoundTrip() throws Exception {
    var tenantId = TestSupport.createTenantId(TestSupport.DEFAULT_SEED_TENANT);
    TestSupport.createCatalog(catalogService, "cat-stats", "");

    var conn =
        TestSupport.createConnector(
            connectors,
            ConnectorSpec.newBuilder()
                .setDisplayName("dummy-stats")
                .setKind(ConnectorKind.CK_UNITY)
                .setUri("dummy://ignored")
                .setSource(source(List.of("db")))
                .setDestination(dest("cat-stats"))
                .setAuth(AuthConfig.newBuilder().setScheme("none").build())
                .build());

    var job = runReconcile(conn.getResourceId());
    assertNotNull(job);
    assertEquals("JS_SUCCEEDED", job.state, () -> "job failed: " + job.message);

    var catId = catalogs.getByName(tenantId.getId(), "cat-stats").orElseThrow().getResourceId();

    var dbNsId = namespaces.getByPath(tenantId.getId(), catId.getId(), List.of("db")).orElseThrow();

    var tbl =
        tables
            .list(
                tenantId.getId(),
                catId.getId(),
                dbNsId.getResourceId().getId(),
                50,
                "",
                new StringBuilder())
            .get(0)
            .getResourceId();

    var fileResp =
        statsService.listFileColumnStats(
            ListFileColumnStatsRequest.newBuilder()
                .setTableId(tbl)
                .setSnapshot(
                    SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT).build())
                .setPage(PageRequest.newBuilder().setPageSize(100))
                .build());

    assertEquals(3, fileResp.getFileColumnsCount(), "expected 3 files");

    for (var f : fileResp.getFileColumnsList()) {
      assertTrue(f.getColumnsCount() > 0, "file should have per-column stats");

      var byName =
          f.getColumnsList().stream()
              .collect(Collectors.toMap(ColumnStats::getColumnName, cs -> cs));

      assertTrue(byName.containsKey("id"), "per-file stats should include id column");
      var idCol = byName.get("id");
      assertEquals(0L, idCol.getNullCount());
      assertTrue(idCol.getValueCount() > 0, "value_count should be > 0 for id");
    }
  }

  @Test
  void dummyConnectorRespectsDestinationTableDisplayName() throws Exception {
    var tenantId = TestSupport.createTenantId(TestSupport.DEFAULT_SEED_TENANT);
    TestSupport.createCatalog(catalogService, "cat-dest-table", "");

    var dest =
        DestinationTarget.newBuilder()
            .setCatalogDisplayName("cat-dest-table")
            .setNamespace(
                NamespacePath.newBuilder().addSegments("analytics").addSegments("sales").build())
            .setTableDisplayName("my_events_copy")
            .build();

    var src =
        SourceSelector.newBuilder()
            .setNamespace(NamespacePath.newBuilder().addSegments("db").build())
            .setTable("events")
            .build();

    var conn =
        TestSupport.createConnector(
            connectors,
            ConnectorSpec.newBuilder()
                .setDisplayName("dummy-dest-table")
                .setKind(ConnectorKind.CK_UNITY)
                .setUri("dummy://ignored")
                .setSource(src)
                .setDestination(dest)
                .setAuth(AuthConfig.newBuilder().setScheme("none").build())
                .build());

    var job = runReconcile(conn.getResourceId());
    assertNotNull(job);
    assertEquals("JS_SUCCEEDED", job.state, () -> "job failed: " + job.message);

    var catId =
        catalogs.getByName(tenantId.getId(), "cat-dest-table").orElseThrow().getResourceId();

    var ns =
        namespaces
            .getByPath(tenantId.getId(), catId.getId(), List.of("analytics", "sales"))
            .orElseThrow();

    var outTables =
        tables.list(
            tenantId.getId(),
            catId.getId(),
            ns.getResourceId().getId(),
            50,
            "",
            new StringBuilder());

    assertEquals(1, outTables.size(), "expected exactly one table in destination namespace");
    assertEquals("my_events_copy", outTables.get(0).getDisplayName());
  }

  @Test
  void GlueIcebergRESTconnectorEndToEnd() throws Exception {
    boolean enabled =
        ConfigProvider.getConfig()
            .getOptionalValue("test.glue.enabled", Boolean.class)
            .orElse(false);
    Assumptions.assumeTrue(enabled, "Disabled by test.glue.enabled=false");

    var tenantId = TestSupport.createTenantId(TestSupport.DEFAULT_SEED_TENANT);

    TestSupport.createCatalog(catalogService, "glue-iceberg-rest", "");

    var conn =
        TestSupport.createConnector(
            connectors,
            ConnectorSpec.newBuilder()
                .setDisplayName("Glue Iceberg")
                .setKind(ConnectorKind.CK_ICEBERG)
                .setUri("https://glue.us-east-1.amazonaws.com/iceberg/")
                .setSource(source(List.of("tpcds_iceberg")))
                .setDestination(dest("glue-iceberg-rest"))
                .setAuth(AuthConfig.newBuilder().setScheme("aws-sigv4").build())
                .build());

    var rid = conn.getResourceId();
    var job = runReconcile(rid);
    assertNotNull(job);
    assertEquals("JS_SUCCEEDED", job.state);

    var catId =
        catalogs.getByName(tenantId.getId(), "glue-iceberg-rest").orElseThrow().getResourceId();

    assertEquals(1, namespaces.count(tenantId.getId(), catId.getId(), List.of()));

    var tpcdsNsId =
        namespaces
            .getByPath(tenantId.getId(), catId.getId(), List.of("tpcds_iceberg"))
            .orElseThrow();

    assertEquals(
        24,
        tables
            .list(
                tenantId.getId(),
                catId.getId(),
                tpcdsNsId.getResourceId().getId(),
                50,
                "",
                new StringBuilder())
            .size());
  }

  ReconcileJobStore.ReconcileJob runReconcile(ResourceId rid) throws Exception {
    assertEquals(ResourceKind.RK_CONNECTOR, rid.getKind());

    var trig =
        connectors.triggerReconcile(
            TriggerReconcileRequest.newBuilder().setConnectorId(rid).setFullRescan(true).build());

    String jobId = trig.getJobId();
    assertFalse(jobId.isBlank());

    var deadline = System.nanoTime() + Duration.ofSeconds(900).toNanos();
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

    return job;
  }

  @Test
  void dummyNestedSchemaAndFiltering() {
    var proto =
        Connector.newBuilder()
            .setDisplayName("dummy-conn-nested")
            .setKind(ConnectorKind.CK_UNITY)
            .setUri("dummy://ignored")
            .setDestination(dest("cat-e2e"))
            .build();

    var cfg = ConnectorConfigMapper.fromProto(proto);
    try (var conn = ConnectorFactory.create(cfg)) {
      assertEquals("dummy", conn.id());

      var td = conn.describe("db", "events");
      assertTrue(td.schemaJson().contains("\"user\""));
      assertTrue(td.schemaJson().contains("\"items\""));
      assertTrue(td.schemaJson().contains("\"attrs\""));

      var onlyIds =
          conn.enumerateSnapshotsWithStats(
              "db",
              "events",
              ResourceId.newBuilder()
                  .setTenantId("t")
                  .setId("tbl")
                  .setKind(ResourceKind.RK_TABLE)
                  .build(),
              Set.of("#1", "#4", "#9"));

      var cs1 = onlyIds.get(0).columnStats();
      var ids1 =
          cs1.stream().map(ColumnStats::getColumnId).collect(java.util.stream.Collectors.toSet());
      var names1 =
          cs1.stream().map(ColumnStats::getColumnName).collect(java.util.stream.Collectors.toSet());

      assertEquals(Set.of("1", "4", "9"), ids1);
      assertEquals(Set.of("id", "user.id", "items.element.qty"), names1);

      var onlyNames =
          conn.enumerateSnapshotsWithStats(
              "db",
              "events",
              ResourceId.newBuilder()
                  .setTenantId("t")
                  .setId("tbl")
                  .setKind(ResourceKind.RK_TABLE)
                  .build(),
              Set.of("user.name", "attrs.value"));

      var cs2 = onlyNames.get(0).columnStats();
      var ids2 =
          cs2.stream().map(ColumnStats::getColumnId).collect(java.util.stream.Collectors.toSet());
      var names2 =
          cs2.stream().map(ColumnStats::getColumnName).collect(java.util.stream.Collectors.toSet());

      assertEquals(Set.of("5", "12"), ids2);
      assertEquals(Set.of("user.name", "attrs.value"), names2);

      var mixed =
          conn.enumerateSnapshotsWithStats(
              "db",
              "events",
              ResourceId.newBuilder()
                  .setTenantId("t")
                  .setId("tbl")
                  .setKind(ResourceKind.RK_TABLE)
                  .build(),
              Set.of("#2", "items.element.sku"));

      var cs3 = mixed.get(0).columnStats();
      var ids3 =
          cs3.stream().map(ColumnStats::getColumnId).collect(java.util.stream.Collectors.toSet());
      var names3 =
          cs3.stream().map(ColumnStats::getColumnName).collect(java.util.stream.Collectors.toSet());

      assertEquals(Set.of("2", "8"), ids3);
      assertEquals(Set.of("ts", "items.element.sku"), names3);

      var allCols =
          conn.enumerateSnapshotsWithStats(
              "db",
              "events",
              ResourceId.newBuilder()
                  .setTenantId("t")
                  .setId("tbl")
                  .setKind(ResourceKind.RK_TABLE)
                  .build(),
              Collections.emptySet());

      assertEquals(8, allCols.get(0).columnStats().size());
    }
  }

  @Test
  void createConnectorIdempotent() {
    TestSupport.createCatalog(catalogService, "cat-idem", "");
    var spec =
        ConnectorSpec.newBuilder()
            .setDisplayName("idem-1")
            .setKind(ConnectorKind.CK_UNITY)
            .setUri("dummy://x")
            .setSource(source(List.of("a", "b")))
            .setDestination(dest("cat-idem"))
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
            .setTenantId(TestSupport.DEFAULT_SEED_TENANT)
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
    TestSupport.createCatalog(catalogService, "cat-p", "");
    for (int i = 0; i < 5; i++) {
      TestSupport.createConnector(
          connectors,
          ConnectorSpec.newBuilder()
              .setDisplayName("p-" + i)
              .setKind(ConnectorKind.CK_UNITY)
              .setUri("dummy://x")
              .setSource(source(List.of("a", "b")))
              .setDestination(dest("cat-p"))
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
    TestSupport.createCatalog(catalogService, "cat-u", "");
    var a =
        TestSupport.createConnector(
            connectors,
            ConnectorSpec.newBuilder()
                .setDisplayName("u-a")
                .setKind(ConnectorKind.CK_UNITY)
                .setUri("dummy://x")
                .setSource(source(List.of("a", "b")))
                .setDestination(dest("cat-u"))
                .build());

    var b =
        TestSupport.createConnector(
            connectors,
            ConnectorSpec.newBuilder()
                .setDisplayName("u-b")
                .setKind(ConnectorKind.CK_UNITY)
                .setUri("dummy://x")
                .setSource(source(List.of("a", "b")))
                .setDestination(dest("cat-u"))
                .build());

    // rename u-a -> u-a1
    FieldMask mask = FieldMask.newBuilder().addPaths("display_name").build();
    var ok =
        connectors.updateConnector(
            UpdateConnectorRequest.newBuilder()
                .setConnectorId(a.getResourceId())
                .setSpec(ConnectorSpec.newBuilder().setDisplayName("u-a1"))
                .setUpdateMask(mask)
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
                        .setUpdateMask(mask)
                        .build()));

    TestSupport.assertGrpcAndMc(
        ex, Status.Code.ABORTED, ErrorCode.MC_CONFLICT, "Connector \"u-a1\" already exists");
  }

  @Test
  void updateConnectorPreconditionMismatch() throws Exception {
    TestSupport.createCatalog(catalogService, "cat-pre", "");
    var c =
        TestSupport.createConnector(
            connectors,
            ConnectorSpec.newBuilder()
                .setDisplayName("pre-a")
                .setKind(ConnectorKind.CK_UNITY)
                .setUri("dummy://x")
                .setSource(source(List.of("a", "b")))
                .setDestination(dest("cat-pre"))
                .build());

    FieldMask mask = FieldMask.newBuilder().addPaths("uri").build();
    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                connectors.updateConnector(
                    UpdateConnectorRequest.newBuilder()
                        .setConnectorId(c.getResourceId())
                        .setSpec(ConnectorSpec.newBuilder().setUri("dummy://changed"))
                        .setUpdateMask(mask)
                        .setPrecondition(
                            Precondition.newBuilder().setExpectedVersion(9999)) // wrong version
                        .build()));

    TestSupport.assertGrpcAndMc(
        ex, Status.Code.FAILED_PRECONDITION, ErrorCode.MC_PRECONDITION_FAILED, "Version mismatch");
  }

  @Test
  void deleteConnectorIdempotent() throws Exception {
    TestSupport.createCatalog(catalogService, "cat-del", "");
    var c =
        TestSupport.createConnector(
            connectors,
            ConnectorSpec.newBuilder()
                .setDisplayName("del-1")
                .setKind(ConnectorKind.CK_UNITY)
                .setUri("dummy://x")
                .setSource(source(List.of("a", "b")))
                .setDestination(dest("cat-del"))
                .build());

    connectors.deleteConnector(
        DeleteConnectorRequest.newBuilder().setConnectorId(c.getResourceId()).build());

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                connectors.deleteConnector(
                    DeleteConnectorRequest.newBuilder().setConnectorId(c.getResourceId()).build()));

    TestSupport.assertGrpcAndMc(
        ex, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, "Connector not found");
  }

  @Test
  void triggerReconcileNotFound() throws Exception {
    var rid =
        ResourceId.newBuilder()
            .setTenantId(TestSupport.DEFAULT_SEED_TENANT)
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
                        .setUri("dummy://x")
                        .setDestination(dest("cat-v")))
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
                                .setUri("dummy://x")
                                .setDestination(dest("cat-v")))
                        .build()));

    TestSupport.assertGrpcAndMc(
        ex, Status.Code.INVALID_ARGUMENT, ErrorCode.MC_INVALID_ARGUMENT, "Invalid argument");
  }

  private static DestinationTarget dest(String catalogDisplayName) {
    return DestinationTarget.newBuilder().setCatalogDisplayName(catalogDisplayName).build();
  }

  private static SourceSelector source(List<String> namespace) {
    return SourceSelector.newBuilder()
        .setNamespace(NamespacePath.newBuilder().addAllSegments(namespace).build())
        .build();
  }
}
