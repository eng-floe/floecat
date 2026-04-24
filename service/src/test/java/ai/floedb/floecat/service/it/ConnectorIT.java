/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.service.it;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.FileContent;
import ai.floedb.floecat.catalog.rpc.FileTargetStats;
import ai.floedb.floecat.catalog.rpc.GetIndexArtifactRequest;
import ai.floedb.floecat.catalog.rpc.IndexFileTarget;
import ai.floedb.floecat.catalog.rpc.IndexTarget;
import ai.floedb.floecat.catalog.rpc.ListTargetStatsRequest;
import ai.floedb.floecat.catalog.rpc.StatsTargetKind;
import ai.floedb.floecat.catalog.rpc.TableIndexServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableStatisticsServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ErrorCode;
import ai.floedb.floecat.common.rpc.IdempotencyKey;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.Precondition;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.connector.rpc.*;
import ai.floedb.floecat.connector.spi.ConnectorConfigMapper;
import ai.floedb.floecat.connector.spi.ConnectorFactory;
import ai.floedb.floecat.gateway.iceberg.rest.common.TestDeltaFixtures;
import ai.floedb.floecat.gateway.iceberg.rest.common.TestS3Fixtures;
import ai.floedb.floecat.reconciler.impl.ReconcilerScheduler;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.rpc.CaptureMode;
import ai.floedb.floecat.reconciler.rpc.CaptureScope;
import ai.floedb.floecat.reconciler.rpc.GetReconcileJobRequest;
import ai.floedb.floecat.reconciler.rpc.ReconcileControlGrpc;
import ai.floedb.floecat.reconciler.rpc.StartCaptureRequest;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.repo.impl.*;
import ai.floedb.floecat.service.repo.impl.AccountRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import ai.floedb.floecat.storage.spi.BlobStore;
import com.google.protobuf.FieldMask;
import com.sun.net.httpserver.HttpServer;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.SeekableInputStream;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.*;

@QuarkusTest
public class ConnectorIT {
  private static final String YB_TPCDS_TABLE_DIR = "call_center-78092955d9dc452fbe14ab11d90a85ce";
  private static final String YB_TPCDS_METADATA_LOCATION =
      "s3://yb-iceberg-tpcds/"
          + YB_TPCDS_TABLE_DIR
          + "/metadata/00002-de3da00d-3ebd-4f5c-a101-ad64a8d489d0.metadata.json";
  private static final String YB_TPCDS_RUST_SIDECAR_RESOURCE =
      "/iceberg-fixtures/yb-iceberg-tpcds/"
          + YB_TPCDS_TABLE_DIR
          + "/20250821_110458_00081_wm2db-1c0c595a-f1e3-4d2b-ab20-5041cb4042b8_parquet__d53f2b42c191ac5a7f935bb7.parquet";

  @GrpcClient("floecat")
  ConnectorsGrpc.ConnectorsBlockingStub connectors;

  @GrpcClient("floecat")
  ReconcileControlGrpc.ReconcileControlBlockingStub reconcileControl;

  @GrpcClient("floecat")
  DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;

  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalogService;

  @GrpcClient("floecat")
  TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub statsService;

  @GrpcClient("floecat")
  TableIndexServiceGrpc.TableIndexServiceBlockingStub indexes;

  @Inject ReconcileJobStore jobs;
  @Inject ReconcilerScheduler scheduler;
  @Inject AccountRepository accountRepository;
  @Inject CatalogRepository catalogs;
  @Inject NamespaceRepository namespaces;
  @Inject TableRepository tables;
  @Inject ViewRepository views;
  @Inject SnapshotRepository snaps;
  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;
  @Inject BlobStore blobs;

  private ResourceId seedAccountId;

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();
    seedAccountId =
        accountRepository.getByName(TestSupport.DEFAULT_SEED_ACCOUNT).orElseThrow().getResourceId();
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
    var accountId = seedAccountId;
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
                .setSource(source(List.of("examples", "iceberg")))
                .setDestination(dest("cat-e2e"))
                .setAuth(AuthConfig.newBuilder().setScheme("none").build())
                .build());

    rid = conn.getResourceId();
    var job2 = runReconcile(rid);
    assertNotNull(job2);
    assertEquals("JS_SUCCEEDED", job2.state, () -> "job failed: " + job2.message);

    var catId = catalogs.getByName(accountId.getId(), "cat-e2e").orElseThrow().getResourceId();

    assertEquals(3, namespaces.count(accountId.getId(), catId.getId(), List.of()));

    var dbNsId =
        namespaces.getByPath(accountId.getId(), catId.getId(), List.of("db")).orElseThrow();
    var anaNsId =
        namespaces
            .getByPath(accountId.getId(), catId.getId(), List.of("examples", "iceberg"))
            .orElseThrow();

    assertEquals(
        2,
        tables
            .list(
                accountId.getId(),
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
                accountId.getId(),
                catId.getId(),
                anaNsId.getResourceId().getId(),
                50,
                "",
                new StringBuilder())
            .size());

    var anyTable =
        tables
            .list(
                accountId.getId(),
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
    var accountId = seedAccountId;
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

    var catId = catalogs.getByName(accountId.getId(), "cat-stats").orElseThrow().getResourceId();

    var dbNsId =
        namespaces.getByPath(accountId.getId(), catId.getId(), List.of("db")).orElseThrow();

    var tbl =
        tables
            .list(
                accountId.getId(),
                catId.getId(),
                dbNsId.getResourceId().getId(),
                50,
                "",
                new StringBuilder())
            .get(0)
            .getResourceId();

    var fileStats = awaitCurrentFileStats(tbl, 100, Duration.ofSeconds(30));

    assertEquals(3, fileStats.size(), "expected 3 files");

    for (var f : fileStats) {
      assertTrue(f.getColumnsCount() > 0, "file should have per-column stats");

      var byName =
          f.getColumnsList().stream()
              .filter(FileColumnStats::hasScalar)
              .collect(Collectors.toMap(cs -> cs.getScalar().getDisplayName(), cs -> cs));

      assertTrue(byName.containsKey("id"), "per-file stats should include id column");
      var idCol = byName.get("id");
      assertEquals(0L, idCol.getScalar().getNullCount());
      assertTrue(idCol.getScalar().getValueCount() > 0, "value_count should be > 0 for id");
    }
  }

  @Test
  void icebergFixtureFileStatsIncludeSequenceNumber() throws Exception {
    TestS3Fixtures.seedFixturesOnce();

    var accountId = seedAccountId;
    TestSupport.createCatalog(catalogService, "cat-iceberg-fixture", "");

    var dest =
        DestinationTarget.newBuilder()
            .setCatalogDisplayName("cat-iceberg-fixture")
            .setNamespace(NamespacePath.newBuilder().addSegments("iceberg").build())
            .setTableDisplayName("trino_test")
            .build();

    var props = new HashMap<String, String>();
    props.putAll(
        TestS3Fixtures.fileIoProperties(
            TestS3Fixtures.bucketPath().getParent().toAbsolutePath().toString()));
    props.put("external.namespace", "fixtures.simple");
    props.put("external.table-name", "trino_test");
    props.put("stats.ndv.enabled", "false");
    props.put("iceberg.source", "filesystem");
    String metadataLocation =
        TestS3Fixtures.bucketUri(
            "metadata/00002-503f4508-3824-4cb6-bdf1-4bd6bf5a0ade.metadata.json");

    var conn =
        TestSupport.createConnector(
            connectors,
            ConnectorSpec.newBuilder()
                .setDisplayName("fixture-iceberg-simple")
                .setKind(ConnectorKind.CK_ICEBERG)
                .setUri(metadataLocation)
                .setSource(source(List.of("fixtures", "simple")))
                .setDestination(dest)
                .setAuth(AuthConfig.newBuilder().setScheme("none").build())
                .putAllProperties(props)
                .build());

    var job = runReconcile(conn.getResourceId());
    assertNotNull(job);
    assertEquals("JS_SUCCEEDED", job.state, () -> "job failed: " + job.message);

    var catId =
        catalogs.getByName(accountId.getId(), "cat-iceberg-fixture").orElseThrow().getResourceId();
    var ns =
        namespaces.getByPath(accountId.getId(), catId.getId(), List.of("iceberg")).orElseThrow();
    var table =
        tables
            .getByName(accountId.getId(), catId.getId(), ns.getResourceId().getId(), "trino_test")
            .orElseThrow();

    var fileStats = awaitCurrentFileStats(table.getResourceId(), 200, Duration.ofSeconds(30));

    assertTrue(fileStats.size() > 0, "expected file stats for iceberg fixture");
    boolean hasSeq =
        fileStats.stream().anyMatch(f -> f.hasSequenceNumber() && f.getSequenceNumber() > 0);
    assertTrue(hasSeq, "expected at least one file with sequence_number");
  }

  @Test
  void icebergFixtureSnapshotPlanningProducesRealParquetFileGroups() throws Exception {
    TestS3Fixtures.seedFixturesOnce();

    var accountId = seedAccountId;
    TestSupport.createCatalog(catalogService, "cat-iceberg-plan-files", "");

    var dest =
        DestinationTarget.newBuilder()
            .setCatalogDisplayName("cat-iceberg-plan-files")
            .setNamespace(NamespacePath.newBuilder().addSegments("iceberg").build())
            .setTableDisplayName("trino_test")
            .build();

    var props = new HashMap<String, String>();
    props.putAll(
        TestS3Fixtures.fileIoProperties(
            TestS3Fixtures.bucketPath().getParent().toAbsolutePath().toString()));
    props.put("external.namespace", "fixtures.simple");
    props.put("external.table-name", "trino_test");
    props.put("stats.ndv.enabled", "false");
    props.put("iceberg.source", "filesystem");
    String metadataLocation =
        TestS3Fixtures.bucketUri(
            "metadata/00002-503f4508-3824-4cb6-bdf1-4bd6bf5a0ade.metadata.json");

    var conn =
        TestSupport.createConnector(
            connectors,
            ConnectorSpec.newBuilder()
                .setDisplayName("fixture-iceberg-plan-files")
                .setKind(ConnectorKind.CK_ICEBERG)
                .setUri(metadataLocation)
                .setSource(source(List.of("fixtures", "simple")))
                .setDestination(dest)
                .setAuth(AuthConfig.newBuilder().setScheme("none").build())
                .putAllProperties(props)
                .build());

    var planJob = runReconcile(conn.getResourceId(), true, null, true);
    assertNotNull(planJob);
    assertEquals("JS_SUCCEEDED", planJob.state, () -> "plan job failed: " + planJob.message);
    awaitAggregatePlanJob(planJob, System.nanoTime() + Duration.ofSeconds(300).toNanos());

    var tablePlanJobs =
        awaitJobsTerminal(
            jobs.childJobs(accountId.getId(), planJob.jobId).stream()
                .filter(job -> job.jobKind == ReconcileJobKind.PLAN_TABLE)
                .toList(),
            System.nanoTime() + Duration.ofSeconds(300).toNanos());
    var snapshotPlanJobs =
        awaitJobsTerminal(
            tablePlanJobs.stream()
                .flatMap(job -> jobs.childJobs(accountId.getId(), job.jobId).stream())
                .filter(job -> job.jobKind == ReconcileJobKind.PLAN_SNAPSHOT)
                .toList(),
            System.nanoTime() + Duration.ofSeconds(300).toNanos());
    var fileGroupJobs =
        awaitJobsTerminal(
            snapshotPlanJobs.stream()
                .flatMap(job -> jobs.childJobs(accountId.getId(), job.jobId).stream())
                .filter(job -> job.jobKind == ReconcileJobKind.EXEC_FILE_GROUP)
                .toList(),
            System.nanoTime() + Duration.ofSeconds(300).toNanos());

    assertFalse(fileGroupJobs.isEmpty(), "expected EXEC_FILE_GROUP jobs for fixture snapshots");
    assertTrue(
        fileGroupJobs.stream()
            .allMatch(job -> "JS_SUCCEEDED".equals(job.state) && job.fileGroupTask != null),
        "expected file-group jobs to succeed with payloads");
    assertTrue(
        fileGroupJobs.stream().mapToLong(job -> job.statsProcessed).sum() > 0,
        "expected file-group jobs to persist file-target stats");
    assertTrue(
        fileGroupJobs.stream()
            .allMatch(
                job ->
                    !job.fileGroupTask.fileResults().isEmpty()
                        && job.fileGroupTask.fileResults().stream()
                            .allMatch(
                                file ->
                                    file.state()
                                        == ai.floedb.floecat.reconciler.jobs.ReconcileFileResult
                                            .State.SUCCEEDED)),
        "expected file-group jobs to persist per-file success results");

    var selectedSnapshotPlan = snapshotPlanJobs.getFirst();
    var selectedSnapshotFileGroups =
        fileGroupJobs.stream()
            .filter(job -> selectedSnapshotPlan.jobId.equals(job.parentJobId))
            .toList();
    var snapshotJobResponse =
        reconcileControl.getReconcileJob(
            GetReconcileJobRequest.newBuilder().setJobId(selectedSnapshotPlan.jobId).build());
    assertEquals(
        selectedSnapshotPlan.snapshotTask.fileGroups().size(),
        snapshotJobResponse.getFileGroupsTotal(),
        "expected snapshot response to expose planned file-group count");
    assertEquals(
        selectedSnapshotPlan.snapshotTask.fileGroups().stream()
            .mapToLong(group -> group.filePaths().size())
            .sum(),
        snapshotJobResponse.getFilesTotal(),
        "expected snapshot response to expose planned file count");
    assertEquals(
        selectedSnapshotFileGroups.size(),
        snapshotJobResponse.getFileGroupsCompleted(),
        "expected snapshot response to expose completed file-group count");

    var filePaths =
        snapshotPlanJobs.stream()
            .flatMap(job -> job.snapshotTask.fileGroups().stream())
            .flatMap(group -> group.filePaths().stream())
            .collect(Collectors.toCollection(ArrayList::new));
    assertFalse(filePaths.isEmpty(), "expected planned parquet file paths");
    assertTrue(
        filePaths.stream().noneMatch(path -> path.startsWith("snapshot://")),
        "expected connector-native planning instead of synthetic snapshot handles");
    assertTrue(
        filePaths.stream().allMatch(path -> path.endsWith(".parquet")),
        "expected parquet file paths in file-group tasks");
    assertTrue(
        filePaths.stream().anyMatch(path -> path.contains("/data/")),
        "expected iceberg fixture data file paths");
  }

  @Test
  void icebergRustSidecarFixtureMatchesJavaIndexerOutput() throws Exception {
    TestS3Fixtures.seedFixturesOnce();

    var accountId = seedAccountId;
    TestSupport.createCatalog(catalogService, "cat-iceberg-rust-sidecar", "");

    var dest =
        DestinationTarget.newBuilder()
            .setCatalogDisplayName("cat-iceberg-rust-sidecar")
            .setNamespace(NamespacePath.newBuilder().addSegments("iceberg").build())
            .setTableDisplayName("call_center")
            .build();

    var props = new HashMap<String, String>();
    props.putAll(
        TestS3Fixtures.fileIoProperties(
            TestS3Fixtures.bucketPath().getParent().toAbsolutePath().toString()));
    props.put("external.namespace", "fixtures.yb_iceberg_tpcds");
    props.put("external.table-name", "call_center");
    props.put("stats.ndv.enabled", "false");
    props.put("iceberg.source", "filesystem");

    var conn =
        TestSupport.createConnector(
            connectors,
            ConnectorSpec.newBuilder()
                .setDisplayName("fixture-iceberg-rust-sidecar")
                .setKind(ConnectorKind.CK_ICEBERG)
                .setUri(YB_TPCDS_METADATA_LOCATION)
                .setSource(source(List.of("fixtures", "yb_iceberg_tpcds")))
                .setDestination(dest)
                .setAuth(AuthConfig.newBuilder().setScheme("none").build())
                .putAllProperties(props)
                .build());

    var job = runReconcile(conn.getResourceId(), true);
    assertNotNull(job);
    assertEquals("JS_SUCCEEDED", job.state, () -> "job failed: " + job.message);

    var catId =
        catalogs
            .getByName(accountId.getId(), "cat-iceberg-rust-sidecar")
            .orElseThrow()
            .getResourceId();
    var ns =
        namespaces.getByPath(accountId.getId(), catId.getId(), List.of("iceberg")).orElseThrow();
    var table =
        tables
            .getByName(accountId.getId(), catId.getId(), ns.getResourceId().getId(), "call_center")
            .orElseThrow();

    var fileStats = awaitCurrentFileStats(table.getResourceId(), 20, Duration.ofSeconds(30));
    assertEquals(1, fileStats.size(), "expected one current data file in yb fixture");
    String filePath = fileStats.getFirst().getFilePath();

    var artifact =
        awaitCurrentIndexArtifact(table.getResourceId(), filePath, Duration.ofSeconds(30));

    byte[] javaSidecarBytes = blobs.get(artifact.getRecord().getArtifactUri());
    byte[] rustSidecarBytes = readFixtureResourceBytes(YB_TPCDS_RUST_SIDECAR_RESOURCE);

    SidecarContents javaSidecar = readSidecar(javaSidecarBytes);
    SidecarContents rustSidecar = readSidecar(rustSidecarBytes);
    Set<String> rustColumns =
        rustSidecar.rows().stream()
            .map(row -> row.get("column_name"))
            .filter(name -> name != null && !name.isBlank())
            .collect(Collectors.toSet());
    List<Map<String, String>> filteredJavaRows =
        javaSidecar.rows().stream()
            .filter(row -> rustColumns.contains(row.get("column_name")))
            .toList();

    assertEquals("parquet", artifact.getRecord().getArtifactFormat());
    assertEquals(filePath, javaSidecar.metadata().get("sidecar.data_file_path"));
    assertEquals(
        rustSidecar.metadata().get("sidecar.data_file_path"),
        javaSidecar.metadata().get("sidecar.data_file_path"));
    assertEquals(rustSidecar.schema(), javaSidecar.schema(), "expected matching sidecar schema");
    assertEquals(rustSidecar.rows(), filteredJavaRows, "expected Java sidecar to match Rust");
  }

  @Test
  void metadataAndStatsReturnsBeforeFollowUpStatsAreVisible() throws Exception {
    TestS3Fixtures.seedFixturesOnce();

    var accountId = seedAccountId;
    TestSupport.createCatalog(catalogService, "cat-iceberg-deferred-contract", "");

    var dest =
        DestinationTarget.newBuilder()
            .setCatalogDisplayName("cat-iceberg-deferred-contract")
            .setNamespace(NamespacePath.newBuilder().addSegments("iceberg").build())
            .setTableDisplayName("trino_test")
            .build();

    var props = new HashMap<String, String>();
    props.putAll(
        TestS3Fixtures.fileIoProperties(
            TestS3Fixtures.bucketPath().getParent().toAbsolutePath().toString()));
    props.put("external.namespace", "fixtures.simple");
    props.put("external.table-name", "trino_test");
    props.put("stats.ndv.enabled", "false");
    props.put("iceberg.source", "filesystem");
    String metadataLocation =
        TestS3Fixtures.bucketUri(
            "metadata/00002-503f4508-3824-4cb6-bdf1-4bd6bf5a0ade.metadata.json");

    var conn =
        TestSupport.createConnector(
            connectors,
            ConnectorSpec.newBuilder()
                .setDisplayName("fixture-iceberg-deferred-contract")
                .setKind(ConnectorKind.CK_ICEBERG)
                .setUri(metadataLocation)
                .setSource(source(List.of("fixtures", "simple")))
                .setDestination(dest)
                .setAuth(AuthConfig.newBuilder().setScheme("none").build())
                .putAllProperties(props)
                .build());

    var metadataJob = runReconcile(conn.getResourceId(), true);
    assertNotNull(metadataJob);
    assertEquals("JS_SUCCEEDED", metadataJob.state, () -> "job failed: " + metadataJob.message);
    assertEquals(
        0L,
        metadataJob.statsProcessed,
        "metadata+stats reconcile enqueues follow-up stats jobs; it does not inline stats");

    var catId =
        catalogs
            .getByName(accountId.getId(), "cat-iceberg-deferred-contract")
            .orElseThrow()
            .getResourceId();
    var ns =
        namespaces.getByPath(accountId.getId(), catId.getId(), List.of("iceberg")).orElseThrow();
    var table =
        tables
            .getByName(accountId.getId(), catId.getId(), ns.getResourceId().getId(), "trino_test")
            .orElseThrow();

    var immediate = listCurrentFileStats(table.getResourceId(), 200);
    assertTrue(
        immediate.isEmpty(),
        "metadata+stats should complete before follow-up STATS_ONLY capture makes stats visible");

    var eventual = awaitCurrentFileStats(table.getResourceId(), 200, Duration.ofSeconds(30));
    assertFalse(eventual.isEmpty(), "expected follow-up STATS_ONLY job to materialize file stats");
  }

  @Test
  void icebergFixtureIncrementalReconcileSkipsAlreadyIngestedSnapshots() throws Exception {
    TestS3Fixtures.seedFixturesOnce();

    var accountId = seedAccountId;
    TestSupport.createCatalog(catalogService, "cat-iceberg-incremental", "");

    var dest =
        DestinationTarget.newBuilder()
            .setCatalogDisplayName("cat-iceberg-incremental")
            .setNamespace(NamespacePath.newBuilder().addSegments("iceberg").build())
            .setTableDisplayName("trino_test")
            .build();

    var props = new HashMap<String, String>();
    props.putAll(
        TestS3Fixtures.fileIoProperties(
            TestS3Fixtures.bucketPath().getParent().toAbsolutePath().toString()));
    props.put("external.namespace", "fixtures.simple");
    props.put("external.table-name", "trino_test");
    props.put("stats.ndv.enabled", "false");
    props.put("iceberg.source", "filesystem");
    String metadataLocation =
        TestS3Fixtures.bucketUri(
            "metadata/00002-503f4508-3824-4cb6-bdf1-4bd6bf5a0ade.metadata.json");

    var conn =
        TestSupport.createConnector(
            connectors,
            ConnectorSpec.newBuilder()
                .setDisplayName("fixture-iceberg-incremental")
                .setKind(ConnectorKind.CK_ICEBERG)
                .setUri(metadataLocation)
                .setSource(source(List.of("fixtures", "simple")))
                .setDestination(dest)
                .setAuth(AuthConfig.newBuilder().setScheme("none").build())
                .putAllProperties(props)
                .build());

    var fullJob = runReconcile(conn.getResourceId(), true);
    assertNotNull(fullJob);
    assertEquals("JS_SUCCEEDED", fullJob.state, () -> "job failed: " + fullJob.message);
    assertTrue(fullJob.fullRescan);
    assertTrue(fullJob.snapshotsProcessed > 0, "expected full reconcile to process snapshots");
    assertEquals(
        0L,
        fullJob.statsProcessed,
        "metadata+stats reconcile enqueues follow-up stats jobs; it does not inline stats");

    var catId =
        catalogs
            .getByName(accountId.getId(), "cat-iceberg-incremental")
            .orElseThrow()
            .getResourceId();
    var ns =
        namespaces.getByPath(accountId.getId(), catId.getId(), List.of("iceberg")).orElseThrow();
    var table =
        tables
            .getByName(accountId.getId(), catId.getId(), ns.getResourceId().getId(), "trino_test")
            .orElseThrow();

    int snapshotCountAfterFull = snaps.count(table.getResourceId());
    assertTrue(snapshotCountAfterFull > 0, "expected snapshots to be materialized after full run");

    var incrementalJob = runReconcile(conn.getResourceId(), false);
    assertNotNull(incrementalJob);
    assertEquals(
        "JS_SUCCEEDED", incrementalJob.state, () -> "job failed: " + incrementalJob.message);
    assertFalse(incrementalJob.fullRescan);
    assertEquals(0L, incrementalJob.snapshotsProcessed, "incremental should find no new snapshots");
    assertEquals(0L, incrementalJob.statsProcessed, "incremental should process no new stats");
    assertEquals(snapshotCountAfterFull, snaps.count(table.getResourceId()));
  }

  @Test
  void icebergFixtureIncrementalReconcileCapturesStatsForNewSnapshot() throws Exception {
    TestS3Fixtures.seedFixturesOnce();

    var accountId = seedAccountId;
    TestSupport.createCatalog(catalogService, "cat-iceberg-incremental-advance", "");

    var dest =
        DestinationTarget.newBuilder()
            .setCatalogDisplayName("cat-iceberg-incremental-advance")
            .setNamespace(NamespacePath.newBuilder().addSegments("iceberg").build())
            .setTableDisplayName("trino_test")
            .build();

    var props = new HashMap<String, String>();
    props.putAll(
        TestS3Fixtures.fileIoProperties(
            TestS3Fixtures.bucketPath().getParent().toAbsolutePath().toString()));
    props.put("external.namespace", "fixtures.simple");
    props.put("external.table-name", "trino_test");
    props.put("stats.ndv.enabled", "false");
    props.put("iceberg.source", "filesystem");
    String metadataLocation =
        TestS3Fixtures.bucketUri(
            "metadata/00000-16393a9a-3433-440c-98f4-fe023ed03973.metadata.json");

    var conn =
        TestSupport.createConnector(
            connectors,
            ConnectorSpec.newBuilder()
                .setDisplayName("fixture-iceberg-incremental-advance")
                .setKind(ConnectorKind.CK_ICEBERG)
                .setUri(metadataLocation)
                .setSource(source(List.of("fixtures", "simple")))
                .setDestination(dest)
                .setAuth(AuthConfig.newBuilder().setScheme("none").build())
                .putAllProperties(props)
                .build());

    var fullJob = runReconcile(conn.getResourceId(), true);
    assertNotNull(fullJob);
    assertEquals("JS_SUCCEEDED", fullJob.state, () -> "job failed: " + fullJob.message);
    assertTrue(fullJob.fullRescan);
    assertEquals(
        1L,
        fullJob.snapshotsProcessed,
        "expected initial fixture reconcile to process one snapshot");

    var catId =
        catalogs
            .getByName(accountId.getId(), "cat-iceberg-incremental-advance")
            .orElseThrow()
            .getResourceId();
    var ns =
        namespaces.getByPath(accountId.getId(), catId.getId(), List.of("iceberg")).orElseThrow();
    var table =
        tables
            .getByName(accountId.getId(), catId.getId(), ns.getResourceId().getId(), "trino_test")
            .orElseThrow();

    assertEquals(
        1, snaps.count(table.getResourceId()), "expected one snapshot after initial reconcile");

    conn =
        updateConnectorUri(
            conn.getResourceId(),
            TestS3Fixtures.bucketUri(
                "metadata/00001-084f601d-8c4e-4315-8747-5152a12ad2ea.metadata.json"));

    var incrementalJob = runReconcile(conn.getResourceId(), false);
    assertNotNull(incrementalJob);
    assertEquals(
        "JS_SUCCEEDED", incrementalJob.state, () -> "job failed: " + incrementalJob.message);
    assertFalse(incrementalJob.fullRescan);
    assertEquals(
        1L, incrementalJob.snapshotsProcessed, "incremental should ingest one new snapshot");
    assertEquals(
        0L,
        incrementalJob.statsProcessed,
        "metadata+stats reconcile enqueues follow-up stats jobs; it does not inline stats");
    assertEquals(
        2,
        snaps.count(table.getResourceId()),
        "expected second snapshot after incremental reconcile");

    var fileResp = awaitCurrentFileStats(table.getResourceId(), 200, Duration.ofSeconds(30));
    assertTrue(
        fileResp.size() > 0, "expected current snapshot file stats after incremental reconcile");
  }

  @Test
  void icebergFixtureIncrementalReconcileCapturesDeleteStatsForNewSnapshot() throws Exception {
    TestS3Fixtures.seedFixturesOnce();

    var accountId = seedAccountId;
    TestSupport.createCatalog(catalogService, "cat-iceberg-incremental-delete", "");

    var dest =
        DestinationTarget.newBuilder()
            .setCatalogDisplayName("cat-iceberg-incremental-delete")
            .setNamespace(NamespacePath.newBuilder().addSegments("iceberg").build())
            .setTableDisplayName("trino_test")
            .build();

    var props = new HashMap<String, String>();
    props.putAll(
        TestS3Fixtures.fileIoProperties(
            TestS3Fixtures.bucketPath().getParent().toAbsolutePath().toString()));
    props.put("external.namespace", "fixtures.simple");
    props.put("external.table-name", "trino_test");
    props.put("stats.ndv.enabled", "false");
    props.put("iceberg.source", "filesystem");
    String metadataLocation =
        TestS3Fixtures.bucketUri(
            "metadata/00001-084f601d-8c4e-4315-8747-5152a12ad2ea.metadata.json");

    var conn =
        TestSupport.createConnector(
            connectors,
            ConnectorSpec.newBuilder()
                .setDisplayName("fixture-iceberg-incremental-delete")
                .setKind(ConnectorKind.CK_ICEBERG)
                .setUri(metadataLocation)
                .setSource(source(List.of("fixtures", "simple")))
                .setDestination(dest)
                .setAuth(AuthConfig.newBuilder().setScheme("none").build())
                .putAllProperties(props)
                .build());

    var fullJob = runReconcile(conn.getResourceId(), true);
    assertNotNull(fullJob);
    assertEquals("JS_SUCCEEDED", fullJob.state, () -> "job failed: " + fullJob.message);
    assertTrue(fullJob.fullRescan);
    assertEquals(
        2L,
        fullJob.snapshotsProcessed,
        "expected initial fixture reconcile to process two snapshots");

    var catId =
        catalogs
            .getByName(accountId.getId(), "cat-iceberg-incremental-delete")
            .orElseThrow()
            .getResourceId();
    var ns =
        namespaces.getByPath(accountId.getId(), catId.getId(), List.of("iceberg")).orElseThrow();
    var table =
        tables
            .getByName(accountId.getId(), catId.getId(), ns.getResourceId().getId(), "trino_test")
            .orElseThrow();

    assertEquals(
        2, snaps.count(table.getResourceId()), "expected two snapshots after initial reconcile");

    conn =
        updateConnectorUri(
            conn.getResourceId(),
            TestS3Fixtures.bucketUri(
                "metadata/00002-503f4508-3824-4cb6-bdf1-4bd6bf5a0ade.metadata.json"));

    var incrementalJob = runReconcile(conn.getResourceId(), false);
    assertNotNull(incrementalJob);
    assertEquals(
        "JS_SUCCEEDED", incrementalJob.state, () -> "job failed: " + incrementalJob.message);
    assertFalse(incrementalJob.fullRescan);
    assertEquals(
        1L, incrementalJob.snapshotsProcessed, "incremental should ingest one delete snapshot");
    assertEquals(
        0L,
        incrementalJob.statsProcessed,
        "metadata+stats reconcile enqueues follow-up stats jobs; it does not inline stats");
    assertEquals(
        3,
        snaps.count(table.getResourceId()),
        "expected third snapshot after incremental reconcile");

    var fileResp = awaitCurrentFileStats(table.getResourceId(), 200, Duration.ofSeconds(30));
    assertTrue(
        fileResp.stream().anyMatch(f -> f.getFileContent() == FileContent.FC_POSITION_DELETES),
        "expected current snapshot to expose a position delete file");
  }

  @Test
  void icebergComplexFixtureGeneratesStats() throws Exception {
    TestS3Fixtures.seedFixturesOnce();

    var accountId = seedAccountId;
    TestSupport.createCatalog(catalogService, "cat-iceberg-complex", "");

    var dest =
        DestinationTarget.newBuilder()
            .setCatalogDisplayName("cat-iceberg-complex")
            .setNamespace(NamespacePath.newBuilder().addSegments("sales").addSegments("us").build())
            .setTableDisplayName("trino_types")
            .build();

    var props = new HashMap<String, String>();
    props.putAll(
        TestS3Fixtures.fileIoProperties(
            TestS3Fixtures.bucketPath().getParent().toAbsolutePath().toString()));
    props.put("external.namespace", "sales.us");
    props.put("external.table-name", "trino_types");
    props.put("stats.ndv.enabled", "false");
    props.put("iceberg.source", "filesystem");
    String metadataLocation =
        "s3://floecat/sales/us/trino_types/metadata/00001-d751d7ce-209e-443e-9937-c25e2a08fc29.metadata.json";

    var conn =
        TestSupport.createConnector(
            connectors,
            ConnectorSpec.newBuilder()
                .setDisplayName("fixture-iceberg-complex")
                .setKind(ConnectorKind.CK_ICEBERG)
                .setUri(metadataLocation)
                .setSource(source(List.of("sales", "us")))
                .setDestination(dest)
                .setAuth(AuthConfig.newBuilder().setScheme("none").build())
                .putAllProperties(props)
                .build());

    var job = runReconcile(conn.getResourceId(), true);
    assertNotNull(job);
    assertEquals("JS_SUCCEEDED", job.state, () -> "job failed: " + job.message);
    assertTrue(job.fullRescan);
    assertTrue(job.tablesScanned > 0, "expected complex fixture reconcile to scan tables");
    assertTrue(
        job.tablesChanged > 0, "expected complex fixture reconcile to persist table updates");
    assertTrue(
        job.snapshotsProcessed > 0, "expected complex fixture reconcile to process snapshots");
    assertEquals(
        0L,
        job.statsProcessed,
        "metadata+stats reconcile enqueues follow-up stats jobs; it does not inline stats");

    var catId =
        catalogs.getByName(accountId.getId(), "cat-iceberg-complex").orElseThrow().getResourceId();
    var ns =
        namespaces
            .getByPath(accountId.getId(), catId.getId(), List.of("sales", "us"))
            .orElseThrow();
    var table =
        tables
            .getByName(accountId.getId(), catId.getId(), ns.getResourceId().getId(), "trino_types")
            .orElseThrow();

    var fileResp = awaitCurrentFileStats(table.getResourceId(), 200, Duration.ofSeconds(30));

    assertFalse(fileResp.isEmpty(), "expected file stats for complex iceberg fixture");

    var columnNames =
        fileResp.stream()
            .flatMap(f -> f.getColumnsList().stream())
            .filter(FileColumnStats::hasScalar)
            .map(c -> c.getScalar().getDisplayName())
            .collect(Collectors.toSet());
    assertTrue(columnNames.contains("c_time"), "expected TIME column stats to be materialized");
    assertTrue(columnNames.contains("c_ts"), "expected TIMESTAMP column stats to be materialized");
    assertTrue(
        columnNames.contains("c_tstz"), "expected TIMESTAMPTZ column stats to be materialized");
  }

  @Test
  void dummyConnectorRespectsDestinationTableDisplayName() throws Exception {
    var accountId = seedAccountId;
    TestSupport.createCatalog(catalogService, "cat-dest-table", "");

    var dest =
        DestinationTarget.newBuilder()
            .setCatalogDisplayName("cat-dest-table")
            .setNamespace(
                NamespacePath.newBuilder().addSegments("examples").addSegments("iceberg").build())
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
        catalogs.getByName(accountId.getId(), "cat-dest-table").orElseThrow().getResourceId();

    var ns =
        namespaces
            .getByPath(accountId.getId(), catId.getId(), List.of("examples", "iceberg"))
            .orElseThrow();

    var outTables =
        tables.list(
            accountId.getId(),
            catId.getId(),
            ns.getResourceId().getId(),
            50,
            "",
            new StringBuilder());

    assertEquals(1, outTables.size(), "expected exactly one table in destination namespace");
    assertEquals("my_events_copy", outTables.get(0).getDisplayName());
  }

  @Test
  void splitScopedReconcileMissingDestinationTableIdReturnsExplicitFailure() throws Exception {
    TestSupport.createCatalog(catalogService, "cat-scope-miss", "");

    var conn =
        TestSupport.createConnector(
            connectors,
            ConnectorSpec.newBuilder()
                .setDisplayName("dummy-scope-miss")
                .setKind(ConnectorKind.CK_UNITY)
                .setUri("dummy://ignored")
                .setSource(source(List.of("db")))
                .setDestination(dest("cat-scope-miss"))
                .setAuth(AuthConfig.newBuilder().setScheme("none").build())
                .build());

    var job =
        runReconcile(
            conn.getResourceId(),
            true,
            CaptureScope.newBuilder()
                .setConnectorId(conn.getResourceId())
                .setDestinationTableId("missing_table")
                .build(),
            false);

    assertNotNull(job);
    assertEquals("JS_FAILED", job.state);
    assertTrue(
        job.message.contains("Destination table id does not exist: missing_table"), job.message);
  }

  @Test
  void splitPhasePlanJobSyncsViewsOncePerTopLevelJob() throws Exception {
    var accountId = seedAccountId;
    TestSupport.createCatalog(catalogService, "cat-plan-views", "");

    var conn =
        TestSupport.createConnector(
            connectors,
            ConnectorSpec.newBuilder()
                .setDisplayName("dummy-plan-views")
                .setKind(ConnectorKind.CK_UNITY)
                .setUri("dummy://ignored")
                .setSource(source(List.of("db")))
                .setDestination(
                    DestinationTarget.newBuilder()
                        .setCatalogDisplayName("cat-plan-views")
                        .setNamespace(NamespacePath.newBuilder().addSegments("analytics").build())
                        .build())
                .setAuth(AuthConfig.newBuilder().setScheme("none").build())
                .putProperties("dummy.view.enabled", "true")
                .putProperties("dummy.view.namespace", "db")
                .putProperties("dummy.view.name", "events_summary")
                .putProperties("dummy.view.sql", "SELECT id FROM db.events")
                .build());

    var planJob = runReconcile(conn.getResourceId(), true, null, true);

    assertNotNull(planJob);
    assertEquals("JS_SUCCEEDED", planJob.state, () -> "plan job failed: " + planJob.message);
    assertEquals(2L, planJob.tablesScanned, "expected 2 planned tables");
    assertEquals(1L, planJob.viewsScanned, "expected 1 planned view");
    assertEquals(0L, planJob.tablesChanged, "plan job should not mutate destination objects");
    assertEquals(0L, planJob.viewsChanged, "plan job should not mutate destination objects");

    var aggregatedJob =
        awaitAggregatePlanJob(planJob, System.nanoTime() + Duration.ofSeconds(300).toNanos());
    assertEquals(
        "JS_SUCCEEDED",
        aggregatedJob.state,
        () -> "aggregate job failed: " + aggregatedJob.message);
    assertEquals(2L, aggregatedJob.tablesScanned, "expected 2 executed tables");
    assertEquals(2L, aggregatedJob.tablesChanged, "expected two newly created destination tables");
    assertEquals(1L, aggregatedJob.viewsScanned, "expected 1 executed view");
    assertEquals(1L, aggregatedJob.viewsChanged, "expected one newly created destination view");

    var childJobs = jobs.childJobs(accountId.getId(), planJob.jobId);
    assertEquals(3, childJobs.size(), "expected child jobs for 2 tables plus 1 view");
    assertEquals(
        2L,
        childJobs.stream().filter(job -> job.jobKind == ReconcileJobKind.PLAN_TABLE).count(),
        "expected one child PLAN_TABLE job per planned table");
    assertEquals(
        1L,
        childJobs.stream().filter(job -> job.jobKind == ReconcileJobKind.PLAN_VIEW).count(),
        "expected one child PLAN_VIEW job for planned views");

    var catId =
        catalogs.getByName(accountId.getId(), "cat-plan-views").orElseThrow().getResourceId();
    var ns =
        namespaces.getByPath(accountId.getId(), catId.getId(), List.of("analytics")).orElseThrow();

    assertEquals(1, views.count(accountId.getId(), catId.getId(), ns.getResourceId().getId()));
    assertTrue(
        views
            .getByName(
                accountId.getId(), catId.getId(), ns.getResourceId().getId(), "events_summary")
            .isPresent());
  }

  @Test
  void secondReconcilePlansSnapshotJobsForExistingTables() throws Exception {
    var accountId = seedAccountId;
    TestSupport.createCatalog(catalogService, "cat-plan-snapshots", "");

    var conn =
        TestSupport.createConnector(
            connectors,
            ConnectorSpec.newBuilder()
                .setDisplayName("dummy-plan-snapshots")
                .setKind(ConnectorKind.CK_UNITY)
                .setUri("dummy://ignored")
                .setSource(source(List.of("db")))
                .setDestination(
                    DestinationTarget.newBuilder()
                        .setCatalogDisplayName("cat-plan-snapshots")
                        .setNamespace(NamespacePath.newBuilder().addSegments("analytics").build())
                        .build())
                .setAuth(AuthConfig.newBuilder().setScheme("none").build())
                .build());

    var initialJob = runReconcile(conn.getResourceId(), true);
    assertNotNull(initialJob);
    assertEquals(
        "JS_SUCCEEDED",
        initialJob.state,
        () -> "initial aggregate job failed: " + initialJob.message);

    var planJob = runReconcile(conn.getResourceId(), true, null, true);
    assertNotNull(planJob);
    assertEquals("JS_SUCCEEDED", planJob.state, () -> "plan job failed: " + planJob.message);

    var aggregatedJob =
        awaitAggregatePlanJob(planJob, System.nanoTime() + Duration.ofSeconds(300).toNanos());
    assertEquals(
        "JS_SUCCEEDED",
        aggregatedJob.state,
        () -> "aggregate job failed: " + aggregatedJob.message);
    assertEquals(2L, aggregatedJob.tablesScanned, "expected 2 executed tables");
    var childJobs = jobs.childJobs(accountId.getId(), planJob.jobId);
    assertEquals(2, childJobs.size(), "expected only table planning jobs under PLAN_CONNECTOR");
    assertEquals(
        2L,
        childJobs.stream().filter(job -> job.jobKind == ReconcileJobKind.PLAN_TABLE).count(),
        "expected one child PLAN_TABLE job per planned table");
    assertEquals(
        0L,
        childJobs.stream().filter(job -> job.jobKind == ReconcileJobKind.PLAN_SNAPSHOT).count(),
        "expected snapshot planning to be owned by PLAN_TABLE jobs");

    var tablePlanJobs =
        childJobs.stream().filter(job -> job.jobKind == ReconcileJobKind.PLAN_TABLE).toList();
    var snapshotPlanJobs =
        tablePlanJobs.stream()
            .flatMap(job -> jobs.childJobs(accountId.getId(), job.jobId).stream())
            .filter(job -> job.jobKind == ReconcileJobKind.PLAN_SNAPSHOT)
            .toList();
    var completedSnapshotPlanJobs =
        awaitJobsTerminal(snapshotPlanJobs, System.nanoTime() + Duration.ofSeconds(300).toNanos());
    assertEquals(
        2, completedSnapshotPlanJobs.size(), "expected one PLAN_SNAPSHOT grandchild per table");
    assertTrue(
        completedSnapshotPlanJobs.stream()
            .allMatch(job -> "JS_SUCCEEDED".equals(job.state) && job.snapshotTask != null),
        "expected each snapshot plan job to succeed with a snapshot task payload");

    var fileGroupJobs =
        completedSnapshotPlanJobs.stream()
            .flatMap(job -> jobs.childJobs(accountId.getId(), job.jobId).stream())
            .filter(job -> job.jobKind == ReconcileJobKind.EXEC_FILE_GROUP)
            .toList();
    var completedFileGroupJobs =
        awaitJobsTerminal(fileGroupJobs, System.nanoTime() + Duration.ofSeconds(300).toNanos());
    assertEquals(
        2, completedFileGroupJobs.size(), "expected one EXEC_FILE_GROUP child per snapshot plan");
    assertTrue(
        completedFileGroupJobs.stream()
            .allMatch(job -> "JS_SUCCEEDED".equals(job.state) && job.fileGroupTask != null),
        "expected each file-group execution job to succeed with a file group payload");
  }

  @Test
  void deltaConnectorEndToEnd() throws Exception {
    Assumptions.assumeTrue(
        TestDeltaFixtures.useAwsFixtures(), "Delta fixtures require S3/localstack");

    TestDeltaFixtures.seedFixturesOnce();

    var accountId = seedAccountId;
    TestSupport.createCatalog(catalogService, "cat-delta", "");

    try (UcStubServer uc = UcStubServer.start(TestDeltaFixtures.tableUri())) {
      var spec =
          ConnectorSpec.newBuilder()
              .setDisplayName("delta-conn")
              .setKind(ConnectorKind.CK_DELTA)
              .setUri(uc.baseUri())
              .setSource(
                  SourceSelector.newBuilder()
                      .setNamespace(
                          NamespacePath.newBuilder()
                              .addSegments("main")
                              .addSegments("tpcds")
                              .build())
                      .setTable("call_center"))
              .setDestination(dest("cat-delta"))
              .setAuth(AuthConfig.newBuilder().setScheme("none").build())
              .putAllProperties(TestDeltaFixtures.s3Options())
              .build();

      var conn = TestSupport.createConnector(connectors, spec);
      var job = runReconcile(conn.getResourceId());
      assertNotNull(job);
      assertEquals("JS_SUCCEEDED", job.state, () -> "job failed: " + job.message);

      var catId = catalogs.getByName(accountId.getId(), "cat-delta").orElseThrow().getResourceId();

      var ns =
          namespaces
              .getByPath(accountId.getId(), catId.getId(), List.of("main", "tpcds"))
              .orElseThrow();

      var outTables =
          tables.list(
              accountId.getId(),
              catId.getId(),
              ns.getResourceId().getId(),
              50,
              "",
              new StringBuilder());

      assertEquals(1, outTables.size(), "expected exactly one table in destination namespace");
      assertEquals("call_center", outTables.get(0).getDisplayName());

      var outTableId = outTables.get(0).getResourceId();
      assertTrue(snaps.getById(outTableId, 0L).isPresent(), "expected Delta snapshot_id=0");

      var fileStats =
          awaitFileStats(
              outTableId,
              SnapshotRef.newBuilder().setSnapshotId(0L).build(),
              10,
              Duration.ofSeconds(30));
      assertFalse(fileStats.isEmpty(), "expected file stats at current snapshot");
    }
  }

  @Test
  void deltaSnapshotPlanningProducesRealParquetFileGroups() throws Exception {
    Assumptions.assumeTrue(
        TestDeltaFixtures.useAwsFixtures(), "Delta fixtures require S3/localstack");

    TestDeltaFixtures.seedFixturesOnce();

    var accountId = seedAccountId;
    TestSupport.createCatalog(catalogService, "cat-delta-plan-files", "");

    try (UcStubServer uc = UcStubServer.start(TestDeltaFixtures.tableUri())) {
      var conn =
          TestSupport.createConnector(
              connectors,
              ConnectorSpec.newBuilder()
                  .setDisplayName("delta-plan-files")
                  .setKind(ConnectorKind.CK_DELTA)
                  .setUri(uc.baseUri())
                  .setSource(
                      SourceSelector.newBuilder()
                          .setNamespace(
                              NamespacePath.newBuilder()
                                  .addSegments("main")
                                  .addSegments("tpcds")
                                  .build())
                          .setTable("call_center"))
                  .setDestination(dest("cat-delta-plan-files"))
                  .setAuth(AuthConfig.newBuilder().setScheme("none").build())
                  .putAllProperties(TestDeltaFixtures.s3Options())
                  .build());

      var planJob = runReconcile(conn.getResourceId(), true, null, true);
      assertNotNull(planJob);
      assertEquals("JS_SUCCEEDED", planJob.state, () -> "plan job failed: " + planJob.message);
      awaitAggregatePlanJob(planJob, System.nanoTime() + Duration.ofSeconds(300).toNanos());

      var tablePlanJobs =
          awaitJobsTerminal(
              jobs.childJobs(accountId.getId(), planJob.jobId).stream()
                  .filter(job -> job.jobKind == ReconcileJobKind.PLAN_TABLE)
                  .toList(),
              System.nanoTime() + Duration.ofSeconds(300).toNanos());
      var snapshotPlanJobs =
          awaitJobsTerminal(
              tablePlanJobs.stream()
                  .flatMap(job -> jobs.childJobs(accountId.getId(), job.jobId).stream())
                  .filter(job -> job.jobKind == ReconcileJobKind.PLAN_SNAPSHOT)
                  .toList(),
              System.nanoTime() + Duration.ofSeconds(300).toNanos());
      var fileGroupJobs =
          awaitJobsTerminal(
              snapshotPlanJobs.stream()
                  .flatMap(job -> jobs.childJobs(accountId.getId(), job.jobId).stream())
                  .filter(job -> job.jobKind == ReconcileJobKind.EXEC_FILE_GROUP)
                  .toList(),
              System.nanoTime() + Duration.ofSeconds(300).toNanos());

      assertFalse(fileGroupJobs.isEmpty(), "expected EXEC_FILE_GROUP jobs for delta snapshot");
      assertTrue(
          fileGroupJobs.stream()
              .allMatch(job -> "JS_SUCCEEDED".equals(job.state) && job.fileGroupTask != null),
          "expected file-group jobs to succeed with payloads");
      assertTrue(
          fileGroupJobs.stream().mapToLong(job -> job.statsProcessed).sum() > 0,
          "expected file-group jobs to persist file-target stats");
      assertTrue(
          fileGroupJobs.stream()
              .allMatch(
                  job ->
                      !job.fileGroupTask.fileResults().isEmpty()
                          && job.fileGroupTask.fileResults().stream()
                              .allMatch(
                                  file ->
                                      file.state()
                                          == ai.floedb.floecat.reconciler.jobs.ReconcileFileResult
                                              .State.SUCCEEDED)),
          "expected file-group jobs to persist per-file success results");

      var selectedSnapshotPlan = snapshotPlanJobs.getFirst();
      var selectedSnapshotFileGroups =
          fileGroupJobs.stream()
              .filter(job -> selectedSnapshotPlan.jobId.equals(job.parentJobId))
              .toList();
      var snapshotJobResponse =
          reconcileControl.getReconcileJob(
              GetReconcileJobRequest.newBuilder().setJobId(selectedSnapshotPlan.jobId).build());
      assertEquals(
          selectedSnapshotPlan.snapshotTask.fileGroups().size(),
          snapshotJobResponse.getFileGroupsTotal(),
          "expected snapshot response to expose planned file-group count");
      assertEquals(
          selectedSnapshotPlan.snapshotTask.fileGroups().stream()
              .mapToLong(group -> group.filePaths().size())
              .sum(),
          snapshotJobResponse.getFilesTotal(),
          "expected snapshot response to expose planned file count");
      assertEquals(
          selectedSnapshotFileGroups.size(),
          snapshotJobResponse.getFileGroupsCompleted(),
          "expected snapshot response to expose completed file-group count");

      var filePaths =
          snapshotPlanJobs.stream()
              .flatMap(job -> job.snapshotTask.fileGroups().stream())
              .flatMap(group -> group.filePaths().stream())
              .collect(Collectors.toCollection(ArrayList::new));
      assertFalse(filePaths.isEmpty(), "expected planned parquet file paths");
      assertTrue(
          filePaths.stream().noneMatch(path -> path.startsWith("snapshot://")),
          "expected connector-native planning instead of synthetic snapshot handles");
      assertTrue(
          filePaths.stream().allMatch(path -> path.endsWith(".parquet")),
          "expected parquet file paths in file-group tasks");
      assertTrue(
          filePaths.stream().allMatch(path -> path.startsWith("s3://floecat-delta/call_center/")),
          "expected delta fixture data files rooted at the seeded fixture table");
    }
  }

  @Test
  void GlueIcebergRESTconnectorEndToEnd() throws Exception {
    boolean enabled =
        ConfigProvider.getConfig()
            .getOptionalValue("test.glue.enabled", Boolean.class)
            .orElse(false);
    Assumptions.assumeTrue(enabled, "Disabled by test.glue.enabled=false");

    var accountId = seedAccountId;

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
        catalogs.getByName(accountId.getId(), "glue-iceberg-rest").orElseThrow().getResourceId();

    assertEquals(1, namespaces.count(accountId.getId(), catId.getId(), List.of()));

    var tpcdsNsId =
        namespaces
            .getByPath(accountId.getId(), catId.getId(), List.of("tpcds_iceberg"))
            .orElseThrow();

    assertEquals(
        24,
        tables
            .list(
                accountId.getId(),
                catId.getId(),
                tpcdsNsId.getResourceId().getId(),
                50,
                "",
                new StringBuilder())
            .size());
  }

  ReconcileJobStore.ReconcileJob runReconcile(ResourceId rid) throws Exception {
    return runReconcile(rid, true);
  }

  private Connector updateConnectorProperties(
      ResourceId connectorId, java.util.Map<String, String> properties) {
    return connectors
        .updateConnector(
            UpdateConnectorRequest.newBuilder()
                .setConnectorId(connectorId)
                .setSpec(ConnectorSpec.newBuilder().putAllProperties(properties).build())
                .setUpdateMask(FieldMask.newBuilder().addPaths("properties").build())
                .build())
        .getConnector();
  }

  private Connector updateConnectorUri(ResourceId connectorId, String uri) {
    return connectors
        .updateConnector(
            UpdateConnectorRequest.newBuilder()
                .setConnectorId(connectorId)
                .setSpec(ConnectorSpec.newBuilder().setUri(uri).build())
                .setUpdateMask(FieldMask.newBuilder().addPaths("uri").build())
                .build())
        .getConnector();
  }

  ReconcileJobStore.ReconcileJob runReconcile(ResourceId rid, boolean fullRescan) throws Exception {
    return runReconcile(rid, fullRescan, null, false);
  }

  ReconcileJobStore.ReconcileJob runReconcile(
      ResourceId rid, boolean fullRescan, CaptureScope scope, boolean returnPlanJob)
      throws Exception {
    assertEquals(ResourceKind.RK_CONNECTOR, rid.getKind());

    var trig =
        reconcileControl.startCapture(
            StartCaptureRequest.newBuilder()
                .setScope(
                    scope == null ? CaptureScope.newBuilder().setConnectorId(rid).build() : scope)
                .setMode(CaptureMode.CM_METADATA_AND_STATS)
                .setFullRescan(fullRescan)
                .build());

    String jobId = trig.getJobId();
    assertFalse(jobId.isBlank());

    long startedAt = System.nanoTime();
    var deadline = System.nanoTime() + Duration.ofSeconds(300).toNanos();
    ReconcileJobStore.ReconcileJob job;
    for (; ; ) {
      job = jobs.get(jobId).orElse(null);
      if (job != null && isTerminal(job.state)) {
        break;
      }
      if (System.nanoTime() > deadline) {
        break;
      }
      Thread.sleep(250);
    }

    if (job == null || job.jobKind != ReconcileJobKind.PLAN_CONNECTOR) {
      return job;
    }

    if (returnPlanJob) {
      return job;
    }

    return awaitAggregatePlanJob(job, deadline);
  }

  private ReconcileJobStore.ReconcileJob awaitAggregatePlanJob(
      ReconcileJobStore.ReconcileJob planJob, long deadlineNanos) throws Exception {
    List<ReconcileJobStore.ReconcileJob> descendantJobs = List.of();
    for (; ; ) {
      descendantJobs = descendantJobsFor(planJob);
      if (!descendantJobs.isEmpty()
          && descendantJobs.stream().allMatch(job -> isTerminal(job.state))) {
        break;
      }
      if ("JS_FAILED".equals(planJob.state) || "JS_CANCELLED".equals(planJob.state)) {
        return planJob;
      }
      if (System.nanoTime() > deadlineNanos) {
        break;
      }
      Thread.sleep(250);
    }

    if (descendantJobs.isEmpty()) {
      return planJob;
    }

    if ("JS_FAILED".equals(planJob.state) || "JS_CANCELLED".equals(planJob.state)) {
      long startedAtMs =
          descendantJobs.stream()
              .mapToLong(job -> job.startedAtMs)
              .filter(v -> v > 0L)
              .min()
              .orElse(planJob.startedAtMs);
      long finishedAtMs =
          Math.max(
              planJob.finishedAtMs,
              descendantJobs.stream().mapToLong(job -> job.finishedAtMs).max().orElse(0L));
      long tablesScanned = descendantJobs.stream().mapToLong(job -> job.tablesScanned).sum();
      long tablesChanged = descendantJobs.stream().mapToLong(job -> job.tablesChanged).sum();
      long viewsScanned = descendantJobs.stream().mapToLong(job -> job.viewsScanned).sum();
      long viewsChanged = descendantJobs.stream().mapToLong(job -> job.viewsChanged).sum();
      long errors = descendantJobs.stream().mapToLong(job -> job.errors).sum();
      long snapshotsProcessed =
          descendantJobs.stream().mapToLong(job -> job.snapshotsProcessed).sum();
      long statsProcessed = descendantJobs.stream().mapToLong(job -> job.statsProcessed).sum();

      return new ReconcileJobStore.ReconcileJob(
          planJob.jobId,
          planJob.accountId,
          planJob.connectorId,
          planJob.state,
          planJob.message,
          startedAtMs,
          finishedAtMs,
          tablesScanned,
          tablesChanged,
          viewsScanned,
          viewsChanged,
          errors,
          planJob.fullRescan,
          planJob.captureMode,
          snapshotsProcessed,
          statsProcessed,
          planJob.scope,
          planJob.executionPolicy,
          planJob.executorId,
          planJob.jobKind,
          planJob.tableTask,
          planJob.parentJobId);
    }

    boolean failed = descendantJobs.stream().anyMatch(job -> "JS_FAILED".equals(job.state));
    boolean cancelled = descendantJobs.stream().anyMatch(job -> "JS_CANCELLED".equals(job.state));
    String state = failed ? "JS_FAILED" : (cancelled ? "JS_CANCELLED" : "JS_SUCCEEDED");
    String message =
        descendantJobs.stream()
            .filter(job -> job.message != null && !job.message.isBlank())
            .filter(job -> !"JS_SUCCEEDED".equals(job.state))
            .map(job -> job.jobId + ": " + job.message)
            .findFirst()
            .orElse(planJob.message);

    long startedAtMs =
        descendantJobs.stream()
            .mapToLong(job -> job.startedAtMs)
            .filter(v -> v > 0L)
            .min()
            .orElse(planJob.startedAtMs);
    long finishedAtMs =
        descendantJobs.stream()
            .mapToLong(job -> job.finishedAtMs)
            .max()
            .orElse(planJob.finishedAtMs);
    long tablesScanned = descendantJobs.stream().mapToLong(job -> job.tablesScanned).sum();
    long tablesChanged = descendantJobs.stream().mapToLong(job -> job.tablesChanged).sum();
    long viewsScanned = descendantJobs.stream().mapToLong(job -> job.viewsScanned).sum();
    long viewsChanged = descendantJobs.stream().mapToLong(job -> job.viewsChanged).sum();
    long errors = descendantJobs.stream().mapToLong(job -> job.errors).sum();
    long snapshotsProcessed =
        descendantJobs.stream().mapToLong(job -> job.snapshotsProcessed).sum();
    long statsProcessed = descendantJobs.stream().mapToLong(job -> job.statsProcessed).sum();

    return new ReconcileJobStore.ReconcileJob(
        planJob.jobId,
        planJob.accountId,
        planJob.connectorId,
        state,
        message,
        startedAtMs,
        finishedAtMs,
        tablesScanned,
        tablesChanged,
        viewsScanned,
        viewsChanged,
        errors,
        planJob.fullRescan,
        planJob.captureMode,
        snapshotsProcessed,
        statsProcessed,
        planJob.scope,
        planJob.executionPolicy,
        planJob.executorId,
        planJob.jobKind,
        planJob.tableTask,
        planJob.parentJobId);
  }

  private List<ReconcileJobStore.ReconcileJob> childJobsFor(
      ReconcileJobStore.ReconcileJob planJob) {
    List<ReconcileJobStore.ReconcileJob> out = new ArrayList<>();
    String nextToken = "";
    do {
      var page = jobs.list(planJob.accountId, 200, nextToken, planJob.connectorId, Set.of());
      for (var candidate : page.jobs) {
        if (planJob.jobId.equals(candidate.parentJobId)) {
          out.add(candidate);
        }
      }
      nextToken = page.nextPageToken;
    } while (nextToken != null && !nextToken.isBlank());
    return out;
  }

  private List<ReconcileJobStore.ReconcileJob> descendantJobsFor(
      ReconcileJobStore.ReconcileJob rootJob) {
    List<ReconcileJobStore.ReconcileJob> descendants = new ArrayList<>();
    List<ReconcileJobStore.ReconcileJob> frontier = childJobsFor(rootJob);
    while (!frontier.isEmpty()) {
      List<ReconcileJobStore.ReconcileJob> nonExecutionJobs =
          frontier.stream().filter(job -> job.jobKind != ReconcileJobKind.EXEC_FILE_GROUP).toList();
      descendants.addAll(nonExecutionJobs);
      frontier = nonExecutionJobs.stream().flatMap(job -> childJobsFor(job).stream()).toList();
    }
    return descendants;
  }

  private List<ReconcileJobStore.ReconcileJob> awaitJobsTerminal(
      List<ReconcileJobStore.ReconcileJob> jobsToAwait, long deadlineNanos) throws Exception {
    List<ReconcileJobStore.ReconcileJob> current = jobsToAwait;
    for (; ; ) {
      current = current.stream().map(job -> jobs.get(job.jobId).orElse(job)).toList();
      if (current.stream().allMatch(job -> isTerminal(job.state))) {
        return current;
      }
      if (System.nanoTime() > deadlineNanos) {
        return current;
      }
      Thread.sleep(250);
    }
  }

  private static boolean isTerminal(String state) {
    return "JS_SUCCEEDED".equals(state)
        || "JS_FAILED".equals(state)
        || "JS_CANCELLED".equals(state);
  }

  private static byte[] readFixtureResourceBytes(String resourcePath) throws Exception {
    try (InputStream in = ConnectorIT.class.getResourceAsStream(resourcePath)) {
      assertNotNull(in, () -> "fixture resource not found: " + resourcePath);
      return in.readAllBytes();
    }
  }

  private ai.floedb.floecat.catalog.rpc.GetIndexArtifactResponse awaitCurrentIndexArtifact(
      ResourceId tableId, String filePath, Duration timeout) throws Exception {
    long deadlineNanos = System.nanoTime() + timeout.toNanos();
    StatusRuntimeException last = null;
    while (System.nanoTime() <= deadlineNanos) {
      try {
        return indexes.getIndexArtifact(
            GetIndexArtifactRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshot(
                    SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT).build())
                .setTarget(
                    IndexTarget.newBuilder()
                        .setFile(IndexFileTarget.newBuilder().setFilePath(filePath).build())
                        .build())
                .build());
      } catch (StatusRuntimeException e) {
        if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
          throw e;
        }
        last = e;
      }
      Thread.sleep(250);
    }
    if (last != null) {
      throw last;
    }
    throw new AssertionError("timed out waiting for current index artifact for " + filePath);
  }

  private static SidecarContents readSidecar(byte[] bytes) throws Exception {
    InputFile inputFile = new ByteArrayInputFile(bytes);
    try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
      String schema = reader.getFooter().getFileMetaData().getSchema().toString();
      Map<String, String> metadata =
          new LinkedHashMap<>(reader.getFooter().getFileMetaData().getKeyValueMetaData());
      List<Map<String, String>> rows = new ArrayList<>();
      MessageColumnIO columnIO =
          new ColumnIOFactory().getColumnIO(reader.getFooter().getFileMetaData().getSchema());
      PageReadStore pages;
      while ((pages = reader.readNextRowGroup()) != null) {
        RecordReader<Group> recordReader =
            columnIO.getRecordReader(
                pages, new GroupRecordConverter(reader.getFooter().getFileMetaData().getSchema()));
        long rowCount = pages.getRowCount();
        for (long i = 0; i < rowCount; i++) {
          rows.add(toComparableRow(recordReader.read()));
        }
      }
      return new SidecarContents(schema, metadata, rows);
    }
  }

  private static Map<String, String> toComparableRow(Group row) {
    var schema = row.getType();
    Map<String, String> out = new LinkedHashMap<>();
    for (int i = 0; i < schema.getFieldCount(); i++) {
      String fieldName = schema.getFieldName(i);
      out.put(fieldName, row.getFieldRepetitionCount(i) == 0 ? null : row.getValueToString(i, 0));
    }
    return out;
  }

  private record SidecarContents(
      String schema, Map<String, String> metadata, List<Map<String, String>> rows) {}

  private static final class ByteArrayInputFile implements InputFile {
    private final byte[] bytes;

    private ByteArrayInputFile(byte[] bytes) {
      this.bytes = bytes;
    }

    @Override
    public long getLength() {
      return bytes.length;
    }

    @Override
    public SeekableInputStream newStream() {
      return new ByteArraySeekableInputStream(bytes);
    }
  }

  private static final class ByteArraySeekableInputStream extends SeekableInputStream {
    private final byte[] bytes;
    private int position = 0;

    private ByteArraySeekableInputStream(byte[] bytes) {
      this.bytes = bytes;
    }

    @Override
    public long getPos() {
      return position;
    }

    @Override
    public void seek(long newPos) {
      if (newPos < 0 || newPos > bytes.length) {
        throw new IllegalArgumentException("invalid seek position: " + newPos);
      }
      position = (int) newPos;
    }

    @Override
    public int read() {
      if (position >= bytes.length) {
        return -1;
      }
      return bytes[position++] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) {
      if (position >= bytes.length) {
        return -1;
      }
      int toRead = Math.min(len, bytes.length - position);
      System.arraycopy(bytes, position, b, off, toRead);
      position += toRead;
      return toRead;
    }

    @Override
    public int read(ByteBuffer dst) {
      if (position >= bytes.length) {
        return -1;
      }
      int toRead = Math.min(dst.remaining(), bytes.length - position);
      dst.put(bytes, position, toRead);
      position += toRead;
      return toRead;
    }

    @Override
    public void readFully(byte[] bytes) throws IOException {
      readFully(bytes, 0, bytes.length);
    }

    @Override
    public void readFully(byte[] bytes, int start, int len) throws IOException {
      int total = 0;
      while (total < len) {
        int n = read(bytes, start + total, len - total);
        if (n < 0) {
          throw new IOException("Unexpected EOF");
        }
        total += n;
      }
    }

    @Override
    public void readFully(ByteBuffer dst) throws IOException {
      while (dst.hasRemaining()) {
        int n = read(dst);
        if (n < 0) {
          throw new IOException("Unexpected EOF");
        }
      }
    }

    @Override
    public void close() {}
  }

  private static final class UcStubServer implements AutoCloseable {
    private final HttpServer server;
    private final String baseUri;

    private UcStubServer(HttpServer server) {
      this.server = server;
      this.baseUri = "http://localhost:" + server.getAddress().getPort();
    }

    static UcStubServer start(String storageLocation) throws IOException {
      HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);

      String catalogsJson = "{\"catalogs\":[{\"name\":\"main\"}]}";
      String schemasJson = "{\"schemas\":[{\"name\":\"tpcds\"}]}";
      String tablesJson =
          "{\"tables\":[{\"name\":\"call_center\",\"table_type\":\"TABLE\",\"data_source_format\":\"DELTA\"}]}";
      String tableJson =
          """
          {"name":"call_center","catalog_name":"main","schema_name":"tpcds","table_type":"MANAGED","data_source_format":"DELTA","storage_location":"%s","columns":[{"name":"cc_call_center_sk","type_text":"int","nullable":true},{"name":"cc_call_center_id","type_text":"string","nullable":true}]}
          """
              .formatted(storageLocation);

      server.createContext(
          "/api/2.1/unity-catalog/catalogs", exchange -> writeJson(exchange, catalogsJson));
      server.createContext(
          "/api/2.1/unity-catalog/schemas", exchange -> writeJson(exchange, schemasJson));
      server.createContext(
          "/api/2.1/unity-catalog/tables", exchange -> writeJson(exchange, tablesJson));
      server.createContext(
          "/api/2.1/unity-catalog/tables/", exchange -> writeJson(exchange, tableJson));

      server.start();
      return new UcStubServer(server);
    }

    String baseUri() {
      return baseUri;
    }

    @Override
    public void close() {
      server.stop(0);
    }

    private static void writeJson(com.sun.net.httpserver.HttpExchange exchange, String body)
        throws IOException {
      byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      exchange.sendResponseHeaders(200, bytes.length);
      try (OutputStream out = exchange.getResponseBody()) {
        out.write(bytes);
      }
    }
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

      ResourceId tableId =
          ResourceId.newBuilder()
              .setAccountId("t")
              .setId("tbl")
              .setKind(ResourceKind.RK_TABLE)
              .build();

      var cs1 =
          conn
              .captureSnapshotTargetStats("db", "events", tableId, 1L, Set.of("#1", "#4", "#9"))
              .stream()
              .filter(
                  r ->
                      r.hasTarget()
                          && r.getTarget().hasColumn()
                          && r.hasScalar()
                          && r.getTarget().getColumn().getColumnId() > 0)
              .toList();
      var ids1 =
          cs1.stream()
              .map(v -> v.getTarget().getColumn().getColumnId())
              .collect(java.util.stream.Collectors.toSet());
      var names1 =
          cs1.stream()
              .map(v -> v.getScalar().getDisplayName())
              .collect(java.util.stream.Collectors.toSet());

      assertEquals(Set.of(1L, 4L, 9L), ids1);
      assertEquals(Set.of("id", "user.id", "items.element.qty"), names1);

      var cs2 =
          conn
              .captureSnapshotTargetStats(
                  "db", "events", tableId, 1L, Set.of("user.name", "attrs.value"))
              .stream()
              .filter(
                  r ->
                      r.hasTarget()
                          && r.getTarget().hasColumn()
                          && r.hasScalar()
                          && r.getTarget().getColumn().getColumnId() > 0)
              .toList();
      var ids2 =
          cs2.stream()
              .map(v -> v.getTarget().getColumn().getColumnId())
              .collect(java.util.stream.Collectors.toSet());
      var names2 =
          cs2.stream()
              .map(v -> v.getScalar().getDisplayName())
              .collect(java.util.stream.Collectors.toSet());

      assertEquals(Set.of(5L, 12L), ids2);
      assertEquals(Set.of("user.name", "attrs.value"), names2);

      var cs3 =
          conn
              .captureSnapshotTargetStats(
                  "db", "events", tableId, 1L, Set.of("#2", "items.element.sku"))
              .stream()
              .filter(
                  r ->
                      r.hasTarget()
                          && r.getTarget().hasColumn()
                          && r.hasScalar()
                          && r.getTarget().getColumn().getColumnId() > 0)
              .toList();
      var ids3 =
          cs3.stream()
              .map(v -> v.getTarget().getColumn().getColumnId())
              .collect(java.util.stream.Collectors.toSet());
      var names3 =
          cs3.stream()
              .map(v -> v.getScalar().getDisplayName())
              .collect(java.util.stream.Collectors.toSet());

      assertEquals(Set.of(2L, 8L), ids3);
      assertEquals(Set.of("ts", "items.element.sku"), names3);

      long allColsCount =
          conn
              .captureSnapshotTargetStats("db", "events", tableId, 1L, Collections.emptySet())
              .stream()
              .filter(
                  r ->
                      r.hasTarget()
                          && r.getTarget().hasColumn()
                          && r.hasScalar()
                          && r.getTarget().getColumn().getColumnId() > 0)
              .count();
      assertEquals(8, allColsCount);
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
  void connectorResourceIdKindIsPreservedAcrossCreateGetAndList() {
    TestSupport.createCatalog(catalogService, "cat-kind", "");
    var spec =
        ConnectorSpec.newBuilder()
            .setDisplayName("kind-check")
            .setKind(ConnectorKind.CK_UNITY)
            .setUri("dummy://x")
            .setSource(source(List.of("a", "b")))
            .setDestination(dest("cat-kind"))
            .build();

    var created =
        connectors.createConnector(CreateConnectorRequest.newBuilder().setSpec(spec).build());
    assertEquals(
        ResourceKind.RK_CONNECTOR,
        created.getConnector().getResourceId().getKind(),
        "createConnector should return a connector resource id");

    var fetched =
        connectors.getConnector(
            GetConnectorRequest.newBuilder()
                .setConnectorId(created.getConnector().getResourceId())
                .build());
    assertEquals(
        ResourceKind.RK_CONNECTOR,
        fetched.getConnector().getResourceId().getKind(),
        "getConnector should preserve connector resource id kind");

    var listed =
        connectors.listConnectors(ListConnectorsRequest.newBuilder().build()).getConnectorsList();
    assertTrue(
        listed.stream()
            .filter(c -> c.getDisplayName().equals("kind-check"))
            .allMatch(c -> c.getResourceId().getKind() == ResourceKind.RK_CONNECTOR),
        "listConnectors should preserve connector resource id kind");
  }

  @Test
  void connectorAuthIsMaskedOnResponses() {
    TestSupport.createCatalog(catalogService, "cat-auth", "");
    var auth =
        AuthConfig.newBuilder()
            .setScheme("oauth2")
            .setCredentials(
                AuthCredentials.newBuilder()
                    .setBearer(AuthCredentials.BearerToken.newBuilder().setToken("secret-token")))
            .putProperties("client_secret", "super-secret")
            .putProperties("scope", "all-apis")
            .putProperties("token", "also-secret")
            .putHeaderHints("Authorization", "Bearer secret-token")
            .putHeaderHints("X-Test", "keep")
            .build();
    var spec =
        ConnectorSpec.newBuilder()
            .setDisplayName("auth-mask")
            .setKind(ConnectorKind.CK_UNITY)
            .setUri("dummy://x")
            .setSource(source(List.of("a", "b")))
            .setDestination(dest("cat-auth"))
            .setAuth(auth)
            .build();

    var created =
        connectors.createConnector(CreateConnectorRequest.newBuilder().setSpec(spec).build());
    var returnedAuth = created.getConnector().getAuth();

    assertFalse(returnedAuth.hasCredentials());
    assertEquals("****", returnedAuth.getPropertiesMap().get("client_secret"));
    assertEquals("****", returnedAuth.getPropertiesMap().get("token"));
    assertEquals("all-apis", returnedAuth.getPropertiesMap().get("scope"));
    assertEquals("****", returnedAuth.getHeaderHintsMap().get("Authorization"));
    assertEquals("keep", returnedAuth.getHeaderHintsMap().get("X-Test"));

    var fetched =
        connectors.getConnector(
            GetConnectorRequest.newBuilder()
                .setConnectorId(created.getConnector().getResourceId())
                .build());
    var fetchedAuth = fetched.getConnector().getAuth();

    assertFalse(fetchedAuth.hasCredentials());
    assertEquals("****", fetchedAuth.getPropertiesMap().get("client_secret"));
    assertEquals("****", fetchedAuth.getPropertiesMap().get("token"));
    assertEquals("all-apis", fetchedAuth.getPropertiesMap().get("scope"));
    assertEquals("****", fetchedAuth.getHeaderHintsMap().get("Authorization"));
    assertEquals("keep", fetchedAuth.getHeaderHintsMap().get("X-Test"));
  }

  @Test
  void createConnectorIdempotencyMismatchOnUri() throws Exception {
    TestSupport.createCatalog(catalogService, "cat-idem-2", "");
    var specA =
        ConnectorSpec.newBuilder()
            .setDisplayName("idem-2")
            .setKind(ConnectorKind.CK_UNITY)
            .setUri("dummy://x")
            .setSource(source(List.of("a", "b")))
            .setDestination(dest("cat-idem-2"))
            .build();

    var specB =
        ConnectorSpec.newBuilder()
            .setDisplayName("idem-2")
            .setKind(ConnectorKind.CK_UNITY)
            .setUri("dummy://y")
            .setSource(source(List.of("a", "b")))
            .setDestination(dest("cat-idem-2"))
            .build();

    var idem = IdempotencyKey.newBuilder().setKey("fixed-key-2").build();
    connectors.createConnector(
        CreateConnectorRequest.newBuilder().setSpec(specA).setIdempotency(idem).build());

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                connectors.createConnector(
                    CreateConnectorRequest.newBuilder()
                        .setSpec(specB)
                        .setIdempotency(idem)
                        .build()));

    TestSupport.assertGrpcAndMc(
        ex, Status.Code.ABORTED, ErrorCode.MC_CONFLICT, "Idempotency key mismatch");
  }

  @Test
  void getConnectorNotFound() {
    var badRid =
        ResourceId.newBuilder()
            .setAccountId(TestSupport.DEFAULT_SEED_ACCOUNT)
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
        ex, Status.Code.ALREADY_EXISTS, ErrorCode.MC_CONFLICT, "Connector \"u-a1\" already exists");
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

    assertDoesNotThrow(
        () ->
            connectors.deleteConnector(
                DeleteConnectorRequest.newBuilder().setConnectorId(c.getResourceId()).build()));
  }

  @Test
  void startCaptureNotFound() throws Exception {
    var rid =
        ResourceId.newBuilder()
            .setAccountId(TestSupport.DEFAULT_SEED_ACCOUNT)
            .setId("missing")
            .setKind(ResourceKind.RK_CONNECTOR)
            .build();
    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                reconcileControl.startCapture(
                    StartCaptureRequest.newBuilder()
                        .setScope(CaptureScope.newBuilder().setConnectorId(rid).build())
                        .setMode(CaptureMode.CM_METADATA_AND_STATS)
                        .build()));

    TestSupport.assertGrpcAndMc(
        ex, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, "Connector not found");
  }

  @Test
  void getReconcileJobNotFound() throws Exception {
    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                reconcileControl.getReconcileJob(
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

  private List<FileTargetStats> listFileStats(
      ResourceId tableId, SnapshotRef snapshot, int pageSize) {
    var response =
        statsService.listTargetStats(
            ListTargetStatsRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshot(snapshot)
                .addTargetKinds(StatsTargetKind.STK_FILE)
                .setPage(PageRequest.newBuilder().setPageSize(pageSize))
                .build());
    return response.getRecordsList().stream()
        .filter(TargetStatsRecord::hasFile)
        .map(TargetStatsRecord::getFile)
        .toList();
  }

  private List<FileTargetStats> listCurrentFileStats(ResourceId tableId, int pageSize) {
    return listFileStats(
        tableId, SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT).build(), pageSize);
  }

  private List<FileTargetStats> awaitFileStats(
      ResourceId tableId, SnapshotRef snapshot, int pageSize, Duration timeout)
      throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    List<FileTargetStats> stats = List.of();
    while (System.nanoTime() <= deadline) {
      stats = listFileStats(tableId, snapshot, pageSize);
      if (!stats.isEmpty()) {
        return stats;
      }
      Thread.sleep(200);
    }
    return stats;
  }

  private List<FileTargetStats> awaitCurrentFileStats(
      ResourceId tableId, int pageSize, Duration timeout) throws InterruptedException {
    return awaitFileStats(
        tableId,
        SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT).build(),
        pageSize,
        timeout);
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
