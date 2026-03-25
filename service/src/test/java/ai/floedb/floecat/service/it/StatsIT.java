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

import ai.floedb.floecat.catalog.rpc.*;
import ai.floedb.floecat.common.rpc.ErrorCode;
import ai.floedb.floecat.common.rpc.IdempotencyKey;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
class StatsIT {
  @Inject PointerStore ptr;
  @Inject BlobStore blob;

  @GrpcClient("floecat")
  DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;

  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  @GrpcClient("floecat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;

  @GrpcClient("floecat")
  TableServiceGrpc.TableServiceBlockingStub table;

  @GrpcClient("floecat")
  SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshot;

  @GrpcClient("floecat")
  TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub statistic;

  @GrpcClient("floecat")
  MutinyTableStatisticsServiceGrpc.MutinyTableStatisticsServiceStub statisticMutiny;

  String tablePrefix = this.getClass().getSimpleName() + "_";

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();
  }

  @Test
  void statsCreateListGetCurrent() throws Exception {
    var catName = tablePrefix + "cat_stats";
    var cat = TestSupport.createCatalog(catalog, catName, "cat for stats");

    var parents = List.of("db_stats", "schema_stats");
    var nsLeaf = "it_ns";
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), nsLeaf, parents, "ns for stats");
    var nsId = ns.getResourceId();
    assertEquals(ResourceKind.RK_NAMESPACE, nsId.getKind());

    var nsPath = new ArrayList<>(parents);
    nsPath.add(nsLeaf);
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            nsId,
            "fact_orders",
            "s3://bucket/fact_orders",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"},"
                + "{\"name\":\"ts\",\"type\":\"timestamp\"}]}",
            "none");
    var tblId = tbl.getResourceId();
    assertEquals(ResourceKind.RK_TABLE, tblId.getKind());

    final long snapOld = 101L;
    final long snapNew = 202L;
    final long fixedNowMs = 1_700_000_000_000L;

    TestSupport.createSnapshot(snapshot, tblId, snapOld, System.currentTimeMillis() - 86_400_000L);
    TestSupport.createSnapshot(snapshot, tblId, snapNew, System.currentTimeMillis());

    var tableStatsOld =
        TableStats.newBuilder()
            .setTableId(tblId)
            .setSnapshotId(snapOld)
            .setUpstream(
                UpstreamStamp.newBuilder()
                    .setSystem(TableFormat.TF_ICEBERG)
                    .setTableNativeId("iceberg://warehouse/db/fact_orders")
                    .setCommitRef(Long.toString(snapOld))
                    .setFetchedAt(Timestamps.fromMillis(fixedNowMs))
                    .build())
            .setRowCount(1_000)
            .setDataFileCount(5)
            .setTotalSizeBytes(123_456)
            .setNdv(Ndv.newBuilder().setExact(950).build())
            .setMetadata(
                StatsMetadata.newBuilder()
                    .setProducer(StatsProducer.SPROD_SOURCE_NATIVE)
                    .setCaptureMode(StatsCaptureMode.SCM_ASYNC)
                    .setCompleteness(StatsCompleteness.SC_PARTIAL)
                    .setConfidenceLevel(0.72d)
                    .setCoverage(
                        StatsCoverage.newBuilder()
                            .setRowsScanned(1_000)
                            .setFilesScanned(5)
                            .setRowGroupsSampled(2)
                            .setBytesScanned(12_345)
                            .putProperties("sampling_strategy", "metadata-guided")
                            .build())
                    .setCapturedAt(Timestamps.fromMillis(fixedNowMs - 5_000L))
                    .setRefreshedAt(Timestamps.fromMillis(fixedNowMs))
                    .putProperties("estimator", "baseline")
                    .build())
            .build();

    var putTableRespOld =
        statistic.putTableStats(
            PutTableStatsRequest.newBuilder()
                .setTableId(tblId)
                .setSnapshotId(snapOld)
                .setStats(tableStatsOld)
                .build());
    assertNotNull(putTableRespOld.getMeta().getPointerKey());

    var colIdStats =
        ColumnStats.newBuilder()
            .setTableId(tblId)
            .setSnapshotId(snapOld)
            .setColumnId(1)
            .setColumnName("id")
            .setLogicalType("int")
            .setNullCount(0)
            .setNdv(Ndv.newBuilder().setExact(1_000).build())
            .setMin("1")
            .setMax("1000")
            .setUpstream(tableStatsOld.getUpstream())
            .setMetadata(
                StatsMetadata.newBuilder()
                    .setProducer(StatsProducer.SPROD_UPSTREAM_METADATA_DERIVED)
                    .setCaptureMode(StatsCaptureMode.SCM_SYNC)
                    .setCompleteness(StatsCompleteness.SC_COMPLETE)
                    .setConfidenceLevel(0.99d)
                    .setCoverage(
                        StatsCoverage.newBuilder()
                            .setRowsScanned(1_000)
                            .setFilesScanned(5)
                            .setBytesScanned(9_876)
                            .build())
                    .setCapturedAt(Timestamps.fromMillis(fixedNowMs - 10_000L))
                    .setRefreshedAt(Timestamps.fromMillis(fixedNowMs - 2_000L))
                    .build())
            .build();

    var colTsStats =
        ColumnStats.newBuilder()
            .setTableId(tblId)
            .setSnapshotId(snapOld)
            .setColumnId(2)
            .setColumnName("ts")
            .setLogicalType("timestamp")
            .setNullCount(10)
            .setNdv(Ndv.newBuilder().build())
            .setMin("2024-01-01T00:00:00Z")
            .setMax("2024-12-31T23:59:59Z")
            .setUpstream(tableStatsOld.getUpstream())
            .build();

    var putColsRespOld =
        statisticMutiny
            .putColumnStats(
                io.smallrye.mutiny.Multi.createFrom()
                    .items(
                        PutColumnStatsRequest.newBuilder()
                            .setTableId(tblId)
                            .setSnapshotId(snapOld)
                            .addColumns(colIdStats)
                            .addColumns(colTsStats)
                            .build()))
            .await()
            .atMost(java.time.Duration.ofSeconds(30));
    assertTrue(putColsRespOld.getUpserted() >= 2);

    var gotTableOld =
        statistic.getTableStats(
            GetTableStatsRequest.newBuilder()
                .setTableId(tblId)
                .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapOld).build())
                .build());
    assertEquals(1_000, gotTableOld.getStats().getRowCount());
    assertEquals(123_456, gotTableOld.getStats().getTotalSizeBytes());
    assertEquals(
        StatsProducer.SPROD_SOURCE_NATIVE, gotTableOld.getStats().getMetadata().getProducer());
    assertEquals(
        StatsCompleteness.SC_PARTIAL, gotTableOld.getStats().getMetadata().getCompleteness());
    assertEquals(0.72d, gotTableOld.getStats().getMetadata().getConfidenceLevel(), 0.0001d);
    assertEquals(2L, gotTableOld.getStats().getMetadata().getCoverage().getRowGroupsSampled());

    var listColsOld =
        statistic.listColumnStats(
            ListColumnStatsRequest.newBuilder()
                .setTableId(tblId)
                .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapOld).build())
                .setPage(PageRequest.newBuilder().setPageSize(100).build())
                .build());
    assertEquals(2, listColsOld.getColumnsCount());
    assertEquals(1, listColsOld.getColumns(0).getColumnId());
    var firstCol =
        listColsOld.getColumnsList().stream().filter(c -> c.getColumnId() == 1).findFirst();
    assertTrue(firstCol.isPresent());
    assertEquals(
        StatsProducer.SPROD_UPSTREAM_METADATA_DERIVED, firstCol.get().getMetadata().getProducer());
    assertEquals(0.99d, firstCol.get().getMetadata().getConfidenceLevel(), 0.0001d);

    var tableStatsNew =
        tableStatsOld.toBuilder()
            .setSnapshotId(snapNew)
            .setRowCount(2_500)
            .setDataFileCount(9)
            .setTotalSizeBytes(987_654)
            .setUpstream(
                tableStatsOld.getUpstream().toBuilder()
                    .setCommitRef(Long.toString(snapNew))
                    .setFetchedAt(Timestamps.fromMillis(fixedNowMs + 1_000L))
                    .build())
            .build();

    statistic.putTableStats(
        PutTableStatsRequest.newBuilder()
            .setTableId(tblId)
            .setSnapshotId(snapNew)
            .setStats(tableStatsNew)
            .build());

    statisticMutiny
        .putColumnStats(
            io.smallrye.mutiny.Multi.createFrom()
                .items(
                    PutColumnStatsRequest.newBuilder()
                        .setTableId(tblId)
                        .setSnapshotId(snapNew)
                        .addColumns(
                            colIdStats.toBuilder()
                                .setSnapshotId(snapNew)
                                .setNdv(Ndv.newBuilder().setExact(2_500)))
                        .addColumns(
                            colTsStats.toBuilder()
                                .setSnapshotId(snapNew)
                                .setNdv(Ndv.newBuilder().build()))
                        .build()))
        .await()
        .atMost(java.time.Duration.ofSeconds(30));

    var currentTableStats =
        statistic.getTableStats(
            GetTableStatsRequest.newBuilder()
                .setTableId(tblId)
                .setSnapshot(SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT))
                .build());
    assertEquals(snapNew, currentTableStats.getStats().getSnapshotId());
    assertEquals(2_500, currentTableStats.getStats().getRowCount());

    var currentCols =
        statistic.listColumnStats(
            ListColumnStatsRequest.newBuilder()
                .setTableId(tblId)
                .setSnapshot(SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT))
                .setPage(PageRequest.newBuilder().setPageSize(100).build())
                .build());
    assertTrue(currentCols.getColumnsCount() >= 2);
    assertEquals(snapNew, currentCols.getColumns(0).getSnapshotId());

    var notFound =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                statistic.getTableStats(
                    GetTableStatsRequest.newBuilder()
                        .setTableId(tblId)
                        .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(9_999_999L).build())
                        .build()));
    TestSupport.assertGrpcAndMc(
        notFound, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, "not found");
  }

  @Test
  void statsWithoutMetadataRemainBackwardCompatible() throws Exception {
    var cat = TestSupport.createCatalog(catalog, tablePrefix + "cat_no_metadata", "cat");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "it_ns_no_metadata", List.of(), "ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "fact_no_metadata",
            "s3://bucket/fact_no_metadata",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");
    var tableId = tbl.getResourceId();
    long snapshotId = 7001L;
    TestSupport.createSnapshot(snapshot, tableId, snapshotId, System.currentTimeMillis());

    var tableStats =
        TableStats.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setRowCount(123)
            .setDataFileCount(2)
            .setTotalSizeBytes(456)
            .build();
    statistic.putTableStats(
        PutTableStatsRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setStats(tableStats)
            .build());

    var columnStats =
        ColumnStats.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setColumnId(1)
            .setColumnName("id")
            .setLogicalType("int")
            .setNullCount(0)
            .build();
    statisticMutiny
        .putColumnStats(
            io.smallrye.mutiny.Multi.createFrom()
                .items(
                    PutColumnStatsRequest.newBuilder()
                        .setTableId(tableId)
                        .setSnapshotId(snapshotId)
                        .addColumns(columnStats)
                        .build()))
        .await()
        .atMost(java.time.Duration.ofSeconds(30));

    var got =
        statistic.getTableStats(
            GetTableStatsRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapshotId).build())
                .build());
    assertFalse(got.getStats().hasMetadata());

    var listed =
        statistic.listColumnStats(
            ListColumnStatsRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapshotId).build())
                .setPage(PageRequest.newBuilder().setPageSize(10).build())
                .build());
    assertEquals(1, listed.getColumnsCount());
    assertFalse(listed.getColumns(0).hasMetadata());
  }

  @Test
  void putTableStatsSameIdempotencyKeyRejectsMetadataOnlyMismatch() throws Exception {
    var cat = TestSupport.createCatalog(catalog, tablePrefix + "cat_idem_metadata", "cat");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "it_ns_idem_metadata", List.of(), "ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "fact_idem_metadata",
            "s3://bucket/fact_idem_metadata",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");
    var tableId = tbl.getResourceId();
    long snapshotId = 7002L;
    TestSupport.createSnapshot(snapshot, tableId, snapshotId, System.currentTimeMillis());

    var key = IdempotencyKey.newBuilder().setKey("stats-idem-metadata-mismatch").build();
    var base =
        TableStats.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setRowCount(10)
            .setDataFileCount(1)
            .setTotalSizeBytes(100)
            .build();

    statistic.putTableStats(
        PutTableStatsRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setStats(
                base.toBuilder()
                    .setMetadata(
                        StatsMetadata.newBuilder()
                            .setProducer(StatsProducer.SPROD_SOURCE_NATIVE)
                            .setCaptureMode(StatsCaptureMode.SCM_SYNC)
                            .setCompleteness(StatsCompleteness.SC_PARTIAL)
                            .setConfidenceLevel(0.5d)
                            .build())
                    .build())
            .setIdempotency(key)
            .build());

    var mismatch =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                statistic.putTableStats(
                    PutTableStatsRequest.newBuilder()
                        .setTableId(tableId)
                        .setSnapshotId(snapshotId)
                        .setStats(
                            base.toBuilder()
                                .setMetadata(
                                    StatsMetadata.newBuilder()
                                        .setProducer(StatsProducer.SPROD_SOURCE_NATIVE)
                                        .setCaptureMode(StatsCaptureMode.SCM_SYNC)
                                        .setCompleteness(StatsCompleteness.SC_PARTIAL)
                                        .setConfidenceLevel(0.9d)
                                        .build())
                                .build())
                        .setIdempotency(key)
                        .build()));
    TestSupport.assertGrpcAndMc(
        mismatch, Status.Code.ABORTED, ErrorCode.MC_CONFLICT, "Idempotency key mismatch");
  }

  @Test
  void putTableStatsSameIdempotencyKeyAllowsRefreshedAtOnlyChange() throws Exception {
    var cat = TestSupport.createCatalog(catalog, tablePrefix + "cat_idem_refresh_only", "cat");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "it_ns_idem_refresh_only", List.of(), "ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "fact_idem_refresh_only",
            "s3://bucket/fact_idem_refresh_only",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");
    var tableId = tbl.getResourceId();
    long snapshotId = 7003L;
    TestSupport.createSnapshot(snapshot, tableId, snapshotId, System.currentTimeMillis());

    var key = IdempotencyKey.newBuilder().setKey("stats-idem-refresh-only").build();
    var base =
        TableStats.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setRowCount(10)
            .setDataFileCount(1)
            .setTotalSizeBytes(100)
            .build();

    var firstCapturedAt = Timestamps.fromMillis(1_700_000_000_000L);
    var firstRefreshedAt = Timestamps.fromMillis(1_700_000_001_000L);
    var secondRefreshedAt = Timestamps.fromMillis(1_700_000_002_000L);

    statistic.putTableStats(
        PutTableStatsRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setStats(
                base.toBuilder()
                    .setMetadata(
                        StatsMetadata.newBuilder()
                            .setProducer(StatsProducer.SPROD_SOURCE_NATIVE)
                            .setCaptureMode(StatsCaptureMode.SCM_SYNC)
                            .setCompleteness(StatsCompleteness.SC_COMPLETE)
                            .setCapturedAt(firstCapturedAt)
                            .setRefreshedAt(firstRefreshedAt)
                            .build())
                    .build())
            .setIdempotency(key)
            .build());

    assertDoesNotThrow(
        () ->
            statistic.putTableStats(
                PutTableStatsRequest.newBuilder()
                    .setTableId(tableId)
                    .setSnapshotId(snapshotId)
                    .setStats(
                        base.toBuilder()
                            .setMetadata(
                                StatsMetadata.newBuilder()
                                    .setProducer(StatsProducer.SPROD_SOURCE_NATIVE)
                                    .setCaptureMode(StatsCaptureMode.SCM_SYNC)
                                    .setCompleteness(StatsCompleteness.SC_COMPLETE)
                                    .setCapturedAt(firstCapturedAt)
                                    .setRefreshedAt(secondRefreshedAt)
                                    .build())
                            .build())
                    .setIdempotency(key)
                    .build()));

    var stored =
        statistic.getTableStats(
            GetTableStatsRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapshotId).build())
                .build());
    // Replay should not rewrite content when only refreshed_at differs.
    assertEquals(firstRefreshedAt, stored.getStats().getMetadata().getRefreshedAt());
  }

  @Test
  void putTableStatsSameIdempotencyKeyAllowsCapturedAtOnlyChange() throws Exception {
    var cat = TestSupport.createCatalog(catalog, tablePrefix + "cat_idem_capture_only", "cat");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "it_ns_idem_capture_only", List.of(), "ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "fact_idem_capture_only",
            "s3://bucket/fact_idem_capture_only",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");
    var tableId = tbl.getResourceId();
    long snapshotId = 7006L;
    TestSupport.createSnapshot(snapshot, tableId, snapshotId, System.currentTimeMillis());

    var key = IdempotencyKey.newBuilder().setKey("stats-idem-capture-only").build();
    var base =
        TableStats.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setRowCount(10)
            .setDataFileCount(1)
            .setTotalSizeBytes(100)
            .build();

    var firstCapturedAt = Timestamps.fromMillis(1_700_000_010_000L);
    var secondCapturedAt = Timestamps.fromMillis(1_700_000_020_000L);
    var refreshedAt = Timestamps.fromMillis(1_700_000_030_000L);

    statistic.putTableStats(
        PutTableStatsRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setStats(
                base.toBuilder()
                    .setMetadata(
                        StatsMetadata.newBuilder()
                            .setProducer(StatsProducer.SPROD_SOURCE_NATIVE)
                            .setCaptureMode(StatsCaptureMode.SCM_SYNC)
                            .setCompleteness(StatsCompleteness.SC_COMPLETE)
                            .setCapturedAt(firstCapturedAt)
                            .setRefreshedAt(refreshedAt)
                            .build())
                    .build())
            .setIdempotency(key)
            .build());

    assertDoesNotThrow(
        () ->
            statistic.putTableStats(
                PutTableStatsRequest.newBuilder()
                    .setTableId(tableId)
                    .setSnapshotId(snapshotId)
                    .setStats(
                        base.toBuilder()
                            .setMetadata(
                                StatsMetadata.newBuilder()
                                    .setProducer(StatsProducer.SPROD_SOURCE_NATIVE)
                                    .setCaptureMode(StatsCaptureMode.SCM_SYNC)
                                    .setCompleteness(StatsCompleteness.SC_COMPLETE)
                                    .setCapturedAt(secondCapturedAt)
                                    .setRefreshedAt(refreshedAt)
                                    .build())
                            .build())
                    .setIdempotency(key)
                    .build()));

    var stored =
        statistic.getTableStats(
            GetTableStatsRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapshotId).build())
                .build());
    // Replay should not rewrite content when only captured_at differs.
    assertEquals(firstCapturedAt, stored.getStats().getMetadata().getCapturedAt());
  }

  @Test
  void putTableStatsRejectsInvalidMetadataConfidence() throws Exception {
    var cat = TestSupport.createCatalog(catalog, tablePrefix + "cat_invalid_confidence", "cat");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "it_ns_invalid_confidence", List.of(), "ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "fact_invalid_confidence",
            "s3://bucket/fact_invalid_confidence",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");
    var tableId = tbl.getResourceId();
    long snapshotId = 7004L;
    TestSupport.createSnapshot(snapshot, tableId, snapshotId, System.currentTimeMillis());

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                statistic.putTableStats(
                    PutTableStatsRequest.newBuilder()
                        .setTableId(tableId)
                        .setSnapshotId(snapshotId)
                        .setStats(
                            TableStats.newBuilder()
                                .setTableId(tableId)
                                .setSnapshotId(snapshotId)
                                .setRowCount(1)
                                .setDataFileCount(1)
                                .setTotalSizeBytes(1)
                                .setMetadata(
                                    StatsMetadata.newBuilder()
                                        .setProducer(StatsProducer.SPROD_SOURCE_NATIVE)
                                        .setCaptureMode(StatsCaptureMode.SCM_SYNC)
                                        .setCompleteness(StatsCompleteness.SC_PARTIAL)
                                        .setConfidenceLevel(1.1d)
                                        .build())
                                .build())
                        .build()));
    TestSupport.assertGrpcAndMc(
        ex, Status.Code.INVALID_ARGUMENT, ErrorCode.MC_INVALID_ARGUMENT, "confidence_level");
  }

  @Test
  void putTableStatsRejectsNegativeMetadataCoverage() throws Exception {
    var cat = TestSupport.createCatalog(catalog, tablePrefix + "cat_negative_coverage", "cat");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "it_ns_negative_coverage", List.of(), "ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "fact_negative_coverage",
            "s3://bucket/fact_negative_coverage",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");
    var tableId = tbl.getResourceId();
    long snapshotId = 7005L;
    TestSupport.createSnapshot(snapshot, tableId, snapshotId, System.currentTimeMillis());

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                statistic.putTableStats(
                    PutTableStatsRequest.newBuilder()
                        .setTableId(tableId)
                        .setSnapshotId(snapshotId)
                        .setStats(
                            TableStats.newBuilder()
                                .setTableId(tableId)
                                .setSnapshotId(snapshotId)
                                .setRowCount(1)
                                .setDataFileCount(1)
                                .setTotalSizeBytes(1)
                                .setMetadata(
                                    StatsMetadata.newBuilder()
                                        .setProducer(StatsProducer.SPROD_SOURCE_NATIVE)
                                        .setCaptureMode(StatsCaptureMode.SCM_SYNC)
                                        .setCompleteness(StatsCompleteness.SC_PARTIAL)
                                        .setCoverage(
                                            StatsCoverage.newBuilder().setRowsScanned(-1L).build())
                                        .build())
                                .build())
                        .build()));
    TestSupport.assertGrpcAndMc(
        ex, Status.Code.INVALID_ARGUMENT, ErrorCode.MC_INVALID_ARGUMENT, "rows_scanned");
  }
}
