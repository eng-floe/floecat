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
import ai.floedb.floecat.common.rpc.ResourceId;
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
        TableValueStats.newBuilder()
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
            .build();

    var tableMetadataOld =
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
            .build();

    var putTableRespOld = putTableStats(tblId, snapOld, tableStatsOld, tableMetadataOld, null);
    assertEquals(1, putTableRespOld.getUpserted());

    var colIdMetadata =
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
            .build();

    var colIdStats =
        ScalarStats.newBuilder()
            .setDisplayName("id")
            .setLogicalType("int")
            .setNullCount(0)
            .setNdv(Ndv.newBuilder().setExact(1_000).build())
            .setMin("1")
            .setMax("1000")
            .setUpstream(tableStatsOld.getUpstream())
            .build();

    var colTsStats =
        ScalarStats.newBuilder()
            .setDisplayName("ts")
            .setLogicalType("timestamp")
            .setNullCount(10)
            .setNdv(Ndv.newBuilder().build())
            .setMin("2024-01-01T00:00:00Z")
            .setMax("2024-12-31T23:59:59Z")
            .setUpstream(tableStatsOld.getUpstream())
            .build();

    var putColsRespOld =
        putColumnStats(
            tblId,
            snapOld,
            null,
            new ColumnWrite(1L, colIdStats, colIdMetadata),
            new ColumnWrite(2L, colTsStats, null));
    assertTrue(putColsRespOld.getUpserted() >= 2);

    var gotTableOld = getTableStats(tblId, SnapshotRef.newBuilder().setSnapshotId(snapOld).build());
    assertEquals(1_000, gotTableOld.getTable().getRowCount());
    assertEquals(123_456, gotTableOld.getTable().getTotalSizeBytes());
    assertEquals(StatsProducer.SPROD_SOURCE_NATIVE, gotTableOld.getMetadata().getProducer());
    assertEquals(StatsCompleteness.SC_PARTIAL, gotTableOld.getMetadata().getCompleteness());
    assertEquals(0.72d, gotTableOld.getMetadata().getConfidenceLevel(), 0.0001d);
    assertEquals(2L, gotTableOld.getMetadata().getCoverage().getRowGroupsSampled());

    var listColsOld =
        listColumnStats(tblId, SnapshotRef.newBuilder().setSnapshotId(snapOld).build(), 100);
    assertEquals(2, listColsOld.size());
    assertEquals(1, listColsOld.get(0).getTarget().getColumn().getColumnId());
    var firstCol =
        listColsOld.stream().filter(c -> c.getTarget().getColumn().getColumnId() == 1).findFirst();
    assertTrue(firstCol.isPresent());
    assertEquals(
        StatsProducer.SPROD_UPSTREAM_METADATA_DERIVED, firstCol.get().getMetadata().getProducer());
    assertEquals(0.99d, firstCol.get().getMetadata().getConfidenceLevel(), 0.0001d);

    var tableStatsNew =
        tableStatsOld.toBuilder()
            .setRowCount(2_500)
            .setDataFileCount(9)
            .setTotalSizeBytes(987_654)
            .setUpstream(
                tableStatsOld.getUpstream().toBuilder()
                    .setCommitRef(Long.toString(snapNew))
                    .setFetchedAt(Timestamps.fromMillis(fixedNowMs + 1_000L))
                    .build())
            .build();

    putTableStats(tblId, snapNew, tableStatsNew, null, null);

    putColumnStats(
        tblId,
        snapNew,
        null,
        new ColumnWrite(
            1L, colIdStats.toBuilder().setNdv(Ndv.newBuilder().setExact(2_500)).build(), null),
        new ColumnWrite(2L, colTsStats.toBuilder().setNdv(Ndv.newBuilder().build()).build(), null));

    var currentTableStats =
        getTableStats(
            tblId, SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT).build());
    assertEquals(snapNew, currentTableStats.getSnapshotId());
    assertEquals(2_500, currentTableStats.getTable().getRowCount());

    var currentCols =
        listColumnStats(
            tblId, SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT).build(), 100);
    assertTrue(currentCols.size() >= 2);
    assertEquals(snapNew, currentCols.get(0).getSnapshotId());

    var notFound =
        assertThrows(
            StatusRuntimeException.class,
            () -> getTableStats(tblId, SnapshotRef.newBuilder().setSnapshotId(9_999_999L).build()));
    TestSupport.assertGrpcAndMc(
        notFound, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, "not found");
  }

  @Test
  void getTargetStatsSupportsFileAndExpressionTargets() throws Exception {
    var catName = tablePrefix + "cat_target_kinds";
    var cat = TestSupport.createCatalog(catalog, catName, "cat for target kinds");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "ns_target_kinds", List.of("db"), "ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "target_kinds_tbl",
            "s3://bucket/target_kinds_tbl",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");
    var tableId = tbl.getResourceId();
    long snapshotId = 909L;
    TestSupport.createSnapshot(snapshot, tableId, snapshotId, System.currentTimeMillis());

    var metadata =
        StatsMetadata.newBuilder()
            .setProducer(StatsProducer.SPROD_SOURCE_NATIVE)
            .setCaptureMode(StatsCaptureMode.SCM_SYNC)
            .setCompleteness(StatsCompleteness.SC_COMPLETE)
            .setCoverage(StatsCoverage.newBuilder().setRowsScanned(10).setFilesScanned(1).build())
            .build();

    var fileStats =
        FileTargetStats.newBuilder()
            .setFilePath("/data/file-0001.parquet")
            .setFileFormat("parquet")
            .setRowCount(10)
            .setSizeBytes(1024)
            .build();

    var expressionTarget =
        EngineExpressionStatsTarget.newBuilder()
            .setEngineKind("duckdb")
            .setEngineExpressionKey(com.google.protobuf.ByteString.copyFromUtf8("sum(id)"))
            .build();
    var expressionScalar =
        ScalarStats.newBuilder()
            .setDisplayName("sum(id)")
            .setLogicalType("int64")
            .setValueCount(10)
            .setNullCount(0)
            .setNdv(Ndv.newBuilder().setExact(1).build())
            .setMin("55")
            .setMax("55")
            .build();

    var fileRecord =
        TargetStatsRecord.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setTarget(
                StatsTarget.newBuilder()
                    .setFile(FileStatsTarget.newBuilder().setFilePath(fileStats.getFilePath()))
                    .build())
            .setFile(fileStats)
            .setMetadata(metadata)
            .build();
    var expressionRecord =
        TargetStatsRecord.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setTarget(StatsTarget.newBuilder().setExpression(expressionTarget))
            .setScalar(expressionScalar)
            .setMetadata(metadata)
            .build();
    putTargetRecords(tableId, snapshotId, null, fileRecord, expressionRecord);

    var fileResponse =
        statistic.getTargetStats(
            GetTargetStatsRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapshotId).build())
                .setTarget(
                    StatsTarget.newBuilder()
                        .setFile(FileStatsTarget.newBuilder().setFilePath(fileStats.getFilePath()))
                        .build())
                .build());
    assertTrue(fileResponse.getStats().hasFile());
    assertEquals("/data/file-0001.parquet", fileResponse.getStats().getFile().getFilePath());
    assertEquals(10L, fileResponse.getStats().getFile().getRowCount());
    assertEquals(
        StatsCompleteness.SC_COMPLETE, fileResponse.getStats().getMetadata().getCompleteness());

    var expressionResponse =
        statistic.getTargetStats(
            GetTargetStatsRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapshotId).build())
                .setTarget(StatsTarget.newBuilder().setExpression(expressionTarget).build())
                .build());
    assertTrue(expressionResponse.getStats().hasScalar());
    assertEquals("sum(id)", expressionResponse.getStats().getScalar().getDisplayName());
    assertEquals(
        "duckdb", expressionResponse.getStats().getTarget().getExpression().getEngineKind());
    assertEquals(
        "sum(id)",
        expressionResponse
            .getStats()
            .getTarget()
            .getExpression()
            .getEngineExpressionKey()
            .toStringUtf8());
  }

  @Test
  void listTargetStatsSupportsKindFiltersAndRejectsMultipleKinds() throws Exception {
    var cat = TestSupport.createCatalog(catalog, tablePrefix + "cat_list_kinds", "cat");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "ns_list_kinds", List.of(), "ns");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            "tbl_list_kinds",
            "s3://bucket/tbl_list_kinds",
            "{\"cols\":[{\"name\":\"id\",\"type\":\"int\"}]}",
            "none");
    var tableId = tbl.getResourceId();
    long snapshotId = 910L;
    TestSupport.createSnapshot(snapshot, tableId, snapshotId, System.currentTimeMillis());

    var tableRecord =
        TargetStatsRecord.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setTarget(StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()))
            .setTable(TableValueStats.newBuilder().setRowCount(1L).build())
            .build();
    var columnRecord =
        TargetStatsRecord.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setTarget(
                StatsTarget.newBuilder().setColumn(ColumnStatsTarget.newBuilder().setColumnId(1L)))
            .setScalar(ScalarStats.newBuilder().setDisplayName("id").setLogicalType("int").build())
            .build();
    var expressionRecord =
        TargetStatsRecord.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setTarget(
                StatsTarget.newBuilder()
                    .setExpression(
                        EngineExpressionStatsTarget.newBuilder()
                            .setEngineKind("duckdb")
                            .setEngineExpressionKey(
                                com.google.protobuf.ByteString.copyFromUtf8("expr:id"))
                            .build()))
            .setScalar(
                ScalarStats.newBuilder().setDisplayName("expr").setLogicalType("int").build())
            .build();
    putTargetRecords(tableId, snapshotId, null, tableRecord, columnRecord, expressionRecord);

    var tableOnly =
        statistic.listTargetStats(
            ListTargetStatsRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapshotId).build())
                .setPage(PageRequest.newBuilder().setPageSize(50).build())
                .addTargetKinds(StatsTargetKind.STK_TABLE)
                .build());
    assertEquals(1, tableOnly.getRecordsCount());
    assertTrue(tableOnly.getRecords(0).getTarget().hasTable());

    var columnOnly =
        statistic.listTargetStats(
            ListTargetStatsRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapshotId).build())
                .setPage(PageRequest.newBuilder().setPageSize(50).build())
                .addTargetKinds(StatsTargetKind.STK_COLUMN)
                .build());
    assertEquals(1, columnOnly.getRecordsCount());
    assertTrue(columnOnly.getRecords(0).getTarget().hasColumn());

    var expressionOnly =
        statistic.listTargetStats(
            ListTargetStatsRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapshotId).build())
                .setPage(PageRequest.newBuilder().setPageSize(50).build())
                .addTargetKinds(StatsTargetKind.STK_EXPRESSION)
                .build());
    assertEquals(1, expressionOnly.getRecordsCount());
    assertTrue(expressionOnly.getRecords(0).getTarget().hasExpression());

    var multiKinds =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                statistic.listTargetStats(
                    ListTargetStatsRequest.newBuilder()
                        .setTableId(tableId)
                        .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapshotId).build())
                        .setPage(PageRequest.newBuilder().setPageSize(50).build())
                        .addTargetKinds(StatsTargetKind.STK_TABLE)
                        .addTargetKinds(StatsTargetKind.STK_COLUMN)
                        .build()));
    TestSupport.assertGrpcAndMc(
        multiKinds, Status.Code.INVALID_ARGUMENT, ErrorCode.MC_INVALID_ARGUMENT, null);
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
        TableValueStats.newBuilder()
            .setRowCount(123)
            .setDataFileCount(2)
            .setTotalSizeBytes(456)
            .build();
    putTableStats(tableId, snapshotId, tableStats, null);

    var columnStats =
        ScalarStats.newBuilder().setDisplayName("id").setLogicalType("int").setNullCount(0).build();
    putColumnStats(tableId, snapshotId, null, new ColumnWrite(1L, columnStats, null));

    var got = getTableStats(tableId, SnapshotRef.newBuilder().setSnapshotId(snapshotId).build());
    assertFalse(got.hasMetadata());

    var listed =
        listColumnStats(tableId, SnapshotRef.newBuilder().setSnapshotId(snapshotId).build(), 10);
    assertEquals(1, listed.size());
    assertFalse(listed.get(0).hasMetadata());
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
        TableValueStats.newBuilder()
            .setRowCount(10)
            .setDataFileCount(1)
            .setTotalSizeBytes(100)
            .build();
    var metadataA =
        StatsMetadata.newBuilder()
            .setProducer(StatsProducer.SPROD_SOURCE_NATIVE)
            .setCaptureMode(StatsCaptureMode.SCM_SYNC)
            .setCompleteness(StatsCompleteness.SC_PARTIAL)
            .setConfidenceLevel(0.5d)
            .build();
    var metadataB = metadataA.toBuilder().setConfidenceLevel(0.9d).build();

    putTableStats(tableId, snapshotId, base, metadataA, key);

    var mismatch =
        assertThrows(
            StatusRuntimeException.class,
            () -> putTableStats(tableId, snapshotId, base, metadataB, key));
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
        TableValueStats.newBuilder()
            .setRowCount(10)
            .setDataFileCount(1)
            .setTotalSizeBytes(100)
            .build();

    var firstCapturedAt = Timestamps.fromMillis(1_700_000_000_000L);
    var firstRefreshedAt = Timestamps.fromMillis(1_700_000_001_000L);
    var secondRefreshedAt = Timestamps.fromMillis(1_700_000_002_000L);

    var firstMetadata =
        StatsMetadata.newBuilder()
            .setProducer(StatsProducer.SPROD_SOURCE_NATIVE)
            .setCaptureMode(StatsCaptureMode.SCM_SYNC)
            .setCompleteness(StatsCompleteness.SC_COMPLETE)
            .setCapturedAt(firstCapturedAt)
            .setRefreshedAt(firstRefreshedAt)
            .build();
    var secondMetadata = firstMetadata.toBuilder().setRefreshedAt(secondRefreshedAt).build();

    putTableStats(tableId, snapshotId, base, firstMetadata, key);

    assertDoesNotThrow(() -> putTableStats(tableId, snapshotId, base, secondMetadata, key));

    var stored = getTableStats(tableId, SnapshotRef.newBuilder().setSnapshotId(snapshotId).build());
    // Replay should not rewrite content when only refreshed_at differs.
    assertEquals(firstRefreshedAt, stored.getMetadata().getRefreshedAt());
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
        TableValueStats.newBuilder()
            .setRowCount(10)
            .setDataFileCount(1)
            .setTotalSizeBytes(100)
            .build();

    var firstCapturedAt = Timestamps.fromMillis(1_700_000_010_000L);
    var secondCapturedAt = Timestamps.fromMillis(1_700_000_020_000L);
    var refreshedAt = Timestamps.fromMillis(1_700_000_030_000L);

    var firstMetadata =
        StatsMetadata.newBuilder()
            .setProducer(StatsProducer.SPROD_SOURCE_NATIVE)
            .setCaptureMode(StatsCaptureMode.SCM_SYNC)
            .setCompleteness(StatsCompleteness.SC_COMPLETE)
            .setCapturedAt(firstCapturedAt)
            .setRefreshedAt(refreshedAt)
            .build();
    var secondMetadata = firstMetadata.toBuilder().setCapturedAt(secondCapturedAt).build();

    putTableStats(tableId, snapshotId, base, firstMetadata, key);

    assertDoesNotThrow(() -> putTableStats(tableId, snapshotId, base, secondMetadata, key));

    var stored = getTableStats(tableId, SnapshotRef.newBuilder().setSnapshotId(snapshotId).build());
    // Replay should not rewrite content when only captured_at differs.
    assertEquals(firstCapturedAt, stored.getMetadata().getCapturedAt());
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
                putTableStats(
                    tableId,
                    snapshotId,
                    TableValueStats.newBuilder()
                        .setRowCount(1)
                        .setDataFileCount(1)
                        .setTotalSizeBytes(1)
                        .build(),
                    StatsMetadata.newBuilder()
                        .setProducer(StatsProducer.SPROD_SOURCE_NATIVE)
                        .setCaptureMode(StatsCaptureMode.SCM_SYNC)
                        .setCompleteness(StatsCompleteness.SC_PARTIAL)
                        .setConfidenceLevel(1.1d)
                        .build(),
                    null));
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
                putTableStats(
                    tableId,
                    snapshotId,
                    TableValueStats.newBuilder()
                        .setRowCount(1)
                        .setDataFileCount(1)
                        .setTotalSizeBytes(1)
                        .build(),
                    StatsMetadata.newBuilder()
                        .setProducer(StatsProducer.SPROD_SOURCE_NATIVE)
                        .setCaptureMode(StatsCaptureMode.SCM_SYNC)
                        .setCompleteness(StatsCompleteness.SC_PARTIAL)
                        .setCoverage(StatsCoverage.newBuilder().setRowsScanned(-1L).build())
                        .build(),
                    null));
    TestSupport.assertGrpcAndMc(
        ex, Status.Code.INVALID_ARGUMENT, ErrorCode.MC_INVALID_ARGUMENT, "rows_scanned");
  }

  private PutTargetStatsResponse putTableStats(
      ResourceId tableId, long snapshotId, TableValueStats tableStats, IdempotencyKey idempotency) {
    return putTableStats(tableId, snapshotId, tableStats, null, idempotency);
  }

  private PutTargetStatsResponse putTableStats(
      ResourceId tableId,
      long snapshotId,
      TableValueStats tableStats,
      StatsMetadata metadata,
      IdempotencyKey idempotency) {
    PutTargetStatsRequest.Builder request =
        PutTargetStatsRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .addRecords(toTableRecord(tableId, snapshotId, tableStats, metadata));
    if (idempotency != null) {
      request.setIdempotency(idempotency);
    }
    return statisticMutiny
        .putTargetStats(io.smallrye.mutiny.Multi.createFrom().item(request.build()))
        .await()
        .atMost(java.time.Duration.ofSeconds(30));
  }

  private record ColumnWrite(long columnId, ScalarStats stats, StatsMetadata metadata) {}

  private PutTargetStatsResponse putColumnStats(
      ResourceId tableId, long snapshotId, IdempotencyKey idempotency, ColumnWrite... columns) {
    PutTargetStatsRequest.Builder request =
        PutTargetStatsRequest.newBuilder().setTableId(tableId).setSnapshotId(snapshotId);
    for (ColumnWrite column : columns) {
      request.addRecords(
          toColumnRecord(
              tableId, snapshotId, column.columnId(), column.stats(), column.metadata()));
    }
    if (idempotency != null) {
      request.setIdempotency(idempotency);
    }
    return statisticMutiny
        .putTargetStats(io.smallrye.mutiny.Multi.createFrom().item(request.build()))
        .await()
        .atMost(java.time.Duration.ofSeconds(30));
  }

  private PutTargetStatsResponse putTargetRecords(
      ResourceId tableId,
      long snapshotId,
      IdempotencyKey idempotency,
      TargetStatsRecord... records) {
    PutTargetStatsRequest.Builder request =
        PutTargetStatsRequest.newBuilder().setTableId(tableId).setSnapshotId(snapshotId);
    request.addAllRecords(List.of(records));
    if (idempotency != null) {
      request.setIdempotency(idempotency);
    }
    return statisticMutiny
        .putTargetStats(io.smallrye.mutiny.Multi.createFrom().item(request.build()))
        .await()
        .atMost(java.time.Duration.ofSeconds(30));
  }

  private TargetStatsRecord getTableStats(ResourceId tableId, SnapshotRef snapshotRef) {
    var rpc =
        statistic.getTargetStats(
            GetTargetStatsRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshot(snapshotRef)
                .setTarget(
                    StatsTarget.newBuilder()
                        .setTable(TableStatsTarget.newBuilder().build())
                        .build())
                .build());
    return rpc.getStats();
  }

  private List<TargetStatsRecord> listColumnStats(
      ResourceId tableId, SnapshotRef snapshotRef, int pageSize) {
    var rpc =
        statistic.listTargetStats(
            ListTargetStatsRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshot(snapshotRef)
                .setPage(PageRequest.newBuilder().setPageSize(pageSize).build())
                .addTargetKinds(StatsTargetKind.STK_COLUMN)
                .build());
    List<TargetStatsRecord> columns = new ArrayList<>();
    for (TargetStatsRecord record : rpc.getRecordsList()) {
      if (record.getTarget().hasColumn()
          && record.getValueCase() == TargetStatsRecord.ValueCase.SCALAR) {
        columns.add(record);
      }
    }
    return columns;
  }

  private static TargetStatsRecord toTableRecord(
      ResourceId tableId, long snapshotId, TableValueStats tableStats, StatsMetadata metadata) {
    TableValueStats.Builder tableValue =
        TableValueStats.newBuilder()
            .setRowCount(tableStats.getRowCount())
            .setDataFileCount(tableStats.getDataFileCount())
            .setTotalSizeBytes(tableStats.getTotalSizeBytes())
            .putAllProperties(tableStats.getPropertiesMap());
    if (tableStats.hasUpstream()) {
      tableValue.setUpstream(tableStats.getUpstream());
    }
    TargetStatsRecord.Builder builder =
        TargetStatsRecord.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setTarget(
                StatsTarget.newBuilder().setTable(TableStatsTarget.newBuilder().build()).build())
            .setTable(tableValue.build());
    if (metadata != null) {
      builder.setMetadata(metadata);
    }
    return builder.build();
  }

  private static TargetStatsRecord toColumnRecord(
      ResourceId tableId,
      long snapshotId,
      long columnId,
      ScalarStats columnStats,
      StatsMetadata metadata) {
    ScalarStats.Builder scalar =
        ScalarStats.newBuilder()
            .setDisplayName(columnStats.getDisplayName())
            .setLogicalType(columnStats.getLogicalType())
            .setValueCount(columnStats.getValueCount())
            .setHistogram(columnStats.getHistogram())
            .setTdigest(columnStats.getTdigest())
            .putAllProperties(columnStats.getPropertiesMap());
    if (columnStats.hasUpstream()) {
      scalar.setUpstream(columnStats.getUpstream());
    }
    if (columnStats.hasNullCount()) {
      scalar.setNullCount(columnStats.getNullCount());
    }
    if (columnStats.hasNanCount()) {
      scalar.setNanCount(columnStats.getNanCount());
    }
    if (columnStats.hasNdv()) {
      scalar.setNdv(columnStats.getNdv());
    }
    if (columnStats.hasMin()) {
      scalar.setMin(columnStats.getMin());
    }
    if (columnStats.hasMax()) {
      scalar.setMax(columnStats.getMax());
    }
    TargetStatsRecord.Builder builder =
        TargetStatsRecord.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setTarget(
                StatsTarget.newBuilder()
                    .setColumn(ColumnStatsTarget.newBuilder().setColumnId(columnId).build())
                    .build())
            .setScalar(scalar.build());
    if (metadata != null) {
      builder.setMetadata(metadata);
    }
    return builder.build();
  }

  // Column stats remain in TargetStatsRecord form in tests so identity and metadata stay explicit.
}
