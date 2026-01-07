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
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import ai.floedb.floecat.storage.BlobStore;
import ai.floedb.floecat.storage.PointerStore;
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
                    .setFetchedAt(Timestamps.fromMillis(System.currentTimeMillis()))
                    .build())
            .setRowCount(1_000)
            .setDataFileCount(5)
            .setTotalSizeBytes(123_456)
            .setNdv(Ndv.newBuilder().setExact(950).build())
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

    var listColsOld =
        statistic.listColumnStats(
            ListColumnStatsRequest.newBuilder()
                .setTableId(tblId)
                .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapOld).build())
                .setPage(PageRequest.newBuilder().setPageSize(100).build())
                .build());
    assertEquals(2, listColsOld.getColumnsCount());
    assertEquals(1, listColsOld.getColumns(0).getColumnId());

    var tableStatsNew =
        tableStatsOld.toBuilder()
            .setSnapshotId(snapNew)
            .setRowCount(2_500)
            .setDataFileCount(9)
            .setTotalSizeBytes(987_654)
            .setUpstream(
                tableStatsOld.getUpstream().toBuilder()
                    .setCommitRef(Long.toString(snapNew))
                    .setFetchedAt(Timestamps.fromMillis(System.currentTimeMillis()))
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
}
