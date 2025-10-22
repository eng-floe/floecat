package ai.floedb.metacat.service.it;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.*;
import ai.floedb.metacat.common.rpc.ErrorCode;
import ai.floedb.metacat.common.rpc.PageRequest;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.common.rpc.SnapshotRef;
import ai.floedb.metacat.common.rpc.SpecialSnapshot;
import ai.floedb.metacat.service.storage.BlobStore;
import ai.floedb.metacat.service.storage.PointerStore;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

@QuarkusTest
class StatsIT {
  @Inject PointerStore ptr;
  @Inject BlobStore blob;

  @GrpcClient("resource-mutation")
  ResourceMutationGrpc.ResourceMutationBlockingStub mutation;

  @GrpcClient("resource-access")
  ResourceAccessGrpc.ResourceAccessBlockingStub access;

  @GrpcClient("directory")
  DirectoryGrpc.DirectoryBlockingStub directory;

  @GrpcClient("stats-mutation")
  StatsMutationGrpc.StatsMutationBlockingStub statsMutation;

  @GrpcClient("stats-access")
  StatsAccessGrpc.StatsAccessBlockingStub statsAccess;

  String tablePrefix = this.getClass().getSimpleName() + "_";

  @Test
  void snapshot_stats_create_list_get_and_current() throws Exception {
    var catName = tablePrefix + "cat_stats";
    var cat = TestSupport.createCatalog(mutation, catName, "cat for stats");
    String tenantId = TestSupport.seedTenantId(directory, catName);
    assertEquals(tenantId, cat.getResourceId().getTenantId());

    var parents = List.of("db_stats", "schema_stats");
    var nsLeaf = "it_ns";
    var ns =
        TestSupport.createNamespace(mutation, cat.getResourceId(), nsLeaf, parents, "ns for stats");
    var nsId = ns.getResourceId();
    assertEquals(ResourceKind.RK_NAMESPACE, nsId.getKind());

    var nsPath = new ArrayList<>(parents);
    nsPath.add(nsLeaf);
    var tbl =
        TestSupport.createTable(
            mutation,
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

    TestSupport.createSnapshot(mutation, tblId, snapOld, System.currentTimeMillis() - 86_400_000L);
    TestSupport.createSnapshot(mutation, tblId, snapNew, System.currentTimeMillis());

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
            .setNdv(Ndv.newBuilder().setKind(NdvKind.NK_EXACT).setExact(950).build())
            .build();

    var putTableRespOld =
        statsMutation.putTableStats(
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
            .setColumnId("id")
            .setColumnName("id")
            .setLogicalType("int")
            .setNullCount(0)
            .setNdv(Ndv.newBuilder().setKind(NdvKind.NK_EXACT).setExact(1_000).build())
            .setMin("1")
            .setMax("1000")
            .setUpstream(tableStatsOld.getUpstream())
            .build();

    var colTsStats =
        ColumnStats.newBuilder()
            .setTableId(tblId)
            .setSnapshotId(snapOld)
            .setColumnId("ts")
            .setColumnName("ts")
            .setLogicalType("timestamp")
            .setNullCount(10)
            .setNdv(
                Ndv.newBuilder()
                    .setKind(NdvKind.NK_HLL)
                    .setHllSketch(ByteString.copyFrom(new byte[] {1, 2, 3, 4})))
            .setMin("2024-01-01T00:00:00Z")
            .setMax("2024-12-31T23:59:59Z")
            .setUpstream(tableStatsOld.getUpstream())
            .build();

    var putColsRespOld =
        statsMutation.putColumnStatsBatch(
            PutColumnStatsBatchRequest.newBuilder()
                .setTableId(tblId)
                .setSnapshotId(snapOld)
                .addColumns(colIdStats)
                .addColumns(colTsStats)
                .build());
    assertTrue(putColsRespOld.getUpserted() >= 2);

    var gotTableOld =
        statsAccess.getTableStats(
            GetTableStatsRequest.newBuilder()
                .setTableId(tblId)
                .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapOld).build())
                .build());
    assertEquals(1_000, gotTableOld.getStats().getRowCount());
    assertEquals(123_456, gotTableOld.getStats().getTotalSizeBytes());

    var listColsOld =
        statsAccess.listColumnStats(
            ListColumnStatsRequest.newBuilder()
                .setTableId(tblId)
                .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapOld).build())
                .setPage(PageRequest.newBuilder().setPageSize(100).build())
                .build());
    assertEquals(2, listColsOld.getColumnsCount());
    assertEquals("id", listColsOld.getColumns(0).getColumnId());

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

    statsMutation.putTableStats(
        PutTableStatsRequest.newBuilder()
            .setTableId(tblId)
            .setSnapshotId(snapNew)
            .setStats(tableStatsNew)
            .build());

    statsMutation.putColumnStatsBatch(
        PutColumnStatsBatchRequest.newBuilder()
            .setTableId(tblId)
            .setSnapshotId(snapNew)
            .addColumns(
                colIdStats.toBuilder()
                    .setSnapshotId(snapNew)
                    .setNdv(Ndv.newBuilder().setKind(NdvKind.NK_EXACT).setExact(2_500)))
            .addColumns(
                colTsStats.toBuilder()
                    .setSnapshotId(snapNew)
                    .setNdv(
                        Ndv.newBuilder()
                            .setKind(NdvKind.NK_HLL)
                            .setHllSketch(ByteString.copyFrom(new byte[] {5, 6}))))
            .build());

    var currentTableStats =
        statsAccess.getTableStats(
            GetTableStatsRequest.newBuilder()
                .setTableId(tblId)
                .setSnapshot(SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT))
                .build());
    assertEquals(snapNew, currentTableStats.getStats().getSnapshotId());
    assertEquals(2_500, currentTableStats.getStats().getRowCount());

    var currentCols =
        statsAccess.listColumnStats(
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
                statsAccess.getTableStats(
                    GetTableStatsRequest.newBuilder()
                        .setTableId(tblId)
                        .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(9_999_999L).build())
                        .build()));
    TestSupport.assertGrpcAndMc(
        notFound, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, "not found");
  }
}
