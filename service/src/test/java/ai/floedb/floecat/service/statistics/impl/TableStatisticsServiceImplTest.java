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
package ai.floedb.floecat.service.statistics.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.FileTargetStats;
import ai.floedb.floecat.catalog.rpc.ListTargetStatsResponse;
import ai.floedb.floecat.catalog.rpc.Ndv;
import ai.floedb.floecat.catalog.rpc.PutTargetStatsRequest;
import ai.floedb.floecat.catalog.rpc.ScalarStats;
import ai.floedb.floecat.catalog.rpc.SketchPayload;
import ai.floedb.floecat.catalog.rpc.SketchRole;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.StatsMetadata;
import ai.floedb.floecat.catalog.rpc.TableValueStats;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.catalog.rpc.TargetStatsView;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.scanner.spi.CatalogGraphView;
import ai.floedb.floecat.service.catalog.impl.TableRootWriter;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.service.statistics.StatsOrchestrator;
import ai.floedb.floecat.service.testsupport.TestNodes;
import ai.floedb.floecat.service.testsupport.TestPrincipals;
import ai.floedb.floecat.stats.identity.TargetStatsRecords;
import ai.floedb.floecat.stats.spi.StatsStore;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.smallrye.mutiny.Multi;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class TableStatisticsServiceImplTest {

  @Test
  void listFetchLimitBoundsStoreReadsBeforeResponseSizing() {
    assertEquals(
        TableStatisticsServiceImpl.LIST_FETCH_MAX_RECORDS,
        TableStatisticsServiceImpl.boundedListLimit(Integer.MAX_VALUE));
    assertEquals(1, TableStatisticsServiceImpl.boundedListLimit(0));
  }

  @Test
  void summaryViewStripsOnlyRawSketchBytesAndDoesNotMutateTheSourceRecord() {
    TargetStatsRecord stored = fileRecord("file-1", 256);

    TargetStatsRecord summary =
        TableStatisticsServiceImpl.buildListResponse(
                new StatsStore.StatsStorePage(List.of(stored), ""),
                1,
                TargetStatsView.TSV_UNSPECIFIED,
                Integer.MAX_VALUE)
            .getRecords(0);

    ScalarStats scalar = summary.getFile().getColumns(0).getScalar();
    assertEquals(17L, scalar.getRowCount());
    assertEquals(2L, scalar.getNullCount());
    assertEquals(1L, scalar.getNanCount());
    assertEquals("10", scalar.getMin());
    assertEquals("99", scalar.getMax());
    assertEquals(12.5, scalar.getNdv().getApprox().getEstimate());
    assertEquals("theta-v1", scalar.getNdv().getSketches(0).getSketchType());
    assertTrue(scalar.getNdv().getSketches(0).getData().isEmpty());
    assertEquals("kll-v1", scalar.getSketches(0).getSketchType());
    assertTrue(scalar.getSketches(0).getData().isEmpty());
    assertEquals(stored.getMetadata(), summary.getMetadata());
    assertFalse(stored.getFile().getColumns(0).getScalar().getSketches(0).getData().isEmpty());
    assertFalse(
        stored.getFile().getColumns(0).getScalar().getNdv().getSketches(0).getData().isEmpty());
  }

  @Test
  void fullViewPreservesRawSketchPayloads() {
    TargetStatsRecord stored = fileRecord("file-1", 256);
    var page = new StatsStore.StatsStorePage(List.of(stored), "");

    ListTargetStatsResponse response =
        TableStatisticsServiceImpl.buildListResponse(
            page, 1, TargetStatsView.TSV_FULL, Integer.MAX_VALUE);

    assertEquals(stored, response.getRecords(0));
    assertFalse(
        response
            .getRecords(0)
            .getFile()
            .getColumns(0)
            .getScalar()
            .getSketches(0)
            .getData()
            .isEmpty());
  }

  @Test
  void byteBudgetReturnsAnExactContinuationAfterTheLastIncludedRecord() {
    TargetStatsRecord first = fileRecord("file-1", 128);
    TargetStatsRecord second = fileRecord("file-2", 128);
    var page =
        new StatsStore.StatsStorePage(
            List.of(first, second), "storage-page-end", List.of("after-file-1", "after-file-2"));
    int bothRecordsBytes =
        ListTargetStatsResponse.newBuilder()
            .addRecords(first)
            .addRecords(second)
            .setPage(
                ai.floedb.floecat.common.rpc.PageResponse.newBuilder()
                    .setNextPageToken("storage-page-end")
                    .setTotalSize(2))
            .build()
            .getSerializedSize();

    ListTargetStatsResponse response =
        TableStatisticsServiceImpl.buildListResponse(
            page, 2, TargetStatsView.TSV_FULL, bothRecordsBytes - 1);

    assertEquals(1, response.getRecordsCount());
    assertEquals(first, response.getRecords(0));
    assertEquals("after-file-1", response.getPage().getNextPageToken());
    assertTrue(response.getSerializedSize() <= bothRecordsBytes - 1);
  }

  @Test
  void recordLimitIsPreservedWhenAllFetchedRecordsFit() {
    TargetStatsRecord first = fileRecord("file-1", 8);
    TargetStatsRecord second = fileRecord("file-2", 8);
    var page =
        new StatsStore.StatsStorePage(
            List.of(first, second), "next-storage-page", List.of("after-file-1", "after-file-2"));

    ListTargetStatsResponse response =
        TableStatisticsServiceImpl.buildListResponse(
            page, 7, TargetStatsView.TSV_SUMMARY, Integer.MAX_VALUE);

    assertEquals(2, response.getRecordsCount());
    assertEquals("next-storage-page", response.getPage().getNextPageToken());
    assertEquals(7, response.getPage().getTotalSize());
  }

  @Test
  void oversizedFullRecordFailsWithResourceExhausted() {
    TargetStatsRecord record = fileRecord("too-large", 1024);
    int tooSmall =
        TableStatisticsServiceImpl.buildListResponse(
                    new StatsStore.StatsStorePage(List.of(record), ""),
                    1,
                    TargetStatsView.TSV_FULL,
                    Integer.MAX_VALUE)
                .getSerializedSize()
            - 1;

    StatusRuntimeException error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                TableStatisticsServiceImpl.buildListResponse(
                    new StatsStore.StatsStorePage(List.of(record), ""),
                    1,
                    TargetStatsView.TSV_FULL,
                    tooSmall));

    assertEquals(Status.Code.RESOURCE_EXHAUSTED, error.getStatus().getCode());
    assertTrue(error.getStatus().getDescription().contains("one target stats record exceeds"));
  }

  @Test
  void putTargetStatsRejectsSystemTableBeforePersistence() {
    var svc = new TableStatisticsServiceImpl();
    svc.snapshots = mock(SnapshotRepository.class);
    svc.statsStore = mock(StatsStore.class);
    svc.principal = mock(PrincipalProvider.class);
    svc.authz = mock(Authorizer.class);
    svc.idempotencyStore = mock(IdempotencyRepository.class);
    svc.graphView = mock(CatalogGraphView.class);

    var tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("sys_stats_table")
            .build();

    when(svc.graphView.resolve(tableId)).thenReturn(Optional.of(TestNodes.systemTableNode(tableId)));
    var pc = TestPrincipals.stubPrincipal(svc.principal, svc.authz);

    var request =
        PutTargetStatsRequest.newBuilder().setTableId(tableId).setSnapshotId(123L).build();

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () -> svc.putTargetStats(Multi.createFrom().item(request)).await().indefinitely());

    assertEquals(Status.Code.PERMISSION_DENIED, ex.getStatus().getCode());
    verifyNoInteractions(svc.snapshots, svc.statsStore, svc.idempotencyStore);
  }

  @Test
  void putTargetStatsCommitsTheGenerationRefOncePerStream() {
    var svc = new TableStatisticsServiceImpl();
    svc.snapshots = mock(SnapshotRepository.class);
    svc.statsStore = mock(StatsStore.class);
    svc.principal = mock(PrincipalProvider.class);
    svc.authz = mock(Authorizer.class);
    svc.idempotencyStore = mock(IdempotencyRepository.class);
    svc.statsOrchestrator = mock(StatsOrchestrator.class);
    svc.graphView = mock(CatalogGraphView.class);
    svc.rootWriter = mock(TableRootWriter.class);

    var tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("tbl")
            .build();
    when(svc.graphView.resolve(tableId)).thenReturn(Optional.of(TestNodes.tableNode(tableId, "{}")));
    when(svc.snapshots.getById(tableId, 123L))
        .thenReturn(
            Optional.of(Snapshot.newBuilder().setTableId(tableId).setSnapshotId(123L).build()));
    TestPrincipals.stubPrincipal(svc.principal, svc.authz);

    var record =
        TargetStatsRecords.tableRecord(
            tableId, 123L, TableValueStats.newBuilder().setRowCount(1L).build(), null);
    var request =
        PutTargetStatsRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(123L)
            .addRecords(record)
            .build();

    svc.putTargetStats(Multi.createFrom().items(request, request)).await().indefinitely();

    // Records write within one active generation; the root ref is recorded once per stream,
    // not per record or per request — the per-record hot path pays no root reads.
    verify(svc.rootWriter, times(1)).commitStatsGeneration(tableId, 123L);
  }

  @Test
  void aStreamThatFailsMidwayNeverPublishesTheGeneration() {
    // The root commit is the generation's PUBLICATION point — and under the visibility gate
    // potentially the snapshot's visibility commit. Publishing on the first chunk would let
    // queries pin a generation whose later chunks were still in flight; a failed stream must
    // leave it unpublished (the previous generation keeps serving, the live-active pointer
    // protects the partial write from GC until a retry completes).
    var svc = new TableStatisticsServiceImpl();
    svc.snapshots = mock(SnapshotRepository.class);
    svc.statsStore = mock(StatsStore.class);
    svc.principal = mock(PrincipalProvider.class);
    svc.authz = mock(Authorizer.class);
    svc.idempotencyStore = mock(IdempotencyRepository.class);
    svc.statsOrchestrator = mock(StatsOrchestrator.class);
    svc.graphView = mock(CatalogGraphView.class);
    svc.rootWriter = mock(TableRootWriter.class);

    var tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("tbl")
            .build();
    when(svc.graphView.resolve(tableId)).thenReturn(Optional.of(TestNodes.tableNode(tableId, "{}")));
    when(svc.snapshots.getById(tableId, 123L))
        .thenReturn(
            Optional.of(Snapshot.newBuilder().setTableId(tableId).setSnapshotId(123L).build()));
    TestPrincipals.stubPrincipal(svc.principal, svc.authz);

    var record =
        TargetStatsRecords.tableRecord(
            tableId, 123L, TableValueStats.newBuilder().setRowCount(1L).build(), null);
    var goodChunk =
        PutTargetStatsRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(123L)
            .addRecords(record)
            .build();
    // The second chunk targets a different snapshot: STATS_INCONSISTENT_TARGET fails the stream
    // after the first chunk's records already persisted.
    var badChunk =
        PutTargetStatsRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(456L)
            .addRecords(record)
            .build();

    assertThrows(
        StatusRuntimeException.class,
        () ->
            svc.putTargetStats(Multi.createFrom().items(goodChunk, badChunk))
                .await()
                .indefinitely());

    verify(svc.rootWriter, times(0))
        .commitStatsGeneration(
            org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyLong());
  }

  private static TargetStatsRecord fileRecord(String path, int sketchBytes) {
    SketchPayload ndvSketch =
        SketchPayload.newBuilder()
            .setRole(SketchRole.SKETCH_ROLE_NDV)
            .setSketchType("theta-v1")
            .setData(ByteString.copyFrom(new byte[sketchBytes]))
            .putParams("k", "4096")
            .setCapturedAtMs(123L)
            .build();
    SketchPayload scalarSketch =
        SketchPayload.newBuilder()
            .setRole(SketchRole.SKETCH_ROLE_QUANTILES)
            .setSketchType("kll-v1")
            .setData(ByteString.copyFrom(new byte[sketchBytes]))
            .putParams("k", "200")
            .setCapturedAtMs(456L)
            .build();
    ScalarStats scalar =
        ScalarStats.newBuilder()
            .setDisplayName("c1")
            .setLogicalType("BIGINT")
            .setRowCount(17L)
            .setNullCount(2L)
            .setNanCount(1L)
            .setMin("10")
            .setMax("99")
            .setNdv(
                Ndv.newBuilder()
                    .setApprox(
                        ai.floedb.floecat.catalog.rpc.NdvApprox.newBuilder().setEstimate(12.5))
                    .addSketches(ndvSketch))
            .addSketches(scalarSketch)
            .putProperties("source", "test")
            .build();
    return TargetStatsRecord.newBuilder()
        .setTableId(
            ResourceId.newBuilder()
                .setAccountId("acct")
                .setKind(ResourceKind.RK_TABLE)
                .setId("tbl"))
        .setSnapshotId(123L)
        .setMetadata(StatsMetadata.newBuilder().putProperties("capture", "footer"))
        .setFile(
            FileTargetStats.newBuilder()
                .setFilePath(path)
                .setRowCount(17L)
                .addColumns(FileColumnStats.newBuilder().setColumnId(1L).setScalar(scalar)))
        .build();
  }
}
