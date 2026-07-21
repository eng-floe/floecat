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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.PutTargetStatsRequest;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.TableValueStats;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
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
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.smallrye.mutiny.Multi;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class TableStatisticsServiceImplTest {

  @Test
  void putTargetStatsRejectsSystemTableBeforePersistence() {
    var svc = new TableStatisticsServiceImpl();
    svc.snapshots = mock(SnapshotRepository.class);
    svc.statsStore = mock(StatsStore.class);
    svc.principal = mock(PrincipalProvider.class);
    svc.authz = mock(Authorizer.class);
    svc.idempotencyStore = mock(IdempotencyRepository.class);
    svc.overlay = mock(CatalogOverlay.class);

    var tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("sys_stats_table")
            .build();

    when(svc.overlay.resolve(tableId)).thenReturn(Optional.of(TestNodes.systemTableNode(tableId)));
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
    svc.overlay = mock(CatalogOverlay.class);
    svc.rootWriter = mock(TableRootWriter.class);

    var tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("tbl")
            .build();
    when(svc.overlay.resolve(tableId)).thenReturn(Optional.of(TestNodes.tableNode(tableId, "{}")));
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
    svc.overlay = mock(CatalogOverlay.class);
    svc.rootWriter = mock(TableRootWriter.class);

    var tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("tbl")
            .build();
    when(svc.overlay.resolve(tableId)).thenReturn(Optional.of(TestNodes.tableNode(tableId, "{}")));
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
}
