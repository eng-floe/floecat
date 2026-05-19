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

package ai.floedb.floecat.service.statistics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.ColumnStatsTarget;
import ai.floedb.floecat.catalog.rpc.EngineExpressionStatsTarget;
import ai.floedb.floecat.catalog.rpc.FileStatsTarget;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableStatsTarget;
import ai.floedb.floecat.catalog.rpc.TableValueStats;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.reconciler.impl.ReconcilerService;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchRequest;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchResult;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsExecutionMode;
import ai.floedb.floecat.stats.spi.StatsResolutionResult;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.stats.spi.StatsSyncOutcome;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class StatsOrchestratorTest {

  // ---------------------------------------------------------------------------
  // Store-hit path
  // ---------------------------------------------------------------------------

  @Test
  void returnsStoreHitDirectly() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsSyncCapture syncCapture = Mockito.mock(StatsSyncCapture.class);
    StatsOrchestrator orchestrator =
        orchestrator(statsStore, jobStore, tableRepository, syncCapture);

    StatsCaptureRequest request = tableRequest(StatsExecutionMode.SYNC);
    TargetStatsRecord record = record(request);
    when(statsStore.getTargetStats(request.tableId(), request.snapshotId(), request.target()))
        .thenReturn(Optional.of(record));

    StatsResolutionResult result = orchestrator.resolve(request);

    assertThat(result.outcome()).isEqualTo(StatsSyncOutcome.HIT);
    assertThat(result.stats()).contains(record);
    verify(jobStore, never()).enqueue(anyString(), anyString(), anyBoolean(), any(), any());
    verify(syncCapture, never()).capture(anyString(), anyString(), any(), any());
  }

  // ---------------------------------------------------------------------------
  // Sync-first path
  // ---------------------------------------------------------------------------

  @Test
  void syncMissThenCaptureSucceeds() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsSyncCapture syncCapture = Mockito.mock(StatsSyncCapture.class);
    StatsOrchestrator orchestrator =
        orchestrator(statsStore, jobStore, tableRepository, syncCapture);

    StatsCaptureRequest request =
        tableRequest(StatsExecutionMode.SYNC, Optional.of(Duration.ofSeconds(1)));
    TargetStatsRecord record = record(request);
    when(statsStore.getTargetStats(request.tableId(), request.snapshotId(), request.target()))
        .thenReturn(Optional.empty()) // first read: miss
        .thenReturn(Optional.of(record)); // second read after capture: hit
    when(tableRepository.getById(request.tableId())).thenReturn(Optional.of(upstreamTable()));
    when(syncCapture.capture(anyString(), anyString(), any(), any()))
        .thenReturn(StatsSyncOutcome.CAPTURED);

    StatsResolutionResult result = orchestrator.resolve(request);

    assertThat(result.outcome()).isEqualTo(StatsSyncOutcome.CAPTURED);
    assertThat(result.stats()).contains(record);
    // No async enqueue because sync succeeded.
    verify(jobStore, never()).enqueue(anyString(), anyString(), anyBoolean(), any(), any());
  }

  @Test
  void syncMissTimeoutEnqueuesAsyncFollowUp() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsSyncCapture syncCapture = Mockito.mock(StatsSyncCapture.class);
    StatsOrchestrator orchestrator =
        orchestrator(statsStore, jobStore, tableRepository, syncCapture);

    StatsCaptureRequest request =
        tableRequest(StatsExecutionMode.SYNC, Optional.of(Duration.ofSeconds(1)));
    when(statsStore.getTargetStats(request.tableId(), request.snapshotId(), request.target()))
        .thenReturn(Optional.empty());
    when(tableRepository.getById(request.tableId())).thenReturn(Optional.of(upstreamTable()));
    when(syncCapture.capture(anyString(), anyString(), any(), any()))
        .thenReturn(StatsSyncOutcome.TIMEOUT);
    when(jobStore.enqueue(anyString(), anyString(), anyBoolean(), any(), any()))
        .thenReturn("job-followup");

    StatsResolutionResult result = orchestrator.resolve(request);

    assertThat(result.outcome()).isEqualTo(StatsSyncOutcome.TIMEOUT);
    assertThat(result.stats()).isEmpty();
    assertThat(result.outcomeDetail()).contains("async follow-up enqueued");
    verify(jobStore).enqueue(anyString(), anyString(), anyBoolean(), any(), any());
  }

  @Test
  void syncMissFailedEnqueuesAsyncFollowUp() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsSyncCapture syncCapture = Mockito.mock(StatsSyncCapture.class);
    StatsOrchestrator orchestrator =
        orchestrator(statsStore, jobStore, tableRepository, syncCapture);

    StatsCaptureRequest request =
        tableRequest(StatsExecutionMode.SYNC, Optional.of(Duration.ofSeconds(1)));
    when(statsStore.getTargetStats(request.tableId(), request.snapshotId(), request.target()))
        .thenReturn(Optional.empty());
    when(tableRepository.getById(request.tableId())).thenReturn(Optional.of(upstreamTable()));
    when(syncCapture.capture(anyString(), anyString(), any(), any()))
        .thenReturn(StatsSyncOutcome.FAILED);
    when(jobStore.enqueue(anyString(), anyString(), anyBoolean(), any(), any()))
        .thenReturn("job-followup");

    StatsResolutionResult result = orchestrator.resolve(request);

    assertThat(result.outcome()).isEqualTo(StatsSyncOutcome.FAILED);
    assertThat(result.stats()).isEmpty();
    verify(jobStore).enqueue(anyString(), anyString(), anyBoolean(), any(), any());
  }

  // ---------------------------------------------------------------------------
  // Async-only fallback (sync not attempted)
  // ---------------------------------------------------------------------------

  @Test
  void missEnqueuesUnifiedCaptureJob() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsOrchestrator orchestrator = new StatsOrchestrator(statsStore, jobStore, tableRepository);

    StatsCaptureRequest request = tableRequest(StatsExecutionMode.SYNC);
    when(statsStore.getTargetStats(request.tableId(), request.snapshotId(), request.target()))
        .thenReturn(Optional.empty());
    when(tableRepository.getById(request.tableId())).thenReturn(Optional.of(upstreamTable()));
    when(jobStore.enqueue(anyString(), anyString(), anyBoolean(), any(), any()))
        .thenReturn("job-1");

    StatsResolutionResult result = orchestrator.resolve(request);

    assertThat(result.outcome()).isEqualTo(StatsSyncOutcome.SKIPPED);
    assertThat(result.stats()).isEmpty();
    ArgumentCaptor<ReconcileScope> scopeCaptor = ArgumentCaptor.forClass(ReconcileScope.class);
    verify(jobStore)
        .enqueue(
            Mockito.eq(request.tableId().getAccountId()),
            Mockito.eq("connector-1"),
            Mockito.eq(false),
            Mockito.eq(ReconcilerService.CaptureMode.CAPTURE_ONLY),
            scopeCaptor.capture());
    ReconcileScope scope = scopeCaptor.getValue();
    assertThat(scope.destinationTableId()).isEqualTo(request.tableId().getId());
    assertThat(scope.destinationCaptureRequests()).hasSize(1);
    assertThat(scope.capturePolicy().outputs())
        .containsExactly(ReconcileCapturePolicy.Output.TABLE_STATS);
    assertThat(scope.capturePolicy().columns())
        .extracting(ReconcileCapturePolicy.Column::selector)
        .containsExactlyInAnyOrder("id", "region");
    assertThat(scope.capturePolicy().selectorsForStats()).containsExactlyInAnyOrder("id", "region");
  }

  // ---------------------------------------------------------------------------
  // triggerBatch path (unchanged semantics)
  // ---------------------------------------------------------------------------

  @Test
  void triggerBatchGroupsRequestsIntoSingleTableJob() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsOrchestrator orchestrator = new StatsOrchestrator(statsStore, jobStore, tableRepository);

    StatsCaptureRequest tableRequest = tableRequest(StatsExecutionMode.ASYNC);
    StatsCaptureRequest columnRequest =
        StatsCaptureRequest.builder(
                tableRequest.tableId(),
                tableRequest.snapshotId(),
                StatsTarget.newBuilder()
                    .setColumn(ColumnStatsTarget.newBuilder().setColumnId(9).build())
                    .build())
            .columnSelectors(Set.of("region"))
            .executionMode(StatsExecutionMode.ASYNC)
            .connectorType("iceberg")
            .correlationId("cid-1")
            .build();
    when(tableRepository.getById(tableRequest.tableId())).thenReturn(Optional.of(upstreamTable()));
    when(jobStore.enqueue(anyString(), anyString(), anyBoolean(), any(), any()))
        .thenReturn("job-2");

    StatsCaptureBatchResult result =
        orchestrator.triggerBatch(
            StatsCaptureBatchRequest.of(List.of(tableRequest, columnRequest)));

    assertThat(result.results()).hasSize(2);
    assertThat(result.results()).allMatch(item -> item.outcome().name().equals("QUEUED"));
    ArgumentCaptor<ReconcileScope> scopeCaptor = ArgumentCaptor.forClass(ReconcileScope.class);
    verify(jobStore)
        .enqueue(
            Mockito.eq(tableRequest.tableId().getAccountId()),
            Mockito.eq("connector-1"),
            Mockito.eq(false),
            Mockito.eq(ReconcilerService.CaptureMode.CAPTURE_ONLY),
            scopeCaptor.capture());
    ReconcileScope scope = scopeCaptor.getValue();
    assertThat(scope.destinationCaptureRequests()).hasSize(2);
    assertThat(scope.capturePolicy().outputs())
        .containsExactlyInAnyOrder(
            ReconcileCapturePolicy.Output.TABLE_STATS, ReconcileCapturePolicy.Output.COLUMN_STATS);
    assertThat(scope.capturePolicy().selectorsForStats())
        .containsExactlyInAnyOrder("id", "region", "#9");
  }

  @Test
  void triggerBatchDerivesSelectorFromColumnTarget() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsOrchestrator orchestrator = new StatsOrchestrator(statsStore, jobStore, tableRepository);

    StatsCaptureRequest columnRequest =
        StatsCaptureRequest.builder(
                ResourceId.newBuilder().setAccountId("acct").setId("table-1").build(),
                42L,
                StatsTarget.newBuilder()
                    .setColumn(ColumnStatsTarget.newBuilder().setColumnId(9).build())
                    .build())
            .executionMode(StatsExecutionMode.ASYNC)
            .connectorType("iceberg")
            .correlationId("cid-1")
            .build();
    when(tableRepository.getById(columnRequest.tableId())).thenReturn(Optional.of(upstreamTable()));
    when(jobStore.enqueue(anyString(), anyString(), anyBoolean(), any(), any()))
        .thenReturn("job-3");

    orchestrator.triggerBatch(StatsCaptureBatchRequest.of(List.of(columnRequest)));

    ArgumentCaptor<ReconcileScope> scopeCaptor = ArgumentCaptor.forClass(ReconcileScope.class);
    verify(jobStore)
        .enqueue(
            Mockito.eq(columnRequest.tableId().getAccountId()),
            Mockito.eq("connector-1"),
            Mockito.eq(false),
            Mockito.eq(ReconcilerService.CaptureMode.CAPTURE_ONLY),
            scopeCaptor.capture());
    assertThat(scopeCaptor.getValue().capturePolicy().outputs())
        .containsExactly(ReconcileCapturePolicy.Output.COLUMN_STATS);
    assertThat(scopeCaptor.getValue().capturePolicy().selectorsForStats()).containsExactly("#9");
  }

  @Test
  void triggerBatchRejectsFileTargetBeforeEnqueue() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsOrchestrator orchestrator = new StatsOrchestrator(statsStore, jobStore, tableRepository);

    StatsCaptureRequest request =
        StatsCaptureRequest.builder(
                ResourceId.newBuilder().setAccountId("acct").setId("table-1").build(),
                42L,
                StatsTarget.newBuilder()
                    .setFile(
                        FileStatsTarget.newBuilder()
                            .setFilePath("s3://bucket/file.parquet")
                            .build())
                    .build())
            .executionMode(StatsExecutionMode.ASYNC)
            .connectorType("iceberg")
            .correlationId("cid-1")
            .build();

    StatsCaptureBatchResult result =
        orchestrator.triggerBatch(StatsCaptureBatchRequest.of(List.of(request)));

    assertThat(result.results())
        .singleElement()
        .satisfies(
            item -> {
              assertThat(item.request()).isEqualTo(request);
              assertThat(item.outcome().name()).isEqualTo("UNCAPTURABLE");
              assertThat(item.detail()).contains("file-group scoped");
            });
    verify(jobStore, never()).enqueue(anyString(), anyString(), anyBoolean(), any(), any());
    verify(tableRepository, never()).getById(any());
  }

  @Test
  void triggerBatchRejectsExpressionTargetBeforeEnqueue() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsOrchestrator orchestrator = new StatsOrchestrator(statsStore, jobStore, tableRepository);

    StatsCaptureRequest request =
        StatsCaptureRequest.builder(
                ResourceId.newBuilder().setAccountId("acct").setId("table-1").build(),
                42L,
                StatsTarget.newBuilder()
                    .setExpression(
                        EngineExpressionStatsTarget.newBuilder()
                            .setEngineKind("trino")
                            .setEngineExpressionKey(
                                com.google.protobuf.ByteString.copyFromUtf8("x + 1"))
                            .build())
                    .build())
            .executionMode(StatsExecutionMode.ASYNC)
            .connectorType("iceberg")
            .correlationId("cid-1")
            .build();

    StatsCaptureBatchResult result =
        orchestrator.triggerBatch(StatsCaptureBatchRequest.of(List.of(request)));

    assertThat(result.results())
        .singleElement()
        .satisfies(
            item -> {
              assertThat(item.request()).isEqualTo(request);
              assertThat(item.outcome().name()).isEqualTo("UNCAPTURABLE");
              assertThat(item.detail()).contains("not yet implemented");
            });
    verify(jobStore, never()).enqueue(anyString(), anyString(), anyBoolean(), any(), any());
    verify(tableRepository, never()).getById(any());
  }

  @Test
  void triggerBatchReturnsUncapturableWhenTableLookupFails() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsOrchestrator orchestrator = new StatsOrchestrator(statsStore, jobStore, tableRepository);

    StatsCaptureRequest request = tableRequest(StatsExecutionMode.ASYNC);
    when(tableRepository.getById(request.tableId())).thenReturn(Optional.empty());

    StatsCaptureBatchResult result =
        orchestrator.triggerBatch(StatsCaptureBatchRequest.of(List.of(request)));

    assertThat(result.results())
        .singleElement()
        .satisfies(
            item -> {
              assertThat(item.request()).isEqualTo(request);
              assertThat(item.outcome().name()).isEqualTo("UNCAPTURABLE");
              assertThat(item.detail()).isEqualTo("missing_table");
            });
    verify(jobStore, never()).enqueue(anyString(), anyString(), anyBoolean(), any(), any());
  }

  @Test
  void triggerBatchReturnsUncapturableWhenConnectorIdIsBlank() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsOrchestrator orchestrator = new StatsOrchestrator(statsStore, jobStore, tableRepository);

    StatsCaptureRequest request = tableRequest(StatsExecutionMode.ASYNC);
    when(tableRepository.getById(request.tableId()))
        .thenReturn(Optional.of(upstreamTableWithConnectorId(" ")));

    StatsCaptureBatchResult result =
        orchestrator.triggerBatch(StatsCaptureBatchRequest.of(List.of(request)));

    assertThat(result.results())
        .singleElement()
        .satisfies(
            item -> {
              assertThat(item.request()).isEqualTo(request);
              assertThat(item.outcome().name()).isEqualTo("UNCAPTURABLE");
              assertThat(item.detail()).isEqualTo("blank_connector_id");
            });
    verify(jobStore, never()).enqueue(anyString(), anyString(), anyBoolean(), any(), any());
  }

  @Test
  void triggerBatchReturnsDegradedWhenEnqueueFails() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsOrchestrator orchestrator = new StatsOrchestrator(statsStore, jobStore, tableRepository);

    StatsCaptureRequest request = tableRequest(StatsExecutionMode.ASYNC);
    when(tableRepository.getById(request.tableId())).thenReturn(Optional.of(upstreamTable()));
    doThrow(new RuntimeException("boom"))
        .when(jobStore)
        .enqueue(anyString(), anyString(), anyBoolean(), any(), any());

    StatsCaptureBatchResult result =
        orchestrator.triggerBatch(StatsCaptureBatchRequest.of(List.of(request)));

    assertThat(result.results())
        .singleElement()
        .satisfies(
            item -> {
              assertThat(item.request()).isEqualTo(request);
              assertThat(item.outcome().name()).isEqualTo("DEGRADED");
              assertThat(item.detail()).isEqualTo("failed to enqueue reconcile capture");
            });
  }

  @Test
  void triggerBatchReturnsMixedPerItemOutcomesAcrossGroups() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsOrchestrator orchestrator = new StatsOrchestrator(statsStore, jobStore, tableRepository);

    StatsCaptureRequest accepted = tableRequest(StatsExecutionMode.ASYNC);
    StatsCaptureRequest skipped =
        StatsCaptureRequest.builder(
                ResourceId.newBuilder().setAccountId("acct").setId("table-2").build(),
                99L,
                StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()).build())
            .executionMode(StatsExecutionMode.ASYNC)
            .connectorType("iceberg")
            .correlationId("cid-2")
            .build();
    when(tableRepository.getById(accepted.tableId())).thenReturn(Optional.of(upstreamTable()));
    when(tableRepository.getById(skipped.tableId())).thenReturn(Optional.empty());
    when(jobStore.enqueue(anyString(), anyString(), anyBoolean(), any(), any()))
        .thenReturn("job-2");

    StatsCaptureBatchResult result =
        orchestrator.triggerBatch(StatsCaptureBatchRequest.of(List.of(accepted, skipped)));

    assertThat(result.results()).hasSize(2);
    assertThat(result.results().get(0).request()).isEqualTo(accepted);
    assertThat(result.results().get(0).outcome().name()).isEqualTo("QUEUED");
    assertThat(result.results().get(1).request()).isEqualTo(skipped);
    assertThat(result.results().get(1).outcome().name()).isEqualTo("UNCAPTURABLE");
    assertThat(result.results().get(1).detail()).isEqualTo("missing_table");
  }

  @Test
  void invalidSnapshotIsRejectedBeforeOrchestration() {
    assertThatThrownBy(
            () ->
                StatsCaptureRequest.builder(
                        ResourceId.newBuilder().setAccountId("acct").setId("table-1").build(),
                        -1L,
                        StatsTarget.newBuilder()
                            .setTable(TableStatsTarget.getDefaultInstance())
                            .build())
                    .executionMode(StatsExecutionMode.ASYNC)
                    .connectorType("iceberg")
                    .correlationId("cid-1")
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("snapshotId must be non-negative");
  }

  // ---------------------------------------------------------------------------
  // Async follow-up scope preservation
  // ---------------------------------------------------------------------------

  @Test
  void syncFollowUpPreservesColumnSelectorsFromOriginalRequest() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsSyncCapture syncCapture = Mockito.mock(StatsSyncCapture.class);
    StatsOrchestrator orchestrator =
        orchestrator(statsStore, jobStore, tableRepository, syncCapture);

    StatsCaptureRequest request =
        StatsCaptureRequest.builder(
                ResourceId.newBuilder().setAccountId("acct").setId("table-1").build(),
                42L,
                StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()).build())
            .columnSelectors(Set.of("col_a", "col_b"))
            .executionMode(StatsExecutionMode.SYNC)
            .correlationId("cid-1")
            .latencyBudget(Optional.of(Duration.ofSeconds(1)))
            .build();
    when(statsStore.getTargetStats(any(), Mockito.anyLong(), any())).thenReturn(Optional.empty());
    when(tableRepository.getById(any())).thenReturn(Optional.of(upstreamTable()));
    when(syncCapture.capture(anyString(), anyString(), any(), any()))
        .thenReturn(StatsSyncOutcome.TIMEOUT);
    when(jobStore.enqueue(anyString(), anyString(), anyBoolean(), any(), any()))
        .thenReturn("job-followup");

    orchestrator.resolve(request);

    ArgumentCaptor<ReconcileScope> scopeCaptor = ArgumentCaptor.forClass(ReconcileScope.class);
    verify(jobStore).enqueue(anyString(), anyString(), anyBoolean(), any(), scopeCaptor.capture());
    // Selectors from the original request must be preserved in the follow-up scope.
    assertThat(scopeCaptor.getValue().capturePolicy().selectorsForStats())
        .containsExactlyInAnyOrder("col_a", "col_b");
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static StatsOrchestrator orchestrator(
      StatsStore statsStore,
      ReconcileJobStore jobStore,
      TableRepository tableRepository,
      StatsSyncCapture syncCapture) {
    return new StatsOrchestrator(statsStore, jobStore, tableRepository, syncCapture, true, null);
  }

  private static StatsCaptureRequest tableRequest(StatsExecutionMode mode) {
    return tableRequest(mode, Optional.empty());
  }

  private static StatsCaptureRequest tableRequest(
      StatsExecutionMode mode, Optional<Duration> budget) {
    return StatsCaptureRequest.builder(
            ResourceId.newBuilder().setAccountId("acct").setId("table-1").build(),
            42L,
            StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()).build())
        .columnSelectors(Set.of("id", "region"))
        .executionMode(mode)
        .connectorType("iceberg")
        .correlationId("cid-1")
        .latencyBudget(budget)
        .build();
  }

  private static TargetStatsRecord record(StatsCaptureRequest request) {
    return TargetStatsRecord.newBuilder()
        .setTableId(request.tableId())
        .setSnapshotId(request.snapshotId())
        .setTarget(request.target())
        .setTable(TableValueStats.newBuilder().setRowCount(7).setTotalSizeBytes(11).build())
        .build();
  }

  private static Table upstreamTable() {
    return upstreamTableWithConnectorId("connector-1");
  }

  private static Table upstreamTableWithConnectorId(String connectorId) {
    return Table.newBuilder()
        .setResourceId(ResourceId.newBuilder().setAccountId("acct").setId("table-1").build())
        .setUpstream(
            UpstreamRef.newBuilder()
                .setConnectorId(
                    ResourceId.newBuilder().setAccountId("acct").setId(connectorId).build())
                .build())
        .build();
  }
}
