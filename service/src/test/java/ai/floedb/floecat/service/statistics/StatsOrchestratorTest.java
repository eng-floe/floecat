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
import static org.mockito.ArgumentMatchers.anyLong;
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
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchRequest;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchResult;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsExecutionMode;
import ai.floedb.floecat.stats.spi.StatsResolutionResult;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.stats.spi.StatsSyncOutcome;
import java.time.Duration;
import java.util.List;
import java.util.Map;
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
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, connectorRepositoryWith());

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

  @Test
  void missDoesNotEnqueueWhenCanonicalConnectorIsDeleted() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, connectorRepositoryMissing());

    StatsCaptureRequest request = tableRequest(StatsExecutionMode.SYNC);
    when(statsStore.getTargetStats(request.tableId(), request.snapshotId(), request.target()))
        .thenReturn(Optional.empty());
    when(tableRepository.getById(request.tableId())).thenReturn(Optional.of(upstreamTable()));

    StatsResolutionResult result = orchestrator.resolve(request);

    assertThat(result.outcome()).isEqualTo(StatsSyncOutcome.SKIPPED);
    assertThat(result.stats()).isEmpty();
    verify(jobStore, never()).enqueue(anyString(), anyString(), anyBoolean(), any(), any());
  }

  // ---------------------------------------------------------------------------
  // triggerBatch path (unchanged semantics)
  // ---------------------------------------------------------------------------

  @Test
  void triggerBatchGroupsRequestsIntoSingleTableJob() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, connectorRepositoryWith());

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
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, connectorRepositoryWith());

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
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, connectorRepositoryWith());

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
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, connectorRepositoryWith());

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
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, connectorRepositoryWith());

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
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, connectorRepositoryWith());

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
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, connectorRepositoryWith());

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
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, connectorRepositoryWith());

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
    return new StatsOrchestrator(
        statsStore, jobStore, tableRepository, connectorRepositoryWith(), syncCapture, true, null);
  }

  private static ConnectorRepository connectorRepositoryWith() {
    ConnectorRepository connectorRepository = Mockito.mock(ConnectorRepository.class);
    when(connectorRepository.existsById(any())).thenReturn(true);
    return connectorRepository;
  }

  private static ConnectorRepository connectorRepositoryMissing() {
    ConnectorRepository connectorRepository = Mockito.mock(ConnectorRepository.class);
    when(connectorRepository.existsById(any())).thenReturn(false);
    return connectorRepository;
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

  // ---------------------------------------------------------------------------
  // resolvePlannerBatch — cache correctness tests
  // ---------------------------------------------------------------------------

  @Test
  void resolvePlannerBatch_firstCallHitsDynamoDB_secondCallHitsCache() {
    StatsStore store = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepo = Mockito.mock(TableRepository.class);
    StatsSyncCapture syncCapture = Mockito.mock(StatsSyncCapture.class);
    StatsOrchestrator o = orchestrator(store, jobStore, tableRepo, syncCapture);

    StatsCaptureRequest req = columnRequest(42L, 7L);
    TargetStatsRecord rec = columnRecord(req);
    String storageId = StatsTargetIdentity.storageId(req.target());

    // First call: DynamoDB is queried.
    when(store.getTargetStatsBatch(req.tableId(), req.snapshotId(), List.of(req.target())))
        .thenReturn(Map.of(storageId, Optional.of(rec)));
    Map<String, StatsResolutionResult> result1 =
        o.resolvePlannerBatch(List.of(req), false, Long.MAX_VALUE);
    assertThat(result1.get(storageId).stats()).isPresent().contains(rec);
    verify(store, Mockito.times(1)).getTargetStatsBatch(any(), anyLong(), any());

    // Second call (same snapshot): served from cache, store NOT called again.
    Map<String, StatsResolutionResult> result2 =
        o.resolvePlannerBatch(List.of(req), false, Long.MAX_VALUE);
    assertThat(result2.get(storageId).stats()).isPresent().contains(rec);
    verify(store, Mockito.times(1)).getTargetStatsBatch(any(), anyLong(), any()); // still 1
  }

  @Test
  void resolvePlannerBatch_differentSnapshotId_isCacheMiss_neverServesStaleStats() {
    StatsStore store = Mockito.mock(StatsStore.class);
    StatsOrchestrator o =
        orchestrator(
            store,
            Mockito.mock(ReconcileJobStore.class),
            Mockito.mock(TableRepository.class),
            Mockito.mock(StatsSyncCapture.class));

    StatsCaptureRequest reqSnap42 = columnRequest(42L, 7L);
    StatsCaptureRequest reqSnap99 = columnRequest(99L, 7L); // same column, DIFFERENT snapshot

    TargetStatsRecord recSnap42 = columnRecord(reqSnap42);
    TargetStatsRecord recSnap99 = columnRecord(reqSnap99);

    String storageId = StatsTargetIdentity.storageId(reqSnap42.target());
    when(store.getTargetStatsBatch(reqSnap42.tableId(), 42L, List.of(reqSnap42.target())))
        .thenReturn(Map.of(storageId, Optional.of(recSnap42)));
    when(store.getTargetStatsBatch(reqSnap99.tableId(), 99L, List.of(reqSnap99.target())))
        .thenReturn(Map.of(storageId, Optional.of(recSnap99)));

    // Prime cache with snapshot 42.
    o.resolvePlannerBatch(List.of(reqSnap42), false, Long.MAX_VALUE);

    // Query snapshot 99 — must NOT return snapshot-42 data.
    Map<String, StatsResolutionResult> result =
        o.resolvePlannerBatch(List.of(reqSnap99), false, Long.MAX_VALUE);
    assertThat(result.get(storageId).stats()).isPresent().contains(recSnap99);
    // Store was called once for snap42 and once for snap99 (cache miss for different snapshot).
    verify(store, Mockito.times(2)).getTargetStatsBatch(any(), anyLong(), any());
  }

  @Test
  void resolvePlannerBatch_differentStatsGeneration_isCacheMiss() {
    StatsStore store = Mockito.mock(StatsStore.class);
    StatsOrchestrator o =
        orchestrator(
            store,
            Mockito.mock(ReconcileJobStore.class),
            Mockito.mock(TableRepository.class),
            Mockito.mock(StatsSyncCapture.class));

    StatsCaptureRequest req = columnRequest(42L, 7L);
    String storageId = StatsTargetIdentity.storageId(req.target());
    TargetStatsRecord gen1Record = columnRecord(req, 1L);
    TargetStatsRecord gen2Record = columnRecord(req, 2L);

    when(store.getTargetStatsBatchInGeneration(
            req.tableId(), req.snapshotId(), "gen-1", List.of(req.target())))
        .thenReturn(Map.of(storageId, Optional.of(gen1Record)));
    when(store.getTargetStatsBatchInGeneration(
            req.tableId(), req.snapshotId(), "gen-2", List.of(req.target())))
        .thenReturn(Map.of(storageId, Optional.of(gen2Record)));

    o.resolvePlannerBatchInGeneration(List.of(req), Optional.of("gen-1"), false, Long.MAX_VALUE);
    Map<String, StatsResolutionResult> result =
        o.resolvePlannerBatchInGeneration(
            List.of(req), Optional.of("gen-2"), false, Long.MAX_VALUE);

    assertThat(result.get(storageId).stats().get().getTable().getRowCount()).isEqualTo(2L);
    verify(store, Mockito.times(2)).getTargetStatsBatchInGeneration(any(), anyLong(), any(), any());
  }

  @Test
  void resolvePlannerBatch_differentTableId_isCacheMiss() {
    StatsStore store = Mockito.mock(StatsStore.class);
    StatsOrchestrator o =
        orchestrator(
            store,
            Mockito.mock(ReconcileJobStore.class),
            Mockito.mock(TableRepository.class),
            Mockito.mock(StatsSyncCapture.class));

    ResourceId tableA = ResourceId.newBuilder().setAccountId("acct").setId("table-A").build();
    ResourceId tableB = ResourceId.newBuilder().setAccountId("acct").setId("table-B").build();
    StatsTarget colTarget = StatsTargetIdentity.columnTarget(1L);
    String storageId = StatsTargetIdentity.storageId(colTarget);

    StatsCaptureRequest reqA =
        StatsCaptureRequest.builder(tableA, 42L, colTarget)
            .executionMode(StatsExecutionMode.ASYNC)
            .connectorType("iceberg")
            .build();
    StatsCaptureRequest reqB =
        StatsCaptureRequest.builder(tableB, 42L, colTarget)
            .executionMode(StatsExecutionMode.ASYNC)
            .connectorType("iceberg")
            .build();

    TargetStatsRecord recA =
        TargetStatsRecord.newBuilder()
            .setTableId(tableA)
            .setSnapshotId(42L)
            .setTarget(colTarget)
            .setTable(TableValueStats.newBuilder().setRowCount(1).build())
            .build();
    TargetStatsRecord recB =
        TargetStatsRecord.newBuilder()
            .setTableId(tableB)
            .setSnapshotId(42L)
            .setTarget(colTarget)
            .setTable(TableValueStats.newBuilder().setRowCount(2).build())
            .build();

    when(store.getTargetStatsBatch(tableA, 42L, List.of(colTarget)))
        .thenReturn(Map.of(storageId, Optional.of(recA)));
    when(store.getTargetStatsBatch(tableB, 42L, List.of(colTarget)))
        .thenReturn(Map.of(storageId, Optional.of(recB)));

    o.resolvePlannerBatch(List.of(reqA), false, Long.MAX_VALUE);
    Map<String, StatsResolutionResult> result =
        o.resolvePlannerBatch(List.of(reqB), false, Long.MAX_VALUE);

    // Table-B must return Table-B's row count, not Table-A's cached value.
    assertThat(result.get(storageId).stats().get().getTable().getRowCount()).isEqualTo(2L);
    verify(store, Mockito.times(2)).getTargetStatsBatch(any(), anyLong(), any());
  }

  @Test
  void resolvePlannerBatch_absentResultNotCached_storeQueriedNextTime() {
    StatsStore store = Mockito.mock(StatsStore.class);
    StatsOrchestrator o =
        orchestrator(
            store,
            Mockito.mock(ReconcileJobStore.class),
            Mockito.mock(TableRepository.class),
            Mockito.mock(StatsSyncCapture.class));

    StatsCaptureRequest req = columnRequest(42L, 7L);
    String storageId = StatsTargetIdentity.storageId(req.target());

    // First call: store returns absent (stats not yet captured).
    when(store.getTargetStatsBatch(req.tableId(), req.snapshotId(), List.of(req.target())))
        .thenReturn(Map.of(storageId, Optional.empty()));
    o.resolvePlannerBatch(List.of(req), false, Long.MAX_VALUE);

    // Second call: store is queried AGAIN (absent not cached — stats may arrive via sync capture).
    TargetStatsRecord rec = columnRecord(req);
    when(store.getTargetStatsBatch(req.tableId(), req.snapshotId(), List.of(req.target())))
        .thenReturn(Map.of(storageId, Optional.of(rec)));
    Map<String, StatsResolutionResult> result =
        o.resolvePlannerBatch(List.of(req), false, Long.MAX_VALUE);
    assertThat(result.get(storageId).stats()).isPresent().contains(rec);
    verify(store, Mockito.times(2)).getTargetStatsBatch(any(), anyLong(), any());
  }

  @Test
  void invalidateStatsCacheForTargetForcesStoreRefresh() {
    StatsStore store = Mockito.mock(StatsStore.class);
    StatsOrchestrator o =
        orchestrator(
            store,
            Mockito.mock(ReconcileJobStore.class),
            Mockito.mock(TableRepository.class),
            Mockito.mock(StatsSyncCapture.class));

    StatsCaptureRequest req = columnRequest(42L, 7L);
    String storageId = StatsTargetIdentity.storageId(req.target());
    TargetStatsRecord first = columnRecord(req, 1L);
    TargetStatsRecord replacement = columnRecord(req, 2L);
    when(store.getTargetStatsBatch(req.tableId(), req.snapshotId(), List.of(req.target())))
        .thenReturn(Map.of(storageId, Optional.of(first)))
        .thenReturn(Map.of(storageId, Optional.of(replacement)));

    assertThat(o.resolvePlannerBatch(List.of(req), false, Long.MAX_VALUE).get(storageId).stats())
        .contains(first);

    o.invalidateStatsCache(req.tableId(), req.snapshotId(), req.target());

    assertThat(o.resolvePlannerBatch(List.of(req), false, Long.MAX_VALUE).get(storageId).stats())
        .contains(replacement);
    verify(store, Mockito.times(2)).getTargetStatsBatch(any(), anyLong(), any());
  }

  @Test
  void invalidateStatsCacheForTargetClearsGenerationScopedEntries() {
    StatsStore store = Mockito.mock(StatsStore.class);
    StatsOrchestrator o =
        orchestrator(
            store,
            Mockito.mock(ReconcileJobStore.class),
            Mockito.mock(TableRepository.class),
            Mockito.mock(StatsSyncCapture.class));

    StatsCaptureRequest req = columnRequest(42L, 7L);
    String storageId = StatsTargetIdentity.storageId(req.target());
    TargetStatsRecord first = columnRecord(req, 1L);
    TargetStatsRecord replacement = columnRecord(req, 2L);
    when(store.getTargetStatsBatchInGeneration(
            req.tableId(), req.snapshotId(), "gen-1", List.of(req.target())))
        .thenReturn(Map.of(storageId, Optional.of(first)))
        .thenReturn(Map.of(storageId, Optional.of(replacement)));

    assertThat(
            o.resolvePlannerBatchInGeneration(
                    List.of(req), Optional.of("gen-1"), false, Long.MAX_VALUE)
                .get(storageId)
                .stats())
        .contains(first);

    o.invalidateStatsCache(req.tableId(), req.snapshotId(), req.target());

    assertThat(
            o.resolvePlannerBatchInGeneration(
                    List.of(req), Optional.of("gen-1"), false, Long.MAX_VALUE)
                .get(storageId)
                .stats())
        .contains(replacement);
    verify(store, Mockito.times(2)).getTargetStatsBatchInGeneration(any(), anyLong(), any(), any());
  }

  @Test
  void resolvePlannerBatch_pinnedGenerationWinsWhenPresent() {
    StatsStore store = Mockito.mock(StatsStore.class);
    StatsOrchestrator o =
        orchestrator(
            store,
            Mockito.mock(ReconcileJobStore.class),
            Mockito.mock(TableRepository.class),
            Mockito.mock(StatsSyncCapture.class));

    StatsCaptureRequest req = columnRequest(42L, 7L);
    String storageId = StatsTargetIdentity.storageId(req.target());
    // The pinned generation has the target (3); a newer generation exists with a different value.
    when(store.getTargetStatsBatchInGeneration(
            req.tableId(), req.snapshotId(), "gen-pinned", List.of(req.target())))
        .thenReturn(Map.of(storageId, Optional.of(columnRecord(req, 3L))));
    when(store.getTargetStatsBatch(req.tableId(), req.snapshotId(), List.of(req.target())))
        .thenReturn(Map.of(storageId, Optional.of(columnRecord(req, 9L))));

    Map<String, StatsResolutionResult> result =
        o.resolvePlannerBatchInGeneration(
            List.of(req), Optional.of("gen-pinned"), false, Long.MAX_VALUE);

    // The pinned generation is authoritative; newest is never consulted when the pin has the
    // target.
    assertThat(result.get(storageId).stats().get().getTable().getRowCount()).isEqualTo(3L);
    verify(store, Mockito.never()).getTargetStatsBatch(any(), anyLong(), any());
  }

  @Test
  void resolvePlannerBatch_newestFillsWhenPinnedLacksTarget() {
    StatsStore store = Mockito.mock(StatsStore.class);
    StatsOrchestrator o =
        orchestrator(
            store,
            Mockito.mock(ReconcileJobStore.class),
            Mockito.mock(TableRepository.class),
            Mockito.mock(StatsSyncCapture.class));

    StatsCaptureRequest req = columnRequest(42L, 7L);
    String storageId = StatsTargetIdentity.storageId(req.target());
    // Pinned generation lacks this target...
    when(store.getTargetStatsBatchInGeneration(
            req.tableId(), req.snapshotId(), "gen-pinned", List.of(req.target())))
        .thenReturn(Map.of(storageId, Optional.empty()));
    // ...so the newest generation fills it instead of yielding NOT_FOUND.
    when(store.getTargetStatsBatch(req.tableId(), req.snapshotId(), List.of(req.target())))
        .thenReturn(Map.of(storageId, Optional.of(columnRecord(req, 20L))));

    Map<String, StatsResolutionResult> result =
        o.resolvePlannerBatchInGeneration(
            List.of(req), Optional.of("gen-pinned"), false, Long.MAX_VALUE);

    assertThat(result.get(storageId).hasStats()).isTrue();
    assertThat(result.get(storageId).stats().get().getTable().getRowCount()).isEqualTo(20L);
  }

  @Test
  void resolvePlannerBatch_staleAfterPinnedAndNewestMiss() {
    StatsStore store = Mockito.mock(StatsStore.class);
    StatsOrchestrator o =
        orchestrator(
            store,
            Mockito.mock(ReconcileJobStore.class),
            Mockito.mock(TableRepository.class),
            Mockito.mock(StatsSyncCapture.class));

    StatsCaptureRequest req = columnRequest(42L, 7L);
    String storageId = StatsTargetIdentity.storageId(req.target());
    when(store.getTargetStatsBatchInGeneration(
            req.tableId(), req.snapshotId(), "gen-pinned", List.of(req.target())))
        .thenReturn(Map.of(storageId, Optional.empty()));
    when(store.getTargetStatsBatch(req.tableId(), req.snapshotId(), List.of(req.target())))
        .thenReturn(Map.of(storageId, Optional.empty()));
    when(store.getStaleTargetStatsBatch(req.tableId(), req.snapshotId(), List.of(req.target())))
        .thenReturn(Map.of(storageId, Optional.of(columnRecord(req, 5L))));

    Map<String, StatsResolutionResult> result =
        o.resolvePlannerBatchInGeneration(
            List.of(req), Optional.of("gen-pinned"), true, Long.MAX_VALUE);

    assertThat(result.get(storageId).hasStats()).isTrue();
    assertThat(result.get(storageId).stats().get().getTable().getRowCount()).isEqualTo(5L);
  }

  @Test
  void resolvePlannerBatch_pinnedGenerationServedFromCacheOnReplay() {
    StatsStore store = Mockito.mock(StatsStore.class);
    StatsOrchestrator o =
        orchestrator(
            store,
            Mockito.mock(ReconcileJobStore.class),
            Mockito.mock(TableRepository.class),
            Mockito.mock(StatsSyncCapture.class));

    StatsCaptureRequest req = columnRequest(42L, 7L);
    String storageId = StatsTargetIdentity.storageId(req.target());
    when(store.getTargetStatsBatchInGeneration(
            req.tableId(), req.snapshotId(), "gen-pinned", List.of(req.target())))
        .thenReturn(Map.of(storageId, Optional.of(columnRecord(req, 1L))));

    // Two resolutions on the same pin: the second is served from the pinned-generation cache, so
    // the
    // plan is reproducible and the store is read only once.
    for (int i = 0; i < 2; i++) {
      assertThat(
              o.resolvePlannerBatchInGeneration(
                      List.of(req), Optional.of("gen-pinned"), false, Long.MAX_VALUE)
                  .get(storageId)
                  .stats()
                  .get()
                  .getTable()
                  .getRowCount())
          .isEqualTo(1L);
    }
    verify(store, Mockito.times(1)).getTargetStatsBatchInGeneration(any(), anyLong(), any(), any());
  }

  @Test
  void resolveInGeneration_pinnedGenerationWinsWhenPresent() {
    StatsStore store = Mockito.mock(StatsStore.class);
    StatsOrchestrator o =
        orchestrator(
            store,
            Mockito.mock(ReconcileJobStore.class),
            Mockito.mock(TableRepository.class),
            Mockito.mock(StatsSyncCapture.class));

    StatsCaptureRequest req = columnRequest(42L, 7L);
    when(store.getTargetStatsInGeneration(
            req.tableId(), req.snapshotId(), "gen-pinned", req.target()))
        .thenReturn(Optional.of(columnRecord(req, 9L)));

    StatsResolutionResult r = o.resolveInGeneration(req, Optional.of("gen-pinned"));

    // The pinned generation is authoritative; the newest generation is never consulted.
    assertThat(r.hasStats()).isTrue();
    assertThat(r.stats().get().getTable().getRowCount()).isEqualTo(9L);
    verify(store, Mockito.never()).getTargetStats(any(), anyLong(), any());
  }

  @Test
  void resolveInGeneration_fillsFromNewestWhenPinnedMissing() {
    StatsStore store = Mockito.mock(StatsStore.class);
    StatsOrchestrator o =
        orchestrator(
            store,
            Mockito.mock(ReconcileJobStore.class),
            Mockito.mock(TableRepository.class),
            Mockito.mock(StatsSyncCapture.class));

    StatsCaptureRequest req = columnRequest(42L, 7L);
    when(store.getTargetStatsInGeneration(
            req.tableId(), req.snapshotId(), "gen-pinned", req.target()))
        .thenReturn(Optional.empty());
    when(store.getTargetStats(req.tableId(), req.snapshotId(), req.target()))
        .thenReturn(Optional.of(columnRecord(req, 3L)));

    StatsResolutionResult r = o.resolveInGeneration(req, Optional.of("gen-pinned"));

    // Pinned generation lacks the target, so the newest generation backstops before capture.
    assertThat(r.hasStats()).isTrue();
    assertThat(r.stats().get().getTable().getRowCount()).isEqualTo(3L);
  }

  /**
   * Completeness predicate used by the planner-completeness tests: the orchestrator treats the
   * predicate as opaque, so a simple row-count threshold stands in for "carries the requested
   * sketch payloads" (rowCount &lt; 10 = partial record, &ge; 10 = complete).
   */
  private static Map<String, java.util.function.Predicate<TargetStatsRecord>> completeAtRowCount(
      String storageId, long threshold) {
    return Map.of(storageId, record -> record.getTable().getRowCount() >= threshold);
  }

  @Test
  void resolvePlannerBatch_partialPinnedFallsToSatisfyingNewest() {
    StatsStore store = Mockito.mock(StatsStore.class);
    StatsOrchestrator o =
        orchestrator(
            store,
            Mockito.mock(ReconcileJobStore.class),
            Mockito.mock(TableRepository.class),
            Mockito.mock(StatsSyncCapture.class));

    StatsCaptureRequest req = columnRequest(42L, 7L);
    String storageId = StatsTargetIdentity.storageId(req.target());
    // Pinned generation HAS the target but only partially (fails the predicate)...
    when(store.getTargetStatsBatchInGeneration(
            req.tableId(), req.snapshotId(), "gen-pinned", List.of(req.target())))
        .thenReturn(Map.of(storageId, Optional.of(columnRecord(req, 1L))));
    // ...while the newest generation of the SAME snapshot satisfies it.
    when(store.getTargetStatsBatch(req.tableId(), req.snapshotId(), List.of(req.target())))
        .thenReturn(Map.of(storageId, Optional.of(columnRecord(req, 10L))));

    Map<String, StatsResolutionResult> result =
        o.resolvePlannerBatchInGeneration(
            List.of(req),
            Optional.of("gen-pinned"),
            completeAtRowCount(storageId, 10L),
            false,
            Long.MAX_VALUE);

    assertThat(result.get(storageId).stats().get().getTable().getRowCount()).isEqualTo(10L);
  }

  @Test
  void resolvePlannerBatch_completePinnedRecordSkipsNewest() {
    StatsStore store = Mockito.mock(StatsStore.class);
    StatsOrchestrator o =
        orchestrator(
            store,
            Mockito.mock(ReconcileJobStore.class),
            Mockito.mock(TableRepository.class),
            Mockito.mock(StatsSyncCapture.class));

    StatsCaptureRequest req = columnRequest(42L, 7L);
    String storageId = StatsTargetIdentity.storageId(req.target());
    when(store.getTargetStatsBatchInGeneration(
            req.tableId(), req.snapshotId(), "gen-pinned", List.of(req.target())))
        .thenReturn(Map.of(storageId, Optional.of(columnRecord(req, 10L))));

    Map<String, StatsResolutionResult> result =
        o.resolvePlannerBatchInGeneration(
            List.of(req),
            Optional.of("gen-pinned"),
            completeAtRowCount(storageId, 10L),
            false,
            Long.MAX_VALUE);

    // The pinned record satisfies the need: the hot path is one pinned read, newest untouched.
    assertThat(result.get(storageId).stats().get().getTable().getRowCount()).isEqualTo(10L);
    verify(store, Mockito.never()).getTargetStatsBatch(any(), anyLong(), any());
  }

  @Test
  void resolvePlannerBatch_keepsPinnedPartialWhenNewestNoBetter() {
    StatsStore store = Mockito.mock(StatsStore.class);
    StatsOrchestrator o =
        orchestrator(
            store,
            Mockito.mock(ReconcileJobStore.class),
            Mockito.mock(TableRepository.class),
            Mockito.mock(StatsSyncCapture.class));

    StatsCaptureRequest req = columnRequest(42L, 7L);
    String storageId = StatsTargetIdentity.storageId(req.target());
    // Both generations hold the target, neither satisfies the predicate.
    when(store.getTargetStatsBatchInGeneration(
            req.tableId(), req.snapshotId(), "gen-pinned", List.of(req.target())))
        .thenReturn(Map.of(storageId, Optional.of(columnRecord(req, 1L))));
    when(store.getTargetStatsBatch(req.tableId(), req.snapshotId(), List.of(req.target())))
        .thenReturn(Map.of(storageId, Optional.of(columnRecord(req, 2L))));

    Map<String, StatsResolutionResult> result =
        o.resolvePlannerBatchInGeneration(
            List.of(req),
            Optional.of("gen-pinned"),
            completeAtRowCount(storageId, 10L),
            /* staleOk= */ true,
            Long.MAX_VALUE);

    // Between equally incomplete records, consistency prefers the pinned generation — and a
    // partial record is still a hit: it never falls through to the stale ladder rung.
    assertThat(result.get(storageId).stats().get().getTable().getRowCount()).isEqualTo(1L);
    verify(store, Mockito.never()).getStaleTargetStatsBatch(any(), anyLong(), any());
  }

  @Test
  void resolvePlannerBatch_cachedPartialServesWhenPinnedRereadFailsAndNewestEmpty() {
    StatsStore store = Mockito.mock(StatsStore.class);
    StatsOrchestrator o =
        orchestrator(
            store,
            Mockito.mock(ReconcileJobStore.class),
            Mockito.mock(TableRepository.class),
            Mockito.mock(StatsSyncCapture.class));

    StatsCaptureRequest req = columnRequest(42L, 7L);
    String storageId = StatsTargetIdentity.storageId(req.target());

    // The pinned-generation batch read succeeds the first time (priming the cache with a
    // scalar-only record) then throws — the frozen manifest becomes unreadable between queries.
    RuntimeException manifestGone =
        new RuntimeException("frozen stats generation manifest missing for snapshot 42");
    java.util.concurrent.atomic.AtomicInteger pinnedReads =
        new java.util.concurrent.atomic.AtomicInteger();
    when(store.getTargetStatsBatchInGeneration(
            req.tableId(), req.snapshotId(), "gen-pinned", List.of(req.target())))
        .thenAnswer(
            inv -> {
              if (pinnedReads.getAndIncrement() == 0) {
                return Map.of(storageId, Optional.of(columnRecord(req, 1L)));
              }
              throw manifestGone;
            });
    when(store.getTargetStatsInGeneration(
            req.tableId(), req.snapshotId(), "gen-pinned", req.target()))
        .thenThrow(manifestGone);
    // The newest generation of the same snapshot has nothing to gap-fill with.
    when(store.getTargetStatsBatch(req.tableId(), req.snapshotId(), List.of(req.target())))
        .thenReturn(Map.of(storageId, Optional.empty()));

    // 1. Prime: a need with no completeness predicate caches the scalar-only pinned record.
    o.resolvePlannerBatchInGeneration(
        List.of(req), Optional.of("gen-pinned"), false, Long.MAX_VALUE);

    // 2. A richer need the cached record cannot satisfy, staleOk=false so a fall-through would
    // reach sync capture. The pinned re-read fails and the newest generation is empty — the only
    // thing standing between the planner and a needless capture is the incomplete cached record,
    // which is still a valid (degraded) hit and must be served.
    Map<String, StatsResolutionResult> result =
        o.resolvePlannerBatchInGeneration(
            List.of(req),
            Optional.of("gen-pinned"),
            completeAtRowCount(storageId, 10L),
            false,
            Long.MAX_VALUE);

    assertThat(result.get(storageId).hasStats()).isTrue();
    assertThat(result.get(storageId).stats().get().getTable().getRowCount()).isEqualTo(1L);
    // The partial hit means no stale read and no capture were triggered.
    verify(store, Mockito.never()).getStaleTargetStatsBatch(any(), anyLong(), any());
  }

  @Test
  void resolvePlannerBatch_cachedPartialDoesNotShortCircuitRicherNeed() {
    StatsStore store = Mockito.mock(StatsStore.class);
    StatsOrchestrator o =
        orchestrator(
            store,
            Mockito.mock(ReconcileJobStore.class),
            Mockito.mock(TableRepository.class),
            Mockito.mock(StatsSyncCapture.class));

    StatsCaptureRequest req = columnRequest(42L, 7L);
    String storageId = StatsTargetIdentity.storageId(req.target());
    when(store.getTargetStatsBatchInGeneration(
            req.tableId(), req.snapshotId(), "gen-pinned", List.of(req.target())))
        .thenReturn(Map.of(storageId, Optional.of(columnRecord(req, 1L))));
    when(store.getTargetStatsBatch(req.tableId(), req.snapshotId(), List.of(req.target())))
        .thenReturn(Map.of(storageId, Optional.of(columnRecord(req, 10L))));

    // A need without a completeness predicate caches the pinned record: presence = complete.
    assertThat(
            o.resolvePlannerBatchInGeneration(
                    List.of(req), Optional.of("gen-pinned"), false, Long.MAX_VALUE)
                .get(storageId)
                .stats()
                .get()
                .getTable()
                .getRowCount())
        .isEqualTo(1L);

    // A richer need must not be short-circuited by that cached record: the cache read fails the
    // predicate, the pinned re-read is still partial, and the newest generation serves it.
    assertThat(
            o.resolvePlannerBatchInGeneration(
                    List.of(req),
                    Optional.of("gen-pinned"),
                    completeAtRowCount(storageId, 10L),
                    false,
                    Long.MAX_VALUE)
                .get(storageId)
                .stats()
                .get()
                .getTable()
                .getRowCount())
        .isEqualTo(10L);
  }

  @Test
  void resolvePlannerBatch_unreadablePinnedGenerationFallsThroughToNewest() {
    StatsStore store = Mockito.mock(StatsStore.class);
    StatsOrchestrator o =
        orchestrator(
            store,
            Mockito.mock(ReconcileJobStore.class),
            Mockito.mock(TableRepository.class),
            Mockito.mock(StatsSyncCapture.class));

    StatsCaptureRequest req = columnRequest(42L, 7L);
    String storageId = StatsTargetIdentity.storageId(req.target());
    // The frozen manifest is unreadable: every pinned-generation read throws (batch and the
    // per-target isolation retry alike), so the primary read resolves to FAILED.
    RuntimeException manifestGone =
        new RuntimeException("frozen stats generation manifest missing for snapshot 42");
    when(store.getTargetStatsBatchInGeneration(
            req.tableId(), req.snapshotId(), "gen-pinned", List.of(req.target())))
        .thenThrow(manifestGone);
    when(store.getTargetStatsInGeneration(
            req.tableId(), req.snapshotId(), "gen-pinned", req.target()))
        .thenThrow(manifestGone);
    when(store.getTargetStatsBatch(req.tableId(), req.snapshotId(), List.of(req.target())))
        .thenReturn(Map.of(storageId, Optional.of(columnRecord(req, 10L))));

    Map<String, StatsResolutionResult> result =
        o.resolvePlannerBatchInGeneration(
            List.of(req), Optional.of("gen-pinned"), false, Long.MAX_VALUE);

    // A pinned read failure must not zero the batch: the newest generation is an independent
    // path and still serves the target.
    assertThat(result.get(storageId).hasStats()).isTrue();
    assertThat(result.get(storageId).stats().get().getTable().getRowCount()).isEqualTo(10L);
  }

  @Test
  void resolveInGeneration_unreadablePinnedGenerationFallsThroughToNewest() {
    StatsStore store = Mockito.mock(StatsStore.class);
    StatsOrchestrator o =
        orchestrator(
            store,
            Mockito.mock(ReconcileJobStore.class),
            Mockito.mock(TableRepository.class),
            Mockito.mock(StatsSyncCapture.class));

    StatsCaptureRequest req = columnRequest(42L, 7L);
    when(store.getTargetStatsInGeneration(
            req.tableId(), req.snapshotId(), "gen-pinned", req.target()))
        .thenThrow(
            new RuntimeException("frozen stats generation manifest missing for snapshot 42"));
    when(store.getTargetStats(req.tableId(), req.snapshotId(), req.target()))
        .thenReturn(Optional.of(columnRecord(req, 10L)));

    StatsResolutionResult r = o.resolveInGeneration(req, Optional.of("gen-pinned"));

    // Same contract as the batch path: a pinned read failure is a miss, not a lookup failure —
    // the newest generation still serves the target.
    assertThat(r.hasStats()).isTrue();
    assertThat(r.stats().get().getTable().getRowCount()).isEqualTo(10L);
  }

  @Test
  void resolvePlannerBatch_countsTheLadderRungThatServedEachTarget() {
    StatsStore store = Mockito.mock(StatsStore.class);
    ai.floedb.floecat.telemetry.Observability obs =
        Mockito.mock(ai.floedb.floecat.telemetry.Observability.class);
    @SuppressWarnings("unchecked")
    jakarta.enterprise.inject.Instance<ai.floedb.floecat.telemetry.Observability> obsInstance =
        Mockito.mock(jakarta.enterprise.inject.Instance.class);
    when(obsInstance.isUnsatisfied()).thenReturn(false);
    when(obsInstance.get()).thenReturn(obs);
    StatsOrchestrator o =
        new StatsOrchestrator(
            store,
            Mockito.mock(ReconcileJobStore.class),
            Mockito.mock(TableRepository.class),
            connectorRepositoryWith(),
            Mockito.mock(StatsSyncCapture.class),
            true,
            obsInstance);

    StatsCaptureRequest req = columnRequest(42L, 7L);
    String storageId = StatsTargetIdentity.storageId(req.target());
    // Pinned generation is partial; the newest generation satisfies → NEWEST_FILL rung.
    when(store.getTargetStatsBatchInGeneration(
            req.tableId(), req.snapshotId(), "gen-pinned", List.of(req.target())))
        .thenReturn(Map.of(storageId, Optional.of(columnRecord(req, 1L))));
    when(store.getTargetStatsBatch(req.tableId(), req.snapshotId(), List.of(req.target())))
        .thenReturn(Map.of(storageId, Optional.of(columnRecord(req, 10L))));

    o.resolvePlannerBatchInGeneration(
        List.of(req),
        Optional.of("gen-pinned"),
        completeAtRowCount(storageId, 10L),
        false,
        Long.MAX_VALUE);

    // Exactly the rung that served the target is counted, tagged result=newest_fill.
    ArgumentCaptor<ai.floedb.floecat.telemetry.Tag[]> tags =
        ArgumentCaptor.forClass(ai.floedb.floecat.telemetry.Tag[].class);
    verify(obs)
        .counter(
            Mockito.eq(
                ai.floedb.floecat.service.telemetry.ServiceMetrics.Stats
                    .PLANNER_LOOKUP_OUTCOMES_TOTAL),
            Mockito.eq(1.0),
            tags.capture());
    assertThat(java.util.Arrays.stream(tags.getValue()))
        .anyMatch(tag -> tag.key().equals("result") && tag.value().equals("newest_fill"));
  }

  @Test
  void invalidateStatsCacheForPersistedRecordsUsesExplicitTableSnapshot() {
    StatsStore store = Mockito.mock(StatsStore.class);
    StatsOrchestrator o =
        orchestrator(
            store,
            Mockito.mock(ReconcileJobStore.class),
            Mockito.mock(TableRepository.class),
            Mockito.mock(StatsSyncCapture.class));

    StatsCaptureRequest req = columnRequest(42L, 7L);
    String storageId = StatsTargetIdentity.storageId(req.target());
    TargetStatsRecord first = columnRecord(req, 1L);
    TargetStatsRecord replacement = columnRecord(req, 2L);
    TargetStatsRecord targetOnlyRecord =
        TargetStatsRecord.newBuilder().setTarget(req.target()).build();
    when(store.getTargetStatsBatch(req.tableId(), req.snapshotId(), List.of(req.target())))
        .thenReturn(Map.of(storageId, Optional.of(first)))
        .thenReturn(Map.of(storageId, Optional.of(replacement)));

    assertThat(o.resolvePlannerBatch(List.of(req), false, Long.MAX_VALUE).get(storageId).stats())
        .contains(first);

    o.invalidateStatsCache(req.tableId(), req.snapshotId(), List.of(targetOnlyRecord));

    assertThat(o.resolvePlannerBatch(List.of(req), false, Long.MAX_VALUE).get(storageId).stats())
        .contains(replacement);
    verify(store, Mockito.times(2)).getTargetStatsBatch(any(), anyLong(), any());
  }

  @Test
  void invalidateStatsCacheForSnapshotForcesStoreRefresh() {
    StatsStore store = Mockito.mock(StatsStore.class);
    StatsOrchestrator o =
        orchestrator(
            store,
            Mockito.mock(ReconcileJobStore.class),
            Mockito.mock(TableRepository.class),
            Mockito.mock(StatsSyncCapture.class));

    StatsCaptureRequest req = columnRequest(42L, 7L);
    String storageId = StatsTargetIdentity.storageId(req.target());
    TargetStatsRecord first = columnRecord(req, 1L);
    TargetStatsRecord replacement = columnRecord(req, 2L);
    when(store.getTargetStatsBatch(req.tableId(), req.snapshotId(), List.of(req.target())))
        .thenReturn(Map.of(storageId, Optional.of(first)))
        .thenReturn(Map.of(storageId, Optional.of(replacement)));

    assertThat(o.resolvePlannerBatch(List.of(req), false, Long.MAX_VALUE).get(storageId).stats())
        .contains(first);

    o.invalidateStatsCache(req.tableId(), req.snapshotId());

    assertThat(o.resolvePlannerBatch(List.of(req), false, Long.MAX_VALUE).get(storageId).stats())
        .contains(replacement);
    verify(store, Mockito.times(2)).getTargetStatsBatch(any(), anyLong(), any());
  }

  private static StatsCaptureRequest columnRequest(long snapshotId, long columnId) {
    return StatsCaptureRequest.builder(
            ResourceId.newBuilder().setAccountId("acct").setId("table-1").build(),
            snapshotId,
            StatsTargetIdentity.columnTarget(columnId))
        .executionMode(StatsExecutionMode.ASYNC)
        .connectorType("iceberg")
        .build();
  }

  private static TargetStatsRecord columnRecord(StatsCaptureRequest req) {
    return columnRecord(req, 42L);
  }

  private static TargetStatsRecord columnRecord(StatsCaptureRequest req, long rowCount) {
    return TargetStatsRecord.newBuilder()
        .setTableId(req.tableId())
        .setSnapshotId(req.snapshotId())
        .setTarget(req.target())
        .setTable(TableValueStats.newBuilder().setRowCount(rowCount).build())
        .build();
  }
}
