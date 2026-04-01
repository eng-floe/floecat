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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.EngineExpressionStatsTarget;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.TableStatsTarget;
import ai.floedb.floecat.catalog.rpc.TableValueStats;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.statistics.engine.StatsEngineRegistry;
import ai.floedb.floecat.stats.spi.StatsCaptureEngine;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsCaptureResult;
import ai.floedb.floecat.stats.spi.StatsExecutionMode;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.stats.spi.StatsTargetType;
import ai.floedb.floecat.stats.spi.StatsUnsupportedTargetException;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class StatsOrchestratorTest {

  @Test
  void returnsStoreHitDirectly() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsEngineRegistry registry = Mockito.mock(StatsEngineRegistry.class);
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, registry);

    StatsCaptureRequest request = request(StatsExecutionMode.SYNC);
    TargetStatsRecord record = tableRecord(request);
    when(statsStore.getTargetStats(request.tableId(), request.snapshotId(), request.target()))
        .thenReturn(Optional.of(record));

    Optional<TargetStatsRecord> resolved = orchestrator.resolve(request);

    assertThat(resolved).contains(record);
    verify(registry, never()).capture(any());
    verify(jobStore, never()).enqueue(any(), any(), any(Boolean.class), any(), any());
  }

  @Test
  void syncMissUsesEngineCaptureBeforeEnqueue() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsEngineRegistry registry = Mockito.mock(StatsEngineRegistry.class);
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, registry);

    StatsCaptureRequest request = request(StatsExecutionMode.SYNC);
    TargetStatsRecord record = tableRecord(request);
    when(statsStore.getTargetStats(request.tableId(), request.snapshotId(), request.target()))
        .thenReturn(Optional.empty());
    when(registry.capture(request))
        .thenReturn(
            Optional.of(StatsCaptureResult.forRecord("native", record, java.util.Map.of())));

    Optional<TargetStatsRecord> resolved = orchestrator.resolve(request);

    assertThat(resolved).contains(record);
    verify(jobStore, never()).enqueue(any(), any(), any(Boolean.class), any(), any());
  }

  @Test
  void syncMissEvaluatesCaptureSupportBeforeEnqueuePolicy() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsEngineRegistry registry = Mockito.mock(StatsEngineRegistry.class);
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, registry);

    StatsCaptureRequest request = request(StatsExecutionMode.SYNC);
    when(statsStore.getTargetStats(request.tableId(), request.snapshotId(), request.target()))
        .thenReturn(Optional.empty());
    when(registry.capture(request)).thenReturn(Optional.empty());
    when(registry.candidates(any())).thenReturn(List.of());

    Optional<TargetStatsRecord> resolved = orchestrator.resolve(request);

    assertThat(resolved).isEmpty();
    ArgumentCaptor<StatsCaptureRequest> candidateCaptor =
        ArgumentCaptor.forClass(StatsCaptureRequest.class);
    verify(registry, Mockito.atLeastOnce()).candidates(candidateCaptor.capture());
    assertThat(candidateCaptor.getAllValues())
        .extracting(StatsCaptureRequest::executionMode)
        .contains(StatsExecutionMode.SYNC);
    verify(jobStore, never()).enqueue(any(), any(), any(Boolean.class), any(), any());
  }

  @Test
  void asyncMissEnqueuesScopedStatsJob() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsEngineRegistry registry = Mockito.mock(StatsEngineRegistry.class);
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, registry);

    StatsCaptureRequest request = request(StatsExecutionMode.ASYNC);
    when(statsStore.getTargetStats(request.tableId(), request.snapshotId(), request.target()))
        .thenReturn(Optional.empty());
    when(registry.candidates(any())).thenReturn(List.of(Mockito.mock(StatsCaptureEngine.class)));
    when(tableRepository.getById(request.tableId()))
        .thenReturn(Optional.of(tableWithUpstream(request.tableId())));

    Optional<TargetStatsRecord> resolved = orchestrator.resolve(request);

    assertThat(resolved).isEmpty();
    verify(registry, never()).capture(any());
    ArgumentCaptor<ai.floedb.floecat.reconciler.jobs.ReconcileScope> scopeCaptor =
        ArgumentCaptor.forClass(ai.floedb.floecat.reconciler.jobs.ReconcileScope.class);
    verify(jobStore)
        .enqueue(
            Mockito.eq(request.tableId().getAccountId()),
            Mockito.eq("conn-1"),
            Mockito.eq(false),
            Mockito.eq(ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode.STATS_ONLY),
            scopeCaptor.capture());
    assertThat(scopeCaptor.getValue().destinationSnapshotIds())
        .containsExactly(request.snapshotId());
  }

  @Test
  void syncUnsupportedTargetDoesNotEnqueue() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsEngineRegistry registry = Mockito.mock(StatsEngineRegistry.class);
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, registry);

    StatsCaptureRequest request =
        request(
            StatsExecutionMode.SYNC,
            StatsTarget.newBuilder()
                .setExpression(
                    EngineExpressionStatsTarget.newBuilder()
                        .setEngineKind("trino")
                        .setEngineExpressionKey(ByteString.copyFromUtf8("expr")))
                .build(),
            42L);
    when(statsStore.getTargetStats(request.tableId(), request.snapshotId(), request.target()))
        .thenReturn(Optional.empty());
    when(registry.capture(request))
        .thenThrow(new StatsUnsupportedTargetException(StatsTargetType.EXPRESSION, request));

    Optional<TargetStatsRecord> resolved = orchestrator.resolve(request);

    assertThat(resolved).isEmpty();
    verify(jobStore, never()).enqueue(any(), any(), any(Boolean.class), any(), any());
  }

  @Test
  void asyncMissUnsupportedTargetDoesNotEnqueue() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsEngineRegistry registry = Mockito.mock(StatsEngineRegistry.class);
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, registry);

    StatsCaptureRequest request =
        request(
            StatsExecutionMode.ASYNC,
            StatsTarget.newBuilder()
                .setExpression(
                    EngineExpressionStatsTarget.newBuilder()
                        .setEngineKind("trino")
                        .setEngineExpressionKey(ByteString.copyFromUtf8("expr")))
                .build(),
            42L);
    when(statsStore.getTargetStats(request.tableId(), request.snapshotId(), request.target()))
        .thenReturn(Optional.empty());
    when(registry.candidates(any())).thenReturn(List.of());

    Optional<TargetStatsRecord> resolved = orchestrator.resolve(request);

    assertThat(resolved).isEmpty();
    verify(jobStore, never()).enqueue(any(), any(), any(Boolean.class), any(), any());
    verify(registry, never()).capture(any());
  }

  @Test
  void asyncMissWithNonPositiveSnapshotDoesNotEnqueue() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsEngineRegistry registry = Mockito.mock(StatsEngineRegistry.class);
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, registry);

    StatsCaptureRequest request =
        request(
            StatsExecutionMode.ASYNC,
            StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()).build(),
            0L);
    when(statsStore.getTargetStats(request.tableId(), request.snapshotId(), request.target()))
        .thenReturn(Optional.empty());
    when(registry.candidates(any())).thenReturn(List.of(Mockito.mock(StatsCaptureEngine.class)));

    Optional<TargetStatsRecord> resolved = orchestrator.resolve(request);

    assertThat(resolved).isEmpty();
    verify(jobStore, never()).enqueue(any(), any(), any(Boolean.class), any(), any());
  }

  @Test
  void asyncMissWithNoAsyncCandidatesDoesNotEnqueue() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsEngineRegistry registry = Mockito.mock(StatsEngineRegistry.class);
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, registry);

    StatsCaptureRequest request = request(StatsExecutionMode.ASYNC);
    when(statsStore.getTargetStats(request.tableId(), request.snapshotId(), request.target()))
        .thenReturn(Optional.empty());
    when(registry.candidates(any())).thenReturn(List.of());
    when(tableRepository.getById(request.tableId()))
        .thenReturn(Optional.of(tableWithUpstream(request.tableId(), TableFormat.TF_UNSPECIFIED)));

    Optional<TargetStatsRecord> resolved = orchestrator.resolve(request);

    assertThat(resolved).isEmpty();
    verify(jobStore, never()).enqueue(any(), any(), any(Boolean.class), any(), any());
  }

  @Test
  void asyncMissWithNoUpstreamDoesNotEnqueue() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsEngineRegistry registry = Mockito.mock(StatsEngineRegistry.class);
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, registry);

    StatsCaptureRequest request = request(StatsExecutionMode.ASYNC);
    when(statsStore.getTargetStats(request.tableId(), request.snapshotId(), request.target()))
        .thenReturn(Optional.empty());
    when(registry.candidates(any())).thenReturn(List.of(Mockito.mock(StatsCaptureEngine.class)));
    when(tableRepository.getById(request.tableId()))
        .thenReturn(
            Optional.of(
                Table.newBuilder()
                    .setResourceId(request.tableId())
                    .setDisplayName("events")
                    .build()));

    Optional<TargetStatsRecord> resolved = orchestrator.resolve(request);

    assertThat(resolved).isEmpty();
    verify(jobStore, never()).enqueue(any(), any(), any(Boolean.class), any(), any());
  }

  @Test
  void asyncMissWithBlankConnectorIdDoesNotEnqueue() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsEngineRegistry registry = Mockito.mock(StatsEngineRegistry.class);
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, registry);

    StatsCaptureRequest request = request(StatsExecutionMode.ASYNC);
    when(statsStore.getTargetStats(request.tableId(), request.snapshotId(), request.target()))
        .thenReturn(Optional.empty());
    when(registry.candidates(any())).thenReturn(List.of(Mockito.mock(StatsCaptureEngine.class)));
    when(tableRepository.getById(request.tableId()))
        .thenReturn(
            Optional.of(
                Table.newBuilder()
                    .setResourceId(request.tableId())
                    .setDisplayName("events")
                    .setUpstream(
                        ai.floedb.floecat.catalog.rpc.UpstreamRef.newBuilder()
                            .setConnectorId(ResourceId.newBuilder().setAccountId("acct").setId(""))
                            .setFormat(TableFormat.TF_ICEBERG)
                            .addNamespacePath("db")
                            .setTableDisplayName("events")
                            .build())
                    .build()));

    Optional<TargetStatsRecord> resolved = orchestrator.resolve(request);

    assertThat(resolved).isEmpty();
    verify(jobStore, never()).enqueue(any(), any(), any(Boolean.class), any(), any());
  }

  @Test
  void asyncMissWithExpressionTargetEnqueuesWhenEngineSupportsIt() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsEngineRegistry registry = Mockito.mock(StatsEngineRegistry.class);
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(statsStore, jobStore, tableRepository, registry);

    StatsCaptureRequest request =
        request(
            StatsExecutionMode.ASYNC,
            StatsTarget.newBuilder()
                .setExpression(
                    EngineExpressionStatsTarget.newBuilder()
                        .setEngineKind("trino")
                        .setEngineExpressionKey(ByteString.copyFromUtf8("expr")))
                .build(),
            42L);
    when(statsStore.getTargetStats(request.tableId(), request.snapshotId(), request.target()))
        .thenReturn(Optional.empty());
    when(registry.candidates(any())).thenReturn(List.of(Mockito.mock(StatsCaptureEngine.class)));
    when(tableRepository.getById(request.tableId()))
        .thenReturn(Optional.of(tableWithUpstream(request.tableId())));

    Optional<TargetStatsRecord> resolved = orchestrator.resolve(request);

    assertThat(resolved).isEmpty();
    verify(jobStore)
        .enqueue(
            Mockito.eq(request.tableId().getAccountId()),
            Mockito.eq("conn-1"),
            Mockito.eq(false),
            Mockito.eq(ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode.STATS_ONLY),
            any());
  }

  private static StatsCaptureRequest request(StatsExecutionMode mode) {
    return request(
        mode,
        StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()).build(),
        42L);
  }

  private static StatsCaptureRequest request(
      StatsExecutionMode mode, StatsTarget target, long snapshotId) {
    return new StatsCaptureRequest(
        ResourceId.newBuilder().setAccountId("acct").setId("table-1").build(),
        snapshotId,
        target,
        Set.of(),
        Set.of(),
        mode,
        "iceberg",
        "corr",
        false);
  }

  private static TargetStatsRecord tableRecord(StatsCaptureRequest request) {
    return TargetStatsRecord.newBuilder()
        .setTableId(request.tableId())
        .setSnapshotId(request.snapshotId())
        .setTarget(request.target())
        .setTable(TableValueStats.newBuilder().setRowCount(10L).build())
        .build();
  }

  private static Table tableWithUpstream(ResourceId tableId) {
    return tableWithUpstream(tableId, TableFormat.TF_ICEBERG);
  }

  private static Table tableWithUpstream(ResourceId tableId, TableFormat format) {
    return Table.newBuilder()
        .setResourceId(tableId)
        .setDisplayName("events")
        .setUpstream(
            ai.floedb.floecat.catalog.rpc.UpstreamRef.newBuilder()
                .setConnectorId(
                    ResourceId.newBuilder().setAccountId("acct").setId("conn-1").build())
                .setFormat(format)
                .addNamespacePath("db")
                .setTableDisplayName("events")
                .build())
        .build();
  }
}
