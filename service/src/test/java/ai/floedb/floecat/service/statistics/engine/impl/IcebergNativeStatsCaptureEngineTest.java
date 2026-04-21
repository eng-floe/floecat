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

package ai.floedb.floecat.service.statistics.engine.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.ColumnStatsTarget;
import ai.floedb.floecat.catalog.rpc.FileStatsTarget;
import ai.floedb.floecat.catalog.rpc.FileTargetStats;
import ai.floedb.floecat.catalog.rpc.ScalarStats;
import ai.floedb.floecat.catalog.rpc.StatsCaptureMode;
import ai.floedb.floecat.catalog.rpc.StatsCompleteness;
import ai.floedb.floecat.catalog.rpc.StatsProducer;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableStatsTarget;
import ai.floedb.floecat.catalog.rpc.TableValueStats;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.connector.spi.CredentialResolver;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchRequest;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchResult;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsExecutionMode;
import ai.floedb.floecat.stats.spi.StatsKind;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.stats.spi.StatsTriggerOutcome;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class IcebergNativeStatsCaptureEngineTest {

  @Test
  void capturesTableStatsAndPersistsBundleRecords() {
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    ConnectorRepository connectorRepository = Mockito.mock(ConnectorRepository.class);
    CredentialResolver credentialResolver = Mockito.mock(CredentialResolver.class);
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    FloecatConnector floecatConnector = Mockito.mock(FloecatConnector.class);

    IcebergNativeStatsCaptureEngine engine =
        new IcebergNativeStatsCaptureEngine(
            tableRepository, connectorRepository, credentialResolver, statsStore);
    engine.connectorOpener = config -> floecatConnector;
    List<Runnable> queuedWrites = new CopyOnWriteArrayList<>();
    engine.persistExecutor = queuedWrites::add;

    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct").setId("table-1").build();
    ResourceId connectorId = ResourceId.newBuilder().setAccountId("acct").setId("conn-1").build();
    when(tableRepository.getById(tableId))
        .thenReturn(
            Optional.of(
                Table.newBuilder()
                    .setResourceId(tableId)
                    .setDisplayName("events")
                    .setUpstream(
                        ai.floedb.floecat.catalog.rpc.UpstreamRef.newBuilder()
                            .setConnectorId(connectorId)
                            .addNamespacePath("db")
                            .setTableDisplayName("events")
                            .build())
                    .build()));
    when(connectorRepository.getById(connectorId))
        .thenReturn(
            Optional.of(
                Connector.newBuilder()
                    .setResourceId(connectorId)
                    .setDisplayName("iceberg-main")
                    .setKind(ConnectorKind.CK_ICEBERG)
                    .setUri("s3://warehouse")
                    .build()));

    StatsTarget tableTarget =
        StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()).build();
    StatsTarget columnTarget =
        StatsTarget.newBuilder().setColumn(ColumnStatsTarget.newBuilder().setColumnId(7L)).build();
    TargetStatsRecord tableRecord =
        TargetStatsRecord.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(101L)
            .setTarget(tableTarget)
            .setTable(TableValueStats.newBuilder().setRowCount(11L).build())
            .build();
    TargetStatsRecord columnRecord =
        TargetStatsRecord.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(101L)
            .setTarget(columnTarget)
            .setScalar(ScalarStats.newBuilder().setDisplayName("c7").setLogicalType("BIGINT"))
            .build();

    when(floecatConnector.captureSnapshotTargetStats(any(), any(), any(), anyLong(), any(), any()))
        .thenReturn(List.of(tableRecord, columnRecord));

    StatsCaptureRequest request =
        StatsCaptureRequest.builder(tableId, 101L, tableTarget)
            .columnSelectors(Set.of("c7"))
            .requestedKinds(Set.of(StatsKind.ROW_COUNT))
            .executionMode(StatsExecutionMode.SYNC)
            .connectorType("iceberg")
            .correlationId("corr")
            .build();

    Optional<ai.floedb.floecat.stats.spi.StatsCaptureResult> result = engine.capture(request);

    assertThat(result).isPresent();
    assertThat(result.get().record().getTable()).isEqualTo(tableRecord.getTable());
    assertThat(result.get().record().getMetadata().getProducer())
        .isEqualTo(StatsProducer.SPROD_SOURCE_NATIVE);
    assertThat(result.get().record().getMetadata().getCompleteness())
        .isEqualTo(StatsCompleteness.SC_COMPLETE);
    assertThat(result.get().record().getMetadata().getCaptureMode())
        .isEqualTo(StatsCaptureMode.SCM_SYNC);
    assertThat(result.get().record().getMetadata().hasConfidenceLevel()).isTrue();
    assertThat(result.get().record().getMetadata().hasCoverage()).isTrue();
    assertThat(result.get().record().getMetadata().hasCapturedAt()).isTrue();
    assertThat(result.get().record().getMetadata().hasRefreshedAt()).isTrue();
    assertThat(result.get().record().getMetadata().getPropertiesMap())
        .containsEntry("method", "connector_native")
        .containsEntry("engine_id", IcebergNativeStatsCaptureEngine.ENGINE_ID);
    verify(floecatConnector)
        .captureSnapshotTargetStats(
            any(), any(), any(), anyLong(), argThat(selectors -> selectors.contains("c7")), any());
    assertThat(queuedWrites).hasSize(1);
    verify(statsStore, never()).putTargetStats(any());
    queuedWrites.getFirst().run();
    verify(statsStore, times(1))
        .putTargetStats(argThat(r -> r.hasTable() && r.getSnapshotId() == 101L));
    verify(statsStore, times(1))
        .putTargetStats(
            argThat(
                r ->
                    r.hasScalar()
                        && r.hasTarget()
                        && r.getTarget().hasColumn()
                        && r.getTarget().getColumn().getColumnId() == 7L));
  }

  @Test
  void capturesRequestedFileStats() {
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    ConnectorRepository connectorRepository = Mockito.mock(ConnectorRepository.class);
    CredentialResolver credentialResolver = Mockito.mock(CredentialResolver.class);
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    FloecatConnector floecatConnector = Mockito.mock(FloecatConnector.class);

    IcebergNativeStatsCaptureEngine engine =
        new IcebergNativeStatsCaptureEngine(
            tableRepository, connectorRepository, credentialResolver, statsStore);
    engine.connectorOpener = config -> floecatConnector;
    engine.persistExecutor = Runnable::run;

    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct").setId("table-1").build();
    ResourceId connectorId = ResourceId.newBuilder().setAccountId("acct").setId("conn-1").build();
    when(tableRepository.getById(tableId))
        .thenReturn(
            Optional.of(
                Table.newBuilder()
                    .setResourceId(tableId)
                    .setDisplayName("events")
                    .setUpstream(
                        ai.floedb.floecat.catalog.rpc.UpstreamRef.newBuilder()
                            .setConnectorId(connectorId)
                            .addNamespacePath("db")
                            .setTableDisplayName("events")
                            .build())
                    .build()));
    when(connectorRepository.getById(connectorId))
        .thenReturn(
            Optional.of(
                Connector.newBuilder()
                    .setResourceId(connectorId)
                    .setDisplayName("iceberg-main")
                    .setKind(ConnectorKind.CK_ICEBERG)
                    .setUri("s3://warehouse")
                    .build()));

    StatsTarget fileTarget =
        StatsTarget.newBuilder()
            .setFile(FileStatsTarget.newBuilder().setFilePath("/data/file-1.parquet"))
            .build();
    FileTargetStats fileStats =
        FileTargetStats.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(101L)
            .setFilePath("/data/file-1.parquet")
            .setFileFormat("parquet")
            .setRowCount(10L)
            .setSizeBytes(100L)
            .build();
    TargetStatsRecord fileRecord =
        TargetStatsRecord.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(101L)
            .setTarget(fileTarget)
            .setFile(fileStats)
            .build();

    when(floecatConnector.captureSnapshotTargetStats(any(), any(), any(), anyLong(), any(), any()))
        .thenReturn(List.of(fileRecord));

    StatsCaptureRequest request =
        StatsCaptureRequest.builder(tableId, 101L, fileTarget)
            .requestedKinds(Set.of(StatsKind.ROW_COUNT, StatsKind.TOTAL_BYTES))
            .executionMode(StatsExecutionMode.SYNC)
            .connectorType("iceberg")
            .correlationId("corr")
            .build();

    Optional<ai.floedb.floecat.stats.spi.StatsCaptureResult> result = engine.capture(request);

    assertThat(result).isPresent();
    assertThat(result.get().record().getFile()).isEqualTo(fileStats);
    verify(statsStore, times(1))
        .putTargetStats(
            argThat(r -> r.hasFile() && "/data/file-1.parquet".equals(r.getFile().getFilePath())));
  }

  @Test
  void suppressesDuplicateInFlightPersistenceForSameTarget() {
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    ConnectorRepository connectorRepository = Mockito.mock(ConnectorRepository.class);
    CredentialResolver credentialResolver = Mockito.mock(CredentialResolver.class);
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    FloecatConnector floecatConnector = Mockito.mock(FloecatConnector.class);

    IcebergNativeStatsCaptureEngine engine =
        new IcebergNativeStatsCaptureEngine(
            tableRepository, connectorRepository, credentialResolver, statsStore);
    engine.connectorOpener = config -> floecatConnector;
    List<Runnable> queuedWrites = new CopyOnWriteArrayList<>();
    engine.persistExecutor = queuedWrites::add;

    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct").setId("table-1").build();
    ResourceId connectorId = ResourceId.newBuilder().setAccountId("acct").setId("conn-1").build();
    when(tableRepository.getById(tableId))
        .thenReturn(
            Optional.of(
                Table.newBuilder()
                    .setResourceId(tableId)
                    .setDisplayName("events")
                    .setUpstream(
                        ai.floedb.floecat.catalog.rpc.UpstreamRef.newBuilder()
                            .setConnectorId(connectorId)
                            .addNamespacePath("db")
                            .setTableDisplayName("events")
                            .build())
                    .build()));
    when(connectorRepository.getById(connectorId))
        .thenReturn(
            Optional.of(
                Connector.newBuilder()
                    .setResourceId(connectorId)
                    .setDisplayName("iceberg-main")
                    .setKind(ConnectorKind.CK_ICEBERG)
                    .setUri("s3://warehouse")
                    .build()));

    StatsTarget tableTarget =
        StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()).build();
    TargetStatsRecord tableRecord =
        TargetStatsRecord.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(101L)
            .setTarget(tableTarget)
            .setTable(TableValueStats.newBuilder().setRowCount(11L).build())
            .build();
    when(floecatConnector.captureSnapshotTargetStats(any(), any(), any(), anyLong(), any(), any()))
        .thenReturn(List.of(tableRecord));

    StatsCaptureRequest request =
        StatsCaptureRequest.builder(tableId, 101L, tableTarget)
            .executionMode(StatsExecutionMode.SYNC)
            .connectorType("iceberg")
            .correlationId("corr")
            .build();

    assertThat(engine.capture(request)).isPresent();
    assertThat(queuedWrites).hasSize(1);

    assertThat(engine.capture(request)).isPresent();
    assertThat(queuedWrites).hasSize(1);
    verify(statsStore, never()).putTargetStats(any());

    queuedWrites.get(0).run();

    verify(statsStore, times(1))
        .putTargetStats(argThat(r -> r.hasTable() && r.getSnapshotId() == 101L));
  }

  @Test
  void fallsBackToCallerThreadPersistenceWhenQueueRejects() {
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    ConnectorRepository connectorRepository = Mockito.mock(ConnectorRepository.class);
    CredentialResolver credentialResolver = Mockito.mock(CredentialResolver.class);
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    FloecatConnector floecatConnector = Mockito.mock(FloecatConnector.class);

    IcebergNativeStatsCaptureEngine engine =
        new IcebergNativeStatsCaptureEngine(
            tableRepository, connectorRepository, credentialResolver, statsStore);
    engine.connectorOpener = config -> floecatConnector;
    engine.persistExecutor =
        runnable -> {
          throw new RejectedExecutionException("queue full");
        };

    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct").setId("table-1").build();
    ResourceId connectorId = ResourceId.newBuilder().setAccountId("acct").setId("conn-1").build();
    when(tableRepository.getById(tableId))
        .thenReturn(
            Optional.of(
                Table.newBuilder()
                    .setResourceId(tableId)
                    .setDisplayName("events")
                    .setUpstream(
                        ai.floedb.floecat.catalog.rpc.UpstreamRef.newBuilder()
                            .setConnectorId(connectorId)
                            .addNamespacePath("db")
                            .setTableDisplayName("events")
                            .build())
                    .build()));
    when(connectorRepository.getById(connectorId))
        .thenReturn(
            Optional.of(
                Connector.newBuilder()
                    .setResourceId(connectorId)
                    .setDisplayName("iceberg-main")
                    .setKind(ConnectorKind.CK_ICEBERG)
                    .setUri("s3://warehouse")
                    .build()));

    StatsTarget tableTarget =
        StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()).build();
    TargetStatsRecord tableRecord =
        TargetStatsRecord.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(101L)
            .setTarget(tableTarget)
            .setTable(TableValueStats.newBuilder().setRowCount(11L).build())
            .build();
    when(floecatConnector.captureSnapshotTargetStats(any(), any(), any(), anyLong(), any(), any()))
        .thenReturn(List.of(tableRecord));

    StatsCaptureRequest request =
        StatsCaptureRequest.builder(tableId, 101L, tableTarget)
            .executionMode(StatsExecutionMode.SYNC)
            .connectorType("iceberg")
            .correlationId("corr")
            .build();

    assertThat(engine.capture(request)).isPresent();

    verify(statsStore, times(1))
        .putTargetStats(argThat(r -> r.hasTable() && r.getSnapshotId() == 101L));
  }

  @Test
  void returnsEmptyWhenSnapshotNotFoundInConnectorCapture() {
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    ConnectorRepository connectorRepository = Mockito.mock(ConnectorRepository.class);
    CredentialResolver credentialResolver = Mockito.mock(CredentialResolver.class);
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    FloecatConnector floecatConnector = Mockito.mock(FloecatConnector.class);

    IcebergNativeStatsCaptureEngine engine =
        new IcebergNativeStatsCaptureEngine(
            tableRepository, connectorRepository, credentialResolver, statsStore);
    engine.connectorOpener = config -> floecatConnector;
    engine.persistExecutor = Runnable::run;

    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct").setId("table-1").build();
    ResourceId connectorId = ResourceId.newBuilder().setAccountId("acct").setId("conn-1").build();
    when(tableRepository.getById(tableId))
        .thenReturn(
            Optional.of(
                Table.newBuilder()
                    .setResourceId(tableId)
                    .setDisplayName("events")
                    .setUpstream(
                        ai.floedb.floecat.catalog.rpc.UpstreamRef.newBuilder()
                            .setConnectorId(connectorId)
                            .addNamespacePath("db")
                            .setTableDisplayName("events")
                            .build())
                    .build()));
    when(connectorRepository.getById(connectorId))
        .thenReturn(
            Optional.of(
                Connector.newBuilder()
                    .setResourceId(connectorId)
                    .setDisplayName("iceberg-main")
                    .setKind(ConnectorKind.CK_ICEBERG)
                    .setUri("s3://warehouse")
                    .build()));
    when(floecatConnector.captureSnapshotTargetStats(any(), any(), any(), anyLong(), any(), any()))
        .thenReturn(List.of());

    StatsCaptureRequest request =
        StatsCaptureRequest.builder(
                tableId,
                999999L,
                StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()).build())
            .executionMode(StatsExecutionMode.SYNC)
            .connectorType("iceberg")
            .correlationId("corr")
            .build();

    assertThat(engine.capture(request)).isEmpty();
    verify(floecatConnector)
        .captureSnapshotTargetStats(any(), any(), any(), anyLong(), any(), any());
    verify(statsStore, never()).putTargetStats(any());
  }

  @Test
  void captureBatchOpensConnectorOncePerTableSnapshotAndCapturesMultipleTargets() {
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    ConnectorRepository connectorRepository = Mockito.mock(ConnectorRepository.class);
    CredentialResolver credentialResolver = Mockito.mock(CredentialResolver.class);
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    FloecatConnector floecatConnector = Mockito.mock(FloecatConnector.class);

    IcebergNativeStatsCaptureEngine engine =
        new IcebergNativeStatsCaptureEngine(
            tableRepository, connectorRepository, credentialResolver, statsStore);
    AtomicInteger openCount = new AtomicInteger();
    engine.connectorOpener =
        config -> {
          openCount.incrementAndGet();
          return floecatConnector;
        };
    engine.persistExecutor = Runnable::run;

    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct").setId("table-1").build();
    ResourceId connectorId = ResourceId.newBuilder().setAccountId("acct").setId("conn-1").build();
    when(tableRepository.getById(tableId))
        .thenReturn(
            Optional.of(
                Table.newBuilder()
                    .setResourceId(tableId)
                    .setDisplayName("events")
                    .setUpstream(
                        ai.floedb.floecat.catalog.rpc.UpstreamRef.newBuilder()
                            .setConnectorId(connectorId)
                            .addNamespacePath("db")
                            .setTableDisplayName("events")
                            .build())
                    .build()));
    when(connectorRepository.getById(connectorId))
        .thenReturn(
            Optional.of(
                Connector.newBuilder()
                    .setResourceId(connectorId)
                    .setDisplayName("iceberg-main")
                    .setKind(ConnectorKind.CK_ICEBERG)
                    .setUri("s3://warehouse")
                    .build()));

    StatsTarget tableTarget =
        StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()).build();
    StatsTarget col7 =
        StatsTarget.newBuilder().setColumn(ColumnStatsTarget.newBuilder().setColumnId(7L)).build();
    StatsTarget col9 =
        StatsTarget.newBuilder().setColumn(ColumnStatsTarget.newBuilder().setColumnId(9L)).build();
    when(floecatConnector.captureSnapshotTargetStats(any(), any(), any(), anyLong(), any(), any()))
        .thenReturn(
            List.of(
                TargetStatsRecord.newBuilder()
                    .setTableId(tableId)
                    .setSnapshotId(101L)
                    .setTarget(tableTarget)
                    .setTable(TableValueStats.newBuilder().setRowCount(11L).build())
                    .build(),
                TargetStatsRecord.newBuilder()
                    .setTableId(tableId)
                    .setSnapshotId(101L)
                    .setTarget(col7)
                    .setScalar(
                        ScalarStats.newBuilder().setDisplayName("c7").setLogicalType("BIGINT"))
                    .build(),
                TargetStatsRecord.newBuilder()
                    .setTableId(tableId)
                    .setSnapshotId(101L)
                    .setTarget(col9)
                    .setScalar(
                        ScalarStats.newBuilder().setDisplayName("c9").setLogicalType("BIGINT"))
                    .build()));

    StatsCaptureRequest tableReq =
        StatsCaptureRequest.builder(tableId, 101L, tableTarget)
            .executionMode(StatsExecutionMode.ASYNC)
            .connectorType("iceberg")
            .correlationId("corr")
            .build();
    StatsCaptureRequest col7Req =
        StatsCaptureRequest.builder(tableId, 101L, col7)
            .executionMode(StatsExecutionMode.ASYNC)
            .connectorType("iceberg")
            .correlationId("corr")
            .build();
    StatsCaptureRequest col9Req =
        StatsCaptureRequest.builder(tableId, 101L, col9)
            .executionMode(StatsExecutionMode.ASYNC)
            .connectorType("iceberg")
            .correlationId("corr")
            .build();

    StatsCaptureBatchResult out =
        engine.captureBatch(StatsCaptureBatchRequest.of(List.of(tableReq, col7Req, col9Req)));

    assertThat(out.results()).hasSize(3);
    assertThat(out.results()).allMatch(item -> item.outcome() == StatsTriggerOutcome.CAPTURED);
    assertThat(openCount.get()).isEqualTo(1);
    verify(floecatConnector, times(1))
        .captureSnapshotTargetStats(any(), any(), any(), anyLong(), any(), any());
    verify(statsStore, times(3)).putTargetStats(any(TargetStatsRecord.class));
  }

  @Test
  void captureBatchMarksGroupDegradedWhenConnectorThrows() {
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    ConnectorRepository connectorRepository = Mockito.mock(ConnectorRepository.class);
    CredentialResolver credentialResolver = Mockito.mock(CredentialResolver.class);
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    FloecatConnector floecatConnector = Mockito.mock(FloecatConnector.class);

    IcebergNativeStatsCaptureEngine engine =
        new IcebergNativeStatsCaptureEngine(
            tableRepository, connectorRepository, credentialResolver, statsStore);
    engine.connectorOpener = config -> floecatConnector;

    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct").setId("table-1").build();
    ResourceId connectorId = ResourceId.newBuilder().setAccountId("acct").setId("conn-1").build();
    when(tableRepository.getById(tableId))
        .thenReturn(
            Optional.of(
                Table.newBuilder()
                    .setResourceId(tableId)
                    .setDisplayName("events")
                    .setUpstream(
                        ai.floedb.floecat.catalog.rpc.UpstreamRef.newBuilder()
                            .setConnectorId(connectorId)
                            .addNamespacePath("db")
                            .setTableDisplayName("events")
                            .build())
                    .build()));
    when(connectorRepository.getById(connectorId))
        .thenReturn(
            Optional.of(
                Connector.newBuilder()
                    .setResourceId(connectorId)
                    .setDisplayName("iceberg-main")
                    .setKind(ConnectorKind.CK_ICEBERG)
                    .setUri("s3://warehouse")
                    .build()));
    when(floecatConnector.captureSnapshotTargetStats(any(), any(), any(), anyLong(), any(), any()))
        .thenThrow(new RuntimeException("connector boom"));

    StatsTarget tableTarget =
        StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()).build();
    StatsTarget columnTarget =
        StatsTarget.newBuilder().setColumn(ColumnStatsTarget.newBuilder().setColumnId(7L)).build();
    StatsCaptureRequest tableReq =
        StatsCaptureRequest.builder(tableId, 101L, tableTarget)
            .executionMode(StatsExecutionMode.ASYNC)
            .connectorType("iceberg")
            .correlationId("corr")
            .build();
    StatsCaptureRequest columnReq =
        StatsCaptureRequest.builder(tableId, 101L, columnTarget)
            .executionMode(StatsExecutionMode.ASYNC)
            .connectorType("iceberg")
            .correlationId("corr")
            .build();

    StatsCaptureBatchResult out =
        engine.captureBatch(StatsCaptureBatchRequest.of(List.of(tableReq, columnReq)));

    assertThat(out.results()).hasSize(2);
    assertThat(out.results()).allMatch(item -> item.outcome() == StatsTriggerOutcome.DEGRADED);
    verify(statsStore, never()).putTargetStats(any());
  }
}
