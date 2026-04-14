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
import static org.mockito.Mockito.never;
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
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsExecutionMode;
import ai.floedb.floecat.stats.spi.StatsKind;
import ai.floedb.floecat.stats.spi.StatsStore;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class DeltaNativeStatsCaptureEngineTest {

  @Test
  void capturesTableStatsAndPersistsBundleRecords() {
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    ConnectorRepository connectorRepository = Mockito.mock(ConnectorRepository.class);
    CredentialResolver credentialResolver = Mockito.mock(CredentialResolver.class);
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    FloecatConnector floecatConnector = Mockito.mock(FloecatConnector.class);

    DeltaNativeStatsCaptureEngine engine =
        new DeltaNativeStatsCaptureEngine(
            tableRepository, connectorRepository, credentialResolver, statsStore);
    engine.connectorOpener = config -> floecatConnector;

    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct").setId("table-1").build();
    ResourceId connectorId = ResourceId.newBuilder().setAccountId("acct").setId("conn-2").build();
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
                    .setDisplayName("delta-main")
                    .setKind(ConnectorKind.CK_DELTA)
                    .setUri("s3://delta")
                    .build()));

    StatsTarget tableTarget =
        StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()).build();
    StatsTarget columnTarget =
        StatsTarget.newBuilder().setColumn(ColumnStatsTarget.newBuilder().setColumnId(9L)).build();
    TargetStatsRecord tableRecord =
        TargetStatsRecord.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(0L)
            .setTarget(tableTarget)
            .setTable(TableValueStats.newBuilder().setRowCount(21L).build())
            .build();
    TargetStatsRecord columnRecord =
        TargetStatsRecord.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(0L)
            .setTarget(columnTarget)
            .setScalar(ScalarStats.newBuilder().setDisplayName("c9").setLogicalType("BIGINT"))
            .build();
    when(floecatConnector.captureSnapshotTargetStats(any(), any(), any(), anyLong(), any()))
        .thenReturn(List.of(tableRecord, columnRecord));

    StatsCaptureRequest request =
        StatsCaptureRequest.builder(tableId, 0L, tableTarget)
            .columnSelectors(Set.of("c9"))
            .requestedKinds(Set.of(StatsKind.ROW_COUNT))
            .executionMode(StatsExecutionMode.SYNC)
            .connectorType("delta")
            .correlationId("corr")
            .build();

    Optional<ai.floedb.floecat.stats.spi.StatsCaptureResult> result = engine.capture(request);

    assertThat(result).isPresent();
    assertThat(result.get().record().getTable()).isEqualTo(tableRecord.getTable());
    verify(statsStore)
        .putTargetStats(
            org.mockito.ArgumentMatchers.argThat(r -> r.hasTable() && r.getSnapshotId() == 0L));
    verify(statsStore)
        .putTargetStats(
            org.mockito.ArgumentMatchers.argThat(
                r ->
                    r.hasScalar()
                        && r.hasTarget()
                        && r.getTarget().hasColumn()
                        && r.getTarget().getColumn().getColumnId() == 9L));
  }

  @Test
  void capturesRequestedColumnStats() {
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    ConnectorRepository connectorRepository = Mockito.mock(ConnectorRepository.class);
    CredentialResolver credentialResolver = Mockito.mock(CredentialResolver.class);
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    FloecatConnector floecatConnector = Mockito.mock(FloecatConnector.class);

    DeltaNativeStatsCaptureEngine engine =
        new DeltaNativeStatsCaptureEngine(
            tableRepository, connectorRepository, credentialResolver, statsStore);
    engine.connectorOpener = config -> floecatConnector;

    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct").setId("table-1").build();
    ResourceId connectorId = ResourceId.newBuilder().setAccountId("acct").setId("conn-2").build();
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
                    .setDisplayName("delta-main")
                    .setKind(ConnectorKind.CK_DELTA)
                    .setUri("s3://delta")
                    .build()));

    StatsTarget requested =
        StatsTarget.newBuilder().setColumn(ColumnStatsTarget.newBuilder().setColumnId(9L)).build();
    TargetStatsRecord columnRecord =
        TargetStatsRecord.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(77L)
            .setTarget(requested)
            .setScalar(ScalarStats.newBuilder().setDisplayName("c9").setLogicalType("BIGINT"))
            .build();
    when(floecatConnector.captureSnapshotTargetStats(any(), any(), any(), anyLong(), any()))
        .thenReturn(List.of(columnRecord));

    StatsCaptureRequest request =
        StatsCaptureRequest.builder(tableId, 77L, requested)
            .columnSelectors(Set.of("c9"))
            .requestedKinds(Set.of(StatsKind.NULL_COUNT))
            .executionMode(StatsExecutionMode.SYNC)
            .connectorType("delta")
            .correlationId("corr")
            .build();

    Optional<ai.floedb.floecat.stats.spi.StatsCaptureResult> result = engine.capture(request);

    assertThat(result).isPresent();
    assertThat(result.get().record().getScalar()).isEqualTo(columnRecord.getScalar());
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
        .containsEntry("engine_id", DeltaNativeStatsCaptureEngine.ENGINE_ID);
    verify(floecatConnector)
        .captureSnapshotTargetStats(
            any(),
            any(),
            any(),
            anyLong(),
            org.mockito.ArgumentMatchers.argThat(selectors -> selectors.contains("c9")));
    verify(statsStore)
        .putTargetStats(
            org.mockito.ArgumentMatchers.argThat(
                r ->
                    r.hasScalar()
                        && r.hasTarget()
                        && r.getTarget().hasColumn()
                        && r.getTarget().getColumn().getColumnId() == 9L));
  }

  @Test
  void returnsEmptyWhenSnapshotIdNotSet() {
    DeltaNativeStatsCaptureEngine engine =
        new DeltaNativeStatsCaptureEngine(
            Mockito.mock(TableRepository.class),
            Mockito.mock(ConnectorRepository.class),
            Mockito.mock(CredentialResolver.class),
            Mockito.mock(StatsStore.class));

    StatsCaptureRequest request =
        StatsCaptureRequest.builder(
                ResourceId.newBuilder().setAccountId("acct").setId("table-1").build(),
                0L,
                StatsTarget.newBuilder()
                    .setColumn(ColumnStatsTarget.newBuilder().setColumnId(9L))
                    .build())
            .executionMode(StatsExecutionMode.SYNC)
            .connectorType("delta")
            .correlationId("corr")
            .build();

    assertThat(engine.capture(request)).isEmpty();
  }

  @Test
  void capturesRequestedFileStats() {
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    ConnectorRepository connectorRepository = Mockito.mock(ConnectorRepository.class);
    CredentialResolver credentialResolver = Mockito.mock(CredentialResolver.class);
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    FloecatConnector floecatConnector = Mockito.mock(FloecatConnector.class);

    DeltaNativeStatsCaptureEngine engine =
        new DeltaNativeStatsCaptureEngine(
            tableRepository, connectorRepository, credentialResolver, statsStore);
    engine.connectorOpener = config -> floecatConnector;

    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct").setId("table-1").build();
    ResourceId connectorId = ResourceId.newBuilder().setAccountId("acct").setId("conn-2").build();
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
                    .setDisplayName("delta-main")
                    .setKind(ConnectorKind.CK_DELTA)
                    .setUri("s3://delta")
                    .build()));

    StatsTarget fileTarget =
        StatsTarget.newBuilder()
            .setFile(FileStatsTarget.newBuilder().setFilePath("/delta/file-1.parquet"))
            .build();
    FileTargetStats fileStats =
        FileTargetStats.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(77L)
            .setFilePath("/delta/file-1.parquet")
            .setFileFormat("parquet")
            .setRowCount(9L)
            .setSizeBytes(90L)
            .build();
    TargetStatsRecord fileRecord =
        TargetStatsRecord.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(77L)
            .setTarget(fileTarget)
            .setFile(fileStats)
            .build();
    when(floecatConnector.captureSnapshotTargetStats(any(), any(), any(), anyLong(), any()))
        .thenReturn(List.of(fileRecord));

    StatsCaptureRequest request =
        StatsCaptureRequest.builder(tableId, 77L, fileTarget)
            .requestedKinds(Set.of(StatsKind.ROW_COUNT, StatsKind.TOTAL_BYTES))
            .executionMode(StatsExecutionMode.SYNC)
            .connectorType("delta")
            .correlationId("corr")
            .build();

    Optional<ai.floedb.floecat.stats.spi.StatsCaptureResult> result = engine.capture(request);

    assertThat(result).isPresent();
    assertThat(result.get().record().getFile()).isEqualTo(fileStats);
    verify(statsStore)
        .putTargetStats(
            org.mockito.ArgumentMatchers.argThat(
                r -> r.hasFile() && "/delta/file-1.parquet".equals(r.getFile().getFilePath())));
  }

  @Test
  void returnsEmptyWhenSnapshotNotFoundInConnectorCapture() {
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    ConnectorRepository connectorRepository = Mockito.mock(ConnectorRepository.class);
    CredentialResolver credentialResolver = Mockito.mock(CredentialResolver.class);
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    FloecatConnector floecatConnector = Mockito.mock(FloecatConnector.class);

    DeltaNativeStatsCaptureEngine engine =
        new DeltaNativeStatsCaptureEngine(
            tableRepository, connectorRepository, credentialResolver, statsStore);
    engine.connectorOpener = config -> floecatConnector;

    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct").setId("table-1").build();
    ResourceId connectorId = ResourceId.newBuilder().setAccountId("acct").setId("conn-2").build();
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
                    .setDisplayName("delta-main")
                    .setKind(ConnectorKind.CK_DELTA)
                    .setUri("s3://delta")
                    .build()));
    when(floecatConnector.captureSnapshotTargetStats(any(), any(), any(), anyLong(), any()))
        .thenReturn(List.of());

    StatsCaptureRequest request =
        StatsCaptureRequest.builder(
                tableId,
                999999L,
                StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()).build())
            .executionMode(StatsExecutionMode.SYNC)
            .connectorType("delta")
            .correlationId("corr")
            .build();

    assertThat(engine.capture(request)).isEmpty();
    verify(floecatConnector).captureSnapshotTargetStats(any(), any(), any(), anyLong(), any());
    verify(statsStore, never()).putTargetStats(any());
  }
}
