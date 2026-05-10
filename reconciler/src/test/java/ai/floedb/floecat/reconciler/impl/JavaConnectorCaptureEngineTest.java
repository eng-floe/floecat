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

package ai.floedb.floecat.reconciler.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.FileStatsTarget;
import ai.floedb.floecat.catalog.rpc.FileTargetStats;
import ai.floedb.floecat.catalog.rpc.Ndv;
import ai.floedb.floecat.catalog.rpc.ScalarStats;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.common.ndv.ColumnNdv;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.connector.spi.ConnectorConfig;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.reconciler.spi.capture.CaptureEngineRequest;
import io.grpc.Status;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.datasketches.theta.UpdateSketch;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class JavaConnectorCaptureEngineTest {
  private static final Connector SOURCE_CONNECTOR =
      Connector.newBuilder().setKind(ConnectorKind.CK_ICEBERG).build();

  @Test
  void capabilitiesAdvertiseFileGroupContract() {
    JavaConnectorCaptureEngine engine = new JavaConnectorCaptureEngine();

    assertThat(engine.capabilities().statsTargetKinds())
        .containsExactlyInAnyOrder(
            FloecatConnector.StatsTargetKind.TABLE,
            FloecatConnector.StatsTargetKind.COLUMN,
            FloecatConnector.StatsTargetKind.FILE);
    assertThat(engine.capabilities().pageIndex()).isTrue();
    assertThat(engine.capabilities().supportsExpressionTargets()).isFalse();
    assertThat(engine.capabilities().columnSelectors()).isTrue();
    assertThat(engine.capabilities().executionScope())
        .isEqualTo(
            ai.floedb.floecat.reconciler.spi.capture.CaptureEngineCapabilities.ExecutionScope
                .FILE_GROUP_ONLY);
    assertThat(engine.capabilities().resultContract())
        .isEqualTo(
            ai.floedb.floecat.reconciler.spi.capture.CaptureEngineCapabilities.ResultContract
                .COMPLETE_FILE_GROUP_OUTPUTS);
    assertThat(engine.capabilities().executionRuntime())
        .isEqualTo(
            ai.floedb.floecat.reconciler.spi.capture.CaptureEngineCapabilities.ExecutionRuntime
                .LOCAL_ONLY);
  }

  @Test
  void supportsRejectsRequestsOutsideAdvertisedFileGroupContract() {
    JavaConnectorCaptureEngine engine = new JavaConnectorCaptureEngine();
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct").setId("table-1").build();

    CaptureEngineRequest missingPlannedFiles =
        new CaptureEngineRequest(
            SOURCE_CONNECTOR,
            "db",
            "events",
            tableId,
            55L,
            "plan-1",
            "group-1",
            List.of(),
            Set.of(),
            Set.of(),
            Set.of(FloecatConnector.StatsTargetKind.FILE),
            false,
            Optional.empty());

    assertThat(engine.supports(missingPlannedFiles)).isFalse();
  }

  @Test
  void captureDeclinesRequestsOutsideAdvertisedFileGroupContract() {
    FloecatConnector connector = Mockito.mock(FloecatConnector.class);
    JavaConnectorCaptureEngine engine = new JavaConnectorCaptureEngine();
    engine.connectorOpener = ignored -> connector;

    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct").setId("table-1").build();
    CaptureEngineRequest request =
        new CaptureEngineRequest(
            SOURCE_CONNECTOR,
            "db",
            "events",
            tableId,
            55L,
            "plan-1",
            "group-1",
            List.of(),
            Set.of(),
            Set.of(),
            Set.of(FloecatConnector.StatsTargetKind.FILE),
            false,
            Optional.empty());

    assertThat(engine.capture(request)).isEmpty();
    verify(connector, never())
        .capturePlannedFileGroup(any(), any(), any(), anyLong(), any(), any(), any(), anyBoolean());
    verify(connector, never())
        .captureSnapshotTargetStats(any(), any(), any(), anyLong(), any(), any());
  }

  @Test
  void capturePassesAuthorizationTokenToStorageResolver() {
    FloecatConnector connector = Mockito.mock(FloecatConnector.class);
    JavaConnectorCaptureEngine engine = new JavaConnectorCaptureEngine();
    engine.connectorOpener = ignored -> connector;

    ServerSideStorageConfigResolver storageResolver =
        Mockito.mock(ServerSideStorageConfigResolver.class);
    engine.serverSideStorageConfigResolver = storageResolver;

    ConnectorConfig resolvedConfig =
        new ConnectorConfig(
            ConnectorConfig.Kind.ICEBERG,
            "resolved",
            "s3://warehouse/table",
            java.util.Map.of(),
            new ConnectorConfig.Auth("none", java.util.Map.of(), java.util.Map.of()));
    when(storageResolver.resolveWithAuthorization(
            eq(Optional.of("worker-token")), eq(SOURCE_CONNECTOR), any()))
        .thenReturn(resolvedConfig);
    when(connector.capturePlannedFileGroup(
            any(), any(), any(), anyLong(), any(), any(), any(), anyBoolean()))
        .thenReturn(FloecatConnector.FileGroupCaptureResult.of(List.of(), List.of()));

    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct").setId("table-1").build();
    CaptureEngineRequest request =
        new CaptureEngineRequest(
            SOURCE_CONNECTOR,
            "db",
            "events",
            tableId,
            55L,
            "plan-1",
            "group-1",
            List.of("s3://bucket/path/file-1.parquet"),
            Set.of("id"),
            Set.of(),
            Set.of(FloecatConnector.StatsTargetKind.COLUMN),
            false,
            Optional.of("worker-token"));

    assertThat(engine.capture(request)).isPresent();
    verify(storageResolver)
        .resolveWithAuthorization(eq(Optional.of("worker-token")), eq(SOURCE_CONNECTOR), any());
  }

  @Test
  void captureMarksPermissionDeniedTerminal() {
    JavaConnectorCaptureEngine engine = new JavaConnectorCaptureEngine();
    engine.connectorOpener =
        ignored -> {
          throw Status.PERMISSION_DENIED.withDescription("Forbidden").asRuntimeException();
        };

    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct").setId("table-1").build();
    CaptureEngineRequest request =
        new CaptureEngineRequest(
            SOURCE_CONNECTOR,
            "db",
            "events",
            tableId,
            55L,
            "plan-1",
            "group-1",
            List.of("s3://bucket/path/file-1.parquet"),
            Set.of("id"),
            Set.of(),
            Set.of(FloecatConnector.StatsTargetKind.COLUMN),
            false,
            Optional.empty());

    assertThatThrownBy(() -> engine.capture(request))
        .isInstanceOf(ReconcileFailureException.class)
        .satisfies(
            failure ->
                assertThat(((ReconcileFailureException) failure).retryDisposition())
                    .isEqualTo(ReconcileExecutor.ExecutionResult.RetryDisposition.TERMINAL));
  }

  @Test
  void captureDerivesTableAndColumnStatsFromFileGroupStats() {
    FloecatConnector connector = Mockito.mock(FloecatConnector.class);
    JavaConnectorCaptureEngine engine = new JavaConnectorCaptureEngine();
    engine.connectorOpener = ignored -> connector;

    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct").setId("table-1").build();
    String plannedFile = "s3://bucket/path/file-1.parquet";

    TargetStatsRecord fileRecord =
        TargetStatsRecord.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(55L)
            .setTarget(
                StatsTarget.newBuilder()
                    .setFile(FileStatsTarget.newBuilder().setFilePath(plannedFile)))
            .setFile(
                FileTargetStats.newBuilder()
                    .setTableId(tableId)
                    .setSnapshotId(55L)
                    .setFilePath(plannedFile)
                    .setFileFormat("PARQUET")
                    .setRowCount(100L)
                    .setSizeBytes(1024L)
                    .addColumns(
                        FileColumnStats.newBuilder()
                            .setColumnId(7L)
                            .setScalar(
                                ScalarStats.newBuilder()
                                    .setDisplayName("id")
                                    .setLogicalType("BIGINT")
                                    .setValueCount(100L)
                                    .setNullCount(0L))
                            .build())
                    .build())
            .build();

    when(connector.capturePlannedFileGroup(
            eq("db"),
            eq("events"),
            eq(tableId),
            eq(55L),
            eq(Set.of(plannedFile)),
            eq(Set.of("id")),
            eq(
                Set.of(
                    FloecatConnector.StatsTargetKind.TABLE,
                    FloecatConnector.StatsTargetKind.COLUMN,
                    FloecatConnector.StatsTargetKind.FILE)),
            eq(false)))
        .thenReturn(FloecatConnector.FileGroupCaptureResult.of(List.of(fileRecord), List.of()));

    CaptureEngineRequest request =
        new CaptureEngineRequest(
            SOURCE_CONNECTOR,
            "db",
            "events",
            tableId,
            55L,
            "plan-1",
            "group-1",
            List.of(plannedFile),
            Set.of("id"),
            Set.of(),
            Set.of(
                FloecatConnector.StatsTargetKind.TABLE,
                FloecatConnector.StatsTargetKind.COLUMN,
                FloecatConnector.StatsTargetKind.FILE),
            false,
            Optional.empty());

    var result = engine.capture(request);

    assertThat(result).isPresent();
    assertThat(result.get().statsRecords())
        .extracting(TargetStatsRecord::getValueCase)
        .containsExactlyInAnyOrder(
            TargetStatsRecord.ValueCase.TABLE,
            TargetStatsRecord.ValueCase.SCALAR,
            TargetStatsRecord.ValueCase.FILE);
    assertThat(result.get().statsRecords())
        .filteredOn(TargetStatsRecord::hasFile)
        .singleElement()
        .extracting(record -> record.getFile().getFilePath())
        .isEqualTo(plannedFile);
    assertThat(result.get().statsRecords())
        .filteredOn(TargetStatsRecord::hasTable)
        .singleElement()
        .extracting(record -> record.getTable().getDataFileCount())
        .isEqualTo(1L);
    assertThat(result.get().statsRecords())
        .filteredOn(record -> record.hasScalar() && record.getTarget().hasColumn())
        .singleElement()
        .extracting(record -> record.getScalar().getValueCount())
        .isEqualTo(100L);

    verify(connector, never())
        .captureSnapshotTargetStats(any(), any(), any(), anyLong(), any(), any());
    verify(connector)
        .capturePlannedFileGroup(any(), any(), any(), anyLong(), any(), any(), any(), anyBoolean());
  }

  @Test
  void captureUsesOnlyFileGroupStatsForFileOnlyRequests() {
    FloecatConnector connector = Mockito.mock(FloecatConnector.class);
    JavaConnectorCaptureEngine engine = new JavaConnectorCaptureEngine();
    engine.connectorOpener = ignored -> connector;

    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct").setId("table-1").build();
    String plannedFile = "s3://bucket/path/file-1.parquet";
    TargetStatsRecord fileRecord =
        TargetStatsRecord.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(55L)
            .setTarget(
                StatsTarget.newBuilder()
                    .setFile(FileStatsTarget.newBuilder().setFilePath(plannedFile)))
            .setFile(
                FileTargetStats.newBuilder()
                    .setTableId(tableId)
                    .setSnapshotId(55L)
                    .setFilePath(plannedFile)
                    .setFileFormat("PARQUET")
                    .setRowCount(40L)
                    .setSizeBytes(1024L))
            .build();

    when(connector.capturePlannedFileGroup(
            eq("db"),
            eq("events"),
            eq(tableId),
            eq(55L),
            eq(Set.of(plannedFile)),
            eq(Set.of()),
            eq(Set.of(FloecatConnector.StatsTargetKind.FILE)),
            eq(false)))
        .thenReturn(FloecatConnector.FileGroupCaptureResult.of(List.of(fileRecord), List.of()));

    CaptureEngineRequest request =
        new CaptureEngineRequest(
            SOURCE_CONNECTOR,
            "db",
            "events",
            tableId,
            55L,
            "plan-1",
            "group-1",
            List.of(plannedFile),
            Set.of(),
            Set.of(),
            Set.of(FloecatConnector.StatsTargetKind.FILE),
            false,
            Optional.empty());

    var result = engine.capture(request);

    assertThat(result).isPresent();
    assertThat(result.get().statsRecords()).containsExactly(fileRecord);
    verify(connector, never())
        .captureSnapshotTargetStats(any(), any(), any(), anyLong(), any(), any());
    verify(connector)
        .capturePlannedFileGroup(any(), any(), any(), anyLong(), any(), any(), any(), anyBoolean());
  }

  @Test
  void captureAggregatesThetaSketchNdvAcrossFileGroupStats() {
    FloecatConnector connector = Mockito.mock(FloecatConnector.class);
    JavaConnectorCaptureEngine engine = new JavaConnectorCaptureEngine();
    engine.connectorOpener = ignored -> connector;

    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct").setId("table-1").build();
    String fileOne = "s3://bucket/path/file-1.parquet";
    String fileTwo = "s3://bucket/path/file-2.parquet";

    when(connector.capturePlannedFileGroup(
            eq("db"),
            eq("events"),
            eq(tableId),
            eq(55L),
            eq(Set.of(fileOne, fileTwo)),
            eq(Set.of("id")),
            eq(Set.of(FloecatConnector.StatsTargetKind.COLUMN)),
            eq(false)))
        .thenReturn(
            FloecatConnector.FileGroupCaptureResult.of(
                List.of(
                    fileRecordWithColumnNdv(tableId, 55L, fileOne, ndvWithThetaSketch(1L, 2L)),
                    fileRecordWithColumnNdv(tableId, 55L, fileTwo, ndvWithThetaSketch(2L, 3L))),
                List.of()));

    CaptureEngineRequest request =
        new CaptureEngineRequest(
            SOURCE_CONNECTOR,
            "db",
            "events",
            tableId,
            55L,
            "plan-1",
            "group-1",
            List.of(fileOne, fileTwo),
            Set.of("id"),
            Set.of(),
            Set.of(FloecatConnector.StatsTargetKind.COLUMN),
            false,
            Optional.empty());

    var result = engine.capture(request);

    assertThat(result).isPresent();
    assertThat(result.get().statsRecords())
        .filteredOn(record -> record.hasScalar() && record.getTarget().hasColumn())
        .singleElement()
        .satisfies(
            record -> {
              assertThat(record.getScalar().hasNdv()).isTrue();
              assertThat(record.getScalar().getNdv().hasApprox()).isTrue();
              assertThat(record.getScalar().getNdv().getSketchesCount()).isEqualTo(1);
              assertThat(record.getScalar().getNdv().getApprox().getMethod())
                  .isEqualTo("apache-datasketches-theta");
              assertThat(record.getScalar().getNdv().getApprox().getEstimate())
                  .isBetween(2.5d, 3.5d);
            });
  }

  @Test
  void captureKeepsStatsAndPageIndexSelectorsSeparate() {
    FloecatConnector connector = Mockito.mock(FloecatConnector.class);
    JavaConnectorCaptureEngine engine = new JavaConnectorCaptureEngine();
    engine.connectorOpener = ignored -> connector;

    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct").setId("table-1").build();
    String plannedFile = "s3://bucket/path/file-1.parquet";
    TargetStatsRecord fileRecord =
        TargetStatsRecord.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(55L)
            .setTarget(
                StatsTarget.newBuilder()
                    .setFile(FileStatsTarget.newBuilder().setFilePath(plannedFile)))
            .setFile(
                FileTargetStats.newBuilder()
                    .setTableId(tableId)
                    .setSnapshotId(55L)
                    .setFilePath(plannedFile)
                    .setFileFormat("PARQUET")
                    .setRowCount(40L)
                    .setSizeBytes(1024L))
            .build();

    when(connector.capturePlannedFileGroup(
            eq("db"),
            eq("events"),
            eq(tableId),
            eq(55L),
            eq(Set.of(plannedFile)),
            eq(Set.of("stats_only")),
            eq(Set.of(FloecatConnector.StatsTargetKind.FILE)),
            eq(true)))
        .thenReturn(
            FloecatConnector.FileGroupCaptureResult.of(
                List.of(fileRecord),
                List.of(
                    new FloecatConnector.ParquetPageIndexEntry(
                        plannedFile,
                        "stats_only",
                        0,
                        0,
                        0L,
                        1,
                        1,
                        16L,
                        32,
                        8L,
                        8,
                        true,
                        "INT64",
                        "ZSTD",
                        (short) 1,
                        (short) 0,
                        null,
                        null,
                        null),
                    new FloecatConnector.ParquetPageIndexEntry(
                        plannedFile,
                        "index_only",
                        0,
                        0,
                        0L,
                        1,
                        1,
                        16L,
                        32,
                        8L,
                        8,
                        true,
                        "INT64",
                        "ZSTD",
                        (short) 1,
                        (short) 0,
                        null,
                        null,
                        null))));

    CaptureEngineRequest request =
        new CaptureEngineRequest(
            SOURCE_CONNECTOR,
            "db",
            "events",
            tableId,
            55L,
            "plan-1",
            "group-1",
            List.of(plannedFile),
            Set.of("stats_only"),
            Set.of("index_only"),
            Set.of(FloecatConnector.StatsTargetKind.FILE),
            true,
            Optional.empty());

    var result = engine.capture(request);

    assertThat(result).isPresent();
    assertThat(result.get().statsRecords()).containsExactly(fileRecord);
    assertThat(result.get().pageIndexEntries())
        .extracting(FloecatConnector.ParquetPageIndexEntry::columnName)
        .containsExactly("index_only");
  }

  private static TargetStatsRecord fileRecordWithColumnNdv(
      ResourceId tableId, long snapshotId, String filePath, Ndv ndv) {
    return TargetStatsRecord.newBuilder()
        .setTableId(tableId)
        .setSnapshotId(snapshotId)
        .setTarget(
            StatsTarget.newBuilder().setFile(FileStatsTarget.newBuilder().setFilePath(filePath)))
        .setFile(
            FileTargetStats.newBuilder()
                .setTableId(tableId)
                .setSnapshotId(snapshotId)
                .setFilePath(filePath)
                .setFileFormat("PARQUET")
                .setRowCount(10L)
                .setSizeBytes(128L)
                .addColumns(
                    FileColumnStats.newBuilder()
                        .setColumnId(7L)
                        .setScalar(
                            ScalarStats.newBuilder()
                                .setDisplayName("id")
                                .setLogicalType("BIGINT")
                                .setValueCount(10L)
                                .setNdv(ndv))
                        .build())
                .build())
        .build();
  }

  private static Ndv ndvWithThetaSketch(long... values) {
    UpdateSketch sketch = UpdateSketch.builder().build();
    for (long value : values) {
      sketch.update(value);
    }

    ColumnNdv columnNdv = new ColumnNdv();
    columnNdv.mergeTheta(sketch.compact());
    columnNdv.finalizeTheta();

    Ndv.Builder builder = Ndv.newBuilder();
    if (columnNdv.approx != null) {
      builder
          .getApproxBuilder()
          .setEstimate(columnNdv.approx.estimate)
          .setMethod(columnNdv.approx.method == null ? "" : columnNdv.approx.method);
    }
    columnNdv.sketches.forEach(
        sketchModel ->
            builder
                .addSketchesBuilder()
                .setType(sketchModel.type == null ? "" : sketchModel.type)
                .setData(com.google.protobuf.ByteString.copyFrom(sketchModel.data))
                .setEncoding(sketchModel.encoding == null ? "" : sketchModel.encoding)
                .setCompression(sketchModel.compression == null ? "" : sketchModel.compression)
                .setVersion(sketchModel.version == null ? 0 : sketchModel.version));
    return builder.build();
  }

  @Test
  void captureIgnoresUnparseableEncodedBoundsWhenRollingUpColumns() {
    FloecatConnector connector = Mockito.mock(FloecatConnector.class);
    JavaConnectorCaptureEngine engine = new JavaConnectorCaptureEngine();
    engine.connectorOpener = ignored -> connector;

    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct").setId("table-1").build();
    String fileOne = "s3://bucket/path/file-1.parquet";
    String fileTwo = "s3://bucket/path/file-2.parquet";

    TargetStatsRecord fileRecordOne =
        TargetStatsRecord.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(55L)
            .setTarget(
                StatsTarget.newBuilder().setFile(FileStatsTarget.newBuilder().setFilePath(fileOne)))
            .setFile(
                FileTargetStats.newBuilder()
                    .setTableId(tableId)
                    .setSnapshotId(55L)
                    .setFilePath(fileOne)
                    .setFileFormat("PARQUET")
                    .setRowCount(10L)
                    .setSizeBytes(100L)
                    .addColumns(
                        FileColumnStats.newBuilder()
                            .setColumnId(1L)
                            .setScalar(
                                ScalarStats.newBuilder()
                                    .setDisplayName("id")
                                    .setLogicalType("INT")
                                    .setValueCount(10L)
                                    .setNullCount(0L)
                                    .setMin("f1_min_1")
                                    .setMax("f1_max_1"))
                            .build())
                    .build())
            .build();

    TargetStatsRecord fileRecordTwo =
        TargetStatsRecord.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(55L)
            .setTarget(
                StatsTarget.newBuilder().setFile(FileStatsTarget.newBuilder().setFilePath(fileTwo)))
            .setFile(
                FileTargetStats.newBuilder()
                    .setTableId(tableId)
                    .setSnapshotId(55L)
                    .setFilePath(fileTwo)
                    .setFileFormat("PARQUET")
                    .setRowCount(20L)
                    .setSizeBytes(200L)
                    .addColumns(
                        FileColumnStats.newBuilder()
                            .setColumnId(1L)
                            .setScalar(
                                ScalarStats.newBuilder()
                                    .setDisplayName("id")
                                    .setLogicalType("INT")
                                    .setValueCount(20L)
                                    .setNullCount(0L)
                                    .setMin("f2_min_1")
                                    .setMax("f2_max_1"))
                            .build())
                    .build())
            .build();

    when(connector.capturePlannedFileGroup(
            eq("db"),
            eq("events"),
            eq(tableId),
            eq(55L),
            eq(Set.of(fileOne, fileTwo)),
            eq(Set.of()),
            eq(
                Set.of(
                    FloecatConnector.StatsTargetKind.TABLE,
                    FloecatConnector.StatsTargetKind.COLUMN,
                    FloecatConnector.StatsTargetKind.FILE)),
            eq(false)))
        .thenReturn(
            FloecatConnector.FileGroupCaptureResult.of(
                List.of(fileRecordOne, fileRecordTwo), List.of()));

    CaptureEngineRequest request =
        new CaptureEngineRequest(
            SOURCE_CONNECTOR,
            "db",
            "events",
            tableId,
            55L,
            "plan-1",
            "group-1",
            List.of(fileOne, fileTwo),
            Set.of(),
            Set.of(),
            Set.of(
                FloecatConnector.StatsTargetKind.TABLE,
                FloecatConnector.StatsTargetKind.COLUMN,
                FloecatConnector.StatsTargetKind.FILE),
            false,
            Optional.empty());

    var result = engine.capture(request);

    assertThat(result).isPresent();
    assertThat(result.get().statsRecords())
        .filteredOn(record -> record.hasScalar() && record.getTarget().hasColumn())
        .singleElement()
        .extracting(record -> record.getScalar().getValueCount())
        .isEqualTo(30L);
  }
}
