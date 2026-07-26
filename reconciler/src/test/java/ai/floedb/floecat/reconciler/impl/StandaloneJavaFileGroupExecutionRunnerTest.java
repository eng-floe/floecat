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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.FileStatsTarget;
import ai.floedb.floecat.catalog.rpc.FileTargetStats;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.spi.capture.CaptureEngineRegistry;
import ai.floedb.floecat.reconciler.spi.capture.CaptureEngineRequest;
import ai.floedb.floecat.reconciler.spi.capture.CaptureEngineResult;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class StandaloneJavaFileGroupExecutionRunnerTest {

  @Test
  void executePassesWorkerAuthorizationToCaptureEngine() {
    var runner = new StandaloneJavaFileGroupExecutionRunner();
    runner.captureEngineRegistry = mock(CaptureEngineRegistry.class);
    runner.reconcileWorkerAuthProvider = ignored -> Optional.of("Bearer worker-token");
    when(runner.captureEngineRegistry.capture(any(), any()))
        .thenReturn(CaptureEngineResult.empty());
    BooleanSupplier shouldStop = () -> false;

    runner.execute(payload(), shouldStop, ignored -> {});

    ArgumentCaptor<CaptureEngineRequest> request =
        ArgumentCaptor.forClass(CaptureEngineRequest.class);
    org.mockito.Mockito.verify(runner.captureEngineRegistry).capture(request.capture(), any());
    assertThat(request.getValue().authorizationToken()).contains("Bearer worker-token");
    assertThat(request.getValue().storageLocation()).contains("s3://bucket/path");
    assertThat(request.getValue().shouldStop()).isSameAs(shouldStop);
  }

  @Test
  void executeAllowsMissingWorkerAuthorization() {
    var runner = new StandaloneJavaFileGroupExecutionRunner();
    runner.captureEngineRegistry = mock(CaptureEngineRegistry.class);
    runner.reconcileWorkerAuthProvider = ignored -> Optional.empty();
    when(runner.captureEngineRegistry.capture(any(), any()))
        .thenReturn(CaptureEngineResult.empty());

    runner.execute(payload(), () -> false, ignored -> {});

    ArgumentCaptor<CaptureEngineRequest> request =
        ArgumentCaptor.forClass(CaptureEngineRequest.class);
    org.mockito.Mockito.verify(runner.captureEngineRegistry).capture(request.capture(), any());
    assertThat(request.getValue().authorizationToken()).isEmpty();
  }

  @Test
  void executeDoesNotLeakWorkerAuthorizationAcrossAccounts() {
    var runner = new StandaloneJavaFileGroupExecutionRunner();
    runner.captureEngineRegistry = mock(CaptureEngineRegistry.class);
    runner.reconcileWorkerAuthProvider =
        accountId -> Optional.of("Bearer worker-token-" + accountId);
    when(runner.captureEngineRegistry.capture(any(), any()))
        .thenReturn(CaptureEngineResult.empty());

    runner.execute(payload("acct-a"), () -> false, ignored -> {});
    runner.execute(payload("acct-b"), () -> false, ignored -> {});

    ArgumentCaptor<CaptureEngineRequest> request =
        ArgumentCaptor.forClass(CaptureEngineRequest.class);
    org.mockito.Mockito.verify(runner.captureEngineRegistry, org.mockito.Mockito.times(2))
        .capture(request.capture(), any());
    assertThat(request.getAllValues())
        .extracting(CaptureEngineRequest::authorizationToken)
        .containsExactly(
            Optional.of("Bearer worker-token-acct-a"), Optional.of("Bearer worker-token-acct-b"));
  }

  @Test
  void executeRequestsFileStatsForTableOnlyCapturePolicy() {
    var runner = new StandaloneJavaFileGroupExecutionRunner();
    runner.captureEngineRegistry = mock(CaptureEngineRegistry.class);
    runner.reconcileWorkerAuthProvider = ignored -> Optional.empty();
    when(runner.captureEngineRegistry.capture(any(), any()))
        .thenReturn(CaptureEngineResult.empty());

    runner.execute(payload(), () -> false, ignored -> {});

    ArgumentCaptor<CaptureEngineRequest> request =
        ArgumentCaptor.forClass(CaptureEngineRequest.class);
    org.mockito.Mockito.verify(runner.captureEngineRegistry).capture(request.capture(), any());
    assertThat(request.getValue().requestedStatsTargetKinds())
        .containsExactlyInAnyOrder(
            ai.floedb.floecat.connector.spi.FloecatConnector.StatsTargetKind.TABLE,
            ai.floedb.floecat.connector.spi.FloecatConnector.StatsTargetKind.FILE);
  }

  @Test
  void executePublishesCompletedFileStatsThroughCallerSink() {
    var runner = new StandaloneJavaFileGroupExecutionRunner();
    runner.captureEngineRegistry = mock(CaptureEngineRegistry.class);
    runner.reconcileWorkerAuthProvider = ignored -> Optional.empty();
    TargetStatsRecord fileStats =
        TargetStatsRecord.newBuilder()
            .setTarget(
                StatsTarget.newBuilder()
                    .setFile(
                        FileStatsTarget.newBuilder().setFilePath("s3://bucket/path/file.parquet")))
            .setFile(FileTargetStats.newBuilder().setFilePath("s3://bucket/path/file.parquet"))
            .build();
    when(runner.captureEngineRegistry.capture(any(), any()))
        .thenAnswer(
            invocation -> {
              ai.floedb.floecat.reconciler.spi.capture.CaptureFileResultConsumer consumer =
                  invocation.getArgument(1);
              consumer.accept(List.of(fileStats), List.of());
              return CaptureEngineResult.empty();
            });
    List<TargetStatsRecord> published = new java.util.ArrayList<>();

    CaptureEngineResult result = runner.execute(payload(), () -> false, published::add);

    assertThat(published).containsExactly(fileStats);
    assertThat(result.statsRecords()).isEmpty();
  }

  @Test
  void executeStagesIndexesOnlyForFilesInTheLeasedGroup() {
    var runner = new StandaloneJavaFileGroupExecutionRunner();
    runner.captureEngineRegistry = mock(CaptureEngineRegistry.class);
    runner.reconcileWorkerAuthProvider = ignored -> Optional.empty();
    String plannedFile = "s3://bucket/path/file.parquet";
    String associatedDeleteFile = "s3://bucket/path/delete.parquet";
    TargetStatsRecord plannedStats = fileStats(plannedFile);
    TargetStatsRecord deleteStats = fileStats(associatedDeleteFile);
    when(runner.captureEngineRegistry.capture(any(), any()))
        .thenAnswer(
            invocation -> {
              ai.floedb.floecat.reconciler.spi.capture.CaptureFileResultConsumer consumer =
                  invocation.getArgument(1);
              consumer.accept(
                  List.of(plannedStats, deleteStats),
                  List.of(pageIndexEntry(plannedFile), pageIndexEntry(associatedDeleteFile)));
              return CaptureEngineResult.empty();
            });

    CaptureEngineResult result = runner.execute(indexPayload(), () -> false, ignored -> {});

    assertThat(result.stagedIndexArtifacts())
        .extracting(artifact -> artifact.record().getTarget().getFile().getFilePath())
        .containsExactly(plannedFile);
  }

  private static StandaloneFileGroupExecutionPayload payload() {
    return payload("acct");
  }

  private static StandaloneFileGroupExecutionPayload payload(String accountId) {
    return new StandaloneFileGroupExecutionPayload(
        "job-1",
        "lease-1",
        "parent-1",
        Connector.newBuilder().setKind(ConnectorKind.CK_ICEBERG).build(),
        "ns",
        "table",
        "s3://bucket/path",
        ResourceId.newBuilder()
            .setAccountId(accountId)
            .setKind(ResourceKind.RK_TABLE)
            .setId("table-id")
            .build(),
        1L,
        "plan-1",
        "group-1",
        "/result.pb",
        "/stats.pb",
        List.of("s3://bucket/path/file.parquet"),
        "",
        List.of(),
        ReconcileCapturePolicy.of(List.of(), Set.of(ReconcileCapturePolicy.Output.TABLE_STATS)));
  }

  private static StandaloneFileGroupExecutionPayload indexPayload() {
    StandaloneFileGroupExecutionPayload base = payload();
    return new StandaloneFileGroupExecutionPayload(
        base.jobId(),
        base.leaseEpoch(),
        base.parentJobId(),
        base.sourceConnector(),
        base.sourceNamespace(),
        base.sourceTable(),
        base.storageLocation(),
        base.tableId(),
        base.snapshotId(),
        base.planId(),
        base.groupId(),
        base.resultPayloadUri(),
        base.statsObjectPrefix(),
        base.plannedFilePaths(),
        base.executionSchemaJson(),
        base.fileExecutionPlans(),
        ReconcileCapturePolicy.of(
            List.of(),
            Set.of(
                ReconcileCapturePolicy.Output.FILE_STATS,
                ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX)));
  }

  private static TargetStatsRecord fileStats(String filePath) {
    return TargetStatsRecord.newBuilder()
        .setTarget(
            StatsTarget.newBuilder().setFile(FileStatsTarget.newBuilder().setFilePath(filePath)))
        .setFile(
            FileTargetStats.newBuilder()
                .setFilePath(filePath)
                .setFileFormat("PARQUET")
                .setRowCount(1L)
                .setSizeBytes(1L))
        .build();
  }

  private static FloecatConnector.ParquetPageIndexEntry pageIndexEntry(String filePath) {
    return new FloecatConnector.ParquetPageIndexEntry(
        filePath, "id", 0, 0, 0L, 1, 1, 16L, 32, 8L, 8, true, "INT64", "ZSTD", (short) 1, (short) 0,
        null, null, null);
  }
}
