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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.FileContent;
import ai.floedb.floecat.catalog.rpc.FileTargetStats;
import ai.floedb.floecat.catalog.rpc.IndexArtifactRecord;
import ai.floedb.floecat.catalog.rpc.IndexArtifactState;
import ai.floedb.floecat.catalog.rpc.IndexFileTarget;
import ai.floedb.floecat.catalog.rpc.IndexTarget;
import ai.floedb.floecat.catalog.rpc.ScalarStats;
import ai.floedb.floecat.catalog.rpc.TableValueStats;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.spi.ReconcileExecutor;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend;
import ai.floedb.floecat.reconciler.spi.capture.CaptureEngineResult;
import ai.floedb.floecat.stats.identity.TargetStatsRecords;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

class FileGroupExecutionReconcileExecutorTest {

  private static final ResourceId TABLE_ID =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setKind(ResourceKind.RK_TABLE)
          .setId("table-1")
          .build();

  @Test
  void executeResolvesPlannedGroupFromParentSnapshotPlan() {
    var jobs = mock(ReconcileJobStore.class);
    var backend = mock(ReconcilerBackend.class);
    when(jobs.get("acct", "parent-1"))
        .thenReturn(
            Optional.of(
                new ReconcileJobStore.ReconcileJob(
                    "parent-1",
                    "acct",
                    "connector-1",
                    "JS_SUCCEEDED",
                    "",
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    false,
                    CaptureMode.METADATA_AND_CAPTURE,
                    0L,
                    0L,
                    defaultCaptureScope(),
                    ReconcileExecutionPolicy.defaults(),
                    "",
                    ReconcileJobKind.PLAN_SNAPSHOT,
                    ReconcileTableTask.empty(),
                    ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
                    ReconcileSnapshotTask.of(
                        "table-1",
                        55L,
                        "db",
                        "events",
                        List.of(
                            ReconcileFileGroupTask.of(
                                "plan-1",
                                "group-1",
                                "table-1",
                                55L,
                                List.of("s3://bucket/path/file-1.parquet")))),
                    ReconcileFileGroupTask.empty(),
                    "")));
    when(backend.capturePlannedFileGroup(
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.argThat(
                request ->
                    request != null
                        && request.snapshotId() == 55L
                        && request.capturePageIndex()
                        && request
                            .requestedStatsTargetKinds()
                            .contains(
                                ai.floedb.floecat.connector.spi.FloecatConnector.StatsTargetKind
                                    .FILE)
                        && request
                            .plannedFilePaths()
                            .equals(List.of("s3://bucket/path/file-1.parquet")))))
        .thenReturn(
            CaptureEngineResult.of(
                List.of(
                    TargetStatsRecord.newBuilder()
                        .setFile(
                            FileTargetStats.newBuilder()
                                .setFilePath("s3://bucket/path/file-1.parquet")
                                .setFileFormat("parquet")
                                .setRowCount(123L)
                                .setSizeBytes(4096L)
                                .setFileContent(FileContent.FC_DATA)
                                .setSequenceNumber(77L))
                        .build()),
                List.of(),
                List.of(
                    new ReconcilerBackend.StagedIndexArtifact(
                        IndexArtifactRecord.newBuilder()
                            .setTableId(
                                ai.floedb.floecat.common.rpc.ResourceId.newBuilder()
                                    .setAccountId("acct")
                                    .setKind(ai.floedb.floecat.common.rpc.ResourceKind.RK_TABLE)
                                    .setId("table-1")
                                    .build())
                            .setSnapshotId(55L)
                            .setTarget(
                                IndexTarget.newBuilder()
                                    .setFile(
                                        IndexFileTarget.newBuilder()
                                            .setFilePath("s3://bucket/path/file-1.parquet")
                                            .build())
                                    .build())
                            .setArtifactUri(
                                "/accounts/acct/tables/table-1/index-sidecars/0000000000000000055/file%3As3%3A%2F%2Fbucket%2Fpath%2Ffile-1.parquet/abc.parquet")
                            .setArtifactFormat("parquet")
                            .setArtifactFormatVersion(1)
                            .setState(IndexArtifactState.IAS_READY)
                            .build(),
                        new byte[] {1, 2, 3},
                        "application/x-parquet"))));
    var executor = new FileGroupExecutionReconcileExecutor(jobs, backend, true);
    var lease =
        new ReconcileJobStore.LeasedJob(
            "job-1",
            "acct",
            "connector-1",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            defaultCaptureScope(),
            ReconcileExecutionPolicy.defaults(),
            "lease-1",
            "",
            "",
            ReconcileJobKind.EXEC_FILE_GROUP,
            ReconcileTableTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask.empty(),
            ReconcileFileGroupTask.of("plan-1", "group-1", "table-1", 55L, List.of()),
            "parent-1");

    var result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease,
                () -> false,
                (tablesScanned,
                    tablesChanged,
                    viewsScanned,
                    viewsChanged,
                    errors,
                    snapshotsProcessed,
                    statsProcessed,
                    message) -> {}));

    assertThat(result.ok()).isTrue();
    assertThat(result.message).contains("Executed file group group-1");
    assertThat(result.statsProcessed).isEqualTo(1);
    verify(jobs)
        .persistFileGroupResult(
            org.mockito.ArgumentMatchers.eq("job-1"),
            org.mockito.ArgumentMatchers.argThat(
                task ->
                    task != null
                        && task.fileResults().size() == 1
                        && task.fileResults().getFirst().statsProcessed() == 1L
                        && task.fileResults()
                            .getFirst()
                            .indexArtifact()
                            .artifactUri()
                            .equals(
                                "/accounts/acct/tables/table-1/index-sidecars/0000000000000000055/file%3As3%3A%2F%2Fbucket%2Fpath%2Ffile-1.parquet/abc.parquet")));
    verify(backend)
        .putTargetStats(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyList());
    verify(backend)
        .putIndexArtifacts(
            org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyList());
    verify(backend)
        .capturePlannedFileGroup(
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.argThat(
                request ->
                    request != null
                        && request.snapshotId() == 55L
                        && request.capturePageIndex()
                        && request
                            .requestedStatsTargetKinds()
                            .contains(
                                ai.floedb.floecat.connector.spi.FloecatConnector.StatsTargetKind
                                    .FILE)));
  }

  @Test
  void executePublishesTableColumnAndFileStatsAlongsideIndexArtifacts() {
    var jobs = mock(ReconcileJobStore.class);
    var backend = mock(ReconcilerBackend.class);
    TargetStatsRecord tableRecord =
        TargetStatsRecords.tableRecord(
            TABLE_ID,
            55L,
            TableValueStats.newBuilder().setRowCount(123L).setDataFileCount(1L).build(),
            null);
    TargetStatsRecord columnRecord =
        TargetStatsRecords.columnRecord(
            TABLE_ID,
            55L,
            7L,
            ScalarStats.newBuilder()
                .setDisplayName("id")
                .setLogicalType("BIGINT")
                .setValueCount(123L)
                .setNullCount(0L)
                .build(),
            null);
    TargetStatsRecord fileRecord =
        TargetStatsRecords.fileRecord(
            TABLE_ID,
            55L,
            FileTargetStats.newBuilder()
                .setFilePath("s3://bucket/path/file-1.parquet")
                .setFileFormat("parquet")
                .setRowCount(123L)
                .setSizeBytes(4096L)
                .setFileContent(FileContent.FC_DATA)
                .build());

    when(backend.capturePlannedFileGroup(
            org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any()))
        .thenReturn(
            CaptureEngineResult.of(
                List.of(tableRecord, columnRecord, fileRecord),
                List.of(),
                List.of(
                    new ReconcilerBackend.StagedIndexArtifact(
                        IndexArtifactRecord.newBuilder()
                            .setTableId(TABLE_ID)
                            .setSnapshotId(55L)
                            .setTarget(
                                IndexTarget.newBuilder()
                                    .setFile(
                                        IndexFileTarget.newBuilder()
                                            .setFilePath("s3://bucket/path/file-1.parquet")
                                            .build())
                                    .build())
                            .setArtifactUri("s3://artifacts/file-1.parquet.idx")
                            .setArtifactFormat("parquet")
                            .setArtifactFormatVersion(1)
                            .setState(IndexArtifactState.IAS_READY)
                            .build(),
                        new byte[] {1, 2, 3},
                        "application/x-parquet"))));
    var executor = new FileGroupExecutionReconcileExecutor(jobs, backend, true);
    var lease = simpleFileGroupLease();

    var result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease,
                () -> false,
                (tablesScanned,
                    tablesChanged,
                    viewsScanned,
                    viewsChanged,
                    errors,
                    snapshotsProcessed,
                    statsProcessed,
                    message) -> {}));

    assertThat(result.ok()).isTrue();
    assertThat(result.statsProcessed).isEqualTo(3);
    verify(backend)
        .putTargetStats(
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.argThat(
                stats ->
                    stats != null
                        && stats.size() == 3
                        && ((List<TargetStatsRecord>) stats)
                            .stream().anyMatch(TargetStatsRecord::hasTable)
                        && ((List<TargetStatsRecord>) stats)
                            .stream().anyMatch(TargetStatsRecord::hasScalar)
                        && ((List<TargetStatsRecord>) stats)
                            .stream().anyMatch(TargetStatsRecord::hasFile)
                        && ((List<TargetStatsRecord>) stats)
                            .stream()
                                .filter(TargetStatsRecord::hasFile)
                                .findFirst()
                                .map(record -> record.getFile().getFilePath())
                                .orElse("")
                                .equals("s3://bucket/path/file-1.parquet")));
    verify(backend)
        .putIndexArtifacts(
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.argThat(
                artifacts ->
                    artifacts != null
                        && artifacts.size() == 1
                        && ((List<ReconcilerBackend.StagedIndexArtifact>) artifacts)
                            .getFirst()
                            .record()
                            .getTarget()
                            .getFile()
                            .getFilePath()
                            .equals("s3://bucket/path/file-1.parquet")
                        && ((List<ReconcilerBackend.StagedIndexArtifact>) artifacts)
                            .getFirst()
                            .record()
                            .getArtifactFormat()
                            .equals("parquet")));
  }

  @Test
  void executeFailsWhenIndexCaptureReturnsNoArtifacts() {
    var jobs = mock(ReconcileJobStore.class);
    var backend = mock(ReconcilerBackend.class);
    when(backend.capturePlannedFileGroup(
            org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any()))
        .thenReturn(
            CaptureEngineResult.of(
                List.of(
                    TargetStatsRecord.newBuilder()
                        .setFile(
                            FileTargetStats.newBuilder()
                                .setFilePath("s3://bucket/path/file-1.parquet")
                                .setRowCount(4L)
                                .build())
                        .build()),
                List.of(),
                List.of()));
    var executor = new FileGroupExecutionReconcileExecutor(jobs, backend, true);
    var lease = simpleFileGroupLease();

    var result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease,
                () -> false,
                (tablesScanned,
                    tablesChanged,
                    viewsScanned,
                    viewsChanged,
                    errors,
                    snapshotsProcessed,
                    statsProcessed,
                    message) -> {}));

    assertThat(result.ok()).isFalse();
    verify(backend)
        .capturePlannedFileGroup(
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.argThat(
                request ->
                    request != null
                        && request.capturePageIndex()
                        && request
                            .requestedStatsTargetKinds()
                            .equals(
                                Set.of(
                                    ai.floedb.floecat.connector.spi.FloecatConnector.StatsTargetKind
                                        .TABLE,
                                    ai.floedb.floecat.connector.spi.FloecatConnector.StatsTargetKind
                                        .COLUMN,
                                    ai.floedb.floecat.connector.spi.FloecatConnector.StatsTargetKind
                                        .FILE))));
    verify(backend, never())
        .putTargetStats(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyList());
    verify(backend, never())
        .putIndexArtifacts(
            org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyList());
  }

  @Test
  void executeHonorsStatsOnlyCapturePolicy() {
    var jobs = mock(ReconcileJobStore.class);
    var backend = mock(ReconcilerBackend.class);
    when(backend.capturePlannedFileGroup(
            org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any()))
        .thenReturn(
            CaptureEngineResult.of(
                List.of(
                    TargetStatsRecord.newBuilder()
                        .setFile(
                            FileTargetStats.newBuilder()
                                .setFilePath("s3://bucket/path/file-1.parquet")
                                .setRowCount(4L)
                                .build())
                        .build()),
                List.of(),
                List.of()));
    var executor = new FileGroupExecutionReconcileExecutor(jobs, backend, true);
    var lease =
        simpleFileGroupLease(
            ReconcileScope.of(
                List.of(),
                "table-1",
                List.of(),
                ReconcileCapturePolicy.of(
                    List.of(new ReconcileCapturePolicy.Column("id", true, false)),
                    Set.of(
                        ReconcileCapturePolicy.Output.TABLE_STATS,
                        ReconcileCapturePolicy.Output.FILE_STATS,
                        ReconcileCapturePolicy.Output.COLUMN_STATS))));

    var result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease,
                () -> false,
                (tablesScanned,
                    tablesChanged,
                    viewsScanned,
                    viewsChanged,
                    errors,
                    snapshotsProcessed,
                    statsProcessed,
                    message) -> {}));

    assertThat(result.ok()).isTrue();
    verify(backend)
        .capturePlannedFileGroup(
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.argThat(
                request ->
                    request != null
                        && !request.capturePageIndex()
                        && request.statsColumns().equals(Set.of("id"))
                        && request.indexColumns().isEmpty()
                        && request
                            .requestedStatsTargetKinds()
                            .equals(
                                Set.of(
                                    ai.floedb.floecat.connector.spi.FloecatConnector.StatsTargetKind
                                        .TABLE,
                                    ai.floedb.floecat.connector.spi.FloecatConnector.StatsTargetKind
                                        .COLUMN,
                                    ai.floedb.floecat.connector.spi.FloecatConnector.StatsTargetKind
                                        .FILE))));
    verify(backend)
        .putTargetStats(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyList());
    verify(backend, never())
        .putIndexArtifacts(
            org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyList());
    verify(backend, never())
        .putIndexArtifacts(
            org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyList());
  }

  @Test
  void executePublishesPreStagedIndexArtifacts() {
    var jobs = mock(ReconcileJobStore.class);
    var backend = mock(ReconcilerBackend.class);
    when(backend.capturePlannedFileGroup(
            org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any()))
        .thenReturn(
            CaptureEngineResult.of(
                List.of(),
                List.of(),
                List.of(
                    new ReconcilerBackend.StagedIndexArtifact(
                        IndexArtifactRecord.newBuilder()
                            .setTarget(
                                IndexTarget.newBuilder()
                                    .setFile(
                                        IndexFileTarget.newBuilder()
                                            .setFilePath("s3://bucket/path/file-1.parquet")
                                            .build())
                                    .build())
                            .setArtifactUri("s3://artifacts/file-1.parquet.idx")
                            .setArtifactFormat("parquet")
                            .setArtifactFormatVersion(1)
                            .setState(IndexArtifactState.IAS_READY)
                            .build(),
                        new byte[] {1},
                        "application/x-parquet"))));
    var executor = new FileGroupExecutionReconcileExecutor(jobs, backend, true);
    var lease = simpleFileGroupLease();

    var result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease,
                () -> false,
                (tablesScanned,
                    tablesChanged,
                    viewsScanned,
                    viewsChanged,
                    errors,
                    snapshotsProcessed,
                    statsProcessed,
                    message) -> {}));

    assertThat(result.ok()).isTrue();
    assertThat(result.statsProcessed).isZero();
    verify(backend)
        .putIndexArtifacts(
            org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyList());
  }

  @Test
  void executeHonorsIndexOnlyCapturePolicy() {
    var jobs = mock(ReconcileJobStore.class);
    var backend = mock(ReconcilerBackend.class);
    when(backend.capturePlannedFileGroup(
            org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any()))
        .thenReturn(
            CaptureEngineResult.of(
                List.of(),
                List.of(),
                List.of(
                    new ReconcilerBackend.StagedIndexArtifact(
                        IndexArtifactRecord.newBuilder()
                            .setTarget(
                                IndexTarget.newBuilder()
                                    .setFile(
                                        IndexFileTarget.newBuilder()
                                            .setFilePath("s3://bucket/path/file-1.parquet")
                                            .build())
                                    .build())
                            .setArtifactUri("s3://artifacts/file-1.parquet.idx")
                            .setArtifactFormat("parquet")
                            .setArtifactFormatVersion(1)
                            .setState(IndexArtifactState.IAS_READY)
                            .build(),
                        new byte[] {1},
                        "application/x-parquet"))));
    var executor = new FileGroupExecutionReconcileExecutor(jobs, backend, true);
    var lease =
        simpleFileGroupLease(
            ReconcileScope.of(
                List.of(),
                "table-1",
                List.of(),
                ReconcileCapturePolicy.of(
                    List.of(new ReconcileCapturePolicy.Column("id", false, true)),
                    Set.of(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX))));

    var result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease,
                () -> false,
                (tablesScanned,
                    tablesChanged,
                    viewsScanned,
                    viewsChanged,
                    errors,
                    snapshotsProcessed,
                    statsProcessed,
                    message) -> {}));

    assertThat(result.ok()).isTrue();
    verify(backend)
        .capturePlannedFileGroup(
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.argThat(
                request ->
                    request != null
                        && request.requestedStatsTargetKinds().isEmpty()
                        && request.capturePageIndex()
                        && request.statsColumns().isEmpty()
                        && request.indexColumns().equals(Set.of("id"))));
    verify(backend, never())
        .putTargetStats(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyList());
    verify(backend)
        .putIndexArtifacts(
            org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyList());
  }

  @Test
  void executeFailsWhenFileGroupTaskMissing() {
    var jobs = mock(ReconcileJobStore.class);
    var backend = mock(ReconcilerBackend.class);
    var executor = new FileGroupExecutionReconcileExecutor(jobs, backend, true);
    var lease =
        new ReconcileJobStore.LeasedJob(
            "job-1",
            "acct",
            "connector-1",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileExecutionPolicy.defaults(),
            "lease-1",
            "",
            "",
            ReconcileJobKind.EXEC_FILE_GROUP,
            ReconcileTableTask.empty(),
            "");

    var result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease,
                () -> false,
                (tablesScanned,
                    tablesChanged,
                    viewsScanned,
                    viewsChanged,
                    errors,
                    snapshotsProcessed,
                    statsProcessed,
                    message) -> {}));

    assertThat(result.ok()).isFalse();
    assertThat(result.message).contains("file group reference is required");
    verify(jobs, never())
        .persistFileGroupResult(
            org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.any());
  }

  @Test
  void executeFailsWhenParentSnapshotPlanCannotBeResolved() {
    var jobs = mock(ReconcileJobStore.class);
    var backend = mock(ReconcilerBackend.class);
    when(jobs.get("acct", "parent-1")).thenReturn(Optional.empty());
    var executor = new FileGroupExecutionReconcileExecutor(jobs, backend, true);
    var lease =
        new ReconcileJobStore.LeasedJob(
            "job-1",
            "acct",
            "connector-1",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileExecutionPolicy.defaults(),
            "lease-1",
            "",
            "",
            ReconcileJobKind.EXEC_FILE_GROUP,
            ReconcileTableTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask.empty(),
            ReconcileFileGroupTask.of("plan-1", "group-1", "table-1", 55L, List.of()),
            "parent-1");

    var result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease,
                () -> false,
                (tablesScanned,
                    tablesChanged,
                    viewsScanned,
                    viewsChanged,
                    errors,
                    snapshotsProcessed,
                    statsProcessed,
                    message) -> {}));

    assertThat(result.ok()).isFalse();
    assertThat(result.message).contains("planned file group could not be resolved");
    verify(jobs, never())
        .persistFileGroupResult(
            org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.any());
  }

  @Test
  void executeFailsWhenCapturePolicyIsMissingForCaptureMode() {
    var jobs = mock(ReconcileJobStore.class);
    var backend = mock(ReconcilerBackend.class);
    var executor = new FileGroupExecutionReconcileExecutor(jobs, backend, true);
    var lease =
        new ReconcileJobStore.LeasedJob(
            "job-1",
            "acct",
            "connector-1",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileExecutionPolicy.defaults(),
            "lease-1",
            "",
            "",
            ReconcileJobKind.EXEC_FILE_GROUP,
            ReconcileTableTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask.empty(),
            ReconcileFileGroupTask.of(
                "plan-1", "group-1", "table-1", 55L, List.of("s3://bucket/path/file-1.parquet")),
            "");

    var result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease,
                () -> false,
                (tablesScanned,
                    tablesChanged,
                    viewsScanned,
                    viewsChanged,
                    errors,
                    snapshotsProcessed,
                    statsProcessed,
                    message) -> {}));

    assertThat(result.ok()).isFalse();
    assertThat(result.message).contains("capture policy is required");
    verify(backend, never())
        .capturePlannedFileGroup(
            org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());
  }

  private static ReconcileJobStore.LeasedJob simpleFileGroupLease() {
    return simpleFileGroupLease(defaultCaptureScope());
  }

  private static ReconcileJobStore.LeasedJob simpleFileGroupLease(ReconcileScope scope) {
    return new ReconcileJobStore.LeasedJob(
        "job-1",
        "acct",
        "connector-1",
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        scope,
        ReconcileExecutionPolicy.defaults(),
        "lease-1",
        "",
        "",
        ReconcileJobKind.EXEC_FILE_GROUP,
        ReconcileTableTask.empty(),
        ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
        ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask.empty(),
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", "table-1", 55L, List.of("s3://bucket/path/file-1.parquet")),
        "");
  }

  private static ReconcileScope defaultCaptureScope() {
    return ReconcileScope.of(
        List.of(),
        "table-1",
        List.of(),
        ReconcileCapturePolicy.of(
            List.of(),
            Set.of(
                ReconcileCapturePolicy.Output.TABLE_STATS,
                ReconcileCapturePolicy.Output.FILE_STATS,
                ReconcileCapturePolicy.Output.COLUMN_STATS,
                ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX)));
  }
}
