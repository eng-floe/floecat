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
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.spi.ReconcileExecutor;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class FileGroupExecutionReconcileExecutorTest {

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
                    CaptureMode.METADATA_AND_STATS,
                    0L,
                    0L,
                    ReconcileScope.empty(),
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
    when(backend.capturePlannedFileGroupStats(
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.eq(55L),
            org.mockito.ArgumentMatchers.eq(List.of("s3://bucket/path/file-1.parquet"))))
        .thenReturn(
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
                    .build()));
    when(backend.capturePlannedFileGroupPageIndexEntries(
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.eq(55L),
            org.mockito.ArgumentMatchers.eq(List.of("s3://bucket/path/file-1.parquet"))))
        .thenReturn(
            List.of(
                new ai.floedb.floecat.connector.spi.FloecatConnector.ParquetPageIndexEntry(
                    "s3://bucket/path/file-1.parquet",
                    "id",
                    0,
                    0,
                    0L,
                    123,
                    123,
                    128L,
                    512,
                    64L,
                    64,
                    true,
                    "INT64",
                    "ZSTD",
                    (short) 1,
                    (short) 0,
                    null,
                    null,
                    null)));
    when(backend.materializePlannedFileGroupIndexArtifacts(
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.eq(55L),
            org.mockito.ArgumentMatchers.eq(List.of("s3://bucket/path/file-1.parquet")),
            org.mockito.ArgumentMatchers.anyList(),
            org.mockito.ArgumentMatchers.anyList()))
        .thenReturn(
            List.of(
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
                    .build()));
    var executor = new FileGroupExecutionReconcileExecutor(jobs, backend, true);
    var lease =
        new ReconcileJobStore.LeasedJob(
            "job-1",
            "acct",
            "connector-1",
            false,
            CaptureMode.METADATA_AND_STATS,
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
        .materializePlannedFileGroupIndexArtifacts(
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.eq(55L),
            org.mockito.ArgumentMatchers.eq(List.of("s3://bucket/path/file-1.parquet")),
            org.mockito.ArgumentMatchers.argThat(
                stats ->
                    stats != null
                        && stats.size() == 1
                        && ((TargetStatsRecord) stats.getFirst()).getFile().getRowCount() == 123L),
            org.mockito.ArgumentMatchers.argThat(
                entries ->
                    entries != null
                        && entries.size() == 1
                        && ((ai.floedb.floecat.connector.spi.FloecatConnector.ParquetPageIndexEntry)
                                    entries.getFirst())
                                .pageTotalCompressedSize()
                            == 512));
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
            CaptureMode.METADATA_AND_STATS,
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
            CaptureMode.METADATA_AND_STATS,
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
}
