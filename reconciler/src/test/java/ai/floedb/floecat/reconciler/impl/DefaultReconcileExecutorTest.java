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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import ai.floedb.floecat.reconciler.spi.ReconcileExecutor;
import java.util.List;
import org.junit.jupiter.api.Test;

class DefaultReconcileExecutorTest {

  @Test
  void executePreservesConnectorMissingFailureKind() {
    var reconcilerService = mock(ReconcilerService.class);
    var jobs = mock(ReconcileJobStore.class);
    var executorRegistry = mock(ReconcileExecutorRegistry.class);
    var executor = new DefaultReconcileExecutor(reconcilerService, jobs, executorRegistry, true);
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
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("sales", "orders", "orders-id", "orders"),
            "");
    var failure =
        new ReconcileFailureException(
            ReconcileExecutor.ExecutionResult.FailureKind.CONNECTOR_MISSING,
            "getConnector failed: connector-1",
            null);

    when(reconcilerService.reconcilePlannedTableExecution(
            any(), any(), anyBoolean(), any(), any(), any(), nullable(String.class), any(), any()))
        .thenReturn(new ReconcilerService.Result(0, 0, 1, 0, 0, failure));

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

    assertThat(result.failureKind)
        .isEqualTo(ReconcileExecutor.ExecutionResult.FailureKind.CONNECTOR_MISSING);
    assertThat(result.error).isSameAs(failure);
  }

  @Test
  void executeViewJobUsesReconcileViewPath() {
    var reconcilerService = mock(ReconcilerService.class);
    var jobs = mock(ReconcileJobStore.class);
    var executorRegistry = mock(ReconcileExecutorRegistry.class);
    var executor = new DefaultReconcileExecutor(reconcilerService, jobs, executorRegistry, true);
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
            ReconcileJobKind.PLAN_VIEW,
            ReconcileTableTask.empty(),
            ReconcileViewTask.of("sales", "orders_view", "dest-analytics-id", "orders-view-id"),
            "");

    when(reconcilerService.reconcileView(
            any(), any(), any(), any(), nullable(String.class), any(), any()))
        .thenReturn(new ReconcilerService.Result(1, 1, 0, 0, 0, null));

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
    assertThat(result.scanned).isEqualTo(1);
    assertThat(result.changed).isEqualTo(1);
    verify(jobs, never())
        .enqueueSnapshotPlan(any(), any(), anyBoolean(), any(), any(), any(), any(), any(), any());
  }

  @Test
  void executeTablePlanEnqueuesSnapshotPlansAndSuppressesParentSnapshotCounts() {
    var reconcilerService = mock(ReconcilerService.class);
    var jobs = mock(ReconcileJobStore.class);
    var executorRegistry = mock(ReconcileExecutorRegistry.class);
    when(executorRegistry.hasExecutorForJobKind(ReconcileJobKind.PLAN_SNAPSHOT)).thenReturn(true);
    var executor = new DefaultReconcileExecutor(reconcilerService, jobs, executorRegistry, true);
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
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.discovery("sales", "orders", "dest-ns", "", "orders"),
            "");

    when(reconcilerService.reconcilePlannedTableExecution(
            any(), any(), anyBoolean(), any(), any(), any(), nullable(String.class), any(), any()))
        .thenReturn(
            new ReconcilerService.Result(
                1, 1, 0, 0, 0, 0, 0, null, List.of(), List.of("orders-id")));
    when(reconcilerService.planSnapshotTasks(
            any(), any(), anyBoolean(), any(), any(), any(), nullable(String.class)))
        .thenReturn(List.of(ReconcileSnapshotTask.of("orders-id", 42L, "sales", "orders")));

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
    assertThat(result.tablesScanned).isEqualTo(1);
    assertThat(result.snapshotsProcessed).isZero();
    verify(jobs)
        .enqueueSnapshotPlan(
            org.mockito.ArgumentMatchers.eq("acct"),
            org.mockito.ArgumentMatchers.eq("connector-1"),
            anyBoolean(),
            org.mockito.ArgumentMatchers.eq(CaptureMode.METADATA_AND_CAPTURE),
            org.mockito.ArgumentMatchers.eq(ReconcileScope.empty()),
            org.mockito.ArgumentMatchers.eq(
                ReconcileSnapshotTask.of("orders-id", 42L, "sales", "orders")),
            org.mockito.ArgumentMatchers.eq(ReconcileExecutionPolicy.defaults()),
            org.mockito.ArgumentMatchers.eq("job-1"),
            org.mockito.ArgumentMatchers.eq(""));
  }

  @Test
  void executeCaptureOnlyTablePlanAlsoEnqueuesSnapshotPlans() {
    var reconcilerService = mock(ReconcilerService.class);
    var jobs = mock(ReconcileJobStore.class);
    var executorRegistry = mock(ReconcileExecutorRegistry.class);
    when(executorRegistry.hasExecutorForJobKind(ReconcileJobKind.PLAN_SNAPSHOT)).thenReturn(true);
    var executor = new DefaultReconcileExecutor(reconcilerService, jobs, executorRegistry, true);
    var lease =
        new ReconcileJobStore.LeasedJob(
            "job-2",
            "acct",
            "connector-1",
            false,
            CaptureMode.CAPTURE_ONLY,
            ReconcileScope.empty(),
            ReconcileExecutionPolicy.defaults(),
            "lease-2",
            "",
            "",
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("sales", "orders", "orders-id", "orders"),
            "");

    when(reconcilerService.reconcilePlannedTableExecution(
            any(), any(), anyBoolean(), any(), any(), any(), nullable(String.class), any(), any()))
        .thenReturn(new ReconcilerService.Result(1, 0, 0, 0, 0, 0, 0, null));
    when(reconcilerService.planSnapshotTasks(
            any(), any(), anyBoolean(), any(), any(), any(), nullable(String.class)))
        .thenReturn(List.of(ReconcileSnapshotTask.of("orders-id", 84L, "sales", "orders")));

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
    verify(jobs)
        .enqueueSnapshotPlan(
            org.mockito.ArgumentMatchers.eq("acct"),
            org.mockito.ArgumentMatchers.eq("connector-1"),
            anyBoolean(),
            org.mockito.ArgumentMatchers.eq(CaptureMode.CAPTURE_ONLY),
            org.mockito.ArgumentMatchers.eq(ReconcileScope.empty()),
            org.mockito.ArgumentMatchers.eq(
                ReconcileSnapshotTask.of("orders-id", 84L, "sales", "orders")),
            org.mockito.ArgumentMatchers.eq(ReconcileExecutionPolicy.defaults()),
            org.mockito.ArgumentMatchers.eq("job-2"),
            org.mockito.ArgumentMatchers.eq(""));
  }
}
