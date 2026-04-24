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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import ai.floedb.floecat.reconciler.spi.ReconcileExecutor;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class ConnectorPlanningReconcileExecutorTest {

  @Test
  void executePlansTableAndViewJobs() {
    var reconcilerService = mock(ReconcilerService.class);
    var jobs = mock(ReconcileJobStore.class);
    var executorRegistry = mock(ReconcileExecutorRegistry.class);
    when(executorRegistry.hasExecutorForJobKind(ReconcileJobKind.PLAN_TABLE)).thenReturn(true);
    when(executorRegistry.hasExecutorForJobKind(ReconcileJobKind.PLAN_VIEW)).thenReturn(true);
    var executor =
        new ConnectorPlanningReconcileExecutor(reconcilerService, jobs, executorRegistry, true);
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
            ReconcileJobKind.PLAN_CONNECTOR,
            ReconcileTableTask.empty(),
            "");

    when(reconcilerService.planTableTasks(any(), any(), any(), any()))
        .thenReturn(
            List.of(
                ReconcileTableTask.discovery("sales", "orders", "ns-1", "orders"),
                ReconcileTableTask.discovery("sales", "customers", "ns-1", "customers")));
    when(reconcilerService.planViewTasks(any(), any(), any(), any()))
        .thenReturn(
            List.of(ReconcileViewTask.discovery("sales", "orders_view", "ns-1", "orders_view")));
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
    assertThat(result.scanned).isEqualTo(3);
    verify(jobs)
        .enqueueTablePlan(
            eq("acct"),
            eq("connector-1"),
            anyBoolean(),
            eq(CaptureMode.METADATA_AND_STATS),
            argThat(
                scope -> scope != null && !scope.hasTableFilter() && !scope.hasNamespaceFilter()),
            eq(ReconcileTableTask.discovery("sales", "orders", "ns-1", "orders")),
            eq(ReconcileExecutionPolicy.defaults()),
            eq("job-1"),
            anyString());
    verify(jobs)
        .enqueueTablePlan(
            eq("acct"),
            eq("connector-1"),
            anyBoolean(),
            eq(CaptureMode.METADATA_AND_STATS),
            argThat(
                scope -> scope != null && !scope.hasTableFilter() && !scope.hasNamespaceFilter()),
            eq(ReconcileTableTask.discovery("sales", "customers", "ns-1", "customers")),
            eq(ReconcileExecutionPolicy.defaults()),
            eq("job-1"),
            anyString());
    verify(jobs)
        .enqueueViewPlan(
            eq("acct"),
            eq("connector-1"),
            anyBoolean(),
            eq(CaptureMode.METADATA_AND_STATS),
            eq(ReconcileScope.empty()),
            eq(ReconcileViewTask.discovery("sales", "orders_view", "ns-1", "orders_view")),
            eq(ReconcileExecutionPolicy.defaults()),
            eq("job-1"),
            anyString());
  }

  @Test
  void executePlansSnapshotJobsForExistingDestinationTables() {
    var reconcilerService = mock(ReconcilerService.class);
    var jobs = mock(ReconcileJobStore.class);
    var executorRegistry = mock(ReconcileExecutorRegistry.class);
    when(executorRegistry.hasExecutorForJobKind(ReconcileJobKind.PLAN_TABLE)).thenReturn(true);
    var executor =
        new ConnectorPlanningReconcileExecutor(reconcilerService, jobs, executorRegistry, true);
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
            ReconcileJobKind.PLAN_CONNECTOR,
            ReconcileTableTask.empty(),
            "");
    var task = ReconcileTableTask.discovery("sales", "orders", "ns-1", "orders-id", "orders");

    when(reconcilerService.planTableTasks(any(), any(), any(), any())).thenReturn(List.of(task));
    when(reconcilerService.planViewTasks(any(), any(), any(), any())).thenReturn(List.of());

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
        .enqueueTablePlan(
            eq("acct"),
            eq("connector-1"),
            anyBoolean(),
            eq(CaptureMode.METADATA_AND_STATS),
            eq(ReconcileScope.empty()),
            eq(task),
            eq(ReconcileExecutionPolicy.defaults()),
            eq("job-1"),
            anyString());
    verify(jobs, org.mockito.Mockito.never())
        .enqueueSnapshotPlan(any(), any(), anyBoolean(), any(), any(), any(), any(), any(), any());
  }

  @Test
  void executeDoesNotRequireSnapshotPlanningExecutor() {
    var reconcilerService = mock(ReconcilerService.class);
    var jobs = mock(ReconcileJobStore.class);
    var executorRegistry = mock(ReconcileExecutorRegistry.class);
    when(executorRegistry.hasExecutorForJobKind(ReconcileJobKind.PLAN_TABLE)).thenReturn(true);
    var executor =
        new ConnectorPlanningReconcileExecutor(reconcilerService, jobs, executorRegistry, true);
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
            ReconcileJobKind.PLAN_CONNECTOR,
            ReconcileTableTask.empty(),
            "");
    var task = ReconcileTableTask.discovery("sales", "orders", "ns-1", "orders-id", "orders");

    when(reconcilerService.planTableTasks(any(), any(), any(), any())).thenReturn(List.of(task));
    when(reconcilerService.planViewTasks(any(), any(), any(), any())).thenReturn(List.of());

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
        .enqueueTablePlan(
            eq("acct"),
            eq("connector-1"),
            anyBoolean(),
            eq(CaptureMode.METADATA_AND_STATS),
            eq(ReconcileScope.empty()),
            eq(task),
            eq(ReconcileExecutionPolicy.defaults()),
            eq("job-1"),
            anyString());
  }

  @Test
  void executeFailsWhenScopedPlanMatchesNoTables() {
    var reconcilerService = mock(ReconcilerService.class);
    var jobs = mock(ReconcileJobStore.class);
    var executorRegistry = mock(ReconcileExecutorRegistry.class);
    when(executorRegistry.hasExecutorForJobKind(ReconcileJobKind.PLAN_TABLE)).thenReturn(true);
    when(executorRegistry.hasExecutorForJobKind(ReconcileJobKind.PLAN_VIEW)).thenReturn(true);
    var executor =
        new ConnectorPlanningReconcileExecutor(reconcilerService, jobs, executorRegistry, true);
    var scope = ReconcileScope.of(java.util.List.of(), "missing-table-id");
    var lease =
        new ReconcileJobStore.LeasedJob(
            "job-1",
            "acct",
            "connector-1",
            false,
            CaptureMode.METADATA_AND_STATS,
            scope,
            ReconcileExecutionPolicy.defaults(),
            "lease-1",
            "",
            "",
            ReconcileJobKind.PLAN_CONNECTOR,
            ReconcileTableTask.empty(),
            "");

    when(reconcilerService.planTableTasks(any(), any(), eq(scope), any())).thenReturn(List.of());
    when(reconcilerService.planViewTasks(any(), any(), eq(scope), any())).thenReturn(List.of());

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
    assertThat(result.errors).isEqualTo(1);
    assertThat(result.message).contains("No tables matched scope: missing-table-id");
  }

  @Test
  void executeTreatsNamespaceScopeMissAsNoopPlan() {
    var reconcilerService = mock(ReconcilerService.class);
    var jobs = mock(ReconcileJobStore.class);
    var executorRegistry = mock(ReconcileExecutorRegistry.class);
    when(executorRegistry.hasExecutorForJobKind(ReconcileJobKind.PLAN_VIEW)).thenReturn(true);
    var executor =
        new ConnectorPlanningReconcileExecutor(reconcilerService, jobs, executorRegistry, true);
    var scope = ReconcileScope.of(List.of("requested-ns-id"), null);
    var lease =
        new ReconcileJobStore.LeasedJob(
            "job-1",
            "acct",
            "connector-1",
            false,
            CaptureMode.METADATA_AND_STATS,
            scope,
            ReconcileExecutionPolicy.defaults(),
            "lease-1",
            "",
            "",
            ReconcileJobKind.PLAN_CONNECTOR,
            ReconcileTableTask.empty(),
            "");
    AtomicBoolean progressCalled = new AtomicBoolean(false);

    when(reconcilerService.planTableTasks(any(), any(), eq(scope), any())).thenReturn(List.of());
    when(reconcilerService.planViewTasks(any(), any(), eq(scope), any())).thenReturn(List.of());

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
                    message) -> progressCalled.set(true)));

    assertThat(result.ok()).isTrue();
    assertThat(result.scanned).isEqualTo(0);
    assertThat(result.changed).isEqualTo(0);
    verify(jobs, org.mockito.Mockito.never())
        .enqueueTablePlan(any(), any(), anyBoolean(), any(), any(), any(), any(), any(), any());
    verify(jobs, org.mockito.Mockito.never())
        .enqueueViewPlan(any(), any(), anyBoolean(), any(), any(), any(), any(), any(), any());
    assertThat(progressCalled.get()).isFalse();
  }

  @Test
  void executeFailsWhenViewTasksHaveNoExecutionExecutor() {
    var reconcilerService = mock(ReconcilerService.class);
    var jobs = mock(ReconcileJobStore.class);
    var executorRegistry = mock(ReconcileExecutorRegistry.class);
    when(executorRegistry.hasExecutorForJobKind(ReconcileJobKind.PLAN_VIEW)).thenReturn(false);
    var executor =
        new ConnectorPlanningReconcileExecutor(reconcilerService, jobs, executorRegistry, true);
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
            ReconcileJobKind.PLAN_CONNECTOR,
            ReconcileTableTask.empty(),
            "");

    when(reconcilerService.planTableTasks(any(), any(), any(), any())).thenReturn(List.of());
    when(reconcilerService.planViewTasks(any(), any(), any(), any()))
        .thenReturn(
            List.of(ReconcileViewTask.discovery("sales", "orders_view", "ns-1", "orders_view")));
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
    assertThat(result.errors).isEqualTo(1);
    assertThat(result.message)
        .contains("No enabled reconcile executor is available for PLAN_VIEW jobs");
    verify(jobs, org.mockito.Mockito.never())
        .enqueueViewPlan(any(), any(), anyBoolean(), any(), any(), any(), any(), any(), any());
  }

  @Test
  void executeStatsOnlySkipsViewPlanning() {
    var reconcilerService = mock(ReconcilerService.class);
    var jobs = mock(ReconcileJobStore.class);
    var executorRegistry = mock(ReconcileExecutorRegistry.class);
    when(executorRegistry.hasExecutorForJobKind(ReconcileJobKind.PLAN_TABLE)).thenReturn(true);
    var executor =
        new ConnectorPlanningReconcileExecutor(reconcilerService, jobs, executorRegistry, true);
    var lease =
        new ReconcileJobStore.LeasedJob(
            "job-1",
            "acct",
            "connector-1",
            false,
            CaptureMode.STATS_ONLY,
            ReconcileScope.of(java.util.List.of(), "trino_test"),
            ReconcileExecutionPolicy.defaults(),
            "lease-1",
            "",
            "",
            ReconcileJobKind.PLAN_CONNECTOR,
            ReconcileTableTask.empty(),
            "");

    when(reconcilerService.planTableTasks(any(), any(), any(), any()))
        .thenReturn(
            List.of(ReconcileTableTask.of("iceberg", "trino_test", "trino-test-id", "trino_test")));

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
    assertThat(result.viewsScanned).isEqualTo(0);
    verify(reconcilerService, org.mockito.Mockito.never())
        .planViewTasks(any(), any(), any(), any());
    verify(jobs, org.mockito.Mockito.never())
        .enqueueViewPlan(any(), any(), anyBoolean(), any(), any(), any(), any(), any(), any());
  }
}
