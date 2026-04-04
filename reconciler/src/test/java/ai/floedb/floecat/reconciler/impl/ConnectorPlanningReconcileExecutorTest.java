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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.spi.ReconcileExecutor;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class ConnectorPlanningReconcileExecutorTest {

  @Test
  void executeFansOutTableJobs() throws Exception {
    var reconcilerService = mock(ReconcilerService.class);
    var jobs = mock(ReconcileJobStore.class);
    var executor = new ConnectorPlanningReconcileExecutor(reconcilerService, jobs, true);

    var scope = ReconcileScope.empty();
    var executionPolicy = ReconcileExecutionPolicy.defaults();
    var lease =
        new ReconcileJobStore.LeasedJob(
            "job-1",
            "acct",
            "connector",
            false,
            ReconcilerService.CaptureMode.METADATA_AND_STATS,
            scope,
            executionPolicy,
            "lease-1",
            "",
            "default_reconciler",
            ReconcileJobKind.PLAN_CONNECTOR,
            ReconcileTableTask.empty(),
            "");

    when(reconcilerService.planTableTasks(any(), any(), eq(scope), eq(null)))
        .thenReturn(
            List.of(
                ReconcileTableTask.of("ns", "table_a"), ReconcileTableTask.of("ns", "table_b")));

    var scanned = new AtomicLong();
    var result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease,
                () -> false,
                (s, changed, errors, snapshotsProcessed, statsProcessed, message) ->
                    scanned.set(s)));

    assertTrue(result.ok());
    assertEquals(2L, result.scanned);
    assertEquals(2L, scanned.get());
    verify(jobs, times(1))
        .enqueueTableExecution(
            "acct",
            "connector",
            false,
            ReconcilerService.CaptureMode.METADATA_AND_STATS,
            scope,
            ReconcileTableTask.of("ns", "table_a"),
            executionPolicy,
            "job-1",
            "");
    verify(jobs, times(1))
        .enqueueTableExecution(
            "acct",
            "connector",
            false,
            ReconcilerService.CaptureMode.METADATA_AND_STATS,
            scope,
            ReconcileTableTask.of("ns", "table_b"),
            executionPolicy,
            "job-1",
            "");
  }

  @Test
  void executeUsesConfiguredPinnedExecutorForChildTableJobs() throws Exception {
    var reconcilerService = mock(ReconcilerService.class);
    var jobs = mock(ReconcileJobStore.class);
    System.setProperty("floecat.reconciler.auto.pinned-executor-id", "remote-executor");
    try {
      var executor = new ConnectorPlanningReconcileExecutor(reconcilerService, jobs, true);

      var lease =
          new ReconcileJobStore.LeasedJob(
              "job-1",
              "acct",
              "connector",
              false,
              ReconcilerService.CaptureMode.METADATA_AND_STATS,
              ReconcileScope.empty(),
              ReconcileExecutionPolicy.defaults(),
              "lease-1",
              "planner_reconciler",
              "planner_reconciler",
              ReconcileJobKind.PLAN_CONNECTOR,
              ReconcileTableTask.empty(),
              "");

      when(reconcilerService.planTableTasks(any(), any(), any(), eq(null)))
          .thenReturn(List.of(ReconcileTableTask.of("ns", "table_a")));

      executor.execute(
          new ReconcileExecutor.ExecutionContext(
              lease,
              () -> false,
              (s, changed, errors, snapshotsProcessed, statsProcessed, message) -> {}));

      verify(jobs)
          .enqueueTableExecution(
              "acct",
              "connector",
              false,
              ReconcilerService.CaptureMode.METADATA_AND_STATS,
              ReconcileScope.empty(),
              ReconcileTableTask.of("ns", "table_a"),
              ReconcileExecutionPolicy.defaults(),
              "job-1",
              "remote-executor");
    } finally {
      System.clearProperty("floecat.reconciler.auto.pinned-executor-id");
    }
  }

  @Test
  void plannerExecutorSupportsAnyLaneForPlanJobs() {
    var executor =
        new ConnectorPlanningReconcileExecutor(
            mock(ReconcilerService.class), mock(ReconcileJobStore.class), true);

    assertTrue(executor.supportedLanes().isEmpty());
    assertTrue(executor.supportsLane(""));
    assertTrue(executor.supportsLane("remote"));
    assertTrue(executor.supportsLane("custom-lane"));
  }

  @Test
  void executePreservesPartialProgressWhenFanOutFails() throws Exception {
    var reconcilerService = mock(ReconcilerService.class);
    var jobs = mock(ReconcileJobStore.class);
    var executor = new ConnectorPlanningReconcileExecutor(reconcilerService, jobs, true);

    var scope = ReconcileScope.empty();
    var executionPolicy = ReconcileExecutionPolicy.defaults();
    var lease =
        new ReconcileJobStore.LeasedJob(
            "job-1",
            "acct",
            "connector",
            false,
            ReconcilerService.CaptureMode.METADATA_AND_STATS,
            scope,
            executionPolicy,
            "lease-1",
            "",
            "default_reconciler",
            ReconcileJobKind.PLAN_CONNECTOR,
            ReconcileTableTask.empty(),
            "");

    var first = ReconcileTableTask.of("ns", "table_a");
    var second = ReconcileTableTask.of("ns", "table_b");
    when(reconcilerService.planTableTasks(any(), any(), eq(scope), eq(null)))
        .thenReturn(List.of(first, second));
    doThrow(new RuntimeException("boom"))
        .when(jobs)
        .enqueueTableExecution(
            eq("acct"),
            eq("connector"),
            eq(false),
            eq(ReconcilerService.CaptureMode.METADATA_AND_STATS),
            eq(scope),
            eq(second),
            eq(executionPolicy),
            eq("job-1"),
            eq(""));

    var result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease,
                () -> false,
                (s, changed, errors, snapshotsProcessed, statsProcessed, message) -> {}));

    assertFalse(result.ok());
    assertEquals(1L, result.scanned);
    assertEquals(1L, result.errors);
    assertTrue(result.message.contains("after enqueuing 1 table jobs"));
  }
}
