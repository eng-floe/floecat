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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionClass;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.spi.ReconcileExecutor;
import java.util.EnumSet;
import java.util.Optional;
import org.eclipse.microprofile.config.Config;
import org.junit.jupiter.api.Test;

class ReconcilerSchedulerTest {

  /**
   * Regression test for: leaked inFlight slot when leaseNext() throws.
   *
   * <p>Before the fix, an exception from leaseNext() left inFlight=1 permanently. With
   * maxParallelism=1 that caused every subsequent pollOnce() to short-circuit via
   * reserveWorkerSlot(), so leaseNext() was never called again and the queue stayed stuck in
   * "Queued (full)".
   *
   * <p>After the fix, the slot is released in the catch block and the next pollOnce() call reaches
   * leaseNext() successfully.
   */
  @Test
  void pollOnceReleasesWorkerSlotWhenLeaseNextThrows() throws Exception {
    var jobStore = mock(ReconcileJobStore.class);
    when(jobStore.leaseNext(org.mockito.ArgumentMatchers.any()))
        .thenThrow(new RuntimeException("simulated StorageNotFoundException"))
        .thenReturn(Optional.empty());

    var executor = mock(ReconcileExecutor.class);
    when(executor.enabled()).thenReturn(true);
    when(executor.supportedExecutionClasses())
        .thenReturn(EnumSet.allOf(ReconcileExecutionClass.class));
    when(executor.supportedLanes()).thenReturn(java.util.Set.of(""));

    var scheduler = new ReconcilerScheduler();
    // jobs is package-private (@Inject with no access modifier)
    scheduler.jobs = jobStore;
    scheduler.executorRegistry = new ReconcileExecutorRegistry(java.util.List.of(executor));
    scheduler.schedulerEnabled = true;
    // maxParallelism defaults to 1 (DEFAULT_MAX_PARALLELISM); workers stays null so submitLease()
    // would immediately release any slot it acquires — but we never reach submitLease() here.

    scheduler.pollOnce(); // leaseNext() throws → slot must be released
    scheduler.pollOnce(); // must reach leaseNext() again (slot was freed)

    verify(jobStore, times(2)).leaseNext(org.mockito.ArgumentMatchers.any());
  }

  @Test
  void runLeaseDispatchesThroughExecutorRegistry() {
    var jobStore = mock(ReconcileJobStore.class);
    var executor = mock(ReconcileExecutor.class);
    when(executor.enabled()).thenReturn(true);
    when(executor.supportedExecutionClasses())
        .thenReturn(EnumSet.allOf(ReconcileExecutionClass.class));
    when(executor.supportedLanes()).thenReturn(java.util.Set.of(""));
    when(executor.supports(org.mockito.ArgumentMatchers.any())).thenReturn(true);
    when(executor.supportsExecutionClass(org.mockito.ArgumentMatchers.any())).thenReturn(true);
    when(executor.supportsLane(org.mockito.ArgumentMatchers.any())).thenReturn(true);
    when(executor.id()).thenReturn("default_reconciler");
    var registry = new ReconcileExecutorRegistry(java.util.List.of(executor));

    var scheduler = new ReconcilerScheduler();
    scheduler.jobs = jobStore;
    scheduler.executorRegistry = registry;
    scheduler.cancellations = new ReconcileCancellationRegistry();
    scheduler.config = mock(Config.class);
    when(scheduler.config.getOptionalValue(
            org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.eq(Long.class)))
        .thenReturn(Optional.empty());

    var lease =
        new ReconcileJobStore.LeasedJob(
            "job-1",
            "acct",
            "connector",
            false,
            ReconcilerService.CaptureMode.METADATA_AND_STATS,
            ai.floedb.floecat.reconciler.jobs.ReconcileScope.empty(),
            ReconcileExecutionPolicy.defaults(),
            "lease-1",
            "",
            "");

    when(executor.execute(org.mockito.ArgumentMatchers.any()))
        .thenReturn(ReconcileExecutor.ExecutionResult.success(2, 1, 0, 3, 4, "OK"));

    scheduler.runLease(lease);

    verify(executor, times(1)).execute(org.mockito.ArgumentMatchers.any());
    verify(jobStore, times(1))
        .markRunning(
            org.mockito.ArgumentMatchers.eq("job-1"),
            org.mockito.ArgumentMatchers.eq("lease-1"),
            org.mockito.ArgumentMatchers.anyLong(),
            org.mockito.ArgumentMatchers.eq("default_reconciler"));
    verify(jobStore, times(1))
        .markSucceeded(
            org.mockito.ArgumentMatchers.eq("job-1"),
            org.mockito.ArgumentMatchers.eq("lease-1"),
            org.mockito.ArgumentMatchers.anyLong(),
            org.mockito.ArgumentMatchers.eq(2L),
            org.mockito.ArgumentMatchers.eq(1L),
            org.mockito.ArgumentMatchers.eq(3L),
            org.mockito.ArgumentMatchers.eq(4L));
    verify(jobStore, never())
        .markFailed(
            org.mockito.ArgumentMatchers.anyString(),
            org.mockito.ArgumentMatchers.anyString(),
            org.mockito.ArgumentMatchers.anyLong(),
            org.mockito.ArgumentMatchers.anyString(),
            org.mockito.ArgumentMatchers.anyLong(),
            org.mockito.ArgumentMatchers.anyLong(),
            org.mockito.ArgumentMatchers.anyLong(),
            org.mockito.ArgumentMatchers.anyLong(),
            org.mockito.ArgumentMatchers.anyLong());
  }
}
