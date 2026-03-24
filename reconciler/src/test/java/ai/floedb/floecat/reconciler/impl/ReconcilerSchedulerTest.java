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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import java.util.Optional;
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
    when(jobStore.leaseNext())
        .thenThrow(new RuntimeException("simulated StorageNotFoundException"))
        .thenReturn(Optional.empty());

    var scheduler = new ReconcilerScheduler();
    // jobs is package-private (@Inject with no access modifier)
    scheduler.jobs = jobStore;
    // maxParallelism defaults to 1 (DEFAULT_MAX_PARALLELISM); workers stays null so submitLease()
    // would immediately release any slot it acquires — but we never reach submitLease() here.

    scheduler.pollOnce(); // leaseNext() throws → slot must be released
    scheduler.pollOnce(); // must reach leaseNext() again (slot was freed)

    verify(jobStore, times(2)).leaseNext();
  }
}
