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

package ai.floedb.floecat.service.reconciler.jobs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionClass;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.StatsPriorityClass;
import ai.floedb.floecat.reconciler.jobs.impl.InMemoryReconcileJobStore;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Tests for priority-aware dispatch in {@link InMemoryReconcileJobStore}.
 *
 * <p>Verifies that the {@link ai.floedb.floecat.reconciler.jobs.impl.PriorityReadyQueue} correctly
 * orders jobs by class (P0 before P3) and by score (higher score first within a class).
 */
class InMemoryStorePriorityDispatchTest {

  private static ReconcileExecutionPolicy policyFor(StatsPriorityClass cls, long score) {
    return ReconcileExecutionPolicy.of(ReconcileExecutionClass.DEFAULT, "", Map.of(), cls, score);
  }

  private static String enqueueJob(
      InMemoryReconcileJobStore store, String connId, ReconcileExecutionPolicy policy) {
    return store.enqueue(
        "acct",
        connId,
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.of(List.of(), "tbl"),
        ReconcileJobKind.PLAN_CONNECTOR,
        null,
        null,
        null,
        null,
        policy,
        "",
        "");
  }

  @Test
  void p0IsDispatchedBeforeP3WhenP3EnqueuedFirst() {
    var store = new InMemoryReconcileJobStore();
    enqueueJob(store, "conn-p3", policyFor(StatsPriorityClass.P3_BACKGROUND, 0L));
    String p0Id = enqueueJob(store, "conn-p0", policyFor(StatsPriorityClass.P0_SYNC, 0L));

    var first = store.leaseNext(ReconcileJobStore.LeaseRequest.all());
    assertTrue(first.isPresent(), "First lease must succeed");
    assertEquals(
        p0Id, first.get().jobId, "P0 job must be dispatched first regardless of enqueue order");
    assertEquals(StatsPriorityClass.P0_SYNC, first.get().executionPolicy.priorityClass());
  }

  @Test
  void p1IsDispatchedBeforeP2AndP3() {
    var store = new InMemoryReconcileJobStore();
    enqueueJob(store, "conn-p3", policyFor(StatsPriorityClass.P3_BACKGROUND, 0L));
    enqueueJob(store, "conn-p2", policyFor(StatsPriorityClass.P2_REPAIR, 0L));
    String p1Id = enqueueJob(store, "conn-p1", policyFor(StatsPriorityClass.P1_FRESHNESS, 0L));

    var first = store.leaseNext(ReconcileJobStore.LeaseRequest.all());
    assertTrue(first.isPresent());
    assertEquals(p1Id, first.get().jobId, "P1 must be dispatched before P2 and P3");
  }

  @Test
  void higherScoreIsDispatchedBeforeLowerScoreWithinSameClass() {
    var store = new InMemoryReconcileJobStore();
    enqueueJob(store, "conn-low", policyFor(StatsPriorityClass.P3_BACKGROUND, 10L));
    String highId =
        enqueueJob(store, "conn-high", policyFor(StatsPriorityClass.P3_BACKGROUND, 500L));

    var first = store.leaseNext(ReconcileJobStore.LeaseRequest.all());
    assertTrue(first.isPresent());
    assertEquals(
        highId,
        first.get().jobId,
        "Higher-score P3 job (500) must be dispatched before lower-score P3 job (10)");
  }

  @Test
  void classOrderPrevalenceOverScore() {
    // P3 at maximum score must still lose to P0 at score 0.
    var store = new InMemoryReconcileJobStore();
    enqueueJob(store, "conn-p3", policyFor(StatsPriorityClass.P3_BACKGROUND, Long.MAX_VALUE));
    String p0Id = enqueueJob(store, "conn-p0", policyFor(StatsPriorityClass.P0_SYNC, 0L));

    var first = store.leaseNext(ReconcileJobStore.LeaseRequest.all());
    assertTrue(first.isPresent());
    assertEquals(p0Id, first.get().jobId, "P0 at score=0 must beat P3 at max score");
  }
}
