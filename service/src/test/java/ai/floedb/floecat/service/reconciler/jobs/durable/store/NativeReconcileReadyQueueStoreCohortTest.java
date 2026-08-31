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

package ai.floedb.floecat.service.reconciler.jobs.durable.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.LeaseRequest;
import ai.floedb.floecat.reconciler.jobs.ReconcileWorkerAffinity;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.repo.model.Keys;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Covers the production ready-queue store rather than the in-memory double: cohort isolation and
 * cohort-partitioned ready indexes both live here.
 */
class NativeReconcileReadyQueueStoreCohortTest {

  private static final long DUE_AT_MS = 1_000L;

  @Test
  void jobIsOnlyLeasableByItsOwnCohort() {
    NativeReconcileReadyQueueStore store = store();
    StoredReconcileJob record = record(ReconcileWorkerAffinity.of("ci-branch"));

    assertTrue(
        store.matchesLeaseRequest(
            record, request().withWorkerAffinity(ReconcileWorkerAffinity.of("ci-branch"))));
    assertFalse(
        store.matchesLeaseRequest(
            record, request().withWorkerAffinity(ReconcileWorkerAffinity.of("steady-state"))));
    // An unconfigured deployment must not pick up a cohorted job.
    assertFalse(store.matchesLeaseRequest(record, request()));
  }

  @Test
  void cohortedWorkerDoesNotLeaseLegacyUncohortedJob() {
    NativeReconcileReadyQueueStore store = store();
    StoredReconcileJob record = record(ReconcileWorkerAffinity.DISABLED);

    assertTrue(store.matchesLeaseRequest(record, request()));
    assertFalse(
        store.matchesLeaseRequest(
            record, request().withWorkerAffinity(ReconcileWorkerAffinity.of("ci-branch"))));
  }

  @Test
  void cohortedJobKeepsTheFilteredReadyIndexesInsteadOfOnePinnedSlice() {
    NativeReconcileReadyQueueStore store = store();
    StoredReconcileJob record = record(ReconcileWorkerAffinity.of("ci-branch"));

    List<String> keys = store.readyPointerKeys(record);

    // The regression this guards: overloading pinnedExecutorId collapsed every cohorted job into a
    // single pinned slice and emitted exactly one pointer.
    assertEquals(5, keys.size());
    assertTrue(keys.stream().anyMatch(key -> key.contains("by-job-kind")));
    assertTrue(keys.stream().anyMatch(key -> key.contains("by-execution-class")));
    assertTrue(keys.stream().anyMatch(key -> key.contains("by-execution-lane")));
    assertTrue(keys.stream().noneMatch(key -> key.contains("by-pinned-executor")));
  }

  @Test
  void executionPolicyLaneAndCanonicalLaneKeyHaveDistinctReadyIndexes() {
    NativeReconcileReadyQueueStore store = store();
    ReconcileWorkerAffinity affinity = ReconcileWorkerAffinity.of("ci-branch");

    for (ReconcileJobKind kind :
        List.of(ReconcileJobKind.EXEC_FILE_GROUP, ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE)) {
      StoredReconcileJob record = record(affinity);
      record.jobKind = kind.name();

      List<String> keys = store.readyPointerKeys(record);
      String policyLane = affinity.indexFilterValue(record.executionPolicy().lane());
      String canonicalLane = affinity.indexFilterValue(record.laneKey);
      String policyLaneKey =
          keys.stream()
              .filter(
                  key ->
                      key.startsWith(Keys.reconcileReadyByExecutionLanePointerPrefix(policyLane)))
              .findFirst()
              .orElseThrow();
      String canonicalLaneKey =
          keys.stream()
              .filter(
                  key ->
                      key.startsWith(
                          Keys.reconcileReadyByExecutionLanePointerPrefix(canonicalLane)))
              .findFirst()
              .orElseThrow();

      assertEquals(2, keys.stream().filter(key -> key.contains("by-execution-lane")).count());
      assertTrue(readyPointerMatches(store, record, policyLaneKey, policyLane));
      assertTrue(readyPointerMatches(store, record, canonicalLaneKey, canonicalLane));
      assertTrue(
          store.matchesLeaseRequest(
              record,
              LeaseRequest.of(Set.of(), Set.of("ci-run"), Set.of(), EnumSet.of(kind))
                  .withWorkerAffinity(affinity)));
      assertFalse(
          store.matchesLeaseRequest(
              record,
              LeaseRequest.of(Set.of(), Set.of("other-run"), Set.of(), EnumSet.of(kind))
                  .withWorkerAffinity(affinity)));
    }
  }

  @Test
  void cohortsGetDistinctSlicesOfTheSameIndex() {
    NativeReconcileReadyQueueStore store = store();

    List<String> branchKeys =
        store.readyPointerKeys(record(ReconcileWorkerAffinity.of("ci-branch")));
    List<String> steadyKeys =
        store.readyPointerKeys(record(ReconcileWorkerAffinity.of("steady-state")));
    List<String> legacyKeys = store.readyPointerKeys(record(ReconcileWorkerAffinity.DISABLED));

    String branchJobKind = onlyMatching(branchKeys, "by-job-kind");
    String steadyJobKind = onlyMatching(steadyKeys, "by-job-kind");
    String legacyJobKind = onlyMatching(legacyKeys, "by-job-kind");

    assertFalse(branchJobKind.equals(steadyJobKind));
    assertFalse(branchJobKind.equals(legacyJobKind));
  }

  @Test
  void anExplicitExecutorPinStillUsesThePinnedIndexAlone() {
    NativeReconcileReadyQueueStore store = store();
    StoredReconcileJob record = record(ReconcileWorkerAffinity.of("ci-branch"));
    record.pinnedExecutorId = "executor-1";

    List<String> keys = store.readyPointerKeys(record);

    assertEquals(1, keys.size());
    assertTrue(keys.get(0).contains("by-pinned-executor"));
  }

  @Test
  void cohortsGetDistinctPinnedSlicesForTheSameExecutor() {
    NativeReconcileReadyQueueStore store = store();
    StoredReconcileJob branch = record(ReconcileWorkerAffinity.of("ci-branch"));
    StoredReconcileJob steady = record(ReconcileWorkerAffinity.of("steady-state"));
    StoredReconcileJob unversioned = record(ReconcileWorkerAffinity.DISABLED);
    branch.pinnedExecutorId = "executor-1";
    steady.pinnedExecutorId = "executor-1";
    unversioned.pinnedExecutorId = "executor-1";

    String branchKey = store.readyPointerKeys(branch).getFirst();
    String steadyKey = store.readyPointerKeys(steady).getFirst();
    String unversionedKey = store.readyPointerKeys(unversioned).getFirst();

    assertFalse(branchKey.equals(steadyKey));
    assertFalse(branchKey.equals(unversionedKey));
    assertEquals("executor-1", branch.pinnedExecutorId());
  }

  @Test
  void pinnedPointerValidationRequiresTheCohortQualifiedExecutor() {
    NativeReconcileReadyQueueStore store = store();
    ReconcileWorkerAffinity affinity = ReconcileWorkerAffinity.of("ci-branch");
    StoredReconcileJob record = record(affinity);
    record.pinnedExecutorId = "executor-1";
    String qualifiedExecutor = affinity.indexFilterValue(record.pinnedExecutorId());
    String qualifiedKey =
        store.readyPointerKeyFor(
            record,
            ReconcileReadyQueueStore.ReadyIndexType.PINNED_EXECUTOR,
            DUE_AT_MS,
            qualifiedExecutor);

    assertTrue(
        store.readyPointerMatchesRecord(
            new ReconcileReadyQueueStore.ReadyQueueEntry(
                qualifiedKey,
                "canonical-1",
                record.accountId,
                record.jobId,
                DUE_AT_MS,
                ReconcileReadyQueueStore.ReadyIndexType.PINNED_EXECUTOR,
                qualifiedExecutor),
            record));

    String rawKey =
        store.readyPointerKeyFor(
            record,
            ReconcileReadyQueueStore.ReadyIndexType.PINNED_EXECUTOR,
            DUE_AT_MS,
            record.pinnedExecutorId());
    assertFalse(
        store.readyPointerMatchesRecord(
            new ReconcileReadyQueueStore.ReadyQueueEntry(
                rawKey,
                "canonical-1",
                record.accountId,
                record.jobId,
                DUE_AT_MS,
                ReconcileReadyQueueStore.ReadyIndexType.PINNED_EXECUTOR,
                record.pinnedExecutorId()),
            record));
  }

  private static String onlyMatching(List<String> keys, String fragment) {
    return keys.stream().filter(key -> key.contains(fragment)).findFirst().orElseThrow();
  }

  private static boolean readyPointerMatches(
      NativeReconcileReadyQueueStore store,
      StoredReconcileJob record,
      String readyPointerKey,
      String lane) {
    return store.readyPointerMatchesRecord(
        new ReconcileReadyQueueStore.ReadyQueueEntry(
            readyPointerKey,
            "canonical-1",
            record.accountId,
            record.jobId,
            DUE_AT_MS,
            ReconcileReadyQueueStore.ReadyIndexType.EXECUTION_LANE,
            lane),
        record);
  }

  private static LeaseRequest request() {
    return LeaseRequest.of(
        Set.of(), Set.of(), Set.of(), EnumSet.of(ReconcileJobKind.PLAN_CONNECTOR));
  }

  private static NativeReconcileReadyQueueStore store() {
    NativeReconcileReadyQueueStore store = new NativeReconcileReadyQueueStore();
    store.bind(null, null, null, 128, record -> true, record -> false);
    return store;
  }

  private static StoredReconcileJob record(ReconcileWorkerAffinity workerAffinity) {
    StoredReconcileJob record = new StoredReconcileJob();
    record.jobId = "job-1";
    record.accountId = "acct-1";
    record.jobKind = ReconcileJobKind.PLAN_CONNECTOR.name();
    record.state = "JS_QUEUED";
    record.executionClass = "DEFAULT";
    record.executionLane = "ci-run";
    record.laneKey = "file-group|table-1|snapshot-1|group-0";
    record.nextAttemptAtMs = DUE_AT_MS;
    record.executionAttributes =
        workerAffinity == null || !workerAffinity.enabled()
            ? Map.of()
            : Map.of(ReconcileWorkerAffinity.ATTRIBUTE, workerAffinity.value());
    return record;
  }
}
