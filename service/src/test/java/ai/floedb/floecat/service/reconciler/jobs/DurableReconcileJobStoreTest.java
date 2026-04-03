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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.common.rpc.BlobHeader;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionClass;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.ReconcileJob;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.errors.StorageNotFoundException;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.BlobStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DurableReconcileJobStoreTest {
  private static final String ACCOUNT_ID = "acct-1";
  private static final String CONNECTOR_ID = "conn-1";

  private DurableReconcileJobStore store;

  @BeforeEach
  void setUp() {
    store = new DurableReconcileJobStore();
    store.pointerStore = new InMemoryPointerStore();
    store.blobStore = new InMemoryBlobStore();
    store.mapper = new ObjectMapper();
    store.config = ConfigProvider.getConfig();
  }

  @AfterEach
  void tearDown() {
    System.clearProperty("floecat.reconciler.job-store.max-attempts");
    System.clearProperty("floecat.reconciler.job-store.base-backoff-ms");
    System.clearProperty("floecat.reconciler.job-store.max-backoff-ms");
    System.clearProperty("floecat.reconciler.job-store.lease-ms");
    System.clearProperty("floecat.reconciler.job-store.reclaim-interval-ms");
  }

  @Test
  void enqueueDedupesWhileJobIsActive() {
    store.init();
    ReconcileScope scope = ReconcileScope.of(List.of(List.of("ns")), "tbl", List.of("c1"));

    String first =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_STATS, scope);
    String second =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_STATS, scope);

    assertEquals(first, second);
  }

  @Test
  void enqueueDoesNotDedupeAcrossDifferentExecutionPolicies() {
    store.init();
    ReconcileScope scope = ReconcileScope.of(List.of(List.of("ns")), "tbl", List.of("c1"));

    String first =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_STATS,
            scope,
            ReconcileExecutionPolicy.of(ReconcileExecutionClass.DEFAULT, "", java.util.Map.of()),
            "");
    String second =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_STATS,
            scope,
            ReconcileExecutionPolicy.of(
                ReconcileExecutionClass.HEAVY, "remote", java.util.Map.of()),
            "");

    assertNotEquals(first, second);
  }

  @Test
  void leaseNextFiltersByExecutionPolicy() {
    store.init();

    store.enqueue(
        ACCOUNT_ID,
        "conn-default",
        false,
        CaptureMode.METADATA_AND_STATS,
        ReconcileScope.empty(),
        ReconcileExecutionPolicy.defaults(),
        "");
    String remoteJobId =
        store.enqueue(
            ACCOUNT_ID,
            "conn-remote",
            false,
            CaptureMode.METADATA_AND_STATS,
            ReconcileScope.empty(),
            ReconcileExecutionPolicy.of(
                ReconcileExecutionClass.HEAVY, "remote", java.util.Map.of()),
            "");

    var remoteLease =
        store
            .leaseNext(
                ReconcileJobStore.LeaseRequest.of(
                    java.util.EnumSet.of(ReconcileExecutionClass.HEAVY),
                    java.util.Set.of("remote")))
            .orElseThrow();

    assertEquals(remoteJobId, remoteLease.jobId);
    assertEquals("remote", remoteLease.executionPolicy.lane());
  }

  @Test
  void leaseNextRespectsPinnedExecutorId() {
    store.init();

    store.enqueue(
        ACCOUNT_ID,
        "conn-a",
        false,
        CaptureMode.METADATA_AND_STATS,
        ReconcileScope.empty(),
        ReconcileExecutionPolicy.of(ReconcileExecutionClass.HEAVY, "remote", java.util.Map.of()),
        "remote-a");
    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            "conn-b",
            false,
            CaptureMode.METADATA_AND_STATS,
            ReconcileScope.empty(),
            ReconcileExecutionPolicy.of(
                ReconcileExecutionClass.HEAVY, "remote", java.util.Map.of()),
            "remote-b");

    var lease =
        store
            .leaseNext(
                ReconcileJobStore.LeaseRequest.of(
                    java.util.EnumSet.of(ReconcileExecutionClass.HEAVY),
                    java.util.Set.of("remote"),
                    java.util.Set.of("remote-b")))
            .orElseThrow();

    assertEquals(jobId, lease.jobId);
    assertEquals("remote-b", lease.pinnedExecutorId);
  }

  @Test
  void successfulJobClearsDedupeAndAllowsFreshEnqueue() {
    store.init();
    ReconcileScope scope = ReconcileScope.of(List.of(List.of("ns")), "tbl", List.of());
    String first =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_STATS, scope);

    var leased = store.leaseNext().orElseThrow();
    store.markSucceeded(first, leased.leaseEpoch, System.currentTimeMillis(), 10, 2, 4, 20);

    String second =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_STATS, scope);
    assertNotEquals(first, second);
  }

  @Test
  void enqueuePreservesSnapshotScopeIncludingZero() {
    store.init();
    ReconcileScope scope =
        ReconcileScope.of(List.of(List.of("ns")), "tbl", List.of(), List.of(0L, 3L));

    String jobId =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_STATS, scope);

    ReconcileJob job = store.get(jobId).orElseThrow();
    assertEquals(List.of(0L, 3L), job.scope.destinationSnapshotIds());
    assertTrue(job.scope.hasSnapshotFilter());
  }

  @Test
  void enqueuePreservesExecutionPolicyAndPinnedExecutorAcrossLeaseAndAssignment() {
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_STATS,
            ReconcileScope.empty(),
            ReconcileExecutionPolicy.of(
                ReconcileExecutionClass.HEAVY, "remote", java.util.Map.of("tier", "gold")),
            "remote-executor");

    ReconcileJob job = store.get(jobId).orElseThrow();
    assertEquals(ReconcileExecutionClass.HEAVY, job.executionPolicy.executionClass());
    assertEquals("remote", job.executionPolicy.lane());
    assertEquals("gold", job.executionPolicy.attributes().get("tier"));
    assertEquals("", job.executorId);

    var lease = store.leaseNext().orElseThrow();
    assertEquals(jobId, lease.jobId);
    assertEquals("remote-executor", lease.pinnedExecutorId);
    assertEquals(ReconcileExecutionClass.HEAVY, lease.executionPolicy.executionClass());

    store.markRunning(lease.jobId, lease.leaseEpoch, System.currentTimeMillis(), "remote-executor");
    assertEquals("remote-executor", store.get(jobId).orElseThrow().executorId);
  }

  @Test
  void markFailedRequeuesAndEventuallyTransitionsToFailed() throws Exception {
    System.setProperty("floecat.reconciler.job-store.max-attempts", "2");
    System.setProperty("floecat.reconciler.job-store.base-backoff-ms", "100");
    System.setProperty("floecat.reconciler.job-store.max-backoff-ms", "100");
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_STATS,
            ReconcileScope.empty());
    var firstLease = store.leaseNext().orElseThrow();

    store.markFailed(
        jobId, firstLease.leaseEpoch, System.currentTimeMillis(), "transient", 1, 0, 1, 2, 3);
    ReconcileJob retried = store.get(jobId).orElseThrow();
    assertEquals("JS_QUEUED", retried.state);

    Thread.sleep(120L);
    var secondLease = store.leaseNext().orElseThrow();
    store.markFailed(
        jobId, secondLease.leaseEpoch, System.currentTimeMillis(), "terminal", 1, 0, 2, 2, 3);
    ReconcileJob failed = store.get(jobId).orElseThrow();
    assertEquals("JS_FAILED", failed.state);
  }

  @Test
  void getIsScopedToAccount() {
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_STATS,
            ReconcileScope.empty());

    assertTrue(store.get(ACCOUNT_ID, jobId).isPresent());
    assertTrue(store.get("acct-2", jobId).isEmpty());
  }

  @Test
  void listPaginationDoesNotSkipLaterMatchingJobs() {
    store.init();

    String first =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_STATS,
            ReconcileScope.of(List.of(List.of("ns1")), "t1", List.of()));
    String second =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_STATS,
            ReconcileScope.of(List.of(List.of("ns2")), "t2", List.of()));

    var firstPage = store.list(ACCOUNT_ID, 1, "", CONNECTOR_ID, java.util.Set.of());
    assertEquals(1, firstPage.jobs.size());
    assertTrue(
        !firstPage.nextPageToken.isBlank(), "expected a continuation token for remaining jobs");

    var secondPage =
        store.list(ACCOUNT_ID, 1, firstPage.nextPageToken, CONNECTOR_ID, java.util.Set.of());
    assertEquals(1, secondPage.jobs.size());

    var seen = List.of(firstPage.jobs.get(0).jobId, secondPage.jobs.get(0).jobId);
    assertTrue(seen.contains(first));
    assertTrue(seen.contains(second));
  }

  @Test
  void expiredLeasesAreReclaimedAndRequeued() throws Exception {
    System.setProperty("floecat.reconciler.job-store.lease-ms", "1000");
    System.setProperty("floecat.reconciler.job-store.reclaim-interval-ms", "1000");
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_STATS,
            ReconcileScope.empty());
    var firstLease = store.leaseNext();
    assertTrue(firstLease.isPresent());
    assertEquals(jobId, firstLease.get().jobId);

    Thread.sleep(1150L);

    var secondLease = store.leaseNext();
    assertTrue(secondLease.isPresent());
    assertEquals(jobId, secondLease.get().jobId);
  }

  @Test
  void cancellingLeaseIsReclaimedWithoutLosingCancellationIntent() throws Exception {
    System.setProperty("floecat.reconciler.job-store.lease-ms", "1000");
    System.setProperty("floecat.reconciler.job-store.reclaim-interval-ms", "1000");
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_STATS,
            ReconcileScope.empty());
    var firstLease = store.leaseNext().orElseThrow();
    store.markRunning(
        firstLease.jobId, firstLease.leaseEpoch, System.currentTimeMillis(), "default_reconciler");
    store.cancel(ACCOUNT_ID, jobId, "stop");
    assertEquals("JS_CANCELLING", store.get(jobId).orElseThrow().state);

    Thread.sleep(1150L);

    var secondLease = store.leaseNext().orElseThrow();
    assertEquals(jobId, secondLease.jobId);
    assertEquals("JS_CANCELLING", store.get(jobId).orElseThrow().state);

    store.markCancelled(
        secondLease.jobId,
        secondLease.leaseEpoch,
        System.currentTimeMillis(),
        "Cancelled",
        0,
        0,
        0,
        0,
        0);
    assertEquals("JS_CANCELLED", store.get(jobId).orElseThrow().state);
  }

  @Test
  void cancelPokesLeaseExpiryForFasterReclaim() throws Exception {
    System.setProperty("floecat.reconciler.job-store.lease-ms", "5000");
    System.setProperty("floecat.reconciler.job-store.reclaim-interval-ms", "200");
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_STATS,
            ReconcileScope.empty());
    var firstLease = store.leaseNext().orElseThrow();
    store.markRunning(
        firstLease.jobId, firstLease.leaseEpoch, System.currentTimeMillis(), "default_reconciler");
    store.cancel(ACCOUNT_ID, jobId, "stop");

    // Cancel should poke lease expiry, allowing reclaim well before the original 5s lease.
    Thread.sleep(1300L);

    var secondLease = store.leaseNext().orElseThrow();
    assertEquals(jobId, secondLease.jobId);
    assertEquals("JS_CANCELLING", store.get(jobId).orElseThrow().state);
  }

  @Test
  void renewLeaseExtendsLeaseAndDelaysReclaim() throws Exception {
    System.setProperty("floecat.reconciler.job-store.lease-ms", "2000");
    System.setProperty("floecat.reconciler.job-store.reclaim-interval-ms", "200");
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_STATS,
            ReconcileScope.empty());
    var lease = store.leaseNext().orElseThrow();
    store.markRunning(
        lease.jobId, lease.leaseEpoch, System.currentTimeMillis(), "default_reconciler");

    Thread.sleep(500L);
    assertTrue(store.renewLease(jobId, lease.leaseEpoch));

    Thread.sleep(700L);
    assertTrue(store.leaseNext().isEmpty());

    Thread.sleep(1600L);
    assertEquals(jobId, store.leaseNext().orElseThrow().jobId);
  }

  @Test
  void renewLeaseRejectsStaleEpoch() {
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_STATS,
            ReconcileScope.empty());
    var lease = store.leaseNext().orElseThrow();
    store.markRunning(
        lease.jobId, lease.leaseEpoch, System.currentTimeMillis(), "default_reconciler");

    assertTrue(!store.renewLease(jobId, "stale-epoch"));
  }

  @Test
  void parseDueMillisAcceptsReadyKeyWithoutLeadingSlash() throws Exception {
    store.init();
    Method parseDueMillis =
        DurableReconcileJobStore.class.getDeclaredMethod("parseDueMillis", String.class);
    parseDueMillis.setAccessible(true);

    long dueAt = 123456789L;
    String canonical = Keys.reconcileReadyPointerByDue(dueAt, ACCOUNT_ID, "lane", "job-1");
    String normalized = canonical.substring(1);

    long parsedCanonical = (long) parseDueMillis.invoke(store, canonical);
    long parsedNormalized = (long) parseDueMillis.invoke(store, normalized);

    assertEquals(dueAt, parsedCanonical);
    assertEquals(dueAt, parsedNormalized);
  }

  @Test
  void queueStatsReflectQueuedRunningAndCancellingJobs() {
    store.init();

    store.enqueue(
        ACCOUNT_ID, "conn-q", false, CaptureMode.METADATA_AND_STATS, ReconcileScope.empty());
    store.enqueue(
        ACCOUNT_ID, "conn-r", false, CaptureMode.METADATA_AND_STATS, ReconcileScope.empty());
    store.enqueue(
        ACCOUNT_ID, "conn-c", false, CaptureMode.METADATA_AND_STATS, ReconcileScope.empty());

    var firstLease = store.leaseNext().orElseThrow();
    store.markRunning(
        firstLease.jobId, firstLease.leaseEpoch, System.currentTimeMillis(), "default_reconciler");

    var cancellingLease = store.leaseNext().orElseThrow();
    store.markRunning(
        cancellingLease.jobId,
        cancellingLease.leaseEpoch,
        System.currentTimeMillis(),
        "default_reconciler");
    store.cancel(ACCOUNT_ID, cancellingLease.jobId, "stop");

    var stats = store.queueStats();

    assertEquals(1L, stats.queued);
    assertEquals(1L, stats.running);
    assertEquals(1L, stats.cancelling);
    assertTrue(stats.oldestQueuedCreatedAtMs > 0L);
  }

  /**
   * Regression test for: StorageNotFoundException from blobStore.get() inside readRecordByBlobUri()
   * propagating through reclaimExpiredLeasesIfDue() and leaseNext().
   *
   * <p>Before the fix, a missing blob caused leaseNext() to throw, which leaked a worker slot in
   * ReconcilerScheduler and permanently halted dispatch. After the fix, readRecordByBlobUri()
   * treats StorageNotFoundException exception the same as a missing blob and returns
   * Optional.empty().
   */
  @Test
  void leaseNextDoesNotThrowWhenReclaimedBlobIsMissing() throws Exception {
    System.setProperty("floecat.reconciler.job-store.lease-ms", "1");
    System.setProperty("floecat.reconciler.job-store.reclaim-interval-ms", "1");
    store.init();

    store.enqueue(
        ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_STATS, ReconcileScope.empty());
    // Take the first lease so the job is removed from the ready queue.
    // leaseNext() short-circuits via leaseReadyDue() here — reclaim is NOT triggered yet.
    store.leaseNext().orElseThrow();

    // Replace blobStore with one that throws StorageNotFoundException on every get().
    // This simulates the S3 "not found" case reported in the bug.
    store.blobStore = new ThrowingOnGetBlobStore();

    Thread.sleep(50L); // ensure lease-ms and reclaim-interval-ms have elapsed

    // leaseNext() now has nothing in the ready queue, so it calls reclaimExpiredLeasesIfDue(),
    // which iterates lookup pointers and calls readRecordByBlobUri() → blobStore.get() → throws.
    // Before the fix: StorageNotFoundException escapes leaseNext().
    // After the fix: readRecordByBlobUri() catches it and returns Optional.empty().
    assertDoesNotThrow(() -> store.leaseNext());
    assertTrue(store.leaseNext().isEmpty());
  }

  /** BlobStore that throws StorageNotFoundException on every get() call. */
  private static class ThrowingOnGetBlobStore implements BlobStore {
    @Override
    public byte[] get(String uri) {
      throw new StorageNotFoundException("simulated missing blob: " + uri);
    }

    @Override
    public void put(String uri, byte[] bytes, String contentType) {}

    @Override
    public Optional<BlobHeader> head(String uri) {
      return Optional.empty();
    }

    @Override
    public boolean delete(String uri) {
      return false;
    }

    @Override
    public void deletePrefix(String prefix) {}

    @Override
    public Page list(String prefix, int limit, String pageToken) {
      return new Page() {
        @Override
        public List<String> keys() {
          return Collections.emptyList();
        }

        @Override
        public String nextToken() {
          return "";
        }
      };
    }
  }
}
