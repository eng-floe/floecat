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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DurableReconcileJobStoreLeaseOutcomeTest {
  private static final String ACCOUNT_ID = "acct-1";
  private static final String CONNECTOR_ID = "conn-1";
  private static final String INLINE_JOB_LEASE_PREFIX = "inline:reconcile-lease:";

  private DurableReconcileJobStore store;

  @BeforeEach
  void setUp() {
    store = new DurableReconcileJobStore();
    store.pointerStore = new InMemoryPointerStore();
    store.blobStore = new InMemoryBlobStore();
    store.mapper = new ObjectMapper();
    store.config = ConfigProvider.getConfig();
    store.init();
  }

  @Test
  void applyLeaseOutcomeReturnsTrueForAcceptedTransitions() {
    String succeededJobId = enqueueRoot();
    ReconcileJobStore.LeasedJob succeededLease = store.leaseNext().orElseThrow();
    assertTrue(
        store.applyLeaseOutcome(
            succeededJobId,
            succeededLease.leaseEpoch,
            ReconcileJobStore.CompletionKind.SUCCEEDED,
            2_000L,
            "done",
            1L,
            1L,
            0L,
            0L,
            0L,
            0L,
            0L));
    assertEquals("JS_SUCCEEDED", store.get(succeededJobId).orElseThrow().state);

    String cancelledJobId = enqueueRoot();
    ReconcileJobStore.LeasedJob cancelledLease = store.leaseNext().orElseThrow();
    store.cancel(ACCOUNT_ID, cancelledJobId, "stop");
    assertTrue(
        store.applyLeaseOutcome(
            cancelledJobId,
            cancelledLease.leaseEpoch,
            ReconcileJobStore.CompletionKind.CANCELLED,
            3_000L,
            "stop",
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L));
    assertEquals("JS_CANCELLED", store.get(cancelledJobId).orElseThrow().state);
  }

  @Test
  void applyLeaseOutcomeCancellingSuccessResolvesImmediatelyToCancelled() {
    String jobId = enqueueRoot();
    ReconcileJobStore.LeasedJob lease = store.leaseNext().orElseThrow();
    store.cancel(ACCOUNT_ID, jobId, "stop");

    assertTrue(
        store.applyLeaseOutcome(
            jobId,
            lease.leaseEpoch,
            ReconcileJobStore.CompletionKind.SUCCEEDED,
            4_000L,
            "done",
            3L,
            2L,
            1L,
            0L,
            0L,
            5L,
            7L));

    ReconcileJobStore.ReconcileJob job = store.get(jobId).orElseThrow();
    assertEquals("JS_CANCELLED", job.state);
    assertEquals("stop", job.message);
    assertEquals(4_000L, job.finishedAtMs);
    assertEquals(3L, job.tablesScanned);
    assertEquals(2L, job.tablesChanged);
    assertEquals(1L, job.viewsScanned);
    assertEquals(0L, job.viewsChanged);
    assertEquals(0L, job.errors);
    assertEquals(5L, job.snapshotsProcessed);
    assertEquals(7L, job.statsProcessed);
    assertFalse(
        store.pointerStore.get(Keys.reconcileJobLeasePointerById(ACCOUNT_ID, jobId)).isPresent());
  }

  @Test
  void applyLeaseOutcomeCancellingFailureResolvesImmediatelyToCancelled() {
    String jobId = enqueueRoot();
    ReconcileJobStore.LeasedJob lease = store.leaseNext().orElseThrow();
    store.cancel(ACCOUNT_ID, jobId, "stop");

    assertTrue(
        store.applyLeaseOutcome(
            jobId,
            lease.leaseEpoch,
            ReconcileJobStore.CompletionKind.FAILED_TERMINAL,
            5_000L,
            "boom",
            8L,
            4L,
            2L,
            1L,
            6L,
            0L,
            0L));

    ReconcileJobStore.ReconcileJob job = store.get(jobId).orElseThrow();
    assertEquals("JS_CANCELLED", job.state);
    assertEquals("stop", job.message);
    assertEquals(5_000L, job.finishedAtMs);
    assertEquals(8L, job.tablesScanned);
    assertEquals(4L, job.tablesChanged);
    assertEquals(2L, job.viewsScanned);
    assertEquals(1L, job.viewsChanged);
    assertEquals(6L, job.errors);
    assertFalse(
        store.pointerStore.get(Keys.reconcileJobLeasePointerById(ACCOUNT_ID, jobId)).isPresent());
  }

  @Test
  void applyLeaseOutcomeReturnsFalseForStaleLeaseEpoch() {
    String jobId = enqueueRoot();
    store.leaseNext().orElseThrow();

    assertFalse(
        store.applyLeaseOutcome(
            jobId,
            "stale-epoch",
            ReconcileJobStore.CompletionKind.SUCCEEDED,
            2_000L,
            "done",
            1L,
            1L,
            0L,
            0L,
            0L,
            0L,
            0L));

    assertEquals("JS_RUNNING", store.get(jobId).orElseThrow().state);
  }

  @Test
  void applyLeaseOutcomeReturnsFalseForMissingJob() {
    assertFalse(
        store.applyLeaseOutcome(
            "missing-job",
            "missing-epoch",
            ReconcileJobStore.CompletionKind.SUCCEEDED,
            2_000L,
            "done",
            1L,
            1L,
            0L,
            0L,
            0L,
            0L,
            0L));
  }

  @Test
  void applyLeaseOutcomeReturnsFalseForTerminalJob() {
    String jobId = enqueueRoot();
    ReconcileJobStore.LeasedJob lease = store.leaseNext().orElseThrow();
    assertTrue(
        store.applyLeaseOutcome(
            jobId,
            lease.leaseEpoch,
            ReconcileJobStore.CompletionKind.SUCCEEDED,
            2_000L,
            "done",
            1L,
            1L,
            0L,
            0L,
            0L,
            0L,
            0L));

    assertFalse(
        store.applyLeaseOutcome(
            jobId,
            lease.leaseEpoch,
            ReconcileJobStore.CompletionKind.FAILED_TERMINAL,
            3_000L,
            "late failure",
            1L,
            1L,
            0L,
            0L,
            1L,
            0L,
            0L));

    assertEquals("JS_SUCCEEDED", store.get(jobId).orElseThrow().state);
  }

  @Test
  void applyLeaseOutcomeReturnsFalseForExpiredLease() {
    String jobId = enqueueRoot();
    ReconcileJobStore.LeasedJob lease = store.leaseNext().orElseThrow();
    expireLease(jobId);

    assertFalse(
        store.applyLeaseOutcome(
            jobId,
            lease.leaseEpoch,
            ReconcileJobStore.CompletionKind.SUCCEEDED,
            2_000L,
            "done",
            1L,
            1L,
            0L,
            0L,
            0L,
            0L,
            0L));

    assertEquals("JS_RUNNING", store.get(jobId).orElseThrow().state);
  }

  private String enqueueRoot() {
    return store.enqueue(
        ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, ReconcileScope.empty());
  }

  private void expireLease(String jobId) {
    String pointerKey = Keys.reconcileJobLeasePointerById(ACCOUNT_ID, jobId);
    Pointer pointer = store.pointerStore.get(pointerKey).orElseThrow();
    DurableReconcileJobStore.StoredJobLease lease = readStoredLease(pointer.getBlobUri());
    lease.expiresAtMs = System.currentTimeMillis() - 1L;
    Pointer expiredPointer =
        Pointer.newBuilder()
            .setKey(pointerKey)
            .setBlobUri(
                INLINE_JOB_LEASE_PREFIX
                    + java.util.Base64.getUrlEncoder()
                        .withoutPadding()
                        .encodeToString(
                            org.junit.jupiter.api.Assertions.assertDoesNotThrow(
                                () -> store.mapper.writeValueAsBytes(lease))))
            .setVersion(pointer.getVersion() + 1L)
            .build();
    assertTrue(store.pointerStore.compareAndSet(pointerKey, pointer.getVersion(), expiredPointer));
  }

  private DurableReconcileJobStore.StoredJobLease readStoredLease(String reference) {
    assertTrue(reference.startsWith(INLINE_JOB_LEASE_PREFIX));
    return org.junit.jupiter.api.Assertions.assertDoesNotThrow(
        () ->
            store.mapper.readValue(
                java.util.Base64.getUrlDecoder()
                    .decode(reference.substring(INLINE_JOB_LEASE_PREFIX.length())),
                DurableReconcileJobStore.StoredJobLease.class));
  }
}
