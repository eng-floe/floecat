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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.ReconcileJob;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Method;
import java.util.List;
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

    String first = store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, scope);
    String second = store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, scope);

    assertEquals(first, second);
  }

  @Test
  void successfulJobClearsDedupeAndAllowsFreshEnqueue() {
    store.init();
    ReconcileScope scope = ReconcileScope.of(List.of(List.of("ns")), "tbl", List.of());
    String first = store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, scope);

    var leased = store.leaseNext();
    assertTrue(leased.isPresent());
    store.markSucceeded(first, System.currentTimeMillis(), 10, 2);

    String second = store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, scope);
    assertNotEquals(first, second);
  }

  @Test
  void markFailedRequeuesAndEventuallyTransitionsToFailed() {
    System.setProperty("floecat.reconciler.job-store.max-attempts", "2");
    System.setProperty("floecat.reconciler.job-store.base-backoff-ms", "100");
    System.setProperty("floecat.reconciler.job-store.max-backoff-ms", "100");
    store.init();

    String jobId = store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, ReconcileScope.empty());
    assertTrue(store.leaseNext().isPresent());

    store.markFailed(jobId, System.currentTimeMillis(), "transient", 1, 0, 1);
    ReconcileJob retried = store.get(jobId).orElseThrow();
    assertEquals("JS_QUEUED", retried.state);

    store.markFailed(jobId, System.currentTimeMillis(), "terminal", 1, 0, 2);
    ReconcileJob failed = store.get(jobId).orElseThrow();
    assertEquals("JS_FAILED", failed.state);
  }

  @Test
  void expiredLeasesAreReclaimedAndRequeued() throws Exception {
    System.setProperty("floecat.reconciler.job-store.lease-ms", "1000");
    System.setProperty("floecat.reconciler.job-store.reclaim-interval-ms", "1000");
    store.init();

    String jobId = store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, ReconcileScope.empty());
    var firstLease = store.leaseNext();
    assertTrue(firstLease.isPresent());
    assertEquals(jobId, firstLease.get().jobId);

    Thread.sleep(1150L);

    var secondLease = store.leaseNext();
    assertTrue(secondLease.isPresent());
    assertEquals(jobId, secondLease.get().jobId);
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
}
