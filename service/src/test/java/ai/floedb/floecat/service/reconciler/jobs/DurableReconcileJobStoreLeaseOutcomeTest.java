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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredJobLease;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.inmemory.InMemoryReconcileJobIndexStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.inmemory.InMemoryReconcileLeaseStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.inmemory.InMemoryReconcileReadyQueueStore;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.aws.dynamodb.DynamoPointerStore;
import ai.floedb.floecat.storage.kv.dynamodb.DynamoDbKvStore;
import ai.floedb.floecat.storage.kv.dynamodb.ps.PointerStoreEntity;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.util.Map;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

class DurableReconcileJobStoreLeaseOutcomeTest {
  private static final String ACCOUNT_ID = "acct-1";
  private static final String CONNECTOR_ID = "conn-1";
  private static DynamoDbClient sharedDynamoDbClient;
  private static DynamoDbAsyncClient sharedDynamoDbAsyncClient;
  private DurableReconcileJobStore store;

  @BeforeAll
  static void setUpSharedDynamoClients() {
    if (!isDynamoMode()) {
      return;
    }
    sharedDynamoDbClient = createDynamoDbClientStatic();
    sharedDynamoDbAsyncClient = createDynamoDbAsyncClientStatic();
  }

  @AfterAll
  static void tearDownSharedDynamoClients() {
    if (sharedDynamoDbClient != null) {
      sharedDynamoDbClient.close();
      sharedDynamoDbClient = null;
    }
    if (sharedDynamoDbAsyncClient != null) {
      sharedDynamoDbAsyncClient.close();
      sharedDynamoDbAsyncClient = null;
    }
  }

  @BeforeEach
  void setUp() {
    store = new DurableReconcileJobStore();
    store.pointerStore = new InMemoryPointerStore();
    store.blobStore = new InMemoryBlobStore();
    store.mapper = new ObjectMapper();
    store.config = ConfigProvider.getConfig();
    if (isDynamoMode()) {
      ensureSharedDynamoClients();
      clearDynamoTable();
      store.kvTable =
          store
              .config
              .getOptionalValue("floecat.kv.table", String.class)
              .orElse("floecat_pointers");
      store.pointerStore = createDynamoPointerStore();
      store.jobIndexBackend =
          new ai.floedb.floecat.service.reconciler.jobs.durable.store
              .DynamoReconcileJobIndexBackend();
      ((ai.floedb.floecat.service.reconciler.jobs.durable.store.DynamoReconcileJobIndexBackend)
              store.jobIndexBackend)
          .bind(sharedDynamoDbClient, store.kvTable);
      store.leaseBackend =
          new ai.floedb.floecat.service.reconciler.jobs.durable.store.DynamoReconcileLeaseBackend();
      ((ai.floedb.floecat.service.reconciler.jobs.durable.store.DynamoReconcileLeaseBackend)
              store.leaseBackend)
          .bind(sharedDynamoDbClient, store.kvTable);
      store.readyQueueBackend =
          new ai.floedb.floecat.service.reconciler.jobs.durable.store
              .DynamoReconcileReadyQueueBackend();
      ((ai.floedb.floecat.service.reconciler.jobs.durable.store.DynamoReconcileReadyQueueBackend)
              store.readyQueueBackend)
          .bind(sharedDynamoDbClient, store.kvTable);
    } else {
      store.jobIndexStore = new InMemoryReconcileJobIndexStore();
      store.leaseStore = new InMemoryReconcileLeaseStore();
      store.readyQueueStore = new InMemoryReconcileReadyQueueStore();
    }
    store.init();
  }

  @AfterEach
  void tearDown() {}

  @Test
  void applyLeaseOutcomeReturnsTrueForAcceptedTransitions() {
    String succeededJobId = enqueueRoot();
    ReconcileJobStore.LeasedJob succeededLease = awaitLease("succeeded job lease", succeededJobId);
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
    assertEquals(
        "JS_SUCCEEDED",
        waitForValue(
                () -> store.getLeaseView(succeededJobId).orElseThrow(),
                current -> "JS_SUCCEEDED".equals(current.state),
                "succeeded lease outcome canonical view")
            .state);

    String cancelledJobId = enqueueRoot();
    ReconcileJobStore.LeasedJob cancelledLease = awaitLease("cancelled job lease", cancelledJobId);
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
    assertEquals(
        "JS_CANCELLED",
        waitForValue(
                () -> store.getLeaseView(cancelledJobId).orElseThrow(),
                current -> "JS_CANCELLED".equals(current.state),
                "cancelled lease outcome canonical view")
            .state);
  }

  @Test
  void applyLeaseOutcomeCancellingSuccessResolvesImmediatelyToCancelled() {
    String jobId = enqueueRoot();
    ReconcileJobStore.LeasedJob lease = awaitLease("cancelling success lease", jobId);
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

    ReconcileJobStore.ReconcileJob job =
        waitForValue(
            () -> store.getLeaseView(jobId).orElseThrow(),
            current -> "JS_CANCELLED".equals(current.state) && current.finishedAtMs == 4_000L,
            "cancelling success resolves to cancelled");
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
    assertTrue(store.leaseStore.loadLease(ACCOUNT_ID, jobId).isEmpty());
  }

  @Test
  void applyLeaseOutcomeCancellingFailureResolvesImmediatelyToCancelled() {
    String jobId = enqueueRoot();
    ReconcileJobStore.LeasedJob lease = awaitLease("cancelling failure lease", jobId);
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

    ReconcileJobStore.ReconcileJob job =
        waitForValue(
            () -> store.getLeaseView(jobId).orElseThrow(),
            current -> "JS_CANCELLED".equals(current.state) && current.finishedAtMs == 5_000L,
            "cancelling failure resolves to cancelled");
    assertEquals("JS_CANCELLED", job.state);
    assertEquals("stop", job.message);
    assertEquals(5_000L, job.finishedAtMs);
    assertTrue(store.leaseStore.loadLease(ACCOUNT_ID, jobId).isEmpty());
  }

  @Test
  void applyLeaseOutcomeReturnsFalseForStaleLeaseEpoch() {
    String jobId = enqueueRoot();
    awaitLease("stale lease epoch lease", jobId);

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

    assertEquals(
        "JS_RUNNING",
        waitForValue(
                () -> store.getLeaseView(jobId).orElseThrow(),
                current -> "JS_RUNNING".equals(current.state),
                "stale lease epoch leaves canonical state running")
            .state);
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
    ReconcileJobStore.LeasedJob lease = awaitLease("terminal job lease", jobId);
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

    assertEquals(
        "JS_SUCCEEDED",
        waitForValue(
                () -> store.getLeaseView(jobId).orElseThrow(),
                current -> "JS_SUCCEEDED".equals(current.state),
                "terminal lease outcome remains canonically succeeded")
            .state);
  }

  @Test
  void applyLeaseOutcomeReturnsTrueForExpiredLeaseWhenEpochStillMatches() {
    configureLeaseRenewGraceMs(0L);
    String jobId = enqueueRoot();
    ReconcileJobStore.LeasedJob lease = awaitLease("expired lease", jobId);
    expireLease(jobId);

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

    assertEquals(
        "JS_SUCCEEDED",
        waitForValue(
                () -> store.getLeaseView(jobId).orElseThrow(),
                current -> "JS_SUCCEEDED".equals(current.state),
                "expired lease outcome resolves when epoch still matches")
            .state);
  }

  @Test
  void renewLeaseReturnsTrueForExpiredLeaseWhenEpochStillMatches() {
    configureLeaseRenewGraceMs(0L);
    String jobId = enqueueRoot();
    ReconcileJobStore.LeasedJob lease = awaitLease("expired renew lease", jobId);
    expireLease(jobId);

    assertTrue(store.renewLease(jobId, lease.leaseEpoch));
    assertTrue(readStoredLease(ACCOUNT_ID, jobId).expiresAtMs > System.currentTimeMillis());
  }

  private String enqueueRoot() {
    return store.enqueue(
        ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, ReconcileScope.empty());
  }

  private ReconcileJobStore.LeasedJob awaitLease(String description) {
    return waitForValue(() -> store.leaseNext().orElse(null), lease -> lease != null, description);
  }

  private ReconcileJobStore.LeasedJob awaitLease(String description, String jobId) {
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    ReconcileJobStore.LeasedJob leased = null;
    for (int attempt = 0; attempt < 100 && leased == null; attempt++) {
      StoredReconcileJob readyRecord =
          waitForValue(
              () -> readStoredRecord(canonicalPointerKey),
              current ->
                  "JS_QUEUED".equals(current.state)
                      && current.readyPointerKey != null
                      && !current.readyPointerKey.isBlank(),
              description + " ready");
      leased =
          tryGetValue(
              () ->
                  store
                      .leaseStore
                      .leaseCanonical(
                          canonicalPointerKey,
                          readyRecord.readyPointerKey,
                          System.currentTimeMillis(),
                          store
                              .jobIndexStore
                              .loadCanonicalSnapshot(canonicalPointerKey)
                              .orElseThrow(),
                          readyRecord)
                      .orElse(null));
      if (leased == null) {
        store.runMaintenanceOnce(isDynamoMode() ? 10_000L : 100L);
      }
    }
    assertNotNull(leased, "Timed out waiting for " + description);
    return leased;
  }

  private <T> T waitForValue(
      java.util.function.Supplier<T> supplier,
      java.util.function.Predicate<T> done,
      String description) {
    T value = tryGetValue(supplier);
    int maxAttempts = isDynamoMode() ? 1200 : 100;
    long sleepMs = isDynamoMode() ? 25L : 0L;
    for (int attempt = 0; attempt < maxAttempts; attempt++) {
      if (value != null && done.test(value)) {
        return value;
      }
      try {
        Thread.sleep(sleepMs);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted while waiting for " + description, ie);
      }
      value = tryGetValue(supplier);
    }
    assertTrue(
        value != null && done.test(value),
        "Timed out waiting for " + description + "; last value=" + value);
    return value;
  }

  private <T> T tryGetValue(java.util.function.Supplier<T> supplier) {
    try {
      return supplier.get();
    } catch (IllegalStateException | java.util.NoSuchElementException e) {
      return null;
    }
  }

  private void expireLease(String jobId) {
    assertTrue(
        store
            .leaseStore
            .mutateLease(
                ACCOUNT_ID,
                jobId,
                lease -> {
                  lease.expiresAtMs = System.currentTimeMillis() - 1L;
                  return lease;
                })
            .isPresent());
  }

  private StoredJobLease readStoredLease(String accountId, String jobId) {
    return org.junit.jupiter.api.Assertions.assertDoesNotThrow(
            () -> store.leaseStore.loadLease(accountId, jobId))
        .orElseThrow();
  }

  private void configureLeaseRenewGraceMs(long leaseRenewGraceMs) {
    org.junit.jupiter.api.Assertions.assertDoesNotThrow(
        () -> setPrivateField(store, "leaseRenewGraceMs", Math.max(0L, leaseRenewGraceMs)));
  }

  private void setPrivateField(Object target, String name, Object value) throws Exception {
    java.lang.reflect.Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }

  private static boolean isDynamoMode() {
    return "dynamodb"
        .equalsIgnoreCase(
            ConfigProvider.getConfig()
                .getOptionalValue("floecat.kv", String.class)
                .orElse("memory"));
  }

  private static void ensureSharedDynamoClients() {
    if (sharedDynamoDbClient == null) {
      sharedDynamoDbClient = createDynamoDbClientStatic();
    }
    if (sharedDynamoDbAsyncClient == null) {
      sharedDynamoDbAsyncClient = createDynamoDbAsyncClientStatic();
    }
  }

  private static DynamoDbClient createDynamoDbClientStatic() {
    String endpoint =
        ConfigProvider.getConfig()
            .getOptionalValue("floecat.storage.aws.dynamodb.endpoint-override", String.class)
            .orElse("http://localhost:4566");
    String region =
        ConfigProvider.getConfig()
            .getOptionalValue("floecat.storage.aws.region", String.class)
            .orElse("us-east-1");
    String accessKey =
        ConfigProvider.getConfig()
            .getOptionalValue("floecat.storage.aws.access-key-id", String.class)
            .orElse("test");
    String secretKey =
        ConfigProvider.getConfig()
            .getOptionalValue("floecat.storage.aws.secret-access-key", String.class)
            .orElse("test");
    return DynamoDbClient.builder()
        .endpointOverride(URI.create(endpoint))
        .region(Region.of(region))
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
        .build();
  }

  private static DynamoDbAsyncClient createDynamoDbAsyncClientStatic() {
    String endpoint =
        ConfigProvider.getConfig()
            .getOptionalValue("floecat.storage.aws.dynamodb.endpoint-override", String.class)
            .orElse("http://localhost:4566");
    String region =
        ConfigProvider.getConfig()
            .getOptionalValue("floecat.storage.aws.region", String.class)
            .orElse("us-east-1");
    String accessKey =
        ConfigProvider.getConfig()
            .getOptionalValue("floecat.storage.aws.access-key-id", String.class)
            .orElse("test");
    String secretKey =
        ConfigProvider.getConfig()
            .getOptionalValue("floecat.storage.aws.secret-access-key", String.class)
            .orElse("test");
    return DynamoDbAsyncClient.builder()
        .endpointOverride(URI.create(endpoint))
        .region(Region.of(region))
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
        .build();
  }

  private PointerStore createDynamoPointerStore() {
    ensureSharedDynamoClients();
    return new DynamoPointerStore(
        new PointerStoreEntity(new DynamoDbKvStore(sharedDynamoDbAsyncClient, store.kvTable)));
  }

  private void clearDynamoTable() {
    String table =
        store.config.getOptionalValue("floecat.kv.table", String.class).orElse("floecat_pointers");
    Map<String, AttributeValue> startKey = null;
    do {
      var request = ScanRequest.builder().tableName(table);
      if (startKey != null && !startKey.isEmpty()) {
        request.exclusiveStartKey(startKey);
      }
      var response = sharedDynamoDbClient.scan(request.build());
      for (var item : response.items()) {
        sharedDynamoDbClient.deleteItem(
            DeleteItemRequest.builder()
                .tableName(table)
                .key(
                    Map.of(
                        "pk", item.get("pk"),
                        "sk", item.get("sk")))
                .build());
      }
      startKey = response.lastEvaluatedKey();
    } while (startKey != null && !startKey.isEmpty());
  }

  private StoredReconcileJob readStoredRecord(String canonicalPointerKey) {
    return org.junit.jupiter.api.Assertions.assertDoesNotThrow(
            () -> store.jobIndexStore.readCanonicalRecordByKey(canonicalPointerKey))
        .orElseThrow();
  }
}
