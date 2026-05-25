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

import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredJobLease;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.inmemory.InMemoryReconcileJobIndexStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.inmemory.InMemoryReconcileLeaseStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.inmemory.InMemoryReconcileReadyQueueStore;
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
import org.junit.jupiter.api.AfterEach;
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
  private DurableReconcileJobStore store;
  private DynamoDbClient dynamoDbClient;
  private DynamoDbAsyncClient dynamoDbAsyncClient;

  @BeforeEach
  void setUp() {
    store = new DurableReconcileJobStore();
    store.pointerStore = new InMemoryPointerStore();
    store.blobStore = new InMemoryBlobStore();
    store.mapper = new ObjectMapper();
    store.config = ConfigProvider.getConfig();
    if (isDynamoMode()) {
      dynamoDbClient = createDynamoDbClient();
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
          .bind(dynamoDbClient, store.kvTable);
      store.leaseBackend =
          new ai.floedb.floecat.service.reconciler.jobs.durable.store.DynamoReconcileLeaseBackend();
      ((ai.floedb.floecat.service.reconciler.jobs.durable.store.DynamoReconcileLeaseBackend)
              store.leaseBackend)
          .bind(dynamoDbClient, store.kvTable);
      store.readyQueueBackend =
          new ai.floedb.floecat.service.reconciler.jobs.durable.store
              .DynamoReconcileReadyQueueBackend();
      ((ai.floedb.floecat.service.reconciler.jobs.durable.store.DynamoReconcileReadyQueueBackend)
              store.readyQueueBackend)
          .bind(dynamoDbClient, store.kvTable);
    } else {
      store.jobIndexStore = new InMemoryReconcileJobIndexStore();
      store.leaseStore = new InMemoryReconcileLeaseStore();
      store.readyQueueStore = new InMemoryReconcileReadyQueueStore();
    }
    store.init();
  }

  @AfterEach
  void tearDown() {
    System.clearProperty("floecat.reconciler.job-store.lease-renew-grace-ms");
    if (dynamoDbClient != null) {
      dynamoDbClient.close();
      dynamoDbClient = null;
    }
    if (dynamoDbAsyncClient != null) {
      dynamoDbAsyncClient.close();
      dynamoDbAsyncClient = null;
    }
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
    assertTrue(store.leaseStore.loadLease(ACCOUNT_ID, jobId).isEmpty());
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
    assertTrue(store.leaseStore.loadLease(ACCOUNT_ID, jobId).isEmpty());
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
    System.setProperty("floecat.reconciler.job-store.lease-renew-grace-ms", "0");
    store.init();
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

  private boolean isDynamoMode() {
    return "dynamodb"
        .equalsIgnoreCase(
            store.config.getOptionalValue("floecat.kv", String.class).orElse("memory"));
  }

  private DynamoDbClient createDynamoDbClient() {
    String endpoint =
        store
            .config
            .getOptionalValue("floecat.storage.aws.dynamodb.endpoint-override", String.class)
            .orElse("http://localhost:4566");
    String region =
        store
            .config
            .getOptionalValue("floecat.storage.aws.region", String.class)
            .orElse("us-east-1");
    String accessKey =
        store
            .config
            .getOptionalValue("floecat.storage.aws.access-key-id", String.class)
            .orElse("test");
    String secretKey =
        store
            .config
            .getOptionalValue("floecat.storage.aws.secret-access-key", String.class)
            .orElse("test");
    return DynamoDbClient.builder()
        .endpointOverride(URI.create(endpoint))
        .region(Region.of(region))
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
        .build();
  }

  private PointerStore createDynamoPointerStore() {
    String endpoint =
        store
            .config
            .getOptionalValue("floecat.storage.aws.dynamodb.endpoint-override", String.class)
            .orElse("http://localhost:4566");
    String region =
        store
            .config
            .getOptionalValue("floecat.storage.aws.region", String.class)
            .orElse("us-east-1");
    String accessKey =
        store
            .config
            .getOptionalValue("floecat.storage.aws.access-key-id", String.class)
            .orElse("test");
    String secretKey =
        store
            .config
            .getOptionalValue("floecat.storage.aws.secret-access-key", String.class)
            .orElse("test");
    dynamoDbAsyncClient =
        DynamoDbAsyncClient.builder()
            .endpointOverride(URI.create(endpoint))
            .region(Region.of(region))
            .credentialsProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
            .build();
    return new DynamoPointerStore(
        new PointerStoreEntity(new DynamoDbKvStore(dynamoDbAsyncClient, store.kvTable)));
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
      var response = dynamoDbClient.scan(request.build());
      for (var item : response.items()) {
        dynamoDbClient.deleteItem(
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
}
