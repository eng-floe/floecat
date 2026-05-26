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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.common.rpc.BlobHeader;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.reconciler.impl.PlannedFileGroupJob;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.impl.SnapshotPlanBlobStore.SnapshotPlanBlob;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionClass;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileResult;
import ai.floedb.floecat.reconciler.jobs.ReconcileIndexArtifactResult;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.ReconcileJob;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import ai.floedb.floecat.reconciler.jobs.SnapshotPlanManifestIds;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredJobLease;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobListSummary;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobProjection;
import ai.floedb.floecat.service.reconciler.jobs.durable.projection.ReconcileJobProjectionStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.projection.ReconcileJobRootSummaryStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcilePayloadStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileLeaseStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileReadyQueueBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileReadyQueueStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.inmemory.InMemoryReconcileJobIndexStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.inmemory.InMemoryReconcileLeaseStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.inmemory.InMemoryReconcileReadyQueueStore;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.aws.dynamodb.DynamoPointerStore;
import ai.floedb.floecat.storage.errors.StorageNotFoundException;
import ai.floedb.floecat.storage.kv.dynamodb.DynamoDbKvStore;
import ai.floedb.floecat.storage.kv.dynamodb.ps.PointerStoreEntity;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
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

class DurableReconcileJobStoreTest {
  private static final String ACCOUNT_ID = "acct-1";
  private static final String CONNECTOR_ID = "conn-1";
  private static final String LEASE_EXPIRY_POINTER_PREFIX =
      "/accounts/by-id/reconcile/job-leases/by-expiry/";
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
      store.pointerStore = new InMemoryPointerStore();
      store.jobIndexStore = new InMemoryReconcileJobIndexStore();
      store.leaseStore = new InMemoryReconcileLeaseStore();
      store.readyQueueStore = new InMemoryReconcileReadyQueueStore();
    }
  }

  @AfterEach
  void tearDown() {
    System.clearProperty("floecat.reconciler.job-store.max-attempts");
    System.clearProperty("floecat.reconciler.job-store.base-backoff-ms");
    System.clearProperty("floecat.reconciler.job-store.max-backoff-ms");
    System.clearProperty("floecat.reconciler.job-store.lease-ms");
    System.clearProperty("floecat.reconciler.job-store.lease-renew-grace-ms");
    System.clearProperty("floecat.reconciler.job-store.reclaim-interval-ms");
    System.clearProperty("floecat.reconciler.job-store.ready-scan-limit");
  }

  private void assumeMemoryOnly(String reason) {
    Assumptions.assumeFalse(isDynamoMode(), reason);
  }

  @Test
  void enqueueDedupesWhileJobIsActive() {
    store.init();
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String first =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);
    String second =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);

    assertEquals(first, second);
  }

  @Test
  void childJobsPagePaginatesParentIndex() {
    assumeMemoryOnly("legacy pointer pagination assertions are only meaningful in memory mode");
    store.pointerStore = new SinglePointerPageStore();
    store.init();

    String parentJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    String childOne =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("src.ns", "orders", "orders-table-id", "orders"),
            ReconcileExecutionPolicy.defaults(),
            parentJobId,
            "");
    String childTwo =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("src.ns", "customers", "customers-table-id", "customers"),
            ReconcileExecutionPolicy.defaults(),
            parentJobId,
            "");

    var first = store.childJobsPage(ACCOUNT_ID, parentJobId, 1, "");
    var second = store.childJobsPage(ACCOUNT_ID, parentJobId, 1, first.nextPageToken);

    assertEquals(1, first.jobs.size());
    assertEquals(1, second.jobs.size());
    assertTrue(
        first.jobs.get(0).jobId.equals(childOne) || first.jobs.get(0).jobId.equals(childTwo));
    assertNotEquals(first.jobs.get(0).jobId, second.jobs.get(0).jobId);
    assertTrue(second.nextPageToken.isBlank());
  }

  @Test
  void childJobsPageReturnsExecFileGroupIdentityWithoutPlannedPaths() {
    store.init();
    ReconcileScope scope =
        ReconcileScope.of(
            List.of(),
            "table-1",
            List.of(scopedCaptureRequest("table-1", 7L, "orders", List.of("c1"))));

    String parentJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    String childJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            scope,
            ReconcileJobKind.EXEC_FILE_GROUP,
            ReconcileTableTask.empty(),
            ReconcileViewTask.empty(),
            ReconcileSnapshotTask.empty(),
            ReconcileFileGroupTask.of(
                "plan-1",
                "group-1",
                "table-1",
                7L,
                List.of("s3://bucket/data/file-1.parquet", "s3://bucket/data/file-2.parquet")),
            ReconcileExecutionPolicy.defaults(),
            parentJobId,
            "");

    var page = store.childJobsPage(ACCOUNT_ID, parentJobId, 10, "");

    assertEquals(1, page.jobs.size());
    var child = page.jobs.getFirst();
    assertEquals(childJobId, child.jobId);
    assertEquals(ReconcileJobKind.EXEC_FILE_GROUP, child.jobKind);
    assertEquals("plan-1", child.fileGroupTask.planId());
    assertEquals("group-1", child.fileGroupTask.groupId());
    assertEquals("table-1", child.fileGroupTask.tableId());
    assertEquals(7L, child.fileGroupTask.snapshotId());
    assertEquals(2, child.fileGroupTask.fileCount());
    assertEquals(scope.destinationTableId(), child.scope.destinationTableId());
    assertEquals(scope.destinationCaptureRequests(), child.scope.destinationCaptureRequests());
    assertEquals(List.of(), child.fileGroupTask.filePaths());
    assertTrue(page.nextPageToken.isBlank());
  }

  @Test
  void enqueueStoresOnlyDedupeKeyHashOnCanonicalRecord() {
    assumeMemoryOnly("pointer-backed dedupe assertions are only meaningful in memory mode");
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl"),
            ReconcileJobKind.EXEC_FILE_GROUP,
            ReconcileTableTask.empty(),
            ReconcileViewTask.empty(),
            ReconcileSnapshotTask.empty(),
            ReconcileFileGroupTask.of(
                "plan-1",
                "group-1",
                "table-1",
                7L,
                List.of("s3://bucket/path-1.parquet", "s3://bucket/path-2.parquet")),
            ReconcileExecutionPolicy.defaults(),
            "",
            "");

    StoredReconcileJob record = readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, jobId));

    assertFalse(record.dedupeKeyHash.isBlank());
    assertEquals(
        Keys.reconcileDedupePointer(ACCOUNT_ID, record.dedupeKeyHash),
        firstPointerWithPrefix(Keys.reconcileDedupePointerPrefix(ACCOUNT_ID))
            .orElseThrow()
            .getKey());
  }

  @Test
  void enqueueLookupFailureCleansCanonicalRows() {
    assumeMemoryOnly("pointer-store batch fault injection is only meaningful in memory mode");
    store = new DurableReconcileJobStore();
    store.pointerStore =
        new BatchFailingPointerStore(Keys.reconcileJobLookupPointerByIdPrefix(), Integer.MAX_VALUE);
    store.blobStore = new InMemoryBlobStore();
    store.mapper = new ObjectMapper();
    store.config = ConfigProvider.getConfig();
    store.init();

    assertThrows(
        IllegalStateException.class,
        () ->
            store.enqueue(
                ACCOUNT_ID,
                CONNECTOR_ID,
                false,
                CaptureMode.METADATA_AND_CAPTURE,
                ReconcileScope.of(List.of(), "tbl")));

    assertEquals(
        0, store.pointerStore.countByPrefix(Keys.reconcileJobStateRowByIdPrefix(ACCOUNT_ID)));
    assertEquals(
        0, store.pointerStore.countByPrefix(Keys.reconcileDedupePointerPrefix(ACCOUNT_ID)));
  }

  @Test
  void enqueueConnectorIndexFailureCleansCanonicalRows() {
    assumeMemoryOnly("pointer-store batch fault injection is only meaningful in memory mode");
    store = new DurableReconcileJobStore();
    store.pointerStore =
        new BatchFailingPointerStore(
            Keys.reconcileJobByConnectorPointerPrefix(ACCOUNT_ID, CONNECTOR_ID), Integer.MAX_VALUE);
    store.blobStore = new InMemoryBlobStore();
    store.mapper = new ObjectMapper();
    store.config = ConfigProvider.getConfig();
    store.init();

    assertThrows(
        IllegalStateException.class,
        () ->
            store.enqueue(
                ACCOUNT_ID,
                CONNECTOR_ID,
                false,
                CaptureMode.METADATA_AND_CAPTURE,
                ReconcileScope.of(List.of(), "tbl")));

    assertEquals(
        0, store.pointerStore.countByPrefix(Keys.reconcileJobStateRowByIdPrefix(ACCOUNT_ID)));
    assertEquals(
        0,
        store.pointerStore.countByPrefix(
            Keys.reconcileJobByConnectorPointerPrefix(ACCOUNT_ID, CONNECTOR_ID)));
    assertEquals(
        0, store.pointerStore.countByPrefix(Keys.reconcileDedupePointerPrefix(ACCOUNT_ID)));
  }

  @Test
  void enqueueDoesNotDedupeAcrossDifferentExecutionPolicies() {
    store.init();
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String first =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            scope,
            ReconcileExecutionPolicy.of(ReconcileExecutionClass.DEFAULT, "", java.util.Map.of()),
            "");
    String second =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            scope,
            ReconcileExecutionPolicy.of(
                ReconcileExecutionClass.HEAVY, "remote", java.util.Map.of()),
            "");

    assertNotEquals(first, second);
  }

  @Test
  void childJobsUsesParentIndex() {
    store.init();

    String planJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    String childJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("src.ns", "orders", "orders-table-id", "orders"),
            ReconcileExecutionPolicy.defaults(),
            planJobId,
            "");
    store.enqueue(
        ACCOUNT_ID,
        CONNECTOR_ID,
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.empty(),
        ReconcileJobKind.PLAN_TABLE,
        ReconcileTableTask.of("src.ns", "customers", "customers-table-id", "customers"),
        ReconcileExecutionPolicy.defaults(),
        "other-plan",
        "");

    var children = listChildJobs(ACCOUNT_ID, planJobId);

    assertEquals(1, children.size());
    assertEquals(childJobId, children.get(0).jobId);
    assertEquals(planJobId, children.get(0).parentJobId);
  }

  @Test
  void bulkEnqueueCreatesCanonicalLookupParentReadyAndConnectorRowsForEachJob() {
    assumeMemoryOnly("pointer row materialization assertions are only meaningful in memory mode");
    CountingBlobStore blobStore = new CountingBlobStore();
    store.blobStore = blobStore;
    store.init();

    var result =
        store.bulkEnqueue(
            List.of(
                fileGroupSpec("parent-1", "group-1", "s3://bucket/table-1/file-1.parquet"),
                fileGroupSpec("parent-1", "group-2", "s3://bucket/table-1/file-2.parquet")));

    result.requireAllSucceeded("bulk test");
    assertEquals(2, result.items.size());
    assertTrue(result.items.stream().allMatch(item -> item.created));

    for (var item : result.items) {
      String canonicalKey = Keys.reconcileJobPointerById(ACCOUNT_ID, item.jobId);
      var record = readStoredRecord(canonicalKey);
      assertEquals(item.jobId, record.jobId);
      assertTrue(readyEntryExists(record.readyPointerKey));
      assertEquals(
          canonicalKey,
          store
              .pointerStore
              .get(Keys.reconcileJobLookupPointerById(item.jobId))
              .orElseThrow()
              .getBlobUri());
      assertEquals(
          canonicalKey,
          store
              .pointerStore
              .get(Keys.reconcileJobByParentPointer(ACCOUNT_ID, "parent-1", item.jobId))
              .orElseThrow()
              .getBlobUri());
      assertEquals(
          canonicalKey,
          store.pointerStore.get(record.connectorIndexPointerKey).orElseThrow().getBlobUri());
    }

    var children = listChildJobs(ACCOUNT_ID, "parent-1");
    assertEquals(2, children.size());

    blobStore.resetGetCount();
    var page = store.list(ACCOUNT_ID, 10, "", CONNECTOR_ID, java.util.Set.of());
    assertEquals(2, page.jobs.size());
    assertEquals(0, blobStore.getCount());

    var firstLease = store.leaseNext().orElseThrow();
    var secondLease = store.leaseNext().orElseThrow();
    assertEquals(ReconcileJobKind.EXEC_FILE_GROUP, firstLease.jobKind);
    assertEquals(ReconcileJobKind.EXEC_FILE_GROUP, secondLease.jobKind);
    assertNotEquals(firstLease.jobId, secondLease.jobId);
  }

  @Test
  void bulkEnqueuePreservesNewestFirstConnectorHistorySemantics() {
    store.init();

    var result =
        store.bulkEnqueue(
            List.of(
                fileGroupSpec("parent-1", "group-1", "s3://bucket/table-1/file-1.parquet"),
                fileGroupSpec("parent-1", "group-2", "s3://bucket/table-1/file-2.parquet"),
                fileGroupSpec("parent-1", "group-3", "s3://bucket/table-1/file-3.parquet")));

    result.requireAllSucceeded("bulk order test");

    var page = store.list(ACCOUNT_ID, 10, "", CONNECTOR_ID, java.util.Set.of());

    assertEquals(
        List.of(result.items.get(2).jobId, result.items.get(1).jobId, result.items.get(0).jobId),
        page.jobs.stream().map(job -> job.jobId).toList());
  }

  @Test
  void bulkEnqueuePartialFailureRollsBackIncompleteJobsCleanly() {
    Assumptions.assumeFalse(
        isDynamoMode(), "pointer-store batch fault injection is only meaningful in memory mode");
    TrackingBlobStore blobStore = new TrackingBlobStore();
    store.pointerStore =
        new NthAndSubsequentPrefixFailingPointerStore(
            Keys.reconcileReadyByJobKindPointerPrefix(ReconcileJobKind.EXEC_FILE_GROUP.name()), 2);
    store.blobStore = blobStore;
    store.init();

    var result =
        store.bulkEnqueue(
            List.of(
                fileGroupSpec("parent-1", "group-1", "s3://bucket/table-1/file-1.parquet"),
                fileGroupSpec("parent-1", "group-2", "s3://bucket/table-1/file-2.parquet")));

    assertEquals(2, result.items.size());
    assertTrue(result.items.get(0).succeeded());
    assertFalse(result.items.get(1).succeeded());
    assertEquals(
        1, store.pointerStore.countByPrefix(Keys.reconcileJobStateRowByIdPrefix(ACCOUNT_ID)));
    assertEquals(
        1, store.pointerStore.countByPrefix(Keys.reconcileDedupePointerPrefix(ACCOUNT_ID)));
    StoredReconcileJob stored =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, result.items.get(0).jobId));
    assertTrue(readyEntryExists(stored.readyPointerKey));
    assertEquals(
        1,
        store.pointerStore.countByPrefix(
            Keys.reconcileJobByConnectorPointerPrefix(ACCOUNT_ID, CONNECTOR_ID)));
    assertEquals(1, listChildJobs(ACCOUNT_ID, "parent-1").size());
    assertEquals(
        0, blobStore.activeBlobCount(), "inline job definitions should not leave payload blobs");
  }

  @Test
  void bulkEnqueueRollbackDeletesFileGroupResultBlob() {
    Assumptions.assumeFalse(
        isDynamoMode(), "pointer-store batch fault injection is only meaningful in memory mode");
    TrackingBlobStore blobStore = new TrackingBlobStore();
    store.pointerStore =
        new BatchFailingPointerStore(Keys.reconcileReadyPointerPrefix(), Integer.MAX_VALUE);
    store.blobStore = blobStore;
    store.init();

    var result =
        store.bulkEnqueue(
            List.of(
                fileGroupResultSpec(
                    "parent-1",
                    "group-1",
                    "s3://bucket/table-1/file-1.parquet",
                    ReconcileFileResult.succeeded(
                        "s3://bucket/table-1/file-1.parquet",
                        128L,
                        ReconcileIndexArtifactResult.of(
                            "s3://bucket/table-1/file-1.stats", "application/json", 1)))));

    assertTrue(result.hasFailures());
    assertEquals(
        0, store.pointerStore.countByPrefix(Keys.reconcileJobStateRowByIdPrefix(ACCOUNT_ID)));
    assertEquals(
        0, store.pointerStore.countByPrefix(Keys.reconcileDedupePointerPrefix(ACCOUNT_ID)));
    assertEquals(
        0,
        store.pointerStore.countByPrefix(
            "/accounts/" + ACCOUNT_ID + "/reconcile/job-results/by-id/"));
    assertEquals(0, blobStore.activeBlobCount(), "rollback should delete the result blob as well");
  }

  @Test
  void bulkEnqueueDedupesMatchingJobsWithinSameBatch() {
    assumeMemoryOnly("pointer-backed batch dedupe assertions are only meaningful in memory mode");
    TrackingBlobStore blobStore = new TrackingBlobStore();
    store.blobStore = blobStore;
    store.init();

    ReconcileJobStore.BulkEnqueueSpec spec =
        fileGroupSpec("parent-1", "group-1", "s3://bucket/table-1/file-1.parquet");
    var result = store.bulkEnqueue(List.of(spec, spec));

    result.requireAllSucceeded("bulk dedupe test");
    assertTrue(result.items.get(0).created);
    assertFalse(result.items.get(1).created);
    assertEquals(result.items.get(0).jobId, result.items.get(1).jobId);
    assertEquals(
        1, store.pointerStore.countByPrefix(Keys.reconcileJobStateRowByIdPrefix(ACCOUNT_ID)));
    assertEquals(1, listChildJobs(ACCOUNT_ID, "parent-1").size());
    assertEquals(
        0, blobStore.activeBlobCount(), "inline job definitions should not leave payload blobs");
  }

  @Test
  void bulkEnqueueAccumulatesExpectedAndQueuedChildJobsAcrossParentBatches() {
    store.init();

    String parentJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "events"),
            ReconcileExecutionPolicy.defaults(),
            "table-parent-1",
            "");

    var result =
        store.bulkEnqueue(
            List.of(
                fileGroupSpec(parentJobId, "group-1", "s3://bucket/table-1/file-1.parquet"),
                fileGroupSpec(parentJobId, "group-2", "s3://bucket/table-1/file-2.parquet"),
                fileGroupSpec(parentJobId, "group-3", "s3://bucket/table-1/file-3.parquet"),
                fileGroupSpec(parentJobId, "group-4", "s3://bucket/table-1/file-4.parquet"),
                fileGroupSpec(parentJobId, "group-5", "s3://bucket/table-1/file-5.parquet"),
                fileGroupSpec(parentJobId, "group-6", "s3://bucket/table-1/file-6.parquet"),
                fileGroupSpec(parentJobId, "group-7", "s3://bucket/table-1/file-7.parquet"),
                fileGroupSpec(parentJobId, "group-8", "s3://bucket/table-1/file-8.parquet"),
                fileGroupSpec(parentJobId, "group-9", "s3://bucket/table-1/file-9.parquet")));

    result.requireAllSucceeded("bulk expected child count");

    ReconcileJobStore.ReconcileJobPage children =
        waitForValue(
            () -> store.childJobsPage(ACCOUNT_ID, parentJobId, 20, ""),
            page -> page.jobs.size() == 9,
            "snapshot child jobs after bulk enqueue");
    assertEquals(9, children.jobs.size());
    assertTrue(children.jobs.stream().allMatch(job -> "JS_QUEUED".equals(job.state)));
  }

  @Test
  void bulkEnqueueUsesTaskIdentityForSparseScopePlanTableLaneKeys() {
    store.init();

    String connectorJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            true,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());

    ReconcileJobStore.BulkEnqueueResult result =
        store.bulkEnqueue(
            List.of(
                ReconcileJobStore.BulkEnqueueSpec.of(
                    ACCOUNT_ID,
                    CONNECTOR_ID,
                    true,
                    CaptureMode.METADATA_AND_CAPTURE,
                    ReconcileScope.of(List.of("ns"), ""),
                    ReconcileJobKind.PLAN_TABLE,
                    ReconcileTableTask.of("db", "orders", "", ""),
                    ReconcileViewTask.empty(),
                    ReconcileSnapshotTask.empty(),
                    ReconcileFileGroupTask.empty(),
                    ReconcileExecutionPolicy.defaults(),
                    connectorJobId,
                    ""),
                ReconcileJobStore.BulkEnqueueSpec.of(
                    ACCOUNT_ID,
                    CONNECTOR_ID,
                    true,
                    CaptureMode.METADATA_AND_CAPTURE,
                    ReconcileScope.of(List.of("ns"), ""),
                    ReconcileJobKind.PLAN_TABLE,
                    ReconcileTableTask.of("db", "customers", "", ""),
                    ReconcileViewTask.empty(),
                    ReconcileSnapshotTask.empty(),
                    ReconcileFileGroupTask.empty(),
                    ReconcileExecutionPolicy.defaults(),
                    connectorJobId,
                    "")));

    result.requireAllSucceeded("bulkEnqueue sparse-scope plan tables");

    StoredReconcileJob first =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, result.items.get(0).jobId));
    StoredReconcileJob second =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, result.items.get(1).jobId));

    assertEquals("table-source|db|orders", first.laneKey);
    assertEquals("table-source|db|customers", second.laneKey);
  }

  @Test
  void duplicateEnqueueLeavesMissingLookupPointerUnrepaired() {
    assumeMemoryOnly("pointer corruption assertions are only meaningful in memory mode");
    store.init();

    ReconcileJobStore.BulkEnqueueSpec spec =
        fileGroupSpec("parent-1", "group-1", "s3://bucket/table-1/file-1.parquet");
    var first = store.bulkEnqueue(List.of(spec));
    first.requireAllSucceeded("first enqueue");
    String jobId = first.items.getFirst().jobId;
    String lookupKey = Keys.reconcileJobLookupPointerById(jobId);
    String canonicalKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);

    Pointer lookupPointer = store.pointerStore.get(lookupKey).orElseThrow();
    assertTrue(store.pointerStore.compareAndDelete(lookupKey, lookupPointer.getVersion()));
    assertTrue(store.pointerStore.get(lookupKey).isEmpty());

    var duplicate = store.bulkEnqueue(List.of(spec));

    duplicate.requireAllSucceeded("duplicate enqueue");
    assertFalse(duplicate.items.getFirst().created);
    assertEquals(jobId, duplicate.items.getFirst().jobId);
    assertTrue(
        store.pointerStore.get(lookupKey).isEmpty(),
        "duplicate enqueue should not restore the lookup pointer on the hot path");

    var leased = store.leaseNext().orElseThrow();
    assertEquals(jobId, leased.jobId);
  }

  @Test
  void enqueueAndLeaseExecViewPreservesSourceNamespace() {
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of("analytics-namespace-id"), null),
            ReconcileJobKind.PLAN_VIEW,
            ReconcileTableTask.empty(),
            ReconcileViewTask.of(
                "db", "events_summary", "analytics-namespace-id", "events-summary-id"),
            ReconcileExecutionPolicy.defaults(),
            "",
            "");

    var lease = store.leaseNext().orElseThrow();

    assertEquals(jobId, lease.jobId);
    assertEquals("db", lease.viewTask.sourceNamespace());
    assertEquals("events_summary", lease.viewTask.sourceView());
    assertEquals("analytics-namespace-id", lease.viewTask.destinationNamespaceId());
    assertEquals("events-summary-id", lease.viewTask.destinationViewId());
  }

  @Test
  void persistSnapshotPlanUpdatesStoredSnapshotTask() {
    store.init();

    String jobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "events"),
            ReconcileExecutionPolicy.defaults(),
            "parent-1",
            "");

    ReconcileSnapshotTask task =
        ReconcileSnapshotTask.of(
            "table-1",
            55L,
            "db",
            "events",
            List.of(
                ReconcileFileGroupTask.of(
                    jobId,
                    "snapshot-55-group-0",
                    "table-1",
                    55L,
                    List.of("s3://bucket/data/file-1.parquet"))),
            true);
    String manifestUri = store.persistSnapshotPlanManifest(ACCOUNT_ID, jobId, task);
    var lease = leaseJob(jobId, ReconcileJobKind.PLAN_SNAPSHOT);
    store.adoptSnapshotPlanManifest(jobId, lease.leaseEpoch, task, manifestUri, true);

    var job = store.get(ACCOUNT_ID, jobId).orElseThrow();
    assertEquals(1, job.snapshotTask.fileGroups().size());
    assertEquals(
        "s3://bucket/data/file-1.parquet",
        job.snapshotTask.fileGroups().getFirst().filePaths().getFirst());
  }

  @Test
  void adoptSnapshotPlanManifestAcceptsSharedPlannerSnapshotBlob() throws Exception {
    store.init();

    String jobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "events"),
            ReconcileExecutionPolicy.defaults(),
            "parent-1",
            "");

    ReconcileFileGroupTask plannedGroup =
        ReconcileFileGroupTask.of(
            jobId,
            "snapshot-55-group-0",
            "table-1",
            55L,
            List.of("s3://bucket/data/file-1.parquet"));
    String plannerBlobUri =
        SnapshotPlanManifestIds.manifestBlobUri(ACCOUNT_ID, jobId, List.of(plannedGroup));
    store.blobStore.put(
        plannerBlobUri,
        store.mapper.writeValueAsBytes(
            SnapshotPlanBlob.of(
                List.of(new PlannedFileGroupJob(ReconcileScope.empty(), plannedGroup)))),
        "application/json; charset=UTF-8");
    var lease = leaseJob(jobId, ReconcileJobKind.PLAN_SNAPSHOT);

    store.adoptSnapshotPlanManifest(
        jobId,
        lease.leaseEpoch,
        ReconcileSnapshotTask.of(
            "table-1",
            55L,
            "db",
            "events",
            List.of(),
            true,
            ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
            plannerBlobUri,
            1),
        plannerBlobUri,
        true);

    var job = store.get(ACCOUNT_ID, jobId).orElseThrow();
    assertEquals(1, job.snapshotTask.fileGroups().size());
    assertEquals("snapshot-55-group-0", job.snapshotTask.fileGroups().getFirst().groupId());
  }

  @Test
  void enqueueSnapshotFinalizationAcceptsSharedPlannerSnapshotBlob() throws Exception {
    store.init();

    String parentJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "events"),
            ReconcileExecutionPolicy.defaults(),
            "",
            "");

    ReconcileFileGroupTask plannedGroup =
        ReconcileFileGroupTask.of(
            parentJobId,
            "snapshot-55-group-0",
            "table-1",
            55L,
            List.of("s3://bucket/data/file-1.parquet"));
    String plannerBlobUri =
        SnapshotPlanManifestIds.manifestBlobUri(ACCOUNT_ID, parentJobId, List.of(plannedGroup));
    store.blobStore.put(
        plannerBlobUri,
        store.mapper.writeValueAsBytes(
            SnapshotPlanBlob.of(
                List.of(new PlannedFileGroupJob(ReconcileScope.empty(), plannedGroup)))),
        "application/json; charset=UTF-8");

    String finalizerJobId =
        store.enqueueSnapshotFinalization(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileSnapshotTask.of(
                "table-1",
                55L,
                "db",
                "events",
                List.of(),
                true,
                ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
                plannerBlobUri,
                1),
            ReconcileExecutionPolicy.defaults(),
            parentJobId,
            "");

    var job = store.get(ACCOUNT_ID, finalizerJobId).orElseThrow();
    assertEquals(ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE, job.jobKind);
    assertEquals(1, job.snapshotTask.fileGroups().size());
  }

  @Test
  void getReturnsCanonicalSnapshotPlanWhenLookupPointerIsStale() {
    assumeMemoryOnly("pointer corruption assertions are only meaningful in memory mode");
    store.init();

    String jobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "events"),
            ReconcileExecutionPolicy.defaults(),
            "parent-1",
            "");

    String lookupKey = Keys.reconcileJobLookupPointerById(jobId);
    Pointer oldLookup = store.pointerStore.get(lookupKey).orElseThrow();

    ReconcileSnapshotTask task =
        ReconcileSnapshotTask.of(
            "table-1",
            55L,
            "db",
            "events",
            List.of(
                ReconcileFileGroupTask.of(
                    jobId,
                    "snapshot-55-group-0",
                    "table-1",
                    55L,
                    List.of("s3://bucket/data/file-1.parquet"))),
            true);
    String manifestUri = store.persistSnapshotPlanManifest(ACCOUNT_ID, jobId, task);
    var lease = leaseJob(jobId, ReconcileJobKind.PLAN_SNAPSHOT);
    store.adoptSnapshotPlanManifest(jobId, lease.leaseEpoch, task, manifestUri, true);

    Pointer currentLookup = store.pointerStore.get(lookupKey).orElseThrow();
    assertTrue(
        store.pointerStore.compareAndSet(
            lookupKey,
            currentLookup.getVersion(),
            Pointer.newBuilder()
                .setKey(lookupKey)
                .setBlobUri(oldLookup.getBlobUri())
                .setVersion(currentLookup.getVersion() + 1)
                .build()));

    var job = store.get(ACCOUNT_ID, jobId).orElseThrow();

    assertEquals(1, job.snapshotTask.fileGroups().size());
    assertEquals(
        "s3://bucket/data/file-1.parquet",
        job.snapshotTask.fileGroups().getFirst().filePaths().getFirst());
    assertTrue(store.pointerStore.get(lookupKey).isPresent());
  }

  @Test
  void childJobsReturnsCanonicalChildWhenParentPointerIsStale() {
    assumeMemoryOnly("pointer corruption assertions are only meaningful in memory mode");
    store.init();

    String parentJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    String childJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("src.ns", "orders", "orders-table-id", "orders"),
            ReconcileExecutionPolicy.defaults(),
            parentJobId,
            "");

    String parentPointerKey = Keys.reconcileJobByParentPointer(ACCOUNT_ID, parentJobId, childJobId);
    Pointer oldParentPointer = store.pointerStore.get(parentPointerKey).orElseThrow();
    var parentLease = store.leaseNext().orElseThrow();
    assertEquals(parentJobId, parentLease.jobId);
    var childLease = store.leaseNext().orElseThrow();
    assertEquals(childJobId, childLease.jobId);
    store.markRunning(
        childLease.jobId, childLease.leaseEpoch, System.currentTimeMillis(), "exec-1");

    Pointer currentParentPointer = store.pointerStore.get(parentPointerKey).orElseThrow();
    assertTrue(
        store.pointerStore.compareAndSet(
            parentPointerKey,
            currentParentPointer.getVersion(),
            Pointer.newBuilder()
                .setKey(parentPointerKey)
                .setBlobUri(oldParentPointer.getBlobUri())
                .setVersion(currentParentPointer.getVersion() + 1)
                .build()));

    var children = listChildJobs(ACCOUNT_ID, parentJobId);

    assertEquals(1, children.size());
    assertEquals("JS_RUNNING", children.getFirst().state);
    assertEquals(
        Keys.reconcileJobStateRowById(ACCOUNT_ID, childJobId),
        store.pointerStore.get(parentPointerKey).orElseThrow().getBlobUri());
  }

  @Test
  void queueStatsUsesCanonicalJobWhenLookupPointerIsStale() {
    assumeMemoryOnly("pointer corruption assertions are only meaningful in memory mode");
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    String lookupKey = Keys.reconcileJobLookupPointerById(jobId);
    Pointer oldLookup = store.pointerStore.get(lookupKey).orElseThrow();

    var lease = store.leaseNext().orElseThrow();
    store.markRunning(lease.jobId, lease.leaseEpoch, System.currentTimeMillis(), "exec-1");

    Pointer currentLookup = store.pointerStore.get(lookupKey).orElseThrow();
    assertTrue(
        store.pointerStore.compareAndSet(
            lookupKey,
            currentLookup.getVersion(),
            Pointer.newBuilder()
                .setKey(lookupKey)
                .setBlobUri(oldLookup.getBlobUri())
                .setVersion(currentLookup.getVersion() + 1)
                .build()));

    var stats = store.queueStats();

    assertEquals(0L, stats.queued);
    assertEquals(1L, stats.running);
    assertEquals(0L, stats.cancelling);
    assertEquals(
        Keys.reconcileJobStateRowById(ACCOUNT_ID, jobId),
        store.pointerStore.get(lookupKey).orElseThrow().getBlobUri());
  }

  @Test
  void queueStatsUsesStateIndexesInsteadOfLookupListing() {
    assumeMemoryOnly("pointer list-count assertions are only meaningful in memory mode");
    TrackingPointerStore pointerStore = new TrackingPointerStore();
    store.pointerStore = pointerStore;
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    var lease = store.leaseNext().orElseThrow();
    store.markRunning(jobId, lease.leaseEpoch, System.currentTimeMillis(), "exec-1");

    pointerStore.resetListCounts();
    var stats = store.queueStats();

    assertEquals(0L, stats.queued);
    assertEquals(1L, stats.running);
    assertEquals(0, pointerStore.listCount(Keys.reconcileJobLookupPointerByIdPrefix()));
    assertEquals(1, pointerStore.listCount(Keys.reconcileJobByStatePointerPrefix("JS_QUEUED")));
  }

  @Test
  void leaseNextSerializesSnapshotFinalizersPerSnapshot() {
    store.init();
    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of("table-1", 55L, "db", "events", java.util.List.of(), true);

    store.enqueueSnapshotFinalization(
        ACCOUNT_ID,
        CONNECTOR_ID,
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.empty(),
        snapshotTask,
        ReconcileExecutionPolicy.defaults(),
        "parent-1",
        "");
    store.enqueueSnapshotFinalization(
        ACCOUNT_ID,
        CONNECTOR_ID,
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.empty(),
        snapshotTask,
        ReconcileExecutionPolicy.defaults(),
        "parent-2",
        "");

    var firstLease = store.leaseNext().orElseThrow();

    assertEquals(ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE, firstLease.jobKind);
    assertEquals("table-1", firstLease.snapshotTask.tableId());
    assertEquals(55L, firstLease.snapshotTask.snapshotId());
    assertTrue(firstLease.snapshotTask.fileGroupPlanRecorded());
  }

  @Test
  void directEnqueueRejectsImplicitSnapshotCoverageForFinalization() {
    store.init();

    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                store.enqueue(
                    ACCOUNT_ID,
                    CONNECTOR_ID,
                    false,
                    CaptureMode.METADATA_AND_CAPTURE,
                    ReconcileScope.empty(),
                    ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE,
                    ReconcileTableTask.empty(),
                    ReconcileViewTask.empty(),
                    ReconcileSnapshotTask.of("table-1", 55L, "db", "events"),
                    ReconcileFileGroupTask.empty(),
                    ReconcileExecutionPolicy.defaults(),
                    "parent-1",
                    ""));

    assertTrue(error.getMessage().contains("FINALIZE_SNAPSHOT_CAPTURE"));
  }

  @Test
  void persistSnapshotPlanRejectsImplicitCoverageForPlanSnapshotJobs() {
    store.init();

    String jobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "events"),
            ReconcileExecutionPolicy.defaults(),
            "parent-1",
            "");

    var lease = leaseJob(jobId, ReconcileJobKind.PLAN_SNAPSHOT);
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                store.adoptSnapshotPlanManifest(
                    jobId,
                    lease.leaseEpoch,
                    ReconcileSnapshotTask.of(
                        "table-1",
                        55L,
                        "db",
                        "events",
                        List.of(
                            ReconcileFileGroupTask.of(
                                jobId,
                                "snapshot-55-group-0",
                                "table-1",
                                55L,
                                List.of("s3://bucket/data/file-1.parquet")))),
                    "",
                    true));

    assertFalse(error.getMessage().isBlank());
  }

  @Test
  void persistSnapshotPlanRejectsEmptyCoverageForPlanSnapshotJobs() {
    store.init();

    String jobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "events"),
            ReconcileExecutionPolicy.defaults(),
            "parent-1",
            "");

    var lease = leaseJob(jobId, ReconcileJobKind.PLAN_SNAPSHOT);
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                store.adoptSnapshotPlanManifest(
                    jobId, lease.leaseEpoch, ReconcileSnapshotTask.empty(), "", true));

    assertFalse(error.getMessage().isBlank());
  }

  @Test
  void adoptSnapshotPlanManifestReusesExistingManifestBlob() {
    TrackingBlobStore blobStore = new TrackingBlobStore();
    store.blobStore = blobStore;
    store.init();

    String jobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "events", List.of(), true),
            ReconcileExecutionPolicy.defaults(),
            "parent-1",
            "");

    ReconcileSnapshotTask firstTask =
        ReconcileSnapshotTask.of(
            "table-1",
            55L,
            "db",
            "events",
            List.of(ReconcileFileGroupTask.of("plan-1", "group-1", "table-1", 55L, List.of("f1"))),
            true);
    String manifestUri = store.persistSnapshotPlanManifest(ACCOUNT_ID, jobId, firstTask);
    int afterManifestWrite = blobStore.activeBlobCount();

    var lease = leaseJob(jobId, ReconcileJobKind.PLAN_SNAPSHOT);
    store.adoptSnapshotPlanManifest(jobId, lease.leaseEpoch, firstTask, manifestUri, true);
    store.adoptSnapshotPlanManifest(jobId, lease.leaseEpoch, firstTask, manifestUri, true);

    assertEquals(afterManifestWrite, blobStore.activeBlobCount());
  }

  @Test
  void adoptSnapshotPlanManifestPreservesExistingBlobForMetadataOnlyRetry() {
    TrackingBlobStore blobStore = new TrackingBlobStore();
    store.blobStore = blobStore;
    store.init();

    String jobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "events", List.of(), true),
            ReconcileExecutionPolicy.defaults(),
            "parent-1",
            "");

    ReconcileFileGroupTask plannedGroup =
        ReconcileFileGroupTask.of("plan-1", "group-1", "table-1", 55L, List.of("f1"));
    ReconcileSnapshotTask initialTask =
        ReconcileSnapshotTask.of("table-1", 55L, "db", "events", List.of(plannedGroup), true);
    String manifestUri = store.persistSnapshotPlanManifest(ACCOUNT_ID, jobId, initialTask);
    var lease = leaseJob(jobId, ReconcileJobKind.PLAN_SNAPSHOT);
    store.adoptSnapshotPlanManifest(jobId, lease.leaseEpoch, initialTask, manifestUri, true);
    int afterFirstWrite = blobStore.activeBlobCount();

    store.adoptSnapshotPlanManifest(
        jobId,
        lease.leaseEpoch,
        ReconcileSnapshotTask.of(
            "table-1",
            55L,
            "db",
            "events",
            List.of(),
            true,
            ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
            manifestUri,
            1),
        manifestUri,
        true);

    var job = store.get(ACCOUNT_ID, jobId).orElseThrow();
    assertEquals(1, job.snapshotTask.fileGroups().size());
    assertEquals("group-1", job.snapshotTask.fileGroups().getFirst().groupId());
    assertEquals("f1", job.snapshotTask.fileGroups().getFirst().filePaths().getFirst());
    assertEquals(afterFirstWrite, blobStore.activeBlobCount());
  }

  @Test
  void persistSnapshotPlanManifestRejectsImplicitCoverageWithoutWritingBlob() {
    TrackingBlobStore blobStore = new TrackingBlobStore();
    store.blobStore = blobStore;
    store.init();

    String jobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "events", List.of(), true),
            ReconcileExecutionPolicy.defaults(),
            "parent-1",
            "");
    int before = blobStore.activeBlobCount();

    var lease = leaseJob(jobId, ReconcileJobKind.PLAN_SNAPSHOT);
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                store.adoptSnapshotPlanManifest(
                    jobId,
                    lease.leaseEpoch,
                    ReconcileSnapshotTask.of(
                        "table-1",
                        55L,
                        "db",
                        "events",
                        List.of(
                            ReconcileFileGroupTask.of(
                                "plan-1", "group-1", "table-1", 55L, List.of("f1"))),
                        false),
                    "",
                    true));

    assertFalse(error.getMessage().isBlank());
    assertEquals(before, blobStore.activeBlobCount(), "failed mutation should clean up new blob");
  }

  @Test
  void getThrowsWhenInlineDefinitionIsMissing() {
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    StoredReconcileJob record = readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, jobId));

    record.definition = null;
    overwriteCanonicalRecordWithoutSync(Keys.reconcileJobPointerById(ACCOUNT_ID, jobId), record);

    IllegalStateException error =
        assertThrows(IllegalStateException.class, () -> store.get(ACCOUNT_ID, jobId));
    assertTrue(error.getMessage().contains("job definition"));
  }

  @Test
  void leaseSnapshotPlanFailsCorruptJobAndReleasesSnapshotLockWhenDefinitionHydrationFails() {
    assumeMemoryOnly("pointer corruption assertions are only meaningful in memory mode");
    store.init();

    String jobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "orders"),
            ReconcileExecutionPolicy.defaults(),
            "parent-1",
            "");
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    StoredReconcileJob corrupted = readStoredRecord(canonicalPointerKey);
    corrupted.definition = null;
    overwriteCanonicalRecordWithoutSync(canonicalPointerKey, corrupted);

    assertTrue(
        store
            .leaseNext(
                ReconcileJobStore.LeaseRequest.of(
                    Set.of(), Set.of(), Set.of(), Set.of(ReconcileJobKind.PLAN_SNAPSHOT)))
            .isEmpty());

    assertTrue(
        store.pointerStore.get(Keys.reconcileJobLeasePointerById(ACCOUNT_ID, jobId)).isEmpty());
    assertTrue(
        store.pointerStore.get(Keys.reconcileSnapshotLeasePointer("table-1", 55L)).isEmpty());

    StoredReconcileJob failed = readStoredRecord(canonicalPointerKey);
    assertEquals("JS_FAILED", failed.state);
    assertEquals("Missing required job definition", failed.message);
    assertEquals("Missing required job definition", failed.lastError);
    assertTrue(failed.readyPointerKey == null || failed.readyPointerKey.isBlank());
  }

  @Test
  void getThrowsWhenSnapshotPlanBlobIsMissing() {
    store.init();

    String jobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileSnapshotTask.of(
                "table-1",
                55L,
                "db",
                "events",
                List.of(
                    ReconcileFileGroupTask.of(
                        "plan-1", "group-1", "table-1", 55L, List.of("f1", "f2"))),
                true),
            ReconcileExecutionPolicy.defaults(),
            "parent-1",
            "");
    StoredReconcileJob record = readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, jobId));

    assertTrue(store.blobStore.delete(record.snapshotPlanBlobUri));

    IllegalStateException error =
        assertThrows(IllegalStateException.class, () -> store.get(ACCOUNT_ID, jobId));
    assertTrue(error.getMessage().contains("snapshot plan payload"));
  }

  @Test
  void getExecFileGroupDoesNotDependOnChildPlanBlob() {
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileJobKind.EXEC_FILE_GROUP,
            ReconcileTableTask.empty(),
            ReconcileViewTask.empty(),
            ReconcileSnapshotTask.empty(),
            ReconcileFileGroupTask.of(
                "plan-1", "group-1", "table-1", 55L, List.of("s3://bucket/data/file-1.parquet")),
            ReconcileExecutionPolicy.defaults(),
            "parent-1",
            "");
    ReconcileJob job = store.get(ACCOUNT_ID, jobId).orElseThrow();
    assertEquals(ReconcileJobKind.EXEC_FILE_GROUP, job.jobKind);
    assertEquals(List.of(), job.fileGroupTask.filePaths());
  }

  @Test
  void persistFileGroupResultUpdatesCanonicalResultReference() throws Exception {
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileJobKind.EXEC_FILE_GROUP,
            ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask.empty(),
            ReconcileFileGroupTask.of(
                "plan-1", "group-1", "table-1", 55L, List.of("s3://bucket/data/file-1.parquet")),
            ReconcileExecutionPolicy.defaults(),
            "parent-1",
            "");
    var canonicalBefore = readIndexEntry(Keys.reconcileJobPointerById(ACCOUNT_ID, jobId));

    store.persistFileGroupResult(
        jobId,
        ReconcileFileGroupTask.of(
            "plan-1",
            "group-1",
            "table-1",
            55L,
            List.of("s3://bucket/data/file-1.parquet"),
            List.of(
                ai.floedb.floecat.reconciler.jobs.ReconcileFileResult.succeeded(
                    "s3://bucket/data/file-1.parquet",
                    2L,
                    ReconcileIndexArtifactResult.of(
                        "s3://bucket/index/file-1.parquet.index", "parquet", 1)))));

    var canonicalAfter = readIndexEntry(Keys.reconcileJobPointerById(ACCOUNT_ID, jobId));
    var job = store.get(ACCOUNT_ID, jobId).orElseThrow();
    assertTrue(canonicalAfter.version() > canonicalBefore.version());
    assertEquals(1, job.fileGroupTask.fileResults().size());
    assertEquals(2L, job.fileGroupTask.fileResults().getFirst().statsProcessed());
    assertEquals(1L, job.indexesProcessed);
    assertEquals(
        "s3://bucket/index/file-1.parquet.index",
        job.fileGroupTask.fileResults().getFirst().indexArtifact().artifactUri());
  }

  @Test
  void persistFileGroupResultRejectsMismatchedIdentity() {
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileJobKind.EXEC_FILE_GROUP,
            ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask.empty(),
            ReconcileFileGroupTask.of(
                "plan-1", "group-1", "table-1", 55L, List.of("s3://bucket/data/file-1.parquet")),
            ReconcileExecutionPolicy.defaults(),
            "parent-1",
            "");

    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                store.persistFileGroupResult(
                    jobId,
                    ReconcileFileGroupTask.of(
                        "plan-2",
                        "group-1",
                        "table-1",
                        55L,
                        List.of("s3://bucket/data/file-1.parquet"),
                        List.of(
                            ReconcileFileResult.succeeded(
                                "s3://bucket/data/file-1.parquet", 2L)))));

    assertEquals(
        "EXEC_FILE_GROUP result identity does not match canonical job identity",
        error.getMessage());
  }

  @Test
  void markRunningPreservesExecFileGroupCanonicalIdentity() {
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileJobKind.EXEC_FILE_GROUP,
            ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask.empty(),
            ReconcileFileGroupTask.of(
                "plan-1", "group-1", "table-1", 55L, List.of("s3://bucket/data/file-1.parquet")),
            ReconcileExecutionPolicy.defaults(),
            "parent-1",
            "");

    var lease = store.leaseNext().orElseThrow();
    store.markRunning(jobId, lease.leaseEpoch, System.currentTimeMillis(), "exec-1");

    var job = store.get(ACCOUNT_ID, jobId).orElseThrow();
    assertEquals("plan-1", job.fileGroupTask.planId());
    assertEquals("group-1", job.fileGroupTask.groupId());
    assertEquals("table-1", job.fileGroupTask.tableId());
    assertEquals(55L, job.fileGroupTask.snapshotId());
    assertEquals(1, job.fileGroupTask.fileCount());
    assertEquals("JS_RUNNING", job.state);
  }

  @Test
  void leaseNextPreservesExecFileGroupCanonicalIdentity() {
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileJobKind.EXEC_FILE_GROUP,
            ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask.empty(),
            ReconcileFileGroupTask.of(
                "plan-1", "group-1", "table-1", 55L, List.of("s3://bucket/data/file-1.parquet")),
            ReconcileExecutionPolicy.defaults(),
            "parent-1",
            "");

    var lease = store.leaseNext().orElseThrow();

    assertEquals(jobId, lease.jobId);
    assertEquals("plan-1", lease.fileGroupTask.planId());
    assertEquals("group-1", lease.fileGroupTask.groupId());
    assertEquals("table-1", lease.fileGroupTask.tableId());
    assertEquals(55L, lease.fileGroupTask.snapshotId());
    assertEquals(1, lease.fileGroupTask.fileCount());

    var job = store.get(ACCOUNT_ID, jobId).orElseThrow();
    assertEquals("plan-1", job.fileGroupTask.planId());
    assertEquals("group-1", job.fileGroupTask.groupId());
    assertEquals("table-1", job.fileGroupTask.tableId());
    assertEquals(55L, job.fileGroupTask.snapshotId());
    assertEquals(1, job.fileGroupTask.fileCount());
    assertEquals("JS_RUNNING", job.state);
  }

  @Test
  void execFileGroupIdentityIsPreservedAcrossLeaseRunningAndCompletion() {
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileJobKind.EXEC_FILE_GROUP,
            ReconcileTableTask.empty(),
            ReconcileViewTask.empty(),
            ReconcileSnapshotTask.empty(),
            ReconcileFileGroupTask.of(
                "plan-1", "group-1", "table-1", 55L, List.of("s3://bucket/data/file-1.parquet")),
            ReconcileExecutionPolicy.defaults(),
            "parent-1",
            "");

    ReconcileJobStore.LeasedJob lease = store.leaseNext().orElseThrow();
    store.markRunning(jobId, lease.leaseEpoch, System.currentTimeMillis(), "exec-1");
    store.persistFileGroupResult(
        jobId,
        ReconcileFileGroupTask.of(
            "plan-1",
            "group-1",
            "table-1",
            55L,
            List.of("s3://bucket/data/file-1.parquet"),
            List.of(ReconcileFileResult.succeeded("s3://bucket/data/file-1.parquet", 1L))));
    store.markSucceeded(
        jobId, lease.leaseEpoch, System.currentTimeMillis(), 0L, 0L, 0L, 0L, 0L, 1L);

    ReconcileJob job = store.get(ACCOUNT_ID, jobId).orElseThrow();
    assertEquals("plan-1", job.fileGroupTask.planId());
    assertEquals("group-1", job.fileGroupTask.groupId());
    assertEquals("table-1", job.fileGroupTask.tableId());
    assertEquals(55L, job.fileGroupTask.snapshotId());
    assertEquals(1, job.fileGroupTask.fileCount());
    assertEquals("JS_SUCCEEDED", job.state);
  }

  @Test
  void canonicalStateBlobDoesNotInlineLargeSnapshotOrResultPayloads() throws Exception {
    assumeMemoryOnly("pointer payload assertions are only meaningful in memory mode");
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileJobKind.EXEC_FILE_GROUP,
            ReconcileTableTask.empty(),
            ReconcileViewTask.empty(),
            ReconcileSnapshotTask.of(
                "table-1",
                55L,
                "db",
                "events",
                List.of(
                    ReconcileFileGroupTask.of(
                        "plan-1",
                        "snapshot-group-1",
                        "table-1",
                        55L,
                        List.of("s3://bucket/data/file-1.parquet"))),
                true),
            ReconcileFileGroupTask.of(
                "plan-1", "group-1", "table-1", 55L, List.of("s3://bucket/data/file-1.parquet")),
            ReconcileExecutionPolicy.defaults(),
            "parent-1",
            "");

    store.persistFileGroupResult(
        jobId,
        ReconcileFileGroupTask.of(
            "plan-1",
            "group-1",
            "table-1",
            55L,
            List.of("s3://bucket/data/file-1.parquet"),
            List.of(
                ai.floedb.floecat.reconciler.jobs.ReconcileFileResult.succeeded(
                    "s3://bucket/data/file-1.parquet", 2L))));

    Pointer canonicalPointer =
        store.pointerStore.get(Keys.reconcileJobPointerById(ACCOUNT_ID, jobId)).orElseThrow();
    var stateJson = store.mapper.valueToTree(readStoredRecord(canonicalPointer.getKey()));
    var resultJson = readBlobJson(stateJson.path("fileGroupResultBlobUri").asText());

    assertFalse(stateJson.has("snapshotTaskFileGroups"));
    assertFalse(stateJson.has("fileGroupPaths"));
    assertFalse(stateJson.has("fileGroupResults"));
    assertFalse(stateJson.path("fileGroupResultBlobUri").asText().isBlank());
    assertTrue(stateJson.has("definition"));
    assertFalse(stateJson.path("snapshotPlanBlobUri").asText().isBlank());
    assertFalse(stateJson.has("fileGroupPlanBlobUri"));
    assertEquals(1, stateJson.path("fileGroupFileCount").asInt());
    assertFalse(resultJson.has("planId"));
    assertFalse(resultJson.has("groupId"));
    assertFalse(resultJson.has("tableId"));
    assertFalse(resultJson.has("snapshotId"));
    assertFalse(resultJson.has("fileCount"));
  }

  @Test
  void enqueueExecViewRejectsMismatchedDestinationNamespaceIds() {
    store.init();

    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                store.enqueue(
                    ACCOUNT_ID,
                    CONNECTOR_ID,
                    false,
                    CaptureMode.METADATA_AND_CAPTURE,
                    ReconcileScope.of(List.of("analytics-namespace-id"), null),
                    ReconcileJobKind.PLAN_VIEW,
                    ReconcileTableTask.empty(),
                    ReconcileViewTask.of(
                        "db", "events_summary", "other-namespace-id", "events-summary-id"),
                    ReconcileExecutionPolicy.defaults(),
                    "",
                    ""));

    assertEquals(
        "view task destinationNamespaceId does not match scope destinationNamespaceIds",
        error.getMessage());
  }

  @Test
  void enqueueExecTableDedupesOnDestinationTableIdNotDisplayName() {
    store.init();

    String first =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("src.ns", "orders", "orders-table-id", "orders_v1"),
            ReconcileExecutionPolicy.defaults(),
            "",
            "");
    String second =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("src.ns", "orders", "orders-table-id", "orders_v2"),
            ReconcileExecutionPolicy.defaults(),
            "",
            "");

    assertEquals(first, second);
  }

  @Test
  void enqueueExecViewDedupesOnDestinationIdsNotDisplayName() {
    store.init();

    String first =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of("analytics-namespace-id"), null),
            ReconcileJobKind.PLAN_VIEW,
            ReconcileTableTask.empty(),
            ReconcileViewTask.of(
                "db", "events_summary", "analytics-namespace-id", "events-summary-id"),
            ReconcileExecutionPolicy.defaults(),
            "",
            "");
    String second =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of("analytics-namespace-id"), null),
            ReconcileJobKind.PLAN_VIEW,
            ReconcileTableTask.empty(),
            ReconcileViewTask.of(
                "db", "events_summary", "analytics-namespace-id", "events-summary-id"),
            ReconcileExecutionPolicy.defaults(),
            "",
            "");

    assertEquals(first, second);
  }

  @Test
  void enqueueNamespaceScopedExecViewDedupesOnDestinationNamespaceIds() {
    store.init();

    String first =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of("analytics-namespace-id"), null),
            ReconcileJobKind.PLAN_VIEW,
            ReconcileTableTask.empty(),
            ReconcileViewTask.empty(),
            ReconcileExecutionPolicy.defaults(),
            "",
            "");
    String second =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of("analytics-namespace-id"), null),
            ReconcileJobKind.PLAN_VIEW,
            ReconcileTableTask.empty(),
            ReconcileViewTask.empty(),
            ReconcileExecutionPolicy.defaults(),
            "",
            "");

    assertEquals(first, second);
  }

  @Test
  void leaseNextFiltersByExecutionPolicy() {
    store.init();

    store.enqueue(
        ACCOUNT_ID,
        "conn-default",
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.empty(),
        ReconcileExecutionPolicy.defaults(),
        "");
    String remoteJobId =
        store.enqueue(
            ACCOUNT_ID,
            "conn-remote",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
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
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.empty(),
        ReconcileExecutionPolicy.of(ReconcileExecutionClass.HEAVY, "remote", java.util.Map.of()),
        "remote-a");
    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            "conn-b",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
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
  void leaseNextDoesNotMatchPinnedJobWhenExecutorFilterIsEmpty() {
    store.init();

    store.enqueue(
        ACCOUNT_ID,
        "conn-pinned",
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.empty(),
        ReconcileExecutionPolicy.of(ReconcileExecutionClass.HEAVY, "remote", java.util.Map.of()),
        "remote-a");

    assertTrue(
        store
            .leaseNext(
                ReconcileJobStore.LeaseRequest.of(
                    java.util.EnumSet.of(ReconcileExecutionClass.HEAVY),
                    java.util.Set.of("remote")))
            .isEmpty());
  }

  @Test
  void leaseNextAllowsOnlyOneRunningJobPerLane() {
    store.init();
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String metadataJob =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);
    String statsJob =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.CAPTURE_ONLY, scope);

    var firstLease = store.leaseNext().orElseThrow();
    assertTrue(firstLease.jobId.equals(metadataJob) || firstLease.jobId.equals(statsJob));

    assertTrue(store.leaseNext().isEmpty());

    store.markSucceeded(
        firstLease.jobId, firstLease.leaseEpoch, System.currentTimeMillis(), 1, 1, 1, 1);

    var secondLease = store.leaseNext().orElseThrow();
    String remainingJobId = firstLease.jobId.equals(metadataJob) ? statsJob : metadataJob;
    assertEquals(remainingJobId, secondLease.jobId);
  }

  @Test
  void markSucceededReleasesLaneSoNextQueuedSameLaneLeasesImmediately() {
    assumeMemoryOnly("lane owner pointer assertions are only meaningful in memory mode");
    store.init();
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String firstJob =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);
    String secondJob = store.enqueue(ACCOUNT_ID, "conn-b", false, CaptureMode.CAPTURE_ONLY, scope);
    var firstLease = store.leaseNext().orElseThrow();
    assertTrue(firstLease.jobId.equals(firstJob) || firstLease.jobId.equals(secondJob));
    String firstCanonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, firstLease.jobId);
    StoredReconcileJob firstRecord = readStoredRecord(firstCanonicalPointerKey);
    String lanePointerKey = Keys.reconcileLaneLeasePointer(ACCOUNT_ID, firstRecord.laneKey);
    assertTrue(store.pointerStore.get(lanePointerKey).isPresent());

    store.markSucceeded(
        firstLease.jobId, firstLease.leaseEpoch, System.currentTimeMillis(), 1, 1, 1, 1);

    assertTrue(store.pointerStore.get(lanePointerKey).isEmpty());
    var secondLease = store.leaseNext().orElseThrow();
    String remainingJobId = firstLease.jobId.equals(firstJob) ? secondJob : firstJob;
    assertEquals(remainingJobId, secondLease.jobId);
  }

  @Test
  void staleLaneLeasePointerStillBlocksConcurrentLease() throws Exception {
    assumeMemoryOnly("lane owner pointer corruption assertions are only meaningful in memory mode");
    store.init();
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String firstJob =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, firstJob);
    var firstLease = store.leaseNext().orElseThrow();

    StoredReconcileJob currentJob = readStoredRecord(canonicalPointerKey);
    String lanePointerKey = Keys.reconcileLaneLeasePointer(ACCOUNT_ID, currentJob.laneKey);
    Pointer lanePointer = store.pointerStore.get(lanePointerKey).orElseThrow();
    assertTrue(
        store.pointerStore.compareAndSet(
            lanePointerKey,
            lanePointer.getVersion(),
            Pointer.newBuilder()
                .setKey(lanePointerKey)
                .setBlobUri(canonicalPointerKey)
                .setVersion(lanePointer.getVersion() + 1)
                .build()));

    store.enqueue(ACCOUNT_ID, "conn-b", false, CaptureMode.CAPTURE_ONLY, scope);

    assertTrue(store.leaseNext().isEmpty());

    store.markSucceeded(firstJob, firstLease.leaseEpoch, System.currentTimeMillis(), 1, 1, 1, 1);
    assertEquals("conn-b", store.leaseNext().orElseThrow().connectorId);
  }

  @Test
  void staleLanePointerToTerminalJobIsClearedDuringLease() {
    assumeMemoryOnly("lane owner pointer assertions are only meaningful in memory mode");
    store.init();
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String firstJob =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);
    String firstCanonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, firstJob);
    var firstLease = store.leaseNext().orElseThrow();
    store.markSucceeded(firstJob, firstLease.leaseEpoch, System.currentTimeMillis(), 1, 1, 1, 1);

    StoredReconcileJob terminalRecord = readStoredRecord(firstCanonicalPointerKey);
    String lanePointerKey = Keys.reconcileLaneLeasePointer(ACCOUNT_ID, terminalRecord.laneKey);
    assertTrue(
        store.pointerStore.compareAndSet(
            lanePointerKey,
            0L,
            Pointer.newBuilder()
                .setKey(lanePointerKey)
                .setBlobUri(firstCanonicalPointerKey)
                .setVersion(1L)
                .build()));

    String secondJob = store.enqueue(ACCOUNT_ID, "conn-b", false, CaptureMode.CAPTURE_ONLY, scope);
    var secondLease = store.leaseNext().orElseThrow();

    assertEquals(secondJob, secondLease.jobId);
    assertEquals(
        Keys.reconcileJobPointerById(ACCOUNT_ID, secondJob),
        store.pointerStore.get(lanePointerKey).orElseThrow().getBlobUri());
  }

  @Test
  void clearLaneLeaseIfOwnedDoesNotClearRetainedOldBlobForActiveCanonicalOwner() throws Exception {
    assumeMemoryOnly(
        "in-memory lease owner retention assertions are only meaningful in memory mode");
    store.init();
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String jobId =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    Pointer initialCanonical = store.pointerStore.get(canonicalPointerKey).orElseThrow();

    store.leaseNext().orElseThrow();

    Pointer currentCanonical = store.pointerStore.get(canonicalPointerKey).orElseThrow();
    assertTrue(currentCanonical.getVersion() > initialCanonical.getVersion());

    StoredReconcileJob activeRecord = readStoredRecord(canonicalPointerKey);

    String lanePointerKey = Keys.reconcileLaneLeasePointer(ACCOUNT_ID, activeRecord.laneKey);
    Pointer lanePointer = store.pointerStore.get(lanePointerKey).orElseThrow();
    assertTrue(
        store.pointerStore.compareAndSet(
            lanePointerKey,
            lanePointer.getVersion(),
            Pointer.newBuilder()
                .setKey(lanePointerKey)
                .setBlobUri(canonicalPointerKey)
                .setVersion(lanePointer.getVersion() + 1)
                .build()));

    ReconcileLeaseStore leaseManager = leaseManager();
    assertDoesNotThrow(() -> leaseManager.clearLaneLeaseIfOwned(activeRecord, canonicalPointerKey));

    Pointer retainedPointer = store.pointerStore.get(lanePointerKey).orElseThrow();
    assertEquals(canonicalPointerKey, retainedPointer.getBlobUri());
  }

  @Test
  void tryAcquireLaneLeaseRetainsStaleSelfOwnedLanePointerForSameJob() throws Exception {
    assumeMemoryOnly("in-memory lease owner alias assertions are only meaningful in memory mode");
    store.init();
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String jobId =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    StoredReconcileJob queuedRecord = readStoredRecord(canonicalPointerKey);

    ReconcileLeaseStore leaseManager = leaseManager();

    boolean firstClaim =
        leaseManager.tryAcquireLaneLease(
            queuedRecord, canonicalPointerKey, System.currentTimeMillis());
    Pointer staleLanePointer =
        store
            .pointerStore
            .get(Keys.reconcileLaneLeasePointer(ACCOUNT_ID, queuedRecord.laneKey))
            .orElseThrow();
    boolean secondClaim =
        leaseManager.tryAcquireLaneLease(
            queuedRecord, canonicalPointerKey, System.currentTimeMillis());

    assertTrue(firstClaim);
    assertTrue(secondClaim);
    Pointer retainedLanePointer =
        store
            .pointerStore
            .get(Keys.reconcileLaneLeasePointer(ACCOUNT_ID, queuedRecord.laneKey))
            .orElseThrow();
    assertEquals(staleLanePointer.getVersion(), retainedLanePointer.getVersion());
    assertEquals(canonicalPointerKey, retainedLanePointer.getBlobUri());
  }

  @Test
  void tryAcquireLaneLeaseAcceptsStaleSameJobAliasPointerWithoutSelfAuthorizing() throws Exception {
    assumeMemoryOnly("in-memory lease owner alias assertions are only meaningful in memory mode");
    store.init();
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String jobId =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    StoredReconcileJob queuedRecord = readStoredRecord(canonicalPointerKey);
    Pointer canonicalPointer = store.pointerStore.get(canonicalPointerKey).orElseThrow();

    String aliasCanonicalKey = canonicalPointerKey + "-alias";
    assertTrue(
        store.pointerStore.compareAndSet(
            aliasCanonicalKey,
            0L,
            Pointer.newBuilder()
                .setKey(aliasCanonicalKey)
                .setBlobUri(canonicalPointer.getBlobUri())
                .setVersion(1L)
                .build()));

    String lanePointerKey = Keys.reconcileLaneLeasePointer(ACCOUNT_ID, queuedRecord.laneKey);
    assertTrue(
        store.pointerStore.compareAndSet(
            lanePointerKey,
            0L,
            Pointer.newBuilder()
                .setKey(lanePointerKey)
                .setBlobUri(aliasCanonicalKey)
                .setVersion(1L)
                .build()));

    ReconcileLeaseStore leaseManager = leaseManager();

    boolean acquired =
        leaseManager.tryAcquireLaneLease(
            queuedRecord, canonicalPointerKey, System.currentTimeMillis());

    assertTrue(acquired);
    assertEquals(
        aliasCanonicalKey, store.pointerStore.get(lanePointerKey).orElseThrow().getBlobUri());
  }

  @Test
  void tryAcquireLaneLeaseTreatsQueuedButLeasedOwnerAsActive() throws Exception {
    assumeMemoryOnly("pointer corruption assertions are only meaningful in memory mode");
    System.setProperty("floecat.reconciler.job-store.lease-ms", "60000");
    store.init();
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String firstJob =
        store.enqueue(ACCOUNT_ID, "conn-a", false, CaptureMode.METADATA_AND_CAPTURE, scope);
    String secondJob =
        store.enqueue(ACCOUNT_ID, "conn-b", false, CaptureMode.METADATA_AND_CAPTURE, scope);

    var firstLease = store.leaseNext().orElseThrow();
    String firstCanonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, firstJob);
    StoredReconcileJob firstRecord = readStoredRecord(firstCanonicalPointerKey);
    firstRecord.state = "JS_QUEUED";
    overwriteCanonicalRecordWithoutSync(firstCanonicalPointerKey, firstRecord);

    String secondCanonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, secondJob);
    StoredReconcileJob secondRecord = readStoredRecord(secondCanonicalPointerKey);
    String lanePointerKey = Keys.reconcileLaneLeasePointer(ACCOUNT_ID, secondRecord.laneKey);
    String laneOwnerBeforeAcquire =
        store.pointerStore.get(lanePointerKey).orElseThrow().getBlobUri();

    ReconcileLeaseStore leaseManager = leaseManager();

    boolean acquired =
        leaseManager.tryAcquireLaneLease(
            secondRecord, secondCanonicalPointerKey, System.currentTimeMillis());

    assertFalse(acquired);
    assertEquals(
        laneOwnerBeforeAcquire, store.pointerStore.get(lanePointerKey).orElseThrow().getBlobUri());
  }

  @Test
  void leaseNextAllowsOnlyOneRunningJobPerTableAcrossConnectors() {
    store.init();
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String firstJob =
        store.enqueue(ACCOUNT_ID, "conn-a", false, CaptureMode.METADATA_AND_CAPTURE, scope);
    String secondJob =
        store.enqueue(ACCOUNT_ID, "conn-b", false, CaptureMode.METADATA_AND_CAPTURE, scope);

    var firstLease = store.leaseNext().orElseThrow();
    assertTrue(firstLease.jobId.equals(firstJob) || firstLease.jobId.equals(secondJob));

    assertTrue(store.leaseNext().isEmpty());

    store.markSucceeded(
        firstLease.jobId, firstLease.leaseEpoch, System.currentTimeMillis(), 1, 1, 1, 1);

    var secondLease = store.leaseNext().orElseThrow();
    String remainingJobId = firstLease.jobId.equals(firstJob) ? secondJob : firstJob;
    assertEquals(remainingJobId, secondLease.jobId);
  }

  @Test
  void staleSnapshotLeasePointerStillBlocksConcurrentSnapshotLease() throws Exception {
    assumeMemoryOnly(
        "snapshot owner pointer corruption assertions are only meaningful in memory mode");
    store.init();

    String firstJob =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "events"),
            ReconcileExecutionPolicy.defaults(),
            "parent-a",
            "");
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, firstJob);
    var firstLease = store.leaseNext().orElseThrow();

    String snapshotLeasePointerKey = Keys.reconcileSnapshotLeasePointer("table-1", 55L);
    Pointer snapshotLeasePointer = store.pointerStore.get(snapshotLeasePointerKey).orElseThrow();
    assertTrue(
        store.pointerStore.compareAndSet(
            snapshotLeasePointerKey,
            snapshotLeasePointer.getVersion(),
            Pointer.newBuilder()
                .setKey(snapshotLeasePointerKey)
                .setBlobUri(canonicalPointerKey)
                .setVersion(snapshotLeasePointer.getVersion() + 1)
                .build()));

    store.enqueueSnapshotPlan(
        ACCOUNT_ID,
        "conn-b",
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.empty(),
        ReconcileSnapshotTask.of("table-1", 55L, "db", "events"),
        ReconcileExecutionPolicy.defaults(),
        "parent-b",
        "");

    assertTrue(store.leaseNext().isEmpty());

    store.markSucceeded(firstJob, firstLease.leaseEpoch, System.currentTimeMillis(), 1, 1, 1, 1);
    assertEquals("conn-b", store.leaseNext().orElseThrow().connectorId);
  }

  @Test
  void clearSnapshotLeaseIfOwnedDoesNotClearRetainedOldBlobForActiveCanonicalOwner()
      throws Exception {
    assumeMemoryOnly(
        "in-memory snapshot owner retention assertions are only meaningful in memory mode");
    store.init();

    String jobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "events"),
            ReconcileExecutionPolicy.defaults(),
            "parent-a",
            "");
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    Pointer initialCanonical = store.pointerStore.get(canonicalPointerKey).orElseThrow();

    store.leaseNext().orElseThrow();

    Pointer currentCanonical = store.pointerStore.get(canonicalPointerKey).orElseThrow();
    assertTrue(currentCanonical.getVersion() > initialCanonical.getVersion());

    String snapshotLeasePointerKey = Keys.reconcileSnapshotLeasePointer("table-1", 55L);
    Pointer snapshotLeasePointer = store.pointerStore.get(snapshotLeasePointerKey).orElseThrow();
    assertTrue(
        store.pointerStore.compareAndSet(
            snapshotLeasePointerKey,
            snapshotLeasePointer.getVersion(),
            Pointer.newBuilder()
                .setKey(snapshotLeasePointerKey)
                .setBlobUri(canonicalPointerKey)
                .setVersion(snapshotLeasePointer.getVersion() + 1)
                .build()));

    StoredReconcileJob activeRecord = readStoredRecord(canonicalPointerKey);

    ReconcileLeaseStore leaseManager = leaseManager();
    assertDoesNotThrow(
        () -> leaseManager.clearSnapshotLeaseIfOwned(activeRecord, canonicalPointerKey));

    Pointer retainedPointer = store.pointerStore.get(snapshotLeasePointerKey).orElseThrow();
    assertEquals(canonicalPointerKey, retainedPointer.getBlobUri());
  }

  @Test
  void leaseNextTreatsEquivalentMultiNamespaceScopesAsSameTableLane() {
    store.init();
    ReconcileScope firstScope = ReconcileScope.of(List.of("b", "a"), null);
    ReconcileScope secondScope = ReconcileScope.of(List.of("a", "b"), null);

    String firstJob =
        store.enqueue(ACCOUNT_ID, "conn-a", false, CaptureMode.METADATA_AND_CAPTURE, firstScope);
    String secondJob =
        store.enqueue(ACCOUNT_ID, "conn-b", false, CaptureMode.CAPTURE_ONLY, secondScope);

    var firstLease = store.leaseNext().orElseThrow();
    assertTrue(firstLease.jobId.equals(firstJob) || firstLease.jobId.equals(secondJob));

    assertTrue(store.leaseNext().isEmpty());

    store.markSucceeded(
        firstLease.jobId, firstLease.leaseEpoch, System.currentTimeMillis(), 1, 1, 1, 1);

    var secondLease = store.leaseNext().orElseThrow();
    String remainingJobId = firstLease.jobId.equals(firstJob) ? secondJob : firstJob;
    assertEquals(remainingJobId, secondLease.jobId);
  }

  @Test
  void leaseNextAllowsConcurrentFileGroupJobsForSameTable() {
    store.init();

    String firstJob =
        store.enqueueFileGroupExecution(
            ACCOUNT_ID,
            "conn-a",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileFileGroupTask.of(
                "plan-1",
                "snapshot-55-group-0",
                "table-1",
                55L,
                List.of("s3://bucket/table-1/file-0.parquet")),
            ReconcileExecutionPolicy.defaults(),
            "snapshot-plan-a",
            "");
    String secondJob =
        store.enqueueFileGroupExecution(
            ACCOUNT_ID,
            "conn-a",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileFileGroupTask.of(
                "plan-1",
                "snapshot-55-group-1",
                "table-1",
                55L,
                List.of("s3://bucket/table-1/file-1.parquet")),
            ReconcileExecutionPolicy.defaults(),
            "snapshot-plan-a",
            "");

    var firstLease = store.leaseNext().orElseThrow();
    var secondLease = store.leaseNext().orElseThrow();

    assertEquals(firstJob, firstLease.jobId);
    assertEquals(secondJob, secondLease.jobId);
    assertNotEquals(firstLease.jobId, secondLease.jobId);
  }

  @Test
  void leaseNextAllowsConcurrentFileGroupJobsForSnapshotZero() {
    store.init();

    String firstJob =
        store.enqueueFileGroupExecution(
            ACCOUNT_ID,
            "conn-a",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileFileGroupTask.of(
                "plan-0",
                "snapshot-0-group-0",
                "table-1",
                0L,
                List.of("s3://bucket/table-1/file-0.parquet")),
            ReconcileExecutionPolicy.defaults(),
            "snapshot-plan-a",
            "");
    String secondJob =
        store.enqueueFileGroupExecution(
            ACCOUNT_ID,
            "conn-a",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileFileGroupTask.of(
                "plan-0",
                "snapshot-0-group-1",
                "table-1",
                0L,
                List.of("s3://bucket/table-1/file-1.parquet")),
            ReconcileExecutionPolicy.defaults(),
            "snapshot-plan-a",
            "");

    var firstLease = store.leaseNext().orElseThrow();
    var secondLease = store.leaseNext().orElseThrow();

    assertEquals(firstJob, firstLease.jobId);
    assertEquals(secondJob, secondLease.jobId);
    assertNotEquals(firstLease.jobId, secondLease.jobId);
  }

  @Test
  void enqueueFileGroupExecutionRequiresCanonicalIdentityFields() {
    store.init();

    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                store.enqueueFileGroupExecution(
                    ACCOUNT_ID,
                    "conn-a",
                    false,
                    CaptureMode.METADATA_AND_CAPTURE,
                    ReconcileScope.of(List.of(), "table-1"),
                    ReconcileFileGroupTask.of(
                        "",
                        "group-1",
                        "table-1",
                        55L,
                        List.of("s3://bucket/table-1/file-1.parquet")),
                    ReconcileExecutionPolicy.defaults(),
                    "snapshot-plan-a",
                    ""));

    assertEquals(
        "EXEC_FILE_GROUP requires planId, groupId, tableId, and snapshotId; got "
            + "planId= groupId=group-1 tableId=table-1 snapshotId=55 fileCount=1 paths=1 results=0",
        error.getMessage());
  }

  @Test
  void successfulJobClearsDedupeAndAllowsFreshEnqueue() {
    store.init();
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");
    String first =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);

    var leased = store.leaseNext().orElseThrow();
    store.markSucceeded(first, leased.leaseEpoch, System.currentTimeMillis(), 10, 2, 4, 20);

    String second =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);
    assertNotEquals(first, second);
  }

  @Test
  void terminalTransitionClearsReadyAndDedupePointers() {
    assumeMemoryOnly("pointer cleanup assertions are only meaningful in memory mode");
    store.init();
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String jobId =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);
    StoredReconcileJob queuedRecord =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, jobId));
    String lanePointerKey = Keys.reconcileLaneLeasePointer(ACCOUNT_ID, queuedRecord.laneKey);

    Pointer dedupePointer =
        firstPointerWithPrefix(Keys.reconcileDedupePointerPrefix(ACCOUNT_ID)).orElseThrow();
    var lease = store.leaseNext().orElseThrow();
    store.markSucceeded(jobId, lease.leaseEpoch, System.currentTimeMillis(), 1, 1, 1, 1);

    assertTrue(store.pointerStore.get(dedupePointer.getKey()).isEmpty());
    assertEquals(0, globalReadyEntryCount());
    assertTrue(store.pointerStore.get(lanePointerKey).isEmpty());
  }

  @Test
  void duplicateEnqueueLeavesMissingSecondaryPointersUnrepaired() {
    assumeMemoryOnly("pointer corruption assertions are only meaningful in memory mode");
    store.init();
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String jobId =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    StoredReconcileJob job = readStoredRecord(canonicalPointerKey);
    String expectedReadyKey =
        Keys.reconcileReadyPointerByDue(job.nextAttemptAtMs, job.accountId, job.laneKey, job.jobId);
    job.readyPointerKey = "";
    overwriteCanonicalRecordWithoutSync(canonicalPointerKey, job);

    String lookupKey = Keys.reconcileJobLookupPointerById(jobId);
    var lookupPointer = store.pointerStore.get(lookupKey).orElseThrow();
    assertTrue(store.pointerStore.compareAndDelete(lookupKey, lookupPointer.getVersion()));

    var readyPointer = store.pointerStore.get(expectedReadyKey).orElseThrow();
    assertTrue(store.pointerStore.compareAndDelete(expectedReadyKey, readyPointer.getVersion()));

    Pointer dedupePointer =
        firstPointerWithPrefix(Keys.reconcileDedupePointerPrefix(ACCOUNT_ID)).orElseThrow();
    assertTrue(
        store.pointerStore.compareAndSet(
            dedupePointer.getKey(),
            dedupePointer.getVersion(),
            Pointer.newBuilder()
                .setKey(dedupePointer.getKey())
                .setBlobUri(canonicalPointerKey)
                .setVersion(dedupePointer.getVersion() + 1)
                .build()));

    String dedupedJobId =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);

    assertEquals(jobId, dedupedJobId);
    assertTrue(store.pointerStore.get(lookupKey).isEmpty());
    assertFalse(readyEntryExists(expectedReadyKey));
    Pointer retainedDedupePointer =
        firstPointerWithPrefix(Keys.reconcileDedupePointerPrefix(ACCOUNT_ID)).orElseThrow();
    assertEquals(canonicalPointerKey, retainedDedupePointer.getBlobUri());
    StoredReconcileJob unrepairedJob = readStoredRecord(canonicalPointerKey);
    assertTrue(unrepairedJob.readyPointerKey == null || unrepairedJob.readyPointerKey.isBlank());

    assertTrue(store.leaseNext().isEmpty());
    store.runMaintenanceOnce(1_000L);
    assertTrue(store.leaseNext().isEmpty());
    assertTrue(store.pointerStore.get(lookupKey).isEmpty());
    assertFalse(readyEntryExists(expectedReadyKey));
  }

  @Test
  void leaseNextLeasesReadyJobWithoutRestoringMissingLookupPointer() {
    assumeMemoryOnly("pointer corruption assertions are only meaningful in memory mode");
    store.init();
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String jobId =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);
    String lookupKey = Keys.reconcileJobLookupPointerById(jobId);
    Pointer lookupPointer = store.pointerStore.get(lookupKey).orElseThrow();
    assertTrue(store.pointerStore.compareAndDelete(lookupKey, lookupPointer.getVersion()));

    var lease = store.leaseNext().orElseThrow();

    assertEquals(jobId, lease.jobId);
    assertTrue(store.pointerStore.get(lookupKey).isEmpty());
  }

  @Test
  void leaseNextAtomicallyWritesLeaseRowAndExpiryIndex() {
    assumeMemoryOnly("pointer lease row assertions are only meaningful in memory mode");
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());

    var lease = store.leaseNext().orElseThrow();

    String leaseKey = Keys.reconcileJobLeasePointerById(ACCOUNT_ID, jobId);
    Pointer leasePointer = store.pointerStore.get(leaseKey).orElseThrow();
    StoredJobLease storedLease = readStoredLease(ACCOUNT_ID, jobId);
    String expiryKey = leaseExpiryPointerKey(storedLease.expiresAtMs, ACCOUNT_ID, jobId);

    assertEquals(lease.leaseEpoch, storedLease.epoch);
    assertEquals(
        Keys.reconcileJobPointerById(ACCOUNT_ID, jobId),
        store.pointerStore.get(expiryKey).orElseThrow().getBlobUri());
  }

  @Test
  void renewLeaseMovesExpiryIndexAtomically() throws Exception {
    assumeMemoryOnly("pointer lease expiry assertions are only meaningful in memory mode");
    System.setProperty("floecat.reconciler.job-store.lease-ms", "200");
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());

    var lease = store.leaseNext().orElseThrow();
    String leaseKey = Keys.reconcileJobLeasePointerById(ACCOUNT_ID, jobId);
    StoredJobLease beforeRenew = readStoredLease(ACCOUNT_ID, jobId);
    String oldExpiryKey = leaseExpiryPointerKey(beforeRenew.expiresAtMs, ACCOUNT_ID, jobId);
    awaitNextMillis(System.currentTimeMillis());

    assertTrue(store.renewLease(jobId, lease.leaseEpoch));

    StoredJobLease afterRenew = readStoredLease(ACCOUNT_ID, jobId);
    assertTrue(afterRenew.expiresAtMs > beforeRenew.expiresAtMs);
    String newExpiryKey = leaseExpiryPointerKey(afterRenew.expiresAtMs, ACCOUNT_ID, jobId);
    assertNotEquals(oldExpiryKey, newExpiryKey);
    assertTrue(store.pointerStore.get(oldExpiryKey).isEmpty());
    assertEquals(
        Keys.reconcileJobPointerById(ACCOUNT_ID, jobId),
        store.pointerStore.get(newExpiryKey).orElseThrow().getBlobUri());
  }

  @Test
  void renewLeaseExtendsLeaseAndRewritesExpiryIndexEvenBeforeHalfLife() {
    assumeMemoryOnly("pointer lease expiry assertions are only meaningful in memory mode");
    System.setProperty("floecat.reconciler.job-store.lease-ms", "60000");
    System.setProperty("floecat.reconciler.job-store.base-backoff-ms", "0");
    System.setProperty("floecat.reconciler.job-store.max-backoff-ms", "0");
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());

    var lease = store.leaseNext().orElseThrow();
    String leaseKey = Keys.reconcileJobLeasePointerById(ACCOUNT_ID, jobId);
    Pointer beforeLeasePointer = store.pointerStore.get(leaseKey).orElseThrow();
    StoredJobLease beforeLease = readStoredLease(ACCOUNT_ID, jobId);
    String expiryKey = leaseExpiryPointerKey(beforeLease.expiresAtMs, ACCOUNT_ID, jobId);
    Pointer beforeExpiryPointer = store.pointerStore.get(expiryKey).orElseThrow();

    assertTrue(store.renewLease(jobId, lease.leaseEpoch));

    Pointer afterLeasePointer = store.pointerStore.get(leaseKey).orElseThrow();
    StoredJobLease afterLease = readStoredLease(ACCOUNT_ID, jobId);
    String newExpiryKey = leaseExpiryPointerKey(afterLease.expiresAtMs, ACCOUNT_ID, jobId);
    Pointer afterExpiryPointer = store.pointerStore.get(newExpiryKey).orElseThrow();
    assertTrue(afterLease.expiresAtMs >= beforeLease.expiresAtMs);
    if (newExpiryKey.equals(expiryKey)) {
      assertTrue(store.pointerStore.get(expiryKey).isPresent());
    } else {
      assertTrue(store.pointerStore.get(expiryKey).isEmpty());
    }
    assertTrue(afterLeasePointer.getVersion() >= beforeLeasePointer.getVersion());
    if (afterLeasePointer.getVersion() > beforeLeasePointer.getVersion()) {
      assertNotEquals(beforeLeasePointer.getBlobUri(), afterLeasePointer.getBlobUri());
    }
    assertEquals(Keys.reconcileJobPointerById(ACCOUNT_ID, jobId), afterExpiryPointer.getBlobUri());
  }

  @Test
  void nonTerminalMutationLeavesMissingDedupePointerUnrepaired() {
    assumeMemoryOnly("pointer corruption assertions are only meaningful in memory mode");
    store.init();
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String jobId =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);
    Pointer dedupePointer =
        firstPointerWithPrefix(Keys.reconcileDedupePointerPrefix(ACCOUNT_ID)).orElseThrow();
    assertTrue(
        store.pointerStore.compareAndDelete(dedupePointer.getKey(), dedupePointer.getVersion()));

    var lease = store.leaseNext().orElseThrow();
    store.markRunning(jobId, lease.leaseEpoch, System.currentTimeMillis(), "exec-1");

    assertTrue(store.pointerStore.get(dedupePointer.getKey()).isEmpty());
    assertEquals("JS_RUNNING", store.get(ACCOUNT_ID, jobId).orElseThrow().state);
  }

  @Test
  void enqueueAllocatesNewJobWhenCanonicalStateIsCorrupt() {
    assumeMemoryOnly("pointer corruption assertions are only meaningful in memory mode");
    store.init();
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String jobId =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);

    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    Pointer canonicalPointer = store.pointerStore.get(canonicalPointerKey).orElseThrow();
    assertTrue(
        store.pointerStore.compareAndSet(
            canonicalPointerKey,
            canonicalPointer.getVersion(),
            Pointer.newBuilder()
                .setKey(canonicalPointerKey)
                .setBlobUri("inline:reconcile-job:corrupt")
                .setVersion(canonicalPointer.getVersion() + 1L)
                .build()));

    String dedupedJobId =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);

    assertNotEquals(jobId, dedupedJobId);
    assertTrue(store.pointerStore.get(canonicalPointerKey).isPresent());
  }

  @Test
  void maintenanceRequeuesExpiredRunningJobAndRecreatesReadyPointer() throws Exception {
    assumeMemoryOnly("pointer mutation assertions are only meaningful in memory mode");
    System.setProperty("floecat.reconciler.job-store.lease-ms", "1000");
    System.setProperty("floecat.reconciler.job-store.reclaim-interval-ms", "1");
    System.setProperty("floecat.reconciler.job-store.lease-renew-grace-ms", "0");
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    var lease = store.leaseNext().orElseThrow();
    store.markRunning(jobId, lease.leaseEpoch, System.currentTimeMillis(), "exec-1");

    StoredReconcileJob running = readStoredRecord(canonicalPointerKey);
    running.readyPointerKey = "";
    overwriteCanonicalRecordWithoutSync(canonicalPointerKey, running);

    forceLeaseExpired(jobId);

    assertTrue(
        store
            .leaseNext(
                ReconcileJobStore.LeaseRequest.of(
                    java.util.EnumSet.of(ReconcileExecutionClass.HEAVY),
                    java.util.Set.of("remote")))
            .isEmpty());
    store.runMaintenanceOnce(1_000L);

    StoredReconcileJob requeued = readStoredRecord(canonicalPointerKey);
    assertEquals("JS_QUEUED", requeued.state);
    assertFalse(requeued.readyPointerKey.isBlank());
    assertTrue(readyEntryExists(requeued.readyPointerKey));
  }

  @Test
  void maintenanceReclaimUsesLeaseExpiryIndexWithoutGlobalCanonicalScan() throws Exception {
    assumeMemoryOnly("pointer scan-count assertions are only meaningful in memory mode");
    TrackingPointerStore pointerStore = new TrackingPointerStore();
    store.pointerStore = pointerStore;
    System.setProperty("floecat.reconciler.job-store.lease-ms", "1000");
    System.setProperty("floecat.reconciler.job-store.reclaim-interval-ms", "1");
    System.setProperty("floecat.reconciler.job-store.lease-renew-grace-ms", "0");
    System.setProperty("floecat.reconciler.job-store.base-backoff-ms", "0");
    System.setProperty("floecat.reconciler.job-store.max-backoff-ms", "0");
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);

    var lease = store.leaseNext().orElseThrow();
    store.markRunning(jobId, lease.leaseEpoch, System.currentTimeMillis(), "exec-1");
    pointerStore.resetListCounts();

    forceLeaseExpired(jobId);

    store.runMaintenanceOnce(1_000L);
    StoredReconcileJob reclaimed = readStoredRecord(canonicalPointerKey);
    assertEquals("JS_QUEUED", reclaimed.state);
    assertTrue(reclaimed.readyPointerKey != null && !reclaimed.readyPointerKey.isBlank());
    assertEquals(0, pointerStore.listCount("/accounts/"));
    assertTrue(pointerStore.listCount(LEASE_EXPIRY_POINTER_PREFIX) > 0);
  }

  @Test
  void enqueuePreservesScopedCaptureRequests() {
    store.init();
    ReconcileScope scope =
        ReconcileScope.of(
            List.of(),
            "tbl",
            List.of(scopedCaptureRequest("tbl", 42L, "table", List.of("c1", "#3"))));

    String jobId =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);

    ReconcileJob job = store.get(jobId).orElseThrow();
    assertEquals(
        List.of(scopedCaptureRequest("tbl", 42L, "table", List.of("#3", "c1"))),
        job.scope.destinationCaptureRequests());
  }

  @Test
  void enqueuePreservesExecutionPolicyAndPinnedExecutorAcrossLeaseAndAssignment() {
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileExecutionPolicy.of(
                ReconcileExecutionClass.HEAVY, "remote", java.util.Map.of("tier", "gold")),
            "remote-executor");

    ReconcileJob job = store.get(jobId).orElseThrow();
    assertEquals(ReconcileExecutionClass.HEAVY, job.executionPolicy.executionClass());
    assertEquals("remote", job.executionPolicy.lane());
    assertEquals("gold", job.executionPolicy.attributes().get("tier"));
    assertEquals("", job.executorId);

    var lease =
        store
            .leaseNext(
                ReconcileJobStore.LeaseRequest.of(
                    java.util.EnumSet.of(ReconcileExecutionClass.HEAVY),
                    java.util.Set.of("remote"),
                    java.util.Set.of("remote-executor")))
            .orElseThrow();
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
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    StoredReconcileJob queuedRecord =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, jobId));
    String lanePointerKey = Keys.reconcileLaneLeasePointer(ACCOUNT_ID, queuedRecord.laneKey);
    var firstLease = store.leaseNext().orElseThrow();

    store.markFailed(
        jobId, firstLease.leaseEpoch, System.currentTimeMillis(), "transient", 1, 0, 1, 2, 3);
    ReconcileJob retried =
        waitForValue(
            () -> store.get(jobId).orElseThrow(),
            current -> "JS_QUEUED".equals(current.state) && !leaseOwnerEntryExists(lanePointerKey),
            "retryable failure to requeue " + jobId);
    assertEquals("JS_QUEUED", retried.state);
    assertFalse(leaseOwnerEntryExists(lanePointerKey));

    forceQueuedJobDueNow(jobId);
    var secondLease = leaseJob(jobId);
    store.markFailed(
        jobId, secondLease.leaseEpoch, System.currentTimeMillis(), "terminal", 1, 0, 2, 2, 3);
    ReconcileJob failed =
        waitForValue(
            () -> store.get(jobId).orElseThrow(),
            current -> "JS_FAILED".equals(current.state),
            "terminal failure " + jobId);
    assertEquals("JS_FAILED", failed.state);
  }

  @Test
  void markFailedPreservesViewTaskContext() {
    System.setProperty("floecat.reconciler.job-store.max-attempts", "1");
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileJobKind.PLAN_VIEW,
            ReconcileTableTask.empty(),
            ReconcileViewTask.of("src_ns", "src_view", "dst-ns-id", "dst-view-id"),
            ReconcileExecutionPolicy.defaults(),
            "",
            "");
    var lease = store.leaseNext().orElseThrow();

    store.markFailed(jobId, lease.leaseEpoch, System.currentTimeMillis(), "boom", 0, 0, 1, 0, 1);

    ReconcileJob failed = store.get(jobId).orElseThrow();
    assertEquals("JS_FAILED", failed.state);
    assertEquals("src_ns", failed.viewTask.sourceNamespace());
    assertEquals("src_view", failed.viewTask.sourceView());
    assertEquals("dst-ns-id", failed.viewTask.destinationNamespaceId());
    assertEquals("dst-view-id", failed.viewTask.destinationViewId());
  }

  @Test
  void markSucceededClearsSnapshotLeasePointer() {
    assumeMemoryOnly("snapshot owner pointer assertions are only meaningful in memory mode");
    store.init();

    String jobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "events"),
            ReconcileExecutionPolicy.defaults(),
            "parent-a",
            "");

    String snapshotLeasePointerKey = Keys.reconcileSnapshotLeasePointer("table-1", 55L);
    var lease = store.leaseNext().orElseThrow();
    assertTrue(store.pointerStore.get(snapshotLeasePointerKey).isPresent());

    store.markSucceeded(jobId, lease.leaseEpoch, System.currentTimeMillis(), 1, 1, 1, 1);

    assertTrue(store.pointerStore.get(snapshotLeasePointerKey).isEmpty());
  }

  @Test
  void cancelQueuedJobClearsStaleLaneAndSnapshotPointersWithoutLease() {
    assumeMemoryOnly("owner pointer cleanup assertions are only meaningful in memory mode");
    store.init();

    String jobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "events"),
            ReconcileExecutionPolicy.defaults(),
            "parent-a",
            "");
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    StoredReconcileJob queuedRecord = readStoredRecord(canonicalPointerKey);
    String lanePointerKey = Keys.reconcileLaneLeasePointer(ACCOUNT_ID, queuedRecord.laneKey);
    String snapshotLeasePointerKey = Keys.reconcileSnapshotLeasePointer("table-1", 55L);

    assertTrue(
        store.pointerStore.compareAndSet(
            lanePointerKey,
            0L,
            Pointer.newBuilder()
                .setKey(lanePointerKey)
                .setBlobUri(canonicalPointerKey)
                .setVersion(1L)
                .build()));
    assertTrue(
        store.pointerStore.compareAndSet(
            snapshotLeasePointerKey,
            0L,
            Pointer.newBuilder()
                .setKey(snapshotLeasePointerKey)
                .setBlobUri(canonicalPointerKey)
                .setVersion(1L)
                .build()));

    ReconcileJob cancelled = store.cancel(ACCOUNT_ID, jobId, "stop").orElseThrow();

    assertEquals("JS_CANCELLED", cancelled.state);
    assertTrue(store.pointerStore.get(lanePointerKey).isEmpty());
    assertTrue(store.pointerStore.get(snapshotLeasePointerKey).isEmpty());
  }

  @Test
  void cancelQueuedJobClearsStaleLanePointerWithoutLease() {
    assumeMemoryOnly("owner pointer cleanup assertions are only meaningful in memory mode");
    store.init();
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String jobId =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    StoredReconcileJob queuedRecord = readStoredRecord(canonicalPointerKey);
    String lanePointerKey = Keys.reconcileLaneLeasePointer(ACCOUNT_ID, queuedRecord.laneKey);

    assertTrue(
        store.pointerStore.compareAndSet(
            lanePointerKey,
            0L,
            Pointer.newBuilder()
                .setKey(lanePointerKey)
                .setBlobUri(canonicalPointerKey)
                .setVersion(1L)
                .build()));

    ReconcileJob cancelled = store.cancel(ACCOUNT_ID, jobId, "stop").orElseThrow();

    assertEquals("JS_CANCELLED", cancelled.state);
    assertTrue(store.pointerStore.get(lanePointerKey).isEmpty());
  }

  @Test
  void getIsScopedToAccount() {
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
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
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "t1"));
    String second =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "t2"));

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
  void listByConnectorUsesConnectorIndexInsteadOfAccountWideScan() {
    assumeMemoryOnly("pointer list-count assertions are only meaningful in memory mode");
    TrackingPointerStore pointerStore = new TrackingPointerStore();
    store.pointerStore = pointerStore;
    store.init();

    store.enqueue(
        ACCOUNT_ID,
        CONNECTOR_ID,
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.of(List.of(), "c1-t1"));
    store.enqueue(
        ACCOUNT_ID,
        CONNECTOR_ID,
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.of(List.of(), "c1-t2"));
    store.enqueue(
        ACCOUNT_ID,
        "conn-2",
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.of(List.of(), "c2-t1"));

    pointerStore.resetListCounts();

    var page = store.list(ACCOUNT_ID, 10, "", CONNECTOR_ID, java.util.Set.of());

    assertEquals(2, page.jobs.size());
    assertEquals(
        0,
        pointerStore.listCount(Keys.reconcileJobPointerByIdPrefix(ACCOUNT_ID)),
        "connector listing should not scan account-wide canonical rows");
    assertTrue(
        pointerStore.listCount(Keys.reconcileJobByConnectorPointerPrefix(ACCOUNT_ID, CONNECTOR_ID))
            > 0);
  }

  @Test
  void listByConnectorPaginatesNewestFirstFromConnectorIndex() throws Exception {
    store.init();

    String first =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "first"));
    awaitNextMillis(readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, first)).createdAtMs);
    String second =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "second"));
    awaitNextMillis(readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, second)).createdAtMs);
    String third =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "third"));

    var firstPage =
        waitForValue(
            () -> store.list(ACCOUNT_ID, 2, "", CONNECTOR_ID, java.util.Set.of()),
            current -> current.jobs.size() == 2,
            "first connector history page");
    assertEquals(List.of(third, second), firstPage.jobs.stream().map(job -> job.jobId).toList());
    assertFalse(firstPage.nextPageToken.isBlank());

    var secondPage =
        waitForValue(
            () ->
                store.list(
                    ACCOUNT_ID, 2, firstPage.nextPageToken, CONNECTOR_ID, java.util.Set.of()),
            current -> current.jobs.size() == 1,
            "second connector history page");
    assertEquals(List.of(first), secondPage.jobs.stream().map(job -> job.jobId).toList());
    assertEquals("", secondPage.nextPageToken);
  }

  @Test
  void listByConnectorKeepsTerminalJobsInHistory() {
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "terminal"));

    var lease = store.leaseNext().orElseThrow();
    assertEquals(jobId, lease.jobId);
    store.markSucceeded(jobId, lease.leaseEpoch, System.currentTimeMillis(), 1, 1, 1, 1);

    var page =
        waitForValue(
            () -> store.list(ACCOUNT_ID, 10, "", CONNECTOR_ID, java.util.Set.of()),
            current ->
                current.jobs.stream()
                    .anyMatch(job -> jobId.equals(job.jobId) && "JS_SUCCEEDED".equals(job.state)),
            "connector history retains succeeded terminal job");

    assertTrue(page.jobs.stream().anyMatch(job -> jobId.equals(job.jobId)));
    assertEquals(
        "JS_SUCCEEDED",
        page.jobs.stream().filter(job -> jobId.equals(job.jobId)).findFirst().orElseThrow().state);
  }

  @Test
  void listByConnectorStateFilteringStillWorks() {
    store.init();

    String queuedJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "queued"));
    String succeededJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "done"));

    var lease = store.leaseNext().orElseThrow();
    String leasedJobId = lease.jobId;
    if (!succeededJobId.equals(leasedJobId)) {
      lease = store.leaseNext().orElseThrow();
      leasedJobId = lease.jobId;
    }
    store.markSucceeded(
        leasedJobId, lease.leaseEpoch, System.currentTimeMillis(), 0, 0, 0, 0, 0, 0);

    var page =
        waitForValue(
            () -> store.list(ACCOUNT_ID, 10, "", CONNECTOR_ID, java.util.Set.of("JS_SUCCEEDED")),
            current ->
                current.jobs.stream()
                    .map(job -> job.jobId)
                    .toList()
                    .equals(List.of(succeededJobId)),
            "connector succeeded-state listing");

    assertEquals(List.of(succeededJobId), page.jobs.stream().map(job -> job.jobId).toList());
    assertTrue(page.jobs.stream().noneMatch(job -> queuedJobId.equals(job.jobId)));
  }

  @Test
  void listRootJobsUsesProjectedRootSummaryIndex() {
    store.init();

    String connectorJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "root"));
    Optional<ReconcileJob> waiting =
        waitForValue(
            () ->
                store
                    .listRootJobs(ACCOUNT_ID, 20, "", CONNECTOR_ID, java.util.Set.of())
                    .jobs
                    .stream()
                    .filter(job -> connectorJobId.equals(job.jobId))
                    .findFirst(),
            Optional::isPresent,
            "root summary list waits for projected connector state");

    assertEquals("JS_QUEUED", waiting.orElseThrow().state);
    assertTrue(waiting.orElseThrow().parentJobId.isBlank());
  }

  @Test
  void listRootJobsRepairsTerminalSummaryMissingFinishedAtMs() {
    assumeMemoryOnly("root-summary repair is injected directly in memory mode");
    store.init();

    String connectorJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    String tableJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("db", "orders", "table-1", "orders"),
            ReconcileExecutionPolicy.defaults(),
            connectorJobId,
            "");

    ReconcileJobStore.LeasedJob connectorLease =
        leaseJob(connectorJobId, ReconcileJobKind.PLAN_CONNECTOR);
    store.markRunning(connectorJobId, connectorLease.leaseEpoch, 100L, "executor-connector");
    assertTrue(
        store.applyLeaseOutcome(
            connectorJobId,
            connectorLease.leaseEpoch,
            ReconcileJobStore.CompletionKind.SUCCEEDED_WAITING,
            150L,
            "Waiting on child work",
            1L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L));

    ReconcileJobStore.LeasedJob tableLease = leaseJob(tableJobId, ReconcileJobKind.PLAN_TABLE);
    store.markRunning(tableJobId, tableLease.leaseEpoch, 200L, "executor-table");
    store.markSucceeded(tableJobId, tableLease.leaseEpoch, 300L, 1L, 1L, 0L, 0L);

    ReconcileJob terminal =
        waitForValue(
            () ->
                store.listRootJobs(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
                    .filter(job -> connectorJobId.equals(job.jobId))
                    .findFirst()
                    .orElseThrow(),
            job -> "JS_SUCCEEDED".equals(job.state) && job.finishedAtMs == 300L,
            "terminal root summary before corruption");

    StoredReconcileJob canonical =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId));
    ReconcilePayloadStore payloadStore = new ReconcilePayloadStore();
    payloadStore.bind(store.blobStore, store.pointerStore, store.mapper);
    ReconcileJobRootSummaryStore rootSummaryStore = new ReconcileJobRootSummaryStore();
    rootSummaryStore.bind(store.pointerStore, payloadStore);
    rootSummaryStore.upsert(
        new StoredReconcileJobListSummary(
            ACCOUNT_ID,
            connectorJobId,
            CONNECTOR_ID,
            "JS_SUCCEEDED",
            "Succeeded",
            terminal.startedAtMs,
            0L,
            terminal.tablesScanned,
            terminal.tablesChanged,
            terminal.viewsScanned,
            terminal.viewsChanged,
            terminal.errors,
            terminal.fullRescan,
            terminal.captureMode,
            terminal.snapshotsProcessed,
            terminal.statsProcessed,
            terminal.indexesProcessed,
            terminal.executorId,
            terminal.executionPolicy.executionClass(),
            terminal.executionPolicy.lane(),
            terminal.executionPolicy.attributes(),
            terminal.jobKind,
            terminal.plannedFileGroups,
            terminal.plannedFiles,
            terminal.completedFileGroups,
            terminal.failedFileGroups,
            terminal.completedFiles,
            terminal.failedFiles,
            canonical.createdAtMs));

    ReconcileJob repaired =
        store.listRootJobs(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
            .filter(job -> connectorJobId.equals(job.jobId))
            .findFirst()
            .orElseThrow();
    assertEquals("JS_SUCCEEDED", repaired.state);
    assertEquals(300L, repaired.finishedAtMs);
  }

  @Test
  void listByConnectorIgnoresStaleConnectorIndexPointers() {
    assumeMemoryOnly("pointer corruption assertions are only meaningful in memory mode");
    store.init();

    String staleJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "stale"));
    String liveJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "live"));

    String staleCanonicalKey = Keys.reconcileJobPointerById(ACCOUNT_ID, staleJobId);
    Pointer staleCanonicalPointer = store.pointerStore.get(staleCanonicalKey).orElseThrow();
    String staleIndexKey = readStoredRecord(staleCanonicalKey).connectorIndexPointerKey;
    assertFalse(staleIndexKey.isBlank());
    assertTrue(
        store.pointerStore.compareAndDelete(staleCanonicalKey, staleCanonicalPointer.getVersion()));

    var page = store.list(ACCOUNT_ID, 10, "", CONNECTOR_ID, java.util.Set.of());

    assertEquals(List.of(liveJobId), page.jobs.stream().map(job -> job.jobId).toList());
    assertTrue(store.pointerStore.get(staleIndexKey).isPresent());
  }

  @Test
  void listByConnectorStateFilteringPaginatesAcrossIndexPages() {
    assumeMemoryOnly("legacy page-token assertions are only meaningful in memory mode");
    store.init();

    String succeededJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "succeeded"));
    var lease = store.leaseNext().orElseThrow();
    assertEquals(succeededJobId, lease.jobId);
    store.markSucceeded(succeededJobId, lease.leaseEpoch, System.currentTimeMillis(), 1, 1, 1, 1);

    for (int i = 0; i < 70; i++) {
      store.enqueue(
          ACCOUNT_ID,
          CONNECTOR_ID,
          false,
          CaptureMode.METADATA_AND_CAPTURE,
          ReconcileScope.of(List.of(), "queued-" + i));
    }

    var page =
        waitForValue(
            () -> store.list(ACCOUNT_ID, 1, "", CONNECTOR_ID, java.util.Set.of("JS_SUCCEEDED")),
            current ->
                current.jobs.stream().map(job -> job.jobId).toList().equals(List.of(succeededJobId))
                    && current.nextPageToken.isBlank(),
            "connector succeeded-state pagination across index pages");

    assertEquals(List.of(succeededJobId), page.jobs.stream().map(job -> job.jobId).toList());
    assertEquals("", page.nextPageToken);
  }

  @Test
  void listByConnectorMultiStateFilteringPaginatesAcrossStates() {
    assumeMemoryOnly("legacy page-token assertions are only meaningful in memory mode");
    store.init();

    String succeededJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "succeeded"));

    var lease = store.leaseNext().orElseThrow();
    if (!succeededJobId.equals(lease.jobId)) {
      lease = store.leaseNext().orElseThrow();
    }
    assertEquals(succeededJobId, lease.jobId);
    store.markSucceeded(succeededJobId, lease.leaseEpoch, System.currentTimeMillis(), 1, 1, 1, 1);

    String queuedJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "queued"));

    var firstPage =
        waitForValue(
            () ->
                store.list(
                    ACCOUNT_ID, 1, "", CONNECTOR_ID, java.util.Set.of("JS_SUCCEEDED", "JS_QUEUED")),
            current -> current.jobs.size() == 1 && !current.nextPageToken.isBlank(),
            "first connector multi-state page");

    var secondPage =
        waitForValue(
            () ->
                store.list(
                    ACCOUNT_ID,
                    1,
                    firstPage.nextPageToken,
                    CONNECTOR_ID,
                    java.util.Set.of("JS_SUCCEEDED", "JS_QUEUED")),
            current ->
                java.util.stream.Stream.concat(firstPage.jobs.stream(), current.jobs.stream())
                        .map(job -> job.jobId)
                        .collect(java.util.stream.Collectors.toSet())
                        .equals(java.util.Set.of(queuedJobId, succeededJobId))
                    && current.nextPageToken.isBlank(),
            "second connector multi-state page");

    assertEquals(
        java.util.Set.of(queuedJobId, succeededJobId),
        java.util.stream.Stream.concat(firstPage.jobs.stream(), secondPage.jobs.stream())
            .map(job -> job.jobId)
            .collect(java.util.stream.Collectors.toSet()));
    assertTrue(secondPage.nextPageToken.isBlank());
  }

  @Test
  void listByConnectorDoesNotBackfillCanonicalOnlyJobs() {
    assumeMemoryOnly("pointer corruption assertions are only meaningful in memory mode");
    store.init();

    String indexedJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "indexed"));
    String canonicalOnlyJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "canonical-only"));

    String canonicalOnlyKey = Keys.reconcileJobPointerById(ACCOUNT_ID, canonicalOnlyJobId);
    StoredReconcileJob canonicalOnlyRecord = readStoredRecord(canonicalOnlyKey);
    String connectorIndexKey = canonicalOnlyRecord.connectorIndexPointerKey;
    assertFalse(connectorIndexKey.isBlank());
    Pointer connectorIndexPointer = store.pointerStore.get(connectorIndexKey).orElseThrow();
    assertTrue(
        store.pointerStore.compareAndDelete(connectorIndexKey, connectorIndexPointer.getVersion()));
    canonicalOnlyRecord.connectorIndexPointerKey = "";
    overwriteCanonicalRecordWithoutSync(canonicalOnlyKey, canonicalOnlyRecord);

    var page = store.list(ACCOUNT_ID, 10, "", CONNECTOR_ID, java.util.Set.of());

    assertTrue(page.jobs.stream().anyMatch(job -> indexedJobId.equals(job.jobId)));
    assertTrue(page.jobs.stream().noneMatch(job -> canonicalOnlyJobId.equals(job.jobId)));
  }

  @Test
  void listByConnectorStateFilteringUsesStateIndexWithoutPageCapScan() {
    assumeMemoryOnly("pointer pagination assertions are only meaningful in memory mode");
    SinglePointerPageStore pointerStore = new SinglePointerPageStore();
    store.pointerStore = pointerStore;
    store.init();

    for (int i = 0; i < 1001; i++) {
      store.enqueue(
          ACCOUNT_ID,
          CONNECTOR_ID,
          false,
          CaptureMode.METADATA_AND_CAPTURE,
          ReconcileScope.of(List.of(), "queued-cap-" + i));
    }

    TestLogHandler handler = new TestLogHandler();
    org.jboss.logmanager.Logger logger =
        org.jboss.logmanager.Logger.getLogger(DurableReconcileJobStore.class.getName());
    logger.addHandler(handler);
    try {
      var page = store.list(ACCOUNT_ID, 1, "", CONNECTOR_ID, java.util.Set.of("JS_SUCCEEDED"));

      assertTrue(page.jobs.isEmpty());
      assertTrue(page.nextPageToken.isBlank());
      assertTrue(
          handler.messages().stream()
              .noneMatch(message -> message.contains("Connector reconcile job list hit page cap")));
    } finally {
      logger.removeHandler(handler);
      handler.close();
    }
  }

  @Test
  void listAccountWideMultiStateFilteringUsesStateIndexesWithoutPageCapScan() {
    assumeMemoryOnly("pointer pagination assertions are only meaningful in memory mode");
    SinglePointerPageStore pointerStore = new SinglePointerPageStore();
    store.pointerStore = pointerStore;
    store.init();

    for (int i = 0; i < 1001; i++) {
      store.enqueue(
          ACCOUNT_ID,
          CONNECTOR_ID,
          false,
          CaptureMode.METADATA_AND_CAPTURE,
          ReconcileScope.of(List.of(), "queued-cap-" + i));
    }

    TestLogHandler handler = new TestLogHandler();
    org.jboss.logmanager.Logger logger =
        org.jboss.logmanager.Logger.getLogger(DurableReconcileJobStore.class.getName());
    logger.addHandler(handler);
    try {
      var page = store.list(ACCOUNT_ID, 1, "", "", java.util.Set.of("JS_RUNNING", "JS_SUCCEEDED"));

      assertTrue(page.jobs.isEmpty());
      assertTrue(page.nextPageToken.isBlank());
      assertTrue(
          handler.messages().stream()
              .noneMatch(
                  message -> message.contains("Account-wide reconcile job list hit page cap")));
    } finally {
      logger.removeHandler(handler);
      handler.close();
    }
  }

  @Test
  void listDoesNotReadDefinitionBlobsForSummaryRows() {
    CountingBlobStore blobStore = new CountingBlobStore();
    store.blobStore = blobStore;
    store.init();

    store.enqueue(
        ACCOUNT_ID,
        CONNECTOR_ID,
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.of(List.of(), "table-1"));
    store.enqueue(
        ACCOUNT_ID,
        CONNECTOR_ID,
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.of(List.of(), "table-2"));
    blobStore.resetGetCount();

    var page = store.list(ACCOUNT_ID, 10, "", CONNECTOR_ID, java.util.Set.of());

    assertEquals(2, page.jobs.size());
    assertEquals(0, blobStore.getCount());
    assertEquals(ReconcileScope.empty(), page.jobs.getFirst().scope);
    assertEquals(ReconcileTableTask.empty(), page.jobs.getFirst().tableTask);
    assertEquals(ReconcileViewTask.empty(), page.jobs.getFirst().viewTask);
    assertEquals(ReconcileSnapshotTask.empty(), page.jobs.getFirst().snapshotTask);
    assertEquals(ReconcileFileGroupTask.empty(), page.jobs.getFirst().fileGroupTask);
  }

  @Test
  void listByConnectorSummaryDoesNotReadDefinitionSnapshotOrFileGroupBlobs() {
    CountingBlobStore blobStore = new CountingBlobStore();
    store.blobStore = blobStore;
    store.init();

    store.enqueue(
        ACCOUNT_ID,
        CONNECTOR_ID,
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.empty(),
        ReconcileJobKind.PLAN_SNAPSHOT,
        ReconcileTableTask.empty(),
        ReconcileViewTask.empty(),
        ReconcileSnapshotTask.of(
            "table-1",
            11L,
            "src",
            "orders",
            List.of(
                ReconcileFileGroupTask.of(
                    "plan-1", "group-1", "table-1", 11L, List.of("s3://bucket/orders-1.parquet"))),
            true),
        ReconcileFileGroupTask.empty(),
        ReconcileExecutionPolicy.defaults(),
        "",
        "");
    store.enqueue(
        ACCOUNT_ID,
        CONNECTOR_ID,
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.empty(),
        ReconcileJobKind.EXEC_FILE_GROUP,
        ReconcileTableTask.empty(),
        ReconcileViewTask.empty(),
        ReconcileSnapshotTask.empty(),
        ReconcileFileGroupTask.of(
            "plan-2",
            "group-2",
            "table-2",
            22L,
            List.of("s3://bucket/orders-2.parquet"),
            List.of(
                ReconcileFileResult.succeeded(
                    "s3://bucket/orders-2.parquet",
                    123L,
                    ReconcileIndexArtifactResult.of(
                        "s3://bucket/orders-2.stats", "application/json", 1)))),
        ReconcileExecutionPolicy.defaults(),
        "",
        "");
    blobStore.resetGetCount();

    var page = store.list(ACCOUNT_ID, 10, "", CONNECTOR_ID, java.util.Set.of());

    assertEquals(2, page.jobs.size());
    assertEquals(0, blobStore.getCount());
    assertEquals(ReconcileScope.empty(), page.jobs.getFirst().scope);
    assertEquals(ReconcileTableTask.empty(), page.jobs.getFirst().tableTask);
    assertEquals(ReconcileViewTask.empty(), page.jobs.getFirst().viewTask);
    assertEquals(ReconcileSnapshotTask.empty(), page.jobs.getFirst().snapshotTask);
    assertEquals(ReconcileFileGroupTask.empty(), page.jobs.getFirst().fileGroupTask);
  }

  @Test
  void expiredLeasesAreReclaimedAndRequeuedByMaintenance() throws Exception {
    assumeMemoryOnly("legacy lease-expiry pointer assertions are only meaningful in memory mode");
    System.setProperty("floecat.reconciler.job-store.lease-ms", "1000");
    System.setProperty("floecat.reconciler.job-store.reclaim-interval-ms", "1000");
    System.setProperty("floecat.reconciler.job-store.lease-renew-grace-ms", "0");
    System.setProperty("floecat.reconciler.job-store.base-backoff-ms", "0");
    System.setProperty("floecat.reconciler.job-store.max-backoff-ms", "0");
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    var firstLease = store.leaseNext();
    assertTrue(firstLease.isPresent());
    assertEquals(jobId, firstLease.get().jobId);
    store.markRunning(
        firstLease.get().jobId,
        firstLease.get().leaseEpoch,
        System.currentTimeMillis(),
        "default_reconciler");

    forceLeaseExpired(jobId);

    store.runMaintenanceOnce(1_000L);
    StoredReconcileJob reclaimed =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, jobId));
    assertEquals("JS_QUEUED", reclaimed.state);
    assertTrue(reclaimed.readyPointerKey != null && !reclaimed.readyPointerKey.isBlank());
  }

  @Test
  void expiredLeaseReclaimAppliesBackoffBeforeRequeue() throws Exception {
    assumeMemoryOnly("legacy lease-expiry pointer assertions are only meaningful in memory mode");
    System.setProperty("floecat.reconciler.job-store.lease-ms", "1000");
    System.setProperty("floecat.reconciler.job-store.reclaim-interval-ms", "1000");
    System.setProperty("floecat.reconciler.job-store.lease-renew-grace-ms", "0");
    System.setProperty("floecat.reconciler.job-store.base-backoff-ms", "1000");
    System.setProperty("floecat.reconciler.job-store.max-backoff-ms", "1000");
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    var firstLease = store.leaseNext();
    assertTrue(firstLease.isPresent());
    assertEquals(jobId, firstLease.get().jobId);

    forceLeaseExpired(jobId);

    store.runMaintenanceOnce(1_000L);
    assertTrue(store.leaseNext().isEmpty());

    forceQueuedJobDueNow(jobId);
    var secondLease = store.leaseNext();
    assertTrue(secondLease.isPresent());
    assertEquals(jobId, secondLease.get().jobId);
  }

  @Test
  void leaseNextLeavesLookupPointerMissingWhenReadyPointerStillExists() {
    assumeMemoryOnly("pointer corruption assertions are only meaningful in memory mode");
    System.setProperty("floecat.reconciler.job-store.reclaim-interval-ms", "1");
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    String lookupKey = Keys.reconcileJobLookupPointerById(jobId);
    Pointer lookupPointer = store.pointerStore.get(lookupKey).orElseThrow();
    assertTrue(store.pointerStore.compareAndDelete(lookupKey, lookupPointer.getVersion()));

    var leased = store.leaseNext().orElseThrow();
    assertEquals(jobId, leased.jobId);
    assertTrue(store.pointerStore.get(lookupKey).isEmpty());
  }

  @Test
  void cancellingLeaseExpiryFinalizesCancellation() throws Exception {
    assumeMemoryOnly("legacy lease-expiry pointer assertions are only meaningful in memory mode");
    System.setProperty("floecat.reconciler.job-store.lease-ms", "1000");
    System.setProperty("floecat.reconciler.job-store.reclaim-interval-ms", "0");
    System.setProperty("floecat.reconciler.job-store.lease-renew-grace-ms", "0");
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    var firstLease = store.leaseNext().orElseThrow();
    store.markRunning(
        firstLease.jobId, firstLease.leaseEpoch, System.currentTimeMillis(), "default_reconciler");
    store.cancel(ACCOUNT_ID, jobId, "stop");
    assertEquals(
        "JS_CANCELLING",
        waitForValue(
                () -> store.get(jobId).orElseThrow(),
                current -> "JS_CANCELLING".equals(current.state),
                "job entering cancelling state " + jobId)
            .state);

    forceLeaseExpired(jobId);

    assertTrue(store.leaseNext().isEmpty());
    store.runMaintenanceOnce(1_000L);

    var cancelled =
        waitForValue(
            () -> store.get(jobId).orElseThrow(),
            current -> "JS_CANCELLED".equals(current.state),
            "job cancellation finalization " + jobId);
    assertEquals("JS_CANCELLED", cancelled.state);
    assertEquals("stop", cancelled.message);
    assertTrue(cancelled.finishedAtMs > 0L);
    assertFalse(anyReadyEntryMatches(key -> key.contains(jobId)));
  }

  @Test
  void cancelPokesLeaseExpiryForFasterReclaim() throws Exception {
    assumeMemoryOnly("legacy lease-expiry pointer assertions are only meaningful in memory mode");
    assumeMemoryOnly("legacy lease-expiry pointer assertions are only meaningful in memory mode");
    System.setProperty("floecat.reconciler.job-store.lease-ms", "5000");
    System.setProperty("floecat.reconciler.job-store.reclaim-interval-ms", "200");
    System.setProperty("floecat.reconciler.job-store.lease-renew-grace-ms", "0");
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    var firstLease = store.leaseNext().orElseThrow();
    store.markRunning(
        firstLease.jobId, firstLease.leaseEpoch, System.currentTimeMillis(), "default_reconciler");
    long originalExpiresAtMs = readStoredLease(ACCOUNT_ID, jobId).expiresAtMs;
    store.cancel(ACCOUNT_ID, jobId, "stop");
    assertFalse(store.leaseNext().isPresent());

    // Cancel should poke lease expiry, allowing reclaim well before the original 5s lease.
    StoredJobLease shortenedLease = readStoredLease(ACCOUNT_ID, jobId);
    assertTrue(shortenedLease.expiresAtMs < originalExpiresAtMs);
    assertTrue(shortenedLease.expiresAtMs <= System.currentTimeMillis() + 1_000L);
    forceLeaseExpired(jobId);

    assertTrue(store.leaseNext().isEmpty());
    store.runMaintenanceOnce(1_000L);

    var cancelled = store.get(jobId).orElseThrow();
    assertEquals("JS_CANCELLED", cancelled.state);
    assertEquals("stop", cancelled.message);
    assertFalse(anyReadyEntryMatches(key -> key.contains(jobId)));
  }

  @Test
  void cancelIsIdempotentForCancellingJobs() {
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    var lease = store.leaseNext().orElseThrow();
    store.markRunning(
        lease.jobId, lease.leaseEpoch, System.currentTimeMillis(), "default_reconciler");

    store.cancel(ACCOUNT_ID, jobId, "first stop");
    assertEquals(
        "JS_CANCELLING",
        waitForValue(
                () -> store.get(jobId).orElseThrow(),
                current -> "JS_CANCELLING".equals(current.state),
                "job entering cancelling state " + jobId)
            .state);

    store.cancel(ACCOUNT_ID, jobId, "second stop");
    var stillCancelling =
        waitForValue(
            () -> store.get(jobId).orElseThrow(),
            current -> "JS_CANCELLING".equals(current.state),
            "job remaining cancelling " + jobId);

    assertEquals("JS_CANCELLING", stillCancelling.state);
    assertEquals("first stop", stillCancelling.message);
  }

  @Test
  void cancelRunningJobDoesNotCreateOrRetainReadyPointers() throws Exception {
    assumeMemoryOnly("pointer corruption assertions are only meaningful in memory mode");
    assumeMemoryOnly("pointer corruption assertions are only meaningful in memory mode");
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    var lease = store.leaseNext().orElseThrow();
    store.markRunning(jobId, lease.leaseEpoch, System.currentTimeMillis(), "default_reconciler");

    StoredReconcileJob running = readStoredRecord(canonicalPointerKey);
    long dueAtMs = System.currentTimeMillis();
    StoredReconcileJob staleReadyProjection =
        store.mapper.convertValue(running, StoredReconcileJob.class);
    staleReadyProjection.state = "JS_QUEUED";
    staleReadyProjection.nextAttemptAtMs = dueAtMs;
    List<String> staleReadyKeys = readyPointerKeysFor(staleReadyProjection);
    staleReadyProjection.readyPointerKey = staleReadyKeys.get(0);

    running.nextAttemptAtMs = dueAtMs;
    running.readyPointerKey = staleReadyProjection.readyPointerKey;
    overwriteCanonicalRecordWithoutSync(canonicalPointerKey, running);
    for (String readyKey : staleReadyKeys) {
      assertTrue(
          store.pointerStore.compareAndSet(
              readyKey,
              0L,
              Pointer.newBuilder()
                  .setKey(readyKey)
                  .setBlobUri(canonicalPointerKey)
                  .setVersion(1L)
                  .build()));
    }

    store.cancel(ACCOUNT_ID, jobId, "stop");

    StoredReconcileJob cancelling = readStoredRecord(canonicalPointerKey);
    assertEquals("JS_CANCELLING", cancelling.state);
    assertTrue(cancelling.readyPointerKey == null || cancelling.readyPointerKey.isBlank());
    assertTrue(staleReadyKeys.stream().allMatch(key -> !readyEntryExists(key)));
  }

  @Test
  void maintenanceLeavesMissingReadyPointersUnrepairedForQueuedJob() {
    assumeMemoryOnly("pointer corruption assertions are only meaningful in memory mode");
    assumeMemoryOnly("pointer corruption assertions are only meaningful in memory mode");
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    StoredReconcileJob queued = readStoredRecord(canonicalPointerKey);
    String originalReadyKey = queued.readyPointerKey;

    assertDoesNotThrow(
        () -> {
          for (String readyKey : readyPointerKeysFor(queued)) {
            deletePointerIfPresent(readyKey);
          }
        });
    queued.readyPointerKey = "";
    overwriteCanonicalRecordWithoutSync(canonicalPointerKey, queued);

    assertTrue(store.leaseNext().isEmpty());
    store.runMaintenanceOnce(1_000L);
    assertTrue(store.leaseNext().isEmpty());

    assertFalse(readyEntryExists(originalReadyKey));
  }

  @Test
  void maintenanceDoesNotReindexCancellingJobs() {
    assumeMemoryOnly("ready index pointer assertions are only meaningful in memory mode");
    assumeMemoryOnly("ready index pointer assertions are only meaningful in memory mode");
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    var lease = store.leaseNext().orElseThrow();
    store.markRunning(jobId, lease.leaseEpoch, System.currentTimeMillis(), "default_reconciler");
    store.cancel(ACCOUNT_ID, jobId, "stop");

    StoredReconcileJob cancelling = readStoredRecord(canonicalPointerKey);
    assertEquals("JS_CANCELLING", cancelling.state);
    assertTrue(cancelling.readyPointerKey == null || cancelling.readyPointerKey.isBlank());

    assertTrue(store.leaseNext().isEmpty());
    store.runMaintenanceOnce(1_000L);
    assertEquals("JS_CANCELLING", readStoredRecord(canonicalPointerKey).state);
    assertEquals(0, globalReadyEntryCount());
  }

  @Test
  void renewLeaseExtendsLeaseAndDelaysReclaim() throws Exception {
    assumeMemoryOnly("pointer lease expiry assertions are only meaningful in memory mode");
    assumeMemoryOnly("pointer lease expiry assertions are only meaningful in memory mode");
    System.setProperty("floecat.reconciler.job-store.lease-ms", "2000");
    System.setProperty("floecat.reconciler.job-store.reclaim-interval-ms", "200");
    System.setProperty("floecat.reconciler.job-store.lease-renew-grace-ms", "0");
    System.setProperty("floecat.reconciler.job-store.base-backoff-ms", "0");
    System.setProperty("floecat.reconciler.job-store.max-backoff-ms", "0");
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    var lease = store.leaseNext().orElseThrow();
    store.markRunning(
        lease.jobId, lease.leaseEpoch, System.currentTimeMillis(), "default_reconciler");
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    Pointer beforeRenew = store.pointerStore.get(canonicalPointerKey).orElseThrow();

    awaitNextMillis(System.currentTimeMillis());
    assertTrue(store.renewLease(jobId, lease.leaseEpoch));
    Pointer afterRenew = store.pointerStore.get(canonicalPointerKey).orElseThrow();
    assertEquals(beforeRenew.getVersion(), afterRenew.getVersion());
    assertEquals(beforeRenew.getBlobUri(), afterRenew.getBlobUri());

    assertTrue(store.leaseNext().isEmpty());

    forceLeaseExpired(jobId);
    store.runMaintenanceOnce(1_000L);
    StoredReconcileJob reclaimed = readStoredRecord(canonicalPointerKey);
    assertEquals("JS_QUEUED", reclaimed.state);
    assertTrue(reclaimed.readyPointerKey != null && !reclaimed.readyPointerKey.isBlank());
  }

  @Test
  void renewLeaseRejectsStaleEpoch() {
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    var lease = store.leaseNext().orElseThrow();
    store.markRunning(
        lease.jobId, lease.leaseEpoch, System.currentTimeMillis(), "default_reconciler");

    assertTrue(!store.renewLease(jobId, "stale-epoch"));
  }

  @Test
  void markSucceededClearsPriorFailureStateAfterRetry() throws Exception {
    System.setProperty("floecat.reconciler.job-store.base-backoff-ms", "100");
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());

    ReconcileJobStore.LeasedJob firstLease = store.leaseNext().orElseThrow();
    store.markRunning(jobId, firstLease.leaseEpoch, 100L, "executor-1");
    store.markFailed(jobId, firstLease.leaseEpoch, 200L, "boom", 1L, 1L, 0L, 0L, 3L, 2L, 4L);

    forceQueuedJobDueNow(jobId);

    ReconcileJobStore.LeasedJob secondLease = leaseJob(jobId);
    assertEquals(jobId, secondLease.jobId);
    store.markRunning(jobId, secondLease.leaseEpoch, 300L, "executor-2");
    store.markSucceeded(jobId, secondLease.leaseEpoch, 400L, 5L, 5L, 0L, 0L, 6L, 7L);

    StoredReconcileJob succeeded =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, jobId));
    assertEquals("JS_SUCCEEDED", succeeded.state);
    assertEquals(0L, succeeded.errors);
    assertTrue(succeeded.lastError == null || succeeded.lastError.isBlank());

    ReconcileJob publicJob = store.getLeaseView(jobId).orElseThrow();
    assertEquals("JS_SUCCEEDED", publicJob.state);
    assertEquals(0L, publicJob.errors);
  }

  @Test
  void applyLeaseOutcomeAllowsTerminalCompletionWithinLeaseGraceWindow()
      throws InterruptedException {
    System.setProperty("floecat.reconciler.job-store.lease-ms", "1000");
    System.setProperty("floecat.reconciler.job-store.lease-renew-grace-ms", "5000");
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());

    ReconcileJobStore.LeasedJob lease = store.leaseNext().orElseThrow();
    store.markRunning(jobId, lease.leaseEpoch, 100L, "executor-1");

    forceLeaseExpired(jobId);

    assertTrue(
        store.applyLeaseOutcome(
            jobId,
            lease.leaseEpoch,
            ReconcileJobStore.CompletionKind.SUCCEEDED,
            200L,
            "done",
            1L,
            1L,
            0L,
            0L,
            0L,
            2L,
            3L));

    ReconcileJob leaseView = store.getLeaseView(jobId).orElseThrow();
    assertEquals("JS_SUCCEEDED", leaseView.state);
    assertEquals("Succeeded", leaseView.message);
  }

  @Test
  void maintenanceDoesNotReclaimLeaseBeforeExpiryGraceElapses() throws Exception {
    System.setProperty("floecat.reconciler.job-store.lease-ms", "1000");
    System.setProperty("floecat.reconciler.job-store.lease-renew-grace-ms", "5000");
    System.setProperty("floecat.reconciler.job-store.reclaim-interval-ms", "1");
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);

    ReconcileJobStore.LeasedJob lease = store.leaseNext().orElseThrow();
    store.markRunning(jobId, lease.leaseEpoch, 100L, "executor-1");

    forceLeaseExpired(jobId);

    store.runMaintenanceOnce(1_000L);

    StoredReconcileJob stillRunning = readStoredRecord(canonicalPointerKey);
    assertEquals("JS_RUNNING", stillRunning.state);
    assertTrue(store.getLeaseView(jobId).isPresent());
    assertEquals("JS_RUNNING", store.getLeaseView(jobId).orElseThrow().state);
    assertTrue(store.leaseNext().isEmpty());
  }

  @Test
  void readyPointerKeyForDueBuildsExpectedGlobalReadyKey() throws Exception {
    store.init();
    long dueAt = 123456789L;
    assertEquals(
        Keys.reconcileReadyPointerByDue(dueAt, ACCOUNT_ID, "lane", "job-1"),
        readyQueue().readyPointerKeyForDue(ACCOUNT_ID, "lane", "job-1", dueAt));
  }

  @Test
  void leaseNextStopsReadyScanAtFirstFuturePointer() {
    assumeMemoryOnly("pointer ready ordering assertions are only meaningful in memory mode");
    assumeMemoryOnly("pointer ready ordering assertions are only meaningful in memory mode");
    TrackingPointerStore pointerStore = new TrackingPointerStore();
    store.pointerStore = pointerStore;
    System.setProperty("floecat.reconciler.job-store.ready-scan-limit", "1");
    store.init();

    String firstJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    String secondJobId =
        store.enqueue(
            ACCOUNT_ID,
            "conn-2",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "other"));

    long futureDueAt = System.currentTimeMillis() + 60_000L;
    for (String jobId : List.of(firstJobId, secondJobId)) {
      String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
      StoredReconcileJob record = readStoredRecord(canonicalPointerKey);
      String oldReadyKey = record.readyPointerKey;
      Pointer oldReadyPointer = store.pointerStore.get(oldReadyKey).orElseThrow();
      assertTrue(store.pointerStore.compareAndDelete(oldReadyKey, oldReadyPointer.getVersion()));
      record.nextAttemptAtMs = futureDueAt;
      record.readyPointerKey =
          Keys.reconcileReadyPointerByDue(
              futureDueAt, record.accountId, record.laneKey, record.jobId);
      overwriteCanonicalRecordWithoutSync(canonicalPointerKey, record);
      assertTrue(
          store.pointerStore.compareAndSet(
              record.readyPointerKey,
              0L,
              Pointer.newBuilder()
                  .setKey(record.readyPointerKey)
                  .setBlobUri(canonicalPointerKey)
                  .setVersion(1L)
                  .build()));
      futureDueAt += 1L;
    }

    assertTrue(store.leaseNext().isEmpty());
    assertEquals(2, globalReadyEntryCount());
  }

  @Test
  void filteredLeaseByPinnedExecutorUsesPinnedReadyIndex() {
    assumeMemoryOnly(
        "pointer-specific ready slice ordering assertions are only meaningful in memory mode");
    assumeMemoryOnly(
        "pointer-specific ready slice ordering assertions are only meaningful in memory mode");
    TrackingPointerStore pointerStore = new TrackingPointerStore();
    store.pointerStore = pointerStore;
    store.init();

    for (int i = 0; i < 25; i++) {
      store.enqueue(
          ACCOUNT_ID,
          "conn-unpinned-" + i,
          false,
          CaptureMode.METADATA_AND_CAPTURE,
          ReconcileScope.of(List.of(), "tbl-" + i));
    }

    String pinnedJobId =
        store.enqueue(
            ACCOUNT_ID,
            "conn-pinned",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-pinned"),
            ReconcileExecutionPolicy.defaults(),
            "executor-a");
    var lease =
        store
            .leaseNext(
                ReconcileJobStore.LeaseRequest.of(
                    Set.of(), Set.of(), Set.of("executor-a"), Set.of()))
            .orElseThrow();

    assertEquals(pinnedJobId, lease.jobId);
  }

  @Test
  void readyPointerKeysIncludeExpectedOrderedSlices() throws Exception {
    store.init();
    StoredReconcileJob record = new StoredReconcileJob();
    record.accountId = ACCOUNT_ID;
    record.jobId = "job-5";
    record.state = "JS_QUEUED";
    record.jobKind = ReconcileJobKind.PLAN_VIEW.name();
    record.executionClass = ReconcileExecutionClass.BATCH.name();
    record.executionLane = "lane-1";
    record.laneKey = "lane-1";
    record.pinnedExecutorId = "executor-1";
    record.nextAttemptAtMs = 123L;

    List<String> readyKeys = readyPointerKeysFor(record);

    assertTrue(
        readyKeys.contains(Keys.reconcileReadyPointerByDue(123L, ACCOUNT_ID, "lane-1", "job-5")));
    assertTrue(
        readyKeys.contains(
            Keys.reconcileReadyByExecutionClassPointerByDue(
                123L, ReconcileExecutionClass.BATCH.name(), ACCOUNT_ID, "job-5")));
    assertTrue(
        readyKeys.contains(
            Keys.reconcileReadyByExecutionLanePointerByDue(123L, "lane-1", ACCOUNT_ID, "job-5")));
    assertTrue(
        readyKeys.contains(
            Keys.reconcileReadyByPinnedExecutorPointerByDue(
                123L, "executor-1", ACCOUNT_ID, "job-5")));
    assertTrue(
        readyKeys.contains(
            Keys.reconcileReadyByJobKindPointerByDue(
                123L, ReconcileJobKind.PLAN_VIEW.name(), ACCOUNT_ID, "job-5")));
  }

  @Test
  void filteredLeaseByExecutionClassFindsMatchingJobWithoutGlobalScan() {
    assumeMemoryOnly("pointer scan instrumentation is only meaningful in memory mode");
    TrackingPointerStore pointerStore = new TrackingPointerStore();
    store.pointerStore = pointerStore;
    store.init();

    for (int i = 0; i < 25; i++) {
      store.enqueue(
          ACCOUNT_ID,
          "conn-default-" + i,
          false,
          CaptureMode.METADATA_AND_CAPTURE,
          ReconcileScope.of(List.of(), "tbl-default-" + i));
    }

    String batchJobId =
        store.enqueue(
            ACCOUNT_ID,
            "conn-batch",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-batch"),
            ReconcileExecutionPolicy.of(ReconcileExecutionClass.BATCH, "", Map.of()),
            "");
    var lease =
        store
            .leaseNext(
                ReconcileJobStore.LeaseRequest.of(Set.of(ReconcileExecutionClass.BATCH), Set.of()))
            .orElseThrow();

    assertEquals(batchJobId, lease.jobId);
  }

  @Test
  void filteredLeaseByLaneUsesLaneReadyIndex() {
    assumeMemoryOnly("pointer scan instrumentation is only meaningful in memory mode");
    TrackingPointerStore pointerStore = new TrackingPointerStore();
    store.pointerStore = pointerStore;
    store.init();

    for (int i = 0; i < 25; i++) {
      store.enqueue(
          ACCOUNT_ID,
          "conn-lane-other-" + i,
          false,
          CaptureMode.METADATA_AND_CAPTURE,
          ReconcileScope.of(List.of(), "tbl-lane-other-" + i),
          ReconcileExecutionPolicy.of(ReconcileExecutionClass.DEFAULT, "lane-other", Map.of()),
          "");
    }

    String laneJobId =
        store.enqueue(
            ACCOUNT_ID,
            "conn-lane-a",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-lane-a"),
            ReconcileExecutionPolicy.of(ReconcileExecutionClass.DEFAULT, "lane-a", Map.of()),
            "");
    var lease =
        store
            .leaseNext(ReconcileJobStore.LeaseRequest.of(Set.of(), Set.of("lane-a")))
            .orElseThrow();

    assertEquals(laneJobId, lease.jobId);
  }

  @Test
  void filteredLeaseByJobKindUsesJobKindReadyIndex() {
    assumeMemoryOnly("pointer scan instrumentation is only meaningful in memory mode");
    TrackingPointerStore pointerStore = new TrackingPointerStore();
    store.pointerStore = pointerStore;
    store.init();

    for (int i = 0; i < 25; i++) {
      store.enqueue(
          ACCOUNT_ID,
          "conn-plan-" + i,
          false,
          CaptureMode.METADATA_AND_CAPTURE,
          ReconcileScope.of(List.of(), "tbl-plan-" + i));
    }

    String viewJobId =
        store.enqueueViewPlan(
            ACCOUNT_ID,
            "conn-view",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.ofView(List.of(), "view-1"),
            ReconcileViewTask.of("src-ns", "src-view", "dest-ns", "view-1", "View One"),
            ReconcileExecutionPolicy.defaults(),
            "",
            "");
    var lease =
        store
            .leaseNext(
                ReconcileJobStore.LeaseRequest.of(
                    Set.of(), Set.of(), Set.of(), Set.of(ReconcileJobKind.PLAN_VIEW)))
            .orElseThrow();

    assertEquals(viewJobId, lease.jobId);
  }

  @Test
  void filteredLeaseByPinnedExecutorFallsBackToGlobalReadyIndex() {
    assumeMemoryOnly("pointer scan instrumentation is only meaningful in memory mode");
    TrackingPointerStore pointerStore = new TrackingPointerStore();
    store.pointerStore = pointerStore;
    store.init();

    String globalJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-global"));

    var lease =
        store
            .leaseNext(
                ReconcileJobStore.LeaseRequest.of(
                    Set.of(), Set.of(), Set.of("executor-a"), Set.of()))
            .orElseThrow();

    assertEquals(globalJobId, lease.jobId);
    assertEquals(
        0,
        readyEntryKeysForSlice(
                ReconcileReadyQueueStore.ReadyIndexType.PINNED_EXECUTOR, "executor-a")
            .size());
  }

  @Test
  void dueTimeChangeRewritesSecondaryReadyIndexes() {
    assumeMemoryOnly("pointer ready index rewrite assertions are only meaningful in memory mode");
    assumeMemoryOnly("pointer ready index rewrite assertions are only meaningful in memory mode");
    store.init();

    String jobId =
        store.enqueueViewPlan(
            ACCOUNT_ID,
            "conn-retry",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.ofView(List.of(), "view-retry"),
            ReconcileViewTask.of("src-ns", "src-view", "dest-ns", "view-retry", "View Retry"),
            ReconcileExecutionPolicy.of(ReconcileExecutionClass.BATCH, "lane-retry", Map.of()),
            "",
            "executor-retry");
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    StoredReconcileJob queued = readStoredRecord(canonicalPointerKey);
    long oldDueAt = queued.nextAttemptAtMs;
    String oldGlobalReadyKey = queued.readyPointerKey;
    String oldClassReadyKey =
        Keys.reconcileReadyByExecutionClassPointerByDue(
            oldDueAt, ReconcileExecutionClass.BATCH.name(), ACCOUNT_ID, jobId);
    String oldLaneReadyKey =
        Keys.reconcileReadyByExecutionLanePointerByDue(oldDueAt, "lane-retry", ACCOUNT_ID, jobId);
    String oldPinnedReadyKey =
        Keys.reconcileReadyByPinnedExecutorPointerByDue(
            oldDueAt, "executor-retry", ACCOUNT_ID, jobId);
    String oldKindReadyKey =
        Keys.reconcileReadyByJobKindPointerByDue(
            oldDueAt, ReconcileJobKind.PLAN_VIEW.name(), ACCOUNT_ID, jobId);

    var lease =
        store
            .leaseNext(
                ReconcileJobStore.LeaseRequest.of(
                    Set.of(), Set.of(), Set.of("executor-retry"), Set.of()))
            .orElseThrow();
    store.markRunning(lease.jobId, lease.leaseEpoch, System.currentTimeMillis(), "executor-retry");
    store.markFailed(
        lease.jobId, lease.leaseEpoch, System.currentTimeMillis(), "retry", 0, 0, 0, 0, 1, 0, 0);

    StoredReconcileJob requeued = readStoredRecord(canonicalPointerKey);
    long newDueAt = requeued.nextAttemptAtMs;
    assertNotEquals(oldDueAt, newDueAt);

    assertFalse(readyEntryExists(oldGlobalReadyKey));
    assertFalse(readyEntryExists(oldClassReadyKey));
    assertFalse(readyEntryExists(oldLaneReadyKey));
    assertFalse(readyEntryExists(oldPinnedReadyKey));
    assertFalse(readyEntryExists(oldKindReadyKey));

    assertTrue(readyEntryExists(requeued.readyPointerKey));
  }

  @Test
  void bulkEnqueueRollbackClearsPartiallyWrittenReadyIndexes() {
    assumeMemoryOnly("pointer-store batch fault injection is only meaningful in memory mode");
    store = new DurableReconcileJobStore();
    store.pointerStore =
        new BatchFailingPointerStore(
            Keys.reconcileReadyByExecutionLanePointerPrefix("lane-partial"), Integer.MAX_VALUE);
    store.blobStore = new InMemoryBlobStore();
    store.mapper = new ObjectMapper();
    store.config = ConfigProvider.getConfig();
    store.init();

    assertThrows(
        IllegalStateException.class,
        () ->
            store.enqueue(
                ACCOUNT_ID,
                "conn-partial",
                false,
                CaptureMode.METADATA_AND_CAPTURE,
                ReconcileScope.of(List.of(), "tbl-partial"),
                ReconcileExecutionPolicy.of(
                    ReconcileExecutionClass.BATCH, "lane-partial", Map.of()),
                "executor-partial"));

    assertEquals(0, globalReadyEntryCount());
    assertEquals(
        0,
        readyEntryKeysForSlice(
                ReconcileReadyQueueStore.ReadyIndexType.EXECUTION_CLASS,
                ReconcileExecutionClass.BATCH.name())
            .size());
    assertEquals(
        0,
        readyEntryKeysForSlice(
                ReconcileReadyQueueStore.ReadyIndexType.EXECUTION_LANE, "lane-partial")
            .size());
    assertEquals(
        0,
        readyEntryKeysForSlice(
                ReconcileReadyQueueStore.ReadyIndexType.PINNED_EXECUTOR, "executor-partial")
            .size());
    assertEquals(
        0,
        readyEntryKeysForSlice(
                ReconcileReadyQueueStore.ReadyIndexType.JOB_KIND,
                ReconcileJobKind.PLAN_CONNECTOR.name())
            .size());
  }

  @Test
  void terminalStateClearsAllReadyIndexes() {
    assumeMemoryOnly("pointer ready index cleanup assertions are only meaningful in memory mode");
    assumeMemoryOnly("pointer ready index cleanup assertions are only meaningful in memory mode");
    store.init();

    String jobId =
        store.enqueueViewPlan(
            ACCOUNT_ID,
            "conn-cancel",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.ofView(List.of(), "view-cancel"),
            ReconcileViewTask.of("src-ns", "src-view", "dest-ns", "view-cancel", "View Cancel"),
            ReconcileExecutionPolicy.of(ReconcileExecutionClass.BATCH, "lane-cancel", Map.of()),
            "",
            "executor-cancel");
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    StoredReconcileJob queued = readStoredRecord(canonicalPointerKey);
    long dueAt = queued.nextAttemptAtMs;

    store.cancel(ACCOUNT_ID, jobId, "cancel");

    assertFalse(readyEntryExists(queued.readyPointerKey));
    assertFalse(
        readyEntryExists(
            Keys.reconcileReadyByExecutionClassPointerByDue(
                dueAt, ReconcileExecutionClass.BATCH.name(), ACCOUNT_ID, jobId)));
    assertFalse(
        readyEntryExists(
            Keys.reconcileReadyByExecutionLanePointerByDue(
                dueAt, "lane-cancel", ACCOUNT_ID, jobId)));
    assertFalse(
        readyEntryExists(
            Keys.reconcileReadyByPinnedExecutorPointerByDue(
                dueAt, "executor-cancel", ACCOUNT_ID, jobId)));
    assertFalse(
        readyEntryExists(
            Keys.reconcileReadyByJobKindPointerByDue(
                dueAt, ReconcileJobKind.PLAN_VIEW.name(), ACCOUNT_ID, jobId)));
  }

  @Test
  void cancelRunningDoesNotReadyIndexCancellingJob() {
    assumeMemoryOnly("pointer ready index assertions are only meaningful in memory mode");
    assumeMemoryOnly("pointer ready index assertions are only meaningful in memory mode");
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            "conn-cancelling-due",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-cancelling-due"));
    var lease = store.leaseNext().orElseThrow();
    store.markRunning(lease.jobId, lease.leaseEpoch, System.currentTimeMillis(), "executor-1");

    store.cancel(ACCOUNT_ID, jobId, "cancel");

    StoredReconcileJob record = readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, jobId));
    assertEquals("JS_CANCELLING", record.state);
    assertTrue(record.nextAttemptAtMs > 0L);
    assertTrue(record.readyPointerKey == null || record.readyPointerKey.isBlank());
    assertEquals(0, globalReadyEntryCount());
  }

  @Test
  void unfilteredLeaseStillUsesGlobalReadyIndex() {
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-global"));
    StoredReconcileJob queued = readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, jobId));
    assertTrue(readyEntryExists(queued.readyPointerKey));
    assertEquals(1, globalReadyEntryCount());
    assertTrue(store.leaseNext().isPresent());
  }

  @Test
  void leaseNextRollsBackWhenLeaseExpiryIndexWriteFails() {
    assumeMemoryOnly("pointer fault-injection assertions are only meaningful in memory mode");
    assumeMemoryOnly("pointer fault-injection assertions are only meaningful in memory mode");
    store.pointerStore =
        new BatchFailingPointerStore(LEASE_EXPIRY_POINTER_PREFIX, Integer.MAX_VALUE);
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    StoredReconcileJob queued = readStoredRecord(canonicalPointerKey);

    assertTrue(store.leaseNext().isEmpty());

    StoredReconcileJob afterAttempt = readStoredRecord(canonicalPointerKey);
    assertEquals("JS_QUEUED", afterAttempt.state);
    assertEquals(queued.readyPointerKey, afterAttempt.readyPointerKey);
    assertTrue(readyEntryExists(queued.readyPointerKey));
    assertTrue(
        store.pointerStore.get(Keys.reconcileJobLeasePointerById(ACCOUNT_ID, jobId)).isEmpty());
  }

  @Test
  void loadByAnyAccountReturnsEmptyForBlankJobId() throws Exception {
    store.init();

    Object loaded = invokePrivateMethod("loadByAnyAccount", new Class<?>[] {String.class}, " ");

    assertEquals(Optional.empty(), loaded);
  }

  @Test
  void pinnedExecutorJobsAreNotLeasedByUnpinnedOrWrongExecutorRequests() {
    assumeMemoryOnly("pointer scan instrumentation is only meaningful in memory mode");
    TrackingPointerStore pointerStore = new TrackingPointerStore();
    store.pointerStore = pointerStore;
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            "conn-pinned-only",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-pinned-only"),
            ReconcileExecutionPolicy.defaults(),
            "executor-a");

    assertTrue(store.leaseNext().isEmpty());
    assertEquals(1, globalReadyEntryCount());

    assertTrue(
        store
            .leaseNext(
                ReconcileJobStore.LeaseRequest.of(
                    Set.of(), Set.of(), Set.of("executor-b"), Set.of()))
            .isEmpty());
    assertEquals(
        0,
        readyEntryKeysForSlice(
                ReconcileReadyQueueStore.ReadyIndexType.PINNED_EXECUTOR, "executor-b")
            .size());

    var lease =
        store
            .leaseNext(
                ReconcileJobStore.LeaseRequest.of(
                    Set.of(), Set.of(), Set.of("executor-a"), Set.of()))
            .orElseThrow();
    assertEquals(jobId, lease.jobId);
  }

  @Test
  void leaseBatchFailureDoesNotConsumeReadyPointer() {
    assumeMemoryOnly("pointer fault-injection assertions are only meaningful in memory mode");
    assumeMemoryOnly("pointer fault-injection assertions are only meaningful in memory mode");
    store.pointerStore =
        new PrefixFailingPointerStore(
            "/accounts/" + ACCOUNT_ID + "/reconcile/job-leases/by-id/", 100);
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    String readyKey =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, jobId)).readyPointerKey;

    assertTrue(store.leaseNext().isEmpty());
    assertTrue(readyEntryExists(readyKey));
    assertTrue(
        store.pointerStore.get(Keys.reconcileJobLeasePointerById(ACCOUNT_ID, jobId)).isEmpty());
  }

  @Test
  void markProgressDoesNotRewriteStableLookupOrConnectorPointers() {
    assumeMemoryOnly("pointer-store fault injection is only meaningful in memory mode");
    store.pointerStore =
        new NthPrefixFailingPointerStore(Keys.reconcileJobLookupPointerByIdPrefix(), 2);
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    var lease = store.leaseNext().orElseThrow();

    store.markProgress(jobId, lease.leaseEpoch, 4, 2, 1, 0, 0, 3, 5, "working");

    var job = store.get(ACCOUNT_ID, jobId).orElseThrow();
    assertEquals(4L, job.tablesScanned);
    assertEquals(2L, job.tablesChanged);
    assertEquals("working", job.message);
  }

  @Test
  void markSucceededClearsLeaseRowAndExpiryIndexAtomically() {
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());

    var lease = store.leaseNext().orElseThrow();
    StoredJobLease storedLease = readStoredLease(ACCOUNT_ID, jobId);
    String expiryKey = leaseExpiryPointerKey(storedLease.expiresAtMs, ACCOUNT_ID, jobId);

    store.markSucceeded(jobId, lease.leaseEpoch, System.currentTimeMillis(), 1, 1, 1, 1);

    assertFalse(leaseEntryExists(ACCOUNT_ID, jobId));
    assertFalse(leaseExpiryEntryExists(expiryKey));
  }

  @Test
  void queueStatsReflectQueuedWaitingRunningAndCancellingJobs() {
    store.init();
    ReconcileScope queuedScope = ReconcileScope.of(List.of(), "tbl-q");
    ReconcileScope waitingScope = ReconcileScope.of(List.of(), "tbl-w");
    ReconcileScope runningScope = ReconcileScope.of(List.of(), "tbl-r");
    ReconcileScope cancellingScope = ReconcileScope.of(List.of(), "tbl-c");

    store.enqueue(ACCOUNT_ID, "conn-q", false, CaptureMode.METADATA_AND_CAPTURE, queuedScope);
    store.enqueue(ACCOUNT_ID, "conn-w", false, CaptureMode.METADATA_AND_CAPTURE, waitingScope);
    store.enqueue(ACCOUNT_ID, "conn-r", false, CaptureMode.METADATA_AND_CAPTURE, runningScope);
    store.enqueue(ACCOUNT_ID, "conn-c", false, CaptureMode.METADATA_AND_CAPTURE, cancellingScope);

    var firstLease = store.leaseNext().orElseThrow();
    store.markRunning(
        firstLease.jobId, firstLease.leaseEpoch, System.currentTimeMillis(), "default_reconciler");

    ReconcileJobStore.LeasedJob waitingLease = store.leaseNext().orElseThrow();
    store.markRunning(
        waitingLease.jobId,
        waitingLease.leaseEpoch,
        System.currentTimeMillis(),
        "default_reconciler");
    store.markWaiting(
        waitingLease.jobId,
        waitingLease.leaseEpoch,
        System.currentTimeMillis(),
        "Waiting on dependency",
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L);

    var cancellingLease = store.leaseNext().orElseThrow();
    store.markRunning(
        cancellingLease.jobId,
        cancellingLease.leaseEpoch,
        System.currentTimeMillis(),
        "default_reconciler");
    store.cancel(ACCOUNT_ID, cancellingLease.jobId, "stop");

    var stats =
        waitForValue(
            () -> store.queueStats(),
            current -> current.queued == 2L && current.running == 1L && current.cancelling == 1L,
            "queue stats to reflect queued/waiting/running/cancelling jobs");

    assertEquals(2L, stats.queued);
    assertEquals(1L, stats.running);
    assertEquals(1L, stats.cancelling);
    assertTrue(stats.oldestQueuedCreatedAtMs > 0L);
  }

  /**
   * Regression test for: reclaim and ready leasing must not depend on BlobStore for canonical job
   * or lease state.
   *
   * <p>BlobStore failures are still tolerated for optional payload lookups, but hot-path reclaim
   * and canonical state reads must remain KV-inline only.
   */
  @Test
  void leaseNextDoesNotThrowWhenReclaimedBlobIsMissing() throws Exception {
    System.setProperty("floecat.reconciler.job-store.lease-ms", "1");
    System.setProperty("floecat.reconciler.job-store.reclaim-interval-ms", "1");
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    // Take the first lease so the job is removed from the ready queue.
    // leaseNext() short-circuits via leaseReadyDue() here — reclaim is NOT triggered yet.
    store.leaseNext().orElseThrow();

    // Replace blobStore with one that throws StorageNotFoundException on every get().
    // This simulates the S3 "not found" case reported in the bug.
    store.blobStore = new ThrowingOnGetBlobStore();

    forceLeaseExpired(jobId);

    // leaseNext() must not reclaim in the hot path any more.
    assertTrue(store.leaseNext().isEmpty());

    // Explicit maintenance may reclaim without touching BlobStore-backed payloads.
    assertDoesNotThrow(() -> store.runMaintenanceOnce(1_000L));
  }

  @Test
  void listSummariesDoNotRequireDefinitionSnapshotOrFileGroupBlobs() {
    store.init();

    String connectorJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    String snapshotJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileSnapshotTask.of(
                "table-1",
                55L,
                "db",
                "orders",
                List.of(
                    ReconcileFileGroupTask.of(
                        "plan-1",
                        "group-1",
                        "table-1",
                        55L,
                        List.of("s3://bucket/data/file-1.parquet"))),
                true),
            ReconcileExecutionPolicy.defaults(),
            connectorJobId,
            "");
    String execJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileJobKind.EXEC_FILE_GROUP,
            ReconcileTableTask.empty(),
            ReconcileViewTask.empty(),
            ReconcileSnapshotTask.empty(),
            ReconcileFileGroupTask.of(
                "plan-1", "group-1", "table-1", 55L, List.of("s3://bucket/data/file-1.parquet")),
            ReconcileExecutionPolicy.defaults(),
            snapshotJobId,
            "");

    ReconcileJobStore.LeasedJob execLease = leaseJob(execJobId);
    store.markRunning(execJobId, execLease.leaseEpoch, 100L, "executor-exec");
    store.persistFileGroupResult(
        execJobId,
        ReconcileFileGroupTask.of(
            "plan-1",
            "group-1",
            "table-1",
            55L,
            List.of("s3://bucket/data/file-1.parquet"),
            List.of(ReconcileFileResult.succeeded("s3://bucket/data/file-1.parquet", 1L))));

    store.blobStore = new ThrowingOnGetBlobStore();

    ReconcileJobStore.ReconcileJobPage page =
        assertDoesNotThrow(() -> store.list(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()));
    assertEquals(3, page.jobs.size());
    assertTrue(page.jobs.stream().anyMatch(job -> connectorJobId.equals(job.jobId)));
    assertTrue(page.jobs.stream().anyMatch(job -> snapshotJobId.equals(job.jobId)));
    assertTrue(page.jobs.stream().anyMatch(job -> execJobId.equals(job.jobId)));
  }

  @Test
  void readPathsDoNotRebuildMissingContributionProjectionPointers() throws Exception {
    store.init();

    String parentJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    String childJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("db", "orders", "table-1", "orders"),
            ReconcileExecutionPolicy.defaults(),
            parentJobId,
            "");

    assertDoesNotThrow(() -> store.list(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()));

    assertDoesNotThrow(() -> store.get(ACCOUNT_ID, parentJobId));
  }

  @Test
  void leaseNextAvoidsRedundantReadyCandidateBlobReloadBeforeLeaseMutation() {
    CountingBlobStore blobStore = new CountingBlobStore();
    store.blobStore = blobStore;
    store.init();

    store.enqueue(
        ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, ReconcileScope.empty());
    blobStore.resetGetCount();

    var lease = store.leaseNext().orElseThrow();

    assertEquals(CONNECTOR_ID, lease.connectorId);
    assertEquals(
        0,
        blobStore.getCount(),
        "inline job definitions should avoid blob reads on the uncontended happy path");
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

  private static final class CountingBlobStore extends InMemoryBlobStore {
    private final AtomicInteger getCount = new AtomicInteger();

    @Override
    public byte[] get(String uri) {
      getCount.incrementAndGet();
      return super.get(uri);
    }

    int getCount() {
      return getCount.get();
    }

    void resetGetCount() {
      getCount.set(0);
    }
  }

  private static final class TrackingPointerStore extends InMemoryPointerStore {
    private final Map<String, Integer> listCounts = new HashMap<>();

    @Override
    public List<Pointer> listPointersByPrefix(
        String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
      listCounts.merge(prefix, 1, Integer::sum);
      return super.listPointersByPrefix(prefix, limit, pageToken, nextTokenOut);
    }

    int listCount(String prefix) {
      return listCounts.getOrDefault(prefix, 0);
    }

    void resetListCounts() {
      listCounts.clear();
    }
  }

  private static final class SinglePointerPageStore extends InMemoryPointerStore {
    @Override
    public List<Pointer> listPointersByPrefix(
        String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
      return super.listPointersByPrefix(prefix, 1, pageToken, nextTokenOut);
    }
  }

  private static final class TestLogHandler extends Handler {
    private final List<String> messages = new ArrayList<>();

    @Override
    public void publish(LogRecord record) {
      if (record != null && record.getLevel().intValue() >= Level.WARNING.intValue()) {
        messages.add(record.getMessage());
      }
    }

    @Override
    public void flush() {}

    @Override
    public void close() {}

    List<String> messages() {
      return List.copyOf(messages);
    }
  }

  private static ReconcileScope.ScopedCaptureRequest scopedCaptureRequest(
      String tableId, long snapshotId, String targetSpec, List<String> columnSelectors) {
    return new ReconcileScope.ScopedCaptureRequest(
        tableId, snapshotId, targetSpec, columnSelectors);
  }

  @Test
  void connectorParentStoresCanonicalChildProgressForGetAndList() {
    store.init();

    String connectorJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    String tableJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("db", "orders", "table-1", "orders"),
            ReconcileExecutionPolicy.defaults(),
            connectorJobId,
            "");

    ReconcileJobStore.LeasedJob tableLease = leaseJob(tableJobId, ReconcileJobKind.PLAN_TABLE);
    store.markRunning(tableJobId, tableLease.leaseEpoch, 100L, "executor-a");
    store.markProgress(
        tableJobId, tableLease.leaseEpoch, 1L, 1L, 2L, 1L, 0L, 3L, 4L, "Table running");

    ReconcileJob projected =
        waitForValue(
            () -> store.get(ACCOUNT_ID, connectorJobId).orElseThrow(),
            current ->
                current.tablesScanned == 1L
                    && current.tablesChanged == 1L
                    && current.viewsScanned == 2L
                    && current.viewsChanged == 1L
                    && current.snapshotsProcessed == 3L
                    && current.statsProcessed == 4L,
            "connector parent projected child progress");
    assertEquals(1L, projected.tablesScanned);
    assertEquals(1L, projected.tablesChanged);
    assertEquals(2L, projected.viewsScanned);
    assertEquals(1L, projected.viewsChanged);
    assertEquals(3L, projected.snapshotsProcessed);
    assertEquals(4L, projected.statsProcessed);

    ReconcileJob listed =
        store.list(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
            .filter(job -> connectorJobId.equals(job.jobId))
            .findFirst()
            .orElseThrow();
    assertTrue(listed.aggregateSummaryPresent);
    assertEquals(1L, listed.tablesScanned);
    assertEquals(1L, listed.tablesChanged);
    assertEquals(2L, listed.viewsScanned);
    assertEquals(1L, listed.viewsChanged);
    assertEquals(3L, listed.snapshotsProcessed);
    assertEquals(4L, listed.statsProcessed);
  }

  @Test
  void connectorParentIgnoresCanonicalSelfCountersWhenProjectingChildren() {
    store.init();

    String connectorJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    String tableJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("db", "orders", "table-1", "orders"),
            ReconcileExecutionPolicy.defaults(),
            connectorJobId,
            "");

    ReconcileJobStore.LeasedJob connectorLease = leaseJob(connectorJobId);
    store.markRunning(connectorJobId, connectorLease.leaseEpoch, 50L, "connector-executor");
    store.markProgress(
        connectorJobId, connectorLease.leaseEpoch, 1L, 1L, 7L, 3L, 0L, 5L, 11L, "connector self");

    ReconcileJobStore.LeasedJob tableLease = leaseJob(tableJobId);
    store.markRunning(tableJobId, tableLease.leaseEpoch, 100L, "table-executor");
    store.markProgress(
        tableJobId, tableLease.leaseEpoch, 1L, 1L, 2L, 1L, 0L, 3L, 4L, "table child");

    ReconcileJob projected =
        waitForValue(
            () -> store.get(ACCOUNT_ID, connectorJobId).orElseThrow(),
            job ->
                job.tablesScanned == 1L
                    && job.tablesChanged == 1L
                    && job.viewsScanned == 2L
                    && job.viewsChanged == 1L
                    && job.snapshotsProcessed == 3L
                    && job.statsProcessed == 4L,
            "connector child-only projection");
    assertEquals(1L, projected.tablesScanned);
    assertEquals(1L, projected.tablesChanged);
    assertEquals(2L, projected.viewsScanned);
    assertEquals(1L, projected.viewsChanged);
    assertEquals(3L, projected.snapshotsProcessed);
    assertEquals(4L, projected.statsProcessed);
  }

  @Test
  void parentStateProjectsFailedFromChildContribution() {
    store.init();

    String parentJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    String childJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("db", "orders", "table-1", "orders"),
            ReconcileExecutionPolicy.defaults(),
            parentJobId,
            "");

    ReconcileJobStore.LeasedJob childLease = leaseJob(childJobId, ReconcileJobKind.PLAN_TABLE);
    store.markRunning(childJobId, childLease.leaseEpoch, 100L, "executor-fail");
    store.markFailedTerminal(
        childJobId, childLease.leaseEpoch, 200L, "child failed", 0L, 0L, 0L, 0L, 1L, 0L, 0L);

    ReconcileJob parent =
        waitForValue(
            () -> store.get(ACCOUNT_ID, parentJobId).orElseThrow(),
            current -> "JS_FAILED".equals(current.state),
            "parent failed state projected from child contribution");
    assertEquals("JS_FAILED", parent.state);
    assertEquals("child failed", parent.message);
  }

  @Test
  void parentStateProjectsCancelledFromChildContribution() {
    store.init();

    String parentJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    String childJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("db", "orders", "table-1", "orders"),
            ReconcileExecutionPolicy.defaults(),
            parentJobId,
            "");

    ReconcileJobStore.LeasedJob childLease = leaseJob(childJobId, ReconcileJobKind.PLAN_TABLE);
    store.markRunning(childJobId, childLease.leaseEpoch, 100L, "executor-cancel");
    store.markCancelled(
        childJobId, childLease.leaseEpoch, 200L, "child cancelled", 0L, 0L, 0L, 0L, 0L, 0L, 0L);

    ReconcileJob cancelledChild = store.get(ACCOUNT_ID, childJobId).orElseThrow();
    assertEquals("JS_CANCELLED", cancelledChild.state);
    assertEquals("child cancelled", cancelledChild.message);

    ReconcileJob parent =
        waitForValue(
            () -> store.get(ACCOUNT_ID, parentJobId).orElseThrow(),
            current -> "JS_CANCELLED".equals(current.state),
            "parent cancelled state projected from child contribution");
    assertEquals("JS_CANCELLED", parent.state);
    assertEquals("child cancelled", parent.message);
  }

  @Test
  void parentStateProjectsSucceededWithSucceededMessageAfterChildCompletes() {
    store.init();

    String parentJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    String childJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("db", "orders", "table-1", "orders"),
            ReconcileExecutionPolicy.defaults(),
            parentJobId,
            "");

    ReconcileJobStore.LeasedJob childLease = leaseJob(childJobId, ReconcileJobKind.PLAN_TABLE);
    store.markRunning(childJobId, childLease.leaseEpoch, 100L, "executor-success");

    ReconcileJob runningParent =
        waitForValue(
            () -> store.get(ACCOUNT_ID, parentJobId).orElseThrow(),
            current ->
                "JS_RUNNING".equals(current.state)
                    && "Running".equals(current.message)
                    && "executor-success".equals(current.executorId),
            "parent running state projected from child contribution");
    assertEquals("JS_RUNNING", runningParent.state);
    assertEquals("Running", runningParent.message);
    assertEquals("executor-success", runningParent.executorId);

    store.markSucceeded(childJobId, childLease.leaseEpoch, 200L, 1L, 1L, 0L, 0L);

    ReconcileJob parent =
        waitForValue(
            () -> store.get(ACCOUNT_ID, parentJobId).orElseThrow(),
            current -> "JS_SUCCEEDED".equals(current.state) && current.finishedAtMs == 200L,
            "parent succeeded state projected from child contribution");
    assertEquals("JS_SUCCEEDED", parent.state);
    assertEquals(200L, parent.finishedAtMs);
  }

  @Test
  void waitingChildProjectsParentAsQueuedWithWaitingMessage() {
    store.init();

    String parentJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    String childJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-waiting"),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("db", "waiting_table", "table-waiting", "waiting_table"),
            ReconcileExecutionPolicy.defaults(),
            parentJobId,
            "");

    ReconcileJobStore.LeasedJob childLease = leaseJob(childJobId, ReconcileJobKind.PLAN_TABLE);
    store.markRunning(childJobId, childLease.leaseEpoch, 100L, "executor-waiting");
    store.markWaiting(
        childJobId,
        childLease.leaseEpoch,
        200L,
        "Waiting on dependency",
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L);

    ReconcileJob parent =
        waitForValue(
            () -> store.get(ACCOUNT_ID, parentJobId).orElseThrow(),
            current -> "JS_WAITING".equals(current.state),
            "parent waiting state projected from child contribution");
    assertEquals("JS_WAITING", parent.state);
    assertEquals("Waiting on dependency", parent.message);
  }

  @Test
  void completedPlanConnectorRevertsToWaitingWhenLateChildIsEnqueued() throws Exception {
    store.init();

    String parentJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());

    ReconcileJobStore.LeasedJob parentLease =
        leaseJob(parentJobId, ReconcileJobKind.PLAN_CONNECTOR);
    store.markRunning(parentJobId, parentLease.leaseEpoch, 100L, "executor-parent");
    store.markSucceeded(parentJobId, parentLease.leaseEpoch, 200L, 1L, 1L, 0L, 0L);

    StoredReconcileJob completedParent =
        waitForValue(
            () -> readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, parentJobId)),
            current -> "JS_SUCCEEDED".equals(current.state),
            "completed parent canonical state before late child enqueue");
    assertEquals("JS_SUCCEEDED", completedParent.state);
    assertTrue(
        completedParent.readyPointerKey == null || completedParent.readyPointerKey.isBlank());

    String childJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("db", "orders", "table-1", "orders"),
            ReconcileExecutionPolicy.defaults(),
            parentJobId,
            "");

    ReconcileJob waitingParent =
        waitForValue(
            () -> store.get(ACCOUNT_ID, parentJobId).orElseThrow(),
            current -> "JS_WAITING".equals(current.state),
            "parent waiting projection after late child enqueue");
    assertEquals("JS_WAITING", waitingParent.state);
    StoredReconcileJob canonicalParent =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, parentJobId));
    assertTrue(
        canonicalParent.readyPointerKey == null || canonicalParent.readyPointerKey.isBlank());
    assertTrue(
        readyPointerKeysFor(canonicalParent).stream()
            .allMatch(readyPointerKey -> !readyEntryExists(readyPointerKey)));
    assertTrue(
        store
            .leaseNext(
                ReconcileJobStore.LeaseRequest.of(
                    Set.of(), Set.of(), Set.of(), Set.of(ReconcileJobKind.PLAN_CONNECTOR)))
            .isEmpty());

    ReconcileJobStore.LeasedJob childLease = leaseJob(childJobId, ReconcileJobKind.PLAN_TABLE);
    assertEquals(childJobId, childLease.jobId);
  }

  @Test
  void connectorParentListSummaryWaitsForChildTablesBeforeShowingSucceeded() {
    store.init();

    String connectorJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());

    String tableJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("db", "orders", "table-1", "orders"),
            ReconcileExecutionPolicy.defaults(),
            connectorJobId,
            "");

    forceWaitingParentState(connectorJobId, "Waiting on child work", 100L, 19L, 0L, 0L);

    ReconcileJob waiting =
        waitForValue(
            () -> store.get(ACCOUNT_ID, connectorJobId).orElseThrow(),
            current ->
                "JS_WAITING".equals(current.state)
                    && current.tablesScanned == 19L
                    && current.tablesChanged == 0L
                    && current.finishedAtMs == 0L,
            "connector parent waiting summary after child enqueue");
    assertEquals("JS_WAITING", waiting.state);
    assertEquals(19L, waiting.tablesScanned);
    assertEquals(0L, waiting.tablesChanged);
    assertEquals(0L, waiting.finishedAtMs);
    assertEquals("Waiting on child work", waiting.message);

    ReconcileJob listedWaiting =
        waitForValue(
            () ->
                store.list(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
                    .filter(job -> connectorJobId.equals(job.jobId))
                    .findFirst()
                    .orElseThrow(),
            current ->
                "JS_WAITING".equals(current.state)
                    && current.tablesScanned == 19L
                    && current.tablesChanged == 0L
                    && current.finishedAtMs == 0L,
            "listed connector parent waiting summary");
    assertEquals("JS_WAITING", listedWaiting.state);
    assertEquals(19L, listedWaiting.tablesScanned);
    assertEquals(0L, listedWaiting.tablesChanged);
    assertEquals(0L, listedWaiting.finishedAtMs);
    assertEquals("Waiting on child work", listedWaiting.message);

    ReconcileJob stillWaiting =
        waitForValue(
            () -> store.get(ACCOUNT_ID, connectorJobId).orElseThrow(),
            current -> "JS_WAITING".equals(current.state),
            "connector parent remains waiting before child completion");
    assertEquals("JS_WAITING", stillWaiting.state);

    ReconcileJobStore.LeasedJob tableLease = leaseJob(tableJobId, ReconcileJobKind.PLAN_TABLE);
    store.markRunning(tableJobId, tableLease.leaseEpoch, 250L, "executor-table");
    store.markSucceeded(tableJobId, tableLease.leaseEpoch, 300L, 1L, 1L, 0L, 0L);

    ReconcileJob succeeded =
        waitForValue(
            () -> store.get(ACCOUNT_ID, connectorJobId).orElseThrow(),
            current ->
                "JS_SUCCEEDED".equals(current.state)
                    && "Succeeded".equals(current.message)
                    && current.finishedAtMs == 300L
                    && current.tablesScanned >= 18L
                    && current.tablesChanged == 1L,
            "connector parent succeeded summary after child completion");
    assertEquals("JS_SUCCEEDED", succeeded.state);
    assertEquals(300L, succeeded.finishedAtMs);
    assertTrue(succeeded.tablesScanned >= 18L);
    assertEquals(1L, succeeded.tablesChanged);
    assertEquals("Succeeded", succeeded.message);

    ReconcileJob listedSucceeded =
        waitForValue(
            () ->
                store.list(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
                    .filter(job -> connectorJobId.equals(job.jobId))
                    .findFirst()
                    .orElseThrow(),
            current ->
                "JS_SUCCEEDED".equals(current.state)
                    && "Succeeded".equals(current.message)
                    && current.finishedAtMs == 300L
                    && current.tablesScanned >= 18L
                    && current.tablesChanged == 1L,
            "listed connector parent succeeded summary");
    assertEquals("JS_SUCCEEDED", listedSucceeded.state);
    assertEquals(300L, listedSucceeded.finishedAtMs);
    assertTrue(listedSucceeded.tablesScanned >= 18L);
    assertEquals(1L, listedSucceeded.tablesChanged);
    assertEquals("Succeeded", listedSucceeded.message);
  }

  @Test
  void connectorParentNormalizesPlannedMessageToSucceededAfterChildCompletion() {
    store.init();

    String connectorJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());

    String tableJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("db", "orders", "table-1", "orders"),
            ReconcileExecutionPolicy.defaults(),
            connectorJobId,
            "");

    forceWaitingParentState(connectorJobId, "Planned 1 table job(s)", 100L, 1L, 0L, 0L);

    ReconcileJob waiting =
        waitForValue(
            () -> store.get(ACCOUNT_ID, connectorJobId).orElseThrow(),
            current -> "JS_WAITING".equals(current.state),
            "connector parent waiting state after planned child enqueue");
    assertEquals("JS_WAITING", waiting.state);
    assertEquals("Planned 1 table job(s)", waiting.message);

    ReconcileJobStore.LeasedJob tableLease = leaseJob(tableJobId, ReconcileJobKind.PLAN_TABLE);
    store.markRunning(tableJobId, tableLease.leaseEpoch, 250L, "executor-table");
    store.markSucceeded(tableJobId, tableLease.leaseEpoch, 300L, 1L, 1L, 0L, 0L);

    ReconcileJob succeeded =
        waitForValue(
            () -> store.get(ACCOUNT_ID, connectorJobId).orElseThrow(),
            current -> "JS_SUCCEEDED".equals(current.state) && "Succeeded".equals(current.message),
            "connector parent succeeded state after child completion");
    assertEquals("JS_SUCCEEDED", succeeded.state);
    assertEquals("Succeeded", succeeded.message);

    ReconcileJob listedSucceeded =
        waitForValue(
            () ->
                store.list(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
                    .filter(job -> connectorJobId.equals(job.jobId))
                    .findFirst()
                    .orElseThrow(),
            current -> "JS_SUCCEEDED".equals(current.state) && "Succeeded".equals(current.message),
            "listed connector parent succeeded state");
    assertEquals("JS_SUCCEEDED", listedSucceeded.state);
    assertEquals("Succeeded", listedSucceeded.message);
  }

  @Test
  void staleQueuedProjectionDoesNotHideCanonicalStartedAtForWaitingParent() {
    store.init();

    String connectorJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());

    store.runMaintenanceOnce(1_000L);

    ReconcileJobStore.LeasedJob connectorLease =
        leaseJob(connectorJobId, ReconcileJobKind.PLAN_CONNECTOR);
    store.markRunning(connectorJobId, connectorLease.leaseEpoch, 100L, "executor-connector");

    store.enqueue(
        ACCOUNT_ID,
        CONNECTOR_ID,
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.of(List.of(), "table-1"),
        ReconcileJobKind.PLAN_TABLE,
        ReconcileTableTask.of("db", "orders", "table-1", "orders"),
        ReconcileExecutionPolicy.defaults(),
        connectorJobId,
        "");

    assertTrue(
        store.applyLeaseOutcome(
            connectorJobId,
            connectorLease.leaseEpoch,
            ReconcileJobStore.CompletionKind.SUCCEEDED_WAITING,
            200L,
            "Planned 1 table job(s)",
            1L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L));

    long canonicalStartedAtMs = store.getLeaseView(connectorJobId).orElseThrow().startedAtMs;
    ReconcileJob waiting =
        waitForValue(
            () -> store.get(ACCOUNT_ID, connectorJobId).orElseThrow(),
            current ->
                "JS_WAITING".equals(current.state) && current.startedAtMs == canonicalStartedAtMs,
            "waiting parent preserves canonical startedAt");
    assertEquals("JS_WAITING", waiting.state);
    assertTrue(canonicalStartedAtMs > 0L);
    assertEquals(canonicalStartedAtMs, waiting.startedAtMs);

    ReconcileJob listedWaiting =
        waitForValue(
            () ->
                store.list(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
                    .filter(job -> connectorJobId.equals(job.jobId))
                    .findFirst()
                    .orElseThrow(),
            current ->
                "JS_WAITING".equals(current.state) && current.startedAtMs == canonicalStartedAtMs,
            "listed waiting parent preserves canonical startedAt");
    assertEquals("JS_WAITING", listedWaiting.state);
    assertEquals(canonicalStartedAtMs, listedWaiting.startedAtMs);
  }

  @Test
  void waitingParentTerminalTransitionCleansUpHistoricalReadyPointersWithoutStoredReadyKey()
      throws Exception {
    assumeMemoryOnly(
        "historical ready pointer cleanup assertions are only meaningful in memory mode");
    store.init();

    String connectorJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId);
    StoredReconcileJob queuedParent = readStoredRecord(canonicalPointerKey);
    List<String> staleReadyKeys = readyPointerKeysFor(queuedParent);

    ReconcileJobStore.LeasedJob connectorLease =
        leaseJob(connectorJobId, ReconcileJobKind.PLAN_CONNECTOR);
    store.markRunning(connectorJobId, connectorLease.leaseEpoch, 100L, "executor-connector");

    String tableJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("db", "orders", "table-1", "orders"),
            ReconcileExecutionPolicy.defaults(),
            connectorJobId,
            "");

    assertTrue(
        store.applyLeaseOutcome(
            connectorJobId,
            connectorLease.leaseEpoch,
            ReconcileJobStore.CompletionKind.SUCCEEDED_WAITING,
            200L,
            "Waiting on child work",
            1L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L));

    StoredReconcileJob waitingParent = readStoredRecord(canonicalPointerKey);
    assertEquals("JS_WAITING", waitingParent.state);
    assertTrue(waitingParent.readyPointerKey == null || waitingParent.readyPointerKey.isBlank());
    assertTrue(waitingParent.nextAttemptAtMs > 0L);

    for (String staleReadyKey : staleReadyKeys) {
      Pointer pointer =
          Pointer.newBuilder()
              .setKey(staleReadyKey)
              .setBlobUri(canonicalPointerKey)
              .setVersion(1L)
              .build();
      assertTrue(store.pointerStore.compareAndSet(staleReadyKey, 0L, pointer));
    }

    ReconcileJobStore.LeasedJob tableLease = leaseJob(tableJobId, ReconcileJobKind.PLAN_TABLE);
    store.markRunning(tableJobId, tableLease.leaseEpoch, 250L, "executor-table");
    store.markSucceeded(tableJobId, tableLease.leaseEpoch, 300L, 1L, 1L, 0L, 0L);

    ReconcileJob succeededParent =
        waitForValue(
            () -> store.get(ACCOUNT_ID, connectorJobId).orElseThrow(),
            job -> "JS_SUCCEEDED".equals(job.state) && job.finishedAtMs == 300L,
            "waiting parent succeeded projection transition");
    assertEquals("JS_SUCCEEDED", succeededParent.state);
    assertEquals("Succeeded", succeededParent.message);
    assertEquals(300L, succeededParent.finishedAtMs);
    assertTrue(
        store
            .leaseNext(
                ReconcileJobStore.LeaseRequest.of(
                    Set.of(), Set.of(), Set.of(), Set.of(ReconcileJobKind.PLAN_CONNECTOR)))
            .isEmpty());
  }

  @Test
  void tableParentWaitsForDirectSnapshotChildrenBeforeShowingSucceeded() {
    assumeMemoryOnly("pointer lease helper assertions are only meaningful in memory mode");
    store.init();

    String tableJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("db", "orders", "table-1", "orders"),
            ReconcileExecutionPolicy.defaults(),
            "parent-connector",
            "");
    String snapshotJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "orders", List.of(), true),
            ReconcileExecutionPolicy.defaults(),
            tableJobId,
            "");

    ReconcileJobStore.LeasedJob tableLease = leaseJob(tableJobId, ReconcileJobKind.PLAN_TABLE);
    store.markRunning(tableJobId, tableLease.leaseEpoch, 100L, "executor-table");
    assertTrue(
        store.applyLeaseOutcome(
            tableJobId,
            tableLease.leaseEpoch,
            ReconcileJobStore.CompletionKind.SUCCEEDED_WAITING,
            200L,
            "Waiting on child work",
            1L,
            0L,
            0L,
            0L,
            0L,
            1L,
            0L));

    ReconcileJob waiting =
        waitForValue(
            () -> store.get(ACCOUNT_ID, tableJobId).orElseThrow(),
            current -> "JS_WAITING".equals(current.state) && current.finishedAtMs == 0L,
            "table parent waiting before snapshot completion");
    assertEquals("JS_WAITING", waiting.state);
    assertEquals("Waiting on child work", waiting.message);
    assertEquals(0L, waiting.finishedAtMs);

    ReconcileJob listedWaiting =
        waitForValue(
            () ->
                store.list(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
                    .filter(job -> tableJobId.equals(job.jobId))
                    .findFirst()
                    .orElseThrow(),
            current -> "JS_WAITING".equals(current.state) && current.finishedAtMs == 0L,
            "listed table parent waiting before snapshot completion");
    assertEquals("JS_WAITING", listedWaiting.state);
    assertEquals(0L, listedWaiting.finishedAtMs);

    ReconcileJobStore.LeasedJob snapshotLease =
        leaseSpecificReadyJob(snapshotJobId, ReconcileJobKind.PLAN_SNAPSHOT);
    store.markRunning(snapshotJobId, snapshotLease.leaseEpoch, 250L, "executor-snapshot");
    store.markSucceeded(snapshotJobId, snapshotLease.leaseEpoch, 300L, 0L, 0L, 0L, 0L);

    ReconcileJob succeeded =
        waitForValue(
            () -> store.get(ACCOUNT_ID, tableJobId).orElseThrow(),
            job ->
                "JS_SUCCEEDED".equals(job.state)
                    && "Succeeded".equals(job.message)
                    && job.finishedAtMs == 300L,
            "table parent succeeded after direct snapshot child completes");
    assertEquals("JS_SUCCEEDED", succeeded.state);
    assertEquals("Succeeded", succeeded.message);
    assertEquals(300L, succeeded.finishedAtMs);

    ReconcileJob listedSucceeded =
        waitForValue(
            () ->
                store.list(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
                    .filter(job -> tableJobId.equals(job.jobId))
                    .findFirst()
                    .orElseThrow(),
            job ->
                "JS_SUCCEEDED".equals(job.state)
                    && "Succeeded".equals(job.message)
                    && job.finishedAtMs == 300L,
            "listed table parent succeeded after direct snapshot child completes");
    assertEquals("JS_SUCCEEDED", listedSucceeded.state);
    assertEquals("Succeeded", listedSucceeded.message);
    assertEquals(300L, listedSucceeded.finishedAtMs);
  }

  @Test
  void childEnqueueDoesNotFlipLeasedParentToWaitingBeforeLeaseOutcome() {
    store.init();

    String connectorJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());

    ReconcileJobStore.LeasedJob connectorLease =
        leaseJob(connectorJobId, ReconcileJobKind.PLAN_CONNECTOR);
    store.markRunning(connectorJobId, connectorLease.leaseEpoch, 100L, "executor-connector");

    store.enqueue(
        ACCOUNT_ID,
        CONNECTOR_ID,
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.of(List.of(), "table-1"),
        ReconcileJobKind.PLAN_TABLE,
        ReconcileTableTask.of("db", "orders", "table-1", "orders"),
        ReconcileExecutionPolicy.defaults(),
        connectorJobId,
        "");

    ReconcileJob leaseViewBeforeSuccess = store.getLeaseView(connectorJobId).orElseThrow();
    assertEquals("JS_RUNNING", leaseViewBeforeSuccess.state);
    assertEquals("Running", leaseViewBeforeSuccess.message);

    assertTrue(
        store.applyLeaseOutcome(
            connectorJobId,
            connectorLease.leaseEpoch,
            ReconcileJobStore.CompletionKind.SUCCEEDED_WAITING,
            200L,
            "Waiting on child work",
            0L,
            1L,
            0L,
            0L,
            0L,
            0L,
            0L));

    ReconcileJob leaseViewAfterSuccess =
        waitForValue(
            () -> store.getLeaseView(connectorJobId).orElseThrow(),
            current -> "JS_WAITING".equals(current.state),
            "lease view waiting state after parent success");
    assertEquals("JS_WAITING", leaseViewAfterSuccess.state);
    assertEquals("Waiting on child work", leaseViewAfterSuccess.message);
  }

  @Test
  void waitingParentStaysCanonicalWaitingWhileChildIsRunning() {
    store.init();

    String connectorJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());

    String tableJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("db", "orders", "table-1", "orders"),
            ReconcileExecutionPolicy.defaults(),
            connectorJobId,
            "");

    forceWaitingParentState(connectorJobId, "Waiting on child work", 100L, 1L, 0L, 0L);

    ReconcileJob waitingParent =
        waitForValue(
            () -> store.get(ACCOUNT_ID, connectorJobId).orElseThrow(),
            current -> "JS_WAITING".equals(current.state),
            "waiting parent state after parent success");
    assertEquals("JS_WAITING", waitingParent.state);
    assertEquals("Waiting on child work", waitingParent.message);

    ReconcileJobStore.LeasedJob tableLease = leaseJob(tableJobId, ReconcileJobKind.PLAN_TABLE);
    store.markRunning(tableJobId, tableLease.leaseEpoch, 250L, "executor-table");

    ReconcileJob stillWaitingParent =
        waitForValue(
            () -> store.get(ACCOUNT_ID, connectorJobId).orElseThrow(),
            current -> "JS_WAITING".equals(current.state),
            "waiting parent state while child is running");
    assertEquals("JS_WAITING", stillWaitingParent.state);
    assertEquals("Waiting on child work", stillWaitingParent.message);

    StoredReconcileJob storedParent =
        waitForValue(
            () -> readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId)),
            current -> "JS_WAITING".equals(current.state),
            "stored parent waiting state while child is running");
    assertEquals("JS_WAITING", storedParent.state);
    assertEquals("Waiting on child work", storedParent.message);
  }

  @Test
  void waitingParentStaysWaitingWhileChildRunsEvenIfExpectedChildJobsIsCorruptedLow()
      throws Exception {
    assumeMemoryOnly("pointer corruption assertions are only meaningful in memory mode");
    store.init();

    String connectorJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());

    String tableJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("db", "orders", "table-1", "orders"),
            ReconcileExecutionPolicy.defaults(),
            connectorJobId,
            "");

    forceWaitingParentState(connectorJobId, "Waiting on child work", 100L, 1L, 0L, 0L);

    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId);

    ReconcileJobStore.LeasedJob tableLease = leaseJob(tableJobId, ReconcileJobKind.PLAN_TABLE);
    store.markRunning(tableJobId, tableLease.leaseEpoch, 250L, "executor-table");

    StoredReconcileJob storedParent = readStoredRecord(canonicalPointerKey);
    assertEquals("JS_WAITING", storedParent.state);
    assertEquals("Waiting on child work", storedParent.message);

    ReconcileJob projectedParent =
        waitForValue(
            () -> store.get(ACCOUNT_ID, connectorJobId).orElseThrow(),
            current ->
                "JS_WAITING".equals(current.state)
                    && "Waiting on child work".equals(current.message),
            "projected waiting parent " + connectorJobId);
    assertEquals("JS_WAITING", projectedParent.state);
    assertEquals("Waiting on child work", projectedParent.message);
  }

  @Test
  void snapshotParentListSummaryWaitsForAllPlannedFileGroupsBeforeShowingSucceeded() {
    store.init();

    String tableJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("db", "orders", "table-1", "orders"),
            ReconcileExecutionPolicy.defaults(),
            "parent-connector",
            "");
    String snapshotJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileSnapshotTask.of(
                "table-1",
                55L,
                "db",
                "orders",
                List.of(
                    ReconcileFileGroupTask.of(
                        "plan-1",
                        "group-1",
                        "table-1",
                        55L,
                        List.of("s3://bucket/data/file-1.parquet")),
                    ReconcileFileGroupTask.of(
                        "plan-1",
                        "group-2",
                        "table-1",
                        55L,
                        List.of("s3://bucket/data/file-2.parquet"))),
                true),
            ReconcileExecutionPolicy.defaults(),
            tableJobId,
            "");
    String execJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileJobKind.EXEC_FILE_GROUP,
            ReconcileTableTask.empty(),
            ReconcileViewTask.empty(),
            ReconcileSnapshotTask.empty(),
            ReconcileFileGroupTask.of(
                "plan-1", "group-1", "table-1", 55L, List.of("s3://bucket/data/file-1.parquet")),
            ReconcileExecutionPolicy.defaults(),
            snapshotJobId,
            "");
    String execJobId2 =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileJobKind.EXEC_FILE_GROUP,
            ReconcileTableTask.empty(),
            ReconcileViewTask.empty(),
            ReconcileSnapshotTask.empty(),
            ReconcileFileGroupTask.of(
                "plan-1", "group-2", "table-1", 55L, List.of("s3://bucket/data/file-2.parquet")),
            ReconcileExecutionPolicy.defaults(),
            snapshotJobId,
            "");

    ReconcileJobStore.LeasedJob snapshotLease =
        leaseSpecificReadyJob(snapshotJobId, ReconcileJobKind.PLAN_SNAPSHOT);
    store.markRunning(snapshotJobId, snapshotLease.leaseEpoch, 100L, "executor-snapshot");
    assertTrue(
        store.applyLeaseOutcome(
            snapshotJobId,
            snapshotLease.leaseEpoch,
            ReconcileJobStore.CompletionKind.SUCCEEDED_WAITING,
            200L,
            "Waiting on child work",
            0L,
            0L,
            1L,
            1L,
            0L,
            0L,
            0L));

    ReconcileJobStore.LeasedJob execLease =
        store
            .leaseNext(
                ReconcileJobStore.LeaseRequest.of(
                    Set.of(), Set.of(), Set.of(), Set.of(ReconcileJobKind.EXEC_FILE_GROUP)))
            .orElseThrow();
    String firstExecJobId = execLease.jobId;
    String firstExecGroupId = execJobId.equals(firstExecJobId) ? "group-1" : "group-2";
    String firstExecFile =
        execJobId.equals(firstExecJobId)
            ? "s3://bucket/data/file-1.parquet"
            : "s3://bucket/data/file-2.parquet";
    long firstExecRows = execJobId.equals(firstExecJobId) ? 3L : 4L;
    store.markRunning(firstExecJobId, execLease.leaseEpoch, 250L, "executor-exec");
    store.persistFileGroupResult(
        firstExecJobId,
        ReconcileFileGroupTask.of(
            "plan-1",
            firstExecGroupId,
            "table-1",
            55L,
            List.of(firstExecFile),
            List.of(ReconcileFileResult.succeeded(firstExecFile, firstExecRows))));
    store.markSucceeded(firstExecJobId, execLease.leaseEpoch, 300L, 0L, 0L, 0L, 0L, 0L, 1L);

    ReconcileJob snapshot =
        waitForValue(
            () -> store.get(ACCOUNT_ID, snapshotJobId).orElseThrow(),
            current ->
                "JS_WAITING".equals(current.state)
                    && current.plannedFileGroups == 2L
                    && current.completedFileGroups == 1L,
            "snapshot parent waiting summary after first file group completes");
    assertEquals("JS_WAITING", snapshot.state);
    assertEquals(2L, snapshot.plannedFileGroups);
    assertEquals(1L, snapshot.completedFileGroups);
    assertEquals("Waiting on child work", snapshot.message);

    ReconcileJob listedWaiting =
        waitForValue(
            () -> findListedJobById(snapshotJobId),
            current ->
                "JS_WAITING".equals(current.state)
                    && current.plannedFileGroups == 2L
                    && current.completedFileGroups == 1L,
            "listed snapshot parent waiting summary");
    assertEquals("JS_WAITING", listedWaiting.state);
    assertEquals(2L, listedWaiting.plannedFileGroups);
    assertEquals(1L, listedWaiting.completedFileGroups);
    assertEquals("Waiting on child work", listedWaiting.message);

    String secondExecJobId = execJobId.equals(firstExecJobId) ? execJobId2 : execJobId;
    ReconcileJobStore.LeasedJob execLease2 = leaseJob(secondExecJobId);
    String secondExecGroupId = execJobId.equals(secondExecJobId) ? "group-1" : "group-2";
    String secondExecFile =
        execJobId.equals(secondExecJobId)
            ? "s3://bucket/data/file-1.parquet"
            : "s3://bucket/data/file-2.parquet";
    long secondExecRows = execJobId.equals(secondExecJobId) ? 3L : 4L;
    store.markRunning(secondExecJobId, execLease2.leaseEpoch, 350L, "executor-exec");
    store.persistFileGroupResult(
        secondExecJobId,
        ReconcileFileGroupTask.of(
            "plan-1",
            secondExecGroupId,
            "table-1",
            55L,
            List.of(secondExecFile),
            List.of(ReconcileFileResult.succeeded(secondExecFile, secondExecRows))));
    store.markSucceeded(secondExecJobId, execLease2.leaseEpoch, 400L, 0L, 0L, 0L, 0L, 0L, 1L);

    ReconcileJob snapshotSucceeded =
        waitForValue(
            () -> store.get(ACCOUNT_ID, snapshotJobId).orElseThrow(),
            current ->
                "JS_SUCCEEDED".equals(current.state)
                    && "Succeeded".equals(current.message)
                    && current.plannedFileGroups == 2L
                    && current.completedFileGroups == 2L,
            "snapshot parent succeeded summary after all file groups complete");
    assertEquals("JS_SUCCEEDED", snapshotSucceeded.state);
    assertEquals(2L, snapshotSucceeded.plannedFileGroups);
    assertEquals(2L, snapshotSucceeded.completedFileGroups);
    assertEquals("Succeeded", snapshotSucceeded.message);

    ReconcileJob listedSucceeded =
        waitForValue(
            () -> findListedJobById(snapshotJobId),
            current ->
                "JS_SUCCEEDED".equals(current.state)
                    && "Succeeded".equals(current.message)
                    && current.plannedFileGroups == 2L
                    && current.completedFileGroups == 2L,
            "listed snapshot parent succeeded summary");
    assertEquals("JS_SUCCEEDED", listedSucceeded.state);
    assertEquals(2L, listedSucceeded.plannedFileGroups);
    assertEquals(2L, listedSucceeded.completedFileGroups);
    assertEquals("Succeeded", listedSucceeded.message);
  }

  @Test
  void terminalParentRollupRefreshesAfterLateChildContribution() {
    store.init();

    String connectorJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());

    ReconcileJobStore.LeasedJob connectorLease =
        leaseJob(connectorJobId, ReconcileJobKind.PLAN_CONNECTOR);
    store.markRunning(connectorJobId, connectorLease.leaseEpoch, 100L, "executor-connector");
    store.markSucceeded(connectorJobId, connectorLease.leaseEpoch, 200L, 0L, 0L, 0L, 0L);

    ReconcileJob terminalBefore =
        waitForValue(
            () -> store.get(ACCOUNT_ID, connectorJobId).orElseThrow(),
            current ->
                "JS_SUCCEEDED".equals(current.state)
                    && "Succeeded".equals(current.message)
                    && current.finishedAtMs == 200L,
            "terminal parent before late child contribution");
    assertEquals("JS_SUCCEEDED", terminalBefore.state);
    assertEquals("Succeeded", terminalBefore.message);
    assertEquals(200L, terminalBefore.finishedAtMs);

    String childJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("db", "orders", "table-1", "orders"),
            ReconcileExecutionPolicy.defaults(),
            connectorJobId,
            "");

    ReconcileJobStore.LeasedJob childLease = leaseJob(childJobId, ReconcileJobKind.PLAN_TABLE);
    store.markRunning(childJobId, childLease.leaseEpoch, 250L, "executor-table");
    store.markSucceeded(childJobId, childLease.leaseEpoch, 400L, 1L, 1L, 0L, 0L);

    ReconcileJob terminalAfter =
        waitForValue(
            () -> store.get(ACCOUNT_ID, connectorJobId).orElseThrow(),
            current ->
                "JS_SUCCEEDED".equals(current.state)
                    && "Succeeded".equals(current.message)
                    && current.finishedAtMs == 400L
                    && current.tablesScanned == 1L
                    && current.tablesChanged == 1L,
            "terminal parent rollup after late child contribution");
    assertTrue(terminalAfter.aggregateSummaryPresent);
    assertEquals("JS_SUCCEEDED", terminalAfter.state);
    assertEquals("Succeeded", terminalAfter.message);
    assertEquals(400L, terminalAfter.finishedAtMs);
    assertEquals(1L, terminalAfter.tablesScanned);
    assertEquals(1L, terminalAfter.tablesChanged);

    ReconcileJob listedTerminal =
        waitForValue(
            () ->
                store.list(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
                    .filter(job -> connectorJobId.equals(job.jobId))
                    .findFirst()
                    .orElseThrow(),
            current ->
                "JS_SUCCEEDED".equals(current.state)
                    && "Succeeded".equals(current.message)
                    && current.finishedAtMs == 400L
                    && current.tablesScanned == 1L
                    && current.tablesChanged == 1L,
            "listed terminal parent rollup after late child contribution");
    assertEquals("JS_SUCCEEDED", listedTerminal.state);
    assertEquals("Succeeded", listedTerminal.message);
    assertEquals(400L, listedTerminal.finishedAtMs);
    assertEquals(1L, listedTerminal.tablesScanned);
    assertEquals(1L, listedTerminal.tablesChanged);

    ReconcileJob listedRootTerminal =
        waitForValue(
            () ->
                store.listRootJobs(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
                    .filter(job -> connectorJobId.equals(job.jobId))
                    .findFirst()
                    .orElseThrow(),
            current ->
                "JS_SUCCEEDED".equals(current.state)
                    && "Succeeded".equals(current.message)
                    && current.finishedAtMs == 400L
                    && current.tablesScanned == 1L
                    && current.tablesChanged == 1L,
            "root summary terminal parent rollup after late child contribution");
    assertEquals("JS_SUCCEEDED", listedRootTerminal.state);
    assertEquals("Succeeded", listedRootTerminal.message);
    assertEquals(400L, listedRootTerminal.finishedAtMs);
    assertEquals(1L, listedRootTerminal.tablesScanned);
    assertEquals(1L, listedRootTerminal.tablesChanged);
  }

  @Test
  void nestedContributionsPropagateFileGroupResultsAndStayOutOfCanonicalState() throws Exception {
    store.init();

    String connectorJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    String tableJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("db", "orders", "table-1", "orders"),
            ReconcileExecutionPolicy.defaults(),
            connectorJobId,
            "");
    String snapshotJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "orders", List.of(), true),
            ReconcileExecutionPolicy.defaults(),
            tableJobId,
            "");
    String execJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileJobKind.EXEC_FILE_GROUP,
            ReconcileTableTask.empty(),
            ReconcileViewTask.empty(),
            ReconcileSnapshotTask.empty(),
            ReconcileFileGroupTask.of(
                "plan-1",
                "group-1",
                "table-1",
                55L,
                List.of("s3://bucket/data/file-1.parquet", "s3://bucket/data/file-2.parquet")),
            ReconcileExecutionPolicy.defaults(),
            snapshotJobId,
            "");

    ReconcileJobStore.LeasedJob connectorLease =
        leaseJob(connectorJobId, ReconcileJobKind.PLAN_CONNECTOR);
    store.markRunning(connectorJobId, connectorLease.leaseEpoch, 50L, "executor-connector");
    assertTrue(
        store.applyLeaseOutcome(
            connectorJobId,
            connectorLease.leaseEpoch,
            ReconcileJobStore.CompletionKind.SUCCEEDED_WAITING,
            60L,
            "Waiting on child work",
            1L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L));

    ReconcileJobStore.LeasedJob tableLease = leaseJob(tableJobId, ReconcileJobKind.PLAN_TABLE);
    store.markRunning(tableJobId, tableLease.leaseEpoch, 100L, "executor-table");
    assertTrue(
        store.applyLeaseOutcome(
            tableJobId,
            tableLease.leaseEpoch,
            ReconcileJobStore.CompletionKind.SUCCEEDED_WAITING,
            125L,
            "Waiting on child work",
            1L,
            1L,
            0L,
            0L,
            0L,
            0L,
            0L));

    ReconcileJobStore.LeasedJob execLease = leaseJob(execJobId, ReconcileJobKind.EXEC_FILE_GROUP);
    store.markRunning(execJobId, execLease.leaseEpoch, 150L, "executor-exec");
    store.persistFileGroupResult(
        execJobId,
        ReconcileFileGroupTask.of(
            "plan-1",
            "group-1",
            "table-1",
            55L,
            List.of("s3://bucket/data/file-1.parquet", "s3://bucket/data/file-2.parquet"),
            List.of(
                ReconcileFileResult.succeeded(
                    "s3://bucket/data/file-1.parquet",
                    2L,
                    ReconcileIndexArtifactResult.of("s3://bucket/index/file-1.idx", "parquet", 1)),
                ReconcileFileResult.failed("s3://bucket/data/file-2.parquet", "boom"))));
    store.markSucceeded(execJobId, execLease.leaseEpoch, 200L, 0L, 0L, 0L, 0L, 0L, 2L);

    StoredReconcileJobProjection snapshot =
        waitForValue(
                () -> projectionStore().load(ACCOUNT_ID, snapshotJobId),
                Optional::isPresent,
                "snapshot projection " + snapshotJobId)
            .orElseThrow();
    ReconcileJob table =
        waitForValue(
                () ->
                    store
                        .get(ACCOUNT_ID, tableJobId)
                        .filter(
                            current ->
                                current.plannedFileGroups == 1L
                                    && current.completedFiles == 1L
                                    && current.failedFiles == 1L
                                    && current.indexesProcessed == 1L),
                Optional::isPresent,
                "table projection " + tableJobId)
            .orElseThrow();
    ReconcileJob connector =
        waitForValue(
                () ->
                    store
                        .get(ACCOUNT_ID, connectorJobId)
                        .filter(
                            current ->
                                current.tablesScanned == 1L
                                    && current.tablesChanged == 1L
                                    && current.completedFiles == 1L
                                    && current.failedFiles == 1L
                                    && current.indexesProcessed == 1L),
                Optional::isPresent,
                "connector projection " + connectorJobId)
            .orElseThrow();

    assertEquals(1L, snapshot.plannedFileGroups());
    assertEquals(2L, snapshot.plannedFiles());
    assertEquals(1L, snapshot.completedFileGroups());
    assertEquals(1L, snapshot.completedFiles());
    assertEquals(1L, snapshot.failedFiles());
    assertEquals(1L, snapshot.indexesProcessed());
    assertEquals(2L, snapshot.statsProcessed());

    assertEquals(1L, table.plannedFileGroups);
    assertEquals(1L, table.completedFiles);
    assertEquals(1L, table.failedFiles);
    assertEquals(1L, table.indexesProcessed);
    assertEquals(1L, connector.tablesScanned);
    assertEquals(1L, connector.tablesChanged);
    assertEquals(1L, connector.completedFiles);
    assertEquals(1L, connector.failedFiles);
    assertEquals(1L, connector.indexesProcessed);

    ReconcileJob listed =
        waitForValue(
                () ->
                    store.list(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
                        .filter(job -> connectorJobId.equals(job.jobId))
                        .filter(
                            job ->
                                job.aggregateSummaryPresent
                                    && job.completedFiles == 1L
                                    && job.failedFiles == 1L
                                    && job.indexesProcessed == 1L)
                        .findFirst(),
                Optional::isPresent,
                "listed connector projection " + connectorJobId)
            .orElseThrow();
    assertTrue(listed.aggregateSummaryPresent);
    assertEquals(1L, listed.completedFiles);
    assertEquals(1L, listed.failedFiles);
    assertEquals(1L, listed.indexesProcessed);

    var stateJson =
        store.mapper.valueToTree(
            readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, snapshotJobId)));
    assertEquals(0L, stateJson.get("plannedFileGroups").asLong());
    assertEquals(0L, stateJson.get("plannedFiles").asLong());
    assertEquals(0L, stateJson.get("completedFileGroups").asLong());
    assertEquals(0L, stateJson.get("failedFileGroups").asLong());
    assertEquals(0L, stateJson.get("completedFiles").asLong());
    assertEquals(0L, stateJson.get("failedFiles").asLong());
    assertEquals(0L, stateJson.get("indexesProcessed").asLong());
  }

  private static ReconcileJobStore.BulkEnqueueSpec fileGroupSpec(
      String parentJobId, String groupId, String filePath) {
    return ReconcileJobStore.BulkEnqueueSpec.of(
        ACCOUNT_ID,
        CONNECTOR_ID,
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.of(List.of(), "table-1"),
        ReconcileJobKind.EXEC_FILE_GROUP,
        ReconcileTableTask.empty(),
        ReconcileViewTask.empty(),
        ReconcileSnapshotTask.empty(),
        ReconcileFileGroupTask.of("plan-1", groupId, "table-1", 55L, List.of(filePath)),
        ReconcileExecutionPolicy.defaults(),
        parentJobId,
        "");
  }

  private static ReconcileJobStore.BulkEnqueueSpec fileGroupResultSpec(
      String parentJobId, String groupId, String filePath, ReconcileFileResult fileResult) {
    return ReconcileJobStore.BulkEnqueueSpec.of(
        ACCOUNT_ID,
        CONNECTOR_ID,
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.of(List.of(), "table-1"),
        ReconcileJobKind.EXEC_FILE_GROUP,
        ReconcileTableTask.empty(),
        ReconcileViewTask.empty(),
        ReconcileSnapshotTask.empty(),
        ReconcileFileGroupTask.of(
            "plan-1", groupId, "table-1", 55L, List.of(filePath), List.of(fileResult)),
        ReconcileExecutionPolicy.defaults(),
        parentJobId,
        "");
  }

  private Object invokePrivateMethod(String name, Class<?>[] parameterTypes, Object... args)
      throws Exception {
    return invokePrivateMethod(store, name, parameterTypes, args);
  }

  private Object invokePrivateMethod(
      Object target, String name, Class<?>[] parameterTypes, Object... args) throws Exception {
    Method method = target.getClass().getDeclaredMethod(name, parameterTypes);
    method.setAccessible(true);
    return method.invoke(target, args);
  }

  private void deletePointerIfPresent(String key) {
    store
        .pointerStore
        .get(key)
        .ifPresent(pointer -> store.pointerStore.compareAndDelete(key, pointer.getVersion()));
  }

  private List<String> readyPointerKeysFor(StoredReconcileJob record) {
    try {
      return readyQueue().readyPointerKeys(record);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private boolean readyEntryExists(String readyPointerKey) {
    try {
      ReconcileReadyQueueBackend.ReadyQueueSlice slice = readySliceForKey(readyPointerKey);
      if (slice == null) {
        return false;
      }
      String token = "";
      for (int pages = 0; pages < 1_000; pages++) {
        var page = readyQueue().scanReadySlice(slice, 100, token);
        if (page.entries().stream()
            .anyMatch(entry -> readyPointerKey.equals(entry.readyPointerKey()))) {
          return true;
        }
        if (page.nextPageToken().isBlank() || page.nextPageToken().equals(token)) {
          return false;
        }
        token = page.nextPageToken();
      }
      throw new IllegalStateException("Ready scan page cap exceeded for key " + readyPointerKey);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private boolean anyReadyEntryMatches(java.util.function.Predicate<String> keyPredicate) {
    try {
      for (var slice : allReadySlices()) {
        String token = "";
        for (int pages = 0; pages < 1_000; pages++) {
          var page = readyQueue().scanReadySlice(slice, 100, token);
          if (page.entries().stream()
              .map(entry -> entry.readyPointerKey())
              .anyMatch(keyPredicate)) {
            return true;
          }
          if (page.nextPageToken().isBlank() || page.nextPageToken().equals(token)) {
            break;
          }
          token = page.nextPageToken();
        }
      }
      return false;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private int globalReadyEntryCount() {
    return readyEntriesForSlice(
            new ReconcileReadyQueueBackend.ReadyQueueSlice(
                ReconcileReadyQueueStore.ReadyIndexType.GLOBAL, ""))
        .size();
  }

  private List<String> readyEntryKeysForSlice(
      ReconcileReadyQueueStore.ReadyIndexType indexType, String filterValue) {
    return readyEntriesForSlice(
            new ReconcileReadyQueueBackend.ReadyQueueSlice(indexType, filterValue))
        .stream()
        .map(entry -> entry.readyPointerKey())
        .toList();
  }

  private List<ReconcileReadyQueueStore.ReadyQueueEntry> readyEntriesForSlice(
      ReconcileReadyQueueBackend.ReadyQueueSlice slice) {
    try {
      List<ReconcileReadyQueueStore.ReadyQueueEntry> entries = new ArrayList<>();
      String token = "";
      for (int pages = 0; pages < 1_000; pages++) {
        var page = readyQueue().scanReadySlice(slice, 100, token);
        entries.addAll(page.entries());
        if (page.nextPageToken().isBlank() || page.nextPageToken().equals(token)) {
          return entries;
        }
        token = page.nextPageToken();
      }
      throw new IllegalStateException("Ready scan page cap exceeded for slice " + slice);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private List<ReconcileReadyQueueBackend.ReadyQueueSlice> allReadySlices() {
    return List.of(
        new ReconcileReadyQueueBackend.ReadyQueueSlice(
            ReconcileReadyQueueStore.ReadyIndexType.GLOBAL, ""),
        new ReconcileReadyQueueBackend.ReadyQueueSlice(
            ReconcileReadyQueueStore.ReadyIndexType.EXECUTION_CLASS,
            ReconcileExecutionClass.BATCH.name()),
        new ReconcileReadyQueueBackend.ReadyQueueSlice(
            ReconcileReadyQueueStore.ReadyIndexType.EXECUTION_CLASS,
            ReconcileExecutionClass.DEFAULT.name()),
        new ReconcileReadyQueueBackend.ReadyQueueSlice(
            ReconcileReadyQueueStore.ReadyIndexType.EXECUTION_CLASS,
            ReconcileExecutionClass.HEAVY.name()),
        new ReconcileReadyQueueBackend.ReadyQueueSlice(
            ReconcileReadyQueueStore.ReadyIndexType.EXECUTION_LANE, ""),
        new ReconcileReadyQueueBackend.ReadyQueueSlice(
            ReconcileReadyQueueStore.ReadyIndexType.EXECUTION_LANE, "lane-cancel"),
        new ReconcileReadyQueueBackend.ReadyQueueSlice(
            ReconcileReadyQueueStore.ReadyIndexType.EXECUTION_LANE, "lane-orphan"),
        new ReconcileReadyQueueBackend.ReadyQueueSlice(
            ReconcileReadyQueueStore.ReadyIndexType.EXECUTION_LANE, "lane-partial"),
        new ReconcileReadyQueueBackend.ReadyQueueSlice(
            ReconcileReadyQueueStore.ReadyIndexType.EXECUTION_LANE, "lane-retry"),
        new ReconcileReadyQueueBackend.ReadyQueueSlice(
            ReconcileReadyQueueStore.ReadyIndexType.PINNED_EXECUTOR, "executor-cancel"),
        new ReconcileReadyQueueBackend.ReadyQueueSlice(
            ReconcileReadyQueueStore.ReadyIndexType.PINNED_EXECUTOR, "executor-partial"),
        new ReconcileReadyQueueBackend.ReadyQueueSlice(
            ReconcileReadyQueueStore.ReadyIndexType.PINNED_EXECUTOR, "executor-retry"),
        new ReconcileReadyQueueBackend.ReadyQueueSlice(
            ReconcileReadyQueueStore.ReadyIndexType.JOB_KIND,
            ReconcileJobKind.EXEC_FILE_GROUP.name()),
        new ReconcileReadyQueueBackend.ReadyQueueSlice(
            ReconcileReadyQueueStore.ReadyIndexType.JOB_KIND,
            ReconcileJobKind.PLAN_CONNECTOR.name()),
        new ReconcileReadyQueueBackend.ReadyQueueSlice(
            ReconcileReadyQueueStore.ReadyIndexType.JOB_KIND, ReconcileJobKind.PLAN_VIEW.name()));
  }

  private ReconcileJob findListedJobById(String jobId) {
    return waitForValue(
            () -> findListedJobByIdOnce(jobId), Optional::isPresent, "listed job " + jobId)
        .orElseThrow();
  }

  private Optional<ReconcileJob> findListedJobByIdOnce(String jobId) {
    String pageToken = "";
    do {
      var page = store.list(ACCOUNT_ID, 100, pageToken, CONNECTOR_ID, Set.of());
      Optional<ReconcileJob> job =
          page.jobs.stream().filter(candidate -> jobId.equals(candidate.jobId)).findFirst();
      if (job.isPresent()) {
        return job;
      }
      pageToken = page.nextPageToken;
    } while (pageToken != null && !pageToken.isBlank());
    return Optional.empty();
  }

  private void forceWaitingParentState(
      String jobId,
      String message,
      long startedAtMs,
      long tablesScanned,
      long tablesChanged,
      long snapshotsProcessed) {
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    StoredReconcileJob record = readStoredRecord(canonicalPointerKey);
    for (String readyPointerKey : readyPointerKeysFor(record)) {
      deletePointerIfPresent(readyPointerKey);
    }
    record.state = "JS_WAITING";
    record.message = message;
    record.startedAtMs = startedAtMs;
    record.finishedAtMs = 0L;
    record.tablesScanned = tablesScanned;
    record.tablesChanged = tablesChanged;
    record.snapshotsProcessed = snapshotsProcessed;
    record.executorId = "";
    record.readyPointerKey = "";
    record.nextAttemptAtMs = 0L;
    record.updatedAtMs = Math.max(record.updatedAtMs, startedAtMs);
    overwriteCanonicalRecordWithoutSync(canonicalPointerKey, record);
    runMaintenance();
  }

  private void forceQueuedJobDueNow(String jobId) {
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    StoredReconcileJob existing = readStoredRecord(canonicalPointerKey);
    clearLeaseArtifactsIfPresent(existing);
    long dueAtMs = Math.max(1L, System.currentTimeMillis() - 1L);
    StoredReconcileJob queued =
        assertDoesNotThrow(
                () ->
                    store.jobIndexStore.mutateByCanonicalPointerReturningRecord(
                        canonicalPointerKey,
                        current -> {
                          current.state = "JS_QUEUED";
                          current.executorId = "";
                          current.finishedAtMs = 0L;
                          current.nextAttemptAtMs = dueAtMs;
                          current.updatedAtMs = Math.max(current.updatedAtMs, dueAtMs);
                          List<String> readyPointerKeys = readyPointerKeysFor(current);
                          current.readyPointerKey =
                              readyPointerKeys.isEmpty() ? "" : readyPointerKeys.getFirst();
                          return current;
                        }))
            .orElseThrow()
            .record();
    assertFalse(queued.readyPointerKey == null || queued.readyPointerKey.isBlank());
    assertTrue(readyEntryExists(queued.readyPointerKey));
  }

  private void clearLeaseArtifactsIfPresent(StoredReconcileJob record) {
    List<ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileLeaseBackend.LeaseWriteOp>
        writes = new ArrayList<>();
    var leaseSnapshot = store.leaseBackend.loadLease(ACCOUNT_ID, record.jobId).orElse(null);
    var decodedLease =
        assertDoesNotThrow(() -> store.leaseStore.loadLease(ACCOUNT_ID, record.jobId));
    if (leaseSnapshot != null) {
      writes.add(
          new ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileLeaseBackend
              .LeaseRecordDelete(ACCOUNT_ID, record.jobId, leaseSnapshot.version()));
    }
    if (decodedLease.isPresent()) {
      String expiryKey =
          leaseExpiryPointerKey(decodedLease.get().expiresAtMs, ACCOUNT_ID, record.jobId);
      var expirySnapshot = store.leaseBackend.loadLeaseExpiry(expiryKey).orElse(null);
      if (expirySnapshot != null) {
        writes.add(
            new ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileLeaseBackend
                .LeaseExpiryDelete(expiryKey, expirySnapshot.version()));
      }
    }
    String laneOwnerKey = Keys.reconcileLaneLeasePointer(ACCOUNT_ID, record.laneKey);
    var ownerSnapshot = store.leaseBackend.loadOwner(laneOwnerKey).orElse(null);
    if (ownerSnapshot != null) {
      writes.add(
          new ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileLeaseBackend
              .LeaseOwnerDelete(laneOwnerKey, ownerSnapshot.version()));
    }
    if (!writes.isEmpty()) {
      assertTrue(
          store.leaseBackend.compareAndSetBatch(
              ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexStore
                  .JobIndexWriteBatch.empty(),
              new ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileLeaseBackend
                  .LeaseWriteBatch(List.copyOf(writes))));
    }
  }

  private void forceLeaseExpired(String jobId) {
    forceLeaseExpiry(jobId, Math.max(1L, System.currentTimeMillis() - 1L));
  }

  private void forceLeaseExpiry(String jobId, long expiresAtMs) {
    assertTrue(
        store
            .leaseStore
            .mutateLease(
                ACCOUNT_ID,
                jobId,
                current -> {
                  current.expiresAtMs = expiresAtMs;
                  return current;
                })
            .isPresent(),
        () -> "expected active lease for job " + jobId);
  }

  private <T> T waitForValue(
      java.util.function.Supplier<T> supplier,
      java.util.function.Predicate<T> done,
      String description) {
    int attempts = isDynamoMode() ? 400 : 20;
    long sleepMs = isDynamoMode() ? 50L : 0L;
    runMaintenance();
    T value = tryGetValue(supplier);
    for (int attempt = 0; attempt < attempts; attempt++) {
      if (value != null && done.test(value)) {
        return value;
      }
      if (attempt + 1 < attempts) {
        try {
          Thread.sleep(sleepMs);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException("Interrupted while waiting for " + description, ie);
        }
      }
      runMaintenance();
      value = tryGetValue(supplier);
    }
    assertTrue(
        value != null && done.test(value),
        "Timed out waiting for " + description + "; last value=" + describeValue(value));
    return value;
  }

  private <T> T tryGetValue(java.util.function.Supplier<T> supplier) {
    try {
      return supplier.get();
    } catch (IllegalStateException | java.util.NoSuchElementException e) {
      return null;
    }
  }

  private void runMaintenance() {
    store.runMaintenanceOnce(isDynamoMode() ? 10L : 50L);
  }

  private static void awaitNextMillis(long previousMs) {
    long now = System.currentTimeMillis();
    int spins = 0;
    while (now <= previousMs) {
      Thread.onSpinWait();
      now = System.currentTimeMillis();
      if (++spins > 10_000_000) {
        throw new IllegalStateException(
            "Timed out waiting for wall clock to advance beyond " + previousMs);
      }
    }
  }

  private static String describeValue(Object value) {
    if (value instanceof ReconcileJob job) {
      return "ReconcileJob{"
          + "jobId="
          + job.jobId
          + ", state="
          + job.state
          + ", message="
          + job.message
          + ", startedAtMs="
          + job.startedAtMs
          + ", finishedAtMs="
          + job.finishedAtMs
          + ", tablesScanned="
          + job.tablesScanned
          + ", tablesChanged="
          + job.tablesChanged
          + ", viewsScanned="
          + job.viewsScanned
          + ", viewsChanged="
          + job.viewsChanged
          + ", errors="
          + job.errors
          + ", snapshotsProcessed="
          + job.snapshotsProcessed
          + ", statsProcessed="
          + job.statsProcessed
          + ", indexesProcessed="
          + job.indexesProcessed
          + ", plannedFileGroups="
          + job.plannedFileGroups
          + ", completedFileGroups="
          + job.completedFileGroups
          + ", parentJobId="
          + job.parentJobId
          + "}";
    }
    return String.valueOf(value);
  }

  private Optional<ReconcileJobStore.LeasedJob> waitForLease(
      ReconcileJobStore.LeaseRequest request) {
    return waitForValue(
        () -> store.leaseNext(request), Optional::isPresent, "lease for request " + request);
  }

  private ReconcileReadyQueueBackend.ReadyQueueSlice readySliceForKey(String readyPointerKey) {
    if (readyPointerKey == null || readyPointerKey.isBlank()) {
      return null;
    }
    if (readyPointerKey.startsWith(Keys.reconcileReadyPointerPrefix())) {
      return new ReconcileReadyQueueBackend.ReadyQueueSlice(
          ReconcileReadyQueueStore.ReadyIndexType.GLOBAL, "");
    }
    String filterValue =
        readyFilterValue(readyPointerKey, Keys.reconcileReadyByExecutionClassPointerPrefix());
    if (filterValue != null) {
      return new ReconcileReadyQueueBackend.ReadyQueueSlice(
          ReconcileReadyQueueStore.ReadyIndexType.EXECUTION_CLASS, filterValue);
    }
    filterValue =
        readyFilterValue(readyPointerKey, Keys.reconcileReadyByExecutionLanePointerPrefix());
    if (filterValue != null) {
      return new ReconcileReadyQueueBackend.ReadyQueueSlice(
          ReconcileReadyQueueStore.ReadyIndexType.EXECUTION_LANE, filterValue);
    }
    filterValue =
        readyFilterValue(readyPointerKey, Keys.reconcileReadyByPinnedExecutorPointerPrefix());
    if (filterValue != null) {
      return new ReconcileReadyQueueBackend.ReadyQueueSlice(
          ReconcileReadyQueueStore.ReadyIndexType.PINNED_EXECUTOR, filterValue);
    }
    filterValue = readyFilterValue(readyPointerKey, Keys.reconcileReadyByJobKindPointerPrefix());
    if (filterValue != null) {
      return new ReconcileReadyQueueBackend.ReadyQueueSlice(
          ReconcileReadyQueueStore.ReadyIndexType.JOB_KIND, filterValue);
    }
    return null;
  }

  private String readyFilterValue(String readyPointerKey, String prefix) {
    if (!readyPointerKey.startsWith(prefix)) {
      return null;
    }
    int slash = readyPointerKey.indexOf('/', prefix.length());
    if (slash < 0) {
      return null;
    }
    return URLDecoder.decode(
        readyPointerKey.substring(prefix.length(), slash), StandardCharsets.UTF_8);
  }

  private ReconcileLeaseStore leaseManager() throws Exception {
    return (ReconcileLeaseStore) invokePrivateMethod("leaseManager", new Class<?>[] {});
  }

  private ReconcileReadyQueueStore readyQueue() throws Exception {
    return (ReconcileReadyQueueStore) invokePrivateMethod("readyQueue", new Class<?>[] {});
  }

  private ReconcileJobProjectionStore projectionStore() {
    return (ReconcileJobProjectionStore)
        assertDoesNotThrow(() -> invokePrivateMethod("projections", new Class<?>[] {}));
  }

  private Optional<Pointer> firstPointerWithPrefix(String prefix) {
    StringBuilder next = new StringBuilder();
    List<Pointer> pointers = store.pointerStore.listPointersByPrefix(prefix, 10, "", next);
    return pointers.stream().findFirst();
  }

  private ReconcileJobStore.LeasedJob leaseJob(String jobId) {
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    StoredReconcileJob readyRecord =
        waitForValue(
            () -> readStoredRecord(canonicalPointerKey),
            current ->
                "JS_QUEUED".equals(current.state)
                    && current.readyPointerKey != null
                    && !current.readyPointerKey.isBlank(),
            "job " + jobId + " to become ready for leasing");
    return leaseSpecificReadyJob(jobId, readyRecord.jobKind());
  }

  private ReconcileJobStore.LeasedJob leaseJob(String jobId, ReconcileJobKind jobKind) {
    return leaseSpecificReadyJob(jobId, jobKind);
  }

  private ReconcileJobStore.LeasedJob leaseSpecificReadyJob(
      String jobId, ReconcileJobKind jobKind) {
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    Optional<ReconcileJobStore.LeasedJob> leased = Optional.empty();
    for (int attempt = 0; attempt < 100 && leased.isEmpty(); attempt++) {
      StoredReconcileJob readyRecord =
          waitForValue(
              () -> readStoredRecord(canonicalPointerKey),
              current ->
                  jobKind.equals(current.jobKind())
                      && "JS_QUEUED".equals(current.state)
                      && current.readyPointerKey != null
                      && !current.readyPointerKey.isBlank()
                      && readyEntryExists(current.readyPointerKey)
                      && !leaseEntryExists(ACCOUNT_ID, jobId)
                      && !leaseOwnerEntryExists(
                          Keys.reconcileLaneLeasePointer(ACCOUNT_ID, current.laneKey)),
              "job " + jobId + " to become ready for leasing");
      leased =
          assertDoesNotThrow(
              () ->
                  leaseManager()
                      .leaseCanonical(
                          canonicalPointerKey,
                          readyRecord.readyPointerKey,
                          System.currentTimeMillis(),
                          store
                              .jobIndexStore
                              .loadCanonicalSnapshot(canonicalPointerKey)
                              .orElseThrow(),
                          readyRecord));
      if (leased.isEmpty()) {
        runMaintenance();
      }
    }
    return leased.orElseThrow(
        () -> new IllegalStateException("Unable to lease ready job " + jobId));
  }

  private StoredReconcileJob readStoredRecord(String canonicalPointerKey) {
    return assertDoesNotThrow(
            () -> store.jobIndexStore.readCanonicalRecordByKey(canonicalPointerKey))
        .orElseThrow();
  }

  private ai.floedb.floecat.service.reconciler.jobs.durable.store.JobIndexEntrySnapshot
      readIndexEntry(String pointerKey) {
    return assertDoesNotThrow(() -> store.jobIndexBackend.loadIndexEntry(pointerKey)).orElseThrow();
  }

  private StoredJobLease readStoredLease(String accountId, String jobId) {
    return assertDoesNotThrow(() -> store.leaseStore.loadLease(accountId, jobId)).orElseThrow();
  }

  private boolean leaseEntryExists(String accountId, String jobId) {
    return assertDoesNotThrow(() -> store.leaseBackend.loadLease(accountId, jobId)).isPresent();
  }

  private boolean leaseExpiryEntryExists(String leaseExpiryKey) {
    return assertDoesNotThrow(() -> store.leaseBackend.loadLeaseExpiry(leaseExpiryKey)).isPresent();
  }

  private boolean leaseOwnerEntryExists(String ownerKey) {
    return assertDoesNotThrow(() -> store.leaseBackend.loadOwner(ownerKey)).isPresent();
  }

  private com.fasterxml.jackson.databind.JsonNode readBlobJson(String blobUri) {
    return assertDoesNotThrow(() -> store.mapper.readTree(store.blobStore.get(blobUri)));
  }

  private static String leaseExpiryPointerKey(long expiresAtMs, String accountId, String jobId) {
    return String.format(
        "%s%019d/accounts/%s/jobs/%s",
        LEASE_EXPIRY_POINTER_PREFIX,
        expiresAtMs,
        Keys.encodeSegment(accountId),
        Keys.encodeSegment(jobId));
  }

  private void overwriteCanonicalRecordWithoutSync(
      String canonicalPointerKey, StoredReconcileJob record) {
    var currentSnapshot =
        assertDoesNotThrow(() -> store.jobIndexStore.loadCanonicalSnapshot(canonicalPointerKey))
            .orElseThrow();
    String blobUri =
        "inline:reconcile-job:"
            + java.util.Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(assertDoesNotThrow(() -> store.mapper.writeValueAsBytes(record)));
    var writeBatch =
        new ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexStore
            .JobIndexWriteBatch(
            List.of(
                new ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexStore
                    .JobIndexUpsert(canonicalPointerKey, currentSnapshot.version(), blobUri)),
            ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexStore
                .ReadyQueueMutation.empty());
    assertTrue(
        assertDoesNotThrow(() -> store.jobIndexBackend.compareAndSetBatch(writeBatch)),
        () -> "expected canonical record to exist for " + canonicalPointerKey);
  }

  private static final class PrefixFailingPointerStore extends InMemoryPointerStore {
    private final String failingPrefix;
    private final AtomicInteger remainingFailures;

    private PrefixFailingPointerStore(String failingPrefix, int failures) {
      this.failingPrefix = failingPrefix;
      this.remainingFailures = new AtomicInteger(failures);
    }

    @Override
    public boolean compareAndSet(String key, long expectedVersion, Pointer next) {
      if (key != null
          && key.startsWith(failingPrefix)
          && remainingFailures.getAndUpdate(current -> current > 0 ? current - 1 : 0) > 0) {
        return false;
      }
      return super.compareAndSet(key, expectedVersion, next);
    }

    @Override
    public boolean compareAndSetBatch(List<CasOp> ops) {
      if (ops != null
          && ops.stream()
              .anyMatch(
                  op -> op instanceof CasUpsert upsert && upsert.key().startsWith(failingPrefix))
          && remainingFailures.getAndUpdate(current -> current > 0 ? current - 1 : 0) > 0) {
        return false;
      }
      return super.compareAndSetBatch(ops);
    }
  }

  private static final class NthPrefixFailingPointerStore extends InMemoryPointerStore {
    private final String failingPrefix;
    private final AtomicInteger invocation = new AtomicInteger();
    private final int failAtInvocation;

    private NthPrefixFailingPointerStore(String failingPrefix, int failAtInvocation) {
      this.failingPrefix = failingPrefix;
      this.failAtInvocation = failAtInvocation;
    }

    @Override
    public boolean compareAndSet(String key, long expectedVersion, Pointer next) {
      if (key != null
          && key.startsWith(failingPrefix)
          && invocation.incrementAndGet() == failAtInvocation) {
        return false;
      }
      return super.compareAndSet(key, expectedVersion, next);
    }

    @Override
    public boolean compareAndSetBatch(List<CasOp> ops) {
      if (ops != null
          && ops.stream()
              .anyMatch(
                  op -> op instanceof CasUpsert upsert && upsert.key().startsWith(failingPrefix))
          && invocation.incrementAndGet() == failAtInvocation) {
        return false;
      }
      return super.compareAndSetBatch(ops);
    }
  }

  private static final class NthAndSubsequentPrefixFailingPointerStore
      extends InMemoryPointerStore {
    private final String failingPrefix;
    private final AtomicInteger invocation = new AtomicInteger();
    private final int failAtInvocation;

    private NthAndSubsequentPrefixFailingPointerStore(String failingPrefix, int failAtInvocation) {
      this.failingPrefix = failingPrefix;
      this.failAtInvocation = failAtInvocation;
    }

    @Override
    public boolean compareAndSetBatch(List<CasOp> ops) {
      if (ops != null
          && ops.stream()
              .anyMatch(
                  op -> op instanceof CasUpsert upsert && upsert.key().startsWith(failingPrefix))
          && invocation.incrementAndGet() >= failAtInvocation) {
        return false;
      }
      return super.compareAndSetBatch(ops);
    }
  }

  private static final class BatchFailingPointerStore extends InMemoryPointerStore {
    private final String failingPrefix;
    private final AtomicInteger remainingFailures;

    private BatchFailingPointerStore(String failingPrefix, int failures) {
      this.failingPrefix = failingPrefix;
      this.remainingFailures = new AtomicInteger(failures);
    }

    @Override
    public boolean compareAndSetBatch(List<CasOp> ops) {
      if (ops != null
          && ops.stream()
              .anyMatch(
                  op -> op instanceof CasUpsert upsert && upsert.key().startsWith(failingPrefix))
          && remainingFailures.getAndUpdate(current -> current > 0 ? current - 1 : 0) > 0) {
        return false;
      }
      return super.compareAndSetBatch(ops);
    }
  }

  private List<ReconcileJobStore.ReconcileJob> listChildJobs(String accountId, String parentJobId) {
    java.util.ArrayList<ReconcileJobStore.ReconcileJob> out = new java.util.ArrayList<>();
    String nextToken = "";
    do {
      ReconcileJobStore.ReconcileJobPage page =
          store.childJobsPage(accountId, parentJobId, 200, nextToken);
      out.addAll(page.jobs);
      nextToken = page.nextPageToken;
    } while (nextToken != null && !nextToken.isBlank());
    return List.copyOf(out);
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
                .key(Map.of("pk", item.get("pk"), "sk", item.get("sk")))
                .build());
      }
      startKey = response.lastEvaluatedKey();
    } while (startKey != null && !startKey.isEmpty());
  }

  private static final class TrackingBlobStore extends InMemoryBlobStore {
    private final java.util.Set<String> active = java.util.concurrent.ConcurrentHashMap.newKeySet();

    @Override
    public void put(String uri, byte[] bytes, String contentType) {
      super.put(uri, bytes, contentType);
      active.add(uri);
    }

    @Override
    public boolean delete(String uri) {
      boolean deleted = super.delete(uri);
      active.remove(uri);
      return deleted;
    }

    int activeBlobCount() {
      return active.size();
    }
  }
}
