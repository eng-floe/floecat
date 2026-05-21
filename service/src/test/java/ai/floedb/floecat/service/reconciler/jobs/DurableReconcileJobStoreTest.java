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
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.errors.StorageNotFoundException;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore.CasOp;
import ai.floedb.floecat.storage.spi.PointerStore.CasUpsert;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DurableReconcileJobStoreTest {
  private static final String ACCOUNT_ID = "acct-1";
  private static final String CONNECTOR_ID = "conn-1";
  private static final String CATALOG = "cat-1";
  private static final List<String> NAMESPACE_PATH = List.of("ns");
  private static final String LEASE_EXPIRY_POINTER_PREFIX =
      "/accounts/by-id/reconcile/job-leases/by-expiry/";
  private static final String INLINE_JOB_LEASE_PREFIX = "inline:reconcile-lease:";
  private static final String INLINE_JOB_CONTRIBUTION_PREFIX = "inline:reconcile-contribution:";

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
    System.clearProperty("floecat.reconciler.job-store.lease-renew-grace-ms");
    System.clearProperty("floecat.reconciler.job-store.reclaim-interval-ms");
    System.clearProperty("floecat.reconciler.job-store.ready-scan-limit");
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
  void childJobsPageReturnsDetailedExecFileGroupTasks() {
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
    assertTrue(page.nextPageToken.isBlank());
  }

  @Test
  void enqueueStoresOnlyDedupeKeyHashOnCanonicalRecord() {
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

    DurableReconcileJobStore.StoredReconcileJob record =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, jobId));

    assertFalse(record.dedupeKeyHash.isBlank());
    assertEquals(
        Keys.reconcileDedupePointer(ACCOUNT_ID, record.dedupeKeyHash),
        firstPointerWithPrefix(Keys.reconcileDedupePointerPrefix(ACCOUNT_ID))
            .orElseThrow()
            .getKey());
  }

  @Test
  void enqueueLookupFailureCleansCanonicalRows() {
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
      assertEquals(
          canonicalKey, store.pointerStore.get(record.readyPointerKey).orElseThrow().getBlobUri());
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
    DurableReconcileJobStore.StoredReconcileJob stored =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, result.items.get(0).jobId));
    assertTrue(store.pointerStore.get(stored.readyPointerKey).isPresent());
    assertEquals(
        1,
        store.pointerStore.countByPrefix(
            Keys.reconcileJobByConnectorPointerPrefix(ACCOUNT_ID, CONNECTOR_ID)));
    assertEquals(1, listChildJobs(ACCOUNT_ID, "parent-1").size());
    assertEquals(
        2, blobStore.activeBlobCount(), "only the successful job payload blobs should remain");
  }

  @Test
  void bulkEnqueueRollbackDeletesFileGroupResultBlob() {
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
    assertEquals(2, blobStore.activeBlobCount(), "dedupe hit should clean up prewritten payloads");
  }

  @Test
  void bulkEnqueueAccumulatesExpectedChildJobsOncePerParentBatch() {
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
                fileGroupSpec(parentJobId, "group-3", "s3://bucket/table-1/file-3.parquet")));

    result.requireAllSucceeded("bulk expected child count");

    DurableReconcileJobStore.StoredReconcileJob parent =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, parentJobId));
    assertEquals(3L, parent.expectedChildJobs);
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

    DurableReconcileJobStore.StoredReconcileJob first =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, result.items.get(0).jobId));
    DurableReconcileJobStore.StoredReconcileJob second =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, result.items.get(1).jobId));

    assertEquals("table-source|db|orders", first.laneKey);
    assertEquals("table-source|db|customers", second.laneKey);
  }

  @Test
  void bulkEnqueueDefersLookupRepairOnHotPathUntilMaintenanceFlow() {
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
        "duplicate enqueue should not repair lookup inline on the hot path");

    var leased = store.leaseNext().orElseThrow();

    assertEquals(jobId, leased.jobId);
    assertEquals(canonicalKey, store.pointerStore.get(lookupKey).orElseThrow().getBlobUri());
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

    store.persistSnapshotPlan(
        jobId,
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
            true));

    var job = store.get(ACCOUNT_ID, jobId).orElseThrow();
    assertEquals(1, job.snapshotTask.fileGroups().size());
    assertEquals(
        "s3://bucket/data/file-1.parquet",
        job.snapshotTask.fileGroups().getFirst().filePaths().getFirst());
  }

  @Test
  void persistSnapshotPlanRejectsLegacyPlannerSnapshotBlob() throws Exception {
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
        "/accounts/acct-1/reconcile/jobs/" + jobId + "/snapshot-plan/planner.json";
    store.blobStore.put(
        plannerBlobUri,
        store.mapper.writeValueAsBytes(
            new SnapshotPlanBlob(
                List.of(new PlannedFileGroupJob(ReconcileScope.empty(), plannedGroup)))),
        "application/json; charset=UTF-8");

    IllegalStateException error =
        assertThrows(
            IllegalStateException.class,
            () ->
                store.persistSnapshotPlan(
                    jobId,
                    ReconcileSnapshotTask.of(
                        "table-1",
                        55L,
                        "db",
                        "events",
                        List.of(),
                        true,
                        ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
                        plannerBlobUri,
                        1)));

    assertTrue(error.getMessage().contains("snapshot plan payload"));
  }

  @Test
  void enqueueSnapshotFinalizationRejectsLegacyPlannerSnapshotBlob() throws Exception {
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
        "/accounts/acct-1/reconcile/jobs/" + parentJobId + "/snapshot-plan/finalizer-planner.json";
    store.blobStore.put(
        plannerBlobUri,
        store.mapper.writeValueAsBytes(
            new SnapshotPlanBlob(
                List.of(new PlannedFileGroupJob(ReconcileScope.empty(), plannedGroup)))),
        "application/json; charset=UTF-8");

    IllegalStateException error =
        assertThrows(
            IllegalStateException.class,
            () ->
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
                    ""));

    assertTrue(error.getMessage().contains("snapshot plan payload"));
  }

  @Test
  void getReturnsCanonicalSnapshotPlanWhenLookupPointerIsStale() {
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

    store.persistSnapshotPlan(
        jobId,
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
            true));

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
    assertEquals(
        Keys.reconcileJobStateRowById(ACCOUNT_ID, jobId),
        store.pointerStore.get(lookupKey).orElseThrow().getBlobUri());
  }

  @Test
  void childJobsReturnsCanonicalChildWhenParentPointerIsStale() {
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
    assertTrue(store.leaseNext().isEmpty());
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

    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                store.persistSnapshotPlan(
                    jobId,
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
                                List.of("s3://bucket/data/file-1.parquet"))))));

    assertTrue(error.getMessage().contains("persistSnapshotPlan"));
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

    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () -> store.persistSnapshotPlan(jobId, ReconcileSnapshotTask.empty()));

    assertTrue(error.getMessage().contains("persistSnapshotPlan"));
  }

  @Test
  void persistSnapshotPlanDeletesPreviousBlob() {
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

    store.persistSnapshotPlan(
        jobId,
        ReconcileSnapshotTask.of(
            "table-1",
            55L,
            "db",
            "events",
            List.of(ReconcileFileGroupTask.of("plan-1", "group-1", "table-1", 55L, List.of("f1"))),
            true));
    int afterFirstWrite = blobStore.activeBlobCount();

    store.persistSnapshotPlan(
        jobId,
        ReconcileSnapshotTask.of(
            "table-1",
            55L,
            "db",
            "events",
            List.of(ReconcileFileGroupTask.of("plan-1", "group-2", "table-1", 55L, List.of("f2"))),
            true));

    assertEquals(afterFirstWrite, blobStore.activeBlobCount());
  }

  @Test
  void persistSnapshotPlanPreservesExistingBlobForMetadataOnlyUpdate() {
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
    store.persistSnapshotPlan(
        jobId,
        ReconcileSnapshotTask.of("table-1", 55L, "db", "events", List.of(plannedGroup), true));
    int afterFirstWrite = blobStore.activeBlobCount();

    store.persistSnapshotPlan(
        jobId,
        ReconcileSnapshotTask.of(
            "table-1",
            55L,
            "db",
            "events",
            List.of(),
            true,
            ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
            "",
            1));

    var job = store.get(ACCOUNT_ID, jobId).orElseThrow();
    assertEquals(1, job.snapshotTask.fileGroups().size());
    assertEquals("group-1", job.snapshotTask.fileGroups().getFirst().groupId());
    assertEquals("f1", job.snapshotTask.fileGroups().getFirst().filePaths().getFirst());
    assertEquals(afterFirstWrite, blobStore.activeBlobCount());
  }

  @Test
  void persistSnapshotPlanDeletesNewBlobWhenMutatorThrows() {
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

    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                store.persistSnapshotPlan(
                    jobId,
                    ReconcileSnapshotTask.of(
                        "table-1",
                        55L,
                        "db",
                        "events",
                        List.of(
                            ReconcileFileGroupTask.of(
                                "plan-1", "group-1", "table-1", 55L, List.of("f1"))),
                        false)));

    assertTrue(error.getMessage().contains("persistSnapshotPlan"));
    assertEquals(before, blobStore.activeBlobCount(), "failed mutation should clean up new blob");
  }

  @Test
  void getThrowsWhenDefinitionBlobIsMissing() {
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    DurableReconcileJobStore.StoredReconcileJob record =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, jobId));

    assertTrue(store.blobStore.delete(record.definitionBlobUri));

    IllegalStateException error =
        assertThrows(IllegalStateException.class, () -> store.get(ACCOUNT_ID, jobId));
    assertTrue(error.getMessage().contains("job definition"));
  }

  @Test
  void leaseSnapshotPlanRollsBackLeaseAndSnapshotLockWhenDefinitionHydrationFails() {
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
    DurableReconcileJobStore.StoredReconcileJob corrupted = readStoredRecord(canonicalPointerKey);
    corrupted.definitionBlobUri = "blob://missing-definition";
    overwriteCanonicalRecordWithoutSync(canonicalPointerKey, corrupted);

    assertThrows(
        IllegalStateException.class,
        () ->
            store.leaseNext(
                ReconcileJobStore.LeaseRequest.of(
                    Set.of(), Set.of(), Set.of(), Set.of(ReconcileJobKind.PLAN_SNAPSHOT))));

    assertTrue(
        store.pointerStore.get(Keys.reconcileJobLeasePointerById(ACCOUNT_ID, jobId)).isEmpty());
    assertTrue(
        store.pointerStore.get(Keys.reconcileSnapshotLeasePointer("table-1", 55L)).isEmpty());

    DurableReconcileJobStore.StoredReconcileJob restored = readStoredRecord(canonicalPointerKey);
    assertEquals("JS_QUEUED", restored.state);
    assertEquals("Queued", restored.message);
    assertTrue(restored.readyPointerKey != null && !restored.readyPointerKey.isBlank());
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
    DurableReconcileJobStore.StoredReconcileJob record =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, jobId));

    assertTrue(store.blobStore.delete(record.snapshotPlanBlobUri));

    IllegalStateException error =
        assertThrows(IllegalStateException.class, () -> store.get(ACCOUNT_ID, jobId));
    assertTrue(error.getMessage().contains("snapshot plan payload"));
  }

  @Test
  void getThrowsWhenFileGroupPlanBlobIsMissing() {
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
    DurableReconcileJobStore.StoredReconcileJob record =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, jobId));

    assertTrue(store.blobStore.delete(record.fileGroupPlanBlobUri));

    ReconcileJob job = store.get(ACCOUNT_ID, jobId).orElseThrow();
    assertEquals(ReconcileJobKind.EXEC_FILE_GROUP, job.jobKind);
    assertEquals(List.of(), job.fileGroupTask.filePaths());
  }

  @Test
  void upsertReferencePointerDoesNotRewriteMatchingPointer() throws Exception {
    store.init();

    String pointerKey = "/test/pointer";
    String reference = "inline:test-reference";
    assertTrue(
        (Boolean)
            invokePrivateMethod(
                "upsertReferencePointer",
                new Class<?>[] {String.class, String.class},
                pointerKey,
                reference));

    Pointer original = store.pointerStore.get(pointerKey).orElseThrow();

    assertTrue(
        (Boolean)
            invokePrivateMethod(
                "upsertReferencePointer",
                new Class<?>[] {String.class, String.class},
                pointerKey,
                reference));

    Pointer unchanged = store.pointerStore.get(pointerKey).orElseThrow();
    assertEquals(original.getVersion(), unchanged.getVersion());
    assertEquals(reference, unchanged.getBlobUri());
  }

  @Test
  void persistFileGroupResultUpdatesStoredExecFileGroupTask() {
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
    Pointer canonicalBefore =
        store.pointerStore.get(Keys.reconcileJobPointerById(ACCOUNT_ID, jobId)).orElseThrow();

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

    Pointer canonicalAfter =
        store.pointerStore.get(Keys.reconcileJobPointerById(ACCOUNT_ID, jobId)).orElseThrow();
    Pointer resultPointer =
        store.pointerStore.get(Keys.reconcileJobResultPointerById(ACCOUNT_ID, jobId)).orElseThrow();
    var job = store.get(ACCOUNT_ID, jobId).orElseThrow();
    assertEquals(canonicalBefore.getVersion(), canonicalAfter.getVersion());
    assertFalse(resultPointer.getBlobUri().isBlank());
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
    Pointer resultPointer =
        store.pointerStore.get(Keys.reconcileJobResultPointerById(ACCOUNT_ID, jobId)).orElseThrow();
    var resultJson = readBlobJson(resultPointer.getBlobUri());

    assertFalse(stateJson.has("snapshotTaskFileGroups"));
    assertFalse(stateJson.has("fileGroupPaths"));
    assertFalse(stateJson.has("fileGroupResults"));
    assertFalse(stateJson.has("fileGroupResultBlobUri"));
    assertFalse(stateJson.path("snapshotPlanBlobUri").asText().isBlank());
    assertFalse(stateJson.path("fileGroupPlanBlobUri").asText().isBlank());
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
    assertEquals(metadataJob, firstLease.jobId);

    assertTrue(store.leaseNext().isEmpty());

    store.markSucceeded(metadataJob, firstLease.leaseEpoch, System.currentTimeMillis(), 1, 1, 1, 1);

    var secondLease = store.leaseNext().orElseThrow();
    assertEquals(statsJob, secondLease.jobId);
  }

  @Test
  void markSucceededReleasesLaneSoNextQueuedSameLaneLeasesImmediately() {
    store.init();
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String firstJob =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);
    String secondJob = store.enqueue(ACCOUNT_ID, "conn-b", false, CaptureMode.CAPTURE_ONLY, scope);
    var firstLease = store.leaseNext().orElseThrow();
    assertEquals(firstJob, firstLease.jobId);
    String firstCanonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, firstLease.jobId);
    DurableReconcileJobStore.StoredReconcileJob firstRecord =
        readStoredRecord(firstCanonicalPointerKey);
    String lanePointerKey = Keys.reconcileLaneLeasePointer(ACCOUNT_ID, firstRecord.laneKey);
    assertTrue(store.pointerStore.get(lanePointerKey).isPresent());

    store.markSucceeded(firstJob, firstLease.leaseEpoch, System.currentTimeMillis(), 1, 1, 1, 1);

    assertTrue(store.pointerStore.get(lanePointerKey).isEmpty());
    var secondLease = store.leaseNext().orElseThrow();
    assertEquals(secondJob, secondLease.jobId);
  }

  @Test
  void staleLaneLeasePointerStillBlocksConcurrentLease() throws Exception {
    store.init();
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String firstJob =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, firstJob);
    var firstLease = store.leaseNext().orElseThrow();

    DurableReconcileJobStore.StoredReconcileJob currentJob = readStoredRecord(canonicalPointerKey);
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
    store.init();
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String firstJob =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);
    String firstCanonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, firstJob);
    var firstLease = store.leaseNext().orElseThrow();
    store.markSucceeded(firstJob, firstLease.leaseEpoch, System.currentTimeMillis(), 1, 1, 1, 1);

    DurableReconcileJobStore.StoredReconcileJob terminalRecord =
        readStoredRecord(firstCanonicalPointerKey);
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
    store.init();
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String jobId =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    Pointer initialCanonical = store.pointerStore.get(canonicalPointerKey).orElseThrow();

    store.leaseNext().orElseThrow();

    Pointer currentCanonical = store.pointerStore.get(canonicalPointerKey).orElseThrow();
    assertTrue(currentCanonical.getVersion() > initialCanonical.getVersion());

    DurableReconcileJobStore.StoredReconcileJob activeRecord =
        readStoredRecord(canonicalPointerKey);

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

    Method clearLaneLeaseIfOwned =
        DurableReconcileJobStore.class.getDeclaredMethod(
            "clearLaneLeaseIfOwned",
            DurableReconcileJobStore.StoredReconcileJob.class,
            String.class);
    clearLaneLeaseIfOwned.setAccessible(true);

    assertDoesNotThrow(
        () -> clearLaneLeaseIfOwned.invoke(store, activeRecord, canonicalPointerKey));

    Pointer repairedPointer = store.pointerStore.get(lanePointerKey).orElseThrow();
    assertEquals(canonicalPointerKey, repairedPointer.getBlobUri());
  }

  @Test
  void tryAcquireLaneLeaseReacquiresStaleSelfOwnedLanePointer() throws Exception {
    store.init();
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String jobId =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    DurableReconcileJobStore.StoredReconcileJob queuedRecord =
        readStoredRecord(canonicalPointerKey);

    Method tryAcquireLaneLease =
        DurableReconcileJobStore.class.getDeclaredMethod(
            "tryAcquireLaneLease",
            DurableReconcileJobStore.StoredReconcileJob.class,
            String.class,
            long.class);
    tryAcquireLaneLease.setAccessible(true);

    boolean firstClaim =
        (boolean)
            tryAcquireLaneLease.invoke(
                store, queuedRecord, canonicalPointerKey, System.currentTimeMillis());
    Pointer staleLanePointer =
        store
            .pointerStore
            .get(Keys.reconcileLaneLeasePointer(ACCOUNT_ID, queuedRecord.laneKey))
            .orElseThrow();
    boolean secondClaim =
        (boolean)
            tryAcquireLaneLease.invoke(
                store, queuedRecord, canonicalPointerKey, System.currentTimeMillis());

    assertTrue(firstClaim);
    assertTrue(secondClaim);
    Pointer repairedLanePointer =
        store
            .pointerStore
            .get(Keys.reconcileLaneLeasePointer(ACCOUNT_ID, queuedRecord.laneKey))
            .orElseThrow();
    assertEquals(staleLanePointer.getVersion(), repairedLanePointer.getVersion());
    assertEquals(canonicalPointerKey, repairedLanePointer.getBlobUri());
  }

  @Test
  void tryAcquireLaneLeaseCanonicalizesStaleSameJobPointerWithoutSelfAuthorizing()
      throws Exception {
    store.init();
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String jobId =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    DurableReconcileJobStore.StoredReconcileJob queuedRecord =
        readStoredRecord(canonicalPointerKey);
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

    Method tryAcquireLaneLease =
        DurableReconcileJobStore.class.getDeclaredMethod(
            "tryAcquireLaneLease",
            DurableReconcileJobStore.StoredReconcileJob.class,
            String.class,
            long.class);
    tryAcquireLaneLease.setAccessible(true);

    boolean acquired =
        (boolean)
            tryAcquireLaneLease.invoke(
                store, queuedRecord, canonicalPointerKey, System.currentTimeMillis());

    assertTrue(acquired);
    assertEquals(
        canonicalPointerKey, store.pointerStore.get(lanePointerKey).orElseThrow().getBlobUri());
  }

  @Test
  void tryAcquireLaneLeaseTreatsQueuedButLeasedOwnerAsActive() throws Exception {
    System.setProperty("floecat.reconciler.job-store.lease-ms", "60000");
    store.init();
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String firstJob =
        store.enqueue(ACCOUNT_ID, "conn-a", false, CaptureMode.METADATA_AND_CAPTURE, scope);
    String secondJob =
        store.enqueue(ACCOUNT_ID, "conn-b", false, CaptureMode.METADATA_AND_CAPTURE, scope);

    var firstLease = store.leaseNext().orElseThrow();
    String firstCanonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, firstJob);
    DurableReconcileJobStore.StoredReconcileJob firstRecord =
        readStoredRecord(firstCanonicalPointerKey);
    firstRecord.state = "JS_QUEUED";
    overwriteCanonicalRecordWithoutSync(firstCanonicalPointerKey, firstRecord);

    String secondCanonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, secondJob);
    DurableReconcileJobStore.StoredReconcileJob secondRecord =
        readStoredRecord(secondCanonicalPointerKey);

    Method tryAcquireLaneLease =
        DurableReconcileJobStore.class.getDeclaredMethod(
            "tryAcquireLaneLease",
            DurableReconcileJobStore.StoredReconcileJob.class,
            String.class,
            long.class);
    tryAcquireLaneLease.setAccessible(true);

    boolean acquired =
        (boolean)
            tryAcquireLaneLease.invoke(
                store, secondRecord, secondCanonicalPointerKey, System.currentTimeMillis());

    assertFalse(acquired);
    assertEquals(
        firstCanonicalPointerKey,
        store
            .pointerStore
            .get(Keys.reconcileLaneLeasePointer(ACCOUNT_ID, secondRecord.laneKey))
            .orElseThrow()
            .getBlobUri());
    assertTrue(store.renewLease(firstJob, firstLease.leaseEpoch));
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
    assertEquals(firstJob, firstLease.jobId);

    assertTrue(store.leaseNext().isEmpty());

    store.markSucceeded(firstJob, firstLease.leaseEpoch, System.currentTimeMillis(), 1, 1, 1, 1);

    var secondLease = store.leaseNext().orElseThrow();
    assertEquals(secondJob, secondLease.jobId);
  }

  @Test
  void staleSnapshotLeasePointerStillBlocksConcurrentSnapshotLease() throws Exception {
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

    DurableReconcileJobStore.StoredReconcileJob activeRecord =
        readStoredRecord(canonicalPointerKey);

    Method clearSnapshotLeaseIfOwned =
        DurableReconcileJobStore.class.getDeclaredMethod(
            "clearSnapshotLeaseIfOwned",
            DurableReconcileJobStore.StoredReconcileJob.class,
            String.class);
    clearSnapshotLeaseIfOwned.setAccessible(true);

    assertDoesNotThrow(
        () -> clearSnapshotLeaseIfOwned.invoke(store, activeRecord, canonicalPointerKey));

    Pointer repairedPointer = store.pointerStore.get(snapshotLeasePointerKey).orElseThrow();
    assertEquals(canonicalPointerKey, repairedPointer.getBlobUri());
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
    store.init();
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String jobId =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);
    DurableReconcileJobStore.StoredReconcileJob queuedRecord =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, jobId));
    String lanePointerKey = Keys.reconcileLaneLeasePointer(ACCOUNT_ID, queuedRecord.laneKey);

    Pointer dedupePointer =
        firstPointerWithPrefix(Keys.reconcileDedupePointerPrefix(ACCOUNT_ID)).orElseThrow();
    var lease = store.leaseNext().orElseThrow();
    store.markSucceeded(jobId, lease.leaseEpoch, System.currentTimeMillis(), 1, 1, 1, 1);

    assertTrue(store.pointerStore.get(dedupePointer.getKey()).isEmpty());
    assertEquals(0, store.pointerStore.countByPrefix(Keys.reconcileReadyPointerPrefix()));
    assertTrue(store.pointerStore.get(lanePointerKey).isEmpty());
  }

  @Test
  void enqueueRepairsMissingSecondaryPointersForDedupedQueuedJob() {
    store.init();
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String jobId =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    DurableReconcileJobStore.StoredReconcileJob job = readStoredRecord(canonicalPointerKey);
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
    assertTrue(store.pointerStore.get(expectedReadyKey).isEmpty());
    Pointer repairedDedupePointer =
        firstPointerWithPrefix(Keys.reconcileDedupePointerPrefix(ACCOUNT_ID)).orElseThrow();
    assertEquals(canonicalPointerKey, repairedDedupePointer.getBlobUri());
    DurableReconcileJobStore.StoredReconcileJob unrepairedJob =
        readStoredRecord(canonicalPointerKey);
    assertTrue(unrepairedJob.readyPointerKey == null || unrepairedJob.readyPointerKey.isBlank());

    assertTrue(store.leaseNext().isEmpty());
    store.runMaintenanceOnce(1_000L);
    var lease = store.leaseNext().orElseThrow();
    assertEquals(jobId, lease.jobId);
    assertTrue(store.pointerStore.get(lookupKey).isPresent());
    assertTrue(store.pointerStore.get(expectedReadyKey).isEmpty());
  }

  @Test
  void leaseNextRepairsMissingLookupPointerFromReadyReference() {
    store.init();
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String jobId =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);
    String lookupKey = Keys.reconcileJobLookupPointerById(jobId);
    Pointer lookupPointer = store.pointerStore.get(lookupKey).orElseThrow();
    assertTrue(store.pointerStore.compareAndDelete(lookupKey, lookupPointer.getVersion()));

    var lease = store.leaseNext().orElseThrow();

    assertEquals(jobId, lease.jobId);
    assertEquals(
        Keys.reconcileJobStateRowById(ACCOUNT_ID, jobId),
        store.pointerStore.get(lookupKey).orElseThrow().getBlobUri());
  }

  @Test
  void leaseNextAtomicallyWritesLeaseRowAndExpiryIndex() {
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
    DurableReconcileJobStore.StoredJobLease storedLease =
        readStoredLease(leasePointer.getBlobUri());
    String expiryKey = leaseExpiryPointerKey(storedLease.expiresAtMs, ACCOUNT_ID, jobId);

    assertEquals(lease.leaseEpoch, storedLease.epoch);
    assertEquals(
        Keys.reconcileJobPointerById(ACCOUNT_ID, jobId),
        store.pointerStore.get(expiryKey).orElseThrow().getBlobUri());
  }

  @Test
  void renewLeaseMovesExpiryIndexAtomically() throws Exception {
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
    DurableReconcileJobStore.StoredJobLease beforeRenew =
        readStoredLease(store.pointerStore.get(leaseKey).orElseThrow().getBlobUri());
    String oldExpiryKey = leaseExpiryPointerKey(beforeRenew.expiresAtMs, ACCOUNT_ID, jobId);
    Thread.sleep(650L);

    assertTrue(store.renewLease(jobId, lease.leaseEpoch));

    DurableReconcileJobStore.StoredJobLease afterRenew =
        readStoredLease(store.pointerStore.get(leaseKey).orElseThrow().getBlobUri());
    assertTrue(afterRenew.expiresAtMs > beforeRenew.expiresAtMs);
    String newExpiryKey = leaseExpiryPointerKey(afterRenew.expiresAtMs, ACCOUNT_ID, jobId);
    assertNotEquals(oldExpiryKey, newExpiryKey);
    assertTrue(store.pointerStore.get(oldExpiryKey).isEmpty());
    assertEquals(
        Keys.reconcileJobPointerById(ACCOUNT_ID, jobId),
        store.pointerStore.get(newExpiryKey).orElseThrow().getBlobUri());
  }

  @Test
  void renewLeaseNoOpDoesNotRewriteLeaseOrExpiryIndex() {
    System.setProperty("floecat.reconciler.job-store.lease-ms", "60000");
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
    DurableReconcileJobStore.StoredJobLease beforeLease =
        readStoredLease(beforeLeasePointer.getBlobUri());
    String expiryKey = leaseExpiryPointerKey(beforeLease.expiresAtMs, ACCOUNT_ID, jobId);
    Pointer beforeExpiryPointer = store.pointerStore.get(expiryKey).orElseThrow();

    assertTrue(store.renewLease(jobId, lease.leaseEpoch));

    Pointer afterLeasePointer = store.pointerStore.get(leaseKey).orElseThrow();
    Pointer afterExpiryPointer = store.pointerStore.get(expiryKey).orElseThrow();
    assertEquals(beforeLeasePointer.getVersion(), afterLeasePointer.getVersion());
    assertEquals(beforeLeasePointer.getBlobUri(), afterLeasePointer.getBlobUri());
    assertEquals(beforeExpiryPointer.getVersion(), afterExpiryPointer.getVersion());
    assertEquals(beforeExpiryPointer.getBlobUri(), afterExpiryPointer.getBlobUri());
  }

  @Test
  void nonTerminalMutationDoesNotRepairMissingDedupePointer() {
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

    DurableReconcileJobStore.StoredReconcileJob running = readStoredRecord(canonicalPointerKey);
    running.readyPointerKey = "";
    overwriteCanonicalRecordWithoutSync(canonicalPointerKey, running);

    Thread.sleep(1150L);

    assertTrue(
        store
            .leaseNext(
                ReconcileJobStore.LeaseRequest.of(
                    java.util.EnumSet.of(ReconcileExecutionClass.HEAVY),
                    java.util.Set.of("remote")))
            .isEmpty());
    store.runMaintenanceOnce(1_000L);

    DurableReconcileJobStore.StoredReconcileJob requeued = readStoredRecord(canonicalPointerKey);
    assertEquals("JS_QUEUED", requeued.state);
    assertFalse(requeued.readyPointerKey.isBlank());
    assertTrue(store.pointerStore.get(requeued.readyPointerKey).isPresent());
  }

  @Test
  void maintenanceReclaimUsesLeaseExpiryIndexWithoutGlobalCanonicalScan() throws Exception {
    TrackingPointerStore pointerStore = new TrackingPointerStore();
    store.pointerStore = pointerStore;
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
    pointerStore.resetListCounts();

    Thread.sleep(1150L);

    store.runMaintenanceOnce(1_000L);
    assertTrue(store.leaseNext().isPresent());
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
    DurableReconcileJobStore.StoredReconcileJob queuedRecord =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, jobId));
    String lanePointerKey = Keys.reconcileLaneLeasePointer(ACCOUNT_ID, queuedRecord.laneKey);
    var firstLease = store.leaseNext().orElseThrow();

    store.markFailed(
        jobId, firstLease.leaseEpoch, System.currentTimeMillis(), "transient", 1, 0, 1, 2, 3);
    ReconcileJob retried = store.get(jobId).orElseThrow();
    assertEquals("JS_QUEUED", retried.state);
    assertTrue(store.pointerStore.get(lanePointerKey).isEmpty());

    Thread.sleep(120L);
    var secondLease = store.leaseNext().orElseThrow();
    store.markFailed(
        jobId, secondLease.leaseEpoch, System.currentTimeMillis(), "terminal", 1, 0, 2, 2, 3);
    ReconcileJob failed = store.get(jobId).orElseThrow();
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
    DurableReconcileJobStore.StoredReconcileJob queuedRecord =
        readStoredRecord(canonicalPointerKey);
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
    store.init();
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String jobId =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    DurableReconcileJobStore.StoredReconcileJob queuedRecord =
        readStoredRecord(canonicalPointerKey);
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
    Thread.sleep(5L);
    String second =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "second"));
    Thread.sleep(5L);
    String third =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "third"));

    var firstPage = store.list(ACCOUNT_ID, 2, "", CONNECTOR_ID, java.util.Set.of());
    assertEquals(List.of(third, second), firstPage.jobs.stream().map(job -> job.jobId).toList());
    assertFalse(firstPage.nextPageToken.isBlank());

    var secondPage =
        store.list(ACCOUNT_ID, 2, firstPage.nextPageToken, CONNECTOR_ID, java.util.Set.of());
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

    var page = store.list(ACCOUNT_ID, 10, "", CONNECTOR_ID, java.util.Set.of());

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

    var page = store.list(ACCOUNT_ID, 10, "", CONNECTOR_ID, java.util.Set.of("JS_SUCCEEDED"));

    assertEquals(List.of(succeededJobId), page.jobs.stream().map(job -> job.jobId).toList());
    assertTrue(page.jobs.stream().noneMatch(job -> queuedJobId.equals(job.jobId)));
  }

  @Test
  void listByConnectorIgnoresStaleConnectorIndexPointers() {
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
    assertTrue(store.pointerStore.get(staleIndexKey).isEmpty());
  }

  @Test
  void listByConnectorStateFilteringPaginatesAcrossIndexPages() {
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

    var page = store.list(ACCOUNT_ID, 1, "", CONNECTOR_ID, java.util.Set.of("JS_SUCCEEDED"));

    assertEquals(List.of(succeededJobId), page.jobs.stream().map(job -> job.jobId).toList());
    assertEquals("", page.nextPageToken);
  }

  @Test
  void listByConnectorMultiStateFilteringPaginatesAcrossStates() {
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
        store.list(ACCOUNT_ID, 1, "", CONNECTOR_ID, java.util.Set.of("JS_SUCCEEDED", "JS_QUEUED"));
    assertEquals(1, firstPage.jobs.size());
    assertFalse(firstPage.nextPageToken.isBlank());

    var secondPage =
        store.list(
            ACCOUNT_ID,
            1,
            firstPage.nextPageToken,
            CONNECTOR_ID,
            java.util.Set.of("JS_SUCCEEDED", "JS_QUEUED"));

    assertEquals(
        java.util.Set.of(queuedJobId, succeededJobId),
        java.util.stream.Stream.concat(firstPage.jobs.stream(), secondPage.jobs.stream())
            .map(job -> job.jobId)
            .collect(java.util.stream.Collectors.toSet()));
    assertTrue(secondPage.nextPageToken.isBlank());
  }

  @Test
  void listByConnectorDoesNotBackfillCanonicalOnlyJobs() {
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
    DurableReconcileJobStore.StoredReconcileJob canonicalOnlyRecord =
        readStoredRecord(canonicalOnlyKey);
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
    System.setProperty("floecat.reconciler.job-store.lease-ms", "1000");
    System.setProperty("floecat.reconciler.job-store.reclaim-interval-ms", "1000");
    System.setProperty("floecat.reconciler.job-store.lease-renew-grace-ms", "0");
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

    Thread.sleep(1150L);

    store.runMaintenanceOnce(1_000L);
    var secondLease = store.leaseNext();
    assertTrue(secondLease.isPresent());
    assertEquals(jobId, secondLease.get().jobId);
  }

  @Test
  void leaseNextRepairsLookupPointerWhenReadyPointerStillExists() {
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
    Pointer canonicalPointer = store.pointerStore.get(canonicalPointerKey).orElseThrow();
    DurableReconcileJobStore.StoredReconcileJob record = readStoredRecord(canonicalPointerKey);
    String lookupKey = Keys.reconcileJobLookupPointerById(jobId);
    Pointer lookupPointer = store.pointerStore.get(lookupKey).orElseThrow();
    assertTrue(store.pointerStore.compareAndDelete(lookupKey, lookupPointer.getVersion()));

    var repairedLease = store.leaseNext().orElseThrow();

    assertEquals(jobId, repairedLease.jobId);
    assertTrue(store.pointerStore.get(lookupKey).isPresent());
  }

  @Test
  void cancellingLeaseExpiryFinalizesCancellation() throws Exception {
    System.setProperty("floecat.reconciler.job-store.lease-ms", "1000");
    System.setProperty("floecat.reconciler.job-store.reclaim-interval-ms", "1000");
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
    assertEquals("JS_CANCELLING", store.get(jobId).orElseThrow().state);

    Thread.sleep(1150L);

    assertTrue(store.leaseNext().isEmpty());
    store.runMaintenanceOnce(1_000L);

    var cancelled = store.get(jobId).orElseThrow();
    assertEquals("JS_CANCELLED", cancelled.state);
    assertEquals("stop", cancelled.message);
    assertTrue(cancelled.finishedAtMs > 0L);
    assertFalse(
        store
            .pointerStore
            .listPointersByPrefix(Keys.reconcileReadyPointerPrefix(), 20, "", new StringBuilder())
            .stream()
            .anyMatch(pointer -> pointer.getKey().contains(jobId)));
  }

  @Test
  void cancelPokesLeaseExpiryForFasterReclaim() throws Exception {
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
    store.cancel(ACCOUNT_ID, jobId, "stop");
    assertFalse(store.leaseNext().isPresent());

    // Cancel should poke lease expiry, allowing reclaim well before the original 5s lease.
    Thread.sleep(1300L);

    assertTrue(store.leaseNext().isEmpty());
    store.runMaintenanceOnce(1_000L);

    var cancelled = store.get(jobId).orElseThrow();
    assertEquals("JS_CANCELLED", cancelled.state);
    assertEquals("stop", cancelled.message);
    assertFalse(
        store
            .pointerStore
            .listPointersByPrefix(Keys.reconcileReadyPointerPrefix(), 20, "", new StringBuilder())
            .stream()
            .anyMatch(pointer -> pointer.getKey().contains(jobId)));
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
    assertEquals("JS_CANCELLING", store.get(jobId).orElseThrow().state);

    store.cancel(ACCOUNT_ID, jobId, "second stop");
    var stillCancelling = store.get(jobId).orElseThrow();

    assertEquals("JS_CANCELLING", stillCancelling.state);
    assertEquals("first stop", stillCancelling.message);
  }

  @Test
  void cancelRunningJobDoesNotCreateOrRetainReadyPointers() throws Exception {
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

    DurableReconcileJobStore.StoredReconcileJob running = readStoredRecord(canonicalPointerKey);
    long dueAtMs = System.currentTimeMillis();
    DurableReconcileJobStore.StoredReconcileJob staleReadyProjection =
        store.mapper.convertValue(running, DurableReconcileJobStore.StoredReconcileJob.class);
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

    DurableReconcileJobStore.StoredReconcileJob cancelling = readStoredRecord(canonicalPointerKey);
    assertEquals("JS_CANCELLING", cancelling.state);
    assertTrue(cancelling.readyPointerKey == null || cancelling.readyPointerKey.isBlank());
    assertTrue(staleReadyKeys.stream().allMatch(key -> store.pointerStore.get(key).isEmpty()));
  }

  @Test
  void maintenanceRepairsMissingReadyPointersForQueuedJob() {
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    DurableReconcileJobStore.StoredReconcileJob queued = readStoredRecord(canonicalPointerKey);
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
    var repairedLease = store.leaseNext().orElseThrow();

    assertEquals(jobId, repairedLease.jobId);
    assertTrue(store.pointerStore.get(originalReadyKey).isEmpty());
  }

  @Test
  void queuedReadyRepairDoesNotReindexCancellingJobs() throws Exception {
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

    DurableReconcileJobStore.StoredReconcileJob cancelling = readStoredRecord(canonicalPointerKey);
    assertEquals("JS_CANCELLING", cancelling.state);
    assertTrue(cancelling.readyPointerKey == null || cancelling.readyPointerKey.isBlank());

    Method repairQueuedReadyPointersIfNeeded =
        DurableReconcileJobStore.class.getDeclaredMethod(
            "repairQueuedReadyPointersIfNeeded", long.class, ReconcileJobStore.LeaseRequest.class);
    repairQueuedReadyPointersIfNeeded.setAccessible(true);

    assertFalse(
        (boolean)
            repairQueuedReadyPointersIfNeeded.invoke(
                store, System.currentTimeMillis(), ReconcileJobStore.LeaseRequest.all()));
    assertEquals("JS_CANCELLING", readStoredRecord(canonicalPointerKey).state);
    assertEquals(0, store.pointerStore.countByPrefix(Keys.reconcileReadyPointerPrefix()));
  }

  @Test
  void renewLeaseExtendsLeaseAndDelaysReclaim() throws Exception {
    System.setProperty("floecat.reconciler.job-store.lease-ms", "2000");
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
    var lease = store.leaseNext().orElseThrow();
    store.markRunning(
        lease.jobId, lease.leaseEpoch, System.currentTimeMillis(), "default_reconciler");
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    Pointer beforeRenew = store.pointerStore.get(canonicalPointerKey).orElseThrow();

    Thread.sleep(500L);
    assertTrue(store.renewLease(jobId, lease.leaseEpoch));
    Pointer afterRenew = store.pointerStore.get(canonicalPointerKey).orElseThrow();
    assertEquals(beforeRenew.getVersion(), afterRenew.getVersion());
    assertEquals(beforeRenew.getBlobUri(), afterRenew.getBlobUri());

    Thread.sleep(700L);
    assertTrue(store.leaseNext().isEmpty());

    Thread.sleep(1600L);
    store.runMaintenanceOnce(1_000L);
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

    Thread.sleep(150L);

    ReconcileJobStore.LeasedJob secondLease = store.leaseNext().orElseThrow();
    assertEquals(jobId, secondLease.jobId);
    store.markRunning(jobId, secondLease.leaseEpoch, 300L, "executor-2");
    store.markSucceeded(jobId, secondLease.leaseEpoch, 400L, 5L, 5L, 0L, 0L, 6L, 7L);

    DurableReconcileJobStore.StoredReconcileJob succeeded =
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

    Thread.sleep(1100L);

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

    Thread.sleep(1100L);

    store.runMaintenanceOnce(1_000L);

    DurableReconcileJobStore.StoredReconcileJob stillRunning =
        readStoredRecord(canonicalPointerKey);
    assertEquals("JS_RUNNING", stillRunning.state);
    assertTrue(store.getLeaseView(jobId).isPresent());
    assertEquals("JS_RUNNING", store.getLeaseView(jobId).orElseThrow().state);
    assertTrue(store.leaseNext().isEmpty());
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
  void leaseNextStopsReadyScanAtFirstFuturePointer() {
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
      DurableReconcileJobStore.StoredReconcileJob record = readStoredRecord(canonicalPointerKey);
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

    pointerStore.resetListCounts();

    assertTrue(store.leaseNext().isEmpty());
    assertEquals(1, pointerStore.listCount(Keys.reconcileReadyPointerPrefix()));
  }

  @Test
  void filteredLeaseByPinnedExecutorUsesPinnedReadyIndex() {
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

    pointerStore.resetListCounts();

    var lease =
        store
            .leaseNext(
                ReconcileJobStore.LeaseRequest.of(
                    Set.of(), Set.of(), Set.of("executor-a"), Set.of()))
            .orElseThrow();

    assertEquals(pinnedJobId, lease.jobId);
    assertEquals(
        1, pointerStore.listCount(Keys.reconcileReadyByPinnedExecutorPointerPrefix("executor-a")));
    assertEquals(0, pointerStore.listCount(Keys.reconcileReadyPointerPrefix()));
  }

  @Test
  void decodeReadyPointerTargetUsesCorrectOrderedKeySegments() throws Exception {
    store.init();
    Method decodeReadyPointerTarget =
        DurableReconcileJobStore.class.getDeclaredMethod(
            "decodeReadyPointerTarget",
            String.class,
            Class.forName(
                "ai.floedb.floecat.service.reconciler.jobs.DurableReconcileJobStore$ReadyIndexSelection"));
    decodeReadyPointerTarget.setAccessible(true);
    Class<?> readyIndexTypeClass =
        Class.forName(
            "ai.floedb.floecat.service.reconciler.jobs.DurableReconcileJobStore$ReadyIndexType");
    Class<?> readyIndexSelectionClass =
        Class.forName(
            "ai.floedb.floecat.service.reconciler.jobs.DurableReconcileJobStore$ReadyIndexSelection");
    var ctor =
        readyIndexSelectionClass.getDeclaredConstructor(
            String.class, readyIndexTypeClass, String.class);
    ctor.setAccessible(true);

    Object globalSelection =
        ctor.newInstance(
            Keys.reconcileReadyPointerPrefix(),
            Enum.valueOf((Class<Enum>) readyIndexTypeClass, "GLOBAL"),
            "");
    Object classSelection =
        ctor.newInstance(
            Keys.reconcileReadyByExecutionClassPointerPrefix(ReconcileExecutionClass.BATCH.name()),
            Enum.valueOf((Class<Enum>) readyIndexTypeClass, "EXECUTION_CLASS"),
            ReconcileExecutionClass.BATCH.name());
    Object laneSelection =
        ctor.newInstance(
            Keys.reconcileReadyByExecutionLanePointerPrefix("lane-1"),
            Enum.valueOf((Class<Enum>) readyIndexTypeClass, "EXECUTION_LANE"),
            "lane-1");
    Object pinnedSelection =
        ctor.newInstance(
            Keys.reconcileReadyByPinnedExecutorPointerPrefix("executor-1"),
            Enum.valueOf((Class<Enum>) readyIndexTypeClass, "PINNED_EXECUTOR"),
            "executor-1");
    Object kindSelection =
        ctor.newInstance(
            Keys.reconcileReadyByJobKindPointerPrefix(ReconcileJobKind.PLAN_VIEW.name()),
            Enum.valueOf((Class<Enum>) readyIndexTypeClass, "JOB_KIND"),
            ReconcileJobKind.PLAN_VIEW.name());

    String globalKey = Keys.reconcileReadyPointerByDue(123L, ACCOUNT_ID, "lane-1", "job-1");
    Object globalTarget = decodeReadyPointerTarget.invoke(store, globalKey, globalSelection);
    Method accountId = globalTarget.getClass().getDeclaredMethod("accountId");
    Method jobId = globalTarget.getClass().getDeclaredMethod("jobId");
    assertEquals(ACCOUNT_ID, accountId.invoke(globalTarget));
    assertEquals("job-1", jobId.invoke(globalTarget));

    String classKey =
        Keys.reconcileReadyByExecutionClassPointerByDue(
            123L, ReconcileExecutionClass.BATCH.name(), ACCOUNT_ID, "job-2");
    Object classTarget = decodeReadyPointerTarget.invoke(store, classKey, classSelection);
    assertEquals(ACCOUNT_ID, accountId.invoke(classTarget));
    assertEquals("job-2", jobId.invoke(classTarget));

    String laneKey =
        Keys.reconcileReadyByExecutionLanePointerByDue(123L, "lane-1", ACCOUNT_ID, "job-3");
    Object laneTarget = decodeReadyPointerTarget.invoke(store, laneKey, laneSelection);
    assertEquals(ACCOUNT_ID, accountId.invoke(laneTarget));
    assertEquals("job-3", jobId.invoke(laneTarget));

    String pinnedKey =
        Keys.reconcileReadyByPinnedExecutorPointerByDue(123L, "executor-1", ACCOUNT_ID, "job-4");
    Object pinnedTarget = decodeReadyPointerTarget.invoke(store, pinnedKey, pinnedSelection);
    assertEquals(ACCOUNT_ID, accountId.invoke(pinnedTarget));
    assertEquals("job-4", jobId.invoke(pinnedTarget));

    String kindKey =
        Keys.reconcileReadyByJobKindPointerByDue(
            123L, ReconcileJobKind.PLAN_VIEW.name(), ACCOUNT_ID, "job-5");
    Object kindTarget = decodeReadyPointerTarget.invoke(store, kindKey, kindSelection);
    assertEquals(ACCOUNT_ID, accountId.invoke(kindTarget));
    assertEquals("job-5", jobId.invoke(kindTarget));
  }

  @Test
  void filteredLeaseByExecutionClassFindsMatchingJobWithoutGlobalScan() {
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

    pointerStore.resetListCounts();

    var lease =
        store
            .leaseNext(
                ReconcileJobStore.LeaseRequest.of(Set.of(ReconcileExecutionClass.BATCH), Set.of()))
            .orElseThrow();

    assertEquals(batchJobId, lease.jobId);
    assertEquals(
        1,
        pointerStore.listCount(
            Keys.reconcileReadyByExecutionClassPointerPrefix(
                ReconcileExecutionClass.BATCH.name())));
    assertEquals(0, pointerStore.listCount(Keys.reconcileReadyPointerPrefix()));
  }

  @Test
  void filteredLeaseByLaneUsesLaneReadyIndex() {
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

    pointerStore.resetListCounts();

    var lease =
        store
            .leaseNext(ReconcileJobStore.LeaseRequest.of(Set.of(), Set.of("lane-a")))
            .orElseThrow();

    assertEquals(laneJobId, lease.jobId);
    assertEquals(
        1, pointerStore.listCount(Keys.reconcileReadyByExecutionLanePointerPrefix("lane-a")));
    assertEquals(0, pointerStore.listCount(Keys.reconcileReadyPointerPrefix()));
  }

  @Test
  void filteredLeaseByJobKindUsesJobKindReadyIndex() {
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

    pointerStore.resetListCounts();

    var lease =
        store
            .leaseNext(
                ReconcileJobStore.LeaseRequest.of(
                    Set.of(), Set.of(), Set.of(), Set.of(ReconcileJobKind.PLAN_VIEW)))
            .orElseThrow();

    assertEquals(viewJobId, lease.jobId);
    assertEquals(
        1,
        pointerStore.listCount(
            Keys.reconcileReadyByJobKindPointerPrefix(ReconcileJobKind.PLAN_VIEW.name())));
    assertEquals(0, pointerStore.listCount(Keys.reconcileReadyPointerPrefix()));
  }

  @Test
  void filteredLeaseByPinnedExecutorFallsBackToGlobalReadyIndex() {
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

    pointerStore.resetListCounts();

    var lease =
        store
            .leaseNext(
                ReconcileJobStore.LeaseRequest.of(
                    Set.of(), Set.of(), Set.of("executor-a"), Set.of()))
            .orElseThrow();

    assertEquals(globalJobId, lease.jobId);
    assertEquals(
        1, pointerStore.listCount(Keys.reconcileReadyByPinnedExecutorPointerPrefix("executor-a")));
    assertEquals(1, pointerStore.listCount(Keys.reconcileReadyPointerPrefix()));
  }

  @Test
  void leaseNextDeletesStaleSecondaryReadyPointer() {
    store.init();

    long dueAt = System.currentTimeMillis();
    String staleKey =
        Keys.reconcileReadyByExecutionClassPointerByDue(
            dueAt - 1L, ReconcileExecutionClass.DEFAULT.name(), ACCOUNT_ID, "missing-job");
    assertTrue(
        store.pointerStore.compareAndSet(
            staleKey,
            0L,
            Pointer.newBuilder()
                .setKey(staleKey)
                .setBlobUri(Keys.reconcileJobPointerById(ACCOUNT_ID, "missing-job"))
                .setVersion(1L)
                .build()));

    store.enqueue(
        ACCOUNT_ID,
        CONNECTOR_ID,
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.of(List.of(), "tbl-live"));

    assertTrue(
        store
            .leaseNext(
                ReconcileJobStore.LeaseRequest.of(
                    Set.of(ReconcileExecutionClass.DEFAULT), Set.of()))
            .isPresent());
    assertTrue(store.pointerStore.get(staleKey).isEmpty());
  }

  @Test
  void dueTimeChangeRewritesSecondaryReadyIndexes() {
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
    DurableReconcileJobStore.StoredReconcileJob queued = readStoredRecord(canonicalPointerKey);
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

    DurableReconcileJobStore.StoredReconcileJob requeued = readStoredRecord(canonicalPointerKey);
    long newDueAt = requeued.nextAttemptAtMs;
    assertNotEquals(oldDueAt, newDueAt);

    assertTrue(store.pointerStore.get(oldGlobalReadyKey).isEmpty());
    assertTrue(store.pointerStore.get(oldClassReadyKey).isEmpty());
    assertTrue(store.pointerStore.get(oldLaneReadyKey).isEmpty());
    assertTrue(store.pointerStore.get(oldPinnedReadyKey).isEmpty());
    assertTrue(store.pointerStore.get(oldKindReadyKey).isEmpty());

    assertTrue(store.pointerStore.get(requeued.readyPointerKey).isPresent());
    assertTrue(
        store
            .pointerStore
            .get(
                Keys.reconcileReadyByExecutionClassPointerByDue(
                    newDueAt, ReconcileExecutionClass.BATCH.name(), ACCOUNT_ID, jobId))
            .isPresent());
    assertTrue(
        store
            .pointerStore
            .get(
                Keys.reconcileReadyByExecutionLanePointerByDue(
                    newDueAt, "lane-retry", ACCOUNT_ID, jobId))
            .isPresent());
    assertTrue(
        store
            .pointerStore
            .get(
                Keys.reconcileReadyByPinnedExecutorPointerByDue(
                    newDueAt, "executor-retry", ACCOUNT_ID, jobId))
            .isPresent());
    assertTrue(
        store
            .pointerStore
            .get(
                Keys.reconcileReadyByJobKindPointerByDue(
                    newDueAt, ReconcileJobKind.PLAN_VIEW.name(), ACCOUNT_ID, jobId))
            .isPresent());
  }

  @Test
  void bulkEnqueueRollbackClearsPartiallyWrittenReadyIndexes() {
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

    assertEquals(0, store.pointerStore.countByPrefix(Keys.reconcileReadyPointerPrefix()));
    assertEquals(
        0,
        store.pointerStore.countByPrefix(
            Keys.reconcileReadyByExecutionClassPointerPrefix(
                ReconcileExecutionClass.BATCH.name())));
    assertEquals(
        0,
        store.pointerStore.countByPrefix(
            Keys.reconcileReadyByExecutionLanePointerPrefix("lane-partial")));
    assertEquals(
        0,
        store.pointerStore.countByPrefix(
            Keys.reconcileReadyByPinnedExecutorPointerPrefix("executor-partial")));
    assertEquals(
        0,
        store.pointerStore.countByPrefix(
            Keys.reconcileReadyByJobKindPointerPrefix(ReconcileJobKind.PLAN_CONNECTOR.name())));
  }

  @Test
  void staleSecondaryPointerWithOldFilterValueIsDeleted() {
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            "conn-stale-filter",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-stale-filter"),
            ReconcileExecutionPolicy.of(ReconcileExecutionClass.DEFAULT, "", Map.of()),
            "");
    DurableReconcileJobStore.StoredReconcileJob record =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, jobId));
    String staleClassKey =
        Keys.reconcileReadyByExecutionClassPointerByDue(
            record.nextAttemptAtMs, ReconcileExecutionClass.BATCH.name(), ACCOUNT_ID, jobId);
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    assertTrue(
        store.pointerStore.compareAndSet(
            staleClassKey,
            0L,
            Pointer.newBuilder()
                .setKey(staleClassKey)
                .setBlobUri(canonicalPointerKey)
                .setVersion(1L)
                .build()));

    assertTrue(
        store
            .leaseNext(
                ReconcileJobStore.LeaseRequest.of(Set.of(ReconcileExecutionClass.BATCH), Set.of()))
            .isEmpty());
    assertTrue(store.pointerStore.get(staleClassKey).isEmpty());
  }

  @Test
  void terminalStateClearsAllReadyIndexes() {
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
    DurableReconcileJobStore.StoredReconcileJob queued = readStoredRecord(canonicalPointerKey);
    long dueAt = queued.nextAttemptAtMs;

    store.cancel(ACCOUNT_ID, jobId, "cancel");

    assertTrue(store.pointerStore.get(queued.readyPointerKey).isEmpty());
    assertTrue(
        store
            .pointerStore
            .get(
                Keys.reconcileReadyByExecutionClassPointerByDue(
                    dueAt, ReconcileExecutionClass.BATCH.name(), ACCOUNT_ID, jobId))
            .isEmpty());
    assertTrue(
        store
            .pointerStore
            .get(
                Keys.reconcileReadyByExecutionLanePointerByDue(
                    dueAt, "lane-cancel", ACCOUNT_ID, jobId))
            .isEmpty());
    assertTrue(
        store
            .pointerStore
            .get(
                Keys.reconcileReadyByPinnedExecutorPointerByDue(
                    dueAt, "executor-cancel", ACCOUNT_ID, jobId))
            .isEmpty());
    assertTrue(
        store
            .pointerStore
            .get(
                Keys.reconcileReadyByJobKindPointerByDue(
                    dueAt, ReconcileJobKind.PLAN_VIEW.name(), ACCOUNT_ID, jobId))
            .isEmpty());
  }

  @Test
  void cancelRunningDoesNotReadyIndexCancellingJob() {
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

    DurableReconcileJobStore.StoredReconcileJob record =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, jobId));
    assertEquals("JS_CANCELLING", record.state);
    assertTrue(record.nextAttemptAtMs > 0L);
    assertTrue(record.readyPointerKey == null || record.readyPointerKey.isBlank());
    assertEquals(0, store.pointerStore.countByPrefix(Keys.reconcileReadyPointerPrefix()));
  }

  @Test
  void unfilteredLeaseStillUsesGlobalReadyIndex() {
    TrackingPointerStore pointerStore = new TrackingPointerStore();
    store.pointerStore = pointerStore;
    store.init();

    store.enqueue(
        ACCOUNT_ID,
        CONNECTOR_ID,
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.of(List.of(), "tbl-global"));

    pointerStore.resetListCounts();

    assertTrue(store.leaseNext().isPresent());
    assertEquals(1, pointerStore.listCount(Keys.reconcileReadyPointerPrefix()));
    assertEquals(
        0, pointerStore.listCount(Keys.reconcileReadyByPinnedExecutorPointerPrefix("executor-a")));
  }

  @Test
  void leaseNextDeletesMalformedAndOrphanedReadyPointers() {
    store.init();

    long dueAt = System.currentTimeMillis();
    String malformedKey =
        String.format("%s%019d/bad", Keys.reconcileReadyPointerPrefix(), dueAt - 2L);
    assertTrue(
        store.pointerStore.compareAndSet(
            malformedKey,
            0L,
            Pointer.newBuilder()
                .setKey(malformedKey)
                .setBlobUri("ignored")
                .setVersion(1L)
                .build()));

    String orphanKey =
        Keys.reconcileReadyPointerByDue(dueAt - 1L, ACCOUNT_ID, "lane-orphan", "job-orphan");
    assertTrue(
        store.pointerStore.compareAndSet(
            orphanKey,
            0L,
            Pointer.newBuilder()
                .setKey(orphanKey)
                .setBlobUri(Keys.reconcileJobPointerById(ACCOUNT_ID, "missing-job"))
                .setVersion(1L)
                .build()));

    String liveJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());

    var lease = store.leaseNext().orElseThrow();

    assertEquals(liveJobId, lease.jobId);
    assertTrue(store.pointerStore.get(malformedKey).isEmpty());
    assertTrue(store.pointerStore.get(orphanKey).isEmpty());
  }

  @Test
  void leaseNextDeletesMalformedReadyPointerTimestampAtQueueHead() {
    store.init();

    String malformedKey = Keys.reconcileReadyPointerPrefix() + "-bad/head";
    assertTrue(
        store.pointerStore.compareAndSet(
            malformedKey,
            0L,
            Pointer.newBuilder()
                .setKey(malformedKey)
                .setBlobUri("ignored")
                .setVersion(1L)
                .build()));

    String liveJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());

    var lease = store.leaseNext().orElseThrow();

    assertEquals(liveJobId, lease.jobId);
    assertTrue(store.pointerStore.get(malformedKey).isEmpty());
  }

  @Test
  void leaseNextRollsBackWhenLeaseExpiryIndexWriteFails() {
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
    DurableReconcileJobStore.StoredReconcileJob queued = readStoredRecord(canonicalPointerKey);

    assertTrue(store.leaseNext().isEmpty());

    DurableReconcileJobStore.StoredReconcileJob afterAttempt =
        readStoredRecord(canonicalPointerKey);
    assertEquals("JS_QUEUED", afterAttempt.state);
    assertEquals(queued.readyPointerKey, afterAttempt.readyPointerKey);
    assertTrue(store.pointerStore.get(queued.readyPointerKey).isPresent());
    assertTrue(
        store
            .pointerStore
            .get(
                Keys.reconcileReadyByExecutionClassPointerByDue(
                    queued.nextAttemptAtMs,
                    queued.executionPolicy().executionClass().name(),
                    ACCOUNT_ID,
                    jobId))
            .isPresent());
    assertTrue(
        store
            .pointerStore
            .get(
                Keys.reconcileReadyByJobKindPointerByDue(
                    queued.nextAttemptAtMs, queued.jobKind().name(), ACCOUNT_ID, jobId))
            .isPresent());
    assertTrue(
        store.pointerStore.get(Keys.reconcileJobLeasePointerById(ACCOUNT_ID, jobId)).isEmpty());
  }

  @Test
  void duplicateEnqueueRepairsMissingSecondaryReadyIndexes() {
    store.init();

    ReconcileScope scope = ReconcileScope.ofView(List.of(), "tbl-repair");
    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            "conn-repair",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            scope,
            ReconcileJobKind.PLAN_VIEW,
            ReconcileTableTask.empty(),
            ReconcileViewTask.of("src", "view", "dest", "tbl-repair", "Repair View"),
            ReconcileExecutionPolicy.of(ReconcileExecutionClass.BATCH, "lane-repair", Map.of()),
            "",
            "executor-repair");

    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    DurableReconcileJobStore.StoredReconcileJob record = readStoredRecord(canonicalPointerKey);
    String classKey =
        Keys.reconcileReadyByExecutionClassPointerByDue(
            record.nextAttemptAtMs, ReconcileExecutionClass.BATCH.name(), ACCOUNT_ID, jobId);
    String laneKey =
        Keys.reconcileReadyByExecutionLanePointerByDue(
            record.nextAttemptAtMs, "lane-repair", ACCOUNT_ID, jobId);
    String pinnedKey =
        Keys.reconcileReadyByPinnedExecutorPointerByDue(
            record.nextAttemptAtMs, "executor-repair", ACCOUNT_ID, jobId);
    String kindKey =
        Keys.reconcileReadyByJobKindPointerByDue(
            record.nextAttemptAtMs, ReconcileJobKind.PLAN_VIEW.name(), ACCOUNT_ID, jobId);

    store
        .pointerStore
        .get(classKey)
        .ifPresent(pointer -> store.pointerStore.compareAndDelete(classKey, pointer.getVersion()));
    store
        .pointerStore
        .get(laneKey)
        .ifPresent(pointer -> store.pointerStore.compareAndDelete(laneKey, pointer.getVersion()));
    store
        .pointerStore
        .get(pinnedKey)
        .ifPresent(pointer -> store.pointerStore.compareAndDelete(pinnedKey, pointer.getVersion()));
    store
        .pointerStore
        .get(kindKey)
        .ifPresent(pointer -> store.pointerStore.compareAndDelete(kindKey, pointer.getVersion()));

    String dedupedJobId =
        store.enqueue(
            ACCOUNT_ID,
            "conn-repair",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            scope,
            ReconcileJobKind.PLAN_VIEW,
            ReconcileTableTask.empty(),
            ReconcileViewTask.of("src", "view", "dest", "tbl-repair", "Repair View"),
            ReconcileExecutionPolicy.of(ReconcileExecutionClass.BATCH, "lane-repair", Map.of()),
            "",
            "executor-repair");

    assertEquals(jobId, dedupedJobId);
    assertTrue(store.pointerStore.get(classKey).isEmpty());
    assertTrue(store.pointerStore.get(laneKey).isEmpty());
    assertTrue(store.pointerStore.get(pinnedKey).isEmpty());
    assertTrue(store.pointerStore.get(kindKey).isEmpty());
  }

  @Test
  @SuppressWarnings("unchecked")
  void repairReadyPointerRepairsDerivedIndexesAndCanonicalGlobalKeyWhenDrifted() throws Exception {
    store.init();

    String jobId =
        store.enqueueViewPlan(
            ACCOUNT_ID,
            "conn-repair-global",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.ofView(List.of(), "view-repair-global"),
            ReconcileViewTask.of(
                "src-ns", "src-view", "dest-ns", "view-repair-global", "Repair Global"),
            ReconcileExecutionPolicy.of(
                ReconcileExecutionClass.BATCH, "lane-repair-global", Map.of()),
            "",
            "executor-repair-global");
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    DurableReconcileJobStore.StoredReconcileJob record = readStoredRecord(canonicalPointerKey);
    long dueAt = record.nextAttemptAtMs;
    String expectedGlobalKey =
        Keys.reconcileReadyPointerByDue(dueAt, ACCOUNT_ID, record.laneKey, jobId);
    String driftedGlobalKey =
        Keys.reconcileReadyPointerByDue(dueAt + 999L, ACCOUNT_ID, record.laneKey, jobId);
    String classKey =
        Keys.reconcileReadyByExecutionClassPointerByDue(
            dueAt, ReconcileExecutionClass.BATCH.name(), ACCOUNT_ID, jobId);
    String laneKey =
        Keys.reconcileReadyByExecutionLanePointerByDue(
            dueAt, "lane-repair-global", ACCOUNT_ID, jobId);
    String pinnedKey =
        Keys.reconcileReadyByPinnedExecutorPointerByDue(
            dueAt, "executor-repair-global", ACCOUNT_ID, jobId);
    String kindKey =
        Keys.reconcileReadyByJobKindPointerByDue(
            dueAt, ReconcileJobKind.PLAN_VIEW.name(), ACCOUNT_ID, jobId);

    Pointer globalPointer = store.pointerStore.get(expectedGlobalKey).orElseThrow();
    assertTrue(store.pointerStore.compareAndDelete(expectedGlobalKey, globalPointer.getVersion()));
    store
        .pointerStore
        .get(classKey)
        .ifPresent(pointer -> store.pointerStore.compareAndDelete(classKey, pointer.getVersion()));
    store
        .pointerStore
        .get(laneKey)
        .ifPresent(pointer -> store.pointerStore.compareAndDelete(laneKey, pointer.getVersion()));
    store
        .pointerStore
        .get(pinnedKey)
        .ifPresent(pointer -> store.pointerStore.compareAndDelete(pinnedKey, pointer.getVersion()));
    store
        .pointerStore
        .get(kindKey)
        .ifPresent(pointer -> store.pointerStore.compareAndDelete(kindKey, pointer.getVersion()));

    record.readyPointerKey = driftedGlobalKey;
    overwriteCanonicalRecordWithoutSync(canonicalPointerKey, record);

    Method repairReadyPointer =
        DurableReconcileJobStore.class.getDeclaredMethod(
            "repairReadyPointer", String.class, DurableReconcileJobStore.StoredReconcileJob.class);
    repairReadyPointer.setAccessible(true);

    assertTrue((boolean) repairReadyPointer.invoke(store, canonicalPointerKey, record));

    DurableReconcileJobStore.StoredReconcileJob repaired = readStoredRecord(canonicalPointerKey);
    assertEquals(expectedGlobalKey, repaired.readyPointerKey);
    assertTrue(store.pointerStore.get(expectedGlobalKey).isPresent());
    assertTrue(store.pointerStore.get(classKey).isPresent());
    assertTrue(store.pointerStore.get(laneKey).isPresent());
    assertTrue(store.pointerStore.get(pinnedKey).isPresent());
    assertTrue(store.pointerStore.get(kindKey).isPresent());
    assertTrue(store.pointerStore.get(driftedGlobalKey).isEmpty());
  }

  @Test
  void hotPathRepairSuppressionDefersCanonicalPointerRepairHints() throws Exception {
    store.init();

    String jobId =
        store.enqueuePlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-hot-path"),
            ReconcileExecutionPolicy.of(ReconcileExecutionClass.BATCH, "lane-hot-path", Map.of()),
            "");
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    DurableReconcileJobStore.StoredReconcileJob record = readStoredRecord(canonicalPointerKey);

    deletePointerIfPresent(Keys.reconcileJobLookupPointerById(jobId));
    for (String statePointerKey : statePointerKeysFor(record)) {
      deletePointerIfPresent(statePointerKey);
    }
    for (String readyPointerKey : readyPointerKeysFor(record)) {
      deletePointerIfPresent(readyPointerKey);
    }

    withInlineRepairSuppressed(
        () ->
            assertDoesNotThrow(
                () ->
                    invokePrivateMethod(
                        "repairCanonicalPointersIfNeeded",
                        new Class<?>[] {
                          String.class,
                          DurableReconcileJobStore.StoredReconcileJob.class,
                          String.class,
                          String.class
                        },
                        canonicalPointerKey,
                        record,
                        "test_hot_path",
                        "pointer_drift")));

    assertEquals(1, store.pendingRepairHintCount());
    assertTrue(store.pointerStore.get(Keys.reconcileJobLookupPointerById(jobId)).isEmpty());
    assertTrue(
        statePointerKeysFor(record).stream()
            .allMatch(key -> store.pointerStore.get(key).isEmpty()));
    assertTrue(
        readyPointerKeysFor(record).stream()
            .allMatch(key -> store.pointerStore.get(key).isEmpty()));
  }

  @Test
  void leaseNextDoesNotDrainPendingRepairHints() throws Exception {
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);

    withInlineRepairSuppressed(
        () ->
            assertEquals(
                "DEFERRED",
                invokePrivateMethod(
                        "repairLookupPointerDisposition",
                        new Class<?>[] {String.class, String.class, String.class, String.class},
                        jobId,
                        canonicalPointerKey,
                        "test_seed_hint",
                        "lookup_pointer_missing")
                    .toString()));

    assertEquals(1, store.pendingRepairHintCount());
    assertTrue(store.leaseNext().isPresent());
    assertEquals(1, store.pendingRepairHintCount());
  }

  @Test
  void maintenanceDrainRespectsMaxHintsAndMaxMillis() throws Exception {
    store.init();

    String firstJob =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-drain-1"));
    String secondJob =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-drain-2"));

    withInlineRepairSuppressed(
        () -> {
          String firstCanonical = Keys.reconcileJobPointerById(ACCOUNT_ID, firstJob);
          String secondCanonical = Keys.reconcileJobPointerById(ACCOUNT_ID, secondJob);
          invokePrivateMethod(
              "repairLookupPointerDisposition",
              new Class<?>[] {String.class, String.class, String.class, String.class},
              firstJob,
              firstCanonical,
              "test_seed_hint",
              "lookup_pointer_missing");
          invokePrivateMethod(
              "repairLookupPointerDisposition",
              new Class<?>[] {String.class, String.class, String.class, String.class},
              secondJob,
              secondCanonical,
              "test_seed_hint",
              "lookup_pointer_missing");
        });

    assertEquals(2, store.pendingRepairHintCount());
    store.drainPendingRepairHintsForMaintenance(1, 1_000L);
    assertEquals(1, store.pendingRepairHintCount());

    store.drainPendingRepairHintsForMaintenance(10, 0L);
    assertEquals(1, store.pendingRepairHintCount());
  }

  @Test
  void invalidRepairRequestsDoNotEnqueueHints() throws Exception {
    store.init();

    withInlineRepairSuppressed(
        () -> {
          assertEquals(
              "FAILED",
              invokePrivateMethod(
                      "repairLookupPointerDisposition",
                      new Class<?>[] {String.class, String.class, String.class, String.class},
                      "",
                      "/canonical/key",
                      "test_invalid",
                      "blank_job_id")
                  .toString());
          assertEquals(
              "FAILED",
              invokePrivateMethod(
                      "repairLookupPointerDisposition",
                      new Class<?>[] {String.class, String.class, String.class, String.class},
                      "job-1",
                      "",
                      "test_invalid",
                      "blank_canonical_key")
                  .toString());
          assertEquals(
              "FAILED",
              invokePrivateMethod(
                      "repairReadyPointerDisposition",
                      new Class<?>[] {
                        String.class,
                        DurableReconcileJobStore.StoredReconcileJob.class,
                        String.class,
                        String.class
                      },
                      "/canonical/key",
                      null,
                      "test_invalid",
                      "null_ready_record")
                  .toString());
          assertEquals(
              "FAILED",
              invokePrivateMethod(
                      "repairStatePointerDisposition",
                      new Class<?>[] {
                        String.class,
                        DurableReconcileJobStore.StoredReconcileJob.class,
                        String.class,
                        String.class
                      },
                      "/canonical/key",
                      null,
                      "test_invalid",
                      "null_state_record")
                  .toString());
        });

    assertEquals(0, store.pendingRepairHintCount());
  }

  @Test
  void deferredRepairDispositionIsDistinctFromRepaired() throws Exception {
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-disposition"));
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    deletePointerIfPresent(Keys.reconcileJobLookupPointerById(jobId));

    final String[] deferred = new String[1];
    final boolean[] handled = new boolean[1];
    withInlineRepairSuppressed(
        () -> {
          deferred[0] =
              invokePrivateMethod(
                      "repairLookupPointerDisposition",
                      new Class<?>[] {String.class, String.class, String.class, String.class},
                      jobId,
                      canonicalPointerKey,
                      "test_deferred",
                      "lookup_pointer_missing")
                  .toString();
          handled[0] =
              (boolean)
                  invokePrivateMethod(
                      "repairLookupPointer",
                      new Class<?>[] {String.class, String.class, String.class, String.class},
                      jobId,
                      canonicalPointerKey,
                      "test_deferred",
                      "lookup_pointer_missing");
        });

    assertEquals("DEFERRED", deferred[0]);
    assertTrue(handled[0]);
    assertEquals(1, store.pendingRepairHintCount());

    Object repaired =
        invokePrivateMethod(
            "repairLookupPointerDisposition",
            new Class<?>[] {String.class, String.class, String.class, String.class},
            jobId,
            canonicalPointerKey,
            "test_inline",
            "lookup_pointer_missing");
    assertEquals("REPAIRED", repaired.toString());
  }

  @Test
  void loadByAnyAccountReturnsEmptyForBlankJobId() throws Exception {
    store.init();

    Object loaded = invokePrivateMethod("loadByAnyAccount", new Class<?>[] {String.class}, " ");

    assertEquals(Optional.empty(), loaded);
  }

  @Test
  void pinnedExecutorJobsAreNotLeasedByUnpinnedOrWrongExecutorRequests() {
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

    pointerStore.resetListCounts();
    assertTrue(store.leaseNext().isEmpty());
    assertEquals(1, pointerStore.listCount(Keys.reconcileReadyPointerPrefix()));

    pointerStore.resetListCounts();
    assertTrue(
        store
            .leaseNext(
                ReconcileJobStore.LeaseRequest.of(
                    Set.of(), Set.of(), Set.of("executor-b"), Set.of()))
            .isEmpty());
    assertEquals(
        1, pointerStore.listCount(Keys.reconcileReadyByPinnedExecutorPointerPrefix("executor-b")));

    pointerStore.resetListCounts();
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
    assertTrue(store.pointerStore.get(readyKey).isPresent());
    assertTrue(
        store.pointerStore.get(Keys.reconcileJobLeasePointerById(ACCOUNT_ID, jobId)).isEmpty());
  }

  @Test
  void markProgressDoesNotRewriteStableLookupOrConnectorPointers() {
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
    String leaseKey = Keys.reconcileJobLeasePointerById(ACCOUNT_ID, jobId);
    DurableReconcileJobStore.StoredJobLease storedLease =
        readStoredLease(store.pointerStore.get(leaseKey).orElseThrow().getBlobUri());
    String expiryKey = leaseExpiryPointerKey(storedLease.expiresAtMs, ACCOUNT_ID, jobId);

    store.markSucceeded(jobId, lease.leaseEpoch, System.currentTimeMillis(), 1, 1, 1, 1);

    assertTrue(store.pointerStore.get(leaseKey).isEmpty());
    assertTrue(store.pointerStore.get(expiryKey).isEmpty());
  }

  @Test
  void reclaimDeletesStaleExpiryIndexEntryWithoutGlobalRepairScan() {
    System.setProperty("floecat.reconciler.job-store.reclaim-interval-ms", "1");
    store.init();

    String staleExpiryKey =
        leaseExpiryPointerKey(System.currentTimeMillis() - 1_000L, ACCOUNT_ID, "missing");
    assertTrue(
        store.pointerStore.compareAndSet(
            staleExpiryKey,
            0L,
            Pointer.newBuilder()
                .setKey(staleExpiryKey)
                .setBlobUri(Keys.reconcileJobPointerById(ACCOUNT_ID, "missing"))
                .setVersion(1L)
                .build()));

    assertTrue(store.leaseNext().isEmpty());
    store.runMaintenanceOnce(1_000L);
    assertTrue(store.pointerStore.get(staleExpiryKey).isEmpty());
  }

  @Test
  void reclaimDeletesMalformedExpiryPointerTimestampAtQueueHead() {
    System.setProperty("floecat.reconciler.job-store.lease-ms", "1");
    System.setProperty("floecat.reconciler.job-store.reclaim-interval-ms", "1");
    System.setProperty("floecat.reconciler.job-store.lease-renew-grace-ms", "0");
    store.init();

    String malformedExpiryKey = LEASE_EXPIRY_POINTER_PREFIX + "-bad/head";
    assertTrue(
        store.pointerStore.compareAndSet(
            malformedExpiryKey,
            0L,
            Pointer.newBuilder()
                .setKey(malformedExpiryKey)
                .setBlobUri("ignored")
                .setVersion(1L)
                .build()));

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());

    var lease = store.leaseNext().orElseThrow();
    store.markRunning(jobId, lease.leaseEpoch, System.currentTimeMillis(), "exec-1");
    assertDoesNotThrow(() -> Thread.sleep(1150L));

    Optional<ReconcileJobStore.LeasedJob> reclaimed = Optional.empty();
    for (int i = 0; i < 10 && reclaimed.isEmpty(); i++) {
      store.runMaintenanceOnce(1_000L);
      reclaimed = store.leaseNext();
      if (reclaimed.isEmpty()) {
        assertDoesNotThrow(() -> Thread.sleep(25L));
      }
    }

    assertEquals(jobId, reclaimed.orElseThrow().jobId);
    assertTrue(store.pointerStore.get(malformedExpiryKey).isEmpty());
  }

  @Test
  void queueStatsReflectQueuedWaitingRunningAndCancellingJobs() {
    store.init();
    ReconcileScope queuedScope = ReconcileScope.of(List.of(), "tbl-q");
    ReconcileScope waitingScope = ReconcileScope.of(List.of(), "tbl-w");
    ReconcileScope runningScope = ReconcileScope.of(List.of(), "tbl-r");
    ReconcileScope cancellingScope = ReconcileScope.of(List.of(), "tbl-c");

    store.enqueue(ACCOUNT_ID, "conn-q", false, CaptureMode.METADATA_AND_CAPTURE, queuedScope);
    String waitingJobId =
        store.enqueue(ACCOUNT_ID, "conn-w", false, CaptureMode.METADATA_AND_CAPTURE, waitingScope);
    store.enqueue(ACCOUNT_ID, "conn-r", false, CaptureMode.METADATA_AND_CAPTURE, runningScope);
    store.enqueue(ACCOUNT_ID, "conn-c", false, CaptureMode.METADATA_AND_CAPTURE, cancellingScope);

    var firstLease = store.leaseNext().orElseThrow();
    store.markRunning(
        firstLease.jobId, firstLease.leaseEpoch, System.currentTimeMillis(), "default_reconciler");

    ReconcileJobStore.LeasedJob waitingLease = leaseJob(waitingJobId);
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

    var stats = store.queueStats();

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

    store.enqueue(
        ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, ReconcileScope.empty());
    // Take the first lease so the job is removed from the ready queue.
    // leaseNext() short-circuits via leaseReadyDue() here — reclaim is NOT triggered yet.
    store.leaseNext().orElseThrow();

    // Replace blobStore with one that throws StorageNotFoundException on every get().
    // This simulates the S3 "not found" case reported in the bug.
    store.blobStore = new ThrowingOnGetBlobStore();

    Thread.sleep(50L); // ensure lease-ms and reclaim-interval-ms have elapsed

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
  void readPathsDoNotRebuildMissingContributionPointers() {
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

    String contributionKey =
        Keys.reconcileJobContributionPointer(ACCOUNT_ID, parentJobId, childJobId);
    deletePointerIfPresent(contributionKey);
    assertTrue(store.pointerStore.get(contributionKey).isEmpty());

    assertDoesNotThrow(() -> store.list(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()));
    assertTrue(store.pointerStore.get(contributionKey).isEmpty());

    assertDoesNotThrow(() -> store.get(ACCOUNT_ID, parentJobId));
    assertTrue(store.pointerStore.get(contributionKey).isEmpty());
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
        1, blobStore.getCount(), "expected one ready-candidate read on the uncontended happy path");
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
  void connectorParentProjectsChildProgressInGetWhileListStaysInlineOnly() {
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

    ReconcileJob projected = store.get(ACCOUNT_ID, connectorJobId).orElseThrow();
    assertEquals("JS_RUNNING", projected.state);
    assertEquals("Table running", projected.message);
    assertEquals(1L, projected.tablesScanned);
    assertEquals(1L, projected.tablesChanged);
    assertEquals(2L, projected.viewsScanned);
    assertEquals(1L, projected.viewsChanged);
    assertEquals(3L, projected.snapshotsProcessed);
    assertEquals(4L, projected.statsProcessed);
    assertEquals("executor-a", projected.executorId);

    ReconcileJob listed =
        store.list(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
            .filter(job -> connectorJobId.equals(job.jobId))
            .findFirst()
            .orElseThrow();
    assertTrue(
        "JS_QUEUED".equals(listed.state)
            || "JS_WAITING".equals(listed.state)
            || "JS_RUNNING".equals(listed.state));
    assertEquals(0L, listed.tablesScanned);
    assertEquals(0L, listed.statsProcessed);
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

    ReconcileJob projected = store.get(ACCOUNT_ID, connectorJobId).orElseThrow();
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

    ReconcileJobStore.LeasedJob childLease = leaseJob(childJobId);
    store.markRunning(childJobId, childLease.leaseEpoch, 100L, "executor-fail");
    store.markFailedTerminal(
        childJobId, childLease.leaseEpoch, 200L, "child failed", 0L, 0L, 0L, 0L, 1L, 0L, 0L);

    ReconcileJob parent = store.get(ACCOUNT_ID, parentJobId).orElseThrow();
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

    ReconcileJobStore.LeasedJob childLease = leaseJob(childJobId);
    store.markRunning(childJobId, childLease.leaseEpoch, 100L, "executor-cancel");
    store.markCancelled(
        childJobId, childLease.leaseEpoch, 200L, "child cancelled", 0L, 0L, 0L, 0L, 0L, 0L, 0L);

    ReconcileJob parent = store.get(ACCOUNT_ID, parentJobId).orElseThrow();
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

    ReconcileJobStore.LeasedJob childLease = leaseJob(childJobId);
    store.markRunning(childJobId, childLease.leaseEpoch, 100L, "executor-success");

    ReconcileJob runningParent = store.get(ACCOUNT_ID, parentJobId).orElseThrow();
    assertEquals("JS_RUNNING", runningParent.state);
    assertEquals("Running", runningParent.message);

    store.markSucceeded(childJobId, childLease.leaseEpoch, 200L, 1L, 1L, 0L, 0L);

    ReconcileJob parent = store.get(ACCOUNT_ID, parentJobId).orElseThrow();
    assertEquals("JS_SUCCEEDED", parent.state);
    assertEquals("Succeeded", parent.message);
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

    ReconcileJobStore.LeasedJob childLease = leaseJob(childJobId);
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

    ReconcileJob parent = store.get(ACCOUNT_ID, parentJobId).orElseThrow();
    assertEquals("JS_WAITING", parent.state);
    assertEquals("Waiting on dependency", parent.message);
  }

  @Test
  void completedPlanConnectorIsNotRequeuedWhenQueuedChildRollsUpCounters() throws Exception {
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

    DurableReconcileJobStore.StoredReconcileJob completedParent =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, parentJobId));
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

    DurableReconcileJobStore.StoredReconcileJob canonicalParent =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, parentJobId));
    assertEquals("JS_SUCCEEDED", canonicalParent.state);
    assertTrue(
        canonicalParent.readyPointerKey == null || canonicalParent.readyPointerKey.isBlank());
    assertTrue(
        readyPointerKeysFor(canonicalParent).stream()
            .allMatch(readyPointerKey -> store.pointerStore.get(readyPointerKey).isEmpty()));
    assertEquals("JS_SUCCEEDED", store.getLeaseView(parentJobId).orElseThrow().state);

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

    store.markSucceeded(connectorJobId, connectorLease.leaseEpoch, 200L, 19L, 0L, 0L, 0L);

    ReconcileJob waiting = store.get(ACCOUNT_ID, connectorJobId).orElseThrow();
    assertEquals("JS_WAITING", waiting.state);
    assertEquals("Waiting on child work", waiting.message);
    assertEquals(19L, waiting.tablesScanned);
    assertEquals(0L, waiting.tablesChanged);
    assertEquals(0L, waiting.finishedAtMs);

    ReconcileJob listedWaiting =
        store.list(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
            .filter(job -> connectorJobId.equals(job.jobId))
            .findFirst()
            .orElseThrow();
    assertEquals("JS_WAITING", listedWaiting.state);
    assertEquals("Waiting on child work", listedWaiting.message);
    assertEquals(19L, listedWaiting.tablesScanned);
    assertEquals(0L, listedWaiting.tablesChanged);
    assertEquals(0L, listedWaiting.finishedAtMs);

    ReconcileJob stillWaiting = store.get(ACCOUNT_ID, connectorJobId).orElseThrow();
    assertEquals("JS_WAITING", stillWaiting.state);

    ReconcileJobStore.LeasedJob tableLease = leaseJob(tableJobId, ReconcileJobKind.PLAN_TABLE);
    store.markRunning(tableJobId, tableLease.leaseEpoch, 250L, "executor-table");
    store.markSucceeded(tableJobId, tableLease.leaseEpoch, 300L, 1L, 1L, 0L, 0L);

    ReconcileJob succeeded = store.get(ACCOUNT_ID, connectorJobId).orElseThrow();
    assertEquals("JS_SUCCEEDED", succeeded.state);
    assertEquals("Succeeded", succeeded.message);
    assertEquals(300L, succeeded.finishedAtMs);
    assertEquals(1L, succeeded.tablesChanged);

    ReconcileJob listedSucceeded =
        store.list(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
            .filter(job -> connectorJobId.equals(job.jobId))
            .findFirst()
            .orElseThrow();
    assertEquals("JS_SUCCEEDED", listedSucceeded.state);
    assertEquals("Succeeded", listedSucceeded.message);
    assertEquals(300L, listedSucceeded.finishedAtMs);
    assertEquals(1L, listedSucceeded.tablesChanged);
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
            ReconcileJobStore.CompletionKind.SUCCEEDED,
            200L,
            "Planned 1 table job(s)",
            1L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L));

    ReconcileJob waiting = store.get(ACCOUNT_ID, connectorJobId).orElseThrow();
    assertEquals("JS_WAITING", waiting.state);
    assertEquals("Planned 1 table job(s)", waiting.message);

    ReconcileJobStore.LeasedJob tableLease = leaseJob(tableJobId, ReconcileJobKind.PLAN_TABLE);
    store.markRunning(tableJobId, tableLease.leaseEpoch, 250L, "executor-table");
    store.markSucceeded(tableJobId, tableLease.leaseEpoch, 300L, 1L, 1L, 0L, 0L);

    ReconcileJob succeeded = store.get(ACCOUNT_ID, connectorJobId).orElseThrow();
    assertEquals("JS_SUCCEEDED", succeeded.state);
    assertEquals("Succeeded", succeeded.message);

    ReconcileJob listedSucceeded =
        store.list(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
            .filter(job -> connectorJobId.equals(job.jobId))
            .findFirst()
            .orElseThrow();
    assertEquals("JS_SUCCEEDED", listedSucceeded.state);
    assertEquals("Succeeded", listedSucceeded.message);
  }

  @Test
  void waitingParentTerminalTransitionCleansUpHistoricalReadyPointersWithoutStoredReadyKey()
      throws Exception {
    store.init();

    String connectorJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId);
    DurableReconcileJobStore.StoredReconcileJob queuedParent =
        readStoredRecord(canonicalPointerKey);
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

    store.markSucceeded(connectorJobId, connectorLease.leaseEpoch, 200L, 1L, 0L, 0L, 0L);

    DurableReconcileJobStore.StoredReconcileJob waitingParent =
        readStoredRecord(canonicalPointerKey);
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

    DurableReconcileJobStore.StoredReconcileJob succeededParent =
        readStoredRecord(canonicalPointerKey);
    assertEquals("JS_SUCCEEDED", succeededParent.state);
    assertTrue(
        staleReadyKeys.stream()
            .allMatch(readyPointerKey -> store.pointerStore.get(readyPointerKey).isEmpty()));
  }

  @Test
  void tableParentWaitsForDirectSnapshotChildrenBeforeShowingSucceeded() {
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
    store.markSucceeded(tableJobId, tableLease.leaseEpoch, 200L, 1L, 0L, 0L, 1L);

    ReconcileJob waiting = store.get(ACCOUNT_ID, tableJobId).orElseThrow();
    assertEquals("JS_WAITING", waiting.state);
    assertEquals("Waiting on child work", waiting.message);
    assertEquals(0L, waiting.finishedAtMs);

    ReconcileJob listedWaiting =
        store.list(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
            .filter(job -> tableJobId.equals(job.jobId))
            .findFirst()
            .orElseThrow();
    assertEquals("JS_WAITING", listedWaiting.state);
    assertEquals(0L, listedWaiting.finishedAtMs);

    ReconcileJobStore.LeasedJob snapshotLease =
        leaseJob(snapshotJobId, ReconcileJobKind.PLAN_SNAPSHOT);
    store.markRunning(snapshotJobId, snapshotLease.leaseEpoch, 250L, "executor-snapshot");
    store.markSucceeded(snapshotJobId, snapshotLease.leaseEpoch, 300L, 0L, 0L, 0L, 0L);

    ReconcileJob succeeded = store.get(ACCOUNT_ID, tableJobId).orElseThrow();
    assertEquals("JS_SUCCEEDED", succeeded.state);
    assertEquals(300L, succeeded.finishedAtMs);

    ReconcileJob listedSucceeded =
        store.list(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
            .filter(job -> tableJobId.equals(job.jobId))
            .findFirst()
            .orElseThrow();
    assertEquals("JS_SUCCEEDED", listedSucceeded.state);
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

    store.markSucceeded(connectorJobId, connectorLease.leaseEpoch, 200L, 0L, 1L, 0L, 0L);

    ReconcileJob leaseViewAfterSuccess = store.getLeaseView(connectorJobId).orElseThrow();
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

    store.markSucceeded(connectorJobId, connectorLease.leaseEpoch, 200L, 1L, 0L, 0L, 0L);

    ReconcileJob waitingParent = store.get(ACCOUNT_ID, connectorJobId).orElseThrow();
    assertEquals("JS_WAITING", waitingParent.state);
    assertEquals("Waiting on child work", waitingParent.message);

    ReconcileJobStore.LeasedJob tableLease = leaseJob(tableJobId, ReconcileJobKind.PLAN_TABLE);
    store.markRunning(tableJobId, tableLease.leaseEpoch, 250L, "executor-table");

    ReconcileJob stillWaitingParent = store.get(ACCOUNT_ID, connectorJobId).orElseThrow();
    assertEquals("JS_WAITING", stillWaitingParent.state);
    assertEquals("Waiting on child work", stillWaitingParent.message);

    DurableReconcileJobStore.StoredReconcileJob storedParent =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId));
    assertEquals("JS_WAITING", storedParent.state);
    assertEquals("Waiting on child work", storedParent.message);
  }

  @Test
  void waitingParentStaysWaitingWhileChildRunsEvenIfExpectedChildJobsIsCorruptedLow()
      throws Exception {
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

    store.markSucceeded(connectorJobId, connectorLease.leaseEpoch, 200L, 1L, 0L, 0L, 0L);

    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId);
    DurableReconcileJobStore.StoredReconcileJob waitingParent =
        readStoredRecord(canonicalPointerKey);
    waitingParent.expectedChildJobs = 0L;
    overwriteCanonicalRecordWithoutSync(canonicalPointerKey, waitingParent);

    ReconcileJobStore.LeasedJob tableLease = leaseJob(tableJobId, ReconcileJobKind.PLAN_TABLE);
    store.markRunning(tableJobId, tableLease.leaseEpoch, 250L, "executor-table");

    DurableReconcileJobStore.StoredReconcileJob storedParent =
        readStoredRecord(canonicalPointerKey);
    assertEquals("JS_WAITING", storedParent.state);
    assertEquals("Waiting on child work", storedParent.message);

    ReconcileJob projectedParent = store.get(ACCOUNT_ID, connectorJobId).orElseThrow();
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
        leaseJob(snapshotJobId, ReconcileJobKind.PLAN_SNAPSHOT);
    store.markRunning(snapshotJobId, snapshotLease.leaseEpoch, 100L, "executor-snapshot");
    store.markSucceeded(snapshotJobId, snapshotLease.leaseEpoch, 200L, 0L, 0L, 1L, 1L);

    ReconcileJobStore.LeasedJob execLease = leaseJob(execJobId, ReconcileJobKind.EXEC_FILE_GROUP);
    store.markRunning(execJobId, execLease.leaseEpoch, 250L, "executor-exec");
    store.persistFileGroupResult(
        execJobId,
        ReconcileFileGroupTask.of(
            "plan-1",
            "group-1",
            "table-1",
            55L,
            List.of("s3://bucket/data/file-1.parquet"),
            List.of(ReconcileFileResult.succeeded("s3://bucket/data/file-1.parquet", 3L))));
    store.markSucceeded(execJobId, execLease.leaseEpoch, 300L, 0L, 0L, 0L, 0L, 0L, 1L);

    ReconcileJob snapshot = store.get(ACCOUNT_ID, snapshotJobId).orElseThrow();
    assertEquals("JS_WAITING", snapshot.state);
    assertEquals("Waiting on child work", snapshot.message);
    assertEquals(2L, snapshot.plannedFileGroups);
    assertEquals(1L, snapshot.completedFileGroups);

    ReconcileJob listedWaiting =
        store.list(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
            .filter(job -> snapshotJobId.equals(job.jobId))
            .findFirst()
            .orElseThrow();
    assertEquals("JS_WAITING", listedWaiting.state);
    assertEquals("Waiting on child work", listedWaiting.message);
    assertEquals(2L, listedWaiting.plannedFileGroups);
    assertEquals(1L, listedWaiting.completedFileGroups);

    ReconcileJobStore.LeasedJob execLease2 = leaseJob(execJobId2, ReconcileJobKind.EXEC_FILE_GROUP);
    store.markRunning(execJobId2, execLease2.leaseEpoch, 350L, "executor-exec");
    store.persistFileGroupResult(
        execJobId2,
        ReconcileFileGroupTask.of(
            "plan-1",
            "group-2",
            "table-1",
            55L,
            List.of("s3://bucket/data/file-2.parquet"),
            List.of(ReconcileFileResult.succeeded("s3://bucket/data/file-2.parquet", 4L))));
    store.markSucceeded(execJobId2, execLease2.leaseEpoch, 400L, 0L, 0L, 0L, 0L, 0L, 1L);

    ReconcileJob snapshotSucceeded = store.get(ACCOUNT_ID, snapshotJobId).orElseThrow();
    assertEquals("JS_SUCCEEDED", snapshotSucceeded.state);
    assertEquals("Succeeded", snapshotSucceeded.message);
    assertEquals(2L, snapshotSucceeded.plannedFileGroups);
    assertEquals(2L, snapshotSucceeded.completedFileGroups);

    ReconcileJob listedSucceeded =
        store.list(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
            .filter(job -> snapshotJobId.equals(job.jobId))
            .findFirst()
            .orElseThrow();
    assertEquals("JS_SUCCEEDED", listedSucceeded.state);
    assertEquals("Succeeded", listedSucceeded.message);
    assertEquals(2L, listedSucceeded.plannedFileGroups);
    assertEquals(2L, listedSucceeded.completedFileGroups);
  }

  @Test
  void terminalParentCountsAndDurationStayFrozenAfterLateChildContribution() {
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

    ReconcileJob terminalBefore = store.get(ACCOUNT_ID, connectorJobId).orElseThrow();
    assertEquals("JS_SUCCEEDED", terminalBefore.state);
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

    ReconcileJob terminalAfter = store.get(ACCOUNT_ID, connectorJobId).orElseThrow();
    assertEquals("JS_SUCCEEDED", terminalAfter.state);
    assertEquals(200L, terminalAfter.finishedAtMs);
    assertEquals(0L, terminalAfter.tablesScanned);
    assertEquals(0L, terminalAfter.tablesChanged);

    ReconcileJob listedTerminal =
        store.list(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
            .filter(job -> connectorJobId.equals(job.jobId))
            .findFirst()
            .orElseThrow();
    assertEquals("JS_SUCCEEDED", listedTerminal.state);
    assertEquals(200L, listedTerminal.finishedAtMs);
    assertEquals(0L, listedTerminal.tablesScanned);
    assertEquals(0L, listedTerminal.tablesChanged);
  }

  @Test
  void nestedContributionsPropagateFileGroupResultsAndStayOutOfCanonicalState() {
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

    ReconcileJobStore.LeasedJob tableLease = leaseJob(tableJobId);
    store.markRunning(tableJobId, tableLease.leaseEpoch, 100L, "executor-table");
    store.markProgress(
        tableJobId, tableLease.leaseEpoch, 1L, 1L, 0L, 0L, 0L, 0L, 0L, "table progress");

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

    ReconcileJob snapshot = store.get(ACCOUNT_ID, snapshotJobId).orElseThrow();
    ReconcileJob table = store.get(ACCOUNT_ID, tableJobId).orElseThrow();
    ReconcileJob connector = store.get(ACCOUNT_ID, connectorJobId).orElseThrow();

    assertEquals(1L, snapshot.plannedFileGroups);
    assertEquals(2L, snapshot.plannedFiles);
    assertEquals(1L, snapshot.completedFileGroups);
    assertEquals(1L, snapshot.completedFiles);
    assertEquals(1L, snapshot.failedFiles);
    assertEquals(1L, snapshot.indexesProcessed);
    assertEquals(2L, snapshot.statsProcessed);

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
        store.list(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
            .filter(job -> connectorJobId.equals(job.jobId))
            .findFirst()
            .orElseThrow();
    assertEquals(0L, listed.completedFiles);
    assertEquals(0L, listed.failedFiles);
    assertEquals(0L, listed.indexesProcessed);

    Pointer contributionPointer =
        store
            .pointerStore
            .get(Keys.reconcileJobContributionPointer(ACCOUNT_ID, snapshotJobId, execJobId))
            .orElseThrow();
    assertTrue(contributionPointer.getBlobUri().startsWith(INLINE_JOB_CONTRIBUTION_PREFIX));

    var stateJson =
        store.mapper.valueToTree(
            readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, snapshotJobId)));
    assertFalse(stateJson.has("plannedFileGroups"));
    assertFalse(stateJson.has("plannedFiles"));
    assertFalse(stateJson.has("completedFileGroups"));
    assertFalse(stateJson.has("failedFileGroups"));
    assertFalse(stateJson.has("completedFiles"));
    assertFalse(stateJson.has("failedFiles"));
    assertFalse(stateJson.has("indexesProcessed"));
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

  private void withInlineRepairSuppressed(ThrowingRunnable runnable) throws Exception {
    Field suppressField = DurableReconcileJobStore.class.getDeclaredField("suppressInlineRepair");
    suppressField.setAccessible(true);
    @SuppressWarnings("unchecked")
    ThreadLocal<Boolean> suppressInlineRepair = (ThreadLocal<Boolean>) suppressField.get(store);
    Boolean prior = suppressInlineRepair.get();
    suppressInlineRepair.set(true);
    try {
      runnable.run();
    } finally {
      if (Boolean.TRUE.equals(prior)) {
        suppressInlineRepair.set(true);
      } else {
        suppressInlineRepair.remove();
      }
    }
  }

  private Object invokePrivateMethod(String name, Class<?>[] parameterTypes, Object... args)
      throws Exception {
    Method method = DurableReconcileJobStore.class.getDeclaredMethod(name, parameterTypes);
    method.setAccessible(true);
    return method.invoke(store, args);
  }

  private void deletePointerIfPresent(String key) {
    store
        .pointerStore
        .get(key)
        .ifPresent(pointer -> store.pointerStore.compareAndDelete(key, pointer.getVersion()));
  }

  private List<String> statePointerKeysFor(DurableReconcileJobStore.StoredReconcileJob record)
      throws Exception {
    @SuppressWarnings("unchecked")
    List<String> keys =
        (List<String>)
            invokePrivateMethod(
                "statePointerKeys",
                new Class<?>[] {DurableReconcileJobStore.StoredReconcileJob.class},
                record);
    return keys;
  }

  private List<String> readyPointerKeysFor(DurableReconcileJobStore.StoredReconcileJob record)
      throws Exception {
    @SuppressWarnings("unchecked")
    List<String> keys =
        (List<String>)
            invokePrivateMethod(
                "readyPointerKeys",
                new Class<?>[] {DurableReconcileJobStore.StoredReconcileJob.class},
                record);
    return keys;
  }

  private Optional<Pointer> firstPointerWithPrefix(String prefix) {
    StringBuilder next = new StringBuilder();
    List<Pointer> pointers = store.pointerStore.listPointersByPrefix(prefix, 10, "", next);
    return pointers.stream().findFirst();
  }

  private ReconcileJobStore.LeasedJob leaseJob(String jobId) {
    for (int i = 0; i < 16; i++) {
      ReconcileJobStore.LeasedJob lease = store.leaseNext().orElseThrow();
      if (jobId.equals(lease.jobId)) {
        return lease;
      }
    }
    throw new IllegalStateException("Unable to lease job " + jobId);
  }

  private ReconcileJobStore.LeasedJob leaseJob(String jobId, ReconcileJobKind jobKind) {
    for (int i = 0; i < 16; i++) {
      ReconcileJobStore.LeasedJob lease =
          store
              .leaseNext(
                  ReconcileJobStore.LeaseRequest.of(Set.of(), Set.of(), Set.of(), Set.of(jobKind)))
              .orElseThrow();
      if (jobId.equals(lease.jobId)) {
        return lease;
      }
    }
    throw new IllegalStateException("Unable to lease job " + jobId);
  }

  private DurableReconcileJobStore.StoredReconcileJob readStoredRecord(String canonicalPointerKey) {
    Pointer pointer = store.pointerStore.get(canonicalPointerKey).orElseThrow();
    String reference = pointer.getBlobUri();
    assertTrue(reference.startsWith("inline:reconcile-job:"));
    return assertDoesNotThrow(
        () ->
            store.mapper.readValue(
                java.util.Base64.getUrlDecoder()
                    .decode(reference.substring("inline:reconcile-job:".length())),
                DurableReconcileJobStore.StoredReconcileJob.class));
  }

  private DurableReconcileJobStore.StoredJobLease readStoredLease(String reference) {
    assertTrue(reference.startsWith(INLINE_JOB_LEASE_PREFIX));
    return assertDoesNotThrow(
        () ->
            store.mapper.readValue(
                java.util.Base64.getUrlDecoder()
                    .decode(reference.substring(INLINE_JOB_LEASE_PREFIX.length())),
                DurableReconcileJobStore.StoredJobLease.class));
  }

  private com.fasterxml.jackson.databind.JsonNode readBlobJson(String blobUri) {
    return assertDoesNotThrow(() -> store.mapper.readTree(store.blobStore.get(blobUri)));
  }

  private static String leaseExpiryPointerKey(long expiresAtMs, String accountId, String jobId) {
    return String.format(
        "%s%019d/%s/%s",
        LEASE_EXPIRY_POINTER_PREFIX,
        expiresAtMs,
        Keys.encodeSegment(accountId),
        Keys.encodeSegment(jobId));
  }

  private Pointer overwriteCanonicalRecordWithoutSync(
      String canonicalPointerKey, DurableReconcileJobStore.StoredReconcileJob record) {
    Pointer currentPointer = store.pointerStore.get(canonicalPointerKey).orElseThrow();
    Pointer nextPointer =
        Pointer.newBuilder()
            .setKey(canonicalPointerKey)
            .setBlobUri(
                "inline:reconcile-job:"
                    + java.util.Base64.getUrlEncoder()
                        .withoutPadding()
                        .encodeToString(
                            assertDoesNotThrow(() -> store.mapper.writeValueAsBytes(record))))
            .setVersion(currentPointer.getVersion() + 1L)
            .build();
    assertTrue(
        store.pointerStore.compareAndSet(
            canonicalPointerKey, currentPointer.getVersion(), nextPointer));
    return nextPointer;
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

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
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
