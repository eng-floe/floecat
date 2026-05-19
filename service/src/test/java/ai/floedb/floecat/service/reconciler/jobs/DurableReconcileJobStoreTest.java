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
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionClass;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileResult;
import ai.floedb.floecat.reconciler.jobs.ReconcileIndexArtifactResult;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.ReconcileJob;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotSelection;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DurableReconcileJobStoreTest {
  private static final String ACCOUNT_ID = "acct-1";
  private static final String CONNECTOR_ID = "conn-1";
  private static final String CATALOG = "cat-1";
  private static final List<String> NAMESPACE_PATH = List.of("ns");

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

    var children = store.childJobs(ACCOUNT_ID, planJobId);

    assertEquals(1, children.size());
    assertEquals(childJobId, children.get(0).jobId);
    assertEquals(planJobId, children.get(0).parentJobId);
  }

  @Test
  void missingCanonicalLookupReferenceIsTreatedAsAbsentAndClearsLookupPointer() {
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-missing-canonical"));
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    String lookupPointerKey = Keys.reconcileJobLookupPointerById(jobId);

    assertTrue(store.pointerStore.delete(canonicalPointerKey));

    assertTrue(store.get(ACCOUNT_ID, jobId).isEmpty());
    assertTrue(store.pointerStore.get(lookupPointerKey).isEmpty());
  }

  @Test
  void dedupeRepairRestoresMissingParentPointer() {
    store.init();

    String planJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    ReconcileScope childScope = ReconcileScope.of(List.of(), "orders-table-id");
    String childJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            childScope,
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("src.ns", "orders", "orders-table-id", "orders"),
            ReconcileExecutionPolicy.defaults(),
            planJobId,
            "");
    String parentPointerKey = Keys.reconcileJobByParentPointer(ACCOUNT_ID, planJobId, childJobId);

    assertTrue(store.pointerStore.delete(parentPointerKey));
    assertTrue(store.childJobs(ACCOUNT_ID, planJobId).isEmpty());

    String dedupedJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            childScope,
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("src.ns", "orders", "orders-table-id", "orders"),
            ReconcileExecutionPolicy.defaults(),
            planJobId,
            "");

    assertEquals(childJobId, dedupedJobId);
    assertTrue(store.pointerStore.get(parentPointerKey).isPresent());
    assertEquals(
        List.of(childJobId),
        store.childJobs(ACCOUNT_ID, planJobId).stream().map(job -> job.jobId).toList());
  }

  @Test
  void enqueueTablePlanPreservesSnapshotSelectionForStrictTableScope() {
    store.init();

    String childJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_ONLY,
            ReconcileScope.of(
                List.of(),
                null,
                null,
                List.of(),
                ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy.empty(),
                ReconcileSnapshotSelection.current()),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("src.ns", "orders", "orders-table-id", "orders"),
            ReconcileExecutionPolicy.defaults(),
            "parent-1",
            "");

    ReconcileJob job = store.get(childJobId).orElseThrow();

    assertEquals(ReconcileSnapshotSelection.Kind.CURRENT, job.scope.snapshotSelection().kind());
    assertEquals("orders-table-id", job.scope.destinationTableId());
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
        Keys.reconcileJobRefBlobUri(ACCOUNT_ID, jobId),
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

    var children = store.childJobs(ACCOUNT_ID, parentJobId);

    assertEquals(1, children.size());
    assertEquals("JS_RUNNING", children.getFirst().state);
    assertEquals(
        Keys.reconcileJobRefBlobUri(ACCOUNT_ID, childJobId),
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
        Keys.reconcileJobRefBlobUri(ACCOUNT_ID, jobId),
        store.pointerStore.get(lookupKey).orElseThrow().getBlobUri());
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

    var job = store.get(ACCOUNT_ID, jobId).orElseThrow();
    assertEquals(1, job.fileGroupTask.fileResults().size());
    assertEquals(2L, job.fileGroupTask.fileResults().getFirst().statsProcessed());
    assertEquals(
        "s3://bucket/index/file-1.parquet.index",
        job.fileGroupTask.fileResults().getFirst().indexArtifact().artifactUri());
  }

  @Test
  void getReturnsPreAggregatedParentSummaryFromStoredChildren() {
    store.init();

    String parentJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "events", List.of(), true),
            ReconcileExecutionPolicy.defaults(),
            "plan-table-1",
            "");
    String successfulChildJobId =
        store.enqueueFileGroupExecution(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileFileGroupTask.of(
                parentJobId, "group-1", "table-1", 55L, List.of("s3://bucket/data/file-1.parquet")),
            ReconcileExecutionPolicy.defaults(),
            parentJobId,
            "");
    String failedChildJobId =
        store.enqueueFileGroupExecution(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileFileGroupTask.of(
                parentJobId, "group-2", "table-1", 55L, List.of("s3://bucket/data/file-2.parquet")),
            ReconcileExecutionPolicy.defaults(),
            parentJobId,
            "");

    var parentLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, java.util.EnumSet.of(ReconcileJobKind.PLAN_SNAPSHOT)))
            .orElseThrow();
    assertEquals(parentJobId, parentLease.jobId);
    store.markRunning(parentJobId, parentLease.leaseEpoch, System.currentTimeMillis(), "planner-1");
    store.markSucceeded(
        parentJobId, parentLease.leaseEpoch, System.currentTimeMillis(), 0, 0, 0, 0, 0, 0);

    var firstChildLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, java.util.EnumSet.of(ReconcileJobKind.EXEC_FILE_GROUP)))
            .orElseThrow();
    var secondChildLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, java.util.EnumSet.of(ReconcileJobKind.EXEC_FILE_GROUP)))
            .orElseThrow();

    completeSuccessfulFileGroupChild(
        firstChildLease.jobId.equals(successfulChildJobId) ? firstChildLease : secondChildLease,
        parentJobId);
    failFileGroupChild(
        firstChildLease.jobId.equals(failedChildJobId) ? firstChildLease : secondChildLease);

    var parent = store.get(ACCOUNT_ID, parentJobId).orElseThrow();

    assertTrue(parent.aggregateSummaryPresent);
    assertEquals("JS_QUEUED", parent.state);
    assertEquals(1L, parent.errors);
    assertEquals(7L, parent.statsProcessed);
    assertEquals(1L, parent.indexesProcessed);
  }

  @Test
  void getPreservesParentStartedAtWhenChildrenStartLater() {
    store.init();

    String parentJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "events", List.of(), true),
            ReconcileExecutionPolicy.defaults(),
            "plan-table-1",
            "");

    var parentLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, java.util.EnumSet.of(ReconcileJobKind.PLAN_SNAPSHOT)))
            .orElseThrow();
    assertEquals(parentJobId, parentLease.jobId);
    long parentStartedAtMs = store.get(ACCOUNT_ID, parentJobId).orElseThrow().startedAtMs;
    assertTrue(parentStartedAtMs > 0L);
    store.markRunning(parentJobId, parentLease.leaseEpoch, 1_000L, "planner-1");
    store.markSucceeded(parentJobId, parentLease.leaseEpoch, 1_500L, 0L, 0L, 0L, 0L, 0L, 0L);

    String childJobId =
        store.enqueueFileGroupExecution(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileFileGroupTask.of(
                parentJobId, "group-1", "table-1", 55L, List.of("s3://bucket/data/file-1.parquet")),
            ReconcileExecutionPolicy.defaults(),
            parentJobId,
            "");

    var childLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, java.util.EnumSet.of(ReconcileJobKind.EXEC_FILE_GROUP)))
            .orElseThrow();
    assertEquals(childJobId, childLease.jobId);
    store.markRunning(childJobId, childLease.leaseEpoch, 2_000L, "worker-1");

    var parent = store.get(ACCOUNT_ID, parentJobId).orElseThrow();

    assertTrue(parent.aggregateSummaryPresent);
    assertEquals("JS_RUNNING", parent.state);
    assertEquals(parentStartedAtMs, parent.startedAtMs);
    assertEquals(0L, parent.finishedAtMs);
  }

  @Test
  void repeatedChildProgressOverwritesParentCountersInsteadOfAccumulating() {
    store.init();

    String parentJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "events", List.of(), true),
            ReconcileExecutionPolicy.defaults(),
            "plan-table-1",
            "");
    String childJobId =
        store.enqueueFileGroupExecution(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileFileGroupTask.of(
                parentJobId, "group-1", "table-1", 55L, List.of("s3://bucket/data/file-1.parquet")),
            ReconcileExecutionPolicy.defaults(),
            parentJobId,
            "");

    var childLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, java.util.EnumSet.of(ReconcileJobKind.EXEC_FILE_GROUP)))
            .orElseThrow();
    assertEquals(childJobId, childLease.jobId);
    store.markRunning(childJobId, childLease.leaseEpoch, 1_000L, "worker-1");
    store.markProgress(childJobId, childLease.leaseEpoch, 3L, 1L, 0L, 0L, 1L, 0L, 2L, "phase-1");
    store.markProgress(childJobId, childLease.leaseEpoch, 5L, 2L, 0L, 0L, 2L, 0L, 4L, "phase-2");

    var parent = store.get(ACCOUNT_ID, parentJobId).orElseThrow();

    assertEquals("JS_RUNNING", parent.state);
    assertEquals(5L, parent.tablesScanned);
    assertEquals(2L, parent.tablesChanged);
    assertEquals(2L, parent.errors);
    assertEquals(4L, parent.statsProcessed);
  }

  @Test
  void replacingChildFileGroupResultsDoesNotDoubleCountParentArtifacts() {
    store.init();

    String parentJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "events", List.of(), true),
            ReconcileExecutionPolicy.defaults(),
            "plan-table-1",
            "");
    String childJobId =
        store.enqueueFileGroupExecution(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileFileGroupTask.of(
                parentJobId, "group-1", "table-1", 55L, List.of("s3://bucket/data/file-1.parquet")),
            ReconcileExecutionPolicy.defaults(),
            parentJobId,
            "");

    var childLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, java.util.EnumSet.of(ReconcileJobKind.EXEC_FILE_GROUP)))
            .orElseThrow();
    assertEquals(childJobId, childLease.jobId);
    store.markRunning(childJobId, childLease.leaseEpoch, 1_000L, "worker-1");
    store.persistFileGroupResult(
        childJobId,
        ReconcileFileGroupTask.of(
            parentJobId,
            "group-1",
            "table-1",
            55L,
            List.of("s3://bucket/data/file-1.parquet"),
            List.of(
                ReconcileFileResult.succeeded(
                    "s3://bucket/data/file-1.parquet",
                    2L,
                    ReconcileIndexArtifactResult.of(
                        "s3://bucket/index/file-1.idx", "parquet", 1)))));
    store.persistFileGroupResult(
        childJobId,
        ReconcileFileGroupTask.of(
            parentJobId,
            "group-1",
            "table-1",
            55L,
            List.of("s3://bucket/data/file-1.parquet"),
            List.of(ReconcileFileResult.succeeded("s3://bucket/data/file-1.parquet", 5L, null))));
    store.markSucceeded(childJobId, childLease.leaseEpoch, 2_000L, 0L, 0L, 0L, 0L, 0L, 5L);

    var parent = store.get(ACCOUNT_ID, parentJobId).orElseThrow();

    assertEquals(5L, parent.statsProcessed);
    assertEquals(0L, parent.indexesProcessed);
  }

  @Test
  void childProgressRebuildsParentRollupInsteadOfApplyingDeltaToStaleAggregate() {
    store.init();

    String parentJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "events", List.of(), true),
            ReconcileExecutionPolicy.defaults(),
            "plan-table-1",
            "");
    String firstChildJobId =
        store.enqueueFileGroupExecution(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileFileGroupTask.of(
                parentJobId, "group-1", "table-1", 55L, List.of("s3://bucket/data/file-1.parquet")),
            ReconcileExecutionPolicy.defaults(),
            parentJobId,
            "");
    String secondChildJobId =
        store.enqueueFileGroupExecution(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileFileGroupTask.of(
                parentJobId, "group-2", "table-1", 55L, List.of("s3://bucket/data/file-2.parquet")),
            ReconcileExecutionPolicy.defaults(),
            parentJobId,
            "");

    var firstLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, java.util.EnumSet.of(ReconcileJobKind.EXEC_FILE_GROUP)))
            .orElseThrow();
    var secondLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, java.util.EnumSet.of(ReconcileJobKind.EXEC_FILE_GROUP)))
            .orElseThrow();
    assertEquals(
        java.util.Set.of(firstChildJobId, secondChildJobId),
        java.util.Set.of(firstLease.jobId, secondLease.jobId));

    store.markRunning(firstLease.jobId, firstLease.leaseEpoch, 1_000L, "worker-1");
    store.markProgress(
        firstLease.jobId, firstLease.leaseEpoch, 0L, 0L, 0L, 0L, 0L, 0L, 3L, "first");
    store.markRunning(secondLease.jobId, secondLease.leaseEpoch, 1_000L, "worker-2");
    store.markProgress(
        secondLease.jobId, secondLease.leaseEpoch, 0L, 0L, 0L, 0L, 0L, 0L, 7L, "second");

    DurableReconcileJobStore.StoredAggregateRollup staleRollup = storedAggregateRollup(parentJobId);
    staleRollup.statsProcessed = 3L;
    overwriteAggregateRollup(parentJobId, staleRollup);

    store.markProgress(
        secondLease.jobId, secondLease.leaseEpoch, 0L, 0L, 0L, 0L, 0L, 0L, 11L, "second-updated");

    var parent = store.get(ACCOUNT_ID, parentJobId).orElseThrow();

    assertEquals(7L, parent.statsProcessed);
  }

  @Test
  void explicitAggregateRepairRebuildsAfterContributionWriteSucceedsButRollupWriteFails() {
    FailingAggregateRollupBlobStore blobStore = new FailingAggregateRollupBlobStore();
    store.blobStore = blobStore;
    store.init();

    String parentJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "events", List.of(), true),
            ReconcileExecutionPolicy.defaults(),
            "plan-table-1",
            "");
    String childJobId =
        store.enqueueFileGroupExecution(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileFileGroupTask.of(
                parentJobId, "group-1", "table-1", 55L, List.of("s3://bucket/data/file-1.parquet")),
            ReconcileExecutionPolicy.defaults(),
            parentJobId,
            "");

    var childLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, java.util.EnumSet.of(ReconcileJobKind.EXEC_FILE_GROUP)))
            .orElseThrow();
    assertEquals(childJobId, childLease.jobId);
    store.markRunning(childJobId, childLease.leaseEpoch, 1_000L, "worker-1");

    blobStore.failNextAggregateRollupPut();
    IllegalStateException error =
        assertThrows(
            IllegalStateException.class,
            () ->
                store.markProgress(
                    childJobId, childLease.leaseEpoch, 0L, 0L, 0L, 0L, 0L, 0L, 9L, "progress"));
    assertTrue(error.getMessage().contains("aggregate payload"));
    assertTrue(
        store
            .pointerStore
            .get(Keys.reconcileAggregateRollupPointer(ACCOUNT_ID, parentJobId))
            .isPresent());

    var staleParent = store.get(ACCOUNT_ID, parentJobId).orElseThrow();
    assertEquals(0L, staleParent.statsProcessed);

    ensureCurrentAggregateRollup(parentJobId);

    var parent = store.get(ACCOUNT_ID, parentJobId).orElseThrow();

    assertEquals(9L, parent.statsProcessed);
    assertEquals(9L, storedAggregateRollup(parentJobId).statsProcessed);
  }

  @Test
  void terminalChildCompletionRepairsContradictoryRunningAncestorRollup() {
    store.init();

    String connectorJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    String tableJobId =
        store.enqueueTablePlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileTableTask.of("src.ns", "orders", "orders-table-id", "orders"),
            ReconcileExecutionPolicy.defaults(),
            connectorJobId,
            "");

    var connectorLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, java.util.EnumSet.of(ReconcileJobKind.PLAN_CONNECTOR)))
            .orElseThrow();
    store.markRunning(connectorJobId, connectorLease.leaseEpoch, 1_000L, "planner-1");
    store.markSucceeded(connectorJobId, connectorLease.leaseEpoch, 2_000L, 1L, 0L, 0L, 0L, 0L, 0L);

    var tableLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, java.util.EnumSet.of(ReconcileJobKind.PLAN_TABLE)))
            .orElseThrow();
    store.markRunning(tableJobId, tableLease.leaseEpoch, 3_000L, "worker-1");

    DurableReconcileJobStore.StoredAggregateRollup staleRootRollup =
        storedAggregateRollup(connectorJobId);
    staleRootRollup.runningChildren = 2L;
    overwriteAggregateRollup(connectorJobId, staleRootRollup);

    store.markSucceeded(tableJobId, tableLease.leaseEpoch, 4_000L, 0L, 0L, 0L, 0L, 0L, 0L);

    ReconcileJob connectorJob = store.get(ACCOUNT_ID, connectorJobId).orElseThrow();
    DurableReconcileJobStore.StoredAggregateRollup repairedRootRollup =
        storedAggregateRollup(connectorJobId);

    assertEquals("JS_SUCCEEDED", connectorJob.state);
    assertEquals(0L, repairedRootRollup.runningChildren);
    assertEquals(1L, repairedRootRollup.succeededChildren);
  }

  @Test
  void connectorAggregateDoesNotDoubleCountPlannerTableTotals() {
    store.init();

    String connectorJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    String firstTableJobId =
        store.enqueueTablePlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileTableTask.of("src.ns", "orders", "orders-table-id", "orders"),
            ReconcileExecutionPolicy.defaults(),
            connectorJobId,
            "");
    String secondTableJobId =
        store.enqueueTablePlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileTableTask.of("src.ns", "customers", "customers-table-id", "customers"),
            ReconcileExecutionPolicy.defaults(),
            connectorJobId,
            "");

    var connectorLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, java.util.EnumSet.of(ReconcileJobKind.PLAN_CONNECTOR)))
            .orElseThrow();
    assertEquals(connectorJobId, connectorLease.jobId);
    store.markRunning(connectorJobId, connectorLease.leaseEpoch, 1_000L, "planner-1");
    store.markSucceeded(connectorJobId, connectorLease.leaseEpoch, 2_000L, 2L, 0L, 0L, 0L, 0L, 0L);

    completePlanTableSuccessfully(firstTableJobId);
    completePlanTableSuccessfully(secondTableJobId);

    var connectorJob = store.get(ACCOUNT_ID, connectorJobId).orElseThrow();

    assertTrue(connectorJob.aggregateSummaryPresent);
    assertEquals(2L, connectorJob.tablesScanned);
    assertEquals(2L, connectorJob.tablesChanged);
  }

  @Test
  void planTablePublicProjectionPreservesSelfTableProgressCounters() {
    store.init();

    String connectorJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    String tableJobId =
        store.enqueueTablePlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileTableTask.of("src.ns", "orders", "orders-table-id", "orders"),
            ReconcileExecutionPolicy.defaults(),
            connectorJobId,
            "");

    var connectorLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, java.util.EnumSet.of(ReconcileJobKind.PLAN_CONNECTOR)))
            .orElseThrow();
    store.markRunning(connectorJobId, connectorLease.leaseEpoch, 1_000L, "planner-1");
    store.markSucceeded(connectorJobId, connectorLease.leaseEpoch, 2_000L, 1L, 0L, 0L, 0L, 0L, 0L);

    completePlanTableSuccessfully(tableJobId);

    ReconcileJob projectedChild =
        store.childJobs(ACCOUNT_ID, connectorJobId).stream()
            .filter(job -> tableJobId.equals(job.jobId))
            .findFirst()
            .orElseThrow();

    assertEquals(1L, projectedChild.tablesScanned);
    assertEquals(1L, projectedChild.tablesChanged);
  }

  @Test
  void connectorAggregateCountsSuccessfulPlanTablesWithoutDescendantsOrExplicitCounters() {
    store.init();

    String connectorJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());

    java.util.List<String> tableJobIds = new java.util.ArrayList<>();
    for (int i = 0; i < 19; i++) {
      String tableName = "table_" + i;
      tableJobIds.add(
          store.enqueueTablePlan(
              ACCOUNT_ID,
              CONNECTOR_ID,
              false,
              CaptureMode.METADATA_AND_CAPTURE,
              ReconcileScope.empty(),
              ReconcileTableTask.of("src.ns", tableName, "table-id-" + i, tableName),
              ReconcileExecutionPolicy.defaults(),
              connectorJobId,
              ""));
    }

    var connectorLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, java.util.EnumSet.of(ReconcileJobKind.PLAN_CONNECTOR)))
            .orElseThrow();
    store.markRunning(connectorJobId, connectorLease.leaseEpoch, 1_000L, "planner-1");
    store.markSucceeded(connectorJobId, connectorLease.leaseEpoch, 2_000L, 0L, 0L, 0L, 0L, 0L, 0L);

    for (int i = 0; i < tableJobIds.size(); i++) {
      var tableLease =
          store
              .leaseNext(
                  new ReconcileJobStore.LeaseRequest(
                      null, null, null, java.util.EnumSet.of(ReconcileJobKind.PLAN_TABLE)))
              .orElseThrow();
      assertTrue(tableJobIds.contains(tableLease.jobId));
      store.markRunning(
          tableLease.jobId, tableLease.leaseEpoch, System.currentTimeMillis(), "worker-1");
      store.markSucceeded(
          tableLease.jobId,
          tableLease.leaseEpoch,
          System.currentTimeMillis(),
          0L,
          0L,
          0L,
          0L,
          0L,
          0L);
    }

    ReconcileJob connectorJob = store.get(ACCOUNT_ID, connectorJobId).orElseThrow();

    assertEquals(19L, connectorJob.tablesScanned);
    assertEquals(19L, connectorJob.tablesChanged);
  }

  @Test
  void getAndListUsePersistedAggregateRollupsWithoutScanningChildren() {
    store.init();

    String connectorJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    String tableJobId =
        store.enqueueTablePlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileTableTask.of("src.ns", "orders", "orders-table-id", "orders"),
            ReconcileExecutionPolicy.defaults(),
            connectorJobId,
            "");

    var connectorLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, java.util.EnumSet.of(ReconcileJobKind.PLAN_CONNECTOR)))
            .orElseThrow();
    store.markRunning(connectorJobId, connectorLease.leaseEpoch, 1_000L, "planner-1");
    store.markSucceeded(connectorJobId, connectorLease.leaseEpoch, 2_000L, 1L, 0L, 0L, 0L, 0L, 0L);
    completePlanTableSuccessfully(tableJobId);

    DurableReconcileJobStore.StoredAggregateRollup staleRollup =
        storedAggregateRollup(connectorJobId);
    staleRollup.tablesScanned = 91L;
    staleRollup.tablesChanged = 37L;
    overwriteAggregateRollup(connectorJobId, staleRollup);

    store.pointerStore =
        new GuardedParentPointerStore(
            store.pointerStore, Keys.reconcileJobByParentPointerPrefix(ACCOUNT_ID, connectorJobId));

    ReconcileJob fromGet = store.get(ACCOUNT_ID, connectorJobId).orElseThrow();
    ReconcileJob fromList =
        store.list(ACCOUNT_ID, 20, "", CONNECTOR_ID, java.util.Set.of()).jobs.stream()
            .filter(job -> connectorJobId.equals(job.jobId))
            .findFirst()
            .orElseThrow();

    assertEquals(91L, fromGet.tablesScanned);
    assertEquals(37L, fromGet.tablesChanged);
    assertEquals(91L, fromList.tablesScanned);
    assertEquals(37L, fromList.tablesChanged);
  }

  @Test
  void childJobsUsePersistedAggregateRollupsWithoutScanningGrandchildren() {
    store.init();

    String connectorJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    String tableJobId =
        store.enqueueTablePlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileTableTask.of("db", "events", "table-1", "events"),
            ReconcileExecutionPolicy.defaults(),
            connectorJobId,
            "");
    ReconcileFileGroupTask group =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", "table-1", 55L, List.of("s3://bucket/file-1.parquet"));
    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of("table-1", 55L, "db", "events", List.of(group), true);
    String snapshotJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            snapshotTask,
            ReconcileExecutionPolicy.defaults(),
            tableJobId,
            "");
    store.persistSnapshotPlan(snapshotJobId, snapshotTask);

    var connectorLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, java.util.EnumSet.of(ReconcileJobKind.PLAN_CONNECTOR)))
            .orElseThrow();
    store.markRunning(connectorJobId, connectorLease.leaseEpoch, 1_000L, "planner");
    store.markSucceeded(connectorJobId, connectorLease.leaseEpoch, 2_000L, 1L, 0L, 0L, 0L, 0L, 0L);
    completePlanTableSuccessfully(tableJobId);

    var snapshotLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, java.util.EnumSet.of(ReconcileJobKind.PLAN_SNAPSHOT)))
            .orElseThrow();
    store.markRunning(snapshotJobId, snapshotLease.leaseEpoch, 3_000L, "planner");
    store.markSucceeded(snapshotJobId, snapshotLease.leaseEpoch, 4_000L, 0L, 0L, 0L, 0L, 0L, 0L);

    DurableReconcileJobStore.StoredAggregateRollup staleRollup = storedAggregateRollup(tableJobId);
    staleRollup.snapshotsProcessed = 73L;
    overwriteAggregateRollup(tableJobId, staleRollup);

    store.pointerStore =
        new GuardedParentPointerStore(
            store.pointerStore, Keys.reconcileJobByParentPointerPrefix(ACCOUNT_ID, tableJobId));

    ReconcileJob projectedChild =
        store.childJobs(ACCOUNT_ID, connectorJobId).stream()
            .filter(job -> tableJobId.equals(job.jobId))
            .findFirst()
            .orElseThrow();

    assertEquals(74L, projectedChild.snapshotsProcessed);
  }

  private void completeSuccessfulFileGroupChild(
      ReconcileJobStore.LeasedJob childLease, String parentJobId) {
    store.markRunning(
        childLease.jobId, childLease.leaseEpoch, System.currentTimeMillis(), "worker-1");
    store.persistFileGroupResult(
        childLease.jobId,
        ReconcileFileGroupTask.of(
            parentJobId,
            "group-1",
            "table-1",
            55L,
            List.of("s3://bucket/data/file-1.parquet"),
            List.of(
                ReconcileFileResult.succeeded(
                    "s3://bucket/data/file-1.parquet",
                    7L,
                    ReconcileIndexArtifactResult.of(
                        "s3://bucket/index/file-1.parquet.index", "parquet", 1)))));
    store.markSucceeded(
        childLease.jobId, childLease.leaseEpoch, System.currentTimeMillis(), 0, 0, 0, 0, 0, 7);
  }

  private void failFileGroupChild(ReconcileJobStore.LeasedJob childLease) {
    store.markRunning(
        childLease.jobId, childLease.leaseEpoch, System.currentTimeMillis(), "worker-2");
    store.markFailed(
        childLease.jobId,
        childLease.leaseEpoch,
        System.currentTimeMillis(),
        "boom",
        0,
        0,
        0,
        0,
        1,
        0,
        0);
  }

  private void completePlanTableSuccessfully(String jobId) {
    var lease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, java.util.EnumSet.of(ReconcileJobKind.PLAN_TABLE)))
            .orElseThrow();
    store.markRunning(lease.jobId, lease.leaseEpoch, System.currentTimeMillis(), "worker-1");
    store.markSucceeded(
        lease.jobId, lease.leaseEpoch, System.currentTimeMillis(), 1L, 1L, 0L, 0L, 1L, 0L);
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
        ReconcileScope.of(List.of(), "default-table"),
        ReconcileExecutionPolicy.defaults(),
        "");
    String remoteJobId =
        store.enqueue(
            ACCOUNT_ID,
            "conn-remote",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "remote-table"),
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
        ReconcileScope.of(List.of(), "remote-a-table"),
        ReconcileExecutionPolicy.of(ReconcileExecutionClass.HEAVY, "remote", java.util.Map.of()),
        "remote-a");
    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            "conn-b",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "remote-b-table"),
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
        ReconcileScope.of(List.of(), "pinned-table"),
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
  void staleLaneLeasePointerStillBlocksConcurrentLease() throws Exception {
    store.init();
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String firstJob =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, firstJob);
    Pointer initialCanonical = store.pointerStore.get(canonicalPointerKey).orElseThrow();
    var firstLease = store.leaseNext().orElseThrow();

    Pointer currentCanonical = store.pointerStore.get(canonicalPointerKey).orElseThrow();
    DurableReconcileJobStore.StoredReconcileJob currentJob =
        store.mapper.readValue(
            store.blobStore.get(currentCanonical.getBlobUri()),
            DurableReconcileJobStore.StoredReconcileJob.class);
    String lanePointerKey = Keys.reconcileLaneLeasePointer(ACCOUNT_ID, currentJob.laneKey);
    Pointer lanePointer = store.pointerStore.get(lanePointerKey).orElseThrow();
    assertTrue(
        store.pointerStore.compareAndSet(
            lanePointerKey,
            lanePointer.getVersion(),
            Pointer.newBuilder()
                .setKey(lanePointerKey)
                .setBlobUri(initialCanonical.getBlobUri())
                .setVersion(lanePointer.getVersion() + 1)
                .build()));

    store.enqueue(ACCOUNT_ID, "conn-b", false, CaptureMode.CAPTURE_ONLY, scope);

    assertTrue(store.leaseNext().isEmpty());

    store.markSucceeded(firstJob, firstLease.leaseEpoch, System.currentTimeMillis(), 1, 1, 1, 1);
    assertEquals("conn-b", store.leaseNext().orElseThrow().connectorId);
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
    assertNotEquals(initialCanonical.getBlobUri(), currentCanonical.getBlobUri());

    DurableReconcileJobStore.StoredReconcileJob activeRecord =
        assertDoesNotThrow(
            () ->
                store.mapper.readValue(
                    store.blobStore.get(currentCanonical.getBlobUri()),
                    DurableReconcileJobStore.StoredReconcileJob.class));

    String lanePointerKey = Keys.reconcileLaneLeasePointer(ACCOUNT_ID, activeRecord.laneKey);
    Pointer lanePointer = store.pointerStore.get(lanePointerKey).orElseThrow();
    assertTrue(
        store.pointerStore.compareAndSet(
            lanePointerKey,
            lanePointer.getVersion(),
            Pointer.newBuilder()
                .setKey(lanePointerKey)
                .setBlobUri(initialCanonical.getBlobUri())
                .setVersion(lanePointer.getVersion() + 1)
                .build()));

    Method clearLaneLeaseIfOwned =
        DurableReconcileJobStore.class.getDeclaredMethod(
            "clearLaneLeaseIfOwned",
            DurableReconcileJobStore.StoredReconcileJob.class,
            String.class);
    clearLaneLeaseIfOwned.setAccessible(true);

    assertDoesNotThrow(
        () -> clearLaneLeaseIfOwned.invoke(store, activeRecord, initialCanonical.getBlobUri()));

    Pointer repairedPointer = store.pointerStore.get(lanePointerKey).orElseThrow();
    assertEquals(Keys.reconcileJobRefBlobUri(ACCOUNT_ID, jobId), repairedPointer.getBlobUri());
  }

  @Test
  void tryAcquireLaneLeaseRejectsDuplicateClaimForQueuedBlob() throws Exception {
    store.init();
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String jobId =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    Pointer canonicalPointer = store.pointerStore.get(canonicalPointerKey).orElseThrow();
    DurableReconcileJobStore.StoredReconcileJob queuedRecord =
        assertDoesNotThrow(
            () ->
                store.mapper.readValue(
                    store.blobStore.get(canonicalPointer.getBlobUri()),
                    DurableReconcileJobStore.StoredReconcileJob.class));

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
                store, queuedRecord, canonicalPointer.getBlobUri(), System.currentTimeMillis());
    boolean secondClaim =
        (boolean)
            tryAcquireLaneLease.invoke(
                store, queuedRecord, canonicalPointer.getBlobUri(), System.currentTimeMillis());

    assertTrue(firstClaim);
    assertTrue(secondClaim);
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
    Pointer initialCanonical = store.pointerStore.get(canonicalPointerKey).orElseThrow();
    var firstLease = store.leaseNext().orElseThrow();

    String snapshotLeasePointerKey = Keys.reconcileSnapshotLeasePointer(ACCOUNT_ID, "table-1", 55L);
    Pointer snapshotLeasePointer = store.pointerStore.get(snapshotLeasePointerKey).orElseThrow();
    assertTrue(
        store.pointerStore.compareAndSet(
            snapshotLeasePointerKey,
            snapshotLeasePointer.getVersion(),
            Pointer.newBuilder()
                .setKey(snapshotLeasePointerKey)
                .setBlobUri(initialCanonical.getBlobUri())
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
    assertNotEquals(initialCanonical.getBlobUri(), currentCanonical.getBlobUri());

    String snapshotLeasePointerKey = Keys.reconcileSnapshotLeasePointer(ACCOUNT_ID, "table-1", 55L);
    Pointer snapshotLeasePointer = store.pointerStore.get(snapshotLeasePointerKey).orElseThrow();
    assertTrue(
        store.pointerStore.compareAndSet(
            snapshotLeasePointerKey,
            snapshotLeasePointer.getVersion(),
            Pointer.newBuilder()
                .setKey(snapshotLeasePointerKey)
                .setBlobUri(initialCanonical.getBlobUri())
                .setVersion(snapshotLeasePointer.getVersion() + 1)
                .build()));

    DurableReconcileJobStore.StoredReconcileJob activeRecord =
        assertDoesNotThrow(
            () ->
                store.mapper.readValue(
                    store.blobStore.get(currentCanonical.getBlobUri()),
                    DurableReconcileJobStore.StoredReconcileJob.class));

    Method clearSnapshotLeaseIfOwned =
        DurableReconcileJobStore.class.getDeclaredMethod(
            "clearSnapshotLeaseIfOwned",
            DurableReconcileJobStore.StoredReconcileJob.class,
            String.class);
    clearSnapshotLeaseIfOwned.setAccessible(true);

    assertDoesNotThrow(
        () -> clearSnapshotLeaseIfOwned.invoke(store, activeRecord, initialCanonical.getBlobUri()));

    Pointer repairedPointer = store.pointerStore.get(snapshotLeasePointerKey).orElseThrow();
    assertEquals(Keys.reconcileJobRefBlobUri(ACCOUNT_ID, jobId), repairedPointer.getBlobUri());
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
    assertEquals(firstJob, firstLease.jobId);

    assertTrue(store.leaseNext().isEmpty());

    store.markSucceeded(firstJob, firstLease.leaseEpoch, System.currentTimeMillis(), 1, 1, 1, 1);

    var secondLease = store.leaseNext().orElseThrow();
    assertEquals(secondJob, secondLease.jobId);
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
  void snapshotPlanLaneIncludesSnapshotId() throws Exception {
    store.init();

    String firstJobId =
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
    String secondJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileSnapshotTask.of("table-1", 56L, "db", "events"),
            ReconcileExecutionPolicy.defaults(),
            "parent-b",
            "");

    DurableReconcileJobStore.StoredReconcileJob first =
        store.mapper.readValue(
            store.blobStore.get(
                store
                    .pointerStore
                    .get(Keys.reconcileJobPointerById(ACCOUNT_ID, firstJobId))
                    .orElseThrow()
                    .getBlobUri()),
            DurableReconcileJobStore.StoredReconcileJob.class);
    DurableReconcileJobStore.StoredReconcileJob second =
        store.mapper.readValue(
            store.blobStore.get(
                store
                    .pointerStore
                    .get(Keys.reconcileJobPointerById(ACCOUNT_ID, secondJobId))
                    .orElseThrow()
                    .getBlobUri()),
            DurableReconcileJobStore.StoredReconcileJob.class);

    assertEquals("snapshot-plan|table-1|55", first.laneKey);
    assertEquals("snapshot-plan|table-1|56", second.laneKey);
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
  void markRunningTransitionsLeasedQueuedJobToRunning() {
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    var lease = store.leaseNext().orElseThrow();

    store.markRunning(jobId, lease.leaseEpoch, System.currentTimeMillis(), "remote-executor");

    var running = store.get(jobId).orElseThrow();
    assertEquals("JS_RUNNING", running.state);
    assertEquals("remote-executor", running.executorId);
    assertTrue(running.startedAtMs > 0L);

    store.markSucceeded(jobId, lease.leaseEpoch, System.currentTimeMillis(), 1, 0, 0, 0, 0, 0);

    var completed = store.get(jobId).orElseThrow();
    assertEquals("JS_SUCCEEDED", completed.state);
  }

  @Test
  void enqueueRepairsMissingSecondaryPointersForDedupedQueuedJob() {
    store.init();
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String jobId =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    var canonicalPointer = store.pointerStore.get(canonicalPointerKey).orElseThrow();
    DurableReconcileJobStore.StoredReconcileJob job =
        assertDoesNotThrow(
            () ->
                store.mapper.readValue(
                    store.blobStore.get(canonicalPointer.getBlobUri()),
                    DurableReconcileJobStore.StoredReconcileJob.class));
    String expectedLaneQueueKey =
        Keys.reconcileLaneQueuePointerByDue(
            job.nextAttemptAtMs, job.accountId, job.laneKey, job.jobId);
    String expectedRunnableKey =
        Keys.reconcileRunnableLanePointerByDue(
            job.nextAttemptAtMs, job.accountId, job.laneKey, job.jobId);
    job.readyPointerKey = "";
    job.laneQueuePointerKey = "";
    overwriteCanonicalRecordWithoutSync(canonicalPointerKey, job);

    String lookupKey = Keys.reconcileJobLookupPointerById(jobId);
    var lookupPointer = store.pointerStore.get(lookupKey).orElseThrow();
    assertTrue(store.pointerStore.compareAndDelete(lookupKey, lookupPointer.getVersion()));

    var laneQueuePointer = store.pointerStore.get(expectedLaneQueueKey).orElseThrow();
    assertTrue(
        store.pointerStore.compareAndDelete(expectedLaneQueueKey, laneQueuePointer.getVersion()));
    var runnablePointer = store.pointerStore.get(expectedRunnableKey).orElseThrow();
    assertTrue(
        store.pointerStore.compareAndDelete(expectedRunnableKey, runnablePointer.getVersion()));

    Pointer dedupePointer =
        firstPointerWithPrefix(Keys.reconcileDedupePointerPrefix(ACCOUNT_ID)).orElseThrow();
    assertTrue(
        store.pointerStore.compareAndSet(
            dedupePointer.getKey(),
            dedupePointer.getVersion(),
            Pointer.newBuilder()
                .setKey(dedupePointer.getKey())
                .setBlobUri(canonicalPointer.getBlobUri())
                .setVersion(dedupePointer.getVersion() + 1)
                .build()));

    String dedupedJobId =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);

    assertEquals(jobId, dedupedJobId);
    assertTrue(store.pointerStore.get(lookupKey).isPresent());
    assertTrue(store.pointerStore.get(expectedLaneQueueKey).isPresent());
    assertTrue(store.pointerStore.get(expectedRunnableKey).isPresent());
    Pointer repairedCanonical = store.pointerStore.get(canonicalPointerKey).orElseThrow();
    Pointer repairedDedupePointer =
        firstPointerWithPrefix(Keys.reconcileDedupePointerPrefix(ACCOUNT_ID)).orElseThrow();
    assertEquals(
        Keys.reconcileJobRefBlobUri(ACCOUNT_ID, jobId), repairedDedupePointer.getBlobUri());
    DurableReconcileJobStore.StoredReconcileJob repairedJob =
        assertDoesNotThrow(
            () ->
                store.mapper.readValue(
                    store.blobStore.get(repairedCanonical.getBlobUri()),
                    DurableReconcileJobStore.StoredReconcileJob.class));
    assertEquals(expectedLaneQueueKey, repairedJob.laneQueuePointerKey);

    var lease = store.leaseNext().orElseThrow();
    assertEquals(jobId, lease.jobId);
  }

  @Test
  void enqueuePreservesDedupeOwnershipWhenCanonicalPointerBlobIsMissing() {
    store.init();
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String jobId =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);

    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    Pointer canonicalPointer = store.pointerStore.get(canonicalPointerKey).orElseThrow();
    DurableReconcileJobStore.StoredReconcileJob job =
        assertDoesNotThrow(
            () ->
                store.mapper.readValue(
                    store.blobStore.get(canonicalPointer.getBlobUri()),
                    DurableReconcileJobStore.StoredReconcileJob.class));
    Pointer advancedCanonicalPointer =
        overwriteCanonicalRecordWithoutSync(canonicalPointerKey, job);
    assertTrue(store.blobStore.delete(advancedCanonicalPointer.getBlobUri()));

    String dedupedJobId =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);

    assertNotEquals("", dedupedJobId);
    assertTrue(store.pointerStore.get(canonicalPointerKey).isPresent());
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
  void listFiltersByProjectedAggregateStateForPlanJobs() {
    store.init();

    String parentJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "events", List.of(), true),
            ReconcileExecutionPolicy.defaults(),
            "plan-table-1",
            "");
    String childJobId =
        store.enqueueFileGroupExecution(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileFileGroupTask.of(
                parentJobId, "group-1", "table-1", 55L, List.of("s3://bucket/data/file-1.parquet")),
            ReconcileExecutionPolicy.defaults(),
            parentJobId,
            "");

    var childLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, java.util.EnumSet.of(ReconcileJobKind.EXEC_FILE_GROUP)))
            .orElseThrow();
    assertEquals(childJobId, childLease.jobId);
    store.markRunning(childJobId, childLease.leaseEpoch, 1_000L, "worker-1");

    var runningPage = store.list(ACCOUNT_ID, 20, "", "", java.util.Set.of("JS_RUNNING"));

    assertTrue(runningPage.jobs.stream().anyMatch(job -> parentJobId.equals(job.jobId)));
    assertEquals(
        "JS_RUNNING",
        runningPage.jobs.stream()
            .filter(job -> parentJobId.equals(job.jobId))
            .findFirst()
            .orElseThrow()
            .state);
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
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    var firstLease = store.leaseNext();
    assertTrue(firstLease.isPresent());
    assertEquals(jobId, firstLease.get().jobId);

    Thread.sleep(1150L);
    store.reclaimNowForTesting();

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
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    var firstLease = store.leaseNext().orElseThrow();
    store.markRunning(
        firstLease.jobId, firstLease.leaseEpoch, System.currentTimeMillis(), "default_reconciler");
    store.cancel(ACCOUNT_ID, jobId, "stop");
    assertEquals("JS_CANCELLING", store.get(jobId).orElseThrow().state);

    Thread.sleep(1150L);
    store.reclaimNowForTesting();
    DurableReconcileJobStore.StoredReconcileJob reclaimed = storedJob(jobId);
    assertEquals("JS_CANCELLING", reclaimed.state);
    assertEquals("JS_CANCELLING", store.get(jobId).orElseThrow().state);
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
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    var firstLease = store.leaseNext().orElseThrow();
    store.markRunning(
        firstLease.jobId, firstLease.leaseEpoch, System.currentTimeMillis(), "default_reconciler");
    store.cancel(ACCOUNT_ID, jobId, "stop");
    assertFalse(store.leaseNext().isPresent());

    // Cancel should poke lease expiry, allowing reclaim well before the original 5s lease.
    Thread.sleep(1300L);
    store.reclaimNowForTesting();
    DurableReconcileJobStore.StoredReconcileJob reclaimed = storedJob(jobId);
    assertEquals("JS_CANCELLING", reclaimed.state);
    assertEquals("JS_CANCELLING", store.get(jobId).orElseThrow().state);
    var stillCancelling = store.get(jobId).orElseThrow();
    assertEquals("JS_CANCELLING", stillCancelling.state);
    assertEquals("stop", stillCancelling.message);
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
  void renewLeaseExtendsLeaseAndDelaysReclaim() throws Exception {
    System.setProperty("floecat.reconciler.job-store.lease-ms", "2000");
    System.setProperty("floecat.reconciler.job-store.reclaim-interval-ms", "200");
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

    Thread.sleep(500L);
    assertTrue(store.renewLease(jobId, lease.leaseEpoch));

    Thread.sleep(700L);
    store.reclaimNowForTesting();
    assertTrue(store.leaseNext().isEmpty());

    Thread.sleep(1600L);
    store.reclaimNowForTesting();
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
  void leaseNextRereadsCanonicalRecordBeforeLeasing() {
    MutatingPointerStore pointerStore = new MutatingPointerStore();
    store.pointerStore = pointerStore;
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-reread"));
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);

    pointerStore.mutateOnNthGet(
        canonicalPointerKey,
        2,
        () -> {
          DurableReconcileJobStore.StoredReconcileJob current = storedJob(jobId);
          current.lastError = "newer-canonical-payload";
          current.tablesChanged = 7L;
          overwriteCanonicalRecordWithoutSync(canonicalPointerKey, current);
        });

    var lease = store.leaseNext().orElseThrow();

    assertEquals(jobId, lease.jobId);
    DurableReconcileJobStore.StoredReconcileJob leased = storedJob(jobId);
    assertEquals("newer-canonical-payload", leased.lastError);
    assertEquals(7L, leased.tablesChanged);
  }

  @Test
  void renewLeaseRejectsOldEpochAfterReclaimAndReacquire() throws Exception {
    System.setProperty("floecat.reconciler.job-store.lease-ms", "25");
    System.setProperty("floecat.reconciler.job-store.reclaim-interval-ms", "1");
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-reclaim"));
    var firstLease = store.leaseNext().orElseThrow();
    store.markRunning(
        firstLease.jobId, firstLease.leaseEpoch, System.currentTimeMillis(), "default_reconciler");

    Thread.sleep(40L);
    store.reclaimNowForTesting();

    assertTrue(store.renewLease(jobId, firstLease.leaseEpoch));
    assertTrue(store.leaseNext().isEmpty());
  }

  @Test
  void waitingJobsProjectAsWaitingAndAreRequeuedForDependencyRecheck() throws Exception {
    System.setProperty("floecat.reconciler.job-store.base-backoff-ms", "100");
    System.setProperty("floecat.reconciler.job-store.max-backoff-ms", "100");
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-waiting"));
    var lease = store.leaseNext().orElseThrow();
    store.markWaiting(
        jobId,
        lease.leaseEpoch,
        System.currentTimeMillis(),
        "waiting on dependency",
        0,
        0,
        0,
        0,
        1,
        0,
        0);

    ReconcileJob job = store.get(jobId).orElseThrow();
    assertEquals("JS_WAITING", job.state);
    assertTrue(job.message.contains("waiting on dependency"));
    assertTrue(store.leaseNext().isEmpty());
    assertEquals(0L, store.queueStats().queued);

    DurableReconcileJobStore.StoredReconcileJob stored = storedJob(jobId);
    assertEquals("JS_WAITING", stored.state);
    assertFalse(stored.waitingOnDependency);
    assertTrue(stored.nextAttemptAtMs > 0L);
    assertTrue(store.pointerStore.get(stored.laneQueuePointerKey).isPresent());

    Thread.sleep(150L);
    assertEquals(jobId, store.leaseNext().orElseThrow().jobId);
  }

  @Test
  void legacyQueuedWaitingRecordsProjectAsWaitingAndFilterByWaitingState() {
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-legacy-waiting"));
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    DurableReconcileJobStore.StoredReconcileJob legacy = storedJob(jobId);
    legacy.state = "JS_QUEUED";
    legacy.waitingOnDependency = true;
    legacy.message = "legacy waiting";
    legacy.laneQueuePointerKey = null;
    overwriteCanonicalRecordWithoutSync(canonicalPointerKey, legacy);

    ReconcileJob getJob = store.get(jobId).orElseThrow();
    var waitingJobs = store.list(ACCOUNT_ID, 50, "", CONNECTOR_ID, java.util.Set.of("JS_WAITING"));

    assertEquals("JS_WAITING", getJob.state);
    assertEquals(List.of(jobId), waitingJobs.jobs.stream().map(job -> job.jobId).toList());
    assertTrue(store.leaseNext().isEmpty());
    assertEquals(0L, store.queueStats().queued);
  }

  @Test
  void waitingJobsDoNotReupsertLaneQueuePointersOrBlockSameLaneWithStaleLeaseArtifacts()
      throws Exception {
    store.init();

    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl-waiting-stale");
    String firstJob =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);
    var firstLease = store.leaseNext().orElseThrow();
    store.markWaiting(
        firstJob,
        firstLease.leaseEpoch,
        System.currentTimeMillis(),
        "waiting on dependency",
        0,
        0,
        0,
        0,
        1,
        0,
        0);

    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, firstJob);
    Pointer previousPointer = store.pointerStore.get(canonicalPointerKey).orElseThrow();
    DurableReconcileJobStore.StoredReconcileJob previous = storedJob(firstJob);
    String staleLaneQueuePointerKey =
        Keys.reconcileLaneQueuePointerByDue(
            0L, previous.accountId, previous.laneKey, previous.jobId);
    DurableReconcileJobStore.StoredReconcileJob waitingWithStaleArtifacts =
        store.mapper.convertValue(previous, DurableReconcileJobStore.StoredReconcileJob.class);
    waitingWithStaleArtifacts.laneQueuePointerKey = staleLaneQueuePointerKey;
    waitingWithStaleArtifacts.leaseOwner = "stale-owner";
    waitingWithStaleArtifacts.leaseEpoch = "stale-epoch";
    waitingWithStaleArtifacts.leaseExpiresAtMs = System.currentTimeMillis() + 60_000L;
    Pointer currentPointer =
        overwriteCanonicalRecordWithoutSync(canonicalPointerKey, waitingWithStaleArtifacts);

    Method reconcileIndexPointers =
        DurableReconcileJobStore.class.getDeclaredMethod(
            "reconcileIndexPointers",
            DurableReconcileJobStore.StoredReconcileJob.class,
            DurableReconcileJobStore.StoredReconcileJob.class,
            String.class,
            String.class);
    reconcileIndexPointers.setAccessible(true);
    assertDoesNotThrow(
        () ->
            reconcileIndexPointers.invoke(
                store,
                previous,
                waitingWithStaleArtifacts,
                previousPointer.getBlobUri(),
                currentPointer.getBlobUri()));
    assertTrue(store.pointerStore.get(staleLaneQueuePointerKey).isPresent());

    installManualLeaseState(
        firstJob, "stale-owner", "stale-epoch", System.currentTimeMillis() + 60_000L);
    upsertReferencePointer(
        Keys.reconcileLaneLeasePointer(ACCOUNT_ID, waitingWithStaleArtifacts.laneKey),
        Keys.reconcileJobRefBlobUri(ACCOUNT_ID, firstJob));

    String secondJob = store.enqueue(ACCOUNT_ID, "conn-2", false, CaptureMode.CAPTURE_ONLY, scope);
    var secondLease = store.leaseNext().orElseThrow();

    assertEquals(firstJob, secondLease.jobId);
  }

  @Test
  void execFileGroupPublicProjectionPreservesSelfCompletionCounters() {
    store.init();

    ReconcileFileGroupTask group =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", "table-1", 55L, List.of("s3://bucket/file-1.parquet"));
    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of("table-1", 55L, "db", "events", List.of(group), true);

    String snapshotPlanJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            snapshotTask,
            ReconcileExecutionPolicy.defaults(),
            "table-plan-1",
            "");
    store.persistSnapshotPlan(snapshotPlanJobId, snapshotTask);

    String fileGroupJobId =
        store.enqueueFileGroupExecution(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            group,
            ReconcileExecutionPolicy.defaults(),
            snapshotPlanJobId,
            "");
    var fileGroupLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, java.util.EnumSet.of(ReconcileJobKind.EXEC_FILE_GROUP)))
            .orElseThrow();
    store.persistFileGroupResult(
        fileGroupJobId,
        group.withFileResults(
            List.of(ReconcileFileResult.succeeded("s3://bucket/file-1.parquet", 1L))));
    store.markSucceeded(
        fileGroupJobId,
        fileGroupLease.leaseEpoch,
        System.currentTimeMillis(),
        0L,
        0L,
        0L,
        0L,
        0L,
        1L);

    ReconcileJob projectedChild =
        store.childJobs(ACCOUNT_ID, snapshotPlanJobId).stream()
            .filter(job -> fileGroupJobId.equals(job.jobId))
            .findFirst()
            .orElseThrow();
    ReconcileJob projectedParent = store.get(snapshotPlanJobId).orElseThrow();

    assertEquals(1L, projectedChild.plannedFileGroups);
    assertEquals(1L, projectedChild.completedFileGroups);
    assertEquals(1L, projectedChild.plannedFiles);
    assertEquals(1L, projectedChild.completedFiles);
    assertEquals(projectedParent.plannedFileGroups, projectedParent.completedFileGroups);
  }

  @Test
  void readsRebuildMissingAggregateRollups() {
    store.init();

    ReconcileFileGroupTask group =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", "table-1", 55L, List.of("s3://bucket/file-1.parquet"));
    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of("table-1", 55L, "db", "events", List.of(group), true);

    String snapshotPlanJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            snapshotTask,
            ReconcileExecutionPolicy.defaults(),
            "table-plan-1",
            "");
    store.persistSnapshotPlan(snapshotPlanJobId, snapshotTask);

    var snapshotLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, java.util.EnumSet.of(ReconcileJobKind.PLAN_SNAPSHOT)))
            .orElseThrow();
    store.markRunning(snapshotPlanJobId, snapshotLease.leaseEpoch, 1_000L, "planner");
    store.markSucceeded(
        snapshotPlanJobId, snapshotLease.leaseEpoch, 2_000L, 0L, 0L, 0L, 0L, 1L, 0L);

    String fileGroupJobId =
        store.enqueueFileGroupExecution(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            group,
            ReconcileExecutionPolicy.defaults(),
            snapshotPlanJobId,
            "");
    var fileGroupLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, java.util.EnumSet.of(ReconcileJobKind.EXEC_FILE_GROUP)))
            .orElseThrow();
    store.persistFileGroupResult(
        fileGroupJobId,
        group.withFileResults(
            List.of(ReconcileFileResult.succeeded("s3://bucket/file-1.parquet", 1L))));
    store.markSucceeded(fileGroupJobId, fileGroupLease.leaseEpoch, 3_000L, 0L, 0L, 0L, 0L, 0L, 1L);

    String rollupPointerKey = Keys.reconcileAggregateRollupPointer(ACCOUNT_ID, snapshotPlanJobId);
    Pointer rollupPointer = store.pointerStore.get(rollupPointerKey).orElseThrow();
    assertTrue(store.blobStore.delete(rollupPointer.getBlobUri()));

    ReconcileJob fromGet = store.get(snapshotPlanJobId).orElseThrow();
    ReconcileJob fromList =
        store.list(ACCOUNT_ID, 20, "", CONNECTOR_ID, java.util.Set.of()).jobs.stream()
            .filter(job -> snapshotPlanJobId.equals(job.jobId))
            .findFirst()
            .orElseThrow();

    assertEquals(1L, fromGet.snapshotsProcessed);
    assertEquals(1L, fromGet.plannedFileGroups);
    assertEquals(1L, fromGet.plannedFiles);
    assertEquals(1L, fromGet.completedFileGroups);
    assertEquals(1L, fromGet.completedFiles);
    assertEquals(1L, fromList.snapshotsProcessed);
    assertEquals(1L, fromList.plannedFileGroups);
    assertEquals(1L, fromList.plannedFiles);
    assertEquals(1L, fromList.completedFileGroups);
    assertEquals(1L, fromList.completedFiles);
  }

  @Test
  void explicitAggregateRepairRebuildsStaleFileGroupContributionsFromChildRecords() {
    store.init();

    ReconcileFileGroupTask group =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", "table-1", 55L, List.of("s3://bucket/file-1.parquet"));
    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of("table-1", 55L, "db", "events", List.of(group), true);

    String snapshotPlanJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            snapshotTask,
            ReconcileExecutionPolicy.defaults(),
            "table-plan-1",
            "");
    store.persistSnapshotPlan(snapshotPlanJobId, snapshotTask);

    String fileGroupJobId =
        store.enqueueFileGroupExecution(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            group,
            ReconcileExecutionPolicy.defaults(),
            snapshotPlanJobId,
            "");
    var fileGroupLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, java.util.EnumSet.of(ReconcileJobKind.EXEC_FILE_GROUP)))
            .orElseThrow();
    store.persistFileGroupResult(
        fileGroupJobId,
        group.withFileResults(
            List.of(ReconcileFileResult.succeeded("s3://bucket/file-1.parquet", 1L))));
    store.markSucceeded(
        fileGroupJobId,
        fileGroupLease.leaseEpoch,
        System.currentTimeMillis(),
        0L,
        0L,
        0L,
        0L,
        0L,
        1L);

    DurableReconcileJobStore.StoredAggregateContribution staleContribution =
        storedAggregateContribution(snapshotPlanJobId, fileGroupJobId);
    staleContribution.plannedFileGroups = 0L;
    staleContribution.plannedFiles = 0L;
    staleContribution.completedFileGroups = 0L;
    staleContribution.completedFiles = 0L;
    overwriteAggregateContribution(snapshotPlanJobId, fileGroupJobId, staleContribution);

    DurableReconcileJobStore.StoredAggregateRollup staleRollup =
        storedAggregateRollup(snapshotPlanJobId);
    staleRollup.plannedFileGroups = 0L;
    staleRollup.plannedFiles = 0L;
    staleRollup.completedFileGroups = 0L;
    staleRollup.completedFiles = 0L;
    overwriteAggregateRollup(snapshotPlanJobId, staleRollup);

    ensureCurrentAggregateRollup(snapshotPlanJobId);

    ReconcileJob repairedParent = store.get(snapshotPlanJobId).orElseThrow();
    DurableReconcileJobStore.StoredAggregateRollup repairedRollup =
        storedAggregateRollup(snapshotPlanJobId);
    DurableReconcileJobStore.StoredAggregateContribution repairedContribution =
        storedAggregateContribution(snapshotPlanJobId, fileGroupJobId);

    assertEquals(1L, repairedParent.plannedFileGroups);
    assertEquals(1L, repairedParent.completedFileGroups);
    assertEquals(1L, repairedParent.plannedFiles);
    assertEquals(1L, repairedParent.completedFiles);
    assertEquals(1L, repairedRollup.completedFileGroups);
    assertEquals(1L, repairedContribution.completedFileGroups);
  }

  @Test
  void explicitTopLevelAggregateRepairRecursivelyRepairsStaleChildRollups() {
    store.init();

    String connectorJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    String tableJobId =
        store.enqueueTablePlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileTableTask.of("db", "events", "table-1", "events"),
            ReconcileExecutionPolicy.defaults(),
            connectorJobId,
            "");

    ReconcileFileGroupTask group =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", "table-1", 55L, List.of("s3://bucket/file-1.parquet"));
    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of("table-1", 55L, "db", "events", List.of(group), true);
    String snapshotJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            snapshotTask,
            ReconcileExecutionPolicy.defaults(),
            tableJobId,
            "");
    store.persistSnapshotPlan(snapshotJobId, snapshotTask);

    var connectorLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, java.util.EnumSet.of(ReconcileJobKind.PLAN_CONNECTOR)))
            .orElseThrow();
    store.markRunning(
        connectorJobId, connectorLease.leaseEpoch, System.currentTimeMillis(), "planner");
    store.markSucceeded(
        connectorJobId,
        connectorLease.leaseEpoch,
        System.currentTimeMillis(),
        1L,
        0L,
        0L,
        0L,
        0L,
        0L);

    completePlanTableSuccessfully(tableJobId);

    var snapshotLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, java.util.EnumSet.of(ReconcileJobKind.PLAN_SNAPSHOT)))
            .orElseThrow();
    store.markRunning(
        snapshotJobId, snapshotLease.leaseEpoch, System.currentTimeMillis(), "planner");
    store.markSucceeded(
        snapshotJobId,
        snapshotLease.leaseEpoch,
        System.currentTimeMillis(),
        0L,
        0L,
        0L,
        0L,
        0L,
        0L);

    String fileGroupJobId =
        store.enqueueFileGroupExecution(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            group,
            ReconcileExecutionPolicy.defaults(),
            snapshotJobId,
            "");
    var fileGroupLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, java.util.EnumSet.of(ReconcileJobKind.EXEC_FILE_GROUP)))
            .orElseThrow();
    store.persistFileGroupResult(
        fileGroupJobId,
        group.withFileResults(
            List.of(ReconcileFileResult.succeeded("s3://bucket/file-1.parquet", 1L))));
    store.markSucceeded(
        fileGroupJobId,
        fileGroupLease.leaseEpoch,
        System.currentTimeMillis(),
        0L,
        0L,
        0L,
        0L,
        0L,
        1L);

    assertEquals(1L, store.get(connectorJobId).orElseThrow().statsProcessed);

    DurableReconcileJobStore.StoredAggregateContribution staleSnapshotContribution =
        storedAggregateContribution(tableJobId, snapshotJobId);
    staleSnapshotContribution.statsProcessed = 0L;
    staleSnapshotContribution.completedFileGroups = 0L;
    staleSnapshotContribution.completedFiles = 0L;
    overwriteAggregateContribution(tableJobId, snapshotJobId, staleSnapshotContribution);

    DurableReconcileJobStore.StoredAggregateRollup staleSnapshotRollup =
        storedAggregateRollup(snapshotJobId);
    staleSnapshotRollup.statsProcessed = 0L;
    staleSnapshotRollup.completedFileGroups = 0L;
    staleSnapshotRollup.completedFiles = 0L;
    overwriteAggregateRollup(snapshotJobId, staleSnapshotRollup);

    DurableReconcileJobStore.StoredAggregateContribution staleTableContribution =
        storedAggregateContribution(connectorJobId, tableJobId);
    staleTableContribution.statsProcessed = 0L;
    overwriteAggregateContribution(connectorJobId, tableJobId, staleTableContribution);

    DurableReconcileJobStore.StoredAggregateRollup staleTableRollup =
        storedAggregateRollup(tableJobId);
    staleTableRollup.statsProcessed = 0L;
    overwriteAggregateRollup(tableJobId, staleTableRollup);

    ensureCurrentAggregateRollup(connectorJobId);

    ReconcileJob repairedConnector = store.get(connectorJobId).orElseThrow();
    ReconcileJob repairedTable = store.get(tableJobId).orElseThrow();
    ReconcileJob repairedSnapshot = store.get(snapshotJobId).orElseThrow();

    assertEquals(1L, repairedSnapshot.completedFileGroups);
    assertEquals(1L, repairedSnapshot.completedFiles);
    assertEquals(1L, repairedTable.statsProcessed);
    assertEquals(1L, repairedConnector.statsProcessed);
  }

  @Test
  void waitingSnapshotJobsDoNotBlockSameSnapshotWithStaleSnapshotLeaseArtifacts() {
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
    var firstLease = store.leaseNext().orElseThrow();
    store.markWaiting(
        firstJob,
        firstLease.leaseEpoch,
        System.currentTimeMillis(),
        "waiting for snapshot file groups",
        0,
        0,
        0,
        0,
        1,
        0,
        0);

    DurableReconcileJobStore.StoredReconcileJob waitingJob = storedJob(firstJob);
    installManualLeaseState(
        firstJob, "stale-owner", "stale-epoch", System.currentTimeMillis() + 60_000L);
    upsertReferencePointer(
        Keys.reconcileSnapshotLeasePointer(ACCOUNT_ID, "table-1", 55L),
        Keys.reconcileJobRefBlobUri(ACCOUNT_ID, firstJob));
    upsertReferencePointer(
        Keys.reconcileLaneLeasePointer(ACCOUNT_ID, waitingJob.laneKey),
        Keys.reconcileJobRefBlobUri(ACCOUNT_ID, firstJob));

    String secondJob =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            "conn-2",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "events"),
            ReconcileExecutionPolicy.defaults(),
            "parent-b",
            "");

    var secondLease = store.leaseNext().orElseThrow();
    assertEquals(secondJob, secondLease.jobId);
  }

  @Test
  void queuedJobsAreStillLeaseableWhenStaleLeaseStateRemains() {
    store.init();

    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl-queued-stale-lease");
    String jobId =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);
    DurableReconcileJobStore.StoredReconcileJob queued = storedJob(jobId);
    long expiry = System.currentTimeMillis() + 60_000L;
    installManualLeaseState(jobId, "stale-owner", "stale-epoch", expiry);

    var lease = store.leaseNext().orElseThrow();

    assertEquals(jobId, lease.jobId);
    assertNotEquals("stale-epoch", lease.leaseEpoch);
    assertEquals("JS_QUEUED", queued.state);
  }

  @Test
  void failedLeaseCasCleanupDoesNotDeleteNewerLeaseState() {
    LeaseStateRacingPointerStore pointerStore = new LeaseStateRacingPointerStore();
    store.pointerStore = pointerStore;
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-lease-race"));
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    pointerStore.failNextCanonicalLeaseCas(
        canonicalPointerKey, () -> installReplacementLeaseState(jobId));

    store.leaseNext();

    DurableReconcileJobStore.StoredLeaseState leaseState = storedLeaseState(jobId);
    assertFalse(leaseState.leaseEpoch == null || leaseState.leaseEpoch.isBlank());
    assertEquals(ACCOUNT_ID, leaseState.accountId);
    assertEquals(jobId, leaseState.jobId);
    assertTrue(
        store
            .pointerStore
            .get(
                Keys.reconcileLeaseExpiryPointerByDue(
                    leaseState.leaseExpiresAtMs, ACCOUNT_ID, jobId))
            .isPresent());
  }

  @Test
  void reclaimDoesNotClobberRenewedLeaseWhenOlderExpiryPointerIsDue() throws Exception {
    System.setProperty("floecat.reconciler.job-store.lease-ms", "25");
    System.setProperty("floecat.reconciler.job-store.reclaim-interval-ms", "1");
    store.init();

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "tbl-reclaim-renew-race"));
    var lease = store.leaseNext().orElseThrow();
    store.markRunning(jobId, lease.leaseEpoch, System.currentTimeMillis(), "worker-1");
    installReplacementLeaseState(jobId);

    Thread.sleep(40L);
    store.reclaimNowForTesting();

    DurableReconcileJobStore.StoredReconcileJob record = storedJob(jobId);
    DurableReconcileJobStore.StoredLeaseState leaseState = storedLeaseState(jobId);
    assertEquals("JS_RUNNING", record.state);
    assertEquals("replacement-epoch", leaseState.leaseEpoch);
    assertTrue(store.leaseNext().isEmpty());
  }

  @Test
  void snapshotZeroIsDistinctFromEmptyAndUsesAccountScopedLeaseKey() {
    store.init();

    assertEquals(-1L, ReconcileSnapshotTask.empty().snapshotId());
    assertEquals(-1L, ReconcileFileGroupTask.empty().snapshotId());
    assertTrue(ReconcileSnapshotTask.of("table-1", 0L, "db", "events").snapshotId() == 0L);
    assertFalse(ReconcileSnapshotTask.of("table-1", 0L, "db", "events").isEmpty());
    assertFalse(
        ReconcileFileGroupTask.of("plan-0", "group-0", "table-1", 0L, List.of("f")).isEmpty());

    String jobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileSnapshotTask.of("table-1", 0L, "db", "events"),
            ReconcileExecutionPolicy.defaults(),
            "parent-0",
            "");

    ReconcileJob job = store.get(jobId).orElseThrow();
    assertEquals(0L, job.snapshotTask.snapshotId());
    assertEquals("snapshot-plan|table-1|0", storedJob(jobId).laneKey);

    var lease = store.leaseNext().orElseThrow();
    assertEquals(jobId, lease.jobId);
    assertTrue(
        store
            .pointerStore
            .get(Keys.reconcileSnapshotLeasePointer(ACCOUNT_ID, "table-1", 0L))
            .isPresent());
  }

  @Test
  void snapshotLeasesDoNotCollideAcrossAccounts() {
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
    String secondJob =
        store.enqueueSnapshotPlan(
            "acct-2",
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "events"),
            ReconcileExecutionPolicy.defaults(),
            "parent-b",
            "");

    var firstLease = store.leaseNext().orElseThrow();
    var secondLease = store.leaseNext().orElseThrow();

    assertEquals(
        java.util.Set.of(firstJob, secondJob),
        java.util.Set.of(firstLease.jobId, secondLease.jobId));
    assertTrue(
        store
            .pointerStore
            .get(Keys.reconcileSnapshotLeasePointer(ACCOUNT_ID, "table-1", 55L))
            .isPresent());
    assertTrue(
        store
            .pointerStore
            .get(Keys.reconcileSnapshotLeasePointer("acct-2", "table-1", 55L))
            .isPresent());
  }

  @Test
  void parseDueMillisAcceptsReadyKeyWithoutLeadingSlash() throws Exception {
    store.init();
    Method parseDueMillis =
        DurableReconcileJobStore.class.getDeclaredMethod("parseDueMillis", String.class);
    parseDueMillis.setAccessible(true);

    long dueAt = 123456789L;
    String canonical = Keys.reconcileRunnableLanePointerByDue(dueAt, ACCOUNT_ID, "lane", "job-1");
    String normalized = canonical.substring(1);

    long parsedCanonical = (long) parseDueMillis.invoke(store, canonical);
    long parsedNormalized = (long) parseDueMillis.invoke(store, normalized);

    assertEquals(dueAt, parsedCanonical);
    assertEquals(dueAt, parsedNormalized);
  }

  @Test
  void queueStatsReflectQueuedRunningAndCancellingJobs() {
    store.init();
    ReconcileScope queuedScope = ReconcileScope.of(List.of(), "tbl-q");
    ReconcileScope runningScope = ReconcileScope.of(List.of(), "tbl-r");
    ReconcileScope cancellingScope = ReconcileScope.of(List.of(), "tbl-c");

    store.enqueue(ACCOUNT_ID, "conn-q", false, CaptureMode.METADATA_AND_CAPTURE, queuedScope);
    store.enqueue(ACCOUNT_ID, "conn-r", false, CaptureMode.METADATA_AND_CAPTURE, runningScope);
    store.enqueue(ACCOUNT_ID, "conn-c", false, CaptureMode.METADATA_AND_CAPTURE, cancellingScope);

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
   * propagating through reclaimExpiredLeasesIfDue().
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
        ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, ReconcileScope.empty());
    // Take the first lease so the job is removed from the ready queue.
    // leaseNext() short-circuits via leaseReadyDue() here — reclaim is NOT triggered yet.
    store.leaseNext().orElseThrow();

    // Replace blobStore with one that throws StorageNotFoundException on every get().
    // This simulates the S3 "not found" case reported in the bug.
    store.blobStore = new ThrowingOnGetBlobStore();

    Thread.sleep(50L); // ensure lease-ms and reclaim-interval-ms have elapsed

    // Background reclaim iterates lookup pointers and calls readRecordByBlobUri()
    // -> blobStore.get() -> throws.
    // Before the fix: StorageNotFoundException escapes reclaim and can break dispatch.
    // After the fix: readRecordByBlobUri() catches it and returns Optional.empty().
    assertDoesNotThrow(() -> store.reclaimNowForTesting());
    assertTrue(store.leaseNext().isEmpty());
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
        11,
        blobStore.getCount(),
        "metacat currently rereads candidate and lane state multiple times on the happy path");
  }

  @Test
  void leaseNextSkipsCanonicalPointerLookupsForFilteredReadyCandidates() {
    CountingPointerStore pointerStore = new CountingPointerStore();
    CountingBlobStore blobStore = new CountingBlobStore();
    store.pointerStore = pointerStore;
    store.blobStore = blobStore;
    System.setProperty(
        "floecat.reconciler.job-store.reclaim-interval-ms", Long.toString(Long.MAX_VALUE));
    store.init();

    store.enqueue(
        ACCOUNT_ID,
        "conn-remote-1",
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.of(List.of(), "remote-filter-1"),
        ReconcileExecutionPolicy.of(ReconcileExecutionClass.HEAVY, "remote", java.util.Map.of()),
        "");
    store.enqueue(
        ACCOUNT_ID,
        "conn-remote-2",
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.of(List.of(), "remote-filter-2"),
        ReconcileExecutionPolicy.of(ReconcileExecutionClass.HEAVY, "remote", java.util.Map.of()),
        "");

    pointerStore.resetCounts();
    blobStore.resetGetCount();

    assertTrue(
        store
            .leaseNext(
                ReconcileJobStore.LeaseRequest.of(
                    java.util.EnumSet.of(ReconcileExecutionClass.DEFAULT), java.util.Set.of("")))
            .isEmpty());
    assertEquals(
        2,
        pointerStore.getCount(),
        "metacat filters these candidates without extra canonical pointer rereads");
    assertEquals(4, blobStore.getCount(), "metacat currently reads four runnable-head blobs here");
  }

  @Test
  void leaseNextStopsReadyPaginationAtFirstFutureDuePointer() {
    CountingPointerStore pointerStore = new CountingPointerStore();
    store.pointerStore = pointerStore;
    System.setProperty("floecat.reconciler.job-store.base-backoff-ms", "60000");
    System.setProperty("floecat.reconciler.job-store.ready-scan-limit", "1");
    System.setProperty(
        "floecat.reconciler.job-store.reclaim-interval-ms", Long.toString(Long.MAX_VALUE));
    store.init();

    String firstJobId =
        store.enqueue(
            ACCOUNT_ID,
            "conn-future-1",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "future-1"));
    String secondJobId =
        store.enqueue(
            ACCOUNT_ID,
            "conn-future-2",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "future-2"));

    var firstLease = store.leaseNext().orElseThrow();
    store.markWaiting(
        firstLease.jobId,
        firstLease.leaseEpoch,
        System.currentTimeMillis(),
        "retry",
        0,
        0,
        0,
        0,
        0,
        0,
        0);
    var secondLease = store.leaseNext().orElseThrow();
    store.markWaiting(
        secondLease.jobId,
        secondLease.leaseEpoch,
        System.currentTimeMillis(),
        "retry",
        0,
        0,
        0,
        0,
        0,
        0,
        0);

    assertEquals(firstJobId, firstLease.jobId);
    assertEquals(secondJobId, secondLease.jobId);

    pointerStore.resetCounts();

    assertTrue(store.leaseNext().isEmpty());
    assertEquals(1, pointerStore.listCount(), "future-ready scan should stop after the first page");
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

  private static final class FailingAggregateRollupBlobStore extends InMemoryBlobStore {
    private volatile boolean failNextAggregateRollupPut;

    void failNextAggregateRollupPut() {
      failNextAggregateRollupPut = true;
    }

    @Override
    public void put(String uri, byte[] bytes, String contentType) {
      if (failNextAggregateRollupPut && uri.contains("/reconcile/aggregates/rollups/")) {
        failNextAggregateRollupPut = false;
        throw new IllegalStateException("simulated aggregate rollup write failure");
      }
      super.put(uri, bytes, contentType);
    }
  }

  private static final class CountingPointerStore extends InMemoryPointerStore {
    private final AtomicInteger getCount = new AtomicInteger();
    private final AtomicInteger listCount = new AtomicInteger();

    @Override
    public Optional<Pointer> get(String key) {
      getCount.incrementAndGet();
      return super.get(key);
    }

    @Override
    public List<Pointer> listPointersByPrefix(
        String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
      listCount.incrementAndGet();
      return super.listPointersByPrefix(prefix, limit, pageToken, nextTokenOut);
    }

    int getCount() {
      return getCount.get();
    }

    int listCount() {
      return listCount.get();
    }

    void resetCounts() {
      getCount.set(0);
      listCount.set(0);
    }
  }

  private static final class GuardedParentPointerStore
      implements ai.floedb.floecat.storage.spi.PointerStore {
    private final ai.floedb.floecat.storage.spi.PointerStore delegate;
    private final String forbiddenPrefix;

    GuardedParentPointerStore(
        ai.floedb.floecat.storage.spi.PointerStore delegate, String forbiddenPrefix) {
      this.delegate = delegate;
      this.forbiddenPrefix = forbiddenPrefix;
    }

    @Override
    public Optional<Pointer> get(String key) {
      return delegate.get(key);
    }

    @Override
    public boolean compareAndSet(String key, long expectedVersion, Pointer next) {
      return delegate.compareAndSet(key, expectedVersion, next);
    }

    @Override
    public boolean delete(String key) {
      return delegate.delete(key);
    }

    @Override
    public boolean compareAndDelete(String key, long expectedVersion) {
      return delegate.compareAndDelete(key, expectedVersion);
    }

    @Override
    public boolean compareAndSetBatch(List<ai.floedb.floecat.storage.spi.PointerStore.CasOp> ops) {
      return delegate.compareAndSetBatch(ops);
    }

    @Override
    public List<Pointer> listPointersByPrefix(
        String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
      if (prefix.equals(forbiddenPrefix)) {
        throw new AssertionError("unexpected child traversal for prefix " + prefix);
      }
      return delegate.listPointersByPrefix(prefix, limit, pageToken, nextTokenOut);
    }

    @Override
    public int deleteByPrefix(String prefix) {
      return delegate.deleteByPrefix(prefix);
    }

    @Override
    public int countByPrefix(String prefix) {
      return delegate.countByPrefix(prefix);
    }

    @Override
    public boolean isEmpty() {
      return delegate.isEmpty();
    }

    @Override
    public void dump(String header) {
      delegate.dump(header);
    }
  }

  private static final class MutatingPointerStore extends InMemoryPointerStore {
    private String targetKey;
    private int triggerCount;
    private int currentCount;
    private Runnable mutation;

    void mutateOnNthGet(String key, int nthGet, Runnable mutation) {
      this.targetKey = key;
      this.triggerCount = nthGet;
      this.currentCount = 0;
      this.mutation = mutation;
    }

    @Override
    public Optional<Pointer> get(String key) {
      if (mutation != null && key.equals(targetKey)) {
        currentCount++;
        if (currentCount == triggerCount) {
          Runnable currentMutation = mutation;
          mutation = null;
          currentMutation.run();
        }
      }
      return super.get(key);
    }
  }

  private static final class LeaseStateRacingPointerStore extends InMemoryPointerStore {
    private String targetCanonicalPointerKey;
    private Runnable raceMutation;

    void failNextCanonicalLeaseCas(String canonicalPointerKey, Runnable raceMutation) {
      this.targetCanonicalPointerKey = canonicalPointerKey;
      this.raceMutation = raceMutation;
    }

    @Override
    public boolean compareAndSet(String key, long version, Pointer next) {
      if (raceMutation != null && key.equals(targetCanonicalPointerKey)) {
        Runnable currentRaceMutation = raceMutation;
        raceMutation = null;
        currentRaceMutation.run();
        return false;
      }
      return super.compareAndSet(key, version, next);
    }
  }

  private static ReconcileScope.ScopedCaptureRequest scopedCaptureRequest(
      String tableId, long snapshotId, String targetSpec, List<String> columnSelectors) {
    return new ReconcileScope.ScopedCaptureRequest(
        tableId, snapshotId, targetSpec, columnSelectors);
  }

  private Optional<Pointer> firstPointerWithPrefix(String prefix) {
    StringBuilder next = new StringBuilder();
    List<Pointer> pointers = store.pointerStore.listPointersByPrefix(prefix, 10, "", next);
    return pointers.stream().findFirst();
  }

  private DurableReconcileJobStore.StoredReconcileJob storedJob(String jobId) {
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    Pointer canonicalPointer = store.pointerStore.get(canonicalPointerKey).orElseThrow();
    return assertDoesNotThrow(
        () ->
            store.mapper.readValue(
                store.blobStore.get(canonicalPointer.getBlobUri()),
                DurableReconcileJobStore.StoredReconcileJob.class));
  }

  private DurableReconcileJobStore.StoredLeaseState storedLeaseState(String jobId) {
    String pointerKey = Keys.reconcileLeaseStatePointer(ACCOUNT_ID, jobId);
    Pointer pointer = store.pointerStore.get(pointerKey).orElseThrow();
    return assertDoesNotThrow(
        () ->
            store.mapper.readValue(
                store.blobStore.get(pointer.getBlobUri()),
                DurableReconcileJobStore.StoredLeaseState.class));
  }

  private DurableReconcileJobStore.StoredAggregateRollup storedAggregateRollup(String jobId) {
    String rollupPointerKey = Keys.reconcileAggregateRollupPointer(ACCOUNT_ID, jobId);
    Pointer rollupPointer = store.pointerStore.get(rollupPointerKey).orElseThrow();
    return assertDoesNotThrow(
        () ->
            store.mapper.readValue(
                store.blobStore.get(rollupPointer.getBlobUri()),
                DurableReconcileJobStore.StoredAggregateRollup.class));
  }

  private DurableReconcileJobStore.StoredAggregateContribution storedAggregateContribution(
      String parentJobId, String childJobId) {
    String pointerKey =
        Keys.reconcileAggregateContributionPointer(ACCOUNT_ID, parentJobId, childJobId);
    Pointer pointer = store.pointerStore.get(pointerKey).orElseThrow();
    return assertDoesNotThrow(
        () ->
            store.mapper.readValue(
                store.blobStore.get(pointer.getBlobUri()),
                DurableReconcileJobStore.StoredAggregateContribution.class));
  }

  private Pointer overwriteAggregateRollup(
      String jobId, DurableReconcileJobStore.StoredAggregateRollup rollup) {
    String rollupPointerKey = Keys.reconcileAggregateRollupPointer(ACCOUNT_ID, jobId);
    Pointer currentPointer = store.pointerStore.get(rollupPointerKey).orElseThrow();
    String nextBlobUri =
        Keys.reconcileAggregateRollupBlobUri(ACCOUNT_ID, jobId, "test-manual-" + UUID.randomUUID());
    assertDoesNotThrow(
        () ->
            store.blobStore.put(
                nextBlobUri,
                store.mapper.writeValueAsBytes(rollup),
                "application/json; charset=UTF-8"));
    Pointer nextPointer =
        Pointer.newBuilder()
            .setKey(rollupPointerKey)
            .setBlobUri(nextBlobUri)
            .setVersion(currentPointer.getVersion() + 1L)
            .build();
    assertTrue(
        store.pointerStore.compareAndSet(
            rollupPointerKey, currentPointer.getVersion(), nextPointer));
    return nextPointer;
  }

  private Pointer overwriteAggregateContribution(
      String parentJobId,
      String childJobId,
      DurableReconcileJobStore.StoredAggregateContribution contribution) {
    String pointerKey =
        Keys.reconcileAggregateContributionPointer(ACCOUNT_ID, parentJobId, childJobId);
    Pointer currentPointer = store.pointerStore.get(pointerKey).orElseThrow();
    String nextBlobUri =
        Keys.reconcileAggregateContributionBlobUri(
            ACCOUNT_ID, parentJobId, childJobId, "test-manual-" + UUID.randomUUID());
    assertDoesNotThrow(
        () ->
            store.blobStore.put(
                nextBlobUri,
                store.mapper.writeValueAsBytes(contribution),
                "application/json; charset=UTF-8"));
    Pointer nextPointer =
        Pointer.newBuilder()
            .setKey(pointerKey)
            .setBlobUri(nextBlobUri)
            .setVersion(currentPointer.getVersion() + 1L)
            .build();
    assertTrue(
        store.pointerStore.compareAndSet(pointerKey, currentPointer.getVersion(), nextPointer));
    return nextPointer;
  }

  private void ensureCurrentAggregateRollup(String jobId) {
    DurableReconcileJobStore.StoredReconcileJob job = storedJob(jobId);
    assertDoesNotThrow(
        () -> {
          Method method =
              DurableReconcileJobStore.class.getDeclaredMethod(
                  "ensureCurrentAggregateRollup",
                  DurableReconcileJobStore.StoredReconcileJob.class);
          method.setAccessible(true);
          method.invoke(store, job);
        });
  }

  private Pointer overwriteCanonicalRecordWithoutSync(
      String canonicalPointerKey, DurableReconcileJobStore.StoredReconcileJob record) {
    Pointer currentPointer = store.pointerStore.get(canonicalPointerKey).orElseThrow();
    String nextBlobUri =
        Keys.reconcileJobBlobUri(
            record.accountId, record.jobId, "test-manual-" + UUID.randomUUID());
    assertDoesNotThrow(
        () ->
            store.blobStore.put(
                nextBlobUri,
                store.mapper.writeValueAsBytes(record),
                "application/json; charset=UTF-8"));
    Pointer nextPointer =
        Pointer.newBuilder()
            .setKey(canonicalPointerKey)
            .setBlobUri(nextBlobUri)
            .setVersion(currentPointer.getVersion() + 1L)
            .build();
    assertTrue(
        store.pointerStore.compareAndSet(
            canonicalPointerKey, currentPointer.getVersion(), nextPointer));
    return nextPointer;
  }

  private void installReplacementLeaseState(String jobId) {
    String pointerKey = Keys.reconcileLeaseStatePointer(ACCOUNT_ID, jobId);
    Pointer currentPointer = store.pointerStore.get(pointerKey).orElseThrow();
    DurableReconcileJobStore.StoredLeaseState leaseState = storedLeaseState(jobId);
    String referenceBlobUri = Keys.reconcileJobRefBlobUri(ACCOUNT_ID, jobId);
    leaseState.leaseOwner = "replacement-owner";
    leaseState.leaseEpoch = "replacement-epoch";
    leaseState.leaseExpiresAtMs = leaseState.leaseExpiresAtMs + 5_000L;
    leaseState.updatedAtMs = leaseState.updatedAtMs + 1L;
    String nextBlobUri =
        Keys.reconcileLeaseStateBlobUri(ACCOUNT_ID, jobId, "test-replacement-" + UUID.randomUUID());
    assertDoesNotThrow(
        () ->
            store.blobStore.put(
                nextBlobUri,
                store.mapper.writeValueAsBytes(leaseState),
                "application/json; charset=UTF-8"));
    Pointer nextPointer =
        Pointer.newBuilder()
            .setKey(pointerKey)
            .setBlobUri(nextBlobUri)
            .setVersion(currentPointer.getVersion() + 1L)
            .build();
    assertTrue(
        store.pointerStore.compareAndSet(pointerKey, currentPointer.getVersion(), nextPointer));
    String replacementExpiryKey =
        Keys.reconcileLeaseExpiryPointerByDue(leaseState.leaseExpiresAtMs, ACCOUNT_ID, jobId);
    Pointer replacementExpiryPointer =
        Pointer.newBuilder()
            .setKey(replacementExpiryKey)
            .setBlobUri(referenceBlobUri)
            .setVersion(1L)
            .build();
    assertTrue(
        store.pointerStore.compareAndSet(replacementExpiryKey, 0L, replacementExpiryPointer));
  }

  private void installManualLeaseState(
      String jobId, String leaseOwner, String leaseEpoch, long leaseExpiresAtMs) {
    String pointerKey = Keys.reconcileLeaseStatePointer(ACCOUNT_ID, jobId);
    Optional<Pointer> currentPointer = store.pointerStore.get(pointerKey);
    DurableReconcileJobStore.StoredLeaseState leaseState =
        new DurableReconcileJobStore.StoredLeaseState();
    leaseState.accountId = ACCOUNT_ID;
    leaseState.jobId = jobId;
    leaseState.leaseOwner = leaseOwner;
    leaseState.leaseEpoch = leaseEpoch;
    leaseState.leaseExpiresAtMs = leaseExpiresAtMs;
    leaseState.updatedAtMs = System.currentTimeMillis();
    String nextBlobUri =
        Keys.reconcileLeaseStateBlobUri(ACCOUNT_ID, jobId, "test-manual-" + UUID.randomUUID());
    assertDoesNotThrow(
        () ->
            store.blobStore.put(
                nextBlobUri,
                store.mapper.writeValueAsBytes(leaseState),
                "application/json; charset=UTF-8"));
    Pointer nextPointer =
        Pointer.newBuilder()
            .setKey(pointerKey)
            .setBlobUri(nextBlobUri)
            .setVersion(currentPointer.map(pointer -> pointer.getVersion() + 1L).orElse(1L))
            .build();
    assertTrue(
        store.pointerStore.compareAndSet(
            pointerKey, currentPointer.map(Pointer::getVersion).orElse(0L), nextPointer));
  }

  private void upsertReferencePointer(String pointerKey, String blobUri) {
    Optional<Pointer> currentPointer = store.pointerStore.get(pointerKey);
    Pointer nextPointer =
        Pointer.newBuilder()
            .setKey(pointerKey)
            .setBlobUri(blobUri)
            .setVersion(currentPointer.map(pointer -> pointer.getVersion() + 1L).orElse(1L))
            .build();
    assertTrue(
        store.pointerStore.compareAndSet(
            pointerKey, currentPointer.map(Pointer::getVersion).orElse(0L), nextPointer));
  }
}
