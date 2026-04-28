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
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
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
                    List.of("s3://bucket/data/file-1.parquet")))));

    var job = store.get(ACCOUNT_ID, jobId).orElseThrow();
    assertEquals(1, job.snapshotTask.fileGroups().size());
    assertEquals(
        "s3://bucket/data/file-1.parquet",
        job.snapshotTask.fileGroups().getFirst().filePaths().getFirst());
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
    assertEquals(currentCanonical.getBlobUri(), repairedPointer.getBlobUri());
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

    String snapshotLeasePointerKey = Keys.reconcileSnapshotLeasePointer("table-1", 55L);
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

    String snapshotLeasePointerKey = Keys.reconcileSnapshotLeasePointer("table-1", 55L);
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
    assertEquals(currentCanonical.getBlobUri(), repairedPointer.getBlobUri());
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
                .setBlobUri(canonicalPointer.getBlobUri())
                .setVersion(dedupePointer.getVersion() + 1)
                .build()));

    String dedupedJobId =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);

    assertEquals(jobId, dedupedJobId);
    assertTrue(store.pointerStore.get(lookupKey).isPresent());
    assertTrue(store.pointerStore.get(expectedReadyKey).isPresent());
    Pointer repairedCanonical = store.pointerStore.get(canonicalPointerKey).orElseThrow();
    Pointer repairedDedupePointer =
        firstPointerWithPrefix(Keys.reconcileDedupePointerPrefix(ACCOUNT_ID)).orElseThrow();
    assertEquals(repairedCanonical.getBlobUri(), repairedDedupePointer.getBlobUri());
    DurableReconcileJobStore.StoredReconcileJob repairedJob =
        assertDoesNotThrow(
            () ->
                store.mapper.readValue(
                    store.blobStore.get(repairedCanonical.getBlobUri()),
                    DurableReconcileJobStore.StoredReconcileJob.class));
    assertEquals(expectedReadyKey, repairedJob.readyPointerKey);

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

    assertEquals(jobId, dedupedJobId);
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

    var secondLease = store.leaseNext();
    assertTrue(secondLease.isPresent());
    assertEquals(jobId, secondLease.get().jobId);
  }

  @Test
  void leaseNextRepairsCanonicalJobWhenLookupAndReadyPointersAreMissing() {
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
    DurableReconcileJobStore.StoredReconcileJob record =
        assertDoesNotThrow(
            () ->
                store.mapper.readValue(
                    store.blobStore.get(canonicalPointer.getBlobUri()),
                    DurableReconcileJobStore.StoredReconcileJob.class));
    String lookupKey = Keys.reconcileJobLookupPointerById(jobId);
    String readyKey = record.readyPointerKey;

    Pointer lookupPointer = store.pointerStore.get(lookupKey).orElseThrow();
    assertTrue(store.pointerStore.compareAndDelete(lookupKey, lookupPointer.getVersion()));
    Pointer readyPointer = store.pointerStore.get(readyKey).orElseThrow();
    assertTrue(store.pointerStore.compareAndDelete(readyKey, readyPointer.getVersion()));

    var repairedLease = store.leaseNext().orElseThrow();

    assertEquals(jobId, repairedLease.jobId);
    assertTrue(store.pointerStore.get(lookupKey).isPresent());
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
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    var firstLease = store.leaseNext().orElseThrow();
    store.markRunning(
        firstLease.jobId, firstLease.leaseEpoch, System.currentTimeMillis(), "default_reconciler");
    store.cancel(ACCOUNT_ID, jobId, "stop");
    assertFalse(store.leaseNext().isPresent());

    // Cancel should poke lease expiry, allowing reclaim well before the original 5s lease.
    Thread.sleep(1300L);

    var secondLease = store.leaseNext().orElseThrow();
    assertEquals(jobId, secondLease.jobId);
    assertEquals("JS_CANCELLING", store.get(jobId).orElseThrow().state);

    store.markRunning(
        secondLease.jobId,
        secondLease.leaseEpoch,
        System.currentTimeMillis(),
        "default_reconciler");
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
            CaptureMode.METADATA_AND_CAPTURE,
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
        ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, ReconcileScope.empty());
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
}
