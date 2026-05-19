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

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
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
  void applyLeaseOutcomeRejectsLateFailureAfterSuccess() {
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
            ReconcileJobStore.CompletionKind.FAILED_RETRYABLE,
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
  void applyLeaseOutcomeRejectsLateSuccessAfterRetryableRequeue() {
    String jobId = enqueueRoot();
    ReconcileJobStore.LeasedJob lease = store.leaseNext().orElseThrow();

    assertTrue(
        store.applyLeaseOutcome(
            jobId,
            lease.leaseEpoch,
            ReconcileJobStore.CompletionKind.FAILED_RETRYABLE,
            2_000L,
            "retry",
            1L,
            0L,
            0L,
            0L,
            1L,
            0L,
            0L));

    assertFalse(
        store.applyLeaseOutcome(
            jobId,
            lease.leaseEpoch,
            ReconcileJobStore.CompletionKind.SUCCEEDED,
            3_000L,
            "late success",
            1L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L));

    assertEquals("JS_QUEUED", store.get(jobId).orElseThrow().state);
  }

  @Test
  void queuedJobsDoNotRetainLeaseStateWhileRunningJobsDo() {
    String jobId = enqueueRoot();

    DurableReconcileJobStore.StoredReconcileJob queued = storedJob(jobId);
    assertEquals("JS_QUEUED", queued.state);
    assertTrue(blank(queued.leaseEpoch));
    assertEquals(0L, queued.leaseExpiresAtMs);
    assertFalse(
        store.pointerStore.get(Keys.reconcileLeaseStatePointer(ACCOUNT_ID, jobId)).isPresent());

    ReconcileJobStore.LeasedJob lease = store.leaseNext().orElseThrow();
    DurableReconcileJobStore.StoredReconcileJob running = storedJob(jobId);
    assertEquals("JS_RUNNING", running.state);
    assertEquals(lease.leaseEpoch, running.leaseEpoch);
    assertTrue(running.leaseExpiresAtMs > 0L);
    assertTrue(
        store.pointerStore.get(Keys.reconcileLeaseStatePointer(ACCOUNT_ID, jobId)).isPresent());
  }

  @Test
  void retryableFailureClearsLeaseArtifactsBeforeReturningToQueued() {
    String jobId = enqueueRoot();
    ReconcileJobStore.LeasedJob lease = store.leaseNext().orElseThrow();

    assertTrue(
        store.applyLeaseOutcome(
            jobId,
            lease.leaseEpoch,
            ReconcileJobStore.CompletionKind.FAILED_RETRYABLE,
            2_000L,
            "retry",
            0L,
            0L,
            0L,
            0L,
            1L,
            0L,
            0L));

    DurableReconcileJobStore.StoredReconcileJob queued = storedJob(jobId);
    assertEquals("JS_QUEUED", queued.state);
    assertTrue(blank(queued.leaseEpoch));
    assertEquals(0L, queued.leaseExpiresAtMs);
    assertNotNull(queued.laneQueuePointerKey);
    assertTrue(store.pointerStore.get(queued.laneQueuePointerKey).isPresent());
    assertFalse(
        store.pointerStore.get(Keys.reconcileLeaseStatePointer(ACCOUNT_ID, jobId)).isPresent());
  }

  @Test
  void listAndGetUseSameProjectionForRunningRoot() {
    String parentJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask.of(
                "table-1", 55L, "db", "events"),
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
            ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask.of(
                parentJobId,
                "group-1",
                "table-1",
                55L,
                java.util.List.of("s3://bucket/file.parquet")),
            ReconcileExecutionPolicy.defaults(),
            parentJobId,
            "");

    ReconcileJobStore.LeasedJob childLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null,
                    null,
                    null,
                    java.util.EnumSet.of(
                        ai.floedb.floecat.reconciler.jobs.ReconcileJobKind.EXEC_FILE_GROUP)))
            .orElseThrow();
    assertEquals(childJobId, childLease.jobId);

    ReconcileJobStore.ReconcileJob getJob = store.get(ACCOUNT_ID, parentJobId).orElseThrow();
    ReconcileJobStore.ReconcileJob listJob =
        store.list(ACCOUNT_ID, 50, "", CONNECTOR_ID, java.util.Set.of()).jobs.stream()
            .filter(job -> parentJobId.equals(job.jobId))
            .findFirst()
            .orElseThrow();

    assertEquals(getJob.state, listJob.state);
    assertEquals(getJob.message, listJob.message);
    assertEquals(getJob.startedAtMs, listJob.startedAtMs);
    assertEquals(getJob.finishedAtMs, listJob.finishedAtMs);
  }

  @Test
  void startedParentWithQueuedChildProjectsAsQueuedUntilChildStarts() {
    String rootJobId = enqueueRoot();
    ReconcileJobStore.LeasedJob rootLease = store.leaseNext().orElseThrow();

    assertTrue(
        store.applyLeaseOutcome(
            rootJobId,
            rootLease.leaseEpoch,
            ReconcileJobStore.CompletionKind.SUCCEEDED,
            2_000L,
            "Planned 1 table job(s)",
            1L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L));

    store.enqueueTablePlan(
        ACCOUNT_ID,
        CONNECTOR_ID,
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.empty(),
        ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.of(
            "db", "events", "table-1", "events"),
        ReconcileExecutionPolicy.defaults(),
        rootJobId,
        "");

    ReconcileJobStore.ReconcileJob getJob = store.get(ACCOUNT_ID, rootJobId).orElseThrow();
    ReconcileJobStore.ReconcileJob listJob =
        store.list(ACCOUNT_ID, 50, "", CONNECTOR_ID, java.util.Set.of()).jobs.stream()
            .filter(job -> rootJobId.equals(job.jobId))
            .findFirst()
            .orElseThrow();

    assertEquals("JS_QUEUED", getJob.state);
    assertEquals(getJob.state, listJob.state);
    assertTrue(getJob.startedAtMs > 0L);
    assertTrue(getJob.message.contains(":"));
  }

  @Test
  void rootProjectsFailedWhenTerminalChildFailureCoexistsWithQueuedChild() {
    String rootJobId = enqueueRoot();
    ReconcileJobStore.LeasedJob rootLease = store.leaseNext().orElseThrow();

    assertTrue(
        store.applyLeaseOutcome(
            rootJobId,
            rootLease.leaseEpoch,
            ReconcileJobStore.CompletionKind.SUCCEEDED,
            2_000L,
            "Planned 2 table job(s)",
            2L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L));

    String failedTableJobId =
        store.enqueueTablePlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.of(
                "db", "events", "table-1", "events"),
            ReconcileExecutionPolicy.defaults(),
            rootJobId,
            "");
    String waitingTableJobId =
        store.enqueueTablePlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.of(
                "db", "clicks", "table-2", "clicks"),
            ReconcileExecutionPolicy.defaults(),
            rootJobId,
            "");

    ReconcileJobStore.LeasedJob firstLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, java.util.EnumSet.of(ReconcileJobKind.PLAN_TABLE)))
            .orElseThrow();
    ReconcileJobStore.LeasedJob secondLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, java.util.EnumSet.of(ReconcileJobKind.PLAN_TABLE)))
            .orElseThrow();

    ReconcileJobStore.LeasedJob failedLease =
        failedTableJobId.equals(firstLease.jobId) ? firstLease : secondLease;
    ReconcileJobStore.LeasedJob waitingLease =
        waitingTableJobId.equals(firstLease.jobId) ? firstLease : secondLease;

    assertTrue(
        store.applyLeaseOutcome(
            failedLease.jobId,
            failedLease.leaseEpoch,
            ReconcileJobStore.CompletionKind.FAILED_TERMINAL,
            3_000L,
            "terminal table failure",
            1L,
            0L,
            0L,
            0L,
            1L,
            0L,
            0L));
    assertTrue(
        store.applyLeaseOutcome(
            waitingLease.jobId,
            waitingLease.leaseEpoch,
            ReconcileJobStore.CompletionKind.FAILED_WAITING,
            3_100L,
            "dependency not ready",
            1L,
            0L,
            0L,
            0L,
            1L,
            0L,
            0L));

    ReconcileJobStore.ReconcileJob getJob = store.get(ACCOUNT_ID, rootJobId).orElseThrow();
    ReconcileJobStore.ReconcileJob listJob =
        store.list(ACCOUNT_ID, 50, "", CONNECTOR_ID, java.util.Set.of()).jobs.stream()
            .filter(job -> rootJobId.equals(job.jobId))
            .findFirst()
            .orElseThrow();

    assertEquals("JS_FAILED", getJob.state);
    assertEquals(getJob.state, listJob.state);
    assertTrue(getJob.message.contains("terminal table failure"));
  }

  @Test
  void rootProjectsWaitingWhenOnlyDescendantIsWaiting() {
    String rootJobId = enqueueRoot();
    ReconcileJobStore.LeasedJob rootLease = store.leaseNext().orElseThrow();

    assertTrue(
        store.applyLeaseOutcome(
            rootJobId,
            rootLease.leaseEpoch,
            ReconcileJobStore.CompletionKind.SUCCEEDED,
            2_000L,
            "Planned 1 table job(s)",
            1L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L));

    String tableJobId =
        store.enqueueTablePlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.of(
                "db", "events", "table-1", "events"),
            ReconcileExecutionPolicy.defaults(),
            rootJobId,
            "");
    ReconcileJobStore.LeasedJob tableLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, java.util.EnumSet.of(ReconcileJobKind.PLAN_TABLE)))
            .orElseThrow();
    assertEquals(tableJobId, tableLease.jobId);
    assertTrue(
        store.applyLeaseOutcome(
            tableJobId,
            tableLease.leaseEpoch,
            ReconcileJobStore.CompletionKind.FAILED_WAITING,
            3_000L,
            "dependency not ready",
            1L,
            0L,
            0L,
            0L,
            1L,
            0L,
            0L));

    ReconcileJobStore.ReconcileJob getJob = store.get(ACCOUNT_ID, rootJobId).orElseThrow();
    ReconcileJobStore.ReconcileJob listJob =
        store.list(ACCOUNT_ID, 50, "", CONNECTOR_ID, java.util.Set.of()).jobs.stream()
            .filter(job -> rootJobId.equals(job.jobId))
            .findFirst()
            .orElseThrow();
    ReconcileJobStore.ReconcileJob waitingListJob =
        store.list(ACCOUNT_ID, 50, "", CONNECTOR_ID, java.util.Set.of("JS_WAITING")).jobs.stream()
            .filter(job -> rootJobId.equals(job.jobId))
            .findFirst()
            .orElseThrow();

    assertEquals("JS_WAITING", getJob.state);
    assertEquals(getJob.state, listJob.state);
    assertEquals(getJob.state, waitingListJob.state);
    assertTrue(getJob.message.contains("dependency not ready"));
  }

  @Test
  void rootSurfacesDeepWaitingMessageWithoutRepeatedAncestorPrefixes() {
    String rootJobId = enqueueRoot();
    ReconcileJobStore.LeasedJob rootLease = store.leaseNext().orElseThrow();

    assertTrue(
        store.applyLeaseOutcome(
            rootJobId,
            rootLease.leaseEpoch,
            ReconcileJobStore.CompletionKind.SUCCEEDED,
            2_000L,
            "Planned 1 table job(s)",
            1L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L));

    String tableJobId =
        store.enqueueTablePlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.of(
                "db", "events", "table-1", "events"),
            ReconcileExecutionPolicy.defaults(),
            rootJobId,
            "");
    ReconcileJobStore.LeasedJob tableLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, java.util.EnumSet.of(ReconcileJobKind.PLAN_TABLE)))
            .orElseThrow();
    assertTrue(
        store.applyLeaseOutcome(
            tableJobId,
            tableLease.leaseEpoch,
            ReconcileJobStore.CompletionKind.SUCCEEDED,
            3_000L,
            "Planned 1 snapshot job(s)",
            1L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L));

    String snapshotJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask.of(
                "table-1", 55L, "db", "events"),
            ReconcileExecutionPolicy.defaults(),
            tableJobId,
            "");
    ReconcileJobStore.LeasedJob snapshotLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, java.util.EnumSet.of(ReconcileJobKind.PLAN_SNAPSHOT)))
            .orElseThrow();
    assertTrue(
        store.applyLeaseOutcome(
            snapshotJobId,
            snapshotLease.leaseEpoch,
            ReconcileJobStore.CompletionKind.FAILED_WAITING,
            4_000L,
            "waiting for downstream file groups",
            0L,
            0L,
            0L,
            0L,
            1L,
            0L,
            0L));

    ReconcileJobStore.ReconcileJob rootJob = store.get(ACCOUNT_ID, rootJobId).orElseThrow();

    assertEquals("JS_WAITING", rootJob.state);
    assertTrue(rootJob.message.contains(snapshotJobId + ": waiting for downstream file groups"));
    assertFalse(rootJob.message.contains(tableJobId + ": " + snapshotJobId + ": "));
  }

  @Test
  void planTableAggregateDerivesChangedTableFromSuccessfulSnapshotWork() {
    String rootJobId = enqueueRoot();
    ReconcileJobStore.LeasedJob rootLease = store.leaseNext().orElseThrow();

    assertTrue(
        store.applyLeaseOutcome(
            rootJobId,
            rootLease.leaseEpoch,
            ReconcileJobStore.CompletionKind.SUCCEEDED,
            2_000L,
            "Planned 1 table job(s)",
            1L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L));

    String tableJobId =
        store.enqueueTablePlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.of(
                "db", "events", "table-1", "events"),
            ReconcileExecutionPolicy.defaults(),
            rootJobId,
            "");
    ReconcileJobStore.LeasedJob tableLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, java.util.EnumSet.of(ReconcileJobKind.PLAN_TABLE)))
            .orElseThrow();
    assertEquals(tableJobId, tableLease.jobId);
    assertTrue(
        store.applyLeaseOutcome(
            tableJobId,
            tableLease.leaseEpoch,
            ReconcileJobStore.CompletionKind.SUCCEEDED,
            3_000L,
            "Planned 1 snapshot job(s)",
            1L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L));

    String snapshotJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask.of(
                "table-1", 55L, "db", "events"),
            ReconcileExecutionPolicy.defaults(),
            tableJobId,
            "");
    ReconcileJobStore.LeasedJob snapshotLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, java.util.EnumSet.of(ReconcileJobKind.PLAN_SNAPSHOT)))
            .orElseThrow();
    assertEquals(snapshotJobId, snapshotLease.jobId);
    assertTrue(
        store.applyLeaseOutcome(
            snapshotJobId,
            snapshotLease.leaseEpoch,
            ReconcileJobStore.CompletionKind.SUCCEEDED,
            4_000L,
            "Finalized snapshot capture 55",
            0L,
            0L,
            0L,
            0L,
            0L,
            1L,
            5L));

    ReconcileJobStore.ReconcileJob tableJob = store.get(ACCOUNT_ID, tableJobId).orElseThrow();
    ReconcileJobStore.ReconcileJob rootJob = store.get(ACCOUNT_ID, rootJobId).orElseThrow();

    assertEquals("JS_SUCCEEDED", tableJob.state);
    assertEquals(1L, tableJob.tablesScanned);
    assertEquals(1L, tableJob.tablesChanged);
    assertEquals(1L, tableJob.snapshotsProcessed);
    assertEquals(5L, tableJob.statsProcessed);
    assertEquals("", tableJob.message);
    assertEquals(1L, rootJob.tablesScanned);
    assertEquals(1L, rootJob.tablesChanged);
    assertEquals(1L, rootJob.snapshotsProcessed);
    assertEquals(5L, rootJob.statsProcessed);
    assertEquals("", rootJob.message);
  }

  @Test
  void planOnlySuccessDoesNotCountTableAsScannedOrChanged() {
    String rootJobId = enqueueRoot();
    ReconcileJobStore.LeasedJob rootLease = store.leaseNext().orElseThrow();

    assertTrue(
        store.applyLeaseOutcome(
            rootJobId,
            rootLease.leaseEpoch,
            ReconcileJobStore.CompletionKind.SUCCEEDED,
            2_000L,
            "Planned 1 table job(s)",
            1L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L));

    String tableJobId =
        store.enqueueTablePlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.of(
                "db", "events", "table-1", "events"),
            ReconcileExecutionPolicy.defaults(),
            rootJobId,
            "");
    ReconcileJobStore.LeasedJob tableLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, java.util.EnumSet.of(ReconcileJobKind.PLAN_TABLE)))
            .orElseThrow();
    assertEquals(tableJobId, tableLease.jobId);
    assertTrue(
        store.applyLeaseOutcome(
            tableJobId,
            tableLease.leaseEpoch,
            ReconcileJobStore.CompletionKind.SUCCEEDED,
            3_000L,
            "Planned 0 snapshot job(s)",
            1L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L));

    ReconcileJobStore.ReconcileJob tableJob = store.get(ACCOUNT_ID, tableJobId).orElseThrow();
    ReconcileJobStore.ReconcileJob rootJob = store.get(ACCOUNT_ID, rootJobId).orElseThrow();

    assertEquals("JS_SUCCEEDED", tableJob.state);
    assertEquals(0L, tableJob.tablesScanned);
    assertEquals(0L, tableJob.tablesChanged);
    assertEquals("", tableJob.message);
    assertEquals(0L, rootJob.tablesScanned);
    assertEquals(0L, rootJob.tablesChanged);
    assertEquals("", rootJob.message);
  }

  @Test
  void connectorRootDoesNotSumNoOpPlanTableChildrenAsScannedTables() {
    String rootJobId = enqueueRoot();
    ReconcileJobStore.LeasedJob rootLease = store.leaseNext().orElseThrow();

    assertTrue(
        store.applyLeaseOutcome(
            rootJobId,
            rootLease.leaseEpoch,
            ReconcileJobStore.CompletionKind.SUCCEEDED,
            2_000L,
            "Planned 2 table job(s)",
            2L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L));

    String firstTableJobId =
        store.enqueueTablePlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.of(
                "db", "events", "table-1", "events"),
            ReconcileExecutionPolicy.defaults(),
            rootJobId,
            "");
    String secondTableJobId =
        store.enqueueTablePlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.of(
                "db", "orders", "table-2", "orders"),
            ReconcileExecutionPolicy.defaults(),
            rootJobId,
            "");

    ReconcileJobStore.LeasedJob firstTableLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, java.util.EnumSet.of(ReconcileJobKind.PLAN_TABLE)))
            .orElseThrow();
    ReconcileJobStore.LeasedJob secondTableLease =
        store
            .leaseNext(
                new ReconcileJobStore.LeaseRequest(
                    null, null, null, java.util.EnumSet.of(ReconcileJobKind.PLAN_TABLE)))
            .orElseThrow();

    assertTrue(
        store.applyLeaseOutcome(
            firstTableLease.jobId,
            firstTableLease.leaseEpoch,
            ReconcileJobStore.CompletionKind.SUCCEEDED,
            3_000L,
            "Planned 0 snapshot job(s)",
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L));
    assertTrue(
        store.applyLeaseOutcome(
            secondTableLease.jobId,
            secondTableLease.leaseEpoch,
            ReconcileJobStore.CompletionKind.SUCCEEDED,
            3_100L,
            "Planned 0 snapshot job(s)",
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L));

    ReconcileJobStore.ReconcileJob firstTableJob =
        store.get(ACCOUNT_ID, firstTableJobId).orElseThrow();
    ReconcileJobStore.ReconcileJob secondTableJob =
        store.get(ACCOUNT_ID, secondTableJobId).orElseThrow();
    ReconcileJobStore.ReconcileJob rootJob = store.get(ACCOUNT_ID, rootJobId).orElseThrow();

    assertEquals(0L, firstTableJob.tablesScanned);
    assertEquals(0L, secondTableJob.tablesScanned);
    assertEquals(0L, rootJob.tablesScanned);
    assertEquals(0L, rootJob.tablesChanged);
  }

  private String enqueueRoot() {
    return store.enqueue(
        ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, ReconcileScope.empty());
  }

  private DurableReconcileJobStore.StoredReconcileJob storedJob(String jobId) {
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    Pointer canonicalPointer = store.pointerStore.get(canonicalPointerKey).orElseThrow();
    try {
      return store.mapper.readValue(
          store.blobStore.get(canonicalPointer.getBlobUri()),
          DurableReconcileJobStore.StoredReconcileJob.class);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }
}
