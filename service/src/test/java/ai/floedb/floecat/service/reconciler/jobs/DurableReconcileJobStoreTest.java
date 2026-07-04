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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import ai.floedb.floecat.service.it.profiles.ReconcileJobStoreControlPlaneProfile;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobProjection;
import ai.floedb.floecat.service.reconciler.jobs.durable.projection.ReconcileJobProjectionStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.projection.ReconcileJobRootSummaryStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.JobIndexEntrySnapshot;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.MemoryReconcileJobIndexBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.MemoryReconcileLeaseBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.MemoryReconcileReadyQueueBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileLeaseStore;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.aws.dynamodb.DynamoPointerStore;
import ai.floedb.floecat.storage.kv.dynamodb.DynamoDbKvStore;
import ai.floedb.floecat.storage.kv.dynamodb.ps.PointerStoreEntity;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;
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

@QuarkusTest
@TestProfile(ReconcileJobStoreControlPlaneProfile.class)
class DurableReconcileJobStoreTest {
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
    }
    store.init();
  }

  @AfterEach
  void tearDown() {}

  @Test
  void enqueueDedupesWhileJobIsActive() {
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String first =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);
    String second =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, scope);

    assertEquals(first, second);
  }

  @Test
  void enqueueDedupesAcrossDifferentPinnedExecutors() {
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String first =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            scope,
            ReconcileJobKind.PLAN_CONNECTOR,
            ReconcileTableTask.empty(),
            ReconcileViewTask.empty(),
            ReconcileSnapshotTask.empty(),
            ReconcileFileGroupTask.empty(),
            ReconcileExecutionPolicy.defaults(),
            "",
            "executor-a");
    String second =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            scope,
            ReconcileJobKind.PLAN_CONNECTOR,
            ReconcileTableTask.empty(),
            ReconcileViewTask.empty(),
            ReconcileSnapshotTask.empty(),
            ReconcileFileGroupTask.empty(),
            ReconcileExecutionPolicy.defaults(),
            "",
            "executor-b");

    assertEquals(first, second);
  }

  @Test
  void enqueuePlanDoesNotDedupeAcrossDifferentExplicitSnapshotSelections() {
    ReconcileScope firstScope =
        ReconcileScope.of(
            List.of(),
            "tbl",
            null,
            List.of(),
            ReconcileScope.empty().capturePolicy(),
            ReconcileSnapshotSelection.explicit(List.of(101L)));
    ReconcileScope secondScope =
        ReconcileScope.of(
            List.of(),
            "tbl",
            null,
            List.of(),
            ReconcileScope.empty().capturePolicy(),
            ReconcileSnapshotSelection.explicit(List.of(202L)));

    String first =
        store.enqueue(
            ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, firstScope);
    String second =
        store.enqueue(
            ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, secondScope);

    assertNotEquals(first, second);
  }

  @Test
  void enqueueSnapshotPlanDoesNotDedupeAcrossDifferentPlanTableParents() {
    String firstParentJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "parent-table-1"),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("db", "parent_one", "parent-table-1", "parent_one"),
            ReconcileExecutionPolicy.defaults(),
            "",
            "");
    String secondParentJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "parent-table-2"),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("db", "parent_two", "parent-table-2", "parent_two"),
            ReconcileExecutionPolicy.defaults(),
            "",
            "");
    ReconcileSnapshotTask snapshotTask = ReconcileSnapshotTask.of("table-1", 55L, "db", "orders");

    String first =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            snapshotTask,
            ReconcileExecutionPolicy.defaults(),
            firstParentJobId,
            "");
    String second =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            snapshotTask,
            ReconcileExecutionPolicy.defaults(),
            secondParentJobId,
            "");

    assertNotEquals(first, second);
    assertEquals(
        List.of(first),
        store.childJobsPage(ACCOUNT_ID, firstParentJobId, 200, "").jobs.stream()
            .filter(job -> job.jobKind == ReconcileJobKind.PLAN_SNAPSHOT)
            .map(job -> job.jobId)
            .toList());
    assertEquals(
        List.of(second),
        store.childJobsPage(ACCOUNT_ID, secondParentJobId, 200, "").jobs.stream()
            .filter(job -> job.jobKind == ReconcileJobKind.PLAN_SNAPSHOT)
            .map(job -> job.jobId)
            .toList());

    var firstParentLease = leaseJob(firstParentJobId);
    store.markRunning(firstParentJobId, firstParentLease.leaseEpoch, 100L, "executor-table-1");
    store.markWaiting(
        firstParentJobId,
        firstParentLease.leaseEpoch,
        110L,
        ReconcileJobStore.WaitingReason.CHILD_WORK_FINALIZED,
        "Waiting on child work",
        1L,
        1L,
        0L,
        0L,
        0L,
        0L,
        0L);

    var secondParentLease = leaseJob(secondParentJobId);
    store.markRunning(secondParentJobId, secondParentLease.leaseEpoch, 120L, "executor-table-2");
    store.markWaiting(
        secondParentJobId,
        secondParentLease.leaseEpoch,
        130L,
        ReconcileJobStore.WaitingReason.CHILD_WORK_FINALIZED,
        "Waiting on child work",
        1L,
        1L,
        0L,
        0L,
        0L,
        0L,
        0L);

    var firstSnapshotLease = leaseJob(first);
    store.markRunning(first, firstSnapshotLease.leaseEpoch, 140L, "executor-snapshot-1");
    store.markSucceeded(first, firstSnapshotLease.leaseEpoch, 150L, 0L, 0L, 0L, 0L, 0L, 0L);

    var secondSnapshotLease = leaseJob(second);
    store.markRunning(second, secondSnapshotLease.leaseEpoch, 160L, "executor-snapshot-2");
    store.markSucceeded(second, secondSnapshotLease.leaseEpoch, 170L, 0L, 0L, 0L, 0L, 0L, 0L);

    assertEquals(
        "JS_SUCCEEDED",
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, firstParentJobId)).state);
    assertEquals(
        "JS_SUCCEEDED",
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, secondParentJobId)).state);
  }

  @Test
  void enqueueExecFileGroupDoesNotDedupeAcrossDifferentSnapshotPlanParents() {
    ReconcileScope scope = ReconcileScope.of(List.of(), "table-1");
    String firstParentJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "parent-table-1"),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("db", "parent_one", "parent-table-1", "parent_one"),
            ReconcileExecutionPolicy.defaults(),
            "",
            "");
    String secondParentJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "parent-table-2"),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("db", "parent_two", "parent-table-2", "parent_two"),
            ReconcileExecutionPolicy.defaults(),
            "",
            "");
    ReconcileFileGroupTask firstPlanGroup =
        ReconcileFileGroupTask.of(
            "plan-1", "snapshot-55-group-0", "table-1", 55L, List.of("s3://bucket/data/a.parquet"));
    ReconcileFileGroupTask secondPlanGroup =
        ReconcileFileGroupTask.of(
            "plan-2", "snapshot-55-group-0", "table-1", 55L, List.of("s3://bucket/data/a.parquet"));

    String first =
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
            firstPlanGroup,
            ReconcileExecutionPolicy.defaults(),
            firstParentJobId,
            "");
    String second =
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
            secondPlanGroup,
            ReconcileExecutionPolicy.defaults(),
            secondParentJobId,
            "");

    assertNotEquals(first, second);
  }

  @Test
  void enqueueSnapshotFinalizationDoesNotDedupeAcrossDifferentSnapshotPlanParents() {
    ReconcileFileGroupTask firstPlanGroup =
        ReconcileFileGroupTask.of(
            "plan-1", "snapshot-55-group-0", "table-1", 55L, List.of("s3://bucket/data/a.parquet"));
    ReconcileFileGroupTask secondPlanGroup =
        ReconcileFileGroupTask.of(
            "plan-2", "snapshot-55-group-0", "table-1", 55L, List.of("s3://bucket/data/a.parquet"));
    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of("table-1", 55L, "db", "orders", List.of(firstPlanGroup), true);
    ReconcileSnapshotTask sameSnapshotDifferentPlanTask =
        ReconcileSnapshotTask.of("table-1", 55L, "db", "orders", List.of(secondPlanGroup), true);
    String firstParentJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "parent-table-1"),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("db", "parent_one", "parent-table-1", "parent_one"),
            ReconcileExecutionPolicy.defaults(),
            "",
            "");
    String secondParentJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "parent-table-2"),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("db", "parent_two", "parent-table-2", "parent_two"),
            ReconcileExecutionPolicy.defaults(),
            "",
            "");

    String first =
        store.enqueueSnapshotFinalization(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            snapshotTask,
            ReconcileExecutionPolicy.defaults(),
            firstParentJobId,
            "");
    String second =
        store.enqueueSnapshotFinalization(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            sameSnapshotDifferentPlanTask,
            ReconcileExecutionPolicy.defaults(),
            secondParentJobId,
            "");

    assertNotEquals(first, second);
  }

  @Test
  void bulkEnqueueAndApplyLeaseOutcomeDoesNotInsertChildrenWhenLeaseIsRejected() {
    String connectorJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    var connectorLease = leaseJob(connectorJobId);
    store.markRunning(connectorJobId, connectorLease.leaseEpoch, 100L, "executor-connector");

    boolean accepted =
        store.bulkEnqueueAndApplyLeaseOutcome(
            List.of(
                ReconcileJobStore.BulkEnqueueSpec.of(
                    ACCOUNT_ID,
                    CONNECTOR_ID,
                    false,
                    CaptureMode.METADATA_AND_CAPTURE,
                    ReconcileScope.of(List.of(), "table-1"),
                    ReconcileJobKind.PLAN_TABLE,
                    ReconcileTableTask.of("db", "orders", "table-1", "orders"),
                    ReconcileViewTask.empty(),
                    ReconcileSnapshotTask.empty(),
                    ReconcileFileGroupTask.empty(),
                    ReconcileExecutionPolicy.defaults(),
                    connectorJobId,
                    "")),
            connectorJobId,
            "wrong-lease",
            ReconcileJobStore.CompletionKind.SUCCEEDED_WAITING,
            200L,
            "Planned 1 table job(s)",
            1L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L);

    assertFalse(accepted);
    assertTrue(store.childJobs(ACCOUNT_ID, connectorJobId).isEmpty());
    assertEquals(
        "JS_RUNNING",
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId)).state);
  }

  @Test
  void bulkEnqueueAndApplyLeaseOutcomeAdvancesParentProjectionGenerationForCreatedChildren() {
    String connectorJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    var connectorLease = leaseJob(connectorJobId);
    store.markRunning(connectorJobId, connectorLease.leaseEpoch, 100L, "executor-connector");

    boolean accepted =
        store.bulkEnqueueAndApplyLeaseOutcome(
            List.of(
                ReconcileJobStore.BulkEnqueueSpec.of(
                    ACCOUNT_ID,
                    CONNECTOR_ID,
                    false,
                    CaptureMode.METADATA_AND_CAPTURE,
                    ReconcileScope.of(List.of(), "table-1"),
                    ReconcileJobKind.PLAN_TABLE,
                    ReconcileTableTask.of("db", "orders", "table-1", "orders"),
                    ReconcileViewTask.empty(),
                    ReconcileSnapshotTask.empty(),
                    ReconcileFileGroupTask.empty(),
                    ReconcileExecutionPolicy.defaults(),
                    connectorJobId,
                    "")),
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
            0L);

    assertTrue(accepted);
    StoredReconcileJob parentRecord =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId));
    assertEquals(1L, parentRecord.expectedDirectChildren);
    assertTrue(parentRecord.projectionRequestedGeneration > 0L);
    assertEquals("JS_WAITING", parentRecord.state);
    assertEquals(1, store.childJobs(ACCOUNT_ID, connectorJobId).size());
  }

  @Test
  void bulkEnqueueAndApplyLeaseOutcomeCommitsParentAfterChunkRetryDedupesChildren() {
    InMemoryPointerStore pointerStore = new InMemoryPointerStore();
    MemoryReconcileJobIndexBackend delegateBackend = new MemoryReconcileJobIndexBackend();
    delegateBackend.bind(pointerStore);
    FailingCompareAndSetBatchBackend failingBackend =
        new FailingCompareAndSetBatchBackend(delegateBackend);

    store = newIsolatedInMemoryStore(pointerStore, new InMemoryBlobStore(), failingBackend);

    String connectorJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    var connectorLease = leaseJob(connectorJobId);
    store.markRunning(connectorJobId, connectorLease.leaseEpoch, 100L, "executor-connector");

    List<ReconcileJobStore.BulkEnqueueSpec> childSpecs =
        java.util.stream.IntStream.range(0, 24)
            .mapToObj(
                i ->
                    ReconcileJobStore.BulkEnqueueSpec.of(
                        ACCOUNT_ID,
                        CONNECTOR_ID,
                        false,
                        CaptureMode.METADATA_AND_CAPTURE,
                        ReconcileScope.of(List.of(), "table-" + i),
                        ReconcileJobKind.PLAN_TABLE,
                        ReconcileTableTask.of("db", "orders_" + i, "table-" + i, "orders_" + i),
                        ReconcileViewTask.empty(),
                        ReconcileSnapshotTask.empty(),
                        ReconcileFileGroupTask.empty(),
                        ReconcileExecutionPolicy.defaults(),
                        connectorJobId,
                        ""))
            .toList();

    failingBackend.failOnArmedCall(2);
    boolean accepted =
        store.bulkEnqueueAndApplyLeaseOutcome(
            childSpecs,
            connectorJobId,
            connectorLease.leaseEpoch,
            ReconcileJobStore.CompletionKind.SUCCEEDED_WAITING,
            200L,
            "Planned 24 table job(s)",
            24L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L);

    assertTrue(accepted);
    assertTrue(failingBackend.armedCalls() > 1);
    StoredReconcileJob parentRecord =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId));
    assertEquals("JS_WAITING", parentRecord.state);
    assertEquals(24L, parentRecord.expectedDirectChildren);
    assertEquals(24, store.childJobs(ACCOUNT_ID, connectorJobId).size());
  }

  @Test
  void bulkEnqueueAndApplyLeaseOutcomePreservesCommittedChildPayloadsWhenRetryAborts() {
    InMemoryPointerStore pointerStore = new InMemoryPointerStore();
    MemoryReconcileJobIndexBackend delegateBackend = new MemoryReconcileJobIndexBackend();
    delegateBackend.bind(pointerStore);
    FailingCompareAndSetBatchBackend failingBackend =
        new FailingCompareAndSetBatchBackend(delegateBackend);
    InMemoryBlobStore blobStore = new InMemoryBlobStore();

    store = newIsolatedInMemoryStore(pointerStore, blobStore, failingBackend);

    String tableJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-parent"),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("db", "orders", "table-parent", "orders"),
            ReconcileExecutionPolicy.defaults(),
            "",
            "");
    var tableLease = leaseJob(tableJobId);
    store.markRunning(tableJobId, tableLease.leaseEpoch, 100L, "executor-table");

    List<ReconcileJobStore.BulkEnqueueSpec> childSpecs =
        java.util.stream.IntStream.range(0, 24)
            .mapToObj(
                i -> {
                  long snapshotId = 10_000L + i;
                  String tableId = "table-" + i;
                  return ReconcileJobStore.BulkEnqueueSpec.of(
                      ACCOUNT_ID,
                      CONNECTOR_ID,
                      false,
                      CaptureMode.METADATA_AND_CAPTURE,
                      ReconcileScope.of(List.of(), tableId),
                      ReconcileJobKind.PLAN_SNAPSHOT,
                      ReconcileTableTask.empty(),
                      ReconcileViewTask.empty(),
                      ReconcileSnapshotTask.of(
                          tableId,
                          snapshotId,
                          "db",
                          "orders_" + i,
                          List.of(
                              ReconcileFileGroupTask.of(
                                  "plan-" + i,
                                  "group-" + i,
                                  tableId,
                                  snapshotId,
                                  List.of("s3://bucket/data/file-" + i + ".parquet"))),
                          true),
                      ReconcileFileGroupTask.empty(),
                      ReconcileExecutionPolicy.defaults(),
                      tableJobId,
                      "");
                })
            .toList();

    failingBackend.failOnArmedCall(2);
    failingBackend.onFailedCompareAndSetBatch(
        () -> pointerStore.delete(Keys.reconcileJobLookupPointerById(tableJobId)));

    assertThrows(
        IllegalStateException.class,
        () ->
            store.bulkEnqueueAndApplyLeaseOutcome(
                childSpecs,
                tableJobId,
                tableLease.leaseEpoch,
                ReconcileJobStore.CompletionKind.SUCCEEDED_WAITING,
                200L,
                "Planned 24 snapshot job(s)",
                24L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L));

    List<ReconcileJob> committedChildren = store.childJobs(ACCOUNT_ID, tableJobId);
    assertFalse(committedChildren.isEmpty());
    assertTrue(committedChildren.size() < childSpecs.size());
    for (ReconcileJob child : committedChildren) {
      StoredReconcileJob committed =
          readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, child.jobId));
      assertFalse(committed.snapshotPlanBlobUri.isBlank());
      assertNotNull(blobStore.get(committed.snapshotPlanBlobUri));
    }
  }

  @Test
  void bulkEnqueueAndApplyLeaseOutcomeTreatsDuplicateTerminalParentCompletionAsIdempotentSuccess() {
    String connectorJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    var connectorLease = leaseJob(connectorJobId);
    store.markRunning(connectorJobId, connectorLease.leaseEpoch, 100L, "executor-connector");

    ReconcileJobStore.BulkEnqueueSpec tableSpec =
        ReconcileJobStore.BulkEnqueueSpec.of(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("db", "orders", "table-1", "orders"),
            ReconcileViewTask.empty(),
            ReconcileSnapshotTask.empty(),
            ReconcileFileGroupTask.empty(),
            ReconcileExecutionPolicy.defaults(),
            connectorJobId,
            "");

    assertTrue(
        store.bulkEnqueueAndApplyLeaseOutcome(
            List.of(tableSpec),
            connectorJobId,
            connectorLease.leaseEpoch,
            ReconcileJobStore.CompletionKind.FAILED_TERMINAL,
            200L,
            "planner failed",
            1L,
            0L,
            0L,
            0L,
            1L,
            0L,
            0L));

    assertTrue(
        store.bulkEnqueueAndApplyLeaseOutcome(
            List.of(tableSpec),
            connectorJobId,
            connectorLease.leaseEpoch,
            ReconcileJobStore.CompletionKind.FAILED_TERMINAL,
            200L,
            "planner failed",
            1L,
            0L,
            0L,
            0L,
            1L,
            0L,
            0L));

    assertEquals(
        "JS_FAILED",
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId)).state);
    assertEquals(1, store.childJobs(ACCOUNT_ID, connectorJobId).size());
  }

  @Test
  void bulkEnqueueAndApplyLeaseOutcomeRejectsStalePlanSnapshotSuccessWaitingAfterRequeue() {
    configureLeaseRenewGraceMs(0L);
    String snapshotJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "orders", List.of(), true),
            ReconcileExecutionPolicy.defaults(),
            "",
            "");
    var snapshotLease = leaseJob(snapshotJobId);
    store.markRunning(snapshotJobId, snapshotLease.leaseEpoch, 100L, "executor-snapshot");
    reclaimExpiredLease(snapshotJobId);

    StoredReconcileJob requeued =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, snapshotJobId));
    assertEquals("Lease expired; requeued", requeued.message);
    assertEquals("JS_QUEUED", requeued.state);

    boolean accepted =
        store.bulkEnqueueAndApplyLeaseOutcome(
            List.of(
                ReconcileJobStore.BulkEnqueueSpec.of(
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
                        snapshotJobId,
                        "snapshot-0-group-0",
                        "table-1",
                        55L,
                        List.of("s3://bucket/data/file-1.parquet")),
                    ReconcileExecutionPolicy.defaults(),
                    snapshotJobId,
                    "")),
            snapshotJobId,
            snapshotLease.leaseEpoch,
            ReconcileJobStore.CompletionKind.SUCCEEDED_WAITING,
            200L,
            "Snapshot plan recorded for db.orders with 1 file group(s)",
            0L,
            0L,
            0L,
            0L,
            0L,
            1L,
            0L);

    assertFalse(accepted);
    assertTrue(store.childJobs(ACCOUNT_ID, snapshotJobId).isEmpty());
    assertEquals(
        "JS_QUEUED",
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, snapshotJobId)).state);
  }

  @Test
  void bulkEnqueueAndApplyLeaseOutcomeRejectsStalePlanTableSuccessWaitingAfterRequeue() {
    configureLeaseRenewGraceMs(0L);
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
            "",
            "");
    var tableLease = leaseJob(tableJobId);
    store.markRunning(tableJobId, tableLease.leaseEpoch, 100L, "executor-table");
    reclaimExpiredLease(tableJobId);

    StoredReconcileJob requeued =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, tableJobId));
    assertEquals("Lease expired; requeued", requeued.message);
    assertEquals("JS_QUEUED", requeued.state);

    boolean accepted =
        store.bulkEnqueueAndApplyLeaseOutcome(
            List.of(
                ReconcileJobStore.BulkEnqueueSpec.of(
                    ACCOUNT_ID,
                    CONNECTOR_ID,
                    false,
                    CaptureMode.METADATA_AND_CAPTURE,
                    ReconcileScope.of(List.of(), "table-1"),
                    ReconcileJobKind.PLAN_SNAPSHOT,
                    ReconcileTableTask.empty(),
                    ReconcileViewTask.empty(),
                    ReconcileSnapshotTask.of("table-1", 55L, "db", "orders", List.of(), true),
                    ReconcileFileGroupTask.empty(),
                    ReconcileExecutionPolicy.defaults(),
                    tableJobId,
                    "")),
            tableJobId,
            tableLease.leaseEpoch,
            ReconcileJobStore.CompletionKind.SUCCEEDED_WAITING,
            200L,
            "Planned 1 snapshot job(s)",
            1L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L);

    assertFalse(accepted);
    assertTrue(store.childJobs(ACCOUNT_ID, tableJobId).isEmpty());
    assertEquals(
        "JS_QUEUED", readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, tableJobId)).state);
  }

  @Test
  void execFileGroupLeaseExpiryRequeueCanBeLeasedAgain() throws Exception {
    configureLeaseRenewGraceMs(0L);
    configureRetryPolicy(3, 0L, 0L);

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
            "",
            "");

    var firstLease = leaseJob(execJobId);
    store.markRunning(execJobId, firstLease.leaseEpoch, 100L, "executor-exec");

    reclaimExpiredLease(execJobId);

    StoredReconcileJob requeued =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, execJobId));
    assertEquals("Lease expired; requeued", requeued.message);
    assertEquals("JS_QUEUED", requeued.state);
    assertTrue(requeued.nextAttemptAtMs > 0L);
    assertNotNull(requeued.readyPointerKey);

    makeQueuedJobDueNow(execJobId);

    java.util.Optional<ReconcileJobStore.LeasedJob> secondLease =
        store.leaseNext(
            ReconcileJobStore.LeaseRequest.of(
                java.util.EnumSet.of(ReconcileExecutionClass.DEFAULT),
                Set.of(ReconcileJobStore.LeaseRequest.anyLaneToken()),
                Set.of("floescan_ingest"),
                java.util.EnumSet.of(ReconcileJobKind.EXEC_FILE_GROUP)));

    assertTrue(secondLease.isPresent());

    assertEquals(execJobId, secondLease.orElseThrow().jobId);
    assertTrue(!secondLease.orElseThrow().leaseEpoch.isBlank());
    assertNotEquals(firstLease.leaseEpoch, secondLease.orElseThrow().leaseEpoch);
    assertEquals(ReconcileJobKind.EXEC_FILE_GROUP, secondLease.orElseThrow().jobKind);
    assertEquals("plan-1", secondLease.orElseThrow().fileGroupTask.planId());
    assertEquals("group-1", secondLease.orElseThrow().fileGroupTask.groupId());
    assertEquals(
        List.of("s3://bucket/data/file-1.parquet"),
        secondLease.orElseThrow().fileGroupTask.filePaths());
  }

  @Test
  void leaseNextRejectsWhenLeaseScanCapacityIsExhausted() {
    store.leaseAcquireTimeoutMs = 0L;
    store.leaseScanPermits = new java.util.concurrent.Semaphore(0);

    LeaseScanCapacityExceededException error =
        assertThrows(
            LeaseScanCapacityExceededException.class,
            () -> store.leaseNext(ReconcileJobStore.LeaseRequest.all()));

    assertTrue(error.getMessage().contains("capacity exhausted"));
  }

  @Test
  void leaseNextPropagatesInterruptedAdmissionAsCancellation() {
    store.leaseAcquireTimeoutMs = 5_000L;
    store.leaseScanPermits = new java.util.concurrent.Semaphore(0);
    Thread.currentThread().interrupt();
    try {
      CancellationException error =
          assertThrows(
              CancellationException.class,
              () -> store.leaseNext(ReconcileJobStore.LeaseRequest.all()));

      assertTrue(error.getMessage().contains("admission interrupted"));
      assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void enqueueUpsertsStoredRootSummaryImmediately() {
    String connectorJobId =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, null);

    StoredReconcileJob canonical =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId));
    var summary =
        rootSummaryStore()
            .listSummaries(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of())
            .summaries()
            .stream()
            .filter(current -> connectorJobId.equals(current.jobId()))
            .findFirst()
            .orElseThrow();

    assertEquals(canonical.jobId, summary.jobId());
    assertEquals(canonical.state, summary.state());
    assertEquals(canonical.message, summary.message());
    assertEquals(canonical.startedAtMs, summary.startedAtMs());
    assertEquals(canonical.finishedAtMs, summary.finishedAtMs());
  }

  @Test
  void terminalChildCommitsCanonicalParentStateBeforeDirtyProjectionSignal() {
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

    var connectorLease = leaseJob(connectorJobId);
    store.markRunning(connectorJobId, connectorLease.leaseEpoch, 50L, "executor-connector");
    store.markWaiting(
        connectorJobId,
        connectorLease.leaseEpoch,
        60L,
        ReconcileJobStore.WaitingReason.CHILD_WORK_FINALIZED,
        "Waiting on child work",
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L);

    var tableLease = leaseJob(tableJobId);
    store.markRunning(tableJobId, tableLease.leaseEpoch, 100L, "executor-table");

    PointerStore originalPointerStore = store.pointerStore;
    store.pointerStore = new DirtyParentFailingPointerStore(originalPointerStore);

    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () ->
                store.markSucceeded(
                    tableJobId, tableLease.leaseEpoch, 200L, 1L, 1L, 0L, 0L, 0L, 0L));
    assertTrue(thrown.getMessage().contains("Failed to mark reconcile parent dirty"));

    StoredReconcileJob connectorCanonical =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId));
    StoredReconcileJob tableCanonical =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, tableJobId));

    assertEquals("JS_SUCCEEDED", tableCanonical.state);
    assertEquals("JS_SUCCEEDED", connectorCanonical.state);
  }

  @Test
  void listRootJobsDoesNotRefreshStoredProjectionOnRead() {
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

    var connectorLease = leaseJob(connectorJobId);
    store.markRunning(connectorJobId, connectorLease.leaseEpoch, 50L, "executor-connector");
    store.markWaiting(
        connectorJobId,
        connectorLease.leaseEpoch,
        60L,
        ReconcileJobStore.WaitingReason.CHILD_WORK_FINALIZED,
        "Waiting on child work",
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L);

    var tableLease = leaseJob(tableJobId);
    store.markRunning(tableJobId, tableLease.leaseEpoch, 100L, "executor-table");
    store.markSucceeded(tableJobId, tableLease.leaseEpoch, 200L, 1L, 1L, 0L, 0L, 0L, 0L);

    assertDoesNotThrow(
        () ->
            invokePrivateMethod(
                store,
                "mutateByCanonicalPointerReturningRecord",
                new Class<?>[] {String.class, UnaryOperator.class},
                Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId),
                (UnaryOperator<StoredReconcileJob>)
                    current -> {
                      current.projectionRequestedGeneration =
                          Math.max(1L, current.projectionAppliedGeneration + 1L);
                      return current;
                    }));

    projectionStore()
        .upsert(
            new StoredReconcileJobProjection(
                ACCOUNT_ID,
                connectorJobId,
                0L,
                "JS_WAITING",
                "Waiting on child work",
                50L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                "",
                true));

    StoredReconcileJobProjection connectorProjectionBeforeRead =
        projectionStore().load(ACCOUNT_ID, connectorJobId).orElseThrow();

    ReconcileJob rootSummary =
        store.listRootJobs(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
            .filter(job -> connectorJobId.equals(job.jobId))
            .findFirst()
            .orElseThrow();

    StoredReconcileJobProjection connectorProjectionAfterRead =
        projectionStore().load(ACCOUNT_ID, connectorJobId).orElseThrow();

    assertEquals("JS_SUCCEEDED", rootSummary.state);
    assertEquals(
        connectorProjectionBeforeRead.tablesScanned(),
        connectorProjectionAfterRead.tablesScanned());
    assertEquals(
        connectorProjectionBeforeRead.tablesChanged(),
        connectorProjectionAfterRead.tablesChanged());
    assertEquals(
        connectorProjectionBeforeRead.appliedGeneration(),
        connectorProjectionAfterRead.appliedGeneration());
  }

  @Test
  void refreshProjectedParentKeepsCanonicalTerminalChildStateWhenStoredProjectionIsStale() {
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

    var connectorLease = leaseJob(connectorJobId);
    store.markRunning(connectorJobId, connectorLease.leaseEpoch, 50L, "executor-connector");
    store.markWaiting(
        connectorJobId,
        connectorLease.leaseEpoch,
        60L,
        ReconcileJobStore.WaitingReason.CHILD_WORK_FINALIZED,
        "Waiting on child work",
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L);

    var tableLease = leaseJob(tableJobId);
    store.markRunning(tableJobId, tableLease.leaseEpoch, 100L, "executor-table");
    store.markSucceeded(tableJobId, tableLease.leaseEpoch, 200L, 1L, 1L, 0L, 0L, 0L, 0L);

    projectionStore()
        .upsert(
            new StoredReconcileJobProjection(
                ACCOUNT_ID,
                tableJobId,
                0L,
                "JS_WAITING",
                "Waiting on child work",
                100L,
                0L,
                1L,
                1L,
                0L,
                0L,
                0L,
                1L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                "",
                true));

    assertDoesNotThrow(
        () ->
            invokePrivateMethod(
                store,
                "refreshProjectedParent",
                new Class<?>[] {String.class, String.class},
                ACCOUNT_ID,
                connectorJobId));

    StoredReconcileJobProjection connectorProjection =
        projectionStore().load(ACCOUNT_ID, connectorJobId).orElseThrow();
    assertEquals("JS_SUCCEEDED", connectorProjection.state());
  }

  @Test
  void childJobsPagePaginatesDirectChildrenOnly() {
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
            ReconcileScope.of(List.of(), "orders-id"),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("src.ns", "orders", "orders-id", "orders"),
            ReconcileExecutionPolicy.defaults(),
            parentJobId,
            "");
    String childTwo =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "customers-id"),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("src.ns", "customers", "customers-id", "customers"),
            ReconcileExecutionPolicy.defaults(),
            parentJobId,
            "");
    store.enqueue(
        ACCOUNT_ID,
        CONNECTOR_ID,
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.of(List.of(), "nested-id"),
        ReconcileJobKind.PLAN_TABLE,
        ReconcileTableTask.of("src.ns", "nested", "nested-id", "nested"),
        ReconcileExecutionPolicy.defaults(),
        childOne,
        "");

    var first = store.childJobsPage(ACCOUNT_ID, parentJobId, 1, "");
    var second = store.childJobsPage(ACCOUNT_ID, parentJobId, 1, first.nextPageToken);

    assertEquals(1, first.jobs.size());
    assertEquals(1, second.jobs.size());
    assertNotEquals(first.jobs.getFirst().jobId, second.jobs.getFirst().jobId);
    assertTrue(
        Set.of(childOne, childTwo).contains(first.jobs.getFirst().jobId)
            && Set.of(childOne, childTwo).contains(second.jobs.getFirst().jobId));
  }

  @Test
  void connectorProjectionKeepsCanonicalPlanTableCountsWhenChildProjectionHasSubtreeTotals() {
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
            ReconcileScope.of(List.of(), "orders-id"),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("src.ns", "orders", "orders-id", "orders"),
            ReconcileExecutionPolicy.defaults(),
            connectorJobId,
            "");

    assertDoesNotThrow(
        () ->
            invokePrivateMethod(
                store,
                "mutateByCanonicalPointerReturningRecord",
                new Class<?>[] {String.class, UnaryOperator.class},
                Keys.reconcileJobPointerById(ACCOUNT_ID, tableJobId),
                (UnaryOperator<StoredReconcileJob>)
                    current -> {
                      current.state = "JS_SUCCEEDED";
                      current.message = "Succeeded";
                      current.tablesScanned = 1L;
                      current.tablesChanged = 1L;
                      current.startedAtMs = 100L;
                      current.finishedAtMs = 200L;
                      current.updatedAtMs = 200L;
                      return current;
                    }));

    projectionStore()
        .upsert(
            new StoredReconcileJobProjection(
                ACCOUNT_ID,
                tableJobId,
                1L,
                "JS_SUCCEEDED",
                "Succeeded",
                100L,
                200L,
                2L,
                2L,
                0L,
                0L,
                0L,
                1L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                "",
                true));

    StoredReconcileJob connectorRecord =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId));
    StoredReconcileJobProjection projection =
        (StoredReconcileJobProjection)
            assertDoesNotThrow(
                () ->
                    invokePrivateMethod(
                        store,
                        "recomputeSummaryProjection",
                        new Class<?>[] {StoredReconcileJob.class, boolean.class, boolean.class},
                        connectorRecord,
                        false,
                        true));

    assertEquals(1L, projection.tablesScanned());
    assertEquals(1L, projection.tablesChanged());
    assertEquals(1L, projection.snapshotsProcessed());
  }

  @Test
  void readPathsDoNotRefreshStaleRootProjection() {
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
            ReconcileScope.of(List.of(), "orders-id"),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("src.ns", "orders", "orders-id", "orders"),
            ReconcileExecutionPolicy.defaults(),
            connectorJobId,
            "");

    assertDoesNotThrow(
        () ->
            store.jobIndexStore.mutateByCanonicalPointerReturningRecord(
                Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId),
                current -> {
                  current.state = "JS_WAITING";
                  current.message = "Waiting on child work";
                  current.startedAtMs = 10L;
                  current.projectionRequestedGeneration = 11L;
                  current.projectionAppliedGeneration = 10L;
                  current.updatedAtMs = Math.max(current.updatedAtMs, 10L);
                  return current;
                }));
    assertDoesNotThrow(
        () ->
            store.jobIndexStore.mutateByCanonicalPointerReturningRecord(
                Keys.reconcileJobPointerById(ACCOUNT_ID, tableJobId),
                current -> {
                  current.state = "JS_SUCCEEDED";
                  current.message = "Succeeded";
                  current.tablesScanned = 1L;
                  current.tablesChanged = 1L;
                  current.startedAtMs = 20L;
                  current.finishedAtMs = 30L;
                  current.updatedAtMs = 30L;
                  return current;
                }));

    projectionStore()
        .upsert(
            new StoredReconcileJobProjection(
                ACCOUNT_ID,
                tableJobId,
                10L,
                "JS_SUCCEEDED",
                "Succeeded",
                20L,
                30L,
                1L,
                1L,
                0L,
                0L,
                0L,
                1L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                "",
                true));
    projectionStore()
        .upsert(
            new StoredReconcileJobProjection(
                ACCOUNT_ID,
                connectorJobId,
                10L,
                "JS_WAITING",
                "Waiting on child work",
                10L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                "",
                false));

    var summaryBeforeRead =
        rootSummaryStore()
            .listSummaries(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of())
            .summaries()
            .stream()
            .filter(summary -> connectorJobId.equals(summary.jobId()))
            .findFirst()
            .orElseThrow();

    ReconcileJob fromGet = store.get(ACCOUNT_ID, connectorJobId).orElseThrow();
    ReconcileJob fromRootList =
        store.listRootJobs(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
            .filter(job -> connectorJobId.equals(job.jobId))
            .findFirst()
            .orElseThrow();

    StoredReconcileJobProjection connectorProjectionAfterRead =
        projectionStore().load(ACCOUNT_ID, connectorJobId).orElseThrow();
    var summaryAfterRead =
        rootSummaryStore()
            .listSummaries(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of())
            .summaries()
            .stream()
            .filter(summary -> connectorJobId.equals(summary.jobId()))
            .findFirst()
            .orElseThrow();

    assertEquals("JS_WAITING", fromGet.state);
    assertEquals(summaryBeforeRead.state(), fromRootList.state);
    assertEquals(0L, fromGet.tablesScanned);
    assertEquals(0L, fromGet.tablesChanged);
    assertEquals(summaryBeforeRead.tablesScanned(), fromRootList.tablesScanned);
    assertEquals(summaryBeforeRead.tablesChanged(), fromRootList.tablesChanged);
    assertEquals(10L, connectorProjectionAfterRead.appliedGeneration());
    assertEquals(0L, connectorProjectionAfterRead.tablesScanned());
    assertEquals(0L, connectorProjectionAfterRead.tablesChanged());
    assertEquals(summaryBeforeRead.tablesScanned(), summaryAfterRead.tablesScanned());
    assertEquals(summaryBeforeRead.tablesChanged(), summaryAfterRead.tablesChanged());
  }

  @Test
  void requestProjectionRefreshCleansDirtyMarkerWhenGenerationAdvanceFails() {
    String missingParentJobId = "missing-parent";
    String dirtyMarkerKey = Keys.reconcileDirtyParentPointer(ACCOUNT_ID, missingParentJobId);

    InvocationTargetException failure =
        assertThrows(
            InvocationTargetException.class,
            () ->
                invokePrivateMethod(
                    store,
                    "requestProjectionRefresh",
                    new Class<?>[] {String.class, String.class, long.class},
                    ACCOUNT_ID,
                    missingParentJobId,
                    0L));

    assertTrue(failure.getCause() instanceof IllegalStateException);
    assertTrue(
        failure.getCause().getMessage().contains("Failed to advance reconcile parent projection"));
    assertTrue(store.pointerStore.get(dirtyMarkerKey).isEmpty());
  }

  @Test
  void requestProjectionRefreshRevertsGenerationAdvanceWhenMarkerWriteFails() {
    String parentJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    StoredReconcileJob before =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, parentJobId));
    Optional<Pointer> markerBefore =
        store.pointerStore.get(Keys.reconcileDirtyParentPointer(ACCOUNT_ID, parentJobId));
    PointerStore originalPointerStore = store.pointerStore;
    store.pointerStore = new DirtyParentFailingPointerStore(originalPointerStore);

    InvocationTargetException failure =
        assertThrows(
            InvocationTargetException.class,
            () ->
                invokePrivateMethod(
                    store,
                    "requestProjectionRefresh",
                    new Class<?>[] {String.class, String.class, long.class},
                    ACCOUNT_ID,
                    parentJobId,
                    1L));

    assertTrue(failure.getCause() instanceof IllegalStateException);
    StoredReconcileJob after =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, parentJobId));
    assertEquals(before.projectionRequestedGeneration, after.projectionRequestedGeneration);
    assertEquals(before.expectedDirectChildren, after.expectedDirectChildren);
    assertEquals(
        markerBefore,
        store.pointerStore.get(Keys.reconcileDirtyParentPointer(ACCOUNT_ID, parentJobId)));
  }

  @Test
  void leaseNextRespectsPinnedExecutorAndExecutionClass() {
    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileJobKind.PLAN_CONNECTOR,
            ReconcileTableTask.empty(),
            ReconcileViewTask.empty(),
            ReconcileSnapshotTask.empty(),
            ReconcileFileGroupTask.empty(),
            ReconcileExecutionPolicy.of(
                ReconcileExecutionClass.HEAVY, "remote", Map.of("worker", "remote")),
            "",
            "remote-executor");

    var lease =
        store
            .leaseNext(
                ReconcileJobStore.LeaseRequest.of(
                    java.util.EnumSet.of(ReconcileExecutionClass.HEAVY),
                    Set.of("remote"),
                    Set.of("remote-executor")))
            .orElseThrow();

    assertEquals(jobId, lease.jobId);
    assertEquals("remote-executor", lease.pinnedExecutorId);
    assertEquals(ReconcileExecutionClass.HEAVY, lease.executionPolicy.executionClass());
  }

  @Test
  void leaseNextStillFindsUnpinnedJobWhenExecutorIdsArePresent() {
    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileJobKind.PLAN_CONNECTOR,
            ReconcileTableTask.empty(),
            ReconcileViewTask.empty(),
            ReconcileSnapshotTask.empty(),
            ReconcileFileGroupTask.empty(),
            ReconcileExecutionPolicy.defaults(),
            "",
            "");

    var lease =
        store
            .leaseNext(
                ReconcileJobStore.LeaseRequest.of(
                    java.util.EnumSet.of(ReconcileExecutionClass.DEFAULT),
                    Set.of(ReconcileJobStore.LeaseRequest.anyLaneToken()),
                    Set.of("remote_planner_worker"),
                    java.util.EnumSet.of(ReconcileJobKind.PLAN_CONNECTOR)))
            .orElseThrow();

    assertEquals(jobId, lease.jobId);
    assertEquals("", lease.pinnedExecutorId);
    assertEquals(ReconcileExecutionClass.DEFAULT, lease.executionPolicy.executionClass());
  }

  @Test
  void leaseNextFallsBackToGlobalReadyRowForFilteredRequests() throws Exception {
    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileJobKind.PLAN_CONNECTOR,
            ReconcileTableTask.empty(),
            ReconcileViewTask.empty(),
            ReconcileSnapshotTask.empty(),
            ReconcileFileGroupTask.empty(),
            ReconcileExecutionPolicy.of(
                ReconcileExecutionClass.HEAVY, "remote", Map.of("worker", "remote")),
            "",
            "");

    StoredReconcileJob queued = readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, jobId));
    @SuppressWarnings("unchecked")
    List<String> readyKeys =
        (List<String>)
            invokePrivateMethod(
                store, "readyPointerKeys", new Class<?>[] {StoredReconcileJob.class}, queued);
    for (String readyKey : readyKeys) {
      if (readyKey.equals(queued.readyPointerKey)) {
        continue;
      }
      if (readyEntryExists(readyKey)) {
        assertTrue(deleteReadyEntry(readyKey));
      }
    }

    var lease =
        store
            .leaseNext(
                ReconcileJobStore.LeaseRequest.of(
                    java.util.EnumSet.of(ReconcileExecutionClass.HEAVY), Set.of("remote")))
            .orElseThrow();

    assertEquals(jobId, lease.jobId);
    assertEquals(ReconcileExecutionClass.HEAVY, lease.executionPolicy.executionClass());
    assertEquals("remote", lease.executionPolicy.lane());
  }

  @Test
  void rollbackLeaseCanonicalOnHydrationFailureDoesNotRegressNewerLeaseOwner() {
    String jobId =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, null);
    var oldLease = leaseJob(jobId);
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    StoredReconcileJob baseline = readStoredRecord(canonicalPointerKey);
    AtomicBoolean injectedNewerLease = new AtomicBoolean(false);
    Object leaseManager = leaseManager();
    Object originalMutator =
        assertDoesNotThrow(() -> getPrivateField(leaseManager, "mutateCanonicalJob"));

    assertDoesNotThrow(
        () ->
            setPrivateField(
                leaseManager,
                "mutateCanonicalJob",
                (ReconcileLeaseStore.CanonicalJobMutator)
                    (key, mutator) -> {
                      if (canonicalPointerKey.equals(key)
                          && injectedNewerLease.compareAndSet(false, true)) {
                        store.leaseStore.mutateLease(
                            ACCOUNT_ID,
                            jobId,
                            lease ->
                                ai.floedb.floecat.service.reconciler.jobs.durable.model
                                    .StoredJobLease.active(
                                    ACCOUNT_ID,
                                    jobId,
                                    "newer-lease",
                                    System.currentTimeMillis() + 60_000L));
                        store.jobIndexStore.mutateByJobIdReturningRecord(
                            jobId,
                            current -> {
                              current.state = "JS_RUNNING";
                              current.message = "Leased";
                              current.executorId = "newer-executor";
                              return current;
                            });
                      }
                      return ((ReconcileLeaseStore.CanonicalJobMutator) originalMutator)
                          .apply(key, mutator);
                    }));

    assertDoesNotThrow(
        () ->
            invokePrivateMethod(
                leaseManager,
                "rollbackLeaseCanonicalOnHydrationFailure",
                new Class<?>[] {String.class, StoredReconcileJob.class, String.class},
                canonicalPointerKey,
                baseline,
                oldLease.leaseEpoch));

    StoredReconcileJob current = readStoredRecord(canonicalPointerKey);
    assertEquals("JS_RUNNING", current.state);
    assertEquals("Leased", current.message);
    assertEquals("newer-executor", current.executorId);
    assertEquals("newer-lease", store.leaseStore.loadLease(ACCOUNT_ID, jobId).orElseThrow().epoch);

    assertDoesNotThrow(() -> setPrivateField(leaseManager, "mutateCanonicalJob", originalMutator));
  }

  @Test
  void failLeaseCanonicalOnHydrationFailureDoesNotFailNewerLeaseOwner() {
    String jobId =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, null);
    var oldLease = leaseJob(jobId);
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    StoredReconcileJob baseline = readStoredRecord(canonicalPointerKey);
    AtomicBoolean injectedNewerLease = new AtomicBoolean(false);
    Object leaseManager = leaseManager();
    Object originalMutator =
        assertDoesNotThrow(() -> getPrivateField(leaseManager, "mutateCanonicalJob"));

    assertDoesNotThrow(
        () ->
            setPrivateField(
                leaseManager,
                "mutateCanonicalJob",
                (ReconcileLeaseStore.CanonicalJobMutator)
                    (key, mutator) -> {
                      if (canonicalPointerKey.equals(key)
                          && injectedNewerLease.compareAndSet(false, true)) {
                        store.leaseStore.mutateLease(
                            ACCOUNT_ID,
                            jobId,
                            lease ->
                                ai.floedb.floecat.service.reconciler.jobs.durable.model
                                    .StoredJobLease.active(
                                    ACCOUNT_ID,
                                    jobId,
                                    "newer-lease",
                                    System.currentTimeMillis() + 60_000L));
                        store.jobIndexStore.mutateByJobIdReturningRecord(
                            jobId,
                            current -> {
                              current.state = "JS_RUNNING";
                              current.message = "Leased";
                              current.executorId = "newer-executor";
                              return current;
                            });
                      }
                      return ((ReconcileLeaseStore.CanonicalJobMutator) originalMutator)
                          .apply(key, mutator);
                    }));

    assertDoesNotThrow(
        () ->
            invokePrivateMethod(
                leaseManager,
                "failLeaseCanonicalOnHydrationFailure",
                new Class<?>[] {String.class, StoredReconcileJob.class, String.class, long.class},
                canonicalPointerKey,
                baseline,
                oldLease.leaseEpoch,
                System.currentTimeMillis()));

    StoredReconcileJob current = readStoredRecord(canonicalPointerKey);
    assertEquals("JS_RUNNING", current.state);
    assertEquals("Leased", current.message);
    assertEquals("newer-executor", current.executorId);
    assertEquals("newer-lease", store.leaseStore.loadLease(ACCOUNT_ID, jobId).orElseThrow().epoch);

    assertDoesNotThrow(() -> setPrivateField(leaseManager, "mutateCanonicalJob", originalMutator));
  }

  @Test
  void retryThenSuccessClearsPriorFailureState() {
    configureRetryPolicy(2, 0L, 0L);

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    var firstLease = leaseJob(jobId);
    store.markFailed(jobId, firstLease.leaseEpoch, 100L, "transient", 1L, 0L, 3L, 0L, 2L);

    var secondLease = leaseJob(jobId);
    assertTrue(!secondLease.leaseEpoch.isBlank());
    assertNotEquals(firstLease.leaseEpoch, secondLease.leaseEpoch);
    store.markSucceeded(jobId, secondLease.leaseEpoch, 200L, 1L, 1L, 0L, 0L, 0L, 0L);

    ReconcileJob terminal = store.getLeaseView(jobId).orElseThrow();
    assertEquals("JS_SUCCEEDED", terminal.state);
    assertEquals(0L, terminal.errors);
    assertEquals(200L, terminal.finishedAtMs);
  }

  @Test
  void retryThenFailureTransitionsToTerminalFailed() {
    configureRetryPolicy(2, 0L, 0L);

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    var firstLease = leaseJob(jobId);
    store.markFailed(jobId, firstLease.leaseEpoch, 100L, "transient", 1L, 0L, 3L, 0L, 2L);

    var secondLease = leaseJob(jobId);
    assertTrue(!secondLease.leaseEpoch.isBlank());
    assertNotEquals(firstLease.leaseEpoch, secondLease.leaseEpoch);
    store.markFailed(jobId, secondLease.leaseEpoch, 200L, "terminal", 1L, 0L, 5L, 0L, 2L);

    ReconcileJob failed = store.getLeaseView(jobId).orElseThrow();
    assertEquals("JS_FAILED", failed.state);
    assertEquals(200L, failed.finishedAtMs);
  }

  @Test
  void cancellingLeaseOutcomeResolvesImmediatelyToCancelled() {
    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    var lease = leaseJob(jobId);
    store.cancel(ACCOUNT_ID, jobId, "stop");
    store.markCancelled(jobId, lease.leaseEpoch, 300L, "stop", 0L, 0L, 0L, 0L, 0L);

    ReconcileJob cancelled = store.getLeaseView(jobId).orElseThrow();

    assertEquals("JS_CANCELLED", cancelled.state);
    assertEquals("stop", cancelled.message);
    assertEquals(300L, cancelled.finishedAtMs);
  }

  @Test
  void nestedWaitingParentsPromoteCanonicalStateBottomUp() {
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
            ReconcileTableTask.of("sales", "trino_types", "table-1", "trino_types"),
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
            ReconcileSnapshotTask.of("table-1", 55L, "sales", "trino_types", List.of(), true),
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

    var connectorLease = leaseJob(connectorJobId);
    store.markRunning(connectorJobId, connectorLease.leaseEpoch, 50L, "executor-connector");
    store.markWaiting(
        connectorJobId,
        connectorLease.leaseEpoch,
        60L,
        ReconcileJobStore.WaitingReason.CHILD_WORK_FINALIZED,
        "Waiting on child work",
        1L,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L);

    var tableLease = leaseJob(tableJobId);
    store.markRunning(tableJobId, tableLease.leaseEpoch, 100L, "executor-table");
    store.markWaiting(
        tableJobId,
        tableLease.leaseEpoch,
        110L,
        ReconcileJobStore.WaitingReason.CHILD_WORK_FINALIZED,
        "Waiting on child work",
        1L,
        1L,
        0L,
        0L,
        0L,
        0L,
        0L);

    assertDoesNotThrow(
        () ->
            invokePrivateMethod(
                store,
                "mutateByCanonicalPointerReturningRecord",
                new Class<?>[] {String.class, UnaryOperator.class},
                Keys.reconcileJobPointerById(ACCOUNT_ID, snapshotJobId),
                (UnaryOperator<StoredReconcileJob>)
                    current -> {
                      current.state = "JS_WAITING";
                      current.message =
                          "Snapshot plan recorded for sales.trino_types with 1 file group(s)";
                      current.startedAtMs = Math.max(current.startedAtMs, 120L);
                      current.finishedAtMs = 0L;
                      current.childrenFinalized = true;
                      current.readyPointerKey = null;
                      current.updatedAtMs = Math.max(current.updatedAtMs, 130L);
                      return current;
                    }));

    var execLease = leaseJob(execJobId);
    store.markRunning(execJobId, execLease.leaseEpoch, 150L, "executor-exec");
    store.markSucceeded(execJobId, execLease.leaseEpoch, 200L, 0L, 0L, 0L, 0L, 0L, 0L);

    StoredReconcileJob snapshotCanonical =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, snapshotJobId));
    StoredReconcileJob tableCanonical =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, tableJobId));
    StoredReconcileJob connectorCanonical =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId));

    assertEquals("JS_SUCCEEDED", snapshotCanonical.state);
    assertEquals("JS_SUCCEEDED", tableCanonical.state);
    assertEquals("JS_SUCCEEDED", connectorCanonical.state);
  }

  @Test
  void nestedFileGroupContributionRollsIntoProjectionAncestors() {
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

    var connectorLease = leaseJob(connectorJobId);
    store.markRunning(connectorJobId, connectorLease.leaseEpoch, 50L, "executor-connector");
    store.markWaiting(
        connectorJobId,
        connectorLease.leaseEpoch,
        60L,
        ReconcileJobStore.WaitingReason.CHILD_WORK_FINALIZED,
        "Waiting on child work",
        1L,
        0L,
        0L,
        0L,
        0L);

    var tableLease = leaseJob(tableJobId);
    store.markRunning(tableJobId, tableLease.leaseEpoch, 100L, "executor-table");
    store.markWaiting(
        tableJobId,
        tableLease.leaseEpoch,
        125L,
        ReconcileJobStore.WaitingReason.CHILD_WORK_FINALIZED,
        "Waiting on child work",
        1L,
        1L,
        0L,
        0L,
        0L);

    var snapshotLease = leaseJob(snapshotJobId);
    store.markRunning(snapshotJobId, snapshotLease.leaseEpoch, 130L, "executor-snapshot");
    store.markWaiting(
        snapshotJobId,
        snapshotLease.leaseEpoch,
        140L,
        ReconcileJobStore.WaitingReason.CHILD_WORK_FINALIZED,
        "Waiting on child work",
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L);

    var execLease = leaseJob(execJobId);
    store.markRunning(execJobId, execLease.leaseEpoch, 150L, "executor-exec");
    store.persistFileGroupResult(
        execJobId,
        execLease.leaseEpoch,
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
    StoredReconcileJob execCanonicalAfterResult =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, execJobId));
    if (execCanonicalAfterResult.fileGroupResultBlobUri != null
        && !execCanonicalAfterResult.fileGroupResultBlobUri.isBlank()) {
      store.blobStore.delete(execCanonicalAfterResult.fileGroupResultBlobUri);
    }
    store.markSucceeded(execJobId, execLease.leaseEpoch, 200L, 0L, 0L, 0L, 0L, 0L, 2L);
    runProjectionMaintenance();

    StoredReconcileJobProjection snapshotProjection =
        projectionStore().load(ACCOUNT_ID, snapshotJobId).orElseThrow();
    StoredReconcileJobProjection tableProjection =
        projectionStore().load(ACCOUNT_ID, tableJobId).orElseThrow();
    StoredReconcileJobProjection connectorProjection =
        projectionStore().load(ACCOUNT_ID, connectorJobId).orElseThrow();

    assertEquals("JS_SUCCEEDED", snapshotProjection.state());
    assertEquals(1L, snapshotProjection.plannedFileGroups());
    assertEquals(1L, snapshotProjection.completedFileGroups());
    assertEquals(1L, snapshotProjection.completedFiles());
    assertEquals(1L, snapshotProjection.failedFiles());
    assertEquals(1L, snapshotProjection.indexesProcessed());
    assertEquals(2L, snapshotProjection.statsProcessed());
    assertEquals(1L, tableProjection.plannedFileGroups());
    assertEquals(1L, tableProjection.completedFileGroups());
    assertEquals(1L, tableProjection.completedFiles());
    assertEquals(1L, tableProjection.failedFiles());
    assertEquals(1L, tableProjection.indexesProcessed());
    assertEquals(1L, connectorProjection.tablesScanned());
    assertEquals(1L, connectorProjection.tablesChanged());
    assertEquals(1L, connectorProjection.completedFileGroups());
    assertEquals(1L, connectorProjection.completedFiles());
    assertEquals(1L, connectorProjection.failedFiles());
    assertEquals(1L, connectorProjection.indexesProcessed());
  }

  @Test
  void succeededChildDoesNotOverwriteCancellingAncestor() {
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

    var connectorLease = leaseJob(connectorJobId);
    store.markRunning(connectorJobId, connectorLease.leaseEpoch, 50L, "executor-connector");
    store.cancel(ACCOUNT_ID, connectorJobId, "stop");

    var tableLease = leaseJob(tableJobId);
    store.markRunning(tableJobId, tableLease.leaseEpoch, 100L, "executor-table");
    store.markSucceeded(tableJobId, tableLease.leaseEpoch, 200L, 1L, 1L, 0L, 0L, 0L, 0L);

    ReconcileJob connector = store.getLeaseView(connectorJobId).orElseThrow();
    assertEquals("JS_CANCELLING", connector.state);
    assertEquals("stop", connector.message);
  }

  @Test
  void repeatedSnapshotFinalizationResetsFinalizedStatusUntilNewFinalizerSucceeds() {
    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of("table-1", 55L, "db", "orders", List.of(), true);

    String firstJobId =
        store.enqueueSnapshotFinalization(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            snapshotTask,
            ReconcileExecutionPolicy.defaults(),
            "",
            "");

    var firstLease = leaseJob(firstJobId);
    store.markRunning(firstJobId, firstLease.leaseEpoch, 100L, "executor-finalizer-1");
    store.markSucceeded(firstJobId, firstLease.leaseEpoch, 200L, 0L, 0L, 0L, 0L, 1L, 1L);

    Optional<ReconcileJobStore.FinalizedSnapshotEvent> firstFinalized =
        store.getFinalizedSnapshot(ACCOUNT_ID, "table-1", 55L);
    assertTrue(firstFinalized.isPresent());
    assertEquals(firstJobId, firstFinalized.orElseThrow().finalizerJobId);

    String secondJobId =
        store.enqueueSnapshotFinalization(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            snapshotTask,
            ReconcileExecutionPolicy.defaults(),
            "",
            "");

    assertNotEquals(firstJobId, secondJobId);
    assertFalse(store.getFinalizedSnapshot(ACCOUNT_ID, "table-1", 55L).isPresent());

    var secondLease = leaseJob(secondJobId);
    store.markRunning(secondJobId, secondLease.leaseEpoch, 300L, "executor-finalizer-2");
    store.markSucceeded(secondJobId, secondLease.leaseEpoch, 400L, 0L, 0L, 0L, 0L, 1L, 1L);

    Optional<ReconcileJobStore.FinalizedSnapshotEvent> secondFinalized =
        store.getFinalizedSnapshot(ACCOUNT_ID, "table-1", 55L);
    assertTrue(secondFinalized.isPresent());
    assertEquals(secondJobId, secondFinalized.orElseThrow().finalizerJobId);
    assertEquals(400L, secondFinalized.orElseThrow().finalizedAtMs);
  }

  @Test
  void persistSnapshotFinalizeDirectStatsProgressResetsLaterChunksOnFullRescanRestart() {
    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of(
            "table-1",
            55L,
            "db",
            "orders",
            List.of(),
            true,
            ReconcileSnapshotTask.CompletionMode.DIRECT_STATS,
            "blob://planner-direct-stats",
            0,
            0,
            "blob://planner-direct-stats",
            3);

    String jobId =
        store.enqueueSnapshotFinalization(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            snapshotTask,
            ReconcileExecutionPolicy.defaults(),
            "",
            "");

    var lease = leaseJob(jobId);
    store.markRunning(jobId, lease.leaseEpoch, 100L, "executor-finalizer");

    store.persistSnapshotFinalizeDirectStatsProgress(jobId, lease.leaseEpoch, false, 0, 1);
    store.persistSnapshotFinalizeDirectStatsProgress(jobId, lease.leaseEpoch, false, 1, 1);
    store.persistSnapshotFinalizeDirectStatsProgress(jobId, lease.leaseEpoch, false, 2, 1);

    ReconcileJob beforeRetry = store.getLeaseView(jobId).orElseThrow();
    assertEquals(3, beforeRetry.snapshotTask.directStatsPersistedRecordCount());
    assertEquals(
        Map.of(0, 1, 1, 1, 2, 1),
        beforeRetry.snapshotTask.directStatsPersistedRecordCountsByChunk());

    store.persistSnapshotFinalizeDirectStatsProgress(jobId, lease.leaseEpoch, true, 0, 1);

    ReconcileJob afterRetry = store.getLeaseView(jobId).orElseThrow();
    assertEquals(1, afterRetry.snapshotTask.directStatsPersistedRecordCount());
    assertEquals(Map.of(0, 1), afterRetry.snapshotTask.directStatsPersistedRecordCountsByChunk());
  }

  @Test
  void remoteApplyLeaseOutcomeSucceededRecordsFinalizedSnapshotAfterWaitingPass() {
    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of("table-1", 55L, "db", "orders", List.of(), true);

    String jobId =
        store.enqueueSnapshotFinalization(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            snapshotTask,
            ReconcileExecutionPolicy.defaults(),
            "",
            "");

    var firstLease = leaseJob(jobId);
    store.markRunning(jobId, firstLease.leaseEpoch, 100L, "executor-finalizer-1");
    assertTrue(
        store.applyLeaseOutcome(
            jobId,
            firstLease.leaseEpoch,
            ReconcileJobStore.CompletionKind.FAILED_WAITING_ON_DEPENDENCY,
            200L,
            "Waiting for snapshot file groups 0/1 pending=[group-0]",
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L));
    assertFalse(store.getFinalizedSnapshot(ACCOUNT_ID, "table-1", 55L).isPresent());

    var secondLease = leaseJob(jobId);
    store.markRunning(jobId, secondLease.leaseEpoch, 300L, "executor-finalizer-2");
    assertTrue(
        store.applyLeaseOutcome(
            jobId,
            secondLease.leaseEpoch,
            ReconcileJobStore.CompletionKind.SUCCEEDED,
            400L,
            "Finalized snapshot capture 55",
            0L,
            0L,
            0L,
            0L,
            0L,
            1L,
            1L));

    Optional<ReconcileJobStore.FinalizedSnapshotEvent> finalized =
        store.getFinalizedSnapshot(ACCOUNT_ID, "table-1", 55L);
    assertTrue(finalized.isPresent());
    assertEquals(jobId, finalized.orElseThrow().finalizerJobId);
    assertEquals(400L, finalized.orElseThrow().finalizedAtMs);
  }

  @Test
  void execFileGroupSuccessEnqueuesSnapshotFinalizationOnlyAfterLastGroupCompletes() {
    ReconcileFileGroupTask firstGroup =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", "table-1", 55L, List.of("s3://bucket/data/file-1.parquet"));
    ReconcileFileGroupTask secondGroup =
        ReconcileFileGroupTask.of(
            "plan-1", "group-2", "table-1", 55L, List.of("s3://bucket/data/file-2.parquet"));
    String snapshotJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileSnapshotTask.of(
                "table-1", 55L, "db", "orders", List.of(firstGroup, secondGroup), true),
            ReconcileExecutionPolicy.defaults(),
            "",
            "");
    String firstExecJobId =
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
            firstGroup,
            ReconcileExecutionPolicy.defaults(),
            snapshotJobId,
            "");
    String secondExecJobId =
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
            secondGroup,
            ReconcileExecutionPolicy.defaults(),
            snapshotJobId,
            "");

    var firstLease = leaseJob(firstExecJobId);
    store.markRunning(firstExecJobId, firstLease.leaseEpoch, 100L, "executor-exec-1");
    store.persistFileGroupResult(
        firstExecJobId,
        firstLease.leaseEpoch,
        ReconcileFileGroupTask.of(
            "plan-1",
            "group-1",
            "table-1",
            55L,
            List.of("s3://bucket/data/file-1.parquet"),
            List.of(ReconcileFileResult.succeeded("s3://bucket/data/file-1.parquet", 1L))));
    store.markSucceeded(firstExecJobId, firstLease.leaseEpoch, 150L, 0L, 0L, 0L, 0L, 0L, 1L);

    assertEquals(
        0L,
        store.childJobsPage(ACCOUNT_ID, snapshotJobId, 200, "").jobs.stream()
            .filter(job -> job.jobKind == ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE)
            .count());

    var secondLease = leaseJob(secondExecJobId);
    store.markRunning(secondExecJobId, secondLease.leaseEpoch, 200L, "executor-exec-2");
    store.persistFileGroupResult(
        secondExecJobId,
        secondLease.leaseEpoch,
        ReconcileFileGroupTask.of(
            "plan-1",
            "group-2",
            "table-1",
            55L,
            List.of("s3://bucket/data/file-2.parquet"),
            List.of(ReconcileFileResult.succeeded("s3://bucket/data/file-2.parquet", 1L))));
    store.markSucceeded(secondExecJobId, secondLease.leaseEpoch, 250L, 0L, 0L, 0L, 0L, 0L, 1L);

    List<ReconcileJob> children = store.childJobsPage(ACCOUNT_ID, snapshotJobId, 200, "").jobs;
    List<ReconcileJob> finalizers =
        children.stream()
            .filter(job -> job.jobKind == ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE)
            .toList();
    assertEquals(1, finalizers.size());
    assertEquals("JS_QUEUED", finalizers.get(0).state);
  }

  @Test
  void execFileGroupSuccessIgnoresUnexpectedSiblingChildrenWhenEnqueuingSnapshotFinalization() {
    ReconcileFileGroupTask firstGroup =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", "table-1", 55L, List.of("s3://bucket/data/file-1.parquet"));
    ReconcileFileGroupTask secondGroup =
        ReconcileFileGroupTask.of(
            "plan-1", "group-2", "table-1", 55L, List.of("s3://bucket/data/file-2.parquet"));
    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of(
            "table-1",
            55L,
            "db",
            "events",
            List.of(firstGroup, secondGroup),
            true,
            ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
            "",
            2);
    String snapshotJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            snapshotTask,
            ReconcileExecutionPolicy.defaults(),
            "",
            "");
    String firstExecJobId =
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
            firstGroup,
            ReconcileExecutionPolicy.defaults(),
            snapshotJobId,
            "");
    String secondExecJobId =
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
            secondGroup,
            ReconcileExecutionPolicy.defaults(),
            snapshotJobId,
            "");
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
            "other-plan",
            "other-group",
            "table-1",
            55L,
            List.of("s3://bucket/data/file-3.parquet")),
        ReconcileExecutionPolicy.defaults(),
        snapshotJobId,
        "");

    var firstLease = leaseJob(firstExecJobId);
    store.markRunning(firstExecJobId, firstLease.leaseEpoch, 100L, "executor-exec-1");
    store.persistFileGroupResult(
        firstExecJobId,
        firstLease.leaseEpoch,
        ReconcileFileGroupTask.of(
            "plan-1",
            "group-1",
            "table-1",
            55L,
            List.of("s3://bucket/data/file-1.parquet"),
            List.of(ReconcileFileResult.succeeded("s3://bucket/data/file-1.parquet", 1L))));
    store.markSucceeded(firstExecJobId, firstLease.leaseEpoch, 150L, 0L, 0L, 0L, 0L, 0L, 1L);

    var secondLease = leaseJob(secondExecJobId);
    store.markRunning(secondExecJobId, secondLease.leaseEpoch, 200L, "executor-exec-2");
    store.persistFileGroupResult(
        secondExecJobId,
        secondLease.leaseEpoch,
        ReconcileFileGroupTask.of(
            "plan-1",
            "group-2",
            "table-1",
            55L,
            List.of("s3://bucket/data/file-2.parquet"),
            List.of(ReconcileFileResult.succeeded("s3://bucket/data/file-2.parquet", 1L))));
    store.markSucceeded(secondExecJobId, secondLease.leaseEpoch, 250L, 0L, 0L, 0L, 0L, 0L, 1L);

    List<ReconcileJob> children = store.childJobsPage(ACCOUNT_ID, snapshotJobId, 200, "").jobs;
    List<ReconcileJob> finalizers =
        children.stream()
            .filter(job -> job.jobKind == ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE)
            .toList();
    assertEquals(1, finalizers.size());
  }

  @Test
  void execFileGroupDoesNotDedupeAcrossDifferentSnapshotParents() {
    ReconcileFileGroupTask group =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", "table-1", 55L, List.of("s3://bucket/data/file-1.parquet"));
    String firstParentJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "parent-table-1"),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("db", "parent_one", "parent-table-1", "parent_one"),
            ReconcileExecutionPolicy.defaults(),
            "",
            "");
    String secondParentJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "parent-table-2"),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("db", "parent_two", "parent-table-2", "parent_two"),
            ReconcileExecutionPolicy.defaults(),
            "",
            "");

    String firstExecJobId =
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
            group,
            ReconcileExecutionPolicy.defaults(),
            firstParentJobId,
            "");
    String secondExecJobId =
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
            group,
            ReconcileExecutionPolicy.defaults(),
            secondParentJobId,
            "");

    assertNotEquals(firstExecJobId, secondExecJobId);
    assertEquals(
        List.of(firstExecJobId),
        store.childJobsPage(ACCOUNT_ID, firstParentJobId, 200, "").jobs.stream()
            .filter(job -> job.jobKind == ReconcileJobKind.EXEC_FILE_GROUP)
            .map(job -> job.jobId)
            .toList());
    assertEquals(
        List.of(secondExecJobId),
        store.childJobsPage(ACCOUNT_ID, secondParentJobId, 200, "").jobs.stream()
            .filter(job -> job.jobKind == ReconcileJobKind.EXEC_FILE_GROUP)
            .map(job -> job.jobId)
            .toList());
  }

  @Test
  void detailReadsDegradeGracefullyWhenPayloadBlobsAreMissing() {
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
            "",
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

    var execLease = leaseJob(execJobId);
    store.markRunning(execJobId, execLease.leaseEpoch, 100L, "executor-exec");
    store.persistFileGroupResult(
        execJobId,
        execLease.leaseEpoch,
        ReconcileFileGroupTask.of(
            "plan-1",
            "group-1",
            "table-1",
            55L,
            List.of("s3://bucket/data/file-1.parquet", "s3://bucket/data/file-2.parquet"),
            List.of(
                ReconcileFileResult.succeeded("s3://bucket/data/file-1.parquet", 2L),
                ReconcileFileResult.failed("s3://bucket/data/file-2.parquet", "boom"))));

    StoredReconcileJob snapshotCanonical =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, snapshotJobId));
    StoredReconcileJob execCanonical =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, execJobId));
    store.blobStore.delete(snapshotCanonical.snapshotPlanBlobUri);
    store.blobStore.delete(execCanonical.fileGroupResultBlobUri);

    ReconcileJob snapshotJob =
        assertDoesNotThrow(() -> store.get(ACCOUNT_ID, snapshotJobId).orElseThrow());
    ReconcileJob execJob = assertDoesNotThrow(() -> store.get(ACCOUNT_ID, execJobId).orElseThrow());
    ReconcileJob execLeaseView =
        assertDoesNotThrow(() -> store.getLeaseView(execJobId).orElseThrow());

    assertEquals(
        snapshotCanonical.snapshotPlanBlobUri, snapshotJob.snapshotTask.fileGroupPlanBlobUri());
    assertEquals(1, snapshotJob.snapshotTask.fileGroupCount());
    assertTrue(snapshotJob.snapshotTask.fileGroups().isEmpty());

    assertEquals("plan-1", execJob.fileGroupTask.planId());
    assertEquals(2, execJob.fileGroupTask.fileCount());
    assertTrue(execJob.fileGroupTask.filePaths().isEmpty());
    assertTrue(execJob.fileGroupTask.fileResults().isEmpty());

    assertEquals("plan-1", execLeaseView.fileGroupTask.planId());
    assertEquals(2, execLeaseView.fileGroupTask.fileCount());
    assertTrue(execLeaseView.fileGroupTask.filePaths().isEmpty());
    assertTrue(execLeaseView.fileGroupTask.fileResults().isEmpty());
  }

  private ReconcileJobStore.LeasedJob leaseJob(String jobId) {
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    var snapshot =
        assertDoesNotThrow(() -> store.jobIndexStore.loadCanonicalSnapshot(canonicalPointerKey))
            .orElseThrow();
    StoredReconcileJob readyRecord = readStoredRecord(canonicalPointerKey);
    assertEquals("JS_QUEUED", readyRecord.state);
    return assertDoesNotThrow(
            () ->
                leaseManager()
                    .leaseCanonical(
                        canonicalPointerKey,
                        readyRecord.readyPointerKey,
                        System.currentTimeMillis(),
                        snapshot,
                        readyRecord))
        .orElseThrow(() -> new IllegalStateException("Unable to lease ready job " + jobId));
  }

  private DurableReconcileJobStore newIsolatedInMemoryStore(
      InMemoryPointerStore pointerStore,
      InMemoryBlobStore blobStore,
      ReconcileJobIndexBackend jobIndexBackend) {
    DurableReconcileJobStore isolatedStore = new DurableReconcileJobStore();
    isolatedStore.pointerStore = pointerStore;
    isolatedStore.blobStore = blobStore;
    isolatedStore.mapper = new ObjectMapper();
    isolatedStore.config = ConfigProvider.getConfig();
    isolatedStore.jobIndexBackend = jobIndexBackend;
    isolatedStore.leaseBackend = new MemoryReconcileLeaseBackend(pointerStore);
    isolatedStore.readyQueueBackend = new MemoryReconcileReadyQueueBackend(pointerStore);
    isolatedStore.init();
    return isolatedStore;
  }

  private boolean deleteReadyEntry(String readyPointerKey) {
    assertDoesNotThrow(() -> invokePrivateMethod(store, "readyQueue", new Class<?>[] {}));
    return store.readyQueueBackend.deleteReadyEntry(readyPointerKey);
  }

  private boolean readyEntryExists(String readyPointerKey) {
    assertDoesNotThrow(() -> invokePrivateMethod(store, "readyQueue", new Class<?>[] {}));
    return store.readyQueueBackend.loadCanonicalSnapshot(readyPointerKey, null).isPresent();
  }

  private ReconcileLeaseStore leaseManager() {
    return (ReconcileLeaseStore)
        assertDoesNotThrow(() -> invokePrivateMethod(store, "leaseManager", new Class<?>[] {}));
  }

  private void configureRetryPolicy(int maxAttempts, long baseBackoffMs, long maxBackoffMs) {
    assertDoesNotThrow(() -> setPrivateField(store, "maxAttempts", Math.max(1, maxAttempts)));
    assertDoesNotThrow(
        () -> setPrivateField(store, "baseBackoffMs", Math.max(100L, baseBackoffMs)));
    assertDoesNotThrow(() -> setPrivateField(store, "maxBackoffMs", Math.max(100L, maxBackoffMs)));
  }

  private ReconcileJobProjectionStore projectionStore() {
    return (ReconcileJobProjectionStore)
        assertDoesNotThrow(() -> invokePrivateMethod(store, "projections", new Class<?>[] {}));
  }

  private ReconcileJobRootSummaryStore rootSummaryStore() {
    return (ReconcileJobRootSummaryStore)
        assertDoesNotThrow(() -> invokePrivateMethod(store, "rootSummaries", new Class<?>[] {}));
  }

  private void markDirtyParent(String accountId, String parentJobId) {
    assertDoesNotThrow(
        () ->
            invokePrivateMethod(
                store,
                "markDirtyParent",
                new Class<?>[] {String.class, String.class},
                accountId,
                parentJobId));
  }

  private StoredReconcileJob readStoredRecord(String canonicalPointerKey) {
    return assertDoesNotThrow(
            () -> store.jobIndexStore.readCanonicalRecordByKey(canonicalPointerKey))
        .orElseThrow();
  }

  private void runProjectionMaintenance() {
    store.runProjectionMaintenanceOnce(isDynamoMode() ? 10_000L : 100L);
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

  private void reclaimExpiredLease(String jobId) {
    expireLease(jobId);
    long nowMs = System.currentTimeMillis();
    ReconcileLeaseStore.LeaseExpiryEntry leaseExpiryEntry =
        store.leaseStore.scanExpiredLeasePointersPage(nowMs, 100, "").entries().stream()
            .filter(
                entry ->
                    Keys.reconcileJobPointerById(ACCOUNT_ID, jobId)
                        .equals(entry.canonicalPointerKey()))
            .findFirst()
            .orElseThrow();
    store.leaseStore.reclaimExpiredLease(leaseExpiryEntry, nowMs);
  }

  private void makeQueuedJobDueNow(String jobId) {
    String canonicalPointerKey = Keys.reconcileJobPointerById(ACCOUNT_ID, jobId);
    long nowMs = System.currentTimeMillis();
    assertDoesNotThrow(
        () ->
            invokePrivateMethod(
                store,
                "mutateByCanonicalPointerReturningRecord",
                new Class<?>[] {String.class, UnaryOperator.class},
                canonicalPointerKey,
                (UnaryOperator<StoredReconcileJob>)
                    current -> {
                      current.nextAttemptAtMs = nowMs;
                      current.readyPointerKey =
                          assertDoesNotThrow(
                                  () ->
                                      invokePrivateMethod(
                                          store,
                                          "readyPointerKeyFor",
                                          new Class<?>[] {StoredReconcileJob.class, long.class},
                                          current,
                                          nowMs))
                              .toString();
                      return current;
                    }));
  }

  private void configureLeaseRenewGraceMs(long leaseRenewGraceMs) {
    assertDoesNotThrow(
        () -> setPrivateField(store, "leaseRenewGraceMs", Math.max(0L, leaseRenewGraceMs)));
  }

  private static final class DirtyParentFailingPointerStore implements PointerStore {
    private final PointerStore delegate;

    private DirtyParentFailingPointerStore(PointerStore delegate) {
      this.delegate = delegate;
    }

    @Override
    public Optional<Pointer> get(String key) {
      return delegate.get(key);
    }

    @Override
    public boolean compareAndSet(String key, long expectedVersion, Pointer next) {
      if (key != null && key.startsWith(Keys.reconcileDirtyParentPointerPrefix())) {
        return false;
      }
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
    public boolean compareAndSetBatch(List<CasOp> ops) {
      return delegate.compareAndSetBatch(ops);
    }

    @Override
    public List<Pointer> listPointersByPrefix(
        String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
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
  }

  private static final class FailingCompareAndSetBatchBackend implements ReconcileJobIndexBackend {
    private final ReconcileJobIndexBackend delegate;
    private final AtomicInteger armedCalls = new AtomicInteger();
    private volatile int failOnCall = -1;
    private volatile boolean failed;
    private volatile Runnable onFailedCompareAndSetBatch = () -> {};

    private FailingCompareAndSetBatchBackend(ReconcileJobIndexBackend delegate) {
      this.delegate = delegate;
    }

    void failOnArmedCall(int callNumber) {
      armedCalls.set(0);
      failOnCall = Math.max(1, callNumber);
      failed = false;
    }

    int armedCalls() {
      return armedCalls.get();
    }

    void onFailedCompareAndSetBatch(Runnable callback) {
      onFailedCompareAndSetBatch = callback == null ? () -> {} : callback;
    }

    @Override
    public Optional<JobIndexEntrySnapshot> loadIndexEntry(String pointerKey) {
      return delegate.loadIndexEntry(pointerKey);
    }

    @Override
    public boolean compareAndSetBatch(
        ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexStore
                .JobIndexWriteBatch
            batch) {
      int call = armedCalls.incrementAndGet();
      if (!failed && failOnCall > 0 && call == failOnCall) {
        failed = true;
        onFailedCompareAndSetBatch.run();
        return false;
      }
      return delegate.compareAndSetBatch(batch);
    }

    @Override
    public JobIndexQueryPage listCanonicalEntries(String accountId, int limit, String pageToken) {
      return delegate.listCanonicalEntries(accountId, limit, pageToken);
    }

    @Override
    public JobIndexQueryPage listDedupeEntries(String accountId, int limit, String pageToken) {
      return delegate.listDedupeEntries(accountId, limit, pageToken);
    }

    @Override
    public JobIndexQueryPage listParentEntries(
        String accountId, String parentJobId, int limit, String pageToken) {
      return delegate.listParentEntries(accountId, parentJobId, limit, pageToken);
    }

    @Override
    public JobIndexQueryPage listConnectorEntries(
        String accountId, String connectorId, int limit, String pageToken) {
      return delegate.listConnectorEntries(accountId, connectorId, limit, pageToken);
    }

    @Override
    public JobIndexQueryPage listGlobalStateEntries(String state, int limit, String pageToken) {
      return delegate.listGlobalStateEntries(state, limit, pageToken);
    }

    @Override
    public JobIndexQueryPage listAccountStateEntries(
        String accountId, String state, int limit, String pageToken) {
      return delegate.listAccountStateEntries(accountId, state, limit, pageToken);
    }

    @Override
    public JobIndexQueryPage listConnectorStateEntries(
        String accountId, String connectorId, String state, int limit, String pageToken) {
      return delegate.listConnectorStateEntries(accountId, connectorId, state, limit, pageToken);
    }

    @Override
    public boolean purgeEntriesByCanonicalReference(String canonicalPointerKey) {
      return delegate.purgeEntriesByCanonicalReference(canonicalPointerKey);
    }
  }

  private Object invokePrivateMethod(Object target, String name, Class<?>[] parameterTypes)
      throws Exception {
    return invokePrivateMethod(target, name, parameterTypes, new Object[] {});
  }

  private Object invokePrivateMethod(
      Object target, String name, Class<?>[] parameterTypes, Object... args) throws Exception {
    Method method = target.getClass().getDeclaredMethod(name, parameterTypes);
    method.setAccessible(true);
    return method.invoke(target, args);
  }

  private void setPrivateField(Object target, String name, Object value) throws Exception {
    java.lang.reflect.Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }

  private Object getPrivateField(Object target, String name) throws Exception {
    java.lang.reflect.Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.get(target);
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
}
