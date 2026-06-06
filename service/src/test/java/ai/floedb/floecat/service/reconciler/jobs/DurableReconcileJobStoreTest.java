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
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import ai.floedb.floecat.service.it.profiles.ReconcileJobStoreControlPlaneProfile;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobProjection;
import ai.floedb.floecat.service.reconciler.jobs.durable.projection.ReconcileJobProjectionStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.projection.ReconcileJobRootSummaryStore;
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
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
  private static final Pattern JOB_ID_PATTERN =
      Pattern.compile("([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})");
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
    assertEquals("JS_RUNNING", store.get(ACCOUNT_ID, connectorJobId).orElseThrow().state);
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
        waitForValue(
            () -> readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, snapshotJobId)),
            current -> "JS_QUEUED".equals(current.state),
            "requeued snapshot job " + snapshotJobId);
    assertEquals("Lease expired; requeued", requeued.message);

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
    assertEquals("JS_QUEUED", store.get(ACCOUNT_ID, snapshotJobId).orElseThrow().state);
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
        waitForValue(
            () -> readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, tableJobId)),
            current -> "JS_QUEUED".equals(current.state),
            "requeued table job " + tableJobId);
    assertEquals("Lease expired; requeued", requeued.message);

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
    assertEquals("JS_QUEUED", store.get(ACCOUNT_ID, tableJobId).orElseThrow().state);
  }

  @Test
  void enqueueMakesRootJobImmediatelyVisibleInRootList() {
    String connectorJobId =
        store.enqueue(ACCOUNT_ID, CONNECTOR_ID, false, CaptureMode.METADATA_AND_CAPTURE, null);

    StoredReconcileJob canonical =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId));
    ReconcileJob listed =
        store.listRootJobs(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
            .filter(job -> connectorJobId.equals(job.jobId))
            .findFirst()
            .orElseThrow();

    assertEquals(canonical.jobId, listed.jobId);
    assertEquals(canonical.state, listed.state);
    assertEquals(canonical.message, listed.message);
    assertEquals(canonical.startedAtMs, listed.startedAtMs);
    assertEquals(canonical.finishedAtMs, listed.finishedAtMs);
  }

  @Test
  void completedWaitingParentPromotesCanonicalStateAndDoesNotDedupe() {
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
        1L,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L);

    var tableLease = leaseJob(tableJobId);
    store.markRunning(tableJobId, tableLease.leaseEpoch, 100L, "executor-table");
    store.markSucceeded(tableJobId, tableLease.leaseEpoch, 200L, 1L, 1L, 0L, 0L, 0L, 0L);

    StoredReconcileJob canonical =
        waitForValue(
            () -> readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId)),
            current -> "JS_SUCCEEDED".equals(current.state),
            "canonical connector promotion " + connectorJobId);

    ReconcileJob rootSummary =
        waitForValue(
            () ->
                store.listRootJobs(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
                    .filter(job -> connectorJobId.equals(job.jobId))
                    .findFirst()
                    .orElseThrow(),
            current -> "JS_SUCCEEDED".equals(current.state),
            "root summary connector promotion " + connectorJobId);

    assertEquals("JS_SUCCEEDED", canonical.state);
    assertEquals("JS_SUCCEEDED", rootSummary.state);
    assertEquals("Succeeded", rootSummary.message);

    String second =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());

    assertNotEquals(connectorJobId, second);
  }

  @Test
  void completedWaitingParentPromotesCanonicalStateEvenIfParentLeaseIsStillLive() {
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

    assertDoesNotThrow(
        () ->
            invokePrivateMethod(
                store,
                "mutateByCanonicalPointerReturningRecord",
                new Class<?>[] {String.class, UnaryOperator.class},
                Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId),
                (UnaryOperator<StoredReconcileJob>)
                    current -> {
                      current.state = "JS_WAITING";
                      current.message = "Waiting on child work";
                      current.childrenFinalized = true;
                      current.startedAtMs = 50L;
                      return current;
                    }));

    var tableLease = leaseJob(tableJobId);
    store.markRunning(tableJobId, tableLease.leaseEpoch, 100L, "executor-table");
    store.markSucceeded(tableJobId, tableLease.leaseEpoch, 200L, 1L, 1L, 0L, 0L, 0L, 0L);

    StoredReconcileJob canonical =
        waitForValue(
            () -> readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId)),
            current -> "JS_SUCCEEDED".equals(current.state),
            "canonical connector promotion with live waiting lease " + connectorJobId);

    ReconcileJob rootSummary =
        waitForValue(
            () ->
                store.listRootJobs(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
                    .filter(job -> connectorJobId.equals(job.jobId))
                    .findFirst()
                    .orElseThrow(),
            current -> "JS_SUCCEEDED".equals(current.state),
            "root summary connector promotion with live waiting lease " + connectorJobId);

    assertEquals("JS_SUCCEEDED", canonical.state);
    assertEquals("JS_SUCCEEDED", rootSummary.state);
    assertEquals("Succeeded", rootSummary.message);
  }

  @Test
  void connectorProjectionDedupesDuplicateDirectPlanTableChildrenByLogicalTable() {
    String connectorJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    String firstTableJobId =
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
    String secondTableJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-2"),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("db", "orders_retry", "table-2", "orders_retry"),
            ReconcileExecutionPolicy.defaults(),
            connectorJobId,
            "");

    assertDoesNotThrow(
        () ->
            invokePrivateMethod(
                store,
                "mutateByCanonicalPointerReturningRecord",
                new Class<?>[] {String.class, UnaryOperator.class},
                Keys.reconcileJobPointerById(ACCOUNT_ID, secondTableJobId),
                (UnaryOperator<StoredReconcileJob>)
                    current -> {
                      current.definition.taskDestinationTableId = "table-1";
                      current.definition.sourceNamespace = "db";
                      current.definition.sourceTable = "orders";
                      current.updatedAtMs = Math.max(current.updatedAtMs, 2L);
                      return current;
                    }));

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

    var firstLease = leaseJob(firstTableJobId);
    store.markRunning(firstTableJobId, firstLease.leaseEpoch, 100L, "executor-table-1");
    store.markSucceeded(firstTableJobId, firstLease.leaseEpoch, 200L, 1L, 1L, 0L, 0L, 0L, 0L);

    var secondLease = leaseJob(secondTableJobId);
    store.markRunning(secondTableJobId, secondLease.leaseEpoch, 120L, "executor-table-2");
    store.markSucceeded(secondTableJobId, secondLease.leaseEpoch, 220L, 1L, 1L, 0L, 0L, 0L, 0L);

    ReconcileJob rootSummary =
        waitForValue(
            () ->
                store.listRootJobs(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
                    .filter(job -> connectorJobId.equals(job.jobId))
                    .findFirst()
                    .orElseThrow(),
            current -> "JS_SUCCEEDED".equals(current.state),
            "deduped connector root summary " + connectorJobId);

    assertEquals("JS_SUCCEEDED", rootSummary.state);
    assertEquals("Succeeded", rootSummary.message);
    assertEquals(1L, rootSummary.tablesScanned);
    assertEquals(1L, rootSummary.tablesChanged);
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
  void repeatedParentProjectionRefreshKeepsExactAggregateCounts() {
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
                  current.updatedAtMs = 10L;
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
                1L,
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

    markDirtyParent(ACCOUNT_ID, connectorJobId);
    runMaintenance();

    StoredReconcileJobProjection firstProjection =
        waitForValue(
                () -> projectionStore().load(ACCOUNT_ID, connectorJobId),
                current ->
                    current.isPresent()
                        && current.get().tablesScanned() == 1L
                        && current.get().tablesChanged() == 1L,
                "first connector projection refresh " + connectorJobId)
            .orElseThrow();
    ReconcileJob firstRootSummary =
        waitForValue(
            () ->
                store.listRootJobs(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
                    .filter(job -> connectorJobId.equals(job.jobId))
                    .findFirst()
                    .orElseThrow(),
            current -> current.tablesScanned == 1L && current.tablesChanged == 1L,
            "first root summary refresh " + connectorJobId);

    markDirtyParent(ACCOUNT_ID, connectorJobId);
    runMaintenance();

    StoredReconcileJobProjection secondProjection =
        waitForValue(
                () -> projectionStore().load(ACCOUNT_ID, connectorJobId),
                current ->
                    current.isPresent()
                        && current.get().tablesScanned() == 1L
                        && current.get().tablesChanged() == 1L,
                "second connector projection refresh " + connectorJobId)
            .orElseThrow();
    ReconcileJob secondRootSummary =
        waitForValue(
            () ->
                store.listRootJobs(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
                    .filter(job -> connectorJobId.equals(job.jobId))
                    .findFirst()
                    .orElseThrow(),
            current -> current.tablesScanned == 1L && current.tablesChanged == 1L,
            "second root summary refresh " + connectorJobId);

    assertEquals(1L, firstProjection.tablesScanned());
    assertEquals(1L, firstProjection.tablesChanged());
    assertEquals(1L, secondProjection.tablesScanned());
    assertEquals(1L, secondProjection.tablesChanged());
    assertEquals(1L, firstRootSummary.tablesScanned);
    assertEquals(1L, firstRootSummary.tablesChanged);
    assertEquals(1L, secondRootSummary.tablesScanned);
    assertEquals(1L, secondRootSummary.tablesChanged);
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

    ReconcileJob terminal =
        waitForValue(
            () -> store.getLeaseView(jobId).orElseThrow(),
            current -> "JS_SUCCEEDED".equals(current.state) && current.errors == 0L,
            "successful retry canonical view " + jobId);
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

    ReconcileJob failed =
        waitForValue(
            () -> store.getLeaseView(jobId).orElseThrow(),
            current -> "JS_FAILED".equals(current.state),
            "terminal failure canonical view " + jobId);
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

    ReconcileJob cancelled =
        waitForValue(
            () -> store.getLeaseView(jobId).orElseThrow(),
            current -> "JS_CANCELLED".equals(current.state),
            "job cancellation canonical view " + jobId);

    assertEquals("JS_CANCELLED", cancelled.state);
    assertEquals("stop", cancelled.message);
    assertEquals(300L, cancelled.finishedAtMs);
  }

  @Test
  void parentHandoffProjectsTerminalRootSummaryFromChildCompletion() {
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
        0L);

    var tableLease = leaseJob(tableJobId);
    store.markRunning(tableJobId, tableLease.leaseEpoch, 100L, "executor-table");
    store.markSucceeded(tableJobId, tableLease.leaseEpoch, 200L, 1L, 1L, 0L, 0L, 1L, 0L);

    ReconcileJob rootSummary =
        waitForValue(
            () ->
                store.listRootJobs(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
                    .filter(job -> connectorJobId.equals(job.jobId))
                    .findFirst()
                    .orElseThrow(),
            current ->
                "JS_SUCCEEDED".equals(current.state)
                    && current.finishedAtMs == 200L
                    && current.tablesScanned == 1L
                    && current.tablesChanged == 1L,
            "terminal root summary for " + connectorJobId);

    assertEquals("JS_SUCCEEDED", rootSummary.state);
    assertEquals(200L, rootSummary.finishedAtMs);
    assertEquals(1L, rootSummary.tablesScanned);
    assertEquals(1L, rootSummary.tablesChanged);
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
        waitForValue(
            () -> readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, snapshotJobId)),
            current -> "JS_SUCCEEDED".equals(current.state),
            "canonical snapshot promotion " + snapshotJobId);
    StoredReconcileJob tableCanonical =
        waitForValue(
            () -> readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, tableJobId)),
            current -> "JS_SUCCEEDED".equals(current.state),
            "canonical table promotion " + tableJobId);
    StoredReconcileJob connectorCanonical =
        waitForValue(
            () -> readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId)),
            current -> "JS_SUCCEEDED".equals(current.state),
            "canonical connector promotion " + connectorJobId);

    assertEquals("JS_SUCCEEDED", snapshotCanonical.state);
    assertEquals("JS_SUCCEEDED", tableCanonical.state);
    assertEquals("JS_SUCCEEDED", connectorCanonical.state);
  }

  @Test
  void transactionalParentChainPromotesSchedulingBeforeExactProjectionRollups() {
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

    StoredReconcileJob snapshotCanonical =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, snapshotJobId));
    StoredReconcileJob tableCanonical =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, tableJobId));
    StoredReconcileJob connectorCanonical =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId));

    assertEquals("JS_SUCCEEDED", snapshotCanonical.state);
    assertEquals("JS_SUCCEEDED", tableCanonical.state);
    assertEquals("JS_SUCCEEDED", connectorCanonical.state);
    assertEquals(0L, snapshotCanonical.completedFileGroups);
    assertEquals(0L, snapshotCanonical.completedFiles);
    assertEquals(0L, snapshotCanonical.failedFiles);
    assertEquals(0L, snapshotCanonical.indexesProcessed);
    assertEquals(0L, snapshotCanonical.statsProcessed);
    assertEquals(0L, tableCanonical.completedFileGroups);
    assertEquals(0L, tableCanonical.completedFiles);
    assertEquals(0L, tableCanonical.failedFiles);
    assertEquals(0L, tableCanonical.indexesProcessed);
    assertEquals(0L, connectorCanonical.completedFileGroups);
    assertEquals(0L, connectorCanonical.completedFiles);
    assertEquals(0L, connectorCanonical.failedFiles);
    assertEquals(0L, connectorCanonical.indexesProcessed);

    ReconcileJob snapshot =
        waitForValue(
            () -> store.get(ACCOUNT_ID, snapshotJobId).orElseThrow(),
            current ->
                "JS_SUCCEEDED".equals(current.state)
                    && current.plannedFileGroups == 1L
                    && current.completedFileGroups == 1L
                    && current.completedFiles == 1L
                    && current.failedFiles == 1L
                    && current.indexesProcessed == 1L
                    && current.statsProcessed == 2L,
            "transactional snapshot projection rollup " + snapshotJobId);
    ReconcileJob table =
        waitForValue(
            () -> store.get(ACCOUNT_ID, tableJobId).orElseThrow(),
            current ->
                "JS_SUCCEEDED".equals(current.state)
                    && current.tablesScanned == 1L
                    && current.tablesChanged == 1L
                    && current.plannedFileGroups == 1L
                    && current.completedFileGroups == 1L
                    && current.completedFiles == 1L
                    && current.failedFiles == 1L
                    && current.indexesProcessed == 1L,
            "transactional table projection rollup " + tableJobId);
    ReconcileJob connector =
        waitForValue(
            () -> store.get(ACCOUNT_ID, connectorJobId).orElseThrow(),
            current ->
                "JS_SUCCEEDED".equals(current.state)
                    && current.tablesScanned == 1L
                    && current.tablesChanged == 1L
                    && current.completedFileGroups == 1L
                    && current.completedFiles == 1L
                    && current.failedFiles == 1L
                    && current.indexesProcessed == 1L,
            "transactional connector projection rollup " + connectorJobId);
    ReconcileJob rootSummary =
        waitForValue(
            () ->
                store.listRootJobs(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
                    .filter(job -> connectorJobId.equals(job.jobId))
                    .findFirst()
                    .orElseThrow(),
            current ->
                "JS_SUCCEEDED".equals(current.state)
                    && current.tablesScanned == 1L
                    && current.tablesChanged == 1L
                    && current.completedFileGroups == 1L
                    && current.completedFiles == 1L
                    && current.failedFiles == 1L
                    && current.indexesProcessed == 1L,
            "transactional root summary rollup " + connectorJobId);

    assertEquals(1L, snapshot.completedFileGroups);
    assertEquals(1L, snapshot.completedFiles);
    assertEquals(1L, snapshot.failedFiles);
    assertEquals(1L, snapshot.indexesProcessed);
    assertEquals(2L, snapshot.statsProcessed);
    assertEquals(1L, table.tablesScanned);
    assertEquals(1L, table.tablesChanged);
    assertEquals(1L, table.completedFileGroups);
    assertEquals(1L, table.completedFiles);
    assertEquals(1L, table.failedFiles);
    assertEquals(1L, table.indexesProcessed);
    assertEquals(1L, connector.tablesScanned);
    assertEquals(1L, connector.tablesChanged);
    assertEquals(1L, connector.completedFileGroups);
    assertEquals(1L, connector.completedFiles);
    assertEquals(1L, connector.failedFiles);
    assertEquals(1L, connector.indexesProcessed);
    assertEquals(1L, rootSummary.tablesScanned);
    assertEquals(1L, rootSummary.tablesChanged);
    assertEquals(1L, rootSummary.completedFileGroups);
    assertEquals(1L, rootSummary.completedFiles);
    assertEquals(1L, rootSummary.failedFiles);
    assertEquals(1L, rootSummary.indexesProcessed);
  }

  @Test
  void connectorProjectionSucceedsWhenCompletedLogicalTablesMatchObservedChildren() {
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

    assertDoesNotThrow(
        () ->
            invokePrivateMethod(
                store,
                "mutateByCanonicalPointerReturningRecord",
                new Class<?>[] {String.class, UnaryOperator.class},
                Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId),
                (UnaryOperator<StoredReconcileJob>)
                    current -> {
                      current.expectedDirectChildren = 2L;
                      current.projectionRequestedGeneration =
                          Math.max(1L, current.projectionRequestedGeneration + 1L);
                      return current;
                    }));

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
        0L);

    var tableLease = leaseJob(tableJobId);
    store.markRunning(tableJobId, tableLease.leaseEpoch, 100L, "executor-table");
    store.markSucceeded(tableJobId, tableLease.leaseEpoch, 200L, 1L, 1L, 0L, 0L, 1L, 0L);

    ReconcileJob rootSummary =
        waitForValue(
            () ->
                store.listRootJobs(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
                    .filter(job -> connectorJobId.equals(job.jobId))
                    .findFirst()
                    .orElseThrow(),
            current -> current.tablesScanned == 1L && current.tablesChanged == 1L,
            "incomplete child root summary for " + connectorJobId);

    assertEquals("JS_SUCCEEDED", rootSummary.state);
    assertEquals(1L, rootSummary.tablesScanned);
    assertEquals(1L, rootSummary.tablesChanged);
  }

  @Test
  void nestedFileGroupContributionRollsIntoCanonicalAncestors() {
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

    ReconcileJob snapshot =
        waitForValue(
            () -> store.get(ACCOUNT_ID, snapshotJobId).orElseThrow(),
            current ->
                "JS_SUCCEEDED".equals(current.state)
                    && current.plannedFileGroups == 1L
                    && current.completedFileGroups == 1L
                    && current.completedFiles == 1L
                    && current.failedFiles == 1L
                    && current.indexesProcessed == 1L
                    && current.statsProcessed == 2L,
            "snapshot projected rollup " + snapshotJobId);
    ReconcileJob table =
        waitForValue(
            () -> store.get(ACCOUNT_ID, tableJobId).orElseThrow(),
            current ->
                "JS_SUCCEEDED".equals(current.state)
                    && current.plannedFileGroups == 1L
                    && current.completedFileGroups == 1L
                    && current.completedFiles == 1L
                    && current.failedFiles == 1L
                    && current.indexesProcessed == 1L,
            "table projected rollup " + tableJobId);
    ReconcileJob connector =
        waitForValue(
            () -> store.get(ACCOUNT_ID, connectorJobId).orElseThrow(),
            current ->
                "JS_SUCCEEDED".equals(current.state)
                    && current.tablesScanned == 1L
                    && current.tablesChanged == 1L
                    && current.completedFileGroups == 1L
                    && current.completedFiles == 1L
                    && current.failedFiles == 1L
                    && current.indexesProcessed == 1L,
            "connector projected rollup " + connectorJobId);

    assertEquals("JS_SUCCEEDED", snapshot.state);
    assertEquals(1L, snapshot.plannedFileGroups);
    assertEquals(1L, snapshot.completedFileGroups);
    assertEquals(1L, snapshot.completedFiles);
    assertEquals(1L, snapshot.failedFiles);
    assertEquals(1L, snapshot.indexesProcessed);
    assertEquals(2L, snapshot.statsProcessed);
    assertEquals(1L, table.plannedFileGroups);
    assertEquals(1L, table.completedFileGroups);
    assertEquals(1L, table.completedFiles);
    assertEquals(1L, table.failedFiles);
    assertEquals(1L, table.indexesProcessed);
    assertEquals(1L, connector.tablesScanned);
    assertEquals(1L, connector.tablesChanged);
    assertEquals(1L, connector.completedFileGroups);
    assertEquals(1L, connector.completedFiles);
    assertEquals(1L, connector.failedFiles);
    assertEquals(1L, connector.indexesProcessed);
  }

  @Test
  void rootSummaryDoesNotPromoteWaitingRootToSucceededWhileDescendantsRemainActive() {
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
        110L,
        ReconcileJobStore.WaitingReason.CHILD_WORK_FINALIZED,
        "Waiting on child work",
        1L,
        1L,
        0L,
        0L,
        0L);

    projectionStore()
        .upsert(
            new StoredReconcileJobProjection(
                ACCOUNT_ID,
                tableJobId,
                0L,
                "JS_SUCCEEDED",
                "Succeeded",
                100L,
                200L,
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
    markDirtyParent(ACCOUNT_ID, connectorJobId);
    runMaintenance();

    StoredReconcileJobProjection tableProjection =
        waitForValue(
                () -> projectionStore().load(ACCOUNT_ID, tableJobId),
                current ->
                    current.isPresent()
                        && Set.of("JS_QUEUED", "JS_RUNNING", "JS_WAITING", "JS_CANCELLING")
                            .contains(current.get().state()),
                "active table projection " + tableJobId)
            .orElseThrow();
    ReconcileJob rootSummary =
        waitForValue(
            () ->
                store.listRootJobs(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
                    .filter(job -> connectorJobId.equals(job.jobId))
                    .findFirst()
                    .orElseThrow(),
            current ->
                Set.of("JS_QUEUED", "JS_RUNNING", "JS_WAITING", "JS_CANCELLING")
                        .contains(current.state)
                    && current.tablesScanned == 1L
                    && current.tablesChanged == 1L,
            "active root summary " + connectorJobId);

    assertTrue(
        Set.of("JS_QUEUED", "JS_RUNNING", "JS_WAITING", "JS_CANCELLING")
            .contains(tableProjection.state()));
    assertTrue(
        Set.of("JS_QUEUED", "JS_RUNNING", "JS_WAITING", "JS_CANCELLING")
            .contains(rootSummary.state));
    assertEquals(1L, rootSummary.tablesScanned);
    assertEquals(1L, rootSummary.tablesChanged);
  }

  @Test
  void waitingSnapshotWithSucceededMessageDoesNotPromoteAncestorsToSucceeded() {
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
        110L,
        ReconcileJobStore.WaitingReason.CHILD_WORK_FINALIZED,
        "Waiting on child work",
        1L,
        1L,
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
                      current.message = "Succeeded";
                      current.startedAtMs = Math.max(current.startedAtMs, 120L);
                      current.updatedAtMs = Math.max(current.updatedAtMs, 130L);
                      return current;
                    }));

    markDirtyParent(ACCOUNT_ID, tableJobId);
    markDirtyParent(ACCOUNT_ID, connectorJobId);
    runMaintenance();

    StoredReconcileJobProjection tableProjection =
        waitForValue(
                () -> projectionStore().load(ACCOUNT_ID, tableJobId),
                current ->
                    current.isPresent()
                        && Set.of("JS_QUEUED", "JS_RUNNING", "JS_WAITING", "JS_CANCELLING")
                            .contains(current.get().state()),
                "active table projection " + tableJobId)
            .orElseThrow();
    StoredReconcileJobProjection connectorProjection =
        waitForValue(
                () -> projectionStore().load(ACCOUNT_ID, connectorJobId),
                current ->
                    current.isPresent()
                        && Set.of("JS_QUEUED", "JS_RUNNING", "JS_WAITING", "JS_CANCELLING")
                            .contains(current.get().state()),
                "active connector projection " + connectorJobId)
            .orElseThrow();
    ReconcileJob rootSummary =
        waitForValue(
            () ->
                store.listRootJobs(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
                    .filter(job -> connectorJobId.equals(job.jobId))
                    .findFirst()
                    .orElseThrow(),
            current ->
                Set.of("JS_QUEUED", "JS_RUNNING", "JS_WAITING", "JS_CANCELLING")
                    .contains(current.state),
            "active root summary " + connectorJobId);

    assertTrue(
        Set.of("JS_QUEUED", "JS_RUNNING", "JS_WAITING", "JS_CANCELLING")
            .contains(tableProjection.state()));
    assertTrue(
        Set.of("JS_QUEUED", "JS_RUNNING", "JS_WAITING", "JS_CANCELLING")
            .contains(connectorProjection.state()));
    assertTrue(
        Set.of("JS_QUEUED", "JS_RUNNING", "JS_WAITING", "JS_CANCELLING")
            .contains(rootSummary.state));
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

    ReconcileJob connector =
        waitForValue(
            () -> store.getLeaseView(connectorJobId).orElseThrow(),
            current -> "JS_CANCELLING".equals(current.state),
            "cancelling connector canonical view " + connectorJobId);

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

    store.persistFileGroupResult(
        execJobId,
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
    Optional<ReconcileJobStore.LeasedJob> leased = Optional.empty();
    for (int attempt = 0; attempt < 100 && leased.isEmpty(); attempt++) {
      StoredReconcileJob readyRecord =
          waitForValue(
              () -> readStoredRecord(canonicalPointerKey),
              current ->
                  "JS_QUEUED".equals(current.state)
                      && current.readyPointerKey != null
                      && !current.readyPointerKey.isBlank(),
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

  private <T> T waitForValue(Supplier<T> supplier, Predicate<T> done, String description) {
    int attempts = isDynamoMode() ? 400 : 40;
    long sleepMs = isDynamoMode() ? 50L : 0L;
    runMaintenance();
    T value = tryGetValue(supplier);
    for (int attempt = 0; attempt < attempts; attempt++) {
      if (value != null && done.test(value)) {
        return value;
      }
      if (attempt + 1 < attempts && sleepMs > 0L) {
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
        "Timed out waiting for "
            + description
            + "; last value="
            + value
            + timeoutDiagnostics(description, value));
    return value;
  }

  private <T> T tryGetValue(Supplier<T> supplier) {
    try {
      return supplier.get();
    } catch (IllegalStateException | java.util.NoSuchElementException e) {
      return null;
    }
  }

  private void runMaintenance() {
    store.runMaintenanceOnce(isDynamoMode() ? 10_000L : 100L);
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

  private void configureLeaseRenewGraceMs(long leaseRenewGraceMs) {
    assertDoesNotThrow(
        () -> setPrivateField(store, "leaseRenewGraceMs", Math.max(0L, leaseRenewGraceMs)));
  }

  private String timeoutDiagnostics(String description, Object value) {
    StringBuilder out = new StringBuilder();
    out.append("\nTimeout diagnostics:");
    DebugContext context = debugContext(description, value);
    out.append("\n  context=").append(context);
    if (!context.accountId().isBlank() && !context.jobId().isBlank()) {
      String canonicalKey = Keys.reconcileJobPointerById(context.accountId(), context.jobId());
      out.append("\n  canonical=");
      try {
        out.append(store.jobIndexStore.readCanonicalRecordByKey(canonicalKey).orElse(null));
      } catch (Exception e) {
        out.append("<error ").append(e.getMessage()).append(">");
      }
      out.append("\n  projection=");
      try {
        out.append(projectionStore().load(context.accountId(), context.jobId()).orElse(null));
      } catch (Exception e) {
        out.append("<error ").append(e.getMessage()).append(">");
      }
      out.append("\n  dirtyMarker=");
      try {
        out.append(
            store
                .pointerStore
                .get(Keys.reconcileDirtyParentPointer(context.accountId(), context.jobId()))
                .orElse(null));
      } catch (Exception e) {
        out.append("<error ").append(e.getMessage()).append(">");
      }
      out.append("\n  rootSummary=");
      try {
        out.append(
            rootSummaryStore()
                .listSummaries(context.accountId(), 50, "", "", Set.of())
                .summaries()
                .stream()
                .filter(summary -> context.jobId().equals(summary.jobId()))
                .findFirst()
                .orElse(null));
      } catch (Exception e) {
        out.append("<error ").append(e.getMessage()).append(">");
      }
    }
    if (!context.accountId().isBlank()) {
      out.append("\n  accountCanonicalJobs=").append(listCanonicalJobs(context.accountId(), 20));
      out.append("\n  accountProjections=")
          .append(
              listPointersForPrefix(
                  Keys.reconcileJobProjectionPointerPrefix(context.accountId()), 20));
      out.append("\n  accountDirtyMarkers=")
          .append(listPointersForPrefix(Keys.reconcileDirtyParentPointerPrefix(), 20));
      out.append("\n  accountRootSummaries=");
      try {
        out.append(
            rootSummaryStore()
                .listSummaries(context.accountId(), 20, "", "", Set.of())
                .summaries());
      } catch (Exception e) {
        out.append("<error ").append(e.getMessage()).append(">");
      }
    }
    return out.toString();
  }

  private List<StoredReconcileJob> listCanonicalJobs(String accountId, int limit) {
    StringBuilder next = new StringBuilder();
    return store
        .pointerStore
        .listPointersByPrefix(Keys.reconcileJobPointerByIdPrefix(accountId), limit, "", next)
        .stream()
        .map(pointer -> readStoredRecord(pointer.getKey()))
        .toList();
  }

  private List<Pointer> listPointersForPrefix(String prefix, int limit) {
    StringBuilder next = new StringBuilder();
    return store.pointerStore.listPointersByPrefix(prefix, limit, "", next);
  }

  private DebugContext debugContext(String description, Object value) {
    String accountId = ACCOUNT_ID;
    String jobId = "";
    if (value instanceof ReconcileJob job) {
      accountId = blankToDefault(job.accountId, ACCOUNT_ID);
      jobId = blankToEmpty(job.jobId);
    } else if (value instanceof StoredReconcileJob job) {
      accountId = blankToDefault(job.accountId, ACCOUNT_ID);
      jobId = blankToEmpty(job.jobId);
    } else if (value instanceof Optional<?> optional && optional.isPresent()) {
      return debugContext(description, optional.get());
    } else if (value instanceof StoredReconcileJobProjection projection) {
      accountId = blankToDefault(projection.accountId(), ACCOUNT_ID);
      jobId = blankToEmpty(projection.jobId());
    }
    if (jobId.isBlank()) {
      Matcher matcher = JOB_ID_PATTERN.matcher(description == null ? "" : description);
      if (matcher.find()) {
        jobId = matcher.group(1);
      }
    }
    return new DebugContext(accountId, jobId);
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value;
  }

  private static String blankToDefault(String value, String fallback) {
    return value == null || value.isBlank() ? fallback : value;
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

  private record DebugContext(String accountId, String jobId) {}

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
