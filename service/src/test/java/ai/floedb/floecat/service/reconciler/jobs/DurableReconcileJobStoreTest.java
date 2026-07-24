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
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
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
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobListSummary;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobProjection;
import ai.floedb.floecat.service.reconciler.jobs.durable.projection.ReconcileJobProjectionStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.projection.ReconcileJobProjector;
import ai.floedb.floecat.service.reconciler.jobs.durable.projection.ReconcileJobRootSummaryStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.queue.ReconcileAncestorRollupService;
import ai.floedb.floecat.service.reconciler.jobs.durable.queue.ReconcileCancellationMaintenanceService;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.JobIndexEntrySnapshot;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.MemoryReconcileJobIndexBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.MemoryReconcileLeaseBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.MemoryReconcileReadyQueueBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexCleanupManifest;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileLeaseStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileReadyQueueStore;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.stats.spi.StatsStore.UnpublishedGenerationDeleteResult;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
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
          .bind(() -> sharedDynamoDbClient, store.kvTable);
      store.leaseBackend =
          new ai.floedb.floecat.service.reconciler.jobs.durable.store.DynamoReconcileLeaseBackend();
      ((ai.floedb.floecat.service.reconciler.jobs.durable.store.DynamoReconcileLeaseBackend)
              store.leaseBackend)
          .bind(() -> sharedDynamoDbClient, store.kvTable);
      store.readyQueueBackend =
          new ai.floedb.floecat.service.reconciler.jobs.durable.store
              .DynamoReconcileReadyQueueBackend();
      ((ai.floedb.floecat.service.reconciler.jobs.durable.store.DynamoReconcileReadyQueueBackend)
              store.readyQueueBackend)
          .bind(() -> sharedDynamoDbClient, store.kvTable);
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
  void enqueuePlanRejectsDeletedConnectorBeforeCreatingRootJob() {
    store.connectorRepo = Mockito.mock(ConnectorRepository.class);
    Mockito.when(store.connectorRepo.existsById(connectorResourceId())).thenReturn(false);

    ReconcileJobStore.BulkEnqueueResult result =
        store.bulkEnqueue(
            List.of(
                ReconcileJobStore.BulkEnqueueSpec.of(
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
                    "")));

    assertEquals(
        ReconcileJobStore.BulkEnqueueItemResult.FailureReason.CONNECTOR_DELETED,
        result.items.getFirst().failureReason);
    assertEquals(CONNECTOR_ID, result.items.getFirst().failureSubjectId);

    var thrown =
        assertThrows(
            DurableReconcileJobStore.ConnectorDeletedException.class,
            () ->
                store.enqueue(
                    ACCOUNT_ID,
                    CONNECTOR_ID,
                    false,
                    CaptureMode.METADATA_AND_CAPTURE,
                    ReconcileScope.empty()));

    assertEquals("connector deleted: " + CONNECTOR_ID, thrown.getMessage());
    assertTrue(store.listRootJobs(ACCOUNT_ID, 10, "", "", null).jobs.isEmpty());
  }

  @Test
  void enqueuePlanAllowsExistingCanonicalConnector() {
    store.connectorRepo = Mockito.mock(ConnectorRepository.class);
    Mockito.when(store.connectorRepo.existsById(connectorResourceId())).thenReturn(true);

    String jobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());

    assertTrue(store.get(ACCOUNT_ID, jobId).isPresent());
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
  void enqueuePlanDoesNotDedupeAcrossDifferentExecutionAttributes() {
    ReconcileScope scope = ReconcileScope.of(List.of(), "tbl");

    String first =
        store.enqueuePlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            scope,
            ReconcileExecutionPolicy.of(
                ReconcileExecutionClass.DEFAULT, "", Map.of("post_commit_transaction_id", "tx-1")),
            "");
    String second =
        store.enqueuePlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            scope,
            ReconcileExecutionPolicy.of(
                ReconcileExecutionClass.DEFAULT, "", Map.of("post_commit_transaction_id", "tx-2")),
            "");

    assertNotEquals(first, second);
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
    runProjectionMaintenance(4);

    assertEquals(
        "JS_SUCCEEDED",
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, firstParentJobId)).state);
    assertEquals(
        "JS_SUCCEEDED",
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, secondParentJobId)).state);
  }

  @Test
  void planSnapshotLeasesCanRunInParallelForDifferentSnapshotsOfSameTable() {
    ReconcileScope scope = ReconcileScope.of(List.of(), "table-1");
    ReconcileExecutionPolicy policy = ReconcileExecutionPolicy.defaults();
    String firstJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            scope,
            ReconcileSnapshotTask.of("table-1", 101L, "db", "orders"),
            policy,
            "",
            "");
    String secondJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            scope,
            ReconcileSnapshotTask.of("table-1", 202L, "db", "orders"),
            policy,
            "",
            "");

    var firstLease = leaseJob(firstJobId);
    store.markRunning(firstJobId, firstLease.leaseEpoch, 100L, "executor-snapshot-1");

    var secondLease =
        store
            .leaseNext(
                ReconcileJobStore.LeaseRequest.of(
                    java.util.EnumSet.of(ReconcileExecutionClass.DEFAULT),
                    Set.of(ReconcileJobStore.LeaseRequest.anyLaneToken()),
                    Set.of(),
                    java.util.EnumSet.of(ReconcileJobKind.PLAN_SNAPSHOT)))
            .orElseThrow();

    assertEquals(secondJobId, secondLease.jobId);
    assertEquals(ReconcileJobKind.PLAN_SNAPSHOT, secondLease.jobKind);
    assertEquals(202L, secondLease.snapshotTask.snapshotId());
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
    assertTrue(
        store
            .pointerStore
            .get(Keys.reconcileDirtyParentPointer(ACCOUNT_ID, connectorJobId))
            .isPresent());
    assertEquals("JS_WAITING", parentRecord.state);
    assertEquals(1, store.childJobs(ACCOUNT_ID, connectorJobId).size());
  }

  @Test
  void bulkEnqueueAndApplyLeaseOutcomeCommitsParentBeforeChunkRetryDedupesChildren() {
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

    AtomicBoolean parentCommittedBeforeChildChunkFailure = new AtomicBoolean(false);
    failingBackend.failOnArmedCall(2);
    failingBackend.onFailedCompareAndSetBatch(
        () -> {
          StoredReconcileJob committedParent =
              readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId));
          parentCommittedBeforeChildChunkFailure.set(
              "JS_WAITING".equals(committedParent.state)
                  && committedParent.expectedDirectChildren == childSpecs.size());
        });
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
    assertTrue(parentCommittedBeforeChildChunkFailure.get());
    StoredReconcileJob parentRecord =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId));
    assertEquals("JS_WAITING", parentRecord.state);
    assertEquals(24L, parentRecord.expectedDirectChildren);
    assertEquals(24, store.childJobs(ACCOUNT_ID, connectorJobId).size());
  }

  @Test
  void plannerOutcomeReplaySurvivesLeaseCleanupAndRejectsDifferentChildSet() {
    InMemoryPointerStore pointerStore = new InMemoryPointerStore();
    MemoryReconcileJobIndexBackend delegateBackend = new MemoryReconcileJobIndexBackend();
    delegateBackend.bind(pointerStore);
    FailingCompareAndSetBatchBackend failingBackend =
        new FailingCompareAndSetBatchBackend(delegateBackend);
    store = newIsolatedInMemoryStore(pointerStore, new InMemoryBlobStore(), failingBackend);
    configureLeaseRenewGraceMs(0L);

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

    failingBackend.throwOnArmedCall(2);
    assertThrows(
        IllegalStateException.class,
        () ->
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
                0L));

    StoredReconcileJob partiallyCommittedParent =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId));
    assertEquals("JS_WAITING", partiallyCommittedParent.state);
    assertFalse(partiallyCommittedParent.plannerOutcomeFingerprint.isBlank());
    assertEquals(connectorLease.leaseEpoch, partiallyCommittedParent.plannerOutcomeLeaseEpoch);
    assertTrue(store.childJobs(ACCOUNT_ID, connectorJobId).size() < childSpecs.size());

    ReconcileJob committedChild = store.childJobs(ACCOUNT_ID, connectorJobId).getFirst();
    var committedChildLease = leaseJob(committedChild.jobId);
    store.markRunning(committedChild.jobId, committedChildLease.leaseEpoch, 150L, "executor-table");
    store.markSucceeded(
        committedChild.jobId, committedChildLease.leaseEpoch, 175L, 1L, 1L, 0L, 0L, 0L, 0L);

    reclaimExpiredLease(connectorJobId);
    assertEquals(
        "JS_QUEUED",
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId)).state);
    var repairLease = leaseJob(connectorJobId);
    store.markRunning(connectorJobId, repairLease.leaseEpoch, 180L, "executor-repair");
    int partiallyCommittedChildCount = store.childJobs(ACCOUNT_ID, connectorJobId).size();
    assertThrows(
        IllegalStateException.class,
        () ->
            store.bulkEnqueueAndApplyLeaseOutcome(
                List.of(),
                connectorJobId,
                repairLease.leaseEpoch,
                ReconcileJobStore.CompletionKind.SUCCEEDED,
                200L,
                "Planned 0 table job(s)",
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L));
    StoredReconcileJob parentAfterEmptyReplay =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId));
    assertEquals("JS_RUNNING", parentAfterEmptyReplay.state);
    assertEquals(
        partiallyCommittedParent.plannerOutcomeFingerprint,
        parentAfterEmptyReplay.plannerOutcomeFingerprint);
    assertEquals(childSpecs.size(), parentAfterEmptyReplay.expectedDirectChildren);
    assertEquals(partiallyCommittedChildCount, store.childJobs(ACCOUNT_ID, connectorJobId).size());

    assertTrue(
        store.bulkEnqueueAndApplyLeaseOutcome(
            childSpecs,
            connectorJobId,
            repairLease.leaseEpoch,
            ReconcileJobStore.CompletionKind.SUCCEEDED_WAITING,
            201L,
            "Planned 24 table job(s)",
            24L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L));
    assertEquals(24, store.childJobs(ACCOUNT_ID, connectorJobId).size());
    assertTrue(
        store.getCompletionLeaseView(connectorJobId, repairLease.leaseEpoch, true).isPresent());

    List<ReconcileJobStore.BulkEnqueueSpec> differentChildSpecs =
        new ArrayList<>(childSpecs.subList(0, childSpecs.size() - 1));
    differentChildSpecs.add(
        ReconcileJobStore.BulkEnqueueSpec.of(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "replacement-table"),
            ReconcileJobKind.PLAN_TABLE,
            ReconcileTableTask.of("db", "replacement", "replacement-table", "replacement"),
            ReconcileViewTask.empty(),
            ReconcileSnapshotTask.empty(),
            ReconcileFileGroupTask.empty(),
            ReconcileExecutionPolicy.defaults(),
            connectorJobId,
            ""));
    assertThrows(
        IllegalStateException.class,
        () ->
            store.bulkEnqueueAndApplyLeaseOutcome(
                differentChildSpecs,
                connectorJobId,
                repairLease.leaseEpoch,
                ReconcileJobStore.CompletionKind.SUCCEEDED_WAITING,
                202L,
                "Planned 24 table job(s)",
                24L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L));
    assertEquals(24, store.childJobs(ACCOUNT_ID, connectorJobId).size());
  }

  @Test
  void connectorParentCanonicalStateSucceedsWhenSiblingTerminalCompletionsRace() {
    InMemoryPointerStore pointerStore = new InMemoryPointerStore();
    MemoryReconcileJobIndexBackend delegateBackend = new MemoryReconcileJobIndexBackend();
    delegateBackend.bind(pointerStore);
    HookedCompareAndSetBatchBackend hookedBackend =
        new HookedCompareAndSetBatchBackend(delegateBackend);
    store = newIsolatedInMemoryStore(pointerStore, new InMemoryBlobStore(), hookedBackend);

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
        java.util.stream.IntStream.range(0, 2)
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

    assertTrue(
        store.bulkEnqueueAndApplyLeaseOutcome(
            childSpecs,
            connectorJobId,
            connectorLease.leaseEpoch,
            ReconcileJobStore.CompletionKind.SUCCEEDED_WAITING,
            200L,
            "Planned 2 table job(s)",
            2L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L));

    List<ReconcileJob> children = store.childJobsPage(ACCOUNT_ID, connectorJobId, 20, "").jobs;
    assertEquals(2, children.size());
    ReconcileJob firstChild = children.get(0);
    ReconcileJob secondChild = children.get(1);
    var firstLease = leaseJob(firstChild.jobId);
    var secondLease = leaseJob(secondChild.jobId);
    store.markRunning(firstChild.jobId, firstLease.leaseEpoch, 300L, "executor-table-a");
    store.markRunning(secondChild.jobId, secondLease.leaseEpoch, 300L, "executor-table-b");

    String parentCanonicalKey = Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId);
    String firstChildCanonicalKey = Keys.reconcileJobPointerById(ACCOUNT_ID, firstChild.jobId);
    AtomicBoolean injectedSiblingCompletion = new AtomicBoolean(false);
    hookedBackend.beforeCompareAndSetBatch(
        batch -> {
          if (injectedSiblingCompletion.get()) {
            return;
          }
          if (!batchContainsCanonicalMutation(batch, firstChildCanonicalKey)
              || batchContainsCanonicalMutation(batch, parentCanonicalKey)) {
            return;
          }
          injectedSiblingCompletion.set(true);
          store.markSucceeded(
              secondChild.jobId, secondLease.leaseEpoch, 400L, 1L, 1L, 0L, 0L, 0L, 0L);
        });

    store.markSucceeded(firstChild.jobId, firstLease.leaseEpoch, 400L, 1L, 1L, 0L, 0L, 0L, 0L);
    runProjectionMaintenance(4);

    StoredReconcileJob parentRecord =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId));
    assertTrue(injectedSiblingCompletion.get());
    assertEquals("JS_SUCCEEDED", parentRecord.state);
    assertEquals("Succeeded", parentRecord.message);
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
    assertEquals(1, secondLease.orElseThrow().fileGroupTask.fileCount());
    assertTrue(secondLease.orElseThrow().fileGroupTask.filePaths().isEmpty());
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
  void leaseNextReportsDeadlineAbortAsCapacityExceededInsteadOfEmpty() {
    ReconcileReadyQueueStore original = store.readyQueueStore;
    ReconcileReadyQueueStore readyQueue = Mockito.mock(ReconcileReadyQueueStore.class);
    store.readyQueueStore = readyQueue;
    try {
      Mockito.when(readyQueue.leaseReadyDue(Mockito.anyLong(), Mockito.any(), Mockito.any()))
          .thenAnswer(
              invocation -> {
                ReconcileReadyQueueStore.LeaseScanStats stats = invocation.getArgument(2);
                stats.abortedByDeadline = true;
                return Optional.empty();
              });

      LeaseScanCapacityExceededException error =
          assertThrows(
              LeaseScanCapacityExceededException.class,
              () -> store.leaseNext(ReconcileJobStore.LeaseRequest.all()));

      assertTrue(error.getMessage().contains("deadline exceeded"));
    } finally {
      store.readyQueueStore = original;
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
  void terminalChildCommitAtomicallyQueuesDirtyParentProjection() {
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

    StoredReconcileJob connectorCanonical =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId));
    StoredReconcileJob tableCanonical =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, tableJobId));

    assertEquals("JS_SUCCEEDED", tableCanonical.state);
    assertEquals("JS_WAITING", connectorCanonical.state);
    assertTrue(
        store
            .pointerStore
            .get(Keys.reconcileDirtyParentPointer(ACCOUNT_ID, connectorJobId))
            .isPresent());
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
    runProjectionMaintenance();

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
  void listRootJobsShowsCanonicalCancellationWhenStoredProjectionIsStale() {
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
                3L,
                0L,
                0L,
                0L,
                0L,
                10L,
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

    store.cancel(ACCOUNT_ID, connectorJobId, "Cancelled");

    ReconcileJob rootSummary =
        store.listRootJobs(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
            .filter(job -> connectorJobId.equals(job.jobId))
            .findFirst()
            .orElseThrow();

    assertEquals("JS_CANCELLED", rootSummary.state);
    assertEquals("Cancelled", rootSummary.message);
    assertEquals(3L, rootSummary.tablesScanned);
    assertEquals(10L, rootSummary.snapshotsProcessed);
    assertTrue(rootSummary.finishedAtMs > 0L);
    assertTrue(store.get(ACCOUNT_ID, tableJobId).isPresent());
  }

  @Test
  void listRootJobsShowsCancelledWhenCancellingParentProjectionIsTerminal() {
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

    var tableLease = leaseJob(tableJobId);
    store.markRunning(tableJobId, tableLease.leaseEpoch, 100L, "executor-table");
    store.cancel(ACCOUNT_ID, connectorJobId, "Cancelled");
    runCancellationMaintenance();
    store.markCancelled(
        tableJobId, tableLease.leaseEpoch, 200L, "Cancelled", 1L, 1L, 0L, 0L, 0L, 7L, 0L);
    runCancellationMaintenance();
    runProjectionMaintenance();

    ReconcileJob treeRoot = store.childJobsPage(ACCOUNT_ID, connectorJobId, 20, "").jobs.get(0);
    ReconcileJob rootSummary =
        store.listRootJobs(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
            .filter(job -> connectorJobId.equals(job.jobId))
            .findFirst()
            .orElseThrow();

    assertEquals("JS_CANCELLED", store.getLeaseView(connectorJobId).orElseThrow().state);
    assertEquals("JS_CANCELLED", treeRoot.state);
    assertEquals("JS_CANCELLED", rootSummary.state);
    assertTrue(rootSummary.finishedAtMs > 0L);
  }

  @Test
  void finalizedCancelledParentIsNotMarkedForCleanupAgainWhenChildCompletes() {
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

    var tableLease = leaseJob(tableJobId);
    store.markRunning(tableJobId, tableLease.leaseEpoch, 100L, "executor-table");
    store.cancel(ACCOUNT_ID, connectorJobId, "Cancelled");
    runCancellationMaintenance();
    store.markCancelled(
        tableJobId, tableLease.leaseEpoch, 200L, "Cancelled", 1L, 1L, 0L, 0L, 0L, 7L, 0L);
    runCancellationMaintenance();

    StoredReconcileJob connector =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId));
    assertEquals("JS_CANCELLED", connector.state);
    assertTrue(connector.finishedAtMs > 0L);
    assertTrue(connector.childrenFinalized);
    assertTrue(
        store
            .pointerStore
            .get(Keys.reconcileCancellationCleanupPointer(ACCOUNT_ID, connectorJobId))
            .isEmpty());

    StoredReconcileJob table =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, tableJobId));
    assertDoesNotThrow(
        () ->
            invokePrivateMethod(
                store,
                "requestParentCancellationCleanupIfNeeded",
                new Class<?>[] {StoredReconcileJob.class},
                table));

    assertTrue(
        store
            .pointerStore
            .get(Keys.reconcileCancellationCleanupPointer(ACCOUNT_ID, connectorJobId))
            .isEmpty());
  }

  @Test
  void listRootJobsRepairsStaleCancellingSummaryWhenCanonicalRootIsCancelled() {
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

    var tableLease = leaseJob(tableJobId);
    store.markRunning(tableJobId, tableLease.leaseEpoch, 100L, "executor-table");
    store.cancel(ACCOUNT_ID, connectorJobId, "Cancelled");
    runCancellationMaintenance();
    store.markCancelled(
        tableJobId, tableLease.leaseEpoch, 200L, "Cancelled", 1L, 1L, 0L, 0L, 0L, 7L, 0L);
    runCancellationMaintenance();

    var cancelledSummary =
        rootSummaryStore()
            .listSummaries(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of())
            .summaries()
            .stream()
            .filter(summary -> connectorJobId.equals(summary.jobId()))
            .findFirst()
            .orElseThrow();
    rootSummaryStore()
        .upsert(
            new StoredReconcileJobListSummary(
                cancelledSummary.accountId(),
                cancelledSummary.jobId(),
                cancelledSummary.connectorId(),
                "JS_CANCELLING",
                "Cancelled",
                cancelledSummary.startedAtMs(),
                0L,
                cancelledSummary.tablesScanned(),
                cancelledSummary.tablesChanged(),
                cancelledSummary.viewsScanned(),
                cancelledSummary.viewsChanged(),
                cancelledSummary.errors(),
                cancelledSummary.fullRescan(),
                cancelledSummary.captureMode(),
                cancelledSummary.snapshotsProcessed(),
                cancelledSummary.statsProcessed(),
                cancelledSummary.indexesProcessed(),
                cancelledSummary.executorId(),
                cancelledSummary.executionClass(),
                cancelledSummary.executionLane(),
                cancelledSummary.executionAttributes(),
                cancelledSummary.jobKind(),
                cancelledSummary.plannedFileGroups(),
                cancelledSummary.plannedFiles(),
                cancelledSummary.completedFileGroups(),
                cancelledSummary.failedFileGroups(),
                cancelledSummary.completedFiles(),
                cancelledSummary.failedFiles(),
                cancelledSummary.createdAtMs()));

    ReconcileJob rootSummary =
        store.listRootJobs(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
            .filter(job -> connectorJobId.equals(job.jobId))
            .findFirst()
            .orElseThrow();
    var repairedSummary =
        rootSummaryStore()
            .listSummaries(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of())
            .summaries()
            .stream()
            .filter(summary -> connectorJobId.equals(summary.jobId()))
            .findFirst()
            .orElseThrow();

    assertEquals("JS_CANCELLED", store.getLeaseView(connectorJobId).orElseThrow().state);
    assertEquals("JS_CANCELLED", rootSummary.state);
    assertEquals("JS_CANCELLED", repairedSummary.state());
    assertTrue(repairedSummary.finishedAtMs() > 0L);
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

    requestProjectionRefresh(connectorJobId);
    runProjectionMaintenance(4);

    StoredReconcileJobProjection connectorProjection =
        projectionStore().load(ACCOUNT_ID, connectorJobId).orElseThrow();
    StoredReconcileJobProjection tableProjection =
        projectionStore().load(ACCOUNT_ID, tableJobId).orElseThrow();
    StoredReconcileJob connectorCanonical =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId));
    assertEquals("JS_SUCCEEDED", connectorProjection.state());
    assertEquals("JS_SUCCEEDED", tableProjection.state());
    assertEquals("JS_SUCCEEDED", connectorCanonical.state);
  }

  @Test
  void refreshProjectedParentDoesNotPublishProjectionSchedulingStateToCanonicalRecord() {
    String connectorJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
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

    StoredReconcileJob before =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId));
    projectionStore()
        .upsert(
            new StoredReconcileJobProjection(
                ACCOUNT_ID,
                connectorJobId,
                Math.max(0L, before.projectionRequestedGeneration),
                "JS_SUCCEEDED",
                "Succeeded",
                50L,
                100L,
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
                "executor-projection",
                true));

    requestProjectionRefresh(connectorJobId);
    runProjectionMaintenance(2);

    StoredReconcileJob after =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId));
    assertEquals("JS_WAITING", after.state);
    assertEquals("Waiting on child work", after.message);
    assertEquals(0L, after.finishedAtMs);
    assertEquals("", after.executorId);
    assertTrue(after.childrenFinalized);
  }

  @Test
  void markProgressMarksParentProjectionsDirtyForRollup() {
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
    long tableGenerationBefore =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, tableJobId))
            .projectionRequestedGeneration;
    long connectorGenerationBefore =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId))
            .projectionRequestedGeneration;

    store.markProgress(tableJobId, tableLease.leaseEpoch, 0L, 0L, 0L, 0L, 0L, 7L, 0L, "progress");

    StoredReconcileJob tableAfterProgress =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, tableJobId));
    StoredReconcileJob connectorAfterProgress =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId));

    assertEquals(tableGenerationBefore, tableAfterProgress.projectionRequestedGeneration);
    assertEquals(connectorGenerationBefore, connectorAfterProgress.projectionRequestedGeneration);
    assertTrue(
        store
            .pointerStore
            .get(Keys.reconcileDirtyParentPointer(ACCOUNT_ID, tableJobId))
            .isPresent());

    runProjectionMaintenance(4);

    StoredReconcileJobProjection connectorProjection =
        projectionStore().load(ACCOUNT_ID, connectorJobId).orElseThrow();
    assertEquals(7L, connectorProjection.snapshotsProcessed());
  }

  @Test
  void sameGenerationProjectionChangeMarksAncestorDirtyForRollup() {
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
    store.markProgress(tableJobId, tableLease.leaseEpoch, 0L, 0L, 0L, 0L, 0L, 1L, 0L, "progress");
    runProjectionMaintenance(4);
    assertTrue(
        store
            .pointerStore
            .get(Keys.reconcileDirtyParentPointer(ACCOUNT_ID, connectorJobId))
            .isEmpty());
    assertEquals(
        1L, projectionStore().load(ACCOUNT_ID, connectorJobId).orElseThrow().snapshotsProcessed());

    StoredReconcileJob before =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, tableJobId));
    assertEquals(before.projectionRequestedGeneration, before.projectionAppliedGeneration);
    assertDoesNotThrow(
        () ->
            invokePrivateMethod(
                store,
                "mutateByCanonicalPointerReturningRecord",
                new Class<?>[] {String.class, UnaryOperator.class},
                Keys.reconcileJobPointerById(ACCOUNT_ID, tableJobId),
                (UnaryOperator<StoredReconcileJob>)
                    current -> {
                      current.snapshotsProcessed = 7L;
                      current.statsProcessed = 11L;
                      current.indexesProcessed = 5L;
                      current.message = "Processing snapshot 7";
                      current.updatedAtMs = 200L;
                      return current;
                    }));

    requestProjectionRefresh(tableJobId);
    runProjectionMaintenance(2);

    runProjectionMaintenance(2);
    assertEquals(
        7L, projectionStore().load(ACCOUNT_ID, connectorJobId).orElseThrow().snapshotsProcessed());
    assertEquals(
        11L, projectionStore().load(ACCOUNT_ID, connectorJobId).orElseThrow().statsProcessed());
    assertEquals(
        5L, projectionStore().load(ACCOUNT_ID, connectorJobId).orElseThrow().indexesProcessed());
  }

  @Test
  void projectionStoreReplacesSameGenerationProjectionWhenAggregateChanges() {
    String jobId = "projection-job";
    projectionStore().upsert(projection(jobId, 5L, "JS_WAITING", "Waiting", 0L, 0L, 0L, 0L));

    projectionStore().upsert(projection(jobId, 5L, "JS_WAITING", "Processing", 3L, 2L, 17L, 11L));

    StoredReconcileJobProjection projection =
        projectionStore().load(ACCOUNT_ID, jobId).orElseThrow();
    assertEquals(5L, projection.appliedGeneration());
    assertEquals("Processing", projection.message());
    assertEquals(3L, projection.tablesScanned());
    assertEquals(2L, projection.tablesChanged());
    assertEquals(17L, projection.snapshotsProcessed());
    assertEquals(11L, projection.statsProcessed());
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

    requestProjectionRefresh(connectorJobId);
    runProjectionMaintenance(2);
    StoredReconcileJobProjection projection =
        projectionStore().load(ACCOUNT_ID, connectorJobId).orElseThrow();

    assertEquals(1L, projection.tablesScanned());
    assertEquals(1L, projection.tablesChanged());
    assertEquals(0L, projection.snapshotsProcessed());
  }

  @Test
  void ancestorRefreshIgnoresStoredChildProjection() {
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
    String snapshotJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "orders-id"),
            ReconcileJobKind.PLAN_SNAPSHOT,
            ReconcileTableTask.empty(),
            ReconcileViewTask.empty(),
            ReconcileSnapshotTask.of("orders-id", 55L, "src.ns", "orders"),
            ReconcileFileGroupTask.empty(),
            ReconcileExecutionPolicy.defaults(),
            tableJobId,
            "");

    assertDoesNotThrow(
        () ->
            store.jobIndexStore.mutateByCanonicalPointerReturningRecord(
                Keys.reconcileJobPointerById(ACCOUNT_ID, tableJobId),
                current -> {
                  current.state = "JS_SUCCEEDED";
                  current.message = "Succeeded";
                  current.tablesScanned = 1L;
                  current.tablesChanged = 1L;
                  current.startedAtMs = 100L;
                  current.finishedAtMs = 200L;
                  current.projectionRequestedGeneration = 5L;
                  current.projectionAppliedGeneration = 5L;
                  current.updatedAtMs = 200L;
                  return current;
                }));
    assertDoesNotThrow(
        () ->
            store.jobIndexStore.mutateByCanonicalPointerReturningRecord(
                Keys.reconcileJobPointerById(ACCOUNT_ID, snapshotJobId),
                current -> {
                  current.state = "JS_SUCCEEDED";
                  current.message = "Succeeded";
                  current.startedAtMs = 120L;
                  current.finishedAtMs = 180L;
                  current.projectionRequestedGeneration = 2L;
                  current.projectionAppliedGeneration = 2L;
                  current.updatedAtMs = 180L;
                  return current;
                }));

    projectionStore()
        .upsert(
            new StoredReconcileJobProjection(
                ACCOUNT_ID,
                tableJobId,
                5L,
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
                7L,
                7L,
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
                snapshotJobId,
                2L,
                "JS_SUCCEEDED",
                "Succeeded",
                120L,
                180L,
                0L,
                0L,
                0L,
                0L,
                0L,
                1L,
                99L,
                99L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                "",
                true));

    requestProjectionRefresh(connectorJobId);
    runProjectionMaintenance(2);
    StoredReconcileJobProjection projection =
        projectionStore().load(ACCOUNT_ID, connectorJobId).orElseThrow();

    assertEquals(0L, projection.statsProcessed());
    assertEquals(0L, projection.indexesProcessed());
  }

  @Test
  void parentRefreshRecomputesFromCanonicalChildren() {
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
            "",
            "");
    String snapshotJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "orders-id"),
            ReconcileJobKind.PLAN_SNAPSHOT,
            ReconcileTableTask.empty(),
            ReconcileViewTask.empty(),
            ReconcileSnapshotTask.of("orders-id", 55L, "src.ns", "orders"),
            ReconcileFileGroupTask.empty(),
            ReconcileExecutionPolicy.defaults(),
            tableJobId,
            "");

    assertDoesNotThrow(
        () ->
            store.jobIndexStore.mutateByCanonicalPointerReturningRecord(
                Keys.reconcileJobPointerById(ACCOUNT_ID, tableJobId),
                current -> {
                  current.state = "JS_WAITING";
                  current.message = "Waiting on child work";
                  current.startedAtMs = 100L;
                  current.tablesScanned = 86L;
                  current.tablesChanged = 86L;
                  current.snapshotsProcessed = 158L;
                  current.statsProcessed = 1208L;
                  current.indexesProcessed = 1208L;
                  current.plannedFileGroups = 85L;
                  current.plannedFiles = 1208L;
                  current.completedFileGroups = 85L;
                  current.completedFiles = 1208L;
                  current.projectionRequestedGeneration = 796L;
                  current.projectionAppliedGeneration = 794L;
                  current.updatedAtMs = 200L;
                  return current;
                }));

    projectionStore()
        .upsert(
            new StoredReconcileJobProjection(
                ACCOUNT_ID,
                tableJobId,
                794L,
                "JS_WAITING",
                "Waiting on child work",
                100L,
                0L,
                86L,
                86L,
                0L,
                0L,
                0L,
                158L,
                1208L,
                1208L,
                85L,
                1208L,
                85L,
                0L,
                1208L,
                0L,
                "",
                true));
    projectionStore()
        .upsert(
            new StoredReconcileJobProjection(
                ACCOUNT_ID,
                snapshotJobId,
                1L,
                "JS_SUCCEEDED",
                "Succeeded",
                120L,
                180L,
                0L,
                0L,
                0L,
                0L,
                0L,
                1L,
                9999L,
                9999L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                "",
                true));

    requestProjectionRefresh(tableJobId);
    runProjectionMaintenance(2);

    StoredReconcileJobProjection projection =
        projectionStore().load(ACCOUNT_ID, tableJobId).orElseThrow();
    assertTrue(projection.appliedGeneration() > 796L);
    assertEquals(86L, projection.tablesScanned());
    assertEquals(86L, projection.tablesChanged());
    assertEquals(1208L, projection.statsProcessed());
    assertEquals(1208L, projection.indexesProcessed());
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
  void projectionMaintenanceDeletesMarkerForMissingParent() {
    String missingParentJobId = "missing-parent";
    String dirtyMarkerKey = Keys.reconcileDirtyParentPointer(ACCOUNT_ID, missingParentJobId);

    requestProjectionRefresh(missingParentJobId);
    assertTrue(store.pointerStore.get(dirtyMarkerKey).isPresent());
    runProjectionMaintenance();
    assertTrue(store.pointerStore.get(dirtyMarkerKey).isEmpty());
  }

  @Test
  void projectionRefreshTouchCannotBeConsumedWithAnOlderMarkerToken() {
    String parentJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    String markerKey = Keys.reconcileDirtyParentPointer(ACCOUNT_ID, parentJobId);

    requestProjectionRefresh(parentJobId);
    Pointer first = store.pointerStore.get(markerKey).orElseThrow();
    requestProjectionRefresh(parentJobId);
    Pointer replacement = store.pointerStore.get(markerKey).orElseThrow();

    assertNotEquals(first.getVersion(), replacement.getVersion());
    assertFalse(store.pointerStore.compareAndDelete(markerKey, first.getVersion()));
    assertEquals(replacement, store.pointerStore.get(markerKey).orElseThrow());
  }

  @Test
  void requestProjectionRefreshDoesNotMutateCanonicalParentWhenMarkerWriteFails() {
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
                    new Class<?>[] {String.class, String.class},
                    ACCOUNT_ID,
                    parentJobId));

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
  void leaseNextFilteredRequestsDoNotScanGlobalReadyRows() throws Exception {
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
      assertTrue(deleteReadyEntry(readyKey));
    }

    var filteredLease =
        store.leaseNext(
            ReconcileJobStore.LeaseRequest.of(
                java.util.EnumSet.of(ReconcileExecutionClass.HEAVY), Set.of("remote")));

    assertTrue(filteredLease.isEmpty());

    var lease = store.leaseNext(ReconcileJobStore.LeaseRequest.all()).orElseThrow();
    assertEquals(jobId, lease.jobId);
    assertEquals(ReconcileExecutionClass.HEAVY, lease.executionPolicy.executionClass());
    assertEquals("remote", lease.executionPolicy.lane());
  }

  @Test
  void execFileGroupReadyIndexesUseCanonicalLaneKey() throws Exception {
    String jobId =
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
                "snapshot-20-group-0",
                "table-1",
                20L,
                List.of("s3://bucket/data.parquet")),
            ReconcileExecutionPolicy.defaults(),
            "",
            "");

    StoredReconcileJob queued = readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, jobId));
    assertEquals("", queued.executionPolicy().lane());
    assertFalse(queued.laneKey.isBlank());

    @SuppressWarnings("unchecked")
    List<String> readyKeys =
        (List<String>)
            invokePrivateMethod(
                store, "readyPointerKeys", new Class<?>[] {StoredReconcileJob.class}, queued);

    assertTrue(
        readyKeys.stream()
            .anyMatch(
                key ->
                    key.startsWith(
                        Keys.reconcileReadyByExecutionLanePointerPrefix(queued.laneKey))));
    assertTrue(
        readyKeys.stream()
            .anyMatch(
                key ->
                    key.startsWith(
                        Keys.reconcileReadyByJobKindPointerPrefix(
                            ReconcileJobKind.EXEC_FILE_GROUP.name()))));

    var lease =
        store
            .leaseNext(
                ReconcileJobStore.LeaseRequest.of(
                    java.util.EnumSet.of(ReconcileExecutionClass.DEFAULT),
                    Set.of(queued.laneKey),
                    Set.of(),
                    java.util.EnumSet.of(ReconcileJobKind.EXEC_FILE_GROUP)))
            .orElseThrow();

    assertEquals(jobId, lease.jobId);
    assertEquals(ReconcileJobKind.EXEC_FILE_GROUP, lease.jobKind);
  }

  @Test
  void pinnedJobsOnlyCreatePinnedReadyIndexRows() throws Exception {
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

    StoredReconcileJob queued = readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, jobId));
    @SuppressWarnings("unchecked")
    List<String> readyKeys =
        (List<String>)
            invokePrivateMethod(
                store, "readyPointerKeys", new Class<?>[] {StoredReconcileJob.class}, queued);

    assertEquals(1, readyKeys.size());
    assertTrue(
        readyKeys.get(0).contains("/reconcile/jobs/ready/by-pinned-executor/remote-executor/"));

    var otherExecutorLease =
        store.leaseNext(
            ReconcileJobStore.LeaseRequest.of(
                java.util.EnumSet.of(ReconcileExecutionClass.HEAVY),
                Set.of("remote"),
                Set.of("other-executor")));
    assertTrue(otherExecutorLease.isEmpty());

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
  void retryThenSuccessClearsPriorFailureFromParentProjection() {
    configureRetryPolicy(2, 0L, 0L);

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

    var firstTableLease = leaseJob(tableJobId);
    store.markRunning(tableJobId, firstTableLease.leaseEpoch, 100L, "executor-table");
    store.markFailed(
        tableJobId, firstTableLease.leaseEpoch, 110L, "transient", 1L, 0L, 0L, 0L, 1L, 0L, 0L);
    runProjectionMaintenance(4);

    assertEquals(1L, projectionStore().load(ACCOUNT_ID, connectorJobId).orElseThrow().errors());

    var secondTableLease = leaseJob(tableJobId);
    store.markRunning(tableJobId, secondTableLease.leaseEpoch, 120L, "executor-table");
    store.markSucceeded(tableJobId, secondTableLease.leaseEpoch, 130L, 1L, 1L, 0L, 0L, 0L, 0L);
    runProjectionMaintenance(4);

    assertEquals(0L, projectionStore().load(ACCOUNT_ID, connectorJobId).orElseThrow().errors());
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
  void cancelParentBlocksQueuedDescendantsBeforeMaintenanceCleanup() {
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
    String grandchildJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileSnapshotTask.of("table-1", 1L, "db", "orders", List.of(), true),
            ReconcileExecutionPolicy.defaults(),
            childJobId,
            "");

    store.cancel(ACCOUNT_ID, parentJobId, "stop");

    assertTrue(store.isCancellationRequested(childJobId));
    assertTrue(store.isCancellationRequested(grandchildJobId));
    assertTrue(store.leaseNext(ReconcileJobStore.LeaseRequest.all()).isEmpty());
    assertEquals("JS_CANCELLED", store.getLeaseView(parentJobId).orElseThrow().state);
    assertEquals("JS_QUEUED", store.getLeaseView(childJobId).orElseThrow().state);
    assertEquals("JS_QUEUED", store.getLeaseView(grandchildJobId).orElseThrow().state);
  }

  @Test
  void maintenanceLeavesCanonicallyCancelledDescendantsTerminal() {
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
    String grandchildJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileSnapshotTask.of("table-1", 1L, "db", "orders", List.of(), true),
            ReconcileExecutionPolicy.defaults(),
            childJobId,
            "");

    store.cancel(ACCOUNT_ID, parentJobId, "stop");

    runCancellationMaintenance();
    runCancellationMaintenance();
    runCancellationMaintenance();
    assertEquals("JS_CANCELLED", store.getLeaseView(childJobId).orElseThrow().state);
    assertEquals("JS_CANCELLED", store.getLeaseView(grandchildJobId).orElseThrow().state);
    assertTrue(store.isCancellationRequested(childJobId));
    assertTrue(store.isCancellationRequested(grandchildJobId));
    assertTrue(store.leaseNext(ReconcileJobStore.LeaseRequest.all()).isEmpty());
  }

  @Test
  void fullRescanSnapshotCancellationCleansDraftStatsAfterDescendantsTerminal() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    store.statsStore = statsStore;
    ReconcileFileGroupTask group =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", "table-1", 55L, List.of("s3://bucket/data/file-1.parquet"));
    String snapshotJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            true,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "orders", List.of(group), true),
            ReconcileExecutionPolicy.defaults(),
            "",
            "");
    store.enqueue(
        ACCOUNT_ID,
        CONNECTOR_ID,
        true,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.of(List.of(), "table-1"),
        ReconcileJobKind.EXEC_FILE_GROUP,
        ReconcileTableTask.empty(),
        ReconcileViewTask.empty(),
        ReconcileSnapshotTask.empty(),
        group,
        ReconcileExecutionPolicy.defaults(),
        snapshotJobId,
        "");
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(ACCOUNT_ID)
            .setKind(ResourceKind.RK_TABLE)
            .setId("table-1")
            .build();
    Mockito.doReturn(UnpublishedGenerationDeleteResult.DELETED)
        .when(statsStore)
        .deleteUnpublishedStatsGeneration(tableId, 55L, "full-rescan-" + snapshotJobId);

    store.cancel(ACCOUNT_ID, snapshotJobId, "stop");

    Mockito.verify(statsStore, Mockito.never())
        .deleteUnpublishedStatsGeneration(tableId, 55L, "full-rescan-" + snapshotJobId);

    runCancellationMaintenance();
    runCancellationMaintenance();
    runCancellationMaintenance();

    assertEquals("JS_CANCELLED", store.getLeaseView(snapshotJobId).orElseThrow().state);
    Mockito.verify(statsStore, Mockito.times(1))
        .deleteUnpublishedStatsGeneration(tableId, 55L, "full-rescan-" + snapshotJobId);
  }

  @Test
  void fullRescanSnapshotCancellationRetriesDraftStatsCleanupAfterTransientFailure() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    store.statsStore = statsStore;
    ReconcileFileGroupTask group =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", "table-1", 55L, List.of("s3://bucket/data/file-1.parquet"));
    String snapshotJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            true,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "orders", List.of(group), true),
            ReconcileExecutionPolicy.defaults(),
            "",
            "");
    store.enqueue(
        ACCOUNT_ID,
        CONNECTOR_ID,
        true,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.of(List.of(), "table-1"),
        ReconcileJobKind.EXEC_FILE_GROUP,
        ReconcileTableTask.empty(),
        ReconcileViewTask.empty(),
        ReconcileSnapshotTask.empty(),
        group,
        ReconcileExecutionPolicy.defaults(),
        snapshotJobId,
        "");
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(ACCOUNT_ID)
            .setKind(ResourceKind.RK_TABLE)
            .setId("table-1")
            .build();
    Mockito.doThrow(new RuntimeException("temporary stats store outage"))
        .doReturn(UnpublishedGenerationDeleteResult.DELETED)
        .when(statsStore)
        .deleteUnpublishedStatsGeneration(tableId, 55L, "full-rescan-" + snapshotJobId);

    store.cancel(ACCOUNT_ID, snapshotJobId, "stop");
    runCancellationMaintenance();
    runCancellationMaintenance();
    assertEquals("JS_CANCELLED", store.getLeaseView(snapshotJobId).orElseThrow().state);
    Mockito.verify(statsStore, Mockito.times(2))
        .deleteUnpublishedStatsGeneration(tableId, 55L, "full-rescan-" + snapshotJobId);

    runCancellationMaintenance();

    Mockito.verifyNoMoreInteractions(statsStore);
  }

  @Test
  void fullRescanSnapshotCancellationKeepsStatsCleanupPendingWhenGenerationPublishing() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    store.statsStore = statsStore;
    ReconcileFileGroupTask group =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", "table-1", 55L, List.of("s3://bucket/data/file-1.parquet"));
    String snapshotJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            true,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "orders", List.of(group), true),
            ReconcileExecutionPolicy.defaults(),
            "",
            "");
    store.enqueue(
        ACCOUNT_ID,
        CONNECTOR_ID,
        true,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.of(List.of(), "table-1"),
        ReconcileJobKind.EXEC_FILE_GROUP,
        ReconcileTableTask.empty(),
        ReconcileViewTask.empty(),
        ReconcileSnapshotTask.empty(),
        group,
        ReconcileExecutionPolicy.defaults(),
        snapshotJobId,
        "");
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(ACCOUNT_ID)
            .setKind(ResourceKind.RK_TABLE)
            .setId("table-1")
            .build();
    Mockito.doReturn(UnpublishedGenerationDeleteResult.RETRYABLE_IN_PROGRESS)
        .doReturn(UnpublishedGenerationDeleteResult.DELETED)
        .when(statsStore)
        .deleteUnpublishedStatsGeneration(tableId, 55L, "full-rescan-" + snapshotJobId);

    store.cancel(ACCOUNT_ID, snapshotJobId, "stop");
    runCancellationMaintenance();
    assertEquals("JS_CANCELLED", store.getLeaseView(snapshotJobId).orElseThrow().state);
    Mockito.verify(statsStore, Mockito.times(1))
        .deleteUnpublishedStatsGeneration(tableId, 55L, "full-rescan-" + snapshotJobId);

    runProjectionMaintenance();
    Mockito.verify(statsStore, Mockito.times(1))
        .deleteUnpublishedStatsGeneration(tableId, 55L, "full-rescan-" + snapshotJobId);

    runStatsCleanupMaintenance();
    Mockito.verify(statsStore, Mockito.times(2))
        .deleteUnpublishedStatsGeneration(tableId, 55L, "full-rescan-" + snapshotJobId);

    runStatsCleanupMaintenance();
    Mockito.verifyNoMoreInteractions(statsStore);
  }

  @Test
  void abandonedStatsCleanupAdvancesPastPartialPageOnNextRun() throws Exception {
    ReconcileJobIndexStore indexStore = Mockito.mock(ReconcileJobIndexStore.class);
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    store.jobIndexStore = indexStore;
    store.statsStore = statsStore;
    StoredReconcileJob skipped = new StoredReconcileJob();

    Mockito.when(indexStore.listStoredJobsPendingStatsCleanup(128, ""))
        .thenReturn(new ReconcileJobIndexStore.StoredJobPage(List.of(), "pending-page-2"));
    Mockito.when(indexStore.listStoredJobsPendingStatsCleanup(128, "pending-page-2"))
        .thenAnswer(
            ignored -> {
              Thread.sleep(30L);
              return new ReconcileJobIndexStore.StoredJobPage(List.of(skipped), "pending-page-3");
            });
    Mockito.when(indexStore.listStoredJobsPendingStatsCleanup(128, "pending-page-3"))
        .thenReturn(new ReconcileJobIndexStore.StoredJobPage(List.of(), ""));

    store.runAbandonedFullRescanStatsCleanupMaintenanceOnce(20L);
    store.runAbandonedFullRescanStatsCleanupMaintenanceOnce(1_000L);

    Mockito.verify(indexStore, Mockito.times(1)).listStoredJobsPendingStatsCleanup(128, "");
    Mockito.verify(indexStore, Mockito.times(1))
        .listStoredJobsPendingStatsCleanup(128, "pending-page-2");
    Mockito.verify(indexStore, Mockito.times(1))
        .listStoredJobsPendingStatsCleanup(128, "pending-page-3");
    Mockito.verifyNoInteractions(statsStore);
  }

  @Test
  void fullRescanSnapshotCancellationDoesNotCleanDraftStatsWithLiveDescendant() {
    StatsStore statsStore = Mockito.mock(StatsStore.class);
    store.statsStore = statsStore;
    ReconcileFileGroupTask group =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", "table-1", 55L, List.of("s3://bucket/data/file-1.parquet"));
    String snapshotJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            true,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileSnapshotTask.of("table-1", 55L, "db", "orders", List.of(group), true),
            ReconcileExecutionPolicy.defaults(),
            "",
            "");
    String execJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            true,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileJobKind.EXEC_FILE_GROUP,
            ReconcileTableTask.empty(),
            ReconcileViewTask.empty(),
            ReconcileSnapshotTask.empty(),
            group,
            ReconcileExecutionPolicy.defaults(),
            snapshotJobId,
            "");
    var execLease = leaseJob(execJobId);
    store.markRunning(execJobId, execLease.leaseEpoch, 100L, "executor-file-group");
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(ACCOUNT_ID)
            .setKind(ResourceKind.RK_TABLE)
            .setId("table-1")
            .build();

    store.cancel(ACCOUNT_ID, snapshotJobId, "stop");
    runCancellationMaintenance();
    runCancellationMaintenance();

    assertEquals("JS_CANCELLING", store.getLeaseView(snapshotJobId).orElseThrow().state);
    assertEquals("JS_RUNNING", store.getLeaseView(execJobId).orElseThrow().state);
    Mockito.verify(statsStore, Mockito.never())
        .deleteUnpublishedStatsGeneration(tableId, 55L, "full-rescan-" + snapshotJobId);
  }

  @Test
  void cancellingParentProjectionReflectsCanonicalRunningChild() {
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
    var childLease = leaseJob(childJobId);
    store.markRunning(childJobId, childLease.leaseEpoch, 100L, "executor-table");
    store.markProgress(childJobId, childLease.leaseEpoch, 0L, 0L, 0L, 0L, 0L, 7L, 0L, "progress");

    store.cancel(ACCOUNT_ID, parentJobId, "stop");
    runCancellationMaintenance();
    runProjectionMaintenance();

    assertTrue(
        Set.of("JS_CANCELLING", "JS_CANCELLED")
            .contains(store.getLeaseView(parentJobId).orElseThrow().state));
    assertEquals("JS_RUNNING", store.getLeaseView(childJobId).orElseThrow().state);
    Optional<StoredReconcileJobProjection> parentProjection =
        projectionStore().load(ACCOUNT_ID, parentJobId);
    parentProjection.ifPresent(
        projection -> {
          assertEquals("JS_CANCELLING", projection.state());
          assertEquals(7L, projection.snapshotsProcessed());
        });
  }

  @Test
  void projectionMaintenanceDoesNotScanAncestorsBeforeRefreshingDescendant() {
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
    String grandchildJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileSnapshotTask.of("table-1", 1L, "db", "orders", List.of(), true),
            ReconcileExecutionPolicy.defaults(),
            childJobId,
            "");

    store.cancel(ACCOUNT_ID, parentJobId, "stop");

    requestProjectionRefresh(grandchildJobId);
    runProjectionMaintenance();
    assertTrue(projectionStore().load(ACCOUNT_ID, grandchildJobId).isPresent());
    assertTrue(
        Set.of("JS_CANCELLING", "JS_CANCELLED")
            .contains(store.getLeaseView(parentJobId).orElseThrow().state));
  }

  @Test
  void maintenanceLeavesRunningDescendantActiveUnderCancellingParent() {
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
    var childLease = leaseJob(childJobId);
    store.markRunning(childJobId, childLease.leaseEpoch, 100L, "executor-table");

    store.cancel(ACCOUNT_ID, parentJobId, "stop");
    runCancellationMaintenance();
    runCancellationMaintenance();
    runCancellationMaintenance();

    ReconcileJob child = store.getLeaseView(childJobId).orElseThrow();
    assertEquals("JS_CANCELLING", store.getLeaseView(parentJobId).orElseThrow().state);
    assertEquals("JS_RUNNING", child.state);
    assertTrue(store.isCancellationRequested(childJobId));
  }

  @Test
  void maintenanceCancelsRunningDescendantWithoutLiveLeaseUnderCancellingParent() {
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
    String grandchildJobId =
        store.enqueueSnapshotPlan(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileSnapshotTask.of("table-1", 1L, "db", "orders", List.of(), true),
            ReconcileExecutionPolicy.defaults(),
            childJobId,
            "");
    var childLease = leaseJob(childJobId);
    store.markRunning(childJobId, childLease.leaseEpoch, 100L, "executor-table");
    assertTrue(
        store.leaseStore.clearLeaseIfEpochMatches(ACCOUNT_ID, childJobId, childLease.leaseEpoch));

    store.cancel(ACCOUNT_ID, parentJobId, "stop");
    runCancellationMaintenance();
    runCancellationMaintenance();
    runCancellationMaintenance();
    runCancellationMaintenance();

    assertEquals("JS_CANCELLED", store.getLeaseView(parentJobId).orElseThrow().state);
    assertEquals("JS_CANCELLED", store.getLeaseView(childJobId).orElseThrow().state);
    assertEquals("JS_CANCELLED", store.getLeaseView(grandchildJobId).orElseThrow().state);
    assertTrue(store.leaseNext(ReconcileJobStore.LeaseRequest.all()).isEmpty());
  }

  @Test
  void lateChildCompletionDoesNotOverwriteCancelledParent() {
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
    var childLease = leaseJob(childJobId);
    store.markRunning(childJobId, childLease.leaseEpoch, 100L, "executor-table");

    store.cancel(ACCOUNT_ID, parentJobId, "stop");
    store.markSucceeded(childJobId, childLease.leaseEpoch, 200L, 1L, 1L, 0L, 0L, 0L, 0L);
    runCancellationMaintenance();

    ReconcileJob parent = store.getLeaseView(parentJobId).orElseThrow();
    assertEquals("JS_CANCELLED", parent.state);
    assertEquals("stop", parent.message);
    assertTrue(store.isCancellationRequested(childJobId));
  }

  @Test
  void retryableChildFailureBecomesCancelledWhenAncestorIsCancelling() {
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

    var parentLease = leaseJob(parentJobId);
    store.markRunning(parentJobId, parentLease.leaseEpoch, 100L, "executor-parent");
    var childLease = leaseJob(childJobId);
    store.markRunning(childJobId, childLease.leaseEpoch, 120L, "executor-child");

    store.cancel(ACCOUNT_ID, parentJobId, "stop");
    store.markFailed(childJobId, childLease.leaseEpoch, 200L, "expired token", 0L, 0L, 0L, 0L, 1L);

    ReconcileJob child = store.getLeaseView(childJobId).orElseThrow();
    assertEquals("JS_CANCELLED", child.state);
    assertEquals(200L, child.finishedAtMs);
    assertTrue(store.leaseNext(ReconcileJobStore.LeaseRequest.all()).isEmpty());
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
    persistFileGroupResult(
        store,
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
    assertEquals(
        0L,
        projectionStore()
            .load(ACCOUNT_ID, tableJobId)
            .map(StoredReconcileJobProjection::indexesProcessed)
            .orElse(0L));
    runProjectionMaintenance(8);

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
    assertEquals(2L, tableProjection.statsProcessed());
    assertEquals(1L, connectorProjection.tablesScanned());
    assertEquals(1L, connectorProjection.tablesChanged());
  }

  @Test
  void finalizerSuccessAdvancesNestedCanonicalParentsThroughMaintenance() {
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
    ReconcileFileGroupTask fileGroup =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", "table-1", 55L, List.of("s3://bucket/data/file-1.parquet"));
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
                List.of(fileGroup),
                true,
                ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
                "",
                1),
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
            fileGroup,
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

    var snapshotLease = leaseJob(snapshotJobId);
    store.markRunning(snapshotJobId, snapshotLease.leaseEpoch, 120L, "executor-snapshot");
    store.markWaiting(
        snapshotJobId,
        snapshotLease.leaseEpoch,
        125L,
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
    persistFileGroupResult(
        store,
        execJobId,
        execLease.leaseEpoch,
        ReconcileFileGroupTask.of(
            "plan-1",
            "group-1",
            "table-1",
            55L,
            List.of("s3://bucket/data/file-1.parquet"),
            List.of(ReconcileFileResult.succeeded("s3://bucket/data/file-1.parquet", 2L))));
    runProjectionMaintenance(4);
    ReconcileJob finalizer =
        store.childJobsPage(ACCOUNT_ID, snapshotJobId, 200, "").jobs.stream()
            .filter(job -> job.jobKind == ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE)
            .findFirst()
            .orElseThrow();
    assertEquals("JS_QUEUED", finalizer.state);
    assertEquals(
        "JS_WAITING",
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, snapshotJobId)).state);

    var finalizerLease = leaseJob(finalizer.jobId);
    store.markRunning(finalizer.jobId, finalizerLease.leaseEpoch, 250L, "executor-finalizer");
    store.markSucceeded(finalizer.jobId, finalizerLease.leaseEpoch, 300L, 0L, 0L, 0L, 0L, 1L, 2L);
    runProjectionMaintenance(8);

    assertEquals(
        "JS_SUCCEEDED",
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, snapshotJobId)).state);
    assertEquals(
        "JS_SUCCEEDED",
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, tableJobId)).state);
    assertEquals(
        "JS_SUCCEEDED",
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId)).state);
  }

  @Test
  void atomicFileGroupCompletionRollsContributionIntoProjection() {
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

    var snapshotLease = leaseJob(snapshotJobId);
    store.markRunning(snapshotJobId, snapshotLease.leaseEpoch, 100L, "executor-snapshot");
    store.markWaiting(
        snapshotJobId,
        snapshotLease.leaseEpoch,
        125L,
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
    store.pointerStore.delete(Keys.reconcileDirtyParentPointer(ACCOUNT_ID, snapshotJobId));
    assertTrue(
        store
            .pointerStore
            .get(Keys.reconcileDirtyParentPointer(ACCOUNT_ID, snapshotJobId))
            .isEmpty());
    persistFileGroupResult(
        store,
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
                ReconcileFileResult.succeeded("s3://bucket/data/file-2.parquet", 3L))));
    assertTrue(
        store
            .pointerStore
            .get(Keys.reconcileDirtyParentPointer(ACCOUNT_ID, snapshotJobId))
            .isPresent());
    runProjectionMaintenance();

    StoredReconcileJobProjection snapshotProjection =
        projectionStore().load(ACCOUNT_ID, snapshotJobId).orElseThrow();
    assertEquals("JS_SUCCEEDED", snapshotProjection.state());
    assertEquals(1L, snapshotProjection.completedFileGroups());
    assertEquals(2L, snapshotProjection.completedFiles());
    assertEquals(1L, snapshotProjection.indexesProcessed());
    assertEquals(2L, snapshotProjection.statsProcessed());
  }

  @Test
  void deferredProjectionMaintenanceAdvancesParentCanonicalAggregates() {
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

    var snapshotLease = leaseJob(snapshotJobId);
    store.markRunning(snapshotJobId, snapshotLease.leaseEpoch, 120L, "executor-snapshot");
    store.markWaiting(
        snapshotJobId,
        snapshotLease.leaseEpoch,
        125L,
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
    persistFileGroupResult(
        store,
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
                ReconcileFileResult.succeeded("s3://bucket/data/file-2.parquet", 3L))));

    runProjectionMaintenance(8);

    StoredReconcileJob tableAfterSnapshotProjection =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, tableJobId));
    assertEquals(2L, tableAfterSnapshotProjection.statsProcessed);
    assertEquals(1L, tableAfterSnapshotProjection.indexesProcessed);
    assertEquals(1L, tableAfterSnapshotProjection.completedFileGroups);
    assertEquals(2L, tableAfterSnapshotProjection.completedFiles);

    StoredReconcileJobProjection tableProjection =
        projectionStore().load(ACCOUNT_ID, tableJobId).orElseThrow();
    StoredReconcileJob connectorAfterTablePatch =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId));

    assertEquals("JS_SUCCEEDED", tableProjection.state());
    assertEquals(2L, tableProjection.statsProcessed());
    assertEquals(1L, tableProjection.indexesProcessed());
    assertEquals(1L, tableProjection.completedFileGroups());
    assertEquals(2L, tableProjection.completedFiles());
    assertEquals(2L, connectorAfterTablePatch.statsProcessed);
    assertEquals(1L, connectorAfterTablePatch.indexesProcessed);
    assertEquals(1L, connectorAfterTablePatch.completedFileGroups);
    assertEquals(2L, connectorAfterTablePatch.completedFiles);
  }

  @Test
  void enqueueSnapshotPlanDoesNotCreateSelfProjectionMarkerBeforeChildWorkExists() {
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

    assertTrue(
        store
            .pointerStore
            .get(Keys.reconcileDirtyParentPointer(ACCOUNT_ID, snapshotJobId))
            .isEmpty());
  }

  @Test
  void connectorRollupUsesLatestLogicalTableChildInsteadOfStaleCancelledDuplicate() {
    ReconcileJobProjector projector = new ReconcileJobProjector();
    ReconcileAncestorRollupService rollup = new ReconcileAncestorRollupService();
    rollup.bind(null, projector, (record, tolerateLeasePointerDrift, nowMs) -> false);

    StoredReconcileJob connector = new StoredReconcileJob();
    connector.accountId = ACCOUNT_ID;
    connector.jobId = "connector-parent";
    connector.connectorId = CONNECTOR_ID;
    connector.jobKind = ReconcileJobKind.PLAN_CONNECTOR.name();
    connector.state = "JS_WAITING";
    connector.message = "Waiting on child work";
    connector.startedAtMs = 10L;
    connector.childrenFinalized = true;
    connector.expectedDirectChildren = 1L;
    connector.projectionRequestedGeneration = 5L;

    StoredReconcileJob latestSucceededTable = new StoredReconcileJob();
    latestSucceededTable.accountId = ACCOUNT_ID;
    latestSucceededTable.jobId = "table-latest";
    latestSucceededTable.connectorId = CONNECTOR_ID;
    latestSucceededTable.parentJobId = connector.jobId;
    latestSucceededTable.jobKind = ReconcileJobKind.PLAN_TABLE.name();
    latestSucceededTable.state = "JS_SUCCEEDED";
    latestSucceededTable.message = "Succeeded";
    latestSucceededTable.startedAtMs = 20L;
    latestSucceededTable.finishedAtMs = 30L;
    latestSucceededTable.createdAtMs = 200L;
    latestSucceededTable.updatedAtMs = 300L;
    latestSucceededTable.childrenFinalized = true;
    latestSucceededTable.expectedDirectChildren = 1L;
    latestSucceededTable.snapshotsProcessed = 1L;
    latestSucceededTable.statsProcessed = 5L;
    latestSucceededTable.indexesProcessed = 5L;
    latestSucceededTable.definition.taskDestinationTableId = "table-1";

    StoredReconcileJob staleCancelledTable = new StoredReconcileJob();
    staleCancelledTable.accountId = ACCOUNT_ID;
    staleCancelledTable.jobId = "table-stale";
    staleCancelledTable.connectorId = CONNECTOR_ID;
    staleCancelledTable.parentJobId = connector.jobId;
    staleCancelledTable.jobKind = ReconcileJobKind.PLAN_TABLE.name();
    staleCancelledTable.state = "JS_CANCELLED";
    staleCancelledTable.message = "Cancelled";
    staleCancelledTable.startedAtMs = 15L;
    staleCancelledTable.finishedAtMs = 40L;
    staleCancelledTable.createdAtMs = 100L;
    staleCancelledTable.updatedAtMs = 400L;
    staleCancelledTable.childrenFinalized = true;
    staleCancelledTable.expectedDirectChildren = 1L;
    staleCancelledTable.definition.taskDestinationTableId = "table-1";

    StoredReconcileJobProjection projection =
        rollup.recomputeParentProjection(
            connector, List.of(latestSucceededTable, staleCancelledTable), true);

    assertEquals("JS_SUCCEEDED", projection.state());
    assertEquals(1L, projection.tablesScanned());
    assertEquals(1L, projection.tablesChanged());
    assertEquals(1L, projection.snapshotsProcessed());
    assertEquals(5L, projection.statsProcessed());
    assertEquals(5L, projection.indexesProcessed());
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

    ReconcileJob connector = store.getLeaseView(connectorJobId).orElseThrow();
    assertEquals("JS_CANCELLING", connector.state);
    assertEquals("stop", connector.message);
    assertEquals("JS_QUEUED", store.getLeaseView(tableJobId).orElseThrow().state);
    assertTrue(store.isCancellationRequested(tableJobId));
    assertTrue(store.leaseNext(ReconcileJobStore.LeaseRequest.all()).isEmpty());
  }

  @Test
  void canonicalCancellationWithQueuedDescendantsFinalizesRootAndReleasesDedupe() {
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
                      current.state = "JS_RUNNING";
                      current.message = "Running";
                      current.startedAtMs = Math.max(current.startedAtMs, 50L);
                      current.finishedAtMs = 0L;
                      current.childrenFinalized = true;
                      current.expectedDirectChildren = Math.max(1L, current.expectedDirectChildren);
                      current.readyPointerKey = null;
                      current.nextAttemptAtMs = 0L;
                      return current;
                    }));

    store.cancel(ACCOUNT_ID, connectorJobId, "stop");
    store.runCancellationMaintenanceOnce(1_000L);

    ReconcileJob connector = store.getLeaseView(connectorJobId).orElseThrow();
    assertEquals("JS_CANCELLED", connector.state);
    assertEquals("stop", connector.message);
    assertEquals("JS_CANCELLED", store.getLeaseView(tableJobId).orElseThrow().state);

    String replacementJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    assertNotEquals(connectorJobId, replacementJobId);
  }

  @Test
  void cancelledFinalizedParentStillCancelsQueuedCanonicalChildren() {
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
                      current.state = "JS_CANCELLED";
                      current.message = "Cancelled";
                      current.startedAtMs = Math.max(current.startedAtMs, 50L);
                      current.finishedAtMs = Math.max(current.finishedAtMs, 100L);
                      current.childrenFinalized = true;
                      current.expectedDirectChildren = Math.max(1L, current.expectedDirectChildren);
                      current.readyPointerKey = null;
                      current.nextAttemptAtMs = 0L;
                      return current;
                    }));

    store.cancel(ACCOUNT_ID, connectorJobId, "Cancelled");

    String cleanupKey = Keys.reconcileCancellationCleanupPointer(ACCOUNT_ID, connectorJobId);
    assertTrue(store.pointerStore.get(cleanupKey).isPresent());

    runCancellationMaintenance();

    assertEquals("JS_CANCELLED", store.getLeaseView(connectorJobId).orElseThrow().state);
    assertEquals("JS_CANCELLED", store.getLeaseView(tableJobId).orElseThrow().state);
    assertTrue(store.pointerStore.get(cleanupKey).isEmpty());
    assertTrue(store.leaseNext(ReconcileJobStore.LeaseRequest.all()).isEmpty());
  }

  @Test
  void cancelledFinalizedHierarchyStillCancelsQueuedGrandchildren() {
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
            ReconcileSnapshotTask.of("table-1", 1L, "db", "orders", List.of(), true),
            ReconcileExecutionPolicy.defaults(),
            tableJobId,
            "");

    for (String jobId : List.of(connectorJobId, tableJobId)) {
      assertDoesNotThrow(
          () ->
              invokePrivateMethod(
                  store,
                  "mutateByCanonicalPointerReturningRecord",
                  new Class<?>[] {String.class, UnaryOperator.class},
                  Keys.reconcileJobPointerById(ACCOUNT_ID, jobId),
                  (UnaryOperator<StoredReconcileJob>)
                      current -> {
                        current.state = "JS_CANCELLED";
                        current.message = "Cancelled";
                        current.startedAtMs = Math.max(current.startedAtMs, 50L);
                        current.finishedAtMs = Math.max(current.finishedAtMs, 100L);
                        current.childrenFinalized = true;
                        current.expectedDirectChildren =
                            Math.max(1L, current.expectedDirectChildren);
                        current.readyPointerKey = null;
                        current.nextAttemptAtMs = 0L;
                        return current;
                      }));
    }

    store.cancel(ACCOUNT_ID, connectorJobId, "Cancelled");

    runCancellationMaintenance();
    runCancellationMaintenance();
    runCancellationMaintenance();

    assertEquals("JS_CANCELLED", store.getLeaseView(connectorJobId).orElseThrow().state);
    assertEquals("JS_CANCELLED", store.getLeaseView(tableJobId).orElseThrow().state);
    assertEquals("JS_CANCELLED", store.getLeaseView(snapshotJobId).orElseThrow().state);
    assertTrue(
        store
            .pointerStore
            .get(Keys.reconcileCancellationCleanupPointer(ACCOUNT_ID, connectorJobId))
            .isEmpty());
    assertTrue(
        store
            .pointerStore
            .get(Keys.reconcileCancellationCleanupPointer(ACCOUNT_ID, tableJobId))
            .isEmpty());
    assertTrue(store.leaseNext(ReconcileJobStore.LeaseRequest.all()).isEmpty());
  }

  @Test
  void enqueueDoesNotDedupeToStaleCancellingRoot() {
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
                      current.state = "JS_CANCELLING";
                      current.message = "Cancelling";
                      current.startedAtMs = Math.max(current.startedAtMs, 50L);
                      current.finishedAtMs = 0L;
                      current.childrenFinalized = true;
                      current.expectedDirectChildren = Math.max(1L, current.expectedDirectChildren);
                      current.readyPointerKey = null;
                      current.nextAttemptAtMs = 0L;
                      return current;
                    }));

    String replacementJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());

    assertNotEquals(connectorJobId, replacementJobId);
    assertEquals("JS_CANCELLING", store.getLeaseView(connectorJobId).orElseThrow().state);
    assertEquals("JS_QUEUED", store.getLeaseView(tableJobId).orElseThrow().state);
    assertEquals("JS_QUEUED", store.getLeaseView(replacementJobId).orElseThrow().state);
  }

  @Test
  void cancellationCleanupRequestResumesPausedParentMarker() {
    String connectorJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    String key = Keys.reconcileCancellationCleanupPointer(ACCOUNT_ID, connectorJobId);
    String payload =
        ReconcileCancellationMaintenanceService.cancellationCleanupPayload(
            new ReconcileCancellationMaintenanceService.CancellationCleanupRequest(
                ACCOUNT_ID, connectorJobId, "", true, false, true));
    assertTrue(
        store.pointerStore.compareAndSet(
            key, 0L, PointerReferences.opaqueMarkerPointer(key, payload, 1L)));

    requestCancellationCleanup(ACCOUNT_ID, connectorJobId);

    Pointer marker = store.pointerStore.get(key).orElseThrow();
    ReconcileCancellationMaintenanceService.CancellationCleanupRequest request =
        ReconcileCancellationMaintenanceService.cancellationCleanupRequest(marker);
    assertNotNull(request);
    assertFalse(request.paused());
    assertEquals("", request.childPageToken());
    assertFalse(request.delegatedNonTerminalSeen());
    assertFalse(request.blockingNonTerminalSeen());
  }

  @Test
  void cancellationMaintenanceDeletesPausedMarkerForCompletedCancelledRoot() {
    String connectorJobId =
        store.enqueue(
            ACCOUNT_ID,
            CONNECTOR_ID,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    assertDoesNotThrow(
        () ->
            invokePrivateMethod(
                store,
                "mutateByCanonicalPointerReturningRecord",
                new Class<?>[] {String.class, UnaryOperator.class},
                Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId),
                (UnaryOperator<StoredReconcileJob>)
                    current -> {
                      current.state = "JS_CANCELLED";
                      current.message = "Cancelled";
                      current.startedAtMs = Math.max(current.startedAtMs, 50L);
                      current.finishedAtMs = Math.max(current.finishedAtMs, 100L);
                      current.childrenFinalized = true;
                      current.readyPointerKey = null;
                      current.nextAttemptAtMs = 0L;
                      return current;
                    }));
    String key = Keys.reconcileCancellationCleanupPointer(ACCOUNT_ID, connectorJobId);
    String payload =
        ReconcileCancellationMaintenanceService.cancellationCleanupPayload(
            new ReconcileCancellationMaintenanceService.CancellationCleanupRequest(
                ACCOUNT_ID, connectorJobId, "", true, false, true));
    assertTrue(
        store.pointerStore.compareAndSet(
            key, 0L, PointerReferences.opaqueMarkerPointer(key, payload, 1L)));

    runCancellationMaintenance();

    assertTrue(store.pointerStore.get(key).isEmpty());
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
  void atomicSnapshotFinalizeSuccessRecordsFinalizedSnapshot() {
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

    var lease = leaseJob(jobId);
    store.markRunning(jobId, lease.leaseEpoch, 100L, "executor-finalizer");
    assertTrue(
        store.completeSnapshotFinalizeSuccess(
            jobId,
            lease.leaseEpoch,
            "result-1",
            "s3://bucket/capture-manifest.pb",
            123L,
            "manifest-sha256",
            1,
            3,
            4L,
            200L,
            "Finalized snapshot capture 55"));

    Optional<ReconcileJobStore.FinalizedSnapshotEvent> finalized =
        store.getFinalizedSnapshot(ACCOUNT_ID, "table-1", 55L);
    assertTrue(finalized.isPresent());
    assertEquals(jobId, finalized.orElseThrow().finalizerJobId);
    assertEquals(200L, finalized.orElseThrow().finalizedAtMs);
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

    var snapshotLease = leaseJob(snapshotJobId);
    store.markRunning(snapshotJobId, snapshotLease.leaseEpoch, 90L, "executor-snapshot");
    store.markWaiting(
        snapshotJobId,
        snapshotLease.leaseEpoch,
        95L,
        ReconcileJobStore.WaitingReason.CHILD_WORK_FINALIZED,
        "Waiting on child work",
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L);

    ReconcileJobIndexStore observedJobIndexStore = Mockito.spy(store.jobIndexStore);
    store.jobIndexStore = observedJobIndexStore;

    var firstLease = leaseJob(firstExecJobId);
    store.markRunning(firstExecJobId, firstLease.leaseEpoch, 100L, "executor-exec-1");
    persistFileGroupResult(
        store,
        firstExecJobId,
        firstLease.leaseEpoch,
        ReconcileFileGroupTask.of(
            "plan-1",
            "group-1",
            "table-1",
            55L,
            List.of("s3://bucket/data/file-1.parquet"),
            List.of(ReconcileFileResult.succeeded("s3://bucket/data/file-1.parquet", 1L))));
    assertEquals(
        0L,
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, snapshotJobId))
            .completedFileGroups);
    Mockito.verify(observedJobIndexStore, Mockito.never())
        .listStoredChildJobs(
            Mockito.eq(ACCOUNT_ID),
            Mockito.eq(snapshotJobId),
            Mockito.anyInt(),
            Mockito.anyString());
    assertEquals(
        0L,
        store.childJobsPage(ACCOUNT_ID, snapshotJobId, 200, "").jobs.stream()
            .filter(job -> job.jobKind == ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE)
            .count());
    Mockito.clearInvocations(observedJobIndexStore);

    var secondLease = leaseJob(secondExecJobId);
    store.markRunning(secondExecJobId, secondLease.leaseEpoch, 200L, "executor-exec-2");
    persistFileGroupResult(
        store,
        secondExecJobId,
        secondLease.leaseEpoch,
        ReconcileFileGroupTask.of(
            "plan-1",
            "group-2",
            "table-1",
            55L,
            List.of("s3://bucket/data/file-2.parquet"),
            List.of(ReconcileFileResult.succeeded("s3://bucket/data/file-2.parquet", 1L))));
    assertEquals(
        0L,
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, snapshotJobId))
            .completedFileGroups);
    Mockito.verify(observedJobIndexStore, Mockito.never())
        .listStoredChildJobs(
            Mockito.eq(ACCOUNT_ID),
            Mockito.eq(snapshotJobId),
            Mockito.anyInt(),
            Mockito.anyString());
    runProjectionMaintenance();
    Mockito.verify(observedJobIndexStore, Mockito.atLeastOnce())
        .listStoredChildJobs(
            Mockito.eq(ACCOUNT_ID),
            Mockito.eq(snapshotJobId),
            Mockito.anyInt(),
            Mockito.anyString());
    List<ReconcileJob> children = store.childJobsPage(ACCOUNT_ID, snapshotJobId, 200, "").jobs;
    List<ReconcileJob> finalizers =
        children.stream()
            .filter(job -> job.jobKind == ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE)
            .toList();
    assertEquals(1, finalizers.size());
    assertEquals("JS_QUEUED", finalizers.get(0).state);
    assertEquals(2, finalizers.get(0).snapshotTask.fileGroupCount());
    assertEquals(
        2,
        store
            .getCompactLeaseView(finalizers.get(0).jobId)
            .orElseThrow()
            .snapshotTask
            .fileGroupCount());
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

    var snapshotLease = leaseJob(snapshotJobId);
    store.markRunning(snapshotJobId, snapshotLease.leaseEpoch, 90L, "executor-snapshot");
    store.markWaiting(
        snapshotJobId,
        snapshotLease.leaseEpoch,
        95L,
        ReconcileJobStore.WaitingReason.CHILD_WORK_FINALIZED,
        "Waiting on child work",
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L);

    var firstLease = leaseJob(firstExecJobId);
    store.markRunning(firstExecJobId, firstLease.leaseEpoch, 100L, "executor-exec-1");
    persistFileGroupResult(
        store,
        firstExecJobId,
        firstLease.leaseEpoch,
        ReconcileFileGroupTask.of(
            "plan-1",
            "group-1",
            "table-1",
            55L,
            List.of("s3://bucket/data/file-1.parquet"),
            List.of(ReconcileFileResult.succeeded("s3://bucket/data/file-1.parquet", 1L))));
    var secondLease = leaseJob(secondExecJobId);
    store.markRunning(secondExecJobId, secondLease.leaseEpoch, 200L, "executor-exec-2");
    persistFileGroupResult(
        store,
        secondExecJobId,
        secondLease.leaseEpoch,
        ReconcileFileGroupTask.of(
            "plan-1",
            "group-2",
            "table-1",
            55L,
            List.of("s3://bucket/data/file-2.parquet"),
            List.of(ReconcileFileResult.succeeded("s3://bucket/data/file-2.parquet", 1L))));
    runProjectionMaintenance(2);
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
    persistFileGroupResult(
        store,
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
    isolatedStore.leaseBackend = new MemoryReconcileLeaseBackend();
    isolatedStore.readyQueueBackend = new MemoryReconcileReadyQueueBackend(pointerStore);
    isolatedStore.init();
    return isolatedStore;
  }

  private static boolean batchContainsCanonicalMutation(
      ReconcileJobIndexStore.JobIndexWriteBatch batch, String canonicalPointerKey) {
    if (batch == null || canonicalPointerKey == null || canonicalPointerKey.isBlank()) {
      return false;
    }
    return batch.writes().stream()
        .filter(ReconcileJobIndexStore.JobIndexUpsert.class::isInstance)
        .map(ReconcileJobIndexStore.JobIndexUpsert.class::cast)
        .anyMatch(upsert -> canonicalPointerKey.equals(upsert.pointerKey()));
  }

  private boolean deleteReadyEntry(String readyPointerKey) {
    assertDoesNotThrow(() -> invokePrivateMethod(store, "readyQueue", new Class<?>[] {}));
    return store.readyQueueBackend.deleteReadyEntry(readyPointerKey);
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

  private StoredReconcileJobProjection projection(
      String jobId,
      long generation,
      String state,
      String message,
      long tablesScanned,
      long tablesChanged,
      long snapshotsProcessed,
      long statsProcessed) {
    return new StoredReconcileJobProjection(
        ACCOUNT_ID,
        jobId,
        generation,
        state,
        message,
        100L,
        0L,
        tablesScanned,
        tablesChanged,
        0L,
        0L,
        0L,
        snapshotsProcessed,
        statsProcessed,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        "",
        true);
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

  private void runProjectionMaintenance(int passes) {
    for (int pass = 0; pass < passes; pass++) {
      runProjectionMaintenance();
    }
  }

  private void requestProjectionRefresh(String parentJobId) {
    assertDoesNotThrow(
        () ->
            invokePrivateMethod(
                store,
                "requestProjectionRefresh",
                new Class<?>[] {String.class, String.class},
                ACCOUNT_ID,
                parentJobId));
  }

  private void runCancellationMaintenance() {
    store.runCancellationMaintenanceOnce(isDynamoMode() ? 10_000L : 100L);
  }

  private void runStatsCleanupMaintenance() {
    store.runAbandonedFullRescanStatsCleanupMaintenanceOnce(isDynamoMode() ? 10_000L : 100L);
  }

  private void requestCancellationCleanup(String accountId, String rootJobId) {
    assertDoesNotThrow(
        () ->
            invokePrivateMethod(
                store,
                "requestCancellationCleanup",
                new Class<?>[] {String.class, String.class},
                accountId,
                rootJobId));
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
      if (ops != null
          && ops.stream()
              .filter(UnconditionalUpsert.class::isInstance)
              .map(UnconditionalUpsert.class::cast)
              .anyMatch(
                  upsert -> upsert.key().startsWith(Keys.reconcileDirtyParentPointerPrefix()))) {
        return false;
      }
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
    private volatile int throwOnCall = -1;
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

    void throwOnArmedCall(int callNumber) {
      armedCalls.set(0);
      throwOnCall = Math.max(1, callNumber);
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
    public ReconcileJobIndexCleanupManifest loadCleanupManifest(String canonicalPointerKey) {
      return delegate.loadCleanupManifest(canonicalPointerKey);
    }

    @Override
    public boolean compareAndSetBatch(
        ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexStore
                .JobIndexWriteBatch
            batch) {
      return compareAndSetBatch(batch, java.util.List.of());
    }

    @Override
    public boolean compareAndSetBatch(
        ReconcileJobIndexStore.JobIndexWriteBatch batch,
        java.util.List<PointerStore.CasOp> extraPointerOps) {
      int call = armedCalls.incrementAndGet();
      if (throwOnCall > 0 && call == throwOnCall) {
        throwOnCall = -1;
        throw new IllegalStateException("injected compare-and-set failure");
      }
      if (!failed && failOnCall > 0 && call == failOnCall) {
        failed = true;
        onFailedCompareAndSetBatch.run();
        return false;
      }
      return delegate.compareAndSetBatch(batch, extraPointerOps);
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
  }

  private static final class HookedCompareAndSetBatchBackend implements ReconcileJobIndexBackend {
    private final ReconcileJobIndexBackend delegate;
    private volatile Consumer<ReconcileJobIndexStore.JobIndexWriteBatch> beforeCompareAndSetBatch =
        batch -> {};

    private HookedCompareAndSetBatchBackend(ReconcileJobIndexBackend delegate) {
      this.delegate = delegate;
    }

    void beforeCompareAndSetBatch(Consumer<ReconcileJobIndexStore.JobIndexWriteBatch> callback) {
      beforeCompareAndSetBatch = callback == null ? batch -> {} : callback;
    }

    @Override
    public Optional<JobIndexEntrySnapshot> loadIndexEntry(String pointerKey) {
      return delegate.loadIndexEntry(pointerKey);
    }

    @Override
    public ReconcileJobIndexCleanupManifest loadCleanupManifest(String canonicalPointerKey) {
      return delegate.loadCleanupManifest(canonicalPointerKey);
    }

    @Override
    public boolean compareAndSetBatch(ReconcileJobIndexStore.JobIndexWriteBatch batch) {
      return compareAndSetBatch(batch, java.util.List.of());
    }

    @Override
    public boolean compareAndSetBatch(
        ReconcileJobIndexStore.JobIndexWriteBatch batch,
        java.util.List<PointerStore.CasOp> extraPointerOps) {
      beforeCompareAndSetBatch.accept(batch);
      return delegate.compareAndSetBatch(batch, extraPointerOps);
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

  private static void persistFileGroupResult(
      ReconcileJobStore store, String jobId, String leaseEpoch, ReconcileFileGroupTask task) {
    ReconcileJobStore.ReconcileJob job = store.getLeaseView(jobId).orElseThrow();
    int succeeded =
        (int)
            task.fileResults().stream()
                .filter(result -> result.state() == ReconcileFileResult.State.SUCCEEDED)
                .count();
    int failed =
        (int)
            task.fileResults().stream()
                .filter(result -> result.state() == ReconcileFileResult.State.FAILED)
                .count();
    int skipped =
        (int)
            task.fileResults().stream()
                .filter(result -> result.state() == ReconcileFileResult.State.SKIPPED)
                .count();
    int indexArtifacts =
        task.fileResults().stream()
            .mapToInt(result -> result.indexArtifact().isEmpty() ? 0 : 1)
            .sum();
    store.completeFileGroupSuccess(
        jobId,
        leaseEpoch,
        new ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupResultDescriptor(
            1,
            job.accountId,
            job.connectorId,
            job.parentJobId,
            jobId,
            task.planId(),
            task.groupId(),
            task.tableId(),
            task.snapshotId(),
            leaseEpoch,
            "test-result-" + jobId,
            "/test-results/" + jobId + ".pb",
            1L,
            "dGVzdA==",
            task.fileCount(),
            succeeded,
            failed,
            skipped,
            task.partialAggregateRecords().size(),
            indexArtifacts,
            "/test-results/" + jobId + ".stats.pb",
            1L,
            "dGVzdA==",
            task.fileCount(),
            System.currentTimeMillis()),
        System.currentTimeMillis(),
        "Succeeded");
  }

  private static ResourceId connectorResourceId() {
    return ResourceId.newBuilder()
        .setAccountId(ACCOUNT_ID)
        .setId(CONNECTOR_ID)
        .setKind(ResourceKind.RK_CONNECTOR)
        .build();
  }
}
