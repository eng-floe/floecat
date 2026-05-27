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
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobProjection;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileProjectionRefreshCursor;
import ai.floedb.floecat.service.reconciler.jobs.durable.projection.ReconcileJobProjectionStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.projection.ReconcileJobRootSummaryStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.projection.ReconcileProjectionRefreshCursorStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileLeaseStore;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.aws.dynamodb.DynamoPointerStore;
import ai.floedb.floecat.storage.kv.dynamodb.DynamoDbKvStore;
import ai.floedb.floecat.storage.kv.dynamodb.ps.PointerStoreEntity;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
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

class DurableReconcileJobStoreTest {
  private static final Pattern JOB_ID_PATTERN =
      Pattern.compile("([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})");
  private static final String ACCOUNT_ID_PREFIX = "acct";
  private static final String CONNECTOR_ID_PREFIX = "conn";

  private static DynamoDbClient sharedDynamoDbClient;
  private static DynamoDbAsyncClient sharedDynamoDbAsyncClient;

  private String ACCOUNT_ID;
  private String CONNECTOR_ID;
  private DurableReconcileJobStore store;
  private boolean maintenanceEnabled;

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
    ACCOUNT_ID = ACCOUNT_ID_PREFIX + "-" + UUID.randomUUID();
    CONNECTOR_ID = CONNECTOR_ID_PREFIX + "-" + UUID.randomUUID();
    maintenanceEnabled = true;
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
  void tearDown() {
    System.clearProperty("floecat.reconciler.job-store.max-attempts");
    System.clearProperty("floecat.reconciler.job-store.base-backoff-ms");
    System.clearProperty("floecat.reconciler.job-store.max-backoff-ms");
    System.clearProperty("floecat.reconciler.job-store.lease-ms");
    System.clearProperty("floecat.reconciler.job-store.lease-renew-grace-ms");
    System.clearProperty("floecat.reconciler.job-store.reclaim-interval-ms");
    System.clearProperty("floecat.reconciler.projection-refresh.child-page-size");
    System.clearProperty("floecat.reconciler.projection-refresh.max-pages-per-maintenance");
  }

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
  void partialProjectionRefreshDoesNotPublishProjectionOrRootSummary() {
    withMaintenanceDisabled(
        () -> {
          configureProjectionRefresh(1, 1);
          String connectorJobId =
              store.enqueue(
                  ACCOUNT_ID,
                  CONNECTOR_ID,
                  false,
                  CaptureMode.METADATA_AND_CAPTURE,
                  ReconcileScope.empty());
          String childOne = enqueueTableChild(connectorJobId, "orders-1");
          String childTwo = enqueueTableChild(connectorJobId, "orders-2");
          markChildSucceededWithoutMaintenance(childOne, 100L);
          markChildSucceededWithoutMaintenance(childTwo, 200L);

          assertFalse(refreshProjectedParent(ACCOUNT_ID, connectorJobId));

          assertEquals(0L, store.get(ACCOUNT_ID, connectorJobId).orElseThrow().tablesScanned);
          assertEquals(
              0L,
              store.listRootJobs(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
                  .filter(job -> connectorJobId.equals(job.jobId))
                  .findFirst()
                  .map(job -> job.tablesScanned)
                  .orElse(0L));

          finishProjectionRefresh(ACCOUNT_ID, childOne, 10);
          finishProjectionRefresh(ACCOUNT_ID, childTwo, 10);
          finishProjectionRefresh(ACCOUNT_ID, connectorJobId, 10);

          assertEquals(
              2L, waitForProjectionTablesScanned(ACCOUNT_ID, connectorJobId, 2L).tablesScanned());
          assertEquals(
              2L,
              store.listRootJobs(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
                  .filter(job -> connectorJobId.equals(job.jobId))
                  .findFirst()
                  .map(job -> job.tablesScanned)
                  .orElse(0L));
        });
  }

  @Test
  void finalProjectionRefreshPublishesProjectionAndAdvancesAppliedGeneration() {
    withMaintenanceDisabled(
        () -> {
          configureProjectionRefresh(1, 1);
          String connectorJobId =
              store.enqueue(
                  ACCOUNT_ID,
                  CONNECTOR_ID,
                  false,
                  CaptureMode.METADATA_AND_CAPTURE,
                  ReconcileScope.empty());
          String tableJobId = enqueueTableChild(connectorJobId, "orders");
          String snapshotOne = enqueueSnapshotChild(tableJobId, "orders", 1L);
          String snapshotTwo = enqueueSnapshotChild(tableJobId, "orders", 2L);
          markSnapshotChildSucceededWithoutMaintenance(snapshotOne, 100L);
          markSnapshotChildSucceededWithoutMaintenance(snapshotTwo, 200L);
          finishProjectionRefresh(ACCOUNT_ID, snapshotOne, 10);
          finishProjectionRefresh(ACCOUNT_ID, snapshotTwo, 10);

          finishProjectionRefresh(ACCOUNT_ID, tableJobId, 10);

          StoredReconcileJobProjection projection =
              waitForProjectionPresent(ACCOUNT_ID, tableJobId);
          StoredReconcileJob canonical =
              readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, tableJobId));

          assertTrue(projection.appliedGeneration() > 0L);
          assertEquals(
              canonical.projectionRequestedGeneration, canonical.projectionAppliedGeneration);
          assertTrue(projectionRefreshCursor(tableJobId) == null);
        });
  }

  @Test
  void parentProjectionRefreshDefersUntilDirtyChildProjectionPublishes() {
    withMaintenanceDisabled(
        () -> {
          configureProjectionRefresh(10, 10);
          String connectorJobId =
              store.enqueue(
                  ACCOUNT_ID,
                  CONNECTOR_ID,
                  false,
                  CaptureMode.METADATA_AND_CAPTURE,
                  ReconcileScope.empty());
          String tableJobId = enqueueTableChild(connectorJobId, "orders");
          String snapshotJobId = enqueueSnapshotChild(tableJobId, "orders", 1L);
          markSnapshotChildSucceededWithoutMaintenance(snapshotJobId, 100L);

          assertFalse(refreshProjectedParent(ACCOUNT_ID, connectorJobId));

          StoredReconcileJob connectorCanonical =
              readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, connectorJobId));
          assertTrue(
              connectorCanonical.projectionAppliedGeneration
                  < connectorCanonical.projectionRequestedGeneration);
          assertEquals(
              0L,
              store.listRootJobs(ACCOUNT_ID, 20, "", CONNECTOR_ID, Set.of()).jobs.stream()
                  .filter(job -> connectorJobId.equals(job.jobId))
                  .findFirst()
                  .map(job -> job.tablesScanned)
                  .orElse(0L));

          finishProjectionRefresh(ACCOUNT_ID, snapshotJobId, 10);
          finishProjectionRefresh(ACCOUNT_ID, tableJobId, 10);
          finishProjectionRefresh(ACCOUNT_ID, connectorJobId, 10);

          StoredReconcileJobProjection projection =
              waitForProjectionTablesScanned(ACCOUNT_ID, connectorJobId, 1L);
          assertEquals(1L, projection.tablesScanned());
        });
  }

  @Test
  void generationAdvanceDuringPartialRefreshRestartsCursorAtNewGeneration() {
    withMaintenanceDisabled(
        () -> {
          configureProjectionRefresh(1, 1);
          String connectorJobId =
              store.enqueue(
                  ACCOUNT_ID,
                  CONNECTOR_ID,
                  false,
                  CaptureMode.METADATA_AND_CAPTURE,
                  ReconcileScope.empty());
          String childOne = enqueueTableChild(connectorJobId, "orders-1");
          String childTwo = enqueueTableChild(connectorJobId, "orders-2");
          markChildSucceededWithoutMaintenance(childOne, 100L);
          markChildSucceededWithoutMaintenance(childTwo, 200L);
          finishProjectionRefresh(ACCOUNT_ID, childOne, 10);
          finishProjectionRefresh(ACCOUNT_ID, childTwo, 10);

          assertFalse(refreshProjectedParent(ACCOUNT_ID, connectorJobId));
          StoredReconcileProjectionRefreshCursor firstCursor =
              projectionRefreshCursor(connectorJobId);
          assertNotNull(firstCursor);

          assertDoesNotThrow(
              () ->
                  invokePrivateMethod(
                      store,
                      "mutateByCanonicalPointerReturningRecord",
                      new Class<?>[] {String.class, UnaryOperator.class},
                      Keys.reconcileJobPointerById(ACCOUNT_ID, childTwo),
                      (UnaryOperator<StoredReconcileJob>)
                          current -> {
                            current.message = "mutated";
                            current.updatedAtMs = Math.max(current.updatedAtMs, 250L);
                            return current;
                          }));
          markDirtyParent(ACCOUNT_ID, connectorJobId);

          assertFalse(refreshProjectedParent(ACCOUNT_ID, connectorJobId));

          StoredReconcileProjectionRefreshCursor restartedCursor =
              projectionRefreshCursor(connectorJobId);
          assertNotNull(restartedCursor);
          assertTrue(restartedCursor.targetGeneration() > firstCursor.targetGeneration());
          assertTrue(restartedCursor.childrenScanned() >= 0L);
          assertTrue(restartedCursor.childrenScanned() <= 1L);
          assertEquals(restartedCursor.childrenScanned(), restartedCursor.pageCount());
        });
  }

  @Test
  void staleSameGenerationCursorUpsertDoesNotRegressProgress() {
    withMaintenanceDisabled(
        () ->
            withStoreOverrides(
                Map.of(
                    "floecat.reconciler.projection-refresh.child-page-size", "1",
                    "floecat.reconciler.projection-refresh.max-pages-per-maintenance", "1"),
                ignored -> {
                  String connectorJobId =
                      store.enqueue(
                          ACCOUNT_ID,
                          CONNECTOR_ID,
                          false,
                          CaptureMode.METADATA_AND_CAPTURE,
                          ReconcileScope.empty());
                  String childOne = enqueueTableChild(connectorJobId, "orders-1");
                  String childTwo = enqueueTableChild(connectorJobId, "orders-2");
                  String childThree = enqueueTableChild(connectorJobId, "orders-3");
                  markChildSucceededWithoutMaintenance(childOne, 100L);
                  markChildSucceededWithoutMaintenance(childTwo, 200L);
                  markChildSucceededWithoutMaintenance(childThree, 300L);
                  finishProjectionRefresh(ACCOUNT_ID, childOne, 10);
                  finishProjectionRefresh(ACCOUNT_ID, childTwo, 10);
                  finishProjectionRefresh(ACCOUNT_ID, childThree, 10);

                  assertFalse(refreshProjectedParent(ACCOUNT_ID, connectorJobId));

                  StoredReconcileProjectionRefreshCursor currentCursor =
                      waitForValueWithoutMaintenance(
                              () -> Optional.ofNullable(projectionRefreshCursor(connectorJobId)),
                              Optional::isPresent,
                              "projection refresh cursor " + connectorJobId)
                          .orElseThrow();
                  assertTrue(currentCursor.targetGeneration() > 0L);

                  projectionRefreshCursorStore()
                      .upsert(
                          new StoredReconcileProjectionRefreshCursor(
                              currentCursor.accountId(),
                              currentCursor.parentJobId(),
                              currentCursor.targetGeneration(),
                              currentCursor.continuationPointerKey(),
                              currentCursor.accumulator(),
                              0L,
                              0L,
                              currentCursor.startedAtMs(),
                              currentCursor.updatedAtMs()));

                  finishProjectionRefresh(ACCOUNT_ID, connectorJobId, 10);

                  StoredReconcileJobProjection projection =
                      waitForProjectionTablesScanned(ACCOUNT_ID, connectorJobId, 3L);
                  assertEquals(3L, projection.tablesScanned());
                  assertTrue(projectionRefreshCursor(connectorJobId) == null);
                }));
  }

  @Test
  void projectionRefreshCursorSurvivesStoreRestart() {
    withMaintenanceDisabled(
        () -> {
          configureProjectionRefresh(1, 1);
          String connectorJobId =
              store.enqueue(
                  ACCOUNT_ID,
                  CONNECTOR_ID,
                  false,
                  CaptureMode.METADATA_AND_CAPTURE,
                  ReconcileScope.empty());
          String tableJobId = enqueueTableChild(connectorJobId, "orders");
          String snapshotOne = enqueueSnapshotChild(tableJobId, "orders", 1L);
          String snapshotTwo = enqueueSnapshotChild(tableJobId, "orders", 2L);
          markSnapshotChildSucceededWithoutMaintenance(snapshotOne, 100L);
          markSnapshotChildSucceededWithoutMaintenance(snapshotTwo, 200L);
          finishProjectionRefresh(ACCOUNT_ID, snapshotOne, 10);
          finishProjectionRefresh(ACCOUNT_ID, snapshotTwo, 10);
          assertFalse(refreshProjectedParent(ACCOUNT_ID, tableJobId));
          StoredReconcileProjectionRefreshCursor partialCursor =
              projectionRefreshCursor(tableJobId);
          assertNotNull(partialCursor);

          recreateStorePreservingState();
          finishProjectionRefresh(ACCOUNT_ID, tableJobId, 10);

          StoredReconcileJobProjection projection =
              waitForProjectionPresent(ACCOUNT_ID, tableJobId);
          assertTrue(projection.appliedGeneration() > 0L);
          assertTrue(projectionRefreshCursor(tableJobId) == null);
        });
  }

  @Test
  void ancestorPropagationWaitsForFinalProjectionPublication() {
    withMaintenanceDisabled(
        () -> {
          String accountId = ACCOUNT_ID;
          String connectorId = CONNECTOR_ID;
          configureProjectionRefresh(1, 1);
          String connectorJobId =
              store.enqueue(
                  accountId,
                  connectorId,
                  false,
                  CaptureMode.METADATA_AND_CAPTURE,
                  ReconcileScope.empty());
          String tableJobId = enqueueTableChild(accountId, connectorId, connectorJobId, "orders");
          String snapshotOne =
              store.enqueueSnapshotPlan(
                  accountId,
                  connectorId,
                  false,
                  CaptureMode.METADATA_AND_CAPTURE,
                  ReconcileScope.of(List.of(), "orders"),
                  ReconcileSnapshotTask.of("orders", 1L, "db", "orders", List.of(), true),
                  ReconcileExecutionPolicy.defaults(),
                  tableJobId,
                  "");
          String snapshotTwo =
              store.enqueueSnapshotPlan(
                  accountId,
                  connectorId,
                  false,
                  CaptureMode.METADATA_AND_CAPTURE,
                  ReconcileScope.of(List.of(), "orders"),
                  ReconcileSnapshotTask.of("orders", 2L, "db", "orders", List.of(), true),
                  ReconcileExecutionPolicy.defaults(),
                  tableJobId,
                  "");
          markSnapshotChildSucceededWithoutMaintenance(accountId, snapshotOne, 100L);
          markSnapshotChildSucceededWithoutMaintenance(accountId, snapshotTwo, 200L);
          finishProjectionRefresh(accountId, snapshotOne, 10);
          finishProjectionRefresh(accountId, snapshotTwo, 10);

          assertFalse(refreshProjectedParent(accountId, tableJobId));
          assertEquals(
              0L,
              store.listRootJobs(accountId, 20, "", connectorId, Set.of()).jobs.stream()
                  .filter(job -> connectorJobId.equals(job.jobId))
                  .findFirst()
                  .map(job -> job.tablesScanned)
                  .orElse(0L));

          finishProjectionRefresh(accountId, tableJobId, 10);
          assertEquals(
              0L,
              store.listRootJobs(accountId, 20, "", connectorId, Set.of()).jobs.stream()
                  .filter(job -> connectorJobId.equals(job.jobId))
                  .findFirst()
                  .map(job -> job.tablesScanned)
                  .orElse(0L));
          finishProjectionRefresh(accountId, connectorJobId, 10);
          assertEquals(
              2L, waitForProjectionTablesScanned(accountId, connectorJobId, 2L).tablesScanned());
        });
  }

  @Test
  void wideParentRefreshProcessesBoundedPagesPerMaintenancePass() {
    withMaintenanceDisabled(
        () -> {
          configureProjectionRefresh(2, 1);
          String connectorJobId =
              store.enqueue(
                  ACCOUNT_ID,
                  CONNECTOR_ID,
                  false,
                  CaptureMode.METADATA_AND_CAPTURE,
                  ReconcileScope.empty());
          String tableJobId = enqueueTableChild(connectorJobId, "orders");
          for (int i = 0; i < 5; i++) {
            String childJobId = enqueueSnapshotChild(tableJobId, "orders", i + 1L);
            markSnapshotChildSucceededWithoutMaintenance(childJobId, 100L + i);
            finishProjectionRefresh(ACCOUNT_ID, childJobId, 10);
          }

          assertFalse(refreshProjectedParent(ACCOUNT_ID, tableJobId));
          assertEquals(0L, store.get(ACCOUNT_ID, tableJobId).orElseThrow().tablesScanned);

          finishProjectionRefresh(ACCOUNT_ID, tableJobId, 10);

          StoredReconcileJobProjection projection =
              waitForValue(
                      () -> projectionStore().load(ACCOUNT_ID, tableJobId),
                      current ->
                          current.isPresent()
                              && current.get().snapshotsProcessed() == 5L
                              && projectionRefreshCursor(tableJobId) == null,
                      "projection " + tableJobId)
                  .orElseThrow();
          assertEquals(5L, projection.snapshotsProcessed());
        });
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
    withStoreOverrides(
        Map.of(
            "floecat.reconciler.job-store.max-attempts", "2",
            "floecat.reconciler.job-store.base-backoff-ms", "0",
            "floecat.reconciler.job-store.max-backoff-ms", "0"),
        ignored -> {
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
                  () -> store.get(jobId).orElseThrow(),
                  current -> "JS_SUCCEEDED".equals(current.state) && current.errors == 0L,
                  "successful retry " + jobId);
          assertEquals("JS_SUCCEEDED", terminal.state);
          assertEquals(0L, terminal.errors);
          assertEquals(200L, terminal.finishedAtMs);
        });
  }

  @Test
  void retryThenFailureTransitionsToTerminalFailed() {
    String accountId = ACCOUNT_ID;
    String connectorId = CONNECTOR_ID;
    String jobId =
        store.enqueue(
            accountId,
            connectorId,
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty());
    var firstLease = leaseJob(accountId, jobId);
    store.markFailed(jobId, firstLease.leaseEpoch, 100L, "transient", 1L, 0L, 3L, 0L, 2L);

    var secondLease = leaseJob(accountId, jobId);
    assertTrue(!secondLease.leaseEpoch.isBlank());
    assertNotEquals(firstLease.leaseEpoch, secondLease.leaseEpoch);
    store.markFailedTerminal(jobId, secondLease.leaseEpoch, 200L, "terminal", 1L, 0L, 5L, 0L, 2L);

    ReconcileJob failed =
        waitForValue(
            () -> store.get(accountId, jobId).orElseThrow(),
            current -> "JS_FAILED".equals(current.state),
            "terminal failure " + jobId);
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
            () -> store.get(jobId).orElseThrow(),
            current -> "JS_CANCELLED".equals(current.state),
            "job cancellation completion " + jobId);

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
  void rootProjectionDoesNotSucceedUntilExpectedDirectChildrenAreObserved() {
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

    assertTrue(
        Set.of("JS_QUEUED", "JS_RUNNING", "JS_WAITING", "JS_CANCELLING")
            .contains(rootSummary.state));
    assertEquals(1L, rootSummary.tablesScanned);
    assertEquals(1L, rootSummary.tablesChanged);
  }

  @Test
  void nestedFileGroupContributionProjectsToAncestorsWithoutCanonicalRollups() {
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

    StoredReconcileJobProjection snapshot =
        waitForValue(
                () -> projectionStore().load(ACCOUNT_ID, snapshotJobId),
                current ->
                    current.isPresent()
                        && current.get().plannedFileGroups() == 1L
                        && current.get().completedFileGroups() == 1L
                        && current.get().completedFiles() == 1L
                        && current.get().failedFiles() == 1L
                        && current.get().indexesProcessed() == 1L
                        && current.get().statsProcessed() == 2L,
                "snapshot projection " + snapshotJobId)
            .orElseThrow();
    ReconcileJob table =
        waitForValue(
            () -> store.get(ACCOUNT_ID, tableJobId).orElseThrow(),
            current ->
                current.plannedFileGroups == 1L
                    && current.completedFiles == 1L
                    && current.failedFiles == 1L
                    && current.indexesProcessed == 1L,
            "table projection " + tableJobId);
    ReconcileJob connector =
        waitForValue(
            () -> store.get(ACCOUNT_ID, connectorJobId).orElseThrow(),
            current ->
                current.tablesScanned == 1L
                    && current.tablesChanged == 1L
                    && current.completedFiles == 1L
                    && current.failedFiles == 1L
                    && current.indexesProcessed == 1L,
            "connector projection " + connectorJobId);
    StoredReconcileJob snapshotCanonical =
        readStoredRecord(Keys.reconcileJobPointerById(ACCOUNT_ID, snapshotJobId));

    assertEquals(1L, snapshot.plannedFileGroups());
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
    assertEquals(0L, snapshotCanonical.plannedFileGroups);
    assertEquals(0L, snapshotCanonical.completedFileGroups);
    assertEquals(0L, snapshotCanonical.completedFiles);
    assertEquals(0L, snapshotCanonical.failedFiles);
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
    return leaseJob(ACCOUNT_ID, jobId);
  }

  private ReconcileJobStore.LeasedJob leaseJob(String accountId, String jobId) {
    String effectiveAccountId = accountId == null || accountId.isBlank() ? ACCOUNT_ID : accountId;
    String canonicalPointerKey = Keys.reconcileJobPointerById(effectiveAccountId, jobId);
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

  private ReconcileJobStore.LeasedJob leaseJobWithoutMaintenance(String jobId) {
    return leaseJobWithoutMaintenance(ACCOUNT_ID, jobId);
  }

  private ReconcileJobStore.LeasedJob leaseJobWithoutMaintenance(String accountId, String jobId) {
    String effectiveAccountId = accountId == null || accountId.isBlank() ? ACCOUNT_ID : accountId;
    String canonicalPointerKey = Keys.reconcileJobPointerById(effectiveAccountId, jobId);
    StoredReconcileJob readyRecord =
        waitForValueWithoutMaintenance(
            () -> readStoredRecord(canonicalPointerKey),
            current ->
                "JS_QUEUED".equals(current.state)
                    && current.readyPointerKey != null
                    && !current.readyPointerKey.isBlank(),
            "job " + jobId + " to become ready for leasing");
    return assertDoesNotThrow(
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
                        readyRecord))
        .orElseThrow(() -> new IllegalStateException("Unable to lease ready job " + jobId));
  }

  private ReconcileLeaseStore leaseManager() {
    return (ReconcileLeaseStore)
        assertDoesNotThrow(() -> invokePrivateMethod(store, "leaseManager", new Class<?>[] {}));
  }

  private ReconcileJobProjectionStore projectionStore() {
    return (ReconcileJobProjectionStore)
        assertDoesNotThrow(() -> invokePrivateMethod(store, "projections", new Class<?>[] {}));
  }

  private ReconcileJobRootSummaryStore rootSummaryStore() {
    return (ReconcileJobRootSummaryStore)
        assertDoesNotThrow(() -> invokePrivateMethod(store, "rootSummaries", new Class<?>[] {}));
  }

  private ReconcileProjectionRefreshCursorStore projectionRefreshCursorStore() {
    return (ReconcileProjectionRefreshCursorStore)
        assertDoesNotThrow(
            () -> invokePrivateMethod(store, "projectionRefreshCursors", new Class<?>[] {}));
  }

  private StoredReconcileProjectionRefreshCursor projectionRefreshCursor(String parentJobId) {
    return projectionRefreshCursor(ACCOUNT_ID, parentJobId);
  }

  private StoredReconcileProjectionRefreshCursor projectionRefreshCursor(
      String accountId, String parentJobId) {
    String effectiveAccountId = accountId == null || accountId.isBlank() ? ACCOUNT_ID : accountId;
    return projectionRefreshCursorStore().load(effectiveAccountId, parentJobId).orElse(null);
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

  private boolean refreshProjectedParent(String accountId, String parentJobId) {
    return (boolean)
        assertDoesNotThrow(
            () ->
                invokePrivateMethod(
                    store,
                    "refreshProjectedParent",
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

  private <T> T waitForValueWithoutMaintenance(
      Supplier<T> supplier, Predicate<T> done, String description) {
    int attempts = isDynamoMode() ? 400 : 40;
    long sleepMs = isDynamoMode() ? 50L : 0L;
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
    if (!maintenanceEnabled) {
      return;
    }
    store.runMaintenanceOnce(isDynamoMode() ? 10_000L : 100L);
  }

  private void finishProjectionRefresh(String accountId, String parentJobId, int maxPasses) {
    for (int pass = 0; pass < maxPasses; pass++) {
      if (refreshProjectedParent(accountId, parentJobId)) {
        clearDirtyParentPointer(accountId, parentJobId);
        return;
      }
      StoredReconcileJob parent =
          readStoredRecord(Keys.reconcileJobPointerById(accountId, parentJobId));
      if (Math.max(0L, parent.projectionAppliedGeneration)
              >= Math.max(0L, parent.projectionRequestedGeneration)
          && projectionRefreshCursor(accountId, parentJobId) == null) {
        clearDirtyParentPointer(accountId, parentJobId);
        return;
      }
    }
    throw new IllegalStateException("Projection refresh did not complete for " + parentJobId);
  }

  private StoredReconcileJobProjection waitForProjectionTablesScanned(
      String jobId, long expectedTablesScanned) {
    return waitForValue(
            () -> projectionStore().load(ACCOUNT_ID, jobId),
            current ->
                current.isPresent() && current.get().tablesScanned() == expectedTablesScanned,
            "projection " + jobId)
        .orElseThrow();
  }

  private StoredReconcileJobProjection waitForProjectionPresent(String jobId) {
    return waitForProjectionPresent(ACCOUNT_ID, jobId);
  }

  private StoredReconcileJobProjection waitForProjectionPresent(String accountId, String jobId) {
    String effectiveAccountId = accountId == null || accountId.isBlank() ? ACCOUNT_ID : accountId;
    return waitForValue(
            () -> projectionStore().load(effectiveAccountId, jobId),
            Optional::isPresent,
            "projection " + jobId)
        .orElseThrow();
  }

  private void withMaintenanceDisabled(Runnable body) {
    boolean previous = maintenanceEnabled;
    maintenanceEnabled = false;
    try {
      body.run();
    } finally {
      maintenanceEnabled = previous;
    }
  }

  private Optional<Pointer> dirtyParentPointer(String parentJobId) {
    return dirtyParentPointer(ACCOUNT_ID, parentJobId);
  }

  private Optional<Pointer> dirtyParentPointer(String accountId, String parentJobId) {
    String effectiveAccountId = accountId == null || accountId.isBlank() ? ACCOUNT_ID : accountId;
    return store.pointerStore.get(
        Keys.reconcileDirtyParentPointer(effectiveAccountId, parentJobId));
  }

  private void clearDirtyParentPointer(String parentJobId) {
    clearDirtyParentPointer(ACCOUNT_ID, parentJobId);
  }

  private void clearDirtyParentPointer(String accountId, String parentJobId) {
    String effectiveAccountId = accountId == null || accountId.isBlank() ? ACCOUNT_ID : accountId;
    int attempts = isDynamoMode() ? 20 : 5;
    long sleepMs = isDynamoMode() ? 25L : 0L;
    for (int attempt = 0; attempt < attempts; attempt++) {
      Optional<Pointer> pointer = dirtyParentPointer(effectiveAccountId, parentJobId);
      if (pointer.isEmpty()) {
        return;
      }
      if (store.pointerStore.compareAndDelete(pointer.get().getKey(), pointer.get().getVersion())) {
        return;
      }
      if (attempt + 1 < attempts && sleepMs > 0L) {
        try {
          Thread.sleep(sleepMs);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(
              "Interrupted while clearing dirty parent pointer for " + parentJobId, ie);
        }
      }
    }
    assertTrue(
        dirtyParentPointer(effectiveAccountId, parentJobId).isEmpty(),
        "Unable to clear dirty parent pointer for " + parentJobId);
  }

  private StoredReconcileJobProjection waitForProjectionTablesScanned(
      String accountId, String jobId, long expectedTablesScanned) {
    String effectiveAccountId = accountId == null || accountId.isBlank() ? ACCOUNT_ID : accountId;
    return waitForValue(
            () -> projectionStore().load(effectiveAccountId, jobId),
            current ->
                current.isPresent() && current.get().tablesScanned() == expectedTablesScanned,
            "projection " + jobId)
        .orElseThrow();
  }

  private void configureProjectionRefresh(int childPageSize, int maxPagesPerMaintenance) {
    applyStoreOverrides(
        Map.of(
            "floecat.reconciler.projection-refresh.child-page-size",
            Integer.toString(Math.max(1, childPageSize)),
            "floecat.reconciler.projection-refresh.max-pages-per-maintenance",
            Integer.toString(Math.max(1, maxPagesPerMaintenance))));
  }

  private String enqueueTableChild(String parentJobId, String tableId) {
    return enqueueTableChild(ACCOUNT_ID, CONNECTOR_ID, parentJobId, tableId);
  }

  private String enqueueTableChild(
      String accountId, String connectorId, String parentJobId, String tableId) {
    return store.enqueue(
        accountId,
        connectorId,
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.of(List.of(), tableId),
        ReconcileJobKind.PLAN_TABLE,
        ReconcileTableTask.of("db", tableId, tableId, tableId),
        ReconcileExecutionPolicy.defaults(),
        parentJobId,
        "");
  }

  private String enqueueSnapshotChild(String parentJobId, String tableId, long snapshotId) {
    return store.enqueueSnapshotPlan(
        ACCOUNT_ID,
        CONNECTOR_ID,
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.of(List.of(), tableId),
        ReconcileSnapshotTask.of(tableId, snapshotId, "db", tableId, List.of(), true),
        ReconcileExecutionPolicy.defaults(),
        parentJobId,
        "");
  }

  private void markChildSucceeded(String jobId, long finishedAtMs) {
    var lease = leaseJob(jobId);
    store.markSucceeded(jobId, lease.leaseEpoch, finishedAtMs, 1L, 1L, 0L, 0L, 0L, 0L);
  }

  private void markChildSucceededWithoutMaintenance(String jobId, long finishedAtMs) {
    var lease = leaseJobWithoutMaintenance(jobId);
    store.markSucceeded(jobId, lease.leaseEpoch, finishedAtMs, 1L, 1L, 0L, 0L, 0L, 0L);
  }

  private void markChildSucceededWithoutMaintenance(
      String accountId, String jobId, long finishedAtMs) {
    var lease = leaseJobWithoutMaintenance(accountId, jobId);
    store.markSucceeded(jobId, lease.leaseEpoch, finishedAtMs, 1L, 1L, 0L, 0L, 0L, 0L);
  }

  private void markSnapshotChildSucceededWithoutMaintenance(String jobId, long finishedAtMs) {
    var lease = leaseJobWithoutMaintenance(jobId);
    store.markSucceeded(jobId, lease.leaseEpoch, finishedAtMs, 0L, 0L, 0L, 0L, 1L, 0L);
  }

  private void markSnapshotChildSucceededWithoutMaintenance(
      String accountId, String jobId, long finishedAtMs) {
    var lease = leaseJobWithoutMaintenance(accountId, jobId);
    store.markSucceeded(jobId, lease.leaseEpoch, finishedAtMs, 0L, 0L, 0L, 0L, 1L, 0L);
  }

  private void recreateStorePreservingState() {
    PointerStore existingPointerStore = store.pointerStore;
    var existingBlobStore = store.blobStore;
    var existingJobIndexBackend = store.jobIndexBackend;
    var existingLeaseBackend = store.leaseBackend;
    var existingReadyQueueBackend = store.readyQueueBackend;

    DurableReconcileJobStore replacement = new DurableReconcileJobStore();
    replacement.pointerStore = existingPointerStore;
    replacement.blobStore = existingBlobStore;
    replacement.mapper = new ObjectMapper();
    replacement.config = ConfigProvider.getConfig();
    replacement.kvTable = store.kvTable;
    replacement.jobIndexBackend = existingJobIndexBackend;
    replacement.leaseBackend = existingLeaseBackend;
    replacement.readyQueueBackend = existingReadyQueueBackend;
    store = replacement;
    store.init();
  }

  private void applyStoreOverrides(Map<String, String> overrides) {
    Map<String, String> effectiveOverrides = overrides == null ? Map.of() : overrides;
    for (Map.Entry<String, String> entry : effectiveOverrides.entrySet()) {
      if (entry.getValue() == null) {
        System.clearProperty(entry.getKey());
      } else {
        System.setProperty(entry.getKey(), entry.getValue());
      }
    }
    recreateStorePreservingState();
  }

  private void withStoreOverrides(
      Map<String, String> overrides, Consumer<DurableReconcileJobStore> body) {
    Map<String, String> previous = new HashMap<>();
    Map<String, String> effectiveOverrides = overrides == null ? Map.of() : overrides;
    try {
      for (Map.Entry<String, String> entry : effectiveOverrides.entrySet()) {
        previous.put(entry.getKey(), System.getProperty(entry.getKey()));
      }
      applyStoreOverrides(effectiveOverrides);
      body.accept(store);
    } finally {
      applyStoreOverrides(previous);
    }
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
    waitForDynamoReady(table);
    Map<String, AttributeValue> startKey = null;
    do {
      final Map<String, AttributeValue> exclusiveStartKey = startKey;
      var response =
          withDynamoRetry(
              () -> {
                var request = ScanRequest.builder().tableName(table);
                if (exclusiveStartKey != null && !exclusiveStartKey.isEmpty()) {
                  request.exclusiveStartKey(exclusiveStartKey);
                }
                return sharedDynamoDbClient.scan(request.build());
              });
      for (var item : response.items()) {
        withDynamoRetry(
            () -> {
              sharedDynamoDbClient.deleteItem(
                  DeleteItemRequest.builder()
                      .tableName(table)
                      .key(Map.of("pk", item.get("pk"), "sk", item.get("sk")))
                      .build());
              return null;
            });
      }
      startKey = response.lastEvaluatedKey();
    } while (startKey != null && !startKey.isEmpty());
  }

  private void waitForDynamoReady(String table) {
    withDynamoRetry(
        () -> {
          sharedDynamoDbClient.describeTable(request -> request.tableName(table));
          return null;
        });
  }

  private <T> T withDynamoRetry(Supplier<T> supplier) {
    int attempts = 20;
    RuntimeException last = null;
    for (int attempt = 0; attempt < attempts; attempt++) {
      try {
        return supplier.get();
      } catch (RuntimeException e) {
        last = e;
        if (attempt + 1 == attempts || !isRetryableDynamoTestFailure(e)) {
          throw e;
        }
        try {
          Thread.sleep(250L);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException("Interrupted while waiting for Dynamo test fixture", ie);
        }
      }
    }
    throw last == null ? new IllegalStateException("Dynamo retry unexpectedly exhausted") : last;
  }

  private boolean isRetryableDynamoTestFailure(Throwable throwable) {
    Throwable current = throwable;
    while (current != null) {
      String message = current.getMessage();
      if (current instanceof java.net.ConnectException
          || current instanceof java.net.SocketException
          || current instanceof software.amazon.awssdk.core.exception.SdkClientException
          || current
              instanceof software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException
          || (message != null
              && (message.contains("Connection refused")
                  || message.contains("Operation not permitted")
                  || message.contains("Unable to execute HTTP request")))) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }
}
