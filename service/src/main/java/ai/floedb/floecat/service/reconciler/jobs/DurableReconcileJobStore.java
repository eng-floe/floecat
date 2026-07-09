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

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.reconciler.impl.PlannedFileGroupJob;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.impl.SnapshotPlanBlobStore.SnapshotPlanBlob;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import ai.floedb.floecat.reconciler.jobs.SnapshotPlanManifestIds;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredFileGroupResultPayload;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredJobLease;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobListSummary;
import ai.floedb.floecat.service.reconciler.jobs.durable.projection.ReconcileJobProjectionStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.projection.ReconcileJobProjector;
import ai.floedb.floecat.service.reconciler.jobs.durable.projection.ReconcileJobProjector.JobProjection;
import ai.floedb.floecat.service.reconciler.jobs.durable.projection.ReconcileJobRootSummaryStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.queue.ReconcileAncestorRollupService;
import ai.floedb.floecat.service.reconciler.jobs.durable.queue.ReconcileJobCancellationService;
import ai.floedb.floecat.service.reconciler.jobs.durable.queue.ReconcileJobCompleter;
import ai.floedb.floecat.service.reconciler.jobs.durable.queue.ReconcileJobEnqueuer;
import ai.floedb.floecat.service.reconciler.jobs.durable.queue.ReconcileJobMaintenanceService;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileJobDetailLoader;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileJobExecutionLoader;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileJobIndexes;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileLeaseStateCodec;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcilePayloadStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.DynamoReconcileJobIndexBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.DynamoReconcileLeaseBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.DynamoReconcileReadyQueueBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.LeaseScanAbortedException;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.MemoryReconcileJobIndexBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.MemoryReconcileLeaseBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.MemoryReconcileReadyQueueBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.NativeReconcileJobIndexStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.NativeReconcileLeaseStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.NativeReconcileReadyQueueStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileLeaseBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileLeaseStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileReadyQueueBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileReadyQueueStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileReadyQueueStore.LeaseScanStats;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.storage.aws.DynamoDbClientManager;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Context;
import io.grpc.Deadline;
import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@ApplicationScoped
@IfBuildProperty(name = "floecat.reconciler.job-store", stringValue = "durable")
// Domain model:
// 1. Canonical job state owns all derived job-index pointers and updates them transactionally.
// 2. Lease coordination pointers are a separate runtime ownership domain owned by
//    ReconcileLeaseStore.
// 3. File-group result pointers are payload-reference state; aggregate counters and parent state
//    are observability projection state and are no longer updated on the child hot path.
// This store expects the post-port inline canonical reconcile layout only. It does not read
// legacy StoredJobReference indirection, lane queue/head scheduling rows, or separate lease-state
// blobs.
public class DurableReconcileJobStore implements ReconcileJobStore {
  public static final class ConnectorDeletedException extends IllegalStateException {
    public final String connectorId;

    public ConnectorDeletedException(String connectorId) {
      super("connector deleted: " + blankToEmpty(connectorId));
      this.connectorId = blankToEmpty(connectorId);
    }
  }

  private static final Logger LOG = Logger.getLogger(DurableReconcileJobStore.class);

  private static final int DEFAULT_MAX_ATTEMPTS = 8;
  private static final long DEFAULT_BASE_BACKOFF_MS = 500L;
  private static final long DEFAULT_MAX_BACKOFF_MS = 30_000L;
  private static final long DEFAULT_LEASE_MS = 30_000L;
  private static final long DEFAULT_RECLAIM_INTERVAL_MS = 5_000L;
  private static final long DEFAULT_LEASE_RENEW_GRACE_MS = 5_000L;
  private static final long CANCEL_POKE_MAX_DELAY_MS = 1_000L;
  private static final int DEFAULT_READY_SCAN_LIMIT = 128;
  // Caps how many ready-queue lease scans may run concurrently across all callers (the in-process
  // poller plus every external executor that funnels through leaseNext). Without this, client-side
  // lease timeouts + retries amplify into a runaway fan-out of overlapping DynamoDB scans that
  // saturate the client and collapse throughput.
  private static final int DEFAULT_LEASE_MAX_CONCURRENCY = 8;
  // How long a caller waits for a scan permit before rejecting the attempt.
  private static final long DEFAULT_LEASE_ACQUIRE_TIMEOUT_MS = 250L;
  // Wall-clock budget for a single ready scan. Kept under the worker-control client deadline so a
  // scan a caller has already abandoned yields its permit instead of running to completion. 0 =
  // off.
  private static final long DEFAULT_LEASE_SCAN_BUDGET_MS = 8_000L;
  private static final int CAS_MAX = 16;
  private static final String INLINE_FINALIZED_SNAPSHOT_EVENT_PREFIX =
      "inline:reconcile-finalized-snapshot:";
  @Inject PointerStore pointerStore;
  @Inject BlobStore blobStore;
  @Inject ObjectMapper mapper;
  @Inject Config config;
  @Inject Instance<DynamoDbClientManager> dynamoDbClientManager;

  @ConfigProperty(name = "floecat.kv.table", defaultValue = "floecat_pointers")
  String kvTable = "floecat_pointers";

  @Inject ReconcilePayloadStore payloadStore;
  @Inject ReconcileJobIndexes jobIndexes;
  @Inject ReconcileJobIndexBackend jobIndexBackend;
  @Inject ReconcileJobIndexStore jobIndexStore;
  @Inject ReconcileJobProjector projector;
  @Inject ReconcileJobProjectionStore projectionStore;
  @Inject ReconcileJobRootSummaryStore rootSummaryStore;
  @Inject ReconcileAncestorRollupService ancestorRollupService;
  @Inject ReconcileLeaseStore leaseStore;
  @Inject ReconcileJobDetailLoader detailLoader;
  @Inject ReconcileJobExecutionLoader executionLoader;
  @Inject ReconcileLeaseStateCodec leaseStateCodec;
  @Inject ReconcileLeaseBackend leaseBackend;
  @Inject ReconcileJobEnqueuer enqueuer;
  @Inject ReconcileJobCancellationService cancellationService;
  @Inject ReconcileJobCompleter completer;
  @Inject ReconcileJobMaintenanceService maintenanceService;
  @Inject ReconcileReadyQueueStore readyQueueStore;
  @Inject ReconcileReadyQueueBackend readyQueueBackend;
  @Inject ConnectorRepository connectorRepo;
  private int maxAttempts = DEFAULT_MAX_ATTEMPTS;
  private long baseBackoffMs = DEFAULT_BASE_BACKOFF_MS;
  private long maxBackoffMs = DEFAULT_MAX_BACKOFF_MS;
  private long leaseMs = DEFAULT_LEASE_MS;
  private long reclaimIntervalMs = DEFAULT_RECLAIM_INTERVAL_MS;
  private long leaseRenewGraceMs = DEFAULT_LEASE_RENEW_GRACE_MS;
  private int readyScanLimit = DEFAULT_READY_SCAN_LIMIT;
  private int leaseMaxConcurrency = DEFAULT_LEASE_MAX_CONCURRENCY;
  long leaseAcquireTimeoutMs = DEFAULT_LEASE_ACQUIRE_TIMEOUT_MS;
  private long leaseScanBudgetMs = DEFAULT_LEASE_SCAN_BUDGET_MS;
  volatile Semaphore leaseScanPermits = new Semaphore(DEFAULT_LEASE_MAX_CONCURRENCY, true);

  private ReconcilePayloadStore payloads() {
    if (payloadStore == null) {
      payloadStore = new ReconcilePayloadStore();
    }
    payloadStore.bind(blobStore, pointerStore, mapper);
    return payloadStore;
  }

  private ReconcileJobProjector projector() {
    if (projector == null) {
      projector = new ReconcileJobProjector();
    }
    projector.bind(detailReader());
    return projector;
  }

  private ReconcileJobDetailLoader detailReader() {
    if (detailLoader == null) {
      detailLoader = new ReconcileJobDetailLoader();
    }
    detailLoader.bind(payloads());
    return detailLoader;
  }

  private ReconcileJobExecutionLoader executionLoader() {
    if (executionLoader == null) {
      executionLoader = new ReconcileJobExecutionLoader();
    }
    executionLoader.bind(payloads());
    return executionLoader;
  }

  private ReconcileLeaseStateCodec leaseStateCodec() {
    if (leaseStateCodec == null) {
      leaseStateCodec = new ReconcileLeaseStateCodec();
    }
    leaseStateCodec.bind(payloads());
    return leaseStateCodec;
  }

  private ReconcileJobIndexStore jobIndexStore() {
    if (jobIndexStore == null) {
      jobIndexStore = new NativeReconcileJobIndexStore();
    }
    if (jobIndexBackend == null) {
      String kvMode = config.getOptionalValue("floecat.kv", String.class).orElse("memory");
      if ("memory".equalsIgnoreCase(kvMode)) {
        jobIndexBackend = new MemoryReconcileJobIndexBackend();
      } else if ("dynamodb".equalsIgnoreCase(kvMode)) {
        jobIndexBackend = new DynamoReconcileJobIndexBackend();
      } else {
        throw new IllegalStateException(
            "No reconcile job index backend available for floecat.kv=" + kvMode);
      }
    }
    if (jobIndexBackend instanceof MemoryReconcileJobIndexBackend memoryBackend) {
      memoryBackend.bind(pointerStore);
    } else if (jobIndexBackend instanceof DynamoReconcileJobIndexBackend dynamoBackend
        && dynamoDbClientManager != null
        && dynamoDbClientManager.isResolvable()) {
      DynamoDbClientManager manager = dynamoDbClientManager.get();
      dynamoBackend.bind(manager::current, kvTable, manager::refreshAfterFailure);
    }
    jobIndexStore.bind(
        jobIndexBackend,
        payloads(),
        indexes(),
        CAS_MAX,
        DurableReconcileJobStore::assertImmutableJobIdentityPreserved,
        this::logStateTransition);
    return jobIndexStore;
  }

  private ReconcileJobIndexes indexes() {
    if (jobIndexes == null) {
      jobIndexes = new ReconcileJobIndexes();
    }
    jobIndexes.bind(
        pointerStore, DurableReconcileJobStore::requiresReadyPointer, this::readyPointerKeys);
    return jobIndexes;
  }

  private ReconcileJobProjectionStore projections() {
    if (projectionStore == null) {
      projectionStore = new ReconcileJobProjectionStore();
    }
    projectionStore.bind(pointerStore, payloads());
    return projectionStore;
  }

  private ReconcileJobRootSummaryStore rootSummaries() {
    if (rootSummaryStore == null) {
      rootSummaryStore = new ReconcileJobRootSummaryStore();
    }
    rootSummaryStore.bind(pointerStore, payloads());
    return rootSummaryStore;
  }

  private ReconcileAncestorRollupService ancestorRollups() {
    if (ancestorRollupService == null) {
      ancestorRollupService = new ReconcileAncestorRollupService();
    }
    ancestorRollupService.bind(jobIndexStore(), projector(), this::hasLiveLeaseForProjection);
    return ancestorRollupService;
  }

  private boolean hasLiveLeaseForProjection(
      StoredReconcileJob record, boolean tolerateLeasePointerDrift, long nowMs) {
    return leaseManager().hasLiveLease(record, tolerateLeasePointerDrift, nowMs);
  }

  private ReconcileLeaseStore leaseManager() {
    if (leaseStore == null) {
      leaseStore = new NativeReconcileLeaseStore();
    }
    if (leaseBackend == null) {
      String kvMode = config.getOptionalValue("floecat.kv", String.class).orElse("memory");
      if ("memory".equalsIgnoreCase(kvMode)) {
        leaseBackend = new MemoryReconcileLeaseBackend();
      } else if ("dynamodb".equalsIgnoreCase(kvMode)) {
        leaseBackend = new DynamoReconcileLeaseBackend();
      } else {
        throw new IllegalStateException(
            "No reconcile lease backend available for floecat.kv=" + kvMode);
      }
    }
    if (leaseBackend instanceof MemoryReconcileLeaseBackend memoryBackend) {
      memoryBackend.bind(pointerStore);
    } else if (leaseBackend instanceof DynamoReconcileLeaseBackend dynamoBackend
        && dynamoDbClientManager != null
        && dynamoDbClientManager.isResolvable()) {
      DynamoDbClientManager manager = dynamoDbClientManager.get();
      dynamoBackend.bind(manager::current, kvTable, manager::refreshAfterFailure);
    }
    leaseStore.bind(
        leaseBackend,
        executionLoader(),
        leaseStateCodec(),
        CAS_MAX,
        leaseMs,
        leaseRenewGraceMs,
        jobIndexStore(),
        (canonicalPointerKey, mutator) ->
            jobIndexStore()
                .loadCanonicalSnapshot(canonicalPointerKey)
                .flatMap(
                    ignored ->
                        mutateByCanonicalPointerReturningRecord(canonicalPointerKey, mutator)
                            .map(
                                env ->
                                    new ReconcileJobIndexStore.CanonicalEnvelope(
                                        env.canonicalPointerKey, env.record))),
        DurableReconcileJobStore::isTerminalState,
        DurableReconcileJobStore::assertImmutableJobIdentityPreserved,
        maxAttempts,
        this::backoffMs);
    return leaseStore;
  }

  private ReconcileReadyQueueStore readyQueue() {
    if (readyQueueStore == null) {
      readyQueueStore = new NativeReconcileReadyQueueStore();
    }
    if (readyQueueBackend == null) {
      String kvMode = config.getOptionalValue("floecat.kv", String.class).orElse("memory");
      if ("memory".equalsIgnoreCase(kvMode)) {
        MemoryReconcileReadyQueueBackend memoryBackend = new MemoryReconcileReadyQueueBackend();
        memoryBackend.bind(pointerStore);
        readyQueueBackend = memoryBackend;
      } else if ("dynamodb".equalsIgnoreCase(kvMode)) {
        readyQueueBackend = new DynamoReconcileReadyQueueBackend();
      } else {
        throw new IllegalStateException(
            "No reconcile ready queue backend available for floecat.kv=" + kvMode);
      }
    }
    if (readyQueueBackend instanceof MemoryReconcileReadyQueueBackend memoryBackend) {
      memoryBackend.bind(pointerStore);
    } else if (readyQueueBackend instanceof DynamoReconcileReadyQueueBackend dynamoBackend
        && dynamoDbClientManager != null
        && dynamoDbClientManager.isResolvable()) {
      DynamoDbClientManager manager = dynamoDbClientManager.get();
      dynamoBackend.bind(manager::current, kvTable, manager::refreshAfterFailure);
    }
    readyQueueStore.bind(
        readyQueueBackend,
        jobIndexStore(),
        leaseManager(),
        readyScanLimit,
        DurableReconcileJobStore::requiresReadyPointer,
        this::isBlockedByAncestorCancellation);
    return readyQueueStore;
  }

  private ReconcileJobCompleter completer() {
    if (completer == null) {
      completer = new ReconcileJobCompleter();
    }
    completer.bind(
        leaseManager(),
        (jobId, mutator) ->
            mutateByJobIdReturningRecord(jobId, mutator)
                .map(
                    env ->
                        new ReconcileJobCompleter.CanonicalEnvelope(
                            env.canonicalPointerKey, env.record)));
    return completer;
  }

  private ReconcileJobEnqueuer enqueuer() {
    if (enqueuer == null) {
      enqueuer = new ReconcileJobEnqueuer();
    }
    enqueuer.bind(
        blobStore,
        payloads(),
        projector(),
        jobIndexStore(),
        indexes(),
        this::materializeSnapshotPlanFileGroups,
        readyQueue()::readyPointerKeys,
        this::statePointerKeys,
        readyQueue()::readyPointerKeyForDue,
        this::writeFileGroupResultPayloadBlobReference);
    return enqueuer;
  }

  private ReconcileJobCancellationService cancellation() {
    if (cancellationService == null) {
      cancellationService = new ReconcileJobCancellationService();
    }
    cancellationService.bind(
        leaseManager(),
        jobId ->
            loadByAnyAccount(jobId)
                .map(
                    env ->
                        new ReconcileJobCancellationService.CanonicalEnvelope(
                            env.canonicalPointerKey, env.record)),
        (canonicalPointerKey, mutator) ->
            mutateByCanonicalPointerReturningRecord(canonicalPointerKey, mutator)
                .map(
                    env ->
                        new ReconcileJobCancellationService.CanonicalEnvelope(
                            env.canonicalPointerKey, env.record)),
        this::get,
        (env, jobId, leaseEpoch) ->
            clearExecutionLeasesIfOwned(
                new StoredEnvelope(env.canonicalPointerKey(), env.record()), jobId, leaseEpoch),
        CANCEL_POKE_MAX_DELAY_MS);
    return cancellationService;
  }

  private ReconcileJobMaintenanceService maintenance() {
    if (maintenanceService == null) {
      maintenanceService = new ReconcileJobMaintenanceService();
    }
    maintenanceService.bind(
        leaseManager(),
        pointerStore,
        this::reclaimExpiredLease,
        this::refreshProjectedParent,
        readyScanLimit,
        reclaimIntervalMs);
    return maintenanceService;
  }

  @PostConstruct
  void init() {
    maxAttempts =
        Math.max(
            1,
            config
                .getOptionalValue("floecat.reconciler.job-store.max-attempts", Integer.class)
                .orElse(DEFAULT_MAX_ATTEMPTS));
    baseBackoffMs =
        Math.max(
            100L,
            config
                .getOptionalValue("floecat.reconciler.job-store.base-backoff-ms", Long.class)
                .orElse(DEFAULT_BASE_BACKOFF_MS));
    maxBackoffMs =
        Math.max(
            baseBackoffMs,
            config
                .getOptionalValue("floecat.reconciler.job-store.max-backoff-ms", Long.class)
                .orElse(DEFAULT_MAX_BACKOFF_MS));
    leaseMs =
        Math.max(
            1_000L,
            config
                .getOptionalValue("floecat.reconciler.job-store.lease-ms", Long.class)
                .orElse(DEFAULT_LEASE_MS));
    reclaimIntervalMs =
        Math.max(
            1_000L,
            config
                .getOptionalValue("floecat.reconciler.job-store.reclaim-interval-ms", Long.class)
                .orElse(DEFAULT_RECLAIM_INTERVAL_MS));
    leaseRenewGraceMs =
        Math.max(
            0L,
            config
                .getOptionalValue("floecat.reconciler.job-store.lease-renew-grace-ms", Long.class)
                .orElse(DEFAULT_LEASE_RENEW_GRACE_MS));
    readyScanLimit =
        Math.max(
            1,
            config
                .getOptionalValue("floecat.reconciler.job-store.ready-scan-limit", Integer.class)
                .orElse(DEFAULT_READY_SCAN_LIMIT));
    leaseMaxConcurrency =
        Math.max(
            1,
            config
                .getOptionalValue(
                    "floecat.reconciler.job-store.lease-max-concurrency", Integer.class)
                .orElse(DEFAULT_LEASE_MAX_CONCURRENCY));
    leaseAcquireTimeoutMs =
        Math.max(
            0L,
            config
                .getOptionalValue(
                    "floecat.reconciler.job-store.lease-acquire-timeout-ms", Long.class)
                .orElse(DEFAULT_LEASE_ACQUIRE_TIMEOUT_MS));
    leaseScanBudgetMs =
        Math.max(
            0L,
            config
                .getOptionalValue("floecat.reconciler.job-store.lease-scan-budget-ms", Long.class)
                .orElse(DEFAULT_LEASE_SCAN_BUDGET_MS));
    leaseScanPermits = new Semaphore(leaseMaxConcurrency, true);
  }

  @Override
  public String enqueue(
      String accountId,
      String connectorId,
      boolean fullRescan,
      CaptureMode captureMode,
      ReconcileScope incomingScope,
      ReconcileJobKind jobKind,
      ReconcileTableTask tableTask,
      ReconcileViewTask viewTask,
      ReconcileSnapshotTask snapshotTask,
      ReconcileFileGroupTask fileGroupTask,
      ReconcileExecutionPolicy executionPolicy,
      String parentJobId,
      String pinnedExecutorId) {
    BulkEnqueueResult result =
        bulkEnqueue(
            List.of(
                BulkEnqueueSpec.of(
                    accountId,
                    connectorId,
                    fullRescan,
                    captureMode,
                    incomingScope,
                    jobKind,
                    tableTask,
                    viewTask,
                    snapshotTask,
                    fileGroupTask,
                    executionPolicy,
                    parentJobId,
                    pinnedExecutorId)));
    throwIfConnectorDeleted(result);
    return result.singleJobId();
  }

  @Override
  public BulkEnqueueResult bulkEnqueue(List<BulkEnqueueSpec> specs) {
    List<BulkEnqueueSpec> effectiveSpecs = specs == null ? List.of() : specs;
    var admittedSpecs = new ArrayList<BulkEnqueueSpec>(effectiveSpecs.size());
    var rejectedItems = new ArrayList<BulkEnqueueItemResult>();
    for (int index = 0; index < effectiveSpecs.size(); index++) {
      BulkEnqueueSpec spec = effectiveSpecs.get(index);
      if (isRootPlanConnector(spec)
          && !canonicalConnectorExists(spec.accountId, spec.connectorId)) {
        rejectedItems.add(
            new BulkEnqueueItemResult(
                index,
                "",
                false,
                "connector deleted: " + blankToEmpty(spec.connectorId),
                false,
                BulkEnqueueItemResult.FailureReason.CONNECTOR_DELETED,
                blankToEmpty(spec.connectorId)));
        continue;
      }
      admittedSpecs.add(spec);
    }
    BulkEnqueueResult admittedResult = onHotPath(() -> enqueuer().bulkEnqueue(admittedSpecs));
    var itemsByIndex = new java.util.HashMap<Integer, BulkEnqueueItemResult>();
    for (BulkEnqueueItemResult item : rejectedItems) {
      itemsByIndex.put(item.index, item);
    }
    int admittedIndex = 0;
    for (int originalIndex = 0; originalIndex < effectiveSpecs.size(); originalIndex++) {
      if (itemsByIndex.containsKey(originalIndex)) {
        continue;
      }
      BulkEnqueueItemResult item = admittedResult.items.get(admittedIndex++);
      itemsByIndex.put(
          originalIndex,
          new BulkEnqueueItemResult(
              originalIndex,
              item.jobId,
              item.created,
              item.error,
              item.invalidRequest,
              item.failureReason,
              item.failureSubjectId));
    }
    BulkEnqueueResult result =
        new BulkEnqueueResult(
            java.util.stream.IntStream.range(0, effectiveSpecs.size())
                .mapToObj(itemsByIndex::get)
                .toList());
    for (BulkEnqueueItemResult item : result.items) {
      if (item == null || !item.succeeded()) {
        continue;
      }
      BulkEnqueueSpec spec =
          item.index >= 0 && item.index < effectiveSpecs.size()
              ? effectiveSpecs.get(item.index)
              : null;
      if (spec == null) {
        continue;
      }
      upsertRootSummaryByJobId(item.jobId);
      if (isParentCapable(spec.jobKind)) {
        markDirtyParent(spec.accountId, item.jobId);
      }
      if (item.created) {
        resetFinalizedSnapshotIfEligible(spec);
        requestProjectionRefresh(spec.accountId, spec.parentJobId, 1L);
      } else {
        markDirtyParent(spec.accountId, spec.parentJobId);
      }
    }
    return result;
  }

  private boolean isRootPlanConnector(BulkEnqueueSpec spec) {
    return spec != null
        && spec.jobKind == ReconcileJobKind.PLAN_CONNECTOR
        && blankToEmpty(spec.parentJobId).isBlank();
  }

  private void throwIfConnectorDeleted(BulkEnqueueResult result) {
    if (result == null || result.items.size() != 1) {
      return;
    }
    BulkEnqueueItemResult item = result.items.getFirst();
    if (item == null
        || item.failureReason != BulkEnqueueItemResult.FailureReason.CONNECTOR_DELETED) {
      return;
    }
    throw new ConnectorDeletedException(item.failureSubjectId);
  }

  private boolean canonicalConnectorExists(String accountId, String connectorId) {
    if (connectorRepo == null) {
      return true;
    }
    if (blankToEmpty(accountId).isBlank() || blankToEmpty(connectorId).isBlank()) {
      return false;
    }
    var resourceId =
        ai.floedb.floecat.common.rpc.ResourceId.newBuilder()
            .setAccountId(accountId)
            .setId(connectorId)
            .setKind(ai.floedb.floecat.common.rpc.ResourceKind.RK_CONNECTOR)
            .build();
    return connectorRepo.existsById(resourceId);
  }

  @Override
  public boolean bulkEnqueueAndApplyLeaseOutcome(
      List<BulkEnqueueSpec> specs,
      String jobId,
      String leaseEpoch,
      CompletionKind completionKind,
      long finishedAtMs,
      String message,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed) {
    List<BulkEnqueueSpec> effectiveSpecs = specs == null ? List.of() : specs;
    if (effectiveSpecs.isEmpty()) {
      return applyLeaseOutcome(
          jobId,
          leaseEpoch,
          completionKind,
          finishedAtMs,
          message,
          tablesScanned,
          tablesChanged,
          viewsScanned,
          viewsChanged,
          errors,
          snapshotsProcessed,
          statsProcessed);
    }
    AtomicReference<StoredEnvelope> completedParent = new AtomicReference<>();
    BulkEnqueueResult result;
    try {
      result =
          onHotPath(
              () ->
                  enqueuer()
                      .bulkEnqueue(
                          effectiveSpecs,
                          queuedInserts -> {
                            StoredEnvelope loaded = loadByAnyAccount(jobId).orElse(null);
                            if (loaded == null) {
                              throw new IllegalStateException(
                                  "reconcile job not found while applying planner outcome: "
                                      + jobId);
                            }
                            var snapshot =
                                jobIndexStore()
                                    .loadCanonicalSnapshot(loaded.canonicalPointerKey)
                                    .orElse(null);
                            StoredReconcileJob previous =
                                snapshot == null
                                    ? null
                                    : jobIndexStore().readRecord(snapshot).orElse(null);
                            if (snapshot == null || previous == null) {
                              throw new IllegalStateException(
                                  "reconcile job missing canonical state while applying planner outcome: "
                                      + jobId);
                            }
                            StoredReconcileJob next =
                                applyLeaseOutcomeToRecord(
                                    loaded.canonicalPointerKey,
                                    jobIndexStore().cloneStoredRecord(previous),
                                    jobId,
                                    leaseEpoch,
                                    completionKind,
                                    finishedAtMs,
                                    message,
                                    tablesScanned,
                                    tablesChanged,
                                    viewsScanned,
                                    viewsChanged,
                                    errors,
                                    snapshotsProcessed,
                                    statsProcessed);
                            if (next == null) {
                              throw new LeaseOutcomeRejectedException();
                            }
                            completedParent.set(
                                new StoredEnvelope(
                                    loaded.canonicalPointerKey,
                                    jobIndexStore().cloneStoredRecord(next)));
                            return List.of(
                                new ReconcileJobIndexStore.CanonicalRecordMutation(
                                    snapshot, previous, next));
                          }));
    } catch (LeaseOutcomeRejectedException rejected) {
      return isIdempotentTerminalLeaseOutcome(
          jobId,
          completionKind,
          finishedAtMs,
          message,
          tablesScanned,
          tablesChanged,
          viewsScanned,
          viewsChanged,
          errors,
          snapshotsProcessed,
          statsProcessed);
    }
    if (completedParent.get() == null) {
      return isIdempotentTerminalLeaseOutcome(
          jobId,
          completionKind,
          finishedAtMs,
          message,
          tablesScanned,
          tablesChanged,
          viewsScanned,
          viewsChanged,
          errors,
          snapshotsProcessed,
          statsProcessed);
    }
    for (BulkEnqueueItemResult item : result.items) {
      if (item == null || item.succeeded()) {
        continue;
      }
      throw new IllegalStateException(
          "Atomic planner enqueue failed index="
              + item.index
              + " error="
              + blankToEmpty(item.error));
    }
    StoredEnvelope persistedParent = loadByAnyAccount(jobId).orElse(null);
    if (!matchesLeaseOutcome(
        persistedParent == null ? null : persistedParent.record,
        completionKind,
        finishedAtMs,
        message,
        tablesScanned,
        tablesChanged,
        viewsScanned,
        viewsChanged,
        errors,
        snapshotsProcessed,
        statsProcessed)) {
      return false;
    }
    clearExecutionLeasesIfOwned(persistedParent, jobId, leaseEpoch);
    markDirtyParentForRecord(persistedParent.record);
    upsertRootSummaryForRecord(persistedParent.record);
    for (BulkEnqueueItemResult item : result.items) {
      if (item == null || !item.succeeded()) {
        continue;
      }
      BulkEnqueueSpec spec =
          item.index >= 0 && item.index < effectiveSpecs.size()
              ? effectiveSpecs.get(item.index)
              : null;
      if (spec == null) {
        continue;
      }
      upsertRootSummaryByJobId(item.jobId);
      if (isParentCapable(spec.jobKind)) {
        markDirtyParent(spec.accountId, item.jobId);
      }
      if (item.created) {
        resetFinalizedSnapshotIfEligible(spec);
        requestProjectionRefresh(spec.accountId, spec.parentJobId, 1L);
      } else {
        markDirtyParent(spec.accountId, spec.parentJobId);
      }
    }
    return true;
  }

  @Override
  public Optional<ReconcileJob> get(String accountId, String jobId) {
    var loaded = loadByAnyAccount(jobId);
    if (loaded.isEmpty()) {
      return Optional.empty();
    }
    if (accountId != null
        && !accountId.isBlank()
        && !accountId.equals(loaded.get().record.accountId)) {
      return Optional.empty();
    }
    return Optional.of(
        projector()
            .toPublicJob(loaded.get().record, storedProjectionForRead(loaded.get().record), true));
  }

  @Override
  public Optional<ReconcileJob> getLeaseView(String jobId) {
    var loaded = loadByAnyAccount(jobId);
    if (loaded.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(projector().toCanonicalLeaseView(loaded.get().record));
  }

  @Override
  public ReconcileJobPage list(
      String accountId,
      int pageSize,
      String pageToken,
      String connectorId,
      java.util.Set<String> states) {
    var page =
        jobIndexStore()
            .listStoredJobs(
                accountId,
                pageSize,
                pageToken,
                connectorId,
                states == null ? java.util.Set.of() : states);
    return new ReconcileJobPage(
        page.records().stream()
            .map(stored -> projector().toPublicJobSummary(stored, storedProjectionForRead(stored)))
            .toList(),
        page.nextPageToken());
  }

  @Override
  public ReconcileJobPage listRootJobs(
      String accountId,
      int pageSize,
      String pageToken,
      String connectorId,
      java.util.Set<String> states) {
    var page = rootSummaries().listSummaries(accountId, pageSize, pageToken, connectorId, states);
    return new ReconcileJobPage(
        page.summaries().stream().map(ReconcileJobRootSummaryStore::toPublicJob).toList(),
        page.nextPageToken());
  }

  @Override
  public ReconcileJobPage childJobsPage(
      String accountId, String parentJobId, int pageSize, String pageToken) {
    var page = jobIndexStore().listStoredChildJobs(accountId, parentJobId, pageSize, pageToken);
    List<ReconcileJob> out = new java.util.ArrayList<>(page.records().size());
    for (StoredReconcileJob stored : page.records()) {
      out.add(projector().toPublicJob(stored, storedProjectionForRead(stored), true));
    }
    return new ReconcileJobPage(out, page.nextPageToken());
  }

  @Override
  public Optional<FinalizedSnapshotEvent> getFinalizedSnapshot(
      String accountId, String tableId, long snapshotId) {
    if (blank(accountId) || blank(tableId) || snapshotId < 0L) {
      return Optional.empty();
    }
    String key = Keys.reconcileFinalizedSnapshotIdentityPointer(accountId, tableId, snapshotId);
    return pointerStore
        .get(key)
        .flatMap(pointer -> readFinalizedSnapshotEvent(pointer.getBlobUri()))
        .map(
            stored ->
                new FinalizedSnapshotEvent(
                    blankToEmpty(stored.eventId),
                    blankToEmpty(stored.accountId),
                    blankToEmpty(stored.tableId),
                    stored.snapshotId,
                    stored.finalizedAtMs,
                    blankToEmpty(stored.finalizerJobId)));
  }

  @Override
  public List<ReconcileJob> jobTree(String accountId, String rootJobId) {
    if (rootJobId == null || rootJobId.isBlank()) {
      return List.of();
    }
    StoredEnvelope rootEnvelope = loadByAnyAccount(rootJobId).orElse(null);
    if (rootEnvelope == null
        || (accountId != null
            && !accountId.isBlank()
            && !accountId.equals(rootEnvelope.record.accountId))) {
      return List.of();
    }
    List<ReconcileJob> out = new java.util.ArrayList<>();
    java.util.ArrayDeque<String> pendingParents = new java.util.ArrayDeque<>();
    out.add(
        projector()
            .toPublicTreeJob(rootEnvelope.record, storedProjectionForRead(rootEnvelope.record)));
    pendingParents.add(rootJobId);
    while (!pendingParents.isEmpty()) {
      String parentJobId = pendingParents.removeFirst();
      String nextToken = "";
      do {
        var page =
            jobIndexStore()
                .listStoredChildJobs(rootEnvelope.record.accountId, parentJobId, 1000, nextToken);
        for (StoredReconcileJob stored : page.records()) {
          out.add(projector().toPublicTreeJob(stored, storedProjectionForRead(stored)));
          if (isParentCapable(stored.jobKind())) {
            pendingParents.addLast(stored.jobId);
          }
        }
        nextToken = page.nextPageToken();
      } while (nextToken != null && !nextToken.isBlank());
    }
    return out;
  }

  @Override
  public QueueStats queueStats() {
    return new QueueStats(0L, 0L, 0L, 0L);
  }

  @Override
  public Optional<LeasedJob> leaseNext(LeaseRequest request) {
    Semaphore permits = leaseScanPermits;
    boolean acquired;
    try {
      acquired = permits.tryAcquire(leaseAcquireTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new CancellationException("reconcile lease scan admission interrupted");
    }
    if (!acquired) {
      throw new LeaseScanCapacityExceededException(
          "reconcile lease scan capacity exhausted cap=" + leaseMaxConcurrency);
    }
    long startedAtMs = System.currentTimeMillis();
    LeaseRequest effective = request == null ? LeaseRequest.all() : request;
    LeaseScanStats scanStats = new LeaseScanStats();
    configureLeaseScanStats(scanStats, startedAtMs);
    try {
      Optional<LeasedJob> leased;
      try {
        leased = leaseReadyDue(startedAtMs, effective, scanStats);
      } catch (LeaseScanAbortedException aborted) {
        if (aborted.callerCancelled()) {
          scanStats.abortedByCaller = true;
        } else {
          scanStats.abortedByDeadline = true;
        }
        leased = Optional.empty();
      }
      long totalMs = System.currentTimeMillis() - startedAtMs;
      if (scanStats.abortedByCaller) {
        LOG.warnf(
            "leaseNext cancelled by caller total_ms=%d scan_count=%d candidate_count=%d"
                + " pruned=%d",
            totalMs, scanStats.scanCount, scanStats.candidateCount, scanStats.prunedCount);
      } else if (scanStats.abortedByDeadline) {
        LOG.warnf(
            "leaseNext aborted by scan deadline total_ms=%d scan_count=%d candidate_count=%d"
                + " pruned=%d budget_ms=%d",
            totalMs,
            scanStats.scanCount,
            scanStats.candidateCount,
            scanStats.prunedCount,
            leaseScanBudgetMs);
      } else {
        LOG.debugf(
            "leaseNext total_ms=%d scan_count=%d candidate_count=%d pruned=%d leased=%s",
            totalMs,
            scanStats.scanCount,
            scanStats.candidateCount,
            scanStats.prunedCount,
            leased.isPresent());
      }
      return leased;
    } finally {
      permits.release();
    }
  }

  private void configureLeaseScanStats(LeaseScanStats scanStats, long startedAtMs) {
    Context grpcContext = Context.current();
    scanStats.cancelled = grpcContext::isCancelled;
    long deadlineAtMs = leaseScanBudgetMs <= 0L ? 0L : startedAtMs + leaseScanBudgetMs;
    Deadline grpcDeadline = grpcContext.getDeadline();
    if (grpcDeadline != null) {
      long remainingMs = grpcDeadline.timeRemaining(TimeUnit.MILLISECONDS);
      long grpcDeadlineAtMs = startedAtMs + Math.max(0L, remainingMs);
      deadlineAtMs =
          deadlineAtMs <= 0L ? grpcDeadlineAtMs : Math.min(deadlineAtMs, grpcDeadlineAtMs);
    }
    scanStats.deadlineAtMs = deadlineAtMs;
  }

  void runMaintenanceOnce(long maxMillis) {
    maintenance().runMaintenanceOnce(maxMillis);
  }

  void runLeaseMaintenanceOnce(long maxMillis) {
    maintenance().runLeaseMaintenanceOnce(maxMillis);
  }

  void runProjectionMaintenanceOnce(long maxMillis) {
    maintenance().runProjectionMaintenanceOnce(maxMillis);
  }

  @Override
  public boolean renewLease(String jobId, String leaseEpoch) {
    var loaded = loadByAnyAccount(jobId);
    if (loaded.isEmpty()) {
      return false;
    }
    return renewLeaseByJobId(loaded.get().record.accountId, jobId, leaseEpoch);
  }

  @Override
  public Optional<LeasedJob> getCompletionLeaseView(
      String jobId, String leaseEpoch, boolean allowExpiredWithinGrace) {
    var loaded = loadByAnyAccount(jobId);
    if (loaded.isEmpty()) {
      return Optional.empty();
    }
    StoredReconcileJob existing = loaded.get().record;
    if (!leaseManager()
        .hasActiveLease(
            jobId,
            leaseEpoch,
            existing,
            "getCompletionLeaseView",
            true,
            true,
            allowExpiredWithinGrace)) {
      return Optional.empty();
    }
    ReconcileJob job = projector().toCanonicalLeaseView(existing);
    return Optional.of(
        new LeasedJob(
            job.jobId,
            job.accountId,
            job.connectorId,
            job.fullRescan,
            job.captureMode,
            job.scope,
            job.executionPolicy,
            leaseEpoch,
            job.pinnedExecutorId,
            job.executorId,
            job.jobKind,
            job.tableTask,
            job.viewTask,
            job.snapshotTask,
            job.fileGroupTask,
            job.parentJobId));
  }

  @Override
  public String persistSnapshotPlanManifest(
      String accountId, String jobId, ReconcileSnapshotTask snapshotTask) {
    ReconcileSnapshotTask effective =
        snapshotTask == null ? ReconcileSnapshotTask.empty() : snapshotTask;
    if (effective.completionMode() != ReconcileSnapshotTask.CompletionMode.FILE_GROUPS
        || !effective.fileGroupPlanRecorded()
        || effective.fileGroupCount() == 0) {
      return "";
    }
    List<ReconcileFileGroupTask> plannedFileGroups = materializeSnapshotPlanFileGroups(effective);
    if (plannedFileGroups.isEmpty()) {
      throw new IllegalStateException(
          "persistSnapshotPlanManifest requires a recorded file-group plan payload");
    }
    String effectiveAccountId = blankToEmpty(accountId);
    if (effectiveAccountId.isBlank()) {
      var loaded = loadByAnyAccount(jobId);
      if (loaded.isEmpty()) {
        throw new IllegalArgumentException("reconcile job not found: " + jobId);
      }
      effectiveAccountId = loaded.get().record.accountId;
    }
    SnapshotPlanBlob payload = snapshotPlanBlob(plannedFileGroups);
    return payloads()
        .writeBlob(
            SnapshotPlanManifestIds.manifestBlobUri(effectiveAccountId, jobId, plannedFileGroups),
            payload,
            "Failed to persist snapshot plan payload");
  }

  @Override
  public boolean adoptSnapshotPlanManifest(
      String jobId,
      String leaseEpoch,
      ReconcileSnapshotTask snapshotTask,
      String manifestUri,
      boolean allowExpiredWithinGrace) {
    return onHotPath(
        () -> {
          ReconcileSnapshotTask effective =
              snapshotTask == null ? ReconcileSnapshotTask.empty() : snapshotTask;
          String effectiveManifestUri = manifestUri == null ? "" : manifestUri.trim();
          LOG.debugf(
              "adoptSnapshotPlanManifest jobId=%s leaseEpoch=%s tableId=%s snapshotId=%d"
                  + " fileGroups=%d manifestUri=%s",
              jobId,
              leaseEpoch,
              effective.tableId(),
              effective.snapshotId(),
              effective.fileGroupCount(),
              effectiveManifestUri);
          var loaded = loadByAnyAccount(jobId);
          if (loaded.isEmpty()) {
            return false;
          }
          List<ReconcileFileGroupTask> materializedFileGroups =
              validateSnapshotPlanManifest(loaded.get().record, effective, effectiveManifestUri);
          ReconcileSnapshotTask projectedSnapshotTask =
              materializedSnapshotPlanTask(effective, effectiveManifestUri, materializedFileGroups);
          final boolean[] alreadyAdopted = {false};
          Optional<StoredEnvelope> updated =
              mutateByJobIdReturningRecord(
                  jobId,
                  existing -> {
                    if (!leaseManager()
                        .hasActiveLease(
                            jobId,
                            leaseEpoch,
                            existing,
                            "adoptSnapshotPlanManifest",
                            true,
                            true,
                            allowExpiredWithinGrace)) {
                      return null;
                    }
                    if (ReconcileJobKind.PLAN_SNAPSHOT != existing.jobKind()) {
                      throw new IllegalArgumentException(
                          "adoptSnapshotPlanManifest requires a PLAN_SNAPSHOT job");
                    }
                    validateSnapshotPlanCanonicalIdentity(existing, effective);
                    if (snapshotPlanMatches(existing, effective, effectiveManifestUri)) {
                      alreadyAdopted[0] = true;
                      return null;
                    }
                    existing.snapshotTaskTableId = blankToEmpty(effective.tableId());
                    existing.snapshotTaskSnapshotId = effective.snapshotId();
                    existing.snapshotTaskSourceNamespace =
                        blankToEmpty(effective.sourceNamespace());
                    existing.snapshotTaskSourceTable = blankToEmpty(effective.sourceTable());
                    existing.snapshotTaskFileGroupPlanRecorded = effective.fileGroupPlanRecorded();
                    existing.snapshotTaskCompletionMode = effective.completionMode().name();
                    existing.snapshotTaskSourceFileCount = effective.sourceFileCount();
                    existing.snapshotTaskDirectStatsBlobUri =
                        blankToEmpty(effective.directStatsBlobUri());
                    existing.snapshotTaskDirectStatsRecordCount =
                        effective.directStatsRecordCount();
                    existing.snapshotTaskDirectStatsPersistedRecordCountsByChunk =
                        effective.directStatsPersistedRecordCountsByChunk();
                    existing.snapshotPlanBlobUri = effectiveManifestUri;
                    JobProjection projection =
                        projector().projectSnapshotPlan(projectedSnapshotTask);
                    existing.indexesProcessed = projection.indexesProcessed();
                    existing.plannedFileGroups = projection.plannedFileGroups();
                    existing.plannedFiles = projection.plannedFiles();
                    existing.completedFileGroups = projection.completedFileGroups();
                    existing.failedFileGroups = projection.failedFileGroups();
                    existing.completedFiles = projection.completedFiles();
                    existing.failedFiles = projection.failedFiles();
                    return existing;
                  });
          if (updated.isEmpty()) {
            return alreadyAdopted[0];
          }
          return true;
        });
  }

  private List<ReconcileFileGroupTask> materializeSnapshotPlanFileGroups(
      ReconcileSnapshotTask snapshotTask) {
    ReconcileSnapshotTask effective =
        snapshotTask == null ? ReconcileSnapshotTask.empty() : snapshotTask;
    if (!effective.fileGroups().isEmpty()) {
      return effective.fileGroups();
    }
    if (!effective.fileGroupPlanRecorded()
        || effective.fileGroupCount() <= 0
        || blank(effective.fileGroupPlanBlobUri())) {
      return List.of();
    }
    return payloads()
        .requireBlob(
            effective.fileGroupPlanBlobUri(), SnapshotPlanBlob.class, "snapshot plan payload", "")
        .fileGroups();
  }

  private List<ReconcileFileGroupTask> validateSnapshotPlanManifest(
      StoredReconcileJob currentState, ReconcileSnapshotTask snapshotTask, String manifestUri) {
    ReconcileSnapshotTask effective =
        snapshotTask == null ? ReconcileSnapshotTask.empty() : snapshotTask;
    if (effective.completionMode() != ReconcileSnapshotTask.CompletionMode.FILE_GROUPS) {
      if (!manifestUri.isBlank()) {
        throw new IllegalArgumentException(
            "snapshot plan manifest URI is only valid for FILE_GROUPS completion");
      }
      return List.of();
    }
    if (!effective.fileGroupPlanRecorded()) {
      throw new IllegalArgumentException(
          "snapshot plan adoption requires explicit file-group plan coverage metadata");
    }
    if (effective.fileGroupCount() == 0) {
      if (manifestUri.isBlank()) {
        return List.of();
      }
      SnapshotPlanBlob payload =
          payloads()
              .requireBlob(
                  manifestUri, SnapshotPlanBlob.class, "snapshot plan payload", currentState.jobId);
      validateSnapshotPlanManifestHash(manifestUri, payload.fileGroups());
      if (!payload.fileGroups().isEmpty()) {
        throw new IllegalArgumentException(
            "snapshot plan manifest file-group count mismatch expected=0 actual="
                + payload.fileGroups().size());
      }
      return payload.fileGroups();
    }
    if (manifestUri.isBlank()) {
      throw new IllegalArgumentException(
          "snapshot plan adoption requires a pre-persisted manifest URI");
    }
    SnapshotPlanBlob payload =
        payloads()
            .requireBlob(
                manifestUri, SnapshotPlanBlob.class, "snapshot plan payload", currentState.jobId);
    List<ReconcileFileGroupTask> plannedFileGroups = payload.fileGroups();
    validateSnapshotPlanManifestHash(manifestUri, plannedFileGroups);
    if (plannedFileGroups.size() != effective.fileGroupCount()) {
      throw new IllegalArgumentException(
          "snapshot plan manifest file-group count mismatch expected="
              + effective.fileGroupCount()
              + " actual="
              + plannedFileGroups.size());
    }
    for (ReconcileFileGroupTask fileGroup : plannedFileGroups) {
      if (fileGroup == null || fileGroup.isEmpty()) {
        throw new IllegalArgumentException("snapshot plan manifest contained an empty file group");
      }
      if (!blankToEmpty(effective.tableId()).equals(blankToEmpty(fileGroup.tableId()))) {
        throw new IllegalArgumentException(
            "snapshot plan manifest tableId mismatch expected="
                + effective.tableId()
                + " actual="
                + fileGroup.tableId());
      }
      if (effective.snapshotId() != fileGroup.snapshotId()) {
        throw new IllegalArgumentException(
            "snapshot plan manifest snapshotId mismatch expected="
                + effective.snapshotId()
                + " actual="
                + fileGroup.snapshotId());
      }
    }
    return plannedFileGroups;
  }

  private void validateSnapshotPlanCanonicalIdentity(
      StoredReconcileJob currentState, ReconcileSnapshotTask snapshotTask) {
    ReconcileSnapshotTask effective =
        snapshotTask == null ? ReconcileSnapshotTask.empty() : snapshotTask;
    if (!blank(currentState.snapshotTaskTableId)
        && !blankToEmpty(currentState.snapshotTaskTableId)
            .equals(blankToEmpty(effective.tableId()))) {
      throw new IllegalArgumentException(
          "snapshot plan adoption tableId mismatch expected="
              + currentState.snapshotTaskTableId
              + " actual="
              + effective.tableId());
    }
    if (currentState.snapshotTaskSnapshotId >= 0L
        && currentState.snapshotTaskSnapshotId != effective.snapshotId()) {
      throw new IllegalArgumentException(
          "snapshot plan adoption snapshotId mismatch expected="
              + currentState.snapshotTaskSnapshotId
              + " actual="
              + effective.snapshotId());
    }
    if (!blank(currentState.snapshotTaskSourceNamespace)
        && !blankToEmpty(currentState.snapshotTaskSourceNamespace)
            .equals(blankToEmpty(effective.sourceNamespace()))) {
      throw new IllegalArgumentException(
          "snapshot plan adoption sourceNamespace mismatch expected="
              + currentState.snapshotTaskSourceNamespace
              + " actual="
              + effective.sourceNamespace());
    }
    if (!blank(currentState.snapshotTaskSourceTable)
        && !blankToEmpty(currentState.snapshotTaskSourceTable)
            .equals(blankToEmpty(effective.sourceTable()))) {
      throw new IllegalArgumentException(
          "snapshot plan adoption sourceTable mismatch expected="
              + currentState.snapshotTaskSourceTable
              + " actual="
              + effective.sourceTable());
    }
  }

  private boolean snapshotPlanMatches(
      StoredReconcileJob currentState, ReconcileSnapshotTask snapshotTask, String manifestUri) {
    ReconcileSnapshotTask effective =
        snapshotTask == null ? ReconcileSnapshotTask.empty() : snapshotTask;
    return blankToEmpty(currentState.snapshotTaskTableId).equals(blankToEmpty(effective.tableId()))
        && currentState.snapshotTaskSnapshotId == effective.snapshotId()
        && blankToEmpty(currentState.snapshotTaskSourceNamespace)
            .equals(blankToEmpty(effective.sourceNamespace()))
        && blankToEmpty(currentState.snapshotTaskSourceTable)
            .equals(blankToEmpty(effective.sourceTable()))
        && currentState.snapshotTaskFileGroupPlanRecorded == effective.fileGroupPlanRecorded()
        && blankToEmpty(currentState.snapshotTaskCompletionMode)
            .equals(effective.completionMode().name())
        && currentState.snapshotTaskSourceFileCount == effective.sourceFileCount()
        && blankToEmpty(currentState.snapshotTaskDirectStatsBlobUri)
            .equals(blankToEmpty(effective.directStatsBlobUri()))
        && currentState.snapshotTaskDirectStatsRecordCount == effective.directStatsRecordCount()
        && java.util.Objects.equals(
            currentState.snapshotTaskDirectStatsPersistedRecordCountsByChunk,
            effective.directStatsPersistedRecordCountsByChunk())
        && blankToEmpty(currentState.snapshotPlanBlobUri).equals(blankToEmpty(manifestUri));
  }

  private void validateSnapshotPlanManifestHash(
      String manifestUri, List<ReconcileFileGroupTask> plannedFileGroups) {
    String effectiveManifestUri = blankToEmpty(manifestUri);
    if (effectiveManifestUri.isBlank()) {
      return;
    }
    String expectedManifestUri =
        SnapshotPlanManifestIds.manifestBlobUri(
            "ignored-account", "ignored-job", plannedFileGroups);
    String expectedFilename =
        expectedManifestUri.substring(expectedManifestUri.lastIndexOf('/') + 1);
    if (!effectiveManifestUri.endsWith("/" + expectedFilename)
        && !effectiveManifestUri.endsWith(expectedFilename)) {
      throw new IllegalArgumentException(
          "snapshot plan manifest URI hash mismatch expectedSuffix="
              + expectedFilename.substring(0, expectedFilename.length() - ".json".length())
              + " actualUri="
              + effectiveManifestUri);
    }
  }

  private ReconcileSnapshotTask materializedSnapshotPlanTask(
      ReconcileSnapshotTask snapshotTask,
      String manifestUri,
      List<ReconcileFileGroupTask> materializedFileGroups) {
    ReconcileSnapshotTask effective =
        snapshotTask == null ? ReconcileSnapshotTask.empty() : snapshotTask;
    List<ReconcileFileGroupTask> effectiveFileGroups =
        materializedFileGroups == null ? List.of() : materializedFileGroups;
    if (effectiveFileGroups.isEmpty() && !effective.fileGroups().isEmpty()) {
      effectiveFileGroups = effective.fileGroups();
    }
    int adoptedFileGroupCount = effectiveFileGroups.size();
    return ReconcileSnapshotTask.of(
        effective.tableId(),
        effective.snapshotId(),
        effective.sourceNamespace(),
        effective.sourceTable(),
        effectiveFileGroups,
        effective.fileGroupPlanRecorded(),
        effective.completionMode(),
        manifestUri == null ? "" : manifestUri.trim(),
        adoptedFileGroupCount,
        effective.sourceFileCount(),
        effective.directStatsBlobUri(),
        effective.directStatsRecordCount(),
        effective.directStatsPersistedRecordCountsByChunk());
  }

  @Override
  public void persistFileGroupResult(
      String jobId, String leaseEpoch, ReconcileFileGroupTask fileGroupTask) {
    onHotPath(
        () -> {
          ReconcileFileGroupTask effective =
              fileGroupTask == null ? ReconcileFileGroupTask.empty() : fileGroupTask;
          var loaded = loadByAnyAccount(jobId);
          if (loaded.isEmpty()) {
            return;
          }
          validateFileGroupResultMatchesCanonical(loaded.get().record, effective);
          String blobUri =
              writeFileGroupResultPayloadBlobReference(
                  loaded.get().record.accountId, loaded.get().record.jobId, effective);
          try {
            Optional<StoredEnvelope> updated =
                mutateByJobIdReturningRecord(
                    loaded.get().record.jobId,
                    existing -> {
                      if (existing == null
                          || !"JS_RUNNING".equals(existing.state)
                          || !leaseManager()
                              .hasActiveLease(
                                  jobId,
                                  leaseEpoch,
                                  existing,
                                  "persistFileGroupResult",
                                  false,
                                  true,
                                  false)) {
                        return null;
                      }
                      var projection = projector().projectExecFileGroup(effective, existing.state);
                      existing.fileGroupResultBlobUri = blobUri;
                      existing.indexesProcessed = projection.indexesProcessed();
                      existing.plannedFileGroups = projection.plannedFileGroups();
                      existing.plannedFiles = projection.plannedFiles();
                      existing.completedFileGroups = projection.completedFileGroups();
                      existing.failedFileGroups = projection.failedFileGroups();
                      existing.completedFiles = projection.completedFiles();
                      existing.failedFiles = projection.failedFiles();
                      return existing;
                    });
            if (updated.isEmpty()) {
              blobStore.delete(blobUri);
              throw new IllegalStateException("Failed to update reconcile job result reference");
            }
          } catch (RuntimeException e) {
            blobStore.delete(blobUri);
            throw e;
          }
        });
  }

  @Override
  public void persistSnapshotFinalizeDirectStatsProgress(
      String jobId,
      String leaseEpoch,
      boolean fullRescan,
      int chunkIndex,
      int directStatsPersistedRecordCount) {
    onHotPath(
        () -> {
          Optional<StoredEnvelope> updated =
              mutateByJobIdReturningRecord(
                  jobId,
                  existing -> {
                    if (existing == null
                        || !"JS_RUNNING".equals(existing.state)
                        || !leaseManager()
                            .hasActiveLease(
                                jobId,
                                leaseEpoch,
                                existing,
                                "persistSnapshotFinalizeDirectStatsProgress",
                                false,
                                true,
                                false)) {
                      return null;
                    }
                    if (ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE != existing.jobKind()) {
                      throw new IllegalArgumentException(
                          "persistSnapshotFinalizeDirectStatsProgress requires a"
                              + " FINALIZE_SNAPSHOT_CAPTURE job");
                    }
                    java.util.Map<Integer, Integer> persistedCountsByChunk =
                        (fullRescan && Math.max(0, chunkIndex) == 0)
                            ? new java.util.LinkedHashMap<>()
                            : existing.snapshotTaskDirectStatsPersistedRecordCountsByChunk == null
                                ? new java.util.LinkedHashMap<>()
                                : new java.util.LinkedHashMap<>(
                                    existing.snapshotTaskDirectStatsPersistedRecordCountsByChunk);
                    int normalizedChunkIndex = Math.max(0, chunkIndex);
                    int normalizedPersistedRecordCount =
                        Math.max(0, directStatsPersistedRecordCount);
                    if (normalizedPersistedRecordCount == 0) {
                      persistedCountsByChunk.remove(normalizedChunkIndex);
                    } else {
                      persistedCountsByChunk.put(
                          normalizedChunkIndex, normalizedPersistedRecordCount);
                    }
                    existing.snapshotTaskDirectStatsPersistedRecordCountsByChunk =
                        persistedCountsByChunk.isEmpty()
                            ? java.util.Map.of()
                            : java.util.Map.copyOf(persistedCountsByChunk);
                    return existing;
                  });
          if (updated.isEmpty()) {
            throw new IllegalStateException(
                "Failed to persist snapshot finalize direct stats progress");
          }
        });
  }

  @Override
  public void markRunning(String jobId, String leaseEpoch, long startedAtMs, String executorId) {
    onHotPath(() -> completer().markRunning(jobId, leaseEpoch, startedAtMs, executorId));
  }

  @Override
  public void markProgress(
      String jobId,
      String leaseEpoch,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed,
      String message) {
    onHotPath(
        () ->
            completer()
                .markProgress(
                    jobId,
                    leaseEpoch,
                    tablesScanned,
                    tablesChanged,
                    viewsScanned,
                    viewsChanged,
                    errors,
                    snapshotsProcessed,
                    statsProcessed,
                    message));
  }

  @Override
  public void markSucceeded(
      String jobId,
      String leaseEpoch,
      long finishedAtMs,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long snapshotsProcessed,
      long statsProcessed) {
    applyLeaseOutcomeInternal(
        jobId,
        leaseEpoch,
        CompletionKind.SUCCEEDED,
        finishedAtMs,
        "",
        tablesScanned,
        tablesChanged,
        viewsScanned,
        viewsChanged,
        0L,
        snapshotsProcessed,
        statsProcessed);
    recordFinalizedSnapshotIfEligible(jobId);
    enqueueSnapshotFinalizationIfEligible(jobId);
  }

  @Override
  public void markFailed(
      String jobId,
      String leaseEpoch,
      long finishedAtMs,
      String message,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed) {
    applyLeaseOutcomeInternal(
        jobId,
        leaseEpoch,
        CompletionKind.FAILED_RETRYABLE,
        finishedAtMs,
        message,
        tablesScanned,
        tablesChanged,
        viewsScanned,
        viewsChanged,
        errors,
        snapshotsProcessed,
        statsProcessed);
  }

  @Override
  public void markWaiting(
      String jobId,
      String leaseEpoch,
      long finishedAtMs,
      WaitingReason waitingReason,
      String message,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed) {
    applyLeaseOutcomeInternal(
        jobId,
        leaseEpoch,
        waitingReason == WaitingReason.CHILD_WORK_FINALIZED
            ? CompletionKind.SUCCEEDED_WAITING
            : CompletionKind.FAILED_WAITING_ON_DEPENDENCY,
        finishedAtMs,
        message,
        tablesScanned,
        tablesChanged,
        viewsScanned,
        viewsChanged,
        errors,
        snapshotsProcessed,
        statsProcessed);
  }

  @Override
  public void markFailedTerminal(
      String jobId,
      String leaseEpoch,
      long finishedAtMs,
      String message,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed) {
    applyLeaseOutcomeInternal(
        jobId,
        leaseEpoch,
        CompletionKind.FAILED_TERMINAL,
        finishedAtMs,
        message,
        tablesScanned,
        tablesChanged,
        viewsScanned,
        viewsChanged,
        errors,
        snapshotsProcessed,
        statsProcessed);
  }

  @Override
  public Optional<ReconcileJob> cancel(String accountId, String jobId, String reason) {
    return cancellation().cancel(accountId, jobId, reason);
  }

  @Override
  public boolean isCancellationRequested(String jobId) {
    if (cancellation().isCancellationRequested(jobId)) {
      return true;
    }
    StoredEnvelope loaded = loadByAnyAccount(jobId).orElse(null);
    return loaded != null && isBlockedByAncestorCancellation(loaded.record);
  }

  @Override
  public void markCancelled(
      String jobId,
      String leaseEpoch,
      long finishedAtMs,
      String message,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed) {
    applyLeaseOutcomeInternal(
        jobId,
        leaseEpoch,
        CompletionKind.CANCELLED,
        finishedAtMs,
        message,
        tablesScanned,
        tablesChanged,
        viewsScanned,
        viewsChanged,
        errors,
        snapshotsProcessed,
        statsProcessed);
  }

  private void clearExecutionLeasesIfOwned(StoredEnvelope env, String jobId, String leaseEpoch) {
    if (env == null || env.record == null) {
      return;
    }
    leaseManager().clearLeaseIfEpochMatches(env.record.accountId, jobId, leaseEpoch);
    leaseManager().clearLaneLeaseIfOwned(env.record, env.canonicalPointerKey);
    leaseManager().clearSnapshotLeaseIfOwned(env.record, env.canonicalPointerKey);
  }

  private void onHotPath(Runnable runnable) {
    runnable.run();
  }

  private <T> T onHotPath(Supplier<T> supplier) {
    return supplier.get();
  }

  private Optional<StoredEnvelope> loadByAnyAccount(String jobId) {
    return jobIndexStore()
        .loadByAnyAccount(jobId)
        .map(env -> new StoredEnvelope(env.canonicalPointerKey(), env.record()));
  }

  private Optional<StoredEnvelope> mutateByJobIdReturningRecord(
      String jobId, UnaryOperator<StoredReconcileJob> mutator) {
    Optional<StoredEnvelope> updated =
        jobIndexStore()
            .mutateByJobIdReturningRecord(jobId, mutator)
            .map(env -> new StoredEnvelope(env.canonicalPointerKey(), env.record()));
    updated.ifPresent(
        env -> {
          markDirtyParentForRecord(env.record);
          upsertRootSummaryForRecord(env.record);
        });
    return updated;
  }

  private Optional<StoredEnvelope> mutateByCanonicalPointerReturningRecord(
      String canonicalPointerKey, UnaryOperator<StoredReconcileJob> mutator) {
    Optional<StoredEnvelope> updated =
        jobIndexStore()
            .mutateByCanonicalPointerReturningRecord(canonicalPointerKey, mutator)
            .map(env -> new StoredEnvelope(env.canonicalPointerKey(), env.record()));
    updated.ifPresent(
        env -> {
          markDirtyParentForRecord(env.record);
          upsertRootSummaryForRecord(env.record);
        });
    return updated;
  }

  private boolean commitCanonicalMutationsWithMaintenance(
      List<ReconcileJobIndexStore.CanonicalRecordMutation> mutations,
      List<StoredReconcileJob> changedRecords) {
    if (!jobIndexStore().compareAndSetCanonicalMutations(mutations)) {
      return false;
    }
    for (StoredReconcileJob changedRecord : changedRecords) {
      markDirtyParentForRecord(changedRecord);
      upsertRootSummaryForRecord(changedRecord);
    }
    return true;
  }

  private boolean renewLeaseByJobId(String accountId, String jobId, String leaseEpoch) {
    if (leaseEpoch == null || leaseEpoch.isBlank()) {
      logLeaseSkip(
          "renewLease",
          "Skipping renewLease for reconcile job %s due to missing lease epoch",
          jobId);
      return false;
    }
    StoredJobLease current = leaseManager().loadLease(accountId, jobId).orElse(null);
    if (current == null || current.epoch == null || current.epoch.isBlank()) {
      logLeaseSkip(
          "renewLease", "Skipping renewLease for reconcile job %s due to missing lease", jobId);
      return false;
    }
    if (!leaseEpoch.equals(current.epoch)) {
      logLeaseSkip(
          "renewLease",
          "Skipping renewLease for reconcile job %s due to stale lease epoch=%s",
          jobId,
          current.epoch);
      return false;
    }
    StoredJobLease renewed =
        leaseManager().renewLeaseIfEpochMatches(accountId, jobId, leaseEpoch).orElse(null);
    return renewed != null;
  }

  private void reclaimExpiredLease(
      ReconcileLeaseStore.LeaseExpiryEntry leaseExpiryEntry, long nowMs) {
    leaseManager().reclaimExpiredLease(leaseExpiryEntry, nowMs);
  }

  private void refreshProjectedParent(String accountId, String parentJobId) {
    if (blankToEmpty(accountId).isBlank() || blankToEmpty(parentJobId).isBlank()) {
      return;
    }
    var loaded = loadByAnyAccount(parentJobId);
    if (loaded.isEmpty()) {
      projections().delete(accountId, parentJobId);
      return;
    }
    StoredReconcileJob parent = loaded.get().record;
    if (!accountId.equals(parent.accountId) || !isParentCapable(parent.jobKind())) {
      return;
    }
    StoredReconcileJob rollupParent = parent;
    List<StoredReconcileJob> directChildren =
        listAllStoredChildJobs(accountId, parentJobId).stream()
            .map(child -> projectedSummaryRecordForParentRollup(rollupParent, child, true, true))
            .toList();
    long requestedGeneration = Math.max(0L, parent.projectionRequestedGeneration);
    long previousAppliedGeneration = Math.max(0L, parent.projectionAppliedGeneration);
    var nextProjection = ancestorRollups().recomputeParentProjection(parent, directChildren);
    if (nextProjection == null) {
      return;
    }
    projections().upsert(nextProjection);
    if (blankToEmpty(parent.parentJobId).isBlank()) {
      rootSummaries().upsert(toRootListSummary(parent, nextProjection));
    }
    advanceAppliedProjectionGeneration(accountId, parentJobId, requestedGeneration);
    if (requestedGeneration > previousAppliedGeneration) {
      markDirtyParent(parent.accountId, parent.parentJobId);
    }
  }

  private List<StoredReconcileJob> listAllStoredChildJobs(String accountId, String parentJobId) {
    List<StoredReconcileJob> children = new java.util.ArrayList<>();
    String token = "";
    while (true) {
      ReconcileJobIndexStore.StoredJobPage page =
          jobIndexStore().listStoredChildJobs(accountId, parentJobId, 200, token);
      children.addAll(page.records());
      String nextToken = blankToEmpty(page.nextPageToken());
      if (nextToken.isBlank() || nextToken.equals(token)) {
        return children;
      }
      token = nextToken;
    }
  }

  private void markDirtyParentForRecord(StoredReconcileJob record) {
    if (record == null) {
      return;
    }
    if (isParentCapable(record.jobKind())) {
      markDirtyParent(record.accountId, record.jobId);
    }
    markDirtyParent(record.accountId, record.parentJobId);
  }

  private void upsertRootSummaryByJobId(String jobId) {
    if (blankToEmpty(jobId).isBlank()) {
      return;
    }
    loadByAnyAccount(jobId).ifPresent(env -> upsertRootSummaryForRecord(env.record));
  }

  private void upsertRootSummaryForRecord(StoredReconcileJob record) {
    if (record == null || !blankToEmpty(record.parentJobId).isBlank()) {
      return;
    }
    rootSummaries().upsert(toRootListSummary(record, storedProjectionForRead(record)));
  }

  private StoredReconcileJob applyCanonicalParentSchedulingState(
      StoredReconcileJob existing,
      ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobProjection rollup) {
    if (existing == null || rollup == null || !isParentCapable(existing.jobKind())) {
      return existing;
    }
    existing.state = rollup.state();
    existing.message = rollup.message();
    existing.startedAtMs = projectedStartedAtMs(existing, rollup);
    existing.finishedAtMs = projectedFinishedAtMs(existing, rollup);
    existing.executorId = rollup.executorId();
    existing.readyPointerKey = null;
    existing.nextAttemptAtMs = 0L;
    existing.childrenFinalized =
        "JS_WAITING".equals(blankToEmpty(rollup.state())) || isTerminalState(rollup.state());
    return existing;
  }

  private Optional<
          ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobProjection>
      projectionFor(StoredReconcileJob record) {
    if (record == null || !isParentCapable(record.jobKind())) {
      return Optional.empty();
    }
    return projections().load(record.accountId, record.jobId);
  }

  private ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobProjection
      freshProjectionForSummary(StoredReconcileJob record) {
    return freshProjection(record, true);
  }

  private ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobProjection
      storedProjectionForRead(StoredReconcileJob record) {
    return projectionFor(record).orElse(null);
  }

  private ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobProjection
      freshProjection(StoredReconcileJob record, boolean persistProjection) {
    if (record == null || !isParentCapable(record.jobKind())) {
      return null;
    }
    var storedProjection = projectionFor(record).orElse(null);
    if (!projectionNeedsRefresh(record, storedProjection)) {
      return storedProjection;
    }
    var refreshedProjection = recomputeSummaryProjection(record, true, persistProjection);
    if (persistProjection && refreshedProjection != null) {
      projections().upsert(refreshedProjection);
    }
    return refreshedProjection;
  }

  private StoredReconcileJob projectedSummaryRecord(StoredReconcileJob record) {
    return projectedSummaryRecord(record, false);
  }

  private StoredReconcileJob projectedSummaryRecordForRefresh(StoredReconcileJob record) {
    return projectedSummaryRecord(record, true, true);
  }

  private StoredReconcileJob projectedSummaryRecord(
      StoredReconcileJob record, boolean refreshStaleProjection) {
    return projectedSummaryRecord(record, refreshStaleProjection, true);
  }

  private StoredReconcileJob projectedSummaryRecord(
      StoredReconcileJob record, boolean refreshStaleProjection, boolean persistProjection) {
    if (record == null || !isParentCapable(record.jobKind())) {
      return record;
    }
    var projection = projections().load(record.accountId, record.jobId).orElse(null);
    if (refreshStaleProjection && projectionNeedsRefresh(record, projection)) {
      var refreshedProjection = recomputeSummaryProjection(record, true, persistProjection);
      if (persistProjection && refreshedProjection != null) {
        projections().upsert(refreshedProjection);
      }
      projection = refreshedProjection;
    }
    if (projection == null) {
      return record;
    }
    if (refreshStaleProjection
        && shouldKeepCanonicalStateForTerminalProjection(record, projection)) {
      var refreshedProjection = recomputeSummaryProjection(record, true, persistProjection);
      if (refreshedProjection != null) {
        if (persistProjection && !Objects.equals(projection, refreshedProjection)) {
          projections().upsert(refreshedProjection);
        }
        projection = refreshedProjection;
      }
    }
    if (shouldKeepCanonicalStateForTerminalProjection(record, projection)) {
      return copyProjectedAggregateSummaryRecord(record, projection);
    }
    if (shouldKeepProjectionAggregatesForTerminalCanonicalState(record, projection)) {
      return copyProjectedAggregateSummaryRecord(record, projection);
    }
    if (!shouldUseProjectedSummaryRecord(record, projection)) {
      return record;
    }
    return copyProjectedSummaryRecord(record, projection);
  }

  private boolean shouldUseProjectedSummaryRecord(
      StoredReconcileJob record,
      ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobProjection
          projection) {
    String canonicalState = blankToEmpty(record == null ? "" : record.state);
    String projectionState = blankToEmpty(projection == null ? "" : projection.state());
    if ("JS_WAITING".equals(canonicalState) || "JS_SUCCEEDED".equals(canonicalState)) {
      return true;
    }
    if (!projectionState.equals(canonicalState)) {
      if ("JS_QUEUED".equals(projectionState) && !projectionHasAggregateSignal(projection)) {
        return false;
      }
      return true;
    }
    return projectionHasAggregateSignal(projection);
  }

  private boolean projectionNeedsRefresh(
      StoredReconcileJob record,
      ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobProjection
          projection) {
    if (record == null || !isParentCapable(record.jobKind())) {
      return false;
    }
    if (projection == null) {
      return true;
    }
    long requestedGeneration = Math.max(0L, record.projectionRequestedGeneration);
    long appliedGeneration = Math.max(0L, projection.appliedGeneration());
    if (requestedGeneration > appliedGeneration) {
      return true;
    }
    return shouldKeepCanonicalStateForTerminalProjection(record, projection);
  }

  private boolean shouldKeepCanonicalStateForTerminalProjection(
      StoredReconcileJob record,
      ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobProjection
          projection) {
    String canonicalState = blankToEmpty(record == null ? "" : record.state);
    String projectionState = blankToEmpty(projection == null ? "" : projection.state());
    return !isTerminalState(canonicalState)
        && isTerminalState(projectionState)
        && hasActiveCanonicalSubtree(record);
  }

  private boolean shouldKeepProjectionAggregatesForTerminalCanonicalState(
      StoredReconcileJob record,
      ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobProjection
          projection) {
    String canonicalState = blankToEmpty(record == null ? "" : record.state);
    String projectionState = blankToEmpty(projection == null ? "" : projection.state());
    return isTerminalState(canonicalState)
        && !projectionState.isBlank()
        && !canonicalState.equals(projectionState);
  }

  private boolean hasActiveCanonicalSubtree(StoredReconcileJob record) {
    if (record == null) {
      return false;
    }
    String state = blankToEmpty(record.state);
    if (isTerminalState(state)) {
      return false;
    }
    if (!isParentCapable(record.jobKind())) {
      return true;
    }
    List<StoredReconcileJob> children = listAllStoredChildJobs(record.accountId, record.jobId);
    if (children.isEmpty()) {
      return true;
    }
    for (StoredReconcileJob child : children) {
      if (hasActiveCanonicalSubtree(child)) {
        return true;
      }
    }
    return false;
  }

  private StoredReconcileJob copyProjectedSummaryRecord(
      StoredReconcileJob record,
      ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobProjection
          projection) {
    StoredReconcileJob copy = copyProjectedAggregateSummaryRecord(record, projection);
    copy.state = projection.state();
    copy.message = projection.message();
    copy.finishedAtMs = projectedFinishedAtMs(record, projection);
    return copy;
  }

  private StoredReconcileJob copyProjectedAggregateSummaryRecord(
      StoredReconcileJob record,
      ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobProjection
          projection) {
    StoredReconcileJob copy = jobIndexStore().cloneStoredRecord(record);
    copy.startedAtMs = projectedStartedAtMs(record, projection);
    copy.tablesScanned = projection.tablesScanned();
    copy.tablesChanged = projection.tablesChanged();
    copy.viewsScanned = projection.viewsScanned();
    copy.viewsChanged = projection.viewsChanged();
    copy.errors = projection.errors();
    copy.snapshotsProcessed = projection.snapshotsProcessed();
    copy.statsProcessed = projection.statsProcessed();
    copy.indexesProcessed = projection.indexesProcessed();
    copy.plannedFileGroups = projection.plannedFileGroups();
    copy.plannedFiles = projection.plannedFiles();
    copy.completedFileGroups = projection.completedFileGroups();
    copy.failedFileGroups = projection.failedFileGroups();
    copy.completedFiles = projection.completedFiles();
    copy.failedFiles = projection.failedFiles();
    copy.executorId = projection.executorId();
    return copy;
  }

  private ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobProjection
      recomputeSummaryProjection(StoredReconcileJob record) {
    return recomputeSummaryProjection(record, false);
  }

  private ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobProjection
      recomputeSummaryProjection(StoredReconcileJob record, boolean refreshStaleDescendants) {
    return recomputeSummaryProjection(record, refreshStaleDescendants, true);
  }

  private ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobProjection
      recomputeSummaryProjection(
          StoredReconcileJob record, boolean refreshStaleDescendants, boolean persistProjection) {
    if (record == null || !isParentCapable(record.jobKind())) {
      return projectionFor(record).orElse(null);
    }
    List<StoredReconcileJob> directChildren =
        listAllStoredChildJobs(record.accountId, record.jobId).stream()
            .map(
                child ->
                    projectedSummaryRecordForParentRollup(
                        record, child, refreshStaleDescendants, persistProjection))
            .toList();
    return ancestorRollups().recomputeParentProjection(record, directChildren);
  }

  private StoredReconcileJob projectedSummaryRecordForParentRollup(
      StoredReconcileJob parent,
      StoredReconcileJob child,
      boolean refreshStaleProjection,
      boolean persistProjection) {
    StoredReconcileJob summary =
        refreshStaleProjection
            ? projectedSummaryRecord(child, true, persistProjection)
            : projectedSummaryRecord(child);
    if (parent == null
        || child == null
        || summary == null
        || parent.jobKind() != ReconcileJobKind.PLAN_CONNECTOR
        || child.jobKind() != ReconcileJobKind.PLAN_TABLE) {
      return summary;
    }
    StoredReconcileJob copy = jobIndexStore().cloneStoredRecord(summary);
    copy.tablesScanned = Math.max(0L, child.tablesScanned);
    copy.tablesChanged = Math.max(0L, child.tablesChanged);
    return copy;
  }

  private long projectedStartedAtMs(
      StoredReconcileJob record,
      ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobProjection
          projection) {
    long canonical = record == null ? 0L : Math.max(0L, record.startedAtMs);
    long projected = projection == null ? 0L : Math.max(0L, projection.startedAtMs());
    if (canonical <= 0L) {
      return projected;
    }
    if (projected <= 0L) {
      return canonical;
    }
    return Math.min(canonical, projected);
  }

  private long projectedFinishedAtMs(
      StoredReconcileJob record,
      ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobProjection
          projection) {
    String state = blankToEmpty(projection == null ? "" : projection.state());
    long canonical = record == null ? 0L : Math.max(0L, record.finishedAtMs);
    long projected = projection == null ? 0L : Math.max(0L, projection.finishedAtMs());
    if (!isTerminalState(state)) {
      return projected;
    }
    return Math.max(canonical, projected);
  }

  private boolean projectionHasAggregateSignal(
      ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobProjection
          projection) {
    return projection.tablesScanned() > 0L
        || projection.tablesChanged() > 0L
        || projection.viewsScanned() > 0L
        || projection.viewsChanged() > 0L
        || projection.errors() > 0L
        || projection.snapshotsProcessed() > 0L
        || projection.statsProcessed() > 0L
        || projection.indexesProcessed() > 0L
        || projection.plannedFileGroups() > 0L
        || projection.plannedFiles() > 0L
        || projection.completedFileGroups() > 0L
        || projection.failedFileGroups() > 0L
        || projection.completedFiles() > 0L
        || projection.failedFiles() > 0L;
  }

  private StoredReconcileJobListSummary toRootListSummary(
      StoredReconcileJob record,
      ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobProjection
          projection) {
    ReconcileJob summary = projector().toPublicJobSummary(record, projection);
    return new StoredReconcileJobListSummary(
        summary.accountId,
        summary.jobId,
        summary.connectorId,
        summary.state,
        summary.message,
        summary.startedAtMs,
        summary.finishedAtMs,
        summary.tablesScanned,
        summary.tablesChanged,
        summary.viewsScanned,
        summary.viewsChanged,
        summary.errors,
        summary.fullRescan,
        summary.captureMode,
        summary.snapshotsProcessed,
        summary.statsProcessed,
        summary.indexesProcessed,
        summary.executorId,
        summary.executionPolicy.executionClass(),
        summary.executionPolicy.lane(),
        summary.executionPolicy.attributes(),
        summary.jobKind,
        summary.plannedFileGroups,
        summary.plannedFiles,
        summary.completedFileGroups,
        summary.failedFileGroups,
        summary.completedFiles,
        summary.failedFiles,
        record.createdAtMs);
  }

  private void markDirtyParent(String accountId, String parentJobId) {
    requestProjectionRefresh(accountId, parentJobId, 0L);
  }

  private void requestProjectionRefresh(
      String accountId, String parentJobId, long expectedDirectChildrenDelta) {
    String effectiveAccountId = blankToEmpty(accountId);
    String effectiveParentJobId = blankToEmpty(parentJobId);
    if (effectiveAccountId.isBlank() || effectiveParentJobId.isBlank()) {
      return;
    }
    String key = Keys.reconcileDirtyParentPointer(effectiveAccountId, effectiveParentJobId);
    String payload = effectiveAccountId + "\n" + effectiveParentJobId;
    var updated =
        jobIndexStore()
            .mutateByJobIdReturningRecord(
                effectiveParentJobId,
                existing -> {
                  if (existing == null
                      || !effectiveAccountId.equals(existing.accountId)
                      || !isParentCapable(existing.jobKind())) {
                    return null;
                  }
                  existing.expectedDirectChildren =
                      Math.max(0L, existing.expectedDirectChildren)
                          + Math.max(0L, expectedDirectChildrenDelta);
                  existing.projectionRequestedGeneration =
                      Math.max(0L, existing.projectionRequestedGeneration) + 1L;
                  return existing;
                });
    if (updated.isEmpty()) {
      throw new IllegalStateException(
          "Failed to advance reconcile parent projection generation accountId="
              + effectiveAccountId
              + " parentJobId="
              + effectiveParentJobId);
    }
    long requestedGenerationAfterAdvance =
        Math.max(0L, updated.get().record().projectionRequestedGeneration);
    long expectedChildrenAfterAdvance = Math.max(0L, updated.get().record().expectedDirectChildren);
    for (int attempt = 0; attempt < CAS_MAX; attempt++) {
      Pointer current = pointerStore.get(key).orElse(null);
      long expectedVersion = current == null ? 0L : current.getVersion();
      Pointer next =
          current == null
              ? PointerReferences.opaqueMarkerPointer(key, payload, 1L)
              : PointerReferences.asOpaqueMarkerPointer(
                      current.toBuilder().setVersion(current.getVersion() + 1L), payload)
                  .build();
      if (pointerStore.compareAndSet(key, expectedVersion, next)) {
        return;
      }
    }
    rollbackProjectionRefreshRequest(
        effectiveAccountId,
        effectiveParentJobId,
        expectedDirectChildrenDelta,
        requestedGenerationAfterAdvance,
        expectedChildrenAfterAdvance);
    throw new IllegalStateException(
        "Failed to mark reconcile parent dirty accountId="
            + effectiveAccountId
            + " parentJobId="
            + effectiveParentJobId);
  }

  private void rollbackProjectionRefreshRequest(
      String accountId,
      String parentJobId,
      long expectedDirectChildrenDelta,
      long requestedGenerationAfterAdvance,
      long expectedChildrenAfterAdvance) {
    jobIndexStore()
        .mutateByJobIdReturningRecord(
            parentJobId,
            existing -> {
              if (existing == null
                  || !accountId.equals(existing.accountId)
                  || !isParentCapable(existing.jobKind())
                  || Math.max(0L, existing.projectionRequestedGeneration)
                      != Math.max(0L, requestedGenerationAfterAdvance)
                  || Math.max(0L, existing.expectedDirectChildren)
                      != Math.max(0L, expectedChildrenAfterAdvance)) {
                return existing;
              }
              existing.expectedDirectChildren =
                  Math.max(0L, existing.expectedDirectChildren)
                      - Math.max(0L, expectedDirectChildrenDelta);
              existing.projectionRequestedGeneration =
                  Math.max(0L, existing.projectionRequestedGeneration) - 1L;
              return existing;
            });
  }

  private void recordFinalizedSnapshotIfEligible(String jobId) {
    StoredEnvelope loaded = loadByAnyAccount(jobId).orElse(null);
    if (loaded == null
        || loaded.record == null
        || !ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE.name().equals(loaded.record.jobKind)
        || !"JS_SUCCEEDED".equals(loaded.record.state)
        || blank(loaded.record.accountId)
        || blank(loaded.record.snapshotTaskTableId)
        || blank(loaded.record.jobId)) {
      return;
    }
    upsertFinalizedSnapshotEvent(
        new StoredFinalizedSnapshotEvent(
            finalizedSnapshotEventId(
                loaded.record.accountId,
                loaded.record.snapshotTaskTableId,
                loaded.record.snapshotTaskSnapshotId),
            loaded.record.accountId,
            loaded.record.snapshotTaskTableId,
            loaded.record.snapshotTaskSnapshotId,
            Math.max(0L, loaded.record.finishedAtMs),
            loaded.record.jobId));
  }

  private void enqueueSnapshotFinalizationIfEligible(String jobId) {
    StoredEnvelope loaded = loadByAnyAccount(jobId).orElse(null);
    if (loaded == null
        || loaded.record == null
        || !ReconcileJobKind.EXEC_FILE_GROUP.name().equals(loaded.record.jobKind)
        || !"JS_SUCCEEDED".equals(loaded.record.state)
        || blank(loaded.record.accountId)
        || blank(loaded.record.parentJobId)) {
      return;
    }
    ReconcileJob parent = get(loaded.record.accountId, loaded.record.parentJobId).orElse(null);
    ReconcileSnapshotTask snapshotTask =
        parent == null || parent.snapshotTask == null
            ? ReconcileSnapshotTask.empty()
            : parent.snapshotTask;
    if (parent == null
        || parent.jobKind != ReconcileJobKind.PLAN_SNAPSHOT
        || snapshotTask.completionMode() != ReconcileSnapshotTask.CompletionMode.FILE_GROUPS
        || !snapshotTask.fileGroupPlanRecorded()
        || snapshotTask.fileGroupCount() <= 0L) {
      return;
    }

    List<ReconcileFileGroupTask> expectedGroups = materializeSnapshotPlanFileGroups(snapshotTask);
    if (expectedGroups.isEmpty()) {
      return;
    }
    java.util.Set<String> expectedKeys = new java.util.LinkedHashSet<>();
    for (ReconcileFileGroupTask expectedGroup : expectedGroups) {
      String key = execFileGroupKey(expectedGroup);
      if (key.isBlank() || !expectedKeys.add(key)) {
        return;
      }
    }

    java.util.Set<String> succeededKeys = new java.util.LinkedHashSet<>();
    boolean hasLiveFinalizer = false;
    String pageToken = "";
    do {
      ReconcileJobPage page =
          childJobsPage(loaded.record.accountId, loaded.record.parentJobId, 200, pageToken);
      if (page == null || page.jobs == null) {
        break;
      }
      for (ReconcileJob child : page.jobs) {
        if (child == null) {
          continue;
        }
        if (child.jobKind == ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE
            && !"JS_CANCELLED".equals(child.state)
            && !"JS_FAILED".equals(child.state)) {
          hasLiveFinalizer = true;
          continue;
        }
        if (child.jobKind != ReconcileJobKind.EXEC_FILE_GROUP
            || child.fileGroupTask == null
            || child.fileGroupTask.isEmpty()) {
          continue;
        }
        String key = execFileGroupKey(child.fileGroupTask);
        if (key.isBlank() || !expectedKeys.contains(key)) {
          continue;
        }
        if (!succeededKeys.add(key)) {
          LOG.warnf(
              "Snapshot finalization trigger found duplicate EXEC_FILE_GROUP child accountId=%s"
                  + " parentJobId=%s key=%s",
              loaded.record.accountId, loaded.record.parentJobId, key);
          return;
        }
        if (!"JS_SUCCEEDED".equals(child.state)) {
          return;
        }
      }
      pageToken = page.nextPageToken == null ? "" : page.nextPageToken;
    } while (!pageToken.isBlank());

    if (hasLiveFinalizer || !succeededKeys.equals(expectedKeys)) {
      return;
    }

    enqueueSnapshotFinalization(
        parent.accountId,
        parent.connectorId,
        parent.fullRescan,
        parent.captureMode,
        parent.scope,
        snapshotTask,
        parent.executionPolicy,
        parent.jobId,
        "");
  }

  private static String execFileGroupKey(ReconcileFileGroupTask fileGroupTask) {
    if (fileGroupTask == null) {
      return "";
    }
    String planId = blankToEmpty(fileGroupTask.planId());
    String groupId = blankToEmpty(fileGroupTask.groupId());
    if (planId.isBlank() || groupId.isBlank()) {
      return "";
    }
    return planId + "|" + groupId;
  }

  private void resetFinalizedSnapshotIfEligible(BulkEnqueueSpec spec) {
    if (spec == null
        || spec.jobKind != ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE
        || blank(spec.accountId)
        || spec.snapshotTask == null
        || blank(spec.snapshotTask.tableId())) {
      return;
    }
    pointerStore.delete(
        Keys.reconcileFinalizedSnapshotIdentityPointer(
            spec.accountId, spec.snapshotTask.tableId(), spec.snapshotTask.snapshotId()));
  }

  private void upsertFinalizedSnapshotEvent(StoredFinalizedSnapshotEvent event) {
    if (event == null
        || blank(event.accountId)
        || blank(event.tableId)
        || blank(event.finalizerJobId)) {
      return;
    }
    String identityKey =
        Keys.reconcileFinalizedSnapshotIdentityPointer(
            event.accountId, event.tableId, event.snapshotId);
    String blobUri = encodeInlineFinalizedSnapshotEvent(event);
    Pointer created = PointerReferences.inlineJsonPointer(identityKey, blobUri, 1L);
    if (!pointerStore.compareAndSet(identityKey, 0L, created)) {
      return;
    }
  }

  private String encodeInlineFinalizedSnapshotEvent(StoredFinalizedSnapshotEvent event) {
    try {
      return INLINE_FINALIZED_SNAPSHOT_EVENT_PREFIX
          + Base64.getUrlEncoder().withoutPadding().encodeToString(mapper.writeValueAsBytes(event));
    } catch (Exception e) {
      throw new IllegalStateException("Failed to encode finalized snapshot event", e);
    }
  }

  private Optional<StoredFinalizedSnapshotEvent> readFinalizedSnapshotEvent(String reference) {
    if (reference == null
        || reference.isBlank()
        || !reference.startsWith(INLINE_FINALIZED_SNAPSHOT_EVENT_PREFIX)) {
      return Optional.empty();
    }
    try {
      byte[] payload =
          Base64.getUrlDecoder()
              .decode(reference.substring(INLINE_FINALIZED_SNAPSHOT_EVENT_PREFIX.length()));
      return Optional.ofNullable(mapper.readValue(payload, StoredFinalizedSnapshotEvent.class));
    } catch (Exception e) {
      LOG.warnf(e, "Failed to decode finalized snapshot event reference=%s", reference);
      return Optional.empty();
    }
  }

  private String finalizedSnapshotEventId(String accountId, String tableId, long snapshotId) {
    return blankToEmpty(accountId) + ":" + blankToEmpty(tableId) + ":" + Math.max(0L, snapshotId);
  }

  private void advanceAppliedProjectionGeneration(
      String accountId, String parentJobId, long appliedGeneration) {
    String effectiveAccountId = blankToEmpty(accountId);
    String effectiveParentJobId = blankToEmpty(parentJobId);
    if (effectiveAccountId.isBlank() || effectiveParentJobId.isBlank() || appliedGeneration <= 0L) {
      return;
    }
    jobIndexStore()
        .mutateByJobIdReturningRecord(
            effectiveParentJobId,
            existing -> {
              if (existing == null
                  || !effectiveAccountId.equals(existing.accountId)
                  || !isParentCapable(existing.jobKind())) {
                return null;
              }
              existing.projectionAppliedGeneration =
                  Math.max(
                      Math.max(0L, existing.projectionAppliedGeneration),
                      Math.min(
                          Math.max(0L, existing.projectionRequestedGeneration), appliedGeneration));
              return existing;
            });
  }

  private Optional<LeasedJob> leaseReadyDue(
      long nowMs, LeaseRequest request, LeaseScanStats scanStats) {
    return readyQueue().leaseReadyDue(nowMs, request, scanStats);
  }

  private boolean isBlockedByAncestorCancellation(StoredReconcileJob record) {
    if (record == null) {
      return false;
    }
    String accountId = blankToEmpty(record.accountId);
    String parentJobId = blankToEmpty(record.parentJobId);
    java.util.HashSet<String> visited = new java.util.HashSet<>();
    while (!accountId.isBlank() && !parentJobId.isBlank() && visited.add(parentJobId)) {
      StoredEnvelope parent = loadByAnyAccount(parentJobId).orElse(null);
      if (parent == null || parent.record == null || !accountId.equals(parent.record.accountId)) {
        return false;
      }
      if (isCancellationState(parent.record.state)) {
        return true;
      }
      parentJobId = blankToEmpty(parent.record.parentJobId);
    }
    return false;
  }

  private boolean isJobBlockedByAncestorCancellation(String jobId) {
    StoredEnvelope loaded = loadByAnyAccount(jobId).orElse(null);
    return loaded != null && isBlockedByAncestorCancellation(loaded.record);
  }

  private String readyPointerKeyFor(StoredReconcileJob record, long dueAtMs) {
    return readyQueue().readyPointerKeyFor(record, dueAtMs);
  }

  private List<String> readyPointerKeys(StoredReconcileJob record) {
    return readyQueue().readyPointerKeys(record);
  }

  @Override
  public boolean applyLeaseOutcome(
      String jobId,
      String leaseEpoch,
      CompletionKind completionKind,
      long finishedAtMs,
      String message,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed) {
    boolean accepted =
        applyLeaseOutcomeInternal(
            jobId,
            leaseEpoch,
            completionKind,
            finishedAtMs,
            message,
            tablesScanned,
            tablesChanged,
            viewsScanned,
            viewsChanged,
            errors,
            snapshotsProcessed,
            statsProcessed);
    if (accepted && completionKind == CompletionKind.SUCCEEDED) {
      recordFinalizedSnapshotIfEligible(jobId);
      enqueueSnapshotFinalizationIfEligible(jobId);
    }
    return accepted;
  }

  private boolean applyLeaseOutcomeInternal(
      String jobId,
      String leaseEpoch,
      CompletionKind completionKind,
      long finishedAtMs,
      String message,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed) {
    CompletionKind nextCompletionKind = completionKind;
    String nextMessage = message;
    if (isRetryableCompletion(completionKind) && isJobBlockedByAncestorCancellation(jobId)) {
      nextCompletionKind = CompletionKind.CANCELLED;
      nextMessage = blank(message) ? "Cancelled" : message;
    }
    final CompletionKind effectiveCompletionKind = nextCompletionKind;
    final String effectiveMessage = nextMessage;
    String terminalState = terminalStateForCompletionKind(effectiveCompletionKind);
    if (terminalState.isBlank()) {
      return applyLeaseOutcomeDirectly(
          jobId,
          leaseEpoch,
          effectiveCompletionKind,
          finishedAtMs,
          effectiveMessage,
          tablesScanned,
          tablesChanged,
          viewsScanned,
          viewsChanged,
          errors,
          snapshotsProcessed,
          statsProcessed);
    }
    if (applyLeaseOutcomeTransactionally(
        jobId,
        leaseEpoch,
        effectiveCompletionKind,
        finishedAtMs,
        effectiveMessage,
        tablesScanned,
        tablesChanged,
        viewsScanned,
        viewsChanged,
        errors,
        snapshotsProcessed,
        statsProcessed)) {
      return true;
    }
    if (isIdempotentTerminalLeaseOutcome(
        jobId,
        effectiveCompletionKind,
        finishedAtMs,
        effectiveMessage,
        tablesScanned,
        tablesChanged,
        viewsScanned,
        viewsChanged,
        errors,
        snapshotsProcessed,
        statsProcessed)) {
      return true;
    }
    return false;
  }

  private boolean applyLeaseOutcomeDirectly(
      String jobId,
      String leaseEpoch,
      CompletionKind completionKind,
      long finishedAtMs,
      String message,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed) {
    Optional<StoredEnvelope> updated =
        mutateByJobIdReturningRecord(
            jobId,
            existing ->
                applyLeaseOutcomeToRecord(
                    existing.canonicalPointerKey,
                    existing,
                    jobId,
                    leaseEpoch,
                    completionKind,
                    finishedAtMs,
                    message,
                    tablesScanned,
                    tablesChanged,
                    viewsScanned,
                    viewsChanged,
                    errors,
                    snapshotsProcessed,
                    statsProcessed));
    updated.ifPresent(env -> clearExecutionLeasesIfOwned(env, jobId, leaseEpoch));
    return updated.isPresent();
  }

  private boolean applyLeaseOutcomeTransactionally(
      String jobId,
      String leaseEpoch,
      CompletionKind completionKind,
      long finishedAtMs,
      String message,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed) {
    for (int attempt = 0; attempt < CAS_MAX; attempt++) {
      StoredEnvelope loaded = loadByAnyAccount(jobId).orElse(null);
      if (loaded == null) {
        return false;
      }
      var childSnapshot =
          jobIndexStore().loadCanonicalSnapshot(loaded.canonicalPointerKey).orElse(null);
      StoredReconcileJob previousChild =
          childSnapshot == null ? null : jobIndexStore().readRecord(childSnapshot).orElse(null);
      if (childSnapshot == null || previousChild == null) {
        return false;
      }
      StoredReconcileJob nextChild =
          applyLeaseOutcomeToRecord(
              loaded.canonicalPointerKey,
              jobIndexStore().cloneStoredRecord(previousChild),
              jobId,
              leaseEpoch,
              completionKind,
              finishedAtMs,
              message,
              tablesScanned,
              tablesChanged,
              viewsScanned,
              viewsChanged,
              errors,
              snapshotsProcessed,
              statsProcessed);
      if (nextChild == null) {
        return false;
      }
      if (!isTerminalState(nextChild.state)) {
        return false;
      }
      List<ReconcileJobIndexStore.CanonicalRecordMutation> mutations = new ArrayList<>();
      List<StoredReconcileJob> changedRecords = new ArrayList<>();
      Map<String, StoredReconcileJob> updatedRecordsByJobId = new LinkedHashMap<>();
      mutations.add(
          new ReconcileJobIndexStore.CanonicalRecordMutation(
              childSnapshot, previousChild, nextChild));
      changedRecords.add(nextChild);
      updatedRecordsByJobId.put(nextChild.jobId, nextChild);

      String accountId = blankToEmpty(nextChild.accountId);
      String currentParentJobId = blankToEmpty(nextChild.parentJobId);
      while (!accountId.isBlank() && !currentParentJobId.isBlank()) {
        StoredEnvelope parentEnvelope = loadByAnyAccount(currentParentJobId).orElse(null);
        if (parentEnvelope == null
            || !accountId.equals(parentEnvelope.record.accountId)
            || !isParentCapable(parentEnvelope.record.jobKind())) {
          break;
        }
        var parentSnapshot =
            jobIndexStore().loadCanonicalSnapshot(parentEnvelope.canonicalPointerKey).orElse(null);
        StoredReconcileJob previousParent =
            parentSnapshot == null ? null : jobIndexStore().readRecord(parentSnapshot).orElse(null);
        if (parentSnapshot == null || previousParent == null) {
          return false;
        }
        List<StoredReconcileJob> directChildren = new ArrayList<>();
        for (StoredReconcileJob child : listAllStoredChildJobs(accountId, previousParent.jobId)) {
          StoredReconcileJob updatedChild = updatedRecordsByJobId.get(child.jobId);
          directChildren.add(updatedChild == null ? child : updatedChild);
        }
        var rollup =
            ancestorRollups().recomputeParentProjection(previousParent, directChildren, true);
        if (rollup == null) {
          break;
        }
        StoredReconcileJob nextParent = jobIndexStore().cloneStoredRecord(previousParent);
        applyCanonicalParentSchedulingState(nextParent, rollup);
        if (canonicalSchedulingEquivalent(previousParent, nextParent)) {
          // Even when the parent still appears semantically unchanged, keep it in the CAS set.
          // Concurrent sibling completions can otherwise each commit child-only updates against the
          // same waiting parent snapshot and strand canonical state behind the last terminal child.
          mutations.add(
              new ReconcileJobIndexStore.CanonicalRecordMutation(
                  parentSnapshot, previousParent, nextParent));
          changedRecords.add(nextParent);
          updatedRecordsByJobId.put(nextParent.jobId, nextParent);
          currentParentJobId = blankToEmpty(previousParent.parentJobId);
          continue;
        }
        nextParent.updatedAtMs = System.currentTimeMillis();
        nextParent.canonicalPointerKey = parentEnvelope.canonicalPointerKey;
        mutations.add(
            new ReconcileJobIndexStore.CanonicalRecordMutation(
                parentSnapshot, previousParent, nextParent));
        changedRecords.add(nextParent);
        updatedRecordsByJobId.put(nextParent.jobId, nextParent);
        currentParentJobId = blankToEmpty(nextParent.parentJobId);
      }
      if (!commitCanonicalMutationsWithMaintenance(mutations, changedRecords)) {
        continue;
      }
      clearExecutionLeasesIfOwned(
          new StoredEnvelope(loaded.canonicalPointerKey, nextChild), jobId, leaseEpoch);
      return true;
    }
    return false;
  }

  private boolean isIdempotentTerminalLeaseOutcome(
      String jobId,
      CompletionKind completionKind,
      long finishedAtMs,
      String message,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed) {
    String expectedTerminalState = terminalStateForCompletionKind(completionKind);
    if (blankToEmpty(jobId).isBlank() || expectedTerminalState.isBlank()) {
      return false;
    }
    StoredEnvelope loaded = loadByAnyAccount(jobId).orElse(null);
    if (loaded == null || loaded.record == null) {
      return false;
    }
    if (!expectedTerminalState.equals(blankToEmpty(loaded.record.state))
        || Math.max(0L, loaded.record.finishedAtMs) <= 0L) {
      return false;
    }
    return matchesLeaseOutcome(
        loaded.record,
        completionKind,
        finishedAtMs,
        message,
        tablesScanned,
        tablesChanged,
        viewsScanned,
        viewsChanged,
        errors,
        snapshotsProcessed,
        statsProcessed);
  }

  private boolean matchesLeaseOutcome(
      StoredReconcileJob record,
      CompletionKind completionKind,
      long finishedAtMs,
      String message,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed) {
    if (record == null) {
      return false;
    }
    return switch (completionKind) {
      case SUCCEEDED_WAITING ->
          "JS_WAITING".equals(blankToEmpty(record.state))
              && record.finishedAtMs == 0L
              && blankToEmpty(record.message)
                  .equals(blank(message) ? "Waiting on child work" : message)
              && record.tablesScanned >= Math.max(0L, tablesScanned)
              && record.tablesChanged >= Math.max(0L, tablesChanged)
              && record.viewsScanned >= Math.max(0L, viewsScanned)
              && record.viewsChanged >= Math.max(0L, viewsChanged)
              && record.snapshotsProcessed >= Math.max(0L, snapshotsProcessed)
              && record.statsProcessed >= Math.max(0L, statsProcessed);
      case SUCCEEDED ->
          "JS_SUCCEEDED".equals(blankToEmpty(record.state))
              && record.finishedAtMs == finishedAtMs
              && record.tablesScanned >= Math.max(0L, tablesScanned)
              && record.tablesChanged >= Math.max(0L, tablesChanged)
              && record.viewsScanned >= Math.max(0L, viewsScanned)
              && record.viewsChanged >= Math.max(0L, viewsChanged)
              && record.snapshotsProcessed >= Math.max(0L, snapshotsProcessed)
              && record.statsProcessed >= Math.max(0L, statsProcessed);
      case FAILED_TERMINAL ->
          "JS_FAILED".equals(blankToEmpty(record.state))
              && record.finishedAtMs == finishedAtMs
              && blankToEmpty(record.message).equals(blank(message) ? "Failed" : message)
              && record.tablesScanned >= Math.max(0L, tablesScanned)
              && record.tablesChanged >= Math.max(0L, tablesChanged)
              && record.viewsScanned >= Math.max(0L, viewsScanned)
              && record.viewsChanged >= Math.max(0L, viewsChanged)
              && record.errors == Math.max(0L, errors)
              && record.snapshotsProcessed >= Math.max(0L, snapshotsProcessed)
              && record.statsProcessed >= Math.max(0L, statsProcessed);
      case CANCELLED ->
          "JS_CANCELLED".equals(blankToEmpty(record.state))
              && record.finishedAtMs == finishedAtMs
              && blankToEmpty(record.message).equals(blank(message) ? "Cancelled" : message)
              && record.tablesScanned >= Math.max(0L, tablesScanned)
              && record.tablesChanged >= Math.max(0L, tablesChanged)
              && record.viewsScanned >= Math.max(0L, viewsScanned)
              && record.viewsChanged >= Math.max(0L, viewsChanged)
              && record.errors == Math.max(0L, errors)
              && record.snapshotsProcessed >= Math.max(0L, snapshotsProcessed)
              && record.statsProcessed >= Math.max(0L, statsProcessed);
      default -> false;
    };
  }

  private StoredReconcileJob applyLeaseOutcomeToRecord(
      String canonicalPointerKey,
      StoredReconcileJob nextChild,
      String jobId,
      String leaseEpoch,
      CompletionKind completionKind,
      long finishedAtMs,
      String message,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed) {
    String op = operationName(completionKind);
    boolean allowExpiredWithinGrace =
        completionKind == CompletionKind.SUCCEEDED
            || completionKind == CompletionKind.FAILED_TERMINAL
            || completionKind == CompletionKind.CANCELLED;
    if (!leaseManager()
        .hasActiveLease(jobId, leaseEpoch, nextChild, op, true, true, allowExpiredWithinGrace)) {
      return null;
    }
    if (isTerminalState(nextChild.state)) {
      return null;
    }
    if ("JS_CANCELLING".equals(nextChild.state) && completionKind != CompletionKind.CANCELLED) {
      nextChild.state = "JS_CANCELLED";
      nextChild.message = blank(nextChild.message) ? "Cancelled" : blankToEmpty(nextChild.message);
      if (nextChild.startedAtMs <= 0L) {
        nextChild.startedAtMs = finishedAtMs;
      }
      nextChild.finishedAtMs = finishedAtMs;
      nextChild.tablesScanned = Math.max(nextChild.tablesScanned, tablesScanned);
      nextChild.tablesChanged = Math.max(nextChild.tablesChanged, tablesChanged);
      nextChild.viewsScanned = Math.max(nextChild.viewsScanned, viewsScanned);
      nextChild.viewsChanged = Math.max(nextChild.viewsChanged, viewsChanged);
      nextChild.errors = errors;
      nextChild.snapshotsProcessed = Math.max(nextChild.snapshotsProcessed, snapshotsProcessed);
      nextChild.statsProcessed = Math.max(nextChild.statsProcessed, statsProcessed);
      nextChild.childrenFinalized = false;
      nextChild.readyPointerKey = null;
      nextChild.nextAttemptAtMs = 0L;
      nextChild.updatedAtMs = System.currentTimeMillis();
      nextChild.canonicalPointerKey = canonicalPointerKey;
      return nextChild;
    }
    switch (completionKind) {
      case SUCCEEDED ->
          applySucceededToRecord(
              nextChild,
              finishedAtMs,
              "JS_SUCCEEDED",
              "Succeeded",
              finishedAtMs,
              blankToEmpty(nextChild.executorId),
              false,
              tablesScanned,
              tablesChanged,
              viewsScanned,
              viewsChanged,
              snapshotsProcessed,
              statsProcessed);
      case SUCCEEDED_WAITING ->
          applySucceededToRecord(
              nextChild,
              finishedAtMs,
              "JS_WAITING",
              blank(message) ? "Waiting on child work" : message,
              0L,
              "",
              true,
              tablesScanned,
              tablesChanged,
              viewsScanned,
              viewsChanged,
              snapshotsProcessed,
              statsProcessed);
      case FAILED_RETRYABLE ->
          applyFailedRetryableToRecord(
              nextChild,
              finishedAtMs,
              message,
              tablesScanned,
              tablesChanged,
              viewsScanned,
              viewsChanged,
              errors,
              snapshotsProcessed,
              statsProcessed);
      case FAILED_WAITING_ON_DEPENDENCY ->
          applyFailedWaitingToRecord(
              nextChild,
              false,
              message,
              tablesScanned,
              tablesChanged,
              viewsScanned,
              viewsChanged,
              errors,
              snapshotsProcessed,
              statsProcessed);
      case FAILED_TERMINAL ->
          applyFailedTerminalToRecord(
              nextChild,
              finishedAtMs,
              message,
              tablesScanned,
              tablesChanged,
              viewsScanned,
              viewsChanged,
              errors,
              snapshotsProcessed,
              statsProcessed);
      case CANCELLED ->
          applyCancelledToRecord(
              nextChild,
              finishedAtMs,
              message,
              tablesScanned,
              tablesChanged,
              viewsScanned,
              viewsChanged,
              errors,
              snapshotsProcessed,
              statsProcessed);
    }
    nextChild.updatedAtMs = System.currentTimeMillis();
    nextChild.canonicalPointerKey = canonicalPointerKey;
    return nextChild;
  }

  private boolean canonicalSchedulingEquivalent(
      StoredReconcileJob previous, StoredReconcileJob current) {
    if (previous == null || current == null) {
      return previous == current;
    }
    return Objects.equals(previous.state, current.state)
        && Objects.equals(previous.message, current.message)
        && previous.startedAtMs == current.startedAtMs
        && previous.finishedAtMs == current.finishedAtMs
        && Objects.equals(previous.executorId, current.executorId)
        && previous.childrenFinalized == current.childrenFinalized
        && previous.nextAttemptAtMs == current.nextAttemptAtMs
        && Objects.equals(previous.readyPointerKey, current.readyPointerKey);
  }

  private StoredReconcileJob applySucceededToRecord(
      StoredReconcileJob existing,
      long finishedAtMs,
      String nextState,
      String nextMessage,
      long nextFinishedAtMs,
      String nextExecutorId,
      boolean childrenFinalized,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long snapshotsProcessed,
      long statsProcessed) {
    existing.tablesScanned = Math.max(existing.tablesScanned, tablesScanned);
    existing.tablesChanged = Math.max(existing.tablesChanged, tablesChanged);
    existing.viewsScanned = Math.max(existing.viewsScanned, viewsScanned);
    existing.viewsChanged = Math.max(existing.viewsChanged, viewsChanged);
    existing.errors = 0L;
    existing.snapshotsProcessed = Math.max(existing.snapshotsProcessed, snapshotsProcessed);
    existing.statsProcessed = Math.max(existing.statsProcessed, statsProcessed);
    existing.lastError = "";
    existing.state = nextState;
    existing.message = nextMessage;
    existing.childrenFinalized = childrenFinalized;
    existing.finishedAtMs = nextFinishedAtMs;
    existing.executorId = nextExecutorId;
    existing.nextAttemptAtMs = 0L;
    if (existing.startedAtMs <= 0L) {
      existing.startedAtMs = finishedAtMs;
    }
    existing.readyPointerKey = null;
    return existing;
  }

  private StoredReconcileJob applyFailedRetryableToRecord(
      StoredReconcileJob existing,
      long finishedAtMs,
      String message,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed) {
    existing.attempt = Math.max(0, existing.attempt) + 1;
    existing.tablesScanned = Math.max(existing.tablesScanned, tablesScanned);
    existing.tablesChanged = Math.max(existing.tablesChanged, tablesChanged);
    existing.viewsScanned = Math.max(existing.viewsScanned, viewsScanned);
    existing.viewsChanged = Math.max(existing.viewsChanged, viewsChanged);
    existing.errors = errors;
    existing.snapshotsProcessed = Math.max(existing.snapshotsProcessed, snapshotsProcessed);
    existing.statsProcessed = Math.max(existing.statsProcessed, statsProcessed);
    existing.lastError = message == null ? "Failed" : message;
    existing.childrenFinalized = false;

    if (existing.attempt >= maxAttempts) {
      existing.state = "JS_FAILED";
      existing.message = message == null ? "Failed" : message;
      if (existing.startedAtMs <= 0L) {
        existing.startedAtMs = finishedAtMs;
      }
      existing.finishedAtMs = finishedAtMs;
      existing.nextAttemptAtMs = 0L;
      existing.readyPointerKey = null;
      return existing;
    }

    existing.state = "JS_QUEUED";
    existing.message = message == null ? "Retrying" : message;
    existing.executorId = "";
    existing.nextAttemptAtMs = System.currentTimeMillis() + backoffMs(existing.attempt);
    existing.finishedAtMs = 0L;
    existing.readyPointerKey = readyPointerKeyFor(existing, existing.nextAttemptAtMs);
    return existing;
  }

  private StoredReconcileJob applyFailedWaitingToRecord(
      StoredReconcileJob existing,
      boolean childrenFinalized,
      String message,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed) {
    existing.tablesScanned = Math.max(existing.tablesScanned, tablesScanned);
    existing.tablesChanged = Math.max(existing.tablesChanged, tablesChanged);
    existing.viewsScanned = Math.max(existing.viewsScanned, viewsScanned);
    existing.viewsChanged = Math.max(existing.viewsChanged, viewsChanged);
    existing.errors = errors;
    existing.snapshotsProcessed = Math.max(existing.snapshotsProcessed, snapshotsProcessed);
    existing.statsProcessed = Math.max(existing.statsProcessed, statsProcessed);
    existing.lastError = message == null ? "Waiting on dependency" : message;
    existing.state = "JS_QUEUED";
    existing.message = message == null ? "Waiting on dependency" : message;
    existing.childrenFinalized = childrenFinalized;
    existing.executorId = "";
    existing.nextAttemptAtMs = System.currentTimeMillis() + baseBackoffMs;
    existing.finishedAtMs = 0L;
    existing.readyPointerKey = readyPointerKeyFor(existing, existing.nextAttemptAtMs);
    return existing;
  }

  private StoredReconcileJob applyFailedTerminalToRecord(
      StoredReconcileJob existing,
      long finishedAtMs,
      String message,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed) {
    existing.attempt = Math.max(0, existing.attempt) + 1;
    existing.tablesScanned = Math.max(existing.tablesScanned, tablesScanned);
    existing.tablesChanged = Math.max(existing.tablesChanged, tablesChanged);
    existing.viewsScanned = Math.max(existing.viewsScanned, viewsScanned);
    existing.viewsChanged = Math.max(existing.viewsChanged, viewsChanged);
    existing.errors = errors;
    existing.snapshotsProcessed = Math.max(existing.snapshotsProcessed, snapshotsProcessed);
    existing.statsProcessed = Math.max(existing.statsProcessed, statsProcessed);
    existing.lastError = message == null ? "Failed" : message;
    existing.childrenFinalized = false;
    existing.state = "JS_FAILED";
    existing.message = message == null ? "Failed" : message;
    if (existing.startedAtMs <= 0L) {
      existing.startedAtMs = finishedAtMs;
    }
    existing.finishedAtMs = finishedAtMs;
    existing.nextAttemptAtMs = 0L;
    existing.readyPointerKey = null;
    return existing;
  }

  private StoredReconcileJob applyCancelledToRecord(
      StoredReconcileJob existing,
      long finishedAtMs,
      String message,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed) {
    existing.state = "JS_CANCELLED";
    existing.message = message == null || message.isBlank() ? "Cancelled" : message;
    if (existing.startedAtMs <= 0L) {
      existing.startedAtMs = finishedAtMs;
    }
    existing.finishedAtMs = finishedAtMs;
    existing.tablesScanned = Math.max(existing.tablesScanned, tablesScanned);
    existing.tablesChanged = Math.max(existing.tablesChanged, tablesChanged);
    existing.viewsScanned = Math.max(existing.viewsScanned, viewsScanned);
    existing.viewsChanged = Math.max(existing.viewsChanged, viewsChanged);
    existing.errors = errors;
    existing.snapshotsProcessed = Math.max(existing.snapshotsProcessed, snapshotsProcessed);
    existing.statsProcessed = Math.max(existing.statsProcessed, statsProcessed);
    existing.childrenFinalized = false;
    existing.nextAttemptAtMs = 0L;
    existing.readyPointerKey = null;
    return existing;
  }

  private static String operationName(CompletionKind completionKind) {
    return switch (completionKind) {
      case SUCCEEDED -> "markSucceeded";
      case SUCCEEDED_WAITING -> "markSucceededWaiting";
      case FAILED_RETRYABLE -> "markFailed";
      case FAILED_WAITING_ON_DEPENDENCY -> "markWaitingOnDependency";
      case FAILED_TERMINAL -> "markFailedTerminal";
      case CANCELLED -> "markCancelled";
    };
  }

  private static String terminalStateForCompletionKind(CompletionKind completionKind) {
    return switch (completionKind) {
      case SUCCEEDED -> "JS_SUCCEEDED";
      case FAILED_TERMINAL -> "JS_FAILED";
      case CANCELLED -> "JS_CANCELLED";
      default -> "";
    };
  }

  private List<String> statePointerKeys(StoredReconcileJob record) {
    return indexes().statePointerKeys(record);
  }

  private long backoffMs(int attempts) {
    long base = baseBackoffMs * (1L << Math.min(8, Math.max(0, attempts - 1)));
    return Math.min(maxBackoffMs, base);
  }

  private String writeFileGroupResultPayloadBlobReference(
      String accountId, String jobId, ReconcileFileGroupTask fileGroupTask) {
    String nextBlobUri =
        Keys.reconcileJobResultBlobUri(
            accountId,
            jobId,
            "file-group-result-" + System.currentTimeMillis() + "-" + UUID.randomUUID());
    ReconcileFileGroupTask effective =
        fileGroupTask == null ? ReconcileFileGroupTask.empty() : fileGroupTask;
    payloads()
        .writeBlob(
            nextBlobUri,
            StoredFileGroupResultPayload.of(effective),
            "Failed to persist file group result payload");
    return nextBlobUri;
  }

  private static SnapshotPlanBlob snapshotPlanBlob(List<ReconcileFileGroupTask> fileGroups) {
    List<PlannedFileGroupJob> plannedJobs =
        (fileGroups == null ? List.<ReconcileFileGroupTask>of() : fileGroups)
            .stream()
                .filter(fileGroup -> fileGroup != null && !fileGroup.isEmpty())
                .map(fileGroup -> new PlannedFileGroupJob(ReconcileScope.empty(), fileGroup))
                .toList();
    return SnapshotPlanBlob.of(plannedJobs);
  }

  private boolean isParentCapable(ReconcileJobKind jobKind) {
    return jobKind == ReconcileJobKind.PLAN_CONNECTOR
        || jobKind == ReconcileJobKind.PLAN_TABLE
        || jobKind == ReconcileJobKind.PLAN_SNAPSHOT;
  }

  private static long firstPositiveMin(long first, long second) {
    if (first <= 0L) {
      return Math.max(0L, second);
    }
    if (second <= 0L) {
      return first;
    }
    return Math.min(first, second);
  }

  private static boolean requiresReadyPointer(StoredReconcileJob record) {
    if (record == null) {
      return false;
    }
    return "JS_QUEUED".equals(record.state);
  }

  private void logLeaseSkip(String op, String format, Object... args) {
    if ("markProgress".equals(op)) {
      LOG.debugf(format, args);
    } else {
      LOG.warnf(format, args);
    }
  }

  private void logStateTransition(
      StoredReconcileJob previous, StoredReconcileJob current, String transitionSource) {
    if (previous == null || current == null) {
      return;
    }
    if (blankToEmpty(previous.state).equals(blankToEmpty(current.state))) {
      return;
    }
    LOG.infof(
        "reconcile state transition source=%s jobId=%s kind=%s oldState=%s newState=%s"
            + " readyPointerKey=%s completionHint=%s",
        blankToEmpty(transitionSource),
        current.jobId,
        current.jobKind(),
        blankToEmpty(previous.state),
        blankToEmpty(current.state),
        blankToEmpty(current.readyPointerKey),
        blankToEmpty(current.message));
  }

  private static boolean isTerminalState(String state) {
    if (state == null) {
      return false;
    }
    return switch (state) {
      case "JS_SUCCEEDED", "JS_FAILED", "JS_CANCELLED" -> true;
      default -> false;
    };
  }

  private static boolean isCancellationState(String state) {
    return "JS_CANCELLING".equals(blankToEmpty(state))
        || "JS_CANCELLED".equals(blankToEmpty(state));
  }

  private static boolean isRetryableCompletion(CompletionKind completionKind) {
    return completionKind == CompletionKind.FAILED_RETRYABLE
        || completionKind == CompletionKind.FAILED_WAITING_ON_DEPENDENCY;
  }

  private static void validateFileGroupResultMatchesCanonical(
      StoredReconcileJob state, ReconcileFileGroupTask result) {
    if (state == null || state.jobKind() != ReconcileJobKind.EXEC_FILE_GROUP) {
      return;
    }
    ReconcileFileGroupTask effective = result == null ? ReconcileFileGroupTask.empty() : result;
    if ((!blank(effective.planId())
            && !Objects.equals(
                blankToEmpty(state.fileGroupPlanId), blankToEmpty(effective.planId())))
        || (!blank(effective.groupId())
            && !Objects.equals(
                blankToEmpty(state.fileGroupGroupId), blankToEmpty(effective.groupId())))
        || (!blank(effective.tableId())
            && !Objects.equals(
                blankToEmpty(state.fileGroupTableId), blankToEmpty(effective.tableId())))
        || (effective.snapshotId() >= 0L && state.fileGroupSnapshotId != effective.snapshotId())) {
      throw new IllegalArgumentException(
          "EXEC_FILE_GROUP result identity does not match canonical job identity");
    }
  }

  private static void assertImmutableJobIdentityPreserved(
      StoredReconcileJob baseline, StoredReconcileJob nextRecord) {
    if (baseline == null || nextRecord == null) {
      return;
    }
    if (baseline.jobKind() != ReconcileJobKind.EXEC_FILE_GROUP) {
      return;
    }
    if (nextRecord.jobKind() != ReconcileJobKind.EXEC_FILE_GROUP
        || !Objects.equals(baseline.fileGroupPlanId, nextRecord.fileGroupPlanId)
        || !Objects.equals(baseline.fileGroupGroupId, nextRecord.fileGroupGroupId)
        || !Objects.equals(baseline.fileGroupTableId, nextRecord.fileGroupTableId)
        || baseline.fileGroupSnapshotId != nextRecord.fileGroupSnapshotId) {
      throw new IllegalStateException(
          "EXEC_FILE_GROUP canonical identity is immutable; baseline planId="
              + blankToEmpty(baseline.fileGroupPlanId)
              + " groupId="
              + blankToEmpty(baseline.fileGroupGroupId)
              + " tableId="
              + blankToEmpty(baseline.fileGroupTableId)
              + " snapshotId="
              + baseline.fileGroupSnapshotId
              + " next planId="
              + blankToEmpty(nextRecord.fileGroupPlanId)
              + " groupId="
              + blankToEmpty(nextRecord.fileGroupGroupId)
              + " tableId="
              + blankToEmpty(nextRecord.fileGroupTableId)
              + " snapshotId="
              + nextRecord.fileGroupSnapshotId);
    }
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value.trim();
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }

  private static final class StoredEnvelope {
    final String canonicalPointerKey;
    final StoredReconcileJob record;

    private StoredEnvelope(String canonicalPointerKey, StoredReconcileJob record) {
      this.canonicalPointerKey = canonicalPointerKey;
      this.record = record;
    }
  }

  private static final class LeaseOutcomeRejectedException extends RuntimeException {
    private static final long serialVersionUID = 1L;
  }

  static final class StoredFinalizedSnapshotEvent {
    public String eventId;
    public String accountId;
    public String tableId;
    public long snapshotId;
    public long finalizedAtMs;
    public String finalizerJobId;

    StoredFinalizedSnapshotEvent(
        String eventId,
        String accountId,
        String tableId,
        long snapshotId,
        long finalizedAtMs,
        String finalizerJobId) {
      this.eventId = eventId == null ? "" : eventId;
      this.accountId = accountId == null ? "" : accountId;
      this.tableId = tableId == null ? "" : tableId;
      this.snapshotId = Math.max(0L, snapshotId);
      this.finalizedAtMs = Math.max(0L, finalizedAtMs);
      this.finalizerJobId = finalizerJobId == null ? "" : finalizerJobId;
    }

    StoredFinalizedSnapshotEvent() {}
  }
}
