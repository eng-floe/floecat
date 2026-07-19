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
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.reconciler.impl.PlannedFileGroupJob;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.impl.SnapshotPlanBlobStore.SnapshotPlanBlob;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileResult;
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
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobProjection;
import ai.floedb.floecat.service.reconciler.jobs.durable.projection.ReconcileJobProjectionStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.projection.ReconcileJobProjector;
import ai.floedb.floecat.service.reconciler.jobs.durable.projection.ReconcileJobProjector.JobProjection;
import ai.floedb.floecat.service.reconciler.jobs.durable.projection.ReconcileJobRootSummaryStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.queue.ReconcileAncestorRollupService;
import ai.floedb.floecat.service.reconciler.jobs.durable.queue.ReconcileCancellationMaintenanceService;
import ai.floedb.floecat.service.reconciler.jobs.durable.queue.ReconcileJobCancellationService;
import ai.floedb.floecat.service.reconciler.jobs.durable.queue.ReconcileJobCompleter;
import ai.floedb.floecat.service.reconciler.jobs.durable.queue.ReconcileJobEnqueuer;
import ai.floedb.floecat.service.reconciler.jobs.durable.queue.ReconcileLeaseMaintenanceService;
import ai.floedb.floecat.service.reconciler.jobs.durable.queue.ReconcileProjectionMaintenanceService;
import ai.floedb.floecat.service.reconciler.jobs.durable.queue.ReconcileReadyIndexMaintenanceService;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileJobDetailLoader;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileJobExecutionLoader;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileJobIndexes;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileLeaseStateCodec;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcilePayloadStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.CanonicalPointerSnapshot;
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
import ai.floedb.floecat.service.telemetry.ServiceMetrics;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.stats.spi.StatsStore.UnpublishedGenerationDeleteResult;
import ai.floedb.floecat.storage.aws.DynamoDbClientManager;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Context;
import io.grpc.Deadline;
import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
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
  private static final String STATS_CLEANUP_PENDING = "PENDING";
  private static final String STATS_CLEANUP_COMPLETED = "COMPLETED";

  private static final int DEFAULT_MAX_ATTEMPTS = 8;
  private static final long DEFAULT_BASE_BACKOFF_MS = 500L;
  private static final long DEFAULT_MAX_BACKOFF_MS = 30_000L;
  private static final long DEFAULT_LEASE_MS = 120_000L;
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
  @Inject ReconcileLeaseMaintenanceService leaseMaintenanceService;
  @Inject ReconcileProjectionMaintenanceService projectionMaintenanceService;
  @Inject ReconcileCancellationMaintenanceService cancellationMaintenanceService;
  @Inject ReconcileReadyIndexMaintenanceService readyIndexMaintenanceService;
  @Inject ReconcileReadyQueueStore readyQueueStore;
  @Inject ReconcileReadyQueueBackend readyQueueBackend;
  @Inject ConnectorRepository connectorRepo;
  @Inject Observability observability;
  @Inject StatsStore statsStore;
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
      dynamoBackend.bind(dynamoDbClientManager.get(), kvTable);
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

  public void initializeJobIndexStore() {
    jobIndexStore();
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
    projectionStore.bind(pointerStore, payloads(), jobIndexStore());
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
      jobIndexStore();
      memoryBackend.bind(pointerStore, jobIndexBackend);
    } else if (leaseBackend instanceof DynamoReconcileLeaseBackend dynamoBackend
        && dynamoDbClientManager != null
        && dynamoDbClientManager.isResolvable()) {
      dynamoBackend.bind(dynamoDbClientManager.get(), kvTable);
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
      dynamoBackend.bind(dynamoDbClientManager.get(), kvTable);
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

  private ReconcileLeaseMaintenanceService leaseMaintenance() {
    if (leaseMaintenanceService == null) {
      leaseMaintenanceService = new ReconcileLeaseMaintenanceService();
    }
    ReconcileReadyQueueStore queue = readyQueue();
    leaseMaintenanceService.bind(
        leaseManager(),
        readyQueueBackend,
        queue,
        jobIndexStore(),
        this::reclaimExpiredLease,
        this::isBlockedByAncestorCancellation,
        readyScanLimit,
        reclaimIntervalMs);
    return leaseMaintenanceService;
  }

  private ReconcileProjectionMaintenanceService projectionMaintenance() {
    if (projectionMaintenanceService == null) {
      projectionMaintenanceService = new ReconcileProjectionMaintenanceService();
    }
    projectionMaintenanceService.bind(
        pointerStore,
        this::refreshProjectedParent,
        this::isObsoleteDirtyParentProjection,
        readyScanLimit);
    return projectionMaintenanceService;
  }

  private ReconcileCancellationMaintenanceService cancellationMaintenance() {
    if (cancellationMaintenanceService == null) {
      cancellationMaintenanceService = new ReconcileCancellationMaintenanceService();
    }
    cancellationMaintenanceService.bind(
        pointerStore,
        this::cleanupCancellationRoot,
        this::isObsoleteCancellationCleanupRoot,
        readyScanLimit);
    return cancellationMaintenanceService;
  }

  private ReconcileReadyIndexMaintenanceService readyIndexMaintenance() {
    if (readyIndexMaintenanceService == null) {
      readyIndexMaintenanceService = new ReconcileReadyIndexMaintenanceService();
    }
    readyIndexMaintenanceService.bind(jobIndexStore(), readyQueue(), readyScanLimit);
    return readyIndexMaintenanceService;
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
    observeLeaseScanPermitGauges();
    jobIndexStore();
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
    BulkEnqueueSpec spec =
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
            pinnedExecutorId);
    BulkEnqueueResult result = bulkEnqueue(List.of(spec));
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
      if (shouldMarkSelfDirtyAfterEnqueue(spec)) {
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
                            String plannerOutcomeFingerprint =
                                plannerOutcomeFingerprint(jobId, queuedInserts);
                            long expectedDirectChildren =
                                directPlannerChildCount(jobId, queuedInserts);
                            boolean repairingCommittedPlannerOutcome =
                                completionKind == CompletionKind.SUCCEEDED_WAITING
                                    && !blankToEmpty(previous.plannerOutcomeFingerprint).isBlank();
                            if (repairingCommittedPlannerOutcome
                                && ((!"JS_WAITING".equals(previous.state)
                                        && !"JS_RUNNING".equals(previous.state))
                                    || !plannerOutcomeFingerprint.equals(
                                        blankToEmpty(previous.plannerOutcomeFingerprint))
                                    || expectedDirectChildren
                                        != Math.max(0L, previous.expectedDirectChildren)
                                    || ("JS_WAITING".equals(previous.state)
                                        && !blankToEmpty(leaseEpoch)
                                            .equals(
                                                blankToEmpty(
                                                    previous.plannerOutcomeLeaseEpoch))))) {
                              throw new IllegalStateException(
                                  "planner outcome replay does not match the committed child set for job "
                                      + jobId);
                            }
                            if (matchesLeaseOutcome(
                                previous,
                                completionKind,
                                finishedAtMs,
                                message,
                                tablesScanned,
                                tablesChanged,
                                viewsScanned,
                                viewsChanged,
                                errors,
                                snapshotsProcessed,
                                statsProcessed,
                                0L)) {
                              StoredReconcileJob guarded =
                                  jobIndexStore().cloneStoredRecord(previous);
                              guarded.updatedAtMs = System.currentTimeMillis();
                              guarded.canonicalPointerKey = loaded.canonicalPointerKey;
                              completedParent.set(
                                  new StoredEnvelope(
                                      loaded.canonicalPointerKey,
                                      jobIndexStore().cloneStoredRecord(guarded)));
                              return List.of(
                                  new ReconcileJobIndexStore.CanonicalRecordMutation(
                                      snapshot, previous, guarded));
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
                                    statsProcessed,
                                    0L);
                            if (next == null) {
                              throw new LeaseOutcomeRejectedException();
                            }
                            next.expectedDirectChildren = expectedDirectChildren;
                            if (completionKind == CompletionKind.SUCCEEDED_WAITING) {
                              next.plannerOutcomeFingerprint = plannerOutcomeFingerprint;
                              next.plannerOutcomeLeaseEpoch = blankToEmpty(leaseEpoch);
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
          statsProcessed,
          0L);
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
          statsProcessed,
          0L);
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
        statsProcessed,
        0L)) {
      return false;
    }
    clearExecutionLeasesIfOwned(persistedParent, jobId, leaseEpoch);
    markDirtyParentForRecord(persistedParent.record);
    upsertRootSummaryForRecord(persistedParent.record);
    advanceCanonicalSchedulingAfterRecordChange(persistedParent.record);
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
      if (shouldMarkSelfDirtyAfterEnqueue(spec)) {
        markDirtyParent(spec.accountId, item.jobId);
      }
      boolean directPlannerChild = jobId.equals(blankToEmpty(spec.parentJobId));
      if (item.created) {
        resetFinalizedSnapshotIfEligible(spec);
      }
      // Direct children were reserved on the parent in the outcome transaction. Keep the
      // per-child increment as the fallback for any non-direct enqueue routed through this path.
      if (item.created && !directPlannerChild) {
        requestProjectionRefresh(spec.accountId, spec.parentJobId, 1L);
      } else {
        markDirtyParent(spec.accountId, spec.parentJobId);
      }
    }
    return true;
  }

  private long directPlannerChildCount(
      String parentJobId, List<ReconcileJobIndexStore.QueuedJobInsert> inserts) {
    return (inserts == null ? List.<ReconcileJobIndexStore.QueuedJobInsert>of() : inserts)
        .stream()
            .filter(Objects::nonNull)
            .filter(
                insert ->
                    insert.record() != null
                        && parentJobId.equals(blankToEmpty(insert.record().parentJobId)))
            .map(ReconcileJobIndexStore.QueuedJobInsert::dedupePointerKey)
            .filter(key -> !blankToEmpty(key).isBlank())
            .distinct()
            .count();
  }

  private String plannerOutcomeFingerprint(
      String parentJobId, List<ReconcileJobIndexStore.QueuedJobInsert> inserts) {
    try {
      var digest = java.security.MessageDigest.getInstance("SHA-256");
      digest.update(blankToEmpty(parentJobId).getBytes(java.nio.charset.StandardCharsets.UTF_8));
      digest.update((byte) 0);
      (inserts == null ? List.<ReconcileJobIndexStore.QueuedJobInsert>of() : inserts)
          .stream()
              .filter(Objects::nonNull)
              .filter(
                  insert ->
                      insert.record() != null
                          && parentJobId.equals(blankToEmpty(insert.record().parentJobId)))
              .map(ReconcileJobIndexStore.QueuedJobInsert::dedupePointerKey)
              .filter(key -> !blankToEmpty(key).isBlank())
              .distinct()
              .sorted()
              .forEach(
                  key -> {
                    digest.update(key.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                    digest.update((byte) 0);
                  });
      return java.util.HexFormat.of().formatHex(digest.digest());
    } catch (java.security.NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("SHA-256 is unavailable", impossible);
    }
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
    List<ReconcileJob> jobs = new ArrayList<>(page.summaries().size());
    for (StoredReconcileJobListSummary summary : page.summaries()) {
      StoredReconcileJobListSummary repaired = repairStaleCancellationRootSummary(summary);
      if (repaired == null) {
        continue;
      }
      if (states != null && !states.isEmpty() && !states.contains(blankToEmpty(repaired.state()))) {
        continue;
      }
      jobs.add(ReconcileJobRootSummaryStore.toPublicJob(repaired));
    }
    return new ReconcileJobPage(jobs, page.nextPageToken());
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
    long startedAtMs = System.currentTimeMillis();
    boolean acquired;
    try {
      acquired = permits.tryAcquire(leaseAcquireTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new CancellationException("reconcile lease scan admission interrupted");
    }
    if (!acquired) {
      observeLeaseNext(startedAtMs, new LeaseScanStats(), "capacity_exceeded");
      throw new LeaseScanCapacityExceededException(
          "reconcile lease scan capacity exhausted cap=" + leaseMaxConcurrency);
    }
    LeaseRequest effective = request == null ? LeaseRequest.all() : request;
    LeaseScanStats scanStats = new LeaseScanStats();
    configureLeaseScanStats(scanStats, startedAtMs);
    try {
      Optional<LeasedJob> leased;
      String outcome;
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
      if (scanStats.abortedByCaller) {
        outcome = "caller_cancelled";
      } else if (scanStats.abortedByDeadline) {
        outcome = "deadline_aborted";
      } else {
        outcome = leased.isPresent() ? "leased" : "empty";
      }
      long totalMs = System.currentTimeMillis() - startedAtMs;
      if (scanStats.abortedByCaller) {
        LOG.warnf(
            "leaseNext cancelled by caller total_ms=%d scan_count=%d candidate_count=%d",
            totalMs, scanStats.scanCount, scanStats.candidateCount);
      } else if (scanStats.abortedByDeadline) {
        LOG.warnf(
            "leaseNext aborted by scan deadline total_ms=%d scan_count=%d candidate_count=%d"
                + " budget_ms=%d",
            totalMs, scanStats.scanCount, scanStats.candidateCount, leaseScanBudgetMs);
      } else {
        LOG.debugf(
            "leaseNext total_ms=%d scan_count=%d candidate_count=%d leased=%s",
            totalMs, scanStats.scanCount, scanStats.candidateCount, leased.isPresent());
      }
      observeLeaseNext(startedAtMs, scanStats, outcome);
      return leased;
    } catch (RuntimeException e) {
      observeLeaseNext(startedAtMs, scanStats, "error");
      throw e;
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
    runLeaseMaintenanceOnce(maxMillis);
    runProjectionMaintenanceOnce(maxMillis);
  }

  public void runLeaseMaintenanceOnce(long maxMillis) {
    leaseMaintenance().runLeaseMaintenanceOnce(maxMillis);
  }

  public void runProjectionMaintenanceOnce(long maxMillis) {
    projectionMaintenance().runProjectionMaintenanceOnce(maxMillis);
    runAbandonedFullRescanStatsCleanupMaintenanceOnce(maxMillis);
  }

  public void runCancellationMaintenanceOnce(long maxMillis) {
    runAbandonedFullRescanStatsCleanupMaintenanceOnce(maxMillis);
    cancellationMaintenance().runCancellationMaintenanceOnce(maxMillis);
  }

  public void runReadyIndexMaintenanceOnce(long maxMillis) {
    readyIndexMaintenance().runReadyIndexMaintenanceOnce(maxMillis);
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
    boolean replayingCommittedWaitingOutcome =
        "JS_WAITING".equals(existing.state)
            && !blankToEmpty(existing.plannerOutcomeFingerprint).isBlank()
            && !blankToEmpty(leaseEpoch).isBlank()
            && blankToEmpty(leaseEpoch).equals(blankToEmpty(existing.plannerOutcomeLeaseEpoch));
    if (!replayingCommittedWaitingOutcome
        && !leaseManager()
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
            job.parentJobId,
            existing.laneKey));
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
          Optional<StoredEnvelope> updated;
          try {
            updated =
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
                      existing.statsProcessed =
                          Math.max(existing.statsProcessed, fileGroupStatsProcessed(effective));
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
            markDirtyParentForRecord(updated.get().record);
          } catch (RuntimeException e) {
            blobStore.delete(blobUri);
            throw e;
          }
        });
  }

  private boolean shouldMarkSelfDirtyAfterEnqueue(BulkEnqueueSpec spec) {
    return spec != null
        && isParentCapable(spec.jobKind)
        && spec.jobKind != ReconcileJobKind.PLAN_SNAPSHOT;
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
    onHotPath(
        () -> {
          completer().markRunning(jobId, leaseEpoch, startedAtMs, executorId);
        });
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
        () -> {
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
                  message);
        });
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
        statsProcessed,
        0L);
    recordFinalizedSnapshotIfEligible(jobId);
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
        statsProcessed,
        0L);
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
        statsProcessed,
        0L);
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
        statsProcessed,
        0L);
  }

  @Override
  public Optional<ReconcileJob> cancel(String accountId, String jobId, String reason) {
    StoredEnvelope loaded = loadByAnyAccount(jobId).orElse(null);
    Optional<ReconcileJob> cancelled = cancellation().cancel(accountId, jobId, reason);
    if (cancelled.isPresent() || cancellationPropagationAllowed(accountId, loaded)) {
      StoredReconcileJob cancelledRecord =
          cancelled.isPresent()
              ? loadByAnyAccount(jobId).map(env -> env.record).orElse(null)
              : loaded == null ? null : loaded.record;
      requestCancellationCleanupForRecord(cancelledRecord);
      markDirtyParentForRecord(cancelledRecord);
      return get(accountId, jobId);
    }
    return cancelled;
  }

  private boolean cancellationPropagationAllowed(String accountId, StoredEnvelope loaded) {
    if (loaded == null || loaded.record == null) {
      return false;
    }
    if (!blank(accountId)
        && !blankToEmpty(accountId).equals(blankToEmpty(loaded.record.accountId))) {
      return false;
    }
    return isCancellationState(loaded.record.state);
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
        statsProcessed,
        0L);
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
    refreshProjectedParent(accountId, parentJobId, true);
  }

  private void refreshProjectedParent(
      String accountId, String parentJobId, boolean propagateDirtyParent) {
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
    if (isBlockedByAncestorCancellation(parent)) {
      return;
    }
    long requestedGeneration = Math.max(0L, parent.projectionRequestedGeneration);
    long previousAppliedGeneration = Math.max(0L, parent.projectionAppliedGeneration);
    var previousProjection = projections().load(accountId, parentJobId).orElse(null);
    if (hasCurrentTerminalProjection(parent, previousProjection)) {
      if (blankToEmpty(parent.parentJobId).isBlank()) {
        rootSummaries().upsert(toRootListSummary(parent, previousProjection));
      }
      enqueueSnapshotFinalizationForPlanSnapshotIfEligible(
          projector().toPublicJob(parent, previousProjection, true),
          snapshotTaskFromStored(parent));
      advanceAppliedProjectionGeneration(accountId, parentJobId, requestedGeneration);
      LOG.debugf(
          "reconcile terminal projection refresh skipped accountId=%s jobId=%s kind=%s"
              + " requestedGeneration=%d previousAppliedGeneration=%d projectionGeneration=%d",
          accountId,
          parentJobId,
          parent.jobKind(),
          Long.valueOf(requestedGeneration),
          Long.valueOf(previousAppliedGeneration),
          Long.valueOf(previousProjection.appliedGeneration()));
      return;
    }
    if (!projectionNeedsRefresh(parent, previousProjection)
        && projectionAggregateMatchesCanonical(parent, previousProjection)) {
      if (blankToEmpty(parent.parentJobId).isBlank()) {
        rootSummaries().upsert(toRootListSummary(parent, previousProjection));
      }
      enqueueSnapshotFinalizationForPlanSnapshotIfEligible(
          projector().toPublicJob(parent, previousProjection, true),
          snapshotTaskFromStored(parent));
      advanceAppliedProjectionGeneration(accountId, parentJobId, requestedGeneration);
      if (propagateDirtyParent && requestedGeneration > previousAppliedGeneration) {
        markDirtyParent(parent.accountId, parent.parentJobId);
      }
      LOG.debugf(
          "reconcile projection refresh skipped accountId=%s jobId=%s kind=%s"
              + " requestedGeneration=%d previousAppliedGeneration=%d projectionGeneration=%d",
          accountId,
          parentJobId,
          parent.jobKind(),
          Long.valueOf(requestedGeneration),
          Long.valueOf(previousAppliedGeneration),
          Long.valueOf(previousProjection.appliedGeneration()));
      return;
    }
    if (previousProjection != null
        && projectionAggregateMatchesCanonical(parent, previousProjection)
        && requestedGeneration > Math.max(0L, previousProjection.appliedGeneration())
        && canPublishCanonicalProjectionPatch(parent, previousProjection)) {
      var nextProjection =
          projectionWithCanonicalFields(parent, previousProjection, requestedGeneration);
      projections().upsert(nextProjection);
      LOG.infof(
          "reconcile projection generation patch accountId=%s jobId=%s kind=%s parentJobId=%s"
              + " requestedGeneration=%d previousAppliedGeneration=%d projectionGeneration=%d"
              + " state=%s tables=%d/%d stats=%d indexes=%d fileGroups=%d/%d files=%d/%d",
          accountId,
          parentJobId,
          parent.jobKind(),
          blankToEmpty(parent.parentJobId),
          Long.valueOf(requestedGeneration),
          Long.valueOf(previousAppliedGeneration),
          Long.valueOf(nextProjection.appliedGeneration()),
          nextProjection.state(),
          Long.valueOf(nextProjection.tablesChanged()),
          Long.valueOf(nextProjection.tablesScanned()),
          Long.valueOf(nextProjection.statsProcessed()),
          Long.valueOf(nextProjection.indexesProcessed()),
          Long.valueOf(nextProjection.completedFileGroups()),
          Long.valueOf(nextProjection.plannedFileGroups()),
          Long.valueOf(nextProjection.completedFiles()),
          Long.valueOf(nextProjection.plannedFiles()));
      if (blankToEmpty(parent.parentJobId).isBlank()) {
        rootSummaries().upsert(toRootListSummary(parent, nextProjection));
      }
      enqueueSnapshotFinalizationForPlanSnapshotIfEligible(
          projector().toPublicJob(parent, nextProjection, true), snapshotTaskFromStored(parent));
      advanceAppliedProjectionGeneration(accountId, parentJobId, requestedGeneration);
      if (propagateDirtyParent && requestedGeneration > previousAppliedGeneration) {
        markDirtyParent(parent.accountId, parent.parentJobId);
      }
      return;
    }
    if (canPublishCanonicalAggregatePatch(parent, previousProjection)) {
      var nextProjection =
          projectionWithCanonicalFields(parent, previousProjection, requestedGeneration);
      boolean projectionChanged = !Objects.equals(previousProjection, nextProjection);
      if (projectionChanged) {
        if (!commitProjectionAggregateChange(parent, previousProjection, nextProjection)) {
          throw new IllegalStateException(
              "Failed to apply reconcile projection aggregate patch to parent job "
                  + blankToEmpty(parent.parentJobId));
        }
      }
      LOG.infof(
          "reconcile projection aggregate patch accountId=%s jobId=%s kind=%s parentJobId=%s"
              + " requestedGeneration=%d previousAppliedGeneration=%d projectionGeneration=%d"
              + " state=%s stats=%d indexes=%d fileGroups=%d/%d files=%d/%d",
          accountId,
          parentJobId,
          parent.jobKind(),
          blankToEmpty(parent.parentJobId),
          Long.valueOf(requestedGeneration),
          Long.valueOf(previousAppliedGeneration),
          Long.valueOf(nextProjection.appliedGeneration()),
          nextProjection.state(),
          Long.valueOf(nextProjection.statsProcessed()),
          Long.valueOf(nextProjection.indexesProcessed()),
          Long.valueOf(nextProjection.completedFileGroups()),
          Long.valueOf(nextProjection.plannedFileGroups()),
          Long.valueOf(nextProjection.completedFiles()),
          Long.valueOf(nextProjection.plannedFiles()));
      if (blankToEmpty(parent.parentJobId).isBlank()) {
        rootSummaries().upsert(toRootListSummary(parent, nextProjection));
      }
      enqueueSnapshotFinalizationForPlanSnapshotIfEligible(
          projector().toPublicJob(parent, nextProjection, true), snapshotTaskFromStored(parent));
      advanceAppliedProjectionGeneration(accountId, parentJobId, requestedGeneration);
      if (propagateDirtyParent
          && (requestedGeneration > previousAppliedGeneration || projectionChanged)) {
        markDirtyParent(parent.accountId, parent.parentJobId);
      }
      return;
    }
    List<StoredReconcileJob> directChildren = listAllStoredChildJobs(accountId, parentJobId);
    var nextProjection = recomputeSummaryProjection(parent, directChildren, false, true);
    if (nextProjection == null) {
      return;
    }
    boolean projectionChanged = !Objects.equals(previousProjection, nextProjection);
    if (projectionChanged) {
      if (!commitProjectionAggregateChange(parent, previousProjection, nextProjection)) {
        throw new IllegalStateException(
            "Failed to apply reconcile projection aggregate patch to parent job "
                + blankToEmpty(parent.parentJobId));
      }
    }
    if (hasExactCanonicalProjectionMismatch(parent, nextProjection)
        && !applyCanonicalAggregateProjection(parent, nextProjection)) {
      throw new IllegalStateException(
          "Failed to repair reconcile projection aggregate counters for job " + parentJobId);
    }
    LOG.infof(
        "reconcile projection refresh accountId=%s jobId=%s kind=%s parentJobId=%s children=%d"
            + " requestedGeneration=%d previousAppliedGeneration=%d projectionGeneration=%d"
            + " state=%s tables=%d/%d views=%d/%d snapshots=%d stats=%d indexes=%d errors=%d",
        accountId,
        parentJobId,
        parent.jobKind(),
        blankToEmpty(parent.parentJobId),
        Integer.valueOf(directChildren.size()),
        Long.valueOf(requestedGeneration),
        Long.valueOf(previousAppliedGeneration),
        Long.valueOf(nextProjection.appliedGeneration()),
        nextProjection.state(),
        Long.valueOf(nextProjection.tablesChanged()),
        Long.valueOf(nextProjection.tablesScanned()),
        Long.valueOf(nextProjection.viewsChanged()),
        Long.valueOf(nextProjection.viewsScanned()),
        Long.valueOf(nextProjection.snapshotsProcessed()),
        Long.valueOf(nextProjection.statsProcessed()),
        Long.valueOf(nextProjection.indexesProcessed()),
        Long.valueOf(nextProjection.errors()));
    if (blankToEmpty(parent.parentJobId).isBlank()) {
      rootSummaries().upsert(toRootListSummary(parent, nextProjection));
    }
    enqueueSnapshotFinalizationForPlanSnapshotIfEligible(
        projector().toPublicJob(parent, nextProjection, true), snapshotTaskFromStored(parent));
    advanceAppliedProjectionGeneration(accountId, parentJobId, requestedGeneration);
    if (propagateDirtyParent
        && (requestedGeneration > previousAppliedGeneration || projectionChanged)) {
      markDirtyParent(parent.accountId, parent.parentJobId);
    }
  }

  private boolean hasLiveDirectChildLease(StoredReconcileJob parent) {
    if (parent == null || !isParentCapable(parent.jobKind())) {
      return false;
    }
    long now = System.currentTimeMillis();
    for (StoredReconcileJob child : listAllStoredChildJobs(parent.accountId, parent.jobId)) {
      if (child != null && leaseManager().hasLiveLease(child, true, now)) {
        return true;
      }
    }
    return false;
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

  private void requestCancellationCleanupForRecord(StoredReconcileJob record) {
    if (record == null || !isCancellationState(record.state)) {
      return;
    }
    requestCancellationCleanup(record.accountId, record.jobId);
  }

  private boolean requestCancellationCleanup(String accountId, String rootJobId) {
    String effectiveAccountId = blankToEmpty(accountId);
    String effectiveRootJobId = blankToEmpty(rootJobId);
    if (effectiveAccountId.isBlank() || effectiveRootJobId.isBlank()) {
      return false;
    }
    String key = Keys.reconcileCancellationCleanupPointer(effectiveAccountId, effectiveRootJobId);
    String payload =
        ReconcileCancellationMaintenanceService.cancellationCleanupPayload(
            new ReconcileCancellationMaintenanceService.CancellationCleanupRequest(
                effectiveAccountId, effectiveRootJobId, "", false, false, false));
    for (int attempt = 0; attempt < CAS_MAX; attempt++) {
      Pointer current = pointerStore.get(key).orElse(null);
      if (current != null) {
        ReconcileCancellationMaintenanceService.CancellationCleanupRequest currentRequest =
            ReconcileCancellationMaintenanceService.cancellationCleanupRequest(current);
        if (currentRequest == null || !currentRequest.paused()) {
          return true;
        }
        Pointer next =
            PointerReferences.asOpaqueMarkerPointer(
                    current.toBuilder().setVersion(current.getVersion() + 1L), payload)
                .build();
        if (pointerStore.compareAndSet(key, current.getVersion(), next)) {
          return true;
        }
        continue;
      }
      long expectedVersion = current == null ? 0L : current.getVersion();
      Pointer next =
          current == null
              ? PointerReferences.opaqueMarkerPointer(key, payload, 1L)
              : PointerReferences.asOpaqueMarkerPointer(
                      current.toBuilder().setVersion(current.getVersion() + 1L), payload)
                  .build();
      if (pointerStore.compareAndSet(key, expectedVersion, next)) {
        return true;
      }
    }
    throw new IllegalStateException(
        "Failed to mark reconcile cancellation cleanup accountId="
            + effectiveAccountId
            + " rootJobId="
            + effectiveRootJobId);
  }

  private ReconcileCancellationMaintenanceService.CancellationCleanupResult cleanupCancellationRoot(
      ReconcileCancellationMaintenanceService.CancellationCleanupRequest request,
      int childPageSize) {
    if (request == null) {
      return new ReconcileCancellationMaintenanceService.CancellationCleanupResult(
          true, "", false, false, false);
    }
    StoredReconcileJob root = activeCancellationCleanupRoot(request);
    if (root == null) {
      throw ReconcileCancellationMaintenanceService.obsoleteMarker();
    }
    return propagateDirectChildCancellation(root, request, childPageSize);
  }

  private boolean isObsoleteCancellationCleanupRoot(
      ReconcileCancellationMaintenanceService.CancellationCleanupRequest request) {
    return activeCancellationCleanupRoot(request) == null;
  }

  private StoredReconcileJob activeCancellationCleanupRoot(
      ReconcileCancellationMaintenanceService.CancellationCleanupRequest request) {
    if (request == null) {
      return null;
    }
    String accountId = blankToEmpty(request.accountId());
    String rootJobId = blankToEmpty(request.rootJobId());
    StoredEnvelope loaded = loadByAnyAccount(rootJobId).orElse(null);
    if (loaded == null
        || loaded.record == null
        || !accountId.equals(blankToEmpty(loaded.record.accountId))
        || !isCancellationState(loaded.record.state)
        || cancellationCleanupRootComplete(loaded.record)) {
      return null;
    }
    return loaded.record;
  }

  private boolean cancellationCleanupRootComplete(StoredReconcileJob root) {
    return root != null
        && "JS_CANCELLED".equals(blankToEmpty(root.state))
        && root.finishedAtMs > 0L
        && root.childrenFinalized
        && !needsAbandonedFullRescanStatsCleanup(root);
  }

  private ReconcileCancellationMaintenanceService.CancellationCleanupResult
      propagateDirectChildCancellation(
          StoredReconcileJob root,
          ReconcileCancellationMaintenanceService.CancellationCleanupRequest request,
          int childPageSize) {
    if (root == null || !isCancellationState(root.state)) {
      return new ReconcileCancellationMaintenanceService.CancellationCleanupResult(
          true, "", false, false, false);
    }
    String accountId = blankToEmpty(root.accountId);
    String rootJobId = blankToEmpty(root.jobId);
    if (accountId.isBlank() || rootJobId.isBlank()) {
      return new ReconcileCancellationMaintenanceService.CancellationCleanupResult(
          true, "", false, false, false);
    }
    String message = firstNonBlank(root.message, "Cancelled");
    int pageSize = Math.max(1, Math.min(100, childPageSize <= 0 ? 100 : childPageSize));
    String childPageToken = blankToEmpty(request.childPageToken());
    ReconcileJobIndexStore.StoredJobPage childPage =
        jobIndexStore().listStoredChildJobs(accountId, rootJobId, pageSize, childPageToken);
    List<StoredReconcileJob> children = childPage.records();
    String nextChildPageToken = blankToEmpty(childPage.nextPageToken());
    long now = System.currentTimeMillis();
    List<ReconcileJobIndexStore.CanonicalRecordMutation> pendingMutations = new ArrayList<>();
    List<StoredEnvelope> terminalChildrenToClear = new ArrayList<>();
    List<StoredReconcileJob> changedChildren = new ArrayList<>();
    int childMarkersRequested = 0;
    int delegatedNonTerminalChildren = 0;
    int blockingNonTerminalChildren = 0;
    boolean delegatedNonTerminalSeen = request.delegatedNonTerminalSeen();
    boolean blockingNonTerminalSeen = request.blockingNonTerminalSeen();
    for (StoredReconcileJob child : children) {
      if (child == null || !accountId.equals(blankToEmpty(child.accountId))) {
        continue;
      }
      boolean hasNonTerminalChildren =
          cancellationCleanupMayNeedDescendantState(child, now)
              && hasNonTerminalDirectChildren(child);
      StoredReconcileJob next =
          directChildCancellationState(child, message, now, hasNonTerminalChildren);
      StoredReconcileJob effectiveChild = next == null ? child : next;
      boolean requiresChildCleanup =
          requiresCancellationCleanupMarker(effectiveChild, hasNonTerminalChildren);
      if (requiresChildCleanup) {
        if (requestCancellationCleanup(effectiveChild.accountId, effectiveChild.jobId)) {
          childMarkersRequested++;
        }
      }
      if (!isTerminalState(effectiveChild.state)) {
        if (requiresChildCleanup) {
          delegatedNonTerminalChildren++;
          delegatedNonTerminalSeen = true;
        } else {
          blockingNonTerminalChildren++;
          blockingNonTerminalSeen = true;
        }
      }
      if (next == null || canonicalSchedulingEquivalent(child, next)) {
        continue;
      }
      Optional<ReconcileJobIndexStore.CanonicalRecordMutation> mutation =
          canonicalMutation(child, next);
      if (mutation.isEmpty()) {
        return new ReconcileCancellationMaintenanceService.CancellationCleanupResult(
            false,
            childPageToken,
            request.delegatedNonTerminalSeen(),
            request.blockingNonTerminalSeen(),
            false);
      }
      pendingMutations.add(mutation.get());
      changedChildren.add(next);
      if ("JS_CANCELLED".equals(blankToEmpty(next.state))) {
        terminalChildrenToClear.add(new StoredEnvelope(next.canonicalPointerKey, next));
      }
    }
    CancellationCommitStats commitStats =
        commitCancellationMutations(accountId, rootJobId, pendingMutations);
    if (!commitStats.success()) {
      return new ReconcileCancellationMaintenanceService.CancellationCleanupResult(
          false,
          childPageToken,
          request.delegatedNonTerminalSeen(),
          request.blockingNonTerminalSeen(),
          false);
    }
    terminalChildrenToClear.forEach(this::clearCancellationLeaseIfTerminal);
    for (StoredReconcileJob child : changedChildren) {
      markDirtyParentForRecord(child);
      cleanupAbandonedFullRescanStatsGenerationIfTerminal(child);
    }
    if (!nextChildPageToken.isBlank()) {
      LOG.infof(
          "reconcile cancellation cleanup root accountId=%s rootJobId=%s child_page_token=%s"
              + " next_child_page_token=%s children=%d child_markers_requested=%d mutations=%d"
              + " batches=%d write_items=%d delegated_non_terminal=%d blocking_non_terminal=%d"
              + " completed=false",
          accountId,
          rootJobId,
          childPageToken,
          nextChildPageToken,
          Integer.valueOf(children.size()),
          Integer.valueOf(childMarkersRequested),
          Integer.valueOf(commitStats.mutations()),
          Integer.valueOf(commitStats.batches()),
          Integer.valueOf(commitStats.writeItems()),
          Integer.valueOf(delegatedNonTerminalChildren),
          Integer.valueOf(blockingNonTerminalChildren));
      return new ReconcileCancellationMaintenanceService.CancellationCleanupResult(
          false, nextChildPageToken, delegatedNonTerminalSeen, blockingNonTerminalSeen, false);
    }
    boolean directChildrenTerminal = !delegatedNonTerminalSeen && !blockingNonTerminalSeen;
    boolean cleanupComplete =
        updateCancellationRootAfterDirectChildren(root, directChildrenTerminal, now);
    if (!directChildrenTerminal
        && !cleanupComplete
        && !blockingNonTerminalSeen
        && delegatedNonTerminalSeen) {
      LOG.infof(
          "reconcile cancellation cleanup root accountId=%s rootJobId=%s children=%d"
              + " child_markers_requested=%d mutations=%d batches=%d write_items=%d"
              + " delegated_non_terminal=%d blocking_non_terminal=%d"
              + " direct_children_terminal=%s completed=false paused=true",
          accountId,
          rootJobId,
          Integer.valueOf(children.size()),
          Integer.valueOf(childMarkersRequested),
          Integer.valueOf(commitStats.mutations()),
          Integer.valueOf(commitStats.batches()),
          Integer.valueOf(commitStats.writeItems()),
          Integer.valueOf(delegatedNonTerminalChildren),
          Integer.valueOf(blockingNonTerminalChildren),
          Boolean.valueOf(directChildrenTerminal));
      return new ReconcileCancellationMaintenanceService.CancellationCleanupResult(
          false, "", delegatedNonTerminalSeen, blockingNonTerminalSeen, true);
    }
    LOG.infof(
        "reconcile cancellation cleanup root accountId=%s rootJobId=%s children=%d"
            + " child_markers_requested=%d mutations=%d batches=%d write_items=%d"
            + " delegated_non_terminal=%d blocking_non_terminal=%d"
            + " direct_children_terminal=%s completed=%s",
        accountId,
        rootJobId,
        Integer.valueOf(children.size()),
        Integer.valueOf(childMarkersRequested),
        Integer.valueOf(commitStats.mutations()),
        Integer.valueOf(commitStats.batches()),
        Integer.valueOf(commitStats.writeItems()),
        Integer.valueOf(delegatedNonTerminalChildren),
        Integer.valueOf(blockingNonTerminalChildren),
        Boolean.valueOf(directChildrenTerminal),
        Boolean.valueOf(cleanupComplete));
    return new ReconcileCancellationMaintenanceService.CancellationCleanupResult(
        cleanupComplete, "", false, false, false);
  }

  private boolean requiresCancellationCleanupMarker(StoredReconcileJob child) {
    return requiresCancellationCleanupMarker(child, hasNonTerminalDirectChildren(child));
  }

  private boolean requiresCancellationCleanupMarker(
      StoredReconcileJob child, boolean hasNonTerminalChildren) {
    return child != null
        && isParentCapable(child.jobKind())
        && isCancellationState(child.state)
        && hasNonTerminalChildren;
  }

  private boolean cancellationCleanupMayNeedDescendantState(StoredReconcileJob child, long now) {
    if (child == null || !isParentCapable(child.jobKind())) {
      return false;
    }
    String state = blankToEmpty(child.state);
    if (!isCancellationState(state) && isTerminalState(state)) {
      return false;
    }
    if ("JS_RUNNING".equals(state)) {
      return !leaseManager().hasLiveLease(child, true, now);
    }
    return !("JS_CANCELLED".equals(state) && child.finishedAtMs > 0L && child.childrenFinalized);
  }

  private StoredReconcileJob directChildCancellationState(
      StoredReconcileJob child, String reason, long now, boolean hasNonTerminalChildren) {
    if (child == null) {
      return null;
    }
    String state = blankToEmpty(child.state);
    boolean terminal = isTerminalState(state);
    if (terminal && (!"JS_CANCELLED".equals(state) || !hasNonTerminalChildren)) {
      return null;
    }
    if ("JS_RUNNING".equals(state) && leaseManager().hasLiveLease(child, true, now)) {
      return null;
    }
    if ("JS_CANCELLING".equals(state) && leaseManager().hasLiveLease(child, true, now)) {
      return null;
    }
    StoredReconcileJob next = jobIndexStore().cloneStoredRecord(child);
    if (hasNonTerminalChildren) {
      if ("JS_CANCELLING".equals(state)) {
        return null;
      }
      next.state = "JS_CANCELLING";
      next.message = firstNonBlank(reason, child.message, "Cancelling");
      if (next.startedAtMs <= 0L) {
        next.startedAtMs = now;
      }
      next.finishedAtMs = 0L;
      next.childrenFinalized = true;
    } else {
      next.state = "JS_CANCELLED";
      next.message = firstNonBlank(reason, child.message, "Cancelled");
      if (next.startedAtMs <= 0L) {
        next.startedAtMs = now;
      }
      next.finishedAtMs = now;
      next.childrenFinalized = true;
    }
    markStatsCleanupPendingIfRequired(next, now);
    next.nextAttemptAtMs = 0L;
    next.readyPointerKey = null;
    next.updatedAtMs = now;
    return next;
  }

  private boolean updateCancellationRootAfterDirectChildren(
      StoredReconcileJob root, boolean directChildrenTerminal, long now) {
    if (root == null) {
      return true;
    }
    String state = blankToEmpty(root.state);
    if (!directChildrenTerminal) {
      if ("JS_CANCELLING".equals(state)) {
        return false;
      }
      StoredReconcileJob next = jobIndexStore().cloneStoredRecord(root);
      next.state = "JS_CANCELLING";
      next.message = firstNonBlank(root.message, "Cancelling");
      if (next.startedAtMs <= 0L) {
        next.startedAtMs = now;
      }
      next.finishedAtMs = 0L;
      next.childrenFinalized = true;
      next.nextAttemptAtMs = 0L;
      next.readyPointerKey = null;
      next.updatedAtMs = now;
      if (commitSingleCancellationMutation(root, next)) {
        markDirtyParentForRecord(next);
        upsertRootSummaryForRecord(next);
      }
      return false;
    }
    if ("JS_CANCELLING".equals(state) && leaseManager().hasLiveLease(root, true, now)) {
      return false;
    }
    if ("JS_CANCELLED".equals(state) && root.finishedAtMs > 0L) {
      StoredReconcileJob effectiveRoot = root;
      if (!root.childrenFinalized) {
        StoredReconcileJob next = jobIndexStore().cloneStoredRecord(root);
        next.childrenFinalized = true;
        markStatsCleanupPendingIfRequired(next, now);
        next.nextAttemptAtMs = 0L;
        next.readyPointerKey = null;
        next.updatedAtMs = now;
        if (!commitSingleCancellationMutation(root, next)) {
          return false;
        }
        markDirtyParentForRecord(next);
        upsertRootSummaryForRecord(next);
        effectiveRoot = next;
      }
      requestParentCancellationCleanupIfNeeded(effectiveRoot);
      return cleanupAbandonedFullRescanStatsGenerationIfTerminal(effectiveRoot);
    }
    StoredReconcileJob next = jobIndexStore().cloneStoredRecord(root);
    next.state = "JS_CANCELLED";
    next.message = firstNonBlank(root.message, "Cancelled");
    if (next.startedAtMs <= 0L) {
      next.startedAtMs = now;
    }
    next.finishedAtMs = now;
    next.childrenFinalized = true;
    markStatsCleanupPendingIfRequired(next, now);
    next.nextAttemptAtMs = 0L;
    next.readyPointerKey = null;
    next.updatedAtMs = now;
    if (!commitSingleCancellationMutation(root, next)) {
      return false;
    }
    clearCancellationLeaseIfTerminal(new StoredEnvelope(next.canonicalPointerKey, next));
    markDirtyParentForRecord(next);
    upsertRootSummaryForRecord(next);
    requestParentCancellationCleanupIfNeeded(next);
    return cleanupAbandonedFullRescanStatsGenerationIfTerminal(next);
  }

  private void requestParentCancellationCleanupIfNeeded(StoredReconcileJob child) {
    if (child == null || blankToEmpty(child.parentJobId).isBlank()) {
      return;
    }
    StoredEnvelope parent = loadByAnyAccount(child.parentJobId).orElse(null);
    if (parent == null
        || parent.record == null
        || !blankToEmpty(child.accountId).equals(blankToEmpty(parent.record.accountId))
        || !isCancellationState(parent.record.state)) {
      return;
    }
    if ("JS_CANCELLED".equals(blankToEmpty(parent.record.state))
        && parent.record.finishedAtMs > 0L
        && parent.record.childrenFinalized) {
      return;
    }
    requestCancellationCleanup(parent.record.accountId, parent.record.jobId);
  }

  private boolean commitSingleCancellationMutation(
      StoredReconcileJob previous, StoredReconcileJob current) {
    Optional<ReconcileJobIndexStore.CanonicalRecordMutation> mutation =
        canonicalMutation(previous, current);
    return mutation.isPresent()
        && commitCancellationMutations(previous.accountId, previous.jobId, List.of(mutation.get()))
            .success();
  }

  private Optional<ReconcileJobIndexStore.CanonicalRecordMutation> canonicalMutation(
      StoredReconcileJob previous, StoredReconcileJob current) {
    if (previous == null
        || current == null
        || blankToEmpty(previous.canonicalPointerKey).isBlank()) {
      return Optional.empty();
    }
    Optional<CanonicalPointerSnapshot> snapshot =
        jobIndexStore().loadCanonicalSnapshot(previous.canonicalPointerKey);
    if (snapshot.isEmpty()) {
      return Optional.empty();
    }
    current.canonicalPointerKey = previous.canonicalPointerKey;
    return Optional.of(
        new ReconcileJobIndexStore.CanonicalRecordMutation(
            snapshot.get(), jobIndexStore().cloneStoredRecord(previous), current));
  }

  private CancellationCommitStats commitCancellationMutations(
      String accountId,
      String rootJobId,
      List<ReconcileJobIndexStore.CanonicalRecordMutation> mutations) {
    if (mutations == null || mutations.isEmpty()) {
      return new CancellationCommitStats(true, 0, 0, 0);
    }
    List<ReconcileJobIndexStore.JobWritePlan<ReconcileJobIndexStore.CanonicalRecordMutation>>
        plans = new ArrayList<>();
    int maxWriteItems = jobIndexStore().maxWriteItemsPerBatch();
    for (ReconcileJobIndexStore.CanonicalRecordMutation mutation : mutations) {
      if (mutation == null || mutation.snapshot() == null || mutation.current() == null) {
        continue;
      }
      ReconcileJobIndexStore.JobIndexWriteBatch batch =
          jobIndexStore()
              .buildJobIndexWriteBatch(
                  mutation.snapshot(), mutation.previous(), mutation.current());
      int mutationWriteItems = jobIndexStore().writeItemCount(batch, List.of());
      if (mutationWriteItems > maxWriteItems) {
        LOG.warnf(
            "reconcile cancellation cleanup skipped oversized mutation accountId=%s rootJobId=%s"
                + " jobId=%s write_items=%d max_write_items=%d",
            blankToEmpty(accountId),
            blankToEmpty(rootJobId),
            blankToEmpty(mutation.current().jobId),
            Integer.valueOf(mutationWriteItems),
            Integer.valueOf(maxWriteItems));
        return new CancellationCommitStats(false, 0, 0, 0);
      }
      plans.add(new ReconcileJobIndexStore.JobWritePlan<>(mutation, batch, List.of()));
    }
    int batches = 0;
    int writeItems = 0;
    int committedMutations = 0;
    for (ReconcileJobIndexStore.JobWriteChunk<ReconcileJobIndexStore.CanonicalRecordMutation>
        chunk : jobIndexStore().chunkJobWritePlans(plans)) {
      int chunkWriteItems =
          jobIndexStore().writeItemCount(chunk.indexBatch(), chunk.extraPointerOps());
      boolean committed =
          jobIndexStore()
              .compareAndSetBatchWithPointerOps(chunk.indexBatch(), chunk.extraPointerOps());
      if (!committed) {
        LOG.warnf(
            "reconcile cancellation cleanup batch CAS failed accountId=%s rootJobId=%s"
                + " batch_mutations=%d batch_write_items=%d",
            blankToEmpty(accountId),
            blankToEmpty(rootJobId),
            Integer.valueOf(chunk.plans().size()),
            Integer.valueOf(chunkWriteItems));
        return new CancellationCommitStats(false, batches, committedMutations, writeItems);
      }
      batches++;
      committedMutations += chunk.plans().size();
      writeItems += chunkWriteItems;
    }
    return new CancellationCommitStats(true, batches, committedMutations, writeItems);
  }

  private record CancellationCommitStats(
      boolean success, int batches, int mutations, int writeItems) {}

  private boolean hasNonTerminalDirectChildren(StoredReconcileJob parent) {
    if (parent == null || !isParentCapable(parent.jobKind())) {
      return false;
    }
    for (StoredReconcileJob child : listAllStoredChildJobs(parent.accountId, parent.jobId)) {
      if (!isTerminalState(child.state)) {
        return true;
      }
    }
    return false;
  }

  private void clearCancellationLeaseIfTerminal(StoredEnvelope env) {
    if (env == null
        || env.record == null
        || !"JS_CANCELLED".equals(blankToEmpty(env.record.state))) {
      return;
    }
    leaseManager()
        .loadLease(env.record)
        .ifPresent(lease -> clearExecutionLeasesIfOwned(env, env.record.jobId, lease.epoch));
  }

  private void markDirtyParentForRecord(StoredReconcileJob record) {
    if (record == null) {
      return;
    }
    if (isParentCapable(record.jobKind())) {
      requestProjectionRefresh(record.accountId, record.jobId, 0L);
      return;
    }
    requestProjectionRefresh(record.accountId, record.parentJobId, 0L);
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

  private StoredReconcileJobListSummary repairStaleCancellationRootSummary(
      StoredReconcileJobListSummary summary) {
    if (summary == null || !"JS_CANCELLING".equals(blankToEmpty(summary.state()))) {
      return summary;
    }
    StoredEnvelope loaded = loadByAnyAccount(summary.jobId()).orElse(null);
    if (loaded == null
        || loaded.record == null
        || !blankToEmpty(summary.accountId()).equals(blankToEmpty(loaded.record.accountId))
        || !blankToEmpty(loaded.record.parentJobId).isBlank()
        || !"JS_CANCELLED".equals(blankToEmpty(loaded.record.state))) {
      return summary;
    }
    StoredReconcileJobListSummary repaired =
        toRootListSummary(loaded.record, storedProjectionForRead(loaded.record));
    if (!Objects.equals(summary, repaired)) {
      rootSummaries().upsert(repaired);
    }
    return repaired;
  }

  private void advanceCanonicalSchedulingAfterRecordChange(StoredReconcileJob changedRecord) {
    if (changedRecord == null) {
      return;
    }
    String nextParentJobId =
        isParentCapable(changedRecord.jobKind())
                && "JS_WAITING".equals(blankToEmpty(changedRecord.state))
            ? blankToEmpty(changedRecord.jobId)
            : blankToEmpty(changedRecord.parentJobId);
    String accountId = blankToEmpty(changedRecord.accountId);
    for (int depth = 0; depth < 64; depth++) {
      if (accountId.isBlank() || nextParentJobId.isBlank()) {
        return;
      }
      StoredReconcileJob advanced = advanceCanonicalParentIfComplete(accountId, nextParentJobId);
      if (advanced == null) {
        return;
      }
      markDirtyParentForRecord(advanced);
      upsertRootSummaryForRecord(advanced);
      if (!isTerminalState(advanced.state)) {
        return;
      }
      accountId = blankToEmpty(advanced.accountId);
      nextParentJobId = blankToEmpty(advanced.parentJobId);
    }
    LOG.warnf(
        "Stopped reconcile canonical parent advancement after depth cap accountId=%s jobId=%s",
        accountId, nextParentJobId);
  }

  private StoredReconcileJob advanceCanonicalParentIfComplete(
      String accountId, String parentJobId) {
    StoredEnvelope loaded = loadByAnyAccount(parentJobId).orElse(null);
    if (loaded == null
        || loaded.record == null
        || !blankToEmpty(accountId).equals(blankToEmpty(loaded.record.accountId))
        || !isParentCapable(loaded.record.jobKind())
        || leaseManager().hasLiveLease(loaded.record, true, System.currentTimeMillis())) {
      return null;
    }
    StoredReconcileJob parent = loaded.record;
    if (parent.jobKind() == ReconcileJobKind.PLAN_SNAPSHOT) {
      enqueueSnapshotFinalizationForPlanSnapshotIfEligible(
          projector().toPublicJob(parent, storedProjectionForRead(parent), true),
          snapshotTaskFromStored(parent));
      loaded = loadByAnyAccount(parentJobId).orElse(null);
      if (loaded == null || loaded.record == null) {
        return null;
      }
      parent = loaded.record;
    }
    List<StoredReconcileJob> directChildren =
        listAllStoredChildJobs(parent.accountId, parent.jobId);
    StoredReconcileJobProjection rollup =
        recomputeSummaryProjection(parent, directChildren, false, false);
    if (rollup == null) {
      return null;
    }
    if (shouldDeferPlanSnapshotSuccessUntilFinalizer(parent, rollup)) {
      enqueueSnapshotFinalizationForPlanSnapshotIfEligible(
          projector().toPublicJob(parent, rollup, true), snapshotTaskFromStored(parent));
      loaded = loadByAnyAccount(parentJobId).orElse(null);
      if (loaded == null || loaded.record == null) {
        return null;
      }
      parent = loaded.record;
      directChildren = listAllStoredChildJobs(parent.accountId, parent.jobId);
      rollup = recomputeSummaryProjection(parent, directChildren, false, false);
      if (rollup == null || shouldDeferPlanSnapshotSuccessUntilFinalizer(parent, rollup)) {
        return null;
      }
    }
    if (!isCanonicalParentSchedulingAdvance(parent, rollup)) {
      return null;
    }
    String expectedAccountId = blankToEmpty(parent.accountId);
    String expectedJobId = blankToEmpty(parent.jobId);
    AtomicReference<StoredReconcileJob> updated = new AtomicReference<>();
    jobIndexStore()
        .mutateByJobIdReturningRecord(
            expectedJobId,
            existing -> {
              if (existing == null
                  || !expectedAccountId.equals(blankToEmpty(existing.accountId))
                  || !expectedJobId.equals(blankToEmpty(existing.jobId))
                  || !isParentCapable(existing.jobKind())
                  || leaseManager().hasLiveLease(existing, true, System.currentTimeMillis())) {
                return null;
              }
              List<StoredReconcileJob> currentChildren =
                  listAllStoredChildJobs(existing.accountId, existing.jobId);
              StoredReconcileJobProjection currentRollup =
                  recomputeSummaryProjection(existing, currentChildren, false, false);
              if (currentRollup == null
                  || shouldDeferPlanSnapshotSuccessUntilFinalizer(existing, currentRollup)
                  || !isCanonicalParentSchedulingAdvance(existing, currentRollup)) {
                return null;
              }
              StoredReconcileJob next = jobIndexStore().cloneStoredRecord(existing);
              applySynchronousParentSchedulingState(next, currentRollup);
              if (canonicalSchedulingEquivalent(existing, next)) {
                return null;
              }
              next.updatedAtMs = System.currentTimeMillis();
              updated.set(jobIndexStore().cloneStoredRecord(next));
              return next;
            });
    StoredReconcileJob advanced = updated.get();
    cleanupAbandonedFullRescanStatsGenerationIfTerminal(advanced);
    return advanced;
  }

  private boolean shouldDeferPlanSnapshotSuccessUntilFinalizer(
      StoredReconcileJob parent, StoredReconcileJobProjection rollup) {
    if (parent == null
        || parent.jobKind() != ReconcileJobKind.PLAN_SNAPSHOT
        || !"JS_SUCCEEDED".equals(blankToEmpty(rollup == null ? "" : rollup.state()))) {
      return false;
    }
    ReconcileSnapshotTask snapshotTask = snapshotTaskFromStored(parent);
    if (snapshotTask.completionMode() != ReconcileSnapshotTask.CompletionMode.FILE_GROUPS
        || !snapshotTask.fileGroupPlanRecorded()
        || snapshotTask.fileGroupCount() <= 0L) {
      return false;
    }
    return listAllStoredChildJobs(parent.accountId, parent.jobId).stream()
        .noneMatch(
            child ->
                child != null
                    && child.jobKind() == ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE
                    && "JS_SUCCEEDED".equals(blankToEmpty(child.state)));
  }

  private boolean isCanonicalParentSchedulingAdvance(
      StoredReconcileJob parent, StoredReconcileJobProjection rollup) {
    if (parent == null || rollup == null || !isParentCapable(parent.jobKind())) {
      return false;
    }
    String state = blankToEmpty(rollup.state());
    if (!isTerminalState(state)) {
      return false;
    }
    if (isTerminalState(parent.state) && parent.finishedAtMs > 0L) {
      return false;
    }
    if (!parent.childrenFinalized) {
      return false;
    }
    if (Math.max(0L, parent.expectedDirectChildren) <= 0L) {
      return false;
    }
    if (hasLiveDirectChildLease(parent)) {
      return false;
    }
    return true;
  }

  private StoredReconcileJob applySynchronousParentSchedulingState(
      StoredReconcileJob existing,
      ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobProjection rollup) {
    if (existing == null || rollup == null || !isParentCapable(existing.jobKind())) {
      return existing;
    }
    if (isCancellationState(existing.state)) {
      existing.readyPointerKey = null;
      existing.nextAttemptAtMs = 0L;
      if ("JS_CANCELLING".equals(existing.state)
          && "JS_CANCELLED".equals(blankToEmpty(rollup.state()))) {
        existing.state = "JS_CANCELLED";
        existing.message = firstNonBlank(existing.message, rollup.message(), "Cancelled");
        existing.startedAtMs = projectedStartedAtMs(existing, rollup);
        existing.finishedAtMs = projectedFinishedAtMs(existing, rollup);
        existing.executorId = rollup.executorId();
        markStatsCleanupPendingIfRequired(existing, System.currentTimeMillis());
        return existing;
      }
      if (blank(existing.message)) {
        existing.message = "JS_CANCELLING".equals(existing.state) ? "Cancelling" : "Cancelled";
      }
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
    markStatsCleanupPendingIfRequired(existing, System.currentTimeMillis());
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
    if (hasCurrentTerminalProjection(record, storedProjection)) {
      return storedProjection;
    }
    if (!projectionNeedsRefresh(record, storedProjection)
        && projectionAggregateMatchesCanonical(record, storedProjection)) {
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
    if (refreshStaleProjection && hasCurrentTerminalProjection(record, projection)) {
      return copyProjectedAggregateSummaryRecord(record, projection);
    }
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
    if (hasCurrentTerminalProjection(record, projection)) {
      return false;
    }
    long requestedGeneration = Math.max(0L, record.projectionRequestedGeneration);
    long appliedGeneration = Math.max(0L, projection.appliedGeneration());
    if (requestedGeneration > appliedGeneration) {
      return true;
    }
    return shouldKeepCanonicalStateForTerminalProjection(record, projection);
  }

  private boolean projectionAggregateMatchesCanonical(
      StoredReconcileJob record, StoredReconcileJobProjection projection) {
    if (record == null || projection == null || !isParentCapable(record.jobKind())) {
      return true;
    }
    return Math.max(0L, record.tablesScanned) == Math.max(0L, projection.tablesScanned())
        && Math.max(0L, record.tablesChanged) == Math.max(0L, projection.tablesChanged())
        && Math.max(0L, record.viewsScanned) == Math.max(0L, projection.viewsScanned())
        && Math.max(0L, record.viewsChanged) == Math.max(0L, projection.viewsChanged())
        && Math.max(0L, record.statsProcessed) == Math.max(0L, projection.statsProcessed())
        && Math.max(0L, record.indexesProcessed) == Math.max(0L, projection.indexesProcessed())
        && Math.max(0L, record.errors) == Math.max(0L, projection.errors())
        && Math.max(0L, record.snapshotsProcessed) == Math.max(0L, projection.snapshotsProcessed())
        && Math.max(0L, record.plannedFileGroups) == Math.max(0L, projection.plannedFileGroups())
        && Math.max(0L, record.plannedFiles) == Math.max(0L, projection.plannedFiles())
        && Math.max(0L, record.completedFileGroups)
            == Math.max(0L, projection.completedFileGroups())
        && Math.max(0L, record.completedFiles) == Math.max(0L, projection.completedFiles())
        && Math.max(0L, record.failedFileGroups) == Math.max(0L, projection.failedFileGroups())
        && Math.max(0L, record.failedFiles) == Math.max(0L, projection.failedFiles());
  }

  private boolean hasCurrentTerminalProjection(
      StoredReconcileJob record,
      ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobProjection
          projection) {
    if (record == null || projection == null || !isParentCapable(record.jobKind())) {
      return false;
    }
    if (!isTerminalState(record.state)) {
      return false;
    }
    long requestedGeneration = Math.max(0L, record.projectionRequestedGeneration);
    long appliedGeneration = Math.max(0L, projection.appliedGeneration());
    return appliedGeneration >= requestedGeneration;
  }

  private boolean canPublishCanonicalAggregatePatch(
      StoredReconcileJob record, StoredReconcileJobProjection projection) {
    return canPublishCanonicalProjectionPatch(record, projection)
        && !projectionAggregateMatchesCanonical(record, projection);
  }

  private boolean canPublishCanonicalProjectionPatch(
      StoredReconcileJob record, StoredReconcileJobProjection projection) {
    if (record == null || projection == null || !isParentCapable(record.jobKind())) {
      return false;
    }
    String canonicalState = blankToEmpty(record.state);
    String projectionState = blankToEmpty(projection.state());
    if (canonicalState.isBlank()
        || isTerminalState(canonicalState)
        || isTerminalState(projectionState)
        || isCancellationState(canonicalState)) {
      return false;
    }
    return true;
  }

  private StoredReconcileJobProjection projectionWithCanonicalFields(
      StoredReconcileJob record, StoredReconcileJobProjection previous, long appliedGeneration) {
    return new StoredReconcileJobProjection(
        previous.accountId(),
        previous.jobId(),
        Math.max(Math.max(0L, appliedGeneration), Math.max(0L, previous.appliedGeneration())),
        previous.state(),
        previous.message(),
        previous.startedAtMs(),
        previous.finishedAtMs(),
        Math.max(0L, record.tablesScanned),
        Math.max(0L, record.tablesChanged),
        Math.max(0L, record.viewsScanned),
        Math.max(0L, record.viewsChanged),
        Math.max(0L, record.errors),
        Math.max(0L, record.snapshotsProcessed),
        Math.max(0L, record.statsProcessed),
        Math.max(0L, record.indexesProcessed),
        Math.max(0L, record.plannedFileGroups),
        Math.max(0L, record.plannedFiles),
        Math.max(0L, record.completedFileGroups),
        Math.max(0L, record.failedFileGroups),
        Math.max(0L, record.completedFiles),
        Math.max(0L, record.failedFiles),
        previous.executorId(),
        previous.aggregateSummaryPresent());
  }

  private boolean commitProjectionAggregateChange(
      StoredReconcileJob child,
      StoredReconcileJobProjection previousProjection,
      StoredReconcileJobProjection nextProjection) {
    if (child == null || nextProjection == null) {
      return true;
    }
    if (blankToEmpty(child.parentJobId).isBlank()
        || !projectionHasAggregateSignal(nextProjection)) {
      projections().upsert(nextProjection);
      return true;
    }
    long snapshotsDelta =
        projectionDelta(previousProjection, nextProjection, AggregateCounter.SNAPSHOTS);
    long statsDelta = projectionDelta(previousProjection, nextProjection, AggregateCounter.STATS);
    long indexesDelta =
        projectionDelta(previousProjection, nextProjection, AggregateCounter.INDEXES);
    long errorsDelta = projectionDelta(previousProjection, nextProjection, AggregateCounter.ERRORS);
    long plannedFileGroupsDelta =
        projectionDelta(previousProjection, nextProjection, AggregateCounter.PLANNED_FILE_GROUPS);
    long plannedFilesDelta =
        projectionDelta(previousProjection, nextProjection, AggregateCounter.PLANNED_FILES);
    long completedFileGroupsDelta =
        projectionDelta(previousProjection, nextProjection, AggregateCounter.COMPLETED_FILE_GROUPS);
    long failedFileGroupsDelta =
        projectionDelta(previousProjection, nextProjection, AggregateCounter.FAILED_FILE_GROUPS);
    long completedFilesDelta =
        projectionDelta(previousProjection, nextProjection, AggregateCounter.COMPLETED_FILES);
    long failedFilesDelta =
        projectionDelta(previousProjection, nextProjection, AggregateCounter.FAILED_FILES);
    if (statsDelta == 0L
        && indexesDelta == 0L
        && errorsDelta == 0L
        && snapshotsDelta == 0L
        && plannedFileGroupsDelta == 0L
        && plannedFilesDelta == 0L
        && completedFileGroupsDelta == 0L
        && failedFileGroupsDelta == 0L
        && completedFilesDelta == 0L
        && failedFilesDelta == 0L) {
      projections().upsert(nextProjection);
      return true;
    }
    var loadedParent = jobIndexStore().loadByAnyAccount(child.parentJobId).orElse(null);
    if (loadedParent == null
        || loadedParent.record() == null
        || !blankToEmpty(child.accountId).equals(blankToEmpty(loadedParent.record().accountId))
        || !isParentCapable(loadedParent.record().jobKind())) {
      return false;
    }
    boolean childSnapshotCountOwnedByTable =
        loadedParent.record().jobKind() == ReconcileJobKind.PLAN_TABLE
            && child.jobKind() == ReconcileJobKind.PLAN_SNAPSHOT;
    if (childSnapshotCountOwnedByTable) {
      snapshotsDelta = 0L;
    }
    if (statsDelta == 0L
        && indexesDelta == 0L
        && errorsDelta == 0L
        && snapshotsDelta == 0L
        && plannedFileGroupsDelta == 0L
        && plannedFilesDelta == 0L
        && completedFileGroupsDelta == 0L
        && failedFileGroupsDelta == 0L
        && completedFilesDelta == 0L
        && failedFilesDelta == 0L) {
      projections().upsert(nextProjection);
      requestProjectionRefresh(loadedParent.record().accountId, loadedParent.record().jobId, 0L);
      return true;
    }
    var parentSnapshot =
        jobIndexStore().loadCanonicalSnapshot(loadedParent.canonicalPointerKey()).orElse(null);
    if (parentSnapshot == null) {
      return false;
    }
    StoredReconcileJob previousParent = jobIndexStore().cloneStoredRecord(loadedParent.record());
    StoredReconcileJob nextParent = jobIndexStore().cloneStoredRecord(loadedParent.record());
    nextParent.snapshotsProcessed =
        Math.max(0L, Math.max(0L, nextParent.snapshotsProcessed) + snapshotsDelta);
    nextParent.statsProcessed = Math.max(0L, Math.max(0L, nextParent.statsProcessed) + statsDelta);
    nextParent.indexesProcessed =
        Math.max(0L, Math.max(0L, nextParent.indexesProcessed) + indexesDelta);
    nextParent.errors = Math.max(0L, Math.max(0L, nextParent.errors) + errorsDelta);
    nextParent.plannedFileGroups =
        Math.max(0L, Math.max(0L, nextParent.plannedFileGroups) + plannedFileGroupsDelta);
    nextParent.plannedFiles =
        Math.max(0L, Math.max(0L, nextParent.plannedFiles) + plannedFilesDelta);
    nextParent.completedFileGroups =
        Math.max(0L, Math.max(0L, nextParent.completedFileGroups) + completedFileGroupsDelta);
    nextParent.failedFileGroups =
        Math.max(0L, Math.max(0L, nextParent.failedFileGroups) + failedFileGroupsDelta);
    nextParent.completedFiles =
        Math.max(0L, Math.max(0L, nextParent.completedFiles) + completedFilesDelta);
    nextParent.failedFiles = Math.max(0L, Math.max(0L, nextParent.failedFiles) + failedFilesDelta);
    nextParent.updatedAtMs = System.currentTimeMillis();
    nextParent.canonicalPointerKey = loadedParent.canonicalPointerKey();
    var batch = jobIndexStore().buildJobIndexWriteBatch(parentSnapshot, previousParent, nextParent);
    if (!projections().upsertWithCanonicalMutation(previousProjection, nextProjection, batch)) {
      return false;
    }
    requestProjectionRefresh(nextParent.accountId, nextParent.jobId, 0L);
    upsertRootSummaryForRecord(nextParent);
    return true;
  }

  private void applyCanonicalAggregateFields(
      StoredReconcileJob record, StoredReconcileJobProjection aggregate) {
    record.snapshotsProcessed = aggregate.snapshotsProcessed();
    record.statsProcessed = aggregate.statsProcessed();
    record.indexesProcessed = aggregate.indexesProcessed();
    record.errors = aggregate.errors();
    record.plannedFileGroups = aggregate.plannedFileGroups();
    record.plannedFiles = aggregate.plannedFiles();
    record.completedFileGroups = aggregate.completedFileGroups();
    record.failedFileGroups = aggregate.failedFileGroups();
    record.completedFiles = aggregate.completedFiles();
    record.failedFiles = aggregate.failedFiles();
  }

  private boolean applyCanonicalAggregateProjection(
      StoredReconcileJob record, StoredReconcileJobProjection projection) {
    if (record == null || projection == null) {
      return true;
    }
    AtomicReference<StoredReconcileJob> updatedRecord = new AtomicReference<>();
    Optional<ReconcileJobIndexStore.CanonicalEnvelope> updated =
        jobIndexStore()
            .mutateByJobIdReturningRecord(
                record.jobId,
                existing -> {
                  if (existing == null
                      || !blankToEmpty(record.accountId).equals(blankToEmpty(existing.accountId))
                      || !isParentCapable(existing.jobKind())) {
                    return null;
                  }
                  applyCanonicalAggregateFields(existing, projection);
                  existing.updatedAtMs = System.currentTimeMillis();
                  updatedRecord.set(jobIndexStore().cloneStoredRecord(existing));
                  return existing;
                });
    if (updated.isEmpty()) {
      return false;
    }
    StoredReconcileJob updatedStoredRecord = updatedRecord.get();
    if (updatedStoredRecord != null) {
      upsertRootSummaryForRecord(updatedStoredRecord);
    }
    return true;
  }

  private boolean hasExactCanonicalProjectionMismatch(
      StoredReconcileJob record, StoredReconcileJobProjection projection) {
    if (record == null || projection == null || !isParentCapable(record.jobKind())) {
      return false;
    }
    return Math.max(0L, record.completedFileGroups)
            != Math.max(0L, projection.completedFileGroups())
        || Math.max(0L, record.failedFileGroups) != Math.max(0L, projection.failedFileGroups())
        || Math.max(0L, record.completedFiles) != Math.max(0L, projection.completedFiles())
        || Math.max(0L, record.failedFiles) != Math.max(0L, projection.failedFiles());
  }

  private long projectionDelta(
      StoredReconcileJobProjection previousProjection,
      StoredReconcileJobProjection nextProjection,
      AggregateCounter counter) {
    long previous =
        previousProjection == null ? 0L : Math.max(0L, counter.value(previousProjection));
    long next = nextProjection == null ? 0L : Math.max(0L, counter.value(nextProjection));
    return next - previous;
  }

  private enum AggregateCounter {
    STATS {
      @Override
      long value(StoredReconcileJobProjection projection) {
        return projection.statsProcessed();
      }
    },
    INDEXES {
      @Override
      long value(StoredReconcileJobProjection projection) {
        return projection.indexesProcessed();
      }
    },
    ERRORS {
      @Override
      long value(StoredReconcileJobProjection projection) {
        return projection.errors();
      }
    },
    SNAPSHOTS {
      @Override
      long value(StoredReconcileJobProjection projection) {
        return projection.snapshotsProcessed();
      }
    },
    PLANNED_FILE_GROUPS {
      @Override
      long value(StoredReconcileJobProjection projection) {
        return projection.plannedFileGroups();
      }
    },
    PLANNED_FILES {
      @Override
      long value(StoredReconcileJobProjection projection) {
        return projection.plannedFiles();
      }
    },
    COMPLETED_FILE_GROUPS {
      @Override
      long value(StoredReconcileJobProjection projection) {
        return projection.completedFileGroups();
      }
    },
    FAILED_FILE_GROUPS {
      @Override
      long value(StoredReconcileJobProjection projection) {
        return projection.failedFileGroups();
      }
    },
    COMPLETED_FILES {
      @Override
      long value(StoredReconcileJobProjection projection) {
        return projection.completedFiles();
      }
    },
    FAILED_FILES {
      @Override
      long value(StoredReconcileJobProjection projection) {
        return projection.failedFiles();
      }
    };

    abstract long value(StoredReconcileJobProjection projection);
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
    return recomputeSummaryProjection(
        record,
        record == null || !isParentCapable(record.jobKind())
            ? List.of()
            : listAllStoredChildJobs(record.accountId, record.jobId),
        refreshStaleDescendants,
        persistProjection);
  }

  private ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobProjection
      recomputeSummaryProjection(
          StoredReconcileJob record,
          List<StoredReconcileJob> directChildren,
          boolean refreshStaleDescendants,
          boolean persistProjection) {
    if (record == null || !isParentCapable(record.jobKind())) {
      return projectionFor(record).orElse(null);
    }
    List<StoredReconcileJob> summaryChildren =
        (directChildren == null ? List.<StoredReconcileJob>of() : directChildren)
            .stream()
                .map(
                    child ->
                        projectedSummaryRecordForParentRollup(
                            record, child, refreshStaleDescendants, persistProjection))
                .toList();
    return ancestorRollups().recomputeParentProjection(record, summaryChildren);
  }

  private StoredReconcileJob projectedSummaryRecordForParentRollup(
      StoredReconcileJob parent,
      StoredReconcileJob child,
      boolean refreshStaleProjection,
      boolean persistProjection) {
    if (child != null && refreshStaleProjection && isParentCapable(child.jobKind())) {
      var projection = projections().load(child.accountId, child.jobId).orElse(null);
      if (hasCurrentTerminalProjection(child, projection)) {
        StoredReconcileJob terminalSummary = copyProjectedAggregateSummaryRecord(child, projection);
        return projectedSummaryRecordForConnectorTable(parent, child, terminalSummary);
      }
      if (projectionNeedsRefresh(child, projection)
          || !projectionAggregateMatchesCanonical(child, projection)) {
        var refreshedProjection = recomputeSummaryProjection(child, true, persistProjection);
        if (persistProjection && refreshedProjection != null) {
          projections().upsert(refreshedProjection);
        }
        if (refreshedProjection != null) {
          StoredReconcileJob refreshedSummary =
              shouldKeepCanonicalStateForTerminalProjection(child, refreshedProjection)
                      || shouldKeepProjectionAggregatesForTerminalCanonicalState(
                          child, refreshedProjection)
                  ? copyProjectedAggregateSummaryRecord(child, refreshedProjection)
                  : copyProjectedSummaryRecord(child, refreshedProjection);
          return projectedSummaryRecordForConnectorTable(parent, child, refreshedSummary);
        }
      }
    }
    StoredReconcileJob summary =
        refreshStaleProjection
            ? projectedSummaryRecord(child, true, persistProjection)
            : projectedSummaryRecord(child);
    return projectedSummaryRecordForConnectorTable(parent, child, summary);
  }

  private StoredReconcileJob projectedSummaryRecordForConnectorTable(
      StoredReconcileJob parent, StoredReconcileJob child, StoredReconcileJob summary) {
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
    String summaryState = rootSummaryState(record, summary.state);
    String summaryMessage =
        summaryState.equals(blankToEmpty(record == null ? "" : record.state))
            ? blankToEmpty(record.message)
            : summary.message;
    long summaryFinishedAtMs =
        isTerminalState(summaryState)
            ? Math.max(Math.max(0L, summary.finishedAtMs), Math.max(0L, record.finishedAtMs))
            : summary.finishedAtMs;
    return new StoredReconcileJobListSummary(
        summary.accountId,
        summary.jobId,
        summary.connectorId,
        summaryState,
        summaryMessage,
        summary.startedAtMs,
        summaryFinishedAtMs,
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

  private String rootSummaryState(StoredReconcileJob record, String projectedState) {
    String canonicalState = blankToEmpty(record == null ? "" : record.state);
    if (isTerminalState(canonicalState) || "JS_CANCELLING".equals(canonicalState)) {
      return canonicalState;
    }
    return blankToEmpty(projectedState);
  }

  private static long fileGroupStatsProcessed(ReconcileFileGroupTask fileGroupTask) {
    if (fileGroupTask == null || fileGroupTask.isEmpty()) {
      return 0L;
    }
    long fileResultStats =
        fileGroupTask.fileResults().stream()
            .filter(result -> result != null && !result.isEmpty())
            .mapToLong(ReconcileFileResult::statsProcessed)
            .sum();
    return Math.max(fileResultStats, Math.max(0, fileGroupTask.fileStatsRecordCount()));
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
    if (isObsoleteDirtyParentProjection(effectiveAccountId, effectiveParentJobId)) {
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

  private void enqueueSnapshotFinalizationForPlanSnapshotIfEligible(
      ReconcileJob parent, ReconcileSnapshotTask snapshotTask) {
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
      ReconcileJobPage page = childJobsPage(parent.accountId, parent.jobId, 200, pageToken);
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
              parent.accountId, parent.jobId, key);
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

  private ReconcileSnapshotTask snapshotTaskFromStored(StoredReconcileJob record) {
    if (record == null || blankToEmpty(record.snapshotTaskTableId).isBlank()) {
      return ReconcileSnapshotTask.empty();
    }
    return ReconcileSnapshotTask.of(
        blankToEmpty(record.snapshotTaskTableId),
        record.snapshotTaskSnapshotId,
        blankToEmpty(record.snapshotTaskSourceNamespace),
        blankToEmpty(record.snapshotTaskSourceTable),
        List.of(),
        record.snapshotTaskFileGroupPlanRecorded,
        ReconcileSnapshotTask.CompletionMode.fromString(record.snapshotTaskCompletionMode),
        blankToEmpty(record.snapshotPlanBlobUri),
        (int) Math.min(Integer.MAX_VALUE, Math.max(0L, record.plannedFileGroups)),
        Math.max(0, record.snapshotTaskSourceFileCount),
        blankToEmpty(record.snapshotTaskDirectStatsBlobUri),
        Math.max(0, record.snapshotTaskDirectStatsRecordCount),
        record.snapshotTaskDirectStatsPersistedRecordCountsByChunk == null
            ? Map.of()
            : record.snapshotTaskDirectStatsPersistedRecordCountsByChunk);
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

  private void observeLeaseScanPermitGauges() {
    if (observability == null) {
      return;
    }
    observability.gauge(
        ServiceMetrics.Reconcile.LEASE_SCAN_PERMITS_IN_USE,
        () -> Math.max(0, leaseMaxConcurrency - leaseScanPermits.availablePermits()),
        "Current number of reconcile lease scan permits in use",
        Tag.of(TagKey.COMPONENT, "service"),
        Tag.of(TagKey.OPERATION, "lease_scan"));
    observability.gauge(
        ServiceMetrics.Reconcile.LEASE_SCAN_PERMITS_AVAILABLE,
        () -> Math.max(0, leaseScanPermits.availablePermits()),
        "Current number of reconcile lease scan permits available",
        Tag.of(TagKey.COMPONENT, "service"),
        Tag.of(TagKey.OPERATION, "lease_scan"));
  }

  private void observeLeaseNext(long startedAtMs, LeaseScanStats stats, String outcome) {
    if (observability == null) {
      return;
    }
    LeaseScanStats effective = stats == null ? new LeaseScanStats() : stats;
    String result = blankToEmpty(outcome);
    if (result.isBlank()) {
      result = "unknown";
    }
    Tag component = Tag.of(TagKey.COMPONENT, "service");
    Tag operation = Tag.of(TagKey.OPERATION, "lease_next");
    Tag resultTag = Tag.of(TagKey.RESULT, result);
    long elapsedMs = Math.max(0L, System.currentTimeMillis() - startedAtMs);
    observability.timer(
        ServiceMetrics.Reconcile.LEASE_NEXT_LATENCY,
        Duration.ofMillis(elapsedMs),
        component,
        operation,
        resultTag);
    observability.summary(
        ServiceMetrics.Reconcile.LEASE_NEXT_CANDIDATES,
        effective.candidateCount,
        component,
        operation,
        resultTag);
    observability.summary(
        ServiceMetrics.Reconcile.LEASE_NEXT_SCANS,
        effective.scanCount,
        component,
        operation,
        resultTag);
    observability.counter(
        ServiceMetrics.Reconcile.LEASE_NEXT_OUTCOMES, 1.0, component, operation, resultTag);
    effective
        .skipCounts()
        .forEach((reason, count) -> observeLeaseNextSkip(component, operation, reason, count));
  }

  private void observeLeaseNextSkip(Tag component, Tag operation, String reason, int count) {
    if (count <= 0) {
      return;
    }
    observability.counter(
        ServiceMetrics.Reconcile.LEASE_NEXT_SKIPS,
        count,
        component,
        operation,
        Tag.of(TagKey.REASON, reason));
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

  private boolean isObsoleteDirtyParentProjection(String accountId, String parentJobId) {
    if (blankToEmpty(accountId).isBlank() || blankToEmpty(parentJobId).isBlank()) {
      return true;
    }
    StoredEnvelope loaded = loadByAnyAccount(parentJobId).orElse(null);
    if (loaded == null || loaded.record == null) {
      return true;
    }
    StoredReconcileJob record = loaded.record;
    if (!blankToEmpty(accountId).equals(blankToEmpty(record.accountId))
        || !isParentCapable(record.jobKind())) {
      return true;
    }
    if ("JS_CANCELLED".equals(blankToEmpty(record.state))) {
      return !hasNonTerminalDirectChildren(record);
    }
    return isBlockedByAncestorCancellation(record);
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
            statsProcessed,
            0L);
    if (accepted && completionKind == CompletionKind.SUCCEEDED) {
      recordFinalizedSnapshotIfEligible(jobId);
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
      long statsProcessed,
      long indexesProcessed) {
    CompletionKind nextCompletionKind = completionKind;
    String nextMessage = message;
    if (isRetryableCompletion(completionKind) && isJobBlockedByAncestorCancellation(jobId)) {
      nextCompletionKind = CompletionKind.CANCELLED;
      nextMessage = blank(message) ? "Cancelled" : message;
    }
    final CompletionKind effectiveCompletionKind = nextCompletionKind;
    final String effectiveMessage = nextMessage;
    if (applyLeaseOutcomeDirectly(
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
        statsProcessed,
        indexesProcessed)) {
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
        statsProcessed,
        indexesProcessed)) {
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
      long statsProcessed,
      long indexesProcessed) {
    Optional<StoredEnvelope> updated =
        mutateByJobIdReturningRecord(
            jobId,
            existing -> {
              if ((completionKind == CompletionKind.SUCCEEDED
                      || completionKind == CompletionKind.SUCCEEDED_WAITING)
                  && !blankToEmpty(existing.plannerOutcomeFingerprint).isBlank()
                  && !"JS_CANCELLING".equals(blankToEmpty(existing.state))) {
                throw new IllegalStateException(
                    "planner outcome replay does not match the committed child set for job "
                        + jobId);
              }
              return applyLeaseOutcomeToRecord(
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
                  statsProcessed,
                  indexesProcessed);
            });
    updated.ifPresent(
        env -> {
          clearExecutionLeasesIfOwned(env, jobId, leaseEpoch);
          markDirtyParentForRecord(env.record);
          upsertRootSummaryForRecord(env.record);
          advanceCanonicalSchedulingAfterRecordChange(env.record);
        });
    return updated.isPresent();
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
      long statsProcessed,
      long indexesProcessed) {
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
        statsProcessed,
        indexesProcessed);
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
      long statsProcessed,
      long indexesProcessed) {
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
              && record.statsProcessed >= Math.max(0L, statsProcessed)
              && record.indexesProcessed >= Math.max(0L, indexesProcessed);
      case SUCCEEDED ->
          "JS_SUCCEEDED".equals(blankToEmpty(record.state))
              && record.finishedAtMs > 0L
              && record.tablesScanned >= Math.max(0L, tablesScanned)
              && record.tablesChanged >= Math.max(0L, tablesChanged)
              && record.viewsScanned >= Math.max(0L, viewsScanned)
              && record.viewsChanged >= Math.max(0L, viewsChanged)
              && record.snapshotsProcessed >= Math.max(0L, snapshotsProcessed)
              && record.statsProcessed >= Math.max(0L, statsProcessed)
              && record.indexesProcessed >= Math.max(0L, indexesProcessed);
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
              && record.statsProcessed >= Math.max(0L, statsProcessed)
              && record.indexesProcessed >= Math.max(0L, indexesProcessed);
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
              && record.statsProcessed >= Math.max(0L, statsProcessed)
              && record.indexesProcessed >= Math.max(0L, indexesProcessed);
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
      long statsProcessed,
      long indexesProcessed) {
    String op = operationName(completionKind);
    boolean allowExpiredWithinGrace =
        completionKind == CompletionKind.SUCCEEDED_WAITING
            || completionKind == CompletionKind.SUCCEEDED
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
      nextChild.indexesProcessed = Math.max(nextChild.indexesProcessed, indexesProcessed);
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
              statsProcessed,
              indexesProcessed);
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
              statsProcessed,
              indexesProcessed);
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
              statsProcessed,
              indexesProcessed);
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
              statsProcessed,
              indexesProcessed);
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
              statsProcessed,
              indexesProcessed);
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
              statsProcessed,
              indexesProcessed);
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
      long statsProcessed,
      long indexesProcessed) {
    existing.tablesScanned = Math.max(existing.tablesScanned, tablesScanned);
    existing.tablesChanged = Math.max(existing.tablesChanged, tablesChanged);
    existing.viewsScanned = Math.max(existing.viewsScanned, viewsScanned);
    existing.viewsChanged = Math.max(existing.viewsChanged, viewsChanged);
    existing.errors = 0L;
    existing.snapshotsProcessed = Math.max(existing.snapshotsProcessed, snapshotsProcessed);
    existing.statsProcessed = Math.max(existing.statsProcessed, statsProcessed);
    existing.indexesProcessed = Math.max(existing.indexesProcessed, indexesProcessed);
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
      long statsProcessed,
      long indexesProcessed) {
    existing.attempt = Math.max(0, existing.attempt) + 1;
    existing.tablesScanned = Math.max(existing.tablesScanned, tablesScanned);
    existing.tablesChanged = Math.max(existing.tablesChanged, tablesChanged);
    existing.viewsScanned = Math.max(existing.viewsScanned, viewsScanned);
    existing.viewsChanged = Math.max(existing.viewsChanged, viewsChanged);
    existing.errors = errors;
    existing.snapshotsProcessed = Math.max(existing.snapshotsProcessed, snapshotsProcessed);
    existing.statsProcessed = Math.max(existing.statsProcessed, statsProcessed);
    existing.indexesProcessed = Math.max(existing.indexesProcessed, indexesProcessed);
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
      long statsProcessed,
      long indexesProcessed) {
    existing.tablesScanned = Math.max(existing.tablesScanned, tablesScanned);
    existing.tablesChanged = Math.max(existing.tablesChanged, tablesChanged);
    existing.viewsScanned = Math.max(existing.viewsScanned, viewsScanned);
    existing.viewsChanged = Math.max(existing.viewsChanged, viewsChanged);
    existing.errors = errors;
    existing.snapshotsProcessed = Math.max(existing.snapshotsProcessed, snapshotsProcessed);
    existing.statsProcessed = Math.max(existing.statsProcessed, statsProcessed);
    existing.indexesProcessed = Math.max(existing.indexesProcessed, indexesProcessed);
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
      long statsProcessed,
      long indexesProcessed) {
    existing.attempt = Math.max(0, existing.attempt) + 1;
    existing.tablesScanned = Math.max(existing.tablesScanned, tablesScanned);
    existing.tablesChanged = Math.max(existing.tablesChanged, tablesChanged);
    existing.viewsScanned = Math.max(existing.viewsScanned, viewsScanned);
    existing.viewsChanged = Math.max(existing.viewsChanged, viewsChanged);
    existing.errors = errors;
    existing.snapshotsProcessed = Math.max(existing.snapshotsProcessed, snapshotsProcessed);
    existing.statsProcessed = Math.max(existing.statsProcessed, statsProcessed);
    existing.indexesProcessed = Math.max(existing.indexesProcessed, indexesProcessed);
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
      long statsProcessed,
      long indexesProcessed) {
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
    existing.indexesProcessed = Math.max(existing.indexesProcessed, indexesProcessed);
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

  private boolean cleanupAbandonedFullRescanStatsGenerationIfTerminal(StoredReconcileJob job) {
    if (statsStore == null
        || job == null
        || job.jobKind() != ReconcileJobKind.PLAN_SNAPSHOT
        || !job.fullRescan
        || !job.childrenFinalized
        || !failedOrCancelled(job.state)
        || STATS_CLEANUP_COMPLETED.equals(blankToEmpty(job.statsCleanupState))
        || blankToEmpty(job.snapshotTaskTableId).isBlank()
        || job.snapshotTaskSnapshotId < 0L
        || blankToEmpty(job.accountId).isBlank()
        || blankToEmpty(job.jobId).isBlank()) {
      return true;
    }
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(blankToEmpty(job.accountId))
            .setKind(ResourceKind.RK_TABLE)
            .setId(blankToEmpty(job.snapshotTaskTableId))
            .build();
    String generationId = "full-rescan-" + blankToEmpty(job.jobId);
    try {
      UnpublishedGenerationDeleteResult deleteResult =
          statsStore.deleteUnpublishedStatsGeneration(
              tableId, job.snapshotTaskSnapshotId, generationId);
      if (deleteResult == UnpublishedGenerationDeleteResult.RETRYABLE_IN_PROGRESS) {
        LOG.infof(
            "Deferred abandoned full-rescan stats generation cleanup accountId=%s jobId=%s"
                + " tableId=%s snapshotId=%d generationId=%s state=%s result=%s",
            job.accountId,
            job.jobId,
            tableId.getId(),
            Long.valueOf(job.snapshotTaskSnapshotId),
            generationId,
            blankToEmpty(job.state),
            deleteResult);
        return false;
      }
      markStatsCleanupCompleted(job.accountId, job.jobId);
      if (deleteResult == UnpublishedGenerationDeleteResult.DELETED) {
        LOG.infof(
            "Deleted abandoned full-rescan stats generation accountId=%s jobId=%s tableId=%s"
                + " snapshotId=%d generationId=%s state=%s",
            job.accountId,
            job.jobId,
            tableId.getId(),
            Long.valueOf(job.snapshotTaskSnapshotId),
            generationId,
            blankToEmpty(job.state));
      }
      return true;
    } catch (RuntimeException e) {
      LOG.warnf(
          e,
          "Failed to delete abandoned full-rescan stats generation accountId=%s jobId=%s tableId=%s"
              + " snapshotId=%d generationId=%s state=%s",
          job.accountId,
          job.jobId,
          tableId.getId(),
          Long.valueOf(job.snapshotTaskSnapshotId),
          generationId,
          blankToEmpty(job.state));
      return false;
    }
  }

  private void runAbandonedFullRescanStatsCleanupMaintenanceOnce(long maxMillis) {
    if (statsStore == null) {
      return;
    }
    long startedAtMs = System.currentTimeMillis();
    long deadlineMs = maxMillis <= 0L ? startedAtMs : startedAtMs + Math.max(1L, maxMillis);
    int scanned = 0;
    int attempted = 0;
    int completed = 0;
    for (String state : List.of("JS_FAILED", "JS_CANCELLED")) {
      String token = "";
      while (true) {
        if (System.currentTimeMillis() > deadlineMs) {
          logStatsCleanupMaintenance(startedAtMs, scanned, attempted, completed, false);
          return;
        }
        ReconcileJobIndexStore.StoredJobPage page =
            jobIndexStore().listStoredJobsInState(state, readyScanLimit, token);
        for (StoredReconcileJob job : page.records()) {
          scanned++;
          if (!needsAbandonedFullRescanStatsCleanup(job)) {
            continue;
          }
          attempted++;
          if (cleanupAbandonedFullRescanStatsGenerationIfTerminal(job)) {
            completed++;
          }
        }
        String nextToken = blankToEmpty(page.nextPageToken());
        if (nextToken.isBlank() || nextToken.equals(token)) {
          break;
        }
        token = nextToken;
      }
    }
    logStatsCleanupMaintenance(startedAtMs, scanned, attempted, completed, true);
  }

  private void logStatsCleanupMaintenance(
      long startedAtMs, int scanned, int attempted, int completed, boolean finished) {
    long elapsedMs = System.currentTimeMillis() - startedAtMs;
    if (attempted <= 0 && elapsedMs <= 500L) {
      LOG.debugf(
          "runAbandonedFullRescanStatsCleanupMaintenanceOnce total_ms=%d completed=%s",
          Long.valueOf(elapsedMs), Boolean.valueOf(finished));
      return;
    }
    LOG.infof(
        "runAbandonedFullRescanStatsCleanupMaintenanceOnce total_ms=%d completed=%s"
            + " scanned=%d attempted=%d cleaned=%d",
        Long.valueOf(elapsedMs),
        Boolean.valueOf(finished),
        Integer.valueOf(scanned),
        Integer.valueOf(attempted),
        Integer.valueOf(completed));
  }

  private boolean needsAbandonedFullRescanStatsCleanup(StoredReconcileJob job) {
    return job != null
        && job.jobKind() == ReconcileJobKind.PLAN_SNAPSHOT
        && job.fullRescan
        && job.childrenFinalized
        && failedOrCancelled(job.state)
        && !STATS_CLEANUP_COMPLETED.equals(blankToEmpty(job.statsCleanupState))
        && !blankToEmpty(job.snapshotTaskTableId).isBlank()
        && job.snapshotTaskSnapshotId >= 0L
        && !blankToEmpty(job.accountId).isBlank()
        && !blankToEmpty(job.jobId).isBlank();
  }

  private void markStatsCleanupPendingIfRequired(StoredReconcileJob job, long now) {
    if (job == null || !needsStatsCleanupMarker(job)) {
      return;
    }
    if (!STATS_CLEANUP_COMPLETED.equals(blankToEmpty(job.statsCleanupState))) {
      job.statsCleanupState = STATS_CLEANUP_PENDING;
      job.statsCleanupUpdatedAtMs = now;
    }
  }

  private boolean needsStatsCleanupMarker(StoredReconcileJob job) {
    return job != null
        && job.jobKind() == ReconcileJobKind.PLAN_SNAPSHOT
        && job.fullRescan
        && job.childrenFinalized
        && failedOrCancelled(job.state)
        && !blankToEmpty(job.snapshotTaskTableId).isBlank()
        && job.snapshotTaskSnapshotId >= 0L
        && !blankToEmpty(job.accountId).isBlank()
        && !blankToEmpty(job.jobId).isBlank();
  }

  private void markStatsCleanupCompleted(String accountId, String jobId) {
    String effectiveAccountId = blankToEmpty(accountId);
    String effectiveJobId = blankToEmpty(jobId);
    if (effectiveAccountId.isBlank() || effectiveJobId.isBlank()) {
      return;
    }
    jobIndexStore()
        .mutateByJobIdReturningRecord(
            effectiveJobId,
            existing -> {
              if (existing == null
                  || !effectiveAccountId.equals(blankToEmpty(existing.accountId))
                  || !effectiveJobId.equals(blankToEmpty(existing.jobId))
                  || !needsStatsCleanupMarker(existing)) {
                return null;
              }
              if (STATS_CLEANUP_COMPLETED.equals(blankToEmpty(existing.statsCleanupState))) {
                return null;
              }
              StoredReconcileJob next = jobIndexStore().cloneStoredRecord(existing);
              next.statsCleanupState = STATS_CLEANUP_COMPLETED;
              next.statsCleanupUpdatedAtMs = System.currentTimeMillis();
              next.updatedAtMs = next.statsCleanupUpdatedAtMs;
              return next;
            });
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

  private static boolean failedOrCancelled(String state) {
    return "JS_FAILED".equals(blankToEmpty(state)) || "JS_CANCELLED".equals(blankToEmpty(state));
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

  private static String firstNonBlank(String... values) {
    if (values == null) {
      return "";
    }
    for (String value : values) {
      if (!blank(value)) {
        return value.trim();
      }
    }
    return "";
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
