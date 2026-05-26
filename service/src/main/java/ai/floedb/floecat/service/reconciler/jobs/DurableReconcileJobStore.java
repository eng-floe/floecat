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
import ai.floedb.floecat.service.reconciler.jobs.durable.queue.ReconcileJobLister;
import ai.floedb.floecat.service.reconciler.jobs.durable.queue.ReconcileJobMaintenanceService;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileJobDetailLoader;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileJobExecutionLoader;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileJobIndexes;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileLeaseStateCodec;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcilePayloadStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.DynamoReconcileJobIndexBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.DynamoReconcileLeaseBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.DynamoReconcileReadyQueueBackend;
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
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

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
  private static final Logger LOG = Logger.getLogger(DurableReconcileJobStore.class);

  private static final int DEFAULT_MAX_ATTEMPTS = 8;
  private static final long DEFAULT_BASE_BACKOFF_MS = 500L;
  private static final long DEFAULT_MAX_BACKOFF_MS = 30_000L;
  private static final long DEFAULT_LEASE_MS = 30_000L;
  private static final long DEFAULT_RECLAIM_INTERVAL_MS = 5_000L;
  private static final long DEFAULT_LEASE_RENEW_GRACE_MS = 5_000L;
  private static final long CANCEL_POKE_MAX_DELAY_MS = 1_000L;
  private static final int DEFAULT_READY_SCAN_LIMIT = 128;
  private static final int CAS_MAX = 16;
  @Inject PointerStore pointerStore;
  @Inject BlobStore blobStore;
  @Inject ObjectMapper mapper;
  @Inject Config config;
  @Inject Instance<DynamoDbClient> dynamoDb;

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
  @Inject ReconcileJobLister lister;
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
  private int maxAttempts = DEFAULT_MAX_ATTEMPTS;
  private long baseBackoffMs = DEFAULT_BASE_BACKOFF_MS;
  private long maxBackoffMs = DEFAULT_MAX_BACKOFF_MS;
  private long leaseMs = DEFAULT_LEASE_MS;
  private long reclaimIntervalMs = DEFAULT_RECLAIM_INTERVAL_MS;
  private long leaseRenewGraceMs = DEFAULT_LEASE_RENEW_GRACE_MS;
  private int readyScanLimit = DEFAULT_READY_SCAN_LIMIT;

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
        && dynamoDb != null
        && dynamoDb.isResolvable()) {
      dynamoBackend.bind(dynamoDb.get(), kvTable);
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

  private ReconcileJobLister lister() {
    if (lister == null) {
      lister = new ReconcileJobLister();
    }
    lister.bind(jobIndexStore(), projector(), projections());
    return lister;
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
        && dynamoDb != null
        && dynamoDb.isResolvable()) {
      dynamoBackend.bind(dynamoDb.get(), kvTable);
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
        && dynamoDb != null
        && dynamoDb.isResolvable()) {
      dynamoBackend.bind(dynamoDb.get(), kvTable);
    }
    readyQueueStore.bind(
        readyQueueBackend,
        jobIndexStore(),
        leaseManager(),
        readyScanLimit,
        DurableReconcileJobStore::requiresReadyPointer);
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
                            env.canonicalPointerKey, env.record)),
        this::backoffMs,
        (record, dueAtMs) -> readyPointerKeyFor(record, dueAtMs.longValue()),
        (env, jobId, leaseEpoch) ->
            clearExecutionLeasesIfOwned(
                new StoredEnvelope(env.canonicalPointerKey(), env.record()), jobId, leaseEpoch),
        maxAttempts,
        baseBackoffMs);
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
    return bulkEnqueue(
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
                    pinnedExecutorId)))
        .singleJobId();
  }

  @Override
  public BulkEnqueueResult bulkEnqueue(List<BulkEnqueueSpec> specs) {
    BulkEnqueueResult result = onHotPath(() -> enqueuer().bulkEnqueue(specs));
    List<BulkEnqueueSpec> effectiveSpecs = specs == null ? List.of() : specs;
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
      if (isParentCapable(spec.jobKind)) {
        markDirtyParent(spec.accountId, item.jobId);
      }
      markDirtyParent(spec.accountId, spec.parentJobId);
    }
    return result;
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
            .toPublicJob(
                loaded.get().record, projectionFor(loaded.get().record).orElse(null), true));
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
    return lister().list(accountId, pageSize, pageToken, connectorId, states);
  }

  @Override
  public ReconcileJobPage listRootJobs(
      String accountId,
      int pageSize,
      String pageToken,
      String connectorId,
      java.util.Set<String> states) {
    var page = rootSummaries().listSummaries(accountId, pageSize, pageToken, connectorId, states);
    List<ReconcileJob> jobs = new java.util.ArrayList<>(page.summaries().size());
    for (StoredReconcileJobListSummary summary : page.summaries()) {
      jobs.add(ReconcileJobRootSummaryStore.toPublicJob(summary));
    }
    return new ReconcileJobPage(jobs, page.nextPageToken());
  }

  @Override
  public ReconcileJobPage childJobsPage(
      String accountId, String parentJobId, int pageSize, String pageToken) {
    return lister().childJobsPage(accountId, parentJobId, pageSize, pageToken);
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
            .toPublicTreeJob(rootEnvelope.record, projectionFor(rootEnvelope.record).orElse(null)));
    pendingParents.add(rootJobId);
    while (!pendingParents.isEmpty()) {
      String parentJobId = pendingParents.removeFirst();
      String nextToken = "";
      do {
        var page =
            jobIndexStore()
                .listStoredChildJobs(rootEnvelope.record.accountId, parentJobId, 1000, nextToken);
        for (StoredReconcileJob stored : page.records()) {
          out.add(projector().toPublicTreeJob(stored, projectionFor(stored).orElse(null)));
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
    long queuedCount = Math.max(0, jobIndexStore().countStoredJobsInState("JS_QUEUED"));
    long waitingCount = Math.max(0, jobIndexStore().countStoredJobsInState("JS_WAITING"));
    long queued = queuedCount + waitingCount;
    long running = Math.max(0, jobIndexStore().countStoredJobsInState("JS_RUNNING"));
    long cancelling = Math.max(0, jobIndexStore().countStoredJobsInState("JS_CANCELLING"));
    long oldestQueued =
        firstPositiveMin(
            queuedCount > 0 ? jobIndexStore().oldestStoredJobTimestampInState("JS_QUEUED") : 0L,
            waitingCount > 0 ? jobIndexStore().oldestStoredJobTimestampInState("JS_WAITING") : 0L);
    return new QueueStats(queued, running, cancelling, oldestQueued);
  }

  @Override
  public Optional<LeasedJob> leaseNext(LeaseRequest request) {
    LeaseRequest effective = request == null ? LeaseRequest.all() : request;
    long startedAtMs = System.currentTimeMillis();
    LeaseScanStats scanStats = new LeaseScanStats();
    var leased = leaseReadyDue(startedAtMs, effective, scanStats);
    LOG.debugf(
        "leaseNext total_ms=%d scan_count=%d candidate_count=%d leased=%s",
        System.currentTimeMillis() - startedAtMs,
        scanStats.scanCount,
        scanStats.candidateCount,
        leased.isPresent());
    return leased;
  }

  void runMaintenanceOnce(long maxMillis) {
    maintenance().runMaintenanceOnce(maxMillis);
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
                    existing.snapshotTaskDirectStatsBlobUri =
                        blankToEmpty(effective.directStatsBlobUri());
                    existing.snapshotTaskDirectStatsRecordCount =
                        effective.directStatsRecordCount();
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
        && blankToEmpty(currentState.snapshotTaskDirectStatsBlobUri)
            .equals(blankToEmpty(effective.directStatsBlobUri()))
        && currentState.snapshotTaskDirectStatsRecordCount == effective.directStatsRecordCount()
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
        effective.directStatsBlobUri(),
        effective.directStatsRecordCount());
  }

  @Override
  public void persistFileGroupResult(String jobId, ReconcileFileGroupTask fileGroupTask) {
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
          Optional<StoredEnvelope> updated =
              mutateByJobIdReturningRecord(
                  loaded.get().record.jobId,
                  existing -> {
                    if (existing == null) {
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
        CompletionKind.FAILED_WAITING,
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
    return cancellation().isCancellationRequested(jobId);
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
    updated.ifPresent(env -> markDirtyParentForRecord(env.record));
    return updated;
  }

  private Optional<StoredEnvelope> mutateByCanonicalPointerReturningRecord(
      String canonicalPointerKey, UnaryOperator<StoredReconcileJob> mutator) {
    Optional<StoredEnvelope> updated =
        jobIndexStore()
            .mutateByCanonicalPointerReturningRecord(canonicalPointerKey, mutator)
            .map(env -> new StoredEnvelope(env.canonicalPointerKey(), env.record()));
    updated.ifPresent(env -> markDirtyParentForRecord(env.record));
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
    long now = System.currentTimeMillis();
    if (current.expiresAtMs > 0L && now - current.expiresAtMs > leaseRenewGraceMs) {
      LOG.warnf(
          "Skipping renewLease for reconcile job %s due to lease expiry beyond grace now=%d"
              + " expiry=%d graceMs=%d",
          jobId, now, current.expiresAtMs, leaseRenewGraceMs);
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
    List<StoredReconcileJob> directChildren =
        listAllStoredChildJobs(accountId, parentJobId).stream()
            .map(this::projectedSummaryRecordForRefresh)
            .toList();
    var nextProjection = ancestorRollups().recomputeParentProjection(parent, directChildren);
    var currentProjection = projections().load(accountId, parentJobId).orElse(null);
    if (Objects.equals(currentProjection, nextProjection)) {
      refreshRootSummary(parent, nextProjection);
      return;
    }
    projections().upsert(nextProjection);
    refreshRootSummary(parent, nextProjection);
    markDirtyParent(parent.accountId, parent.parentJobId);
  }

  private void refreshRootSummary(
      StoredReconcileJob parent,
      ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobProjection
          projection) {
    if (parent == null || !parent.parentJobId.isBlank()) {
      return;
    }
    rootSummaries().upsert(toRootListSummary(parent, projection));
  }

  private List<StoredReconcileJob> listAllStoredChildJobs(String accountId, String parentJobId) {
    List<StoredReconcileJob> children = new java.util.ArrayList<>();
    String token = "";
    while (true) {
      ReconcileJobIndexStore.StoredJobPage page =
          jobIndexStore().listStoredChildJobs(accountId, parentJobId, 200, token);
      children.addAll(page.records());
      if (page.nextPageToken().isBlank() || page.nextPageToken().equals(token)) {
        return children;
      }
      token = page.nextPageToken();
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

  private Optional<
          ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobProjection>
      projectionFor(StoredReconcileJob record) {
    if (record == null || !isParentCapable(record.jobKind())) {
      return Optional.empty();
    }
    return projections().load(record.accountId, record.jobId);
  }

  private StoredReconcileJob projectedSummaryRecord(StoredReconcileJob record) {
    return projectedSummaryRecord(record, false);
  }

  private StoredReconcileJob projectedSummaryRecordForRefresh(StoredReconcileJob record) {
    return projectedSummaryRecord(record, true);
  }

  private StoredReconcileJob projectedSummaryRecord(
      StoredReconcileJob record, boolean refreshStaleProjection) {
    if (record == null || !isParentCapable(record.jobKind())) {
      return record;
    }
    var projection = projections().load(record.accountId, record.jobId).orElse(null);
    if (projection == null) {
      return record;
    }
    if (refreshStaleProjection
        && shouldKeepCanonicalStateForTerminalProjection(record, projection)) {
      var refreshedProjection = recomputeSummaryProjection(record, true);
      if (refreshedProjection != null) {
        if (!Objects.equals(projection, refreshedProjection)) {
          projections().upsert(refreshedProjection);
        }
        projection = refreshedProjection;
      }
    }
    if (shouldKeepCanonicalStateForTerminalProjection(record, projection)) {
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
    if (record == null || !isParentCapable(record.jobKind())) {
      return projectionFor(record).orElse(null);
    }
    List<StoredReconcileJob> directChildren =
        listAllStoredChildJobs(record.accountId, record.jobId).stream()
            .map(
                child ->
                    refreshStaleDescendants
                        ? projectedSummaryRecordForRefresh(child)
                        : projectedSummaryRecord(child))
            .toList();
    return ancestorRollups().recomputeParentProjection(record, directChildren);
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
    String effectiveAccountId = blankToEmpty(accountId);
    String effectiveParentJobId = blankToEmpty(parentJobId);
    if (effectiveAccountId.isBlank() || effectiveParentJobId.isBlank()) {
      return;
    }
    String key = Keys.reconcileDirtyParentPointer(effectiveAccountId, effectiveParentJobId);
    String payload = effectiveAccountId + "\n" + effectiveParentJobId;
    Pointer current = pointerStore.get(key).orElse(null);
    if (current == null) {
      pointerStore.compareAndSet(
          key, 0L, Pointer.newBuilder().setKey(key).setBlobUri(payload).setVersion(1L).build());
      return;
    }
    pointerStore.compareAndSet(
        key,
        current.getVersion(),
        current.toBuilder().setBlobUri(payload).setVersion(current.getVersion() + 1L).build());
  }

  private Optional<LeasedJob> leaseReadyDue(
      long nowMs, LeaseRequest request, LeaseScanStats scanStats) {
    return readyQueue().leaseReadyDue(nowMs, request, scanStats);
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
    return applyLeaseOutcomeInternal(
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
    return onHotPath(
        () ->
            completer()
                .applyLeaseOutcome(
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

  private static void validateFileGroupResultMatchesCanonical(
      StoredReconcileJob state, ReconcileFileGroupTask result) {
    if (state == null || state.jobKind() != ReconcileJobKind.EXEC_FILE_GROUP) {
      return;
    }
    ReconcileFileGroupTask effective = result == null ? ReconcileFileGroupTask.empty() : result;
    if ((!blank(effective.planId()) && !state.fileGroupPlanId.equals(effective.planId()))
        || (!blank(effective.groupId()) && !state.fileGroupGroupId.equals(effective.groupId()))
        || (!blank(effective.tableId()) && !state.fileGroupTableId.equals(effective.tableId()))
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
}
