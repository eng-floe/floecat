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
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionClass;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileResult;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotSelection;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import ai.floedb.floecat.service.common.Canonicalizer;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.errors.StorageNotFoundException;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.arc.properties.IfBuildProperty;
import io.quarkus.scheduler.Scheduled;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.UnaryOperator;
import org.eclipse.microprofile.config.Config;
import org.jboss.logging.Logger;

@ApplicationScoped
@IfBuildProperty(name = "floecat.reconciler.job-store", stringValue = "durable")
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
  private static final int DEFAULT_RECLAIM_BATCH_LIMIT = 256;
  private static final int CAS_MAX = 16;
  private static final String LIST_TOKEN_V1_PREFIX = "v1:";

  @Inject PointerStore pointerStore;
  @Inject BlobStore blobStore;
  @Inject ObjectMapper mapper;
  @Inject Config config;

  private int maxAttempts = DEFAULT_MAX_ATTEMPTS;
  private long baseBackoffMs = DEFAULT_BASE_BACKOFF_MS;
  private long maxBackoffMs = DEFAULT_MAX_BACKOFF_MS;
  private long leaseMs = DEFAULT_LEASE_MS;
  private long reclaimIntervalMs = DEFAULT_RECLAIM_INTERVAL_MS;
  private long leaseRenewGraceMs = DEFAULT_LEASE_RENEW_GRACE_MS;
  private int readyScanLimit = DEFAULT_READY_SCAN_LIMIT;
  private int reclaimBatchLimit = DEFAULT_RECLAIM_BATCH_LIMIT;

  private final String leaseOwner = "reconcile-store-" + UUID.randomUUID();
  private volatile long lastReclaimAtMs;
  private final ReentrantLock reclaimLock = new ReentrantLock();

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
    reclaimBatchLimit =
        Math.max(
            1,
            config
                .getOptionalValue("floecat.reconciler.job-store.reclaim-batch-limit", Integer.class)
                .orElse(DEFAULT_RECLAIM_BATCH_LIMIT));
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
    ReconcileJobKind effectiveJobKind = jobKind == null ? ReconcileJobKind.PLAN_CONNECTOR : jobKind;
    ReconcileTableTask effectiveTableTask =
        tableTask == null ? ReconcileTableTask.empty() : tableTask;
    ReconcileViewTask effectiveViewTask = viewTask == null ? ReconcileViewTask.empty() : viewTask;
    ReconcileSnapshotTask effectiveSnapshotTask =
        snapshotTask == null ? ReconcileSnapshotTask.empty() : snapshotTask;
    ReconcileFileGroupTask effectiveFileGroupTask =
        fileGroupTask == null ? ReconcileFileGroupTask.empty() : fileGroupTask;
    requireExplicitSnapshotCoverage(effectiveJobKind, effectiveSnapshotTask, "enqueue");
    ReconcileScope scope =
        normalizeScopeForJobKind(
            incomingScope == null ? ReconcileScope.empty() : incomingScope,
            effectiveJobKind,
            effectiveTableTask,
            effectiveViewTask);
    ReconcileExecutionPolicy policy =
        executionPolicy == null ? ReconcileExecutionPolicy.defaults() : executionPolicy;
    String laneKey =
        laneKey(
            connectorId,
            scope,
            effectiveJobKind,
            effectiveTableTask,
            effectiveViewTask,
            effectiveSnapshotTask,
            effectiveFileGroupTask);
    String dedupeKey =
        dedupeKey(
            accountId,
            connectorId,
            fullRescan,
            captureMode,
            scope,
            effectiveJobKind,
            effectiveTableTask,
            effectiveViewTask,
            effectiveSnapshotTask,
            effectiveFileGroupTask,
            policy,
            parentJobId,
            pinnedExecutorId);
    String dedupePointerKey = Keys.reconcileDedupePointer(accountId, hashValue(dedupeKey));

    for (int attempt = 0; attempt < CAS_MAX; attempt++) {
      var existing = loadActiveFromDedupe(dedupePointerKey);
      if (existing.isPresent()) {
        return existing.get().jobId;
      }

      String jobId = UUID.randomUUID().toString();
      long now = System.currentTimeMillis();
      String canonicalKey = Keys.reconcileJobPointerById(accountId, jobId);
      String lookupKey = Keys.reconcileJobLookupPointerById(jobId);
      String parentKey = parentPointerKey(accountId, parentJobId, jobId);
      String laneQueueKey = Keys.reconcileLaneQueuePointerByDue(now, accountId, laneKey, jobId);
      String blobUri =
          Keys.reconcileJobBlobUri(accountId, jobId, "create-" + now + "-" + UUID.randomUUID());
      String refBlobUri = Keys.reconcileJobRefBlobUri(accountId, jobId);

      Pointer dedupeReserve =
          Pointer.newBuilder()
              .setKey(dedupePointerKey)
              .setBlobUri(refBlobUri)
              .setVersion(1L)
              .build();
      if (!pointerStore.compareAndSet(dedupePointerKey, 0L, dedupeReserve)) {
        continue;
      }

      StoredReconcileJob record =
          StoredReconcileJob.queued(
              jobId,
              accountId,
              connectorId,
              fullRescan,
              captureMode,
              scope,
              effectiveJobKind,
              effectiveTableTask,
              effectiveViewTask,
              effectiveSnapshotTask,
              effectiveFileGroupTask,
              policy,
              parentJobId,
              pinnedExecutorId,
              laneKey,
              dedupeKey,
              now,
              laneQueueKey);
      if (effectiveJobKind == ReconcileJobKind.PLAN_SNAPSHOT) {
        LOG.debugf(
            "enqueue persisted PLAN_SNAPSHOT jobId=%s parentJobId=%s connectorId=%s tableId=%s snapshotId=%d source=%s.%s fileGroups=%d",
            jobId,
            parentJobId,
            connectorId,
            effectiveSnapshotTask.tableId(),
            effectiveSnapshotTask.snapshotId(),
            effectiveSnapshotTask.sourceNamespace(),
            effectiveSnapshotTask.sourceTable(),
            effectiveSnapshotTask.fileGroups().size());
      }

      try {
        blobStore.put(
            blobUri,
            mapper.writeValueAsBytes(record),
            "application/json; charset=" + StandardCharsets.UTF_8.name());
        blobStore.put(
            refBlobUri,
            mapper.writeValueAsBytes(StoredJobReference.of(accountId, jobId, canonicalKey)),
            "application/json; charset=" + StandardCharsets.UTF_8.name());
      } catch (Exception e) {
        clearDedupeReservation(dedupePointerKey, refBlobUri);
        throw new IllegalStateException("Failed to persist reconcile job payload", e);
      }

      Pointer canonical =
          Pointer.newBuilder().setKey(canonicalKey).setBlobUri(blobUri).setVersion(1L).build();
      if (!pointerStore.compareAndSet(canonicalKey, 0L, canonical)) {
        clearDedupeReservation(dedupePointerKey, refBlobUri);
        blobStore.delete(blobUri);
        continue;
      }

      Pointer lookup =
          Pointer.newBuilder().setKey(lookupKey).setBlobUri(refBlobUri).setVersion(1L).build();
      if (!pointerStore.compareAndSet(lookupKey, 0L, lookup)) {
        clearPointerIfMatches(canonicalKey, blobUri);
        clearDedupeReservation(dedupePointerKey, refBlobUri);
        blobStore.delete(blobUri);
        continue;
      }

      if (!parentKey.isBlank()) {
        Pointer parent =
            Pointer.newBuilder().setKey(parentKey).setBlobUri(refBlobUri).setVersion(1L).build();
        if (!pointerStore.compareAndSet(parentKey, 0L, parent)) {
          clearPointerIfMatches(lookupKey, refBlobUri);
          clearPointerIfMatches(canonicalKey, blobUri);
          clearDedupeReservation(dedupePointerKey, refBlobUri);
          blobStore.delete(blobUri);
          continue;
        }
      }

      Pointer laneQueue =
          Pointer.newBuilder().setKey(laneQueueKey).setBlobUri(refBlobUri).setVersion(1L).build();
      if (!pointerStore.compareAndSet(laneQueueKey, 0L, laneQueue)) {
        clearPointerIfMatches(parentKey, refBlobUri);
        clearPointerIfMatches(lookupKey, refBlobUri);
        clearPointerIfMatches(canonicalKey, blobUri);
        clearDedupeReservation(dedupePointerKey, refBlobUri);
        blobStore.delete(blobUri);
        continue;
      }

      refreshRunnableLaneHead(accountId, laneKey);
      if (supportsChildAggregation(record.jobKind())) {
        ensureAggregateRollupExists(record);
      }

      if (parentJobId != null && !parentJobId.isBlank()) {
        syncAggregateChainByJobId(jobId, "enqueue");
      }
      return jobId;
    }

    throw new IllegalStateException("Unable to enqueue reconcile job after CAS retries");
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
        toPublicJob(loaded.get().record, projectedSummaryForRead(loaded.get().record)));
  }

  @Override
  public ReconcileJobPage list(
      String accountId,
      int pageSize,
      String pageToken,
      String connectorId,
      java.util.Set<String> states) {
    int limit = Math.max(1, pageSize);
    ListCursor cursor = decodeListCursor(pageToken);
    String token = cursor.storeToken();
    int skip = cursor.skip();
    List<ReconcileJob> out = new java.util.ArrayList<>(limit);
    String nextToken = "";
    while (out.size() < limit) {
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers =
          pointerStore.listPointersByPrefix(
              Keys.reconcileJobPointerByIdPrefix(accountId), Math.max(limit * 2, 64), token, next);
      if (pointers.isEmpty()) {
        break;
      }
      int startIndex = Math.min(skip, pointers.size());
      skip = 0;
      for (int i = startIndex; i < pointers.size(); i++) {
        Pointer ptr = pointers.get(i);
        var rec = readRecord(ptr);
        if (rec.isEmpty()) {
          continue;
        }
        var stored = rec.get();
        if (connectorId != null
            && !connectorId.isBlank()
            && !connectorId.equals(stored.connectorId)) {
          continue;
        }
        var job = toPublicJob(stored, projectedSummaryForRead(stored));
        if (states != null && !states.isEmpty() && !states.contains(job.state)) {
          continue;
        }
        out.add(job);
        if (out.size() >= limit) {
          boolean hasMore = i + 1 < pointers.size() || next.length() > 0;
          if (!hasMore) {
            nextToken = "";
          } else if (i + 1 < pointers.size()) {
            nextToken = encodeListCursor(token, i + 1);
          } else {
            nextToken = next.toString();
          }
          break;
        }
      }
      if (out.size() >= limit) {
        break;
      }
      nextToken = next.toString();
      if (nextToken.isBlank()) {
        break;
      }
      token = nextToken;
    }
    return new ReconcileJobPage(out, nextToken);
  }

  @Override
  public List<ReconcileJob> childJobs(String accountId, String parentJobId) {
    if (accountId == null || accountId.isBlank() || parentJobId == null || parentJobId.isBlank()) {
      return List.of();
    }
    List<ReconcileJob> out = new ArrayList<>();
    String token = "";
    String prefix = Keys.reconcileJobByParentPointerPrefix(accountId, parentJobId);
    do {
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers = pointerStore.listPointersByPrefix(prefix, 200, token, next);
      for (Pointer ptr : pointers) {
        var rec = readCurrentRecordFromIndexPointer(ptr, ptr.getKey());
        rec.ifPresent(stored -> out.add(toPublicJob(stored, projectedSummaryForRead(stored))));
      }
      token = next.toString();
    } while (token != null && !token.isBlank());
    return List.copyOf(out);
  }

  @Override
  public QueueStats queueStats() {
    long queued = 0L;
    long running = 0L;
    long cancelling = 0L;
    long oldestQueued = 0L;
    String token = "";
    int pages = 0;
    while (true) {
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers =
          pointerStore.listPointersByPrefix(
              Keys.reconcileJobLookupPointerByIdPrefix(), 256, token, next);
      if (pointers.isEmpty()) {
        break;
      }
      for (Pointer ptr : pointers) {
        var rec = readCurrentRecordFromIndexPointer(ptr, ptr.getKey());
        if (rec.isEmpty() || rec.get().state == null) {
          continue;
        }
        switch (rec.get().state) {
          case "JS_QUEUED", "JS_WAITING" -> {
            if ("JS_WAITING".equals(projectedStoredState(rec.get()))) {
              continue;
            }
            queued++;
            long created = Math.max(0L, rec.get().createdAtMs);
            if (created > 0L && (oldestQueued == 0L || created < oldestQueued)) {
              oldestQueued = created;
            }
          }
          case "JS_RUNNING" -> running++;
          case "JS_CANCELLING" -> cancelling++;
          default -> {}
        }
      }
      token = next.toString();
      pages++;
      if (token.isBlank() || pages >= 10_000) {
        break;
      }
    }
    return new QueueStats(queued, running, cancelling, oldestQueued);
  }

  @Override
  public Optional<LeasedJob> leaseNext(LeaseRequest request) {
    LeaseRequest effective = request == null ? LeaseRequest.all() : request;
    return leaseReadyDue(System.currentTimeMillis(), effective);
  }

  @Scheduled(
      every = "{reconciler.reclaimEvery:1s}",
      concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
  void reclaimExpiredLeases() {
    reclaimExpiredLeasesIfDue(System.currentTimeMillis());
  }

  void reclaimNowForTesting() {
    reclaimExpiredLeasesIfDue(System.currentTimeMillis());
  }

  @Override
  public boolean renewLease(String jobId, String leaseEpoch) {
    var loaded = loadByAnyAccount(jobId);
    if (loaded.isEmpty()) {
      return false;
    }
    return renewLeaseByCanonicalPointer(loaded.get().canonicalPointerKey, jobId, leaseEpoch);
  }

  @Override
  public ProgressUpdate reportProgress(
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
    var loaded = loadByAnyAccount(jobId);
    if (loaded.isEmpty()) {
      return new ProgressUpdate(false, false);
    }
    return mutateProgressByCanonicalPointer(
        loaded.get().canonicalPointerKey,
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
  }

  @Override
  public void persistSnapshotPlan(String jobId, ReconcileSnapshotTask snapshotTask) {
    ReconcileSnapshotTask effective =
        snapshotTask == null ? ReconcileSnapshotTask.empty() : snapshotTask;
    LOG.debugf(
        "persistSnapshotPlan jobId=%s tableId=%s snapshotId=%d fileGroups=%d",
        jobId, effective.tableId(), effective.snapshotId(), effective.fileGroups().size());
    mutateByJobId(
        jobId,
        existing -> {
          if (ReconcileJobKind.PLAN_SNAPSHOT.name().equals(existing.jobKind)
              && !effective.fileGroupPlanRecorded()) {
            throw new IllegalArgumentException(
                "persistSnapshotPlan requires explicit snapshot coverage metadata for PLAN_SNAPSHOT jobs");
          }
          existing.snapshotTaskTableId = blankToEmpty(effective.tableId());
          existing.snapshotTaskSnapshotId = effective.snapshotId();
          existing.snapshotTaskSourceNamespace = effective.sourceNamespace();
          existing.snapshotTaskSourceTable = effective.sourceTable();
          existing.snapshotTaskFileGroups = effective.fileGroups();
          existing.snapshotTaskFileGroupPlanRecorded = effective.fileGroupPlanRecorded();
          existing.snapshotTaskCompletionMode = effective.completionMode().name();
          existing.snapshotTaskFileGroupPlanBlobUri =
              blankToEmpty(effective.fileGroupPlanBlobUri());
          existing.snapshotTaskFileGroupCount = effective.fileGroupCount();
          return existing;
        });
    syncAggregateChainByJobId(jobId, "persistSnapshotPlan");
  }

  @Override
  public void persistFileGroupResult(String jobId, ReconcileFileGroupTask fileGroupTask) {
    ReconcileFileGroupTask effective =
        fileGroupTask == null ? ReconcileFileGroupTask.empty() : fileGroupTask;
    mutateByJobId(
        jobId,
        existing -> {
          existing.fileGroupPlanId = blankToEmpty(effective.planId());
          existing.fileGroupGroupId = blankToEmpty(effective.groupId());
          existing.fileGroupTableId = blankToEmpty(effective.tableId());
          existing.fileGroupSnapshotId = effective.snapshotId();
          existing.fileGroupFileCount = effective.fileCount();
          existing.fileGroupPaths = effective.filePaths();
          existing.fileGroupResults = effective.fileResults();
          return existing;
        });
    syncAggregateChainByJobId(jobId, "persistFileGroupResult");
  }

  @Override
  public void markRunning(String jobId, String leaseEpoch, long startedAtMs, String executorId) {
    mutateByJobId(
        jobId,
        existing -> {
          if (!hasActiveLease(jobId, leaseEpoch, existing, "markRunning", true, false, true)) {
            return null;
          }
          boolean cancelling = "JS_CANCELLING".equals(existing.state);
          if (!cancelling) {
            existing.state = "JS_RUNNING";
            existing.message = "Running";
            existing.waitingOnDependency = false;
          }
          if (existing.startedAtMs <= 0L) {
            existing.startedAtMs = startedAtMs;
          }
          existing.executorId = executorId == null ? "" : executorId;
          return existing;
        });
    syncAggregateChainByJobId(jobId, "markRunning");
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
    mutateByJobId(
        jobId,
        existing -> {
          if (!hasActiveLease(jobId, leaseEpoch, existing, "markProgress", false, false, true)) {
            return null;
          }
          if (isTerminalState(existing.state)) {
            return existing;
          }
          existing.tablesScanned = tablesScanned;
          existing.tablesChanged = tablesChanged;
          existing.viewsScanned = viewsScanned;
          existing.viewsChanged = viewsChanged;
          existing.errors = errors;
          existing.snapshotsProcessed = snapshotsProcessed;
          existing.statsProcessed = statsProcessed;
          if (message != null && !message.isBlank()) {
            existing.message = message;
          }
          return existing;
        });
    syncAggregateChainByJobId(jobId, "markProgress");
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
    applyLeaseOutcome(
        jobId,
        leaseEpoch,
        CompletionKind.SUCCEEDED,
        finishedAtMs,
        null,
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
    applyLeaseOutcome(
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
    applyLeaseOutcome(
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
    applyLeaseOutcome(
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
    var loaded = loadByAnyAccount(jobId);
    if (loaded.isEmpty()) {
      return Optional.empty();
    }
    if (accountId != null
        && !accountId.isBlank()
        && !accountId.equals(loaded.get().record.accountId)) {
      return Optional.empty();
    }
    mutateByCanonicalPointer(
        loaded.get().canonicalPointerKey,
        existing -> {
          if (isTerminalState(existing.state) || "JS_CANCELLING".equals(existing.state)) {
            return existing;
          }
          if ("JS_RUNNING".equals(existing.state)) {
            long now = System.currentTimeMillis();
            existing.state = "JS_CANCELLING";
            existing.message = (reason == null || reason.isBlank()) ? "Cancelling" : reason;
            long cancelPokeExpiry = now + CANCEL_POKE_MAX_DELAY_MS;
            if (existing.leaseExpiresAtMs <= 0L) {
              existing.leaseExpiresAtMs = cancelPokeExpiry;
            } else {
              existing.leaseExpiresAtMs = Math.min(existing.leaseExpiresAtMs, cancelPokeExpiry);
            }
            existing.readyPointerKey = null;
            existing.laneQueuePointerKey = null;
            existing.updatedAtMs = now;
            return existing;
          }
          existing.state = "JS_CANCELLED";
          existing.message = (reason == null || reason.isBlank()) ? "Cancelled" : reason;
          long now = System.currentTimeMillis();
          if (existing.startedAtMs <= 0L) {
            existing.startedAtMs = now;
          }
          existing.finishedAtMs = now;
          existing.leaseOwner = null;
          existing.leaseEpoch = null;
          existing.leaseExpiresAtMs = 0L;
          existing.readyPointerKey = null;
          existing.laneQueuePointerKey = null;
          return existing;
        });
    syncAggregateChainByJobId(jobId, "cancel");
    var post = get(accountId, jobId);
    if (post.isPresent()
        && ("JS_CANCELLED".equals(post.get().state) || "JS_CANCELLING".equals(post.get().state))) {
      if ("JS_CANCELLING".equals(post.get().state)) {
        shortenLeaseForCancellation(loaded.get().record);
      } else {
        deleteLeaseArtifacts(
            loaded.get().record, currentLeaseState(loaded.get().record).orElse(null));
      }
      return post;
    }
    return Optional.empty();
  }

  @Override
  public boolean isCancellationRequested(String jobId) {
    var job = get(null, jobId);
    if (job.isEmpty()) {
      return false;
    }
    String state = job.get().state;
    return "JS_CANCELLING".equals(state) || "JS_CANCELLED".equals(state);
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
    applyLeaseOutcome(
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
    var loaded = loadByAnyAccount(jobId);
    if (loaded.isEmpty()) {
      return false;
    }
    return completeLeaseByCanonicalPointer(
        loaded.get().canonicalPointerKey,
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

  private Optional<StoredEnvelope> loadByAnyAccount(String jobId) {
    String lookupKey = Keys.reconcileJobLookupPointerById(jobId);
    Pointer lookup = pointerStore.get(lookupKey).orElse(null);
    if (lookup != null) {
      var ref = readJobReferenceByBlobUri(lookup.getBlobUri());
      if (ref.isPresent() && jobId.equals(ref.get().jobId) && !ref.get().accountId.isBlank()) {
        String canonicalPointerKey =
            blank(ref.get().canonicalPointerKey)
                ? Keys.reconcileJobPointerById(ref.get().accountId, ref.get().jobId)
                : ref.get().canonicalPointerKey;
        Pointer canonicalPointer = pointerStore.get(canonicalPointerKey).orElse(null);
        if (canonicalPointer != null) {
          var canonicalRec = readRecord(canonicalPointer);
          if (canonicalRec.isPresent()) {
            return Optional.of(new StoredEnvelope(canonicalPointerKey, canonicalRec.get()));
          }
        }
        pointerStore.compareAndDelete(lookupKey, lookup.getVersion());
        return Optional.empty();
      }
      pointerStore.compareAndDelete(lookupKey, lookup.getVersion());
    }
    return Optional.empty();
  }

  private Optional<StoredReconcileJob> readCurrentRecordFromIndexPointer(
      Pointer indexPointer, String pointerKey) {
    if (indexPointer == null) {
      return Optional.empty();
    }
    return resolveCurrentRecordByReferenceBlobUri(indexPointer.getBlobUri());
  }

  private void mutateByJobId(String jobId, UnaryOperator<StoredReconcileJob> mutator) {
    var loaded = loadByAnyAccount(jobId);
    if (loaded.isEmpty()) {
      return;
    }
    mutateByCanonicalPointer(loaded.get().canonicalPointerKey, mutator);
  }

  private void mutateByCanonicalPointer(
      String canonicalPointerKey, UnaryOperator<StoredReconcileJob> mutator) {
    mutateByCanonicalPointerResult(canonicalPointerKey, mutator);
  }

  private boolean mutateByCanonicalPointerResult(
      String canonicalPointerKey, UnaryOperator<StoredReconcileJob> mutator) {
    for (int i = 0; i < CAS_MAX; i++) {
      Pointer currentPointer = pointerStore.get(canonicalPointerKey).orElse(null);
      if (currentPointer == null) {
        return false;
      }
      var currentOpt = readRecord(currentPointer);
      if (currentOpt.isEmpty()) {
        return false;
      }

      StoredReconcileJob baseline = cloneStoredRecord(currentOpt.get());
      baseline.currentBlobUri = currentPointer.getBlobUri();
      StoredReconcileJob current = cloneStoredRecord(currentOpt.get());
      current.currentBlobUri = currentPointer.getBlobUri();
      StoredReconcileJob nextRecord = mutator.apply(current);
      if (nextRecord == null) {
        return false;
      }

      refreshStoredCountersWithBlob(nextRecord);
      if (writeMutatedRecord(canonicalPointerKey, currentPointer, baseline, nextRecord, "mutate")) {
        return true;
      }
    }
    return false;
  }

  private boolean writeMutatedRecord(
      String canonicalPointerKey,
      Pointer currentPointer,
      StoredReconcileJob baseline,
      StoredReconcileJob nextRecord,
      String reason) {
    nextRecord.updatedAtMs = System.currentTimeMillis();
    nextRecord.canonicalPointerKey = canonicalPointerKey;
    String nextBlobUri =
        Keys.reconcileJobBlobUri(
            nextRecord.accountId,
            nextRecord.jobId,
            reason + "-" + (currentPointer.getVersion() + 1) + "-" + UUID.randomUUID());
    try {
      blobStore.put(
          nextBlobUri,
          mapper.writeValueAsBytes(nextRecord),
          "application/json; charset=" + StandardCharsets.UTF_8.name());
    } catch (Exception e) {
      throw new IllegalStateException("Failed to write reconcile job payload", e);
    }

    Pointer nextPointer =
        Pointer.newBuilder()
            .setKey(canonicalPointerKey)
            .setBlobUri(nextBlobUri)
            .setVersion(currentPointer.getVersion() + 1)
            .build();
    if (pointerStore.compareAndSet(canonicalPointerKey, currentPointer.getVersion(), nextPointer)) {
      reconcileIndexPointers(baseline, nextRecord, currentPointer.getBlobUri(), nextBlobUri);
      return true;
    }
    blobStore.delete(nextBlobUri);
    return false;
  }

  private boolean renewLeaseByCanonicalPointer(
      String canonicalPointerKey, String jobId, String leaseEpoch) {
    Pointer currentPointer = pointerStore.get(canonicalPointerKey).orElse(null);
    if (currentPointer == null) {
      return false;
    }
    var currentOpt = readRecord(currentPointer);
    if (currentOpt.isEmpty()) {
      return false;
    }
    StoredReconcileJob current = cloneStoredRecord(currentOpt.get());
    if (!hasActiveLease(jobId, leaseEpoch, current, "renewLease", false, true, false)) {
      return false;
    }
    StoredLeaseStateEnvelope leaseStateEnvelope =
        readLeaseStateEnvelope(current.accountId, current.jobId).orElse(null);
    if (leaseStateEnvelope == null) {
      return false;
    }
    StoredLeaseState leaseState = leaseStateEnvelope.leaseState();
    long now = System.currentTimeMillis();
    long expiry = leaseState.leaseExpiresAtMs;
    if (expiry > 0L && (expiry - now) > (leaseMs / 2L)) {
      return true;
    }
    if (expiry > 0L && now - expiry > leaseRenewGraceMs) {
      LOG.warnf(
          "Skipping renewLease for reconcile job %s due to lease expiry beyond grace now=%d"
              + " expiry=%d graceMs=%d",
          jobId, now, expiry, leaseRenewGraceMs);
      return false;
    }
    return updateLeaseStateExpiry(leaseStateEnvelope, now + leaseMs);
  }

  private ProgressUpdate mutateProgressByCanonicalPointer(
      String canonicalPointerKey,
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
    for (int i = 0; i < CAS_MAX; i++) {
      Pointer currentPointer = pointerStore.get(canonicalPointerKey).orElse(null);
      if (currentPointer == null) {
        return new ProgressUpdate(false, false);
      }
      var currentOpt = readRecord(currentPointer);
      if (currentOpt.isEmpty()) {
        return new ProgressUpdate(false, false);
      }

      StoredReconcileJob baseline = cloneStoredRecord(currentOpt.get());
      baseline.currentBlobUri = currentPointer.getBlobUri();
      StoredReconcileJob current = cloneStoredRecord(currentOpt.get());
      LeaseUpdate leaseUpdate =
          applyLeaseRenewalForMutation(
              jobId, leaseEpoch, current, "reportProgress", false, false, false);
      if (!leaseUpdate.accepted()) {
        return new ProgressUpdate(false, leaseUpdate.cancellationRequested());
      }
      if (isTerminalState(current.state)) {
        return new ProgressUpdate(true, leaseUpdate.cancellationRequested());
      }
      current.tablesScanned = tablesScanned;
      current.tablesChanged = tablesChanged;
      current.viewsScanned = viewsScanned;
      current.viewsChanged = viewsChanged;
      current.errors = errors;
      current.snapshotsProcessed = snapshotsProcessed;
      current.statsProcessed = statsProcessed;
      if (message != null && !message.isBlank()) {
        current.message = message;
      }
      if (writeMutatedRecord(canonicalPointerKey, currentPointer, baseline, current, "progress")) {
        syncAggregateChainByJobId(jobId, "reportProgress");
        return new ProgressUpdate(true, leaseUpdate.cancellationRequested());
      }
    }
    return new ProgressUpdate(false, false);
  }

  private boolean completeLeaseByCanonicalPointer(
      String canonicalPointerKey,
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
    CompletionKind effectiveKind =
        completionKind == null ? CompletionKind.FAILED_RETRYABLE : completionKind;
    for (int i = 0; i < CAS_MAX; i++) {
      Pointer currentPointer = pointerStore.get(canonicalPointerKey).orElse(null);
      if (currentPointer == null) {
        return false;
      }
      var currentOpt = readRecord(currentPointer);
      if (currentOpt.isEmpty()) {
        return false;
      }

      StoredReconcileJob baseline = cloneStoredRecord(currentOpt.get());
      baseline.currentBlobUri = currentPointer.getBlobUri();
      StoredReconcileJob current = cloneStoredRecord(currentOpt.get());
      LeaseUpdate leaseUpdate =
          applyLeaseRenewalForMutation(
              jobId,
              leaseEpoch,
              current,
              "completeLease",
              false,
              effectiveKind == CompletionKind.CANCELLED,
              false);
      if (!leaseUpdate.accepted()) {
        return false;
      }
      if (isTerminalState(current.state)) {
        return true;
      }

      StoredLeaseState priorLeaseState = currentLeaseState(current).orElse(null);

      switch (effectiveKind) {
        case SUCCEEDED -> {
          if ("JS_CANCELLING".equals(current.state)) {
            current.state = "JS_CANCELLED";
            current.message =
                blank(current.message)
                    ? (message == null || message.isBlank() ? "Cancelled" : message)
                    : current.message;
            break;
          }
          current.state = "JS_SUCCEEDED";
          current.message = message == null || message.isBlank() ? "Succeeded" : message;
        }
        case FAILED_WAITING -> {
          if ("JS_CANCELLING".equals(current.state)) {
            current.state = "JS_CANCELLED";
            current.message = blank(current.message) ? "Cancelled" : current.message;
            break;
          }
          current.state = "JS_WAITING";
          current.message = message == null ? "Waiting on dependency" : message;
          current.waitingOnDependency = false;
          current.lastError = current.message;
          current.executorId = "";
          current.nextAttemptAtMs = finishedAtMs + backoffMs(Math.max(1, current.attempt));
          current.finishedAtMs = 0L;
          current.laneQueuePointerKey =
              Keys.reconcileLaneQueuePointerByDue(
                  current.nextAttemptAtMs, current.accountId, current.laneKey, current.jobId);
        }
        case FAILED_TERMINAL -> {
          if ("JS_CANCELLING".equals(current.state)) {
            current.state = "JS_CANCELLED";
            current.message = blank(current.message) ? "Cancelled" : current.message;
            break;
          }
          current.attempt = Math.max(0, current.attempt) + 1;
          current.state = "JS_FAILED";
          current.waitingOnDependency = false;
          current.message = message == null ? "Failed" : message;
          current.lastError = current.message;
        }
        case CANCELLED -> {
          current.state = "JS_CANCELLED";
          current.message = message == null || message.isBlank() ? "Cancelled" : message;
        }
        case FAILED_RETRYABLE -> {
          if ("JS_CANCELLING".equals(current.state)) {
            current.state = "JS_CANCELLED";
            current.message = blank(current.message) ? "Cancelled" : current.message;
            break;
          }
          current.attempt = Math.max(0, current.attempt) + 1;
          current.lastError = message == null ? "Failed" : message;
          if (current.attempt >= maxAttempts) {
            current.state = "JS_FAILED";
            current.waitingOnDependency = false;
            current.message = current.lastError;
          } else {
            current.state = "JS_QUEUED";
            current.message = message == null ? "Retrying" : message;
            current.waitingOnDependency = false;
            current.executorId = "";
            current.nextAttemptAtMs = finishedAtMs + backoffMs(current.attempt);
            current.finishedAtMs = 0L;
            current.laneQueuePointerKey =
                Keys.reconcileLaneQueuePointerByDue(
                    current.nextAttemptAtMs, current.accountId, current.laneKey, current.jobId);
          }
        }
      }

      current.tablesScanned = tablesScanned;
      current.tablesChanged = tablesChanged;
      current.viewsScanned = viewsScanned;
      current.viewsChanged = viewsChanged;
      current.errors = errors;
      current.snapshotsProcessed = snapshotsProcessed;
      current.statsProcessed = statsProcessed;
      if (current.startedAtMs <= 0L) {
        current.startedAtMs = finishedAtMs;
      }
      if (isTerminalState(current.state)) {
        current.finishedAtMs = finishedAtMs;
      }
      releaseLeaseOwnership(current);
      current.readyPointerKey = null;
      if (!"JS_QUEUED".equals(current.state) && !"JS_WAITING".equals(current.state)) {
        current.laneQueuePointerKey = null;
      }
      refreshStoredCountersWithBlob(current);
      if (writeMutatedRecord(canonicalPointerKey, currentPointer, baseline, current, "complete")) {
        deleteLeaseArtifacts(current, priorLeaseState);
        if (!blank(current.accountId) && !blank(current.laneKey)) {
          refreshRunnableLaneHead(current.accountId, current.laneKey);
        }
        syncAggregateChainByJobId(jobId, "completeLease");
        return true;
      }
    }
    return false;
  }

  private Optional<LeasedJob> leaseCanonical(
      String canonicalPointerKey,
      String laneQueuePointerKey,
      String runnableLanePointerKey,
      long now,
      LeaseReadyDueMetrics metrics) {
    for (int i = 0; i < CAS_MAX; i++) {
      Pointer currentPointer = pointerStore.get(canonicalPointerKey).orElse(null);
      if (currentPointer == null) {
        return Optional.empty();
      }
      var recordOpt = readRecord(currentPointer);
      if (recordOpt.isEmpty()) {
        return Optional.empty();
      }
      StoredReconcileJob record = recordOpt.get();
      StoredReconcileJob baseline = cloneStoredRecord(record);
      baseline.currentBlobUri = currentPointer.getBlobUri();
      StoredReconcileJob current = cloneStoredRecord(record);

      if (isTerminalState(current.state)) {
        clearDedupeIfOwned(current);
        return Optional.empty();
      }

      StoredLeaseState existingLeaseState = currentLeaseState(current).orElse(null);
      if (("JS_RUNNING".equals(current.state) || "JS_CANCELLING".equals(current.state))
          && existingLeaseState != null
          && existingLeaseState.leaseExpiresAtMs > now
          && !blank(existingLeaseState.leaseEpoch)) {
        return Optional.empty();
      }

      long snapshotLeaseStartedNs = System.nanoTime();
      boolean snapshotLeaseAcquired =
          tryAcquireSnapshotLease(
              current, stableReferenceBlobUri(current.accountId, current.jobId), now);
      if (metrics != null) {
        metrics.tryAcquireSnapshotLeaseNs += System.nanoTime() - snapshotLeaseStartedNs;
      }
      if (!snapshotLeaseAcquired) {
        if (metrics != null) {
          metrics.snapshotLeaseMisses++;
        }
        if (current.accountId != null
            && !current.accountId.isBlank()
            && current.laneKey != null
            && !current.laneKey.isBlank()) {
          refreshRunnableLaneHead(current.accountId, current.laneKey);
        }
        return Optional.empty();
      }

      boolean cancelling = "JS_CANCELLING".equals(current.state);
      StoredLeaseState leaseState = new StoredLeaseState();
      leaseState.accountId = current.accountId;
      leaseState.jobId = current.jobId;
      leaseState.leaseOwner = leaseOwner;
      leaseState.leaseEpoch = UUID.randomUUID().toString();
      leaseState.leaseExpiresAtMs = now + leaseMs;
      leaseState.updatedAtMs = now;
      StoredLeaseStateEnvelope createdLeaseState =
          createLeaseStateIfAbsentOrExpired(
              leaseState,
              now,
              "JS_QUEUED".equals(current.state) || "JS_WAITING".equals(current.state));
      if (createdLeaseState == null) {
        clearSnapshotLeaseIfOwned(
            current, stableReferenceBlobUri(current.accountId, current.jobId));
        if (current.accountId != null
            && !current.accountId.isBlank()
            && current.laneKey != null
            && !current.laneKey.isBlank()) {
          refreshRunnableLaneHead(current.accountId, current.laneKey);
        }
        return Optional.empty();
      }
      leaseState = createdLeaseState.leaseState();
      String referenceBlobUri = stableReferenceBlobUri(current.accountId, current.jobId);
      upsertBlobPointer(leaseExpiryPointerKey(leaseState), referenceBlobUri);
      current.leaseOwner = leaseState.leaseOwner;
      current.leaseEpoch = leaseState.leaseEpoch;
      current.leaseExpiresAtMs = leaseState.leaseExpiresAtMs;
      current.waitingOnDependency = false;
      current.readyPointerKey = null;
      current.laneQueuePointerKey = null;
      if (cancelling) {
        current.state = "JS_CANCELLING";
        if (blank(current.message)) {
          current.message = "Cancelling";
        }
      } else {
        current.state = "JS_RUNNING";
        current.message = "Running";
      }
      if (current.startedAtMs <= 0L) {
        current.startedAtMs = now;
      }
      refreshStoredCountersWithBlob(current);
      if (writeMutatedRecord(canonicalPointerKey, currentPointer, baseline, current, "lease")) {
        if (current.jobKind() == ReconcileJobKind.PLAN_SNAPSHOT) {
          ReconcileSnapshotTask snapshotTask = current.snapshotTask();
          LOG.debugf(
              "leaseCanonical PLAN_SNAPSHOT jobId=%s connectorId=%s tableId=%s snapshotId=%d source=%s.%s fileGroups=%d",
              current.jobId,
              current.connectorId,
              snapshotTask.tableId(),
              snapshotTask.snapshotId(),
              snapshotTask.sourceNamespace(),
              snapshotTask.sourceTable(),
              snapshotTask.fileGroups().size());
        }
        return Optional.of(
            new LeasedJob(
                current.jobId,
                current.accountId,
                current.connectorId,
                current.fullRescan,
                current.captureMode(),
                current.toScope(),
                current.executionPolicy(),
                leaseState.leaseEpoch,
                current.pinnedExecutorId(),
                current.executorId(),
                current.jobKind(),
                current.tableTask(),
                current.viewTask(),
                current.snapshotTask(),
                current.fileGroupTask(),
                current.parentJobId()));
      }

      clearPointerIfMatches(leaseExpiryPointerKey(leaseState), referenceBlobUri);
      deleteLeaseStateIfOwned(createdLeaseState);
      clearSnapshotLeaseIfOwned(current, stableReferenceBlobUri(current.accountId, current.jobId));
    }

    return Optional.empty();
  }

  private boolean tryAcquireLaneLease(StoredReconcileJob record, String blobUri, long now) {
    if (record == null
        || record.accountId == null
        || record.accountId.isBlank()
        || record.laneKey == null
        || record.laneKey.isBlank()
        || blobUri == null
        || blobUri.isBlank()) {
      return false;
    }
    String lanePointerKey = Keys.reconcileLaneLeasePointer(record.accountId, record.laneKey);
    for (int i = 0; i < CAS_MAX; i++) {
      Pointer existing = pointerStore.get(lanePointerKey).orElse(null);
      if (existing == null) {
        Pointer created =
            Pointer.newBuilder().setKey(lanePointerKey).setBlobUri(blobUri).setVersion(1L).build();
        if (pointerStore.compareAndSet(lanePointerKey, 0L, created)) {
          return true;
        }
        continue;
      }
      if (blobUri.equals(existing.getBlobUri())) {
        var owner = currentJobRecordForBlobUri(existing.getBlobUri());
        if (owner.isEmpty()) {
          pointerStore.compareAndDelete(lanePointerKey, existing.getVersion());
          continue;
        }
        if (record.jobId.equals(owner.get().jobId)
            && record.accountId.equals(owner.get().accountId)) {
          return true;
        }
        if (!hasActiveLaneLease(owner.get(), now)) {
          pointerStore.compareAndDelete(lanePointerKey, existing.getVersion());
          continue;
        }
        return false;
      }

      var owner = currentJobRecordForBlobUri(existing.getBlobUri());
      if (owner.isPresent()
          && record.jobId.equals(owner.get().jobId)
          && record.accountId.equals(owner.get().accountId)) {
        return true;
      }
      if (owner.isPresent() && hasActiveLaneLease(owner.get(), now)) {
        return false;
      }
      if (pointerStore.compareAndDelete(lanePointerKey, existing.getVersion())) {
        continue;
      }
    }
    return false;
  }

  private boolean hasActiveLaneLease(StoredReconcileJob record, long now) {
    if (record == null) {
      return false;
    }
    if (!"JS_RUNNING".equals(record.state) && !"JS_CANCELLING".equals(record.state)) {
      return false;
    }
    StoredLeaseState leaseState = currentLeaseState(record).orElse(null);
    return leaseState != null && !blank(leaseState.leaseEpoch) && leaseState.leaseExpiresAtMs > now;
  }

  private void clearLaneLeaseIfOwned(StoredReconcileJob record, String expectedBlobUri) {
    if (record == null
        || record.accountId == null
        || record.accountId.isBlank()
        || record.laneKey == null
        || record.laneKey.isBlank()
        || expectedBlobUri == null
        || expectedBlobUri.isBlank()) {
      return;
    }
    String lanePointerKey = Keys.reconcileLaneLeasePointer(record.accountId, record.laneKey);
    Pointer existing = pointerStore.get(lanePointerKey).orElse(null);
    if (existing == null) {
      return;
    }
    if (expectedBlobUri != null
        && !expectedBlobUri.isBlank()
        && !expectedBlobUri.equals(existing.getBlobUri())) {
      return;
    }
    var owner = currentJobRecordForBlobUri(existing.getBlobUri());
    if (owner.isEmpty()
        || !record.jobId.equals(owner.get().jobId)
        || !record.accountId.equals(owner.get().accountId)) {
      return;
    }
    if (hasActiveLaneLease(owner.get(), System.currentTimeMillis())) {
      replacePointerBlobUriIfMatch(
          lanePointerKey,
          existing.getBlobUri(),
          stableReferenceBlobUri(record.accountId, record.jobId));
      return;
    }
    pointerStore.compareAndDelete(lanePointerKey, existing.getVersion());
  }

  private Optional<StoredReconcileJob> loadActiveFromDedupe(String dedupePointerKey) {
    Pointer dedupePointer = pointerStore.get(dedupePointerKey).orElse(null);
    if (dedupePointer == null) {
      return Optional.empty();
    }

    var recordOpt = resolveCurrentRecordByReferenceBlobUri(dedupePointer.getBlobUri());
    if (recordOpt.isEmpty()) {
      pointerStore.compareAndDelete(dedupePointerKey, dedupePointer.getVersion());
      return Optional.empty();
    }

    StoredReconcileJob record = recordOpt.get();
    if (record.accountId == null
        || record.accountId.isBlank()
        || record.jobId == null
        || record.jobId.isBlank()) {
      pointerStore.compareAndDelete(dedupePointerKey, dedupePointer.getVersion());
      return Optional.empty();
    }

    String canonicalPointerKey = Keys.reconcileJobPointerById(record.accountId, record.jobId);
    Pointer canonicalPointer = pointerStore.get(canonicalPointerKey).orElse(null);
    if (canonicalPointer == null) {
      pointerStore.compareAndDelete(dedupePointerKey, dedupePointer.getVersion());
      return Optional.empty();
    }

    var canonicalRecordOpt = readRecord(canonicalPointer);
    if (canonicalRecordOpt.isEmpty()) {
      LOG.errorf(
          "Canonical reconcile job pointer exists but payload is missing for job %s canonical=%s"
              + " blob=%s",
          record.jobId, canonicalPointerKey, canonicalPointer.getBlobUri());
      return Optional.of(record);
    }
    record = canonicalRecordOpt.get();

    String referenceBlobUri = stableReferenceBlobUri(record.accountId, record.jobId);
    replacePointerBlobUriIfMatch(dedupePointerKey, dedupePointer.getBlobUri(), referenceBlobUri);

    if (!repairLookupPointer(record.jobId, referenceBlobUri)) {
      LOG.warnf("Failed to repair reconcile lookup pointer for active job %s", record.jobId);
    }
    if (!repairParentPointer(record, referenceBlobUri)) {
      LOG.warnf(
          "Failed to repair reconcile parent pointer for active job %s parentJobId=%s",
          record.jobId, record.parentJobId());
    }

    if (isTerminalState(record.state)) {
      pointerStore.compareAndDelete(dedupePointerKey, dedupePointer.getVersion());
      return Optional.empty();
    }

    if (requiresLaneQueuePointer(record)
        && !hasValidLaneQueuePointer(record.laneQueuePointerKey, referenceBlobUri)
        && !repairLaneQueuePointer(canonicalPointerKey, record, referenceBlobUri)) {
      LOG.warnf(
          "Active reconcile job %s state=%s has no valid lane queue pointer and repair failed",
          record.jobId, record.state);
    }

    Pointer repairedCanonicalPointer =
        pointerStore.get(canonicalPointerKey).orElse(canonicalPointer);
    var repairedRecordOpt = readRecord(repairedCanonicalPointer);
    return repairedRecordOpt.isPresent() ? repairedRecordOpt : Optional.of(record);
  }

  private boolean reclaimExpiredLeasesIfDue(long nowMs) {
    if (nowMs - lastReclaimAtMs < reclaimIntervalMs) {
      return false;
    }
    if (!reclaimLock.tryLock()) {
      return false;
    }
    try {
      if (nowMs - lastReclaimAtMs < reclaimIntervalMs) {
        return false;
      }
      lastReclaimAtMs = nowMs;

      reclaimLeaseExpiryPointers(nowMs);
      return true;
    } finally {
      reclaimLock.unlock();
    }
  }

  private void reclaimLeaseExpiryPointers(long nowMs) {
    String token = "";
    int processed = 0;
    while (processed < reclaimBatchLimit) {
      StringBuilder next = new StringBuilder();
      List<Pointer> expiryPointers =
          pointerStore.listPointersByPrefix(
              Keys.reconcileLeaseExpiryPointerPrefix(), reclaimBatchLimit, token, next);
      if (expiryPointers.isEmpty()) {
        return;
      }
      for (Pointer expiryPointer : expiryPointers) {
        LeaseExpiryPointerTarget target = decodeLeaseExpiryPointerTarget(expiryPointer.getKey());
        if (target == null) {
          pointerStore.compareAndDelete(expiryPointer.getKey(), expiryPointer.getVersion());
          continue;
        }
        if (target.dueAtMs() > nowMs) {
          return;
        }
        reclaimLeaseExpiryPointer(expiryPointer, target, nowMs);
        processed++;
        if (processed >= reclaimBatchLimit) {
          return;
        }
      }
      String nextToken = next.toString();
      if (nextToken.isBlank() || nextToken.equals(token)) {
        return;
      }
      token = nextToken;
    }
  }

  private void reclaimLeaseExpiryPointer(
      Pointer expiryPointer, LeaseExpiryPointerTarget target, long nowMs) {
    String canonicalKey = Keys.reconcileJobPointerById(target.accountId(), target.jobId());
    Pointer canonicalPointer = pointerStore.get(canonicalKey).orElse(null);
    if (canonicalPointer == null) {
      pointerStore.compareAndDelete(expiryPointer.getKey(), expiryPointer.getVersion());
      return;
    }
    var canonicalRecordOpt = readRecordByBlobUri(canonicalPointer.getBlobUri());
    if (canonicalRecordOpt.isEmpty()) {
      pointerStore.compareAndDelete(expiryPointer.getKey(), expiryPointer.getVersion());
      return;
    }
    var canonicalRecord = canonicalRecordOpt.get();
    StoredLeaseState leaseState =
        readLeaseState(canonicalRecord.accountId, canonicalRecord.jobId).orElse(null);
    if (leaseState == null
        || blank(leaseState.leaseEpoch)
        || leaseState.leaseExpiresAtMs != target.dueAtMs()
        || leaseState.leaseExpiresAtMs > nowMs) {
      pointerStore.compareAndDelete(expiryPointer.getKey(), expiryPointer.getVersion());
      return;
    }

    boolean reclaimed =
        mutateByCanonicalPointerResult(
            canonicalKey,
            record -> {
              StoredLeaseState currentLeaseState =
                  readLeaseState(record.accountId, record.jobId).orElse(null);
              if (!"JS_RUNNING".equals(record.state)
                  && !"JS_CANCELLING".equals(record.state)
                  && !"JS_QUEUED".equals(record.state)) {
                return null;
              }
              if (currentLeaseState == null
                  || !blankToEmpty(currentLeaseState.leaseEpoch)
                      .equals(blankToEmpty(leaseState.leaseEpoch))
                  || currentLeaseState.leaseExpiresAtMs != target.dueAtMs()
                  || currentLeaseState.leaseExpiresAtMs <= 0L
                  || currentLeaseState.leaseExpiresAtMs > nowMs) {
                return null;
              }

              boolean wasCancelling = "JS_CANCELLING".equals(record.state);
              record.state = wasCancelling ? "JS_CANCELLING" : "JS_QUEUED";
              record.waitingOnDependency = false;
              record.message =
                  wasCancelling
                      ? (blank(record.message) ? "Lease expired while cancelling" : record.message)
                      : "Lease expired; requeued";
              if (!wasCancelling) {
                record.executorId = "";
                releaseLeaseOwnership(record);
              }
              record.nextAttemptAtMs = nowMs;

              String laneQueueKey =
                  Keys.reconcileLaneQueuePointerByDue(
                      nowMs, record.accountId, record.laneKey, record.jobId);
              record.readyPointerKey = null;
              record.laneQueuePointerKey = laneQueueKey;
              return record;
            });
    if (!reclaimed) {
      return;
    }
    deleteLeaseArtifacts(canonicalRecord, leaseState);
    refreshRunnableLaneHead(canonicalRecord.accountId, canonicalRecord.laneKey);
    syncAggregateChainByJobId(canonicalRecord.jobId, "reclaimLease");
  }

  private Optional<LeasedJob> leaseReadyDue(long nowMs, LeaseRequest request) {
    LeaseReadyDueMetrics metrics = new LeaseReadyDueMetrics(nowMs);
    long startedNs = System.nanoTime();
    String token = "";
    int pages = 0;
    while (true) {
      boolean restartScan = false;
      StringBuilder next = new StringBuilder();
      long listStartedNs = System.nanoTime();
      List<Pointer> runnableHeads =
          pointerStore.listPointersByPrefix(
              Keys.reconcileRunnableLanePointerPrefix(), readyScanLimit, token, next);
      metrics.listPointersByPrefixNs += System.nanoTime() - listStartedNs;
      metrics.readyPagesScanned++;
      if (runnableHeads.isEmpty()) {
        logLeaseReadyDueMetrics(metrics, startedNs, false);
        return Optional.empty();
      }

      for (Pointer candidate : runnableHeads) {
        metrics.readyCandidatesScanned++;
        long dueAt = parseDueMillis(candidate.getKey());
        if (dueAt > nowMs) {
          logLeaseReadyDueMetrics(metrics, startedNs, false);
          return Optional.empty();
        }

        var runnableTarget = decodeRunnableLanePointerTarget(candidate.getKey());
        if (runnableTarget == null) {
          continue;
        }
        long candidateReadStartedNs = System.nanoTime();
        var candidateRecordOpt = readRunnableLaneCandidateRecord(candidate, runnableTarget);
        metrics.candidateReadRecordNs += System.nanoTime() - candidateReadStartedNs;
        if (candidateRecordOpt.isEmpty()) {
          continue;
        }
        StoredReconcileJob candidateRecord = candidateRecordOpt.get();
        if (!matchesLeaseRequest(candidateRecord, request)) {
          continue;
        }

        long canonicalGetStartedNs = System.nanoTime();
        Pointer canonicalPointer =
            pointerStore.get(runnableTarget.canonicalPointerKey()).orElse(null);
        metrics.canonicalPointerGetNs += System.nanoTime() - canonicalGetStartedNs;
        if (canonicalPointer == null) {
          refreshRunnableLaneHead(runnableTarget.accountId(), runnableTarget.laneKey());
          restartScan = true;
          break;
        }
        var recordOpt = readRecord(canonicalPointer);
        if (recordOpt.isEmpty()) {
          continue;
        }
        StoredReconcileJob record = recordOpt.get();
        if (!matchesLeaseRequest(record, request)) {
          continue;
        }
        if (!requiresLaneQueuePointer(record)
            || blank(record.laneQueuePointerKey)
            || !record.laneQueuePointerKey.equals(runnableTarget.laneQueuePointerKey())) {
          refreshRunnableLaneHead(runnableTarget.accountId(), runnableTarget.laneKey());
          restartScan = true;
          break;
        }
        long laneLeaseStartedNs = System.nanoTime();
        boolean laneLeaseAcquired =
            tryAcquireLaneLease(
                record, stableReferenceBlobUri(record.accountId, record.jobId), nowMs);
        metrics.tryAcquireLaneLeaseNs += System.nanoTime() - laneLeaseStartedNs;
        if (!laneLeaseAcquired) {
          metrics.laneLeaseMisses++;
          refreshRunnableLaneHead(runnableTarget.accountId(), runnableTarget.laneKey());
          restartScan = true;
          break;
        }
        long readyDeleteStartedNs = System.nanoTime();
        boolean deleted = pointerStore.compareAndDelete(candidate.getKey(), candidate.getVersion());
        metrics.readyPointerDeleteNs += System.nanoTime() - readyDeleteStartedNs;
        if (!deleted) {
          clearLaneLeaseIfOwned(record, stableReferenceBlobUri(record.accountId, record.jobId));
          continue;
        }
        long leaseCanonicalStartedNs = System.nanoTime();
        var leased =
            leaseCanonical(
                runnableTarget.canonicalPointerKey(),
                runnableTarget.laneQueuePointerKey(),
                candidate.getKey(),
                nowMs,
                metrics);
        metrics.leaseCanonicalNs += System.nanoTime() - leaseCanonicalStartedNs;
        if (leased.isPresent()) {
          logLeaseReadyDueMetrics(metrics, startedNs, true);
          return leased;
        }
        clearLaneLeaseIfOwned(record, stableReferenceBlobUri(record.accountId, record.jobId));
        refreshRunnableLaneHead(runnableTarget.accountId(), runnableTarget.laneKey());
        restartScan = true;
        break;
      }

      if (restartScan) {
        token = "";
        pages = 0;
        continue;
      }

      String nextToken = next.toString();
      if (nextToken.isBlank()) {
        logLeaseReadyDueMetrics(metrics, startedNs, false);
        return Optional.empty();
      }
      if (nextToken.equals(token)) {
        LOG.warn(
            "Reconcile ready pagination token did not advance; aborting ready scan to avoid"
                + " livelock");
        logLeaseReadyDueMetrics(metrics, startedNs, false);
        return Optional.empty();
      }
      token = nextToken;
      pages++;
      if (pages >= 10_000) {
        LOG.warn("Reconcile ready pagination hit safety page cap; aborting scan");
        logLeaseReadyDueMetrics(metrics, startedNs, false);
        return Optional.empty();
      }
    }
  }

  private void logLeaseReadyDueMetrics(
      LeaseReadyDueMetrics metrics, long startedNs, boolean leased) {
    if (metrics == null) {
      return;
    }
    long totalMs =
        java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNs);
    long listMs =
        java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(metrics.listPointersByPrefixNs);
    long candidateReadMs =
        java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(metrics.candidateReadRecordNs);
    long canonicalGetMs =
        java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(metrics.canonicalPointerGetNs);
    long laneLeaseMs =
        java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(metrics.tryAcquireLaneLeaseNs);
    long snapshotLeaseMs =
        java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(metrics.tryAcquireSnapshotLeaseNs);
    long readyDeleteMs =
        java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(metrics.readyPointerDeleteNs);
    long leaseCanonicalMs =
        java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(metrics.leaseCanonicalNs);
    LOG.infof(
        "leaseReadyDue result=%s total_ms=%d ready_pages=%d ready_candidates=%d lane_misses=%d"
            + " snapshot_misses=%d list_ms=%d candidate_read_ms=%d canonical_get_ms=%d"
            + " lane_lease_ms=%d snapshot_lease_ms=%d ready_delete_ms=%d lease_canonical_ms=%d"
            + " now_ms=%d",
        leased ? "leased" : "empty",
        totalMs,
        metrics.readyPagesScanned,
        metrics.readyCandidatesScanned,
        metrics.laneLeaseMisses,
        metrics.snapshotLeaseMisses,
        listMs,
        candidateReadMs,
        canonicalGetMs,
        laneLeaseMs,
        snapshotLeaseMs,
        readyDeleteMs,
        leaseCanonicalMs,
        metrics.nowMs);
  }

  private Optional<StoredReconcileJob> readRunnableLaneCandidateRecord(
      Pointer runnableLanePointer, RunnableLanePointerTarget runnableTarget) {
    if (runnableLanePointer == null || runnableTarget == null) {
      return Optional.empty();
    }
    var record = resolveCurrentRecordByReferenceBlobUri(runnableLanePointer.getBlobUri());
    if (record.isEmpty()) {
      return Optional.empty();
    }
    StoredReconcileJob current = record.get();
    if (!runnableTarget.accountId().equals(current.accountId)
        || !runnableTarget.jobId().equals(current.jobId)
        || !runnableTarget.laneKey().equals(current.laneKey)) {
      LOG.warnf(
          "Reconcile runnable-lane pointer mismatch key=%s readyAccountId=%s readyLane=%s"
              + " readyJobId=%s candidateAccountId=%s candidateLane=%s candidateJobId=%s",
          runnableLanePointer.getKey(),
          runnableTarget.accountId(),
          runnableTarget.laneKey(),
          runnableTarget.jobId(),
          current.accountId,
          current.laneKey,
          current.jobId);
      return Optional.empty();
    }
    current.canonicalPointerKey = runnableTarget.canonicalPointerKey();
    return Optional.of(current);
  }

  private Optional<StoredReconcileJob> readRecord(Pointer canonicalPointer) {
    var record = readRecordByBlobUri(canonicalPointer.getBlobUri());
    if (record.isEmpty()) {
      return Optional.empty();
    }
    record.get().canonicalPointerKey = canonicalPointer.getKey();
    return record;
  }

  private Optional<StoredJobReference> readJobReferenceByBlobUri(String blobUri) {
    return readAuxiliaryRecordByBlobUri(blobUri, StoredJobReference.class)
        .filter(ref -> !blank(ref.accountId) && !blank(ref.jobId));
  }

  private Optional<StoredReconcileJob> resolveCurrentRecordByReferenceBlobUri(String blobUri) {
    var ref = readJobReferenceByBlobUri(blobUri);
    if (ref.isPresent()) {
      String canonicalPointerKey =
          blank(ref.get().canonicalPointerKey)
              ? Keys.reconcileJobPointerById(ref.get().accountId, ref.get().jobId)
              : ref.get().canonicalPointerKey;
      Pointer canonicalPointer = pointerStore.get(canonicalPointerKey).orElse(null);
      if (canonicalPointer == null) {
        return Optional.empty();
      }
      return readRecord(canonicalPointer);
    }
    return readRecordByBlobUri(blobUri);
  }

  private boolean matchesLeaseRequest(String canonicalPointerKey, LeaseRequest request) {
    LeaseRequest effective = request == null ? LeaseRequest.all() : request;
    Pointer canonicalPointer = pointerStore.get(canonicalPointerKey).orElse(null);
    if (canonicalPointer == null) {
      return false;
    }
    var record = readRecord(canonicalPointer);
    return record.isPresent() && matchesLeaseRequest(record.get(), effective);
  }

  private boolean matchesLeaseRequest(StoredReconcileJob record, LeaseRequest request) {
    LeaseRequest effective = request == null ? LeaseRequest.all() : request;
    return record != null
        && effective.matches(record.executionPolicy(), record.pinnedExecutorId(), record.jobKind());
  }

  private StoredAggregateSummary projectedSummaryForRead(StoredReconcileJob stored) {
    if (stored == null) {
      return StoredAggregateSummary.empty();
    }
    if (!supportsChildAggregation(stored.jobKind())) {
      return aggregateSummary(stored, null);
    }
    StoredAggregateRollup rollup = repairAggregateRollupForReadIfMissing(stored);
    return aggregateSummary(stored, rollup);
  }

  private StoredAggregateRollup repairAggregateRollupForReadIfMissing(StoredReconcileJob job) {
    if (job == null || !supportsChildAggregation(job.jobKind())) {
      return null;
    }
    return readAggregateRollup(job.accountId, job.jobId)
        .orElseGet(() -> rebuildAggregateRollup(job));
  }

  private ReconcileJob toPublicJob(StoredReconcileJob stored, StoredAggregateSummary summary) {
    StoredAggregateSummary projectedSummary =
        summary == null ? StoredAggregateSummary.empty() : summary;
    StoredReconcileJob projected = cloneStoredRecord(stored);
    return new ReconcileJob(
        projected.jobId,
        projected.accountId,
        projected.connectorId,
        blankToEmpty(projectedSummary.state),
        blankToEmpty(projectedSummary.message),
        projectedSummary.startedAtMs,
        projectedSummary.finishedAtMs,
        projectedSummary.tablesScanned,
        projectedSummary.tablesChanged,
        projectedSummary.viewsScanned,
        projectedSummary.viewsChanged,
        projectedSummary.errors,
        projected.fullRescan,
        projected.captureMode(),
        projectedSummary.snapshotsProcessed,
        projectedSummary.statsProcessed,
        projectedSummary.indexesProcessed,
        supportsChildAggregation(projected.jobKind()),
        projected.toScope(),
        projected.executionPolicy(),
        projected.pinnedExecutorId(),
        blankToEmpty(projectedSummary.executorId),
        projected.jobKind(),
        projected.tableTask(),
        projected.viewTask(),
        projected.snapshotTask(),
        projected.fileGroupTask(),
        projectedSummary.plannedFileGroups,
        projectedSummary.plannedFiles,
        projectedSummary.completedFileGroups,
        projectedSummary.failedFileGroups,
        projectedSummary.completedFiles,
        projectedSummary.failedFiles,
        projected.parentJobId());
  }

  private boolean upsertBlobPointer(String pointerKey, String blobUri) {
    for (int i = 0; i < CAS_MAX; i++) {
      Pointer existing = pointerStore.get(pointerKey).orElse(null);
      if (existing == null) {
        Pointer created =
            Pointer.newBuilder().setKey(pointerKey).setBlobUri(blobUri).setVersion(1L).build();
        if (pointerStore.compareAndSet(pointerKey, 0L, created)) {
          return true;
        }
        continue;
      }
      if (blobUri.equals(existing.getBlobUri())) {
        return true;
      }

      Pointer next =
          Pointer.newBuilder()
              .setKey(pointerKey)
              .setBlobUri(blobUri)
              .setVersion(existing.getVersion() + 1)
              .build();
      if (pointerStore.compareAndSet(pointerKey, existing.getVersion(), next)) {
        return true;
      }
    }

    return false;
  }

  private void clearReadyPointer(String readyPointerKey) {
    if (readyPointerKey == null || readyPointerKey.isBlank()) {
      return;
    }
    Pointer existing = pointerStore.get(readyPointerKey).orElse(null);
    if (existing != null) {
      pointerStore.compareAndDelete(readyPointerKey, existing.getVersion());
    }
  }

  private void refreshRunnableLaneHead(String accountId, String laneKey) {
    if (blank(accountId) || blank(laneKey)) {
      return;
    }

    String laneHeadKey = Keys.reconcileLaneHeadPointer(accountId, laneKey);
    Pointer previousLaneHead = pointerStore.get(laneHeadKey).orElse(null);
    String previousRunnableKey =
        runnableLanePointerKeyForBlob(accountId, laneKey, previousLaneHead);

    Pointer laneQueueHead = firstRunnableLaneQueuePointer(accountId, laneKey).orElse(null);
    if (laneQueueHead == null) {
      if (previousLaneHead != null) {
        clearPointerIfMatches(laneHeadKey, previousLaneHead.getBlobUri());
      }
      if (!blank(previousRunnableKey)) {
        clearReadyPointer(previousRunnableKey);
      }
      return;
    }

    var queueTarget = decodeLaneQueuePointerTarget(laneQueueHead.getKey());
    if (queueTarget == null) {
      return;
    }

    long now = System.currentTimeMillis();
    Pointer laneLeasePointer =
        pointerStore.get(Keys.reconcileLaneLeasePointer(accountId, laneKey)).orElse(null);
    boolean laneBusy =
        laneLeasePointer != null
            && currentJobRecordForBlobUri(laneLeasePointer.getBlobUri())
                .filter(owner -> hasActiveLaneLease(owner, now))
                .isPresent();

    String nextRunnableKey =
        Keys.reconcileRunnableLanePointerByDue(
            queueTarget.dueAtMs(), accountId, laneKey, queueTarget.jobId());
    upsertBlobPointer(laneHeadKey, laneQueueHead.getBlobUri());
    if (laneBusy) {
      if (!blank(previousRunnableKey)) {
        clearReadyPointer(previousRunnableKey);
      }
      clearReadyPointer(nextRunnableKey);
      return;
    }

    upsertBlobPointer(nextRunnableKey, laneQueueHead.getBlobUri());

    if (!blank(previousRunnableKey) && !previousRunnableKey.equals(nextRunnableKey)) {
      clearReadyPointer(previousRunnableKey);
    }
  }

  private Optional<Pointer> firstRunnableLaneQueuePointer(String accountId, String laneKey) {
    if (blank(accountId) || blank(laneKey)) {
      return Optional.empty();
    }
    String token = "";
    do {
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers =
          pointerStore.listPointersByPrefix(
              Keys.reconcileLaneQueuePointerPrefix(accountId, laneKey), 32, token, next);
      for (Pointer pointer : pointers) {
        var recordOpt = readCurrentRecordFromIndexPointer(pointer, pointer.getKey());
        if (recordOpt.isEmpty()) {
          pointerStore.compareAndDelete(pointer.getKey(), pointer.getVersion());
          continue;
        }
        StoredReconcileJob record = recordOpt.get();
        String referenceBlobUri = stableReferenceBlobUri(record.accountId, record.jobId);
        if (!requiresLaneQueuePointer(record)
            || blank(record.laneQueuePointerKey)
            || !pointer.getKey().equals(record.laneQueuePointerKey)
            || !referenceBlobUri.equals(pointer.getBlobUri())) {
          pointerStore.compareAndDelete(pointer.getKey(), pointer.getVersion());
          continue;
        }
        return Optional.of(pointer);
      }
      token = next.toString();
    } while (token != null && !token.isBlank());
    return Optional.empty();
  }

  private String runnableLanePointerKeyForBlob(
      String accountId, String laneKey, Pointer laneHeadPointer) {
    if (blank(accountId) || blank(laneKey) || laneHeadPointer == null) {
      return "";
    }
    var record = currentJobRecordForBlobUri(laneHeadPointer.getBlobUri());
    if (record.isEmpty()) {
      return "";
    }
    String laneQueuePointerKey = blankToEmpty(record.get().laneQueuePointerKey);
    LaneQueuePointerTarget queueTarget = decodeLaneQueuePointerTarget(laneQueuePointerKey);
    if (queueTarget != null) {
      return Keys.reconcileRunnableLanePointerByDue(
          queueTarget.dueAtMs(), accountId, laneKey, queueTarget.jobId());
    }
    long dueAt =
        record.get().nextAttemptAtMs > 0L
            ? record.get().nextAttemptAtMs
            : System.currentTimeMillis();
    return Keys.reconcileRunnableLanePointerByDue(dueAt, accountId, laneKey, record.get().jobId);
  }

  private void clearDedupeIfOwned(StoredReconcileJob record) {
    if (record.dedupeKey == null || record.dedupeKey.isBlank()) {
      return;
    }
    String dedupeKey = Keys.reconcileDedupePointer(record.accountId, hashValue(record.dedupeKey));
    Pointer existing = pointerStore.get(dedupeKey).orElse(null);
    if (existing == null) {
      return;
    }
    var owner = resolveCurrentRecordByReferenceBlobUri(existing.getBlobUri());
    if (owner.isPresent()
        && record.jobId.equals(owner.get().jobId)
        && record.accountId.equals(owner.get().accountId)) {
      pointerStore.compareAndDelete(dedupeKey, existing.getVersion());
    }
  }

  private boolean repairLookupPointer(String jobId, String blobUri) {
    if (jobId == null || jobId.isBlank() || blobUri == null || blobUri.isBlank()) {
      return false;
    }
    return upsertBlobPointer(Keys.reconcileJobLookupPointerById(jobId), blobUri);
  }

  private boolean repairParentPointer(StoredReconcileJob record, String blobUri) {
    if (record == null || blank(record.parentJobId())) {
      return true;
    }
    if (blank(record.accountId) || blank(record.jobId) || blank(blobUri)) {
      return false;
    }
    return upsertBlobPointer(
        parentPointerKey(record.accountId, record.parentJobId(), record.jobId), blobUri);
  }

  private boolean hasValidLaneQueuePointer(String laneQueuePointerKey, String expectedBlobUri) {
    if (laneQueuePointerKey == null
        || laneQueuePointerKey.isBlank()
        || expectedBlobUri == null
        || expectedBlobUri.isBlank()) {
      return false;
    }
    Pointer existing = pointerStore.get(laneQueuePointerKey).orElse(null);
    return existing != null && expectedBlobUri.equals(existing.getBlobUri());
  }

  private boolean repairLaneQueuePointer(
      String canonicalPointerKey, StoredReconcileJob record, String blobUri) {
    if (canonicalPointerKey == null
        || canonicalPointerKey.isBlank()
        || record == null
        || blobUri == null
        || blobUri.isBlank()) {
      return false;
    }
    boolean synthesizedKey =
        record.laneQueuePointerKey == null || record.laneQueuePointerKey.isBlank();
    long dueAt = record.nextAttemptAtMs > 0L ? record.nextAttemptAtMs : System.currentTimeMillis();
    String laneQueuePointerKey =
        synthesizedKey
            ? Keys.reconcileLaneQueuePointerByDue(
                dueAt, record.accountId, record.laneKey, record.jobId)
            : record.laneQueuePointerKey;
    boolean repaired = upsertBlobPointer(laneQueuePointerKey, blobUri);
    if (repaired) {
      record.readyPointerKey = null;
      record.laneQueuePointerKey = laneQueuePointerKey;
      if (synthesizedKey) {
        persistLaneQueuePointerKeyIfMissing(canonicalPointerKey, laneQueuePointerKey);
      }
      refreshRunnableLaneHead(record.accountId, record.laneKey);
    }
    return repaired;
  }

  private Optional<StoredReconcileJob> currentJobRecordForBlobUri(String blobUri) {
    var ownerOpt = resolveCurrentRecordByReferenceBlobUri(blobUri);
    if (ownerOpt.isEmpty()) {
      return Optional.empty();
    }
    return currentJobRecordForReference(ownerOpt.get());
  }

  private Optional<StoredReconcileJob> currentJobRecordForReference(StoredReconcileJob record) {
    if (record == null
        || record.accountId == null
        || record.accountId.isBlank()
        || record.jobId == null
        || record.jobId.isBlank()) {
      return Optional.ofNullable(record);
    }
    String canonicalKey = Keys.reconcileJobPointerById(record.accountId, record.jobId);
    Pointer canonicalPointer = pointerStore.get(canonicalKey).orElse(null);
    if (canonicalPointer == null) {
      return Optional.of(record);
    }
    var currentOpt = readRecord(canonicalPointer);
    return currentOpt.isPresent() ? currentOpt : Optional.of(record);
  }

  private void persistLaneQueuePointerKeyIfMissing(
      String canonicalPointerKey, String laneQueuePointerKey) {
    mutateByCanonicalPointer(
        canonicalPointerKey,
        existing -> {
          if (!requiresLaneQueuePointer(existing)) {
            return existing;
          }
          if (existing.laneQueuePointerKey != null && !existing.laneQueuePointerKey.isBlank()) {
            return existing;
          }
          existing.readyPointerKey = null;
          existing.laneQueuePointerKey = laneQueuePointerKey;
          return existing;
        });
  }

  private boolean tryAcquireSnapshotLease(
      StoredReconcileJob record, String expectedBlobUri, long now) {
    String pointerKey = snapshotLeasePointerKey(record);
    if (pointerKey.isBlank()) {
      return true;
    }
    for (int i = 0; i < CAS_MAX; i++) {
      Pointer existing = pointerStore.get(pointerKey).orElse(null);
      if (existing == null) {
        Pointer created =
            Pointer.newBuilder()
                .setKey(pointerKey)
                .setBlobUri(expectedBlobUri)
                .setVersion(1L)
                .build();
        if (pointerStore.compareAndSet(pointerKey, 0L, created)) {
          return true;
        }
        continue;
      }
      if (expectedBlobUri.equals(existing.getBlobUri())) {
        var owner = currentJobRecordForBlobUri(existing.getBlobUri());
        if (owner.isEmpty()) {
          pointerStore.compareAndDelete(pointerKey, existing.getVersion());
          continue;
        }
        if (record.jobId.equals(owner.get().jobId)
            && record.accountId.equals(owner.get().accountId)) {
          return true;
        }
        if (!hasActiveSnapshotLease(owner.get(), now)) {
          pointerStore.compareAndDelete(pointerKey, existing.getVersion());
          continue;
        }
        return false;
      }
      var owner = currentJobRecordForBlobUri(existing.getBlobUri());
      if (owner.isEmpty()) {
        pointerStore.compareAndDelete(pointerKey, existing.getVersion());
        continue;
      }
      if (record.jobId.equals(owner.get().jobId)
          && record.accountId.equals(owner.get().accountId)) {
        return true;
      }
      if (!hasActiveSnapshotLease(owner.get(), now)) {
        pointerStore.compareAndDelete(pointerKey, existing.getVersion());
        continue;
      }
      return false;
    }
    return false;
  }

  private void clearSnapshotLeaseIfOwned(StoredReconcileJob record, String expectedBlobUri) {
    String pointerKey = snapshotLeasePointerKey(record);
    if (pointerKey.isBlank()) {
      return;
    }
    Pointer existing = pointerStore.get(pointerKey).orElse(null);
    if (existing == null) {
      return;
    }
    if (expectedBlobUri != null
        && !expectedBlobUri.isBlank()
        && !expectedBlobUri.equals(existing.getBlobUri())) {
      return;
    }
    var owner = currentJobRecordForBlobUri(existing.getBlobUri());
    if (owner.isEmpty()
        || !record.jobId.equals(owner.get().jobId)
        || !record.accountId.equals(owner.get().accountId)) {
      return;
    }
    if (hasActiveSnapshotLease(owner.get(), System.currentTimeMillis())) {
      replacePointerBlobUriIfMatch(
          pointerKey,
          existing.getBlobUri(),
          stableReferenceBlobUri(record.accountId, record.jobId));
      return;
    }
    pointerStore.compareAndDelete(pointerKey, existing.getVersion());
  }

  private void clearDedupeReservation(String dedupePointerKey, String expectedBlobUri) {
    clearPointerIfMatches(dedupePointerKey, expectedBlobUri);
  }

  private void clearPointerIfMatches(String pointerKey, String expectedBlobUri) {
    if (pointerKey == null || pointerKey.isBlank()) {
      return;
    }
    Pointer existing = pointerStore.get(pointerKey).orElse(null);
    if (existing == null) {
      return;
    }
    if (expectedBlobUri != null
        && !expectedBlobUri.isBlank()
        && !expectedBlobUri.equals(existing.getBlobUri())) {
      return;
    }
    pointerStore.compareAndDelete(pointerKey, existing.getVersion());
  }

  private void clearPointerIfMatchesAny(
      String pointerKey, String primaryExpectedBlobUri, String secondaryExpectedBlobUri) {
    if (pointerKey == null || pointerKey.isBlank()) {
      return;
    }
    Pointer existing = pointerStore.get(pointerKey).orElse(null);
    if (existing == null) {
      return;
    }
    String existingBlobUri = existing.getBlobUri();
    boolean primaryMatch =
        primaryExpectedBlobUri != null
            && !primaryExpectedBlobUri.isBlank()
            && primaryExpectedBlobUri.equals(existingBlobUri);
    boolean secondaryMatch =
        secondaryExpectedBlobUri != null
            && !secondaryExpectedBlobUri.isBlank()
            && secondaryExpectedBlobUri.equals(existingBlobUri);
    if (!primaryMatch && !secondaryMatch) {
      return;
    }
    pointerStore.compareAndDelete(pointerKey, existing.getVersion());
  }

  private Optional<StoredReconcileJob> readRecordByBlobUri(String blobUri) {
    byte[] payload;
    try {
      payload = blobStore.get(blobUri);
    } catch (StorageNotFoundException e) {
      LOG.warnf(
          e, "Reconcile job blob missing during reclaim, treating as absent blob=%s", blobUri);
      return Optional.empty();
    }
    if (payload == null || payload.length == 0) {
      return Optional.empty();
    }
    try {
      return Optional.ofNullable(mapper.readValue(payload, StoredReconcileJob.class));
    } catch (Exception e) {
      LOG.warnf(e, "Failed to decode reconcile job payload blob=%s", blobUri);
      return Optional.empty();
    }
  }

  private void reconcileIndexPointers(
      StoredReconcileJob previous,
      StoredReconcileJob current,
      String oldBlobUri,
      String newBlobUri) {
    if (current == null
        || oldBlobUri == null
        || oldBlobUri.isBlank()
        || newBlobUri == null
        || newBlobUri.isBlank()
        || oldBlobUri.equals(newBlobUri)) {
      return;
    }
    String referenceBlobUri = stableReferenceBlobUri(current.accountId, current.jobId);
    String previousReferenceBlobUri =
        previous == null ? "" : stableReferenceBlobUri(previous.accountId, previous.jobId);

    if (!upsertBlobPointer(Keys.reconcileJobLookupPointerById(current.jobId), referenceBlobUri)) {
      LOG.errorf("Failed to update reconcile lookup pointer for job %s", current.jobId);
    }

    String previousParentPointerKey =
        previous == null
            ? ""
            : parentPointerKey(previous.accountId, previous.parentJobId(), previous.jobId);
    String currentParentPointerKey =
        parentPointerKey(current.accountId, current.parentJobId(), current.jobId);
    if (!currentParentPointerKey.isBlank()) {
      if (!upsertBlobPointer(currentParentPointerKey, referenceBlobUri)) {
        LOG.errorf(
            "Failed to update reconcile parent pointer for job %s parentJobId=%s",
            current.jobId, current.parentJobId());
      }
    }
    if (!previousParentPointerKey.isBlank()
        && !previousParentPointerKey.equals(currentParentPointerKey)) {
      clearPointerIfMatchesAny(previousParentPointerKey, previousReferenceBlobUri, oldBlobUri);
    }

    String previousLegacyReadyPointerKey =
        previous == null ? "" : blankToEmpty(previous.readyPointerKey);
    String currentLegacyReadyPointerKey = blankToEmpty(current.readyPointerKey);
    if (!previousLegacyReadyPointerKey.isBlank()
        && !previousLegacyReadyPointerKey.equals(currentLegacyReadyPointerKey)) {
      clearPointerIfMatchesAny(previousLegacyReadyPointerKey, previousReferenceBlobUri, oldBlobUri);
    }
    if (!currentLegacyReadyPointerKey.isBlank()) {
      clearReadyPointer(currentLegacyReadyPointerKey);
    }

    String previousLaneQueuePointerKey =
        previous == null ? "" : blankToEmpty(previous.laneQueuePointerKey);
    String currentLaneQueuePointerKey = blankToEmpty(current.laneQueuePointerKey);
    if (!currentLaneQueuePointerKey.isBlank() && requiresLaneQueuePointer(current)) {
      if (!upsertBlobPointer(currentLaneQueuePointerKey, referenceBlobUri)) {
        LOG.errorf(
            "Failed to update reconcile lane queue pointer for job %s state=%s queueKey=%s",
            current.jobId, current.state, currentLaneQueuePointerKey);
      }
    }
    if (!previousLaneQueuePointerKey.isBlank()
        && !previousLaneQueuePointerKey.equals(currentLaneQueuePointerKey)) {
      clearPointerIfMatchesAny(previousLaneQueuePointerKey, previousReferenceBlobUri, oldBlobUri);
    }

    long now = System.currentTimeMillis();
    String previousLanePointerKey = previous == null ? "" : laneLeasePointerKey(previous);
    String currentLanePointerKey = laneLeasePointerKey(current);
    if (hasActiveLaneLease(current, now) && !currentLanePointerKey.isBlank()) {
      if (!upsertBlobPointer(currentLanePointerKey, referenceBlobUri)) {
        LOG.warnf(
            "Failed to update reconcile lane lease pointer for job %s laneKey=%s",
            current.jobId, current.laneKey);
      }
    }
    if (!previousLanePointerKey.isBlank()
        && (!previousLanePointerKey.equals(currentLanePointerKey)
            || !hasActiveLaneLease(current, now))) {
      clearPointerIfMatchesAny(previousLanePointerKey, previousReferenceBlobUri, oldBlobUri);
    }
    boolean laneQueueChanged = !previousLaneQueuePointerKey.equals(currentLaneQueuePointerKey);
    boolean laneLeaseChanged =
        !previousLanePointerKey.equals(currentLanePointerKey)
            || hasActiveLaneLease(previous, now) != hasActiveLaneLease(current, now);
    if ((laneQueueChanged || laneLeaseChanged)
        && !blank(current.accountId)
        && !blank(current.laneKey)) {
      refreshRunnableLaneHead(current.accountId, current.laneKey);
    } else if ((laneQueueChanged || laneLeaseChanged)
        && previous != null
        && !blank(previous.accountId)
        && !blank(previous.laneKey)) {
      refreshRunnableLaneHead(previous.accountId, previous.laneKey);
    }

    String previousSnapshotLeasePointerKey =
        previous == null ? "" : snapshotLeasePointerKey(previous);
    String currentSnapshotLeasePointerKey = snapshotLeasePointerKey(current);
    if (hasActiveSnapshotLease(current, now) && !currentSnapshotLeasePointerKey.isBlank()) {
      if (!upsertBlobPointer(currentSnapshotLeasePointerKey, referenceBlobUri)) {
        LOG.warnf(
            "Failed to update reconcile snapshot lease pointer for job %s tableId=%s snapshotId=%d",
            current.jobId, current.snapshotTaskTableId, current.snapshotTaskSnapshotId);
      }
    }
    if (!previousSnapshotLeasePointerKey.isBlank()
        && (!previousSnapshotLeasePointerKey.equals(currentSnapshotLeasePointerKey)
            || !hasActiveSnapshotLease(current, now))) {
      clearPointerIfMatchesAny(
          previousSnapshotLeasePointerKey, previousReferenceBlobUri, oldBlobUri);
    }

    String previousDedupePointerKey = previous == null ? "" : dedupePointerKey(previous);
    String currentDedupePointerKey = dedupePointerKey(current);
    if (!currentDedupePointerKey.isBlank() && !isTerminalState(current.state)) {
      if (!upsertBlobPointer(currentDedupePointerKey, referenceBlobUri)) {
        LOG.warnf("Failed to update reconcile dedupe pointer for job %s", current.jobId);
      }
    }
    if (!previousDedupePointerKey.isBlank()
        && (!previousDedupePointerKey.equals(currentDedupePointerKey)
            || isTerminalState(current.state))) {
      clearPointerIfMatchesAny(previousDedupePointerKey, previousReferenceBlobUri, oldBlobUri);
    }
  }

  private void replacePointerBlobUriIfMatch(
      String pointerKey, String expectedBlobUri, String nextBlobUri) {
    for (int i = 0; i < CAS_MAX; i++) {
      Pointer existing = pointerStore.get(pointerKey).orElse(null);
      if (existing == null) {
        return;
      }
      if (nextBlobUri.equals(existing.getBlobUri())) {
        return;
      }
      if (!expectedBlobUri.equals(existing.getBlobUri())) {
        return;
      }
      Pointer next =
          Pointer.newBuilder()
              .setKey(pointerKey)
              .setBlobUri(nextBlobUri)
              .setVersion(existing.getVersion() + 1L)
              .build();
      if (pointerStore.compareAndSet(pointerKey, existing.getVersion(), next)) {
        return;
      }
    }
  }

  private RunnableLanePointerTarget decodeRunnableLanePointerTarget(String runnablePointerKey) {
    String normalizedKey = normalizePointerKey(runnablePointerKey);
    String prefix = normalizePointerKey(Keys.reconcileRunnableLanePointerPrefix());
    if (!normalizedKey.startsWith(prefix)) {
      return null;
    }
    String[] parts = normalizedKey.substring(prefix.length()).split("/");
    if (parts.length < 4) {
      return null;
    }
    try {
      long dueAtMs = Long.parseLong(parts[0]);
      String accountId = URLDecoder.decode(parts[1], StandardCharsets.UTF_8);
      String laneKey = URLDecoder.decode(parts[2], StandardCharsets.UTF_8);
      String jobId = URLDecoder.decode(parts[3], StandardCharsets.UTF_8);
      return new RunnableLanePointerTarget(
          accountId,
          laneKey,
          jobId,
          dueAtMs,
          Keys.reconcileJobPointerById(accountId, jobId),
          Keys.reconcileLaneQueuePointerByDue(dueAtMs, accountId, laneKey, jobId));
    } catch (Exception e) {
      return null;
    }
  }

  private LaneQueuePointerTarget decodeLaneQueuePointerTarget(String laneQueuePointerKey) {
    String normalizedKey = normalizePointerKey(laneQueuePointerKey);
    if (!normalizedKey.startsWith("/accounts/")) {
      return null;
    }
    String[] parts = normalizedKey.substring(1).split("/");
    if (parts.length < 7) {
      return null;
    }
    if (!"accounts".equals(parts[0])
        || !"reconcile".equals(parts[2])
        || !"lane-queues".equals(parts[3])) {
      return null;
    }
    try {
      String accountId = URLDecoder.decode(parts[1], StandardCharsets.UTF_8);
      String laneKey = URLDecoder.decode(parts[4], StandardCharsets.UTF_8);
      long dueAtMs = Long.parseLong(parts[5]);
      String jobId = URLDecoder.decode(parts[6], StandardCharsets.UTF_8);
      return new LaneQueuePointerTarget(
          accountId, laneKey, jobId, dueAtMs, Keys.reconcileJobPointerById(accountId, jobId));
    } catch (Exception e) {
      return null;
    }
  }

  private LeaseExpiryPointerTarget decodeLeaseExpiryPointerTarget(String leaseExpiryPointerKey) {
    String normalizedKey = normalizePointerKey(leaseExpiryPointerKey);
    String prefix = normalizePointerKey(Keys.reconcileLeaseExpiryPointerPrefix());
    if (!normalizedKey.startsWith(prefix)) {
      return null;
    }
    String[] parts = normalizedKey.substring(prefix.length()).split("/");
    if (parts.length < 3) {
      return null;
    }
    try {
      long dueAtMs = Long.parseLong(parts[0]);
      String accountId = URLDecoder.decode(parts[1], StandardCharsets.UTF_8);
      String jobId = URLDecoder.decode(parts[2], StandardCharsets.UTF_8);
      return new LeaseExpiryPointerTarget(accountId, jobId, dueAtMs);
    } catch (Exception e) {
      return null;
    }
  }

  private long parseDueMillis(String queuePointerKey) {
    var runnableTarget = decodeRunnableLanePointerTarget(queuePointerKey);
    if (runnableTarget != null) {
      return runnableTarget.dueAtMs();
    }
    var laneQueueTarget = decodeLaneQueuePointerTarget(queuePointerKey);
    if (laneQueueTarget != null) {
      return laneQueueTarget.dueAtMs();
    }
    return Long.MAX_VALUE;
  }

  private static String normalizePointerKey(String key) {
    if (key == null || key.isBlank()) {
      return "/";
    }
    return key.startsWith("/") ? key : "/" + key;
  }

  private long backoffMs(int attempts) {
    long base = baseBackoffMs * (1L << Math.min(8, Math.max(0, attempts - 1)));
    return Math.min(maxBackoffMs, base);
  }

  private boolean hasActiveSnapshotLease(StoredReconcileJob record, long now) {
    StoredLeaseState leaseState = currentLeaseState(record).orElse(null);
    return record != null
        && record.jobKind() == ReconcileJobKind.PLAN_SNAPSHOT
        && record.snapshotTaskTableId != null
        && !record.snapshotTaskTableId.isBlank()
        && record.snapshotTaskSnapshotId >= 0L
        && ("JS_RUNNING".equals(record.state) || "JS_CANCELLING".equals(record.state))
        && leaseState != null
        && leaseState.leaseExpiresAtMs > now;
  }

  private Optional<StoredLeaseState> currentLeaseState(StoredReconcileJob record) {
    if (record == null || blank(record.accountId) || blank(record.jobId)) {
      return Optional.empty();
    }
    return readLeaseState(record.accountId, record.jobId);
  }

  private LeaseUpdate applyLeaseRenewalForMutation(
      String jobId,
      String leaseEpoch,
      StoredReconcileJob current,
      String op,
      boolean allowQueued,
      boolean allowCancelling,
      boolean requireUnexpiredLease) {
    boolean cancellationRequested =
        current != null
            && ("JS_CANCELLING".equals(current.state) || "JS_CANCELLED".equals(current.state));
    if (!hasActiveLease(
        jobId, leaseEpoch, current, op, allowQueued, allowCancelling, requireUnexpiredLease)) {
      return new LeaseUpdate(false, cancellationRequested);
    }
    long now = System.currentTimeMillis();
    StoredLeaseStateEnvelope leaseStateEnvelope =
        readLeaseStateEnvelope(current.accountId, current.jobId).orElse(null);
    if (leaseStateEnvelope == null) {
      return new LeaseUpdate(false, cancellationRequested);
    }
    StoredLeaseState leaseState = leaseStateEnvelope.leaseState();
    long expiry = leaseState.leaseExpiresAtMs;
    if (expiry > 0L && now - expiry > leaseRenewGraceMs) {
      LOG.warnf(
          "Skipping %s for reconcile job %s due to lease expiry beyond grace now=%d expiry=%d"
              + " graceMs=%d",
          op, jobId, now, expiry, leaseRenewGraceMs);
      return new LeaseUpdate(false, cancellationRequested);
    }
    long nextExpiry = now + leaseMs;
    if (!updateLeaseStateExpiry(leaseStateEnvelope, nextExpiry)) {
      return new LeaseUpdate(false, cancellationRequested);
    }
    current.leaseOwner = leaseState.leaseOwner;
    current.leaseEpoch = leaseState.leaseEpoch;
    current.leaseExpiresAtMs = nextExpiry;
    return new LeaseUpdate(true, cancellationRequested);
  }

  private static void releaseLeaseOwnership(StoredReconcileJob current) {
    current.leaseOwner = null;
    current.leaseEpoch = null;
    current.leaseExpiresAtMs = 0L;
  }

  private void deleteLeaseArtifacts(StoredReconcileJob record, StoredLeaseState priorLeaseState) {
    if (record == null) {
      return;
    }
    String referenceBlobUri = stableReferenceBlobUri(record.accountId, record.jobId);
    clearLaneLeaseIfOwned(record, referenceBlobUri);
    clearSnapshotLeaseIfOwned(record, referenceBlobUri);
    if (priorLeaseState != null) {
      clearPointerIfMatches(leaseExpiryPointerKey(priorLeaseState), referenceBlobUri);
    }
    deleteLeaseStateIfOwned(record.accountId, record.jobId, priorLeaseState);
  }

  private void shortenLeaseForCancellation(StoredReconcileJob record) {
    if (record == null) {
      return;
    }
    StoredLeaseStateEnvelope leaseStateEnvelope =
        readLeaseStateEnvelope(record.accountId, record.jobId).orElse(null);
    if (leaseStateEnvelope == null) {
      return;
    }
    StoredLeaseState leaseState = leaseStateEnvelope.leaseState();
    long cancelPokeExpiry = System.currentTimeMillis() + CANCEL_POKE_MAX_DELAY_MS;
    long nextExpiry =
        leaseState.leaseExpiresAtMs <= 0L
            ? cancelPokeExpiry
            : Math.min(leaseState.leaseExpiresAtMs, cancelPokeExpiry);
    updateLeaseStateExpiry(leaseStateEnvelope, nextExpiry);
  }

  private String snapshotLeasePointerKey(StoredReconcileJob record) {
    if (record == null
        || record.jobKind() != ReconcileJobKind.PLAN_SNAPSHOT
        || record.snapshotTaskTableId == null
        || record.snapshotTaskTableId.isBlank()
        || record.snapshotTaskSnapshotId < 0L) {
      return "";
    }
    return Keys.reconcileSnapshotLeasePointer(
        record.accountId, record.snapshotTaskTableId, record.snapshotTaskSnapshotId);
  }

  private boolean hasActiveLease(
      String jobId,
      String leaseEpoch,
      StoredReconcileJob existing,
      String op,
      boolean allowQueued,
      boolean allowCancelling,
      boolean requireUnexpiredLease) {
    if (leaseEpoch == null || leaseEpoch.isBlank()) {
      logLeaseSkip(op, "Skipping %s for reconcile job %s due to missing lease epoch", op, jobId);
      return false;
    }
    boolean stateAllowed =
        (allowQueued && "JS_QUEUED".equals(existing.state))
            || "JS_RUNNING".equals(existing.state)
            || (allowCancelling && "JS_CANCELLING".equals(existing.state));
    if (!stateAllowed) {
      logLeaseSkip(
          op,
          "Skipping %s for reconcile job %s due to non-running state=%s",
          op,
          jobId,
          existing.state);
      return false;
    }
    long now = System.currentTimeMillis();
    StoredLeaseState leaseState = currentLeaseState(existing).orElse(null);
    if (leaseState == null) {
      logLeaseSkip(op, "Skipping %s for reconcile job %s due to missing lease state", op, jobId);
      return false;
    }
    if (requireUnexpiredLease && leaseState.leaseExpiresAtMs <= now) {
      logLeaseSkip(
          op,
          "Skipping %s for reconcile job %s due to expired lease expiresAtMs=%d now=%d",
          op,
          jobId,
          leaseState.leaseExpiresAtMs,
          now);
      return false;
    }
    if (!leaseEpoch.equals(leaseState.leaseEpoch)) {
      logLeaseSkip(
          op,
          "Skipping %s for reconcile job %s due to stale lease epoch=%s",
          op,
          jobId,
          leaseState.leaseEpoch);
      return false;
    }
    return true;
  }

  private StoredReconcileJob cloneStoredRecord(StoredReconcileJob source) {
    if (source == null) {
      return null;
    }
    return mapper.convertValue(source, StoredReconcileJob.class);
  }

  private static String laneLeasePointerKey(StoredReconcileJob record) {
    if (record == null
        || record.accountId == null
        || record.accountId.isBlank()
        || record.laneKey == null
        || record.laneKey.isBlank()) {
      return "";
    }
    return Keys.reconcileLaneLeasePointer(record.accountId, record.laneKey);
  }

  private static String leaseExpiryPointerKey(StoredReconcileJob record) {
    if (record == null || !hasLeaseExpiryIndex(record)) {
      return "";
    }
    return Keys.reconcileLeaseExpiryPointerByDue(
        record.leaseExpiresAtMs, record.accountId, record.jobId);
  }

  private static String leaseExpiryPointerKey(StoredLeaseState leaseState) {
    if (leaseState == null
        || blank(leaseState.accountId)
        || blank(leaseState.jobId)
        || blank(leaseState.leaseEpoch)
        || leaseState.leaseExpiresAtMs <= 0L) {
      return "";
    }
    return Keys.reconcileLeaseExpiryPointerByDue(
        leaseState.leaseExpiresAtMs, leaseState.accountId, leaseState.jobId);
  }

  private static String dedupePointerKey(StoredReconcileJob record) {
    if (record == null
        || record.accountId == null
        || record.accountId.isBlank()
        || record.dedupeKey == null
        || record.dedupeKey.isBlank()) {
      return "";
    }
    return Keys.reconcileDedupePointer(record.accountId, hashValue(record.dedupeKey));
  }

  private static boolean requiresLaneQueuePointer(StoredReconcileJob record) {
    if (record == null) {
      return false;
    }
    if ("JS_QUEUED".equals(record.state) || "JS_WAITING".equals(record.state)) {
      return true;
    }
    return "JS_CANCELLING".equals(record.state)
        && (record.leaseEpoch == null
            || record.leaseEpoch.isBlank()
            || record.leaseExpiresAtMs <= 0L);
  }

  private static boolean hasLeaseExpiryIndex(StoredReconcileJob record) {
    return record != null
        && ("JS_RUNNING".equals(record.state) || "JS_CANCELLING".equals(record.state))
        && !blank(record.accountId)
        && !blank(record.jobId)
        && !blank(record.leaseEpoch)
        && record.leaseExpiresAtMs > 0L;
  }

  private void logLeaseSkip(String op, String format, Object... args) {
    if ("markProgress".equals(op)) {
      LOG.debugf(format, args);
    } else {
      LOG.warnf(format, args);
    }
  }

  private void syncAggregateChainByJobId(String jobId, String operation) {
    long startedNs = System.nanoTime();
    var currentEnvelope = loadByAnyAccount(jobId);
    if (currentEnvelope.isEmpty()) {
      return;
    }

    StoredReconcileJob currentJob = currentEnvelope.get().record;
    StoredAggregateRollup currentRollup = aggregateRollupForDeltaWrite(currentJob);
    int ancestorsUpdated = 0;
    int repairs = 0;
    int depth = 0;

    while (currentJob != null && !currentJob.parentJobId().isBlank() && depth++ < 256) {
      var parentEnvelope = loadByAnyAccount(currentJob.parentJobId());
      if (parentEnvelope.isEmpty()) {
        break;
      }
      StoredReconcileJob parentJob = parentEnvelope.get().record;
      StoredAggregateContribution previousContribution =
          readAggregateContribution(parentJob.accountId, parentJob.jobId, currentJob.jobId)
              .orElse(null);
      StoredAggregateContribution nextContribution =
          contributionSnapshot(parentJob.accountId, parentJob.jobId, currentJob, currentRollup);
      boolean contributionChanged = !sameContribution(previousContribution, nextContribution);
      StoredAggregateRollup parentRollup = null;
      if (contributionChanged) {
        writeAggregateContribution(nextContribution);
        ancestorsUpdated++;
      }
      if (supportsChildAggregation(parentJob.jobKind())) {
        AggregateRollupUpdate parentRollupUpdate =
            contributionChanged
                ? updateAggregateRollup(parentJob, previousContribution, nextContribution)
                : aggregateRollupForNoOpWrite(parentJob);
        parentRollup = parentRollupUpdate.rollup();
        if (parentRollupUpdate.repaired()) {
          repairs++;
        }
      }
      currentJob = parentJob;
      currentRollup = parentRollup;
    }

    long elapsedMs = (System.nanoTime() - startedNs) / 1_000_000L;
    if (ancestorsUpdated > 0 || elapsedMs >= 100L || repairs > 0) {
      LOG.infof(
          "Reconcile aggregate sync op=%s jobId=%s ancestorsUpdated=%d repairs=%d elapsedMs=%d",
          operation, jobId, ancestorsUpdated, repairs, elapsedMs);
    } else {
      LOG.debugf(
          "Reconcile aggregate sync op=%s jobId=%s ancestorsUpdated=%d repairs=%d elapsedMs=%d",
          operation, jobId, ancestorsUpdated, repairs, elapsedMs);
    }
  }

  private static boolean supportsChildAggregation(ReconcileJobKind jobKind) {
    return jobKind == ReconcileJobKind.PLAN_CONNECTOR
        || jobKind == ReconcileJobKind.PLAN_TABLE
        || jobKind == ReconcileJobKind.PLAN_SNAPSHOT;
  }

  private List<StoredReconcileJob> childJobRecords(String accountId, String parentJobId) {
    if (accountId == null || accountId.isBlank() || parentJobId == null || parentJobId.isBlank()) {
      return List.of();
    }
    List<StoredReconcileJob> out = new ArrayList<>();
    String token = "";
    String prefix = Keys.reconcileJobByParentPointerPrefix(accountId, parentJobId);
    do {
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers = pointerStore.listPointersByPrefix(prefix, 200, token, next);
      for (Pointer ptr : pointers) {
        readCurrentRecordFromIndexPointer(ptr, ptr.getKey()).ifPresent(out::add);
      }
      token = next.toString();
    } while (token != null && !token.isBlank());
    return List.copyOf(out);
  }

  private void refreshStoredCountersWithBlob(StoredReconcileJob job) {
    refreshStoredCounters(job);
    if (job == null || job.jobKind() != ReconcileJobKind.PLAN_SNAPSHOT) {
      return;
    }
    if (job.plannedFiles > 0L || blank(job.snapshotTaskFileGroupPlanBlobUri)) {
      return;
    }
    try {
      long plannedFiles =
          mapper
              .readValue(
                  blobStore.get(job.snapshotTaskFileGroupPlanBlobUri),
                  ai.floedb.floecat.reconciler.impl.SnapshotPlanBlobStore.SnapshotPlanBlob.class)
              .fileGroupJobs()
              .stream()
              .filter(java.util.Objects::nonNull)
              .map(ai.floedb.floecat.reconciler.impl.PlannedFileGroupJob::fileGroupTask)
              .filter(java.util.Objects::nonNull)
              .mapToLong(
                  group -> group.fileCount() > 0 ? group.fileCount() : group.filePaths().size())
              .sum();
      job.plannedFiles = plannedFiles;
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to load snapshot plan blob " + job.snapshotTaskFileGroupPlanBlobUri, e);
    }
  }

  private static void refreshStoredCounters(StoredReconcileJob job) {
    if (job == null) {
      return;
    }
    if (job.jobKind() == ReconcileJobKind.EXEC_FILE_GROUP) {
      long plannedFiles =
          job.fileGroupFileCount > 0
              ? job.fileGroupFileCount
              : (job.fileGroupPaths == null ? 0L : job.fileGroupPaths.size());
      long completedFiles = 0L;
      long failedFiles = 0L;
      if (job.fileGroupResults != null) {
        for (ReconcileFileResult result : job.fileGroupResults) {
          if (result == null || result.isEmpty()) {
            continue;
          }
          if (result.state() == ReconcileFileResult.State.SUCCEEDED
              || result.state() == ReconcileFileResult.State.SKIPPED) {
            completedFiles++;
          } else if (result.state() == ReconcileFileResult.State.FAILED) {
            failedFiles++;
          }
        }
      }
      if (completedFiles == 0L && failedFiles == 0L) {
        if ("JS_SUCCEEDED".equals(job.state)) {
          completedFiles = plannedFiles;
        } else if ("JS_FAILED".equals(job.state) || "JS_CANCELLED".equals(job.state)) {
          failedFiles = plannedFiles;
        }
      }
      job.plannedFileGroups = job.fileGroupTask().isEmpty() ? 0L : 1L;
      job.plannedFiles = plannedFiles;
      job.completedFileGroups = "JS_SUCCEEDED".equals(job.state) ? job.plannedFileGroups : 0L;
      job.failedFileGroups =
          ("JS_FAILED".equals(job.state) || "JS_CANCELLED".equals(job.state))
              ? job.plannedFileGroups
              : 0L;
      job.completedFiles = completedFiles;
      job.failedFiles = failedFiles;
      return;
    }
    if (job.jobKind() == ReconcileJobKind.PLAN_SNAPSHOT) {
      job.plannedFileGroups =
          Math.max(job.plannedFileGroups, Math.max(0L, job.snapshotTaskFileGroupCount));
      if (job.snapshotTaskFileGroups != null && !job.snapshotTaskFileGroups.isEmpty()) {
        job.plannedFiles =
            job.snapshotTaskFileGroups.stream()
                .filter(java.util.Objects::nonNull)
                .mapToLong(
                    group -> group.fileCount() > 0 ? group.fileCount() : group.filePaths().size())
                .sum();
      }
      return;
    }
    job.plannedFileGroups = 0L;
    job.plannedFiles = 0L;
    job.completedFileGroups = 0L;
    job.failedFileGroups = 0L;
    job.completedFiles = 0L;
    job.failedFiles = 0L;
  }

  private StoredAggregateRollup ensureAggregateRollupExists(StoredReconcileJob job) {
    if (job == null || !supportsChildAggregation(job.jobKind())) {
      return null;
    }
    return readAggregateRollup(job.accountId, job.jobId)
        .orElseGet(() -> rebuildAggregateRollup(job));
  }

  private StoredAggregateRollup aggregateRollupForDeltaWrite(StoredReconcileJob job) {
    if (job == null || !supportsChildAggregation(job.jobKind())) {
      return null;
    }
    return readAggregateRollup(job.accountId, job.jobId).orElse(null);
  }

  private AggregateRollupUpdate aggregateRollupForNoOpWrite(StoredReconcileJob parentJob) {
    StoredAggregateRollup rollup = aggregateRollupForDeltaWrite(parentJob);
    if (rollup != null) {
      return new AggregateRollupUpdate(rollup, false);
    }
    return new AggregateRollupUpdate(rebuildAggregateRollup(parentJob), true);
  }

  private AggregateRollupUpdate ensureCurrentAggregateRollup(StoredReconcileJob parentJob) {
    StoredAggregateRollup rollup =
        readAggregateRollup(parentJob.accountId, parentJob.jobId).orElse(null);
    if (rollup != null && !aggregateRollupNeedsRepair(parentJob, rollup)) {
      return new AggregateRollupUpdate(rollup, false);
    }
    return new AggregateRollupUpdate(rebuildAggregateRollup(parentJob), true);
  }

  private boolean aggregateRollupNeedsRepair(
      StoredReconcileJob parentJob, StoredAggregateRollup existingRollup) {
    if (parentJob == null || existingRollup == null) {
      return true;
    }
    StoredAggregateRollup recomputed =
        StoredAggregateRollup.empty(parentJob.accountId, parentJob.jobId);
    for (StoredReconcileJob child : childJobRecords(parentJob.accountId, parentJob.jobId)) {
      StoredAggregateRollup childRollup =
          supportsChildAggregation(child.jobKind())
              ? ensureCurrentAggregateRollup(child).rollup()
              : null;
      StoredAggregateContribution contribution =
          contributionSnapshot(parentJob.accountId, parentJob.jobId, child, childRollup);
      recomputed = applyContributionDelta(parentJob, recomputed, null, contribution);
    }
    return !sameAggregateRollup(existingRollup, recomputed);
  }

  private AggregateRollupUpdate updateAggregateRollup(
      StoredReconcileJob parentJob,
      StoredAggregateContribution previousContribution,
      StoredAggregateContribution nextContribution) {
    String pointerKey = Keys.reconcileAggregateRollupPointer(parentJob.accountId, parentJob.jobId);
    for (int i = 0; i < CAS_MAX; i++) {
      Pointer currentPointer = pointerStore.get(pointerKey).orElse(null);
      if (currentPointer == null) {
        return new AggregateRollupUpdate(rebuildAggregateRollup(parentJob), true);
      }
      StoredAggregateRollup currentRollup = readAggregateRollup(currentPointer).orElse(null);
      if (currentRollup == null) {
        return new AggregateRollupUpdate(rebuildAggregateRollup(parentJob), true);
      }
      StoredAggregateRollup nextRollup =
          applyContributionDelta(parentJob, currentRollup, previousContribution, nextContribution);
      if (rollupHasContradictoryActiveCounts(parentJob, nextRollup)) {
        return new AggregateRollupUpdate(rebuildAggregateRollup(parentJob), true);
      }
      if (writeAggregateRollupCompareAndSet(pointerKey, currentPointer, nextRollup)) {
        return new AggregateRollupUpdate(nextRollup, false);
      }
    }
    return new AggregateRollupUpdate(rebuildAggregateRollup(parentJob), true);
  }

  private boolean rollupHasContradictoryActiveCounts(
      StoredReconcileJob parentJob, StoredAggregateRollup rollup) {
    if (parentJob == null || rollup == null) {
      return false;
    }
    boolean rollupClaimsActive =
        rollup.queuedChildren > 0L
            || rollup.waitingChildren > 0L
            || rollup.runningChildren > 0L
            || rollup.cancellingChildren > 0L;
    if (!rollupClaimsActive) {
      return false;
    }

    long queuedChildren = 0L;
    long waitingChildren = 0L;
    long runningChildren = 0L;
    long cancellingChildren = 0L;
    long totalChildren = 0L;
    long terminalChildren = 0L;
    for (StoredAggregateContribution contribution :
        listAggregateContributions(parentJob.accountId, parentJob.jobId)) {
      totalChildren++;
      switch (blankToEmpty(contribution.state)) {
        case "JS_QUEUED" -> queuedChildren++;
        case "JS_WAITING" -> waitingChildren++;
        case "JS_RUNNING" -> runningChildren++;
        case "JS_CANCELLING" -> cancellingChildren++;
        case "JS_SUCCEEDED", "JS_FAILED", "JS_CANCELLED" -> terminalChildren++;
        default -> {}
      }
    }

    if (queuedChildren != rollup.queuedChildren
        || waitingChildren != rollup.waitingChildren
        || runningChildren != rollup.runningChildren
        || cancellingChildren != rollup.cancellingChildren) {
      return true;
    }
    return totalChildren > 0L && terminalChildren == totalChildren;
  }

  private StoredAggregateRollup rebuildAggregateRollup(StoredReconcileJob parentJob) {
    StoredAggregateRollup rollup =
        StoredAggregateRollup.empty(parentJob.accountId, parentJob.jobId);
    for (StoredReconcileJob child : childJobRecords(parentJob.accountId, parentJob.jobId)) {
      StoredAggregateRollup childRollup =
          supportsChildAggregation(child.jobKind())
              ? ensureCurrentAggregateRollup(child).rollup()
              : null;
      StoredAggregateContribution contribution =
          contributionSnapshot(parentJob.accountId, parentJob.jobId, child, childRollup);
      writeAggregateContribution(contribution);
      rollup = applyContributionDelta(parentJob, rollup, null, contribution);
    }
    writeAggregateRollup(rollup);
    return rollup;
  }

  private StoredAggregateContribution contributionSnapshot(
      String accountId, String parentJobId, StoredReconcileJob job, StoredAggregateRollup rollup) {
    StoredAggregateContribution contribution = new StoredAggregateContribution();
    contribution.accountId = accountId;
    contribution.parentJobId = parentJobId;
    contribution.childJobId = job.jobId;
    contribution.state = aggregateState(job, rollup);
    contribution.message = aggregateMessage(job, rollup);
    contribution.startedAtMs = aggregateStartedAtMs(job, rollup);
    contribution.finishedAtMs = aggregateFinishedAtMs(job, rollup);
    contribution.tablesScanned = aggregateTablesScanned(job, rollup);
    contribution.tablesChanged = aggregateTablesChanged(job, rollup);
    contribution.viewsScanned = aggregateViewsScanned(job, rollup);
    contribution.viewsChanged = aggregateViewsChanged(job, rollup);
    contribution.errors = aggregateErrors(job, rollup);
    contribution.snapshotsProcessed = aggregateSnapshotsProcessed(job, rollup);
    contribution.statsProcessed = aggregateStatsProcessed(job, rollup);
    contribution.indexesProcessed = aggregateIndexesProcessed(job, rollup);
    contribution.plannedFileGroups = aggregatePlannedFileGroups(job, rollup);
    contribution.plannedFiles = aggregatePlannedFiles(job, rollup);
    contribution.completedFileGroups = aggregateCompletedFileGroups(job, rollup);
    contribution.failedFileGroups = aggregateFailedFileGroups(job, rollup);
    contribution.completedFiles = aggregateCompletedFiles(job, rollup);
    contribution.failedFiles = aggregateFailedFiles(job, rollup);
    contribution.executorId = aggregateExecutorId(job, rollup);
    contribution.updatedAtMs = System.currentTimeMillis();
    return contribution;
  }

  private StoredAggregateRollup applyContributionDelta(
      StoredReconcileJob parentJob,
      StoredAggregateRollup rollup,
      StoredAggregateContribution previous,
      StoredAggregateContribution next) {
    StoredAggregateRollup updated =
        rollup == null
            ? StoredAggregateRollup.empty(parentJob.accountId, parentJob.jobId)
            : cloneRollup(rollup);
    adjustStateCounts(updated, previous, -1L);
    adjustStateCounts(updated, next, 1L);
    updated.tablesScanned +=
        delta(previous == null ? 0L : previous.tablesScanned, next.tablesScanned);
    updated.tablesChanged +=
        delta(previous == null ? 0L : previous.tablesChanged, next.tablesChanged);
    updated.viewsScanned += delta(previous == null ? 0L : previous.viewsScanned, next.viewsScanned);
    updated.viewsChanged += delta(previous == null ? 0L : previous.viewsChanged, next.viewsChanged);
    updated.errors += delta(previous == null ? 0L : previous.errors, next.errors);
    updated.snapshotsProcessed +=
        delta(previous == null ? 0L : previous.snapshotsProcessed, next.snapshotsProcessed);
    updated.statsProcessed +=
        delta(previous == null ? 0L : previous.statsProcessed, next.statsProcessed);
    updated.indexesProcessed +=
        delta(previous == null ? 0L : previous.indexesProcessed, next.indexesProcessed);
    updated.plannedFileGroups +=
        delta(previous == null ? 0L : previous.plannedFileGroups, next.plannedFileGroups);
    updated.plannedFiles += delta(previous == null ? 0L : previous.plannedFiles, next.plannedFiles);
    updated.completedFileGroups +=
        delta(previous == null ? 0L : previous.completedFileGroups, next.completedFileGroups);
    updated.failedFileGroups +=
        delta(previous == null ? 0L : previous.failedFileGroups, next.failedFileGroups);
    updated.completedFiles +=
        delta(previous == null ? 0L : previous.completedFiles, next.completedFiles);
    updated.failedFiles += delta(previous == null ? 0L : previous.failedFiles, next.failedFiles);
    refreshRollupStartedAt(updated, previous, next);
    refreshRollupFinishedAt(updated, previous, next);
    refreshRollupMessage(parentJob, updated, previous, next);
    refreshRollupExecutor(updated, previous, next);
    updated.updatedAtMs = System.currentTimeMillis();
    return updated;
  }

  private void refreshRollupStartedAt(
      StoredAggregateRollup rollup,
      StoredAggregateContribution previous,
      StoredAggregateContribution next) {
    if (next.startedAtMs > 0L
        && (rollup.startedAtMs <= 0L || next.startedAtMs < rollup.startedAtMs)) {
      rollup.startedAtMs = next.startedAtMs;
      rollup.startedAtChildJobId = next.childJobId;
      return;
    }
    if (previous != null
        && previous.childJobId.equals(rollup.startedAtChildJobId)
        && previous.startedAtMs != next.startedAtMs) {
      recomputeStartedAt(rollup);
    }
  }

  private void refreshRollupFinishedAt(
      StoredAggregateRollup rollup,
      StoredAggregateContribution previous,
      StoredAggregateContribution next) {
    if (next.finishedAtMs > 0L && next.finishedAtMs >= rollup.finishedAtMs) {
      rollup.finishedAtMs = next.finishedAtMs;
      rollup.finishedAtChildJobId = next.childJobId;
      return;
    }
    if (previous != null
        && previous.childJobId.equals(rollup.finishedAtChildJobId)
        && previous.finishedAtMs != next.finishedAtMs) {
      recomputeFinishedAt(rollup);
    }
  }

  private void refreshRollupMessage(
      StoredReconcileJob parentJob,
      StoredAggregateRollup rollup,
      StoredAggregateContribution previous,
      StoredAggregateContribution next) {
    if (qualifiesSurfaceMessage(next)
        && (rollup.surfacedMessageChildJobId.isBlank()
            || messagePriority(next.state) > messagePriority(rollup.surfacedMessageChildState)
            || (messagePriority(next.state) == messagePriority(rollup.surfacedMessageChildState)
                && next.updatedAtMs >= rollup.surfacedMessageUpdatedAtMs))) {
      rollup.surfacedMessageChildJobId = next.childJobId;
      rollup.surfacedMessageChildState = next.state;
      rollup.surfacedMessage = surfacedContributionMessage(next);
      rollup.surfacedMessageUpdatedAtMs = next.updatedAtMs;
      return;
    }
    if (previous != null
        && previous.childJobId.equals(rollup.surfacedMessageChildJobId)
        && !sameSurface(previous, next)) {
      recomputeSurfaceMessage(parentJob, rollup);
    }
  }

  private void refreshRollupExecutor(
      StoredAggregateRollup rollup,
      StoredAggregateContribution previous,
      StoredAggregateContribution next) {
    if (qualifiesExecutor(next)
        && (rollup.executorChildJobId.isBlank()
            || next.childJobId.equals(rollup.executorChildJobId))) {
      rollup.executorChildJobId = next.childJobId;
      rollup.executorId = next.executorId;
      return;
    }
    if (previous != null
        && previous.childJobId.equals(rollup.executorChildJobId)
        && !sameExecutor(previous, next)) {
      recomputeExecutor(rollup);
    }
  }

  private void recomputeStartedAt(StoredAggregateRollup rollup) {
    rollup.startedAtMs = 0L;
    rollup.startedAtChildJobId = "";
    for (StoredAggregateContribution contribution :
        listAggregateContributions(rollup.accountId, rollup.jobId)) {
      if (contribution.startedAtMs > 0L
          && (rollup.startedAtMs <= 0L || contribution.startedAtMs < rollup.startedAtMs)) {
        rollup.startedAtMs = contribution.startedAtMs;
        rollup.startedAtChildJobId = contribution.childJobId;
      }
    }
  }

  private void recomputeFinishedAt(StoredAggregateRollup rollup) {
    rollup.finishedAtMs = 0L;
    rollup.finishedAtChildJobId = "";
    for (StoredAggregateContribution contribution :
        listAggregateContributions(rollup.accountId, rollup.jobId)) {
      if (contribution.finishedAtMs > 0L && contribution.finishedAtMs >= rollup.finishedAtMs) {
        rollup.finishedAtMs = contribution.finishedAtMs;
        rollup.finishedAtChildJobId = contribution.childJobId;
      }
    }
  }

  private void recomputeSurfaceMessage(StoredReconcileJob parentJob, StoredAggregateRollup rollup) {
    rollup.surfacedMessage = "";
    rollup.surfacedMessageChildJobId = "";
    rollup.surfacedMessageChildState = "";
    rollup.surfacedMessageUpdatedAtMs = 0L;
    if ("JS_FAILED".equals(parentJob.state) || "JS_CANCELLED".equals(parentJob.state)) {
      return;
    }
    for (StoredAggregateContribution contribution :
        listAggregateContributions(rollup.accountId, rollup.jobId)) {
      if (!qualifiesSurfaceMessage(contribution)) {
        continue;
      }
      if (rollup.surfacedMessageChildJobId.isBlank()
          || messagePriority(contribution.state) > messagePriority(rollup.surfacedMessageChildState)
          || (messagePriority(contribution.state)
                  == messagePriority(rollup.surfacedMessageChildState)
              && contribution.updatedAtMs >= rollup.surfacedMessageUpdatedAtMs)) {
        rollup.surfacedMessageChildJobId = contribution.childJobId;
        rollup.surfacedMessageChildState = contribution.state;
        rollup.surfacedMessage = surfacedContributionMessage(contribution);
        rollup.surfacedMessageUpdatedAtMs = contribution.updatedAtMs;
      }
    }
  }

  private void recomputeExecutor(StoredAggregateRollup rollup) {
    rollup.executorId = "";
    rollup.executorChildJobId = "";
    for (StoredAggregateContribution contribution :
        listAggregateContributions(rollup.accountId, rollup.jobId)) {
      if (qualifiesExecutor(contribution)) {
        rollup.executorId = contribution.executorId;
        rollup.executorChildJobId = contribution.childJobId;
        return;
      }
    }
  }

  private List<StoredAggregateContribution> listAggregateContributions(
      String accountId, String parentJobId) {
    List<StoredAggregateContribution> out = new ArrayList<>();
    String token = "";
    String prefix = Keys.reconcileAggregateContributionPointerPrefix(accountId, parentJobId);
    do {
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers = pointerStore.listPointersByPrefix(prefix, 200, token, next);
      for (Pointer pointer : pointers) {
        readAggregateContribution(pointer).ifPresent(out::add);
      }
      token = next.toString();
    } while (token != null && !token.isBlank());
    return List.copyOf(out);
  }

  private Optional<StoredAggregateRollup> readAggregateRollup(String accountId, String jobId) {
    Pointer pointer =
        pointerStore.get(Keys.reconcileAggregateRollupPointer(accountId, jobId)).orElse(null);
    return readAggregateRollup(pointer);
  }

  private Optional<StoredAggregateRollup> readAggregateRollup(Pointer pointer) {
    if (pointer == null) {
      return Optional.empty();
    }
    return readAuxiliaryRecordByBlobUri(pointer.getBlobUri(), StoredAggregateRollup.class);
  }

  private Optional<StoredLeaseState> readLeaseState(String accountId, String jobId) {
    return readLeaseStateEnvelope(accountId, jobId).map(StoredLeaseStateEnvelope::leaseState);
  }

  private Optional<StoredLeaseStateEnvelope> readLeaseStateEnvelope(
      String accountId, String jobId) {
    Pointer pointer =
        pointerStore.get(Keys.reconcileLeaseStatePointer(accountId, jobId)).orElse(null);
    if (pointer == null) {
      return Optional.empty();
    }
    return readAuxiliaryRecordByBlobUri(pointer.getBlobUri(), StoredLeaseState.class)
        .map(leaseState -> new StoredLeaseStateEnvelope(pointer, leaseState));
  }

  private Optional<StoredAggregateContribution> readAggregateContribution(
      String accountId, String parentJobId, String childJobId) {
    Pointer pointer =
        pointerStore
            .get(Keys.reconcileAggregateContributionPointer(accountId, parentJobId, childJobId))
            .orElse(null);
    return readAggregateContribution(pointer);
  }

  private Optional<StoredAggregateContribution> readAggregateContribution(Pointer pointer) {
    if (pointer == null) {
      return Optional.empty();
    }
    return readAuxiliaryRecordByBlobUri(pointer.getBlobUri(), StoredAggregateContribution.class);
  }

  private <T> Optional<T> readAuxiliaryRecordByBlobUri(String blobUri, Class<T> type) {
    byte[] payload;
    try {
      payload = blobStore.get(blobUri);
    } catch (StorageNotFoundException e) {
      LOG.warnf(e, "Aggregate blob missing during read blob=%s", blobUri);
      return Optional.empty();
    }
    if (payload == null || payload.length == 0) {
      return Optional.empty();
    }
    try {
      return Optional.ofNullable(mapper.readValue(payload, type));
    } catch (Exception e) {
      LOG.warnf(e, "Failed to decode aggregate payload blob=%s", blobUri);
      return Optional.empty();
    }
  }

  private void writeAggregateRollup(StoredAggregateRollup rollup) {
    writeAuxiliaryRecord(
        Keys.reconcileAggregateRollupPointer(rollup.accountId, rollup.jobId),
        () ->
            Keys.reconcileAggregateRollupBlobUri(
                rollup.accountId,
                rollup.jobId,
                "v" + System.currentTimeMillis() + "-" + UUID.randomUUID()),
        rollup);
  }

  private boolean writeAggregateRollupCompareAndSet(
      String pointerKey, Pointer currentPointer, StoredAggregateRollup rollup) {
    if (currentPointer == null) {
      return false;
    }
    String nextBlobUri =
        Keys.reconcileAggregateRollupBlobUri(
            rollup.accountId,
            rollup.jobId,
            "v" + System.currentTimeMillis() + "-" + UUID.randomUUID());
    try {
      blobStore.put(
          nextBlobUri,
          mapper.writeValueAsBytes(rollup),
          "application/json; charset=" + StandardCharsets.UTF_8.name());
    } catch (Exception e) {
      throw new IllegalStateException("Failed to persist aggregate payload", e);
    }
    Pointer next =
        Pointer.newBuilder()
            .setKey(pointerKey)
            .setBlobUri(nextBlobUri)
            .setVersion(currentPointer.getVersion() + 1L)
            .build();
    if (pointerStore.compareAndSet(pointerKey, currentPointer.getVersion(), next)) {
      return true;
    }
    blobStore.delete(nextBlobUri);
    return false;
  }

  private void writeAggregateContribution(StoredAggregateContribution contribution) {
    writeAuxiliaryRecord(
        Keys.reconcileAggregateContributionPointer(
            contribution.accountId, contribution.parentJobId, contribution.childJobId),
        () ->
            Keys.reconcileAggregateContributionBlobUri(
                contribution.accountId,
                contribution.parentJobId,
                contribution.childJobId,
                "v" + System.currentTimeMillis() + "-" + UUID.randomUUID()),
        contribution);
  }

  private StoredLeaseStateEnvelope createLeaseStateIfAbsentOrExpired(
      StoredLeaseState leaseState, long nowMs, boolean allowReplaceUnexpired) {
    if (leaseState == null || blank(leaseState.accountId) || blank(leaseState.jobId)) {
      return null;
    }
    String pointerKey = Keys.reconcileLeaseStatePointer(leaseState.accountId, leaseState.jobId);
    for (int i = 0; i < CAS_MAX; i++) {
      Pointer currentPointer = pointerStore.get(pointerKey).orElse(null);
      long expectedVersion = currentPointer == null ? 0L : currentPointer.getVersion();
      String priorExpiryPointerKey = "";
      String referenceBlobUri = stableReferenceBlobUri(leaseState.accountId, leaseState.jobId);
      if (currentPointer != null) {
        StoredLeaseState currentState =
            readAuxiliaryRecordByBlobUri(currentPointer.getBlobUri(), StoredLeaseState.class)
                .orElse(null);
        if (currentState == null) {
          if (pointerStore.compareAndDelete(pointerKey, currentPointer.getVersion())) {
            continue;
          }
          continue;
        }
        if (!allowReplaceUnexpired
            && currentState.leaseExpiresAtMs > nowMs
            && !blank(currentState.leaseEpoch)) {
          return null;
        }
        priorExpiryPointerKey = leaseExpiryPointerKey(currentState);
      }
      String nextBlobUri =
          Keys.reconcileLeaseStateBlobUri(
              leaseState.accountId,
              leaseState.jobId,
              "v" + System.currentTimeMillis() + "-" + UUID.randomUUID());
      try {
        blobStore.put(
            nextBlobUri,
            mapper.writeValueAsBytes(leaseState),
            "application/json; charset=" + StandardCharsets.UTF_8.name());
      } catch (Exception e) {
        throw new IllegalStateException("Failed to persist aggregate payload", e);
      }
      Pointer next =
          Pointer.newBuilder()
              .setKey(pointerKey)
              .setBlobUri(nextBlobUri)
              .setVersion(expectedVersion + 1L)
              .build();
      if (pointerStore.compareAndSet(pointerKey, expectedVersion, next)) {
        String nextExpiryPointerKey = leaseExpiryPointerKey(leaseState);
        if (!blank(priorExpiryPointerKey) && !priorExpiryPointerKey.equals(nextExpiryPointerKey)) {
          clearPointerIfMatches(priorExpiryPointerKey, referenceBlobUri);
        }
        return new StoredLeaseStateEnvelope(
            next, mapper.convertValue(leaseState, StoredLeaseState.class));
      }
      blobStore.delete(nextBlobUri);
    }
    throw new IllegalStateException("Failed to update aggregate pointer " + pointerKey);
  }

  private void deleteLeaseStateIfOwned(StoredLeaseStateEnvelope leaseStateEnvelope) {
    if (leaseStateEnvelope == null) {
      return;
    }
    deleteLeaseStateIfOwned(
        leaseStateEnvelope.leaseState().accountId,
        leaseStateEnvelope.leaseState().jobId,
        leaseStateEnvelope.leaseState());
  }

  private void deleteLeaseStateIfOwned(
      String accountId, String jobId, StoredLeaseState expectedLeaseState) {
    if (blank(accountId) || blank(jobId) || expectedLeaseState == null) {
      return;
    }
    StoredLeaseStateEnvelope currentEnvelope =
        readLeaseStateEnvelope(accountId, jobId).orElse(null);
    if (currentEnvelope == null) {
      return;
    }
    StoredLeaseState currentState = currentEnvelope.leaseState();
    if (currentState == null
        || !accountId.equals(currentState.accountId)
        || !jobId.equals(currentState.jobId)
        || !blankToEmpty(expectedLeaseState.leaseEpoch)
            .equals(blankToEmpty(currentState.leaseEpoch))) {
      return;
    }
    pointerStore.compareAndDelete(
        Keys.reconcileLeaseStatePointer(accountId, jobId), currentEnvelope.pointer().getVersion());
  }

  private boolean updateLeaseStateExpiry(
      StoredLeaseStateEnvelope leaseStateEnvelope, long nextExpiryMs) {
    if (leaseStateEnvelope == null || leaseStateEnvelope.pointer() == null) {
      return false;
    }
    StoredLeaseState leaseState = leaseStateEnvelope.leaseState();
    if (leaseState == null || blank(leaseState.accountId) || blank(leaseState.jobId)) {
      return false;
    }
    String pointerKey = Keys.reconcileLeaseStatePointer(leaseState.accountId, leaseState.jobId);
    Pointer currentPointer = pointerStore.get(pointerKey).orElse(null);
    if (currentPointer == null
        || currentPointer.getVersion() != leaseStateEnvelope.pointer().getVersion()
        || !currentPointer.getBlobUri().equals(leaseStateEnvelope.pointer().getBlobUri())) {
      return false;
    }
    StoredLeaseState currentState =
        readAuxiliaryRecordByBlobUri(currentPointer.getBlobUri(), StoredLeaseState.class)
            .orElse(null);
    if (currentState == null
        || !leaseState.accountId.equals(currentState.accountId)
        || !leaseState.jobId.equals(currentState.jobId)
        || !blankToEmpty(leaseState.leaseEpoch).equals(blankToEmpty(currentState.leaseEpoch))) {
      return false;
    }
    String previousExpiryPointerKey = leaseExpiryPointerKey(leaseState);
    StoredLeaseState nextLeaseState = mapper.convertValue(leaseState, StoredLeaseState.class);
    nextLeaseState.leaseExpiresAtMs = nextExpiryMs;
    nextLeaseState.updatedAtMs = System.currentTimeMillis();
    String nextBlobUri =
        Keys.reconcileLeaseStateBlobUri(
            nextLeaseState.accountId,
            nextLeaseState.jobId,
            "v" + System.currentTimeMillis() + "-" + UUID.randomUUID());
    try {
      blobStore.put(
          nextBlobUri,
          mapper.writeValueAsBytes(nextLeaseState),
          "application/json; charset=" + StandardCharsets.UTF_8.name());
    } catch (Exception e) {
      throw new IllegalStateException("Failed to persist aggregate payload", e);
    }
    Pointer nextPointer =
        Pointer.newBuilder()
            .setKey(pointerKey)
            .setBlobUri(nextBlobUri)
            .setVersion(currentPointer.getVersion() + 1L)
            .build();
    if (!pointerStore.compareAndSet(pointerKey, currentPointer.getVersion(), nextPointer)) {
      blobStore.delete(nextBlobUri);
      return false;
    }
    String currentExpiryPointerKey = leaseExpiryPointerKey(nextLeaseState);
    String referenceBlobUri = stableReferenceBlobUri(leaseState.accountId, leaseState.jobId);
    if (!blank(currentExpiryPointerKey)) {
      upsertBlobPointer(currentExpiryPointerKey, referenceBlobUri);
    }
    if (!blank(previousExpiryPointerKey)
        && !previousExpiryPointerKey.equals(currentExpiryPointerKey)) {
      clearPointerIfMatches(previousExpiryPointerKey, referenceBlobUri);
    }
    leaseState.leaseExpiresAtMs = nextExpiryMs;
    leaseState.updatedAtMs = nextLeaseState.updatedAtMs;
    return true;
  }

  private void writeAuxiliaryRecord(
      String pointerKey, java.util.function.Supplier<String> blobUriSupplier, Object record) {
    for (int i = 0; i < CAS_MAX; i++) {
      Pointer current = pointerStore.get(pointerKey).orElse(null);
      long expectedVersion = current == null ? 0L : current.getVersion();
      String nextBlobUri = blobUriSupplier.get();
      try {
        blobStore.put(
            nextBlobUri,
            mapper.writeValueAsBytes(record),
            "application/json; charset=" + StandardCharsets.UTF_8.name());
      } catch (Exception e) {
        throw new IllegalStateException("Failed to persist aggregate payload", e);
      }
      Pointer next =
          Pointer.newBuilder()
              .setKey(pointerKey)
              .setBlobUri(nextBlobUri)
              .setVersion(expectedVersion + 1L)
              .build();
      if (pointerStore.compareAndSet(pointerKey, expectedVersion, next)) {
        return;
      }
      blobStore.delete(nextBlobUri);
    }
    throw new IllegalStateException("Failed to update aggregate pointer " + pointerKey);
  }

  private static long delta(long previous, long next) {
    return next - previous;
  }

  private static void adjustStateCounts(
      StoredAggregateRollup rollup, StoredAggregateContribution contribution, long direction) {
    if (contribution == null || contribution.state == null || contribution.state.isBlank()) {
      return;
    }
    switch (contribution.state) {
      case "JS_QUEUED" -> rollup.queuedChildren += direction;
      case "JS_WAITING" -> rollup.waitingChildren += direction;
      case "JS_RUNNING" -> rollup.runningChildren += direction;
      case "JS_CANCELLING" -> rollup.cancellingChildren += direction;
      case "JS_FAILED" -> rollup.failedChildren += direction;
      case "JS_CANCELLED" -> rollup.cancelledChildren += direction;
      case "JS_SUCCEEDED" -> rollup.succeededChildren += direction;
      default -> {}
    }
  }

  private static boolean sameContribution(
      StoredAggregateContribution previous, StoredAggregateContribution next) {
    if (previous == null) {
      return false;
    }
    return blankToEmpty(previous.state).equals(blankToEmpty(next.state))
        && blankToEmpty(previous.message).equals(blankToEmpty(next.message))
        && previous.startedAtMs == next.startedAtMs
        && previous.finishedAtMs == next.finishedAtMs
        && previous.tablesScanned == next.tablesScanned
        && previous.tablesChanged == next.tablesChanged
        && previous.viewsScanned == next.viewsScanned
        && previous.viewsChanged == next.viewsChanged
        && previous.errors == next.errors
        && previous.snapshotsProcessed == next.snapshotsProcessed
        && previous.statsProcessed == next.statsProcessed
        && previous.indexesProcessed == next.indexesProcessed
        && previous.plannedFileGroups == next.plannedFileGroups
        && previous.plannedFiles == next.plannedFiles
        && previous.completedFileGroups == next.completedFileGroups
        && previous.failedFileGroups == next.failedFileGroups
        && previous.completedFiles == next.completedFiles
        && previous.failedFiles == next.failedFiles
        && blankToEmpty(previous.executorId).equals(blankToEmpty(next.executorId));
  }

  private static boolean qualifiesSurfaceMessage(StoredAggregateContribution contribution) {
    return contribution != null
        && !blank(contribution.message)
        && !"JS_SUCCEEDED".equals(contribution.state);
  }

  private static int messagePriority(String state) {
    return switch (blankToEmpty(state)) {
      case "JS_FAILED" -> 50;
      case "JS_WAITING" -> 45;
      case "JS_CANCELLING" -> 40;
      case "JS_CANCELLED" -> 30;
      case "JS_RUNNING" -> 20;
      case "JS_QUEUED" -> 10;
      default -> 0;
    };
  }

  private static boolean sameSurface(
      StoredAggregateContribution previous, StoredAggregateContribution next) {
    return qualifiesSurfaceMessage(previous) == qualifiesSurfaceMessage(next)
        && blankToEmpty(previous.message).equals(blankToEmpty(next.message))
        && blankToEmpty(previous.state).equals(blankToEmpty(next.state));
  }

  private static boolean qualifiesExecutor(StoredAggregateContribution contribution) {
    return contribution != null
        && !blank(contribution.executorId)
        && ("JS_RUNNING".equals(contribution.state) || "JS_CANCELLING".equals(contribution.state));
  }

  private static boolean sameExecutor(
      StoredAggregateContribution previous, StoredAggregateContribution next) {
    return blankToEmpty(previous.executorId).equals(blankToEmpty(next.executorId))
        && blankToEmpty(previous.state).equals(blankToEmpty(next.state));
  }

  private static boolean sameAggregateRollup(
      StoredAggregateRollup previous, StoredAggregateRollup next) {
    return previous.queuedChildren == next.queuedChildren
        && previous.waitingChildren == next.waitingChildren
        && previous.runningChildren == next.runningChildren
        && previous.cancellingChildren == next.cancellingChildren
        && previous.failedChildren == next.failedChildren
        && previous.cancelledChildren == next.cancelledChildren
        && previous.succeededChildren == next.succeededChildren
        && previous.tablesScanned == next.tablesScanned
        && previous.tablesChanged == next.tablesChanged
        && previous.viewsScanned == next.viewsScanned
        && previous.viewsChanged == next.viewsChanged
        && previous.errors == next.errors
        && previous.snapshotsProcessed == next.snapshotsProcessed
        && previous.statsProcessed == next.statsProcessed
        && previous.indexesProcessed == next.indexesProcessed
        && previous.plannedFileGroups == next.plannedFileGroups
        && previous.plannedFiles == next.plannedFiles
        && previous.completedFileGroups == next.completedFileGroups
        && previous.failedFileGroups == next.failedFileGroups
        && previous.completedFiles == next.completedFiles
        && previous.failedFiles == next.failedFiles
        && previous.startedAtMs == next.startedAtMs
        && previous.finishedAtMs == next.finishedAtMs
        && blankToEmpty(previous.startedAtChildJobId).equals(blankToEmpty(next.startedAtChildJobId))
        && blankToEmpty(previous.finishedAtChildJobId)
            .equals(blankToEmpty(next.finishedAtChildJobId))
        && blankToEmpty(previous.surfacedMessage).equals(blankToEmpty(next.surfacedMessage))
        && blankToEmpty(previous.surfacedMessageChildJobId)
            .equals(blankToEmpty(next.surfacedMessageChildJobId))
        && blankToEmpty(previous.surfacedMessageChildState)
            .equals(blankToEmpty(next.surfacedMessageChildState))
        && blankToEmpty(previous.executorId).equals(blankToEmpty(next.executorId))
        && blankToEmpty(previous.executorChildJobId).equals(blankToEmpty(next.executorChildJobId));
  }

  private static String aggregateState(StoredReconcileJob job, StoredAggregateRollup rollup) {
    if (job == null) {
      return "";
    }
    String projectedState = projectedStoredState(job);
    if ("JS_FAILED".equals(projectedState) || "JS_CANCELLED".equals(projectedState)) {
      return projectedState;
    }
    if ("JS_CANCELLING".equals(projectedState)) {
      return "JS_CANCELLING";
    }
    if (rollup == null) {
      return projectedState;
    }
    long childCount =
        rollup.queuedChildren
            + rollup.waitingChildren
            + rollup.runningChildren
            + rollup.cancellingChildren
            + rollup.failedChildren
            + rollup.cancelledChildren
            + rollup.succeededChildren;
    if (childCount <= 0L) {
      return projectedState;
    }
    if (rollup.cancellingChildren > 0L) {
      return "JS_CANCELLING";
    }
    if (rollup.runningChildren > 0L) {
      return "JS_RUNNING";
    }
    if (rollup.failedChildren > 0L) {
      return "JS_FAILED";
    }
    if (rollup.cancelledChildren > 0L) {
      return "JS_CANCELLED";
    }
    if (rollup.waitingChildren > 0L && rollup.queuedChildren <= 0L) {
      return "JS_WAITING";
    }
    if (rollup.queuedChildren > 0L) {
      return "JS_QUEUED";
    }
    return "JS_SUCCEEDED";
  }

  private static String projectedStoredState(StoredReconcileJob job) {
    if (job == null) {
      return "";
    }
    String state = blankToEmpty(job.state);
    if ("JS_QUEUED".equals(state) && job.waitingOnDependency) {
      return "JS_WAITING";
    }
    return state;
  }

  private static String aggregateMessage(StoredReconcileJob job, StoredAggregateRollup rollup) {
    if (job == null) {
      return "";
    }
    if ("JS_FAILED".equals(job.state) || "JS_CANCELLED".equals(job.state)) {
      return blankToEmpty(job.message);
    }
    String state = aggregateState(job, rollup);
    if (rollup != null && !blank(rollup.surfacedMessage)) {
      return rollup.surfacedMessage;
    }
    if ("JS_SUCCEEDED".equals(state) && rollup != null) {
      return "";
    }
    if (!isTerminalState(state) && rollup != null) {
      return isTerminalState(job.state) ? "" : blankToEmpty(job.message);
    }
    return blankToEmpty(job.message);
  }

  private static String surfacedContributionMessage(StoredAggregateContribution contribution) {
    if (contribution == null || blank(contribution.message)) {
      return "";
    }
    String message = contribution.message;
    String childPrefix = blankToEmpty(contribution.childJobId) + ": ";
    if (!blank(contribution.childJobId)
        && !message.startsWith(childPrefix)
        && !message.contains(": ")) {
      return childPrefix + message;
    }
    return message;
  }

  private static long aggregateStartedAtMs(StoredReconcileJob job, StoredAggregateRollup rollup) {
    if (job == null) {
      return 0L;
    }
    long childMin = rollup == null ? 0L : rollup.startedAtMs;
    if (job.startedAtMs > 0L && childMin > 0L) {
      return Math.min(job.startedAtMs, childMin);
    }
    return job.startedAtMs > 0L ? job.startedAtMs : childMin;
  }

  private static long aggregateFinishedAtMs(StoredReconcileJob job, StoredAggregateRollup rollup) {
    String state = aggregateState(job, rollup);
    if (!isTerminalState(state)) {
      return 0L;
    }
    return Math.max(job == null ? 0L : job.finishedAtMs, rollup == null ? 0L : rollup.finishedAtMs);
  }

  private static long aggregateTablesScanned(StoredReconcileJob job, StoredAggregateRollup rollup) {
    if (job != null && job.jobKind() == ReconcileJobKind.PLAN_TABLE) {
      if (isExplicitNoOpPlanTable(job)) {
        return 0L;
      }
      long self = aggregateSelfTablesScanned(job);
      if (self > 0L) {
        return self;
      }
      return isTerminalState(job.state) || "JS_RUNNING".equals(job.state) ? 1L : 0L;
    }
    return aggregateSelfTablesScanned(job) + (rollup == null ? 0L : rollup.tablesScanned);
  }

  private static long aggregateTablesChanged(StoredReconcileJob job, StoredAggregateRollup rollup) {
    if (job != null && job.jobKind() == ReconcileJobKind.PLAN_TABLE) {
      if (isExplicitNoOpPlanTable(job)) {
        return 0L;
      }
      long self = aggregateSelfTablesChanged(job);
      if (self > 0L) {
        return self;
      }
      return "JS_SUCCEEDED".equals(job.state) ? 1L : 0L;
    }
    return aggregateSelfTablesChanged(job) + (rollup == null ? 0L : rollup.tablesChanged);
  }

  private static long aggregateViewsScanned(StoredReconcileJob job, StoredAggregateRollup rollup) {
    return aggregateSelfViewsScanned(job) + (rollup == null ? 0L : rollup.viewsScanned);
  }

  private static long aggregateViewsChanged(StoredReconcileJob job, StoredAggregateRollup rollup) {
    return aggregateSelfViewsChanged(job) + (rollup == null ? 0L : rollup.viewsChanged);
  }

  private static long aggregateErrors(StoredReconcileJob job, StoredAggregateRollup rollup) {
    return (job == null ? 0L : job.errors) + (rollup == null ? 0L : rollup.errors);
  }

  private static long aggregateSnapshotsProcessed(
      StoredReconcileJob job, StoredAggregateRollup rollup) {
    return (job == null ? 0L : job.snapshotsProcessed)
        + (rollup == null ? 0L : rollup.snapshotsProcessed);
  }

  private static long aggregateStatsProcessed(
      StoredReconcileJob job, StoredAggregateRollup rollup) {
    return (job == null ? 0L : job.statsProcessed) + (rollup == null ? 0L : rollup.statsProcessed);
  }

  private static long aggregateIndexesProcessed(
      StoredReconcileJob job, StoredAggregateRollup rollup) {
    return (job == null ? 0L : indexesProcessedSelf(job))
        + (rollup == null ? 0L : rollup.indexesProcessed);
  }

  private static String aggregateExecutorId(StoredReconcileJob job, StoredAggregateRollup rollup) {
    if (rollup != null && !blank(rollup.executorId)) {
      return rollup.executorId;
    }
    return job == null ? "" : job.executorId();
  }

  private static long aggregatePlannedFileGroups(
      StoredReconcileJob job, StoredAggregateRollup rollup) {
    if (job == null) {
      return 0L;
    }
    if (job.jobKind() == ReconcileJobKind.EXEC_FILE_GROUP) {
      return job.plannedFileGroups;
    }
    if (job.jobKind() != ReconcileJobKind.PLAN_SNAPSHOT) {
      return 0L;
    }
    if (job.plannedFileGroups > 0L) {
      return job.plannedFileGroups;
    }
    return rollup == null ? 0L : rollup.plannedFileGroups;
  }

  private static long aggregatePlannedFiles(StoredReconcileJob job, StoredAggregateRollup rollup) {
    if (job == null) {
      return 0L;
    }
    if (job.jobKind() == ReconcileJobKind.EXEC_FILE_GROUP) {
      return job.plannedFiles;
    }
    if (job.jobKind() != ReconcileJobKind.PLAN_SNAPSHOT) {
      return 0L;
    }
    if (job.plannedFiles > 0L) {
      return job.plannedFiles;
    }
    return rollup == null ? 0L : rollup.plannedFiles;
  }

  private static long aggregateCompletedFileGroups(
      StoredReconcileJob job, StoredAggregateRollup rollup) {
    if (job == null) {
      return 0L;
    }
    if (job.jobKind() == ReconcileJobKind.EXEC_FILE_GROUP) {
      return job.completedFileGroups;
    }
    if (job.jobKind() == ReconcileJobKind.PLAN_SNAPSHOT && rollup != null) {
      return rollup.completedFileGroups;
    }
    return 0L;
  }

  private static long aggregateFailedFileGroups(
      StoredReconcileJob job, StoredAggregateRollup rollup) {
    if (job == null) {
      return 0L;
    }
    if (job.jobKind() == ReconcileJobKind.EXEC_FILE_GROUP) {
      return job.failedFileGroups;
    }
    if (job.jobKind() == ReconcileJobKind.PLAN_SNAPSHOT && rollup != null) {
      return rollup.failedFileGroups;
    }
    return 0L;
  }

  private static long aggregateCompletedFiles(
      StoredReconcileJob job, StoredAggregateRollup rollup) {
    if (job == null) {
      return 0L;
    }
    if (job.jobKind() == ReconcileJobKind.EXEC_FILE_GROUP) {
      return job.completedFiles;
    }
    if (job.jobKind() == ReconcileJobKind.PLAN_SNAPSHOT && rollup != null) {
      return rollup.completedFiles;
    }
    return 0L;
  }

  private static long aggregateFailedFiles(StoredReconcileJob job, StoredAggregateRollup rollup) {
    if (job == null) {
      return 0L;
    }
    if (job.jobKind() == ReconcileJobKind.EXEC_FILE_GROUP) {
      return job.failedFiles;
    }
    if (job.jobKind() == ReconcileJobKind.PLAN_SNAPSHOT && rollup != null) {
      return rollup.failedFiles;
    }
    return 0L;
  }

  private static StoredAggregateSummary aggregateSummary(
      StoredReconcileJob job, StoredAggregateRollup rollup) {
    StoredAggregateSummary summary = new StoredAggregateSummary();
    summary.accountId = job == null ? "" : job.accountId;
    summary.jobId = job == null ? "" : job.jobId;
    summary.state = aggregateState(job, rollup);
    summary.message = aggregateMessage(job, rollup);
    summary.startedAtMs = aggregateStartedAtMs(job, rollup);
    summary.finishedAtMs = aggregateFinishedAtMs(job, rollup);
    summary.tablesScanned = aggregateTablesScanned(job, rollup);
    summary.tablesChanged = aggregateTablesChanged(job, rollup);
    summary.viewsScanned = aggregateViewsScanned(job, rollup);
    summary.viewsChanged = aggregateViewsChanged(job, rollup);
    summary.errors = aggregateErrors(job, rollup);
    summary.snapshotsProcessed = aggregateSnapshotsProcessed(job, rollup);
    summary.statsProcessed = aggregateStatsProcessed(job, rollup);
    summary.indexesProcessed = aggregateIndexesProcessed(job, rollup);
    summary.executorId = aggregateExecutorId(job, rollup);
    summary.plannedFileGroups = aggregatePlannedFileGroups(job, rollup);
    summary.plannedFiles = aggregatePlannedFiles(job, rollup);
    summary.completedFileGroups = aggregateCompletedFileGroups(job, rollup);
    summary.failedFileGroups = aggregateFailedFileGroups(job, rollup);
    summary.completedFiles = aggregateCompletedFiles(job, rollup);
    summary.failedFiles = aggregateFailedFiles(job, rollup);
    summary.updatedAtMs = System.currentTimeMillis();
    return summary;
  }

  private static long aggregateSelfTablesScanned(StoredReconcileJob job) {
    if (job == null) {
      return 0L;
    }
    return job.jobKind() == ReconcileJobKind.PLAN_CONNECTOR ? 0L : job.tablesScanned;
  }

  private static long aggregateSelfTablesChanged(StoredReconcileJob job) {
    if (job == null) {
      return 0L;
    }
    return job.jobKind() == ReconcileJobKind.PLAN_CONNECTOR ? 0L : job.tablesChanged;
  }

  private static long aggregateSelfViewsScanned(StoredReconcileJob job) {
    if (job == null) {
      return 0L;
    }
    return job.jobKind() == ReconcileJobKind.PLAN_CONNECTOR ? 0L : job.viewsScanned;
  }

  private static long aggregateSelfViewsChanged(StoredReconcileJob job) {
    if (job == null) {
      return 0L;
    }
    return job.jobKind() == ReconcileJobKind.PLAN_CONNECTOR ? 0L : job.viewsChanged;
  }

  private static boolean isExplicitNoOpPlanTable(StoredReconcileJob job) {
    return job != null
        && job.jobKind() == ReconcileJobKind.PLAN_TABLE
        && "JS_SUCCEEDED".equals(job.state)
        && "Planned 0 snapshot job(s)".equals(blankToEmpty(job.message));
  }

  private static StoredAggregateRollup cloneRollup(StoredAggregateRollup source) {
    StoredAggregateRollup copy = new StoredAggregateRollup();
    copy.accountId = source.accountId;
    copy.jobId = source.jobId;
    copy.queuedChildren = source.queuedChildren;
    copy.waitingChildren = source.waitingChildren;
    copy.runningChildren = source.runningChildren;
    copy.cancellingChildren = source.cancellingChildren;
    copy.failedChildren = source.failedChildren;
    copy.cancelledChildren = source.cancelledChildren;
    copy.succeededChildren = source.succeededChildren;
    copy.tablesScanned = source.tablesScanned;
    copy.tablesChanged = source.tablesChanged;
    copy.viewsScanned = source.viewsScanned;
    copy.viewsChanged = source.viewsChanged;
    copy.errors = source.errors;
    copy.snapshotsProcessed = source.snapshotsProcessed;
    copy.statsProcessed = source.statsProcessed;
    copy.indexesProcessed = source.indexesProcessed;
    copy.plannedFileGroups = source.plannedFileGroups;
    copy.plannedFiles = source.plannedFiles;
    copy.completedFileGroups = source.completedFileGroups;
    copy.failedFileGroups = source.failedFileGroups;
    copy.completedFiles = source.completedFiles;
    copy.failedFiles = source.failedFiles;
    copy.startedAtMs = source.startedAtMs;
    copy.startedAtChildJobId = source.startedAtChildJobId;
    copy.finishedAtMs = source.finishedAtMs;
    copy.finishedAtChildJobId = source.finishedAtChildJobId;
    copy.surfacedMessage = source.surfacedMessage;
    copy.surfacedMessageChildJobId = source.surfacedMessageChildJobId;
    copy.surfacedMessageChildState = source.surfacedMessageChildState;
    copy.surfacedMessageUpdatedAtMs = source.surfacedMessageUpdatedAtMs;
    copy.executorId = source.executorId;
    copy.executorChildJobId = source.executorChildJobId;
    copy.updatedAtMs = source.updatedAtMs;
    return copy;
  }

  private static long indexesProcessedSelf(StoredReconcileJob job) {
    if (job == null || job.fileGroupResults == null || job.fileGroupResults.isEmpty()) {
      return 0L;
    }
    return job.fileGroupResults.stream()
        .filter(result -> result != null && result.indexArtifact() != null)
        .filter(
            result ->
                !result.indexArtifact().artifactUri().isBlank()
                    || !result.indexArtifact().artifactFormat().isBlank()
                    || result.indexArtifact().artifactFormatVersion() > 0)
        .count();
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

  private static ReconcileScope normalizeScopeForJobKind(
      ReconcileScope scope,
      ReconcileJobKind jobKind,
      ReconcileTableTask tableTask,
      ReconcileViewTask viewTask) {
    ReconcileScope effectiveScope = scope == null ? ReconcileScope.empty() : scope;
    if (jobKind == ReconcileJobKind.PLAN_TABLE
        && tableTask != null
        && tableTask.strict()
        && !blank(tableTask.destinationTableId())) {
      if (effectiveScope.hasTableFilter()
          && !tableTask.destinationTableId().equals(effectiveScope.destinationTableId())) {
        throw new IllegalArgumentException(
            "table task destinationTableId does not match scope destinationTableId");
      }
      if (effectiveScope.hasViewFilter() || effectiveScope.hasNamespaceFilter()) {
        throw new IllegalArgumentException(
            "table task destinationTableId cannot be combined with namespace or view scope");
      }
      return effectiveScope.hasTableFilter()
          ? effectiveScope
          : ReconcileScope.of(
              List.of(),
              tableTask.destinationTableId(),
              null,
              effectiveScope.destinationCaptureRequests(),
              effectiveScope.capturePolicy(),
              effectiveScope.snapshotSelection());
    }
    if (jobKind == ReconcileJobKind.PLAN_VIEW
        && viewTask != null
        && viewTask.strict()
        && !blank(viewTask.destinationViewId())) {
      if (effectiveScope.hasViewFilter()
          && !viewTask.destinationViewId().equals(effectiveScope.destinationViewId())) {
        throw new IllegalArgumentException(
            "view task destinationViewId does not match scope destinationViewId");
      }
      if (effectiveScope.hasNamespaceFilter()
          && !effectiveScope
              .destinationNamespaceIds()
              .contains(viewTask.destinationNamespaceId())) {
        throw new IllegalArgumentException(
            "view task destinationNamespaceId does not match scope destinationNamespaceIds");
      }
      if (effectiveScope.hasTableFilter() || effectiveScope.hasCaptureRequestFilter()) {
        throw new IllegalArgumentException(
            "view task destinationViewId cannot be combined with table or capture scope");
      }
      return effectiveScope.hasViewFilter()
          ? effectiveScope
          : ReconcileScope.of(
              List.of(),
              null,
              viewTask.destinationViewId(),
              List.of(),
              effectiveScope.capturePolicy(),
              effectiveScope.snapshotSelection());
    }
    return effectiveScope;
  }

  private static String laneKey(
      String connectorId,
      ReconcileScope scope,
      ReconcileJobKind jobKind,
      ReconcileTableTask tableTask,
      ReconcileViewTask viewTask,
      ReconcileSnapshotTask snapshotTask,
      ReconcileFileGroupTask fileGroupTask) {
    String namespaces =
        scope.destinationNamespaceIds().stream().sorted().reduce((a, b) -> a + "," + b).orElse("*");
    if (jobKind == ReconcileJobKind.PLAN_TABLE && tableTask != null) {
      String destinationTableId = blankToEmpty(scope.destinationTableId());
      if (!destinationTableId.isBlank()) {
        return "table|" + destinationTableId;
      }
      String taskDestinationTableId = blankToEmpty(tableTask.destinationTableId());
      if (!taskDestinationTableId.isBlank()) {
        return "table|" + taskDestinationTableId;
      }
      String sourceNamespace = blankToEmpty(tableTask.sourceNamespace());
      String sourceTable = blankToEmpty(tableTask.sourceTable());
      if (!sourceNamespace.isBlank() && !sourceTable.isBlank()) {
        return "table-source|" + sourceNamespace + "|" + sourceTable;
      }
      String destinationNamespaceId = blankToEmpty(tableTask.destinationNamespaceId());
      String destinationDisplayName = blankToEmpty(tableTask.destinationTableDisplayName());
      if (!destinationNamespaceId.isBlank() && !destinationDisplayName.isBlank()) {
        return "table-discovery|" + destinationNamespaceId + "|" + destinationDisplayName;
      }
      return "tables|" + namespaces;
    }
    if (jobKind == ReconcileJobKind.PLAN_VIEW && viewTask != null) {
      return scope.destinationViewId() == null || scope.destinationViewId().isBlank()
          ? "views|" + namespaces
          : "view|" + scope.destinationViewId();
    }
    if (jobKind == ReconcileJobKind.PLAN_SNAPSHOT
        && snapshotTask != null
        && !blank(snapshotTask.tableId())) {
      String snapshotPart =
          snapshotTask.snapshotId() >= 0L ? Long.toString(snapshotTask.snapshotId()) : "*";
      return "snapshot-plan|" + snapshotTask.tableId() + "|" + snapshotPart;
    }
    if (jobKind == ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE
        && snapshotTask != null
        && !blank(snapshotTask.tableId())) {
      String snapshotPart =
          snapshotTask.snapshotId() >= 0L ? Long.toString(snapshotTask.snapshotId()) : "*";
      return "snapshot-finalize|" + snapshotTask.tableId() + "|" + snapshotPart;
    }
    if (jobKind == ReconcileJobKind.EXEC_FILE_GROUP
        && fileGroupTask != null
        && !blank(fileGroupTask.tableId())) {
      String snapshotPart =
          fileGroupTask.snapshotId() >= 0L ? Long.toString(fileGroupTask.snapshotId()) : "*";
      String groupPart = blank(fileGroupTask.groupId()) ? "*" : fileGroupTask.groupId();
      return "file-group|" + fileGroupTask.tableId() + "|" + snapshotPart + "|" + groupPart;
    }
    String resource =
        scope.destinationTableId() != null
            ? scope.destinationTableId()
            : (scope.destinationViewId() == null ? "*" : scope.destinationViewId());
    return namespaces + "|" + resource;
  }

  private static String dedupeKey(
      String accountId,
      String connectorId,
      boolean fullRescan,
      CaptureMode captureMode,
      ReconcileScope scope,
      ReconcileJobKind jobKind,
      ReconcileTableTask tableTask,
      ReconcileViewTask viewTask,
      ReconcileSnapshotTask snapshotTask,
      ReconcileFileGroupTask fileGroupTask,
      ReconcileExecutionPolicy executionPolicy,
      String parentJobId,
      String pinnedExecutorId) {
    String namespaces =
        scope.destinationNamespaceIds().stream().sorted().reduce((a, b) -> a + "," + b).orElse("*");
    String table = scope.destinationTableId() == null ? "*" : scope.destinationTableId();
    String captureRequests =
        scope.destinationCaptureRequests().stream()
            .map(DurableReconcileJobStore::canonicalCaptureRequest)
            .sorted()
            .reduce((a, b) -> a + "," + b)
            .orElse("");
    String capturePolicy = canonicalCapturePolicy(scope.capturePolicy());
    String canonicalTableDisplayName =
        tableTask != null && tableTask.strict() && !blank(tableTask.destinationTableId())
            ? ""
            : (tableTask == null ? "" : tableTask.destinationTableDisplayName());
    String canonicalViewDisplayName =
        viewTask != null && viewTask.strict() && !blank(viewTask.destinationViewId())
            ? ""
            : (viewTask == null ? "" : viewTask.destinationViewDisplayName());
    ReconcileExecutionPolicy policy =
        executionPolicy == null ? ReconcileExecutionPolicy.defaults() : executionPolicy;
    Canonicalizer canonicalizer = new Canonicalizer();
    canonicalizer
        .scalar("account_id", accountId)
        .scalar("connector_id", connectorId)
        .scalar(
            "job_kind", (jobKind == null ? ReconcileJobKind.PLAN_CONNECTOR.name() : jobKind.name()))
        .scalar("full_rescan", fullRescan)
        .scalar("capture_mode", java.util.Objects.requireNonNull(captureMode, "captureMode").name())
        .scalar("table_task.source_namespace", tableTask == null ? "" : tableTask.sourceNamespace())
        .scalar("table_task.source_table", tableTask == null ? "" : tableTask.sourceTable())
        .scalar(
            "table_task.destination_table_id",
            tableTask == null ? "" : blankToEmpty(tableTask.destinationTableId()))
        .scalar(
            "table_task.destination_namespace_id",
            tableTask == null ? "" : tableTask.destinationNamespaceId())
        .scalar("table_task.destination_table_display_name", canonicalTableDisplayName)
        .scalar("table_task.mode", tableTask == null ? "" : tableTask.mode().name())
        .scalar("view_task.source_namespace", viewTask == null ? "" : viewTask.sourceNamespace())
        .scalar("view_task.source_view", viewTask == null ? "" : viewTask.sourceView())
        .scalar(
            "view_task.destination_namespace_id",
            viewTask == null ? "" : viewTask.destinationNamespaceId())
        .scalar(
            "view_task.destination_view_id",
            viewTask == null ? "" : blankToEmpty(viewTask.destinationViewId()))
        .scalar("view_task.destination_view_display_name", canonicalViewDisplayName)
        .scalar("view_task.mode", viewTask == null ? "" : viewTask.mode().name())
        .scalar(
            "snapshot_task.table_id",
            snapshotTask == null ? "" : blankToEmpty(snapshotTask.tableId()))
        .scalar(
            "snapshot_task.snapshot_id",
            String.valueOf(snapshotTask == null ? -1L : snapshotTask.snapshotId()))
        .scalar(
            "snapshot_task.source_namespace",
            snapshotTask == null ? "" : blankToEmpty(snapshotTask.sourceNamespace()))
        .scalar(
            "snapshot_task.source_table",
            snapshotTask == null ? "" : blankToEmpty(snapshotTask.sourceTable()))
        .scalar(
            "snapshot_task.file_group_plan_recorded",
            String.valueOf(snapshotTask != null && snapshotTask.fileGroupPlanRecorded()))
        .scalar(
            "snapshot_task.completion_mode",
            snapshotTask == null ? "" : snapshotTask.completionMode().name())
        .list(
            "snapshot_task.file_groups",
            canonicalSnapshotFileGroups(
                snapshotTask == null ? List.of() : snapshotTask.fileGroups()))
        .scalar(
            "file_group_task.plan_id",
            fileGroupTask == null ? "" : blankToEmpty(fileGroupTask.planId()))
        .scalar(
            "file_group_task.group_id",
            fileGroupTask == null ? "" : blankToEmpty(fileGroupTask.groupId()))
        .scalar(
            "file_group_task.table_id",
            fileGroupTask == null ? "" : blankToEmpty(fileGroupTask.tableId()))
        .scalar(
            "file_group_task.snapshot_id",
            String.valueOf(fileGroupTask == null ? -1L : fileGroupTask.snapshotId()))
        .list(
            "file_group_task.file_paths",
            fileGroupTask == null ? List.of() : fileGroupTask.filePaths())
        .scalar("scope.namespaces", namespaces)
        .scalar("scope.table", table)
        .scalar("scope.view", scope.destinationViewId() == null ? "" : scope.destinationViewId())
        .scalar("scope.capture_requests", captureRequests)
        .scalar("scope.capture_policy", capturePolicy)
        .scalar("policy.execution_class", policy.executionClass().name())
        .scalar("policy.lane", policy.lane())
        .map("policy.attributes", policy.attributes())
        .scalar("parent_job_id", parentJobId == null ? "" : parentJobId.trim())
        .scalar("pinned_executor_id", pinnedExecutorId == null ? "" : pinnedExecutorId.trim());
    return new String(canonicalizer.bytes(), StandardCharsets.UTF_8);
  }

  private static ListCursor decodeListCursor(String pageToken) {
    if (pageToken == null || pageToken.isBlank()) {
      return new ListCursor("", 0);
    }
    if (!pageToken.startsWith(LIST_TOKEN_V1_PREFIX)) {
      return new ListCursor(pageToken, 0);
    }
    try {
      String decoded =
          new String(
              Base64.getUrlDecoder().decode(pageToken.substring(LIST_TOKEN_V1_PREFIX.length())),
              StandardCharsets.UTF_8);
      int separator = decoded.indexOf('\n');
      if (separator < 0) {
        return new ListCursor(decoded, 0);
      }
      String storeToken = decoded.substring(0, separator);
      int skip = Integer.parseInt(decoded.substring(separator + 1));
      return new ListCursor(storeToken, Math.max(0, skip));
    } catch (RuntimeException e) {
      return new ListCursor(pageToken, 0);
    }
  }

  private static void requireExplicitSnapshotCoverage(
      ReconcileJobKind jobKind, ReconcileSnapshotTask snapshotTask, String operation) {
    if (jobKind == null || snapshotTask == null) {
      return;
    }
    if (jobKind == ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE
        && !snapshotTask.fileGroupPlanRecorded()) {
      throw new IllegalArgumentException(
          "FINALIZE_SNAPSHOT_CAPTURE requires explicit snapshot coverage metadata");
    }
  }

  private static String encodeListCursor(String storeToken, int skip) {
    if (skip <= 0) {
      return storeToken == null ? "" : storeToken;
    }
    String payload = (storeToken == null ? "" : storeToken) + "\n" + skip;
    return LIST_TOKEN_V1_PREFIX
        + Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(payload.getBytes(StandardCharsets.UTF_8));
  }

  private static String hashValue(String value) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] payload = value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
      return Base64.getUrlEncoder().withoutPadding().encodeToString(digest.digest(payload));
    } catch (Exception e) {
      return Base64.getUrlEncoder()
          .withoutPadding()
          .encodeToString(String.valueOf(value).getBytes(StandardCharsets.UTF_8));
    }
  }

  private static String canonicalCaptureRequest(ReconcileScope.ScopedCaptureRequest request) {
    if (request == null) {
      return "";
    }
    String selectors =
        request.columnSelectors().stream().sorted().reduce((a, b) -> a + "," + b).orElse("");
    return request.tableId()
        + "|"
        + request.snapshotId()
        + "|"
        + request.targetSpec()
        + "|"
        + selectors;
  }

  private static String canonicalCapturePolicy(ReconcileCapturePolicy policy) {
    if (policy == null || policy.isEmpty()) {
      return "";
    }
    String columns =
        policy.columns().stream()
            .map(
                column ->
                    column.selector() + ":" + column.captureStats() + ":" + column.captureIndex())
            .sorted()
            .reduce((a, b) -> a + "," + b)
            .orElse("");
    String outputs =
        policy.outputs().stream().map(Enum::name).sorted().reduce((a, b) -> a + "," + b).orElse("");
    return columns + "|" + outputs;
  }

  private static List<String> canonicalSnapshotFileGroups(List<ReconcileFileGroupTask> fileGroups) {
    if (fileGroups == null || fileGroups.isEmpty()) {
      return List.of();
    }
    return fileGroups.stream()
        .filter(group -> group != null && !group.isEmpty())
        .map(
            group ->
                blankToEmpty(group.planId())
                    + "|"
                    + blankToEmpty(group.groupId())
                    + "|"
                    + blankToEmpty(group.tableId())
                    + "|"
                    + group.snapshotId()
                    + "|"
                    + String.join(",", group.filePaths()))
        .sorted()
        .toList();
  }

  private static String urlEncode(String value) {
    if (value == null || value.isBlank()) {
      return "";
    }
    return Keys.encodeSegment(value);
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value.trim();
  }

  private static String blankToNull(String value) {
    return value == null || value.isBlank() ? null : value.trim();
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }

  private static String parentPointerKey(String accountId, String parentJobId, String jobId) {
    if (accountId == null
        || accountId.isBlank()
        || parentJobId == null
        || parentJobId.isBlank()
        || jobId == null
        || jobId.isBlank()) {
      return "";
    }
    return Keys.reconcileJobByParentPointer(accountId, parentJobId, jobId);
  }

  private static String stableReferenceBlobUri(String accountId, String jobId) {
    if (blank(accountId) || blank(jobId)) {
      return "";
    }
    return Keys.reconcileJobRefBlobUri(accountId, jobId);
  }

  private static final class StoredEnvelope {
    final String canonicalPointerKey;
    final StoredReconcileJob record;

    private StoredEnvelope(String canonicalPointerKey, StoredReconcileJob record) {
      this.canonicalPointerKey = canonicalPointerKey;
      this.record = record;
    }
  }

  private record RunnableLanePointerTarget(
      String accountId,
      String laneKey,
      String jobId,
      long dueAtMs,
      String canonicalPointerKey,
      String laneQueuePointerKey) {}

  private record LaneQueuePointerTarget(
      String accountId, String laneKey, String jobId, long dueAtMs, String canonicalPointerKey) {}

  private record LeaseExpiryPointerTarget(String accountId, String jobId, long dueAtMs) {}

  private record ListCursor(String storeToken, int skip) {}

  private record LeaseUpdate(boolean accepted, boolean cancellationRequested) {}

  private record AggregateRollupUpdate(StoredAggregateRollup rollup, boolean repaired) {}

  private record StoredLeaseStateEnvelope(Pointer pointer, StoredLeaseState leaseState) {}

  private static final class LeaseReadyDueMetrics {
    final long nowMs;
    long readyPagesScanned;
    long readyCandidatesScanned;
    long laneLeaseMisses;
    long snapshotLeaseMisses;
    long listPointersByPrefixNs;
    long candidateReadRecordNs;
    long canonicalPointerGetNs;
    long tryAcquireLaneLeaseNs;
    long tryAcquireSnapshotLeaseNs;
    long readyPointerDeleteNs;
    long leaseCanonicalNs;

    private LeaseReadyDueMetrics(long nowMs) {
      this.nowMs = nowMs;
    }
  }

  static final class StoredAggregateContribution {
    public String accountId;
    public String parentJobId;
    public String childJobId;
    public String state;
    public String message;
    public long startedAtMs;
    public long finishedAtMs;
    public long tablesScanned;
    public long tablesChanged;
    public long viewsScanned;
    public long viewsChanged;
    public long errors;
    public long snapshotsProcessed;
    public long statsProcessed;
    public long indexesProcessed;
    public long plannedFileGroups;
    public long plannedFiles;
    public long completedFileGroups;
    public long failedFileGroups;
    public long completedFiles;
    public long failedFiles;
    public String executorId;
    public long updatedAtMs;
  }

  static final class StoredAggregateRollup {
    public String accountId;
    public String jobId;
    public long queuedChildren;
    public long waitingChildren;
    public long runningChildren;
    public long cancellingChildren;
    public long failedChildren;
    public long cancelledChildren;
    public long succeededChildren;
    public long tablesScanned;
    public long tablesChanged;
    public long viewsScanned;
    public long viewsChanged;
    public long errors;
    public long snapshotsProcessed;
    public long statsProcessed;
    public long indexesProcessed;
    public long plannedFileGroups;
    public long plannedFiles;
    public long completedFileGroups;
    public long failedFileGroups;
    public long completedFiles;
    public long failedFiles;
    public long startedAtMs;
    public String startedAtChildJobId;
    public long finishedAtMs;
    public String finishedAtChildJobId;
    public String surfacedMessage;
    public String surfacedMessageChildJobId;
    public String surfacedMessageChildState;
    public long surfacedMessageUpdatedAtMs;
    public String executorId;
    public String executorChildJobId;
    public long updatedAtMs;

    static StoredAggregateRollup empty(String accountId, String jobId) {
      StoredAggregateRollup rollup = new StoredAggregateRollup();
      rollup.accountId = accountId;
      rollup.jobId = jobId;
      rollup.startedAtChildJobId = "";
      rollup.finishedAtChildJobId = "";
      rollup.surfacedMessage = "";
      rollup.surfacedMessageChildJobId = "";
      rollup.surfacedMessageChildState = "";
      rollup.executorId = "";
      rollup.executorChildJobId = "";
      return rollup;
    }
  }

  static final class StoredAggregateSummary {
    public String accountId;
    public String jobId;
    public String state;
    public String message;
    public long startedAtMs;
    public long finishedAtMs;
    public long tablesScanned;
    public long tablesChanged;
    public long viewsScanned;
    public long viewsChanged;
    public long errors;
    public long snapshotsProcessed;
    public long statsProcessed;
    public long indexesProcessed;
    public String executorId;
    public long plannedFileGroups;
    public long plannedFiles;
    public long completedFileGroups;
    public long failedFileGroups;
    public long completedFiles;
    public long failedFiles;
    public long updatedAtMs;

    static StoredAggregateSummary empty() {
      StoredAggregateSummary summary = new StoredAggregateSummary();
      summary.state = "";
      summary.message = "";
      summary.executorId = "";
      return summary;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  static final class StoredJobReference {
    public String accountId;
    public String jobId;
    public String canonicalPointerKey;

    static StoredJobReference of(String accountId, String jobId, String canonicalPointerKey) {
      StoredJobReference ref = new StoredJobReference();
      ref.accountId = accountId;
      ref.jobId = jobId;
      ref.canonicalPointerKey = canonicalPointerKey;
      return ref;
    }
  }

  static final class StoredLeaseState {
    public String accountId;
    public String jobId;
    public String leaseOwner;
    public String leaseEpoch;
    public long leaseExpiresAtMs;
    public long updatedAtMs;
  }

  static final class StoredReconcileJob {
    public String jobId;
    public String accountId;
    public String connectorId;
    public String jobKind;
    public String parentJobId;
    public boolean fullRescan;
    public String captureMode;
    public String executionClass;
    public String executionLane;
    public java.util.Map<String, String> executionAttributes = java.util.Map.of();
    public String pinnedExecutorId;
    public String executorId;
    public String sourceNamespace;
    public String sourceTable;
    public String taskMode;
    public String taskDestinationNamespaceId;
    public String taskDestinationTableId;
    public String taskDestinationTableDisplayName;
    public String sourceView;
    public String taskDestinationViewId;
    public String taskDestinationViewDisplayName;
    public String snapshotTaskTableId;
    public long snapshotTaskSnapshotId;
    public String snapshotTaskSourceNamespace;
    public String snapshotTaskSourceTable;
    public List<ReconcileFileGroupTask> snapshotTaskFileGroups = List.of();
    public boolean snapshotTaskFileGroupPlanRecorded;
    public String snapshotTaskCompletionMode =
        ReconcileSnapshotTask.CompletionMode.FILE_GROUPS.name();
    public String snapshotTaskFileGroupPlanBlobUri;
    public int snapshotTaskFileGroupCount;
    public String fileGroupPlanId;
    public String fileGroupGroupId;
    public String fileGroupTableId;
    public long fileGroupSnapshotId;
    public int fileGroupFileCount;
    public List<String> fileGroupPaths = List.of();
    public List<ReconcileFileResult> fileGroupResults = List.of();
    public String destinationTableId;
    public String destinationViewId;
    public List<String> destinationNamespaceIds = List.of();
    public List<ReconcileScope.ScopedCaptureRequest> destinationCaptureRequests = List.of();
    public List<ReconcileCapturePolicy.Column> capturePolicyColumns = List.of();
    public List<String> capturePolicyOutputs = List.of();
    public String snapshotSelectionKind;
    public List<Long> snapshotSelectionIds = List.of();
    public int snapshotSelectionLatestN;
    public String state;
    public String message;
    public long startedAtMs;
    public long finishedAtMs;
    public long tablesScanned;
    public long tablesChanged;
    public long viewsScanned;
    public long viewsChanged;
    public long errors;
    public long snapshotsProcessed;
    public long statsProcessed;
    public String aggregateState;
    public String aggregateMessage;
    public long aggregateStartedAtMs;
    public long aggregateFinishedAtMs;
    public long aggregateTablesScanned;
    public long aggregateTablesChanged;
    public long aggregateViewsScanned;
    public long aggregateViewsChanged;
    public long aggregateErrors;
    public long aggregateSnapshotsProcessed;
    public long aggregateStatsProcessed;
    public long aggregateIndexesProcessed;
    public String aggregateExecutorId;
    public long plannedFileGroups;
    public long plannedFiles;
    public long completedFileGroups;
    public long failedFileGroups;
    public long completedFiles;
    public long failedFiles;

    public int attempt;
    public long nextAttemptAtMs;
    public String leaseOwner;
    public String leaseEpoch;
    public long leaseExpiresAtMs;
    public String lastError;
    public boolean waitingOnDependency;

    public String laneKey;
    public String dedupeKey;
    public String readyPointerKey;
    public String laneQueuePointerKey;
    public String canonicalPointerKey;
    public String currentBlobUri;

    public long createdAtMs;
    public long updatedAtMs;

    static StoredReconcileJob queued(
        String jobId,
        String accountId,
        String connectorId,
        boolean fullRescan,
        CaptureMode captureMode,
        ReconcileScope scope,
        ReconcileJobKind jobKind,
        ReconcileTableTask tableTask,
        ReconcileViewTask viewTask,
        ReconcileSnapshotTask snapshotTask,
        ReconcileFileGroupTask fileGroupTask,
        ReconcileExecutionPolicy executionPolicy,
        String parentJobId,
        String pinnedExecutorId,
        String laneKey,
        String dedupeKey,
        long now,
        String laneQueuePointerKey) {
      StoredReconcileJob rec = new StoredReconcileJob();
      rec.jobId = jobId;
      rec.accountId = accountId;
      rec.connectorId = connectorId;
      rec.jobKind = (jobKind == null ? ReconcileJobKind.PLAN_CONNECTOR : jobKind).name();
      rec.parentJobId = parentJobId == null ? "" : parentJobId.trim();
      rec.fullRescan = fullRescan;
      rec.captureMode = java.util.Objects.requireNonNull(captureMode, "captureMode").name();
      ReconcileExecutionPolicy policy =
          executionPolicy == null ? ReconcileExecutionPolicy.defaults() : executionPolicy;
      rec.executionClass = policy.executionClass().name();
      rec.executionLane = policy.lane();
      rec.executionAttributes = policy.attributes();
      rec.pinnedExecutorId = pinnedExecutorId == null ? "" : pinnedExecutorId.trim();
      rec.executorId = "";
      ReconcileTableTask effectiveTask = tableTask == null ? ReconcileTableTask.empty() : tableTask;
      ReconcileViewTask effectiveViewTask = viewTask == null ? ReconcileViewTask.empty() : viewTask;
      ReconcileSnapshotTask effectiveSnapshotTask =
          snapshotTask == null ? ReconcileSnapshotTask.empty() : snapshotTask;
      ReconcileFileGroupTask effectiveFileGroupTask =
          fileGroupTask == null ? ReconcileFileGroupTask.empty() : fileGroupTask;
      rec.sourceNamespace =
          jobKind == ReconcileJobKind.PLAN_VIEW
              ? effectiveViewTask.sourceNamespace()
              : effectiveTask.sourceNamespace();
      rec.sourceTable = effectiveTask.sourceTable();
      rec.taskMode =
          jobKind == ReconcileJobKind.PLAN_VIEW
              ? effectiveViewTask.mode().name()
              : effectiveTask.mode().name();
      rec.taskDestinationNamespaceId =
          jobKind == ReconcileJobKind.PLAN_VIEW
              ? effectiveViewTask.destinationNamespaceId()
              : effectiveTask.destinationNamespaceId();
      rec.taskDestinationTableId = blankToEmpty(effectiveTask.destinationTableId());
      rec.taskDestinationTableDisplayName = effectiveTask.destinationTableDisplayName();
      rec.sourceView = effectiveViewTask.sourceView();
      rec.taskDestinationViewId = blankToEmpty(effectiveViewTask.destinationViewId());
      rec.taskDestinationViewDisplayName = effectiveViewTask.destinationViewDisplayName();
      rec.snapshotTaskTableId = blankToEmpty(effectiveSnapshotTask.tableId());
      rec.snapshotTaskSnapshotId = effectiveSnapshotTask.snapshotId();
      rec.snapshotTaskSourceNamespace = effectiveSnapshotTask.sourceNamespace();
      rec.snapshotTaskSourceTable = effectiveSnapshotTask.sourceTable();
      rec.snapshotTaskFileGroups = effectiveSnapshotTask.fileGroups();
      rec.snapshotTaskFileGroupPlanRecorded = effectiveSnapshotTask.fileGroupPlanRecorded();
      rec.snapshotTaskCompletionMode = effectiveSnapshotTask.completionMode().name();
      rec.snapshotTaskFileGroupPlanBlobUri =
          blankToEmpty(effectiveSnapshotTask.fileGroupPlanBlobUri());
      rec.snapshotTaskFileGroupCount = effectiveSnapshotTask.fileGroupCount();
      rec.fileGroupPlanId = blankToEmpty(effectiveFileGroupTask.planId());
      rec.fileGroupGroupId = blankToEmpty(effectiveFileGroupTask.groupId());
      rec.fileGroupTableId = blankToEmpty(effectiveFileGroupTask.tableId());
      rec.fileGroupSnapshotId = effectiveFileGroupTask.snapshotId();
      rec.fileGroupFileCount = effectiveFileGroupTask.fileCount();
      rec.fileGroupPaths = effectiveFileGroupTask.filePaths();
      rec.fileGroupResults = effectiveFileGroupTask.fileResults();
      rec.destinationNamespaceIds = scope.destinationNamespaceIds();
      rec.destinationTableId = scope.destinationTableId();
      rec.destinationViewId = scope.destinationViewId();
      rec.destinationCaptureRequests = scope.destinationCaptureRequests();
      rec.capturePolicyColumns = scope.capturePolicy().columns();
      rec.capturePolicyOutputs = scope.capturePolicy().outputs().stream().map(Enum::name).toList();
      rec.snapshotSelectionKind = scope.snapshotSelection().kind().name();
      rec.snapshotSelectionIds = scope.snapshotSelection().snapshotIds();
      rec.snapshotSelectionLatestN = scope.snapshotSelection().latestN();
      rec.state = "JS_QUEUED";
      rec.message = fullRescan ? "Queued (full)" : "Queued";
      rec.aggregateState = rec.state;
      rec.aggregateMessage = rec.message;
      rec.aggregateExecutorId = rec.executorId;
      rec.nextAttemptAtMs = now;
      rec.waitingOnDependency = false;
      rec.attempt = 0;
      rec.laneKey = laneKey;
      rec.dedupeKey = dedupeKey;
      rec.readyPointerKey = null;
      rec.laneQueuePointerKey = laneQueuePointerKey;
      rec.createdAtMs = now;
      rec.updatedAtMs = now;
      refreshStoredCounters(rec);
      return rec;
    }

    CaptureMode captureMode() {
      if (captureMode == null || captureMode.isBlank()) {
        throw new IllegalStateException("reconcile job missing capture mode");
      }
      try {
        return CaptureMode.valueOf(captureMode);
      } catch (IllegalArgumentException ignored) {
        throw new IllegalStateException("reconcile job has invalid capture mode: " + captureMode);
      }
    }

    ReconcileScope toScope() {
      return ReconcileScope.of(
          destinationNamespaceIds,
          destinationTableId,
          destinationViewId,
          destinationCaptureRequests,
          ReconcileCapturePolicy.of(
              capturePolicyColumns,
              capturePolicyOutputs.stream()
                  .map(ReconcileCapturePolicy.Output::valueOf)
                  .collect(java.util.stream.Collectors.toSet())),
          new ReconcileSnapshotSelection(
              snapshotSelectionKind == null || snapshotSelectionKind.isBlank()
                  ? ReconcileSnapshotSelection.Kind.UNSPECIFIED
                  : ReconcileSnapshotSelection.Kind.valueOf(snapshotSelectionKind),
              snapshotSelectionIds,
              snapshotSelectionLatestN));
    }

    ReconcileJobKind jobKind() {
      return ReconcileJobKind.fromString(jobKind);
    }

    ReconcileTableTask tableTask() {
      if (ReconcileTableTask.Mode.DISCOVERY.name().equals(taskMode)) {
        return ReconcileTableTask.discovery(
            sourceNamespace,
            sourceTable,
            taskDestinationNamespaceId,
            blankToNull(taskDestinationTableId),
            taskDestinationTableDisplayName);
      }
      return ReconcileTableTask.of(
          sourceNamespace,
          sourceTable,
          taskDestinationNamespaceId,
          taskDestinationTableId,
          taskDestinationTableDisplayName);
    }

    ReconcileViewTask viewTask() {
      if (ReconcileViewTask.Mode.DISCOVERY.name().equals(taskMode)) {
        return ReconcileViewTask.discovery(
            sourceNamespace,
            sourceView,
            taskDestinationNamespaceId,
            blankToNull(taskDestinationViewId),
            taskDestinationViewDisplayName);
      }
      return ReconcileViewTask.of(
          sourceNamespace,
          sourceView,
          taskDestinationNamespaceId,
          taskDestinationViewId,
          taskDestinationViewDisplayName);
    }

    ReconcileSnapshotTask snapshotTask() {
      return ReconcileSnapshotTask.of(
          snapshotTaskTableId,
          snapshotTaskSnapshotId,
          snapshotTaskSourceNamespace,
          snapshotTaskSourceTable,
          snapshotTaskFileGroups,
          snapshotTaskFileGroupPlanRecorded,
          ReconcileSnapshotTask.CompletionMode.fromString(snapshotTaskCompletionMode),
          snapshotTaskFileGroupPlanBlobUri,
          snapshotTaskFileGroupCount);
    }

    ReconcileFileGroupTask fileGroupTask() {
      return ReconcileFileGroupTask.of(
          fileGroupPlanId,
          fileGroupGroupId,
          fileGroupTableId,
          fileGroupSnapshotId,
          fileGroupFileCount,
          fileGroupPaths,
          fileGroupResults);
    }

    ReconcileExecutionPolicy executionPolicy() {
      return ReconcileExecutionPolicy.of(
          ReconcileExecutionClass.fromString(executionClass), executionLane, executionAttributes);
    }

    String pinnedExecutorId() {
      return pinnedExecutorId == null ? "" : pinnedExecutorId;
    }

    String executorId() {
      return executorId == null ? "" : executorId;
    }

    String parentJobId() {
      return parentJobId == null ? "" : parentJobId;
    }
  }
}
