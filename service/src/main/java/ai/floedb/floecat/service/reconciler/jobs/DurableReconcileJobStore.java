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
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.BulkEnqueueItemResult;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.BulkEnqueueResult;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.BulkEnqueueSpec;
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
import ai.floedb.floecat.storage.spi.PointerStore.CasDelete;
import ai.floedb.floecat.storage.spi.PointerStore.CasOp;
import ai.floedb.floecat.storage.spi.PointerStore.CasUpsert;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import org.eclipse.microprofile.config.Config;
import org.jboss.logging.Logger;

@ApplicationScoped
@IfBuildProperty(name = "floecat.reconciler.job-store", stringValue = "durable")
// This store expects the post-port inline canonical reconcile layout only. It does not read
// legacy StoredJobReference indirection, lane queue/head scheduling rows, or separate lease-state
// blobs. Aggregate parent counters are derived from dedicated contribution pointers, not from the
// canonical job blob.
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
  private static final int LIST_SCAN_MAX_PAGES = 1_000;
  private static final String LIST_TOKEN_V1_PREFIX = "v1:";
  private static final String STATE_LIST_TOKEN_V1_PREFIX = "v1s:";
  private static final String INLINE_JOB_STATE_PREFIX = "inline:reconcile-job:";
  private static final String INLINE_JOB_LEASE_PREFIX = "inline:reconcile-lease:";
  private static final String INLINE_JOB_CONTRIBUTION_PREFIX = "inline:reconcile-contribution:";
  private static final String LEASE_EXPIRY_POINTER_PREFIX =
      "/accounts/by-id/reconcile/job-leases/by-expiry/";
  private static final long INVALID_ORDERED_POINTER_MS = -1L;
  private static final long REVERSE_SORT_MAX_MS = Long.MAX_VALUE;
  private static final int MAX_PENDING_REPAIR_HINTS = 4_096;

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

  private volatile long lastReclaimAtMs;
  private final ReentrantLock reclaimLock = new ReentrantLock();
  // This only suppresses inline DurableReconcileJobStore pointer/index repair. It does not
  // suppress PointerStore aggregate or ancestor repair unless that lower layer separately honors
  // this signal.
  // The goal is to protect enqueue/reportProgress/markRunning/completeLease and adjacent hot-path
  // mutations from stalling on inline pointer repair. Lease, reclaim, and maintenance paths may
  // still perform inline repair when they are not explicitly suppressing it.
  private final ThreadLocal<Boolean> suppressInlineRepair = ThreadLocal.withInitial(() -> false);
  // These hints are in-memory, best-effort cleanup signals only. They are not durable state and
  // may be dropped on restart, deduped, or skipped when invalid.
  private final ConcurrentHashMap<String, RepairHint> pendingRepairHints =
      new ConcurrentHashMap<>();

  private enum RepairDisposition {
    REPAIRED,
    DEFERRED,
    FAILED,
    NOT_NEEDED
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
    return onHotPath(() -> bulkEnqueueInternal(specs));
  }

  private BulkEnqueueResult bulkEnqueueInternal(List<BulkEnqueueSpec> specs) {
    if (specs == null || specs.isEmpty()) {
      return new BulkEnqueueResult(List.of());
    }

    List<BulkEnqueueItemResult> results = new java.util.ArrayList<>(specs.size());
    for (int i = 0; i < specs.size(); i++) {
      results.add(null);
    }
    List<PendingBulkEnqueue> preparedEntries = new java.util.ArrayList<>(specs.size());
    long batchStartedAtMs = System.currentTimeMillis();

    for (int index = 0; index < specs.size(); index++) {
      BulkEnqueueSpec spec = specs.get(index);
      try {
        preparedEntries.add(prepareBulkEnqueue(index, spec, batchStartedAtMs + index));
      } catch (RuntimeException e) {
        results.set(index, failedBulkEnqueue(index, e));
      }
    }

    java.util.Map<String, BulkEnqueueItemResult> batchDedupe = new java.util.HashMap<>();
    java.util.Map<String, PendingBulkEnqueue> representativeByParent =
        new java.util.LinkedHashMap<>();
    java.util.Map<String, Integer> createdChildrenByParent = new java.util.LinkedHashMap<>();
    for (PendingBulkEnqueue entry : preparedEntries) {
      if (results.get(entry.index) != null) {
        continue;
      }

      BulkEnqueueItemResult batchExisting = batchDedupe.get(entry.dedupePointerKey);
      if (batchExisting != null && batchExisting.succeeded()) {
        results.set(
            entry.index, new BulkEnqueueItemResult(entry.index, batchExisting.jobId, false, ""));
        continue;
      }

      try {
        persistBulkPayloads(entry);
        BulkEnqueueItemResult existingResult = commitBulkEntry(entry);
        if (existingResult != null) {
          rollbackFailedBulkEnqueue(entry);
          results.set(entry.index, existingResult);
          if (existingResult.succeeded()) {
            batchDedupe.put(entry.dedupePointerKey, existingResult);
          }
          continue;
        }
      } catch (RuntimeException e) {
        rollbackFailedBulkEnqueue(entry);
        results.set(entry.index, failedBulkEnqueue(entry.index, e));
        continue;
      }

      BulkEnqueueItemResult created =
          new BulkEnqueueItemResult(entry.index, entry.record.jobId, true, "");
      results.set(entry.index, created);
      batchDedupe.put(entry.dedupePointerKey, created);
      if (!blank(entry.record.parentJobId)) {
        representativeByParent.putIfAbsent(entry.record.parentJobId, entry);
        createdChildrenByParent.merge(entry.record.parentJobId, 1, Integer::sum);
      }
    }

    for (var parentEntry : createdChildrenByParent.entrySet()) {
      incrementExpectedChildJobs(parentEntry.getKey(), parentEntry.getValue());
      PendingBulkEnqueue representative = representativeByParent.get(parentEntry.getKey());
      if (representative != null) {
        refreshAncestorContributionRollups(representative.record, false);
      }
    }

    return new BulkEnqueueResult(results);
  }

  private BulkEnqueueItemResult commitBulkEntry(PendingBulkEnqueue entry) {
    for (int attempt = 0; attempt < CAS_MAX; attempt++) {
      var existing = loadActiveFromDedupe(entry.dedupePointerKey);
      if (existing.isPresent()) {
        return new BulkEnqueueItemResult(entry.index, existing.get().jobId, false, "");
      }
      if (pointerStore.compareAndSetBatch(bulkEnqueuePointerOps(entry))) {
        return null;
      }
    }
    return new BulkEnqueueItemResult(
        entry.index, "", false, "Unable to enqueue reconcile job after CAS retries");
  }

  private PendingBulkEnqueue prepareBulkEnqueue(int index, BulkEnqueueSpec spec, long now) {
    if (spec == null) {
      throw new IllegalArgumentException("bulk enqueue spec is required");
    }
    ReconcileJobKind effectiveJobKind =
        spec.jobKind == null ? ReconcileJobKind.PLAN_CONNECTOR : spec.jobKind;
    ReconcileTableTask effectiveTableTask =
        spec.tableTask == null ? ReconcileTableTask.empty() : spec.tableTask;
    ReconcileViewTask effectiveViewTask =
        spec.viewTask == null ? ReconcileViewTask.empty() : spec.viewTask;
    ReconcileSnapshotTask effectiveSnapshotTask =
        spec.snapshotTask == null ? ReconcileSnapshotTask.empty() : spec.snapshotTask;
    List<ReconcileFileGroupTask> snapshotPlanFileGroups =
        materializeSnapshotPlanFileGroups(effectiveSnapshotTask);
    ReconcileFileGroupTask effectiveFileGroupTask =
        spec.fileGroupTask == null ? ReconcileFileGroupTask.empty() : spec.fileGroupTask;
    String jobId = UUID.randomUUID().toString();
    if (effectiveJobKind == ReconcileJobKind.EXEC_FILE_GROUP) {
      LOG.infof(
          "enqueue EXEC_FILE_GROUP identity jobId=%s parentJobId=%s planId=%s groupId=%s tableId=%s snapshotId=%d fileCount=%d paths=%d results=%d",
          jobId,
          spec.parentJobId,
          effectiveFileGroupTask.planId(),
          effectiveFileGroupTask.groupId(),
          effectiveFileGroupTask.tableId(),
          effectiveFileGroupTask.snapshotId(),
          effectiveFileGroupTask.fileCount(),
          effectiveFileGroupTask.filePaths().size(),
          effectiveFileGroupTask.fileResults().size());
    }
    requireExplicitSnapshotCoverage(effectiveJobKind, effectiveSnapshotTask, "bulkEnqueue");
    requireExecFileGroupIdentity(effectiveJobKind, effectiveFileGroupTask);
    ReconcileScope scope =
        normalizeScopeForJobKind(
            spec.scope == null ? ReconcileScope.empty() : spec.scope,
            effectiveJobKind,
            effectiveTableTask,
            effectiveViewTask);
    ReconcileExecutionPolicy policy =
        spec.executionPolicy == null ? ReconcileExecutionPolicy.defaults() : spec.executionPolicy;
    String laneKey =
        laneKey(
            spec.connectorId,
            scope,
            effectiveJobKind,
            effectiveTableTask,
            effectiveViewTask,
            effectiveSnapshotTask,
            effectiveFileGroupTask);
    if (effectiveJobKind == ReconcileJobKind.PLAN_TABLE) {
      LOG.infof(
          "enqueue PLAN_TABLE lane jobId=%s parentJobId=%s target=%s taskTarget=%s source=%s.%s laneKey=%s",
          jobId,
          spec.parentJobId,
          blankToEmpty(scope.destinationTableId()),
          blankToEmpty(effectiveTableTask.destinationTableId()),
          blankToEmpty(effectiveTableTask.sourceNamespace()),
          blankToEmpty(effectiveTableTask.sourceTable()),
          laneKey);
    }
    String dedupeKey =
        dedupeKey(
            spec.accountId,
            spec.connectorId,
            spec.fullRescan,
            spec.captureMode,
            scope,
            effectiveJobKind,
            effectiveTableTask,
            effectiveViewTask,
            effectiveSnapshotTask,
            effectiveFileGroupTask,
            policy,
            spec.parentJobId,
            spec.pinnedExecutorId);
    String dedupeKeyHash = hashValue(dedupeKey);
    String canonicalKey = Keys.reconcileJobStateRowById(spec.accountId, jobId);
    String definitionBlobUri =
        Keys.reconcileJobBlobUri(
            spec.accountId, jobId, "definition-" + now + "-" + UUID.randomUUID());
    String snapshotPlanBlobUri =
        snapshotPlanFileGroups.isEmpty()
            ? ""
            : Keys.reconcileJobBlobUri(
                spec.accountId, jobId, "snapshot-plan-" + now + "-" + UUID.randomUUID());
    String fileGroupPlanBlobUri =
        effectiveFileGroupTask.filePaths().isEmpty()
            ? ""
            : Keys.reconcileJobBlobUri(
                spec.accountId, jobId, "file-group-plan-" + now + "-" + UUID.randomUUID());
    StoredReconcileJob record =
        StoredReconcileJob.queued(
            jobId,
            spec.accountId,
            spec.connectorId,
            spec.fullRescan,
            spec.captureMode,
            scope,
            effectiveJobKind,
            effectiveTableTask,
            effectiveViewTask,
            effectiveSnapshotTask,
            effectiveFileGroupTask,
            policy,
            spec.parentJobId,
            spec.pinnedExecutorId,
            laneKey,
            dedupeKeyHash,
            now,
            readyPointerKeyForDue(spec.accountId, laneKey, jobId, now),
            connectorIndexPointerKey(spec.accountId, spec.connectorId, now, jobId),
            definitionBlobUri,
            snapshotPlanBlobUri,
            fileGroupPlanBlobUri);
    if (effectiveJobKind == ReconcileJobKind.PLAN_SNAPSHOT) {
      LOG.debugf(
          "enqueue persisted PLAN_SNAPSHOT jobId=%s parentJobId=%s connectorId=%s tableId=%s"
              + " snapshotId=%d source=%s.%s fileGroups=%d",
          jobId,
          spec.parentJobId,
          spec.connectorId,
          effectiveSnapshotTask.tableId(),
          effectiveSnapshotTask.snapshotId(),
          effectiveSnapshotTask.sourceNamespace(),
          effectiveSnapshotTask.sourceTable(),
          snapshotPlanFileGroups.size());
    }
    return new PendingBulkEnqueue(
        index,
        Keys.reconcileDedupePointer(spec.accountId, dedupeKeyHash),
        canonicalKey,
        Keys.reconcileJobLookupPointerById(jobId),
        parentPointerKey(spec.accountId, spec.parentJobId, jobId),
        readyPointerKeys(record),
        statePointerKeys(record),
        record.connectorIndexPointerKey,
        definitionBlobUri,
        snapshotPlanBlobUri,
        fileGroupPlanBlobUri,
        StoredJobDefinition.of(scope, effectiveTableTask, effectiveViewTask),
        StoredSnapshotPlanPayload.of(snapshotPlanFileGroups),
        StoredFileGroupPlanPayload.of(effectiveFileGroupTask),
        effectiveFileGroupTask.fileResults().isEmpty()
            ? ReconcileFileGroupTask.empty()
            : effectiveFileGroupTask,
        record);
  }

  private void persistBulkPayloads(PendingBulkEnqueue entry) {
    writeBlob(
        entry.definitionBlobUri, entry.definition, "Failed to persist reconcile job definition");
    entry.definitionWritten = true;
    if (!entry.snapshotPlanBlobUri.isBlank()) {
      writeBlob(
          entry.snapshotPlanBlobUri,
          entry.snapshotPlanPayload,
          "Failed to persist snapshot plan payload");
      entry.snapshotPlanWritten = true;
    }
    if (!entry.fileGroupPlanBlobUri.isBlank()) {
      writeBlob(
          entry.fileGroupPlanBlobUri,
          entry.fileGroupPlanPayload,
          "Failed to persist file group plan payload");
      entry.fileGroupPlanWritten = true;
    }
    if (!entry.resultPayloadTask.isEmpty()) {
      entry.resultBlobUri =
          writeFileGroupResultPayload(
              entry.record.accountId, entry.record.jobId, entry.resultPayloadTask);
      entry.resultPointerWritten = true;
    }
  }

  private void rollbackFailedBulkEnqueue(PendingBulkEnqueue entry) {
    if (entry == null) {
      return;
    }
    if (entry.definitionWritten) {
      blobStore.delete(entry.definitionBlobUri);
    }
    if (entry.snapshotPlanWritten) {
      blobStore.delete(entry.snapshotPlanBlobUri);
    }
    if (entry.fileGroupPlanWritten) {
      blobStore.delete(entry.fileGroupPlanBlobUri);
    }
    if (entry.resultPointerWritten) {
      deletePointerIfPresent(
          Keys.reconcileJobResultPointerById(entry.record.accountId, entry.record.jobId));
    }
    if (entry.resultBlobUri != null && !entry.resultBlobUri.isBlank()) {
      blobStore.delete(entry.resultBlobUri);
    }
  }

  private BulkEnqueueItemResult failedBulkEnqueue(int index, RuntimeException error) {
    Throwable cause = error.getCause() == null ? error : error.getCause();
    String message = error.getMessage();
    if ((message == null || message.isBlank()) && cause != null) {
      message = cause.getMessage();
    }
    if (message == null || message.isBlank()) {
      message = error.getClass().getSimpleName();
    }
    return new BulkEnqueueItemResult(
        index, "", false, message, error instanceof IllegalArgumentException);
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
    return Optional.of(toPublicJob(loaded.get().record));
  }

  @Override
  public Optional<ReconcileJob> getLeaseView(String jobId) {
    var loaded = loadByAnyAccount(jobId);
    if (loaded.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(toCanonicalLeaseView(loaded.get().record));
  }

  @Override
  public ReconcileJobPage list(
      String accountId,
      int pageSize,
      String pageToken,
      String connectorId,
      java.util.Set<String> states) {
    java.util.Set<String> normalizedStates = normalizeStateFilter(states);
    if (!normalizedStates.isEmpty()) {
      List<String> orderedStates = orderedStateFilter(normalizedStates);
      if (connectorId != null && !connectorId.isBlank()) {
        return listByConnectorStateIndexes(
            accountId, pageSize, pageToken, connectorId, orderedStates);
      }
      return listByAccountStateIndexes(accountId, pageSize, pageToken, orderedStates);
    }
    if (connectorId != null && !connectorId.isBlank()) {
      return listByConnectorIndex(accountId, pageSize, pageToken, connectorId, normalizedStates);
    }
    return listAccountWide(accountId, pageSize, pageToken, normalizedStates);
  }

  private ReconcileJobPage listAccountWide(
      String accountId, int pageSize, String pageToken, java.util.Set<String> states) {
    long startedAtMs = System.currentTimeMillis();
    int limit = Math.max(1, pageSize);
    ListCursor cursor = decodeListCursor(pageToken);
    String token = cursor.storeToken();
    int skip = cursor.skip();
    List<ReconcileJob> out = new java.util.ArrayList<>(limit);
    String nextToken = "";
    int pages = 0;
    int pointerCount = 0;
    int recordCount = 0;
    while (out.size() < limit) {
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers =
          pointerStore.listPointersByPrefix(
              Keys.reconcileJobPointerByIdPrefix(accountId), Math.max(limit * 2, 64), token, next);
      if (pointers.isEmpty()) {
        break;
      }
      pointerCount += pointers.size();
      int startIndex = Math.min(skip, pointers.size());
      skip = 0;
      for (int i = startIndex; i < pointers.size(); i++) {
        Pointer ptr = pointers.get(i);
        if (!isCanonicalJobPointerKey(accountId, ptr.getKey())) {
          continue;
        }
        var rec = readRecord(ptr);
        if (rec.isEmpty()) {
          continue;
        }
        recordCount++;
        var job = toPublicJobSummary(rec.get());
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
      pages++;
      if (pages >= LIST_SCAN_MAX_PAGES) {
        LOG.warnf(
            "Account-wide reconcile job list hit page cap accountId=%s out=%d",
            accountId, out.size());
        break;
      }
      token = nextToken;
    }
    LOG.debugf(
        "list total_ms=%d pointer_count=%d record_count=%d contribution_count=%d mode=account accountId=%s returned=%d",
        System.currentTimeMillis() - startedAtMs,
        pointerCount,
        recordCount,
        0,
        accountId,
        out.size());
    return new ReconcileJobPage(out, nextToken);
  }

  private ReconcileJobPage listByConnectorIndex(
      String accountId,
      int pageSize,
      String pageToken,
      String connectorId,
      java.util.Set<String> states) {
    long startedAtMs = System.currentTimeMillis();
    int limit = Math.max(1, pageSize);
    ListCursor cursor = decodeListCursor(pageToken);
    String token = cursor.storeToken();
    int skip = cursor.skip();
    String prefix = Keys.reconcileJobByConnectorPointerPrefix(accountId, connectorId);
    List<ReconcileJob> out = new java.util.ArrayList<>(limit);
    String nextToken = "";
    int pages = 0;
    int pointerCount = 0;
    int recordCount = 0;
    while (out.size() < limit) {
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers =
          pointerStore.listPointersByPrefix(prefix, Math.max(limit * 2, 64), token, next);
      if (pointers.isEmpty()) {
        break;
      }
      pointerCount += pointers.size();
      int startIndex = Math.min(skip, pointers.size());
      skip = 0;
      for (int i = startIndex; i < pointers.size(); i++) {
        Pointer ptr = pointers.get(i);
        var rec = readCurrentRecordFromIndexPointer(ptr);
        if (rec.isEmpty()) {
          continue;
        }
        recordCount++;
        StoredReconcileJob stored = rec.get();
        if (!connectorId.equals(stored.connectorId)) {
          continue;
        }
        var job = toPublicJobSummary(stored);
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
      pages++;
      if (pages >= LIST_SCAN_MAX_PAGES) {
        LOG.warnf(
            "Connector reconcile job list hit page cap accountId=%s connectorId=%s out=%d",
            accountId, connectorId, out.size());
        break;
      }
      token = nextToken;
    }
    LOG.debugf(
        "list total_ms=%d pointer_count=%d record_count=%d contribution_count=%d mode=connector accountId=%s connectorId=%s returned=%d",
        System.currentTimeMillis() - startedAtMs,
        pointerCount,
        recordCount,
        0,
        accountId,
        connectorId,
        out.size());
    return new ReconcileJobPage(out, nextToken);
  }

  private ReconcileJobPage listByAccountStateIndex(
      String accountId, int pageSize, String pageToken, String state) {
    if (blank(accountId) || blank(state)) {
      return new ReconcileJobPage(List.of(), "");
    }
    return listByStatePrefix(
        Keys.reconcileJobByAccountStatePointerPrefix(accountId, state),
        pageSize,
        pageToken,
        stored -> accountId.equals(stored.accountId) && state.equals(stored.state));
  }

  private ReconcileJobPage listByAccountStateIndexes(
      String accountId, int pageSize, String pageToken, List<String> states) {
    if (blank(accountId) || states == null || states.isEmpty()) {
      return new ReconcileJobPage(List.of(), "");
    }
    return listByStateIndexes(
        pageSize,
        pageToken,
        states,
        request ->
            listByAccountStateIndex(
                accountId, request.pageSize(), request.pageToken(), request.state()));
  }

  private ReconcileJobPage listByConnectorStateIndex(
      String accountId, int pageSize, String pageToken, String connectorId, String state) {
    if (blank(accountId) || blank(connectorId) || blank(state)) {
      return new ReconcileJobPage(List.of(), "");
    }
    return listByStatePrefix(
        Keys.reconcileJobByConnectorStatePointerPrefix(accountId, connectorId, state),
        pageSize,
        pageToken,
        stored ->
            accountId.equals(stored.accountId)
                && connectorId.equals(stored.connectorId)
                && state.equals(stored.state));
  }

  private ReconcileJobPage listByConnectorStateIndexes(
      String accountId, int pageSize, String pageToken, String connectorId, List<String> states) {
    if (blank(accountId) || blank(connectorId) || states == null || states.isEmpty()) {
      return new ReconcileJobPage(List.of(), "");
    }
    return listByStateIndexes(
        pageSize,
        pageToken,
        states,
        request ->
            listByConnectorStateIndex(
                accountId, request.pageSize(), request.pageToken(), connectorId, request.state()));
  }

  private ReconcileJobPage listByStateIndexes(
      int pageSize,
      String pageToken,
      List<String> states,
      java.util.function.Function<StateListRequest, ReconcileJobPage> fetchPage) {
    int limit = Math.max(1, pageSize);
    StateListCursor cursor = decodeStateListCursor(pageToken);
    if (states == null || states.isEmpty() || cursor.stateIndex() >= states.size()) {
      return new ReconcileJobPage(List.of(), "");
    }

    List<ReconcileJob> out = new ArrayList<>(limit);
    int stateIndex = Math.max(0, cursor.stateIndex());
    String nestedPageToken = cursor.pageToken();

    while (stateIndex < states.size() && out.size() < limit) {
      ReconcileJobPage page =
          fetchPage.apply(
              new StateListRequest(
                  states.get(stateIndex), Math.max(1, limit - out.size()), nestedPageToken));

      if (page != null && page.jobs != null && !page.jobs.isEmpty()) {
        out.addAll(page.jobs);
      }

      String nextNestedToken = page == null ? "" : page.nextPageToken;
      boolean currentStateExhausted = nextNestedToken == null || nextNestedToken.isBlank();

      if (out.size() >= limit) {
        if (!currentStateExhausted) {
          return new ReconcileJobPage(out, encodeStateListCursor(stateIndex, nextNestedToken));
        }
        if (stateIndex + 1 < states.size()) {
          return new ReconcileJobPage(out, encodeStateListCursor(stateIndex + 1, ""));
        }
        return new ReconcileJobPage(out, "");
      }

      if (!currentStateExhausted) {
        nestedPageToken = nextNestedToken;
        continue;
      }

      stateIndex++;
      nestedPageToken = "";
    }

    return new ReconcileJobPage(out, "");
  }

  private ReconcileJobPage listByStatePrefix(
      String prefix,
      int pageSize,
      String pageToken,
      java.util.function.Predicate<StoredReconcileJob> filter) {
    long startedAtMs = System.currentTimeMillis();
    int limit = Math.max(1, pageSize);
    ListCursor cursor = decodeListCursor(pageToken);
    String token = cursor.storeToken();
    int skip = cursor.skip();
    List<ReconcileJob> out = new ArrayList<>(limit);
    String nextToken = "";
    int pages = 0;
    int pointerCount = 0;
    int recordCount = 0;
    while (out.size() < limit) {
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers =
          pointerStore.listPointersByPrefix(prefix, Math.max(limit * 2, 64), token, next);
      if (pointers.isEmpty()) {
        break;
      }
      pointerCount += pointers.size();
      int startIndex = Math.min(skip, pointers.size());
      skip = 0;
      for (int i = startIndex; i < pointers.size(); i++) {
        Pointer ptr = pointers.get(i);
        var rec = readCurrentRecordFromStateIndexPointer(ptr, filter);
        if (rec.isEmpty()) {
          continue;
        }
        recordCount++;
        out.add(toPublicJobSummary(rec.get()));
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
      pages++;
      if (pages >= LIST_SCAN_MAX_PAGES) {
        LOG.warnf(
            "State-index reconcile job list hit page cap prefix=%s out=%d", prefix, out.size());
        break;
      }
      token = nextToken;
    }
    LOG.debugf(
        "list total_ms=%d pointer_count=%d record_count=%d contribution_count=%d mode=state prefix=%s returned=%d",
        System.currentTimeMillis() - startedAtMs, pointerCount, recordCount, 0, prefix, out.size());
    return new ReconcileJobPage(out, nextToken);
  }

  @Override
  public ReconcileJobPage childJobsPage(
      String accountId, String parentJobId, int pageSize, String pageToken) {
    if (accountId == null || accountId.isBlank() || parentJobId == null || parentJobId.isBlank()) {
      return new ReconcileJobPage(List.of(), "");
    }
    int limit = Math.max(1, pageSize);
    List<ReconcileJob> out = new ArrayList<>();
    String token = pageToken == null ? "" : pageToken;
    String prefix = Keys.reconcileJobByParentPointerPrefix(accountId, parentJobId);
    StringBuilder next = new StringBuilder();
    List<Pointer> pointers = pointerStore.listPointersByPrefix(prefix, limit, token, next);
    for (Pointer ptr : pointers) {
      var rec = readCurrentRecordFromIndexPointer(ptr);
      rec.ifPresent(stored -> out.add(toPublicJob(stored)));
    }
    return new ReconcileJobPage(out, next.toString());
  }

  @Override
  public QueueStats queueStats() {
    long queued =
        Math.max(0, pointerStore.countByPrefix(statePointerPrefix("JS_QUEUED")))
            + Math.max(0, pointerStore.countByPrefix(statePointerPrefix("JS_WAITING")));
    long running = Math.max(0, pointerStore.countByPrefix(statePointerPrefix("JS_RUNNING")));
    long cancelling = Math.max(0, pointerStore.countByPrefix(statePointerPrefix("JS_CANCELLING")));
    long oldestQueued =
        firstPositiveMin(oldestStateTimestamp("JS_QUEUED"), oldestStateTimestamp("JS_WAITING"));
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
    long startedAtMs = System.currentTimeMillis();
    long budgetMs = Math.max(0L, maxMillis);
    long stepStartedAtMs = startedAtMs;
    drainPendingRepairHintsForMaintenance(MAX_PENDING_REPAIR_HINTS, budgetMs);
    long drainElapsedMs = System.currentTimeMillis() - stepStartedAtMs;
    if (budgetMs > 0L && System.currentTimeMillis() - startedAtMs >= budgetMs) {
      LOG.debugf(
          "runMaintenanceOnce total_ms=%d drain_ms=%d ready_repair_ms=%d reclaim_ms=%d",
          System.currentTimeMillis() - startedAtMs, drainElapsedMs, 0, 0);
      return;
    }

    stepStartedAtMs = System.currentTimeMillis();
    repairQueuedReadyPointersIfNeeded(System.currentTimeMillis(), LeaseRequest.all());
    long readyRepairElapsedMs = System.currentTimeMillis() - stepStartedAtMs;
    if (budgetMs > 0L && System.currentTimeMillis() - startedAtMs >= budgetMs) {
      LOG.debugf(
          "runMaintenanceOnce total_ms=%d drain_ms=%d ready_repair_ms=%d reclaim_ms=%d",
          System.currentTimeMillis() - startedAtMs, drainElapsedMs, readyRepairElapsedMs, 0);
      return;
    }

    stepStartedAtMs = System.currentTimeMillis();
    reclaimExpiredLeasesIfDue(System.currentTimeMillis());
    LOG.debugf(
        "runMaintenanceOnce total_ms=%d drain_ms=%d ready_repair_ms=%d reclaim_ms=%d",
        System.currentTimeMillis() - startedAtMs,
        drainElapsedMs,
        readyRepairElapsedMs,
        System.currentTimeMillis() - stepStartedAtMs);
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
  public void persistSnapshotPlan(String jobId, ReconcileSnapshotTask snapshotTask) {
    onHotPath(
        () -> {
          ReconcileSnapshotTask effective =
              snapshotTask == null ? ReconcileSnapshotTask.empty() : snapshotTask;
          LOG.debugf(
              "persistSnapshotPlan jobId=%s tableId=%s snapshotId=%d fileGroups=%d",
              jobId, effective.tableId(), effective.snapshotId(), effective.fileGroups().size());
          var loaded = loadByAnyAccount(jobId);
          if (loaded.isEmpty()) {
            return;
          }
          String accountId = loaded.get().record.accountId;
          String existingBlobUri = blankToEmpty(loaded.get().record.snapshotPlanBlobUri);
          List<ReconcileFileGroupTask> plannedFileGroups =
              materializeSnapshotPlanFileGroups(effective);
          if (effective.fileGroupPlanRecorded()
              && effective.fileGroupCount() > 0
              && plannedFileGroups.isEmpty()
              && blank(existingBlobUri)) {
            throw new IllegalStateException(
                "PLAN_SNAPSHOT has fileGroupPlanRecorded=true but no file groups and no existing"
                    + " snapshot plan blob");
          }
          String nextBlobUri =
              plannedFileGroups.isEmpty()
                  ? existingBlobUri
                  : writeBlob(
                      Keys.reconcileJobBlobUri(
                          accountId,
                          jobId,
                          "snapshot-plan-" + System.currentTimeMillis() + "-" + UUID.randomUUID()),
                      StoredSnapshotPlanPayload.of(plannedFileGroups),
                      "Failed to persist snapshot plan payload");
          AtomicReference<String> previousSnapshotPlanBlobUri = new AtomicReference<>("");
          Optional<StoredEnvelope> updated;
          try {
            updated =
                mutateByJobIdReturningRecord(
                    jobId,
                    existing -> {
                      previousSnapshotPlanBlobUri.set(blankToEmpty(existing.snapshotPlanBlobUri));
                      if (ReconcileJobKind.PLAN_SNAPSHOT.name().equals(existing.jobKind)
                          && !effective.fileGroupPlanRecorded()) {
                        throw new IllegalArgumentException(
                            "persistSnapshotPlan requires explicit snapshot coverage metadata for"
                                + " PLAN_SNAPSHOT jobs");
                      }
                      existing.snapshotTaskTableId = blankToEmpty(effective.tableId());
                      existing.snapshotTaskSnapshotId = effective.snapshotId();
                      existing.snapshotTaskSourceNamespace = effective.sourceNamespace();
                      existing.snapshotTaskSourceTable = effective.sourceTable();
                      existing.snapshotTaskFileGroupPlanRecorded =
                          effective.fileGroupPlanRecorded();
                      existing.snapshotTaskCompletionMode = effective.completionMode().name();
                      existing.snapshotTaskDirectStatsBlobUri =
                          blankToEmpty(effective.directStatsBlobUri());
                      existing.snapshotTaskDirectStatsRecordCount =
                          effective.directStatsRecordCount();
                      existing.snapshotPlanBlobUri = nextBlobUri;
                      return existing;
                    });
          } catch (RuntimeException e) {
            if (!nextBlobUri.isBlank()) {
              blobStore.delete(nextBlobUri);
            }
            throw e;
          }
          if (updated.isEmpty()) {
            if (!nextBlobUri.isBlank()) {
              blobStore.delete(nextBlobUri);
            }
            return;
          }
          refreshAncestorContributionRollups(updated.get().record, true);
          String previousBlobUri = previousSnapshotPlanBlobUri.get();
          if (!plannedFileGroups.isEmpty()
              && !previousBlobUri.isBlank()
              && !previousBlobUri.equals(nextBlobUri)) {
            blobStore.delete(previousBlobUri);
          }
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
    return requireBlob(
            effective.fileGroupPlanBlobUri(),
            StoredSnapshotPlanPayload.class,
            "snapshot plan payload",
            "")
        .fileGroups();
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
          writeFileGroupResultPayload(
              loaded.get().record.accountId, loaded.get().record.jobId, effective);
        });
  }

  @Override
  public void markRunning(String jobId, String leaseEpoch, long startedAtMs, String executorId) {
    onHotPath(
        () ->
            mutateByJobIdReturningRecord(
                    jobId,
                    existing -> {
                      if (!hasActiveLease(
                          jobId, leaseEpoch, existing, "markRunning", false, true, false)) {
                        return null;
                      }
                      boolean cancelling = "JS_CANCELLING".equals(existing.state);
                      if (!cancelling) {
                        existing.state = "JS_RUNNING";
                        existing.message = "Running";
                      }
                      if (existing.startedAtMs <= 0L) {
                        existing.startedAtMs = startedAtMs;
                      }
                      existing.executorId = executorId == null ? "" : executorId;
                      return existing;
                    })
                .ifPresent(env -> refreshAncestorContributionRollups(env.record, false)));
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
          boolean[] changedMaterially = new boolean[] {false};
          mutateByJobIdReturningRecord(
                  jobId,
                  existing -> {
                    if (!hasActiveLease(
                        jobId, leaseEpoch, existing, "markProgress", false, true, false)) {
                      return null;
                    }
                    if (isTerminalState(existing.state)) {
                      return null;
                    }
                    changedMaterially[0] =
                        existing.tablesScanned != tablesScanned
                            || existing.tablesChanged != tablesChanged
                            || existing.viewsScanned != viewsScanned
                            || existing.viewsChanged != viewsChanged
                            || existing.errors != errors
                            || existing.snapshotsProcessed != snapshotsProcessed
                            || existing.statsProcessed != statsProcessed
                            || (message != null
                                && !message.isBlank()
                                && !message.equals(blankToEmpty(existing.message)));
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
                  })
              .ifPresent(
                  env -> {
                    if (changedMaterially[0]) {
                      refreshDirectParentContribution(env.record, false);
                    }
                  });
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
    var loaded = loadByAnyAccount(jobId);
    if (loaded.isEmpty()) {
      return Optional.empty();
    }
    if (accountId != null
        && !accountId.isBlank()
        && !accountId.equals(loaded.get().record.accountId)) {
      return Optional.empty();
    }
    String priorLeaseEpoch = loadLease(loaded.get().record).map(lease -> lease.epoch).orElse("");
    var updated =
        mutateByCanonicalPointerReturningRecord(
            loaded.get().canonicalPointerKey,
            existing -> {
              if (isTerminalState(existing.state) || "JS_CANCELLING".equals(existing.state)) {
                return null;
              }
              if ("JS_RUNNING".equals(existing.state)) {
                long now = System.currentTimeMillis();
                existing.state = "JS_CANCELLING";
                existing.message = (reason == null || reason.isBlank()) ? "Cancelling" : reason;
                existing.nextAttemptAtMs = now;
                existing.readyPointerKey = null;
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
              existing.readyPointerKey = null;
              return existing;
            });
    if (updated.isPresent() && priorLeaseEpoch != null && !priorLeaseEpoch.isBlank()) {
      writeLease(
          loaded.get().record.accountId,
          jobId,
          current -> {
            if (!priorLeaseEpoch.equals(current.epoch)) {
              return null;
            }
            long now = System.currentTimeMillis();
            current.expiresAtMs =
                Math.min(Math.max(now, current.expiresAtMs), now + CANCEL_POKE_MAX_DELAY_MS);
            return current;
          });
    }
    updated.ifPresent(env -> refreshAncestorContributionRollups(env.record, false));
    var post = get(accountId, jobId);
    if (post.isPresent()
        && ("JS_CANCELLED".equals(post.get().state) || "JS_CANCELLING".equals(post.get().state))) {
      if (updated.isPresent()
          && "JS_CANCELLED".equals(post.get().state)
          && priorLeaseEpoch != null
          && !priorLeaseEpoch.isBlank()) {
        clearExecutionLeasesIfOwned(updated.get(), jobId, priorLeaseEpoch);
      } else if (updated.isPresent() && "JS_CANCELLED".equals(post.get().state)) {
        clearLaneLeaseIfOwned(updated.get().record, updated.get().canonicalPointerKey);
        clearSnapshotLeaseIfOwned(updated.get().record, updated.get().canonicalPointerKey);
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
    clearLeaseIfEpochMatches(env.record.accountId, jobId, leaseEpoch);
    clearLaneLeaseIfOwned(env.record, env.canonicalPointerKey);
    clearSnapshotLeaseIfOwned(env.record, env.canonicalPointerKey);
  }

  private void onHotPath(Runnable runnable) {
    onHotPath(
        () -> {
          runnable.run();
          return null;
        });
  }

  private <T> T onHotPath(Supplier<T> supplier) {
    boolean prior = suppressInlineRepair.get();
    suppressInlineRepair.set(true);
    try {
      return supplier.get();
    } finally {
      if (prior) {
        suppressInlineRepair.set(true);
      } else {
        suppressInlineRepair.remove();
      }
    }
  }

  private boolean inlineRepairAllowed() {
    return !Boolean.TRUE.equals(suppressInlineRepair.get());
  }

  // This only defers DurableReconcileJobStore pointer/index repair. It is intentionally scoped to
  // this class; lower storage layers remain responsible for their own repair policy.
  private RepairDisposition deferRepairIfHotPath(
      String repairPhase,
      String repairReason,
      String repairTargetKind,
      String canonicalPointerKey,
      String jobId,
      String state) {
    if (inlineRepairAllowed()) {
      return RepairDisposition.NOT_NEEDED;
    }
    return enqueueRepairHint(
        repairPhase, repairReason, repairTargetKind, canonicalPointerKey, jobId, state);
  }

  private RepairDisposition enqueueRepairHint(
      String repairPhase,
      String repairReason,
      String repairTargetKind,
      String canonicalPointerKey,
      String jobId,
      String state) {
    if (blank(canonicalPointerKey) || ("lookup_pointer".equals(repairTargetKind) && blank(jobId))) {
      LOG.infof(
          "Reconcile repair deferred disposition=SKIPPED_INVALID_REPAIR_HINT phase=%s reason=%s"
              + " target=%s jobId=%s state=%s canonicalKey=%s pendingHints=%d",
          blankToEmpty(repairPhase),
          blankToEmpty(repairReason),
          blankToEmpty(repairTargetKind),
          blankToEmpty(jobId),
          blankToEmpty(state),
          blankToEmpty(canonicalPointerKey),
          pendingRepairHints.size());
      return RepairDisposition.FAILED;
    }
    if (pendingRepairHints.size() >= MAX_PENDING_REPAIR_HINTS) {
      LOG.infof(
          "Reconcile repair deferred disposition=DROPPED_IN_MEMORY_REPAIR_HINT_QUEUE_FULL"
              + " phase=%s reason=%s target=%s jobId=%s state=%s canonicalKey=%s pendingHints=%d",
          blankToEmpty(repairPhase),
          blankToEmpty(repairReason),
          blankToEmpty(repairTargetKind),
          blankToEmpty(jobId),
          blankToEmpty(state),
          blankToEmpty(canonicalPointerKey),
          pendingRepairHints.size());
      return RepairDisposition.FAILED;
    }
    RepairHint hint =
        new RepairHint(
            canonicalPointerKey,
            blankToEmpty(jobId),
            blankToEmpty(state),
            blankToEmpty(repairPhase),
            blankToEmpty(repairReason),
            blankToEmpty(repairTargetKind));
    pendingRepairHints.put(hint.hintKey(), hint);
    LOG.infof(
        "Reconcile repair deferred disposition=DEFERRED_IN_MEMORY_REPAIR_HINT phase=%s reason=%s"
            + " target=%s jobId=%s state=%s canonicalKey=%s pendingHints=%d",
        hint.repairPhase(),
        hint.repairReason(),
        hint.repairTargetKind(),
        hint.jobId(),
        hint.state(),
        hint.canonicalPointerKey(),
        pendingRepairHints.size());
    return RepairDisposition.DEFERRED;
  }

  // This is the only supported production drain entrypoint for deferred repair hints. It is
  // intended for explicit maintenance cadence, not lease acquisition or other hot-path mutations.
  void drainPendingRepairHintsForMaintenance(int maxHints, long maxMillis) {
    if (maxHints <= 0
        || maxMillis <= 0L
        || !inlineRepairAllowed()
        || pendingRepairHints.isEmpty()) {
      return;
    }
    long startNanos = System.nanoTime();
    long budgetNanos = Math.max(0L, maxMillis) * 1_000_000L;
    int drained = 0;
    for (RepairHint hint : new ArrayList<>(pendingRepairHints.values())) {
      if (drained >= maxHints || System.nanoTime() - startNanos > budgetNanos) {
        return;
      }
      if (!pendingRepairHints.remove(hint.hintKey(), hint)) {
        continue;
      }
      applyRepairHint(hint);
      drained++;
    }
  }

  int pendingRepairHintCount() {
    return pendingRepairHints.size();
  }

  private void applyRepairHint(RepairHint hint) {
    if (hint == null || blank(hint.canonicalPointerKey())) {
      return;
    }
    var current = readCanonicalRecordByKey(hint.canonicalPointerKey());
    if (current.isEmpty()) {
      return;
    }
    repairCanonicalPointersIfNeeded(
        hint.canonicalPointerKey(),
        current.get(),
        "deferred_repair:" + hint.repairPhase(),
        hint.repairReason());
  }

  private Optional<StoredEnvelope> loadByAnyAccount(String jobId) {
    if (blank(jobId)) {
      return Optional.empty();
    }
    String lookupKey = Keys.reconcileJobLookupPointerById(jobId);
    Pointer lookup = pointerStore.get(lookupKey).orElse(null);
    if (lookup == null || blank(lookup.getBlobUri())) {
      return Optional.empty();
    }
    String canonicalPointerKey = lookup.getBlobUri();
    Pointer canonicalPointer = pointerStore.get(canonicalPointerKey).orElse(null);
    if (canonicalPointer == null) {
      pointerStore.compareAndDelete(lookupKey, lookup.getVersion());
      return Optional.empty();
    }
    var canonicalRec = readRecord(canonicalPointer);
    if (canonicalRec.isEmpty()) {
      return Optional.empty();
    }
    StoredReconcileJob current = canonicalRec.get();
    if (!jobId.equals(current.jobId)) {
      LOG.warnf(
          "Reconcile lookup pointer mismatch jobId=%s canonicalKey=%s canonicalJobId=%s",
          jobId, canonicalPointerKey, current.jobId);
      pointerStore.compareAndDelete(lookupKey, lookup.getVersion());
      return Optional.empty();
    }
    return Optional.of(new StoredEnvelope(canonicalPointerKey, current));
  }

  private Optional<StoredReconcileJob> readCurrentRecordFromIndexPointer(Pointer indexPointer) {
    if (indexPointer == null || blank(indexPointer.getBlobUri())) {
      return Optional.empty();
    }
    String canonicalPointerKey = indexPointer.getBlobUri();
    Pointer canonicalPointer = pointerStore.get(canonicalPointerKey).orElse(null);
    if (canonicalPointer == null) {
      pointerStore.compareAndDelete(indexPointer.getKey(), indexPointer.getVersion());
      return Optional.empty();
    }
    var canonicalRecord = readRecord(canonicalPointer);
    if (canonicalRecord.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(canonicalRecord.get());
  }

  private Optional<StoredReconcileJob> readCurrentRecordFromStateIndexPointer(
      Pointer indexPointer, java.util.function.Predicate<StoredReconcileJob> filter) {
    var current = readCurrentRecordFromIndexPointer(indexPointer);
    if (current.isEmpty()) {
      return Optional.empty();
    }
    if (filter == null || filter.test(current.get())) {
      return current;
    }
    if (!statePointerKeys(current.get()).contains(indexPointer.getKey())) {
      pointerStore.compareAndDelete(indexPointer.getKey(), indexPointer.getVersion());
    }
    return Optional.empty();
  }

  private boolean mutateByJobId(String jobId, UnaryOperator<StoredReconcileJob> mutator) {
    return mutateByJobIdReturningRecord(jobId, mutator).isPresent();
  }

  private Optional<StoredEnvelope> mutateByJobIdReturningRecord(
      String jobId, UnaryOperator<StoredReconcileJob> mutator) {
    var loaded = loadByAnyAccount(jobId);
    if (loaded.isEmpty()) {
      return Optional.empty();
    }
    return mutateByCanonicalPointerReturningRecord(loaded.get().canonicalPointerKey, mutator);
  }

  private boolean mutateByCanonicalPointer(
      String canonicalPointerKey, UnaryOperator<StoredReconcileJob> mutator) {
    return mutateByCanonicalPointerReturningRecord(canonicalPointerKey, mutator).isPresent();
  }

  private Optional<StoredEnvelope> mutateByCanonicalPointerReturningRecord(
      String canonicalPointerKey, UnaryOperator<StoredReconcileJob> mutator) {
    for (int i = 0; i < CAS_MAX; i++) {
      Pointer currentPointer = pointerStore.get(canonicalPointerKey).orElse(null);
      if (currentPointer == null) {
        return Optional.empty();
      }
      var currentOpt = readRecord(currentPointer);
      if (currentOpt.isEmpty()) {
        return Optional.empty();
      }

      StoredReconcileJob baseline = cloneStoredRecord(currentOpt.get());
      StoredReconcileJob current = cloneStoredRecord(currentOpt.get());
      StoredReconcileJob nextRecord = mutator.apply(current);
      if (nextRecord == null) {
        return Optional.empty();
      }
      assertImmutableJobIdentityPreserved(baseline, nextRecord);

      nextRecord.updatedAtMs = System.currentTimeMillis();
      nextRecord.canonicalPointerKey = canonicalPointerKey;

      Pointer nextPointer =
          Pointer.newBuilder()
              .setKey(canonicalPointerKey)
              .setBlobUri(encodeInlineJobState(nextRecord))
              .setVersion(currentPointer.getVersion() + 1)
              .build();

      List<CasOp> pointerOps =
          reconcilePointerBatchOps(
              canonicalPointerKey, currentPointer, baseline, nextRecord, nextPointer);
      if (pointerStore.compareAndSetBatch(pointerOps)) {
        logStateTransition(baseline, nextRecord, "mutate");
        return Optional.of(new StoredEnvelope(canonicalPointerKey, cloneStoredRecord(nextRecord)));
      }
    }
    return Optional.empty();
  }

  private boolean renewLeaseByJobId(String accountId, String jobId, String leaseEpoch) {
    if (leaseEpoch == null || leaseEpoch.isBlank()) {
      logLeaseSkip(
          "renewLease",
          "Skipping renewLease for reconcile job %s due to missing lease epoch",
          jobId);
      return false;
    }
    StoredJobLease current = loadLease(accountId, jobId).orElse(null);
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
    StoredJobLease renewed = renewLeaseIfEpochMatches(accountId, jobId, leaseEpoch).orElse(null);
    return renewed != null;
  }

  private Optional<LeasedJob> leaseCanonical(
      String canonicalPointerKey, String readyPointerKey, long now) {
    return leaseCanonical(canonicalPointerKey, readyPointerKey, now, null, null);
  }

  private Optional<LeasedJob> leaseCanonical(
      String canonicalPointerKey,
      String readyPointerKey,
      long now,
      Pointer initialPointer,
      StoredReconcileJob initialRecord) {
    for (int i = 0; i < CAS_MAX; i++) {
      Pointer currentPointer;
      StoredReconcileJob record;
      if (i == 0 && initialPointer != null && initialRecord != null) {
        currentPointer = initialPointer;
        record = initialRecord;
      } else {
        currentPointer = pointerStore.get(canonicalPointerKey).orElse(null);
        if (currentPointer == null) {
          return Optional.empty();
        }

        var recordOpt = readRecord(currentPointer);
        if (recordOpt.isEmpty()) {
          return Optional.empty();
        }
        record = recordOpt.get();
      }
      StoredReconcileJob baseline = cloneStoredRecord(record);
      StoredReconcileJob current = cloneStoredRecord(record);

      if (isTerminalState(current.state)) {
        clearDedupeIfOwned(current);
        return Optional.empty();
      }

      if ("JS_WAITING".equals(current.state)) {
        clearReadyPointersIfOwned(current, canonicalPointerKey);
        return Optional.empty();
      }

      if (hasLiveLease(current, true, now)) {
        return Optional.empty();
      }

      if ("JS_CANCELLING".equals(current.state)) {
        return Optional.empty();
      }

      if (!tryAcquireSnapshotLease(current, canonicalPointerKey, now)) {
        repairReadyPointersIfStillCurrent(canonicalPointerKey);
        return Optional.empty();
      }

      String nextLeaseEpoch = "";
      boolean releaseSnapshotLease = true;
      try {
        boolean cancelling = "JS_CANCELLING".equals(current.state);
        if (!cancelling) {
          current.state = "JS_RUNNING";
          current.message = "Leased";
        }
        if (current.startedAtMs <= 0L) {
          current.startedAtMs = now;
        }
        if (!blank(current.readyPointerKey)) {
          current.readyPointerKey = null;
        }

        current.updatedAtMs = now;
        current.canonicalPointerKey = canonicalPointerKey;
        nextLeaseEpoch = UUID.randomUUID().toString();
        assertImmutableJobIdentityPreserved(baseline, current);

        Pointer nextPointer =
            Pointer.newBuilder()
                .setKey(canonicalPointerKey)
                .setBlobUri(encodeInlineJobState(current))
                .setVersion(currentPointer.getVersion() + 1)
                .build();

        String leasePointerKey =
            Keys.reconcileJobLeasePointerById(current.accountId, current.jobId);
        Pointer currentLeasePointer = pointerStore.get(leasePointerKey).orElse(null);
        StoredJobLease currentLease =
            currentLeasePointer == null
                ? StoredJobLease.empty(current.accountId, current.jobId)
                : readInlineJobLease(currentLeasePointer.getBlobUri())
                    .orElse(StoredJobLease.empty(current.accountId, current.jobId));
        if (currentLease.epoch != null
            && !currentLease.epoch.isBlank()
            && currentLease.expiresAtMs > now) {
          repairReadyPointersIfStillCurrent(canonicalPointerKey);
          return Optional.empty();
        }
        StoredJobLease nextLease =
            StoredJobLease.active(current.accountId, current.jobId, nextLeaseEpoch, now + leaseMs);

        long mutationStartMs = System.currentTimeMillis();
        List<CasOp> pointerOps =
            new ArrayList<>(
                reconcilePointerBatchOps(
                    canonicalPointerKey, currentPointer, baseline, current, nextPointer));
        pointerOps.addAll(
            leasePointerBatchOps(leasePointerKey, currentLeasePointer, currentLease, nextLease));
        if (pointerStore.compareAndSetBatch(pointerOps)) {
          releaseSnapshotLease = false;
          try {
            long mutationElapsedMs = System.currentTimeMillis() - mutationStartMs;
            long definitionStartMs = System.currentTimeMillis();
            StoredJobDefinition definition = requireDefinition(current);
            long definitionElapsedMs = System.currentTimeMillis() - definitionStartMs;
            long snapshotTaskElapsedMs = 0L;
            ReconcileSnapshotTask snapshotTask = ReconcileSnapshotTask.empty();
            if (current.jobKind() == ReconcileJobKind.PLAN_SNAPSHOT
                || current.jobKind() == ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE) {
              long snapshotTaskStartMs = System.currentTimeMillis();
              snapshotTask = snapshotTaskFor(current);
              snapshotTaskElapsedMs = System.currentTimeMillis() - snapshotTaskStartMs;
            }
            long fileGroupTaskElapsedMs = 0L;
            ReconcileFileGroupTask fileGroupTask = ReconcileFileGroupTask.empty();
            if (current.jobKind() == ReconcileJobKind.EXEC_FILE_GROUP) {
              long fileGroupTaskStartMs = System.currentTimeMillis();
              fileGroupTask = fileGroupTaskFor(current);
              fileGroupTaskElapsedMs = System.currentTimeMillis() - fileGroupTaskStartMs;
            }
            LOG.debugf(
                "leaseCanonical breakdown jobId=%s kind=%s mutate_ms=%d load_definition_ms=%d"
                    + " snapshot_task_ms=%d file_group_task_ms=%d",
                current.jobId,
                current.jobKind(),
                mutationElapsedMs,
                definitionElapsedMs,
                snapshotTaskElapsedMs,
                fileGroupTaskElapsedMs);
            if (current.jobKind() == ReconcileJobKind.PLAN_SNAPSHOT
                || current.jobKind() == ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE) {
              LOG.debugf(
                  "leaseCanonical snapshot-backed jobId=%s kind=%s connectorId=%s tableId=%s snapshotId=%d"
                      + " source=%s.%s fileGroups=%d",
                  current.jobId,
                  current.jobKind(),
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
                    definition.toScope(),
                    current.executionPolicy(),
                    nextLeaseEpoch,
                    current.pinnedExecutorId(),
                    current.executorId(),
                    current.jobKind(),
                    definition.tableTask(),
                    definition.viewTask(),
                    snapshotTask,
                    fileGroupTask,
                    current.parentJobId()));
          } catch (RuntimeException e) {
            rollbackLeaseCanonicalOnHydrationFailure(canonicalPointerKey, baseline, nextLeaseEpoch);
            throw e;
          }
        }

        clearLaneLeaseIfOwned(current, canonicalPointerKey);
      } finally {
        if (releaseSnapshotLease) {
          clearSnapshotLeaseIfOwned(current, canonicalPointerKey);
        }
      }
    }

    return Optional.empty();
  }

  private void rollbackLeaseCanonicalOnHydrationFailure(
      String canonicalPointerKey, StoredReconcileJob baseline, String leaseEpoch) {
    if (baseline == null || blank(canonicalPointerKey) || blank(leaseEpoch)) {
      return;
    }
    clearLeaseIfEpochMatches(baseline.accountId, baseline.jobId, leaseEpoch);
    clearLaneLeaseIfOwned(baseline, canonicalPointerKey);
    clearSnapshotLeaseIfOwned(baseline, canonicalPointerKey);
    mutateByCanonicalPointer(
        canonicalPointerKey,
        existing -> {
          if (existing == null
              || !"JS_RUNNING".equals(existing.state)
              || !"Leased".equals(blankToEmpty(existing.message))) {
            return existing;
          }
          existing.state = baseline.state;
          existing.message = baseline.message;
          existing.startedAtMs = baseline.startedAtMs;
          existing.finishedAtMs = baseline.finishedAtMs;
          existing.executorId = baseline.executorId;
          existing.attempt = baseline.attempt;
          existing.nextAttemptAtMs = baseline.nextAttemptAtMs;
          existing.lastError = baseline.lastError;
          existing.readyPointerKey = baseline.readyPointerKey;
          return existing;
        });
  }

  private boolean tryAcquireLaneLease(
      StoredReconcileJob record, String canonicalPointerKey, long now) {
    if (record == null
        || record.accountId == null
        || record.accountId.isBlank()
        || record.laneKey == null
        || record.laneKey.isBlank()
        || canonicalPointerKey == null
        || canonicalPointerKey.isBlank()) {
      return false;
    }
    String lanePointerKey = Keys.reconcileLaneLeasePointer(record.accountId, record.laneKey);
    for (int i = 0; i < CAS_MAX; i++) {
      Pointer existing = pointerStore.get(lanePointerKey).orElse(null);
      if (existing == null) {
        Pointer created =
            Pointer.newBuilder()
                .setKey(lanePointerKey)
                .setBlobUri(canonicalPointerKey)
                .setVersion(1L)
                .build();
        if (pointerStore.compareAndSet(lanePointerKey, 0L, created)) {
          return true;
        }
        continue;
      }
      if (canonicalPointerKey.equals(existing.getBlobUri())) {
        var owner = readCanonicalRecordByKey(existing.getBlobUri());
        if (owner.isPresent() && hasActiveLaneLease(owner.get(), now)) {
          return false;
        }
        if (upsertReferencePointer(lanePointerKey, canonicalPointerKey)) {
          return true;
        }
        continue;
      }

      var owner = readCanonicalRecordByKey(existing.getBlobUri());
      if (owner.isPresent()
          && record.jobId.equals(owner.get().jobId)
          && record.accountId.equals(owner.get().accountId)) {
        String ownerCanonicalKey =
            Keys.reconcileJobStateRowById(owner.get().accountId, owner.get().jobId);
        if (!ownerCanonicalKey.equals(existing.getBlobUri())) {
          if (upsertReferencePointer(lanePointerKey, ownerCanonicalKey)) {
            continue;
          }
        }
        if (hasActiveLaneLease(owner.get(), now)) {
          return false;
        }
        if (pointerStore.compareAndDelete(lanePointerKey, existing.getVersion())) {
          continue;
        }
        continue;
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
    return record != null && hasUnexpiredJobLease(record.accountId, record.jobId, now);
  }

  private void clearLaneLeaseIfOwned(StoredReconcileJob record, String expectedReference) {
    if (record == null
        || record.accountId == null
        || record.accountId.isBlank()
        || record.laneKey == null
        || record.laneKey.isBlank()
        || expectedReference == null
        || expectedReference.isBlank()) {
      return;
    }
    String lanePointerKey = Keys.reconcileLaneLeasePointer(record.accountId, record.laneKey);
    Pointer existing = pointerStore.get(lanePointerKey).orElse(null);
    if (existing == null) {
      return;
    }
    if (!expectedReference.equals(existing.getBlobUri())) {
      var owner = readCanonicalRecordByKey(existing.getBlobUri());
      if (owner.isEmpty()
          || !record.jobId.equals(owner.get().jobId)
          || !record.accountId.equals(owner.get().accountId)) {
        return;
      }
    }
    var owner = readCanonicalRecordByKey(existing.getBlobUri());
    if (owner.isEmpty()
        || !record.jobId.equals(owner.get().jobId)
        || !record.accountId.equals(owner.get().accountId)) {
      return;
    }
    if (hasActiveLaneLease(owner.get(), System.currentTimeMillis())) {
      String canonicalKey = Keys.reconcileJobStateRowById(owner.get().accountId, owner.get().jobId);
      if (!canonicalKey.equals(existing.getBlobUri())) {
        upsertReferencePointer(lanePointerKey, canonicalKey);
      }
      return;
    }
    pointerStore.compareAndDelete(lanePointerKey, existing.getVersion());
  }

  private Optional<StoredReconcileJob> loadActiveFromDedupe(String dedupePointerKey) {
    Pointer dedupePointer = pointerStore.get(dedupePointerKey).orElse(null);
    if (dedupePointer == null) {
      return Optional.empty();
    }

    String canonicalPointerKey = dedupePointer.getBlobUri();
    Pointer canonicalPointer = pointerStore.get(canonicalPointerKey).orElse(null);
    if (canonicalPointer == null) {
      pointerStore.compareAndDelete(dedupePointerKey, dedupePointer.getVersion());
      return Optional.empty();
    }

    var canonicalRecordOpt = readRecord(canonicalPointer);
    if (canonicalRecordOpt.isEmpty()) {
      pointerStore.compareAndDelete(dedupePointerKey, dedupePointer.getVersion());
      return Optional.empty();
    }
    StoredReconcileJob record = canonicalRecordOpt.get();

    if (isTerminalState(record.state)) {
      pointerStore.compareAndDelete(dedupePointerKey, dedupePointer.getVersion());
      return Optional.empty();
    }

    record =
        repairCanonicalPointersIfNeeded(
            canonicalPointerKey, record, "active_dedupe_lookup", "canonical_pointer_drift");
    if (requiresReadyPointer(record)
        && !hasValidReadyPointers(record, canonicalPointerKey)
        && !repairReadyPointer(
            canonicalPointerKey, record, "active_dedupe_lookup", "ready_pointer_missing")) {
      LOG.warnf(
          "Active reconcile job %s state=%s has invalid reconcile ready pointers and repair failed",
          record.jobId, record.state);
    }
    if (!hasValidLookupPointer(record.jobId, canonicalPointerKey)
        && !repairLookupPointer(
            record.jobId, canonicalPointerKey, "active_dedupe_lookup", "lookup_pointer_missing")) {
      LOG.warnf(
          "Active reconcile job %s state=%s has no valid lookup pointer and repair failed",
          record.jobId, record.state);
    }

    return Optional.of(record);
  }

  private boolean repairQueuedReadyPointersIfNeeded(long nowMs, LeaseRequest request) {
    boolean repaired = false;
    String token = "";
    int pages = 0;
    while (true) {
      String priorToken = token;
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers =
          pointerStore.listPointersByPrefix(
              statePointerPrefix("JS_QUEUED"), readyScanLimit, token, next);
      if (pointers.isEmpty()) {
        return repaired;
      }
      for (Pointer statePointer : pointers) {
        String canonicalPointerKey = statePointer.getBlobUri();
        if (blank(canonicalPointerKey)) {
          continue;
        }
        var current =
            readCurrentRecordFromStateIndexPointer(
                statePointer,
                record ->
                    record != null
                        && "JS_QUEUED".equals(record.state)
                        && record.nextAttemptAtMs <= nowMs
                        && matchesLeaseRequest(record, request));
        if (current.isEmpty()) {
          continue;
        }
        if (hasValidReadyPointers(current.get(), canonicalPointerKey)) {
          continue;
        }
        repaired |=
            repairReadyPointer(
                canonicalPointerKey, current.get(), "ready_scan_repair", "ready_pointer_missing");
      }
      token = next.toString();
      if (token.isBlank() || token.equals(priorToken)) {
        return repaired;
      }
      pages++;
      if (pages >= LIST_SCAN_MAX_PAGES) {
        return repaired;
      }
    }
  }

  private void reclaimExpiredLeasesIfDue(long nowMs) {
    if (nowMs - lastReclaimAtMs < reclaimIntervalMs) {
      return;
    }
    if (!reclaimLock.tryLock()) {
      return;
    }
    try {
      if (nowMs - lastReclaimAtMs < reclaimIntervalMs) {
        return;
      }
      lastReclaimAtMs = nowMs;

      scanLeaseExpiryPointersForReclaim(nowMs);
    } finally {
      reclaimLock.unlock();
    }
  }

  private void scanLeaseExpiryPointersForReclaim(long nowMs) {
    String token = "";
    int pages = 0;
    while (true) {
      StringBuilder next = new StringBuilder();
      List<Pointer> leaseExpiries =
          pointerStore.listPointersByPrefix(
              LEASE_EXPIRY_POINTER_PREFIX, readyScanLimit, token, next);
      if (leaseExpiries.isEmpty()) {
        return;
      }
      for (Pointer leaseExpiry : leaseExpiries) {
        long expiresAtMs = parseLeaseExpiryMillis(leaseExpiry.getKey());
        if (expiresAtMs == INVALID_ORDERED_POINTER_MS) {
          pointerStore.compareAndDelete(leaseExpiry.getKey(), leaseExpiry.getVersion());
          continue;
        }
        if (expiresAtMs > nowMs) {
          return;
        }
        repairAndReclaimCanonicalJob(leaseExpiry, nowMs);
      }

      String nextToken = next.toString();
      if (nextToken.isBlank()) {
        return;
      }
      if (nextToken.equals(token)) {
        LOG.warn(
            "Reconcile lease-expiry reclaim pagination token did not advance; aborting scan to"
                + " avoid livelock");
        return;
      }
      token = nextToken;
      pages++;
      if (pages >= 10_000) {
        LOG.warn("Reconcile lease-expiry reclaim pagination hit safety page cap; aborting scan");
        return;
      }
    }
  }

  private void repairAndReclaimCanonicalJob(Pointer leaseExpiryPointer, long nowMs) {
    if (leaseExpiryPointer == null || blank(leaseExpiryPointer.getBlobUri())) {
      if (leaseExpiryPointer != null) {
        pointerStore.compareAndDelete(leaseExpiryPointer.getKey(), leaseExpiryPointer.getVersion());
      }
      return;
    }
    String canonicalKey = leaseExpiryPointer.getBlobUri();
    Pointer canonicalPointer = pointerStore.get(canonicalKey).orElse(null);
    if (canonicalPointer == null) {
      pointerStore.compareAndDelete(leaseExpiryPointer.getKey(), leaseExpiryPointer.getVersion());
      return;
    }
    var canonicalRecordOpt = readRecord(canonicalPointer);
    if (canonicalRecordOpt.isEmpty()) {
      pointerStore.compareAndDelete(leaseExpiryPointer.getKey(), leaseExpiryPointer.getVersion());
      return;
    }
    var canonicalRecord = canonicalRecordOpt.get();
    StoredJobLease lease = loadLease(canonicalRecord).orElse(null);
    if (lease == null || lease.epoch == null || lease.epoch.isBlank() || lease.expiresAtMs <= 0L) {
      pointerStore.compareAndDelete(leaseExpiryPointer.getKey(), leaseExpiryPointer.getVersion());
      return;
    }
    String expectedLeaseExpiryKey =
        leaseExpiryPointerKey(lease.expiresAtMs, canonicalRecord.accountId, canonicalRecord.jobId);
    if (!expectedLeaseExpiryKey.equals(leaseExpiryPointer.getKey())) {
      if (!replaceLeaseExpiryPointer(leaseExpiryPointer, expectedLeaseExpiryKey, canonicalKey)) {
        LOG.errorf(
            "Failed to repair reconcile lease-expiry pointer for job %s canonicalKey=%s"
                + " expectedKey=%s",
            canonicalRecord.jobId, canonicalKey, expectedLeaseExpiryKey);
        return;
      }
      return;
    }
    if (lease.expiresAtMs > nowMs) {
      return;
    }
    if (nowMs - lease.expiresAtMs <= leaseRenewGraceMs) {
      return;
    }
    if (!"JS_RUNNING".equals(canonicalRecord.state)
        && !"JS_CANCELLING".equals(canonicalRecord.state)) {
      clearLeaseIfEpochMatches(canonicalRecord.accountId, canonicalRecord.jobId, lease.epoch);
      return;
    }
    if (!hasValidLookupPointer(canonicalRecord.jobId, canonicalKey)
        && !repairLookupPointer(
            canonicalRecord.jobId,
            canonicalKey,
            "lease_reclaim_precheck",
            "lookup_pointer_missing")) {
      LOG.errorf(
          "Failed to repair reconcile lookup pointer for job %s canonicalKey=%s",
          canonicalRecord.jobId, canonicalKey);
    }
    if (requiresReadyPointer(canonicalRecord)
        && !hasValidReadyPointers(canonicalRecord, canonicalKey)
        && !repairReadyPointer(
            canonicalKey, canonicalRecord, "lease_reclaim_precheck", "ready_pointer_missing")) {
      LOG.errorf(
          "Failed to repair reconcile ready pointers for job %s state=%s readyKey=%s",
          canonicalRecord.jobId, canonicalRecord.state, canonicalRecord.readyPointerKey);
    }
    AtomicReference<String> expiredEpoch = new AtomicReference<>("");
    boolean updated =
        mutateByCanonicalPointer(
            canonicalKey,
            record -> {
              if (!"JS_RUNNING".equals(record.state) && !"JS_CANCELLING".equals(record.state)) {
                return null;
              }
              StoredJobLease currentLease = loadLease(record).orElse(null);
              if (currentLease == null
                  || currentLease.epoch == null
                  || currentLease.epoch.isBlank()
                  || currentLease.expiresAtMs <= 0L
                  || currentLease.expiresAtMs > nowMs
                  || nowMs - currentLease.expiresAtMs <= leaseRenewGraceMs) {
                return null;
              }
              expiredEpoch.set(currentLease.epoch);

              boolean wasCancelling = "JS_CANCELLING".equals(record.state);
              if (wasCancelling) {
                record.state = "JS_CANCELLED";
                record.message =
                    blank(record.message) ? "Cancelled after lease expiry" : record.message;
                if (record.startedAtMs <= 0L) {
                  record.startedAtMs = nowMs;
                }
                record.finishedAtMs = nowMs;
                record.readyPointerKey = null;
              } else {
                record.state = "JS_QUEUED";
                record.message = "Lease expired; requeued";
                record.executorId = "";
                record.nextAttemptAtMs = nowMs;
                record.readyPointerKey = readyPointerKeyFor(record, nowMs);
              }
              return record;
            });
    if (updated && expiredEpoch.get() != null && !expiredEpoch.get().isBlank()) {
      clearLeaseIfEpochMatches(
          canonicalRecord.accountId, canonicalRecord.jobId, expiredEpoch.get());
    }
    if (updated) {
      readCanonicalRecordByKey(canonicalKey).ifPresent(this::refreshContributionChain);
    }
  }

  private Optional<LeasedJob> leaseReadyDue(long nowMs, LeaseRequest request) {
    return leaseReadyDue(nowMs, request, null);
  }

  private Optional<LeasedJob> leaseReadyDue(
      long nowMs, LeaseRequest request, LeaseScanStats scanStats) {
    LeaseRequest effective = request == null ? LeaseRequest.all() : request;
    for (ReadyIndexSelection selection : readyScanSelections(effective)) {
      Optional<LeasedJob> leased = leaseReadyDueFromPrefix(nowMs, effective, selection, scanStats);
      if (leased.isPresent()) {
        return leased;
      }
    }
    return Optional.empty();
  }

  private Optional<LeasedJob> leaseReadyDueFromPrefix(
      long nowMs, LeaseRequest request, ReadyIndexSelection selection, LeaseScanStats scanStats) {
    String token = "";
    int pages = 0;
    while (true) {
      if (scanStats != null) {
        scanStats.scanCount++;
      }
      StringBuilder next = new StringBuilder();
      List<Pointer> ready =
          pointerStore.listPointersByPrefix(selection.prefix(), readyScanLimit, token, next);
      if (ready.isEmpty()) {
        return Optional.empty();
      }

      for (Pointer candidate : ready) {
        if (scanStats != null) {
          scanStats.candidateCount++;
        }
        var readyTarget = decodeReadyPointerTarget(candidate.getKey(), selection);
        if (readyTarget == null) {
          if (!shouldSkipMalformedReadyPointer(candidate.getKey(), selection)) {
            pointerStore.compareAndDelete(candidate.getKey(), candidate.getVersion());
          }
          continue;
        }
        if (readyTarget.dueAtMs() > nowMs) {
          return Optional.empty();
        }
        if (!readyTarget.canonicalPointerKey().equals(candidate.getBlobUri())) {
          pointerStore.compareAndDelete(candidate.getKey(), candidate.getVersion());
          continue;
        }
        Pointer canonicalPointer = pointerStore.get(readyTarget.canonicalPointerKey()).orElse(null);
        if (canonicalPointer == null) {
          pointerStore.compareAndDelete(candidate.getKey(), candidate.getVersion());
          continue;
        }
        var recordOpt = readRecord(canonicalPointer);
        if (recordOpt.isEmpty()) {
          pointerStore.compareAndDelete(candidate.getKey(), candidate.getVersion());
          continue;
        }
        StoredReconcileJob record = recordOpt.get();
        if ("JS_WAITING".equals(record.state)) {
          clearReadyPointersIfOwned(record, readyTarget.canonicalPointerKey());
          pointerStore.compareAndDelete(candidate.getKey(), candidate.getVersion());
          continue;
        }
        if (!readyPointerMatchesRecord(candidate.getKey(), readyTarget, record)) {
          pointerStore.compareAndDelete(candidate.getKey(), candidate.getVersion());
          continue;
        }
        if (!hasValidLookupPointer(record.jobId, readyTarget.canonicalPointerKey())) {
          repairLookupPointer(
              record.jobId,
              readyTarget.canonicalPointerKey(),
              "ready_scan",
              "lookup_pointer_missing");
        }
        if (!matchesLeaseRequest(record, request)) {
          continue;
        }
        if (!tryAcquireLaneLease(record, readyTarget.canonicalPointerKey(), nowMs)) {
          continue;
        }
        var leased =
            leaseCanonical(
                readyTarget.canonicalPointerKey(),
                candidate.getKey(),
                nowMs,
                canonicalPointer,
                record);
        if (leased.isPresent()) {
          return leased;
        }
        restoreReadyPointerIfStillCurrent(candidate.getKey(), readyTarget);
        clearLaneLeaseIfOwned(record, readyTarget.canonicalPointerKey());
      }

      String nextToken = next.toString();
      if (nextToken.isBlank()) {
        return Optional.empty();
      }
      if (nextToken.equals(token)) {
        LOG.warn(
            "Reconcile ready pagination token did not advance; aborting ready scan to avoid"
                + " livelock");
        return Optional.empty();
      }
      token = nextToken;
      pages++;
      if (pages >= 10_000) {
        LOG.warn("Reconcile ready pagination hit safety page cap; aborting scan");
        return Optional.empty();
      }
    }
  }

  private Optional<StoredReconcileJob> readRecord(Pointer canonicalPointer) {
    var record = readInlineJobState(canonicalPointer.getBlobUri());
    if (record.isEmpty()) {
      return Optional.empty();
    }
    record.get().canonicalPointerKey = canonicalPointer.getKey();
    return record;
  }

  private boolean matchesLeaseRequest(StoredReconcileJob record, LeaseRequest request) {
    LeaseRequest effective = request == null ? LeaseRequest.all() : request;
    String pinnedExecutorId = record == null ? "" : record.pinnedExecutorId();
    if (!blank(pinnedExecutorId) && !effective.executorIds.contains(pinnedExecutorId)) {
      return false;
    }
    return record != null
        && effective.matches(record.executionPolicy(), pinnedExecutorId, record.jobKind());
  }

  private String readyPointerKeyFor(StoredReconcileJob record, long dueAtMs) {
    return readyPointerKeyForDue(record.accountId, record.laneKey, record.jobId, dueAtMs);
  }

  private String readyPointerKeyForDue(
      String accountId, String laneKey, String jobId, long dueAtMs) {
    return Keys.reconcileReadyPointerByDue(dueAtMs, accountId, laneKey, jobId);
  }

  private String readyPointerKeyFor(
      StoredReconcileJob record, ReadyIndexType indexType, long dueAtMs, String filterValue) {
    if (record == null) {
      return "";
    }
    String normalizedFilterValue = blankToEmpty(filterValue);
    return switch (indexType) {
      case GLOBAL -> readyPointerKeyFor(record, dueAtMs);
      case EXECUTION_CLASS ->
          normalizedFilterValue.isBlank()
              ? ""
              : Keys.reconcileReadyByExecutionClassPointerByDue(
                  dueAtMs, normalizedFilterValue, record.accountId, record.jobId);
      case EXECUTION_LANE ->
          normalizedFilterValue.isBlank()
              ? ""
              : Keys.reconcileReadyByExecutionLanePointerByDue(
                  dueAtMs, normalizedFilterValue, record.accountId, record.jobId);
      case PINNED_EXECUTOR ->
          normalizedFilterValue.isBlank()
              ? ""
              : Keys.reconcileReadyByPinnedExecutorPointerByDue(
                  dueAtMs, normalizedFilterValue, record.accountId, record.jobId);
      case JOB_KIND ->
          normalizedFilterValue.isBlank()
              ? ""
              : Keys.reconcileReadyByJobKindPointerByDue(
                  dueAtMs, normalizedFilterValue, record.accountId, record.jobId);
    };
  }

  private List<String> readyPointerKeys(StoredReconcileJob record) {
    if (record == null || !requiresReadyPointer(record)) {
      return List.of();
    }
    long dueAtMs = readyPointerDueAt(record);
    ReconcileExecutionPolicy executionPolicy = record.executionPolicy();
    List<String> readyKeys = new ArrayList<>();
    readyKeys.add(readyPointerKeyFor(record, dueAtMs));
    String executionClassReadyKey =
        readyPointerKeyFor(
            record,
            ReadyIndexType.EXECUTION_CLASS,
            dueAtMs,
            executionPolicy.executionClass().name());
    if (!executionClassReadyKey.isBlank()) {
      readyKeys.add(executionClassReadyKey);
    }
    String executionLaneReadyKey =
        readyPointerKeyFor(record, ReadyIndexType.EXECUTION_LANE, dueAtMs, executionPolicy.lane());
    if (!executionLaneReadyKey.isBlank()) {
      readyKeys.add(executionLaneReadyKey);
    }
    if (!blank(record.pinnedExecutorId())) {
      String pinnedReadyKey =
          readyPointerKeyFor(
              record, ReadyIndexType.PINNED_EXECUTOR, dueAtMs, record.pinnedExecutorId());
      if (!pinnedReadyKey.isBlank()) {
        readyKeys.add(pinnedReadyKey);
      }
    }
    String kindReadyKey =
        readyPointerKeyFor(record, ReadyIndexType.JOB_KIND, dueAtMs, record.jobKind().name());
    if (!kindReadyKey.isBlank()) {
      readyKeys.add(kindReadyKey);
    }
    return readyKeys;
  }

  private List<ReadyIndexSelection> readyScanSelections(LeaseRequest request) {
    LeaseRequest effective = request == null ? LeaseRequest.all() : request;
    List<ReadyIndexSelection> selections = new ArrayList<>();

    List<String> executorIds =
        effective.executorIds.stream()
            .sorted()
            .filter(executorId -> !executorId.isBlank())
            .toList();
    for (String executorId : executorIds) {
      selections.add(
          new ReadyIndexSelection(
              Keys.reconcileReadyByPinnedExecutorPointerPrefix(executorId),
              ReadyIndexType.PINNED_EXECUTOR,
              executorId));
    }

    if (!effective.lanes.isEmpty() && !effective.lanes.contains(LeaseRequest.anyLaneToken())) {
      effective.lanes.stream()
          .sorted()
          .filter(lane -> !lane.isBlank())
          .forEach(
              lane ->
                  selections.add(
                      new ReadyIndexSelection(
                          Keys.reconcileReadyByExecutionLanePointerPrefix(lane),
                          ReadyIndexType.EXECUTION_LANE,
                          lane)));
    }

    if (!effective.jobKinds.isEmpty()) {
      effective.jobKinds.stream()
          .map(Enum::name)
          .sorted()
          .forEach(
              jobKind ->
                  selections.add(
                      new ReadyIndexSelection(
                          Keys.reconcileReadyByJobKindPointerPrefix(jobKind),
                          ReadyIndexType.JOB_KIND,
                          jobKind)));
    }

    if (!effective.executionClasses.isEmpty()) {
      effective.executionClasses.stream()
          .map(Enum::name)
          .sorted()
          .forEach(
              executionClass ->
                  selections.add(
                      new ReadyIndexSelection(
                          Keys.reconcileReadyByExecutionClassPointerPrefix(executionClass),
                          ReadyIndexType.EXECUTION_CLASS,
                          executionClass)));
    }

    selections.add(
        new ReadyIndexSelection(Keys.reconcileReadyPointerPrefix(), ReadyIndexType.GLOBAL, ""));
    return selections;
  }

  private ReconcileJob toPublicJob(StoredReconcileJob stored) {
    return toPublicJob(stored, true);
  }

  private ReconcileJob toPublicJob(StoredReconcileJob stored, boolean includeDetails) {
    ProjectedPublicJob projected = projectPublicJob(stored, includeDetails, false);
    StoredJobDefinition definition = includeDetails ? requireDefinition(stored) : null;
    ReconcileSnapshotTask snapshotTask =
        includeDetails ? snapshotTaskFor(stored) : ReconcileSnapshotTask.empty();
    ReconcileFileGroupTask fileGroupTask =
        includeDetails ? fileGroupTaskFor(stored) : ReconcileFileGroupTask.empty();
    return new ReconcileJob(
        stored.jobId,
        stored.accountId,
        stored.connectorId,
        projected.state,
        projected.message,
        projected.startedAtMs,
        projected.finishedAtMs,
        projected.tablesScanned,
        projected.tablesChanged,
        projected.viewsScanned,
        projected.viewsChanged,
        projected.errors,
        stored.fullRescan,
        stored.captureMode(),
        projected.snapshotsProcessed,
        projected.statsProcessed,
        projected.projection.indexesProcessed,
        false,
        includeDetails ? definition.toScope() : ReconcileScope.empty(),
        stored.executionPolicy(),
        stored.pinnedExecutorId(),
        projected.executorId,
        stored.jobKind(),
        includeDetails ? definition.tableTask() : ReconcileTableTask.empty(),
        includeDetails ? definition.viewTask() : ReconcileViewTask.empty(),
        snapshotTask,
        fileGroupTask,
        projected.projection.plannedFileGroups,
        projected.projection.plannedFiles,
        projected.projection.completedFileGroups,
        projected.projection.failedFileGroups,
        projected.projection.completedFiles,
        projected.projection.failedFiles,
        stored.parentJobId());
  }

  private ReconcileJob toCanonicalLeaseView(StoredReconcileJob stored) {
    StoredJobDefinition definition = requireDefinition(stored);
    ReconcileSnapshotTask snapshotTask = snapshotTaskFor(stored);
    ReconcileFileGroupTask fileGroupTask = fileGroupTaskFor(stored);
    JobProjection projection = inlineSummaryProjection(stored);
    String state = blankToEmpty(stored.state);
    return new ReconcileJob(
        stored.jobId,
        stored.accountId,
        stored.connectorId,
        state,
        normalizeWaitingStateMessage(state, stored.message),
        stored.startedAtMs,
        stored.finishedAtMs,
        stored.tablesScanned,
        stored.tablesChanged,
        stored.viewsScanned,
        stored.viewsChanged,
        stored.errors,
        stored.fullRescan,
        stored.captureMode(),
        stored.snapshotsProcessed,
        stored.statsProcessed,
        projection.indexesProcessed,
        false,
        definition.toScope(),
        stored.executionPolicy(),
        stored.pinnedExecutorId(),
        stored.executorId(),
        stored.jobKind(),
        definition.tableTask(),
        definition.viewTask(),
        snapshotTask,
        fileGroupTask,
        projection.plannedFileGroups,
        projection.plannedFiles,
        projection.completedFileGroups,
        projection.failedFileGroups,
        projection.completedFiles,
        projection.failedFiles,
        stored.parentJobId());
  }

  private ReconcileJob toPublicJobSummary(StoredReconcileJob stored) {
    JobProjection projection = inlineSummaryProjection(stored);
    String state = blankToEmpty(stored.state);
    return new ReconcileJob(
        stored.jobId,
        stored.accountId,
        stored.connectorId,
        state,
        normalizeWaitingStateMessage(state, stored.message),
        stored.startedAtMs,
        stored.finishedAtMs,
        stored.tablesScanned,
        stored.tablesChanged,
        stored.viewsScanned,
        stored.viewsChanged,
        stored.errors,
        stored.fullRescan,
        stored.captureMode(),
        stored.snapshotsProcessed,
        stored.statsProcessed,
        projection.indexesProcessed,
        false,
        ReconcileScope.empty(),
        stored.executionPolicy(),
        stored.pinnedExecutorId(),
        stored.executorId(),
        stored.jobKind(),
        ReconcileTableTask.empty(),
        ReconcileViewTask.empty(),
        ReconcileSnapshotTask.empty(),
        ReconcileFileGroupTask.empty(),
        projection.plannedFileGroups,
        projection.plannedFiles,
        projection.completedFileGroups,
        projection.failedFileGroups,
        projection.completedFiles,
        projection.failedFiles,
        stored.parentJobId());
  }

  private boolean upsertReadyPointer(String readyPointerKey, String blobUri) {
    return upsertReferencePointer(readyPointerKey, blobUri);
  }

  private boolean upsertReferencePointer(String pointerKey, String reference) {
    for (int i = 0; i < CAS_MAX; i++) {
      Pointer existing = pointerStore.get(pointerKey).orElse(null);
      if (existing == null) {
        Pointer created =
            Pointer.newBuilder().setKey(pointerKey).setBlobUri(reference).setVersion(1L).build();
        if (pointerStore.compareAndSet(pointerKey, 0L, created)) {
          return true;
        }
        continue;
      }
      if (reference.equals(existing.getBlobUri())) {
        return true;
      }

      Pointer next =
          Pointer.newBuilder()
              .setKey(pointerKey)
              .setBlobUri(reference)
              .setVersion(existing.getVersion() + 1)
              .build();
      if (pointerStore.compareAndSet(pointerKey, existing.getVersion(), next)) {
        return true;
      }
    }

    return false;
  }

  private void clearDedupeIfOwned(StoredReconcileJob record) {
    if (record.dedupeKeyHash == null || record.dedupeKeyHash.isBlank()) {
      return;
    }
    String dedupeKey = Keys.reconcileDedupePointer(record.accountId, record.dedupeKeyHash);
    Pointer existing = pointerStore.get(dedupeKey).orElse(null);
    if (existing == null) {
      return;
    }
    var owner = readCanonicalRecordByKey(existing.getBlobUri());
    if (owner.isPresent()
        && record.jobId.equals(owner.get().jobId)
        && record.accountId.equals(owner.get().accountId)) {
      pointerStore.compareAndDelete(dedupeKey, existing.getVersion());
    }
  }

  private void clearReadyPointersIfOwned(StoredReconcileJob record, String canonicalPointerKey) {
    if (record == null || blank(canonicalPointerKey)) {
      return;
    }
    for (String readyPointerKey : readyPointerKeysForCleanup(record)) {
      Pointer existing = pointerStore.get(readyPointerKey).orElse(null);
      if (existing != null && canonicalPointerKey.equals(existing.getBlobUri())) {
        pointerStore.compareAndDelete(readyPointerKey, existing.getVersion());
      }
    }
  }

  private boolean hasValidReadyPointer(String readyPointerKey, String expectedReference) {
    if (readyPointerKey == null
        || readyPointerKey.isBlank()
        || expectedReference == null
        || expectedReference.isBlank()) {
      return false;
    }
    Pointer existing = pointerStore.get(readyPointerKey).orElse(null);
    return existing != null && expectedReference.equals(existing.getBlobUri());
  }

  private boolean hasValidReadyPointers(StoredReconcileJob record, String expectedReference) {
    if (record == null || blank(expectedReference)) {
      return false;
    }
    List<String> readyKeys = readyPointerKeys(record);
    if (readyKeys.isEmpty()) {
      return !requiresReadyPointer(record);
    }
    for (String readyKey : readyKeys) {
      if (!hasValidReadyPointer(readyKey, expectedReference)) {
        return false;
      }
    }
    return true;
  }

  private boolean hasValidLookupPointer(String jobId, String expectedReference) {
    if (jobId == null
        || jobId.isBlank()
        || expectedReference == null
        || expectedReference.isBlank()) {
      return false;
    }
    Pointer existing = pointerStore.get(Keys.reconcileJobLookupPointerById(jobId)).orElse(null);
    return existing != null && expectedReference.equals(existing.getBlobUri());
  }

  private boolean repairLookupPointer(String jobId, String canonicalPointerKey) {
    return repairHandled(repairLookupPointerDisposition(jobId, canonicalPointerKey, "", ""));
  }

  private boolean repairLookupPointer(
      String jobId, String canonicalPointerKey, String repairPhase, String repairReason) {
    return repairHandled(
        repairLookupPointerDisposition(jobId, canonicalPointerKey, repairPhase, repairReason));
  }

  private RepairDisposition repairLookupPointerDisposition(
      String jobId, String canonicalPointerKey, String repairPhase, String repairReason) {
    if (jobId == null
        || jobId.isBlank()
        || canonicalPointerKey == null
        || canonicalPointerKey.isBlank()) {
      return RepairDisposition.FAILED;
    }
    RepairDisposition deferred =
        deferRepairIfHotPath(
            repairPhase, repairReason, "lookup_pointer", canonicalPointerKey, jobId, "");
    if (deferred == RepairDisposition.DEFERRED || deferred == RepairDisposition.FAILED) {
      return deferred;
    }
    boolean repaired =
        upsertReferencePointer(Keys.reconcileJobLookupPointerById(jobId), canonicalPointerKey);
    logRepairEvent(
        repairPhase,
        repairReason,
        "lookup_pointer",
        canonicalPointerKey,
        jobId,
        "",
        repaired ? RepairDisposition.REPAIRED : RepairDisposition.FAILED);
    return repaired ? RepairDisposition.REPAIRED : RepairDisposition.FAILED;
  }

  private StoredReconcileJob repairCanonicalPointersIfNeeded(
      String canonicalPointerKey, StoredReconcileJob record) {
    return repairCanonicalPointersIfNeeded(canonicalPointerKey, record, "", "");
  }

  private StoredReconcileJob repairCanonicalPointersIfNeeded(
      String canonicalPointerKey,
      StoredReconcileJob record,
      String repairPhase,
      String repairReason) {
    if (record == null || blank(canonicalPointerKey)) {
      return record;
    }
    boolean lookupMissing = !hasValidLookupPointer(record.jobId, canonicalPointerKey);
    boolean stateMissing =
        hasStateIndex(record) && !hasValidStatePointer(record, canonicalPointerKey);
    boolean readyMissing =
        requiresReadyPointer(record) && !hasValidReadyPointers(record, canonicalPointerKey);
    boolean actionableRepair =
        (lookupMissing && canRepairLookupPointer(record.jobId, canonicalPointerKey))
            || (stateMissing && canRepairStatePointer(canonicalPointerKey, record))
            || (readyMissing && canRepairReadyPointer(canonicalPointerKey, record));
    if ((lookupMissing || stateMissing || readyMissing)
        && actionableRepair
        && deferRepairIfHotPath(
                repairPhase,
                repairReason,
                "canonical_pointers",
                canonicalPointerKey,
                record.jobId,
                record.state)
            == RepairDisposition.DEFERRED) {
      return record;
    }
    RepairDisposition repaired = RepairDisposition.NOT_NEEDED;
    if (lookupMissing) {
      repaired =
          mergeRepairDisposition(
              repaired,
              repairLookupPointerDisposition(
                  record.jobId, canonicalPointerKey, repairPhase, repairReason));
    }
    if (stateMissing) {
      repaired =
          mergeRepairDisposition(
              repaired,
              repairStatePointerDisposition(
                  canonicalPointerKey, record, repairPhase, repairReason));
    }
    if (readyMissing) {
      repaired =
          mergeRepairDisposition(
              repaired,
              repairReadyPointerDisposition(
                  canonicalPointerKey, record, repairPhase, repairReason));
    }
    if (repaired != RepairDisposition.REPAIRED) {
      return record;
    }
    return readCanonicalRecordByKey(canonicalPointerKey).orElse(record);
  }

  private boolean repairReadyPointer(String canonicalPointerKey, StoredReconcileJob record) {
    return repairHandled(repairReadyPointerDisposition(canonicalPointerKey, record, "", ""));
  }

  private boolean repairReadyPointer(
      String canonicalPointerKey,
      StoredReconcileJob record,
      String repairPhase,
      String repairReason) {
    return repairHandled(
        repairReadyPointerDisposition(canonicalPointerKey, record, repairPhase, repairReason));
  }

  private RepairDisposition repairReadyPointerDisposition(
      String canonicalPointerKey,
      StoredReconcileJob record,
      String repairPhase,
      String repairReason) {
    if (canonicalPointerKey == null || canonicalPointerKey.isBlank() || record == null) {
      return RepairDisposition.FAILED;
    }
    if (!requiresReadyPointer(record)) {
      return RepairDisposition.NOT_NEEDED;
    }
    RepairDisposition deferred =
        deferRepairIfHotPath(
            repairPhase,
            repairReason,
            "ready_pointer",
            canonicalPointerKey,
            record.jobId,
            record.state);
    if (deferred == RepairDisposition.DEFERRED || deferred == RepairDisposition.FAILED) {
      return deferred;
    }
    long dueAt = readyPointerDueAt(record);
    String readyPointerKey = readyPointerKeyFor(record, dueAt);
    boolean needsCanonicalRepair = !readyPointerKey.equals(blankToEmpty(record.readyPointerKey));
    record.readyPointerKey = readyPointerKey;
    boolean repaired = true;
    for (String readyKey : readyPointerKeys(record)) {
      repaired &= upsertReadyPointer(readyKey, canonicalPointerKey);
    }
    if (repaired && needsCanonicalRepair) {
      persistReadyPointerKeyIfChanged(canonicalPointerKey, readyPointerKey);
    }
    logRepairEvent(
        repairPhase,
        repairReason,
        "ready_pointer",
        canonicalPointerKey,
        record.jobId,
        record.state,
        repaired ? RepairDisposition.REPAIRED : RepairDisposition.FAILED);
    return repaired ? RepairDisposition.REPAIRED : RepairDisposition.FAILED;
  }

  private void persistReadyPointerKeyIfChanged(String canonicalPointerKey, String readyPointerKey) {
    mutateByCanonicalPointer(
        canonicalPointerKey,
        existing -> {
          if (!requiresReadyPointer(existing)) {
            return existing;
          }
          if (readyPointerKey.equals(blankToEmpty(existing.readyPointerKey))) {
            return existing;
          }
          existing.readyPointerKey = readyPointerKey;
          return existing;
        });
  }

  private boolean tryAcquireSnapshotLease(
      StoredReconcileJob record, String expectedReference, long now) {
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
                .setBlobUri(expectedReference)
                .setVersion(1L)
                .build();
        if (pointerStore.compareAndSet(pointerKey, 0L, created)) {
          return true;
        }
        continue;
      }
      if (expectedReference.equals(existing.getBlobUri())) {
        return true;
      }
      var owner = readCanonicalRecordByKey(existing.getBlobUri());
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

  private void clearSnapshotLeaseIfOwned(StoredReconcileJob record, String expectedReference) {
    String pointerKey = snapshotLeasePointerKey(record);
    if (pointerKey.isBlank()) {
      return;
    }
    Pointer existing = pointerStore.get(pointerKey).orElse(null);
    if (existing == null) {
      return;
    }
    if (!blank(expectedReference) && !expectedReference.equals(existing.getBlobUri())) {
      var owner = readCanonicalRecordByKey(existing.getBlobUri());
      if (owner.isEmpty()
          || !record.jobId.equals(owner.get().jobId)
          || !record.accountId.equals(owner.get().accountId)) {
        return;
      }
    }
    var owner = readCanonicalRecordByKey(existing.getBlobUri());
    if (owner.isEmpty()
        || !record.jobId.equals(owner.get().jobId)
        || !record.accountId.equals(owner.get().accountId)) {
      return;
    }
    if (hasActiveSnapshotLease(owner.get(), System.currentTimeMillis())) {
      String canonicalKey = Keys.reconcileJobStateRowById(owner.get().accountId, owner.get().jobId);
      if (!canonicalKey.equals(existing.getBlobUri())) {
        upsertReferencePointer(pointerKey, canonicalKey);
      }
      return;
    }
    pointerStore.compareAndDelete(pointerKey, existing.getVersion());
  }

  private void clearPointerIfMatches(String pointerKey, String expectedReference) {
    if (pointerKey == null || pointerKey.isBlank()) {
      return;
    }
    Pointer existing = pointerStore.get(pointerKey).orElse(null);
    if (existing == null) {
      return;
    }
    if (expectedReference != null
        && !expectedReference.isBlank()
        && !expectedReference.equals(existing.getBlobUri())) {
      return;
    }
    pointerStore.compareAndDelete(pointerKey, existing.getVersion());
  }

  private boolean deletePointerIfPresent(String pointerKey) {
    Pointer existing = pointerStore.get(pointerKey).orElse(null);
    return existing != null && pointerStore.compareAndDelete(pointerKey, existing.getVersion());
  }

  private Optional<StoredReconcileJob> readCanonicalRecordByKey(String canonicalPointerKey) {
    if (canonicalPointerKey == null || canonicalPointerKey.isBlank()) {
      return Optional.empty();
    }
    Pointer canonicalPointer = pointerStore.get(canonicalPointerKey).orElse(null);
    return canonicalPointer == null ? Optional.empty() : readRecord(canonicalPointer);
  }

  private List<CasOp> bulkEnqueuePointerOps(PendingBulkEnqueue entry) {
    List<CasOp> ops = new ArrayList<>();
    ops.add(
        new CasUpsert(
            entry.dedupePointerKey,
            0L,
            referencePointer(entry.dedupePointerKey, entry.canonicalKey, 1L)));
    ops.add(
        new CasUpsert(
            entry.canonicalKey, 0L, inlineJobPointer(entry.canonicalKey, entry.record, 1L)));
    ops.add(
        new CasUpsert(
            entry.lookupKey, 0L, referencePointer(entry.lookupKey, entry.canonicalKey, 1L)));
    if (!entry.parentKey.isBlank()) {
      ops.add(
          new CasUpsert(
              entry.parentKey, 0L, referencePointer(entry.parentKey, entry.canonicalKey, 1L)));
    }
    if (!entry.connectorIndexKey.isBlank()) {
      ops.add(
          new CasUpsert(
              entry.connectorIndexKey,
              0L,
              referencePointer(entry.connectorIndexKey, entry.canonicalKey, 1L)));
    }
    for (String stateKey : entry.stateKeys) {
      ops.add(new CasUpsert(stateKey, 0L, referencePointer(stateKey, entry.canonicalKey, 1L)));
    }
    for (String readyKey : entry.readyKeys) {
      ops.add(new CasUpsert(readyKey, 0L, referencePointer(readyKey, entry.canonicalKey, 1L)));
    }
    return ops;
  }

  private List<CasOp> reconcilePointerBatchOps(
      String canonicalPointerKey,
      Pointer currentPointer,
      StoredReconcileJob previous,
      StoredReconcileJob current,
      Pointer nextPointer) {
    List<CasOp> ops = new ArrayList<>();
    ops.add(new CasUpsert(canonicalPointerKey, currentPointer.getVersion(), nextPointer));

    String previousLookupKey =
        previous == null || blank(previous.jobId)
            ? ""
            : Keys.reconcileJobLookupPointerById(previous.jobId);
    String currentLookupKey =
        blank(current.jobId) ? "" : Keys.reconcileJobLookupPointerById(current.jobId);
    if (!currentLookupKey.equals(previousLookupKey)) {
      appendReferenceUpsert(ops, currentLookupKey, canonicalPointerKey);
      if (!previousLookupKey.isBlank()) {
        appendOwnedDelete(ops, previousLookupKey, canonicalPointerKey);
      }
    }
    String previousConnectorIndexPointerKey =
        previous == null ? "" : blankToEmpty(previous.connectorIndexPointerKey);
    String currentConnectorIndexPointerKey = blankToEmpty(current.connectorIndexPointerKey);
    if (!currentConnectorIndexPointerKey.equals(previousConnectorIndexPointerKey)
        && !currentConnectorIndexPointerKey.isBlank()) {
      appendReferenceUpsert(ops, currentConnectorIndexPointerKey, canonicalPointerKey);
    }
    if (!previousConnectorIndexPointerKey.isBlank()
        && !previousConnectorIndexPointerKey.equals(currentConnectorIndexPointerKey)) {
      appendOwnedDelete(ops, previousConnectorIndexPointerKey, canonicalPointerKey);
    }

    List<String> previousStatePointerKeys =
        previous == null ? List.of() : statePointerKeys(previous);
    List<String> currentStatePointerKeys = statePointerKeys(current);
    if (!currentStatePointerKeys.equals(previousStatePointerKeys)) {
      for (String currentStatePointerKey : currentStatePointerKeys) {
        appendReferenceUpsert(ops, currentStatePointerKey, canonicalPointerKey);
      }
      for (String previousStatePointerKey : previousStatePointerKeys) {
        if (!currentStatePointerKeys.contains(previousStatePointerKey)) {
          appendOwnedDelete(ops, previousStatePointerKey, canonicalPointerKey);
        }
      }
    }

    List<String> previousReadyPointerKeys =
        previous == null ? List.of() : readyPointerKeysForCleanup(previous);
    List<String> currentReadyPointerKeys = readyPointerKeys(current);
    if (!currentReadyPointerKeys.equals(previousReadyPointerKeys)) {
      for (String currentReadyPointerKey : currentReadyPointerKeys) {
        appendReferenceUpsert(ops, currentReadyPointerKey, canonicalPointerKey);
      }
      for (String previousReadyPointerKey : previousReadyPointerKeys) {
        if (!currentReadyPointerKeys.contains(previousReadyPointerKey)) {
          appendOwnedDelete(ops, previousReadyPointerKey, canonicalPointerKey);
        }
      }
    }

    String previousDedupePointerKey = previous == null ? "" : dedupePointerKey(previous);
    String currentDedupePointerKey = dedupePointerKey(current);
    boolean previousTerminal = previous != null && isTerminalState(previous.state);
    boolean currentTerminal = isTerminalState(current.state);
    if (currentTerminal) {
      String dedupePointerToDelete =
          previousDedupePointerKey.isBlank() ? currentDedupePointerKey : previousDedupePointerKey;
      if (!dedupePointerToDelete.isBlank()
          && (!previousTerminal
              || !Objects.equals(previousDedupePointerKey, currentDedupePointerKey))) {
        appendOwnedDelete(ops, dedupePointerToDelete, canonicalPointerKey);
      }
    } else if (!currentDedupePointerKey.isBlank()
        && (previousTerminal
            || !Objects.equals(previousDedupePointerKey, currentDedupePointerKey))) {
      appendReferenceUpsert(ops, currentDedupePointerKey, canonicalPointerKey);
      if (!previousDedupePointerKey.isBlank()
          && !Objects.equals(previousDedupePointerKey, currentDedupePointerKey)) {
        appendOwnedDelete(ops, previousDedupePointerKey, canonicalPointerKey);
      }
    }
    return ops;
  }

  private void appendReferenceUpsert(List<CasOp> ops, String pointerKey, String reference) {
    if (blank(pointerKey) || blank(reference)) {
      return;
    }
    Pointer existing = pointerStore.get(pointerKey).orElse(null);
    if (existing != null && reference.equals(existing.getBlobUri())) {
      return;
    }
    long expectedVersion = existing == null ? 0L : existing.getVersion();
    ops.add(
        new CasUpsert(
            pointerKey,
            expectedVersion,
            referencePointer(pointerKey, reference, expectedVersion + 1L)));
  }

  private void appendOwnedDelete(List<CasOp> ops, String pointerKey, String canonicalPointerKey) {
    if (blank(pointerKey) || blank(canonicalPointerKey)) {
      return;
    }
    Pointer existing = pointerStore.get(pointerKey).orElse(null);
    if (existing == null) {
      return;
    }
    if (canonicalPointerKey.equals(existing.getBlobUri())) {
      ops.add(new CasDelete(pointerKey, existing.getVersion()));
    }
  }

  private Pointer referencePointer(String key, String blobUri, long version) {
    return Pointer.newBuilder().setKey(key).setBlobUri(blobUri).setVersion(version).build();
  }

  private Pointer inlineJobPointer(String key, StoredReconcileJob record, long version) {
    return Pointer.newBuilder()
        .setKey(key)
        .setBlobUri(encodeInlineJobState(record))
        .setVersion(version)
        .build();
  }

  private void clearPointerIfOwnedByJob(String pointerKey, StoredReconcileJob ownerRecord) {
    if (pointerKey == null || pointerKey.isBlank() || ownerRecord == null) {
      return;
    }
    Pointer existing = pointerStore.get(pointerKey).orElse(null);
    if (existing == null) {
      return;
    }
    var currentOwner = readCanonicalRecordByKey(existing.getBlobUri());
    if (currentOwner.isPresent()
        && ownerRecord.jobId.equals(currentOwner.get().jobId)
        && ownerRecord.accountId.equals(currentOwner.get().accountId)) {
      pointerStore.compareAndDelete(pointerKey, existing.getVersion());
    }
  }

  private ReadyPointerTarget decodeReadyPointerTarget(
      String readyPointerKey, ReadyIndexSelection selection) {
    long dueAt = parseTimestampFromOrderedPointer(readyPointerKey, selection.prefix());
    if (dueAt == INVALID_ORDERED_POINTER_MS) {
      return null;
    }
    String normalizedKey = normalizePointerKey(readyPointerKey);
    String prefix = normalizePointerKey(selection.prefix());
    if (!normalizedKey.startsWith(prefix)) {
      return null;
    }
    String[] parts = normalizedKey.substring(prefix.length()).split("/");
    try {
      String accountId;
      String jobId;
      if (selection.type() == ReadyIndexType.GLOBAL) {
        if (parts.length != 4) {
          return null;
        }
        accountId = URLDecoder.decode(parts[1], StandardCharsets.UTF_8);
        jobId = URLDecoder.decode(parts[3], StandardCharsets.UTF_8);
      } else {
        if (parts.length != 3) {
          return null;
        }
        accountId = URLDecoder.decode(parts[1], StandardCharsets.UTF_8);
        jobId = URLDecoder.decode(parts[2], StandardCharsets.UTF_8);
      }
      return new ReadyPointerTarget(
          Keys.reconcileJobStateRowById(accountId, jobId),
          accountId,
          jobId,
          dueAt,
          selection.type(),
          selection.filterValue());
    } catch (Exception e) {
      return null;
    }
  }

  private boolean readyPointerMatchesRecord(
      String candidateKey, ReadyPointerTarget target, StoredReconcileJob record) {
    if (record == null || target == null || candidateKey == null || candidateKey.isBlank()) {
      return false;
    }
    if ("JS_WAITING".equals(record.state)) {
      return false;
    }
    if (!requiresReadyPointer(record)) {
      return false;
    }
    if (!target.accountId().equals(record.accountId) || !target.jobId().equals(record.jobId)) {
      return false;
    }
    if (record.nextAttemptAtMs != target.dueAtMs()) {
      return false;
    }
    if (!readyIndexFilterMatchesRecord(target, record)) {
      return false;
    }
    String expectedKey =
        readyPointerKeyFor(record, target.indexType(), target.dueAtMs(), target.filterValue());
    if (!candidateKey.equals(expectedKey)) {
      return false;
    }
    return target.indexType() != ReadyIndexType.GLOBAL
        || candidateKey.equals(record.readyPointerKey);
  }

  private boolean readyIndexFilterMatchesRecord(
      ReadyPointerTarget target, StoredReconcileJob record) {
    if (target == null || record == null) {
      return false;
    }
    ReconcileExecutionPolicy policy = record.executionPolicy();
    return switch (target.indexType()) {
      case GLOBAL -> true;
      case EXECUTION_CLASS -> target.filterValue().equals(policy.executionClass().name());
      case EXECUTION_LANE -> target.filterValue().equals(policy.lane());
      case PINNED_EXECUTOR -> target.filterValue().equals(record.pinnedExecutorId());
      case JOB_KIND -> target.filterValue().equals(record.jobKind().name());
    };
  }

  private void restoreReadyPointerIfStillCurrent(
      String readyPointerKey, ReadyPointerTarget target) {
    if (readyPointerKey == null || readyPointerKey.isBlank() || target == null) {
      return;
    }
    Pointer existing = pointerStore.get(readyPointerKey).orElse(null);
    if (existing != null && target.canonicalPointerKey().equals(existing.getBlobUri())) {
      return;
    }
    var current = readCanonicalRecordByKey(target.canonicalPointerKey());
    if (current.isEmpty() || !readyPointerMatchesRecord(readyPointerKey, target, current.get())) {
      return;
    }
    repairReadyPointer(target.canonicalPointerKey(), current.get());
  }

  private void repairReadyPointersIfStillCurrent(String canonicalPointerKey) {
    var current = readCanonicalRecordByKey(canonicalPointerKey);
    current.ifPresent(
        record -> {
          // Lease conflict recovery is intentionally not treated as a hot mutation path, so it may
          // still repair inline unless this thread is already running under hot-path suppression.
          if (requiresReadyPointer(record)) {
            if (deferRepairIfHotPath(
                    "lease_conflict",
                    "ready_pointer_missing_after_lease_conflict",
                    "ready_pointer",
                    canonicalPointerKey,
                    record.jobId,
                    record.state)
                == RepairDisposition.DEFERRED) {
              return;
            }
            repairReadyPointer(
                canonicalPointerKey,
                record,
                "lease_conflict",
                "ready_pointer_missing_after_lease_conflict");
          }
        });
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
        () -> {
          long operationStartedAtMs = System.currentTimeMillis();
          var updated =
              mutateByJobIdReturningRecord(
                  jobId,
                  existing -> {
                    boolean allowCancelling = true;
                    String op =
                        switch (completionKind) {
                          case SUCCEEDED -> "markSucceeded";
                          case FAILED_RETRYABLE -> "markFailed";
                          case FAILED_WAITING -> "markWaiting";
                          case FAILED_TERMINAL -> "markFailedTerminal";
                          case CANCELLED -> "markCancelled";
                        };
                    boolean allowExpiredWithinGrace =
                        completionKind == CompletionKind.SUCCEEDED
                            || completionKind == CompletionKind.FAILED_TERMINAL
                            || completionKind == CompletionKind.CANCELLED;
                    if (!hasActiveLease(
                        jobId,
                        leaseEpoch,
                        existing,
                        op,
                        allowCancelling,
                        true,
                        allowExpiredWithinGrace)) {
                      return null;
                    }
                    if (isTerminalState(existing.state)) {
                      return null;
                    }
                    if ("JS_CANCELLING".equals(existing.state)
                        && completionKind != CompletionKind.CANCELLED) {
                      existing.state = "JS_CANCELLED";
                      existing.message =
                          blank(existing.message) ? "Cancelled" : blankToEmpty(existing.message);
                      if (existing.startedAtMs <= 0L) {
                        existing.startedAtMs = finishedAtMs;
                      }
                      existing.finishedAtMs = finishedAtMs;
                      existing.tablesScanned = tablesScanned;
                      existing.tablesChanged = tablesChanged;
                      existing.viewsScanned = viewsScanned;
                      existing.viewsChanged = viewsChanged;
                      existing.errors = errors;
                      existing.snapshotsProcessed = snapshotsProcessed;
                      existing.statsProcessed = statsProcessed;
                      existing.readyPointerKey = null;
                      return existing;
                    }
                    switch (completionKind) {
                      case SUCCEEDED -> {
                        existing.tablesScanned = tablesScanned;
                        existing.tablesChanged = tablesChanged;
                        existing.viewsScanned = viewsScanned;
                        existing.viewsChanged = viewsChanged;
                        existing.errors = 0L;
                        existing.snapshotsProcessed = snapshotsProcessed;
                        existing.statsProcessed = statsProcessed;
                        existing.lastError = "";
                        long directChildJobs =
                            countDirectChildJobs(existing.accountId, existing.jobId);
                        if (hasIncompleteDirectChildJobs(existing)
                            || (directChildJobs > 0L
                                && !directChildJobsComplete(
                                    maxExpectedChildJobs(existing, directChildJobs),
                                    existing.completedChildJobs,
                                    existing.failedChildJobs,
                                    existing.cancelledChildJobs))) {
                          if (existing.expectedChildJobs <= 0L) {
                            existing.expectedChildJobs = directChildJobs;
                          }
                          existing.state = "JS_WAITING";
                          existing.message = blank(message) ? "Waiting on child work" : message;
                          existing.finishedAtMs = 0L;
                          existing.executorId = "";
                        } else {
                          existing.state = "JS_SUCCEEDED";
                          existing.message = "Succeeded";
                          existing.finishedAtMs = finishedAtMs;
                        }
                        if (existing.startedAtMs <= 0L) {
                          existing.startedAtMs = finishedAtMs;
                        }
                        existing.readyPointerKey = null;
                        return existing;
                      }
                      case FAILED_RETRYABLE -> {
                        existing.attempt = Math.max(0, existing.attempt) + 1;
                        existing.tablesScanned = tablesScanned;
                        existing.tablesChanged = tablesChanged;
                        existing.viewsScanned = viewsScanned;
                        existing.viewsChanged = viewsChanged;
                        existing.errors = errors;
                        existing.snapshotsProcessed = snapshotsProcessed;
                        existing.statsProcessed = statsProcessed;
                        existing.lastError = message == null ? "Failed" : message;

                        if (existing.attempt >= maxAttempts) {
                          existing.state = "JS_FAILED";
                          existing.message = message == null ? "Failed" : message;
                          if (existing.startedAtMs <= 0L) {
                            existing.startedAtMs = finishedAtMs;
                          }
                          existing.finishedAtMs = finishedAtMs;
                          existing.readyPointerKey = null;
                          return existing;
                        }

                        long now = System.currentTimeMillis();
                        existing.state = "JS_QUEUED";
                        existing.message = message == null ? "Retrying" : message;
                        existing.executorId = "";
                        existing.nextAttemptAtMs = now + backoffMs(existing.attempt);
                        existing.finishedAtMs = 0L;
                        existing.readyPointerKey =
                            readyPointerKeyFor(existing, existing.nextAttemptAtMs);
                        return existing;
                      }
                      case FAILED_WAITING -> {
                        existing.tablesScanned = tablesScanned;
                        existing.tablesChanged = tablesChanged;
                        existing.viewsScanned = viewsScanned;
                        existing.viewsChanged = viewsChanged;
                        existing.errors = errors;
                        existing.snapshotsProcessed = snapshotsProcessed;
                        existing.statsProcessed = statsProcessed;
                        existing.lastError = message == null ? "Waiting on dependency" : message;
                        existing.state = "JS_QUEUED";
                        existing.message = message == null ? "Waiting on dependency" : message;
                        existing.executorId = "";
                        existing.nextAttemptAtMs = System.currentTimeMillis() + baseBackoffMs;
                        existing.finishedAtMs = 0L;
                        existing.readyPointerKey =
                            readyPointerKeyFor(existing, existing.nextAttemptAtMs);
                        return existing;
                      }
                      case FAILED_TERMINAL -> {
                        existing.attempt = Math.max(0, existing.attempt) + 1;
                        existing.tablesScanned = tablesScanned;
                        existing.tablesChanged = tablesChanged;
                        existing.viewsScanned = viewsScanned;
                        existing.viewsChanged = viewsChanged;
                        existing.errors = errors;
                        existing.snapshotsProcessed = snapshotsProcessed;
                        existing.statsProcessed = statsProcessed;
                        existing.lastError = message == null ? "Failed" : message;
                        existing.state = "JS_FAILED";
                        existing.message = message == null ? "Failed" : message;
                        if (existing.startedAtMs <= 0L) {
                          existing.startedAtMs = finishedAtMs;
                        }
                        existing.finishedAtMs = finishedAtMs;
                        existing.readyPointerKey = null;
                        return existing;
                      }
                      case CANCELLED -> {
                        existing.state = "JS_CANCELLED";
                        existing.message =
                            message == null || message.isBlank() ? "Cancelled" : message;
                        if (existing.startedAtMs <= 0L) {
                          existing.startedAtMs = finishedAtMs;
                        }
                        existing.finishedAtMs = finishedAtMs;
                        existing.tablesScanned = tablesScanned;
                        existing.tablesChanged = tablesChanged;
                        existing.viewsScanned = viewsScanned;
                        existing.viewsChanged = viewsChanged;
                        existing.errors = errors;
                        existing.snapshotsProcessed = snapshotsProcessed;
                        existing.statsProcessed = statsProcessed;
                        existing.readyPointerKey = null;
                        return existing;
                      }
                    }
                    return null;
                  });
          updated.ifPresent(
              env -> {
                long mutationElapsedMs = System.currentTimeMillis() - operationStartedAtMs;
                clearExecutionLeasesIfOwned(env, jobId, leaseEpoch);
                long refreshStartedAtMs = System.currentTimeMillis();
                refreshAncestorContributionRollups(
                    env.record,
                    isTerminalState(env.record.state)
                        || env.record.jobKind() == ReconcileJobKind.PLAN_SNAPSHOT);
                LOG.debugf(
                    "applyLeaseOutcome mutation_ms=%d contribution_refresh_ms=%d jobId=%s completionKind=%s",
                    mutationElapsedMs,
                    System.currentTimeMillis() - refreshStartedAtMs,
                    jobId,
                    completionKind);
              });
          return updated.isPresent();
        });
  }

  private long readyPointerDueAt(StoredReconcileJob record) {
    return record != null && record.nextAttemptAtMs > 0L
        ? record.nextAttemptAtMs
        : System.currentTimeMillis();
  }

  private long parseLeaseExpiryMillis(String leaseExpiryPointerKey) {
    return parseTimestampFromOrderedPointer(leaseExpiryPointerKey, LEASE_EXPIRY_POINTER_PREFIX);
  }

  private long parseDueMillis(String readyPointerKey) {
    return parseTimestampFromOrderedPointer(readyPointerKey, Keys.reconcileReadyPointerPrefix());
  }

  private long parseStatePointerMillis(String statePointerKey, String state) {
    return parseTimestampFromOrderedPointer(statePointerKey, statePointerPrefix(state));
  }

  private long parseTimestampFromOrderedPointer(String pointerKey, String prefix) {
    if (pointerKey == null || pointerKey.isBlank()) {
      return INVALID_ORDERED_POINTER_MS;
    }
    String normalizedKey = normalizePointerKey(pointerKey);
    String normalizedPrefix = normalizePointerKey(prefix);
    if (!normalizedKey.startsWith(normalizedPrefix)) {
      return INVALID_ORDERED_POINTER_MS;
    }
    int slash = normalizedKey.indexOf('/', normalizedPrefix.length());
    if (slash < 0) {
      return INVALID_ORDERED_POINTER_MS;
    }

    String token = normalizedKey.substring(normalizedPrefix.length(), slash);
    try {
      return Long.parseLong(token);
    } catch (NumberFormatException nfe) {
      return INVALID_ORDERED_POINTER_MS;
    }
  }

  private String statePointerPrefix(String state) {
    return Keys.reconcileJobByStatePointerPrefix(state);
  }

  private boolean hasStateIndex(StoredReconcileJob record) {
    return record != null
        && !blank(record.state)
        && !blank(record.accountId)
        && !blank(record.jobId);
  }

  private String statePointerKey(StoredReconcileJob record) {
    if (!hasStateIndex(record)) {
      return "";
    }
    return Keys.reconcileJobByStatePointer(
        record.state, Math.max(0L, record.createdAtMs), record.accountId, record.jobId);
  }

  private String accountStatePointerKey(StoredReconcileJob record) {
    if (!hasStateIndex(record)) {
      return "";
    }
    return Keys.reconcileJobByAccountStatePointer(
        record.accountId, record.state, Math.max(0L, record.createdAtMs), record.jobId);
  }

  private String connectorStatePointerKey(StoredReconcileJob record) {
    if (!hasStateIndex(record) || blank(record.connectorId)) {
      return "";
    }
    return Keys.reconcileJobByConnectorStatePointer(
        record.accountId,
        record.connectorId,
        record.state,
        Math.max(0L, record.createdAtMs),
        record.jobId);
  }

  private List<String> statePointerKeys(StoredReconcileJob record) {
    if (!hasStateIndex(record)) {
      return List.of();
    }
    List<String> keys = new ArrayList<>(3);
    String globalStateKey = statePointerKey(record);
    if (!globalStateKey.isBlank()) {
      keys.add(globalStateKey);
    }
    String accountStateKey = accountStatePointerKey(record);
    if (!accountStateKey.isBlank()) {
      keys.add(accountStateKey);
    }
    String connectorStateKey = connectorStatePointerKey(record);
    if (!connectorStateKey.isBlank()) {
      keys.add(connectorStateKey);
    }
    return keys;
  }

  private boolean hasValidStatePointer(StoredReconcileJob record, String canonicalPointerKey) {
    for (String pointerKey : statePointerKeys(record)) {
      Pointer existing = pointerStore.get(pointerKey).orElse(null);
      if (existing == null || !canonicalPointerKey.equals(existing.getBlobUri())) {
        return false;
      }
    }
    return true;
  }

  private boolean repairStatePointer(String canonicalPointerKey, StoredReconcileJob record) {
    return repairHandled(repairStatePointerDisposition(canonicalPointerKey, record, "", ""));
  }

  private boolean repairStatePointer(
      String canonicalPointerKey,
      StoredReconcileJob record,
      String repairPhase,
      String repairReason) {
    return repairHandled(
        repairStatePointerDisposition(canonicalPointerKey, record, repairPhase, repairReason));
  }

  private RepairDisposition repairStatePointerDisposition(
      String canonicalPointerKey,
      StoredReconcileJob record,
      String repairPhase,
      String repairReason) {
    if (canonicalPointerKey == null || canonicalPointerKey.isBlank() || record == null) {
      return RepairDisposition.FAILED;
    }
    List<String> pointerKeys = statePointerKeys(record);
    if (pointerKeys.isEmpty()) {
      return RepairDisposition.NOT_NEEDED;
    }
    RepairDisposition deferred =
        deferRepairIfHotPath(
            repairPhase,
            repairReason,
            "state_pointer",
            canonicalPointerKey,
            record.jobId,
            record.state);
    if (deferred == RepairDisposition.DEFERRED || deferred == RepairDisposition.FAILED) {
      return deferred;
    }
    boolean repaired = true;
    for (String pointerKey : pointerKeys) {
      repaired &= upsertReferencePointer(pointerKey, canonicalPointerKey);
    }
    logRepairEvent(
        repairPhase,
        repairReason,
        "state_pointer",
        canonicalPointerKey,
        record.jobId,
        record.state,
        repaired ? RepairDisposition.REPAIRED : RepairDisposition.FAILED);
    return repaired ? RepairDisposition.REPAIRED : RepairDisposition.FAILED;
  }

  private void logRepairEvent(
      String repairPhase,
      String repairReason,
      String repairTargetKind,
      String canonicalPointerKey,
      String jobId,
      String state,
      RepairDisposition disposition) {
    if (disposition != RepairDisposition.REPAIRED) {
      return;
    }
    LOG.infof(
        "Reconcile repair phase=%s reason=%s target=%s jobId=%s state=%s canonicalKey=%s",
        blankToEmpty(repairPhase),
        blankToEmpty(repairReason),
        blankToEmpty(repairTargetKind),
        blankToEmpty(jobId),
        blankToEmpty(state),
        blankToEmpty(canonicalPointerKey));
  }

  private boolean canRepairLookupPointer(String jobId, String canonicalPointerKey) {
    return !blank(jobId) && !blank(canonicalPointerKey);
  }

  private boolean canRepairReadyPointer(String canonicalPointerKey, StoredReconcileJob record) {
    return !blank(canonicalPointerKey) && record != null && requiresReadyPointer(record);
  }

  private boolean canRepairStatePointer(String canonicalPointerKey, StoredReconcileJob record) {
    return !blank(canonicalPointerKey) && record != null && !statePointerKeys(record).isEmpty();
  }

  private RepairDisposition mergeRepairDisposition(
      RepairDisposition left, RepairDisposition right) {
    if (left == RepairDisposition.FAILED || right == RepairDisposition.FAILED) {
      return RepairDisposition.FAILED;
    }
    if (left == RepairDisposition.DEFERRED || right == RepairDisposition.DEFERRED) {
      return RepairDisposition.DEFERRED;
    }
    if (left == RepairDisposition.REPAIRED || right == RepairDisposition.REPAIRED) {
      return RepairDisposition.REPAIRED;
    }
    return RepairDisposition.NOT_NEEDED;
  }

  private boolean repairHandled(RepairDisposition disposition) {
    // "Handled" only means the immediate caller does not need to warn or take compensating
    // action synchronously. It does not imply the pointer exists right now; DEFERRED is still
    // best-effort in-memory follow-up work.
    return disposition != RepairDisposition.FAILED;
  }

  private long oldestStateTimestamp(String state) {
    String prefix = statePointerPrefix(state);
    StringBuilder next = new StringBuilder();
    List<Pointer> pointers = pointerStore.listPointersByPrefix(prefix, 1, "", next);
    if (pointers.isEmpty()) {
      return 0L;
    }
    return Math.max(0L, parseStatePointerMillis(pointers.get(0).getKey(), state));
  }

  private boolean shouldSkipMalformedReadyPointer(
      String readyPointerKey, ReadyIndexSelection selection) {
    if (blank(readyPointerKey) || selection == null) {
      return false;
    }
    if (selection.type() != ReadyIndexType.GLOBAL) {
      return false;
    }
    return readyPointerKey.startsWith(Keys.reconcileReadyByExecutionClassPointerPrefix())
        || readyPointerKey.startsWith(Keys.reconcileReadyByExecutionLanePointerPrefix())
        || readyPointerKey.startsWith(Keys.reconcileReadyByPinnedExecutorPointerPrefix())
        || readyPointerKey.startsWith(Keys.reconcileReadyByJobKindPointerPrefix());
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

  private List<CasOp> leasePointerBatchOps(
      String leasePointerKey,
      Pointer currentLeasePointer,
      StoredJobLease previousLease,
      StoredJobLease nextLease) {
    String canonicalPointerKey =
        Keys.reconcileJobStateRowById(previousLease.accountId, previousLease.jobId);
    List<CasOp> ops = new ArrayList<>(3);
    long leaseExpectedVersion = currentLeasePointer == null ? 0L : currentLeasePointer.getVersion();
    Pointer nextLeasePointer =
        Pointer.newBuilder()
            .setKey(leasePointerKey)
            .setBlobUri(encodeInlineJobLease(nextLease))
            .setVersion(leaseExpectedVersion + 1L)
            .build();
    ops.add(new CasUpsert(leasePointerKey, leaseExpectedVersion, nextLeasePointer));

    String previousExpiryKey = leaseExpiryPointerKey(previousLease);
    String nextExpiryKey = leaseExpiryPointerKey(nextLease);

    if (!nextExpiryKey.isBlank()) {
      Pointer currentExpiryPointer = pointerStore.get(nextExpiryKey).orElse(null);
      long expiryExpectedVersion =
          currentExpiryPointer == null ? 0L : currentExpiryPointer.getVersion();
      Pointer nextExpiryPointer =
          Pointer.newBuilder()
              .setKey(nextExpiryKey)
              .setBlobUri(canonicalPointerKey)
              .setVersion(expiryExpectedVersion + 1L)
              .build();
      ops.add(new CasUpsert(nextExpiryKey, expiryExpectedVersion, nextExpiryPointer));
    }

    if (!previousExpiryKey.isBlank() && !previousExpiryKey.equals(nextExpiryKey)) {
      Pointer previousExpiryPointer = pointerStore.get(previousExpiryKey).orElse(null);
      if (previousExpiryPointer != null
          && canonicalPointerKey.equals(previousExpiryPointer.getBlobUri())) {
        ops.add(new CasDelete(previousExpiryKey, previousExpiryPointer.getVersion()));
      }
    }

    return List.copyOf(ops);
  }

  private String leaseExpiryPointerKey(StoredJobLease lease) {
    if (lease == null) {
      return "";
    }
    return leaseExpiryPointerKey(lease.expiresAtMs, lease.accountId, lease.jobId);
  }

  private String leaseExpiryPointerKey(long expiresAtMs, String accountId, String jobId) {
    if (expiresAtMs <= 0L || blank(accountId) || blank(jobId)) {
      return "";
    }
    return String.format(
        "%s%019d/%s/%s",
        LEASE_EXPIRY_POINTER_PREFIX,
        expiresAtMs,
        Keys.encodeSegment(accountId),
        Keys.encodeSegment(jobId));
  }

  private boolean replaceLeaseExpiryPointer(
      Pointer stalePointer, String expectedPointerKey, String canonicalPointerKey) {
    if (stalePointer == null
        || blank(stalePointer.getKey())
        || blank(expectedPointerKey)
        || blank(canonicalPointerKey)) {
      return false;
    }
    Pointer currentExpected = pointerStore.get(expectedPointerKey).orElse(null);
    List<CasOp> ops = new ArrayList<>(2);
    if (currentExpected == null || !canonicalPointerKey.equals(currentExpected.getBlobUri())) {
      long expectedVersion = currentExpected == null ? 0L : currentExpected.getVersion();
      ops.add(
          new CasUpsert(
              expectedPointerKey,
              expectedVersion,
              Pointer.newBuilder()
                  .setKey(expectedPointerKey)
                  .setBlobUri(canonicalPointerKey)
                  .setVersion(expectedVersion + 1L)
                  .build()));
    }
    ops.add(new CasDelete(stalePointer.getKey(), stalePointer.getVersion()));
    return pointerStore.compareAndSetBatch(ops);
  }

  private boolean hasActiveSnapshotLease(StoredReconcileJob record, long now) {
    return record != null
        && record.jobKind() == ReconcileJobKind.PLAN_SNAPSHOT
        && record.snapshotTaskTableId != null
        && !record.snapshotTaskTableId.isBlank()
        && record.snapshotTaskSnapshotId >= 0L
        && hasUnexpiredJobLease(record.accountId, record.jobId, now);
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
        record.snapshotTaskTableId, record.snapshotTaskSnapshotId);
  }

  private boolean hasActiveLease(
      String jobId,
      String leaseEpoch,
      StoredReconcileJob existing,
      String op,
      boolean allowCancelling,
      boolean requireUnexpiredLease,
      boolean allowExpiredWithinGrace) {
    if (leaseEpoch == null || leaseEpoch.isBlank()) {
      logLeaseSkip(op, "Skipping %s for reconcile job %s due to missing lease epoch", op, jobId);
      return false;
    }
    boolean stateAllowed =
        "JS_RUNNING".equals(existing.state)
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
    StoredJobLease lease = loadLease(existing).orElse(null);
    if (lease == null) {
      logLeaseSkip(op, "Skipping %s for reconcile job %s due to missing lease", op, jobId);
      return false;
    }
    long now = System.currentTimeMillis();
    if (!leaseEpoch.equals(lease.epoch)) {
      logLeaseSkip(
          op,
          "Skipping %s for reconcile job %s due to stale lease epoch=%s",
          op,
          jobId,
          lease.epoch);
      return false;
    }
    if (requireUnexpiredLease && lease.expiresAtMs <= now) {
      if (!allowExpiredWithinGrace || now - lease.expiresAtMs > leaseRenewGraceMs) {
        logLeaseSkip(
            op,
            "Skipping %s for reconcile job %s due to expired lease expiresAtMs=%d now=%d graceMs=%d",
            op,
            jobId,
            lease.expiresAtMs,
            now,
            leaseRenewGraceMs);
        return false;
      }
    }
    return true;
  }

  private boolean hasLiveLease(StoredReconcileJob record, boolean allowCancelling, long now) {
    if (record == null) {
      return false;
    }
    boolean stateAllowed =
        "JS_RUNNING".equals(record.state)
            || (allowCancelling && "JS_CANCELLING".equals(record.state));
    if (!stateAllowed) {
      return false;
    }
    StoredJobLease lease = loadLease(record).orElse(null);
    return lease != null
        && lease.epoch != null
        && !lease.epoch.isBlank()
        && lease.expiresAtMs > now;
  }

  private boolean hasUnexpiredJobLease(String accountId, String jobId, long now) {
    if (blank(accountId) || blank(jobId)) {
      return false;
    }
    StoredJobLease lease = loadLease(accountId, jobId).orElse(null);
    return lease != null
        && lease.epoch != null
        && !lease.epoch.isBlank()
        && lease.expiresAtMs > now;
  }

  private StoredReconcileJob cloneStoredRecord(StoredReconcileJob source) {
    if (source == null) {
      return null;
    }
    StoredReconcileJob copy = new StoredReconcileJob();
    copy.jobId = source.jobId;
    copy.accountId = source.accountId;
    copy.connectorId = source.connectorId;
    copy.jobKind = source.jobKind;
    copy.parentJobId = source.parentJobId;
    copy.fullRescan = source.fullRescan;
    copy.captureMode = source.captureMode;
    copy.executionClass = source.executionClass;
    copy.executionLane = source.executionLane;
    copy.executionAttributes =
        source.executionAttributes == null
            ? java.util.Map.of()
            : java.util.Map.copyOf(source.executionAttributes);
    copy.pinnedExecutorId = source.pinnedExecutorId;
    copy.executorId = source.executorId;
    copy.snapshotTaskTableId = source.snapshotTaskTableId;
    copy.snapshotTaskSnapshotId = source.snapshotTaskSnapshotId;
    copy.snapshotTaskSourceNamespace = source.snapshotTaskSourceNamespace;
    copy.snapshotTaskSourceTable = source.snapshotTaskSourceTable;
    copy.snapshotTaskFileGroupPlanRecorded = source.snapshotTaskFileGroupPlanRecorded;
    copy.snapshotTaskCompletionMode = source.snapshotTaskCompletionMode;
    copy.snapshotTaskDirectStatsBlobUri = source.snapshotTaskDirectStatsBlobUri;
    copy.snapshotTaskDirectStatsRecordCount = source.snapshotTaskDirectStatsRecordCount;
    copy.fileGroupPlanId = source.fileGroupPlanId;
    copy.fileGroupGroupId = source.fileGroupGroupId;
    copy.fileGroupTableId = source.fileGroupTableId;
    copy.fileGroupSnapshotId = source.fileGroupSnapshotId;
    copy.fileGroupFileCount = source.fileGroupFileCount;
    copy.definitionBlobUri = source.definitionBlobUri;
    copy.snapshotPlanBlobUri = source.snapshotPlanBlobUri;
    copy.fileGroupPlanBlobUri = source.fileGroupPlanBlobUri;
    copy.state = source.state;
    copy.message = source.message;
    copy.startedAtMs = source.startedAtMs;
    copy.finishedAtMs = source.finishedAtMs;
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
    copy.expectedChildJobs = source.expectedChildJobs;
    copy.completedChildJobs = source.completedChildJobs;
    copy.failedChildJobs = source.failedChildJobs;
    copy.cancelledChildJobs = source.cancelledChildJobs;
    copy.attempt = source.attempt;
    copy.nextAttemptAtMs = source.nextAttemptAtMs;
    copy.lastError = source.lastError;
    copy.laneKey = source.laneKey;
    copy.dedupeKeyHash = source.dedupeKeyHash;
    copy.readyPointerKey = source.readyPointerKey;
    copy.connectorIndexPointerKey = source.connectorIndexPointerKey;
    copy.canonicalPointerKey = source.canonicalPointerKey;
    copy.createdAtMs = source.createdAtMs;
    copy.updatedAtMs = source.updatedAtMs;
    return copy;
  }

  private StoredJobLease cloneLease(StoredJobLease source) {
    if (source == null) {
      return null;
    }
    StoredJobLease copy = new StoredJobLease();
    copy.accountId = source.accountId;
    copy.jobId = source.jobId;
    copy.epoch = source.epoch;
    copy.expiresAtMs = source.expiresAtMs;
    return copy;
  }

  private boolean leaseStateEquals(StoredJobLease left, StoredJobLease right) {
    if (left == right) {
      return true;
    }
    if (left == null || right == null) {
      return false;
    }
    return java.util.Objects.equals(left.accountId, right.accountId)
        && java.util.Objects.equals(left.jobId, right.jobId)
        && java.util.Objects.equals(left.epoch, right.epoch)
        && left.expiresAtMs == right.expiresAtMs;
  }

  private <T> Optional<T> readBlob(String blobUri, Class<T> type) {
    if (blobUri == null || blobUri.isBlank()) {
      return Optional.empty();
    }
    byte[] payload;
    try {
      payload = blobStore.get(blobUri);
    } catch (StorageNotFoundException e) {
      LOG.warnf(e, "Reconcile payload blob missing blob=%s", blobUri);
      return Optional.empty();
    }
    if (payload == null || payload.length == 0) {
      return Optional.empty();
    }
    try {
      return Optional.ofNullable(mapper.readValue(payload, type));
    } catch (Exception e) {
      LOG.warnf(e, "Failed to decode reconcile payload blob=%s type=%s", blobUri, type.getName());
      return Optional.empty();
    }
  }

  private String encodeInlineJobState(StoredReconcileJob payload) {
    return encodeInlineJson(INLINE_JOB_STATE_PREFIX, payload);
  }

  private Optional<StoredReconcileJob> readInlineJobState(String reference) {
    return decodeInlineJson(reference, INLINE_JOB_STATE_PREFIX, StoredReconcileJob.class);
  }

  private String encodeInlineJobLease(StoredJobLease payload) {
    return encodeInlineJson(INLINE_JOB_LEASE_PREFIX, payload);
  }

  private Optional<StoredJobLease> readInlineJobLease(String reference) {
    return decodeInlineJson(reference, INLINE_JOB_LEASE_PREFIX, StoredJobLease.class);
  }

  private String encodeInlineJobContribution(StoredJobContribution payload) {
    return encodeInlineJson(INLINE_JOB_CONTRIBUTION_PREFIX, payload);
  }

  private Optional<StoredJobContribution> readInlineJobContribution(String reference) {
    return decodeInlineJson(reference, INLINE_JOB_CONTRIBUTION_PREFIX, StoredJobContribution.class);
  }

  private <T> String encodeInlineJson(String prefix, T payload) {
    try {
      return prefix
          + Base64.getUrlEncoder()
              .withoutPadding()
              .encodeToString(mapper.writeValueAsBytes(payload));
    } catch (Exception e) {
      throw new IllegalStateException("Failed to encode inline reconcile payload", e);
    }
  }

  private <T> Optional<T> decodeInlineJson(String reference, String prefix, Class<T> type) {
    if (reference == null
        || reference.isBlank()
        || prefix == null
        || !reference.startsWith(prefix)) {
      return Optional.empty();
    }
    try {
      byte[] payload = Base64.getUrlDecoder().decode(reference.substring(prefix.length()));
      return Optional.ofNullable(mapper.readValue(payload, type));
    } catch (Exception e) {
      LOG.warnf(e, "Failed to decode inline reconcile payload type=%s", type.getName());
      return Optional.empty();
    }
  }

  private String writeBlob(String blobUri, Object payload, String failureMessage) {
    try {
      blobStore.put(
          blobUri,
          mapper.writeValueAsBytes(payload),
          "application/json; charset=" + StandardCharsets.UTF_8.name());
      return blobUri;
    } catch (Exception e) {
      throw new IllegalStateException(failureMessage, e);
    }
  }

  private Optional<StoredJobDefinition> loadDefinition(StoredReconcileJob state) {
    if (state == null) {
      return Optional.empty();
    }
    return readBlob(state.definitionBlobUri, StoredJobDefinition.class);
  }

  private StoredJobDefinition requireDefinition(StoredReconcileJob state) {
    if (state == null) {
      throw new IllegalStateException("Reconcile job definition missing for null job state");
    }
    return requireBlob(
        state.definitionBlobUri, StoredJobDefinition.class, "job definition", state.jobId);
  }

  private Optional<StoredJobLease> loadLease(String accountId, String jobId) {
    if (accountId == null || accountId.isBlank() || jobId == null || jobId.isBlank()) {
      return Optional.empty();
    }
    String pointerKey = Keys.reconcileJobLeasePointerById(accountId, jobId);
    Pointer leasePointer = pointerStore.get(pointerKey).orElse(null);
    if (leasePointer == null) {
      return Optional.empty();
    }
    return readInlineJobLease(leasePointer.getBlobUri());
  }

  private Optional<StoredJobLease> loadLease(StoredReconcileJob state) {
    if (state == null) {
      return Optional.empty();
    }
    return loadLease(state.accountId, state.jobId);
  }

  private String writeFileGroupResultPayload(
      String accountId, String jobId, ReconcileFileGroupTask fileGroupTask) {
    String pointerKey = Keys.reconcileJobResultPointerById(accountId, jobId);
    String nextBlobUri =
        Keys.reconcileJobResultBlobUri(
            accountId,
            jobId,
            "file-group-result-" + System.currentTimeMillis() + "-" + UUID.randomUUID());
    ReconcileFileGroupTask effective =
        fileGroupTask == null ? ReconcileFileGroupTask.empty() : fileGroupTask;
    writeBlob(
        nextBlobUri,
        StoredFileGroupResultPayload.of(effective),
        "Failed to persist file group result payload");

    for (int i = 0; i < CAS_MAX; i++) {
      Pointer currentPointer = pointerStore.get(pointerKey).orElse(null);
      if (currentPointer == null) {
        Pointer created =
            Pointer.newBuilder().setKey(pointerKey).setBlobUri(nextBlobUri).setVersion(1L).build();
        if (pointerStore.compareAndSet(pointerKey, 0L, created)) {
          return nextBlobUri;
        }
        continue;
      }
      if (nextBlobUri.equals(currentPointer.getBlobUri())) {
        return nextBlobUri;
      }
      Pointer nextPointer =
          Pointer.newBuilder()
              .setKey(pointerKey)
              .setBlobUri(nextBlobUri)
              .setVersion(currentPointer.getVersion() + 1L)
              .build();
      if (pointerStore.compareAndSet(pointerKey, currentPointer.getVersion(), nextPointer)) {
        return nextBlobUri;
      }
    }

    if (!nextBlobUri.isBlank()) {
      blobStore.delete(nextBlobUri);
    }
    throw new IllegalStateException("Failed to update reconcile job result pointer");
  }

  private Optional<StoredJobLease> writeLease(
      String accountId, String jobId, UnaryOperator<StoredJobLease> mutator) {
    if (accountId == null || accountId.isBlank() || jobId == null || jobId.isBlank()) {
      return Optional.empty();
    }
    String pointerKey = Keys.reconcileJobLeasePointerById(accountId, jobId);
    for (int i = 0; i < CAS_MAX; i++) {
      Pointer currentPointer = pointerStore.get(pointerKey).orElse(null);
      StoredJobLease current =
          currentPointer == null
              ? StoredJobLease.empty(accountId, jobId)
              : readInlineJobLease(currentPointer.getBlobUri())
                  .orElse(StoredJobLease.empty(accountId, jobId));
      StoredJobLease next = mutator.apply(cloneLease(current));
      if (next == null) {
        return Optional.empty();
      }
      if (leaseStateEquals(current, next)) {
        return Optional.of(current);
      }
      if (pointerStore.compareAndSetBatch(
          leasePointerBatchOps(pointerKey, currentPointer, current, next))) {
        return Optional.of(next);
      }
    }
    return Optional.empty();
  }

  private Optional<StoredJobLease> writeLeaseIfAbsentOrExpired(
      String accountId, String jobId, String epoch, long expiresAtMs) {
    return writeLease(
        accountId,
        jobId,
        current -> {
          long now = System.currentTimeMillis();
          if (current.epoch != null && !current.epoch.isBlank() && current.expiresAtMs > now) {
            return null;
          }
          return StoredJobLease.active(accountId, jobId, epoch, expiresAtMs);
        });
  }

  private Optional<StoredJobLease> renewLeaseIfEpochMatches(
      String accountId, String jobId, String leaseEpoch) {
    return writeLease(
        accountId,
        jobId,
        current -> {
          if (leaseEpoch == null || leaseEpoch.isBlank()) {
            return null;
          }
          long now = System.currentTimeMillis();
          long expiry = current.expiresAtMs;
          if (current.epoch == null
              || current.epoch.isBlank()
              || !leaseEpoch.equals(current.epoch)) {
            return null;
          }
          if (expiry > 0L && (expiry - now) > (leaseMs / 2L)) {
            return current;
          }
          if (expiry > 0L && now - expiry > leaseRenewGraceMs) {
            return null;
          }
          current.expiresAtMs = now + leaseMs;
          return current;
        });
  }

  private boolean clearLeaseIfEpochMatches(String accountId, String jobId, String leaseEpoch) {
    if (accountId == null || accountId.isBlank() || jobId == null || jobId.isBlank()) {
      return false;
    }
    String pointerKey = Keys.reconcileJobLeasePointerById(accountId, jobId);
    for (int i = 0; i < CAS_MAX; i++) {
      Pointer currentPointer = pointerStore.get(pointerKey).orElse(null);
      if (currentPointer == null) {
        return false;
      }
      StoredJobLease current =
          readInlineJobLease(currentPointer.getBlobUri())
              .orElse(StoredJobLease.empty(accountId, jobId));
      if (leaseEpoch == null || leaseEpoch.isBlank() || !leaseEpoch.equals(current.epoch)) {
        return false;
      }
      List<CasOp> ops = new ArrayList<>(2);
      ops.add(new CasDelete(pointerKey, currentPointer.getVersion()));
      String expiryKey = leaseExpiryPointerKey(current.expiresAtMs, accountId, jobId);
      if (!expiryKey.isBlank()) {
        Pointer expiryPointer = pointerStore.get(expiryKey).orElse(null);
        String canonicalPointerKey = Keys.reconcileJobStateRowById(accountId, jobId);
        if (expiryPointer != null && canonicalPointerKey.equals(expiryPointer.getBlobUri())) {
          ops.add(new CasDelete(expiryKey, expiryPointer.getVersion()));
        }
      }
      if (pointerStore.compareAndSetBatch(ops)) {
        return true;
      }
    }
    return false;
  }

  private List<ReconcileFileGroupTask> loadSnapshotFileGroups(StoredReconcileJob state) {
    if (state == null) {
      return List.of();
    }
    if (blank(state.snapshotPlanBlobUri)) {
      return List.of();
    }
    return requireBlob(
            state.snapshotPlanBlobUri,
            StoredSnapshotPlanPayload.class,
            "snapshot plan payload",
            state.jobId)
        .fileGroups();
  }

  private List<String> loadFileGroupPaths(StoredReconcileJob state) {
    if (state == null) {
      return List.of();
    }
    return readBlob(state.fileGroupPlanBlobUri, StoredFileGroupPlanPayload.class)
        .map(StoredFileGroupPlanPayload::filePaths)
        .orElse(List.of());
  }

  private Optional<StoredFileGroupPlanPayload> loadFileGroupPlanPayload(StoredReconcileJob state) {
    if (state == null || blank(state.fileGroupPlanBlobUri)) {
      return Optional.empty();
    }
    return Optional.of(
        requireBlob(
            state.fileGroupPlanBlobUri,
            StoredFileGroupPlanPayload.class,
            "file-group plan payload",
            state.jobId));
  }

  private List<ReconcileFileResult> loadFileGroupResults(StoredReconcileJob state) {
    if (state == null) {
      return List.of();
    }
    return loadFileGroupResultPayload(state)
        .map(StoredFileGroupResultPayload::fileResults)
        .orElse(List.of());
  }

  private Optional<StoredFileGroupResultPayload> loadFileGroupResultPayload(
      StoredReconcileJob state) {
    if (state == null) {
      return Optional.empty();
    }
    if (!blank(state.accountId) && !blank(state.jobId)) {
      Pointer resultPointer =
          pointerStore
              .get(Keys.reconcileJobResultPointerById(state.accountId, state.jobId))
              .orElse(null);
      if (resultPointer != null && !blank(resultPointer.getBlobUri())) {
        return Optional.of(
            requireBlob(
                resultPointer.getBlobUri(),
                StoredFileGroupResultPayload.class,
                "file-group result payload",
                state.jobId));
      }
    }
    return Optional.empty();
  }

  private ReconcileSnapshotTask snapshotTaskFor(StoredReconcileJob state) {
    if (state == null) {
      return ReconcileSnapshotTask.empty();
    }
    List<ReconcileFileGroupTask> fileGroups = loadSnapshotFileGroups(state);
    return ReconcileSnapshotTask.of(
        state.snapshotTaskTableId,
        state.snapshotTaskSnapshotId,
        state.snapshotTaskSourceNamespace,
        state.snapshotTaskSourceTable,
        fileGroups,
        state.snapshotTaskFileGroupPlanRecorded,
        ReconcileSnapshotTask.CompletionMode.fromString(state.snapshotTaskCompletionMode),
        blankToEmpty(state.snapshotPlanBlobUri),
        fileGroups.size(),
        blankToEmpty(state.snapshotTaskDirectStatsBlobUri),
        state.snapshotTaskDirectStatsRecordCount);
  }

  private ReconcileFileGroupTask fileGroupTaskFor(StoredReconcileJob state) {
    if (state == null) {
      return ReconcileFileGroupTask.empty();
    }
    StoredFileGroupResultPayload resultPayload = loadFileGroupResultPayload(state).orElse(null);
    return ReconcileFileGroupTask.of(
        state.fileGroupPlanId,
        state.fileGroupGroupId,
        state.fileGroupTableId,
        state.fileGroupSnapshotId,
        state.fileGroupFileCount,
        resultPayload == null ? loadFileGroupPaths(state) : resultPayload.filePaths(),
        resultPayload == null ? loadFileGroupResults(state) : resultPayload.fileResults());
  }

  private JobProjection projectJob(
      StoredReconcileJob stored,
      ReconcileSnapshotTask snapshotTask,
      ReconcileFileGroupTask fileGroupTask) {
    if (stored == null) {
      return JobProjection.empty();
    }
    if (stored.jobKind() == ReconcileJobKind.EXEC_FILE_GROUP) {
      return projectExecFileGroup(fileGroupTask, stored.state);
    }
    if (stored.jobKind() == ReconcileJobKind.PLAN_SNAPSHOT) {
      return projectSnapshotPlan(snapshotTask);
    }
    return JobProjection.empty();
  }

  private JobProjection inlineSummaryProjection(StoredReconcileJob stored) {
    if (stored == null) {
      return JobProjection.empty();
    }
    if (isParentCapable(stored.jobKind())) {
      return new JobProjection(
          stored.indexesProcessed,
          stored.plannedFileGroups,
          stored.plannedFiles,
          stored.completedFileGroups,
          stored.failedFileGroups,
          stored.completedFiles,
          stored.failedFiles);
    }
    if (stored.jobKind() == ReconcileJobKind.EXEC_FILE_GROUP) {
      long plannedFiles = Math.max(0L, stored.fileGroupFileCount);
      long completedFileGroups = "JS_SUCCEEDED".equals(stored.state) ? 1L : 0L;
      long failedFileGroups =
          ("JS_FAILED".equals(stored.state) || "JS_CANCELLED".equals(stored.state)) ? 1L : 0L;
      long completedFiles = completedFileGroups > 0L ? plannedFiles : 0L;
      long failedFiles = failedFileGroups > 0L ? plannedFiles : 0L;
      return new JobProjection(
          0L, 1L, plannedFiles, completedFileGroups, failedFileGroups, completedFiles, failedFiles);
    }
    return JobProjection.empty();
  }

  private ProjectedPublicJob projectPublicJob(
      StoredReconcileJob stored, boolean includeSelfProjectionPayloads) {
    return projectPublicJob(stored, includeSelfProjectionPayloads, false);
  }

  private ProjectedPublicJob projectPublicJob(
      StoredReconcileJob stored,
      boolean includeSelfProjectionPayloads,
      boolean allowContributionRepair) {
    if (stored == null) {
      return ProjectedPublicJob.empty();
    }
    if (isParentCapable(stored.jobKind())
        && isTerminalState(stored.state)
        && stored.finishedAtMs > 0L) {
      return ProjectedPublicJob.self(stored, inlineSummaryProjection(stored));
    }
    ReconcileSnapshotTask snapshotTask =
        includeSelfProjectionPayloads && stored.jobKind() == ReconcileJobKind.PLAN_SNAPSHOT
            ? snapshotTaskFor(stored)
            : ReconcileSnapshotTask.empty();
    ReconcileFileGroupTask fileGroupTask =
        includeSelfProjectionPayloads && stored.jobKind() == ReconcileJobKind.EXEC_FILE_GROUP
            ? fileGroupTaskFor(stored)
            : ReconcileFileGroupTask.empty();
    JobProjection selfProjection =
        isParentCapable(stored.jobKind())
            ? inlineSummaryProjection(stored)
            : projectJob(stored, snapshotTask, fileGroupTask);
    if (!isParentCapable(stored.jobKind())) {
      return ProjectedPublicJob.self(stored, selfProjection);
    }

    List<StoredJobContribution> contributions =
        allowContributionRepair
            ? loadDirectContributionsWithRepair(stored.accountId, stored.jobId)
            : loadDirectContributionsNoRepair(stored.accountId, stored.jobId);
    if (contributions.isEmpty()) {
      return ProjectedPublicJob.self(stored, selfProjection);
    }

    return projectParentPublicJob(stored, selfProjection, contributions);
  }

  private ProjectedPublicJob projectParentPublicJob(
      StoredReconcileJob stored,
      JobProjection selfProjection,
      List<StoredJobContribution> contributions) {

    long tablesScanned = Math.max(0L, stored.tablesScanned);
    long tablesChanged = 0L;
    long viewsScanned = Math.max(0L, stored.viewsScanned);
    long viewsChanged = 0L;
    long errors = 0L;
    long snapshotsProcessed = 0L;
    long statsProcessed = 0L;
    long indexesProcessed = 0L;
    long plannedFileGroups = selfProjection.plannedFileGroups;
    long plannedFiles = selfProjection.plannedFiles;
    long completedFileGroups = selfProjection.completedFileGroups;
    long failedFileGroups = selfProjection.failedFileGroups;
    long completedFiles = selfProjection.completedFiles;
    long failedFiles = selfProjection.failedFiles;

    long contributionPlannedFileGroups = 0L;
    long contributionPlannedFiles = 0L;
    long contributionCompletedFileGroups = 0L;
    long contributionFailedFileGroups = 0L;
    long contributionCompletedFiles = 0L;
    long contributionFailedFiles = 0L;

    long startedAtMs =
        firstNonZeroMin(
            stored.startedAtMs, contributions, contribution -> contribution.startedAtMs);
    String state = stored.state;
    String message = blankToEmpty(stored.message);
    String executorId = blankToEmpty(stored.executorId);

    StoredJobContribution cancellingChild = null;
    StoredJobContribution runningChild = null;
    StoredJobContribution failedChild = null;
    StoredJobContribution cancelledChild = null;
    StoredJobContribution waitingChild = null;
    StoredJobContribution queuedChild = null;
    DirectChildCounts directChildCounts = DirectChildCounts.empty();
    boolean allSucceeded = true;

    for (StoredJobContribution contribution : contributions) {
      tablesScanned += contribution.tablesScanned;
      tablesChanged += contribution.tablesChanged;
      viewsScanned += contribution.viewsScanned;
      viewsChanged += contribution.viewsChanged;
      errors += contribution.errors;
      snapshotsProcessed += contribution.snapshotsProcessed;
      statsProcessed += contribution.statsProcessed;
      indexesProcessed += contribution.indexesProcessed;
      contributionPlannedFileGroups += contribution.plannedFileGroups;
      contributionPlannedFiles += contribution.plannedFiles;
      contributionCompletedFileGroups += contribution.completedFileGroups;
      contributionFailedFileGroups += contribution.failedFileGroups;
      contributionCompletedFiles += contribution.completedFiles;
      contributionFailedFiles += contribution.failedFiles;

      String childState = blankToEmpty(contribution.state);
      directChildCounts = directChildCounts.incrementedBy(childState);
      if (!"JS_SUCCEEDED".equals(childState)) {
        allSucceeded = false;
      }
      if ("JS_CANCELLING".equals(childState) && cancellingChild == null) {
        cancellingChild = contribution;
      } else if ("JS_RUNNING".equals(childState) && runningChild == null) {
        runningChild = contribution;
      } else if ("JS_FAILED".equals(childState) && failedChild == null) {
        failedChild = contribution;
      } else if ("JS_CANCELLED".equals(childState) && cancelledChild == null) {
        cancelledChild = contribution;
      } else if ("JS_WAITING".equals(childState) && waitingChild == null) {
        waitingChild = contribution;
      } else if ("JS_QUEUED".equals(childState) && queuedChild == null) {
        queuedChild = contribution;
      }
    }

    if (stored.jobKind() == ReconcileJobKind.PLAN_TABLE) {
      tablesScanned =
          contributions.stream().anyMatch(DurableReconcileJobStore::planTableDescendantScanned)
              ? 1L
              : 0L;
      tablesChanged =
          contributions.stream().anyMatch(DurableReconcileJobStore::planTableDescendantChanged)
              ? 1L
              : 0L;
    } else if (stored.jobKind() == ReconcileJobKind.PLAN_CONNECTOR) {
      tablesScanned = Math.max(0L, stored.tablesScanned);
      viewsScanned = Math.max(0L, stored.viewsScanned);
    }

    if ("JS_FAILED".equals(stored.state)) {
      state = "JS_FAILED";
      message = blankToEmpty(stored.message);
    } else if ("JS_CANCELLED".equals(stored.state)) {
      state = "JS_CANCELLED";
      message = blankToEmpty(stored.message);
    } else if ("JS_WAITING".equals(stored.state)
        && !directChildJobsComplete(stored, directChildCounts)) {
      state = "JS_WAITING";
      message = firstNonBlank(stored.message, "Waiting on child work");
      executorId = "";
    } else if (cancellingChild != null) {
      state = "JS_CANCELLING";
      message = firstNonBlank(cancellingChild.message, message, "Cancelling");
      executorId = firstNonBlank(cancellingChild.executorId, executorId);
    } else if (runningChild != null) {
      state = "JS_RUNNING";
      message = firstNonBlank(runningChild.message, message, "Running");
      executorId = firstNonBlank(runningChild.executorId, executorId);
    } else if (failedChild != null) {
      state = "JS_FAILED";
      message = firstNonBlank(failedChild.message, message, "Failed");
      executorId = firstNonBlank(failedChild.executorId, executorId);
    } else if (cancelledChild != null) {
      state = "JS_CANCELLED";
      message = firstNonBlank(cancelledChild.message, message, "Cancelled");
      executorId = firstNonBlank(cancelledChild.executorId, executorId);
    } else if (waitingChild != null) {
      state = "JS_WAITING";
      message = firstNonBlank(waitingChild.message, message, "Waiting on child work");
      executorId = "";
    } else if (queuedChild != null
        && ("JS_WAITING".equals(stored.state) || "JS_SUCCEEDED".equals(stored.state))) {
      state = "JS_WAITING";
      message = "Waiting on child work";
      executorId = "";
    } else if (allSucceeded && directChildJobsComplete(stored, directChildCounts)) {
      state = "JS_SUCCEEDED";
      message = normalizeSucceededMessage(message);
    } else if (allSucceeded && hasExpectedDirectChildJobs(stored)) {
      state = "JS_WAITING";
      message = "Waiting on child work";
      executorId = "";
    } else if ("JS_RUNNING".equals(state)
        && !hasLiveLease(stored, true, System.currentTimeMillis())
        && hasExpectedDirectChildJobs(stored)) {
      state = "JS_WAITING";
      message = "Waiting on child work";
      executorId = "";
    }

    long finishedAtMs =
        isTerminalState(state) ? maxTerminalFinishedAtMs(stored, contributions, state) : 0L;
    if (isTerminalState(stored.state) && !blank(stored.executorId)) {
      executorId = stored.executorId;
    }
    plannedFileGroups = Math.max(plannedFileGroups, contributionPlannedFileGroups);
    plannedFiles = Math.max(plannedFiles, contributionPlannedFiles);
    if (contributionCompletedFileGroups > 0L) {
      completedFileGroups = contributionCompletedFileGroups;
    }
    if (contributionFailedFileGroups > 0L) {
      failedFileGroups = contributionFailedFileGroups;
    }
    if (contributionCompletedFiles > 0L) {
      completedFiles = contributionCompletedFiles;
    }
    if (contributionFailedFiles > 0L) {
      failedFiles = contributionFailedFiles;
    }

    return new ProjectedPublicJob(
        state,
        firstNonBlank(message, stored.message),
        startedAtMs,
        finishedAtMs,
        tablesScanned,
        tablesChanged,
        viewsScanned,
        viewsChanged,
        errors,
        snapshotsProcessed,
        statsProcessed,
        firstNonBlank(executorId, stored.executorId),
        new JobProjection(
            indexesProcessed,
            plannedFileGroups,
            plannedFiles,
            completedFileGroups,
            failedFileGroups,
            completedFiles,
            failedFiles));
  }

  private boolean isParentCapable(ReconcileJobKind jobKind) {
    return jobKind == ReconcileJobKind.PLAN_CONNECTOR
        || jobKind == ReconcileJobKind.PLAN_TABLE
        || jobKind == ReconcileJobKind.PLAN_SNAPSHOT;
  }

  private static boolean planTableDescendantScanned(StoredJobContribution contribution) {
    return contribution != null
        && (contribution.snapshotsProcessed > 0L
            || contribution.statsProcessed > 0L
            || contribution.indexesProcessed > 0L
            || contribution.errors > 0L
            || contribution.completedFileGroups > 0L
            || contribution.failedFileGroups > 0L
            || contribution.completedFiles > 0L
            || contribution.failedFiles > 0L);
  }

  private static boolean planTableDescendantChanged(StoredJobContribution contribution) {
    return contribution != null
        && (contribution.snapshotsProcessed > 0L
            || contribution.statsProcessed > 0L
            || contribution.indexesProcessed > 0L
            || contribution.completedFileGroups > 0L
            || contribution.completedFiles > 0L);
  }

  private List<StoredJobContribution> loadDirectContributions(
      String accountId, String parentJobId) {
    return loadDirectContributionsNoRepair(accountId, parentJobId);
  }

  private List<StoredJobContribution> loadDirectContributionsNoRepair(
      String accountId, String parentJobId) {
    return loadDirectContributions(accountId, parentJobId, false);
  }

  private List<StoredJobContribution> loadDirectContributionsWithRepair(
      String accountId, String parentJobId) {
    return loadDirectContributions(accountId, parentJobId, true);
  }

  private List<StoredJobContribution> loadDirectContributions(
      String accountId, String parentJobId, boolean allowRepair) {
    if (blank(accountId) || blank(parentJobId)) {
      return List.of();
    }
    String prefix = Keys.reconcileJobContributionPointerPrefix(accountId, parentJobId);
    List<StoredJobContribution> out = new ArrayList<>();
    String token = "";
    while (true) {
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers = pointerStore.listPointersByPrefix(prefix, 256, token, next);
      if (pointers.isEmpty()) {
        break;
      }
      for (Pointer pointer : pointers) {
        readContribution(pointer)
            .filter(
                contribution ->
                    accountId.equals(contribution.accountId)
                        && parentJobId.equals(contribution.parentJobId))
            .ifPresent(out::add);
      }
      token = next.toString();
      if (token.isBlank()) {
        break;
      }
    }
    if (out.isEmpty() && allowRepair && hasDirectChildren(accountId, parentJobId)) {
      rebuildDirectChildContributions(accountId, parentJobId);
      return loadDirectContributions(accountId, parentJobId, false);
    }
    return out;
  }

  private Optional<StoredJobContribution> readContribution(Pointer pointer) {
    if (pointer == null || blank(pointer.getBlobUri())) {
      return Optional.empty();
    }
    return readInlineJobContribution(pointer.getBlobUri());
  }

  private boolean hasDirectChildren(String accountId, String parentJobId) {
    StringBuilder next = new StringBuilder();
    return !pointerStore
        .listPointersByPrefix(
            Keys.reconcileJobByParentPointerPrefix(accountId, parentJobId), 1, "", next)
        .isEmpty();
  }

  private void rebuildDirectChildContributions(String accountId, String parentJobId) {
    if (blank(accountId) || blank(parentJobId)) {
      return;
    }
    String prefix = Keys.reconcileJobByParentPointerPrefix(accountId, parentJobId);
    String token = "";
    java.util.Set<String> activeChildIds = new java.util.HashSet<>();
    while (true) {
      StringBuilder next = new StringBuilder();
      List<Pointer> childPointers = pointerStore.listPointersByPrefix(prefix, 256, token, next);
      if (childPointers.isEmpty()) {
        break;
      }
      for (Pointer childPointer : childPointers) {
        var child = readCurrentRecordFromIndexPointer(childPointer);
        if (child.isEmpty()) {
          continue;
        }
        activeChildIds.add(child.get().jobId);
        upsertContributionForParent(child.get());
      }
      token = next.toString();
      if (token.isBlank()) {
        break;
      }
    }
    cleanupStaleContributionPointers(accountId, parentJobId, activeChildIds);
  }

  private void cleanupStaleContributionPointers(
      String accountId, String parentJobId, java.util.Set<String> activeChildIds) {
    String prefix = Keys.reconcileJobContributionPointerPrefix(accountId, parentJobId);
    String token = "";
    while (true) {
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers = pointerStore.listPointersByPrefix(prefix, 256, token, next);
      if (pointers.isEmpty()) {
        return;
      }
      for (Pointer pointer : pointers) {
        StoredJobContribution contribution = readContribution(pointer).orElse(null);
        if (contribution == null || !activeChildIds.contains(contribution.childJobId)) {
          pointerStore.compareAndDelete(pointer.getKey(), pointer.getVersion());
        }
      }
      token = next.toString();
      if (token.isBlank()) {
        return;
      }
    }
  }

  private void refreshContributionChain(StoredReconcileJob record) {
    StoredReconcileJob current = record;
    while (current != null && !blank(current.parentJobId)) {
      upsertContributionForParent(current, true);
      current = loadByAnyAccount(current.parentJobId).map(envelope -> envelope.record).orElse(null);
    }
  }

  private void refreshDirectParentContribution(
      StoredReconcileJob childJob, boolean includeSelfProjectionPayloads) {
    if (childJob == null || blank(childJob.parentJobId)) {
      return;
    }
    upsertContributionForParent(childJob, includeSelfProjectionPayloads);
  }

  private void refreshAncestorContributionRollups(
      StoredReconcileJob childJob, boolean includeSelfProjectionPayloads) {
    if (childJob == null || blank(childJob.parentJobId)) {
      return;
    }
    StoredReconcileJob currentChild = childJob;
    boolean includeCurrentPayloads = includeSelfProjectionPayloads;
    while (currentChild != null && !blank(currentChild.parentJobId)) {
      upsertContributionForParent(currentChild, includeCurrentPayloads);
      StoredEnvelope parentEnvelope = loadByAnyAccount(currentChild.parentJobId).orElse(null);
      if (parentEnvelope == null || parentEnvelope.record == null) {
        return;
      }
      currentChild = refreshCanonicalCountersFromContributions(parentEnvelope);
      includeCurrentPayloads = false;
    }
  }

  private StoredReconcileJob refreshCanonicalCountersFromContributions(
      StoredEnvelope parentEnvelope) {
    if (parentEnvelope == null
        || parentEnvelope.record == null
        || !isParentCapable(parentEnvelope.record.jobKind())) {
      return parentEnvelope == null ? null : parentEnvelope.record;
    }
    if (isTerminalState(parentEnvelope.record.state)) {
      return parentEnvelope.record;
    }
    List<StoredJobContribution> contributions =
        loadDirectContributionsNoRepair(
            parentEnvelope.record.accountId, parentEnvelope.record.jobId);
    DirectChildCounts directChildCounts = countDirectChildStates(contributions);
    ProjectedPublicJob projected = projectPublicJob(parentEnvelope.record, false, false);
    Optional<StoredEnvelope> updated =
        mutateByCanonicalPointerReturningRecord(
            parentEnvelope.canonicalPointerKey,
            existing -> {
              if (!isParentCapable(existing.jobKind())) {
                return existing;
              }
              if (isTerminalState(existing.state)) {
                return null;
              }
              boolean leaseOwnsCanonicalState =
                  hasLiveLease(existing, true, System.currentTimeMillis());
              String nextState =
                  canonicalParentStateFromProjection(existing, projected, directChildCounts);
              String nextMessage =
                  leaseOwnsCanonicalState
                      ? existing.message
                      : canonicalParentMessageFromProjection(existing, projected, nextState);
              String nextExecutorId =
                  leaseOwnsCanonicalState
                      ? existing.executorId
                      : canonicalParentExecutorFromProjection(existing, projected, nextState);
              long nextFinishedAtMs =
                  leaseOwnsCanonicalState
                      ? existing.finishedAtMs
                      : (isTerminalState(nextState)
                          ? Math.max(projected.finishedAtMs, existing.finishedAtMs)
                          : 0L);
              if (existing.tablesScanned == projected.tablesScanned
                  && existing.tablesChanged == projected.tablesChanged
                  && existing.viewsScanned == projected.viewsScanned
                  && existing.viewsChanged == projected.viewsChanged
                  && existing.errors == projected.errors
                  && existing.snapshotsProcessed == projected.snapshotsProcessed
                  && existing.statsProcessed == projected.statsProcessed
                  && existing.indexesProcessed == projected.projection.indexesProcessed
                  && existing.plannedFileGroups == projected.projection.plannedFileGroups
                  && existing.plannedFiles == projected.projection.plannedFiles
                  && existing.completedFileGroups == projected.projection.completedFileGroups
                  && existing.failedFileGroups == projected.projection.failedFileGroups
                  && existing.completedFiles == projected.projection.completedFiles
                  && existing.failedFiles == projected.projection.failedFiles
                  && existing.completedChildJobs == directChildCounts.completed
                  && existing.failedChildJobs == directChildCounts.failed
                  && existing.cancelledChildJobs == directChildCounts.cancelled
                  && blankToEmpty(existing.state)
                      .equals(leaseOwnsCanonicalState ? blankToEmpty(existing.state) : nextState)
                  && blankToEmpty(existing.message).equals(nextMessage)
                  && blankToEmpty(existing.executorId).equals(nextExecutorId)
                  && existing.finishedAtMs == nextFinishedAtMs) {
                return null;
              }
              if (!leaseOwnsCanonicalState) {
                existing.state = nextState;
              }
              existing.message = nextMessage;
              existing.executorId = nextExecutorId;
              existing.finishedAtMs = nextFinishedAtMs;
              if (!leaseOwnsCanonicalState && isTerminalState(nextState)) {
                existing.readyPointerKey = null;
              }
              existing.tablesScanned = projected.tablesScanned;
              existing.tablesChanged = projected.tablesChanged;
              existing.viewsScanned = projected.viewsScanned;
              existing.viewsChanged = projected.viewsChanged;
              existing.errors = projected.errors;
              existing.snapshotsProcessed = projected.snapshotsProcessed;
              existing.statsProcessed = projected.statsProcessed;
              existing.indexesProcessed = projected.projection.indexesProcessed;
              existing.plannedFileGroups = projected.projection.plannedFileGroups;
              existing.plannedFiles = projected.projection.plannedFiles;
              existing.completedFileGroups = projected.projection.completedFileGroups;
              existing.failedFileGroups = projected.projection.failedFileGroups;
              existing.completedFiles = projected.projection.completedFiles;
              existing.failedFiles = projected.projection.failedFiles;
              existing.completedChildJobs = directChildCounts.completed;
              existing.failedChildJobs = directChildCounts.failed;
              existing.cancelledChildJobs = directChildCounts.cancelled;
              return existing;
            });
    return updated.map(env -> env.record).orElse(parentEnvelope.record);
  }

  private static String canonicalParentStateFromProjection(
      StoredReconcileJob existing,
      ProjectedPublicJob projected,
      DirectChildCounts directChildCounts) {
    String projectedState = blankToEmpty(projected.state);
    if (isTerminalState(projectedState)) {
      return projectedState;
    }
    if ((hasExpectedDirectChildJobs(existing) || directChildCounts.totalObserved() > 0L)
        && !directChildJobsComplete(existing, directChildCounts)) {
      return "JS_WAITING";
    }
    if ("JS_WAITING".equals(projectedState)
        && directChildJobsComplete(existing, directChildCounts)) {
      return "JS_SUCCEEDED";
    }
    return switch (projectedState) {
      case "JS_QUEUED" -> projectedState;
      case "JS_RUNNING", "JS_CANCELLING" -> projectedState;
      case "JS_FAILED", "JS_CANCELLED" -> projectedState;
      default -> "JS_WAITING";
    };
  }

  private static String canonicalParentMessageFromProjection(
      StoredReconcileJob existing, ProjectedPublicJob projected, String nextState) {
    if ("JS_WAITING".equals(nextState)) {
      if ("JS_WAITING".equals(blankToEmpty(projected.state))) {
        return firstNonBlank(projected.message, existing.message, "Waiting on child work");
      }
      return firstNonBlank(existing.message, "Waiting on child work");
    }
    if ("JS_SUCCEEDED".equals(nextState)) {
      return normalizeSucceededMessage(
          firstNonBlank(projected.message, existing.message, "Succeeded"));
    }
    return firstNonBlank(projected.message, existing.message, nextState);
  }

  private static String canonicalParentExecutorFromProjection(
      StoredReconcileJob existing, ProjectedPublicJob projected, String nextState) {
    if ("JS_WAITING".equals(nextState) || "JS_SUCCEEDED".equals(nextState)) {
      return "";
    }
    return firstNonBlank(projected.executorId, existing.executorId);
  }

  private void upsertContributionForParent(StoredReconcileJob childJob) {
    upsertContributionForParent(childJob, true);
  }

  private void upsertContributionForParent(
      StoredReconcileJob childJob, boolean includeSelfProjectionPayloads) {
    if (childJob == null || blank(childJob.parentJobId)) {
      return;
    }
    StoredJobContribution contribution =
        contributionSnapshot(
            childJob.accountId, childJob.parentJobId, childJob, includeSelfProjectionPayloads);
    upsertReferencePointer(
        Keys.reconcileJobContributionPointer(
            contribution.accountId, contribution.parentJobId, contribution.childJobId),
        encodeInlineJobContribution(contribution));
  }

  private StoredJobContribution contributionSnapshot(
      String parentAccountId,
      String parentJobId,
      StoredReconcileJob childJob,
      boolean includeSelfProjectionPayloads) {
    ProjectedPublicJob projected = projectPublicJob(childJob, includeSelfProjectionPayloads, false);
    return StoredJobContribution.of(
        parentAccountId,
        parentJobId,
        childJob.jobId,
        projected.state,
        projected.message,
        projected.startedAtMs,
        projected.finishedAtMs,
        projected.tablesScanned,
        projected.tablesChanged,
        projected.viewsScanned,
        projected.viewsChanged,
        projected.errors,
        projected.snapshotsProcessed,
        projected.statsProcessed,
        projected.projection.indexesProcessed,
        projected.projection.plannedFileGroups,
        projected.projection.plannedFiles,
        projected.projection.completedFileGroups,
        projected.projection.failedFileGroups,
        projected.projection.completedFiles,
        projected.projection.failedFiles,
        projected.executorId,
        Math.max(childJob.updatedAtMs, System.currentTimeMillis()));
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

  private static long firstNonZeroMin(
      long seed,
      List<StoredJobContribution> contributions,
      java.util.function.ToLongFunction<StoredJobContribution> extractor) {
    long result = seed > 0L ? seed : 0L;
    for (StoredJobContribution contribution : contributions) {
      long value = extractor.applyAsLong(contribution);
      if (value <= 0L) {
        continue;
      }
      result = result <= 0L ? value : Math.min(result, value);
    }
    return result;
  }

  private long maxTerminalFinishedAtMs(
      StoredReconcileJob stored, List<StoredJobContribution> contributions, String projectedState) {
    long result = isTerminalState(stored.state) ? Math.max(0L, stored.finishedAtMs) : 0L;
    for (StoredJobContribution contribution : contributions) {
      if (!isTerminalState(contribution.state) || contribution.finishedAtMs <= 0L) {
        continue;
      }
      if ("JS_SUCCEEDED".equals(projectedState) && !"JS_SUCCEEDED".equals(contribution.state)) {
        continue;
      }
      result = Math.max(result, contribution.finishedAtMs);
    }
    return result;
  }

  private void incrementExpectedChildJobs(String parentJobId) {
    incrementExpectedChildJobs(parentJobId, 1);
  }

  private void incrementExpectedChildJobs(String parentJobId, int delta) {
    if (blank(parentJobId)) {
      return;
    }
    int effectiveDelta = Math.max(0, delta);
    if (effectiveDelta == 0) {
      return;
    }
    mutateByJobIdReturningRecord(
        parentJobId,
        existing -> {
          if (existing == null
              || !isParentCapable(existing.jobKind())
              || (isTerminalState(existing.state) && existing.finishedAtMs > 0L)) {
            return null;
          }
          existing.expectedChildJobs =
              Math.max(0L, existing.expectedChildJobs) + (long) effectiveDelta;
          return existing;
        });
  }

  private static boolean hasExpectedDirectChildJobs(StoredReconcileJob stored) {
    return stored != null && Math.max(0L, stored.expectedChildJobs) > 0L;
  }

  private static boolean hasIncompleteDirectChildJobs(StoredReconcileJob stored) {
    return hasExpectedDirectChildJobs(stored) && !directChildJobsComplete(stored);
  }

  private static boolean directChildJobsComplete(StoredReconcileJob stored) {
    if (stored == null) {
      return true;
    }
    return directChildJobsComplete(
        Math.max(0L, stored.expectedChildJobs),
        stored.completedChildJobs,
        stored.failedChildJobs,
        stored.cancelledChildJobs);
  }

  private static boolean directChildJobsComplete(
      long expectedChildJobs,
      long completedChildJobs,
      long failedChildJobs,
      long cancelledChildJobs) {
    long expected = Math.max(0L, expectedChildJobs);
    if (expected <= 0L) {
      return true;
    }
    long terminalChildren =
        Math.max(0L, completedChildJobs)
            + Math.max(0L, failedChildJobs)
            + Math.max(0L, cancelledChildJobs);
    return terminalChildren >= expected;
  }

  private static boolean directChildJobsComplete(
      StoredReconcileJob stored, DirectChildCounts directChildCounts) {
    if (stored == null) {
      return true;
    }
    long expected = maxExpectedChildJobs(stored, directChildCounts.totalObserved());
    if (expected <= 0L) {
      return true;
    }
    return directChildCounts.totalTerminal() >= expected;
  }

  private static long maxExpectedChildJobs(StoredReconcileJob stored, long directChildJobs) {
    if (stored == null) {
      return Math.max(0L, directChildJobs);
    }
    return Math.max(Math.max(0L, stored.expectedChildJobs), Math.max(0L, directChildJobs));
  }

  private static DirectChildCounts countDirectChildStates(
      List<StoredJobContribution> contributions) {
    DirectChildCounts counts = DirectChildCounts.empty();
    for (StoredJobContribution contribution : contributions) {
      counts = counts.incrementedBy(contribution == null ? "" : contribution.state);
    }
    return counts;
  }

  private long countDirectChildJobs(String accountId, String parentJobId) {
    if (blank(accountId) || blank(parentJobId)) {
      return 0L;
    }
    return Math.max(
        0L,
        pointerStore.countByPrefix(Keys.reconcileJobByParentPointerPrefix(accountId, parentJobId)));
  }

  private static JobProjection projectSnapshotPlan(ReconcileSnapshotTask snapshotTask) {
    if (snapshotTask == null || snapshotTask.isEmpty()) {
      return JobProjection.empty();
    }
    long plannedFileGroups =
        snapshotTask.fileGroupCount() > 0
            ? snapshotTask.fileGroupCount()
            : snapshotTask.fileGroups().size();
    long plannedFiles =
        snapshotTask.fileGroups().stream()
            .mapToLong(DurableReconcileJobStore::plannedFilesForGroup)
            .sum();
    return new JobProjection(0L, plannedFileGroups, plannedFiles, 0L, 0L, 0L, 0L);
  }

  private static JobProjection projectExecFileGroup(
      ReconcileFileGroupTask fileGroupTask, String state) {
    if (fileGroupTask == null || fileGroupTask.isEmpty()) {
      return JobProjection.empty();
    }
    long plannedFiles = plannedFilesForGroup(fileGroupTask);
    long indexesProcessed = 0L;
    long completedFiles = 0L;
    long failedFiles = 0L;
    for (ReconcileFileResult result : fileGroupTask.fileResults()) {
      if (result == null || result.isEmpty()) {
        continue;
      }
      if (hasIndexArtifact(result)) {
        indexesProcessed++;
      }
      if (result.state() == ReconcileFileResult.State.SUCCEEDED) {
        completedFiles++;
      } else {
        failedFiles++;
      }
    }
    if (completedFiles == 0L && failedFiles == 0L) {
      if ("JS_SUCCEEDED".equals(state)) {
        completedFiles = plannedFiles;
      } else if ("JS_FAILED".equals(state) || "JS_CANCELLED".equals(state)) {
        failedFiles = plannedFiles;
      }
    }
    return new JobProjection(
        indexesProcessed,
        1L,
        plannedFiles,
        "JS_SUCCEEDED".equals(state) ? 1L : 0L,
        ("JS_FAILED".equals(state) || "JS_CANCELLED".equals(state)) ? 1L : 0L,
        completedFiles,
        failedFiles);
  }

  private static boolean hasIndexArtifact(ReconcileFileResult result) {
    return result != null
        && result.indexArtifact() != null
        && (!result.indexArtifact().artifactUri().isBlank()
            || !result.indexArtifact().artifactFormat().isBlank()
            || result.indexArtifact().artifactFormatVersion() > 0);
  }

  private <T> T requireBlob(String blobUri, Class<T> type, String payloadName, String jobId) {
    return readBlob(blobUri, type)
        .orElseThrow(
            () ->
                new IllegalStateException(
                    String.format(
                        "Reconcile job %s is missing required %s blob=%s",
                        blankToEmpty(jobId), payloadName, blankToEmpty(blobUri))));
  }

  private static long plannedFilesForGroup(ReconcileFileGroupTask fileGroupTask) {
    if (fileGroupTask == null || fileGroupTask.isEmpty()) {
      return 0L;
    }
    if (fileGroupTask.fileCount() > 0) {
      return fileGroupTask.fileCount();
    }
    return fileGroupTask.filePaths().size();
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

  private boolean isCanonicalJobPointerKey(String accountId, String pointerKey) {
    String prefix = Keys.reconcileJobStateRowByIdPrefix(accountId);
    if (pointerKey == null || prefix == null || !pointerKey.startsWith(prefix)) {
      return false;
    }
    String suffix = pointerKey.substring(prefix.length());
    return !suffix.isBlank() && suffix.indexOf('/') < 0;
  }

  private boolean isCanonicalJobPointerKey(String pointerKey) {
    if (pointerKey == null
        || pointerKey.isBlank()
        || pointerKey.startsWith(Keys.reconcileJobLookupPointerByIdPrefix())) {
      return false;
    }
    int marker = pointerKey.indexOf("/reconcile/jobs/by-id/");
    if (!pointerKey.startsWith("/accounts/") || marker < 0) {
      return false;
    }
    String suffix = pointerKey.substring(marker + "/reconcile/jobs/by-id/".length());
    return !suffix.isBlank() && suffix.indexOf('/') < 0;
  }

  private static String dedupePointerKey(StoredReconcileJob record) {
    if (record == null
        || record.accountId == null
        || record.accountId.isBlank()
        || record.dedupeKeyHash == null
        || record.dedupeKeyHash.isBlank()) {
      return "";
    }
    return Keys.reconcileDedupePointer(record.accountId, record.dedupeKeyHash);
  }

  private static boolean requiresReadyPointer(StoredReconcileJob record) {
    if (record == null) {
      return false;
    }
    return "JS_QUEUED".equals(record.state);
  }

  private List<String> readyPointerKeysForCleanup(StoredReconcileJob record) {
    if (record == null) {
      return List.of();
    }
    java.util.LinkedHashSet<String> readyKeys =
        new java.util.LinkedHashSet<>(readyPointerKeys(record));
    boolean hasStoredReadyPointer = !blank(record.readyPointerKey);
    if (hasStoredReadyPointer) {
      readyKeys.add(record.readyPointerKey);
    }
    boolean shouldReconstructHistoricalReadyKeys =
        hasStoredReadyPointer || !isTerminalState(record.state);
    if (shouldReconstructHistoricalReadyKeys) {
      long dueAtMs =
          record.nextAttemptAtMs > 0L
              ? record.nextAttemptAtMs
              : parseDueMillis(blankToEmpty(record.readyPointerKey));
      if (dueAtMs != INVALID_ORDERED_POINTER_MS && dueAtMs > 0L) {
        ReconcileExecutionPolicy executionPolicy = record.executionPolicy();
        readyKeys.add(readyPointerKeyFor(record, dueAtMs));
        readyKeys.add(
            readyPointerKeyFor(
                record,
                ReadyIndexType.EXECUTION_CLASS,
                dueAtMs,
                executionPolicy.executionClass().name()));
        readyKeys.add(
            readyPointerKeyFor(
                record, ReadyIndexType.EXECUTION_LANE, dueAtMs, executionPolicy.lane()));
        if (!blank(record.pinnedExecutorId())) {
          readyKeys.add(
              readyPointerKeyFor(
                  record, ReadyIndexType.PINNED_EXECUTOR, dueAtMs, record.pinnedExecutorId()));
        }
        readyKeys.add(
            readyPointerKeyFor(record, ReadyIndexType.JOB_KIND, dueAtMs, record.jobKind().name()));
      }
    }
    readyKeys.removeIf(key -> blank(key));
    return List.copyOf(readyKeys);
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
        "reconcile state transition source=%s jobId=%s kind=%s oldState=%s newState=%s readyPointerKey=%s completionHint=%s",
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

  private static String normalizeSucceededMessage(String message) {
    String normalized = blankToEmpty(message);
    if (normalized.isBlank()) {
      return "Succeeded";
    }
    if (normalized.startsWith("Planned ") || normalized.startsWith("Snapshot plan recorded")) {
      return "Succeeded";
    }
    return switch (normalized) {
      case "Queued",
          "Queued (full)",
          "Leased",
          "Running",
          "Waiting",
          "Cancelling",
          "Retrying",
          "Waiting on dependency",
          "Waiting on child work" ->
          "Succeeded";
      default -> normalized;
    };
  }

  private static String normalizeWaitingStateMessage(String state, String message) {
    String normalizedState = blankToEmpty(state);
    String normalizedMessage = blankToEmpty(message);
    if (!"JS_WAITING".equals(normalizedState)) {
      return normalizedMessage;
    }
    return switch (normalizedMessage) {
      case "", "Queued", "Queued (full)", "Leased", "Running", "Waiting", "Retrying" ->
          "Waiting on child work";
      default -> normalizedMessage;
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
      if (destinationTableId.isBlank()) {
        destinationTableId = blankToEmpty(tableTask.destinationTableId());
      }
      if (!destinationTableId.isBlank()) {
        return "table|" + destinationTableId;
      }
      String sourceNamespace = blankToEmpty(tableTask.sourceNamespace());
      String sourceTable = blankToEmpty(tableTask.sourceTable());
      if (!sourceNamespace.isBlank() || !sourceTable.isBlank()) {
        return "table-source|" + sourceNamespace + "|" + sourceTable;
      }
      return "tables|" + namespaces;
    }
    if (jobKind == ReconcileJobKind.PLAN_VIEW && viewTask != null) {
      String destinationViewId = blankToEmpty(scope.destinationViewId());
      if (destinationViewId.isBlank()) {
        destinationViewId = blankToEmpty(viewTask.destinationViewId());
      }
      if (!destinationViewId.isBlank()) {
        return "view|" + destinationViewId;
      }
      String sourceNamespace = blankToEmpty(viewTask.sourceNamespace());
      String sourceView = blankToEmpty(viewTask.sourceView());
      if (!sourceNamespace.isBlank() || !sourceView.isBlank()) {
        return "view-source|" + sourceNamespace + "|" + sourceView;
      }
      return "views|" + namespaces;
    }
    if (jobKind == ReconcileJobKind.PLAN_SNAPSHOT
        && snapshotTask != null
        && !blank(snapshotTask.tableId())) {
      return "snapshot-plan|" + snapshotTask.tableId();
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
            String.valueOf(snapshotTask == null ? 0L : snapshotTask.snapshotId()))
        .scalar(
            "snapshot_task.source_namespace",
            snapshotTask == null ? "" : blankToEmpty(snapshotTask.sourceNamespace()))
        .scalar(
            "snapshot_task.source_table",
            snapshotTask == null ? "" : blankToEmpty(snapshotTask.sourceTable()))
        .scalar(
            "snapshot_task.file_group_plan_recorded",
            String.valueOf(snapshotTask != null && snapshotTask.fileGroupPlanRecorded()))
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
            String.valueOf(fileGroupTask == null ? 0L : fileGroupTask.snapshotId()))
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

  private static void requireExecFileGroupIdentity(
      ReconcileJobKind jobKind, ReconcileFileGroupTask fileGroupTask) {
    if (jobKind != ReconcileJobKind.EXEC_FILE_GROUP) {
      return;
    }
    ReconcileFileGroupTask effective =
        fileGroupTask == null ? ReconcileFileGroupTask.empty() : fileGroupTask;
    if (blank(effective.planId())
        || blank(effective.groupId())
        || blank(effective.tableId())
        || effective.snapshotId() < 0L) {
      throw new IllegalArgumentException(
          "EXEC_FILE_GROUP requires planId, groupId, tableId, and snapshotId; got "
              + "planId="
              + blankToEmpty(effective.planId())
              + " groupId="
              + blankToEmpty(effective.groupId())
              + " tableId="
              + blankToEmpty(effective.tableId())
              + " snapshotId="
              + effective.snapshotId()
              + " fileCount="
              + effective.fileCount()
              + " paths="
              + effective.filePaths().size()
              + " results="
              + effective.fileResults().size());
    }
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

  private static StateListCursor decodeStateListCursor(String pageToken) {
    if (pageToken == null || pageToken.isBlank()) {
      return new StateListCursor(0, "");
    }
    if (!pageToken.startsWith(STATE_LIST_TOKEN_V1_PREFIX)) {
      return new StateListCursor(0, pageToken);
    }
    try {
      String decoded =
          new String(
              Base64.getUrlDecoder()
                  .decode(pageToken.substring(STATE_LIST_TOKEN_V1_PREFIX.length())),
              StandardCharsets.UTF_8);
      int separator = decoded.indexOf('\n');
      if (separator < 0) {
        return new StateListCursor(Math.max(0, Integer.parseInt(decoded)), "");
      }
      int stateIndex = Integer.parseInt(decoded.substring(0, separator));
      String nestedPageToken = decoded.substring(separator + 1);
      return new StateListCursor(Math.max(0, stateIndex), nestedPageToken);
    } catch (RuntimeException e) {
      return new StateListCursor(0, "");
    }
  }

  private static String encodeStateListCursor(int stateIndex, String nestedPageToken) {
    if (stateIndex < 0) {
      return "";
    }
    String payload = stateIndex + "\n" + (nestedPageToken == null ? "" : nestedPageToken);
    return STATE_LIST_TOKEN_V1_PREFIX
        + Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(payload.getBytes(StandardCharsets.UTF_8));
  }

  private static java.util.Set<String> normalizeStateFilter(java.util.Set<String> states) {
    if (states == null || states.isEmpty()) {
      return java.util.Set.of();
    }
    return states.stream()
        .filter(state -> state != null && !state.isBlank())
        .map(String::trim)
        .collect(java.util.stream.Collectors.toUnmodifiableSet());
  }

  private static List<String> orderedStateFilter(java.util.Set<String> states) {
    if (states == null || states.isEmpty()) {
      return List.of();
    }
    return states.stream().sorted().toList();
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
    return columns
        + "|"
        + outputs
        + "|"
        + policy.defaultColumnScope().name()
        + "|"
        + policy.maxDefaultColumns();
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

  private static String connectorIndexPointerKey(
      String accountId, String connectorId, long createdAtMs, String jobId) {
    if (blank(accountId) || blank(connectorId) || blank(jobId)) {
      return "";
    }
    return Keys.reconcileJobByConnectorPointer(
        accountId, connectorId, connectorSortableJobToken(createdAtMs, jobId));
  }

  private static String connectorSortableJobToken(long createdAtMs, String jobId) {
    long created = Math.max(0L, createdAtMs);
    // Invert createdAtMs so lexicographic ascending scans return jobs in descending created-time
    // order, with jobId as the stable tiebreaker.
    long reversedCreated = REVERSE_SORT_MAX_MS - created;
    return String.format("%019d-%s", reversedCreated, jobId);
  }

  private static final class StoredEnvelope {
    final String canonicalPointerKey;
    final StoredReconcileJob record;

    private StoredEnvelope(String canonicalPointerKey, StoredReconcileJob record) {
      this.canonicalPointerKey = canonicalPointerKey;
      this.record = record;
    }
  }

  private static final class PendingBulkEnqueue {
    final int index;
    final String dedupePointerKey;
    final String canonicalKey;
    final String lookupKey;
    final String parentKey;
    final List<String> readyKeys;
    final List<String> stateKeys;
    final String connectorIndexKey;
    final String definitionBlobUri;
    final String snapshotPlanBlobUri;
    final String fileGroupPlanBlobUri;
    final StoredJobDefinition definition;
    final StoredSnapshotPlanPayload snapshotPlanPayload;
    final StoredFileGroupPlanPayload fileGroupPlanPayload;
    final ReconcileFileGroupTask resultPayloadTask;
    final StoredReconcileJob record;
    boolean definitionWritten;
    boolean snapshotPlanWritten;
    boolean fileGroupPlanWritten;
    boolean resultPointerWritten;
    String resultBlobUri;

    private PendingBulkEnqueue(
        int index,
        String dedupePointerKey,
        String canonicalKey,
        String lookupKey,
        String parentKey,
        List<String> readyKeys,
        List<String> stateKeys,
        String connectorIndexKey,
        String definitionBlobUri,
        String snapshotPlanBlobUri,
        String fileGroupPlanBlobUri,
        StoredJobDefinition definition,
        StoredSnapshotPlanPayload snapshotPlanPayload,
        StoredFileGroupPlanPayload fileGroupPlanPayload,
        ReconcileFileGroupTask resultPayloadTask,
        StoredReconcileJob record) {
      this.index = index;
      this.dedupePointerKey = dedupePointerKey;
      this.canonicalKey = canonicalKey;
      this.lookupKey = lookupKey;
      this.parentKey = parentKey;
      this.readyKeys = readyKeys == null ? List.of() : List.copyOf(readyKeys);
      this.stateKeys = stateKeys == null ? List.of() : List.copyOf(stateKeys);
      this.connectorIndexKey = connectorIndexKey;
      this.definitionBlobUri = definitionBlobUri;
      this.snapshotPlanBlobUri = snapshotPlanBlobUri;
      this.fileGroupPlanBlobUri = fileGroupPlanBlobUri;
      this.definition = definition;
      this.snapshotPlanPayload = snapshotPlanPayload;
      this.fileGroupPlanPayload = fileGroupPlanPayload;
      this.resultPayloadTask = resultPayloadTask;
      this.record = record;
    }
  }

  private enum ReadyIndexType {
    GLOBAL,
    EXECUTION_CLASS,
    EXECUTION_LANE,
    PINNED_EXECUTOR,
    JOB_KIND
  }

  private record ReadyIndexSelection(String prefix, ReadyIndexType type, String filterValue) {}

  private static final class LeaseScanStats {
    private int scanCount;
    private int candidateCount;
  }

  private record ReadyPointerTarget(
      String canonicalPointerKey,
      String accountId,
      String jobId,
      long dueAtMs,
      ReadyIndexType indexType,
      String filterValue) {}

  private record ListCursor(String storeToken, int skip) {}

  private record StateListCursor(int stateIndex, String pageToken) {}

  private record StateListRequest(String state, int pageSize, String pageToken) {}

  static final class StoredJobLease {
    public String accountId;
    public String jobId;
    public String epoch;
    public long expiresAtMs;

    static StoredJobLease empty(String accountId, String jobId) {
      StoredJobLease lease = new StoredJobLease();
      lease.accountId = accountId;
      lease.jobId = jobId;
      lease.epoch = "";
      lease.expiresAtMs = 0L;
      return lease;
    }

    static StoredJobLease active(String accountId, String jobId, String epoch, long expiresAtMs) {
      StoredJobLease lease = empty(accountId, jobId);
      lease.epoch = epoch == null ? "" : epoch;
      lease.expiresAtMs = expiresAtMs;
      return lease;
    }
  }

  static final class StoredJobDefinition {
    public String sourceNamespace;
    public String sourceTable;
    public String taskMode;
    public String taskDestinationNamespaceId;
    public String taskDestinationTableId;
    public String taskDestinationTableDisplayName;
    public String sourceView;
    public String taskDestinationViewId;
    public String taskDestinationViewDisplayName;
    public String destinationTableId;
    public String destinationViewId;
    public List<String> destinationNamespaceIds = List.of();
    public List<ReconcileScope.ScopedCaptureRequest> destinationCaptureRequests = List.of();
    public List<ReconcileCapturePolicy.Column> capturePolicyColumns = List.of();
    public List<String> capturePolicyOutputs = List.of();
    public String capturePolicyDefaultColumnScope;
    public int capturePolicyMaxDefaultColumns;
    public String snapshotSelectionKind;
    public List<Long> snapshotSelectionSnapshotIds = List.of();
    public int snapshotSelectionLatestN;

    static StoredJobDefinition of(
        ReconcileScope scope, ReconcileTableTask tableTask, ReconcileViewTask viewTask) {
      ReconcileScope effectiveScope = scope == null ? ReconcileScope.empty() : scope;
      ReconcileTableTask effectiveTableTask =
          tableTask == null ? ReconcileTableTask.empty() : tableTask;
      ReconcileViewTask effectiveViewTask = viewTask == null ? ReconcileViewTask.empty() : viewTask;
      StoredJobDefinition definition = new StoredJobDefinition();
      definition.sourceNamespace = effectiveTableTask.sourceNamespace();
      definition.sourceTable = effectiveTableTask.sourceTable();
      definition.taskMode = effectiveTableTask.mode().name();
      definition.taskDestinationNamespaceId = effectiveTableTask.destinationNamespaceId();
      definition.taskDestinationTableId = blankToEmpty(effectiveTableTask.destinationTableId());
      definition.taskDestinationTableDisplayName = effectiveTableTask.destinationTableDisplayName();
      definition.sourceView = effectiveViewTask.sourceView();
      if (!effectiveViewTask.isEmpty()) {
        definition.sourceNamespace = effectiveViewTask.sourceNamespace();
        definition.taskMode = effectiveViewTask.mode().name();
        definition.taskDestinationNamespaceId = effectiveViewTask.destinationNamespaceId();
      }
      definition.taskDestinationViewId = blankToEmpty(effectiveViewTask.destinationViewId());
      definition.taskDestinationViewDisplayName = effectiveViewTask.destinationViewDisplayName();
      definition.destinationNamespaceIds = effectiveScope.destinationNamespaceIds();
      definition.destinationTableId = effectiveScope.destinationTableId();
      definition.destinationViewId = effectiveScope.destinationViewId();
      definition.destinationCaptureRequests = effectiveScope.destinationCaptureRequests();
      definition.capturePolicyColumns = effectiveScope.capturePolicy().columns();
      definition.capturePolicyOutputs =
          effectiveScope.capturePolicy().outputs().stream().map(Enum::name).toList();
      definition.capturePolicyDefaultColumnScope =
          effectiveScope.capturePolicy().defaultColumnScope().name();
      definition.capturePolicyMaxDefaultColumns =
          effectiveScope.capturePolicy().maxDefaultColumns();
      ReconcileSnapshotSelection snapshotSelection = effectiveScope.snapshotSelection();
      definition.snapshotSelectionKind = snapshotSelection.kind().name();
      definition.snapshotSelectionSnapshotIds = snapshotSelection.snapshotIds();
      definition.snapshotSelectionLatestN = snapshotSelection.latestN();
      return definition;
    }

    ReconcileScope toScope() {
      ReconcileSnapshotSelection snapshotSelection =
          switch (blankToEmpty(snapshotSelectionKind)) {
            case "CURRENT" -> ReconcileSnapshotSelection.current();
            case "LATEST_N" -> ReconcileSnapshotSelection.latestN(snapshotSelectionLatestN);
            case "EXPLICIT" ->
                ReconcileSnapshotSelection.explicit(
                    snapshotSelectionSnapshotIds == null
                        ? List.of()
                        : snapshotSelectionSnapshotIds);
            case "ALL" -> ReconcileSnapshotSelection.all();
            default -> ReconcileSnapshotSelection.unspecified();
          };
      return ReconcileScope.of(
          destinationNamespaceIds,
          destinationTableId,
          destinationViewId,
          destinationCaptureRequests,
          ReconcileCapturePolicy.of(
              capturePolicyColumns,
              capturePolicyOutputs.stream()
                  .map(ReconcileCapturePolicy.Output::valueOf)
                  .collect(java.util.stream.Collectors.toSet()),
              blankToEmpty(capturePolicyDefaultColumnScope).isBlank()
                  ? ReconcileCapturePolicy.DefaultColumnScope.FIRST_N
                  : ReconcileCapturePolicy.DefaultColumnScope.valueOf(
                      capturePolicyDefaultColumnScope),
              capturePolicyMaxDefaultColumns),
          snapshotSelection);
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
  }

  static final class StoredSnapshotPlanPayload {
    public List<ReconcileFileGroupTask> fileGroups = List.of();

    static StoredSnapshotPlanPayload of(List<ReconcileFileGroupTask> fileGroups) {
      StoredSnapshotPlanPayload payload = new StoredSnapshotPlanPayload();
      payload.fileGroups = fileGroups == null ? List.of() : fileGroups;
      return payload;
    }

    List<ReconcileFileGroupTask> fileGroups() {
      return fileGroups == null ? List.of() : fileGroups;
    }
  }

  static final class StoredFileGroupPlanPayload {
    public int fileCount;
    public List<String> filePaths = List.of();

    static StoredFileGroupPlanPayload of(ReconcileFileGroupTask task) {
      ReconcileFileGroupTask effective = task == null ? ReconcileFileGroupTask.empty() : task;
      StoredFileGroupPlanPayload payload = new StoredFileGroupPlanPayload();
      payload.fileCount = effective.fileCount();
      payload.filePaths = effective.filePaths() == null ? List.of() : effective.filePaths();
      return payload;
    }

    int fileCount() {
      if (fileCount > 0) {
        return fileCount;
      }
      return filePaths == null ? 0 : filePaths.size();
    }

    List<String> filePaths() {
      return filePaths == null ? List.of() : filePaths;
    }
  }

  static final class StoredFileGroupResultPayload {
    public List<String> filePaths = List.of();
    public List<ReconcileFileResult> fileResults = List.of();

    static StoredFileGroupResultPayload of(ReconcileFileGroupTask task) {
      ReconcileFileGroupTask effective = task == null ? ReconcileFileGroupTask.empty() : task;
      StoredFileGroupResultPayload payload = new StoredFileGroupResultPayload();
      payload.filePaths = effective.filePaths() == null ? List.of() : effective.filePaths();
      payload.fileResults = effective.fileResults() == null ? List.of() : effective.fileResults();
      return payload;
    }

    List<String> filePaths() {
      return filePaths == null ? List.of() : filePaths;
    }

    List<ReconcileFileResult> fileResults() {
      return fileResults == null ? List.of() : fileResults;
    }
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
    public String snapshotTaskTableId;
    public long snapshotTaskSnapshotId;
    public String snapshotTaskSourceNamespace;
    public String snapshotTaskSourceTable;
    public boolean snapshotTaskFileGroupPlanRecorded;
    public String snapshotTaskCompletionMode;
    public String snapshotTaskDirectStatsBlobUri;
    public int snapshotTaskDirectStatsRecordCount;
    public String fileGroupPlanId;
    public String fileGroupGroupId;
    public String fileGroupTableId;
    public long fileGroupSnapshotId;
    public int fileGroupFileCount;
    public String definitionBlobUri;
    public String snapshotPlanBlobUri;
    public String fileGroupPlanBlobUri;
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
    public long expectedChildJobs;
    public long completedChildJobs;
    public long failedChildJobs;
    public long cancelledChildJobs;

    public int attempt;
    public long nextAttemptAtMs;
    public String lastError;

    public String laneKey;
    public String dedupeKeyHash;
    public String readyPointerKey;
    public String connectorIndexPointerKey;
    public String canonicalPointerKey;

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
        String dedupeKeyHash,
        long now,
        String readyPointerKey,
        String connectorIndexPointerKey,
        String definitionBlobUri,
        String snapshotPlanBlobUri,
        String fileGroupPlanBlobUri) {
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
      ReconcileSnapshotTask effectiveSnapshotTask =
          snapshotTask == null ? ReconcileSnapshotTask.empty() : snapshotTask;
      ReconcileFileGroupTask effectiveFileGroupTask =
          fileGroupTask == null ? ReconcileFileGroupTask.empty() : fileGroupTask;
      rec.snapshotTaskTableId = blankToEmpty(effectiveSnapshotTask.tableId());
      rec.snapshotTaskSnapshotId = effectiveSnapshotTask.snapshotId();
      rec.snapshotTaskSourceNamespace = effectiveSnapshotTask.sourceNamespace();
      rec.snapshotTaskSourceTable = effectiveSnapshotTask.sourceTable();
      rec.snapshotTaskFileGroupPlanRecorded = effectiveSnapshotTask.fileGroupPlanRecorded();
      rec.snapshotTaskCompletionMode = effectiveSnapshotTask.completionMode().name();
      rec.snapshotTaskDirectStatsBlobUri = blankToEmpty(effectiveSnapshotTask.directStatsBlobUri());
      rec.snapshotTaskDirectStatsRecordCount = effectiveSnapshotTask.directStatsRecordCount();
      rec.fileGroupPlanId = blankToEmpty(effectiveFileGroupTask.planId());
      rec.fileGroupGroupId = blankToEmpty(effectiveFileGroupTask.groupId());
      rec.fileGroupTableId = blankToEmpty(effectiveFileGroupTask.tableId());
      rec.fileGroupSnapshotId = effectiveFileGroupTask.snapshotId();
      rec.fileGroupFileCount = (int) plannedFilesForGroup(effectiveFileGroupTask);
      rec.definitionBlobUri = definitionBlobUri;
      rec.snapshotPlanBlobUri = snapshotPlanBlobUri;
      rec.fileGroupPlanBlobUri = fileGroupPlanBlobUri;
      JobProjection initialProjection =
          switch (rec.jobKind()) {
            case PLAN_SNAPSHOT -> projectSnapshotPlan(effectiveSnapshotTask);
            case EXEC_FILE_GROUP -> projectExecFileGroup(effectiveFileGroupTask, rec.state);
            default -> JobProjection.empty();
          };
      rec.indexesProcessed = initialProjection.indexesProcessed;
      rec.plannedFileGroups = initialProjection.plannedFileGroups;
      rec.plannedFiles = initialProjection.plannedFiles;
      rec.completedFileGroups = initialProjection.completedFileGroups;
      rec.failedFileGroups = initialProjection.failedFileGroups;
      rec.completedFiles = initialProjection.completedFiles;
      rec.failedFiles = initialProjection.failedFiles;
      rec.state = "JS_QUEUED";
      rec.message = fullRescan ? "Queued (full)" : "Queued";
      rec.nextAttemptAtMs = now;
      rec.attempt = 0;
      rec.laneKey = laneKey;
      rec.dedupeKeyHash = dedupeKeyHash;
      rec.readyPointerKey = readyPointerKey;
      rec.connectorIndexPointerKey = connectorIndexPointerKey;
      rec.createdAtMs = now;
      rec.updatedAtMs = now;
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

    ReconcileJobKind jobKind() {
      return ReconcileJobKind.fromString(jobKind);
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

  static final class StoredJobContribution {
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

    static StoredJobContribution of(
        String accountId,
        String parentJobId,
        String childJobId,
        String state,
        String message,
        long startedAtMs,
        long finishedAtMs,
        long tablesScanned,
        long tablesChanged,
        long viewsScanned,
        long viewsChanged,
        long errors,
        long snapshotsProcessed,
        long statsProcessed,
        long indexesProcessed,
        long plannedFileGroups,
        long plannedFiles,
        long completedFileGroups,
        long failedFileGroups,
        long completedFiles,
        long failedFiles,
        String executorId,
        long updatedAtMs) {
      StoredJobContribution contribution = new StoredJobContribution();
      contribution.accountId = blankToEmpty(accountId);
      contribution.parentJobId = blankToEmpty(parentJobId);
      contribution.childJobId = blankToEmpty(childJobId);
      contribution.state = blankToEmpty(state);
      contribution.message = blankToEmpty(message);
      contribution.startedAtMs = startedAtMs;
      contribution.finishedAtMs = finishedAtMs;
      contribution.tablesScanned = tablesScanned;
      contribution.tablesChanged = tablesChanged;
      contribution.viewsScanned = viewsScanned;
      contribution.viewsChanged = viewsChanged;
      contribution.errors = errors;
      contribution.snapshotsProcessed = snapshotsProcessed;
      contribution.statsProcessed = statsProcessed;
      contribution.indexesProcessed = indexesProcessed;
      contribution.plannedFileGroups = plannedFileGroups;
      contribution.plannedFiles = plannedFiles;
      contribution.completedFileGroups = completedFileGroups;
      contribution.failedFileGroups = failedFileGroups;
      contribution.completedFiles = completedFiles;
      contribution.failedFiles = failedFiles;
      contribution.executorId = blankToEmpty(executorId);
      contribution.updatedAtMs = updatedAtMs;
      return contribution;
    }
  }

  private record RepairHint(
      String canonicalPointerKey,
      String jobId,
      String state,
      String repairPhase,
      String repairReason,
      String repairTargetKind) {
    String hintKey() {
      return canonicalPointerKey + "|" + repairTargetKind;
    }
  }

  private record ProjectedPublicJob(
      String state,
      String message,
      long startedAtMs,
      long finishedAtMs,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed,
      String executorId,
      JobProjection projection) {
    static ProjectedPublicJob empty() {
      return new ProjectedPublicJob(
          "", "", 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, "", JobProjection.empty());
    }

    static ProjectedPublicJob self(StoredReconcileJob stored, JobProjection projection) {
      String state = blankToEmpty(stored.state);
      return new ProjectedPublicJob(
          state,
          normalizeWaitingStateMessage(state, stored.message),
          stored.startedAtMs,
          stored.finishedAtMs,
          stored.tablesScanned,
          stored.tablesChanged,
          stored.viewsScanned,
          stored.viewsChanged,
          stored.errors,
          stored.snapshotsProcessed,
          stored.statsProcessed,
          blankToEmpty(stored.executorId),
          projection);
    }
  }

  private record DirectChildCounts(
      long completed, long failed, long cancelled, long totalObserved) {
    static DirectChildCounts empty() {
      return new DirectChildCounts(0L, 0L, 0L, 0L);
    }

    DirectChildCounts incrementedBy(String state) {
      return switch (blankToEmpty(state)) {
        case "JS_SUCCEEDED" ->
            new DirectChildCounts(completed + 1L, failed, cancelled, totalObserved + 1L);
        case "JS_FAILED" ->
            new DirectChildCounts(completed, failed + 1L, cancelled, totalObserved + 1L);
        case "JS_CANCELLED" ->
            new DirectChildCounts(completed, failed, cancelled + 1L, totalObserved + 1L);
        default -> new DirectChildCounts(completed, failed, cancelled, totalObserved + 1L);
      };
    }

    long totalTerminal() {
      return completed + failed + cancelled;
    }
  }

  private record JobProjection(
      long indexesProcessed,
      long plannedFileGroups,
      long plannedFiles,
      long completedFileGroups,
      long failedFileGroups,
      long completedFiles,
      long failedFiles) {
    static JobProjection empty() {
      return new JobProjection(0L, 0L, 0L, 0L, 0L, 0L, 0L);
    }
  }
}
