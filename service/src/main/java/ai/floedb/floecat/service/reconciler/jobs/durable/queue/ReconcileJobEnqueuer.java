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

package ai.floedb.floecat.service.reconciler.jobs.durable.queue;

import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.impl.SnapshotPlanBlobStore.SnapshotPlanBlob;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.BulkEnqueueItemResult;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.BulkEnqueueResult;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.BulkEnqueueSpec;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotSelection;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import ai.floedb.floecat.service.common.Canonicalizer;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredJobDefinition;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.projection.ReconcileJobProjector;
import ai.floedb.floecat.service.reconciler.jobs.durable.projection.ReconcileJobProjector.JobProjection;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileJobIndexes;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcilePayloadStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexStore;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.BlobStore;
import jakarta.enterprise.context.ApplicationScoped;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import org.jboss.logging.Logger;

@ApplicationScoped
public class ReconcileJobEnqueuer {
  private static final Logger LOG = Logger.getLogger(ReconcileJobEnqueuer.class);

  private BlobStore blobStore;
  private ReconcilePayloadStore payloadStore;
  private ReconcileJobProjector projector;
  private ReconcileJobIndexStore jobIndexStore;
  private ReconcileJobIndexes indexes;
  private Function<ReconcileSnapshotTask, List<ReconcileFileGroupTask>>
      materializeSnapshotPlanFileGroups;
  private Function<StoredReconcileJob, List<String>> readyPointerKeys;
  private Function<StoredReconcileJob, List<String>> statePointerKeys;
  private ReadyPointerKeyForDue readyPointerKeyForDue;

  @FunctionalInterface
  public interface ReadyPointerKeyForDue {
    String apply(String accountId, String laneKey, String jobId, long dueAtMs);
  }

  public void bind(
      BlobStore blobStore,
      ReconcilePayloadStore payloadStore,
      ReconcileJobProjector projector,
      ReconcileJobIndexStore jobIndexStore,
      ReconcileJobIndexes indexes,
      Function<ReconcileSnapshotTask, List<ReconcileFileGroupTask>>
          materializeSnapshotPlanFileGroups,
      Function<StoredReconcileJob, List<String>> readyPointerKeys,
      Function<StoredReconcileJob, List<String>> statePointerKeys,
      ReadyPointerKeyForDue readyPointerKeyForDue) {
    this.blobStore = blobStore;
    this.payloadStore = payloadStore;
    this.projector = projector;
    this.jobIndexStore = jobIndexStore;
    this.indexes = indexes;
    this.materializeSnapshotPlanFileGroups = materializeSnapshotPlanFileGroups;
    this.readyPointerKeys = readyPointerKeys;
    this.statePointerKeys = statePointerKeys;
    this.readyPointerKeyForDue = readyPointerKeyForDue;
  }

  public BulkEnqueueResult bulkEnqueue(List<BulkEnqueueSpec> specs) {
    return bulkEnqueue(specs, null);
  }

  public BulkEnqueueResult bulkEnqueue(
      List<BulkEnqueueSpec> specs,
      Function<
              List<ReconcileJobIndexStore.QueuedJobInsert>,
              List<ReconcileJobIndexStore.CanonicalRecordMutation>>
          ancestorMutationsBuilder) {
    if (specs == null || specs.isEmpty()) {
      return new BulkEnqueueResult(List.of());
    }

    List<BulkEnqueueItemResult> results = new ArrayList<>(specs.size());
    for (int i = 0; i < specs.size(); i++) {
      results.add(null);
    }
    List<PendingBulkEnqueue> preparedEntries = new ArrayList<>(specs.size());
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
    for (PendingBulkEnqueue entry : preparedEntries) {
      if (results.get(entry.index) != null) {
        continue;
      }

      BulkEnqueueItemResult batchExisting = batchDedupe.get(entry.dedupePointerKey);
      if (batchExisting != null && batchExisting.succeeded()) {
        BulkEnqueueItemResult deduped =
            new BulkEnqueueItemResult(entry.index, batchExisting.jobId, false, "");
        results.set(entry.index, deduped);
        logEnqueueDeduped(entry, deduped.jobId, "batch");
        continue;
      }

      try {
        persistBulkPayloads(entry);
      } catch (RuntimeException e) {
        rollbackFailedBulkEnqueue(entry);
        BulkEnqueueItemResult failed = failedBulkEnqueue(entry.index, e);
        results.set(entry.index, failed);
        logEnqueueFailed(entry, failed.error);
        continue;
      }
      batchDedupe.put(
          entry.dedupePointerKey,
          new BulkEnqueueItemResult(entry.index, entry.record.jobId, true, ""));
    }

    List<PendingBulkEnqueue> pending = new ArrayList<>();
    for (PendingBulkEnqueue entry : preparedEntries) {
      if (results.get(entry.index) == null) {
        pending.add(entry);
      }
    }
    if (ancestorMutationsBuilder != null
        && results.stream()
            .filter(java.util.Objects::nonNull)
            .anyMatch(result -> !result.succeeded())) {
      for (PendingBulkEnqueue entry : pending) {
        rollbackFailedBulkEnqueue(entry);
      }
      throw new IllegalStateException(
          "atomic planner enqueue preparation failed before parent outcome commit");
    }
    if (pending.isEmpty()) {
      return new BulkEnqueueResult(results);
    }

    var pendingByIndex = new java.util.LinkedHashMap<Integer, PendingBulkEnqueue>();
    List<ReconcileJobIndexStore.QueuedJobInsert> inserts = new ArrayList<>(pending.size());
    for (PendingBulkEnqueue entry : pending) {
      pendingByIndex.put(entry.index, entry);
      inserts.add(toQueuedInsert(entry));
    }

    List<BulkEnqueueItemResult> storeResults;
    try {
      storeResults = jobIndexStore.commitQueuedJobInserts(inserts, ancestorMutationsBuilder);
    } catch (RuntimeException e) {
      for (PendingBulkEnqueue entry : rollbackEntriesForStoreFailure(pending, pendingByIndex, e)) {
        rollbackFailedBulkEnqueue(entry);
      }
      if (e instanceof ReconcileJobIndexStore.BulkEnqueueCommitException scoped
          && scoped.getCause() instanceof RuntimeException runtimeCause) {
        throw runtimeCause;
      }
      throw e;
    }
    java.util.Set<Integer> resolvedIndexes = new java.util.HashSet<>();
    for (BulkEnqueueItemResult storeResult : storeResults) {
      PendingBulkEnqueue entry = pendingByIndex.get(storeResult.index);
      if (entry == null) {
        continue;
      }
      resolvedIndexes.add(storeResult.index);
      rollbackFailedBulkEnqueue(entry);
      results.set(storeResult.index, storeResult);
      if (storeResult.succeeded()) {
        logEnqueueDeduped(entry, storeResult.jobId, "store");
      } else {
        logEnqueueFailed(entry, storeResult.error);
      }
    }
    for (PendingBulkEnqueue entry : pending) {
      if (resolvedIndexes.contains(entry.index)) {
        continue;
      }
      BulkEnqueueItemResult created =
          new BulkEnqueueItemResult(entry.index, entry.record.jobId, true, "");
      results.set(entry.index, created);
      logEnqueueCreated(entry, created.jobId);
    }

    return new BulkEnqueueResult(results);
  }

  private ReconcileJobIndexStore.QueuedJobInsert toQueuedInsert(PendingBulkEnqueue entry) {
    return new ReconcileJobIndexStore.QueuedJobInsert(
        entry.index,
        entry.dedupePointerKey,
        entry.canonicalKey,
        entry.lookupKey,
        entry.parentKey,
        entry.readyKeys,
        entry.stateKeys,
        entry.connectorIndexKey,
        entry.record);
  }

  private BulkEnqueueItemResult commitBulkEntry(PendingBulkEnqueue entry) {
    return jobIndexStore.commitQueuedJobInsert(
        new ReconcileJobIndexStore.QueuedJobInsert(
            entry.index,
            entry.dedupePointerKey,
            entry.canonicalKey,
            entry.lookupKey,
            entry.parentKey,
            entry.readyKeys,
            entry.stateKeys,
            entry.connectorIndexKey,
            entry.record));
  }

  private void logEnqueueCreated(PendingBulkEnqueue entry, String jobId) {
    LOG.infof(
        "enqueue committed jobId=%s parentJobId=%s kind=%s laneKey=%s dedupe=%s",
        blankToEmpty(jobId),
        blankToEmpty(entry.record.parentJobId),
        entry.record.jobKind,
        blankToEmpty(entry.record.laneKey),
        blankToEmpty(entry.dedupePointerKey));
  }

  private void logEnqueueDeduped(PendingBulkEnqueue entry, String existingJobId, String source) {
    LOG.infof(
        "enqueue deduped source=%s existingJobId=%s parentJobId=%s kind=%s laneKey=%s dedupe=%s",
        source,
        blankToEmpty(existingJobId),
        blankToEmpty(entry.record.parentJobId),
        entry.record.jobKind,
        blankToEmpty(entry.record.laneKey),
        blankToEmpty(entry.dedupePointerKey));
  }

  private void logEnqueueFailed(PendingBulkEnqueue entry, String failureReason) {
    LOG.warnf(
        "enqueue failed parentJobId=%s kind=%s laneKey=%s dedupe=%s reason=%s",
        blankToEmpty(entry.record.parentJobId),
        entry.record.jobKind,
        blankToEmpty(entry.record.laneKey),
        blankToEmpty(entry.dedupePointerKey),
        blankToEmpty(failureReason));
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
        materializeSnapshotPlanFileGroups.apply(effectiveSnapshotTask);
    ReconcileFileGroupTask effectiveFileGroupTask =
        spec.fileGroupTask == null ? ReconcileFileGroupTask.empty() : spec.fileGroupTask;
    String jobId = UUID.randomUUID().toString();
    if (effectiveJobKind == ReconcileJobKind.EXEC_FILE_GROUP) {
      LOG.infof(
          "enqueue EXEC_FILE_GROUP identity jobId=%s parentJobId=%s planId=%s groupId=%s tableId=%s snapshotId=%d fileCount=%d paths=%d",
          jobId,
          spec.parentJobId,
          effectiveFileGroupTask.planId(),
          effectiveFileGroupTask.groupId(),
          effectiveFileGroupTask.tableId(),
          effectiveFileGroupTask.snapshotId(),
          effectiveFileGroupTask.fileCount(),
          effectiveFileGroupTask.filePaths().size());
    }
    requireExplicitSnapshotCoverage(effectiveJobKind, effectiveSnapshotTask);
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
            spec.parentJobId);
    String dedupeKeyHash = hashValue(dedupeKey);
    String canonicalKey = Keys.reconcileJobStateRowById(spec.accountId, jobId);
    String snapshotPlanBlobUri =
        snapshotPlanFileGroups.isEmpty()
            ? ""
            : Keys.reconcileJobBlobUri(
                spec.accountId, jobId, "snapshot-plan-" + now + "-" + UUID.randomUUID());
    StoredJobDefinition definition =
        StoredJobDefinition.of(scope, effectiveTableTask, effectiveViewTask);
    StoredReconcileJob record =
        newQueuedRecord(
            jobId,
            spec.accountId,
            spec.connectorId,
            spec.fullRescan,
            spec.captureMode,
            effectiveJobKind,
            effectiveSnapshotTask,
            effectiveFileGroupTask,
            policy,
            spec.parentJobId,
            spec.pinnedExecutorId,
            laneKey,
            dedupeKeyHash,
            now,
            readyPointerKeyForDue.apply(spec.accountId, laneKey, jobId, now),
            indexes.connectorIndexPointerKey(spec.accountId, spec.connectorId, now, jobId),
            definition,
            snapshotPlanBlobUri);
    if (effectiveJobKind == ReconcileJobKind.PLAN_SNAPSHOT) {
      LOG.debugf(
          "enqueue persisted PLAN_SNAPSHOT jobId=%s parentJobId=%s connectorId=%s tableId=%s snapshotId=%d source=%s.%s fileGroups=%d",
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
        indexes.parentPointerKey(spec.accountId, spec.parentJobId, jobId),
        readyPointerKeys.apply(record),
        statePointerKeys.apply(record),
        record.connectorIndexPointerKey,
        snapshotPlanBlobUri,
        snapshotPlanBlob(snapshotPlanFileGroups),
        record);
  }

  private StoredReconcileJob newQueuedRecord(
      String jobId,
      String accountId,
      String connectorId,
      boolean fullRescan,
      CaptureMode captureMode,
      ReconcileJobKind jobKind,
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
      StoredJobDefinition definition,
      String snapshotPlanBlobUri) {
    StoredReconcileJob rec = new StoredReconcileJob();
    rec.jobId = jobId;
    rec.accountId = accountId;
    rec.connectorId = connectorId;
    rec.jobKind = (jobKind == null ? ReconcileJobKind.PLAN_CONNECTOR : jobKind).name();
    rec.parentJobId = parentJobId == null ? "" : parentJobId.trim();
    rec.fullRescan = fullRescan;
    rec.captureMode = Objects.requireNonNull(captureMode, "captureMode").name();
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
    rec.snapshotTaskFileGroupCount = effectiveSnapshotTask.fileGroupCount();
    rec.snapshotTaskSourceFileCount = effectiveSnapshotTask.sourceFileCount();
    rec.snapshotTaskDirectStatsBlobUri = blankToEmpty(effectiveSnapshotTask.directStatsBlobUri());
    rec.snapshotTaskDirectStatsRecordCount = effectiveSnapshotTask.directStatsRecordCount();
    rec.fileGroupPlanId = blankToEmpty(effectiveFileGroupTask.planId());
    rec.fileGroupGroupId = blankToEmpty(effectiveFileGroupTask.groupId());
    rec.fileGroupTableId = blankToEmpty(effectiveFileGroupTask.tableId());
    rec.fileGroupSnapshotId = effectiveFileGroupTask.snapshotId();
    rec.fileGroupFileCount = (int) plannedFilesForGroup(effectiveFileGroupTask);
    rec.definition = definition == null ? new StoredJobDefinition() : definition;
    rec.snapshotPlanBlobUri = snapshotPlanBlobUri;

    JobProjection initialProjection =
        switch (rec.jobKind()) {
          case PLAN_SNAPSHOT -> projector.projectSnapshotPlan(effectiveSnapshotTask);
          case EXEC_FILE_GROUP -> projector.projectExecFileGroup(effectiveFileGroupTask, null);
          default -> JobProjection.empty();
        };
    rec.indexesProcessed = initialProjection.indexesProcessed();
    rec.plannedFileGroups = initialProjection.plannedFileGroups();
    rec.plannedFiles = initialProjection.plannedFiles();
    rec.completedFileGroups = initialProjection.completedFileGroups();
    rec.failedFileGroups = initialProjection.failedFileGroups();
    rec.completedFiles = initialProjection.completedFiles();
    rec.failedFiles = initialProjection.failedFiles();
    rec.expectedDirectChildren = 0L;
    rec.childrenFinalized = false;
    rec.projectionRequestedGeneration = 0L;
    rec.projectionAppliedGeneration = 0L;
    rec.state = "JS_QUEUED";
    rec.message = fullRescan ? "Queued (full)" : "Queued";
    rec.nextAttemptAtMs = now;
    rec.attempt = 0;
    rec.laneKey = laneKey;
    rec.dedupeKeyHash = dedupeKeyHash;
    rec.readyPointerKey = readyPointerKey;
    rec.readyIndexVersion = ReconcileReadyIndexMaintenanceService.CURRENT_READY_INDEX_VERSION;
    rec.connectorIndexPointerKey = connectorIndexPointerKey;
    rec.createdAtMs = now;
    rec.updatedAtMs = now;
    return rec;
  }

  private void persistBulkPayloads(PendingBulkEnqueue entry) {
    if (!entry.snapshotPlanBlobUri.isBlank()) {
      payloadStore.writeBlob(
          entry.snapshotPlanBlobUri,
          entry.snapshotPlanPayload,
          "Failed to persist snapshot plan payload");
      entry.snapshotPlanWritten = true;
    }
  }

  private void rollbackFailedBulkEnqueue(PendingBulkEnqueue entry) {
    if (entry == null) {
      return;
    }
    if (entry.snapshotPlanWritten) {
      blobStore.delete(entry.snapshotPlanBlobUri);
    }
  }

  private List<PendingBulkEnqueue> rollbackEntriesForStoreFailure(
      List<PendingBulkEnqueue> pending,
      java.util.Map<Integer, PendingBulkEnqueue> pendingByIndex,
      RuntimeException failure) {
    if (!(failure instanceof ReconcileJobIndexStore.BulkEnqueueCommitException scoped)) {
      return pending;
    }
    List<PendingBulkEnqueue> rollback = new ArrayList<>();
    for (Integer index : scoped.rollbackIndexes()) {
      PendingBulkEnqueue entry = pendingByIndex.get(index);
      if (entry != null) {
        rollback.add(entry);
      }
    }
    return rollback;
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

  private static SnapshotPlanBlob snapshotPlanBlob(List<ReconcileFileGroupTask> fileGroups) {
    return SnapshotPlanBlob.of(
        (fileGroups == null ? List.<ReconcileFileGroupTask>of() : fileGroups)
            .stream()
                .filter(fileGroup -> fileGroup != null && !fileGroup.isEmpty())
                .map(
                    fileGroup ->
                        new ai.floedb.floecat.reconciler.impl.PlannedFileGroupJob(
                            ReconcileScope.empty(), fileGroup))
                .toList());
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
      String parentJobId) {
    String namespaces =
        scope.destinationNamespaceIds().stream().sorted().reduce((a, b) -> a + "," + b).orElse("*");
    String table = scope.destinationTableId() == null ? "*" : scope.destinationTableId();
    String captureRequests =
        scope.destinationCaptureRequests().stream()
            .map(ReconcileJobEnqueuer::canonicalCaptureRequest)
            .sorted()
            .reduce((a, b) -> a + "," + b)
            .orElse("");
    String capturePolicy = canonicalCapturePolicy(scope.capturePolicy());
    String snapshotSelection = canonicalSnapshotSelection(scope.snapshotSelection());
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
        .scalar("capture_mode", Objects.requireNonNull(captureMode, "captureMode").name())
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
                snapshotTask == null ? List.of() : snapshotTask.fileGroups(), true))
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
        .scalar("scope.snapshot_selection", snapshotSelection)
        .scalar("policy.execution_class", policy.executionClass().name())
        .scalar("policy.lane", policy.lane())
        .map("policy.attributes", policy.attributes())
        .scalar("parent_job_id", parentJobId == null ? "" : parentJobId.trim());
    return new String(canonicalizer.bytes(), StandardCharsets.UTF_8);
  }

  private static void requireExplicitSnapshotCoverage(
      ReconcileJobKind jobKind, ReconcileSnapshotTask snapshotTask) {
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
              + effective.filePaths().size());
    }
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

  private static String canonicalSnapshotSelection(ReconcileSnapshotSelection selection) {
    if (selection == null || !selection.isSpecified()) {
      return "";
    }
    return selection.kind().name()
        + "|"
        + selection.latestN()
        + "|"
        + selection.snapshotIds().stream()
            .map(String::valueOf)
            .sorted()
            .reduce((a, b) -> a + "," + b)
            .orElse("");
  }

  private static List<String> canonicalSnapshotFileGroups(
      List<ReconcileFileGroupTask> fileGroups, boolean includePlanId) {
    if (fileGroups == null || fileGroups.isEmpty()) {
      return List.of();
    }
    return fileGroups.stream()
        .filter(group -> group != null && !group.isEmpty())
        .map(
            group ->
                (includePlanId ? blankToEmpty(group.planId()) : "")
                    + "|"
                    + blankToEmpty(group.groupId())
                    + "|"
                    + blankToEmpty(group.tableId())
                    + "|"
                    + group.snapshotId()
                    + "|"
                    + String.join(",", canonicalFilePaths(group)))
        .sorted()
        .toList();
  }

  private static List<String> canonicalFilePaths(ReconcileFileGroupTask group) {
    if (group == null || group.filePaths() == null || group.filePaths().isEmpty()) {
      return List.of();
    }
    return group.filePaths().stream()
        .filter(path -> path != null && !path.isBlank())
        .map(String::trim)
        .sorted()
        .toList();
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value.trim();
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
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
    final String snapshotPlanBlobUri;
    final SnapshotPlanBlob snapshotPlanPayload;
    final StoredReconcileJob record;
    boolean snapshotPlanWritten;

    private PendingBulkEnqueue(
        int index,
        String dedupePointerKey,
        String canonicalKey,
        String lookupKey,
        String parentKey,
        List<String> readyKeys,
        List<String> stateKeys,
        String connectorIndexKey,
        String snapshotPlanBlobUri,
        SnapshotPlanBlob snapshotPlanPayload,
        StoredReconcileJob record) {
      this.index = index;
      this.dedupePointerKey = dedupePointerKey;
      this.canonicalKey = canonicalKey;
      this.lookupKey = lookupKey;
      this.parentKey = parentKey;
      this.readyKeys = readyKeys == null ? List.of() : List.copyOf(readyKeys);
      this.stateKeys = stateKeys == null ? List.of() : List.copyOf(stateKeys);
      this.connectorIndexKey = connectorIndexKey;
      this.snapshotPlanBlobUri = snapshotPlanBlobUri;
      this.snapshotPlanPayload = snapshotPlanPayload;
      this.record = record;
    }
  }
}
