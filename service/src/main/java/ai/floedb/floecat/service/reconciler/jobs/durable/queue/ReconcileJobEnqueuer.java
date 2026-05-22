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

import ai.floedb.floecat.common.rpc.Pointer;
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
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import ai.floedb.floecat.service.common.Canonicalizer;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredFileGroupPlanPayload;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredJobDefinition;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.projection.ReconcileJobProjector;
import ai.floedb.floecat.service.reconciler.jobs.durable.projection.ReconcileJobProjector.JobProjection;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileJobIndexes;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcilePayloadStore;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.storage.spi.PointerStore.CasOp;
import ai.floedb.floecat.storage.spi.PointerStore.CasUpsert;
import jakarta.enterprise.context.ApplicationScoped;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.jboss.logging.Logger;

@ApplicationScoped
public class ReconcileJobEnqueuer {
  private static final Logger LOG = Logger.getLogger(ReconcileJobEnqueuer.class);

  @FunctionalInterface
  public interface LoadActiveFromDedupe {
    Optional<StoredReconcileJob> load(String dedupePointerKey);
  }

  @FunctionalInterface
  public interface ResultPayloadWriter {
    String write(String accountId, String jobId, ReconcileFileGroupTask fileGroupTask);
  }

  @FunctionalInterface
  public interface ChildCounterIncrementer {
    void increment(String parentJobId, int delta);
  }

  private PointerStore pointerStore;
  private BlobStore blobStore;
  private ReconcilePayloadStore payloadStore;
  private ReconcileJobProjector projector;
  private ReconcileJobIndexes indexes;
  private Function<ReconcileSnapshotTask, List<ReconcileFileGroupTask>>
      materializeSnapshotPlanFileGroups;
  private Function<StoredReconcileJob, List<String>> readyPointerKeys;
  private Function<StoredReconcileJob, List<String>> statePointerKeys;
  private ReadyPointerKeyForDue readyPointerKeyForDue;
  private LoadActiveFromDedupe loadActiveFromDedupe;
  private ResultPayloadWriter writeFileGroupResultPayload;
  private ChildCounterIncrementer incrementExpectedChildJobs;
  private BiConsumer<StoredReconcileJob, Boolean> refreshAncestorContributionRollups;
  private int casMax;

  @FunctionalInterface
  public interface ReadyPointerKeyForDue {
    String apply(String accountId, String laneKey, String jobId, long dueAtMs);
  }

  public void bind(
      PointerStore pointerStore,
      BlobStore blobStore,
      ReconcilePayloadStore payloadStore,
      ReconcileJobProjector projector,
      ReconcileJobIndexes indexes,
      Function<ReconcileSnapshotTask, List<ReconcileFileGroupTask>>
          materializeSnapshotPlanFileGroups,
      Function<StoredReconcileJob, List<String>> readyPointerKeys,
      Function<StoredReconcileJob, List<String>> statePointerKeys,
      ReadyPointerKeyForDue readyPointerKeyForDue,
      LoadActiveFromDedupe loadActiveFromDedupe,
      ResultPayloadWriter writeFileGroupResultPayload,
      ChildCounterIncrementer incrementExpectedChildJobs,
      BiConsumer<StoredReconcileJob, Boolean> refreshAncestorContributionRollups,
      int casMax) {
    this.pointerStore = pointerStore;
    this.blobStore = blobStore;
    this.payloadStore = payloadStore;
    this.projector = projector;
    this.indexes = indexes;
    this.materializeSnapshotPlanFileGroups = materializeSnapshotPlanFileGroups;
    this.readyPointerKeys = readyPointerKeys;
    this.statePointerKeys = statePointerKeys;
    this.readyPointerKeyForDue = readyPointerKeyForDue;
    this.loadActiveFromDedupe = loadActiveFromDedupe;
    this.writeFileGroupResultPayload = writeFileGroupResultPayload;
    this.incrementExpectedChildJobs = incrementExpectedChildJobs;
    this.refreshAncestorContributionRollups = refreshAncestorContributionRollups;
    this.casMax = casMax;
  }

  public BulkEnqueueResult bulkEnqueue(List<BulkEnqueueSpec> specs) {
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
      incrementExpectedChildJobs.increment(parentEntry.getKey(), parentEntry.getValue());
      PendingBulkEnqueue representative = representativeByParent.get(parentEntry.getKey());
      if (representative != null) {
        refreshAncestorContributionRollups.accept(representative.record, false);
      }
    }

    return new BulkEnqueueResult(results);
  }

  private BulkEnqueueItemResult commitBulkEntry(PendingBulkEnqueue entry) {
    for (int attempt = 0; attempt < casMax; attempt++) {
      Pointer existingDedupePointer = pointerStore.get(entry.dedupePointerKey).orElse(null);
      var existing = loadActiveFromDedupe.load(entry.dedupePointerKey);
      if (existing.isPresent()) {
        return new BulkEnqueueItemResult(entry.index, existing.get().jobId, false, "");
      }
      if (pointerStore.compareAndSetBatch(bulkEnqueuePointerOps(entry, existingDedupePointer))) {
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
        materializeSnapshotPlanFileGroups.apply(effectiveSnapshotTask);
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
            definitionBlobUri,
            snapshotPlanBlobUri,
            fileGroupPlanBlobUri);
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
        definitionBlobUri,
        snapshotPlanBlobUri,
        fileGroupPlanBlobUri,
        StoredJobDefinition.of(scope, effectiveTableTask, effectiveViewTask),
        snapshotPlanBlob(snapshotPlanFileGroups),
        StoredFileGroupPlanPayload.of(effectiveFileGroupTask),
        effectiveFileGroupTask.fileResults().isEmpty()
            ? ReconcileFileGroupTask.empty()
            : effectiveFileGroupTask,
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

  private void persistBulkPayloads(PendingBulkEnqueue entry) {
    payloadStore.writeBlob(
        entry.definitionBlobUri, entry.definition, "Failed to persist reconcile job definition");
    entry.definitionWritten = true;
    if (!entry.snapshotPlanBlobUri.isBlank()) {
      payloadStore.writeBlob(
          entry.snapshotPlanBlobUri,
          entry.snapshotPlanPayload,
          "Failed to persist snapshot plan payload");
      entry.snapshotPlanWritten = true;
    }
    if (!entry.fileGroupPlanBlobUri.isBlank()) {
      payloadStore.writeBlob(
          entry.fileGroupPlanBlobUri,
          entry.fileGroupPlanPayload,
          "Failed to persist file group plan payload");
      entry.fileGroupPlanWritten = true;
    }
    if (!entry.resultPayloadTask.isEmpty()) {
      entry.resultBlobUri =
          writeFileGroupResultPayload.write(
              entry.record.accountId, entry.record.jobId, entry.resultPayloadTask);
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
    if (entry.resultBlobUri != null && !entry.resultBlobUri.isBlank()) {
      blobStore.delete(entry.resultBlobUri);
    }
  }

  private List<CasOp> bulkEnqueuePointerOps(
      PendingBulkEnqueue entry, Pointer existingDedupePointer) {
    List<CasOp> ops = new ArrayList<>();
    long dedupeExpectedVersion =
        existingDedupePointer == null ? 0L : existingDedupePointer.getVersion();
    long dedupeNextVersion =
        existingDedupePointer == null ? 1L : existingDedupePointer.getVersion() + 1L;
    ops.add(
        new CasUpsert(
            entry.dedupePointerKey,
            dedupeExpectedVersion,
            Pointer.newBuilder()
                .setKey(entry.dedupePointerKey)
                .setBlobUri(entry.canonicalKey)
                .setVersion(dedupeNextVersion)
                .build()));
    ops.add(
        new CasUpsert(
            entry.canonicalKey,
            0L,
            Pointer.newBuilder()
                .setKey(entry.canonicalKey)
                .setBlobUri(payloadStore.encodeInlineJobState(entry.record))
                .setVersion(1L)
                .build()));
    ops.add(
        new CasUpsert(
            entry.lookupKey,
            0L,
            Pointer.newBuilder()
                .setKey(entry.lookupKey)
                .setBlobUri(entry.canonicalKey)
                .setVersion(1L)
                .build()));
    if (!entry.parentKey.isBlank()) {
      ops.add(
          new CasUpsert(
              entry.parentKey,
              0L,
              Pointer.newBuilder()
                  .setKey(entry.parentKey)
                  .setBlobUri(entry.canonicalKey)
                  .setVersion(1L)
                  .build()));
    }
    if (!entry.connectorIndexKey.isBlank()) {
      ops.add(
          new CasUpsert(
              entry.connectorIndexKey,
              0L,
              Pointer.newBuilder()
                  .setKey(entry.connectorIndexKey)
                  .setBlobUri(entry.canonicalKey)
                  .setVersion(1L)
                  .build()));
    }
    for (String stateKey : entry.stateKeys) {
      ops.add(
          new CasUpsert(
              stateKey,
              0L,
              Pointer.newBuilder()
                  .setKey(stateKey)
                  .setBlobUri(entry.canonicalKey)
                  .setVersion(1L)
                  .build()));
    }
    for (String readyKey : entry.readyKeys) {
      ops.add(
          new CasUpsert(
              readyKey,
              0L,
              Pointer.newBuilder()
                  .setKey(readyKey)
                  .setBlobUri(entry.canonicalKey)
                  .setVersion(1L)
                  .build()));
    }
    if (entry.resultBlobUri != null && !entry.resultBlobUri.isBlank()) {
      String resultPointerKey =
          Keys.reconcileJobResultPointerById(entry.record.accountId, entry.record.jobId);
      ops.add(
          new CasUpsert(
              resultPointerKey,
              0L,
              Pointer.newBuilder()
                  .setKey(resultPointerKey)
                  .setBlobUri(entry.resultBlobUri)
                  .setVersion(1L)
                  .build()));
    }
    return ops;
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
      String parentJobId,
      String pinnedExecutorId) {
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
              + effective.filePaths().size()
              + " results="
              + effective.fileResults().size());
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
    final String definitionBlobUri;
    final String snapshotPlanBlobUri;
    final String fileGroupPlanBlobUri;
    final StoredJobDefinition definition;
    final SnapshotPlanBlob snapshotPlanPayload;
    final StoredFileGroupPlanPayload fileGroupPlanPayload;
    final ReconcileFileGroupTask resultPayloadTask;
    final StoredReconcileJob record;
    boolean definitionWritten;
    boolean snapshotPlanWritten;
    boolean fileGroupPlanWritten;
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
        SnapshotPlanBlob snapshotPlanPayload,
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
}
