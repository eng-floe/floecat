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

package ai.floedb.floecat.service.reconciler.jobs.durable.storage;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.queue.ReconcileReadyQueue;
import ai.floedb.floecat.service.reconciler.jobs.durable.queue.ReconcileReadyQueue.ReadyIndexType;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.storage.spi.PointerStore.CasDelete;
import ai.floedb.floecat.storage.spi.PointerStore.CasOp;
import ai.floedb.floecat.storage.spi.PointerStore.CasUpsert;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.UnaryOperator;
import org.jboss.logging.Logger;

@ApplicationScoped
public class ReconcileJobRepository {
  private static final Logger LOG = Logger.getLogger(ReconcileJobRepository.class);
  private static final long INVALID_ORDERED_POINTER_MS = -1L;

  public record CanonicalEnvelope(String canonicalPointerKey, StoredReconcileJob record) {}

  private PointerStore pointerStore;
  private ReconcilePayloadStore payloadStore;
  private ReconcileJobIndexes indexes;
  private ReconcileReadyQueue readyQueue;
  private int casMax;
  private BiConsumer<StoredReconcileJob, StoredReconcileJob> assertImmutableJobIdentityPreserved;
  private TriConsumer<StoredReconcileJob, StoredReconcileJob, String> logStateTransition;

  @FunctionalInterface
  public interface TriConsumer<A, B, C> {
    void accept(A a, B b, C c);
  }

  public void bind(
      PointerStore pointerStore,
      ReconcilePayloadStore payloadStore,
      ReconcileJobIndexes indexes,
      ReconcileReadyQueue readyQueue,
      int casMax,
      BiConsumer<StoredReconcileJob, StoredReconcileJob> assertImmutableJobIdentityPreserved,
      TriConsumer<StoredReconcileJob, StoredReconcileJob, String> logStateTransition) {
    this.pointerStore = pointerStore;
    this.payloadStore = payloadStore;
    this.indexes = indexes;
    this.readyQueue = readyQueue;
    this.casMax = casMax;
    this.assertImmutableJobIdentityPreserved = assertImmutableJobIdentityPreserved;
    this.logStateTransition = logStateTransition;
  }

  public Optional<CanonicalEnvelope> loadByAnyAccount(String jobId) {
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
    return Optional.of(new CanonicalEnvelope(canonicalPointerKey, current));
  }

  public Optional<CanonicalEnvelope> mutateByJobIdReturningRecord(
      String jobId, UnaryOperator<StoredReconcileJob> mutator) {
    var loaded = loadByAnyAccount(jobId);
    if (loaded.isEmpty()) {
      return Optional.empty();
    }
    return mutateByCanonicalPointerReturningRecord(loaded.get().canonicalPointerKey(), mutator);
  }

  public Optional<CanonicalEnvelope> mutateByCanonicalPointerReturningRecord(
      String canonicalPointerKey, UnaryOperator<StoredReconcileJob> mutator) {
    for (int i = 0; i < casMax; i++) {
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
      assertImmutableJobIdentityPreserved.accept(baseline, nextRecord);

      nextRecord.updatedAtMs = System.currentTimeMillis();
      nextRecord.canonicalPointerKey = canonicalPointerKey;

      Pointer nextPointer =
          Pointer.newBuilder()
              .setKey(canonicalPointerKey)
              .setBlobUri(payloadStore.encodeInlineJobState(nextRecord))
              .setVersion(currentPointer.getVersion() + 1)
              .build();

      List<CasOp> pointerOps =
          reconcilePointerBatchOps(
              canonicalPointerKey, currentPointer, baseline, nextRecord, nextPointer);
      if (pointerStore.compareAndSetBatch(pointerOps)) {
        logStateTransition.accept(baseline, nextRecord, "mutate");
        return Optional.of(
            new CanonicalEnvelope(canonicalPointerKey, cloneStoredRecord(nextRecord)));
      }
    }
    return Optional.empty();
  }

  public Optional<StoredReconcileJob> readCanonicalRecordByKey(String canonicalPointerKey) {
    if (blank(canonicalPointerKey)) {
      return Optional.empty();
    }
    Pointer canonicalPointer = pointerStore.get(canonicalPointerKey).orElse(null);
    return canonicalPointer == null ? Optional.empty() : readRecord(canonicalPointer);
  }

  public Optional<StoredReconcileJob> readRecord(Pointer canonicalPointer) {
    var record = payloadStore.readInlineJobState(canonicalPointer.getBlobUri());
    if (record.isEmpty()) {
      return Optional.empty();
    }
    record.get().canonicalPointerKey = canonicalPointer.getKey();
    return record;
  }

  public void clearDedupeIfOwned(StoredReconcileJob record) {
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

  public void clearReadyPointersIfOwned(StoredReconcileJob record, String canonicalPointerKey) {
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

  public void clearPointerIfMatches(String pointerKey, String expectedReference) {
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

  public boolean deletePointerIfPresent(String pointerKey) {
    Pointer existing = pointerStore.get(pointerKey).orElse(null);
    return existing != null && pointerStore.compareAndDelete(pointerKey, existing.getVersion());
  }

  public void clearPointerIfOwnedByJob(String pointerKey, StoredReconcileJob ownerRecord) {
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

  public List<CasOp> reconcilePointerBatchOps(
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
        previous == null ? List.of() : indexes.statePointerKeys(previous);
    List<String> currentStatePointerKeys = indexes.statePointerKeys(current);
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
    List<String> currentReadyPointerKeys = readyQueue.readyPointerKeys(current);
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

    String previousDedupePointerKey = previous == null ? "" : indexes.dedupePointerKey(previous);
    String currentDedupePointerKey = indexes.dedupePointerKey(current);
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

  public StoredReconcileJob cloneStoredRecord(StoredReconcileJob source) {
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
            Pointer.newBuilder()
                .setKey(pointerKey)
                .setBlobUri(reference)
                .setVersion(expectedVersion + 1L)
                .build()));
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

  private List<String> readyPointerKeysForCleanup(StoredReconcileJob record) {
    if (record == null) {
      return List.of();
    }
    java.util.LinkedHashSet<String> readyKeys =
        new java.util.LinkedHashSet<>(readyQueue.readyPointerKeys(record));
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
        readyKeys.add(readyQueue.readyPointerKeyFor(record, dueAtMs));
        readyKeys.add(
            readyQueue.readyPointerKeyFor(
                record,
                ReadyIndexType.EXECUTION_CLASS,
                dueAtMs,
                executionPolicy.executionClass().name()));
        readyKeys.add(
            readyQueue.readyPointerKeyFor(
                record, ReadyIndexType.EXECUTION_LANE, dueAtMs, executionPolicy.lane()));
        if (!blank(record.pinnedExecutorId())) {
          readyKeys.add(
              readyQueue.readyPointerKeyFor(
                  record, ReadyIndexType.PINNED_EXECUTOR, dueAtMs, record.pinnedExecutorId()));
        }
        readyKeys.add(
            readyQueue.readyPointerKeyFor(
                record, ReadyIndexType.JOB_KIND, dueAtMs, record.jobKind().name()));
      }
    }
    readyKeys.removeIf(ReconcileJobRepository::blank);
    return List.copyOf(readyKeys);
  }

  private long parseDueMillis(String readyPointerKey) {
    return parseTimestampFromOrderedPointer(readyPointerKey, Keys.reconcileReadyPointerPrefix());
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

  private static String normalizePointerKey(String key) {
    if (key == null || key.isBlank()) {
      return "/";
    }
    return key.startsWith("/") ? key : "/" + key;
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value.trim();
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
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
}
