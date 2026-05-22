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
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import org.jboss.logging.Logger;

@ApplicationScoped
public class ReconcileJobIndexes {
  private static final Logger LOG = Logger.getLogger(ReconcileJobIndexes.class);

  private PointerStore pointerStore;
  private int maxPendingRepairHints;
  private BiFunction<String, String, Boolean> upsertReferencePointer;
  private Function<String, Optional<StoredReconcileJob>> readCanonicalRecordByKey;
  private BiFunction<String, UnaryOperator<StoredReconcileJob>, Boolean> mutateByCanonicalPointer;
  private Predicate<StoredReconcileJob> requiresReadyPointer;
  private Function<StoredReconcileJob, List<String>> readyPointerKeys;
  private Function<StoredReconcileJob, Long> readyPointerDueAt;
  private BiFunction<StoredReconcileJob, Long, String> readyPointerKeyFor;

  private final ThreadLocal<Boolean> suppressInlineRepair = ThreadLocal.withInitial(() -> false);
  private final ConcurrentHashMap<String, RepairHint> pendingRepairHints =
      new ConcurrentHashMap<>();

  public void bind(
      PointerStore pointerStore,
      int maxPendingRepairHints,
      BiFunction<String, String, Boolean> upsertReferencePointer,
      Function<String, Optional<StoredReconcileJob>> readCanonicalRecordByKey,
      BiFunction<String, UnaryOperator<StoredReconcileJob>, Boolean> mutateByCanonicalPointer,
      Predicate<StoredReconcileJob> requiresReadyPointer,
      Function<StoredReconcileJob, List<String>> readyPointerKeys,
      Function<StoredReconcileJob, Long> readyPointerDueAt,
      BiFunction<StoredReconcileJob, Long, String> readyPointerKeyFor) {
    this.pointerStore = pointerStore;
    this.maxPendingRepairHints = maxPendingRepairHints;
    this.upsertReferencePointer = upsertReferencePointer;
    this.readCanonicalRecordByKey = readCanonicalRecordByKey;
    this.mutateByCanonicalPointer = mutateByCanonicalPointer;
    this.requiresReadyPointer = requiresReadyPointer;
    this.readyPointerKeys = readyPointerKeys;
    this.readyPointerDueAt = readyPointerDueAt;
    this.readyPointerKeyFor = readyPointerKeyFor;
  }

  public void onHotPath(Runnable runnable) {
    onHotPath(
        () -> {
          runnable.run();
          return null;
        });
  }

  public <T> T onHotPath(Supplier<T> supplier) {
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

  public boolean inlineRepairAllowed() {
    return !Boolean.TRUE.equals(suppressInlineRepair.get());
  }

  public RepairDisposition deferRepairIfHotPath(
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

  public void drainPendingRepairHintsForMaintenance(int maxHints, long maxMillis) {
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

  public int pendingRepairHintCount() {
    return pendingRepairHints.size();
  }

  public String parentPointerKey(String accountId, String parentJobId, String jobId) {
    if (blank(accountId) || blank(parentJobId) || blank(jobId)) {
      return "";
    }
    return Keys.reconcileJobByParentPointer(accountId, parentJobId, jobId);
  }

  public String connectorIndexPointerKey(
      String accountId, String connectorId, long createdAtMs, String jobId) {
    if (blank(accountId) || blank(connectorId) || blank(jobId)) {
      return "";
    }
    return Keys.reconcileJobByConnectorPointer(
        accountId, connectorId, connectorSortableJobToken(createdAtMs, jobId));
  }

  public String dedupePointerKey(StoredReconcileJob record) {
    if (record == null || blank(record.accountId) || blank(record.dedupeKeyHash)) {
      return "";
    }
    return Keys.reconcileDedupePointer(record.accountId, record.dedupeKeyHash);
  }

  public boolean hasValidLookupPointer(String jobId, String expectedReference) {
    if (blank(jobId) || blank(expectedReference)) {
      return false;
    }
    Pointer existing = pointerStore.get(Keys.reconcileJobLookupPointerById(jobId)).orElse(null);
    return existing != null && expectedReference.equals(existing.getBlobUri());
  }

  public boolean hasStateIndex(StoredReconcileJob record) {
    return record != null
        && !blank(record.state)
        && !blank(record.accountId)
        && !blank(record.jobId);
  }

  public String statePointerPrefix(String state) {
    return Keys.reconcileJobByStatePointerPrefix(state);
  }

  public String statePointerKey(StoredReconcileJob record) {
    if (!hasStateIndex(record)) {
      return "";
    }
    return Keys.reconcileJobByStatePointer(
        record.state, Math.max(0L, record.createdAtMs), record.accountId, record.jobId);
  }

  public List<String> statePointerKeys(StoredReconcileJob record) {
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

  public boolean hasValidStatePointer(StoredReconcileJob record, String canonicalPointerKey) {
    for (String pointerKey : statePointerKeys(record)) {
      Pointer existing = pointerStore.get(pointerKey).orElse(null);
      if (existing == null || !canonicalPointerKey.equals(existing.getBlobUri())) {
        return false;
      }
    }
    return true;
  }

  public boolean hasValidReadyPointers(StoredReconcileJob record, String expectedReference) {
    if (record == null || blank(expectedReference)) {
      return false;
    }
    List<String> currentReadyKeys = readyPointerKeys.apply(record);
    if (currentReadyKeys.isEmpty()) {
      return !requiresReadyPointer.test(record);
    }
    for (String readyKey : currentReadyKeys) {
      if (!hasValidReadyPointer(readyKey, expectedReference)) {
        return false;
      }
    }
    return true;
  }

  public boolean repairLookupPointer(String jobId, String canonicalPointerKey) {
    return repairHandled(repairLookupPointerDisposition(jobId, canonicalPointerKey, "", ""));
  }

  public boolean repairLookupPointer(
      String jobId, String canonicalPointerKey, String repairPhase, String repairReason) {
    return repairHandled(
        repairLookupPointerDisposition(jobId, canonicalPointerKey, repairPhase, repairReason));
  }

  public StoredReconcileJob repairCanonicalPointersIfNeeded(
      String canonicalPointerKey, StoredReconcileJob record) {
    return repairCanonicalPointersIfNeeded(canonicalPointerKey, record, "", "");
  }

  public StoredReconcileJob repairCanonicalPointersIfNeeded(
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
        requiresReadyPointer.test(record) && !hasValidReadyPointers(record, canonicalPointerKey);
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
    return readCanonicalRecordByKey.apply(canonicalPointerKey).orElse(record);
  }

  public boolean repairReadyPointer(String canonicalPointerKey, StoredReconcileJob record) {
    return repairHandled(repairReadyPointerDisposition(canonicalPointerKey, record, "", ""));
  }

  public boolean repairReadyPointer(
      String canonicalPointerKey,
      StoredReconcileJob record,
      String repairPhase,
      String repairReason) {
    return repairHandled(
        repairReadyPointerDisposition(canonicalPointerKey, record, repairPhase, repairReason));
  }

  public boolean repairStatePointer(String canonicalPointerKey, StoredReconcileJob record) {
    return repairHandled(repairStatePointerDisposition(canonicalPointerKey, record, "", ""));
  }

  public boolean repairStatePointer(
      String canonicalPointerKey,
      StoredReconcileJob record,
      String repairPhase,
      String repairReason) {
    return repairHandled(
        repairStatePointerDisposition(canonicalPointerKey, record, repairPhase, repairReason));
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
    if (pendingRepairHints.size() >= maxPendingRepairHints) {
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

  private void applyRepairHint(RepairHint hint) {
    if (hint == null || blank(hint.canonicalPointerKey())) {
      return;
    }
    var current = readCanonicalRecordByKey.apply(hint.canonicalPointerKey());
    if (current.isEmpty()) {
      return;
    }
    repairCanonicalPointersIfNeeded(
        hint.canonicalPointerKey(),
        current.get(),
        "deferred_repair:" + hint.repairPhase(),
        hint.repairReason());
  }

  private boolean hasValidReadyPointer(String readyPointerKey, String expectedReference) {
    if (blank(readyPointerKey) || blank(expectedReference)) {
      return false;
    }
    Pointer existing = pointerStore.get(readyPointerKey).orElse(null);
    return existing != null && expectedReference.equals(existing.getBlobUri());
  }

  private RepairDisposition repairLookupPointerDisposition(
      String jobId, String canonicalPointerKey, String repairPhase, String repairReason) {
    if (blank(jobId) || blank(canonicalPointerKey)) {
      return RepairDisposition.FAILED;
    }
    RepairDisposition deferred =
        deferRepairIfHotPath(
            repairPhase, repairReason, "lookup_pointer", canonicalPointerKey, jobId, "");
    if (deferred == RepairDisposition.DEFERRED || deferred == RepairDisposition.FAILED) {
      return deferred;
    }
    boolean repaired =
        upsertReferencePointer.apply(
            Keys.reconcileJobLookupPointerById(jobId), canonicalPointerKey);
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

  private RepairDisposition repairReadyPointerDisposition(
      String canonicalPointerKey,
      StoredReconcileJob record,
      String repairPhase,
      String repairReason) {
    if (blank(canonicalPointerKey) || record == null) {
      return RepairDisposition.FAILED;
    }
    if (!requiresReadyPointer.test(record)) {
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
    long dueAt = readyPointerDueAt.apply(record);
    String readyPointerKey = readyPointerKeyFor.apply(record, dueAt);
    boolean needsCanonicalRepair = !readyPointerKey.equals(blankToEmpty(record.readyPointerKey));
    record.readyPointerKey = readyPointerKey;
    boolean repaired = true;
    for (String readyKey : readyPointerKeys.apply(record)) {
      repaired &= upsertReferencePointer.apply(readyKey, canonicalPointerKey);
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

  private RepairDisposition repairStatePointerDisposition(
      String canonicalPointerKey,
      StoredReconcileJob record,
      String repairPhase,
      String repairReason) {
    if (blank(canonicalPointerKey) || record == null) {
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
      repaired &= upsertReferencePointer.apply(pointerKey, canonicalPointerKey);
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

  private void persistReadyPointerKeyIfChanged(String canonicalPointerKey, String readyPointerKey) {
    mutateByCanonicalPointer.apply(
        canonicalPointerKey,
        existing -> {
          if (!requiresReadyPointer.test(existing)) {
            return existing;
          }
          if (readyPointerKey.equals(blankToEmpty(existing.readyPointerKey))) {
            return existing;
          }
          existing.readyPointerKey = readyPointerKey;
          return existing;
        });
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
    return !blank(canonicalPointerKey) && record != null && requiresReadyPointer.test(record);
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
    return disposition != RepairDisposition.FAILED;
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

  private static String connectorSortableJobToken(long createdAtMs, String jobId) {
    long created = Math.max(0L, createdAtMs);
    long reversedCreated = Long.MAX_VALUE - created;
    return String.format("%019d-%s", reversedCreated, jobId);
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value.trim();
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }

  public enum RepairDisposition {
    REPAIRED,
    DEFERRED,
    FAILED,
    NOT_NEEDED
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
}
