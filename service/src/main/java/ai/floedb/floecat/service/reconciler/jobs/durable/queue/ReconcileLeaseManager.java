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
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.LeasedJob;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredJobDefinition;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredJobLease;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcilePayloadStore;
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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import org.jboss.logging.Logger;

@ApplicationScoped
public class ReconcileLeaseManager {
  private static final Logger LOG = Logger.getLogger(ReconcileLeaseManager.class);
  private static final String LEASE_EXPIRY_POINTER_PREFIX =
      "/accounts/by-id/reconcile/job-leases/by-expiry/";

  private PointerStore pointerStore;
  private ReconcilePayloadStore payloadStore;
  private int casMax;
  private long leaseMs;
  private long leaseRenewGraceMs;
  private Function<String, Optional<StoredReconcileJob>> readCanonicalRecordByKey;
  private Function<Pointer, Optional<StoredReconcileJob>> readRecord;
  private BiFunction<String, String, Boolean> upsertReferencePointer;
  private BiFunction<Pointer, Predicate<StoredReconcileJob>, Optional<StoredReconcileJob>>
      readCurrentRecordFromStateIndexPointer;
  private BiFunction<String, UnaryOperator<StoredReconcileJob>, Boolean> mutateByCanonicalPointer;
  private BiPredicate<String, String> hasValidLookupPointer;
  private BiFunction<String, String, Boolean> repairLookupPointer;
  private Function<StoredReconcileJob, Boolean> requiresReadyPointer;
  private BiPredicate<StoredReconcileJob, String> hasValidReadyPointers;
  private BiFunction<String, StoredReconcileJob, Boolean> repairReadyPointer;
  private BiFunction<StoredReconcileJob, Long, String> readyPointerKeyFor;
  private Consumer<StoredReconcileJob> refreshContributionChain;
  private Function<StoredReconcileJob, StoredReconcileJob> cloneStoredRecord;
  private Predicate<String> isTerminalState;
  private Consumer<StoredReconcileJob> clearDedupeIfOwned;
  private BiConsumer<StoredReconcileJob, String> clearReadyPointersIfOwned;
  private Consumer<String> repairReadyPointersIfStillCurrent;
  private BiConsumer<StoredReconcileJob, StoredReconcileJob> assertImmutableJobIdentityPreserved;
  private CanonicalPointerBatchBuilder reconcilePointerBatchOps;

  @FunctionalInterface
  public interface CanonicalPointerBatchBuilder {
    List<CasOp> build(
        String canonicalPointerKey,
        Pointer currentPointer,
        StoredReconcileJob previous,
        StoredReconcileJob current,
        Pointer nextPointer);
  }

  public void bind(
      PointerStore pointerStore,
      ReconcilePayloadStore payloadStore,
      int casMax,
      long leaseMs,
      long leaseRenewGraceMs,
      Function<String, Optional<StoredReconcileJob>> readCanonicalRecordByKey,
      Function<Pointer, Optional<StoredReconcileJob>> readRecord,
      BiFunction<String, String, Boolean> upsertReferencePointer,
      BiFunction<Pointer, Predicate<StoredReconcileJob>, Optional<StoredReconcileJob>>
          readCurrentRecordFromStateIndexPointer,
      BiFunction<String, UnaryOperator<StoredReconcileJob>, Boolean> mutateByCanonicalPointer,
      BiPredicate<String, String> hasValidLookupPointer,
      BiFunction<String, String, Boolean> repairLookupPointer,
      Function<StoredReconcileJob, Boolean> requiresReadyPointer,
      BiPredicate<StoredReconcileJob, String> hasValidReadyPointers,
      BiFunction<String, StoredReconcileJob, Boolean> repairReadyPointer,
      BiFunction<StoredReconcileJob, Long, String> readyPointerKeyFor,
      Consumer<StoredReconcileJob> refreshContributionChain,
      Function<StoredReconcileJob, StoredReconcileJob> cloneStoredRecord,
      Predicate<String> isTerminalState,
      Consumer<StoredReconcileJob> clearDedupeIfOwned,
      BiConsumer<StoredReconcileJob, String> clearReadyPointersIfOwned,
      Consumer<String> repairReadyPointersIfStillCurrent,
      BiConsumer<StoredReconcileJob, StoredReconcileJob> assertImmutableJobIdentityPreserved,
      CanonicalPointerBatchBuilder reconcilePointerBatchOps) {
    this.pointerStore = pointerStore;
    this.payloadStore = payloadStore;
    this.casMax = casMax;
    this.leaseMs = leaseMs;
    this.leaseRenewGraceMs = leaseRenewGraceMs;
    this.readCanonicalRecordByKey = readCanonicalRecordByKey;
    this.readRecord = readRecord;
    this.upsertReferencePointer = upsertReferencePointer;
    this.readCurrentRecordFromStateIndexPointer = readCurrentRecordFromStateIndexPointer;
    this.mutateByCanonicalPointer = mutateByCanonicalPointer;
    this.hasValidLookupPointer = hasValidLookupPointer;
    this.repairLookupPointer = repairLookupPointer;
    this.requiresReadyPointer = requiresReadyPointer;
    this.hasValidReadyPointers = hasValidReadyPointers;
    this.repairReadyPointer = repairReadyPointer;
    this.readyPointerKeyFor = readyPointerKeyFor;
    this.refreshContributionChain = refreshContributionChain;
    this.cloneStoredRecord = cloneStoredRecord;
    this.isTerminalState = isTerminalState;
    this.clearDedupeIfOwned = clearDedupeIfOwned;
    this.clearReadyPointersIfOwned = clearReadyPointersIfOwned;
    this.repairReadyPointersIfStillCurrent = repairReadyPointersIfStillCurrent;
    this.assertImmutableJobIdentityPreserved = assertImmutableJobIdentityPreserved;
    this.reconcilePointerBatchOps = reconcilePointerBatchOps;
  }

  public Optional<LeasedJob> leaseCanonical(
      String canonicalPointerKey,
      String readyPointerKey,
      long now,
      Pointer initialPointer,
      StoredReconcileJob initialRecord) {
    for (int i = 0; i < casMax; i++) {
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

        var recordOpt = readRecord.apply(currentPointer);
        if (recordOpt.isEmpty()) {
          return Optional.empty();
        }
        record = recordOpt.get();
      }
      StoredReconcileJob baseline = cloneStoredRecord.apply(record);
      StoredReconcileJob current = cloneStoredRecord.apply(record);

      if (Boolean.TRUE.equals(isTerminalState.test(current.state))) {
        clearDedupeIfOwned.accept(current);
        return Optional.empty();
      }

      if ("JS_WAITING".equals(current.state)) {
        clearReadyPointersIfOwned.accept(current, canonicalPointerKey);
        return Optional.empty();
      }

      if (hasLiveLease(current, true, now)) {
        return Optional.empty();
      }

      if ("JS_CANCELLING".equals(current.state)) {
        return Optional.empty();
      }

      if (!tryAcquireSnapshotLease(current, canonicalPointerKey, now)) {
        repairReadyPointersIfStillCurrent.accept(canonicalPointerKey);
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
        assertImmutableJobIdentityPreserved.accept(baseline, current);

        Pointer nextPointer =
            Pointer.newBuilder()
                .setKey(canonicalPointerKey)
                .setBlobUri(payloadStore.encodeInlineJobState(current))
                .setVersion(currentPointer.getVersion() + 1)
                .build();

        String leasePointerKey =
            Keys.reconcileJobLeasePointerById(current.accountId, current.jobId);
        Pointer currentLeasePointer = pointerStore.get(leasePointerKey).orElse(null);
        StoredJobLease currentLease =
            currentLeasePointer == null
                ? StoredJobLease.empty(current.accountId, current.jobId)
                : payloadStore
                    .readInlineJobLease(currentLeasePointer.getBlobUri())
                    .orElse(StoredJobLease.empty(current.accountId, current.jobId));
        if (currentLease.epoch != null
            && !currentLease.epoch.isBlank()
            && currentLease.expiresAtMs > now) {
          repairReadyPointersIfStillCurrent.accept(canonicalPointerKey);
          return Optional.empty();
        }
        StoredJobLease nextLease =
            StoredJobLease.active(current.accountId, current.jobId, nextLeaseEpoch, now + leaseMs);

        long mutationStartMs = System.currentTimeMillis();
        List<CasOp> pointerOps =
            new ArrayList<>(
                reconcilePointerBatchOps.build(
                    canonicalPointerKey, currentPointer, baseline, current, nextPointer));
        pointerOps.addAll(
            buildLeasePointerBatchOps(
                leasePointerKey, currentLeasePointer, currentLease, nextLease));
        if (pointerStore.compareAndSetBatch(pointerOps)) {
          releaseSnapshotLease = false;
          try {
            long mutationElapsedMs = System.currentTimeMillis() - mutationStartMs;
            long definitionStartMs = System.currentTimeMillis();
            StoredJobDefinition definition = payloadStore.requireDefinition(current);
            long definitionElapsedMs = System.currentTimeMillis() - definitionStartMs;
            long snapshotTaskElapsedMs = 0L;
            ReconcileSnapshotTask snapshotTask = ReconcileSnapshotTask.empty();
            if (current.jobKind() == ReconcileJobKind.PLAN_SNAPSHOT
                || current.jobKind() == ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE) {
              long snapshotTaskStartMs = System.currentTimeMillis();
              snapshotTask = payloadStore.snapshotTaskFor(current);
              snapshotTaskElapsedMs = System.currentTimeMillis() - snapshotTaskStartMs;
            }
            long fileGroupTaskElapsedMs = 0L;
            ReconcileFileGroupTask fileGroupTask = ReconcileFileGroupTask.empty();
            if (current.jobKind() == ReconcileJobKind.EXEC_FILE_GROUP) {
              long fileGroupTaskStartMs = System.currentTimeMillis();
              fileGroupTask = payloadStore.fileGroupTaskFor(current);
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

  public void rollbackLeaseCanonicalOnHydrationFailure(
      String canonicalPointerKey, StoredReconcileJob baseline, String leaseEpoch) {
    if (baseline == null || blank(canonicalPointerKey) || blank(leaseEpoch)) {
      return;
    }
    clearLeaseIfEpochMatches(baseline.accountId, baseline.jobId, leaseEpoch);
    clearLaneLeaseIfOwned(baseline, canonicalPointerKey);
    clearSnapshotLeaseIfOwned(baseline, canonicalPointerKey);
    mutateByCanonicalPointer.apply(
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

  public boolean hasActiveLease(
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

  public boolean hasLiveLease(StoredReconcileJob record, boolean allowCancelling, long now) {
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

  public boolean hasUnexpiredJobLease(String accountId, String jobId, long now) {
    if (blank(accountId) || blank(jobId)) {
      return false;
    }
    StoredJobLease lease = loadLease(accountId, jobId).orElse(null);
    return lease != null
        && lease.epoch != null
        && !lease.epoch.isBlank()
        && lease.expiresAtMs > now;
  }

  public Optional<StoredJobLease> loadLease(String accountId, String jobId) {
    if (blank(accountId) || blank(jobId)) {
      return Optional.empty();
    }
    String pointerKey = Keys.reconcileJobLeasePointerById(accountId, jobId);
    Pointer leasePointer = pointerStore.get(pointerKey).orElse(null);
    if (leasePointer == null) {
      return Optional.empty();
    }
    return payloadStore.readInlineJobLease(leasePointer.getBlobUri());
  }

  public Optional<StoredJobLease> loadLease(StoredReconcileJob state) {
    if (state == null) {
      return Optional.empty();
    }
    return loadLease(state.accountId, state.jobId);
  }

  public Optional<StoredJobLease> writeLeaseIfAbsentOrExpired(
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

  public Optional<StoredJobLease> mutateLease(
      String accountId, String jobId, UnaryOperator<StoredJobLease> mutator) {
    return writeLease(accountId, jobId, mutator);
  }

  public Optional<StoredJobLease> renewLeaseIfEpochMatches(
      String accountId, String jobId, String leaseEpoch) {
    return writeLease(
        accountId,
        jobId,
        current -> {
          if (blank(leaseEpoch)) {
            return null;
          }
          long now = System.currentTimeMillis();
          long expiry = current.expiresAtMs;
          if (blank(current.epoch) || !leaseEpoch.equals(current.epoch)) {
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

  public boolean clearLeaseIfEpochMatches(String accountId, String jobId, String leaseEpoch) {
    if (blank(accountId) || blank(jobId)) {
      return false;
    }
    String pointerKey = Keys.reconcileJobLeasePointerById(accountId, jobId);
    for (int i = 0; i < casMax; i++) {
      Pointer currentPointer = pointerStore.get(pointerKey).orElse(null);
      if (currentPointer == null) {
        return false;
      }
      StoredJobLease current =
          payloadStore
              .readInlineJobLease(currentPointer.getBlobUri())
              .orElse(StoredJobLease.empty(accountId, jobId));
      if (blank(leaseEpoch) || !leaseEpoch.equals(current.epoch)) {
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

  public boolean tryAcquireLaneLease(
      StoredReconcileJob record, String canonicalPointerKey, long now) {
    if (record == null
        || blank(record.accountId)
        || blank(record.laneKey)
        || blank(canonicalPointerKey)) {
      return false;
    }
    String lanePointerKey = Keys.reconcileLaneLeasePointer(record.accountId, record.laneKey);
    for (int i = 0; i < casMax; i++) {
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
        var owner = readCanonicalRecordByKey.apply(existing.getBlobUri());
        if (owner.isPresent() && hasActiveLaneLease(owner.get(), now)) {
          return false;
        }
        if (upsertReferencePointer.apply(lanePointerKey, canonicalPointerKey)) {
          return true;
        }
        continue;
      }

      var owner = readCanonicalRecordByKey.apply(existing.getBlobUri());
      if (owner.isPresent()
          && record.jobId.equals(owner.get().jobId)
          && record.accountId.equals(owner.get().accountId)) {
        String ownerCanonicalKey =
            Keys.reconcileJobStateRowById(owner.get().accountId, owner.get().jobId);
        if (!ownerCanonicalKey.equals(existing.getBlobUri())) {
          if (upsertReferencePointer.apply(lanePointerKey, ownerCanonicalKey)) {
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

  public void clearLaneLeaseIfOwned(StoredReconcileJob record, String expectedReference) {
    if (record == null
        || blank(record.accountId)
        || blank(record.laneKey)
        || blank(expectedReference)) {
      return;
    }
    String lanePointerKey = Keys.reconcileLaneLeasePointer(record.accountId, record.laneKey);
    Pointer existing = pointerStore.get(lanePointerKey).orElse(null);
    if (existing == null) {
      return;
    }
    if (!expectedReference.equals(existing.getBlobUri())) {
      var owner = readCanonicalRecordByKey.apply(existing.getBlobUri());
      if (owner.isEmpty()
          || !record.jobId.equals(owner.get().jobId)
          || !record.accountId.equals(owner.get().accountId)) {
        return;
      }
    }
    var owner = readCanonicalRecordByKey.apply(existing.getBlobUri());
    if (owner.isEmpty()
        || !record.jobId.equals(owner.get().jobId)
        || !record.accountId.equals(owner.get().accountId)) {
      return;
    }
    if (hasActiveLaneLease(owner.get(), System.currentTimeMillis())) {
      String canonicalKey = Keys.reconcileJobStateRowById(owner.get().accountId, owner.get().jobId);
      if (!canonicalKey.equals(existing.getBlobUri())) {
        upsertReferencePointer.apply(lanePointerKey, canonicalKey);
      }
      return;
    }
    pointerStore.compareAndDelete(lanePointerKey, existing.getVersion());
  }

  public boolean tryAcquireSnapshotLease(
      StoredReconcileJob record, String expectedReference, long now) {
    String pointerKey = snapshotLeasePointerKey(record);
    if (pointerKey.isBlank()) {
      return true;
    }
    for (int i = 0; i < casMax; i++) {
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
      var owner = readCanonicalRecordByKey.apply(existing.getBlobUri());
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

  public void clearSnapshotLeaseIfOwned(StoredReconcileJob record, String expectedReference) {
    String pointerKey = snapshotLeasePointerKey(record);
    if (pointerKey.isBlank()) {
      return;
    }
    Pointer existing = pointerStore.get(pointerKey).orElse(null);
    if (existing == null) {
      return;
    }
    if (!blank(expectedReference) && !expectedReference.equals(existing.getBlobUri())) {
      var owner = readCanonicalRecordByKey.apply(existing.getBlobUri());
      if (owner.isEmpty()
          || !record.jobId.equals(owner.get().jobId)
          || !record.accountId.equals(owner.get().accountId)) {
        return;
      }
    }
    var owner = readCanonicalRecordByKey.apply(existing.getBlobUri());
    if (owner.isEmpty()
        || !record.jobId.equals(owner.get().jobId)
        || !record.accountId.equals(owner.get().accountId)) {
      return;
    }
    if (hasActiveSnapshotLease(owner.get(), System.currentTimeMillis())) {
      String canonicalKey = Keys.reconcileJobStateRowById(owner.get().accountId, owner.get().jobId);
      if (!canonicalKey.equals(existing.getBlobUri())) {
        upsertReferencePointer.apply(pointerKey, canonicalKey);
      }
      return;
    }
    pointerStore.compareAndDelete(pointerKey, existing.getVersion());
  }

  public String leaseExpiryPointerKey(StoredJobLease lease) {
    if (lease == null) {
      return "";
    }
    return leaseExpiryPointerKey(lease.expiresAtMs, lease.accountId, lease.jobId);
  }

  public String leaseExpiryPointerKey(long expiresAtMs, String accountId, String jobId) {
    if (expiresAtMs <= 0L || blank(accountId) || blank(jobId)) {
      return "";
    }
    return LEASE_EXPIRY_POINTER_PREFIX
        + String.format("%019d", expiresAtMs)
        + "/accounts/"
        + accountId
        + "/jobs/"
        + jobId;
  }

  public List<CasOp> buildLeasePointerBatchOps(
      String leasePointerKey,
      Pointer currentLeasePointer,
      StoredJobLease previousLease,
      StoredJobLease nextLease) {
    return leasePointerBatchOps(leasePointerKey, currentLeasePointer, previousLease, nextLease);
  }

  public void repairAndReclaimCanonicalJob(Pointer leaseExpiryPointer, long nowMs) {
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
    var canonicalRecordOpt = readCanonicalRecordByKey.apply(canonicalKey);
    if (canonicalRecordOpt.isEmpty()) {
      pointerStore.compareAndDelete(leaseExpiryPointer.getKey(), leaseExpiryPointer.getVersion());
      return;
    }
    var canonicalRecord = canonicalRecordOpt.get();
    StoredJobLease lease = loadLease(canonicalRecord).orElse(null);
    if (lease == null || blank(lease.epoch) || lease.expiresAtMs <= 0L) {
      pointerStore.compareAndDelete(leaseExpiryPointer.getKey(), leaseExpiryPointer.getVersion());
      return;
    }
    String expectedLeaseExpiryKey =
        leaseExpiryPointerKey(lease.expiresAtMs, canonicalRecord.accountId, canonicalRecord.jobId);
    if (!expectedLeaseExpiryKey.equals(leaseExpiryPointer.getKey())) {
      if (!replaceLeaseExpiryPointer(leaseExpiryPointer, expectedLeaseExpiryKey, canonicalKey)) {
        LOG.errorf(
            "Failed to repair reconcile lease-expiry pointer for job %s canonicalKey=%s expectedKey=%s",
            canonicalRecord.jobId, canonicalKey, expectedLeaseExpiryKey);
      }
      return;
    }
    if (lease.expiresAtMs > nowMs || nowMs - lease.expiresAtMs <= leaseRenewGraceMs) {
      return;
    }
    reclaimRunningOrCancellingJob(canonicalKey, canonicalRecord, lease, nowMs);
  }

  public void repairAndReclaimFromStatePointer(
      Pointer statePointer, String expectedState, long nowMs) {
    if (statePointer == null || blank(statePointer.getBlobUri())) {
      if (statePointer != null) {
        pointerStore.compareAndDelete(statePointer.getKey(), statePointer.getVersion());
      }
      return;
    }
    String canonicalKey = statePointer.getBlobUri();
    var canonicalRecordOpt =
        readCurrentRecordFromStateIndexPointer.apply(
            statePointer, record -> expectedState.equals(blankToEmpty(record.state)));
    if (canonicalRecordOpt.isEmpty()) {
      return;
    }
    StoredReconcileJob canonicalRecord = canonicalRecordOpt.get();
    StoredJobLease lease = loadLease(canonicalRecord).orElse(null);
    if (lease == null || blank(lease.epoch) || lease.expiresAtMs <= 0L) {
      return;
    }
    String expectedLeaseExpiryKey = leaseExpiryPointerKey(lease);
    if (expectedLeaseExpiryKey.isBlank()) {
      return;
    }
    Pointer leaseExpiryPointer = pointerStore.get(expectedLeaseExpiryKey).orElse(null);
    if (leaseExpiryPointer == null || !canonicalKey.equals(leaseExpiryPointer.getBlobUri())) {
      if (!upsertReferencePointer.apply(expectedLeaseExpiryKey, canonicalKey)) {
        LOG.errorf(
            "Failed to repair reconcile lease-expiry pointer from state scan jobId=%s canonicalKey=%s",
            canonicalRecord.jobId, canonicalKey);
        return;
      }
    }
    if (lease.expiresAtMs > nowMs || nowMs - lease.expiresAtMs <= leaseRenewGraceMs) {
      return;
    }
    reclaimRunningOrCancellingJob(canonicalKey, canonicalRecord, lease, nowMs);
  }

  private boolean hasActiveLaneLease(StoredReconcileJob record, long now) {
    return record != null && hasUnexpiredJobLease(record.accountId, record.jobId, now);
  }

  private void reclaimRunningOrCancellingJob(
      String canonicalKey, StoredReconcileJob canonicalRecord, StoredJobLease lease, long nowMs) {
    if (!"JS_RUNNING".equals(canonicalRecord.state)
        && !"JS_CANCELLING".equals(canonicalRecord.state)) {
      clearLeaseIfEpochMatches(canonicalRecord.accountId, canonicalRecord.jobId, lease.epoch);
      return;
    }
    if (!hasValidLookupPointer.test(canonicalRecord.jobId, canonicalKey)
        && !repairLookupPointer.apply(canonicalRecord.jobId, canonicalKey)) {
      LOG.errorf(
          "Failed to repair reconcile lookup pointer for job %s canonicalKey=%s",
          canonicalRecord.jobId, canonicalKey);
    }
    if (Boolean.TRUE.equals(requiresReadyPointer.apply(canonicalRecord))
        && !hasValidReadyPointers.test(canonicalRecord, canonicalKey)
        && !repairReadyPointer.apply(canonicalKey, canonicalRecord)) {
      LOG.errorf(
          "Failed to repair reconcile ready pointers for job %s state=%s readyKey=%s",
          canonicalRecord.jobId, canonicalRecord.state, canonicalRecord.readyPointerKey);
    }
    AtomicReference<String> expiredEpoch = new AtomicReference<>("");
    boolean updated =
        mutateByCanonicalPointer.apply(
            canonicalKey,
            record -> {
              if (!"JS_RUNNING".equals(record.state) && !"JS_CANCELLING".equals(record.state)) {
                return null;
              }
              StoredJobLease currentLease = loadLease(record).orElse(null);
              if (currentLease == null
                  || blank(currentLease.epoch)
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
                record.readyPointerKey = readyPointerKeyFor.apply(record, nowMs);
              }
              return record;
            });
    if (updated && !blank(expiredEpoch.get())) {
      clearLeaseIfEpochMatches(
          canonicalRecord.accountId, canonicalRecord.jobId, expiredEpoch.get());
    }
    if (updated) {
      readCanonicalRecordByKey.apply(canonicalKey).ifPresent(refreshContributionChain);
    }
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
        && !blank(record.snapshotTaskTableId)
        && record.snapshotTaskSnapshotId >= 0L
        && hasUnexpiredJobLease(record.accountId, record.jobId, now);
  }

  private String snapshotLeasePointerKey(StoredReconcileJob record) {
    if (record == null
        || record.jobKind() != ReconcileJobKind.PLAN_SNAPSHOT
        || blank(record.snapshotTaskTableId)
        || record.snapshotTaskSnapshotId < 0L) {
      return "";
    }
    return Keys.reconcileSnapshotLeasePointer(
        record.snapshotTaskTableId, record.snapshotTaskSnapshotId);
  }

  private Optional<StoredJobLease> writeLease(
      String accountId, String jobId, UnaryOperator<StoredJobLease> mutator) {
    if (blank(accountId) || blank(jobId)) {
      return Optional.empty();
    }
    String pointerKey = Keys.reconcileJobLeasePointerById(accountId, jobId);
    for (int i = 0; i < casMax; i++) {
      Pointer currentPointer = pointerStore.get(pointerKey).orElse(null);
      StoredJobLease current =
          currentPointer == null
              ? StoredJobLease.empty(accountId, jobId)
              : payloadStore
                  .readInlineJobLease(currentPointer.getBlobUri())
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
    return Objects.equals(left.accountId, right.accountId)
        && Objects.equals(left.jobId, right.jobId)
        && Objects.equals(left.epoch, right.epoch)
        && left.expiresAtMs == right.expiresAtMs;
  }

  private List<CasOp> leasePointerBatchOps(
      String leasePointerKey,
      Pointer currentLeasePointer,
      StoredJobLease previousLease,
      StoredJobLease nextLease) {
    List<CasOp> ops = new ArrayList<>(3);
    String canonicalPointerKey =
        Keys.reconcileJobStateRowById(previousLease.accountId, previousLease.jobId);
    long leaseExpectedVersion = currentLeasePointer == null ? 0L : currentLeasePointer.getVersion();
    Pointer nextLeasePointer =
        Pointer.newBuilder()
            .setKey(leasePointerKey)
            .setBlobUri(payloadStore.encodeInlineJobLease(nextLease))
            .setVersion(leaseExpectedVersion + 1L)
            .build();
    ops.add(new CasUpsert(leasePointerKey, leaseExpectedVersion, nextLeasePointer));

    String previousExpiryKey = leaseExpiryPointerKey(previousLease);
    String nextExpiryKey = leaseExpiryPointerKey(nextLease);
    if (!Objects.equals(previousExpiryKey, nextExpiryKey)) {
      if (!previousExpiryKey.isBlank()) {
        Pointer previousExpiryPointer = pointerStore.get(previousExpiryKey).orElse(null);
        if (previousExpiryPointer != null
            && canonicalPointerKey.equals(previousExpiryPointer.getBlobUri())) {
          ops.add(new CasDelete(previousExpiryKey, previousExpiryPointer.getVersion()));
        }
      }
      if (!nextExpiryKey.isBlank()) {
        Pointer nextExpiryPointer =
            Pointer.newBuilder()
                .setKey(nextExpiryKey)
                .setBlobUri(canonicalPointerKey)
                .setVersion(
                    pointerStore
                        .get(nextExpiryKey)
                        .map(pointer -> pointer.getVersion() + 1L)
                        .orElse(1L))
                .build();
        long expectedVersion = pointerStore.get(nextExpiryKey).map(Pointer::getVersion).orElse(0L);
        ops.add(new CasUpsert(nextExpiryKey, expectedVersion, nextExpiryPointer));
      }
    }
    return ops;
  }

  private void logLeaseSkip(String op, String format, Object... args) {
    LOG.debugf(format, args);
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value.trim();
  }
}
