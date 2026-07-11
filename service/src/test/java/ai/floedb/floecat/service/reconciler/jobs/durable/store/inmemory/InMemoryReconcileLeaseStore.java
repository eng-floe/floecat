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

package ai.floedb.floecat.service.reconciler.jobs.durable.store.inmemory;

import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.LeasedJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredJobDefinition;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredJobLease;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileJobExecutionLoader;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileLeaseStateCodec;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.CanonicalPointerSnapshot;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileLeaseBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileLeaseStore;
import ai.floedb.floecat.service.repo.model.Keys;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.IntToLongFunction;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import org.jboss.logging.Logger;

/** Test-scope in-memory lease store with explicit lease-domain state. */
public final class InMemoryReconcileLeaseStore implements ReconcileLeaseStore {
  private static final Logger LOG = Logger.getLogger(InMemoryReconcileLeaseStore.class);
  private static final String LEASE_EXPIRY_POINTER_PREFIX =
      "/accounts/by-id/reconcile/job-leases/by-expiry/";
  private static final long INVALID_ORDERED_POINTER_MS = -1L;

  private final InMemoryReconcileLeaseState state = new InMemoryReconcileLeaseState();

  private ReconcileLeaseBackend leaseBackend;
  private ReconcileJobExecutionLoader executionLoader;
  private ReconcileLeaseStateCodec leaseStateCodec;
  private int casMax;
  private long leaseMs;
  private long leaseRenewGraceMs;
  private ReconcileJobIndexStore jobIndexStore;
  private CanonicalJobMutator mutateCanonicalJob;
  private Predicate<String> isTerminalState;
  private BiConsumer<StoredReconcileJob, StoredReconcileJob> assertImmutableJobIdentityPreserved;
  private int maxAttempts;
  private IntToLongFunction backoffMs;

  @Override
  public void bind(
      ReconcileLeaseBackend leaseBackend,
      ReconcileJobExecutionLoader executionLoader,
      ReconcileLeaseStateCodec leaseStateCodec,
      int casMax,
      long leaseMs,
      long leaseRenewGraceMs,
      ReconcileJobIndexStore jobIndexStore,
      CanonicalJobMutator mutateCanonicalJob,
      Predicate<String> isTerminalState,
      BiConsumer<StoredReconcileJob, StoredReconcileJob> assertImmutableJobIdentityPreserved,
      int maxAttempts,
      IntToLongFunction backoffMs) {
    this.leaseBackend = leaseBackend;
    this.executionLoader = executionLoader;
    this.leaseStateCodec = leaseStateCodec;
    this.casMax = casMax;
    this.leaseMs = leaseMs;
    this.leaseRenewGraceMs = leaseRenewGraceMs;
    this.jobIndexStore = jobIndexStore;
    this.mutateCanonicalJob = mutateCanonicalJob;
    this.isTerminalState = isTerminalState;
    this.assertImmutableJobIdentityPreserved = assertImmutableJobIdentityPreserved;
    this.maxAttempts = Math.max(1, maxAttempts);
    this.backoffMs = backoffMs == null ? ignored -> 0L : backoffMs;
  }

  @Override
  public Optional<LeasedJob> leaseCanonical(
      String canonicalPointerKey,
      String readyPointerKey,
      long now,
      CanonicalPointerSnapshot initialSnapshot,
      StoredReconcileJob initialRecord) {
    for (int i = 0; i < casMax; i++) {
      CanonicalPointerSnapshot currentSnapshot;
      StoredReconcileJob record;
      if (i == 0 && initialSnapshot != null && initialRecord != null) {
        currentSnapshot = initialSnapshot;
        record = initialRecord;
      } else {
        currentSnapshot = jobIndexStore.loadCanonicalSnapshot(canonicalPointerKey).orElse(null);
        if (currentSnapshot == null) {
          return Optional.empty();
        }
        record = jobIndexStore.readRecord(currentSnapshot).orElse(null);
        if (record == null) {
          return Optional.empty();
        }
      }

      StoredReconcileJob baseline = jobIndexStore.cloneStoredRecord(record);
      StoredReconcileJob current = jobIndexStore.cloneStoredRecord(record);
      if (Boolean.TRUE.equals(isTerminalState.test(current.state))
          || "JS_WAITING".equals(current.state)
          || "JS_CANCELLING".equals(current.state)
          || hasLiveLease(current, true, now)) {
        return Optional.empty();
      }
      String nextLeaseEpoch = "";
      current.state = "JS_RUNNING";
      current.message = "Leased";
      if (current.startedAtMs <= 0L) {
        current.startedAtMs = now;
      }
      current.readyPointerKey = null;
      current.updatedAtMs = now;
      current.canonicalPointerKey = canonicalPointerKey;
      nextLeaseEpoch = UUID.randomUUID().toString();
      assertImmutableJobIdentityPreserved.accept(baseline, current);

      var currentLeaseSnapshot =
          leaseBackend.loadLease(current.accountId, current.jobId).orElse(null);
      StoredJobLease previousLease =
          loadLease(current.accountId, current.jobId)
              .orElse(StoredJobLease.empty(current.accountId, current.jobId));
      if (!blank(previousLease.epoch) && previousLease.expiresAtMs > now) {
        return Optional.empty();
      }
      var ownerWrites = buildExclusivityClaimWrites(current, canonicalPointerKey, now).orElse(null);
      if (ownerWrites == null) {
        return Optional.empty();
      }
      StoredJobLease nextLease =
          StoredJobLease.active(current.accountId, current.jobId, nextLeaseEpoch, now + leaseMs);
      ReconcileJobIndexStore.JobIndexWriteBatch jobIndexBatch =
          buildLeaseJobIndexWriteBatch(currentSnapshot, baseline, current);
      if (leaseBackend.compareAndSetBatch(
          jobIndexBatch,
          mergeLeaseWrites(
              ownerWrites, buildLeaseWriteBatch(currentLeaseSnapshot, previousLease, nextLease)))) {
        synchronized (state) {
          state.leasesByJob.put(
              jobStateKey(current.accountId, current.jobId), cloneLease(nextLease));
        }
        try {
          StoredJobDefinition definition = executionLoader.requireDefinition(current);
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
                  executionLoader.snapshotTask(current),
                  executionLoader.fileGroupTask(current),
                  current.parentJobId()));
        } catch (RuntimeException e) {
          if (isMissingRequiredJobDefinition(e)) {
            failLeaseCanonicalOnHydrationFailure(
                canonicalPointerKey, baseline, nextLeaseEpoch, now);
            return Optional.empty();
          }
          rollbackLeaseCanonicalOnHydrationFailure(canonicalPointerKey, baseline, nextLeaseEpoch);
          throw e;
        }
      }
    }
    return Optional.empty();
  }

  private ReconcileJobIndexStore.JobIndexWriteBatch buildLeaseJobIndexWriteBatch(
      CanonicalPointerSnapshot currentSnapshot,
      StoredReconcileJob previous,
      StoredReconcileJob current) {
    return jobIndexStore.buildJobIndexWriteBatch(currentSnapshot, previous, current);
  }

  @Override
  public boolean hasActiveLease(
      String jobId,
      String leaseEpoch,
      StoredReconcileJob current,
      String context,
      boolean allowCancelling,
      boolean requireUnexpiredLease,
      boolean allowExpiredWithinGrace) {
    if (blank(leaseEpoch)) {
      return false;
    }
    boolean stateAllowed =
        "JS_RUNNING".equals(current.state)
            || (allowCancelling && "JS_CANCELLING".equals(current.state));
    if (!stateAllowed) {
      return false;
    }
    StoredJobLease lease = loadLease(current).orElse(null);
    if (lease == null || !leaseEpoch.equals(lease.epoch)) {
      return false;
    }
    long now = System.currentTimeMillis();
    if (requireUnexpiredLease && lease.expiresAtMs <= now && !allowExpiredWithinGrace) {
      return false;
    }
    return true;
  }

  @Override
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
    return lease != null && !blank(lease.epoch) && lease.expiresAtMs > now;
  }

  @Override
  public Optional<StoredJobLease> loadLease(String accountId, String jobId) {
    synchronized (state) {
      return Optional.ofNullable(cloneLease(state.leasesByJob.get(jobStateKey(accountId, jobId))));
    }
  }

  @Override
  public Optional<StoredJobLease> loadLease(StoredReconcileJob record) {
    if (record == null) {
      return Optional.empty();
    }
    return loadLease(record.accountId, record.jobId);
  }

  @Override
  public Optional<StoredJobLease> mutateLease(
      String accountId, String jobId, UnaryOperator<StoredJobLease> mutator) {
    synchronized (state) {
      StoredJobLease current =
          cloneLease(
              state.leasesByJob.getOrDefault(
                  jobStateKey(accountId, jobId), StoredJobLease.empty(accountId, jobId)));
      StoredJobLease next = mutator.apply(cloneLease(current));
      if (next == null) {
        return Optional.empty();
      }
      if (leaseStateEquals(current, next)) {
        return Optional.of(current);
      }
      if (!leaseBackend.compareAndSetBatch(
          ReconcileJobIndexStore.JobIndexWriteBatch.empty(),
          buildLeaseWriteBatch(
              leaseBackend.loadLease(accountId, jobId).orElse(null), current, next))) {
        return Optional.empty();
      }
      state.leasesByJob.put(jobStateKey(accountId, jobId), cloneLease(next));
      return Optional.of(cloneLease(next));
    }
  }

  @Override
  public Optional<StoredJobLease> renewLeaseIfEpochMatches(
      String accountId, String jobId, String leaseEpoch) {
    return mutateLease(
        accountId,
        jobId,
        current -> {
          if (blank(leaseEpoch) || !leaseEpoch.equals(current.epoch)) {
            return null;
          }
          long now = System.currentTimeMillis();
          if (current.expiresAtMs <= now) {
            return null;
          }
          current.expiresAtMs = now + leaseMs;
          return current;
        });
  }

  @Override
  public LeaseExpiryScanPage scanExpiredLeasePointersPage(
      long nowMs, int pageSize, String pageToken) {
    LeaseExpiryScanPage scanPage =
        leaseBackend.scanExpiredLeaseEntries(Math.max(1, pageSize), blankToEmpty(pageToken));
    List<LeaseExpiryEntry> pointers = new ArrayList<>();
    for (LeaseExpiryEntry pointer : scanPage.entries()) {
      long expiresAtMs = parseLeaseExpiryMillis(pointer.leaseExpiryPointerKey());
      if (expiresAtMs == INVALID_ORDERED_POINTER_MS) {
        continue;
      }
      if (expiresAtMs > nowMs) {
        break;
      }
      pointers.add(pointer);
    }
    return new LeaseExpiryScanPage(pointers, scanPage.nextPageToken());
  }

  @Override
  public void reclaimExpiredLease(LeaseExpiryEntry leaseExpiryEntry, long nowMs) {
    if (leaseExpiryEntry == null || blank(leaseExpiryEntry.canonicalPointerKey())) {
      return;
    }
    String canonicalKey = leaseExpiryEntry.canonicalPointerKey();
    StoredReconcileJob canonicalRecord =
        jobIndexStore.readCanonicalRecordByKey(canonicalKey).orElse(null);
    if (canonicalRecord == null) {
      return;
    }
    StoredJobLease lease = loadLease(canonicalRecord).orElse(null);
    if (lease == null || blank(lease.epoch)) {
      return;
    }
    if (!leaseExpiryPointerKey(lease).equals(leaseExpiryEntry.leaseExpiryPointerKey())) {
      return;
    }
    if (lease.expiresAtMs > nowMs || nowMs - lease.expiresAtMs <= leaseRenewGraceMs) {
      return;
    }
    reclaimRunningOrCancellingJob(canonicalKey, canonicalRecord, lease, nowMs);
  }

  @Override
  public boolean clearLeaseIfEpochMatches(String accountId, String jobId, String leaseEpoch) {
    synchronized (state) {
      StoredJobLease current = state.leasesByJob.get(jobStateKey(accountId, jobId));
      if (current == null || !Objects.equals(current.epoch, leaseEpoch)) {
        return false;
      }
      if (!leaseBackend.compareAndSetBatch(
          ReconcileJobIndexStore.JobIndexWriteBatch.empty(), buildLeaseDeleteBatch(current))) {
        return false;
      }
      state.leasesByJob.remove(jobStateKey(accountId, jobId));
      return true;
    }
  }

  @Override
  public boolean tryAcquireLaneLease(
      StoredReconcileJob record, String canonicalPointerKey, long nowMs) {
    if (record == null
        || blank(record.accountId)
        || blank(record.laneKey)
        || blank(canonicalPointerKey)) {
      return false;
    }
    String lanePointerKey = Keys.reconcileLaneLeasePointer(record.accountId, record.laneKey);
    for (int i = 0; i < casMax; i++) {
      var existing = leaseBackend.loadOwner(lanePointerKey).orElse(null);
      if (existing == null) {
        if (leaseBackend.compareAndSetBatch(
            ReconcileJobIndexStore.JobIndexWriteBatch.empty(),
            new ReconcileLeaseBackend.LeaseWriteBatch(
                List.of(
                    new ReconcileLeaseBackend.LeaseOwnerUpsert(
                        lanePointerKey, 0L, canonicalPointerKey))))) {
          synchronized (state) {
            state.laneOwnerByKey.put(lanePointerKey, canonicalPointerKey);
          }
          return true;
        }
        continue;
      }
      synchronized (state) {
        state.laneOwnerByKey.put(lanePointerKey, existing.canonicalPointerKey());
      }
      if (canonicalPointerKey.equals(existing.canonicalPointerKey())) {
        StoredReconcileJob owner =
            jobIndexStore.readCanonicalRecordByKey(existing.canonicalPointerKey()).orElse(null);
        if (owner != null && hasActiveLaneLease(owner, nowMs)) {
          return false;
        }
        return true;
      }
      StoredReconcileJob owner =
          jobIndexStore.readCanonicalRecordByKey(existing.canonicalPointerKey()).orElse(null);
      if (owner != null
          && record.jobId.equals(owner.jobId)
          && record.accountId.equals(owner.accountId)) {
        if (hasActiveLaneLease(owner, nowMs)) {
          return false;
        }
        return true;
      }
      if (owner != null && hasActiveLaneLease(owner, nowMs)) {
        return false;
      }
      if (leaseBackend.compareAndSetBatch(
          ReconcileJobIndexStore.JobIndexWriteBatch.empty(),
          new ReconcileLeaseBackend.LeaseWriteBatch(
              List.of(
                  new ReconcileLeaseBackend.LeaseOwnerDelete(
                      lanePointerKey, existing.version()))))) {
        synchronized (state) {
          state.laneOwnerByKey.remove(lanePointerKey);
        }
      }
    }
    return false;
  }

  @Override
  public void clearLaneLeaseIfOwned(StoredReconcileJob record, String expectedReference) {
    if (record == null
        || blank(record.accountId)
        || blank(record.laneKey)
        || blank(expectedReference)) {
      return;
    }
    String lanePointerKey = Keys.reconcileLaneLeasePointer(record.accountId, record.laneKey);
    var existing = leaseBackend.loadOwner(lanePointerKey).orElse(null);
    if (existing == null) {
      synchronized (state) {
        state.laneOwnerByKey.remove(lanePointerKey);
      }
      return;
    }
    synchronized (state) {
      state.laneOwnerByKey.put(lanePointerKey, existing.canonicalPointerKey());
    }
    if (!expectedReference.equals(existing.canonicalPointerKey())) {
      StoredReconcileJob owner =
          jobIndexStore.readCanonicalRecordByKey(existing.canonicalPointerKey()).orElse(null);
      if (owner == null
          || !record.jobId.equals(owner.jobId)
          || !record.accountId.equals(owner.accountId)) {
        return;
      }
    }
    StoredReconcileJob owner =
        jobIndexStore.readCanonicalRecordByKey(existing.canonicalPointerKey()).orElse(null);
    if (owner == null
        || !record.jobId.equals(owner.jobId)
        || !record.accountId.equals(owner.accountId)) {
      return;
    }
    if (holdsExecutionLease(owner) && hasActiveLaneLease(owner, System.currentTimeMillis())) {
      return;
    }
    if (leaseBackend.compareAndSetBatch(
        ReconcileJobIndexStore.JobIndexWriteBatch.empty(),
        new ReconcileLeaseBackend.LeaseWriteBatch(
            List.of(
                new ReconcileLeaseBackend.LeaseOwnerDelete(lanePointerKey, existing.version()))))) {
      synchronized (state) {
        state.laneOwnerByKey.remove(lanePointerKey);
      }
    }
  }

  @Override
  public boolean tryAcquireSnapshotLease(
      StoredReconcileJob record, String canonicalPointerKey, long nowMs) {
    String pointerKey = snapshotLeasePointerKey(record);
    if (pointerKey.isBlank()) {
      return true;
    }
    for (int i = 0; i < casMax; i++) {
      var existing = leaseBackend.loadOwner(pointerKey).orElse(null);
      if (existing == null) {
        if (leaseBackend.compareAndSetBatch(
            ReconcileJobIndexStore.JobIndexWriteBatch.empty(),
            new ReconcileLeaseBackend.LeaseWriteBatch(
                List.of(
                    new ReconcileLeaseBackend.LeaseOwnerUpsert(
                        pointerKey, 0L, canonicalPointerKey))))) {
          synchronized (state) {
            state.snapshotOwnerByKey.put(pointerKey, canonicalPointerKey);
          }
          return true;
        }
        continue;
      }
      synchronized (state) {
        state.snapshotOwnerByKey.put(pointerKey, existing.canonicalPointerKey());
      }
      if (canonicalPointerKey.equals(existing.canonicalPointerKey())) {
        return true;
      }
      StoredReconcileJob owner =
          jobIndexStore.readCanonicalRecordByKey(existing.canonicalPointerKey()).orElse(null);
      if (owner == null) {
        if (leaseBackend.compareAndSetBatch(
            ReconcileJobIndexStore.JobIndexWriteBatch.empty(),
            new ReconcileLeaseBackend.LeaseWriteBatch(
                List.of(
                    new ReconcileLeaseBackend.LeaseOwnerDelete(pointerKey, existing.version()))))) {
          synchronized (state) {
            state.snapshotOwnerByKey.remove(pointerKey);
          }
        }
        continue;
      }
      if (record.jobId.equals(owner.jobId) && record.accountId.equals(owner.accountId)) {
        return true;
      }
      if (!hasActiveSnapshotLease(owner, nowMs)) {
        if (leaseBackend.compareAndSetBatch(
            ReconcileJobIndexStore.JobIndexWriteBatch.empty(),
            new ReconcileLeaseBackend.LeaseWriteBatch(
                List.of(
                    new ReconcileLeaseBackend.LeaseOwnerDelete(pointerKey, existing.version()))))) {
          synchronized (state) {
            state.snapshotOwnerByKey.remove(pointerKey);
          }
        }
        continue;
      }
      return false;
    }
    return false;
  }

  @Override
  public void clearSnapshotLeaseIfOwned(StoredReconcileJob record, String expectedReference) {
    String pointerKey = snapshotLeasePointerKey(record);
    if (pointerKey.isBlank()) {
      return;
    }
    var existing = leaseBackend.loadOwner(pointerKey).orElse(null);
    if (existing == null) {
      synchronized (state) {
        state.snapshotOwnerByKey.remove(pointerKey);
      }
      return;
    }
    synchronized (state) {
      state.snapshotOwnerByKey.put(pointerKey, existing.canonicalPointerKey());
    }
    if (!blank(expectedReference) && !expectedReference.equals(existing.canonicalPointerKey())) {
      StoredReconcileJob owner =
          jobIndexStore.readCanonicalRecordByKey(existing.canonicalPointerKey()).orElse(null);
      if (owner == null
          || !record.jobId.equals(owner.jobId)
          || !record.accountId.equals(owner.accountId)) {
        return;
      }
    }
    StoredReconcileJob owner =
        jobIndexStore.readCanonicalRecordByKey(existing.canonicalPointerKey()).orElse(null);
    if (owner == null
        || !record.jobId.equals(owner.jobId)
        || !record.accountId.equals(owner.accountId)) {
      return;
    }
    if (holdsExecutionLease(owner) && hasActiveSnapshotLease(owner, System.currentTimeMillis())) {
      return;
    }
    if (leaseBackend.compareAndSetBatch(
        ReconcileJobIndexStore.JobIndexWriteBatch.empty(),
        new ReconcileLeaseBackend.LeaseWriteBatch(
            List.of(new ReconcileLeaseBackend.LeaseOwnerDelete(pointerKey, existing.version()))))) {
      synchronized (state) {
        state.snapshotOwnerByKey.remove(pointerKey);
      }
    }
  }

  @Override
  public String leaseExpiryPointerKey(StoredJobLease lease) {
    if (lease == null) {
      return "";
    }
    return leaseExpiryPointerKey(lease.expiresAtMs, lease.accountId, lease.jobId);
  }

  @Override
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

  private void rollbackLeaseCanonicalOnHydrationFailure(
      String canonicalPointerKey, StoredReconcileJob baseline, String leaseEpoch) {
    AtomicBoolean rollbackOwnedLease = new AtomicBoolean(false);
    mutateCanonicalJob.apply(
        canonicalPointerKey,
        existing -> {
          if (existing == null
              || !"JS_RUNNING".equals(existing.state)
              || !"Leased".equals(blankToEmpty(existing.message))
              || !loadLease(existing).map(lease -> leaseEpoch.equals(lease.epoch)).orElse(false)) {
            return existing;
          }
          rollbackOwnedLease.set(true);
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
    if (!rollbackOwnedLease.get()) {
      return;
    }
    clearLeaseIfEpochMatches(baseline.accountId, baseline.jobId, leaseEpoch);
    clearLaneLeaseIfOwned(baseline, canonicalPointerKey);
    clearSnapshotLeaseIfOwned(baseline, canonicalPointerKey);
  }

  private void failLeaseCanonicalOnHydrationFailure(
      String canonicalPointerKey, StoredReconcileJob baseline, String leaseEpoch, long now) {
    if (baseline == null || blank(canonicalPointerKey) || blank(leaseEpoch)) {
      return;
    }
    AtomicBoolean failOwnedLease = new AtomicBoolean(false);
    Optional<ReconcileJobIndexStore.CanonicalEnvelope> updated =
        mutateCanonicalJob.apply(
            canonicalPointerKey,
            existing -> {
              if (existing == null
                  || !"JS_RUNNING".equals(existing.state)
                  || !"Leased".equals(blankToEmpty(existing.message))
                  || !loadLease(existing)
                      .map(lease -> leaseEpoch.equals(lease.epoch))
                      .orElse(false)) {
                return existing;
              }
              failOwnedLease.set(true);
              existing.attempt = Math.max(0, existing.attempt) + 1;
              existing.lastError = "Missing required job definition";
              existing.state = "JS_FAILED";
              existing.message = "Missing required job definition";
              if (existing.startedAtMs <= 0L) {
                existing.startedAtMs = now;
              }
              existing.finishedAtMs = now;
              existing.readyPointerKey = null;
              return existing;
            });
    if (!failOwnedLease.get()) {
      return;
    }
    clearLeaseIfEpochMatches(baseline.accountId, baseline.jobId, leaseEpoch);
    clearLaneLeaseIfOwned(baseline, canonicalPointerKey);
    clearSnapshotLeaseIfOwned(baseline, canonicalPointerKey);
  }

  private ReconcileLeaseBackend.LeaseWriteBatch mergeLeaseWrites(
      List<ReconcileLeaseBackend.LeaseWriteOp> ownerWrites,
      ReconcileLeaseBackend.LeaseWriteBatch leaseWrites) {
    List<ReconcileLeaseBackend.LeaseWriteOp> writes =
        new ArrayList<>(ownerWrites.size() + leaseWrites.writes().size());
    writes.addAll(ownerWrites);
    writes.addAll(leaseWrites.writes());
    return new ReconcileLeaseBackend.LeaseWriteBatch(List.copyOf(writes));
  }

  private Optional<List<ReconcileLeaseBackend.LeaseWriteOp>> buildExclusivityClaimWrites(
      StoredReconcileJob record, String canonicalPointerKey, long nowMs) {
    List<ReconcileLeaseBackend.LeaseWriteOp> writes = new ArrayList<>(4);
    var laneWrites = buildLaneLeaseClaimWrites(record, canonicalPointerKey, nowMs).orElse(null);
    if (laneWrites == null && requiresLaneLease(record)) {
      return Optional.empty();
    }
    if (laneWrites != null) {
      writes.addAll(laneWrites);
    }
    var snapshotWrites =
        buildSnapshotLeaseClaimWrites(record, canonicalPointerKey, nowMs).orElse(null);
    if (snapshotWrites == null && requiresSnapshotLease(record)) {
      return Optional.empty();
    }
    if (snapshotWrites != null) {
      writes.addAll(snapshotWrites);
    }
    return Optional.of(List.copyOf(writes));
  }

  private Optional<List<ReconcileLeaseBackend.LeaseWriteOp>> buildLaneLeaseClaimWrites(
      StoredReconcileJob record, String canonicalPointerKey, long nowMs) {
    if (!requiresLaneLease(record) || blank(canonicalPointerKey)) {
      return Optional.empty();
    }
    String lanePointerKey = Keys.reconcileLaneLeasePointer(record.accountId, record.laneKey);
    var existing = leaseBackend.loadOwner(lanePointerKey).orElse(null);
    if (existing == null) {
      return Optional.of(
          List.of(
              new ReconcileLeaseBackend.LeaseOwnerUpsert(lanePointerKey, 0L, canonicalPointerKey)));
    }
    StoredReconcileJob owner =
        jobIndexStore.readCanonicalRecordByKey(existing.canonicalPointerKey()).orElse(null);
    var ownerLeaseSnapshot =
        owner == null ? null : leaseBackend.loadLease(owner.accountId, owner.jobId).orElse(null);
    if (canonicalPointerKey.equals(existing.canonicalPointerKey()) || sameJobOwner(record, owner)) {
      return owner != null && hasActiveLaneLease(owner, nowMs)
          ? Optional.empty()
          : Optional.of(
              ownerClaimWrites(
                  lanePointerKey,
                  existing.version(),
                  canonicalPointerKey,
                  owner,
                  ownerLeaseSnapshot));
    }
    if (owner != null && hasActiveLaneLease(owner, nowMs)) {
      return Optional.empty();
    }
    return Optional.of(
        ownerClaimWrites(
            lanePointerKey, existing.version(), canonicalPointerKey, owner, ownerLeaseSnapshot));
  }

  private Optional<List<ReconcileLeaseBackend.LeaseWriteOp>> buildSnapshotLeaseClaimWrites(
      StoredReconcileJob record, String canonicalPointerKey, long nowMs) {
    if (!requiresSnapshotLease(record) || blank(canonicalPointerKey)) {
      return Optional.empty();
    }
    String pointerKey = snapshotLeasePointerKey(record);
    var existing = leaseBackend.loadOwner(pointerKey).orElse(null);
    if (existing == null) {
      return Optional.of(
          List.of(new ReconcileLeaseBackend.LeaseOwnerUpsert(pointerKey, 0L, canonicalPointerKey)));
    }
    StoredReconcileJob owner =
        jobIndexStore.readCanonicalRecordByKey(existing.canonicalPointerKey()).orElse(null);
    var ownerLeaseSnapshot =
        owner == null ? null : leaseBackend.loadLease(owner.accountId, owner.jobId).orElse(null);
    if (canonicalPointerKey.equals(existing.canonicalPointerKey()) || sameJobOwner(record, owner)) {
      return owner != null && hasActiveSnapshotLease(owner, nowMs)
          ? Optional.empty()
          : Optional.of(
              ownerClaimWrites(
                  pointerKey, existing.version(), canonicalPointerKey, owner, ownerLeaseSnapshot));
    }
    if (owner != null && hasActiveSnapshotLease(owner, nowMs)) {
      return Optional.empty();
    }
    return Optional.of(
        ownerClaimWrites(
            pointerKey, existing.version(), canonicalPointerKey, owner, ownerLeaseSnapshot));
  }

  private List<ReconcileLeaseBackend.LeaseWriteOp> ownerClaimWrites(
      String ownerKey,
      long ownerVersion,
      String canonicalPointerKey,
      StoredReconcileJob owner,
      ReconcileLeaseBackend.LeaseRecordSnapshot ownerLeaseSnapshot) {
    List<ReconcileLeaseBackend.LeaseWriteOp> writes = new ArrayList<>(2);
    if (owner != null) {
      writes.add(
          new ReconcileLeaseBackend.LeaseRecordCondition(
              owner.accountId,
              owner.jobId,
              ownerLeaseSnapshot == null ? 0L : ownerLeaseSnapshot.version()));
    }
    writes.add(
        new ReconcileLeaseBackend.LeaseOwnerUpsert(ownerKey, ownerVersion, canonicalPointerKey));
    return List.copyOf(writes);
  }

  private void reclaimRunningOrCancellingJob(
      String canonicalKey, StoredReconcileJob canonicalRecord, StoredJobLease lease, long nowMs) {
    if (!"JS_RUNNING".equals(canonicalRecord.state)
        && !"JS_CANCELLING".equals(canonicalRecord.state)) {
      clearLeaseIfEpochMatches(canonicalRecord.accountId, canonicalRecord.jobId, lease.epoch);
      return;
    }
    AtomicReference<String> expiredEpoch = new AtomicReference<>("");
    boolean updated =
        mutateCanonicalJob
            .apply(
                canonicalKey,
                record -> {
                  if (!"JS_RUNNING".equals(record.state) && !"JS_CANCELLING".equals(record.state)) {
                    return null;
                  }
                  StoredJobLease currentLease = loadLease(record).orElse(null);
                  if (currentLease == null
                      || blank(currentLease.epoch)
                      || currentLease.expiresAtMs > nowMs
                      || nowMs - currentLease.expiresAtMs <= leaseRenewGraceMs) {
                    return null;
                  }
                  expiredEpoch.set(currentLease.epoch);
                  boolean wasCancelling = "JS_CANCELLING".equals(record.state);
                  if (wasCancelling) {
                    record.state = "JS_CANCELLED";
                    if (blank(record.message)) {
                      record.message = "Cancelled after lease expiry";
                    }
                    record.finishedAtMs = nowMs;
                    record.readyPointerKey = null;
                  } else {
                    record.attempt = Math.max(0, record.attempt) + 1;
                    record.lastError = "Lease expired";
                    record.executorId = "";
                    if (record.attempt >= maxAttempts) {
                      record.state = "JS_FAILED";
                      record.message = "Lease expired repeatedly; failed";
                      if (record.startedAtMs <= 0L) {
                        record.startedAtMs = nowMs;
                      }
                      record.finishedAtMs = nowMs;
                      record.readyPointerKey = null;
                    } else {
                      long nextAttemptAtMs =
                          nowMs + Math.max(0L, backoffMs.applyAsLong(record.attempt));
                      record.state = "JS_QUEUED";
                      record.message = "Lease expired; requeued";
                      record.nextAttemptAtMs = nextAttemptAtMs;
                      record.finishedAtMs = 0L;
                      record.readyPointerKey =
                          Keys.reconcileReadyPointerByDue(
                              nextAttemptAtMs, record.accountId, record.laneKey, record.jobId);
                    }
                  }
                  return record;
                })
            .isPresent();
    if (updated && !blank(expiredEpoch.get())) {
      clearLeaseIfEpochMatches(
          canonicalRecord.accountId, canonicalRecord.jobId, expiredEpoch.get());
    }
  }

  private boolean hasUnexpiredJobLease(String accountId, String jobId, long nowMs) {
    StoredJobLease lease = loadLease(accountId, jobId).orElse(null);
    return lease != null && !blank(lease.epoch) && lease.expiresAtMs > nowMs;
  }

  private boolean requiresLaneLease(StoredReconcileJob record) {
    return record != null
        && record.jobKind() != ReconcileJobKind.EXEC_FILE_GROUP
        && record.jobKind() != ReconcileJobKind.PLAN_SNAPSHOT
        && !blank(record.accountId)
        && !blank(record.laneKey);
  }

  private boolean requiresSnapshotLease(StoredReconcileJob record) {
    return !snapshotLeasePointerKey(record).isBlank();
  }

  private boolean sameJobOwner(StoredReconcileJob claimant, StoredReconcileJob owner) {
    return claimant != null
        && owner != null
        && Objects.equals(claimant.jobId, owner.jobId)
        && Objects.equals(claimant.accountId, owner.accountId);
  }

  private boolean hasActiveSnapshotLease(StoredReconcileJob record, long nowMs) {
    return record != null
        && record.jobKind() == ReconcileJobKind.PLAN_SNAPSHOT
        && !blank(record.snapshotTaskTableId)
        && record.snapshotTaskSnapshotId >= 0L
        && hasUnexpiredJobLease(record.accountId, record.jobId, nowMs);
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

  private ReconcileLeaseBackend.LeaseWriteBatch buildLeaseWriteBatch(
      ReconcileLeaseBackend.LeaseRecordSnapshot currentLeaseSnapshot,
      StoredJobLease previousLease,
      StoredJobLease nextLease) {
    long expectedVersion = currentLeaseSnapshot == null ? 0L : currentLeaseSnapshot.version();
    List<ReconcileLeaseBackend.LeaseWriteOp> ops = new ArrayList<>();
    ops.add(
        new ReconcileLeaseBackend.LeaseRecordUpsert(
            nextLease.accountId,
            nextLease.jobId,
            expectedVersion,
            leaseStateCodec.encode(nextLease)));

    String canonicalPointerKey =
        Keys.reconcileJobStateRowById(nextLease.accountId, nextLease.jobId);
    String previousExpiryKey = leaseExpiryPointerKey(previousLease);
    String nextExpiryKey = leaseExpiryPointerKey(nextLease);
    if (!Objects.equals(previousExpiryKey, nextExpiryKey)) {
      if (!blank(previousExpiryKey)) {
        var prev = leaseBackend.loadLeaseExpiry(previousExpiryKey).orElse(null);
        if (prev != null && canonicalPointerKey.equals(prev.canonicalPointerKey())) {
          ops.add(new ReconcileLeaseBackend.LeaseExpiryDelete(previousExpiryKey, prev.version()));
        }
      }
      if (!blank(nextExpiryKey)) {
        var existing = leaseBackend.loadLeaseExpiry(nextExpiryKey).orElse(null);
        long expiryExpectedVersion = existing == null ? 0L : existing.version();
        ops.add(
            new ReconcileLeaseBackend.LeaseExpiryUpsert(
                nextExpiryKey, expiryExpectedVersion, canonicalPointerKey));
      }
    }
    return new ReconcileLeaseBackend.LeaseWriteBatch(List.copyOf(ops));
  }

  private ReconcileLeaseBackend.LeaseWriteBatch buildLeaseDeleteBatch(StoredJobLease lease) {
    List<ReconcileLeaseBackend.LeaseWriteOp> ops = new ArrayList<>();
    var leasePointer = leaseBackend.loadLease(lease.accountId, lease.jobId).orElse(null);
    if (leasePointer != null) {
      ops.add(
          new ReconcileLeaseBackend.LeaseRecordDelete(
              lease.accountId, lease.jobId, leasePointer.version()));
    }
    String expiryKey = leaseExpiryPointerKey(lease);
    if (!blank(expiryKey)) {
      var expiryPointer = leaseBackend.loadLeaseExpiry(expiryKey).orElse(null);
      String canonicalPointerKey = Keys.reconcileJobStateRowById(lease.accountId, lease.jobId);
      if (expiryPointer != null
          && canonicalPointerKey.equals(expiryPointer.canonicalPointerKey())) {
        ops.add(new ReconcileLeaseBackend.LeaseExpiryDelete(expiryKey, expiryPointer.version()));
      }
    }
    return new ReconcileLeaseBackend.LeaseWriteBatch(List.copyOf(ops));
  }

  private boolean holdsExecutionLease(StoredReconcileJob record) {
    return record != null
        && ("JS_RUNNING".equals(record.state) || "JS_CANCELLING".equals(record.state));
  }

  private boolean hasActiveLaneLease(StoredReconcileJob owner, long nowMs) {
    return owner != null && hasUnexpiredJobLease(owner.accountId, owner.jobId, nowMs);
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
    return Objects.equals(left.accountId, right.accountId)
        && Objects.equals(left.jobId, right.jobId)
        && Objects.equals(left.epoch, right.epoch)
        && left.expiresAtMs == right.expiresAtMs;
  }

  private String jobStateKey(String accountId, String jobId) {
    return InMemoryReconcileLeaseState.jobKey(accountId, jobId);
  }

  private long parseLeaseExpiryMillis(String leaseExpiryPointerKey) {
    if (blank(leaseExpiryPointerKey)
        || !leaseExpiryPointerKey.startsWith(LEASE_EXPIRY_POINTER_PREFIX)) {
      return INVALID_ORDERED_POINTER_MS;
    }
    int slash = leaseExpiryPointerKey.indexOf('/', LEASE_EXPIRY_POINTER_PREFIX.length());
    if (slash < 0) {
      return INVALID_ORDERED_POINTER_MS;
    }
    try {
      return Long.parseLong(
          leaseExpiryPointerKey.substring(LEASE_EXPIRY_POINTER_PREFIX.length(), slash));
    } catch (NumberFormatException e) {
      return INVALID_ORDERED_POINTER_MS;
    }
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }

  private static boolean isMissingRequiredJobDefinition(Throwable error) {
    return error != null
        && error.getMessage() != null
        && error.getMessage().contains("missing required job definition");
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value;
  }
}
