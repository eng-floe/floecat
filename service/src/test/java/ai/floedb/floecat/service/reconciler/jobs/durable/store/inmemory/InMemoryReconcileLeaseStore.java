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

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.LeasedJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredJobDefinition;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredJobLease;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.projection.ReconcileProjectionUpdater;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcilePayloadStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.CanonicalPointerSnapshot;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.JobIndexWriteBatchSupport;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileLeaseBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileLeaseStore;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.PointerStore.CasDelete;
import ai.floedb.floecat.storage.spi.PointerStore.CasOp;
import ai.floedb.floecat.storage.spi.PointerStore.CasUpsert;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import org.jboss.logging.Logger;

/**
 * Test-scope in-memory lease store with explicit lease-domain state. Pointer rows are mirrored for
 * compatibility with existing durable tests that inspect storage keys directly.
 */
public final class InMemoryReconcileLeaseStore implements ReconcileLeaseStore {
  private static final Logger LOG = Logger.getLogger(InMemoryReconcileLeaseStore.class);
  private static final String LEASE_EXPIRY_POINTER_PREFIX =
      "/accounts/by-id/reconcile/job-leases/by-expiry/";
  private static final long INVALID_ORDERED_POINTER_MS = -1L;

  private final InMemoryReconcileLeaseState state = new InMemoryReconcileLeaseState();

  private ReconcileLeaseBackend leaseBackend;
  private ReconcilePayloadStore payloadStore;
  private int casMax;
  private long leaseMs;
  private long leaseRenewGraceMs;
  private ReconcileJobIndexStore jobIndexStore;
  private ReconcileProjectionUpdater projectionUpdater;
  private Predicate<String> isTerminalState;
  private BiConsumer<StoredReconcileJob, StoredReconcileJob> assertImmutableJobIdentityPreserved;

  @Override
  public void bind(
      ReconcileLeaseBackend leaseBackend,
      ReconcilePayloadStore payloadStore,
      int casMax,
      long leaseMs,
      long leaseRenewGraceMs,
      ReconcileJobIndexStore jobIndexStore,
      ReconcileProjectionUpdater projectionUpdater,
      Predicate<String> isTerminalState,
      BiConsumer<StoredReconcileJob, StoredReconcileJob> assertImmutableJobIdentityPreserved) {
    this.leaseBackend = leaseBackend;
    this.payloadStore = payloadStore;
    this.casMax = casMax;
    this.leaseMs = leaseMs;
    this.leaseRenewGraceMs = leaseRenewGraceMs;
    this.jobIndexStore = jobIndexStore;
    this.projectionUpdater = projectionUpdater;
    this.isTerminalState = isTerminalState;
    this.assertImmutableJobIdentityPreserved = assertImmutableJobIdentityPreserved;
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
      if (!tryAcquireSnapshotLease(current, canonicalPointerKey, now)) {
        return Optional.empty();
      }

      String nextLeaseEpoch = "";
      boolean releaseSnapshotLease = true;
      try {
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

        Pointer nextPointer =
            Pointer.newBuilder()
                .setKey(canonicalPointerKey)
                .setBlobUri(payloadStore.encodeInlineJobState(current))
                .setVersion(currentSnapshot.version() + 1L)
                .build();
        StoredJobLease previousLease =
            loadLease(current.accountId, current.jobId)
                .orElse(StoredJobLease.empty(current.accountId, current.jobId));
        if (!blank(previousLease.epoch) && previousLease.expiresAtMs > now) {
          return Optional.empty();
        }
        StoredJobLease nextLease =
            StoredJobLease.active(current.accountId, current.jobId, nextLeaseEpoch, now + leaseMs);
        List<CasOp> ops =
            new ArrayList<>(
                JobIndexWriteBatchSupport.toCasOps(
                    jobIndexStore.buildJobIndexWriteBatch(currentSnapshot, baseline, current),
                    key ->
                        leaseBackend
                            .loadPointer(key)
                            .map(
                                pointer ->
                                    new ai.floedb.floecat.service.reconciler.jobs.durable.store
                                        .StoredPointerSnapshot(
                                        pointer.getKey(),
                                        pointer.getBlobUri(),
                                        pointer.getVersion()))));
        ops.addAll(buildLeaseMirrorOps(previousLease, nextLease));
        if (leaseBackend.compareAndSetBatch(ops)) {
          synchronized (state) {
            state.leasesByJob.put(
                jobStateKey(current.accountId, current.jobId), cloneLease(nextLease));
          }
          releaseSnapshotLease = false;
          try {
            StoredJobDefinition definition = payloadStore.requireDefinition(current);
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
                    payloadStore.snapshotTaskFor(current),
                    payloadStore.fileGroupTaskFor(current),
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
    if (requireUnexpiredLease && lease.expiresAtMs <= now) {
      return allowExpiredWithinGrace && now - lease.expiresAtMs <= leaseRenewGraceMs;
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
      String pointerKey = Keys.reconcileJobLeasePointerById(accountId, jobId);
      Pointer pointer = leaseBackend.loadPointer(pointerKey).orElse(null);
      if (pointer != null) {
        StoredJobLease mirrored =
            payloadStore.readInlineJobLease(pointer.getBlobUri()).orElse(null);
        if (mirrored != null) {
          state.leasesByJob.put(jobStateKey(accountId, jobId), cloneLease(mirrored));
          return Optional.of(cloneLease(mirrored));
        }
      }
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
      if (!leaseBackend.compareAndSetBatch(buildLeaseMirrorOps(current, next))) {
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
          if (current.expiresAtMs > 0L && (current.expiresAtMs - now) > (leaseMs / 2L)) {
            return current;
          }
          if (current.expiresAtMs > 0L && now - current.expiresAtMs > leaseRenewGraceMs) {
            return null;
          }
          current.expiresAtMs = now + leaseMs;
          return current;
        });
  }

  @Override
  public LeaseExpiryScanPage scanExpiredLeasePointersPage(
      long nowMs, int pageSize, String pageToken) {
    StringBuilder next = new StringBuilder();
    List<Pointer> listed =
        leaseBackend.listPointersByPrefix(
            LEASE_EXPIRY_POINTER_PREFIX, Math.max(1, pageSize), blankToEmpty(pageToken), next);
    List<LeaseExpiryEntry> pointers = new ArrayList<>();
    for (Pointer pointer : listed) {
      long expiresAtMs = parseLeaseExpiryMillis(pointer.getKey());
      if (expiresAtMs == INVALID_ORDERED_POINTER_MS) {
        continue;
      }
      if (expiresAtMs > nowMs) {
        break;
      }
      pointers.add(new LeaseExpiryEntry(pointer.getKey(), pointer.getBlobUri()));
    }
    return new LeaseExpiryScanPage(pointers, next.toString());
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
      if (!leaseBackend.compareAndSetBatch(buildLeaseDeleteOps(current))) {
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
      Pointer existing = leaseBackend.loadPointer(lanePointerKey).orElse(null);
      if (existing == null) {
        Pointer created =
            Pointer.newBuilder()
                .setKey(lanePointerKey)
                .setBlobUri(canonicalPointerKey)
                .setVersion(1L)
                .build();
        if (leaseBackend.compareAndSetBatch(List.of(new CasUpsert(lanePointerKey, 0L, created)))) {
          synchronized (state) {
            state.laneOwnerByKey.put(lanePointerKey, canonicalPointerKey);
          }
          return true;
        }
        continue;
      }
      synchronized (state) {
        state.laneOwnerByKey.put(lanePointerKey, existing.getBlobUri());
      }
      if (canonicalPointerKey.equals(existing.getBlobUri())) {
        StoredReconcileJob owner =
            jobIndexStore.readCanonicalRecordByKey(existing.getBlobUri()).orElse(null);
        if (owner != null && hasActiveLaneLease(owner, nowMs)) {
          return false;
        }
        return true;
      }
      StoredReconcileJob owner =
          jobIndexStore.readCanonicalRecordByKey(existing.getBlobUri()).orElse(null);
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
          List.of(new CasDelete(lanePointerKey, existing.getVersion())))) {
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
    Pointer existing = leaseBackend.loadPointer(lanePointerKey).orElse(null);
    if (existing == null) {
      synchronized (state) {
        state.laneOwnerByKey.remove(lanePointerKey);
      }
      return;
    }
    synchronized (state) {
      state.laneOwnerByKey.put(lanePointerKey, existing.getBlobUri());
    }
    if (!expectedReference.equals(existing.getBlobUri())) {
      StoredReconcileJob owner =
          jobIndexStore.readCanonicalRecordByKey(existing.getBlobUri()).orElse(null);
      if (owner == null
          || !record.jobId.equals(owner.jobId)
          || !record.accountId.equals(owner.accountId)) {
        return;
      }
    }
    StoredReconcileJob owner =
        jobIndexStore.readCanonicalRecordByKey(existing.getBlobUri()).orElse(null);
    if (owner == null
        || !record.jobId.equals(owner.jobId)
        || !record.accountId.equals(owner.accountId)) {
      return;
    }
    if (hasActiveLaneLease(owner, System.currentTimeMillis())) {
      return;
    }
    if (leaseBackend.compareAndSetBatch(
        List.of(new CasDelete(lanePointerKey, existing.getVersion())))) {
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
      Pointer existing = leaseBackend.loadPointer(pointerKey).orElse(null);
      if (existing == null) {
        Pointer created =
            Pointer.newBuilder()
                .setKey(pointerKey)
                .setBlobUri(canonicalPointerKey)
                .setVersion(1L)
                .build();
        if (leaseBackend.compareAndSetBatch(List.of(new CasUpsert(pointerKey, 0L, created)))) {
          synchronized (state) {
            state.snapshotOwnerByKey.put(pointerKey, canonicalPointerKey);
          }
          return true;
        }
        continue;
      }
      synchronized (state) {
        state.snapshotOwnerByKey.put(pointerKey, existing.getBlobUri());
      }
      if (canonicalPointerKey.equals(existing.getBlobUri())) {
        return true;
      }
      StoredReconcileJob owner =
          jobIndexStore.readCanonicalRecordByKey(existing.getBlobUri()).orElse(null);
      if (owner == null) {
        if (leaseBackend.compareAndSetBatch(
            List.of(new CasDelete(pointerKey, existing.getVersion())))) {
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
            List.of(new CasDelete(pointerKey, existing.getVersion())))) {
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
    Pointer existing = leaseBackend.loadPointer(pointerKey).orElse(null);
    if (existing == null) {
      synchronized (state) {
        state.snapshotOwnerByKey.remove(pointerKey);
      }
      return;
    }
    synchronized (state) {
      state.snapshotOwnerByKey.put(pointerKey, existing.getBlobUri());
    }
    if (!blank(expectedReference) && !expectedReference.equals(existing.getBlobUri())) {
      StoredReconcileJob owner =
          jobIndexStore.readCanonicalRecordByKey(existing.getBlobUri()).orElse(null);
      if (owner == null
          || !record.jobId.equals(owner.jobId)
          || !record.accountId.equals(owner.accountId)) {
        return;
      }
    }
    StoredReconcileJob owner =
        jobIndexStore.readCanonicalRecordByKey(existing.getBlobUri()).orElse(null);
    if (owner == null
        || !record.jobId.equals(owner.jobId)
        || !record.accountId.equals(owner.accountId)) {
      return;
    }
    if (hasActiveSnapshotLease(owner, System.currentTimeMillis())) {
      return;
    }
    if (leaseBackend.compareAndSetBatch(
        List.of(new CasDelete(pointerKey, existing.getVersion())))) {
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
    clearLeaseIfEpochMatches(baseline.accountId, baseline.jobId, leaseEpoch);
    clearLaneLeaseIfOwned(baseline, canonicalPointerKey);
    clearSnapshotLeaseIfOwned(baseline, canonicalPointerKey);
    jobIndexStore.mutateByCanonicalPointerReturningRecord(
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

  private void reclaimRunningOrCancellingJob(
      String canonicalKey, StoredReconcileJob canonicalRecord, StoredJobLease lease, long nowMs) {
    if (!"JS_RUNNING".equals(canonicalRecord.state)
        && !"JS_CANCELLING".equals(canonicalRecord.state)) {
      clearLeaseIfEpochMatches(canonicalRecord.accountId, canonicalRecord.jobId, lease.epoch);
      return;
    }
    AtomicReference<String> expiredEpoch = new AtomicReference<>("");
    boolean updated =
        jobIndexStore
            .mutateByCanonicalPointerReturningRecord(
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
                    record.state = "JS_QUEUED";
                    record.message = "Lease expired; requeued";
                    record.executorId = "";
                    record.nextAttemptAtMs = nowMs;
                    record.readyPointerKey =
                        Keys.reconcileReadyPointerByDue(
                            nowMs, record.accountId, record.laneKey, record.jobId);
                  }
                  return record;
                })
            .isPresent();
    if (updated && !blank(expiredEpoch.get())) {
      clearLeaseIfEpochMatches(
          canonicalRecord.accountId, canonicalRecord.jobId, expiredEpoch.get());
      jobIndexStore
          .readCanonicalRecordByKey(canonicalKey)
          .ifPresent(projectionUpdater::refreshContributionChain);
    }
  }

  private boolean hasUnexpiredJobLease(String accountId, String jobId, long nowMs) {
    StoredJobLease lease = loadLease(accountId, jobId).orElse(null);
    return lease != null && !blank(lease.epoch) && lease.expiresAtMs > nowMs;
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

  private List<CasOp> buildLeaseMirrorOps(StoredJobLease previousLease, StoredJobLease nextLease) {
    String leasePointerKey =
        Keys.reconcileJobLeasePointerById(nextLease.accountId, nextLease.jobId);
    Pointer currentLeasePointer = leaseBackend.loadPointer(leasePointerKey).orElse(null);
    long expectedVersion = currentLeasePointer == null ? 0L : currentLeasePointer.getVersion();
    Pointer nextLeasePointer =
        Pointer.newBuilder()
            .setKey(leasePointerKey)
            .setBlobUri(payloadStore.encodeInlineJobLease(nextLease))
            .setVersion(expectedVersion + 1L)
            .build();
    List<CasOp> ops = new ArrayList<>();
    ops.add(new CasUpsert(leasePointerKey, expectedVersion, nextLeasePointer));

    String canonicalPointerKey =
        Keys.reconcileJobStateRowById(nextLease.accountId, nextLease.jobId);
    String previousExpiryKey = leaseExpiryPointerKey(previousLease);
    String nextExpiryKey = leaseExpiryPointerKey(nextLease);
    if (!Objects.equals(previousExpiryKey, nextExpiryKey)) {
      if (!blank(previousExpiryKey)) {
        Pointer prev = leaseBackend.loadPointer(previousExpiryKey).orElse(null);
        if (prev != null && canonicalPointerKey.equals(prev.getBlobUri())) {
          ops.add(new CasDelete(previousExpiryKey, prev.getVersion()));
        }
      }
      if (!blank(nextExpiryKey)) {
        Pointer existing = leaseBackend.loadPointer(nextExpiryKey).orElse(null);
        long expiryExpectedVersion = existing == null ? 0L : existing.getVersion();
        Pointer nextExpiry =
            Pointer.newBuilder()
                .setKey(nextExpiryKey)
                .setBlobUri(canonicalPointerKey)
                .setVersion(expiryExpectedVersion + 1L)
                .build();
        ops.add(new CasUpsert(nextExpiryKey, expiryExpectedVersion, nextExpiry));
      }
    }
    return ops;
  }

  private List<CasOp> buildLeaseDeleteOps(StoredJobLease lease) {
    List<CasOp> ops = new ArrayList<>();
    String leasePointerKey = Keys.reconcileJobLeasePointerById(lease.accountId, lease.jobId);
    Pointer leasePointer = leaseBackend.loadPointer(leasePointerKey).orElse(null);
    if (leasePointer != null) {
      ops.add(new CasDelete(leasePointerKey, leasePointer.getVersion()));
    }
    String expiryKey = leaseExpiryPointerKey(lease);
    if (!blank(expiryKey)) {
      Pointer expiryPointer = leaseBackend.loadPointer(expiryKey).orElse(null);
      String canonicalPointerKey = Keys.reconcileJobStateRowById(lease.accountId, lease.jobId);
      if (expiryPointer != null && canonicalPointerKey.equals(expiryPointer.getBlobUri())) {
        ops.add(new CasDelete(expiryKey, expiryPointer.getVersion()));
      }
    }
    return ops;
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

  private static String blankToEmpty(String value) {
    return value == null ? "" : value;
  }
}
