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

package ai.floedb.floecat.service.reconciler.jobs.durable.store;

import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.LeasedJob;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredJobDefinition;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredJobLease;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileJobExecutionLoader;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileLeaseStateCodec;
import ai.floedb.floecat.service.repo.model.Keys;
import jakarta.enterprise.context.ApplicationScoped;
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

@ApplicationScoped
public class NativeReconcileLeaseStore implements ReconcileLeaseStore {
  private static final Logger LOG = Logger.getLogger(NativeReconcileLeaseStore.class);
  private static final long INVALID_ORDERED_POINTER_MS = -1L;

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

  public Optional<LeasedJob> leaseCanonical(
      String canonicalPointerKey,
      String readyPointerKey,
      long now,
      CanonicalPointerSnapshot initialSnapshot,
      StoredReconcileJob initialRecord) {
    return leaseCanonical(
        canonicalPointerKey, readyPointerKey, now, initialSnapshot, initialRecord, null);
  }

  @Override
  public Optional<LeasedJob> leaseCanonical(
      String canonicalPointerKey,
      String readyPointerKey,
      long now,
      CanonicalPointerSnapshot initialSnapshot,
      StoredReconcileJob initialRecord,
      LeaseAttemptStats attemptStats) {
    for (int i = 0; i < casMax; i++) {
      CanonicalPointerSnapshot currentSnapshot;
      StoredReconcileJob record;
      if (i == 0 && initialSnapshot != null && initialRecord != null) {
        currentSnapshot = initialSnapshot;
        record = initialRecord;
      } else {
        currentSnapshot = jobIndexStore.loadCanonicalSnapshot(canonicalPointerKey).orElse(null);
        if (currentSnapshot == null) {
          recordLeaseFailure(attemptStats, "canonical_missing");
          return Optional.empty();
        }

        var recordOpt = jobIndexStore.readRecord(currentSnapshot);
        if (recordOpt.isEmpty()) {
          recordLeaseFailure(attemptStats, "record_missing");
          return Optional.empty();
        }
        record = recordOpt.get();
      }
      StoredReconcileJob baseline = jobIndexStore.cloneStoredRecord(record);
      StoredReconcileJob current = jobIndexStore.cloneStoredRecord(record);

      if (Boolean.TRUE.equals(isTerminalState.test(current.state))
          || "JS_WAITING".equals(current.state)) {
        recordLeaseFailure(
            attemptStats, "JS_WAITING".equals(current.state) ? "waiting" : "terminal");
        return Optional.empty();
      }

      if (hasLiveLease(current, true, now)) {
        recordLeaseFailure(attemptStats, "live_job_lease");
        return Optional.empty();
      }

      if ("JS_CANCELLING".equals(current.state)) {
        recordLeaseFailure(attemptStats, "cancelling");
        return Optional.empty();
      }

      String nextLeaseEpoch = "";
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

      var currentLeaseSnapshot =
          leaseBackend.loadLease(current.accountId, current.jobId).orElse(null);
      StoredJobLease currentLease =
          currentLeaseSnapshot == null
              ? StoredJobLease.empty(current.accountId, current.jobId)
              : leaseStateCodec
                  .decode(currentLeaseSnapshot.encodedLease())
                  .orElse(StoredJobLease.empty(current.accountId, current.jobId));
      if (currentLease.epoch != null
          && !currentLease.epoch.isBlank()
          && currentLease.expiresAtMs > now) {
        recordLeaseFailure(attemptStats, "active_coordination_lease");
        return Optional.empty();
      }
      var ownerClaims =
          buildExclusivityClaimWrites(current, canonicalPointerKey, now, attemptStats).orElse(null);
      if (ownerClaims == null) {
        recordLeaseFailureIfUnset(attemptStats, "owner_claim_conflict");
        return Optional.empty();
      }
      StoredJobLease nextLease =
          StoredJobLease.active(current.accountId, current.jobId, nextLeaseEpoch, now + leaseMs);

      long mutationStartMs = System.currentTimeMillis();
      ReconcileJobIndexStore.JobIndexWriteBatch jobIndexBatch =
          buildLeaseJobIndexWriteBatch(currentSnapshot, baseline, current);
      if (leaseBackend.compareAndSetBatch(
          jobIndexBatch,
          mergeLeaseWrites(
              ownerClaims,
              buildLeaseCoordinationWriteBatch(currentLeaseSnapshot, currentLease, nextLease)))) {
        try {
          long mutationElapsedMs = System.currentTimeMillis() - mutationStartMs;
          long definitionStartMs = System.currentTimeMillis();
          StoredJobDefinition definition = executionLoader.requireDefinition(current);
          long definitionElapsedMs = System.currentTimeMillis() - definitionStartMs;
          long snapshotTaskElapsedMs = 0L;
          ReconcileSnapshotTask snapshotTask = ReconcileSnapshotTask.empty();
          if (current.jobKind() == ReconcileJobKind.PLAN_SNAPSHOT
              || current.jobKind() == ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE) {
            snapshotTask = executionLoader.compactSnapshotTask(current);
          }
          long fileGroupTaskElapsedMs = 0L;
          ReconcileFileGroupTask fileGroupTask = ReconcileFileGroupTask.empty();
          if (current.jobKind() == ReconcileJobKind.EXEC_FILE_GROUP) {
            fileGroupTask = executionLoader.compactFileGroupTask(current);
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
                  current.parentJobId(),
                  current.laneKey));
        } catch (RuntimeException e) {
          if (isMissingRequiredJobDefinition(e)) {
            failLeaseCanonicalOnHydrationFailure(
                canonicalPointerKey, baseline, nextLeaseEpoch, now);
            recordLeaseFailure(attemptStats, "hydration_missing_definition");
            return Optional.empty();
          }
          rollbackLeaseCanonicalOnHydrationFailure(canonicalPointerKey, baseline, nextLeaseEpoch);
          throw e;
        }
      }
      recordLeaseFailure(attemptStats, "cas_conflict");
    }

    return Optional.empty();
  }

  private static void recordLeaseFailure(LeaseAttemptStats attemptStats, String reason) {
    if (attemptStats != null) {
      attemptStats.recordFailure(reason);
    }
  }

  private static void recordLeaseFailureIfUnset(LeaseAttemptStats attemptStats, String reason) {
    if (attemptStats != null && blankToEmpty(attemptStats.failureReason()).isBlank()) {
      attemptStats.recordFailure(reason);
    }
  }

  private ReconcileJobIndexStore.JobIndexWriteBatch buildLeaseJobIndexWriteBatch(
      CanonicalPointerSnapshot currentSnapshot,
      StoredReconcileJob previous,
      StoredReconcileJob current) {
    return jobIndexStore.buildJobIndexWriteBatch(currentSnapshot, previous, current);
  }

  public void rollbackLeaseCanonicalOnHydrationFailure(
      String canonicalPointerKey, StoredReconcileJob baseline, String leaseEpoch) {
    if (baseline == null || blank(canonicalPointerKey) || blank(leaseEpoch)) {
      return;
    }
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
      StoredReconcileJob record,
      String canonicalPointerKey,
      long now,
      LeaseAttemptStats attemptStats) {
    List<ReconcileLeaseBackend.LeaseWriteOp> writes = new ArrayList<>(4);
    var laneWrites = buildLaneLeaseClaimWrites(record, canonicalPointerKey, now).orElse(null);
    if (laneWrites == null && requiresLaneLease(record)) {
      recordLeaseFailure(attemptStats, ownerClaimConflictReason(record, "lane"));
      return Optional.empty();
    }
    if (laneWrites != null) {
      writes.addAll(laneWrites);
    }
    var snapshotWrites =
        buildSnapshotLeaseClaimWrites(record, canonicalPointerKey, now).orElse(null);
    if (snapshotWrites == null && requiresSnapshotLease(record)) {
      recordLeaseFailure(attemptStats, ownerClaimConflictReason(record, "snapshot"));
      return Optional.empty();
    }
    if (snapshotWrites != null) {
      writes.addAll(snapshotWrites);
    }
    return Optional.of(List.copyOf(writes));
  }

  private Optional<List<ReconcileLeaseBackend.LeaseWriteOp>> buildLaneLeaseClaimWrites(
      StoredReconcileJob record, String canonicalPointerKey, long now) {
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
      return owner != null && hasActiveLaneLease(owner, now)
          ? Optional.empty()
          : Optional.of(
              ownerClaimWrites(
                  lanePointerKey,
                  existing.version(),
                  canonicalPointerKey,
                  owner,
                  ownerLeaseSnapshot));
    }
    if (owner != null && hasActiveLaneLease(owner, now)) {
      return Optional.empty();
    }
    return Optional.of(
        ownerClaimWrites(
            lanePointerKey, existing.version(), canonicalPointerKey, owner, ownerLeaseSnapshot));
  }

  private static String ownerClaimConflictReason(StoredReconcileJob record, String ownerScope) {
    String scope = blankToEmpty(ownerScope);
    if (scope.isBlank()) {
      scope = "unknown";
    }
    return "owner_claim_" + scope + "_conflict_" + jobKindReasonSuffix(record);
  }

  private static String jobKindReasonSuffix(StoredReconcileJob record) {
    if (record == null || record.jobKind() == null) {
      return "unknown";
    }
    return switch (record.jobKind()) {
      case PLAN_CONNECTOR -> "plan_connector";
      case PLAN_TABLE -> "plan_table";
      case PLAN_VIEW -> "plan_view";
      case PLAN_SNAPSHOT -> "plan_snapshot";
      case EXEC_FILE_GROUP -> "exec_file_group";
      case FINALIZE_SNAPSHOT_CAPTURE -> "finalize_snapshot_capture";
    };
  }

  private Optional<List<ReconcileLeaseBackend.LeaseWriteOp>> buildSnapshotLeaseClaimWrites(
      StoredReconcileJob record, String canonicalPointerKey, long now) {
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
      return owner != null && hasActiveSnapshotLease(owner, now)
          ? Optional.empty()
          : Optional.of(
              ownerClaimWrites(
                  pointerKey, existing.version(), canonicalPointerKey, owner, ownerLeaseSnapshot));
    }
    if (owner != null && hasActiveSnapshotLease(owner, now)) {
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
    if (requireUnexpiredLease && lease.expiresAtMs <= now && !allowExpiredWithinGrace) {
      logLeaseSkip(
          op,
          "Skipping %s for reconcile job %s due to expired lease expiresAtMs=%d now=%d",
          op,
          jobId,
          lease.expiresAtMs,
          now);
      return false;
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
    return leaseBackend
        .loadLease(accountId, jobId)
        .flatMap(snapshot -> leaseStateCodec.decode(snapshot.encodedLease()));
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
    long startedAtMs = System.currentTimeMillis();
    AtomicReference<Long> previousExpiresAtMs = new AtomicReference<>(null);
    AtomicReference<Long> millisUntilExpiryBeforeRenew = new AtomicReference<>(null);
    Optional<StoredJobLease> renewed =
        writeLease(
            accountId,
            jobId,
            current -> {
              if (blank(leaseEpoch)) {
                return null;
              }
              long now = System.currentTimeMillis();
              if (blank(current.epoch)
                  || !leaseEpoch.equals(current.epoch)
                  || current.expiresAtMs <= now) {
                return null;
              }
              previousExpiresAtMs.set(current.expiresAtMs);
              millisUntilExpiryBeforeRenew.set(current.expiresAtMs - now);
              current.expiresAtMs = now + leaseMs;
              return current;
            });
    renewed.ifPresent(
        lease ->
            logLeaseRenewal(
                accountId,
                jobId,
                leaseEpoch,
                previousExpiresAtMs.get(),
                lease.expiresAtMs,
                millisUntilExpiryBeforeRenew.get(),
                System.currentTimeMillis() - startedAtMs));
    return renewed;
  }

  public LeaseExpiryScanPage scanExpiredLeasePointersPage(
      long nowMs, int pageSize, String pageToken) {
    int limit = Math.max(1, pageSize);
    LeaseExpiryScanPage scanPage =
        leaseBackend.scanExpiredLeaseEntries(limit, pageToken == null ? "" : pageToken);
    if (scanPage.entries().isEmpty()) {
      return new LeaseExpiryScanPage(List.of(), "");
    }
    List<LeaseExpiryEntry> expired = new ArrayList<>(scanPage.entries().size());
    for (LeaseExpiryEntry entry : scanPage.entries()) {
      long expiresAtMs = parseLeaseExpiryMillis(entry.leaseExpiryPointerKey());
      if (expiresAtMs == INVALID_ORDERED_POINTER_MS) {
        continue;
      }
      if (expiresAtMs > nowMs) {
        return new LeaseExpiryScanPage(expired, "");
      }
      expired.add(entry);
    }
    return new LeaseExpiryScanPage(expired, scanPage.nextPageToken());
  }

  public boolean clearLeaseIfEpochMatches(String accountId, String jobId, String leaseEpoch) {
    if (blank(accountId) || blank(jobId)) {
      return false;
    }
    for (int i = 0; i < casMax; i++) {
      var currentSnapshot = leaseBackend.loadLease(accountId, jobId).orElse(null);
      if (currentSnapshot == null) {
        return false;
      }
      StoredJobLease current =
          leaseStateCodec
              .decode(currentSnapshot.encodedLease())
              .orElse(StoredJobLease.empty(accountId, jobId));
      if (blank(leaseEpoch) || !leaseEpoch.equals(current.epoch)) {
        return false;
      }
      List<ReconcileLeaseBackend.LeaseWriteOp> writes = new ArrayList<>(2);
      writes.add(
          new ReconcileLeaseBackend.LeaseRecordDelete(accountId, jobId, currentSnapshot.version()));
      String expiryKey = leaseExpiryPointerKey(current.expiresAtMs, accountId, jobId);
      if (!expiryKey.isBlank()) {
        var expirySnapshot = leaseBackend.loadLeaseExpiry(expiryKey).orElse(null);
        String canonicalPointerKey = Keys.reconcileJobStateRowById(accountId, jobId);
        if (expirySnapshot != null
            && canonicalPointerKey.equals(expirySnapshot.canonicalPointerKey())) {
          writes.add(
              new ReconcileLeaseBackend.LeaseExpiryDelete(expiryKey, expirySnapshot.version()));
        }
      }
      if (leaseBackend.compareAndSetBatch(
          ReconcileJobIndexStore.JobIndexWriteBatch.empty(),
          new ReconcileLeaseBackend.LeaseWriteBatch(List.copyOf(writes)))) {
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
      var existing = leaseBackend.loadOwner(lanePointerKey).orElse(null);
      if (existing == null) {
        if (leaseBackend.compareAndSetBatch(
            ReconcileJobIndexStore.JobIndexWriteBatch.empty(),
            new ReconcileLeaseBackend.LeaseWriteBatch(
                List.of(
                    new ReconcileLeaseBackend.LeaseOwnerUpsert(
                        lanePointerKey, 0L, canonicalPointerKey))))) {
          return true;
        }
        continue;
      }
      if (canonicalPointerKey.equals(existing.canonicalPointerKey())) {
        var owner = jobIndexStore.readCanonicalRecordByKey(existing.canonicalPointerKey());
        if (owner.isPresent() && hasActiveLaneLease(owner.get(), now)) {
          return false;
        }
        return true;
      }

      var owner = jobIndexStore.readCanonicalRecordByKey(existing.canonicalPointerKey());
      if (owner.isPresent()
          && record.jobId.equals(owner.get().jobId)
          && record.accountId.equals(owner.get().accountId)) {
        if (hasActiveLaneLease(owner.get(), now)) {
          return false;
        }
        return true;
      }
      if (owner.isPresent() && hasActiveLaneLease(owner.get(), now)) {
        return false;
      }
      if (leaseBackend.compareAndSetBatch(
          ReconcileJobIndexStore.JobIndexWriteBatch.empty(),
          new ReconcileLeaseBackend.LeaseWriteBatch(
              List.of(
                  new ReconcileLeaseBackend.LeaseOwnerDelete(
                      lanePointerKey, existing.version()))))) {
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
    var existing = leaseBackend.loadOwner(lanePointerKey).orElse(null);
    if (existing == null) {
      return;
    }
    if (!expectedReference.equals(existing.canonicalPointerKey())) {
      var owner = jobIndexStore.readCanonicalRecordByKey(existing.canonicalPointerKey());
      if (owner.isEmpty()
          || !record.jobId.equals(owner.get().jobId)
          || !record.accountId.equals(owner.get().accountId)) {
        return;
      }
    }
    var owner = jobIndexStore.readCanonicalRecordByKey(existing.canonicalPointerKey());
    if (owner.isEmpty()
        || !record.jobId.equals(owner.get().jobId)
        || !record.accountId.equals(owner.get().accountId)) {
      return;
    }
    if (holdsExecutionLease(owner.get())
        && hasActiveLaneLease(owner.get(), System.currentTimeMillis())) {
      return;
    }
    leaseBackend.compareAndSetBatch(
        ReconcileJobIndexStore.JobIndexWriteBatch.empty(),
        new ReconcileLeaseBackend.LeaseWriteBatch(
            List.of(
                new ReconcileLeaseBackend.LeaseOwnerDelete(lanePointerKey, existing.version()))));
  }

  public boolean tryAcquireSnapshotLease(
      StoredReconcileJob record, String expectedReference, long now) {
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
                        pointerKey, 0L, expectedReference))))) {
          return true;
        }
        continue;
      }
      if (expectedReference.equals(existing.canonicalPointerKey())) {
        return true;
      }
      var owner = jobIndexStore.readCanonicalRecordByKey(existing.canonicalPointerKey());
      if (owner.isEmpty()) {
        leaseBackend.compareAndSetBatch(
            ReconcileJobIndexStore.JobIndexWriteBatch.empty(),
            new ReconcileLeaseBackend.LeaseWriteBatch(
                List.of(
                    new ReconcileLeaseBackend.LeaseOwnerDelete(pointerKey, existing.version()))));
        continue;
      }
      if (record.jobId.equals(owner.get().jobId)
          && record.accountId.equals(owner.get().accountId)) {
        return true;
      }
      if (!hasActiveSnapshotLease(owner.get(), now)) {
        leaseBackend.compareAndSetBatch(
            ReconcileJobIndexStore.JobIndexWriteBatch.empty(),
            new ReconcileLeaseBackend.LeaseWriteBatch(
                List.of(
                    new ReconcileLeaseBackend.LeaseOwnerDelete(pointerKey, existing.version()))));
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
    var existing = leaseBackend.loadOwner(pointerKey).orElse(null);
    if (existing == null) {
      return;
    }
    if (!blank(expectedReference) && !expectedReference.equals(existing.canonicalPointerKey())) {
      var owner = jobIndexStore.readCanonicalRecordByKey(existing.canonicalPointerKey());
      if (owner.isEmpty()
          || !record.jobId.equals(owner.get().jobId)
          || !record.accountId.equals(owner.get().accountId)) {
        return;
      }
    }
    var owner = jobIndexStore.readCanonicalRecordByKey(existing.canonicalPointerKey());
    if (owner.isEmpty()
        || !record.jobId.equals(owner.get().jobId)
        || !record.accountId.equals(owner.get().accountId)) {
      return;
    }
    if (holdsExecutionLease(owner.get())
        && hasActiveSnapshotLease(owner.get(), System.currentTimeMillis())) {
      return;
    }
    leaseBackend.compareAndSetBatch(
        ReconcileJobIndexStore.JobIndexWriteBatch.empty(),
        new ReconcileLeaseBackend.LeaseWriteBatch(
            List.of(new ReconcileLeaseBackend.LeaseOwnerDelete(pointerKey, existing.version()))));
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
    return Keys.reconcileJobLeaseExpiryPointer(expiresAtMs, accountId, jobId);
  }

  // Lease coordination state is runtime ownership state and is intentionally separate from
  // canonical job-index pointers.
  public ReconcileLeaseBackend.LeaseWriteBatch buildLeaseCoordinationWriteBatch(
      ReconcileLeaseBackend.LeaseRecordSnapshot currentLeaseSnapshot,
      StoredJobLease previousLease,
      StoredJobLease nextLease) {
    return buildLeaseCoordinationWriteOps(currentLeaseSnapshot, previousLease, nextLease);
  }

  public void reclaimExpiredLease(LeaseExpiryEntry leaseExpiryEntry, long nowMs) {
    if (leaseExpiryEntry == null || blank(leaseExpiryEntry.canonicalPointerKey())) {
      return;
    }
    String canonicalKey = leaseExpiryEntry.canonicalPointerKey();
    var canonicalRecordOpt = jobIndexStore.readCanonicalRecordByKey(canonicalKey);
    if (canonicalRecordOpt.isEmpty()) {
      return;
    }
    var canonicalRecord = canonicalRecordOpt.get();
    StoredJobLease lease = loadLease(canonicalRecord).orElse(null);
    if (lease == null || blank(lease.epoch) || lease.expiresAtMs <= 0L) {
      return;
    }
    String expectedLeaseExpiryKey =
        leaseExpiryPointerKey(lease.expiresAtMs, canonicalRecord.accountId, canonicalRecord.jobId);
    if (!expectedLeaseExpiryKey.equals(leaseExpiryEntry.leaseExpiryPointerKey())) {
      return;
    }
    if (lease.expiresAtMs > nowMs || nowMs - lease.expiresAtMs <= leaseRenewGraceMs) {
      return;
    }
    reclaimRunningOrCancellingJob(canonicalKey, canonicalRecord, lease, nowMs);
  }

  private boolean hasActiveLaneLease(StoredReconcileJob record, long now) {
    return record != null && hasUnexpiredJobLease(record.accountId, record.jobId, now);
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

  private void reclaimRunningOrCancellingJob(
      String canonicalKey, StoredReconcileJob canonicalRecord, StoredJobLease lease, long nowMs) {
    boolean repairIncompletePlannerFanout = waitingPlannerFanoutIncomplete(canonicalRecord);
    if (!"JS_RUNNING".equals(canonicalRecord.state)
        && !"JS_CANCELLING".equals(canonicalRecord.state)
        && !repairIncompletePlannerFanout) {
      clearLeaseIfEpochMatches(canonicalRecord.accountId, canonicalRecord.jobId, lease.epoch);
      return;
    }
    AtomicReference<String> expiredEpoch = new AtomicReference<>("");
    boolean updated =
        mutateCanonicalJob
            .apply(
                canonicalKey,
                record -> {
                  boolean repairingWaitingPlanner =
                      repairIncompletePlannerFanout && "JS_WAITING".equals(record.state);
                  if (!"JS_RUNNING".equals(record.state)
                      && !"JS_CANCELLING".equals(record.state)
                      && !repairingWaitingPlanner) {
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
                    record.attempt = Math.max(0, record.attempt) + 1;
                    record.lastError =
                        repairingWaitingPlanner
                            ? "Planner fanout incomplete after outcome commit"
                            : "Lease expired";
                    record.executorId = "";
                    record.childrenFinalized = false;
                    if (record.attempt >= maxAttempts) {
                      record.state = "JS_FAILED";
                      record.message =
                          repairingWaitingPlanner
                              ? "Planner fanout repair failed repeatedly"
                              : "Lease expired repeatedly; failed";
                      if (record.startedAtMs <= 0L) {
                        record.startedAtMs = nowMs;
                      }
                      record.finishedAtMs = nowMs;
                      record.readyPointerKey = null;
                    } else {
                      long nextAttemptAtMs =
                          nowMs + Math.max(0L, backoffMs.applyAsLong(record.attempt));
                      record.state = "JS_QUEUED";
                      record.message =
                          repairingWaitingPlanner
                              ? "Planner fanout incomplete; requeued for repair"
                              : "Lease expired; requeued";
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

  private boolean waitingPlannerFanoutIncomplete(StoredReconcileJob record) {
    if (record == null
        || !"JS_WAITING".equals(record.state)
        || record.jobKind() != ReconcileJobKind.PLAN_CONNECTOR
        || blank(record.plannerOutcomeFingerprint)
        || blank(record.accountId)
        || blank(record.jobId)
        || Math.max(0L, record.expectedDirectChildren) <= 0L) {
      return false;
    }
    try {
      return jobIndexStore.countStoredChildJobs(record.accountId, record.jobId)
          < Math.max(0L, record.expectedDirectChildren);
    } catch (RuntimeException ignored) {
      // Do not discard the only repair capability when the child-count read is temporarily
      // unavailable. Replanning is deduped and fingerprint-guarded.
      return true;
    }
  }

  private boolean hasActiveSnapshotLease(StoredReconcileJob record, long now) {
    return record != null
        && record.jobKind() == ReconcileJobKind.PLAN_SNAPSHOT
        && !blank(record.snapshotTaskTableId)
        && record.snapshotTaskSnapshotId >= 0L
        && hasUnexpiredJobLease(record.accountId, record.jobId, now);
  }

  private boolean holdsExecutionLease(StoredReconcileJob record) {
    return record != null
        && ("JS_RUNNING".equals(record.state) || "JS_CANCELLING".equals(record.state));
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
    for (int i = 0; i < casMax; i++) {
      var currentSnapshot = leaseBackend.loadLease(accountId, jobId).orElse(null);
      StoredJobLease current =
          currentSnapshot == null
              ? StoredJobLease.empty(accountId, jobId)
              : leaseStateCodec
                  .decode(currentSnapshot.encodedLease())
                  .orElse(StoredJobLease.empty(accountId, jobId));
      StoredJobLease next = mutator.apply(cloneLease(current));
      if (next == null) {
        return Optional.empty();
      }
      if (leaseStateEquals(current, next)) {
        return Optional.of(current);
      }
      if (leaseBackend.compareAndSetBatch(
          ReconcileJobIndexStore.JobIndexWriteBatch.empty(),
          buildLeaseCoordinationWriteOps(currentSnapshot, current, next))) {
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

  private static boolean isMissingRequiredJobDefinition(Throwable error) {
    return error != null
        && error.getMessage() != null
        && error.getMessage().contains("missing required job definition");
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

  private ReconcileLeaseBackend.LeaseWriteBatch buildLeaseCoordinationWriteOps(
      ReconcileLeaseBackend.LeaseRecordSnapshot currentLeaseSnapshot,
      StoredJobLease previousLease,
      StoredJobLease nextLease) {
    List<ReconcileLeaseBackend.LeaseWriteOp> ops = new ArrayList<>(3);
    String canonicalPointerKey =
        Keys.reconcileJobStateRowById(previousLease.accountId, previousLease.jobId);
    long leaseExpectedVersion = currentLeaseSnapshot == null ? 0L : currentLeaseSnapshot.version();
    ops.add(
        new ReconcileLeaseBackend.LeaseRecordUpsert(
            previousLease.accountId,
            previousLease.jobId,
            leaseExpectedVersion,
            leaseStateCodec.encode(nextLease)));

    String previousExpiryKey = leaseExpiryPointerKey(previousLease);
    String nextExpiryKey = leaseExpiryPointerKey(nextLease);
    if (!Objects.equals(previousExpiryKey, nextExpiryKey)) {
      if (!previousExpiryKey.isBlank()) {
        var previousExpiryPointer = leaseBackend.loadLeaseExpiry(previousExpiryKey).orElse(null);
        if (previousExpiryPointer != null
            && canonicalPointerKey.equals(previousExpiryPointer.canonicalPointerKey())) {
          ops.add(
              new ReconcileLeaseBackend.LeaseExpiryDelete(
                  previousExpiryKey, previousExpiryPointer.version()));
        }
      }
      if (!nextExpiryKey.isBlank()) {
        long expectedVersion =
            leaseBackend
                .loadLeaseExpiry(nextExpiryKey)
                .map(ReconcileLeaseBackend.LeaseExpirySnapshot::version)
                .orElse(0L);
        ops.add(
            new ReconcileLeaseBackend.LeaseExpiryUpsert(
                nextExpiryKey, expectedVersion, canonicalPointerKey));
      }
    }
    return new ReconcileLeaseBackend.LeaseWriteBatch(List.copyOf(ops));
  }

  private void logLeaseSkip(String op, String format, Object... args) {
    LOG.debugf(format, args);
  }

  private void logLeaseRenewal(
      String accountId,
      String jobId,
      String leaseEpoch,
      Long oldExpiresAtMs,
      long newExpiresAtMs,
      Long millisUntilExpiryBeforeRenew,
      long renewLatencyMs) {
    if (!LOG.isDebugEnabled()) {
      return;
    }
    String jobKind = resolveJobKindForLeaseLog(accountId, jobId);
    String message =
        String.format(
            "renewLease success jobId=%s kind=%s leaseEpoch=%s oldExpiresAt=%d newExpiresAt=%d"
                + " millisUntilExpiryBeforeRenew=%d renewLatencyMs=%d",
            jobId,
            jobKind,
            leaseEpoch,
            oldExpiresAtMs == null ? -1L : oldExpiresAtMs,
            newExpiresAtMs,
            millisUntilExpiryBeforeRenew == null ? Long.MIN_VALUE : millisUntilExpiryBeforeRenew,
            renewLatencyMs);
    LOG.debug(message);
  }

  private String resolveJobKindForLeaseLog(String accountId, String jobId) {
    if (blank(accountId) || blank(jobId)) {
      return "";
    }
    try {
      String canonicalPointerKey = Keys.reconcileJobStateRowById(accountId, jobId);
      var snapshot = jobIndexStore.loadCanonicalSnapshot(canonicalPointerKey).orElse(null);
      if (snapshot == null) {
        return "";
      }
      var record = jobIndexStore.readRecord(snapshot).orElse(null);
      return record == null ? "" : blankToEmpty(record.jobKind);
    } catch (RuntimeException error) {
      LOG.debugf(
          error, "renewLease log context lookup failed jobId=%s accountId=%s", jobId, accountId);
      return "";
    }
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }

  private long parseLeaseExpiryMillis(String leaseExpiryPointerKey) {
    return parseTimestampFromOrderedPointer(
        leaseExpiryPointerKey, Keys.reconcileJobLeaseExpiryPointerPrefix());
  }

  private long parseTimestampFromOrderedPointer(String pointerKey, String prefix) {
    if (blank(pointerKey)) {
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
    if (blank(key)) {
      return "/";
    }
    return key.startsWith("/") ? key : "/" + key;
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value.trim();
  }
}
