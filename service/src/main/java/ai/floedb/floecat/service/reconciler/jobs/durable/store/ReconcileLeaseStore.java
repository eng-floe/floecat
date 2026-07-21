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

import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.LeasedJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredJobLease;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileJobExecutionLoader;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileLeaseStateCodec;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.IntToLongFunction;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

public interface ReconcileLeaseStore {
  record LeaseExpiryEntry(String leaseExpiryPointerKey, String canonicalPointerKey) {}

  record LeaseExpiryScanPage(List<LeaseExpiryEntry> entries, String nextPageToken) {}

  final class LeaseAttemptStats {
    private String failureReason = "";

    public void recordFailure(String reason) {
      String normalized = reason == null ? "" : reason.trim();
      if (!normalized.isBlank()) {
        failureReason = normalized;
      }
    }

    public String failureReason() {
      return failureReason;
    }
  }

  @FunctionalInterface
  interface CanonicalJobMutator {
    Optional<ReconcileJobIndexStore.CanonicalEnvelope> apply(
        String canonicalPointerKey, UnaryOperator<StoredReconcileJob> mutator);
  }

  void bind(
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
      IntToLongFunction backoffMs);

  Optional<LeasedJob> leaseCanonical(
      String canonicalPointerKey,
      String readyPointerKey,
      long now,
      CanonicalPointerSnapshot initialSnapshot,
      StoredReconcileJob initialRecord);

  default Optional<LeasedJob> leaseCanonical(
      String canonicalPointerKey,
      String readyPointerKey,
      long now,
      CanonicalPointerSnapshot initialSnapshot,
      StoredReconcileJob initialRecord,
      LeaseAttemptStats attemptStats) {
    return leaseCanonical(
        canonicalPointerKey, readyPointerKey, now, initialSnapshot, initialRecord);
  }

  boolean hasActiveLease(
      String jobId,
      String leaseEpoch,
      StoredReconcileJob current,
      String context,
      boolean allowWaitingState,
      boolean logMissingLease,
      boolean allowExpiredWithinGrace);

  boolean hasLiveLease(StoredReconcileJob record, boolean allowCancelling, long now);

  Optional<StoredJobLease> loadLease(String accountId, String jobId);

  Optional<StoredJobLease> loadLease(StoredReconcileJob record);

  Optional<StoredJobLease> mutateLease(
      String accountId, String jobId, UnaryOperator<StoredJobLease> mutator);

  Optional<StoredJobLease> renewLeaseIfEpochMatches(
      String accountId, String jobId, String leaseEpoch);

  LeaseExpiryScanPage scanExpiredLeasePointersPage(long nowMs, int pageSize, String pageToken);

  void reclaimExpiredLease(ReconcileLeaseStore.LeaseExpiryEntry leaseExpiryEntry, long nowMs);

  boolean clearLeaseIfEpochMatches(String accountId, String jobId, String leaseEpoch);

  boolean tryAcquireLaneLease(StoredReconcileJob record, String canonicalPointerKey, long nowMs);

  void clearLaneLeaseIfOwned(StoredReconcileJob record, String expectedReference);

  boolean tryAcquireSnapshotLease(
      StoredReconcileJob record, String canonicalPointerKey, long nowMs);

  void clearSnapshotLeaseIfOwned(StoredReconcileJob record, String expectedReference);

  String leaseExpiryPointerKey(StoredJobLease lease);

  String leaseExpiryPointerKey(long expiresAtMs, String accountId, String jobId);
}
