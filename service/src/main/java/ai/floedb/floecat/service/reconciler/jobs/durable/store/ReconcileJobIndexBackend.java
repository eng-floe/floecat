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

import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.List;
import java.util.Optional;

public interface ReconcileJobIndexBackend {
  enum LegacyMigration {
    CLEANUP,
    LOOKUP
  }

  record LegacyMigrationProgress(
      String pageToken,
      int changed,
      int unresolvable,
      int conflicted,
      int retryable,
      boolean quietPassComplete) {
    public LegacyMigrationProgress {
      pageToken = pageToken == null ? "" : pageToken;
    }

    public static LegacyMigrationProgress empty() {
      return new LegacyMigrationProgress("", 0, 0, 0, 0, false);
    }
  }

  record LegacyMigrationLease(long fence, LegacyMigrationProgress progress) {
    public LegacyMigrationLease {
      progress = progress == null ? LegacyMigrationProgress.empty() : progress;
    }
  }

  record JobIndexQueryPage(List<JobIndexEntrySnapshot> entries, String nextPageToken) {}

  record LegacyLookupMigrationPage(
      int scanned, int migrated, int conflicted, int retryable, String nextPageToken) {}

  record LegacyCleanupMigrationPage(
      int scanned,
      int manifestsUpdated,
      int unresolvable,
      int conflicted,
      int retryable,
      String nextPageToken,
      List<String> canonicalPointerKeys) {
    public LegacyCleanupMigrationPage(
        int scanned,
        int manifestsUpdated,
        int unresolvable,
        int conflicted,
        int retryable,
        String nextPageToken) {
      this(
          scanned, manifestsUpdated, unresolvable, conflicted, retryable, nextPageToken, List.of());
    }

    public LegacyCleanupMigrationPage {
      canonicalPointerKeys =
          canonicalPointerKeys == null ? List.of() : List.copyOf(canonicalPointerKeys);
    }
  }

  /**
   * A cleanup claim. {@code footprintDrained} permits a canonical-only delete after the backend has
   * durably completed its bounded legacy-reference scan.
   */
  record JobCleanupSession(
      CanonicalPointerSnapshot snapshot,
      ReconcileJobIndexCleanupManifest manifest,
      boolean cleanupLocked,
      boolean footprintDrained) {
    public JobCleanupSession(
        CanonicalPointerSnapshot snapshot,
        ReconcileJobIndexCleanupManifest manifest,
        boolean cleanupLocked) {
      this(snapshot, manifest, cleanupLocked, false);
    }
  }

  /** Loads an index entry without modifying the backing store. */
  Optional<JobIndexEntrySnapshot> loadIndexEntry(String pointerKey);

  default LegacyLookupMigrationPage migrateLegacyLookupEntries(int limit, String pageToken) {
    return new LegacyLookupMigrationPage(0, 0, 0, 0, "");
  }

  default LegacyCleanupMigrationPage migrateLegacyCleanupManifests(int limit, String pageToken) {
    return new LegacyCleanupMigrationPage(0, 0, 0, 0, 0, "");
  }

  /** Acquires or renews exclusive migration ownership and returns the durable pass checkpoint. */
  default Optional<LegacyMigrationLease> acquireLegacyMigrationLease(
      LegacyMigration migration, String ownerId, long nowMs, long leaseDurationMs) {
    return Optional.empty();
  }

  /** Persists pass progress only while the caller still owns the matching lease fence. */
  default boolean checkpointLegacyMigration(
      LegacyMigration migration,
      String ownerId,
      long fence,
      LegacyMigrationProgress progress,
      long nowMs,
      long leaseDurationMs) {
    return false;
  }

  /** Writes the completion marker only for an owned, unexpired, durably quiet pass. */
  default boolean completeLegacyMigration(
      LegacyMigration migration, String ownerId, long fence, long nowMs) {
    return false;
  }

  /** Returns whether the durable completion marker exists for the selected migration. */
  default boolean legacyMigrationComplete(LegacyMigration migration) {
    return false;
  }

  default boolean legacyCleanupMigrationComplete() {
    return true;
  }

  default boolean ensureTerminalRetentionBackfill(
      CanonicalPointerSnapshot snapshot,
      String retentionPointerKey,
      ReconcileJobIndexCleanupManifest cleanupManifest) {
    if (snapshot == null || retentionPointerKey == null || retentionPointerKey.isBlank()) {
      return false;
    }
    JobIndexEntrySnapshot existing = loadIndexEntry(retentionPointerKey).orElse(null);
    if (existing != null && !snapshot.canonicalPointerKey().equals(existing.blobUri())) {
      return false;
    }
    List<ReconcileJobIndexStore.JobIndexWriteOp> writes = new java.util.ArrayList<>();
    writes.add(
        new ReconcileJobIndexStore.JobIndexUpsert(
            snapshot.canonicalPointerKey(),
            snapshot.version(),
            snapshot.blobUri(),
            ai.floedb.floecat.common.rpc.PointerReferenceKind.PRK_INLINE_JSON,
            cleanupManifest));
    if (existing == null) {
      writes.add(
          new ReconcileJobIndexStore.JobIndexUpsert(
              retentionPointerKey,
              0L,
              snapshot.canonicalPointerKey(),
              ai.floedb.floecat.common.rpc.PointerReferenceKind.PRK_POINTER_KEY));
    }
    return compareAndSetBatch(
        new ReconcileJobIndexStore.JobIndexWriteBatch(
            List.copyOf(writes), ReconcileJobIndexStore.ReadyQueueMutation.empty()));
  }

  default Optional<JobCleanupSession> beginJobCleanup(
      CanonicalPointerSnapshot expected, ReconcileJobIndexCleanupManifest fallbackManifest) {
    if (expected == null) {
      return Optional.empty();
    }
    ReconcileJobIndexCleanupManifest stored = loadCleanupManifest(expected.canonicalPointerKey());
    ReconcileJobIndexCleanupManifest manifest =
        new ReconcileJobIndexCleanupManifest(
            concat(stored.indexPointerKeys(), fallbackManifest, true),
            concat(stored.readyPointerKeys(), fallbackManifest, false),
            concat(
                stored.pointerKeys(),
                fallbackManifest == null ? List.of() : fallbackManifest.pointerKeys()));
    return manifest.isEmpty()
        ? Optional.empty()
        : Optional.of(new JobCleanupSession(expected, manifest, false));
  }

  boolean compareAndSetBatch(ReconcileJobIndexStore.JobIndexWriteBatch batch);

  default boolean compareAndSetBatch(
      ReconcileJobIndexStore.JobIndexWriteBatch batch, List<PointerStore.CasOp> extraPointerOps) {
    if (extraPointerOps != null && !extraPointerOps.isEmpty()) {
      throw new UnsupportedOperationException(
          "compareAndSetBatch with extra pointer operations is not implemented");
    }
    return compareAndSetBatch(batch);
  }

  JobIndexQueryPage listCanonicalEntries(String accountId, int limit, String pageToken);

  JobIndexQueryPage listDedupeEntries(String accountId, int limit, String pageToken);

  default JobIndexQueryPage listTerminalRetentionEntries(
      String accountId, int limit, String pageToken) {
    return new JobIndexQueryPage(List.of(), "");
  }

  default JobIndexQueryPage listTerminalRetentionEntries(
      String accountId, long cutoffMs, int limit, String pageToken) {
    return listTerminalRetentionEntries(accountId, limit, pageToken);
  }

  JobIndexQueryPage listParentEntries(
      String accountId, String parentJobId, int limit, String pageToken);

  JobIndexQueryPage listConnectorEntries(
      String accountId, String connectorId, int limit, String pageToken);

  JobIndexQueryPage listGlobalStateEntries(String state, int limit, String pageToken);

  JobIndexQueryPage listAccountStateEntries(
      String accountId, String state, int limit, String pageToken);

  JobIndexQueryPage listConnectorStateEntries(
      String accountId, String connectorId, String state, int limit, String pageToken);

  default ReconcileJobIndexCleanupManifest loadCleanupManifest(String canonicalPointerKey) {
    return ReconcileJobIndexCleanupManifest.EMPTY;
  }

  default ReconcileJobIndexCleanupManifest discoverLegacyCleanupManifest(
      String canonicalPointerKey) {
    return ReconcileJobIndexCleanupManifest.EMPTY;
  }

  private static List<String> concat(
      List<String> stored,
      ReconcileJobIndexCleanupManifest fallbackManifest,
      boolean indexPointers) {
    List<String> fallback =
        fallbackManifest == null
            ? List.of()
            : indexPointers
                ? fallbackManifest.indexPointerKeys()
                : fallbackManifest.readyPointerKeys();
    java.util.ArrayList<String> merged =
        new java.util.ArrayList<>((stored == null ? 0 : stored.size()) + fallback.size());
    if (stored != null) {
      merged.addAll(stored);
    }
    merged.addAll(fallback);
    return merged;
  }

  private static List<String> concat(List<String> left, List<String> right) {
    java.util.ArrayList<String> merged =
        new java.util.ArrayList<>(
            (left == null ? 0 : left.size()) + (right == null ? 0 : right.size()));
    if (left != null) {
      merged.addAll(left);
    }
    if (right != null) {
      merged.addAll(right);
    }
    return merged;
  }
}
