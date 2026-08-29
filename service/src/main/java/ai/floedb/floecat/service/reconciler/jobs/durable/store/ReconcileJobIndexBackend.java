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
  record JobIndexQueryPage(List<JobIndexEntrySnapshot> entries, String nextPageToken) {}

  record JobCleanupSession(
      CanonicalPointerSnapshot snapshot,
      ReconcileJobIndexCleanupManifest manifest,
      boolean cleanupLocked) {}

  /** Loads an index entry without modifying the backing store. */
  Optional<JobIndexEntrySnapshot> loadIndexEntry(String pointerKey);

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
