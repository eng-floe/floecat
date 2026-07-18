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

  record LegacyLookupMigrationPage(
      int scanned, int migrated, int conflicted, int retryable, String nextPageToken) {}

  record LegacyCleanupMigrationPage(
      int scanned, int manifestsUpdated, int conflicted, int retryable, String nextPageToken) {}

  Optional<JobIndexEntrySnapshot> loadIndexEntry(String pointerKey);

  default LegacyLookupMigrationPage migrateLegacyLookupEntries(int limit, String pageToken) {
    return new LegacyLookupMigrationPage(0, 0, 0, 0, "");
  }

  default boolean completeLegacyLookupMigration() {
    return true;
  }

  default LegacyCleanupMigrationPage migrateLegacyCleanupManifests(int limit, String pageToken) {
    return new LegacyCleanupMigrationPage(0, 0, 0, 0, "");
  }

  default boolean completeLegacyCleanupMigration() {
    return true;
  }

  default boolean legacyCleanupMigrationComplete() {
    return true;
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
}
