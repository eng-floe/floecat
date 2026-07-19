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

import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.PointerStore;
import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Singleton
@IfBuildProperty(name = "floecat.kv", stringValue = "memory")
public class MemoryReconcileJobIndexBackend implements ReconcileJobIndexBackend {
  private PointerStore pointerStore;
  private final Map<String, ReconcileJobIndexCleanupManifest> cleanupManifests =
      new ConcurrentHashMap<>();
  private final Map<String, Long> cleanupLocks = new ConcurrentHashMap<>();
  private final Map<LegacyMigration, LegacyMigrationState> legacyMigrationStates =
      new EnumMap<>(LegacyMigration.class);
  private final Set<LegacyMigration> completedLegacyMigrations =
      EnumSet.noneOf(LegacyMigration.class);
  private long legacyMigrationFence;

  @Inject
  public MemoryReconcileJobIndexBackend(PointerStore pointerStore) {
    this.pointerStore = pointerStore;
  }

  public MemoryReconcileJobIndexBackend() {}

  public void bind(PointerStore pointerStore) {
    this.pointerStore = pointerStore;
  }

  @Override
  public Optional<JobIndexEntrySnapshot> loadIndexEntry(String pointerKey) {
    return pointerStore
        .get(pointerKey)
        .map(
            pointer ->
                new JobIndexEntrySnapshot(
                    pointer.getKey(),
                    pointer.getBlobUri(),
                    pointer.getVersion(),
                    cleanupLocks.containsKey(pointer.getKey())));
  }

  @Override
  public synchronized Optional<LegacyMigrationLease> acquireLegacyMigrationLease(
      LegacyMigration migration, String ownerId, long nowMs, long leaseDurationMs) {
    if (migration == null
        || ownerId == null
        || ownerId.isBlank()
        || completedLegacyMigrations.contains(migration)) {
      return Optional.empty();
    }
    LegacyMigrationState state =
        legacyMigrationStates.computeIfAbsent(migration, ignored -> new LegacyMigrationState());
    if (state.ownerId != null && !state.ownerId.equals(ownerId) && state.leaseExpiresAtMs > nowMs) {
      return Optional.empty();
    }
    legacyMigrationFence =
        legacyMigrationFence == Long.MAX_VALUE ? Long.MAX_VALUE : legacyMigrationFence + 1L;
    state.ownerId = ownerId;
    state.fence = legacyMigrationFence;
    state.leaseExpiresAtMs = leaseExpiresAt(nowMs, Math.max(1L, leaseDurationMs));
    return Optional.of(new LegacyMigrationLease(state.fence, state.progress));
  }

  @Override
  public synchronized boolean checkpointLegacyMigration(
      LegacyMigration migration,
      String ownerId,
      long fence,
      LegacyMigrationProgress progress,
      long nowMs,
      long leaseDurationMs) {
    LegacyMigrationState state = legacyMigrationStates.get(migration);
    if (state == null
        || ownerId == null
        || !ownerId.equals(state.ownerId)
        || fence != state.fence
        || state.leaseExpiresAtMs < nowMs
        || progress == null) {
      return false;
    }
    state.progress = normalize(progress);
    state.leaseExpiresAtMs = leaseExpiresAt(nowMs, Math.max(1L, leaseDurationMs));
    return true;
  }

  @Override
  public synchronized boolean completeLegacyMigration(
      LegacyMigration migration, String ownerId, long fence, long nowMs) {
    if (completedLegacyMigrations.contains(migration)) {
      return true;
    }
    LegacyMigrationState state = legacyMigrationStates.get(migration);
    if (state == null
        || ownerId == null
        || !ownerId.equals(state.ownerId)
        || fence != state.fence
        || state.leaseExpiresAtMs < nowMs
        || !state.progress.quietPassComplete()
        || state.progress.changed() != 0
        || state.progress.retryable() != 0) {
      return false;
    }
    completedLegacyMigrations.add(migration);
    legacyMigrationStates.remove(migration);
    return true;
  }

  @Override
  public synchronized boolean legacyMigrationComplete(LegacyMigration migration) {
    return migration != null && completedLegacyMigrations.contains(migration);
  }

  @Override
  public synchronized boolean completeLegacyLookupMigration() {
    completedLegacyMigrations.add(LegacyMigration.LOOKUP);
    legacyMigrationStates.remove(LegacyMigration.LOOKUP);
    return true;
  }

  @Override
  public synchronized boolean completeLegacyCleanupMigration() {
    completedLegacyMigrations.add(LegacyMigration.CLEANUP);
    legacyMigrationStates.remove(LegacyMigration.CLEANUP);
    return true;
  }

  @Override
  public boolean compareAndSetBatch(ReconcileJobIndexStore.JobIndexWriteBatch batch) {
    return compareAndSetBatch(batch, List.of());
  }

  @Override
  public synchronized boolean compareAndSetBatch(
      ReconcileJobIndexStore.JobIndexWriteBatch batch, List<PointerStore.CasOp> extraPointerOps) {
    if (batch != null) {
      for (ReconcileJobIndexStore.JobIndexWriteOp write : batch.writes()) {
        if (write instanceof ReconcileJobIndexStore.JobIndexUpsert upsert
            && JobIndexBackendSupport.parseCanonicalJobKey(upsert.pointerKey()) != null
            && cleanupLocks.containsKey(upsert.pointerKey())) {
          return false;
        }
        if (write instanceof ReconcileJobIndexStore.JobIndexCheck check) {
          if (JobIndexBackendSupport.parseCanonicalJobKey(check.pointerKey()) == null) {
            throw new IllegalArgumentException(
                "Unsupported reconcile job index version check key: " + check.pointerKey());
          }
          JobIndexEntrySnapshot existing = loadIndexEntry(check.pointerKey()).orElse(null);
          if (existing == null
              || existing.version() != check.expectedVersion()
              || (check.requireCleanupLock() && !cleanupLocks.containsKey(check.pointerKey()))) {
            return false;
          }
        }
        if (write instanceof ReconcileJobIndexStore.JobIndexDelete delete
            && !delete.expectedCanonicalPointerKey().isBlank()) {
          JobIndexEntrySnapshot existing = loadIndexEntry(delete.pointerKey()).orElse(null);
          if (existing == null && delete.allowAbsent()) {
            continue;
          }
          if (existing == null
              || !delete.expectedCanonicalPointerKey().equals(existing.blobUri())) {
            return false;
          }
        }
        if (write instanceof ReconcileJobIndexStore.JobIndexDelete delete
            && JobIndexBackendSupport.parseCanonicalJobKey(delete.pointerKey()) != null
            && delete.requireCleanupLock()
            && !cleanupLocks.containsKey(delete.pointerKey())) {
          return false;
        }
      }
    }
    List<PointerStore.CasOp> ops =
        new ArrayList<>(
            JobIndexWriteBatchSupport.toCasOps(
                batch,
                key ->
                    pointerStore
                        .get(key)
                        .map(
                            pointer ->
                                new JobIndexEntrySnapshot(
                                    pointer.getKey(),
                                    pointer.getBlobUri(),
                                    pointer.getVersion()))));
    if (extraPointerOps != null) {
      ops.addAll(extraPointerOps);
    }
    boolean committed = pointerStore.compareAndSetBatch(ops);
    if (!committed) {
      return false;
    }
    if (batch != null) {
      for (ReconcileJobIndexStore.JobIndexWriteOp write : batch.writes()) {
        if (write instanceof ReconcileJobIndexStore.JobIndexUpsert upsert
            && JobIndexBackendSupport.parseCanonicalJobKey(upsert.pointerKey()) != null) {
          cleanupManifests.put(upsert.pointerKey(), upsert.cleanupManifest());
        } else if (write instanceof ReconcileJobIndexStore.JobIndexDelete delete
            && JobIndexBackendSupport.parseCanonicalJobKey(delete.pointerKey()) != null) {
          cleanupManifests.remove(delete.pointerKey());
          cleanupLocks.remove(delete.pointerKey());
        }
      }
    }
    return true;
  }

  @Override
  public synchronized ReconcileJobIndexCleanupManifest loadCleanupManifest(
      String canonicalPointerKey) {
    return cleanupManifests.getOrDefault(
        canonicalPointerKey, ReconcileJobIndexCleanupManifest.EMPTY);
  }

  @Override
  public synchronized Optional<JobCleanupSession> beginJobCleanup(
      CanonicalPointerSnapshot expected, ReconcileJobIndexCleanupManifest fallbackManifest) {
    if (expected == null
        || JobIndexBackendSupport.parseCanonicalJobKey(expected.canonicalPointerKey()) == null) {
      return Optional.empty();
    }
    JobIndexEntrySnapshot current = loadIndexEntry(expected.canonicalPointerKey()).orElse(null);
    if (current == null
        || current.version() != expected.version()
        || !java.util.Objects.equals(current.blobUri(), expected.blobUri())) {
      Long lockedVersion = cleanupLocks.get(expected.canonicalPointerKey());
      if (current == null || lockedVersion == null || current.version() != lockedVersion) {
        return Optional.empty();
      }
    }
    ReconcileJobIndexCleanupManifest stored =
        cleanupManifests.getOrDefault(
            expected.canonicalPointerKey(), ReconcileJobIndexCleanupManifest.EMPTY);
    ReconcileJobIndexCleanupManifest manifest = mergeManifests(stored, fallbackManifest);
    if (manifest.isEmpty() || !validCleanupManifest(manifest)) {
      ReconcileJobIndexCleanupManifest discovered =
          discoverLegacyCleanupManifest(expected.canonicalPointerKey());
      manifest =
          validCleanupManifest(fallbackManifest)
              ? mergeManifests(discovered, fallbackManifest)
              : discovered;
      if (manifest.isEmpty() || !validCleanupManifest(manifest)) {
        return Optional.empty();
      }
    }
    cleanupManifests.put(expected.canonicalPointerKey(), manifest);
    cleanupLocks.put(expected.canonicalPointerKey(), current.version());
    return Optional.of(
        new JobCleanupSession(
            new CanonicalPointerSnapshot(
                current.pointerKey(), current.blobUri(), current.version()),
            manifest,
            true));
  }

  @Override
  public ReconcileJobIndexCleanupManifest discoverLegacyCleanupManifest(
      String canonicalPointerKey) {
    if (canonicalPointerKey == null || canonicalPointerKey.isBlank()) {
      return ReconcileJobIndexCleanupManifest.EMPTY;
    }
    java.util.LinkedHashSet<String> indexKeys = new java.util.LinkedHashSet<>();
    java.util.LinkedHashSet<String> readyKeys = new java.util.LinkedHashSet<>();
    var canonicalKey = JobIndexBackendSupport.parseCanonicalJobKey(canonicalPointerKey);
    if (canonicalKey != null) {
      indexKeys.add(Keys.reconcileJobLookupPointerByIdPrefix() + canonicalKey.jobSegment());
    }
    String token = "";
    do {
      StringBuilder next = new StringBuilder();
      for (var pointer : pointerStore.listPointersByPrefix("/", 500, token, next)) {
        if (!canonicalPointerKey.equals(pointer.getBlobUri())) {
          continue;
        }
        if (pointer.getKey().startsWith(Keys.reconcileReadyPointerPrefix())) {
          readyKeys.add(pointer.getKey());
        } else if (!canonicalPointerKey.equals(pointer.getKey())
            && JobIndexBackendSupport.validCleanupIndexPointerKey(pointer.getKey())) {
          indexKeys.add(pointer.getKey());
        }
      }
      token = next.toString();
    } while (!token.isBlank());
    return new ReconcileJobIndexCleanupManifest(List.copyOf(indexKeys), List.copyOf(readyKeys));
  }

  @Override
  public JobIndexQueryPage listCanonicalEntries(String accountId, int limit, String pageToken) {
    return listPointers(Keys.reconcileJobPointerByIdPrefix(accountId), limit, pageToken);
  }

  @Override
  public JobIndexQueryPage listDedupeEntries(String accountId, int limit, String pageToken) {
    return listPointers(Keys.reconcileDedupePointerPrefix(accountId), limit, pageToken);
  }

  @Override
  public JobIndexQueryPage listParentEntries(
      String accountId, String parentJobId, int limit, String pageToken) {
    return listPointers(
        Keys.reconcileJobByParentPointerPrefix(accountId, parentJobId), limit, pageToken);
  }

  @Override
  public JobIndexQueryPage listConnectorEntries(
      String accountId, String connectorId, int limit, String pageToken) {
    return listPointers(
        Keys.reconcileJobByConnectorPointerPrefix(accountId, connectorId), limit, pageToken);
  }

  @Override
  public JobIndexQueryPage listGlobalStateEntries(String state, int limit, String pageToken) {
    return listPointers(Keys.reconcileJobByStatePointerPrefix(state), limit, pageToken);
  }

  @Override
  public JobIndexQueryPage listAccountStateEntries(
      String accountId, String state, int limit, String pageToken) {
    return listPointers(
        Keys.reconcileJobByAccountStatePointerPrefix(accountId, state), limit, pageToken);
  }

  @Override
  public JobIndexQueryPage listConnectorStateEntries(
      String accountId, String connectorId, String state, int limit, String pageToken) {
    return listPointers(
        Keys.reconcileJobByConnectorStatePointerPrefix(accountId, connectorId, state),
        limit,
        pageToken);
  }

  private JobIndexQueryPage listPointers(String prefix, int limit, String pageToken) {
    StringBuilder nextPageToken = new StringBuilder();
    List<JobIndexEntrySnapshot> entries =
        pointerStore.listPointersByPrefix(prefix, limit, pageToken, nextPageToken).stream()
            .map(
                pointer ->
                    new JobIndexEntrySnapshot(
                        pointer.getKey(),
                        pointer.getBlobUri(),
                        pointer.getVersion(),
                        cleanupLocks.containsKey(pointer.getKey())))
            .toList();
    return new JobIndexQueryPage(entries, nextPageToken.toString());
  }

  private static final class LegacyMigrationState {
    private String ownerId;
    private long fence;
    private long leaseExpiresAtMs;
    private LegacyMigrationProgress progress = LegacyMigrationProgress.empty();
  }

  private static LegacyMigrationProgress normalize(LegacyMigrationProgress progress) {
    return new LegacyMigrationProgress(
        progress.pageToken(),
        Math.max(0, progress.changed()),
        Math.max(0, progress.unresolvable()),
        Math.max(0, progress.conflicted()),
        Math.max(0, progress.retryable()),
        progress.quietPassComplete());
  }

  private static long leaseExpiresAt(long nowMs, long leaseDurationMs) {
    if (leaseDurationMs > 0L && nowMs > Long.MAX_VALUE - leaseDurationMs) {
      return Long.MAX_VALUE;
    }
    return nowMs + leaseDurationMs;
  }

  private static ReconcileJobIndexCleanupManifest mergeManifests(
      ReconcileJobIndexCleanupManifest left, ReconcileJobIndexCleanupManifest right) {
    java.util.ArrayList<String> indexKeys = new java.util.ArrayList<>();
    java.util.ArrayList<String> readyKeys = new java.util.ArrayList<>();
    java.util.ArrayList<String> pointerKeys = new java.util.ArrayList<>();
    if (left != null) {
      indexKeys.addAll(left.indexPointerKeys());
      readyKeys.addAll(left.readyPointerKeys());
      pointerKeys.addAll(left.pointerKeys());
    }
    if (right != null) {
      indexKeys.addAll(right.indexPointerKeys());
      readyKeys.addAll(right.readyPointerKeys());
      pointerKeys.addAll(right.pointerKeys());
    }
    return new ReconcileJobIndexCleanupManifest(indexKeys, readyKeys, pointerKeys);
  }

  private static boolean validCleanupManifest(ReconcileJobIndexCleanupManifest manifest) {
    if (manifest == null) {
      return false;
    }
    for (String pointerKey : manifest.indexPointerKeys()) {
      if (!JobIndexBackendSupport.validCleanupIndexPointerKey(pointerKey)) {
        return false;
      }
    }
    return manifest.readyPointerKeys().stream()
            .allMatch(pointerKey -> ReadyQueueBackendSupport.toReadyQueueRow(pointerKey) != null)
        && manifest.pointerKeys().stream().allMatch(JobIndexBackendSupport::validCleanupPointerKey);
  }
}
