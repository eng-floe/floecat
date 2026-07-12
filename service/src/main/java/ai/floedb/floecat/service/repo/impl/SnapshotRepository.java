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

package ai.floedb.floecat.service.repo.impl;

import ai.floedb.floecat.catalog.rpc.CurrentSnapshotPointer;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotManifestEntry;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.catalog.impl.StatsVisibilityGate;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.model.SnapshotKey;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.telemetry.StoreOperationSummary;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;
import org.jboss.logging.Logger;

@ApplicationScoped
public class SnapshotRepository {
  private static final Logger LOG = Logger.getLogger(SnapshotRepository.class);

  public enum CurrentSnapshotPointerUpdateResult {
    UPDATED,
    UNCHANGED,
    TABLE_MISSING,
    CONFLICT
  }

  private final GenericResourceRepository<Snapshot, SnapshotKey> repo;
  private final TableRepository tableRepo;
  private final CurrentSnapshotPointerRepository currentPointerRepo;
  private final PointerStore pointerStore;
  private final TableRootRepository roots;
  private final StatsStore statsStore;
  private final Clock clock;

  @Inject
  public SnapshotRepository(
      PointerStore pointerStore,
      BlobStore blobStore,
      TableRepository tableRepo,
      CurrentSnapshotPointerRepository currentPointerRepo,
      TableRootRepository roots,
      StatsStore statsStore) {
    this(
        pointerStore,
        blobStore,
        tableRepo,
        currentPointerRepo,
        roots,
        statsStore,
        Clock.systemUTC());
  }

  // Convenience constructors below pass statsStore = null: with no stats store the read-time
  // finalize gate is disabled, so getCurrentSnapshot behaves like getCommittedCurrentSnapshot
  // (query-visible == committed). Only the @Inject constructor wires the real StatsStore and thus
  // actually gates; a test that needs gating must construct through it.
  public SnapshotRepository(
      PointerStore pointerStore,
      BlobStore blobStore,
      TableRepository tableRepo,
      CurrentSnapshotPointerRepository currentPointerRepo,
      TableRootRepository roots) {
    this(pointerStore, blobStore, tableRepo, currentPointerRepo, roots, null, Clock.systemUTC());
  }

  public SnapshotRepository(
      PointerStore pointerStore,
      BlobStore blobStore,
      TableRepository tableRepo,
      CurrentSnapshotPointerRepository currentPointerRepo) {
    this(
        pointerStore,
        blobStore,
        tableRepo,
        currentPointerRepo,
        new TableRootRepository(pointerStore, blobStore),
        null);
  }

  public SnapshotRepository(
      PointerStore pointerStore, BlobStore blobStore, TableRepository tableRepo) {
    this(
        pointerStore,
        blobStore,
        tableRepo,
        new CurrentSnapshotPointerRepository(pointerStore, blobStore));
  }

  /** The one constructor that assigns state; every other overload delegates here. */
  private SnapshotRepository(
      PointerStore pointerStore,
      BlobStore blobStore,
      TableRepository tableRepo,
      CurrentSnapshotPointerRepository currentPointerRepo,
      TableRootRepository roots,
      StatsStore statsStore,
      Clock clock) {
    this.repo =
        new GenericResourceRepository<>(
            pointerStore,
            blobStore,
            Schemas.SNAPSHOT,
            Snapshot::parseFrom,
            Snapshot::toByteArray,
            "application/x-protobuf");
    this.tableRepo = tableRepo;
    this.currentPointerRepo = currentPointerRepo;
    this.pointerStore = pointerStore;
    this.roots = roots;
    this.statsStore = statsStore;
    this.clock = clock;
  }

  public void create(Snapshot snapshot) {
    repo.create(snapshot);
  }

  public boolean update(Snapshot snapshot, long expectedPointerVersion) {
    // Deliberately does NOT advance the current-snapshot pointer here. The advance must go through
    // the service layer (SnapshotServiceImpl.updateSnapshot → CurrentSnapshotPointerService), which
    // re-upserts the root entry after advancing — otherwise an UpdateSnapshot that makes a
    // snapshot current would move the pointer but leave the pinned identity stale, and new CURRENT
    // query pins would resolve the old snapshot. Advancing here silently pre-empted that service
    // advance (it saw the pointer already moved → UNCHANGED → no publish).
    return repo.update(snapshot, expectedPointerVersion);
  }

  public boolean delete(ResourceId tableId, long snapshotId) {
    boolean deleted =
        repo.delete(new SnapshotKey(tableId.getAccountId(), tableId.getId(), snapshotId));
    if (deleted) {
      deleteCurrentPointerIfCurrent(tableId, snapshotId);
    }
    return deleted;
  }

  public boolean deleteWithPrecondition(
      ResourceId tableId, long snapshotId, long expectedPointerVersion) {
    boolean deleted =
        repo.deleteWithPrecondition(
            new SnapshotKey(tableId.getAccountId(), tableId.getId(), snapshotId),
            expectedPointerVersion);
    if (deleted) {
      deleteCurrentPointerIfCurrent(tableId, snapshotId);
    }
    return deleted;
  }

  public Optional<Snapshot> getById(ResourceId tableId, long snapshotId) {
    return repo.getByKey(new SnapshotKey(tableId.getAccountId(), tableId.getId(), snapshotId));
  }

  /**
   * Loads a snapshot directly from its immutable blob URI, bypassing the (table, snapshot id)
   * pointer. Used to read the exact snapshot blob a query pinned: an in-place UpdateSnapshot on the
   * same id republishes the pointer to a new blob, so resolving by id could drift the scan to that
   * newer blob between pin validation and the read — reading the pinned URI cannot.
   */
  public Optional<Snapshot> getByBlobUri(String blobUri) {
    return repo.getByBlobUri(blobUri);
  }

  /**
   * The version (etag) of the immutable snapshot blob at {@code blobUri}, or {@code null} if no
   * blob is there, via a single HEAD. Lets a pin validator confirm the exact pinned snapshot blob
   * is present and unchanged without going through the (table, snapshot id) pointer, which an
   * in-place {@code UpdateSnapshot} can repoint to a newer blob while the pinned blob is still
   * retained.
   */
  public String blobEtag(String blobUri) {
    return repo.blobEtag(blobUri);
  }

  public CurrentSnapshotPointerUpdateResult maybeAdvanceCurrentSnapshotPointer(
      ResourceId tableId, Snapshot candidate) {
    var table = tableRepo.getById(tableId);
    if (table.isEmpty()) {
      return CurrentSnapshotPointerUpdateResult.TABLE_MISSING;
    }

    for (int attempt = 0; attempt < 4; attempt++) {
      Optional<CurrentSnapshotPointer> currentPointer = currentPointerRepo.get(tableId);
      if (!shouldAdvanceCurrentSnapshot(tableId, currentPointer.orElse(null), candidate)) {
        return CurrentSnapshotPointerUpdateResult.UNCHANGED;
      }

      CurrentSnapshotPointer next = buildCurrentPointer(tableId, candidate);
      if (currentPointer.isEmpty()) {
        if (currentPointerRepo.createIfAbsent(next)) {
          return CurrentSnapshotPointerUpdateResult.UPDATED;
        }
      } else {
        Optional<CurrentSnapshotPointerObservation> observed =
            currentPointerForUpdate(tableId, currentPointer.get());
        if (observed.isEmpty()) {
          backoffCurrentPointerAdvance(attempt);
          continue;
        }
        if (!shouldAdvanceCurrentSnapshot(tableId, observed.get().pointer(), candidate)) {
          return CurrentSnapshotPointerUpdateResult.UNCHANGED;
        }
        if (currentPointerRepo.update(next, observed.get().pointerVersion())) {
          return CurrentSnapshotPointerUpdateResult.UPDATED;
        }
      }
      backoffCurrentPointerAdvance(attempt);
    }
    return CurrentSnapshotPointerUpdateResult.CONFLICT;
  }

  private Optional<CurrentSnapshotPointerObservation> currentPointerForUpdate(
      ResourceId tableId, CurrentSnapshotPointer expectedPointer) {
    long expectedVersion;
    try {
      expectedVersion = currentPointerRepo.metaFor(tableId).getPointerVersion();
    } catch (BaseResourceRepository.NotFoundException e) {
      return Optional.empty();
    }
    Optional<CurrentSnapshotPointer> afterMeta = currentPointerRepo.get(tableId);
    if (afterMeta.isEmpty() || !afterMeta.get().equals(expectedPointer)) {
      return Optional.empty();
    }
    return Optional.of(new CurrentSnapshotPointerObservation(afterMeta.get(), expectedVersion));
  }

  private record CurrentSnapshotPointerObservation(
      CurrentSnapshotPointer pointer, long pointerVersion) {}

  private static void backoffCurrentPointerAdvance(int attempt) {
    long baseNanos = (1L << Math.min(attempt, 5)) * 1_000_000L;
    long jitterNanos = ThreadLocalRandom.current().nextLong(250_000L, 1_000_001L);
    LockSupport.parkNanos(baseNanos + jitterNanos);
  }

  private boolean shouldAdvanceCurrentSnapshot(
      ResourceId tableId, CurrentSnapshotPointer currentPointer, Snapshot candidateSnapshot) {
    if (currentPointer == null) {
      return true;
    }

    long currentMs =
        currentPointer.hasUpstreamCreatedAt()
            ? Timestamps.toMillis(currentPointer.getUpstreamCreatedAt())
            : Long.MIN_VALUE;
    long candidateMs =
        candidateSnapshot.hasUpstreamCreatedAt()
            ? Timestamps.toMillis(candidateSnapshot.getUpstreamCreatedAt())
            : Long.MIN_VALUE;

    if (currentPointer.getSnapshotId() == candidateSnapshot.getSnapshotId()) {
      if (currentMs != candidateMs) {
        LOG.warnf(
            "same snapshotId has different upstreamCreatedAt; leaving current pointer unchanged:"
                + " tableId=%s snapshotId=%d currentMs=%d candidateMs=%d",
            tableId, candidateSnapshot.getSnapshotId(), currentMs, candidateMs);
      }
      return false;
    }

    if (candidateMs != currentMs) {
      return candidateMs > currentMs;
    }
    return candidateSnapshot.getSnapshotId() > currentPointer.getSnapshotId();
  }

  public static String metadataLocation(Snapshot snapshot) {
    if (snapshot == null || !snapshot.hasMetadataLocation()) {
      return null;
    }
    String value = snapshot.getMetadataLocation().trim();
    return value.isBlank() ? null : value;
  }

  /**
   * The QUERY-VISIBLE current snapshot. The root's current id is the committed logical selection;
   * when a StatsStore is wired (the @Inject constructor) this read applies the finalize gate — an
   * unfinalized committed current resolves to the newest finalized snapshot at or before it, so
   * scans and query-adjacent reads never see an unfinalized snapshot. Without a stats store the
   * gate is off and this returns the committed current (see the constructors' note).
   */
  public Optional<Snapshot> getCurrentSnapshot(ResourceId tableId) {
    return rootCurrentSnapshot(tableId, true);
  }

  /** The committed current snapshot selection, before query-readiness gating. */
  public Optional<Snapshot> getCommittedCurrentSnapshot(ResourceId tableId) {
    return rootCurrentSnapshot(tableId, false);
  }

  private Optional<Snapshot> rootCurrentSnapshot(ResourceId tableId, boolean requireQueryReady) {
    if (tableId == null) {
      return Optional.empty();
    }
    RootLookup lookup = lookupRoot(tableId);
    if (lookup.pointerExists()) {
      if (lookup.root() == null || !lookup.root().hasCurrentSnapshotId()) {
        // Gated (registered but nothing finalized), or the root blob is unreadable — either way,
        // NEVER fall back to the ungated legacy pointer once a root pointer exists.
        return Optional.empty();
      }
      // Load the snapshot the ROOT published (its manifest entry's immutable snapshot_ref), not
      // the live (table, snapshot_id) pointer: an in-place UpdateSnapshot rewrites that pointer
      // before its root re-commit, so getById could serve a blob the current root never
      // referenced (and would keep serving it if the root commit then failed). The root is the
      // publication boundary for reads.
      return snapshotFromRootEntry(
          tableId, lookup.root(), lookup.root().getCurrentSnapshotId(), requireQueryReady);
    }
    return latestRegisteredSnapshot(tableId);
  }

  /**
   * Loads a snapshot by the immutable {@code snapshot_ref} the root's manifest entry carries. A
   * missing entry or ref is a broken root invariant (removal clears currency in the same commit),
   * surfaced as empty rather than by walking to the mutable pointer.
   */
  private Optional<Snapshot> snapshotFromRootEntry(
      ResourceId tableId,
      ai.floedb.floecat.catalog.rpc.TableRoot root,
      long snapshotId,
      boolean requireQueryReady) {
    var head = root.hasSnapshotManifestRef() ? root.getSnapshotManifestRef() : null;
    Optional<SnapshotManifestEntry> committed =
        SnapshotManifests.findEntry(roots, head, snapshotId);
    if (committed.isEmpty()) {
      return Optional.empty();
    }
    SnapshotManifestEntry entry = committed.get();
    if (requireQueryReady
        && StatsVisibilityGate.gateOnFinalize(statsStore)
        && !entry.hasStatsGenerationRef()) {
      // Query-visible current: the committed current is not yet finalized, so serve the newest
      // finalized snapshot at or before it (snapshot isolation — the latest fully-queryable state)
      // rather than reporting no current. Metadata surfaces use getCommittedCurrent* and keep the
      // committed selection. Empty here means nothing is queryable yet (pre-first-finalize).
      Optional<SnapshotManifestEntry> queryable =
          SnapshotManifests.latestQueryableCurrent(roots, head, entry);
      if (queryable.isEmpty()) {
        return Optional.empty();
      }
      entry = queryable.get();
    }
    if (!entry.hasSnapshotRef() || entry.getSnapshotRef().getUri().isEmpty()) {
      return Optional.empty();
    }
    return getByBlobUri(entry.getSnapshotRef().getUri());
  }

  /**
   * The table's root, distinguishing "no root pointer" (a legacy, un-migrated table — the legacy
   * fallback is legitimate) from "root pointer present but blob unreadable" (a superseded root
   * swept between the two reads, or corruption). The latter retries once — the pointer has
   * necessarily moved on a sweep race — and then resolves to a present-but-unreadable lookup so
   * callers stay GATED instead of walking to the legacy pointer.
   */
  private RootLookup lookupRoot(ResourceId tableId) {
    for (int attempt = 0; attempt < 2; attempt++) {
      MutationMeta meta = roots.metaForSafe(tableId);
      if (meta == null || meta.getBlobUri().isEmpty()) {
        return new RootLookup(false, null);
      }
      var root = roots.getByBlobUri(meta.getBlobUri()).orElse(null);
      if (root != null) {
        return new RootLookup(true, root);
      }
    }
    // Both reads missed: the pointer names a blob that is not there (corruption, or a
    // pathological double sweep race). The caller stays GATED — but silently invisible tables
    // are undebuggable, so leave the operator a trace.
    LOG.warnf(
        "table %s has a root pointer whose blob is unreadable; readers stay gated until repaired",
        tableId.getId());
    return new RootLookup(true, null);
  }

  private record RootLookup(boolean pointerExists, ai.floedb.floecat.catalog.rpc.TableRoot root) {}

  /**
   * The latest REGISTERED snapshot (the raw ingest-time pointer), regardless of the visibility
   * gate. Write-side only: finalize targets, credential vending for writers of a not-yet-finalized
   * snapshot, and migration/staging inputs. Never report this to readers.
   */
  public Optional<Snapshot> latestRegisteredSnapshot(ResourceId tableId) {
    if (tableId == null) {
      return Optional.empty();
    }
    Optional<CurrentSnapshotPointer> pointer = currentPointerRepo.get(tableId);
    String fallbackReason = "current_snapshot";
    if (pointer.isPresent()) {
      long snapshotId = pointer.get().getSnapshotId();
      if (snapshotId < 0) {
        fallbackReason = "current_snapshot_invalid_pointer";
      } else {
        Optional<Snapshot> current = getById(tableId, snapshotId);
        StoreOperationSummary.put(
            "current_snapshot_source", current.isPresent() ? "pointer" : "fallback");
        if (current.isPresent()) {
          return current;
        }
        fallbackReason = "current_snapshot_missing_pointer_target";
      }
    }

    if (tableRepo.getById(tableId).isEmpty()) {
      return Optional.empty();
    }
    StoreOperationSummary.put("current_snapshot_source", "fallback");
    StoreOperationSummary.fallback(fallbackReason);
    return latestSnapshotByTime(tableId);
  }

  /** Pointer-shaped view of {@link #getCurrentSnapshot} — the query-visible current snapshot. */
  public Optional<CurrentSnapshotPointer> getCurrentSnapshotPointer(ResourceId tableId) {
    return rootCurrentSnapshotPointer(tableId, true);
  }

  /** Pointer-shaped view of the committed current snapshot, before query-readiness gating. */
  public Optional<CurrentSnapshotPointer> getCommittedCurrentSnapshotPointer(ResourceId tableId) {
    return rootCurrentSnapshotPointer(tableId, false);
  }

  private Optional<CurrentSnapshotPointer> rootCurrentSnapshotPointer(
      ResourceId tableId, boolean requireQueryReady) {
    if (tableId == null) {
      return Optional.empty();
    }
    RootLookup lookup = lookupRoot(tableId);
    if (!lookup.pointerExists()) {
      return currentPointerRepo.get(tableId);
    }
    if (lookup.root() == null || !lookup.root().hasCurrentSnapshotId()) {
      return Optional.empty(); // gated or unreadable root: never the ungated legacy pointer
    }
    // Resolve through the root's published snapshot_ref, not the live pointer (see
    // getCurrentSnapshot).
    return snapshotFromRootEntry(
            tableId, lookup.root(), lookup.root().getCurrentSnapshotId(), requireQueryReady)
        .map(snap -> visibleCurrentPointer(tableId, snap));
  }

  /**
   * Read-side pointer view over a root-resolved current snapshot. Unlike the durable pointer the
   * write path builds, its timestamp is a stable property of the snapshot (ingest time) — a caller
   * polling the pointer for change must not observe a perpetual update.
   */
  private static CurrentSnapshotPointer visibleCurrentPointer(ResourceId tableId, Snapshot snap) {
    CurrentSnapshotPointer.Builder builder =
        CurrentSnapshotPointer.newBuilder().setTableId(tableId).setSnapshotId(snap.getSnapshotId());
    if (snap.hasIngestedAt()) {
      builder.setUpdatedAt(snap.getIngestedAt());
    } else if (snap.hasUpstreamCreatedAt()) {
      builder.setUpdatedAt(snap.getUpstreamCreatedAt());
    }
    if (snap.hasUpstreamCreatedAt()) {
      builder.setUpstreamCreatedAt(snap.getUpstreamCreatedAt());
    }
    return builder.build();
  }

  /** Raw ingest-time pointer; see {@link #latestRegisteredSnapshot} for when this is legitimate. */
  public Optional<CurrentSnapshotPointer> latestRegisteredSnapshotPointer(ResourceId tableId) {
    if (tableId == null) {
      return Optional.empty();
    }
    return currentPointerRepo.get(tableId);
  }

  private CurrentSnapshotPointer buildCurrentPointer(ResourceId tableId, Snapshot snapshot) {
    CurrentSnapshotPointer.Builder builder =
        CurrentSnapshotPointer.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshot.getSnapshotId())
            .setUpdatedAt(Timestamps.fromMillis(clock.millis()));
    if (snapshot.hasUpstreamCreatedAt()) {
      builder.setUpstreamCreatedAt(snapshot.getUpstreamCreatedAt());
    }
    return builder.build();
  }

  private void deleteCurrentPointerIfCurrent(ResourceId tableId, long snapshotId) {
    Optional<CurrentSnapshotPointer> before = currentPointerRepo.get(tableId);
    if (before.isEmpty() || before.get().getSnapshotId() != snapshotId) {
      return;
    }
    long expectedVersion;
    try {
      expectedVersion = currentPointerRepo.metaFor(tableId).getPointerVersion();
    } catch (BaseResourceRepository.NotFoundException e) {
      return;
    }
    Optional<CurrentSnapshotPointer> afterMeta = currentPointerRepo.get(tableId);
    if (afterMeta.isEmpty()
        || afterMeta.get().getSnapshotId() != snapshotId
        || !afterMeta.get().equals(before.get())) {
      return;
    }
    currentPointerRepo.deleteWithPrecondition(tableId, expectedVersion);
  }

  public Optional<Snapshot> getAsOf(ResourceId tableId, Timestamp asOf) {
    long asOfMs = Timestamps.toMillis(asOf);
    if (asOfMs < 0) {
      // Before the epoch: no snapshot can have committed at or before this, and the by-time key
      // encoding requires a non-negative timestamp. Return empty rather than throwing.
      return Optional.empty();
    }
    // Indexed predecessor lookup. The by-time index is ordered newest-first (key = inverted
    // commit time). The boundary is the smallest possible key in the asOf group (inverted time =
    // MAX-asOfMs, largest inverted snapshot id); resuming strictly after it seeks past every newer
    // snapshot to the first one committed at or before asOf — a single indexed read, with no scan
    // of newer snapshots and no blob materialization.
    String boundary =
        Keys.snapshotPointerByTime(tableId.getAccountId(), tableId.getId(), Long.MAX_VALUE, asOfMs);
    return firstByTimeAtOrAfter(tableId, pointerStore.pageTokenAfterKey(boundary));
  }

  public List<Snapshot> list(
      ResourceId tableId, int limit, String pageToken, StringBuilder nextOut) {
    String prefix = Keys.snapshotPointerByIdPrefix(tableId.getAccountId(), tableId.getId());
    return repo.listByPrefix(prefix, limit, pageToken, nextOut);
  }

  public List<Snapshot> listByTime(
      ResourceId tableId, int limit, String pageToken, StringBuilder nextOut) {
    String prefix = Keys.snapshotPointerByTimePrefix(tableId.getAccountId(), tableId.getId());
    return repo.listByPrefix(prefix, limit, pageToken, nextOut);
  }

  public int count(ResourceId tableId) {
    String prefix = Keys.snapshotPointerByIdPrefix(tableId.getAccountId(), tableId.getId());
    return repo.countByPrefix(prefix);
  }

  public MutationMeta metaFor(ResourceId tableId, long snapshotId) {
    return repo.metaFor(new SnapshotKey(tableId.getAccountId(), tableId.getId(), snapshotId));
  }

  public MutationMeta metaFor(ResourceId tableId, long snapshotId, Timestamp nowTs) {
    return repo.metaFor(
        new SnapshotKey(tableId.getAccountId(), tableId.getId(), snapshotId), nowTs);
  }

  public MutationMeta metaForSafe(ResourceId tableId, long snapshotId) {
    return repo.metaForSafe(new SnapshotKey(tableId.getAccountId(), tableId.getId(), snapshotId));
  }

  private Optional<Snapshot> latestSnapshotByTime(ResourceId tableId) {
    // Newest-first index: the first entry is the latest snapshot, and ties on commit time are
    // ordered by descending snapshot id, so the first entry is also the highest-id of that time.
    return firstByTimeAtOrAfter(tableId, "");
  }

  /**
   * The first by-time index entry at or after {@code seekToken} that HYDRATES. The by-id and
   * by-time pointers are independent secondaries created and deleted non-atomically, so a delete or
   * partial registration can leave the newest by-time entry dangling; committing to a single seek
   * would then hide older, fully-intact snapshots behind a spurious not-found. The common case
   * stays one pointer read + one blob read; dangling entries are skipped oldward.
   */
  private Optional<Snapshot> firstByTimeAtOrAfter(ResourceId tableId, String seekToken) {
    String prefix = Keys.snapshotPointerByTimePrefix(tableId.getAccountId(), tableId.getId());
    String token = seekToken;
    while (true) {
      StringBuilder next = new StringBuilder();
      List<Pointer> rows = pointerStore.listPointersByPrefix(prefix, 8, token, next);
      for (Pointer row : rows) {
        Optional<Snapshot> snapshot = getById(tableId, Keys.snapshotIdFromByTimeKey(row.getKey()));
        if (snapshot.isPresent()) {
          return snapshot;
        }
        LOG.debugf(
            "by-time entry %s of table %s is dangling; advancing to the next older snapshot",
            row.getKey(), tableId.getId());
      }
      token = next.toString();
      if (rows.isEmpty() || token.isBlank()) {
        return Optional.empty();
      }
    }
  }
}
