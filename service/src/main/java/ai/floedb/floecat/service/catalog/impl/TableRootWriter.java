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

package ai.floedb.floecat.service.catalog.impl;

import ai.floedb.floecat.catalog.rpc.BlobRef;
import ai.floedb.floecat.catalog.rpc.CurrentSnapshotPointer;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotManifestEntry;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.impl.ConstraintRepository;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.TableRootRepository;
import ai.floedb.floecat.service.repo.model.BlobRefs;
import ai.floedb.floecat.stats.spi.StatsStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * The one place writers record their mutations on the table root. Each method reads the mutation's
 * outcome from its legacy family (the snapshot blob identity, the active stats generation, the
 * constraints bundle, the definition blob) and commits it onto the root, so every family converges
 * on the same immutable object.
 *
 * <p>Failure semantics: reads resolve through the root, so the root commit IS the mutation's
 * publication — a failed commit propagates and fails the calling operation before it is
 * acknowledged (the caller retries; mutators re-derive from committed state, so retries converge).
 * The one exception is {@link #resyncFromCommittedState}: it runs after a transactional apply that
 * is already durable, so its failure is absorbed with a warning and the next commit or reconcile
 * pass self-heals the root.
 */
@ApplicationScoped
public class TableRootWriter {
  private static final Logger LOG = Logger.getLogger(TableRootWriter.class);

  private final TableRootRepository roots;
  private final TableRootCommitter committer;
  private final TableRepository tables;
  private final SnapshotRepository snapshots;
  private final ConstraintRepository constraints;
  private final StatsStore statsStore;

  @Inject
  public TableRootWriter(
      TableRootRepository roots,
      TableRootCommitter committer,
      TableRepository tables,
      SnapshotRepository snapshots,
      ConstraintRepository constraints,
      StatsStore statsStore) {
    this.roots = roots;
    this.committer = committer;
    this.tables = tables;
    this.snapshots = snapshots;
    this.constraints = constraints;
    this.statsStore = statsStore;
  }

  /**
   * Records a written snapshot's immutable blob identity on the root manifest; the mutator applies
   * the same advance rule the current-snapshot pointer machinery used. Every snapshot write funnels
   * through the pointer advance, which calls this — the single point keeping the manifest in step.
   */
  public void commitSnapshotEntry(ResourceId tableId, Snapshot candidate) {
    MutationMeta snapMeta = snapshots.metaForSafe(tableId, candidate.getSnapshotId());
    BlobRef snapshotRef = BlobRefs.refFrom(snapMeta);
    if (snapshotRef == null) {
      return; // snapshot blob not resolvable; nothing coherent to record
    }
    SnapshotManifestEntry.Builder entry =
        SnapshotManifestEntry.newBuilder()
            .setSnapshotId(candidate.getSnapshotId())
            .setSnapshotRef(snapshotRef);
    if (candidate.hasUpstreamCreatedAt()) {
      entry.setUpstreamCreatedAt(candidate.getUpstreamCreatedAt());
    }
    // Root currency tracks the committed current-snapshot selection immediately. Query readers
    // still require the selected manifest entry to carry a stats generation before pinning it, so
    // logical Iceberg metadata can move current without exposing an unfinalized scan.
    boolean advanceAtRegistration = true;
    committer.commit(
        tableId,
        TableRootMutations.upsertSnapshot(
            roots,
            tableId,
            entry.build(),
            BlobRefs.refFrom(tables.metaForSafe(tableId)),
            advanceAtRegistration));
  }

  /** Removes a deleted snapshot's entry from the root manifest. */
  public void removeSnapshot(ResourceId tableId, long snapshotId) {
    committer.commit(tableId, TableRootMutations.removeSnapshot(roots, tableId, snapshotId));
  }

  /** Records the table's (possibly new) immutable definition blob on the root. */
  public void commitDefinition(ResourceId tableId, MutationMeta meta) {
    BlobRef definitionRef = BlobRefs.refFrom(meta);
    if (definitionRef == null) {
      return;
    }
    committer.commit(tableId, TableRootMutations.setDefinition(tableId, definitionRef));
  }

  /**
   * Records the snapshot's active stats generation on its manifest entry — called when a generation
   * is activated (a replace-all publish, a first upsert creating the generation) or removed. The
   * ref is read back from the store rather than threaded through, so concurrent activations
   * converge on the last published generation. Skipped when the store cannot name generations
   * (empty would mean "cannot say", not "none").
   *
   * <p>This commit is the generation's PUBLICATION point: queries serve stats from the generation
   * their pinned root references, so a new generation becomes visible when it lands here — stats
   * are deterministic at a given pointer for a query's lifetime. Every stats write is
   * floecat-mediated (floescan submits through the leased reconcile protocol, other engines via
   * PutTargetStats), so the root stays in sync with the stats family.
   */
  public void commitStatsGeneration(ResourceId tableId, long snapshotId) {
    if (!StatsVisibilityGate.gateOnFinalize(statsStore)) {
      return;
    }
    committer.commit(
        tableId,
        current -> {
          // Read the active generation INSIDE the mutator: concurrent activations race, and a
          // ref captured before a lost CAS could land last, leaving the root referencing a
          // superseded generation forever. Re-reading per attempt makes the last commit reflect
          // the last published generation.
          BlobRef generationRef =
              statsStore
                  .activeStatsGeneration(tableId, snapshotId)
                  .map(uri -> BlobRef.newBuilder().setUri(uri).build())
                  .orElse(null);
          // Read /snapshots/current INSIDE the mutator (like the resync): the finalize advances
          // currency only when this snapshot IS the committed current, so a lost CAS must re-read
          // the authoritative selection per attempt rather than a value captured before the winner.
          Long committedCurrentSnapshotId =
              snapshots
                  .latestRegisteredSnapshotPointer(tableId)
                  .map(CurrentSnapshotPointer::getSnapshotId)
                  .orElse(null);
          return TableRootMutations.setStatsGeneration(
                  roots, tableId, snapshotId, generationRef, committedCurrentSnapshotId)
              .apply(current);
        });
  }

  /**
   * Records the snapshot's constraints bundle on its manifest entry — called after any constraints
   * write. The ref is read back from the constraints family, so a delete clears it.
   */
  public void commitConstraints(ResourceId tableId, long snapshotId) {
    committer.commit(
        tableId,
        TableRootMutations.setConstraints(
            roots,
            tableId,
            snapshotId,
            BlobRefs.refFrom(constraints.metaForSafe(tableId, snapshotId))));
  }

  /**
   * Converge the table's root with its committed pointer families after a write path that mutates
   * pointers directly — the transactional commit (Iceberg REST) applies staged intents by raw
   * pointer CAS and never passes through the service funnels above. Re-reads the definition meta
   * and the current-snapshot pointer and commits them onto the root, FORCING currency to the
   * committed selection (a transaction may legitimately move currency to any snapshot). A table
   * whose definition pointer is gone (transactional drop) has its root pointer deleted; a wrongly
   * deleted root re-materializes lazily from the legacy families on the next touch. Failures are
   * absorbed: the transaction is already durably applied, so the root converges on the next commit
   * or reconcile pass instead of failing an applied transaction.
   *
   * @return whether the resync committed (or was a no-op); {@code false} means the failure was
   *     absorbed and the caller should leave a durable re-drive marker — a table only touched by
   *     transactions has no other writer to converge its root.
   */
  public boolean resyncFromCommittedState(ResourceId tableId) {
    return absorb(
        tableId,
        "root resync",
        () -> {
          if (BlobRefs.refFrom(tables.metaForSafe(tableId)) == null) {
            // Transactional drop: the root goes with the definition. Only an actual deletion (or
            // nothing to delete) counts as converged — giving up under contention must keep the
            // re-drive marker alive.
            return deleteRoot(tableId);
          }
          boolean[] converged = {true};
          committer.commit(
              tableId,
              current -> {
                // The committed families are re-read INSIDE the mutator: resync FORCES currency,
                // so a lost CAS must not re-apply state captured before the winner's commit — it
                // would resurrect stale currency or re-insert a concurrently deleted snapshot's
                // entry. Each retry re-derives from committed state at retry time, keeping this a
                // pure function of "now" like every other mutator.
                converged[0] = true;
                BlobRef definitionRef = BlobRefs.refFrom(tables.metaForSafe(tableId));
                if (definitionRef == null) {
                  converged[0] = false; // dropped mid-resync: the re-probe below cleans up
                  return null;
                }
                SnapshotManifestEntry entry = null;
                var pointer = snapshots.latestRegisteredSnapshotPointer(tableId);
                if (pointer.isPresent()) {
                  entry = snapshotEntry(tableId, pointer.get().getSnapshotId()).orElse(null);
                  if (entry == null) {
                    // Committed current snapshot has no resolvable blob yet: nothing coherent to
                    // force. NOT converged — the marker stays and the re-drive retries later.
                    converged[0] = false;
                    return null;
                  }
                }
                // The registered-snapshot set, re-read per attempt: transactional
                // expire/remove-snapshots clears snapshot pointers by raw CAS, and a multi-snapshot
                // add registers non-current snapshots — neither funnels through removeSnapshot /
                // commitSnapshotEntry, so the resync is the only place the root's membership
                // converges. A store fault listing them throws and aborts the attempt (the marker
                // stays) rather than reconciling against a partial set.
                java.util.Set<Long> liveSnapshotIds = registeredSnapshotIds(tableId);
                return TableRootMutations.resync(
                        roots,
                        tableId,
                        definitionRef,
                        entry,
                        liveSnapshotIds,
                        id -> {
                          SnapshotManifestEntry loaded = snapshotEntry(tableId, id).orElse(null);
                          if (loaded == null) {
                            // A registered snapshot whose blob is not yet resolvable can't join the
                            // manifest this pass. Membership is incomplete, so keep the re-drive
                            // marker (converged=false): a transaction-only table has no other
                            // writer
                            // to converge its root, and a later pass registers it once the blob
                            // lands.
                            converged[0] = false;
                          }
                          return loaded;
                        })
                    .apply(current);
              });
          // A drop can race the commit: the committer persists synthesized history even on a
          // mutator no-op, so a definition-less root (built from lingering snapshot pointers
          // mid-drop-cleanup) may have just been created. Re-probe and take the drop path.
          if (BlobRefs.refFrom(tables.metaForSafe(tableId)) == null) {
            return deleteRoot(tableId);
          }
          return converged[0];
        });
  }

  /**
   * Builds the manifest entry for a snapshot from its committed blob identity — the snapshot ref
   * and (when present) its upstream_created_at. Empty when the snapshot blob is not yet resolvable.
   * Shared by the resync's current-entry force and its missing-snapshot registration.
   */
  private java.util.Optional<SnapshotManifestEntry> snapshotEntry(
      ResourceId tableId, long snapshotId) {
    BlobRef snapshotRef = BlobRefs.refFrom(snapshots.metaForSafe(tableId, snapshotId));
    if (snapshotRef == null) {
      return java.util.Optional.empty();
    }
    SnapshotManifestEntry.Builder builder =
        SnapshotManifestEntry.newBuilder().setSnapshotId(snapshotId).setSnapshotRef(snapshotRef);
    snapshots
        .getById(tableId, snapshotId)
        .filter(Snapshot::hasUpstreamCreatedAt)
        .ifPresent(s -> builder.setUpstreamCreatedAt(s.getUpstreamCreatedAt()));
    return java.util.Optional.of(builder.build());
  }

  /** Every currently-registered snapshot id (a live by-id pointer), paged in full. */
  private java.util.Set<Long> registeredSnapshotIds(ResourceId tableId) {
    java.util.Set<Long> ids = new java.util.HashSet<>();
    String token = "";
    StringBuilder next = new StringBuilder();
    do {
      next.setLength(0);
      java.util.List<Snapshot> page = snapshots.list(tableId, 500, token, next);
      for (Snapshot s : page) {
        ids.add(s.getSnapshotId());
      }
      token = next.toString();
    } while (!token.isEmpty());
    return ids;
  }

  /** Deletes the table's root pointer; true when gone (or already absent), false on contention. */
  private boolean deleteRoot(ResourceId tableId) {
    for (int attempt = 0; attempt < 2; attempt++) {
      MutationMeta meta = roots.metaForSafe(tableId);
      if (meta == null || meta.getPointerVersion() == 0L) {
        return true;
      }
      if (roots.deleteWithPrecondition(tableId, meta.getPointerVersion())) {
        return true;
      }
    }
    return false;
  }

  private boolean absorb(
      ResourceId tableId, String what, java.util.function.BooleanSupplier commit) {
    try {
      return commit.getAsBoolean();
    } catch (RuntimeException e) {
      LOG.warnf(
          e,
          "table root %s skipped for table %s (self-heals on next commit)",
          what,
          tableId.getId());
      return false;
    }
  }
}
