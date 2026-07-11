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
import ai.floedb.floecat.catalog.rpc.SnapshotManifestEntry;
import ai.floedb.floecat.catalog.rpc.TableRoot;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.impl.SnapshotManifests;
import ai.floedb.floecat.service.repo.impl.TableRootRepository;
import java.util.Optional;

/**
 * The standard {@link TableRootCommitter.RootMutator}s for table-state changes. Each is a pure
 * function of the current root (plus content-addressed manifest-page writes, which are idempotent),
 * so the committer can re-run it against a CAS winner's root and the mutations merge. Every mutator
 * walks the manifest through one {@link SnapshotManifests.Chain}, so find, currency check, and
 * rewrite read each page blob at most once per attempt.
 *
 * <p>The current-snapshot advance rule lives here: a snapshot becomes current when its
 * upstream_created_at is newer than the incumbent's (snapshot id breaks ties); an entry update for
 * the already-current snapshot keeps currency without an advance. This is the rule the
 * current-snapshot pointer machinery applied; the root absorbs it.
 */
public final class TableRootMutations {

  private static final org.jboss.logging.Logger LOG =
      org.jboss.logging.Logger.getLogger(TableRootMutations.class);

  private TableRootMutations() {}

  /**
   * Inserts or replaces the manifest entry for {@code entry.snapshot_id}. Replacing an existing
   * entry preserves its stats-generation and constraints refs unless the incoming entry carries its
   * own — an in-place snapshot update must not detach the snapshot's stats or constraints.
   *
   * <p>{@code advance} applies the advance rule so the snapshot may become current at registration.
   * Deployments whose stats store tracks generations pass {@code false}: a snapshot is not visible
   * to queries until it is finalized — its generation (file list, indexes, stats) published — and
   * {@link #setStatsGeneration} is the commit that advances currency. Stores that track no
   * generations cannot express readiness and keep advance-at-registration.
   */
  public static TableRootCommitter.RootMutator upsertSnapshot(
      TableRootRepository roots,
      ResourceId tableId,
      SnapshotManifestEntry entry,
      BlobRef definitionRef,
      boolean advance) {
    return current -> {
      TableRoot base = baseRoot(current, tableId, definitionRef);
      SnapshotManifests.Chain chain = SnapshotManifests.chain(roots, tableId, manifestHead(base));
      SnapshotManifestEntry merged =
          chain
              .findEntry(entry.getSnapshotId())
              .map(existing -> preserveAuxRefs(existing, entry))
              .orElse(entry);
      boolean advanceNow = advance && shouldAdvance(chain, base, merged);
      if (advance && !advanceNow && !merged.hasUpstreamCreatedAt()) {
        // A candidate without upstream_created_at sorts oldest and can never win the advance —
        // deliberate for backfills, but a writer that simply forgot the field would otherwise
        // debug a silently never-current snapshot.
        LOG.debugf(
            "snapshot %d of table %s has no upstream_created_at and lost the currency advance",
            merged.getSnapshotId(), tableId.getId());
      }
      TableRoot.Builder next = base.toBuilder().setSnapshotManifestRef(chain.upsert(merged));
      if (advanceNow) {
        next.setCurrentSnapshotId(merged.getSnapshotId());
      }
      return next.build();
    };
  }

  /**
   * Removes the snapshot's entry. When the removed snapshot was current, currency clears — there is
   * deliberately no fallback advance to an older snapshot, matching the pointer semantics this
   * replaces.
   */
  public static TableRootCommitter.RootMutator removeSnapshot(
      TableRootRepository roots, ResourceId tableId, long snapshotId) {
    return current -> {
      if (current.isEmpty()) {
        return null; // nothing to remove
      }
      TableRoot base = current.get();
      BlobRef head = manifestHead(base);
      BlobRef newHead = SnapshotManifests.chain(roots, tableId, head).remove(snapshotId);
      if (equalsRef(newHead, head)) {
        return null; // id not present: no-op
      }
      TableRoot.Builder next = base.toBuilder();
      if (newHead == null) {
        next.clearSnapshotManifestRef();
      } else {
        next.setSnapshotManifestRef(newHead);
      }
      if (base.hasCurrentSnapshotId() && base.getCurrentSnapshotId() == snapshotId) {
        next.clearCurrentSnapshotId();
      }
      return next.build();
    };
  }

  /** Sets the immutable table-definition ref (DDL / property change). */
  public static TableRootCommitter.RootMutator setDefinition(
      ResourceId tableId, BlobRef definitionRef) {
    return current ->
        baseRoot(current, tableId, definitionRef).toBuilder()
            .setDefinitionRef(definitionRef)
            .build();
  }

  /**
   * Sets the stats-generation ref on an existing snapshot's entry — the generation publish, which
   * is also the snapshot's VISIBILITY commit: a non-null ref finalizes the snapshot, so the same
   * CAS applies the advance rule and may make it current. Snapshot, file list, indexes, and stats
   * become queryable together, or not at all. A null ref (generation removal) never touches
   * currency.
   */
  public static TableRootCommitter.RootMutator setStatsGeneration(
      TableRootRepository roots, ResourceId tableId, long snapshotId, BlobRef generationRef) {
    return updateEntry(
        roots,
        tableId,
        snapshotId,
        e -> {
          var b = e.toBuilder();
          if (generationRef == null) {
            b.clearStatsGenerationRef();
          } else {
            b.setStatsGenerationRef(generationRef);
          }
          return b.build();
        },
        generationRef != null);
  }

  /** Sets the constraints ref on an existing snapshot's entry (constraints write). */
  public static TableRootCommitter.RootMutator setConstraints(
      TableRootRepository roots, ResourceId tableId, long snapshotId, BlobRef constraintsRef) {
    return updateEntry(
        roots,
        tableId,
        snapshotId,
        e -> {
          var b = e.toBuilder();
          if (constraintsRef == null) {
            b.clearConstraintsRef();
          } else {
            b.setConstraintsRef(constraintsRef);
          }
          return b.build();
        },
        false);
  }

  /**
   * Re-derives the root's table-level legs from the committed pointer families: the definition ref
   * and the current-snapshot selection. Currency is FORCED to the committed pointer, not run
   * through the advance rule — the caller observed an authoritative selection (a transactional
   * commit may legitimately move currency to any snapshot, including an older one). {@code
   * currentEntry == null} means the committed state has no current snapshot and clears currency.
   *
   * <p>{@code gateOnFinalize}: when the deployment gates visibility on finalize, currency is forced
   * only onto a FINALIZED entry (one carrying its generation ref). A transaction that committed a
   * brand-new snapshot registers its entry here, but the previous finalized snapshot keeps serving
   * until the post-commit finalize publishes — the same gate every writer obeys.
   */
  public static TableRootCommitter.RootMutator resync(
      TableRootRepository roots,
      ResourceId tableId,
      BlobRef definitionRef,
      SnapshotManifestEntry currentEntry,
      boolean gateOnFinalize) {
    return current -> {
      TableRoot base = baseRoot(current, tableId, definitionRef);
      TableRoot.Builder next = base.toBuilder();
      if (definitionRef != null && !definitionRef.getUri().isEmpty()) {
        next.setDefinitionRef(definitionRef);
      }
      if (currentEntry == null) {
        next.clearCurrentSnapshotId();
      } else {
        SnapshotManifests.Chain chain = SnapshotManifests.chain(roots, tableId, manifestHead(base));
        SnapshotManifestEntry merged =
            chain
                .findEntry(currentEntry.getSnapshotId())
                .map(existing -> preserveAuxRefs(existing, currentEntry))
                .orElse(currentEntry);
        next.setSnapshotManifestRef(chain.upsert(merged));
        if (!gateOnFinalize || merged.hasStatsGenerationRef()) {
          next.setCurrentSnapshotId(merged.getSnapshotId());
        }
      }
      return next.build();
    };
  }

  /**
   * The shared core for in-place entry updates: find the entry, apply the change, and — when the
   * change warrants it — run the advance rule, all over one chain walk. No-ops (missing root,
   * unknown snapshot, unchanged entry) return {@code null} so the committer skips the CAS.
   */
  private static TableRootCommitter.RootMutator updateEntry(
      TableRootRepository roots,
      ResourceId tableId,
      long snapshotId,
      java.util.function.UnaryOperator<SnapshotManifestEntry> change,
      boolean advanceOnChange) {
    return current -> {
      if (current.isEmpty()) {
        return null; // no root yet: nothing to attach the ref to
      }
      TableRoot base = current.get();
      SnapshotManifests.Chain chain = SnapshotManifests.chain(roots, tableId, manifestHead(base));
      Optional<SnapshotManifestEntry> existing = chain.findEntry(snapshotId);
      if (existing.isEmpty()) {
        return null; // snapshot unknown to the manifest: no-op
      }
      SnapshotManifestEntry changed = change.apply(existing.get());
      if (changed.equals(existing.get())) {
        return null;
      }
      boolean advanceNow = advanceOnChange && shouldAdvance(chain, base, changed);
      TableRoot.Builder next = base.toBuilder().setSnapshotManifestRef(chain.upsert(changed));
      if (advanceNow) {
        next.setCurrentSnapshotId(changed.getSnapshotId());
      }
      return next.build();
    };
  }

  private static TableRoot baseRoot(
      Optional<TableRoot> current, ResourceId tableId, BlobRef definitionRef) {
    if (current.isPresent()) {
      return current.get();
    }
    TableRoot.Builder first = TableRoot.newBuilder().setTableId(tableId);
    if (definitionRef != null && !definitionRef.getUri().isEmpty()) {
      first.setDefinitionRef(definitionRef);
    }
    return first.build();
  }

  private static BlobRef manifestHead(TableRoot root) {
    return root.hasSnapshotManifestRef() ? root.getSnapshotManifestRef() : null;
  }

  private static boolean equalsRef(BlobRef a, BlobRef b) {
    return (a == null || b == null) ? a == b : a.equals(b);
  }

  private static SnapshotManifestEntry preserveAuxRefs(
      SnapshotManifestEntry existing, SnapshotManifestEntry incoming) {
    SnapshotManifestEntry.Builder merged = incoming.toBuilder();
    if (!incoming.hasStatsGenerationRef() && existing.hasStatsGenerationRef()) {
      merged.setStatsGenerationRef(existing.getStatsGenerationRef());
    }
    if (!incoming.hasConstraintsRef() && existing.hasConstraintsRef()) {
      merged.setConstraintsRef(existing.getConstraintsRef());
    }
    return merged.build();
  }

  private static boolean shouldAdvance(
      SnapshotManifests.Chain chain, TableRoot base, SnapshotManifestEntry candidate) {
    if (!base.hasCurrentSnapshotId()) {
      return true;
    }
    if (base.getCurrentSnapshotId() == candidate.getSnapshotId()) {
      return false; // entry update for the already-current snapshot: currency unchanged
    }
    Optional<SnapshotManifestEntry> incumbent = chain.findEntry(base.getCurrentSnapshotId());
    if (incumbent.isEmpty()) {
      return true; // currency points at a vanished entry: the candidate takes over
    }
    return SnapshotManifests.newer(candidate, incumbent.get());
  }
}
