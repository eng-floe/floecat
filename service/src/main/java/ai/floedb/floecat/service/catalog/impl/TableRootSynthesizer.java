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
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotManifestEntry;
import ai.floedb.floecat.catalog.rpc.TableRoot;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.impl.ConstraintRepository;
import ai.floedb.floecat.service.repo.impl.SnapshotManifests;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.TableRootRepository;
import ai.floedb.floecat.service.repo.model.BlobRefs;
import ai.floedb.floecat.stats.spi.StatsStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;

/**
 * Builds a table's first {@link TableRoot} from the legacy pointer families, so a deployment with
 * pre-existing data needs no migration step: the first root commit on a legacy table synthesizes
 * its complete history — every snapshot's blob identity, active stats generation, and constraints
 * bundle, plus the current-snapshot selection — and the mutation lands on top of it in the same
 * atomic commit. Without this, the first commit would create a root containing only the new entry
 * and the table's history would vanish when reads move to the root.
 *
 * <p>Manifest pages are content-addressed, so concurrent synthesizers of the same legacy state
 * converge on identical pages and the root CAS picks one winner cleanly.
 */
@ApplicationScoped
public class TableRootSynthesizer {

  private static final int SNAPSHOT_PAGE = 200;

  private final TableRepository tables;
  private final SnapshotRepository snapshots;
  private final StatsStore stats;
  private final ConstraintRepository constraints;
  private final TableRootRepository roots;

  @Inject
  public TableRootSynthesizer(
      TableRepository tables,
      SnapshotRepository snapshots,
      StatsStore stats,
      ConstraintRepository constraints,
      TableRootRepository roots) {
    this.tables = tables;
    this.snapshots = snapshots;
    this.stats = stats;
    this.constraints = constraints;
    this.roots = roots;
  }

  /**
   * Synthesizes the root a legacy table would have if every historical write had been a root
   * commit. Empty when the table does not exist (no definition blob and no snapshots). The result
   * carries no {@code root_seq}/{@code committed_at}; the committer stamps them when it persists.
   */
  public Optional<TableRoot> synthesize(ResourceId tableId) {
    MutationMeta tableMeta = tables.metaForSafe(tableId);
    boolean hasDefinition = tableMeta != null && !tableMeta.getBlobUri().isEmpty();

    boolean hasSnapshots = false;
    java.util.Set<Long> manifestSnapshotIds = new java.util.HashSet<>();
    List<SnapshotManifestEntry> newestFirst = new java.util.ArrayList<>();
    String token = "";
    while (true) {
      StringBuilder next = new StringBuilder();
      List<Snapshot> page = snapshots.listByTime(tableId, SNAPSHOT_PAGE, token, next);
      for (Snapshot snapshot : page) {
        Optional<SnapshotManifestEntry> entry = entryFor(tableId, snapshot);
        if (entry.isPresent()) {
          if (!manifestSnapshotIds.add(entry.get().getSnapshotId())) {
            continue; // prepend's contract: never insert an id the chain already carries
          }
          hasSnapshots = true;
          newestFirst.add(entry.get());
        }
      }
      token = next.toString();
      if (token.isBlank()) {
        break;
      }
    }
    // Fold OLDEST-first so the head page holds the newest snapshots — the invariant every
    // mutation path produces, and what keeps current/recent-AS_OF reads on one page for migrated
    // tables. The fold shares one content-addressed page cache across inserts (withHead) and
    // skips the per-insert existing-id walk (prepend; ids are unique by the seen-ids set) — N
    // snapshots synthesize in O(N) page reads, not O(N^2).
    BlobRef manifestHead = null;
    SnapshotManifests.Chain chain = SnapshotManifests.chain(roots, tableId, null);
    for (int i = newestFirst.size() - 1; i >= 0; i--) {
      chain = chain.withHead(manifestHead);
      manifestHead = chain.prepend(newestFirst.get(i));
    }

    if (!hasDefinition && !hasSnapshots) {
      return Optional.empty(); // no legacy trace of this table
    }

    TableRoot.Builder root = TableRoot.newBuilder().setTableId(tableId);
    if (hasDefinition) {
      root.setDefinitionRef(BlobRefs.refFrom(tableMeta));
    }
    if (manifestHead != null) {
      root.setSnapshotManifestRef(manifestHead);
    }
    // Currency imports from the legacy pointer (the raw one — this IS the migration input) only
    // when its target actually made it into the manifest: a dangling pointer (snapshot blob gone)
    // must not become currency nothing can resolve. Query visibility is applied by readers from the
    // manifest entry's stats_generation_ref, not by suppressing the committed current selection.
    boolean gate = StatsVisibilityGate.gateOnFinalize(stats);
    Optional<Long> pointerTarget =
        snapshots
            .latestRegisteredSnapshotPointer(tableId)
            .map(pointer -> pointer.getSnapshotId())
            .filter(manifestSnapshotIds::contains);
    if (pointerTarget.isPresent()) {
      root.setCurrentSnapshotId(pointerTarget.get());
    } else if (!gate && manifestHead != null) {
      // Dangling or absent legacy pointer on a store that cannot gate: legacy reads fell back to
      // latest-by-time, so the import mirrors that with the advance rule's ordering (upstream
      // time, snapshot id as tiebreak) over the entries that actually made the manifest.
      newestEntry(manifestHead).ifPresent(e -> root.setCurrentSnapshotId(e.getSnapshotId()));
    }
    return Optional.of(root.build());
  }

  /** The advance rule's pick among the manifest's entries: newest upstream time, id tiebreak. */
  private Optional<SnapshotManifestEntry> newestEntry(BlobRef manifestHead) {
    SnapshotManifestEntry[] best = new SnapshotManifestEntry[1];
    SnapshotManifests.forEachEntry(
        roots,
        manifestHead,
        e -> {
          if (best[0] == null || SnapshotManifests.newer(e, best[0])) {
            best[0] = e;
          }
        });
    return Optional.ofNullable(best[0]);
  }

  /**
   * One legacy snapshot's complete entry: blob identity (skipped entirely when the blob is not
   * resolvable — a half-created snapshot cannot be represented coherently), active stats
   * generation, and constraints bundle where they exist.
   */
  private Optional<SnapshotManifestEntry> entryFor(ResourceId tableId, Snapshot snapshot) {
    MutationMeta snapMeta = snapshots.metaForSafe(tableId, snapshot.getSnapshotId());
    if (snapMeta == null || snapMeta.getBlobUri().isEmpty()) {
      return Optional.empty();
    }
    SnapshotManifestEntry.Builder entry =
        SnapshotManifestEntry.newBuilder()
            .setSnapshotId(snapshot.getSnapshotId())
            .setSnapshotRef(BlobRefs.refFrom(snapMeta));
    if (snapshot.hasUpstreamCreatedAt()) {
      entry.setUpstreamCreatedAt(snapshot.getUpstreamCreatedAt());
    }
    stats
        .activeStatsGeneration(tableId, snapshot.getSnapshotId())
        .ifPresent(uri -> entry.setStatsGenerationRef(BlobRef.newBuilder().setUri(uri)));
    BlobRef constraintsRef =
        BlobRefs.refFrom(constraints.metaForSafe(tableId, snapshot.getSnapshotId()));
    if (constraintsRef != null) {
      entry.setConstraintsRef(constraintsRef);
    }
    return Optional.of(entry.build());
  }
}
