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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.BlobRef;
import ai.floedb.floecat.catalog.rpc.SnapshotManifestEntry;
import ai.floedb.floecat.catalog.rpc.TableRoot;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.impl.SnapshotManifests;
import ai.floedb.floecat.service.repo.impl.TableRootRepository;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import com.google.protobuf.util.Timestamps;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TableRootMutationsTest {

  private static final ResourceId TABLE =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("tbl")
          .setKind(ResourceKind.RK_TABLE)
          .build();

  private TableRootRepository roots;
  private TableRootCommitter committer;

  @BeforeEach
  void setUp() {
    roots = new TableRootRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    committer = new TableRootCommitter(roots);
  }

  private static BlobRef ref(String uri) {
    return BlobRef.newBuilder().setUri(uri).setVersion("v-" + uri).build();
  }

  private static SnapshotManifestEntry entry(long snapshotId, long upstreamMs) {
    return SnapshotManifestEntry.newBuilder()
        .setSnapshotId(snapshotId)
        .setSnapshotRef(ref("s3://t/snap-" + snapshotId + ".pb"))
        .setUpstreamCreatedAt(Timestamps.fromMillis(upstreamMs))
        .build();
  }

  private TableRoot commit(TableRootCommitter.RootMutator mutator) {
    return committer.commit(TABLE, mutator).orElseThrow();
  }

  @Test
  void firstSnapshotBecomesCurrentAndCreatesTheRoot() {
    TableRoot root =
        commit(
            TableRootMutations.upsertSnapshot(
                roots, TABLE, entry(7, 1_000), ref("s3://t/def.pb"), true));

    assertEquals(7L, root.getCurrentSnapshotId());
    assertEquals("s3://t/def.pb", root.getDefinitionRef().getUri());
    assertTrue(SnapshotManifests.findEntry(roots, root.getSnapshotManifestRef(), 7).isPresent());
  }

  @Test
  void newerUpstreamAdvancesCurrencyAndOlderDoesNot() {
    commit(
        TableRootMutations.upsertSnapshot(
            roots, TABLE, entry(1, 2_000), ref("s3://t/def.pb"), true));

    TableRoot afterOlder =
        commit(TableRootMutations.upsertSnapshot(roots, TABLE, entry(2, 1_000), null, true));
    assertEquals(1L, afterOlder.getCurrentSnapshotId(), "older upstream must not advance");

    TableRoot afterNewer =
        commit(TableRootMutations.upsertSnapshot(roots, TABLE, entry(3, 3_000), null, true));
    assertEquals(3L, afterNewer.getCurrentSnapshotId());
  }

  @Test
  void equalUpstreamBreaksTiesBySnapshotId() {
    commit(
        TableRootMutations.upsertSnapshot(
            roots, TABLE, entry(5, 1_000), ref("s3://t/def.pb"), true));

    TableRoot afterLowerId =
        commit(TableRootMutations.upsertSnapshot(roots, TABLE, entry(4, 1_000), null, true));
    assertEquals(5L, afterLowerId.getCurrentSnapshotId());

    TableRoot afterHigherId =
        commit(TableRootMutations.upsertSnapshot(roots, TABLE, entry(6, 1_000), null, true));
    assertEquals(6L, afterHigherId.getCurrentSnapshotId());
  }

  @Test
  void inPlaceUpdatePreservesAuxRefsAndKeepsCurrency() {
    commit(
        TableRootMutations.upsertSnapshot(
            roots, TABLE, entry(7, 1_000), ref("s3://t/def.pb"), true));
    commit(TableRootMutations.setStatsGeneration(roots, TABLE, 7, ref("s3://t/gen-1.pb"), 7L));
    commit(TableRootMutations.setConstraints(roots, TABLE, 7, ref("s3://t/constraints-1.pb")));

    // The in-place snapshot update carries a new snapshot blob but no aux refs of its own.
    TableRoot updated =
        commit(
            TableRootMutations.upsertSnapshot(
                roots,
                TABLE,
                entry(7, 1_000).toBuilder().setSnapshotRef(ref("s3://t/snap-7-v2.pb")).build(),
                null,
                true));

    var e = SnapshotManifests.findEntry(roots, updated.getSnapshotManifestRef(), 7).orElseThrow();
    assertEquals("s3://t/snap-7-v2.pb", e.getSnapshotRef().getUri());
    assertEquals("s3://t/gen-1.pb", e.getStatsGenerationRef().getUri(), "stats ref preserved");
    assertEquals(
        "s3://t/constraints-1.pb", e.getConstraintsRef().getUri(), "constraints ref preserved");
    assertEquals(7L, updated.getCurrentSnapshotId(), "same-id update keeps currency");
  }

  @Test
  void removingTheCurrentSnapshotClearsCurrencyWithoutFallback() {
    commit(
        TableRootMutations.upsertSnapshot(
            roots, TABLE, entry(1, 1_000), ref("s3://t/def.pb"), true));
    commit(TableRootMutations.upsertSnapshot(roots, TABLE, entry(2, 2_000), null, true));

    TableRoot afterRemove = commit(TableRootMutations.removeSnapshot(roots, TABLE, 2));

    assertFalse(afterRemove.hasCurrentSnapshotId(), "no fallback advance to snapshot 1");
    assertTrue(
        SnapshotManifests.findEntry(roots, afterRemove.getSnapshotManifestRef(), 1).isPresent());
  }

  @Test
  void removingANonCurrentSnapshotKeepsCurrency() {
    commit(
        TableRootMutations.upsertSnapshot(
            roots, TABLE, entry(1, 1_000), ref("s3://t/def.pb"), true));
    commit(TableRootMutations.upsertSnapshot(roots, TABLE, entry(2, 2_000), null, true));

    TableRoot afterRemove = commit(TableRootMutations.removeSnapshot(roots, TABLE, 1));

    assertEquals(2L, afterRemove.getCurrentSnapshotId());
  }

  @Test
  void removingTheLastSnapshotClearsTheManifest() {
    commit(
        TableRootMutations.upsertSnapshot(
            roots, TABLE, entry(1, 1_000), ref("s3://t/def.pb"), true));

    TableRoot afterRemove = commit(TableRootMutations.removeSnapshot(roots, TABLE, 1));

    assertFalse(afterRemove.hasSnapshotManifestRef());
    assertFalse(afterRemove.hasCurrentSnapshotId());
  }

  @Test
  void removeOfUnknownIdAndAuxUpdateOfUnknownSnapshotAreNoOps() {
    commit(
        TableRootMutations.upsertSnapshot(
            roots, TABLE, entry(1, 1_000), ref("s3://t/def.pb"), true));
    long versionBefore = roots.metaForSafe(TABLE).getPointerVersion();

    committer.commit(TABLE, TableRootMutations.removeSnapshot(roots, TABLE, 99));
    committer.commit(
        TABLE, TableRootMutations.setStatsGeneration(roots, TABLE, 99, ref("g"), null));
    committer.commit(TABLE, TableRootMutations.setConstraints(roots, TABLE, 99, ref("c")));

    assertEquals(versionBefore, roots.metaForSafe(TABLE).getPointerVersion());
  }

  @Test
  void setDefinitionCreatesOrUpdatesTheRootWithoutTouchingSnapshots() {
    // DDL before any snapshot: creates a snapshot-less root.
    TableRoot fresh = commit(TableRootMutations.setDefinition(TABLE, ref("s3://t/def-1.pb")));
    assertFalse(fresh.hasCurrentSnapshotId());

    commit(TableRootMutations.upsertSnapshot(roots, TABLE, entry(1, 1_000), null, true));
    TableRoot updated = commit(TableRootMutations.setDefinition(TABLE, ref("s3://t/def-2.pb")));

    assertEquals("s3://t/def-2.pb", updated.getDefinitionRef().getUri());
    assertEquals(1L, updated.getCurrentSnapshotId());
  }

  @Test
  void snapshotIdZeroIsAValidCurrentSnapshot() {
    TableRoot root =
        commit(
            TableRootMutations.upsertSnapshot(
                roots, TABLE, entry(0, 1_000), ref("s3://t/def.pb"), true));

    assertTrue(root.hasCurrentSnapshotId());
    assertEquals(0L, root.getCurrentSnapshotId());

    TableRoot afterRemove = commit(TableRootMutations.removeSnapshot(roots, TABLE, 0));
    assertFalse(afterRemove.hasCurrentSnapshotId());
  }

  @Test
  void auxRefUpdatesLandOnTheEntryWithoutMovingCurrency() {
    commit(
        TableRootMutations.upsertSnapshot(
            roots, TABLE, entry(1, 1_000), ref("s3://t/def.pb"), true));
    commit(TableRootMutations.upsertSnapshot(roots, TABLE, entry(2, 2_000), null, true));

    TableRoot afterGen =
        commit(TableRootMutations.setStatsGeneration(roots, TABLE, 1, ref("s3://t/gen-a.pb"), 2L));

    var e1 = SnapshotManifests.findEntry(roots, afterGen.getSnapshotManifestRef(), 1).orElseThrow();
    assertEquals("s3://t/gen-a.pb", e1.getStatsGenerationRef().getUri());
    assertEquals(2L, afterGen.getCurrentSnapshotId());

    // Clearing works too (generation retired without a replacement).
    TableRoot cleared = commit(TableRootMutations.setStatsGeneration(roots, TABLE, 1, null, null));
    assertFalse(
        SnapshotManifests.findEntry(roots, cleared.getSnapshotManifestRef(), 1)
            .orElseThrow()
            .hasStatsGenerationRef());
  }

  @Test
  void currencyPointingAtAVanishedEntryYieldsToTheNextCandidate() {
    // Construct a root whose current id has no manifest entry (a repaired/degenerate state).
    commit(
        TableRootMutations.upsertSnapshot(
            roots, TABLE, entry(5, 5_000), ref("s3://t/def.pb"), true));
    commit(TableRootMutations.removeSnapshot(roots, TABLE, 5));
    // Manually re-point currency at the vanished id to simulate the degenerate state.
    TableRoot degenerate =
        committer
            .commit(
                TABLE,
                current -> current.orElseThrow().toBuilder().setCurrentSnapshotId(5L).build())
            .orElseThrow();
    assertEquals(5L, degenerate.getCurrentSnapshotId());

    TableRoot repaired =
        commit(TableRootMutations.upsertSnapshot(roots, TABLE, entry(1, 1_000), null, true));

    assertEquals(1L, repaired.getCurrentSnapshotId(), "candidate takes over from vanished entry");
  }

  @Test
  void registrationWithoutAdvanceLeavesCurrencyUntouched() {
    // Some paths register manifest membership without changing the committed current selection.
    committer.commit(
        TABLE, TableRootMutations.upsertSnapshot(roots, TABLE, entry(7, 7_000), null, false));

    var root = roots.get(TABLE).orElseThrow();
    assertFalse(root.hasCurrentSnapshotId());
    assertTrue(
        ai.floedb.floecat.service.repo.impl.SnapshotManifests.findEntry(
                roots, root.getSnapshotManifestRef(), 7)
            .isPresent());
  }

  @Test
  void generationPublishIsTheVisibilityCommit() {
    committer.commit(
        TABLE, TableRootMutations.upsertSnapshot(roots, TABLE, entry(7, 7_000), null, false));

    // ONE CAS sets the generation ref AND currency: snapshot, file list, indexes, and stats
    // become queryable together.
    committer.commit(
        TABLE,
        TableRootMutations.setStatsGeneration(
            roots,
            TABLE,
            7,
            ai.floedb.floecat.catalog.rpc.BlobRef.newBuilder()
                .setUri("s3://t/stats/7/gen-1.pb")
                .build(),
            7L));

    var root = roots.get(TABLE).orElseThrow();
    assertEquals(7, root.getCurrentSnapshotId());
    assertEquals(
        "s3://t/stats/7/gen-1.pb",
        ai.floedb.floecat.service.repo.impl.SnapshotManifests.findEntry(
                roots, root.getSnapshotManifestRef(), 7)
            .orElseThrow()
            .getStatsGenerationRef()
            .getUri());
  }

  @Test
  void anOlderSnapshotFinalizingLateDoesNotStealCurrency() {
    committer.commit(
        TABLE, TableRootMutations.upsertSnapshot(roots, TABLE, entry(3, 3_000), null, false));
    committer.commit(
        TABLE, TableRootMutations.upsertSnapshot(roots, TABLE, entry(7, 7_000), null, false));
    var gen = ai.floedb.floecat.catalog.rpc.BlobRef.newBuilder().setUri("s3://t/gen.pb").build();
    committer.commit(TABLE, TableRootMutations.setStatsGeneration(roots, TABLE, 7, gen, 7L));
    // Snapshot 3 finalizes out of order: it is not the committed current (7), so it cannot steal.
    committer.commit(TABLE, TableRootMutations.setStatsGeneration(roots, TABLE, 3, gen, 7L));

    assertEquals(7, roots.get(TABLE).orElseThrow().getCurrentSnapshotId());
  }

  @Test
  void finalizeHonorsCommittedCurrentEvenWhenItIsOlderThanTheIncumbent() {
    // The bug: a transaction moved /snapshots/current to an OLDER, not-yet-finalized snapshot (3)
    // while a NEWER snapshot (7) is the finalized incumbent. Ordering (shouldAdvance) would refuse
    // to promote 3 over 7 and would let any newer finalize steal currency. Currency must instead
    // follow the committed current pointer.
    var gen = ai.floedb.floecat.catalog.rpc.BlobRef.newBuilder().setUri("s3://t/gen.pb").build();
    committer.commit(
        TABLE, TableRootMutations.upsertSnapshot(roots, TABLE, entry(7, 7_000), null, false));
    committer.commit(TABLE, TableRootMutations.setStatsGeneration(roots, TABLE, 7, gen, 7L));
    assertEquals(7, roots.get(TABLE).orElseThrow().getCurrentSnapshotId());

    // Transactional rollback: committed current is now the older snapshot 3, registered
    // unfinalized.
    committer.commit(
        TABLE, TableRootMutations.upsertSnapshot(roots, TABLE, entry(3, 3_000), null, false));
    committer.commit(TABLE, TableRootMutations.setStatsGeneration(roots, TABLE, 3, gen, 3L));
    assertEquals(
        3, roots.get(TABLE).orElseThrow().getCurrentSnapshotId(), "older committed current wins");

    // A newer snapshot 9 finalizes while 3 is still the committed current: it must NOT steal it.
    committer.commit(
        TABLE, TableRootMutations.upsertSnapshot(roots, TABLE, entry(9, 9_000), null, false));
    committer.commit(TABLE, TableRootMutations.setStatsGeneration(roots, TABLE, 9, gen, 3L));
    assertEquals(
        3,
        roots.get(TABLE).orElseThrow().getCurrentSnapshotId(),
        "a non-committed newer snapshot cannot steal currency");
  }

  @Test
  void generationRemovalNeverTouchesCurrency() {
    committer.commit(
        TABLE, TableRootMutations.upsertSnapshot(roots, TABLE, entry(7, 7_000), null, false));
    var gen = ai.floedb.floecat.catalog.rpc.BlobRef.newBuilder().setUri("s3://t/gen.pb").build();
    committer.commit(TABLE, TableRootMutations.setStatsGeneration(roots, TABLE, 7, gen, 7L));

    committer.commit(TABLE, TableRootMutations.setStatsGeneration(roots, TABLE, 7, null, null));

    var root = roots.get(TABLE).orElseThrow();
    assertEquals(7, root.getCurrentSnapshotId());
    assertFalse(
        ai.floedb.floecat.service.repo.impl.SnapshotManifests.findEntry(
                roots, root.getSnapshotManifestRef(), 7)
            .orElseThrow()
            .hasStatsGenerationRef());
  }

  @Test
  void resyncPrunesSnapshotsThatAreNoLongerRegistered() {
    // Transactional expire/remove-snapshots clears a non-current snapshot's pointers by raw CAS,
    // never through removeSnapshot. The resync's membership reconcile must drop its root entry.
    committer.commit(
        TABLE, TableRootMutations.upsertSnapshot(roots, TABLE, entry(3, 3_000), null, true));
    committer.commit(
        TABLE, TableRootMutations.upsertSnapshot(roots, TABLE, entry(7, 7_000), null, true));
    assertTrue(
        SnapshotManifests.findEntry(
                roots, roots.get(TABLE).orElseThrow().getSnapshotManifestRef(), 3)
            .isPresent());

    // Snapshot 3 was expired transactionally: only 7 remains registered.
    committer.commit(
        TABLE,
        TableRootMutations.resync(
            roots, TABLE, null, entry(7, 7_000), java.util.Set.of(7L), null));

    var root = roots.get(TABLE).orElseThrow();
    assertFalse(
        SnapshotManifests.findEntry(roots, root.getSnapshotManifestRef(), 3).isPresent(),
        "expired snapshot pruned from the root chain");
    assertTrue(SnapshotManifests.findEntry(roots, root.getSnapshotManifestRef(), 7).isPresent());
    assertEquals(7L, root.getCurrentSnapshotId());
  }

  @Test
  void resyncClearsCurrencyWhenTheCurrentSnapshotIsNoLongerRegistered() {
    // Degenerate: the snapshot currency points at is gone from the registry (and not re-selected).
    // Membership reconcile drops its entry and clears currency rather than dangling.
    committer.commit(
        TABLE, TableRootMutations.upsertSnapshot(roots, TABLE, entry(5, 5_000), null, true));
    assertEquals(5L, roots.get(TABLE).orElseThrow().getCurrentSnapshotId());

    // No committed current, snapshot 5 no longer registered.
    committer.commit(
        TABLE,
        TableRootMutations.resync(roots, TABLE, null, null, java.util.Set.of(), null));

    var root = roots.get(TABLE).orElseThrow();
    assertFalse(root.hasCurrentSnapshotId());
    assertFalse(SnapshotManifests.findEntry(roots, root.getSnapshotManifestRef(), 5).isPresent());
  }

  @Test
  void resyncRegistersLiveSnapshotsMissingFromTheManifest() {
    // A transactional multi-snapshot commit added snapshot 3 as non-current (live by-id pointer)
    // but no writer created its manifest entry. resync must register it from the loader, not only
    // prune — otherwise it stays invisible to root-based enumeration.
    committer.commit(
        TABLE, TableRootMutations.upsertSnapshot(roots, TABLE, entry(7, 7_000), null, true));
    assertFalse(
        SnapshotManifests.findEntry(
                roots, roots.get(TABLE).orElseThrow().getSnapshotManifestRef(), 3)
            .isPresent());

    SnapshotManifestEntry missing = entry(3, 3_000);
    committer.commit(
        TABLE,
        TableRootMutations.resync(
            roots,
            TABLE,
            null,
            entry(7, 7_000),
            java.util.Set.of(3L, 7L),
            id -> id == 3L ? missing : null));

    var root = roots.get(TABLE).orElseThrow();
    assertTrue(
        SnapshotManifests.findEntry(roots, root.getSnapshotManifestRef(), 3).isPresent(),
        "missing live snapshot registered");
    assertTrue(SnapshotManifests.findEntry(roots, root.getSnapshotManifestRef(), 7).isPresent());
    assertEquals(7L, root.getCurrentSnapshotId());
  }

  @Test
  void inPlaceRewriteWithoutTimestampPreservesUpstreamCreatedAt() {
    // An in-place rewrite whose candidate omits upstream_created_at must not re-sort the snapshot
    // to "oldest" for the currency / AS_OF tie-break — the field is preserved like the aux refs.
    commit(
        TableRootMutations.upsertSnapshot(
            roots, TABLE, entry(7, 7_000), ref("s3://t/def.pb"), true));

    TableRoot updated =
        commit(
            TableRootMutations.upsertSnapshot(
                roots,
                TABLE,
                SnapshotManifestEntry.newBuilder()
                    .setSnapshotId(7)
                    .setSnapshotRef(ref("s3://t/snap-7-v2.pb"))
                    .build(),
                null,
                true));

    var e = SnapshotManifests.findEntry(roots, updated.getSnapshotManifestRef(), 7).orElseThrow();
    assertEquals("s3://t/snap-7-v2.pb", e.getSnapshotRef().getUri());
    assertTrue(e.hasUpstreamCreatedAt(), "timestamp preserved");
    assertEquals(7_000L, com.google.protobuf.util.Timestamps.toMillis(e.getUpstreamCreatedAt()));
  }

  @Test
  void reFinalizeAdvancesCurrencyWhenTheCommittedCurrentMovedOntoAnAlreadyFinalEntry() {
    // Snapshot 7 is finalized and current; snapshot 3 is finalized but not current.
    var gen = ai.floedb.floecat.catalog.rpc.BlobRef.newBuilder().setUri("s3://t/gen.pb").build();
    commit(
        TableRootMutations.upsertSnapshot(
            roots, TABLE, entry(7, 7_000), ref("s3://t/def.pb"), true));
    committer.commit(
        TABLE, TableRootMutations.upsertSnapshot(roots, TABLE, entry(3, 3_000), null, false));
    committer.commit(TABLE, TableRootMutations.setStatsGeneration(roots, TABLE, 3, gen, 7L));
    assertEquals(7L, roots.get(TABLE).orElseThrow().getCurrentSnapshotId());

    // /snapshots/current moved to 3 (rollback). A re-finalize carries the SAME ref (unchanged
    // entry) but must still advance root currency onto 3 — the no-op must not skip currency.
    committer.commit(TABLE, TableRootMutations.setStatsGeneration(roots, TABLE, 3, gen, 3L));
    assertEquals(
        3L,
        roots.get(TABLE).orElseThrow().getCurrentSnapshotId(),
        "unchanged-entry re-finalize still advances currency to the committed current");
  }

  @Test
  void gatedResyncRecordsCommittedCurrentEvenWhenTheEntryIsUnfinalized() {
    // The gated table already has finalized snapshot 3; a transaction commits snapshot 7 and moves
    // the committed pointer. Root currency mirrors that committed selection immediately. Query
    // readers apply the finalize gate from the manifest entry, not by leaving root currency stale.
    committer.commit(
        TABLE, TableRootMutations.upsertSnapshot(roots, TABLE, entry(3, 3_000), null, false));
    var gen = ai.floedb.floecat.catalog.rpc.BlobRef.newBuilder().setUri("s3://t/gen-3.pb").build();
    committer.commit(TABLE, TableRootMutations.setStatsGeneration(roots, TABLE, 3, gen, 3L));

    committer.commit(
        TABLE,
        TableRootMutations.resync(
            roots, TABLE, null, entry(7, 7_000), java.util.Set.of(3L, 7L), null));

    var root = roots.get(TABLE).orElseThrow();
    assertEquals(7, root.getCurrentSnapshotId());
    assertTrue(
        ai.floedb.floecat.service.repo.impl.SnapshotManifests.findEntry(
                roots, root.getSnapshotManifestRef(), 7)
            .isPresent());
  }

  @Test
  void theGateTreatsSnapshotIdZeroAsARealId() {
    // Snapshot id 0 is a valid id; presence semantics (hasCurrentSnapshotId) — not the proto
    // default value — must carry currency. Membership-only registration leaves currency empty; the
    // generation publish can still project committed current id 0, with currency PRESENT and equal
    // to zero.
    committer.commit(
        TABLE,
        TableRootMutations.upsertSnapshot(
            roots,
            TABLE,
            SnapshotManifestEntry.newBuilder()
                .setSnapshotId(0)
                .setSnapshotRef(BlobRef.newBuilder().setUri("s3://t/snap-0.pb").setVersion("v0"))
                .setUpstreamCreatedAt(Timestamps.fromMillis(1_000))
                .build(),
            null,
            false));
    assertFalse(roots.get(TABLE).orElseThrow().hasCurrentSnapshotId());

    committer.commit(
        TABLE,
        TableRootMutations.setStatsGeneration(
            roots, TABLE, 0, BlobRef.newBuilder().setUri("s3://t/stats/0/gen.pb").build(), 0L));

    var root = roots.get(TABLE).orElseThrow();
    assertTrue(root.hasCurrentSnapshotId(), "id 0 becomes current at its visibility commit");
    assertEquals(0L, root.getCurrentSnapshotId());
  }
}
