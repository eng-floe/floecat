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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.BlobRef;
import ai.floedb.floecat.catalog.rpc.CurrentSnapshotPointer;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotManifestEntry;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.impl.ConstraintRepository;
import ai.floedb.floecat.service.repo.impl.SnapshotManifests;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.TableRootRepository;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import com.google.protobuf.util.Timestamps;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TableRootWriterTest {

  private TableRootRepository roots;
  private TableRootCommitter committer;
  private StatsStore statsStore;
  private ConstraintRepository constraintRepo;
  private TableRepository tableRepo;
  private SnapshotRepository snapshotRepo;
  private TableRootWriter writer;
  private ResourceId tableId;

  @BeforeEach
  void setUp() {
    roots = new TableRootRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    committer = new TableRootCommitter(roots);
    statsStore = mock(StatsStore.class);
    when(statsStore.tracksStatsGenerations()).thenReturn(true);
    when(statsStore.activeStatsGeneration(any(), anyLong())).thenReturn(Optional.empty());
    constraintRepo = mock(ConstraintRepository.class);
    when(constraintRepo.metaForSafe(any(), anyLong()))
        .thenReturn(MutationMeta.getDefaultInstance());
    tableRepo = mock(TableRepository.class);
    when(tableRepo.metaForSafe(any())).thenReturn(MutationMeta.getDefaultInstance());
    snapshotRepo = mock(SnapshotRepository.class);
    when(snapshotRepo.latestRegisteredSnapshotPointer(any())).thenReturn(Optional.empty());
    writer =
        new TableRootWriter(roots, committer, tableRepo, snapshotRepo, constraintRepo, statsStore);

    tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("tbl")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    committer.commit(
        tableId,
        TableRootMutations.upsertSnapshot(
            roots,
            tableId,
            SnapshotManifestEntry.newBuilder()
                .setSnapshotId(7)
                .setSnapshotRef(BlobRef.newBuilder().setUri("s3://t/snap-7.pb").setVersion("v7"))
                .setUpstreamCreatedAt(Timestamps.fromMillis(7_000))
                .build(),
            null,
            true));
  }

  private SnapshotManifestEntry entry(long snapshotId) {
    var root = roots.get(tableId).orElseThrow();
    return SnapshotManifests.findEntry(roots, root.getSnapshotManifestRef(), snapshotId)
        .orElseThrow();
  }

  private CurrentSnapshotPointer pointer(long snapshotId) {
    return CurrentSnapshotPointer.newBuilder()
        .setTableId(tableId)
        .setSnapshotId(snapshotId)
        .build();
  }

  private MutationMeta snapMeta(long id) {
    return MutationMeta.newBuilder()
        .setBlobUri("s3://t/snap-" + id + ".pb")
        .setEtag("v" + id)
        .build();
  }

  private MutationMeta tableMeta() {
    return MutationMeta.newBuilder().setBlobUri("s3://t/table.pb").setEtag("etag-t").build();
  }

  @Test
  void commitStatsGenerationRecordsTheActiveGenerationOnTheEntry() {
    when(statsStore.activeStatsGeneration(tableId, 7L))
        .thenReturn(Optional.of("s3://t/stats/7/gen-1.pb"));

    writer.commitStatsGeneration(tableId, 7L);

    assertEquals("s3://t/stats/7/gen-1.pb", entry(7).getStatsGenerationRef().getUri());
  }

  @Test
  void commitSnapshotCaptureRegistersAndPublishesInOneRootCommit() {
    Snapshot candidate =
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(8L)
            .setUpstreamCreatedAt(Timestamps.fromMillis(8_000L))
            .build();
    BlobRef manifestRef =
        BlobRef.newBuilder().setUri("s3://t/capture/8.pb").setVersion("sha256-8").build();
    when(snapshotRepo.metaForSafe(tableId, 8L)).thenReturn(snapMeta(8L));
    when(tableRepo.metaForSafe(tableId)).thenReturn(tableMeta());
    when(snapshotRepo.latestRegisteredSnapshotPointer(tableId))
        .thenReturn(Optional.of(pointer(8L)));
    long versionBefore = roots.metaForSafe(tableId).getPointerVersion();

    writer.commitSnapshotCapture(tableId, candidate, manifestRef);

    var root = roots.get(tableId).orElseThrow();
    assertEquals(versionBefore + 1L, roots.metaForSafe(tableId).getPointerVersion());
    assertEquals(8L, root.getCurrentSnapshotId());
    assertEquals("s3://t/snap-8.pb", entry(8L).getSnapshotRef().getUri());
    assertEquals(manifestRef, entry(8L).getStatsGenerationRef());
  }

  @Test
  void commitStatsGenerationClearsTheRefWhenTheGenerationIsGone() {
    when(statsStore.activeStatsGeneration(tableId, 7L))
        .thenReturn(Optional.of("s3://t/stats/7/gen-1.pb"));
    writer.commitStatsGeneration(tableId, 7L);

    when(statsStore.activeStatsGeneration(tableId, 7L)).thenReturn(Optional.empty());
    writer.commitStatsGeneration(tableId, 7L);

    assertFalse(entry(7).hasStatsGenerationRef());
  }

  @Test
  void commitStatsGenerationSkipsStoresThatCannotNameGenerations() {
    // Empty from a non-tracking store means "cannot say", never "none" — the root must not
    // clear a ref on that answer.
    when(statsStore.tracksStatsGenerations()).thenReturn(false);
    long versionBefore = roots.metaForSafe(tableId).getPointerVersion();

    writer.commitStatsGeneration(tableId, 7L);

    assertEquals(versionBefore, roots.metaForSafe(tableId).getPointerVersion());
  }

  @Test
  void commitConstraintsRecordsAndClearsTheBundleRef() {
    when(constraintRepo.metaForSafe(tableId, 7L))
        .thenReturn(
            MutationMeta.newBuilder()
                .setBlobUri("s3://t/constraints/7.pb")
                .setEtag("etag-c7")
                .build());
    writer.commitConstraints(tableId, 7L);
    assertEquals("s3://t/constraints/7.pb", entry(7).getConstraintsRef().getUri());
    assertEquals("etag-c7", entry(7).getConstraintsRef().getVersion());

    // A constraints delete leaves no blob behind; the recommit clears the ref.
    when(constraintRepo.metaForSafe(tableId, 7L)).thenReturn(MutationMeta.getDefaultInstance());
    writer.commitConstraints(tableId, 7L);
    assertFalse(entry(7).hasConstraintsRef());
  }

  @Test
  void auxCommitsForSnapshotsUnknownToTheManifestAreNoOps() {
    when(statsStore.activeStatsGeneration(tableId, 99L))
        .thenReturn(Optional.of("s3://t/stats/99/gen-1.pb"));
    long versionBefore = roots.metaForSafe(tableId).getPointerVersion();

    writer.commitStatsGeneration(tableId, 99L);
    writer.commitConstraints(tableId, 99L);

    assertEquals(versionBefore, roots.metaForSafe(tableId).getPointerVersion());
  }

  @Test
  void resyncForcesCurrencyToTheCommittedPointerEvenBackward() {
    // Root currency is 7 (advance rule). A transactional commit moves the committed pointer to an
    // OLDER — but finalized — snapshot: the resync must follow it, not re-apply the advance rule.
    // (An unfinalized target would be gated; see the gated-resync mutation test.)
    committer.commit(
        tableId, TableRootMutations.upsertSnapshot(roots, tableId, finalizedEntry(3), null, true));
    when(tableRepo.metaForSafe(tableId)).thenReturn(tableMeta());
    when(snapshotRepo.latestRegisteredSnapshotPointer(tableId)).thenReturn(Optional.of(pointer(3)));
    when(snapshotRepo.metaForSafe(tableId, 3L)).thenReturn(snapMeta(3));
    when(snapshotRepo.getById(tableId, 3L)).thenReturn(Optional.empty());

    writer.resyncFromCommittedState(tableId);

    var root = roots.get(tableId).orElseThrow();
    assertEquals(3L, root.getCurrentSnapshotId());
    assertEquals("s3://t/table.pb", root.getDefinitionRef().getUri());
  }

  @Test
  void resyncPrunesSnapshotsNoLongerRegistered() {
    // Root holds entries 3 and 7. A transaction expired snapshot 3 (cleared its pointers by raw
    // CAS, never through removeSnapshot); only 7 is still registered. The resync must drop 3.
    committer.commit(
        tableId, TableRootMutations.upsertSnapshot(roots, tableId, finalizedEntry(3), null, true));
    committer.commit(
        tableId, TableRootMutations.upsertSnapshot(roots, tableId, finalizedEntry(7), null, true));

    when(tableRepo.metaForSafe(tableId)).thenReturn(tableMeta());
    when(snapshotRepo.latestRegisteredSnapshotPointer(tableId)).thenReturn(Optional.of(pointer(7)));
    when(snapshotRepo.metaForSafe(tableId, 7L)).thenReturn(snapMeta(7));
    when(snapshotRepo.getById(tableId, 7L)).thenReturn(Optional.empty());
    when(snapshotRepo.list(eq(tableId), anyInt(), any(), any()))
        .thenReturn(java.util.List.of(Snapshot.newBuilder().setSnapshotId(7).build()));

    writer.resyncFromCommittedState(tableId);

    var root = roots.get(tableId).orElseThrow();
    assertFalse(
        SnapshotManifests.findEntry(roots, root.getSnapshotManifestRef(), 3).isPresent(),
        "expired snapshot 3 pruned from the root");
    assertTrue(SnapshotManifests.findEntry(roots, root.getSnapshotManifestRef(), 7).isPresent());
    assertEquals(7L, root.getCurrentSnapshotId());
  }

  @Test
  void resyncEntryCarriesTheActiveStatsGenerationAndConstraintsRefs() {
    // A transactional resync builds a FRESH manifest entry (no prior entry for preserveAuxRefs to
    // copy from). A finalized + constrained snapshot must land WITH both refs — otherwise it is
    // gated-invisible (no stats generation) and its constraints are dropped.
    when(tableRepo.metaForSafe(tableId)).thenReturn(tableMeta());
    when(snapshotRepo.latestRegisteredSnapshotPointer(tableId)).thenReturn(Optional.of(pointer(7)));
    when(snapshotRepo.metaForSafe(tableId, 7L)).thenReturn(snapMeta(7));
    when(snapshotRepo.getById(eq(tableId), anyLong())).thenReturn(Optional.empty());
    when(snapshotRepo.list(eq(tableId), anyInt(), any(), any()))
        .thenReturn(java.util.List.of(Snapshot.newBuilder().setSnapshotId(7).build()));
    when(statsStore.activeStatsGeneration(tableId, 7L))
        .thenReturn(Optional.of("s3://t/stats/7/gen.pb"));
    when(constraintRepo.metaForSafe(tableId, 7L))
        .thenReturn(
            MutationMeta.newBuilder()
                .setBlobUri("s3://t/constraints/7.pb")
                .setEtag("etag-c7")
                .build());

    writer.resyncFromCommittedState(tableId);

    var entry =
        SnapshotManifests.findEntry(
                roots, roots.get(tableId).orElseThrow().getSnapshotManifestRef(), 7)
            .orElseThrow();
    assertEquals("s3://t/stats/7/gen.pb", entry.getStatsGenerationRef().getUri());
    assertEquals("s3://t/constraints/7.pb", entry.getConstraintsRef().getUri());
  }

  @Test
  void resyncStaysUnconvergedWhenALiveSnapshotBlobIsUnresolvable() {
    // 7 is the committed current (resolvable); 9 is a registered non-current snapshot whose blob is
    // not yet resolvable. Membership is incomplete, so resync must NOT report convergence — the
    // re-drive marker has to survive so a later pass registers 9 (a transaction-only table has no
    // other writer to converge its root).
    when(tableRepo.metaForSafe(tableId)).thenReturn(tableMeta());
    when(snapshotRepo.latestRegisteredSnapshotPointer(tableId)).thenReturn(Optional.of(pointer(7)));
    when(snapshotRepo.metaForSafe(tableId, 7L)).thenReturn(snapMeta(7));
    when(snapshotRepo.metaForSafe(tableId, 9L)).thenReturn(MutationMeta.getDefaultInstance());
    when(snapshotRepo.getById(eq(tableId), anyLong())).thenReturn(Optional.empty());
    when(snapshotRepo.list(eq(tableId), anyInt(), any(), any()))
        .thenReturn(
            java.util.List.of(
                Snapshot.newBuilder().setSnapshotId(7).build(),
                Snapshot.newBuilder().setSnapshotId(9).build()));

    boolean converged = writer.resyncFromCommittedState(tableId);

    assertFalse(converged, "an unresolvable live snapshot leaves the resync unconverged");
    // Progress still lands: the resolvable current (7) is registered even though 9 was skipped.
    assertTrue(
        SnapshotManifests.findEntry(
                roots, roots.get(tableId).orElseThrow().getSnapshotManifestRef(), 7)
            .isPresent());
  }

  @Test
  void resyncDeletesTheRootWhenTheTableIsGone() {
    when(tableRepo.metaForSafe(tableId)).thenReturn(MutationMeta.getDefaultInstance());

    writer.resyncFromCommittedState(tableId);

    assertTrue(roots.get(tableId).isEmpty());
    assertEquals(0L, roots.metaForSafe(tableId).getPointerVersion());
  }

  @Test
  void resyncLeavesTheRootUntouchedWhenTheCommittedSnapshotHasNoBlob() {
    long versionBefore = roots.metaForSafe(tableId).getPointerVersion();
    when(tableRepo.metaForSafe(tableId)).thenReturn(tableMeta());
    when(snapshotRepo.latestRegisteredSnapshotPointer(tableId))
        .thenReturn(Optional.of(pointer(99)));
    when(snapshotRepo.metaForSafe(tableId, 99L)).thenReturn(MutationMeta.getDefaultInstance());

    writer.resyncFromCommittedState(tableId);

    assertEquals(versionBefore, roots.metaForSafe(tableId).getPointerVersion());
  }

  /** A root repository that loses {@code updateFailures[0]} CAS attempts before succeeding. */
  private TableRootRepository contendedRoots(
      InMemoryPointerStore ptr, InMemoryBlobStore blobs, int[] updateFailures) {
    return new TableRootRepository(ptr, blobs) {
      @Override
      public boolean update(ai.floedb.floecat.catalog.rpc.TableRoot desired, long expectedVersion) {
        if (updateFailures[0] > 0) {
          updateFailures[0]--;
          return false;
        }
        return super.update(desired, expectedVersion);
      }
    };
  }

  private static SnapshotManifestEntry finalizedEntry(long snapshotId) {
    return SnapshotManifestEntry.newBuilder()
        .setSnapshotId(snapshotId)
        .setSnapshotRef(
            BlobRef.newBuilder()
                .setUri("s3://t/snap-" + snapshotId + ".pb")
                .setVersion("v" + snapshotId))
        .setStatsGenerationRef(
            BlobRef.newBuilder().setUri("s3://t/stats/" + snapshotId + "/gen.pb"))
        .setUpstreamCreatedAt(Timestamps.fromMillis(snapshotId * 1_000))
        .build();
  }

  @Test
  void aLostResyncCasReReadsTheCommittedStateInsteadOfReplayingItsCapture() {
    // Resync FORCES currency, so a retry that replays state captured before the winner's commit
    // would resurrect it: stale currency, or a manifest entry for a snapshot deleted concurrently.
    // The mutator must re-read the committed pointer families on every attempt. Here the pointer
    // says snapshot 5 (never in the manifest) when the first attempt runs, and snapshot 7 by the
    // time the retry runs — only 7 may land.
    var ptr = new InMemoryPointerStore();
    var blobStore = new InMemoryBlobStore();
    int[] updateFailures = {0};
    var cRoots = contendedRoots(ptr, blobStore, updateFailures);
    var cCommitter = new TableRootCommitter(cRoots);
    var cWriter =
        new TableRootWriter(
            cRoots, cCommitter, tableRepo, snapshotRepo, constraintRepo, statsStore);
    cCommitter.commit(
        tableId, TableRootMutations.upsertSnapshot(cRoots, tableId, finalizedEntry(7), null, true));
    updateFailures[0] = 1;

    when(tableRepo.metaForSafe(tableId)).thenReturn(tableMeta());
    when(snapshotRepo.latestRegisteredSnapshotPointer(tableId))
        .thenReturn(Optional.of(pointer(5)), Optional.of(pointer(7)));
    when(snapshotRepo.metaForSafe(tableId, 5L)).thenReturn(snapMeta(5));
    when(snapshotRepo.metaForSafe(tableId, 7L)).thenReturn(snapMeta(7));
    when(snapshotRepo.getById(any(), anyLong())).thenReturn(Optional.empty());

    cWriter.resyncFromCommittedState(tableId);

    var root = cRoots.get(tableId).orElseThrow();
    assertEquals(7L, root.getCurrentSnapshotId(), "currency follows the state read at retry time");
    assertTrue(
        SnapshotManifests.findEntry(cRoots, root.getSnapshotManifestRef(), 5L).isEmpty(),
        "the first attempt's captured entry must not be replayed into the winner's manifest");
  }

  @Test
  void aLostStatsGenerationCasConvergesOnTheLastPublishedGeneration() {
    // The active-generation read happens INSIDE the mutator: a ref captured before a lost CAS
    // must not land last and leave the root referencing a superseded generation.
    var ptr = new InMemoryPointerStore();
    var blobStore = new InMemoryBlobStore();
    int[] updateFailures = {0};
    var cRoots = contendedRoots(ptr, blobStore, updateFailures);
    var cCommitter = new TableRootCommitter(cRoots);
    var cWriter =
        new TableRootWriter(
            cRoots, cCommitter, tableRepo, snapshotRepo, constraintRepo, statsStore);
    cCommitter.commit(
        tableId, TableRootMutations.upsertSnapshot(cRoots, tableId, finalizedEntry(7), null, true));
    updateFailures[0] = 1;

    when(statsStore.activeStatsGeneration(tableId, 7L))
        .thenReturn(Optional.of("s3://t/stats/7/gen-1.pb"), Optional.of("s3://t/stats/7/gen-2.pb"));

    cWriter.commitStatsGeneration(tableId, 7L);

    var root = cRoots.get(tableId).orElseThrow();
    assertEquals(
        "s3://t/stats/7/gen-2.pb",
        SnapshotManifests.findEntry(cRoots, root.getSnapshotManifestRef(), 7L)
            .orElseThrow()
            .getStatsGenerationRef()
            .getUri(),
        "the committed ref reflects the generation active when the winning attempt ran");
  }

  @Test
  void aDropRacingTheResyncDoesNotLeaveADefinitionlessRoot() {
    // The committer persists synthesized history even on a mutator no-op. A drop landing between
    // the resync's probe and its commit could therefore persist a definition-less root built from
    // lingering snapshot pointers mid-drop-cleanup; the post-commit re-probe must delete it.
    var ptr = new InMemoryPointerStore();
    var blobStore = new InMemoryBlobStore();
    var freshRoots = new TableRootRepository(ptr, blobStore);
    TableRootSynthesizer synthesizer = mock(TableRootSynthesizer.class);
    when(synthesizer.synthesize(tableId))
        .thenReturn(
            Optional.of(
                ai.floedb.floecat.catalog.rpc.TableRoot.newBuilder().setTableId(tableId).build()));
    var freshCommitter = new TableRootCommitter(freshRoots, synthesizer);
    var freshWriter =
        new TableRootWriter(
            freshRoots, freshCommitter, tableRepo, snapshotRepo, constraintRepo, statsStore);
    // Probe sees the definition; by the time the mutator re-reads, the drop has landed.
    when(tableRepo.metaForSafe(tableId))
        .thenReturn(
            MutationMeta.newBuilder().setBlobUri("s3://t/table.pb").setEtag("e").build(),
            MutationMeta.getDefaultInstance());

    boolean converged = freshWriter.resyncFromCommittedState(tableId);

    assertTrue(converged, "the drop path converges by deletion");
    assertTrue(
        freshRoots.get(tableId).isEmpty(), "no definition-less root may survive the drop race");
  }

  @Test
  void resyncReportsNonConvergenceWhenTheRootCannotBeDeleted() {
    // deleteRoot gives up after bounded CAS attempts; that is NOT convergence — the re-drive
    // marker must survive, so the result has to be false.
    var ptr = new InMemoryPointerStore();
    var blobStore = new InMemoryBlobStore();
    var stubbornRoots =
        new TableRootRepository(ptr, blobStore) {
          @Override
          public boolean deleteWithPrecondition(ResourceId id, long expectedVersion) {
            return false;
          }
        };
    var stubbornCommitter = new TableRootCommitter(stubbornRoots);
    var stubbornWriter =
        new TableRootWriter(
            stubbornRoots, stubbornCommitter, tableRepo, snapshotRepo, constraintRepo, statsStore);
    stubbornCommitter.commit(
        tableId,
        TableRootMutations.upsertSnapshot(stubbornRoots, tableId, finalizedEntry(7), null, true));
    when(tableRepo.metaForSafe(tableId)).thenReturn(MutationMeta.getDefaultInstance());

    assertFalse(stubbornWriter.resyncFromCommittedState(tableId));
  }

  @Test
  void anUnresolvableCommittedSnapshotIsNotConvergence() {
    when(tableRepo.metaForSafe(tableId))
        .thenReturn(MutationMeta.newBuilder().setBlobUri("s3://t/table.pb").setEtag("e").build());
    when(snapshotRepo.latestRegisteredSnapshotPointer(tableId))
        .thenReturn(Optional.of(pointer(99)));
    when(snapshotRepo.metaForSafe(tableId, 99L)).thenReturn(MutationMeta.getDefaultInstance());

    assertFalse(
        writer.resyncFromCommittedState(tableId),
        "half-created committed state must keep the re-drive marker alive");
  }

  @Test
  void commitFailuresPropagateAndFailTheCallingWrite() {
    // Reads resolve through the root, so the root commit IS the publication: a failure must fail
    // the write before it is acknowledged, never be silently absorbed.
    when(statsStore.activeStatsGeneration(tableId, 7L))
        .thenThrow(new IllegalStateException("store down"));

    org.junit.jupiter.api.Assertions.assertThrows(
        IllegalStateException.class, () -> writer.commitStatsGeneration(tableId, 7L));
    assertFalse(entry(7).hasStatsGenerationRef());
  }

  @Test
  void resyncFailuresAreAbsorbedBecauseTheTransactionIsAlreadyDurable() {
    when(tableRepo.metaForSafe(tableId)).thenThrow(new IllegalStateException("store down"));

    writer.resyncFromCommittedState(tableId); // must not propagate

    assertTrue(roots.get(tableId).isPresent());
  }
}
