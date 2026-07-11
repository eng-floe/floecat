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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.BlobRef;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotManifestEntry;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableRoot;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Backward compatibility: a deployment with pre-existing (legacy pointer family) data migrates
 * lazily — synthesis reconstructs a table's full history, and the first root commit persists that
 * history together with the new mutation.
 */
class TableRootSynthesizerTest {

  private static final String ACCOUNT = "acct";

  private TableRepository tableRepo;
  private SnapshotRepository snapshotRepo;
  private StatsStore statsRepo;
  private ConstraintRepository constraintRepo;
  private TableRootRepository roots;
  private ai.floedb.floecat.storage.memory.InMemoryPointerStore pointers;
  private ai.floedb.floecat.storage.memory.InMemoryBlobStore blobs;
  private TableRootSynthesizer synthesizer;
  private TableRootCommitter committer;
  private ResourceId tableId;

  @BeforeEach
  void setUp() {
    pointers = new InMemoryPointerStore();
    blobs = new InMemoryBlobStore();
    tableRepo = new TableRepository(pointers, blobs);
    snapshotRepo = new SnapshotRepository(pointers, blobs, tableRepo);
    statsRepo = mock(StatsStore.class);
    when(statsRepo.tracksStatsGenerations()).thenReturn(true);
    when(statsRepo.activeStatsGeneration(any(), anyLong())).thenReturn(Optional.empty());
    constraintRepo = mock(ConstraintRepository.class);
    when(constraintRepo.metaForSafe(any(), anyLong()))
        .thenReturn(MutationMeta.getDefaultInstance());
    roots = new TableRootRepository(pointers, blobs);
    synthesizer =
        new TableRootSynthesizer(tableRepo, snapshotRepo, statsRepo, constraintRepo, roots);
    committer = new TableRootCommitter(roots, synthesizer);

    tableId =
        ResourceId.newBuilder()
            .setAccountId(ACCOUNT)
            .setId("legacy-tbl")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    tableRepo.create(
        Table.newBuilder()
            .setResourceId(tableId)
            .setDisplayName("legacy")
            .setCatalogId(
                ResourceId.newBuilder()
                    .setAccountId(ACCOUNT)
                    .setId("cat")
                    .setKind(ResourceKind.RK_CATALOG))
            .setNamespaceId(
                ResourceId.newBuilder()
                    .setAccountId(ACCOUNT)
                    .setId("ns")
                    .setKind(ResourceKind.RK_NAMESPACE))
            .build());
  }

  private void advanceCurrentTo(long id) {
    snapshotRepo.maybeAdvanceCurrentSnapshotPointer(
        tableId, snapshotRepo.getById(tableId, id).orElseThrow());
  }

  private void legacySnapshot(long id, long upstreamMs) {
    snapshotRepo.create(
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(id)
            .setIngestedAt(Timestamps.fromMillis(upstreamMs))
            .setUpstreamCreatedAt(Timestamps.fromMillis(upstreamMs))
            .build());
  }

  @Test
  void synthesizesTheFullLegacyHistoryWithCurrencyAndAuxRefs() {
    legacySnapshot(1, 1_000);
    legacySnapshot(2, 2_000);
    legacySnapshot(3, 3_000);
    advanceCurrentTo(2);
    when(statsRepo.activeStatsGeneration(tableId, 1L))
        .thenReturn(Optional.of("s3://legacy/gen-1.pb"));
    when(statsRepo.activeStatsGeneration(tableId, 2L))
        .thenReturn(Optional.of("s3://legacy/gen-2.pb"));
    when(constraintRepo.metaForSafe(tableId, 3L))
        .thenReturn(
            MutationMeta.newBuilder()
                .setBlobUri("s3://legacy/constraints-3.pb")
                .setEtag("etag-c3")
                .build());

    TableRoot synthesized = synthesizer.synthesize(tableId).orElseThrow();

    assertEquals(2L, synthesized.getCurrentSnapshotId(), "currency follows the legacy pointer");
    assertFalse(synthesized.getDefinitionRef().getUri().isEmpty(), "definition captured");
    List<Long> ids = new ArrayList<>();
    SnapshotManifests.forEachEntry(
        roots, synthesized.getSnapshotManifestRef(), e -> ids.add(e.getSnapshotId()));
    assertEquals(3, ids.size(), "every legacy snapshot is represented");
    SnapshotManifestEntry e1 =
        SnapshotManifests.findEntry(roots, synthesized.getSnapshotManifestRef(), 1).orElseThrow();
    assertEquals("s3://legacy/gen-1.pb", e1.getStatsGenerationRef().getUri());
    SnapshotManifestEntry e3 =
        SnapshotManifests.findEntry(roots, synthesized.getSnapshotManifestRef(), 3).orElseThrow();
    assertEquals("s3://legacy/constraints-3.pb", e3.getConstraintsRef().getUri());
  }

  @Test
  void firstCommitOnALegacyTablePersistsHistoryPlusTheMutation() {
    legacySnapshot(1, 1_000);
    legacySnapshot(2, 2_000);
    advanceCurrentTo(2);

    // The first-ever root commit: a brand-new snapshot arrives on this legacy table.
    SnapshotManifestEntry incoming =
        SnapshotManifestEntry.newBuilder()
            .setSnapshotId(3)
            .setSnapshotRef(BlobRef.newBuilder().setUri("s3://t/snap-3.pb").setVersion("v3"))
            .setUpstreamCreatedAt(Timestamps.fromMillis(3_000))
            .build();
    TableRoot committed =
        committer
            .commit(
                tableId, TableRootMutations.upsertSnapshot(roots, tableId, incoming, null, true))
            .orElseThrow();

    assertEquals(1L, committed.getRootSeq());
    assertEquals(3L, committed.getCurrentSnapshotId(), "advance rule ran over legacy currency");
    List<Long> ids = new ArrayList<>();
    SnapshotManifests.forEachEntry(
        roots, committed.getSnapshotManifestRef(), e -> ids.add(e.getSnapshotId()));
    assertEquals(3, ids.size(), "legacy history survived the first commit");
    assertTrue(ids.containsAll(List.of(1L, 2L, 3L)));
  }

  @Test
  void ensureRootMaterializesOnceAndIsANoOpAfterwards() {
    legacySnapshot(1, 1_000);
    when(statsRepo.activeStatsGeneration(tableId, 1L))
        .thenReturn(Optional.of("s3://legacy/gen-1.pb"));
    advanceCurrentTo(1);

    TableRoot materialized = committer.ensureRoot(tableId).orElseThrow();
    assertEquals(1L, materialized.getRootSeq());
    assertEquals(1L, materialized.getCurrentSnapshotId());
    long version = roots.metaForSafe(tableId).getPointerVersion();

    // Idempotent: the stored root is returned untouched.
    TableRoot again = committer.ensureRoot(tableId).orElseThrow();
    assertEquals(materialized, again);
    assertEquals(version, roots.metaForSafe(tableId).getPointerVersion());
  }

  @Test
  void anUnfinalizedLegacyCurrentIsGatedUntilItsGenerationPublishes() {
    // The visibility gate applies to migration too — decisively, the FIRST funnel commit on a
    // brand-new table synthesizes its root right after the legacy pointer advanced; importing
    // unfinalized currency would let registration bypass the gate.
    legacySnapshot(1, 1_000);
    advanceCurrentTo(1);

    TableRoot synthesized = synthesizer.synthesize(tableId).orElseThrow();

    assertFalse(synthesized.hasCurrentSnapshotId());
    assertTrue(
        SnapshotManifests.findEntry(roots, synthesized.getSnapshotManifestRef(), 1).isPresent());
  }

  @Test
  void unknownTableYieldsNoRoot() {
    var ghost =
        ResourceId.newBuilder()
            .setAccountId(ACCOUNT)
            .setId("ghost")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    assertTrue(committer.ensureRoot(ghost).isEmpty());
    assertEquals(0L, roots.metaForSafe(ghost).getPointerVersion());
  }

  private void seedDanglingCurrentPointer(long snapshotId) {
    new ai.floedb.floecat.service.repo.impl.CurrentSnapshotPointerRepository(pointers, blobs)
        .createIfAbsent(
            ai.floedb.floecat.catalog.rpc.CurrentSnapshotPointer.newBuilder()
                .setTableId(tableId)
                .setSnapshotId(snapshotId)
                .setUpdatedAt(Timestamps.fromMillis(1_000))
                .build());
  }

  @Test
  void aDanglingLegacyPointerNeverBecomesCurrencyNothingCanResolve() {
    // The pointer targets a snapshot whose blob is gone: importing it as currency would make
    // CURRENT pins fail on an unresolvable manifest entry. Gated stores leave currency empty.
    legacySnapshot(1, 1_000);
    seedDanglingCurrentPointer(99);

    TableRoot synthesized = synthesizer.synthesize(tableId).orElseThrow();

    assertFalse(synthesized.hasCurrentSnapshotId());
    assertTrue(
        SnapshotManifests.findEntry(roots, synthesized.getSnapshotManifestRef(), 1).isPresent());
  }

  @Test
  void aNonTrackingStoreFallsBackToTheNewestEntryWhenThePointerDangles() {
    // Stores that cannot gate mirrored the legacy latest-by-time fallback: the advance rule's
    // pick among the manifest entries becomes currency instead of nothing.
    when(statsRepo.tracksStatsGenerations()).thenReturn(false);
    legacySnapshot(1, 1_000);
    legacySnapshot(2, 2_000);
    seedDanglingCurrentPointer(99);

    TableRoot synthesized = synthesizer.synthesize(tableId).orElseThrow();

    assertEquals(2L, synthesized.getCurrentSnapshotId());
  }

  @Test
  void theSynthesizedHeadPageHoldsTheNewestSnapshotsFirst() {
    // The manifest invariant is newest-entries-first: current and recent-AS_OF reads touch one
    // page. A fold that prepended in listByTime order (newest first) inverted the chain for
    // migrated tables, pushing the newest snapshots to the deepest page.
    legacySnapshot(1, 1_000);
    legacySnapshot(2, 2_000);
    legacySnapshot(3, 3_000);

    TableRoot synthesized = synthesizer.synthesize(tableId).orElseThrow();

    var headPage = roots.getManifestPage(synthesized.getSnapshotManifestRef()).orElseThrow();
    assertEquals(3L, headPage.getEntries(0).getSnapshotId());
    assertEquals(2L, headPage.getEntries(1).getSnapshotId());
    assertEquals(1L, headPage.getEntries(2).getSnapshotId());
  }
}
