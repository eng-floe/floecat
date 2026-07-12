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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.BlobRef;
import ai.floedb.floecat.catalog.rpc.SnapshotManifestEntry;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SnapshotManifestsTest {

  private static final ResourceId TABLE =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("tbl")
          .setKind(ResourceKind.RK_TABLE)
          .build();

  private TableRootRepository roots;

  @BeforeEach
  void setUp() {
    roots = new TableRootRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
  }

  private static SnapshotManifestEntry entry(long snapshotId, String snapUri) {
    return SnapshotManifestEntry.newBuilder()
        .setSnapshotId(snapshotId)
        .setSnapshotRef(BlobRef.newBuilder().setUri(snapUri).setVersion("v-" + snapUri))
        .build();
  }

  @Test
  void upsertIntoEmptyChainCreatesTheFirstPage() {
    BlobRef head = SnapshotManifests.upsert(roots, TABLE, null, entry(7, "s3://t/snap-7.pb"));

    var found = SnapshotManifests.findEntry(roots, head, 7).orElseThrow();
    assertEquals("s3://t/snap-7.pb", found.getSnapshotRef().getUri());
    // Content-addressed: re-writing identical content converges on the identical ref.
    BlobRef again = SnapshotManifests.upsert(roots, TABLE, null, entry(7, "s3://t/snap-7.pb"));
    assertEquals(head, again);
  }

  @Test
  void upsertPrependsNewestFirstAndSnapshotIdZeroIsAValidId() {
    BlobRef head = SnapshotManifests.upsert(roots, TABLE, null, entry(0, "s3://t/snap-0.pb"));
    head = SnapshotManifests.upsert(roots, TABLE, head, entry(1, "s3://t/snap-1.pb"));

    List<Long> order = new ArrayList<>();
    SnapshotManifests.forEachEntry(roots, head, e -> order.add(e.getSnapshotId()));
    assertEquals(List.of(1L, 0L), order);
    assertTrue(SnapshotManifests.findEntry(roots, head, 0).isPresent());
  }

  @Test
  void upsertReplacesAnExistingEntryInPlace() {
    BlobRef head = SnapshotManifests.upsert(roots, TABLE, null, entry(1, "s3://t/snap-1.pb"));
    head = SnapshotManifests.upsert(roots, TABLE, head, entry(2, "s3://t/snap-2.pb"));

    head = SnapshotManifests.upsert(roots, TABLE, head, entry(1, "s3://t/snap-1-v2.pb"));

    assertEquals(
        "s3://t/snap-1-v2.pb",
        SnapshotManifests.findEntry(roots, head, 1).orElseThrow().getSnapshotRef().getUri());
    // Order and the sibling entry are untouched.
    List<Long> order = new ArrayList<>();
    SnapshotManifests.forEachEntry(roots, head, e -> order.add(e.getSnapshotId()));
    assertEquals(List.of(2L, 1L), order);
  }

  @Test
  void fullHeadSpillsIntoTheChainAndDeepReplaceRechainsNewerPages() {
    // Fill past the page bound so the chain has (at least) two pages.
    BlobRef head = null;
    int total = SnapshotManifests.PAGE_ENTRY_BOUND + 3;
    for (long id = 1; id <= total; id++) {
      head = SnapshotManifests.upsert(roots, TABLE, head, entry(id, "s3://t/snap-" + id + ".pb"));
    }
    // Newest entries sit on the small head page; entry 1 is deepest in the old page.
    assertTrue(SnapshotManifests.findEntry(roots, head, 1).isPresent());
    assertTrue(SnapshotManifests.findEntry(roots, head, total).isPresent());

    // Replace the deepest entry: the old page rewrites and the newer page re-chains onto it.
    head = SnapshotManifests.upsert(roots, TABLE, head, entry(1, "s3://t/snap-1-v2.pb"));

    assertEquals(
        "s3://t/snap-1-v2.pb",
        SnapshotManifests.findEntry(roots, head, 1).orElseThrow().getSnapshotRef().getUri());
    List<Long> all = new ArrayList<>();
    SnapshotManifests.forEachEntry(roots, head, e -> all.add(e.getSnapshotId()));
    assertEquals(total, all.size(), "no entry lost across the re-chain");
    assertEquals((long) total, all.get(0), "newest still first");
  }

  @Test
  void removeDropsTheEntryAndCollapsesAnEmptiedPage() {
    BlobRef head = SnapshotManifests.upsert(roots, TABLE, null, entry(1, "s3://t/snap-1.pb"));
    head = SnapshotManifests.upsert(roots, TABLE, head, entry(2, "s3://t/snap-2.pb"));

    head = SnapshotManifests.remove(roots, TABLE, head, 1);

    assertTrue(SnapshotManifests.findEntry(roots, head, 1).isEmpty());
    assertTrue(SnapshotManifests.findEntry(roots, head, 2).isPresent());

    // Removing the last entry empties the chain entirely.
    assertNull(SnapshotManifests.remove(roots, TABLE, head, 2));
  }

  @Test
  void removeOfAnAbsentIdReturnsTheHeadUnchanged() {
    BlobRef head = SnapshotManifests.upsert(roots, TABLE, null, entry(1, "s3://t/snap-1.pb"));
    assertEquals(head, SnapshotManifests.remove(roots, TABLE, head, 99));
  }

  @Test
  void aMissingMidChainPageFailsReadsClosedLikeWrites() {
    // Treating a vanished mid-chain page as end-of-chain would let a finalize silently no-op
    // ("snapshot unknown") and AS_OF resolve too new; reads must fail loud like the writes do.
    var pointers = new ai.floedb.floecat.storage.memory.InMemoryPointerStore();
    var blobStore = new ai.floedb.floecat.storage.memory.InMemoryBlobStore();
    var localRoots = new TableRootRepository(pointers, blobStore);
    BlobRef head = null;
    for (long id = 0; id <= SnapshotManifests.PAGE_ENTRY_BOUND; id++) {
      head = SnapshotManifests.upsert(localRoots, TABLE, head, entry(id, "s3://t/snap-" + id));
    }
    BlobRef spilledHead = head;
    var headPage = localRoots.getManifestPage(spilledHead).orElseThrow();
    blobStore.delete(headPage.getPrevPageRef().getUri());

    // The head page itself still serves without touching the broken elder link.
    assertTrue(
        SnapshotManifests.findEntry(localRoots, spilledHead, SnapshotManifests.PAGE_ENTRY_BOUND)
            .isPresent());
    assertThrows(
        ai.floedb.floecat.service.repo.util.BaseResourceRepository.CorruptionException.class,
        () -> SnapshotManifests.findEntry(localRoots, spilledHead, 0L));
    assertThrows(
        ai.floedb.floecat.service.repo.util.BaseResourceRepository.CorruptionException.class,
        () -> SnapshotManifests.forEachEntry(localRoots, spilledHead, e -> {}));
  }

  @Test
  void aFoldOfPrependsReadsNoPagesFromTheStore() {
    // The synthesizer folds N legacy snapshots into a chain. With withHead sharing the
    // content-addressed cache and prepend skipping the existing-id walk, every page the fold
    // touches was written (and cached) by the fold itself: ZERO store page reads — O(N), where
    // per-entry one-shot upserts were O(N^2).
    var pointers = new ai.floedb.floecat.storage.memory.InMemoryPointerStore();
    var blobStore = new ai.floedb.floecat.storage.memory.InMemoryBlobStore();
    int[] pageReads = {0};
    var countingRoots =
        new TableRootRepository(pointers, blobStore) {
          @Override
          public java.util.Optional<ai.floedb.floecat.catalog.rpc.SnapshotManifestPage>
              getManifestPage(BlobRef ref) {
            pageReads[0]++;
            return super.getManifestPage(ref);
          }
        };

    int n = SnapshotManifests.PAGE_ENTRY_BOUND + 44; // force a spill mid-fold
    SnapshotManifests.Chain chain = SnapshotManifests.chain(countingRoots, TABLE, null);
    BlobRef head = null;
    for (long id = 0; id < n; id++) {
      chain = chain.withHead(head);
      head = chain.prepend(entry(id, "s3://t/snap-" + id));
    }

    assertEquals(0, pageReads[0], "the fold must be served entirely by its own page cache");
    // The chain is intact: both ends and the spill boundary resolve.
    assertTrue(SnapshotManifests.findEntry(countingRoots, head, 0).isPresent());
    assertTrue(
        SnapshotManifests.findEntry(countingRoots, head, SnapshotManifests.PAGE_ENTRY_BOUND)
            .isPresent());
    assertTrue(SnapshotManifests.findEntry(countingRoots, head, n - 1).isPresent());
  }

  private static SnapshotManifestEntry entry(long id, long createdMs, boolean finalized) {
    var b =
        SnapshotManifestEntry.newBuilder()
            .setSnapshotId(id)
            .setSnapshotRef(BlobRef.newBuilder().setUri("s3://t/snap-" + id + ".pb"))
            .setUpstreamCreatedAt(com.google.protobuf.util.Timestamps.fromMillis(createdMs));
    if (finalized) {
      b.setStatsGenerationRef(BlobRef.newBuilder().setUri("s3://t/stats/" + id + "/gen.pb"));
    }
    return b.build();
  }

  @Test
  void latestQueryableCurrentPicksNewestFinalizedAtOrBeforeTheCommittedCurrent() {
    // S1, S2 finalized; S3 is the committed current but not yet finalized.
    var s3 = entry(3, 3_000, false);
    BlobRef head = SnapshotManifests.upsert(roots, TABLE, null, entry(1, 1_000, true));
    head = SnapshotManifests.upsert(roots, TABLE, head, entry(2, 2_000, true));
    head = SnapshotManifests.upsert(roots, TABLE, head, s3);

    var q = SnapshotManifests.latestQueryableCurrent(roots, head, s3).orElseThrow();
    assertEquals(2, q.getSnapshotId(), "newest finalized at or before the committed current");
  }

  @Test
  void latestQueryableCurrentNeverServesNewerThanARolledBackCommittedCurrent() {
    // Currency rolled back to S1 (older, still unfinalized); S2 is finalized but NEWER — a query
    // must not jump forward to it.
    var s1 = entry(1, 1_000, false);
    BlobRef head = SnapshotManifests.upsert(roots, TABLE, null, s1);
    head = SnapshotManifests.upsert(roots, TABLE, head, entry(2, 2_000, true));

    assertTrue(SnapshotManifests.latestQueryableCurrent(roots, head, s1).isEmpty());
  }

  @Test
  void latestQueryableCurrentEmptyWhenNothingIsFinalized() {
    var s1 = entry(1, 1_000, false);
    BlobRef head = SnapshotManifests.upsert(roots, TABLE, null, s1);
    assertTrue(SnapshotManifests.latestQueryableCurrent(roots, head, s1).isEmpty());
  }
}
