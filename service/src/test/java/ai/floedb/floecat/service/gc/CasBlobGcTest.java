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

package ai.floedb.floecat.service.gc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CasBlobGcTest {

  private static final String ACCOUNT_ID = "acct-1";
  private static final String TABLE_ID = "tbl-1";

  private PointerStore pointers;
  private BlobStore blobs;
  private QueryContextStore queryContextStore;
  private CasBlobGc gc;

  @BeforeEach
  void setUp() {
    System.setProperty("floecat.gc.cas.min-age-ms", "0");
    System.setProperty("floecat.gc.cas.page-size", "200");
    pointers = new InMemoryPointerStore();
    blobs = new InMemoryBlobStore();
    queryContextStore = mock(QueryContextStore.class);
    when(queryContextStore.referencedPinBlobUris()).thenReturn(Set.of());
    gc = new CasBlobGc();
    gc.pointerStore = pointers;
    gc.blobStore = blobs;
    gc.queryContextStore = queryContextStore;
    gc.tableRootRepo = new ai.floedb.floecat.service.repo.impl.TableRootRepository(pointers, blobs);
    gc.statsRepository = new ai.floedb.floecat.service.repo.impl.StatsRepository(pointers, blobs);
  }

  @AfterEach
  void tearDown() {
    System.clearProperty("floecat.gc.cas.min-age-ms");
    System.clearProperty("floecat.gc.cas.page-size");
  }

  @Test
  void keepsCanonicalBlob() {
    String blobUri = Keys.tableBlobUri(ACCOUNT_ID, TABLE_ID, "sha-a");
    blobs.put(blobUri, "data".getBytes(StandardCharsets.UTF_8), "text/plain");
    putPointer(Keys.tablePointerById(ACCOUNT_ID, TABLE_ID), blobUri);

    gc.runForAccount(ACCOUNT_ID);

    assertTrue(blobs.head(blobUri).isPresent());
  }

  @Test
  void deletesOrphanBlob() {
    String blobUri = Keys.tableBlobUri(ACCOUNT_ID, TABLE_ID, "sha-orphan");
    blobs.put(blobUri, "orphan".getBytes(StandardCharsets.UTF_8), "text/plain");

    var result = gc.runForAccount(ACCOUNT_ID);

    assertFalse(blobs.head(blobUri).isPresent());
    assertFalse(result.poisoned(), "a healthy sweep is not poisoned (backlog age resets)");
  }

  @Test
  void sweepsSupersededConstraintsBlob() {
    // Constraints blobs are root-referenced now, so a superseded (unreferenced) constraints bundle
    // must be swept like the other table-subtree families, not leaked forever.
    String constraintsBlob = Keys.snapshotConstraintsBlobUri(ACCOUNT_ID, TABLE_ID, 7L, "sha-old");
    blobs.put(constraintsBlob, "old".getBytes(StandardCharsets.UTF_8), "application/octet-stream");

    gc.runForAccount(ACCOUNT_ID);

    assertFalse(blobs.head(constraintsBlob).isPresent(), "superseded constraints blob is swept");
  }

  @Test
  void statsBlobsGcHonorsPointers() {
    long snapshotId = 1L;
    String targetId = StatsTargetIdentity.storageId(StatsTargetIdentity.tableTarget());
    String statsBlob = Keys.snapshotTargetStatsBlobUri(ACCOUNT_ID, TABLE_ID, targetId, "sha-stats");
    String statsPtr = Keys.snapshotTargetStatsPointer(ACCOUNT_ID, TABLE_ID, snapshotId, targetId);

    seedCurrentTable();

    blobs.put(statsBlob, "stats".getBytes(StandardCharsets.UTF_8), "text/plain");
    putPointer(statsPtr, statsBlob);

    gc.runForAccount(ACCOUNT_ID);
    assertTrue(blobs.head(statsBlob).isPresent());

    pointers.delete(statsPtr);
    gc.runForAccount(ACCOUNT_ID);
    assertFalse(blobs.head(statsBlob).isPresent());
  }

  @Test
  void constraintsBlobsGcHonorsPointers() {
    // A live constraints pointer must protect its blob even at min-age=0 — the write window where
    // ConstraintRepository publishes the pointer before commitConstraints records the ref on the
    // root. Symmetric with statsBlobsGcHonorsPointers.
    long snapshotId = 1L;
    String constraintsBlob =
        Keys.snapshotConstraintsBlobUri(ACCOUNT_ID, TABLE_ID, snapshotId, "sha-c");
    String constraintsPtr = Keys.snapshotConstraintsPointer(ACCOUNT_ID, TABLE_ID, snapshotId);

    seedCurrentTable();

    blobs.put(constraintsBlob, "c".getBytes(StandardCharsets.UTF_8), "application/octet-stream");
    putPointer(constraintsPtr, constraintsBlob);

    gc.runForAccount(ACCOUNT_ID);
    assertTrue(
        blobs.head(constraintsBlob).isPresent(),
        "a live constraints pointer protects its blob before the root records the ref");

    pointers.delete(constraintsPtr);
    gc.runForAccount(ACCOUNT_ID);
    assertFalse(blobs.head(constraintsBlob).isPresent());
  }

  @Test
  void keepsBlobPinnedByActiveQuery() {
    // A blob no current pointer references, but that a live query has pinned, must survive GC.
    String blobUri = Keys.tableBlobUri(ACCOUNT_ID, TABLE_ID, "sha-pinned");
    blobs.put(blobUri, "pinned".getBytes(StandardCharsets.UTF_8), "text/plain");
    when(queryContextStore.referencedPinBlobUris()).thenReturn(Set.of(blobUri));

    gc.runForAccount(ACCOUNT_ID);

    assertTrue(blobs.head(blobUri).isPresent());

    // Once the query (and its pin) is gone, the now-orphan blob becomes collectable.
    when(queryContextStore.referencedPinBlobUris()).thenReturn(Set.of());
    gc.runForAccount(ACCOUNT_ID);
    assertFalse(blobs.head(blobUri).isPresent());
  }

  @Test
  void aPointerRetargetedToAnOldExistingBlobMidPassIsNotSwept() {
    // The staging data-loss interleave (eng-floe/core#1904): contents A and B both exist, the
    // pointer is on B when the pass marks its roots, and a concurrent update CASes it back to the
    // OLD blob A before the delete phase reaches A. The mark cannot see the CAS and A is already
    // older than min-age, so only the pre-delete owner re-check can keep it.
    String pointerKey = Keys.tablePointerById(ACCOUNT_ID, TABLE_ID);
    String blobA = Keys.tableBlobUri(ACCOUNT_ID, TABLE_ID, "sha-a");
    String blobB = Keys.tableBlobUri(ACCOUNT_ID, TABLE_ID, "sha-b");
    boolean[] flipped = {false};
    var racingBlobs =
        new InMemoryBlobStore() {
          @Override
          public BlobStore.Page list(String prefix, int limit, String pageToken) {
            // The first LIST of the delete phase runs strictly after the one-time pointer mark:
            // flip the pointer back onto the old blob A right here, mid-pass.
            if (!flipped[0]) {
              flipped[0] = true;
              var current = pointers.get(pointerKey).orElseThrow();
              pointers.compareAndSet(
                  pointerKey,
                  current.getVersion(),
                  PointerReferences.blobPointer(pointerKey, blobA, current.getVersion() + 1));
            }
            return super.list(prefix, limit, pageToken);
          }
        };
    gc.blobStore = racingBlobs;
    racingBlobs.put(blobA, "a".getBytes(StandardCharsets.UTF_8), "text/plain");
    racingBlobs.put(blobB, "b".getBytes(StandardCharsets.UTF_8), "text/plain");
    putPointer(pointerKey, blobB);

    gc.runForAccount(ACCOUNT_ID);

    assertTrue(flipped[0], "the mid-pass pointer CAS was injected");
    assertTrue(
        racingBlobs.head(blobA).isPresent(),
        "a blob its owning pointer re-targeted mid-pass must survive the sweep");

    // Next pass: the mark sees the pointer on A; B is genuinely unreferenced and is collected.
    gc.runForAccount(ACCOUNT_ID);
    assertTrue(racingBlobs.head(blobA).isPresent());
    assertFalse(racingBlobs.head(blobB).isPresent());
  }

  @Test
  void aPointerRetargetedBetweenTheGcHeadAndItsDeleteIsNotSwept() {
    // The narrower TOCTOU inside a single delete candidate: GC HEADs old blob A (stale header),
    // the writer THEN re-PUTs A (new version, fresh LastModified) and CASes the pointer onto it,
    // and only then does GC act. Both GC fences already ran on stale reads — the age fence on the
    // stale header, the owner re-check on the pre-CAS pointer — so only the version-targeted
    // delete can refuse: it names the version the pass age-checked, which the re-PUT superseded.
    String pointerKey = Keys.tablePointerById(ACCOUNT_ID, TABLE_ID);
    String blobA = Keys.tableBlobUri(ACCOUNT_ID, TABLE_ID, "sha-a");
    String blobB = Keys.tableBlobUri(ACCOUNT_ID, TABLE_ID, "sha-b");
    boolean[] injected = {false};
    var racingPointers =
        new InMemoryPointerStore() {
          @Override
          public java.util.Optional<Pointer> get(String key) {
            var observed = super.get(key);
            // The only get() of the table's by-id pointer in a pass is the pre-delete owner
            // re-check (the mark scans by prefix), so GC has already HEAD'd blob A here. Serve
            // the stale pointer (still on B), then land the writer's re-PUT + CAS onto A before
            // GC reaches its delete.
            if (!injected[0] && pointerKey.equals(key)) {
              injected[0] = true;
              blobs.put(blobA, "a".getBytes(StandardCharsets.UTF_8), "text/plain");
              var current = super.get(key).orElseThrow();
              super.compareAndSet(
                  key,
                  current.getVersion(),
                  PointerReferences.blobPointer(pointerKey, blobA, current.getVersion() + 1));
            }
            return observed;
          }
        };
    gc.pointerStore = racingPointers;
    blobs.put(blobA, "a".getBytes(StandardCharsets.UTF_8), "text/plain");
    blobs.put(blobB, "b".getBytes(StandardCharsets.UTF_8), "text/plain");
    racingPointers.compareAndSet(
        pointerKey, 0L, PointerReferences.blobPointer(pointerKey, blobB, 1L));

    var result = gc.runForAccount(ACCOUNT_ID);

    assertTrue(injected[0], "the between-head-and-delete re-PUT + CAS was injected");
    assertTrue(
        blobs.head(blobA).isPresent(),
        "the version-targeted delete must not touch the version the writer just re-PUT");
    assertEquals(0, result.blobsDeleted(), "nothing was deleted this pass");
    assertEquals(
        blobA,
        racingPointers.get(pointerKey).orElseThrow().getBlobUri(),
        "the pointer resolves — no dangling");
  }

  @Test
  void sweepFailsClosedWhenTheStoreCannotDeleteByVersion() {
    // Without immutable version identities (S3: bucket versioning not Enabled) every delete is
    // the eng-floe/core#1904 race, so the pass must collect NOTHING — never fall back to
    // unconditional deletes — and must report the skip so it is gauged, not silent.
    var unversionedBlobs =
        new InMemoryBlobStore() {
          @Override
          public boolean supportsVersionedDeletes() {
            return false;
          }
        };
    gc.blobStore = unversionedBlobs;
    String orphan = Keys.tableBlobUri(ACCOUNT_ID, TABLE_ID, "sha-orphan");
    unversionedBlobs.put(orphan, "x".getBytes(StandardCharsets.UTF_8), "text/plain");

    var result = gc.runForAccount(ACCOUNT_ID);

    assertTrue(unversionedBlobs.head(orphan).isPresent(), "a fail-closed pass deletes nothing");
    assertEquals(0, result.blobsDeleted());
    assertTrue(result.deletesUnsupported(), "the skip surfaces for the scheduler gauge");
  }

  @Test
  void aBlobWhoseHeaderLacksAVersionIdIsSkippedNotDeleted() {
    // Capability says versioned deletes work, but this header carries no versionId: the pass
    // cannot name the version it age-checked, so it must fail closed on this blob rather than
    // fall back to an unconditional delete.
    var versionlessHeads =
        new InMemoryBlobStore() {
          @Override
          public java.util.Optional<ai.floedb.floecat.common.rpc.BlobHeader> head(String key) {
            return super.head(key).map(h -> h.toBuilder().clearVersionId().build());
          }
        };
    gc.blobStore = versionlessHeads;
    String orphan = Keys.tableBlobUri(ACCOUNT_ID, TABLE_ID, "sha-orphan");
    versionlessHeads.put(orphan, "x".getBytes(StandardCharsets.UTF_8), "text/plain");

    var result = gc.runForAccount(ACCOUNT_ID);

    assertTrue(versionlessHeads.head(orphan).isPresent(), "the unnameable version is skipped");
    assertEquals(0, result.blobsDeleted());
  }

  @Test
  void secondaryPointerDoesNotProtectBlob() {
    String blobUri = Keys.tableBlobUri(ACCOUNT_ID, TABLE_ID, "sha-secondary");
    blobs.put(blobUri, "data".getBytes(StandardCharsets.UTF_8), "text/plain");

    String secondaryPtr = Keys.tablePointerByName(ACCOUNT_ID, "cat-1", "ns-1", "tbl_name");
    putPointer(secondaryPtr, blobUri);

    gc.runForAccount(ACCOUNT_ID);

    assertFalse(blobs.head(blobUri).isPresent());
  }

  @Test
  void resolvingPinRootHandsOffToTheCommittedContextAcrossGcRuns() {
    // Full pin lifecycle against a REAL context store (no stubbed root set): the blob is rooted by
    // the transient resolving registration, then by the committed context, and only becomes
    // collectable once the context is gone — with a GC pass probing every stage.
    var store = ai.floedb.floecat.service.query.impl.QueryContextStores.forTesting();
    gc.queryContextStore = store;
    try {
      String pinnedBlob = Keys.tableBlobUri(ACCOUNT_ID, TABLE_ID, "sha-pin-lifecycle");
      blobs.put(pinnedBlob, "pinned".getBytes(StandardCharsets.UTF_8), "text/plain");

      // Stage 1: resolving — the pin is constructed but not yet committed into a context.
      store.registerResolvingPinBlobs("q-gc", java.util.List.of(pinnedBlob));
      gc.runForAccount(ACCOUNT_ID);
      assertTrue(blobs.head(pinnedBlob).isPresent(), "resolving root must protect the blob");

      // Stage 2: committed — the stored context takes over as the durable root.
      ai.floedb.floecat.query.rpc.TablePin pin =
          ai.floedb.floecat.query.rpc.TablePin.newBuilder()
              .setTableId(
                  ai.floedb.floecat.common.rpc.ResourceId.newBuilder()
                      .setAccountId(ACCOUNT_ID)
                      .setId(TABLE_ID)
                      .setKind(ai.floedb.floecat.common.rpc.ResourceKind.RK_TABLE))
              .setPinKind(ai.floedb.floecat.query.rpc.PinKind.PIN_KIND_CURRENT)
              .setSnapshotId(7)
              .setTableBlobUri(pinnedBlob)
              .setSnapshotBlobUri(pinnedBlob)
              .build();
      store.put(
          ai.floedb.floecat.service.query.impl.QueryContext.newActive(
              "q-gc",
              ai.floedb.floecat.common.rpc.PrincipalContext.newBuilder()
                  .setAccountId(ACCOUNT_ID)
                  .build(),
              new byte[0],
              ai.floedb.floecat.query.rpc.RelationPinSet.newBuilder()
                  .addPins(ai.floedb.floecat.service.query.QueryPins.ofTable(pin))
                  .build()
                  .toByteArray(),
              new byte[0],
              new byte[0],
              60_000L,
              1L,
              ai.floedb.floecat.common.rpc.ResourceId.newBuilder().setId("cat").build()));
      gc.runForAccount(ACCOUNT_ID);
      assertTrue(blobs.head(pinnedBlob).isPresent(), "committed context must protect the blob");

      // Stage 3: query gone — nothing roots the blob and the next pass sweeps it.
      store.delete("q-gc");
      gc.runForAccount(ACCOUNT_ID);
      assertFalse(blobs.head(pinnedBlob).isPresent(), "unrooted blob must be swept");
    } finally {
      store.close();
    }
  }

  @Test
  void pinRegisteredMidSweepStillProtectsItsBlob() {
    // The pin-root set captured when the run starts goes stale over a long sweep. Simulate a pin
    // registered after that snapshot (first read: empty; every later per-page refresh: pinned) —
    // the delete pass must consult the fresh roots and keep the blob.
    String blobUri = Keys.tableBlobUri(ACCOUNT_ID, TABLE_ID, "sha-late-pin");
    blobs.put(blobUri, "late".getBytes(StandardCharsets.UTF_8), "text/plain");
    when(queryContextStore.referencedPinBlobUris())
        .thenReturn(Set.of())
        .thenReturn(Set.of(blobUri));

    gc.runForAccount(ACCOUNT_ID);

    assertTrue(
        blobs.head(blobUri).isPresent(),
        "a pin registered after the run-start root snapshot must still protect its blob");
  }

  @Test
  void currentRootChainProtectsEverythingItReferences() {
    // Make the table discoverable so the per-table pass runs.
    seedCurrentTable();

    // A root whose entry references a snapshot blob NO live pointer names (an in-place update
    // moved the pointer on) and a superseded generation manifest (only the root's ref names it).
    var tableId = tableRid();
    String oldSnapBlob = Keys.snapshotBlobUri(ACCOUNT_ID, TABLE_ID, 7L, "sha-old-snap");
    blobs.put(oldSnapBlob, "snap".getBytes(StandardCharsets.UTF_8), "text/plain");
    String genManifest =
        Keys.snapshotTargetStatsManifestBlobUri(ACCOUNT_ID, TABLE_ID, 7L, "gen-old");
    blobs.put(genManifest, "gen".getBytes(StandardCharsets.UTF_8), "text/plain");
    commitRoot(7L, oldSnapBlob, "v-old", genManifest);
    var rootMeta = gc.tableRootRepo.metaForSafe(tableId);
    var root = gc.tableRootRepo.get(tableId).orElseThrow();
    String pageBlob = root.getSnapshotManifestRef().getUri();

    gc.runForAccount(ACCOUNT_ID);

    assertTrue(blobs.head(rootMeta.getBlobUri()).isPresent(), "root blob is a GC root");
    assertTrue(blobs.head(pageBlob).isPresent(), "manifest page is reachable from the root");
    assertTrue(blobs.head(oldSnapBlob).isPresent(), "entry snapshot ref survives the pointer");
    assertTrue(blobs.head(genManifest).isPresent(), "generation manifest ref survives");
  }

  @Test
  void supersededUnpinnedRootBlobsAreSwept() {
    seedCurrentTable();

    // A superseded root blob nothing references: not the current pointer, no pin.
    String oldRoot = Keys.tableRootBlobUri(ACCOUNT_ID, TABLE_ID, "sha-old-root");
    blobs.put(oldRoot, "old-root".getBytes(StandardCharsets.UTF_8), "text/plain");

    gc.runForAccount(ACCOUNT_ID);

    assertFalse(blobs.head(oldRoot).isPresent(), "superseded unpinned root blobs are collected");
  }

  @Test
  void aPinnedRootChainSurvivesSupersession() {
    seedCurrentTable();

    // A superseded root (not the current pointer target) that a live query pinned. The pin roots
    // the root URI; the chain expansion must protect its page and refs too.
    var tableId = tableRid();
    String pinnedSnapBlob = Keys.snapshotBlobUri(ACCOUNT_ID, TABLE_ID, 3L, "sha-pinned-snap");
    blobs.put(pinnedSnapBlob, "snap".getBytes(StandardCharsets.UTF_8), "text/plain");
    commitRoot(3L, pinnedSnapBlob, "v3", null);
    var pinnedRootUri = gc.tableRootRepo.metaForSafe(tableId).getBlobUri();
    String pinnedPage =
        gc.tableRootRepo.get(tableId).orElseThrow().getSnapshotManifestRef().getUri();
    // Supersede it: drop the pointer (as a newer root CAS + a later purge would leave it), keep
    // the pin.
    pointers.delete(Keys.tableRootByTable(ACCOUNT_ID, TABLE_ID));
    when(queryContextStore.referencedPinBlobUris()).thenReturn(Set.of(pinnedRootUri));

    gc.runForAccount(ACCOUNT_ID);

    assertTrue(blobs.head(pinnedRootUri).isPresent(), "pinned root blob survives");
    assertTrue(blobs.head(pinnedPage).isPresent(), "pinned root's page survives via expansion");
    assertTrue(blobs.head(pinnedSnapBlob).isPresent(), "pinned root's snapshot ref survives");

    // Pin released: the whole superseded chain becomes collectable.
    when(queryContextStore.referencedPinBlobUris()).thenReturn(Set.of());
    gc.runForAccount(ACCOUNT_ID);
    assertFalse(blobs.head(pinnedRootUri).isPresent());
    assertFalse(blobs.head(pinnedPage).isPresent());
  }

  @Test
  void aGenerationTheCurrentRootStillReferencesSurvivesTheLivePointerMovingOn() {
    // The finalize's live-pointer flip and its root commit are not atomic: a replace can move the
    // active pointer to G2 while the current root still references G1. In that window G1 is
    // neither live nor pinned — only the root names it — and the generation reclaim must not
    // collect it, or every pin taken on the root serves a deleted generation.
    seedCurrentTable();

    String g1Manifest = Keys.snapshotTargetStatsManifestBlobUri(ACCOUNT_ID, TABLE_ID, 7L, "gen-1");
    blobs.put(g1Manifest, "g1".getBytes(StandardCharsets.UTF_8), "text/plain");
    String g2Manifest = Keys.snapshotTargetStatsManifestBlobUri(ACCOUNT_ID, TABLE_ID, 7L, "gen-2");
    blobs.put(g2Manifest, "g2".getBytes(StandardCharsets.UTF_8), "text/plain");

    // G1 is a reclaim candidate (its generation directory pointer exists) and the live active
    // pointer has already moved on to G2.
    String targetId = StatsTargetIdentity.storageId(StatsTargetIdentity.tableTarget());
    putPointer(
        Keys.snapshotTargetStatsGenerationPointer(ACCOUNT_ID, TABLE_ID, 7L, "gen-1", targetId),
        Keys.snapshotTargetStatsBlobUri(ACCOUNT_ID, TABLE_ID, 7L, "gen-1", targetId, "sha-rec"));
    putPointer(Keys.snapshotTargetStatsManifestPointer(ACCOUNT_ID, TABLE_ID, 7L), g2Manifest);

    // The current root still references G1: root commit for the G2 activation has not landed.
    String snapBlob = Keys.snapshotBlobUri(ACCOUNT_ID, TABLE_ID, 7L, "sha-snap");
    blobs.put(snapBlob, "snap".getBytes(StandardCharsets.UTF_8), "text/plain");
    commitRoot(7L, snapBlob, "v7", g1Manifest);

    gc.runForAccount(ACCOUNT_ID);

    assertTrue(
        blobs.head(g1Manifest).isPresent(),
        "a generation the current root references is a GC root even when the live pointer moved");
  }

  @Test
  void aPinTakenMidSweepProtectsItsRootsGenerations() {
    // A pin registered after the sweep started protects its root's WHOLE chain — including the
    // generation manifests its entries reference — not just the pin's own blob URIs. The pinned
    // root here is superseded (no pointer names it), so only the mid-sweep pin can save G1.
    seedCurrentTable();

    String g1Manifest = Keys.snapshotTargetStatsManifestBlobUri(ACCOUNT_ID, TABLE_ID, 3L, "gen-1");
    blobs.put(g1Manifest, "g1".getBytes(StandardCharsets.UTF_8), "text/plain");
    String targetId = StatsTargetIdentity.storageId(StatsTargetIdentity.tableTarget());
    putPointer(
        Keys.snapshotTargetStatsGenerationPointer(ACCOUNT_ID, TABLE_ID, 3L, "gen-1", targetId),
        Keys.snapshotTargetStatsBlobUri(ACCOUNT_ID, TABLE_ID, 3L, "gen-1", targetId, "sha-rec"));

    var tableId = tableRid();
    String snapBlob = Keys.snapshotBlobUri(ACCOUNT_ID, TABLE_ID, 3L, "sha-snap");
    blobs.put(snapBlob, "snap".getBytes(StandardCharsets.UTF_8), "text/plain");
    commitRoot(3L, snapBlob, "v3", g1Manifest);
    String pinnedRootUri = gc.tableRootRepo.metaForSafe(tableId).getBlobUri();
    // Supersede the root (nothing but the pin will name it) and register the pin only after the
    // sweep's initial root snapshot (first read empty, later reads pinned).
    pointers.delete(Keys.tableRootByTable(ACCOUNT_ID, TABLE_ID));
    when(queryContextStore.referencedPinBlobUris())
        .thenReturn(Set.of())
        .thenReturn(Set.of(pinnedRootUri));

    gc.runForAccount(ACCOUNT_ID);

    assertTrue(
        blobs.head(g1Manifest).isPresent(),
        "a mid-sweep pin must protect the generation manifests its pinned root references");
  }

  @Test
  void aJustPublishedGenerationSurvivesTheReclaimUntilItAges() {
    // publish->flip window: the generation manifest is written BEFORE the active pointer flips
    // and before any root references it — in that instant only its age can protect it. With the
    // min-age guard on, a young manifest must survive even though it is unreferenced, not live,
    // and not pinned.
    System.setProperty("floecat.gc.cas.min-age-ms", "3600000");
    try {
      seedCurrentTable();

      String youngManifest =
          Keys.snapshotTargetStatsManifestBlobUri(ACCOUNT_ID, TABLE_ID, 9L, "gen-new");
      blobs.put(youngManifest, "g".getBytes(StandardCharsets.UTF_8), "text/plain");
      String targetId = StatsTargetIdentity.storageId(StatsTargetIdentity.tableTarget());
      putPointer(
          Keys.snapshotTargetStatsGenerationPointer(ACCOUNT_ID, TABLE_ID, 9L, "gen-new", targetId),
          Keys.snapshotTargetStatsBlobUri(ACCOUNT_ID, TABLE_ID, 9L, "gen-new", targetId, "sha-r"));
      // No active pointer, no root, no pin: pre-guard, this was reclaimable on the spot.

      gc.runForAccount(ACCOUNT_ID);

      assertTrue(
          blobs.head(youngManifest).isPresent(),
          "a manifest younger than min-age must survive the publish->flip window");
    } finally {
      System.setProperty("floecat.gc.cas.min-age-ms", "0");
    }
  }

  @Test
  void aFailedChainWalkPoisonsTheWholeSweep() {
    // Manifest pages and the refs inside them are reachable ONLY through chain walks. If a walk
    // cannot complete (missing page blob, transient storage error), the referenced set is not
    // trustworthy and NOTHING may be deleted this pass — an orphan elsewhere must survive too.
    seedCurrentTable();

    var tableId = tableRid();
    commitRoot(1L, Keys.snapshotBlobUri(ACCOUNT_ID, TABLE_ID, 1L, "sha-s"), "v1", null);
    // Break the chain: sweep the manifest page blob out from under the root.
    String pageUri = gc.tableRootRepo.get(tableId).orElseThrow().getSnapshotManifestRef().getUri();
    blobs.delete(pageUri);

    // An unrelated orphan that a healthy pass would collect.
    String orphan = Keys.tableBlobUri(ACCOUNT_ID, "other-table", "sha-orphan");
    blobs.put(orphan, "orphan".getBytes(StandardCharsets.UTF_8), "text/plain");

    var result = gc.runForAccount(ACCOUNT_ID);

    assertTrue(blobs.head(orphan).isPresent(), "a poisoned sweep must delete NOTHING");
    assertTrue(result.blobsDeleted() == 0, "no deletes when a chain walk failed");
    assertTrue(result.poisoned(), "the poison signal must surface for the backlog gauge");
  }

  @Test
  void retryableChainReadIsRetriedBeforePoisoningTheSweep() {
    seedCurrentTable();

    String snapBlob = Keys.snapshotBlobUri(ACCOUNT_ID, TABLE_ID, 1L, "sha-s");
    blobs.put(snapBlob, "snap".getBytes(StandardCharsets.UTF_8), "text/plain");
    commitRoot(1L, snapBlob, "v1", null);

    String orphan = Keys.tableBlobUri(ACCOUNT_ID, "other-table", "sha-orphan");
    blobs.put(orphan, "orphan".getBytes(StandardCharsets.UTF_8), "text/plain");

    int[] attempts = {0};
    gc.tableRootRepo =
        new ai.floedb.floecat.service.repo.impl.TableRootRepository(pointers, blobs) {
          @Override
          public java.util.Optional<ai.floedb.floecat.catalog.rpc.SnapshotManifestPage>
              getManifestPage(ai.floedb.floecat.catalog.rpc.BlobRef ref) {
            attempts[0]++;
            if (attempts[0] == 1) {
              throw new ai.floedb.floecat.service.repo.util.BaseResourceRepository
                  .AbortRetryableException("one transient page-read failure");
            }
            return super.getManifestPage(ref);
          }
        };

    var result = gc.runForAccount(ACCOUNT_ID);

    assertFalse(result.poisoned(), "a retryable one-off read failure should not poison the sweep");
    assertFalse(blobs.head(orphan).isPresent(), "healthy sweep still collects unrelated garbage");
    assertEquals(2, attempts[0], "the manifest page read was retried once");
  }

  @Test
  void aCyclicManifestChainPoisonsTheSweepInsteadOfHanging() {
    // Content-addressed pages are acyclic by construction, so a repeated prevPageRef means
    // corruption. The walk must fail safe (poison the sweep, delete nothing) rather than loop
    // forever on the GC background thread, which has no request timeout to rescue it.
    String tableBlob = seedCurrentTable();

    var tableId = tableRid();
    // A root whose manifest page points back at itself (self-cycle) — the minimal malformed chain.
    var pageRef =
        ai.floedb.floecat.catalog.rpc.BlobRef.newBuilder()
            .setUri(Keys.snapshotManifestBlobUri(ACCOUNT_ID, TABLE_ID, "sha-cyclic"))
            .build();
    var cyclicPage =
        ai.floedb.floecat.catalog.rpc.SnapshotManifestPage.newBuilder()
            .setPrevPageRef(pageRef)
            .build();
    blobs.put(pageRef.getUri(), cyclicPage.toByteArray(), "application/x-protobuf");
    var root =
        ai.floedb.floecat.catalog.rpc.TableRoot.newBuilder()
            .setTableId(tableId)
            .setDefinitionRef(
                ai.floedb.floecat.catalog.rpc.BlobRef.newBuilder()
                    .setUri(tableBlob)
                    .setVersion("v"))
            .setSnapshotManifestRef(pageRef)
            .build();
    String rootBlob = Keys.tableRootBlobUri(ACCOUNT_ID, TABLE_ID, "sha-root");
    blobs.put(rootBlob, root.toByteArray(), "application/x-protobuf");
    putPointer(Keys.tableRootByTable(ACCOUNT_ID, TABLE_ID), rootBlob);

    // An unrelated orphan a healthy pass would collect.
    String orphan = Keys.tableBlobUri(ACCOUNT_ID, "other", "sha-orphan");
    blobs.put(orphan, "orphan".getBytes(StandardCharsets.UTF_8), "text/plain");

    var result = gc.runForAccount(ACCOUNT_ID);

    assertTrue(blobs.head(orphan).isPresent(), "a cyclic chain poisons the whole sweep");
    assertTrue(result.blobsDeleted() == 0, "no deletes when a chain walk cannot terminate");
    assertTrue(result.poisoned(), "a cyclic chain reports poisoned for the backlog gauge");
  }

  @Test
  void aRootBlobYoungerThanMinAgeSurvivesEvenWhenUnreferenced() {
    // The fence that protects a root committed DURING a sweep: nowMs is frozen at pass start, so a
    // blob written after that (a mid-sweep root commit) is younger than min-age and always
    // skipped, no matter how long the sweep runs. Simulate that "just-written, not-yet-referenced"
    // root with a fresh blob under a long min-age — it must survive.
    System.setProperty("floecat.gc.cas.min-age-ms", "3600000");
    try {
      seedCurrentTable();

      // A root blob no pointer names yet (its CAS pointer would be written microseconds later) —
      // the exact shape of a root committed after this table's one-time mark.
      String freshRoot = Keys.tableRootBlobUri(ACCOUNT_ID, TABLE_ID, "sha-fresh-root");
      blobs.put(freshRoot, "root".getBytes(StandardCharsets.UTF_8), "text/plain");

      gc.runForAccount(ACCOUNT_ID);

      assertTrue(
          blobs.head(freshRoot).isPresent(),
          "a root blob younger than min-age must survive even unreferenced (mid-sweep-commit fence)");
    } finally {
      System.setProperty("floecat.gc.cas.min-age-ms", "0");
    }
  }

  @Test
  void aBlobWithNoReadableHeaderIsSkippedNotDeleted() {
    // A missing HEAD (transient failure or read-after-write lag) must fail SAFE: we cannot prove
    // the blob is old enough, so skip it this pass rather than delete an unreferenced-looking
    // blob that might be brand new — matching the generation reclaim.
    System.setProperty("floecat.gc.cas.min-age-ms", "30000");
    boolean[] hideHead = {true};
    var headlessBlobs =
        new InMemoryBlobStore() {
          @Override
          public java.util.Optional<ai.floedb.floecat.common.rpc.BlobHeader> head(String key) {
            return hideHead[0] ? java.util.Optional.empty() : super.head(key);
          }
        };
    gc.blobStore = headlessBlobs;
    try {
      String orphan = Keys.tableBlobUri(ACCOUNT_ID, TABLE_ID, "sha-headless");
      headlessBlobs.put(orphan, "x".getBytes(StandardCharsets.UTF_8), "text/plain");

      var result = gc.runForAccount(ACCOUNT_ID);

      assertEquals(
          0, result.blobsDeleted(), "a blob whose header cannot be read must not be deleted");
      hideHead[0] = false; // the blob is still there — only the HEAD was hidden
      assertTrue(
          headlessBlobs.head(orphan).isPresent(), "the blob survived the poisoned-head pass");
    } finally {
      System.setProperty("floecat.gc.cas.min-age-ms", "0");
    }
  }

  private void putPointer(String key, String blobUri) {
    Pointer ptr = PointerReferences.blobPointer(key, blobUri, 1L);
    pointers.compareAndSet(key, 0L, ptr);
  }

  private String seedCurrentTable() {
    String tableBlob = Keys.tableBlobUri(ACCOUNT_ID, TABLE_ID, "sha-table");
    blobs.put(tableBlob, "table".getBytes(StandardCharsets.UTF_8), "text/plain");
    putPointer(Keys.tablePointerById(ACCOUNT_ID, TABLE_ID), tableBlob);
    return tableBlob;
  }

  private ai.floedb.floecat.common.rpc.ResourceId tableRid() {
    return ai.floedb.floecat.common.rpc.ResourceId.newBuilder()
        .setAccountId(ACCOUNT_ID)
        .setId(TABLE_ID)
        .setKind(ai.floedb.floecat.common.rpc.ResourceKind.RK_TABLE)
        .build();
  }

  private void commitRoot(
      long snapshotId, String snapBlobUri, String snapVersion, String genManifestUri) {
    var tableId = tableRid();
    var entry =
        ai.floedb.floecat.catalog.rpc.SnapshotManifestEntry.newBuilder()
            .setSnapshotId(snapshotId)
            .setSnapshotRef(
                ai.floedb.floecat.catalog.rpc.BlobRef.newBuilder()
                    .setUri(snapBlobUri)
                    .setVersion(snapVersion));
    if (genManifestUri != null) {
      entry.setStatsGenerationRef(
          ai.floedb.floecat.catalog.rpc.BlobRef.newBuilder().setUri(genManifestUri));
    }
    var committer = new ai.floedb.floecat.service.catalog.impl.TableRootCommitter(gc.tableRootRepo);
    committer.commit(
        tableId,
        ai.floedb.floecat.service.catalog.impl.TableRootMutations.upsertSnapshot(
            gc.tableRootRepo,
            tableId,
            entry.build(),
            ai.floedb.floecat.catalog.rpc.BlobRef.newBuilder()
                .setUri(Keys.tableBlobUri(ACCOUNT_ID, TABLE_ID, "sha-table"))
                .setVersion("v-t")
                .build(),
            true));
  }
}
