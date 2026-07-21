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
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.TableRoot;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * The root-pointer TTL cache contract: warm reads touch no store; every same-process write
 * invalidates (read-your-writes); TTL 0 disables entirely.
 */
class TableRootPointerCacheTest {

  private static ResourceId table(String id) {
    return ai.floedb.floecat.service.util.TestSupport.rid("acct", id, ResourceKind.RK_TABLE);
  }

  /** Pointer store that counts get() calls so "zero remote reads warm" is directly assertable. */
  private static final class CountingPointerStore extends InMemoryPointerStore {
    final AtomicInteger gets = new AtomicInteger();

    @Override
    public java.util.Optional<ai.floedb.floecat.common.rpc.Pointer> get(String key) {
      gets.incrementAndGet();
      return super.get(key);
    }
  }

  private static ai.floedb.floecat.service.repo.cache.ImmutableBlobCache blobCache() {
    return new ai.floedb.floecat.service.repo.cache.ImmutableBlobCache(
        true, 1024 * 1024, java.time.Duration.ofMinutes(1));
  }

  private static TableRootRepository cachedRepo(CountingPointerStore pointers) {
    return cachedRepo(pointers, new InMemoryBlobStore());
  }

  private static TableRootRepository cachedRepo(
      CountingPointerStore pointers, InMemoryBlobStore blobs) {
    return new TableRootRepository(pointers, blobs, blobCache(), 2L);
  }

  @Test
  void warmReadsServeFromMemoryWithoutPointerReads() {
    var pointers = new CountingPointerStore();
    var repo = cachedRepo(pointers);
    var tableId = table("t-warm");
    repo.createIfAbsent(TableRoot.newBuilder().setTableId(tableId).setRootSeq(1).build());

    assertEquals(1, repo.get(tableId).orElseThrow().getRootSeq());
    int afterFirst = pointers.gets.get();
    // Second read within TTL: pointer meta cached, root blob from the decoded cache.
    assertEquals(1, repo.get(tableId).orElseThrow().getRootSeq());
    assertEquals(afterFirst, pointers.gets.get(), "a warm read must not touch the pointer store");
  }

  @Test
  void aSameProcessWriteInvalidatesAndTheNextReadSeesTheNewRoot() {
    var pointers = new CountingPointerStore();
    var repo = cachedRepo(pointers);
    var tableId = table("t-ryw");
    repo.createIfAbsent(TableRoot.newBuilder().setTableId(tableId).setRootSeq(1).build());
    assertEquals(1, repo.get(tableId).orElseThrow().getRootSeq()); // populate the cache

    long version = repo.metaForSafeLive(tableId).getPointerVersion();
    assertTrue(
        repo.update(TableRoot.newBuilder().setTableId(tableId).setRootSeq(2).build(), version));

    // Read-your-writes on the writing instance: no TTL wait needed.
    assertEquals(2, repo.get(tableId).orElseThrow().getRootSeq());
  }

  @Test
  void aPurgeInvalidatesTheCachedPointerLikeAnyOtherWrite() {
    var pointers = new CountingPointerStore();
    var repo = cachedRepo(pointers);
    var tableId = table("t-purge");
    repo.createIfAbsent(TableRoot.newBuilder().setTableId(tableId).setRootSeq(1).build());
    assertEquals(1, repo.get(tableId).orElseThrow().getRootSeq()); // populate the cache

    // DROP/account-cascade purge: the pointer AND the cached entry must go — a bare pointer-store
    // delete would leave the dropped table's root serving from cache for a further TTL.
    repo.purgeRoot(tableId);

    assertTrue(repo.get(tableId).isEmpty(), "read-your-writes must hold for purges too");
  }

  @Test
  void absenceIsNotCachedSoTheFirstRootIsVisibleImmediately() {
    var pointers = new CountingPointerStore();
    var repo = cachedRepo(pointers);
    var tableId = table("t-first");

    assertTrue(repo.get(tableId).isEmpty()); // miss on a table with no root yet
    repo.createIfAbsent(TableRoot.newBuilder().setTableId(tableId).setRootSeq(1).build());
    assertEquals(
        1,
        repo.get(tableId).orElseThrow().getRootSeq(),
        "a cached 'no root' would hide the first commit");
  }

  @Test
  void manifestEntryIndexServesFindEntryWithoutAPageWalk() {
    var pointers = new CountingPointerStore();
    var blobs = new InMemoryBlobStore();
    var repo = cachedRepo(pointers, blobs);
    var tableId = table("t-index");
    var head =
        SnapshotManifests.upsert(
            repo,
            tableId,
            null,
            ai.floedb.floecat.catalog.rpc.SnapshotManifestEntry.newBuilder()
                .setSnapshotId(7)
                .setSnapshotRef(
                    ai.floedb.floecat.catalog.rpc.BlobRef.newBuilder()
                        .setUri("s3://t/snap-7.pb")
                        .setVersion("v7"))
                .build());

    // First lookup builds the index; the second must be a pure map probe (no page decode either
    // way — the head was write-through-cached — but assert the RESULT is identical to the walk).
    var viaIndex = SnapshotManifests.findEntry(repo, head, 7).orElseThrow();
    assertEquals(7, viaIndex.getSnapshotId());
    assertEquals("s3://t/snap-7.pb", viaIndex.getSnapshotRef().getUri());
    assertTrue(SnapshotManifests.findEntry(repo, head, 999).isEmpty());

    // An uncached repo (walk path) agrees with the index path.
    var uncached = new TableRootRepository(pointers, blobs);
    assertEquals(viaIndex, SnapshotManifests.findEntry(uncached, head, 7).orElseThrow());
  }

  @Test
  void headPageMatchServesColdWithoutBuildingTheFullChainIndex() {
    var pointers = new CountingPointerStore();
    var blobs = new CountingBlobStore();
    var writer = cachedRepo(pointers, blobs);
    var tableId = table("t-head-fast");
    // One entry past the page bound: the newest entry spills into a fresh head page, so the chain
    // is two pages and the newest snapshot sits in the head.
    long newest = SnapshotManifests.PAGE_ENTRY_BOUND + 1;
    ai.floedb.floecat.catalog.rpc.BlobRef head = null;
    for (long id = 1; id <= newest; id++) {
      head =
          SnapshotManifests.upsert(
              writer,
              tableId,
              head,
              ai.floedb.floecat.catalog.rpc.SnapshotManifestEntry.newBuilder()
                  .setSnapshotId(id)
                  .setSnapshotRef(
                      ai.floedb.floecat.catalog.rpc.BlobRef.newBuilder()
                          .setUri("s3://t/snap-" + id + ".pb")
                          .setVersion("v" + id))
                  .build());
    }

    // A COLD reader (fresh decoded cache, same stores): the hottest lookup shape (CURRENT / a
    // recent AS_OF) matches in the head page and must cost exactly ONE page read — never a
    // serial full-chain walk to build the index.
    var coldReader = cachedRepo(pointers, blobs);
    blobs.gets.set(0);
    assertEquals(
        newest,
        SnapshotManifests.findEntry(coldReader, head, newest).orElseThrow().getSnapshotId());
    assertEquals(1, blobs.gets.get(), "a head-page match must not walk older pages");

    // A lookup that has to go deeper builds the index and still resolves.
    assertEquals(1, SnapshotManifests.findEntry(coldReader, head, 1).orElseThrow().getSnapshotId());
  }

  /** Blob store that counts get() calls so page-read budgets are directly assertable. */
  private static final class CountingBlobStore extends InMemoryBlobStore {
    final AtomicInteger gets = new AtomicInteger();

    @Override
    public byte[] get(String uri) {
      gets.incrementAndGet();
      return super.get(uri);
    }
  }

  @Test
  void ttlZeroDisablesTheCacheEntirely() {
    var pointers = new CountingPointerStore();
    var repo = new TableRootRepository(pointers, new InMemoryBlobStore());
    var tableId = table("t-off");
    repo.createIfAbsent(TableRoot.newBuilder().setTableId(tableId).setRootSeq(1).build());

    repo.get(tableId);
    int afterFirst = pointers.gets.get();
    repo.get(tableId);
    assertTrue(pointers.gets.get() > afterFirst, "with TTL 0 every read hits the live pointer");
  }
}
