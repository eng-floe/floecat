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

import ai.floedb.floecat.catalog.rpc.BlobRef;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotManifestEntry;
import ai.floedb.floecat.catalog.rpc.TableRoot;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.catalog.impl.TableRootCommitter;
import ai.floedb.floecat.service.catalog.impl.TableRootMutations;
import ai.floedb.floecat.service.catalog.impl.TableRootWriter;
import ai.floedb.floecat.service.metagraph.snapshot.SnapshotRetentionPolicy;
import ai.floedb.floecat.service.repo.impl.SnapshotManifests;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.TableRootRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.service.repo.util.TableBlobReachabilityGuard;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.BlobStore;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class SnapshotExpiryTest {

  private static final ResourceId TABLE =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("tbl")
          .setKind(ResourceKind.RK_TABLE)
          .build();
  private static final Timestamp THREE_DAYS_AGO =
      Timestamps.fromMillis(Instant.now().minus(Duration.ofDays(3)).toEpochMilli());

  private final InMemoryPointerStore pointers = new InMemoryPointerStore();
  private BlobStore blobs = new InMemoryBlobStore();
  private TableRootRepository roots;
  private SnapshotRepository snapshots;

  @Test
  void dropsExpiredSnapshotsWithTheirArtifactsAndRootEntries() {
    SnapshotExpiry expiry = expiry(retention());
    seedSnapshots(THREE_DAYS_AGO);
    List<String> artifacts = seedArtifactPointers(1L);

    assertEquals(2, expiry.expire(TABLE, Long.MAX_VALUE));

    assertEquals(List.of(3L), manifestIds());
    assertTrue(pointers.get(Keys.snapshotPointerById("acct", "tbl", 1L)).isEmpty());
    for (String key : artifacts) {
      assertTrue(pointers.get(key).isEmpty(), key);
    }
  }

  @Test
  void aLegacyEntryIsDatedByItsSnapshotBlob() {
    blobs = new InMemoryBlobStore(Clock.offset(Clock.systemUTC(), Duration.ofDays(-3)));
    SnapshotExpiry expiry = expiry(retention());
    seedSnapshots(null);

    assertEquals(2, expiry.expire(TABLE, Long.MAX_VALUE));
    assertEquals(List.of(3L), manifestIds());
  }

  @Test
  void recentSnapshotsAndDisabledRetentionKeepEverything() {
    seedSnapshots(Timestamps.fromMillis(System.currentTimeMillis()));
    assertEquals(0, expiry(retention()).expire(TABLE, Long.MAX_VALUE));
    assertEquals(0, expiry(SnapshotRetentionPolicy.disabled()).expire(TABLE, Long.MAX_VALUE));
    assertEquals(List.of(3L, 2L, 1L), manifestIds());
  }

  @Test
  void aPassedDeadlineJudgesNothing() {
    SnapshotExpiry expiry = expiry(retention());
    seedSnapshots(THREE_DAYS_AGO);

    assertEquals(0, expiry.expire(TABLE, 0L));
    assertFalse(manifestIds().isEmpty());
  }

  @Test
  void neverDropsASnapshotARollbackJustMadeCurrent() {
    SnapshotExpiry expiry = expiry(retention());
    seedSnapshots(THREE_DAYS_AGO);
    // The committed current moved to snapshot 1; the root has not caught up yet.
    new ai.floedb.floecat.service.repo.impl.CurrentSnapshotPointerRepository(pointers, blobs)
        .createIfAbsent(
            ai.floedb.floecat.catalog.rpc.CurrentSnapshotPointer.newBuilder()
                .setTableId(TABLE)
                .setSnapshotId(1L)
                .build());

    assertEquals(1, expiry.expire(TABLE, Long.MAX_VALUE));
    assertEquals(List.of(3L, 1L), manifestIds());
    assertTrue(pointers.get(Keys.snapshotPointerById("acct", "tbl", 1L)).isPresent());
  }

  @Test
  void expiresEveryTableOfTheAccountAcrossPages() {
    SnapshotExpiry expiry = expiry(retention());
    expiry.tablePageSize = 1;
    seedSnapshots(THREE_DAYS_AGO);
    for (String table : List.of("a-empty", "tbl")) {
      String tableKey = Keys.tablePointerById("acct", table);
      pointers.compareAndSet(
          tableKey, 0L, PointerReferences.blobPointer(tableKey, "s3://" + table + ".pb", 1L));
    }

    assertEquals(2, expiry.expireAccount("acct", Long.MAX_VALUE));
    assertEquals(List.of(3L), manifestIds());
  }

  private static SnapshotRetentionPolicy retention() {
    return new SnapshotRetentionPolicy(Clock.systemUTC(), Duration.ofDays(1), Duration.ofDays(1));
  }

  private SnapshotExpiry expiry(SnapshotRetentionPolicy policy) {
    roots = new TableRootRepository(pointers, blobs);
    snapshots = new SnapshotRepository(pointers, blobs, new TableRepository(pointers, blobs));
    var writer = new TableRootWriter(roots, committer(), null, snapshots, null, null, null);
    return new SnapshotExpiry(pointers, roots, snapshots, writer, null, policy);
  }

  private TableRootCommitter committer() {
    return new TableRootCommitter(roots, new TableBlobReachabilityGuard());
  }

  /** Snapshots 1, 2 and 3 (current), each published at {@code ingestedAt} (null: legacy). */
  private void seedSnapshots(Timestamp ingestedAt) {
    if (roots == null) {
      expiry(SnapshotRetentionPolicy.disabled());
    }
    for (long id = 1; id <= 3; id++) {
      var snapshot = Snapshot.newBuilder().setTableId(TABLE).setSnapshotId(id);
      if (ingestedAt != null) {
        snapshot.setIngestedAt(ingestedAt);
      }
      snapshots.create(snapshot.build());
      var entry =
          SnapshotManifestEntry.newBuilder()
              .setSnapshotId(id)
              .setSnapshotRef(
                  BlobRef.newBuilder().setUri(snapshots.metaForSafe(TABLE, id).getBlobUri()))
              .setUpstreamCreatedAt(Timestamps.fromMillis(id));
      if (ingestedAt != null) {
        entry.setIngestedAt(ingestedAt);
      }
      committer()
          .commit(
              TABLE, TableRootMutations.upsertSnapshot(roots, TABLE, entry.build(), null, true));
    }
  }

  private List<String> seedArtifactPointers(long snapshotId) {
    String blob = Keys.snapshotConstraintsBlobUri("acct", "tbl", snapshotId, "sha-a");
    blobs.put(blob, "a".getBytes(StandardCharsets.UTF_8), "text/plain");
    List<String> keys =
        List.of(
            Keys.snapshotTargetStatsManifestPointer("acct", "tbl", snapshotId),
            Keys.snapshotIndexArtifactActiveGenerationPointer("acct", "tbl", snapshotId),
            Keys.snapshotIndexArtifactCaptureManifestPointer("acct", "tbl", snapshotId),
            Keys.snapshotConstraintsPointer("acct", "tbl", snapshotId));
    for (String key : keys) {
      pointers.compareAndSet(key, 0L, PointerReferences.blobPointer(key, blob, 1L));
    }
    return keys;
  }

  private List<Long> manifestIds() {
    TableRoot root = roots.getByBlobUri(roots.pointerMetaForSafe(TABLE).getBlobUri()).orElseThrow();
    List<Long> ids = new ArrayList<>();
    SnapshotManifests.forEachEntry(
        roots, root.getSnapshotManifestRef(), entry -> ids.add(entry.getSnapshotId()));
    return ids;
  }
}
