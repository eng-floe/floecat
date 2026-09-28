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

package ai.floedb.floecat.service.metagraph.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.catalog.rpc.BlobRef;
import ai.floedb.floecat.catalog.rpc.SnapshotManifestEntry;
import ai.floedb.floecat.catalog.rpc.TableRoot;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.impl.SnapshotManifests;
import ai.floedb.floecat.service.repo.impl.TableRootRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import com.google.protobuf.util.Timestamps;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class SnapshotRetentionPolicyTest {

  private static final Instant NOW = Instant.parse("2026-01-31T00:00:00Z");
  private static final ResourceId TABLE =
      ResourceId.newBuilder().setAccountId("acct").setId("tbl").build();

  private SnapshotRetentionPolicy policy() {
    return new SnapshotRetentionPolicy(
        Clock.fixed(NOW, java.time.ZoneOffset.UTC), Duration.ofDays(30), Duration.ofDays(7));
  }

  private static com.google.protobuf.Timestamp daysAgo(int days, long extraMillis) {
    return Timestamps.fromMillis(NOW.minus(Duration.ofDays(days)).toEpochMilli() - extraMillis);
  }

  @Test
  void visibilityUsesFloecatPublicationTime() {
    var policy = policy();
    assertThat(policy.visible(daysAgo(30, 0))).isTrue();
    assertThat(policy.visible(daysAgo(30, 1))).isFalse();
  }

  @Test
  void expiryRequiresRetentionAndGrace() {
    var policy = policy();
    assertThat(policy.expired(daysAgo(30, 1))).isFalse();
    assertThat(policy.expired(daysAgo(37, 0))).isFalse();
    assertThat(policy.expired(daysAgo(37, 1))).isTrue();
  }

  @Test
  void unknownPublicationIsNotVisibleButNeverExpires() {
    var policy = policy();
    assertThat(policy.visible(null)).isFalse();
    assertThat(policy.expired(null)).isFalse();
  }

  @Test
  void zeroRetentionDisablesExpiry() {
    var policy =
        new SnapshotRetentionPolicy(
            Clock.fixed(NOW, java.time.ZoneOffset.UTC), Duration.ZERO, Duration.ofDays(7));
    assertThat(policy.visible(Timestamps.fromMillis(0))).isTrue();
    assertThat(policy.expired(Timestamps.fromMillis(0))).isFalse();
    assertThat(policy.retentionAndGraceMillis()).isEqualTo(Duration.ofDays(7).toMillis());
  }

  @Test
  void negativeSettingsAreRejected() {
    assertThatThrownBy(
            () ->
                new SnapshotRetentionPolicy(Clock.systemUTC(), Duration.ofDays(-1), Duration.ZERO))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void publishedAtFallsBackFromEntryToPointerToBlobWriteTime() {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore(Clock.fixed(NOW, java.time.ZoneOffset.UTC));
    String blob = Keys.snapshotBlobUri("acct", "tbl", 1L, "sha");
    blobs.put(blob, "s".getBytes(StandardCharsets.UTF_8), "application/x-protobuf");
    var legacy =
        SnapshotManifestEntry.newBuilder()
            .setSnapshotId(1L)
            .setSnapshotRef(BlobRef.newBuilder().setUri(blob))
            .build();

    assertThat(SnapshotRetentionPolicy.publishedAt(TABLE, legacy, pointers, blobs))
        .contains(Timestamps.fromMillis(NOW.toEpochMilli()));

    String key = Keys.snapshotPointerById("acct", "tbl", 1L);
    pointers.compareAndSet(
        key,
        0L,
        Pointer.newBuilder()
            .setKey(key)
            .setBlobUri(blob)
            .setVersion(1L)
            .setIngestedAt(daysAgo(3, 0))
            .build());
    assertThat(SnapshotRetentionPolicy.publishedAt(TABLE, legacy, pointers, blobs))
        .contains(daysAgo(3, 0));

    var stamped = legacy.toBuilder().setIngestedAt(daysAgo(9, 0)).build();
    assertThat(SnapshotRetentionPolicy.publishedAt(TABLE, stamped, pointers, blobs))
        .contains(daysAgo(9, 0));

    blobs.delete(blob);
    var gone = legacy.toBuilder().setSnapshotId(2L).build();
    assertThat(SnapshotRetentionPolicy.publishedAt(TABLE, gone, pointers, blobs)).isEmpty();
  }

  @Test
  void underTheFinalizeGateProtectsTheSnapshotCurrentReadsServe() {
    var roots = new TableRootRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    BlobRef head = null;
    for (long id = 1; id <= 3; id++) {
      var entry = entry(id).toBuilder();
      if (id == 1) {
        entry.setStatsGenerationRef(BlobRef.newBuilder().setUri("s3://tbl/gen-1.pb"));
      }
      head = SnapshotManifests.chain(roots, TABLE, head).upsert(entry.build());
    }
    // Snapshot 3 is the committed current but unfinalized; CURRENT reads serve snapshot 1.
    TableRoot root =
        TableRoot.newBuilder().setCurrentSnapshotId(3L).setSnapshotManifestRef(head).build();
    var policy =
        new SnapshotRetentionPolicy(
            Clock.fixed(NOW, java.time.ZoneOffset.UTC), Duration.ofDays(30), Duration.ZERO);

    assertThat(policy.protectedSnapshotIds(SnapshotManifests.chain(roots, null, head), root, true))
        .containsExactlyInAnyOrder(3L, 1L);
  }

  private static SnapshotManifestEntry entry(long id) {
    return SnapshotManifestEntry.newBuilder()
        .setSnapshotId(id)
        .setSnapshotRef(BlobRef.newBuilder().setUri("s3://tbl/snap-" + id + ".pb"))
        .build();
  }
}
