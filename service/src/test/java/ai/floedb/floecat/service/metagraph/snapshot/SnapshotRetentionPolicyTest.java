/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
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
    assertThatThrownBy(
            () -> new SnapshotRetentionPolicy(Clock.systemUTC(), Duration.ZERO, Duration.ZERO, -1))
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
  void protectsTheCurrentAndTheLastReplacedSnapshots() {
    var roots = new TableRootRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    BlobRef head = null;
    for (long id = 1; id <= 4; id++) {
      head = SnapshotManifests.chain(roots, TABLE, head).upsert(entry(id));
    }
    // Entries are newest first: 4, 3, 2, 1. Snapshot 3 is current.
    TableRoot root =
        TableRoot.newBuilder().setCurrentSnapshotId(3L).setSnapshotManifestRef(head).build();
    var chain = SnapshotManifests.chain(roots, null, head);
    var policy =
        new SnapshotRetentionPolicy(
            Clock.fixed(NOW, java.time.ZoneOffset.UTC), Duration.ofDays(30), Duration.ZERO, 2);

    assertThat(policy.protectedSnapshotIds(chain, root, false))
        .containsExactlyInAnyOrder(3L, 4L, 2L);
  }

  private static SnapshotManifestEntry entry(long id) {
    return SnapshotManifestEntry.newBuilder()
        .setSnapshotId(id)
        .setSnapshotRef(BlobRef.newBuilder().setUri("s3://tbl/snap-" + id + ".pb"))
        .build();
  }
}
