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

import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PointerGcTest {

  private static final String ACCOUNT_ID = "acct-1";
  private static final String TABLE_ID = "tbl-1";

  private PointerStore pointers;
  private BlobStore blobs;
  private PointerGc gc;

  @BeforeEach
  void setUp() {
    pointers = new InMemoryPointerStore();
    blobs = new InMemoryBlobStore();
    gc = new PointerGc();
    gc.pointerStore = pointers;
    gc.blobStore = blobs;
  }

  @AfterEach
  void tearDown() {
    System.clearProperty("floecat.gc.pointer.min-age-ms");
  }

  @Test
  void deletesDanglingPointer() {
    System.setProperty("floecat.gc.pointer.min-age-ms", "0");
    String blobUri = Keys.tableBlobUri(ACCOUNT_ID, TABLE_ID, "sha-missing");
    String ptrKey = Keys.tablePointerById(ACCOUNT_ID, TABLE_ID);
    putPointer(ptrKey, blobUri);

    gc.runForAccount(ACCOUNT_ID, System.currentTimeMillis() + 5_000L);

    assertTrue(pointers.get(ptrKey).isEmpty());
  }

  @Test
  void deletesSnapshotPointerAfterRetentionAndGrace() {
    Instant now = Instant.parse("2026-01-10T00:00:00Z");
    gc.retentionPolicy =
        new ai.floedb.floecat.service.metagraph.snapshot.SnapshotRetentionPolicy(
            Clock.fixed(now, java.time.ZoneOffset.UTC), Duration.ofDays(1), Duration.ofDays(1));
    String tableBlob = Keys.tableBlobUri(ACCOUNT_ID, TABLE_ID, "sha-table");
    blobs.put(tableBlob, "table".getBytes(StandardCharsets.UTF_8), "text/plain");
    putPointer(Keys.tablePointerById(ACCOUNT_ID, TABLE_ID), tableBlob);
    String snapshotBlob = Keys.snapshotBlobUri(ACCOUNT_ID, TABLE_ID, 1L, "sha-old");
    blobs.put(
        snapshotBlob,
        ai.floedb.floecat.catalog.rpc.Snapshot.newBuilder()
            .setTableId(
                ai.floedb.floecat.common.rpc.ResourceId.newBuilder()
                    .setAccountId(ACCOUNT_ID)
                    .setId(TABLE_ID)
                    .build())
            .setSnapshotId(1L)
            .setIngestedAt(
                com.google.protobuf.util.Timestamps.fromMillis(
                    now.minus(Duration.ofDays(3)).toEpochMilli()))
            .build()
            .toByteArray(),
        "application/x-protobuf");
    String snapshotPointer = Keys.snapshotPointerById(ACCOUNT_ID, TABLE_ID, 1L);
    putPointer(snapshotPointer, snapshotBlob);

    gc.runForAccount(ACCOUNT_ID, System.currentTimeMillis() + 5_000L);

    assertTrue(pointers.get(snapshotPointer).isEmpty());
  }

  @Test
  void deletesStaleSecondaryPointer() {
    System.setProperty("floecat.gc.pointer.min-age-ms", "0");
    String blobA = Keys.tableBlobUri(ACCOUNT_ID, TABLE_ID, "sha-a");
    String blobB = Keys.tableBlobUri(ACCOUNT_ID, TABLE_ID, "sha-b");
    blobs.put(blobA, "a".getBytes(StandardCharsets.UTF_8), "text/plain");
    blobs.put(blobB, "b".getBytes(StandardCharsets.UTF_8), "text/plain");

    String canonicalPtr = Keys.tablePointerById(ACCOUNT_ID, TABLE_ID);
    putPointer(canonicalPtr, blobA);

    String secondaryPtr = Keys.tablePointerByName(ACCOUNT_ID, "cat-1", "ns-1", "tbl_name");
    putPointer(secondaryPtr, blobB);

    gc.runForAccount(ACCOUNT_ID, System.currentTimeMillis() + 5_000L);

    assertTrue(pointers.get(canonicalPtr).isPresent());
    assertTrue(pointers.get(secondaryPtr).isEmpty());
  }

  @Test
  void deletesStatsPointerWithMissingBlob() {
    System.setProperty("floecat.gc.pointer.min-age-ms", "0");
    String tableBlob = Keys.tableBlobUri(ACCOUNT_ID, TABLE_ID, "sha-table");
    blobs.put(tableBlob, "table".getBytes(StandardCharsets.UTF_8), "text/plain");
    putPointer(Keys.tablePointerById(ACCOUNT_ID, TABLE_ID), tableBlob);

    long snapshotId = 7L;
    String generationId = "gen-1";
    String targetId = StatsTargetIdentity.storageId(StatsTargetIdentity.tableTarget());
    String statsPtr =
        Keys.snapshotTargetStatsGenerationPointer(
            ACCOUNT_ID, TABLE_ID, snapshotId, generationId, targetId);
    String statsBlob =
        Keys.snapshotTargetStatsBlobUri(
            ACCOUNT_ID, TABLE_ID, snapshotId, generationId, targetId, "sha-stats");
    putPointer(statsPtr, statsBlob);

    gc.runForAccount(ACCOUNT_ID, System.currentTimeMillis() + 5_000L);

    assertTrue(pointers.get(statsPtr).isEmpty());
  }

  @Test
  void deletesStaleCatalogOverlaySecondaryPointer() {
    System.setProperty("floecat.gc.pointer.min-age-ms", "0");
    String currentBlob = Keys.catalogOverlayBlobUri(ACCOUNT_ID, "overlay-1", "sha-current");
    String staleBlob = Keys.catalogOverlayBlobUri(ACCOUNT_ID, "overlay-1", "sha-stale");
    blobs.put(currentBlob, "current".getBytes(StandardCharsets.UTF_8), "text/plain");
    blobs.put(staleBlob, "stale".getBytes(StandardCharsets.UTF_8), "text/plain");
    String canonical = Keys.catalogOverlayPointerById(ACCOUNT_ID, "overlay-1");
    String secondary =
        Keys.catalogOverlayPointerByIntegration(ACCOUNT_ID, "integration-1", "overlay-1");
    putPointer(canonical, currentBlob);
    putPointer(secondary, staleBlob);

    gc.runForAccount(ACCOUNT_ID, System.currentTimeMillis() + 5_000L);

    assertTrue(pointers.get(canonical).isPresent());
    assertTrue(pointers.get(secondary).isEmpty());
  }

  @Test
  void deletesStaleCatalogOverlayByCatalogPointer() {
    System.setProperty("floecat.gc.pointer.min-age-ms", "0");
    String currentBlob = Keys.catalogOverlayBlobUri(ACCOUNT_ID, "overlay-1", "sha-current");
    String staleBlob = Keys.catalogOverlayBlobUri(ACCOUNT_ID, "overlay-1", "sha-stale");
    blobs.put(currentBlob, "current".getBytes(StandardCharsets.UTF_8), "text/plain");
    blobs.put(staleBlob, "stale".getBytes(StandardCharsets.UTF_8), "text/plain");
    String canonical = Keys.catalogOverlayPointerById(ACCOUNT_ID, "overlay-1");
    String secondary = Keys.catalogOverlayPointerByCatalog(ACCOUNT_ID, "catalog-1", "overlay-1");
    putPointer(canonical, currentBlob);
    putPointer(secondary, staleBlob);

    gc.runForAccount(ACCOUNT_ID, System.currentTimeMillis() + 5_000L);

    assertTrue(pointers.get(canonical).isPresent());
    assertTrue(pointers.get(secondary).isEmpty());
  }

  private void putPointer(String key, String blobUri) {
    Pointer ptr = PointerReferences.blobPointer(key, blobUri, 1L);
    pointers.compareAndSet(key, 0L, ptr);
  }
}
