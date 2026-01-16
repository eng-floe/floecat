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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.BlobStore;
import ai.floedb.floecat.storage.InMemoryBlobStore;
import ai.floedb.floecat.storage.InMemoryPointerStore;
import ai.floedb.floecat.storage.PointerStore;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CasBlobGcTest {

  private static final String ACCOUNT_ID = "acct-1";
  private static final String TABLE_ID = "tbl-1";

  private PointerStore pointers;
  private BlobStore blobs;
  private CasBlobGc gc;

  @BeforeEach
  void setUp() {
    System.setProperty("floecat.gc.cas.min-age-ms", "0");
    System.setProperty("floecat.gc.cas.page-size", "200");
    pointers = new InMemoryPointerStore();
    blobs = new InMemoryBlobStore();
    gc = new CasBlobGc();
    gc.pointerStore = pointers;
    gc.blobStore = blobs;
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

    gc.runForAccount(ACCOUNT_ID);

    assertFalse(blobs.head(blobUri).isPresent());
  }

  @Test
  void statsBlobsGcHonorsPointers() {
    long snapshotId = 1L;
    String statsBlob = Keys.snapshotTableStatsBlobUri(ACCOUNT_ID, TABLE_ID, "sha-stats");
    String statsPtr = Keys.snapshotTableStatsPointer(ACCOUNT_ID, TABLE_ID, snapshotId);

    String tableBlob = Keys.tableBlobUri(ACCOUNT_ID, TABLE_ID, "sha-table");
    blobs.put(tableBlob, "table".getBytes(StandardCharsets.UTF_8), "text/plain");
    putPointer(Keys.tablePointerById(ACCOUNT_ID, TABLE_ID), tableBlob);

    blobs.put(statsBlob, "stats".getBytes(StandardCharsets.UTF_8), "text/plain");
    putPointer(statsPtr, statsBlob);

    gc.runForAccount(ACCOUNT_ID);
    assertTrue(blobs.head(statsBlob).isPresent());

    pointers.delete(statsPtr);
    gc.runForAccount(ACCOUNT_ID);
    assertFalse(blobs.head(statsBlob).isPresent());
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

  private void putPointer(String key, String blobUri) {
    Pointer ptr = Pointer.newBuilder().setKey(key).setBlobUri(blobUri).setVersion(1L).build();
    pointers.compareAndSet(key, 0L, ptr);
  }
}
