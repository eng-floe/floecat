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

package ai.floedb.floecat.storage.memory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Version emulation contract: every put mints a new per-key monotonic version (exposed as {@code
 * BlobHeader.versionId}) and a version-targeted delete touches ONLY the named version — mirroring
 * an S3 versioned bucket, so the CAS-GC check-then-delete race is deterministically testable.
 */
class InMemoryBlobStoreVersioningTest {

  private static final byte[] BYTES = "same".getBytes(StandardCharsets.UTF_8);

  @Test
  void everyPutMintsAMonotonicVersionEvenForIdenticalContent() {
    var store = new InMemoryBlobStore();
    store.put("/k", BYTES, "text/plain");
    assertEquals("1", store.head("/k").orElseThrow().getVersionId());

    store.put("/k", BYTES, "text/plain");
    assertEquals("2", store.head("/k").orElseThrow().getVersionId());
  }

  @Test
  void versionTargetedDeleteRefusesASupersededVersion() {
    var store = new InMemoryBlobStore();
    store.put("/k", BYTES, "text/plain");
    String observed = store.head("/k").orElseThrow().getVersionId();
    store.put("/k", BYTES, "text/plain"); // concurrent re-write after the observation

    assertFalse(store.delete("/k", observed), "a superseded version must not be deletable");
    assertTrue(store.head("/k").isPresent(), "the newer version survives");

    assertTrue(store.delete("/k", store.head("/k").orElseThrow().getVersionId()));
    assertTrue(store.head("/k").isEmpty());
  }

  @Test
  void aStaleVersionIdCannotDeleteARecreatedBlob() {
    // Version ids are store-wide unique and never reused: a delete-and-recreate of the same key
    // must not resurrect an old id, or a stale versioned delete (the GC's) would remove the
    // recreation — the ABA shape of the eng-floe/core#1904 race.
    var store = new InMemoryBlobStore();
    store.put("/k", BYTES, "text/plain");
    String observed = store.head("/k").orElseThrow().getVersionId();
    assertTrue(store.delete("/k"));
    store.put("/k", BYTES, "text/plain");

    assertFalse(store.delete("/k", observed), "a pre-delete version id must not match again");
    assertTrue(store.head("/k").isPresent(), "the recreated blob survives the stale delete");
  }

  @Test
  void modelsAVersioningEnabledBucket() {
    assertTrue(new InMemoryBlobStore().supportsVersionedDeletes());
  }

  @Test
  void aBlankVersionIdIsRejectedNotDegradedToUnconditional() {
    var store = new InMemoryBlobStore();
    store.put("/k", BYTES, "text/plain");

    assertThrows(IllegalArgumentException.class, () -> store.delete("/k", ""));
    assertTrue(store.head("/k").isPresent(), "nothing was deleted");
  }

  @Test
  void versionTargetedDeleteOfAnAbsentKeyCountsAsDeleted() {
    // SPI contract (and S3, which maps the 404 to true): the named version being already absent
    // means the caller's goal state holds.
    var store = new InMemoryBlobStore();
    assertTrue(store.delete("/accounts/a/tables/t/table/never-existed.pb", "42"));
  }
}
