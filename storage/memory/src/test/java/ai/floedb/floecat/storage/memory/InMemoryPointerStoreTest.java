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
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.storage.spi.PointerStore.CasCheck;
import ai.floedb.floecat.storage.spi.PointerStore.CasCheckAbsent;
import ai.floedb.floecat.storage.spi.PointerStore.CasUpsert;
import java.util.List;
import org.junit.jupiter.api.Test;

class InMemoryPointerStoreTest {

  @Test
  void casCheckHonoursTheExpectedVersionWithoutWritingTheCheckedKey() {
    InMemoryPointerStore store = new InMemoryPointerStore();
    store.compareAndSet("/fence", 0L, Pointer.newBuilder().setBlobUri("owned").build());
    Pointer next = Pointer.newBuilder().setBlobUri("s3://k").build();

    assertFalse(
        store.compareAndSetBatch(
            List.of(new CasCheck("/fence", 2L), new CasUpsert("/k", 0L, next))),
        "stale version");
    assertTrue(store.get("/k").isEmpty(), "all-or-nothing");
    assertFalse(
        store.compareAndSetBatch(
            List.of(new CasCheck("/absent", 1L), new CasUpsert("/k", 0L, next))),
        "a check on a missing key never passes");
    assertTrue(
        store.compareAndSetBatch(
            List.of(new CasCheck("/fence", 1L), new CasUpsert("/k", 0L, next))));

    assertEquals(1L, store.get("/fence").orElseThrow().getVersion());
    assertEquals("owned", store.get("/fence").orElseThrow().getBlobUri());
    assertEquals(1L, store.get("/k").orElseThrow().getVersion());
  }

  @Test
  void casCheckAbsentFailsOncePresent() {
    InMemoryPointerStore store = new InMemoryPointerStore();
    Pointer next = Pointer.newBuilder().setBlobUri("s3://k").build();

    assertTrue(
        store.compareAndSetBatch(
            List.of(new CasCheckAbsent("/fence"), new CasUpsert("/k", 0L, next))));
    store.compareAndSet("/fence", 0L, Pointer.newBuilder().setBlobUri("owned").build());
    assertFalse(
        store.compareAndSetBatch(
            List.of(new CasCheckAbsent("/fence"), new CasUpsert("/k", 1L, next))));
    assertEquals(1L, store.get("/k").orElseThrow().getVersion());
  }
}
