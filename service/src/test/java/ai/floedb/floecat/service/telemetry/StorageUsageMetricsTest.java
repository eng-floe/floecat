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

package ai.floedb.floecat.service.telemetry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.telemetry.TestObservability;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class StorageUsageMetricsTest {

  @Test
  void pointerMutationsMaintainExactIncrementalUsageWithoutRefreshScans() {
    InMemoryPointerStore delegate = new InMemoryPointerStore();
    InMemoryBlobStore blobs = new InMemoryBlobStore();
    StorageAccountingPointerStore accounting = accounting(delegate, blobs);
    blobs.put("/accounts/acct/blobs/one", new byte[5], "application/octet-stream");
    blobs.put("/accounts/acct/blobs/two", new byte[4], "application/octet-stream");

    assertTrue(
        accounting.compareAndSet(
            "/accounts/acct/one",
            0L,
            PointerReferences.blobPointer("/accounts/acct/one", "/accounts/acct/blobs/one", 1L)));
    assertTrue(
        accounting.compareAndSet(
            "/accounts/acct/two",
            0L,
            PointerReferences.blobPointer("/accounts/acct/two", "/accounts/acct/blobs/one", 1L)));
    assertEquals(new StorageAccountingPointerStore.AccountUsage(2L, 10L), usage(delegate));

    assertTrue(
        accounting.compareAndSet(
            "/accounts/acct/two",
            1L,
            PointerReferences.blobPointer("/accounts/acct/two", "/accounts/acct/blobs/two", 2L)));
    assertEquals(new StorageAccountingPointerStore.AccountUsage(2L, 9L), usage(delegate));

    assertTrue(accounting.compareAndDelete("/accounts/acct/two", 2L));
    assertEquals(new StorageAccountingPointerStore.AccountUsage(1L, 5L), usage(delegate));
  }

  @Test
  void batchAccountingIsAppliedOnlyAfterSuccessfulBatch() {
    InMemoryPointerStore delegate = new InMemoryPointerStore();
    InMemoryBlobStore blobs = new InMemoryBlobStore();
    StorageAccountingPointerStore accounting = accounting(delegate, blobs);
    blobs.put("/accounts/acct/blobs/value", new byte[7], "application/octet-stream");

    assertTrue(
        accounting.compareAndSetBatch(
            List.of(
                new PointerStore.CasUpsert(
                    "/accounts/acct/a",
                    0L,
                    PointerReferences.blobPointer(
                        "/accounts/acct/a", "/accounts/acct/blobs/value", 1L)),
                new PointerStore.CasUpsert(
                    "/accounts/acct/b",
                    0L,
                    PointerReferences.blobPointer(
                        "/accounts/acct/b", "/accounts/acct/blobs/value", 1L)))));

    assertEquals(new StorageAccountingPointerStore.AccountUsage(2L, 14L), usage(delegate));
  }

  @Test
  void rebuildStateAndUsagePayloadsRoundTrip() {
    var state = new StorageUsageMetrics.RebuildState("cursor", 12L, 345L, true);
    var pointer =
        PointerReferences.opaqueMarkerPointer(
            "/accounts/acct/metrics/storage-usage-rebuild",
            StorageUsageMetrics.encodeRebuildState(state),
            1L);

    assertEquals(state, StorageUsageMetrics.decodeRebuildState(pointer));
    assertEquals(
        new StorageAccountingPointerStore.AccountUsage(12L, 345L),
        StorageAccountingPointerStore.decodeUsage(
            PointerReferences.opaqueMarkerPointer(
                "/accounts/acct/metrics/storage-usage",
                StorageAccountingPointerStore.encodeUsage(
                    new StorageAccountingPointerStore.AccountUsage(12L, 345L)),
                1L)));
  }

  @Test
  void completedRebuildMarkerIsReadOnlyOncePerProcess() {
    CountingPointerStore pointers = new CountingPointerStore();
    String completionKey = Keys.storageUsageRebuildCompletePointer();
    assertTrue(
        pointers.compareAndSet(
            completionKey, 0L, PointerReferences.opaqueMarkerPointer(completionKey, "v1", 1L)));
    pointers.gets.set(0);

    StorageUsageMetrics metrics = new StorageUsageMetrics();
    metrics.pointerStore = pointers;
    metrics.observability = new TestObservability();

    metrics.rebuildExistingUsage();
    metrics.rebuildExistingUsage();

    assertEquals(1, pointers.gets.get());
  }

  private static StorageAccountingPointerStore accounting(
      InMemoryPointerStore delegate, InMemoryBlobStore blobs) {
    StorageAccountingPointerStore accounting = new StorageAccountingPointerStore() {};
    accounting.delegate = delegate;
    accounting.blobStore = blobs;
    return accounting;
  }

  private static StorageAccountingPointerStore.AccountUsage usage(InMemoryPointerStore delegate) {
    return StorageAccountingPointerStore.decodeUsage(
        delegate.get(Keys.accountStorageUsagePointer("acct")).orElse(null));
  }

  private static final class CountingPointerStore extends InMemoryPointerStore {
    private final AtomicInteger gets = new AtomicInteger();

    @Override
    public java.util.Optional<ai.floedb.floecat.common.rpc.Pointer> get(String key) {
      gets.incrementAndGet();
      return super.get(key);
    }
  }
}
