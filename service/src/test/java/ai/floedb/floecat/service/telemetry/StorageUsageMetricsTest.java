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
import java.util.List;
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
}
