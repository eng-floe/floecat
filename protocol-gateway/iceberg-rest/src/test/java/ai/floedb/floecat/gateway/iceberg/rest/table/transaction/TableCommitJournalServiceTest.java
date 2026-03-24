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

package ai.floedb.floecat.gateway.iceberg.rest.table.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergCommitJournalEntry;
import ai.floedb.floecat.storage.kv.Keys;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.enterprise.inject.Instance;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class TableCommitJournalServiceTest {
  private final TableCommitJournalService service = new TableCommitJournalService();
  private final PointerStore pointerStore = mock(PointerStore.class);
  private final BlobStore blobStore = mock(BlobStore.class);

  TableCommitJournalServiceTest() {
    service.pointerStore = pointerStore;
    service.blobStore = blobStore;
  }

  @Test
  void getReturnsEmptyWhenPointerMissing() {
    when(pointerStore.get(Keys.tableCommitJournalPointer("acct-1", "tbl-1", "tx-1")))
        .thenReturn(Optional.empty());

    assertTrue(service.get("acct-1", "tbl-1", "tx-1").isEmpty());
  }

  @Test
  void getParsesStoredJournal() {
    IcebergCommitJournalEntry entry =
        IcebergCommitJournalEntry.newBuilder()
            .setVersion(1)
            .setTxId("tx-1")
            .setRequestHash("hash")
            .setTableId(ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-1").build())
            .setTableName("orders")
            .build();
    when(pointerStore.get(Keys.tableCommitJournalPointer("acct-1", "tbl-1", "tx-1")))
        .thenReturn(
            Optional.of(Pointer.newBuilder().setBlobUri("/blob/journal").setVersion(1L).build()));
    when(blobStore.get("/blob/journal")).thenReturn(entry.toByteArray());

    var loaded = service.get("acct-1", "tbl-1", "tx-1");

    assertTrue(loaded.isPresent());
    assertEquals("tx-1", loaded.get().getTxId());
    assertEquals("tbl-1", loaded.get().getTableId().getId());
  }

  @Test
  void getThrowsWhenPayloadIsUnreadable() {
    when(pointerStore.get(Keys.tableCommitJournalPointer("acct-1", "tbl-1", "tx-1")))
        .thenReturn(
            Optional.of(Pointer.newBuilder().setBlobUri("/blob/journal").setVersion(1L).build()));
    when(blobStore.get("/blob/journal")).thenReturn(new byte[] {1, 2, 3});

    assertThrows(IllegalStateException.class, () -> service.get("acct-1", "tbl-1", "tx-1"));
  }

  @Test
  void getCachesResolvedStores() {
    TableCommitJournalService uncached = new TableCommitJournalService();
    @SuppressWarnings("unchecked")
    Instance<PointerStore> pointerStores = mock(Instance.class);
    @SuppressWarnings("unchecked")
    Instance<BlobStore> blobStores = mock(Instance.class);
    uncached.pointerStores = pointerStores;
    uncached.blobStores = blobStores;
    when(pointerStores.isResolvable()).thenReturn(true);
    when(blobStores.isResolvable()).thenReturn(true);
    when(pointerStores.get()).thenReturn(pointerStore);
    when(blobStores.get()).thenReturn(blobStore);
    when(pointerStore.get(Keys.tableCommitJournalPointer("acct-1", "tbl-1", "tx-1")))
        .thenReturn(Optional.empty());

    uncached.get("acct-1", "tbl-1", "tx-1");
    uncached.get("acct-1", "tbl-1", "tx-1");

    verify(pointerStores, times(1)).get();
    verify(blobStores, times(1)).get();
    assertTrue(uncached.pointerStore == pointerStore);
    assertTrue(uncached.blobStore == blobStore);
  }
}
