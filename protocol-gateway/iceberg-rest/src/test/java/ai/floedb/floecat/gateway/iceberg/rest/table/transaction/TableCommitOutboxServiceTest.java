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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergCommitJournalEntry;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergCommitOutboxEntry;
import ai.floedb.floecat.storage.kv.Keys;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.google.protobuf.InvalidProtocolBufferException;
import jakarta.enterprise.inject.Instance;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class TableCommitOutboxServiceTest {
  private final TableCommitOutboxService service = spy(new TableCommitOutboxService());
  private final TableCommitJournalService journalService = mock(TableCommitJournalService.class);
  private final PointerStore pointerStore = mock(PointerStore.class);
  private final BlobStore blobStore = mock(BlobStore.class);
  private final TableGatewaySupport tableSupport = mock(TableGatewaySupport.class);

  @BeforeEach
  void setUp() {
    service.commitJournalService = journalService;
    service.pointerStore = pointerStore;
    service.blobStore = blobStore;
    when(pointerStore.get(any())).thenReturn(Optional.empty());
  }

  @Test
  void processPendingNowClearsMarkerOnSuccess() {
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-1").build();
    String pendingKey = Keys.tableCommitOutboxPendingPointer(123L, "acct-1", "tbl-1", "tx-1");
    doReturn(true).when(service).pruneRemovedSnapshots(tableId, List.of(8L));
    doReturn(true)
        .when(service)
        .runPostCommitStatsSyncAttempt(tableSupport, null, List.of("db"), "orders", List.of(7L));
    when(pointerStore.get(pendingKey))
        .thenReturn(
            Optional.of(Pointer.newBuilder().setBlobUri("/blob/outbox").setVersion(1L).build()));
    when(pointerStore.compareAndDelete(pendingKey, 1L)).thenReturn(true);

    service.processPendingNow(
        tableSupport,
        List.of(
            new TableCommitOutboxService.WorkItem(
                pendingKey, List.of("db"), "orders", tableId, null, List.of(7L), List.of(8L))));

    verify(pointerStore).compareAndDelete(pendingKey, 1L);
    verify(blobStore).delete("/blob/outbox");
  }

  @Test
  void processPendingNowLeavesMarkerWhenSideEffectsFail() {
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-1").build();
    String pendingKey = Keys.tableCommitOutboxPendingPointer(123L, "acct-1", "tbl-1", "tx-1");
    doReturn(true).when(service).pruneRemovedSnapshots(tableId, List.of());
    doReturn(false)
        .when(service)
        .runPostCommitStatsSyncAttempt(tableSupport, null, List.of("db"), "orders", List.of(7L));
    when(pointerStore.get(pendingKey))
        .thenReturn(
            Optional.of(Pointer.newBuilder().setBlobUri("/blob/outbox").setVersion(3L).build()));
    when(blobStore.get("/blob/outbox"))
        .thenReturn(
            IcebergCommitOutboxEntry.newBuilder()
                .setVersion(1)
                .setTxId("tx-1")
                .setRequestHash("hash")
                .setAccountId("acct-1")
                .setTableId("tbl-1")
                .setCreatedAtMs(123L)
                .build()
                .toByteArray());
    when(pointerStore.compareAndSet(eq(pendingKey), eq(3L), any())).thenReturn(true);

    service.processPendingNow(
        tableSupport,
        List.of(
            new TableCommitOutboxService.WorkItem(
                pendingKey, List.of("db"), "orders", tableId, null, List.of(7L), List.of())));

    verify(pointerStore, never()).delete(any());
    verify(blobStore, never()).delete(any());
    verify(blobStore).put(eq("/blob/outbox"), any(), eq("application/x-protobuf"));
    verify(pointerStore).compareAndSet(eq(pendingKey), eq(3L), any());
  }

  @Test
  void drainPendingLoadsJournalAndProcessesWork() {
    String pendingKey = Keys.tableCommitOutboxPendingPointer(123L, "acct-1", "tbl-1", "tx-1");
    when(pointerStore.listPointersByPrefix(
            eq(Keys.tableCommitOutboxPendingScanPrefix()), eq(10), eq(""), any()))
        .thenReturn(
            List.of(
                Pointer.newBuilder()
                    .setKey(pendingKey)
                    .setBlobUri("/blob/outbox")
                    .setVersion(1L)
                    .build()));
    when(blobStore.get("/blob/outbox"))
        .thenReturn(
            IcebergCommitOutboxEntry.newBuilder()
                .setVersion(1)
                .setTxId("tx-1")
                .setRequestHash("hash")
                .setAccountId("acct-1")
                .setTableId("tbl-1")
                .setCreatedAtMs(123L)
                .build()
                .toByteArray());
    when(journalService.get("acct-1", "tbl-1", "tx-1"))
        .thenReturn(
            Optional.of(
                IcebergCommitJournalEntry.newBuilder()
                    .setVersion(1)
                    .setTxId("tx-1")
                    .setRequestHash("hash")
                    .setTableId(
                        ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-1").build())
                    .addNamespacePath("db")
                    .setTableName("orders")
                    .addAddedSnapshotIds(7L)
                    .build()));
    doReturn(true).when(service).pruneRemovedSnapshots(any(), anyList());
    doReturn(true)
        .when(service)
        .runPostCommitStatsSyncAttempt(tableSupport, null, List.of("db"), "orders", List.of(7L));
    when(pointerStore.get(pendingKey))
        .thenReturn(
            Optional.of(Pointer.newBuilder().setBlobUri("/blob/outbox").setVersion(1L).build()));
    when(pointerStore.compareAndDelete(pendingKey, 1L)).thenReturn(true);

    service.drainPending(tableSupport, 10);

    verify(service)
        .runPostCommitStatsSyncAttempt(tableSupport, null, List.of("db"), "orders", List.of(7L));
    verify(pointerStore).compareAndDelete(pendingKey, 1L);
  }

  @Test
  void loadPendingWorkItemsForTxUsesPersistedOutboxAndJournalState() {
    String matchingKey = Keys.tableCommitOutboxPendingPointer(123L, "acct-1", "tbl-1", "tx-1");
    String otherTxKey = Keys.tableCommitOutboxPendingPointer(123L, "acct-1", "tbl-2", "tx-2");
    when(pointerStore.listPointersByPrefix(
            eq(Keys.tableCommitOutboxPendingPrefix("acct-1") + "0000000000000000123/"),
            eq(100),
            eq(""),
            any()))
        .thenReturn(
            List.of(
                Pointer.newBuilder().setKey(matchingKey).setBlobUri("/blob/match").build(),
                Pointer.newBuilder().setKey(otherTxKey).setBlobUri("/blob/other").build()));
    when(blobStore.get("/blob/match"))
        .thenReturn(
            IcebergCommitOutboxEntry.newBuilder()
                .setVersion(1)
                .setTxId("tx-1")
                .setRequestHash("hash-1")
                .setAccountId("acct-1")
                .setTableId("tbl-1")
                .setCreatedAtMs(123L)
                .build()
                .toByteArray());
    when(blobStore.get("/blob/other"))
        .thenReturn(
            IcebergCommitOutboxEntry.newBuilder()
                .setVersion(1)
                .setTxId("tx-2")
                .setRequestHash("hash-1")
                .setAccountId("acct-1")
                .setTableId("tbl-2")
                .setCreatedAtMs(123L)
                .build()
                .toByteArray());
    when(journalService.get("acct-1", "tbl-1", "tx-1"))
        .thenReturn(
            Optional.of(
                IcebergCommitJournalEntry.newBuilder()
                    .setVersion(1)
                    .setTxId("tx-1")
                    .setRequestHash("hash-1")
                    .setTableId(
                        ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-1").build())
                    .addNamespacePath("db")
                    .setTableName("orders")
                    .addAddedSnapshotIds(7L)
                    .build()));

    List<TableCommitOutboxService.WorkItem> items =
        service.loadPendingWorkItemsForTx("acct-1", "tx-1", 123L, "hash-1");

    assertEquals(1, items.size());
    assertEquals(matchingKey, items.getFirst().pendingKey());
    assertEquals("orders", items.getFirst().tableName());
  }

  @Test
  void drainPendingMarksRetryWhenJournalIsMissing() {
    String pendingKey = Keys.tableCommitOutboxPendingPointer(123L, "acct-1", "tbl-1", "tx-1");
    when(pointerStore.listPointersByPrefix(
            eq(Keys.tableCommitOutboxPendingScanPrefix()), eq(10), eq(""), any()))
        .thenReturn(
            List.of(
                Pointer.newBuilder()
                    .setKey(pendingKey)
                    .setBlobUri("/blob/outbox")
                    .setVersion(3L)
                    .build()));
    when(blobStore.get("/blob/outbox"))
        .thenReturn(
            IcebergCommitOutboxEntry.newBuilder()
                .setVersion(1)
                .setTxId("tx-1")
                .setRequestHash("hash")
                .setAccountId("acct-1")
                .setTableId("tbl-1")
                .setCreatedAtMs(123L)
                .build()
                .toByteArray());
    when(journalService.get("acct-1", "tbl-1", "tx-1")).thenReturn(Optional.empty());
    when(pointerStore.get(pendingKey))
        .thenReturn(
            Optional.of(
                Pointer.newBuilder()
                    .setKey(pendingKey)
                    .setBlobUri("/blob/outbox")
                    .setVersion(3L)
                    .build()));
    when(pointerStore.compareAndSet(eq(pendingKey), eq(3L), any())).thenReturn(true);

    service.drainPending(tableSupport, 10);

    verify(blobStore).put(eq("/blob/outbox"), any(), eq("application/x-protobuf"));
    verify(pointerStore).compareAndSet(eq(pendingKey), eq(3L), any());
  }

  @Test
  void drainPendingDropsMissingOutboxBlob() {
    String pendingKey = Keys.tableCommitOutboxPendingPointer(123L, "acct-1", "tbl-1", "tx-1");
    when(pointerStore.listPointersByPrefix(
            eq(Keys.tableCommitOutboxPendingScanPrefix()), eq(10), eq(""), any()))
        .thenReturn(
            List.of(
                Pointer.newBuilder()
                    .setKey(pendingKey)
                    .setBlobUri("/blob/missing")
                    .setVersion(3L)
                    .build()));
    when(blobStore.get("/blob/missing")).thenReturn(null);
    when(pointerStore.compareAndDelete(pendingKey, 3L)).thenReturn(true);

    service.drainPending(tableSupport, 10);

    verify(pointerStore).compareAndDelete(pendingKey, 3L);
    verify(blobStore).delete("/blob/missing");
  }

  @Test
  void drainPendingDropsUnreadableOutboxPayload() {
    String pendingKey = Keys.tableCommitOutboxPendingPointer(123L, "acct-1", "tbl-1", "tx-1");
    when(pointerStore.listPointersByPrefix(
            eq(Keys.tableCommitOutboxPendingScanPrefix()), eq(10), eq(""), any()))
        .thenReturn(
            List.of(
                Pointer.newBuilder()
                    .setKey(pendingKey)
                    .setBlobUri("/blob/corrupt")
                    .setVersion(3L)
                    .build()));
    when(blobStore.get("/blob/corrupt")).thenReturn(new byte[] {0x01, 0x02, 0x03});
    when(pointerStore.compareAndDelete(pendingKey, 3L)).thenReturn(true);

    service.drainPending(tableSupport, 10);

    verify(pointerStore).compareAndDelete(pendingKey, 3L);
    verify(blobStore).delete("/blob/corrupt");
  }

  @Test
  void drainPendingLeavesPendingOnTransientBlobReadFailure() {
    String pendingKey = Keys.tableCommitOutboxPendingPointer(123L, "acct-1", "tbl-1", "tx-1");
    when(pointerStore.listPointersByPrefix(
            eq(Keys.tableCommitOutboxPendingScanPrefix()), eq(10), eq(""), any()))
        .thenReturn(
            List.of(
                Pointer.newBuilder()
                    .setKey(pendingKey)
                    .setBlobUri("/blob/outbox")
                    .setVersion(3L)
                    .build()));
    when(blobStore.get("/blob/outbox"))
        .thenThrow(new IllegalStateException("temporary read failure"));

    service.drainPending(tableSupport, 10);

    verify(pointerStore, never()).delete(pendingKey);
    verify(blobStore, never()).delete("/blob/outbox");
  }

  @Test
  void processPendingNowMovesToDeadLetterAfterMaxAttempts() {
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-1").build();
    String pendingKey = Keys.tableCommitOutboxPendingPointer(123L, "acct-1", "tbl-1", "tx-1");
    doReturn(true).when(service).pruneRemovedSnapshots(tableId, List.of());
    doReturn(false)
        .when(service)
        .runPostCommitStatsSyncAttempt(tableSupport, null, List.of("db"), "orders", List.of(7L));
    when(pointerStore.get(pendingKey))
        .thenReturn(
            Optional.of(Pointer.newBuilder().setBlobUri("/blob/outbox").setVersion(9L).build()));
    when(blobStore.get("/blob/outbox"))
        .thenReturn(
            IcebergCommitOutboxEntry.newBuilder()
                .setVersion(1)
                .setTxId("tx-1")
                .setRequestHash("hash")
                .setAccountId("acct-1")
                .setTableId("tbl-1")
                .setCreatedAtMs(123L)
                .setAttemptCount(5)
                .build()
                .toByteArray());
    when(pointerStore.compareAndSetBatch(anyList())).thenReturn(true);

    service.processPendingNow(
        tableSupport,
        List.of(
            new TableCommitOutboxService.WorkItem(
                pendingKey, List.of("db"), "orders", tableId, null, List.of(7L), List.of())));

    verify(pointerStore, never()).compareAndSet(eq(pendingKey), eq(9L), any());
    verify(pointerStore, times(1)).compareAndSetBatch(anyList());
    ArgumentCaptor<byte[]> payloadCaptor = ArgumentCaptor.forClass(byte[].class);
    verify(blobStore)
        .put(eq("/blob/outbox"), payloadCaptor.capture(), eq("application/x-protobuf"));
    IcebergCommitOutboxEntry updated = parseOutbox(payloadCaptor.getValue());
    assertEquals(6, updated.getAttemptCount());
    assertEquals(0L, updated.getNextAttemptAtMs());
    assertTrue(updated.getDeadLetteredAtMs() > 0);
  }

  @Test
  void processPendingNowDoesNotDeadLetterWhenRetryBlobPersistenceFails() {
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-1").build();
    String pendingKey = Keys.tableCommitOutboxPendingPointer(123L, "acct-1", "tbl-1", "tx-1");
    doReturn(true).when(service).pruneRemovedSnapshots(tableId, List.of());
    doReturn(false)
        .when(service)
        .runPostCommitStatsSyncAttempt(tableSupport, null, List.of("db"), "orders", List.of(7L));
    when(pointerStore.get(pendingKey))
        .thenReturn(
            Optional.of(Pointer.newBuilder().setBlobUri("/blob/outbox").setVersion(9L).build()));
    when(blobStore.get("/blob/outbox"))
        .thenReturn(
            IcebergCommitOutboxEntry.newBuilder()
                .setVersion(1)
                .setTxId("tx-1")
                .setRequestHash("hash")
                .setAccountId("acct-1")
                .setTableId("tbl-1")
                .setCreatedAtMs(123L)
                .setAttemptCount(5)
                .build()
                .toByteArray());
    doThrow(new IllegalStateException("blob put failed"))
        .when(blobStore)
        .put(eq("/blob/outbox"), any(), eq("application/x-protobuf"));

    service.processPendingNow(
        tableSupport,
        List.of(
            new TableCommitOutboxService.WorkItem(
                pendingKey, List.of("db"), "orders", tableId, null, List.of(7L), List.of())));

    verify(pointerStore, never()).compareAndSetBatch(anyList());
    verify(pointerStore, never()).compareAndSet(eq(pendingKey), eq(9L), any());
  }

  @Test
  void isPendingCachesResolvedStores() {
    TableCommitOutboxService uncached = new TableCommitOutboxService();
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
    when(pointerStore.get("pending-key")).thenReturn(Optional.empty());

    uncached.isPending("pending-key");
    uncached.isPending("pending-key");

    verify(pointerStores, times(1)).get();
    verify(blobStores, never()).get();
    assertTrue(uncached.pointerStore == pointerStore);
  }

  private IcebergCommitOutboxEntry parseOutbox(byte[] payload) {
    try {
      return IcebergCommitOutboxEntry.parseFrom(payload);
    } catch (InvalidProtocolBufferException e) {
      throw new AssertionError(e);
    }
  }
}
