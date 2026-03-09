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

package ai.floedb.floecat.gateway.iceberg.rest.services.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergCommitJournalEntry;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergCommitOutboxEntry;
import ai.floedb.floecat.storage.kv.Keys;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class TableCommitOutboxServiceTest {
  private final TableCommitOutboxService service = new TableCommitOutboxService();
  private final TableCommitSideEffectService sideEffectService =
      mock(TableCommitSideEffectService.class);
  private final TableCommitJournalService journalService = mock(TableCommitJournalService.class);
  private final PointerStore pointerStore = mock(PointerStore.class);
  private final BlobStore blobStore = mock(BlobStore.class);
  private final TableGatewaySupport tableSupport = mock(TableGatewaySupport.class);

  @BeforeEach
  void setUp() {
    service.sideEffectService = sideEffectService;
    service.commitJournalService = journalService;
    service.pointerStore = pointerStore;
    service.blobStore = blobStore;
    when(pointerStore.get(any())).thenReturn(Optional.empty());
  }

  @Test
  void processPendingNowClearsMarkerOnSuccess() {
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-1").build();
    String pendingKey = Keys.tableCommitOutboxPendingPointer(123L, "acct-1", "tbl-1", "tx-1");
    when(sideEffectService.pruneRemovedSnapshots(tableId, List.of(8L))).thenReturn(true);
    when(sideEffectService.runPostCommitStatsSyncAttempt(
            tableSupport, null, List.of("db"), "orders", List.of(7L)))
        .thenReturn(true);
    when(pointerStore.get(pendingKey))
        .thenReturn(
            Optional.of(Pointer.newBuilder().setBlobUri("/blob/outbox").setVersion(1L).build()));
    when(pointerStore.delete(pendingKey)).thenReturn(true);

    service.processPendingNow(
        tableSupport,
        List.of(
            new TableCommitOutboxService.WorkItem(
                pendingKey, List.of("db"), "orders", tableId, null, List.of(7L), List.of(8L))));

    verify(pointerStore).delete(pendingKey);
    verify(blobStore).delete("/blob/outbox");
  }

  @Test
  void processPendingNowLeavesMarkerWhenSideEffectsFail() {
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-1").build();
    String pendingKey = Keys.tableCommitOutboxPendingPointer(123L, "acct-1", "tbl-1", "tx-1");
    when(sideEffectService.pruneRemovedSnapshots(tableId, List.of())).thenReturn(true);
    when(sideEffectService.runPostCommitStatsSyncAttempt(
            tableSupport, null, List.of("db"), "orders", List.of(7L)))
        .thenReturn(false);
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
    when(sideEffectService.pruneRemovedSnapshots(any(), anyList())).thenReturn(true);
    when(sideEffectService.runPostCommitStatsSyncAttempt(
            tableSupport, null, List.of("db"), "orders", List.of(7L)))
        .thenReturn(true);
    when(pointerStore.get(pendingKey))
        .thenReturn(
            Optional.of(Pointer.newBuilder().setBlobUri("/blob/outbox").setVersion(1L).build()));
    when(pointerStore.delete(pendingKey)).thenReturn(true);

    service.drainPending(tableSupport, 10);

    verify(sideEffectService)
        .runPostCommitStatsSyncAttempt(tableSupport, null, List.of("db"), "orders", List.of(7L));
    verify(pointerStore).delete(pendingKey);
  }

  @Test
  void processPendingNowMovesToDeadLetterAfterMaxAttempts() {
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-1").build();
    String pendingKey = Keys.tableCommitOutboxPendingPointer(123L, "acct-1", "tbl-1", "tx-1");
    when(sideEffectService.pruneRemovedSnapshots(tableId, List.of())).thenReturn(true);
    when(sideEffectService.runPostCommitStatsSyncAttempt(
            tableSupport, null, List.of("db"), "orders", List.of(7L)))
        .thenReturn(false);
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

  private IcebergCommitOutboxEntry parseOutbox(byte[] payload) {
    try {
      return IcebergCommitOutboxEntry.parseFrom(payload);
    } catch (InvalidProtocolBufferException e) {
      throw new AssertionError(e);
    }
  }
}
