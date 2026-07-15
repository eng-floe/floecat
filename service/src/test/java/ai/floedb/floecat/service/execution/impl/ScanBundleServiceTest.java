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

package ai.floedb.floecat.service.execution.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.query.rpc.PinKind;
import ai.floedb.floecat.query.rpc.TableInfo;
import ai.floedb.floecat.query.rpc.TablePin;
import ai.floedb.floecat.service.query.impl.ScanSession;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.storage.impl.ServerSideFileIoPropertiesResolver;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.stats.spi.StatsStore.StatsStorePage;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ScanBundleServiceTest {

  private static final ResourceId TABLE_ID =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setKind(ResourceKind.RK_TABLE)
          .setId("tbl")
          .build();

  private static final String TABLE_BLOB_URI = "s3://bucket/table/blob-v1.pb";
  private static final String SNAPSHOT_BLOB_URI = "s3://bucket/table/snap-42.pb";

  private static final TablePin PIN =
      TablePin.newBuilder()
          .setTableId(TABLE_ID)
          .setPinKind(PinKind.PIN_KIND_CURRENT)
          .setSnapshotId(42L)
          .setTableBlobUri(TABLE_BLOB_URI)
          .setTableBlobVersion("etag-t1")
          .setSnapshotBlobUri(SNAPSHOT_BLOB_URI)
          .setSnapshotBlobVersion("etag-s42")
          .build();

  private TableRepository tableRepo;
  private SnapshotRepository snapshotRepo;
  private ScanBundleService service;
  private ServerSideFileIoPropertiesResolver resolver;
  private StatsStore statsStore;

  @BeforeEach
  void setUp() {
    tableRepo = mock(TableRepository.class);
    snapshotRepo = mock(SnapshotRepository.class);
    resolver = mock(ServerSideFileIoPropertiesResolver.class);
    statsStore = mock(StatsStore.class);
    service = new ScanBundleService(tableRepo, snapshotRepo, statsStore, resolver);
  }

  private void stubPinnedBlobsPresent() {
    when(tableRepo.getByBlobUriLive(TABLE_BLOB_URI))
        .thenReturn(Optional.of(Table.newBuilder().setResourceId(TABLE_ID).build()));
    when(snapshotRepo.getByBlobUriLive(SNAPSHOT_BLOB_URI))
        .thenReturn(
            Optional.of(Snapshot.newBuilder().setTableId(TABLE_ID).setSnapshotId(42L).build()));
    when(resolver.applyToTableProperties(any(), any(), any())).thenReturn(Map.of());
  }

  @Test
  void initScanBuildsTableInfoFromPinnedBlobsOnly() {
    var table =
        Table.newBuilder()
            .setResourceId(TABLE_ID)
            .setSchemaJson("{\"type\":\"struct\",\"fields\":[]}")
            .putProperties("storage_location", "s3://bucket/table")
            .build();
    var snapshot =
        Snapshot.newBuilder()
            .setTableId(TABLE_ID)
            .setSnapshotId(42L)
            .setSchemaJson("{\"type\":\"struct\",\"fields\":[]}")
            .setMetadataLocation("s3://bucket/table/metadata/00001.metadata.json")
            .build();
    when(tableRepo.getByBlobUriLive(TABLE_BLOB_URI)).thenReturn(Optional.of(table));
    when(snapshotRepo.getByBlobUriLive(SNAPSHOT_BLOB_URI)).thenReturn(Optional.of(snapshot));
    when(resolver.applyToTableProperties(table, null, table.getPropertiesMap()))
        .thenReturn(Map.of("s3.region", "us-east-1"));

    var init = service.initScan("corr", PIN);

    // Both reads come from the pinned immutable blobs, never the live pointers.
    verify(tableRepo).getByBlobUriLive(TABLE_BLOB_URI);
    verify(tableRepo, never()).getById(any());
    verify(snapshotRepo).getByBlobUriLive(SNAPSHOT_BLOB_URI);
    verify(snapshotRepo, never()).getById(any(), anyLong());
    assertEquals(42L, init.snapshotId());
    assertEquals(
        "s3://bucket/table/metadata/00001.metadata.json", init.tableInfo().getMetadataLocation());
    assertEquals("us-east-1", init.tableInfo().getPropertiesMap().get("s3.region"));
    assertFalse(init.tableInfo().getPropertiesMap().containsKey("metadata-location"));
  }

  @Test
  void initScanFailsWhenPinnedTableBlobMissing() {
    when(tableRepo.getByBlobUriLive(TABLE_BLOB_URI)).thenReturn(Optional.empty());

    assertThrows(io.grpc.StatusRuntimeException.class, () -> service.initScan("corr", PIN));
  }

  private static ScanSession session(String statsGeneration) {
    return ScanSession.builder()
        .handleId("h")
        .queryId("q")
        .tableId(TABLE_ID)
        .snapshotId(42L)
        .statsGeneration(statsGeneration)
        .tableInfo(TableInfo.getDefaultInstance())
        .targetBatchItems(10)
        .targetBatchBytes(1 << 20)
        .build();
  }

  @Test
  void pinnedScanKeepsServingItsFrozenGenerationThroughAReplacement() {
    // The generation frozen at initScan ("gen-1") was superseded mid-stream ("gen-2" is live).
    // Retention keeps gen-1's immutable keyspace readable, so the scan streams it to completion —
    // deterministic at the frozen pointer, no live-pointer probes, no abort.
    when(statsStore.listTargetStatsInGeneration(
            eq(TABLE_ID), eq(42L), eq("gen-1"), any(), anyInt(), any()))
        .thenReturn(new StatsStorePage(java.util.List.of(), ""));
    when(statsStore.activeStatsGeneration(TABLE_ID, 42L)).thenReturn(Optional.of("gen-2"));

    var session = session("gen-1");
    var deletes =
        service.streamDeleteFiles(session, "corr").collect().asList().await().indefinitely();
    var data = service.streamDataFiles(session, "corr").collect().asList().await().indefinitely();

    assertEquals(0, deletes.size());
    assertEquals(0, data.size());
    verify(statsStore, never()).activeStatsGeneration(any(), anyLong());
    verify(statsStore, never()).listTargetStats(any(), anyLong(), any(), anyInt(), any());
  }

  @Test
  void generationlessStoreServesTheLiveState() {
    when(statsStore.listTargetStats(eq(TABLE_ID), eq(42L), any(), anyInt(), any()))
        .thenReturn(new StatsStorePage(java.util.List.of(), ""));

    // No generation frozen at initScan (store tracks none): reads serve the live state — the
    // store cannot do better — and never probe generation pointers.
    var deletes =
        service.streamDeleteFiles(session(null), "corr").collect().asList().await().indefinitely();

    assertEquals(0, deletes.size());
    verify(statsStore, never()).activeStatsGeneration(any(), anyLong());
  }

  @Test
  void scanFrozenWithNoGenerationStaysEmptyWhenOnePublishesMidStream() {
    // The store tracks generations but the snapshot had none at initScan (frozen "absent"). A
    // first generation published mid-stream stays invisible: the scan deterministically serves
    // the state it froze — nothing — rather than a torn or surprise file list.
    when(statsStore.activeStatsGeneration(TABLE_ID, 42L)).thenReturn(Optional.of("gen-1"));

    var session = session("");
    var deletes =
        service.streamDeleteFiles(session, "corr").collect().asList().await().indefinitely();
    var data = service.streamDataFiles(session, "corr").collect().asList().await().indefinitely();

    assertEquals(0, deletes.size());
    assertEquals(0, data.size());
    verify(statsStore, never()).listTargetStats(any(), anyLong(), any(), anyInt(), any());
    verify(statsStore, never())
        .listTargetStatsInGeneration(any(), anyLong(), any(), any(), anyInt(), any());
  }

  @Test
  void initScanFreezesThePinnedGenerationNotTheLiveOne() {
    // The pin carries the generation its root referenced ("gen-1"). A later finalize made "gen-2"
    // live (and committed a new root) before InitScan — but the pinned scan must read gen-1, the
    // generation its snapshot was finalized with, and never consult the live store.
    when(statsStore.tracksStatsGenerations()).thenReturn(true);
    when(statsStore.activeStatsGeneration(TABLE_ID, 42L)).thenReturn(Optional.of("gen-2"));
    stubPinnedBlobsPresent();

    var init = service.initScan("corr", PIN.toBuilder().setStatsGenerationRefUri("gen-1").build());

    assertEquals("gen-1", init.statsGeneration(), "the scan freezes the PINNED generation");
    verify(statsStore, never()).activeStatsGeneration(any(), anyLong());
  }

  @Test
  void initScanCapturesAbsentGenerationWhenThePinCarriesNone() {
    // A pin with no generation ref (should not happen under the gate for a finalized snapshot, but
    // the frozen "absent" state is the safe default) yields an empty scan, never a live read.
    when(statsStore.tracksStatsGenerations()).thenReturn(true);
    stubPinnedBlobsPresent();

    var init = service.initScan("corr", PIN);

    assertEquals("", init.statsGeneration());
    verify(statsStore, never()).activeStatsGeneration(any(), anyLong());
  }

  @Test
  void initScanCapturesNoGenerationWhenStoreTracksNone() {
    stubPinnedBlobsPresent();

    var init = service.initScan("corr", PIN);

    assertEquals(null, init.statsGeneration());
    verify(statsStore, never()).activeStatsGeneration(any(), anyLong());
  }

  @Test
  void initScanFailsWhenPinnedSnapshotBlobMissing() {
    when(tableRepo.getByBlobUriLive(TABLE_BLOB_URI))
        .thenReturn(Optional.of(Table.newBuilder().setResourceId(TABLE_ID).build()));
    when(snapshotRepo.getByBlobUriLive(SNAPSHOT_BLOB_URI)).thenReturn(Optional.empty());

    assertThrows(io.grpc.StatusRuntimeException.class, () -> service.initScan("corr", PIN));
  }
}
