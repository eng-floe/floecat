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
package ai.floedb.floecat.service.reconciler.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.BlobRef;
import ai.floedb.floecat.catalog.rpc.SnapshotManifestEntry;
import ai.floedb.floecat.catalog.rpc.TableValueStats;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.catalog.impl.TableRootCommitter;
import ai.floedb.floecat.service.catalog.impl.TableRootMutations;
import ai.floedb.floecat.service.catalog.impl.TableRootWriter;
import ai.floedb.floecat.service.repo.impl.SnapshotManifests;
import ai.floedb.floecat.service.repo.impl.TableRootRepository;
import ai.floedb.floecat.service.statistics.StatsOrchestrator;
import ai.floedb.floecat.stats.identity.TargetStatsRecords;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import com.google.protobuf.util.Timestamps;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The finalize persistence step is where stats generations activate; each activation (or removal)
 * must land the generation's ref on the table root's snapshot entry.
 */
class SnapshotFinalizePersistenceServiceTest {

  private SnapshotFinalizePersistenceService persistence;
  private TableRootRepository roots;
  private ResourceId tableId;

  @BeforeEach
  void setUp() {
    roots = new TableRootRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("tbl")
            .setKind(ResourceKind.RK_TABLE)
            .build();

    persistence = new SnapshotFinalizePersistenceService();
    persistence.statsStore = mock(StatsStore.class);
    persistence.statsOrchestrator = mock(StatsOrchestrator.class);
    when(persistence.statsStore.tracksStatsGenerations()).thenReturn(true);
    when(persistence.statsStore.activeStatsGeneration(any(), anyLong()))
        .thenReturn(Optional.empty());

    var committer = new TableRootCommitter(roots);
    persistence.rootWriter =
        new TableRootWriter(roots, committer, null, null, null, persistence.statsStore);

    committer.commit(
        tableId,
        TableRootMutations.upsertSnapshot(
            roots,
            tableId,
            SnapshotManifestEntry.newBuilder()
                .setSnapshotId(5)
                .setSnapshotRef(BlobRef.newBuilder().setUri("s3://t/snap-5.pb").setVersion("v5"))
                .setUpstreamCreatedAt(Timestamps.fromMillis(5_000))
                .build(),
            null,
            true));
  }

  private SnapshotManifestEntry entry() {
    var root = roots.get(tableId).orElseThrow();
    return SnapshotManifests.findEntry(roots, root.getSnapshotManifestRef(), 5).orElseThrow();
  }

  @Test
  void replaceAllCommitsThePublishedGenerationOntoTheRoot() {
    when(persistence.statsStore.activeStatsGeneration(tableId, 5L))
        .thenReturn(Optional.of("s3://t/stats/5/gen-2.pb"));

    persistence.replaceAllStatsForSnapshot(
        tableId,
        5L,
        List.of(
            TargetStatsRecords.tableRecord(
                tableId, 5L, TableValueStats.newBuilder().setRowCount(1L).build(), null)));

    assertEquals("s3://t/stats/5/gen-2.pb", entry().getStatsGenerationRef().getUri());
  }

  @Test
  void deleteAllClearsTheGenerationRefOnTheRoot() {
    when(persistence.statsStore.activeStatsGeneration(tableId, 5L))
        .thenReturn(Optional.of("s3://t/stats/5/gen-1.pb"));
    persistence.persistStats(
        List.of(
            TargetStatsRecords.tableRecord(
                tableId, 5L, TableValueStats.newBuilder().setRowCount(1L).build(), null)));
    assertEquals("s3://t/stats/5/gen-1.pb", entry().getStatsGenerationRef().getUri());

    when(persistence.statsStore.activeStatsGeneration(tableId, 5L)).thenReturn(Optional.empty());
    when(persistence.statsStore.deleteAllStatsForSnapshot(tableId, 5L)).thenReturn(true);
    persistence.deleteAllStatsForSnapshot(tableId, 5L);

    assertFalse(entry().hasStatsGenerationRef());
  }
}
