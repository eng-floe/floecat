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
package ai.floedb.floecat.service.catalog.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository.CurrentSnapshotPointerUpdateResult;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class CurrentSnapshotPointerServiceTest {

  @Test
  void maybeAdvanceByIdFailsWhenSnapshotIsMissing() {
    var tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("tbl")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    var service = new CurrentSnapshotPointerService();
    service.snapshotRepo = mock(SnapshotRepository.class);
    when(service.snapshotRepo.getById(tableId, 123L)).thenReturn(Optional.empty());

    StatusRuntimeException thrown =
        assertThrows(
            StatusRuntimeException.class, () -> service.maybeAdvance(tableId, 123L, "corr"));

    assertEquals(Status.NOT_FOUND.getCode(), thrown.getStatus().getCode());
  }

  @Test
  void maybeAdvanceRecommitsTheRootEntryOnUnchanged() {
    var tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("tbl")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    var candidate = Snapshot.newBuilder().setTableId(tableId).setSnapshotId(7L).build();
    var service = new CurrentSnapshotPointerService();
    service.snapshotRepo = mock(SnapshotRepository.class);
    service.rootWriter = mock(TableRootWriter.class);
    // An in-place UpdateSnapshot of the already-current snapshot leaves the pointer id unchanged,
    // but the blob may have been rewritten: the root entry is re-committed so pinned identity
    // refreshes (the entry upsert is a no-op when nothing actually changed).
    when(service.snapshotRepo.maybeAdvanceCurrentSnapshotPointer(tableId, candidate))
        .thenReturn(CurrentSnapshotPointerUpdateResult.UNCHANGED);

    service.maybeAdvance(tableId, candidate, "corr");

    verify(service.rootWriter).commitSnapshotEntry(tableId, candidate);
  }

  @Test
  void advanceCommitsTheSnapshotEntryOntoTheTableRoot() {
    var tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("tbl")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    var candidate =
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(7L)
            .setUpstreamCreatedAt(com.google.protobuf.util.Timestamps.fromMillis(1_000L))
            .build();
    var service = new CurrentSnapshotPointerService();
    service.snapshotRepo = mock(SnapshotRepository.class);
    var roots =
        new ai.floedb.floecat.service.repo.impl.TableRootRepository(
            new ai.floedb.floecat.storage.memory.InMemoryPointerStore(),
            new ai.floedb.floecat.storage.memory.InMemoryBlobStore());
    var tableRepo = mock(ai.floedb.floecat.service.repo.impl.TableRepository.class);
    service.rootWriter =
        new TableRootWriter(
            roots, new TableRootCommitter(roots), tableRepo, service.snapshotRepo, null, null);
    when(service.snapshotRepo.maybeAdvanceCurrentSnapshotPointer(tableId, candidate))
        .thenReturn(CurrentSnapshotPointerUpdateResult.UPDATED);
    when(service.snapshotRepo.metaForSafe(tableId, 7L))
        .thenReturn(
            ai.floedb.floecat.common.rpc.MutationMeta.newBuilder()
                .setBlobUri("s3://tbl/snap-7.pb")
                .setEtag("etag-s7")
                .build());
    when(tableRepo.metaForSafe(tableId))
        .thenReturn(
            ai.floedb.floecat.common.rpc.MutationMeta.newBuilder()
                .setBlobUri("s3://tbl/table.pb")
                .setEtag("etag-t")
                .build());

    service.maybeAdvance(tableId, candidate, "corr");

    // The advance funnel is the single point keeping the root manifest in step with snapshots.
    var root = roots.get(tableId).orElseThrow();
    assertEquals(7L, root.getCurrentSnapshotId());
    assertEquals("s3://tbl/table.pb", root.getDefinitionRef().getUri());
    var entry =
        ai.floedb.floecat.service.repo.impl.SnapshotManifests.findEntry(
                roots, root.getSnapshotManifestRef(), 7L)
            .orElseThrow();
    assertEquals("s3://tbl/snap-7.pb", entry.getSnapshotRef().getUri());
    assertEquals("etag-s7", entry.getSnapshotRef().getVersion());
  }
}
