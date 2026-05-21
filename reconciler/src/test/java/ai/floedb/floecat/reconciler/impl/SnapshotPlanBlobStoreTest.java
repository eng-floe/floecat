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

package ai.floedb.floecat.reconciler.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import ai.floedb.floecat.catalog.rpc.TableValueStats;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotSelection;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.stats.identity.TargetStatsRecords;
import ai.floedb.floecat.storage.spi.BlobStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

class SnapshotPlanBlobStoreTest {

  @Test
  void persistPlanRoundTripsScopedFileGroupJobs() {
    SnapshotPlanBlobStore store = new SnapshotPlanBlobStore();
    InMemoryBlobStore blobStore = new InMemoryBlobStore();
    store.blobStore = blobStore;
    store.mapper = new ObjectMapper();

    ReconcileScope scope =
        ReconcileScope.of(
            List.of(),
            "table-1",
            null,
            List.of(
                new ReconcileScope.ScopedCaptureRequest(
                    "table-1", 55L, "target-a", List.of("col_b", "col_a"))),
            ReconcileCapturePolicy.of(
                List.of(new ReconcileCapturePolicy.Column("col_a", true, false)),
                Set.of(ReconcileCapturePolicy.Output.FILE_STATS)),
            ReconcileSnapshotSelection.current());
    ReconcileFileGroupTask group =
        ReconcileFileGroupTask.of(
            "plan-1", "group-1", "table-1", 55L, List.of("s3://bucket/path/file-1.parquet"));
    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of(
            "table-1",
            55L,
            "db",
            "events",
            List.of(),
            true,
            ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
            "",
            1);

    ReconcileSnapshotTask persistedTask =
        store.persistPlan(
            "acct", "job-1", snapshotTask, List.of(new PlannedFileGroupJob(scope, group)));

    assertFalse(persistedTask.fileGroupPlanBlobUri().isBlank());
    assertEquals(1, persistedTask.fileGroupCount());
    assertNotNull(blobStore.bytesByUri.get(persistedTask.fileGroupPlanBlobUri()));

    List<PlannedFileGroupJob> roundTripped = store.loadPlanJobs(persistedTask);
    assertEquals(1, roundTripped.size());
    ReconcileScope roundTrippedScope = roundTripped.getFirst().scope();
    assertEquals(scope.destinationNamespaceIds(), roundTrippedScope.destinationNamespaceIds());
    assertEquals(scope.destinationTableId(), roundTrippedScope.destinationTableId());
    assertEquals(scope.destinationViewId(), roundTrippedScope.destinationViewId());
    assertEquals(
        scope.destinationCaptureRequests(), roundTrippedScope.destinationCaptureRequests());
    assertEquals(scope.capturePolicy().columns(), roundTrippedScope.capturePolicy().columns());
    assertEquals(scope.capturePolicy().outputs(), roundTrippedScope.capturePolicy().outputs());
    assertEquals(scope.snapshotSelection(), roundTrippedScope.snapshotSelection());
    assertEquals(group, roundTripped.getFirst().fileGroupTask());
  }

  @Test
  void persistDirectStatsRoundTripsRecords() {
    SnapshotPlanBlobStore store = new SnapshotPlanBlobStore();
    InMemoryBlobStore blobStore = new InMemoryBlobStore();
    store.blobStore = blobStore;
    store.mapper = new ObjectMapper();

    TargetStatsRecord record =
        TargetStatsRecords.tableRecord(
            tableId(), 55L, TableValueStats.newBuilder().setRowCount(3L).build(), null);
    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of(
            "table-1",
            55L,
            "db",
            "events",
            List.of(),
            true,
            ReconcileSnapshotTask.CompletionMode.DIRECT_STATS);

    ReconcileSnapshotTask persistedTask =
        store.persistDirectStats("acct", "job-1", snapshotTask, List.of(record));

    assertFalse(persistedTask.directStatsBlobUri().isBlank());
    assertEquals(1, persistedTask.directStatsRecordCount());
    assertNotNull(blobStore.bytesByUri.get(persistedTask.directStatsBlobUri()));
    assertEquals(List.of(record), store.loadDirectStats(persistedTask));
  }

  private static ai.floedb.floecat.common.rpc.ResourceId tableId() {
    return ai.floedb.floecat.common.rpc.ResourceId.newBuilder()
        .setAccountId("acct")
        .setId("table-1")
        .setKind(ai.floedb.floecat.common.rpc.ResourceKind.RK_TABLE)
        .build();
  }

  private static final class InMemoryBlobStore implements BlobStore {
    private final Map<String, byte[]> bytesByUri = new HashMap<>();

    @Override
    public byte[] get(String uri) {
      return Optional.ofNullable(bytesByUri.get(uri)).orElseThrow();
    }

    @Override
    public void put(String uri, byte[] bytes, String contentType) {
      bytesByUri.put(uri, bytes);
    }

    @Override
    public Optional<ai.floedb.floecat.common.rpc.BlobHeader> head(String uri) {
      return Optional.empty();
    }

    @Override
    public boolean delete(String uri) {
      return bytesByUri.remove(uri) != null;
    }

    @Override
    public void deletePrefix(String prefix) {
      bytesByUri.keySet().removeIf(key -> key.startsWith(prefix));
    }

    @Override
    public Page list(String prefix, int limit, String pageToken) {
      throw new UnsupportedOperationException("not needed for test");
    }
  }
}
