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
package ai.floedb.floecat.gateway.iceberg.rest.services.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.GetSnapshotResponse;
import ai.floedb.floecat.catalog.rpc.ListSnapshotsResponse;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.GrpcServiceFacade;
import java.util.List;
import org.junit.jupiter.api.Test;

class SnapshotListerTest {

  @Test
  void refsModeIncludesPointerCurrentSnapshotInsteadOfTableProperty() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("tbl")
            .build();
    Table table =
        Table.newBuilder()
            .setResourceId(tableId)
            .putProperties("current-snapshot-id", "10")
            .build();
    Snapshot stale = Snapshot.newBuilder().setTableId(tableId).setSnapshotId(10).build();
    Snapshot current = Snapshot.newBuilder().setTableId(tableId).setSnapshotId(20).build();
    GrpcServiceFacade grpc = mock(GrpcServiceFacade.class);
    when(grpc.listSnapshots(any()))
        .thenReturn(
            ListSnapshotsResponse.newBuilder().addSnapshots(stale).addSnapshots(current).build());
    when(grpc.getSnapshot(any()))
        .thenReturn(GetSnapshotResponse.newBuilder().setSnapshot(current).build());

    List<Snapshot> snapshots = SnapshotLister.fetchSnapshots(grpc, table, SnapshotLister.Mode.REFS);

    assertEquals(List.of(current), snapshots);
  }
}
