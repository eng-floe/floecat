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

package ai.floedb.floecat.gateway.iceberg.rest.services.compat;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.ListSnapshotsResponse;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.SnapshotLister;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.GrpcServiceFacade;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import java.util.List;
import org.junit.jupiter.api.Test;

class DeltaIcebergMetadataServiceTest {

  @Test
  void refsModeReturnsNoSnapshotsWhenNoRefs() {
    DeltaIcebergMetadataService service = new DeltaIcebergMetadataService();
    service.snapshotClient = mock(GrpcServiceFacade.class);
    service.translator = mock(DeltaIcebergMetadataTranslator.class);
    service.manifestMaterializer = mock(DeltaManifestMaterializer.class);

    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:delta_orders").build();
    Table table = Table.newBuilder().setResourceId(tableId).build();
    Snapshot snapshot = Snapshot.newBuilder().setSnapshotId(10L).build();
    when(service.snapshotClient.listSnapshots(org.mockito.ArgumentMatchers.any()))
        .thenReturn(ListSnapshotsResponse.newBuilder().addSnapshots(snapshot).build());
    when(service.translator.translate(table, List.of(snapshot)))
        .thenReturn(IcebergMetadata.newBuilder().setMetadataLocation("floe+delta://x").build());
    when(service.manifestMaterializer.materialize(table, List.of())).thenReturn(List.of());

    DeltaIcebergMetadataService.DeltaLoadResult result =
        service.load(tableId, table, SnapshotLister.Mode.REFS);

    assertEquals(0, result.snapshots().size());
  }

  @Test
  void loadScopesTableResourceIdBeforeManifestMaterialization() {
    DeltaIcebergMetadataService service = new DeltaIcebergMetadataService();
    service.snapshotClient = mock(GrpcServiceFacade.class);
    service.translator = mock(DeltaIcebergMetadataTranslator.class);
    service.manifestMaterializer = mock(DeltaManifestMaterializer.class);

    ResourceId scopedTableId =
        ResourceId.newBuilder().setAccountId("acct-1").setId("cat:db:delta_orders").build();
    Table unscopedTable =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:delta_orders").build())
            .putProperties("storage_location", "s3://floecat-delta/call_center")
            .build();
    Table scopedTable = unscopedTable.toBuilder().setResourceId(scopedTableId).build();
    Snapshot snapshot = Snapshot.newBuilder().setSnapshotId(10L).build();

    when(service.snapshotClient.listSnapshots(org.mockito.ArgumentMatchers.any()))
        .thenReturn(ListSnapshotsResponse.newBuilder().addSnapshots(snapshot).build());
    when(service.translator.translate(scopedTable, List.of(snapshot)))
        .thenReturn(
            IcebergMetadata.newBuilder()
                .setMetadataLocation("floe+delta://cat:db:delta_orders/metadata/10.metadata.json")
                .build());
    when(service.manifestMaterializer.materialize(scopedTable, List.of(snapshot)))
        .thenReturn(List.of(snapshot));

    service.load(scopedTableId, unscopedTable, SnapshotLister.Mode.ALL);

    verify(service.translator).translate(scopedTable, List.of(snapshot));
    verify(service.manifestMaterializer).materialize(scopedTable, List.of(snapshot));
  }
}
