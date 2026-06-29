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

package ai.floedb.floecat.gateway.iceberg.rest.services.table.load;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.ListSnapshotsResponse;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.SnapshotLister;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.GrpcServiceFacade;
import ai.floedb.floecat.gateway.iceberg.rest.services.compat.DeltaManifestMaterializer;
import ai.floedb.floecat.gateway.iceberg.rest.services.compat.TableFormatSupport;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class TableLoadSupportTest {

  @Test
  void deltaCompatLoadDataUsesPointerBackedCurrentSnapshotId() {
    TableLoadSupport support = new TableLoadSupport();
    support.config = mock(IcebergGatewayConfig.class);
    support.snapshotClient = mock(GrpcServiceFacade.class);
    support.tableFormatSupport = mock(TableFormatSupport.class);
    support.deltaManifestMaterializer = mock(DeltaManifestMaterializer.class);

    IcebergGatewayConfig.DeltaCompatConfig deltaCompat =
        mock(IcebergGatewayConfig.DeltaCompatConfig.class);
    when(support.config.deltaCompat()).thenReturn(Optional.of(deltaCompat));
    when(deltaCompat.enabled()).thenReturn(true);

    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct-1")
            .setKind(ResourceKind.RK_TABLE)
            .setId("tbl-1")
            .build();
    Table table =
        Table.newBuilder()
            .setResourceId(tableId)
            .putProperties("current-snapshot-id", "200")
            .build();
    Snapshot current = Snapshot.newBuilder().setSnapshotId(100L).setSequenceNumber(100L).build();
    Snapshot newer = Snapshot.newBuilder().setSnapshotId(200L).setSequenceNumber(200L).build();
    List<Snapshot> snapshots = List.of(current, newer);
    TableGatewaySupport tableSupport = mock(TableGatewaySupport.class);

    when(support.tableFormatSupport.isDelta(table)).thenReturn(true);
    when(tableSupport.loadCurrentSnapshot(table)).thenReturn(current);
    when(support.snapshotClient.listSnapshots(any()))
        .thenReturn(
            ListSnapshotsResponse.newBuilder().addSnapshots(current).addSnapshots(newer).build());
    when(support.deltaManifestMaterializer.materialize(any(), anyList())).thenReturn(snapshots);

    TableLoadSupport.LoadData loadData =
        support.loadData(table, SnapshotLister.Mode.ALL, tableSupport);

    assertNull(loadData.metadataLocation());
    assertEquals(100L, loadData.currentSnapshotId());
    assertEquals(snapshots, loadData.snapshots());
  }
}
