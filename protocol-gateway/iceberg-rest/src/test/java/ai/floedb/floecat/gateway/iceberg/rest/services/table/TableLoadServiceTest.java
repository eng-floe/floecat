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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.CatalogRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.NamespaceRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.TableRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.SnapshotLister;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.SnapshotClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.compat.DeltaIcebergMetadataService;
import ai.floedb.floecat.gateway.iceberg.rest.services.compat.TableFormatSupport;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TableLoadServiceTest {

  private final TableLoadService service = new TableLoadService();
  private final IcebergGatewayConfig config = mock(IcebergGatewayConfig.class);
  private final IcebergGatewayConfig.DeltaCompatConfig deltaCompat =
      mock(IcebergGatewayConfig.DeltaCompatConfig.class);
  private final TableLifecycleService tableLifecycleService = mock(TableLifecycleService.class);
  private final SnapshotClient snapshotClient = mock(SnapshotClient.class);
  private final TableFormatSupport tableFormatSupport = new TableFormatSupport();
  private final DeltaIcebergMetadataService deltaMetadataService =
      mock(DeltaIcebergMetadataService.class);
  private final TableGatewaySupport tableSupport = mock(TableGatewaySupport.class);

  @BeforeEach
  void setUp() {
    service.config = config;
    service.tableLifecycleService = tableLifecycleService;
    service.snapshotClient = snapshotClient;
    service.tableFormatSupport = tableFormatSupport;
    service.deltaMetadataService = deltaMetadataService;

    when(config.deltaCompat()).thenReturn(Optional.of(deltaCompat));
    when(deltaCompat.enabled()).thenReturn(true);
    when(tableSupport.defaultTableConfig()).thenReturn(Map.of());
    when(tableSupport.credentialsForAccessDelegation(any())).thenReturn(null);
  }

  @Test
  void loadUsesDeltaCompatServiceForDeltaTables() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:delta_orders").build();
    Table table =
        Table.newBuilder()
            .setResourceId(tableId)
            .setDisplayName("delta_orders")
            .setUpstream(UpstreamRef.newBuilder().setFormat(TableFormat.TF_DELTA).build())
            .build();
    when(tableLifecycleService.getTable(tableId)).thenReturn(table);

    IcebergMetadata metadata =
        IcebergMetadata.newBuilder()
            .setMetadataLocation("floe+delta://cat:db:delta_orders/metadata/11.metadata.json")
            .setCurrentSnapshotId(11L)
            .build();
    when(deltaMetadataService.load(tableId, table, SnapshotLister.Mode.ALL))
        .thenReturn(new DeltaIcebergMetadataService.DeltaLoadResult(metadata, List.of()));

    TableRequestContext context =
        new TableRequestContext(
            new NamespaceRequestContext(
                new CatalogRequestContext(
                    "pfx", "catalog", ResourceId.newBuilder().setId("cat").build()),
                "db",
                List.of("db"),
                ResourceId.newBuilder().setId("cat:db").build()),
            "delta_orders",
            tableId);

    Response response = service.load(context, "delta_orders", null, null, null, tableSupport);

    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertNotNull(response.getEntity());
    verify(deltaMetadataService).load(tableId, table, SnapshotLister.Mode.ALL);
    verify(tableSupport, never()).loadCurrentMetadata(any());
  }

  @Test
  void loadUsesDistinctEtagsForAllAndRefsSnapshotsModes() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:delta_orders").build();
    Table table =
        Table.newBuilder()
            .setResourceId(tableId)
            .setDisplayName("delta_orders")
            .setUpstream(UpstreamRef.newBuilder().setFormat(TableFormat.TF_DELTA).build())
            .build();
    when(tableLifecycleService.getTable(tableId)).thenReturn(table);

    IcebergMetadata metadata =
        IcebergMetadata.newBuilder()
            .setMetadataLocation("floe+delta://cat:db:delta_orders/metadata/11.metadata.json")
            .setCurrentSnapshotId(11L)
            .build();
    when(deltaMetadataService.load(tableId, table, SnapshotLister.Mode.ALL))
        .thenReturn(new DeltaIcebergMetadataService.DeltaLoadResult(metadata, List.of()));
    when(deltaMetadataService.load(tableId, table, SnapshotLister.Mode.REFS))
        .thenReturn(new DeltaIcebergMetadataService.DeltaLoadResult(metadata, List.of()));

    TableRequestContext context =
        new TableRequestContext(
            new NamespaceRequestContext(
                new CatalogRequestContext(
                    "pfx", "catalog", ResourceId.newBuilder().setId("cat").build()),
                "db",
                List.of("db"),
                ResourceId.newBuilder().setId("cat:db").build()),
            "delta_orders",
            tableId);

    Response allResponse = service.load(context, "delta_orders", null, null, null, tableSupport);
    Response refsResponse =
        service.load(context, "delta_orders", "refs", null, null, tableSupport);

    String allEtag = allResponse.getHeaderString(HttpHeaders.ETAG);
    String refsEtag = refsResponse.getHeaderString(HttpHeaders.ETAG);
    assertNotNull(allEtag);
    assertNotNull(refsEtag);
    assertNotEquals(allEtag, refsEtag);
  }
}
