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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.LoadTableResultDto;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.CatalogRef;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.NamespaceRef;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.TableRef;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.SnapshotLister;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.GrpcServiceFacade;
import ai.floedb.floecat.gateway.iceberg.rest.services.compat.DeltaIcebergMetadataService;
import ai.floedb.floecat.gateway.iceberg.rest.services.compat.TableFormatSupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.TableMetadataImportService;
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
  private final GrpcServiceFacade snapshotClient = mock(GrpcServiceFacade.class);
  private final TableFormatSupport tableFormatSupport = new TableFormatSupport();
  private final DeltaIcebergMetadataService deltaMetadataService =
      mock(DeltaIcebergMetadataService.class);
  private final TableMetadataImportService tableMetadataImportService =
      mock(TableMetadataImportService.class);
  private final TableGatewaySupport tableSupport = mock(TableGatewaySupport.class);
  private final TableLoadSupport loadSupport = new TableLoadSupport();

  @BeforeEach
  void setUp() {
    service.tableLifecycleService = tableLifecycleService;
    service.loadSupport = loadSupport;

    loadSupport.config = config;
    loadSupport.snapshotClient = snapshotClient;
    loadSupport.tableFormatSupport = tableFormatSupport;
    loadSupport.deltaMetadataService = deltaMetadataService;
    loadSupport.tableMetadataImportService = tableMetadataImportService;

    when(config.deltaCompat()).thenReturn(Optional.of(deltaCompat));
    when(deltaCompat.enabled()).thenReturn(true);
    when(tableSupport.defaultTableConfig()).thenReturn(Map.of());
    when(tableSupport.defaultTableConfig(any(Table.class))).thenReturn(Map.of());
    when(tableSupport.credentialsForAccessDelegation(any(Table.class), any())).thenReturn(null);
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
        IcebergMetadata.newBuilder().setCurrentSnapshotId(11L).build();
    when(deltaMetadataService.load(tableId, table, SnapshotLister.Mode.ALL))
        .thenReturn(
            new DeltaIcebergMetadataService.DeltaLoadResult(
                metadata, "floe+delta://cat:db:delta_orders/metadata/11.metadata.json", List.of()));

    TableRef context =
        new TableRef(
            new NamespaceRef(
                new CatalogRef("pfx", "catalog", ResourceId.newBuilder().setId("cat").build()),
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
        IcebergMetadata.newBuilder().setCurrentSnapshotId(11L).build();
    when(deltaMetadataService.load(tableId, table, SnapshotLister.Mode.ALL))
        .thenReturn(
            new DeltaIcebergMetadataService.DeltaLoadResult(
                metadata, "floe+delta://cat:db:delta_orders/metadata/11.metadata.json", List.of()));
    when(deltaMetadataService.load(tableId, table, SnapshotLister.Mode.REFS))
        .thenReturn(
            new DeltaIcebergMetadataService.DeltaLoadResult(
                metadata, "floe+delta://cat:db:delta_orders/metadata/11.metadata.json", List.of()));

    TableRef context =
        new TableRef(
            new NamespaceRef(
                new CatalogRef("pfx", "catalog", ResourceId.newBuilder().setId("cat").build()),
                "db",
                List.of("db"),
                ResourceId.newBuilder().setId("cat:db").build()),
            "delta_orders",
            tableId);

    Response allResponse = service.load(context, "delta_orders", null, null, null, tableSupport);
    Response refsResponse = service.load(context, "delta_orders", "refs", null, null, tableSupport);

    String allEtag = allResponse.getHeaderString(HttpHeaders.ETAG);
    String refsEtag = refsResponse.getHeaderString(HttpHeaders.ETAG);
    assertNotNull(allEtag);
    assertNotNull(refsEtag);
    assertNotEquals(allEtag, refsEtag);
  }

  @Test
  void loadHydratesFromSnapshotMetadataLocation() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    Table table =
        Table.newBuilder().setResourceId(tableId).setDisplayName("orders").build();
    when(tableLifecycleService.getTable(tableId)).thenReturn(table);
    when(tableSupport.loadCurrentMetadata(table))
        .thenReturn(
            IcebergMetadata.newBuilder()
                .build());
    when(tableSupport.loadCurrentMetadataLocation(table))
        .thenReturn("s3://new/metadata/00003.metadata.json");
    when(tableSupport.serverSideFileIoPropertiesForLocation(table, "s3://new/metadata/00003.metadata.json"))
        .thenReturn(Map.of());
    when(tableMetadataImportService.importMetadata(any(), any()))
        .thenReturn(
            new TableMetadataImportService.ImportedMetadata(
                "{\"schema-id\":0,\"type\":\"struct\",\"fields\":[],\"last-column-id\":0}",
                null,
                null,
                IcebergMetadata.newBuilder()
                    .addSchemas(
                        ai.floedb.floecat.gateway.iceberg.rpc.IcebergSchema.newBuilder()
                            .setSchemaId(0)
                            .setSchemaJson(
                                "{\"schema-id\":0,\"type\":\"struct\",\"fields\":[],\"last-column-id\":0}")
                            .setLastColumnId(0)
                            .build())
                    .build(),
                null,
                List.of()));

    TableRef context =
        new TableRef(
            new NamespaceRef(
                new CatalogRef("pfx", "catalog", ResourceId.newBuilder().setId("cat").build()),
                "db",
                List.of("db"),
                ResourceId.newBuilder().setId("cat:db").build()),
            "orders",
            tableId);

    service.load(context, "orders", null, null, null, tableSupport);

    verify(tableMetadataImportService)
        .importMetadata(eq("s3://new/metadata/00003.metadata.json"), any());
  }

  @Test
  void loadRefreshesWhenSnapshotMetadataLocationChanges() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    Table table =
        Table.newBuilder()
            .setResourceId(tableId)
            .setDisplayName("orders")
            .putProperties("format-version", "2")
            .build();
    when(tableLifecycleService.getTable(tableId)).thenReturn(table);
    when(tableSupport.loadCurrentMetadata(table))
        .thenReturn(
            IcebergMetadata.newBuilder()
                .setFormatVersion(1)
                .addSchemas(
                    ai.floedb.floecat.gateway.iceberg.rpc.IcebergSchema.newBuilder()
                        .setSchemaId(0)
                        .build())
                .build());
    when(tableSupport.loadCurrentMetadataLocation(table))
        .thenReturn("s3://warehouse/orders/metadata/00002.metadata.json");
    when(
            tableSupport.serverSideFileIoPropertiesForLocation(
                table, "s3://warehouse/orders/metadata/00002.metadata.json"))
        .thenReturn(Map.of());
    when(tableMetadataImportService.importMetadata(any(), any()))
        .thenReturn(
            new TableMetadataImportService.ImportedMetadata(
                "{\"schema-id\":0,\"type\":\"struct\",\"fields\":[],\"last-column-id\":0}",
                null,
                null,
                IcebergMetadata.newBuilder()
                    .setFormatVersion(2)
                    .addSchemas(
                        ai.floedb.floecat.gateway.iceberg.rpc.IcebergSchema.newBuilder()
                            .setSchemaId(0)
                            .setSchemaJson(
                                "{\"schema-id\":0,\"type\":\"struct\",\"fields\":[],\"last-column-id\":0}")
                            .setLastColumnId(0)
                            .build())
                    .build(),
                null,
                List.of()));

    TableRef context =
        new TableRef(
            new NamespaceRef(
                new CatalogRef("pfx", "catalog", ResourceId.newBuilder().setId("cat").build()),
                "db",
                List.of("db"),
                ResourceId.newBuilder().setId("cat:db").build()),
            "orders",
            tableId);

    Response response = service.load(context, "orders", null, null, null, tableSupport);

    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    LoadTableResultDto dto = (LoadTableResultDto) response.getEntity();
    assertNotNull(dto);
    assertEquals(2, dto.metadata().formatVersion());
    assertEquals("s3://warehouse/orders/metadata/00002.metadata.json", dto.metadataLocation());
    verify(tableMetadataImportService)
        .importMetadata(eq("s3://warehouse/orders/metadata/00002.metadata.json"), any());
  }
}
