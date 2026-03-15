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

package ai.floedb.floecat.gateway.iceberg.minimal.resources.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.ListSnapshotsResponse;
import ai.floedb.floecat.catalog.rpc.ListTablesResponse;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.PageResponse;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.minimal.api.dto.LoadTableResultDto;
import ai.floedb.floecat.gateway.iceberg.minimal.api.dto.TableListResponseDto;
import ai.floedb.floecat.gateway.iceberg.minimal.api.request.MetricsReportRequest;
import ai.floedb.floecat.gateway.iceberg.minimal.api.request.TableCommitRequest;
import ai.floedb.floecat.gateway.iceberg.minimal.api.request.TableCreateRequest;
import ai.floedb.floecat.gateway.iceberg.minimal.config.MinimalGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.minimal.services.metadata.TableMetadataImportService;
import ai.floedb.floecat.gateway.iceberg.minimal.services.table.ConnectorCleanupService;
import ai.floedb.floecat.gateway.iceberg.minimal.services.table.TableBackend;
import ai.floedb.floecat.gateway.iceberg.minimal.services.transaction.TransactionCommitService;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.grpc.Status;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TableResourceTest {
  private final TableBackend backend = Mockito.mock(TableBackend.class);
  private final TableMetadataImportService importer =
      Mockito.mock(TableMetadataImportService.class);
  private final TransactionCommitService transactionCommitService =
      Mockito.mock(TransactionCommitService.class);
  private final ConnectorCleanupService connectorCleanupService =
      Mockito.mock(ConnectorCleanupService.class);
  private final MinimalGatewayConfig config = Mockito.mock(MinimalGatewayConfig.class);
  private final TableResource resource =
      new TableResource(
          backend,
          new ObjectMapper(),
          importer,
          transactionCommitService,
          connectorCleanupService,
          config);

  @Test
  void listsTables() {
    when(backend.list("foo", List.of("db"), 10, null))
        .thenReturn(
            ListTablesResponse.newBuilder()
                .addTables(Table.newBuilder().setDisplayName("orders").build())
                .setPage(PageResponse.newBuilder().setTotalSize(1).build())
                .build());

    TableListResponseDto dto =
        (TableListResponseDto) resource.list("foo", "db", 10, null).getEntity();

    assertEquals("orders", dto.identifiers().getFirst().name());
    assertEquals(List.of("db"), dto.identifiers().getFirst().namespace());
    assertNull(dto.nextPageToken());
  }

  @Test
  void getsTableLoadResult() {
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
            .setDisplayName("orders")
            .setSchemaJson("{\"type\":\"struct\",\"fields\":[]}")
            .putProperties(
                "metadata-location", "s3://bucket/db/orders/metadata/00001.metadata.json")
            .build();
    Snapshot snapshot =
        Snapshot.newBuilder()
            .setSnapshotId(42L)
            .putFormatMetadata(
                "iceberg",
                IcebergMetadata.newBuilder()
                    .setFormatVersion(2)
                    .setMetadataLocation("s3://bucket/db/orders/metadata/00001.metadata.json")
                    .setCurrentSnapshotId(42L)
                    .build()
                    .toByteString())
            .build();
    when(backend.get("foo", List.of("db"), "orders")).thenReturn(table);
    when(backend.currentSnapshot("foo", List.of("db"), "orders")).thenReturn(snapshot);
    when(backend.listSnapshots("foo", List.of("db"), "orders"))
        .thenReturn(ListSnapshotsResponse.newBuilder().addSnapshots(snapshot).build());
    when(importer.loadTableMetadata("s3://bucket/db/orders/metadata/00001.metadata.json", Map.of()))
        .thenThrow(new IllegalArgumentException("metadata unavailable"));

    LoadTableResultDto dto =
        (LoadTableResultDto) resource.get("foo", "db", "orders", null).getEntity();

    assertEquals("s3://bucket/db/orders/metadata/00001.metadata.json", dto.metadataLocation());
    assertEquals(2, dto.metadata().get("format-version"));
    assertEquals(42L, dto.metadata().get("current-snapshot-id"));
  }

  @Test
  void getsTableLoadResultWithoutCurrentSnapshot() {
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
            .setDisplayName("orders")
            .setSchemaJson("{\"type\":\"struct\",\"fields\":[]}")
            .putProperties(
                "metadata-location", "s3://bucket/db/orders/metadata/00001.metadata.json")
            .build();
    when(backend.get("foo", List.of("db"), "orders")).thenReturn(table);
    when(backend.currentSnapshot("foo", List.of("db"), "orders"))
        .thenThrow(Status.NOT_FOUND.asRuntimeException());
    when(importer.loadTableMetadata("s3://bucket/db/orders/metadata/00001.metadata.json", Map.of()))
        .thenReturn(
            TableMetadata.newTableMetadata(
                SchemaParser.fromJson("{\"type\":\"struct\",\"schema-id\":0,\"fields\":[]}"),
                PartitionSpec.unpartitioned(),
                SortOrder.unsorted(),
                "s3://bucket/db/orders",
                Map.of()));

    LoadTableResultDto dto =
        (LoadTableResultDto) resource.get("foo", "db", "orders", null).getEntity();

    assertEquals("s3://bucket/db/orders/metadata/00001.metadata.json", dto.metadataLocation());
    assertEquals(2, dto.metadata().get("format-version"));
    assertNull(dto.metadata().get("current-snapshot-id"));
    assertEquals(
        List.of(Map.of("spec-id", 0, "fields", List.of())), dto.metadata().get("partition-specs"));
    assertEquals(
        List.of(Map.of("order-id", 0, "fields", List.of())), dto.metadata().get("sort-orders"));
  }

  @Test
  void hydratesTableMetadataFromMetadataLocationWhenSnapshotMetadataIsIncomplete() {
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
            .setDisplayName("orders")
            .setSchemaJson("{\"type\":\"struct\",\"schema-id\":7,\"fields\":[]}")
            .putProperties(
                "metadata-location", "s3://bucket/db/orders/metadata/00001.metadata.json")
            .build();
    Snapshot snapshot = Snapshot.newBuilder().setSnapshotId(42L).build();
    when(backend.get("foo", List.of("db"), "orders")).thenReturn(table);
    when(backend.currentSnapshot("foo", List.of("db"), "orders")).thenReturn(snapshot);
    when(importer.loadTableMetadata("s3://bucket/db/orders/metadata/00001.metadata.json", Map.of()))
        .thenReturn(
            TableMetadata.newTableMetadata(
                SchemaParser.fromJson("{\"type\":\"struct\",\"schema-id\":7,\"fields\":[]}"),
                PartitionSpec.unpartitioned(),
                SortOrder.unsorted(),
                "s3://warehouse/db/orders",
                Map.of()));

    LoadTableResultDto dto =
        (LoadTableResultDto) resource.get("foo", "db", "orders", null).getEntity();

    assertEquals("s3://warehouse/db/orders", dto.metadata().get("location"));
  }

  @Test
  void createsTable() {
    var imported =
        new TableMetadataImportService.ImportedMetadata(
            "{\"type\":\"struct\",\"fields\":[]}",
            Map.of(
                "metadata-location", "s3://warehouse/db/orders/metadata/00000.metadata.json",
                "location", "s3://warehouse/db/orders",
                "last-updated-ms", "123456789",
                "last-sequence-number", "0"),
            "s3://warehouse/db/orders",
            IcebergMetadata.newBuilder()
                .setFormatVersion(2)
                .setMetadataLocation("s3://warehouse/db/orders/metadata/00000.metadata.json")
                .setLastUpdatedMs(123456789L)
                .setLastSequenceNumber(0L)
                .build());
    when(importer.buildInitialMetadata(
            eq(
                new TableCreateRequest(
                    "orders",
                    JsonNodeFactory.instance.objectNode(),
                    null,
                    null,
                    "s3://warehouse/db/orders",
                    Map.of("k", "v"),
                    false)),
            eq("s3://warehouse/db/orders/metadata/00000.metadata.json")))
        .thenReturn(imported);
    Table created =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
            .setDisplayName("orders")
            .setSchemaJson("{\"type\":\"struct\",\"fields\":[]}")
            .putProperties("storage_location", "s3://warehouse/db/orders")
            .build();
    when(backend.create(
            eq("foo"),
            eq(List.of("db")),
            eq("orders"),
            any(),
            eq("s3://warehouse/db/orders"),
            eq(
                Map.of(
                    "location", "s3://warehouse/db/orders",
                    "last-updated-ms", "123456789",
                    "last-sequence-number", "0",
                    "k", "v")),
            eq("idem-1")))
        .thenReturn(created);

    LoadTableResultDto dto =
        (LoadTableResultDto)
            resource
                .create(
                    "foo",
                    "db",
                    "idem-1",
                    new TableCreateRequest(
                        "orders",
                        JsonNodeFactory.instance.objectNode(),
                        null,
                        null,
                        "s3://warehouse/db/orders",
                        Map.of("k", "v"),
                        false))
                .getEntity();

    assertEquals(2, dto.metadata().get("format-version"));
    assertEquals(123456789L, dto.metadata().get("last-updated-ms"));
    assertEquals(0L, dto.metadata().get("last-sequence-number"));
  }

  @Test
  void createWithoutLocationUsesDefaultWarehouseLocation() {
    when(config.defaultWarehousePath()).thenReturn(java.util.Optional.of("s3://floecat/"));
    var imported =
        new TableMetadataImportService.ImportedMetadata(
            "{\"type\":\"struct\",\"fields\":[]}",
            Map.of(
                "metadata-location", "s3://floecat/db/orders/metadata/00000.metadata.json",
                "location", "s3://floecat/db/orders",
                "last-updated-ms", "987654321",
                "last-sequence-number", "0"),
            "s3://floecat/db/orders",
            IcebergMetadata.newBuilder()
                .setFormatVersion(2)
                .setMetadataLocation("s3://floecat/db/orders/metadata/00000.metadata.json")
                .setLastUpdatedMs(987654321L)
                .setLastSequenceNumber(0L)
                .setDefaultSortOrderId(0)
                .build());
    when(importer.buildInitialMetadata(
            eq(
                new TableCreateRequest(
                    "orders",
                    JsonNodeFactory.instance.objectNode(),
                    null,
                    null,
                    "s3://floecat/db/orders",
                    Map.of(),
                    false)),
            eq("s3://floecat/db/orders/metadata/00000.metadata.json")))
        .thenReturn(imported);
    Table created =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
            .setDisplayName("orders")
            .setSchemaJson("{\"type\":\"struct\",\"fields\":[]}")
            .putProperties("storage_location", "s3://floecat/db/orders")
            .build();
    when(backend.create(
            eq("foo"),
            eq(List.of("db")),
            eq("orders"),
            any(),
            eq("s3://floecat/db/orders"),
            eq(
                Map.of(
                    "location", "s3://floecat/db/orders",
                    "last-updated-ms", "987654321",
                    "last-sequence-number", "0")),
            eq("idem-1")))
        .thenReturn(created);

    LoadTableResultDto dto =
        (LoadTableResultDto)
            resource
                .create(
                    "foo",
                    "db",
                    "idem-1",
                    new TableCreateRequest(
                        "orders",
                        JsonNodeFactory.instance.objectNode(),
                        null,
                        null,
                        null,
                        Map.of(),
                        false))
                .getEntity();

    assertEquals("s3://floecat/db/orders", dto.metadata().get("location"));
    assertEquals(987654321L, dto.metadata().get("last-updated-ms"));
    assertEquals(0L, dto.metadata().get("last-sequence-number"));
    assertEquals(0, dto.metadata().get("default-sort-order-id"));
  }

  @Test
  void stageCreateReturnsWriteMetadataPathConfig() {
    when(config.defaultWarehousePath()).thenReturn(java.util.Optional.of("s3://floecat/"));
    when(importer.writeInitialMetadata(
            eq(
                new TableCreateRequest(
                    "orders",
                    JsonNodeFactory.instance.objectNode().put("schema-id", 7).putArray("fields"),
                    null,
                    null,
                    "s3://floecat/db/orders",
                    Map.of(),
                    true)),
            eq("s3://floecat/db/orders/metadata/00000-stage.metadata.json"),
            eq(Map.of())))
        .thenReturn(
            new TableMetadataImportService.ImportedMetadata(
                "{\"type\":\"struct\",\"schema-id\":7,\"fields\":[]}",
                Map.of(
                    "metadata-location",
                    "s3://floecat/db/orders/metadata/00000-stage.metadata.json",
                    "table-uuid",
                    "uuid-123"),
                "s3://floecat/db/orders",
                IcebergMetadata.newBuilder()
                    .setFormatVersion(2)
                    .setTableUuid("uuid-123")
                    .setMetadataLocation(
                        "s3://floecat/db/orders/metadata/00000-stage.metadata.json")
                    .setCurrentSchemaId(7)
                    .build()));

    LoadTableResultDto dto =
        (LoadTableResultDto)
            resource
                .create(
                    "foo",
                    "db",
                    "idem-1",
                    new TableCreateRequest(
                        "orders",
                        JsonNodeFactory.instance
                            .objectNode()
                            .put("schema-id", 7)
                            .putArray("fields"),
                        null,
                        null,
                        null,
                        Map.of(),
                        true))
                .getEntity();

    assertEquals("s3://floecat/db/orders/metadata", dto.config().get("write.metadata.path"));
    assertEquals("uuid-123", dto.metadata().get("table-uuid"));
    assertNull(dto.metadata().get("current-snapshot-id"));
  }

  @Test
  void commitsPropertyUpdates() {
    Table updated =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
            .setDisplayName("orders")
            .setSchemaJson("{\"type\":\"struct\",\"fields\":[]}")
            .putProperties("owner", "integration")
            .putProperties("location", "s3://warehouse/new-location")
            .build();
    when(transactionCommitService.commit(Mockito.eq("foo"), Mockito.isNull(), Mockito.any()))
        .thenReturn(Response.noContent().build());
    when(backend.get("foo", List.of("db"), "orders")).thenReturn(updated);
    when(backend.currentSnapshot("foo", List.of("db"), "orders"))
        .thenThrow(Status.NOT_FOUND.asRuntimeException());

    LoadTableResultDto dto =
        (LoadTableResultDto)
            resource
                .commit(
                    "foo",
                    "db",
                    "orders",
                    null,
                    new TableCommitRequest(
                        List.of(),
                        List.of(
                            Map.of(
                                "action",
                                "set-properties",
                                "updates",
                                Map.of("owner", "integration")),
                            Map.of(
                                "action",
                                "set-location",
                                "location",
                                "s3://warehouse/new-location"))))
                .getEntity();

    assertEquals("integration", ((Map<?, ?>) dto.metadata().get("properties")).get("owner"));
    assertEquals("s3://warehouse/new-location", dto.metadata().get("location"));
  }

  @Test
  void rejectsUnsupportedCommitActions() {
    when(transactionCommitService.commit(Mockito.eq("foo"), Mockito.isNull(), Mockito.any()))
        .thenReturn(Response.status(400).build());
    assertEquals(
        400,
        resource
            .commit(
                "foo",
                "db",
                "orders",
                null,
                new TableCommitRequest(List.of(), List.of(Map.of("action", "add-snapshot"))))
            .getStatus());
  }

  @Test
  void metricsReturns204ForValidPayload() {
    assertEquals(
        204,
        resource
            .publishMetrics(
                "db", "orders", new MetricsReportRequest("scan-report", "orders", 42L, Map.of()))
            .getStatus());
  }

  @Test
  void metricsValidatesRequiredFields() {
    assertEquals(
        400,
        resource
            .publishMetrics("db", "orders", new MetricsReportRequest(null, "orders", 42L, Map.of()))
            .getStatus());
  }

  @Test
  void headExistsReturns204() {
    assertEquals(204, resource.exists("foo", "db", "orders").getStatus());
    verify(backend).exists("foo", List.of("db"), "orders");
  }

  @Test
  void deleteReturns204() {
    Table existing =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
            .setDisplayName("orders")
            .build();
    when(backend.get("foo", List.of("db"), "orders")).thenReturn(existing);

    assertEquals(204, resource.delete("foo", "db", "orders").getStatus());
    verify(connectorCleanupService).deleteManagedConnector(existing);
    verify(backend).delete("foo", List.of("db"), "orders");
  }

  @Test
  void missingTableReturns404() {
    when(backend.get(any(), any(), any()))
        .thenThrow(Status.NOT_FOUND.withDescription("missing").asRuntimeException());

    Response response = resource.get("foo", "db", "missing", null);

    assertEquals(404, response.getStatus());
    @SuppressWarnings("unchecked")
    Map<String, Object> body = (Map<String, Object>) response.getEntity();
    assertEquals("NoSuchTableException", ((Map<String, Object>) body.get("error")).get("type"));
  }
}
