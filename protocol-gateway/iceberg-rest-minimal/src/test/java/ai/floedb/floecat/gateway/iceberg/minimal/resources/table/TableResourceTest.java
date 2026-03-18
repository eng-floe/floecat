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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
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
import ai.floedb.floecat.gateway.iceberg.minimal.api.request.TransactionCommitRequest;
import ai.floedb.floecat.gateway.iceberg.minimal.config.MinimalGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.minimal.services.compat.DeltaIcebergMetadataService;
import ai.floedb.floecat.gateway.iceberg.minimal.services.metadata.TableMetadataImportService;
import ai.floedb.floecat.gateway.iceberg.minimal.services.table.ConnectorCleanupService;
import ai.floedb.floecat.gateway.iceberg.minimal.services.table.TableBackend;
import ai.floedb.floecat.gateway.iceberg.minimal.services.table.TableStorageCleanupService;
import ai.floedb.floecat.gateway.iceberg.minimal.services.transaction.TransactionCommitService;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadataLogEntry;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergSnapshotLogEntry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.grpc.Status;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class TableResourceTest {
  private final TableBackend backend = Mockito.mock(TableBackend.class);
  private final TableMetadataImportService importer =
      Mockito.mock(TableMetadataImportService.class);
  private final TransactionCommitService transactionCommitService =
      Mockito.mock(TransactionCommitService.class);
  private final ConnectorCleanupService connectorCleanupService =
      Mockito.mock(ConnectorCleanupService.class);
  private final TableStorageCleanupService tableStorageCleanupService =
      Mockito.mock(TableStorageCleanupService.class);
  private final MinimalGatewayConfig config = Mockito.mock(MinimalGatewayConfig.class);
  private final DeltaIcebergMetadataService deltaMetadataService =
      Mockito.mock(DeltaIcebergMetadataService.class);
  private final TableResource resource =
      new TableResource(
          backend,
          new ObjectMapper(),
          importer,
          transactionCommitService,
          connectorCleanupService,
          tableStorageCleanupService,
          config,
          deltaMetadataService);

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
                    .addSnapshotLog(
                        IcebergSnapshotLogEntry.newBuilder()
                            .setTimestampMs(1234L)
                            .setSnapshotId(42L)
                            .build())
                    .addMetadataLog(
                        IcebergMetadataLogEntry.newBuilder()
                            .setTimestampMs(1200L)
                            .setFile("s3://bucket/db/orders/metadata/00000.metadata.json")
                            .build())
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
        (LoadTableResultDto) resource.get("foo", "db", "orders", null, null, null).getEntity();

    assertEquals("s3://bucket/db/orders/metadata/00001.metadata.json", dto.metadataLocation());
    assertEquals(2, dto.metadata().get("format-version"));
    assertEquals(42L, dto.metadata().get("current-snapshot-id"));
    assertEquals(
        List.of(Map.of("timestamp-ms", 1234L, "snapshot-id", 42L)),
        dto.metadata().get("snapshot-log"));
    assertEquals(
        List.of(
            Map.of(
                "timestamp-ms",
                1200L,
                "metadata-file",
                "s3://bucket/db/orders/metadata/00000.metadata.json")),
        dto.metadata().get("metadata-log"));
  }

  @Test
  void getsTableLoadResultWithRefsSnapshotMode() {
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
            .setDisplayName("orders")
            .setSchemaJson("{\"type\":\"struct\",\"fields\":[]}")
            .putProperties(
                "metadata-location", "s3://bucket/db/orders/metadata/00001.metadata.json")
            .build();
    Snapshot currentSnapshot =
        Snapshot.newBuilder()
            .setSnapshotId(42L)
            .setSequenceNumber(2L)
            .putFormatMetadata(
                "iceberg",
                IcebergMetadata.newBuilder()
                    .setFormatVersion(2)
                    .setMetadataLocation("s3://bucket/db/orders/metadata/00001.metadata.json")
                    .setCurrentSnapshotId(42L)
                    .putRefs(
                        "main",
                        ai.floedb.floecat.gateway.iceberg.rpc.IcebergRef.newBuilder()
                            .setSnapshotId(42L)
                            .setType("branch")
                            .build())
                    .build()
                    .toByteString())
            .build();
    Snapshot oldSnapshot = Snapshot.newBuilder().setSnapshotId(41L).setSequenceNumber(1L).build();
    when(backend.get("foo", List.of("db"), "orders")).thenReturn(table);
    when(backend.currentSnapshot("foo", List.of("db"), "orders")).thenReturn(currentSnapshot);
    when(backend.listSnapshots("foo", List.of("db"), "orders"))
        .thenReturn(
            ListSnapshotsResponse.newBuilder()
                .addSnapshots(oldSnapshot)
                .addSnapshots(currentSnapshot)
                .build());
    when(importer.loadTableMetadata("s3://bucket/db/orders/metadata/00001.metadata.json", Map.of()))
        .thenThrow(new IllegalArgumentException("metadata unavailable"));

    Response response = resource.get("foo", "db", "orders", "refs", null, null);

    LoadTableResultDto dto = (LoadTableResultDto) response.getEntity();
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> snapshots =
        (List<Map<String, Object>>) dto.metadata().get("snapshots");
    assertEquals(1, snapshots.size());
    assertEquals(42L, snapshots.getFirst().get("snapshot-id"));
    assertEquals(
        "\"s3://bucket/db/orders/metadata/00001.metadata.json|snapshots=refs\"",
        response.getHeaderString("ETag"));
  }

  @Test
  void returnsNotModifiedForMatchingEtag() {
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

    Response response =
        resource.get(
            "foo",
            "db",
            "orders",
            null,
            null,
            "W/\"s3://bucket/db/orders/metadata/00001.metadata.json|snapshots=all\"");

    assertEquals(304, response.getStatus());
  }

  @Test
  void rejectsWildcardIfNoneMatch() {
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

    Response response = resource.get("foo", "db", "orders", null, null, "*");

    assertEquals(400, response.getStatus());
  }

  @Test
  void rejectsInvalidSnapshotMode() {
    Response response = resource.get("foo", "db", "orders", "bad", null, null);

    assertEquals(400, response.getStatus());
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
        (LoadTableResultDto) resource.get("foo", "db", "orders", null, null, null).getEntity();

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
        (LoadTableResultDto) resource.get("foo", "db", "orders", null, null, null).getEntity();

    assertEquals("s3://warehouse/db/orders", dto.metadata().get("location"));
  }

  @Test
  void hydratesTableMetadataWhenTableMetadataLocationIsNewerThanSnapshotMetadata() {
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
            .setDisplayName("orders")
            .setSchemaJson("{\"type\":\"struct\",\"schema-id\":7,\"fields\":[]}")
            .putProperties(
                "metadata-location", "s3://bucket/db/orders/metadata/00002.metadata.json")
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
                    .setCurrentSchemaId(0)
                    .setLastColumnId(2)
                    .build()
                    .toByteString())
            .build();
    when(backend.get("foo", List.of("db"), "orders")).thenReturn(table);
    when(backend.currentSnapshot("foo", List.of("db"), "orders")).thenReturn(snapshot);
    when(importer.loadTableMetadata("s3://bucket/db/orders/metadata/00002.metadata.json", Map.of()))
        .thenReturn(
            TableMetadata.newTableMetadata(
                SchemaParser.fromJson(
                    "{\"type\":\"struct\",\"schema-id\":1,\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"int\",\"required\":false},{\"id\":2,\"name\":\"v2\",\"type\":\"string\",\"required\":false},{\"id\":3,\"name\":\"note\",\"type\":\"string\",\"required\":false}],\"last-column-id\":3}"),
                PartitionSpec.unpartitioned(),
                SortOrder.unsorted(),
                "s3://warehouse/db/orders",
                Map.of()));

    LoadTableResultDto dto =
        (LoadTableResultDto) resource.get("foo", "db", "orders", null, null, null).getEntity();

    assertEquals(3, dto.metadata().get("last-column-id"));
  }

  @Test
  void missingTableHeadReturns404WithoutEntity() {
    doThrow(Status.NOT_FOUND.withDescription("missing").asRuntimeException())
        .when(backend)
        .exists("foo", List.of("db"), "missing");

    Response response = resource.exists("foo", "db", "missing");

    assertEquals(404, response.getStatus());
    assertEquals("0", response.getHeaderString("Content-Length"));
    assertNull(response.getEntity());
  }

  @Test
  void getsDeltaTableUsingTranslatedMetadata() {
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:delta_orders"))
            .setDisplayName("delta_orders")
            .putProperties("storage_location", "s3://warehouse/delta_orders")
            .build();
    Snapshot s1 = Snapshot.newBuilder().setSnapshotId(101L).build();
    Snapshot s2 =
        Snapshot.newBuilder()
            .setSnapshotId(102L)
            .setManifestList("s3://warehouse/delta_orders/metadata/manifest.avro")
            .build();
    IcebergMetadata metadata =
        IcebergMetadata.newBuilder()
            .setMetadataLocation("floe+delta://cat:db:delta_orders/metadata/102.metadata.json")
            .setCurrentSnapshotId(102L)
            .setCurrentSchemaId(3)
            .setFormatVersion(2)
            .build();
    when(backend.get("foo", List.of("db"), "delta_orders")).thenReturn(table);
    when(backend.listSnapshots("foo", List.of("db"), "delta_orders"))
        .thenReturn(ListSnapshotsResponse.newBuilder().addSnapshots(s1).addSnapshots(s2).build());
    when(deltaMetadataService.enabledFor(table)).thenReturn(true);
    when(deltaMetadataService.load(table, List.of(s1, s2)))
        .thenReturn(new DeltaIcebergMetadataService.DeltaLoadResult(metadata, List.of(s2)));

    LoadTableResultDto dto =
        (LoadTableResultDto)
            resource.get("foo", "db", "delta_orders", null, null, null).getEntity();

    assertEquals(
        "floe+delta://cat:db:delta_orders/metadata/102.metadata.json", dto.metadataLocation());
    assertEquals(102L, dto.metadata().get("current-snapshot-id"));
  }

  @Test
  void preservesZeroSnapshotIdInDeltaMetadata() {
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:delta_zero"))
            .setDisplayName("delta_zero")
            .putProperties("storage_location", "s3://warehouse/delta_zero")
            .build();
    Snapshot snapshot =
        Snapshot.newBuilder()
            .setSnapshotId(0L)
            .setManifestList("s3://warehouse/delta_zero/metadata/manifest.avro")
            .build();
    IcebergMetadata metadata =
        IcebergMetadata.newBuilder()
            .setMetadataLocation("floe+delta://cat:db:delta_zero/metadata/0.metadata.json")
            .setCurrentSnapshotId(0L)
            .setCurrentSchemaId(1)
            .setFormatVersion(2)
            .putRefs(
                "main",
                ai.floedb.floecat.gateway.iceberg.rpc.IcebergRef.newBuilder()
                    .setSnapshotId(0L)
                    .setType("branch")
                    .build())
            .build();
    when(backend.get("foo", List.of("db"), "delta_zero")).thenReturn(table);
    when(backend.listSnapshots("foo", List.of("db"), "delta_zero"))
        .thenReturn(ListSnapshotsResponse.newBuilder().addSnapshots(snapshot).build());
    when(deltaMetadataService.enabledFor(table)).thenReturn(true);
    when(deltaMetadataService.load(table, List.of(snapshot)))
        .thenReturn(new DeltaIcebergMetadataService.DeltaLoadResult(metadata, List.of(snapshot)));

    LoadTableResultDto dto =
        (LoadTableResultDto) resource.get("foo", "db", "delta_zero", null, null, null).getEntity();

    assertEquals(0L, dto.metadata().get("current-snapshot-id"));
    @SuppressWarnings("unchecked")
    Map<String, Object> refs = (Map<String, Object>) dto.metadata().get("refs");
    @SuppressWarnings("unchecked")
    Map<String, Object> main = (Map<String, Object>) refs.get("main");
    assertEquals(0L, main.get("snapshot-id"));
  }

  @Test
  void preservesZeroParentSnapshotIdInDeltaMetadata() {
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:delta_parent_zero"))
            .setDisplayName("delta_parent_zero")
            .putProperties("storage_location", "s3://warehouse/delta_parent_zero")
            .build();
    Snapshot snapshot =
        Snapshot.newBuilder()
            .setSnapshotId(5L)
            .setParentSnapshotId(0L)
            .setManifestList("s3://warehouse/delta_parent_zero/metadata/manifest.avro")
            .build();
    IcebergMetadata metadata =
        IcebergMetadata.newBuilder()
            .setMetadataLocation("floe+delta://cat:db:delta_parent_zero/metadata/5.metadata.json")
            .setCurrentSnapshotId(5L)
            .setCurrentSchemaId(1)
            .setFormatVersion(2)
            .putRefs(
                "main",
                ai.floedb.floecat.gateway.iceberg.rpc.IcebergRef.newBuilder()
                    .setSnapshotId(5L)
                    .setType("branch")
                    .build())
            .build();
    when(backend.get("foo", List.of("db"), "delta_parent_zero")).thenReturn(table);
    when(backend.listSnapshots("foo", List.of("db"), "delta_parent_zero"))
        .thenReturn(ListSnapshotsResponse.newBuilder().addSnapshots(snapshot).build());
    when(deltaMetadataService.enabledFor(table)).thenReturn(true);
    when(deltaMetadataService.load(table, List.of(snapshot)))
        .thenReturn(new DeltaIcebergMetadataService.DeltaLoadResult(metadata, List.of(snapshot)));

    LoadTableResultDto dto =
        (LoadTableResultDto)
            resource.get("foo", "db", "delta_parent_zero", null, null, null).getEntity();

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> snapshots =
        (List<Map<String, Object>>) dto.metadata().get("snapshots");
    assertEquals(0L, snapshots.getFirst().get("parent-snapshot-id"));
  }

  @Test
  void deltaLoadSnapshotsAlwaysExposeNonNullSummaryMap() {
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:delta_summary"))
            .setDisplayName("delta_summary")
            .putProperties("storage_location", "s3://warehouse/delta_summary")
            .build();
    Snapshot snapshot =
        Snapshot.newBuilder()
            .setSnapshotId(7L)
            .setManifestList("s3://warehouse/delta_summary/metadata/manifest.avro")
            .build();
    IcebergMetadata metadata =
        IcebergMetadata.newBuilder()
            .setMetadataLocation("floe+delta://cat:db:delta_summary/metadata/7.metadata.json")
            .setCurrentSnapshotId(7L)
            .setCurrentSchemaId(1)
            .setFormatVersion(2)
            .putRefs(
                "main",
                ai.floedb.floecat.gateway.iceberg.rpc.IcebergRef.newBuilder()
                    .setSnapshotId(7L)
                    .setType("branch")
                    .build())
            .build();
    when(backend.get("foo", List.of("db"), "delta_summary")).thenReturn(table);
    when(backend.listSnapshots("foo", List.of("db"), "delta_summary"))
        .thenReturn(ListSnapshotsResponse.newBuilder().addSnapshots(snapshot).build());
    when(deltaMetadataService.enabledFor(table)).thenReturn(true);
    when(deltaMetadataService.load(table, List.of(snapshot)))
        .thenReturn(new DeltaIcebergMetadataService.DeltaLoadResult(metadata, List.of(snapshot)));

    LoadTableResultDto dto =
        (LoadTableResultDto)
            resource.get("foo", "db", "delta_summary", null, null, null).getEntity();

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> snapshots =
        (List<Map<String, Object>>) dto.metadata().get("snapshots");
    assertTrue(snapshots.getFirst().containsKey("summary"));
    assertEquals(Map.of("operation", "append"), snapshots.getFirst().get("summary"));
    assertEquals(0L, snapshots.getFirst().get("timestamp-ms"));
    assertEquals(
        "s3://warehouse/delta_summary/metadata/manifest.avro",
        snapshots.getFirst().get("manifest-list"));
    assertEquals(0, snapshots.getFirst().get("schema-id"));
  }

  @Test
  void createsTableTransactionally() {
    var schema = JsonNodeFactory.instance.objectNode();
    schema.put("schema-id", 7);
    schema.putArray("fields");
    when(transactionCommitService.commit(eq("foo"), eq("idem-1"), any()))
        .thenReturn(Response.noContent().build());
    Table created =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
            .setDisplayName("orders")
            .setSchemaJson("{\"type\":\"struct\",\"schema-id\":7,\"fields\":[]}")
            .putProperties("storage_location", "s3://warehouse/db/orders")
            .putProperties("location", "s3://warehouse/db/orders")
            .putProperties("format-version", "2")
            .putProperties("current-schema-id", "7")
            .putProperties("last-sequence-number", "0")
            .build();
    when(backend.get("foo", List.of("db"), "orders")).thenReturn(created);
    when(backend.currentSnapshot("foo", List.of("db"), "orders"))
        .thenThrow(Status.NOT_FOUND.asRuntimeException());

    LoadTableResultDto dto =
        (LoadTableResultDto)
            resource
                .create(
                    "foo",
                    "db",
                    null,
                    "idem-1",
                    new TableCreateRequest(
                        "orders",
                        schema,
                        null,
                        null,
                        "s3://warehouse/db/orders",
                        Map.of("k", "v"),
                        false))
                .getEntity();

    ArgumentCaptor<TransactionCommitRequest> captor =
        ArgumentCaptor.forClass(TransactionCommitRequest.class);
    verify(transactionCommitService).commit(eq("foo"), eq("idem-1"), captor.capture());
    TransactionCommitRequest.TableChange change = captor.getValue().tableChanges().getFirst();
    assertEquals(List.of("db"), change.identifier().namespace());
    assertEquals("orders", change.identifier().name());
    assertEquals(List.of(Map.of("type", "assert-create")), change.requirements());
    assertEquals("set-location", change.updates().get(0).get("action"));
    assertEquals("s3://warehouse/db/orders", change.updates().get(0).get("location"));
    assertEquals("upgrade-format-version", change.updates().get(1).get("action"));
    assertEquals(2, change.updates().get(1).get("format-version"));
    assertEquals("set-properties", change.updates().getLast().get("action"));
    assertEquals(
        Map.of("k", "v", "last-sequence-number", "0"), change.updates().getLast().get("updates"));
    assertNull(dto.metadataLocation());
    assertEquals("s3://warehouse/db/orders", dto.metadata().get("location"));
    assertEquals(2, dto.metadata().get("format-version"));
  }

  @Test
  void createWithoutLocationUsesDefaultWarehouseLocation() {
    when(config.defaultWarehousePath()).thenReturn(java.util.Optional.of("s3://floecat/"));
    var schema = JsonNodeFactory.instance.objectNode();
    schema.put("schema-id", 9);
    schema.putArray("fields");
    when(transactionCommitService.commit(eq("foo"), eq("idem-1"), any()))
        .thenReturn(Response.noContent().build());
    Table created =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
            .setDisplayName("orders")
            .setSchemaJson("{\"type\":\"struct\",\"schema-id\":9,\"fields\":[]}")
            .putProperties("storage_location", "s3://floecat/db/orders")
            .putProperties("location", "s3://floecat/db/orders")
            .putProperties("format-version", "1")
            .putProperties("current-schema-id", "9")
            .putProperties("last-sequence-number", "0")
            .build();
    when(backend.get("foo", List.of("db"), "orders")).thenReturn(created);
    when(backend.currentSnapshot("foo", List.of("db"), "orders"))
        .thenThrow(Status.NOT_FOUND.asRuntimeException());

    LoadTableResultDto dto =
        (LoadTableResultDto)
            resource
                .create(
                    "foo",
                    "db",
                    null,
                    "idem-1",
                    new TableCreateRequest(
                        "orders", schema, null, null, null, Map.of("format-version", "1"), false))
                .getEntity();

    ArgumentCaptor<TransactionCommitRequest> captor =
        ArgumentCaptor.forClass(TransactionCommitRequest.class);
    verify(transactionCommitService).commit(eq("foo"), eq("idem-1"), captor.capture());
    TransactionCommitRequest.TableChange change = captor.getValue().tableChanges().getFirst();
    assertEquals("s3://floecat/db/orders", change.updates().get(0).get("location"));
    assertEquals(1, change.updates().get(1).get("format-version"));
    assertEquals(
        Map.of("format-version", "1", "last-sequence-number", "0"),
        change.updates().getLast().get("updates"));
    assertNull(dto.metadataLocation());
    assertEquals("s3://floecat/db/orders", dto.metadata().get("location"));
    assertEquals(1, dto.metadata().get("format-version"));
  }

  @Test
  void stageCreateReturnsWriteMetadataPathConfig() {
    when(config.defaultWarehousePath()).thenReturn(java.util.Optional.of("s3://floecat/"));
    when(importer.buildInitialMetadata(
            eq(
                new TableCreateRequest(
                    "orders",
                    JsonNodeFactory.instance.objectNode().put("schema-id", 7).putArray("fields"),
                    null,
                    null,
                    "s3://floecat/db/orders",
                    Map.of(),
                    true)),
            eq("s3://floecat/db/orders/metadata/00000-stage.metadata.json")))
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
                    .build(),
                null,
                List.of()));

    LoadTableResultDto dto =
        (LoadTableResultDto)
            resource
                .create(
                    "foo",
                    "db",
                    null,
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
  void loadReturnsStorageCredentialsWhenRequested() {
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
    stubStorageCredentials();

    LoadTableResultDto dto =
        (LoadTableResultDto)
            resource.get("foo", "db", "orders", null, "vended-credentials", null).getEntity();

    assertEquals(1, dto.storageCredentials().size());
    assertEquals("*", dto.storageCredentials().getFirst().prefix());
    assertEquals("s3", dto.storageCredentials().getFirst().config().get("type"));
  }

  @Test
  void createReturnsStorageCredentialsWhenRequested() {
    stubStorageCredentials();
    when(importer.buildInitialMetadata(
            eq(
                new TableCreateRequest(
                    "orders",
                    JsonNodeFactory.instance.objectNode().put("schema-id", 7).putArray("fields"),
                    null,
                    null,
                    "s3://warehouse/db/orders",
                    Map.of(),
                    true)),
            eq("s3://warehouse/db/orders/metadata/00000-stage.metadata.json")))
        .thenReturn(
            new TableMetadataImportService.ImportedMetadata(
                "{\"type\":\"struct\",\"schema-id\":7,\"fields\":[]}",
                Map.of(
                    "metadata-location",
                    "s3://warehouse/db/orders/metadata/00000-stage.metadata.json",
                    "table-uuid",
                    "uuid-123"),
                "s3://warehouse/db/orders",
                IcebergMetadata.newBuilder()
                    .setFormatVersion(2)
                    .setTableUuid("uuid-123")
                    .setMetadataLocation(
                        "s3://warehouse/db/orders/metadata/00000-stage.metadata.json")
                    .setCurrentSchemaId(7)
                    .build(),
                null,
                List.of()));

    LoadTableResultDto dto =
        (LoadTableResultDto)
            resource
                .create(
                    "foo",
                    "db",
                    "vended-credentials",
                    null,
                    new TableCreateRequest(
                        "orders",
                        JsonNodeFactory.instance
                            .objectNode()
                            .put("schema-id", 7)
                            .putArray("fields"),
                        null,
                        null,
                        "s3://warehouse/db/orders",
                        Map.of(),
                        true))
                .getEntity();

    assertEquals(1, dto.storageCredentials().size());
    assertEquals("*", dto.storageCredentials().getFirst().prefix());
  }

  @Test
  void loadRejectsUnsupportedAccessDelegationMode() {
    Response response = resource.get("foo", "db", "orders", null, "unknown", null);

    assertEquals(400, response.getStatus());
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
    when(backend.get("foo", List.of("db"), "orders"))
        .thenReturn(
            Table.newBuilder()
                .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
                .setDisplayName("orders")
                .build());
    assertEquals(
        204,
        resource
            .publishMetrics(
                "foo",
                "db",
                "orders",
                new MetricsReportRequest(
                    "scan-report",
                    "orders",
                    42L,
                    null,
                    null,
                    7,
                    List.of(1),
                    List.of("id"),
                    Map.of("type", "alwaysTrue"),
                    Map.of(),
                    Map.of()))
            .getStatus());
  }

  @Test
  void metricsValidatesRequiredFields() {
    when(backend.get("foo", List.of("db"), "orders"))
        .thenReturn(
            Table.newBuilder()
                .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
                .setDisplayName("orders")
                .build());
    assertEquals(
        400,
        resource
            .publishMetrics(
                "foo",
                "db",
                "orders",
                new MetricsReportRequest(
                    null, "orders", 42L, null, null, null, null, null, null, Map.of(), Map.of()))
            .getStatus());
  }

  @Test
  void metricsAcceptsCommitReportShape() {
    when(backend.get("foo", List.of("db"), "orders"))
        .thenReturn(
            Table.newBuilder()
                .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
                .setDisplayName("orders")
                .build());
    assertEquals(
        204,
        resource
            .publishMetrics(
                "foo",
                "db",
                "orders",
                new MetricsReportRequest(
                    "commit-report",
                    "orders",
                    42L,
                    3L,
                    "append",
                    null,
                    null,
                    null,
                    null,
                    Map.of(),
                    Map.of()))
            .getStatus());
  }

  @Test
  void metricsReturnsNotFoundWhenTableIsMissing() {
    when(backend.get("foo", List.of("db"), "orders"))
        .thenThrow(
            Status.NOT_FOUND.withDescription("Table db.orders not found").asRuntimeException());

    Response response =
        resource.publishMetrics(
            "foo",
            "db",
            "orders",
            new MetricsReportRequest(
                "commit-report",
                "orders",
                42L,
                3L,
                "append",
                null,
                null,
                null,
                null,
                Map.of(),
                Map.of()));

    assertEquals(404, response.getStatus());
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

    assertEquals(204, resource.delete("foo", "db", "orders", null, "idem-1").getStatus());
    verify(connectorCleanupService).deleteManagedConnector(existing);
    verify(backend).delete("foo", List.of("db"), "orders");
  }

  private void stubStorageCredentials() {
    MinimalGatewayConfig.StorageCredentialConfig storageCredential =
        Mockito.mock(MinimalGatewayConfig.StorageCredentialConfig.class);
    when(config.storageCredential()).thenReturn(Optional.of(storageCredential));
    when(storageCredential.scope()).thenReturn(Optional.of("*"));
    when(storageCredential.properties())
        .thenReturn(
            Map.of(
                "type", "s3",
                "s3.access-key-id", "test",
                "s3.secret-access-key", "secret",
                "s3.region", "us-east-1"));
  }

  @Test
  void deleteWithPurgeRemovesTableDataBeforeDeletingCatalogEntry() {
    Table existing =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
            .setDisplayName("orders")
            .putProperties("storage_location", "s3://bucket/db/orders")
            .build();
    when(backend.get("foo", List.of("db"), "orders")).thenReturn(existing);

    assertEquals(204, resource.delete("foo", "db", "orders", true, "idem-1").getStatus());

    verify(tableStorageCleanupService).purgeTableData(existing);
    verify(connectorCleanupService).deleteManagedConnector(existing);
    verify(backend).delete("foo", List.of("db"), "orders");
  }

  @Test
  void missingTableReturns404() {
    when(backend.get(any(), any(), any()))
        .thenThrow(Status.NOT_FOUND.withDescription("missing").asRuntimeException());

    Response response = resource.get("foo", "db", "missing", null, null, null);

    assertEquals(404, response.getStatus());
    @SuppressWarnings("unchecked")
    Map<String, Object> body = (Map<String, Object>) response.getEntity();
    assertEquals("NoSuchTableException", ((Map<String, Object>) body.get("error")).get("type"));
  }
}
