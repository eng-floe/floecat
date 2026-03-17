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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.minimal.api.dto.LoadTableResultDto;
import ai.floedb.floecat.gateway.iceberg.minimal.api.request.TableRegisterRequest;
import ai.floedb.floecat.gateway.iceberg.minimal.config.MinimalGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.minimal.services.compat.DeltaIcebergMetadataService;
import ai.floedb.floecat.gateway.iceberg.minimal.services.metadata.TableMetadataImportService;
import ai.floedb.floecat.gateway.iceberg.minimal.services.table.ConnectorCleanupService;
import ai.floedb.floecat.gateway.iceberg.minimal.services.table.TableBackend;
import ai.floedb.floecat.gateway.iceberg.minimal.services.table.TableStorageCleanupService;
import ai.floedb.floecat.gateway.iceberg.minimal.services.transaction.TransactionCommitService;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TableRegisterResourceTest {
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
  void registersTableFromImportedMetadata() {
    stubImportedMetadata(
        "s3://bucket/db/orders/metadata/00001.metadata.json", "s3://bucket/db/orders", 77L);
    when(transactionCommitService.registerImported(
            eq("foo"),
            eq("idem-1"),
            eq(java.util.List.of("db")),
            eq("orders"),
            any(),
            any(),
            eq(false)))
        .thenReturn(jakarta.ws.rs.core.Response.noContent().build());
    stubLoadedTable("s3://bucket/db/orders/metadata/00001.metadata.json", 77L);

    LoadTableResultDto dto =
        (LoadTableResultDto)
            resource
                .register(
                    "foo",
                    "db",
                    "idem-1",
                    new TableRegisterRequest(
                        "orders",
                        "s3://bucket/db/orders/metadata/00001.metadata.json",
                        false,
                        Map.of()))
                .getEntity();

    assertEquals("s3://bucket/db/orders/metadata/00001.metadata.json", dto.metadataLocation());
    verify(transactionCommitService)
        .registerImported(
            eq("foo"),
            eq("idem-1"),
            eq(java.util.List.of("db")),
            eq("orders"),
            any(),
            any(),
            eq(false));
  }

  @Test
  void registerRequiresMetadataLocation() {
    assertEquals(
        400,
        resource
            .register("foo", "db", null, new TableRegisterRequest("orders", null, false, Map.of()))
            .getStatus());
  }

  @Test
  void registerOverwriteDelegatesToTransactionalImportPath() {
    stubImportedMetadata(
        "s3://bucket/db/orders/metadata/00002.metadata.json", "s3://bucket/db/orders", 88L);
    when(backend.get("foo", java.util.List.of("db"), "orders"))
        .thenReturn(
            Table.newBuilder()
                .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
                .setDisplayName("orders")
                .setSchemaJson("{\"type\":\"struct\",\"fields\":[]}")
                .putProperties("owner", "team-a")
                .build())
        .thenReturn(
            Table.newBuilder()
                .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
                .setDisplayName("orders")
                .setSchemaJson("{\"type\":\"struct\",\"fields\":[]}")
                .putProperties(
                    "metadata-location", "s3://bucket/db/orders/metadata/00002.metadata.json")
                .build());
    when(transactionCommitService.registerImported(
            eq("foo"),
            eq("idem-1"),
            eq(java.util.List.of("db")),
            eq("orders"),
            any(),
            any(),
            eq(true)))
        .thenReturn(jakarta.ws.rs.core.Response.noContent().build());
    when(backend.currentSnapshot("foo", java.util.List.of("db"), "orders"))
        .thenReturn(
            ai.floedb.floecat.catalog.rpc.Snapshot.newBuilder()
                .setSnapshotId(88L)
                .putFormatMetadata(
                    "iceberg",
                    IcebergMetadata.newBuilder()
                        .setFormatVersion(2)
                        .setMetadataLocation("s3://bucket/db/orders/metadata/00002.metadata.json")
                        .setCurrentSnapshotId(88L)
                        .build()
                        .toByteString())
                .build());
    when(importer.loadTableMetadata(
            eq("s3://bucket/db/orders/metadata/00002.metadata.json"), eq(Map.of())))
        .thenReturn(
            TableMetadata.newTableMetadata(
                SchemaParser.fromJson("{\"type\":\"struct\",\"schema-id\":0,\"fields\":[]}"),
                PartitionSpec.unpartitioned(),
                SortOrder.unsorted(),
                "s3://bucket/db/orders",
                Map.of()));

    assertEquals(
        200,
        resource
            .register(
                "foo",
                "db",
                "idem-1",
                new TableRegisterRequest(
                    "orders", "s3://bucket/db/orders/metadata/00002.metadata.json", true, Map.of()))
            .getStatus());
    verify(transactionCommitService)
        .registerImported(
            eq("foo"),
            eq("idem-1"),
            eq(java.util.List.of("db")),
            eq("orders"),
            any(),
            any(),
            eq(true));
  }

  private void stubImportedMetadata(
      String metadataLocation, String tableLocation, long snapshotId) {
    when(importer.importMetadata(eq(metadataLocation), any()))
        .thenReturn(
            new TableMetadataImportService.ImportedMetadata(
                "{\"type\":\"struct\",\"fields\":[]}",
                Map.of("metadata-location", metadataLocation),
                tableLocation,
                IcebergMetadata.newBuilder()
                    .setFormatVersion(2)
                    .setMetadataLocation(metadataLocation)
                    .setCurrentSnapshotId(snapshotId)
                    .build(),
                new TableMetadataImportService.ImportedSnapshot(
                    snapshotId,
                    null,
                    1L,
                    1234L,
                    tableLocation + "/metadata/snap.avro",
                    Map.of(),
                    0),
                java.util.List.of(
                    new TableMetadataImportService.ImportedSnapshot(
                        snapshotId,
                        null,
                        1L,
                        1234L,
                        tableLocation + "/metadata/snap.avro",
                        Map.of(),
                        0))));
  }

  private void stubLoadedTable(String metadataLocation, long snapshotId) {
    when(backend.get("foo", java.util.List.of("db"), "orders"))
        .thenReturn(
            Table.newBuilder()
                .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
                .setDisplayName("orders")
                .setSchemaJson("{\"type\":\"struct\",\"fields\":[]}")
                .putProperties("metadata-location", metadataLocation)
                .build());
    when(backend.currentSnapshot("foo", java.util.List.of("db"), "orders"))
        .thenReturn(
            ai.floedb.floecat.catalog.rpc.Snapshot.newBuilder()
                .setSnapshotId(snapshotId)
                .putFormatMetadata(
                    "iceberg",
                    IcebergMetadata.newBuilder()
                        .setFormatVersion(2)
                        .setMetadataLocation(metadataLocation)
                        .setCurrentSnapshotId(snapshotId)
                        .build()
                        .toByteString())
                .build());
    when(importer.loadTableMetadata(eq(metadataLocation), eq(Map.of())))
        .thenReturn(
            TableMetadata.newTableMetadata(
                SchemaParser.fromJson("{\"type\":\"struct\",\"schema-id\":0,\"fields\":[]}"),
                PartitionSpec.unpartitioned(),
                SortOrder.unsorted(),
                "s3://bucket/db/orders",
                Map.of()));
  }
}
