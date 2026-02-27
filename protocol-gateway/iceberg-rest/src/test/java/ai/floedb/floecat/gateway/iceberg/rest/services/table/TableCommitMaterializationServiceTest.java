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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.common.TableMetadataBuilder;
import ai.floedb.floecat.gateway.iceberg.rest.common.TrinoFixtureTestSupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.MaterializeMetadataResult;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.MaterializeMetadataService;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.MaterializeMetadataService.MaterializeResult;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.TableMetadataImportService;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TableCommitMaterializationServiceTest {
  private static final TrinoFixtureTestSupport.Fixture FIXTURE =
      TrinoFixtureTestSupport.simpleFixture();
  private final TableCommitMaterializationService service = new TableCommitMaterializationService();
  private final MaterializeMetadataService materializeMetadataService =
      mock(MaterializeMetadataService.class);
  private final TableMetadataImportService tableMetadataImportService =
      mock(TableMetadataImportService.class);
  private final IcebergGatewayConfig config = mock(IcebergGatewayConfig.class);

  @BeforeEach
  void setUp() {
    service.materializeMetadataService = materializeMetadataService;
    service.tableMetadataImportService = tableMetadataImportService;
    service.config = config;
    when(config.defaultWarehousePath()).thenReturn(Optional.empty());
  }

  @Test
  void materializeMetadataReturnsResolvedMetadataLocation() throws Exception {
    TableMetadataView metadata =
        metadata("s3://warehouse/tables/orders/metadata/00000-abc.metadata.json");
    TableMetadataView materialized =
        metadata.withMetadataLocation(
            "s3://warehouse/tables/orders/metadata/00001-abc.metadata.json");
    when(materializeMetadataService.materialize(
            "cat.db",
            "orders",
            metadata,
            "s3://warehouse/tables/orders/metadata/00000-abc.metadata.json"))
        .thenReturn(
            new MaterializeResult(
                "s3://warehouse/tables/orders/metadata/00001-abc.metadata.json", materialized));
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();

    Table table =
        Table.newBuilder()
            .setResourceId(tableId)
            .setUpstream(
                UpstreamRef.newBuilder()
                    .setFormat(TableFormat.TF_ICEBERG)
                    .setColumnIdAlgorithm(ColumnIdAlgorithm.CID_FIELD_ID)
                    .setConnectorId(ResourceId.newBuilder().setId("conn-1").build())
                    .build())
            .build();
    MaterializeMetadataResult result =
        service.materializeMetadata(
            "cat.db",
            tableId,
            "orders",
            table,
            metadata,
            "s3://warehouse/tables/orders/metadata/00000-abc.metadata.json");

    assertNull(result.error());
    assertSame(materialized, result.metadata());
    assertEquals(
        "s3://warehouse/tables/orders/metadata/00001-abc.metadata.json", result.metadataLocation());
  }

  @Test
  void materializeMetadataSkipsWhenNoLocationProvided() throws Exception {
    TableMetadataView base =
        metadata("s3://warehouse/tables/orders/metadata/00000-abc.metadata.json");
    Map<String, String> props = new LinkedHashMap<>(base.properties());
    props.remove("metadata-location");
    props.remove("location");
    TableMetadataView noLocation =
        new TableMetadataView(
            base.formatVersion(),
            base.tableUuid(),
            null,
            null,
            base.lastUpdatedMs(),
            props,
            base.lastColumnId(),
            base.currentSchemaId(),
            base.defaultSpecId(),
            base.lastPartitionId(),
            base.defaultSortOrderId(),
            base.currentSnapshotId(),
            base.lastSequenceNumber(),
            base.schemas(),
            base.partitionSpecs(),
            base.sortOrders(),
            base.refs(),
            base.snapshotLog(),
            base.metadataLog(),
            base.statistics(),
            base.partitionStatistics(),
            base.snapshots());
    when(materializeMetadataService.materialize("cat.db", "orders", noLocation, null))
        .thenReturn(new MaterializeResult(null, noLocation));

    MaterializeMetadataResult result =
        service.materializeMetadata("cat.db", null, "orders", null, noLocation, null);

    assertNull(result.error());
    assertSame(noLocation, result.metadata());
    assertNull(result.metadataLocation());
  }

  @Test
  void materializeMetadataDerivesLocationFromTableWhenMissing() throws Exception {
    TableMetadataView base =
        metadata("s3://warehouse/tables/orders/metadata/00000-abc.metadata.json");
    Map<String, String> props = new LinkedHashMap<>(base.properties());
    props.remove("metadata-location");
    props.put("location", "s3://warehouse/tables/orders");
    TableMetadataView noMetadataLocation =
        new TableMetadataView(
            base.formatVersion(),
            base.tableUuid(),
            "s3://warehouse/tables/orders",
            null,
            base.lastUpdatedMs(),
            props,
            base.lastColumnId(),
            base.currentSchemaId(),
            base.defaultSpecId(),
            base.lastPartitionId(),
            base.defaultSortOrderId(),
            base.currentSnapshotId(),
            base.lastSequenceNumber(),
            base.schemas(),
            base.partitionSpecs(),
            base.sortOrders(),
            base.refs(),
            base.snapshotLog(),
            base.metadataLog(),
            base.statistics(),
            base.partitionStatistics(),
            base.snapshots());
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    Table table =
        Table.newBuilder()
            .setResourceId(tableId)
            .putProperties("location", "s3://warehouse/tables/orders")
            .build();
    TableMetadataView materialized =
        noMetadataLocation.withMetadataLocation(
            "s3://warehouse/tables/orders/metadata/00001-new.metadata.json");
    when(materializeMetadataService.materialize(
            eq("cat.db"),
            eq("orders"),
            any(TableMetadataView.class),
            eq("s3://warehouse/tables/orders/metadata/")))
        .thenReturn(
            new MaterializeResult(
                "s3://warehouse/tables/orders/metadata/00001-new.metadata.json", materialized));

    MaterializeMetadataResult result =
        service.materializeMetadata("cat.db", tableId, "orders", table, noMetadataLocation, null);

    assertNull(result.error());
    assertEquals(
        "s3://warehouse/tables/orders/metadata/00001-new.metadata.json", result.metadataLocation());
  }

  @Test
  void materializeMetadataDerivesLocationFromUpstreamUri() throws Exception {
    TableMetadataView base =
        metadata("s3://warehouse/tables/orders/metadata/00000-abc.metadata.json");
    Map<String, String> props = new LinkedHashMap<>(base.properties());
    props.remove("metadata-location");
    TableMetadataView noMetadataLocation =
        new TableMetadataView(
            base.formatVersion(),
            base.tableUuid(),
            null,
            null,
            base.lastUpdatedMs(),
            props,
            base.lastColumnId(),
            base.currentSchemaId(),
            base.defaultSpecId(),
            base.lastPartitionId(),
            base.defaultSortOrderId(),
            base.currentSnapshotId(),
            base.lastSequenceNumber(),
            base.schemas(),
            base.partitionSpecs(),
            base.sortOrders(),
            base.refs(),
            base.snapshotLog(),
            base.metadataLog(),
            base.statistics(),
            base.partitionStatistics(),
            base.snapshots());
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    Table table =
        Table.newBuilder()
            .setResourceId(tableId)
            .setUpstream(
                UpstreamRef.newBuilder()
                    .setFormat(TableFormat.TF_ICEBERG)
                    .setUri("s3://warehouse/tables/orders")
                    .build())
            .build();
    TableMetadataView materialized =
        noMetadataLocation.withMetadataLocation(
            "s3://warehouse/tables/orders/metadata/00001-new.metadata.json");
    when(materializeMetadataService.materialize(
            eq("cat.db"),
            eq("orders"),
            any(TableMetadataView.class),
            eq("s3://warehouse/tables/orders/metadata/")))
        .thenReturn(
            new MaterializeResult(
                "s3://warehouse/tables/orders/metadata/00001-new.metadata.json", materialized));

    MaterializeMetadataResult result =
        service.materializeMetadata("cat.db", tableId, "orders", table, noMetadataLocation, null);

    assertNull(result.error());
    assertEquals(
        "s3://warehouse/tables/orders/metadata/00001-new.metadata.json", result.metadataLocation());
  }

  @Test
  void materializeMetadataDerivesLocationFromDefaultWarehouseWhenMissing() throws Exception {
    when(config.defaultWarehousePath()).thenReturn(Optional.of("s3://warehouse"));
    TableMetadataView base =
        metadata("s3://warehouse/iceberg/orders/metadata/00000-abc.metadata.json");
    Map<String, String> props = new LinkedHashMap<>(base.properties());
    props.remove("metadata-location");
    props.remove("location");
    TableMetadataView noLocation =
        new TableMetadataView(
            base.formatVersion(),
            base.tableUuid(),
            null,
            null,
            base.lastUpdatedMs(),
            props,
            base.lastColumnId(),
            base.currentSchemaId(),
            base.defaultSpecId(),
            base.lastPartitionId(),
            base.defaultSortOrderId(),
            base.currentSnapshotId(),
            base.lastSequenceNumber(),
            base.schemas(),
            base.partitionSpecs(),
            base.sortOrders(),
            base.refs(),
            base.snapshotLog(),
            base.metadataLog(),
            base.statistics(),
            base.partitionStatistics(),
            base.snapshots());
    when(materializeMetadataService.materialize(
            eq("iceberg"),
            eq("orders"),
            any(TableMetadataView.class),
            eq("s3://warehouse/iceberg/orders/metadata/")))
        .thenReturn(
            new MaterializeResult(
                "s3://warehouse/iceberg/orders/metadata/00001-new.metadata.json",
                noLocation.withMetadataLocation(
                    "s3://warehouse/iceberg/orders/metadata/00001-new.metadata.json")));

    MaterializeMetadataResult result =
        service.materializeMetadata("iceberg", null, "orders", null, noLocation, null);

    assertNull(result.error());
    assertEquals(
        "s3://warehouse/iceberg/orders/metadata/00001-new.metadata.json",
        result.metadataLocation());
  }

  @Test
  void materializeMetadataBackfillsRequiredIdsForParser() throws Exception {
    TableMetadataView missingRequiredIds =
        new TableMetadataView(
            2,
            "f7adfd2a-c9f4-4b2f-a053-60e14b7a5ad6",
            "s3://warehouse/tables/orders",
            null,
            System.currentTimeMillis(),
            Map.of(),
            null,
            null,
            null,
            null,
            null,
            null,
            0L,
            List.of(Map.of("type", "struct", "schema-id", 0, "fields", List.of())),
            List.of(),
            List.of(),
            Map.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());
    when(materializeMetadataService.materialize(
            eq("cat.db"),
            eq("orders"),
            argThat(
                md ->
                    md != null
                        && md.lastColumnId() != null
                        && md.currentSchemaId() != null
                        && md.defaultSpecId() != null
                        && md.lastPartitionId() != null
                        && md.defaultSortOrderId() != null),
            eq("s3://warehouse/tables/orders/metadata/")))
        .thenReturn(
            new MaterializeResult(
                "s3://warehouse/tables/orders/metadata/00001-new.metadata.json",
                missingRequiredIds.withMetadataLocation(
                    "s3://warehouse/tables/orders/metadata/00001-new.metadata.json")));

    MaterializeMetadataResult result =
        service.materializeMetadata(
            "cat.db",
            null,
            "orders",
            null,
            missingRequiredIds,
            "s3://warehouse/tables/orders/metadata/");

    assertNull(result.error());
    assertEquals(
        "s3://warehouse/tables/orders/metadata/00001-new.metadata.json", result.metadataLocation());
  }

  @Test
  void materializeMetadataUsesExistingSchemaWhenRequestMetadataOmitsIt() throws Exception {
    TableMetadataView existing =
        metadata("s3://warehouse/tables/orders/metadata/00000-existing.metadata.json");
    Table table =
        FIXTURE.table().toBuilder()
            .putProperties("metadata-location", existing.metadataLocation())
            .build();
    TableMetadataView snapshotOnlyMetadata =
        new TableMetadataView(
            2,
            existing.tableUuid(),
            existing.location(),
            null,
            existing.lastUpdatedMs(),
            existing.properties(),
            existing.lastColumnId(),
            existing.currentSchemaId(),
            existing.defaultSpecId(),
            existing.lastPartitionId(),
            existing.defaultSortOrderId(),
            existing.currentSnapshotId(),
            1L,
            List.of(),
            List.of(),
            List.of(),
            existing.refs(),
            existing.snapshotLog(),
            existing.metadataLog(),
            existing.statistics(),
            existing.partitionStatistics(),
            existing.snapshots());
    when(tableMetadataImportService.importMetadata(eq(existing.metadataLocation()), any()))
        .thenReturn(
            new TableMetadataImportService.ImportedMetadata(
                table.getSchemaJson(),
                Map.of(),
                existing.location(),
                FIXTURE.metadata(),
                null,
                List.of()));
    when(materializeMetadataService.materialize(
            eq("cat.db"),
            eq("orders"),
            argThat(
                md ->
                    md != null
                        && md.schemas() != null
                        && !md.schemas().isEmpty()
                        && md.schemas().getFirst().get("fields") instanceof List<?> fields
                        && !fields.isEmpty()),
            any()))
        .thenReturn(
            new MaterializeResult(
                "s3://warehouse/tables/orders/metadata/00001-new.metadata.json",
                snapshotOnlyMetadata.withMetadataLocation(
                    "s3://warehouse/tables/orders/metadata/00001-new.metadata.json")));

    MaterializeMetadataResult result =
        service.materializeMetadata("cat.db", null, "orders", table, snapshotOnlyMetadata, null);

    assertNull(result.error());
    assertEquals(
        "s3://warehouse/tables/orders/metadata/00001-new.metadata.json", result.metadataLocation());
  }

  @Test
  void materializeMetadataKeepsClientProvidedSchemaWhenPresent() throws Exception {
    TableMetadataView existing =
        metadata("s3://warehouse/tables/orders/metadata/00000-existing.metadata.json");
    Table table =
        FIXTURE.table().toBuilder()
            .putProperties("metadata-location", existing.metadataLocation())
            .build();
    Map<String, Object> schemaChange =
        Map.of(
            "type",
            "struct",
            "schema-id",
            7,
            "fields",
            List.of(Map.of("id", 10, "name", "new_id", "required", false, "type", "long")));
    TableMetadataView schemaChangeMetadata =
        new TableMetadataView(
            2,
            existing.tableUuid(),
            existing.location(),
            null,
            existing.lastUpdatedMs(),
            existing.properties(),
            10,
            7,
            existing.defaultSpecId(),
            existing.lastPartitionId(),
            existing.defaultSortOrderId(),
            existing.currentSnapshotId(),
            1L,
            List.of(schemaChange),
            existing.partitionSpecs(),
            existing.sortOrders(),
            existing.refs(),
            existing.snapshotLog(),
            existing.metadataLog(),
            existing.statistics(),
            existing.partitionStatistics(),
            existing.snapshots());
    when(materializeMetadataService.materialize(
            eq("cat.db"),
            eq("orders"),
            argThat(
                md ->
                    md != null
                        && md.schemas() != null
                        && !md.schemas().isEmpty()
                        && md.schemas().getFirst().get("schema-id").equals(7)),
            any()))
        .thenReturn(
            new MaterializeResult(
                "s3://warehouse/tables/orders/metadata/00001-new.metadata.json",
                schemaChangeMetadata.withMetadataLocation(
                    "s3://warehouse/tables/orders/metadata/00001-new.metadata.json")));

    MaterializeMetadataResult result =
        service.materializeMetadata("cat.db", null, "orders", table, schemaChangeMetadata, null);

    assertNull(result.error());
    verify(tableMetadataImportService, never()).importMetadata(any(), any());
    assertEquals(
        "s3://warehouse/tables/orders/metadata/00001-new.metadata.json", result.metadataLocation());
  }

  private TableMetadataView metadata(String metadataLocation) {
    TableMetadataView base =
        TableMetadataBuilder.fromCatalog(
            "orders",
            FIXTURE.table(),
            new LinkedHashMap<>(FIXTURE.table().getPropertiesMap()),
            FIXTURE.metadata(),
            FIXTURE.snapshots());
    return base.withMetadataLocation(metadataLocation);
  }
}
