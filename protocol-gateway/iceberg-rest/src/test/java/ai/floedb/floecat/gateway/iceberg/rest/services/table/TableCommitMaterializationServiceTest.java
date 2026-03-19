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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.common.TrinoFixtureTestSupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.CanonicalTableMetadataService;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.MaterializeMetadataResult;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.MaterializeMetadataService;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.MaterializeMetadataService.MaterializeResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Optional;
import org.apache.iceberg.TableMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TableCommitMaterializationServiceTest {
  private final TableCommitMaterializationService service = new TableCommitMaterializationService();
  private final MaterializeMetadataService materializeMetadataService =
      mock(MaterializeMetadataService.class);
  private final IcebergGatewayConfig config = mock(IcebergGatewayConfig.class);
  private final CanonicalTableMetadataService canonicalTableMetadataService =
      new CanonicalTableMetadataService();

  @BeforeEach
  void setUp() {
    canonicalTableMetadataService.setMapper(new ObjectMapper());
    service.materializeMetadataService = materializeMetadataService;
    service.config = config;
    service.canonicalTableMetadataService = canonicalTableMetadataService;
    when(config.defaultWarehousePath()).thenReturn(Optional.empty());
  }

  @Test
  void materializeMetadataReturnsResolvedMetadataLocation() {
    TableMetadata metadata =
        fixtureMetadata("s3://warehouse/tables/orders/metadata/00000-abc.metadata.json");
    TableMetadata materialized =
        withMetadataLocation(
            metadata, "s3://warehouse/tables/orders/metadata/00001-abc.metadata.json");
    when(materializeMetadataService.materialize(any(), any(), any(TableMetadata.class), any()))
        .thenReturn(
            new MaterializeResult(
                "s3://warehouse/tables/orders/metadata/00001-abc.metadata.json", materialized));

    MaterializeMetadataResult result =
        service.materializeMetadata(
            "cat.db",
            ResourceId.newBuilder().setId("cat:db:orders").build(),
            "orders",
            Table.newBuilder()
                .setResourceId(ResourceId.newBuilder().setId("cat:db:orders").build())
                .build(),
            metadata,
            "s3://warehouse/tables/orders/metadata/00000-abc.metadata.json");

    assertNull(result.error());
    assertEquals(
        "s3://warehouse/tables/orders/metadata/00001-abc.metadata.json", result.metadataLocation());
    assertEquals(materialized, result.tableMetadata());
    assertEquals(result.metadataLocation(), result.metadata().metadataLocation());
  }

  @Test
  void materializeMetadataSkipsWhenNoLocationProvided() {
    TableMetadata metadata = withMetadataLocation(withLocation(fixtureMetadata(null), null), null);
    when(materializeMetadataService.materialize(any(), any(), any(TableMetadata.class), any()))
        .thenReturn(new MaterializeResult(null, metadata));

    MaterializeMetadataResult result =
        service.materializeMetadata("cat.db", null, "orders", null, metadata, null);

    assertNull(result.error());
    assertNull(result.metadataLocation());
    assertEquals(metadata, result.tableMetadata());
  }

  @Test
  void materializeMetadataDerivesLocationFromTableWhenMissing() {
    TableMetadata metadata = withMetadataLocation(withLocation(fixtureMetadata(null), null), null);
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders").build())
            .putProperties("location", "s3://warehouse/tables/orders")
            .build();
    when(materializeMetadataService.materialize(any(), any(), any(TableMetadata.class), any()))
        .thenReturn(
            new MaterializeResult(
                "s3://warehouse/tables/orders/metadata/00001-new.metadata.json", metadata));

    MaterializeMetadataResult result =
        service.materializeMetadata("cat.db", null, "orders", table, metadata, null);

    assertNull(result.error());
    assertEquals(
        "s3://warehouse/tables/orders/metadata/00001-new.metadata.json", result.metadataLocation());
  }

  @Test
  void materializeMetadataSetsTableLocationFromMetadataPathWhenMissing() {
    TableMetadata metadata = withMetadataLocation(withLocation(fixtureMetadata(null), null), null);
    when(materializeMetadataService.materialize(any(), any(), any(TableMetadata.class), any()))
        .thenReturn(
            new MaterializeResult(
                "s3://warehouse/tables/orders/metadata/00001-new.metadata.json", metadata));

    MaterializeMetadataResult result =
        service.materializeMetadata(
            "cat.db", null, "orders", null, metadata, "s3://warehouse/tables/orders/metadata/");

    assertNull(result.error());
    assertNotNull(result.tableMetadata());
  }

  @Test
  void materializeMetadataDerivesDefaultWarehouseLocation() {
    when(config.defaultWarehousePath()).thenReturn(Optional.of("s3://warehouse-root"));
    TableMetadata metadata = withMetadataLocation(withLocation(fixtureMetadata(null), null), null);
    when(materializeMetadataService.materialize(any(), any(), any(TableMetadata.class), any()))
        .thenReturn(
            new MaterializeResult(
                "s3://warehouse-root/cat/db/orders/metadata/00001-new.metadata.json", metadata));

    MaterializeMetadataResult result =
        service.materializeMetadata("cat.db", null, "orders", null, metadata, null);

    assertNull(result.error());
    assertEquals(
        "s3://warehouse-root/cat/db/orders/metadata/00001-new.metadata.json",
        result.metadataLocation());
  }

  private TableMetadata fixtureMetadata(String metadataLocation) {
    var fixture = TrinoFixtureTestSupport.simpleFixture();
    return canonicalTableMetadataService.bootstrapTableMetadata(
        fixture.table().getDisplayName(),
        fixture.table(),
        fixture.table().getPropertiesMap(),
        fixture.metadata().toBuilder()
            .setMetadataLocation(
                metadataLocation == null ? fixture.metadataLocation() : metadataLocation)
            .build(),
        fixture.snapshots());
  }

  private TableMetadata withMetadataLocation(TableMetadata metadata, String metadataLocation) {
    if (metadata == null) {
      return null;
    }
    if (metadataLocation == null || metadataLocation.isBlank()) {
      return TableMetadata.buildFrom(metadata).discardChanges().build();
    }
    return TableMetadata.buildFrom(metadata)
        .discardChanges()
        .withMetadataLocation(metadataLocation)
        .build();
  }

  private TableMetadata withLocation(TableMetadata metadata, String location) {
    if (metadata == null || location == null) {
      return metadata == null ? null : TableMetadata.buildFrom(metadata).discardChanges().build();
    }
    return TableMetadata.buildFrom(metadata).discardChanges().setLocation(location).build();
  }
}
