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

package ai.floedb.floecat.gateway.iceberg.rest.services.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.CreateSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.CreateSnapshotResponse;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergErrorResponse;
import ai.floedb.floecat.gateway.iceberg.rest.common.TrinoFixtureTestSupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.SnapshotClient;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import jakarta.ws.rs.core.Response;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class SnapshotMetadataServiceTest {
  private SnapshotMetadataService service;
  private TableGatewaySupport tableSupport;
  private static final TrinoFixtureTestSupport.Fixture FIXTURE =
      TrinoFixtureTestSupport.simpleFixture();

  @BeforeEach
  void setUp() {
    service = new SnapshotMetadataService();
    service.snapshotClient = mock(SnapshotClient.class);
    SnapshotUpdateService updateService = new SnapshotUpdateService();
    updateService.mapper = new ObjectMapper();
    updateService.snapshotClient = service.snapshotClient;
    service.updateService = updateService;
    tableSupport = mock(TableGatewaySupport.class);
  }

  @Test
  void snapshotAdditionsFiltersNonAddActions() {
    Map<String, Object> add = new LinkedHashMap<>();
    add.put("action", "add-snapshot");
    Map<String, Object> remove = Map.of("action", "remove-snapshots");

    List<Map<String, Object>> additions = service.snapshotAdditions(List.of(add, remove));

    assertEquals(1, additions.size());
    assertSame(add, additions.get(0));
  }

  @Test
  void applySnapshotUpdatesValidatesRemoveSnapshotIds() {
    Response response =
        service.applySnapshotUpdates(
            tableSupport,
            ResourceId.newBuilder().setId("cat:db:orders").build(),
            List.of("db"),
            "orders",
            neverInvokedTableSupplier(),
            List.of(Map.of("action", "remove-snapshots")),
            "commit-key");

    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = (IcebergErrorResponse) response.getEntity();
    assertEquals("remove-snapshots requires snapshot-ids", error.error().message());
    verifyNoInteractions(tableSupport);
  }

  @Test
  void applySnapshotUpdatesValidatesSnapshotRefFields() {
    Response response =
        service.applySnapshotUpdates(
            tableSupport,
            ResourceId.newBuilder().setId("cat:db:orders").build(),
            List.of("db"),
            "orders",
            neverInvokedTableSupplier(),
            List.of(Map.of("action", "set-snapshot-ref")),
            "commit-key");

    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = (IcebergErrorResponse) response.getEntity();
    assertEquals("set-snapshot-ref requires ref-name", error.error().message());
    verifyNoInteractions(tableSupport);
  }

  @Test
  void applySnapshotUpdatesUsesTableSchemaJsonWhenMissingInSnapshot() {
    Table table = Table.newBuilder().setSchemaJson(FIXTURE.table().getSchemaJson()).build();
    when(service.snapshotClient.createSnapshot(any()))
        .thenReturn(CreateSnapshotResponse.newBuilder().build());
    Map<String, Object> snapshot = new LinkedHashMap<>();
    snapshot.put("snapshot-id", 11L);
    snapshot.put("schema-id", 0);
    snapshot.put("sequence-number", 1L);
    snapshot.put("timestamp-ms", 123L);
    Map<String, Object> update = new LinkedHashMap<>();
    update.put("action", "add-snapshot");
    update.put("snapshot", snapshot);

    Response response =
        service.applySnapshotUpdates(
            tableSupport,
            ResourceId.newBuilder().setId("cat:db:orders").build(),
            List.of("db"),
            "orders",
            () -> table,
            List.of(update),
            "commit-key");

    assertNull(response);
  }

  @Test
  void addSnapshotPreservesIncomingMetadataLocationWhenTablePropertyMissing() throws Exception {
    // Simulate a gRPC connector-driven table with NO metadata-location property
    Table table = Table.newBuilder().setSchemaJson(FIXTURE.table().getSchemaJson()).build();
    // The IcebergMetadata carried by the current snapshot DOES have a metadata-location
    // (e.g. from a prior import). The table property is blank—this is the bug scenario.
    IcebergMetadata metadataWithLocation =
        FIXTURE.metadata().toBuilder()
            .setMetadataLocation("s3://warehouse/tables/orders/metadata/00000-abc.metadata.json")
            .build();
    when(tableSupport.loadCurrentMetadata(table)).thenReturn(metadataWithLocation);

    ArgumentCaptor<CreateSnapshotRequest> captor =
        ArgumentCaptor.forClass(CreateSnapshotRequest.class);
    when(service.snapshotClient.createSnapshot(captor.capture()))
        .thenReturn(CreateSnapshotResponse.newBuilder().build());

    Map<String, Object> snapshot = new LinkedHashMap<>();
    snapshot.put("snapshot-id", 42L);
    snapshot.put("schema-id", 0);
    snapshot.put("sequence-number", 1L);
    snapshot.put("timestamp-ms", 123L);
    Map<String, Object> update = new LinkedHashMap<>();
    update.put("action", "add-snapshot");
    update.put("snapshot", snapshot);

    Response response =
        service.applySnapshotUpdates(
            tableSupport,
            ResourceId.newBuilder().setId("cat:db:orders").build(),
            List.of("db"),
            "orders",
            () -> table,
            List.of(update),
            "commit-key");

    assertNull(response);
    // Verify the snapshot's format metadata carries the metadata-location from the
    // incoming IcebergMetadata, even though the table property was blank.
    CreateSnapshotRequest request = captor.getValue();
    ByteString icebergBytes = request.getSpec().getFormatMetadataOrDefault("iceberg", null);
    assertFalse(icebergBytes == null || icebergBytes.isEmpty(), "iceberg metadata must be present");
    IcebergMetadata written = IcebergMetadata.parseFrom(icebergBytes);
    assertEquals(
        "s3://warehouse/tables/orders/metadata/00000-abc.metadata.json",
        written.getMetadataLocation(),
        "metadata-location must be preserved from incoming metadata when table property is blank");
  }

  private Supplier<Table> neverInvokedTableSupplier() {
    return () -> {
      throw new AssertionError("table should not be loaded");
    };
  }
}
