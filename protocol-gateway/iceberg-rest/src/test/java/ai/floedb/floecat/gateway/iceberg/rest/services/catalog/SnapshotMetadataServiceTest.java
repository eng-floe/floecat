package ai.floedb.floecat.gateway.iceberg.rest.services.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import ai.floedb.floecat.catalog.rpc.PartitionSpecInfo;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergErrorResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.ws.rs.core.Response;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SnapshotMetadataServiceTest {
  private SnapshotMetadataService service;
  private TableGatewaySupport tableSupport;

  @BeforeEach
  void setUp() {
    service = new SnapshotMetadataService();
    service.grpc = mock(GrpcWithHeaders.class);
    service.mapper = new ObjectMapper();
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
            "commit-key",
            new SnapshotUpdateContext());

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
            "commit-key",
            new SnapshotUpdateContext());

    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    IcebergErrorResponse error = (IcebergErrorResponse) response.getEntity();
    assertEquals("set-snapshot-ref requires ref-name", error.error().message());
    verifyNoInteractions(tableSupport);
  }

  @Test
  void buildPartitionSpecAllowsEmptyFields() throws Exception {
    Map<String, Object> spec = new LinkedHashMap<>();
    spec.put("spec-id", 7);
    spec.put("fields", List.of());

    var method = SnapshotMetadataService.class.getDeclaredMethod("buildPartitionSpec", Map.class);
    method.setAccessible(true);
    PartitionSpecInfo specInfo = (PartitionSpecInfo) method.invoke(service, spec);

    Assertions.assertEquals(7, specInfo.getSpecId());
    Assertions.assertEquals(0, specInfo.getFieldsCount());
  }

  @Test
  void buildSortOrderAllowsEmptyFields() throws Exception {
    Map<String, Object> order = new LinkedHashMap<>();
    order.put("sort-order-id", 3);
    order.put("fields", List.of());

    var method = SnapshotMetadataService.class.getDeclaredMethod("buildSortOrder", Map.class);
    method.setAccessible(true);
    var sortOrder =
        (ai.floedb.floecat.gateway.iceberg.rpc.IcebergSortOrder) method.invoke(service, order);
    Assertions.assertEquals(3, sortOrder.getSortOrderId());
    Assertions.assertEquals(0, sortOrder.getFieldsCount());
  }

  @Test
  void snapshotMetadataSynthesizesFromTablePropertiesWhenMissing() throws Exception {
    Table table =
        Table.newBuilder()
            .putProperties("metadata-location", "s3://bucket/orders/metadata/00000.metadata.json")
            .putProperties("table-uuid", "uuid")
            .putProperties("format-version", "2")
            .putProperties("current-schema-id", "1")
            .putProperties("last-column-id", "10")
            .putProperties("default-spec-id", "3")
            .putProperties("last-partition-id", "4")
            .putProperties("default-sort-order-id", "5")
            .putProperties("current-snapshot-id", "6")
            .putProperties("last-sequence-number", "7")
            .build();
    var method =
        SnapshotMetadataService.class.getDeclaredMethod(
            "snapshotIcebergMetadata",
            ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata.class,
            Table.class,
            Long.class,
            Long.class);
    method.setAccessible(true);

    ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata metadata =
        (ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata)
            method.invoke(service, null, table, 9L, 11L);

    assertEquals("s3://bucket/orders/metadata/00000.metadata.json", metadata.getMetadataLocation());
    assertEquals(9L, metadata.getCurrentSnapshotId());
    assertEquals(11L, metadata.getLastSequenceNumber());
    assertEquals(2, metadata.getFormatVersion());
    assertEquals("uuid", metadata.getTableUuid());
  }

  private Supplier<ai.floedb.floecat.catalog.rpc.Table> neverInvokedTableSupplier() {
    return () -> {
      throw new AssertionError("table should not be loaded");
    };
  }
}
