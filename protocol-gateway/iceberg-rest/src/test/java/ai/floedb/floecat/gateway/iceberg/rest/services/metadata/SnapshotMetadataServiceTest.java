package ai.floedb.floecat.gateway.iceberg.rest.services.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.CreateSnapshotResponse;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergErrorResponse;
import ai.floedb.floecat.gateway.iceberg.rest.common.TrinoFixtureTestSupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.SnapshotClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.ws.rs.core.Response;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SnapshotMetadataServiceTest {
  private SnapshotMetadataService service;
  private TableGatewaySupport tableSupport;
  private static final TrinoFixtureTestSupport.Fixture FIXTURE =
      TrinoFixtureTestSupport.simpleFixture();

  @BeforeEach
  void setUp() {
    service = new SnapshotMetadataService();
    service.grpc = mock(GrpcWithHeaders.class);
    service.mapper = new ObjectMapper();
    service.snapshotClient = mock(SnapshotClient.class);
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

  private Supplier<Table> neverInvokedTableSupplier() {
    return () -> {
      throw new AssertionError("table should not be loaded");
    };
  }
}
