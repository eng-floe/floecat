package ai.floedb.metacat.gateway.iceberg.rest.services.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.metacat.gateway.iceberg.rest.api.error.IcebergErrorResponse;
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

  private Supplier<ai.floedb.metacat.catalog.rpc.Table> neverInvokedTableSupplier() {
    return () -> {
      throw new AssertionError("table should not be loaded");
    };
  }
}
