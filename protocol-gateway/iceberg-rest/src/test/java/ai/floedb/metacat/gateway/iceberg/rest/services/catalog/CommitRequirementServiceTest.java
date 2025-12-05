package ai.floedb.metacat.gateway.iceberg.rest.services.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.metacat.catalog.rpc.IcebergMetadata;
import ai.floedb.metacat.catalog.rpc.IcebergRef;
import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.common.rpc.ResourceId;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class CommitRequirementServiceTest {
  private final CommitRequirementService service = new CommitRequirementService();
  private final TableGatewaySupport tableSupport = mock(TableGatewaySupport.class);

  @Test
  void validateRequirementsReturnsNullWhenUnset() {
    Response resp =
        service.validateRequirements(
            tableSupport, List.of(), () -> Table.getDefaultInstance(), validation(), conflict());

    assertNull(resp);
  }

  @Test
  void validateRequirementsDetectsTableUuidMismatch() {
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders").build())
            .build();
    Response resp =
        service.validateRequirements(
            tableSupport,
            List.of(Map.of("type", "assert-table-uuid", "uuid", "different")),
            () -> table,
            validation(),
            conflict());

    assertEquals(409, resp.getStatus());
    assertEquals("assert-table-uuid failed", resp.getEntity());
  }

  @Test
  void validateRequirementsChecksRefSnapshotIds() {
    Table table =
        Table.newBuilder()
            .putProperties("current-snapshot-id", "42")
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders").build())
            .build();
    Response resp =
        service.validateRequirements(
            tableSupport,
            List.of(Map.of("type", "assert-ref-snapshot-id", "ref", "main", "snapshot-id", 99)),
            () -> table,
            validation(),
            conflict());

    assertEquals(409, resp.getStatus());
    assertEquals("assert-ref-snapshot-id failed for ref main", resp.getEntity());
  }

  @Test
  void validateRequirementsAccessesMetadataRefs() {
    Table table = Table.newBuilder().build();
    IcebergMetadata metadata =
        IcebergMetadata.newBuilder()
            .putRefs("branch", IcebergRef.newBuilder().setSnapshotId(5).build())
            .build();
    when(tableSupport.loadCurrentMetadata(table)).thenReturn(metadata);

    Response resp =
        service.validateRequirements(
            tableSupport,
            List.of(Map.of("type", "assert-ref-snapshot-id", "ref", "branch", "snapshot-id", 5)),
            () -> table,
            validation(),
            conflict());

    assertNull(resp);
  }

  private Function<String, Response> validation() {
    return message -> Response.status(Response.Status.BAD_REQUEST).entity(message).build();
  }

  private Function<String, Response> conflict() {
    return message -> Response.status(Response.Status.CONFLICT).entity(message).build();
  }
}
