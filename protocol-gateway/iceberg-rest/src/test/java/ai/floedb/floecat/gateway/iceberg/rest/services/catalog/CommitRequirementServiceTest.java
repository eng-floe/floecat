package ai.floedb.floecat.gateway.iceberg.rest.services.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.common.TrinoFixtureTestSupport;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergRef;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class CommitRequirementServiceTest {
  private final CommitRequirementService service = new CommitRequirementService();
  private final TableGatewaySupport tableSupport = mock(TableGatewaySupport.class);
  private static final TrinoFixtureTestSupport.Fixture FIXTURE =
      TrinoFixtureTestSupport.simpleFixture();

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
  void validateRequirementsUsesTablePropertyWhenMetadataMissing() {
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders").build())
            .putProperties("table-uuid", "prop-uuid")
            .build();

    Response resp =
        service.validateRequirements(
            tableSupport,
            List.of(Map.of("type", "assert-table-uuid", "uuid", "prop-uuid")),
            () -> table,
            validation(),
            conflict());

    assertNull(resp);
  }

  @Test
  void validateRequirementsUsesResourceIdWhenMetadataMissing() {
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders").build())
            .build();

    Response resp =
        service.validateRequirements(
            tableSupport,
            List.of(Map.of("type", "assert-table-uuid", "uuid", "cat:db:orders")),
            () -> table,
            validation(),
            conflict());

    assertNull(resp);
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
    long snapshotId = FIXTURE.metadata().getCurrentSnapshotId();
    IcebergMetadata metadata =
        FIXTURE.metadata().toBuilder()
            .putRefs("branch", IcebergRef.newBuilder().setSnapshotId(snapshotId).build())
            .build();
    when(tableSupport.loadCurrentMetadata(table)).thenReturn(metadata);

    Response resp =
        service.validateRequirements(
            tableSupport,
            List.of(
                Map.of(
                    "type", "assert-ref-snapshot-id", "ref", "branch", "snapshot-id", snapshotId)),
            () -> table,
            validation(),
            conflict());

    assertNull(resp);
  }

  @Test
  void validateRequirementsPrefersMetadataForTableUuid() {
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders").build())
            .putProperties("table-uuid", "prop-uuid")
            .build();
    String fixtureUuid = FIXTURE.metadata().getTableUuid();
    IcebergMetadata metadata = FIXTURE.metadata().toBuilder().setTableUuid(fixtureUuid).build();
    when(tableSupport.loadCurrentMetadata(table)).thenReturn(metadata);

    Response resp =
        service.validateRequirements(
            tableSupport,
            List.of(Map.of("type", "assert-table-uuid", "uuid", fixtureUuid)),
            () -> table,
            validation(),
            conflict());

    assertNull(resp);
  }

  @Test
  void validateRequirementsRejectsPropertyUuidWhenMetadataDiffers() {
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders").build())
            .putProperties("table-uuid", "placeholder-uuid")
            .build();
    IcebergMetadata metadata = FIXTURE.metadata();
    when(tableSupport.loadCurrentMetadata(table)).thenReturn(metadata);

    Response resp =
        service.validateRequirements(
            tableSupport,
            List.of(Map.of("type", "assert-table-uuid", "uuid", "placeholder-uuid")),
            () -> table,
            validation(),
            conflict());

    assertNotNull(resp);
    assertEquals(Response.Status.CONFLICT.getStatusCode(), resp.getStatus());
  }

  @Test
  void validateRequirementsUsesMetadataForMainRefSnapshot() {
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders").build())
            .build();
    IcebergMetadata metadata = FIXTURE.metadata();
    when(tableSupport.loadCurrentMetadata(table)).thenReturn(metadata);

    Response resp =
        service.validateRequirements(
            tableSupport,
            List.of(
                Map.of(
                    "type",
                    "assert-ref-snapshot-id",
                    "ref",
                    "main",
                    "snapshot-id",
                    metadata.getCurrentSnapshotId())),
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
