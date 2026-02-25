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

package ai.floedb.floecat.gateway.iceberg.rest.services.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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

  @Test
  void validateRequirementsRejectsNullRequirementEntry() {
    List<Map<String, Object>> requirements = new java.util.ArrayList<>();
    requirements.add(null);
    Response resp =
        service.validateRequirements(
            tableSupport, requirements, () -> Table.newBuilder().build(), validation(), conflict());

    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());
    assertEquals("commit requirement entry cannot be null", resp.getEntity());
  }

  @Test
  void validateRequirementsRejectsMissingRequirementType() {
    Response resp =
        service.validateRequirements(
            tableSupport,
            List.of(Map.of("uuid", "a")),
            () -> Table.newBuilder().build(),
            validation(),
            conflict());

    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());
    assertEquals("commit requirement missing type", resp.getEntity());
  }

  @Test
  void validateRequirementsRejectsUnsupportedRequirementType() {
    Response resp =
        service.validateRequirements(
            tableSupport,
            List.of(Map.of("type", "assert-unknown")),
            () -> Table.newBuilder().build(),
            validation(),
            conflict());

    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());
    assertEquals("unsupported commit requirement: assert-unknown", resp.getEntity());
  }

  @Test
  void validateRequirementsRejectsMissingTableUuidValue() {
    Response resp =
        service.validateRequirements(
            tableSupport,
            List.of(Map.of("type", "assert-table-uuid")),
            () -> Table.newBuilder().build(),
            validation(),
            conflict());

    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());
    assertEquals("assert-table-uuid requires uuid", resp.getEntity());
  }

  @Test
  void validateRequirementsRejectsMissingRefName() {
    Response resp =
        service.validateRequirements(
            tableSupport,
            List.of(Map.of("type", "assert-ref-snapshot-id", "snapshot-id", 7)),
            () -> Table.newBuilder().build(),
            validation(),
            conflict());

    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());
    assertEquals("assert-ref-snapshot-id requires ref", resp.getEntity());
  }

  @Test
  void validateRequirementsSkipsRefSnapshotCheckWhenSnapshotMissing() {
    Response resp =
        service.validateRequirements(
            tableSupport,
            List.of(Map.of("type", "assert-ref-snapshot-id", "ref", "main")),
            () -> Table.newBuilder().build(),
            validation(),
            conflict());

    assertNull(resp);
  }

  @Test
  void validateRequirementsChecksSchemaAndAssignedIds() {
    Table table = Table.newBuilder().build();
    IcebergMetadata metadata =
        FIXTURE.metadata().toBuilder()
            .setCurrentSchemaId(33)
            .setLastColumnId(88)
            .setLastPartitionId(5)
            .setDefaultSpecId(9)
            .setDefaultSortOrderId(2)
            .build();
    when(tableSupport.loadCurrentMetadata(table)).thenReturn(metadata);

    Response resp =
        service.validateRequirements(
            tableSupport,
            List.of(
                Map.of("type", "assert-current-schema-id", "current-schema-id", 33),
                Map.of("type", "assert-last-assigned-field-id", "last-assigned-field-id", 88),
                Map.of(
                    "type", "assert-last-assigned-partition-id", "last-assigned-partition-id", 5),
                Map.of("type", "assert-default-spec-id", "default-spec-id", 9),
                Map.of("type", "assert-default-sort-order-id", "default-sort-order-id", 2)),
            () -> table,
            validation(),
            conflict());

    assertNull(resp);
    verify(tableSupport, times(1)).loadCurrentMetadata(table);
  }

  @Test
  void validateRequirementsReturnsConflictForSchemaMismatch() {
    Table table = Table.newBuilder().build();
    IcebergMetadata metadata = FIXTURE.metadata().toBuilder().setCurrentSchemaId(11).build();
    when(tableSupport.loadCurrentMetadata(table)).thenReturn(metadata);

    Response resp =
        service.validateRequirements(
            tableSupport,
            List.of(Map.of("type", "assert-current-schema-id", "current-schema-id", 22)),
            () -> table,
            validation(),
            conflict());

    assertNotNull(resp);
    assertEquals(Response.Status.CONFLICT.getStatusCode(), resp.getStatus());
    assertTrue(resp.getEntity().toString().contains("assert-current-schema-id failed"));
  }

  private Function<String, Response> validation() {
    return message -> Response.status(Response.Status.BAD_REQUEST).entity(message).build();
  }

  private Function<String, Response> conflict() {
    return message -> Response.status(Response.Status.CONFLICT).entity(message).build();
  }
}
