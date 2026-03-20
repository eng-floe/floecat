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

package ai.floedb.floecat.gateway.iceberg.rest.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.support.TrinoFixtureTestSupport;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergRef;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.FieldMask;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TableUpdatePlannerTest {
  private static final TrinoFixtureTestSupport.Fixture FIXTURE =
      TrinoFixtureTestSupport.simpleFixture();

  private final TableUpdatePlanner planner = new TableUpdatePlanner();
  private final IcebergMetadataService icebergMetadataService = mock(IcebergMetadataService.class);
  private final TablePropertyService propertyService = spy(new TablePropertyService());
  private final SnapshotUpdateService snapshotUpdateService = mock(SnapshotUpdateService.class);
  private final TableGatewaySupport tableSupport = mock(TableGatewaySupport.class);

  @BeforeEach
  void setUp() {
    planner.icebergMetadataService = icebergMetadataService;
    planner.tablePropertyService = propertyService;
    planner.snapshotUpdateService = snapshotUpdateService;
    planner.mapper = new ObjectMapper();
  }

  @Test
  void planUpdatesMergesNamespaceAndProperties() {
    TableRequests.Commit request =
        new TableRequests.Commit(
            List.of(),
            List.of(
                Map.of(
                    "action",
                    "set-properties",
                    "updates",
                    Map.of("retention", "7d", "owner", "alice"))));

    TableUpdatePlanner.UpdatePlan plan =
        planner.planUpdates(
            command(request),
            tableSupplier(),
            ResourceId.newBuilder().setId("cat:db:orders").build());

    assertFalse(plan.hasError());
    TableSpec spec = plan.spec().build();
    FieldMask mask = plan.mask().build();
    assertEquals("7d", spec.getPropertiesOrThrow("retention"));
    assertEquals("alice", spec.getPropertiesOrThrow("owner"));
    assertTrue(mask.getPathsList().contains("properties"));
    verify(propertyService).applyLocationUpdate(any(), any(), any(), any());
  }

  @Test
  void planUpdatesReturnsValidationErrorForUnsupportedAction() {
    TableRequests.Commit request =
        new TableRequests.Commit(List.of(), List.of(Map.of("action", "drop")));

    TableUpdatePlanner.UpdatePlan plan =
        planner.planUpdates(
            command(request),
            tableSupplier(),
            ResourceId.newBuilder().setId("cat:db:orders").build());

    assertTrue(plan.hasError());
    Response error = plan.error();
    assertNotNull(error);
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), error.getStatus());
    assertTrue(error.getEntity().toString().contains("unsupported commit update action"));
  }

  @Test
  void planUpdatesPropagatesRequirementErrors() {
    TableUpdatePlanner.UpdatePlan plan =
        planner.planUpdates(
            command(
                new TableRequests.Commit(
                    List.of(Map.of("type", "assert-table-uuid", "uuid", "different")), List.of())),
            tableSupplier(),
            ResourceId.newBuilder().setId("cat:db:orders").build());

    assertTrue(plan.hasError());
    assertEquals(Response.Status.CONFLICT.getStatusCode(), plan.error().getStatus());
    verify(propertyService, never()).applyCommitPropertyUpdates(any(), any(), any());
  }

  @Test
  void planUpdatesPersistsTableDefinitionProperties() {
    TableRequests.Commit request =
        new TableRequests.Commit(
            List.of(),
            List.of(
                Map.of("action", "upgrade-format-version", "format-version", 2),
                Map.of(
                    "action", "add-schema", "last-column-id", 2, "schema", Map.of("schema-id", 0)),
                Map.of("action", "set-current-schema", "schema-id", 0),
                Map.of("action", "add-spec", "spec", Map.of("spec-id", 0, "fields", List.of())),
                Map.of("action", "set-default-spec", "spec-id", 0),
                Map.of("action", "set-default-sort-order", "sort-order-id", 0)));

    TableUpdatePlanner.UpdatePlan plan =
        planner.planUpdates(
            command(request),
            tableSupplier(),
            ResourceId.newBuilder().setId("cat:db:orders").build());

    assertFalse(plan.hasError());
    Map<String, String> props = plan.spec().build().getPropertiesMap();
    assertEquals("2", props.get("format-version"));
    assertEquals("2", props.get("last-column-id"));
    assertEquals("0", props.get("current-schema-id"));
    assertEquals("0", props.get("last-partition-id"));
    assertEquals("0", props.get("default-spec-id"));
    assertEquals("0", props.get("default-sort-order-id"));
    assertTrue(plan.mask().build().getPathsList().contains("schema_json"));
    assertTrue(plan.spec().build().getSchemaJson().contains("\"schema-id\":0"));
  }

  @Test
  void planUpdatesResolvesSetLastSentinelsForTableDefinitionIds() {
    TableRequests.Commit request =
        new TableRequests.Commit(
            List.of(),
            List.of(
                Map.of(
                    "action",
                    "add-schema",
                    "schema",
                    Map.of(
                        "schema-id",
                        7,
                        "type",
                        "struct",
                        "fields",
                        List.of(
                            Map.of("id", 1, "name", "id", "required", false, "type", "int"),
                            Map.of("id", 2, "name", "v", "required", false, "type", "string")))),
                Map.of("action", "set-current-schema", "schema-id", -1),
                Map.of("action", "add-spec", "spec", Map.of("spec-id", 9, "fields", List.of())),
                Map.of("action", "set-default-spec", "spec-id", -1),
                Map.of(
                    "action",
                    "add-sort-order",
                    "sort-order",
                    Map.of("order-id", 11, "fields", List.of())),
                Map.of("action", "set-default-sort-order", "sort-order-id", -1)));

    TableUpdatePlanner.UpdatePlan plan =
        planner.planUpdates(
            command(request),
            tableSupplier(),
            ResourceId.newBuilder().setId("cat:db:orders").build());

    assertFalse(plan.hasError());
    Map<String, String> props = plan.spec().build().getPropertiesMap();
    assertEquals("2", props.get("last-column-id"));
    assertEquals("7", props.get("current-schema-id"));
    assertEquals("9", props.get("default-spec-id"));
    assertEquals("11", props.get("default-sort-order-id"));
  }

  @Test
  void validateRequirementsUsesMetadataRefSnapshot() {
    long snapshotId = FIXTURE.metadata().getCurrentSnapshotId();
    IcebergMetadata metadata =
        FIXTURE.metadata().toBuilder()
            .putRefs("branch", IcebergRef.newBuilder().setSnapshotId(snapshotId).build())
            .build();
    when(icebergMetadataService.resolveCurrentIcebergMetadata(any(), any())).thenReturn(metadata);

    Response resp =
        planner.validateRequirements(
            tableSupport,
            List.of(
                Map.of(
                    "type", "assert-ref-snapshot-id", "ref", "branch", "snapshot-id", snapshotId)),
            () -> Table.getDefaultInstance(),
            msg -> Response.ok(msg).build(),
            msg -> Response.status(Response.Status.CONFLICT).entity(msg).build());

    assertNull(resp);
  }

  private Supplier<Table> tableSupplier() {
    Table table = Table.newBuilder().putProperties("existing", "value").build();
    return () -> table;
  }

  private TransactionCommitService.CommitCommand command(TableRequests.Commit commit) {
    return new TransactionCommitService.CommitCommand(
        "foo",
        "db",
        List.of("db"),
        "orders",
        "catalog",
        ResourceId.newBuilder().setId("cat").build(),
        ResourceId.newBuilder().setId("cat:db").build(),
        "idem",
        null,
        "txn",
        commit,
        tableSupport);
  }
}
