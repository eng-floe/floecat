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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.CommitRequirementService;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import com.google.protobuf.FieldMask;
import jakarta.ws.rs.core.Response;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TableUpdatePlannerTest {

  private final TableUpdatePlanner planner = new TableUpdatePlanner();
  private final CommitRequirementService requirements = mock(CommitRequirementService.class);
  private final TablePropertyService propertyService = mock(TablePropertyService.class);
  private final TableGatewaySupport tableSupport = mock(TableGatewaySupport.class);

  @BeforeEach
  void setUp() {
    planner.commitRequirementService = requirements;
    planner.tablePropertyService = propertyService;
  }

  @Test
  void planUpdatesMergesNamespaceAndProperties() {
    when(requirements.validateRequirements(any(), any(), any(), any(), any())).thenReturn(null);
    when(propertyService.hasPropertyUpdates(any())).thenReturn(true);
    doAnswer(
            inv -> {
              @SuppressWarnings("unchecked")
              Map<String, String> props = inv.getArgument(0, Map.class);
              props.remove("metadata-location");
              return null;
            })
        .when(propertyService)
        .stripMetadataLocation(any());
    doAnswer(
            inv -> {
              @SuppressWarnings("unchecked")
              Map<String, String> props = inv.getArgument(0, Map.class);
              @SuppressWarnings("unchecked")
              List<Map<String, Object>> updates = inv.getArgument(1, List.class);
              if (updates != null) {
                for (Map<String, Object> update : updates) {
                  if (update == null) {
                    continue;
                  }
                  Object action = update.get("action");
                  if (!"set-properties".equals(action)) {
                    continue;
                  }
                  Object rawUpdates = update.get("updates");
                  if (rawUpdates instanceof Map<?, ?> map) {
                    for (Map.Entry<?, ?> entry : map.entrySet()) {
                      if (entry.getKey() != null && entry.getValue() != null) {
                        props.put(entry.getKey().toString(), entry.getValue().toString());
                      }
                    }
                  }
                }
              }
              props.put("owner", "alice");
              return null;
            })
        .when(propertyService)
        .applyPropertyUpdates(any(), any());
    when(propertyService.ensurePropertyMap(any(), any())).thenAnswer(inv -> new LinkedHashMap<>());
    when(propertyService.applyLocationUpdate(any(), any(), any(), any())).thenReturn(null);
    TableRequests.Commit request =
        new TableRequests.Commit(
            List.of(),
            List.of(Map.of("action", "set-properties", "updates", Map.of("retention", "7d"))));

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
    when(requirements.validateRequirements(any(), any(), any(), any(), any())).thenReturn(null);
    when(propertyService.hasPropertyUpdates(any())).thenReturn(false);
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
    Response failure = Response.status(Response.Status.CONFLICT).entity("conflict").build();
    when(requirements.validateRequirements(any(), any(), any(), any(), any())).thenReturn(failure);

    TableUpdatePlanner.UpdatePlan plan =
        planner.planUpdates(
            command(new TableRequests.Commit(List.of(), List.of())),
            tableSupplier(),
            ResourceId.newBuilder().setId("cat:db:orders").build());

    assertTrue(plan.hasError());
    assertSame(failure, plan.error());
    verify(propertyService, never()).applyPropertyUpdates(any(), any());
  }

  @Test
  void planUpdatesPersistsTableDefinitionProperties() {
    when(requirements.validateRequirements(any(), any(), any(), any(), any())).thenReturn(null);
    when(propertyService.hasPropertyUpdates(any())).thenReturn(false);

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
                Map.of("action", "set-default-sort-order", "sort-order-id", 0),
                Map.of(
                    "action",
                    "set-location",
                    "location",
                    "s3://floecat/iceberg/duckdb_mutation_smoke")));

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
    assertEquals("s3://floecat/iceberg/duckdb_mutation_smoke", props.get("location"));
  }

  private Supplier<Table> tableSupplier() {
    Table table = Table.newBuilder().putProperties("existing", "value").build();
    return () -> table;
  }

  private TableCommitService.CommitCommand command(TableRequests.Commit commit) {
    return new TableCommitService.CommitCommand(
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
