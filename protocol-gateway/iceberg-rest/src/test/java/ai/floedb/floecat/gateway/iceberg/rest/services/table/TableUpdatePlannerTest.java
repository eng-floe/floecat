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
import static org.mockito.ArgumentMatchers.eq;
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
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.SnapshotMetadataService;
import com.google.protobuf.FieldMask;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TableUpdatePlannerTest {

  private final TableUpdatePlanner planner = new TableUpdatePlanner();
  private final CommitRequirementService requirements = mock(CommitRequirementService.class);
  private final TableLifecycleService lifecycle = mock(TableLifecycleService.class);
  private final TablePropertyService propertyService = mock(TablePropertyService.class);
  private final SnapshotMetadataService snapshotService = mock(SnapshotMetadataService.class);
  private final TableGatewaySupport tableSupport = mock(TableGatewaySupport.class);

  @BeforeEach
  void setUp() {
    planner.commitRequirementService = requirements;
    planner.tableLifecycleService = lifecycle;
    planner.tablePropertyService = propertyService;
    planner.snapshotMetadataService = snapshotService;
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
              props.put("owner", "alice");
              return null;
            })
        .when(propertyService)
        .applyPropertyUpdates(any(), any());
    when(propertyService.applyLocationUpdate(any(), any(), any(), any())).thenReturn(null);
    when(snapshotService.applySnapshotUpdates(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(null);
    ResourceId targetNamespace = ResourceId.newBuilder().setId("cat:analytics.canary").build();
    when(lifecycle.resolveNamespaceId(eq("catalog"), any(ArrayList.class)))
        .thenReturn(targetNamespace);

    TableRequests.Commit request =
        new TableRequests.Commit(
            "orders",
            List.of("analytics", "canary"),
            null,
            Map.of("metadata-location", "s3://current", "retention", "7d"),
            null,
            List.of(),
            List.of(Map.of("action", "set-properties", "updates", Map.of("ignored", "value"))));

    TableUpdatePlanner.UpdatePlan plan =
        planner.planUpdates(
            command(request),
            tableSupplier(),
            ResourceId.newBuilder().setId("cat:db:orders").build());

    assertFalse(plan.hasError());
    TableSpec spec = plan.spec().build();
    FieldMask mask = plan.mask().build();
    assertEquals(targetNamespace, spec.getNamespaceId());
    assertEquals("7d", spec.getPropertiesOrThrow("retention"));
    assertEquals("alice", spec.getPropertiesOrThrow("owner"));
    assertTrue(mask.getPathsList().contains("namespace_id"));
    assertTrue(mask.getPathsList().contains("properties"));
    verify(propertyService).applyLocationUpdate(any(), any(), any(), any());
  }

  @Test
  void planUpdatesReturnsValidationErrorForUnsupportedAction() {
    when(requirements.validateRequirements(any(), any(), any(), any(), any())).thenReturn(null);
    when(propertyService.hasPropertyUpdates(any())).thenReturn(false);
    TableRequests.Commit request =
        new TableRequests.Commit(
            "orders", null, null, null, null, List.of(), List.of(Map.of("action", "drop")));

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
    verify(snapshotService, never())
        .applySnapshotUpdates(any(), any(), any(), any(), any(), any(), any());
  }

  @Test
  void planUpdatesPropagatesRequirementErrors() {
    Response failure = Response.status(Response.Status.CONFLICT).entity("conflict").build();
    when(requirements.validateRequirements(any(), any(), any(), any(), any())).thenReturn(failure);

    TableUpdatePlanner.UpdatePlan plan =
        planner.planUpdates(
            command(
                new TableRequests.Commit("orders", null, null, null, null, List.of(), List.of())),
            tableSupplier(),
            ResourceId.newBuilder().setId("cat:db:orders").build());

    assertTrue(plan.hasError());
    assertSame(failure, plan.error());
    verify(propertyService, never()).applyPropertyUpdates(any(), any());
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
        "txn",
        commit,
        tableSupport);
  }
}
