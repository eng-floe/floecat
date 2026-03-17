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

package ai.floedb.floecat.gateway.iceberg.minimal.resources.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.minimal.api.dto.CredentialsResponseDto;
import ai.floedb.floecat.gateway.iceberg.minimal.api.dto.TablePlanResponseDto;
import ai.floedb.floecat.gateway.iceberg.minimal.api.request.PlanRequests;
import ai.floedb.floecat.gateway.iceberg.minimal.api.request.TaskRequests;
import ai.floedb.floecat.gateway.iceberg.minimal.config.MinimalGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.minimal.services.planning.TablePlanningService;
import ai.floedb.floecat.gateway.iceberg.minimal.services.table.TableBackend;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TableAccessResourceTest {
  private final TableBackend backend = Mockito.mock(TableBackend.class);
  private final TablePlanningService tablePlanningService =
      Mockito.mock(TablePlanningService.class);
  private final MinimalGatewayConfig config = Mockito.mock(MinimalGatewayConfig.class);
  private final TableAccessResource resource =
      new TableAccessResource(backend, tablePlanningService, config);

  @Test
  void loadsCredentials() {
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
            .setDisplayName("orders")
            .build();
    when(backend.get("foo", List.of("db"), "orders")).thenReturn(table);
    MinimalGatewayConfig.StorageCredentialConfig storageCredential =
        Mockito.mock(MinimalGatewayConfig.StorageCredentialConfig.class);
    when(config.storageCredential()).thenReturn(Optional.of(storageCredential));
    when(storageCredential.scope()).thenReturn(Optional.of("*"));
    when(storageCredential.properties())
        .thenReturn(
            Map.of(
                "type", "s3",
                "s3.access-key-id", "test",
                "s3.secret-access-key", "secret",
                "s3.region", "us-east-1"));

    CredentialsResponseDto dto =
        (CredentialsResponseDto) resource.loadCredentials("foo", "db", "orders", null).getEntity();

    assertEquals(1, dto.storageCredentials().size());
    assertEquals("*", dto.storageCredentials().getFirst().prefix());
    assertEquals("s3", dto.storageCredentials().getFirst().config().get("type"));
  }

  @Test
  void plansTableScan() {
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
            .setCatalogId(ResourceId.newBuilder().setId("catalog-1"))
            .setDisplayName("orders")
            .build();
    when(backend.get("foo", List.of("db"), "orders")).thenReturn(table);
    MinimalGatewayConfig.StorageCredentialConfig storageCredential =
        Mockito.mock(MinimalGatewayConfig.StorageCredentialConfig.class);
    when(config.storageCredential()).thenReturn(Optional.of(storageCredential));
    when(storageCredential.scope()).thenReturn(Optional.of("*"));
    when(storageCredential.properties()).thenReturn(Map.of("type", "s3"));
    when(tablePlanningService.plan(eq(table), eq("db"), eq("orders"), any(), any()))
        .thenReturn(
            Response.ok(
                    new TablePlanResponseDto(
                        "completed", "plan-1", List.of(), List.of(), List.of(), List.of()))
                .build());

    Response response =
        resource.planTable("foo", "db", "orders", "idem-1", PlanRequests.Plan.empty());

    assertEquals(200, response.getStatus());
    verify(tablePlanningService).plan(eq(table), eq("db"), eq("orders"), any(), any());
  }

  @Test
  void fetchesMissingPlanAsNotFound() {
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
            .setCatalogId(ResourceId.newBuilder().setId("catalog-1"))
            .setDisplayName("orders")
            .build();
    when(backend.get("foo", List.of("db"), "orders")).thenReturn(table);
    when(tablePlanningService.fetchPlan("plan-1"))
        .thenReturn(
            Response.status(404)
                .entity(
                    Map.of(
                        "error",
                        Map.of(
                            "message",
                            "plan plan-1 not found",
                            "type",
                            "NoSuchPlanIdException",
                            "code",
                            404)))
                .build());

    assertEquals(404, resource.fetchPlan("foo", "db", "orders", "plan-1").getStatus());
  }

  @Test
  void rejectsMissingPlanTask() {
    assertEquals(
        400,
        resource
            .fetchScanTasks("foo", "db", "orders", null, new TaskRequests.Fetch(" "))
            .getStatus());
  }
}
