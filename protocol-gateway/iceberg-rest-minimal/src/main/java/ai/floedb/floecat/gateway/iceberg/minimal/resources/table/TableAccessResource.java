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

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.gateway.iceberg.minimal.api.dto.CredentialsResponseDto;
import ai.floedb.floecat.gateway.iceberg.minimal.api.dto.StorageCredentialDto;
import ai.floedb.floecat.gateway.iceberg.minimal.api.request.PlanRequests;
import ai.floedb.floecat.gateway.iceberg.minimal.api.request.TaskRequests;
import ai.floedb.floecat.gateway.iceberg.minimal.config.MinimalGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.minimal.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.minimal.resources.common.NamespacePaths;
import ai.floedb.floecat.gateway.iceberg.minimal.services.planning.TablePlanningService;
import ai.floedb.floecat.gateway.iceberg.minimal.services.table.TableBackend;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Path("/v1/{prefix}/namespaces/{namespace}/tables/{table}")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class TableAccessResource {
  private final TableBackend backend;
  private final TablePlanningService tablePlanningService;
  private final MinimalGatewayConfig config;

  public TableAccessResource(
      TableBackend backend,
      TablePlanningService tablePlanningService,
      MinimalGatewayConfig config) {
    this.backend = backend;
    this.tablePlanningService = tablePlanningService;
    this.config = config;
  }

  @GET
  @Path("/credentials")
  public Response loadCredentials(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @QueryParam("planId") String planId) {
    try {
      backend.get(prefix, NamespacePaths.split(namespace), table);
      return Response.ok(new CredentialsResponseDto(defaultCredentials())).build();
    } catch (StatusRuntimeException exception) {
      return tableError(exception);
    } catch (IllegalArgumentException exception) {
      return IcebergErrorResponses.validation(exception.getMessage());
    }
  }

  @POST
  @Path("/plan")
  public Response planTable(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      PlanRequests.Plan request) {
    try {
      Table tableRecord = backend.get(prefix, NamespacePaths.split(namespace), table);
      return tablePlanningService.plan(
          tableRecord, namespace, table, request, defaultCredentials());
    } catch (StatusRuntimeException exception) {
      return tableError(exception);
    } catch (IllegalArgumentException exception) {
      return IcebergErrorResponses.validation(exception.getMessage());
    }
  }

  @GET
  @Path("/plan/{planId}")
  public Response fetchPlan(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @PathParam("planId") String planId) {
    try {
      backend.get(prefix, NamespacePaths.split(namespace), table);
      return tablePlanningService.fetchPlan(planId);
    } catch (StatusRuntimeException exception) {
      return tableError(exception);
    }
  }

  @DELETE
  @Path("/plan/{planId}")
  public Response cancelPlan(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      @PathParam("planId") String planId) {
    try {
      backend.get(prefix, NamespacePaths.split(namespace), table);
      return tablePlanningService.cancelPlan(planId);
    } catch (StatusRuntimeException exception) {
      return tableError(exception);
    }
  }

  @POST
  @Path("/tasks")
  public Response fetchScanTasks(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      TaskRequests.Fetch request) {
    if (request == null || request.planTask() == null || request.planTask().isBlank()) {
      return IcebergErrorResponses.validation("plan-task is required");
    }
    try {
      backend.get(prefix, NamespacePaths.split(namespace), table);
      return tablePlanningService.consumeTask(namespace, table, request.planTask().trim());
    } catch (StatusRuntimeException exception) {
      return tableError(exception);
    }
  }

  private Response tableError(StatusRuntimeException exception) {
    if (exception.getStatus().getCode() == Status.Code.NOT_FOUND) {
      String message = exception.getStatus().getDescription();
      return IcebergErrorResponses.noSuchTable(
          message == null || message.isBlank() ? "Table not found" : message);
    }
    return IcebergErrorResponses.grpc(exception);
  }

  private List<StorageCredentialDto> defaultCredentials() {
    Map<String, String> properties = new LinkedHashMap<>();
    config.storageCredential().ifPresent(cfg -> properties.putAll(cfg.properties()));
    properties.values().removeIf(value -> value == null || value.isBlank());
    if (properties.isEmpty()) {
      throw new IllegalArgumentException(
          "Credential vending was requested but no credentials are available");
    }
    String scope =
        config
            .storageCredential()
            .flatMap(MinimalGatewayConfig.StorageCredentialConfig::scope)
            .filter(value -> !value.isBlank())
            .orElse("*");
    return List.of(new StorageCredentialDto(scope, Map.copyOf(properties)));
  }
}
