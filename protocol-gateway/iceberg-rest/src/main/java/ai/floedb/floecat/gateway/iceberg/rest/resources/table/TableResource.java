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

package ai.floedb.floecat.gateway.iceberg.rest.resources.table;

import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TableListResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.MetricsRequests;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.PlanRequests;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TaskRequests;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.NamespaceRef;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.ResourceResolver;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.TableRef;
import ai.floedb.floecat.gateway.iceberg.rest.common.CommitTrafficLogger;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.floecat.gateway.iceberg.rest.services.planning.TablePlanOrchestrationService;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.TableCommitService;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.TableCreateService;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.TableCredentialService;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.TableDeleteService;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.TableMetricsService;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.TableRegisterService;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.load.TableLoadService;
import io.smallrye.common.annotation.Blocking;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

@Path("/v1/{prefix}/namespaces/{namespace}")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class TableResource {
  @Inject TableLifecycleService tableLifecycleService;
  @Inject TableCommitService tableCommitService;
  @Inject TableRegisterService tableRegisterService;
  @Inject TableDeleteService tableDeleteService;
  @Inject TablePlanOrchestrationService tablePlanOrchestrationService;
  @Inject TableLoadService tableLoadService;
  @Inject TableCredentialService tableCredentialService;
  @Inject TableMetricsService tableMetricsService;
  @Inject ResourceResolver resourceResolver;
  @Inject TableCreateService tableCreateService;
  @Inject CommitTrafficLogger commitTrafficLogger;
  @Inject TableGatewaySupport tableSupport;

  @GET
  @Path("/tables")
  public Response list(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @QueryParam("pageToken") String pageToken,
      @QueryParam("pageSize") Integer pageSize) {
    NamespaceRef namespaceContext = resourceResolver.namespace(prefix, namespace);
    TableLifecycleService.ListTablesResult result =
        tableLifecycleService.listTables(
            namespaceContext.catalogName(), namespaceContext.namespace(), pageSize, pageToken);
    return Response.ok(new TableListResponseDto(result.identifiers(), result.nextPageToken()))
        .build();
  }

  @POST
  @Path("/tables")
  @Blocking
  public Response create(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @HeaderParam("X-Iceberg-Access-Delegation") String accessDelegationMode,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      @HeaderParam("Iceberg-Transaction-Id") String transactionId,
      TableRequests.Create req) {
    NamespaceRef namespaceContext = resourceResolver.namespace(prefix, namespace);
    return tableCreateService.create(
        namespaceContext, accessDelegationMode, idempotencyKey, transactionId, req, tableSupport);
  }

  @Path("/tables/{table}")
  @GET
  public Response get(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @QueryParam("snapshots") String snapshots,
      @HeaderParam("X-Iceberg-Access-Delegation") String accessDelegationMode,
      @HeaderParam("If-None-Match") String ifNoneMatch) {
    String path = String.format("/v1/%s/namespaces/%s/tables/%s", prefix, namespace, table);
    if (snapshots != null && !snapshots.isBlank()) {
      path = path + "?snapshots=" + snapshots;
    }
    TableRef tableContext = resourceResolver.table(prefix, namespace, table);
    Response response =
        tableLoadService.load(
            tableContext, table, snapshots, accessDelegationMode, ifNoneMatch, tableSupport);
    commitTrafficLogger.logResponse("GET", path, response.getStatus(), response.getEntity());
    return response;
  }

  @Path("/tables/{table}")
  @HEAD
  public Response exists(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table) {
    resourceResolver.table(prefix, namespace, table);
    return Response.noContent().build();
  }

  @Path("/tables/{table}")
  @DELETE
  public Response delete(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @QueryParam("purgeRequested") Boolean purgeRequested) {
    TableRef tableContext = resourceResolver.table(prefix, namespace, table);
    return tableDeleteService.delete(tableContext, table, purgeRequested, tableSupport);
  }

  @Path("/tables/{table}")
  @POST
  @Blocking
  public Response commit(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @HeaderParam("X-Iceberg-Access-Delegation") String accessDelegationMode,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      @HeaderParam("Iceberg-Transaction-Id") String transactionId,
      TableRequests.Commit req) {
    String path = String.format("/v1/%s/namespaces/%s/tables/%s", prefix, namespace, table);
    NamespaceRef namespaceContext = resourceResolver.namespace(prefix, namespace);
    Response response =
        tableCommitService.commit(
            new TableCommitService.CommitCommand(
                namespaceContext.prefix(),
                namespaceContext.namespace(),
                namespaceContext.namespacePath(),
                table,
                namespaceContext.catalogName(),
                namespaceContext.catalogId(),
                namespaceContext.namespaceId(),
                idempotencyKey,
                null,
                transactionId,
                accessDelegationMode,
                req,
                tableSupport));
    commitTrafficLogger.logResponse("POST", path, response.getStatus(), response.getEntity());
    return response;
  }

  @Path("/tables/{table}/plan")
  @POST
  public Response planTable(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @HeaderParam("X-Iceberg-Access-Delegation") String accessDelegationMode,
      PlanRequests.Plan rawRequest) {
    TableRef tableContext = resourceResolver.table(prefix, namespace, table);
    return tablePlanOrchestrationService.plan(
        tableContext, rawRequest, accessDelegationMode, tableSupport);
  }

  @Path("/tables/{table}/plan/{plan-id}")
  @GET
  public Response fetchPlan(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @PathParam("plan-id") String planId) {
    TableRef tableContext = resourceResolver.table(prefix, namespace, table);
    return tablePlanOrchestrationService.fetchPlan(tableContext, planId);
  }

  @Path("/tables/{table}/plan/{plan-id}")
  @DELETE
  public Response cancelPlan(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @PathParam("plan-id") String planId) {
    TableRef tableContext = resourceResolver.table(prefix, namespace, table);
    return tablePlanOrchestrationService.cancelPlan(tableContext, planId);
  }

  @Path("/tables/{table}/tasks")
  @POST
  public Response scanTasks(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      TaskRequests.Fetch request) {
    if (request == null || request.planTask() == null || request.planTask().isBlank()) {
      return IcebergErrorResponses.validation("plan-task is required");
    }
    TableRef tableContext = resourceResolver.table(prefix, namespace, table);
    return tablePlanOrchestrationService.consumeTask(tableContext, request.planTask().trim());
  }

  @Path("/tables/{table}/credentials")
  @GET
  public Response loadCredentials(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @QueryParam("planId") String planId) {
    TableRef tableContext = resourceResolver.table(prefix, namespace, table);
    return tableCredentialService.load(tableContext, planId);
  }

  @Path("/tables/{table}/metrics")
  @POST
  public Response publishMetrics(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      MetricsRequests.Report request) {
    TableRef tableContext = resourceResolver.table(prefix, namespace, table);
    return tableMetricsService.publish(tableContext, request);
  }

  @Path("/register")
  @POST
  @Blocking
  public Response registerTable(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      TableRequests.Register req) {
    NamespaceRef namespaceContext = resourceResolver.namespace(prefix, namespace);
    return tableRegisterService.register(namespaceContext, idempotencyKey, req, tableSupport);
  }
}
