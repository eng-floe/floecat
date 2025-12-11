package ai.floedb.floecat.gateway.iceberg.rest.resources.table;

import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TableListResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.MetricsRequests;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.PlanRequests;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TaskRequests;
import ai.floedb.floecat.gateway.iceberg.rest.resources.support.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.resources.support.NamespaceRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.resources.support.RequestContextFactory;
import ai.floedb.floecat.gateway.iceberg.rest.resources.support.TableRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableCommitService;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableCreateService;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableCredentialService;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableDeleteService;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLoadService;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableMetricsService;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableRegisterService;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.ConnectorClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.SnapshotClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.TableClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.planning.TablePlanOrchestrationService;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
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
import org.eclipse.microprofile.config.Config;

@Path("/v1/{prefix}/namespaces/{namespace}")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class TableResource {
  @Inject GrpcWithHeaders grpc;
  @Inject IcebergGatewayConfig config;
  @Inject ObjectMapper mapper;
  @Inject Config mpConfig;
  @Inject TableLifecycleService tableLifecycleService;
  @Inject TableCommitService tableCommitService;
  @Inject TableRegisterService tableRegisterService;
  @Inject TableDeleteService tableDeleteService;
  @Inject TablePlanOrchestrationService tablePlanOrchestrationService;
  @Inject TableLoadService tableLoadService;
  @Inject TableCredentialService tableCredentialService;
  @Inject TableMetricsService tableMetricsService;
  @Inject RequestContextFactory requestContextFactory;
  @Inject TableCreateService tableCreateService;
  @Inject TableClient tableClient;
  @Inject SnapshotClient snapshotClient;
  @Inject ConnectorClient connectorClient;

  private TableGatewaySupport tableSupport;

  @PostConstruct
  void initSupport() {
    this.tableSupport =
        new TableGatewaySupport(
            grpc, config, mapper, mpConfig, tableClient, snapshotClient, connectorClient);
  }

  @GET
  @Path("/tables")
  public Response list(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @QueryParam("pageToken") String pageToken,
      @QueryParam("pageSize") Integer pageSize) {
    NamespaceRequestContext namespaceContext = requestContextFactory.namespace(prefix, namespace);
    TableLifecycleService.ListTablesResult result =
        tableLifecycleService.listTables(
            namespaceContext.catalogName(), namespaceContext.namespace(), pageSize, pageToken);
    return Response.ok(new TableListResponseDto(result.identifiers(), result.nextPageToken()))
        .build();
  }

  @POST
  @Path("/tables")
  public Response create(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      @HeaderParam("Iceberg-Transaction-Id") String transactionId,
      TableRequests.Create req) {
    NamespaceRequestContext namespaceContext = requestContextFactory.namespace(prefix, namespace);
    return tableCreateService.create(
        namespaceContext, idempotencyKey, transactionId, req, tableSupport);
  }

  @Path("/tables/{table}")
  @GET
  public Response get(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @QueryParam("snapshots") String snapshots,
      @HeaderParam("If-None-Match") String ifNoneMatch) {
    TableRequestContext tableContext = requestContextFactory.table(prefix, namespace, table);
    return tableLoadService.load(tableContext, table, snapshots, ifNoneMatch, tableSupport);
  }

  @Path("/tables/{table}")
  @HEAD
  public Response exists(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table) {
    requestContextFactory.table(prefix, namespace, table);
    return Response.noContent().build();
  }

  @Path("/tables/{table}")
  @DELETE
  public Response delete(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @QueryParam("purgeRequested") Boolean purgeRequested) {
    TableRequestContext tableContext = requestContextFactory.table(prefix, namespace, table);
    return tableDeleteService.delete(tableContext, table, purgeRequested, tableSupport);
  }

  @Path("/tables/{table}")
  @POST
  public Response commit(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      @HeaderParam("Iceberg-Transaction-Id") String transactionId,
      TableRequests.Commit req) {
    NamespaceRequestContext namespaceContext = requestContextFactory.namespace(prefix, namespace);
    return tableCommitService.commit(
        new TableCommitService.CommitCommand(
            namespaceContext.prefix(),
            namespaceContext.namespace(),
            namespaceContext.namespacePath(),
            table,
            namespaceContext.catalogName(),
            namespaceContext.catalogId(),
            namespaceContext.namespaceId(),
            idempotencyKey,
            transactionId,
            req,
            tableSupport));
  }

  @Path("/tables/{table}/plan")
  @POST
  public Response planTable(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      PlanRequests.Plan rawRequest) {
    TableRequestContext tableContext = requestContextFactory.table(prefix, namespace, table);
    return tablePlanOrchestrationService.plan(tableContext, rawRequest, tableSupport);
  }

  @Path("/tables/{table}/plan/{planId}")
  @GET
  public Response fetchPlan(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @PathParam("planId") String planId) {
    requestContextFactory.table(prefix, namespace, table);
    return tablePlanOrchestrationService.fetchPlan(planId);
  }

  @Path("/tables/{table}/plan/{planId}")
  @DELETE
  public Response cancelPlan(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @PathParam("planId") String planId) {
    requestContextFactory.table(prefix, namespace, table);
    return tablePlanOrchestrationService.cancelPlan(planId);
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
    TableRequestContext tableContext = requestContextFactory.table(prefix, namespace, table);
    return tablePlanOrchestrationService.consumeTask(tableContext, request.planTask().trim());
  }

  @Path("/tables/{table}/credentials")
  @GET
  public Response loadCredentials(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @QueryParam("planId") String planId) {
    TableRequestContext tableContext = requestContextFactory.table(prefix, namespace, table);
    return tableCredentialService.load(tableContext, planId, tableSupport);
  }

  @Path("/tables/{table}/metrics")
  @POST
  public Response publishMetrics(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      MetricsRequests.Report request) {
    TableRequestContext tableContext = requestContextFactory.table(prefix, namespace, table);
    return tableMetricsService.publish(tableContext, request);
  }

  @Path("/register")
  @POST
  public Response registerTable(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      TableRequests.Register req) {
    NamespaceRequestContext namespaceContext = requestContextFactory.namespace(prefix, namespace);
    return tableRegisterService.register(namespaceContext, idempotencyKey, req, tableSupport);
  }
}
