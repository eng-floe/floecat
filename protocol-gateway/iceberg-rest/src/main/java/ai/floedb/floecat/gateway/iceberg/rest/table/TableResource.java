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

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.ContentFileDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.CredentialsResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.FailedPlanningResultDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.FileScanTaskDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.StorageCredentialDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TableListResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TablePlanResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergError;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.MetricsRequests;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.PlanRequests;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.CatalogResolver;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.NamespaceRef;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.ResourceResolver;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.SnapshotLister;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.TableRef;
import ai.floedb.floecat.gateway.iceberg.rest.compat.DeltaIcebergMetadataService;
import ai.floedb.floecat.gateway.iceberg.rest.compat.TableFormatSupport;
import ai.floedb.floecat.gateway.iceberg.rest.support.CommitTrafficLogger;
import ai.floedb.floecat.gateway.iceberg.rest.support.GrpcServiceFacade;
import ai.floedb.floecat.gateway.iceberg.rest.support.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.support.MetadataLocationUtil;
import ai.floedb.floecat.gateway.iceberg.rest.table.IcebergMetadataService.ResolvedMetadata;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
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
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedHashMap;
import java.util.List;
import org.jboss.logging.Logger;

@Path("/v1/{prefix}/namespaces/{namespace}")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class TableResource {
  private static final Logger LOG = Logger.getLogger(TableResource.class);

  @Inject ObjectMapper mapper;
  @Inject IcebergGatewayConfig config;
  @Inject GrpcWithHeaders grpc;
  @Inject TableDropCleanupService tableDropCleanupService;
  @Inject ConnectorProvisioningService connectorProvisioningService;
  @Inject TransactionCommitService transactionCommitService;
  @Inject TablePlanService tablePlanService;
  @Inject ResourceResolver resourceResolver;
  @Inject CommitTrafficLogger commitTrafficLogger;
  @Inject TableGatewaySupport tableSupport;
  @Inject GrpcServiceFacade snapshotClient;
  @Inject TableFormatSupport tableFormatSupport;
  @Inject DeltaIcebergMetadataService deltaMetadataService;
  @Inject IcebergMetadataService icebergMetadataService;

  @GET
  @Path("/tables")
  public Response list(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @QueryParam("pageToken") String pageToken,
      @QueryParam("pageSize") Integer pageSize) {
    NamespaceRef namespaceContext = resourceResolver.namespace(prefix, namespace);
    TableGatewaySupport.ListTablesResult result =
        tableSupport.listTables(
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
    return transactionCommitService.createTable(
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
    TableRef tableContext = resourceResolver.table(prefix, namespace, table);
    return loadTable(tableContext, table, snapshots, accessDelegationMode, ifNoneMatch);
  }

  @Path("/tables/{table}")
  @HEAD
  public Response exists(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table) {
    try {
      resourceResolver.table(prefix, namespace, table);
      return Response.noContent().build();
    } catch (WebApplicationException e) {
      return IcebergErrorResponses.headStatusOnlyOnNotFound(e);
    }
  }

  @Path("/tables/{table}")
  @DELETE
  public Response delete(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      @QueryParam("purgeRequested") Boolean purgeRequested) {
    TableRef tableContext = resourceResolver.table(prefix, namespace, table);
    ResourceId tableId = tableContext.tableId();
    ResourceId connectorId = null;
    Table existing = null;
    try {
      existing = tableSupport.getTable(tableId);
      connectorId = connectorProvisioningService.resolveConnectorId(existing);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
        throw e;
      }
    }
    boolean purge = Boolean.TRUE.equals(purgeRequested);
    if (purge) {
      tableDropCleanupService.purgeTableData(
          tableContext.catalog().catalogName(), tableContext.namespaceName(), table, existing);
    }
    if (connectorId != null) {
      tableSupport.deleteConnector(connectorId);
    }
    tableSupport.deleteTable(tableId, purge);
    return Response.noContent().build();
  }

  @Path("/tables/{table}")
  @POST
  @Blocking
  public Response commit(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      @HeaderParam("Iceberg-Transaction-Id") String transactionId,
      TableRequests.Commit req) {
    String path = String.format("/v1/%s/namespaces/%s/tables/%s", prefix, namespace, table);
    commitTrafficLogger.logRequest("POST", path, req);
    NamespaceRef namespaceContext = resourceResolver.namespace(prefix, namespace);
    Response response =
        transactionCommitService.commitTable(
            new TransactionCommitService.CommitCommand(
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
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      PlanRequests.Plan rawRequest) {
    TableRef tableContext = resourceResolver.table(prefix, namespace, table);
    PlanRequests.Plan request = rawRequest == null ? PlanRequests.Plan.empty() : rawRequest;
    Long startSnapshotId = request.startSnapshotId();
    Long endSnapshotId = request.endSnapshotId();
    Long snapshotId = request.snapshotId();
    if (snapshotId != null && (startSnapshotId != null || endSnapshotId != null)) {
      return IcebergErrorResponses.validation(
          "snapshot-id cannot be combined with start-snapshot-id or end-snapshot-id");
    }
    if (endSnapshotId != null && startSnapshotId == null) {
      return IcebergErrorResponses.validation("end-snapshot-id requires start-snapshot-id");
    }
    if (startSnapshotId != null && endSnapshotId == null) {
      return IcebergErrorResponses.validation("start-snapshot-id requires end-snapshot-id");
    }
    if (startSnapshotId != null && endSnapshotId != null && startSnapshotId >= endSnapshotId) {
      return IcebergErrorResponses.validation(
          "start-snapshot-id must be less than end-snapshot-id");
    }
    Long resolvedSnapshotId = endSnapshotId != null ? endSnapshotId : snapshotId;
    if (startSnapshotId != null && resolvedSnapshotId == null) {
      return IcebergErrorResponses.validation(
          "start-snapshot-id requires snapshot-id or end-snapshot-id");
    }
    boolean caseSensitive =
        request.caseSensitive() == null || Boolean.TRUE.equals(request.caseSensitive());
    boolean useSnapshotSchema = Boolean.TRUE.equals(request.useSnapshotSchema());
    String planId = null;
    try {
      ResourceId catalogId =
          CatalogResolver.resolveCatalogId(grpc, config, tableContext.catalog().catalogName());
      var handle =
          tablePlanService.startPlan(
              catalogId,
              tableContext.tableId(),
              copyOfOrNull(request.select()),
              startSnapshotId,
              endSnapshotId,
              snapshotId,
              copyOfOrNull(request.statsFields()),
              request.filter(),
              caseSensitive,
              useSnapshotSchema,
              request.minRowsRequested());
      planId = handle.queryId();
      tablePlanService.registerSubmittedPlan(
          planId, tableContext.namespaceName(), tableContext.table());
      TablePlanResponseDto planned =
          tablePlanService.fetchPlan(planId, tableSupport.defaultCredentials());
      TablePlanService.PlanDescriptor descriptor =
          tablePlanService.registerCompletedPlan(
              planId,
              tableContext.namespaceName(),
              tableContext.table(),
              copyOfOrEmpty(planned.fileScanTasks()),
              copyOfOrEmpty(planned.deleteFiles()),
              planned.storageCredentials());
      return Response.ok(toPlanResponse(descriptor, true)).build();
    } catch (IllegalArgumentException ex) {
      return IcebergErrorResponses.validation(ex.getMessage());
    } catch (RuntimeException ex) {
      if (planId != null) {
        try {
          tablePlanService.cancelPlan(planId);
        } catch (RuntimeException ignored) {
          // Cancellation is best-effort; errors are surfaced via the original failure.
        }
      }
      String message = ex.getMessage();
      if (message == null || message.isBlank()) {
        message = "Scan planning failed";
      }
      return failedPlanning(message, Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  @Path("/tables/{table}/plan/{plan-id}")
  @GET
  public Response fetchPlan(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @PathParam("plan-id") String planId) {
    resourceResolver.table(prefix, namespace, table);
    return tablePlanService
        .findPlan(planId)
        .map(
            descriptor -> {
              if ("failed".equals(descriptor.status().value())) {
                return failedPlanning(
                    "Scan planning failed", Response.Status.INTERNAL_SERVER_ERROR);
              }
              return Response.ok(toPlanResponse(descriptor, false)).build();
            })
        .orElseGet(() -> IcebergErrorResponses.noSuchPlanId("plan " + planId + " not found"));
  }

  @Path("/tables/{table}/plan/{plan-id}")
  @DELETE
  public Response cancelPlan(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      @PathParam("plan-id") String planId) {
    resourceResolver.table(prefix, namespace, table);
    var plan = tablePlanService.findPlan(planId);
    if (plan.isEmpty()) {
      return IcebergErrorResponses.noSuchPlanId("plan " + planId + " not found");
    }
    tablePlanService.cancelPlan(planId);
    return Response.noContent().build();
  }

  @Path("/tables/{table}/tasks")
  @POST
  public Response scanTasks(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      PlanRequests.FetchTask request) {
    if (request == null || request.planTask() == null || request.planTask().isBlank()) {
      return IcebergErrorResponses.validation("plan-task is required");
    }
    TableRef tableContext = resourceResolver.table(prefix, namespace, table);
    return tablePlanService
        .consumeTask(tableContext.namespaceName(), tableContext.table(), request.planTask().trim())
        .map(response -> Response.ok(response).build())
        .orElseGet(() -> IcebergErrorResponses.noSuchPlanTask("plan-task not found"));
  }

  @Path("/tables/{table}/credentials")
  @GET
  public Response loadCredentials(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @QueryParam("planId") String planId) {
    resourceResolver.table(prefix, namespace, table);
    List<StorageCredentialDto> credentials;
    try {
      credentials = tableSupport.credentialsForAccessDelegation("vended-credentials");
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }
    return Response.ok(new CredentialsResponseDto(credentials)).build();
  }

  @Path("/tables/{table}/metrics")
  @POST
  public Response publishMetrics(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      MetricsRequests.Report request) {
    TableRef tableContext = resourceResolver.table(prefix, namespace, table);
    if (request == null) {
      return IcebergErrorResponses.validation("Request body is required");
    }
    if (isBlank(request.reportType())) {
      return IcebergErrorResponses.validation("report-type is required");
    }
    if (isBlank(request.tableName())) {
      return IcebergErrorResponses.validation("table-name is required");
    }
    if (request.snapshotId() == null) {
      return IcebergErrorResponses.validation("snapshot-id is required");
    }
    if (request.metrics() == null) {
      return IcebergErrorResponses.validation("metrics is required");
    }
    boolean scanReport =
        request.filter() != null
            && request.schemaId() != null
            && !isEmpty(request.projectedFieldIds())
            && !isEmpty(request.projectedFieldNames());
    boolean commitReport = request.sequenceNumber() != null && !isBlank(request.operation());
    if (!scanReport && !commitReport) {
      return IcebergErrorResponses.validation("metrics report missing required fields");
    }
    try {
      LOG.infof(
          "Received metrics report namespace=%s table=%s payload=%s",
          tableContext.namespaceName(), tableContext.table(), mapper.writeValueAsString(request));
    } catch (JsonProcessingException e) {
      LOG.infof(
          "Received metrics report namespace=%s table=%s payload=%s",
          tableContext.namespaceName(), tableContext.table(), String.valueOf(request));
    }
    return Response.noContent().build();
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
    return transactionCommitService.registerTable(
        namespaceContext, idempotencyKey, req, tableSupport);
  }

  private boolean isBlank(String value) {
    return value == null || value.isBlank();
  }

  private boolean isEmpty(List<?> values) {
    return values == null || values.isEmpty();
  }

  private TablePlanResponseDto toPlanResponse(
      TablePlanService.PlanDescriptor descriptor, boolean includePlanId) {
    String status = descriptor.status().value();
    if ("cancelled".equals(status)) {
      return new TablePlanResponseDto(status, null, null, null, null, null);
    }
    if (!"completed".equals(status)) {
      return new TablePlanResponseDto(
          status, includePlanId ? descriptor.planId() : null, null, null, null, null);
    }
    List<FileScanTaskDto> fileScanTasks = copyOfOrEmpty(descriptor.fileScanTasks());
    List<ContentFileDto> deleteFiles = copyOfOrEmpty(descriptor.deleteFiles());
    return new TablePlanResponseDto(
        descriptor.status().value(),
        includePlanId ? descriptor.planId() : null,
        descriptor.planTasks(),
        fileScanTasks,
        deleteFiles,
        descriptor.credentials());
  }

  private static List<String> copyOfOrNull(List<String> values) {
    if (values == null || values.isEmpty()) {
      return null;
    }
    return List.copyOf(values);
  }

  private static <T> List<T> copyOfOrEmpty(List<T> values) {
    if (values == null || values.isEmpty()) {
      return List.of();
    }
    return List.copyOf(values);
  }

  private static Response failedPlanning(String message, Response.Status status) {
    IcebergError error = new IcebergError(message, "InternalServerError", status.getStatusCode());
    return Response.status(status).entity(new FailedPlanningResultDto("failed", error)).build();
  }

  Response loadTable(
      TableRef tableContext,
      String tableName,
      String snapshots,
      String accessDelegationMode,
      String ifNoneMatch) {
    Table tableRecord = tableSupport.getTable(tableContext.tableId());
    SnapshotLister.Mode snapshotMode;
    try {
      snapshotMode = parseSnapshotMode(snapshots);
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }
    IcebergMetadata metadata;
    TableMetadataView metadataView;
    List<Snapshot> snapshotList;
    if (deltaCompatEnabled(tableRecord)) {
      DeltaIcebergMetadataService.DeltaLoadResult delta =
          deltaMetadataService.load(tableContext.tableId(), tableRecord, snapshotMode);
      metadata = delta.metadata();
      metadataView =
          TableMetadataBuilder.fromCatalog(
              tableName,
              tableRecord,
              new LinkedHashMap<>(tableRecord.getPropertiesMap()),
              metadata,
              delta.snapshots());
      snapshotList = delta.snapshots();
    } else {
      try {
        IcebergMetadata currentMetadata =
            icebergMetadataService.resolveCurrentIcebergMetadata(tableRecord, tableSupport);
        ResolvedMetadata resolved =
            icebergMetadataService.resolveMetadata(
                tableName,
                tableRecord,
                currentMetadata,
                tableSupport.defaultFileIoProperties(),
                () ->
                    SnapshotLister.fetchSnapshots(
                        snapshotClient, tableContext.tableId(), snapshotMode, currentMetadata));
        metadata = resolved.icebergMetadata();
        metadataView = resolved.metadataView();
        if (metadataView == null) {
          snapshotList =
              SnapshotLister.fetchSnapshots(
                  snapshotClient, tableContext.tableId(), snapshotMode, metadata);
          metadataView =
              TableMetadataBuilder.fromCatalog(
                  tableName,
                  tableRecord,
                  new LinkedHashMap<>(tableRecord.getPropertiesMap()),
                  metadata,
                  snapshotList);
        }
      } catch (IllegalArgumentException e) {
        LOG.debugf(
            e,
            "Falling back to table record metadata for table=%s location=%s",
            tableName,
            MetadataLocationUtil.metadataLocation(tableRecord.getPropertiesMap()));
        metadata = null;
        snapshotList =
            SnapshotLister.fetchSnapshots(
                snapshotClient, tableContext.tableId(), snapshotMode, null);
        metadataView =
            TableMetadataBuilder.fromCatalog(
                tableName,
                tableRecord,
                new LinkedHashMap<>(tableRecord.getPropertiesMap()),
                null,
                snapshotList);
      }
    }
    String etagValue = etagSource(metadata, snapshotMode, tableRecord);
    if (etagValue != null) {
      etagValue = etagForMetadataLocation(etagValue);
    }
    if (ifNoneMatch != null && ifNoneMatch.trim().equals("*")) {
      return IcebergErrorResponses.validation("If-None-Match may not take the value of '*'");
    }
    if (etagMatches(etagValue, ifNoneMatch)) {
      return Response.notModified().build();
    }
    List<StorageCredentialDto> credentials;
    try {
      credentials = tableSupport.credentialsForAccessDelegation(accessDelegationMode);
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }
    Response.ResponseBuilder builder =
        Response.ok(
            TableResponseMapper.toLoadResult(
                metadataView, tableSupport.defaultTableConfig(), credentials));
    if (etagValue != null) {
      builder.header(HttpHeaders.ETAG, etagValue);
    }
    return builder.build();
  }

  private SnapshotLister.Mode parseSnapshotMode(String raw) {
    if (raw == null || raw.isBlank() || raw.equalsIgnoreCase("all")) {
      return SnapshotLister.Mode.ALL;
    }
    if ("refs".equalsIgnoreCase(raw)) {
      return SnapshotLister.Mode.REFS;
    }
    throw new IllegalArgumentException("snapshots must be one of [all, refs]");
  }

  private boolean etagMatches(String etagValue, String ifNoneMatch) {
    if (etagValue == null || ifNoneMatch == null) {
      return false;
    }
    String expected = normalizeEtag(etagValue);
    for (String raw : ifNoneMatch.split(",")) {
      String token = normalizeEtag(raw);
      if (!token.isEmpty() && token.equals(expected)) {
        return true;
      }
    }
    return false;
  }

  private String normalizeEtag(String token) {
    if (token == null) {
      return "";
    }
    String value = token.trim();
    if (value.startsWith("W/")) {
      value = value.substring(2).trim();
    }
    if (value.startsWith("\"") && value.endsWith("\"") && value.length() >= 2) {
      value = value.substring(1, value.length() - 1);
    }
    return value;
  }

  static String etagForMetadataLocation(String metadataLocation) {
    if (metadataLocation == null) {
      throw new IllegalArgumentException("Unable to generate etag for null metadataLocation");
    }
    return "W/\"" + sha256Hex(metadataLocation) + "\"";
  }

  private static String sha256Hex(String value) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hashed = digest.digest(value.getBytes(StandardCharsets.UTF_8));
      StringBuilder out = new StringBuilder(hashed.length * 2);
      for (byte b : hashed) {
        out.append(Character.forDigit((b >> 4) & 0xF, 16));
        out.append(Character.forDigit(b & 0xF, 16));
      }
      return out.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 unavailable", e);
    }
  }

  private String metadataLocation(IcebergMetadata metadata, Table tableRecord) {
    return MetadataLocationUtil.resolveCurrentMetadataLocation(tableRecord, metadata);
  }

  private String etagSource(
      IcebergMetadata metadata, SnapshotLister.Mode snapshotMode, Table tableRecord) {
    String metadataLocation = metadataLocation(metadata, tableRecord);
    if (metadataLocation == null) {
      return null;
    }
    String mode =
        snapshotMode == null
            ? SnapshotLister.Mode.ALL.name().toLowerCase()
            : snapshotMode.name().toLowerCase();
    return metadataLocation + "|snapshots=" + mode;
  }

  private boolean deltaCompatEnabled(Table table) {
    if (config == null || tableFormatSupport == null || table == null) {
      return false;
    }
    var deltaCompat = config.deltaCompat();
    return deltaCompat.isPresent()
        && deltaCompat.get().enabled()
        && tableFormatSupport.isDelta(table);
  }
}
