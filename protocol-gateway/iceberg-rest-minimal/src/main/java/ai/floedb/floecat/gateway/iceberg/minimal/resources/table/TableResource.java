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

import ai.floedb.floecat.catalog.rpc.ListSnapshotsResponse;
import ai.floedb.floecat.catalog.rpc.ListTablesResponse;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.gateway.iceberg.minimal.api.dto.LoadTableResultDto;
import ai.floedb.floecat.gateway.iceberg.minimal.api.dto.TableIdentifierDto;
import ai.floedb.floecat.gateway.iceberg.minimal.api.dto.TableListResponseDto;
import ai.floedb.floecat.gateway.iceberg.minimal.api.request.MetricsReportRequest;
import ai.floedb.floecat.gateway.iceberg.minimal.api.request.TableCommitRequest;
import ai.floedb.floecat.gateway.iceberg.minimal.api.request.TableCreateRequest;
import ai.floedb.floecat.gateway.iceberg.minimal.api.request.TableRegisterRequest;
import ai.floedb.floecat.gateway.iceberg.minimal.api.request.TransactionCommitRequest;
import ai.floedb.floecat.gateway.iceberg.minimal.config.MinimalGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.minimal.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.minimal.resources.common.NamespacePaths;
import ai.floedb.floecat.gateway.iceberg.minimal.resources.common.NotYetImplementedResponses;
import ai.floedb.floecat.gateway.iceberg.minimal.services.metadata.FileIoFactory;
import ai.floedb.floecat.gateway.iceberg.minimal.services.metadata.TableMetadataImportService;
import ai.floedb.floecat.gateway.iceberg.minimal.services.table.ConnectorCleanupService;
import ai.floedb.floecat.gateway.iceberg.minimal.services.table.TableBackend;
import ai.floedb.floecat.gateway.iceberg.minimal.services.table.TableMetadataMapper;
import ai.floedb.floecat.gateway.iceberg.minimal.services.transaction.TransactionCommitService;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
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
import java.util.List;
import java.util.Map;

@Path("/v1/{prefix}/namespaces/{namespace}")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class TableResource {
  private final TableBackend backend;
  private final ObjectMapper mapper;
  private final TableMetadataImportService metadataImportService;
  private final TransactionCommitService transactionCommitService;
  private final ConnectorCleanupService connectorCleanupService;
  private final MinimalGatewayConfig config;

  public TableResource(
      TableBackend backend,
      ObjectMapper mapper,
      TableMetadataImportService metadataImportService,
      TransactionCommitService transactionCommitService,
      ConnectorCleanupService connectorCleanupService,
      MinimalGatewayConfig config) {
    this.backend = backend;
    this.mapper = mapper;
    this.metadataImportService = metadataImportService;
    this.transactionCommitService = transactionCommitService;
    this.connectorCleanupService = connectorCleanupService;
    this.config = config;
  }

  @GET
  @Path("/tables")
  public Response list(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @QueryParam("pageSize") Integer pageSize,
      @QueryParam("pageToken") String pageToken) {
    try {
      List<String> namespacePath = NamespacePaths.split(namespace);
      ListTablesResponse response = backend.list(prefix, namespacePath, pageSize, pageToken);
      return Response.ok(
              new TableListResponseDto(
                  response.getTablesList().stream()
                      .map(table -> new TableIdentifierDto(namespacePath, table.getDisplayName()))
                      .toList(),
                  response.getPage().getNextPageToken().isBlank()
                      ? null
                      : response.getPage().getNextPageToken()))
          .build();
    } catch (StatusRuntimeException exception) {
      return tableError(exception);
    }
  }

  @POST
  @Path("/tables")
  public Response create(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      TableCreateRequest request) {
    if (request == null) {
      return IcebergErrorResponses.validation("Request body is required");
    }
    if (request.name() == null || request.name().isBlank()) {
      return IcebergErrorResponses.validation("name is required");
    }
    if (request.schema() == null || request.schema().isNull()) {
      return IcebergErrorResponses.validation("schema is required");
    }
    if (Boolean.TRUE.equals(request.stageCreate())) {
      try {
        return Response.ok(stagedCreateResult(namespace, request)).build();
      } catch (IllegalArgumentException exception) {
        return IcebergErrorResponses.validation(exception.getMessage());
      } catch (Exception exception) {
        return IcebergErrorResponses.validation(exception.getMessage());
      }
    }
    try {
      List<String> namespacePath = NamespacePaths.split(namespace);
      String resolvedLocation = resolvedTableLocation(namespace, request);
      TableCreateRequest effectiveRequest =
          new TableCreateRequest(
              request.name(),
              request.schema(),
              request.partitionSpec(),
              request.writeOrder(),
              resolvedLocation,
              request.properties(),
              false);
      String metadataLocation = initialMetadataLocation(resolvedLocation);
      var imported = metadataImportService.buildInitialMetadata(effectiveRequest, metadataLocation);
      Map<String, String> mergedProperties = new java.util.LinkedHashMap<>(imported.properties());
      mergedProperties.remove("metadata-location");
      if (request.properties() != null) {
        mergedProperties.putAll(request.properties());
        mergedProperties.remove("metadata-location");
      }
      Table created =
          backend.create(
              prefix,
              namespacePath,
              request.name().trim(),
              mapper.readTree(imported.schemaJson()),
              resolvedLocation,
              mergedProperties,
              idempotencyKey);
      return Response.ok(
              new LoadTableResultDto(
                  metadataLocation,
                  TableMetadataMapper.loadImportedMetadata(
                      created, imported.icebergMetadata(), mapper),
                  Map.of()))
          .build();
    } catch (StatusRuntimeException exception) {
      return tableError(exception);
    } catch (IllegalArgumentException exception) {
      return IcebergErrorResponses.validation(exception.getMessage());
    } catch (Exception exception) {
      return IcebergErrorResponses.validation(exception.getMessage());
    }
  }

  @GET
  @Path("/tables/{table}")
  public Response get(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @HeaderParam("If-None-Match") String ifNoneMatch) {
    try {
      List<String> namespacePath = NamespacePaths.split(namespace);
      Table tableRecord = backend.get(prefix, namespacePath, table);
      Snapshot snapshot = null;
      try {
        snapshot = backend.currentSnapshot(prefix, namespacePath, table);
      } catch (StatusRuntimeException ignored) {
        // Empty tables are valid and may not have a current snapshot yet.
      }
      String metadataLocation = TableMetadataMapper.metadataLocation(tableRecord, snapshot);
      String etag = metadataLocation == null ? null : '"' + metadataLocation + '"';
      if (etag != null && ifNoneMatch != null && ifNoneMatch.trim().equals(etag)) {
        return Response.notModified().build();
      }
      Response.ResponseBuilder builder =
          Response.ok(loadResult(prefix, namespacePath, tableRecord, snapshot));
      if (etag != null) {
        builder.header("ETag", etag);
      }
      return builder.build();
    } catch (StatusRuntimeException exception) {
      return tableError(exception);
    }
  }

  @POST
  @Path("/tables/{table}")
  public Response commit(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      TableCommitRequest request) {
    if (request == null) {
      return IcebergErrorResponses.validation("Request body is required");
    }
    if (request.updates() == null) {
      return IcebergErrorResponses.validation("updates are required");
    }
    try {
      List<String> namespacePath = NamespacePaths.split(namespace);
      Response txResponse =
          transactionCommitService.commit(
              prefix,
              idempotencyKey,
              new TransactionCommitRequest(
                  List.of(
                      new TransactionCommitRequest.TableChange(
                          new TableIdentifierDto(namespacePath, table),
                          request.requirements() == null ? List.of() : request.requirements(),
                          request.updates()))));
      if (txResponse.getStatus() != Response.Status.NO_CONTENT.getStatusCode()) {
        return txResponse;
      }
      Table committed = backend.get(prefix, namespacePath, table);
      Snapshot snapshot = null;
      try {
        snapshot = backend.currentSnapshot(prefix, namespacePath, table);
      } catch (StatusRuntimeException ignored) {
        // property-only commits may not have snapshots in unit tests
      }
      return Response.ok(loadResult(prefix, namespacePath, committed, snapshot)).build();
    } catch (StatusRuntimeException exception) {
      return tableError(exception);
    }
  }

  @DELETE
  @Path("/tables/{table}")
  public Response delete(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table) {
    try {
      List<String> namespacePath = NamespacePaths.split(namespace);
      Table existing = backend.get(prefix, namespacePath, table);
      connectorCleanupService.deleteManagedConnector(existing);
      backend.delete(prefix, namespacePath, table);
      return Response.noContent().build();
    } catch (StatusRuntimeException exception) {
      return tableError(exception);
    }
  }

  @HEAD
  @Path("/tables/{table}")
  public Response exists(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table) {
    try {
      backend.exists(prefix, NamespacePaths.split(namespace), table);
      return Response.noContent().build();
    } catch (StatusRuntimeException exception) {
      return IcebergErrorResponses.grpc(exception);
    }
  }

  @POST
  @Path("/register")
  public Response register(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      TableRegisterRequest request) {
    if (request == null
        || request.metadataLocation() == null
        || request.metadataLocation().isBlank()) {
      return IcebergErrorResponses.validation("metadata-location is required");
    }
    if (request.name() == null || request.name().isBlank()) {
      return IcebergErrorResponses.validation("name is required");
    }
    if (Boolean.TRUE.equals(request.overwrite())) {
      return NotYetImplementedResponses.endpoint("Register overwrite is not implemented yet");
    }
    try {
      var imported =
          metadataImportService.importMetadata(
              request.metadataLocation().trim(),
              FileIoFactory.filterIoProperties(
                  request.properties() == null ? Map.of() : request.properties()));
      Map<String, String> mergedProperties = new java.util.LinkedHashMap<>(imported.properties());
      if (request.properties() != null) {
        mergedProperties.putAll(request.properties());
      }
      Table created =
          backend.create(
              prefix,
              NamespacePaths.split(namespace),
              request.name().trim(),
              mapper.readTree(imported.schemaJson()),
              imported.tableLocation(),
              mergedProperties,
              idempotencyKey);
      return Response.ok(
              new LoadTableResultDto(
                  request.metadataLocation().trim(),
                  TableMetadataMapper.loadImportedMetadata(
                      created, imported.icebergMetadata(), mapper),
                  Map.of()))
          .build();
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    } catch (Exception e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }
  }

  private LoadTableResultDto loadResult(
      String prefix, List<String> namespacePath, Table tableRecord, Snapshot snapshot) {
    String metadataLocation = TableMetadataMapper.metadataLocation(tableRecord, snapshot);
    Map<String, Object> metadata = TableMetadataMapper.loadMetadata(tableRecord, snapshot, mapper);
    List<Snapshot> snapshots = listSnapshots(prefix, namespacePath, tableRecord.getDisplayName());
    if (metadataLocation != null && TableMetadataMapper.requiresHydration(metadata)) {
      try {
        var hydrated =
            metadataImportService.loadTableMetadata(
                metadataLocation, FileIoFactory.filterIoProperties(tableRecord.getPropertiesMap()));
        metadata =
            TableMetadataMapper.loadHydratedMetadata(
                tableRecord, hydrated, metadataLocation, mapper, snapshots);
      } catch (IllegalArgumentException ignored) {
        // Fall back to the in-catalog view when metadata-file hydration is unavailable.
      }
    }
    metadata = TableMetadataMapper.overlaySnapshots(metadata, snapshots);
    return new LoadTableResultDto(metadataLocation, metadata, Map.of());
  }

  private List<Snapshot> listSnapshots(
      String prefix, List<String> namespacePath, String tableName) {
    try {
      ListSnapshotsResponse response = backend.listSnapshots(prefix, namespacePath, tableName);
      if (response == null) {
        return List.of();
      }
      return List.copyOf(response.getSnapshotsList());
    } catch (StatusRuntimeException exception) {
      return List.of();
    }
  }

  private LoadTableResultDto stagedCreateResult(String namespace, TableCreateRequest request) {
    String resolvedLocation = resolvedTableLocation(namespace, request);
    TableCreateRequest effectiveRequest =
        new TableCreateRequest(
            request.name(),
            request.schema(),
            request.partitionSpec(),
            request.writeOrder(),
            resolvedLocation,
            request.properties(),
            request.stageCreate());
    String metadataLocation = syntheticStageMetadataLocation(resolvedLocation);
    Map<String, String> ioProperties =
        FileIoFactory.filterIoProperties(
            effectiveRequest.properties() == null ? Map.of() : effectiveRequest.properties());
    var imported =
        metadataImportService.writeInitialMetadata(
            effectiveRequest, metadataLocation, ioProperties);
    return new LoadTableResultDto(
        metadataLocation,
        TableMetadataMapper.loadImportedMetadata(
            Table.newBuilder()
                .setDisplayName(effectiveRequest.name().trim())
                .setSchemaJson(imported.schemaJson())
                .putAllProperties(imported.properties())
                .build(),
            imported.icebergMetadata(),
            mapper),
        stagedCreateConfig(metadataLocation));
  }

  private String resolvedTableLocation(String namespace, TableCreateRequest request) {
    if (request.location() != null && !request.location().isBlank()) {
      return request.location().trim();
    }
    String warehouse = config == null ? null : config.defaultWarehousePath().orElse(null);
    if (warehouse == null || warehouse.isBlank()) {
      throw new IllegalArgumentException("location is required");
    }
    List<String> namespacePath = NamespacePaths.split(namespace);
    return joinLocation(warehouse, namespacePath, request.name());
  }

  private String joinLocation(String root, List<String> namespacePath, String tableName) {
    StringBuilder out = new StringBuilder(stripTrailingSlash(root));
    for (String part : namespacePath) {
      if (part != null && !part.isBlank()) {
        out.append('/').append(stripSlashes(part));
      }
    }
    if (tableName != null && !tableName.isBlank()) {
      out.append('/').append(stripSlashes(tableName));
    }
    return out.toString();
  }

  private String stripTrailingSlash(String value) {
    if (value == null) {
      return null;
    }
    String out = value;
    while (out.endsWith("/") && out.length() > 1) {
      out = out.substring(0, out.length() - 1);
    }
    return out;
  }

  private String stripSlashes(String value) {
    String out = value == null ? "" : value.trim();
    while (out.startsWith("/")) {
      out = out.substring(1);
    }
    while (out.endsWith("/")) {
      out = out.substring(0, out.length() - 1);
    }
    return out;
  }

  private String syntheticStageMetadataLocation(String tableLocation) {
    if (tableLocation == null || tableLocation.isBlank()) {
      return null;
    }
    String base =
        tableLocation.endsWith("/")
            ? tableLocation.substring(0, tableLocation.length() - 1)
            : tableLocation;
    return base + "/metadata/00000-stage.metadata.json";
  }

  private String initialMetadataLocation(String tableLocation) {
    if (tableLocation == null || tableLocation.isBlank()) {
      return null;
    }
    String base =
        tableLocation.endsWith("/")
            ? tableLocation.substring(0, tableLocation.length() - 1)
            : tableLocation;
    return base + "/metadata/00000.metadata.json";
  }

  private Map<String, String> stagedCreateConfig(String metadataLocation) {
    String metadataDirectory = metadataDirectory(metadataLocation);
    if (metadataDirectory == null || metadataDirectory.isBlank()) {
      return Map.of();
    }
    return Map.of("write.metadata.path", metadataDirectory);
  }

  private String metadataDirectory(String metadataLocation) {
    if (metadataLocation == null || metadataLocation.isBlank()) {
      return null;
    }
    int slash = metadataLocation.lastIndexOf('/');
    if (slash < 0) {
      return null;
    }
    return metadataLocation.substring(0, slash);
  }

  @POST
  @Path("/tables/{table}/metrics")
  public Response publishMetrics(
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      MetricsReportRequest request) {
    if (request == null) {
      return IcebergErrorResponses.validation("Request body is required");
    }
    if (request.reportType() == null || request.reportType().isBlank()) {
      return IcebergErrorResponses.validation("report-type is required");
    }
    if (request.tableName() == null || request.tableName().isBlank()) {
      return IcebergErrorResponses.validation("table-name is required");
    }
    if (request.snapshotId() == null) {
      return IcebergErrorResponses.validation("snapshot-id is required");
    }
    if (request.metrics() == null) {
      return IcebergErrorResponses.validation("metrics is required");
    }
    return Response.noContent().build();
  }

  private Response tableError(StatusRuntimeException exception) {
    if (exception.getStatus().getCode() == Status.Code.NOT_FOUND) {
      String message = exception.getStatus().getDescription();
      return IcebergErrorResponses.noSuchTable(
          message == null || message.isBlank() ? "Table not found" : message);
    }
    return IcebergErrorResponses.grpc(exception);
  }
}
