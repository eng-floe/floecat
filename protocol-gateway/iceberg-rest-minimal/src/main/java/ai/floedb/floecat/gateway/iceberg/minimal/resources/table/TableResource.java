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
import ai.floedb.floecat.gateway.iceberg.minimal.services.compat.DeltaIcebergMetadataService;
import ai.floedb.floecat.gateway.iceberg.minimal.services.metadata.FileIoFactory;
import ai.floedb.floecat.gateway.iceberg.minimal.services.metadata.TableMetadataImportService;
import ai.floedb.floecat.gateway.iceberg.minimal.services.table.ConnectorCleanupService;
import ai.floedb.floecat.gateway.iceberg.minimal.services.table.TableBackend;
import ai.floedb.floecat.gateway.iceberg.minimal.services.table.TableMetadataMapper;
import ai.floedb.floecat.gateway.iceberg.minimal.services.table.TableStorageCleanupService;
import ai.floedb.floecat.gateway.iceberg.minimal.services.transaction.TransactionCommitService;
import com.fasterxml.jackson.databind.JsonNode;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
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
  private final TableStorageCleanupService tableStorageCleanupService;
  private final MinimalGatewayConfig config;
  private final DeltaIcebergMetadataService deltaMetadataService;

  public TableResource(
      TableBackend backend,
      ObjectMapper mapper,
      TableMetadataImportService metadataImportService,
      TransactionCommitService transactionCommitService,
      ConnectorCleanupService connectorCleanupService,
      TableStorageCleanupService tableStorageCleanupService,
      MinimalGatewayConfig config,
      DeltaIcebergMetadataService deltaMetadataService) {
    this.backend = backend;
    this.mapper = mapper;
    this.metadataImportService = metadataImportService;
    this.transactionCommitService = transactionCommitService;
    this.connectorCleanupService = connectorCleanupService;
    this.tableStorageCleanupService = tableStorageCleanupService;
    this.config = config;
    this.deltaMetadataService = deltaMetadataService;
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
      Response txResponse =
          transactionCommitService.commit(
              prefix,
              idempotencyKey,
              new TransactionCommitRequest(
                  List.of(
                      new TransactionCommitRequest.TableChange(
                          new TableIdentifierDto(namespacePath, request.name().trim()),
                          List.of(Map.of("type", "assert-create")),
                          createTableUpdates(request, resolvedLocation)))));
      if (txResponse.getStatus() != Response.Status.NO_CONTENT.getStatusCode()) {
        return txResponse;
      }
      Table created = backend.get(prefix, namespacePath, request.name().trim());
      Snapshot snapshot = null;
      try {
        snapshot = backend.currentSnapshot(prefix, namespacePath, request.name().trim());
      } catch (StatusRuntimeException ignored) {
        // Newly created tables may not have a current snapshot yet.
      }
      return Response.ok(loadResult(prefix, namespacePath, created, snapshot, SnapshotMode.ALL))
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
      @QueryParam("snapshots") String snapshots,
      @HeaderParam("If-None-Match") String ifNoneMatch) {
    try {
      SnapshotMode snapshotMode = parseSnapshotMode(snapshots);
      List<String> namespacePath = NamespacePaths.split(namespace);
      Table tableRecord = backend.get(prefix, namespacePath, table);
      Snapshot snapshot = null;
      try {
        snapshot = backend.currentSnapshot(prefix, namespacePath, table);
      } catch (StatusRuntimeException ignored) {
        // Empty tables are valid and may not have a current snapshot yet.
      }
      LoadTableResultDto result =
          loadResult(prefix, namespacePath, tableRecord, snapshot, snapshotMode);
      String metadataLocation = result.metadataLocation();
      String etag =
          metadataLocation == null
              ? null
              : '"' + metadataLocation + "|snapshots=" + snapshotMode.name().toLowerCase() + '"';
      if (etag != null && ifNoneMatch != null && ifNoneMatch.trim().equals(etag)) {
        return Response.notModified().build();
      }
      Response.ResponseBuilder builder = Response.ok(result);
      if (etag != null) {
        builder.header("ETag", etag);
      }
      return builder.build();
    } catch (StatusRuntimeException exception) {
      return tableError(exception);
    } catch (IllegalArgumentException exception) {
      return IcebergErrorResponses.validation(exception.getMessage());
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
      return Response.ok(loadResult(prefix, namespacePath, committed, snapshot, SnapshotMode.ALL))
          .build();
    } catch (StatusRuntimeException exception) {
      return tableError(exception);
    }
  }

  @DELETE
  @Path("/tables/{table}")
  public Response delete(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @QueryParam("purgeRequested") Boolean purgeRequested,
      @HeaderParam("Idempotency-Key") String idempotencyKey) {
    try {
      List<String> namespacePath = NamespacePaths.split(namespace);
      Table existing = backend.get(prefix, namespacePath, table);
      if (Boolean.TRUE.equals(purgeRequested)) {
        tableStorageCleanupService.purgeTableData(existing);
      }
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
      return IcebergErrorResponses.grpcStatusOnly(exception);
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
    try {
      List<String> namespacePath = NamespacePaths.split(namespace);
      var imported =
          metadataImportService.importMetadata(
              request.metadataLocation().trim(),
              FileIoFactory.filterIoProperties(
                  request.properties() == null ? Map.of() : request.properties()));
      String tableName = request.name().trim();
      boolean overwrite = Boolean.TRUE.equals(request.overwrite());
      Map<String, String> mergedProperties =
          mergeRegisteredProperties(
              Map.of(), imported, request.metadataLocation().trim(), request.properties());
      if (overwrite) {
        try {
          Table existing = backend.get(prefix, namespacePath, tableName);
          mergedProperties =
              mergeRegisteredProperties(
                  existing.getPropertiesMap(),
                  imported,
                  request.metadataLocation().trim(),
                  request.properties());
        } catch (StatusRuntimeException exception) {
          if (exception.getStatus().getCode() != Status.Code.NOT_FOUND) {
            throw exception;
          }
        }
      }
      Response txResponse =
          transactionCommitService.registerImported(
              prefix,
              idempotencyKey,
              namespacePath,
              tableName,
              imported,
              mergedProperties,
              overwrite);
      if (txResponse.getStatus() != Response.Status.NO_CONTENT.getStatusCode()) {
        return txResponse;
      }
      Table created = backend.get(prefix, namespacePath, tableName);
      Snapshot snapshot = null;
      try {
        snapshot = backend.currentSnapshot(prefix, namespacePath, tableName);
      } catch (StatusRuntimeException ignored) {
        // A registered table may still have no current snapshot in tests.
      }
      return Response.ok(loadResult(prefix, namespacePath, created, snapshot, SnapshotMode.ALL))
          .build();
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    } catch (Exception e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }
  }

  private Map<String, String> mergeRegisteredProperties(
      Map<String, String> existingProperties,
      TableMetadataImportService.ImportedMetadata imported,
      String metadataLocation,
      Map<String, String> requestProperties) {
    Map<String, String> merged = new LinkedHashMap<>();
    if (existingProperties != null && !existingProperties.isEmpty()) {
      merged.putAll(existingProperties);
    }
    if (imported != null && imported.properties() != null) {
      merged.putAll(imported.properties());
    }
    if (imported != null
        && imported.tableLocation() != null
        && !imported.tableLocation().isBlank()) {
      merged.put("location", imported.tableLocation());
    }
    if (requestProperties != null && !requestProperties.isEmpty()) {
      merged.putAll(requestProperties);
    }
    if (metadataLocation != null && !metadataLocation.isBlank()) {
      merged.put("metadata-location", metadataLocation);
    }
    return merged;
  }

  private LoadTableResultDto loadResult(
      String prefix,
      List<String> namespacePath,
      Table tableRecord,
      Snapshot snapshot,
      SnapshotMode snapshotMode) {
    List<Snapshot> allSnapshots =
        listSnapshots(prefix, namespacePath, tableRecord.getDisplayName());
    if (deltaMetadataService != null && deltaMetadataService.enabledFor(tableRecord)) {
      var delta = deltaMetadataService.load(tableRecord, allSnapshots);
      String metadataLocation = delta.metadata().getMetadataLocation();
      Map<String, Object> metadata =
          TableMetadataMapper.loadImportedMetadata(tableRecord, delta.metadata(), mapper);
      metadata =
          TableMetadataMapper.overlaySnapshots(
              metadata, selectSnapshots(metadata, delta.snapshots(), snapshotMode));
      return new LoadTableResultDto(metadataLocation, metadata, Map.of());
    }
    String metadataLocation = TableMetadataMapper.metadataLocation(tableRecord, snapshot);
    Map<String, Object> metadata = TableMetadataMapper.loadMetadata(tableRecord, snapshot, mapper);
    String snapshotMetadataLocation = TableMetadataMapper.snapshotMetadataLocation(snapshot);
    boolean staleSnapshotMetadata =
        metadataLocation != null
            && snapshotMetadataLocation != null
            && !metadataLocation.equals(snapshotMetadataLocation);
    if (metadataLocation != null
        && (TableMetadataMapper.requiresHydration(metadata) || staleSnapshotMetadata)) {
      try {
        List<Snapshot> selectedSnapshots = selectSnapshots(metadata, allSnapshots, snapshotMode);
        var hydrated =
            metadataImportService.loadTableMetadata(
                metadataLocation, FileIoFactory.filterIoProperties(tableRecord.getPropertiesMap()));
        metadata =
            TableMetadataMapper.loadHydratedMetadata(
                tableRecord, hydrated, metadataLocation, mapper, selectedSnapshots);
      } catch (IllegalArgumentException ignored) {
        // Fall back to the in-catalog view when metadata-file hydration is unavailable.
      }
    }
    metadata =
        TableMetadataMapper.overlaySnapshots(
            metadata, selectSnapshots(metadata, allSnapshots, snapshotMode));
    return new LoadTableResultDto(metadataLocation, metadata, Map.of());
  }

  private SnapshotMode parseSnapshotMode(String raw) {
    if (raw == null || raw.isBlank() || "all".equalsIgnoreCase(raw)) {
      return SnapshotMode.ALL;
    }
    if ("refs".equalsIgnoreCase(raw)) {
      return SnapshotMode.REFS;
    }
    throw new IllegalArgumentException("snapshots must be one of [all, refs]");
  }

  private List<Snapshot> selectSnapshots(
      Map<String, Object> metadata, List<Snapshot> snapshots, SnapshotMode snapshotMode) {
    if (snapshotMode != SnapshotMode.REFS || snapshots == null || snapshots.isEmpty()) {
      return snapshots;
    }
    Collection<Long> refIds = referencedSnapshotIds(metadata);
    if (refIds.isEmpty()) {
      return List.of();
    }
    return snapshots.stream().filter(s -> refIds.contains(s.getSnapshotId())).toList();
  }

  @SuppressWarnings("unchecked")
  private Collection<Long> referencedSnapshotIds(Map<String, Object> metadata) {
    Object refs = metadata == null ? null : metadata.get("refs");
    if (!(refs instanceof Map<?, ?> refMap) || refMap.isEmpty()) {
      return List.of();
    }
    List<Long> ids = new ArrayList<>(refMap.size());
    for (Object refValue : refMap.values()) {
      if (!(refValue instanceof Map<?, ?> values)) {
        continue;
      }
      Object snapshotId = values.get("snapshot-id");
      if (snapshotId instanceof Number number) {
        ids.add(number.longValue());
      }
    }
    return List.copyOf(ids);
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
    var imported = metadataImportService.buildInitialMetadata(effectiveRequest, metadataLocation);
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

  private List<Map<String, Object>> createTableUpdates(
      TableCreateRequest request, String resolvedLocation) {
    List<Map<String, Object>> updates = new ArrayList<>();
    updates.add(Map.of("action", "set-location", "location", resolvedLocation));

    Integer formatVersion = requestedFormatVersion(request.properties());
    updates.add(
        Map.of(
            "action",
            "upgrade-format-version",
            "format-version",
            formatVersion == null ? 2 : formatVersion));

    updates.add(
        Map.of("action", "add-schema", "schema", mapper.convertValue(request.schema(), Map.class)));
    updates.add(
        Map.of(
            "action",
            "set-current-schema",
            "schema-id",
            requiredNonNegativeInt(request.schema(), "schema-id", "schema")));

    Map<String, Object> partitionSpec = defaultPartitionSpec(request.partitionSpec());
    updates.add(Map.of("action", "add-spec", "spec", partitionSpec));
    updates.add(
        Map.of(
            "action",
            "set-default-spec",
            "spec-id",
            requiredNonNegativeInt(partitionSpec, "spec-id", "partition-spec")));

    Map<String, Object> sortOrder = defaultSortOrder(request.writeOrder());
    updates.add(Map.of("action", "add-sort-order", "sort-order", sortOrder));
    updates.add(
        Map.of(
            "action",
            "set-default-sort-order",
            "sort-order-id",
            requiredNonNegativeInt(sortOrder, "order-id", "write-order")));

    Map<String, String> properties = createProperties(request.properties());
    if (!properties.isEmpty()) {
      updates.add(Map.of("action", "set-properties", "updates", properties));
    }
    return List.copyOf(updates);
  }

  private enum SnapshotMode {
    ALL,
    REFS
  }

  private Map<String, Object> defaultPartitionSpec(com.fasterxml.jackson.databind.JsonNode node) {
    if (node == null || node.isNull()) {
      return new LinkedHashMap<>(Map.of("spec-id", 0, "fields", List.of()));
    }
    return mapper.convertValue(node, Map.class);
  }

  private Map<String, Object> defaultSortOrder(com.fasterxml.jackson.databind.JsonNode node) {
    if (node == null || node.isNull()) {
      return new LinkedHashMap<>(Map.of("order-id", 0, "fields", List.of()));
    }
    return mapper.convertValue(node, Map.class);
  }

  private Integer requestedFormatVersion(Map<String, String> properties) {
    if (properties == null) {
      return null;
    }
    String value = properties.get("format-version");
    if (value == null || value.isBlank()) {
      return null;
    }
    try {
      int parsed = Integer.parseInt(value);
      return parsed >= 1 ? parsed : null;
    } catch (NumberFormatException ignored) {
      return null;
    }
  }

  private Map<String, String> createProperties(Map<String, String> requestProperties) {
    Map<String, String> properties = new LinkedHashMap<>();
    if (requestProperties != null && !requestProperties.isEmpty()) {
      properties.putAll(requestProperties);
    }
    properties.putIfAbsent("last-sequence-number", "0");
    properties.remove("metadata-location");
    return properties;
  }

  private Integer requiredNonNegativeInt(JsonNode node, String key, String fieldName) {
    if (node == null) {
      throw new IllegalArgumentException(fieldName + " is required");
    }
    return requiredNonNegativeInt(mapper.convertValue(node, Map.class), key, fieldName);
  }

  private Integer requiredNonNegativeInt(Map<String, Object> map, String key, String fieldName) {
    Object raw = map == null ? null : map.get(key);
    Integer value;
    if (raw instanceof Number number) {
      value = number.intValue();
    } else if (raw instanceof String text && !text.isBlank()) {
      try {
        value = Integer.parseInt(text);
      } catch (NumberFormatException e) {
        value = null;
      }
    } else {
      value = null;
    }
    if (value == null || value < 0) {
      throw new IllegalArgumentException(fieldName + " requires " + key);
    }
    return value;
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
    boolean scanReport =
        request.filter() != null
            && request.schemaId() != null
            && !isEmpty(request.projectedFieldIds())
            && !isEmpty(request.projectedFieldNames());
    boolean commitReport =
        request.sequenceNumber() != null
            && request.operation() != null
            && !request.operation().isBlank();
    if (!scanReport && !commitReport) {
      return IcebergErrorResponses.validation("metrics report missing required fields");
    }
    return Response.noContent().build();
  }

  private boolean isEmpty(Collection<?> values) {
    return values == null || values.isEmpty();
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
