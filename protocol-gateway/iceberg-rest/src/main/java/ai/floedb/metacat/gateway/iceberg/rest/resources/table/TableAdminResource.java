package ai.floedb.metacat.gateway.iceberg.rest.resources.table;

import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.catalog.rpc.TableServiceGrpc;
import ai.floedb.metacat.catalog.rpc.TableSpec;
import ai.floedb.metacat.catalog.rpc.UpdateTableRequest;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.metacat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.metacat.gateway.iceberg.rest.api.dto.*;
import ai.floedb.metacat.gateway.iceberg.rest.api.error.IcebergError;
import ai.floedb.metacat.gateway.iceberg.rest.api.error.IcebergErrorResponse;
import ai.floedb.metacat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.metacat.gateway.iceberg.rest.api.request.*;
import ai.floedb.metacat.gateway.iceberg.rest.services.catalog.StageCommitException;
import ai.floedb.metacat.gateway.iceberg.rest.services.catalog.StageCommitProcessor;
import ai.floedb.metacat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.metacat.gateway.iceberg.rest.services.metadata.MetadataMirrorException;
import ai.floedb.metacat.gateway.iceberg.rest.services.metadata.MetadataMirrorService;
import ai.floedb.metacat.gateway.iceberg.rest.services.resolution.NameResolution;
import ai.floedb.metacat.gateway.iceberg.rest.services.staging.StagedTableService;
import ai.floedb.metacat.gateway.iceberg.rest.services.tenant.TenantContext;
import ai.floedb.metacat.gateway.iceberg.rest.support.mapper.TableResponseMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.FieldMask;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.eclipse.microprofile.config.Config;

@Path("/v1/{prefix}/tables")
@Produces(MediaType.APPLICATION_JSON)
public class TableAdminResource {
  @Inject GrpcWithHeaders grpc;
  @Inject IcebergGatewayConfig config;
  @Inject StagedTableService stagedTableService;
  @Inject TenantContext tenantContext;
  @Inject StageCommitProcessor stageCommitProcessor;
  @Inject MetadataMirrorService metadataMirrorService;
  @Inject ObjectMapper mapper;
  @Inject Config mpConfig;

  private TableGatewaySupport tableSupport;

  @PostConstruct
  void initSupport() {
    this.tableSupport = new TableGatewaySupport(grpc, config, mapper, mpConfig);
  }

  @Path("/rename")
  @POST
  public Response rename(
      @PathParam("prefix") String prefix,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      RenameRequest request) {
    if (request == null || request.source() == null || request.destination() == null) {
      return validationError("source and destination are required");
    }
    if (request.source().namespace() == null
        || request.source().name() == null
        || request.destination().namespace() == null
        || request.destination().name() == null) {
      return validationError("namespace and name must be provided");
    }
    String catalogName = resolveCatalog(prefix);
    var sourcePath = request.source().namespace();
    var destinationPath = request.destination().namespace();

    ResourceId tableId =
        NameResolution.resolveTable(grpc, catalogName, sourcePath, request.source().name());
    ResourceId namespaceId = NameResolution.resolveNamespace(grpc, catalogName, destinationPath);

    TableServiceGrpc.TableServiceBlockingStub stub = grpc.withHeaders(grpc.raw().table());
    TableSpec.Builder spec =
        TableSpec.newBuilder()
            .setNamespaceId(namespaceId)
            .setDisplayName(request.destination().name());
    FieldMask mask =
        FieldMask.newBuilder().addPaths("namespace_id").addPaths("display_name").build();
    stub.updateTable(
        UpdateTableRequest.newBuilder()
            .setTableId(tableId)
            .setSpec(spec)
            .setUpdateMask(mask)
            .build());
    return Response.noContent().build();
  }

  @Path("/transactions/commit")
  @POST
  public Response commitTransaction(
      @PathParam("prefix") String prefix, TransactionCommitRequest request) {
    if (request == null
        || request.stagedRefUpdates() == null
        || request.stagedRefUpdates().isEmpty()) {
      return validationError("staged-ref-updates are required");
    }
    String tenantId = tenantContext.getTenantId();
    if (tenantId == null || tenantId.isBlank()) {
      return validationError("tenant context is required");
    }
    String catalogName = resolveCatalog(prefix);
    ResourceId catalogId = resolveCatalogId(prefix);
    List<TransactionCommitResponse.TransactionCommitResult> results = new java.util.ArrayList<>();
    for (TransactionCommitRequest.StagedRefUpdate update : request.stagedRefUpdates()) {
      try {
        StageCommitProcessor.StageCommitResult stageResult =
            stageCommitProcessor.commitStage(
                prefix,
                catalogName,
                tenantId,
                update.table().namespace(),
                update.table().name(),
                update.stageId());
        String namespaceFq = String.join(".", update.table().namespace());
        ResourceId namespaceId =
            NameResolution.resolveNamespace(grpc, catalogName, update.table().namespace());
        MirrorMetadataResult mirrorResult =
            mirrorMetadata(
                namespaceFq,
                stageResult.table().getResourceId(),
                update.table().name(),
                stageResult.loadResult().metadata(),
                stageResult.loadResult().metadataLocation());
        if (mirrorResult.error() != null) {
          return mirrorResult.error();
        }
        TableMetadataView mirroredMetadata =
            mirrorResult.metadata() != null
                ? mirrorResult.metadata()
                : stageResult.loadResult().metadata();
        String metadataLocation =
            nonBlank(mirrorResult.metadataLocation(), stageResult.loadResult().metadataLocation());
        ResourceId connectorId =
            synchronizeConnector(
                prefix,
                update.table().namespace(),
                namespaceId,
                catalogId,
                update.table().name(),
                stageResult.table(),
                mirroredMetadata,
                metadataLocation,
                null);
        runConnectorSyncIfPossible(connectorId, update.table().namespace(), update.table().name());
        TableMetadataView responseMetadata = mirroredMetadata;
        if (responseMetadata == null && metadataLocation != null) {
          responseMetadata =
              TableResponseMapper.toLoadResult(
                      update.table().name(),
                      stageResult.table(),
                      null,
                      List.of(),
                      tableSupport.defaultTableConfig(),
                      tableSupport.defaultCredentials())
                  .metadata();
        }
        results.add(
            new TransactionCommitResponse.TransactionCommitResult(
                update.table(),
                update.stageId(),
                metadataLocation,
                responseMetadata,
                stageResult.loadResult().config(),
                stageResult.loadResult().storageCredentials()));
      } catch (StageCommitException e) {
        return e.toResponse();
      }
    }
    stagedTableService.expireStages();
    return Response.ok(new TransactionCommitResponse(results)).build();
  }

  private String resolveCatalog(String prefix) {
    var mapping = config.catalogMapping();
    return Optional.ofNullable(mapping == null ? null : mapping.get(prefix)).orElse(prefix);
  }

  private ResourceId resolveCatalogId(String prefix) {
    return NameResolution.resolveCatalog(grpc, resolveCatalog(prefix));
  }

  private Response validationError(String message) {
    return Response.status(Response.Status.BAD_REQUEST)
        .entity(new IcebergErrorResponse(new IcebergError(message, "ValidationException", 400)))
        .build();
  }

  private Response conflictError(String message) {
    return Response.status(Response.Status.CONFLICT)
        .entity(new IcebergErrorResponse(new IcebergError(message, "CommitFailedException", 409)))
        .build();
  }

  private Response notFound(String message) {
    return Response.status(Response.Status.NOT_FOUND)
        .entity(new IcebergErrorResponse(new IcebergError(message, "NotFoundException", 404)))
        .build();
  }

  private MirrorMetadataResult mirrorMetadata(
      String namespace,
      ResourceId tableId,
      String table,
      TableMetadataView metadata,
      String metadataLocation) {
    if (metadata == null) {
      return MirrorMetadataResult.success(null, metadataLocation);
    }
    try {
      MetadataMirrorService.MirrorResult mirrorResult =
          metadataMirrorService.mirror(namespace, table, metadata, metadataLocation);
      String resolvedLocation = nonBlank(mirrorResult.metadataLocation(), metadataLocation);
      TableMetadataView resolvedMetadata =
          mirrorResult.metadata() != null ? mirrorResult.metadata() : metadata;
      if (tableId != null) {
        updateTableMetadataProperties(tableId, resolvedMetadata, resolvedLocation);
      }
      return MirrorMetadataResult.success(resolvedMetadata, resolvedLocation);
    } catch (MetadataMirrorException e) {
      return MirrorMetadataResult.failure(
          Response.status(Response.Status.INTERNAL_SERVER_ERROR)
              .entity(
                  new IcebergErrorResponse(
                      new IcebergError(
                          "Failed to persist Iceberg metadata files",
                          "CommitFailedException",
                          500)))
              .build());
    }
  }

  private void updateTableMetadataProperties(
      ResourceId tableId, TableMetadataView metadata, String resolvedLocation) {
    if (tableId == null || metadata == null) {
      return;
    }
    Map<String, String> props =
        metadata.properties() != null
            ? new LinkedHashMap<>(metadata.properties())
            : new LinkedHashMap<>();
    if (resolvedLocation != null && !resolvedLocation.isBlank()) {
      props.put("metadata-location", resolvedLocation);
      props.put("metadata_location", resolvedLocation);
    }
    if (props.isEmpty()) {
      return;
    }
    TableSpec spec = TableSpec.newBuilder().putAllProperties(props).build();
    UpdateTableRequest request =
        UpdateTableRequest.newBuilder()
            .setTableId(tableId)
            .setSpec(spec)
            .setUpdateMask(FieldMask.newBuilder().addPaths("properties").build())
            .build();
    try {
      grpc.withHeaders(grpc.raw().table()).updateTable(request);
    } catch (Exception ignored) {
      // best effort
    }
  }

  private ResourceId synchronizeConnector(
      String prefix,
      List<String> namespacePath,
      ResourceId namespaceId,
      ResourceId catalogId,
      String table,
      Table tableRecord,
      TableMetadataView metadataView,
      String metadataLocation,
      String idempotencyKey) {
    if (tableRecord == null) {
      return null;
    }
    String effectiveMetadata = metadataLocation;
    if ((effectiveMetadata == null || effectiveMetadata.isBlank()) && metadataView != null) {
      effectiveMetadata = metadataView.metadataLocation();
    }
    if (effectiveMetadata == null || effectiveMetadata.isBlank()) {
      Map<String, String> props = tableRecord.getPropertiesMap();
      effectiveMetadata = props.getOrDefault("metadata-location", props.get("metadata_location"));
    }
    if (effectiveMetadata == null || effectiveMetadata.isBlank()) {
      return null;
    }
    ResourceId connectorId =
        tableRecord.hasUpstream() ? tableRecord.getUpstream().getConnectorId() : null;
    if (connectorId == null || connectorId.getId().isBlank()) {
      var connectorTemplate = tableSupport.connectorTemplateFor(prefix);
      if (connectorTemplate != null && connectorTemplate.uri() != null) {
        connectorId =
            tableSupport.createTemplateConnector(
                prefix,
                namespacePath,
                namespaceId,
                catalogId,
                table,
                tableRecord.getResourceId(),
                connectorTemplate,
                idempotencyKey);
        if (connectorId != null) {
          tableSupport.updateTableUpstream(
              tableRecord.getResourceId(),
              namespacePath,
              table,
              connectorId,
              connectorTemplate.uri());
        }
      } else {
        String baseLocation = tableLocation(tableRecord);
        String resolvedLocation =
            tableSupport.resolveTableLocation(baseLocation, effectiveMetadata);
        connectorId =
            tableSupport.createExternalConnector(
                prefix,
                namespacePath,
                namespaceId,
                catalogId,
                table,
                tableRecord.getResourceId(),
                effectiveMetadata,
                resolvedLocation,
                idempotencyKey);
        if (connectorId != null) {
          tableSupport.updateTableUpstream(
              tableRecord.getResourceId(), namespacePath, table, connectorId, resolvedLocation);
        }
      }
    } else {
      tableSupport.updateConnectorMetadata(connectorId, effectiveMetadata);
    }
    return connectorId;
  }

  private String tableLocation(Table tableRecord) {
    if (tableRecord == null) {
      return null;
    }
    if (tableRecord.hasUpstream()) {
      String uri = tableRecord.getUpstream().getUri();
      if (uri != null && !uri.isBlank()) {
        return uri;
      }
    }
    Map<String, String> props = tableRecord.getPropertiesMap();
    String location = props.get("location");
    if (location != null && !location.isBlank()) {
      return location;
    }
    return null;
  }

  private void runConnectorSyncIfPossible(
      ResourceId connectorId, List<String> namespacePath, String tableName) {
    if (connectorId == null || connectorId.getId().isBlank()) {
      return;
    }
    tableSupport.runSyncMetadataCapture(connectorId, namespacePath, tableName);
    tableSupport.triggerScopedReconcile(connectorId, namespacePath, tableName);
  }

  private static String nonBlank(String primary, String fallback) {
    return primary != null && !primary.isBlank() ? primary : fallback;
  }

  private record MirrorMetadataResult(
      Response error, TableMetadataView metadata, String metadataLocation) {
    static MirrorMetadataResult success(TableMetadataView metadata, String location) {
      return new MirrorMetadataResult(null, metadata, location);
    }

    static MirrorMetadataResult failure(Response error) {
      return new MirrorMetadataResult(error, null, null);
    }
  }
}
