package ai.floedb.floecat.gateway.iceberg.rest.services.table;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.catalog.rpc.UpdateTableRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.common.MetadataLocationUtil;
import ai.floedb.floecat.gateway.iceberg.rest.common.TableResponseMapper;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.NamespaceRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.FileIoFactory;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.MetadataLocationSync;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.SnapshotMetadataService;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.TableMetadataImportService;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.TableMetadataImportService.ImportedMetadata;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import com.google.protobuf.FieldMask;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TableRegisterService {
  private static final Logger LOG = Logger.getLogger(TableRegisterService.class);

  @Inject TableLifecycleService tableLifecycleService;
  @Inject TableMetadataImportService tableMetadataImportService;
  @Inject SnapshotMetadataService snapshotMetadataService;
  @Inject TableCommitService tableCommitService;

  public Response register(
      NamespaceRequestContext namespaceContext,
      String idempotencyKey,
      TableRequests.Register req,
      TableGatewaySupport tableSupport) {
    if (req == null || req.metadataLocation() == null || req.metadataLocation().isBlank()) {
      return IcebergErrorResponses.validation("metadata-location is required");
    }
    if (req.name() == null || req.name().isBlank()) {
      return IcebergErrorResponses.validation("name is required");
    }
    String metadataLocation = req.metadataLocation().trim();
    String tableName = req.name().trim();

    Map<String, String> ioProperties =
        req.properties() == null ? new LinkedHashMap<>() : new LinkedHashMap<>(req.properties());
    Map<String, String> sanitizedIoProps = FileIoFactory.filterIoProperties(ioProperties);
    ImportedMetadata importedMetadata;
    try {
      importedMetadata = tableMetadataImportService.importMetadata(metadataLocation, ioProperties);
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }

    TableSpec.Builder spec =
        tableSupport.baseTableSpec(
            namespaceContext.catalogId(), namespaceContext.namespaceId(), tableName);
    if (importedMetadata.schemaJson() != null && !importedMetadata.schemaJson().isBlank()) {
      spec.setSchemaJson(importedMetadata.schemaJson());
    }
    Map<String, String> mergedProps =
        mergeImportedProperties(null, importedMetadata, metadataLocation, sanitizedIoProps);
    if (!mergedProps.isEmpty()) {
      spec.putAllProperties(mergedProps);
    }
    tableSupport.addMetadataLocationProperties(spec, metadataLocation);

    Table created;
    try {
      created = tableLifecycleService.createTable(spec, idempotencyKey);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.ALREADY_EXISTS) {
        if (Boolean.TRUE.equals(req.overwrite())) {
          return overwriteRegisteredTable(
              namespaceContext,
              tableName,
              metadataLocation,
              idempotencyKey,
              importedMetadata,
              sanitizedIoProps,
              tableSupport);
        }
        return IcebergErrorResponses.conflict("Table already exists");
      }
      throw e;
    }
    final Table initialCreated = created;
    Supplier<Table> createdSupplier = () -> initialCreated;
    Response snapshotBootstrap =
        snapshotMetadataService.ensureImportedCurrentSnapshot(
            tableSupport,
            created.getResourceId(),
            namespaceContext.namespacePath(),
            tableName,
            createdSupplier,
            importedMetadata,
            idempotencyKey);
    if (snapshotBootstrap != null) {
      return snapshotBootstrap;
    }
    Table ensuredCreated =
        MetadataLocationSync.ensureMetadataLocation(
            tableLifecycleService,
            tableSupport,
            initialCreated.getResourceId(),
            initialCreated,
            metadataLocation);
    if (ensuredCreated != null) {
      created = ensuredCreated;
    }

    Map<String, String> ioProps = FileIoFactory.filterIoProperties(created.getPropertiesMap());
    LOG.infof(
        "Register table io props namespace=%s.%s props=%s",
        String.join(".", namespaceContext.namespacePath()), tableName, ioProps);

    String resolvedLocation =
        tableSupport.resolveTableLocation(
            importedMetadata != null ? importedMetadata.tableLocation() : null, metadataLocation);
    ResourceId connectorId =
        configureConnector(
            namespaceContext,
            tableName,
            created.getResourceId(),
            metadataLocation,
            resolvedLocation,
            null,
            idempotencyKey,
            tableSupport,
            sanitizedIoProps);
    tableCommitService.runConnectorSync(
        tableSupport, connectorId, namespaceContext.namespacePath(), tableName);

    IcebergMetadata metadata = tableSupport.loadCurrentMetadata(created);
    Response.ResponseBuilder builder =
        Response.ok(
            TableResponseMapper.toLoadResult(
                tableName,
                created,
                metadata,
                List.of(),
                tableSupport.defaultTableConfig(),
                tableSupport.defaultCredentials()));
    String etagValue = metadataLocation(created, metadata);
    if (etagValue != null) {
      builder.tag(etagValue);
    }
    return builder.build();
  }

  private Response overwriteRegisteredTable(
      NamespaceRequestContext namespaceContext,
      String tableName,
      String metadataLocation,
      String idempotencyKey,
      ImportedMetadata importedMetadata,
      Map<String, String> sanitizedIoProps,
      TableGatewaySupport tableSupport) {
    ResourceId tableId =
        tableLifecycleService.resolveTableId(
            namespaceContext.catalogName(), namespaceContext.namespacePath(), tableName);
    Table existing;
    try {
      existing = tableLifecycleService.getTable(tableId);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        return IcebergErrorResponses.notFound(
            "Table "
                + String.join(".", namespaceContext.namespacePath())
                + "."
                + tableName
                + " not found");
      }
      throw e;
    }

    Map<String, String> props =
        mergeImportedProperties(
            existing.getPropertiesMap(), importedMetadata, metadataLocation, sanitizedIoProps);
    LOG.infof(
        "Register overwrite merged metadata namespace=%s.%s merged=%s imported=%s",
        String.join(".", namespaceContext.namespacePath()),
        tableName,
        props.get("metadata-location"),
        metadataLocation);

    FieldMask.Builder mask = FieldMask.newBuilder().addPaths("properties");
    TableSpec.Builder updateSpec = TableSpec.newBuilder().putAllProperties(props);
    tableSupport.addMetadataLocationProperties(updateSpec, metadataLocation);
    if (importedMetadata != null
        && importedMetadata.schemaJson() != null
        && !importedMetadata.schemaJson().isBlank()) {
      updateSpec.setSchemaJson(importedMetadata.schemaJson());
      mask.addPaths("schema_json");
    }
    Table updated =
        tableLifecycleService.updateTable(
            UpdateTableRequest.newBuilder()
                .setTableId(tableId)
                .setSpec(updateSpec)
                .setUpdateMask(mask)
                .build());
    LOG.infof(
        "Register overwrite update response namespace=%s.%s metadata=%s",
        String.join(".", namespaceContext.namespacePath()),
        tableName,
        updated.getPropertiesMap().get("metadata-location"));
    Long currentSnapshotId = propertyLong(props, "current-snapshot-id");
    if (currentSnapshotId != null && currentSnapshotId > 0) {
      snapshotMetadataService.updateSnapshotMetadataLocation(
          tableId, currentSnapshotId, metadataLocation);
    }

    Table snapshotTable = updated;
    Response snapshotBootstrap =
        snapshotMetadataService.ensureImportedCurrentSnapshot(
            tableSupport,
            tableId,
            namespaceContext.namespacePath(),
            tableName,
            () -> snapshotTable,
            importedMetadata,
            idempotencyKey);
    if (snapshotBootstrap != null) {
      return snapshotBootstrap;
    }
    Table ensuredUpdated =
        MetadataLocationSync.ensureMetadataLocation(
            tableLifecycleService, tableSupport, tableId, updated, metadataLocation);
    if (ensuredUpdated != null) {
      updated = ensuredUpdated;
    }

    ResourceId connectorId =
        existing.hasUpstream() && existing.getUpstream().hasConnectorId()
            ? existing.getUpstream().getConnectorId()
            : null;
    String resolvedLocation =
        tableSupport.resolveTableLocation(
            importedMetadata != null ? importedMetadata.tableLocation() : null, metadataLocation);
    if (connectorId == null) {
      connectorId =
          configureConnector(
              namespaceContext,
              tableName,
              tableId,
              metadataLocation,
              resolvedLocation,
              null,
              idempotencyKey,
              tableSupport,
              sanitizedIoProps);
      tableCommitService.runConnectorSync(
          tableSupport, connectorId, namespaceContext.namespacePath(), tableName);
    } else {
      tableSupport.updateConnectorMetadata(connectorId, metadataLocation);
      String existingUri =
          existing.hasUpstream() && existing.getUpstream().getUri() != null
              ? existing.getUpstream().getUri()
              : null;
      if (resolvedLocation != null
          && (existingUri == null || !resolvedLocation.equals(existingUri))) {
        tableSupport.updateTableUpstream(
            tableId, namespaceContext.namespacePath(), tableName, connectorId, resolvedLocation);
      }
      tableCommitService.runConnectorSync(
          tableSupport, connectorId, namespaceContext.namespacePath(), tableName);
    }

    IcebergMetadata metadata = tableSupport.loadCurrentMetadata(updated);
    Response.ResponseBuilder builder =
        Response.ok(
            TableResponseMapper.toLoadResult(
                tableName,
                updated,
                metadata,
                List.of(),
                tableSupport.defaultTableConfig(),
                tableSupport.defaultCredentials()));
    String etagValue = metadataLocation(updated, metadata);
    if (etagValue != null) {
      builder.tag(etagValue);
    }
    return builder.build();
  }

  private ResourceId configureConnector(
      NamespaceRequestContext namespaceContext,
      String tableName,
      ResourceId tableId,
      String metadataLocation,
      String resolvedTableLocation,
      String existingUpstreamUri,
      String idempotencyKey,
      TableGatewaySupport tableSupport,
      Map<String, String> fileIoProps) {
    var connectorTemplate = tableSupport.connectorTemplateFor(namespaceContext.prefix());
    ResourceId connectorId = null;
    String upstreamUri = null;
    if (connectorTemplate != null && connectorTemplate.uri() != null) {
      connectorId =
          tableSupport.createTemplateConnector(
              namespaceContext.prefix(),
              namespaceContext.namespacePath(),
              namespaceContext.namespaceId(),
              namespaceContext.catalogId(),
              tableName,
              tableId,
              connectorTemplate,
              idempotencyKey);
      upstreamUri = connectorTemplate.uri();
    } else if (resolvedTableLocation != null && !resolvedTableLocation.isBlank()) {
      String metadata =
          metadataLocation != null && !metadataLocation.isBlank()
              ? metadataLocation
              : resolvedTableLocation;
      connectorId =
          tableSupport.createExternalConnector(
              namespaceContext.prefix(),
              namespaceContext.namespacePath(),
              namespaceContext.namespaceId(),
              namespaceContext.catalogId(),
              tableName,
              tableId,
              metadata,
              resolvedTableLocation,
              idempotencyKey,
              fileIoProps);
      upstreamUri = resolvedTableLocation;
    }
    if (connectorId == null || upstreamUri == null || upstreamUri.isBlank()) {
      return null;
    }
    if (existingUpstreamUri == null || !existingUpstreamUri.equals(upstreamUri)) {
      tableSupport.updateTableUpstream(
          tableId, namespaceContext.namespacePath(), tableName, connectorId, upstreamUri);
    }
    return connectorId;
  }

  private Map<String, String> mergeImportedProperties(
      Map<String, String> existing,
      ImportedMetadata importedMetadata,
      String metadataLocation,
      Map<String, String> sanitizedIoProps) {
    Map<String, String> merged = new LinkedHashMap<>();
    if (existing != null && !existing.isEmpty()) {
      merged.putAll(existing);
    }
    if (importedMetadata != null && importedMetadata.properties() != null) {
      merged.putAll(importedMetadata.properties());
    }
    if (sanitizedIoProps != null && !sanitizedIoProps.isEmpty()) {
      merged.putAll(sanitizedIoProps);
    }
    if (importedMetadata != null
        && importedMetadata.tableLocation() != null
        && !importedMetadata.tableLocation().isBlank()) {
      merged.put("location", importedMetadata.tableLocation());
    }
    MetadataLocationUtil.setMetadataLocation(merged, metadataLocation);
    return merged;
  }

  private Long propertyLong(Map<String, String> props, String key) {
    if (props == null || props.isEmpty()) {
      return null;
    }
    String value = props.get(key);
    if (value == null || value.isBlank()) {
      return null;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException ignored) {
      return null;
    }
  }

  private String metadataLocation(Table table, IcebergMetadata metadata) {
    Map<String, String> props =
        table == null || table.getPropertiesMap() == null ? Map.of() : table.getPropertiesMap();
    String propertyLocation = MetadataLocationUtil.metadataLocation(props);
    if (propertyLocation != null
        && !propertyLocation.isBlank()
        && !MetadataLocationUtil.isPointer(propertyLocation)) {
      return propertyLocation;
    }
    if (metadata != null
        && metadata.getMetadataLocation() != null
        && !metadata.getMetadataLocation().isBlank()) {
      return metadata.getMetadataLocation();
    }
    if (propertyLocation != null && !propertyLocation.isBlank()) {
      return propertyLocation;
    }
    return table != null && table.hasResourceId() ? table.getResourceId().getId() : null;
  }
}
