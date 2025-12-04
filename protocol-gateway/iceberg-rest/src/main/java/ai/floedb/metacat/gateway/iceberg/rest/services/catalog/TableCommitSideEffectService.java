package ai.floedb.metacat.gateway.iceberg.rest.services.catalog;

import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.catalog.rpc.TableSpec;
import ai.floedb.metacat.catalog.rpc.UpdateTableRequest;
import ai.floedb.metacat.catalog.rpc.UpstreamRef;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.gateway.iceberg.rest.api.dto.CommitTableResponseDto;
import ai.floedb.metacat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.metacat.gateway.iceberg.rest.services.metadata.MetadataMirrorException;
import ai.floedb.metacat.gateway.iceberg.rest.services.metadata.MetadataMirrorService;
import com.google.protobuf.FieldMask;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TableCommitSideEffectService {
  private static final Logger LOG = Logger.getLogger(TableCommitSideEffectService.class);

  @Inject MetadataMirrorService metadataMirrorService;
  @Inject TableLifecycleService tableLifecycleService;

  public MirrorMetadataResult mirrorMetadata(
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
      LOG.warnf(
          e,
          "Failed to mirror Iceberg metadata for %s.%s to %s (serving original metadata)",
          namespace,
          table,
          metadataLocation);
      return MirrorMetadataResult.success(metadata, metadataLocation);
    }
  }

  public CommitTableResponseDto applyMirrorResult(
      CommitTableResponseDto responseDto, MirrorMetadataResult mirrorResult) {
    if (responseDto == null || mirrorResult == null) {
      return responseDto;
    }
    TableMetadataView updatedMetadata =
        mirrorResult.metadata() != null ? mirrorResult.metadata() : responseDto.metadata();
    String updatedLocation =
        nonBlank(mirrorResult.metadataLocation(), responseDto.metadataLocation());
    if (updatedMetadata == responseDto.metadata()
        && java.util.Objects.equals(updatedLocation, responseDto.metadataLocation())) {
      return responseDto;
    }
    return new CommitTableResponseDto(updatedLocation, updatedMetadata);
  }

  public ResourceId synchronizeConnector(
      TableGatewaySupport tableSupport,
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
      return resolveConnectorId(tableRecord);
    }
    ResourceId connectorId = resolveConnectorId(tableRecord);
    if (connectorId == null) {
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

  public void runConnectorSync(
      TableGatewaySupport tableSupport,
      ResourceId connectorId,
      List<String> namespacePath,
      String tableName) {
    if (connectorId == null || connectorId.getId().isBlank()) {
      return;
    }
    tableSupport.runSyncMetadataCapture(connectorId, namespacePath, tableName);
    tableSupport.triggerScopedReconcile(connectorId, namespacePath, tableName);
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
      tableLifecycleService.updateTable(request);
    } catch (Exception e) {
      LOG.warnf(e, "Failed to update metadata properties for tableId=%s", tableId.getId());
    }
  }

  private ResourceId resolveConnectorId(Table tableRecord) {
    if (tableRecord == null
        || !tableRecord.hasUpstream()
        || !tableRecord.getUpstream().hasConnectorId()) {
      return null;
    }
    ResourceId connectorId = tableRecord.getUpstream().getConnectorId();
    if (connectorId == null || connectorId.getId().isBlank()) {
      return null;
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

  private static String nonBlank(String primary, String fallback) {
    return primary != null && !primary.isBlank() ? primary : fallback;
  }
}
