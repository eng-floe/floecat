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

package ai.floedb.floecat.gateway.iceberg.rest.services.table;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.catalog.rpc.UpdateTableRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.CommitTableResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.common.MetadataLocationUtil;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.MaterializeMetadataException;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.MaterializeMetadataResult;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.MaterializeMetadataService;
import com.google.protobuf.FieldMask;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TableCommitSideEffectService {
  private static final Logger LOG = Logger.getLogger(TableCommitSideEffectService.class);

  @Inject MaterializeMetadataService materializeMetadataService;
  @Inject TableLifecycleService tableLifecycleService;

  public MaterializeMetadataResult materializeMetadata(
      String namespace,
      ResourceId tableId,
      String table,
      Table tableRecord,
      TableMetadataView metadata,
      String metadataLocation) {
    if (metadata == null) {
      return MaterializeMetadataResult.failure(
          IcebergErrorResponses.validation("metadata is required for materialization"));
    }
    // When no metadata-location is available (e.g. first gRPC connector-driven
    // commit), derive one from the table's location so that materialization can
    // seed the property and break the chicken-and-egg cycle.
    if (!hasText(metadataLocation) && !hasText(metadata.metadataLocation())) {
      metadataLocation = deriveMetadataLocation(tableRecord, metadata);
    }
    boolean requestedLocation = hasText(metadataLocation) || hasText(metadata.metadataLocation());
    try {
      MaterializeMetadataService.MaterializeResult materializeResult =
          materializeMetadataService.materialize(namespace, table, metadata, metadataLocation);
      String resolvedLocation = materializeResult.metadataLocation();
      if (resolvedLocation == null || resolvedLocation.isBlank()) {
        if (!requestedLocation) {
          TableMetadataView resolvedMetadata =
              materializeResult.metadata() != null ? materializeResult.metadata() : metadata;
          return MaterializeMetadataResult.success(resolvedMetadata, resolvedLocation);
        }
        return MaterializeMetadataResult.failure(
            IcebergErrorResponses.failure(
                "metadata materialization returned empty metadata-location",
                "MaterializeMetadataException",
                Response.Status.INTERNAL_SERVER_ERROR));
      }
      TableMetadataView resolvedMetadata =
          materializeResult.metadata() != null ? materializeResult.metadata() : metadata;
      if (tableId != null) {
        updateTableMetadataProperties(tableId, tableRecord, resolvedMetadata, resolvedLocation);
      }
      return MaterializeMetadataResult.success(resolvedMetadata, resolvedLocation);
    } catch (MaterializeMetadataException e) {
      LOG.warnf(
          e,
          "Failed to materialize Iceberg metadata for %s.%s to %s (serving original metadata)",
          namespace,
          table,
          metadataLocation);
      return MaterializeMetadataResult.failure(
          IcebergErrorResponses.failure(
              "metadata materialization failed",
              "MaterializeMetadataException",
              Response.Status.INTERNAL_SERVER_ERROR));
    }
  }

  private static boolean hasText(String value) {
    return value != null && !value.isBlank();
  }

  public CommitTableResponseDto applyMaterializationResult(
      CommitTableResponseDto responseDto, MaterializeMetadataResult materializationResult) {
    if (responseDto == null || materializationResult == null) {
      return responseDto;
    }
    TableMetadataView updatedMetadata =
        materializationResult.metadata() != null
            ? materializationResult.metadata()
            : responseDto.metadata();
    String updatedLocation = updatedMetadata == null ? null : updatedMetadata.metadataLocation();
    if (updatedMetadata == responseDto.metadata()
        && Objects.equals(updatedLocation, responseDto.metadataLocation())) {
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
    if (!tableSupport.connectorIntegrationEnabled()) {
      LOG.debugf(
          "Connector integration disabled; skipping synchronization for %s.%s",
          prefix, table == null ? "<unnamed>" : table);
      return resolveConnectorId(tableRecord);
    }
    if (tableRecord == null) {
      return null;
    }
    String effectiveMetadata = metadataLocation;
    if ((effectiveMetadata == null || effectiveMetadata.isBlank())
        && metadataView != null
        && metadataView.metadataLocation() != null
        && !metadataView.metadataLocation().isBlank()) {
      effectiveMetadata = metadataView.metadataLocation();
    }
    if (effectiveMetadata == null || effectiveMetadata.isBlank()) {
      LOG.infof(
          "Skipping connector sync for %s.%s because metadata-location was empty",
          namespacePath, table);
      return resolveConnectorId(tableRecord);
    }
    LOG.infof(
        "Synchronizing connector metadata namespace=%s table=%s metadata=%s",
        namespacePath, table, effectiveMetadata);
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
        Map<String, String> connectorIoProps = tableSupport.defaultFileIoProperties();
        LOG.infof(
            "Creating external connector namespace=%s table=%s metadata=%s resolvedLocation=%s"
                + " ioProps=%s",
            namespacePath,
            table,
            effectiveMetadata,
            resolvedLocation,
            connectorIoProps.isEmpty() ? "<none>" : connectorIoProps.keySet());
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
                connectorIoProps,
                idempotencyKey);
        if (connectorId != null) {
          tableSupport.updateTableUpstream(
              tableRecord.getResourceId(), namespacePath, table, connectorId, resolvedLocation);
        }
      }
    } else {
      tableSupport.updateConnectorMetadata(connectorId, effectiveMetadata);
      LOG.infof(
          "Updated connector metadata connectorId=%s namespace=%s table=%s metadata=%s",
          connectorId.getId(), namespacePath, table, effectiveMetadata);
    }
    return connectorId;
  }

  public void runConnectorSync(
      TableGatewaySupport tableSupport,
      ResourceId connectorId,
      List<String> namespacePath,
      String tableName) {
    if (!tableSupport.connectorIntegrationEnabled()) {
      return;
    }
    if (connectorId == null || connectorId.getId().isBlank()) {
      return;
    }
    try {
      tableSupport.triggerScopedReconcile(connectorId, namespacePath, tableName);
    } catch (Throwable e) {
      LOG.warnf(
          e,
          "Connector reconcile trigger failed connectorId=%s namespace=%s table=%s",
          connectorId.getId(),
          namespacePath == null ? "<null>" : String.join(".", namespacePath),
          tableName);
    }
  }

  private void updateTableMetadataProperties(
      ResourceId tableId, Table tableRecord, TableMetadataView metadata, String resolvedLocation) {
    if (tableId == null || metadata == null) {
      return;
    }
    Table current = tableRecord;
    if (current == null) {
      try {
        current = tableLifecycleService.getTable(tableId);
      } catch (Exception e) {
        LOG.warnf(
            e,
            "Failed to load table %s for metadata refresh; skipping metadata-property update",
            tableId.getId());
        return;
      }
    }
    if (current == null) {
      LOG.warnf(
          "Table %s not found for metadata refresh; skipping metadata-property update",
          tableId.getId());
      return;
    }
    Map<String, String> props = new LinkedHashMap<>(current.getPropertiesMap());
    boolean mutated = false;
    Map<String, String> metadataProps = metadata.properties();
    if (metadataProps != null && !metadataProps.isEmpty()) {
      for (Map.Entry<String, String> entry : metadataProps.entrySet()) {
        String key = entry.getKey();
        String value = entry.getValue();
        if (key == null || key.isBlank() || value == null) {
          continue;
        }
        mutated |= putIfChanged(props, key, value);
      }
    }
    if (resolvedLocation != null && !resolvedLocation.isBlank()) {
      mutated |= MetadataLocationUtil.updateMetadataLocation(props, resolvedLocation);
    }
    if (!mutated) {
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

  private static boolean putIfChanged(Map<String, String> props, String key, String value) {
    if (key == null || key.isBlank() || value == null) {
      return false;
    }
    String existing = props.put(key, value);
    return !value.equals(existing);
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

  /**
   * Derive an initial metadata-location directory hint from the table's location (upstream URI or
   * {@code location} property). This is used to seed the first metadata materialization for tables
   * that were registered without a metadata-location (e.g. gRPC connector-driven imports).
   */
  private String deriveMetadataLocation(Table tableRecord, TableMetadataView metadata) {
    String location = tableLocation(tableRecord);
    if (location == null && metadata != null) {
      location = metadata.location();
    }
    if (location == null || location.isBlank()) {
      return null;
    }
    String base = stripTrailingSlash(location);
    if (base.toLowerCase(Locale.ROOT).endsWith("/metadata")) {
      return base + "/";
    }
    return base + "/metadata/";
  }

  private String stripTrailingSlash(String value) {
    if (value == null || value.length() <= 1) {
      return value;
    }
    return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
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

  public PostCommitResult finalizeCommitResponse(
      String namespace,
      String tableName,
      ResourceId tableId,
      Table tableRecord,
      CommitTableResponseDto responseDto,
      boolean skipMaterialization) {
    if (responseDto == null) {
      return PostCommitResult.success(null);
    }
    CommitTableResponseDto resolvedResponse = responseDto;
    if (!skipMaterialization) {
      MaterializeMetadataResult materializationResult =
          materializeMetadata(
              namespace,
              tableId,
              tableName,
              tableRecord,
              responseDto.metadata(),
              responseDto.metadataLocation());
      if (materializationResult.error() != null) {
        LOG.warnf(
            "Metadata materialization failed for %s.%s; returning committed response",
            namespace, tableName);
        return PostCommitResult.success(responseDto);
      }
      resolvedResponse = applyMaterializationResult(responseDto, materializationResult);
    }
    return PostCommitResult.success(resolvedResponse);
  }

  public record PostCommitResult(Response error, CommitTableResponseDto response) {
    static PostCommitResult success(CommitTableResponseDto response) {
      return new PostCommitResult(null, response);
    }

    static PostCommitResult failure(Response error) {
      return new PostCommitResult(error, null);
    }

    boolean hasError() {
      return error != null;
    }
  }
}
