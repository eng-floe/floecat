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

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.rpc.GetConnectorRequest;
import ai.floedb.floecat.gateway.iceberg.rest.config.ConnectorIntegrationConfig;
import ai.floedb.floecat.gateway.iceberg.rest.config.ConnectorIntegrationProperties;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.GrpcServiceFacade;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.FileIoFactory;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.jboss.logging.Logger;

@ApplicationScoped
public class ConnectorProvisioningService {
  private static final Logger LOG = Logger.getLogger(ConnectorProvisioningService.class);
  private static final String CAPTURE_STATISTICS_PROPERTY = "floecat.connector.capture-statistics";
  private static final String CONNECTOR_MODE_PROPERTY = "floecat.connector.mode";
  private static final String CONNECTOR_MODE_CAPTURE_ONLY = "capture-only";

  @Inject GrpcServiceFacade grpcClient;

  public record ProvisionResult(
      ai.floedb.floecat.catalog.rpc.Table table,
      List<ai.floedb.floecat.transaction.rpc.TxChange> connectorTxChanges,
      Response error) {}

  public ProvisionResult resolveOrCreateForCommit(
      String prefix,
      TableGatewaySupport tableSupport,
      List<String> namespacePath,
      String tableName,
      ResourceId tableId,
      ai.floedb.floecat.catalog.rpc.Table table) {
    if (table == null || tableId == null || tableName == null || tableName.isBlank()) {
      return new ProvisionResult(table, List.of(), null);
    }
    String tableLocation = tableLocation(tableSupport, table);
    ResourceId existing = resolveConnectorId(table);
    if (existing != null) {
      if (!connectorExists(existing)) {
        LOG.warnf(
            "Connector %s referenced by table %s was not found", existing.getId(), tableId.getId());
        return new ProvisionResult(
            table,
            List.of(),
            IcebergErrorResponses.failure(
                "connector provisioning failed",
                "CommitStateUnknownException",
                Response.Status.SERVICE_UNAVAILABLE));
      }
      ai.floedb.floecat.catalog.rpc.Table enriched =
          enrichTableUpstream(table, namespacePath, tableName, existing, tableLocation);
      return new ProvisionResult(enriched, List.of(), null);
    }
    if (!tableSupport.connectorIntegrationEnabled()) {
      return new ProvisionResult(table, List.of(), null);
    }
    if (tableLocation == null || tableLocation.isBlank()) {
      LOG.warnf("Table %s is missing location during connector provisioning", tableId);
      return new ProvisionResult(
          table,
          List.of(),
          IcebergErrorResponses.failure(
              "connector provisioning requires table location",
              "CommitStateUnknownException",
              Response.Status.SERVICE_UNAVAILABLE));
    }
    ConnectorIntegrationConfig.RegisterConnectorTemplate template =
        tableSupport.connectorTemplateFor(prefix);
    var provisioning =
        ai.floedb.floecat.transaction.rpc.ConnectorProvisioning.newBuilder()
            .setConnectorUri(tableLocation)
            .addAllSourceNamespacePath(namespacePath == null ? List.of() : namespacePath)
            .setSourceTableName(tableName)
            .setDisplayName(displayName(prefix, namespacePath, tableName, template))
            .setDescription(
                template == null ? "" : template.description().filter(v -> !v.isBlank()).orElse(""))
            .putAllProperties(connectorProperties(tableSupport, table, template));
    List<ai.floedb.floecat.transaction.rpc.TxChange> connectorTxChanges =
        List.of(
            ai.floedb.floecat.transaction.rpc.TxChange.newBuilder()
                .setTableId(tableId)
                .setConnectorProvisioning(provisioning)
                .build());
    ai.floedb.floecat.catalog.rpc.Table enriched =
        tableLocation == null || tableLocation.isBlank()
            ? table
            : enrichTableUpstream(table, namespacePath, tableName, null, tableLocation);
    return new ProvisionResult(enriched, connectorTxChanges, null);
  }

  public ResourceId resolveConnectorId(ai.floedb.floecat.catalog.rpc.Table tableRecord) {
    if (tableRecord == null
        || !tableRecord.hasUpstream()
        || !tableRecord.getUpstream().hasConnectorId()) {
      return null;
    }
    ResourceId connectorId = tableRecord.getUpstream().getConnectorId();
    return connectorId == null || connectorId.getId().isBlank() ? null : connectorId;
  }

  private ai.floedb.floecat.catalog.rpc.Table enrichTableUpstream(
      ai.floedb.floecat.catalog.rpc.Table table,
      List<String> namespacePath,
      String tableName,
      ResourceId connectorId,
      String upstreamUri) {
    UpstreamRef.Builder upstream =
        table.hasUpstream()
            ? table.getUpstream().toBuilder()
            : UpstreamRef.newBuilder()
                .setFormat(TableFormat.TF_ICEBERG)
                .setColumnIdAlgorithm(ColumnIdAlgorithm.CID_FIELD_ID);
    if (connectorId != null) {
      upstream.setConnectorId(connectorId);
    }
    upstream.clearNamespacePath().addAllNamespacePath(namespacePath);
    if (tableName != null && !tableName.isBlank()) {
      upstream.setTableDisplayName(tableName);
    }
    if (upstreamUri != null && !upstreamUri.isBlank()) {
      upstream.setUri(upstreamUri);
    }
    return table.toBuilder().setUpstream(upstream).build();
  }

  private String tableLocation(
      TableGatewaySupport tableSupport, ai.floedb.floecat.catalog.rpc.Table table) {
    if (table == null) {
      return null;
    }
    String requestedLocation = table.getPropertiesMap().get("location");
    String metadataLocation = table.getPropertiesMap().get("metadata-location");
    String resolved =
        tableSupport == null
            ? requestedLocation
            : tableSupport.resolveTableLocation(requestedLocation, metadataLocation);
    if (resolved != null && !resolved.isBlank()) {
      return resolved;
    }
    if (table.hasUpstream()) {
      String upstreamUri = table.getUpstream().getUri();
      if (upstreamUri != null && !upstreamUri.isBlank()) {
        return upstreamUri;
      }
    }
    return null;
  }

  private boolean connectorExists(ResourceId connectorId) {
    if (connectorId == null || connectorId.getId().isBlank()) {
      return false;
    }
    try {
      var response =
          grpcClient.getConnector(
              GetConnectorRequest.newBuilder().setConnectorId(connectorId).build());
      return response != null && response.hasConnector();
    } catch (StatusRuntimeException e) {
      return false;
    }
  }

  private String displayName(
      String prefix,
      List<String> namespacePath,
      String tableName,
      ConnectorIntegrationConfig.RegisterConnectorTemplate template) {
    if (template != null && template.displayName().filter(v -> !v.isBlank()).isPresent()) {
      return template.displayName().orElseThrow();
    }
    String namespaceFq =
        namespacePath == null || namespacePath.isEmpty() ? "" : String.join(".", namespacePath);
    return "register:"
        + (prefix == null ? "" : prefix)
        + (namespaceFq.isBlank() ? "" : ":" + namespaceFq)
        + "."
        + tableName;
  }

  private Map<String, String> connectorProperties(
      TableGatewaySupport tableSupport,
      ai.floedb.floecat.catalog.rpc.Table table,
      ConnectorIntegrationConfig.RegisterConnectorTemplate template) {
    LinkedHashMap<String, String> properties = new LinkedHashMap<>();
    if (template != null && template.properties() != null && !template.properties().isEmpty()) {
      properties.putAll(template.properties());
    }
    if (tableSupport != null) {
      properties.putAll(tableSupport.defaultFileIoProperties(table));
    }
    if (table != null && !table.getPropertiesMap().isEmpty()) {
      table
          .getPropertiesMap()
          .forEach(
              (key, value) -> {
                if (tableSupport != null
                    && FileIoFactory.isFileIoProperty(key)
                    && ConnectorIntegrationProperties.isUsableValue(value)) {
                  properties.put(key, value.trim());
                }
              });
    }
    properties.put("iceberg.source", "filesystem");
    properties.put(
        CAPTURE_STATISTICS_PROPERTY,
        Boolean.toString(template == null || template.captureStatistics()));
    properties.put(CONNECTOR_MODE_PROPERTY, CONNECTOR_MODE_CAPTURE_ONLY);
    return Map.copyOf(properties);
  }
}
