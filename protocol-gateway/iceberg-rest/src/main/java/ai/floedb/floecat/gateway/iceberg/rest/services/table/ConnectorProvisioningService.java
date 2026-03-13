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
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.AuthConfig;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.connector.rpc.ConnectorState;
import ai.floedb.floecat.connector.rpc.DestinationTarget;
import ai.floedb.floecat.connector.rpc.NamespacePath;
import ai.floedb.floecat.connector.rpc.SourceSelector;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.FileIoFactory;
import ai.floedb.floecat.storage.kv.Keys;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.core.Response;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.jboss.logging.Logger;

@ApplicationScoped
public class ConnectorProvisioningService {
  private static final Logger LOG = Logger.getLogger(ConnectorProvisioningService.class);

  public record ProvisionResult(
      ai.floedb.floecat.catalog.rpc.Table table,
      ResourceId connectorId,
      List<ai.floedb.floecat.transaction.rpc.TxChange> connectorTxChanges,
      Response error) {}

  public ProvisionResult resolveOrCreateForCommit(
      String accountId,
      String txId,
      String prefix,
      TableGatewaySupport tableSupport,
      List<String> namespacePath,
      ResourceId namespaceId,
      ResourceId catalogId,
      String tableName,
      ResourceId tableId,
      ai.floedb.floecat.catalog.rpc.Table table) {
    if (table == null || tableId == null || tableName == null || tableName.isBlank()) {
      return new ProvisionResult(table, resolveConnectorId(table), List.of(), null);
    }
    String metadataLocation = tableMetadataLocation(table);
    String requestedLocation = table.getPropertiesMap().get("location");
    String resolvedTableLocation =
        tableSupport.resolveTableLocation(requestedLocation, metadataLocation);
    Timestamp nowTs = Timestamps.fromMillis(System.currentTimeMillis());
    ResourceId existing = resolveConnectorId(table);
    if (existing != null) {
      var existingConnector = tableSupport.getConnector(existing);
      if (existingConnector.isEmpty()) {
        LOG.warnf(
            "Connector %s referenced by table %s was not found", existing.getId(), tableId.getId());
        return new ProvisionResult(
            table,
            null,
            List.of(),
            IcebergErrorResponses.failure(
                "connector provisioning failed",
                "CommitStateUnknownException",
                Response.Status.SERVICE_UNAVAILABLE));
      }
      Connector.Builder updatedConnector = existingConnector.get().toBuilder().setUpdatedAt(nowTs);
      String connectorUri = existingConnector.get().getUri();
      Map<String, String> nextProperties =
          new LinkedHashMap<>(existingConnector.get().getPropertiesMap());
      if (metadataLocation != null && !metadataLocation.isBlank()) {
        nextProperties.put("external.metadata-location", metadataLocation);
      }
      if (resolvedTableLocation != null && !resolvedTableLocation.isBlank()) {
        connectorUri = resolvedTableLocation;
      }
      updatedConnector.clearProperties().putAllProperties(nextProperties);
      if (connectorUri != null && !connectorUri.isBlank()) {
        updatedConnector.setUri(connectorUri);
      }
      Connector connectorRecord = updatedConnector.build();
      List<ai.floedb.floecat.transaction.rpc.TxChange> connectorTxChanges =
          connectorUpsertChanges(accountId, connectorRecord);
      ai.floedb.floecat.catalog.rpc.Table enriched =
          enrichTableUpstream(table, namespacePath, tableName, existing, connectorRecord.getUri());
      return new ProvisionResult(enriched, existing, connectorTxChanges, null);
    }
    if (!tableSupport.connectorIntegrationEnabled()) {
      return new ProvisionResult(table, null, List.of(), null);
    }
    var connectorTemplate = tableSupport.connectorTemplateFor(prefix);

    Connector connectorRecord =
        buildConnectorForCommit(
            accountId,
            txId,
            prefix,
            namespacePath,
            namespaceId,
            catalogId,
            tableName,
            tableId,
            metadataLocation,
            resolvedTableLocation,
            fileIoPropertiesForConnector(tableSupport, table),
            connectorTemplate,
            nowTs);
    if (connectorRecord == null || !connectorRecord.hasResourceId()) {
      return new ProvisionResult(table, null, List.of(), null);
    }
    List<ai.floedb.floecat.transaction.rpc.TxChange> connectorTxChanges =
        connectorUpsertChanges(accountId, connectorRecord);
    ai.floedb.floecat.catalog.rpc.Table enriched =
        enrichTableUpstream(
            table,
            namespacePath,
            tableName,
            connectorRecord.getResourceId(),
            connectorRecord.getUri());
    return new ProvisionResult(enriched, connectorRecord.getResourceId(), connectorTxChanges, null);
  }

  private String tableMetadataLocation(ai.floedb.floecat.catalog.rpc.Table table) {
    return table == null ? null : table.getPropertiesMap().get("metadata-location");
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
    upstream.setConnectorId(connectorId).clearNamespacePath().addAllNamespacePath(namespacePath);
    if (tableName != null && !tableName.isBlank()) {
      upstream.setTableDisplayName(tableName);
    }
    if (upstreamUri != null && !upstreamUri.isBlank()) {
      upstream.setUri(upstreamUri);
    }
    return table.toBuilder().setUpstream(upstream).build();
  }

  private List<ai.floedb.floecat.transaction.rpc.TxChange> connectorUpsertChanges(
      String accountId, Connector connector) {
    if (connector == null || !connector.hasResourceId() || connector.getDisplayName().isBlank()) {
      return List.of();
    }
    ByteString payload = ByteString.copyFrom(connector.toByteArray());
    String connectorId = connector.getResourceId().getId();
    String byIdPointer = Keys.connectorPointerById(accountId, connectorId);
    String byNamePointer = Keys.connectorPointerByName(accountId, connector.getDisplayName());
    return List.of(
        ai.floedb.floecat.transaction.rpc.TxChange.newBuilder()
            .setTargetPointerKey(byIdPointer)
            .setPayload(payload)
            .build(),
        ai.floedb.floecat.transaction.rpc.TxChange.newBuilder()
            .setTargetPointerKey(byNamePointer)
            .setPayload(payload)
            .build());
  }

  private Connector buildConnectorForCommit(
      String accountId,
      String txId,
      String prefix,
      List<String> namespacePath,
      ResourceId namespaceId,
      ResourceId catalogId,
      String tableName,
      ResourceId tableId,
      String metadataLocation,
      String resolvedTableLocation,
      Map<String, String> ioProperties,
      ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig.RegisterConnectorTemplate
          connectorTemplate,
      Timestamp nowTs) {
    String namespaceFq = namespacePath == null ? "" : String.join(".", namespacePath);
    ResourceId connectorId = deterministicConnectorId(accountId, txId, tableId);
    SourceSelector source =
        SourceSelector.newBuilder()
            .setNamespace(NamespacePath.newBuilder().addAllSegments(namespacePath).build())
            .setTable(tableName)
            .build();
    DestinationTarget destination =
        DestinationTarget.newBuilder()
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setTableId(tableId)
            .setTableDisplayName(tableName)
            .build();
    Connector.Builder connector =
        Connector.newBuilder()
            .setResourceId(connectorId)
            .setKind(ConnectorKind.CK_ICEBERG)
            .setSource(source)
            .setDestination(destination)
            .setAuth(AuthConfig.newBuilder().setScheme("none").build())
            .setCreatedAt(nowTs)
            .setUpdatedAt(nowTs)
            .setState(ConnectorState.CS_ACTIVE);
    if (connectorTemplate != null && connectorTemplate.uri() != null) {
      String displayName =
          connectorTemplate
              .displayName()
              .orElseGet(
                  () ->
                      "register:"
                          + prefix
                          + (namespaceFq.isBlank() ? "" : ":" + namespaceFq)
                          + "."
                          + tableName);
      connector.setDisplayName(displayName).setUri(connectorTemplate.uri());
      if (connectorTemplate.description().isPresent()) {
        connector.setDescription(connectorTemplate.description().get());
      }
      if (connectorTemplate.properties() != null && !connectorTemplate.properties().isEmpty()) {
        connector.putAllProperties(connectorTemplate.properties());
      }
      connector.putProperties(
          "floecat.connector.capture-statistics",
          Boolean.toString(connectorTemplate.captureStatistics()));
      return connector.build();
    }
    String metadata =
        metadataLocation != null && !metadataLocation.isBlank()
            ? metadataLocation
            : resolvedTableLocation;
    if (metadata == null || metadata.isBlank()) {
      return null;
    }
    String connectorUri =
        (resolvedTableLocation != null && !resolvedTableLocation.isBlank())
            ? resolvedTableLocation
            : metadata;
    String displayName =
        "register:" + prefix + (namespaceFq.isBlank() ? "" : ":" + namespaceFq) + "." + tableName;
    Map<String, String> props = new LinkedHashMap<>();
    props.put("external.metadata-location", metadata);
    props.put("iceberg.source", "filesystem");
    props.put("external.table-name", tableName);
    props.put("external.namespace", namespaceFq);
    props.put("floecat.connector.capture-statistics", Boolean.toString(true));
    if (ioProperties != null && !ioProperties.isEmpty()) {
      ioProperties.forEach(
          (k, v) -> {
            if (FileIoFactory.isFileIoProperty(k) && v != null && !v.isBlank()) {
              props.put(k, v.trim());
            }
          });
    }
    return connector
        .setDisplayName(displayName)
        .setUri(connectorUri)
        .putAllProperties(props)
        .build();
  }

  private ResourceId deterministicConnectorId(String accountId, String txId, ResourceId tableId) {
    String seed = (txId == null ? "" : txId) + "|" + (tableId == null ? "" : tableId.getId());
    UUID deterministicId = UUID.nameUUIDFromBytes(seed.getBytes(StandardCharsets.UTF_8));
    return ResourceId.newBuilder()
        .setAccountId(accountId == null ? "" : accountId)
        .setId(deterministicId.toString())
        .setKind(ResourceKind.RK_CONNECTOR)
        .build();
  }

  private Map<String, String> fileIoPropertiesForConnector(
      TableGatewaySupport tableSupport, ai.floedb.floecat.catalog.rpc.Table table) {
    Map<String, String> ioProperties =
        new LinkedHashMap<>(
            tableSupport == null ? Map.of() : tableSupport.defaultFileIoProperties());
    if (table == null || table.getPropertiesMap().isEmpty()) {
      return ioProperties.isEmpty() ? Map.of() : Map.copyOf(ioProperties);
    }
    table
        .getPropertiesMap()
        .forEach(
            (key, value) -> {
              if (key != null
                  && value != null
                  && !value.isBlank()
                  && FileIoFactory.isFileIoProperty(key)) {
                ioProperties.put(key, value.trim());
              }
            });
    return ioProperties.isEmpty() ? Map.of() : Map.copyOf(ioProperties);
  }
}
