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

package ai.floedb.floecat.gateway.iceberg.rest.services.catalog;

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.connector.rpc.DeleteConnectorRequest;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.StorageCredentialDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.common.SnapshotMetadataUtil;
import ai.floedb.floecat.gateway.iceberg.rest.config.ConnectorIntegrationConfig;
import ai.floedb.floecat.gateway.iceberg.rest.config.ConnectorIntegrationProperties;
import ai.floedb.floecat.gateway.iceberg.rest.config.StorageAwsConfig;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.CatalogResolver;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.GrpcServiceFacade;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.FileIoFactory;
import ai.floedb.floecat.gateway.iceberg.rest.services.storage.StorageCredentialAuthority;
import ai.floedb.floecat.gateway.iceberg.rest.services.storage.StorageLocationResolver;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TableGatewaySupport {
  private static final Logger LOG = Logger.getLogger(TableGatewaySupport.class);

  private final GrpcWithHeaders grpc;
  private final IcebergGatewayConfig gatewayConfig;
  private final ConnectorIntegrationConfig connectorConfig;
  private final StorageAwsConfig storageAwsConfig;
  private final ObjectMapper mapper;
  private final GrpcServiceFacade grpcClient;
  private final StorageCredentialAuthority storageCredentialAuthority;

  @Inject
  public TableGatewaySupport(
      GrpcWithHeaders grpc,
      IcebergGatewayConfig gatewayConfig,
      ConnectorIntegrationConfig connectorConfig,
      StorageAwsConfig storageAwsConfig,
      ObjectMapper mapper,
      GrpcServiceFacade grpcClient,
      StorageCredentialAuthority storageCredentialAuthority) {
    this.grpc = grpc;
    this.gatewayConfig = gatewayConfig;
    this.connectorConfig = connectorConfig;
    this.storageAwsConfig = storageAwsConfig;
    this.mapper = mapper;
    this.grpcClient = grpcClient;
    this.storageCredentialAuthority = storageCredentialAuthority;
  }

  public TableSpec.Builder buildCreateSpec(
      ResourceId catalogId, ResourceId namespaceId, String tableName, TableRequests.Create req)
      throws JsonProcessingException {
    TableSpec.Builder spec = baseTableSpec(catalogId, namespaceId, tableName);
    if (req == null) {
      return spec;
    }
    if (req.schema() != null && !req.schema().isNull()) {
      String schemaJson = mapper.writeValueAsString(req.schema());
      if (!schemaJson.isBlank()) {
        spec.setSchemaJson(schemaJson);
      }
    }
    if (req.location() != null && !req.location().isBlank()) {
      spec.putProperties("location", req.location());
      UpstreamRef.Builder upstream =
          spec.getUpstream().toBuilder()
              .setFormat(TableFormat.TF_ICEBERG)
              .setColumnIdAlgorithm(ColumnIdAlgorithm.CID_FIELD_ID)
              .setUri(req.location());
      spec.setUpstream(upstream.build());
    }
    if (req.properties() != null && !req.properties().isEmpty()) {
      spec.putAllProperties(sanitizeCreateProperties(req.properties()));
    }
    return spec;
  }

  public TableSpec.Builder baseTableSpec(
      ResourceId catalogId, ResourceId namespaceId, String tableName) {
    return TableSpec.newBuilder()
        .setCatalogId(catalogId)
        .setNamespaceId(namespaceId)
        .setDisplayName(tableName)
        .setUpstream(
            UpstreamRef.newBuilder()
                .setFormat(TableFormat.TF_ICEBERG)
                .setColumnIdAlgorithm(ColumnIdAlgorithm.CID_FIELD_ID)
                .build());
  }

  private Map<String, String> sanitizeCreateProperties(Map<String, String> props) {
    if (props.isEmpty()) {
      return Map.of();
    }
    Map<String, String> sanitized = new LinkedHashMap<>();
    props.forEach(
        (key, value) -> {
          if (key == null || value == null) {
            return;
          }
          if ("metadata-location".equals(key)) {
            return;
          }
          if (FileIoFactory.isFileIoProperty(key)) {
            return;
          }
          sanitized.put(key, value);
        });
    return sanitized;
  }

  public String resolveTableLocation(String requestedLocation, String metadataLocation) {
    if (requestedLocation != null && !requestedLocation.isBlank()) {
      return requestedLocation;
    }
    if (metadataLocation == null || metadataLocation.isBlank()) {
      return null;
    }
    int idx = metadataLocation.indexOf("/metadata/");
    String base;
    if (idx > 0) {
      base = metadataLocation.substring(0, idx);
    } else {
      int slash = metadataLocation.lastIndexOf('/');
      base = slash > 0 ? metadataLocation.substring(0, slash) : metadataLocation;
    }
    return base;
  }

  public boolean connectorIntegrationEnabled() {
    return connectorConfig.enabled();
  }

  public void deleteConnector(ResourceId connectorId) {
    if (connectorId == null) {
      return;
    }
    try {
      grpcClient.deleteConnector(
          DeleteConnectorRequest.newBuilder().setConnectorId(connectorId).build());
    } catch (StatusRuntimeException e) {
      LOG.warnf(e, "Failed to delete connector %s", connectorId.getId());
    }
  }

  public Map<String, String> defaultTableConfig() {
    return ConnectorIntegrationProperties.defaultTableConfig(connectorConfig, storageAwsConfig);
  }

  public Map<String, String> defaultTableConfig(Table table) {
    LinkedHashMap<String, String> resolved = new LinkedHashMap<>(defaultTableConfig());
    resolved.putAll(storageCredentialAuthority.clientSafeConfig(table));
    return resolved.isEmpty() ? Map.of() : Map.copyOf(resolved);
  }

  public Map<String, String> resolveRegisterFileIoProperties(
      Map<String, String> requestProperties) {
    Map<String, String> resolved = new LinkedHashMap<>(defaultFileIoProperties());
    if (requestProperties != null && !requestProperties.isEmpty()) {
      requestProperties.forEach(
          (k, v) -> {
            if (FileIoFactory.isFileIoProperty(k) && isUsableIoValue(v)) {
              resolved.put(k, v.trim());
            }
          });
    }
    return resolved.isEmpty() ? Map.of() : Map.copyOf(resolved);
  }

  public Map<String, String> defaultFileIoProperties() {
    return ConnectorIntegrationProperties.defaultFileIoProperties(
        connectorConfig, storageAwsConfig, FileIoFactory::isFileIoProperty);
  }

  public Map<String, String> defaultFileIoProperties(Table table) {
    LinkedHashMap<String, String> resolved = new LinkedHashMap<>(defaultFileIoProperties());
    defaultTableConfig(table)
        .forEach(
            (key, value) -> {
              if (FileIoFactory.isFileIoProperty(key) && isUsableIoValue(value)) {
                resolved.put(key, value.trim());
              }
            });
    return resolved.isEmpty() ? Map.of() : Map.copyOf(resolved);
  }

  public Map<String, String> serverSideFileIoProperties(Table table) {
    return resolveServerSideFileIoProperties(
        table, StorageLocationResolver.resolveLocationPrefix(table));
  }

  public Map<String, String> serverSideFileIoPropertiesForLocation(Table table, String location) {
    return resolveServerSideFileIoProperties(table, location);
  }

  public Map<String, String> serverSideFileIoPropertiesForLocation(String location) {
    return resolveServerSideFileIoProperties(null, location);
  }

  public List<StorageCredentialDto> credentialsForAccessDelegation(
      Table table, String accessDelegationMode) {
    if (accessDelegationMode == null || accessDelegationMode.isBlank()) {
      return null;
    }
    boolean vended = false;
    for (String raw : accessDelegationMode.split(",")) {
      String mode = raw == null ? "" : raw.trim();
      if (mode.isEmpty()) {
        continue;
      }
      if ("vended-credentials".equalsIgnoreCase(mode)) {
        vended = true;
        continue;
      }
      throw new IllegalArgumentException("Unsupported access delegation mode: " + mode);
    }
    if (!vended) {
      return null;
    }
    List<StorageCredentialDto> credentials =
        storageCredentialAuthority.resolveForTable(table, true);
    if (credentials == null || credentials.isEmpty()) {
      throw new IllegalArgumentException(
          "Credential vending was requested but no credentials are available");
    }
    return credentials;
  }

  public IcebergMetadata loadCurrentMetadata(Table table) {
    if (table == null || !table.hasResourceId()) {
      return null;
    }
    try {
      SnapshotRef.Builder ref = SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT);
      var requestBuilder =
          GetSnapshotRequest.newBuilder().setTableId(table.getResourceId()).setSnapshot(ref);
      var response = grpcClient.getSnapshot(requestBuilder.build());
      if (response == null || !response.hasSnapshot()) {
        return null;
      }
      return SnapshotMetadataUtil.parseSnapshotMetadata(response.getSnapshot());
    } catch (StatusRuntimeException e) {
      return null;
    }
  }

  public String loadCurrentMetadataLocation(Table table) {
    if (table == null || !table.hasResourceId()) {
      return null;
    }
    try {
      SnapshotRef.Builder ref = SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT);
      var requestBuilder =
          GetSnapshotRequest.newBuilder().setTableId(table.getResourceId()).setSnapshot(ref);
      var response = grpcClient.getSnapshot(requestBuilder.build());
      if (response == null || !response.hasSnapshot()) {
        return null;
      }
      return SnapshotMetadataUtil.metadataLocation(response.getSnapshot());
    } catch (StatusRuntimeException e) {
      return null;
    }
  }

  private IcebergMetadata loadSnapshotById(ResourceId tableId, Long snapshotId) {
    if (snapshotId == null || snapshotId <= 0) {
      return null;
    }
    SnapshotRef.Builder ref = SnapshotRef.newBuilder().setSnapshotId(snapshotId);
    var response =
        grpcClient.getSnapshot(
            GetSnapshotRequest.newBuilder().setTableId(tableId).setSnapshot(ref).build());
    if (response == null || !response.hasSnapshot()) {
      return null;
    }
    var snapshot = response.getSnapshot();
    return SnapshotMetadataUtil.parseSnapshotMetadata(snapshot);
  }

  public ConnectorIntegrationConfig.RegisterConnectorTemplate connectorTemplateFor(String prefix) {
    Map<String, ConnectorIntegrationConfig.RegisterConnectorTemplate> templates =
        connectorConfig.registerConnectors();
    if (templates == null || templates.isEmpty()) {
      return null;
    }
    ConnectorIntegrationConfig.RegisterConnectorTemplate direct = templates.get(prefix);
    if (direct != null) {
      return direct;
    }
    String resolved = CatalogResolver.resolveCatalog(gatewayConfig, prefix);
    return templates.get(resolved);
  }

  private static boolean isUsableIoValue(String value) {
    return ConnectorIntegrationProperties.isUsableValue(value);
  }

  private Map<String, String> resolveServerSideFileIoProperties(Table table, String location) {
    LinkedHashMap<String, String> resolved =
        new LinkedHashMap<>(
            table == null ? defaultFileIoProperties() : defaultFileIoProperties(table));
    if (table != null && table.getPropertiesCount() > 0) {
      FileIoFactory.filterIoProperties(table.getPropertiesMap())
          .forEach(
              (key, value) -> {
                if (isUsableIoValue(value)) {
                  resolved.put(key, value.trim());
                }
              });
    }
    resolveAuthorityFileIoProperties(table, location)
        .forEach(
            (key, value) -> {
              if (FileIoFactory.isFileIoProperty(key) && isUsableIoValue(value)) {
                resolved.put(key, value.trim());
              }
            });
    return resolved.isEmpty() ? Map.of() : Map.copyOf(resolved);
  }

  private Map<String, String> resolveAuthorityFileIoProperties(Table table, String location) {
    if (table == null) {
      return Map.of();
    }
    return storageCredentialAuthority.resolveServerSideFileIoConfig(table, false);
  }

  // Snapshot metadata parsing lives in SnapshotMetadataUtil.
}
