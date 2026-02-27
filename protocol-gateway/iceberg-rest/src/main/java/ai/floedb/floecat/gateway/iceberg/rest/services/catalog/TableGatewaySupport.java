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
import ai.floedb.floecat.catalog.rpc.UpdateTableRequest;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.IdempotencyKey;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.connector.rpc.AuthConfig;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.connector.rpc.ConnectorSpec;
import ai.floedb.floecat.connector.rpc.ConnectorState;
import ai.floedb.floecat.connector.rpc.CreateConnectorRequest;
import ai.floedb.floecat.connector.rpc.CreateConnectorResponse;
import ai.floedb.floecat.connector.rpc.DeleteConnectorRequest;
import ai.floedb.floecat.connector.rpc.DestinationTarget;
import ai.floedb.floecat.connector.rpc.GetConnectorRequest;
import ai.floedb.floecat.connector.rpc.NamespacePath;
import ai.floedb.floecat.connector.rpc.SourceSelector;
import ai.floedb.floecat.connector.rpc.SyncCaptureRequest;
import ai.floedb.floecat.connector.rpc.SyncCaptureResponse;
import ai.floedb.floecat.connector.rpc.TriggerReconcileRequest;
import ai.floedb.floecat.connector.rpc.UpdateConnectorRequest;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.StorageCredentialDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.common.MetadataLocationUtil;
import ai.floedb.floecat.gateway.iceberg.rest.common.SnapshotMetadataUtil;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.CatalogResolver;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.ConnectorClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.SnapshotClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.TableClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.FileIoFactory;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.FieldMask;
import com.google.protobuf.util.Timestamps;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.eclipse.microprofile.config.Config;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TableGatewaySupport {
  private static final Logger LOG = Logger.getLogger(TableGatewaySupport.class);
  private static final List<StorageCredentialDto> STATIC_STORAGE_CREDENTIALS =
      List.of(new StorageCredentialDto("*", Map.of("type", "static")));
  private static final String CONNECTOR_CAPTURE_STATS_PROPERTY =
      "floecat.connector.capture-statistics";

  private final GrpcWithHeaders grpc;
  private final IcebergGatewayConfig config;
  private final ObjectMapper mapper;
  private final Config mpConfig;
  private final TableClient tableClient;
  private final SnapshotClient snapshotClient;
  private final ConnectorClient connectorClient;

  private volatile Map<String, String> tableConfigCache;
  private volatile List<StorageCredentialDto> storageCredentialCache;

  public TableGatewaySupport(
      GrpcWithHeaders grpc,
      IcebergGatewayConfig config,
      ObjectMapper mapper,
      Config mpConfig,
      TableClient tableClient,
      SnapshotClient snapshotClient,
      ConnectorClient connectorClient) {
    this.grpc = grpc;
    this.config = config;
    this.mapper = mapper;
    this.mpConfig = mpConfig;
    this.tableClient = tableClient;
    this.snapshotClient = snapshotClient;
    this.connectorClient = connectorClient;
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
    String metadataLocation = metadataLocationFromCreate(req);
    addMetadataLocationProperties(spec, metadataLocation);
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

  public void addMetadataLocationProperties(TableSpec.Builder spec, String metadataLocation) {
    MetadataLocationUtil.setMetadataLocation(spec::putProperties, metadataLocation);
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

  public String metadataLocationFromCreate(TableRequests.Create req) {
    return MetadataLocationUtil.metadataLocation(req == null ? null : req.properties());
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
    return config.connectorIntegrationEnabled();
  }

  public Connector loadConnector(ResourceId connectorId) {
    if (connectorId == null || connectorId.getId().isBlank()) {
      return null;
    }
    var response =
        connectorClient.getConnector(
            GetConnectorRequest.newBuilder().setConnectorId(connectorId).build());
    if (response == null || !response.hasConnector()) {
      LOG.warnf("Connector lookup returned empty response for %s", connectorId.getId());
      return null;
    }
    return response.getConnector();
  }

  public void updateConnectorMetadata(ResourceId connectorId, String metadataLocation) {
    if (connectorId == null || metadataLocation == null || metadataLocation.isBlank()) {
      return;
    }
    try {
      Connector existing = loadConnector(connectorId);
      if (existing == null) {
        return;
      }
      Map<String, String> props = new LinkedHashMap<>(existing.getPropertiesMap());
      props.put("external.metadata-location", metadataLocation);
      props.put("iceberg.source", "filesystem");
      ConnectorSpec spec = ConnectorSpec.newBuilder().putAllProperties(props).build();
      connectorClient.updateConnector(
          UpdateConnectorRequest.newBuilder()
              .setConnectorId(connectorId)
              .setSpec(spec)
              .setUpdateMask(FieldMask.newBuilder().addPaths("properties").build())
              .build());
    } catch (StatusRuntimeException e) {
      LOG.warnf(e, "Failed to update connector metadata for %s", connectorId.getId());
    }
  }

  public void deleteConnector(ResourceId connectorId) {
    if (connectorId == null) {
      return;
    }
    try {
      connectorClient.deleteConnector(
          DeleteConnectorRequest.newBuilder().setConnectorId(connectorId).build());
    } catch (StatusRuntimeException e) {
      LOG.warnf(e, "Failed to delete connector %s", connectorId.getId());
    }
  }

  public Map<String, String> defaultTableConfig() {
    Map<String, String> cached = tableConfigCache;
    if (cached != null) {
      return cached;
    }
    Map<String, String> computed = new LinkedHashMap<>();
    config.metadataFileIo().ifPresent(ioImpl -> computed.putIfAbsent("io-impl", ioImpl));
    config
        .metadataFileIoRoot()
        .ifPresent(root -> computed.putIfAbsent("fs.floecat.test-root", root));
    // Expose non-secret S3 endpoint/path-style settings so clients (e.g. DuckDB) can
    // consistently resolve object storage during both read and write paths.
    config
        .storageCredential()
        .ifPresent(cfg -> cfg.properties().forEach((k, v) -> addClientSafeConfig(computed, k, v)));
    readPrefixedConfig("floecat.gateway.storage-credential.properties.")
        .forEach((k, v) -> addClientSafeConfig(computed, k, v));
    config
        .defaultRegion()
        .filter(region -> region != null && !region.isBlank())
        .ifPresent(
            region -> {
              computed.putIfAbsent("s3.region", region);
              computed.putIfAbsent("region", region);
              computed.putIfAbsent("client.region", region);
            });
    Map<String, String> normalized = computed.isEmpty() ? Map.of() : Map.copyOf(computed);
    tableConfigCache = normalized;
    return normalized;
  }

  private void addClientSafeConfig(Map<String, String> target, String key, String value) {
    if (!isUsableIoValue(value) || key == null || key.isBlank()) {
      return;
    }
    String normalized = key.toLowerCase();
    if (normalized.contains("secret")
        || normalized.contains("access-key")
        || normalized.contains("session-token")
        || normalized.contains("token")) {
      return;
    }
    if ("s3.endpoint".equals(normalized)
        || "s3.path-style-access".equals(normalized)
        || "s3.region".equals(normalized)
        || "region".equals(normalized)
        || "client.region".equals(normalized)) {
      target.putIfAbsent(key, value.trim());
    }
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
    Map<String, String> merged = new LinkedHashMap<>();
    defaultCredentials().stream()
        .findFirst()
        .map(StorageCredentialDto::config)
        .ifPresent(
            credentialProps ->
                credentialProps.forEach(
                    (k, v) -> {
                      if (FileIoFactory.isFileIoProperty(k) && isUsableIoValue(v)) {
                        merged.put(k, v.trim());
                      }
                    }));
    defaultTableConfig()
        .forEach(
            (k, v) -> {
              if (FileIoFactory.isFileIoProperty(k) && isUsableIoValue(v)) {
                merged.put(k, v.trim());
              }
            });
    return merged.isEmpty() ? Map.of() : Map.copyOf(merged);
  }

  public List<StorageCredentialDto> defaultCredentials() {
    List<StorageCredentialDto> cached = storageCredentialCache;
    if (cached != null) {
      return cached;
    }
    Map<String, String> props = new LinkedHashMap<>();
    config.storageCredential().ifPresent(cfg -> props.putAll(cfg.properties()));
    readPrefixedConfig("floecat.gateway.storage-credential.properties.")
        .forEach(
            (k, v) -> {
              if (v != null && !v.isBlank()) {
                props.put(k, v);
              }
            });
    if (props.isEmpty()) {
      storageCredentialCache = STATIC_STORAGE_CREDENTIALS;
      return STATIC_STORAGE_CREDENTIALS;
    }
    String scope =
        config
            .storageCredential()
            .flatMap(IcebergGatewayConfig.StorageCredentialConfig::scope)
            .filter(s -> !s.isBlank())
            .orElseGet(
                () ->
                    mpConfig
                        .getOptionalValue("floecat.gateway.storage-credential.scope", String.class)
                        .filter(s -> s != null && !s.isBlank())
                        .orElse("*"));
    List<StorageCredentialDto> computed =
        List.of(new StorageCredentialDto(scope, Map.copyOf(props)));
    storageCredentialCache = computed;
    return computed;
  }

  public List<StorageCredentialDto> credentialsForAccessDelegation(String accessDelegationMode) {
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
    if (!hasConfiguredCredentials()) {
      throw new IllegalArgumentException(
          "Credential vending was requested but no credentials are available");
    }
    return defaultCredentials();
  }

  private boolean hasConfiguredCredentials() {
    Map<String, String> props = new LinkedHashMap<>();
    config.storageCredential().ifPresent(cfg -> props.putAll(cfg.properties()));
    readPrefixedConfig("floecat.gateway.storage-credential.properties.")
        .forEach(
            (k, v) -> {
              if (v != null && !v.isBlank()) {
                props.put(k, v);
              }
            });
    return !props.isEmpty();
  }

  public IcebergMetadata loadCurrentMetadata(Table table) {
    if (table == null || !table.hasResourceId()) {
      return null;
    }
    try {
      SnapshotRef.Builder ref = SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT);
      var requestBuilder =
          GetSnapshotRequest.newBuilder().setTableId(table.getResourceId()).setSnapshot(ref);
      var response = snapshotClient.getSnapshot(requestBuilder.build());
      if (response == null || !response.hasSnapshot()) {
        return null;
      }
      var snapshot = response.getSnapshot();
      Long propertySnapshotId = propertyLong(table.getPropertiesMap(), "current-snapshot-id");
      if (propertySnapshotId != null
          && propertySnapshotId > 0
          && snapshot.getSnapshotId() != propertySnapshotId) {
        return loadSnapshotById(table.getResourceId(), propertySnapshotId);
      }
      return SnapshotMetadataUtil.parseSnapshotMetadata(snapshot);
    } catch (StatusRuntimeException primaryFailure) {
      return loadSnapshotByProperty(table);
    }
  }

  private IcebergMetadata loadSnapshotByProperty(Table table) {
    Long snapshotId = propertyLong(table.getPropertiesMap(), "current-snapshot-id");
    if (snapshotId == null || snapshotId <= 0) {
      return null;
    }
    try {
      return loadSnapshotById(table.getResourceId(), snapshotId);
    } catch (StatusRuntimeException ignored) {
      return null;
    }
  }

  private IcebergMetadata loadSnapshotById(ResourceId tableId, Long snapshotId) {
    if (snapshotId == null || snapshotId <= 0) {
      return null;
    }
    SnapshotRef.Builder ref = SnapshotRef.newBuilder().setSnapshotId(snapshotId);
    var response =
        snapshotClient.getSnapshot(
            GetSnapshotRequest.newBuilder().setTableId(tableId).setSnapshot(ref).build());
    if (response == null || !response.hasSnapshot()) {
      return null;
    }
    var snapshot = response.getSnapshot();
    return SnapshotMetadataUtil.parseSnapshotMetadata(snapshot);
  }

  public IcebergGatewayConfig.RegisterConnectorTemplate connectorTemplateFor(String prefix) {
    Map<String, IcebergGatewayConfig.RegisterConnectorTemplate> templates =
        config.registerConnectors();
    if (templates == null || templates.isEmpty()) {
      return null;
    }
    IcebergGatewayConfig.RegisterConnectorTemplate direct = templates.get(prefix);
    if (direct != null) {
      return direct;
    }
    String resolved = CatalogResolver.resolveCatalog(config, prefix);
    return templates.get(resolved);
  }

  public ResourceId createTemplateConnector(
      String prefix,
      List<String> namespacePath,
      ResourceId namespaceId,
      ResourceId catalogId,
      String tableName,
      ResourceId tableId,
      IcebergGatewayConfig.RegisterConnectorTemplate template,
      String idempotencyKey) {
    NamespacePath nsPath = NamespacePath.newBuilder().addAllSegments(namespacePath).build();
    SourceSelector source =
        SourceSelector.newBuilder().setNamespace(nsPath).setTable(tableName).build();
    var dest =
        DestinationTarget.newBuilder()
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setTableId(tableId)
            .setTableDisplayName(tableName)
            .build();

    String namespaceFq = String.join(".", namespacePath);
    String displayName =
        template
            .displayName()
            .orElseGet(
                () ->
                    "register:"
                        + prefix
                        + (namespaceFq.isBlank() ? "" : ":" + namespaceFq)
                        + "."
                        + tableName);

    ConnectorSpec.Builder spec =
        ConnectorSpec.newBuilder()
            .setDisplayName(displayName)
            .setKind(ConnectorKind.CK_ICEBERG)
            .setUri(template.uri())
            .setSource(source)
            .setDestination(dest);
    spec.setAuth(
        template
            .auth()
            .map(this::toAuthConfig)
            .orElse(AuthConfig.newBuilder().setScheme("none").build()));
    if (template.properties() != null && !template.properties().isEmpty()) {
      spec.putAllProperties(template.properties());
    }
    spec.putProperties(
        CONNECTOR_CAPTURE_STATS_PROPERTY, Boolean.toString(template.captureStatistics()));
    template.description().ifPresent(spec::setDescription);

    CreateConnectorRequest.Builder request =
        CreateConnectorRequest.newBuilder().setSpec(spec.build());
    if (idempotencyKey != null && !idempotencyKey.isBlank()) {
      request.setIdempotency(
          IdempotencyKey.newBuilder().setKey(idempotencyKey + ":connector").build());
    }
    CreateConnectorResponse response = connectorClient.createConnector(request.build());
    if (response == null || !response.hasConnector()) {
      LOG.warnf(
          "Connector service returned empty response for template register %s.%s",
          prefix, tableName);
      return null;
    }
    return response.getConnector().getResourceId();
  }

  public ResourceId createExternalConnector(
      String prefix,
      List<String> namespacePath,
      ResourceId namespaceId,
      ResourceId catalogId,
      String tableName,
      ResourceId tableId,
      String metadataLocation,
      String tableLocation,
      Map<String, String> ioProperties,
      String idempotencyKey) {
    NamespacePath nsPath = NamespacePath.newBuilder().addAllSegments(namespacePath).build();
    SourceSelector source =
        SourceSelector.newBuilder().setNamespace(nsPath).setTable(tableName).build();
    var dest =
        DestinationTarget.newBuilder()
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setTableId(tableId)
            .setTableDisplayName(tableName)
            .build();

    String namespaceFq = String.join(".", namespacePath);
    String displayName =
        "register:" + prefix + (namespaceFq.isBlank() ? "" : ":" + namespaceFq) + "." + tableName;

    String connectorUri =
        (tableLocation != null && !tableLocation.isBlank()) ? tableLocation : metadataLocation;
    Map<String, String> props = new LinkedHashMap<>();
    props.put("external.metadata-location", metadataLocation);
    props.put("iceberg.source", "filesystem");
    props.put("external.table-name", tableName);
    props.put("external.namespace", namespaceFq);
    props.put(CONNECTOR_CAPTURE_STATS_PROPERTY, Boolean.toString(true));
    if (ioProperties != null && !ioProperties.isEmpty()) {
      ioProperties.forEach(
          (k, v) -> {
            if (FileIoFactory.isFileIoProperty(k) && isUsableIoValue(v)) {
              props.put(k, v.trim());
            }
          });
    }
    ConnectorSpec.Builder spec =
        ConnectorSpec.newBuilder()
            .setDisplayName(displayName)
            .setKind(ConnectorKind.CK_ICEBERG)
            .setUri(connectorUri)
            .setSource(source)
            .setDestination(dest)
            .setAuth(AuthConfig.newBuilder().setScheme("none").build())
            .putAllProperties(props);

    CreateConnectorRequest.Builder request =
        CreateConnectorRequest.newBuilder().setSpec(spec.build());
    if (idempotencyKey != null && !idempotencyKey.isBlank()) {
      request.setIdempotency(
          IdempotencyKey.newBuilder().setKey(idempotencyKey + ":connector").build());
    }
    CreateConnectorResponse response = connectorClient.createConnector(request.build());
    if (response == null || !response.hasConnector()) {
      LOG.warnf(
          "Connector service returned empty response for external connector %s.%s",
          prefix, tableName);
      return null;
    }
    return response.getConnector().getResourceId();
  }

  public Connector buildConnectorForTransactionCreate(
      String prefix,
      List<String> namespacePath,
      ResourceId namespaceId,
      ResourceId catalogId,
      String tableName,
      ResourceId tableId,
      ResourceId connectorId,
      String metadataLocation,
      String tableLocation,
      Map<String, String> ioProperties) {
    if (connectorId == null
        || connectorId.getId().isBlank()
        || tableId == null
        || tableId.getId().isBlank()
        || catalogId == null
        || catalogId.getId().isBlank()
        || namespaceId == null
        || namespaceId.getId().isBlank()
        || tableName == null
        || tableName.isBlank()) {
      return null;
    }
    NamespacePath nsPath = NamespacePath.newBuilder().addAllSegments(namespacePath).build();
    SourceSelector source =
        SourceSelector.newBuilder().setNamespace(nsPath).setTable(tableName).build();
    var dest =
        DestinationTarget.newBuilder()
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setTableId(tableId)
            .setTableDisplayName(tableName)
            .build();
    String namespaceFq = String.join(".", namespacePath);
    var now = Timestamps.fromMillis(System.currentTimeMillis());

    IcebergGatewayConfig.RegisterConnectorTemplate template = connectorTemplateFor(prefix);
    if (template != null && template.uri() != null && !template.uri().isBlank()) {
      String displayName =
          template
              .displayName()
              .orElseGet(
                  () ->
                      "register:"
                          + prefix
                          + (namespaceFq.isBlank() ? "" : ":" + namespaceFq)
                          + "."
                          + tableName);
      Map<String, String> props = new LinkedHashMap<>();
      if (template.properties() != null && !template.properties().isEmpty()) {
        props.putAll(template.properties());
      }
      props.put(CONNECTOR_CAPTURE_STATS_PROPERTY, Boolean.toString(template.captureStatistics()));
      Connector.Builder builder =
          Connector.newBuilder()
              .setResourceId(connectorId)
              .setDisplayName(displayName)
              .setKind(ConnectorKind.CK_ICEBERG)
              .setUri(template.uri())
              .setSource(source)
              .setDestination(dest)
              .setAuth(
                  template
                      .auth()
                      .map(this::toAuthConfig)
                      .orElse(AuthConfig.newBuilder().setScheme("none").build()))
              .putAllProperties(props)
              .setState(ConnectorState.CS_ACTIVE)
              .setCreatedAt(now)
              .setUpdatedAt(now);
      template.description().ifPresent(builder::setDescription);
      return builder.build();
    }

    String displayName =
        "register:" + prefix + (namespaceFq.isBlank() ? "" : ":" + namespaceFq) + "." + tableName;
    String connectorUri =
        (tableLocation != null && !tableLocation.isBlank()) ? tableLocation : metadataLocation;
    Map<String, String> props = new LinkedHashMap<>();
    props.put("external.metadata-location", metadataLocation);
    props.put("iceberg.source", "filesystem");
    props.put("external.table-name", tableName);
    props.put("external.namespace", namespaceFq);
    props.put(CONNECTOR_CAPTURE_STATS_PROPERTY, Boolean.toString(true));
    if (ioProperties != null && !ioProperties.isEmpty()) {
      ioProperties.forEach(
          (k, v) -> {
            if (FileIoFactory.isFileIoProperty(k) && isUsableIoValue(v)) {
              props.put(k, v.trim());
            }
          });
    }
    return Connector.newBuilder()
        .setResourceId(connectorId)
        .setDisplayName(displayName)
        .setKind(ConnectorKind.CK_ICEBERG)
        .setUri(connectorUri)
        .setSource(source)
        .setDestination(dest)
        .setAuth(AuthConfig.newBuilder().setScheme("none").build())
        .putAllProperties(props)
        .setState(ConnectorState.CS_ACTIVE)
        .setCreatedAt(now)
        .setUpdatedAt(now)
        .build();
  }

  public void updateTableUpstream(
      ResourceId tableId,
      List<String> namespacePath,
      String tableName,
      ResourceId connectorId,
      String connectorUri) {
    UpstreamRef.Builder upstream =
        UpstreamRef.newBuilder()
            .setFormat(TableFormat.TF_ICEBERG)
            .setColumnIdAlgorithm(ColumnIdAlgorithm.CID_FIELD_ID)
            .setConnectorId(connectorId)
            .addAllNamespacePath(namespacePath)
            .setTableDisplayName(tableName);
    if (connectorUri != null && !connectorUri.isBlank()) {
      upstream.setUri(connectorUri);
    }
    UpdateTableRequest request =
        UpdateTableRequest.newBuilder()
            .setTableId(tableId)
            .setSpec(TableSpec.newBuilder().setUpstream(upstream).build())
            .setUpdateMask(FieldMask.newBuilder().addPaths("upstream").build())
            .build();
    tableClient.updateTable(request);
  }

  public void runSyncMetadataCapture(
      ResourceId connectorId, List<String> namespacePath, String tableName) {
    runSyncCapture(connectorId, namespacePath, tableName, false, false);
  }

  public SyncCaptureResponse runSyncMetadataCaptureStrict(
      ResourceId connectorId, List<String> namespacePath, String tableName) {
    return runSyncCapture(connectorId, namespacePath, tableName, false, true);
  }

  public void runSyncStatisticsCapture(
      ResourceId connectorId, List<String> namespacePath, String tableName) {
    runSyncCapture(connectorId, namespacePath, tableName, true, false);
  }

  private SyncCaptureResponse runSyncCapture(
      ResourceId connectorId,
      List<String> namespacePath,
      String tableName,
      boolean includeStatistics,
      boolean strict) {
    if (connectorId == null || tableName == null || tableName.isBlank()) {
      return SyncCaptureResponse.getDefaultInstance();
    }
    String namespaceFq =
        namespacePath == null || namespacePath.isEmpty() ? "" : String.join(".", namespacePath);
    try {
      SyncCaptureRequest.Builder request =
          SyncCaptureRequest.newBuilder()
              .setConnectorId(connectorId)
              .setDestinationTableDisplayName(tableName)
              .setIncludeStatistics(includeStatistics);
      if (namespacePath != null && !namespacePath.isEmpty()) {
        request.addDestinationNamespacePaths(
            NamespacePath.newBuilder().addAllSegments(namespacePath).build());
      }
      var response = connectorClient.syncCapture(request.build());
      LOG.infof(
          "Triggered sync metadata capture connector=%s namespace=%s table=%s scanned=%d changed=%d"
              + " errors=%d",
          connectorId.getId(),
          namespaceFq,
          tableName,
          response.getTablesScanned(),
          response.getTablesChanged(),
          response.getErrors());
      if (strict && response.getErrors() > 0) {
        throw new RuntimeException(
            "sync capture reported errors="
                + response.getErrors()
                + " for connector="
                + connectorId.getId());
      }
      return response;
    } catch (Throwable e) {
      if (strict) {
        throw e instanceof RuntimeException runtime ? runtime : new RuntimeException(e);
      }
      LOG.warnf(
          e,
          "Sync metadata capture failed for connector %s table %s",
          connectorId.getId(),
          tableName);
      return SyncCaptureResponse.getDefaultInstance();
    }
  }

  public void triggerScopedReconcile(
      ResourceId connectorId, List<String> namespacePath, String tableName) {
    String namespaceFq =
        namespacePath == null || namespacePath.isEmpty() ? "" : String.join(".", namespacePath);
    try {
      TriggerReconcileRequest.Builder request =
          TriggerReconcileRequest.newBuilder()
              .setConnectorId(connectorId)
              .setFullRescan(false)
              .setDestinationTableDisplayName(tableName);
      if (namespacePath != null && !namespacePath.isEmpty()) {
        request.addDestinationNamespacePaths(
            NamespacePath.newBuilder().addAllSegments(namespacePath).build());
      }
      var response = connectorClient.triggerReconcile(request.build());
      LOG.infof(
          "Triggered reconcile job connector=%s namespace=%s table=%s jobId=%s",
          connectorId == null ? "<missing>" : connectorId.getId(),
          namespaceFq,
          tableName,
          response.getJobId());
    } catch (Throwable e) {
      LOG.warnf(
          e,
          "Reconcile trigger failed for connector %s table %s",
          connectorId == null ? "<missing>" : connectorId.getId(),
          tableName);
    }
  }

  private AuthConfig toAuthConfig(IcebergGatewayConfig.AuthTemplate template) {
    AuthConfig.Builder builder =
        AuthConfig.newBuilder()
            .setScheme(
                template.scheme() == null || template.scheme().isBlank()
                    ? "none"
                    : template.scheme())
            .putAllProperties(template.properties())
            .putAllHeaderHints(template.headerHints());
    return builder.build();
  }

  private Map<String, String> readPrefixedConfig(String prefix) {
    Map<String, String> out = new LinkedHashMap<>();
    for (String name : mpConfig.getPropertyNames()) {
      if (name.startsWith(prefix)) {
        mpConfig
            .getOptionalValue(name, String.class)
            .ifPresent(value -> out.put(name.substring(prefix.length()), value));
      }
    }
    return out;
  }

  private static Long propertyLong(Map<String, String> props, String key) {
    String value = props.get(key);
    if (value == null || value.isBlank()) {
      return null;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static boolean isUsableIoValue(String value) {
    if (value == null) {
      return false;
    }
    String trimmed = value.trim();
    if (trimmed.isEmpty()) {
      return false;
    }
    return !(trimmed.startsWith("<") && trimmed.endsWith(">"));
  }

  // Snapshot metadata parsing lives in SnapshotMetadataUtil.
}
