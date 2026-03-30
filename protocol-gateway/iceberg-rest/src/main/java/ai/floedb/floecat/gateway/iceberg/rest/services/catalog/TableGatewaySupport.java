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
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.DeleteConnectorRequest;
import ai.floedb.floecat.connector.rpc.GetConnectorRequest;
import ai.floedb.floecat.connector.rpc.GetConnectorResponse;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.StorageCredentialDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.common.MetadataLocationUtil;
import ai.floedb.floecat.gateway.iceberg.rest.common.SnapshotMetadataUtil;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.CatalogResolver;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.GrpcServiceFacade;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.FileIoFactory;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.eclipse.microprofile.config.Config;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TableGatewaySupport {
  private static final Logger LOG = Logger.getLogger(TableGatewaySupport.class);
  private static final List<StorageCredentialDto> STATIC_STORAGE_CREDENTIALS =
      List.of(new StorageCredentialDto("*", Map.of("type", "static")));

  private final GrpcWithHeaders grpc;
  private final IcebergGatewayConfig config;
  private final ObjectMapper mapper;
  private final Config mpConfig;
  private final GrpcServiceFacade grpcClient;

  private volatile Map<String, String> tableConfigCache;
  private volatile List<StorageCredentialDto> storageCredentialCache;

  public TableGatewaySupport(
      GrpcWithHeaders grpc,
      IcebergGatewayConfig config,
      ObjectMapper mapper,
      Config mpConfig,
      GrpcServiceFacade grpcClient) {
    this.grpc = grpc;
    this.config = config;
    this.mapper = mapper;
    this.mpConfig = mpConfig;
    this.grpcClient = grpcClient;
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

  public Optional<String> selfUri() {
    return config.selfUri().filter(uri -> uri != null && !uri.isBlank()).map(String::trim);
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
      var response = grpcClient.getSnapshot(requestBuilder.build());
      if (response == null || !response.hasSnapshot()) {
        return null;
      }
      var snapshot = response.getSnapshot();
      IcebergMetadata parsed = SnapshotMetadataUtil.parseSnapshotMetadata(snapshot);
      Long propertySnapshotId = propertyLong(table.getPropertiesMap(), "current-snapshot-id");
      Long resolvedSnapshotId = null;
      if (snapshot.getSnapshotId() > 0) {
        resolvedSnapshotId = snapshot.getSnapshotId();
      } else if (parsed != null && parsed.getCurrentSnapshotId() > 0) {
        resolvedSnapshotId = parsed.getCurrentSnapshotId();
      }
      if (propertySnapshotId != null
          && propertySnapshotId > 0
          && resolvedSnapshotId != null
          && !propertySnapshotId.equals(resolvedSnapshotId)) {
        return loadSnapshotById(table.getResourceId(), propertySnapshotId);
      }
      return parsed;
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
        grpcClient.getSnapshot(
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

  public Optional<Connector> getConnector(ResourceId connectorId) {
    if (connectorId == null || connectorId.getId().isBlank()) {
      return Optional.empty();
    }
    try {
      GetConnectorResponse response =
          grpcClient.getConnector(
              GetConnectorRequest.newBuilder().setConnectorId(connectorId).build());
      if (response == null || !response.hasConnector()) {
        return Optional.empty();
      }
      return Optional.of(response.getConnector());
    } catch (StatusRuntimeException e) {
      return Optional.empty();
    }
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
