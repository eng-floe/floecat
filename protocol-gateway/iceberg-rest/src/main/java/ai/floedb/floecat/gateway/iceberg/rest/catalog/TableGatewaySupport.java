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

package ai.floedb.floecat.gateway.iceberg.rest.catalog;

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.DeleteTableRequest;
import ai.floedb.floecat.catalog.rpc.GetNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.GetTableRequest;
import ai.floedb.floecat.catalog.rpc.GetTableResponse;
import ai.floedb.floecat.catalog.rpc.ListTablesRequest;
import ai.floedb.floecat.catalog.rpc.ListTablesResponse;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.catalog.rpc.UpdateTableRequest;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.DeleteConnectorRequest;
import ai.floedb.floecat.connector.rpc.GetConnectorRequest;
import ai.floedb.floecat.connector.rpc.GetConnectorResponse;
import ai.floedb.floecat.connector.rpc.NamespacePath;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.StorageCredentialDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TableIdentifierDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.support.FileIoFactory;
import ai.floedb.floecat.gateway.iceberg.rest.support.GrpcServiceFacade;
import ai.floedb.floecat.gateway.iceberg.rest.support.MetadataLocationUtil;
import ai.floedb.floecat.gateway.iceberg.rest.support.SnapshotMetadataUtil;
import ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import ai.floedb.floecat.reconciler.rpc.CaptureMode;
import ai.floedb.floecat.reconciler.rpc.CaptureNowRequest;
import ai.floedb.floecat.reconciler.rpc.CaptureNowResponse;
import ai.floedb.floecat.reconciler.rpc.CaptureScope;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
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

  @Inject
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

  public record ListTablesResult(List<TableIdentifierDto> identifiers, String nextPageToken) {}

  public ListTablesResult listTables(
      String catalogName, String namespace, Integer pageSize, String pageToken) {
    List<String> namespacePath = NamespacePaths.split(namespace);
    ResourceId namespaceId = resolveNamespaceId(catalogName, namespacePath);

    ListTablesRequest.Builder request = ListTablesRequest.newBuilder().setNamespaceId(namespaceId);
    if (pageToken != null || pageSize != null) {
      PageRequest.Builder page = PageRequest.newBuilder();
      if (pageToken != null) {
        page.setPageToken(pageToken);
      }
      if (pageSize != null) {
        page.setPageSize(pageSize);
      }
      request.setPage(page);
    }

    ListTablesResponse response = grpcClient.listTables(request.build());
    if (response == null) {
      response = ListTablesResponse.getDefaultInstance();
    }
    List<TableIdentifierDto> identifiers =
        response.getTablesList().stream()
            .map(table -> new TableIdentifierDto(namespacePath, table.getDisplayName()))
            .collect(Collectors.toList());
    String nextToken = null;
    if (response.hasPage()) {
      String token = response.getPage().getNextPageToken();
      if (token != null && !token.isBlank()) {
        nextToken = token;
      }
    }
    return new ListTablesResult(identifiers, nextToken);
  }

  public ResourceId resolveNamespaceId(String catalogName, String namespace) {
    return resolveNamespaceId(catalogName, NamespacePaths.split(namespace));
  }

  public ResourceId resolveNamespaceId(String catalogName, List<String> namespacePath) {
    return NameResolution.resolveNamespace(grpc, catalogName, namespacePath);
  }

  public ResourceId resolveTableId(String catalogName, String namespace, String tableName) {
    return resolveTableId(catalogName, NamespacePaths.split(namespace), tableName);
  }

  public ResourceId resolveTableId(
      String catalogName, List<String> namespacePath, String tableName) {
    return NameResolution.resolveTable(grpc, catalogName, namespacePath, tableName);
  }

  public Table getTable(ResourceId tableId) {
    return getTableResponse(tableId).getTable();
  }

  public GetTableResponse getTableResponse(ResourceId tableId) {
    return grpcClient.getTable(GetTableRequest.newBuilder().setTableId(tableId).build());
  }

  public Table updateTable(UpdateTableRequest request) {
    return grpcClient.updateTable(request).getTable();
  }

  public void deleteTable(ResourceId tableId) {
    deleteTable(tableId, false);
  }

  public void deleteTable(ResourceId tableId, boolean purgeRequested) {
    if (tableId == null) {
      return;
    }
    grpcClient.deleteTable(DeleteTableRequest.newBuilder().setTableId(tableId).build());
  }

  public String defaultTableLocation(NamespaceRef namespaceContext, String tableName) {
    if (namespaceContext == null || tableName == null || tableName.isBlank()) {
      return null;
    }
    String namespaceLocation = namespaceLocation(namespaceContext);
    if (namespaceLocation != null && !namespaceLocation.isBlank()) {
      return joinLocation(namespaceLocation, List.of(tableName));
    }
    String warehouse = config.defaultWarehousePath().orElse(null);
    if (warehouse == null || warehouse.isBlank()) {
      return null;
    }
    return joinLocation(warehouse, joinNamespaceParts(namespaceContext.namespacePath(), tableName));
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

  public void runSyncStatisticsCapture(
      ResourceId connectorId, List<String> namespacePath, String tableName) {
    runSyncStatisticsCapture(connectorId, namespacePath, tableName, List.of());
  }

  public void runSyncStatisticsCapture(
      ResourceId connectorId,
      List<String> namespacePath,
      String tableName,
      List<Long> snapshotIds) {
    runSyncStatisticsCapture(connectorId, namespacePath, tableName, snapshotIds, false);
  }

  public void runSyncStatisticsCapture(
      ResourceId connectorId,
      List<String> namespacePath,
      String tableName,
      List<Long> snapshotIds,
      boolean fullRescan) {
    runSyncCapture(
        connectorId, namespacePath, tableName, snapshotIds, CaptureMode.CM_STATS_ONLY, fullRescan);
  }

  private CaptureNowResponse runSyncCapture(
      ResourceId connectorId,
      List<String> namespacePath,
      String tableName,
      List<Long> snapshotIds,
      CaptureMode mode,
      boolean fullRescan) {
    if (connectorId == null || tableName == null || tableName.isBlank()) {
      return CaptureNowResponse.getDefaultInstance();
    }
    String namespaceFq =
        namespacePath == null || namespacePath.isEmpty() ? "" : String.join(".", namespacePath);
    try {
      CaptureNowRequest.Builder request =
          CaptureNowRequest.newBuilder()
              .setScope(captureScope(connectorId, namespacePath, tableName, snapshotIds))
              .setMode(mode)
              .setFullRescan(fullRescan);
      var response = grpcClient.captureNow(request.build());
      LOG.infof(
          "Triggered sync statistics capture connector=%s namespace=%s table=%s scanned=%d changed=%d"
              + " errors=%d",
          connectorId.getId(),
          namespaceFq,
          tableName,
          response.getTablesScanned(),
          response.getTablesChanged(),
          response.getErrors());
      return response;
    } catch (Throwable e) {
      LOG.warnf(
          e,
          "Sync statistics capture failed for connector %s table %s",
          connectorId.getId(),
          tableName);
      return CaptureNowResponse.getDefaultInstance();
    }
  }

  private static CaptureScope captureScope(
      ResourceId connectorId,
      List<String> namespacePath,
      String tableName,
      List<Long> snapshotIds) {
    CaptureScope.Builder builder =
        CaptureScope.newBuilder()
            .setConnectorId(connectorId == null ? ResourceId.getDefaultInstance() : connectorId)
            .setDestinationTableDisplayName(tableName == null ? "" : tableName);
    if (namespacePath != null && !namespacePath.isEmpty()) {
      builder.addDestinationNamespacePaths(
          NamespacePath.newBuilder().addAllSegments(namespacePath).build());
    }
    if (snapshotIds != null && !snapshotIds.isEmpty()) {
      builder.addAllDestinationSnapshotIds(snapshotIds);
    }
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

  private String namespaceLocation(NamespaceRef namespaceContext) {
    if (namespaceContext == null || namespaceContext.namespaceId() == null) {
      return null;
    }
    try {
      Namespace namespace =
          grpcClient
              .getNamespace(
                  GetNamespaceRequest.newBuilder()
                      .setNamespaceId(namespaceContext.namespaceId())
                      .build())
              .getNamespace();
      if (namespace == null) {
        return null;
      }
      Map<String, String> props = namespace.getPropertiesMap();
      if (props == null || props.isEmpty()) {
        return null;
      }
      return TableMappingUtil.firstNonBlank(props.get("location"), props.get("warehouse"));
    } catch (Exception e) {
      LOG.debugf(
          e, "Failed to resolve namespace location for %s", namespaceContext.namespacePath());
      return null;
    }
  }

  private List<String> joinNamespaceParts(List<String> namespacePath, String tableName) {
    List<String> parts = namespacePath == null ? new ArrayList<>() : new ArrayList<>(namespacePath);
    if (tableName != null && !tableName.isBlank()) {
      parts.add(tableName);
    }
    return parts;
  }

  private String joinLocation(String base, List<String> parts) {
    if (base == null || base.isBlank()) {
      return base;
    }
    String normalized = base;
    while (normalized.endsWith("/") && normalized.length() > 1) {
      normalized = normalized.substring(0, normalized.length() - 1);
    }
    String suffix =
        parts == null
            ? ""
            : parts.stream()
                .filter(part -> part != null && !part.isBlank())
                .reduce("", (left, right) -> left.isEmpty() ? right : left + "/" + right);
    if (suffix.isBlank()) {
      return normalized;
    }
    return normalized + "/" + suffix;
  }

  // Snapshot metadata parsing lives in SnapshotMetadataUtil.
}
