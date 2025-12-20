package ai.floedb.floecat.gateway.iceberg.rest.services.catalog;

import ai.floedb.floecat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.Snapshot;
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
import ai.floedb.floecat.connector.rpc.CreateConnectorRequest;
import ai.floedb.floecat.connector.rpc.CreateConnectorResponse;
import ai.floedb.floecat.connector.rpc.DeleteConnectorRequest;
import ai.floedb.floecat.connector.rpc.DestinationTarget;
import ai.floedb.floecat.connector.rpc.GetConnectorRequest;
import ai.floedb.floecat.connector.rpc.NamespacePath;
import ai.floedb.floecat.connector.rpc.SourceSelector;
import ai.floedb.floecat.connector.rpc.SyncCaptureRequest;
import ai.floedb.floecat.connector.rpc.TriggerReconcileRequest;
import ai.floedb.floecat.connector.rpc.UpdateConnectorRequest;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.StorageCredentialDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.common.MetadataLocationUtil;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.CatalogResolver;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.ConnectorClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.SnapshotClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.TableClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.FileIoFactory;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import ai.floedb.floecat.storage.spi.io.RuntimeFileIoOverrides;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.FieldMask;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.StatusRuntimeException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.eclipse.microprofile.config.Config;
import org.jboss.logging.Logger;

public class TableGatewaySupport {
  private static final Logger LOG = Logger.getLogger(TableGatewaySupport.class);
  private static final List<StorageCredentialDto> STATIC_STORAGE_CREDENTIALS =
      List.of(new StorageCredentialDto("*", Map.of("type", "static")));
  private static final String CONNECTOR_CAPTURE_STATS_PROPERTY =
      "floecat.connector.capture-statistics";
  private static final String ICEBERG_METADATA_KEY = "iceberg";

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
    String schemaJson = req.schemaJson();
    if ((schemaJson == null || schemaJson.isBlank())
        && req.schema() != null
        && !req.schema().isNull()) {
      schemaJson = mapper.writeValueAsString(req.schema());
    }
    if (schemaJson != null && !schemaJson.isBlank()) {
      spec.setSchemaJson(schemaJson);
    }
    if (req.location() != null && !req.location().isBlank()) {
      spec.putProperties("location", req.location());
      UpstreamRef.Builder upstream =
          spec.getUpstream().toBuilder().setFormat(TableFormat.TF_ICEBERG).setUri(req.location());
      spec.setUpstream(upstream.build());
    }
    if (req.properties() != null && !req.properties().isEmpty()) {
      spec.putAllProperties(sanitizeCreateProperties(req.properties()));
    }
    String metadataLocation = metadataLocationFromCreate(req);
    if ((metadataLocation == null || metadataLocation.isBlank())
        && req.location() != null
        && !req.location().isBlank()) {
      metadataLocation = metadataLocationFromTableLocation(req.location());
    }
    addMetadataLocationProperties(spec, metadataLocation);
    return spec;
  }

  public TableSpec.Builder baseTableSpec(
      ResourceId catalogId, ResourceId namespaceId, String tableName) {
    return TableSpec.newBuilder()
        .setCatalogId(catalogId)
        .setNamespaceId(namespaceId)
        .setDisplayName(tableName)
        .setUpstream(UpstreamRef.newBuilder().setFormat(TableFormat.TF_ICEBERG).build());
  }

  public void addMetadataLocationProperties(TableSpec.Builder spec, String metadataLocation) {
    MetadataLocationUtil.setMetadataLocation(spec::putProperties, metadataLocation);
  }

  public String metadataLocationFromCreate(TableRequests.Create req) {
    if (req == null || req.properties() == null || req.properties().isEmpty()) {
      return null;
    }
    String location = req.properties().get("metadata-location");
    if (location == null || location.isBlank()) {
      return null;
    }
    return location;
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
    return stripMetadataMirrorPrefix(base);
  }

  private String metadataLocationFromTableLocation(String tableLocation) {
    if (tableLocation == null || tableLocation.isBlank()) {
      return null;
    }
    String base =
        tableLocation.endsWith("/")
            ? tableLocation.substring(0, tableLocation.length() - 1)
            : tableLocation;
    String dir = base + "/metadata/";
    return dir + String.format("%05d-%s.metadata.json", 0, UUID.randomUUID());
  }

  public boolean connectorIntegrationEnabled() {
    return config.connectorIntegrationEnabled();
  }

  public void updateConnectorMetadata(ResourceId connectorId, String metadataLocation) {
    if (connectorId == null || metadataLocation == null || metadataLocation.isBlank()) {
      return;
    }
    try {
      var response =
          connectorClient.getConnector(
              GetConnectorRequest.newBuilder().setConnectorId(connectorId).build());
      if (response == null || !response.hasConnector()) {
        LOG.warnf("Connector lookup returned empty response for %s", connectorId.getId());
        return;
      }
      Connector existing = response.getConnector();
      Map<String, String> props = new LinkedHashMap<>(existing.getPropertiesMap());
      props.put("external.metadata-location", metadataLocation);
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
    Map<String, String> computed =
        new LinkedHashMap<>(readPrefixedConfig("floecat.gateway.table-config."));
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
      return parseSnapshotMetadata(snapshot);
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
    return parseSnapshotMetadata(snapshot);
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
    props.put("external.table-name", tableName);
    props.put("external.namespace", namespaceFq);
    props.put(CONNECTOR_CAPTURE_STATS_PROPERTY, Boolean.toString(true));
    RuntimeFileIoOverrides.mergeInto(props);
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

  public void updateTableUpstream(
      ResourceId tableId,
      List<String> namespacePath,
      String tableName,
      ResourceId connectorId,
      String connectorUri) {
    UpstreamRef.Builder upstream =
        UpstreamRef.newBuilder()
            .setFormat(TableFormat.TF_ICEBERG)
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
            .setUpdateMask(com.google.protobuf.FieldMask.newBuilder().addPaths("upstream").build())
            .build();
    tableClient.updateTable(request);
  }

  public void runSyncMetadataCapture(
      ResourceId connectorId, List<String> namespacePath, String tableName) {
    if (connectorId == null || tableName == null || tableName.isBlank()) {
      return;
    }
    String namespaceFq =
        namespacePath == null || namespacePath.isEmpty() ? "" : String.join(".", namespacePath);
    try {
      SyncCaptureRequest.Builder request =
          SyncCaptureRequest.newBuilder()
              .setConnectorId(connectorId)
              .setDestinationTableDisplayName(tableName)
              .setIncludeStatistics(false);
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
    } catch (Throwable e) {
      LOG.warnf(
          e,
          "Sync metadata capture failed for connector %s table %s",
          connectorId.getId(),
          tableName);
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
    template.secretRef().ifPresent(builder::setSecretRef);
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

  public boolean isMirrorMetadataLocation(String metadataLocation) {
    return MetadataLocationUtil.isMirrorMetadataLocation(metadataLocation);
  }

  public String stripMetadataMirrorPrefix(String location) {
    return MetadataLocationUtil.stripMetadataMirrorPrefix(location);
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

  private IcebergMetadata parseSnapshotMetadata(Snapshot snapshot) {
    if (snapshot == null) {
      return null;
    }
    ByteString raw = snapshot.getFormatMetadataOrDefault(ICEBERG_METADATA_KEY, ByteString.EMPTY);
    if (raw == null || raw.isEmpty()) {
      return null;
    }
    try {
      return IcebergMetadata.parseFrom(raw);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException(
          "Failed to parse Iceberg metadata for snapshot " + snapshot.getSnapshotId(), e);
    }
  }
}
