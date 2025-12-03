package ai.floedb.metacat.gateway.iceberg.rest;

import ai.floedb.metacat.catalog.rpc.IcebergMetadata;
import ai.floedb.metacat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.catalog.rpc.TableFormat;
import ai.floedb.metacat.catalog.rpc.TableServiceGrpc;
import ai.floedb.metacat.catalog.rpc.TableSpec;
import ai.floedb.metacat.catalog.rpc.UpstreamRef;
import ai.floedb.metacat.common.rpc.IdempotencyKey;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.SnapshotRef;
import ai.floedb.metacat.common.rpc.SpecialSnapshot;
import ai.floedb.metacat.connector.rpc.AuthConfig;
import ai.floedb.metacat.connector.rpc.ConnectorKind;
import ai.floedb.metacat.connector.rpc.ConnectorSpec;
import ai.floedb.metacat.connector.rpc.ConnectorsGrpc;
import ai.floedb.metacat.connector.rpc.CreateConnectorRequest;
import ai.floedb.metacat.connector.rpc.CreateConnectorResponse;
import ai.floedb.metacat.connector.rpc.NamespacePath;
import ai.floedb.metacat.connector.rpc.SourceSelector;
import ai.floedb.metacat.connector.rpc.SyncCaptureRequest;
import ai.floedb.metacat.connector.rpc.TriggerReconcileRequest;
import ai.floedb.metacat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.metacat.gateway.iceberg.grpc.GrpcWithHeaders;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.StatusRuntimeException;
import org.eclipse.microprofile.config.Config;
import org.jboss.logging.Logger;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Shared helpers for translating Iceberg REST requests into Metacat RPCs. */
public class TableGatewaySupport {
  private static final Logger LOG = Logger.getLogger(TableGatewaySupport.class);
  private static final List<StorageCredentialDto> STATIC_STORAGE_CREDENTIALS =
      List.of(new StorageCredentialDto("*", Map.of("type", "static")));

  private final GrpcWithHeaders grpc;
  private final IcebergGatewayConfig config;
  private final ObjectMapper mapper;
  private final Config mpConfig;

  private volatile Map<String, String> tableConfigCache;
  private volatile List<StorageCredentialDto> storageCredentialCache;

  public TableGatewaySupport(
      GrpcWithHeaders grpc,
      IcebergGatewayConfig config,
      ObjectMapper mapper,
      Config mpConfig) {
    this.grpc = grpc;
    this.config = config;
    this.mapper = mapper;
    this.mpConfig = mpConfig;
  }

  public TableSpec.Builder buildCreateSpec(
      ResourceId catalogId,
      ResourceId namespaceId,
      String tableName,
      TableRequests.Create req)
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
      spec.putAllProperties(req.properties());
    }
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
    if (metadataLocation == null || metadataLocation.isBlank()) {
      return;
    }
    spec.putProperties("metadata-location", metadataLocation);
    spec.putProperties("metadata_location", metadataLocation);
  }

  public String metadataLocationFromCreate(TableRequests.Create req) {
    if (req == null || req.properties() == null || req.properties().isEmpty()) {
      return null;
    }
    String location = req.properties().get("metadata-location");
    if (location == null || location.isBlank()) {
      location = req.properties().get("metadata_location");
    }
    return (location == null || location.isBlank()) ? null : location;
  }

  public Map<String, String> defaultTableConfig() {
    Map<String, String> cached = tableConfigCache;
    if (cached != null) {
      return cached;
    }
    Map<String, String> computed = readPrefixedConfig("metacat.gateway.table-config.");
    if (computed.isEmpty()) {
      computed = Map.of();
    } else {
      computed = Map.copyOf(computed);
    }
    tableConfigCache = computed;
    return computed;
  }

  public List<StorageCredentialDto> defaultCredentials() {
    List<StorageCredentialDto> cached = storageCredentialCache;
    if (cached != null) {
      return cached;
    }
    Map<String, String> props =
        readPrefixedConfig("metacat.gateway.storage-credential.properties.");
    if (props.isEmpty()) {
      storageCredentialCache = STATIC_STORAGE_CREDENTIALS;
      return STATIC_STORAGE_CREDENTIALS;
    }
    String scope =
        mpConfig
            .getOptionalValue("metacat.gateway.storage-credential.scope", String.class)
            .orElse("*");
    List<StorageCredentialDto> computed =
        List.of(new StorageCredentialDto(scope, Map.copyOf(props)));
    storageCredentialCache = computed;
    return computed;
  }

  public IcebergMetadata loadCurrentMetadata(Table table) {
    if (table == null || !table.hasResourceId()) {
      return null;
    }
    Long snapshotId = propertyLong(table.getPropertiesMap(), "current-snapshot-id");
    SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshotStub =
        grpc.withHeaders(grpc.raw().snapshot());
    try {
      SnapshotRef.Builder ref = SnapshotRef.newBuilder();
      if (snapshotId != null && snapshotId > 0) {
        ref.setSnapshotId(snapshotId);
      } else {
        ref.setSpecial(SpecialSnapshot.SS_CURRENT);
      }
      var response =
          snapshotStub.getSnapshot(
              ai.floedb.metacat.catalog.rpc.GetSnapshotRequest.newBuilder()
                  .setTableId(table.getResourceId())
                  .setSnapshot(ref)
                  .build());
      if (response == null || !response.hasSnapshot()) {
        return null;
      }
      var snapshot = response.getSnapshot();
      return snapshot.hasIceberg() ? snapshot.getIceberg() : null;
    } catch (StatusRuntimeException ignored) {
      return null;
    }
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
    String resolved = resolveCatalog(prefix);
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
    ConnectorsGrpc.ConnectorsBlockingStub stub = grpc.withHeaders(grpc.raw().connectors());
    NamespacePath nsPath = NamespacePath.newBuilder().addAllSegments(namespacePath).build();
    SourceSelector source =
        SourceSelector.newBuilder().setNamespace(nsPath).setTable(tableName).build();
    var dest =
        ai.floedb.metacat.connector.rpc.DestinationTarget.newBuilder()
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
    template.description().ifPresent(spec::setDescription);

    CreateConnectorRequest.Builder request =
        CreateConnectorRequest.newBuilder().setSpec(spec.build());
    if (idempotencyKey != null && !idempotencyKey.isBlank()) {
      request.setIdempotency(
          IdempotencyKey.newBuilder().setKey(idempotencyKey + ":connector").build());
    }
    CreateConnectorResponse response = stub.createConnector(request.build());
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
      String idempotencyKey) {
    ConnectorsGrpc.ConnectorsBlockingStub stub = grpc.withHeaders(grpc.raw().connectors());
    NamespacePath nsPath = NamespacePath.newBuilder().addAllSegments(namespacePath).build();
    SourceSelector source =
        SourceSelector.newBuilder().setNamespace(nsPath).setTable(tableName).build();
    var dest =
        ai.floedb.metacat.connector.rpc.DestinationTarget.newBuilder()
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setTableId(tableId)
            .setTableDisplayName(tableName)
            .build();

    String namespaceFq = String.join(".", namespacePath);
    String displayName =
        "register:" + prefix + (namespaceFq.isBlank() ? "" : ":" + namespaceFq) + "." + tableName;

    ConnectorSpec.Builder spec =
        ConnectorSpec.newBuilder()
            .setDisplayName(displayName)
            .setKind(ConnectorKind.CK_ICEBERG)
            .setUri(metadataLocation)
            .setSource(source)
            .setDestination(dest)
            .setAuth(AuthConfig.newBuilder().setScheme("none").build())
            .putProperties("external.metadata-location", metadataLocation)
            .putProperties("external.table-name", tableName)
            .putProperties("external.namespace", namespaceFq);

    CreateConnectorRequest.Builder request =
        CreateConnectorRequest.newBuilder().setSpec(spec.build());
    if (idempotencyKey != null && !idempotencyKey.isBlank()) {
      request.setIdempotency(
          IdempotencyKey.newBuilder().setKey(idempotencyKey + ":connector").build());
    }
    return stub.createConnector(request.build()).getConnector().getResourceId();
  }

  public void updateTableUpstream(
      ResourceId tableId,
      List<String> namespacePath,
      String tableName,
      ResourceId connectorId,
      String connectorUri) {
    TableServiceGrpc.TableServiceBlockingStub tableStub = grpc.withHeaders(grpc.raw().table());
    UpstreamRef.Builder upstream =
        UpstreamRef.newBuilder()
            .setFormat(TableFormat.TF_ICEBERG)
            .setConnectorId(connectorId)
            .addAllNamespacePath(namespacePath)
            .setTableDisplayName(tableName);
    if (connectorUri != null && !connectorUri.isBlank()) {
      upstream.setUri(connectorUri);
    }
    ai.floedb.metacat.catalog.rpc.UpdateTableRequest request =
        ai.floedb.metacat.catalog.rpc.UpdateTableRequest.newBuilder()
            .setTableId(tableId)
            .setSpec(TableSpec.newBuilder().setUpstream(upstream).build())
            .setUpdateMask(com.google.protobuf.FieldMask.newBuilder().addPaths("upstream").build())
            .build();
    tableStub.updateTable(request);
  }

  public void runSyncMetadataCapture(ResourceId connectorId, List<String> namespacePath, String tableName) {
    if (connectorId == null || tableName == null || tableName.isBlank()) {
      return;
    }
    try {
      ConnectorsGrpc.ConnectorsBlockingStub stub = grpc.withHeaders(grpc.raw().connectors());
      SyncCaptureRequest.Builder request =
          SyncCaptureRequest.newBuilder()
              .setConnectorId(connectorId)
              .setDestinationTableDisplayName(tableName)
              .setIncludeStatistics(false);
      if (namespacePath != null && !namespacePath.isEmpty()) {
        request.addDestinationNamespacePaths(
            NamespacePath.newBuilder().addAllSegments(namespacePath).build());
      }
      stub.syncCapture(request.build());
    } catch (StatusRuntimeException e) {
      LOG.warnf(
          e,
          "Sync metadata capture failed for connector %s table %s",
          connectorId.getId(),
          tableName);
    }
  }

  public void triggerScopedReconcile(
      ResourceId connectorId, List<String> namespacePath, String tableName) {
    ConnectorsGrpc.ConnectorsBlockingStub stub = grpc.withHeaders(grpc.raw().connectors());
    TriggerReconcileRequest.Builder request =
        TriggerReconcileRequest.newBuilder()
            .setConnectorId(connectorId)
            .setFullRescan(false)
            .setDestinationTableDisplayName(tableName);
    if (namespacePath != null && !namespacePath.isEmpty()) {
      request.addDestinationNamespacePaths(
          NamespacePath.newBuilder().addAllSegments(namespacePath).build());
    }
    stub.triggerReconcile(request.build());
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

  private String resolveCatalog(String prefix) {
    Map<String, String> mapping = config.catalogMapping();
    return Optional.ofNullable(mapping == null ? null : mapping.get(prefix)).orElse(prefix);
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
}
