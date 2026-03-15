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

package ai.floedb.floecat.gateway.iceberg.minimal.services.transaction;

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.AuthConfig;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.connector.rpc.ConnectorState;
import ai.floedb.floecat.connector.rpc.DestinationTarget;
import ai.floedb.floecat.connector.rpc.GetConnectorRequest;
import ai.floedb.floecat.connector.rpc.NamespacePath;
import ai.floedb.floecat.connector.rpc.SourceSelector;
import ai.floedb.floecat.gateway.iceberg.minimal.config.MinimalGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.minimal.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.minimal.services.metadata.FileIoFactory;
import ai.floedb.floecat.storage.kv.Keys;
import ai.floedb.floecat.transaction.rpc.TxChange;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@ApplicationScoped
public class ConnectorProvisioningService {
  private final MinimalGatewayConfig config;
  private final GrpcWithHeaders grpc;

  @Inject
  public ConnectorProvisioningService(MinimalGatewayConfig config, GrpcWithHeaders grpc) {
    this.config = config;
    this.grpc = grpc;
  }

  public record ProvisionResult(
      Table table, ResourceId connectorId, List<TxChange> connectorTxChanges) {}

  public ProvisionResult resolveOrCreateForCommit(
      String accountId,
      String txId,
      List<String> namespacePath,
      ResourceId namespaceId,
      ResourceId catalogId,
      String tableName,
      ResourceId tableId,
      Table table) {
    if (table == null || tableId == null || tableName == null || tableName.isBlank()) {
      return new ProvisionResult(table, resolveConnectorId(table), List.of());
    }
    String resolvedTableLocation = tableLocation(table);
    String metadataLocation = tableMetadataLocation(table);
    ResourceId existingConnectorId = resolveConnectorId(table);
    if (existingConnectorId != null) {
      Connector existingConnector = loadExistingConnector(existingConnectorId);
      Connector refreshedConnector =
          refreshConnector(
              existingConnector,
              namespacePath,
              namespaceId,
              catalogId,
              tableName,
              tableId,
              metadataLocation,
              resolvedTableLocation);
      Table enriched =
          enrichTableUpstream(
              table, namespacePath, tableName, existingConnectorId, refreshedConnector.getUri());
      return new ProvisionResult(
          enriched, existingConnectorId, connectorUpsertChanges(accountId, refreshedConnector));
    }
    if ((resolvedTableLocation == null || resolvedTableLocation.isBlank())
        && (metadataLocation == null || metadataLocation.isBlank())) {
      return new ProvisionResult(table, null, List.of());
    }

    ResourceId connectorId = deterministicConnectorId(accountId, txId, tableId);
    Connector connector =
        buildConnector(
            accountId,
            namespacePath,
            namespaceId,
            catalogId,
            tableName,
            tableId,
            connectorId,
            metadataLocation,
            resolvedTableLocation,
            mergedFileIoProperties(table));
    Table enriched =
        enrichTableUpstream(table, namespacePath, tableName, connectorId, connector.getUri());
    return new ProvisionResult(enriched, connectorId, connectorUpsertChanges(accountId, connector));
  }

  public ResourceId resolveConnectorId(Table tableRecord) {
    if (tableRecord == null
        || !tableRecord.hasUpstream()
        || !tableRecord.getUpstream().hasConnectorId()) {
      return null;
    }
    ResourceId connectorId = tableRecord.getUpstream().getConnectorId();
    return connectorId == null || connectorId.getId().isBlank() ? null : connectorId;
  }

  private Connector buildConnector(
      String accountId,
      List<String> namespacePath,
      ResourceId namespaceId,
      ResourceId catalogId,
      String tableName,
      ResourceId tableId,
      ResourceId connectorId,
      String metadataLocation,
      String resolvedTableLocation,
      Map<String, String> ioProperties) {
    String namespaceFq = namespacePath == null ? "" : String.join(".", namespacePath);
    String connectorUri =
        resolvedTableLocation != null && !resolvedTableLocation.isBlank()
            ? resolvedTableLocation
            : metadataLocation;
    String displayName =
        "register:minimal" + (namespaceFq.isBlank() ? "" : ":" + namespaceFq) + "." + tableName;
    Timestamp nowTs = Timestamps.fromMillis(System.currentTimeMillis());

    Map<String, String> props = new LinkedHashMap<>();
    if (metadataLocation != null && !metadataLocation.isBlank()) {
      props.put("external.metadata-location", metadataLocation);
    }
    props.put("iceberg.source", "filesystem");
    props.put("external.table-name", tableName);
    props.put("external.namespace", namespaceFq);
    props.put("floecat.connector.capture-statistics", Boolean.toString(true));
    props.putAll(ioProperties);

    return Connector.newBuilder()
        .setResourceId(connectorId)
        .setKind(ConnectorKind.CK_ICEBERG)
        .setDisplayName(displayName)
        .setUri(connectorUri == null ? "" : connectorUri)
        .setState(ConnectorState.CS_ACTIVE)
        .setAuth(AuthConfig.newBuilder().setScheme("none").build())
        .setSource(
            SourceSelector.newBuilder()
                .setNamespace(NamespacePath.newBuilder().addAllSegments(namespacePath).build())
                .setTable(tableName)
                .build())
        .setDestination(
            DestinationTarget.newBuilder()
                .setCatalogId(catalogId)
                .setNamespaceId(namespaceId)
                .setTableId(tableId)
                .setTableDisplayName(tableName)
                .build())
        .setCreatedAt(nowTs)
        .setUpdatedAt(nowTs)
        .putAllProperties(props)
        .build();
  }

  private Connector refreshConnector(
      Connector existingConnector,
      List<String> namespacePath,
      ResourceId namespaceId,
      ResourceId catalogId,
      String tableName,
      ResourceId tableId,
      String metadataLocation,
      String resolvedTableLocation) {
    Timestamp nowTs = Timestamps.fromMillis(System.currentTimeMillis());
    Connector.Builder updated =
        existingConnector.toBuilder()
            .setUpdatedAt(nowTs)
            .setSource(
                SourceSelector.newBuilder()
                    .setNamespace(NamespacePath.newBuilder().addAllSegments(namespacePath).build())
                    .setTable(tableName)
                    .build())
            .setDestination(
                DestinationTarget.newBuilder()
                    .setCatalogId(catalogId)
                    .setNamespaceId(namespaceId)
                    .setTableId(tableId)
                    .setTableDisplayName(tableName)
                    .build());
    Map<String, String> nextProperties = new LinkedHashMap<>(existingConnector.getPropertiesMap());
    if (metadataLocation != null && !metadataLocation.isBlank()) {
      nextProperties.put("external.metadata-location", metadataLocation);
    }
    updated.clearProperties().putAllProperties(nextProperties);
    if (resolvedTableLocation != null && !resolvedTableLocation.isBlank()) {
      updated.setUri(resolvedTableLocation);
    }
    return updated.build();
  }

  private Connector loadExistingConnector(ResourceId connectorId) {
    try {
      var connectorStub = grpc.withHeaders(grpc.raw().connector());
      var response =
          connectorStub.getConnector(
              GetConnectorRequest.newBuilder().setConnectorId(connectorId).build());
      if (response == null || !response.hasConnector()) {
        throw Status.NOT_FOUND
            .withDescription("connector not found: " + connectorId.getId())
            .asRuntimeException();
      }
      return response.getConnector();
    } catch (StatusRuntimeException e) {
      throw e;
    } catch (RuntimeException e) {
      throw Status.UNAVAILABLE
          .withDescription("connector provisioning failed: " + e.getMessage())
          .withCause(e)
          .asRuntimeException();
    }
  }

  private Table enrichTableUpstream(
      Table table,
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

  private List<TxChange> connectorUpsertChanges(String accountId, Connector connector) {
    if (connector == null || !connector.hasResourceId() || connector.getDisplayName().isBlank()) {
      return List.of();
    }
    ByteString payload = ByteString.copyFrom(connector.toByteArray());
    return List.of(
        TxChange.newBuilder()
            .setTargetPointerKey(
                Keys.connectorPointerById(accountId, connector.getResourceId().getId()))
            .setPayload(payload)
            .build(),
        TxChange.newBuilder()
            .setTargetPointerKey(Keys.connectorPointerByName(accountId, connector.getDisplayName()))
            .setPayload(payload)
            .build());
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

  private Map<String, String> mergedFileIoProperties(Table table) {
    Map<String, String> ioProperties =
        new LinkedHashMap<>(
            FileIoFactory.filterIoProperties(table == null ? Map.of() : table.getPropertiesMap()));
    config.metadataFileIo().ifPresent(ioImpl -> ioProperties.put("io-impl", ioImpl));
    config.metadataFileIoRoot().ifPresent(root -> ioProperties.put("fs.floecat.test-root", root));
    config.metadataS3Endpoint().ifPresent(endpoint -> ioProperties.put("s3.endpoint", endpoint));
    ioProperties.put("s3.path-style-access", Boolean.toString(config.metadataS3PathStyleAccess()));
    config.metadataS3Region().ifPresent(region -> ioProperties.put("s3.region", region));
    config.metadataClientRegion().ifPresent(region -> ioProperties.put("client.region", region));
    config.metadataS3AccessKeyId().ifPresent(key -> ioProperties.put("s3.access-key-id", key));
    config
        .metadataS3SecretAccessKey()
        .ifPresent(secret -> ioProperties.put("s3.secret-access-key", secret));
    return ioProperties.isEmpty() ? Map.of() : Map.copyOf(ioProperties);
  }

  private String tableMetadataLocation(Table table) {
    return table == null ? null : table.getPropertiesMap().get("metadata-location");
  }

  private String tableLocation(Table table) {
    if (table == null) {
      return null;
    }
    String location = table.getPropertiesMap().get("location");
    if (location != null && !location.isBlank()) {
      return location;
    }
    String storageLocation = table.getPropertiesMap().get("storage_location");
    return storageLocation == null || storageLocation.isBlank() ? null : storageLocation;
  }
}
