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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.GetSnapshotResponse;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.StorageCredentialDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.config.ConnectorIntegrationConfig;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.GrpcServiceFacade;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Status;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class TableGatewaySupportTest {
  private final GrpcWithHeaders grpc = mock(GrpcWithHeaders.class);
  private final IcebergGatewayConfig gatewayConfig = mock(IcebergGatewayConfig.class);
  private final ConnectorIntegrationConfig connectorConfig = mock(ConnectorIntegrationConfig.class);
  private final GrpcServiceFacade grpcClient = mock(GrpcServiceFacade.class);
  private final ObjectMapper mapper = new ObjectMapper();

  private TableGatewaySupport support;

  @BeforeEach
  void setUp() {
    when(connectorConfig.metadataFileIo()).thenReturn(Optional.empty());
    when(connectorConfig.metadataFileIoRoot()).thenReturn(Optional.empty());
    when(connectorConfig.storageCredential()).thenReturn(Optional.empty());
    when(connectorConfig.defaultRegion()).thenReturn(Optional.empty());
    when(connectorConfig.registerConnectors()).thenReturn(Map.of());
    when(connectorConfig.enabled()).thenReturn(true);
    when(gatewayConfig.catalogMapping()).thenReturn(Map.of());

    support = new TableGatewaySupport(grpc, gatewayConfig, connectorConfig, mapper, grpcClient);
  }

  @Test
  void buildCreateSpecSanitizesPropertiesAndSetsMetadataLocation() throws Exception {
    TableRequests.Create req =
        new TableRequests.Create(
            "orders",
            mapper.readTree("{\"type\":\"struct\"}"),
            "s3://bucket/warehouse/orders",
            Map.of(
                "metadata-location",
                "s3://bucket/warehouse/orders/metadata/v1.metadata.json",
                "owner",
                "analytics",
                "s3.secret-key",
                "hidden",
                "fs.floecat.test-root",
                "/tmp/root"),
            null,
            null,
            false);

    var spec =
        support
            .buildCreateSpec(
                ResourceId.newBuilder().setId("cat").build(),
                ResourceId.newBuilder().setId("cat:db").build(),
                "orders",
                req)
            .build();

    assertEquals("orders", spec.getDisplayName());
    assertEquals("s3://bucket/warehouse/orders", spec.getUpstream().getUri());
    assertNotNull(spec.getSchemaJson());
    assertEquals("analytics", spec.getPropertiesOrThrow("owner"));
    assertEquals(
        "s3://bucket/warehouse/orders/metadata/v1.metadata.json",
        spec.getPropertiesOrThrow("metadata-location"));
    assertFalse(spec.getPropertiesMap().containsKey("s3.secret-key"));
    assertFalse(spec.getPropertiesMap().containsKey("fs.floecat.test-root"));
  }

  @Test
  void resolveTableLocationUsesRequestedThenMetadataFallback() {
    assertEquals(
        "s3://explicit/table",
        support.resolveTableLocation("s3://explicit/table", "s3://bucket/ns/table/metadata/v1"));
    assertEquals(
        "s3://bucket/ns/table",
        support.resolveTableLocation(null, "s3://bucket/ns/table/metadata/v1.metadata.json"));
    assertEquals("s3://bucket/ns", support.resolveTableLocation(null, "s3://bucket/ns/file.json"));
    assertNull(support.resolveTableLocation(null, " "));
  }

  @Test
  void defaultTableConfigFiltersSecretsWithoutLeakingSecrets() {
    ConnectorIntegrationConfig.StorageCredentialConfig storage =
        mock(ConnectorIntegrationConfig.StorageCredentialConfig.class);
    when(storage.properties())
        .thenReturn(
            Map.of(
                "s3.endpoint", " http://localhost:4566 ",
                "s3.path-style-access", "true",
                "s3.secret-key", "secret",
                "region", "us-west-2"));
    when(connectorConfig.storageCredential()).thenReturn(Optional.of(storage));
    when(connectorConfig.metadataFileIo()).thenReturn(Optional.of("io.impl.Custom"));
    when(connectorConfig.metadataFileIoRoot()).thenReturn(Optional.of("/warehouse/root"));
    when(connectorConfig.defaultRegion()).thenReturn(Optional.of("us-east-1"));

    Map<String, String> configMap = support.defaultTableConfig();

    assertEquals("io.impl.Custom", configMap.get("io-impl"));
    assertEquals("/warehouse/root", configMap.get("fs.floecat.test-root"));
    assertEquals("http://localhost:4566", configMap.get("s3.endpoint"));
    assertEquals("true", configMap.get("s3.path-style-access"));
    assertEquals("us-west-2", configMap.get("region"));
    assertEquals("us-east-1", configMap.get("s3.region"));
    assertEquals("us-east-1", configMap.get("client.region"));
    assertFalse(configMap.containsKey("s3.secret-key"));
    assertFalse(configMap.containsKey("s3.access-key-id"));
  }

  @Test
  void defaultCredentialsReturnsStaticWhenNoCredentialsConfigured() {
    List<StorageCredentialDto> credentials = support.defaultCredentials();

    assertEquals(1, credentials.size());
    assertEquals("*", credentials.get(0).prefix());
    assertEquals(Map.of("type", "static"), credentials.get(0).config());
  }

  @Test
  void credentialsForAccessDelegationRequiresAndReturnsConfiguredCredentials() {
    ConnectorIntegrationConfig.StorageCredentialConfig storage =
        mock(ConnectorIntegrationConfig.StorageCredentialConfig.class);
    when(storage.properties()).thenReturn(Map.of("s3.endpoint", "http://localhost:4566"));
    when(storage.scope()).thenReturn(Optional.of("tenant/*"));
    when(connectorConfig.storageCredential()).thenReturn(Optional.of(storage));

    IllegalArgumentException unsupported =
        assertThrows(
            IllegalArgumentException.class, () -> support.credentialsForAccessDelegation("sigv4"));
    assertEquals("Unsupported access delegation mode: sigv4", unsupported.getMessage());

    List<StorageCredentialDto> credentials =
        support.credentialsForAccessDelegation("vended-credentials");
    assertNotNull(credentials);
    assertEquals("tenant/*", credentials.get(0).prefix());
    assertEquals("http://localhost:4566", credentials.get(0).config().get("s3.endpoint"));
  }

  @Test
  void resolveRegisterFileIoPropertiesMergesDefaultsAndRequestOverrides() {
    ConnectorIntegrationConfig.StorageCredentialConfig storage =
        mock(ConnectorIntegrationConfig.StorageCredentialConfig.class);
    when(storage.properties())
        .thenReturn(
            Map.of(
                "s3.endpoint", "http://localhost:4566",
                "not-io", "ignored"));
    when(connectorConfig.storageCredential()).thenReturn(Optional.of(storage));
    when(connectorConfig.metadataFileIo())
        .thenReturn(Optional.of("org.apache.iceberg.aws.s3.S3FileIO"));

    Map<String, String> resolved =
        support.resolveRegisterFileIoProperties(
            Map.of(
                "s3.endpoint", " http://override:9000 ",
                "fs.custom", "<unset>",
                "custom", "ignored"));

    assertEquals("http://override:9000", resolved.get("s3.endpoint"));
    assertEquals("org.apache.iceberg.aws.s3.S3FileIO", resolved.get("io-impl"));
    assertFalse(resolved.containsKey("custom"));
    assertFalse(resolved.containsKey("fs.custom"));
  }

  @Test
  void deleteConnectorSkipsNullAndSwallowsFailures() {
    support.deleteConnector(null);
    verify(grpcClient, never()).deleteConnector(any());

    ResourceId connectorId = ResourceId.newBuilder().setId("c-delete").build();
    support.deleteConnector(connectorId);
    verify(grpcClient, times(1)).deleteConnector(any());

    doThrow(Status.INTERNAL.asRuntimeException()).when(grpcClient).deleteConnector(any());
    support.deleteConnector(connectorId);
  }

  @Test
  void connectorTemplateForReturnsDirectAndMappedTemplate() {
    ConnectorIntegrationConfig.RegisterConnectorTemplate directTemplate =
        mock(ConnectorIntegrationConfig.RegisterConnectorTemplate.class);
    ConnectorIntegrationConfig.RegisterConnectorTemplate mappedTemplate =
        mock(ConnectorIntegrationConfig.RegisterConnectorTemplate.class);
    when(connectorConfig.registerConnectors())
        .thenReturn(Map.of("direct", directTemplate, "mapped", mappedTemplate));
    when(gatewayConfig.catalogMapping()).thenReturn(Map.of("alias", "mapped"));

    assertSame(directTemplate, support.connectorTemplateFor("direct"));
    assertSame(mappedTemplate, support.connectorTemplateFor("alias"));
    assertNull(support.connectorTemplateFor("missing"));
  }

  @Test
  void loadCurrentMetadataUsesCurrentSnapshotWhenIdsMatch() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    Table table =
        Table.newBuilder()
            .setResourceId(tableId)
            .putProperties("current-snapshot-id", "44")
            .build();
    IcebergMetadata metadata = IcebergMetadata.newBuilder().setTableUuid("t-44").build();
    when(grpcClient.getSnapshot(any()))
        .thenReturn(
            GetSnapshotResponse.newBuilder()
                .setSnapshot(
                    Snapshot.newBuilder()
                        .setSnapshotId(44L)
                        .putFormatMetadata("iceberg", metadata.toByteString())
                        .build())
                .build());

    IcebergMetadata loaded = support.loadCurrentMetadata(table);

    assertNotNull(loaded);
    assertEquals("t-44", loaded.getTableUuid());
    verify(grpcClient, times(1)).getSnapshot(any());
  }

  @Test
  void loadCurrentMetadataPrefersCurrentSnapshotAndOnlyFallsBackOnError() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    Table table =
        Table.newBuilder()
            .setResourceId(tableId)
            .putProperties("current-snapshot-id", "99")
            .build();
    IcebergMetadata firstMetadata = IcebergMetadata.newBuilder().setTableUuid("t-11").build();
    IcebergMetadata secondMetadata = IcebergMetadata.newBuilder().setTableUuid("t-99").build();
    when(grpcClient.getSnapshot(any()))
        .thenReturn(
            GetSnapshotResponse.newBuilder()
                .setSnapshot(
                    Snapshot.newBuilder()
                        .setSnapshotId(11L)
                        .putFormatMetadata("iceberg", firstMetadata.toByteString())
                        .build())
                .build())
        .thenReturn(
            GetSnapshotResponse.newBuilder()
                .setSnapshot(
                    Snapshot.newBuilder()
                        .setSnapshotId(99L)
                        .putFormatMetadata("iceberg", secondMetadata.toByteString())
                        .build())
                .build());

    IcebergMetadata loaded = support.loadCurrentMetadata(table);

    assertEquals("t-11", loaded.getTableUuid());
    verify(grpcClient, times(1)).getSnapshot(any());

    when(grpcClient.getSnapshot(any()))
        .thenThrow(Status.UNAVAILABLE.asRuntimeException())
        .thenReturn(
            GetSnapshotResponse.newBuilder()
                .setSnapshot(
                    Snapshot.newBuilder()
                        .setSnapshotId(99L)
                        .putFormatMetadata("iceberg", secondMetadata.toByteString())
                        .build())
                .build());
    IcebergMetadata recovered = support.loadCurrentMetadata(table);
    assertEquals("t-99", recovered.getTableUuid());
    ArgumentCaptor<GetSnapshotRequest> captor = ArgumentCaptor.forClass(GetSnapshotRequest.class);
    verify(grpcClient, times(3)).getSnapshot(captor.capture());
    assertEquals(tableId, captor.getAllValues().get(2).getTableId());
    assertEquals(99L, captor.getAllValues().get(2).getSnapshot().getSnapshotId());
  }

  @Test
  void loadCurrentMetadataReturnsNullWhenUnavailable() {
    assertNull(support.loadCurrentMetadata(null));
    assertNull(support.loadCurrentMetadata(Table.newBuilder().build()));
    verify(grpcClient, never()).getSnapshot(any());

    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders").build())
            .build();
    when(grpcClient.getSnapshot(any())).thenReturn(GetSnapshotResponse.newBuilder().build());
    assertNull(support.loadCurrentMetadata(table));

    when(grpcClient.getSnapshot(any())).thenThrow(Status.UNAVAILABLE.asRuntimeException());
    assertNull(support.loadCurrentMetadata(table));
  }

  @Test
  void credentialsForAccessDelegationThrowsWhenNoCredentialsAvailable() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> support.credentialsForAccessDelegation("vended-credentials"));
    assertEquals(
        "Credential vending was requested but no credentials are available", ex.getMessage());
  }

  @Test
  void credentialsForAccessDelegationUsesConfiguredScope() {
    ConnectorIntegrationConfig.StorageCredentialConfig storage =
        mock(ConnectorIntegrationConfig.StorageCredentialConfig.class);
    when(storage.properties())
        .thenReturn(
            Map.of(
                "s3.endpoint", "http://localhost:4566",
                "s3.region", "us-east-1"));
    when(storage.scope()).thenReturn(Optional.of("pref/*"));
    when(connectorConfig.storageCredential()).thenReturn(Optional.of(storage));

    List<StorageCredentialDto> credentials =
        support.credentialsForAccessDelegation("vended-credentials");

    assertEquals(1, credentials.size());
    assertEquals("pref/*", credentials.get(0).prefix());
    assertEquals("http://localhost:4566", credentials.get(0).config().get("s3.endpoint"));
    assertEquals("us-east-1", credentials.get(0).config().get("s3.region"));
  }

  @Test
  void connectorIntegrationEnabledReflectsConfig() {
    when(connectorConfig.enabled()).thenReturn(true).thenReturn(false);
    assertEquals(true, support.connectorIntegrationEnabled());
    assertEquals(false, support.connectorIntegrationEnabled());
  }
}
