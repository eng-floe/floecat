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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.GetSnapshotResponse;
import ai.floedb.floecat.catalog.rpc.ListTablesRequest;
import ai.floedb.floecat.catalog.rpc.ListTablesResponse;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.ResolveTableResponse;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.common.rpc.PageResponse;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcClients;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.StorageCredentialDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TableIdentifierDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.support.GrpcServiceFacade;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import ai.floedb.floecat.reconciler.rpc.CaptureMode;
import ai.floedb.floecat.reconciler.rpc.CaptureNowRequest;
import ai.floedb.floecat.reconciler.rpc.CaptureNowResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Status;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.eclipse.microprofile.config.Config;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class TableGatewaySupportTest {
  private final GrpcWithHeaders grpc = mock(GrpcWithHeaders.class);
  private final GrpcClients clients = mock(GrpcClients.class);
  private final TableServiceGrpc.TableServiceBlockingStub tableStub =
      mock(TableServiceGrpc.TableServiceBlockingStub.class);
  private final DirectoryServiceGrpc.DirectoryServiceBlockingStub directoryStub =
      mock(DirectoryServiceGrpc.DirectoryServiceBlockingStub.class);
  private final IcebergGatewayConfig config = mock(IcebergGatewayConfig.class);
  private final Config mpConfig = mock(Config.class);
  private final GrpcServiceFacade grpcClient = mock(GrpcServiceFacade.class);
  private final ObjectMapper mapper = new ObjectMapper();
  private StorageAccessService storageAccessService;

  private TableGatewaySupport support;

  @BeforeEach
  void setUp() {
    when(config.metadataFileIo()).thenReturn(Optional.empty());
    when(config.metadataFileIoRoot()).thenReturn(Optional.empty());
    when(config.storageCredential()).thenReturn(Optional.empty());
    when(config.defaultRegion()).thenReturn(Optional.empty());
    when(config.catalogMapping()).thenReturn(Map.of());
    when(config.registerConnectors()).thenReturn(Map.of());
    when(mpConfig.getPropertyNames()).thenReturn(List.of());
    when(mpConfig.getOptionalValue(anyString(), eq(String.class))).thenReturn(Optional.empty());
    when(grpc.raw()).thenReturn(clients);
    when(clients.table()).thenReturn(tableStub);
    when(clients.directory()).thenReturn(directoryStub);
    when(grpc.withHeaders(tableStub)).thenReturn(tableStub);
    when(grpc.withHeaders(directoryStub)).thenReturn(directoryStub);

    storageAccessService = new StorageAccessService(config, mpConfig);
    support = new TableGatewaySupport(grpc, config, mapper, grpcClient, storageAccessService);
  }

  @Test
  void listTablesTransformsResponse() {
    ResourceId namespaceId = ResourceId.newBuilder().setId("cat:db").build();
    when(directoryStub.resolveNamespace(any()))
        .thenReturn(ResolveNamespaceResponse.newBuilder().setResourceId(namespaceId).build());
    Table table =
        Table.newBuilder()
            .setDisplayName("orders")
            .setResourceId(ResourceId.newBuilder().build())
            .build();
    var page = PageResponse.newBuilder().setNextPageToken("next-token").build();
    when(grpcClient.listTables(any()))
        .thenReturn(ListTablesResponse.newBuilder().addTables(table).setPage(page).build());

    TableGatewaySupport.ListTablesResult result = support.listTables("cat", "db", 50, "cursor");

    assertEquals(1, result.identifiers().size());
    TableIdentifierDto identifier = result.identifiers().get(0);
    assertEquals(List.of("db"), identifier.namespace());
    assertEquals("orders", identifier.name());
    assertEquals("next-token", result.nextPageToken());

    ArgumentCaptor<ListTablesRequest> captor = ArgumentCaptor.forClass(ListTablesRequest.class);
    verify(grpcClient).listTables(captor.capture());
    ListTablesRequest sent = captor.getValue();
    assertEquals(namespaceId, sent.getNamespaceId());
    assertEquals("cursor", sent.getPage().getPageToken());
    assertEquals(50, sent.getPage().getPageSize());
  }

  @Test
  void deleteTableResolvesIdentifiers() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(directoryStub.resolveTable(any()))
        .thenReturn(ResolveTableResponse.newBuilder().setResourceId(tableId).build());

    support.deleteTable(support.resolveTableId("cat", "db", "orders"));

    verify(grpcClient).deleteTable(any());
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
  void defaultTableConfigFiltersSecretsAndCachesResult() {
    IcebergGatewayConfig.StorageCredentialConfig storage =
        mock(IcebergGatewayConfig.StorageCredentialConfig.class);
    when(storage.properties())
        .thenReturn(
            Map.of(
                "s3.endpoint", " http://localhost:4566 ",
                "s3.secret-key", "secret",
                "region", "us-west-2"));
    when(config.storageCredential()).thenReturn(Optional.of(storage));
    when(config.metadataFileIo()).thenReturn(Optional.of("io.impl.Custom"));
    when(config.metadataFileIoRoot()).thenReturn(Optional.of("/warehouse/root"));
    when(config.defaultRegion()).thenReturn(Optional.of("us-east-1"));
    when(mpConfig.getPropertyNames())
        .thenReturn(
            List.of(
                "floecat.gateway.storage-credential.properties.s3.path-style-access",
                "floecat.gateway.storage-credential.properties.s3.access-key-id"));
    when(mpConfig.getOptionalValue(
            "floecat.gateway.storage-credential.properties.s3.path-style-access", String.class))
        .thenReturn(Optional.of("true"));
    when(mpConfig.getOptionalValue(
            "floecat.gateway.storage-credential.properties.s3.access-key-id", String.class))
        .thenReturn(Optional.of("akid"));

    Map<String, String> configMap = support.defaultTableConfig();
    Map<String, String> cached = support.defaultTableConfig();

    assertSame(configMap, cached);
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
    IcebergGatewayConfig.StorageCredentialConfig storage =
        mock(IcebergGatewayConfig.StorageCredentialConfig.class);
    when(storage.properties()).thenReturn(Map.of("s3.endpoint", "http://localhost:4566"));
    when(storage.scope()).thenReturn(Optional.of("tenant/*"));
    when(config.storageCredential()).thenReturn(Optional.of(storage));

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
    IcebergGatewayConfig.StorageCredentialConfig storage =
        mock(IcebergGatewayConfig.StorageCredentialConfig.class);
    when(storage.properties())
        .thenReturn(
            Map.of(
                "s3.endpoint", "http://localhost:4566",
                "not-io", "ignored"));
    when(config.storageCredential()).thenReturn(Optional.of(storage));
    when(config.metadataFileIo()).thenReturn(Optional.of("org.apache.iceberg.aws.s3.S3FileIO"));

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
    IcebergGatewayConfig.RegisterConnectorTemplate directTemplate =
        mock(IcebergGatewayConfig.RegisterConnectorTemplate.class);
    IcebergGatewayConfig.RegisterConnectorTemplate mappedTemplate =
        mock(IcebergGatewayConfig.RegisterConnectorTemplate.class);
    when(config.registerConnectors())
        .thenReturn(Map.of("direct", directTemplate, "mapped", mappedTemplate));
    when(config.catalogMapping()).thenReturn(Map.of("alias", "mapped"));

    assertSame(directTemplate, support.connectorTemplateFor("direct"));
    assertSame(mappedTemplate, support.connectorTemplateFor("alias"));
    assertNull(support.connectorTemplateFor("missing"));
  }

  @Test
  void runSyncStatisticsCaptureBuildsStatsOnlyRequest() {
    ResourceId connectorId =
        ResourceId.newBuilder().setId("c3").setKind(ResourceKind.RK_CONNECTOR).build();
    when(grpcClient.captureNow(any()))
        .thenReturn(
            CaptureNowResponse.newBuilder()
                .setTablesScanned(1)
                .setTablesChanged(1)
                .setErrors(0)
                .build());

    support.runSyncStatisticsCapture(connectorId, List.of("db", "analytics"), "orders");

    ArgumentCaptor<CaptureNowRequest> captor = ArgumentCaptor.forClass(CaptureNowRequest.class);
    verify(grpcClient, times(1)).captureNow(captor.capture());
    CaptureNowRequest request = captor.getValue();
    assertEquals(connectorId, request.getScope().getConnectorId());
    assertEquals("orders", request.getScope().getDestinationTableDisplayName());
    assertEquals(1, request.getScope().getDestinationNamespacePathsCount());
    assertEquals(CaptureMode.CM_STATS_ONLY, request.getMode());
    assertFalse(request.getFullRescan());
  }

  @Test
  void runSyncStatisticsCaptureIncludesExplicitSnapshotScope() {
    ResourceId connectorId =
        ResourceId.newBuilder().setId("c4").setKind(ResourceKind.RK_CONNECTOR).build();
    when(grpcClient.captureNow(any())).thenReturn(CaptureNowResponse.newBuilder().build());

    support.runSyncStatisticsCapture(
        connectorId, List.of("db", "analytics"), "orders", List.of(101L, 102L));

    ArgumentCaptor<CaptureNowRequest> captor = ArgumentCaptor.forClass(CaptureNowRequest.class);
    verify(grpcClient).captureNow(captor.capture());
    CaptureNowRequest request = captor.getValue();
    assertEquals(List.of(101L, 102L), request.getScope().getDestinationSnapshotIdsList());
    assertEquals(CaptureMode.CM_STATS_ONLY, request.getMode());
    assertFalse(request.getFullRescan());
  }

  @Test
  void runSyncStatisticsCaptureCanRequestFullRescanForExplicitSnapshotScope() {
    ResourceId connectorId =
        ResourceId.newBuilder().setId("c5").setKind(ResourceKind.RK_CONNECTOR).build();
    when(grpcClient.captureNow(any())).thenReturn(CaptureNowResponse.newBuilder().build());

    support.runSyncStatisticsCapture(
        connectorId, List.of("db", "analytics"), "orders", List.of(101L, 102L), true);

    ArgumentCaptor<CaptureNowRequest> captor = ArgumentCaptor.forClass(CaptureNowRequest.class);
    verify(grpcClient).captureNow(captor.capture());
    CaptureNowRequest request = captor.getValue();
    assertEquals(List.of(101L, 102L), request.getScope().getDestinationSnapshotIdsList());
    assertEquals(CaptureMode.CM_STATS_ONLY, request.getMode());
    assertTrue(request.getFullRescan());
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
                        .setSnapshotId(44)
                        .putFormatMetadata("iceberg", metadata.toByteString())
                        .build())
                .build());

    IcebergMetadata loaded = support.loadCurrentMetadata(table);

    assertNotNull(loaded);
    assertEquals("t-44", loaded.getTableUuid());
    verify(grpcClient, times(1)).getSnapshot(any());
  }

  @Test
  void loadCurrentMetadataFallsBackToSnapshotPropertyOnMismatchOrError() {
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
                        .setSnapshotId(11)
                        .putFormatMetadata("iceberg", firstMetadata.toByteString())
                        .build())
                .build())
        .thenReturn(
            GetSnapshotResponse.newBuilder()
                .setSnapshot(
                    Snapshot.newBuilder()
                        .setSnapshotId(99)
                        .putFormatMetadata("iceberg", secondMetadata.toByteString())
                        .build())
                .build());

    IcebergMetadata loaded = support.loadCurrentMetadata(table);

    assertEquals("t-99", loaded.getTableUuid());
    ArgumentCaptor<GetSnapshotRequest> captor = ArgumentCaptor.forClass(GetSnapshotRequest.class);
    verify(grpcClient, times(2)).getSnapshot(captor.capture());
    assertEquals(tableId, captor.getAllValues().get(1).getTableId());
    assertEquals(99L, captor.getAllValues().get(1).getSnapshot().getSnapshotId());

    when(grpcClient.getSnapshot(any()))
        .thenThrow(Status.UNAVAILABLE.asRuntimeException())
        .thenReturn(
            GetSnapshotResponse.newBuilder()
                .setSnapshot(
                    Snapshot.newBuilder()
                        .setSnapshotId(99)
                        .putFormatMetadata("iceberg", secondMetadata.toByteString())
                        .build())
                .build());
    IcebergMetadata recovered = support.loadCurrentMetadata(table);
    assertEquals("t-99", recovered.getTableUuid());
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
  void credentialsForAccessDelegationUsesPrefixedConfigAndScopeFallback() {
    IcebergGatewayConfig.StorageCredentialConfig storage =
        mock(IcebergGatewayConfig.StorageCredentialConfig.class);
    when(storage.properties()).thenReturn(Map.of());
    when(storage.scope()).thenReturn(Optional.of(" "));
    when(config.storageCredential()).thenReturn(Optional.of(storage));
    when(mpConfig.getPropertyNames())
        .thenReturn(
            List.of(
                "floecat.gateway.storage-credential.properties.s3.endpoint",
                "floecat.gateway.storage-credential.properties.s3.region"));
    when(mpConfig.getOptionalValue(
            "floecat.gateway.storage-credential.properties.s3.endpoint", String.class))
        .thenReturn(Optional.of("http://localhost:4566"));
    when(mpConfig.getOptionalValue(
            "floecat.gateway.storage-credential.properties.s3.region", String.class))
        .thenReturn(Optional.of("us-east-1"));
    when(mpConfig.getOptionalValue("floecat.gateway.storage-credential.scope", String.class))
        .thenReturn(Optional.of("pref/*"));

    List<StorageCredentialDto> credentials =
        support.credentialsForAccessDelegation("vended-credentials");

    assertEquals(1, credentials.size());
    assertEquals("pref/*", credentials.get(0).prefix());
    assertEquals("http://localhost:4566", credentials.get(0).config().get("s3.endpoint"));
    assertEquals("us-east-1", credentials.get(0).config().get("s3.region"));
  }

  @Test
  void connectorIntegrationEnabledReflectsConfig() {
    when(config.connectorIntegrationEnabled()).thenReturn(true).thenReturn(false);
    assertEquals(true, support.connectorIntegrationEnabled());
    assertEquals(false, support.connectorIntegrationEnabled());
  }
}
