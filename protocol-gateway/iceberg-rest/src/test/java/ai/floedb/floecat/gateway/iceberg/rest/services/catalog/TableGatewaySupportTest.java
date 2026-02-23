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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
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
import ai.floedb.floecat.catalog.rpc.UpdateTableRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.CreateConnectorRequest;
import ai.floedb.floecat.connector.rpc.CreateConnectorResponse;
import ai.floedb.floecat.connector.rpc.GetConnectorResponse;
import ai.floedb.floecat.connector.rpc.SyncCaptureRequest;
import ai.floedb.floecat.connector.rpc.SyncCaptureResponse;
import ai.floedb.floecat.connector.rpc.TriggerReconcileRequest;
import ai.floedb.floecat.connector.rpc.TriggerReconcileResponse;
import ai.floedb.floecat.connector.rpc.UpdateConnectorRequest;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.StorageCredentialDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.ConnectorClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.SnapshotClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.TableClient;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
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
  private final IcebergGatewayConfig config = mock(IcebergGatewayConfig.class);
  private final Config mpConfig = mock(Config.class);
  private final TableClient tableClient = mock(TableClient.class);
  private final SnapshotClient snapshotClient = mock(SnapshotClient.class);
  private final ConnectorClient connectorClient = mock(ConnectorClient.class);
  private final ObjectMapper mapper = new ObjectMapper();

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

    support =
        new TableGatewaySupport(
            grpc, config, mapper, mpConfig, tableClient, snapshotClient, connectorClient);
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
    IcebergGatewayConfig.StorageCredentialConfig storage = mock(IcebergGatewayConfig.StorageCredentialConfig.class);
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
    IcebergGatewayConfig.StorageCredentialConfig storage = mock(IcebergGatewayConfig.StorageCredentialConfig.class);
    when(storage.properties()).thenReturn(Map.of("s3.endpoint", "http://localhost:4566"));
    when(storage.scope()).thenReturn(Optional.of("tenant/*"));
    when(config.storageCredential()).thenReturn(Optional.of(storage));

    IllegalArgumentException unsupported =
        assertThrows(
            IllegalArgumentException.class,
            () -> support.credentialsForAccessDelegation("sigv4"));
    assertEquals("Unsupported access delegation mode: sigv4", unsupported.getMessage());

    List<StorageCredentialDto> credentials =
        support.credentialsForAccessDelegation("vended-credentials");
    assertNotNull(credentials);
    assertEquals("tenant/*", credentials.get(0).prefix());
    assertEquals("http://localhost:4566", credentials.get(0).config().get("s3.endpoint"));
  }

  @Test
  void resolveRegisterFileIoPropertiesMergesDefaultsAndRequestOverrides() {
    IcebergGatewayConfig.StorageCredentialConfig storage = mock(IcebergGatewayConfig.StorageCredentialConfig.class);
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
  void createExternalConnectorBuildsExpectedRequest() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    ResourceId namespaceId = ResourceId.newBuilder().setId("cat:db").build();
    ResourceId catalogId = ResourceId.newBuilder().setId("cat").build();
    ResourceId connectorId = ResourceId.newBuilder().setId("connector-1").build();
    when(connectorClient.createConnector(any()))
        .thenReturn(
            CreateConnectorResponse.newBuilder()
                .setConnector(Connector.newBuilder().setResourceId(connectorId).build())
                .build());

    ResourceId created =
        support.createExternalConnector(
            "cat",
            List.of("db", "analytics"),
            namespaceId,
            catalogId,
            "orders",
            tableId,
            "s3://bucket/orders/metadata/v1.metadata.json",
            null,
            Map.of("s3.endpoint", " http://localhost:4566 ", "non-io", "x"),
            "idem-123");

    assertEquals(connectorId, created);
    ArgumentCaptor<CreateConnectorRequest> captor =
        ArgumentCaptor.forClass(CreateConnectorRequest.class);
    verify(connectorClient).createConnector(captor.capture());
    CreateConnectorRequest request = captor.getValue();
    assertEquals("idem-123:connector", request.getIdempotency().getKey());
    assertEquals(
        "register:cat:db.analytics.orders", request.getSpec().getDisplayName());
    assertEquals(
        "s3://bucket/orders/metadata/v1.metadata.json", request.getSpec().getUri());
    assertEquals(
        "s3://bucket/orders/metadata/v1.metadata.json",
        request.getSpec().getPropertiesOrThrow("external.metadata-location"));
    assertEquals("filesystem", request.getSpec().getPropertiesOrThrow("iceberg.source"));
    assertEquals("http://localhost:4566", request.getSpec().getPropertiesOrThrow("s3.endpoint"));
    assertFalse(request.getSpec().getPropertiesMap().containsKey("non-io"));
  }

  @Test
  void updateConnectorMetadataMergesPropertiesAndUpdatesMask() {
    ResourceId connectorId = ResourceId.newBuilder().setId("connector-1").build();
    when(connectorClient.getConnector(any()))
        .thenReturn(
            GetConnectorResponse.newBuilder()
                .setConnector(
                    Connector.newBuilder()
                        .setResourceId(connectorId)
                        .putProperties("existing", "value")
                        .build())
                .build());

    support.updateConnectorMetadata(connectorId, "s3://bucket/orders/metadata/v2.metadata.json");

    ArgumentCaptor<UpdateConnectorRequest> captor =
        ArgumentCaptor.forClass(UpdateConnectorRequest.class);
    verify(connectorClient).updateConnector(captor.capture());
    UpdateConnectorRequest request = captor.getValue();
    assertEquals(connectorId, request.getConnectorId());
    assertEquals(List.of("properties"), request.getUpdateMask().getPathsList());
    assertEquals("value", request.getSpec().getPropertiesOrThrow("existing"));
    assertEquals(
        "s3://bucket/orders/metadata/v2.metadata.json",
        request.getSpec().getPropertiesOrThrow("external.metadata-location"));
    assertEquals("filesystem", request.getSpec().getPropertiesOrThrow("iceberg.source"));
  }

  @Test
  void updateConnectorMetadataSkipsInvalidInputsOrMissingConnector() {
    support.updateConnectorMetadata(null, "s3://bucket/meta.json");
    support.updateConnectorMetadata(ResourceId.newBuilder().setId("c1").build(), " ");
    verify(connectorClient, never()).getConnector(any());

    ResourceId connectorId = ResourceId.newBuilder().setId("c1").build();
    when(connectorClient.getConnector(any()))
        .thenReturn(GetConnectorResponse.newBuilder().build());
    support.updateConnectorMetadata(connectorId, "s3://bucket/meta.json");
    verify(connectorClient, times(1)).getConnector(any());
    verify(connectorClient, never()).updateConnector(any());

    when(connectorClient.getConnector(any()))
        .thenThrow(Status.INTERNAL.asRuntimeException());
    support.updateConnectorMetadata(connectorId, "s3://bucket/meta.json");
  }

  @Test
  void deleteConnectorSkipsNullAndSwallowsFailures() {
    support.deleteConnector(null);
    verify(connectorClient, never()).deleteConnector(any());

    ResourceId connectorId = ResourceId.newBuilder().setId("c-delete").build();
    support.deleteConnector(connectorId);
    verify(connectorClient, times(1)).deleteConnector(any());

    doThrow(Status.INTERNAL.asRuntimeException()).when(connectorClient).deleteConnector(any());
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
  void createTemplateConnectorBuildsRequestAndSupportsIdempotency() {
    IcebergGatewayConfig.RegisterConnectorTemplate template =
        mock(IcebergGatewayConfig.RegisterConnectorTemplate.class);
    IcebergGatewayConfig.AuthTemplate authTemplate = mock(IcebergGatewayConfig.AuthTemplate.class);
    when(template.uri()).thenReturn("s3://template/location");
    when(template.displayName()).thenReturn(Optional.empty());
    when(template.description()).thenReturn(Optional.of("desc"));
    when(template.properties()).thenReturn(Map.of("k1", "v1"));
    when(template.captureStatistics()).thenReturn(false);
    when(template.auth()).thenReturn(Optional.of(authTemplate));
    when(authTemplate.scheme()).thenReturn("bearer");
    when(authTemplate.properties()).thenReturn(Map.of("authp", "authv"));
    when(authTemplate.headerHints()).thenReturn(Map.of("Authorization", "Bearer ..."));
    ResourceId connectorId = ResourceId.newBuilder().setId("template-connector").build();
    when(connectorClient.createConnector(any()))
        .thenReturn(
            CreateConnectorResponse.newBuilder()
                .setConnector(Connector.newBuilder().setResourceId(connectorId).build())
                .build());

    ResourceId created =
        support.createTemplateConnector(
            "cat",
            List.of("db"),
            ResourceId.newBuilder().setId("cat:db").build(),
            ResourceId.newBuilder().setId("cat").build(),
            "orders",
            ResourceId.newBuilder().setId("cat:db:orders").build(),
            template,
            "idem-key");

    assertEquals(connectorId, created);
    ArgumentCaptor<CreateConnectorRequest> captor =
        ArgumentCaptor.forClass(CreateConnectorRequest.class);
    verify(connectorClient).createConnector(captor.capture());
    CreateConnectorRequest request = captor.getValue();
    assertEquals("idem-key:connector", request.getIdempotency().getKey());
    assertEquals("register:cat:db.orders", request.getSpec().getDisplayName());
    assertEquals("desc", request.getSpec().getDescription());
    assertEquals("false", request.getSpec().getPropertiesOrThrow("floecat.connector.capture-statistics"));
    assertEquals("v1", request.getSpec().getPropertiesOrThrow("k1"));
    assertEquals("bearer", request.getSpec().getAuth().getScheme());
    assertEquals("authv", request.getSpec().getAuth().getPropertiesOrThrow("authp"));
  }

  @Test
  void createTemplateConnectorReturnsNullForEmptyResponse() {
    IcebergGatewayConfig.RegisterConnectorTemplate template =
        mock(IcebergGatewayConfig.RegisterConnectorTemplate.class);
    when(template.uri()).thenReturn("s3://template/location");
    when(template.displayName()).thenReturn(Optional.of("display"));
    when(template.description()).thenReturn(Optional.empty());
    when(template.properties()).thenReturn(Map.of());
    when(template.captureStatistics()).thenReturn(true);
    when(template.auth()).thenReturn(Optional.empty());
    when(connectorClient.createConnector(any()))
        .thenReturn(CreateConnectorResponse.newBuilder().build());

    ResourceId created =
        support.createTemplateConnector(
            "cat",
            List.of("db"),
            ResourceId.newBuilder().setId("cat:db").build(),
            ResourceId.newBuilder().setId("cat").build(),
            "orders",
            ResourceId.newBuilder().setId("cat:db:orders").build(),
            template,
            null);

    assertNull(created);
  }

  @Test
  void updateTableUpstreamIncludesConnectorNamespaceAndUri() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    ResourceId connectorId = ResourceId.newBuilder().setId("connector-9").build();

    support.updateTableUpstream(
        tableId, List.of("db", "analytics"), "orders", connectorId, "s3://target/orders");

    ArgumentCaptor<UpdateTableRequest> captor = ArgumentCaptor.forClass(UpdateTableRequest.class);
    verify(tableClient).updateTable(captor.capture());
    UpdateTableRequest request = captor.getValue();
    assertEquals(tableId, request.getTableId());
    assertEquals(List.of("upstream"), request.getUpdateMask().getPathsList());
    assertEquals(connectorId, request.getSpec().getUpstream().getConnectorId());
    assertEquals(List.of("db", "analytics"), request.getSpec().getUpstream().getNamespacePathList());
    assertEquals("orders", request.getSpec().getUpstream().getTableDisplayName());
    assertEquals("s3://target/orders", request.getSpec().getUpstream().getUri());
  }

  @Test
  void createExternalConnectorReturnsNullForEmptyResponse() {
    when(connectorClient.createConnector(any()))
        .thenReturn(CreateConnectorResponse.newBuilder().build());

    ResourceId created =
        support.createExternalConnector(
            "cat",
            List.of("db"),
            ResourceId.newBuilder().setId("cat:db").build(),
            ResourceId.newBuilder().setId("cat").build(),
            "orders",
            ResourceId.newBuilder().setId("cat:db:orders").build(),
            "s3://bucket/orders/metadata/v1.metadata.json",
            "s3://bucket/orders",
            Map.of(),
            null);

    assertNull(created);
  }

  @Test
  void runSyncMetadataCaptureSkipsInvalidInputsAndBuildsScopedRequest() {
    support.runSyncMetadataCapture(null, List.of("db"), "orders");
    support.runSyncMetadataCapture(ResourceId.newBuilder().setId("c1").build(), List.of("db"), " ");
    verify(connectorClient, never()).syncCapture(any());

    ResourceId connectorId = ResourceId.newBuilder().setId("c1").build();
    when(connectorClient.syncCapture(any()))
        .thenReturn(
            SyncCaptureResponse.newBuilder()
                .setTablesScanned(1)
                .setTablesChanged(1)
                .setErrors(0)
                .build());

    support.runSyncMetadataCapture(connectorId, List.of("db", "analytics"), "orders");

    ArgumentCaptor<SyncCaptureRequest> captor = ArgumentCaptor.forClass(SyncCaptureRequest.class);
    verify(connectorClient, times(1)).syncCapture(captor.capture());
    SyncCaptureRequest request = captor.getValue();
    assertEquals(connectorId, request.getConnectorId());
    assertEquals("orders", request.getDestinationTableDisplayName());
    assertFalse(request.getIncludeStatistics());
    assertEquals(1, request.getDestinationNamespacePathsCount());
    assertEquals(
        List.of("db", "analytics"),
        request.getDestinationNamespacePaths(0).getSegmentsList());
  }

  @Test
  void triggerScopedReconcileBuildsScopedRequestAndToleratesFailures() {
    ResourceId connectorId = ResourceId.newBuilder().setId("c2").build();
    when(connectorClient.triggerReconcile(any()))
        .thenReturn(TriggerReconcileResponse.newBuilder().setJobId("job-1").build());

    support.triggerScopedReconcile(connectorId, List.of("ns"), "orders");

    ArgumentCaptor<TriggerReconcileRequest> captor =
        ArgumentCaptor.forClass(TriggerReconcileRequest.class);
    verify(connectorClient).triggerReconcile(captor.capture());
    TriggerReconcileRequest request = captor.getValue();
    assertEquals(connectorId, request.getConnectorId());
    assertFalse(request.getFullRescan());
    assertEquals("orders", request.getDestinationTableDisplayName());
    assertEquals(List.of("ns"), request.getDestinationNamespacePaths(0).getSegmentsList());

    when(connectorClient.triggerReconcile(any()))
        .thenThrow(Status.INTERNAL.asRuntimeException());
    support.triggerScopedReconcile(connectorId, List.of("ns"), "orders");
  }

  @Test
  void loadCurrentMetadataUsesCurrentSnapshotWhenIdsMatch() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    Table table = Table.newBuilder().setResourceId(tableId).putProperties("current-snapshot-id", "44").build();
    IcebergMetadata metadata = IcebergMetadata.newBuilder().setTableUuid("t-44").build();
    when(snapshotClient.getSnapshot(any()))
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
    verify(snapshotClient, times(1)).getSnapshot(any());
  }

  @Test
  void loadCurrentMetadataFallsBackToSnapshotPropertyOnMismatchOrError() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    Table table = Table.newBuilder().setResourceId(tableId).putProperties("current-snapshot-id", "99").build();
    IcebergMetadata firstMetadata = IcebergMetadata.newBuilder().setTableUuid("t-11").build();
    IcebergMetadata secondMetadata = IcebergMetadata.newBuilder().setTableUuid("t-99").build();
    when(snapshotClient.getSnapshot(any()))
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
    verify(snapshotClient, times(2)).getSnapshot(captor.capture());
    assertEquals(tableId, captor.getAllValues().get(1).getTableId());
    assertEquals(99L, captor.getAllValues().get(1).getSnapshot().getSnapshotId());

    when(snapshotClient.getSnapshot(any()))
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
    verify(snapshotClient, never()).getSnapshot(any());

    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:orders").build())
            .build();
    when(snapshotClient.getSnapshot(any())).thenReturn(GetSnapshotResponse.newBuilder().build());
    assertNull(support.loadCurrentMetadata(table));

    when(snapshotClient.getSnapshot(any())).thenThrow(Status.UNAVAILABLE.asRuntimeException());
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
    IcebergGatewayConfig.StorageCredentialConfig storage = mock(IcebergGatewayConfig.StorageCredentialConfig.class);
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
  void runSyncMetadataCaptureWithoutNamespacePathOmitsNamespaceFilter() {
    ResourceId connectorId = ResourceId.newBuilder().setId("cx").build();
    when(connectorClient.syncCapture(any()))
        .thenReturn(SyncCaptureResponse.newBuilder().setTablesScanned(0).setTablesChanged(0).setErrors(0).build());

    support.runSyncMetadataCapture(connectorId, null, "orders");

    ArgumentCaptor<SyncCaptureRequest> captor = ArgumentCaptor.forClass(SyncCaptureRequest.class);
    verify(connectorClient, times(1)).syncCapture(captor.capture());
    assertEquals(0, captor.getValue().getDestinationNamespacePathsCount());
  }

  @Test
  void triggerScopedReconcileWithoutNamespacePathOmitsNamespaceFilter() {
    ResourceId connectorId = ResourceId.newBuilder().setId("cy").build();
    when(connectorClient.triggerReconcile(any()))
        .thenReturn(TriggerReconcileResponse.newBuilder().setJobId("job-2").build());

    support.triggerScopedReconcile(connectorId, null, "orders");

    ArgumentCaptor<TriggerReconcileRequest> captor =
        ArgumentCaptor.forClass(TriggerReconcileRequest.class);
    verify(connectorClient, times(1)).triggerReconcile(captor.capture());
    assertEquals(0, captor.getValue().getDestinationNamespacePathsCount());
  }

  @Test
  void connectorIntegrationEnabledReflectsConfig() {
    when(config.connectorIntegrationEnabled()).thenReturn(true).thenReturn(false);
    assertEquals(true, support.connectorIntegrationEnabled());
    assertEquals(false, support.connectorIntegrationEnabled());
  }
}
