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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.StorageCredentialDto;
import ai.floedb.floecat.gateway.iceberg.rest.config.ConnectorIntegrationConfig;
import ai.floedb.floecat.gateway.iceberg.rest.config.StorageAwsConfig;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.GrpcServiceFacade;
import ai.floedb.floecat.gateway.iceberg.rest.services.storage.StorageCredentialAuthority;
import ai.floedb.floecat.storage.rpc.ResolveStorageAuthorityRequest;
import ai.floedb.floecat.storage.rpc.ResolveStorageAuthorityResponse;
import ai.floedb.floecat.storage.rpc.VendedStorageCredential;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class TableGatewaySupportTest {

  @Test
  void serverSideFileIoPropertiesForLocationUsesExplicitCompatLocation() {
    GrpcServiceFacade grpcClient = mock(GrpcServiceFacade.class);
    StorageCredentialAuthority storageCredentialAuthority = mock(StorageCredentialAuthority.class);
    StorageAwsConfig storageAwsConfig = mock(StorageAwsConfig.class);
    StorageAwsConfig.S3Config s3Config = mock(StorageAwsConfig.S3Config.class);
    when(storageAwsConfig.region()).thenReturn(Optional.empty());
    when(storageAwsConfig.s3()).thenReturn(s3Config);
    when(s3Config.endpoint()).thenReturn(Optional.empty());
    when(s3Config.pathStyleAccess()).thenReturn(false);
    when(grpcClient.resolveStorageAuthority(any()))
        .thenReturn(
            ResolveStorageAuthorityResponse.newBuilder()
                .putClientSafeConfig("s3.endpoint", "http://localstack:4566")
                .addStorageCredentials(
                    VendedStorageCredential.newBuilder()
                        .setPrefix(
                            "s3://floecat-dev/accounts/acct-1/tables/tbl-1/snapshots/1/compat/")
                        .putConfig("s3.access-key-id", "key")
                        .putConfig("s3.secret-access-key", "secret")
                        .build())
                .build());

    TableGatewaySupport support =
        new TableGatewaySupport(
            mock(GrpcWithHeaders.class),
            mock(IcebergGatewayConfig.class),
            mock(ConnectorIntegrationConfig.class),
            storageAwsConfig,
            new ObjectMapper(),
            grpcClient,
            storageCredentialAuthority);
    Table table =
        Table.newBuilder()
            .setResourceId(
                ResourceId.newBuilder()
                    .setAccountId("acct-1")
                    .setKind(ResourceKind.RK_TABLE)
                    .setId("tbl-1")
                    .build())
            .putProperties("storage_location", "s3://upstream-bucket/orders")
            .build();

    Map<String, String> props =
        support.serverSideFileIoPropertiesForLocation(
            table,
            "s3://floecat-dev/accounts/acct-1/tables/tbl-1/snapshots/1/compat/iceberg-rest/metadata");

    assertEquals("http://localstack:4566", props.get("s3.endpoint"));
    assertEquals("key", props.get("s3.access-key-id"));
    assertEquals("secret", props.get("s3.secret-access-key"));

    ArgumentCaptor<ResolveStorageAuthorityRequest> requestCaptor =
        ArgumentCaptor.forClass(ResolveStorageAuthorityRequest.class);
    verify(grpcClient).resolveStorageAuthority(requestCaptor.capture());
    assertEquals(
        "s3://floecat-dev/accounts/acct-1/tables/tbl-1/snapshots/1/compat/iceberg-rest/metadata",
        requestCaptor.getValue().getLocationPrefix());
    verify(storageCredentialAuthority, never()).resolveServerSideFileIoConfig(any(), anyBoolean());
  }

  @Test
  void credentialsForAccessDelegationAcceptsDuckdbUnderscoreMode() {
    StorageCredentialAuthority storageCredentialAuthority = mock(StorageCredentialAuthority.class);
    StorageCredentialDto dto =
        new StorageCredentialDto(
            "s3://floecat-dev/", Map.of("s3.endpoint", "http://localstack:4566"), Instant.now());
    when(storageCredentialAuthority.resolveForTable(any(), anyBoolean())).thenReturn(List.of(dto));

    StorageAwsConfig storageAwsConfig = mock(StorageAwsConfig.class);
    StorageAwsConfig.S3Config s3Config = mock(StorageAwsConfig.S3Config.class);
    when(storageAwsConfig.region()).thenReturn(Optional.empty());
    when(storageAwsConfig.s3()).thenReturn(s3Config);
    when(s3Config.endpoint()).thenReturn(Optional.empty());
    when(s3Config.pathStyleAccess()).thenReturn(false);

    TableGatewaySupport support =
        new TableGatewaySupport(
            mock(GrpcWithHeaders.class),
            mock(IcebergGatewayConfig.class),
            mock(ConnectorIntegrationConfig.class),
            storageAwsConfig,
            new ObjectMapper(),
            mock(GrpcServiceFacade.class),
            storageCredentialAuthority);
    Table table =
        Table.newBuilder()
            .setResourceId(
                ResourceId.newBuilder()
                    .setAccountId("acct-1")
                    .setKind(ResourceKind.RK_TABLE)
                    .setId("tbl-1")
                    .build())
            .putProperties("location", "s3://floecat-dev/table")
            .build();

    List<StorageCredentialDto> credentials =
        support.credentialsForAccessDelegation(table, "vended_credentials");

    assertNotNull(credentials);
    assertEquals(1, credentials.size());
    assertEquals("http://localstack:4566", credentials.get(0).config().get("s3.endpoint"));
    assertEquals(true, support.usesVendedCredentials("vended_credentials"));
  }

  @Test
  void serverSideFileIoPropertiesForAuthorityMatchedLocationUsesAuthorityConfigOnly() {
    GrpcServiceFacade grpcClient = mock(GrpcServiceFacade.class);
    StorageCredentialAuthority storageCredentialAuthority = mock(StorageCredentialAuthority.class);
    StorageAwsConfig storageAwsConfig = mock(StorageAwsConfig.class);
    StorageAwsConfig.S3Config s3Config = mock(StorageAwsConfig.S3Config.class);
    when(storageAwsConfig.region()).thenReturn(Optional.of("us-east-1"));
    when(storageAwsConfig.s3()).thenReturn(s3Config);
    when(s3Config.endpoint()).thenReturn(Optional.of("http://localstack:4566"));
    when(s3Config.pathStyleAccess()).thenReturn(true);
    when(grpcClient.resolveStorageAuthority(any()))
        .thenReturn(
            ResolveStorageAuthorityResponse.newBuilder()
                .setAuthorityId(
                    ResourceId.newBuilder()
                        .setAccountId("acct-1")
                        .setKind(ResourceKind.RK_STORAGE_AUTHORITY)
                        .setId("sa-db")
                        .build())
                .addStorageCredentials(
                    VendedStorageCredential.newBuilder()
                        .setPrefix("s3://floedb-databricks-metastore-367509577365/")
                        .putConfig("s3.access-key-id", "key")
                        .putConfig("s3.secret-access-key", "secret")
                        .build())
                .build());

    TableGatewaySupport support =
        new TableGatewaySupport(
            mock(GrpcWithHeaders.class),
            mock(IcebergGatewayConfig.class),
            mock(ConnectorIntegrationConfig.class),
            storageAwsConfig,
            new ObjectMapper(),
            grpcClient,
            storageCredentialAuthority);
    Table table =
        Table.newBuilder()
            .setResourceId(
                ResourceId.newBuilder()
                    .setAccountId("acct-1")
                    .setKind(ResourceKind.RK_TABLE)
                    .setId("tbl-1")
                    .build())
            .putProperties("storage_location", "s3://floecat-dev/obs/floe_prod_otel_spans")
            .build();

    Map<String, String> props =
        support.serverSideFileIoPropertiesForLocation(
            table,
            "s3://floedb-databricks-metastore-367509577365/metastore/82a33d38/tables/9398a868/metadata");

    assertEquals("key", props.get("s3.access-key-id"));
    assertEquals("secret", props.get("s3.secret-access-key"));
    org.junit.jupiter.api.Assertions.assertFalse(props.containsKey("s3.endpoint"));
    org.junit.jupiter.api.Assertions.assertFalse(props.containsKey("s3.path-style-access"));
  }
}
