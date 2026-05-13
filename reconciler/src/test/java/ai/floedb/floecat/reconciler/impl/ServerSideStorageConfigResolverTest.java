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

package ai.floedb.floecat.reconciler.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.connector.spi.ConnectorConfig;
import ai.floedb.floecat.storage.rpc.ResolveStorageAuthorityResponse;
import ai.floedb.floecat.storage.rpc.StorageAuthoritiesGrpc;
import ai.floedb.floecat.storage.rpc.VendedStorageCredential;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ServerSideStorageConfigResolverTest {

  @Test
  void restWarehouseUsesS3PrefixForAuthorityLookup() {
    ConnectorConfig config =
        new ConnectorConfig(
            ConnectorConfig.Kind.ICEBERG,
            "rest",
            "http://polaris:8181/api/catalog",
            Map.of(
                "iceberg.source", "rest",
                "warehouse", "warehouse",
                "s3.endpoint", "http://minio:9000"),
            new ConnectorConfig.Auth("oauth2", Map.of(), Map.of()));

    assertEquals(
        "s3://warehouse/", ServerSideStorageConfigResolver.storageAuthorityLookupLocation(config));
  }

  @Test
  void filesystemConnectorUsesUriForAuthorityLookup() {
    ConnectorConfig config =
        new ConnectorConfig(
            ConnectorConfig.Kind.ICEBERG,
            "filesystem",
            "s3://warehouse/ns/table/metadata/00001.metadata.json",
            Map.of("iceberg.source", "filesystem"),
            new ConnectorConfig.Auth("none", Map.of(), Map.of()));

    assertEquals(
        "s3://warehouse/ns/table/metadata/00001.metadata.json",
        ServerSideStorageConfigResolver.storageAuthorityLookupLocation(config));
  }

  @Test
  void filesystemConnectorWithoutUriDoesNotResolveAuthorityLookupLocation() {
    ConnectorConfig config =
        new ConnectorConfig(
            ConnectorConfig.Kind.ICEBERG,
            "filesystem",
            "",
            Map.of(
                "iceberg.source", "filesystem",
                "metadata-location", "s3://warehouse/ns/table/metadata/00001.metadata.json"),
            new ConnectorConfig.Auth("none", Map.of(), Map.of()));

    assertNull(ServerSideStorageConfigResolver.storageAuthorityLookupLocation(config));
  }

  @Test
  void nonS3RestWarehouseDoesNotResolveAuthorityLookupLocation() {
    ConnectorConfig config =
        new ConnectorConfig(
            ConnectorConfig.Kind.ICEBERG,
            "rest",
            "http://polaris:8181/api/catalog",
            Map.of("iceberg.source", "rest", "warehouse", "file:///tmp/warehouse"),
            new ConnectorConfig.Auth("oauth2", Map.of(), Map.of()));

    assertNull(ServerSideStorageConfigResolver.storageAuthorityLookupLocation(config));
  }

  @Test
  void mergeResolvedStorageConfigAddsServerSideSecretsAndClientSafeProps() {
    ResolveStorageAuthorityResponse response =
        ResolveStorageAuthorityResponse.newBuilder()
            .putClientSafeConfig("s3.endpoint", "http://minio:9000")
            .putClientSafeConfig("s3.path-style-access", "true")
            .addStorageCredentials(
                VendedStorageCredential.newBuilder()
                    .putConfig("type", "s3")
                    .putConfig("s3.access-key-id", "minio")
                    .putConfig("s3.secret-access-key", "miniostorage"))
            .build();

    Map<String, String> merged =
        ServerSideStorageConfigResolver.mergeResolvedStorageConfig(
            Map.of("iceberg.source", "rest", "warehouse", "warehouse"), response);

    assertEquals("http://minio:9000", merged.get("s3.endpoint"));
    assertEquals("true", merged.get("s3.path-style-access"));
    assertEquals("minio", merged.get("s3.access-key-id"));
    assertEquals("miniostorage", merged.get("s3.secret-access-key"));
    assertNull(merged.get("type"));
  }

  @Test
  void mergeResolvedStorageConfigCanSkipServerSideSecrets() {
    ResolveStorageAuthorityResponse response =
        ResolveStorageAuthorityResponse.newBuilder()
            .putClientSafeConfig("s3.endpoint", "http://minio:9000")
            .putClientSafeConfig("s3.path-style-access", "true")
            .addStorageCredentials(
                VendedStorageCredential.newBuilder()
                    .putConfig("type", "s3")
                    .putConfig("s3.access-key-id", "minio")
                    .putConfig("s3.secret-access-key", "miniostorage"))
            .build();

    Map<String, String> merged =
        ServerSideStorageConfigResolver.mergeResolvedStorageConfig(
            Map.of("iceberg.source", "rest", "warehouse", "warehouse"), response, false);

    assertEquals("http://minio:9000", merged.get("s3.endpoint"));
    assertEquals("true", merged.get("s3.path-style-access"));
    assertFalse(merged.containsKey("s3.access-key-id"));
    assertFalse(merged.containsKey("s3.secret-access-key"));
  }

  @Test
  void resolveFallsBackToVendedCredentialsForRestIcebergWhenStorageAuthorityReturnsNoCreds() {
    ServerSideStorageConfigResolver resolver =
        new ServerSideStorageConfigResolver(java.util.Optional.empty(), java.util.Optional.empty());
    resolver.storageAuthorities = mock(StorageAuthoritiesGrpc.StorageAuthoritiesBlockingStub.class);
    when(resolver.storageAuthorities.withInterceptors(any()))
        .thenReturn(resolver.storageAuthorities);
    when(resolver.storageAuthorities.resolveStorageAuthorityForAccountLocation(any()))
        .thenReturn(ResolveStorageAuthorityResponse.getDefaultInstance());

    Connector connector =
        Connector.newBuilder()
            .setKind(ConnectorKind.CK_ICEBERG)
            .setResourceId(
                ResourceId.newBuilder()
                    .setAccountId("acct")
                    .setId("conn")
                    .setKind(ResourceKind.RK_CONNECTOR)
                    .build())
            .build();
    ConnectorConfig config =
        new ConnectorConfig(
            ConnectorConfig.Kind.ICEBERG,
            "rest",
            "http://polaris:8181/api/catalog",
            Map.of(
                "iceberg.source", "rest",
                "warehouse", "quickstart_catalog",
                "s3.endpoint", "http://localstack:4566"),
            new ConnectorConfig.Auth("oauth2", Map.of("token", "x"), Map.of()));

    ConnectorConfig resolved = resolver.resolve(java.util.Optional.empty(), connector, config);

    assertEquals(
        "vended-credentials", resolved.auth().headerHints().get("X-Iceberg-Access-Delegation"));
  }

  @Test
  void resolveFallsBackToVendedCredentialsForRestIcebergWhenStorageAuthorityLookupFails() {
    ServerSideStorageConfigResolver resolver =
        new ServerSideStorageConfigResolver(java.util.Optional.empty(), java.util.Optional.empty());
    resolver.storageAuthorities = mock(StorageAuthoritiesGrpc.StorageAuthoritiesBlockingStub.class);
    when(resolver.storageAuthorities.withInterceptors(any()))
        .thenReturn(resolver.storageAuthorities);
    when(resolver.storageAuthorities.resolveStorageAuthorityForAccountLocation(any()))
        .thenThrow(new StatusRuntimeException(Status.UNKNOWN.withDescription("no aws creds")));

    Connector connector =
        Connector.newBuilder()
            .setKind(ConnectorKind.CK_ICEBERG)
            .setResourceId(
                ResourceId.newBuilder()
                    .setAccountId("acct")
                    .setId("conn")
                    .setKind(ResourceKind.RK_CONNECTOR)
                    .build())
            .build();
    ConnectorConfig config =
        new ConnectorConfig(
            ConnectorConfig.Kind.ICEBERG,
            "rest",
            "http://polaris:8181/api/catalog",
            Map.of(
                "iceberg.source", "rest",
                "warehouse", "quickstart_catalog",
                "s3.endpoint", "http://localstack:4566"),
            new ConnectorConfig.Auth("oauth2", Map.of("token", "x"), Map.of()));

    ConnectorConfig resolved = resolver.resolve(java.util.Optional.empty(), connector, config);

    assertEquals(
        "vended-credentials", resolved.auth().headerHints().get("X-Iceberg-Access-Delegation"));
  }

  @Test
  void resolveSkipsServerSideSecretsWhenAccessDelegationHeaderAlreadyPresent() {
    ServerSideStorageConfigResolver resolver =
        new ServerSideStorageConfigResolver(java.util.Optional.empty(), java.util.Optional.empty());
    resolver.storageAuthorities = mock(StorageAuthoritiesGrpc.StorageAuthoritiesBlockingStub.class);
    when(resolver.storageAuthorities.withInterceptors(any()))
        .thenReturn(resolver.storageAuthorities);
    when(resolver.storageAuthorities.resolveStorageAuthorityForAccountLocation(any()))
        .thenReturn(
            ResolveStorageAuthorityResponse.newBuilder()
                .putClientSafeConfig("s3.endpoint", "http://localstack:4566")
                .putClientSafeConfig("s3.path-style-access", "true")
                .addStorageCredentials(
                    VendedStorageCredential.newBuilder()
                        .putConfig("s3.access-key-id", "test")
                        .putConfig("s3.secret-access-key", "test"))
                .build());

    Connector connector =
        Connector.newBuilder()
            .setKind(ConnectorKind.CK_ICEBERG)
            .setResourceId(
                ResourceId.newBuilder()
                    .setAccountId("acct")
                    .setId("conn")
                    .setKind(ResourceKind.RK_CONNECTOR)
                    .build())
            .build();
    ConnectorConfig config =
        new ConnectorConfig(
            ConnectorConfig.Kind.ICEBERG,
            "rest",
            "http://polaris:8181/api/catalog",
            Map.of(
                "iceberg.source", "rest",
                "warehouse", "quickstart_catalog",
                "s3.endpoint", "http://localstack:4566"),
            new ConnectorConfig.Auth(
                "oauth2",
                Map.of("token", "x"),
                Map.of("X-Iceberg-Access-Delegation", "vended-credentials")));

    ConnectorConfig resolved = resolver.resolve(java.util.Optional.empty(), connector, config);

    assertEquals(
        "vended-credentials", resolved.auth().headerHints().get("X-Iceberg-Access-Delegation"));
    assertEquals("http://localstack:4566", resolved.options().get("s3.endpoint"));
    assertFalse(resolved.options().containsKey("s3.access-key-id"));
    assertFalse(resolved.options().containsKey("s3.secret-access-key"));
  }

  @Test
  void resolveStillPropagatesLookupFailureForNonRestIceberg() {
    ServerSideStorageConfigResolver resolver =
        new ServerSideStorageConfigResolver(java.util.Optional.empty(), java.util.Optional.empty());
    resolver.storageAuthorities = mock(StorageAuthoritiesGrpc.StorageAuthoritiesBlockingStub.class);
    when(resolver.storageAuthorities.withInterceptors(any()))
        .thenReturn(resolver.storageAuthorities);
    when(resolver.storageAuthorities.resolveStorageAuthorityForAccountLocation(any()))
        .thenThrow(new StatusRuntimeException(Status.UNKNOWN.withDescription("no aws creds")));

    Connector connector =
        Connector.newBuilder()
            .setKind(ConnectorKind.CK_ICEBERG)
            .setResourceId(
                ResourceId.newBuilder()
                    .setAccountId("acct")
                    .setId("conn")
                    .setKind(ResourceKind.RK_CONNECTOR)
                    .build())
            .build();
    ConnectorConfig config =
        new ConnectorConfig(
            ConnectorConfig.Kind.ICEBERG,
            "filesystem",
            "s3://warehouse/ns/table/metadata/00001.metadata.json",
            Map.of("iceberg.source", "filesystem"),
            new ConnectorConfig.Auth("none", Map.of(), Map.of()));

    assertThrows(
        StatusRuntimeException.class,
        () -> resolver.resolve(java.util.Optional.empty(), connector, config));
  }
}
