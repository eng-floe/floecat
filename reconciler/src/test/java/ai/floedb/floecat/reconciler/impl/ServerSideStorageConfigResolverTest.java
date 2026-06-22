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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.common.auth.RefreshingAwsCredentialsProviderRegistry;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.connector.spi.ConnectorConfig;
import ai.floedb.floecat.storage.rpc.ResolveStorageAuthorityResponse;
import ai.floedb.floecat.storage.rpc.StorageAuthoritiesGrpc;
import ai.floedb.floecat.storage.rpc.VendStorageCredentialsRequest;
import ai.floedb.floecat.storage.rpc.VendedStorageCredential;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class ServerSideStorageConfigResolverTest {

  @Test
  void restStorageLocationHintUsesTableStorageRootForAuthorityLookup() {
    ConnectorConfig config =
        new ConnectorConfig(
            ConnectorConfig.Kind.ICEBERG,
            "rest",
            "http://polaris:8181/api/catalog",
            Map.of(
                "iceberg.source",
                "rest",
                "warehouse",
                "warehouse",
                "s3.endpoint",
                "http://minio:9000"),
            new ConnectorConfig.Auth("oauth2", Map.of(), Map.of()));

    assertEquals(
        "s3://warehouse/ns/table",
        ServerSideStorageConfigResolver.storageAuthorityLookupLocation(
            java.util.Optional.of("s3://warehouse/ns/table"), config));
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

    assertNull(
        ServerSideStorageConfigResolver.storageAuthorityLookupLocation(
            java.util.Optional.empty(), config));
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

    assertNull(
        ServerSideStorageConfigResolver.storageAuthorityLookupLocation(
            java.util.Optional.empty(), config));
  }

  @Test
  void restConnectorWithoutStorageLocationHintDoesNotResolveAuthorityLookupLocation() {
    ConnectorConfig config =
        new ConnectorConfig(
            ConnectorConfig.Kind.ICEBERG,
            "rest",
            "http://polaris:8181/api/catalog",
            Map.of("iceberg.source", "rest", "warehouse", "s3://warehouse"),
            new ConnectorConfig.Auth("oauth2", Map.of(), Map.of()));

    assertNull(
        ServerSideStorageConfigResolver.storageAuthorityLookupLocation(
            java.util.Optional.empty(), config));
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
  void
      mergeResolvedStorageConfigClearsStaleRefreshableCredentialsWhenNoExecutionBoundCredentials() {
    ResolveStorageAuthorityResponse response =
        ResolveStorageAuthorityResponse.newBuilder()
            .putClientSafeConfig("s3.region", "us-east-1")
            .build();

    Map<String, String> merged =
        ServerSideStorageConfigResolver.mergeResolvedStorageConfig(
            Map.of(
                "s3.access-key-id",
                "stale-access",
                "s3.secret-access-key",
                "stale-secret",
                "s3.session-token",
                "stale-token",
                RefreshingAwsCredentialsProviderRegistry.OPTION_PROVIDER_ID,
                "stale-provider",
                RefreshingAwsCredentialsProviderRegistry.PROPERTY_PROVIDER_ID,
                "stale-provider",
                RefreshingAwsCredentialsProviderRegistry.ICEBERG_CLIENT_CREDENTIALS_PROVIDER,
                "stale-provider",
                RefreshingAwsCredentialsProviderRegistry.ICEBERG_CLIENT_CREDENTIALS_PROVIDER_PREFIX
                    + "stale-provider",
                "stale-provider"),
            response,
            true,
            true,
            () ->
                new ai.floedb.floecat.connector.common.auth.ResolvedStorageCredentials(
                    "refresh-access",
                    "refresh-secret",
                    "refresh-session",
                    Instant.now().plusSeconds(900)));

    assertFalse(merged.containsKey("s3.access-key-id"));
    assertFalse(merged.containsKey("s3.secret-access-key"));
    assertFalse(merged.containsKey("s3.session-token"));
    assertEquals("us-east-1", merged.get("s3.region"));
    assertFalse(merged.containsKey(RefreshingAwsCredentialsProviderRegistry.OPTION_PROVIDER_ID));
    assertFalse(merged.containsKey(RefreshingAwsCredentialsProviderRegistry.PROPERTY_PROVIDER_ID));
    assertFalse(
        merged.containsKey(
            RefreshingAwsCredentialsProviderRegistry.ICEBERG_CLIENT_CREDENTIALS_PROVIDER));
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
  void resolveLeavesConfigUnchangedForRestIcebergWhenStorageAuthorityReturnsNoCreds() {
    ServerSideStorageConfigResolver resolver =
        new ServerSideStorageConfigResolver(java.util.Optional.empty(), java.util.Optional.empty());
    resolver.storageAuthorities = mock(StorageAuthoritiesGrpc.StorageAuthoritiesBlockingStub.class);
    when(resolver.storageAuthorities.withInterceptors(any()))
        .thenReturn(resolver.storageAuthorities);
    when(resolver.storageAuthorities.vendStorageCredentials(any()))
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

    ConnectorConfig resolved =
        resolver.resolveWithAuthorization(
            java.util.Optional.empty(),
            java.util.Optional.empty(),
            java.util.Optional.empty(),
            java.util.Optional.of("s3://warehouse/ns/table"),
            connector,
            config);

    assertEquals(config, resolved);
  }

  @Test
  void resolvePropagatesLookupFailureForRestIceberg() {
    ServerSideStorageConfigResolver resolver =
        new ServerSideStorageConfigResolver(java.util.Optional.empty(), java.util.Optional.empty());
    resolver.storageAuthorities = mock(StorageAuthoritiesGrpc.StorageAuthoritiesBlockingStub.class);
    when(resolver.storageAuthorities.withInterceptors(any()))
        .thenReturn(resolver.storageAuthorities);
    when(resolver.storageAuthorities.vendStorageCredentials(any()))
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

    assertThrows(
        StatusRuntimeException.class,
        () ->
            resolver.resolveWithAuthorization(
                java.util.Optional.empty(),
                java.util.Optional.empty(),
                java.util.Optional.empty(),
                java.util.Optional.of("s3://warehouse/ns/table"),
                connector,
                config));
  }

  @Test
  void resolveWithoutExplicitStorageLocationSkipsAuthorityLookupForFilesystemIceberg() {
    ServerSideStorageConfigResolver resolver =
        new ServerSideStorageConfigResolver(java.util.Optional.empty(), java.util.Optional.empty());
    resolver.storageAuthorities = mock(StorageAuthoritiesGrpc.StorageAuthoritiesBlockingStub.class);
    when(resolver.storageAuthorities.withInterceptors(any()))
        .thenReturn(resolver.storageAuthorities);
    when(resolver.storageAuthorities.vendStorageCredentials(any()))
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

    assertEquals(config, resolver.resolve(java.util.Optional.empty(), connector, config));
    verify(resolver.storageAuthorities, never()).vendStorageCredentials(any());
  }

  @Test
  void resolveMergesResolvedStorageAuthorityForExplicitDeltaStorageLocation() {
    ServerSideStorageConfigResolver resolver =
        new ServerSideStorageConfigResolver(java.util.Optional.empty(), java.util.Optional.empty());
    resolver.storageAuthorities = mock(StorageAuthoritiesGrpc.StorageAuthoritiesBlockingStub.class);
    when(resolver.storageAuthorities.withInterceptors(any()))
        .thenReturn(resolver.storageAuthorities);
    when(resolver.storageAuthorities.vendStorageCredentials(any()))
        .thenReturn(
            ResolveStorageAuthorityResponse.newBuilder()
                .putClientSafeConfig("s3.endpoint", "http://localstack:4566")
                .putClientSafeConfig("s3.path-style-access", "true")
                .putClientSafeConfig("s3.region", "us-east-1")
                .addStorageCredentials(
                    VendedStorageCredential.newBuilder()
                        .putConfig("s3.access-key-id", "test-access")
                        .putConfig("s3.secret-access-key", "test-secret")
                        .putConfig("s3.session-token", "test-session"))
                .build());

    Connector connector =
        Connector.newBuilder()
            .setKind(ConnectorKind.CK_DELTA)
            .setResourceId(
                ResourceId.newBuilder()
                    .setAccountId("acct")
                    .setId("conn")
                    .setKind(ResourceKind.RK_CONNECTOR)
                    .build())
            .build();
    ConnectorConfig config =
        new ConnectorConfig(
            ConnectorConfig.Kind.DELTA,
            "delta",
            "http://localhost",
            Map.of("delta.source", "unity"),
            new ConnectorConfig.Auth("none", Map.of(), Map.of()));

    ConnectorConfig resolved =
        resolver.resolveWithAuthorization(
            java.util.Optional.empty(),
            java.util.Optional.empty(),
            java.util.Optional.empty(),
            java.util.Optional.of("s3://bucket/table"),
            connector,
            config);

    assertEquals("http://localstack:4566", resolved.options().get("s3.endpoint"));
    assertEquals("us-east-1", resolved.options().get("s3.region"));
    assertEquals("true", resolved.options().get("s3.path-style-access"));
    assertEquals("test-access", resolved.options().get("s3.access-key-id"));
    assertEquals("test-secret", resolved.options().get("s3.secret-access-key"));
    assertEquals("test-session", resolved.options().get("s3.session-token"));
  }

  @Test
  void resolvePassesExecutionLeaseThroughAuthorityLookup() {
    ServerSideStorageConfigResolver resolver =
        new ServerSideStorageConfigResolver(java.util.Optional.empty(), java.util.Optional.empty());
    resolver.storageAuthorities = mock(StorageAuthoritiesGrpc.StorageAuthoritiesBlockingStub.class);
    when(resolver.storageAuthorities.withInterceptors(any()))
        .thenReturn(resolver.storageAuthorities);
    when(resolver.storageAuthorities.vendStorageCredentials(any()))
        .thenReturn(ResolveStorageAuthorityResponse.getDefaultInstance());

    Connector connector =
        Connector.newBuilder()
            .setKind(ConnectorKind.CK_DELTA)
            .setResourceId(
                ResourceId.newBuilder()
                    .setAccountId("acct")
                    .setId("conn")
                    .setKind(ResourceKind.RK_CONNECTOR)
                    .build())
            .build();
    ConnectorConfig config =
        new ConnectorConfig(
            ConnectorConfig.Kind.DELTA,
            "delta",
            "http://localhost",
            Map.of("delta.source", "unity"),
            new ConnectorConfig.Auth("none", Map.of(), Map.of()));

    resolver.resolveWithAuthorization(
        java.util.Optional.of("token"),
        java.util.Optional.of("job-1"),
        java.util.Optional.of("lease-1"),
        java.util.Optional.of("s3://bucket/table"),
        connector,
        config);

    ArgumentCaptor<VendStorageCredentialsRequest> requestCaptor =
        ArgumentCaptor.forClass(VendStorageCredentialsRequest.class);
    org.mockito.Mockito.verify(resolver.storageAuthorities)
        .vendStorageCredentials(requestCaptor.capture());
    assertEquals(
        "job-1", requestCaptor.getValue().getExecutionBinding().getReconcileLease().getJobId());
    assertEquals(
        "lease-1",
        requestCaptor.getValue().getExecutionBinding().getReconcileLease().getLeaseEpoch());
  }

  @Test
  void resolveManagedWithAuthorizationUsesRefreshingProviderForTemporaryExecutionCredentials() {
    ServerSideStorageConfigResolver resolver =
        new ServerSideStorageConfigResolver(java.util.Optional.empty(), java.util.Optional.empty());
    resolver.storageAuthorities = mock(StorageAuthoritiesGrpc.StorageAuthoritiesBlockingStub.class);
    when(resolver.storageAuthorities.withInterceptors(any()))
        .thenReturn(resolver.storageAuthorities);
    when(resolver.storageAuthorities.vendStorageCredentials(any()))
        .thenReturn(
            ResolveStorageAuthorityResponse.newBuilder()
                .putClientSafeConfig("s3.region", "us-east-1")
                .addStorageCredentials(
                    VendedStorageCredential.newBuilder()
                        .putConfig("s3.access-key-id", "test-access")
                        .putConfig("s3.secret-access-key", "test-secret")
                        .putConfig("s3.session-token", "test-session")
                        .setExpiresAt(
                            com.google.protobuf.util.Timestamps.fromMillis(
                                Instant.now().plusSeconds(900).toEpochMilli())))
                .build());

    Connector connector =
        Connector.newBuilder()
            .setKind(ConnectorKind.CK_DELTA)
            .setResourceId(
                ResourceId.newBuilder()
                    .setAccountId("acct")
                    .setId("conn")
                    .setKind(ResourceKind.RK_CONNECTOR)
                    .build())
            .build();
    ConnectorConfig config =
        new ConnectorConfig(
            ConnectorConfig.Kind.DELTA,
            "delta",
            "http://localhost",
            Map.of("delta.source", "unity"),
            new ConnectorConfig.Auth("none", Map.of(), Map.of()));

    String providerId;
    try (var resolved =
        resolver.resolveManagedWithAuthorization(
            java.util.Optional.of("token"),
            java.util.Optional.of("job-1"),
            java.util.Optional.of("lease-1"),
            java.util.Optional.of("s3://bucket/table"),
            connector,
            config)) {
      providerId =
          resolved
              .config()
              .options()
              .get(RefreshingAwsCredentialsProviderRegistry.OPTION_PROVIDER_ID);
      assertEquals("us-east-1", resolved.config().options().get("s3.region"));
      assertFalse(resolved.config().options().containsKey("s3.access-key-id"));
      assertFalse(resolved.config().options().containsKey("s3.secret-access-key"));
      assertFalse(resolved.config().options().containsKey("s3.session-token"));
      assertEquals(
          "test-access",
          RefreshingAwsCredentialsProviderRegistry.resolve(providerId).accessKeyId());
    }

    IllegalStateException error =
        assertThrows(
            IllegalStateException.class,
            () -> RefreshingAwsCredentialsProviderRegistry.resolve(providerId));
    assertEquals("Unknown AWS credentials provider id: " + providerId, error.getMessage());
  }

  @Test
  void resolveManagedWithAuthorizationAcceptsExecutionCredentialsWithoutSessionOrExpiry() {
    ServerSideStorageConfigResolver resolver =
        new ServerSideStorageConfigResolver(java.util.Optional.empty(), java.util.Optional.empty());
    resolver.storageAuthorities = mock(StorageAuthoritiesGrpc.StorageAuthoritiesBlockingStub.class);
    when(resolver.storageAuthorities.withInterceptors(any()))
        .thenReturn(resolver.storageAuthorities);
    when(resolver.storageAuthorities.vendStorageCredentials(any()))
        .thenReturn(
            ResolveStorageAuthorityResponse.newBuilder()
                .addStorageCredentials(
                    VendedStorageCredential.newBuilder()
                        .putConfig("s3.access-key-id", "test-access")
                        .putConfig("s3.secret-access-key", "test-secret"))
                .build());

    Connector connector =
        Connector.newBuilder()
            .setKind(ConnectorKind.CK_DELTA)
            .setResourceId(
                ResourceId.newBuilder()
                    .setAccountId("acct")
                    .setId("conn")
                    .setKind(ResourceKind.RK_CONNECTOR)
                    .build())
            .build();
    ConnectorConfig config =
        new ConnectorConfig(
            ConnectorConfig.Kind.DELTA,
            "delta",
            "http://localhost",
            Map.of("delta.source", "unity"),
            new ConnectorConfig.Auth("none", Map.of(), Map.of()));

    try (var resolved =
        resolver.resolveManagedWithAuthorization(
            java.util.Optional.of("token"),
            java.util.Optional.of("job-1"),
            java.util.Optional.of("lease-1"),
            java.util.Optional.of("s3://bucket/table"),
            connector,
            config)) {
      assertFalse(
          resolved
              .config()
              .options()
              .containsKey(RefreshingAwsCredentialsProviderRegistry.OPTION_PROVIDER_ID));
      assertEquals("test-access", resolved.config().options().get("s3.access-key-id"));
      assertEquals("test-secret", resolved.config().options().get("s3.secret-access-key"));
      assertFalse(resolved.config().options().containsKey("s3.session-token"));
    }
  }

  @Test
  void resolveManagedWithAuthorizationAcceptsExecutionCredentialsWithSessionTokenButNoExpiry() {
    ServerSideStorageConfigResolver resolver =
        new ServerSideStorageConfigResolver(java.util.Optional.empty(), java.util.Optional.empty());
    resolver.storageAuthorities = mock(StorageAuthoritiesGrpc.StorageAuthoritiesBlockingStub.class);
    when(resolver.storageAuthorities.withInterceptors(any()))
        .thenReturn(resolver.storageAuthorities);
    when(resolver.storageAuthorities.vendStorageCredentials(any()))
        .thenReturn(
            ResolveStorageAuthorityResponse.newBuilder()
                .addStorageCredentials(
                    VendedStorageCredential.newBuilder()
                        .putConfig("s3.access-key-id", "test-access")
                        .putConfig("s3.secret-access-key", "test-secret")
                        .putConfig("s3.session-token", "test-session"))
                .build());

    Connector connector =
        Connector.newBuilder()
            .setKind(ConnectorKind.CK_DELTA)
            .setResourceId(
                ResourceId.newBuilder()
                    .setAccountId("acct")
                    .setId("conn")
                    .setKind(ResourceKind.RK_CONNECTOR)
                    .build())
            .build();
    ConnectorConfig config =
        new ConnectorConfig(
            ConnectorConfig.Kind.DELTA,
            "delta",
            "http://localhost",
            Map.of("delta.source", "unity"),
            new ConnectorConfig.Auth("none", Map.of(), Map.of()));

    try (var resolved =
        resolver.resolveManagedWithAuthorization(
            java.util.Optional.of("token"),
            java.util.Optional.of("job-1"),
            java.util.Optional.of("lease-1"),
            java.util.Optional.of("s3://bucket/table"),
            connector,
            config)) {
      assertFalse(
          resolved
              .config()
              .options()
              .containsKey(RefreshingAwsCredentialsProviderRegistry.OPTION_PROVIDER_ID));
      assertEquals("test-access", resolved.config().options().get("s3.access-key-id"));
      assertEquals("test-secret", resolved.config().options().get("s3.secret-access-key"));
      assertEquals("test-session", resolved.config().options().get("s3.session-token"));
    }
  }

  @Test
  void resolveManagedWithAuthorizationRejectsRefreshResponseWithoutExpiry() {
    ServerSideStorageConfigResolver resolver =
        new ServerSideStorageConfigResolver(java.util.Optional.empty(), java.util.Optional.empty());
    resolver.storageAuthorities = mock(StorageAuthoritiesGrpc.StorageAuthoritiesBlockingStub.class);
    when(resolver.storageAuthorities.withInterceptors(any()))
        .thenReturn(resolver.storageAuthorities);
    when(resolver.storageAuthorities.vendStorageCredentials(any()))
        .thenReturn(
            ResolveStorageAuthorityResponse.newBuilder()
                .putClientSafeConfig("s3.region", "us-east-1")
                .addStorageCredentials(
                    VendedStorageCredential.newBuilder()
                        .putConfig("s3.access-key-id", "test-access-1")
                        .putConfig("s3.secret-access-key", "test-secret-1")
                        .putConfig("s3.session-token", "test-session-1")
                        .setExpiresAt(
                            com.google.protobuf.util.Timestamps.fromMillis(
                                Instant.now().minusSeconds(1).toEpochMilli())))
                .build())
        .thenReturn(
            ResolveStorageAuthorityResponse.newBuilder()
                .putClientSafeConfig("s3.region", "us-east-1")
                .addStorageCredentials(
                    VendedStorageCredential.newBuilder()
                        .putConfig("s3.access-key-id", "test-access-2")
                        .putConfig("s3.secret-access-key", "test-secret-2")
                        .putConfig("s3.session-token", "test-session-2"))
                .build());

    Connector connector =
        Connector.newBuilder()
            .setKind(ConnectorKind.CK_DELTA)
            .setResourceId(
                ResourceId.newBuilder()
                    .setAccountId("acct")
                    .setId("conn")
                    .setKind(ResourceKind.RK_CONNECTOR)
                    .build())
            .build();
    ConnectorConfig config =
        new ConnectorConfig(
            ConnectorConfig.Kind.DELTA,
            "delta",
            "http://localhost",
            Map.of("delta.source", "unity"),
            new ConnectorConfig.Auth("none", Map.of(), Map.of()));

    try (var resolved =
        resolver.resolveManagedWithAuthorization(
            java.util.Optional.of("token"),
            java.util.Optional.of("job-1"),
            java.util.Optional.of("lease-1"),
            java.util.Optional.of("s3://bucket/table"),
            connector,
            config)) {
      String providerId =
          resolved
              .config()
              .options()
              .get(RefreshingAwsCredentialsProviderRegistry.OPTION_PROVIDER_ID);
      var provider =
          ai.floedb.floecat.connector.common.auth.RegistryBackedAwsCredentialsProvider.create(
              Map.of(RefreshingAwsCredentialsProviderRegistry.PROPERTY_PROVIDER_ID, providerId));
      IllegalStateException error =
          assertThrows(IllegalStateException.class, provider::resolveCredentials);
      assertEquals(
          "Expected refreshable execution-bound credentials with access key, secret key, session"
              + " token, and expiresAt",
          error.getMessage());
    }
  }

  @Test
  void resolveManagedWithAuthorizationRefreshesThroughRegisteredProvider() {
    ServerSideStorageConfigResolver resolver =
        new ServerSideStorageConfigResolver(java.util.Optional.empty(), java.util.Optional.empty());
    resolver.storageAuthorities = mock(StorageAuthoritiesGrpc.StorageAuthoritiesBlockingStub.class);
    when(resolver.storageAuthorities.withInterceptors(any()))
        .thenReturn(resolver.storageAuthorities);
    when(resolver.storageAuthorities.vendStorageCredentials(any()))
        .thenReturn(
            ResolveStorageAuthorityResponse.newBuilder()
                .putClientSafeConfig("s3.region", "us-east-1")
                .addStorageCredentials(
                    VendedStorageCredential.newBuilder()
                        .putConfig("s3.access-key-id", "test-access-1")
                        .putConfig("s3.secret-access-key", "test-secret-1")
                        .putConfig("s3.session-token", "test-session-1")
                        .setExpiresAt(
                            com.google.protobuf.util.Timestamps.fromMillis(
                                Instant.now().minusSeconds(1).toEpochMilli())))
                .build())
        .thenReturn(
            ResolveStorageAuthorityResponse.newBuilder()
                .putClientSafeConfig("s3.region", "us-east-1")
                .addStorageCredentials(
                    VendedStorageCredential.newBuilder()
                        .putConfig("s3.access-key-id", "test-access-2")
                        .putConfig("s3.secret-access-key", "test-secret-2")
                        .putConfig("s3.session-token", "test-session-2")
                        .setExpiresAt(
                            com.google.protobuf.util.Timestamps.fromMillis(
                                Instant.now().plusSeconds(900).toEpochMilli())))
                .build());

    Connector connector =
        Connector.newBuilder()
            .setKind(ConnectorKind.CK_DELTA)
            .setResourceId(
                ResourceId.newBuilder()
                    .setAccountId("acct")
                    .setId("conn")
                    .setKind(ResourceKind.RK_CONNECTOR)
                    .build())
            .build();
    ConnectorConfig config =
        new ConnectorConfig(
            ConnectorConfig.Kind.DELTA,
            "delta",
            "http://localhost",
            Map.of("delta.source", "unity"),
            new ConnectorConfig.Auth("none", Map.of(), Map.of()));

    try (var resolved =
        resolver.resolveManagedWithAuthorization(
            java.util.Optional.of("token"),
            java.util.Optional.of("job-1"),
            java.util.Optional.of("lease-1"),
            java.util.Optional.of("s3://bucket/table"),
            connector,
            config)) {
      String providerId =
          resolved
              .config()
              .options()
              .get(RefreshingAwsCredentialsProviderRegistry.OPTION_PROVIDER_ID);
      var provider =
          ai.floedb.floecat.connector.common.auth.RegistryBackedAwsCredentialsProvider.create(
              Map.of(RefreshingAwsCredentialsProviderRegistry.PROPERTY_PROVIDER_ID, providerId));
      var credentials =
          assertInstanceOf(
              software.amazon.awssdk.auth.credentials.AwsSessionCredentials.class,
              provider.resolveCredentials());
      assertEquals("test-access-2", credentials.accessKeyId());
      assertEquals("test-secret-2", credentials.secretAccessKey());
      assertEquals("test-session-2", credentials.sessionToken());
    }
  }

  @Test
  void mergeResolvedStorageConfigClearsStaleProviderPropertiesWhenRegisteringNewProvider() {
    ResolveStorageAuthorityResponse response =
        ResolveStorageAuthorityResponse.newBuilder()
            .addStorageCredentials(
                VendedStorageCredential.newBuilder()
                    .putConfig("s3.access-key-id", "test-access")
                    .putConfig("s3.secret-access-key", "test-secret")
                    .putConfig("s3.session-token", "test-session")
                    .setExpiresAt(
                        com.google.protobuf.util.Timestamps.fromMillis(
                            Instant.now().plusSeconds(900).toEpochMilli())))
            .build();

    Map<String, String> merged =
        ServerSideStorageConfigResolver.mergeResolvedStorageConfig(
            Map.of(
                "s3.access-key-id",
                "stale-access",
                "s3.secret-access-key",
                "stale-secret",
                "s3.session-token",
                "stale-token",
                RefreshingAwsCredentialsProviderRegistry.OPTION_PROVIDER_ID,
                "stale-provider",
                RefreshingAwsCredentialsProviderRegistry.PROPERTY_PROVIDER_ID,
                "stale-provider",
                RefreshingAwsCredentialsProviderRegistry.ICEBERG_CLIENT_CREDENTIALS_PROVIDER,
                "stale-class",
                RefreshingAwsCredentialsProviderRegistry.ICEBERG_CLIENT_CREDENTIALS_PROVIDER_PREFIX
                    + "floecat-provider-id",
                "stale-provider"),
            response,
            true,
            true,
            () ->
                new ai.floedb.floecat.connector.common.auth.ResolvedStorageCredentials(
                    "refresh-access",
                    "refresh-secret",
                    "refresh-session",
                    Instant.now().plusSeconds(900)));

    assertFalse(merged.containsKey("s3.access-key-id"));
    assertFalse(merged.containsKey("s3.secret-access-key"));
    assertFalse(merged.containsKey("s3.session-token"));
    assertFalse(
        "stale-provider"
            .equals(merged.get(RefreshingAwsCredentialsProviderRegistry.OPTION_PROVIDER_ID)));
    assertFalse(
        merged.containsKey(
            RefreshingAwsCredentialsProviderRegistry.ICEBERG_CLIENT_CREDENTIALS_PROVIDER));

    String providerId = merged.get(RefreshingAwsCredentialsProviderRegistry.OPTION_PROVIDER_ID);
    RefreshingAwsCredentialsProviderRegistry.unregister(providerId);
  }

  @Test
  void resolvedStorageCredentialsParsesExpiryFromResponse() {
    ResolveStorageAuthorityResponse response =
        ResolveStorageAuthorityResponse.newBuilder()
            .addStorageCredentials(
                VendedStorageCredential.newBuilder()
                    .putConfig("s3.access-key-id", "test-access")
                    .putConfig("s3.secret-access-key", "test-secret")
                    .putConfig("s3.session-token", "test-session")
                    .setExpiresAt(
                        com.google.protobuf.util.Timestamps.fromMillis(
                            Instant.ofEpochSecond(1234L).toEpochMilli())))
            .build();

    var resolved =
        ServerSideStorageConfigResolver.resolvedStorageCredentials(response).orElseThrow();

    assertEquals("test-access", resolved.accessKeyId());
    assertEquals("test-secret", resolved.secretAccessKey());
    assertEquals("test-session", resolved.sessionToken());
    assertEquals(Instant.ofEpochSecond(1234L), resolved.expiresAt());
  }

  @Test
  void storageAuthorityLookupLocationUsesExplicitStorageLocation() {
    ConnectorConfig config =
        new ConnectorConfig(
            ConnectorConfig.Kind.DELTA,
            "delta",
            "http://localhost",
            Map.of("delta.source", "unity"),
            new ConnectorConfig.Auth("none", Map.of(), Map.of()));

    assertEquals(
        "s3://bucket/table",
        ServerSideStorageConfigResolver.storageAuthorityLookupLocation(
            java.util.Optional.of("s3://bucket/table"), config));
  }

  @Test
  void resolveWithAuthorizationRejectsPartialExecutionBinding() {
    ServerSideStorageConfigResolver resolver =
        new ServerSideStorageConfigResolver(java.util.Optional.empty(), java.util.Optional.empty());
    resolver.storageAuthorities = mock(StorageAuthoritiesGrpc.StorageAuthoritiesBlockingStub.class);
    when(resolver.storageAuthorities.withInterceptors(any()))
        .thenReturn(resolver.storageAuthorities);
    Connector connector =
        Connector.newBuilder()
            .setKind(ConnectorKind.CK_DELTA)
            .setResourceId(
                ResourceId.newBuilder()
                    .setAccountId("acct")
                    .setId("conn")
                    .setKind(ResourceKind.RK_CONNECTOR)
                    .build())
            .build();
    ConnectorConfig config =
        new ConnectorConfig(
            ConnectorConfig.Kind.DELTA,
            "delta",
            "http://localhost",
            Map.of("delta.source", "unity"),
            new ConnectorConfig.Auth("none", Map.of(), Map.of()));

    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                resolver.resolveWithAuthorization(
                    java.util.Optional.of("token"),
                    java.util.Optional.of("job-1"),
                    java.util.Optional.empty(),
                    java.util.Optional.of("s3://bucket/table"),
                    connector,
                    config));
    assertEquals(
        "executionJobId and executionLeaseEpoch must both be present together", error.getMessage());
  }
}
