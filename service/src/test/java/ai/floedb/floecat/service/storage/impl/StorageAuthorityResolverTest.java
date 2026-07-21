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

package ai.floedb.floecat.service.storage.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.common.auth.ResolvedStorageCredentials;
import ai.floedb.floecat.connector.rpc.AuthCredentials;
import ai.floedb.floecat.storage.rpc.ResolveStorageAuthorityResponse;
import ai.floedb.floecat.storage.rpc.StorageAuthority;
import ai.floedb.floecat.storage.secrets.SecretsManager;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

class StorageAuthorityResolverTest {
  private StorageAuthorityResolver resolver;

  @BeforeEach
  void setUp() {
    resolver = new StorageAuthorityResolver();
    resolver.secretsManager = new StaticSecretsManager();
  }

  @Test
  void buildResponseAllowsStaticAwsSecretsForServerSideAccess() {
    ResolveStorageAuthorityResponse response =
        resolver.buildResponse(
            authority(),
            "s3://warehouse/orders",
            java.util.List.of("s3://warehouse/orders"),
            "acct",
            true);

    assertEquals("us-east-1", response.getClientSafeConfigMap().get("s3.region"));
    assertEquals(1, response.getStorageCredentialsCount());
    assertEquals("s3://warehouse/orders", response.getStorageCredentials(0).getPrefix());
    assertEquals("akid", response.getStorageCredentials(0).getConfigMap().get("s3.access-key-id"));
    assertEquals(
        "secret", response.getStorageCredentials(0).getConfigMap().get("s3.secret-access-key"));
    assertFalse(response.getStorageCredentials(0).hasExpiresAt());
  }

  @Test
  void buildResponseAllowsServerSideCredentialsWithSessionTokenButNoExpiry() {
    StorageAuthorityResolver resolverWithSessionToken = new StorageAuthorityResolver();
    resolverWithSessionToken.secretsManager =
        new SecretsManager() {
          @Override
          public void put(String accountId, String secretType, String secretId, byte[] payload) {}

          @Override
          public Optional<byte[]> get(String accountId, String secretType, String secretId) {
            return Optional.of(
                AuthCredentials.newBuilder()
                    .setAws(
                        AuthCredentials.AwsCredentials.newBuilder()
                            .setAccessKeyId("akid")
                            .setSecretAccessKey("secret")
                            .setSessionToken("token"))
                    .build()
                    .toByteArray());
          }

          @Override
          public void update(
              String accountId, String secretType, String secretId, byte[] payload) {}

          @Override
          public void delete(String accountId, String secretType, String secretId) {}
        };

    ResolveStorageAuthorityResponse response =
        resolverWithSessionToken.buildResponse(
            authority(),
            "s3://warehouse/orders",
            java.util.List.of("s3://warehouse/orders"),
            "acct",
            true);

    assertEquals("akid", response.getStorageCredentials(0).getConfigMap().get("s3.access-key-id"));
    assertEquals(
        "secret", response.getStorageCredentials(0).getConfigMap().get("s3.secret-access-key"));
    assertEquals("token", response.getStorageCredentials(0).getConfigMap().get("s3.session-token"));
    assertFalse(response.getStorageCredentials(0).hasExpiresAt());
  }

  @Test
  void buildResponseForClientWithoutAuthorityFails() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            resolver.buildResponse(
                null,
                "s3://warehouse/orders",
                java.util.List.of("s3://warehouse/orders"),
                "acct",
                false));
  }

  @Test
  void buildResponseForServerSideNoAuthorityFails() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            resolver.buildResponse(
                null,
                "s3://warehouse/orders",
                java.util.List.of("s3://warehouse/orders"),
                "acct",
                true));
  }

  @Test
  void buildResponseRejectsStaticAwsSecretsForClientVending() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            resolver.buildResponse(
                authority(),
                "s3://warehouse/orders",
                java.util.List.of("s3://warehouse/orders"),
                "acct",
                false));
  }

  @Test
  void buildResponseAllowsServerSideAssumeRoleWithStoredSourceCredentials() {
    StorageAuthorityResolver assumeRoleResolver =
        new StorageAuthorityResolver() {
          @Override
          ResolvedStorageCredentials assumeRoleFromStaticSource(
              StorageAuthority authority,
              AuthCredentials.AwsCredentials source,
              java.util.List<String> sessionScopeLocations) {
            return new ResolvedStorageCredentials(
                "temp-akid", "temp-secret", "temp-token", Instant.parse("2026-06-19T12:00:00Z"));
          }
        };
    assumeRoleResolver.secretsManager =
        new EmptySecretsManager() {
          @Override
          public Optional<byte[]> get(String accountId, String secretType, String secretId) {
            return Optional.of(
                AuthCredentials.newBuilder()
                    .setAws(
                        AuthCredentials.AwsCredentials.newBuilder()
                            .setAccessKeyId("akid")
                            .setSecretAccessKey("secret")
                            .setSessionToken("token"))
                    .build()
                    .toByteArray());
          }
        };

    ResolveStorageAuthorityResponse response =
        assumeRoleResolver.buildResponse(
            authority().toBuilder()
                .setAssumeRoleArn("arn:aws:iam::123456789012:role/customer-ro")
                .build(),
            "s3://warehouse/orders",
            java.util.List.of("s3://warehouse/orders"),
            "acct",
            true);

    assertEquals(
        "temp-akid", response.getStorageCredentials(0).getConfigMap().get("s3.access-key-id"));
    assertEquals(
        "temp-token", response.getStorageCredentials(0).getConfigMap().get("s3.session-token"));
    assertEquals(
        Instant.parse("2026-06-19T12:00:00Z").getEpochSecond(),
        response.getStorageCredentials(0).getExpiresAt().getSeconds());
  }

  @Test
  void buildResponseAllowsServerSideAssumeRoleWithAmbientSourceCredentials() {
    StorageAuthorityResolver assumeRoleResolver =
        new StorageAuthorityResolver() {
          @Override
          ResolvedStorageCredentials assumeRoleFromAmbientSource(
              StorageAuthority authority, java.util.List<String> sessionScopeLocations) {
            return new ResolvedStorageCredentials(
                "temp-akid", "temp-secret", "temp-token", Instant.parse("2026-06-19T12:00:00Z"));
          }
        };
    assumeRoleResolver.secretsManager = new EmptySecretsManager();

    ResolveStorageAuthorityResponse response =
        assumeRoleResolver.buildResponse(
            authority().toBuilder()
                .setAssumeRoleArn("arn:aws:iam::123456789012:role/customer-ro")
                .build(),
            "s3://warehouse/orders",
            java.util.List.of("s3://warehouse/orders"),
            "acct",
            true);

    assertEquals(
        "temp-akid", response.getStorageCredentials(0).getConfigMap().get("s3.access-key-id"));
    assertEquals(
        "temp-token", response.getStorageCredentials(0).getConfigMap().get("s3.session-token"));
    assertEquals(
        Instant.parse("2026-06-19T12:00:00Z").getEpochSecond(),
        response.getStorageCredentials(0).getExpiresAt().getSeconds());
  }

  @Test
  void ambientAssumeRoleRebuildsCredentialsProviderWhenStsClientIsRefreshed() {
    StsClient failedClient = mock(StsClient.class);
    StsClient refreshedClient = mock(StsClient.class);
    when(failedClient.assumeRole(any(AssumeRoleRequest.class)))
        .thenThrow(SdkClientException.builder().message("Connection pool shut down").build());
    when(refreshedClient.assumeRole(any(AssumeRoleRequest.class)))
        .thenReturn(
            AssumeRoleResponse.builder()
                .credentials(
                    Credentials.builder()
                        .accessKeyId("temp-akid")
                        .secretAccessKey("temp-secret")
                        .sessionToken("temp-token")
                        .expiration(Instant.parse("2026-06-19T12:00:00Z"))
                        .build())
                .build());

    AtomicInteger clientBuilds = new AtomicInteger();
    ArrayList<AwsCredentialsProvider> providers = new ArrayList<>();
    StorageAuthorityResolver assumeRoleResolver =
        new StorageAuthorityResolver() {
          @Override
          AwsCredentialsProvider ambientCredentialsProvider() {
            return mock(AwsCredentialsProvider.class);
          }

          @Override
          StsClient buildStsClient(StorageAuthority authority, AwsCredentialsProvider provider) {
            providers.add(provider);
            return clientBuilds.getAndIncrement() == 0 ? failedClient : refreshedClient;
          }
        };

    ResolvedStorageCredentials credentials =
        assumeRoleResolver.assumeRoleFromAmbientSource(
            authority().toBuilder()
                .setAssumeRoleArn("arn:aws:iam::123456789012:role/customer-ro")
                .build(),
            java.util.List.of("s3://warehouse/orders"));

    assertEquals("temp-akid", credentials.accessKeyId());
    assertEquals(2, clientBuilds.get());
    assertEquals(2, providers.size());
    assertNotSame(providers.get(0), providers.get(1));
  }

  @Test
  void scopedSessionPolicyOmitsEmptyListPrefixConditionForBucketRoot() {
    String policy = StorageAuthorityResolver.scopedSessionPolicy("s3://warehouse");

    assertTrue(policy.contains("\"Resource\":[\"arn:aws:s3:::warehouse\"]"));
    assertFalse(policy.contains("\"s3:prefix\""));
    assertTrue(policy.contains("\"Resource\":[\"arn:aws:s3:::warehouse/*\"]"));
  }

  @Test
  void scopedSessionPolicyForNonRootParsesAsValidJson() throws Exception {
    String policy = StorageAuthorityResolver.scopedSessionPolicy("s3://warehouse/orders");
    ObjectMapper mapper =
        new ObjectMapper(
            JsonFactory.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build());

    JsonNode root = mapper.readTree(policy);

    assertEquals("2012-10-17", root.get("Version").asText());
    assertEquals(2, root.get("Statement").size());
    assertEquals("Allow", root.get("Statement").get(0).get("Effect").asText());
    assertEquals("Allow", root.get("Statement").get(1).get("Effect").asText());
  }

  @Test
  void scopedSessionPolicyForMultipleFilePathsParsesAsValidJson() throws Exception {
    String policy =
        StorageAuthorityResolver.scopedSessionPolicy(
            java.util.List.of(
                "s3://warehouse/orders/data/part-000.parquet",
                "s3://warehouse/orders/data/part-001.parquet",
                "s3://warehouse/orders/metadata/delete-000.parquet"));
    ObjectMapper mapper =
        new ObjectMapper(
            JsonFactory.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build());

    JsonNode root = mapper.readTree(policy);

    assertEquals("2012-10-17", root.get("Version").asText());
    assertEquals(2, root.get("Statement").size());
    assertEquals("Allow", root.get("Statement").get(0).get("Effect").asText());
    assertEquals("Allow", root.get("Statement").get(1).get("Effect").asText());
    assertTrue(policy.contains("part-000.parquet"));
    assertTrue(policy.contains("delete-000.parquet"));
  }

  private static StorageAuthority authority() {
    return StorageAuthority.newBuilder()
        .setResourceId(
            ResourceId.newBuilder()
                .setAccountId("acct")
                .setKind(ResourceKind.RK_STORAGE_AUTHORITY)
                .setId("sa-1")
                .build())
        .setDisplayName("orders")
        .setEnabled(true)
        .setType("s3")
        .setLocationPrefix("s3://warehouse/orders")
        .setRegion("us-east-1")
        .setEndpoint("http://localhost:4566")
        .setPathStyleAccess(true)
        .build();
  }

  private static final class StaticSecretsManager implements SecretsManager {
    @Override
    public void put(String accountId, String secretType, String secretId, byte[] payload) {}

    @Override
    public Optional<byte[]> get(String accountId, String secretType, String secretId) {
      return Optional.of(
          AuthCredentials.newBuilder()
              .setAws(
                  AuthCredentials.AwsCredentials.newBuilder()
                      .setAccessKeyId("akid")
                      .setSecretAccessKey("secret"))
              .build()
              .toByteArray());
    }

    @Override
    public void update(String accountId, String secretType, String secretId, byte[] payload) {}

    @Override
    public void delete(String accountId, String secretType, String secretId) {}
  }

  private static class EmptySecretsManager implements SecretsManager {
    @Override
    public void put(String accountId, String secretType, String secretId, byte[] payload) {}

    @Override
    public Optional<byte[]> get(String accountId, String secretType, String secretId) {
      return Optional.empty();
    }

    @Override
    public void update(String accountId, String secretType, String secretId, byte[] payload) {}

    @Override
    public void delete(String accountId, String secretType, String secretId) {}
  }
}
