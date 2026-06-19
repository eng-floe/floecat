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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
            true,
            true,
            true,
            false);

    assertEquals("us-east-1", response.getClientSafeConfigMap().get("s3.region"));
    assertEquals(1, response.getStorageCredentialsCount());
    assertEquals("s3://warehouse/orders", response.getStorageCredentials(0).getPrefix());
    assertEquals("akid", response.getStorageCredentials(0).getConfigMap().get("s3.access-key-id"));
    assertEquals(
        "secret", response.getStorageCredentials(0).getConfigMap().get("s3.secret-access-key"));
    assertFalse(response.getStorageCredentials(0).hasExpiresAt());
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
                true,
                true,
                false,
                true));
  }

  @Test
  void buildResponseRejectsStaticAwsSecretsForExecutionBoundServerSideVending() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            resolver.buildResponse(
                authority(),
                "s3://warehouse/orders",
                java.util.List.of("s3://warehouse/orders"),
                "acct",
                true,
                true,
                true,
                true));
  }

  @Test
  void buildResponseAllowsAmbientAssumeRoleWithoutStoredSecret() {
    StorageAuthorityResolver ambientResolver =
        new StorageAuthorityResolver() {
          @Override
          ResolvedStorageCredentials assumeRoleFromAmbientSource(
              StorageAuthority authority, java.util.List<String> sessionScopeLocations) {
            return new ResolvedStorageCredentials(
                "temp-akid", "temp-secret", "temp-token", Instant.parse("2026-06-19T12:00:00Z"));
          }
        };
    ambientResolver.secretsManager = new EmptySecretsManager();

    ResolveStorageAuthorityResponse response =
        ambientResolver.buildResponse(
            authority().toBuilder()
                .setAssumeRoleArn("arn:aws:iam::123456789012:role/customer-ro")
                .build(),
            "s3://warehouse/orders",
            java.util.List.of("s3://warehouse/orders"),
            "acct",
            true,
            true,
            true,
            false);

    assertEquals(
        "temp-akid", response.getStorageCredentials(0).getConfigMap().get("s3.access-key-id"));
    assertEquals(
        "temp-token", response.getStorageCredentials(0).getConfigMap().get("s3.session-token"));
    assertEquals(
        Instant.parse("2026-06-19T12:00:00Z").getEpochSecond(),
        response.getStorageCredentials(0).getExpiresAt().getSeconds());
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

  private static final class EmptySecretsManager implements SecretsManager {
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
