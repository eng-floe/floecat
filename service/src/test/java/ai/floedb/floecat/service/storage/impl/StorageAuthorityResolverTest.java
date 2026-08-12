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
    ResolveStorageAuthorityResponse response = resolver.buildResponse(authority(), "acct", true);

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
          public boolean putIfAbsent(
              String accountId, String secretType, String secretId, byte[] payload) {
            return true;
          }

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
        resolverWithSessionToken.buildResponse(authority(), "acct", true);

    assertEquals("akid", response.getStorageCredentials(0).getConfigMap().get("s3.access-key-id"));
    assertEquals(
        "secret", response.getStorageCredentials(0).getConfigMap().get("s3.secret-access-key"));
    assertEquals("token", response.getStorageCredentials(0).getConfigMap().get("s3.session-token"));
    assertFalse(response.getStorageCredentials(0).hasExpiresAt());
  }

  /**
   * Now a structured status rather than a bare IllegalArgumentException: delegating connectors must
   * distinguish "no authority covers this location" from the other INVALID_ARGUMENT failures
   * vendStorageCredentials returns, and fall back only for this one.
   */
  @Test
  void buildResponseForClientWithoutAuthorityFails() {
    var error =
        assertThrows(
            io.grpc.StatusRuntimeException.class,
            () -> resolver.buildResponse(null, "acct", false));
    assertTrue(
        ai.floedb.floecat.storage.errors.SourceCatalogVendingGrpcStatus
            .isNoMatchingStorageAuthority(error),
        "must carry the structured no-matching-authority reason, not a bare INVALID_ARGUMENT");
  }

  @Test
  void buildResponseForServerSideNoAuthorityFails() {
    var error =
        assertThrows(
            io.grpc.StatusRuntimeException.class, () -> resolver.buildResponse(null, "acct", true));
  }

  @Test
  void buildResponseRejectsStaticAwsSecretsForClientVending() {
    assertThrows(
        IllegalArgumentException.class, () -> resolver.buildResponse(authority(), "acct", false));
  }

  @Test
  void buildResponseAllowsServerSideAssumeRoleWithStoredSourceCredentials() {
    StorageAuthorityResolver assumeRoleResolver =
        new StorageAuthorityResolver() {
          @Override
          ResolvedStorageCredentials assumeRoleFromStaticSource(
              StorageAuthority authority, AuthCredentials.AwsCredentials source) {
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
  void buildResponseReusesFreshAssumeRoleCredentialsForMatchingAuthority() {
    AtomicInteger resolutions = new AtomicInteger();
    StorageAuthorityResolver assumeRoleResolver =
        new StorageAuthorityResolver() {
          @Override
          ResolvedStorageCredentials assumeRoleFromStaticSource(
              StorageAuthority authority, AuthCredentials.AwsCredentials source) {
            resolutions.incrementAndGet();
            return new ResolvedStorageCredentials(
                "temp-akid", "temp-secret", "temp-token", Instant.now().plusSeconds(3600));
          }
        };
    assumeRoleResolver.secretsManager = new StaticSecretsManager();
    StorageAuthority authority =
        authority().toBuilder()
            .setAssumeRoleArn("arn:aws:iam::123456789012:role/customer-ro")
            .build();

    assumeRoleResolver.buildResponse(authority, "acct", true);
    assumeRoleResolver.buildResponse(authority, "acct", true);

    assertEquals(1, resolutions.get());
  }

  @Test
  void assumeRoleCacheIgnoresSecretMapEntryOrdering() {
    AtomicInteger resolutions = new AtomicInteger();
    StorageAuthorityResolver assumeRoleResolver =
        new StorageAuthorityResolver() {
          @Override
          ResolvedStorageCredentials assumeRoleFromStaticSource(
              StorageAuthority authority, AuthCredentials.AwsCredentials source) {
            resolutions.incrementAndGet();
            return new ResolvedStorageCredentials(
                "temp-akid", "temp-secret", "temp-token", Instant.now().plusSeconds(3600));
          }
        };
    StorageAuthority authority =
        authority().toBuilder()
            .setAssumeRoleArn("arn:aws:iam::123456789012:role/customer-ro")
            .build();
    AuthCredentials.Builder base =
        AuthCredentials.newBuilder()
            .setAws(
                AuthCredentials.AwsCredentials.newBuilder()
                    .setAccessKeyId("akid")
                    .setSecretAccessKey("secret"));
    // Protobuf does not order map entries on the wire, so these two logically identical secrets can
    // serialize to different bytes purely from insertion order.
    AuthCredentials forward =
        base.clone()
            .putProperties("alpha", "1")
            .putProperties("beta", "2")
            .putHeaders("x-one", "a")
            .putHeaders("x-two", "b")
            .build();
    AuthCredentials reversed =
        base.clone()
            .putProperties("beta", "2")
            .putProperties("alpha", "1")
            .putHeaders("x-two", "b")
            .putHeaders("x-one", "a")
            .build();

    assumeRoleResolver.assumeRoleCredentials(authority, forward);
    assumeRoleResolver.assumeRoleCredentials(authority, reversed);

    assertEquals(1, resolutions.get());

    assumeRoleResolver.assumeRoleCredentials(
        authority, base.clone().putProperties("alpha", "changed").build());

    assertEquals(2, resolutions.get());
  }

  @Test
  void assumeRoleCacheEvictsCompletedEntriesAtItsConfiguredBound() {
    AtomicInteger resolutions = new AtomicInteger();
    StorageAuthorityResolver assumeRoleResolver =
        new StorageAuthorityResolver(2) {
          @Override
          ResolvedStorageCredentials assumeRoleFromStaticSource(
              StorageAuthority authority, AuthCredentials.AwsCredentials source) {
            int resolution = resolutions.incrementAndGet();
            return new ResolvedStorageCredentials(
                "temp-akid-" + resolution,
                "temp-secret",
                "temp-token",
                Instant.now().plusSeconds(3600));
          }
        };
    StorageAuthority authority =
        authority().toBuilder()
            .setAssumeRoleArn("arn:aws:iam::123456789012:role/customer-ro")
            .build();
    AuthCredentials source =
        AuthCredentials.newBuilder()
            .setAws(
                AuthCredentials.AwsCredentials.newBuilder()
                    .setAccessKeyId("akid")
                    .setSecretAccessKey("secret"))
            .build();

    assumeRoleResolver.assumeRoleCredentials(withLocation(authority, "s3://warehouse/one"), source);
    assumeRoleResolver.assumeRoleCredentials(withLocation(authority, "s3://warehouse/two"), source);
    assumeRoleResolver.assumeRoleCredentials(
        withLocation(authority, "s3://warehouse/three"), source);

    assertEquals(2, assumeRoleResolver.assumeRoleCacheSize());
    assumeRoleResolver.assumeRoleCredentials(withLocation(authority, "s3://warehouse/one"), source);
    assertEquals(4, resolutions.get());
    assertEquals(2, assumeRoleResolver.assumeRoleCacheSize());
  }

  @Test
  void assumeRoleCacheBackpressuresDistinctInflightMissesAtItsConfiguredBound() throws Exception {
    java.util.concurrent.CountDownLatch firstTwoStarted =
        new java.util.concurrent.CountDownLatch(2);
    java.util.concurrent.CountDownLatch thirdStarted = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
    AtomicInteger resolutions = new AtomicInteger();
    StorageAuthorityResolver assumeRoleResolver =
        new StorageAuthorityResolver(2) {
          @Override
          ResolvedStorageCredentials assumeRoleFromStaticSource(
              StorageAuthority authority, AuthCredentials.AwsCredentials source) {
            int resolution = resolutions.incrementAndGet();
            if (resolution <= 2) {
              firstTwoStarted.countDown();
            } else {
              thirdStarted.countDown();
            }
            try {
              if (!release.await(5, java.util.concurrent.TimeUnit.SECONDS)) {
                throw new IllegalStateException("timed out waiting to release AssumeRole");
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new IllegalStateException("interrupted waiting to release AssumeRole", e);
            }
            return new ResolvedStorageCredentials(
                "temp-akid-" + resolution,
                "temp-secret",
                "temp-token",
                Instant.now().plusSeconds(3600));
          }
        };
    StorageAuthority authority =
        authority().toBuilder()
            .setAssumeRoleArn("arn:aws:iam::123456789012:role/customer-ro")
            .build();
    AuthCredentials source =
        AuthCredentials.newBuilder()
            .setAws(
                AuthCredentials.AwsCredentials.newBuilder()
                    .setAccessKeyId("akid")
                    .setSecretAccessKey("secret"))
            .build();
    java.util.concurrent.ExecutorService executor =
        java.util.concurrent.Executors.newFixedThreadPool(3);
    try {
      java.util.concurrent.Future<?> first =
          executor.submit(
              () ->
                  assumeRoleResolver.assumeRoleCredentials(
                      withLocation(authority, "s3://warehouse/one"), source));
      java.util.concurrent.Future<?> second =
          executor.submit(
              () ->
                  assumeRoleResolver.assumeRoleCredentials(
                      withLocation(authority, "s3://warehouse/two"), source));
      assertTrue(firstTwoStarted.await(5, java.util.concurrent.TimeUnit.SECONDS));
      java.util.concurrent.Future<?> third =
          executor.submit(
              () ->
                  assumeRoleResolver.assumeRoleCredentials(
                      withLocation(authority, "s3://warehouse/three"), source));

      assertFalse(thirdStarted.await(200, java.util.concurrent.TimeUnit.MILLISECONDS));
      assertEquals(2, assumeRoleResolver.assumeRoleCacheSize());
      release.countDown();
      first.get(5, java.util.concurrent.TimeUnit.SECONDS);
      second.get(5, java.util.concurrent.TimeUnit.SECONDS);
      third.get(5, java.util.concurrent.TimeUnit.SECONDS);
      assertEquals(3, resolutions.get());
      assertEquals(2, assumeRoleResolver.assumeRoleCacheSize());
    } finally {
      release.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void assumeRoleCacheCapacityWaiterProceedsWhenAnyInflightEntryCompletes() throws Exception {
    java.util.concurrent.CountDownLatch firstStarted = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.CountDownLatch secondStarted = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.CountDownLatch thirdStarted = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.CountDownLatch releaseFirst = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.CountDownLatch releaseSecond = new java.util.concurrent.CountDownLatch(1);
    StorageAuthorityResolver assumeRoleResolver =
        new StorageAuthorityResolver(2) {
          @Override
          ResolvedStorageCredentials assumeRoleFromStaticSource(
              StorageAuthority authority, AuthCredentials.AwsCredentials source) {
            String location = authority.getLocationPrefix();
            java.util.concurrent.CountDownLatch release = null;
            if (location.endsWith("/one")) {
              firstStarted.countDown();
              release = releaseFirst;
            } else if (location.endsWith("/two")) {
              secondStarted.countDown();
              release = releaseSecond;
            } else {
              thirdStarted.countDown();
            }
            try {
              if (release != null && !release.await(5, java.util.concurrent.TimeUnit.SECONDS)) {
                throw new IllegalStateException("timed out waiting to release AssumeRole");
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new IllegalStateException("interrupted waiting to release AssumeRole", e);
            }
            return new ResolvedStorageCredentials(
                "temp-akid-" + location,
                "temp-secret",
                "temp-token",
                Instant.now().plusSeconds(3600));
          }
        };
    StorageAuthority authority =
        authority().toBuilder()
            .setAssumeRoleArn("arn:aws:iam::123456789012:role/customer-ro")
            .build();
    AuthCredentials source =
        AuthCredentials.newBuilder()
            .setAws(
                AuthCredentials.AwsCredentials.newBuilder()
                    .setAccessKeyId("akid")
                    .setSecretAccessKey("secret"))
            .build();
    java.util.concurrent.ExecutorService executor =
        java.util.concurrent.Executors.newFixedThreadPool(3);
    try {
      java.util.concurrent.Future<?> first =
          executor.submit(
              () ->
                  assumeRoleResolver.assumeRoleCredentials(
                      withLocation(authority, "s3://warehouse/one"), source));
      assertTrue(firstStarted.await(5, java.util.concurrent.TimeUnit.SECONDS));
      java.util.concurrent.Future<?> second =
          executor.submit(
              () ->
                  assumeRoleResolver.assumeRoleCredentials(
                      withLocation(authority, "s3://warehouse/two"), source));
      assertTrue(secondStarted.await(5, java.util.concurrent.TimeUnit.SECONDS));
      java.util.concurrent.Future<?> third =
          executor.submit(
              () ->
                  assumeRoleResolver.assumeRoleCredentials(
                      withLocation(authority, "s3://warehouse/three"), source));

      assertFalse(thirdStarted.await(200, java.util.concurrent.TimeUnit.MILLISECONDS));
      releaseSecond.countDown();
      second.get(5, java.util.concurrent.TimeUnit.SECONDS);
      assertTrue(thirdStarted.await(5, java.util.concurrent.TimeUnit.SECONDS));
      third.get(5, java.util.concurrent.TimeUnit.SECONDS);
      assertFalse(first.isDone(), "the oldest in-flight request remains blocked");
      assertEquals(2, assumeRoleResolver.assumeRoleCacheSize());

      releaseFirst.countDown();
      first.get(5, java.util.concurrent.TimeUnit.SECONDS);
    } finally {
      releaseFirst.countDown();
      releaseSecond.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void buildResponseAllowsServerSideAssumeRoleWithAmbientSourceCredentials() {
    StorageAuthorityResolver assumeRoleResolver =
        new StorageAuthorityResolver() {
          @Override
          ResolvedStorageCredentials assumeRoleFromAmbientSource(StorageAuthority authority) {
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
                .build());

    assertEquals("temp-akid", credentials.accessKeyId());
    assertEquals(2, clientBuilds.get());
    assertEquals(2, providers.size());
    assertNotSame(providers.get(0), providers.get(1));
  }

  @Test
  void ambientAssumeRoleRetriesTransientConnectFailure() {
    StsClient failedClient = mock(StsClient.class);
    StsClient refreshedClient = mock(StsClient.class);
    when(failedClient.assumeRole(any(AssumeRoleRequest.class)))
        .thenThrow(SdkClientException.builder().message("Connect timed out").build());
    when(refreshedClient.assumeRole(any(AssumeRoleRequest.class)))
        .thenReturn(
            AssumeRoleResponse.builder()
                .credentials(
                    Credentials.builder()
                        .accessKeyId("temp-akid")
                        .secretAccessKey("temp-secret")
                        .sessionToken("temp-token")
                        .expiration(Instant.now().plusSeconds(3600))
                        .build())
                .build());

    AtomicInteger clientBuilds = new AtomicInteger();
    StorageAuthorityResolver assumeRoleResolver =
        new StorageAuthorityResolver() {
          @Override
          AwsCredentialsProvider ambientCredentialsProvider() {
            return mock(AwsCredentialsProvider.class);
          }

          @Override
          StsClient buildStsClient(StorageAuthority authority, AwsCredentialsProvider provider) {
            return clientBuilds.getAndIncrement() == 0 ? failedClient : refreshedClient;
          }

          @Override
          void pauseBeforeAssumeRoleRetry(int failedAttempt) {}
        };

    ResolvedStorageCredentials credentials =
        assumeRoleResolver.assumeRoleFromAmbientSource(
            authority().toBuilder()
                .setAssumeRoleArn("arn:aws:iam::123456789012:role/customer-ro")
                .build());

    assertEquals("temp-akid", credentials.accessKeyId());
    assertEquals(2, clientBuilds.get());
  }

  @Test
  void scopedSessionPolicyOmitsEmptyListPrefixConditionForBucketRoot() {
    String policy = StorageAuthorityResolver.scopedSessionPolicy("s3://warehouse/");

    assertTrue(policy.contains("\"Resource\":[\"arn:aws:s3:::warehouse\"]"));
    assertFalse(policy.contains("\"s3:prefix\""));
    assertTrue(policy.contains("\"Resource\":[\"arn:aws:s3:::warehouse/*\"]"));
  }

  @Test
  void scopedSessionPolicyForAuthorityPrefixRestrictsListingAndObjects() throws Exception {
    String policy = StorageAuthorityResolver.scopedSessionPolicy("s3://warehouse/warehouse/");
    ObjectMapper mapper =
        new ObjectMapper(
            JsonFactory.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build());

    JsonNode root = mapper.readTree(policy);

    assertEquals("2012-10-17", root.get("Version").asText());
    assertEquals(2, root.get("Statement").size());
    assertEquals("Allow", root.get("Statement").get(0).get("Effect").asText());
    assertEquals("Allow", root.get("Statement").get(1).get("Effect").asText());
    assertTrue(policy.contains("\"s3:prefix\":[\"warehouse\",\"warehouse/*\"]"));
    assertTrue(policy.contains("arn:aws:s3:::warehouse/warehouse/*"));
    assertFalse(policy.contains("\"Resource\":[\"arn:aws:s3:::warehouse/*\"]"));
  }

  @Test
  void authorityPolicyCoversDeletionVectorSiblingBeneathAuthorityPrefix() {
    String policy =
        StorageAuthorityResolver.scopedSessionPolicy(
            "s3://floedb-databricks-metastore-367509577365/metastore/metastore-id/tables/");

    assertTrue(
        policy.contains(
            "arn:aws:s3:::floedb-databricks-metastore-367509577365/metastore/metastore-id/tables/*"));
    assertFalse(policy.contains("table-uuid"));
  }

  @Test
  void invalidS3AuthorityLocationsCannotProduceAnUnscopedPolicy() {
    for (String location :
        new String[] {
          null,
          "",
          " ",
          "s3://",
          "s3:///warehouse",
          "not-an-s3-uri",
          "s3://bad bucket",
          "s3://warehouse/prefix*"
        }) {
      IllegalArgumentException error =
          assertThrows(
              IllegalArgumentException.class,
              () -> StorageAuthorityResolver.scopedSessionPolicy(location));
      assertTrue(error.getMessage().contains("concrete bucket"));
      if (location != null) {
        assertThrows(
            IllegalArgumentException.class,
            () -> resolver.buildResponse(withLocation(authority(), location), "acct", true));
      }
    }
  }

  @Test
  void resolveBestSelectsTheLongestMatchingEnabledAuthority() {
    StorageAuthority bucket = withLocation(authority(), "s3://warehouse/");
    StorageAuthority warehouse =
        withLocation(authority(), "s3://warehouse/warehouse/").toBuilder()
            .setResourceId(authority().getResourceId().toBuilder().setId("sa-2"))
            .build();
    StorageAuthority disabledNarrower =
        withLocation(authority(), "s3://warehouse/warehouse/orders/").toBuilder()
            .setResourceId(authority().getResourceId().toBuilder().setId("sa-3"))
            .setEnabled(false)
            .build();

    Optional<StorageAuthority> resolved =
        StorageAuthorityResolver.resolveBest(
            java.util.List.of(bucket, disabledNarrower, warehouse),
            "s3://warehouse/warehouse/orders/data.parquet");

    assertEquals(warehouse, resolved.orElseThrow());
  }

  private static StorageAuthority withLocation(StorageAuthority authority, String locationPrefix) {
    return authority.toBuilder().setLocationPrefix(locationPrefix).build();
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
    public boolean putIfAbsent(
        String accountId, String secretType, String secretId, byte[] payload) {
      return true;
    }

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
    public boolean putIfAbsent(
        String accountId, String secretType, String secretId, byte[] payload) {
      return true;
    }

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
