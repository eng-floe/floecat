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
            () ->
                resolver.buildResponse(
                    null,
                    "s3://warehouse/orders",
                    java.util.List.of("s3://warehouse/orders"),
                    "acct",
                    false));
    assertTrue(
        ai.floedb.floecat.storage.errors.SourceCatalogVendingGrpcStatus
            .isNoMatchingStorageAuthority(error),
        "must carry the structured no-matching-authority reason, not a bare INVALID_ARGUMENT");
  }

  @Test
  void buildResponseForServerSideNoAuthorityFails() {
    var error =
        assertThrows(
            io.grpc.StatusRuntimeException.class,
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
              java.util.List<String> sessionScopeLocations,
              boolean exactObjectScope) {
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
  void buildResponseReusesFreshAssumeRoleCredentialsForMatchingScope() {
    AtomicInteger resolutions = new AtomicInteger();
    StorageAuthorityResolver assumeRoleResolver =
        new StorageAuthorityResolver() {
          @Override
          ResolvedStorageCredentials assumeRoleFromStaticSource(
              StorageAuthority authority,
              AuthCredentials.AwsCredentials source,
              java.util.List<String> sessionScopeLocations,
              boolean exactObjectScope) {
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

    assumeRoleResolver.buildResponse(
        authority,
        "s3://warehouse/orders",
        java.util.List.of("s3://warehouse/orders"),
        "acct",
        true);
    assumeRoleResolver.buildResponse(
        authority,
        "s3://warehouse/orders",
        java.util.List.of("s3://warehouse/orders"),
        "acct",
        true);

    assertEquals(1, resolutions.get());
  }

  @Test
  void assumeRoleCacheCanonicalizesEquivalentScopeSets() {
    AtomicInteger resolutions = new AtomicInteger();
    StorageAuthorityResolver assumeRoleResolver =
        new StorageAuthorityResolver() {
          @Override
          ResolvedStorageCredentials assumeRoleFromStaticSource(
              StorageAuthority authority,
              AuthCredentials.AwsCredentials source,
              java.util.List<String> sessionScopeLocations,
              boolean exactObjectScope) {
            resolutions.incrementAndGet();
            return new ResolvedStorageCredentials(
                "temp-akid", "temp-secret", "temp-token", Instant.now().plusSeconds(3600));
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

    assumeRoleResolver.assumeRoleCredentials(
        authority,
        source,
        java.util.List.of("s3://warehouse/two/", "s3://warehouse/one"),
        false);
    assumeRoleResolver.assumeRoleCredentials(
        authority,
        source,
        java.util.List.of("S3A://WAREHOUSE/one/", "s3n://warehouse/two"),
        false);

    assertEquals(1, resolutions.get());
  }

  @Test
  void assumeRoleCacheSeparatesPrefixAndExactObjectScopes() {
    AtomicInteger resolutions = new AtomicInteger();
    StorageAuthorityResolver assumeRoleResolver =
        new StorageAuthorityResolver() {
          @Override
          ResolvedStorageCredentials assumeRoleFromStaticSource(
              StorageAuthority authority,
              AuthCredentials.AwsCredentials source,
              java.util.List<String> sessionScopeLocations,
              boolean exactObjectScope) {
            resolutions.incrementAndGet();
            return new ResolvedStorageCredentials(
                "temp-akid", "temp-secret", "temp-token", Instant.now().plusSeconds(3600));
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

    assumeRoleResolver.assumeRoleCredentials(
        authority, source, java.util.List.of("s3://warehouse/orders"), false);
    assumeRoleResolver.assumeRoleCredentials(
        authority, source, java.util.List.of("s3://warehouse/orders"), true);

    assertEquals(2, resolutions.get());
  }

  @Test
  void assumeRoleCacheIgnoresSecretMapEntryOrdering() {
    AtomicInteger resolutions = new AtomicInteger();
    StorageAuthorityResolver assumeRoleResolver =
        new StorageAuthorityResolver() {
          @Override
          ResolvedStorageCredentials assumeRoleFromStaticSource(
              StorageAuthority authority,
              AuthCredentials.AwsCredentials source,
              java.util.List<String> sessionScopeLocations,
              boolean exactObjectScope) {
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

    assumeRoleResolver.assumeRoleCredentials(
        authority, forward, java.util.List.of("s3://warehouse/one"), false);
    assumeRoleResolver.assumeRoleCredentials(
        authority, reversed, java.util.List.of("s3://warehouse/one"), false);

    assertEquals(1, resolutions.get());

    assumeRoleResolver.assumeRoleCredentials(
        authority,
        base.clone().putProperties("alpha", "changed").build(),
        java.util.List.of("s3://warehouse/one"),
        false);

    assertEquals(2, resolutions.get());
  }

  @Test
  void assumeRoleCacheEvictsCompletedEntriesAtItsConfiguredBound() {
    AtomicInteger resolutions = new AtomicInteger();
    StorageAuthorityResolver assumeRoleResolver =
        new StorageAuthorityResolver(2) {
          @Override
          ResolvedStorageCredentials assumeRoleFromStaticSource(
              StorageAuthority authority,
              AuthCredentials.AwsCredentials source,
              java.util.List<String> sessionScopeLocations,
              boolean exactObjectScope) {
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

    assumeRoleResolver.assumeRoleCredentials(
        authority, source, java.util.List.of("s3://warehouse/one"), false);
    assumeRoleResolver.assumeRoleCredentials(
        authority, source, java.util.List.of("s3://warehouse/two"), false);
    assumeRoleResolver.assumeRoleCredentials(
        authority, source, java.util.List.of("s3://warehouse/three"), false);

    assertEquals(2, assumeRoleResolver.assumeRoleCacheSize());
    assumeRoleResolver.assumeRoleCredentials(
        authority, source, java.util.List.of("s3://warehouse/one"), false);
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
              StorageAuthority authority,
              AuthCredentials.AwsCredentials source,
              java.util.List<String> sessionScopeLocations,
              boolean exactObjectScope) {
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
                      authority, source, java.util.List.of("s3://warehouse/one"), false));
      java.util.concurrent.Future<?> second =
          executor.submit(
              () ->
                  assumeRoleResolver.assumeRoleCredentials(
                      authority, source, java.util.List.of("s3://warehouse/two"), false));
      assertTrue(firstTwoStarted.await(5, java.util.concurrent.TimeUnit.SECONDS));
      java.util.concurrent.Future<?> third =
          executor.submit(
              () ->
                  assumeRoleResolver.assumeRoleCredentials(
                      authority, source, java.util.List.of("s3://warehouse/three"), false));

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
              StorageAuthority authority,
              AuthCredentials.AwsCredentials source,
              java.util.List<String> sessionScopeLocations,
              boolean exactObjectScope) {
            String location = sessionScopeLocations.getFirst();
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
                      authority, source, java.util.List.of("s3://warehouse/one"), false));
      assertTrue(firstStarted.await(5, java.util.concurrent.TimeUnit.SECONDS));
      java.util.concurrent.Future<?> second =
          executor.submit(
              () ->
                  assumeRoleResolver.assumeRoleCredentials(
                      authority, source, java.util.List.of("s3://warehouse/two"), false));
      assertTrue(secondStarted.await(5, java.util.concurrent.TimeUnit.SECONDS));
      java.util.concurrent.Future<?> third =
          executor.submit(
              () ->
                  assumeRoleResolver.assumeRoleCredentials(
                      authority, source, java.util.List.of("s3://warehouse/three"), false));

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
          ResolvedStorageCredentials assumeRoleFromAmbientSource(
              StorageAuthority authority,
              java.util.List<String> sessionScopeLocations,
              boolean exactObjectScope) {
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
            java.util.List.of("s3://warehouse/orders"),
            false);

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
                .build(),
            java.util.List.of("s3://warehouse/orders"),
            false);

    assertEquals("temp-akid", credentials.accessKeyId());
    assertEquals(2, clientBuilds.get());
  }

  @Test
  void scopedSessionPolicyOmitsEmptyListPrefixConditionForBucketRoot() {
    String policy = StorageAuthorityResolver.scopedSessionPolicy("s3://warehouse");

    assertTrue(policy.contains("\"Resource\":[\"arn:aws:s3:::warehouse\"]"));
    assertFalse(policy.contains("\"s3:prefix\""));
    assertTrue(policy.contains("\"Resource\":[\"arn:aws:s3:::warehouse/*\"]"));
  }

  @Test
  void scopedSessionPolicyForExactObjectOmitsChildWildcard() {
    String location = "s3://warehouse/orders/data/part-000.parquet";
    String objectArn = "arn:aws:s3:::warehouse/orders/data/part-000.parquet";

    String prefixPolicy = StorageAuthorityResolver.scopedSessionPolicy(java.util.List.of(location));
    // The default (prefix) scope grants the object and everything beneath its key.
    assertTrue(prefixPolicy.contains("\"" + objectArn + "/*\""));

    String exactPolicy =
        StorageAuthorityResolver.scopedSessionPolicy(java.util.List.of(location), true);
    // The exact-object scope grants the object itself and nothing beneath it -- no child wildcard
    // on the object resource, and no child-prefix in the list condition.
    assertTrue(exactPolicy.contains("\"" + objectArn + "\""));
    assertFalse(exactPolicy.contains(objectArn + "/*"));
    assertFalse(exactPolicy.contains("orders/data/part-000.parquet/*"));
  }

  @Test
  void exactObjectScopeRefusesKeysCarryingIamWildcardMetacharacters() {
    // `*` and `?` are legal in an S3 object key and are wildcards in an IAM resource ARN, so a
    // planned file named part-*.parquet would mint access to every sibling it matches -- the
    // opposite of what an exact-object scope promises. IAM cannot express a literal `*`, so the
    // only safe answer is refusal; widening the grant is not an acceptable fallback.
    for (String key : java.util.List.of("part-*.parquet", "part-00?.parquet", "*")) {
      String location = "s3://warehouse/orders/data/" + key;
      assertThrows(
          IllegalArgumentException.class,
          () -> StorageAuthorityResolver.scopedSessionPolicy(java.util.List.of(location), true),
          "expected refusal for key " + key);
    }
  }

  @Test
  void wildcardKeysStillAllowedForPrefixScopesWhichNeverPromisedASingleObject() {
    // Only the exact-object contract is broken by a metacharacter. A prefix scope is broad by
    // construction, so refusing here would reject working authority configurations for no gain.
    String policy =
        StorageAuthorityResolver.scopedSessionPolicy(
            java.util.List.of("s3://warehouse/orders/data/part-*.parquet"));
    assertTrue(policy.contains("arn:aws:s3:::warehouse/orders/data/part-*.parquet"));
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

  @Test
  void scopedSessionPolicyCanonicalizesScopeOrderAndSchemes() {
    String first =
        StorageAuthorityResolver.scopedSessionPolicy(
            java.util.List.of("s3://warehouse/two/", "s3://warehouse/one"));
    String second =
        StorageAuthorityResolver.scopedSessionPolicy(
            java.util.List.of("S3A://WAREHOUSE/one/", "s3n://warehouse/two"));

    assertEquals(first, second);
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
