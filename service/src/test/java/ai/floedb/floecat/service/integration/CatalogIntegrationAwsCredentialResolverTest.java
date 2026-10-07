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

package ai.floedb.floecat.service.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.integration.rpc.AwsAssumeRoleAuthentication;
import ai.floedb.floecat.integration.rpc.AwsDefaultAuthentication;
import ai.floedb.floecat.integration.rpc.AwsSigV4Authentication;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

class CatalogIntegrationAwsCredentialResolverTest {
  @Test
  void resolvesTheAmbientAwsChainWithoutAnyCatalogFormatDependency() {
    var resolver = new CatalogIntegrationAwsCredentialResolver();
    resolver.defaultCredentials = () -> AwsSessionCredentials.create("access", "secret", "session");
    var authentication =
        AwsSigV4Authentication.newBuilder()
            .setAwsDefault(AwsDefaultAuthentication.getDefaultInstance())
            .setRegion("us-east-1")
            .build();

    ResolvedAwsCredentials resolved = resolver.resolve("account", authentication);

    assertEquals("access", resolved.accessKeyId());
    assertEquals("secret", resolved.secretAccessKey());
    assertEquals("session", resolved.sessionToken());
    assertNull(resolved.expiresAt());
  }

  @Test
  void assumesTheConfiguredRoleUsingTheSigV4Region() {
    var resolver = new CatalogIntegrationAwsCredentialResolver();
    AtomicReference<String> stsRegion = new AtomicReference<>();
    AtomicReference<AssumeRoleRequest> request = new AtomicReference<>();
    Instant now = Instant.parse("2026-10-07T14:00:00Z");
    Instant expiration = now.plus(Duration.ofHours(1));
    resolver.clock = Clock.fixed(now, ZoneOffset.UTC);
    resolver.assumeRole =
        (region, configured) -> {
          stsRegion.set(region);
          request.set(configured);
          return Credentials.builder()
              .accessKeyId("access")
              .secretAccessKey("secret")
              .sessionToken("session")
              .expiration(expiration)
              .build();
        };
    var authentication =
        AwsSigV4Authentication.newBuilder()
            .setAwsAssumeRole(
                AwsAssumeRoleAuthentication.newBuilder()
                    .setRoleArn("arn:aws:iam::123456789012:role/catalog")
                    .setExternalId("external")
                    .setRoleSessionName("catalog-session"))
            .setRegion("us-east-1")
            .setSigningName("glue")
            .build();

    ResolvedAwsCredentials resolved = resolver.resolve("account", authentication);

    assertEquals("us-east-1", stsRegion.get());
    assertEquals("arn:aws:iam::123456789012:role/catalog", request.get().roleArn());
    assertEquals("external", request.get().externalId());
    assertEquals("catalog-session", request.get().roleSessionName());
    assertEquals("access", resolved.accessKeyId());
    assertEquals("session", resolved.sessionToken());
    assertEquals(expiration, resolved.expiresAt());
  }

  @Test
  void usesTheSigV4RegionForStsWhenNoRoutingRegionIsConfigured() {
    var resolver = new CatalogIntegrationAwsCredentialResolver();
    AtomicReference<String> stsRegion = new AtomicReference<>();
    resolver.assumeRole =
        (region, configured) -> {
          stsRegion.set(region);
          return Credentials.builder()
              .accessKeyId("access")
              .secretAccessKey("secret")
              .sessionToken("session")
              .build();
        };
    var authentication =
        AwsSigV4Authentication.newBuilder()
            .setAwsAssumeRole(
                AwsAssumeRoleAuthentication.newBuilder()
                    .setRoleArn("arn:aws:iam::123456789012:role/catalog"))
            .setRegion("eu-central-1")
            .build();

    resolver.resolve("account", authentication);

    assertEquals("eu-central-1", stsRegion.get());
  }

  @Test
  void reusesFreshAssumeRoleCredentialsAcrossOpenPaths() {
    var resolver = new CatalogIntegrationAwsCredentialResolver();
    Instant now = Instant.parse("2026-10-07T14:00:00Z");
    resolver.clock = Clock.fixed(now, ZoneOffset.UTC);
    AtomicInteger calls = new AtomicInteger();
    resolver.assumeRole =
        (region, configured) -> {
          calls.incrementAndGet();
          return Credentials.builder()
              .accessKeyId("access")
              .secretAccessKey("secret")
              .sessionToken("session")
              .expiration(now.plus(Duration.ofHours(1)))
              .build();
        };
    var authentication =
        AwsSigV4Authentication.newBuilder()
            .setAwsAssumeRole(
                AwsAssumeRoleAuthentication.newBuilder()
                    .setRoleArn("arn:aws:iam::123456789012:role/catalog"))
            .setRegion("us-east-1")
            .build();

    resolver.resolve("account", authentication);
    resolver.resolve("account", authentication);

    assertEquals(1, calls.get());
  }

  @Test
  void refreshesCachedCredentialsBeforeTheyExpire() {
    var resolver = new CatalogIntegrationAwsCredentialResolver();
    Instant now = Instant.parse("2026-10-07T14:00:00Z");
    resolver.clock = Clock.fixed(now, ZoneOffset.UTC);
    AtomicInteger calls = new AtomicInteger();
    resolver.assumeRole =
        (region, configured) ->
            Credentials.builder()
                .accessKeyId("access-" + calls.incrementAndGet())
                .secretAccessKey("secret")
                .sessionToken("session")
                .expiration(now.plus(Duration.ofHours(calls.get())))
                .build();
    var authentication =
        AwsSigV4Authentication.newBuilder()
            .setAwsAssumeRole(
                AwsAssumeRoleAuthentication.newBuilder()
                    .setRoleArn("arn:aws:iam::123456789012:role/catalog"))
            .setRegion("us-east-1")
            .build();

    resolver.resolve("account", authentication);
    resolver.clock = Clock.fixed(now.plus(Duration.ofMinutes(56)), ZoneOffset.UTC);
    ResolvedAwsCredentials refreshed = resolver.resolve("account", authentication);

    assertEquals(2, calls.get());
    assertEquals("access-2", refreshed.accessKeyId());
  }

  @Test
  void doesNotStackRetriesAroundTheSdkClient() {
    var resolver = new CatalogIntegrationAwsCredentialResolver();
    Instant now = Instant.parse("2026-10-07T14:00:00Z");
    resolver.clock = Clock.fixed(now, ZoneOffset.UTC);
    AtomicInteger calls = new AtomicInteger();
    resolver.assumeRole =
        (region, configured) -> {
          calls.incrementAndGet();
          throw software.amazon.awssdk.services.sts.model.StsException.builder()
              .statusCode(400)
              .awsErrorDetails(
                  software.amazon.awssdk.awscore.exception.AwsErrorDetails.builder()
                      .errorCode("Throttling")
                      .build())
              .build();
        };
    var authentication =
        AwsSigV4Authentication.newBuilder()
            .setAwsAssumeRole(
                AwsAssumeRoleAuthentication.newBuilder()
                    .setRoleArn("arn:aws:iam::123456789012:role/catalog"))
            .setRegion("us-east-1")
            .build();

    assertThrows(
        software.amazon.awssdk.services.sts.model.StsException.class,
        () -> resolver.resolve("account", authentication));

    assertEquals(1, calls.get());
  }

  @Test
  void configuresOneStandardSdkRetryLayerAndTheActiveCallTimeout() {
    var resolver = new CatalogIntegrationAwsCredentialResolver();
    resolver.nanoTime = () -> 0L;
    resolver.defaultCredentials =
        () -> AwsSessionCredentials.create("source-access", "source-secret", "source-session");
    var sts = mock(StsClient.class);
    Instant expiration = Instant.parse("2026-10-07T15:00:00Z");
    when(sts.assumeRole(org.mockito.ArgumentMatchers.any(AssumeRoleRequest.class)))
        .thenReturn(
            AssumeRoleResponse.builder().credentials(credentials("access", expiration)).build());
    AtomicReference<Duration> timeout = new AtomicReference<>();
    resolver.stsClientFactory =
        (region, sourceCredentials, callTimeout) -> {
          assertEquals("us-east-1", region);
          assertEquals("source-access", sourceCredentials.accessKeyId());
          timeout.set(callTimeout);
          return sts;
        };

    ResolvedAwsCredentials resolved =
        CatalogUpstreamBudget.start(Duration.ofSeconds(7), () -> 0L)
            .call(() -> resolver.resolve("account", assumeRoleAuthentication("catalog")));

    assertEquals("access", resolved.accessKeyId());
    assertEquals(Duration.ofSeconds(7), timeout.get());
    assertEquals(
        java.util.Optional.of(RetryMode.STANDARD),
        CatalogIntegrationAwsCredentialResolver.stsOverrideConfiguration(Duration.ofSeconds(7))
            .retryMode());
    verify(sts).assumeRole(org.mockito.ArgumentMatchers.any(AssumeRoleRequest.class));
    verify(sts).close();
  }

  @Test
  void boundsAmbientCredentialResolutionBeforeCreatingTheStsClient() {
    var resolver = new CatalogIntegrationAwsCredentialResolver(2, Duration.ofMillis(20));
    CountDownLatch release = new CountDownLatch(1);
    resolver.defaultCredentials =
        () -> {
          await(release);
          return AwsSessionCredentials.create("access", "secret", "session");
        };
    AtomicInteger clients = new AtomicInteger();
    resolver.stsClientFactory =
        (region, sourceCredentials, callTimeout) -> {
          clients.incrementAndGet();
          return mock(StsClient.class);
        };

    try {
      assertThrows(
          CatalogIntegrationAwsCredentialResolver.CredentialSourceTimeoutException.class,
          () -> resolver.resolve("account", assumeRoleAuthentication("catalog")));
      assertEquals(0, clients.get());
    } finally {
      release.countDown();
    }
  }

  @Test
  void doesNotCacheCallerSpecificApiCallTimeouts() {
    var resolver = new CatalogIntegrationAwsCredentialResolver();
    AtomicInteger calls = new AtomicInteger();
    resolver.assumeRole =
        (region, configured) -> {
          calls.incrementAndGet();
          throw ApiCallTimeoutException.create(1);
        };
    AwsSigV4Authentication authentication = assumeRoleAuthentication("catalog");

    assertThrows(ApiCallTimeoutException.class, () -> resolver.resolve("account", authentication));
    assertThrows(ApiCallTimeoutException.class, () -> resolver.resolve("account", authentication));

    assertEquals(2, calls.get());
  }

  @Test
  void concurrentMissesCoalesceIntoOneStsCall() throws Exception {
    var resolver = new CatalogIntegrationAwsCredentialResolver();
    Instant now = Instant.parse("2026-10-07T14:00:00Z");
    resolver.clock = Clock.fixed(now, ZoneOffset.UTC);
    AtomicInteger calls = new AtomicInteger();
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    resolver.assumeRole =
        (region, configured) -> {
          calls.incrementAndGet();
          entered.countDown();
          await(release);
          return credentials("access", now.plus(Duration.ofHours(1)));
        };
    AwsSigV4Authentication authentication = assumeRoleAuthentication("catalog");

    CompletableFuture<ResolvedAwsCredentials> first = asyncResolve(resolver, authentication);
    assertTrue(entered.await(2, TimeUnit.SECONDS));
    CompletableFuture<ResolvedAwsCredentials> second = asyncResolve(resolver, authentication);
    Thread.sleep(20);
    assertFalse(second.isDone());
    release.countDown();

    assertEquals("access", first.get(2, TimeUnit.SECONDS).accessKeyId());
    assertEquals("access", second.get(2, TimeUnit.SECONDS).accessKeyId());
    assertEquals(1, calls.get());
  }

  @Test
  void waitersReceiveOwnerFailureAndNewOpensBackOffBeforeRetrying() throws Exception {
    var resolver = new CatalogIntegrationAwsCredentialResolver();
    Instant now = Instant.parse("2026-10-07T14:00:00Z");
    resolver.clock = Clock.fixed(now, ZoneOffset.UTC);
    AtomicInteger calls = new AtomicInteger();
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    var ownerFailure =
        software.amazon.awssdk.services.sts.model.StsException.builder()
            .statusCode(403)
            .message("denied")
            .build();
    resolver.assumeRole =
        (region, configured) -> {
          if (calls.incrementAndGet() == 1) {
            entered.countDown();
            await(release);
            throw ownerFailure;
          }
          throw software.amazon.awssdk.services.sts.model.StsException.builder()
              .statusCode(403)
              .message("denied again")
              .build();
        };
    AwsSigV4Authentication authentication = assumeRoleAuthentication("catalog");

    CompletableFuture<ResolvedAwsCredentials> first = asyncResolve(resolver, authentication);
    assertTrue(entered.await(2, TimeUnit.SECONDS));
    CompletableFuture<ResolvedAwsCredentials> second = asyncResolve(resolver, authentication);
    Thread.sleep(20);
    assertFalse(second.isDone());
    release.countDown();

    assertSame(ownerFailure, assertThrows(CompletionException.class, first::join).getCause());
    assertSame(ownerFailure, assertThrows(CompletionException.class, second::join).getCause());
    assertThrows(
        software.amazon.awssdk.services.sts.model.StsException.class,
        () -> resolver.resolve("account", authentication));
    assertEquals(1, calls.get());

    resolver.clock = Clock.fixed(now.plusSeconds(5), ZoneOffset.UTC);
    assertThrows(
        software.amazon.awssdk.services.sts.model.StsException.class,
        () -> resolver.resolve("account", authentication));
    assertEquals(2, calls.get());
  }

  @Test
  void cacheRemainsBoundedWhenAllEntriesAreInFlight() throws Exception {
    var resolver = new CatalogIntegrationAwsCredentialResolver(2, Duration.ofSeconds(2));
    Instant now = Instant.parse("2026-10-07T14:00:00Z");
    resolver.clock = Clock.fixed(now, ZoneOffset.UTC);
    CountDownLatch entered = new CountDownLatch(3);
    CountDownLatch release = new CountDownLatch(1);
    resolver.assumeRole =
        (region, configured) -> {
          entered.countDown();
          await(release);
          return credentials(configured.roleArn(), now.plus(Duration.ofHours(1)));
        };

    var first = asyncResolve(resolver, assumeRoleAuthentication("one"));
    var second = asyncResolve(resolver, assumeRoleAuthentication("two"));
    var third = asyncResolve(resolver, assumeRoleAuthentication("three"));
    assertTrue(entered.await(2, TimeUnit.SECONDS));

    assertEquals(2, resolver.cacheSize());
    release.countDown();
    CompletableFuture.allOf(first, second, third).get(2, TimeUnit.SECONDS);
    assertEquals(2, resolver.cacheSize());
  }

  @Test
  void waiterHonorsTimeoutWithoutRemovingOwnersEntry() throws Exception {
    var resolver = new CatalogIntegrationAwsCredentialResolver(2, Duration.ofMillis(20));
    Instant now = Instant.parse("2026-10-07T14:00:00Z");
    resolver.clock = Clock.fixed(now, ZoneOffset.UTC);
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    resolver.assumeRole =
        (region, configured) -> {
          entered.countDown();
          await(release);
          return credentials("access", now.plus(Duration.ofHours(1)));
        };
    AwsSigV4Authentication authentication = assumeRoleAuthentication("catalog");
    var owner = asyncResolve(resolver, authentication);
    assertTrue(entered.await(2, TimeUnit.SECONDS));

    assertThrows(
        CatalogIntegrationAwsCredentialResolver.CredentialWaitTimeoutException.class,
        () -> resolver.resolve("account", authentication));
    assertEquals(1, resolver.cacheSize());

    release.countDown();
    owner.get(2, TimeUnit.SECONDS);
  }

  @Test
  void missingSourceCredentialsAreNotRetried() {
    var resolver = new CatalogIntegrationAwsCredentialResolver();
    AtomicInteger calls = new AtomicInteger();
    resolver.assumeRole =
        (region, configured) -> {
          calls.incrementAndGet();
          throw new CatalogIntegrationAwsCredentialResolver.MissingAwsCredentialsException(
              SdkClientException.create("no source credentials"));
        };

    assertThrows(
        CatalogIntegrationAwsCredentialResolver.MissingAwsCredentialsException.class,
        () -> resolver.resolve("account", assumeRoleAuthentication("catalog")));
    assertEquals(1, calls.get());
  }

  @Test
  void interruptedOwnerStopsBeforeCallingSts() {
    var resolver = new CatalogIntegrationAwsCredentialResolver();
    AtomicInteger calls = new AtomicInteger();
    resolver.assumeRole =
        (region, configured) -> {
          calls.incrementAndGet();
          return credentials("unused", Instant.now());
        };

    try {
      Thread.currentThread().interrupt();
      assertThrows(
          CancellationException.class,
          () -> resolver.resolve("account", assumeRoleAuthentication("catalog")));
      assertEquals(0, calls.get());
      assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void cancelledOwnerDoesNotCancelWaiterForTheSameRole() throws Exception {
    var resolver = new CatalogIntegrationAwsCredentialResolver();
    Instant now = Instant.parse("2026-10-07T14:00:00Z");
    resolver.clock = Clock.fixed(now, ZoneOffset.UTC);
    AtomicInteger calls = new AtomicInteger();
    AtomicReference<Thread> ownerThread = new AtomicReference<>();
    CountDownLatch ownerEntered = new CountDownLatch(1);
    resolver.assumeRole =
        (region, configured) -> {
          if (calls.incrementAndGet() == 1) {
            ownerThread.set(Thread.currentThread());
            ownerEntered.countDown();
            try {
              new CountDownLatch(1).await();
            } catch (InterruptedException failure) {
              Thread.currentThread().interrupt();
              throw SdkClientException.create("owner cancelled", failure);
            }
          }
          return credentials("waiter-access", now.plus(Duration.ofHours(1)));
        };
    AwsSigV4Authentication authentication = assumeRoleAuthentication("catalog");

    CompletableFuture<ResolvedAwsCredentials> owner = asyncResolve(resolver, authentication);
    assertTrue(ownerEntered.await(2, TimeUnit.SECONDS));
    AtomicReference<Thread> waiterThread = new AtomicReference<>();
    CompletableFuture<ResolvedAwsCredentials> waiter =
        asyncResolve(resolver, authentication, waiterThread);
    assertTrue(awaitBlocked(waiterThread));
    assertFalse(waiter.isDone());

    ownerThread.get().interrupt();

    assertThrows(CancellationException.class, owner::join);
    assertEquals("waiter-access", waiter.get(2, TimeUnit.SECONDS).accessKeyId());
    assertEquals(2, calls.get());
  }

  @Test
  void waiterUsesTheActiveUpstreamBudgetInsteadOfTheStandaloneTimeout() throws Exception {
    var resolver = new CatalogIntegrationAwsCredentialResolver(2, Duration.ofSeconds(5));
    Instant now = Instant.parse("2026-10-07T14:00:00Z");
    resolver.clock = Clock.fixed(now, ZoneOffset.UTC);
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    resolver.assumeRole =
        (region, configured) -> {
          entered.countDown();
          await(release);
          return credentials("access", now.plus(Duration.ofHours(1)));
        };
    AwsSigV4Authentication authentication = assumeRoleAuthentication("catalog");
    var owner = asyncResolve(resolver, authentication);
    assertTrue(entered.await(2, TimeUnit.SECONDS));
    AtomicInteger budgetClockReads = new AtomicInteger();

    try {
      assertThrows(
          CatalogIntegrationAwsCredentialResolver.CredentialWaitTimeoutException.class,
          () ->
              CatalogUpstreamBudget.start(
                      Duration.ofSeconds(1),
                      () ->
                          budgetClockReads.getAndIncrement() < 2
                              ? 0L
                              : Duration.ofMillis(900).toNanos())
                  .call(() -> resolver.resolve("account", authentication)));
    } finally {
      release.countDown();
    }
    owner.get(2, TimeUnit.SECONDS);
  }

  @Test
  void reusesAndClosesTheAmbientProvider() {
    var resolver = new CatalogIntegrationAwsCredentialResolver();
    var provider = mock(DefaultCredentialsProvider.class);
    AtomicInteger builds = new AtomicInteger();
    resolver.ambientProviderFactory =
        () -> {
          builds.incrementAndGet();
          return provider;
        };
    when(provider.resolveCredentials())
        .thenReturn(AwsSessionCredentials.create("access", "secret", "session"));
    var authentication =
        AwsSigV4Authentication.newBuilder()
            .setAwsDefault(AwsDefaultAuthentication.getDefaultInstance())
            .setRegion("us-east-1")
            .build();

    resolver.resolve("account", authentication);
    resolver.resolve("account", authentication);
    resolver.close();

    assertEquals(1, builds.get());
    verify(provider).close();
  }

  @Test
  void doesNotShareAssumeRoleCredentialsAcrossAccounts() {
    var resolver = new CatalogIntegrationAwsCredentialResolver();
    Instant now = Instant.parse("2026-10-07T14:00:00Z");
    resolver.clock = Clock.fixed(now, ZoneOffset.UTC);
    AtomicInteger calls = new AtomicInteger();
    resolver.assumeRole =
        (region, configured) ->
            credentials("account-" + calls.incrementAndGet(), now.plus(Duration.ofHours(1)));
    AwsSigV4Authentication authentication = assumeRoleAuthentication("catalog");

    ResolvedAwsCredentials first = resolver.resolve("account-one", authentication);
    ResolvedAwsCredentials second = resolver.resolve("account-two", authentication);

    assertEquals("account-1", first.accessKeyId());
    assertEquals("account-2", second.accessKeyId());
    assertEquals(2, calls.get());
  }

  private static AwsSigV4Authentication assumeRoleAuthentication(String roleName) {
    return AwsSigV4Authentication.newBuilder()
        .setAwsAssumeRole(
            AwsAssumeRoleAuthentication.newBuilder()
                .setRoleArn("arn:aws:iam::123456789012:role/" + roleName))
        .setRegion("us-east-1")
        .build();
  }

  private static Credentials credentials(String accessKeyId, Instant expiration) {
    return Credentials.builder()
        .accessKeyId(accessKeyId)
        .secretAccessKey("secret")
        .sessionToken("session")
        .expiration(expiration)
        .build();
  }

  private static CompletableFuture<ResolvedAwsCredentials> asyncResolve(
      CatalogIntegrationAwsCredentialResolver resolver, AwsSigV4Authentication authentication) {
    return asyncResolve(resolver, authentication, new AtomicReference<>());
  }

  private static CompletableFuture<ResolvedAwsCredentials> asyncResolve(
      CatalogIntegrationAwsCredentialResolver resolver,
      AwsSigV4Authentication authentication,
      AtomicReference<Thread> resolvingThread) {
    var result = new CompletableFuture<ResolvedAwsCredentials>();
    Thread.ofVirtual()
        .start(
            () -> {
              resolvingThread.set(Thread.currentThread());
              try {
                result.complete(resolver.resolve("account", authentication));
              } catch (Throwable failure) {
                result.completeExceptionally(failure);
              }
            });
    return result;
  }

  private static boolean awaitBlocked(AtomicReference<Thread> thread) throws InterruptedException {
    long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
    while (System.nanoTime() < deadline) {
      Thread current = thread.get();
      if (current != null
          && (current.getState() == Thread.State.WAITING
              || current.getState() == Thread.State.TIMED_WAITING)) return true;
      Thread.sleep(1);
    }
    return false;
  }

  private static void await(CountDownLatch latch) {
    try {
      if (!latch.await(2, TimeUnit.SECONDS)) throw new AssertionError("timed out waiting for test");
    } catch (InterruptedException failure) {
      Thread.currentThread().interrupt();
      throw new AssertionError(failure);
    }
  }
}
