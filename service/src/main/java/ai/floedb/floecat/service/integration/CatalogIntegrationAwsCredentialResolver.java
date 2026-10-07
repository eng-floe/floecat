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

import ai.floedb.floecat.integration.rpc.AwsSigV4Authentication;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Clock;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiFunction;
import java.util.function.IntConsumer;
import java.util.function.Supplier;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.Credentials;
import software.amazon.awssdk.services.sts.model.StsException;

/** Resolves renewable AWS credential sources configured on Catalog Integrations. */
@ApplicationScoped
class CatalogIntegrationAwsCredentialResolver {
  private static final Duration CACHE_REFRESH_SKEW = Duration.ofMinutes(5);
  private static final int MAX_ATTEMPTS = 3;
  private static final long RETRY_BASE_MILLIS = 50L;
  private static final int CACHE_MAX_ENTRIES = 1024;
  private static final Duration CACHE_WAIT_TIMEOUT = Duration.ofSeconds(30);

  private final Object cacheLock = new Object();
  private final LinkedHashMap<AssumeRoleCacheKey, CompletableFuture<ResolvedAwsCredentials>> cache =
      new LinkedHashMap<>(16, 0.75f, true);
  private final int cacheMaxEntries;
  private final Duration cacheWaitTimeout;
  private volatile DefaultCredentialsProvider ambientProvider;

  Supplier<DefaultCredentialsProvider> ambientProviderFactory =
      () -> DefaultCredentialsProvider.builder().build();
  Supplier<AwsCredentials> defaultCredentials = this::resolveDefaultCredentials;
  BiFunction<String, AssumeRoleRequest, Credentials> assumeRole = this::assumeRole;
  IntConsumer retryPause = CatalogIntegrationAwsCredentialResolver::pauseBeforeRetry;
  Clock clock = Clock.systemUTC();

  CatalogIntegrationAwsCredentialResolver() {
    this(CACHE_MAX_ENTRIES, CACHE_WAIT_TIMEOUT);
  }

  CatalogIntegrationAwsCredentialResolver(int cacheMaxEntries, Duration cacheWaitTimeout) {
    this.cacheMaxEntries = Math.max(1, cacheMaxEntries);
    this.cacheWaitTimeout =
        cacheWaitTimeout == null || cacheWaitTimeout.isNegative() || cacheWaitTimeout.isZero()
            ? CACHE_WAIT_TIMEOUT
            : cacheWaitTimeout;
  }

  ResolvedAwsCredentials resolve(AwsSigV4Authentication authentication) {
    return switch (authentication.getCredentialsCase()) {
      case AWS_DEFAULT -> fromDefault(defaultCredentials.get());
      case AWS_ASSUME_ROLE -> cachedAssumeRole(authentication);
      case AWS_ACCESS_KEY, CREDENTIALS_NOT_SET ->
          throw new IllegalArgumentException("AWS credential source is not renewable");
    };
  }

  private ResolvedAwsCredentials cachedAssumeRole(AwsSigV4Authentication authentication) {
    String region = requireNonBlank(authentication.getRegion(), "region");
    AssumeRoleRequest request = assumeRoleRequest(authentication.getAwsAssumeRole());
    AssumeRoleCacheKey key = AssumeRoleCacheKey.of(region, request);
    for (; ; ) {
      CompletableFuture<ResolvedAwsCredentials> existing;
      synchronized (cacheLock) {
        existing = cache.get(key);
      }
      if (existing != null) {
        try {
          ResolvedAwsCredentials credentials =
              existing.get(
                  CatalogUpstreamBudget.currentRemainingNanos(cacheWaitTimeout.toNanos()),
                  TimeUnit.NANOSECONDS);
          if (isFresh(credentials)) return credentials;
        } catch (InterruptedException failure) {
          Thread.currentThread().interrupt();
          throw new java.util.concurrent.CancellationException(
              "AWS credential resolution was cancelled");
        } catch (TimeoutException failure) {
          throw new CredentialWaitTimeoutException(failure);
        } catch (CancellationException failure) {
          // The thread that owned this fetch was cancelled. That cancellation belongs only to its
          // caller; discard the failed shared attempt so this waiter can become the next owner.
          remove(key, existing);
          continue;
        } catch (ExecutionException failure) {
          remove(key, existing);
          throw propagate(failure.getCause());
        }
        remove(key, existing);
        continue;
      }

      CompletableFuture<ResolvedAwsCredentials> created = new CompletableFuture<>();
      boolean inserted = false;
      synchronized (cacheLock) {
        existing = cache.get(key);
        if (existing == null) {
          evictCompletedEntries(cacheMaxEntries - 1);
          if (cache.size() < cacheMaxEntries) {
            cache.put(key, created);
            inserted = true;
          }
        }
      }
      if (existing != null) continue;
      if (!inserted) return assumeRoleWithRetry(region, request);
      try {
        ResolvedAwsCredentials credentials = assumeRoleWithRetry(region, request);
        created.complete(credentials);
        if (!isFresh(credentials)) remove(key, created);
        return credentials;
      } catch (Throwable failure) {
        created.completeExceptionally(failure);
        remove(key, created);
        throw propagate(failure);
      }
    }
  }

  private ResolvedAwsCredentials assumeRoleWithRetry(String region, AssumeRoleRequest request) {
    RuntimeException lastFailure = null;
    for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      throwIfInterrupted();
      try {
        return fromAssumed(assumeRole.apply(region, request));
      } catch (RuntimeException failure) {
        throwIfInterrupted();
        if (!retryable(failure) || attempt == MAX_ATTEMPTS) throw failure;
        lastFailure = failure;
        retryPause.accept(attempt);
        throwIfInterrupted();
      }
    }
    throw lastFailure;
  }

  private static void throwIfInterrupted() {
    if (Thread.currentThread().isInterrupted()) {
      throw new CancellationException("AWS credential resolution was cancelled");
    }
  }

  private boolean isFresh(ResolvedAwsCredentials credentials) {
    return credentials != null
        && credentials.expiresAt() != null
        && clock.instant().plus(CACHE_REFRESH_SKEW).isBefore(credentials.expiresAt());
  }

  private void remove(AssumeRoleCacheKey key, CompletableFuture<ResolvedAwsCredentials> expected) {
    synchronized (cacheLock) {
      cache.remove(key, expected);
    }
  }

  private void evictCompletedEntries(int targetSize) {
    if (cache.size() <= targetSize) return;
    var iterator = cache.entrySet().iterator();
    while (cache.size() > targetSize && iterator.hasNext()) {
      if (iterator.next().getValue().isDone()) iterator.remove();
    }
  }

  private static boolean retryable(Throwable failure) {
    for (Throwable current = failure; current != null; current = current.getCause()) {
      if (current instanceof MissingAwsCredentialsException) return false;
      if (current instanceof SdkClientException) return true;
      if (current instanceof StsException sts
          && (CatalogIntegrationAccess.isStsThrottling(sts)
              || sts.statusCode() == 429
              || sts.statusCode() >= 500)) return true;
    }
    return false;
  }

  private static void pauseBeforeRetry(int failedAttempt) {
    long upperBound = RETRY_BASE_MILLIS << Math.max(0, failedAttempt - 1);
    long delayMillis =
        ThreadLocalRandom.current().nextLong(Math.max(1L, upperBound / 2), upperBound + 1);
    LockSupport.parkNanos(Duration.ofMillis(delayMillis).toNanos());
  }

  private static RuntimeException propagate(Throwable failure) {
    if (failure instanceof RuntimeException runtime) return runtime;
    if (failure instanceof Error fatal) throw fatal;
    return new RuntimeException(failure);
  }

  static AssumeRoleRequest assumeRoleRequest(
      ai.floedb.floecat.integration.rpc.AwsAssumeRoleAuthentication configured) {
    String roleArn = requireNonBlank(configured.getRoleArn(), "role_arn");
    String sessionName =
        configured.hasRoleSessionName() && !configured.getRoleSessionName().isBlank()
            ? configured.getRoleSessionName().trim()
            : "floecat-catalog-integration";
    return AssumeRoleRequest.builder()
        .roleArn(roleArn)
        .roleSessionName(sessionName)
        .externalId(
            configured.hasExternalId() && !configured.getExternalId().isBlank()
                ? configured.getExternalId().trim()
                : null)
        .build();
  }

  private AwsCredentials resolveDefaultCredentials() {
    try {
      return ambientCredentialsProvider().resolveCredentials();
    } catch (SdkClientException failure) {
      // The SDK collapses an exhausted chain and temporary IMDS/ECS lookup failures into the same
      // exception shape. Treat it as missing deployment credentials: retrying every open cannot
      // reliably distinguish or repair the latter and would make a bad deployment look transient.
      throw new MissingAwsCredentialsException(failure);
    }
  }

  private AwsCredentialsProvider ambientCredentialsProvider() {
    DefaultCredentialsProvider provider = ambientProvider;
    if (provider != null) return provider;
    synchronized (this) {
      if (ambientProvider == null) ambientProvider = ambientProviderFactory.get();
      return ambientProvider;
    }
  }

  @PreDestroy
  void close() {
    DefaultCredentialsProvider provider = ambientProvider;
    ambientProvider = null;
    if (provider != null) provider.close();
  }

  private Credentials assumeRole(String region, AssumeRoleRequest request) {
    var provider = StaticCredentialsProvider.create(resolveDefaultCredentials());
    try (var sts =
        StsClient.builder().credentialsProvider(provider).region(Region.of(region)).build()) {
      return sts.assumeRole(request).credentials();
    }
  }

  private static ResolvedAwsCredentials fromDefault(AwsCredentials credentials) {
    if (credentials == null) {
      throw new IllegalStateException("AWS default credential chain returned no credentials");
    }
    return new ResolvedAwsCredentials(
        requireNonBlank(credentials.accessKeyId(), "access_key_id"),
        requireNonBlank(credentials.secretAccessKey(), "secret_access_key"),
        credentials instanceof AwsSessionCredentials session ? session.sessionToken() : null,
        credentials instanceof AwsSessionCredentials session
            ? session.expirationTime().orElse(null)
            : null);
  }

  private static ResolvedAwsCredentials fromAssumed(Credentials credentials) {
    if (credentials == null) {
      throw new IllegalStateException("AWS STS AssumeRole returned no credentials");
    }
    return new ResolvedAwsCredentials(
        requireNonBlank(credentials.accessKeyId(), "access_key_id"),
        requireNonBlank(credentials.secretAccessKey(), "secret_access_key"),
        requireNonBlank(credentials.sessionToken(), "session_token"),
        credentials.expiration());
  }

  private static String requireNonBlank(String value, String field) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("AWS Catalog Integration " + field + " must be non-blank");
    }
    return value.trim();
  }

  private record AssumeRoleCacheKey(
      String region, String roleArn, String externalId, String roleSessionName) {
    private static AssumeRoleCacheKey of(String region, AssumeRoleRequest request) {
      return new AssumeRoleCacheKey(
          region, request.roleArn(), request.externalId(), request.roleSessionName());
    }
  }

  static final class MissingAwsCredentialsException extends RuntimeException {
    MissingAwsCredentialsException(Throwable cause) {
      super("AWS default credential chain returned no credentials", cause);
    }
  }

  static final class CredentialWaitTimeoutException extends RuntimeException {
    CredentialWaitTimeoutException(Throwable cause) {
      super("Timed out waiting for AWS credential resolution", cause);
    }
  }

  int cacheSize() {
    synchronized (cacheLock) {
      return cache.size();
    }
  }
}
