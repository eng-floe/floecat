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

import ai.floedb.floecat.aws.RefreshingAwsClient;
import ai.floedb.floecat.connector.common.auth.CredentialResolverSupport;
import ai.floedb.floecat.connector.common.auth.ResolvedStorageCredentials;
import ai.floedb.floecat.connector.rpc.AuthCredentials;
import ai.floedb.floecat.storage.rpc.ResolveStorageAuthorityResponse;
import ai.floedb.floecat.storage.rpc.StorageAuthority;
import ai.floedb.floecat.storage.rpc.VendedStorageCredential;
import ai.floedb.floecat.storage.secrets.SecretsManager;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.Credentials;

@ApplicationScoped
public class StorageAuthorityResolver {
  static final String STORAGE_AUTHORITY_SECRET_TYPE = "storage-authorities";
  private static final Logger LOG = Logger.getLogger(StorageAuthorityResolver.class.getName());
  private static final Duration ASSUME_ROLE_CACHE_REFRESH_SKEW = Duration.ofMinutes(5);
  private static final int ASSUME_ROLE_MAX_ATTEMPTS = 3;
  private static final long ASSUME_ROLE_RETRY_BASE_MILLIS = 50L;
  private static final int DEFAULT_ASSUME_ROLE_CACHE_MAX_ENTRIES = 1024;

  private final Object assumeRoleCacheLock = new Object();
  private final LinkedHashMap<AssumeRoleCacheKey, CompletableFuture<ResolvedStorageCredentials>>
      assumeRoleCache = new LinkedHashMap<>(16, 0.75f, true);
  private final int assumeRoleCacheMaxEntries;

  @Inject SecretsManager secretsManager;

  public StorageAuthorityResolver() {
    this(DEFAULT_ASSUME_ROLE_CACHE_MAX_ENTRIES);
  }

  StorageAuthorityResolver(int assumeRoleCacheMaxEntries) {
    this.assumeRoleCacheMaxEntries = Math.max(1, assumeRoleCacheMaxEntries);
  }

  ResolveStorageAuthorityResponse buildResponse(
      StorageAuthority authority,
      String locationPrefix,
      List<String> sessionScopeLocations,
      String accountId,
      boolean serverSide) {
    return buildResponse(
        authority, locationPrefix, sessionScopeLocations, accountId, serverSide, false);
  }

  ResolveStorageAuthorityResponse buildResponse(
      StorageAuthority authority,
      String locationPrefix,
      List<String> sessionScopeLocations,
      String accountId,
      boolean serverSide,
      boolean exactObjectScope) {
    if (authority == null) {
      // Carries a structured reason rather than a bare IllegalArgumentException. Both map to
      // INVALID_ARGUMENT, but so do account_id, execution_binding and location_prefix validation
      // failures -- and a delegating connector must fall back to its catalog for *this* condition
      // only, not for every request error.
      throw ai.floedb.floecat.storage.errors.SourceCatalogVendingGrpcStatus
          .noMatchingStorageAuthority(
              "Credential vending was requested but no storage credential authority is configured"
                  + " for this table");
    }

    ResolveStorageAuthorityResponse.Builder response =
        ResolveStorageAuthorityResponse.newBuilder().setAuthorityId(authority.getResourceId());
    response.putAllClientSafeConfig(clientSafeConfig(authority));
    AuthCredentials authoritySecret =
        resolveAuthoritySecret(accountId, authority.getResourceId().getId()).orElse(null);
    ResolvedStorageCredentials resolved;
    if (!serverSide) {
      resolved =
          mintTemporaryCredentials(
              authority, authoritySecret, sessionScopeLocations, exactObjectScope);
      if (!resolved.hasKnownExpiry()) {
        throw new IllegalArgumentException(
            "Credential vending requires credentials with a known expiry minted from a storage authority role");
      }
    } else {
      resolved =
          resolveServerSideCredentials(
              authority, authoritySecret, sessionScopeLocations, exactObjectScope);
    }

    LinkedHashMap<String, String> storageConfig = new LinkedHashMap<>();
    storageConfig.put("type", authority.getType().isBlank() ? "s3" : authority.getType());
    storageConfig.putAll(clientSafeConfig(authority));
    storageConfig.putAll(resolved.asS3Properties());
    VendedStorageCredential.Builder credential =
        VendedStorageCredential.newBuilder()
            .setPrefix(locationPrefix)
            .putAllConfig(Map.copyOf(storageConfig));
    if (resolved.expiresAt() != null) {
      credential.setExpiresAt(
          com.google.protobuf.util.Timestamps.fromMillis(resolved.expiresAt().toEpochMilli()));
    }
    response.addStorageCredentials(credential.build());
    return response.build();
  }

  Optional<AuthCredentials> resolveAuthoritySecret(String accountId, String authorityId) {
    if (accountId == null || accountId.isBlank() || authorityId == null || authorityId.isBlank()) {
      return Optional.empty();
    }
    return secretsManager
        .get(accountId, STORAGE_AUTHORITY_SECRET_TYPE, authorityId)
        .map(
            payload -> {
              try {
                return AuthCredentials.parseFrom(payload);
              } catch (Exception e) {
                throw new IllegalStateException(
                    "Failed to parse storage credential authority secret", e);
              }
            });
  }

  ResolvedStorageCredentials mintTemporaryCredentials(
      StorageAuthority authority,
      AuthCredentials authoritySecret,
      List<String> sessionScopeLocations) {
    return mintTemporaryCredentials(authority, authoritySecret, sessionScopeLocations, false);
  }

  ResolvedStorageCredentials mintTemporaryCredentials(
      StorageAuthority authority,
      AuthCredentials authoritySecret,
      List<String> sessionScopeLocations,
      boolean exactObjectScope) {
    Optional<ResolvedStorageCredentials> resolved =
        authoritySecret == null
            ? Optional.empty()
            : CredentialResolverSupport.resolveStorageCredentials(authoritySecret);
    if (authority.hasAssumeRoleArn() && !authority.getAssumeRoleArn().isBlank()) {
      return assumeRoleCredentials(
          authority, authoritySecret, sessionScopeLocations, exactObjectScope);
    }
    if (resolved.isPresent() && resolved.get().hasKnownExpiry()) {
      return resolved.get();
    }
    throw new IllegalArgumentException(
        "Credential vending requires credentials with a known expiry minted from a storage authority role");
  }

  ResolvedStorageCredentials resolveServerSideCredentials(
      StorageAuthority authority,
      AuthCredentials authoritySecret,
      List<String> sessionScopeLocations) {
    return resolveServerSideCredentials(authority, authoritySecret, sessionScopeLocations, false);
  }

  ResolvedStorageCredentials resolveServerSideCredentials(
      StorageAuthority authority,
      AuthCredentials authoritySecret,
      List<String> sessionScopeLocations,
      boolean exactObjectScope) {
    if (authority.hasAssumeRoleArn() && !authority.getAssumeRoleArn().isBlank()) {
      return assumeRoleCredentials(
          authority, authoritySecret, sessionScopeLocations, exactObjectScope);
    }
    if (authoritySecret == null) {
      throw new IllegalArgumentException("Unsupported storage credential authority");
    }
    ResolvedStorageCredentials resolved =
        CredentialResolverSupport.resolveStorageCredentials(authoritySecret)
            .orElseThrow(
                () -> new IllegalArgumentException("Unsupported storage credential authority"));
    return resolved;
  }

  Map<String, String> clientSafeConfig(StorageAuthority authority) {
    LinkedHashMap<String, String> computed = new LinkedHashMap<>();
    if (authority == null) {
      return Map.of();
    }
    if (authority.hasRegion() && !authority.getRegion().isBlank()) {
      putRegionConfig(computed, authority.getRegion());
    }
    if (authority.hasEndpoint() && !authority.getEndpoint().isBlank()) {
      computed.put("s3.endpoint", authority.getEndpoint());
    }
    if (authority.hasPathStyleAccess()) {
      computed.put("s3.path-style-access", Boolean.toString(authority.getPathStyleAccess()));
    }
    return computed.isEmpty() ? Map.of() : Map.copyOf(computed);
  }

  ResolvedStorageCredentials assumeRoleCredentials(
      StorageAuthority authority,
      AuthCredentials authoritySecret,
      List<String> sessionScopeLocations,
      boolean exactObjectScope) {
    AssumeRoleCacheKey key =
        AssumeRoleCacheKey.of(
            authority, authoritySecret, sessionScopeLocations, exactObjectScope);
    return cachedAssumeRole(
        key,
        () -> {
          if (authoritySecret != null
              && authoritySecret.getCredentialCase() == AuthCredentials.CredentialCase.AWS) {
            return assumeRoleFromStaticSource(
                authority,
                authoritySecret.getAws(),
                sessionScopeLocations,
                exactObjectScope);
          }
          return assumeRoleFromAmbientSource(
              authority, sessionScopeLocations, exactObjectScope);
        });
  }

  private ResolvedStorageCredentials cachedAssumeRole(
      AssumeRoleCacheKey key, Supplier<ResolvedStorageCredentials> loader) {
    for (; ; ) {
      CompletableFuture<ResolvedStorageCredentials> existing = cachedAssumeRoleFuture(key);
      if (existing != null) {
        try {
          ResolvedStorageCredentials credentials = existing.join();
          if (isFreshForCache(credentials)) {
            return credentials;
          }
        } catch (CompletionException error) {
          removeCachedAssumeRole(key, existing);
          throw propagate(error.getCause());
        }
        removeCachedAssumeRole(key, existing);
        continue;
      }

      CompletableFuture<ResolvedStorageCredentials> created = new CompletableFuture<>();
      AssumeRoleCacheReservation reservation = reserveAssumeRoleCache(key, created);
      if (reservation.existing() != null) {
        continue;
      }
      if (!reservation.inserted()) {
        awaitAssumeRoleCacheCapacity();
        continue;
      }
      try {
        ResolvedStorageCredentials credentials = loader.get();
        created.complete(credentials);
        if (!isFreshForCache(credentials)) {
          removeCachedAssumeRole(key, created);
        } else {
          trimAssumeRoleCache();
        }
        return credentials;
      } catch (Throwable error) {
        created.completeExceptionally(error);
        removeCachedAssumeRole(key, created);
        throw propagate(error);
      }
    }
  }

  private CompletableFuture<ResolvedStorageCredentials> cachedAssumeRoleFuture(
      AssumeRoleCacheKey key) {
    synchronized (assumeRoleCacheLock) {
      return assumeRoleCache.get(key);
    }
  }

  private AssumeRoleCacheReservation reserveAssumeRoleCache(
      AssumeRoleCacheKey key, CompletableFuture<ResolvedStorageCredentials> created) {
    synchronized (assumeRoleCacheLock) {
      CompletableFuture<ResolvedStorageCredentials> existing = assumeRoleCache.get(key);
      if (existing != null) {
        return new AssumeRoleCacheReservation(existing, false);
      }
      trimAssumeRoleCacheLocked(assumeRoleCacheMaxEntries - 1);
      if (assumeRoleCache.size() >= assumeRoleCacheMaxEntries) {
        return new AssumeRoleCacheReservation(null, false);
      }
      assumeRoleCache.put(key, created);
      return new AssumeRoleCacheReservation(null, true);
    }
  }

  private void awaitAssumeRoleCacheCapacity() {
    synchronized (assumeRoleCacheLock) {
      trimAssumeRoleCacheLocked(assumeRoleCacheMaxEntries - 1);
      if (assumeRoleCache.size() < assumeRoleCacheMaxEntries) {
        return;
      }
      try {
        assumeRoleCacheLock.wait();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(
            "interrupted while waiting for AssumeRole cache capacity", e);
      }
    }
  }

  private void removeCachedAssumeRole(
      AssumeRoleCacheKey key, CompletableFuture<ResolvedStorageCredentials> expected) {
    synchronized (assumeRoleCacheLock) {
      assumeRoleCache.remove(key, expected);
      assumeRoleCacheLock.notifyAll();
    }
  }

  private void trimAssumeRoleCache() {
    synchronized (assumeRoleCacheLock) {
      trimAssumeRoleCacheLocked();
      // A completed entry can now be evicted by a capacity waiter even when this trim did not need
      // to remove it at the normal maximum size.
      assumeRoleCacheLock.notifyAll();
    }
  }

  private void trimAssumeRoleCacheLocked() {
    trimAssumeRoleCacheLocked(assumeRoleCacheMaxEntries);
  }

  private void trimAssumeRoleCacheLocked(int targetSize) {
    if (assumeRoleCache.size() <= targetSize) {
      return;
    }
    var iterator = assumeRoleCache.entrySet().iterator();
    while (assumeRoleCache.size() > targetSize && iterator.hasNext()) {
      if (iterator.next().getValue().isDone()) {
        iterator.remove();
      }
    }
  }

  int assumeRoleCacheSize() {
    synchronized (assumeRoleCacheLock) {
      return assumeRoleCache.size();
    }
  }

  private record AssumeRoleCacheReservation(
      CompletableFuture<ResolvedStorageCredentials> existing, boolean inserted) {}

  private static boolean isFreshForCache(ResolvedStorageCredentials credentials) {
    return credentials != null
        && credentials.expiresAt() != null
        && Instant.now().plus(ASSUME_ROLE_CACHE_REFRESH_SKEW).isBefore(credentials.expiresAt());
  }

  private static RuntimeException propagate(Throwable error) {
    if (error instanceof RuntimeException runtime) {
      return runtime;
    }
    if (error instanceof Error fatal) {
      throw fatal;
    }
    return new RuntimeException(error);
  }

  ResolvedStorageCredentials assumeRoleFromStaticSource(
      StorageAuthority authority,
      AuthCredentials.AwsCredentials source,
      List<String> sessionScopeLocations,
      boolean exactObjectScope) {
    AwsCredentialsProvider provider =
        source.getSessionToken() == null || source.getSessionToken().isBlank()
            ? StaticCredentialsProvider.create(
                AwsBasicCredentials.create(source.getAccessKeyId(), source.getSecretAccessKey()))
            : StaticCredentialsProvider.create(
                AwsSessionCredentials.create(
                    source.getAccessKeyId(),
                    source.getSecretAccessKey(),
                    source.getSessionToken()));

    return assumeRole(authority, () -> provider, sessionScopeLocations, exactObjectScope);
  }

  ResolvedStorageCredentials assumeRoleFromAmbientSource(
      StorageAuthority authority, List<String> sessionScopeLocations, boolean exactObjectScope) {
    return assumeRole(
        authority, this::ambientCredentialsProvider, sessionScopeLocations, exactObjectScope);
  }

  AwsCredentialsProvider ambientCredentialsProvider() {
    return DefaultCredentialsProvider.builder().build();
  }

  private ResolvedStorageCredentials assumeRole(
      StorageAuthority authority,
      Supplier<AwsCredentialsProvider> providerFactory,
      List<String> sessionScopeLocations,
      boolean exactObjectScope) {
    RuntimeException lastFailure = null;
    for (int attempt = 1; attempt <= ASSUME_ROLE_MAX_ATTEMPTS; attempt++) {
      try {
        return assumeRoleOnce(
            authority, providerFactory, sessionScopeLocations, exactObjectScope);
      } catch (RuntimeException error) {
        if (!retryableAssumeRoleFailure(error) || attempt == ASSUME_ROLE_MAX_ATTEMPTS) {
          if (retryableAssumeRoleFailure(error)) {
            throw new CredentialVendingUnavailableException(error);
          }
          throw error;
        }
        lastFailure = error;
        LOG.log(
            Level.WARNING,
            "Retrying transient STS AssumeRole failure authorityId={0} attempt={1}",
            new Object[] {authority.getResourceId().getId(), attempt});
        pauseBeforeAssumeRoleRetry(attempt);
      }
    }
    throw new CredentialVendingUnavailableException(lastFailure);
  }

  private ResolvedStorageCredentials assumeRoleOnce(
      StorageAuthority authority,
      Supplier<AwsCredentialsProvider> providerFactory,
      List<String> sessionScopeLocations,
      boolean exactObjectScope) {
    Integer duration = authority.hasDurationSeconds() ? authority.getDurationSeconds() : null;
    AssumeRoleRequest request =
        AssumeRoleRequest.builder()
            .roleArn(authority.getAssumeRoleArn())
            .roleSessionName(
                firstNonBlank(
                    authority.hasAssumeRoleSessionName()
                        ? authority.getAssumeRoleSessionName()
                        : null,
                    "floecat-storage-authority"))
            .externalId(
                authority.hasAssumeRoleExternalId() ? authority.getAssumeRoleExternalId() : null)
            .policy(scopedSessionPolicy(sessionScopeLocations, exactObjectScope))
            .durationSeconds(duration != null && duration > 0 ? duration : null)
            .build();

    try (var sts =
        RefreshingAwsClient.withResourceFactory(
            () -> {
              AwsCredentialsProvider provider = providerFactory.get();
              try {
                return RefreshingAwsClient.clientResource(
                    buildStsClient(authority, provider),
                    RefreshingAwsClient.closeableResource(provider));
              } catch (RuntimeException | Error e) {
                RefreshingAwsClient.closeQuietly(RefreshingAwsClient.closeableResource(provider));
                throw e;
              }
            })) {
      Credentials credentials =
          sts.callUnchecked(client -> client.assumeRole(request)).credentials();
      return new ResolvedStorageCredentials(
          credentials.accessKeyId(),
          credentials.secretAccessKey(),
          credentials.sessionToken(),
          credentials.expiration());
    }
  }

  void pauseBeforeAssumeRoleRetry(int failedAttempt) {
    long upperBound = ASSUME_ROLE_RETRY_BASE_MILLIS << Math.max(0, failedAttempt - 1);
    long delayMillis =
        ThreadLocalRandom.current().nextLong(Math.max(1L, upperBound / 2), upperBound + 1);
    LockSupport.parkNanos(Duration.ofMillis(delayMillis).toNanos());
  }

  private static boolean retryableAssumeRoleFailure(Throwable error) {
    for (Throwable current = error; current != null; current = current.getCause()) {
      if (current instanceof software.amazon.awssdk.core.exception.SdkClientException) {
        return true;
      }
      if (current instanceof software.amazon.awssdk.services.sts.model.StsException sts
          && (sts.statusCode() == 429 || sts.statusCode() >= 500)) {
        return true;
      }
    }
    return false;
  }

  StsClient buildStsClient(StorageAuthority authority, AwsCredentialsProvider provider) {
    var builder = StsClient.builder().credentialsProvider(provider);
    if (authority.hasRegion() && !authority.getRegion().isBlank()) {
      builder.region(Region.of(authority.getRegion()));
    }
    return builder.build();
  }

  static final class CredentialVendingUnavailableException extends RuntimeException {
    CredentialVendingUnavailableException(Throwable cause) {
      super("Temporary failure vending scoped storage credentials", cause);
    }
  }

  private record AssumeRoleCacheKey(
      String authorityFingerprint,
      String sourceCredentialFingerprint,
      List<String> scopes,
      boolean exactObjectScope) {
    private static AssumeRoleCacheKey of(
        StorageAuthority authority,
        AuthCredentials authoritySecret,
        List<String> sessionScopeLocations,
        boolean exactObjectScope) {
      return new AssumeRoleCacheKey(
          sha256(authority.toByteArray()),
          sha256(authoritySecret == null ? new byte[0] : authoritySecret.toByteArray()),
          List.copyOf(normalizeS3Scopes(sessionScopeLocations)),
          exactObjectScope);
    }
  }

  private static String sha256(byte[] value) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value));
    } catch (NoSuchAlgorithmException error) {
      throw new IllegalStateException("SHA-256 is unavailable", error);
    }
  }

  static String scopedSessionPolicy(String locationPrefix) {
    return scopedSessionPolicy(locationPrefix == null ? List.of() : List.of(locationPrefix));
  }

  static String scopedSessionPolicy(List<String> locationPrefixes) {
    return scopedSessionPolicy(locationPrefixes, false);
  }

  static String scopedSessionPolicy(List<String> locationPrefixes, boolean exactObject) {
    List<S3Location> scopes =
        normalizeS3Scopes(locationPrefixes).stream()
            .map(S3Location::parse)
            .filter(scope -> scope != null)
            .toList();
    if (scopes.isEmpty()) {
      return null;
    }
    if (exactObject) {
      scopes.forEach(scope -> requireLiteralObjectKey(scope.keyPrefix()));
    }
    ArrayList<String> statements = new ArrayList<>();
    for (S3BucketScope bucketScope : groupByBucket(scopes)) {
      statements.add(bucketScope.listStatementJson(exactObject));
      statements.add(bucketScope.objectStatementJson(exactObject));
    }
    return """
        {
          "Version":"2012-10-17",
          "Statement":[%s]
        }
        """
        .formatted(String.join(",", statements))
        .replace('\n', ' ')
        .replaceAll("\\s+", " ")
        .trim();
  }

  /**
   * Refuses an exact-object scope whose key carries an IAM wildcard metacharacter.
   *
   * <p>An exact-object scope promises credentials for one named file. The key goes into the policy
   * resource ARN verbatim, and IAM reads {@code *} and {@code ?} there as wildcards -- both are
   * legal in an S3 object key, so a planned file named {@code part-*.parquet} would silently mint
   * access to every sibling it matches, which is the opposite of the guarantee.
   *
   * <p>Rejection rather than escaping, because IAM has no escape for these: a resource ARN cannot
   * express a literal {@code *}. Widening the grant is not an acceptable fallback, so a key that
   * cannot be expressed exactly is refused and the caller configures a storage authority instead.
   */
  private static void requireLiteralObjectKey(String keyPrefix) {
    if (keyPrefix == null || keyPrefix.isEmpty()) {
      return;
    }
    if (keyPrefix.indexOf('*') >= 0 || keyPrefix.indexOf('?') >= 0) {
      throw new IllegalArgumentException(
          "Cannot mint an exact-object credential scope for a key containing an IAM wildcard"
              + " metacharacter (* or ?); the resulting policy would match more than the named"
              + " object");
    }
  }

  private static List<String> normalizeS3Scopes(List<String> locationPrefixes) {
    if (locationPrefixes == null || locationPrefixes.isEmpty()) {
      return List.of();
    }
    LinkedHashSet<String> normalized = new LinkedHashSet<>();
    for (String locationPrefix : locationPrefixes) {
      if (locationPrefix == null || locationPrefix.isBlank()) {
        continue;
      }
      normalized.add(locationPrefix.trim());
    }
    return List.copyOf(normalized);
  }

  private static List<S3BucketScope> groupByBucket(List<S3Location> scopes) {
    LinkedHashMap<String, ArrayList<S3Location>> grouped = new LinkedHashMap<>();
    for (S3Location scope : scopes) {
      grouped.computeIfAbsent(scope.bucket(), ignored -> new ArrayList<>()).add(scope);
    }
    ArrayList<S3BucketScope> bucketScopes = new ArrayList<>();
    grouped.forEach(
        (bucket, bucketLocations) -> bucketScopes.add(new S3BucketScope(bucket, bucketLocations)));
    return List.copyOf(bucketScopes);
  }

  private static String jsonEscape(String value) {
    if (value == null) {
      return "";
    }
    return value.replace("\\", "\\\\").replace("\"", "\\\"");
  }

  private record S3Location(String bucket, String keyPrefix) {
    static S3Location parse(String locationPrefix) {
      if (locationPrefix == null || locationPrefix.isBlank()) {
        return null;
      }
      String trimmed = locationPrefix.trim();
      String lower = trimmed.toLowerCase(java.util.Locale.ROOT);
      String normalized;
      if (lower.startsWith("s3://")) {
        normalized = trimmed.substring(5);
      } else if (lower.startsWith("s3a://") || lower.startsWith("s3n://")) {
        normalized = trimmed.substring(trimmed.indexOf("://") + 3);
      } else {
        return null;
      }
      int slash = normalized.indexOf('/');
      String bucket = slash < 0 ? normalized : normalized.substring(0, slash);
      if (bucket.isBlank()) {
        return null;
      }
      String prefix = slash < 0 ? "" : normalized.substring(slash + 1);
      while (prefix.endsWith("/")) {
        prefix = prefix.substring(0, prefix.length() - 1);
      }
      return new S3Location(bucket, prefix);
    }

    String listPrefix() {
      return keyPrefix;
    }

    String objectResourceJson() {
      if (keyPrefix.isBlank()) {
        return "[\"arn:aws:s3:::%s/*\"]".formatted(jsonEscape(bucket));
      }
      String objectArn = jsonEscape("arn:aws:s3:::%s/%s".formatted(bucket, keyPrefix));
      return "[\"%s\",\"%s/*\"]".formatted(objectArn, objectArn);
    }
  }

  private record S3BucketScope(String bucket, List<S3Location> scopes) {
    String listStatementJson(boolean exactObject) {
      String escapedBucket = jsonEscape(bucket);
      if (scopes.stream().anyMatch(scope -> scope.keyPrefix().isBlank())) {
        return """
            {
              "Effect":"Allow",
              "Action":["s3:ListBucket","s3:GetBucketLocation"],
              "Resource":["arn:aws:s3:::%s"]
            }
            """
            .formatted(escapedBucket)
            .replace('\n', ' ')
            .replaceAll("\\s+", " ")
            .trim();
      }
      ArrayList<String> prefixes = new ArrayList<>();
      for (S3Location scope : scopes) {
        String prefix = jsonEscape(scope.listPrefix());
        prefixes.add("\"" + prefix + "\"");
        // For an exact-object scope the key names a single object, not a prefix, so the
        // child-prefix clause that would authorize everything beneath it is suppressed.
        if (!exactObject) {
          prefixes.add("\"" + prefix + "/*\"");
        }
      }
      return """
          {
            "Effect":"Allow",
            "Action":["s3:ListBucket","s3:GetBucketLocation"],
            "Resource":["arn:aws:s3:::%s"],
            "Condition":{"StringLike":{"s3:prefix":[%s]}}
          }
          """
          .formatted(escapedBucket, String.join(",", prefixes))
          .replace('\n', ' ')
          .replaceAll("\\s+", " ")
          .trim();
    }

    String objectStatementJson(boolean exactObject) {
      LinkedHashSet<String> resources = new LinkedHashSet<>();
      for (S3Location scope : scopes) {
        if (scope.keyPrefix().isBlank()) {
          resources.add("\"arn:aws:s3:::%s/*\"".formatted(jsonEscape(bucket)));
          continue;
        }
        String objectArn = jsonEscape("arn:aws:s3:::%s/%s".formatted(bucket, scope.keyPrefix()));
        resources.add("\"" + objectArn + "\"");
        // Exact-object scope grants the object itself and nothing beneath its key.
        if (!exactObject) {
          resources.add("\"" + objectArn + "/*\"");
        }
      }
      return """
          {
            "Effect":"Allow",
            "Action":["s3:GetObject","s3:GetObjectVersion"],
            "Resource":[%s]
          }
          """
          .formatted(String.join(",", resources))
          .replace('\n', ' ')
          .replaceAll("\\s+", " ")
          .trim();
    }
  }

  static Optional<StorageAuthority> resolveBest(
      List<StorageAuthority> authorities, String locationPrefix) {
    StorageAuthority best = null;
    if (authorities == null
        || authorities.isEmpty()
        || locationPrefix == null
        || locationPrefix.isBlank()) {
      return Optional.empty();
    }
    for (StorageAuthority authority : authorities) {
      if (authority == null
          || !authority.getEnabled()
          || authority.getLocationPrefix() == null
          || authority.getLocationPrefix().isBlank()
          || !matchesLocationPrefix(locationPrefix, authority.getLocationPrefix())) {
        continue;
      }
      if (best == null
          || stripTrailingSlash(authority.getLocationPrefix()).length()
              > stripTrailingSlash(best.getLocationPrefix()).length()) {
        best = authority;
      }
    }
    return Optional.ofNullable(best);
  }

  static boolean matchesLocationPrefix(String location, String configuredPrefix) {
    if (!isNonBlank(location) || !isNonBlank(configuredPrefix)) {
      return false;
    }
    String normalizedLocation = location.trim();
    String normalizedPrefix = stripTrailingSlash(configuredPrefix.trim());
    if (normalizedPrefix.isEmpty() || !normalizedLocation.startsWith(normalizedPrefix)) {
      return false;
    }
    if (normalizedLocation.length() == normalizedPrefix.length()) {
      return true;
    }
    return normalizedLocation.charAt(normalizedPrefix.length()) == '/';
  }

  static String stripTrailingSlash(String value) {
    if (value == null || value.isEmpty()) {
      return "";
    }
    int end = value.length();
    while (end > 0 && value.charAt(end - 1) == '/') {
      end--;
    }
    return value.substring(0, end);
  }

  static boolean isNonBlank(String value) {
    return value != null && !value.isBlank();
  }

  static String firstNonBlank(String... values) {
    if (values == null) {
      return null;
    }
    for (String value : values) {
      if (isNonBlank(value)) {
        return value.trim();
      }
    }
    return null;
  }

  private static void putRegionConfig(Map<String, String> target, String region) {
    if (!isNonBlank(region)) {
      return;
    }
    target.put("s3.region", region);
    target.put("region", region);
    target.put("client.region", region);
  }
}
