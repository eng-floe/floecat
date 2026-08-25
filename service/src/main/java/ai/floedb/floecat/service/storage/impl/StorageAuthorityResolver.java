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
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
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
  public static final String STORAGE_AUTHORITY_SECRET_TYPE = "storage-authorities";
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
      StorageAuthority authority, String accountId, boolean serverSide) {
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
    requireValidAuthorityLocation(authority);

    ResolveStorageAuthorityResponse.Builder response =
        ResolveStorageAuthorityResponse.newBuilder().setAuthorityId(authority.getResourceId());
    response.putAllClientSafeConfig(clientSafeConfig(authority));
    AuthCredentials authoritySecret =
        resolveAuthoritySecret(accountId, authority.getResourceId().getId()).orElse(null);
    ResolvedStorageCredentials resolved;
    if (!serverSide) {
      resolved = mintTemporaryCredentials(authority, authoritySecret);
      if (!resolved.hasKnownExpiry()) {
        throw new IllegalArgumentException(
            "Credential vending requires credentials with a known expiry minted from a storage authority role");
      }
    } else {
      resolved = resolveServerSideCredentials(authority, authoritySecret);
    }

    LinkedHashMap<String, String> storageConfig = new LinkedHashMap<>();
    storageConfig.put("type", authority.getType().isBlank() ? "s3" : authority.getType());
    storageConfig.putAll(clientSafeConfig(authority));
    storageConfig.putAll(resolved.asS3Properties());
    VendedStorageCredential.Builder credential =
        VendedStorageCredential.newBuilder()
            .setPrefix(authority.getLocationPrefix())
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
      StorageAuthority authority, AuthCredentials authoritySecret) {
    Optional<ResolvedStorageCredentials> resolved =
        authoritySecret == null
            ? Optional.empty()
            : CredentialResolverSupport.resolveStorageCredentials(authoritySecret);
    if (authority.hasAssumeRoleArn() && !authority.getAssumeRoleArn().isBlank()) {
      return assumeRoleCredentials(authority, authoritySecret);
    }
    if (resolved.isPresent() && resolved.get().hasKnownExpiry()) {
      return resolved.get();
    }
    throw new IllegalArgumentException(
        "Credential vending requires credentials with a known expiry minted from a storage authority role");
  }

  ResolvedStorageCredentials resolveServerSideCredentials(
      StorageAuthority authority, AuthCredentials authoritySecret) {
    if (authority.hasAssumeRoleArn() && !authority.getAssumeRoleArn().isBlank()) {
      return assumeRoleCredentials(authority, authoritySecret);
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
      StorageAuthority authority, AuthCredentials authoritySecret) {
    AssumeRoleCacheKey key = AssumeRoleCacheKey.of(authority, authoritySecret);
    return cachedAssumeRole(
        key,
        () -> {
          if (authoritySecret != null
              && authoritySecret.getCredentialCase() == AuthCredentials.CredentialCase.AWS) {
            return assumeRoleFromStaticSource(authority, authoritySecret.getAws());
          }
          return assumeRoleFromAmbientSource(authority);
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
      StorageAuthority authority, AuthCredentials.AwsCredentials source) {
    AwsCredentialsProvider provider =
        source.getSessionToken() == null || source.getSessionToken().isBlank()
            ? StaticCredentialsProvider.create(
                AwsBasicCredentials.create(source.getAccessKeyId(), source.getSecretAccessKey()))
            : StaticCredentialsProvider.create(
                AwsSessionCredentials.create(
                    source.getAccessKeyId(),
                    source.getSecretAccessKey(),
                    source.getSessionToken()));

    return assumeRole(authority, () -> provider);
  }

  ResolvedStorageCredentials assumeRoleFromAmbientSource(StorageAuthority authority) {
    return assumeRole(authority, this::ambientCredentialsProvider);
  }

  AwsCredentialsProvider ambientCredentialsProvider() {
    return DefaultCredentialsProvider.builder().build();
  }

  private ResolvedStorageCredentials assumeRole(
      StorageAuthority authority, Supplier<AwsCredentialsProvider> providerFactory) {
    RuntimeException lastFailure = null;
    for (int attempt = 1; attempt <= ASSUME_ROLE_MAX_ATTEMPTS; attempt++) {
      try {
        return assumeRoleOnce(authority, providerFactory);
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
      StorageAuthority authority, Supplier<AwsCredentialsProvider> providerFactory) {
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
            .policy(scopedSessionPolicy(authority.getLocationPrefix()))
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
      String authorityFingerprint, String sourceCredentialFingerprint) {
    private static AssumeRoleCacheKey of(
        StorageAuthority authority, AuthCredentials authoritySecret) {
      return new AssumeRoleCacheKey(
          sha256(authority.toByteArray()), sha256(canonicalBytes(authoritySecret)));
    }

    /**
     * Fingerprints the source credential over a canonical form. {@code AuthCredentials} carries
     * {@code map<string,string>} properties and headers, and protobuf serialization does not order
     * map entries, so {@code toByteArray()} alone can hash two logically identical secrets to
     * different digests and defeat the cache. Sorting the map entries out into a stable
     * representation removes that dependence on serialization order.
     */
    private static byte[] canonicalBytes(AuthCredentials authoritySecret) {
      if (authoritySecret == null) {
        return new byte[0];
      }
      AuthCredentials withoutMaps =
          authoritySecret.toBuilder().clearProperties().clearHeaders().build();
      StringBuilder canonical = new StringBuilder();
      canonical.append(HexFormat.of().formatHex(withoutMaps.toByteArray()));
      appendSortedEntries(canonical, "p", authoritySecret.getPropertiesMap());
      appendSortedEntries(canonical, "h", authoritySecret.getHeadersMap());
      return canonical.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static void appendSortedEntries(
        StringBuilder canonical, String prefix, Map<String, String> entries) {
      // Length-prefixed so keys or values containing the separator cannot forge another entry.
      new TreeMap<>(entries)
          .forEach(
              (key, value) ->
                  canonical
                      .append('|')
                      .append(prefix)
                      .append(':')
                      .append(key.length())
                      .append(':')
                      .append(key)
                      .append(':')
                      .append(value.length())
                      .append(':')
                      .append(value));
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
    S3Location scope = S3Location.parse(locationPrefix);
    if (scope == null) {
      throw new IllegalArgumentException(
          "S3 storage authority location_prefix must identify a concrete bucket");
    }
    return """
        {
          "Version":"2012-10-17",
          "Statement":[%s,%s]
        }
        """
        .formatted(scope.listStatementJson(), scope.objectStatementJson())
        .replace('\n', ' ')
        .replaceAll("\\s+", " ")
        .trim();
  }

  static boolean isValidAuthorityLocation(String type, String locationPrefix) {
    return isSupportedAuthorityType(type) && S3Location.parse(locationPrefix) != null;
  }

  static boolean isSupportedAuthorityType(String type) {
    return "s3".equalsIgnoreCase(firstNonBlank(type, "s3"));
  }

  private static void requireValidAuthorityLocation(StorageAuthority authority) {
    if (!isValidAuthorityLocation(authority.getType(), authority.getLocationPrefix())) {
      throw new IllegalArgumentException(
          "Storage authority must use type s3 and a location_prefix identifying a concrete bucket");
    }
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
      String bucket =
          (slash < 0 ? normalized : normalized.substring(0, slash))
              .toLowerCase(java.util.Locale.ROOT);
      if (!bucket.matches("[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]")) {
        return null;
      }
      String prefix = slash < 0 ? "" : normalized.substring(slash + 1);
      while (prefix.endsWith("/")) {
        prefix = prefix.substring(0, prefix.length() - 1);
      }
      if (prefix.indexOf('*') >= 0
          || prefix.indexOf('?') >= 0
          || prefix.chars().anyMatch(Character::isISOControl)) {
        return null;
      }
      return new S3Location(bucket, prefix);
    }

    String listStatementJson() {
      String escapedBucket = jsonEscape(bucket);
      if (keyPrefix.isBlank()) {
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
      String prefix = jsonEscape(keyPrefix);
      return """
          {
            "Effect":"Allow",
            "Action":["s3:ListBucket","s3:GetBucketLocation"],
            "Resource":["arn:aws:s3:::%s"],
            "Condition":{"StringLike":{"s3:prefix":["%s","%s/*"]}}
          }
          """
          .formatted(escapedBucket, prefix, prefix)
          .replace('\n', ' ')
          .replaceAll("\\s+", " ")
          .trim();
    }

    String objectStatementJson() {
      String resources;
      if (keyPrefix.isBlank()) {
        resources = "\"arn:aws:s3:::%s/*\"".formatted(jsonEscape(bucket));
      } else {
        String objectArn = jsonEscape("arn:aws:s3:::%s/%s".formatted(bucket, keyPrefix));
        resources = "\"%s\",\"%s/*\"".formatted(objectArn, objectArn);
      }
      return """
          {
            "Effect":"Allow",
            "Action":["s3:GetObject","s3:GetObjectVersion"],
            "Resource":[%s]
          }
          """
          .formatted(resources)
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
          || !isValidAuthorityLocation(authority.getType(), authority.getLocationPrefix())
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
