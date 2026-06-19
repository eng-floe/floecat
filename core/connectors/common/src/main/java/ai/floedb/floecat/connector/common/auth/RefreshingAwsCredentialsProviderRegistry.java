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

package ai.floedb.floecat.connector.common.auth;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

public final class RefreshingAwsCredentialsProviderRegistry {
  public static final String OPTION_PROVIDER_ID = "floecat.aws.credentials-provider-id";
  public static final String PROPERTY_PROVIDER_ID = "floecat-provider-id";
  public static final String CLIENT_PROVIDER_CLASS =
      RegistryBackedAwsCredentialsProvider.class.getName();
  public static final String ICEBERG_CLIENT_CREDENTIALS_PROVIDER = "client.credentials-provider";
  public static final String ICEBERG_CLIENT_CREDENTIALS_PROVIDER_PREFIX =
      ICEBERG_CLIENT_CREDENTIALS_PROVIDER + ".";

  private static final Duration DEFAULT_REFRESH_SKEW = Duration.ofMinutes(5);
  private static final ConcurrentMap<String, Entry> REGISTRY = new ConcurrentHashMap<>();

  private RefreshingAwsCredentialsProviderRegistry() {}

  public static String register(
      ResolvedStorageCredentials initialCredentials,
      Supplier<ResolvedStorageCredentials> refresher) {
    return register(
        UUID.randomUUID().toString(), initialCredentials, refresher, DEFAULT_REFRESH_SKEW);
  }

  static String register(
      String providerId,
      ResolvedStorageCredentials initialCredentials,
      Supplier<ResolvedStorageCredentials> refresher,
      Duration refreshSkew) {
    String normalizedProviderId = requireNonBlank(providerId, "providerId");
    Objects.requireNonNull(initialCredentials, "initialCredentials");
    Objects.requireNonNull(refresher, "refresher");
    Duration effectiveSkew =
        computeRefreshSkew(
            initialCredentials,
            refreshSkew == null || refreshSkew.isNegative() ? DEFAULT_REFRESH_SKEW : refreshSkew);
    REGISTRY.put(normalizedProviderId, new Entry(initialCredentials, refresher, effectiveSkew));
    return normalizedProviderId;
  }

  public static void unregister(String providerId) {
    if (providerId == null || providerId.isBlank()) {
      return;
    }
    REGISTRY.remove(providerId.trim());
  }

  public static AwsCredentials resolve(String providerId) {
    Entry entry = REGISTRY.get(requireNonBlank(providerId, "providerId"));
    if (entry == null) {
      throw new IllegalStateException("Unknown AWS credentials provider id: " + providerId);
    }
    return entry.resolveCredentials();
  }

  public static Map<String, String> propertiesFor(String providerId) {
    String normalizedProviderId = requireNonBlank(providerId, "providerId");
    return Map.of(
        OPTION_PROVIDER_ID, normalizedProviderId, PROPERTY_PROVIDER_ID, normalizedProviderId);
  }

  public static Duration computeRefreshSkew(
      ResolvedStorageCredentials credentials, Duration defaultRefreshSkew) {
    Duration baseline =
        defaultRefreshSkew == null || defaultRefreshSkew.isNegative()
            ? DEFAULT_REFRESH_SKEW
            : defaultRefreshSkew;
    if (credentials == null || credentials.expiresAt() == null) {
      return baseline;
    }
    Instant now = Instant.now();
    if (!credentials.expiresAt().isAfter(now)) {
      return Duration.ZERO;
    }
    Duration lifetime = Duration.between(now, credentials.expiresAt());
    Duration adaptive = lifetime.dividedBy(3L);
    if (adaptive.isNegative()) {
      return Duration.ZERO;
    }
    return adaptive.compareTo(baseline) < 0 ? adaptive : baseline;
  }

  private static AwsCredentials toAwsCredentials(ResolvedStorageCredentials resolved) {
    if (resolved == null
        || isBlank(resolved.accessKeyId())
        || isBlank(resolved.secretAccessKey())) {
      throw new IllegalStateException("AWS credential refresher returned incomplete credentials");
    }
    if (!isBlank(resolved.sessionToken())) {
      return AwsSessionCredentials.create(
          resolved.accessKeyId(), resolved.secretAccessKey(), resolved.sessionToken());
    }
    return AwsBasicCredentials.create(resolved.accessKeyId(), resolved.secretAccessKey());
  }

  private static boolean shouldRefresh(
      ResolvedStorageCredentials current, Duration refreshSkew, Instant now) {
    if (current == null) {
      return true;
    }
    if (!current.isTemporary() || current.expiresAt() == null) {
      return false;
    }
    return !now.isBefore(current.expiresAt().minus(refreshSkew));
  }

  private static String requireNonBlank(String value, String field) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(field + " must be non-blank");
    }
    return value.trim();
  }

  private static boolean isBlank(String value) {
    return value == null || value.isBlank();
  }

  private static final class Entry {
    private final Supplier<ResolvedStorageCredentials> refresher;
    private final Duration refreshSkew;
    private volatile ResolvedStorageCredentials current;

    private Entry(
        ResolvedStorageCredentials initialCredentials,
        Supplier<ResolvedStorageCredentials> refresher,
        Duration refreshSkew) {
      this.current = initialCredentials;
      this.refresher = refresher;
      this.refreshSkew = refreshSkew;
    }

    private AwsCredentials resolveCredentials() {
      ResolvedStorageCredentials snapshot = current;
      Instant now = Instant.now();
      if (shouldRefresh(snapshot, refreshSkew, now)) {
        synchronized (this) {
          snapshot = current;
          now = Instant.now();
          if (shouldRefresh(snapshot, refreshSkew, now)) {
            try {
              current = Objects.requireNonNull(refresher.get(), "refresher returned null");
            } catch (RuntimeException e) {
              if (snapshot != null
                  && snapshot.expiresAt() != null
                  && now.isBefore(snapshot.expiresAt())) {
                return toAwsCredentials(snapshot);
              }
              throw e;
            }
            snapshot = current;
          }
        }
      }
      return toAwsCredentials(snapshot);
    }
  }
}
