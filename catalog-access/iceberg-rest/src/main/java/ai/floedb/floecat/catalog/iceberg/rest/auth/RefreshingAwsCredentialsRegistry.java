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

package ai.floedb.floecat.catalog.iceberg.rest.auth;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

/** Process-local registry used by Iceberg and AWS SDK credential-provider reflection hooks. */
public final class RefreshingAwsCredentialsRegistry {
  private static final Logger LOG =
      Logger.getLogger(RefreshingAwsCredentialsRegistry.class.getName());

  public static final String STORAGE_PROVIDER_ID = "floecat.aws.credentials-provider-id";
  public static final String CATALOG_PROVIDER_ID = "floecat.catalog.aws.credentials-provider-id";
  static final String PROVIDER_ID_PROPERTY = "floecat-provider-id";
  static final String CREDENTIAL_SCOPE_PROPERTY = "floecat-credential-scope";
  static final String CLIENT_PROVIDER = "client.credentials-provider";
  static final String CLIENT_PROVIDER_PREFIX = CLIENT_PROVIDER + ".";

  private static final Duration DEFAULT_REFRESH_SKEW = Duration.ofMinutes(5);
  private static final ConcurrentMap<String, Entry> REGISTRY = new ConcurrentHashMap<>();

  private RefreshingAwsCredentialsRegistry() {}

  public static Registration register(
      AwsCredentialValue initialCredentials, Supplier<AwsCredentialValue> refresher) {
    return register(
        UUID.randomUUID().toString(),
        initialCredentials,
        refresher,
        DEFAULT_REFRESH_SKEW,
        Clock.systemUTC());
  }

  static Registration register(
      String providerId,
      AwsCredentialValue initialCredentials,
      Supplier<AwsCredentialValue> refresher,
      Duration refreshSkew) {
    return register(providerId, initialCredentials, refresher, refreshSkew, Clock.systemUTC());
  }

  static Registration register(
      String providerId,
      AwsCredentialValue initialCredentials,
      Supplier<AwsCredentialValue> refresher,
      Duration refreshSkew,
      Clock clock) {
    String normalizedProviderId = requireNonBlank(providerId, "providerId");
    Objects.requireNonNull(initialCredentials, "initialCredentials");
    Objects.requireNonNull(refresher, "refresher");
    Duration defaultRefreshSkew = normalizeRefreshSkew(refreshSkew);
    Entry entry =
        new Entry(
            normalizedProviderId,
            initialCredentials,
            refresher,
            defaultRefreshSkew,
            Objects.requireNonNull(clock, "clock"));
    Entry previous = REGISTRY.putIfAbsent(normalizedProviderId, entry);
    if (previous != null) {
      throw new IllegalStateException("AWS credentials provider id is already registered");
    }
    LOG.log(
        Level.INFO,
        "Registered catalog-access AWS credentials provider; providerRef={0}, expiresAt={1},"
            + " refreshSkewMs={2}",
        new Object[] {
          entry.providerRef,
          initialCredentials.expiresAt(),
          entry.currentState.refreshSkew().toMillis()
        });
    return new Registration(normalizedProviderId);
  }

  public static AwsCredentials resolve(String providerId, AwsCredentialScope scope) {
    Entry entry = REGISTRY.get(requireNonBlank(providerId, "providerId"));
    if (entry == null) {
      throw new IllegalStateException("Unknown AWS credentials provider id");
    }
    return entry.resolveCredentials(Objects.requireNonNull(scope, "scope"));
  }

  public static Map<String, String> propertiesFor(
      Registration registration, AwsCredentialScope scope) {
    Objects.requireNonNull(registration, "registration");
    Objects.requireNonNull(scope, "scope");
    return Map.of(
        scope == AwsCredentialScope.CATALOG ? CATALOG_PROVIDER_ID : STORAGE_PROVIDER_ID,
        registration.providerId());
  }

  public static Duration computeRefreshSkew(
      AwsCredentialValue credentials, Duration defaultRefreshSkew) {
    return computeRefreshSkew(credentials, normalizeRefreshSkew(defaultRefreshSkew), Instant.now());
  }

  private static Duration computeRefreshSkew(
      AwsCredentialValue credentials, Duration baseline, Instant now) {
    if (credentials == null || credentials.expiresAt() == null) {
      return baseline;
    }
    if (!credentials.expiresAt().isAfter(now)) {
      return Duration.ZERO;
    }
    Duration adaptive = Duration.between(now, credentials.expiresAt()).dividedBy(3L);
    if (adaptive.isNegative()) {
      return Duration.ZERO;
    }
    return adaptive.compareTo(baseline) < 0 ? adaptive : baseline;
  }

  private static Duration normalizeRefreshSkew(Duration refreshSkew) {
    return refreshSkew == null || refreshSkew.isNegative() ? DEFAULT_REFRESH_SKEW : refreshSkew;
  }

  private static void unregister(String providerId) {
    Entry removed = REGISTRY.remove(providerId);
    if (removed != null) {
      LOG.log(
          Level.INFO,
          "Unregistered catalog-access AWS credentials provider; providerRef={0},"
              + " observedScopes={1}",
          new Object[] {removed.providerRef, removed.observedScopes});
    }
  }

  private static AwsCredentials toAwsCredentials(AwsCredentialValue resolved) {
    Objects.requireNonNull(resolved, "resolved");
    if (resolved.isSessionCredential()) {
      return AwsSessionCredentials.create(
          resolved.accessKeyId(), resolved.secretAccessKey(), resolved.sessionToken());
    }
    return AwsBasicCredentials.create(resolved.accessKeyId(), resolved.secretAccessKey());
  }

  private static boolean shouldRefresh(CredentialState state, Instant now) {
    if (state == null || state.credentials() == null) {
      return true;
    }
    if (!state.credentials().hasKnownExpiry()) {
      return !now.isBefore(state.refreshedAt().plus(state.refreshSkew()));
    }
    return !now.isBefore(state.credentials().expiresAt().minus(state.refreshSkew()));
  }

  private static String requireNonBlank(String value, String field) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(field + " must be non-blank");
    }
    return value.trim();
  }

  private static String providerRef(String providerId) {
    return Integer.toUnsignedString(providerId.hashCode(), 16);
  }

  public static final class Registration implements AutoCloseable {
    private final String providerId;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private Registration(String providerId) {
      this.providerId = providerId;
    }

    String providerId() {
      if (closed.get()) {
        throw new IllegalStateException("AWS credentials registration is closed");
      }
      return providerId;
    }

    @Override
    public void close() {
      if (closed.compareAndSet(false, true)) {
        unregister(providerId);
      }
    }

    @Override
    public String toString() {
      return "Registration[providerRef=" + providerRef(providerId) + ", closed=" + closed + "]";
    }
  }

  private static final class Entry {
    private final String providerRef;
    private final Supplier<AwsCredentialValue> refresher;
    private final Duration defaultRefreshSkew;
    private final Clock clock;
    private final Set<AwsCredentialScope> observedScopes = ConcurrentHashMap.newKeySet();
    private volatile CredentialState currentState;
    private volatile TerminalCredentialRefreshException terminalFailure;

    private Entry(
        String providerId,
        AwsCredentialValue initialCredentials,
        Supplier<AwsCredentialValue> refresher,
        Duration defaultRefreshSkew,
        Clock clock) {
      this.providerRef = providerRef(providerId);
      this.refresher = refresher;
      this.defaultRefreshSkew = defaultRefreshSkew;
      this.clock = clock;
      Instant registeredAt = clock.instant();
      this.currentState =
          new CredentialState(
              initialCredentials,
              computeRefreshSkew(initialCredentials, defaultRefreshSkew, registeredAt),
              registeredAt);
    }

    private AwsCredentials resolveCredentials(AwsCredentialScope scope) {
      CredentialState snapshot = currentState;
      if (observedScopes.add(scope)) {
        LOG.log(
            Level.INFO,
            "Resolved catalog-access AWS credential scope; providerRef={0}, scope={1},"
                + " expiresAt={2}",
            new Object[] {providerRef, scope, snapshot.credentials().expiresAt()});
      }
      TerminalCredentialRefreshException terminal = terminalFailure;
      if (terminal != null) {
        throw terminal;
      }
      Instant now = clock.instant();
      if (shouldRefresh(snapshot, now)) {
        synchronized (this) {
          terminal = terminalFailure;
          if (terminal != null) {
            throw terminal;
          }
          snapshot = currentState;
          now = clock.instant();
          if (shouldRefresh(snapshot, now)) {
            try {
              AwsCredentialValue refreshed =
                  Objects.requireNonNull(refresher.get(), "refresher returned null");
              toAwsCredentials(refreshed);
              Instant refreshedAt = clock.instant();
              currentState =
                  new CredentialState(
                      refreshed,
                      computeRefreshSkew(refreshed, defaultRefreshSkew, refreshedAt),
                      refreshedAt);
              LOG.log(
                  Level.INFO,
                  "Refreshed catalog-access AWS credentials; providerRef={0}, scope={1},"
                      + " previousExpiresAt={2}, newExpiresAt={3}, refreshSkewMs={4}",
                  new Object[] {
                    providerRef,
                    scope,
                    snapshot.credentials().expiresAt(),
                    refreshed.expiresAt(),
                    currentState.refreshSkew().toMillis()
                  });
            } catch (RuntimeException e) {
              if (e instanceof TerminalCredentialRefreshException terminalRefresh) {
                terminalFailure = terminalRefresh;
                throw terminalRefresh;
              }
              if (snapshot.credentials().expiresAt() != null
                  && now.isBefore(snapshot.credentials().expiresAt())) {
                return toAwsCredentials(snapshot.credentials());
              }
              throw e;
            }
            snapshot = currentState;
          }
        }
      }
      return toAwsCredentials(snapshot.credentials());
    }
  }

  private record CredentialState(
      AwsCredentialValue credentials, Duration refreshSkew, Instant refreshedAt) {}
}
