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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

class RefreshingAwsCredentialsRegistryTest {
  @Test
  void refreshesExpiredSessionCredentialsOnce() {
    AtomicInteger refreshes = new AtomicInteger();
    try (var registration =
        RefreshingAwsCredentialsRegistry.register(
            "refresh-test",
            credentials("old", Instant.now().minusSeconds(1)),
            () -> {
              refreshes.incrementAndGet();
              return credentials("new", Instant.now().plusSeconds(3600));
            },
            Duration.ZERO)) {
      var first =
          RefreshingAwsCredentialsRegistry.resolve(
              registration.providerId(), AwsCredentialScope.CATALOG);
      var second =
          RefreshingAwsCredentialsRegistry.resolve(
              registration.providerId(), AwsCredentialScope.CATALOG);

      assertEquals("access-new", first.accessKeyId());
      assertEquals("access-new", second.accessKeyId());
      assertEquals("token-new", ((AwsSessionCredentials) first).sessionToken());
      assertEquals(1, refreshes.get());
    }
  }

  @Test
  void cachesTerminalRefreshFailure() {
    AtomicInteger refreshes = new AtomicInteger();
    TerminalCredentialRefreshException terminal =
        new TerminalCredentialRefreshException("lease lost", new IllegalStateException("stale"));
    try (var registration =
        RefreshingAwsCredentialsRegistry.register(
            "terminal-test",
            credentials("old", Instant.now().minusSeconds(1)),
            () -> {
              refreshes.incrementAndGet();
              throw terminal;
            },
            Duration.ZERO)) {
      assertSame(
          terminal,
          assertThrows(
              TerminalCredentialRefreshException.class,
              () ->
                  RefreshingAwsCredentialsRegistry.resolve(
                      registration.providerId(), AwsCredentialScope.CATALOG)));
      assertSame(
          terminal,
          assertThrows(
              TerminalCredentialRefreshException.class,
              () ->
                  RefreshingAwsCredentialsRegistry.resolve(
                      registration.providerId(), AwsCredentialScope.CATALOG)));
      assertEquals(1, refreshes.get());
    }
  }

  @Test
  void closedRegistrationCanNoLongerResolve() {
    var registration =
        RefreshingAwsCredentialsRegistry.register(
            credentials("active", null), () -> credentials("unused", null));
    String providerId = registration.providerId();

    registration.close();

    assertThrows(
        IllegalStateException.class,
        () -> RefreshingAwsCredentialsRegistry.resolve(providerId, AwsCredentialScope.STORAGE));
  }

  @Test
  void credentialAndRegistrationStringsDoNotExposeSecretsOrProviderIds() {
    AwsCredentialValue credentials = credentials("private", null);
    try (var registration =
        RefreshingAwsCredentialsRegistry.register(credentials, () -> credentials)) {
      assertFalse(credentials.toString().contains("access-private"));
      assertFalse(credentials.toString().contains("secret-private"));
      assertFalse(credentials.toString().contains("token-private"));
      assertFalse(registration.toString().contains(registration.providerId()));
    }
  }

  @Test
  void reflectionProviderResolvesTheConfiguredCredentialScope() {
    try (var catalogRegistration =
            RefreshingAwsCredentialsRegistry.register(
                credentials("catalog", null), () -> credentials("unused-catalog", null));
        var storageRegistration =
            RefreshingAwsCredentialsRegistry.register(
                credentials("storage", null), () -> credentials("unused-storage", null))) {
      var catalogProvider =
          RegistryBackedAwsCredentialsProvider.create(
              providerProperties(catalogRegistration, AwsCredentialScope.CATALOG));
      var storageProvider =
          RegistryBackedAwsCredentialsProvider.create(
              providerProperties(storageRegistration, AwsCredentialScope.STORAGE));

      assertEquals("access-catalog", catalogProvider.resolveCredentials().accessKeyId());
      assertEquals("access-storage", storageProvider.resolveCredentials().accessKeyId());
    }
  }

  @Test
  void recomputesAdaptiveSkewAfterRefreshChangesCredentialLifetime() {
    Instant start = Instant.parse("2026-08-05T12:00:00Z");
    MutableClock clock = new MutableClock(start);
    AtomicInteger refreshes = new AtomicInteger();
    try (var registration =
        RefreshingAwsCredentialsRegistry.register(
            "adaptive-skew-test",
            credentials("old", start.plus(Duration.ofHours(1))),
            () ->
                credentials(
                    "new-" + refreshes.incrementAndGet(),
                    clock.instant().plus(Duration.ofMinutes(2))),
            Duration.ofMinutes(5),
            clock)) {
      clock.advance(Duration.ofMinutes(56));

      RefreshingAwsCredentialsRegistry.resolve(
          registration.providerId(), AwsCredentialScope.CATALOG);
      RefreshingAwsCredentialsRegistry.resolve(
          registration.providerId(), AwsCredentialScope.CATALOG);

      assertEquals(1, refreshes.get());
    }
  }

  @Test
  void periodicallyRefreshesCredentialsWithoutKnownExpiry() {
    Instant start = Instant.parse("2026-08-05T12:00:00Z");
    MutableClock clock = new MutableClock(start);
    AtomicInteger refreshes = new AtomicInteger();
    try (var registration =
        RefreshingAwsCredentialsRegistry.register(
            "unknown-expiry-test",
            credentials("old", null),
            () -> credentials("new-" + refreshes.incrementAndGet(), null),
            Duration.ofMinutes(5),
            clock)) {
      var initial =
          RefreshingAwsCredentialsRegistry.resolve(
              registration.providerId(), AwsCredentialScope.CATALOG);
      clock.advance(Duration.ofMinutes(5));
      var refreshed =
          RefreshingAwsCredentialsRegistry.resolve(
              registration.providerId(), AwsCredentialScope.CATALOG);
      var cached =
          RefreshingAwsCredentialsRegistry.resolve(
              registration.providerId(), AwsCredentialScope.CATALOG);

      assertEquals("access-old", initial.accessKeyId());
      assertEquals("access-new-1", refreshed.accessKeyId());
      assertEquals("access-new-1", cached.accessKeyId());
      assertEquals(1, refreshes.get());
    }
  }

  @Test
  void failedRefreshDoesNotReturnCredentialsThatExpiredDuringTheRefresh() {
    Instant start = Instant.parse("2026-08-05T12:00:00Z");
    MutableClock clock = new MutableClock(start);
    IllegalStateException refreshFailure = new IllegalStateException("refresh failed");
    try (var registration =
        RefreshingAwsCredentialsRegistry.register(
            "expiry-during-refresh-test",
            credentials("old", start.plusSeconds(30)),
            () -> {
              clock.advance(Duration.ofSeconds(10));
              throw refreshFailure;
            },
            Duration.ofMinutes(1),
            clock)) {
      clock.advance(Duration.ofSeconds(21));
      assertSame(
          refreshFailure,
          assertThrows(
              IllegalStateException.class,
              () ->
                  RefreshingAwsCredentialsRegistry.resolve(
                      registration.providerId(), AwsCredentialScope.CATALOG)));
    }
  }

  private static Map<String, String> providerProperties(
      RefreshingAwsCredentialsRegistry.Registration registration, AwsCredentialScope scope) {
    String providerId =
        RefreshingAwsCredentialsRegistry.propertiesFor(registration, scope)
            .values()
            .iterator()
            .next();
    return Map.of(
        RefreshingAwsCredentialsRegistry.PROVIDER_ID_PROPERTY,
        providerId,
        RefreshingAwsCredentialsRegistry.CREDENTIAL_SCOPE_PROPERTY,
        scope.name());
  }

  private static AwsCredentialValue credentials(String suffix, Instant expiresAt) {
    return new AwsCredentialValue(
        "access-" + suffix, "secret-" + suffix, "token-" + suffix, expiresAt);
  }

  private static final class MutableClock extends Clock {
    private Instant current;

    private MutableClock(Instant current) {
      this.current = current;
    }

    private void advance(Duration duration) {
      current = current.plus(duration);
    }

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return Clock.fixed(current, zone);
    }

    @Override
    public Instant instant() {
      return current;
    }
  }
}
