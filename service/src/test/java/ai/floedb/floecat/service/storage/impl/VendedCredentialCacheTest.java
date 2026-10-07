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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.rpc.AuthCredentials;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.integration.rpc.CatalogIntegration;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class VendedCredentialCacheTest {

  private static final Instant VENDED = Instant.parse("2026-09-01T14:00:00Z");
  private static final Instant ONE_HOUR = VENDED.plus(Duration.ofHours(1));
  private static final Connector SOURCE =
      Connector.newBuilder().setResourceId(ResourceId.newBuilder().setId("c1")).build();
  private static final CatalogIntegration INTEGRATION =
      CatalogIntegration.newBuilder().setResourceId(ResourceId.newBuilder().setId("i1")).build();
  private static final AuthCredentials SECRET =
      AuthCredentials.newBuilder()
          .setBearer(AuthCredentials.BearerToken.newBuilder().setToken("t1"))
          .build();

  private final AtomicLong nanos = new AtomicLong();

  @Test
  void halfLifeHoldsAnAnswerForHalfItsLifetime() {
    assertThat(cache(0.5).holdFor(VENDED, ONE_HOUR)).isEqualTo(Duration.ofMinutes(30));
  }

  @Test
  void aFractionOfZeroIsTheIcebergRefreshRule() {
    VendedCredentialCache cache = cache(0.0);

    assertThat(cache.holdFor(VENDED, ONE_HOUR)).isEqualTo(Duration.ofMinutes(55));
    assertThat(cache.holdFor(VENDED, VENDED.plus(Duration.ofHours(12))))
        .isEqualTo(Duration.ofHours(12).minusMinutes(5));
  }

  @Test
  void neverHandsOutLessThanTheIcebergFloor() {
    VendedCredentialCache cache = cache(0.5);

    // Half of eight minutes would leave four; the five-minute floor wins.
    assertThat(cache.holdFor(VENDED, VENDED.plus(Duration.ofMinutes(8))))
        .isEqualTo(Duration.ofMinutes(3));
    assertThat(cache.holdFor(VENDED, VENDED.plus(Duration.ofMinutes(4)))).isZero();
    assertThat(cache.holdFor(VENDED, VENDED.minus(Duration.ofMinutes(1)))).isZero();
  }

  @Test
  void aFarFutureExpiryDoesNotOverflow() {
    assertThat(cache(0.5).holdFor(VENDED, Instant.MAX)).isPositive();
  }

  @Test
  void anAnswerIsReusedUntilItsHoldEnds() {
    VendedCredentialCache cache = cache(0.5);
    AtomicInteger vends = new AtomicInteger();

    assertThat(vend(cache, vends, ONE_HOUR)).isEqualTo("ASIA-1");
    nanos.addAndGet(Duration.ofMinutes(29).toNanos());
    assertThat(vend(cache, vends, ONE_HOUR)).isEqualTo("ASIA-1");
    nanos.addAndGet(Duration.ofMinutes(1).toNanos());
    assertThat(vend(cache, vends, ONE_HOUR)).isEqualTo("ASIA-2");
  }

  @Test
  void answersWithoutAnExpiryOrWithoutCredentialsAreNotHeld() {
    VendedCredentialCache cache = cache(0.5);
    AtomicInteger vends = new AtomicInteger();

    vend(cache, vends, null);
    vend(cache, vends, null);
    assertThat(vends).hasValue(2);

    for (int i = 0; i < 2; i++) {
      cache.connectorVend(
          SOURCE,
          SECRET,
          "cat.schema",
          "orders",
          () -> {
            vends.incrementAndGet();
            return Optional.of(
                new FloecatConnector.VendedStorageCredentials(Map.of(), null, ONE_HOUR));
          },
          SourceCatalogCredentialVendor::connectorExpiry);
    }
    assertThat(vends).hasValue(4);
  }

  @Test
  void anAnswerNoCallerCouldUseIsNotHeld() {
    VendedCredentialCache cache = cache(0.5);
    AtomicInteger vends = new AtomicInteger();

    for (int i = 0; i < 2; i++) {
      cache.connectorVend(
          SOURCE,
          SECRET,
          "cat.schema",
          "orders",
          () -> {
            vends.incrementAndGet();
            return Optional.of(
                new FloecatConnector.VendedStorageCredentials(
                    Map.of("s3.access-key-id", "ASIA"), null, ONE_HOUR));
          },
          SourceCatalogCredentialVendor::connectorExpiry);
    }
    assertThat(vends).hasValue(2);

    for (int i = 0; i < 2; i++) {
      cache.integrationVend(
          INTEGRATION,
          "cat.schema",
          "orders",
          () -> {
            vends.incrementAndGet();
            return Optional.of(
                new ai.floedb.floecat.catalog.access.VendedStorageCredentials(
                    Map.of("s3.access-key-id", "ASIA", "s3.secret-access-key", "s"),
                    "",
                    Optional.of(ONE_HOUR)));
          },
          SourceCatalogCredentialVendor::integrationExpiry);
    }
    assertThat(vends).hasValue(4);
  }

  @Test
  void anAnswerOnlySomeCallersCanUseIsHeld() {
    VendedCredentialCache cache = cache(0.5);
    AtomicInteger vends = new AtomicInteger();

    // A key pair with an expiry but no session token serves a query, not a reconcile; each call
    // checks the held answer for its own use.
    for (int i = 0; i < 2; i++) {
      cache.connectorVend(
          SOURCE,
          SECRET,
          "cat.schema",
          "orders",
          () -> {
            vends.incrementAndGet();
            return Optional.of(
                new FloecatConnector.VendedStorageCredentials(
                    Map.of("s3.access-key-id", "ASIA", "s3.secret-access-key", "s"),
                    null,
                    ONE_HOUR));
          },
          SourceCatalogCredentialVendor::connectorExpiry);
    }
    assertThat(vends).hasValue(1);
  }

  @Test
  void aReconfiguredSourceOrARotatedCredentialIsADifferentKey() {
    VendedCredentialCache cache = cache(0.5);
    AtomicInteger vends = new AtomicInteger();
    vend(cache, vends, ONE_HOUR);

    assertThat(
            cache.connectorVend(
                SOURCE.toBuilder().putProperties("s3.region", "eu-west-1").build(),
                SECRET,
                "cat.schema",
                "orders",
                () -> credentials("RECONFIGURED", ONE_HOUR),
                SourceCatalogCredentialVendor::connectorExpiry))
        .hasValueSatisfying(
            answer -> assertThat(answer.properties()).containsValue("RECONFIGURED"));
    AuthCredentials rotated =
        SECRET.toBuilder()
            .setBearer(AuthCredentials.BearerToken.newBuilder().setToken("t2"))
            .build();
    assertThat(
            cache.connectorVend(
                SOURCE,
                rotated,
                "cat.schema",
                "orders",
                () -> credentials("ROTATED", ONE_HOUR),
                SourceCatalogCredentialVendor::connectorExpiry))
        .hasValueSatisfying(answer -> assertThat(answer.properties()).containsValue("ROTATED"));
    assertThat(vend(cache, vends, ONE_HOUR)).isEqualTo("ASIA-1");
  }

  @Test
  void aNonPositiveBoundDisablesTheCache() {
    VendedCredentialCache cache = VendedCredentialCache.of(0.5, 0, clockAt(VENDED), nanos::get);
    AtomicInteger vends = new AtomicInteger();

    vend(cache, vends, ONE_HOUR);
    vend(cache, vends, ONE_HOUR);
    assertThat(vends).hasValue(2);
  }

  @Test
  void rejectsInvalidSettings() {
    assertThatThrownBy(() -> VendedCredentialCache.of(1.5, 10, clockAt(VENDED), nanos::get))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> VendedCredentialCache.of(-0.1, 10, clockAt(VENDED), nanos::get))
        .isInstanceOf(IllegalArgumentException.class);
  }

  private VendedCredentialCache cache(double minRemainingFraction) {
    return VendedCredentialCache.of(minRemainingFraction, 100, clockAt(VENDED), nanos::get);
  }

  private static String vend(VendedCredentialCache cache, AtomicInteger vends, Instant expiresAt) {
    return cache
        .connectorVend(
            SOURCE,
            SECRET,
            "cat.schema",
            "orders",
            () -> credentials("ASIA-" + vends.incrementAndGet(), expiresAt),
            SourceCatalogCredentialVendor::connectorExpiry)
        .orElseThrow()
        .properties()
        .get("s3.access-key-id");
  }

  private static Optional<FloecatConnector.VendedStorageCredentials> credentials(
      String accessKey, Instant expiresAt) {
    return Optional.of(
        new FloecatConnector.VendedStorageCredentials(
            Map.of("s3.access-key-id", accessKey, "s3.secret-access-key", "secret"),
            null,
            expiresAt));
  }

  private static Clock clockAt(Instant instant) {
    return Clock.fixed(instant, ZoneOffset.UTC);
  }
}
