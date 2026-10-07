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

import ai.floedb.floecat.cache.CacheEvents;
import ai.floedb.floecat.cache.CaffeineExpiringCache;
import ai.floedb.floecat.cache.ExpiringCache;
import ai.floedb.floecat.catalog.access.CatalogObjectName;
import ai.floedb.floecat.connector.rpc.AuthCredentials;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.integration.rpc.CatalogIntegration;
import ai.floedb.floecat.service.cache.MetadataCaches;
import ai.floedb.floecat.telemetry.Observability;
import com.google.protobuf.Message;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Upstream answers to source-catalog credential vends, reused while enough of their lifetime
 * remains.
 *
 * <p>Keyed by the source record (a {@code Connector}, with the credential it authenticates with, or
 * a {@code CatalogIntegration}), compared by value so a reconfigured source misses, and the
 * upstream table. An answer is served only while at least {@code minRemainingFraction} of its
 * lifetime and at least {@link #MIN_REMAINING} remain. The vendor names each answer's expiry, null
 * for an answer that must not be held, and keeps credentials exchanged from a caller's own token
 * out of the cache. With a fraction of 0, an answer is served until {@link #MIN_REMAINING} before
 * it expires, which is when an Iceberg client refreshes a vended credential. Heap state per
 * process; {@code maxEntries} bounds each source kind, and a non-positive bound disables the cache.
 */
@ApplicationScoped
public class VendedCredentialCache {

  /** The Iceberg client refreshes a vended credential this long before it expires. */
  static final Duration MIN_REMAINING = Duration.ofMinutes(5);

  /**
   * One upstream table as seen through one source configuration. The namespace keeps its segments,
   * so {@code [a.b, c]} and {@code [a, b.c]} are different tables.
   */
  private record Key(Message source, List<String> namespacePath, String tableName) {
    Key {
      Objects.requireNonNull(source, "source");
      namespacePath = List.copyOf(Objects.requireNonNull(namespacePath, "namespacePath"));
      Objects.requireNonNull(tableName, "tableName");
    }
  }

  /** An upstream answer and how long it may be served from when it was vended. */
  private record Held<T>(T answer, Duration holdFor) {}

  @ConfigProperty(
      name = "floecat.storage.source-catalog.vend-cache.min-remaining-fraction",
      defaultValue = "0.5")
  double minRemainingFraction;

  @ConfigProperty(
      name = "floecat.storage.source-catalog.vend-cache.max-entries",
      defaultValue = "10000")
  long maxEntries;

  @Inject Observability observability;

  Clock clock = Clock.systemUTC();

  private ExpiringCache<Key, Held<Optional<FloecatConnector.VendedStorageCredentials>>>
      connectorVends;
  private ExpiringCache<
          Key, Held<Optional<ai.floedb.floecat.catalog.access.VendedStorageCredentials>>>
      integrationVends;

  @PostConstruct
  void init() {
    validate();
    start(
        MetadataCaches.cacheEvents(observability, "source-catalog-vend", "vended-credential"),
        System::nanoTime);
  }

  private void validate() {
    if (!(minRemainingFraction >= 0.0 && minRemainingFraction <= 1.0)) {
      throw new IllegalArgumentException(
          "vend-cache min-remaining-fraction must be in [0, 1]: " + minRemainingFraction);
    }
  }

  private void start(CacheEvents events, LongSupplier ticker) {
    if (maxEntries <= 0) {
      return;
    }
    connectorVends = build(events, ticker);
    integrationVends = build(events, ticker);
  }

  private <T> ExpiringCache<Key, Held<T>> build(CacheEvents events, LongSupplier ticker) {
    return CaffeineExpiringCache.create(maxEntries, Held::holdFor, ticker, events);
  }

  /** A cache with explicit settings, clock and ticker, reporting no telemetry. */
  static VendedCredentialCache of(
      double minRemainingFraction, long maxEntries, Clock clock, LongSupplier ticker) {
    VendedCredentialCache cache = new VendedCredentialCache();
    cache.minRemainingFraction = minRemainingFraction;
    cache.maxEntries = maxEntries;
    cache.clock = clock;
    cache.validate();
    cache.start(CacheEvents.none(), ticker);
    return cache;
  }

  /** A cache that holds nothing: every vend goes upstream. */
  static VendedCredentialCache disabled() {
    return of(0.0, 0L, Clock.systemUTC(), System::nanoTime);
  }

  /**
   * A vend of {@code tableName} in {@code namespacePath} through {@code connector} authenticating
   * with {@code credentials}: a held answer, or {@code vend}'s, held against {@code
   * expiresAt(answer)} and not at all when that is null.
   */
  Optional<FloecatConnector.VendedStorageCredentials> connectorVend(
      Connector connector,
      AuthCredentials credentials,
      List<String> namespacePath,
      String tableName,
      Supplier<Optional<FloecatConnector.VendedStorageCredentials>> vend,
      Function<Optional<FloecatConnector.VendedStorageCredentials>, Instant> expiresAt) {
    return get(
        connectorVends,
        // The credential joins the record, so a secret rotated in the credential store misses.
        new Key(
            connector.toBuilder()
                .setAuth(connector.getAuth().toBuilder().setCredentials(credentials))
                .build(),
            namespacePath,
            tableName),
        vend,
        expiresAt);
  }

  /**
   * A vend of {@code table} through {@code integration}: a held answer, or {@code vend}'s, held
   * against {@code expiresAt(answer)} and not at all when that is null.
   */
  Optional<ai.floedb.floecat.catalog.access.VendedStorageCredentials> integrationVend(
      CatalogIntegration integration,
      CatalogObjectName table,
      Supplier<Optional<ai.floedb.floecat.catalog.access.VendedStorageCredentials>> vend,
      Function<Optional<ai.floedb.floecat.catalog.access.VendedStorageCredentials>, Instant>
          expiresAt) {
    // The name the integration vends, so the key is exactly the upstream table.
    return get(
        integrationVends,
        new Key(integration, table.namespace().segments(), table.name()),
        vend,
        expiresAt);
  }

  private <T> T get(
      ExpiringCache<Key, Held<T>> cache,
      Key key,
      Supplier<T> vend,
      Function<? super T, Instant> expiresAt) {
    if (cache == null) {
      return vend.get();
    }
    return cache
        .get(
            key,
            ignored -> {
              T answer = vend.get();
              return new Held<>(answer, holdFor(clock.instant(), expiresAt.apply(answer)));
            })
        .answer();
  }

  /** How long an answer vended at {@code vendedAt} and expiring at {@code expiresAt} is served. */
  Duration holdFor(Instant vendedAt, Instant expiresAt) {
    if (expiresAt == null || !expiresAt.isAfter(vendedAt)) {
      return Duration.ZERO;
    }
    Duration lifetime = Duration.between(vendedAt, expiresAt);
    // Whole seconds: exact enough for credential lifetimes, and free of overflow for any expiry.
    Duration untilFraction =
        Duration.ofSeconds((long) Math.floor(lifetime.getSeconds() * (1.0 - minRemainingFraction)));
    Duration hold = min(untilFraction, lifetime.minus(MIN_REMAINING));
    return hold.isNegative() ? Duration.ZERO : hold;
  }

  private static Duration min(Duration a, Duration b) {
    return a.compareTo(b) <= 0 ? a : b;
  }
}
