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

package ai.floedb.floecat.service.repo.cache;

import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.helpers.CacheMetrics;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
import com.google.protobuf.MessageLite;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Process-wide cache of DECODED immutable blob content, keyed by blob URI.
 *
 * <p>Everything a query reads below the two mutable pointer reads (root pointer, name→id) is a
 * content-addressed or frozen blob: the content at a given URI never changes, only pointers move.
 * So this cache needs NO invalidation logic to be correct — an entry is right forever; eviction
 * (byte-weight LRU + idle decay) exists only for memory. It stores decoded values, not bytes: at
 * the sub-millisecond planning budget a re-parse per read (~100–500 µs for a manifest page or
 * snapshot blob) is the cost that matters once the S3 round-trip is gone.
 *
 * <p>The cache is a pure engine: callers pass their existing load-and-parse lambda, so decode logic
 * and exception semantics stay at the repository seams and are never duplicated here. Absence is
 * never cached (a loader returning empty stores nothing — the blob may appear later), and loads are
 * single-flight per URI (a cold hot-table burst collapses to one backend fetch).
 *
 * <p>Correctness contract for callers: only route a read through this cache when the blob is
 * declared immutable — {@code schema.casBlobs} at the repository seams, or frozen-by-contract
 * manifests. This cache serves CONTENT, never existence: liveness probes (HEAD/etag) must keep
 * hitting the live store.
 */
@Singleton
public class ImmutableBlobCache {

  /** Per-entry bookkeeping overhead added to the serialized size (key string + cache node). */
  private static final long ENTRY_OVERHEAD_BYTES = 64L;

  /**
   * Decoded protobuf objects retain roughly 2–5× their WIRE size on the heap (object headers, boxed
   * fields, ByteString copies, repeated-field arrays). The weigher budgets that retained estimate —
   * serializedSize × this factor — so {@code max-weight-bytes} approximates real heap, not wire
   * bytes; without it a "256 MB" cache could pin over 1 GB and defeat operator sizing.
   */
  private static final long RETAINED_HEAP_FACTOR = 3L;

  /** Weight for values that expose no serialized size (non-proto derived forms). */
  private static final int DEFAULT_WEIGHT_BYTES = 1024;

  /** Estimated per-entry overhead inside a cached index map (boxed key + HashMap node). */
  private static final long MAP_ENTRY_OVERHEAD_BYTES = 72L;

  private final Cache<String, Object> cache;
  private final CacheMetrics metrics;
  private final boolean enabled;

  @Inject
  public ImmutableBlobCache(
      Observability observability,
      @ConfigProperty(name = "floecat.blob.cache.enabled", defaultValue = "true") boolean enabled,
      @ConfigProperty(name = "floecat.blob.cache.max-weight-bytes", defaultValue = "268435456")
          long maxWeightBytes,
      @ConfigProperty(name = "floecat.blob.cache.expire-after-access-minutes", defaultValue = "15")
          long expireAfterAccessMinutes) {
    this(
        enabled,
        maxWeightBytes,
        Duration.ofMinutes(expireAfterAccessMinutes),
        new CacheMetrics(observability, "service", "blob-cache", "blob-cache"));
    if (metrics != null) {
      metrics.trackEnabled(() -> this.enabled ? 1 : 0, "Immutable blob cache enabled");
      metrics.trackMaxWeight(() -> maxWeightBytes, "Immutable blob cache max weight (bytes)");
      metrics.trackSize(cache::estimatedSize, "Immutable blob cache entries");
      metrics.trackWeightedSize(
          () -> cache.policy().eviction().map(e -> e.weightedSize().orElse(0L)).orElse(0L),
          "Immutable blob cache weighted size (bytes)");
    }
  }

  /** Test/embedded constructor: no telemetry. */
  public ImmutableBlobCache(boolean enabled, long maxWeightBytes, Duration expireAfterAccess) {
    this(enabled, maxWeightBytes, expireAfterAccess, null);
  }

  private ImmutableBlobCache(
      boolean enabled, long maxWeightBytes, Duration expireAfterAccess, CacheMetrics metrics) {
    this.enabled = enabled;
    this.metrics = metrics;
    var builder =
        Caffeine.newBuilder()
            .maximumWeight(Math.max(1L, maxWeightBytes))
            .weigher((Weigher<String, Object>) ImmutableBlobCache::weight);
    // <=0 means "no idle expiry" (the conventional reading); a literal zero would expire every
    // entry immediately — a permanently cold cache masquerading as enabled.
    if (!expireAfterAccess.isZero() && !expireAfterAccess.isNegative()) {
      builder.expireAfterAccess(expireAfterAccess);
    }
    this.cache = builder.build();
  }

  private static int weight(String uri, Object value) {
    long size = ENTRY_OVERHEAD_BYTES + 2L * uri.length();
    if (value instanceof MessageLite message) {
      size += RETAINED_HEAP_FACTOR * message.getSerializedSize();
    } else if (value instanceof String s) {
      size += 2L * s.length();
    } else if (value instanceof Map<?, ?> map) {
      // Derived indexes (e.g. snapshotId -> manifest entry): weigh the proto values honestly so a
      // wide index cannot hide behind the flat default.
      for (Object v : map.values()) {
        size +=
            MAP_ENTRY_OVERHEAD_BYTES
                + (v instanceof MessageLite m
                    ? RETAINED_HEAP_FACTOR * m.getSerializedSize()
                    : DEFAULT_WEIGHT_BYTES);
      }
    } else if (value instanceof ai.floedb.floecat.metagraph.model.GraphNode node) {
      // Weigh nodes by their dominant retained field — a wide table's schema JSON can be tens of
      // KB to MB; a flat estimate would let #node entries pin heap far beyond the budget.
      long content = DEFAULT_WEIGHT_BYTES;
      if (node instanceof ai.floedb.floecat.metagraph.model.UserTableNode t
          && t.schemaJson() != null) {
        content += 2L * t.schemaJson().length();
      } else if (node instanceof ai.floedb.floecat.metagraph.model.ViewNode v) {
        for (var def : v.sqlDefinitions()) {
          content += 2L * def.getSql().length();
        }
      }
      size += 4L * content; // x4: maps/refs/boxing around the dominant string
    } else {
      size += DEFAULT_WEIGHT_BYTES;
    }
    return (int) Math.min(size, Integer.MAX_VALUE);
  }

  public boolean enabled() {
    return enabled;
  }

  /**
   * The decoded content at {@code uri}, loading (single-flight) through {@code loader} on a miss.
   * An empty loader result is returned as-is and NOT cached; loader exceptions propagate uncached.
   *
   * <p>A URI is always decoded as exactly one type, so the URI-keyed lookup is type-sound; the
   * unchecked cast below encodes that contract.
   */
  @SuppressWarnings("unchecked")
  public <T> Optional<T> get(String uri, Function<String, Optional<T>> loader) {
    if (!enabled || uri == null || uri.isBlank()) {
      return loader.apply(uri);
    }
    // One cache lookup; the miss flag is set inside the single-flighted loader, so a cold burst on
    // one URI counts exactly one miss (the piled-up waiters are, correctly, hits).
    boolean[] missed = new boolean[1];
    Object value =
        cache.get(
            uri,
            k -> {
              missed[0] = true;
              return loader.apply(k).orElse(null);
            });
    if (missed[0]) {
      recordMiss();
    } else {
      recordHit();
    }
    return Optional.ofNullable((T) value);
  }

  /**
   * The cached decodes among {@code uris} — a probe for batch readers, which fetch the misses
   * through their own batched backend read and {@link #put} the results back.
   */
  @SuppressWarnings("unchecked")
  public <T> Map<String, T> getAllPresent(Collection<String> uris) {
    if (!enabled || uris == null || uris.isEmpty()) {
      return Map.of();
    }
    Map<String, Object> present = cache.getAllPresent(uris);
    if (metrics != null) {
      for (int i = present.size(); i > 0; i--) {
        metrics.recordHit();
      }
      for (int i = uris.size() - present.size(); i > 0; i--) {
        metrics.recordMiss();
      }
    }
    // Caffeine's result is already an unmodifiable snapshot; re-typing it is free, copying is not.
    return (Map<String, T>) (Map<String, ?>) present;
  }

  /**
   * Presence probe for probe-then-build-then-put callers (derived indexes, graph nodes). Records a
   * HIT when present — that is real cache serving — but nothing on absence: the caller's build
   * loads through the underlying URI, which does its own miss accounting, and double-counting the
   * probe would deflate the hit ratio on exactly the hottest paths.
   */
  @SuppressWarnings("unchecked")
  public <T> T probe(String uri) {
    if (!enabled || uri == null || uri.isBlank()) {
      return null;
    }
    Object hit = cache.getIfPresent(uri);
    if (hit != null) {
      recordHit();
    }
    return (T) hit;
  }

  /**
   * Populate an entry the caller just wrote or decoded (write-through from the commit/publish
   * funnels, or a batch reader storing fetched misses). A no-op when disabled or for null values.
   */
  public void put(String uri, Object value) {
    if (!enabled || uri == null || uri.isBlank() || value == null) {
      return;
    }
    cache.put(uri, value);
  }

  /** Belt-and-braces eviction (correctness never depends on it — content is immutable). */
  public void invalidate(String uri) {
    if (uri != null) {
      cache.invalidate(uri);
    }
  }

  /** Runs Caffeine's pending maintenance (eviction is async); for tests asserting on bounds. */
  void cleanUp() {
    cache.cleanUp();
  }

  private void recordHit() {
    if (metrics != null) {
      metrics.recordHit();
    }
  }

  private void recordMiss() {
    if (metrics != null) {
      metrics.recordMiss();
    }
  }
}
