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

package ai.floedb.floecat.service.metagraph.cache;

import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import ai.floedb.floecat.telemetry.helpers.CacheMetrics;
import java.time.Duration;
import java.util.Objects;

/**
 * Mutation-meta (pointer) cache manager for the user graph.
 *
 * <p>Derived nodes no longer live here: they are pure functions of their source blob and are held
 * content-keyed in the process-wide {@code ImmutableBlobCache}. What remains is the TTL-bounded
 * pointer cache plus the node-load latency instrumentation.
 *
 * <p>Consistency model: the optional mutation-meta cache is best-effort and eventually consistent.
 * After a write, callers may observe a stale pointer version for at most the configured meta cache
 * TTL window.
 */
public class GraphCacheManager {

  private final boolean cacheEnabled;
  private final boolean metaCacheEnabled;
  private final Observability observability;
  private final CacheMetrics cacheMetrics;
  private final CacheMetrics metaCacheMetrics;
  // The shared hardened pointer-cache implementation (version-guarded populate, absence never
  // cached) — same contract as the root-pointer cache in TableRootRepository, one implementation.
  private final ai.floedb.floecat.service.repo.cache.PointerTtlCache<ResourceId> metaCache;

  public GraphCacheManager(
      boolean cacheEnabled,
      long cacheMaxSize,
      long metaCacheTtlSeconds,
      Observability observability) {
    this.cacheEnabled = cacheEnabled;
    this.metaCacheEnabled = metaCacheTtlSeconds > 0;
    this.observability = Objects.requireNonNull(observability, "observability");
    this.metaCache =
        new ai.floedb.floecat.service.repo.cache.PointerTtlCache<>(
            metaCacheTtlSeconds, Math.max(1L, cacheMaxSize));
    this.cacheMetrics =
        new CacheMetrics(this.observability, "service", "graph-cache", "graph-cache");
    this.metaCacheMetrics =
        new CacheMetrics(this.observability, "service", "graph-cache", "graph-meta-cache");
    metaCacheMetrics.trackEnabled(
        () -> metaCacheEnabled ? 1.0 : 0.0, "Graph metadata cache enabled");
    metaCacheMetrics.trackMaxEntries(
        () -> metaCacheEnabled ? (double) metaCache.maxEntries() : 0.0,
        "Graph metadata cache max entries (the bound that actually governs the cache)");
    metaCacheMetrics.trackSize(
        () -> (double) metaCache.estimatedSize(), "Estimated graph metadata cache entries");
  }

  public GraphCacheManager(boolean cacheEnabled, long cacheMaxSize, Observability observability) {
    this(cacheEnabled, cacheMaxSize, 0L, observability);
  }

  /** Evicts the cached pointer meta; derived nodes are content-keyed and need no invalidation. */
  public void invalidate(ResourceId id) {
    metaCache.invalidate(id);
  }

  public MutationMeta getMeta(ResourceId id) {
    if (!metaCache.enabled()) {
      return null;
    }
    MutationMeta meta = metaCache.getIfPresent(id);
    incrementMetaCounter(id.getAccountId(), meta != null);
    return meta;
  }

  public void putMeta(ResourceId id, MutationMeta meta) {
    Objects.requireNonNull(meta, "meta");
    // Absence filtering is this seam's job (see PointerTtlCache.putIfFresher): a missing pointer
    // arrives as a blank-URI meta, and caching it would evict real entries from the size-bounded
    // cache without ever serving anything — resolve refuses blank URIs and re-reads live anyway.
    if (meta.getBlobUri().isBlank()) {
      return;
    }
    // Version-guarded (see PointerTtlCache): a reader that read the pointer BEFORE a concurrent
    // DDL commit can no longer overwrite the commit's fresher entry after the writer's invalidate
    // — the same read-your-writes hardening the root-pointer cache received.
    metaCache.putIfFresher(id, meta);
  }

  private void incrementMetaCounter(String accountId, boolean hit) {
    if (accountId == null || accountId.isBlank()) {
      return;
    }
    Tag accountTag = Tag.of(TagKey.ACCOUNT, accountId);
    if (hit) {
      metaCacheMetrics.recordHit(accountTag);
    } else {
      metaCacheMetrics.recordMiss(accountTag);
    }
  }

  public void recordLoad(Duration duration) {
    if (duration == null) {
      return;
    }
    if (!cacheEnabled) {
      return;
    }
    cacheMetrics.recordLoad(duration, true);
  }

  public void recordLoadFailure(Duration duration, Throwable error) {
    if (duration == null || !cacheEnabled) {
      return;
    }
    cacheMetrics.recordLoadFailure(duration, error);
  }
}
