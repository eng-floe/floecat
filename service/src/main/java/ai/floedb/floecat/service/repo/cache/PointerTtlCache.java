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

import ai.floedb.floecat.common.rpc.MutationMeta;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.time.Duration;

/**
 * The ONE implementation of a short-TTL cache over a mutable pointer family (the deliberately-
 * stale reads: table-definition pointers, table-root pointers), carrying the hardened contract in
 * one place so every pointer cache behaves identically:
 *
 * <ul>
 *   <li><b>Version-guarded populate</b> — {@link #putIfFresher} is a merge that keeps the highest
 *       pointer version, so a straggling pre-commit populate cannot overwrite a resident fresher
 *       entry (a bare put here was a read-your-writes race). The guard holds while the fresh entry
 *       is resident; if it expires/evicts before an extremely late straggler arrives, the stale
 *       meta can serve for at most one further TTL — the same bound the cross-instance contract
 *       accepts. Sub-TTL freshness is a best effort, not an absolute ordering guarantee.
 *   <li><b>Absence is never cached</b> — a blank-blobUri meta stores nothing, so a resource's first
 *       write is visible on the next read.
 *   <li><b>Write paths invalidate, then repopulate from a LIVE read</b> (caller's job via {@link
 *       #invalidate} + a live read fed back through {@link #putIfFresher}); the fresh,
 *       higher-version meta then outruns any straggling stale populate.
 *   <li><b>Currency only, never liveness</b> — a cached meta says which immutable blob is current;
 *       it proves nothing about a blob still existing. Liveness probes must hit the live store.
 * </ul>
 *
 * <p>TTL 0 disables the cache entirely (every read is live); serving staleness is bounded by the
 * TTL cross-instance and by the invalidate+repopulate contract on the writing instance.
 */
public final class PointerTtlCache<K> {

  private static final long DEFAULT_MAX_ENTRIES = 100_000;

  private final Cache<K, MutationMeta> cache; // null = disabled
  private final long maxEntries;

  public PointerTtlCache(long ttlSeconds) {
    this(ttlSeconds, DEFAULT_MAX_ENTRIES);
  }

  public PointerTtlCache(long ttlSeconds, long maxEntries) {
    this.maxEntries = Math.max(1L, maxEntries);
    this.cache =
        ttlSeconds > 0
            ? Caffeine.newBuilder()
                .maximumSize(this.maxEntries)
                .expireAfterWrite(Duration.ofSeconds(ttlSeconds))
                .build()
            : null;
  }

  /** The entry bound that actually governs this cache (for gauges). */
  public long maxEntries() {
    return maxEntries;
  }

  public boolean enabled() {
    return cache != null;
  }

  /** The cached meta, or {@code null} when disabled or absent. */
  public MutationMeta getIfPresent(K key) {
    return cache == null ? null : cache.getIfPresent(key);
  }

  /**
   * Version-guarded populate: stores {@code live} unless a FRESHER (higher pointer version) entry
   * is already cached. What counts as "absent" differs per pointer family (blank blob uri for
   * roots, an empty repository read for graph metas), so ABSENCE FILTERING IS THE CALLER'S JOB —
   * never feed a meta that represents absence into this cache.
   */
  public void putIfFresher(K key, MutationMeta live) {
    if (cache == null || live == null) {
      return;
    }
    // NOT a compute-based merge: Caffeine resets expireAfterWrite on every compute write, so a
    // LOSING stale populate would still extend the resident entry's lifetime past the documented
    // <=TTL bound. Losers must leave the winner's clock untouched; only a strictly fresher meta
    // writes (and legitimately restarts the TTL — it IS a fresh live observation).
    for (; ; ) {
      MutationMeta cur = cache.asMap().putIfAbsent(key, live);
      if (cur == null || live.getPointerVersion() <= cur.getPointerVersion()) {
        return;
      }
      if (cache.asMap().replace(key, cur, live)) {
        return;
      }
    }
  }

  public void invalidate(K key) {
    if (cache != null) {
      cache.invalidate(key);
    }
  }

  /** Estimated resident entries, for gauges. */
  public long estimatedSize() {
    return cache == null ? 0L : cache.estimatedSize();
  }
}
