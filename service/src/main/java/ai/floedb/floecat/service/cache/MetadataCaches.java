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

package ai.floedb.floecat.service.cache;

import ai.floedb.floecat.cache.BlobCache;
import ai.floedb.floecat.cache.BlobCacheEvents;
import ai.floedb.floecat.cache.CacheEvents;
import ai.floedb.floecat.cache.CacheFamily;
import ai.floedb.floecat.cache.DiskBlobCache;
import ai.floedb.floecat.connector.common.resolver.LogicalSchemaMapper;
import ai.floedb.floecat.service.concurrent.MetadataFanout;
import ai.floedb.floecat.service.repo.cache.AuthoritativePointerStore;
import ai.floedb.floecat.service.repo.cache.BlobCacheAccess;
import ai.floedb.floecat.service.repo.cache.CachingPointerStore;
import ai.floedb.floecat.service.repo.cache.PointerCache;
import ai.floedb.floecat.service.repo.impl.RelationHintsRepository;
import ai.floedb.floecat.storage.spi.CachedPointerStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.storage.spi.RawPointerStore;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import ai.floedb.floecat.telemetry.helpers.CacheMetrics;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import java.nio.file.Path;
import java.time.Duration;
import java.util.function.LongSupplier;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/** Where the pointer cache is built, and the pointer store that wraps it. */
@ApplicationScoped
public class MetadataCaches {

  /**
   * The query-serving pointer view: the real store with its cache in front, or the real store alone
   * when the cache is switched off. The raw store is reachable only by asking for it with {@link
   * RawPointerStore}, which nothing but this does.
   *
   * <p>Off means the decorator is not installed, not that a cache is installed and holds nothing. A
   * budget of zero is refused at startup rather than read as "off" -- a cache sized zero reports a
   * 0% hit rate, which looks like a cache that is not helping rather than one that was turned off.
   * So disabling is its own switch, and it is the operator's way back to the pre-cache read path
   * without a rollback.
   */
  // @Singleton, not @ApplicationScoped: a normal scope is injected as a client proxy, and a proxy
  // is not the object -- an instanceof or a field read sees the proxy, not what it stands for.
  // Nothing here needs scope semantics, and the proxy only hides what is wired.
  @Produces
  @Singleton
  @CachedPointerStore
  public PointerStore cachedPointerStore(
      @RawPointerStore PointerStore raw,
      PointerCache pointers,
      @ConfigProperty(name = "floecat.cache.pointer.enabled", defaultValue = "true")
          boolean enabled) {
    return enabled ? new CachingPointerStore(raw, pointers) : raw;
  }

  /**
   * The safe default for mutation, GC and reconciliation code. Ordinary reads on this view are
   * authoritative; query-serving code opts into {@link CachedPointerStore} once at injection.
   */
  @Produces
  @Singleton
  public PointerStore pointerStore(@CachedPointerStore PointerStore cached) {
    return AuthoritativePointerStore.of(cached);
  }

  /**
   * Pointers: a complete sorted index for SQL addressing plus an admission-controlled remainder,
   * sharing one family budget and one telemetry contract.
   */
  @Produces
  @ApplicationScoped
  public PointerCache pointers(
      @RawPointerStore PointerStore raw,
      CacheBudgetResolver budgets,
      Observability observability,
      @ConfigProperty(name = "floecat.cache.pointer.enabled", defaultValue = "true")
          boolean enabled,
      @ConfigProperty(name = "floecat.cache.pointer.load-parallelism", defaultValue = "0")
          int configuredLoadParallelism,
      @ConfigProperty(name = "floecat.cache.pointer.degraded-retry-seconds", defaultValue = "30")
          long degradedRetrySeconds) {
    var metrics = metricsFor(CacheFamily.POINTER, observability);
    int loadParallelism =
        configuredLoadParallelism == 0
            ? Runtime.getRuntime().availableProcessors()
            : configuredLoadParallelism;
    if (loadParallelism < 1) {
      throw new IllegalArgumentException(
          "floecat.cache.pointer.load-parallelism must be >= 0; zero derives from available"
              + " processors");
    }
    if (degradedRetrySeconds < 1L) {
      throw new IllegalArgumentException(
          "floecat.cache.pointer.degraded-retry-seconds must be >= 1");
    }
    var cache =
        new PointerCache(
            AuthoritativePointerStore.of(raw),
            budgets.bytesFor(CacheFamily.POINTER),
            events(metrics),
            loadParallelism == 1
                ? MetadataFanout.serial()
                : MetadataFanout.concurrent(loadParallelism),
            Duration.ofSeconds(degradedRetrySeconds));
    report(cache, budgets, metrics, enabled);
    return cache;
  }

  /** Objects: decoded engine-neutral SQL metadata under one admission-controlled byte budget. */
  @Produces
  @ApplicationScoped
  public ObjectCache objects(
      CacheBudgetResolver budgets,
      Observability observability,
      LogicalSchemaMapper schemaMapper,
      @ConfigProperty(name = "floecat.cache.object.enabled", defaultValue = "true")
          boolean enabled) {
    var metrics = metricsFor(CacheFamily.OBJECT, observability);
    var cache =
        new ObjectCache(
            budgets.bytesFor(CacheFamily.OBJECT), events(metrics), schemaMapper, enabled);
    report(cache.family(), cache::entryCount, cache::bytes, budgets, metrics, cache.enabled());
    return cache;
  }

  /** Hints: decoded engine-specific relation metadata under its own heap budget. */
  @Produces
  @ApplicationScoped
  public HintCache hints(
      RelationHintsRepository repository,
      CacheBudgetResolver budgets,
      Observability observability,
      @ConfigProperty(name = "floecat.cache.hint.enabled", defaultValue = "true") boolean enabled) {
    var metrics = metricsFor(CacheFamily.HINT, observability);
    var cache =
        new HintCache(repository, budgets.bytesFor(CacheFamily.HINT), events(metrics), enabled);
    report(cache.family(), cache::entryCount, cache::bytes, budgets, metrics, cache.enabled());
    return cache;
  }

  /** Blobs: serialized immutable bodies on local disk, with no resident heap index. */
  @Produces
  @Singleton
  public BlobCache blobs(
      Observability observability,
      @ConfigProperty(name = "floecat.cache.blob.disk.enabled", defaultValue = "false")
          boolean enabled,
      @ConfigProperty(name = "floecat.cache.blob.disk.path", defaultValue = "/mnt/nvme/floecat")
          String path,
      @ConfigProperty(name = "floecat.cache.blob.disk.max-bytes", defaultValue = "108447924224")
          long maxBytes,
      @ConfigProperty(
              name = "floecat.cache.blob.disk.mmap-threshold-bytes",
              defaultValue = "262144")
          int mmapThresholdBytes,
      @ConfigProperty(
              name = "floecat.cache.blob.disk.access-touch-interval-seconds",
              defaultValue = "60")
          long accessTouchIntervalSeconds) {
    var metrics = metricsFor(CacheFamily.BLOB, observability);
    BlobCache cache =
        enabled
            ? new DiskBlobCache(
                Path.of(path),
                maxBytes,
                mmapThresholdBytes,
                Duration.ofSeconds(accessTouchIntervalSeconds),
                blobEvents(metrics))
            : BlobCache.disabled();
    metrics.trackEnabled(() -> cache.enabled() ? 1.0 : 0.0, "Whether the blob cache is enabled");
    metrics.trackSize(cache::entryCount, "Entries held by the blob cache");
    metrics.trackWeightedSize(cache::bytes, "Physical bytes held by the blob cache");
    metrics.trackMaxWeight(cache::maxBytes, "Disk byte budget for the blob cache");
    metrics.trackLiveMappings(
        cache::liveMappings, "Mapped blob-cache entries protected from reclamation");
    return cache;
  }

  @Produces
  @Singleton
  public BlobCacheAccess blobAccess(BlobCache cache) {
    return new BlobCacheAccess(cache);
  }

  private static CacheMetrics metricsFor(CacheFamily family, Observability observability) {
    return new CacheMetrics(observability, "service", "metadata-cache", family.tag());
  }

  /**
   * Counts hits and misses and times the loads. Enough, with the gauges below, to answer the
   * questions asked of a cache that is behaving oddly: whether it is on, whether it is being used,
   * what a miss costs, how full it is, whether loads are failing, and whether it has stopped
   * retaining what it loads, and whether write-through publications are applied or safety-guarded.
   */
  private static CacheEvents events(CacheMetrics metrics, Tag... tags) {
    return new CacheEvents() {
      @Override
      public CacheEvents forAccount(String accountId) {
        return events(metrics, Tag.of(TagKey.ACCOUNT, accountId));
      }

      @Override
      public void hit(java.time.Duration served) {
        metrics.recordHit(tags);
        metrics.recordLoad(served, true, tags);
      }

      @Override
      public void miss() {
        metrics.recordMiss(tags);
      }

      @Override
      public void loadTime(java.time.Duration elapsed) {
        metrics.recordLoad(elapsed, false, tags);
      }

      @Override
      public void loadFailed(java.time.Duration elapsed, RuntimeException error) {
        metrics.recordLoadFailure(elapsed, error, tags);
      }

      @Override
      public void loadDiscarded() {
        metrics.recordLoadDiscarded(tags);
      }

      @Override
      public void admissionRejected() {
        metrics.recordAdmissionRejected(tags);
      }

      @Override
      public void writeThrough(WriteThroughResult result) {
        metrics.recordWriteThrough(result == WriteThroughResult.APPLIED, tags);
      }

      @Override
      public void evicted(long weightBytes) {
        metrics.recordEviction(weightBytes, tags);
      }
    };
  }

  private static BlobCacheEvents blobEvents(CacheMetrics metrics) {
    return new BlobCacheEvents() {
      @Override
      public void hit(Duration served) {
        metrics.recordHit();
        metrics.recordLoad(served, true);
      }

      @Override
      public void miss() {
        metrics.recordMiss();
      }

      @Override
      public void loadTime(Duration elapsed) {
        metrics.recordLoad(elapsed, false);
      }

      @Override
      public void loadFailed(Duration elapsed, RuntimeException error) {
        metrics.recordLoadFailure(elapsed, error);
      }

      @Override
      public void admissionRejected() {
        metrics.recordAdmissionRejected();
      }

      @Override
      public void evicted(long weightBytes) {
        metrics.recordEviction(weightBytes);
      }

      @Override
      public void corrupted(long bytes) {
        metrics.recordCorruption();
      }

      @Override
      public void swept(BlobCache.SweepResult result) {
        metrics.recordSweep(result.bytesReclaimed());
      }
    };
  }

  /**
   * One metric shape for every family, registered where the cache is built rather than inside it.
   * Keyed by the family tag, so families are directly comparable and adding a cache brings its
   * telemetry with it instead of being remembered separately.
   */
  private static void report(
      PointerCache cache, CacheBudgetResolver budgets, CacheMetrics metrics, boolean enabled) {
    report(cache.family(), cache::entryCount, cache::bytes, budgets, metrics, enabled);
    String tag = cache.family().tag();
    metrics.trackAccounts(
        cache::loadingAccountCount,
        "Accounts whose complete pointer index is loading",
        Tag.of(TagKey.RESULT, "loading"));
    metrics.trackAccounts(
        cache::completeAccountCount,
        "Accounts with a complete pointer index",
        Tag.of(TagKey.RESULT, "complete"));
    metrics.trackAccounts(
        cache::degradedAccountCount,
        "Accounts falling back to the pointer store",
        Tag.of(TagKey.RESULT, "degraded"));
    // Hits, misses, load latency and evictions are not registered here: they are events, recorded
    // per read by the cache itself through CacheEvents, not gauges sampled from a running total.
  }

  private static void report(
      CacheFamily family,
      LongSupplier entryCount,
      LongSupplier bytes,
      CacheBudgetResolver budgets,
      CacheMetrics metrics,
      boolean enabled) {
    String tag = family.tag();
    // Fixed at construction, so both gauges read the same captured value rather than one of them
    // re-resolving the budget on every scrape.
    long budget = budgets.bytesFor(family);
    metrics.trackEnabled(() -> enabled ? 1.0 : 0.0, "Whether the " + tag + " cache is enabled");
    metrics.trackSize(() -> entryCount.getAsLong(), "Entries held by the " + tag + " cache");
    metrics.trackWeightedSize(
        () -> (double) bytes.getAsLong(), "Retained bytes held by the " + tag + " cache");
    metrics.trackMaxWeight(() -> (double) budget, "Byte budget for the " + tag + " cache");
  }
}
