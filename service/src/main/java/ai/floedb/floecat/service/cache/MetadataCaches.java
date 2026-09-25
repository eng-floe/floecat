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
import ai.floedb.floecat.service.metagraph.snapshot.SnapshotRetentionPolicy;
import ai.floedb.floecat.service.repo.cache.BlobCacheAccess;
import ai.floedb.floecat.service.repo.cache.DurablePointerReads;
import ai.floedb.floecat.service.repo.cache.IndexedPointerStore;
import ai.floedb.floecat.service.repo.cache.PlanningPointerIndex;
import ai.floedb.floecat.service.repo.impl.RelationHintsRepository;
import ai.floedb.floecat.service.telemetry.ServiceMetrics;
import ai.floedb.floecat.storage.spi.CachedPointerStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.storage.spi.RawPointerStore;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import ai.floedb.floecat.telemetry.helpers.CacheMetrics;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.LongSupplier;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

/** Builds the single pointer-store seam used by planning, mutation, and maintenance code. */
@ApplicationScoped
public class MetadataCaches {
  private static final Logger LOG = Logger.getLogger(MetadataCaches.class);

  @Produces
  @Singleton
  @CachedPointerStore
  public PointerStore cachedPointerStore(
      @RawPointerStore PointerStore raw, PlanningPointerIndex index) {
    return new IndexedPointerStore(raw, index);
  }

  /**
   * The durable read seam, composed here because this is the one place that may select the raw
   * view. Injected by readers whose emptiness is load-bearing over objects the index never loaded.
   */
  @Produces
  @Singleton
  public DurablePointerReads durablePointerReads(@RawPointerStore PointerStore raw) {
    return new DurablePointerReads(raw);
  }

  /** Callers do not select a cached or durable view; the indexed store makes that decision. */
  @Produces
  @Singleton
  public PointerStore pointerStore(@CachedPointerStore PointerStore indexed) {
    return indexed;
  }

  @Produces
  @Singleton
  public PlanningPointerIndex pointers(
      @RawPointerStore PointerStore raw,
      Observability observability,
      Instance<PlanningPointerIndex.Ownership> configuredOwnership,
      @ConfigProperty(name = "floecat.planner.pointer-index.enabled", defaultValue = "true")
          boolean enabled,
      @ConfigProperty(name = "floecat.planner.pointer-index.max-heap-share", defaultValue = "0.10")
          double maxHeapShare,
      @ConfigProperty(
              name = "floecat.planner.pointer-index.max-total-heap-share",
              defaultValue = "0.15")
          double maxTotalHeapShare,
      @ConfigProperty(name = "floecat.planner.pointer-index.warm-concurrency", defaultValue = "2")
          int warmConcurrency) {
    if (!(maxHeapShare > 0.0) || maxHeapShare > 1.0) {
      throw new IllegalArgumentException(
          "floecat.planner.pointer-index.max-heap-share must be in (0, 1], but was "
              + maxHeapShare);
    }
    if (!(maxTotalHeapShare >= maxHeapShare) || maxTotalHeapShare > 1.0) {
      // A total below the per-account cap would refuse the first account that reached its own
      // cap, which reads as the per-account setting being ignored.
      throw new IllegalArgumentException(
          "floecat.planner.pointer-index.max-total-heap-share must be in [max-heap-share, 1], but"
              + " was "
              + maxTotalHeapShare);
    }
    PlanningPointerIndex.Ownership ownership =
        configuredOwnership.isUnsatisfied()
            ? PlanningPointerIndex.Ownership.ALWAYS_OWNED
            : configuredOwnership.get();
    Tag[] baseTags =
        new Tag[] {Tag.of(TagKey.COMPONENT, "service"), Tag.of(TagKey.OPERATION, "metadata-index")};
    long maxHeapBytes = Runtime.getRuntime().maxMemory();
    long maxBytesPerAccount = (long) (maxHeapBytes * maxHeapShare);
    long maxBytesTotal = (long) (maxHeapBytes * maxTotalHeapShare);
    // Bounded on purpose: warms are whole-account scans, and the common pool would start one per
    // core the moment several accounts go cold together.
    ExecutorService warmExecutor =
        Executors.newFixedThreadPool(
            Math.max(1, warmConcurrency),
            runnable -> {
              Thread thread = new Thread(runnable, "planner-pointer-warm");
              thread.setDaemon(true);
              return thread;
            });
    PlanningPointerIndex index =
        new PlanningPointerIndex(
            raw,
            ownership,
            warmExecutor,
            new PlanningPointerWarmTelemetry(observability, baseTags, maxBytesPerAccount),
            new PlanningPointerIndex.Policy(enabled, maxBytesPerAccount, maxBytesTotal));
    observability.gauge(
        ServiceMetrics.PlanningPointer.ENTRIES,
        index::entryCount,
        "Planner pointer entries resident",
        baseTags);
    observability.gauge(
        ServiceMetrics.PlanningPointer.PARTITIONS,
        index::loadingPartitionCount,
        "Planner pointer partitions still loading",
        append(baseTags, Tag.of(TagKey.RESULT, "loading")));
    observability.gauge(
        ServiceMetrics.PlanningPointer.PARTITIONS,
        index::completePartitionCount,
        "Planner pointer partitions complete",
        append(baseTags, Tag.of(TagKey.RESULT, "complete")));
    observability.gauge(
        ServiceMetrics.PlanningPointer.BYTES,
        index::residentBytes,
        "Planner pointer heap admitted across resident accounts",
        baseTags);
    observability.gauge(
        ServiceMetrics.PlanningPointer.PARTITIONS,
        index::refusedPartitionCount,
        "Planner pointer partitions refused for size",
        append(baseTags, Tag.of(TagKey.RESULT, "refused")));
    return index;
  }

  @Produces
  @ApplicationScoped
  public ObjectCache objects(
      CacheBudgetResolver budgets,
      Observability observability,
      LogicalSchemaMapper schemaMapper,
      SnapshotRetentionPolicy retentionPolicy,
      @ConfigProperty(name = "floecat.cache.object.enabled", defaultValue = "true")
          boolean enabled) {
    var metrics = metricsFor(CacheFamily.OBJECT, observability);
    var cache =
        new ObjectCache(
            budgets.bytesFor(CacheFamily.OBJECT),
            events(metrics),
            schemaMapper,
            enabled,
            retentionPolicy);
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
   * what a miss costs, how full it is, whether loads are failing, and whether it retains what it
   * loads.
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
      public void evicted(long weightBytes) {
        metrics.recordEviction(weightBytes, tags);
      }
    };
  }

  /** One metric shape for each memory-cache family. */
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

  /** One metric shape for each memory-cache family. */
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

  private static Tag[] append(Tag[] base, Tag... extra) {
    Tag[] result = java.util.Arrays.copyOf(base, base.length + extra.length);
    System.arraycopy(extra, 0, result, base.length, extra.length);
    return result;
  }
}
