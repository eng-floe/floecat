package ai.floedb.floecat.telemetry.helpers;

import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import java.util.Objects;
import java.util.function.Supplier;

/** Helper that emits cache-related metrics with canonical tags. */
public final class CacheMetrics extends BaseMetrics {
  private final Observability observability;

  public CacheMetrics(
      Observability observability,
      String component,
      String operation,
      String cacheName,
      Tag... tags) {
    super(component, operation, tagged(cacheName, tags));
    this.observability = Objects.requireNonNull(observability, "observability");
  }

  private static Tag[] tagged(String cacheName, Tag... tags) {
    Tag[] result = new Tag[(tags == null ? 0 : tags.length) + 1];
    result[0] = Tag.of(TagKey.CACHE_NAME, cacheName);
    if (tags != null && tags.length > 0) {
      System.arraycopy(tags, 0, result, 1, tags.length);
    }
    return result;
  }

  public void recordHit(Tag... extraTags) {
    observability.counter(Telemetry.Metrics.CACHE_HITS, 1, metricTags(extraTags));
  }

  public void recordMiss(Tag... extraTags) {
    observability.counter(Telemetry.Metrics.CACHE_MISSES, 1, metricTags(extraTags));
  }

  public void trackSize(Supplier<? extends Number> supplier, String description, Tag... extraTags) {
    observability.gauge(Telemetry.Metrics.CACHE_SIZE, supplier, description, metricTags(extraTags));
  }
}
