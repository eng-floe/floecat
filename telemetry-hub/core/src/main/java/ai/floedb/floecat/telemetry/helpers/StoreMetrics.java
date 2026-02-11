package ai.floedb.floecat.telemetry.helpers;

import ai.floedb.floecat.telemetry.MetricId;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** Helper that emits store-related metrics with canonical tags. */
public final class StoreMetrics extends BaseMetrics {
  private final Observability observability;

  public StoreMetrics(
      Observability observability, String component, String operation, Tag... tags) {
    super(component, operation, tags);
    this.observability = observability;
  }

  public void recordRequest(String result, Tag... extraTags) {
    recording(Telemetry.Metrics.STORE_REQUESTS, 1, result, extraTags);
  }

  public void recordLatency(Duration duration, String result, Tag... extraTags) {
    recordTimer(Telemetry.Metrics.STORE_LATENCY, duration, result, extraTags);
  }

  public void recordBytes(double bytes, String result, Tag... extraTags) {
    recording(Telemetry.Metrics.STORE_BYTES, bytes, result, extraTags);
  }

  private void recording(MetricId metric, double amount, String result, Tag... extraTags) {
    List<Tag> dynamic = new ArrayList<>();
    dynamic.add(Tag.of(TagKey.RESULT, result));
    addExtra(dynamic, extraTags);
    observability.counter(metric, amount, metricTags(dynamic));
  }

  private void recordTimer(MetricId metric, Duration duration, String result, Tag... extraTags) {
    List<Tag> dynamic = new ArrayList<>();
    dynamic.add(Tag.of(TagKey.RESULT, result));
    addExtra(dynamic, extraTags);
    observability.timer(metric, duration, metricTags(dynamic));
  }
}
