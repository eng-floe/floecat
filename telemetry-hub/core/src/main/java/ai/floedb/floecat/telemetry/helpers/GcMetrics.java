package ai.floedb.floecat.telemetry.helpers;

import ai.floedb.floecat.telemetry.MetricId;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** Helper that emits GC-related metrics with canonical tags. */
public final class GcMetrics extends BaseMetrics {
  private final Observability observability;

  public GcMetrics(Observability observability, String component, String operation, String gcName) {
    super(component, operation, Tag.of(TagKey.GC_NAME, gcName));
    this.observability = observability;
  }

  public void recordCollection(double count, Tag... extraTags) {
    record(Telemetry.Metrics.GC_COLLECTIONS, count, extraTags);
  }

  public void recordPause(Duration duration, Tag... extraTags) {
    List<Tag> dynamic = buildDynamicTags(extraTags);
    observability.timer(Telemetry.Metrics.GC_PAUSE, duration, metricTags(dynamic));
  }

  private void record(MetricId metric, double amount, Tag... extraTags) {
    List<Tag> dynamic = buildDynamicTags(extraTags);
    observability.counter(metric, amount, metricTags(dynamic));
  }

  private List<Tag> buildDynamicTags(Tag... extraTags) {
    List<Tag> dynamic = new ArrayList<>();
    addExtra(dynamic, extraTags);
    return dynamic;
  }
}
