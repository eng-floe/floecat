package ai.floedb.floecat.telemetry;

import java.time.Duration;
import java.util.function.Supplier;

/** Interface that application code uses to emit telemetry. */
public interface Observability {

  /** Metric categories supported by the hub. */
  enum Category {
    RPC,
    STORE,
    CACHE,
    BACKGROUND,
    GC,
    OTHER
  }

  void counter(MetricId metric, double amount, Tag... tags);

  void summary(MetricId metric, double value, Tag... tags);

  void timer(MetricId metric, Duration duration, Tag... tags);

  <T extends Number> void gauge(
      MetricId metric, Supplier<T> supplier, String description, Tag... tags);

  ObservationScope observe(Category category, String component, String operation, Tag... tags);
}
