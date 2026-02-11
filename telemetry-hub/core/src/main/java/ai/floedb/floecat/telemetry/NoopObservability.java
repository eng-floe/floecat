package ai.floedb.floecat.telemetry;

import java.time.Duration;
import java.util.function.Supplier;

/** No-op implementation of {@link Observability}. */
public final class NoopObservability implements Observability {
  private static final ObservationScope NOOP_SCOPE =
      new ObservationScope() {
        @Override
        public void success() {}

        @Override
        public void error(Throwable throwable) {}

        @Override
        public void retry() {}
      };

  @Override
  public void counter(MetricId metric, double amount, Tag... tags) {}

  @Override
  public void summary(MetricId metric, double value, Tag... tags) {}

  @Override
  public void timer(MetricId metric, Duration duration, Tag... tags) {}

  @Override
  public <T extends Number> void gauge(
      MetricId metric, Supplier<T> supplier, String description, Tag... tags) {}

  @Override
  public ObservationScope observe(
      Category category, String component, String operation, Tag... tags) {
    return NOOP_SCOPE;
  }
}
