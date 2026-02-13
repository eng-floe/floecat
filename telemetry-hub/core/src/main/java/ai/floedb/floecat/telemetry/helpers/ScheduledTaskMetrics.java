package ai.floedb.floecat.telemetry.helpers;

import ai.floedb.floecat.telemetry.MetricId;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import java.util.Objects;
import java.util.function.Supplier;

public final class ScheduledTaskMetrics extends BaseMetrics {
  private final Observability observability;

  public ScheduledTaskMetrics(
      Observability observability, String component, String operation, String taskName) {
    super(component, operation, Tag.of(TagKey.TASK, taskName));
    this.observability = Objects.requireNonNull(observability, "observability");
  }

  public void gaugeEnabled(Supplier<? extends Number> supplier, String description) {
    registerGauge(Telemetry.Metrics.TASK_ENABLED, supplier, description);
  }

  public void gaugeRunning(Supplier<? extends Number> supplier, String description) {
    registerGauge(Telemetry.Metrics.TASK_RUNNING, supplier, description);
  }

  public void gaugeLastTickStart(Supplier<? extends Number> supplier, String description) {
    registerGauge(Telemetry.Metrics.TASK_LAST_TICK_START, supplier, description);
  }

  public void gaugeLastTickEnd(Supplier<? extends Number> supplier, String description) {
    registerGauge(Telemetry.Metrics.TASK_LAST_TICK_END, supplier, description);
  }

  private void registerGauge(
      MetricId metric, Supplier<? extends Number> supplier, String description) {
    observability.gauge(metric, safeSupplier(supplier), description, metricTags());
  }
}
