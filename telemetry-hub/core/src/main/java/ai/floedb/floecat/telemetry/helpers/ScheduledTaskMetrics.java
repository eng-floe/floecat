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
