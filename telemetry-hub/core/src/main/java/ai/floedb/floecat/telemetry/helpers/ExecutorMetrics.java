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
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/** Helper that records executor-specific metrics through the telemetry hub. */
public final class ExecutorMetrics extends BaseMetrics {
  private static final String QUEUE_DESC = "Number of queued tasks in the pool";
  private static final String ACTIVE_DESC = "Number of active threads executing tasks";

  private final Observability observability;
  private final AtomicBoolean queueRegistered = new AtomicBoolean();
  private final AtomicBoolean activeRegistered = new AtomicBoolean();

  public ExecutorMetrics(
      Observability observability, String component, String operation, String pool) {
    super(component, operation, Tag.of(TagKey.POOL, pool));
    this.observability = Objects.requireNonNull(observability, "observability");
  }

  public void gaugeQueueDepth(Supplier<Number> supplier) {
    registerGauge(Telemetry.Metrics.EXEC_QUEUE_DEPTH, supplier, QUEUE_DESC, queueRegistered);
  }

  public void gaugeActive(Supplier<Number> supplier) {
    registerGauge(Telemetry.Metrics.EXEC_ACTIVE, supplier, ACTIVE_DESC, activeRegistered);
  }

  public void counterRejected(double amount) {
    if (amount <= 0) {
      return;
    }
    observability.counter(Telemetry.Metrics.EXEC_REJECTED, amount, metricTags());
  }

  public void timerTaskWait(Duration duration) {
    if (duration == null) {
      return;
    }
    observability.timer(Telemetry.Metrics.EXEC_TASK_WAIT, duration, metricTags());
  }

  public void timerTaskRun(Duration duration) {
    if (duration == null) {
      return;
    }
    observability.timer(Telemetry.Metrics.EXEC_TASK_RUN, duration, metricTags());
  }

  private void registerGauge(
      MetricId metric, Supplier<Number> supplier, String description, AtomicBoolean guard) {
    Objects.requireNonNull(supplier, "supplier");
    if (guard.compareAndSet(false, true)) {
      observability.gauge(metric, supplier, description, metricTags());
    }
  }
}
