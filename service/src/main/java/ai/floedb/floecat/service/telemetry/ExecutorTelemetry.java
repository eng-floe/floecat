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

package ai.floedb.floecat.service.telemetry;

import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.helpers.ExecutorMetrics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class ExecutorTelemetry {
  private static final String WORKER_POOL = "vert.x-worker-thread";
  private static final String INTERNAL_BLOCKING_POOL = "vert.x-internal-blocking";

  private final MeterRegistry meterRegistry;
  private final ExecutorMetrics workerPoolMetrics;
  private final ExecutorMetrics internalBlockingMetrics;
  private final Map<String, Double> lastRejected = new ConcurrentHashMap<>();

  @Inject
  public ExecutorTelemetry(Observability observability, MeterRegistry meterRegistry) {
    this.meterRegistry = meterRegistry;
    this.workerPoolMetrics =
        new ExecutorMetrics(observability, "service", "worker.pool", WORKER_POOL);
    this.internalBlockingMetrics =
        new ExecutorMetrics(observability, "service", "worker.pool", INTERNAL_BLOCKING_POOL);

    workerPoolMetrics.gaugeQueueDepth(() -> gaugeValue("worker_pool_queue_size", WORKER_POOL));
    workerPoolMetrics.gaugeActive(() -> gaugeValue("worker_pool_active", WORKER_POOL));
    internalBlockingMetrics.gaugeQueueDepth(
        () -> gaugeValue("worker_pool_queue_size", INTERNAL_BLOCKING_POOL));
    internalBlockingMetrics.gaugeActive(
        () -> gaugeValue("worker_pool_active", INTERNAL_BLOCKING_POOL));
  }

  @Scheduled(every = "30s")
  void sampleRejectedCounts() {
    sampleRejected(WORKER_POOL, workerPoolMetrics);
    sampleRejected(INTERNAL_BLOCKING_POOL, internalBlockingMetrics);
  }

  private void sampleRejected(String poolName, ExecutorMetrics metrics) {
    double current = counterValue("worker_pool_rejected_total", poolName);
    double previous = lastRejected.getOrDefault(poolName, current);
    double delta = current - previous;
    if (delta > 0) {
      metrics.counterRejected(delta);
    }
    lastRejected.put(poolName, current);
  }

  private double gaugeValue(String metric, String poolName) {
    Gauge gauge = meterRegistry.find(metric).tag("pool_name", poolName).gauge();
    return gauge == null ? 0.0 : gauge.value();
  }

  private double counterValue(String metric, String poolName) {
    Counter counter = meterRegistry.find(metric).tag("pool_name", poolName).counter();
    return counter == null ? 0.0 : counter.count();
  }
}
