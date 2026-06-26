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
import ai.floedb.floecat.telemetry.Observability.Category;
import ai.floedb.floecat.telemetry.ObservationScope;
import ai.floedb.floecat.telemetry.StoreOperationSummary;
import ai.floedb.floecat.telemetry.StoreTraceScope;
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

  public ObservationScope observe(Tag... extraTags) {
    Tag[] tags = scopeTags(extraTags);
    ObservationScope metricsScope =
        observability.observe(Category.STORE, component(), operation(), tags);
    StoreTraceScope traceScope = observability.storeTraceScope(component(), operation(), tags);
    return new StoreObservationScope(metricsScope, traceScope, component(), operation());
  }

  private static final class StoreObservationScope implements ObservationScope {
    private final ObservationScope metricsScope;
    private final StoreTraceScope traceScope;
    private final String component;
    private final String operation;
    private final boolean recordInSummary;
    private final long startNanos = System.nanoTime();
    private Throwable error;

    StoreObservationScope(
        ObservationScope metricsScope,
        StoreTraceScope traceScope,
        String component,
        String operation) {
      this.metricsScope = metricsScope;
      this.traceScope = traceScope;
      this.component = component;
      this.operation = operation;
      this.recordInSummary = StoreOperationSummary.enterScope();
    }

    @Override
    public void success() {
      error = null;
      metricsScope.success();
      traceScope.success();
    }

    @Override
    public void error(Throwable throwable) {
      error = throwable;
      metricsScope.error(throwable);
      traceScope.error(throwable);
    }

    @Override
    public void retry() {
      metricsScope.retry();
    }

    @Override
    public void status(String status) {
      metricsScope.status(status);
    }

    @Override
    public void close() {
      try {
        if (recordInSummary) {
          StoreOperationSummary.record(
              component,
              operation,
              Duration.ofNanos(Math.max(0L, System.nanoTime() - startNanos)),
              error == null);
        }
        try {
          metricsScope.close();
        } finally {
          traceScope.close();
        }
      } finally {
        StoreOperationSummary.exitScope();
      }
    }
  }

  private void recording(MetricId metric, double amount, String result, Tag... extraTags) {
    List<Tag> dynamic = new ArrayList<>();
    addExtra(dynamic, extraTags);
    observability.counter(metric, amount, metricTagsWithResult(result, dynamic));
  }

  private void recordTimer(MetricId metric, Duration duration, String result, Tag... extraTags) {
    List<Tag> dynamic = new ArrayList<>();
    addExtra(dynamic, extraTags);
    observability.timer(metric, duration, metricTagsWithResult(result, dynamic));
  }

  private Tag[] metricTagsWithResult(String result, List<Tag> extra) {
    List<Tag> tags = new ArrayList<>(metricTagList(extra));
    if (result != null) {
      tags.add(Tag.of(TagKey.RESULT, result));
    }
    return tags.toArray(Tag[]::new);
  }
}
