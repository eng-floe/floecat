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
package ai.floedb.floecat.telemetry;

import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/** Lightweight observability implementation for tests. */
public final class TestObservability implements Observability {
  private final Map<MetricId, Double> counters = new LinkedHashMap<>();
  private final Map<MetricId, List<List<Tag>>> counterTags = new LinkedHashMap<>();
  private final Map<MetricId, List<Double>> summaries = new LinkedHashMap<>();
  private final Map<MetricId, List<List<Tag>>> summaryTags = new LinkedHashMap<>();
  private final Map<MetricId, List<Duration>> timers = new LinkedHashMap<>();
  private final Map<MetricId, List<List<Tag>>> timerTags = new LinkedHashMap<>();
  private final Map<MetricId, Supplier<? extends Number>> gauges = new LinkedHashMap<>();
  private final Map<MetricId, List<Tag>> gaugeTags = new LinkedHashMap<>();
  private final Map<String, List<TestObservationScope>> scopes = new LinkedHashMap<>();

  @Override
  public void counter(MetricId metric, double amount, Tag... tags) {
    counters.merge(metric, amount, Double::sum);
    counterTags.computeIfAbsent(metric, key -> new ArrayList<>()).add(copyTags(tags));
  }

  @Override
  public void summary(MetricId metric, double value, Tag... tags) {
    summaries.computeIfAbsent(metric, key -> new ArrayList<>()).add(value);
    summaryTags.computeIfAbsent(metric, key -> new ArrayList<>()).add(copyTags(tags));
  }

  @Override
  public void timer(MetricId metric, Duration duration, Tag... tags) {
    timers.computeIfAbsent(metric, key -> new ArrayList<>()).add(duration);
    timerTags.computeIfAbsent(metric, key -> new ArrayList<>()).add(copyTags(tags));
  }

  @Override
  public <T extends Number> void gauge(
      MetricId metric, Supplier<T> supplier, String description, Tag... tags) {
    gauges.put(metric, Objects.requireNonNull(supplier, "supplier"));
    gaugeTags.put(metric, copyTags(tags));
  }

  @Override
  public ObservationScope observe(
      Category category, String component, String operation, Tag... tags) {
    List<Tag> baseTags = new ArrayList<>();
    baseTags.add(Tag.of(TagKey.COMPONENT, component));
    baseTags.add(Tag.of(TagKey.OPERATION, operation));
    baseTags.addAll(copyTags(tags));
    TestObservationScope scope =
        new TestObservationScope(this, category, component, operation, List.copyOf(baseTags));
    scopes.computeIfAbsent(category.name(), key -> new ArrayList<>()).add(scope);
    return scope;
  }

  private static List<Tag> copyTags(Tag[] tags) {
    if (tags == null || tags.length == 0) {
      return List.of();
    }
    return List.copyOf(Arrays.asList(tags));
  }

  public double counterValue(MetricId metric) {
    return counters.getOrDefault(metric, 0d);
  }

  public List<Double> summaryValues(MetricId metric) {
    return Collections.unmodifiableList(summaries.getOrDefault(metric, Collections.emptyList()));
  }

  public List<Duration> timerValues(MetricId metric) {
    return Collections.unmodifiableList(timers.getOrDefault(metric, Collections.emptyList()));
  }

  public Supplier<? extends Number> gauge(MetricId metric) {
    return gauges.get(metric);
  }

  public List<Tag> gaugeTags(MetricId metric) {
    return gaugeTags.getOrDefault(metric, List.of());
  }

  public List<List<Tag>> counterTagHistory(MetricId metric) {
    return counterTags.containsKey(metric)
        ? Collections.unmodifiableList(counterTags.get(metric))
        : Collections.emptyList();
  }

  public List<List<Tag>> timerTagHistory(MetricId metric) {
    return timerTags.containsKey(metric)
        ? Collections.unmodifiableList(timerTags.get(metric))
        : Collections.emptyList();
  }

  public List<List<Tag>> summaryTagHistory(MetricId metric) {
    return summaryTags.containsKey(metric)
        ? Collections.unmodifiableList(summaryTags.get(metric))
        : Collections.emptyList();
  }

  public Map<String, List<TestObservationScope>> scopes() {
    Map<String, List<TestObservationScope>> copy = new LinkedHashMap<>();
    scopes.forEach((k, v) -> copy.put(k, List.copyOf(v)));
    return Collections.unmodifiableMap(copy);
  }

  public static final class TestObservationScope implements ObservationScope {
    private final TestObservability parent;
    private final Category category;
    private final String component;
    private final String operation;
    private final List<Tag> tags;
    private final long startNanos = System.nanoTime();
    private boolean closed;
    private boolean success;
    private Throwable error;
    private int retries;
    private String status;

    private TestObservationScope(
        TestObservability parent,
        Category category,
        String component,
        String operation,
        List<Tag> tags) {
      this.parent = Objects.requireNonNull(parent, "parent");
      this.category = category;
      this.component = component;
      this.operation = operation;
      this.tags = List.copyOf(tags);
    }

    @Override
    public void success() {
      if (error == null) {
        success = true;
      }
    }

    @Override
    public void error(Throwable throwable) {
      error = throwable;
      success = false;
    }

    @Override
    public void retry() {
      retries++;
    }

    @Override
    public void status(String status) {
      if (status != null && !status.isBlank()) {
        this.status = status;
      }
    }

    @Override
    public void close() {
      if (closed) {
        return;
      }
      closed = true;
      Duration elapsed = Duration.ofNanos(Math.max(0, System.nanoTime() - startNanos));
      parent.recordScope(this, elapsed);
    }

    public boolean isSuccess() {
      return success;
    }

    public Throwable error() {
      return error;
    }

    public int retries() {
      return retries;
    }

    public String component() {
      return component;
    }

    public String operation() {
      return operation;
    }

    public List<Tag> tags() {
      return tags;
    }

    public Observability.Category category() {
      return category;
    }

    private String status() {
      return status;
    }

    private boolean closed() {
      return closed;
    }
  }

  void recordScope(TestObservationScope scope, Duration elapsed) {
    ScopeMetrics metrics = metricsFor(scope.category());
    List<Tag> tags = new ArrayList<>(scope.tags());
    if (metrics == null) {
      return;
    }
    String grpcStatus = scope.status() == null ? "UNKNOWN" : scope.status();
    if (scope.category() == Category.RPC) {
      tags.add(Tag.of(TagKey.STATUS, grpcStatus));
    }
    String result;
    Throwable error = scope.error;
    if (!scope.success && error == null) {
      result = "unknown";
    } else {
      result = (error == null) ? "success" : "error";
    }
    tags.add(Tag.of(TagKey.RESULT, result));
    if (error != null) {
      tags.add(Tag.of(TagKey.EXCEPTION, error.getClass().getSimpleName()));
    }
    timer(metrics.latency(), elapsed, tags.toArray(Tag[]::new));
    if (error != null) {
      counter(metrics.errors(), 1, tags.toArray(Tag[]::new));
    }
    if (scope.retries() > 0) {
      MetricId retriesMetric = metrics.retries();
      if (retriesMetric != null) {
        counter(
            retriesMetric,
            scope.retries(),
            Tag.of(TagKey.COMPONENT, scope.component()),
            Tag.of(TagKey.OPERATION, scope.operation()));
      }
    }
  }

  private ScopeMetrics metricsFor(Category category) {
    return switch (category) {
      case RPC -> RPC_SCOPE_METRICS;
      case STORE -> STORE_SCOPE_METRICS;
      case CACHE -> CACHE_SCOPE_METRICS;
      case GC -> GC_SCOPE_METRICS;
      default -> null;
    };
  }

  private static final ScopeMetrics RPC_SCOPE_METRICS =
      new ScopeMetrics(
          Telemetry.Metrics.RPC_LATENCY,
          Telemetry.Metrics.RPC_ERRORS,
          Telemetry.Metrics.RPC_RETRIES);

  private static final ScopeMetrics STORE_SCOPE_METRICS =
      new ScopeMetrics(
          Telemetry.Metrics.STORE_LATENCY,
          Telemetry.Metrics.STORE_ERRORS,
          Telemetry.Metrics.STORE_RETRIES);

  private static final ScopeMetrics CACHE_SCOPE_METRICS =
      new ScopeMetrics(Telemetry.Metrics.CACHE_LATENCY, Telemetry.Metrics.CACHE_ERRORS, null);

  private static final ScopeMetrics GC_SCOPE_METRICS =
      new ScopeMetrics(
          Telemetry.Metrics.GC_PAUSE, Telemetry.Metrics.GC_ERRORS, Telemetry.Metrics.GC_RETRIES);

  private record ScopeMetrics(MetricId latency, MetricId errors, MetricId retries) {}
}
