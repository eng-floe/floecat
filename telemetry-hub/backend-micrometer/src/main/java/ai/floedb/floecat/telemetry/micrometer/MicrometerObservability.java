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
package ai.floedb.floecat.telemetry.micrometer;

import ai.floedb.floecat.telemetry.MetricDef;
import ai.floedb.floecat.telemetry.MetricId;
import ai.floedb.floecat.telemetry.MetricType;
import ai.floedb.floecat.telemetry.MetricValidator;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.ObservationScope;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry;
import ai.floedb.floecat.telemetry.TelemetryPolicy;
import ai.floedb.floecat.telemetry.TelemetryRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Micrometer-based {@link Observability} implementation. */
public final class MicrometerObservability implements Observability {
  private final MeterRegistry registry;
  private final TelemetryRegistry telemetryRegistry;
  private final MetricValidator validator;
  private final Counter droppedTagsCounter;
  private final ConcurrentMap<MeterKey, Counter> counters = new ConcurrentHashMap<>();
  private final ConcurrentMap<MeterKey, Timer> timers = new ConcurrentHashMap<>();
  private final ConcurrentMap<MeterKey, DistributionSummary> summaries = new ConcurrentHashMap<>();
  private final ConcurrentMap<MeterKey, Gauge> gauges = new ConcurrentHashMap<>();

  private final TelemetryPolicy policy;

  private static final Logger LOG = LoggerFactory.getLogger(MicrometerObservability.class);

  public MicrometerObservability(
      MeterRegistry registry, TelemetryRegistry telemetryRegistry, TelemetryPolicy policy) {
    this.registry = Objects.requireNonNull(registry, "registry");
    this.telemetryRegistry = Objects.requireNonNull(telemetryRegistry, "telemetryRegistry");
    this.validator =
        new MetricValidator(telemetryRegistry, Objects.requireNonNull(policy, "policy"));
    this.droppedTagsCounter = registry.counter(Telemetry.Metrics.DROPPED_TAGS.name());
    this.policy = policy;
  }

  @Override
  public void counter(MetricId metric, double amount, Tag... tags) {
    MeterKey key = validate(MetricType.COUNTER, metric, tags);
    if (key == null) {
      return;
    }
    counters.computeIfAbsent(key, this::registerCounter).increment(amount);
  }

  @Override
  public void summary(MetricId metric, double value, Tag... tags) {
    MeterKey key = validate(MetricType.SUMMARY, metric, tags);
    if (key == null) {
      return;
    }
    summaries.computeIfAbsent(key, this::registerSummary).record(value);
  }

  @Override
  public void timer(MetricId metric, Duration duration, Tag... tags) {
    MeterKey key = validate(MetricType.TIMER, metric, tags);
    if (key == null) {
      return;
    }
    timers.computeIfAbsent(key, this::registerTimer).record(duration);
  }

  @Override
  public <T extends Number> void gauge(
      MetricId metric, Supplier<T> supplier, String description, Tag... tags) {
    Objects.requireNonNull(supplier, "supplier");
    MeterKey key = validate(MetricType.GAUGE, metric, tags);
    if (key == null) {
      return;
    }
    gauges.compute(
        key,
        (ignored, existing) -> {
          if (existing != null) {
            if (policy.isStrict()) {
              throw new IllegalArgumentException(
                  "Gauge already registered for metric "
                      + metric.name()
                      + " with tags "
                      + key.tags());
            }
            registry.remove(existing);
          }
          Supplier<Number> safeSupplier =
              () -> {
                T value = supplier.get();
                return value == null ? Double.NaN : value.doubleValue();
              };
          return Gauge.builder(metric.name(), safeSupplier)
              .description(description)
              .tags(micrometerTags(key.tags()))
              .register(registry);
        });
  }

  @Override
  public ObservationScope observe(
      Category category, String component, String operation, Tag... tags) {
    ScopeMetrics metrics = metricsFor(category);
    if (metrics == null) {
      return NOOP_SCOPE;
    }
    List<Tag> baseTags = buildScopeTags(component, operation, tags);
    return new MicrometerObservationScope(category, component, operation, baseTags, metrics);
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

  private MeterKey validate(MetricType expected, MetricId id, Tag... tags) {
    MetricValidator.ValidationResult result = validator.validate(id, expected, tags);
    if (result.droppedTags() > 0) {
      droppedTagsCounter.increment(result.droppedTags());
    }
    if (!result.emit()) {
      return null;
    }
    return new MeterKey(id, sort(result.tags()));
  }

  private Counter registerCounter(MeterKey key) {
    return Counter.builder(key.metric().name())
        .description(descriptionFor(key.metric()))
        .tags(micrometerTags(key.tags()))
        .register(registry);
  }

  private DistributionSummary registerSummary(MeterKey key) {
    return DistributionSummary.builder(key.metric().name())
        .description(descriptionFor(key.metric()))
        .tags(micrometerTags(key.tags()))
        .register(registry);
  }

  private Timer registerTimer(MeterKey key) {
    return Timer.builder(key.metric().name())
        .description(descriptionFor(key.metric()))
        .tags(micrometerTags(key.tags()))
        .register(registry);
  }

  private String descriptionFor(MetricId metric) {
    MetricDef def = telemetryRegistry.metric(metric.name());
    return def != null ? def.description() : "";
  }

  private static List<Tag> sort(List<Tag> tags) {
    if (tags.isEmpty()) {
      return List.of();
    }
    List<Tag> sorted = new ArrayList<>(tags);
    sorted.sort(Comparator.comparing(Tag::key));
    return Collections.unmodifiableList(sorted);
  }

  private List<Tag> buildScopeTags(String component, String operation, Tag... tags) {
    LinkedHashMap<String, Tag> canon = new LinkedHashMap<>();
    Tag componentTag = Tag.of(Telemetry.TagKey.COMPONENT, component);
    Tag operationTag = Tag.of(Telemetry.TagKey.OPERATION, operation);
    canon.put(Telemetry.TagKey.COMPONENT, componentTag);
    canon.put(Telemetry.TagKey.OPERATION, operationTag);
    if (tags != null) {
      for (Tag tag : tags) {
        if (tag == null) {
          continue;
        }
        if (canon.containsKey(tag.key())) {
          if (policy.isStrict() && !isCanonicalTag(tag.key())) {
            throw new IllegalArgumentException("Duplicate tag key: " + tag.key());
          }
          continue;
        }
        canon.put(tag.key(), tag);
      }
    }
    return List.copyOf(canon.values());
  }

  private static boolean isCanonicalTag(String key) {
    return Telemetry.TagKey.COMPONENT.equals(key) || Telemetry.TagKey.OPERATION.equals(key);
  }

  private static Iterable<io.micrometer.core.instrument.Tag> micrometerTags(List<Tag> tags) {
    if (tags.isEmpty()) {
      return Collections.emptyList();
    }
    return tags.stream()
        .map(tag -> io.micrometer.core.instrument.Tag.of(tag.key(), tag.value()))
        .collect(Collectors.toList());
  }

  private record MeterKey(MetricId metric, List<Tag> tags) {
    MeterKey {
      Objects.requireNonNull(metric, "metric");
      Objects.requireNonNull(tags, "tags");
    }
  }

  private final class MicrometerObservationScope implements ObservationScope {
    private final Category category;
    private final String component;
    private final String operation;
    private final List<Tag> baseTags;
    private final long startNanos = System.nanoTime();
    private boolean closed;
    private Throwable error;
    private int retries;
    private boolean successCalled;
    private String grpcStatus;
    private final ScopeMetrics metrics;

    private MicrometerObservationScope(
        Category category,
        String component,
        String operation,
        List<Tag> baseTags,
        ScopeMetrics metrics) {
      this.category = category;
      this.component = component;
      this.operation = operation;
      this.baseTags = baseTags;
      this.metrics = metrics;
    }

    @Override
    public void success() {
      if (closed) {
        return;
      }
      successCalled = true;
      error = null;
      grpcStatus = "OK";
    }

    @Override
    public void error(Throwable throwable) {
      if (closed) {
        return;
      }
      this.error = throwable;
      this.successCalled = false;
    }

    @Override
    public void status(String status) {
      if (status != null && !status.isBlank()) {
        grpcStatus = status;
      }
    }

    @Override
    public void retry() {
      if (closed) {
        return;
      }
      retries++;
    }

    @Override
    public void close() {
      if (closed) {
        return;
      }
      closed = true;
      Duration elapsed = Duration.ofNanos(Math.max(0, System.nanoTime() - startNanos));
      List<Tag> latencyTags = new ArrayList<>(baseTags.size() + 3);
      latencyTags.addAll(baseTags);
      String resultTag;
      boolean outcomeMissing = !successCalled && error == null;
      if (outcomeMissing && MicrometerObservability.this.policy.isStrict()) {
        throw new IllegalStateException(
            "Observation closed without success() or error() in strict mode");
      }
      if (outcomeMissing) {
        LOG.warn(
            "Observation closed without explicit outcome for {}.{}; emitting result=unknown",
            component,
            operation);
        resultTag = "unknown";
      } else {
        resultTag = (error != null) ? "error" : "success";
      }
      if (category == Category.RPC) {
        latencyTags.add(
            Tag.of(Telemetry.TagKey.STATUS, grpcStatus == null ? "UNKNOWN" : grpcStatus));
      }
      latencyTags.add(Tag.of(Telemetry.TagKey.RESULT, resultTag));
      if (error != null) {
        latencyTags.add(Tag.of(Telemetry.TagKey.EXCEPTION, error.getClass().getSimpleName()));
      }
      Tag[] latencyArray = latencyTags.toArray(Tag[]::new);
      timer(metrics.latency(), elapsed, latencyArray);
      if (error != null) {
        counter(metrics.errors(), 1, latencyArray);
      }
      if (retries > 0) {
        MetricId retriesMetric = metrics.retries();
        if (retriesMetric != null) {
          counter(
              retriesMetric,
              retries,
              Tag.of(Telemetry.TagKey.COMPONENT, component),
              Tag.of(Telemetry.TagKey.OPERATION, operation));
        }
      }
    }
  }

  private static final ObservationScope NOOP_SCOPE =
      new ObservationScope() {
        @Override
        public void success() {}

        @Override
        public void error(Throwable throwable) {}

        @Override
        public void retry() {}
      };
}
