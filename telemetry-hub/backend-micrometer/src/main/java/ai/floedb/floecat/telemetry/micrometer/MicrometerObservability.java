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

import ai.floedb.floecat.telemetry.DropMetricReason;
import ai.floedb.floecat.telemetry.MetricDef;
import ai.floedb.floecat.telemetry.MetricId;
import ai.floedb.floecat.telemetry.MetricType;
import ai.floedb.floecat.telemetry.MetricValidator;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.ObservationScope;
import ai.floedb.floecat.telemetry.StoreTraceScope;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry;
import ai.floedb.floecat.telemetry.TelemetryPolicy;
import ai.floedb.floecat.telemetry.TelemetryRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
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
  private final Counter invalidMetricCounter;
  private final Counter duplicateGaugeCounter;
  private final ConcurrentMap<String, Counter> droppedMetricCounters = new ConcurrentHashMap<>();
  private final ConcurrentMap<MeterKey, Counter> counters = new ConcurrentHashMap<>();
  private final ConcurrentMap<MeterKey, Timer> timers = new ConcurrentHashMap<>();
  private final ConcurrentMap<MeterKey, DistributionSummary> summaries = new ConcurrentHashMap<>();
  private final ConcurrentMap<MeterKey, Gauge> gauges = new ConcurrentHashMap<>();

  private final TelemetryPolicy policy;
  private final Tracer tracer;

  private static final Set<String> HISTOGRAM_TIMERS =
      Set.of(
          Telemetry.Metrics.RPC_LATENCY.name(),
          Telemetry.Metrics.STORE_LATENCY.name(),
          Telemetry.Metrics.CACHE_LATENCY.name(),
          Telemetry.Metrics.EXEC_TASK_WAIT.name(),
          Telemetry.Metrics.EXEC_TASK_RUN.name());
  private static final Duration HISTOGRAM_EXPIRY = Duration.ofMinutes(5);
  private static final Duration HISTOGRAM_MIN = Duration.ofNanos(10_000);
  private static final Duration[] RPC_SLOS =
      new Duration[] {
        Duration.ofMillis(5),
        Duration.ofMillis(10),
        Duration.ofMillis(25),
        Duration.ofMillis(50),
        Duration.ofMillis(100),
        Duration.ofMillis(250),
        Duration.ofMillis(500),
        Duration.ofMillis(1000)
      };
  private static final Duration[] STORE_SLOS =
      new Duration[] {
        Duration.ofMillis(10),
        Duration.ofMillis(50),
        Duration.ofMillis(100),
        Duration.ofMillis(200),
        Duration.ofMillis(500),
        Duration.ofMillis(1000),
        Duration.ofMillis(2000)
      };
  private static final Duration[] CACHE_SLOS =
      new Duration[] {
        Duration.ofNanos(500_000),
        Duration.ofMillis(1),
        Duration.ofMillis(2),
        Duration.ofMillis(5),
        Duration.ofMillis(10),
        Duration.ofMillis(25),
        Duration.ofMillis(50)
      };
  private static final Duration[] EXEC_SLOS =
      new Duration[] {
        Duration.ofMillis(1),
        Duration.ofMillis(2),
        Duration.ofMillis(5),
        Duration.ofMillis(10),
        Duration.ofMillis(25),
        Duration.ofMillis(50),
        Duration.ofMillis(100),
        Duration.ofMillis(200)
      };
  private static final boolean PUBLISH_PERCENTILES =
      Boolean.getBoolean("floecat.telemetry.publish-percentiles");

  private static final Logger LOG = LoggerFactory.getLogger(MicrometerObservability.class);

  public MicrometerObservability(
      MeterRegistry registry, TelemetryRegistry telemetryRegistry, TelemetryPolicy policy) {
    this.registry = Objects.requireNonNull(registry, "registry");
    this.telemetryRegistry = Objects.requireNonNull(telemetryRegistry, "telemetryRegistry");
    this.validator =
        new MetricValidator(telemetryRegistry, Objects.requireNonNull(policy, "policy"));
    this.droppedTagsCounter = registry.counter(Telemetry.Metrics.DROPPED_TAGS.name());
    this.invalidMetricCounter =
        Counter.builder(Telemetry.Metrics.OBSERVABILITY_INVALID_METRIC.name())
            .description(descriptionFor(Telemetry.Metrics.OBSERVABILITY_INVALID_METRIC))
            .tags(Telemetry.TagKey.REASON, "unknown_metric")
            .register(registry);
    this.duplicateGaugeCounter =
        Counter.builder(Telemetry.Metrics.OBSERVABILITY_DUPLICATE_GAUGE.name())
            .description(descriptionFor(Telemetry.Metrics.OBSERVABILITY_DUPLICATE_GAUGE))
            .tags(Telemetry.TagKey.REASON, "duplicate_gauge")
            .register(registry);
    gauge(
        Telemetry.Metrics.OBSERVABILITY_REGISTRY_SIZE,
        () -> (double) telemetryRegistry.metrics().size(),
        descriptionFor(Telemetry.Metrics.OBSERVABILITY_REGISTRY_SIZE));
    this.policy = policy;
    this.tracer = GlobalOpenTelemetry.getTracer("ai.floedb.floecat.telemetry");
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
    Iterable<io.micrometer.core.instrument.Tag> meterTags = micrometerTags(key.tags());
    gauges.compute(
        key,
        (ignored, existing) -> {
          if (existing != null) {
            duplicateGaugeCounter.increment();
            if (policy.isStrict()) {
              throw new IllegalArgumentException(
                  "Gauge already registered for metric "
                      + metric.name()
                      + " with tags "
                      + key.tags());
            }
            registry.remove(existing);
          } else {
            Gauge registryGauge = registry.find(metric.name()).tags(meterTags).gauge();
            if (registryGauge != null) {
              duplicateGaugeCounter.increment();
              if (policy.isStrict()) {
                throw new IllegalArgumentException(
                    "Gauge already registered for metric " + metric.name());
              }
              return registryGauge;
            }
          }
          Supplier<Number> safeSupplier =
              () -> {
                T value = supplier.get();
                return value == null ? Double.NaN : value.doubleValue();
              };
          return Gauge.builder(metric.name(), safeSupplier)
              .description(description)
              .tags(meterTags)
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
      DropMetricReason reason = result.reason();
      if (reason == DropMetricReason.UNKNOWN_METRIC) {
        invalidMetricCounter.increment();
      } else {
        droppedMetricCounter(reason);
      }
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
    Timer.Builder builder =
        Timer.builder(key.metric().name())
            .description(descriptionFor(key.metric()))
            .tags(micrometerTags(key.tags()));
    return applyHistogramConfig(builder, key.metric().name()).register(registry);
  }

  private String descriptionFor(MetricId metric) {
    MetricDef def = telemetryRegistry.metric(metric.name());
    return def != null ? def.description() : "";
  }

  private Timer.Builder applyHistogramConfig(Timer.Builder builder, String metricName) {
    if (HISTOGRAM_TIMERS.contains(metricName)) {
      builder.publishPercentileHistogram(true);
      if (PUBLISH_PERCENTILES) {
        builder.publishPercentiles(0.5, 0.95, 0.99);
      }
      Duration[] slos = sloFor(metricName);
      if (slos != null) {
        builder.serviceLevelObjectives(slos);
      }
      builder.distributionStatisticExpiry(HISTOGRAM_EXPIRY).minimumExpectedValue(HISTOGRAM_MIN);
    }
    return builder;
  }

  private Duration[] sloFor(String metricName) {
    if (Telemetry.Metrics.RPC_LATENCY.name().equals(metricName)) {
      return RPC_SLOS;
    }
    if (Telemetry.Metrics.STORE_LATENCY.name().equals(metricName)) {
      return STORE_SLOS;
    }
    if (Telemetry.Metrics.CACHE_LATENCY.name().equals(metricName)) {
      return CACHE_SLOS;
    }
    if (Telemetry.Metrics.EXEC_TASK_WAIT.name().equals(metricName)
        || Telemetry.Metrics.EXEC_TASK_RUN.name().equals(metricName)) {
      return EXEC_SLOS;
    }
    return null;
  }

  private void logTraceCorrelation(
      String component,
      String operation,
      String status,
      String result,
      Duration elapsed,
      Throwable error) {
    if (!LOG.isTraceEnabled()) {
      return;
    }
    Span span = Span.current();
    SpanContext context = span.getSpanContext();
    if (!context.isValid()) {
      return;
    }
    LOG.trace(
        "Telemetry observation {}.{} result={} status={} duration={}ms traceId={} spanId={} error={}",
        component,
        operation,
        result,
        status == null ? "UNKNOWN" : status,
        elapsed.toMillis(),
        context.getTraceId(),
        context.getSpanId(),
        error == null ? "none" : error.getClass().getSimpleName());
  }

  private void droppedMetricCounter(DropMetricReason reason) {
    String normalized = normalizeReason(reason);
    droppedMetricCounters
        .computeIfAbsent(
            normalized,
            key ->
                Counter.builder(Telemetry.Metrics.OBSERVABILITY_DROPPED_METRIC.name())
                    .description(descriptionFor(Telemetry.Metrics.OBSERVABILITY_DROPPED_METRIC))
                    .tags(Telemetry.TagKey.REASON, key)
                    .register(registry))
        .increment();
  }

  private static String normalizeReason(DropMetricReason reason) {
    if (reason == null) {
      return "unknown_reason";
    }
    return switch (reason) {
      case UNKNOWN_METRIC -> "unknown_metric";
      case MISSING_REQUIRED_TAG -> "required_missing";
      case UNKNOWN_TAG -> "tag_not_allowed";
      case INVALID_VALUE -> "tag_value_invalid";
      case TYPE_MISMATCH -> "type_mismatch";
      default -> "unknown_reason";
    };
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
      logTraceCorrelation(component, operation, grpcStatus, resultTag, elapsed, error);
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

  @Override
  public StoreTraceScope storeTraceScope(String component, String operation, Tag... tags) {
    Span parent = Span.current();
    if (!parent.getSpanContext().isValid() || !parent.isRecording()) {
      return StoreTraceScope.NOOP;
    }
    String safeOperation = sanitizeOperation(operation);
    Context parentContext = Context.current().with(parent);
    SpanBuilder builder =
        tracer
            .spanBuilder("store." + safeOperation)
            .setParent(parentContext)
            .setSpanKind(SpanKind.INTERNAL);
    Span span = builder.startSpan();
    Scope scope = span.makeCurrent();
    span.setAttribute("floecat.store.operation", operation);
    span.setAttribute("floecat.component", component);
    for (Tag tag : buildScopeTags(component, operation, tags)) {
      if ("telemetry.contract.version".equals(tag.key())) {
        span.setAttribute(tag.key(), tag.value());
      }
    }
    return new OtelStoreTraceScope(span, scope);
  }

  private static String sanitizeOperation(String operation) {
    if (operation == null || operation.isBlank()) {
      return "unknown";
    }
    return operation.replaceAll("[^A-Za-z0-9_.-]", "_");
  }

  private static final class OtelStoreTraceScope implements StoreTraceScope {
    private final Span span;
    private final Scope scope;

    private OtelStoreTraceScope(Span span, Scope scope) {
      this.span = span;
      this.scope = scope;
    }

    @Override
    public void success() {
      if (span.isRecording()) {
        span.setStatus(StatusCode.OK);
      }
    }

    @Override
    public void error(Throwable throwable) {
      if (span.isRecording()) {
        span.recordException(throwable);
        span.setStatus(StatusCode.ERROR);
      }
    }

    @Override
    public void close() {
      try {
        if (scope != null) {
          scope.close();
        }
      } finally {
        span.end();
      }
    }
  }
}
