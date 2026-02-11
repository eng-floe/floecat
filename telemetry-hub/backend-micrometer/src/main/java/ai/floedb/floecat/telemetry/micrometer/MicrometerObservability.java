package ai.floedb.floecat.telemetry.micrometer;

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
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** Micrometer-based {@link Observability} implementation. */
public final class MicrometerObservability implements Observability {
  private final MeterRegistry registry;
  private final MetricValidator validator;
  private final Counter droppedTagsCounter;
  private final ConcurrentMap<MeterKey, Counter> counters = new ConcurrentHashMap<>();
  private final ConcurrentMap<MeterKey, Timer> timers = new ConcurrentHashMap<>();
  private final ConcurrentMap<MeterKey, DistributionSummary> summaries = new ConcurrentHashMap<>();
  private final ConcurrentMap<MeterKey, Gauge> gauges = new ConcurrentHashMap<>();

  private final TelemetryPolicy policy;

  public MicrometerObservability(
      MeterRegistry registry, TelemetryRegistry telemetryRegistry, TelemetryPolicy policy) {
    this.registry = Objects.requireNonNull(registry, "registry");
    Objects.requireNonNull(telemetryRegistry, "telemetryRegistry");
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
    if (category != Category.RPC) {
      return NOOP_SCOPE;
    }
    List<Tag> baseTags = buildScopeTags(component, operation, tags);
    return new MicrometerObservationScope(component, operation, baseTags);
  }

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
    return Counter.builder(key.metric().name()).tags(micrometerTags(key.tags())).register(registry);
  }

  private DistributionSummary registerSummary(MeterKey key) {
    return DistributionSummary.builder(key.metric().name())
        .tags(micrometerTags(key.tags()))
        .register(registry);
  }

  private Timer registerTimer(MeterKey key) {
    return Timer.builder(key.metric().name()).tags(micrometerTags(key.tags())).register(registry);
  }

  private static List<Tag> sort(List<Tag> tags) {
    if (tags.isEmpty()) {
      return List.of();
    }
    List<Tag> sorted = new ArrayList<>(tags);
    sorted.sort(Comparator.comparing(Tag::key));
    return Collections.unmodifiableList(sorted);
  }

  private static List<Tag> buildScopeTags(String component, String operation, Tag... tags) {
    List<Tag> base = new ArrayList<>();
    base.add(Tag.of(Telemetry.TagKey.COMPONENT, component));
    base.add(Tag.of(Telemetry.TagKey.OPERATION, operation));
    if (tags != null) {
      for (Tag tag : tags) {
        if (tag != null) {
          base.add(tag);
        }
      }
    }
    return Collections.unmodifiableList(base);
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
    private final String component;
    private final String operation;
    private final List<Tag> baseTags;
    private final long startNanos = System.nanoTime();
    private boolean closed;
    private Throwable error;
    private int retries;
    private boolean successCalled;

    private MicrometerObservationScope(String component, String operation, List<Tag> baseTags) {
      this.component = component;
      this.operation = operation;
      this.baseTags = baseTags;
    }

    @Override
    public void success() {
      if (closed) {
        return;
      }
      successCalled = true;
      error = null;
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
      if (!successCalled && error == null) {
        if (MicrometerObservability.this.policy.isStrict()) {
          throw new IllegalStateException(
              "Observation scope closed without calling success() or error()");
        }
        return;
      }
      Duration elapsed = Duration.ofNanos(Math.max(0, System.nanoTime() - startNanos));
      List<Tag> latencyTags = new ArrayList<>(baseTags.size() + 2);
      latencyTags.addAll(baseTags);
      latencyTags.add(Tag.of(Telemetry.TagKey.RESULT, error != null ? "error" : "success"));
      if (error != null) {
        latencyTags.add(Tag.of(Telemetry.TagKey.EXCEPTION, error.getClass().getSimpleName()));
      }
      Tag[] latencyArray = latencyTags.toArray(Tag[]::new);
      timer(Telemetry.Metrics.RPC_LATENCY, elapsed, latencyArray);
      if (error != null) {
        counter(Telemetry.Metrics.RPC_ERRORS, 1, latencyArray);
      }
      if (retries > 0) {
        counter(
            Telemetry.Metrics.RPC_RETRIES,
            retries,
            Tag.of(Telemetry.TagKey.COMPONENT, component),
            Tag.of(Telemetry.TagKey.OPERATION, operation));
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
