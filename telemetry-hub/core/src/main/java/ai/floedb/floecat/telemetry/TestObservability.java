package ai.floedb.floecat.telemetry;

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
    TestObservationScope scope =
        new TestObservationScope(category, component, operation, copyTags(tags));
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

  static final class TestObservationScope implements ObservationScope {
    private final Observability.Category category;
    private final String component;
    private final String operation;
    private final List<Tag> tags;
    private boolean success;
    private Throwable error;
    private int retries;

    private TestObservationScope(
        Observability.Category category, String component, String operation, List<Tag> tags) {
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
  }
}
