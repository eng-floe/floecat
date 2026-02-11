package ai.floedb.floecat.telemetry;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Static facade for the shared telemetry catalog. */
public final class Telemetry {
  private Telemetry() {}

  public static TelemetryRegistry newRegistryWithCore() {
    TelemetryRegistry registry = new TelemetryRegistry();
    registry.register(new CoreTelemetryContributor());
    return registry;
  }

  public static void applyCore(TelemetryRegistry registry) {
    registry.register(new CoreTelemetryContributor());
  }

  public static Optional<MetricDef> metricDef(TelemetryRegistry registry, MetricId metric) {
    Objects.requireNonNull(registry, "registry");
    Objects.requireNonNull(metric, "metric");
    return Optional.ofNullable(registry.metric(metric.name()));
  }

  public static MetricDef requireMetricDef(TelemetryRegistry registry, MetricId metric) {
    return metricDef(registry, metric)
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    "Metric not registered: "
                        + metric.name()
                        + " (ensure the registry has core metrics)"));
  }

  public static Map<String, MetricDef> metricCatalog(TelemetryRegistry registry) {
    Objects.requireNonNull(registry, "registry");
    return registry.metrics();
  }

  /** Tag keys shared across multiple metrics. */
  public static final class TagKey {
    public static final String COMPONENT = "component";
    public static final String OPERATION = "operation";
    public static final String ACCOUNT = "account";
    public static final String STATUS = "status";
    public static final String RESULT = "result";
    public static final String EXCEPTION = "exception";
    public static final String CACHE_NAME = "cache";
    public static final String GC_NAME = "gc";

    private TagKey() {}
  }

  /** Built-in metrics that every module can rely on. */
  public static final class Metrics {
    public static final MetricId DROPPED_TAGS =
        new MetricId("observability.dropped.tags.total", MetricType.COUNTER, "count", "v1", "core");
    public static final MetricId RPC_REQUESTS =
        new MetricId("rpc.requests", MetricType.COUNTER, "count", "v1", "core");
    public static final MetricId RPC_ACTIVE =
        new MetricId("rpc.active", MetricType.GAUGE, "count", "v1", "core");
    public static final MetricId RPC_LATENCY =
        new MetricId("rpc.latency", MetricType.TIMER, "seconds", "v1", "core");
    public static final MetricId RPC_ERRORS =
        new MetricId("rpc.errors", MetricType.COUNTER, "count", "v1", "core");
    public static final MetricId RPC_RETRIES =
        new MetricId("rpc.retries", MetricType.COUNTER, "count", "v1", "core");
    public static final MetricId STORE_REQUESTS =
        new MetricId("store.requests", MetricType.COUNTER, "count", "v1", "core");
    public static final MetricId STORE_LATENCY =
        new MetricId("store.latency", MetricType.TIMER, "seconds", "v1", "core");
    public static final MetricId STORE_BYTES =
        new MetricId("store.bytes", MetricType.COUNTER, "bytes", "v1", "core");
    public static final MetricId CACHE_HITS =
        new MetricId("cache.hits", MetricType.COUNTER, "count", "v1", "core");
    public static final MetricId CACHE_MISSES =
        new MetricId("cache.misses", MetricType.COUNTER, "count", "v1", "core");
    public static final MetricId CACHE_SIZE =
        new MetricId("cache.size", MetricType.GAUGE, "count", "v1", "core");
    public static final MetricId GC_COLLECTIONS =
        new MetricId("gc.collections", MetricType.COUNTER, "count", "v1", "core");
    public static final MetricId GC_PAUSE =
        new MetricId("gc.pause", MetricType.TIMER, "seconds", "v1", "core");

    private static final Map<MetricId, MetricDef> DEFINITIONS = buildDefinitions();

    private Metrics() {}

    public static Map<MetricId, MetricDef> definitions() {
      return DEFINITIONS;
    }

    private static Map<MetricId, MetricDef> buildDefinitions() {
      Map<MetricId, MetricDef> definitions = new LinkedHashMap<>();
      add(definitions, DROPPED_TAGS, Set.of(), Set.of());

      Set<String> rpcReqTags =
          Set.of(TagKey.COMPONENT, TagKey.OPERATION, TagKey.ACCOUNT, TagKey.STATUS);
      Set<String> rpcScopeRequired = Set.of(TagKey.COMPONENT, TagKey.OPERATION, TagKey.RESULT);
      Set<String> rpcScopeAllowed = addTags(rpcScopeRequired, TagKey.EXCEPTION, TagKey.ACCOUNT);

      add(definitions, RPC_REQUESTS, rpcReqTags, rpcReqTags);
      add(
          definitions,
          RPC_ACTIVE,
          Set.of(TagKey.COMPONENT, TagKey.OPERATION),
          Set.of(TagKey.COMPONENT, TagKey.OPERATION));
      add(definitions, RPC_LATENCY, rpcScopeRequired, rpcScopeAllowed);
      add(definitions, RPC_ERRORS, rpcScopeRequired, rpcScopeAllowed);
      add(
          definitions,
          RPC_RETRIES,
          Set.of(TagKey.COMPONENT, TagKey.OPERATION),
          Set.of(TagKey.COMPONENT, TagKey.OPERATION));

      Set<String> storeRequired = Set.of(TagKey.COMPONENT, TagKey.OPERATION, TagKey.RESULT);
      Set<String> storeAllowed =
          addTags(storeRequired, TagKey.ACCOUNT, TagKey.STATUS, TagKey.EXCEPTION);
      add(definitions, STORE_REQUESTS, storeRequired, storeAllowed);
      add(definitions, STORE_LATENCY, storeRequired, storeAllowed);
      add(definitions, STORE_BYTES, storeRequired, storeAllowed);

      Set<String> cacheRequired = Set.of(TagKey.COMPONENT, TagKey.OPERATION, TagKey.CACHE_NAME);
      Set<String> cacheAllowed = addTags(cacheRequired, TagKey.RESULT, TagKey.ACCOUNT);
      add(definitions, CACHE_HITS, cacheRequired, cacheAllowed);
      add(definitions, CACHE_MISSES, cacheRequired, cacheAllowed);
      add(definitions, CACHE_SIZE, cacheRequired, cacheAllowed);

      Set<String> gcRequired = Set.of(TagKey.COMPONENT, TagKey.OPERATION, TagKey.GC_NAME);
      Set<String> gcAllowed = addTags(gcRequired, TagKey.RESULT, TagKey.EXCEPTION);
      add(definitions, GC_COLLECTIONS, gcRequired, gcAllowed);
      add(definitions, GC_PAUSE, gcRequired, gcAllowed);

      return Collections.unmodifiableMap(definitions);
    }

    private static void add(
        Map<MetricId, MetricDef> definitions,
        MetricId metric,
        Set<String> required,
        Set<String> allowed) {
      definitions.put(metric, new MetricDef(metric, required, allowed));
    }

    private static Set<String> addTags(Set<String> base, String... extras) {
      Set<String> copy = new LinkedHashSet<>(base);
      for (String extra : extras) {
        copy.add(extra);
      }
      return Collections.unmodifiableSet(copy);
    }
  }
}
