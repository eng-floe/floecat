package ai.floedb.floecat.telemetry;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;

/** Static facade for the shared telemetry catalog. */
public final class Telemetry {
  private Telemetry() {}

  public static TelemetryRegistry newRegistryWithCore() {
    TelemetryRegistry registry = new TelemetryRegistry();
    registry.register(new CoreTelemetryContributor());
    registerServiceContributors(registry);
    return registry;
  }

  public static void applyCore(TelemetryRegistry registry) {
    registry.register(new CoreTelemetryContributor());
    registerServiceContributors(registry);
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

  private static void registerServiceContributors(TelemetryRegistry registry) {
    ServiceLoader<TelemetryContributor> loader = ServiceLoader.load(TelemetryContributor.class);
    loader.stream()
        .map(ServiceLoader.Provider::get)
        .sorted(Comparator.comparing(c -> c.getClass().getName()))
        .forEach(contributor -> contributor.contribute(registry));
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
        new MetricId(
            "floecat.core.observability.dropped.tags.total", MetricType.COUNTER, "", "v1", "core");
    public static final MetricId RPC_REQUESTS =
        new MetricId("floecat.core.rpc.requests", MetricType.COUNTER, "", "v1", "core");
    public static final MetricId RPC_ACTIVE =
        new MetricId("floecat.core.rpc.active", MetricType.GAUGE, "", "v1", "core");
    public static final MetricId RPC_LATENCY =
        new MetricId("floecat.core.rpc.latency", MetricType.TIMER, "seconds", "v1", "core");
    public static final MetricId RPC_ERRORS =
        new MetricId("floecat.core.rpc.errors", MetricType.COUNTER, "", "v1", "core");
    public static final MetricId RPC_RETRIES =
        new MetricId("floecat.core.rpc.retries", MetricType.COUNTER, "", "v1", "core");
    public static final MetricId STORE_REQUESTS =
        new MetricId("floecat.core.store.requests", MetricType.COUNTER, "", "v1", "core");
    public static final MetricId STORE_LATENCY =
        new MetricId("floecat.core.store.latency", MetricType.TIMER, "seconds", "v1", "core");
    public static final MetricId STORE_BYTES =
        new MetricId("floecat.core.store.bytes", MetricType.COUNTER, "bytes", "v1", "core");
    public static final MetricId CACHE_HITS =
        new MetricId("floecat.core.cache.hits", MetricType.COUNTER, "", "v1", "core");
    public static final MetricId CACHE_MISSES =
        new MetricId("floecat.core.cache.misses", MetricType.COUNTER, "", "v1", "core");
    public static final MetricId CACHE_SIZE =
        new MetricId("floecat.core.cache.size", MetricType.GAUGE, "", "v1", "core");
    public static final MetricId GC_COLLECTIONS =
        new MetricId("floecat.core.gc.collections", MetricType.COUNTER, "", "v1", "core");
    public static final MetricId GC_PAUSE =
        new MetricId("floecat.core.gc.pause", MetricType.TIMER, "seconds", "v1", "core");

    private static final Map<MetricId, MetricDef> DEFINITIONS = buildDefinitions();

    private Metrics() {}

    public static Map<MetricId, MetricDef> definitions() {
      return DEFINITIONS;
    }

    private static Map<MetricId, MetricDef> buildDefinitions() {
      Map<MetricId, MetricDef> definitions = new LinkedHashMap<>();
      add(
          definitions,
          DROPPED_TAGS,
          Set.of(),
          Set.of(),
          "Total number of tags dropped because they violated telemetry contracts.");

      Set<String> rpcReqTags =
          Set.of(TagKey.COMPONENT, TagKey.OPERATION, TagKey.ACCOUNT, TagKey.STATUS);
      Set<String> rpcScopeRequired = Set.of(TagKey.COMPONENT, TagKey.OPERATION, TagKey.RESULT);
      Set<String> rpcScopeAllowed =
          addTags(rpcScopeRequired, TagKey.EXCEPTION, TagKey.ACCOUNT, TagKey.STATUS);

      add(
          definitions,
          RPC_REQUESTS,
          rpcReqTags,
          rpcReqTags,
          "Total RPC requests processed, tagged by account and status.");
      add(
          definitions,
          RPC_ACTIVE,
          Set.of(TagKey.COMPONENT, TagKey.OPERATION),
          Set.of(TagKey.COMPONENT, TagKey.OPERATION),
          "Number of in-flight RPCs per component/operation.");
      add(
          definitions,
          RPC_LATENCY,
          rpcScopeRequired,
          rpcScopeAllowed,
          "Latency distribution for RPC operations.");
      add(
          definitions,
          RPC_ERRORS,
          rpcScopeRequired,
          rpcScopeAllowed,
          "Count of RPC failures per component/operation.");
      add(
          definitions,
          RPC_RETRIES,
          Set.of(TagKey.COMPONENT, TagKey.OPERATION),
          Set.of(TagKey.COMPONENT, TagKey.OPERATION),
          "Number of RPC retries invoked.");

      Set<String> storeRequired = Set.of(TagKey.COMPONENT, TagKey.OPERATION, TagKey.RESULT);
      Set<String> storeAllowed =
          addTags(storeRequired, TagKey.ACCOUNT, TagKey.STATUS, TagKey.EXCEPTION);
      add(
          definitions,
          STORE_REQUESTS,
          storeRequired,
          storeAllowed,
          "Number of store requests emitted per component/operation.");
      add(
          definitions,
          STORE_LATENCY,
          storeRequired,
          storeAllowed,
          "Store operation latency distribution.");
      add(
          definitions,
          STORE_BYTES,
          storeRequired,
          storeAllowed,
          "Count of bytes processed by store operations.");

      Set<String> cacheRequired = Set.of(TagKey.COMPONENT, TagKey.OPERATION, TagKey.CACHE_NAME);
      Set<String> cacheAllowed = addTags(cacheRequired, TagKey.RESULT, TagKey.ACCOUNT);
      add(
          definitions,
          CACHE_HITS,
          cacheRequired,
          cacheAllowed,
          "Cache hits count for the graph cache.");
      add(
          definitions,
          CACHE_MISSES,
          cacheRequired,
          cacheAllowed,
          "Cache misses count for the graph cache.");
      add(
          definitions,
          CACHE_SIZE,
          cacheRequired,
          cacheAllowed,
          "Approximate graph cache size by cache name.");

      Set<String> gcRequired = Set.of(TagKey.COMPONENT, TagKey.OPERATION, TagKey.GC_NAME);
      Set<String> gcAllowed = addTags(gcRequired, TagKey.RESULT, TagKey.EXCEPTION);
      add(
          definitions,
          GC_COLLECTIONS,
          gcRequired,
          gcAllowed,
          "Number of GC collections per GC type.");
      add(definitions, GC_PAUSE, gcRequired, gcAllowed, "GC pause time per GC type.");

      return Collections.unmodifiableMap(definitions);
    }

    private static void add(
        Map<MetricId, MetricDef> definitions,
        MetricId metric,
        Set<String> required,
        Set<String> allowed,
        String description) {
      definitions.put(metric, new MetricDef(metric, required, allowed, description));
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
