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
    public static final class Observability {
      public static final MetricId DROPPED_TAGS =
          new MetricId(
              "floecat.core.observability.dropped.tags.total",
              MetricType.COUNTER,
              "",
              "v1",
              "core");
    }

    public static final class Rpc {
      public static final MetricId REQUESTS =
          new MetricId("floecat.core.rpc.requests", MetricType.COUNTER, "", "v1", "core");
      public static final MetricId ACTIVE =
          new MetricId("floecat.core.rpc.active", MetricType.GAUGE, "", "v1", "core");
      public static final MetricId LATENCY =
          new MetricId("floecat.core.rpc.latency", MetricType.TIMER, "seconds", "v1", "core");
      public static final MetricId ERRORS =
          new MetricId("floecat.core.rpc.errors", MetricType.COUNTER, "", "v1", "core");
      public static final MetricId RETRIES =
          new MetricId("floecat.core.rpc.retries", MetricType.COUNTER, "", "v1", "core");
    }

    public static final class Store {
      public static final MetricId REQUESTS =
          new MetricId("floecat.core.store.requests", MetricType.COUNTER, "", "v1", "core");
      public static final MetricId LATENCY =
          new MetricId("floecat.core.store.latency", MetricType.TIMER, "seconds", "v1", "core");
      public static final MetricId BYTES =
          new MetricId("floecat.core.store.bytes", MetricType.COUNTER, "bytes", "v1", "core");
      public static final MetricId ERRORS =
          new MetricId("floecat.core.store.errors", MetricType.COUNTER, "", "v1", "core");
      public static final MetricId RETRIES =
          new MetricId("floecat.core.store.retries", MetricType.COUNTER, "", "v1", "core");
    }

    public static final class Cache {
      public static final MetricId HITS =
          new MetricId("floecat.core.cache.hits", MetricType.COUNTER, "", "v1", "core");
      public static final MetricId MISSES =
          new MetricId("floecat.core.cache.misses", MetricType.COUNTER, "", "v1", "core");
      public static final MetricId SIZE =
          new MetricId("floecat.core.cache.size", MetricType.GAUGE, "", "v1", "core");
      public static final MetricId LATENCY =
          new MetricId("floecat.core.cache.latency", MetricType.TIMER, "seconds", "v1", "core");
      public static final MetricId ERRORS =
          new MetricId("floecat.core.cache.errors", MetricType.COUNTER, "", "v1", "core");
    }

    public static final class Gc {
      public static final MetricId COLLECTIONS =
          new MetricId("floecat.core.gc.collections", MetricType.COUNTER, "", "v1", "core");
      public static final MetricId PAUSE =
          new MetricId("floecat.core.gc.pause", MetricType.TIMER, "seconds", "v1", "core");
      public static final MetricId ERRORS =
          new MetricId("floecat.core.gc.errors", MetricType.COUNTER, "", "v1", "core");
      public static final MetricId RETRIES =
          new MetricId("floecat.core.gc.retries", MetricType.COUNTER, "", "v1", "core");
    }

    public static final MetricId DROPPED_TAGS = Observability.DROPPED_TAGS;
    public static final MetricId RPC_REQUESTS = Rpc.REQUESTS;
    public static final MetricId RPC_ACTIVE = Rpc.ACTIVE;
    public static final MetricId RPC_LATENCY = Rpc.LATENCY;
    public static final MetricId RPC_ERRORS = Rpc.ERRORS;
    public static final MetricId RPC_RETRIES = Rpc.RETRIES;
    public static final MetricId STORE_REQUESTS = Store.REQUESTS;
    public static final MetricId STORE_LATENCY = Store.LATENCY;
    public static final MetricId STORE_BYTES = Store.BYTES;
    public static final MetricId STORE_ERRORS = Store.ERRORS;
    public static final MetricId STORE_RETRIES = Store.RETRIES;
    public static final MetricId CACHE_HITS = Cache.HITS;
    public static final MetricId CACHE_MISSES = Cache.MISSES;
    public static final MetricId CACHE_SIZE = Cache.SIZE;
    public static final MetricId CACHE_LATENCY = Cache.LATENCY;
    public static final MetricId CACHE_ERRORS = Cache.ERRORS;
    public static final MetricId GC_COLLECTIONS = Gc.COLLECTIONS;
    public static final MetricId GC_PAUSE = Gc.PAUSE;
    public static final MetricId GC_ERRORS = Gc.ERRORS;
    public static final MetricId GC_RETRIES = Gc.RETRIES;

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
      Set<String> storeAllowed = addTags(storeRequired, TagKey.ACCOUNT, TagKey.EXCEPTION);
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
      add(
          definitions,
          STORE_ERRORS,
          storeRequired,
          storeAllowed,
          "Store failure count per component/operation.");
      add(
          definitions,
          STORE_RETRIES,
          Set.of(TagKey.COMPONENT, TagKey.OPERATION),
          Set.of(TagKey.COMPONENT, TagKey.OPERATION),
          "Store retries per component/operation.");

      Set<String> cacheBase = Set.of(TagKey.COMPONENT, TagKey.OPERATION, TagKey.CACHE_NAME);
      Set<String> cacheWithAccount = addTags(cacheBase, TagKey.ACCOUNT);
      Set<String> cacheWithResult = addTags(cacheBase, TagKey.RESULT);
      Set<String> cacheLatencyAllowed = addTags(cacheWithResult, TagKey.ACCOUNT, TagKey.EXCEPTION);
      add(
          definitions,
          CACHE_HITS,
          cacheBase,
          cacheWithAccount,
          "Cache hits count for the graph cache.");
      add(
          definitions,
          CACHE_MISSES,
          cacheBase,
          cacheWithAccount,
          "Cache misses count for the graph cache.");
      add(
          definitions,
          CACHE_SIZE,
          cacheBase,
          cacheWithAccount,
          "Approximate graph cache size by cache name.");
      add(
          definitions,
          CACHE_LATENCY,
          cacheWithResult,
          cacheLatencyAllowed,
          "Cache latency distribution for operations.");
      add(
          definitions,
          CACHE_ERRORS,
          cacheWithResult,
          cacheLatencyAllowed,
          "Cache failures per cache name.");

      Set<String> gcRequired =
          Set.of(TagKey.COMPONENT, TagKey.OPERATION, TagKey.GC_NAME, TagKey.RESULT);
      Set<String> gcAllowed = addTags(gcRequired, TagKey.EXCEPTION);
      add(definitions, GC_ERRORS, gcRequired, gcAllowed, "GC failures per GC type.");
      add(
          definitions,
          GC_RETRIES,
          Set.of(TagKey.COMPONENT, TagKey.OPERATION),
          Set.of(TagKey.COMPONENT, TagKey.OPERATION),
          "GC retries per component/operation.");
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
