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
    public static final String TASK = "task";
    public static final String REASON = "reason";
    public static final String POOL = "pool";
    public static final String RESOURCE = "resource";
    public static final String TRIGGER = "trigger";
    public static final String MODE = "mode";
    public static final String SCOPE = "scope";

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

    public static final class ObservabilityHealth {
      public static final MetricId DROPPED_METRIC =
          new MetricId(
              "floecat.core.observability.dropped.metric.total",
              MetricType.COUNTER,
              "",
              "v1",
              "core");
      public static final MetricId INVALID_METRIC =
          new MetricId(
              "floecat.core.observability.invalid.metric.total",
              MetricType.COUNTER,
              "",
              "v1",
              "core");
      public static final MetricId DUPLICATE_GAUGE =
          new MetricId(
              "floecat.core.observability.duplicate.gauge.total",
              MetricType.COUNTER,
              "",
              "v1",
              "core");
      public static final MetricId REGISTRY_SIZE =
          new MetricId(
              "floecat.core.observability.registry.size", MetricType.GAUGE, "count", "v1", "core");
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
      public static final MetricId ENABLED =
          new MetricId("floecat.core.cache.enabled", MetricType.GAUGE, "", "v1", "core");
      public static final MetricId MAX_ENTRIES =
          new MetricId("floecat.core.cache.max.entries", MetricType.GAUGE, "count", "v1", "core");
      public static final MetricId MAX_WEIGHT =
          new MetricId(
              "floecat.core.cache.max.weight.bytes", MetricType.GAUGE, "bytes", "v1", "core");
      public static final MetricId ENTRIES =
          new MetricId("floecat.core.cache.entries", MetricType.GAUGE, "count", "v1", "core");
      public static final MetricId ACCOUNTS =
          new MetricId("floecat.core.cache.accounts", MetricType.GAUGE, "count", "v1", "core");
      public static final MetricId WEIGHTED_SIZE =
          new MetricId(
              "floecat.core.cache.weighted.size.bytes", MetricType.GAUGE, "bytes", "v1", "core");
      public static final MetricId HITS =
          new MetricId("floecat.core.cache.hits", MetricType.COUNTER, "", "v1", "core");
      public static final MetricId MISSES =
          new MetricId("floecat.core.cache.misses", MetricType.COUNTER, "", "v1", "core");
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

    public static final class Task {
      public static final MetricId ENABLED =
          new MetricId("floecat.core.task.enabled", MetricType.GAUGE, "", "v1", "core");
      public static final MetricId RUNNING =
          new MetricId("floecat.core.task.running", MetricType.GAUGE, "", "v1", "core");
      public static final MetricId LAST_TICK_START =
          new MetricId(
              "floecat.core.task.last.tick.start.ms",
              MetricType.GAUGE,
              "milliseconds",
              "v1",
              "core");
      public static final MetricId LAST_TICK_END =
          new MetricId(
              "floecat.core.task.last.tick.end.ms", MetricType.GAUGE, "milliseconds", "v1", "core");
    }

    public static final class Executor {
      public static final MetricId QUEUE_DEPTH =
          new MetricId("floecat.core.exec.queue.depth", MetricType.GAUGE, "count", "v1", "core");
      public static final MetricId ACTIVE =
          new MetricId("floecat.core.exec.active", MetricType.GAUGE, "count", "v1", "core");
      public static final MetricId REJECTED =
          new MetricId("floecat.core.exec.rejected", MetricType.COUNTER, "", "v1", "core");
      public static final MetricId TASK_WAIT =
          new MetricId("floecat.core.exec.task.wait", MetricType.TIMER, "seconds", "v1", "core");
      public static final MetricId TASK_RUN =
          new MetricId("floecat.core.exec.task.run", MetricType.TIMER, "seconds", "v1", "core");
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
    public static final MetricId CACHE_ENABLED = Cache.ENABLED;
    public static final MetricId CACHE_MAX_ENTRIES = Cache.MAX_ENTRIES;
    public static final MetricId CACHE_MAX_WEIGHT = Cache.MAX_WEIGHT;
    public static final MetricId CACHE_HITS = Cache.HITS;
    public static final MetricId CACHE_MISSES = Cache.MISSES;
    public static final MetricId CACHE_SIZE = Cache.ENTRIES;
    public static final MetricId CACHE_ACCOUNTS = Cache.ACCOUNTS;
    public static final MetricId CACHE_WEIGHTED_SIZE = Cache.WEIGHTED_SIZE;
    public static final MetricId CACHE_LATENCY = Cache.LATENCY;
    public static final MetricId CACHE_ERRORS = Cache.ERRORS;
    public static final MetricId GC_COLLECTIONS = Gc.COLLECTIONS;
    public static final MetricId GC_PAUSE = Gc.PAUSE;
    public static final MetricId GC_ERRORS = Gc.ERRORS;
    public static final MetricId GC_RETRIES = Gc.RETRIES;
    public static final MetricId TASK_ENABLED = Task.ENABLED;
    public static final MetricId TASK_RUNNING = Task.RUNNING;
    public static final MetricId TASK_LAST_TICK_START = Task.LAST_TICK_START;
    public static final MetricId TASK_LAST_TICK_END = Task.LAST_TICK_END;
    public static final MetricId EXEC_QUEUE_DEPTH = Executor.QUEUE_DEPTH;
    public static final MetricId EXEC_ACTIVE = Executor.ACTIVE;
    public static final MetricId EXEC_REJECTED = Executor.REJECTED;
    public static final MetricId EXEC_TASK_WAIT = Executor.TASK_WAIT;
    public static final MetricId EXEC_TASK_RUN = Executor.TASK_RUN;
    public static final MetricId OBSERVABILITY_INVALID_METRIC = ObservabilityHealth.INVALID_METRIC;
    public static final MetricId OBSERVABILITY_DUPLICATE_GAUGE =
        ObservabilityHealth.DUPLICATE_GAUGE;
    public static final MetricId OBSERVABILITY_DROPPED_METRIC = ObservabilityHealth.DROPPED_METRIC;
    public static final MetricId OBSERVABILITY_REGISTRY_SIZE = ObservabilityHealth.REGISTRY_SIZE;
    private static final Set<String> EXECUTOR_COMMON_TAGS =
        Set.of(TagKey.COMPONENT, TagKey.OPERATION, TagKey.POOL);
    private static final Set<String> EXECUTOR_TIMER_ALLOWED =
        addTags(EXECUTOR_COMMON_TAGS, TagKey.RESULT);
    private static final Set<String> EXECUTOR_TIMER_REQUIRED = EXECUTOR_COMMON_TAGS;
    private static final Set<String> EXECUTOR_ALLOWED_TAGS = EXECUTOR_COMMON_TAGS;
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
      add(
          definitions,
          EXEC_QUEUE_DEPTH,
          EXECUTOR_COMMON_TAGS,
          EXECUTOR_ALLOWED_TAGS,
          "Number of work items waiting in the executor queue per pool.");
      add(
          definitions,
          EXEC_ACTIVE,
          EXECUTOR_COMMON_TAGS,
          EXECUTOR_ALLOWED_TAGS,
          "Number of threads actively executing tasks per pool.");
      add(
          definitions,
          EXEC_REJECTED,
          EXECUTOR_COMMON_TAGS,
          EXECUTOR_ALLOWED_TAGS,
          "Number of task submissions rejected by the executor.");
      add(
          definitions,
          EXEC_TASK_WAIT,
          EXECUTOR_TIMER_REQUIRED,
          EXECUTOR_TIMER_ALLOWED,
          "Duration spent waiting in the queue before execution starts.");
      add(
          definitions,
          EXEC_TASK_RUN,
          EXECUTOR_TIMER_REQUIRED,
          EXECUTOR_TIMER_ALLOWED,
          "Duration spent running the task on a worker thread.");

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
          "Number of cache lookup hits, tagged by cache name.");
      add(
          definitions,
          CACHE_MISSES,
          cacheBase,
          cacheWithAccount,
          "Number of cache lookup misses, tagged by cache name.");
      add(
          definitions,
          CACHE_ENABLED,
          cacheBase,
          cacheWithAccount,
          "Indicator that the cache is enabled (1=enabled, 0=disabled).");
      add(
          definitions,
          CACHE_MAX_ENTRIES,
          cacheBase,
          cacheWithAccount,
          "Configured max entries for the cache.");
      add(
          definitions,
          CACHE_MAX_WEIGHT,
          cacheBase,
          cacheWithAccount,
          "Configured maximum weight (bytes) for the cache.");
      add(
          definitions,
          CACHE_SIZE,
          cacheBase,
          cacheWithAccount,
          "Approximate number of entries in the cache, tagged by cache name.");
      add(
          definitions,
          CACHE_ACCOUNTS,
          cacheBase,
          cacheWithAccount,
          "Number of accounts with an active cache entry, tagged by cache name.");
      add(
          definitions,
          CACHE_WEIGHTED_SIZE,
          cacheBase,
          cacheWithAccount,
          "Total weight (bytes) of cache entries, tagged by cache name.");
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
          "Number of cache operation failures (load errors), tagged by cache name.");

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

      Set<String> taskRequired = Set.of(TagKey.COMPONENT, TagKey.OPERATION, TagKey.TASK);
      Set<String> taskAllowed = addTags(taskRequired, TagKey.ACCOUNT);
      add(
          definitions,
          TASK_ENABLED,
          taskRequired,
          taskAllowed,
          "Indicator that a scheduled task is enabled (1=enabled, 0=disabled).");
      add(
          definitions,
          TASK_RUNNING,
          taskRequired,
          taskAllowed,
          "Number of active ticks for the scheduled task (usually 0 or 1).");
      add(
          definitions,
          TASK_LAST_TICK_START,
          taskRequired,
          taskAllowed,
          "Timestamp (ms since epoch) when the scheduled task last started a tick.");
      add(
          definitions,
          TASK_LAST_TICK_END,
          taskRequired,
          taskAllowed,
          "Timestamp (ms since epoch) when the scheduled task last finished a tick.");
      Set<String> observabilityAllowed = Set.of(TagKey.REASON);
      add(
          definitions,
          OBSERVABILITY_DROPPED_METRIC,
          Set.of(),
          observabilityAllowed,
          "Total number of metric emissions rejected because validation failed.");
      add(
          definitions,
          OBSERVABILITY_INVALID_METRIC,
          Set.of(),
          observabilityAllowed,
          "Total number of metrics rejected because they were not registered.");
      add(
          definitions,
          OBSERVABILITY_DUPLICATE_GAUGE,
          Set.of(),
          observabilityAllowed,
          "Count of duplicate gauge registration attempts.");
      add(
          definitions,
          OBSERVABILITY_REGISTRY_SIZE,
          Set.of(),
          Set.of(),
          "Current size of the telemetry registry.");

      return Collections.unmodifiableMap(definitions);
    }

    private static void add(
        Map<MetricId, MetricDef> definitions,
        MetricId metric,
        Set<String> required,
        Set<String> allowed,
        String description) {
      MetricDef prev =
          definitions.put(metric, new MetricDef(metric, required, allowed, description));
      if (prev != null) {
        throw new IllegalArgumentException(
            "Duplicate metric def in core Telemetry.Metrics: " + metric.name());
      }
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
