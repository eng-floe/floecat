package ai.floedb.floecat.service.telemetry;

import ai.floedb.floecat.service.telemetry.ServiceMetrics.Cache;
import ai.floedb.floecat.service.telemetry.ServiceMetrics.GC;
import ai.floedb.floecat.service.telemetry.ServiceMetrics.Hint;
import ai.floedb.floecat.service.telemetry.ServiceMetrics.Storage;
import ai.floedb.floecat.telemetry.MetricDef;
import ai.floedb.floecat.telemetry.MetricId;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import ai.floedb.floecat.telemetry.TelemetryContributor;
import ai.floedb.floecat.telemetry.TelemetryRegistry;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public final class ServiceTelemetryContributor implements TelemetryContributor {
  private static final Map<MetricId, MetricDef> DEFINITIONS = buildDefinitions();

  private static Map<MetricId, MetricDef> buildDefinitions() {
    Map<MetricId, MetricDef> defs = new LinkedHashMap<>();
    Set<String> empty = Collections.emptySet();
    Set<String> accountTag = Set.of(TagKey.ACCOUNT);
    Set<String> resultTag = Set.of(TagKey.RESULT);

    add(defs, Cache.ENABLED, empty, empty, "Indicator that the graph cache is enabled.");
    add(defs, Cache.MAX_SIZE, empty, empty, "Configured max entries for the graph cache.");
    add(defs, Cache.ACCOUNTS, empty, empty, "Number of accounts with active caches.");
    add(defs, Cache.ENTRIES, empty, empty, "Estimated total graph cache entries across accounts.");
    add(
        defs,
        Cache.LOAD_LATENCY,
        empty,
        empty,
        "Latency for loading graph entries when caching is enabled.");

    add(
        defs,
        GC.POINTER_RUNNING,
        empty,
        empty,
        "Indicator that the pointer GC tick is currently active (1 when running, 0 when idle).");
    add(
        defs,
        GC.POINTER_ENABLED,
        empty,
        empty,
        "Indicator that the pointer GC scheduler is enabled (1=enabled, 0=disabled).");
    add(
        defs,
        GC.POINTER_LAST_TICK_START,
        empty,
        empty,
        "Timestamp when the pointer GC last started (ms since epoch from the most recent tick). Tick interval is controlled via `floecat.gc.pointer.tick-every`.");
    add(
        defs,
        GC.POINTER_LAST_TICK_END,
        empty,
        empty,
        "Timestamp when the pointer GC last finished.");
    add(
        defs,
        GC.CAS_RUNNING,
        empty,
        empty,
        "Indicator that the CAS GC tick is currently active (1 when running, 0 when idle).");
    add(
        defs,
        GC.CAS_ENABLED,
        empty,
        empty,
        "Indicator that the CAS GC scheduler is enabled (1=enabled, 0=disabled).");
    add(defs, GC.CAS_LAST_TICK_START, empty, empty, "Timestamp when the CAS GC last started.");
    add(defs, GC.CAS_LAST_TICK_END, empty, empty, "Timestamp when the CAS GC last finished.");
    add(
        defs,
        GC.IDEMP_RUNNING,
        empty,
        empty,
        "Indicator that the idempotency GC tick is currently active (1 when running, 0 when idle).");
    add(
        defs,
        GC.IDEMP_ENABLED,
        empty,
        empty,
        "Indicator that the idempotency GC scheduler is enabled (1=enabled, 0=disabled).");
    add(
        defs,
        GC.IDEMP_LAST_TICK_START,
        empty,
        empty,
        "Timestamp when the idempotency GC last started.");
    add(
        defs,
        GC.IDEMP_LAST_TICK_END,
        empty,
        empty,
        "Timestamp when the idempotency GC last finished.");

    add(
        defs,
        Storage.ACCOUNT_POINTERS,
        accountTag,
        accountTag,
        "Per-account pointer count stored in the service.");
    add(
        defs,
        Storage.ACCOUNT_BYTES,
        accountTag,
        accountTag,
        "Per-account byte consumption for storage.");
    add(defs, Hint.CACHE_HITS, empty, empty, "Engine hint cache hits.");
    add(defs, Hint.CACHE_MISSES, empty, empty, "Engine hint cache misses.");
    add(defs, Hint.CACHE_WEIGHT, empty, empty, "Estimated weight of the hint cache.");
    return Collections.unmodifiableMap(defs);
  }

  private static void add(
      Map<MetricId, MetricDef> defs,
      MetricId metric,
      Set<String> required,
      Set<String> allowed,
      String description) {
    defs.put(metric, new MetricDef(metric, required, allowed, description));
  }

  @Override
  public void contribute(TelemetryRegistry registry) {
    DEFINITIONS.values().forEach(registry::register);
  }
}
