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

    add(defs, Cache.ENABLED, empty, empty, "Indicator that the graph cache is enabled.");
    add(defs, Cache.MAX_SIZE, empty, empty, "Configured max entries for the graph cache.");
    add(defs, Cache.ACCOUNTS, empty, empty, "Number of accounts with active caches.");

    Set<String> gcTag = Set.of(TagKey.GC_NAME);
    add(
        defs,
        GC.SCHEDULER_RUNNING,
        gcTag,
        gcTag,
        "Whether a GC scheduler tick is currently active (1=running, 0=idle), tagged by gc type.");
    add(
        defs,
        GC.SCHEDULER_ENABLED,
        gcTag,
        gcTag,
        "Whether a GC scheduler is enabled (1=enabled, 0=disabled), tagged by gc type.");
    add(
        defs,
        GC.SCHEDULER_LAST_TICK_START,
        gcTag,
        gcTag,
        "Timestamp (ms since epoch) when the GC scheduler last started a tick, tagged by gc type.");
    add(
        defs,
        GC.SCHEDULER_LAST_TICK_END,
        gcTag,
        gcTag,
        "Timestamp (ms since epoch) when the GC scheduler last finished a tick, tagged by gc type.");
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
        "Estimated per-account storage byte consumption (sampled, not exact).");
    add(defs, Hint.CACHE_WEIGHT, empty, empty, "Estimated weight of the hint cache.");
    return Collections.unmodifiableMap(defs);
  }

  private static void add(
      Map<MetricId, MetricDef> defs,
      MetricId metric,
      Set<String> required,
      Set<String> allowed,
      String description) {
    MetricDef prev = defs.put(metric, new MetricDef(metric, required, allowed, description));
    if (prev != null) {
      throw new IllegalArgumentException(
          "Duplicate metric def in ServiceTelemetryContributor: " + metric.name());
    }
  }

  @Override
  public void contribute(TelemetryRegistry registry) {
    DEFINITIONS.values().forEach(registry::register);
  }
}
