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

    add(defs, Cache.ENABLED, empty, empty);
    add(defs, Cache.MAX_SIZE, empty, empty);
    add(defs, Cache.ACCOUNTS, empty, empty);
    add(defs, Cache.ENTRIES, empty, empty);
    add(defs, Cache.LOAD_LATENCY, empty, resultTag);

    add(defs, GC.POINTER_RUNNING, empty, empty);
    add(defs, GC.POINTER_ENABLED, empty, empty);
    add(defs, GC.POINTER_LAST_TICK_START, empty, empty);
    add(defs, GC.POINTER_LAST_TICK_END, empty, empty);
    add(defs, GC.CAS_RUNNING, empty, empty);
    add(defs, GC.CAS_ENABLED, empty, empty);
    add(defs, GC.CAS_LAST_TICK_START, empty, empty);
    add(defs, GC.CAS_LAST_TICK_END, empty, empty);
    add(defs, GC.IDEMP_RUNNING, empty, empty);
    add(defs, GC.IDEMP_ENABLED, empty, empty);
    add(defs, GC.IDEMP_LAST_TICK_START, empty, empty);
    add(defs, GC.IDEMP_LAST_TICK_END, empty, empty);

    add(defs, Storage.ACCOUNT_POINTERS, accountTag, accountTag);
    add(defs, Storage.ACCOUNT_BYTES, accountTag, accountTag);
    add(defs, Hint.CACHE_HITS, empty, empty);
    add(defs, Hint.CACHE_MISSES, empty, empty);
    add(defs, Hint.CACHE_WEIGHT, empty, empty);
    return Collections.unmodifiableMap(defs);
  }

  private static void add(
      Map<MetricId, MetricDef> defs, MetricId metric, Set<String> required, Set<String> allowed) {
    defs.put(metric, new MetricDef(metric, required, allowed));
  }

  @Override
  public void contribute(TelemetryRegistry registry) {
    DEFINITIONS.values().forEach(registry::register);
  }
}
