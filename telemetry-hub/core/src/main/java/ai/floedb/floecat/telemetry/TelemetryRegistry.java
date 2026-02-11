package ai.floedb.floecat.telemetry;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Registry of all declared metrics. */
public final class TelemetryRegistry {
  private final Map<String, MetricDef> metrics = new LinkedHashMap<>();

  /** Registers another contributor’s metrics. */
  public synchronized void register(TelemetryContributor contributor) {
    Objects.requireNonNull(contributor, "contributor");
    TelemetryRegistry temp = new TelemetryRegistry();
    contributor.contribute(temp);
    Map<String, MetricDef> newDefs = temp.metrics();
    for (String name : newDefs.keySet()) {
      if (metrics.containsKey(name)) {
        throw new IllegalArgumentException("Metric already registered: " + name);
      }
    }
    metrics.putAll(newDefs);
  }

  /** Registers another metric definition. */
  public synchronized void register(MetricDef def) {
    Objects.requireNonNull(def, "def");
    String name = def.id().name();
    if (metrics.containsKey(name)) {
      throw new IllegalArgumentException("Metric already registered: " + name);
    }
    metrics.put(name, def);
  }

  /** Returns the definition for the given metric, or null if unknown. */
  public synchronized MetricDef metric(String name) {
    return metrics.get(name);
  }

  /** Returns an ordered map of all registered metrics. */
  public synchronized Map<String, MetricDef> metrics() {
    return Collections.unmodifiableMap(new LinkedHashMap<>(metrics));
  }
}
