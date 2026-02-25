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
