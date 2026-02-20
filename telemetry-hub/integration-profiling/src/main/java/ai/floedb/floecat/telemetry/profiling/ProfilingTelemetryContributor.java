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

package ai.floedb.floecat.telemetry.profiling;

import ai.floedb.floecat.telemetry.MetricDef;
import ai.floedb.floecat.telemetry.MetricId;
import ai.floedb.floecat.telemetry.Telemetry;
import ai.floedb.floecat.telemetry.TelemetryContributor;
import ai.floedb.floecat.telemetry.TelemetryRegistry;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public final class ProfilingTelemetryContributor implements TelemetryContributor {
  private static final Map<MetricId, MetricDef> DEFINITIONS = buildDefinitions();

  private static Map<MetricId, MetricDef> buildDefinitions() {
    Map<MetricId, MetricDef> defs = new LinkedHashMap<>();
    Set<String> tags =
        Set.of(
            Telemetry.TagKey.COMPONENT,
            Telemetry.TagKey.OPERATION,
            Telemetry.TagKey.TRIGGER,
            Telemetry.TagKey.SCOPE,
            Telemetry.TagKey.MODE,
            Telemetry.TagKey.RESULT,
            Telemetry.TagKey.REASON,
            Telemetry.TagKey.POLICY);
    add(
        defs,
        ProfilingMetrics.Captures.TOTAL,
        Set.of(
            Telemetry.TagKey.COMPONENT,
            Telemetry.TagKey.OPERATION,
            Telemetry.TagKey.TRIGGER,
            Telemetry.TagKey.SCOPE,
            Telemetry.TagKey.MODE,
            Telemetry.TagKey.RESULT),
        tags,
        "Profiling capture lifecycle counts (started/completed/failed/dropped).");
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
          "Duplicate metric def in ProfilingTelemetryContributor: " + metric.name());
    }
  }

  @Override
  public void contribute(TelemetryRegistry registry) {
    DEFINITIONS.values().forEach(registry::register);
  }
}
