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

package ai.floedb.floecat.telemetry.jvm;

import ai.floedb.floecat.telemetry.MetricDef;
import ai.floedb.floecat.telemetry.MetricId;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import ai.floedb.floecat.telemetry.TelemetryContributor;
import ai.floedb.floecat.telemetry.TelemetryRegistry;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public final class JvmTelemetryContributor implements TelemetryContributor {
  private static final Map<MetricId, MetricDef> DEFINITIONS = buildDefinitions();

  private static Map<MetricId, MetricDef> buildDefinitions() {
    Map<MetricId, MetricDef> defs = new LinkedHashMap<>();
    Set<String> runtimeCommon = Set.of(TagKey.COMPONENT, TagKey.OPERATION);
    Set<String> runtimeMemoryRequired = addTags(runtimeCommon, TagKey.RESOURCE);
    Set<String> runtimeGcRequired = addTags(runtimeCommon, TagKey.GC_NAME);

    add(
        defs,
        JvmMetrics.PROCESS_CPU_USAGE,
        runtimeCommon,
        runtimeCommon,
        "Process CPU usage (0-1 fraction).");
    add(
        defs,
        JvmMetrics.MEMORY_USED,
        runtimeMemoryRequired,
        runtimeMemoryRequired,
        "Used bytes for heap/metaspace/direct buffers.");
    add(defs, JvmMetrics.THREAD_COUNT, runtimeCommon, runtimeCommon, "Current live thread count.");
    add(
        defs,
        JvmMetrics.GC_LIVE_DATA_BYTES,
        runtimeGcRequired,
        runtimeGcRequired,
        "Estimated live data (bytes) held by each garbage collector.");
    add(
        defs,
        JvmMetrics.GC_LIVE_DATA_GROWTH_RATE,
        runtimeGcRequired,
        runtimeGcRequired,
        "Live data growth rate (bytes/second) for GC-managed pools.");
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
          "Duplicate metric def in JvmTelemetryContributor: " + metric.name());
    }
  }

  private static Set<String> addTags(Set<String> base, String... extras) {
    Set<String> copy = new java.util.LinkedHashSet<>(base);
    for (String extra : extras) {
      copy.add(extra);
    }
    return Collections.unmodifiableSet(copy);
  }

  @Override
  public void contribute(TelemetryRegistry registry) {
    DEFINITIONS.values().forEach(registry::register);
  }
}
