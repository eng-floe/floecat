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
package ai.floedb.floecat.service.telemetry;

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
    Set<String> accountTag = Set.of(TagKey.ACCOUNT);
    Set<String> flightRequired = Set.of(TagKey.COMPONENT, TagKey.OPERATION, TagKey.STATUS);
    Set<String> flightAllowed =
        Set.of(TagKey.COMPONENT, TagKey.OPERATION, TagKey.STATUS, TagKey.RESOURCE, TagKey.REASON);
    Set<String> flightInFlightRequired = Set.of(TagKey.COMPONENT, TagKey.OPERATION);
    Set<String> flightInFlightAllowed = Set.of(TagKey.COMPONENT, TagKey.OPERATION, TagKey.RESOURCE);

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
    add(
        defs,
        ServiceMetrics.Flight.REQUESTS,
        flightRequired,
        flightAllowed,
        "Total Flight requests by operation, table, and terminal status.");
    add(
        defs,
        ServiceMetrics.Flight.LATENCY,
        flightRequired,
        flightAllowed,
        "Flight request latency by operation, table, and terminal status.");
    add(
        defs,
        ServiceMetrics.Flight.ERRORS,
        Set.of(TagKey.COMPONENT, TagKey.OPERATION, TagKey.REASON),
        flightAllowed,
        "Flight request failures by operation, table, and reason.");
    add(
        defs,
        ServiceMetrics.Flight.CANCELLED,
        Set.of(TagKey.COMPONENT, TagKey.OPERATION, TagKey.REASON),
        flightAllowed,
        "Flight request cancellations by operation, table, and reason.");
    add(
        defs,
        ServiceMetrics.Flight.INFLIGHT,
        flightInFlightRequired,
        flightInFlightAllowed,
        "Current number of in-flight Flight streams.");
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
