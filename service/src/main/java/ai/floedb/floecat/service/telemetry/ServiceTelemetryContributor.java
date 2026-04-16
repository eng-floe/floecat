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
    Set<String> reconcileRequired =
        Set.of(TagKey.COMPONENT, TagKey.OPERATION, TagKey.RESULT, TagKey.TRIGGER);
    Set<String> reconcileAllowed =
        Set.of(TagKey.COMPONENT, TagKey.OPERATION, TagKey.RESULT, TagKey.TRIGGER, TagKey.REASON);
    Set<String> reconcileRequestRequired =
        Set.of(TagKey.COMPONENT, TagKey.OPERATION, TagKey.RESULT);
    Set<String> reconcileRequestAllowed =
        Set.of(TagKey.COMPONENT, TagKey.OPERATION, TagKey.RESULT, TagKey.REASON);
    Set<String> reconcileExecutionRequired =
        Set.of(TagKey.COMPONENT, TagKey.OPERATION, TagKey.RESULT, TagKey.MODE);
    Set<String> reconcileExecutionAllowed =
        Set.of(TagKey.COMPONENT, TagKey.OPERATION, TagKey.RESULT, TagKey.MODE, TagKey.REASON);
    Set<String> reconcileQueueRequired = Set.of(TagKey.COMPONENT, TagKey.OPERATION);
    Set<String> reconcileQueueAllowed = Set.of(TagKey.COMPONENT, TagKey.OPERATION);
    Set<String> statsSystemScanRequired =
        Set.of(TagKey.COMPONENT, TagKey.OPERATION, TagKey.RESOURCE);
    Set<String> statsSystemScanAllowed =
        Set.of(TagKey.COMPONENT, TagKey.OPERATION, TagKey.RESOURCE);

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
    add(
        defs,
        ServiceMetrics.Reconcile.CAPTURE_NOW,
        reconcileRequired,
        reconcileAllowed,
        "CaptureNow request outcomes by trigger type.");
    add(
        defs,
        ServiceMetrics.Reconcile.START_CAPTURE,
        reconcileRequired,
        reconcileAllowed,
        "StartCapture request outcomes by trigger type.");
    add(
        defs,
        ServiceMetrics.Reconcile.GET_JOB,
        reconcileRequestRequired,
        reconcileRequestAllowed,
        "GetReconcileJob request outcomes.");
    add(
        defs,
        ServiceMetrics.Reconcile.LIST_JOBS,
        reconcileRequestRequired,
        reconcileRequestAllowed,
        "ListReconcileJobs request outcomes.");
    add(
        defs,
        ServiceMetrics.Reconcile.CANCEL_JOB,
        reconcileRequestRequired,
        reconcileRequestAllowed,
        "CancelReconcileJob request outcomes.");
    add(
        defs,
        ServiceMetrics.Reconcile.GET_SETTINGS,
        reconcileRequestRequired,
        reconcileRequestAllowed,
        "GetReconcilerSettings request outcomes.");
    add(
        defs,
        ServiceMetrics.Reconcile.UPDATE_SETTINGS,
        reconcileRequestRequired,
        reconcileRequestAllowed,
        "UpdateReconcilerSettings request outcomes.");
    add(
        defs,
        ServiceMetrics.Reconcile.JOBS,
        reconcileExecutionRequired,
        reconcileExecutionAllowed,
        "Reconcile job terminal outcomes by execution mode.");
    add(
        defs,
        ServiceMetrics.Reconcile.JOB_LATENCY,
        reconcileExecutionRequired,
        reconcileExecutionAllowed,
        "Reconcile job terminal latency by execution mode.");
    add(
        defs,
        ServiceMetrics.Reconcile.SNAPSHOTS_PROCESSED,
        reconcileExecutionRequired,
        reconcileExecutionAllowed,
        "Snapshots processed by reconcile jobs.");
    add(
        defs,
        ServiceMetrics.Reconcile.STATS_PROCESSED,
        reconcileExecutionRequired,
        reconcileExecutionAllowed,
        "Statistics payloads processed by reconcile jobs.");
    add(
        defs,
        ServiceMetrics.Reconcile.TABLES_SCANNED,
        reconcileExecutionRequired,
        reconcileExecutionAllowed,
        "Tables scanned by reconcile jobs.");
    add(
        defs,
        ServiceMetrics.Reconcile.TABLES_CHANGED,
        reconcileExecutionRequired,
        reconcileExecutionAllowed,
        "Tables changed by reconcile jobs.");
    add(
        defs,
        ServiceMetrics.Reconcile.ERRORS,
        reconcileExecutionRequired,
        reconcileExecutionAllowed,
        "Errors recorded by reconcile jobs.");
    add(
        defs,
        ServiceMetrics.Reconcile.JOBS_QUEUED,
        reconcileQueueRequired,
        reconcileQueueAllowed,
        "Current number of queued reconcile jobs.");
    add(
        defs,
        ServiceMetrics.Reconcile.JOBS_RUNNING,
        reconcileQueueRequired,
        reconcileQueueAllowed,
        "Current number of running reconcile jobs.");
    add(
        defs,
        ServiceMetrics.Reconcile.JOBS_CANCELLING,
        reconcileQueueRequired,
        reconcileQueueAllowed,
        "Current number of reconcile jobs waiting for cancellation.");
    add(
        defs,
        ServiceMetrics.Reconcile.QUEUE_OLDEST_AGE,
        reconcileQueueRequired,
        reconcileQueueAllowed,
        "Age in milliseconds of the oldest queued reconcile job.");
    add(
        defs,
        ServiceMetrics.Reconcile.PLANNER_TICKS,
        reconcileRequestRequired,
        reconcileRequestAllowed,
        "Automatic reconcile planner tick outcomes.");
    add(
        defs,
        ServiceMetrics.Reconcile.PLANNER_TICK_LATENCY,
        reconcileRequestRequired,
        reconcileRequestAllowed,
        "Automatic reconcile planner tick latency.");
    add(
        defs,
        ServiceMetrics.Reconcile.PLANNER_ENQUEUE,
        reconcileExecutionRequired,
        reconcileExecutionAllowed,
        "Automatic reconcile planner enqueue decisions by mode.");
    add(
        defs,
        ServiceMetrics.Stats.SYSTABLE_ROWS_SCANNED,
        statsSystemScanRequired,
        statsSystemScanAllowed,
        "System-table stats scan rows read from persisted storage.");
    add(
        defs,
        ServiceMetrics.Stats.SYSTABLE_ROWS_EMITTED,
        statsSystemScanRequired,
        statsSystemScanAllowed,
        "System-table stats scan rows emitted by scanners before transport-level filtering.");
    add(
        defs,
        ServiceMetrics.Stats.SYSTABLE_BATCHES_EMITTED,
        statsSystemScanRequired,
        statsSystemScanAllowed,
        "System-table stats scan Arrow batches emitted.");
    add(
        defs,
        ServiceMetrics.Stats.SYSTABLE_BUILD_LATENCY,
        statsSystemScanRequired,
        statsSystemScanAllowed,
        "System-table stats scan build latency in milliseconds.");
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
