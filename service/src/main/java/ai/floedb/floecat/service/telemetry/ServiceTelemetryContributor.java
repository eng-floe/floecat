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
    Set<String> reconcileLeaseResultRequired =
        Set.of(TagKey.COMPONENT, TagKey.OPERATION, TagKey.RESULT);
    Set<String> reconcileLeaseResultAllowed =
        Set.of(TagKey.COMPONENT, TagKey.OPERATION, TagKey.RESULT);
    Set<String> reconcileLeaseSkipRequired =
        Set.of(TagKey.COMPONENT, TagKey.OPERATION, TagKey.REASON);
    Set<String> reconcileLeaseSkipAllowed =
        Set.of(TagKey.COMPONENT, TagKey.OPERATION, TagKey.REASON);
    Set<String> reconcileQueueRequired = Set.of(TagKey.COMPONENT, TagKey.OPERATION);
    Set<String> reconcileQueueAllowed = Set.of(TagKey.COMPONENT, TagKey.OPERATION);
    Set<String> gcRequired = Set.of(TagKey.COMPONENT, TagKey.OPERATION);
    Set<String> gcAllowed = Set.of(TagKey.COMPONENT, TagKey.OPERATION);
    Set<String> statsRequired = Set.of(TagKey.COMPONENT, TagKey.OPERATION);
    Set<String> statsAllowed =
        Set.of(
            TagKey.COMPONENT,
            TagKey.OPERATION,
            TagKey.RESULT,
            TagKey.TRIGGER,
            TagKey.MODE,
            TagKey.REASON,
            TagKey.RESOURCE,
            TagKey.SCOPE);

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
    Set<String> partialStateTags = Set.of(TagKey.OPERATION, TagKey.RESOURCE);
    add(
        defs,
        Storage.PARTIAL_STATE,
        partialStateTags,
        partialStateTags,
        "Stored partial-pointer-state anomalies surfaced (non-retryably) by the repository layer: a"
            + " canonical/secondary pointer mismatch that an atomic create/createIfAbsent can never"
            + " itself produce and that must be reconciled out of band.");
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
        ServiceMetrics.Reconcile.VIEWS_SCANNED,
        reconcileExecutionRequired,
        reconcileExecutionAllowed,
        "Views scanned by reconcile jobs.");
    add(
        defs,
        ServiceMetrics.Reconcile.VIEWS_CHANGED,
        reconcileExecutionRequired,
        reconcileExecutionAllowed,
        "Views changed by reconcile jobs.");
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
        ServiceMetrics.Reconcile.LEASE_NEXT_LATENCY,
        reconcileLeaseResultRequired,
        reconcileLeaseResultAllowed,
        "Durable reconcile leaseNext latency by terminal outcome.");
    add(
        defs,
        ServiceMetrics.Reconcile.LEASE_NEXT_CANDIDATES,
        reconcileLeaseResultRequired,
        reconcileLeaseResultAllowed,
        "Number of ready-queue candidates examined by each durable reconcile leaseNext call.");
    add(
        defs,
        ServiceMetrics.Reconcile.LEASE_NEXT_SCANS,
        reconcileLeaseResultRequired,
        reconcileLeaseResultAllowed,
        "Number of ready-queue slice scans performed by each durable reconcile leaseNext call.");
    add(
        defs,
        ServiceMetrics.Reconcile.LEASE_NEXT_OUTCOMES,
        reconcileLeaseResultRequired,
        reconcileLeaseResultAllowed,
        "Durable reconcile leaseNext outcomes.");
    add(
        defs,
        ServiceMetrics.Reconcile.LEASE_NEXT_SKIPS,
        reconcileLeaseSkipRequired,
        reconcileLeaseSkipAllowed,
        "Ready-queue candidates skipped by durable reconcile leaseNext, grouped by reason.");
    add(
        defs,
        ServiceMetrics.Reconcile.LEASE_SCAN_PERMITS_IN_USE,
        reconcileQueueRequired,
        reconcileQueueAllowed,
        "Current number of durable reconcile lease scan permits in use.");
    add(
        defs,
        ServiceMetrics.Reconcile.LEASE_SCAN_PERMITS_AVAILABLE,
        reconcileQueueRequired,
        reconcileQueueAllowed,
        "Current number of durable reconcile lease scan permits available.");
    add(
        defs,
        ServiceMetrics.Gc.CAS_POISONED_ACCOUNTS,
        gcRequired,
        gcAllowed,
        "Accounts whose CAS GC delete phase was poisoned in the last tick.");
    add(
        defs,
        ServiceMetrics.Gc.CAS_OLDEST_SWEEP_AGE,
        gcRequired,
        gcAllowed,
        "Age in milliseconds of the least-recently cleanly-swept CAS GC account.");
    add(
        defs,
        ServiceMetrics.Gc.RECONCILE_JOB_ACCOUNTS_LAST_TICK,
        gcRequired,
        gcAllowed,
        "Reconcile job GC accounts processed in the last completed tick.");
    add(
        defs,
        ServiceMetrics.Gc.RECONCILE_JOB_ACCOUNT_PAGE_INDEX,
        gcRequired,
        gcAllowed,
        "Current reconcile job GC index within the cached account page.");
    add(
        defs,
        ServiceMetrics.Gc.RECONCILE_JOB_ACCOUNT_PAGE_SIZE,
        gcRequired,
        gcAllowed,
        "Current reconcile job GC cached account page size.");
    add(
        defs,
        ServiceMetrics.Gc.RECONCILE_JOB_ACTIVE_ACCOUNT_TOKENS,
        gcRequired,
        gcAllowed,
        "Accounts with active reconcile job GC continuation tokens.");
    add(
        defs,
        ServiceMetrics.Gc.RECONCILE_JOB_QUARANTINED_LAST_TICK,
        gcRequired,
        gcAllowed,
        "Unreadable reconcile job GC payloads retained in the last completed tick.");
    add(
        defs,
        ServiceMetrics.Gc.RECONCILE_JOB_DELETED_LAST_TICK,
        gcRequired,
        gcAllowed,
        "Reconcile job GC pointer/blob deletes completed in the last completed tick.");
    add(
        defs,
        ServiceMetrics.Stats.BATCH_ITEMS_TOTAL,
        statsRequired,
        statsAllowed,
        "Stats batch item counters (untagged series tracks submitted items; tagged series with"
            + " result=* tracks per-outcome items; query with result tag filters to avoid"
            + " double-counting).");
    add(
        defs,
        ServiceMetrics.Stats.BATCH_GROUPS_TOTAL,
        statsRequired,
        statsAllowed,
        "Stats batch request groups processed.");
    add(
        defs,
        ServiceMetrics.Stats.ENGINE_BATCH_CALLS_TOTAL,
        statsRequired,
        statsAllowed,
        "Stats engine batch capture calls.");
    add(
        defs,
        ServiceMetrics.Stats.STORE_HITS_TOTAL,
        statsRequired,
        statsAllowed,
        "Stats store hit count for batch resolution.");
    add(
        defs,
        ServiceMetrics.Stats.STORE_MISSES_TOTAL,
        statsRequired,
        statsAllowed,
        "Stats store miss count for batch resolution.");
    add(
        defs,
        ServiceMetrics.Stats.SYNC_OUTCOMES_TOTAL,
        statsRequired,
        statsAllowed,
        "Sync-first resolution outcomes by result (HIT, CAPTURED, PARTIAL, TIMEOUT, FAILED,"
            + " SKIPPED).");
    add(
        defs,
        ServiceMetrics.Stats.SYNC_LATENCY,
        statsRequired,
        statsAllowed,
        "End-to-end latency of a single sync-first resolution attempt including store reads.");
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
