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

import ai.floedb.floecat.telemetry.MetricId;
import ai.floedb.floecat.telemetry.MetricType;

public final class ServiceMetrics {

  private ServiceMetrics() {}

  private static final String CONTRACT = "v1";

  public static final class Storage {
    public static final MetricId ACCOUNT_POINTERS =
        new MetricId(
            "floecat.service.storage.account.pointers", MetricType.GAUGE, "", CONTRACT, "service");
    public static final MetricId ACCOUNT_BYTES =
        new MetricId(
            "floecat.service.storage.account.bytes",
            MetricType.GAUGE,
            "bytes",
            CONTRACT,
            "service");
    public static final MetricId PARTIAL_STATE =
        new MetricId(
            "floecat.service.storage.partial_state.total",
            MetricType.COUNTER,
            "",
            CONTRACT,
            "service");
    public static final MetricId REFRESH_DURATION =
        new MetricId(
            "floecat.service.storage.refresh.duration",
            MetricType.TIMER,
            "ms",
            CONTRACT,
            "service");
    public static final MetricId REBUILD_OBJECTS_SAMPLED =
        new MetricId(
            "floecat.service.storage.rebuild.objects_sampled",
            MetricType.SUMMARY,
            "count",
            CONTRACT,
            "service");
    public static final MetricId REBUILD_DURATION =
        new MetricId(
            "floecat.service.storage.rebuild.duration",
            MetricType.TIMER,
            "ms",
            CONTRACT,
            "service");
    public static final MetricId FAILURES =
        new MetricId(
            "floecat.service.storage.failures.total", MetricType.COUNTER, "", CONTRACT, "service");
  }

  public static final class Flight {
    public static final MetricId REQUESTS =
        new MetricId(
            "floecat.service.flight.requests.total", MetricType.COUNTER, "", CONTRACT, "service");
    public static final MetricId LATENCY =
        new MetricId("floecat.service.flight.latency", MetricType.TIMER, "ms", CONTRACT, "service");
    public static final MetricId ERRORS =
        new MetricId(
            "floecat.service.flight.errors.total", MetricType.COUNTER, "", CONTRACT, "service");
    public static final MetricId CANCELLED =
        new MetricId(
            "floecat.service.flight.cancelled.total", MetricType.COUNTER, "", CONTRACT, "service");
    public static final MetricId INFLIGHT =
        new MetricId("floecat.service.flight.inflight", MetricType.GAUGE, "", CONTRACT, "service");
  }

  public static final class Reconcile {
    public static final MetricId CAPTURE_NOW =
        new MetricId(
            "floecat.service.reconcile.capture_now.total",
            MetricType.COUNTER,
            "",
            CONTRACT,
            "service");
    public static final MetricId START_CAPTURE =
        new MetricId(
            "floecat.service.reconcile.start_capture.total",
            MetricType.COUNTER,
            "",
            CONTRACT,
            "service");
    public static final MetricId GET_JOB =
        new MetricId(
            "floecat.service.reconcile.get_job.total", MetricType.COUNTER, "", CONTRACT, "service");
    public static final MetricId LIST_JOBS =
        new MetricId(
            "floecat.service.reconcile.list_jobs.total",
            MetricType.COUNTER,
            "",
            CONTRACT,
            "service");
    public static final MetricId CANCEL_JOB =
        new MetricId(
            "floecat.service.reconcile.cancel_job.total",
            MetricType.COUNTER,
            "",
            CONTRACT,
            "service");
    public static final MetricId GET_SETTINGS =
        new MetricId(
            "floecat.service.reconcile.get_settings.total",
            MetricType.COUNTER,
            "",
            CONTRACT,
            "service");
    public static final MetricId UPDATE_SETTINGS =
        new MetricId(
            "floecat.service.reconcile.update_settings.total",
            MetricType.COUNTER,
            "",
            CONTRACT,
            "service");
    public static final MetricId JOBS =
        new MetricId(
            "floecat.service.reconcile.jobs.total", MetricType.COUNTER, "", CONTRACT, "service");
    public static final MetricId JOB_LATENCY =
        new MetricId(
            "floecat.service.reconcile.job.latency", MetricType.TIMER, "ms", CONTRACT, "service");
    public static final MetricId SNAPSHOTS_PROCESSED =
        new MetricId(
            "floecat.service.reconcile.snapshots_processed.total",
            MetricType.COUNTER,
            "",
            CONTRACT,
            "service");
    public static final MetricId STATS_PROCESSED =
        new MetricId(
            "floecat.service.reconcile.stats_processed.total",
            MetricType.COUNTER,
            "",
            CONTRACT,
            "service");
    public static final MetricId TABLES_SCANNED =
        new MetricId(
            "floecat.service.reconcile.tables_scanned.total",
            MetricType.COUNTER,
            "",
            CONTRACT,
            "service");
    public static final MetricId TABLES_CHANGED =
        new MetricId(
            "floecat.service.reconcile.tables_changed.total",
            MetricType.COUNTER,
            "",
            CONTRACT,
            "service");
    public static final MetricId VIEWS_SCANNED =
        new MetricId(
            "floecat.service.reconcile.views_scanned.total",
            MetricType.COUNTER,
            "",
            CONTRACT,
            "service");
    public static final MetricId VIEWS_CHANGED =
        new MetricId(
            "floecat.service.reconcile.views_changed.total",
            MetricType.COUNTER,
            "",
            CONTRACT,
            "service");
    public static final MetricId ERRORS =
        new MetricId(
            "floecat.service.reconcile.errors.total", MetricType.COUNTER, "", CONTRACT, "service");
    public static final MetricId JOBS_QUEUED =
        new MetricId(
            "floecat.service.reconcile.jobs.queued", MetricType.GAUGE, "", CONTRACT, "service");
    public static final MetricId JOBS_RUNNING =
        new MetricId(
            "floecat.service.reconcile.jobs.running", MetricType.GAUGE, "", CONTRACT, "service");
    public static final MetricId JOBS_CANCELLING =
        new MetricId(
            "floecat.service.reconcile.jobs.cancelling", MetricType.GAUGE, "", CONTRACT, "service");
    public static final MetricId QUEUE_OLDEST_AGE =
        new MetricId(
            "floecat.service.reconcile.queue.oldest_age",
            MetricType.GAUGE,
            "ms",
            CONTRACT,
            "service");
    public static final MetricId PLANNER_TICKS =
        new MetricId(
            "floecat.service.reconcile.planner.ticks.total",
            MetricType.COUNTER,
            "",
            CONTRACT,
            "service");
    public static final MetricId PLANNER_TICK_LATENCY =
        new MetricId(
            "floecat.service.reconcile.planner.tick.latency",
            MetricType.TIMER,
            "ms",
            CONTRACT,
            "service");
    public static final MetricId PLANNER_ENQUEUE =
        new MetricId(
            "floecat.service.reconcile.planner.enqueue.total",
            MetricType.COUNTER,
            "",
            CONTRACT,
            "service");
    public static final MetricId LEASE_NEXT_LATENCY =
        new MetricId(
            "floecat.service.reconcile.lease_next.latency",
            MetricType.TIMER,
            "ms",
            CONTRACT,
            "service");
    public static final MetricId LEASE_NEXT_CANDIDATES =
        new MetricId(
            "floecat.service.reconcile.lease_next.candidates",
            MetricType.SUMMARY,
            "count",
            CONTRACT,
            "service");
    public static final MetricId LEASE_NEXT_SCANS =
        new MetricId(
            "floecat.service.reconcile.lease_next.scans",
            MetricType.SUMMARY,
            "count",
            CONTRACT,
            "service");
    public static final MetricId LEASE_NEXT_OUTCOMES =
        new MetricId(
            "floecat.service.reconcile.lease_next.outcomes.total",
            MetricType.COUNTER,
            "",
            CONTRACT,
            "service");
    public static final MetricId LEASE_NEXT_SKIPS =
        new MetricId(
            "floecat.service.reconcile.lease_next.skips.total",
            MetricType.COUNTER,
            "",
            CONTRACT,
            "service");
    public static final MetricId LEASE_SCAN_PERMITS_IN_USE =
        new MetricId(
            "floecat.service.reconcile.lease_scan.permits.in_use",
            MetricType.GAUGE,
            "count",
            CONTRACT,
            "service");
    public static final MetricId LEASE_SCAN_PERMITS_AVAILABLE =
        new MetricId(
            "floecat.service.reconcile.lease_scan.permits.available",
            MetricType.GAUGE,
            "count",
            CONTRACT,
            "service");
  }

  public static final class Stats {
    public static final MetricId BATCH_ITEMS_TOTAL =
        new MetricId(
            "floecat.service.stats.batch_items.total", MetricType.COUNTER, "", CONTRACT, "service");
    public static final MetricId BATCH_GROUPS_TOTAL =
        new MetricId(
            "floecat.service.stats.batch_groups.total",
            MetricType.COUNTER,
            "",
            CONTRACT,
            "service");
    public static final MetricId ENGINE_BATCH_CALLS_TOTAL =
        new MetricId(
            "floecat.service.stats.engine_batch_calls.total",
            MetricType.COUNTER,
            "",
            CONTRACT,
            "service");
    public static final MetricId STORE_HITS_TOTAL =
        new MetricId(
            "floecat.service.stats.store_hits.total", MetricType.COUNTER, "", CONTRACT, "service");
    public static final MetricId STORE_MISSES_TOTAL =
        new MetricId(
            "floecat.service.stats.store_misses.total",
            MetricType.COUNTER,
            "",
            CONTRACT,
            "service");
    public static final MetricId SYNC_OUTCOMES_TOTAL =
        new MetricId(
            "floecat.service.stats.sync_outcomes.total",
            MetricType.COUNTER,
            "",
            CONTRACT,
            "service");
    public static final MetricId SYNC_LATENCY =
        new MetricId(
            "floecat.service.stats.sync.latency", MetricType.TIMER, "ms", CONTRACT, "service");

    /**
     * Per-target planner lookup outcomes, tagged {@code result=<outcome>} with the ladder rung that
     * served (or failed) each target: cache/pinned/newest-fill/partial/stale/captured/
     * pending/failed. Answers "which rung is serving planner stats" without log archaeology.
     */
    public static final MetricId PLANNER_LOOKUP_OUTCOMES_TOTAL =
        new MetricId(
            "floecat.service.stats.planner_lookup_outcomes.total",
            MetricType.COUNTER,
            "",
            CONTRACT,
            "service");
  }

  /** CAS blob-GC backlog health: the signals that answer "is GC falling behind?". */
  public static final class Gc {
    private Gc() {}

    /**
     * Accounts whose delete phase was skipped ("poisoned") in the last tick because a root-chain
     * walk could not complete. A persistently non-zero value means an account's garbage is
     * accumulating until the corrupt chain is repaired.
     */
    public static final MetricId CAS_POISONED_ACCOUNTS =
        new MetricId(
            "floecat.service.gc.cas.poisoned_accounts", MetricType.GAUGE, "", CONTRACT, "service");

    /**
     * Accounts whose sweep was skipped in the last tick because the blob store cannot delete by
     * immutable version (on S3: bucket versioning not Enabled, or s3:GetBucketVersioning denied).
     * Fail-closed is safe but collects NOTHING — a persistently non-zero value means the bucket or
     * IAM policy is misconfigured and garbage is accumulating.
     */
    public static final MetricId CAS_DELETE_UNSUPPORTED_ACCOUNTS =
        new MetricId(
            "floecat.service.gc.cas.delete_unsupported_accounts",
            MetricType.GAUGE,
            "",
            CONTRACT,
            "service");

    /**
     * Age in milliseconds of the LEAST-recently cleanly-swept account — the direct "GC is N behind"
     * signal. It climbs when an account is poisoned or is starved by the per-tick deadline, and
     * resets as each account completes a clean sweep.
     */
    public static final MetricId CAS_OLDEST_SWEEP_AGE =
        new MetricId(
            "floecat.service.gc.cas.oldest_sweep_age", MetricType.GAUGE, "ms", CONTRACT, "service");

    public static final MetricId RECONCILE_JOB_ACCOUNTS_LAST_TICK =
        new MetricId(
            "floecat.service.gc.reconcile_jobs.accounts.last_tick",
            MetricType.GAUGE,
            "count",
            CONTRACT,
            "service");
    public static final MetricId RECONCILE_JOB_ACCOUNT_PAGE_INDEX =
        new MetricId(
            "floecat.service.gc.reconcile_jobs.account_page.index",
            MetricType.GAUGE,
            "count",
            CONTRACT,
            "service");
    public static final MetricId RECONCILE_JOB_ACCOUNT_PAGE_SIZE =
        new MetricId(
            "floecat.service.gc.reconcile_jobs.account_page.size",
            MetricType.GAUGE,
            "count",
            CONTRACT,
            "service");
    public static final MetricId RECONCILE_JOB_ACTIVE_ACCOUNT_TOKENS =
        new MetricId(
            "floecat.service.gc.reconcile_jobs.account_tokens.active",
            MetricType.GAUGE,
            "count",
            CONTRACT,
            "service");
    public static final MetricId RECONCILE_JOB_QUARANTINED_LAST_TICK =
        new MetricId(
            "floecat.service.gc.reconcile_jobs.quarantined.last_tick",
            MetricType.GAUGE,
            "count",
            CONTRACT,
            "service");
    public static final MetricId RECONCILE_JOB_DELETED_LAST_TICK =
        new MetricId(
            "floecat.service.gc.reconcile_jobs.deleted.last_tick",
            MetricType.GAUGE,
            "count",
            CONTRACT,
            "service");
  }
}
