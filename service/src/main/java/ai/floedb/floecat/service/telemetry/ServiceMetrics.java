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

    /**
     * Per-priority-class depth of the ready queue. Tag: {@code priority_class} (p0_sync, etc.).
     * Autoscaler hook: when class depth grows, dispatch capacity should increase.
     */
    public static final MetricId QUEUE_DEPTH_BY_CLASS =
        new MetricId(
            "floecat.service.reconcile.queue.depth_by_class",
            MetricType.GAUGE,
            "",
            CONTRACT,
            "service");

    /**
     * Current health band (0=GREEN, 1=YELLOW, 2=ORANGE, 3=RED). This is the primary autoscaler
     * signal — alert when value ≥ 2 (ORANGE).
     */
    public static final MetricId HEALTH_BAND =
        new MetricId(
            "floecat.service.reconcile.health_band", MetricType.GAUGE, "", CONTRACT, "service");

    /**
     * Cumulative number of jobs deferred at enqueue due to admission control. Tag: priority_class.
     */
    public static final MetricId ADMISSION_DEFERRED =
        new MetricId(
            "floecat.service.reconcile.admission.deferred.total",
            MetricType.COUNTER,
            "",
            CONTRACT,
            "service");

    /**
     * Cumulative number of starvation-aging promotions. A sustained rate indicates P3 starvation.
     */
    public static final MetricId AGING_PROMOTIONS =
        new MetricId(
            "floecat.service.reconcile.aging.promotions.total",
            MetricType.COUNTER,
            "",
            CONTRACT,
            "service");

    /**
     * Scoring signals that were present in SchedulerSignalIndex at scoring time. Tag: signal_type.
     */
    public static final MetricId SIGNAL_KNOWN =
        new MetricId(
            "floecat.service.reconcile.signal.known.total",
            MetricType.COUNTER,
            "",
            CONTRACT,
            "service");

    /**
     * Scoring signals that were absent (returned conservative default). Tag: signal_type. A
     * sustained rate indicates the write path is not running correctly.
     */
    public static final MetricId SIGNAL_UNKNOWN =
        new MetricId(
            "floecat.service.reconcile.signal.unknown.total",
            MetricType.COUNTER,
            "",
            CONTRACT,
            "service");

    /**
     * Scoring-time signal miss: incremented when a scoring read ({@code lastSuccessfulCaptureMs},
     * {@code coverageLevel}, {@code snapshotDeltaRows}) found no signal and fell back to the
     * conservative default. Tag: {@code signal_type}. Sustained high rates mean the scoring path is
     * operating blind for that signal type.
     *
     * <p>Complements {@link #SIGNAL_KNOWN}/{@link #SIGNAL_UNKNOWN} which measure write throughput;
     * this metric measures read-side availability at actual scoring time.
     */
    public static final MetricId SIGNAL_LOOKUP_MISS =
        new MetricId(
            "floecat.service.reconcile.signal.lookup.miss.total",
            MetricType.COUNTER,
            "",
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

    /**
     * Distribution of priority scores emitted by the active {@link
     * ai.floedb.floecat.service.statistics.scheduler.SchedulerPriorityPolicy}. Tag: {@code
     * priority_class}. Range: [0, 1000].
     */
    public static final MetricId SCORING_SCORE_DIST =
        new MetricId(
            "floecat.service.reconcile.scoring.score", MetricType.SUMMARY, "", CONTRACT, "service");

    /**
     * Constant-1 gauge whose {@code profile_name} tag reports the active scheduler profile. Useful
     * for dashboards and alerting rules that need to know which profile is loaded.
     */
    public static final MetricId POLICY_PROFILE =
        new MetricId(
            "floecat.service.reconcile.scheduler.profile",
            MetricType.GAUGE,
            "",
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
  }
}
