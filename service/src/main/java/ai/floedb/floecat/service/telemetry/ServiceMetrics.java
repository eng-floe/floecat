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
  }

}
