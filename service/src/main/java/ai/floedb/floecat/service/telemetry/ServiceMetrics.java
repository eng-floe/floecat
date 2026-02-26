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
    public static final MetricId SYNC_CAPTURE =
        new MetricId(
            "floecat.service.reconcile.sync_capture.total",
            MetricType.COUNTER,
            "",
            CONTRACT,
            "service");
    public static final MetricId TRIGGER =
        new MetricId(
            "floecat.service.reconcile.trigger.total", MetricType.COUNTER, "", CONTRACT, "service");
  }
}
