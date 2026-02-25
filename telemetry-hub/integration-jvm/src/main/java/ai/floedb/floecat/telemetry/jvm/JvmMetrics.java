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

import ai.floedb.floecat.telemetry.MetricId;
import ai.floedb.floecat.telemetry.MetricType;

public final class JvmMetrics {
  private static final String CONTRACT = "v1";
  private static final String ORIGIN = "Extra JVM";

  private JvmMetrics() {}

  public static final MetricId GC_LIVE_DATA_BYTES =
      new MetricId("floecat.jvm.gc.live.data.bytes", MetricType.GAUGE, "bytes", CONTRACT, ORIGIN);
  public static final MetricId GC_LIVE_DATA_GROWTH_RATE =
      new MetricId(
          "floecat.jvm.gc.live.data.growth.rate",
          MetricType.GAUGE,
          "bytes_per_second",
          CONTRACT,
          ORIGIN);
}
