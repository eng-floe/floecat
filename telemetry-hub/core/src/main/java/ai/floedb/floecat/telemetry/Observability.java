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
package ai.floedb.floecat.telemetry;

import java.time.Duration;
import java.util.function.Supplier;

/** Interface that application code uses to emit telemetry. */
public interface Observability {

  /** Metric categories supported by the hub. */
  enum Category {
    RPC,
    STORE,
    CACHE,
    BACKGROUND,
    GC,
    OTHER
  }

  void counter(MetricId metric, double amount, Tag... tags);

  void summary(MetricId metric, double value, Tag... tags);

  void timer(MetricId metric, Duration duration, Tag... tags);

  <T extends Number> void gauge(
      MetricId metric, Supplier<T> supplier, String description, Tag... tags);

  ObservationScope observe(Category category, String component, String operation, Tag... tags);

  /** Starts a store child span that parallels the store metrics scopes. */
  default StoreTraceScope storeTraceScope(String component, String operation, Tag... tags) {
    return StoreTraceScope.NOOP;
  }
}
