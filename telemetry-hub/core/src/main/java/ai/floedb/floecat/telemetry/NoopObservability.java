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

/** No-op implementation of {@link Observability}. */
public final class NoopObservability implements Observability {
  private static final ObservationScope NOOP_SCOPE =
      new ObservationScope() {
        @Override
        public void success() {}

        @Override
        public void error(Throwable throwable) {}

        @Override
        public void retry() {}
      };

  @Override
  public void counter(MetricId metric, double amount, Tag... tags) {}

  @Override
  public void summary(MetricId metric, double value, Tag... tags) {}

  @Override
  public void timer(MetricId metric, Duration duration, Tag... tags) {}

  @Override
  public <T extends Number> void gauge(
      MetricId metric, Supplier<T> supplier, String description, Tag... tags) {}

  @Override
  public ObservationScope observe(
      Category category, String component, String operation, Tag... tags) {
    return NOOP_SCOPE;
  }

  @Override
  public StoreTraceScope storeTraceScope(String component, String operation, Tag... tags) {
    return StoreTraceScope.NOOP;
  }
}
