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

package ai.floedb.floecat.service.storage;

import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.ObservationScope;
import ai.floedb.floecat.telemetry.StoreOperationSummary;
import ai.floedb.floecat.telemetry.helpers.StoreMetrics;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

/** Production adapter from raw metadata-store reads to the existing store telemetry contract. */
@Singleton
public class TelemetryStoreReadObserver implements StoreReadObserver {
  private final Observability observability;

  @Inject
  public TelemetryStoreReadObserver(Observability observability) {
    this.observability = observability;
  }

  @Override
  public Observation begin(ReadCall call) {
    StoreMetrics metrics =
        new StoreMetrics(observability, call.store().component(), call.operation().metricName());
    ObservationScope scope = metrics.observe();
    return new Observation() {
      @Override
      public void success(long bytes) {
        record("success", bytes);
        scope.success();
      }

      @Override
      public void failure(Throwable failure) {
        record("error", -1L);
        scope.error(failure);
      }

      private void record(String result, long bytes) {
        metrics.recordRequest(result);
        if (call.itemCount() > 0) {
          metrics.recordItems(call.itemCount(), result);
        }
        if (bytes >= 0L) {
          metrics.recordBytes(bytes, result);
        }
        StoreOperationSummary.add(call.summaryPrefix() + "_calls", 1L);
        if (call.itemCount() > 0) {
          StoreOperationSummary.add(call.summaryPrefix() + "_items", call.itemCount());
        }
      }

      @Override
      public void close() {
        scope.close();
      }
    };
  }
}
