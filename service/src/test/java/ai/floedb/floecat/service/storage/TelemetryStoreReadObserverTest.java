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

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.service.storage.StoreReadObserver.Observation;
import ai.floedb.floecat.service.storage.StoreReadObserver.Operation;
import ai.floedb.floecat.service.storage.StoreReadObserver.ReadCall;
import ai.floedb.floecat.service.storage.StoreReadObserver.Store;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.PhaseDiagnostics;
import ai.floedb.floecat.telemetry.StoreOperationSummary;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import ai.floedb.floecat.telemetry.TestObservability;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.util.List;
import org.junit.jupiter.api.Test;

class TelemetryStoreReadObserverTest {
  @Test
  void reusesStoreMetricsAndKeepsTargetsOutOfTags() {
    TestObservability observability = new TestObservability();
    TelemetryStoreReadObserver observer = new TelemetryStoreReadObserver(observability);
    Observation observation =
        observer.begin(
            new ReadCall(Store.BLOB, Operation.GET_BATCH, 3, List.of("secret-a", "secret-b")));

    observation.success(42L);
    observation.close();

    assertThat(observability.counterValue(Telemetry.Metrics.STORE_REQUESTS)).isEqualTo(1d);
    assertThat(observability.counterValue(Telemetry.Metrics.STORE_ITEMS)).isEqualTo(3d);
    assertThat(observability.counterValue(Telemetry.Metrics.STORE_BYTES)).isEqualTo(42d);
    assertThat(observability.timerValues(Telemetry.Metrics.STORE_LATENCY)).hasSize(1);
    assertThat(observability.counterTagHistory(Telemetry.Metrics.STORE_REQUESTS).get(0))
        .contains(
            Tag.of(TagKey.COMPONENT, "blob_store"),
            Tag.of(TagKey.OPERATION, "get_batch"),
            Tag.of(TagKey.RESULT, "success"))
        .allSatisfy(tag -> assertThat(tag.value()).doesNotContain("secret"));
  }

  @Test
  void recordsFailuresWithTheSameCallAndItemUnits() {
    TestObservability observability = new TestObservability();
    TelemetryStoreReadObserver observer = new TelemetryStoreReadObserver(observability);
    Observation observation =
        observer.begin(new ReadCall(Store.POINTER, Operation.GET_BATCH, 4, List.of()));

    observation.failure(new IllegalStateException("boom"));
    observation.close();

    assertThat(observability.counterValue(Telemetry.Metrics.STORE_REQUESTS)).isEqualTo(1d);
    assertThat(observability.counterValue(Telemetry.Metrics.STORE_ITEMS)).isEqualTo(4d);
    assertThat(observability.counterValue(Telemetry.Metrics.STORE_ERRORS)).isEqualTo(1d);
    assertThat(observability.counterTagHistory(Telemetry.Metrics.STORE_REQUESTS).get(0))
        .contains(Tag.of(TagKey.RESULT, "error"));
    assertThat(
            observability
                .storeTraceScopes()
                .get(Observability.Category.STORE.name())
                .get(0)
                .errorType())
        .isEqualTo(IllegalStateException.class);
  }

  @Test
  void addsPhysicalCallsAndItemsToTheExistingRequestSummary() {
    TestObservability observability = new TestObservability();
    TelemetryStoreReadObserver observer = new TelemetryStoreReadObserver(observability);

    try (Scope ignored = StoreOperationSummary.start(Context.current(), true).makeCurrent()) {
      Observation observation =
          observer.begin(new ReadCall(Store.BLOB, Operation.GET_BATCH, 3, List.of()));
      observation.success(42L);
      observation.close();

      PhaseDiagnostics diagnostics = observability.diagnostics("svc", "op");
      StoreOperationSummary.addTo(diagnostics);
      diagnostics.emit("summary");
    }

    assertThat(observability.diagnosticEvents().get(0).fields())
        .containsEntry("store_blob_get_batch_calls", 1L)
        .containsEntry("store_blob_get_batch_items", 3L);
  }
}
