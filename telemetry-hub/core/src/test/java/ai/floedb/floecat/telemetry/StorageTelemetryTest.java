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

import static ai.floedb.floecat.telemetry.StorageTelemetry.Backend.DYNAMODB;
import static ai.floedb.floecat.telemetry.StorageTelemetry.Operation.GET;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.telemetry.StorageTelemetry.Call;
import ai.floedb.floecat.telemetry.StorageTelemetry.Measurement;
import ai.floedb.floecat.telemetry.helpers.StoreMetrics;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Contract tests for physical storage metrics and request-local aggregation. */
class StorageTelemetryTest {
  @Test
  void recordsPhysicalBackendInsideRepositoryScope() {
    TestObservability observability = new TestObservability();
    StorageTelemetry telemetry = new StorageTelemetry(observability);
    Context context = StoreOperationSummary.start(Context.current(), true);

    try (Scope ignored = context.makeCurrent()) {
      try (ObservationScope repository =
          new StoreMetrics(observability, "repository", "table.get").observe()) {
        byte[] value =
            telemetry.observe(
                new Call(DYNAMODB, GET), () -> new byte[12], result -> Measurement.of(12, 1));
        assertThat(value).hasSize(12);
        repository.success();
      }
      PhaseDiagnostics diagnostics = observability.diagnostics("test", "summary");
      StoreOperationSummary.addTo(diagnostics);
      diagnostics.emit("summary");
    }

    assertThat(observability.diagnosticEvents().get(0).fields())
        .containsEntry("store_operations", 1L)
        .containsEntry("backend_dynamodb_get_count", 1L)
        .containsEntry("backend_dynamodb_get_bytes", 12L)
        .containsEntry("backend_dynamodb_get_items", 1L);
    assertThat(observability.storeTraceScopes().get("STORE"))
        .filteredOn(scope -> scope.component().equals("dynamodb"))
        .singleElement()
        .satisfies(scope -> assertThat(scope.operation()).isEqualTo("dynamodb.get"));
  }

  @Test
  void recordsFailureAndRethrowsIt() {
    TestObservability observability = new TestObservability();
    StorageTelemetry telemetry = new StorageTelemetry(observability);
    IllegalStateException failure = new IllegalStateException("failed");

    assertThatThrownBy(
            () ->
                telemetry.observe(
                    new Call(DYNAMODB, GET),
                    () -> {
                      throw failure;
                    },
                    ignored -> Measurement.none()))
        .isSameAs(failure);

    assertThat(observability.scopes().get("STORE").get(0).error()).isSameAs(failure);
  }

  @Test
  void preservesNotFoundAsABoundedMetricResult() {
    TestObservability observability = new TestObservability();
    StorageTelemetry telemetry = new StorageTelemetry(observability);

    telemetry.observe(new Call(DYNAMODB, GET), () -> "missing", ignored -> Measurement.notFound());

    assertThat(observability.counterTagHistory(Telemetry.Metrics.STORE_REQUESTS))
        .singleElement()
        .satisfies(tags -> assertThat(tags).contains(Tag.of("result", "not_found")));
  }

  @Test
  void requestContextsDoNotMixAcrossThreads() throws Exception {
    Map<String, Object>[] fields = new Map[2];
    Thread[] threads = new Thread[2];

    for (int index = 0; index < threads.length; index++) {
      int slot = index;
      threads[index] =
          new Thread(
              () -> {
                TestObservability observability = new TestObservability();
                StorageTelemetry telemetry = new StorageTelemetry(observability);
                Context context = StoreOperationSummary.start(Context.root(), true);
                try (Scope ignored = context.makeCurrent()) {
                  telemetry.observe(
                      new Call(DYNAMODB, GET), () -> slot, result -> Measurement.of(slot + 1L, 1));
                  PhaseDiagnostics diagnostics = observability.diagnostics("test", "summary");
                  StoreOperationSummary.addTo(diagnostics);
                  diagnostics.emit("summary-" + slot);
                  fields[slot] =
                      observability.diagnosticEvents().stream()
                          .filter(event -> event.eventName().equals("summary-" + slot))
                          .findFirst()
                          .orElseThrow()
                          .fields();
                }
              });
      threads[index].start();
    }
    for (Thread thread : threads) {
      thread.join();
    }

    assertThat(fields[0]).containsEntry("backend_dynamodb_get_bytes", 1L);
    assertThat(fields[1]).containsEntry("backend_dynamodb_get_bytes", 2L);
  }
}
