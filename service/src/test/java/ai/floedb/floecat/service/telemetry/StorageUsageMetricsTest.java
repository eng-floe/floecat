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

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import ai.floedb.floecat.telemetry.TestObservability;
import org.junit.jupiter.api.Test;

class StorageUsageMetricsTest {

  @Test
  void publishesGcEstimateWithoutReadingOrWritingStorage() {
    TestObservability observability = new TestObservability();
    StorageUsageMetrics metrics = new StorageUsageMetrics();
    metrics.observability = observability;

    metrics.recordGcEstimate("acct", 12L, 900L, 3L, 4L);

    assertEquals(
        12L, observability.gauge(ServiceMetrics.Storage.ACCOUNT_GC_ESTIMATED_POINTERS).get());
    assertEquals(
        900L, observability.gauge(ServiceMetrics.Storage.ACCOUNT_GC_ESTIMATED_BYTES).get());
    assertEquals(
        0.75d,
        observability.gauge(ServiceMetrics.Storage.ACCOUNT_GC_SIZE_COVERAGE).get().doubleValue());
    assertEquals(
        java.util.List.of(Tag.of(TagKey.ACCOUNT, "acct")),
        observability.gaugeTags(ServiceMetrics.Storage.ACCOUNT_GC_SIZE_COVERAGE));
  }

  @Test
  void emptyBlobPointerSetHasUnknownCoverageAndNegativeInputsClampToZero() {
    TestObservability observability = new TestObservability();
    StorageUsageMetrics metrics = new StorageUsageMetrics();
    metrics.observability = observability;

    metrics.recordGcEstimate("acct", -1L, -2L, 0L, 0L);

    assertEquals(
        0L, observability.gauge(ServiceMetrics.Storage.ACCOUNT_GC_ESTIMATED_POINTERS).get());
    assertEquals(0L, observability.gauge(ServiceMetrics.Storage.ACCOUNT_GC_ESTIMATED_BYTES).get());
    assertEquals(
        0.0d,
        observability.gauge(ServiceMetrics.Storage.ACCOUNT_GC_SIZE_COVERAGE).get().doubleValue());
  }
}
