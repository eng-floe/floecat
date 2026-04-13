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

package ai.floedb.floecat.stats.spi;

import java.util.Objects;
import java.util.Optional;

/** Per-item result emitted by batch capture and trigger paths. */
public record StatsCaptureBatchItemResult(
    StatsCaptureRequest request,
    StatsTriggerOutcome outcome,
    Optional<StatsCaptureResult> captureResult,
    String detail) {

  public StatsCaptureBatchItemResult {
    request = Objects.requireNonNull(request, "request");
    outcome = Objects.requireNonNull(outcome, "outcome");
    captureResult = Objects.requireNonNull(captureResult, "captureResult");
    detail = detail == null ? "" : detail;
  }

  public static StatsCaptureBatchItemResult captured(
      StatsCaptureRequest request, StatsCaptureResult result) {
    return new StatsCaptureBatchItemResult(
        request,
        StatsTriggerOutcome.CAPTURED,
        Optional.of(Objects.requireNonNull(result, "result")),
        "captured");
  }

  public static StatsCaptureBatchItemResult queued(StatsCaptureRequest request, String detail) {
    return new StatsCaptureBatchItemResult(
        request, StatsTriggerOutcome.QUEUED, Optional.empty(), detail);
  }

  public static StatsCaptureBatchItemResult uncapturable(
      StatsCaptureRequest request, String detail) {
    return new StatsCaptureBatchItemResult(
        request, StatsTriggerOutcome.UNCAPTURABLE, Optional.empty(), detail);
  }

  public static StatsCaptureBatchItemResult degraded(StatsCaptureRequest request, String detail) {
    return new StatsCaptureBatchItemResult(
        request, StatsTriggerOutcome.DEGRADED, Optional.empty(), detail);
  }
}
