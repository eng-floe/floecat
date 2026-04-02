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

/**
 * Result payload for a stats trigger attempt.
 *
 * <p>When {@link #outcome()} is {@link StatsTriggerOutcome#CAPTURED}, {@link #captureResult()}
 * contains the engine record payload. For other outcomes, {@code captureResult} is empty and {@link
 * #detail()} carries an operator-facing reason.
 */
public record StatsTriggerResult(
    StatsTriggerOutcome outcome, Optional<StatsCaptureResult> captureResult, String detail) {

  public StatsTriggerResult {
    outcome = Objects.requireNonNull(outcome, "outcome");
    captureResult = Objects.requireNonNull(captureResult, "captureResult");
    detail = detail == null ? "" : detail;
  }

  /** Build a successful captured result with payload. */
  public static StatsTriggerResult captured(StatsCaptureResult captureResult) {
    return new StatsTriggerResult(
        StatsTriggerOutcome.CAPTURED,
        Optional.of(Objects.requireNonNull(captureResult, "captureResult")),
        "captured");
  }

  /** Build a queued outcome for deferred/background execution. */
  public static StatsTriggerResult queued(String detail) {
    return new StatsTriggerResult(StatsTriggerOutcome.QUEUED, Optional.empty(), detail);
  }

  /** Build a capability/configuration miss outcome. */
  public static StatsTriggerResult uncapturable(String detail) {
    return new StatsTriggerResult(StatsTriggerOutcome.UNCAPTURABLE, Optional.empty(), detail);
  }

  /** Build a degraded outcome for runtime/transient failures. */
  public static StatsTriggerResult degraded(String detail) {
    return new StatsTriggerResult(StatsTriggerOutcome.DEGRADED, Optional.empty(), detail);
  }

  /** Optional capture payload; present only when outcome is {@code CAPTURED}. */
  public Optional<StatsCaptureResult> captureResult() {
    return captureResult;
  }
}
