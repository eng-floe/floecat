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

import java.util.Optional;

/**
 * Service Provider Interface for pluggable statistics capture.
 *
 * <p>OSS engine authors: implement this interface, annotate the implementation with {@code
 * @jakarta.enterprise.context.ApplicationScoped}, and it will be auto-discovered by {@code
 * StatsEngineRegistry} through CDI {@code Instance<>} injection.
 *
 * <p>See native connector engines (for example Iceberg/Delta) for reference implementations.
 */
public interface StatsCaptureEngine {
  /**
   * Stable unique engine identifier.
   *
   * <p>Used for logging, diagnostics, and result attribution.
   */
  String id();

  /**
   * Capture priority where lower values win.
   *
   * <p>Baseline persisted engine uses {@code 10_000}. Most specialized engines should use lower
   * values. The default return value is {@code 100}, which takes priority over the baseline.
   */
  default int priority() {
    return 100;
  }

  /** Advertised static capabilities for routing. */
  StatsCapabilities capabilities();

  /**
   * Returns whether this engine supports the request.
   *
   * <p>The default implementation delegates to {@link #capabilities()}. Override to add dynamic
   * checks (for example feature flags or table-specific constraints).
   */
  default boolean supports(StatsCaptureRequest request) {
    return capabilities().supports(request);
  }

  /** Attempts to capture stats for the request. */
  Optional<StatsCaptureResult> capture(StatsCaptureRequest request);

  /**
   * Attempts to capture stats for multiple requests.
   *
   * <p>Implementations must satisfy all of the following:
   *
   * <ol>
   *   <li>Return a result list with {@code results().size() == batchRequest.requests().size()}.
   *   <li>Preserve input order. Result index {@code i} must correspond to request index {@code i}.
   *   <li>Process all items. Failure for one item must not prevent attempts for other items.
   *   <li>Represent failures as per-item {@link StatsTriggerOutcome#DEGRADED} or {@link
   *       StatsTriggerOutcome#UNCAPTURABLE} outcomes instead of throwing.
   * </ol>
   */
  StatsCaptureBatchResult captureBatch(StatsCaptureBatchRequest batchRequest);
}
