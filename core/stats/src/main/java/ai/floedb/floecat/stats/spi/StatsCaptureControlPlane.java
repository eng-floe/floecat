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

import java.util.stream.Collectors;

/**
 * Control-plane entrypoint for explicit stats capture requests.
 *
 * <p>Implementations centralize capture policy and engine routing so callers such as reconciler do
 * not duplicate selection logic.
 */
public interface StatsCaptureControlPlane {

  /**
   * Executes one capture trigger attempt for the provided request.
   *
   * <p>Implementations should return:
   *
   * <ul>
   *   <li>{@code CAPTURED} when a capture payload was produced
   *   <li>{@code QUEUED} when work was accepted for deferred execution
   *   <li>{@code UNCAPTURABLE} when request/engine capabilities do not match
   *   <li>{@code DEGRADED} for runtime/transient failures
   * </ul>
   */
  StatsTriggerResult trigger(StatsCaptureRequest request);

  /**
   * Executes multiple explicit trigger attempts.
   *
   * <p>Default behavior delegates each request to {@link #trigger(StatsCaptureRequest)} in order,
   * so existing control-plane implementations remain compatible.
   */
  default StatsCaptureBatchResult triggerBatch(StatsCaptureBatchRequest batchRequest) {
    return StatsCaptureBatchResult.of(
        batchRequest.requests().stream()
            .map(
                request -> {
                  StatsTriggerResult result = trigger(request);
                  return switch (result.outcome()) {
                    case CAPTURED ->
                        StatsCaptureBatchItemResult.captured(
                            request, result.captureResult().orElseThrow());
                    case QUEUED -> StatsCaptureBatchItemResult.queued(request, result.detail());
                    case UNCAPTURABLE ->
                        StatsCaptureBatchItemResult.uncapturable(request, result.detail());
                    case DEGRADED -> StatsCaptureBatchItemResult.degraded(request, result.detail());
                  };
                })
            .collect(Collectors.toList()));
  }
}
