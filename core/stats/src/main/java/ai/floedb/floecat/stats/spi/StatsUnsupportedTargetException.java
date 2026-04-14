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

/** Thrown when no registered stats engine supports a given request target. */
public final class StatsUnsupportedTargetException extends RuntimeException {

  private final StatsTargetType targetType;
  private final StatsCaptureRequest request;

  /**
   * Creates an unsupported-target exception with routing context.
   *
   * @param targetType canonical target category that had no matching engine
   * @param request original capture request
   */
  public StatsUnsupportedTargetException(StatsTargetType targetType, StatsCaptureRequest request) {
    super("No stats engine supports capture for target type " + targetType.name());
    this.targetType = Objects.requireNonNull(targetType, "targetType");
    this.request = Objects.requireNonNull(request, "request");
  }

  /** Returns the unsupported target category. */
  public StatsTargetType targetType() {
    return targetType;
  }

  /** Returns the original capture request associated with this error. */
  public StatsCaptureRequest request() {
    return request;
  }
}
