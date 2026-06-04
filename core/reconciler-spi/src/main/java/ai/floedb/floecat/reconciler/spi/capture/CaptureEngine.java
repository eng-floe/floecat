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

package ai.floedb.floecat.reconciler.spi.capture;

import ai.floedb.floecat.stats.spi.JobCostHint;
import java.util.Optional;

/** Unified SPI for file-group scoped capture that may produce stats, index data, or both. */
public interface CaptureEngine {
  String id();

  default int priority() {
    return 100;
  }

  CaptureEngineCapabilities capabilities();

  default boolean supports(CaptureEngineRequest request) {
    return capabilities().supports(request);
  }

  /**
   * Cost estimate for capturing the given request. Default: {@link JobCostHint#EXPENSIVE} —
   * file-group scoped engines typically perform full Parquet file reads.
   *
   * <p>Engines that do only footer reads should override and return {@link JobCostHint#CHEAP} or
   * {@link JobCostHint#MEDIUM}.
   */
  default JobCostHint estimatedCost(CaptureEngineRequest request) {
    return JobCostHint.EXPENSIVE;
  }

  Optional<CaptureEngineResult> capture(CaptureEngineRequest request);
}
