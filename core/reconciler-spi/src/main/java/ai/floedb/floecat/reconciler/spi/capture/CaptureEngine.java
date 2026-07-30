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
   * Captures one file group, publishing each file-scoped stats record to the caller as soon as it
   * is available.
   *
   * <p>The returned result contains only group-scoped aggregate partials and index outputs. File
   * stats must not be retained in the result.
   */
  Optional<CaptureEngineResult> capture(
      CaptureEngineRequest request, CaptureFileResultConsumer fileResultConsumer);
}
