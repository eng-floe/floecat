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

package ai.floedb.floecat.scanner.spi;

/** Optional telemetry callbacks for system object scans. */
public interface ScanTelemetryHook {
  default void onRowsScanned(long delta) {}

  default void onRowsEmitted(long delta) {}

  default void onBatchEmitted(long rows) {}

  default void onComplete(long elapsedMs) {}

  ScanTelemetryHook NOOP = new ScanTelemetryHook() {};
}
