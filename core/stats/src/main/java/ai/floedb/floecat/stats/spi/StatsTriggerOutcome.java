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

/** High-level outcome of a stats trigger attempt. */
public enum StatsTriggerOutcome {
  /** Capture completed and returned a record payload. */
  CAPTURED,
  /** Work was accepted and queued for background execution. */
  QUEUED,
  /**
   * Request cannot be satisfied by the current engine capabilities/configuration.
   *
   * <p>Use this for permanent capability mismatches (for example unsupported target kind, connector
   * family mismatch, or requested kind gap). Registry routing treats this as retryable with the
   * next candidate engine.
   */
  UNCAPTURABLE,
  /**
   * Request could not be completed because of a runtime/transient failure.
   *
   * <p>Use this for operational failures after selection (for example connector availability,
   * network timeouts, credential issues). Registry routing treats this as final for the request.
   */
  DEGRADED
}
