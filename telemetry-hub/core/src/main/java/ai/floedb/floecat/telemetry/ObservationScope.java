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

package ai.floedb.floecat.telemetry;

/** Lifecycle handle for scoped observations. */
public interface ObservationScope extends AutoCloseable {

  /** Marks the scope as a success. */
  void success();

  /** Marks the scope as an error (providing the cause). */
  void error(Throwable throwable);

  /** Signals a retry event inside the scope. */
  void retry();

  /** Adds a runtime status tag (e.g., RPC status) before closing the scope. */
  default void status(String status) {}

  @Override
  default void close() {
    // Closing does not mutate the outcome; callers must call success()/error() explicitly.
  }
}
