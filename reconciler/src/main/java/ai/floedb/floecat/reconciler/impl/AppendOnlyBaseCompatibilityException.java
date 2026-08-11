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

package ai.floedb.floecat.reconciler.impl;

/**
 * Marks a deterministic append-only contract violation. Only failures that would repeat on every
 * replay belong here: the service reacts by scheduling a full capture, so transient blob reads must
 * keep throwing their own storage exceptions and stay retryable.
 */
final class AppendOnlyBaseCompatibilityException extends IllegalArgumentException {
  AppendOnlyBaseCompatibilityException(String message) {
    super(message);
  }

  AppendOnlyBaseCompatibilityException(String message, Throwable cause) {
    super(message, cause);
  }
}
