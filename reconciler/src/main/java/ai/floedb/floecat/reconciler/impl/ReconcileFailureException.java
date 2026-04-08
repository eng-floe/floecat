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

import ai.floedb.floecat.reconciler.spi.ReconcileExecutor;

final class ReconcileFailureException extends RuntimeException {
  private final ReconcileExecutor.ExecutionResult.FailureKind failureKind;

  ReconcileFailureException(
      ReconcileExecutor.ExecutionResult.FailureKind failureKind, String message, Throwable cause) {
    super(message, cause);
    this.failureKind =
        failureKind == null ? ReconcileExecutor.ExecutionResult.FailureKind.INTERNAL : failureKind;
  }

  ReconcileExecutor.ExecutionResult.FailureKind failureKind() {
    return failureKind;
  }
}
