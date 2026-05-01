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

public final class ReconcileFailureException extends IllegalArgumentException {
  private final ReconcileExecutor.ExecutionResult.FailureKind failureKind;
  private final ReconcileExecutor.ExecutionResult.RetryDisposition retryDisposition;
  private final ReconcileExecutor.ExecutionResult.RetryClass retryClass;

  public ReconcileFailureException(
      ReconcileExecutor.ExecutionResult.FailureKind failureKind, String message, Throwable cause) {
    this(
        failureKind,
        ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE,
        ReconcileExecutor.ExecutionResult.RetryClass.TRANSIENT_ERROR,
        message,
        cause);
  }

  public ReconcileFailureException(
      ReconcileExecutor.ExecutionResult.FailureKind failureKind,
      ReconcileExecutor.ExecutionResult.RetryDisposition retryDisposition,
      String message,
      Throwable cause) {
    this(
        failureKind,
        retryDisposition,
        ReconcileExecutor.ExecutionResult.RetryClass.TRANSIENT_ERROR,
        message,
        cause);
  }

  public ReconcileFailureException(
      ReconcileExecutor.ExecutionResult.FailureKind failureKind,
      ReconcileExecutor.ExecutionResult.RetryDisposition retryDisposition,
      ReconcileExecutor.ExecutionResult.RetryClass retryClass,
      String message,
      Throwable cause) {
    super(message, cause);
    this.failureKind =
        failureKind == null ? ReconcileExecutor.ExecutionResult.FailureKind.INTERNAL : failureKind;
    this.retryDisposition =
        retryDisposition == null
            ? ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE
            : retryDisposition;
    this.retryClass =
        retryClass == null ? ReconcileExecutor.ExecutionResult.RetryClass.NONE : retryClass;
  }

  ReconcileExecutor.ExecutionResult.FailureKind failureKind() {
    return failureKind;
  }

  ReconcileExecutor.ExecutionResult.RetryDisposition retryDisposition() {
    return retryDisposition;
  }

  ReconcileExecutor.ExecutionResult.RetryClass retryClass() {
    return retryClass;
  }
}
