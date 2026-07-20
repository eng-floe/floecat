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

import ai.floedb.floecat.common.rpc.Error;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import java.util.LinkedHashSet;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import software.amazon.awssdk.awscore.exception.AwsServiceException;

final class ReconcileFailureClassifier {
  private static final String STORAGE_AUTHORITY_MISSING = "storage.authority.missing";
  private static final String QUERY_SNAPSHOT_NOT_FINALIZED = "query.snapshot.not.finalized";

  private ReconcileFailureClassifier() {}

  static Exception normalize(Exception error) {
    if (error == null || error instanceof ReconcileFailureException) {
      return error;
    }
    ReconcileFailureException terminal = terminalAuthFailure(error);
    return terminal != null ? terminal : error;
  }

  static ReconcileFailureException terminalAuthFailure(Throwable error) {
    var seen = new LinkedHashSet<Throwable>();
    Throwable cur = error;
    while (cur != null && !seen.contains(cur)) {
      if (cur instanceof StatusRuntimeException sre) {
        Status.Code code = sre.getStatus().getCode();
        if (code == Status.Code.UNAUTHENTICATED || code == Status.Code.PERMISSION_DENIED) {
          return terminalInternal(sre.getMessage(), sre);
        }
      }
      if (cur instanceof ForbiddenException || cur instanceof NotAuthorizedException) {
        return terminalInternal(cur.getMessage(), cur);
      }
      if (cur instanceof AwsServiceException aws && isTerminalAwsAuthFailure(aws)) {
        return terminalInternal(aws.getMessage(), aws);
      }
      seen.add(cur);
      cur = cur.getCause();
    }
    return null;
  }

  static ReconcileExecutor.ExecutionResult.RetryDisposition retryDisposition(Throwable error) {
    ReconcileFailureException reconcileFailure = reconcileFailure(error);
    if (reconcileFailure != null) {
      return reconcileFailure.retryDisposition();
    }
    StatusRuntimeException statusError = grpcStatusError(error);
    if (statusError != null && statusError.getStatus().getCode() == Status.Code.INVALID_ARGUMENT) {
      return ReconcileExecutor.ExecutionResult.RetryDisposition.TERMINAL;
    }
    if (statusError != null
        && statusError.getStatus().getCode() == Status.Code.FAILED_PRECONDITION
        && STORAGE_AUTHORITY_MISSING.equals(floecatMessageKey(statusError))) {
      return ReconcileExecutor.ExecutionResult.RetryDisposition.TERMINAL;
    }
    return ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE;
  }

  static ReconcileExecutor.ExecutionResult.RetryClass retryClass(Throwable error) {
    ReconcileFailureException reconcileFailure = reconcileFailure(error);
    if (reconcileFailure != null) {
      return reconcileFailure.retryClass();
    }
    StatusRuntimeException statusError = grpcStatusError(error);
    if (statusError != null
        && retryDisposition(error) == ReconcileExecutor.ExecutionResult.RetryDisposition.TERMINAL) {
      return ReconcileExecutor.ExecutionResult.RetryClass.NONE;
    }
    if (statusError != null
        && statusError.getStatus().getCode() == Status.Code.FAILED_PRECONDITION
        && QUERY_SNAPSHOT_NOT_FINALIZED.equals(floecatMessageKey(statusError))) {
      return ReconcileExecutor.ExecutionResult.RetryClass.DEPENDENCY_NOT_READY;
    }
    return ReconcileExecutor.ExecutionResult.RetryClass.TRANSIENT_ERROR;
  }

  private static ReconcileFailureException reconcileFailure(Throwable error) {
    var seen = new LinkedHashSet<Throwable>();
    Throwable current = error;
    while (current != null && seen.add(current)) {
      if (current instanceof ReconcileFailureException failure) {
        return failure;
      }
      current = current.getCause();
    }
    return null;
  }

  private static StatusRuntimeException grpcStatusError(Throwable error) {
    var seen = new LinkedHashSet<Throwable>();
    Throwable current = error;
    while (current != null && seen.add(current)) {
      if (current instanceof StatusRuntimeException statusError) {
        return statusError;
      }
      current = current.getCause();
    }
    return null;
  }

  private static String floecatMessageKey(StatusRuntimeException error) {
    com.google.rpc.Status richStatus = StatusProto.fromThrowable(error);
    if (richStatus == null) {
      return "";
    }
    for (com.google.protobuf.Any detail : richStatus.getDetailsList()) {
      if (detail.is(Error.class)) {
        try {
          return detail.unpack(Error.class).getMessageKey();
        } catch (com.google.protobuf.InvalidProtocolBufferException ignored) {
          return "";
        }
      }
    }
    return "";
  }

  private static ReconcileFailureException terminalInternal(String message, Throwable cause) {
    return new ReconcileFailureException(
        ReconcileExecutor.ExecutionResult.FailureKind.INTERNAL,
        ReconcileExecutor.ExecutionResult.RetryDisposition.TERMINAL,
        message,
        cause);
  }

  private static boolean isTerminalAwsAuthFailure(AwsServiceException aws) {
    int statusCode = aws.statusCode();
    if (statusCode == 401 || statusCode == 403) {
      return true;
    }
    if (aws.awsErrorDetails() == null || aws.awsErrorDetails().errorCode() == null) {
      return false;
    }
    return switch (aws.awsErrorDetails().errorCode()) {
      case "AccessDenied",
          "AccessDeniedException",
          "ExpiredToken",
          "ExpiredTokenException",
          "Forbidden",
          "InvalidClientTokenId",
          "InvalidToken",
          "SignatureDoesNotMatch",
          "UnrecognizedClientException" ->
          true;
      default -> false;
    };
  }
}
