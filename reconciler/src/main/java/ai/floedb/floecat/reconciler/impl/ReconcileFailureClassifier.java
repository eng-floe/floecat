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

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.LinkedHashSet;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import software.amazon.awssdk.awscore.exception.AwsServiceException;

final class ReconcileFailureClassifier {
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
