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

import ai.floedb.floecat.connector.common.auth.TerminalCredentialRefreshException;
import ai.floedb.floecat.storage.errors.SourceCatalogVendingGrpcStatus;
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
      if (cur instanceof TerminalCredentialRefreshException terminalRefresh) {
        return terminalInternal(terminalRefresh.getMessage(), terminalRefresh);
      }
      if (cur instanceof StatusRuntimeException sre) {
        // Any auth failure, wherever it came from -- floecat's own services included, which is why
        // this stays a raw-code rule rather than folding into the structured check below. A
        // source-catalog refusal additionally carries SOURCE_CATALOG_VEND_REFUSED under these same
        // codes, so a consumer that needs to tell the two apart reads the reason, not the code.
        Status.Code code = sre.getStatus().getCode();
        if (code == Status.Code.UNAUTHENTICATED || code == Status.Code.PERMISSION_DENIED) {
          return terminalInternal(sre.getMessage(), sre);
        }
        // Matched by structured reason, not by status code: FAILED_PRECONDITION is shared with
        // lease-precondition failures, which are retryable by design. A catalog that vends an
        // incomplete session tuple, or refuses for a reason a retry cannot change -- a vanished
        // upstream table, credentials outside the table's location -- will keep producing the same
        // answer, so retrying only loops. The vending path deliberately keeps everything a later
        // attempt could clear off these two reasons.
        if (SourceCatalogVendingGrpcStatus.isVendedCredentialsNotRefreshable(sre)
            || SourceCatalogVendingGrpcStatus.isSourceCatalogVendRefused(sre)) {
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
