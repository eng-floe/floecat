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

package ai.floedb.floecat.connector.common.auth;

import java.util.HashSet;
import java.util.Optional;
import software.amazon.awssdk.awscore.exception.AwsServiceException;

/** Typed classification shared by credential producers and consumers. */
public final class AwsCredentialFailureClassifier {
  private AwsCredentialFailureClassifier() {}

  public enum Denial {
    AUTHENTICATION,
    AUTHORIZATION
  }

  public record TerminalAuthenticationFailure(AwsServiceException cause, Denial denial) {}

  /** Finds a permanent AWS authentication failure in a cause chain. */
  public static Optional<TerminalAuthenticationFailure> findTerminalAuthenticationFailure(
      Throwable failure) {
    var seen = new HashSet<Throwable>();
    for (Throwable current = failure;
        current != null && seen.add(current);
        current = current.getCause()) {
      if (current instanceof AwsServiceException aws) {
        Optional<TerminalAuthenticationFailure> classified =
            classifyTerminalAuthenticationFailure(aws);
        if (classified.isPresent()) {
          return classified;
        }
      }
    }
    return Optional.empty();
  }

  /** Classifies one AWS service failure without inspecting wrappers. */
  public static Optional<TerminalAuthenticationFailure> classifyTerminalAuthenticationFailure(
      AwsServiceException failure) {
    if (!isTerminalAuthenticationFailure(failure)) {
      return Optional.empty();
    }
    Denial denial = failure.statusCode() == 401 ? Denial.AUTHENTICATION : Denial.AUTHORIZATION;
    return Optional.of(new TerminalAuthenticationFailure(failure, denial));
  }

  private static boolean isTerminalAuthenticationFailure(AwsServiceException failure) {
    if (failure.statusCode() == 401 || failure.statusCode() == 403) {
      return true;
    }
    if (failure.awsErrorDetails() == null || failure.awsErrorDetails().errorCode() == null) {
      return false;
    }
    return switch (failure.awsErrorDetails().errorCode()) {
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
