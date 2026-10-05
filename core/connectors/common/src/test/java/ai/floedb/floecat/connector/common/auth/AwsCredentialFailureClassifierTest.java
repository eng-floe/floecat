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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;

class AwsCredentialFailureClassifierTest {
  @Test
  void typedTerminalErrorCodesAreAuthorizationDenials() {
    for (String errorCode :
        List.of(
            "AccessDenied",
            "AccessDeniedException",
            "ExpiredToken",
            "ExpiredTokenException",
            "Forbidden",
            "InvalidClientTokenId",
            "InvalidToken",
            "SignatureDoesNotMatch",
            "UnrecognizedClientException")) {
      AwsServiceException failure = failure(400, errorCode);

      var classified =
          AwsCredentialFailureClassifier.classifyTerminalAuthenticationFailure(failure)
              .orElseThrow();

      assertSame(failure, classified.cause(), errorCode);
      assertEquals(AwsCredentialFailureClassifier.Denial.AUTHORIZATION, classified.denial());
    }
  }

  @Test
  void httpStatusDistinguishesAuthenticationFromAuthorization() {
    assertEquals(
        AwsCredentialFailureClassifier.Denial.AUTHENTICATION,
        AwsCredentialFailureClassifier.classifyTerminalAuthenticationFailure(
                failure(401, "Unknown"))
            .orElseThrow()
            .denial());
    assertEquals(
        AwsCredentialFailureClassifier.Denial.AUTHORIZATION,
        AwsCredentialFailureClassifier.classifyTerminalAuthenticationFailure(
                failure(403, "Unknown"))
            .orElseThrow()
            .denial());
  }

  @Test
  void wholeChainLookupFindsTypedOriginButLeavesTemporaryFailuresUnclassified() {
    AwsServiceException denied = failure(403, "InvalidClientTokenId");
    var wrapped = new IllegalStateException("vend failed", new RuntimeException("sts", denied));

    assertSame(
        denied,
        AwsCredentialFailureClassifier.findTerminalAuthenticationFailure(wrapped)
            .orElseThrow()
            .cause());
    assertTrue(
        AwsCredentialFailureClassifier.findTerminalAuthenticationFailure(
                failure(429, "ThrottlingException"))
            .isEmpty());
    assertTrue(
        AwsCredentialFailureClassifier.findTerminalAuthenticationFailure(
                failure(503, "ServiceUnavailable"))
            .isEmpty());
  }

  @Test
  void awsFailureWithoutErrorDetailsIsUnclassified() {
    AwsServiceException failure =
        AwsServiceException.builder().message("unknown failure").statusCode(400).build();

    assertTrue(
        AwsCredentialFailureClassifier.classifyTerminalAuthenticationFailure(failure).isEmpty());
  }

  @Test
  void wholeChainLookupTerminatesOnACauseCycle() {
    RuntimeException first = new RuntimeException("first");
    RuntimeException second = new RuntimeException("second");
    first.initCause(second);
    second.initCause(first);

    assertTrue(AwsCredentialFailureClassifier.findTerminalAuthenticationFailure(first).isEmpty());
  }

  private static AwsServiceException failure(int statusCode, String errorCode) {
    return AwsServiceException.builder()
        .message(errorCode)
        .statusCode(statusCode)
        .awsErrorDetails(AwsErrorDetails.builder().errorCode(errorCode).build())
        .build();
  }
}
