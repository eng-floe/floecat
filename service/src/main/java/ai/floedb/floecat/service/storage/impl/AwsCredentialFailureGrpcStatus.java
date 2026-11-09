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

package ai.floedb.floecat.service.storage.impl;

import ai.floedb.floecat.common.rpc.ErrorCode;
import ai.floedb.floecat.connector.common.auth.AwsCredentialFailureClassifier;
import io.grpc.Status;
import java.util.Optional;

/** Service-protocol mapping for the protocol-neutral AWS credential verdict. */
final class AwsCredentialFailureGrpcStatus {
  private AwsCredentialFailureGrpcStatus() {}

  record Failure(Status.Code grpcCode, ErrorCode errorCode) {}

  static Optional<Failure> findTerminalAuthenticationFailure(Throwable error) {
    return AwsCredentialFailureClassifier.findTerminalAuthenticationFailure(error)
        .map(
            failure ->
                switch (failure.denial()) {
                  case AUTHENTICATION ->
                      new Failure(Status.Code.UNAUTHENTICATED, ErrorCode.MC_UNAUTHENTICATED);
                  case AUTHORIZATION ->
                      new Failure(Status.Code.PERMISSION_DENIED, ErrorCode.MC_PERMISSION_DENIED);
                });
  }
}
