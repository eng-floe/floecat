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

package ai.floedb.floecat.gateway.iceberg.rest.table.transaction;

import ai.floedb.floecat.common.rpc.Error;
import ai.floedb.floecat.common.rpc.ErrorCode;
import ai.floedb.floecat.gateway.iceberg.rest.support.IcebergErrorResponses;
import ai.floedb.floecat.transaction.rpc.TransactionState;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.core.Response;

@ApplicationScoped
public class TransactionOutcomePolicy {
  public enum OutcomeClass {
    DETERMINISTIC_LOCAL_FAILURE,
    KNOWN_NOT_APPLIED,
    KNOWN_APPLIED,
    AMBIGUOUS_BEGIN_READBACK,
    AMBIGUOUS_PREPARE_CONFIRMATION,
    AMBIGUOUS_COMMIT_STATE,
    POST_COMMIT_HYDRATION_FAILURE
  }

  public boolean isApplied(TransactionState state) {
    return state == TransactionState.TS_APPLIED;
  }

  public OutcomeClass classifyBeginReadbackFailure(Throwable failure) {
    return OutcomeClass.AMBIGUOUS_BEGIN_READBACK;
  }

  public OutcomeClass classifyPrepareFailure(Throwable failure) {
    if (!(failure instanceof StatusRuntimeException statusFailure)) {
      return OutcomeClass.AMBIGUOUS_PREPARE_CONFIRMATION;
    }
    Status.Code code = statusFailure.getStatus().getCode();
    if (code == Status.Code.NOT_FOUND
        || code == Status.Code.FAILED_PRECONDITION
        || code == Status.Code.ALREADY_EXISTS) {
      return OutcomeClass.KNOWN_NOT_APPLIED;
    }
    if (code == Status.Code.ABORTED
        && extractFloecatErrorCode(statusFailure) != ErrorCode.MC_ABORT_RETRYABLE) {
      return OutcomeClass.KNOWN_NOT_APPLIED;
    }
    return OutcomeClass.AMBIGUOUS_PREPARE_CONFIRMATION;
  }

  public OutcomeClass classifyCommitState(TransactionState state) {
    if (isApplied(state)) {
      return OutcomeClass.KNOWN_APPLIED;
    }
    if (isDeterministicFailedState(state)) {
      return OutcomeClass.KNOWN_NOT_APPLIED;
    }
    return OutcomeClass.AMBIGUOUS_COMMIT_STATE;
  }

  public OutcomeClass classifyHydrationFailure(Throwable failure) {
    return OutcomeClass.POST_COMMIT_HYDRATION_FAILURE;
  }

  public boolean isDeterministicFailedState(TransactionState state) {
    return state == TransactionState.TS_APPLY_FAILED_CONFLICT
        || state == TransactionState.TS_ABORTED;
  }

  public boolean shouldConfirmAmbiguousCommitState(TransactionState state) {
    return state == TransactionState.TS_UNSPECIFIED
        || state == TransactionState.TS_OPEN
        || state == TransactionState.TS_PREPARED
        || state == TransactionState.TS_APPLYING
        || state == TransactionState.TS_APPLY_FAILED_RETRYABLE;
  }

  public Response mapPrepareFailure(StatusRuntimeException failure) {
    Status.Code code = failure.getStatus().getCode();
    if (code == Status.Code.NOT_FOUND) {
      return IcebergErrorResponses.failure(
          "transaction commit failed", "NoSuchTableException", Response.Status.NOT_FOUND);
    }
    if (code == Status.Code.FAILED_PRECONDITION || code == Status.Code.ALREADY_EXISTS) {
      return IcebergErrorResponses.failure(
          "transaction commit failed", "CommitFailedException", Response.Status.CONFLICT);
    }
    if (code == Status.Code.ABORTED) {
      ErrorCode detailCode = extractFloecatErrorCode(failure);
      if (detailCode == ErrorCode.MC_ABORT_RETRYABLE) {
        return stateUnknown();
      }
      return IcebergErrorResponses.failure(
          "transaction commit failed", "CommitFailedException", Response.Status.CONFLICT);
    }
    Response mapped = mapCommitFailure(code);
    return mapped == null ? internalStateUnknown() : mapped;
  }

  public Response mapBeginReadbackFailure(Throwable failure) {
    if (!(failure instanceof StatusRuntimeException statusFailure)) {
      return internalStateUnknown();
    }
    Response mapped = mapAmbiguousStateFailure(statusFailure.getStatus().getCode());
    return mapped == null ? internalStateUnknown() : mapped;
  }

  public Response mapCommitFailure(StatusRuntimeException failure) {
    if (isDeterministicCommitFailure(failure)) {
      return IcebergErrorResponses.failure(
          "transaction commit failed", "CommitFailedException", Response.Status.CONFLICT);
    }
    if (isRetryableCommitAbort(failure)) {
      return stateUnknown();
    }
    Response mapped = mapCommitFailure(failure.getStatus().getCode());
    return mapped == null ? internalStateUnknown() : mapped;
  }

  public boolean isDeterministicCommitFailure(StatusRuntimeException failure) {
    Status.Code code = failure.getStatus().getCode();
    if (code == Status.Code.ABORTED) {
      return extractFloecatErrorCode(failure) != ErrorCode.MC_ABORT_RETRYABLE;
    }
    return code == Status.Code.FAILED_PRECONDITION || code == Status.Code.ALREADY_EXISTS;
  }

  public boolean isRetryableCommitAbort(StatusRuntimeException failure) {
    return failure.getStatus().getCode() == Status.Code.ABORTED
        && extractFloecatErrorCode(failure) == ErrorCode.MC_ABORT_RETRYABLE;
  }

  public Response stateUnknown() {
    return IcebergErrorResponses.failure(
        "transaction commit failed",
        "CommitStateUnknownException",
        Response.Status.SERVICE_UNAVAILABLE);
  }

  public Response internalStateUnknown() {
    return IcebergErrorResponses.failure(
        "transaction commit failed",
        "CommitStateUnknownException",
        Response.Status.INTERNAL_SERVER_ERROR);
  }

  private Response mapAmbiguousStateFailure(Status.Code code) {
    if (code == Status.Code.UNAVAILABLE) {
      return stateUnknown();
    }
    if (code == Status.Code.UNKNOWN) {
      return IcebergErrorResponses.failure(
          "transaction state could not be confirmed",
          "CommitStateUnknownException",
          Response.Status.BAD_GATEWAY);
    }
    if (code == Status.Code.DEADLINE_EXCEEDED) {
      return IcebergErrorResponses.failure(
          "transaction state could not be confirmed",
          "CommitStateUnknownException",
          Response.Status.GATEWAY_TIMEOUT);
    }
    return null;
  }

  private Response mapCommitFailure(Status.Code code) {
    if (code == Status.Code.INVALID_ARGUMENT) {
      return IcebergErrorResponses.failure(
          "transaction commit failed", "ValidationException", Response.Status.BAD_REQUEST);
    }
    if (code == Status.Code.UNAVAILABLE) {
      return stateUnknown();
    }
    if (code == Status.Code.UNAUTHENTICATED) {
      return IcebergErrorResponses.failure(
          "transaction commit failed", "UnauthorizedException", Response.Status.UNAUTHORIZED);
    }
    if (code == Status.Code.PERMISSION_DENIED) {
      return IcebergErrorResponses.failure(
          "transaction commit failed", "ForbiddenException", Response.Status.FORBIDDEN);
    }
    if (code == Status.Code.UNKNOWN) {
      return IcebergErrorResponses.failure(
          "transaction commit failed", "CommitStateUnknownException", Response.Status.BAD_GATEWAY);
    }
    if (code == Status.Code.DEADLINE_EXCEEDED) {
      return IcebergErrorResponses.failure(
          "transaction commit failed",
          "CommitStateUnknownException",
          Response.Status.GATEWAY_TIMEOUT);
    }
    return null;
  }

  private ErrorCode extractFloecatErrorCode(StatusRuntimeException exception) {
    var statusProto = StatusProto.fromThrowable(exception);
    if (statusProto == null) {
      return null;
    }
    for (com.google.protobuf.Any detail : statusProto.getDetailsList()) {
      if (!detail.is(Error.class)) {
        continue;
      }
      try {
        return detail.unpack(Error.class).getCode();
      } catch (Exception ignored) {
      }
    }
    return null;
  }
}
