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

package ai.floedb.floecat.gateway.iceberg.rest.table;

import ai.floedb.floecat.common.rpc.Error;
import ai.floedb.floecat.common.rpc.ErrorCode;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.support.GrpcServiceFacade;
import ai.floedb.floecat.gateway.iceberg.rest.support.IcebergErrorResponses;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TransactionApplyService {
  private static final Logger LOG = Logger.getLogger(TransactionApplyService.class);
  private static final int DEFAULT_COMMIT_CONFIRM_MAX_ATTEMPTS = 6;
  private static final long DEFAULT_COMMIT_CONFIRM_INITIAL_SLEEP_MS = 20L;
  private static final long DEFAULT_COMMIT_CONFIRM_MAX_SLEEP_MS = 200L;

  @Inject GrpcServiceFacade grpcClient;
  @Inject TableCommitOutboxService commitOutboxService;

  public Response applyTransaction(
      String txId,
      TransactionCommitService.CommitContext context,
      TableGatewaySupport tableSupport) {
    boolean applied = isCommitAccepted(context.currentState());
    if (!applied) {
      if (context.currentState() == ai.floedb.floecat.transaction.rpc.TransactionState.TS_OPEN) {
        try {
          grpcClient.prepareTransaction(
              ai.floedb.floecat.transaction.rpc.PrepareTransactionRequest.newBuilder()
                  .setTxId(txId)
                  .addAllChanges(context.txChanges())
                  .setIdempotency(
                      ai.floedb.floecat.common.rpc.IdempotencyKey.newBuilder()
                          .setKey(
                              context.idempotencyBase() == null
                                  ? ""
                                  : context.idempotencyBase() + ":prepare"))
                  .build());
        } catch (RuntimeException e) {
          abortTransactionQuietly(txId, "transaction prepare failed");
          if (e instanceof StatusRuntimeException prepareFailure) {
            return mapPreCommitFailure(prepareFailure);
          }
          return preCommitStateUnknown();
        }
      }
      try {
        ai.floedb.floecat.transaction.rpc.CommitTransactionRequest.Builder commitRequest =
            ai.floedb.floecat.transaction.rpc.CommitTransactionRequest.newBuilder().setTxId(txId);
        if (context.currentState()
            != ai.floedb.floecat.transaction.rpc.TransactionState.TS_APPLY_FAILED_RETRYABLE) {
          commitRequest.setIdempotency(
              ai.floedb.floecat.common.rpc.IdempotencyKey.newBuilder()
                  .setKey(
                      context.idempotencyBase() == null
                          ? ""
                          : context.idempotencyBase() + ":commit"));
        }
        var commitResponse = grpcClient.commitTransaction(commitRequest.build());
        var commitState =
            commitResponse != null && commitResponse.hasTransaction()
                ? commitResponse.getTransaction().getState()
                : ai.floedb.floecat.transaction.rpc.TransactionState.TS_UNSPECIFIED;
        if (isCommitAccepted(commitState)) {
          applied = true;
        } else if (isDeterministicFailedState(commitState)) {
          return IcebergErrorResponses.failure(
              "transaction commit did not reach applied state",
              "CommitFailedException",
              Response.Status.CONFLICT);
        } else if (shouldConfirmAmbiguousCommitState(commitState) && waitForAppliedState(txId)) {
          applied = true;
        } else {
          return IcebergErrorResponses.failure(
              "transaction commit did not reach applied state",
              "CommitStateUnknownException",
              Response.Status.SERVICE_UNAVAILABLE);
        }
      } catch (StatusRuntimeException commitFailure) {
        if (isDeterministicCommitFailure(commitFailure)) {
          return IcebergErrorResponses.failure(
              "transaction commit failed", "CommitFailedException", Response.Status.CONFLICT);
        }
        if (isRetryableCommitAbort(commitFailure)) {
          if (waitForAppliedState(txId)) {
            applied = true;
          } else {
            return IcebergErrorResponses.failure(
                "transaction commit failed",
                "CommitStateUnknownException",
                Response.Status.SERVICE_UNAVAILABLE);
          }
        } else {
          Response mapped = mapCommitFailureByStatus(commitFailure.getStatus().getCode());
          return mapped == null ? preCommitStateUnknown() : mapped;
        }
      } catch (RuntimeException commitFailure) {
        return IcebergErrorResponses.failure(
            "transaction commit failed",
            "CommitStateUnknownException",
            Response.Status.INTERNAL_SERVER_ERROR);
      }
    }

    if (!applied) {
      return IcebergErrorResponses.failure(
          "transaction commit did not reach applied state",
          "CommitStateUnknownException",
          Response.Status.SERVICE_UNAVAILABLE);
    }

    if (!context.outboxWorkItems().isEmpty()) {
      try {
        commitOutboxService.processPendingNow(tableSupport, context.outboxWorkItems());
      } catch (RuntimeException e) {
        LOG.warnf(e, "Best-effort outbox processing failed for tx=%s", txId);
      }
    }

    return Response.noContent().build();
  }

  Response mapPreCommitFailure(StatusRuntimeException failure) {
    return mapPreCommitFailureInternal(failure);
  }

  Response preCommitStateUnknown() {
    return preCommitStateUnknownInternal();
  }

  void abortQuietly(String txId, String reason) {
    abortTransactionQuietly(txId, reason);
  }

  private boolean isCommitAccepted(ai.floedb.floecat.transaction.rpc.TransactionState state) {
    return state == ai.floedb.floecat.transaction.rpc.TransactionState.TS_APPLIED;
  }

  private boolean isDeterministicFailedState(
      ai.floedb.floecat.transaction.rpc.TransactionState state) {
    return state == ai.floedb.floecat.transaction.rpc.TransactionState.TS_APPLY_FAILED_CONFLICT
        || state == ai.floedb.floecat.transaction.rpc.TransactionState.TS_ABORTED;
  }

  private boolean isDeterministicCommitFailure(StatusRuntimeException failure) {
    Status.Code code = failure.getStatus().getCode();
    if (code == Status.Code.ABORTED) {
      ErrorCode detailCode = extractFloecatErrorCode(failure);
      if (detailCode == ErrorCode.MC_ABORT_RETRYABLE) {
        return false;
      }
      return true;
    }
    return code == Status.Code.FAILED_PRECONDITION || code == Status.Code.ALREADY_EXISTS;
  }

  private boolean isRetryableCommitAbort(StatusRuntimeException failure) {
    if (failure.getStatus().getCode() != Status.Code.ABORTED) {
      return false;
    }
    return extractFloecatErrorCode(failure) == ErrorCode.MC_ABORT_RETRYABLE;
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

  private boolean waitForAppliedState(String txId) {
    if (txId == null || txId.isBlank()) {
      return false;
    }
    int maxAttempts =
        Math.max(
            1,
            ConfigProvider.getConfig()
                .getOptionalValue("floecat.gateway.commit.confirm.max-attempts", Integer.class)
                .orElse(DEFAULT_COMMIT_CONFIRM_MAX_ATTEMPTS));
    long sleepMillis =
        Math.max(
            0L,
            ConfigProvider.getConfig()
                .getOptionalValue("floecat.gateway.commit.confirm.initial-sleep-ms", Long.class)
                .orElse(DEFAULT_COMMIT_CONFIRM_INITIAL_SLEEP_MS));
    long maxSleepMillis =
        Math.max(
            sleepMillis,
            ConfigProvider.getConfig()
                .getOptionalValue("floecat.gateway.commit.confirm.max-sleep-ms", Long.class)
                .orElse(DEFAULT_COMMIT_CONFIRM_MAX_SLEEP_MS));

    for (int attempt = 0; attempt < maxAttempts; attempt++) {
      try {
        var current =
            grpcClient.getTransaction(
                ai.floedb.floecat.transaction.rpc.GetTransactionRequest.newBuilder()
                    .setTxId(txId)
                    .build());
        if (current != null && current.hasTransaction()) {
          var state = current.getTransaction().getState();
          if (state == ai.floedb.floecat.transaction.rpc.TransactionState.TS_APPLIED) {
            return true;
          }
          if (state == ai.floedb.floecat.transaction.rpc.TransactionState.TS_APPLY_FAILED_CONFLICT
              || state == ai.floedb.floecat.transaction.rpc.TransactionState.TS_ABORTED) {
            return false;
          }
        }
      } catch (RuntimeException ignored) {
        return false;
      }
      if (attempt + 1 < maxAttempts && sleepMillis > 0L) {
        try {
          Thread.sleep(sleepMillis);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          return false;
        }
        sleepMillis = Math.min(maxSleepMillis, Math.max(1L, sleepMillis * 2L));
      }
    }
    return false;
  }

  private boolean shouldConfirmAmbiguousCommitState(
      ai.floedb.floecat.transaction.rpc.TransactionState state) {
    return state == ai.floedb.floecat.transaction.rpc.TransactionState.TS_UNSPECIFIED
        || state == ai.floedb.floecat.transaction.rpc.TransactionState.TS_OPEN
        || state == ai.floedb.floecat.transaction.rpc.TransactionState.TS_PREPARED
        || state == ai.floedb.floecat.transaction.rpc.TransactionState.TS_APPLYING
        || state == ai.floedb.floecat.transaction.rpc.TransactionState.TS_APPLY_FAILED_RETRYABLE;
  }

  private Response mapPreCommitFailureInternal(StatusRuntimeException failure) {
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
        return IcebergErrorResponses.failure(
            "transaction commit failed",
            "CommitStateUnknownException",
            Response.Status.SERVICE_UNAVAILABLE);
      }
      return IcebergErrorResponses.failure(
          "transaction commit failed", "CommitFailedException", Response.Status.CONFLICT);
    }
    Response mapped = mapCommitFailureByStatus(code);
    return mapped == null ? preCommitStateUnknownInternal() : mapped;
  }

  private Response mapCommitFailureByStatus(Status.Code code) {
    if (code == Status.Code.INVALID_ARGUMENT) {
      return IcebergErrorResponses.failure(
          "transaction commit failed", "ValidationException", Response.Status.BAD_REQUEST);
    }
    if (code == Status.Code.UNAVAILABLE) {
      return IcebergErrorResponses.failure(
          "transaction commit failed",
          "CommitStateUnknownException",
          Response.Status.SERVICE_UNAVAILABLE);
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

  private Response preCommitStateUnknownInternal() {
    return IcebergErrorResponses.failure(
        "transaction commit failed",
        "CommitStateUnknownException",
        Response.Status.INTERNAL_SERVER_ERROR);
  }

  private void abortTransactionQuietly(String txId, String reason) {
    if (txId == null || txId.isBlank()) {
      return;
    }
    try {
      grpcClient.abortTransaction(
          ai.floedb.floecat.transaction.rpc.AbortTransactionRequest.newBuilder()
              .setTxId(txId)
              .setReason(reason == null ? "" : reason)
              .build());
    } catch (RuntimeException e) {
      LOG.debugf(e, "Best-effort abort failed for tx=%s", txId);
    }
  }
}
