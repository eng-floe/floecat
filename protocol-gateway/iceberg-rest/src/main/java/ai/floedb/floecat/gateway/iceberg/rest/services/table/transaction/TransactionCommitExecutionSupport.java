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

package ai.floedb.floecat.gateway.iceberg.rest.services.table.transaction;

import ai.floedb.floecat.common.rpc.Error;
import ai.floedb.floecat.common.rpc.ErrorCode;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.GrpcServiceFacade;
import ai.floedb.floecat.transaction.rpc.GetTransactionRequest;
import ai.floedb.floecat.transaction.rpc.TransactionState;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Set;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TransactionCommitExecutionSupport {
  private static final int DEFAULT_COMMIT_CONFIRM_MAX_ATTEMPTS = 6;
  private static final long DEFAULT_COMMIT_CONFIRM_INITIAL_SLEEP_MS = 20L;
  private static final long DEFAULT_COMMIT_CONFIRM_MAX_SLEEP_MS = 200L;
  private static final Set<TransactionState> CONFIRMABLE_COMMIT_STATES =
      Set.of(
          TransactionState.TS_UNSPECIFIED,
          TransactionState.TS_OPEN,
          TransactionState.TS_PREPARED,
          TransactionState.TS_APPLYING,
          TransactionState.TS_APPLY_FAILED_RETRYABLE);
  private static final Logger LOG = Logger.getLogger(TransactionCommitExecutionSupport.class);

  @Inject GrpcServiceFacade grpcClient;

  record OpenTransaction(
      String txId,
      TransactionState currentState,
      ai.floedb.floecat.transaction.rpc.GetTransactionResponse currentTxn,
      String idempotencyBase) {}

  record OpenTransactionResult(OpenTransaction transaction, Response error) {}

  OpenTransactionResult openTransaction(
      String idempotencyKey,
      boolean allowGeneratedBeginIdempotency,
      String catalogName,
      String requestHash) {
    String beginIdempotency =
        firstNonBlank(
            idempotencyKey,
            allowGeneratedBeginIdempotency ? "req:" + catalogName + ":" + requestHash : null);
    ai.floedb.floecat.transaction.rpc.BeginTransactionResponse begin;
    try {
      begin =
          grpcClient.beginTransaction(
              ai.floedb.floecat.transaction.rpc.BeginTransactionRequest.newBuilder()
                  .setIdempotency(
                      ai.floedb.floecat.common.rpc.IdempotencyKey.newBuilder()
                          .setKey(beginIdempotency == null ? "" : beginIdempotency))
                  .putProperties(TransactionCommitService.TX_REQUEST_HASH_PROPERTY, requestHash)
                  .build());
    } catch (StatusRuntimeException beginFailure) {
      return new OpenTransactionResult(null, mapPrepareFailure(beginFailure));
    } catch (RuntimeException beginFailure) {
      return new OpenTransactionResult(null, stateUnknown());
    }

    String txId = begin.getTransaction().getTxId();
    if (txId == null || txId.isBlank()) {
      return new OpenTransactionResult(
          null,
          IcebergErrorResponses.failure(
              "Failed to begin transaction",
              "InternalServerError",
              Response.Status.INTERNAL_SERVER_ERROR));
    }

    ai.floedb.floecat.transaction.rpc.GetTransactionResponse currentTxn;
    TransactionState currentState;
    try {
      currentTxn =
          grpcClient.getTransaction(GetTransactionRequest.newBuilder().setTxId(txId).build());
      currentState =
          currentTxn != null && currentTxn.hasTransaction()
              ? currentTxn.getTransaction().getState()
              : TransactionState.TS_UNSPECIFIED;
    } catch (StatusRuntimeException e) {
      abortTransactionQuietly(txId, "failed to load transaction");
      return new OpenTransactionResult(null, mapPrepareFailure(e));
    } catch (RuntimeException e) {
      abortTransactionQuietly(txId, "failed to load transaction");
      return new OpenTransactionResult(null, stateUnknown());
    }

    if (currentTxn != null && currentTxn.hasTransaction()) {
      String existingRequestHash =
          currentTxn
              .getTransaction()
              .getPropertiesMap()
              .get(TransactionCommitService.TX_REQUEST_HASH_PROPERTY);
      if (existingRequestHash != null
          && !existingRequestHash.isBlank()
          && !existingRequestHash.equals(requestHash)) {
        abortIfOpen(currentState, txId, "transaction request-hash mismatch");
        return new OpenTransactionResult(
            null,
            IcebergErrorResponses.failure(
                "transaction request does not match existing transaction payload",
                "CommitFailedException",
                Response.Status.CONFLICT));
      }
    }

    if (currentState == TransactionState.TS_APPLY_FAILED_CONFLICT) {
      return new OpenTransactionResult(
          null,
          IcebergErrorResponses.failure(
              "transaction commit failed", "CommitFailedException", Response.Status.CONFLICT));
    }

    return new OpenTransactionResult(
        new OpenTransaction(txId, currentState, currentTxn, firstNonBlank(idempotencyKey, txId)),
        null);
  }

  Response apply(
      OpenTransaction transaction, List<ai.floedb.floecat.transaction.rpc.TxChange> txChanges) {
    String txId = transaction.txId();
    String idempotencyBase = transaction.idempotencyBase();
    TransactionState currentState = transaction.currentState();
    boolean applied = isCommitAccepted(currentState);
    if (!applied) {
      if (currentState == TransactionState.TS_OPEN) {
        try {
          grpcClient.prepareTransaction(
              ai.floedb.floecat.transaction.rpc.PrepareTransactionRequest.newBuilder()
                  .setTxId(txId)
                  .addAllChanges(txChanges)
                  .setIdempotency(
                      ai.floedb.floecat.common.rpc.IdempotencyKey.newBuilder()
                          .setKey(idempotencyBase == null ? "" : idempotencyBase + ":prepare"))
                  .build());
        } catch (RuntimeException e) {
          abortTransactionQuietly(txId, "transaction prepare failed");
          if (e instanceof StatusRuntimeException prepareFailure) {
            return mapPrepareFailure(prepareFailure);
          }
          return stateUnknown();
        }
      }
      try {
        ai.floedb.floecat.transaction.rpc.CommitTransactionRequest.Builder commitRequest =
            ai.floedb.floecat.transaction.rpc.CommitTransactionRequest.newBuilder().setTxId(txId);
        if (currentState != TransactionState.TS_APPLY_FAILED_RETRYABLE) {
          commitRequest.setIdempotency(
              ai.floedb.floecat.common.rpc.IdempotencyKey.newBuilder()
                  .setKey(idempotencyBase == null ? "" : idempotencyBase + ":commit"));
        }
        var commitResponse = grpcClient.commitTransaction(commitRequest.build());
        TransactionState commitState =
            commitResponse != null && commitResponse.hasTransaction()
                ? commitResponse.getTransaction().getState()
                : TransactionState.TS_UNSPECIFIED;
        if (isCommitAccepted(commitState)) {
          applied = true;
        } else {
          if (isDeterministicFailedState(commitState)) {
            return IcebergErrorResponses.failure(
                "transaction commit did not reach applied state",
                "CommitFailedException",
                Response.Status.CONFLICT);
          }
          if (shouldConfirmAmbiguousCommitState(commitState) && waitForAppliedState(txId)) {
            applied = true;
          } else {
            return IcebergErrorResponses.failure(
                "transaction commit did not reach applied state",
                "CommitStateUnknownException",
                Response.Status.SERVICE_UNAVAILABLE);
          }
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
          return mapped == null ? stateUnknown() : mapped;
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
    return Response.noContent().build();
  }

  Response mapPrepareFailure(StatusRuntimeException failure) {
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
    return mapped == null ? stateUnknown() : mapped;
  }

  Response stateUnknown() {
    return IcebergErrorResponses.failure(
        "transaction commit failed",
        "CommitStateUnknownException",
        Response.Status.INTERNAL_SERVER_ERROR);
  }

  void abortIfOpen(TransactionState currentState, String txId, String reason) {
    if (currentState == TransactionState.TS_OPEN) {
      abortTransactionQuietly(txId, reason);
    }
  }

  private boolean isCommitAccepted(TransactionState state) {
    return state == TransactionState.TS_APPLIED;
  }

  private boolean isDeterministicFailedState(TransactionState state) {
    return state == TransactionState.TS_APPLY_FAILED_CONFLICT
        || state == TransactionState.TS_ABORTED;
  }

  private boolean isDeterministicCommitFailure(StatusRuntimeException failure) {
    Status.Code code = failure.getStatus().getCode();
    if (code == Status.Code.ABORTED) {
      ErrorCode detailCode = extractFloecatErrorCode(failure);
      if (detailCode == ErrorCode.MC_ABORT_RETRYABLE) {
        return false;
      }
      if (detailCode == ErrorCode.MC_CONFLICT || detailCode == ErrorCode.MC_PRECONDITION_FAILED) {
        return true;
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
        // Continue scanning details.
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
            grpcClient.getTransaction(GetTransactionRequest.newBuilder().setTxId(txId).build());
        if (current != null && current.hasTransaction()) {
          TransactionState state = current.getTransaction().getState();
          if (state == TransactionState.TS_APPLIED) {
            return true;
          }
          if (state == TransactionState.TS_APPLY_FAILED_CONFLICT
              || state == TransactionState.TS_ABORTED) {
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

  private boolean shouldConfirmAmbiguousCommitState(TransactionState state) {
    return CONFIRMABLE_COMMIT_STATES.contains(state);
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

  private static String firstNonBlank(String... values) {
    if (values == null) {
      return null;
    }
    for (String value : values) {
      if (value != null && !value.isBlank()) {
        return value;
      }
    }
    return null;
  }
}
