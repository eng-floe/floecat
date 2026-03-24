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

import ai.floedb.floecat.gateway.iceberg.rest.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.support.GrpcServiceFacade;
import ai.floedb.floecat.gateway.iceberg.rest.support.IcebergErrorResponses;
import ai.floedb.floecat.transaction.rpc.TransactionState;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TransactionExecutor {
  private static final Logger LOG = Logger.getLogger(TransactionExecutor.class);
  private static final int DEFAULT_COMMIT_CONFIRM_MAX_ATTEMPTS = 6;
  private static final long DEFAULT_COMMIT_CONFIRM_INITIAL_SLEEP_MS = 20L;
  private static final long DEFAULT_COMMIT_CONFIRM_MAX_SLEEP_MS = 200L;

  @Inject GrpcServiceFacade grpcClient;
  @Inject TableCommitOutboxService commitOutboxService;
  @Inject TransactionAborter transactionAborter;
  @Inject TransactionOutcomePolicy outcomePolicy;

  public Response execute(
      CommitRequestContext context, CommitPlan plan, TableGatewaySupport tableSupport) {
    boolean applied = outcomePolicy.isApplied(context.currentState());
    if (!applied) {
      Response prepareFailure = prepare(context, plan);
      if (prepareFailure != null) {
        return prepareFailure;
      }
      Response commitFailure = commit(context);
      if (commitFailure != null) {
        return commitFailure;
      }
      applied = true;
    }

    if (!applied) {
      return IcebergErrorResponses.failure(
          "transaction commit did not reach applied state",
          "CommitStateUnknownException",
          Response.Status.SERVICE_UNAVAILABLE);
    }

    if (!plan.outboxItems().isEmpty()) {
      try {
        commitOutboxService.processPendingNow(tableSupport, plan.outboxItems());
      } catch (RuntimeException e) {
        LOG.warnf(e, "Best-effort outbox processing failed for tx=%s", context.txId());
      }
    }

    return Response.noContent().build();
  }

  public Response mapPrepareFailure(StatusRuntimeException failure) {
    return outcomePolicy.mapPrepareFailure(failure);
  }

  public Response stateUnknown() {
    return outcomePolicy.internalStateUnknown();
  }

  private Response prepare(CommitRequestContext context, CommitPlan plan) {
    if (context.currentState() != TransactionState.TS_OPEN) {
      return null;
    }
    try {
      grpcClient.prepareTransaction(
          ai.floedb.floecat.transaction.rpc.PrepareTransactionRequest.newBuilder()
              .setTxId(context.txId())
              .addAllChanges(plan.txChanges())
              .setIdempotency(
                  ai.floedb.floecat.common.rpc.IdempotencyKey.newBuilder()
                      .setKey(
                          context.idempotencyBase() == null
                              ? ""
                              : context.idempotencyBase() + ":prepare"))
              .build());
      return null;
    } catch (RuntimeException e) {
      transactionAborter.abortQuietly(context.txId(), "transaction prepare failed");
      if (e instanceof StatusRuntimeException prepareFailure) {
        return outcomePolicy.mapPrepareFailure(prepareFailure);
      }
      return outcomePolicy.internalStateUnknown();
    }
  }

  private Response commit(CommitRequestContext context) {
    try {
      ai.floedb.floecat.transaction.rpc.CommitTransactionRequest.Builder commitRequest =
          ai.floedb.floecat.transaction.rpc.CommitTransactionRequest.newBuilder()
              .setTxId(context.txId());
      if (context.currentState() != TransactionState.TS_APPLY_FAILED_RETRYABLE) {
        commitRequest.setIdempotency(
            ai.floedb.floecat.common.rpc.IdempotencyKey.newBuilder()
                .setKey(
                    context.idempotencyBase() == null
                        ? ""
                        : context.idempotencyBase() + ":commit"));
      }
      var commitResponse = grpcClient.commitTransaction(commitRequest.build());
      TransactionState commitState =
          commitResponse != null && commitResponse.hasTransaction()
              ? commitResponse.getTransaction().getState()
              : TransactionState.TS_UNSPECIFIED;
      if (outcomePolicy.isApplied(commitState)) {
        return null;
      }
      if (outcomePolicy.isDeterministicFailedState(commitState)) {
        return IcebergErrorResponses.failure(
            "transaction commit did not reach applied state",
            "CommitFailedException",
            Response.Status.CONFLICT);
      }
      if (outcomePolicy.shouldConfirmAmbiguousCommitState(commitState)
          && waitForAppliedState(context.txId())) {
        return null;
      }
      return outcomePolicy.stateUnknown();
    } catch (StatusRuntimeException commitFailure) {
      if (outcomePolicy.isRetryableCommitAbort(commitFailure)) {
        return waitForAppliedState(context.txId()) ? null : outcomePolicy.stateUnknown();
      }
      return outcomePolicy.mapCommitFailure(commitFailure);
    } catch (RuntimeException commitFailure) {
      return outcomePolicy.internalStateUnknown();
    }
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
          TransactionState state = current.getTransaction().getState();
          if (outcomePolicy.isApplied(state)) {
            return true;
          }
          if (outcomePolicy.isDeterministicFailedState(state)) {
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
}
