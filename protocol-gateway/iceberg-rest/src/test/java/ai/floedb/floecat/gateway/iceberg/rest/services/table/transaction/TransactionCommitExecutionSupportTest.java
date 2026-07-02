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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.Error;
import ai.floedb.floecat.common.rpc.ErrorCode;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.GrpcServiceFacade;
import ai.floedb.floecat.transaction.rpc.BeginTransactionResponse;
import ai.floedb.floecat.transaction.rpc.GetTransactionResponse;
import ai.floedb.floecat.transaction.rpc.Transaction;
import ai.floedb.floecat.transaction.rpc.TransactionState;
import ai.floedb.floecat.transaction.rpc.TxChange;
import com.google.protobuf.Any;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import jakarta.ws.rs.core.Response;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TransactionCommitExecutionSupportTest {

  @Test
  void applyMarksPreparePreconditionFailureAsRetryableConflict() {
    TransactionCommitExecutionSupport support = new TransactionCommitExecutionSupport();
    support.grpcClient = Mockito.mock(GrpcServiceFacade.class);

    StatusRuntimeException prepareFailure =
        StatusProto.toStatusRuntimeException(
            com.google.rpc.Status.newBuilder()
                .setCode(com.google.rpc.Code.FAILED_PRECONDITION_VALUE)
                .setMessage("precondition failed")
                .addDetails(
                    Any.pack(Error.newBuilder().setCode(ErrorCode.MC_PRECONDITION_FAILED).build()))
                .build());
    when(support.grpcClient.prepareTransaction(any())).thenThrow(prepareFailure);

    Response response =
        support.apply(
            new TransactionCommitExecutionSupport.OpenTransaction(
                "tx-1", TransactionState.TS_OPEN, null, "tx-1"),
            List.of(
                TxChange.newBuilder()
                    .setTargetPointerKey("/accounts/acct/tables/by-id/tbl-1")
                    .setIntendedBlobUri("/accounts/acct/objects/blob-1")
                    .build()));

    assertEquals(Response.Status.CONFLICT.getStatusCode(), response.getStatus());
    assertTrue(support.isRetryableConflict(response));
  }

  @Test
  void openTransactionDerivesPerAttemptBeginIdempotencyFromClientKey() {
    TransactionCommitExecutionSupport support = new TransactionCommitExecutionSupport();
    support.grpcClient = Mockito.mock(GrpcServiceFacade.class);

    when(support.grpcClient.beginTransaction(
            argThat(
                request ->
                    request != null
                        && request.hasIdempotency()
                        && "client-key:tx-attempt-1".equals(request.getIdempotency().getKey()))))
        .thenReturn(
            BeginTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-2")
                        .putProperties(TransactionCommitService.TX_REQUEST_HASH_PROPERTY, "hash-1")
                        .build())
                .build());
    when(support.grpcClient.getTransaction(any()))
        .thenReturn(
            GetTransactionResponse.newBuilder()
                .setTransaction(
                    Transaction.newBuilder()
                        .setTxId("tx-2")
                        .setState(TransactionState.TS_OPEN)
                        .putProperties(TransactionCommitService.TX_REQUEST_HASH_PROPERTY, "hash-1")
                        .build())
                .build());

    var result = support.openTransaction("client-key", true, "examples", "hash-1", "tx-attempt-1");

    assertEquals(null, result.error());
    assertEquals("tx-2", result.transaction().txId());
    assertEquals("tx-2", result.transaction().idempotencyBase());
  }
}
