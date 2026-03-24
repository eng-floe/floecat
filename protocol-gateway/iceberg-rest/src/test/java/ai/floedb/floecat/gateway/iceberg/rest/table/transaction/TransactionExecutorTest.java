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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.support.GrpcServiceFacade;
import ai.floedb.floecat.transaction.rpc.TransactionState;
import io.grpc.Status;
import jakarta.ws.rs.core.Response;
import java.util.AbstractList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TransactionExecutorTest {
  private final TransactionExecutor executor = new TransactionExecutor();
  private final GrpcServiceFacade grpcClient = Mockito.mock(GrpcServiceFacade.class);
  private final TableCommitOutboxService commitOutboxService =
      Mockito.mock(TableCommitOutboxService.class);
  private final TransactionAborter transactionAborter = Mockito.mock(TransactionAborter.class);
  private final TransactionOutcomePolicy outcomePolicy =
      Mockito.spy(new TransactionOutcomePolicy());
  private final TableGatewaySupport tableSupport = Mockito.mock(TableGatewaySupport.class);

  @BeforeEach
  void setUp() {
    executor.grpcClient = grpcClient;
    executor.commitOutboxService = commitOutboxService;
    executor.transactionAborter = transactionAborter;
    executor.outcomePolicy = outcomePolicy;
  }

  @Test
  void executeAbortsWhenPrepareRequestAssemblyFailsLocally() {
    CommitPlan plan =
        new CommitPlan(
            new AbstractList<>() {
              @Override
              public ai.floedb.floecat.transaction.rpc.TxChange get(int index) {
                throw new IllegalStateException("boom");
              }

              @Override
              public int size() {
                throw new IllegalStateException("boom");
              }
            },
            List.of());
    Response unknown = Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
    when(outcomePolicy.internalStateUnknown()).thenReturn(unknown);

    Response response = executor.execute(openContext(), plan);

    assertSame(unknown, response);
    verify(transactionAborter).abortQuietly("tx-1", "transaction prepare request assembly failed");
    verify(grpcClient, never()).prepareTransaction(any());
  }

  @Test
  void executeDoesNotAbortWhenPrepareTransportFailureIsAmbiguous() {
    when(grpcClient.prepareTransaction(any()))
        .thenThrow(Status.DEADLINE_EXCEEDED.asRuntimeException());

    Response response = executor.execute(openContext(), new CommitPlan(List.of(), List.of()));

    assertEquals(Response.Status.GATEWAY_TIMEOUT.getStatusCode(), response.getStatus());
    verify(transactionAborter, never()).abortQuietly(any(), any());
  }

  private CommitRequestContext openContext() {
    return new CommitRequestContext(
        "acct-1",
        "tx-1",
        "foo",
        "catalog",
        ResourceId.newBuilder().setId("cat").build(),
        "idem",
        "hash",
        123L,
        TransactionState.TS_OPEN,
        tableSupport,
        List.of(),
        true,
        List.of());
  }
}
