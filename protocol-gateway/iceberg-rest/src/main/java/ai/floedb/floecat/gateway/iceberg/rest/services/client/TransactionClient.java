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

package ai.floedb.floecat.gateway.iceberg.rest.services.client;

import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.transaction.rpc.AbortTransactionRequest;
import ai.floedb.floecat.transaction.rpc.AbortTransactionResponse;
import ai.floedb.floecat.transaction.rpc.BeginTransactionRequest;
import ai.floedb.floecat.transaction.rpc.BeginTransactionResponse;
import ai.floedb.floecat.transaction.rpc.CommitTransactionRequest;
import ai.floedb.floecat.transaction.rpc.CommitTransactionResponse;
import ai.floedb.floecat.transaction.rpc.GetTransactionRequest;
import ai.floedb.floecat.transaction.rpc.GetTransactionResponse;
import ai.floedb.floecat.transaction.rpc.PrepareTransactionRequest;
import ai.floedb.floecat.transaction.rpc.PrepareTransactionResponse;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class TransactionClient {
  private final GrpcWithHeaders grpc;

  @Inject
  public TransactionClient(GrpcWithHeaders grpc) {
    this.grpc = grpc;
  }

  public BeginTransactionResponse beginTransaction(BeginTransactionRequest request) {
    return grpc.withHeaders(grpc.raw().transactions()).beginTransaction(request);
  }

  public PrepareTransactionResponse prepareTransaction(PrepareTransactionRequest request) {
    return grpc.withHeaders(grpc.raw().transactions()).prepareTransaction(request);
  }

  public CommitTransactionResponse commitTransaction(CommitTransactionRequest request) {
    return grpc.withHeaders(grpc.raw().transactions()).commitTransaction(request);
  }

  public AbortTransactionResponse abortTransaction(AbortTransactionRequest request) {
    return grpc.withHeaders(grpc.raw().transactions()).abortTransaction(request);
  }

  public GetTransactionResponse getTransaction(GetTransactionRequest request) {
    return grpc.withHeaders(grpc.raw().transactions()).getTransaction(request);
  }
}
