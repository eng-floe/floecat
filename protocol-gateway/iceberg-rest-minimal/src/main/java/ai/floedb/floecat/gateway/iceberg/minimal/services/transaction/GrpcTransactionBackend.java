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

package ai.floedb.floecat.gateway.iceberg.minimal.services.transaction;

import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.GetTableRequest;
import ai.floedb.floecat.catalog.rpc.GetTableResponse;
import ai.floedb.floecat.catalog.rpc.ListSnapshotsRequest;
import ai.floedb.floecat.catalog.rpc.ListSnapshotsResponse;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogRequest;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogResponse;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.ResolveTableRequest;
import ai.floedb.floecat.catalog.rpc.ResolveTableResponse;
import ai.floedb.floecat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.common.rpc.IdempotencyKey;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.minimal.config.MinimalGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.minimal.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.minimal.resources.common.CatalogResolver;
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
import ai.floedb.floecat.transaction.rpc.TransactionsGrpc;
import ai.floedb.floecat.transaction.rpc.TxChange;
import com.google.protobuf.util.Durations;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;

@ApplicationScoped
public class GrpcTransactionBackend implements TransactionBackend {
  private static final String REQUEST_HASH_PROPERTY = "iceberg.commit.request-hash";

  private final GrpcWithHeaders grpc;
  private final MinimalGatewayConfig config;

  @Inject
  public GrpcTransactionBackend(GrpcWithHeaders grpc, MinimalGatewayConfig config) {
    this.grpc = grpc;
    this.config = config;
  }

  @Override
  public ResolveCatalogResponse resolveCatalog(String prefix) {
    return directoryStub()
        .resolveCatalog(
            ResolveCatalogRequest.newBuilder()
                .setRef(
                    NameRef.newBuilder()
                        .setCatalog(CatalogResolver.resolveCatalog(config, prefix))
                        .build())
                .build());
  }

  @Override
  public ResolveNamespaceResponse resolveNamespace(String prefix, List<String> namespacePath) {
    return directoryStub()
        .resolveNamespace(
            ResolveNamespaceRequest.newBuilder()
                .setRef(
                    NameRef.newBuilder()
                        .setCatalog(CatalogResolver.resolveCatalog(config, prefix))
                        .addAllPath(namespacePath)
                        .build())
                .build());
  }

  @Override
  public ResolveTableResponse resolveTable(
      String prefix, List<String> namespacePath, String tableName) {
    return directoryStub()
        .resolveTable(
            ResolveTableRequest.newBuilder()
                .setRef(
                    NameRef.newBuilder()
                        .setCatalog(CatalogResolver.resolveCatalog(config, prefix))
                        .addAllPath(namespacePath)
                        .setName(tableName)
                        .build())
                .build());
  }

  @Override
  public GetTableResponse getTable(ResourceId tableId) {
    return tableStub().getTable(GetTableRequest.newBuilder().setTableId(tableId).build());
  }

  @Override
  public ListSnapshotsResponse listSnapshots(ResourceId tableId) {
    ListSnapshotsResponse.Builder merged = ListSnapshotsResponse.newBuilder();
    String token = "";
    while (true) {
      ListSnapshotsResponse response =
          snapshotStub()
              .listSnapshots(
                  ListSnapshotsRequest.newBuilder()
                      .setTableId(tableId)
                      .setPage(
                          ai.floedb.floecat.common.rpc.PageRequest.newBuilder()
                              .setPageSize(1000)
                              .setPageToken(token)
                              .build())
                      .build());
      if (response == null) {
        break;
      }
      merged.addAllSnapshots(response.getSnapshotsList());
      if (!response.hasPage()) {
        break;
      }
      String nextToken = response.getPage().getNextPageToken();
      if (nextToken == null || nextToken.isBlank() || nextToken.equals(token)) {
        break;
      }
      token = nextToken;
    }
    return merged.build();
  }

  @Override
  public BeginTransactionResponse beginTransaction(
      String idempotencyKey, String requestHash, Duration ttl) {
    BeginTransactionRequest.Builder request = BeginTransactionRequest.newBuilder();
    if (ttl != null && !ttl.isNegative() && !ttl.isZero()) {
      request.setTtl(Durations.fromMillis(ttl.toMillis()));
    }
    if (idempotencyKey != null && !idempotencyKey.isBlank()) {
      request.setIdempotency(IdempotencyKey.newBuilder().setKey(idempotencyKey));
    }
    if (requestHash != null && !requestHash.isBlank()) {
      request.putProperties(REQUEST_HASH_PROPERTY, requestHash);
    }
    return transactionStub().beginTransaction(request.build());
  }

  @Override
  public GetTransactionResponse getTransaction(String txId) {
    return transactionStub()
        .getTransaction(GetTransactionRequest.newBuilder().setTxId(txId).build());
  }

  @Override
  public PrepareTransactionResponse prepareTransaction(
      String txId, String idempotencyKey, List<TxChange> changes) {
    PrepareTransactionRequest.Builder request =
        PrepareTransactionRequest.newBuilder().setTxId(txId).addAllChanges(changes);
    if (idempotencyKey != null && !idempotencyKey.isBlank()) {
      request.setIdempotency(IdempotencyKey.newBuilder().setKey(idempotencyKey));
    }
    return transactionStub().prepareTransaction(request.build());
  }

  @Override
  public CommitTransactionResponse commitTransaction(String txId, String idempotencyKey) {
    CommitTransactionRequest.Builder request = CommitTransactionRequest.newBuilder().setTxId(txId);
    if (idempotencyKey != null && !idempotencyKey.isBlank()) {
      request.setIdempotency(IdempotencyKey.newBuilder().setKey(idempotencyKey));
    }
    return transactionStub().commitTransaction(request.build());
  }

  @Override
  public AbortTransactionResponse abortTransaction(String txId, String reason) {
    return transactionStub()
        .abortTransaction(
            AbortTransactionRequest.newBuilder().setTxId(txId).setReason(reason).build());
  }

  private DirectoryServiceGrpc.DirectoryServiceBlockingStub directoryStub() {
    return grpc.withHeaders(grpc.raw().directory());
  }

  private TableServiceGrpc.TableServiceBlockingStub tableStub() {
    return grpc.withHeaders(grpc.raw().table());
  }

  private SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshotStub() {
    return grpc.withHeaders(grpc.raw().snapshot());
  }

  private TransactionsGrpc.TransactionsBlockingStub transactionStub() {
    return grpc.withHeaders(grpc.raw().transactions());
  }
}
