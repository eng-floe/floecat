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

import ai.floedb.floecat.catalog.rpc.CreateNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.CreateNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.CreateSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.CreateSnapshotResponse;
import ai.floedb.floecat.catalog.rpc.CreateViewRequest;
import ai.floedb.floecat.catalog.rpc.CreateViewResponse;
import ai.floedb.floecat.catalog.rpc.DeleteNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.DeleteNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.DeleteSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.DeleteSnapshotResponse;
import ai.floedb.floecat.catalog.rpc.DeleteTableRequest;
import ai.floedb.floecat.catalog.rpc.DeleteTableResponse;
import ai.floedb.floecat.catalog.rpc.DeleteViewRequest;
import ai.floedb.floecat.catalog.rpc.DeleteViewResponse;
import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.GetNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.GetNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.GetSnapshotResponse;
import ai.floedb.floecat.catalog.rpc.GetTableRequest;
import ai.floedb.floecat.catalog.rpc.GetTableResponse;
import ai.floedb.floecat.catalog.rpc.GetViewRequest;
import ai.floedb.floecat.catalog.rpc.GetViewResponse;
import ai.floedb.floecat.catalog.rpc.ListNamespacesRequest;
import ai.floedb.floecat.catalog.rpc.ListNamespacesResponse;
import ai.floedb.floecat.catalog.rpc.ListSnapshotsRequest;
import ai.floedb.floecat.catalog.rpc.ListSnapshotsResponse;
import ai.floedb.floecat.catalog.rpc.ListTablesRequest;
import ai.floedb.floecat.catalog.rpc.ListTablesResponse;
import ai.floedb.floecat.catalog.rpc.ListViewsRequest;
import ai.floedb.floecat.catalog.rpc.ListViewsResponse;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogRequest;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogResponse;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.ResolveTableRequest;
import ai.floedb.floecat.catalog.rpc.ResolveTableResponse;
import ai.floedb.floecat.catalog.rpc.ResolveViewRequest;
import ai.floedb.floecat.catalog.rpc.ResolveViewResponse;
import ai.floedb.floecat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.catalog.rpc.UpdateNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.UpdateNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.UpdateSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.UpdateSnapshotResponse;
import ai.floedb.floecat.catalog.rpc.UpdateTableRequest;
import ai.floedb.floecat.catalog.rpc.UpdateTableResponse;
import ai.floedb.floecat.catalog.rpc.UpdateViewRequest;
import ai.floedb.floecat.catalog.rpc.UpdateViewResponse;
import ai.floedb.floecat.catalog.rpc.ViewServiceGrpc;
import ai.floedb.floecat.connector.rpc.ConnectorsGrpc;
import ai.floedb.floecat.connector.rpc.CreateConnectorRequest;
import ai.floedb.floecat.connector.rpc.CreateConnectorResponse;
import ai.floedb.floecat.connector.rpc.DeleteConnectorRequest;
import ai.floedb.floecat.connector.rpc.GetConnectorRequest;
import ai.floedb.floecat.connector.rpc.GetConnectorResponse;
import ai.floedb.floecat.connector.rpc.UpdateConnectorRequest;
import ai.floedb.floecat.connector.rpc.UpdateConnectorResponse;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.query.rpc.BeginQueryRequest;
import ai.floedb.floecat.query.rpc.BeginQueryResponse;
import ai.floedb.floecat.query.rpc.DescribeInputsRequest;
import ai.floedb.floecat.query.rpc.DescribeInputsResponse;
import ai.floedb.floecat.query.rpc.EndQueryRequest;
import ai.floedb.floecat.query.rpc.FetchScanBundleRequest;
import ai.floedb.floecat.query.rpc.FetchScanBundleResponse;
import ai.floedb.floecat.query.rpc.GetQueryRequest;
import ai.floedb.floecat.query.rpc.GetQueryResponse;
import ai.floedb.floecat.query.rpc.QueryScanServiceGrpc;
import ai.floedb.floecat.query.rpc.QuerySchemaServiceGrpc;
import ai.floedb.floecat.query.rpc.QueryServiceGrpc;
import ai.floedb.floecat.reconciler.rpc.ReconcileControlGrpc;
import ai.floedb.floecat.reconciler.rpc.StartCaptureRequest;
import ai.floedb.floecat.reconciler.rpc.StartCaptureResponse;
import ai.floedb.floecat.storage.rpc.ResolveStorageAuthorityRequest;
import ai.floedb.floecat.storage.rpc.ResolveStorageAuthorityResponse;
import ai.floedb.floecat.storage.rpc.StorageAuthoritiesGrpc;
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
import ai.floedb.floecat.transaction.rpc.ReserveTransactionTableIdRequest;
import ai.floedb.floecat.transaction.rpc.ReserveTransactionTableIdResponse;
import ai.floedb.floecat.transaction.rpc.TransactionsGrpc;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class GrpcServiceFacade {
  private final GrpcWithHeaders grpc;

  @Inject
  public GrpcServiceFacade(GrpcWithHeaders grpc) {
    this.grpc = grpc;
  }

  public ListNamespacesResponse listNamespaces(ListNamespacesRequest request) {
    return namespaceStub().listNamespaces(request);
  }

  public GetNamespaceResponse getNamespace(GetNamespaceRequest request) {
    return namespaceStub().getNamespace(request);
  }

  public CreateNamespaceResponse createNamespace(CreateNamespaceRequest request) {
    return namespaceStub().createNamespace(request);
  }

  public DeleteNamespaceResponse deleteNamespace(DeleteNamespaceRequest request) {
    return namespaceStub().deleteNamespace(request);
  }

  public UpdateNamespaceResponse updateNamespace(UpdateNamespaceRequest request) {
    return namespaceStub().updateNamespace(request);
  }

  public ListTablesResponse listTables(ListTablesRequest request) {
    return tableStub().listTables(request);
  }

  public GetTableResponse getTable(GetTableRequest request) {
    return tableStub().getTable(request);
  }

  public UpdateTableResponse updateTable(UpdateTableRequest request) {
    return tableStub().updateTable(request);
  }

  public DeleteTableResponse deleteTable(DeleteTableRequest request) {
    return tableStub().deleteTable(request);
  }

  public ListViewsResponse listViews(ListViewsRequest request) {
    return viewStub().listViews(request);
  }

  public GetViewResponse getView(GetViewRequest request) {
    return viewStub().getView(request);
  }

  public CreateViewResponse createView(CreateViewRequest request) {
    return viewStub().createView(request);
  }

  public DeleteViewResponse deleteView(DeleteViewRequest request) {
    return viewStub().deleteView(request);
  }

  public UpdateViewResponse updateView(UpdateViewRequest request) {
    return viewStub().updateView(request);
  }

  public ListSnapshotsResponse listSnapshots(ListSnapshotsRequest request) {
    return snapshotStub().listSnapshots(request);
  }

  public GetSnapshotResponse getSnapshot(GetSnapshotRequest request) {
    return snapshotStub().getSnapshot(request);
  }

  public CreateSnapshotResponse createSnapshot(CreateSnapshotRequest request) {
    return snapshotStub().createSnapshot(request);
  }

  public UpdateSnapshotResponse updateSnapshot(UpdateSnapshotRequest request) {
    return snapshotStub().updateSnapshot(request);
  }

  public DeleteSnapshotResponse deleteSnapshot(DeleteSnapshotRequest request) {
    return snapshotStub().deleteSnapshot(request);
  }

  public CreateConnectorResponse createConnector(CreateConnectorRequest request) {
    return connectorStub().createConnector(request);
  }

  public GetConnectorResponse getConnector(GetConnectorRequest request) {
    return connectorStub().getConnector(request);
  }

  public UpdateConnectorResponse updateConnector(UpdateConnectorRequest request) {
    return connectorStub().updateConnector(request);
  }

  public void deleteConnector(DeleteConnectorRequest request) {
    connectorStub().deleteConnector(request);
  }

  public StartCaptureResponse startCapture(StartCaptureRequest request) {
    return reconcileStub().startCapture(request);
  }

  public BeginQueryResponse beginQuery(BeginQueryRequest request) {
    return queryStub().beginQuery(request);
  }

  public GetQueryResponse getQuery(GetQueryRequest request) {
    return queryStub().getQuery(request);
  }

  public void endQuery(EndQueryRequest request) {
    queryStub().endQuery(request);
  }

  public FetchScanBundleResponse fetchScanBundle(FetchScanBundleRequest request) {
    return queryScanStub().fetchScanBundle(request);
  }

  public DescribeInputsResponse describeInputs(DescribeInputsRequest request) {
    return querySchemaStub().describeInputs(request);
  }

  public BeginTransactionResponse beginTransaction(BeginTransactionRequest request) {
    return transactionStub().beginTransaction(request);
  }

  public PrepareTransactionResponse prepareTransaction(PrepareTransactionRequest request) {
    return transactionStub().prepareTransaction(request);
  }

  public CommitTransactionResponse commitTransaction(CommitTransactionRequest request) {
    return transactionStub().commitTransaction(request);
  }

  public AbortTransactionResponse abortTransaction(AbortTransactionRequest request) {
    return transactionStub().abortTransaction(request);
  }

  public GetTransactionResponse getTransaction(GetTransactionRequest request) {
    return transactionStub().getTransaction(request);
  }

  public ReserveTransactionTableIdResponse reserveTransactionTableId(
      ReserveTransactionTableIdRequest request) {
    return transactionStub().reserveTransactionTableId(request);
  }

  public ResolveCatalogResponse resolveCatalog(ResolveCatalogRequest request) {
    return directoryStub().resolveCatalog(request);
  }

  public ResolveNamespaceResponse resolveNamespace(ResolveNamespaceRequest request) {
    return directoryStub().resolveNamespace(request);
  }

  public ResolveTableResponse resolveTable(ResolveTableRequest request) {
    return directoryStub().resolveTable(request);
  }

  public ResolveViewResponse resolveView(ResolveViewRequest request) {
    return directoryStub().resolveView(request);
  }

  public ResolveStorageAuthorityResponse resolveStorageAuthority(
      ResolveStorageAuthorityRequest request) {
    return storageAuthorityStub().resolveStorageAuthority(request);
  }

  public QueryServiceGrpc.QueryServiceBlockingStub queryStub() {
    return grpc.withHeaders(grpc.raw().query());
  }

  public QueryScanServiceGrpc.QueryScanServiceBlockingStub queryScanStub() {
    return grpc.withHeaders(grpc.raw().queryScan());
  }

  private NamespaceServiceGrpc.NamespaceServiceBlockingStub namespaceStub() {
    return grpc.withHeaders(grpc.raw().namespace());
  }

  private TableServiceGrpc.TableServiceBlockingStub tableStub() {
    return grpc.withHeaders(grpc.raw().table());
  }

  private ViewServiceGrpc.ViewServiceBlockingStub viewStub() {
    return grpc.withHeaders(grpc.raw().view());
  }

  private SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshotStub() {
    return grpc.withHeaders(grpc.raw().snapshot());
  }

  private ConnectorsGrpc.ConnectorsBlockingStub connectorStub() {
    return grpc.withHeaders(grpc.raw().connectors());
  }

  private ReconcileControlGrpc.ReconcileControlBlockingStub reconcileStub() {
    return grpc.withHeaders(grpc.raw().reconcileControl());
  }

  private QuerySchemaServiceGrpc.QuerySchemaServiceBlockingStub querySchemaStub() {
    return grpc.withHeaders(grpc.raw().querySchema());
  }

  private TransactionsGrpc.TransactionsBlockingStub transactionStub() {
    return grpc.withHeaders(grpc.raw().transactions());
  }

  private DirectoryServiceGrpc.DirectoryServiceBlockingStub directoryStub() {
    return grpc.withHeaders(grpc.raw().directory());
  }

  private StorageAuthoritiesGrpc.StorageAuthoritiesBlockingStub storageAuthorityStub() {
    return grpc.withHeaders(grpc.raw().storageAuthorities());
  }
}
