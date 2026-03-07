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

import ai.floedb.floecat.connector.rpc.ConnectorsGrpc;
import ai.floedb.floecat.connector.rpc.CreateConnectorRequest;
import ai.floedb.floecat.connector.rpc.CreateConnectorResponse;
import ai.floedb.floecat.connector.rpc.DeleteConnectorRequest;
import ai.floedb.floecat.connector.rpc.GetConnectorRequest;
import ai.floedb.floecat.connector.rpc.GetConnectorResponse;
import ai.floedb.floecat.connector.rpc.UpdateConnectorRequest;
import ai.floedb.floecat.connector.rpc.UpdateConnectorResponse;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.reconciler.rpc.CaptureNowRequest;
import ai.floedb.floecat.reconciler.rpc.CaptureNowResponse;
import ai.floedb.floecat.reconciler.rpc.ReconcileControlGrpc;
import ai.floedb.floecat.reconciler.rpc.StartCaptureRequest;
import ai.floedb.floecat.reconciler.rpc.StartCaptureResponse;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class ConnectorClient {
  private final GrpcWithHeaders grpc;

  @Inject
  public ConnectorClient(GrpcWithHeaders grpc) {
    this.grpc = grpc;
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

  public CaptureNowResponse captureNow(CaptureNowRequest request) {
    return reconcileStub().captureNow(request);
  }

  public StartCaptureResponse startCapture(StartCaptureRequest request) {
    return reconcileStub().startCapture(request);
  }

  private ConnectorsGrpc.ConnectorsBlockingStub connectorStub() {
    return grpc.withHeaders(grpc.raw().connectors());
  }

  private ReconcileControlGrpc.ReconcileControlBlockingStub reconcileStub() {
    return grpc.withHeaders(grpc.raw().reconcileControl());
  }
}
