package ai.floedb.floecat.gateway.iceberg.rest.services.client;

import ai.floedb.floecat.connector.rpc.ConnectorsGrpc;
import ai.floedb.floecat.connector.rpc.CreateConnectorRequest;
import ai.floedb.floecat.connector.rpc.CreateConnectorResponse;
import ai.floedb.floecat.connector.rpc.DeleteConnectorRequest;
import ai.floedb.floecat.connector.rpc.GetConnectorRequest;
import ai.floedb.floecat.connector.rpc.GetConnectorResponse;
import ai.floedb.floecat.connector.rpc.SyncCaptureRequest;
import ai.floedb.floecat.connector.rpc.SyncCaptureResponse;
import ai.floedb.floecat.connector.rpc.TriggerReconcileRequest;
import ai.floedb.floecat.connector.rpc.TriggerReconcileResponse;
import ai.floedb.floecat.connector.rpc.UpdateConnectorRequest;
import ai.floedb.floecat.connector.rpc.UpdateConnectorResponse;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
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
    return stub().createConnector(request);
  }

  public GetConnectorResponse getConnector(GetConnectorRequest request) {
    return stub().getConnector(request);
  }

  public UpdateConnectorResponse updateConnector(UpdateConnectorRequest request) {
    return stub().updateConnector(request);
  }

  public void deleteConnector(DeleteConnectorRequest request) {
    stub().deleteConnector(request);
  }

  public SyncCaptureResponse syncCapture(SyncCaptureRequest request) {
    return stub().syncCapture(request);
  }

  public TriggerReconcileResponse triggerReconcile(TriggerReconcileRequest request) {
    return stub().triggerReconcile(request);
  }

  private ConnectorsGrpc.ConnectorsBlockingStub stub() {
    return grpc.withHeaders(grpc.raw().connectors());
  }
}
