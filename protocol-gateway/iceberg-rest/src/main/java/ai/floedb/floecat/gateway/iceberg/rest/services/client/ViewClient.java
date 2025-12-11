package ai.floedb.floecat.gateway.iceberg.rest.services.client;

import ai.floedb.floecat.catalog.rpc.CreateViewRequest;
import ai.floedb.floecat.catalog.rpc.CreateViewResponse;
import ai.floedb.floecat.catalog.rpc.DeleteViewRequest;
import ai.floedb.floecat.catalog.rpc.DeleteViewResponse;
import ai.floedb.floecat.catalog.rpc.GetViewRequest;
import ai.floedb.floecat.catalog.rpc.GetViewResponse;
import ai.floedb.floecat.catalog.rpc.ListViewsRequest;
import ai.floedb.floecat.catalog.rpc.ListViewsResponse;
import ai.floedb.floecat.catalog.rpc.UpdateViewRequest;
import ai.floedb.floecat.catalog.rpc.UpdateViewResponse;
import ai.floedb.floecat.catalog.rpc.ViewServiceGrpc;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class ViewClient {
  private final GrpcWithHeaders grpc;

  @Inject
  public ViewClient(GrpcWithHeaders grpc) {
    this.grpc = grpc;
  }

  public ListViewsResponse listViews(ListViewsRequest request) {
    return stub().listViews(request);
  }

  public GetViewResponse getView(GetViewRequest request) {
    return stub().getView(request);
  }

  public CreateViewResponse createView(CreateViewRequest request) {
    return stub().createView(request);
  }

  public DeleteViewResponse deleteView(DeleteViewRequest request) {
    return stub().deleteView(request);
  }

  public UpdateViewResponse updateView(UpdateViewRequest request) {
    return stub().updateView(request);
  }

  private ViewServiceGrpc.ViewServiceBlockingStub stub() {
    return grpc.withHeaders(grpc.raw().view());
  }
}
