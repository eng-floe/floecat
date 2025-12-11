package ai.floedb.floecat.gateway.iceberg.rest.services.view;

import ai.floedb.floecat.catalog.rpc.DeleteViewRequest;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.ViewRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.ViewClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;

@ApplicationScoped
public class ViewDeleteService {
  @Inject ViewClient viewClient;

  public Response delete(ViewRequestContext viewContext) {
    viewClient.deleteView(DeleteViewRequest.newBuilder().setViewId(viewContext.viewId()).build());
    return Response.noContent().build();
  }
}
