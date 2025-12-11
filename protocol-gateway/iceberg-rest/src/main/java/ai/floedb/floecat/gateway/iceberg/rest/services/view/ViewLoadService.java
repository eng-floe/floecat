package ai.floedb.floecat.gateway.iceberg.rest.services.view;

import ai.floedb.floecat.catalog.rpc.GetViewRequest;
import ai.floedb.floecat.gateway.iceberg.rest.resources.support.ViewRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.ViewClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.view.ViewMetadataService.MetadataContext;
import ai.floedb.floecat.gateway.iceberg.rest.support.mapper.ViewResponseMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.List;

@ApplicationScoped
public class ViewLoadService {
  @Inject ViewClient viewClient;
  @Inject ViewMetadataService viewMetadataService;

  public Response get(
      ViewRequestContext viewContext,
      String namespace,
      String viewName,
      List<String> namespacePath) {
    var resp =
        viewClient.getView(GetViewRequest.newBuilder().setViewId(viewContext.viewId()).build());
    MetadataContext context = viewMetadataService.fromView(namespacePath, viewName, resp.getView());
    return Response.ok(
            ViewResponseMapper.toLoadResult(
                namespace, viewName, resp.getView(), context.metadata()))
        .build();
  }

  public Response exists(ViewRequestContext viewContext) {
    viewClient.getView(GetViewRequest.newBuilder().setViewId(viewContext.viewId()).build());
    return Response.noContent().build();
  }
}
