package ai.floedb.floecat.gateway.iceberg.rest.resources.view;

import ai.floedb.floecat.gateway.iceberg.rest.api.request.ViewRequests;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.NamespaceRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.RequestContextFactory;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.ViewRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.view.ViewCommitService;
import ai.floedb.floecat.gateway.iceberg.rest.services.view.ViewCreateService;
import ai.floedb.floecat.gateway.iceberg.rest.services.view.ViewDeleteService;
import ai.floedb.floecat.gateway.iceberg.rest.services.view.ViewListService;
import ai.floedb.floecat.gateway.iceberg.rest.services.view.ViewLoadService;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.List;

@Path("/v1/{prefix}/namespaces/{namespace}/views")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class ViewResource {
  @Inject RequestContextFactory requestContextFactory;
  @Inject ViewListService viewListService;
  @Inject ViewCreateService viewCreateService;
  @Inject ViewLoadService viewLoadService;
  @Inject ViewDeleteService viewDeleteService;
  @Inject ViewCommitService viewCommitService;

  @GET
  public Response list(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @QueryParam("pageToken") String pageToken,
      @QueryParam("pageSize") Integer pageSize) {
    NamespaceRequestContext namespaceContext = requestContextFactory.namespace(prefix, namespace);
    return viewListService.list(namespaceContext, pageToken, pageSize);
  }

  @POST
  public Response create(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      ViewRequests.Create req) {
    NamespaceRequestContext namespaceContext = requestContextFactory.namespace(prefix, namespace);
    return viewCreateService.create(namespaceContext, idempotencyKey, req);
  }

  @Path("/{view}")
  @GET
  public Response get(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("view") String view) {
    ViewRequestContext viewContext = requestContextFactory.view(prefix, namespace, view);
    List<String> namespacePath = viewContext.namespacePath();
    return viewLoadService.get(viewContext, namespace, view, namespacePath);
  }

  @Path("/{view}")
  @HEAD
  public Response exists(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("view") String view) {
    ViewRequestContext viewContext = requestContextFactory.view(prefix, namespace, view);
    return viewLoadService.exists(viewContext);
  }

  @Path("/{view}")
  @DELETE
  public Response delete(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("view") String view) {
    ViewRequestContext viewContext = requestContextFactory.view(prefix, namespace, view);
    return viewDeleteService.delete(viewContext);
  }

  @Path("/{view}")
  @POST
  public Response commit(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("view") String view,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      ViewRequests.Commit req) {
    ViewRequestContext viewContext = requestContextFactory.view(prefix, namespace, view);
    List<String> namespacePath = viewContext.namespacePath();
    return viewCommitService.commit(viewContext, namespacePath, namespace, view, req);
  }
}
