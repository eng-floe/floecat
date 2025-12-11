package ai.floedb.floecat.gateway.iceberg.rest.services.namespace;

import ai.floedb.floecat.catalog.rpc.DeleteNamespaceRequest;
import ai.floedb.floecat.gateway.iceberg.rest.resources.support.NamespaceRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.NamespaceClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;

@ApplicationScoped
public class NamespaceDeleteService {
  @Inject NamespaceClient namespaceClient;

  public Response delete(NamespaceRequestContext namespaceContext, Boolean requireEmpty) {
    namespaceClient.deleteNamespace(
        DeleteNamespaceRequest.newBuilder()
            .setNamespaceId(namespaceContext.namespaceId())
            .setRequireEmpty(requireEmpty == null || requireEmpty)
            .build());
    return Response.noContent().build();
  }
}
