package ai.floedb.floecat.gateway.iceberg.rest.services.namespace;

import ai.floedb.floecat.catalog.rpc.GetNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.common.NamespaceResponseMapper;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.NamespaceRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.NamespaceClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;

@ApplicationScoped
public class NamespaceInfoService {
  @Inject NamespaceClient namespaceClient;

  public Response get(NamespaceRequestContext namespaceContext) {
    Namespace namespace = load(namespaceContext.namespaceId());
    return Response.ok(NamespaceResponseMapper.toInfo(namespace)).build();
  }

  public Response exists(NamespaceRequestContext namespaceContext) {
    load(namespaceContext.namespaceId());
    return Response.noContent().build();
  }

  private Namespace load(ResourceId namespaceId) {
    return namespaceClient
        .getNamespace(GetNamespaceRequest.newBuilder().setNamespaceId(namespaceId).build())
        .getNamespace();
  }
}
