package ai.floedb.floecat.gateway.iceberg.rest.services.namespace;

import ai.floedb.floecat.catalog.rpc.CreateNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.NamespaceSpec;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.NamespaceRequests;
import ai.floedb.floecat.gateway.iceberg.rest.common.NamespaceResponseMapper;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.CatalogRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.NamespaceClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.resolution.NamespacePaths;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import java.util.List;

@ApplicationScoped
public class NamespaceCreateService {
  @Inject NamespaceClient namespaceClient;

  public Response create(
      CatalogRequestContext catalogContext, UriInfo uriInfo, NamespaceRequests.Create req) {
    if (req == null || req.namespace() == null || req.namespace().isEmpty()) {
      return IcebergErrorResponses.validation("Namespace name must be provided");
    }
    List<String> path = req.namespace();
    if (path.isEmpty() || path.stream().anyMatch(part -> part == null || part.isBlank())) {
      return IcebergErrorResponses.validation("Namespace name must be provided");
    }

    final String displayName = path.get(path.size() - 1);
    final List<String> parents = path.subList(0, path.size() - 1);
    NamespaceSpec.Builder spec =
        NamespaceSpec.newBuilder()
            .setCatalogId(catalogContext.catalogId())
            .addAllPath(parents)
            .setDisplayName(displayName);

    if (req.description() != null) {
      spec.setDescription(req.description());
    }
    if (req.properties() != null) {
      spec.putAllProperties(req.properties());
    }
    if (req.policyRef() != null) {
      spec.setPolicyRef(req.policyRef());
    }

    Namespace created =
        namespaceClient
            .createNamespace(CreateNamespaceRequest.newBuilder().setSpec(spec).build())
            .getNamespace();

    List<String> createdPath = NamespaceResponseMapper.toPath(created);
    String locationNs = NamespacePaths.encode(createdPath);
    return Response.created(uriInfo.getAbsolutePathBuilder().path(locationNs).build())
        .entity(NamespaceResponseMapper.toInfo(created))
        .build();
  }
}
