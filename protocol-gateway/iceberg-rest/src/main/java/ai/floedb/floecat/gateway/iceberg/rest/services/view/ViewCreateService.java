package ai.floedb.floecat.gateway.iceberg.rest.services.view;

import ai.floedb.floecat.catalog.rpc.CreateViewRequest;
import ai.floedb.floecat.catalog.rpc.ViewSpec;
import ai.floedb.floecat.common.rpc.IdempotencyKey;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.ViewRequests;
import ai.floedb.floecat.gateway.iceberg.rest.resources.support.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.resources.support.NamespaceRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.ViewClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.view.ViewMetadataService.MetadataContext;
import ai.floedb.floecat.gateway.iceberg.rest.support.mapper.ViewResponseMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.List;

@ApplicationScoped
public class ViewCreateService {
  @Inject ViewClient viewClient;
  @Inject ViewMetadataService viewMetadataService;

  public Response create(
      NamespaceRequestContext namespaceContext, String idempotencyKey, ViewRequests.Create req) {
    List<String> namespacePath = namespaceContext.namespacePath();
    String viewName = req != null && req.name() != null ? req.name() : "view";

    MetadataContext metadataContext;
    try {
      metadataContext = viewMetadataService.fromCreate(namespacePath, viewName, req);
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }

    ViewSpec.Builder spec =
        ViewSpec.newBuilder()
            .setCatalogId(namespaceContext.catalogId())
            .setNamespaceId(namespaceContext.namespaceId())
            .setDisplayName(viewName);
    spec.setSql(metadataContext.sql());
    try {
      spec.putAllProperties(viewMetadataService.buildPropertyMap(metadataContext));
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }

    CreateViewRequest.Builder request = CreateViewRequest.newBuilder().setSpec(spec);
    if (idempotencyKey != null && !idempotencyKey.isBlank()) {
      request.setIdempotency(IdempotencyKey.newBuilder().setKey(idempotencyKey).build());
    }
    var created = viewClient.createView(request.build());
    MetadataContext responseContext =
        viewMetadataService.fromView(namespacePath, viewName, created.getView());
    return Response.ok(
            ViewResponseMapper.toLoadResult(
                namespaceContext.namespace(),
                viewName,
                created.getView(),
                responseContext.metadata()))
        .build();
  }
}
