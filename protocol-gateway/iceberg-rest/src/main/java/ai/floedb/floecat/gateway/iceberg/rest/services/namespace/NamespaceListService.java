package ai.floedb.floecat.gateway.iceberg.rest.services.namespace;

import ai.floedb.floecat.catalog.rpc.ListNamespacesRequest;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.NamespaceListResponse;
import ai.floedb.floecat.gateway.iceberg.rest.resources.support.CatalogRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.resources.support.NamespaceRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.resources.support.PageRequestHelper;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.NamespaceClient;
import ai.floedb.floecat.gateway.iceberg.rest.support.mapper.NamespaceResponseMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.stream.Collectors;

@ApplicationScoped
public class NamespaceListService {
  @Inject NamespaceClient namespaceClient;

  public Response list(ListCommand command) {
    ListNamespacesRequest.Builder req = ListNamespacesRequest.newBuilder();
    NamespaceRequestContext parentContext = command.parentNamespace();
    if (parentContext != null) {
      req.setNamespaceId(parentContext.namespaceId());
    } else {
      req.setCatalogId(command.catalogContext().catalogId());
    }
    if (Boolean.TRUE.equals(command.childrenOnly())) {
      req.setChildrenOnly(true);
    }
    if (Boolean.TRUE.equals(command.recursive())) {
      req.setRecursive(true);
    }
    if (command.namePrefix() != null) {
      req.setNamePrefix(command.namePrefix());
    }
    PageRequest.Builder page = PageRequestHelper.builder(command.pageToken(), command.pageSize());
    if (page != null) {
      req.setPage(page);
    }

    var resp = namespaceClient.listNamespaces(req.build());
    List<List<String>> namespaces =
        resp.getNamespacesList().stream()
            .map(NamespaceResponseMapper::toPath)
            .collect(Collectors.toList());
    String nextToken =
        resp.hasPage() ? normalizeToken(resp.getPage().getNextPageToken()) : null;
    return Response.ok(new NamespaceListResponse(namespaces, nextToken)).build();
  }

  public record ListCommand(
      CatalogRequestContext catalogContext,
      NamespaceRequestContext parentNamespace,
      Boolean childrenOnly,
      Boolean recursive,
      String namePrefix,
      String pageToken,
      Integer pageSize) {}

  private String normalizeToken(String token) {
    return token == null || token.isBlank() ? null : token;
  }
}
