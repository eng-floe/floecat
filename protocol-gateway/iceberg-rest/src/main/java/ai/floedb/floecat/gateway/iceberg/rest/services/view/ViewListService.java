package ai.floedb.floecat.gateway.iceberg.rest.services.view;

import ai.floedb.floecat.catalog.rpc.ListViewsRequest;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.PageResponse;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TableIdentifierDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.ViewListResponse;
import ai.floedb.floecat.gateway.iceberg.rest.resources.support.NamespaceRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.resources.support.PageRequestHelper;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.ViewClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.stream.Collectors;

@ApplicationScoped
public class ViewListService {
  @Inject ViewClient viewClient;

  public Response list(
      NamespaceRequestContext namespaceContext, String pageToken, Integer pageSize) {
    ListViewsRequest.Builder req =
        ListViewsRequest.newBuilder().setNamespaceId(namespaceContext.namespaceId());
    PageRequest.Builder page = PageRequestHelper.builder(pageToken, pageSize);
    if (page != null) {
      req.setPage(page);
    }

    var resp = viewClient.listViews(req.build());
    List<TableIdentifierDto> identifiers =
        resp.getViewsList().stream()
            .map(
                v ->
                    new TableIdentifierDto(namespaceContext.namespacePath(), v.getDisplayName()))
            .collect(Collectors.toList());
    return Response.ok(new ViewListResponse(identifiers, flattenPageToken(resp.getPage()))).build();
  }

  private String flattenPageToken(PageResponse page) {
    String token = page.getNextPageToken();
    return token == null || token.isBlank() ? null : token;
  }
}
