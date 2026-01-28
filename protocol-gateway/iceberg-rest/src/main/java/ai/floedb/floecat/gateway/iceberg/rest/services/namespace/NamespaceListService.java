/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.gateway.iceberg.rest.services.namespace;

import ai.floedb.floecat.catalog.rpc.ListNamespacesRequest;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.NamespaceListResponse;
import ai.floedb.floecat.gateway.iceberg.rest.common.NamespaceResponseMapper;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.CatalogRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.NamespaceRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.PageRequestHelper;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.NamespaceClient;
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
    PageRequest.Builder page = PageRequestHelper.builder(command.pageToken(), command.pageSize());
    if (page != null) {
      req.setPage(page);
    }

    var resp = namespaceClient.listNamespaces(req.build());
    List<List<String>> namespaces =
        resp.getNamespacesList().stream()
            .map(NamespaceResponseMapper::toPath)
            .collect(Collectors.toList());
    String nextToken = resp.hasPage() ? normalizeToken(resp.getPage().getNextPageToken()) : null;
    return Response.ok(new NamespaceListResponse(namespaces, nextToken)).build();
  }

  public record ListCommand(
      CatalogRequestContext catalogContext,
      NamespaceRequestContext parentNamespace,
      String pageToken,
      Integer pageSize) {}

  private String normalizeToken(String token) {
    return token == null || token.isBlank() ? null : token;
  }
}
