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
