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

package ai.floedb.floecat.gateway.iceberg.rest.resources.namespace;

import ai.floedb.floecat.gateway.iceberg.rest.api.request.NamespacePropertiesRequest;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.NamespaceRequests;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.CatalogRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.NamespaceRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.RequestContextFactory;
import ai.floedb.floecat.gateway.iceberg.rest.services.namespace.NamespaceCreateService;
import ai.floedb.floecat.gateway.iceberg.rest.services.namespace.NamespaceDeleteService;
import ai.floedb.floecat.gateway.iceberg.rest.services.namespace.NamespaceInfoService;
import ai.floedb.floecat.gateway.iceberg.rest.services.namespace.NamespaceListService;
import ai.floedb.floecat.gateway.iceberg.rest.services.namespace.NamespacePropertyService;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;

@Path("/v1/{prefix}/namespaces")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class NamespaceResource {
  @Inject RequestContextFactory requestContextFactory;
  @Inject NamespaceListService namespaceListService;
  @Inject NamespaceInfoService namespaceInfoService;
  @Inject NamespaceCreateService namespaceCreateService;
  @Inject NamespaceDeleteService namespaceDeleteService;
  @Inject NamespacePropertyService namespacePropertyService;
  @Context UriInfo uriInfo;

  @GET
  public Response list(
      @PathParam("prefix") String prefix,
      @QueryParam("parent") String parent,
      @QueryParam("pageToken") String pageToken,
      @QueryParam("pageSize") Integer pageSize) {
    CatalogRequestContext catalogContext = requestContextFactory.catalog(prefix);
    NamespaceRequestContext parentNamespaceContext = null;
    if (parent != null && !parent.isBlank()) {
      parentNamespaceContext = requestContextFactory.namespace(prefix, parent);
    }
    NamespaceListService.ListCommand command =
        new NamespaceListService.ListCommand(
            catalogContext, parentNamespaceContext, pageToken, pageSize);
    return namespaceListService.list(command);
  }

  @Path("/{namespace}")
  @GET
  public Response get(
      @PathParam("prefix") String prefix, @PathParam("namespace") String namespace) {
    NamespaceRequestContext namespaceContext = requestContextFactory.namespace(prefix, namespace);
    return namespaceInfoService.get(namespaceContext);
  }

  @Path("/{namespace}")
  @HEAD
  public Response exists(
      @PathParam("prefix") String prefix, @PathParam("namespace") String namespace) {
    NamespaceRequestContext namespaceContext = requestContextFactory.namespace(prefix, namespace);
    return namespaceInfoService.exists(namespaceContext);
  }

  @POST
  public Response create(@PathParam("prefix") String prefix, NamespaceRequests.Create req) {
    CatalogRequestContext catalogContext = requestContextFactory.catalog(prefix);
    return namespaceCreateService.create(catalogContext, uriInfo, req);
  }

  @Path("/{namespace}")
  @DELETE
  public Response delete(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @QueryParam("requireEmpty") Boolean requireEmpty) {
    NamespaceRequestContext namespaceContext = requestContextFactory.namespace(prefix, namespace);
    return namespaceDeleteService.delete(namespaceContext, requireEmpty);
  }

  @Path("/{namespace}/properties")
  @POST
  public Response updateProperties(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      NamespacePropertiesRequest req) {
    NamespaceRequestContext namespaceContext = requestContextFactory.namespace(prefix, namespace);
    return namespacePropertyService.update(namespaceContext, req);
  }
}
