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

package ai.floedb.floecat.gateway.iceberg.rest.namespace;

import ai.floedb.floecat.gateway.iceberg.rest.api.request.NamespaceRequests;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.CatalogRef;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.NamespaceRef;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.ResourceResolver;
import ai.floedb.floecat.gateway.iceberg.rest.support.IcebergErrorResponses;
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
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;

@Path("/v1/{prefix}/namespaces")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class NamespaceResource {
  @Inject ResourceResolver resourceResolver;
  @Inject NamespaceService namespaceService;
  @Context UriInfo uriInfo;

  @GET
  public Response list(
      @PathParam("prefix") String prefix,
      @QueryParam("parent") String parent,
      @QueryParam("pageToken") String pageToken,
      @QueryParam("pageSize") Integer pageSize) {
    CatalogRef catalogContext = resourceResolver.catalog(prefix);
    NamespaceRef parentNamespaceContext = null;
    if (parent != null && !parent.isBlank()) {
      parentNamespaceContext = resourceResolver.namespace(prefix, parent);
    }
    NamespaceService.ListCommand command =
        new NamespaceService.ListCommand(
            catalogContext, parentNamespaceContext, pageToken, pageSize);
    return namespaceService.list(command);
  }

  @Path("/{namespace}")
  @GET
  public Response get(
      @PathParam("prefix") String prefix, @PathParam("namespace") String namespace) {
    NamespaceRef namespaceContext = resourceResolver.namespace(prefix, namespace);
    return namespaceService.get(namespaceContext);
  }

  @Path("/{namespace}")
  @HEAD
  public Response exists(
      @PathParam("prefix") String prefix, @PathParam("namespace") String namespace) {
    try {
      NamespaceRef namespaceContext = resourceResolver.namespace(prefix, namespace);
      return namespaceService.exists(namespaceContext);
    } catch (WebApplicationException e) {
      return IcebergErrorResponses.headStatusOnlyOnNotFound(e);
    }
  }

  @POST
  public Response create(
      @PathParam("prefix") String prefix,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      NamespaceRequests.Create req) {
    CatalogRef catalogContext = resourceResolver.catalog(prefix);
    return namespaceService.create(catalogContext, uriInfo, idempotencyKey, req);
  }

  @Path("/{namespace}")
  @DELETE
  public Response delete(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @HeaderParam("Idempotency-Key") String idempotencyKey) {
    NamespaceRef namespaceContext = resourceResolver.namespace(prefix, namespace);
    return namespaceService.delete(namespaceContext, idempotencyKey);
  }

  @Path("/{namespace}/properties")
  @POST
  public Response updateProperties(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      NamespaceRequests.Properties req) {
    NamespaceRef namespaceContext = resourceResolver.namespace(prefix, namespace);
    return namespaceService.updateProperties(namespaceContext, idempotencyKey, req);
  }
}
