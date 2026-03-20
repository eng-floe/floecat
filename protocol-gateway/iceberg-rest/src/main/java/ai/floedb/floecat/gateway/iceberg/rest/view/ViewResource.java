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

package ai.floedb.floecat.gateway.iceberg.rest.view;

import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.ViewRequests;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.NamespaceRef;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.ResourceResolver;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.ViewRef;
import ai.floedb.floecat.gateway.iceberg.rest.support.IcebergErrorResponses;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
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
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.List;

@Path("/v1/{prefix}")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class ViewResource {
  @Inject ResourceResolver resourceResolver;
  @Inject ViewService viewService;

  @GET
  @Path("/namespaces/{namespace}/views")
  public Response list(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @QueryParam("pageToken") String pageToken,
      @QueryParam("pageSize") Integer pageSize) {
    NamespaceRef namespaceContext = resourceResolver.namespace(prefix, namespace);
    return viewService.list(namespaceContext, pageToken, pageSize);
  }

  @POST
  @Path("/namespaces/{namespace}/views")
  public Response create(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      ViewRequests.Create req) {
    NamespaceRef namespaceContext = resourceResolver.namespace(prefix, namespace);
    return viewService.create(namespaceContext, idempotencyKey, req);
  }

  @GET
  @Path("/namespaces/{namespace}/views/{view}")
  public Response get(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("view") String view) {
    ViewRef viewContext = resourceResolver.view(prefix, namespace, view);
    List<String> namespacePath = viewContext.namespacePath();
    return viewService.get(viewContext, namespace, view, namespacePath);
  }

  @HEAD
  @Path("/namespaces/{namespace}/views/{view}")
  public Response exists(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("view") String view) {
    try {
      ViewRef viewContext = resourceResolver.view(prefix, namespace, view);
      return viewService.exists(viewContext);
    } catch (WebApplicationException e) {
      return IcebergErrorResponses.headStatusOnlyOnNotFound(e);
    }
  }

  @DELETE
  @Path("/namespaces/{namespace}/views/{view}")
  public Response delete(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("view") String view,
      @HeaderParam("Idempotency-Key") String idempotencyKey) {
    ViewRef viewContext = resourceResolver.view(prefix, namespace, view);
    return viewService.delete(viewContext);
  }

  @POST
  @Path("/namespaces/{namespace}/views/{view}")
  public Response commit(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("view") String view,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      ViewRequests.Commit req) {
    ViewRef viewContext = resourceResolver.view(prefix, namespace, view);
    List<String> namespacePath = viewContext.namespacePath();
    return viewService.commit(viewContext, namespacePath, namespace, view, req);
  }

  @POST
  @Path("/namespaces/{namespace}/register-view")
  public Response register(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      ViewRequests.Register req) {
    NamespaceRef namespaceContext = resourceResolver.namespace(prefix, namespace);
    return viewService.register(namespaceContext, idempotencyKey, req);
  }

  @POST
  @Path("/views/rename")
  public Response rename(
      @PathParam("prefix") String prefix,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      @NotNull @Valid TableRequests.Rename request) {
    return viewService.rename(prefix, idempotencyKey, request);
  }
}
