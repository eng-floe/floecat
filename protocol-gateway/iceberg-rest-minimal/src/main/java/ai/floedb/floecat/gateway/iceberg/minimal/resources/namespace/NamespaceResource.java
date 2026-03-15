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

package ai.floedb.floecat.gateway.iceberg.minimal.resources.namespace;

import ai.floedb.floecat.catalog.rpc.ListNamespacesResponse;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.gateway.iceberg.minimal.api.dto.NamespaceDto;
import ai.floedb.floecat.gateway.iceberg.minimal.api.dto.NamespaceListResponseDto;
import ai.floedb.floecat.gateway.iceberg.minimal.api.request.NamespaceCreateRequest;
import ai.floedb.floecat.gateway.iceberg.minimal.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.minimal.resources.common.NamespacePaths;
import ai.floedb.floecat.gateway.iceberg.minimal.services.namespace.NamespaceBackend;
import io.grpc.StatusRuntimeException;
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
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;

@Path("/v1/{prefix}/namespaces")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class NamespaceResource {
  private final NamespaceBackend backend;

  public NamespaceResource(NamespaceBackend backend) {
    this.backend = backend;
  }

  @GET
  public Response list(
      @PathParam("prefix") String prefix,
      @QueryParam("parent") String parent,
      @QueryParam("pageSize") Integer pageSize,
      @QueryParam("pageToken") String pageToken) {
    try {
      ListNamespacesResponse response =
          backend.list(
              prefix,
              parent == null || parent.isBlank() ? List.of() : NamespacePaths.split(parent),
              pageSize,
              pageToken);
      return Response.ok(
              new NamespaceListResponseDto(
                  response.getNamespacesList().stream().map(NamespacePaths::fullPath).toList(),
                  response.getPage().getNextPageToken().isBlank()
                      ? null
                      : response.getPage().getNextPageToken()))
          .build();
    } catch (StatusRuntimeException exception) {
      return IcebergErrorResponses.grpc(exception);
    }
  }

  @POST
  public Response create(
      @PathParam("prefix") String prefix,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      NamespaceCreateRequest request) {
    if (request == null || request.namespace() == null || request.namespace().isEmpty()) {
      return IcebergErrorResponses.validation("Namespace name must be provided");
    }
    try {
      Namespace namespace =
          backend.create(
              prefix,
              request.namespace(),
              request.properties() == null ? Map.of() : request.properties(),
              idempotencyKey);
      return Response.ok(toDto(namespace)).build();
    } catch (StatusRuntimeException exception) {
      return IcebergErrorResponses.grpc(exception);
    }
  }

  @GET
  @Path("/{namespace}")
  public Response get(
      @PathParam("prefix") String prefix, @PathParam("namespace") String namespace) {
    try {
      return Response.ok(toDto(backend.get(prefix, NamespacePaths.split(namespace)))).build();
    } catch (StatusRuntimeException exception) {
      return IcebergErrorResponses.grpc(exception);
    }
  }

  @HEAD
  @Path("/{namespace}")
  public Response exists(
      @PathParam("prefix") String prefix, @PathParam("namespace") String namespace) {
    try {
      backend.exists(prefix, NamespacePaths.split(namespace));
      return Response.noContent().build();
    } catch (StatusRuntimeException exception) {
      return IcebergErrorResponses.grpc(exception);
    }
  }

  @DELETE
  @Path("/{namespace}")
  public Response delete(
      @PathParam("prefix") String prefix, @PathParam("namespace") String namespace) {
    try {
      backend.delete(prefix, NamespacePaths.split(namespace));
      return Response.noContent().build();
    } catch (StatusRuntimeException exception) {
      return IcebergErrorResponses.grpc(exception);
    }
  }

  private NamespaceDto toDto(Namespace namespace) {
    return new NamespaceDto(NamespacePaths.fullPath(namespace), namespace.getPropertiesMap());
  }
}
