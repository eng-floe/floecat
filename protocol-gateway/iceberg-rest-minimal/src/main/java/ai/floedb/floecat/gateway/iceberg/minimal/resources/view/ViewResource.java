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

package ai.floedb.floecat.gateway.iceberg.minimal.resources.view;

import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.gateway.iceberg.minimal.api.dto.LoadViewResultDto;
import ai.floedb.floecat.gateway.iceberg.minimal.api.dto.TableIdentifierDto;
import ai.floedb.floecat.gateway.iceberg.minimal.api.dto.ViewListResponseDto;
import ai.floedb.floecat.gateway.iceberg.minimal.api.request.ViewRequests;
import ai.floedb.floecat.gateway.iceberg.minimal.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.minimal.resources.common.NamespacePaths;
import ai.floedb.floecat.gateway.iceberg.minimal.services.view.ViewBackend;
import ai.floedb.floecat.gateway.iceberg.minimal.services.view.ViewMetadataService;
import ai.floedb.floecat.gateway.iceberg.minimal.services.view.ViewResponseMapper;
import io.grpc.Status;
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
import java.util.stream.Collectors;

@Path("/v1/{prefix}/namespaces/{namespace}/views")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class ViewResource {
  private final ViewBackend backend;
  private final ViewMetadataService metadataService;

  public ViewResource(ViewBackend backend, ViewMetadataService metadataService) {
    this.backend = backend;
    this.metadataService = metadataService;
  }

  @GET
  public Response list(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @QueryParam("pageSize") Integer pageSize,
      @QueryParam("pageToken") String pageToken) {
    List<String> namespacePath = NamespacePaths.split(namespace);
    try {
      var resp = backend.list(prefix, namespacePath, pageSize, pageToken);
      List<TableIdentifierDto> identifiers =
          resp.getViewsList().stream()
              .map(v -> new TableIdentifierDto(namespacePath, v.getDisplayName()))
              .collect(Collectors.toList());
      String nextToken =
          resp.hasPage() && !resp.getPage().getNextPageToken().isBlank()
              ? resp.getPage().getNextPageToken()
              : null;
      return Response.ok(new ViewListResponseDto(identifiers, nextToken)).build();
    } catch (StatusRuntimeException exception) {
      return mapViewFailure(exception);
    }
  }

  @POST
  public Response create(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      ViewRequests.Create request) {
    List<String> namespacePath = NamespacePaths.split(namespace);
    if (request == null || request.name() == null || request.name().isBlank()) {
      return IcebergErrorResponses.validation("name is required");
    }
    try {
      ViewMetadataService.MetadataContext context =
          metadataService.fromCreate(namespacePath, request.name().trim(), request);
      View created =
          backend.create(
              prefix,
              namespacePath,
              request.name().trim(),
              context.sql(),
              metadataService.extractDialect(context),
              metadataService.extractCreationSearchPath(context),
              metadataService.extractOutputColumns(context),
              metadataService.buildPropertyMap(context),
              idempotencyKey);
      ViewMetadataService.MetadataContext responseContext =
          metadataService.fromView(namespacePath, request.name().trim(), created);
      LoadViewResultDto dto =
          ViewResponseMapper.toLoadResult(
              namespace, request.name().trim(), created, responseContext.metadata());
      return Response.ok(dto).build();
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    } catch (StatusRuntimeException exception) {
      return mapViewFailure(exception);
    }
  }

  @Path("/{view}")
  @GET
  public Response get(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("view") String view) {
    List<String> namespacePath = NamespacePaths.split(namespace);
    try {
      View loaded = backend.get(prefix, namespacePath, view);
      ViewMetadataService.MetadataContext context =
          metadataService.fromView(namespacePath, view, loaded);
      return Response.ok(
              ViewResponseMapper.toLoadResult(namespace, view, loaded, context.metadata()))
          .build();
    } catch (StatusRuntimeException exception) {
      return mapViewFailure(exception);
    }
  }

  @Path("/{view}")
  @HEAD
  public Response exists(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("view") String view) {
    try {
      backend.exists(prefix, NamespacePaths.split(namespace), view);
      return Response.noContent().build();
    } catch (StatusRuntimeException exception) {
      return IcebergErrorResponses.grpcStatusOnly(exception);
    }
  }

  @Path("/{view}")
  @DELETE
  public Response delete(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("view") String view,
      @HeaderParam("Idempotency-Key") String idempotencyKey) {
    try {
      backend.delete(prefix, NamespacePaths.split(namespace), view);
      return Response.noContent().build();
    } catch (StatusRuntimeException exception) {
      return mapViewFailure(exception);
    }
  }

  @Path("/{view}")
  @POST
  public Response commit(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("view") String view,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      ViewRequests.Commit request) {
    List<String> namespacePath = NamespacePaths.split(namespace);
    try {
      View current = backend.get(prefix, namespacePath, view);
      ViewMetadataService.MetadataContext baseContext =
          metadataService.fromView(namespacePath, view, current);
      ViewMetadataService.MetadataContext updated =
          metadataService.applyCommit(namespacePath, baseContext, request);
      View response =
          backend.update(
              prefix,
              namespacePath,
              view,
              updated.sql(),
              metadataService.buildPropertyMap(updated));
      ViewMetadataService.MetadataContext responseContext =
          metadataService.fromView(namespacePath, view, response);
      return Response.ok(
              ViewResponseMapper.toLoadResult(
                  namespace, view, response, responseContext.metadata()))
          .build();
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    } catch (StatusRuntimeException exception) {
      if (exception.getStatus().getCode() == Status.Code.ABORTED
          || exception.getStatus().getCode() == Status.Code.FAILED_PRECONDITION) {
        return IcebergErrorResponses.conflict(descriptionOrCode(exception));
      }
      return mapViewFailure(exception);
    }
  }

  private Response mapViewFailure(StatusRuntimeException exception) {
    if (exception.getStatus().getCode() == Status.Code.NOT_FOUND) {
      String message = descriptionOrCode(exception);
      if (message != null && message.contains("Namespace ")) {
        return IcebergErrorResponses.noSuchNamespace(message);
      }
      return IcebergErrorResponses.noSuchView(message);
    }
    return IcebergErrorResponses.grpc(exception);
  }

  private String descriptionOrCode(StatusRuntimeException exception) {
    return exception.getStatus().getDescription() == null
        ? exception.getStatus().getCode().name()
        : exception.getStatus().getDescription();
  }
}
