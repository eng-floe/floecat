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
import ai.floedb.floecat.gateway.iceberg.minimal.api.metadata.ViewMetadataView;
import ai.floedb.floecat.gateway.iceberg.minimal.api.request.ViewRequests;
import ai.floedb.floecat.gateway.iceberg.minimal.config.MinimalGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.minimal.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.minimal.resources.common.NamespacePaths;
import ai.floedb.floecat.gateway.iceberg.minimal.services.metadata.FileIoFactory;
import ai.floedb.floecat.gateway.iceberg.minimal.services.view.ViewBackend;
import ai.floedb.floecat.gateway.iceberg.minimal.services.view.ViewMetadataService;
import ai.floedb.floecat.gateway.iceberg.minimal.services.view.ViewResponseMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.StatusRuntimeException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;

@Path("/v1/{prefix}/namespaces/{namespace}")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class ViewRegisterResource {
  private final ViewBackend backend;
  private final ViewMetadataService metadataService;
  private final ObjectMapper mapper;
  private final MinimalGatewayConfig config;

  public ViewRegisterResource(
      ViewBackend backend,
      ViewMetadataService metadataService,
      ObjectMapper mapper,
      MinimalGatewayConfig config) {
    this.backend = backend;
    this.metadataService = metadataService;
    this.mapper = mapper;
    this.config = config;
  }

  @POST
  @Path("/register-view")
  public Response register(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      ViewRequests.Register request) {
    if (request == null
        || request.metadataLocation() == null
        || request.metadataLocation().isBlank()) {
      return IcebergErrorResponses.validation("metadata-location is required");
    }
    if (request.name() == null || request.name().isBlank()) {
      return IcebergErrorResponses.validation("name is required");
    }
    String viewName = request.name().trim();
    String metadataLocation = request.metadataLocation().trim();
    List<String> namespacePath = NamespacePaths.split(namespace);

    ViewMetadataView metadata;
    try {
      metadata = loadMetadata(metadataLocation);
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }

    try {
      ViewMetadataService.MetadataContext context =
          metadataService.fromMetadata(
              new ViewMetadataView(
                  metadata.viewUuid(),
                  metadata.formatVersion(),
                  metadataLocation,
                  metadata.currentVersionId(),
                  metadata.versions(),
                  metadata.versionLog(),
                  metadata.schemas(),
                  metadata.properties()));
      View created =
          backend.create(
              prefix,
              namespacePath,
              viewName,
              context.sql(),
              metadataService.extractDialect(context),
              metadataService.extractCreationSearchPath(context),
              metadataService.extractOutputColumns(context),
              metadataService.buildPropertyMap(context),
              idempotencyKey);
      ViewMetadataService.MetadataContext responseContext =
          metadataService.fromView(namespacePath, viewName, created);
      return Response.ok(
              ViewResponseMapper.toLoadResult(
                  namespace, viewName, created, responseContext.metadata()))
          .build();
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    } catch (StatusRuntimeException exception) {
      String message =
          exception.getStatus().getDescription() == null
              ? exception.getStatus().getCode().name()
              : exception.getStatus().getDescription();
      if (message.contains("Namespace ")) {
        return IcebergErrorResponses.noSuchNamespace(message);
      }
      return IcebergErrorResponses.grpc(exception);
    }
  }

  protected ViewMetadataView loadMetadata(String metadataLocation) {
    FileIO fileIO = null;
    try {
      fileIO = FileIoFactory.createFileIo(Map.of(), config);
      InputFile input = fileIO.newInputFile(metadataLocation);
      try (InputStream stream = input.newStream()) {
        return mapper.readValue(
            new String(stream.readAllBytes(), StandardCharsets.UTF_8), ViewMetadataView.class);
      }
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Unable to read view metadata from " + metadataLocation, e);
    } finally {
      if (fileIO instanceof AutoCloseable closable) {
        try {
          closable.close();
        } catch (Exception ignored) {
        }
      }
    }
  }
}
