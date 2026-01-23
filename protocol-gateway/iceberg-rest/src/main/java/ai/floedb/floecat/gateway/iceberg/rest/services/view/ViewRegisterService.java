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

package ai.floedb.floecat.gateway.iceberg.rest.services.view;

import ai.floedb.floecat.catalog.rpc.CreateViewRequest;
import ai.floedb.floecat.catalog.rpc.ViewSpec;
import ai.floedb.floecat.common.rpc.IdempotencyKey;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.ViewMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.ViewRequests;
import ai.floedb.floecat.gateway.iceberg.rest.common.ViewResponseMapper;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.NamespaceRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.ViewClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.FileIoFactory;
import ai.floedb.floecat.gateway.iceberg.rest.services.view.ViewMetadataService.MetadataContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;

@ApplicationScoped
public class ViewRegisterService {
  @Inject ViewClient viewClient;
  @Inject ViewMetadataService viewMetadataService;
  @Inject ObjectMapper mapper;
  @Inject IcebergGatewayConfig config;

  public Response register(
      NamespaceRequestContext namespaceContext, String idempotencyKey, ViewRequests.Register req) {
    if (req == null || req.metadataLocation() == null || req.metadataLocation().isBlank()) {
      return IcebergErrorResponses.validation("metadata-location is required");
    }
    if (req.name() == null || req.name().isBlank()) {
      return IcebergErrorResponses.validation("name is required");
    }
    String metadataLocation = req.metadataLocation().trim();
    String viewName = req.name().trim();

    ViewMetadataView metadata;
    try {
      metadata = loadMetadata(metadataLocation);
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }
    if (metadata == null) {
      return IcebergErrorResponses.validation("metadata is required");
    }
    ViewMetadataView resolvedMetadata =
        new ViewMetadataView(
            metadata.viewUuid(),
            metadata.formatVersion(),
            metadataLocation,
            metadata.currentVersionId(),
            metadata.versions(),
            metadata.versionLog(),
            metadata.schemas(),
            metadata.properties());
    MetadataContext metadataContext;
    try {
      metadataContext = viewMetadataService.fromMetadata(resolvedMetadata);
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }

    ViewSpec.Builder spec =
        ViewSpec.newBuilder()
            .setCatalogId(namespaceContext.catalogId())
            .setNamespaceId(namespaceContext.namespaceId())
            .setDisplayName(viewName)
            .setSql(metadataContext.sql());
    try {
      spec.putAllProperties(viewMetadataService.buildPropertyMap(metadataContext));
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }

    CreateViewRequest.Builder request = CreateViewRequest.newBuilder().setSpec(spec);
    if (idempotencyKey != null && !idempotencyKey.isBlank()) {
      request.setIdempotency(IdempotencyKey.newBuilder().setKey(idempotencyKey).build());
    }
    var created = viewClient.createView(request.build());
    List<String> namespacePath = namespaceContext.namespacePath();
    MetadataContext responseContext =
        viewMetadataService.fromView(namespacePath, viewName, created.getView());
    return Response.ok(
            ViewResponseMapper.toLoadResult(
                namespaceContext.namespace(),
                viewName,
                created.getView(),
                responseContext.metadata()))
        .build();
  }

  private ViewMetadataView loadMetadata(String metadataLocation) {
    FileIO fileIO = null;
    try {
      fileIO = FileIoFactory.createFileIo(Map.of(), config, true);
      InputFile input = fileIO.newInputFile(metadataLocation);
      try (InputStream stream = input.newStream()) {
        String payload = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        return mapper.readValue(payload, ViewMetadataView.class);
      }
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Unable to read view metadata from " + metadataLocation, e);
    } finally {
      closeQuietly(fileIO);
    }
  }

  private void closeQuietly(FileIO fileIO) {
    if (fileIO instanceof AutoCloseable closable) {
      try {
        closable.close();
      } catch (Exception ignored) {
        // ignore close failure
      }
    }
  }
}
