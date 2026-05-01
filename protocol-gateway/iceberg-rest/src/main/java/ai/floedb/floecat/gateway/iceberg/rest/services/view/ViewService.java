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
import ai.floedb.floecat.catalog.rpc.DeleteViewRequest;
import ai.floedb.floecat.catalog.rpc.GetViewRequest;
import ai.floedb.floecat.catalog.rpc.ListViewsRequest;
import ai.floedb.floecat.catalog.rpc.UpdateViewRequest;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.catalog.rpc.ViewSpec;
import ai.floedb.floecat.common.rpc.IdempotencyKey;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.PageResponse;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TableIdentifierDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.ViewListResponse;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.ViewMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.ViewRequests;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.NamespaceRef;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.ViewRef;
import ai.floedb.floecat.gateway.iceberg.rest.common.ViewResponseMapper;
import ai.floedb.floecat.gateway.iceberg.rest.config.ConnectorIntegrationConfig;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.PageRequestHelper;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.GrpcServiceFacade;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.FileIoFactory;
import ai.floedb.floecat.gateway.iceberg.rest.services.view.ViewMetadataService.MetadataContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.FieldMask;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;

@ApplicationScoped
public class ViewService {
  @Inject GrpcServiceFacade viewClient;
  @Inject ViewMetadataService viewMetadataService;
  @Inject TableGatewaySupport tableGatewaySupport;
  @Inject ObjectMapper mapper;
  @Inject ConnectorIntegrationConfig config;

  public Response list(NamespaceRef namespaceContext, String pageToken, Integer pageSize) {
    ListViewsRequest.Builder req =
        ListViewsRequest.newBuilder().setNamespaceId(namespaceContext.namespaceId());
    PageRequest.Builder page = PageRequestHelper.builder(pageToken, pageSize);
    if (page != null) {
      req.setPage(page);
    }

    var resp = viewClient.listViews(req.build());
    List<TableIdentifierDto> identifiers =
        resp.getViewsList().stream()
            .map(v -> new TableIdentifierDto(namespaceContext.namespacePath(), v.getDisplayName()))
            .collect(Collectors.toList());
    return Response.ok(new ViewListResponse(identifiers, flattenPageToken(resp.getPage()))).build();
  }

  public Response create(
      NamespaceRef namespaceContext, String idempotencyKey, ViewRequests.Create req) {
    List<String> namespacePath = namespaceContext.namespacePath();
    if (req == null || req.name() == null || req.name().isBlank()) {
      return IcebergErrorResponses.validation("name is required");
    }
    String viewName = req.name().trim();

    MetadataContext metadataContext;
    try {
      metadataContext = viewMetadataService.fromCreate(namespacePath, viewName, req);
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }

    ViewSpec.Builder spec =
        ViewSpec.newBuilder()
            .setCatalogId(namespaceContext.catalogId())
            .setNamespaceId(namespaceContext.namespaceId())
            .setDisplayName(viewName);
    try {
      spec.putAllProperties(viewMetadataService.buildPropertyMap(metadataContext));
      viewMetadataService.extractSqlDefinitions(metadataContext).forEach(spec::addSqlDefinitions);
      spec.addAllCreationSearchPath(viewMetadataService.extractCreationSearchPath(metadataContext));
      spec.addAllOutputColumns(viewMetadataService.extractOutputColumns(metadataContext));
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }

    CreateViewRequest.Builder request = CreateViewRequest.newBuilder().setSpec(spec);
    if (idempotencyKey != null && !idempotencyKey.isBlank()) {
      request.setIdempotency(IdempotencyKey.newBuilder().setKey(idempotencyKey).build());
    }
    var created = viewClient.createView(request.build());
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

  public Response get(ViewRef viewContext) {
    var resp =
        viewClient.getView(GetViewRequest.newBuilder().setViewId(viewContext.viewId()).build());
    MetadataContext context =
        viewMetadataService.fromView(
            viewContext.namespacePath(), viewContext.view(), resp.getView());
    return Response.ok(
            ViewResponseMapper.toLoadResult(
                viewContext.namespaceName(),
                viewContext.view(),
                resp.getView(),
                context.metadata()))
        .build();
  }

  public Response exists(ViewRef viewContext) {
    viewClient.getView(GetViewRequest.newBuilder().setViewId(viewContext.viewId()).build());
    return Response.noContent().build();
  }

  public Response delete(ViewRef viewContext) {
    viewClient.deleteView(DeleteViewRequest.newBuilder().setViewId(viewContext.viewId()).build());
    return Response.noContent().build();
  }

  public Response commit(ViewRef viewContext, ViewRequests.Commit req) {
    View current =
        viewClient
            .getView(GetViewRequest.newBuilder().setViewId(viewContext.viewId()).build())
            .getView();
    MetadataContext baseContext =
        viewMetadataService.fromView(viewContext.namespacePath(), viewContext.view(), current);
    MetadataContext updated;
    try {
      updated = viewMetadataService.applyCommit(viewContext.namespacePath(), baseContext, req);
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }

    ViewSpec.Builder spec = ViewSpec.newBuilder();
    FieldMask.Builder mask =
        FieldMask.newBuilder().addPaths("sql_definitions").addPaths("properties");
    try {
      viewMetadataService.extractSqlDefinitions(updated).forEach(spec::addSqlDefinitions);
      spec.addAllCreationSearchPath(viewMetadataService.extractCreationSearchPath(updated));
      var outputColumns = viewMetadataService.extractOutputColumns(updated);
      if (!outputColumns.isEmpty()) {
        spec.addAllOutputColumns(outputColumns);
        mask.addPaths("output_columns");
      }
      spec.putAllProperties(viewMetadataService.buildPropertyMap(updated));
      mask.addPaths("creation_search_path");
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }

    var resp =
        viewClient.updateView(
            UpdateViewRequest.newBuilder()
                .setViewId(viewContext.viewId())
                .setSpec(spec)
                .setUpdateMask(mask)
                .build());
    MetadataContext responseContext =
        viewMetadataService.fromView(
            viewContext.namespacePath(), viewContext.view(), resp.getView());
    return Response.ok(
            ViewResponseMapper.toLoadResult(
                viewContext.namespaceName(),
                viewContext.view(),
                resp.getView(),
                responseContext.metadata()))
        .build();
  }

  public Response register(
      NamespaceRef namespaceContext, String idempotencyKey, ViewRequests.Register req) {
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
            .setDisplayName(viewName);
    try {
      viewMetadataService.extractSqlDefinitions(metadataContext).forEach(spec::addSqlDefinitions);
      spec.addAllCreationSearchPath(viewMetadataService.extractCreationSearchPath(metadataContext));
      spec.addAllOutputColumns(viewMetadataService.extractOutputColumns(metadataContext));
      spec.putAllProperties(viewMetadataService.buildPropertyMap(metadataContext));
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }

    CreateViewRequest.Builder request = CreateViewRequest.newBuilder().setSpec(spec);
    if (idempotencyKey != null && !idempotencyKey.isBlank()) {
      request.setIdempotency(IdempotencyKey.newBuilder().setKey(idempotencyKey).build());
    }
    var created = viewClient.createView(request.build());
    MetadataContext responseContext =
        viewMetadataService.fromView(namespaceContext.namespacePath(), viewName, created.getView());
    return Response.ok(
            ViewResponseMapper.toLoadResult(
                namespaceContext.namespace(),
                viewName,
                created.getView(),
                responseContext.metadata()))
        .build();
  }

  private String flattenPageToken(PageResponse page) {
    String token = page.getNextPageToken();
    return token == null || token.isBlank() ? null : token;
  }

  private ViewMetadataView loadMetadata(String metadataLocation) {
    FileIO fileIO = null;
    try {
      Map<String, String> ioProps = tableGatewaySupport.defaultFileIoProperties();
      fileIO = FileIoFactory.createFileIo(ioProps, config, true);
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
