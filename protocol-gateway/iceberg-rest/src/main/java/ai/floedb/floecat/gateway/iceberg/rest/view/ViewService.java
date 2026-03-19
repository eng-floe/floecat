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

import ai.floedb.floecat.catalog.rpc.CreateViewRequest;
import ai.floedb.floecat.catalog.rpc.DeleteViewRequest;
import ai.floedb.floecat.catalog.rpc.GetViewRequest;
import ai.floedb.floecat.catalog.rpc.ListViewsRequest;
import ai.floedb.floecat.catalog.rpc.UpdateViewRequest;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.catalog.rpc.ViewSpec;
import ai.floedb.floecat.common.rpc.IdempotencyKey;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.LoadViewResultDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TableIdentifierDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.ViewListResponse;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.ViewMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.ViewRequests;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.CatalogResolver;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.NameResolution;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.NamespacePaths;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.NamespaceRef;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.ViewRef;
import ai.floedb.floecat.gateway.iceberg.rest.support.FileIoFactory;
import ai.floedb.floecat.gateway.iceberg.rest.support.GrpcServiceFacade;
import ai.floedb.floecat.gateway.iceberg.rest.support.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.support.PageRequestHelper;
import ai.floedb.floecat.gateway.iceberg.rest.view.ViewMetadataService.MetadataContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.FieldMask;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
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
  @Inject IcebergGatewayConfig config;
  @Inject GrpcWithHeaders grpc;

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
    return Response.ok(
            new ViewListResponse(identifiers, PageRequestHelper.nextToken(resp.getPage())))
        .build();
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
    spec.setSql(metadataContext.sql());
    try {
      spec.putAllProperties(viewMetadataService.buildPropertyMap(metadataContext));
      spec.setDialect(viewMetadataService.extractDialect(metadataContext));
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
            toLoadResult(
                namespaceContext.namespace(),
                viewName,
                created.getView(),
                responseContext.metadata()))
        .build();
  }

  public Response get(
      ViewRef viewContext, String namespace, String viewName, List<String> namespacePath) {
    var resp =
        viewClient.getView(GetViewRequest.newBuilder().setViewId(viewContext.viewId()).build());
    MetadataContext context = viewMetadataService.fromView(namespacePath, viewName, resp.getView());
    return Response.ok(toLoadResult(namespace, viewName, resp.getView(), context.metadata()))
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

  public Response commit(
      ViewRef viewContext,
      List<String> namespacePath,
      String namespace,
      String viewName,
      ViewRequests.Commit req) {
    View current =
        viewClient
            .getView(GetViewRequest.newBuilder().setViewId(viewContext.viewId()).build())
            .getView();
    MetadataContext baseContext = viewMetadataService.fromView(namespacePath, viewName, current);
    MetadataContext updated;
    try {
      updated = viewMetadataService.applyCommit(namespacePath, baseContext, req);
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }

    ViewSpec.Builder spec = ViewSpec.newBuilder().setSql(updated.sql());
    FieldMask.Builder mask = FieldMask.newBuilder().addPaths("sql").addPaths("properties");
    try {
      spec.putAllProperties(viewMetadataService.buildPropertyMap(updated));
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
        viewMetadataService.fromView(namespacePath, viewName, resp.getView());
    return Response.ok(
            toLoadResult(namespace, viewName, resp.getView(), responseContext.metadata()))
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
            toLoadResult(
                namespaceContext.namespace(),
                viewName,
                created.getView(),
                responseContext.metadata()))
        .build();
  }

  public Response rename(String prefix, String idempotencyKey, TableRequests.Rename request) {
    String catalogName = CatalogResolver.resolveCatalog(config, prefix);
    ResourceId viewId;
    try {
      viewId =
          NameResolution.resolveView(
              grpc, catalogName, request.source().namespace(), request.source().name());
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        String namespace = String.join(".", request.source().namespace());
        return IcebergErrorResponses.noSuchView(
            "View " + namespace + "." + request.source().name() + " not found");
      }
      throw e;
    }
    ResourceId namespaceId;
    try {
      namespaceId =
          NameResolution.resolveNamespace(grpc, catalogName, request.destination().namespace());
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        String namespace = String.join(".", request.destination().namespace());
        return IcebergErrorResponses.noSuchNamespace("Namespace " + namespace + " not found");
      }
      throw e;
    }

    ViewSpec.Builder spec =
        ViewSpec.newBuilder()
            .setNamespaceId(namespaceId)
            .setDisplayName(request.destination().name());
    FieldMask mask =
        FieldMask.newBuilder().addPaths("namespace_id").addPaths("display_name").build();
    viewClient.updateView(
        UpdateViewRequest.newBuilder().setViewId(viewId).setSpec(spec).setUpdateMask(mask).build());
    return Response.noContent().build();
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
      FileIoFactory.closeQuietly(fileIO);
    }
  }

  private static LoadViewResultDto toLoadResult(
      String namespacePath, String viewName, View view, ViewMetadataView storedMetadata) {
    Map<String, String> props = new LinkedHashMap<>(view.getPropertiesMap());
    String metadataLocation = props.get("metadata-location");

    ViewMetadataView metadata = storedMetadata;
    if (metadata == null) {
      metadata = synthesizeMetadata(namespacePath, viewName, view, props);
      metadataLocation = metadata.location();
    } else if (metadataLocation == null || metadataLocation.isBlank()) {
      metadataLocation = metadata.location();
    } else if (metadata.location() == null || metadata.location().isBlank()) {
      metadata =
          new ViewMetadataView(
              metadata.viewUuid(),
              metadata.formatVersion(),
              metadataLocation,
              metadata.currentVersionId(),
              metadata.versions(),
              metadata.versionLog(),
              metadata.schemas(),
              metadata.properties());
    }
    if (metadataLocation == null || metadataLocation.isBlank()) {
      metadataLocation = "floecat://" + viewName;
    }
    return new LoadViewResultDto(metadataLocation, metadata, Map.of());
  }

  private static ViewMetadataView synthesizeMetadata(
      String namespacePath, String viewName, View view, Map<String, String> props) {
    String metadataLocation =
        props.getOrDefault(
            "metadata-location", props.getOrDefault("location", "floecat://" + viewName));
    Long timestamp =
        view.hasCreatedAt()
            ? view.getCreatedAt().getSeconds() * 1000 + view.getCreatedAt().getNanos() / 1_000_000
            : Instant.now().toEpochMilli();
    List<String> namespace = NamespacePaths.split(namespacePath);
    ViewMetadataView.ViewRepresentation representation =
        new ViewMetadataView.ViewRepresentation(
            "sql", view.getSql(), props.getOrDefault("dialect", "ansi"));
    ViewMetadataView.ViewVersion version =
        new ViewMetadataView.ViewVersion(
            0,
            timestamp,
            0,
            Map.of("operation", "sql"),
            List.of(representation),
            namespace,
            props.get("default-catalog"));
    ViewMetadataView.ViewHistoryEntry history = new ViewMetadataView.ViewHistoryEntry(0, timestamp);
    ViewMetadataView.SchemaSummary schema =
        new ViewMetadataView.SchemaSummary(0, "struct", List.of(), List.of());
    return new ViewMetadataView(
        view.hasResourceId() ? view.getResourceId().getId() : viewName,
        1,
        metadataLocation,
        0,
        List.of(version),
        List.of(history),
        List.of(schema),
        props);
  }
}
