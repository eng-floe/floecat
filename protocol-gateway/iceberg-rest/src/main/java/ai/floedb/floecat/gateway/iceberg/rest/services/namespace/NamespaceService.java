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
import ai.floedb.floecat.catalog.rpc.DeleteNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.GetNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.ListNamespacesRequest;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.NamespaceSpec;
import ai.floedb.floecat.catalog.rpc.UpdateNamespaceRequest;
import ai.floedb.floecat.common.rpc.IdempotencyKey;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.NamespaceListResponse;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.NamespacePropertiesResponse;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.NamespacePropertiesRequest;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.NamespaceRequests;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.CatalogRef;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.NamespaceRef;
import ai.floedb.floecat.gateway.iceberg.rest.common.NamespaceResponseMapper;
import ai.floedb.floecat.gateway.iceberg.rest.common.ReservedPropertyUtil;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.PageRequestHelper;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.GrpcServiceFacade;
import com.google.protobuf.FieldMask;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@ApplicationScoped
public class NamespaceService {
  @Inject GrpcServiceFacade namespaceClient;

  public Response list(
      CatalogRef catalogContext, NamespaceRef parentNamespace, String pageToken, Integer pageSize) {
    ListNamespacesRequest.Builder req = ListNamespacesRequest.newBuilder();
    if (parentNamespace != null) {
      req.setNamespaceId(parentNamespace.namespaceId());
    } else {
      req.setCatalogId(catalogContext.catalogId());
    }
    PageRequest.Builder page = PageRequestHelper.builder(pageToken, pageSize);
    if (page != null) {
      req.setPage(page);
    }

    var resp = namespaceClient.listNamespaces(req.build());
    List<List<String>> namespaces =
        resp.getNamespacesList().stream()
            .map(NamespaceResponseMapper::toPath)
            .collect(Collectors.toList());
    String nextToken = resp.hasPage() ? normalizeToken(resp.getPage().getNextPageToken()) : null;
    return Response.ok(new NamespaceListResponse(namespaces, nextToken)).build();
  }

  public Response get(NamespaceRef namespaceContext) {
    return Response.ok(NamespaceResponseMapper.toInfo(load(namespaceContext.namespaceId())))
        .build();
  }

  public Response exists(NamespaceRef namespaceContext) {
    return Response.noContent().build();
  }

  public Response create(
      CatalogRef catalogContext, String idempotencyKey, NamespaceRequests.Create req) {
    if (req == null || req.namespace() == null || req.namespace().isEmpty()) {
      return IcebergErrorResponses.validation("Namespace name must be provided");
    }
    List<String> path = req.namespace();
    if (path.isEmpty() || path.stream().anyMatch(part -> part == null || part.isBlank())) {
      return IcebergErrorResponses.validation("Namespace name must be provided");
    }

    String displayName = path.get(path.size() - 1);
    List<String> parents = path.subList(0, path.size() - 1);
    NamespaceSpec.Builder spec =
        NamespaceSpec.newBuilder()
            .setCatalogId(catalogContext.catalogId())
            .addAllPath(parents)
            .setDisplayName(displayName);

    if (req.properties() != null) {
      try {
        spec.putAllProperties(ReservedPropertyUtil.validateAndFilter(req.properties()));
      } catch (IllegalArgumentException e) {
        return IcebergErrorResponses.validation(e.getMessage());
      }
    }

    CreateNamespaceRequest.Builder request = CreateNamespaceRequest.newBuilder().setSpec(spec);
    if (idempotencyKey != null && !idempotencyKey.isBlank()) {
      request.setIdempotency(IdempotencyKey.newBuilder().setKey(idempotencyKey));
    }
    Namespace created = namespaceClient.createNamespace(request.build()).getNamespace();
    return Response.ok(NamespaceResponseMapper.toInfo(created)).build();
  }

  public Response delete(NamespaceRef namespaceContext, String idempotencyKey) {
    namespaceClient.deleteNamespace(
        DeleteNamespaceRequest.newBuilder()
            .setNamespaceId(namespaceContext.namespaceId())
            .setRequireEmpty(true)
            .build());
    return Response.noContent().build();
  }

  public Response updateProperties(
      NamespaceRef namespaceContext, String idempotencyKey, NamespacePropertiesRequest req) {
    ResourceId namespaceId = namespaceContext.namespaceId();
    List<String> removals = req == null || req.removals() == null ? List.of() : req.removals();
    Map<String, String> updates = req == null || req.updates() == null ? Map.of() : req.updates();
    try {
      ReservedPropertyUtil.validateAndFilter(updates);
      Map<String, String> removalKeys = new LinkedHashMap<>();
      for (String key : removals) {
        removalKeys.put(key, "");
      }
      ReservedPropertyUtil.validateAndFilter(removalKeys);
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }
    Set<String> conflict = new HashSet<>(removals);
    conflict.retainAll(updates.keySet());
    if (!conflict.isEmpty()) {
      return IcebergErrorResponses.unprocessable("A key cannot be in both removals and updates");
    }

    Namespace existing = load(namespaceId);
    Map<String, String> newProps = new LinkedHashMap<>(existing.getPropertiesMap());

    List<String> removed = new ArrayList<>();
    List<String> missing = new ArrayList<>();
    for (String key : removals) {
      if (newProps.containsKey(key)) {
        newProps.remove(key);
        removed.add(key);
      } else if (!missing.contains(key)) {
        missing.add(key);
      }
    }

    List<String> updated = new ArrayList<>();
    for (Map.Entry<String, String> entry : updates.entrySet()) {
      newProps.put(entry.getKey(), entry.getValue());
      updated.add(entry.getKey());
    }

    NamespaceSpec.Builder spec =
        NamespaceSpec.newBuilder()
            .setCatalogId(existing.getCatalogId())
            .setDisplayName(existing.getDisplayName())
            .addAllPath(existing.getParentsList())
            .putAllProperties(newProps);
    if (existing.hasDescription()) {
      spec.setDescription(existing.getDescription());
    }
    if (existing.hasPolicyRef()) {
      spec.setPolicyRef(existing.getPolicyRef());
    }

    FieldMask.Builder mask = FieldMask.newBuilder().addPaths("properties");
    namespaceClient.updateNamespace(
        UpdateNamespaceRequest.newBuilder()
            .setNamespaceId(namespaceId)
            .setSpec(spec)
            .setUpdateMask(mask)
            .build());
    return Response.ok(new NamespacePropertiesResponse(updated, removed, missing)).build();
  }

  private Namespace load(ResourceId namespaceId) {
    return namespaceClient
        .getNamespace(GetNamespaceRequest.newBuilder().setNamespaceId(namespaceId).build())
        .getNamespace();
  }

  private String normalizeToken(String token) {
    return token == null || token.isBlank() ? null : token;
  }
}
