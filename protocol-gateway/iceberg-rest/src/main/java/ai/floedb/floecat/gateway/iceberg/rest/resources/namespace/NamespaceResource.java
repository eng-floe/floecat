package ai.floedb.floecat.gateway.iceberg.rest.resources.namespace;

import ai.floedb.floecat.catalog.rpc.CreateNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.DeleteNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.GetNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.ListNamespacesRequest;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.NamespaceSpec;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.NamespaceInfoDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.NamespacePropertiesResponse;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.NamespacePropertiesRequest;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.NamespaceRequests;
import ai.floedb.floecat.gateway.iceberg.rest.resources.support.CatalogResolver;
import ai.floedb.floecat.gateway.iceberg.rest.resources.support.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.resources.support.PageRequestHelper;
import ai.floedb.floecat.gateway.iceberg.rest.services.account.AccountContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.resolution.NameResolution;
import ai.floedb.floecat.gateway.iceberg.rest.services.resolution.NamespacePaths;
import com.google.protobuf.FieldMask;
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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Path("/v1/{prefix}/namespaces")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class NamespaceResource {
  @Inject GrpcWithHeaders grpc;
  @Inject IcebergGatewayConfig config;
  @Inject AccountContext accountContext;
  @Context UriInfo uriInfo;

  @GET
  public Response list(
      @PathParam("prefix") String prefix,
      @QueryParam("parent") String parent,
      @QueryParam("namespace") String namespace,
      @QueryParam("childrenOnly") Boolean childrenOnly,
      @QueryParam("recursive") Boolean recursive,
      @QueryParam("namePrefix") String namePrefix,
      @QueryParam("pageToken") String pageToken,
      @QueryParam("pageSize") Integer pageSize) {
    String catalogName = CatalogResolver.resolveCatalog(config, prefix);
    ResourceId catalogId = CatalogResolver.resolveCatalogId(grpc, config, prefix);
    ListNamespacesRequest.Builder req = ListNamespacesRequest.newBuilder();
    String parentNamespace = parent != null && !parent.isBlank() ? parent : namespace;
    if (parentNamespace != null && !parentNamespace.isBlank()) {
      ResourceId namespaceId =
          NameResolution.resolveNamespace(grpc, catalogName, NamespacePaths.split(parentNamespace));
      req.setNamespaceId(namespaceId);
    } else {
      req.setCatalogId(catalogId);
    }

    if (Boolean.TRUE.equals(childrenOnly)) {
      req.setChildrenOnly(true);
    }
    if (Boolean.TRUE.equals(recursive)) {
      req.setRecursive(true);
    }
    if (namePrefix != null) {
      req.setNamePrefix(namePrefix);
    }
    PageRequest.Builder page = PageRequestHelper.builder(pageToken, pageSize);
    if (page != null) {
      req.setPage(page);
    }

    NamespaceServiceGrpc.NamespaceServiceBlockingStub stub =
        grpc.withHeaders(grpc.raw().namespace());
    var resp = stub.listNamespaces(req.build());
    List<List<String>> namespaces =
        resp.getNamespacesList().stream()
            .map(
                ns ->
                    ns.getParentsList().isEmpty()
                        ? List.of(ns.getDisplayName())
                        : concat(ns.getParentsList(), ns.getDisplayName()))
            .collect(Collectors.toList());
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("namespaces", namespaces);
    String nextToken = resp.getPage().getNextPageToken();
    if (nextToken != null && !nextToken.isBlank()) {
      body.put("next-page-token", nextToken);
    }
    return Response.ok(body).build();
  }

  @Path("/{namespace}")
  @GET
  public Response get(
      @PathParam("prefix") String prefix, @PathParam("namespace") String namespace) {
    String catalogName = CatalogResolver.resolveCatalog(config, prefix);
    ResourceId namespaceId =
        NameResolution.resolveNamespace(grpc, catalogName, NamespacePaths.split(namespace));
    NamespaceServiceGrpc.NamespaceServiceBlockingStub stub =
        grpc.withHeaders(grpc.raw().namespace());
    var resp =
        stub.getNamespace(GetNamespaceRequest.newBuilder().setNamespaceId(namespaceId).build());
    return Response.ok(toInfo(resp.getNamespace())).build();
  }

  @Path("/{namespace}")
  @HEAD
  public Response exists(
      @PathParam("prefix") String prefix, @PathParam("namespace") String namespace) {
    String catalogName = CatalogResolver.resolveCatalog(config, prefix);
    ResourceId namespaceId =
        NameResolution.resolveNamespace(grpc, catalogName, NamespacePaths.split(namespace));
    NamespaceServiceGrpc.NamespaceServiceBlockingStub stub =
        grpc.withHeaders(grpc.raw().namespace());
    stub.getNamespace(GetNamespaceRequest.newBuilder().setNamespaceId(namespaceId).build());
    return Response.noContent().build();
  }

  @POST
  public Response create(@PathParam("prefix") String prefix, NamespaceRequests.Create req) {
    if (req == null || req.namespace() == null || req.namespace().isEmpty()) {
      return IcebergErrorResponses.validation("Namespace name must be provided");
    }

    List<String> path = req.namespace();
    if (path.isEmpty() || path.stream().anyMatch(part -> part == null || part.isBlank())) {
      return IcebergErrorResponses.validation("Namespace name must be provided");
    }

    final String displayName = path.get(path.size() - 1);
    final List<String> parents = path.subList(0, path.size() - 1);

    ResourceId catalogId = CatalogResolver.resolveCatalogId(grpc, config, prefix);

    NamespaceSpec.Builder spec =
        NamespaceSpec.newBuilder()
            .setCatalogId(catalogId)
            .addAllPath(parents)
            .setDisplayName(displayName);

    if (req.description() != null) {
      spec.setDescription(req.description());
    }
    if (req.properties() != null) {
      spec.putAllProperties(req.properties());
    }
    if (req.policyRef() != null) {
      spec.setPolicyRef(req.policyRef());
    }

    NamespaceServiceGrpc.NamespaceServiceBlockingStub stub =
        grpc.withHeaders(grpc.raw().namespace());

    var createdNamespace =
        stub.createNamespace(CreateNamespaceRequest.newBuilder().setSpec(spec).build())
            .getNamespace();

    List<String> createdPath =
        createdNamespace.getParentsList().isEmpty()
            ? List.of(createdNamespace.getDisplayName())
            : concat(createdNamespace.getParentsList(), createdNamespace.getDisplayName());
    String locationNs = NamespacePaths.encode(createdPath);

    return Response.created(uriInfo.getAbsolutePathBuilder().path(locationNs).build())
        .entity(toInfo(createdNamespace))
        .build();
  }

  @Path("/{namespace}")
  @DELETE
  public Response delete(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @QueryParam("requireEmpty") Boolean requireEmpty) {
    String catalogName = CatalogResolver.resolveCatalog(config, prefix);
    ResourceId namespaceId =
        NameResolution.resolveNamespace(grpc, catalogName, NamespacePaths.split(namespace));
    NamespaceServiceGrpc.NamespaceServiceBlockingStub stub =
        grpc.withHeaders(grpc.raw().namespace());
    stub.deleteNamespace(
        DeleteNamespaceRequest.newBuilder()
            .setNamespaceId(namespaceId)
            .setRequireEmpty(requireEmpty == null || requireEmpty)
            .build());
    return Response.noContent().build();
  }

  @Path("/{namespace}/properties")
  @POST
  public Response updateProperties(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      NamespacePropertiesRequest req) {
    String catalogName = CatalogResolver.resolveCatalog(config, prefix);
    ResourceId namespaceId =
        NameResolution.resolveNamespace(grpc, catalogName, NamespacePaths.split(namespace));
    NamespaceServiceGrpc.NamespaceServiceBlockingStub stub =
        grpc.withHeaders(grpc.raw().namespace());

    List<String> removals = req == null || req.removals() == null ? List.of() : req.removals();
    Map<String, String> updates = req == null || req.updates() == null ? Map.of() : req.updates();
    Set<String> conflict = new HashSet<>(removals);
    conflict.retainAll(updates.keySet());
    if (!conflict.isEmpty()) {
      return IcebergErrorResponses.unprocessable("A key cannot be in both removals and updates");
    }

    var existing =
        stub.getNamespace(GetNamespaceRequest.newBuilder().setNamespaceId(namespaceId).build())
            .getNamespace();
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
    stub.updateNamespace(
        ai.floedb.floecat.catalog.rpc.UpdateNamespaceRequest.newBuilder()
            .setNamespaceId(namespaceId)
            .setSpec(spec)
            .setUpdateMask(mask)
            .build());
    return Response.ok(new NamespacePropertiesResponse(updated, removed, missing)).build();
  }

  private List<String> concat(List<String> parents, String name) {
    List<String> out = new java.util.ArrayList<>(parents);
    out.add(name);
    return out;
  }

  private NamespaceInfoDto toInfo(Namespace ns) {
    List<String> path =
        ns.getParentsList().isEmpty()
            ? List.of(ns.getDisplayName())
            : concat(ns.getParentsList(), ns.getDisplayName());

    Map<String, String> props = new LinkedHashMap<>(ns.getPropertiesMap());
    if (ns.hasDescription() && ns.getDescription() != null && !ns.getDescription().isBlank()) {
      props.putIfAbsent("description", ns.getDescription());
    }
    if (ns.hasPolicyRef() && ns.getPolicyRef() != null && !ns.getPolicyRef().isBlank()) {
      props.putIfAbsent("policy_ref", ns.getPolicyRef());
    }

    return new NamespaceInfoDto(path, Map.copyOf(props));
  }
}
