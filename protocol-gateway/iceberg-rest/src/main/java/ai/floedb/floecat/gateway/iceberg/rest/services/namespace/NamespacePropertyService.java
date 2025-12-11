package ai.floedb.floecat.gateway.iceberg.rest.services.namespace;

import ai.floedb.floecat.catalog.rpc.GetNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.NamespaceSpec;
import ai.floedb.floecat.catalog.rpc.UpdateNamespaceRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.NamespacePropertiesResponse;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.NamespacePropertiesRequest;
import ai.floedb.floecat.gateway.iceberg.rest.resources.support.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.resources.support.NamespaceRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.NamespaceClient;
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

@ApplicationScoped
public class NamespacePropertyService {
  @Inject NamespaceClient namespaceClient;

  public Response update(NamespaceRequestContext namespaceContext, NamespacePropertiesRequest req) {
    ResourceId namespaceId = namespaceContext.namespaceId();
    List<String> removals = req == null || req.removals() == null ? List.of() : req.removals();
    Map<String, String> updates = req == null || req.updates() == null ? Map.of() : req.updates();
    Set<String> conflict = new HashSet<>(removals);
    conflict.retainAll(updates.keySet());
    if (!conflict.isEmpty()) {
      return IcebergErrorResponses.unprocessable("A key cannot be in both removals and updates");
    }

    var existing =
        namespaceClient
            .getNamespace(GetNamespaceRequest.newBuilder().setNamespaceId(namespaceId).build())
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
    namespaceClient.updateNamespace(
        UpdateNamespaceRequest.newBuilder()
            .setNamespaceId(namespaceId)
            .setSpec(spec)
            .setUpdateMask(mask)
            .build());
    return Response.ok(new NamespacePropertiesResponse(updated, removed, missing)).build();
  }
}
