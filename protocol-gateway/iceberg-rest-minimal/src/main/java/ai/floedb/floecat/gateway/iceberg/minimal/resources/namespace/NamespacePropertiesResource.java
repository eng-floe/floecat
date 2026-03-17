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

import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.gateway.iceberg.minimal.api.dto.NamespacePropertiesResponseDto;
import ai.floedb.floecat.gateway.iceberg.minimal.api.request.NamespacePropertiesRequest;
import ai.floedb.floecat.gateway.iceberg.minimal.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.minimal.resources.common.NamespacePaths;
import ai.floedb.floecat.gateway.iceberg.minimal.services.namespace.NamespaceBackend;
import io.grpc.StatusRuntimeException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

@Path("/v1/{prefix}/namespaces/{namespace}/properties")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class NamespacePropertiesResource {
  private final NamespaceBackend backend;

  public NamespacePropertiesResource(NamespaceBackend backend) {
    this.backend = backend;
  }

  @POST
  public Response updateProperties(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      NamespacePropertiesRequest request) {
    List<String> removals =
        request == null || request.removals() == null ? List.of() : request.removals();
    Map<String, String> updates =
        request == null || request.updates() == null ? Map.of() : request.updates();
    TreeSet<String> conflicts = new TreeSet<>(removals);
    conflicts.retainAll(updates.keySet());
    if (!conflicts.isEmpty()) {
      return IcebergErrorResponses.validation("A key cannot be in both removals and updates");
    }
    try {
      List<String> namespacePath = NamespacePaths.split(namespace);
      Namespace existing = backend.get(prefix, namespacePath);
      Map<String, String> properties = new LinkedHashMap<>(existing.getPropertiesMap());
      List<String> removed = new ArrayList<>();
      List<String> missing = new ArrayList<>();
      for (String key : removals) {
        if (key == null || key.isBlank()) {
          continue;
        }
        if (properties.containsKey(key)) {
          properties.remove(key);
          removed.add(key);
        } else if (!missing.contains(key)) {
          missing.add(key);
        }
      }
      List<String> updated = new ArrayList<>();
      for (Map.Entry<String, String> entry : updates.entrySet()) {
        if (entry.getKey() == null || entry.getKey().isBlank() || entry.getValue() == null) {
          return IcebergErrorResponses.validation(
              "namespace property updates require non-empty keys and values");
        }
        properties.put(entry.getKey(), entry.getValue());
        updated.add(entry.getKey());
      }
      backend.updateProperties(prefix, namespacePath, properties, idempotencyKey);
      return Response.ok(new NamespacePropertiesResponseDto(updated, removed, missing)).build();
    } catch (StatusRuntimeException exception) {
      return IcebergErrorResponses.grpc(exception);
    }
  }
}
