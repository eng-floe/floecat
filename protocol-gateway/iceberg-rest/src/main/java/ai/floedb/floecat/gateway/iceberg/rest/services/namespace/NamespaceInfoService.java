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

import ai.floedb.floecat.catalog.rpc.GetNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.common.NamespaceResponseMapper;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.NamespaceRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.NamespaceClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;

@ApplicationScoped
public class NamespaceInfoService {
  @Inject NamespaceClient namespaceClient;

  public Response get(NamespaceRequestContext namespaceContext) {
    Namespace namespace = load(namespaceContext.namespaceId());
    return Response.ok(NamespaceResponseMapper.toInfo(namespace)).build();
  }

  public Response exists(NamespaceRequestContext namespaceContext) {
    load(namespaceContext.namespaceId());
    return Response.noContent().build();
  }

  private Namespace load(ResourceId namespaceId) {
    return namespaceClient
        .getNamespace(GetNamespaceRequest.newBuilder().setNamespaceId(namespaceId).build())
        .getNamespace();
  }
}
