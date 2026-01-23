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

package ai.floedb.floecat.gateway.iceberg.rest.resources.common;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.floecat.gateway.iceberg.rest.services.resolution.NameResolution;
import ai.floedb.floecat.gateway.iceberg.rest.services.resolution.NamespacePaths;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.WebApplicationException;
import java.util.List;

@ApplicationScoped
public class RequestContextFactory {
  @Inject IcebergGatewayConfig config;
  @Inject GrpcWithHeaders grpc;
  @Inject TableLifecycleService tableLifecycleService;

  public CatalogRequestContext catalog(String prefix) {
    String catalogName = CatalogResolver.resolveCatalog(config, prefix);
    ResourceId catalogId = CatalogResolver.resolveCatalogId(grpc, config, prefix);
    return new CatalogRequestContext(prefix, catalogName, catalogId);
  }

  public NamespaceRequestContext namespace(String prefix, String namespace) {
    CatalogRequestContext catalogContext = catalog(prefix);
    List<String> namespacePath = List.copyOf(NamespacePaths.split(namespace));
    ResourceId namespaceId;
    try {
      namespaceId =
          tableLifecycleService.resolveNamespaceId(catalogContext.catalogName(), namespacePath);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        throw new WebApplicationException(
            IcebergErrorResponses.noSuchNamespace("Namespace " + namespace + " not found"));
      }
      throw e;
    }
    return new NamespaceRequestContext(catalogContext, namespace, namespacePath, namespaceId);
  }

  public TableRequestContext table(String prefix, String namespace, String table) {
    NamespaceRequestContext namespaceContext = namespace(prefix, namespace);
    return table(namespaceContext, table);
  }

  public TableRequestContext table(NamespaceRequestContext namespaceContext, String table) {
    ResourceId tableId;
    try {
      tableId =
          tableLifecycleService.resolveTableId(
              namespaceContext.catalogName(), namespaceContext.namespacePath(), table);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        String namespace = String.join(".", namespaceContext.namespacePath());
        throw new WebApplicationException(
            IcebergErrorResponses.noSuchTable("Table " + namespace + "." + table + " not found"));
      }
      throw e;
    }
    return new TableRequestContext(namespaceContext, table, tableId);
  }

  public ViewRequestContext view(String prefix, String namespace, String view) {
    NamespaceRequestContext namespaceContext = namespace(prefix, namespace);
    return view(namespaceContext, view);
  }

  public ViewRequestContext view(NamespaceRequestContext namespaceContext, String view) {
    ResourceId viewId;
    try {
      viewId =
          NameResolution.resolveView(
              grpc, namespaceContext.catalogName(), namespaceContext.namespacePath(), view);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        String namespace = String.join(".", namespaceContext.namespacePath());
        throw new WebApplicationException(
            IcebergErrorResponses.noSuchView("View " + namespace + "." + view + " not found"));
      }
      throw e;
    }
    return new ViewRequestContext(namespaceContext, view, viewId);
  }
}
