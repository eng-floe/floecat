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

package ai.floedb.floecat.gateway.iceberg.rest.catalog;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.CatalogResolver;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
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
public class ResourceResolver {
  @Inject IcebergGatewayConfig config;
  @Inject GrpcWithHeaders grpc;
  @Inject TableLifecycleService tableLifecycleService;

  public CatalogRef catalog(String prefix) {
    String catalogName = CatalogResolver.resolveCatalog(config, prefix);
    ResourceId catalogId = CatalogResolver.resolveCatalogId(grpc, config, prefix);
    return new CatalogRef(prefix, catalogName, catalogId);
  }

  public NamespaceRef namespace(String prefix, String namespace) {
    return namespace(catalog(prefix), namespace);
  }

  public NamespaceRef namespace(CatalogRef catalog, String namespace) {
    List<String> namespacePath = List.copyOf(NamespacePaths.split(namespace));
    ResourceId namespaceId;
    try {
      namespaceId = tableLifecycleService.resolveNamespaceId(catalog.catalogName(), namespacePath);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        throw new WebApplicationException(
            IcebergErrorResponses.noSuchNamespace("Namespace " + namespace + " not found"));
      }
      throw e;
    }
    return new NamespaceRef(catalog, namespace, namespacePath, namespaceId);
  }

  public ViewRef view(String prefix, String namespace, String view) {
    return view(namespace(prefix, namespace), view);
  }

  public TableRef table(String prefix, String namespace, String table) {
    return table(namespace(prefix, namespace), table);
  }

  public TableRef table(NamespaceRef namespace, String table) {
    ResourceId tableId;
    try {
      tableId =
          tableLifecycleService.resolveTableId(
              namespace.catalogName(), namespace.namespacePath(), table);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        String namespaceName = String.join(".", namespace.namespacePath());
        throw new WebApplicationException(
            IcebergErrorResponses.noSuchTable(
                "Table " + namespaceName + "." + table + " not found"));
      }
      throw e;
    }
    return new TableRef(namespace, table, tableId);
  }

  public ViewRef view(NamespaceRef namespace, String view) {
    ResourceId viewId;
    try {
      viewId =
          NameResolution.resolveView(
              grpc, namespace.catalogName(), namespace.namespacePath(), view);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        String namespaceName = String.join(".", namespace.namespacePath());
        throw new WebApplicationException(
            IcebergErrorResponses.noSuchView("View " + namespaceName + "." + view + " not found"));
      }
      throw e;
    }
    return new ViewRef(namespace, view, viewId);
  }
}
