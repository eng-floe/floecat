package ai.floedb.floecat.gateway.iceberg.rest.resources.support;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.floecat.gateway.iceberg.rest.services.resolution.NameResolution;
import ai.floedb.floecat.gateway.iceberg.rest.services.resolution.NamespacePaths;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
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
    ResourceId namespaceId =
        tableLifecycleService.resolveNamespaceId(catalogContext.catalogName(), namespacePath);
    return new NamespaceRequestContext(catalogContext, namespace, namespacePath, namespaceId);
  }

  public TableRequestContext table(String prefix, String namespace, String table) {
    NamespaceRequestContext namespaceContext = namespace(prefix, namespace);
    return table(namespaceContext, table);
  }

  public TableRequestContext table(NamespaceRequestContext namespaceContext, String table) {
    ResourceId tableId =
        tableLifecycleService.resolveTableId(
            namespaceContext.catalogName(), namespaceContext.namespacePath(), table);
    return new TableRequestContext(namespaceContext, table, tableId);
  }

  public ViewRequestContext view(String prefix, String namespace, String view) {
    NamespaceRequestContext namespaceContext = namespace(prefix, namespace);
    return view(namespaceContext, view);
  }

  public ViewRequestContext view(NamespaceRequestContext namespaceContext, String view) {
    ResourceId viewId =
        NameResolution.resolveView(
            grpc, namespaceContext.catalogName(), namespaceContext.namespacePath(), view);
    return new ViewRequestContext(namespaceContext, view, viewId);
  }
}
