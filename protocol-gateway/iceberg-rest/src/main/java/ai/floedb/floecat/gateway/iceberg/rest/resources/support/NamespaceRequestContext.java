package ai.floedb.floecat.gateway.iceberg.rest.resources.support;

import ai.floedb.floecat.common.rpc.ResourceId;
import java.util.List;

public record NamespaceRequestContext(
    CatalogRequestContext catalog,
    String namespace,
    List<String> namespacePath,
    ResourceId namespaceId) {

  public String prefix() {
    return catalog.prefix();
  }

  public String catalogName() {
    return catalog.catalogName();
  }

  public ResourceId catalogId() {
    return catalog.catalogId();
  }
}
