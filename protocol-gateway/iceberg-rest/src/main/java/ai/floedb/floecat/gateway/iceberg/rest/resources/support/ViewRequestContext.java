package ai.floedb.floecat.gateway.iceberg.rest.resources.support;

import ai.floedb.floecat.common.rpc.ResourceId;
import java.util.List;

public record ViewRequestContext(
    NamespaceRequestContext namespace, String view, ResourceId viewId) {

  public CatalogRequestContext catalog() {
    return namespace.catalog();
  }

  public List<String> namespacePath() {
    return namespace.namespacePath();
  }

  public String namespaceName() {
    return namespace.namespace();
  }
}
