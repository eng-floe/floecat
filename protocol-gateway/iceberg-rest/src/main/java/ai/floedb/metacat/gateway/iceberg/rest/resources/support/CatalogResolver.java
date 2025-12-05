package ai.floedb.metacat.gateway.iceberg.rest.resources.support;

import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.metacat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.metacat.gateway.iceberg.rest.services.resolution.NameResolution;
import java.util.Map;
import java.util.Optional;

public final class CatalogResolver {
  private CatalogResolver() {}

  public static String resolveCatalog(IcebergGatewayConfig config, String prefix) {
    if (config == null) {
      return prefix;
    }
    Map<String, String> mapping = config.catalogMapping();
    return Optional.ofNullable(mapping == null ? null : mapping.get(prefix)).orElse(prefix);
  }

  public static ResourceId resolveCatalogId(
      GrpcWithHeaders grpc, IcebergGatewayConfig config, String prefix) {
    return NameResolution.resolveCatalog(grpc, resolveCatalog(config, prefix));
  }
}
