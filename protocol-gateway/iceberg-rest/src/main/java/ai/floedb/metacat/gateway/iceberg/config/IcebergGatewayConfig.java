package ai.floedb.metacat.gateway.iceberg.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.util.Map;

@ConfigMapping(prefix = "metacat.gateway")
public interface IcebergGatewayConfig {
  @WithDefault("localhost:9000")
  String upstreamTarget();

  @WithDefault("true")
  boolean upstreamPlaintext();

  @WithDefault("x-tenant-id")
  String tenantHeader();

  @WithDefault("authorization")
  String authHeader();

  @WithDefault("")
  Map<String, String> catalogMapping();
}
