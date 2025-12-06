package ai.floedb.metacat.gateway.iceberg.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

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
  String defaultTenantId();

  @WithDefault("")
  String defaultAuthorization();

  Map<String, String> catalogMapping();

  Optional<String> defaultPrefix();

  Map<String, RegisterConnectorTemplate> registerConnectors();

  @WithDefault("PT10M")
  Duration planTaskTtl();

  @WithDefault("128")
  int planTaskFilesPerTask();

  interface RegisterConnectorTemplate {
    String uri();

    Optional<String> displayName();

    Optional<String> description();

    Map<String, String> properties();

    Optional<AuthTemplate> auth();

    @WithDefault("true")
    boolean captureStatistics();
  }

  interface AuthTemplate {
    @WithDefault("none")
    String scheme();

    Map<String, String> properties();

    Map<String, String> headerHints();

    Optional<String> secretRef();
  }
}
