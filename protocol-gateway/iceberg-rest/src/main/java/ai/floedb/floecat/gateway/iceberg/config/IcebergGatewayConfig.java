package ai.floedb.floecat.gateway.iceberg.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

@ConfigMapping(prefix = "floecat.gateway")
public interface IcebergGatewayConfig {
  @WithDefault("localhost:9000")
  String upstreamTarget();

  @WithDefault("true")
  boolean upstreamPlaintext();

  @WithDefault("x-tenant-id")
  String accountHeader();

  @WithDefault("authorization")
  String authHeader();

  @WithDefault("")
  String defaultAccountId();

  @WithDefault("")
  String defaultAuthorization();

  Map<String, String> catalogMapping();

  Optional<String> defaultPrefix();

  Optional<String> defaultWarehousePath();

  Optional<String> defaultRegion();

  Optional<StorageCredentialConfig> storageCredential();

  Optional<String> metadataFileIo();

  Optional<String> metadataFileIoRoot();

  interface StorageCredentialConfig {
    Optional<String> scope();

    Map<String, String> properties();
  }

  Map<String, RegisterConnectorTemplate> registerConnectors();

  @WithDefault("true")
  boolean connectorIntegrationEnabled();

  @WithDefault("PT10M")
  Duration planTaskTtl();

  @WithDefault("128")
  int planTaskFilesPerTask();

  @WithDefault("PT30M")
  Duration idempotencyKeyLifetime();

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
