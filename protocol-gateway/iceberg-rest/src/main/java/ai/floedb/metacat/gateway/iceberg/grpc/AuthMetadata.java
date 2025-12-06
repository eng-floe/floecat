package ai.floedb.metacat.gateway.iceberg.grpc;

import ai.floedb.metacat.gateway.iceberg.config.IcebergGatewayConfig;
import io.grpc.Metadata;
import jakarta.ws.rs.core.HttpHeaders;
import java.util.Optional;

public final class AuthMetadata {
  private AuthMetadata() {}

  public static Metadata fromHeaders(IcebergGatewayConfig config, HttpHeaders headers) {
    Metadata md = new Metadata();
    String tenant = header(headers, config.tenantHeader());
    if ((tenant == null || tenant.isBlank()) && config.defaultTenantId() != null) {
      tenant = config.defaultTenantId().isBlank() ? null : config.defaultTenantId();
    }
    if (tenant != null && !tenant.isBlank()) {
      md.put(key(config.tenantHeader()), tenant);
    }
    String auth = header(headers, config.authHeader());
    if ((auth == null || auth.isBlank()) && config.defaultAuthorization() != null) {
      auth = config.defaultAuthorization().isBlank() ? null : config.defaultAuthorization();
    }
    if (auth != null && !auth.isBlank()) {
      md.put(key(config.authHeader()), auth);
    }
    return md;
  }

  private static Metadata.Key<String> key(String name) {
    return Metadata.Key.of(name.toLowerCase(), Metadata.ASCII_STRING_MARSHALLER);
  }

  private static String header(HttpHeaders headers, String name) {
    return Optional.ofNullable(headers.getHeaderString(name)).orElse(null);
  }
}
