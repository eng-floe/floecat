package ai.floedb.metacat.gateway.iceberg.rest.support.filter;

import ai.floedb.metacat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.metacat.gateway.iceberg.rest.api.error.IcebergError;
import ai.floedb.metacat.gateway.iceberg.rest.api.error.IcebergErrorResponse;
import ai.floedb.metacat.gateway.iceberg.rest.services.tenant.TenantContext;
import jakarta.annotation.Priority;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import java.util.Optional;

@Provider
@Priority(Priorities.AUTHENTICATION)
public class TenantHeaderFilter implements ContainerRequestFilter {
  @Inject Instance<IcebergGatewayConfig> config;
  @Inject TenantContext tenantContext;

  public void setConfigInstance(Instance<IcebergGatewayConfig> config) {
    this.config = config;
  }

  public void setTenantContext(TenantContext tenantContext) {
    this.tenantContext = tenantContext;
  }

  private static final String DEFAULT_TENANT_HEADER = "x-tenant-id";
  private static final String DEFAULT_AUTH_HEADER = "authorization";

  @Override
  public void filter(ContainerRequestContext requestContext) {
    if (requestContext.getUriInfo().getPath().equals("v1/config")) {
      return;
    }
    String tenantHeader = resolveTenantHeaderName();
    String tenant = headerValue(requestContext, tenantHeader);
    if (tenant == null || tenant.isBlank()) {
      tenant = defaultTenant().orElse(null);
      if (tenant != null) {
        requestContext.getHeaders().putSingle(tenantHeader, tenant);
      }
    }
    if (tenant == null || tenant.isBlank()) {
      requestContext.abortWith(unauthorized("missing tenant header"));
      return;
    }
    tenantContext.setTenantId(tenant.trim());

    String authHeader = resolveAuthHeaderName();
    String auth = headerValue(requestContext, authHeader);
    if (auth == null || auth.isBlank()) {
      auth = defaultAuthorization().orElse(null);
      if (auth != null) {
        requestContext.getHeaders().putSingle(authHeader, auth);
      }
    }
    if (auth == null || auth.isBlank()) {
      requestContext.abortWith(unauthorized("missing authorization header"));
    }
  }

  private String resolveTenantHeaderName() {
    if (config.isUnsatisfied()) {
      return DEFAULT_TENANT_HEADER;
    }
    try {
      String header = config.get().tenantHeader();
      return header == null || header.isBlank() ? DEFAULT_TENANT_HEADER : header;
    } catch (Exception ignored) {
      return DEFAULT_TENANT_HEADER;
    }
  }

  private String resolveAuthHeaderName() {
    if (config.isUnsatisfied()) {
      return DEFAULT_AUTH_HEADER;
    }
    try {
      String header = config.get().authHeader();
      return header == null || header.isBlank() ? DEFAULT_AUTH_HEADER : header;
    } catch (Exception ignored) {
      return DEFAULT_AUTH_HEADER;
    }
  }

  private Response unauthorized(String message) {
    return Response.status(Response.Status.UNAUTHORIZED)
        .entity(new IcebergErrorResponse(new IcebergError(message, "UnauthorizedException", 401)))
        .build();
  }

  private Optional<String> defaultTenant() {
    if (config.isUnsatisfied()) {
      return Optional.empty();
    }
    try {
      String value = config.get().defaultTenantId();
      if (value == null || value.isBlank()) {
        return Optional.empty();
      }
      return Optional.of(value.trim());
    } catch (Exception ignored) {
      return Optional.empty();
    }
  }

  private Optional<String> defaultAuthorization() {
    if (config.isUnsatisfied()) {
      return Optional.empty();
    }
    try {
      String value = config.get().defaultAuthorization();
      if (value == null || value.isBlank() || isPlaceholder(value)) {
        return Optional.empty();
      }
      return Optional.of(value.trim());
    } catch (Exception ignored) {
      return Optional.empty();
    }
  }

  private boolean isPlaceholder(String value) {
    String normalized = value.trim();
    return normalized.equalsIgnoreCase("undefined") || normalized.equalsIgnoreCase("null");
  }

  private String headerValue(ContainerRequestContext context, String headerName) {
    String value = context.getHeaderString(headerName);
    return value == null ? null : value.trim();
  }
}
