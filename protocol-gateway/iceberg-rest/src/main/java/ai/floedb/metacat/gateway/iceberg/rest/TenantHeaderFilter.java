package ai.floedb.metacat.gateway.iceberg.rest;

import ai.floedb.metacat.gateway.iceberg.config.IcebergGatewayConfig;
import jakarta.annotation.Priority;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;

/** Enforces presence of the tenant header configured for the gateway. */
@Provider
@Priority(Priorities.AUTHENTICATION)
public class TenantHeaderFilter implements ContainerRequestFilter {
  @Inject Instance<IcebergGatewayConfig> config;
  @Inject TenantContext tenantContext;

  private static final String DEFAULT_TENANT_HEADER = "x-tenant-id";
  private static final String DEFAULT_AUTH_HEADER = "authorization";

  @Override
  public void filter(ContainerRequestContext requestContext) {
    try {
      String headerName = DEFAULT_TENANT_HEADER;
      if (!config.isUnsatisfied()) {
        try {
          headerName = config.get().tenantHeader();
        } catch (Exception ignored) {
          // fallback to default
        }
      }
      // Allow config endpoint without tenant header.
      if (requestContext.getUriInfo().getPath().equals("v1/config")) {
        return;
      }
      String tenant = requestContext.getHeaderString(headerName);
      if ((tenant == null || tenant.isBlank()) && !config.isUnsatisfied()) {
        String fallback = safeValue(config.get().defaultTenantId());
        if (!fallback.isBlank()) {
          tenant = fallback;
        }
      }
      if (tenant == null || tenant.isBlank()) {
        requestContext.abortWith(
            Response.status(Response.Status.UNAUTHORIZED)
                .entity(
                    new IcebergErrorResponse(
                        new IcebergError("missing tenant header", "UnauthorizedException", 401)))
                .build());
        return;
      }
      tenantContext.setTenantId(tenant);
      String authHeader = config.isUnsatisfied() ? DEFAULT_AUTH_HEADER : config.get().authHeader();
      if (authHeader == null || authHeader.isBlank()) {
        authHeader = DEFAULT_AUTH_HEADER;
      }
      String auth = requestContext.getHeaderString(authHeader);
      if ((auth == null || auth.isBlank()) && !config.isUnsatisfied()) {
        String fallback = safeValue(config.get().defaultAuthorization());
        if (!fallback.isBlank()) {
          auth = fallback;
        }
      }
      if (auth == null || auth.isBlank()) {
        requestContext.abortWith(
            Response.status(Response.Status.UNAUTHORIZED)
                .entity(
                    new IcebergErrorResponse(
                        new IcebergError(
                            "missing authorization header", "UnauthorizedException", 401)))
                .build());
      }
    } catch (Exception e) {
      requestContext.abortWith(
          Response.status(Response.Status.UNAUTHORIZED)
              .entity(
                  new IcebergErrorResponse(
                      new IcebergError("missing tenant header", "UnauthorizedException", 401)))
              .build());
    }
  }

  private String safeValue(String value) {
    return value == null ? "" : value;
  }
}
