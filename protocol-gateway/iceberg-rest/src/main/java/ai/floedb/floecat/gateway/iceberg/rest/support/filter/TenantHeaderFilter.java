package ai.floedb.floecat.gateway.iceberg.rest.support.filter;

import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergError;
import ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergErrorResponse;
import ai.floedb.floecat.gateway.iceberg.rest.services.tenant.TenantContext;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.PreMatching;
import jakarta.ws.rs.core.PathSegment;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriBuilder;
import jakarta.ws.rs.core.UriInfo;
import jakarta.ws.rs.ext.Provider;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.jboss.logging.Logger;
import org.jboss.resteasy.reactive.server.ServerRequestFilter;

@Provider
@PreMatching
public class TenantHeaderFilter implements ContainerRequestFilter {
  private static final Logger LOG = Logger.getLogger(TenantHeaderFilter.class);
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
  @ServerRequestFilter(priority = 10)
  public void filter(ContainerRequestContext requestContext) {
    LOG.infof("TenantHeaderFilter invoked for path %s", requestContext.getUriInfo().getPath());
    rewriteDefaultPrefix(requestContext);
    if (requestContext.getProperty("rewrittenPath") != null) {
      LOG.infof("Request URI after rewrite: %s", requestContext.getUriInfo().getRequestUri());
    }
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

  private static final Set<String> PREFIXED_SEGMENTS =
      new HashSet<>(List.of("namespaces", "tables", "views", "transactions"));

  private void rewriteDefaultPrefix(ContainerRequestContext context) {
    String prefix = defaultPrefix();
    if (prefix.isEmpty()) {
      LOG.info("No default prefix configured; skipping path rewrite");
      return;
    }
    LOG.infof(
        "Evaluating prefix rewrite for path %s with prefix '%s'",
        context.getUriInfo().getPath(), prefix);
    UriInfo uriInfo = context.getUriInfo();
    List<PathSegment> segments = uriInfo.getPathSegments();
    if (segments.size() < 2) {
      return;
    }
    if (!"v1".equals(segments.get(0).getPath())) {
      return;
    }
    String second = segments.get(1).getPath();
    if (prefix.equals(second) || !PREFIXED_SEGMENTS.contains(second)) {
      LOG.infof(
          "Skipping prefix rewrite for path %s (second segment=%s)", uriInfo.getPath(), second);
      return;
    }
    StringBuilder newPath = new StringBuilder("/v1/").append(prefix);
    for (int i = 1; i < segments.size(); i++) {
      newPath.append('/').append(segments.get(i).getPath());
    }
    URI newUri =
        UriBuilder.fromUri(uriInfo.getRequestUri()).replacePath(newPath.toString()).build();
    context.setRequestUri(newUri);
    context.setProperty("rewrittenPath", newPath.toString());
    LOG.infof("Rewrote legacy path %s to %s", uriInfo.getPath(), newPath);
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

  private String defaultPrefix() {
    if (config.isUnsatisfied()) {
      LOG.info("Config not available when resolving default prefix");
      return "";
    }
    try {
      var cfg = config.get();
      String value = cfg.defaultPrefix().map(String::trim).orElse("");
      LOG.infof("Resolved default prefix property to '%s'", value);
      if (value.isEmpty()) {
        String systemValue = System.getProperty("floecat.gateway.default-prefix", "");
        String envValue = System.getenv("FLOECAT_GATEWAY_DEFAULT_PREFIX");
        LOG.infof("Fallback system property='%s', env='%s'", systemValue, envValue);
        if (!systemValue.isBlank()) {
          value = systemValue.trim();
        } else if (envValue != null && !envValue.isBlank()) {
          value = envValue.trim();
        }
      }
      return value;
    } catch (Exception ex) {
      LOG.error("Failed to resolve default prefix", ex);
      return "";
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
