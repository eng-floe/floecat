package ai.floedb.metacat.gateway.iceberg.rest;

import ai.floedb.metacat.gateway.iceberg.config.IcebergGatewayConfig;
import jakarta.annotation.Priority;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.PreMatching;
import jakarta.ws.rs.core.UriBuilder;
import jakarta.ws.rs.ext.Provider;
import java.net.URI;
import java.util.Set;
import org.jboss.logging.Logger;

/**
 * Inserts the configured default prefix when REST requests omit the catalog prefix segment.
 *
 * <p>Some Iceberg REST clients do not send the `{prefix}` path component. When a default prefix is
 * configured, this filter rewrites `/v1/namespaces/...` to `/v1/{prefix}/namespaces/...`, keeping
 * the query parameters intact.
 */
@Provider
@PreMatching
@Priority(Priorities.AUTHENTICATION - 10)
public class DefaultPrefixFilter implements ContainerRequestFilter {

  private static final Logger LOG = Logger.getLogger(DefaultPrefixFilter.class);
  private static final Set<String> RESOURCE_ROOTS =
      Set.of("namespaces", "tables", "views", "schemas", "snapshots", "transactions", "metrics");

  @Inject Instance<IcebergGatewayConfig> config;

  @Override
  public void filter(ContainerRequestContext requestContext) {
    if (config.isUnsatisfied()) {
      return;
    }
    String prefix = config.get().defaultPrefix();
    if (prefix == null || prefix.isBlank()) {
      return;
    }
    String path = requestContext.getUriInfo().getPath();
    LOG.debugf(
        "prefix-filter: incoming path=%s query=%s",
        path, requestContext.getUriInfo().getRequestUri().getQuery());
    if (path.startsWith("/")) {
      path = path.substring(1);
    }
    if (!path.startsWith("v1/")) {
      return;
    }
    if (path.startsWith("v1/config")) {
      return;
    }
    String remainder = path.substring("v1/".length());
    if (remainder.isBlank()) {
      return;
    }
    String[] segments = remainder.split("/", 2);
    String root = segments[0];
    if (prefix.equals(root) || !RESOURCE_ROOTS.contains(root)) {
      return;
    }
    String tail = segments.length > 1 ? segments[1] : "";
    String newPath =
        tail.isBlank()
            ? String.format("v1/%s/%s", prefix, root)
            : String.format("v1/%s/%s/%s", prefix, root, tail);
    URI newUri =
        UriBuilder.fromUri(requestContext.getUriInfo().getRequestUri())
            .replacePath(newPath)
            .build();
    LOG.debugf("prefix-filter: rewriting path=%s -> %s", path, newPath);
    requestContext.setRequestUri(newUri);
  }
}
