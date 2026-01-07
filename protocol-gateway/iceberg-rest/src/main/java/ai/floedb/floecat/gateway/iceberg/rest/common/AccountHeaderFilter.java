/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.gateway.iceberg.rest.common;

import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergError;
import ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergErrorResponse;
import ai.floedb.floecat.gateway.iceberg.rest.services.account.AccountContext;
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
import org.jboss.resteasy.reactive.server.ServerRequestFilter;

@Provider
@PreMatching
public class AccountHeaderFilter implements ContainerRequestFilter {
  @Inject Instance<IcebergGatewayConfig> config;
  @Inject AccountContext accountContext;

  public void setConfigInstance(Instance<IcebergGatewayConfig> config) {
    this.config = config;
  }

  public void setAccountContext(AccountContext accountContext) {
    this.accountContext = accountContext;
  }

  private static final String DEFAULT_ACCOUNT_HEADER = "x-tenant-id";
  private static final String DEFAULT_AUTH_HEADER = "authorization";

  @Override
  @ServerRequestFilter(priority = 10)
  public void filter(ContainerRequestContext requestContext) {
    rewriteDefaultPrefix(requestContext);
    if (requestContext.getUriInfo().getPath().equals("v1/config")) {
      return;
    }
    String accountHeader = resolveAccountHeaderName();
    String account = headerValue(requestContext, accountHeader);
    if (account == null || account.isBlank()) {
      account = defaultAccount().orElse(null);
      if (account != null) {
        requestContext.getHeaders().putSingle(accountHeader, account);
      }
    }
    if (account == null || account.isBlank()) {
      requestContext.abortWith(unauthorized("missing account header"));
      return;
    }
    accountContext.setAccountId(account.trim());

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
      return;
    }
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
  }

  private String resolveAccountHeaderName() {
    if (config.isUnsatisfied()) {
      return DEFAULT_ACCOUNT_HEADER;
    }
    try {
      String header = config.get().accountHeader();
      return header == null || header.isBlank() ? DEFAULT_ACCOUNT_HEADER : header;
    } catch (Exception ignored) {
      return DEFAULT_ACCOUNT_HEADER;
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

  private Optional<String> defaultAccount() {
    if (config.isUnsatisfied()) {
      return Optional.empty();
    }
    try {
      String value = config.get().defaultAccountId();
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
      return "";
    }
    try {
      var cfg = config.get();
      String value = cfg.defaultPrefix().map(String::trim).orElse("");
      if (value.isEmpty()) {
        String systemValue = System.getProperty("floecat.gateway.default-prefix", "");
        String envValue = System.getenv("FLOECAT_GATEWAY_DEFAULT_PREFIX");
        if (!systemValue.isBlank()) {
          value = systemValue.trim();
        } else if (envValue != null && !envValue.isBlank()) {
          value = envValue.trim();
        }
      }
      return value;
    } catch (Exception ex) {
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
